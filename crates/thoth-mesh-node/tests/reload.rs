//! Live config reload (ADR-0055): proves a fresh [`ReloadableAcls`]
//! pushed through the exact channel `NodeOptions::reload` carries
//! reaches an *already-established* connection's very next ACL check,
//! not just a connection made after the reload, with no restart and
//! the connection never dropped. Driven directly via a
//! `watch::Sender` rather than a real `SIGHUP`, exactly the way
//! ADR-0055's "library stays config-file-and-signal-agnostic"
//! decision means a test can: `main.rs` owns the signal-and-file
//! side entirely, outside anything this crate's own library exposes.

use std::net::SocketAddr;
use std::time::Duration;

use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};
use thoth_mesh_node::{NodeOptions, ReloadableAcls, TopicAcl};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn spawn_test_node() -> (SocketAddr, watch::Sender<ReloadableAcls>) {
    let (reload_tx, reload_rx) = watch::channel(ReloadableAcls::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(thoth_mesh_node::serve_with_tls(
        listener,
        Vec::new(),
        NodeOptions {
            reload: Some(reload_rx),
            ..Default::default()
        },
    ));
    (addr, reload_tx)
}

async fn connect(addr: SocketAddr) -> Compat<TcpStream> {
    TcpStream::connect(addr).await.unwrap().compat()
}

async fn send(stream: &mut Compat<TcpStream>, envelope: &Envelope) {
    let bytes = envelope.to_bytes().unwrap();
    async_framing::write_frame(stream, &bytes).await.unwrap();
}

/// `None` if nothing arrives within a short window - distinct from
/// [`recv`], which treats that as a test failure. Used here to poll an
/// already-established connection for whether a just-pushed reload has
/// reached it yet, without a fixed sleep racing the reload task's own
/// scheduling.
async fn try_recv(stream: &mut Compat<TcpStream>) -> Option<Envelope> {
    let bytes = timeout(Duration::from_millis(50), async_framing::read_frame(stream))
        .await
        .ok()?
        .unwrap();
    Some(Envelope::from_bytes(&bytes).unwrap())
}

fn topic(s: &str) -> Topic {
    s.parse().unwrap()
}

fn publish_envelope(topic: Topic) -> Envelope {
    Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic,
            payload: b"x".to_vec(),
            retain: false,
            content_type: None,
            reply_to: None,
            in_reply_to: None,
        },
    )
}

#[tokio::test]
async fn a_reload_reaches_an_already_established_connections_very_next_publish() {
    let (addr, reload_tx) = spawn_test_node().await;
    let mut publisher = connect(addr).await;

    // No --topic-acl configured yet - every publish succeeds, and
    // succeeding gives no reply at all on the publisher's own
    // connection (see tests/topic_acl.rs's own tests for the same
    // observation).
    let before_reload = publish_envelope(topic("weather.updates"));
    send(&mut publisher, &before_reload).await;
    assert!(
        try_recv(&mut publisher).await.is_none(),
        "a publish with no ACL configured must never get a reply"
    );

    // Now reload in a topic ACL that permits anonymous to *subscribe*
    // to weather.updates, but not publish to it - via the same
    // channel NodeOptions::reload carries, no restart, and crucially
    // no new connection: `publisher` is reused as-is below.
    let acl = TopicAcl::parse(["anonymous|sub|weather.updates"].into_iter()).unwrap();
    reload_tx
        .send(ReloadableAcls {
            topic_acl: Some(acl),
            ..ReloadableAcls::default()
        })
        .unwrap();

    // The reload applier task runs concurrently with this test, so
    // there's no guarantee it's already applied by the time the send
    // above returns - poll the very same already-established
    // connection until it has, bounded by TEST_TIMEOUT.
    let rejected = timeout(TEST_TIMEOUT, async {
        loop {
            let attempt = publish_envelope(topic("weather.updates"));
            send(&mut publisher, &attempt).await;
            if let Some(envelope) = try_recv(&mut publisher).await {
                return (attempt.id, envelope);
            }
        }
    })
    .await
    .expect("reload never reached the already-established connection");

    let (attempt_id, envelope) = rejected;
    match envelope.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(attempt_id)),
        other => panic!("expected an Error once the reload applied, got {other:?}"),
    }
}
