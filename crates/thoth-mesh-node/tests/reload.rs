//! Live config reload (ADR-0055): proves a fresh [`ReloadableAcls`]
//! pushed through the exact channel `NodeOptions::reload` carries
//! reaches an *already-established* connection's very next ACL check,
//! not just a connection made after the reload, with no restart and
//! the connection never dropped. Driven directly via a
//! `watch::Sender` rather than a real `SIGHUP`, exactly the way
//! ADR-0055's "library stays config-file-and-signal-agnostic"
//! decision means a test can: `main.rs` owns the signal-and-file
//! side entirely, outside anything this crate's own library exposes.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};
use thoth_mesh_node::{NodeOptions, ReloadableAcls, TopicAcl};
use tokio::net::{TcpListener, TcpStream, tcp::OwnedWriteHalf};
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

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

/// Connects to `addr` and immediately spawns a background task that
/// continuously reads frames off the connection into the returned
/// channel, so polling for "has anything arrived yet" is a cancel-safe
/// `mpsc::Receiver::recv` under a timeout - never a `timeout` wrapped
/// directly around `async_framing::read_frame` itself. Cancelling
/// `read_frame` mid-frame (after its length prefix is already
/// consumed from the stream but before its payload is) permanently
/// desyncs every frame after it - the exact hazard ADR-0029 documents
/// for why `connection.rs` itself splits its read and write loops
/// onto independent tasks rather than racing one against a timeout.
async fn connect(addr: SocketAddr) -> (Compat<OwnedWriteHalf>, mpsc::UnboundedReceiver<Envelope>) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, write_half) = stream.into_split();
    let mut reader = read_half.compat();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok(bytes) = async_framing::read_frame(&mut reader).await {
            if tx.send(Envelope::from_bytes(&bytes).unwrap()).is_err() {
                break;
            }
        }
    });
    (write_half.compat_write(), rx)
}

async fn send(stream: &mut Compat<OwnedWriteHalf>, envelope: &Envelope) {
    let bytes = envelope.to_bytes().unwrap();
    async_framing::write_frame(stream, &bytes).await.unwrap();
}

/// `None` if nothing arrives within a short window - distinct from a
/// bare `.recv().await`, which would hang forever once the reader
/// task has nothing left to send. Used to poll an already-established
/// connection for whether a just-pushed reload has reached it yet,
/// without a fixed sleep racing the reload task's own scheduling.
/// Cancel-safe: `mpsc::UnboundedReceiver::recv` never partially
/// consumes a message, so a timed-out attempt loses nothing that a
/// later call would otherwise have seen.
async fn try_recv(incoming: &mut mpsc::UnboundedReceiver<Envelope>) -> Option<Envelope> {
    timeout(Duration::from_millis(50), incoming.recv())
        .await
        .ok()?
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
    let (mut publisher, mut incoming) = connect(addr).await;

    // No --topic-acl configured yet - every publish succeeds, and
    // succeeding gives no reply at all on the publisher's own
    // connection (see tests/topic_acl.rs's own tests for the same
    // observation).
    let before_reload = publish_envelope(topic("weather.updates"));
    send(&mut publisher, &before_reload).await;
    assert!(
        try_recv(&mut incoming).await.is_none(),
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
    // above returns - keep publishing on the very same already-
    // established connection until one gets rejected, bounded by
    // TEST_TIMEOUT. Every attempt id sent is tracked (not just the
    // most recent one): the server processes frames strictly in the
    // order they were sent, but an earlier attempt's own processing -
    // and thus its Error reply, if the reload lands mid-flight - can
    // still be pending when a later attempt is sent, so the first
    // Error to arrive isn't guaranteed to be for the most recent
    // attempt.
    let mut sent = HashSet::new();
    let envelope = timeout(TEST_TIMEOUT, async {
        loop {
            let attempt = publish_envelope(topic("weather.updates"));
            sent.insert(attempt.id);
            send(&mut publisher, &attempt).await;
            if let Some(envelope) = try_recv(&mut incoming).await {
                return envelope;
            }
        }
    })
    .await
    .expect("reload never reached the already-established connection");

    match envelope.kind {
        MessageKind::Error { in_reply_to, .. } => {
            assert!(
                in_reply_to.is_some_and(|id| sent.contains(&id)),
                "rejection {in_reply_to:?} doesn't match any attempt this test actually sent"
            );
        }
        other => panic!("expected an Error once the reload applied, got {other:?}"),
    }
}
