//! Per-topic client authorization (ADR-0018), exercised over plain
//! TCP - every connection here is the `anonymous` principal, since
//! there's no TLS client certificate to fingerprint. A fingerprint
//! distinguishing two different principals is covered in
//! `tests/tls.rs`, which already has the certificate infrastructure
//! this would otherwise have to duplicate.

use std::net::SocketAddr;
use std::time::Duration;

use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};
use thoth_mesh_node::TopicAcl;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn spawn_test_node_with_topic_acl(entries: &[&str]) -> SocketAddr {
    let acl = TopicAcl::parse(entries.iter().copied()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(thoth_mesh_node::serve_with_tls(
        listener,
        Vec::new(),
        None,
        Some(acl),
    ));
    addr
}

async fn connect(addr: SocketAddr) -> Compat<TcpStream> {
    TcpStream::connect(addr).await.unwrap().compat()
}

async fn send(stream: &mut Compat<TcpStream>, envelope: &Envelope) {
    let bytes = envelope.to_bytes().unwrap();
    async_framing::write_frame(stream, &bytes).await.unwrap();
}

async fn recv(stream: &mut Compat<TcpStream>) -> Envelope {
    let bytes = timeout(TEST_TIMEOUT, async_framing::read_frame(stream))
        .await
        .expect("timed out waiting for a frame")
        .unwrap();
    Envelope::from_bytes(&bytes).unwrap()
}

async fn recv_times_out(stream: &mut Compat<TcpStream>) -> bool {
    timeout(
        Duration::from_millis(200),
        async_framing::read_frame(stream),
    )
    .await
    .is_err()
}

fn topic(s: &str) -> Topic {
    s.parse().unwrap()
}

#[tokio::test]
async fn a_listed_subscribe_and_publish_still_work_normally() {
    let addr = spawn_test_node_with_topic_acl(&["anonymous|pubsub|weather.updates"]).await;
    let mut subscriber = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut subscriber, &sub).await;
    assert_eq!(
        recv(&mut subscriber).await.kind,
        MessageKind::Ack {
            in_reply_to: sub.id
        }
    );

    let mut publisher = connect(addr).await;
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, publish.id);
}

#[tokio::test]
async fn subscribing_to_an_unlisted_topic_is_rejected_but_the_connection_stays_open() {
    let addr = spawn_test_node_with_topic_acl(&["anonymous|sub|weather.updates"]).await;
    let mut client = connect(addr).await;

    let rejected = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("secret.topic"),
        },
    );
    send(&mut client, &rejected).await;
    match recv(&mut client).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(rejected.id)),
        other => panic!("expected an Error, got {other:?}"),
    }

    // Unlike a rejected peer link (ADR-0017), a topic ACL rejection
    // doesn't close the connection - the same client can still do
    // something it's actually allowed to.
    let allowed = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut client, &allowed).await;
    assert_eq!(
        recv(&mut client).await.kind,
        MessageKind::Ack {
            in_reply_to: allowed.id
        }
    );
}

#[tokio::test]
async fn publishing_without_permission_is_rejected_and_never_reaches_a_subscriber() {
    // anonymous may subscribe to guarded.topic, but not publish to it -
    // action is its own axis in a --topic-acl entry, independent of
    // whether the same principal has the *other* permission on the
    // same topic.
    let addr = spawn_test_node_with_topic_acl(&["anonymous|sub|guarded.topic"]).await;

    let mut subscriber = connect(addr).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("guarded.topic"),
        },
    );
    send(&mut subscriber, &sub).await;
    assert_eq!(
        recv(&mut subscriber).await.kind,
        MessageKind::Ack {
            in_reply_to: sub.id
        }
    );

    let mut publisher = connect(addr).await;
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("guarded.topic"),
            payload: b"shouldn't arrive".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;
    match recv(&mut publisher).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(publish.id)),
        other => panic!("expected an Error, got {other:?}"),
    }

    assert!(
        recv_times_out(&mut subscriber).await,
        "a publish rejected by the topic ACL must never reach the broker"
    );
}
