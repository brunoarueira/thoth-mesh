//! Per-topic client authorization (ADR-0018) and per-topic peer-link
//! authorization (ADR-0020), exercised over plain TCP - every
//! connection here is the `anonymous` principal, since there's no TLS
//! client certificate to fingerprint. A fingerprint distinguishing two
//! different principals is covered in `tests/tls.rs`, which already
//! has the certificate infrastructure this would otherwise have to
//! duplicate.

use std::net::SocketAddr;
use std::time::Duration;

use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic, TopicFilter};
use thoth_mesh_node::{NodeOptions, TopicAcl};
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
        NodeOptions {
            topic_acl: Some(acl),
            ..Default::default()
        },
    ));
    addr
}

/// Like [`spawn_test_node_with_topic_acl`], but for `--peer-topic-acl`
/// (ADR-0020) instead.
async fn spawn_test_node_with_peer_topic_acl(entries: &[&str]) -> SocketAddr {
    let acl = TopicAcl::parse(entries.iter().copied()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(thoth_mesh_node::serve_with_tls(
        listener,
        Vec::new(),
        NodeOptions {
            peer_topic_acl: Some(acl),
            ..Default::default()
        },
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

fn filter(s: &str) -> TopicFilter {
    s.parse().unwrap()
}

/// Completes a bare `Hello`/`Hello`-reply handshake over `stream`, so
/// the node on the other end registers it as a peer link (ADR-0009)
/// rather than a client - the same distinction `--peer-topic-acl`
/// (ADR-0020) checks against.
async fn become_peer(stream: &mut Compat<TcpStream>) {
    let hello = Envelope::new(PeerId::new(), MessageKind::Hello { listen_addr: None });
    send(stream, &hello).await;
    match recv(stream).await.kind {
        MessageKind::Hello { .. } => {}
        other => panic!("expected a Hello reply, got {other:?}"),
    }
}

/// Like [`recv`], but discards any `Subscribe` read along the way.
/// A peer link can legitimately receive one it didn't ask for and
/// isn't what a given assertion cares about: interest propagation
/// (ADR-0011) echoes a `Subscribe` back to whichever peer link's own
/// `Subscribe` just moved local interest from 0 to 1 (every active
/// link gets the broadcast, including the one that triggered it), and
/// `register_peer_link` catches a newly-registered peer link up on
/// whatever interest already existed the moment it connects.
async fn recv_skipping_subscribes(stream: &mut Compat<TcpStream>) -> Envelope {
    loop {
        let envelope = recv(stream).await;
        if !matches!(envelope.kind, MessageKind::Subscribe { .. }) {
            return envelope;
        }
    }
}

#[tokio::test]
async fn a_listed_subscribe_and_publish_still_work_normally() {
    let addr = spawn_test_node_with_topic_acl(&["anonymous|pubsub|weather.updates"]).await;
    let mut subscriber = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
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
            filter: topic("secret.topic").into(),
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
            filter: topic("weather.updates").into(),
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
            filter: topic("guarded.topic").into(),
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

/// ADR-0022: a wildcard filter is refused outright wherever a
/// `--topic-acl` is configured, regardless of what it would expand to
/// - even one that would only ever match a topic this principal is
/// actually granted `sub` on.
#[tokio::test]
async fn subscribing_to_a_wildcard_filter_is_rejected_when_a_topic_acl_is_configured() {
    let addr = spawn_test_node_with_topic_acl(&["anonymous|sub|weather.updates"]).await;
    let mut client = connect(addr).await;

    let rejected = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: filter("weather.+"),
        },
    );
    send(&mut client, &rejected).await;
    match recv(&mut client).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(rejected.id)),
        other => panic!("expected an Error, got {other:?}"),
    }

    // The connection stays open, same as any other ACL rejection - a
    // literal subscribe it's actually entitled to still works.
    let allowed = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
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

/// ADR-0020: a peer link whose `--peer-topic-acl` grants it `sub` on a
/// topic gets forwarded a matching publish, the same as a client with
/// an equivalent `--topic-acl` grant would.
#[tokio::test]
async fn a_peer_link_permitted_to_subscribe_receives_the_forwarded_publish() {
    let addr = spawn_test_node_with_peer_topic_acl(&["anonymous|sub|weather.updates"]).await;

    let mut peer = connect(addr).await;
    become_peer(&mut peer).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
        },
    );
    send(&mut peer, &sub).await;
    assert_eq!(
        recv(&mut peer).await.kind,
        MessageKind::Ack {
            in_reply_to: sub.id
        }
    );

    // An ordinary client (no --topic-acl configured on this node) can
    // still publish freely - --peer-topic-acl only ever governs the
    // peer link, never a client.
    let mut publisher = connect(addr).await;
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    let delivered = recv_skipping_subscribes(&mut peer).await;
    assert_eq!(delivered.id, publish.id);
}

/// ADR-0020: a peer link *not* granted `sub` on a topic never gets a
/// forwarder registered for it - a rejected `Subscribe` (the same
/// `Error`-then-open-connection shape ADR-0018 already established for
/// clients), and no publish to that topic ever reaches it afterward.
#[tokio::test]
async fn a_peer_link_denied_subscribe_is_rejected_and_never_forwarded_to() {
    let addr = spawn_test_node_with_peer_topic_acl(&["anonymous|pub|weather.updates"]).await;

    let mut peer = connect(addr).await;
    become_peer(&mut peer).await;
    let rejected = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
        },
    );
    send(&mut peer, &rejected).await;
    match recv(&mut peer).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(rejected.id)),
        other => panic!("expected an Error, got {other:?}"),
    }

    let mut publisher = connect(addr).await;
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    assert!(
        recv_times_out(&mut peer).await,
        "a peer link denied Subscribe by --peer-topic-acl must never be forwarded a matching publish"
    );
}

/// ADR-0022's wildcard-ACL restriction applies to a peer link's
/// `--peer-topic-acl` exactly as it does to a client's `--topic-acl`.
#[tokio::test]
async fn a_peer_links_wildcard_subscribe_is_rejected_when_a_peer_topic_acl_is_configured() {
    let addr = spawn_test_node_with_peer_topic_acl(&["anonymous|sub|weather.updates"]).await;

    let mut peer = connect(addr).await;
    become_peer(&mut peer).await;
    let rejected = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: filter("weather.+"),
        },
    );
    send(&mut peer, &rejected).await;
    match recv(&mut peer).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(rejected.id)),
        other => panic!("expected an Error, got {other:?}"),
    }
}

/// ADR-0020: a peer link granted `pub` on a topic can publish to it,
/// and the publish reaches a locally-subscribed client exactly as a
/// client's own publish would.
#[tokio::test]
async fn a_peer_link_permitted_to_publish_is_delivered_to_a_subscriber() {
    let addr = spawn_test_node_with_peer_topic_acl(&["anonymous|pub|weather.updates"]).await;

    let mut subscriber = connect(addr).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
        },
    );
    send(&mut subscriber, &sub).await;
    assert_eq!(
        recv(&mut subscriber).await.kind,
        MessageKind::Ack {
            in_reply_to: sub.id
        }
    );

    let mut peer = connect(addr).await;
    become_peer(&mut peer).await;
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
        },
    );
    send(&mut peer, &publish).await;

    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, publish.id);
}

/// ADR-0020: a peer link *not* granted `pub` on a topic gets its
/// publish rejected with an `Error` - the connection stays open (same
/// shape as every other topic-ACL rejection) - and it never reaches a
/// locally-subscribed client.
#[tokio::test]
async fn a_peer_link_denied_publish_is_rejected_and_never_reaches_a_subscriber() {
    let addr = spawn_test_node_with_peer_topic_acl(&["anonymous|sub|weather.updates"]).await;

    let mut subscriber = connect(addr).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
        },
    );
    send(&mut subscriber, &sub).await;
    assert_eq!(
        recv(&mut subscriber).await.kind,
        MessageKind::Ack {
            in_reply_to: sub.id
        }
    );

    let mut peer = connect(addr).await;
    become_peer(&mut peer).await;
    let rejected = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"shouldn't arrive".to_vec(),
        },
    );
    send(&mut peer, &rejected).await;
    match recv_skipping_subscribes(&mut peer).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(rejected.id)),
        other => panic!("expected an Error, got {other:?}"),
    }

    assert!(
        recv_times_out(&mut subscriber).await,
        "a publish rejected by --peer-topic-acl must never reach the broker"
    );
}

/// ADR-0020: `--topic-acl` and `--peer-topic-acl` are independent -
/// configuring one leaves the other's connections completely
/// unrestricted, the default (pre-either-ADR) behavior.
#[tokio::test]
async fn a_peer_topic_acl_does_not_restrict_an_ordinary_client() {
    let addr = spawn_test_node_with_peer_topic_acl(&["anonymous|pubsub|weather.updates"]).await;

    // A client (never a peer link) publishing to a topic that isn't
    // even mentioned in the peer ACL still works - --peer-topic-acl
    // never applies to a client connection.
    let mut subscriber = connect(addr).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("unrelated.topic").into(),
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
            topic: topic("unrelated.topic"),
            payload: b"fine".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, publish.id);
}
