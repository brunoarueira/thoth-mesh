//! Per-principal publish rate limiting (ADR-0051), exercised over
//! plain TCP - every connection here is the `anonymous` principal,
//! since there's no TLS client certificate to fingerprint, so every
//! client connection in a given test shares one bucket. See
//! `tests/topic_acl.rs` for the same anonymous-only caveat applied to
//! `--topic-acl`.

use std::net::SocketAddr;
use std::time::Duration;

use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};
use thoth_mesh_node::{NodeOptions, RateLimitConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn spawn_test_node_with_rate_limit(per_sec: u32, burst: u32) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(thoth_mesh_node::serve_with_tls(
        listener,
        Vec::new(),
        NodeOptions {
            rate_limit: Some(RateLimitConfig { per_sec, burst }),
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

fn publish(topic: Topic, payload: &[u8]) -> Envelope {
    Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic,
            payload: payload.to_vec(),
            retain: false,
            content_type: None,
            reply_to: None,
            in_reply_to: None,
        },
    )
}

#[tokio::test]
async fn publishes_within_the_burst_still_work_normally() {
    let addr = spawn_test_node_with_rate_limit(1, 3).await;
    let mut subscriber = connect(addr).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
            durable: false,
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
    let msg = publish(topic("weather.updates"), b"sunny");
    send(&mut publisher, &msg).await;

    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, msg.id);
}

#[tokio::test]
async fn a_publish_exceeding_the_burst_is_rejected_and_never_reaches_a_subscriber() {
    let addr = spawn_test_node_with_rate_limit(1, 1).await;

    let mut subscriber = connect(addr).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
            durable: false,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let mut publisher = connect(addr).await;
    let first = publish(topic("weather.updates"), b"sunny");
    send(&mut publisher, &first).await;
    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, first.id);

    // Same principal (anonymous), a second connection - the quota is
    // per-principal, not per-connection, so this is still throttled.
    let mut second_publisher = connect(addr).await;
    let second = publish(topic("weather.updates"), b"cloudy");
    send(&mut second_publisher, &second).await;
    match recv(&mut second_publisher).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(second.id)),
        other => panic!("expected an Error, got {other:?}"),
    }

    assert!(
        recv_times_out(&mut subscriber).await,
        "a publish rejected by the rate limiter must never reach the broker"
    );
}

#[tokio::test]
async fn the_bucket_refills_over_time() {
    // per_sec: 50 refills one token every 20ms - wide enough that the
    // back-to-back first/second publish below (real socket I/O plus
    // task scheduling, not just two function calls) can't plausibly
    // cross it even on a loaded CI runner, unlike a tighter interval
    // that leaves this timing-sensitive.
    let addr = spawn_test_node_with_rate_limit(50, 1).await;
    let mut publisher = connect(addr).await;

    let first = publish(topic("weather.updates"), b"sunny");
    send(&mut publisher, &first).await;
    // No subscriber is listening, so nothing comes back for an
    // accepted publish either way - only a rejected one gets an
    // Error. Confirm the second, near-immediate publish (same
    // connection, same principal) *is* rejected...
    let second = publish(topic("weather.updates"), b"cloudy");
    send(&mut publisher, &second).await;
    match recv(&mut publisher).await.kind {
        MessageKind::Error { in_reply_to, .. } => assert_eq!(in_reply_to, Some(second.id)),
        other => panic!("expected an Error, got {other:?}"),
    }

    // ...then, after the bucket's had time to refill (well past the
    // 20ms refill interval above), a further publish goes through
    // again (no Error reply).
    tokio::time::sleep(Duration::from_millis(60)).await;
    let third = publish(topic("weather.updates"), b"rain");
    send(&mut publisher, &third).await;
    assert!(
        recv_times_out(&mut publisher).await,
        "a refilled bucket should accept the publish with no Error reply"
    );
}
