use std::net::SocketAddr;
use std::time::Duration;

use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic, TopicFilter};
use thoth_mesh_node::test_support::eventually;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

// Generous enough to absorb CI scheduling jitter - the multi-node
// tests spin up several real nodes' worth of background tasks and
// TCP round trips, which can be noticeably slower on a contended
// runner than locally. `recv_times_out`'s negative checks use their
// own, much shorter, independent timeout, so raising this only
// affects how long we wait for an expected delivery.
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn spawn_test_node() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(thoth_mesh_node::serve(listener, Vec::new()));
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
    s.parse::<Topic>().unwrap()
}

fn filter(s: &str) -> TopicFilter {
    s.parse::<TopicFilter>().unwrap()
}

/// Publishes fresh envelopes against `publish_addr` (a new connection
/// and a new `MessageId` each attempt) until one shows up on
/// `subscriber`, or the overall [`TEST_TIMEOUT`] elapses.
///
/// Interest propagation across peer links (ADR-0011) happens in
/// background tasks a caller doesn't otherwise synchronize with, so
/// tests relying on it need to poll rather than publish once and read
/// once - the same reasoning `test_support::eventually` documents for
/// membership updates.
async fn publish_until_delivered(
    publish_addr: SocketAddr,
    subscriber: &mut Compat<TcpStream>,
    topic: Topic,
    payload: &[u8],
) -> Envelope {
    let expected_kind = MessageKind::Publish {
        topic: topic.clone(),
        payload: payload.to_vec(),
        retain: false,
        content_type: None,
    };
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        let mut publisher = connect(publish_addr).await;
        let publish = Envelope::new(PeerId::new(), expected_kind.clone());
        send(&mut publisher, &publish).await;

        // A retry from earlier in this call may have been delivered
        // late rather than lost - drain and ignore duplicates of the
        // envelope we're after rather than treating one as fatal.
        loop {
            match timeout(
                Duration::from_millis(100),
                async_framing::read_frame(subscriber),
            )
            .await
            {
                Ok(Ok(bytes)) => {
                    let delivered = Envelope::from_bytes(&bytes).unwrap();
                    if delivered.kind == expected_kind {
                        return delivered;
                    }
                }
                _ => break, // nothing queued right now; publish again
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "publish on {topic} never propagated to the subscriber within {TEST_TIMEOUT:?}"
            );
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "publish on {topic} never propagated to the subscriber within {TEST_TIMEOUT:?}"
        );
    }
}

#[tokio::test]
async fn subscribe_receives_ack() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut client, &sub).await;

    let ack = recv(&mut client).await;
    assert_eq!(
        ack.kind,
        MessageKind::Ack {
            in_reply_to: sub.id
        }
    );
}

#[tokio::test]
async fn unsubscribe_receives_ack() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut client, &sub).await;
    recv(&mut client).await; // subscribe ack

    let unsub = Envelope::new(
        PeerId::new(),
        MessageKind::Unsubscribe {
            filter: topic("weather.updates").into(),
        },
    );
    send(&mut client, &unsub).await;

    let ack = recv(&mut client).await;
    assert_eq!(
        ack.kind,
        MessageKind::Ack {
            in_reply_to: unsub.id
        }
    );
}

#[tokio::test]
async fn publish_delivers_to_subscriber() {
    let addr = spawn_test_node().await;
    let mut subscriber = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, publish.id);
    assert_eq!(delivered.kind, publish.kind);
}

/// A `content_type` hint (ADR-0044) is carried through delivery
/// verbatim - the node never touches it. Asserting `delivered.kind`
/// equals the published one already covers this (the field is part of
/// the variant), but a dedicated test documents it and would catch a
/// forwarding path that reconstructed the `Publish` instead of
/// passing it through.
#[tokio::test]
async fn a_content_type_hint_reaches_the_subscriber_unchanged() {
    let addr = spawn_test_node().await;
    let mut subscriber = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: br#"{"temp":21}"#.to_vec(),
            retain: false,
            content_type: Some("application/json".to_owned()),
        },
    );
    send(&mut publisher, &publish).await;

    let delivered = recv(&mut subscriber).await;
    match delivered.kind {
        MessageKind::Publish { content_type, .. } => {
            assert_eq!(content_type, Some("application/json".to_owned()));
        }
        other => panic!("expected a Publish, got {other:?}"),
    }
}

/// A `Subscribe { ack: true }` (ADR-0041) still delivers exactly like
/// an ordinary subscription, and sending the resulting `Ack` back is
/// accepted without upsetting the connection - the real-timeout,
/// real-redelivery behavior itself is covered at the unit level
/// (`connection::tests`, against `spawn_ack_forwarder` directly) so
/// this test isn't stuck waiting out `DEFAULT_ACK_TIMEOUT` for real.
#[tokio::test]
async fn an_acked_subscription_delivers_and_accepts_the_clients_ack() {
    let addr = spawn_test_node().await;
    let mut subscriber = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: true,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, publish.id);

    let client_ack = Envelope::new(
        PeerId::new(),
        MessageKind::Ack {
            in_reply_to: delivered.id,
        },
    );
    send(&mut subscriber, &client_ack).await;

    // The connection is still healthy after sending an Ack back -
    // an ordinary publish still reaches it.
    let second_publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"cloudy".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &second_publish).await;
    let second_delivered = recv(&mut subscriber).await;
    assert_eq!(second_delivered.id, second_publish.id);
}

/// `ack: true` and `group: Some(_)` together is refused outright
/// (ADR-0042) - the node hasn't defined what acknowledgement means
/// for a group. The connection stays open and usable afterward, same
/// as any other `Subscribe` rejection (ADR-0018).
#[tokio::test]
async fn ack_and_group_together_is_rejected() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: true,
            group: Some("workers".to_owned()),
        },
    );
    send(&mut client, &sub).await;

    let reply = recv(&mut client).await;
    assert_eq!(
        reply.kind,
        MessageKind::Error {
            in_reply_to: Some(sub.id),
            message: "ack and group are not supported together".to_owned(),
        }
    );

    // The connection is still usable - an ordinary (non-group,
    // non-ack) subscribe on it still works.
    let ordinary = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut client, &ordinary).await;
    let ack = recv(&mut client).await;
    assert_eq!(
        ack.kind,
        MessageKind::Ack {
            in_reply_to: ordinary.id
        }
    );
}

/// Two connections joining the same named group for the same filter
/// each get every other publish, round-robin - not both getting
/// everything (ordinary fan-out) and not either one starved
/// (ADR-0042).
#[tokio::test]
async fn consumer_group_round_robins_across_two_members() {
    let addr = spawn_test_node().await;
    let mut member_a = connect(addr).await;
    let mut member_b = connect(addr).await;
    let mut publisher = connect(addr).await;

    for member in [&mut member_a, &mut member_b] {
        let sub = Envelope::new(
            PeerId::new(),
            MessageKind::Subscribe {
                filter: topic("weather.updates").into(),
                ack: false,
                group: Some("workers".to_owned()),
            },
        );
        send(member, &sub).await;
        recv(member).await; // subscribe ack
    }

    let mut published = Vec::with_capacity(4);
    for i in 0..4u32 {
        let publish = Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: topic("weather.updates"),
                payload: format!("update {i}").into_bytes(),
                retain: false,
                content_type: None,
            },
        );
        send(&mut publisher, &publish).await;
        published.push(publish);
    }

    let received_a = [recv(&mut member_a).await.id, recv(&mut member_a).await.id];
    let received_b = [recv(&mut member_b).await.id, recv(&mut member_b).await.id];
    assert_eq!(received_a, [published[0].id, published[2].id]);
    assert_eq!(received_b, [published[1].id, published[3].id]);
    assert!(recv_times_out(&mut member_a).await);
    assert!(recv_times_out(&mut member_b).await);
}

/// A group member that `Unsubscribe`s is dropped from the round-robin
/// rotation - the remaining member then gets every message, and the
/// one that left gets nothing (ADR-0042). Deliberately tests the
/// `Unsubscribe` path rather than a disconnect: an `Unsubscribe`
/// leaves the connection *open*, so if `leave_group` weren't actually
/// called, `member_b` would still be a live channel and round-robin
/// would send it half the messages - which `recv_times_out(member_b)`
/// below would catch. A disconnect can't isolate that: a dropped
/// connection's channel also closes, and `Broker`'s delivery already
/// skips a closed channel on its own, so the two paths are
/// indistinguishable from the wire. The disconnect path itself just
/// reuses this same `leave_group` call from `shut_down`.
#[tokio::test]
async fn a_group_member_that_unsubscribes_is_dropped_from_the_rotation() {
    let addr = spawn_test_node().await;
    let mut member_a = connect(addr).await;
    let mut member_b = connect(addr).await;
    let mut publisher = connect(addr).await;

    for member in [&mut member_a, &mut member_b] {
        let sub = Envelope::new(
            PeerId::new(),
            MessageKind::Subscribe {
                filter: topic("weather.updates").into(),
                ack: false,
                group: Some("workers".to_owned()),
            },
        );
        send(member, &sub).await;
        recv(member).await; // subscribe ack
    }

    let unsub = Envelope::new(
        PeerId::new(),
        MessageKind::Unsubscribe {
            filter: topic("weather.updates").into(),
        },
    );
    send(&mut member_b, &unsub).await;
    recv(&mut member_b).await; // unsubscribe ack

    let mut published = Vec::with_capacity(4);
    for i in 0..4u32 {
        let publish = Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: topic("weather.updates"),
                payload: format!("update {i}").into_bytes(),
                retain: false,
                content_type: None,
            },
        );
        send(&mut publisher, &publish).await;
        published.push(publish);
    }

    // Every message goes to the one member still in the group, in
    // order - if `member_b` were still in the rotation, it would have
    // taken messages 1 and 3 and these `recv`s would hang.
    for publish in &published {
        assert_eq!(recv(&mut member_a).await.id, publish.id);
    }
    // And the member that left gets nothing at all.
    assert!(recv_times_out(&mut member_b).await);
}

/// Unsubscribing from a group leaves it - `Broker::leave_group` is
/// wired up the same way an ordinary forwarder's teardown is
/// (ADR-0042).
#[tokio::test]
async fn unsubscribing_from_a_group_leaves_it() {
    let addr = spawn_test_node().await;
    let mut member = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: Some("workers".to_owned()),
        },
    );
    send(&mut member, &sub).await;
    recv(&mut member).await; // subscribe ack

    let unsub = Envelope::new(
        PeerId::new(),
        MessageKind::Unsubscribe {
            filter: topic("weather.updates").into(),
        },
    );
    send(&mut member, &unsub).await;
    recv(&mut member).await; // unsubscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"anybody?".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    assert!(recv_times_out(&mut member).await);
}

#[tokio::test]
async fn multiple_subscribers_all_receive() {
    let addr = spawn_test_node().await;
    let mut sub_a = connect(addr).await;
    let mut sub_b = connect(addr).await;
    let mut publisher = connect(addr).await;

    for client in [&mut sub_a, &mut sub_b] {
        let sub = Envelope::new(
            PeerId::new(),
            MessageKind::Subscribe {
                filter: topic("weather.updates").into(),
                ack: false,
                group: None,
            },
        );
        send(client, &sub).await;
        recv(client).await; // subscribe ack
    }

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    assert_eq!(recv(&mut sub_a).await.id, publish.id);
    assert_eq!(recv(&mut sub_b).await.id, publish.id);
}

#[tokio::test]
async fn a_late_subscriber_is_replayed_a_publish_that_happened_before_it_subscribed() {
    // No subscriber exists yet when this is published - see ADR-0021.
    let addr = spawn_test_node().await;
    let mut publisher = connect(addr).await;
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    let mut subscriber = connect(addr).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    // The publish from before this connection even existed still
    // arrives, replayed from the topic's buffer.
    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, publish.id);
    assert_eq!(delivered.kind, publish.kind);
}

/// A `retain: true` publish (ADR-0043) reaches a subscriber that only
/// connects afterward. Uses a *wildcard* subscribe to a pattern
/// nobody has used before - whose own replay buffer is therefore
/// empty (ADR-0022) - so the delivery can only be the retained value,
/// not ordinary replay: an equivalent non-retained publish would not
/// be retroactively matched into a fresh pattern at all.
#[tokio::test]
async fn a_retained_publish_reaches_a_wildcard_subscriber_that_connects_afterward() {
    let addr = spawn_test_node().await;
    let mut publisher = connect(addr).await;
    let retained = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("sensor.temp"),
            payload: b"21C".to_vec(),
            retain: true,
            content_type: None,
        },
    );
    send(&mut publisher, &retained).await;

    let mut subscriber = connect(addr).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: filter("sensor.+"),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, retained.id);
    assert_eq!(delivered.kind, retained.kind);
}

/// Publishing an empty payload with `retain: true` clears the topic's
/// retained message (ADR-0043) - a later wildcard subscriber to a
/// fresh pattern then gets nothing.
#[tokio::test]
async fn an_empty_retained_publish_clears_the_retained_message() {
    let addr = spawn_test_node().await;
    let mut publisher = connect(addr).await;

    for payload in [b"21C".as_slice(), b"".as_slice()] {
        let publish = Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: topic("sensor.temp"),
                payload: payload.to_vec(),
                retain: true,
                content_type: None,
            },
        );
        send(&mut publisher, &publish).await;
    }

    let mut subscriber = connect(addr).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: filter("sensor.+"),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    assert!(recv_times_out(&mut subscriber).await);
}

#[tokio::test]
async fn resubscribing_to_an_already_subscribed_topic_does_not_replay_again() {
    let addr = spawn_test_node().await;
    let mut subscriber = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;
    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, publish.id);

    // Subscribing again to the same topic on the same connection,
    // without ever unsubscribing, is still a no-op - see PROTOCOL.md's
    // Subscribe section. It must not spawn a second forwarder and
    // replay the same publish a second time.
    let resub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &resub).await;
    let ack = recv(&mut subscriber).await;
    assert_eq!(
        ack.kind,
        MessageKind::Ack {
            in_reply_to: resub.id
        }
    );
    assert!(recv_times_out(&mut subscriber).await);
}

#[tokio::test]
async fn unsubscribed_client_does_not_receive_publish() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut client, &sub).await;
    recv(&mut client).await; // subscribe ack

    let unsub = Envelope::new(
        PeerId::new(),
        MessageKind::Unsubscribe {
            filter: topic("weather.updates").into(),
        },
    );
    send(&mut client, &unsub).await;
    recv(&mut client).await; // unsubscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    assert!(recv_times_out(&mut client).await);
}

#[tokio::test]
async fn distinct_topics_do_not_cross_deliver() {
    let addr = spawn_test_node().await;
    let mut subscriber = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("traffic.updates"),
            payload: b"jam".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    assert!(recv_times_out(&mut subscriber).await);
}

#[tokio::test]
async fn a_wildcard_subscriber_receives_a_matching_publish() {
    // ADR-0022: a `+` filter matches any single segment in that
    // position.
    let addr = spawn_test_node().await;
    let mut subscriber = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: filter("weather.+"),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.kind, publish.kind);
}

#[tokio::test]
async fn a_wildcard_subscriber_does_not_receive_a_non_matching_publish() {
    let addr = spawn_test_node().await;
    let mut subscriber = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: filter("weather.+"),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("traffic.updates"),
            payload: b"jam".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    assert!(recv_times_out(&mut subscriber).await);
}

#[tokio::test]
async fn an_exact_and_a_matching_wildcard_subscriber_on_the_same_connection_both_deliver() {
    // Two independent subscriptions (ADR-0022) - the connection should
    // see the matching publish twice, once per subscription.
    let addr = spawn_test_node().await;
    let mut subscriber = connect(addr).await;
    let mut publisher = connect(addr).await;

    for sub_filter in [filter("weather.updates"), filter("weather.+")] {
        let sub = Envelope::new(
            PeerId::new(),
            MessageKind::Subscribe {
                filter: sub_filter,
                ack: false,
                group: None,
            },
        );
        send(&mut subscriber, &sub).await;
        recv(&mut subscriber).await; // subscribe ack
    }

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    let first = recv(&mut subscriber).await;
    let second = recv(&mut subscriber).await;
    assert_eq!(first.kind, publish.kind);
    assert_eq!(second.kind, publish.kind);
}

#[tokio::test]
async fn hello_receives_a_hello_reply_with_our_listen_addr() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;

    let hello = Envelope::new(
        PeerId::new(),
        MessageKind::Hello {
            listen_addr: Some("127.0.0.1:49999".to_owned()),
        },
    );
    send(&mut client, &hello).await;

    let reply = recv(&mut client).await;
    assert_eq!(
        reply.kind,
        MessageKind::Hello {
            listen_addr: Some(addr.to_string())
        }
    );
}

#[tokio::test]
async fn two_real_nodes_see_each_other_as_reachable() {
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn(listener_b, Vec::new());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_a = thoth_mesh_node::spawn(listener_a, vec![addr_b.to_string()]);

    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
    eventually(|| node_b.membership.is_reachable(node_a.id)).await;
}

#[tokio::test]
async fn peer_becomes_unreachable_once_its_connection_drops() {
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn(listener_b, Vec::new());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_a = thoth_mesh_node::spawn(listener_a, vec![addr_b.to_string()]);

    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
    eventually(|| node_b.membership.is_reachable(node_a.id)).await;

    // Sever node A's dialed connection to node B, as if node A had
    // disappeared - node B should notice on its next read and mark
    // it unreachable.
    for handle in &node_a.peer_dials {
        handle.abort();
    }

    eventually(|| !node_b.membership.is_reachable(node_a.id)).await;
}

#[tokio::test]
async fn hello_marks_the_sender_reachable_then_unreachable_on_disconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let node = thoth_mesh_node::spawn(listener, Vec::new());

    let mut client = connect(addr).await;
    let sender = PeerId::new();
    assert!(!node.membership.is_reachable(sender));

    let hello = Envelope::new(sender, MessageKind::Hello { listen_addr: None });
    send(&mut client, &hello).await;
    recv(&mut client).await; // the node's own Hello reply

    // The node applies mark_connected before replying, so this is
    // already true by the time we get the reply above - no polling
    // needed.
    assert!(node.membership.is_reachable(sender));

    drop(client);
    eventually(|| !node.membership.is_reachable(sender)).await;
}

#[tokio::test]
async fn dial_side_peer_link_forwards_local_publishes_once_subscribed() {
    // A raw socket standing in for "peer B" - lets us drive the
    // handshake and post-handshake traffic by hand, the same way the
    // other raw-client tests in this file do, but from the far end of
    // a connection node A dialed rather than one a client dialed into
    // node A. Exercises the dial side specifically (ADR-0010): before
    // that decision, only the accept side ran the broker-wired
    // dispatch loop.
    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer_listener.local_addr().unwrap();

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let _node_a = thoth_mesh_node::spawn(listener_a, vec![peer_addr.to_string()]);

    let (socket, _) = timeout(TEST_TIMEOUT, peer_listener.accept())
        .await
        .expect("timed out waiting for node A to dial")
        .unwrap();
    let mut peer = socket.compat();

    // Complete the handshake node A initiates.
    timeout(TEST_TIMEOUT, async_framing::read_frame(&mut peer))
        .await
        .expect("timed out waiting for node A's Hello")
        .unwrap();
    let peer_id = PeerId::new();
    let hello_reply = Envelope::new(peer_id, MessageKind::Hello { listen_addr: None });
    send(&mut peer, &hello_reply).await;

    // "Peer B" subscribes over the link node A dialed.
    let sub = Envelope::new(
        peer_id,
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut peer, &sub).await;
    let ack = recv(&mut peer).await;
    assert_eq!(
        ack.kind,
        MessageKind::Ack {
            in_reply_to: sub.id
        }
    );

    // This was node A's first interest in the topic, so it's echoed
    // straight back down every peer link, including this one
    // (ADR-0011) - drain that before looking for the forwarded
    // publish.
    let echoed = recv(&mut peer).await;
    assert_eq!(
        echoed.kind,
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        }
    );

    // An ordinary client publishes on node A directly.
    let mut publisher = connect(addr_a).await;
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    // It should be forwarded down the dialed peer link.
    let delivered = recv(&mut peer).await;
    assert_eq!(delivered.id, publish.id);
}

#[tokio::test]
async fn a_peer_links_wildcard_interest_propagates_and_receives_a_matching_publish() {
    // Same shape as the exact-topic version above, but the peer's
    // interest is a pattern (ADR-0022) - interest propagation and
    // forwarding don't special-case that at all.
    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer_listener.local_addr().unwrap();

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let _node_a = thoth_mesh_node::spawn(listener_a, vec![peer_addr.to_string()]);

    let (socket, _) = timeout(TEST_TIMEOUT, peer_listener.accept())
        .await
        .expect("timed out waiting for node A to dial")
        .unwrap();
    let mut peer = socket.compat();

    timeout(TEST_TIMEOUT, async_framing::read_frame(&mut peer))
        .await
        .expect("timed out waiting for node A's Hello")
        .unwrap();
    let peer_id = PeerId::new();
    let hello_reply = Envelope::new(peer_id, MessageKind::Hello { listen_addr: None });
    send(&mut peer, &hello_reply).await;

    let sub = Envelope::new(
        peer_id,
        MessageKind::Subscribe {
            filter: filter("weather.+"),
            ack: false,
            group: None,
        },
    );
    send(&mut peer, &sub).await;
    recv(&mut peer).await; // subscribe ack
    recv(&mut peer).await; // interest echo (ADR-0011)

    let mut publisher = connect(addr_a).await;
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &publish).await;

    let delivered = recv(&mut peer).await;
    assert_eq!(delivered.id, publish.id);
}

#[tokio::test]
async fn multi_hop_interest_propagates_across_a_chain_of_peers() {
    // A - B - C, a chain rather than a full mesh: A and C are never
    // directly peered. A subscriber on C should still see publishes
    // sent to A, once C's interest has flood-filled back to A through
    // B (ADR-0011's whole point - propagation isn't limited to
    // directly peered nodes).
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = thoth_mesh_node::spawn(listener_a, Vec::new());

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn(listener_b, vec![addr_a.to_string()]);

    let listener_c = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_c = listener_c.local_addr().unwrap();
    let node_c = thoth_mesh_node::spawn(listener_c, vec![addr_b.to_string()]);

    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
    eventually(|| node_b.membership.is_reachable(node_c.id)).await;

    let mut subscriber = connect(addr_c).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let delivered =
        publish_until_delivered(addr_a, &mut subscriber, topic("weather.updates"), b"sunny").await;
    assert_eq!(
        delivered.kind,
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        }
    );
}

#[tokio::test]
async fn loop_prevention_stops_a_publish_from_bouncing_forever() {
    // Two nodes with a single peer link between them already forms a
    // cycle: once both ends are interested, A forwards its publish to
    // B, and B - not knowing where an envelope came from, just that
    // it's new to its own broker - rebroadcasts it to every locally
    // interested connection, including the one pointing right back at
    // A. Without the MessageId dedup in Broker::publish (ADR-0011),
    // that publish would bounce forever, and each subscriber would
    // see it delivered more than once.
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = thoth_mesh_node::spawn(listener_a, Vec::new());

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn(listener_b, vec![addr_a.to_string()]);

    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
    eventually(|| node_b.membership.is_reachable(node_a.id)).await;

    let mut sub_a = connect(addr_a).await;
    let mut sub_b = connect(addr_b).await;
    for client in [&mut sub_a, &mut sub_b] {
        let sub = Envelope::new(
            PeerId::new(),
            MessageKind::Subscribe {
                filter: topic("weather.updates").into(),
                ack: false,
                group: None,
            },
        );
        send(client, &sub).await;
        recv(client).await; // subscribe ack
    }

    // Let interest finish settling across the link before the real
    // assertion below - each of these also proves delivery reaches
    // that subscriber at all.
    publish_until_delivered(addr_a, &mut sub_a, topic("weather.updates"), b"settle-a").await;
    publish_until_delivered(addr_a, &mut sub_b, topic("weather.updates"), b"settle-b").await;

    // A retry above may have landed twice (one attempt genuinely lost
    // to convergence still in progress, a later one delivered) -
    // drain any such stragglers so they can't be mistaken for the
    // real assertion below.
    for client in [&mut sub_a, &mut sub_b] {
        while timeout(Duration::from_millis(50), async_framing::read_frame(client))
            .await
            .is_ok()
        {}
    }

    // The real assertion: one more publish should reach each
    // subscriber exactly once, not bounce back and forth over the
    // link and arrive again.
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"final".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut connect(addr_a).await, &publish).await;

    for client in [&mut sub_a, &mut sub_b] {
        let delivered = recv(client).await;
        assert_eq!(delivered.id, publish.id);
        assert!(
            recv_times_out(client).await,
            "subscriber received the same publish more than once - a loop wasn't prevented"
        );
    }
}

#[tokio::test]
async fn gossip_discovers_a_peer_of_a_peer_and_dials_it() {
    // A - B - C, a chain: A and C are only ever configured with B as
    // a seed peer, never with each other. Gossip (ADR-0015) should
    // teach each of them about the other through B, and one side
    // should auto-dial the other so they end up directly connected
    // too - not just reachable transitively through B.
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = thoth_mesh_node::spawn(listener_a, Vec::new());

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn(listener_b, vec![addr_a.to_string()]);

    let listener_c = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_c = thoth_mesh_node::spawn(listener_c, vec![addr_b.to_string()]);

    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
    eventually(|| node_b.membership.is_reachable(node_c.id)).await;

    eventually(|| node_a.membership.is_reachable(node_c.id)).await;
    eventually(|| node_c.membership.is_reachable(node_a.id)).await;
}

#[tokio::test]
async fn a_full_mesh_bootstrap_converges_despite_the_dial_concurrency_bound() {
    // ADR-0026's worst case, constructed directly: every node
    // configured via `--peer` with every *other* node's address means
    // every possible pairwise dial is attempted at once, right from
    // startup - no gossip needed to trigger it. This only converges if
    // dials queued behind the per-node semaphore actually get their
    // turn rather than starving; a generous timeout well beyond
    // test_support::eventually's, since this scenario spins up far
    // more background dial/handshake work than any other test here.
    const NODE_COUNT: usize = 20;
    const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);

    let mut listeners = Vec::with_capacity(NODE_COUNT);
    let mut addrs = Vec::with_capacity(NODE_COUNT);
    for _ in 0..NODE_COUNT {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(listener.local_addr().unwrap().to_string());
        listeners.push(listener);
    }

    let nodes: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(i, listener)| {
            let seed_peers = addrs
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, addr)| addr.clone())
                .collect();
            thoth_mesh_node::spawn(listener, seed_peers)
        })
        .collect();

    let deadline = tokio::time::Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let converged = nodes.iter().all(|node| {
            nodes
                .iter()
                .all(|other| node.id == other.id || node.membership.is_reachable(other.id))
        });
        if converged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a {NODE_COUNT}-node full-mesh bootstrap did not fully converge within {CONVERGENCE_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn malformed_frame_closes_connection() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;

    // A well-formed frame whose payload isn't a valid CBOR-encoded
    // Envelope at all.
    async_framing::write_frame(&mut client, b"not an envelope")
        .await
        .unwrap();

    // The server should close its side rather than hang or crash;
    // the next read surfaces an error (EOF) instead of timing out.
    let result = timeout(TEST_TIMEOUT, async_framing::read_frame(&mut client)).await;
    assert!(
        matches!(result, Ok(Err(_))),
        "expected the server to close the connection, got {result:?}"
    );
}

#[tokio::test]
async fn status_request_reports_this_nodes_id_and_no_peers_when_alone() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let node = thoth_mesh_node::spawn(listener, Vec::new());

    let mut client = connect(addr).await;
    let request = Envelope::new(PeerId::new(), MessageKind::StatusRequest);
    send(&mut client, &request).await;

    let reply = recv(&mut client).await;
    match reply.kind {
        MessageKind::StatusReply {
            in_reply_to,
            node_id,
            peers,
            ..
        } => {
            assert_eq!(in_reply_to, request.id);
            assert_eq!(node_id, node.id);
            assert!(peers.is_empty());
        }
        other => panic!("expected a StatusReply, got {other:?}"),
    }
}

#[tokio::test]
async fn status_request_reports_a_connected_peer() {
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn(listener_b, Vec::new());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = thoth_mesh_node::spawn(listener_a, vec![addr_b.to_string()]);

    eventually(|| node_a.membership.is_reachable(node_b.id)).await;

    let mut client = connect(addr_a).await;
    let request = Envelope::new(PeerId::new(), MessageKind::StatusRequest);
    send(&mut client, &request).await;

    let reply = recv(&mut client).await;
    match reply.kind {
        MessageKind::StatusReply { node_id, peers, .. } => {
            assert_eq!(node_id, node_a.id);
            assert_eq!(peers.len(), 1);
            assert_eq!(peers[0].peer_id, node_b.id);
            assert_eq!(
                peers[0].listen_addr.as_deref(),
                Some(addr_b.to_string()).as_deref()
            );
        }
        other => panic!("expected a StatusReply, got {other:?}"),
    }
}

#[tokio::test]
async fn status_request_reports_a_metrics_summary_reflecting_activity() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut client, &publish).await;

    // Same connection, right after publishing - the node's dispatch
    // loop processes frames strictly in order (handle_publish awaits
    // the broker before the next frame is read), so the counter this
    // bumps is guaranteed visible by the time this status request is
    // handled, with no polling needed.
    let request = Envelope::new(PeerId::new(), MessageKind::StatusRequest);
    send(&mut client, &request).await;

    let reply = recv(&mut client).await;
    match reply.kind {
        MessageKind::StatusReply { metrics, .. } => {
            assert!(metrics.messages_published >= 1);
        }
        other => panic!("expected a StatusReply, got {other:?}"),
    }
}

/// A node run with `--data-dir` persists every publish to disk, and a
/// fresh node pointed at the same directory rehydrates its replay
/// buffers and retained values from it (ADR-0045) - so a restart is
/// invisible to a subscriber that connects afterward.
#[tokio::test]
async fn a_restarted_node_rehydrates_replay_history_and_retained_values_from_disk() {
    let data_dir = tempfile::tempdir().unwrap();
    let opts = || thoth_mesh_node::NodeOptions {
        data_dir: Some(data_dir.path().to_path_buf()),
        ..Default::default()
    };

    // --- node A: publish a retained value and a plain message ---
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = tokio::spawn(thoth_mesh_node::serve_with_tls(
        listener_a,
        Vec::new(),
        opts(),
    ));

    let mut publisher = connect(addr_a).await;
    let retained = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("sensor.temp"),
            payload: b"21C".to_vec(),
            retain: true,
            content_type: Some("text/plain".to_owned()),
        },
    );
    let plain = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
            retain: false,
            content_type: None,
        },
    );
    send(&mut publisher, &retained).await;
    send(&mut publisher, &plain).await;
    // Round-trip a StatusRequest so both publishes are known to have
    // been processed (and persisted) before the restart.
    send(
        &mut publisher,
        &Envelope::new(PeerId::new(), MessageKind::StatusRequest),
    )
    .await;
    recv(&mut publisher).await;
    drop(publisher);

    // --- stop node A, fully, so it releases the SQLite file ---
    node_a.abort();
    let _ = node_a.await;

    // --- node B: same data dir, brand new listener ---
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let _node_b = tokio::spawn(thoth_mesh_node::serve_with_tls(
        listener_b,
        Vec::new(),
        opts(),
    ));

    let mut subscriber = connect(addr_b).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    // The plain message, published to a node that no longer exists,
    // is replayed from the rehydrated buffer.
    let replayed = recv(&mut subscriber).await;
    assert_eq!(replayed.id, plain.id);

    // And a wildcard subscribe to a fresh pattern (empty replay
    // buffer, ADR-0022) still gets the rehydrated retained value.
    let wild = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: filter("sensor.+"),
            ack: false,
            group: None,
        },
    );
    send(&mut subscriber, &wild).await;
    recv(&mut subscriber).await; // subscribe ack
    let retained_delivery = recv(&mut subscriber).await;
    assert_eq!(retained_delivery.id, retained.id);
    match retained_delivery.kind {
        MessageKind::Publish { content_type, .. } => {
            assert_eq!(content_type, Some("text/plain".to_owned()));
        }
        other => panic!("expected a Publish, got {other:?}"),
    }
}
