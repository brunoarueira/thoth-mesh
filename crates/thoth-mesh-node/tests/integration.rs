use std::net::SocketAddr;
use std::time::Duration;

use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};
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
            topic: topic("weather.updates"),
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
            topic: topic("weather.updates"),
        },
    );
    send(&mut client, &sub).await;
    recv(&mut client).await; // subscribe ack

    let unsub = Envelope::new(
        PeerId::new(),
        MessageKind::Unsubscribe {
            topic: topic("weather.updates"),
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
            topic: topic("weather.updates"),
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

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
    assert_eq!(delivered.kind, publish.kind);
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
                topic: topic("weather.updates"),
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
        },
    );
    send(&mut publisher, &publish).await;

    assert_eq!(recv(&mut sub_a).await.id, publish.id);
    assert_eq!(recv(&mut sub_b).await.id, publish.id);
}

#[tokio::test]
async fn unsubscribed_client_does_not_receive_publish() {
    let addr = spawn_test_node().await;
    let mut client = connect(addr).await;
    let mut publisher = connect(addr).await;

    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut client, &sub).await;
    recv(&mut client).await; // subscribe ack

    let unsub = Envelope::new(
        PeerId::new(),
        MessageKind::Unsubscribe {
            topic: topic("weather.updates"),
        },
    );
    send(&mut client, &unsub).await;
    recv(&mut client).await; // unsubscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
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
            topic: topic("weather.updates"),
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("traffic.updates"),
            payload: b"jam".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    assert!(recv_times_out(&mut subscriber).await);
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
            topic: topic("weather.updates"),
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
            topic: topic("weather.updates")
        }
    );

    // An ordinary client publishes on node A directly.
    let mut publisher = connect(addr_a).await;
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"sunny".to_vec(),
        },
    );
    send(&mut publisher, &publish).await;

    // It should be forwarded down the dialed peer link.
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
            topic: topic("weather.updates"),
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
        }
    );
}

#[tokio::test]
async fn loop_prevention_stops_duplicate_delivery_on_a_triangle_mesh() {
    // Three nodes, fully peered (A-B, B-C, C-A) - more than one path
    // between any two of them. Without the MessageId dedup in
    // Broker::publish (ADR-0011), a publish forwarded around the
    // cycle would keep echoing between peers, and each subscriber
    // would see it delivered more than once.
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = thoth_mesh_node::spawn(listener_a, Vec::new());

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn(listener_b, vec![addr_a.to_string()]);

    let listener_c = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_c = listener_c.local_addr().unwrap();
    let node_c = thoth_mesh_node::spawn(listener_c, vec![addr_a.to_string(), addr_b.to_string()]);

    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
    eventually(|| node_a.membership.is_reachable(node_c.id)).await;
    eventually(|| node_b.membership.is_reachable(node_c.id)).await;

    let mut sub_a = connect(addr_a).await;
    let mut sub_b = connect(addr_b).await;
    let mut sub_c = connect(addr_c).await;
    for client in [&mut sub_a, &mut sub_b, &mut sub_c] {
        let sub = Envelope::new(
            PeerId::new(),
            MessageKind::Subscribe {
                topic: topic("weather.updates"),
            },
        );
        send(client, &sub).await;
        recv(client).await; // subscribe ack
    }

    // Let interest finish settling across the full mesh before the
    // real assertion below - each of these also proves delivery
    // reaches that subscriber at all.
    publish_until_delivered(addr_a, &mut sub_a, topic("weather.updates"), b"settle-a").await;
    publish_until_delivered(addr_a, &mut sub_b, topic("weather.updates"), b"settle-b").await;
    publish_until_delivered(addr_a, &mut sub_c, topic("weather.updates"), b"settle-c").await;

    // A retry above may have landed twice (one attempt genuinely lost
    // to convergence still in progress, a later one delivered) -
    // drain any such stragglers so they can't be mistaken for the
    // real assertion below.
    for client in [&mut sub_a, &mut sub_b, &mut sub_c] {
        while timeout(Duration::from_millis(50), async_framing::read_frame(client))
            .await
            .is_ok()
        {}
    }

    // The real assertion: one more publish should reach each
    // subscriber exactly once, not bounce around the triangle and
    // arrive again.
    let publish = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"final".to_vec(),
        },
    );
    send(&mut connect(addr_a).await, &publish).await;

    for client in [&mut sub_a, &mut sub_b, &mut sub_c] {
        let delivered = recv(client).await;
        assert_eq!(delivered.id, publish.id);
        assert!(
            recv_times_out(client).await,
            "subscriber received the same publish more than once - a loop wasn't prevented"
        );
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
