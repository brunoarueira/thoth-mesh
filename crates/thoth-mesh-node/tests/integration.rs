use std::net::SocketAddr;
use std::time::Duration;

use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};
use thoth_mesh_node::test_support::eventually;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

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
