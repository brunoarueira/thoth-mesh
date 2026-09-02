//! Chaos/partition scenarios proving ADR-0011 (loop prevention/dedup)
//! and ADR-0012 (reconnect with backoff) hold up under conditions
//! closer to real failure than the one lightweight test each already
//! had. See ADR-0028.

use std::net::SocketAddr;
use std::time::Duration;

use thoth_mesh_core::async_framing;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};
use thoth_mesh_node::test_support::eventually;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

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

fn topic(s: &str) -> Topic {
    s.parse::<Topic>().unwrap()
}

#[tokio::test]
async fn a_restarted_peer_reconnects_as_a_new_identity_with_no_leftover_duplicate() {
    // B dies mid-mesh and comes back - as a *new* identity, since
    // PeerId is a fresh random value per process start (PROTOCOL.md,
    // also the basis for ADR-0025's eviction caps). A's reconnect
    // loop (ADR-0012) must not get permanently stuck waiting for the
    // old identity, and once the new one is up, delivery must resume
    // cleanly - no duplicate left over from before the drop (ADR-0011's
    // dedup, exercised across a restart rather than just a
    // steady-state cycle). See ADR-0028.
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn(listener_b, Vec::new());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = thoth_mesh_node::spawn(listener_a, vec![addr_b.to_string()]);

    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
    eventually(|| node_b.membership.is_reachable(node_a.id)).await;

    // Baseline: a subscriber on A sees a publish made on B, forwarded
    // across the peer link, before anything dies.
    let mut subscriber = connect(addr_a).await;
    let sub = Envelope::new(
        PeerId::new(),
        MessageKind::Subscribe {
            filter: topic("weather.updates").into(),
        },
    );
    send(&mut subscriber, &sub).await;
    recv(&mut subscriber).await; // subscribe ack

    let before = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"before".to_vec(),
        },
    );
    send(&mut connect(addr_b).await, &before).await;
    assert_eq!(recv(&mut subscriber).await.id, before.id);

    // B dies: abort its accept loop (dropping its listening socket -
    // no more *new* connections), its own peer_dials (none here, since
    // it was spawned with no seed peers), and every connection it's
    // already accepted - including the one A dialed in, which
    // accept_loop alone doesn't touch (see Node::accepted_connections).
    // A's connection to it should be noticed on the next read and
    // marked unreachable.
    node_b.accept_loop.abort();
    for handle in &node_b.peer_dials {
        handle.abort();
    }
    for handle in node_b.accepted_connections.lock().unwrap().drain(..) {
        handle.abort();
    }
    eventually(|| !node_a.membership.is_reachable(node_b.id)).await;

    // B "restarts": a fresh listener rebinds the identical address -
    // safe immediately, since a *listening* socket carries no
    // TIME_WAIT of its own (that only applies to individual
    // established connections the local side closed) - and a brand
    // new Node comes up on it, a distinct PeerId by construction,
    // exactly mirroring a real process restart.
    let listener_b_restarted = TcpListener::bind(addr_b).await.unwrap();
    let node_b_restarted = thoth_mesh_node::spawn(listener_b_restarted, Vec::new());
    assert_ne!(
        node_b.id, node_b_restarted.id,
        "a restarted node keeps a fresh identity, never the old one"
    );

    // A's reconnect loop must not be permanently stuck - it should
    // find and connect to the new identity at the same address, while
    // the old identity stays unreachable forever (it's genuinely
    // gone, not coming back).
    eventually(|| node_a.membership.is_reachable(node_b_restarted.id)).await;
    eventually(|| node_b_restarted.membership.is_reachable(node_a.id)).await;
    assert!(!node_a.membership.is_reachable(node_b.id));

    // A publish made on the restarted B is delivered exactly once to
    // the same subscriber that never resubscribed - proving the new
    // peer link both forwards (A's pre-existing interest survived the
    // reconnect via the same catch-up ADR-0011 already gives a fresh
    // link) and doesn't replay or duplicate anything left over from
    // before the drop.
    let after = Envelope::new(
        PeerId::new(),
        MessageKind::Publish {
            topic: topic("weather.updates"),
            payload: b"after".to_vec(),
        },
    );
    send(&mut connect(addr_b).await, &after).await;
    let delivered = recv(&mut subscriber).await;
    assert_eq!(delivered.id, after.id);
    assert!(
        timeout(
            Duration::from_millis(200),
            async_framing::read_frame(&mut subscriber)
        )
        .await
        .is_err(),
        "subscriber received more than one delivery for the post-restart publish"
    );
}
