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

/// Like `test_support::eventually`, but with a caller-chosen timeout
/// instead of that helper's fixed 2s - for a wait that has to span at
/// least one `peering::next_backoff`-driven retry delay, whose actual
/// wall-clock timing (not just the nominal 500ms/1s/2s.. schedule) can
/// stretch further than 2s under CI's typically slower, more
/// contended scheduling than a local run.
async fn eventually_within(timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition was not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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

#[tokio::test]
async fn a_partition_heals_and_both_sides_reconverge() {
    // A partition, not a death: B stays up throughout - its identity,
    // accept_loop, and (were there any) its own peer_dials are all
    // untouched. Only the one link between A and B is severed, and
    // from B's side specifically, so A's own dial_peer_with_reconnect
    // loop (ADR-0012) is never itself touched either - it has to
    // notice the drop and redial entirely on its own, the exact
    // mechanism this scenario exists to prove. Unlike scenario 1's
    // restart, the identity on both sides is the same throughout. See
    // ADR-0028.
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn(listener_b, Vec::new());

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_a = thoth_mesh_node::spawn(listener_a, vec![addr_b.to_string()]);

    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
    eventually(|| node_b.membership.is_reachable(node_a.id)).await;

    // Sever B's side of the link only - B's accept_loop, listening for
    // A's next dial attempt, is left running untouched. (B's own
    // membership view of A may lag briefly here, since aborting skips
    // B's normal disconnect cleanup for this one connection - it's
    // corrected the moment A's redial lands a fresh accepted
    // connection below, which is what this test actually asserts.)
    for handle in node_b.accepted_connections.lock().unwrap().drain(..) {
        handle.abort();
    }

    // A observes this the same way any dropped connection is noticed
    // - a normal read failure inside handle_connection, returning
    // control to the still-alive dial_peer_with_reconnect loop that's
    // been running around it the whole time.
    eventually(|| !node_a.membership.is_reachable(node_b.id)).await;

    // The partition heals on its own: A's retry loop redials the same
    // address, and B - still up, still listening - accepts it as a
    // fresh connection. Both sides converge back to the same reachable
    // view they had before the partition, the same two identities
    // throughout.
    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
    eventually(|| node_b.membership.is_reachable(node_a.id)).await;
}

#[tokio::test]
async fn a_gossip_discovered_peer_that_is_down_keeps_retrying_until_it_comes_up() {
    // A peer can be *announced* (ADR-0015) without ever being
    // reachable - up when the announcer learned it, but down,
    // unreachable, or gone by the time this node tries to dial it.
    // Auto-dial reuses dial_peer_with_reconnect verbatim, so this
    // should degrade to the same backoff/retry a configured --peer
    // that never comes up already gets (`dial_peer_with_reconnect_
    // retries_until_the_seed_peer_comes_up` in peering.rs) - proven
    // here rather than left as an assumption from code reuse. See
    // ADR-0028.
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let node_a = thoth_mesh_node::spawn(listener_a, Vec::new());

    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let node_b = thoth_mesh_node::spawn(listener_b, vec![addr_a.to_string()]);

    eventually(|| node_a.membership.is_reachable(node_b.id)).await;
    eventually(|| node_b.membership.is_reachable(node_a.id)).await;

    // Reserve a real address for "C" without anything actually
    // listening on it yet - genuinely unreachable, not hypothetical.
    let listener_c = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_c = listener_c.local_addr().unwrap();
    drop(listener_c);

    // A raw connection stands in for C dialing into B just long enough
    // to say Hello and claim addr_c as its listen address - exactly
    // what a real, momentarily-reachable C would have sent - before
    // promptly disappearing again. B records and propagates it via the
    // ordinary announce path (ADR-0015) before ever noticing the
    // disconnect, so this reaches A exactly as a genuine gossiped
    // address would.
    let mut fake_c = connect(addr_b).await;
    // we_should_dial compares PeerIds to pick exactly one side to
    // auto-dial a newly-learned peer - keep generating until it picks
    // A, so this test deterministically exercises A's retry loop
    // rather than leaving that to chance.
    let peer_id_c = loop {
        let candidate = PeerId::new();
        if node_a.id < candidate {
            break candidate;
        }
    };
    let hello = Envelope::new(
        peer_id_c,
        MessageKind::Hello {
            listen_addr: Some(addr_c.to_string()),
        },
    );
    send(&mut fake_c, &hello).await;
    recv(&mut fake_c).await; // B's own Hello reply
    drop(fake_c);

    // A has now learned about C via gossip through B and should be
    // auto-dialing addr_c - and retrying with backoff rather than
    // giving up, since nothing is listening there yet. There's no
    // direct signal to poll for "still retrying"; this and the next
    // step prove it together - if the retry loop had given up instead
    // of backing off forever, C coming up after this delay would never
    // be discovered.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // C comes up for real, at the exact address that was gossiped -
    // A's retry loop, never itself touched, finds it on a subsequent
    // attempt. This depends on at least one backoff delay actually
    // elapsing (peering::next_backoff, not exported here) on top of
    // however long CI's scheduling happens to add on a given run, so
    // it gets a much more generous timeout than the default
    // `eventually` - a real observed CI flake at the default 2s, not
    // a hypothetical margin.
    let listener_c_real = TcpListener::bind(addr_c).await.unwrap();
    let node_c = thoth_mesh_node::spawn(listener_c_real, Vec::new());

    eventually_within(Duration::from_secs(15), || {
        node_a.membership.is_reachable(node_c.id)
    })
    .await;
    eventually_within(Duration::from_secs(15), || {
        node_c.membership.is_reachable(node_a.id)
    })
    .await;
}
