//! Outbound connection management: dials configured seed peers,
//! performs the handshake, and hands the connection off to the same
//! broker-wired dispatch the accept side uses. See ADR-0009 and
//! ADR-0010. A seed peer that's unreachable, or whose link later
//! drops, is retried with exponential backoff rather than abandoned;
//! see ADR-0012.

use std::time::Duration;

use thoth_mesh::dial_handshake;
use thoth_mesh_tls::MaybeTlsStream;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::Instrument;

use crate::connection;
use crate::shared::Shared;

/// Delay before the first reconnect attempt after a dial fails or a
/// link drops, and the delay a healthy attempt resets back to.
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);

/// Upper bound the backoff delay doubles toward but never exceeds.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// How long a dialed connection has to stay up before it counts as
/// evidence the peer is actually reachable, resetting the backoff
/// delay instead of doubling it.
const MIN_HEALTHY_DURATION: Duration = Duration::from_secs(5);

/// Spawns one background task per entry in `seed_peers`, each dialing
/// that address, handshaking, routing messages over the connection
/// exactly as an accepted one would, and redialing with backoff if
/// the attempt fails or the link later drops (see ADR-0012). Returns
/// the tasks' handles so tests can sever a link on demand; ordinary
/// callers can drop them.
pub fn spawn_seed_peers(seed_peers: Vec<String>, shared: Shared) -> Vec<JoinHandle<()>> {
    seed_peers
        .into_iter()
        .map(|peer_addr| tokio::spawn(dial_peer_with_reconnect(peer_addr, shared.clone())))
        .collect()
}

/// Spawns one background task per address received on
/// `discovered_rx` - each an ordinary [`dial_peer_with_reconnect`]
/// loop, the same as a configured seed peer gets, just triggered by
/// gossip instead of `--peer` (see ADR-0015). Runs until
/// `discovered_rx`'s sender (`Shared::discovered_tx`) is dropped,
/// which only happens when every clone of `Shared` holding it does -
/// i.e. never, for a live node.
pub async fn spawn_discovery_dialer(
    mut discovered_rx: mpsc::UnboundedReceiver<String>,
    shared: Shared,
) {
    while let Some(peer_addr) = discovered_rx.recv().await {
        tracing::info!(addr = %peer_addr, "dialing peer discovered via gossip");
        tokio::spawn(dial_peer_with_reconnect(peer_addr, shared.clone()));
    }
}

/// Wraps [`dial_peer`] in an unending retry loop: a seed peer is
/// standing configuration, not a one-off dial, so there's no point at
/// which giving up is more correct than continuing to retry. The
/// delay between attempts is [`next_backoff`], driven by how long the
/// previous attempt's connection stayed up. See ADR-0012.
async fn dial_peer_with_reconnect(peer_addr: String, shared: Shared) {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let attempt_start = Instant::now();
        dial_peer(peer_addr.clone(), shared.clone()).await;

        backoff = next_backoff(backoff, attempt_start.elapsed());
        tracing::info!(addr = %peer_addr, delay = ?backoff, "seed peer link down, will retry");
        tokio::time::sleep(backoff).await;
    }
}

/// The backoff delay to use for the *next* dial attempt, given the
/// delay used for the attempt that just ended and how long its
/// connection stayed up.
///
/// A connection that stayed up for at least [`MIN_HEALTHY_DURATION`]
/// is treated as proof the peer is reachable, resetting back to
/// [`INITIAL_BACKOFF`] rather than compounding a delay that no longer
/// reflects reality. Anything shorter - including a connect or
/// handshake failure, which returns near-instantly - doubles the
/// previous delay, capped at [`MAX_BACKOFF`].
fn next_backoff(previous: Duration, connection_uptime: Duration) -> Duration {
    if connection_uptime >= MIN_HEALTHY_DURATION {
        INITIAL_BACKOFF
    } else {
        (previous * 2).min(MAX_BACKOFF)
    }
}

async fn dial_peer(peer_addr: String, shared: Shared) {
    let span = tracing::info_span!("peer", addr = %peer_addr);
    async move {
        // Bounds how many connect+handshake attempts run at once,
        // across both seed peers and gossip-discovered ones (ADR-0026).
        // Held only for the connecting phase, never for the connection's
        // established lifetime - dropped explicitly below, before the
        // handoff to handle_connection.
        let permit = shared
            .dial_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("dial semaphore is never closed");

        let tcp = match TcpStream::connect(&peer_addr).await {
            Ok(stream) => stream,
            Err(err) => {
                tracing::warn!(%err, "failed to connect to seed peer");
                return;
            }
        };
        // A peer dialing another peer always identifies itself - the
        // same cert/key this node uses to identify itself on accept
        // - and always verifies the far end's server cert (ADR-0016).
        let stream = match &shared.tls_connector {
            Some(connector) => match MaybeTlsStream::connect(connector, tcp, &peer_addr).await {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::warn!(%err, "TLS handshake with seed peer failed");
                    return;
                }
            },
            None => MaybeTlsStream::Plain(tcp),
        };
        let mut conn = stream.compat();

        let info =
            match dial_handshake(&mut conn, shared.node_id, shared.my_listen_addr.clone()).await {
                Ok(info) => info,
                Err(err) => {
                    tracing::warn!(%err, "handshake with seed peer failed");
                    return;
                }
            };
        tracing::info!(
            peer_id = ?info.peer_id,
            peer_listen_addr = ?info.listen_addr,
            "connected to seed peer"
        );

        // The attempt has succeeded through to a completed handshake -
        // release the permit before handing off to handle_connection,
        // which runs for the connection's entire lifetime and must not
        // hold a dial slot for that long (ADR-0026).
        drop(permit);

        // Recover the raw stream (no data loss - Compat adds no
        // buffering of its own) and hand off to the same dispatch
        // loop the accept side uses, with the peer identity we
        // already know from the handshake (see ADR-0010).
        // handle_connection marks the peer connected and registers
        // its link together, right as the dispatch loop starts (see
        // ADR-0011) - not here, so the two can't race apart.
        let stream = conn.into_inner();
        connection::handle_connection(stream, shared, Some(info)).await;
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use thoth_mesh_core::{Envelope, MessageKind, PeerId, async_framing};
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    use super::*;
    use crate::test_support::eventually;

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn dial_peer_sends_hello_and_tracks_membership() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Shared::new(PeerId::new(), Some("127.0.0.1:49500".to_owned()));

        tokio::spawn(dial_peer(addr.to_string(), shared.clone()));

        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("timed out waiting for the dial")
            .unwrap();
        let mut conn = socket.compat();

        let bytes = timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn))
            .await
            .expect("timed out waiting for the Hello")
            .unwrap();
        let hello = Envelope::from_bytes(&bytes).unwrap();
        assert_eq!(hello.sender, shared.node_id);
        assert_eq!(
            hello.kind,
            MessageKind::Hello {
                listen_addr: Some("127.0.0.1:49500".to_owned())
            }
        );

        // Reply with our own Hello so dial_handshake completes rather
        // than hanging.
        let their_id = PeerId::new();
        let reply = Envelope::new(their_id, MessageKind::Hello { listen_addr: None });
        async_framing::write_frame(&mut conn, &reply.to_bytes().unwrap())
            .await
            .unwrap();

        eventually(|| shared.membership.is_reachable(their_id)).await;

        // Closing our side should be noticed and reflected in
        // membership too.
        drop(conn);
        eventually(|| !shared.membership.is_reachable(their_id)).await;
    }

    #[tokio::test]
    async fn dial_peer_forwards_broker_publishes_once_the_peer_subscribes() {
        // Once the handshake completes, dial_peer hands off to the
        // same dispatch loop the accept side uses (ADR-0010) - a
        // Subscribe from the far end should get a working forwarder
        // against the same broker instance bundled into the Shared we
        // hand dial_peer here.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Shared::new(PeerId::new(), None);
        let broker = shared.broker.clone();

        tokio::spawn(dial_peer(addr.to_string(), shared));

        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("timed out waiting for the dial")
            .unwrap();
        let mut conn = socket.compat();

        // Complete the handshake.
        timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn))
            .await
            .expect("timed out waiting for the Hello")
            .unwrap();
        let their_id = PeerId::new();
        let hello_reply = Envelope::new(their_id, MessageKind::Hello { listen_addr: None });
        async_framing::write_frame(&mut conn, &hello_reply.to_bytes().unwrap())
            .await
            .unwrap();

        // Subscribe over the now-established peer link.
        let topic: thoth_mesh_core::Topic = "weather.updates".parse().unwrap();
        let filter: thoth_mesh_core::TopicFilter = topic.clone().into();
        let sub = Envelope::new(
            their_id,
            MessageKind::Subscribe {
                filter: filter.clone(),
            },
        );
        async_framing::write_frame(&mut conn, &sub.to_bytes().unwrap())
            .await
            .unwrap();
        let ack = timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn))
            .await
            .expect("timed out waiting for the subscribe ack")
            .unwrap();
        assert_eq!(
            Envelope::from_bytes(&ack).unwrap().kind,
            MessageKind::Ack {
                in_reply_to: sub.id
            }
        );

        // This subscribe was this node's first interest in the topic,
        // so it also gets echoed straight back down every peer link,
        // including this one (ADR-0011) - drain that before looking
        // for the forwarded publish.
        let echoed = timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn))
            .await
            .expect("timed out waiting for the interest echo")
            .unwrap();
        assert_eq!(
            Envelope::from_bytes(&echoed).unwrap().kind,
            MessageKind::Subscribe {
                filter: filter.clone()
            }
        );

        // A publish on the broker dial_peer was handed - as if from a
        // local client - should now be forwarded down the peer link.
        let publish = Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: topic.clone(),
                payload: b"sunny".to_vec(),
            },
        );
        broker
            .publish(&topic, std::sync::Arc::new(publish.clone()))
            .await;

        let delivered = timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn))
            .await
            .expect("timed out waiting for the forwarded publish")
            .unwrap();
        assert_eq!(Envelope::from_bytes(&delivered).unwrap().id, publish.id);
    }

    #[tokio::test]
    async fn dial_peer_logs_and_returns_when_the_connection_is_refused() {
        // Nothing is listening on this address - the dial itself
        // should fail fast rather than hang or panic. Exercised
        // mainly so this branch isn't silently untested; success is
        // just returning promptly.
        let unused_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = unused_listener.local_addr().unwrap();
        drop(unused_listener);

        let shared = Shared::new(PeerId::new(), None);
        timeout(TEST_TIMEOUT, dial_peer(addr.to_string(), shared))
            .await
            .expect("dial_peer should return promptly on connection refused");
    }

    #[tokio::test]
    async fn dial_peer_catches_the_far_end_up_on_existing_interest() {
        // If our node already has local interest in a topic before
        // this peer link comes up, the peer should be told about it
        // as part of connecting, not just future transitions (ADR-0011).
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Shared::new(PeerId::new(), None);
        let topic: thoth_mesh_core::Topic = "weather.updates".parse().unwrap();
        let filter: thoth_mesh_core::TopicFilter = topic.into();
        shared.interest.subscribe(filter.clone());

        tokio::spawn(dial_peer(addr.to_string(), shared));

        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("timed out waiting for the dial")
            .unwrap();
        let mut conn = socket.compat();

        // Complete the handshake.
        timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn))
            .await
            .expect("timed out waiting for the Hello")
            .unwrap();
        let reply = Envelope::new(PeerId::new(), MessageKind::Hello { listen_addr: None });
        async_framing::write_frame(&mut conn, &reply.to_bytes().unwrap())
            .await
            .unwrap();

        // The catch-up Subscribe should follow, unprompted.
        let bytes = timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn))
            .await
            .expect("timed out waiting for the interest catch-up")
            .unwrap();
        assert_eq!(
            Envelope::from_bytes(&bytes).unwrap().kind,
            MessageKind::Subscribe { filter }
        );
    }

    #[test]
    fn next_backoff_doubles_after_a_connection_that_dropped_quickly() {
        assert_eq!(
            next_backoff(Duration::from_millis(500), Duration::ZERO),
            Duration::from_secs(1)
        );
        assert_eq!(
            next_backoff(Duration::from_secs(1), Duration::from_millis(100)),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn next_backoff_caps_at_the_maximum() {
        assert_eq!(
            next_backoff(Duration::from_secs(20), Duration::ZERO),
            MAX_BACKOFF
        );
        assert_eq!(next_backoff(MAX_BACKOFF, Duration::ZERO), MAX_BACKOFF);
    }

    #[test]
    fn next_backoff_resets_after_a_connection_that_stayed_up_long_enough() {
        // A connection that ran for a while before dropping is
        // evidence the peer is reachable now, not just that the
        // handshake happened to complete right before an immediate
        // drop (ADR-0012) - the next delay should start over from
        // INITIAL_BACKOFF rather than keep compounding.
        assert_eq!(
            next_backoff(Duration::from_secs(16), MIN_HEALTHY_DURATION),
            INITIAL_BACKOFF
        );
        assert_eq!(
            next_backoff(MAX_BACKOFF, MIN_HEALTHY_DURATION + Duration::from_secs(1)),
            INITIAL_BACKOFF
        );
    }

    #[tokio::test]
    async fn dial_peer_with_reconnect_retries_until_the_seed_peer_comes_up() {
        // Reserve an address but don't accept on it yet - the first
        // dial attempt should fail, and the retry loop should try
        // again once its backoff delay elapses rather than giving up
        // (ADR-0012).
        let placeholder = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = placeholder.local_addr().unwrap();
        drop(placeholder);

        let shared = Shared::new(PeerId::new(), None);
        let reconnect_task = tokio::spawn(dial_peer_with_reconnect(addr.to_string(), shared));

        // Give the first (failing) attempt and its backoff sleep room
        // to play out before the peer becomes reachable.
        tokio::time::sleep(INITIAL_BACKOFF + Duration::from_millis(200)).await;
        let listener = TcpListener::bind(addr).await.unwrap();

        timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("retry loop never redialed the seed peer once it came up")
            .unwrap();

        reconnect_task.abort();
    }
}
