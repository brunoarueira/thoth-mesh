//! Outbound connection management: dials configured seed peers,
//! performs the handshake, and hands the connection off to the same
//! broker-wired dispatch the accept side uses. See ADR-0009 and
//! ADR-0010. A seed peer that's unreachable, or whose link later
//! drops, is retried with exponential backoff rather than abandoned;
//! see ADR-0012.

use std::time::Duration;

use thoth_mesh::{HandshakeError, PeerInfo, dial_handshake};
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

/// How long the connect+TLS+handshake phase of a dial attempt may run
/// before it's abandoned - generous for a real network path, but well
/// short of the OS's own TCP connect timeout, so this is the one that
/// actually fires. Bounds how long a stalled peer can hold one of
/// [`crate::shared::DEFAULT_MAX_CONCURRENT_DIALS`]'s dial slots - see
/// ADR-0027.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// The fixed delay before retrying a dial attempt the peer explicitly
/// rejected (an `Error` reply to our `Hello`, see [`DialOutcome`]) -
/// used instead of the normal exponential ramp, every consecutive
/// rejection, never doubling further or resetting based on timing. A
/// rejection is a *known* state, not "maybe still flaky": only an
/// operator fixing the far end's policy and reloading it can change
/// the outcome, a timescale of minutes at the least, so there's
/// nothing to gain from still ramping through sub-second and
/// single-digit-second delays on the way here. See ADR-0061.
const REJECTED_BACKOFF: Duration = Duration::from_secs(120);

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
        let outcome = dial_peer(peer_addr.clone(), shared.clone()).await;

        backoff = next_backoff(backoff, attempt_start.elapsed(), outcome);
        tracing::info!(addr = %peer_addr, delay = ?backoff, "seed peer link down, will retry");
        tokio::time::sleep(backoff).await;
    }
}

/// What one [`dial_peer`] attempt ended on - orthogonal to how long it
/// took (that's `attempt_start.elapsed()`, timed by the caller per
/// ADR-0012): whether a *failed* attempt was an explicit policy
/// rejection or anything else. See ADR-0061.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DialOutcome {
    /// A connection was established - by the time this is returned,
    /// it has already ended too, since `handle_connection` doesn't
    /// return until it does.
    Established,
    /// The peer explicitly rejected us - an `Error` reply to our
    /// `Hello`, a deliberate decision, not a transient problem.
    Rejected,
    /// Anything else: refused, TLS failed, timed out, a confusing
    /// non-`Error` reply. Already logged at the point it happened.
    Other,
}

/// The backoff delay to use for the *next* dial attempt, given the
/// delay used for the attempt that just ended, how long its
/// connection stayed up, and how that attempt ended.
///
/// A connection that stayed up for at least [`MIN_HEALTHY_DURATION`]
/// is treated as proof the peer is reachable, resetting back to
/// [`INITIAL_BACKOFF`] rather than compounding a delay that no longer
/// reflects reality. An explicit rejection always waits
/// [`REJECTED_BACKOFF`], regardless of `previous` - see that
/// constant's own doc comment. Anything else that fell short of
/// [`MIN_HEALTHY_DURATION`] - including a connect or TLS failure,
/// which returns near-instantly - doubles the previous delay, capped
/// at [`MAX_BACKOFF`].
fn next_backoff(previous: Duration, connection_uptime: Duration, outcome: DialOutcome) -> Duration {
    if connection_uptime >= MIN_HEALTHY_DURATION {
        INITIAL_BACKOFF
    } else if outcome == DialOutcome::Rejected {
        REJECTED_BACKOFF
    } else {
        (previous * 2).min(MAX_BACKOFF)
    }
}

async fn dial_peer(peer_addr: String, shared: Shared) -> DialOutcome {
    dial_peer_with_timeout(peer_addr, shared, DIAL_TIMEOUT).await
}

/// [`dial_peer`], with the connect+handshake budget as a parameter
/// rather than always [`DIAL_TIMEOUT`] - exists so a test can prove
/// the timeout actually fires using a short *real* duration, rather
/// than needing `tokio::time::pause`/`advance` paired with a genuine
/// socket connect on the same runtime, a combination that reliably
/// deadlocks (see the timeout test below - discovered while adding
/// it, not a hypothetical). `dial_peer` itself always uses the real
/// constant; this is never reachable from anything other than a test.
async fn dial_peer_with_timeout(
    peer_addr: String,
    shared: Shared,
    dial_timeout: Duration,
) -> DialOutcome {
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

        // Bounded so a peer that's merely stalled - accepts the TCP
        // connection but never finishes TLS or the handshake - can't
        // hold this permit (and thus one of the node's limited dial
        // slots) forever. See ADR-0027.
        let established =
            match tokio::time::timeout(dial_timeout, connect_and_handshake(&peer_addr, &shared))
                .await
            {
                Ok(Ok(established)) => established,
                // connect/TLS/handshake failed; already logged below.
                Ok(Err(DialFailure::Rejected)) => return DialOutcome::Rejected,
                Ok(Err(DialFailure::Other)) => return DialOutcome::Other,
                Err(_) => {
                    tracing::warn!(timeout = ?dial_timeout, "dial to seed peer timed out");
                    return DialOutcome::Other;
                }
            };
        tracing::info!(
            peer_id = ?established.info.peer_id,
            peer_listen_addr = ?established.info.listen_addr,
            "connected to seed peer"
        );

        // The attempt has succeeded through to a completed handshake -
        // release the permit before handing off to handle_connection,
        // which runs for the connection's entire lifetime and must not
        // hold a dial slot for that long (ADR-0026).
        drop(permit);

        // handle_connection marks the peer connected and registers
        // its link together, right as the dispatch loop starts (see
        // ADR-0011) - not here, so the two can't race apart.
        connection::handle_connection(established.stream, shared, Some(established.info)).await;
        DialOutcome::Established
    }
    .instrument(span)
    .await
}

/// The result of a dial attempt that made it all the way through the
/// handshake: the raw stream, ready to hand off to
/// [`connection::handle_connection`], and the peer identity learned
/// from the handshake.
struct EstablishedDial {
    stream: MaybeTlsStream,
    info: PeerInfo,
}

/// Why [`connect_and_handshake`] failed - [`DialOutcome`]'s own
/// precursor, before a timeout (handled one layer up, in
/// [`dial_peer_with_timeout`]) gets folded into the same enum.
enum DialFailure {
    Rejected,
    Other,
}

/// The connect+TLS+handshake phase of a dial attempt, factored out of
/// [`dial_peer`] so it can be wrapped in a single timeout (ADR-0027)
/// rather than one per step. Returns `Err` on any failure - already
/// logged via `tracing::warn!` at the point it happened, so the caller
/// doesn't need to log again, just decide what to do next based on
/// which kind it was (`dial_peer_with_reconnect`'s backoff loop, see
/// ADR-0061).
async fn connect_and_handshake(
    peer_addr: &str,
    shared: &Shared,
) -> Result<EstablishedDial, DialFailure> {
    let tcp = match TcpStream::connect(peer_addr).await {
        Ok(stream) => stream,
        Err(err) => {
            tracing::warn!(%err, "failed to connect to seed peer");
            return Err(DialFailure::Other);
        }
    };
    // A peer dialing another peer always identifies itself - the
    // same cert/key this node uses to identify itself on accept
    // - and always verifies the far end's server cert (ADR-0016).
    let stream = match &shared.tls_connector {
        Some(connector) => match MaybeTlsStream::connect(connector, tcp, peer_addr).await {
            Ok(stream) => stream,
            Err(err) => {
                tracing::warn!(%err, "TLS handshake with seed peer failed");
                return Err(DialFailure::Other);
            }
        },
        None => MaybeTlsStream::Plain(tcp),
    };
    let mut conn = stream.compat();

    let info = match dial_handshake(&mut conn, shared.node_id, shared.my_listen_addr.clone()).await
    {
        Ok(info) => info,
        Err(HandshakeError::Rejected(message)) => {
            tracing::warn!("seed peer explicitly rejected this connection: {message}");
            return Err(DialFailure::Rejected);
        }
        Err(err) => {
            tracing::warn!(%err, "handshake with seed peer failed");
            return Err(DialFailure::Other);
        }
    };

    // Recover the raw stream - no data loss, Compat adds no buffering
    // of its own.
    Ok(EstablishedDial {
        stream: conn.into_inner(),
        info,
    })
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
                ack: false,
                group: None,
                durable: false,
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
                filter: filter.clone(),
                ack: false,
                group: None,
                durable: false,
            }
        );

        // A publish on the broker dial_peer was handed - as if from a
        // local client - should now be forwarded down the peer link.
        let publish = Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: topic.clone(),
                payload: b"sunny".to_vec(),
                retain: false,
                content_type: None,
                reply_to: None,
                in_reply_to: None,
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
        let outcome = timeout(TEST_TIMEOUT, dial_peer(addr.to_string(), shared))
            .await
            .expect("dial_peer should return promptly on connection refused");
        assert_eq!(outcome, DialOutcome::Other);
    }

    #[tokio::test]
    async fn dial_peer_times_out_if_the_handshake_never_completes() {
        // The listener accepts the TCP connection - so the dial gets
        // past connect() - but never sends a Hello reply, so
        // dial_handshake stalls waiting for one indefinitely. Without
        // ADR-0027's timeout, this attempt (and the dial slot it
        // holds) would hang forever.
        //
        // Uses dial_peer_with_timeout with a short *real* duration,
        // deliberately not tokio::time::pause/advance: pausing virtual
        // time on the same runtime that's also doing a genuine socket
        // connect() reliably deadlocked in that connect - confirmed by
        // instrumenting it while writing this test, not a hypothetical
        // worry. A short real timeout exercises the identical
        // timeout/permit-release logic without that hazard.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Shared::new(PeerId::new(), None);
        let dial_timeout = Duration::from_millis(100);

        let dial_task = tokio::spawn(dial_peer_with_timeout(
            addr.to_string(),
            shared,
            dial_timeout,
        ));

        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("timed out waiting for the dial")
            .unwrap();
        let mut conn = socket.compat();
        // Read (and discard) the Hello our dial sends, so we know
        // it's genuinely stalled waiting for a reply, not just slow
        // to connect.
        timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn))
            .await
            .expect("timed out waiting for the Hello")
            .unwrap();

        // Never reply - dial_peer_with_timeout's own timeout should
        // give up on its own well within TEST_TIMEOUT.
        let outcome = timeout(TEST_TIMEOUT, dial_task)
            .await
            .expect("dial_peer_with_timeout should return once its timeout elapses")
            .expect("dial_peer_with_timeout task should not panic");
        assert_eq!(outcome, DialOutcome::Other);
    }

    #[tokio::test]
    async fn dial_peer_is_rejected_when_the_far_end_replies_with_an_error() {
        // The accepting side already does this for an --allow-peer
        // rejection (ADR-0017); this test stands in for it without a
        // real allowlist, to isolate dial_peer's own classification
        // of the reply. See ADR-0061.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Shared::new(PeerId::new(), None);

        let dial_task = tokio::spawn(dial_peer(addr.to_string(), shared));

        let (socket, _) = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("timed out waiting for the dial")
            .unwrap();
        let mut conn = socket.compat();
        let hello = timeout(TEST_TIMEOUT, async_framing::read_frame(&mut conn))
            .await
            .expect("timed out waiting for the Hello")
            .unwrap();
        let hello_id = Envelope::from_bytes(&hello).unwrap().id;

        let rejection = Envelope::new(
            PeerId::new(),
            MessageKind::Error {
                in_reply_to: Some(hello_id),
                message: "not on the allowlist".to_owned(),
            },
        );
        async_framing::write_frame(&mut conn, &rejection.to_bytes().unwrap())
            .await
            .unwrap();

        let outcome = timeout(TEST_TIMEOUT, dial_task)
            .await
            .expect("dial_peer should return promptly once rejected")
            .expect("dial_peer task should not panic");
        assert_eq!(outcome, DialOutcome::Rejected);
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
            MessageKind::Subscribe {
                filter,
                ack: false,
                group: None,
                durable: false,
            }
        );
    }

    #[test]
    fn next_backoff_doubles_after_a_connection_that_dropped_quickly() {
        assert_eq!(
            next_backoff(
                Duration::from_millis(500),
                Duration::ZERO,
                DialOutcome::Other
            ),
            Duration::from_secs(1)
        );
        assert_eq!(
            next_backoff(
                Duration::from_secs(1),
                Duration::from_millis(100),
                DialOutcome::Other
            ),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn next_backoff_caps_at_the_maximum() {
        assert_eq!(
            next_backoff(Duration::from_secs(20), Duration::ZERO, DialOutcome::Other),
            MAX_BACKOFF
        );
        assert_eq!(
            next_backoff(MAX_BACKOFF, Duration::ZERO, DialOutcome::Other),
            MAX_BACKOFF
        );
    }

    #[test]
    fn next_backoff_resets_after_a_connection_that_stayed_up_long_enough() {
        // A connection that ran for a while before dropping is
        // evidence the peer is reachable now, not just that the
        // handshake happened to complete right before an immediate
        // drop (ADR-0012) - the next delay should start over from
        // INITIAL_BACKOFF rather than keep compounding.
        assert_eq!(
            next_backoff(
                Duration::from_secs(16),
                MIN_HEALTHY_DURATION,
                DialOutcome::Established
            ),
            INITIAL_BACKOFF
        );
        assert_eq!(
            next_backoff(
                MAX_BACKOFF,
                MIN_HEALTHY_DURATION + Duration::from_secs(1),
                DialOutcome::Established
            ),
            INITIAL_BACKOFF
        );
    }

    #[test]
    fn next_backoff_uses_a_long_fixed_delay_after_an_explicit_rejection() {
        // Unlike the exponential-doubling cases above, a rejection
        // always waits REJECTED_BACKOFF regardless of `previous` - no
        // ramping toward it, no cap computation (ADR-0061).
        assert_eq!(
            next_backoff(INITIAL_BACKOFF, Duration::ZERO, DialOutcome::Rejected),
            REJECTED_BACKOFF
        );
        assert_eq!(
            next_backoff(MAX_BACKOFF, Duration::ZERO, DialOutcome::Rejected),
            REJECTED_BACKOFF
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
