//! Outbound connection management: dials configured seed peers,
//! performs the handshake, and hands the connection off to the same
//! broker-wired dispatch the accept side uses. See ADR-0009 and
//! ADR-0010.

use thoth_mesh::dial_handshake;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::Instrument;

use crate::connection;
use crate::shared::Shared;

/// Spawns one background task per entry in `seed_peers`, each dialing
/// that address, handshaking, and then routing messages over the
/// connection exactly as an accepted one would.
///
/// Connect and handshake failures are logged and not retried -
/// reconnect/backoff is out of scope here (tracked for Phase 4;
/// see issue #18). Returns the tasks' handles so tests can sever a
/// link on demand; ordinary callers can drop them.
pub fn spawn_seed_peers(seed_peers: Vec<String>, shared: Shared) -> Vec<JoinHandle<()>> {
    seed_peers
        .into_iter()
        .map(|peer_addr| tokio::spawn(dial_peer(peer_addr, shared.clone())))
        .collect()
}

async fn dial_peer(peer_addr: String, shared: Shared) {
    let span = tracing::info_span!("peer", addr = %peer_addr);
    async move {
        let stream = match TcpStream::connect(&peer_addr).await {
            Ok(stream) => stream,
            Err(err) => {
                tracing::warn!(%err, "failed to connect to seed peer");
                return;
            }
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
    use std::time::Duration;

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
        let sub = Envelope::new(
            their_id,
            MessageKind::Subscribe {
                topic: topic.clone(),
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
                topic: topic.clone()
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
        shared.interest.subscribe(topic.clone());

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
            MessageKind::Subscribe { topic }
        );
    }
}
