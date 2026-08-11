//! Outbound connection management: dials configured seed peers,
//! performs the handshake, and keeps each connection open. See
//! ADR-0009.

use thoth_mesh::{Membership, dial_handshake};
use thoth_mesh_core::{PeerId, async_framing};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::Instrument;

/// Spawns one background task per entry in `seed_peers`, each dialing
/// that address, handshaking, and holding the connection open.
///
/// Connect and handshake failures are logged and not retried -
/// reconnect/backoff is out of scope here (tracked for a later
/// phase; see issue #23). Returns the tasks' handles so tests can
/// sever a link on demand; ordinary callers can drop them.
pub fn spawn_seed_peers(
    seed_peers: Vec<String>,
    my_id: PeerId,
    my_listen_addr: Option<String>,
    membership: Membership,
) -> Vec<JoinHandle<()>> {
    seed_peers
        .into_iter()
        .map(|peer_addr| {
            tokio::spawn(dial_peer(
                peer_addr,
                my_id,
                my_listen_addr.clone(),
                membership.clone(),
            ))
        })
        .collect()
}

async fn dial_peer(
    peer_addr: String,
    my_id: PeerId,
    my_listen_addr: Option<String>,
    membership: Membership,
) {
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

        let info = match dial_handshake(&mut conn, my_id, my_listen_addr).await {
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
        membership.mark_connected(info.peer_id, info.listen_addr);

        // Hold the connection open until the peer disconnects.
        // Nothing routes over peer links yet - that's Phase 3 - so
        // any further frames are just logged and discarded.
        loop {
            match async_framing::read_frame(&mut conn).await {
                Ok(_) => tracing::debug!("received a frame from peer (ignored for now)"),
                Err(err) => {
                    tracing::info!(%err, "peer connection closed");
                    break;
                }
            }
        }
        membership.mark_disconnected(info.peer_id);
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use thoth_mesh_core::{Envelope, MessageKind};
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    /// Polls `cond` until it's true, or panics once `TEST_TIMEOUT`
    /// elapses. `mark_connected`/`mark_disconnected` happen in a task
    /// this test doesn't otherwise synchronize with, so membership
    /// assertions need to wait for them rather than check once.
    async fn eventually(mut cond: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        while !cond() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "condition was not met within {TEST_TIMEOUT:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn dial_peer_sends_hello_and_tracks_membership() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let my_id = PeerId::new();
        let membership = Membership::new();

        tokio::spawn(dial_peer(
            addr.to_string(),
            my_id,
            Some("127.0.0.1:49500".to_owned()),
            membership.clone(),
        ));

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
        assert_eq!(hello.sender, my_id);
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

        eventually(|| membership.is_reachable(their_id)).await;

        // Closing our side should be noticed and reflected in
        // membership too.
        drop(conn);
        eventually(|| !membership.is_reachable(their_id)).await;
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

        timeout(
            TEST_TIMEOUT,
            dial_peer(addr.to_string(), PeerId::new(), None, Membership::new()),
        )
        .await
        .expect("dial_peer should return promptly on connection refused");
    }
}
