//! Outbound connection management: dials configured seed peers,
//! performs the handshake, and keeps each connection open. See
//! ADR-0009.

use thoth_mesh::dial_handshake;
use thoth_mesh_core::{PeerId, async_framing};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::Instrument;

/// Spawns one background task per entry in `seed_peers`, each dialing
/// that address, handshaking, and holding the connection open.
///
/// Connect and handshake failures are logged and not retried -
/// reconnect/backoff is out of scope here (tracked for a later
/// phase; see issue #23).
pub fn spawn_seed_peers(seed_peers: Vec<String>, my_id: PeerId, my_listen_addr: Option<String>) {
    for peer_addr in seed_peers {
        tokio::spawn(dial_peer(peer_addr, my_id, my_listen_addr.clone()));
    }
}

async fn dial_peer(peer_addr: String, my_id: PeerId, my_listen_addr: Option<String>) {
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

    #[tokio::test]
    async fn dial_peer_sends_our_hello_with_the_given_listen_addr() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let my_id = PeerId::new();

        tokio::spawn(dial_peer(
            addr.to_string(),
            my_id,
            Some("127.0.0.1:49500".to_owned()),
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
        let reply = Envelope::new(PeerId::new(), MessageKind::Hello { listen_addr: None });
        async_framing::write_frame(&mut conn, &reply.to_bytes().unwrap())
            .await
            .unwrap();
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
            dial_peer(addr.to_string(), PeerId::new(), None),
        )
        .await
        .expect("dial_peer should return promptly on connection refused");
    }
}
