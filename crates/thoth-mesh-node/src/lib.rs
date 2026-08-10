//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.
//!
//! Wires `thoth-mesh-broker` (local pub/sub dispatch) to TCP
//! connections. See ADR-0007. Federation (`thoth-mesh`) is not yet
//! wired in - it's still an empty placeholder.

pub mod connection;

use std::sync::Arc;

use thoth_mesh_broker::Broker;
use thoth_mesh_core::PeerId;
use tokio::net::TcpListener;

pub use thoth_mesh_core::DEFAULT_ADDR;

/// Binds `addr` and serves connections until an unrecoverable listener
/// error occurs.
pub async fn run(addr: &str) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve(listener).await
}

/// Serves connections on an already-bound listener until an
/// unrecoverable listener error occurs.
///
/// Split out from [`run`] so tests can bind an ephemeral port (`:0`)
/// and read back the actual bound address before serving.
pub async fn serve(listener: TcpListener) -> std::io::Result<()> {
    let broker = Arc::new(Broker::new());
    let node_id = PeerId::new();
    tracing::info!(
        ?node_id,
        addr = ?listener.local_addr().ok(),
        "node ready, accepting connections"
    );

    loop {
        let (socket, peer_addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                tracing::error!(%err, "listener accept failed, shutting down");
                return Err(err);
            }
        };
        tracing::debug!(%peer_addr, "accepted connection");
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            connection::handle_connection(socket, broker, node_id).await;
        });
    }
}
