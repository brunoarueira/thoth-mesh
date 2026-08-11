//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.
//!
//! Wires `thoth-mesh-broker` (local pub/sub dispatch) to TCP
//! connections. See ADR-0007. Federation (`thoth-mesh`) is not yet
//! wired in - it's still an empty placeholder.

pub mod connection;
mod peering;

use std::sync::Arc;

use thoth_mesh_broker::Broker;
use thoth_mesh_core::PeerId;
use tokio::net::TcpListener;

pub use thoth_mesh_core::DEFAULT_ADDR;

/// Binds `addr` and serves connections until an unrecoverable listener
/// error occurs, dialing each of `seed_peers` in the background.
pub async fn run(addr: &str, seed_peers: Vec<String>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve(listener, seed_peers).await
}

/// Serves connections on an already-bound listener until an
/// unrecoverable listener error occurs, dialing each of `seed_peers`
/// in the background.
///
/// Split out from [`run`] so tests can bind an ephemeral port (`:0`)
/// and read back the actual bound address before serving.
pub async fn serve(listener: TcpListener, seed_peers: Vec<String>) -> std::io::Result<()> {
    let broker = Arc::new(Broker::new());
    let node_id = PeerId::new();
    let my_listen_addr = listener.local_addr().ok().map(|addr| addr.to_string());
    tracing::info!(
        ?node_id,
        addr = ?my_listen_addr,
        "node ready, accepting connections"
    );

    peering::spawn_seed_peers(seed_peers, node_id, my_listen_addr.clone());

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
        let my_listen_addr = my_listen_addr.clone();
        tokio::spawn(async move {
            connection::handle_connection(socket, broker, node_id, my_listen_addr).await;
        });
    }
}
