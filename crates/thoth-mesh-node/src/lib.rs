//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.
//!
//! Wires `thoth-mesh-broker` (local pub/sub dispatch) to TCP
//! connections. See ADR-0007. Federation (`thoth-mesh`) is not yet
//! wired in - it's still an empty placeholder.

pub mod connection;
pub mod framing;

use std::sync::Arc;

use thoth_mesh_broker::Broker;
use thoth_mesh_core::PeerId;
use tokio::net::TcpListener;

/// Default bind address: a private/dynamic-range port, chosen to avoid
/// colliding with commonly registered services.
pub const DEFAULT_ADDR: &str = "127.0.0.1:49500";

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

    loop {
        let (socket, _) = listener.accept().await?;
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            connection::handle_connection(socket, broker, node_id).await;
        });
    }
}
