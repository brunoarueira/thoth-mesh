//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.
//!
//! Wires `thoth-mesh-broker` (local pub/sub dispatch) to TCP
//! connections (see ADR-0007), and `thoth-mesh` (peer handshake and
//! membership tracking) to outbound and inbound peer links (see
//! ADR-0009). Once a peer link's handshake completes, either side of
//! it is a full participant in local routing - `Subscribe`,
//! `Unsubscribe`, and `Publish` are handled identically regardless of
//! whether they came from a client or a peer (see ADR-0010). Nothing
//! yet causes a node to *originate* interest toward its peers based
//! on its own local clients, or prevents forwarding loops on a
//! cyclic mesh - that's the next Phase 3 issue.

pub mod connection;
mod peering;

use std::sync::Arc;

use thoth_mesh_broker::Broker;
use thoth_mesh_core::PeerId;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub use thoth_mesh::Membership;
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
    let node_id = PeerId::new();
    let membership = Membership::new();
    let broker = Arc::new(Broker::new());
    let my_listen_addr = listener.local_addr().ok().map(|addr| addr.to_string());
    peering::spawn_seed_peers(
        seed_peers,
        node_id,
        my_listen_addr.clone(),
        membership.clone(),
        Arc::clone(&broker),
    );
    accept_loop(listener, node_id, my_listen_addr, membership, broker).await
}

/// A node spawned via [`spawn`], along with the handles tests need to
/// inspect its membership or sever a connection - not needed for
/// ordinary use, which just wants [`run`]/[`serve`] to run forever.
pub struct Node {
    pub id: PeerId,
    pub membership: Membership,
    pub accept_loop: JoinHandle<std::io::Result<()>>,
    pub peer_dials: Vec<JoinHandle<()>>,
}

/// Like [`serve`], but returns immediately with a [`Node`] instead of
/// only once the (non-terminating) accept loop errors out - so tests
/// can query membership, or abort a `peer_dials` entry to simulate
/// that peer disappearing, while the node keeps running.
pub fn spawn(listener: TcpListener, seed_peers: Vec<String>) -> Node {
    let id = PeerId::new();
    let membership = Membership::new();
    let broker = Arc::new(Broker::new());
    let my_listen_addr = listener.local_addr().ok().map(|addr| addr.to_string());
    let peer_dials = peering::spawn_seed_peers(
        seed_peers,
        id,
        my_listen_addr.clone(),
        membership.clone(),
        Arc::clone(&broker),
    );
    let accept_loop = tokio::spawn(accept_loop(
        listener,
        id,
        my_listen_addr,
        membership.clone(),
        broker,
    ));
    Node {
        id,
        membership,
        accept_loop,
        peer_dials,
    }
}

/// Test-only helpers, shared between this crate's own unit tests and
/// its integration tests - not needed for ordinary use.
pub mod test_support {
    use std::time::Duration;

    /// How long [`eventually`] polls before giving up.
    const TIMEOUT: Duration = Duration::from_secs(2);

    /// Polls `cond` until it's true, or panics once [`TIMEOUT`]
    /// elapses. Membership updates happen in a task these tests don't
    /// otherwise synchronize with, so assertions on it need to wait
    /// rather than check once.
    pub async fn eventually(mut cond: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        while !cond() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "condition was not met within {TIMEOUT:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    node_id: PeerId,
    my_listen_addr: Option<String>,
    membership: Membership,
    broker: Arc<Broker>,
) -> std::io::Result<()> {
    tracing::info!(
        ?node_id,
        addr = ?my_listen_addr,
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
        let my_listen_addr = my_listen_addr.clone();
        let membership = membership.clone();
        tokio::spawn(async move {
            connection::handle_connection(
                socket,
                broker,
                node_id,
                my_listen_addr,
                membership,
                None,
            )
            .await;
        });
    }
}
