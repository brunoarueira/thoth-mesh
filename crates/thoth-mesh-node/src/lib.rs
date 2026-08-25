//! `thoth-mesh-node`: the daemon that runs a thoth-mesh node.
//!
//! Wires `thoth-mesh-broker` (local pub/sub dispatch) to TCP
//! connections (see ADR-0007), and `thoth-mesh` (peer handshake and
//! membership tracking) to outbound and inbound peer links (see
//! ADR-0009). Once a peer link's handshake completes, either side of
//! it is a full participant in local routing - `Subscribe`,
//! `Unsubscribe`, and `Publish` are handled identically regardless of
//! whether they came from a client or a peer (see ADR-0010). This
//! node's own aggregate topic interest is also propagated to every
//! active peer link as it changes, and duplicate envelopes looping
//! back around a cyclic mesh are dropped rather than redelivered (see
//! ADR-0011) - so a publish on one node reaches subscribers connected
//! to any other node reachable through the mesh, not just directly
//! peered ones. Peers are similarly gossiped between links, so a node
//! also discovers and dials peers it was never directly configured
//! with (see ADR-0015). Connections are plaintext by default; passing
//! a [`TlsConfig`] to the `_with_tls` variants of `run`/`serve`/
//! `spawn` layers TLS underneath, transparently to everything above
//! (see ADR-0016). Those same variants also accept an optional
//! [`TopicAcl`], for per-topic client publish/subscribe authorization
//! (see ADR-0018).

pub mod connection;
pub mod metrics;
mod metrics_server;
mod peer_links;
mod peering;
mod shared;
mod tls_config;
mod topic_acl;

use std::sync::Arc;

use thoth_mesh_core::PeerId;
use thoth_mesh_tls::MaybeTlsStream;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub use shared::Shared;
pub use thoth_mesh::{Interest, Membership, PeerDirectory};
pub use thoth_mesh_core::DEFAULT_ADDR;
pub use tls_config::TlsConfig;
pub use topic_acl::{Action, Principal, TopicAcl, TopicAclParseError};

/// Binds `addr` and serves connections until an unrecoverable listener
/// error occurs, dialing each of `seed_peers` in the background. If
/// `metrics_addr` is given, also binds it and serves a Prometheus
/// scrape endpoint there in the background (see ADR-0013) - with no
/// `metrics_addr`, no second port is opened and nothing changes.
///
/// Every connection is plaintext and every client can publish/subscribe
/// to any topic - see [`run_with_tls`] for TLS (ADR-0016) and a
/// [`TopicAcl`] (ADR-0018).
pub async fn run(
    addr: &str,
    seed_peers: Vec<String>,
    metrics_addr: Option<String>,
) -> std::io::Result<()> {
    run_with_tls(addr, seed_peers, metrics_addr, None, None).await
}

/// Like [`run`], but with TLS enabled when `tls` is given (ADR-0016)
/// and per-topic client authorization enabled when `topic_acl` is
/// given (ADR-0018). Either `None` - what [`run`] always passes -
/// keeps that aspect unchanged from before its ADR.
pub async fn run_with_tls(
    addr: &str,
    seed_peers: Vec<String>,
    metrics_addr: Option<String>,
    tls: Option<TlsConfig>,
    topic_acl: Option<TopicAcl>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let node_id = PeerId::new();
    let my_listen_addr = listener.local_addr().ok().map(|addr| addr.to_string());
    let (mut shared, discovered_rx) = Shared::new_with_discovery(node_id, my_listen_addr);
    if let Some(tls) = tls {
        let allowed_peers = tls.allowed_peers.clone();
        let (acceptor, connector) = tls.build()?;
        shared.tls_acceptor = Some(acceptor);
        shared.tls_connector = Some(connector);
        shared.allowed_peers = allowed_peers.map(Arc::new);
    }
    shared.topic_acl = topic_acl.map(Arc::new);
    tokio::spawn(peering::spawn_discovery_dialer(
        discovered_rx,
        shared.clone(),
    ));

    if let Some(metrics_addr) = metrics_addr {
        let metrics_listener = TcpListener::bind(&metrics_addr).await?;
        let membership = shared.membership.clone();
        let broker = Arc::clone(&shared.broker);
        let metrics = shared.metrics.clone();
        tokio::spawn(async move {
            if let Err(err) =
                metrics_server::serve_metrics(metrics_listener, membership, broker, metrics).await
            {
                tracing::error!(%err, "metrics endpoint failed");
            }
        });
    }

    peering::spawn_seed_peers(seed_peers, shared.clone());
    accept_loop(listener, shared).await
}

/// Serves connections on an already-bound listener until an
/// unrecoverable listener error occurs, dialing each of `seed_peers`
/// in the background.
///
/// Split out from [`run`] so tests can bind an ephemeral port (`:0`)
/// and read back the actual bound address before serving. Doesn't
/// serve metrics - only [`run`], the daemon binary's entry point,
/// does that. Plaintext, with no topic ACL - see [`serve_with_tls`]
/// for TLS (ADR-0016) and a [`TopicAcl`] (ADR-0018).
pub async fn serve(listener: TcpListener, seed_peers: Vec<String>) -> std::io::Result<()> {
    serve_with_tls(listener, seed_peers, None, None).await
}

/// Like [`serve`], but with TLS enabled when `tls` is given and
/// per-topic client authorization enabled when `topic_acl` is given -
/// see [`run_with_tls`].
pub async fn serve_with_tls(
    listener: TcpListener,
    seed_peers: Vec<String>,
    tls: Option<TlsConfig>,
    topic_acl: Option<TopicAcl>,
) -> std::io::Result<()> {
    let node_id = PeerId::new();
    let my_listen_addr = listener.local_addr().ok().map(|addr| addr.to_string());
    let (mut shared, discovered_rx) = Shared::new_with_discovery(node_id, my_listen_addr);
    if let Some(tls) = tls {
        let allowed_peers = tls.allowed_peers.clone();
        let (acceptor, connector) = tls.build()?;
        shared.tls_acceptor = Some(acceptor);
        shared.tls_connector = Some(connector);
        shared.allowed_peers = allowed_peers.map(Arc::new);
    }
    shared.topic_acl = topic_acl.map(Arc::new);
    tokio::spawn(peering::spawn_discovery_dialer(
        discovered_rx,
        shared.clone(),
    ));
    peering::spawn_seed_peers(seed_peers, shared.clone());
    accept_loop(listener, shared).await
}

/// A node spawned via [`spawn`], along with the handles tests need to
/// inspect its membership or sever a connection - not needed for
/// ordinary use, which just wants [`run`]/[`serve`] to run forever.
pub struct Node {
    pub id: PeerId,
    pub membership: Membership,
    /// Every peer this node has learned a dialable address for, via
    /// direct handshake or gossip (see ADR-0015).
    pub discover: PeerDirectory,
    pub accept_loop: JoinHandle<std::io::Result<()>>,
    pub peer_dials: Vec<JoinHandle<()>>,
}

/// Like [`serve`], but returns immediately with a [`Node`] instead of
/// only once the (non-terminating) accept loop errors out - so tests
/// can query membership, or abort a `peer_dials` entry to simulate
/// that peer disappearing, while the node keeps running. Plaintext,
/// with no topic ACL - see [`spawn_with_tls`] for TLS (ADR-0016) and a
/// [`TopicAcl`] (ADR-0018).
pub fn spawn(listener: TcpListener, seed_peers: Vec<String>) -> Node {
    spawn_with_tls(listener, seed_peers, None, None)
        .expect("plaintext spawn (tls: None) never fails building TLS config")
}

/// Like [`spawn`], but with TLS enabled when `tls` is given and
/// per-topic client authorization enabled when `topic_acl` is given -
/// see [`run_with_tls`]. Fails only if `tls` is given and its
/// certs/key/CA don't load or don't build into a valid config.
pub fn spawn_with_tls(
    listener: TcpListener,
    seed_peers: Vec<String>,
    tls: Option<TlsConfig>,
    topic_acl: Option<TopicAcl>,
) -> std::io::Result<Node> {
    let node_id = PeerId::new();
    let my_listen_addr = listener.local_addr().ok().map(|addr| addr.to_string());
    let (mut shared, discovered_rx) = Shared::new_with_discovery(node_id, my_listen_addr);
    if let Some(tls) = tls {
        let allowed_peers = tls.allowed_peers.clone();
        let (acceptor, connector) = tls.build()?;
        shared.tls_acceptor = Some(acceptor);
        shared.tls_connector = Some(connector);
        shared.allowed_peers = allowed_peers.map(Arc::new);
    }
    shared.topic_acl = topic_acl.map(Arc::new);
    tokio::spawn(peering::spawn_discovery_dialer(
        discovered_rx,
        shared.clone(),
    ));
    let membership = shared.membership.clone();
    let discover = shared.discover.clone();
    let peer_dials = peering::spawn_seed_peers(seed_peers, shared.clone());
    let accept_loop = tokio::spawn(accept_loop(listener, shared));
    Ok(Node {
        id: node_id,
        membership,
        discover,
        accept_loop,
        peer_dials,
    })
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

async fn accept_loop(listener: TcpListener, shared: Shared) -> std::io::Result<()> {
    tracing::info!(
        node_id = ?shared.node_id,
        addr = ?shared.my_listen_addr,
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
        let shared = shared.clone();
        tokio::spawn(async move {
            // TLS, if enabled, wraps every accepted connection before
            // anything else touches it - client or peer alike, since
            // which this is isn't known until a Hello arrives, over
            // an already-secured stream if TLS is on (ADR-0016).
            let socket = match &shared.tls_acceptor {
                Some(acceptor) => match MaybeTlsStream::accept(acceptor, socket).await {
                    Ok(socket) => socket,
                    Err(err) => {
                        tracing::warn!(%err, %peer_addr, "TLS handshake failed, dropping connection");
                        return;
                    }
                },
                None => MaybeTlsStream::Plain(socket),
            };
            connection::handle_connection(socket, shared, None).await;
        });
    }
}
