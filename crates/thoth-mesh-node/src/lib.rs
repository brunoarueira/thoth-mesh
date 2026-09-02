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
//! `spawn` (via [`NodeOptions`]) layers TLS underneath, transparently
//! to everything above (see ADR-0016). [`NodeOptions`] also carries an
//! optional [`TopicAcl`], for per-topic client publish/subscribe
//! authorization (see ADR-0018), and a second, independent one scoped
//! to peer links instead of clients (see ADR-0020). [`run`]/
//! [`run_with_tls`] additionally accept an optional bearer token
//! gating the metrics endpoint, when one is opened at all (see
//! ADR-0019) - kept separate from [`NodeOptions`] since it's the one
//! knob `serve_with_tls`/`spawn_with_tls` have no use for.

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

/// The connection-dispatch-level options shared identically by
/// [`run_with_tls`], [`serve_with_tls`], and [`spawn_with_tls`] -
/// bundled into one struct rather than growing their positional
/// `Option` parameters further. See ADR-0020, which introduced this
/// after ADR-0018 flagged a fourth orthogonal knob as the point worth
/// stopping to reconsider that.
///
/// `Default` (every field `None`) is exactly the plaintext,
/// unrestricted behavior [`run`]/[`serve`]/[`spawn`] always pass.
#[derive(Debug, Default, Clone)]
pub struct NodeOptions {
    /// TLS config for this node, if enabled - see ADR-0016.
    pub tls: Option<TlsConfig>,
    /// Per-topic client publish/subscribe permissions, `--topic-acl` -
    /// see ADR-0018.
    pub topic_acl: Option<TopicAcl>,
    /// Per-topic peer-link publish/subscribe permissions,
    /// `--peer-topic-acl` - see ADR-0020. Independent of `topic_acl`:
    /// a peer link is never checked against `topic_acl`, and a client
    /// connection is never checked against this.
    pub peer_topic_acl: Option<TopicAcl>,
}

/// Binds `addr` and serves connections until an unrecoverable listener
/// error occurs, dialing each of `seed_peers` in the background. If
/// `metrics_addr` is given, also binds it and serves a Prometheus
/// scrape endpoint there in the background (see ADR-0013) - with no
/// `metrics_addr`, no second port is opened and nothing changes.
///
/// Every connection is plaintext, every client and peer link can
/// publish/subscribe to any topic, and the metrics endpoint (if any)
/// has no access control - see [`run_with_tls`] for TLS (ADR-0016), a
/// [`NodeOptions`] (ADR-0018/ADR-0020), and a metrics bearer token
/// (ADR-0019).
pub async fn run(
    addr: &str,
    seed_peers: Vec<String>,
    metrics_addr: Option<String>,
) -> std::io::Result<()> {
    run_with_tls(addr, seed_peers, metrics_addr, NodeOptions::default(), None).await
}

/// Like [`run`], but with `options` (ADR-0020) controlling TLS
/// (ADR-0016), client-scoped topic authorization (ADR-0018), and
/// peer-scoped topic authorization (ADR-0020) all at once, and the
/// metrics endpoint gated behind `metrics_token` when given
/// (ADR-0019). `NodeOptions::default()` - what [`run`] always passes -
/// keeps every aspect unchanged from before its ADR. `metrics_token`
/// only has an effect when `metrics_addr` is also given; nothing binds
/// otherwise.
pub async fn run_with_tls(
    addr: &str,
    seed_peers: Vec<String>,
    metrics_addr: Option<String>,
    options: NodeOptions,
    metrics_token: Option<Arc<str>>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let node_id = PeerId::new();
    let my_listen_addr = listener.local_addr().ok().map(|addr| addr.to_string());
    let (mut shared, discovered_rx) = Shared::new_with_discovery(node_id, my_listen_addr);
    if let Some(tls) = options.tls {
        let allowed_peers = tls.allowed_peers.clone();
        let (acceptor, connector) = tls.build()?;
        shared.tls_acceptor = Some(acceptor);
        shared.tls_connector = Some(connector);
        shared.allowed_peers = allowed_peers.map(Arc::new);
    }
    shared.topic_acl = options.topic_acl.map(Arc::new);
    shared.peer_topic_acl = options.peer_topic_acl.map(Arc::new);
    tokio::spawn(peering::spawn_discovery_dialer(
        discovered_rx,
        shared.clone(),
    ));

    if let Some(metrics_addr) = metrics_addr {
        let metrics_listener = TcpListener::bind(&metrics_addr).await?;
        let membership = shared.membership.clone();
        let broker = Arc::clone(&shared.broker);
        let discover = shared.discover.clone();
        let metrics = shared.metrics.clone();
        tokio::spawn(async move {
            if let Err(err) = metrics_server::serve_metrics(
                metrics_listener,
                membership,
                broker,
                discover,
                metrics,
                metrics_token,
            )
            .await
            {
                tracing::error!(%err, "metrics endpoint failed");
            }
        });
    }

    peering::spawn_seed_peers(seed_peers, shared.clone());
    accept_loop(listener, shared, None).await
}

/// Serves connections on an already-bound listener until an
/// unrecoverable listener error occurs, dialing each of `seed_peers`
/// in the background.
///
/// Split out from [`run`] so tests can bind an ephemeral port (`:0`)
/// and read back the actual bound address before serving. Doesn't
/// serve metrics - only [`run`], the daemon binary's entry point,
/// does that. Plaintext, with no topic ACLs - see [`serve_with_tls`]
/// for TLS (ADR-0016) and a [`NodeOptions`] (ADR-0018/ADR-0020).
pub async fn serve(listener: TcpListener, seed_peers: Vec<String>) -> std::io::Result<()> {
    serve_with_tls(listener, seed_peers, NodeOptions::default()).await
}

/// Like [`serve`], but with `options` (ADR-0020) controlling TLS
/// (ADR-0016), client-scoped topic authorization (ADR-0018), and
/// peer-scoped topic authorization (ADR-0020) - see [`run_with_tls`].
pub async fn serve_with_tls(
    listener: TcpListener,
    seed_peers: Vec<String>,
    options: NodeOptions,
) -> std::io::Result<()> {
    let node_id = PeerId::new();
    let my_listen_addr = listener.local_addr().ok().map(|addr| addr.to_string());
    let (mut shared, discovered_rx) = Shared::new_with_discovery(node_id, my_listen_addr);
    if let Some(tls) = options.tls {
        let allowed_peers = tls.allowed_peers.clone();
        let (acceptor, connector) = tls.build()?;
        shared.tls_acceptor = Some(acceptor);
        shared.tls_connector = Some(connector);
        shared.allowed_peers = allowed_peers.map(Arc::new);
    }
    shared.topic_acl = options.topic_acl.map(Arc::new);
    shared.peer_topic_acl = options.peer_topic_acl.map(Arc::new);
    tokio::spawn(peering::spawn_discovery_dialer(
        discovered_rx,
        shared.clone(),
    ));
    peering::spawn_seed_peers(seed_peers, shared.clone());
    accept_loop(listener, shared, None).await
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
    /// Every accepted connection's handle, added to as this node
    /// accepts them - unlike `peer_dials`, this also covers
    /// connections the *other* side dialed in. Aborting `accept_loop`
    /// alone only stops accepting *new* connections; a test simulating
    /// this node fully dying (see ADR-0028's chaos coverage) needs to
    /// abort these too, or an already-accepted peer link stays up.
    pub accepted_connections: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
}

/// Like [`serve`], but returns immediately with a [`Node`] instead of
/// only once the (non-terminating) accept loop errors out - so tests
/// can query membership, or abort a `peer_dials` entry to simulate
/// that peer disappearing, while the node keeps running. Plaintext,
/// with no topic ACLs - see [`spawn_with_tls`] for TLS (ADR-0016) and
/// a [`NodeOptions`] (ADR-0018/ADR-0020).
pub fn spawn(listener: TcpListener, seed_peers: Vec<String>) -> Node {
    spawn_with_tls(listener, seed_peers, NodeOptions::default())
        .expect("plaintext spawn (tls: None) never fails building TLS config")
}

/// Like [`spawn`], but with `options` (ADR-0020) controlling TLS
/// (ADR-0016), client-scoped topic authorization (ADR-0018), and
/// peer-scoped topic authorization (ADR-0020) - see [`run_with_tls`].
/// Fails only if `options.tls` is given and its certs/key/CA don't
/// load or don't build into a valid config.
pub fn spawn_with_tls(
    listener: TcpListener,
    seed_peers: Vec<String>,
    options: NodeOptions,
) -> std::io::Result<Node> {
    let node_id = PeerId::new();
    let my_listen_addr = listener.local_addr().ok().map(|addr| addr.to_string());
    let (mut shared, discovered_rx) = Shared::new_with_discovery(node_id, my_listen_addr);
    if let Some(tls) = options.tls {
        let allowed_peers = tls.allowed_peers.clone();
        let (acceptor, connector) = tls.build()?;
        shared.tls_acceptor = Some(acceptor);
        shared.tls_connector = Some(connector);
        shared.allowed_peers = allowed_peers.map(Arc::new);
    }
    shared.topic_acl = options.topic_acl.map(Arc::new);
    shared.peer_topic_acl = options.peer_topic_acl.map(Arc::new);
    tokio::spawn(peering::spawn_discovery_dialer(
        discovered_rx,
        shared.clone(),
    ));
    let membership = shared.membership.clone();
    let discover = shared.discover.clone();
    let peer_dials = peering::spawn_seed_peers(seed_peers, shared.clone());
    let accepted_connections = Arc::new(std::sync::Mutex::new(Vec::new()));
    let accept_loop = tokio::spawn(accept_loop(
        listener,
        shared,
        Some(Arc::clone(&accepted_connections)),
    ));
    Ok(Node {
        id: node_id,
        membership,
        discover,
        accept_loop,
        peer_dials,
        accepted_connections,
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

/// `connections` - only ever `Some` from [`spawn_with_tls`] - collects
/// each accepted connection's `JoinHandle` so a test can fully sever
/// every link this node is party to (see [`Node::accepted_connections`]),
/// not just the ones it dialed itself. `None` from every production
/// entry point (`run_with_tls`/`serve_with_tls`): an ordinary
/// long-running node has no test to abort a connection for, and never
/// collecting the handles at all - rather than collecting and never
/// draining them - avoids growing an unbounded list of finished
/// handles over its lifetime (the same bounded-footprint posture
/// ADR-0025 already established elsewhere).
async fn accept_loop(
    listener: TcpListener,
    shared: Shared,
    connections: Option<Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>>,
) -> std::io::Result<()> {
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
        let handle = tokio::spawn(async move {
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
        if let Some(connections) = &connections {
            connections.lock().unwrap().push(handle);
        }
    }
}
