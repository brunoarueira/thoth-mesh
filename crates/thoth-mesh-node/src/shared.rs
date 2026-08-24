//! Bundles the node-wide services shared by every connection task -
//! accepted or dialed alike - so adding a new one doesn't mean
//! growing every function signature that threads state through. See
//! ADR-0011.

use std::collections::HashSet;
use std::sync::Arc;

use thoth_mesh::{Interest, Membership, PeerDirectory};
use thoth_mesh_broker::Broker;
use thoth_mesh_core::PeerId;
use thoth_mesh_tls::{TlsAcceptor, TlsConnector};
use tokio::sync::mpsc;

use crate::metrics::Metrics;
use crate::peer_links::PeerLinks;
use crate::topic_acl::TopicAcl;

/// Everything a connection task needs beyond its own socket and
/// whatever it learns from the peer on the other end.
#[derive(Clone)]
pub struct Shared {
    pub broker: Arc<Broker>,
    pub membership: Membership,
    pub interest: Interest,
    pub peer_links: PeerLinks,
    pub node_id: PeerId,
    pub my_listen_addr: Option<String>,
    pub metrics: Metrics,
    /// Every peer known to be dialable, and where - see ADR-0015.
    pub discover: PeerDirectory,
    /// Where a connection task pushes an address it wants dialed,
    /// once it's decided (via `discover`) that a gossiped peer is
    /// worth auto-dialing. `peering::spawn_discovery_dialer` owns the
    /// receiving end. Not read from directly outside `connection.rs`.
    pub(crate) discovered_tx: mpsc::UnboundedSender<String>,
    /// TLS config for the accept side, if TLS is enabled (see
    /// ADR-0016). `None` - the default - means every accepted
    /// connection stays plaintext, unchanged from before this ADR.
    pub tls_acceptor: Option<Arc<TlsAcceptor>>,
    /// TLS config for the dial side, if TLS is enabled. Always
    /// presents this node's own identity when set (a peer dialing a
    /// peer always identifies itself) - see ADR-0016.
    pub tls_connector: Option<Arc<TlsConnector>>,
    /// Peer certificate fingerprints allowed to link as a peer,
    /// `--allow-peer` (repeatable). `None` - the default - means
    /// unchanged behavior: no allowlist enforcement, same as before
    /// this ADR. `Some` (including an empty set) enforces on every
    /// peer link regardless of which side dialed. See ADR-0017.
    pub allowed_peers: Option<Arc<HashSet<[u8; 32]>>>,
    /// Per-topic client publish/subscribe permissions, `--topic-acl`
    /// (repeatable). `None` - the default - means unchanged behavior:
    /// any client can publish/subscribe to anything. `Some` enforces
    /// default-deny for whatever's not explicitly listed, against
    /// connections not (yet) known to be peer links. See ADR-0018.
    pub topic_acl: Option<Arc<TopicAcl>>,
}

// Hand-rolled rather than derived: `TlsAcceptor`/`TlsConnector` don't
// implement `Debug` (and printing TLS config wouldn't be useful even
// if they did) - report only whether each is set.
impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("broker", &self.broker)
            .field("membership", &self.membership)
            .field("interest", &self.interest)
            .field("peer_links", &self.peer_links)
            .field("node_id", &self.node_id)
            .field("my_listen_addr", &self.my_listen_addr)
            .field("metrics", &self.metrics)
            .field("discover", &self.discover)
            .field("tls_acceptor", &self.tls_acceptor.is_some())
            .field("tls_connector", &self.tls_connector.is_some())
            .field(
                "allowed_peers",
                &self.allowed_peers.as_ref().map(|set| set.len()),
            )
            .field("topic_acl", &self.topic_acl.is_some())
            .finish_non_exhaustive()
    }
}

impl Shared {
    /// Fresh, empty node-wide state for a node identified as
    /// `node_id`, advertising `my_listen_addr` to peers (if it
    /// accepts inbound connections).
    ///
    /// Discovered-peer addresses pushed onto `discovered_tx` in this
    /// configuration are simply dropped - correct for tests that
    /// don't exercise auto-dial, since nothing reads the other end.
    /// Callers that want to observe/act on auto-dial should use
    /// [`Shared::new_with_discovery`] instead.
    pub fn new(node_id: PeerId, my_listen_addr: Option<String>) -> Self {
        Self::new_with_discovery(node_id, my_listen_addr).0
    }

    /// Like [`Shared::new`], but also returns the receiving end of
    /// the discovery channel, for a caller (namely
    /// `peering::spawn_discovery_dialer`) that will actually dial
    /// what comes through it.
    pub fn new_with_discovery(
        node_id: PeerId,
        my_listen_addr: Option<String>,
    ) -> (Self, mpsc::UnboundedReceiver<String>) {
        let (discovered_tx, discovered_rx) = mpsc::unbounded_channel();
        let shared = Self {
            broker: Arc::new(Broker::new()),
            membership: Membership::new(),
            interest: Interest::new(),
            peer_links: PeerLinks::new(),
            node_id,
            my_listen_addr,
            metrics: Metrics::new(),
            discover: PeerDirectory::new(),
            discovered_tx,
            tls_acceptor: None,
            tls_connector: None,
            allowed_peers: None,
            topic_acl: None,
        };
        (shared, discovered_rx)
    }
}
