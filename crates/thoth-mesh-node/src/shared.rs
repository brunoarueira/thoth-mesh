//! Bundles the node-wide services shared by every connection task -
//! accepted or dialed alike - so adding a new one doesn't mean
//! growing every function signature that threads state through. See
//! ADR-0011.

use std::sync::Arc;

use thoth_mesh::{Interest, Membership, PeerDirectory};
use thoth_mesh_broker::Broker;
use thoth_mesh_core::PeerId;
use tokio::sync::mpsc;

use crate::metrics::Metrics;
use crate::peer_links::PeerLinks;

/// Everything a connection task needs beyond its own socket and
/// whatever it learns from the peer on the other end.
#[derive(Debug, Clone)]
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
        };
        (shared, discovered_rx)
    }
}
