//! Bundles the node-wide services shared by every connection task -
//! accepted or dialed alike - so adding a new one doesn't mean
//! growing every function signature that threads state through. See
//! ADR-0011.

use std::sync::Arc;

use thoth_mesh::{Interest, Membership};
use thoth_mesh_broker::Broker;
use thoth_mesh_core::PeerId;

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
}

impl Shared {
    /// Fresh, empty node-wide state for a node identified as
    /// `node_id`, advertising `my_listen_addr` to peers (if it
    /// accepts inbound connections).
    pub fn new(node_id: PeerId, my_listen_addr: Option<String>) -> Self {
        Self {
            broker: Arc::new(Broker::new()),
            membership: Membership::new(),
            interest: Interest::new(),
            peer_links: PeerLinks::new(),
            node_id,
            my_listen_addr,
            metrics: Metrics::new(),
        }
    }
}
