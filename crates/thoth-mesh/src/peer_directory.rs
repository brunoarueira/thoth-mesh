//! Tracks every peer this node has ever learned a dialable address
//! for - whether from directly completing a handshake with it, or
//! from a peer gossiping about it - so a node can discover and dial
//! peers it was never directly configured with. See ADR-0015.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use thoth_mesh_core::PeerId;

/// A thread-safe, cheaply-cloneable registry of every peer known to
/// be dialable and the address to reach it at.
///
/// Deliberately has no concept of "currently connected" - that's
/// `Membership`'s job. This just answers "have I ever recorded this
/// peer before," which is what gates both further gossip propagation
/// and auto-dial (see ADR-0015) - the same way `Interest`'s
/// 0-to-1 transition gates interest propagation (ADR-0011).
#[derive(Debug, Default, Clone)]
pub struct PeerDirectory {
    known: Arc<Mutex<HashMap<PeerId, String>>>,
}

impl PeerDirectory {
    /// An empty registry, with no peers known yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `peer_id` as dialable at `listen_addr`. Returns `true`
    /// only the first time this `peer_id` is recorded - the caller
    /// should propagate it onward and consider auto-dialing it only
    /// in that case. A later call for an already-known `peer_id`
    /// still refreshes its address, but returns `false`.
    pub fn record(&self, peer_id: PeerId, listen_addr: String) -> bool {
        let mut known = self.known.lock().unwrap();
        let is_new = !known.contains_key(&peer_id);
        known.insert(peer_id, listen_addr);
        is_new
    }

    /// Every peer currently known, except `exclude` - used to catch a
    /// newly-linked peer up on everyone else this node already knows
    /// about, without telling it about itself.
    pub fn snapshot_excluding(&self, exclude: PeerId) -> Vec<(PeerId, String)> {
        self.known
            .lock()
            .unwrap()
            .iter()
            .filter(|(peer_id, _)| **peer_id != exclude)
            .map(|(peer_id, listen_addr)| (*peer_id, listen_addr.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_record_is_new() {
        let directory = PeerDirectory::new();
        assert!(directory.record(PeerId::new(), "127.0.0.1:49500".to_owned()));
    }

    #[test]
    fn a_second_record_of_the_same_peer_is_not_new() {
        let directory = PeerDirectory::new();
        let peer_id = PeerId::new();
        directory.record(peer_id, "127.0.0.1:49500".to_owned());
        assert!(!directory.record(peer_id, "127.0.0.1:49500".to_owned()));
    }

    #[test]
    fn a_second_record_still_refreshes_the_address() {
        let directory = PeerDirectory::new();
        let peer_id = PeerId::new();
        directory.record(peer_id, "127.0.0.1:49500".to_owned());
        directory.record(peer_id, "127.0.0.1:49501".to_owned());

        let snapshot = directory.snapshot_excluding(PeerId::new());
        assert_eq!(snapshot, vec![(peer_id, "127.0.0.1:49501".to_owned())]);
    }

    #[test]
    fn snapshot_excluding_omits_the_given_peer() {
        let directory = PeerDirectory::new();
        let excluded = PeerId::new();
        let other = PeerId::new();
        directory.record(excluded, "127.0.0.1:49500".to_owned());
        directory.record(other, "127.0.0.1:49501".to_owned());

        let snapshot = directory.snapshot_excluding(excluded);
        assert_eq!(snapshot, vec![(other, "127.0.0.1:49501".to_owned())]);
    }

    #[test]
    fn snapshot_excluding_is_empty_for_a_fresh_registry() {
        assert!(
            PeerDirectory::new()
                .snapshot_excluding(PeerId::new())
                .is_empty()
        );
    }
}
