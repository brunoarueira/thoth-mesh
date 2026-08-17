//! Tracks which configured peers are currently reachable, updated as
//! peer links come up and go down. See issue #24.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use thoth_mesh_core::PeerId;

/// A peer's last-known connection state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerStatus {
    /// Whether the peer is currently connected.
    pub connected: bool,
    /// The address it reported for other peers to dial it back at, if
    /// any - kept even after a disconnect, since it's still our best
    /// guess for reconnecting later.
    pub listen_addr: Option<String>,
}

/// A thread-safe, cheaply-cloneable registry of peer connection state,
/// shared across every connection task (inbound and outbound) on a
/// node.
///
/// Plain `std::sync::Mutex` rather than an async lock: entries are
/// only ever held for the duration of a `HashMap` operation, never
/// across an `.await`.
#[derive(Debug, Default, Clone)]
pub struct Membership {
    peers: Arc<Mutex<HashMap<PeerId, PeerStatus>>>,
}

impl Membership {
    /// An empty registry, with no peers known yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `peer_id` as connected, storing the listen address it
    /// reported (if any). Call once a peer link's handshake
    /// completes, whether it was dialed or accepted.
    ///
    /// Logs a "peer up" transition, but only the first time - calling
    /// this again for an already-connected peer just refreshes its
    /// listen address.
    pub fn mark_connected(&self, peer_id: PeerId, listen_addr: Option<String>) {
        let mut peers = self.peers.lock().unwrap();
        let was_connected = peers.get(&peer_id).is_some_and(|status| status.connected);
        peers.insert(
            peer_id,
            PeerStatus {
                connected: true,
                listen_addr,
            },
        );
        // Release the lock before logging - tracing::info! isn't
        // free, and there's no reason to hold the mutex while it runs.
        drop(peers);
        if !was_connected {
            tracing::info!(?peer_id, "peer up");
        }
    }

    /// Records `peer_id` as disconnected. Call when its connection
    /// drops (read error/EOF on either an outbound-dialed or
    /// inbound-accepted link).
    ///
    /// A no-op, with no log line, for a peer that was never connected
    /// (or is already marked disconnected).
    pub fn mark_disconnected(&self, peer_id: PeerId) {
        let mut peers = self.peers.lock().unwrap();
        let Some(status) = peers.get_mut(&peer_id) else {
            return;
        };
        let was_connected = status.connected;
        status.connected = false;
        // Same as in mark_connected: don't hold the lock while logging.
        drop(peers);
        if was_connected {
            tracing::info!(?peer_id, "peer down");
        }
    }

    /// Whether `peer_id` is currently connected.
    pub fn is_reachable(&self, peer_id: PeerId) -> bool {
        self.peers
            .lock()
            .unwrap()
            .get(&peer_id)
            .is_some_and(|status| status.connected)
    }

    /// A snapshot of every peer this node has ever seen a `Hello`
    /// from, and its last-known state.
    pub fn snapshot(&self) -> HashMap<PeerId, PeerStatus> {
        self.peers.lock().unwrap().clone()
    }

    /// How many peers are currently connected. Read live rather than
    /// tracked separately, so it can never drift out of sync with
    /// [`mark_connected`](Self::mark_connected)/
    /// [`mark_disconnected`](Self::mark_disconnected) - see ADR-0013.
    pub fn connected_count(&self) -> usize {
        self.peers
            .lock()
            .unwrap()
            .values()
            .filter(|status| status.connected)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_peer_is_not_reachable() {
        let membership = Membership::new();
        assert!(!membership.is_reachable(PeerId::new()));
    }

    #[test]
    fn mark_connected_makes_a_peer_reachable() {
        let membership = Membership::new();
        let peer_id = PeerId::new();

        membership.mark_connected(peer_id, Some("127.0.0.1:49500".to_owned()));

        assert!(membership.is_reachable(peer_id));
        let status = membership.snapshot()[&peer_id].clone();
        assert_eq!(status.listen_addr, Some("127.0.0.1:49500".to_owned()));
    }

    #[test]
    fn mark_disconnected_makes_a_peer_unreachable() {
        let membership = Membership::new();
        let peer_id = PeerId::new();

        membership.mark_connected(peer_id, None);
        membership.mark_disconnected(peer_id);

        assert!(!membership.is_reachable(peer_id));
    }

    #[test]
    fn mark_disconnected_on_an_unknown_peer_is_a_no_op() {
        let membership = Membership::new();
        let peer_id = PeerId::new();

        membership.mark_disconnected(peer_id);

        assert!(!membership.is_reachable(peer_id));
        assert!(membership.snapshot().is_empty());
    }

    #[test]
    fn connected_count_only_counts_currently_connected_peers() {
        let membership = Membership::new();
        let still_connected = PeerId::new();
        let disconnected = PeerId::new();

        membership.mark_connected(still_connected, None);
        membership.mark_connected(disconnected, None);
        membership.mark_disconnected(disconnected);

        assert_eq!(membership.connected_count(), 1);
    }

    #[test]
    fn a_peer_can_reconnect_after_disconnecting() {
        let membership = Membership::new();
        let peer_id = PeerId::new();

        membership.mark_connected(peer_id, Some("127.0.0.1:49500".to_owned()));
        membership.mark_disconnected(peer_id);
        membership.mark_connected(peer_id, Some("127.0.0.1:49501".to_owned()));

        assert!(membership.is_reachable(peer_id));
        let status = membership.snapshot()[&peer_id].clone();
        assert_eq!(status.listen_addr, Some("127.0.0.1:49501".to_owned()));
    }
}
