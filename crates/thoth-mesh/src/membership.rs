//! Tracks which configured peers are currently reachable, updated as
//! peer links come up and go down. See issue #24. Disconnected
//! entries are capped (see ADR-0025) so a long-running node doesn't
//! remember every peer it's ever seen forever - a currently-connected
//! peer is never a candidate.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use thoth_mesh_core::PeerId;

/// How many *disconnected* peer entries [`Membership`] keeps before
/// the oldest is reclaimed (see ADR-0025) - a currently-connected
/// peer's entry is never a candidate, since that count is already
/// bounded by real open sockets, not something this cap needs to
/// protect against. Not currently configurable via a CLI flag.
pub const DEFAULT_MEMBERSHIP_DISCONNECTED_CAPACITY: usize = 4096;

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

/// `peers` and `disconnected_order` live under one lock, not two -
/// avoids any lock-ordering question between the map and its eviction
/// queue (see ADR-0025).
#[derive(Debug, Default)]
struct MembershipState {
    peers: HashMap<PeerId, PeerStatus>,
    /// FIFO of peer IDs in the order they most recently transitioned
    /// to disconnected - eviction candidates. Everything queued here
    /// is guaranteed to currently be disconnected: `mark_connected`
    /// removes a peer's entry the moment it reconnects, so popping the
    /// front and removing it from `peers` is always safe, with no
    /// lazy-skip check needed at eviction time.
    disconnected_order: VecDeque<PeerId>,
}

/// A thread-safe, cheaply-cloneable registry of peer connection state,
/// shared across every connection task (inbound and outbound) on a
/// node.
///
/// Plain `std::sync::Mutex` rather than an async lock: entries are
/// only ever held for the duration of a `HashMap`/`VecDeque`
/// operation, never across an `.await`.
#[derive(Debug, Clone)]
pub struct Membership {
    state: Arc<Mutex<MembershipState>>,
    disconnected_evictions: Arc<AtomicU64>,
}

impl Default for Membership {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(MembershipState::default())),
            disconnected_evictions: Arc::new(AtomicU64::new(0)),
        }
    }
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
    /// listen address. Also clears `peer_id` from the disconnected-
    /// eviction queue, if it was there - a peer that's reconnected is
    /// no longer an eviction candidate at all (see ADR-0025).
    pub fn mark_connected(&self, peer_id: PeerId, listen_addr: Option<String>) {
        let mut state = self.state.lock().unwrap();
        let was_connected = state
            .peers
            .get(&peer_id)
            .is_some_and(|status| status.connected);
        state.peers.insert(
            peer_id,
            PeerStatus {
                connected: true,
                listen_addr,
            },
        );
        state.disconnected_order.retain(|id| *id != peer_id);
        // Release the lock before logging - tracing::info! isn't
        // free, and there's no reason to hold the mutex while it runs.
        drop(state);
        if !was_connected {
            tracing::info!(?peer_id, "peer up");
        }
    }

    /// Records `peer_id` as disconnected. Call when its connection
    /// drops (read error/EOF on either an outbound-dialed or
    /// inbound-accepted link).
    ///
    /// A no-op, with no log line, for a peer that was never connected
    /// (or is already marked disconnected). On a genuine
    /// connected-to-disconnected transition, `peer_id` becomes the
    /// most recent entry in the disconnected-eviction queue; if that
    /// pushes the queue over
    /// [`DEFAULT_MEMBERSHIP_DISCONNECTED_CAPACITY`], the
    /// longest-disconnected peer's entry is reclaimed (see ADR-0025).
    pub fn mark_disconnected(&self, peer_id: PeerId) {
        let mut state = self.state.lock().unwrap();
        let Some(status) = state.peers.get_mut(&peer_id) else {
            return;
        };
        let was_connected = status.connected;
        status.connected = false;
        if was_connected {
            state.disconnected_order.push_back(peer_id);
            if state.disconnected_order.len() > DEFAULT_MEMBERSHIP_DISCONNECTED_CAPACITY {
                if let Some(oldest) = state.disconnected_order.pop_front() {
                    state.peers.remove(&oldest);
                    self.disconnected_evictions.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        // Same as in mark_connected: don't hold the lock while logging.
        drop(state);
        if was_connected {
            tracing::info!(?peer_id, "peer down");
        }
    }

    /// Whether `peer_id` is currently connected.
    pub fn is_reachable(&self, peer_id: PeerId) -> bool {
        self.state
            .lock()
            .unwrap()
            .peers
            .get(&peer_id)
            .is_some_and(|status| status.connected)
    }

    /// A snapshot of every peer this node currently remembers (up to
    /// [`DEFAULT_MEMBERSHIP_DISCONNECTED_CAPACITY`] disconnected ones,
    /// plus every currently-connected one), and its last-known state.
    pub fn snapshot(&self) -> HashMap<PeerId, PeerStatus> {
        self.state.lock().unwrap().peers.clone()
    }

    /// How many peers are currently connected. Read live rather than
    /// tracked separately, so it can never drift out of sync with
    /// [`mark_connected`](Self::mark_connected)/
    /// [`mark_disconnected`](Self::mark_disconnected) - see ADR-0013.
    pub fn connected_count(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .peers
            .values()
            .filter(|status| status.connected)
            .count()
    }

    /// How many disconnected peer entries have been reclaimed for
    /// sitting over [`DEFAULT_MEMBERSHIP_DISCONNECTED_CAPACITY`] (see
    /// ADR-0025).
    pub fn disconnected_evictions(&self) -> u64 {
        self.disconnected_evictions.load(Ordering::Relaxed)
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

    #[test]
    fn disconnected_entries_over_capacity_evict_the_oldest() {
        let membership = Membership::new();
        let evicted = PeerId::new();
        membership.mark_connected(evicted, None);
        membership.mark_disconnected(evicted);

        // One more disconnected peer than the cap holds - `evicted`
        // was the first to disconnect, so it's the one reclaimed.
        for _ in 0..DEFAULT_MEMBERSHIP_DISCONNECTED_CAPACITY {
            let peer_id = PeerId::new();
            membership.mark_connected(peer_id, None);
            membership.mark_disconnected(peer_id);
        }

        assert_eq!(membership.disconnected_evictions(), 1);
        assert!(!membership.snapshot().contains_key(&evicted));
    }

    #[test]
    fn a_connected_peer_is_never_evicted_even_over_capacity() {
        let membership = Membership::new();
        let protected = PeerId::new();
        membership.mark_connected(protected, None);

        // Fill (and exceed) the disconnected cap with unrelated peers
        // - none of this should ever touch `protected`, since it's
        // never disconnected.
        for _ in 0..=DEFAULT_MEMBERSHIP_DISCONNECTED_CAPACITY {
            let peer_id = PeerId::new();
            membership.mark_connected(peer_id, None);
            membership.mark_disconnected(peer_id);
        }

        assert!(membership.is_reachable(protected));
    }

    #[test]
    fn reconnecting_removes_a_peer_from_the_eviction_queue() {
        let membership = Membership::new();
        let reconnected = PeerId::new();
        membership.mark_connected(reconnected, None);
        membership.mark_disconnected(reconnected);
        // Back up - no longer an eviction candidate.
        membership.mark_connected(reconnected, None);

        // Fill the disconnected cap with other peers - if
        // `reconnected`'s stale queue entry weren't cleared, this
        // would (incorrectly) evict a peer that's actually still
        // connected once the queue's real size crossed the cap.
        for _ in 0..DEFAULT_MEMBERSHIP_DISCONNECTED_CAPACITY {
            let peer_id = PeerId::new();
            membership.mark_connected(peer_id, None);
            membership.mark_disconnected(peer_id);
        }

        assert_eq!(membership.disconnected_evictions(), 0);
        assert!(membership.is_reachable(reconnected));
    }
}
