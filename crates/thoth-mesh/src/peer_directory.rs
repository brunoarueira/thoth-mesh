//! Tracks every peer this node has ever learned a dialable address
//! for - whether from directly completing a handshake with it, or
//! from a peer gossiping about it - so a node can discover and dial
//! peers it was never directly configured with. See ADR-0015. Capped
//! (see ADR-0025) so a long-running node doesn't remember every peer
//! it's ever heard of forever - a peer that keeps getting talked
//! about stays fresh, one that stops aging toward eviction.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use thoth_mesh_core::PeerId;

/// How many peers [`PeerDirectory`] remembers before the
/// least-recently-recorded one is reclaimed (see ADR-0025). Unlike
/// [`Membership`](crate::Membership), there's no connected/disconnected
/// distinction here at all - every known peer is an eviction
/// candidate, refreshed each time it's re-recorded. Not currently
/// configurable via a CLI flag.
pub const DEFAULT_PEER_DIRECTORY_CAPACITY: usize = 4096;

/// `known` and `order` live under one lock, not two - avoids any
/// lock-ordering question between the map and its eviction queue (see
/// ADR-0025).
#[derive(Debug, Default)]
struct DirectoryState {
    known: HashMap<PeerId, String>,
    /// FIFO of peer IDs in touch order - the front is the
    /// least-recently-recorded (and therefore next-to-evict) peer.
    order: VecDeque<PeerId>,
}

/// A thread-safe, cheaply-cloneable registry of every peer known to
/// be dialable and the address to reach it at.
///
/// Deliberately has no concept of "currently connected" - that's
/// `Membership`'s job. This just answers "have I ever recorded this
/// peer before," which is what gates both further gossip propagation
/// and auto-dial (see ADR-0015) - the same way `Interest`'s
/// 0-to-1 transition gates interest propagation (ADR-0011).
#[derive(Debug, Clone)]
pub struct PeerDirectory {
    state: Arc<Mutex<DirectoryState>>,
    evictions: Arc<AtomicU64>,
}

impl Default for PeerDirectory {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(DirectoryState::default())),
            evictions: Arc::new(AtomicU64::new(0)),
        }
    }
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
    ///
    /// Either way, `peer_id` becomes the most-recently-touched entry;
    /// if a fresh recording pushes the registry over
    /// [`DEFAULT_PEER_DIRECTORY_CAPACITY`], the least-recently-touched
    /// entry is reclaimed (see ADR-0025).
    pub fn record(&self, peer_id: PeerId, listen_addr: String) -> bool {
        let mut state = self.state.lock().unwrap();
        let is_new = !state.known.contains_key(&peer_id);
        state.known.insert(peer_id, listen_addr);
        if is_new {
            state.order.push_back(peer_id);
        } else {
            // Move to the back - there's nothing to remove for a
            // genuinely new peer, so this scan only runs on a repeat
            // recording.
            state.order.retain(|id| *id != peer_id);
            state.order.push_back(peer_id);
        }
        if state.order.len() > DEFAULT_PEER_DIRECTORY_CAPACITY {
            if let Some(oldest) = state.order.pop_front() {
                state.known.remove(&oldest);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        is_new
    }

    /// Every peer currently known, except `exclude` - used to catch a
    /// newly-linked peer up on everyone else this node already knows
    /// about, without telling it about itself.
    pub fn snapshot_excluding(&self, exclude: PeerId) -> Vec<(PeerId, String)> {
        self.state
            .lock()
            .unwrap()
            .known
            .iter()
            .filter(|(peer_id, _)| **peer_id != exclude)
            .map(|(peer_id, listen_addr)| (*peer_id, listen_addr.clone()))
            .collect()
    }

    /// How many entries have been reclaimed for sitting over
    /// [`DEFAULT_PEER_DIRECTORY_CAPACITY`] (see ADR-0025).
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
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

    #[test]
    fn entries_over_capacity_evict_the_oldest() {
        let directory = PeerDirectory::new();
        let evicted = PeerId::new();
        directory.record(evicted, "127.0.0.1:1".to_owned());

        // One more peer than the cap holds - `evicted` was recorded
        // first and never touched again, so it's the one reclaimed.
        for i in 0..DEFAULT_PEER_DIRECTORY_CAPACITY {
            directory.record(PeerId::new(), format!("127.0.0.1:{}", i + 2));
        }

        assert_eq!(directory.evictions(), 1);
        assert!(
            directory
                .snapshot_excluding(PeerId::new())
                .iter()
                .all(|(peer_id, _)| *peer_id != evicted)
        );
    }

    #[test]
    fn re_recording_an_entry_protects_it_from_being_the_next_eviction() {
        let directory = PeerDirectory::new();
        let refreshed = PeerId::new();
        directory.record(refreshed, "127.0.0.1:1".to_owned());

        // Fill to just below capacity with other peers.
        for i in 0..DEFAULT_PEER_DIRECTORY_CAPACITY - 1 {
            directory.record(PeerId::new(), format!("127.0.0.1:{}", i + 2));
        }
        assert_eq!(directory.evictions(), 0);

        // Touch `refreshed` again, moving it to the back of the queue
        // - the peer recorded right after it (still untouched since
        // its own insert) is now the least-recently-touched entry
        // instead.
        directory.record(refreshed, "127.0.0.1:99".to_owned());
        directory.record(PeerId::new(), "127.0.0.1:100".to_owned());

        assert_eq!(directory.evictions(), 1);
        let snapshot = directory.snapshot_excluding(PeerId::new());
        assert!(
            snapshot
                .iter()
                .any(|(peer_id, addr)| *peer_id == refreshed && addr == "127.0.0.1:99"),
            "refreshed should still be known, with its latest address"
        );
    }
}
