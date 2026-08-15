//! Registry of currently active peer links' outgoing channels, so
//! local topic-interest changes (see `thoth_mesh::Interest`) can be
//! pushed straight out to every peer as soon as they happen, rather
//! than waiting for that connection's task to have something else to
//! send. See ADR-0011.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use thoth_mesh_core::{Envelope, PeerId};
use tokio::sync::mpsc;

/// A thread-safe, cheaply-cloneable registry mapping a currently
/// connected peer's ID to the sending half of its connection's
/// outgoing channel.
#[derive(Debug, Default, Clone)]
pub struct PeerLinks {
    links: Arc<Mutex<HashMap<PeerId, mpsc::Sender<Arc<Envelope>>>>>,
}

impl PeerLinks {
    /// An empty registry, with no peer links registered yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `peer_id`'s outgoing channel, replacing any previous
    /// one for the same ID (e.g. a reconnect).
    pub fn register(&self, peer_id: PeerId, sender: mpsc::Sender<Arc<Envelope>>) {
        self.links.lock().unwrap().insert(peer_id, sender);
    }

    /// Removes `peer_id`'s entry, but only if it still points at
    /// `sender` - guards against a stale disconnect clobbering a
    /// newer reconnect's entry for the same peer.
    pub fn unregister(&self, peer_id: PeerId, sender: &mpsc::Sender<Arc<Envelope>>) {
        let mut links = self.links.lock().unwrap();
        if links
            .get(&peer_id)
            .is_some_and(|current| current.same_channel(sender))
        {
            links.remove(&peer_id);
        }
    }

    /// Sends `envelope` to every currently registered peer link.
    /// Best-effort: a link whose channel is full or already closed is
    /// simply skipped rather than awaited or retried - it's already
    /// disconnecting or badly backed up, and it gets caught up on
    /// current interest from scratch the next time it (re)connects.
    pub fn broadcast(&self, envelope: Arc<Envelope>) {
        let senders: Vec<_> = self.links.lock().unwrap().values().cloned().collect();
        for sender in senders {
            let _ = sender.try_send(envelope.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thoth_mesh_core::MessageKind;

    fn envelope() -> Arc<Envelope> {
        Arc::new(Envelope::new(
            PeerId::new(),
            MessageKind::Hello { listen_addr: None },
        ))
    }

    #[tokio::test]
    async fn broadcast_reaches_every_registered_link() {
        let links = PeerLinks::new();
        let (tx_a, mut rx_a) = mpsc::channel(4);
        let (tx_b, mut rx_b) = mpsc::channel(4);
        links.register(PeerId::new(), tx_a);
        links.register(PeerId::new(), tx_b);

        let sent = envelope();
        links.broadcast(sent.clone());

        assert_eq!(rx_a.try_recv().unwrap().id, sent.id);
        assert_eq!(rx_b.try_recv().unwrap().id, sent.id);
    }

    #[tokio::test]
    async fn unregister_only_removes_a_matching_sender() {
        let links = PeerLinks::new();
        let peer_id = PeerId::new();
        let (tx_old, _rx_old) = mpsc::channel(4);
        let (tx_new, mut rx_new) = mpsc::channel(4);
        links.register(peer_id, tx_old.clone());
        // Simulate a reconnect racing with the old connection's
        // teardown: the new link replaces the old one in the
        // registry...
        links.register(peer_id, tx_new);
        // ...so the old connection's own teardown, unregistering with
        // its own (now-stale) sender, must not remove the new entry.
        links.unregister(peer_id, &tx_old);

        let sent = envelope();
        links.broadcast(sent.clone());
        assert_eq!(rx_new.try_recv().unwrap().id, sent.id);
    }

    #[tokio::test]
    async fn unregister_removes_a_matching_sender() {
        let links = PeerLinks::new();
        let peer_id = PeerId::new();
        let (tx, mut rx) = mpsc::channel(4);
        links.register(peer_id, tx.clone());
        links.unregister(peer_id, &tx);

        links.broadcast(envelope());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn broadcast_skips_a_closed_channel_without_panicking() {
        let links = PeerLinks::new();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        links.register(PeerId::new(), tx);

        links.broadcast(envelope());
    }
}
