//! Registry of currently active peer links' outgoing channels, so
//! local topic-interest changes (see `thoth_mesh::Interest`) can be
//! pushed straight out to every peer as soon as they happen, rather
//! than waiting for that connection's task to have something else to
//! send. See ADR-0011.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use thoth_mesh_core::{Envelope, PeerId, Topic, TopicFilter};
use tokio::sync::mpsc;

use crate::peer_topic_filter::PeerTopicFilter;
use crate::topic_acl::Principal;

/// One registered peer link: its outgoing channel, alongside its own
/// authenticated [`Principal`] - the same fingerprint-or-anonymous
/// identity `--peer-topic-acl`/`--allow-peer` already key on, and what
/// [`PeerLinks::broadcast_interest`] checks against a
/// `--peer-topic-filter` (ADR-0049).
#[derive(Debug, Clone)]
struct Link {
    sender: mpsc::Sender<Arc<Envelope>>,
    principal: Principal,
}

/// A thread-safe, cheaply-cloneable registry mapping a currently
/// connected peer's ID to its [`Link`].
#[derive(Debug, Default, Clone)]
pub struct PeerLinks {
    links: Arc<Mutex<HashMap<PeerId, Link>>>,
}

impl PeerLinks {
    /// An empty registry, with no peer links registered yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `peer_id`'s outgoing channel and authenticated
    /// identity, replacing any previous entry for the same ID (e.g. a
    /// reconnect).
    pub fn register(
        &self,
        peer_id: PeerId,
        sender: mpsc::Sender<Arc<Envelope>>,
        principal: Principal,
    ) {
        self.links
            .lock()
            .unwrap()
            .insert(peer_id, Link { sender, principal });
    }

    /// Removes `peer_id`'s entry, but only if it still points at
    /// `sender` - guards against a stale disconnect clobbering a
    /// newer reconnect's entry for the same peer.
    pub fn unregister(&self, peer_id: PeerId, sender: &mpsc::Sender<Arc<Envelope>>) {
        let mut links = self.links.lock().unwrap();
        if links
            .get(&peer_id)
            .is_some_and(|link| link.sender.same_channel(sender))
        {
            links.remove(&peer_id);
        }
    }

    /// Sends `envelope` to every currently registered peer link.
    /// Best-effort: a link whose channel is full or already closed is
    /// simply skipped rather than awaited or retried - it's already
    /// disconnecting or badly backed up, and it gets caught up on
    /// current interest from scratch the next time it (re)connects.
    ///
    /// Unfiltered by design - used for peer-discovery gossip
    /// (`PeerAnnounce`, ADR-0015), an orthogonal concern from topic
    /// relay that `--peer-topic-filter` has no reason to gate. See
    /// [`broadcast_interest`](Self::broadcast_interest) for the one
    /// that *is* filtered.
    pub fn broadcast(&self, envelope: Arc<Envelope>) {
        let senders: Vec<_> = self
            .links
            .lock()
            .unwrap()
            .values()
            .map(|link| link.sender.clone())
            .collect();
        for sender in senders {
            let _ = sender.try_send(envelope.clone());
        }
    }

    /// Like [`broadcast`](Self::broadcast), for a `Subscribe`/
    /// `Unsubscribe` propagating interest in `filter` (ADR-0011) - but
    /// a peer link is skipped if `peer_topic_filter` is configured
    /// and doesn't permit relaying `filter` to that link's own
    /// identity (ADR-0049). `peer_topic_filter: None` (nothing
    /// configured at all) falls straight through to `broadcast`,
    /// unchanged from before this ADR. A wildcard `filter` (no single
    /// `Topic` behind it) is never relayed to any link a
    /// `--peer-topic-filter` applies to at all - the same conservative
    /// stance `filter_acl_permits` already takes wherever an ACL meets
    /// a wildcard.
    pub fn broadcast_interest(
        &self,
        envelope: Arc<Envelope>,
        filter: &TopicFilter,
        peer_topic_filter: Option<&PeerTopicFilter>,
    ) {
        let Some(peer_topic_filter) = peer_topic_filter else {
            return self.broadcast(envelope);
        };
        let topic: Option<Topic> = filter.as_topic();
        let senders: Vec<_> = self
            .links
            .lock()
            .unwrap()
            .values()
            .filter(|link| {
                topic
                    .as_ref()
                    .is_some_and(|topic| peer_topic_filter.permits(link.principal, topic))
            })
            .map(|link| link.sender.clone())
            .collect();
        for sender in senders {
            let _ = sender.try_send(envelope.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use thoth_mesh_core::MessageKind;

    fn envelope() -> Arc<Envelope> {
        Arc::new(Envelope::new(
            PeerId::new(),
            MessageKind::Hello { listen_addr: None },
        ))
    }

    fn subscribe_envelope(filter: TopicFilter) -> Arc<Envelope> {
        Arc::new(Envelope::new(
            PeerId::new(),
            MessageKind::Subscribe {
                filter,
                ack: false,
                group: None,
                durable: false,
            },
        ))
    }

    #[tokio::test]
    async fn broadcast_reaches_every_registered_link() {
        let links = PeerLinks::new();
        let (tx_a, mut rx_a) = mpsc::channel(4);
        let (tx_b, mut rx_b) = mpsc::channel(4);
        links.register(PeerId::new(), tx_a, Principal::Anonymous);
        links.register(PeerId::new(), tx_b, Principal::Anonymous);

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
        links.register(peer_id, tx_old.clone(), Principal::Anonymous);
        // Simulate a reconnect racing with the old connection's
        // teardown: the new link replaces the old one in the
        // registry...
        links.register(peer_id, tx_new, Principal::Anonymous);
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
        links.register(peer_id, tx.clone(), Principal::Anonymous);
        links.unregister(peer_id, &tx);

        links.broadcast(envelope());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn broadcast_skips_a_closed_channel_without_panicking() {
        let links = PeerLinks::new();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        links.register(PeerId::new(), tx, Principal::Anonymous);

        links.broadcast(envelope());
    }

    #[tokio::test]
    async fn broadcast_interest_with_no_filter_configured_reaches_everyone() {
        let links = PeerLinks::new();
        let (tx, mut rx) = mpsc::channel(4);
        links.register(PeerId::new(), tx, Principal::Fingerprint([1; 32]));

        let filter = TopicFilter::from_str("weather.updates").unwrap();
        let sent = subscribe_envelope(filter.clone());
        links.broadcast_interest(sent.clone(), &filter, None);

        assert_eq!(rx.try_recv().unwrap().id, sent.id);
    }

    #[tokio::test]
    async fn broadcast_interest_skips_a_link_the_filter_does_not_permit() {
        let links = PeerLinks::new();
        let (tx, mut rx) = mpsc::channel(4);
        links.register(PeerId::new(), tx, Principal::Fingerprint([1; 32]));

        let permitted = TopicFilter::from_str("weather.updates").unwrap();
        let filter = TopicFilter::from_str("traffic.updates").unwrap();
        let peer_topic_filter =
            PeerTopicFilter::parse([format!("{}|weather.updates", "01".repeat(32)).as_str()])
                .unwrap();

        links.broadcast_interest(
            subscribe_envelope(filter.clone()),
            &filter,
            Some(&peer_topic_filter),
        );
        assert!(rx.try_recv().is_err());

        // The permitted filter still gets through on the same link.
        let sent = subscribe_envelope(permitted.clone());
        links.broadcast_interest(sent.clone(), &permitted, Some(&peer_topic_filter));
        assert_eq!(rx.try_recv().unwrap().id, sent.id);
    }

    #[tokio::test]
    async fn broadcast_interest_never_relays_a_wildcard_filter_once_any_filter_is_configured() {
        let links = PeerLinks::new();
        let (tx, mut rx) = mpsc::channel(4);
        let fingerprint = [1; 32];
        links.register(PeerId::new(), tx, Principal::Fingerprint(fingerprint));

        // Even an entry that would seem to cover it doesn't help - no
        // pattern-vs-pattern matching (ADR-0049).
        let peer_topic_filter =
            PeerTopicFilter::parse([format!("{}|weather.updates", "01".repeat(32)).as_str()])
                .unwrap();
        let wildcard = TopicFilter::from_str("weather.+").unwrap();

        links.broadcast_interest(
            subscribe_envelope(wildcard.clone()),
            &wildcard,
            Some(&peer_topic_filter),
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn broadcast_interest_distinguishes_links_by_their_own_principal() {
        let links = PeerLinks::new();
        let (tx_a, mut rx_a) = mpsc::channel(4);
        let (tx_b, mut rx_b) = mpsc::channel(4);
        let fp_a = [1; 32];
        let fp_b = [2; 32];
        links.register(PeerId::new(), tx_a, Principal::Fingerprint(fp_a));
        links.register(PeerId::new(), tx_b, Principal::Fingerprint(fp_b));

        let peer_topic_filter =
            PeerTopicFilter::parse([format!("{}|weather.updates", "01".repeat(32)).as_str()])
                .unwrap();
        let filter = TopicFilter::from_str("weather.updates").unwrap();
        let sent = subscribe_envelope(filter.clone());
        links.broadcast_interest(sent.clone(), &filter, Some(&peer_topic_filter));

        assert_eq!(rx_a.try_recv().unwrap().id, sent.id);
        assert!(rx_b.try_recv().is_err());
    }
}
