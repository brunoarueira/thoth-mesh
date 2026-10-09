//! Registry of currently active peer links' outgoing channels, so
//! local topic-interest changes (see `thoth_mesh::Interest`) can be
//! pushed straight out to every peer as soon as they happen, rather
//! than waiting for that connection's task to have something else to
//! send. See ADR-0011.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use thoth_mesh::Interest;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic, TopicFilter};
use tokio::sync::{Notify, mpsc};

use crate::peer_topic_filter::PeerTopicFilter;
use crate::topic_acl::Principal;

/// One registered peer link: its outgoing channel, alongside its own
/// authenticated [`Principal`] - the same fingerprint-or-anonymous
/// identity `--peer-topic-acl`/`--allow-peer` already key on, and what
/// [`PeerLinks::broadcast_interest`] checks against a
/// `--peer-topic-filter` (ADR-0049). `disconnect` is this link's
/// connection task's own signal to end itself - see
/// [`PeerLinks::disconnect_unless`] and ADR-0057.
#[derive(Debug, Clone)]
struct Link {
    sender: mpsc::Sender<Arc<Envelope>>,
    principal: Principal,
    disconnect: Arc<Notify>,
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

    /// Registers `peer_id`'s outgoing channel, authenticated identity,
    /// and its connection's own disconnect signal (ADR-0057),
    /// replacing any previous entry for the same ID (e.g. a
    /// reconnect).
    pub fn register(
        &self,
        peer_id: PeerId,
        sender: mpsc::Sender<Arc<Envelope>>,
        principal: Principal,
        disconnect: Arc<Notify>,
    ) {
        self.links.lock().unwrap().insert(
            peer_id,
            Link {
                sender,
                principal,
                disconnect,
            },
        );
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

    /// Signals every currently registered peer link whose own
    /// `Principal` `still_allowed` rejects to disconnect itself - used
    /// when `--allow-peer` tightens on a config reload (ADR-0057).
    /// `still_allowed` returning `true` for everything (e.g. no
    /// `--allow-peer` configured at all) disconnects nobody, the same
    /// "no list, no restriction" rule `allowlist_permits` already
    /// follows at the initial handshake.
    ///
    /// Best-effort, same as `broadcast`: a link that's already
    /// disconnecting on its own races this harmlessly - `Notify`
    /// doesn't need a live receiver to call `notify_one` on, and a
    /// connection that's already torn itself down simply never reads
    /// it.
    pub fn disconnect_unless(&self, still_allowed: impl Fn(Principal) -> bool) {
        let notifies: Vec<_> = self
            .links
            .lock()
            .unwrap()
            .values()
            .filter(|link| !still_allowed(link.principal))
            .map(|link| Arc::clone(&link.disconnect))
            .collect();
        for notify in notifies {
            notify.notify_one();
        }
    }

    /// Reconciles every currently registered peer link's own view of
    /// this node's aggregate interest against a freshly reloaded
    /// `--peer-topic-filter` (ADR-0057/ADR-0049) - no connection-level
    /// signal needed at all, unlike `disconnect_unless` above, since
    /// this only ever re-runs the *push* side of interest propagation
    /// (ADR-0011), which already lives entirely here.
    ///
    /// For each link, under the *new* `peer_topic_filter`, partitions
    /// the current `interest` snapshot into what that link's own
    /// `Principal` is still permitted to hear about (announced via a
    /// `Subscribe`-shaped interest-announce, same shape
    /// `register_peer_link`'s own catch-up uses) and what it no
    /// longer is (withdrawn via an `Unsubscribe`-shaped one).
    ///
    /// No history of what was previously told to any link needs
    /// tracking, and this runs unconditionally on every reload, not
    /// just one that actually changed `peer_topic_filter` - both
    /// kinds are idempotent from the receiving peer's own
    /// perspective (duplicate interest propagation is already
    /// possible by design, in the gossip-and-mesh reality
    /// `thoth-mesh` discovers peers in; a peer told to unsubscribe
    /// from something it was never told to subscribe to is a no-op),
    /// so recomputing the whole set fresh every time is simpler than
    /// diffing against a remembered history, and correctly re-grants
    /// whatever a *removed* filter should newly permit again -
    /// something a "only reconcile if the filter is Some" short
    /// circuit would otherwise miss entirely.
    pub fn reconcile_interest(
        &self,
        interest: &Interest,
        node_id: PeerId,
        peer_topic_filter: Option<&PeerTopicFilter>,
    ) {
        let snapshot = interest.snapshot();
        let links: Vec<(Principal, mpsc::Sender<Arc<Envelope>>)> = self
            .links
            .lock()
            .unwrap()
            .values()
            .map(|link| (link.principal, link.sender.clone()))
            .collect();
        for (principal, sender) in links {
            for filter in &snapshot {
                let permitted = match peer_topic_filter {
                    None => true,
                    Some(peer_topic_filter) => filter
                        .as_topic()
                        .is_some_and(|topic| peer_topic_filter.permits(principal, &topic)),
                };
                let kind = if permitted {
                    MessageKind::Subscribe {
                        filter: filter.clone(),
                        ack: false,
                        group: None,
                        durable: false,
                    }
                } else {
                    MessageKind::Unsubscribe {
                        filter: filter.clone(),
                    }
                };
                let _ = sender.try_send(Arc::new(Envelope::new(node_id, kind)));
            }
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
        links.register(
            PeerId::new(),
            tx_a,
            Principal::Anonymous,
            Arc::new(Notify::new()),
        );
        links.register(
            PeerId::new(),
            tx_b,
            Principal::Anonymous,
            Arc::new(Notify::new()),
        );

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
        links.register(
            peer_id,
            tx_old.clone(),
            Principal::Anonymous,
            Arc::new(Notify::new()),
        );
        // Simulate a reconnect racing with the old connection's
        // teardown: the new link replaces the old one in the
        // registry...
        links.register(
            peer_id,
            tx_new,
            Principal::Anonymous,
            Arc::new(Notify::new()),
        );
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
        links.register(
            peer_id,
            tx.clone(),
            Principal::Anonymous,
            Arc::new(Notify::new()),
        );
        links.unregister(peer_id, &tx);

        links.broadcast(envelope());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn broadcast_skips_a_closed_channel_without_panicking() {
        let links = PeerLinks::new();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        links.register(
            PeerId::new(),
            tx,
            Principal::Anonymous,
            Arc::new(Notify::new()),
        );

        links.broadcast(envelope());
    }

    #[tokio::test]
    async fn broadcast_interest_with_no_filter_configured_reaches_everyone() {
        let links = PeerLinks::new();
        let (tx, mut rx) = mpsc::channel(4);
        links.register(
            PeerId::new(),
            tx,
            Principal::Fingerprint([1; 32]),
            Arc::new(Notify::new()),
        );

        let filter = TopicFilter::from_str("weather.updates").unwrap();
        let sent = subscribe_envelope(filter.clone());
        links.broadcast_interest(sent.clone(), &filter, None);

        assert_eq!(rx.try_recv().unwrap().id, sent.id);
    }

    #[tokio::test]
    async fn broadcast_interest_skips_a_link_the_filter_does_not_permit() {
        let links = PeerLinks::new();
        let (tx, mut rx) = mpsc::channel(4);
        links.register(
            PeerId::new(),
            tx,
            Principal::Fingerprint([1; 32]),
            Arc::new(Notify::new()),
        );

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
        links.register(
            PeerId::new(),
            tx,
            Principal::Fingerprint(fingerprint),
            Arc::new(Notify::new()),
        );

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
        links.register(
            PeerId::new(),
            tx_a,
            Principal::Fingerprint(fp_a),
            Arc::new(Notify::new()),
        );
        links.register(
            PeerId::new(),
            tx_b,
            Principal::Fingerprint(fp_b),
            Arc::new(Notify::new()),
        );

        let peer_topic_filter =
            PeerTopicFilter::parse([format!("{}|weather.updates", "01".repeat(32)).as_str()])
                .unwrap();
        let filter = TopicFilter::from_str("weather.updates").unwrap();
        let sent = subscribe_envelope(filter.clone());
        links.broadcast_interest(sent.clone(), &filter, Some(&peer_topic_filter));

        assert_eq!(rx_a.try_recv().unwrap().id, sent.id);
        assert!(rx_b.try_recv().is_err());
    }

    #[tokio::test]
    async fn disconnect_unless_notifies_only_the_links_it_rejects() {
        let links = PeerLinks::new();
        let (tx_a, _rx_a) = mpsc::channel(4);
        let (tx_b, _rx_b) = mpsc::channel(4);
        let fp_a = [1; 32];
        let fp_b = [2; 32];
        let disconnect_a = Arc::new(Notify::new());
        let disconnect_b = Arc::new(Notify::new());
        links.register(
            PeerId::new(),
            tx_a,
            Principal::Fingerprint(fp_a),
            Arc::clone(&disconnect_a),
        );
        links.register(
            PeerId::new(),
            tx_b,
            Principal::Fingerprint(fp_b),
            Arc::clone(&disconnect_b),
        );

        links.disconnect_unless(|principal| principal == Principal::Fingerprint(fp_a));

        // fp_a is still allowed - its own Notify never fires.
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                disconnect_a.notified()
            )
            .await
            .is_err()
        );
        // fp_b isn't - its Notify fires right away.
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            disconnect_b.notified(),
        )
        .await
        .expect("fp_b's disconnect signal should have fired");
    }

    #[tokio::test]
    async fn disconnect_unless_leaves_everyone_when_nothing_is_rejected() {
        let links = PeerLinks::new();
        let (tx, _rx) = mpsc::channel(4);
        let disconnect = Arc::new(Notify::new());
        links.register(
            PeerId::new(),
            tx,
            Principal::Fingerprint([1; 32]),
            Arc::clone(&disconnect),
        );

        // The "no --allow-peer configured at all" case: every
        // principal is still allowed.
        links.disconnect_unless(|_principal| true);

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), disconnect.notified())
                .await
                .is_err()
        );
    }
}
