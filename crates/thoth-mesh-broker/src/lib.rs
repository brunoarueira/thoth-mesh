//! In-process pub/sub broker: topic registry and subscriber dispatch.
//!
//! See ADR-0006 for the design rationale, ADR-0011 for the
//! duplicate-envelope dedup this crate now also does, ADR-0021 for the
//! per-topic replay buffer that lets a late subscriber catch up on
//! recent history, and ADR-0022 for wildcard topic filters.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use thoth_mesh_core::{Envelope, MessageId, Topic, TopicFilter};
use tokio::sync::{RwLock, broadcast};

/// Default channel capacity for a topic's broadcast channel.
///
/// Bounds how many envelopes can be buffered for a subscriber before it
/// starts lagging (see [`tokio::sync::broadcast`]'s lag semantics).
pub const DEFAULT_TOPIC_CHANNEL_CAPACITY: usize = 256;

/// How many recently-published message IDs [`Broker`] remembers for
/// duplicate detection (see ADR-0011) - bounded so memory doesn't grow
/// without limit on a long-running node.
pub const DEFAULT_DEDUP_CAPACITY: usize = 4096;

/// How many recent envelopes each topic's replay buffer keeps for a
/// late subscriber to catch up on (see ADR-0021) - sized the same as
/// [`DEFAULT_TOPIC_CHANNEL_CAPACITY`] and, like it, not currently
/// configurable via a CLI flag.
pub const DEFAULT_REPLAY_BUFFER_CAPACITY: usize = 256;

/// An in-process pub/sub broker: routes published envelopes to the
/// subscribers registered for their topic.
///
/// The broker only understands topic-addressed delivery, not envelope
/// semantics - interpreting an incoming message's `MessageKind` and
/// calling [`subscribe`](Broker::subscribe)/[`publish`](Broker::publish)
/// accordingly is the caller's job. The one exception is duplicate
/// detection: every hop of a forwarded envelope keeps its original
/// `MessageId`, and every hop - including the original local publish -
/// already flows through [`publish`](Broker::publish), which makes this
/// the natural place to stop an envelope that's looped back around a
/// cyclic peer mesh from circulating forever (see ADR-0011).
#[derive(Debug)]
pub struct Broker {
    topics: RwLock<HashMap<Topic, Arc<TopicChannel>>>,
    /// Wildcard filter subscriptions (ADR-0022) - kept separate from
    /// `topics` so the exact-match path above is completely unchanged
    /// (same type, same lookup, same cost) for the common
    /// non-wildcard case.
    patterns: RwLock<HashMap<TopicFilter, Arc<TopicChannel>>>,
    seen: Mutex<SeenIds>,
    messages_published: AtomicU64,
}

impl Default for Broker {
    fn default() -> Self {
        Self {
            topics: RwLock::default(),
            patterns: RwLock::default(),
            seen: Mutex::new(SeenIds::new(DEFAULT_DEDUP_CAPACITY)),
            messages_published: AtomicU64::new(0),
        }
    }
}

impl Broker {
    /// Creates a new, empty broker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribes to `filter`, returning its current replay backlog
    /// (oldest first, see ADR-0021) alongside a receiver that yields
    /// every envelope published to a topic `filter` matches, from this
    /// point on.
    ///
    /// A literal `filter` (see [`TopicFilter::is_literal`]) is routed
    /// to the same exact-match registry as before ADR-0022, with no
    /// behavior change; a genuine pattern gets its own
    /// [`TopicChannel`], keyed on the filter itself.
    ///
    /// The backlog and the receiver together cover every matching
    /// envelope published from the moment this call resolves, with no
    /// gap and no duplicate - see [`TopicChannel::subscribe`] for why
    /// that's guaranteed even against a concurrent `publish`.
    ///
    /// Unsubscribing is just dropping the returned receiver.
    pub async fn subscribe(
        &self,
        filter: TopicFilter,
    ) -> (Vec<Arc<Envelope>>, broadcast::Receiver<Arc<Envelope>>) {
        let channel = match filter.as_topic() {
            Some(topic) => {
                let mut topics = self.topics.write().await;
                Arc::clone(
                    topics
                        .entry(topic)
                        .or_insert_with(|| Arc::new(TopicChannel::new())),
                )
            }
            None => {
                let mut patterns = self.patterns.write().await;
                Arc::clone(
                    patterns
                        .entry(filter)
                        .or_insert_with(|| Arc::new(TopicChannel::new())),
                )
            }
        };
        channel.subscribe()
    }

    /// Publishes `envelope` to every subscriber currently registered
    /// for `topic` - exact-match subscribers and every currently
    /// registered pattern filter that matches `topic` (ADR-0022) -
    /// returning how many subscribers received it live in total.
    ///
    /// `envelope` is also appended to each matching channel's replay
    /// buffer (ADR-0021) regardless of whether anyone is currently
    /// subscribed there - a topic (or pattern) with no subscribers yet
    /// still builds up a backlog for whoever subscribes later. A
    /// connection holding both an exact subscribe and an independently
    /// matching pattern subscribe receives the envelope twice, once
    /// per subscription - each is delivered through its own
    /// `TopicChannel`, same as two distinct clients would be. Returns
    /// `0` if there are no live subscribers right now - this is not an
    /// error, publishing to a topic nobody is listening to is normal -
    /// or if an envelope with this same `MessageId` has already been
    /// published here before, which is dropped rather than redelivered
    /// or re-buffered (see ADR-0011).
    pub async fn publish(&self, topic: &Topic, envelope: Arc<Envelope>) -> usize {
        let is_new = self.seen.lock().unwrap().record(envelope.id);
        if !is_new {
            return 0;
        }
        self.messages_published.fetch_add(1, Ordering::Relaxed);

        let exact_channel = {
            let mut topics = self.topics.write().await;
            Arc::clone(
                topics
                    .entry(topic.clone())
                    .or_insert_with(|| Arc::new(TopicChannel::new())),
            )
        };
        let mut delivered = exact_channel.publish(Arc::clone(&envelope));

        // Every *currently registered* pattern is checked against
        // `topic` on each publish - O(number of distinct active
        // patterns), not O(subscribers), since same-pattern
        // subscribers already share one `TopicChannel`. A linear scan
        // is the deliberate v1 answer (see ADR-0022); a prefix index
        // is worth it only once this is shown to matter at scale.
        let patterns = self.patterns.read().await;
        for (filter, channel) in patterns.iter() {
            if filter.matches(topic) {
                delivered += channel.publish(Arc::clone(&envelope));
            }
        }
        delivered
    }

    /// How many distinct (non-duplicate) envelopes have been published
    /// through this broker since it was created - see ADR-0013.
    pub fn messages_published(&self) -> u64 {
        self.messages_published.load(Ordering::Relaxed)
    }
}

/// One topic's live broadcast channel paired with its bounded replay
/// buffer (ADR-0021), guarded by the same lock so a new subscriber's
/// backlog snapshot and its receiver registration happen as one atomic
/// step relative to a concurrent [`publish`](TopicChannel::publish).
#[derive(Debug)]
struct TopicChannel {
    sender: broadcast::Sender<Arc<Envelope>>,
    buffer: Mutex<VecDeque<Arc<Envelope>>>,
}

impl TopicChannel {
    fn new() -> Self {
        Self {
            sender: broadcast::channel(DEFAULT_TOPIC_CHANNEL_CAPACITY).0,
            buffer: Mutex::new(VecDeque::with_capacity(DEFAULT_REPLAY_BUFFER_CAPACITY)),
        }
    }

    /// Registers a new receiver and snapshots the current replay
    /// buffer under the same lock, so a concurrent
    /// [`publish`](Self::publish) can never land in neither (a lost
    /// envelope) or both (a duplicate): the two are strictly ordered by
    /// the lock, so whichever runs first completes in full - buffer
    /// push *and* broadcast send - before the other starts. See
    /// ADR-0021.
    fn subscribe(&self) -> (Vec<Arc<Envelope>>, broadcast::Receiver<Arc<Envelope>>) {
        let buffer = self.buffer.lock().unwrap();
        let receiver = self.sender.subscribe();
        (buffer.iter().cloned().collect(), receiver)
    }

    /// Appends `envelope` to the replay buffer (evicting the oldest
    /// entry once over [`DEFAULT_REPLAY_BUFFER_CAPACITY`]) and
    /// broadcasts it to every live receiver, as one critical section
    /// under the same lock [`subscribe`](Self::subscribe) uses.
    fn publish(&self, envelope: Arc<Envelope>) -> usize {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.push_back(Arc::clone(&envelope));
        if buffer.len() > DEFAULT_REPLAY_BUFFER_CAPACITY {
            buffer.pop_front();
        }
        self.sender.send(envelope).unwrap_or(0)
    }
}

/// A bounded FIFO record of recently-seen message IDs: a [`HashSet`]
/// for O(1) membership checks alongside a [`VecDeque`] tracking
/// insertion order, so the oldest ID can be evicted once `capacity` is
/// exceeded.
#[derive(Debug)]
struct SeenIds {
    capacity: usize,
    order: VecDeque<MessageId>,
    set: HashSet<MessageId>,
}

impl SeenIds {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            set: HashSet::with_capacity(capacity),
        }
    }

    /// Records `id` as seen, returning `true` if it hadn't been
    /// recorded before (the caller should proceed) or `false` if it's
    /// a repeat (the caller should drop whatever it was about to do).
    fn record(&mut self, id: MessageId) -> bool {
        if !self.set.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use thoth_mesh_core::{MessageKind, PeerId, TopicFilter};

    fn publish_envelope(topic: &Topic, payload: &[u8]) -> Arc<Envelope> {
        Arc::new(Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: topic.clone(),
                payload: payload.to_vec(),
            },
        ))
    }

    /// Subscribes and discards the replay backlog, for tests that only
    /// care about live delivery - see ADR-0021 for the backlog itself.
    async fn subscribe_live(broker: &Broker, topic: Topic) -> broadcast::Receiver<Arc<Envelope>> {
        broker.subscribe(topic.into()).await.1
    }

    #[tokio::test]
    async fn subscribe_then_publish_delivers_to_subscriber() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut rx = subscribe_live(&broker, topic.clone()).await;

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 1);
        let received = rx.recv().await.unwrap();
        assert_eq!(received, envelope);
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut rx_a = subscribe_live(&broker, topic.clone()).await;
        let mut rx_b = subscribe_live(&broker, topic.clone()).await;

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 2);
        assert_eq!(rx_a.recv().await.unwrap(), envelope);
        assert_eq!(rx_b.recv().await.unwrap(), envelope);
    }

    #[tokio::test]
    async fn publish_with_no_subscribers_returns_zero() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        let envelope = publish_envelope(&topic, b"sunny");
        assert_eq!(broker.publish(&topic, envelope).await, 0);
    }

    #[tokio::test]
    async fn distinct_topics_do_not_cross_deliver() {
        let broker = Broker::new();
        let weather = Topic::from_str("weather.updates").unwrap();
        let traffic = Topic::from_str("traffic.updates").unwrap();
        let mut rx = subscribe_live(&broker, weather.clone()).await;

        let envelope = publish_envelope(&traffic, b"jam");
        let delivered = broker.publish(&traffic, envelope).await;

        assert_eq!(delivered, 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn dropped_receiver_does_not_affect_others() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let rx_a = subscribe_live(&broker, topic.clone()).await;
        let mut rx_b = subscribe_live(&broker, topic.clone()).await;
        drop(rx_a);

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 1);
        assert_eq!(rx_b.recv().await.unwrap(), envelope);
    }

    #[tokio::test]
    async fn publishing_the_same_envelope_twice_only_delivers_once() {
        // Simulates an envelope that's looped back around a cyclic
        // peer mesh and arrived at the same node again with the same
        // MessageId - see ADR-0011.
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut rx = subscribe_live(&broker, topic.clone()).await;

        let envelope = publish_envelope(&topic, b"sunny");
        assert_eq!(broker.publish(&topic, envelope.clone()).await, 1);
        assert_eq!(broker.publish(&topic, envelope.clone()).await, 0);

        assert_eq!(rx.recv().await.unwrap(), envelope);
        assert!(
            rx.try_recv().is_err(),
            "the duplicate should not have been redelivered"
        );
    }

    #[tokio::test]
    async fn messages_published_counts_only_new_envelopes() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        let first = publish_envelope(&topic, b"sunny");
        let second = publish_envelope(&topic, b"cloudy");
        broker.publish(&topic, first.clone()).await;
        broker.publish(&topic, second).await;
        // A duplicate of `first` shouldn't bump the counter again.
        broker.publish(&topic, first).await;

        assert_eq!(broker.messages_published(), 2);
    }

    #[tokio::test]
    async fn distinct_envelopes_on_the_same_topic_both_deliver() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut rx = subscribe_live(&broker, topic.clone()).await;

        let first = publish_envelope(&topic, b"sunny");
        let second = publish_envelope(&topic, b"cloudy");
        assert_eq!(broker.publish(&topic, first.clone()).await, 1);
        assert_eq!(broker.publish(&topic, second.clone()).await, 1);

        assert_eq!(rx.recv().await.unwrap(), first);
        assert_eq!(rx.recv().await.unwrap(), second);
    }

    #[tokio::test]
    async fn a_late_subscriber_is_replayed_a_publish_that_happened_before_it_subscribed() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        // Nobody is subscribed yet when this is published.
        let envelope = publish_envelope(&topic, b"sunny");
        assert_eq!(broker.publish(&topic, envelope.clone()).await, 0);

        // A late subscriber still gets it, via the backlog rather than
        // live delivery.
        let (backlog, mut rx) = broker.subscribe(topic.into()).await;
        assert_eq!(backlog, vec![envelope]);
        assert!(rx.try_recv().is_err(), "already delivered via backlog");
    }

    #[tokio::test]
    async fn backlog_and_live_delivery_never_double_deliver() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        let before = publish_envelope(&topic, b"sunny");
        broker.publish(&topic, before.clone()).await;

        let (backlog, mut rx) = broker.subscribe(topic.clone().into()).await;
        assert_eq!(backlog, vec![before]);

        let after = publish_envelope(&topic, b"cloudy");
        broker.publish(&topic, after.clone()).await;

        // Only the publish made *after* subscribing arrives live - the
        // earlier one was already handed over in the backlog above.
        assert_eq!(rx.recv().await.unwrap(), after);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn replay_buffer_returns_backlog_oldest_first() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        let first = publish_envelope(&topic, b"sunny");
        let second = publish_envelope(&topic, b"cloudy");
        broker.publish(&topic, first.clone()).await;
        broker.publish(&topic, second.clone()).await;

        let (backlog, _rx) = broker.subscribe(topic.into()).await;
        assert_eq!(backlog, vec![first, second]);
    }

    #[tokio::test]
    async fn replay_buffer_drops_the_oldest_once_over_capacity() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        // One more than the buffer holds - the very first publish
        // should have been evicted by the time anyone reads it back.
        let mut envelopes = Vec::with_capacity(DEFAULT_REPLAY_BUFFER_CAPACITY + 1);
        for i in 0..=DEFAULT_REPLAY_BUFFER_CAPACITY {
            let envelope = publish_envelope(&topic, format!("update {i}").as_bytes());
            broker.publish(&topic, envelope.clone()).await;
            envelopes.push(envelope);
        }

        let (backlog, _rx) = broker.subscribe(topic.into()).await;
        assert_eq!(backlog.len(), DEFAULT_REPLAY_BUFFER_CAPACITY);
        assert_eq!(backlog, &envelopes[1..]);
    }

    #[tokio::test]
    async fn a_duplicate_publish_is_not_replayed_twice() {
        // Same scenario as publishing_the_same_envelope_twice_only_delivers_once,
        // but for the backlog rather than live delivery - see ADR-0011.
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        let envelope = publish_envelope(&topic, b"sunny");
        broker.publish(&topic, envelope.clone()).await;
        broker.publish(&topic, envelope.clone()).await;

        let (backlog, _rx) = broker.subscribe(topic.into()).await;
        assert_eq!(backlog, vec![envelope]);
    }

    fn filter(s: &str) -> TopicFilter {
        TopicFilter::from_str(s).unwrap()
    }

    #[tokio::test]
    async fn a_pattern_subscriber_receives_a_matching_publish() {
        let broker = Broker::new();
        let (_backlog, mut rx) = broker.subscribe(filter("weather.+")).await;

        let topic = Topic::from_str("weather.updates").unwrap();
        let envelope = publish_envelope(&topic, b"sunny");
        assert_eq!(broker.publish(&topic, envelope.clone()).await, 1);
        assert_eq!(rx.recv().await.unwrap(), envelope);
    }

    #[tokio::test]
    async fn a_pattern_subscriber_does_not_receive_a_non_matching_publish() {
        let broker = Broker::new();
        let (_backlog, mut rx) = broker.subscribe(filter("weather.+")).await;

        let topic = Topic::from_str("traffic.updates").unwrap();
        let envelope = publish_envelope(&topic, b"jam");
        assert_eq!(broker.publish(&topic, envelope).await, 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn an_exact_and_a_matching_pattern_subscriber_both_independently_receive() {
        // Two distinct subscriptions - one exact, one a pattern that
        // happens to also match - each get their own delivery through
        // their own TopicChannel (see ADR-0022's Broker::publish doc).
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut exact_rx = subscribe_live(&broker, topic.clone()).await;
        let (_backlog, mut pattern_rx) = broker.subscribe(filter("weather.+")).await;

        let envelope = publish_envelope(&topic, b"sunny");
        assert_eq!(broker.publish(&topic, envelope.clone()).await, 2);
        assert_eq!(exact_rx.recv().await.unwrap(), envelope);
        assert_eq!(pattern_rx.recv().await.unwrap(), envelope);
    }

    #[tokio::test]
    async fn a_late_pattern_subscriber_is_replayed_a_matching_backlog() {
        // Patterns reuse TopicChannel, so a second subscriber to the
        // *same already-registered* pattern gets ADR-0021's replay
        // buffer for free, same as an exact-match topic does. Unlike
        // an exact topic, though, a pattern's buffer can only start
        // accumulating once something has actually subscribed to that
        // pattern string - there's no way to pre-create a buffer for
        // every pattern a future subscriber might use (see ADR-0022).
        let broker = Broker::new();
        let (_backlog, _first_rx) = broker.subscribe(filter("weather.+")).await;

        let topic = Topic::from_str("weather.updates").unwrap();
        let envelope = publish_envelope(&topic, b"sunny");
        broker.publish(&topic, envelope.clone()).await;

        let (backlog, _rx) = broker.subscribe(filter("weather.+")).await;
        assert_eq!(backlog, vec![envelope]);
    }

    #[tokio::test]
    async fn a_publish_before_any_subscriber_ever_used_a_pattern_is_not_retroactively_matched() {
        // The inverse of ADR-0021's "publish creates a topic's buffer
        // even with zero subscribers" - that only applies to the
        // exact-match map. A pattern nobody has subscribed to yet
        // doesn't exist in `patterns` at publish time, so there's
        // nothing to buffer into; a subscriber to that pattern later
        // only sees what's published *after* it first subscribes.
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        broker
            .publish(&topic, publish_envelope(&topic, b"sunny"))
            .await;

        let (backlog, _rx) = broker.subscribe(filter("weather.+")).await;
        assert!(backlog.is_empty());
    }

    #[tokio::test]
    async fn a_bare_hash_pattern_matches_every_topic() {
        let broker = Broker::new();
        let (_backlog, mut rx) = broker.subscribe(filter("#")).await;

        let weather = Topic::from_str("weather.updates").unwrap();
        let traffic = Topic::from_str("traffic.jam").unwrap();
        let sunny = publish_envelope(&weather, b"sunny");
        let heavy = publish_envelope(&traffic, b"heavy");
        broker.publish(&weather, sunny.clone()).await;
        broker.publish(&traffic, heavy.clone()).await;

        assert_eq!(rx.recv().await.unwrap(), sunny);
        assert_eq!(rx.recv().await.unwrap(), heavy);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn seen_ids_evicts_the_oldest_once_over_capacity() {
        let mut seen = SeenIds::new(2);
        let a = MessageId::new();
        let b = MessageId::new();
        let c = MessageId::new();

        assert!(seen.record(a));
        assert!(seen.record(b));
        // `b` is still within capacity, so re-recording it is still a
        // duplicate.
        assert!(!seen.record(b));

        assert!(seen.record(c)); // over capacity - evicts `a`

        // `a` was evicted, so it looks new again.
        assert!(seen.record(a));
        // `c` is recent enough to still be remembered.
        assert!(!seen.record(c));
    }
}
