//! In-process pub/sub broker: topic registry and subscriber dispatch.
//!
//! See ADR-0006 for the design rationale, and ADR-0011 for the
//! duplicate-envelope dedup this crate now also does.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use thoth_mesh_core::{Envelope, MessageId, Topic};
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
    topics: RwLock<HashMap<Topic, broadcast::Sender<Arc<Envelope>>>>,
    seen: Mutex<SeenIds>,
    messages_published: AtomicU64,
}

impl Default for Broker {
    fn default() -> Self {
        Self {
            topics: RwLock::default(),
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

    /// Subscribes to `topic`, returning a receiver that yields every
    /// envelope published to it from this point on.
    ///
    /// Unsubscribing is just dropping the returned receiver.
    pub async fn subscribe(&self, topic: Topic) -> broadcast::Receiver<Arc<Envelope>> {
        let mut topics = self.topics.write().await;
        topics
            .entry(topic)
            .or_insert_with(|| broadcast::channel(DEFAULT_TOPIC_CHANNEL_CAPACITY).0)
            .subscribe()
    }

    /// Publishes `envelope` to every subscriber currently registered for
    /// `topic`, returning how many subscribers received it.
    ///
    /// Returns `0` if there are no subscribers for `topic` - this is not
    /// an error, publishing to a topic nobody is listening to is normal -
    /// or if an envelope with this same `MessageId` has already been
    /// published here before, which is dropped rather than redelivered
    /// (see ADR-0011).
    pub async fn publish(&self, topic: &Topic, envelope: Arc<Envelope>) -> usize {
        let is_new = self.seen.lock().unwrap().record(envelope.id);
        if !is_new {
            return 0;
        }
        self.messages_published.fetch_add(1, Ordering::Relaxed);
        let topics = self.topics.read().await;
        match topics.get(topic) {
            Some(sender) => sender.send(envelope).unwrap_or(0),
            None => 0,
        }
    }

    /// How many distinct (non-duplicate) envelopes have been published
    /// through this broker since it was created - see ADR-0013.
    pub fn messages_published(&self) -> u64 {
        self.messages_published.load(Ordering::Relaxed)
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
    use thoth_mesh_core::{MessageKind, PeerId};

    fn publish_envelope(topic: &Topic, payload: &[u8]) -> Arc<Envelope> {
        Arc::new(Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: topic.clone(),
                payload: payload.to_vec(),
            },
        ))
    }

    #[tokio::test]
    async fn subscribe_then_publish_delivers_to_subscriber() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut rx = broker.subscribe(topic.clone()).await;

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
        let mut rx_a = broker.subscribe(topic.clone()).await;
        let mut rx_b = broker.subscribe(topic.clone()).await;

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
        let mut rx = broker.subscribe(weather.clone()).await;

        let envelope = publish_envelope(&traffic, b"jam");
        let delivered = broker.publish(&traffic, envelope).await;

        assert_eq!(delivered, 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn dropped_receiver_does_not_affect_others() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let rx_a = broker.subscribe(topic.clone()).await;
        let mut rx_b = broker.subscribe(topic.clone()).await;
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
        let mut rx = broker.subscribe(topic.clone()).await;

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
        let mut rx = broker.subscribe(topic.clone()).await;

        let first = publish_envelope(&topic, b"sunny");
        let second = publish_envelope(&topic, b"cloudy");
        assert_eq!(broker.publish(&topic, first.clone()).await, 1);
        assert_eq!(broker.publish(&topic, second.clone()).await, 1);

        assert_eq!(rx.recv().await.unwrap(), first);
        assert_eq!(rx.recv().await.unwrap(), second);
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
