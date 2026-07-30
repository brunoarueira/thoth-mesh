//! In-process pub/sub broker: topic registry and subscriber dispatch.
//!
//! See ADR-0006 for the design rationale.

use std::collections::HashMap;
use std::sync::Arc;

use thoth_mesh_core::{Envelope, Topic};
use tokio::sync::{RwLock, broadcast};

/// Default channel capacity for a topic's broadcast channel.
///
/// Bounds how many envelopes can be buffered for a subscriber before it
/// starts lagging (see [`tokio::sync::broadcast`]'s lag semantics).
pub const DEFAULT_TOPIC_CHANNEL_CAPACITY: usize = 256;

/// An in-process pub/sub broker: routes published envelopes to the
/// subscribers registered for their topic.
///
/// The broker only understands topic-addressed delivery, not envelope
/// semantics - interpreting an incoming message's `MessageKind` and
/// calling [`subscribe`](Broker::subscribe)/[`publish`](Broker::publish)
/// accordingly is the caller's job.
#[derive(Debug, Default)]
pub struct Broker {
    topics: RwLock<HashMap<Topic, broadcast::Sender<Arc<Envelope>>>>,
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
    /// an error, publishing to a topic nobody is listening to is normal.
    pub async fn publish(&self, topic: &Topic, envelope: Arc<Envelope>) -> usize {
        let topics = self.topics.read().await;
        match topics.get(topic) {
            Some(sender) => sender.send(envelope).unwrap_or(0),
            None => 0,
        }
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
}
