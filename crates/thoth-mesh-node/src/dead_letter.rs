//! Republishing an otherwise-unconsumed message to a configured
//! dead-letter topic (ADR-0047) - shared by the on-disk TTL sweep
//! (`ttl.rs`) and an `ack: true` forwarder giving up on redelivery
//! (ADR-0041, `connection.rs`), the two ways a message goes
//! unconsumed that this project currently detects.

use std::sync::Arc;

use thoth_mesh_broker::Broker;
use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};

/// Where, and as whom, to republish an otherwise-unconsumed message
/// (ADR-0047) - bundled into one value since the two always travel
/// together, and so a forwarder needing this takes one parameter
/// instead of two. `topic: None` means "nowhere, just drop it", the
/// same behavior as before this ADR - [`dead_letter`] is always safe
/// to call with one of these, configured or not.
#[derive(Debug, Clone)]
pub struct DeadLetterConfig {
    /// This node's own identity - the `sender` a republish is sent as.
    pub node_id: PeerId,
    /// `--dead-letter-topic`, if configured. `None` (what every call
    /// site defaults to absent that flag) makes every [`dead_letter`]
    /// call with this a no-op.
    pub topic: Option<Topic>,
}

/// Republishes `original` (which must be a `Publish` - anything else
/// is a caller bug, logged and otherwise ignored) to
/// `<config.topic>.<original's own topic>` - e.g.
/// `dead-letter.weather.updates` - as a fresh envelope: its own new
/// `id`, `sender: config.node_id`, `retain: false`, carrying the
/// original payload and content-type hint forward unchanged. Returns
/// whether it was actually republished - always `false` if
/// `config.topic` is `None`.
///
/// A fresh `id` deliberately, rather than reusing `original`'s: that
/// id may already be in this node's loop-prevention `seen` set
/// (ADR-0011) from its original delivery, which would make the
/// republish silently vanish as an apparent duplicate - exactly
/// backward for a mechanism whose entire point is making an
/// otherwise-lost message visible. The per-source topic prefix keeps
/// provenance visible without any wire change: a message arriving on
/// `dead-letter.weather.updates` tells a subscriber exactly what topic
/// it fell off of, and `dead-letter.#` (ADR-0022) subscribes to
/// everything at once.
///
/// Best-effort, like the persist path itself (ADR-0045): if the
/// composed destination doesn't parse as a valid `Topic` (e.g. too
/// long - a `Topic` is capped at [`thoth_mesh_core::MAX_TOPIC_LEN`]
/// bytes), this logs a warning and gives up on dead-lettering that one
/// message rather than failing the caller - it's already gone from
/// wherever `original` was pulled from either way.
pub async fn dead_letter(broker: &Broker, config: &DeadLetterConfig, original: &Envelope) -> bool {
    let Some(dead_letter_topic) = &config.topic else {
        return false;
    };
    let MessageKind::Publish {
        topic,
        payload,
        content_type,
        ..
    } = &original.kind
    else {
        tracing::warn!(
            kind = ?original.kind,
            "dead_letter given a non-Publish envelope, ignoring"
        );
        return false;
    };
    let destination = match format!("{dead_letter_topic}.{topic}").parse::<Topic>() {
        Ok(destination) => destination,
        Err(err) => {
            tracing::warn!(
                %err,
                %dead_letter_topic,
                %topic,
                "dead-letter destination topic is invalid, dropping the message instead"
            );
            return false;
        }
    };
    let envelope = Envelope::new(
        config.node_id,
        MessageKind::Publish {
            topic: destination.clone(),
            payload: payload.clone(),
            retain: false,
            content_type: content_type.clone(),
        },
    );
    broker.publish(&destination, Arc::new(envelope)).await;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn publish_envelope(topic: &str, payload: &[u8]) -> Envelope {
        Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: Topic::from_str(topic).unwrap(),
                payload: payload.to_vec(),
                retain: false,
                content_type: Some("text/plain".to_owned()),
            },
        )
    }

    fn config_for(topic: &str) -> DeadLetterConfig {
        DeadLetterConfig {
            node_id: PeerId::new(),
            topic: Some(Topic::from_str(topic).unwrap()),
        }
    }

    #[tokio::test]
    async fn republishes_to_the_prefixed_destination_topic_with_a_fresh_identity() {
        let broker = Broker::new();
        let config = config_for("dead-letter");
        let destination = Topic::from_str("dead-letter.weather.updates").unwrap();
        let (_backlog, mut rx) = broker.subscribe(destination.clone().into()).await;

        let original = publish_envelope("weather.updates", b"sunny");
        assert!(dead_letter(&broker, &config, &original).await);

        let delivered = rx.try_recv().unwrap();
        assert_ne!(
            delivered.id, original.id,
            "gets a fresh id, not the original's"
        );
        assert_eq!(delivered.sender, config.node_id);
        match &delivered.kind {
            MessageKind::Publish {
                topic,
                payload,
                retain,
                content_type,
            } => {
                assert_eq!(topic, &destination);
                assert_eq!(payload, b"sunny");
                assert!(!retain);
                assert_eq!(content_type.as_deref(), Some("text/plain"));
            }
            other => panic!("expected a Publish, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_topic_configured_is_a_no_op() {
        let broker = Broker::new();
        let config = DeadLetterConfig {
            node_id: PeerId::new(),
            topic: None,
        };
        let original = publish_envelope("weather.updates", b"sunny");

        assert!(!dead_letter(&broker, &config, &original).await);
    }

    #[tokio::test]
    async fn a_non_publish_envelope_is_ignored_rather_than_republished() {
        let broker = Broker::new();
        let config = config_for("dead-letter");
        let (_backlog, mut rx) = broker
            .subscribe(Topic::from_str("dead-letter.hello").unwrap().into())
            .await;

        let non_publish = Envelope::new(PeerId::new(), MessageKind::StatusRequest);
        assert!(!dead_letter(&broker, &config, &non_publish).await);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn an_oversized_destination_topic_is_dropped_rather_than_panicking() {
        let broker = Broker::new();
        // `topic` alone is already at the cap, so appending
        // ".<anything>" always overflows it.
        let config = DeadLetterConfig {
            node_id: PeerId::new(),
            topic: Some(Topic::from_str(&"d".repeat(thoth_mesh_core::MAX_TOPIC_LEN)).unwrap()),
        };
        let original = publish_envelope("weather.updates", b"sunny");

        assert!(!dead_letter(&broker, &config, &original).await);
    }
}
