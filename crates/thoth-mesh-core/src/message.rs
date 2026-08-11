use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::topic::Topic;

/// The identity of a message.
///
/// Backed by a UUIDv7, which embeds a millisecond timestamp and is
/// monotonically sortable, so message ordering is available without a
/// redundant timestamp field on the envelope (see ADR-0005).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageId(Uuid);

impl MessageId {
    /// Generates a new message ID, timestamped at the current time.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Returns the Unix timestamp (seconds, nanoseconds) embedded in this
    /// ID, if it was generated from a UUIDv7 (always true for IDs created
    /// via [`MessageId::new`]).
    pub fn timestamp(&self) -> Option<(u64, u32)> {
        self.0.get_timestamp().map(|ts| ts.to_unix())
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

/// The payload of an [`Envelope`](crate::Envelope).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageKind {
    /// Publish a payload to a topic.
    Publish { topic: Topic, payload: Vec<u8> },
    /// Subscribe to a topic.
    Subscribe { topic: Topic },
    /// Unsubscribe from a topic.
    Unsubscribe { topic: Topic },
    /// Acknowledge a previously received message.
    Ack { in_reply_to: MessageId },
    /// Report an error, optionally in response to a specific message.
    Error {
        in_reply_to: Option<MessageId>,
        message: String,
    },
    /// Identify this connection as a peer link rather than a client
    /// connection. Sent as the first message by the dialing side of a
    /// node-to-node connection; the accepting side replies in kind.
    /// See ADR-0009.
    Hello {
        /// The address other peers should dial to reach the sender,
        /// if it accepts inbound connections.
        listen_addr: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn new_ids_are_distinct_and_ordered() {
        let a = MessageId::new();
        let b = MessageId::new();
        assert_ne!(a, b);
        assert!(a <= b);
    }

    #[test]
    fn timestamp_is_present() {
        assert!(MessageId::new().timestamp().is_some());
    }

    #[test]
    fn message_kind_round_trips_through_cbor() {
        let topic = Topic::from_str("weather.updates").unwrap();
        let kinds = vec![
            MessageKind::Publish {
                topic: topic.clone(),
                payload: vec![1, 2, 3],
            },
            MessageKind::Subscribe {
                topic: topic.clone(),
            },
            MessageKind::Unsubscribe { topic },
            MessageKind::Ack {
                in_reply_to: MessageId::new(),
            },
            MessageKind::Error {
                in_reply_to: Some(MessageId::new()),
                message: "boom".to_owned(),
            },
            MessageKind::Error {
                in_reply_to: None,
                message: "boom".to_owned(),
            },
            MessageKind::Hello {
                listen_addr: Some("127.0.0.1:49500".to_owned()),
            },
            MessageKind::Hello { listen_addr: None },
        ];

        for kind in kinds {
            let mut bytes = Vec::new();
            ciborium::into_writer(&kind, &mut bytes).unwrap();
            let decoded: MessageKind = ciborium::from_reader(&bytes[..]).unwrap();
            assert_eq!(kind, decoded);
        }
    }
}
