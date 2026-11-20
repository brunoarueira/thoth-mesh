use serde::{Deserialize, Serialize};

use crate::error::{DecodeError, EncodeError};
use crate::message::{MessageId, MessageKind, TraceId};
use crate::peer::PeerId;

/// The current wire protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

/// Every message exchanged between peers is wrapped in an `Envelope`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub version: u8,
    pub id: MessageId,
    /// This message's journey, not its identity - see [`TraceId`] and
    /// ADR-0059. `#[serde(default)]` so an older sender omitting this
    /// field entirely decodes as a freshly minted `TraceId`, rather
    /// than an error - tracing just starts at the first hop that
    /// understands the field.
    #[serde(default)]
    pub trace_id: TraceId,
    pub sender: PeerId,
    pub kind: MessageKind,
}

impl Envelope {
    /// Wraps a `MessageKind` in a new envelope, stamped with the current
    /// protocol version, a fresh message ID, and a fresh trace ID.
    pub fn new(sender: PeerId, kind: MessageKind) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            id: MessageId::new(),
            trace_id: TraceId::new(),
            sender,
            kind,
        }
    }

    /// Encodes this envelope to CBOR bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, EncodeError> {
        let mut bytes = Vec::new();
        ciborium::into_writer(self, &mut bytes)?;
        Ok(bytes)
    }

    /// Decodes an envelope from CBOR bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(ciborium::from_reader(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::Topic;
    use std::str::FromStr;

    #[test]
    fn round_trips_through_bytes() {
        let topic = Topic::from_str("weather.updates").unwrap();
        let sender = PeerId::new();
        let kinds = vec![
            MessageKind::Publish {
                topic: topic.clone(),
                payload: vec![1, 2, 3],
                retain: false,
                content_type: None,
                reply_to: None,
                in_reply_to: None,
            },
            MessageKind::Subscribe {
                filter: topic.clone().into(),
                ack: false,
                group: None,
                durable: false,
            },
            MessageKind::Unsubscribe {
                filter: topic.into(),
            },
            MessageKind::Ack {
                in_reply_to: MessageId::new(),
            },
            MessageKind::Error {
                in_reply_to: None,
                message: "boom".to_owned(),
            },
            MessageKind::Hello {
                listen_addr: Some("127.0.0.1:49500".to_owned()),
            },
        ];

        for kind in kinds {
            let envelope = Envelope::new(sender, kind);
            let bytes = envelope.to_bytes().unwrap();
            let decoded = Envelope::from_bytes(&bytes).unwrap();
            assert_eq!(envelope, decoded);
            assert_eq!(decoded.version, PROTOCOL_VERSION);
        }
    }

    #[test]
    fn from_bytes_rejects_garbage() {
        assert!(Envelope::from_bytes(&[0xff, 0x00, 0x01]).is_err());
    }

    #[test]
    fn decodes_an_envelope_missing_trace_id_as_a_freshly_minted_one() {
        // Stands in for an older build's `Envelope`, from before
        // ADR-0059 added `trace_id` - the exact shape every envelope
        // on the wire had before this field existed.
        #[derive(Serialize)]
        struct PreAdr0059Envelope {
            version: u8,
            id: MessageId,
            sender: PeerId,
            kind: MessageKind,
        }

        let old = PreAdr0059Envelope {
            version: PROTOCOL_VERSION,
            id: MessageId::new(),
            sender: PeerId::new(),
            kind: MessageKind::Hello { listen_addr: None },
        };
        let mut bytes = Vec::new();
        ciborium::into_writer(&old, &mut bytes).unwrap();

        let decoded = Envelope::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.id, old.id);
        assert_eq!(decoded.sender, old.sender);

        // No "no trace" sentinel - a missing field mints a fresh
        // TraceId on every decode, rather than some fixed default.
        let decoded_again = Envelope::from_bytes(&bytes).unwrap();
        assert_ne!(decoded.trace_id, decoded_again.trace_id);
    }
}
