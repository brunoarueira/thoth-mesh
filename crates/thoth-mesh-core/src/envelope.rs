use serde::{Deserialize, Serialize};

use crate::error::{DecodeError, EncodeError};
use crate::message::{MessageId, MessageKind};
use crate::peer::PeerId;

/// The current wire protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

/// Every message exchanged between peers is wrapped in an `Envelope`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub version: u8,
    pub id: MessageId,
    pub sender: PeerId,
    pub kind: MessageKind,
}

impl Envelope {
    /// Wraps a `MessageKind` in a new envelope, stamped with the current
    /// protocol version and a fresh message ID.
    pub fn new(sender: PeerId, kind: MessageKind) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            id: MessageId::new(),
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
            },
            MessageKind::Subscribe {
                topic: topic.clone(),
            },
            MessageKind::Unsubscribe { topic },
            MessageKind::Ack {
                in_reply_to: MessageId::new(),
            },
            MessageKind::Error {
                in_reply_to: None,
                message: "boom".to_owned(),
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
}
