//! Core protocol types and wire format shared across the thoth-mesh workspace.
//!
//! See [ADR-0005](https://github.com/brunoarueira/thoth-mesh/blob/main/docs/adr/0005-wire-protocol-v1.md)
//! for the design rationale.

mod envelope;
mod error;
mod framing;
mod message;
mod peer;
mod topic;

pub use envelope::{Envelope, PROTOCOL_VERSION};
pub use error::{DecodeError, EncodeError, FramingError, TopicError};
pub use framing::{MAX_FRAME_LEN, read_frame, write_frame};
pub use message::{MessageId, MessageKind};
pub use peer::PeerId;
pub use topic::{MAX_TOPIC_LEN, Topic};
