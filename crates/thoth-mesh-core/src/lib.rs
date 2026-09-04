//! Core protocol types and wire format shared across the thoth-mesh workspace.
//!
//! See [ADR-0005](https://github.com/brunoarueira/thoth-mesh/blob/main/docs/adr/0005-wire-protocol-v1.md)
//! for the design rationale.

#[cfg(feature = "async")]
pub mod async_framing;
mod envelope;
mod error;
mod filter;
mod framing;
mod message;
mod peer;
mod topic;

pub use envelope::{Envelope, PROTOCOL_VERSION};
pub use error::{DecodeError, EncodeError, FramingError, TopicError, TopicFilterError};
pub use filter::TopicFilter;
pub use framing::{MAX_FRAME_LEN, read_frame, write_frame};
pub use message::{MessageId, MessageKind, MetricsSummary, PeerAdvert, PeerSummary};
pub use peer::PeerId;
pub use topic::{MAX_TOPIC_LEN, Topic};

/// Default node address: a private/dynamic-range port, chosen to avoid
/// colliding with commonly registered services.
///
/// Shared by `thoth-mesh-node` (what it binds to) and `thoth-mesh-cli`
/// (what it connects to by default) so the two can't drift apart.
pub const DEFAULT_ADDR: &str = "127.0.0.1:49500";
