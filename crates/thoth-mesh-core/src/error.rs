use std::io;

/// Failed to encode an [`Envelope`](crate::Envelope) to bytes.
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("failed to encode message: {0}")]
    Cbor(#[from] ciborium::ser::Error<io::Error>),
}

/// Failed to decode bytes into an [`Envelope`](crate::Envelope).
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("failed to decode message: {0}")]
    Cbor(#[from] ciborium::de::Error<io::Error>),
}

/// Failed to read or write a length-prefixed frame.
#[derive(Debug, thiserror::Error)]
pub enum FramingError {
    #[error("i/o error while framing a message: {0}")]
    Io(#[from] io::Error),
    #[error("frame length {len} exceeds the maximum of {max} bytes")]
    FrameTooLarge { len: u32, max: u32 },
}

/// An invalid [`Topic`](crate::Topic) was constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TopicError {
    #[error("topic must not be empty")]
    Empty,
    #[error("topic exceeds the maximum length of {max} bytes (got {len})")]
    TooLong { len: usize, max: usize },
    #[error("topic contains an invalid character: {0:?}")]
    InvalidChar(char),
}
