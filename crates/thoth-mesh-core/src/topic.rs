use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::TopicError;

/// Maximum length of a [`Topic`], in bytes.
pub const MAX_TOPIC_LEN: usize = 256;

/// A validated topic name.
///
/// Topics are non-empty, at most [`MAX_TOPIC_LEN`] bytes, and restricted to
/// ASCII alphanumerics plus `.`, `-`, `_`, and `/`.
///
/// Deserialization is validated the same as construction from a string -
/// this type intentionally does not derive `Serialize`/`Deserialize` via
/// `#[serde(transparent)]`, since that would let an invalid topic arrive
/// over the wire unchecked.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Topic(String);

pub(crate) fn is_valid_topic_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '/')
}

impl Topic {
    /// Returns the topic as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(s: &str) -> Result<(), TopicError> {
        if s.is_empty() {
            return Err(TopicError::Empty);
        }
        if s.len() > MAX_TOPIC_LEN {
            return Err(TopicError::TooLong {
                len: s.len(),
                max: MAX_TOPIC_LEN,
            });
        }
        if let Some(c) = s.chars().find(|c| !is_valid_topic_char(*c)) {
            return Err(TopicError::InvalidChar(c));
        }
        Ok(())
    }
}

impl FromStr for Topic {
    type Err = TopicError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate(s)?;
        Ok(Self(s.to_owned()))
    }
}

impl TryFrom<String> for Topic {
    type Error = TopicError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::validate(&s)?;
        Ok(Self(s))
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for Topic {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Topic {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Topic::try_from(s).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_topics() {
        assert!(Topic::from_str("weather.updates/v1").is_ok());
        assert!(Topic::from_str("a").is_ok());
        assert!(Topic::from_str(&"a".repeat(MAX_TOPIC_LEN)).is_ok());
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(Topic::from_str(""), Err(TopicError::Empty));
    }

    #[test]
    fn rejects_too_long() {
        let s = "a".repeat(MAX_TOPIC_LEN + 1);
        assert_eq!(
            Topic::from_str(&s),
            Err(TopicError::TooLong {
                len: MAX_TOPIC_LEN + 1,
                max: MAX_TOPIC_LEN
            })
        );
    }

    #[test]
    fn rejects_invalid_char() {
        assert_eq!(
            Topic::from_str("weather updates"),
            Err(TopicError::InvalidChar(' '))
        );
    }

    #[test]
    fn deserialize_rejects_invalid_topic() {
        let mut bytes = Vec::new();
        ciborium::into_writer("bad topic", &mut bytes).unwrap();
        let result: Result<Topic, _> = ciborium::from_reader(&bytes[..]);
        assert!(result.is_err());
    }

    #[test]
    fn round_trips_through_cbor() {
        let topic = Topic::from_str("weather.updates/v1").unwrap();
        let mut bytes = Vec::new();
        ciborium::into_writer(&topic, &mut bytes).unwrap();
        let decoded: Topic = ciborium::from_reader(&bytes[..]).unwrap();
        assert_eq!(topic, decoded);
    }
}
