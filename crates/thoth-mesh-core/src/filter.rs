//! A subscribe-side topic filter: an exact topic name, or one
//! containing MQTT-style wildcard segments (`+`, `#`) that matches a
//! whole family of concrete topics at once. See ADR-0022.

use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::TopicFilterError;
use crate::topic::{MAX_TOPIC_LEN, Topic, is_valid_topic_char};

const SEGMENT_SEPARATOR: char = '.';
const SINGLE_LEVEL_WILDCARD: &str = "+";
const MULTI_LEVEL_WILDCARD: &str = "#";

/// A validated topic filter: what a `Subscribe`/`Unsubscribe` carries,
/// as distinct from [`Topic`], which is what a `Publish` carries.
///
/// Segments are `.`-delimited, same as an ordinary [`Topic`]. Two
/// segments carry special meaning: `+` matches exactly one segment,
/// and a trailing `#` matches zero or more remaining segments (valid
/// only as the filter's last segment). Every other segment must be a
/// valid `Topic` segment - in particular, every string that's already
/// a valid `Topic` is also a valid, purely-literal `TopicFilter` with
/// identical matching behavior (see [`From<Topic>`](TopicFilter#impl-From<Topic>-for-TopicFilter)).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicFilter(String);

impl TopicFilter {
    /// Returns the filter as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this filter contains no wildcard segments - a plain
    /// topic name written using filter syntax, functionally identical
    /// to subscribing to the [`Topic`] of the same name.
    pub fn is_literal(&self) -> bool {
        !self
            .0
            .split(SEGMENT_SEPARATOR)
            .any(|segment| segment == SINGLE_LEVEL_WILDCARD || segment == MULTI_LEVEL_WILDCARD)
    }

    /// The concrete [`Topic`] this filter names, if it has no
    /// wildcard segments - `None` for a genuine pattern.
    pub fn as_topic(&self) -> Option<Topic> {
        self.is_literal().then(|| {
            Topic::from_str(&self.0)
                .expect("a literal TopicFilter's charset is always a valid Topic")
        })
    }

    /// Whether `topic` is matched by this filter: `+` matches any one
    /// segment, a trailing `#` matches every remaining segment
    /// (including none), and any other segment must match exactly.
    pub fn matches(&self, topic: &Topic) -> bool {
        let filter_segments: Vec<&str> = self.0.split(SEGMENT_SEPARATOR).collect();
        let topic_segments: Vec<&str> = topic.as_str().split(SEGMENT_SEPARATOR).collect();

        let mut fi = 0;
        let mut ti = 0;
        while fi < filter_segments.len() {
            let segment = filter_segments[fi];
            if segment == MULTI_LEVEL_WILDCARD {
                // Only ever valid (per `validate`) as the last
                // segment, so it consumes everything left.
                return true;
            }
            let Some(&topic_segment) = topic_segments.get(ti) else {
                return false;
            };
            if segment != SINGLE_LEVEL_WILDCARD && segment != topic_segment {
                return false;
            }
            fi += 1;
            ti += 1;
        }
        ti == topic_segments.len()
    }

    fn validate(s: &str) -> Result<(), TopicFilterError> {
        if s.is_empty() {
            return Err(TopicFilterError::Empty);
        }
        if s.len() > MAX_TOPIC_LEN {
            return Err(TopicFilterError::TooLong {
                len: s.len(),
                max: MAX_TOPIC_LEN,
            });
        }
        let segments: Vec<&str> = s.split(SEGMENT_SEPARATOR).collect();
        let last = segments.len() - 1;
        for (i, segment) in segments.iter().enumerate() {
            if *segment == MULTI_LEVEL_WILDCARD {
                if i != last {
                    return Err(TopicFilterError::MultiLevelWildcardNotLast);
                }
                continue;
            }
            if *segment == SINGLE_LEVEL_WILDCARD {
                continue;
            }
            if let Some(c) = segment.chars().find(|c| !is_valid_topic_char(*c)) {
                return Err(TopicFilterError::InvalidChar(c));
            }
        }
        Ok(())
    }
}

impl From<Topic> for TopicFilter {
    fn from(topic: Topic) -> Self {
        // A `Topic`'s charset never contains `+`/`#`, so it's always
        // already a valid literal filter - skip re-validating it.
        Self(topic.as_str().to_owned())
    }
}

impl FromStr for TopicFilter {
    type Err = TopicFilterError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate(s)?;
        Ok(Self(s.to_owned()))
    }
}

impl TryFrom<String> for TopicFilter {
    type Error = TopicFilterError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::validate(&s)?;
        Ok(Self(s))
    }
}

impl fmt::Display for TopicFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for TopicFilter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for TopicFilter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        TopicFilter::try_from(s).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(s: &str) -> Topic {
        Topic::from_str(s).unwrap()
    }

    fn filter(s: &str) -> TopicFilter {
        TopicFilter::from_str(s).unwrap()
    }

    #[test]
    fn a_literal_filter_matches_only_the_same_topic() {
        assert!(filter("weather.updates").matches(&topic("weather.updates")));
        assert!(!filter("weather.updates").matches(&topic("weather.forecast")));
    }

    #[test]
    fn every_valid_topic_string_is_a_valid_literal_filter() {
        let f = filter("weather.updates/v1");
        assert!(f.is_literal());
        assert_eq!(f.as_topic(), Some(topic("weather.updates/v1")));
    }

    #[test]
    fn plus_matches_exactly_one_segment() {
        let f = filter("weather.+");
        assert!(f.matches(&topic("weather.updates")));
        assert!(f.matches(&topic("weather.forecast")));
        assert!(!f.matches(&topic("weather")));
        assert!(!f.matches(&topic("weather.updates.v2")));
    }

    #[test]
    fn plus_can_appear_in_the_middle() {
        let f = filter("weather.+.v1");
        assert!(f.matches(&topic("weather.updates.v1")));
        assert!(!f.matches(&topic("weather.updates.v2")));
        assert!(!f.matches(&topic("weather.updates")));
    }

    #[test]
    fn hash_matches_zero_or_more_trailing_segments() {
        let f = filter("weather.#");
        assert!(f.matches(&topic("weather")));
        assert!(f.matches(&topic("weather.updates")));
        assert!(f.matches(&topic("weather.updates.v1")));
        assert!(!f.matches(&topic("traffic.updates")));
    }

    #[test]
    fn bare_hash_matches_everything() {
        let f = filter("#");
        assert!(f.matches(&topic("weather")));
        assert!(f.matches(&topic("weather.updates.v1")));
    }

    #[test]
    fn is_literal_is_false_for_any_wildcard_segment() {
        assert!(!filter("weather.+").is_literal());
        assert!(!filter("weather.#").is_literal());
        assert!(filter("weather.+").as_topic().is_none());
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(TopicFilter::from_str(""), Err(TopicFilterError::Empty));
    }

    #[test]
    fn rejects_too_long() {
        let s = "a".repeat(MAX_TOPIC_LEN + 1);
        assert_eq!(
            TopicFilter::from_str(&s),
            Err(TopicFilterError::TooLong {
                len: MAX_TOPIC_LEN + 1,
                max: MAX_TOPIC_LEN
            })
        );
    }

    #[test]
    fn rejects_invalid_char() {
        assert_eq!(
            TopicFilter::from_str("weather updates"),
            Err(TopicFilterError::InvalidChar(' '))
        );
    }

    #[test]
    fn rejects_a_hash_that_is_not_the_last_segment() {
        assert_eq!(
            TopicFilter::from_str("weather.#.updates"),
            Err(TopicFilterError::MultiLevelWildcardNotLast)
        );
    }

    #[test]
    fn rejects_a_wildcard_mixed_into_a_literal_segment() {
        assert_eq!(
            TopicFilter::from_str("we+ather"),
            Err(TopicFilterError::InvalidChar('+'))
        );
    }

    #[test]
    fn from_topic_round_trips_as_a_literal_filter() {
        let t = topic("weather.updates");
        let f: TopicFilter = t.clone().into();
        assert!(f.is_literal());
        assert_eq!(f.as_topic(), Some(t));
    }

    #[test]
    fn deserialize_rejects_invalid_filter() {
        let mut bytes = Vec::new();
        ciborium::into_writer("bad filter", &mut bytes).unwrap();
        let result: Result<TopicFilter, _> = ciborium::from_reader(&bytes[..]);
        assert!(result.is_err());
    }

    #[test]
    fn round_trips_through_cbor() {
        let f = filter("weather.+.updates");
        let mut bytes = Vec::new();
        ciborium::into_writer(&f, &mut bytes).unwrap();
        let decoded: TopicFilter = ciborium::from_reader(&bytes[..]).unwrap();
        assert_eq!(f, decoded);
    }
}
