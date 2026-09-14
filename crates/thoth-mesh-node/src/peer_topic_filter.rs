//! Selective per-peer-link topic relay, parsed from repeated
//! `--peer-topic-filter <fingerprint>|<topic>` entries. See ADR-0049.
//!
//! Distinct from `--peer-topic-acl` (ADR-0020): that governs whether
//! an already-connected peer is *permitted* to publish/subscribe to a
//! topic if it explicitly asks. This instead governs what this node
//! *proactively announces* to a specific peer link via interest
//! propagation (ADR-0011) - a peer can still explicitly ask for, and
//! receive, anything `--peer-topic-acl` already permits, even a topic
//! this node would never have volunteered unasked.

use std::collections::HashSet;

use thoth_mesh_core::{Topic, TopicError};
use thoth_mesh_tls::ParseFingerprintError;

use crate::topic_acl::{Principal, parse_principal};

/// A parsed set of `--peer-topic-filter` entries. Default-deny once
/// non-empty, same as [`crate::TopicAcl`]: once configured at all, a
/// peer link's proactive relay is restricted to exactly what's listed
/// for its own identity - a peer link with no entries of its own gets
/// nothing proactively announced, not silently exempted. See ADR-0049.
#[derive(Debug, Default, Clone)]
pub struct PeerTopicFilter {
    entries: HashSet<(Principal, Topic)>,
}

impl PeerTopicFilter {
    /// Builds a [`PeerTopicFilter`] from `--peer-topic-filter`
    /// command-line strings, each parsed via [`parse_entry`]. On a
    /// parse failure, the offending raw string is included in the
    /// error - see `main.rs`'s `--peer-topic-filter` handling, which
    /// relies on that to report which entry was invalid.
    pub fn parse<'a>(
        raw_entries: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, PeerTopicFilterParseError> {
        let mut entries = HashSet::new();
        for raw in raw_entries {
            entries.insert(parse_entry(raw).map_err(|err| {
                PeerTopicFilterParseError::InvalidEntry {
                    raw: raw.to_owned(),
                    source: Box::new(err),
                }
            })?);
        }
        Ok(Self { entries })
    }

    /// Whether `principal` - a peer link's own authenticated identity
    /// - should have `topic` proactively relayed to it.
    pub fn permits(&self, principal: Principal, topic: &Topic) -> bool {
        self.entries.contains(&(principal, topic.clone()))
    }
}

/// Parses one `--peer-topic-filter` entry (`<fingerprint>|<topic>`)
/// into the `(Principal, Topic)` pair it grants relay for.
fn parse_entry(raw: &str) -> Result<(Principal, Topic), PeerTopicFilterParseError> {
    let fields: Vec<&str> = raw.split('|').collect();
    let [principal, topic] = fields.as_slice() else {
        return Err(PeerTopicFilterParseError::WrongFieldCount {
            found: fields.len(),
        });
    };

    let principal =
        parse_principal(principal.trim()).map_err(PeerTopicFilterParseError::InvalidPrincipal)?;
    let topic: Topic = topic
        .trim()
        .parse()
        .map_err(PeerTopicFilterParseError::InvalidTopic)?;

    Ok((principal, topic))
}

#[derive(Debug, thiserror::Error)]
pub enum PeerTopicFilterParseError {
    #[error("expected 2 fields separated by '|' (<fingerprint>|<topic>), found {found}")]
    WrongFieldCount { found: usize },
    #[error("invalid principal: {0}")]
    InvalidPrincipal(ParseFingerprintError),
    #[error("invalid topic: {0}")]
    InvalidTopic(TopicError),
    #[error("{raw:?}: {source}")]
    InvalidEntry {
        raw: String,
        #[source]
        source: Box<PeerTopicFilterParseError>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(s: &str) -> Topic {
        s.parse().unwrap()
    }

    #[test]
    fn parses_a_fingerprint_entry() {
        let filter = PeerTopicFilter::parse([
            "01:02:03:04:05:06:07:08:09:0A:0B:0C:0D:0E:0F:10:11:12:13:14:15:16:17:18:19:1A:1B:1C:1D:1E:1F:20|weather.updates",
        ])
        .unwrap();
        let mut fp = [0u8; 32];
        for (i, byte) in fp.iter_mut().enumerate() {
            *byte = (i + 1) as u8;
        }
        assert!(filter.permits(Principal::Fingerprint(fp), &topic("weather.updates")));
    }

    #[test]
    fn parses_an_anonymous_entry_tolerantly_like_topic_acl() {
        let filter = PeerTopicFilter::parse(["anonymous|status"]).unwrap();
        assert!(filter.permits(Principal::Anonymous, &topic("status")));
    }

    #[test]
    fn is_default_deny_for_anything_not_listed() {
        let filter = PeerTopicFilter::parse(["anonymous|weather.updates"]).unwrap();
        assert!(!filter.permits(Principal::Anonymous, &topic("other.topic")));
        assert!(!filter.permits(Principal::Fingerprint([1; 32]), &topic("weather.updates")));
    }

    #[test]
    fn distinguishes_principals_by_fingerprint() {
        let filter = PeerTopicFilter::parse([
            "anonymous|weather.updates",
            &format!("{}|traffic.updates", "01".repeat(32)),
        ])
        .unwrap();
        assert!(filter.permits(Principal::Anonymous, &topic("weather.updates")));
        assert!(!filter.permits(Principal::Anonymous, &topic("traffic.updates")));
        let mut fp = [0u8; 32];
        fp.fill(0x01);
        assert!(filter.permits(Principal::Fingerprint(fp), &topic("traffic.updates")));
        assert!(!filter.permits(Principal::Fingerprint(fp), &topic("weather.updates")));
    }

    #[test]
    fn rejects_the_wrong_field_count() {
        assert!(matches!(
            parse_entry("anonymous"),
            Err(PeerTopicFilterParseError::WrongFieldCount { found: 1 })
        ));
        assert!(matches!(
            parse_entry("anonymous|topic|extra"),
            Err(PeerTopicFilterParseError::WrongFieldCount { found: 3 })
        ));
    }

    #[test]
    fn rejects_an_invalid_principal() {
        assert!(matches!(
            parse_entry("not-a-fingerprint|topic"),
            Err(PeerTopicFilterParseError::InvalidPrincipal(_))
        ));
    }

    #[test]
    fn rejects_an_invalid_topic() {
        assert!(matches!(
            parse_entry("anonymous|"),
            Err(PeerTopicFilterParseError::InvalidTopic(_))
        ));
    }
}
