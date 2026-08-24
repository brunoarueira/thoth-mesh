//! Per-topic client publish/subscribe authorization, parsed from
//! repeated `--topic-acl <principal>|<action>|<topic>` entries. See
//! ADR-0018.

use std::collections::HashSet;

use thoth_mesh_core::{Topic, TopicError};
use thoth_mesh_tls::{ParseFingerprintError, parse_fingerprint};

/// Who a client connection is, for authorization purposes: the SHA-256
/// fingerprint of its TLS client certificate, if it presented one
/// (the same identity primitive ADR-0017 uses for peers), or
/// `Anonymous` if not - including every connection when TLS is off
/// entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Principal {
    Anonymous,
    Fingerprint([u8; 32]),
}

impl Principal {
    /// The principal a connection presenting `fingerprint` has -
    /// [`Principal::Anonymous`] for `None`, same rule ADR-0017 already
    /// established: no certificate never satisfies a rule naming one,
    /// it just never matches.
    pub fn from_fingerprint(fingerprint: Option<[u8; 32]>) -> Self {
        match fingerprint {
            Some(fp) => Self::Fingerprint(fp),
            None => Self::Anonymous,
        }
    }
}

/// A permission a `--topic-acl` entry can grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    Publish,
    Subscribe,
}

/// A parsed set of `--topic-acl` entries. Default-deny once
/// non-empty: a `(principal, topic, action)` combination is permitted
/// only if some entry says so. See ADR-0018.
#[derive(Debug, Default, Clone)]
pub struct TopicAcl {
    entries: HashSet<(Principal, Topic, Action)>,
}

impl TopicAcl {
    /// Builds a [`TopicAcl`] from `--topic-acl` command-line strings,
    /// each parsed via [`parse_entry`]. On a parse failure, the
    /// offending raw string is included in the error - see
    /// `main.rs`'s `--topic-acl` handling, which relies on that to
    /// report which entry was invalid.
    pub fn parse<'a>(
        raw_entries: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, TopicAclParseError> {
        let mut entries = HashSet::new();
        for raw in raw_entries {
            entries.extend(
                parse_entry(raw).map_err(|err| TopicAclParseError::InvalidEntry {
                    raw: raw.to_owned(),
                    source: Box::new(err),
                })?,
            );
        }
        Ok(Self { entries })
    }

    /// Whether `principal` is allowed to perform `action` on `topic`.
    pub fn permits(&self, principal: Principal, topic: &Topic, action: Action) -> bool {
        self.entries.contains(&(principal, topic.clone(), action))
    }
}

/// Parses one `--topic-acl` entry (`<principal>|<action>|<topic>`)
/// into the one or two `(principal, topic, action)` tuples it grants -
/// two for `pubsub`, which is shorthand for both `pub` and `sub` on
/// the same principal/topic.
fn parse_entry(raw: &str) -> Result<Vec<(Principal, Topic, Action)>, TopicAclParseError> {
    let fields: Vec<&str> = raw.split('|').collect();
    let [principal, action, topic] = fields.as_slice() else {
        return Err(TopicAclParseError::WrongFieldCount {
            found: fields.len(),
        });
    };

    let principal = parse_principal(principal.trim())?;
    let actions = parse_actions(action.trim())?;
    let topic: Topic = topic
        .trim()
        .parse()
        .map_err(TopicAclParseError::InvalidTopic)?;

    Ok(actions
        .into_iter()
        .map(|action| (principal, topic.clone(), action))
        .collect())
}

fn parse_principal(s: &str) -> Result<Principal, TopicAclParseError> {
    if s.eq_ignore_ascii_case("anonymous") {
        return Ok(Principal::Anonymous);
    }
    let fingerprint = parse_fingerprint(s).map_err(TopicAclParseError::InvalidPrincipal)?;
    Ok(Principal::Fingerprint(fingerprint))
}

fn parse_actions(s: &str) -> Result<Vec<Action>, TopicAclParseError> {
    match s.to_ascii_lowercase().as_str() {
        "pub" => Ok(vec![Action::Publish]),
        "sub" => Ok(vec![Action::Subscribe]),
        "pubsub" => Ok(vec![Action::Publish, Action::Subscribe]),
        other => Err(TopicAclParseError::InvalidAction(other.to_owned())),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TopicAclParseError {
    #[error("expected 3 fields separated by '|' (<principal>|<action>|<topic>), found {found}")]
    WrongFieldCount { found: usize },
    #[error("invalid principal: {0}")]
    InvalidPrincipal(ParseFingerprintError),
    #[error("invalid action {0:?}: expected \"pub\", \"sub\", or \"pubsub\"")]
    InvalidAction(String),
    #[error("invalid topic: {0}")]
    InvalidTopic(TopicError),
    #[error("{raw:?}: {source}")]
    InvalidEntry {
        raw: String,
        #[source]
        source: Box<TopicAclParseError>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(s: &str) -> Topic {
        s.parse().unwrap()
    }

    #[test]
    fn parses_a_pub_entry_for_anonymous() {
        let acl = TopicAcl::parse(["anonymous|pub|weather.updates"]).unwrap();
        assert!(acl.permits(
            Principal::Anonymous,
            &topic("weather.updates"),
            Action::Publish
        ));
        assert!(!acl.permits(
            Principal::Anonymous,
            &topic("weather.updates"),
            Action::Subscribe
        ));
    }

    #[test]
    fn parses_a_pubsub_entry_as_both_actions() {
        let acl = TopicAcl::parse(["anonymous|pubsub|weather.updates"]).unwrap();
        assert!(acl.permits(
            Principal::Anonymous,
            &topic("weather.updates"),
            Action::Publish
        ));
        assert!(acl.permits(
            Principal::Anonymous,
            &topic("weather.updates"),
            Action::Subscribe
        ));
    }

    #[test]
    fn parses_a_fingerprint_principal_tolerantly_like_allow_peer() {
        let acl = TopicAcl::parse([
            "sha256 Fingerprint=01:02:03:04:05:06:07:08:09:0A:0B:0C:0D:0E:0F:10:11:12:13:14:15:16:17:18:19:1A:1B:1C:1D:1E:1F:20|sub|status",
        ])
        .unwrap();
        let mut fp = [0u8; 32];
        for (i, byte) in fp.iter_mut().enumerate() {
            *byte = (i + 1) as u8;
        }
        assert!(acl.permits(
            Principal::Fingerprint(fp),
            &topic("status"),
            Action::Subscribe
        ));
    }

    #[test]
    fn is_default_deny_for_anything_not_listed() {
        let acl = TopicAcl::parse(["anonymous|pub|weather.updates"]).unwrap();
        assert!(!acl.permits(Principal::Anonymous, &topic("other.topic"), Action::Publish));
        assert!(!acl.permits(
            Principal::Fingerprint([1; 32]),
            &topic("weather.updates"),
            Action::Publish
        ));
    }

    #[test]
    fn rejects_the_wrong_field_count() {
        assert!(matches!(
            parse_entry("anonymous|pub"),
            Err(TopicAclParseError::WrongFieldCount { found: 2 })
        ));
        assert!(matches!(
            parse_entry("anonymous|pub|topic|extra"),
            Err(TopicAclParseError::WrongFieldCount { found: 4 })
        ));
    }

    #[test]
    fn rejects_an_invalid_action() {
        assert!(matches!(
            parse_entry("anonymous|read|topic"),
            Err(TopicAclParseError::InvalidAction(a)) if a == "read"
        ));
    }

    #[test]
    fn rejects_an_invalid_principal() {
        assert!(matches!(
            parse_entry("not-a-fingerprint|pub|topic"),
            Err(TopicAclParseError::InvalidPrincipal(_))
        ));
    }

    #[test]
    fn rejects_an_invalid_topic() {
        assert!(matches!(
            parse_entry("anonymous|pub|"),
            Err(TopicAclParseError::InvalidTopic(_))
        ));
    }
}
