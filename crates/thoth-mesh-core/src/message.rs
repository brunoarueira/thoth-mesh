use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::filter::TopicFilter;
use crate::peer::PeerId;
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

/// One peer a [`MessageKind::PeerAnnounce`] advertises: a `PeerId`
/// and the address other peers can dial it back at. Only peers with a
/// known listen address are worth advertising - see ADR-0015.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerAdvert {
    pub peer_id: PeerId,
    pub listen_addr: String,
}

/// One peer connected to the node that answered a
/// [`MessageKind::StatusRequest`], as reported in its
/// [`MessageKind::StatusReply`]. Unlike [`PeerAdvert`],
/// `listen_addr` is optional - the same as `Membership`'s own
/// `PeerStatus`, since a peer that only ever dials out never reports
/// one. See ADR-0037.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerSummary {
    pub peer_id: PeerId,
    pub listen_addr: Option<String>,
}

/// A snapshot of a node's counters, as typed fields rather than the
/// Prometheus text `--metrics-addr` exposes (ADR-0013) - the
/// [`MessageKind::StatusReply`] payload. Field names match the
/// Prometheus metric names 1:1 (minus the `thothmesh_` prefix, and
/// `_total`/no suffix mirroring counter vs. gauge there), so the two
/// stay easy to cross-reference. See ADR-0037.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsSummary {
    pub peers_connected: u64,
    pub messages_published: u64,
    pub forwarder_lag_total: u64,
    pub topic_acl_rejections_total: u64,
    pub metrics_auth_rejections_total: u64,
    pub peer_topic_acl_rejections_total: u64,
    pub replayed_messages_total: u64,
    pub lag_recovered_total: u64,
    pub topic_evictions_total: u64,
    pub pattern_evictions_total: u64,
    pub membership_evictions_total: u64,
    pub peer_directory_evictions_total: u64,
    /// Deliveries resent because an `ack: true` subscription's
    /// acknowledgement didn't arrive within the redelivery timeout
    /// (ADR-0041). Counts every resend, whether or not it's
    /// eventually acked.
    pub redelivered_messages_total: u64,
    /// Deliveries given up on after exhausting every redelivery
    /// attempt without ever being acked (ADR-0041).
    pub delivery_ack_timeouts_total: u64,
    /// Publishes the on-disk store failed to durably record
    /// (ADR-0045) - delivery still happened, but those messages won't
    /// survive a restart. Always 0 with no `--data-dir` configured.
    pub persist_failures_total: u64,
}

/// The payload of an [`Envelope`](crate::Envelope).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageKind {
    /// Publish a payload to a topic. Always a concrete [`Topic`], never
    /// a filter - there's no such thing as publishing to a pattern.
    Publish {
        topic: Topic,
        payload: Vec<u8>,
        /// Also make this the topic's retained (last-value) message,
        /// delivered immediately to any later subscriber - see
        /// ADR-0043. `#[serde(default)]` so an older sender omitting
        /// this field decodes as `false` (unchanged behavior). A
        /// `retain: true` publish with an empty `payload` clears the
        /// topic's retained message instead of setting one.
        #[serde(default)]
        retain: bool,
        /// An optional, purely-informational hint at what `payload`
        /// is - a MIME type by convention (`application/json`,
        /// `text/plain`, ...), but never validated or interpreted by
        /// the protocol; a subscriber does what it likes with it. See
        /// ADR-0044. `#[serde(default)]` so an older sender omitting
        /// this field decodes as `None`.
        #[serde(default)]
        content_type: Option<String>,
    },
    /// Subscribe to a topic filter - a plain topic name, or one
    /// containing MQTT-style wildcard segments (see ADR-0022).
    Subscribe {
        filter: TopicFilter,
        /// Opts this subscription into at-least-once delivery: each
        /// `Publish` sent for it is held until acknowledged (see
        /// `Ack` below) and redelivered on a timeout. `#[serde(default)]`
        /// so an older sender omitting this field decodes as `false` -
        /// unchanged fire-and-forget behavior. See ADR-0041.
        #[serde(default)]
        ack: bool,
        /// Joins the named consumer group for `filter` instead of
        /// ordinary fan-out: each matching `Publish` goes to exactly
        /// one currently-live member of the group, round-robin,
        /// rather than every subscriber. `#[serde(default)]`, same
        /// rolling-upgrade story as `ack` - an older sender omitting
        /// this field decodes as `None`, unchanged fan-out behavior.
        /// Combining this with `ack: true` is refused (see `Error`)
        /// rather than guessing at what "acked" means for a group.
        /// See ADR-0042.
        #[serde(default)]
        group: Option<String>,
    },
    /// Unsubscribe from a topic filter previously subscribed to.
    Unsubscribe { filter: TopicFilter },
    /// Acknowledge a previously received message. Sent by a node in
    /// reply to a `Subscribe`/`Unsubscribe`, or by a client
    /// acknowledging an individual `Publish` delivery on a
    /// subscription made with `ack: true` (ADR-0041) - the two are
    /// unambiguous by direction: a node never receives a
    /// `Subscribe`/`Unsubscribe` from a client to acknowledge.
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
    /// Advertise peers the sender knows about, so the receiver can
    /// discover and dial peers it was never directly configured
    /// with. Sent as a batch catch-up when a peer link comes up, and
    /// again whenever the sender learns of a peer it didn't already
    /// know. See ADR-0015.
    PeerAnnounce { peers: Vec<PeerAdvert> },
    /// Request the receiving node's current status: its connected
    /// peers and a metrics summary. Answered on any connection,
    /// client or peer link - see ADR-0037.
    StatusRequest,
    /// Reply to a `StatusRequest`.
    StatusReply {
        in_reply_to: MessageId,
        node_id: PeerId,
        listen_addr: Option<String>,
        peers: Vec<PeerSummary>,
        metrics: MetricsSummary,
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
                retain: false,
                content_type: None,
            },
            MessageKind::Publish {
                topic: topic.clone(),
                payload: vec![1, 2, 3],
                retain: true,
                content_type: Some("application/json".to_owned()),
            },
            MessageKind::Subscribe {
                filter: topic.clone().into(),
                ack: false,
                group: None,
            },
            MessageKind::Subscribe {
                filter: topic.clone().into(),
                ack: true,
                group: None,
            },
            MessageKind::Subscribe {
                filter: topic.clone().into(),
                ack: false,
                group: Some("workers".to_owned()),
            },
            MessageKind::Unsubscribe {
                filter: topic.into(),
            },
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
            MessageKind::PeerAnnounce {
                peers: vec![PeerAdvert {
                    peer_id: PeerId::new(),
                    listen_addr: "127.0.0.1:49501".to_owned(),
                }],
            },
            MessageKind::PeerAnnounce { peers: vec![] },
            MessageKind::StatusRequest,
            MessageKind::StatusReply {
                in_reply_to: MessageId::new(),
                node_id: PeerId::new(),
                listen_addr: Some("127.0.0.1:49500".to_owned()),
                peers: vec![PeerSummary {
                    peer_id: PeerId::new(),
                    listen_addr: None,
                }],
                metrics: MetricsSummary {
                    peers_connected: 1,
                    messages_published: 2,
                    ..Default::default()
                },
            },
        ];

        for kind in kinds {
            let mut bytes = Vec::new();
            ciborium::into_writer(&kind, &mut bytes).unwrap();
            let decoded: MessageKind = ciborium::from_reader(&bytes[..]).unwrap();
            assert_eq!(kind, decoded);
        }
    }

    /// A `Subscribe` encoded without an `ack` field at all - what an
    /// older sender that predates ADR-0041 actually puts on the wire -
    /// still decodes, defaulting to `ack: false` (unchanged
    /// fire-and-forget behavior) rather than failing to parse.
    #[test]
    fn subscribe_without_an_ack_field_decodes_as_not_acked() {
        let topic = Topic::from_str("weather.updates").unwrap();
        let filter: TopicFilter = topic.into();

        #[derive(Serialize)]
        enum PreAdr0041 {
            Subscribe { filter: TopicFilter },
        }

        let mut bytes = Vec::new();
        ciborium::into_writer(
            &PreAdr0041::Subscribe {
                filter: filter.clone(),
            },
            &mut bytes,
        )
        .unwrap();
        let decoded: MessageKind = ciborium::from_reader(&bytes[..]).unwrap();
        assert_eq!(
            decoded,
            MessageKind::Subscribe {
                filter,
                ack: false,
                group: None,
            }
        );
    }

    /// Same as `subscribe_without_an_ack_field_decodes_as_not_acked`,
    /// for `group` (ADR-0042): a sender that predates the field - or
    /// one from just after ADR-0041 that has `ack` but not yet
    /// `group` - still decodes, defaulting to `group: None` (ordinary
    /// fan-out).
    #[test]
    fn subscribe_without_a_group_field_decodes_as_no_group() {
        let topic = Topic::from_str("weather.updates").unwrap();
        let filter: TopicFilter = topic.into();

        #[derive(Serialize)]
        enum PreAdr0042 {
            Subscribe { filter: TopicFilter, ack: bool },
        }

        let mut bytes = Vec::new();
        ciborium::into_writer(
            &PreAdr0042::Subscribe {
                filter: filter.clone(),
                ack: true,
            },
            &mut bytes,
        )
        .unwrap();
        let decoded: MessageKind = ciborium::from_reader(&bytes[..]).unwrap();
        assert_eq!(
            decoded,
            MessageKind::Subscribe {
                filter,
                ack: true,
                group: None,
            }
        );
    }

    /// A `Publish` encoded without a `retain` field - what every
    /// sender that predates ADR-0043 puts on the wire - still decodes,
    /// defaulting to `retain: false` (unchanged behavior).
    #[test]
    fn publish_without_a_retain_field_decodes_as_not_retained() {
        let topic = Topic::from_str("weather.updates").unwrap();

        #[derive(Serialize)]
        enum PreAdr0043 {
            Publish { topic: Topic, payload: Vec<u8> },
        }

        let mut bytes = Vec::new();
        ciborium::into_writer(
            &PreAdr0043::Publish {
                topic: topic.clone(),
                payload: vec![1, 2, 3],
            },
            &mut bytes,
        )
        .unwrap();
        let decoded: MessageKind = ciborium::from_reader(&bytes[..]).unwrap();
        assert_eq!(
            decoded,
            MessageKind::Publish {
                topic,
                payload: vec![1, 2, 3],
                retain: false,
                content_type: None,
            }
        );
    }

    /// A `Publish` encoded with `retain` but no `content_type` - a
    /// sender from between ADR-0043 and ADR-0044 - still decodes,
    /// defaulting `content_type` to `None`.
    #[test]
    fn publish_without_a_content_type_field_decodes_as_none() {
        let topic = Topic::from_str("weather.updates").unwrap();

        #[derive(Serialize)]
        enum PreAdr0044 {
            Publish {
                topic: Topic,
                payload: Vec<u8>,
                retain: bool,
            },
        }

        let mut bytes = Vec::new();
        ciborium::into_writer(
            &PreAdr0044::Publish {
                topic: topic.clone(),
                payload: vec![1, 2, 3],
                retain: true,
            },
            &mut bytes,
        )
        .unwrap();
        let decoded: MessageKind = ciborium::from_reader(&bytes[..]).unwrap();
        assert_eq!(
            decoded,
            MessageKind::Publish {
                topic,
                payload: vec![1, 2, 3],
                retain: true,
                content_type: None,
            }
        );
    }
}
