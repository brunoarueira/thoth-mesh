//! Node-level counters exposed over an opt-in Prometheus scrape
//! endpoint (`--metrics-addr`). See ADR-0013.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use thoth_mesh::Membership;
use thoth_mesh_broker::Broker;

/// The one metric that isn't already naturally owned by an existing
/// type - `Membership` tracks connected peers and `Broker` tracks
/// publishes, but nothing currently counts a lagging forwarder's
/// skipped envelopes. Cheaply cloneable, following the same
/// `Arc`-wrapped pattern as `Membership`/`Interest`/`PeerLinks`.
#[derive(Debug, Default, Clone)]
pub struct Metrics {
    forwarder_lag: Arc<AtomicU64>,
    /// `Subscribe`/`Publish` attempts refused by a `--topic-acl`
    /// (ADR-0018). Zero unless one is configured.
    topic_acl_rejections: Arc<AtomicU64>,
    /// Metrics scrapes refused by a `--metrics-token-file` (ADR-0019).
    /// Zero unless one is configured.
    metrics_auth_rejections: Arc<AtomicU64>,
    /// `Subscribe`/`Publish` attempts from a peer link refused by a
    /// `--peer-topic-acl` (ADR-0020). Zero unless one is configured.
    /// Counted separately from `topic_acl_rejections` so an operator
    /// can tell a misbehaving peer apart from a misbehaving client.
    peer_topic_acl_rejections: Arc<AtomicU64>,
    /// Envelopes delivered to a newly-spawned forwarder from a topic's
    /// replay buffer (ADR-0021), rather than live. Distinct from
    /// `Broker::messages_published`, which counts distinct publishes,
    /// not deliveries.
    replayed_messages: Arc<AtomicU64>,
    /// Envelopes recovered from a topic's replay buffer after a
    /// forwarder lagged mid-stream (ADR-0024) - distinct from
    /// `replayed_messages` (a newly-spawned forwarder's initial
    /// catch-up) and from `forwarder_lag` (the raw skip count
    /// `tokio::sync::broadcast` reports, which can differ from how
    /// much was actually recoverable).
    lag_recovered: Arc<AtomicU64>,
}

impl Metrics {
    /// A fresh counter, starting at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that a forwarder skipped `skipped` envelopes because it
    /// fell behind the broker's broadcast channel (see
    /// [`tokio::sync::broadcast`]'s lag semantics).
    pub fn record_forwarder_lag(&self, skipped: u64) {
        self.forwarder_lag.fetch_add(skipped, Ordering::Relaxed);
    }

    /// Records that a `Subscribe` or `Publish` was refused by a
    /// `--topic-acl` (ADR-0018).
    pub fn record_topic_acl_rejection(&self) {
        self.topic_acl_rejections.fetch_add(1, Ordering::Relaxed);
    }

    /// Records that a metrics scrape was refused by a
    /// `--metrics-token-file` (ADR-0019).
    pub fn record_metrics_auth_rejection(&self) {
        self.metrics_auth_rejections.fetch_add(1, Ordering::Relaxed);
    }

    /// Records that a `Subscribe` or `Publish` from a peer link was
    /// refused by a `--peer-topic-acl` (ADR-0020).
    pub fn record_peer_topic_acl_rejection(&self) {
        self.peer_topic_acl_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records that `count` envelopes were delivered to a newly-spawned
    /// forwarder from a topic's replay buffer (ADR-0021).
    pub fn record_replayed_messages(&self, count: u64) {
        self.replayed_messages.fetch_add(count, Ordering::Relaxed);
    }

    /// Records that `count` envelopes were recovered from a topic's
    /// replay buffer after a forwarder lagged mid-stream (ADR-0024).
    pub fn record_lag_recovered(&self, count: u64) {
        self.lag_recovered.fetch_add(count, Ordering::Relaxed);
    }

    fn forwarder_lag_total(&self) -> u64 {
        self.forwarder_lag.load(Ordering::Relaxed)
    }

    fn topic_acl_rejections_total(&self) -> u64 {
        self.topic_acl_rejections.load(Ordering::Relaxed)
    }

    fn metrics_auth_rejections_total(&self) -> u64 {
        self.metrics_auth_rejections.load(Ordering::Relaxed)
    }

    fn peer_topic_acl_rejections_total(&self) -> u64 {
        self.peer_topic_acl_rejections.load(Ordering::Relaxed)
    }

    fn replayed_messages_total(&self) -> u64 {
        self.replayed_messages.load(Ordering::Relaxed)
    }

    fn lag_recovered_total(&self) -> u64 {
        self.lag_recovered.load(Ordering::Relaxed)
    }
}

/// Renders the current node metrics as Prometheus text exposition
/// format - a `# TYPE` line plus a `name value` line per metric.
pub fn render_prometheus(membership: &Membership, broker: &Broker, metrics: &Metrics) -> String {
    format!(
        "# TYPE thothmesh_peers_connected gauge\n\
         thothmesh_peers_connected {}\n\
         # TYPE thothmesh_messages_published_total counter\n\
         thothmesh_messages_published_total {}\n\
         # TYPE thothmesh_forwarder_lag_total counter\n\
         thothmesh_forwarder_lag_total {}\n\
         # TYPE thothmesh_topic_acl_rejections_total counter\n\
         thothmesh_topic_acl_rejections_total {}\n\
         # TYPE thothmesh_metrics_auth_rejections_total counter\n\
         thothmesh_metrics_auth_rejections_total {}\n\
         # TYPE thothmesh_peer_topic_acl_rejections_total counter\n\
         thothmesh_peer_topic_acl_rejections_total {}\n\
         # TYPE thothmesh_replayed_messages_total counter\n\
         thothmesh_replayed_messages_total {}\n\
         # TYPE thothmesh_lag_recovered_total counter\n\
         thothmesh_lag_recovered_total {}\n\
         # TYPE thothmesh_topic_evictions_total counter\n\
         thothmesh_topic_evictions_total {}\n\
         # TYPE thothmesh_pattern_evictions_total counter\n\
         thothmesh_pattern_evictions_total {}\n",
        membership.connected_count(),
        broker.messages_published(),
        metrics.forwarder_lag_total(),
        metrics.topic_acl_rejections_total(),
        metrics.metrics_auth_rejections_total(),
        metrics.peer_topic_acl_rejections_total(),
        metrics.replayed_messages_total(),
        metrics.lag_recovered_total(),
        broker.topic_evictions(),
        broker.pattern_evictions(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_forwarder_lag_accumulates() {
        let metrics = Metrics::new();
        metrics.record_forwarder_lag(3);
        metrics.record_forwarder_lag(5);
        assert_eq!(metrics.forwarder_lag_total(), 8);
    }

    #[test]
    fn record_topic_acl_rejection_accumulates() {
        let metrics = Metrics::new();
        metrics.record_topic_acl_rejection();
        metrics.record_topic_acl_rejection();
        assert_eq!(metrics.topic_acl_rejections_total(), 2);
    }

    #[test]
    fn record_metrics_auth_rejection_accumulates() {
        let metrics = Metrics::new();
        metrics.record_metrics_auth_rejection();
        metrics.record_metrics_auth_rejection();
        metrics.record_metrics_auth_rejection();
        assert_eq!(metrics.metrics_auth_rejections_total(), 3);
    }

    #[test]
    fn record_peer_topic_acl_rejection_accumulates() {
        let metrics = Metrics::new();
        metrics.record_peer_topic_acl_rejection();
        metrics.record_peer_topic_acl_rejection();
        metrics.record_peer_topic_acl_rejection();
        metrics.record_peer_topic_acl_rejection();
        assert_eq!(metrics.peer_topic_acl_rejections_total(), 4);
    }

    #[test]
    fn record_topic_acl_rejection_and_peer_topic_acl_rejection_are_independent() {
        let metrics = Metrics::new();
        metrics.record_topic_acl_rejection();
        assert_eq!(metrics.topic_acl_rejections_total(), 1);
        assert_eq!(metrics.peer_topic_acl_rejections_total(), 0);
    }

    #[test]
    fn record_replayed_messages_accumulates() {
        let metrics = Metrics::new();
        metrics.record_replayed_messages(3);
        metrics.record_replayed_messages(5);
        assert_eq!(metrics.replayed_messages_total(), 8);
    }

    #[test]
    fn record_lag_recovered_accumulates() {
        let metrics = Metrics::new();
        metrics.record_lag_recovered(4);
        metrics.record_lag_recovered(6);
        assert_eq!(metrics.lag_recovered_total(), 10);
    }

    #[test]
    fn render_prometheus_includes_all_ten_metrics() {
        let membership = Membership::new();
        membership.mark_connected(thoth_mesh_core::PeerId::new(), None);
        let broker = Broker::new();
        let metrics = Metrics::new();
        metrics.record_forwarder_lag(7);
        metrics.record_topic_acl_rejection();
        metrics.record_metrics_auth_rejection();
        metrics.record_peer_topic_acl_rejection();
        metrics.record_replayed_messages(2);
        metrics.record_lag_recovered(5);

        let rendered = render_prometheus(&membership, &broker, &metrics);

        assert!(rendered.contains("thothmesh_peers_connected 1"));
        assert!(rendered.contains("thothmesh_messages_published_total 0"));
        assert!(rendered.contains("thothmesh_forwarder_lag_total 7"));
        assert!(rendered.contains("thothmesh_topic_acl_rejections_total 1"));
        assert!(rendered.contains("thothmesh_metrics_auth_rejections_total 1"));
        assert!(rendered.contains("thothmesh_peer_topic_acl_rejections_total 1"));
        // Freshly-created Broker, well under DEFAULT_TOPIC_MAP_CAPACITY
        // (see ADR-0025) - both eviction counters are still zero, but
        // present.
        assert!(rendered.contains("thothmesh_topic_evictions_total 0"));
        assert!(rendered.contains("thothmesh_pattern_evictions_total 0"));
        assert!(rendered.contains("thothmesh_replayed_messages_total 2"));
        assert!(rendered.contains("thothmesh_lag_recovered_total 5"));
    }
}
