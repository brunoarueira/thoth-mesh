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

    fn forwarder_lag_total(&self) -> u64 {
        self.forwarder_lag.load(Ordering::Relaxed)
    }

    fn topic_acl_rejections_total(&self) -> u64 {
        self.topic_acl_rejections.load(Ordering::Relaxed)
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
         thothmesh_topic_acl_rejections_total {}\n",
        membership.connected_count(),
        broker.messages_published(),
        metrics.forwarder_lag_total(),
        metrics.topic_acl_rejections_total(),
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
    fn render_prometheus_includes_all_four_metrics() {
        let membership = Membership::new();
        membership.mark_connected(thoth_mesh_core::PeerId::new(), None);
        let broker = Broker::new();
        let metrics = Metrics::new();
        metrics.record_forwarder_lag(7);
        metrics.record_topic_acl_rejection();

        let rendered = render_prometheus(&membership, &broker, &metrics);

        assert!(rendered.contains("thothmesh_peers_connected 1"));
        assert!(rendered.contains("thothmesh_messages_published_total 0"));
        assert!(rendered.contains("thothmesh_forwarder_lag_total 7"));
        assert!(rendered.contains("thothmesh_topic_acl_rejections_total 1"));
    }
}
