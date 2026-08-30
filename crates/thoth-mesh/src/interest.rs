//! Tracks this node's own aggregate topic interest - across every
//! connection, client or peer alike - so peer links know when a
//! topic filter transitions from "nobody here wants this" to
//! "somebody does" (or back). See ADR-0011. Keyed on `TopicFilter`,
//! not `Topic` - every literal topic is already a `TopicFilter`, and
//! a wildcard filter is tracked the same way (see ADR-0022).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use thoth_mesh_core::TopicFilter;

/// A thread-safe, cheaply-cloneable registry of how many of this
/// node's connections are currently interested in each topic filter.
///
/// This is deliberately node-wide, aggregated across every
/// connection - distinct from `thoth-mesh-node::connection`'s
/// per-connection forwarder map, which only tracks one connection's
/// own subscriptions. A filter gaining its first interested
/// connection (from *any* client or peer) is what should trigger
/// telling every other active peer link about it; losing its last is
/// what should trigger telling them to stop (see ADR-0011).
#[derive(Debug, Default, Clone)]
pub struct Interest {
    counts: Arc<Mutex<HashMap<TopicFilter, usize>>>,
}

impl Interest {
    /// An empty registry, with no topic interest recorded yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one more interested connection for `filter`. Returns
    /// `true` if this was the filter's first (a 0 -> 1 transition) -
    /// the caller should propagate a `Subscribe` to its other peer
    /// links only in that case.
    pub fn subscribe(&self, filter: TopicFilter) -> bool {
        let mut counts = self.counts.lock().unwrap();
        let count = counts.entry(filter).or_insert(0);
        *count += 1;
        *count == 1
    }

    /// Records one fewer interested connection for `filter`. Returns
    /// `true` if this was the filter's last (a 1 -> 0 transition) -
    /// the caller should propagate an `Unsubscribe` to its other peer
    /// links only in that case.
    ///
    /// A no-op returning `false` for a filter with no recorded
    /// interest - shouldn't happen in practice, since callers only
    /// call this once for each prior successful [`subscribe`](Self::subscribe).
    pub fn unsubscribe(&self, filter: &TopicFilter) -> bool {
        let mut counts = self.counts.lock().unwrap();
        let Some(count) = counts.get_mut(filter) else {
            return false;
        };
        if *count == 0 {
            return false;
        }
        *count -= 1;
        if *count == 0 {
            counts.remove(filter);
            true
        } else {
            false
        }
    }

    /// A snapshot of every filter with at least one interested
    /// connection right now - what a newly-connected peer link should
    /// be caught up on.
    pub fn snapshot(&self) -> Vec<TopicFilter> {
        self.counts.lock().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn topic(s: &str) -> TopicFilter {
        TopicFilter::from_str(s).unwrap()
    }

    #[test]
    fn first_subscribe_is_a_transition() {
        let interest = Interest::new();
        assert!(interest.subscribe(topic("weather.updates")));
    }

    #[test]
    fn second_subscribe_is_not_a_transition() {
        let interest = Interest::new();
        let t = topic("weather.updates");
        interest.subscribe(t.clone());
        assert!(!interest.subscribe(t));
    }

    #[test]
    fn unsubscribe_down_to_zero_is_a_transition() {
        let interest = Interest::new();
        let t = topic("weather.updates");
        interest.subscribe(t.clone());
        assert!(interest.unsubscribe(&t));
    }

    #[test]
    fn unsubscribe_above_zero_is_not_a_transition() {
        let interest = Interest::new();
        let t = topic("weather.updates");
        interest.subscribe(t.clone());
        interest.subscribe(t.clone());
        assert!(!interest.unsubscribe(&t));
    }

    #[test]
    fn unsubscribe_with_no_prior_interest_is_a_no_op() {
        let interest = Interest::new();
        assert!(!interest.unsubscribe(&topic("weather.updates")));
    }

    #[test]
    fn a_topic_can_regain_interest_after_losing_it() {
        let interest = Interest::new();
        let t = topic("weather.updates");
        interest.subscribe(t.clone());
        interest.unsubscribe(&t);

        assert!(interest.subscribe(t));
    }

    #[test]
    fn snapshot_reflects_currently_interested_topics() {
        let interest = Interest::new();
        let weather = topic("weather.updates");
        let traffic = topic("traffic.updates");
        interest.subscribe(weather.clone());
        interest.subscribe(traffic.clone());
        interest.unsubscribe(&traffic);

        assert_eq!(interest.snapshot(), vec![weather]);
    }

    #[test]
    fn snapshot_is_empty_for_a_fresh_registry() {
        assert!(Interest::new().snapshot().is_empty());
    }

    #[test]
    fn a_wildcard_filter_is_tracked_the_same_as_a_literal_one() {
        // Interest doesn't distinguish a pattern from a plain topic
        // name - both are just a TopicFilter (ADR-0022).
        let interest = Interest::new();
        let pattern = topic("weather.+");
        assert!(interest.subscribe(pattern.clone()));
        assert!(!interest.subscribe(pattern.clone()));
        assert!(!interest.unsubscribe(&pattern));
        assert!(interest.unsubscribe(&pattern));
    }
}
