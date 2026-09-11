//! The periodic background sweep that expires on-disk messages older
//! than `--message-ttl-secs` (ADR-0047), dead-lettering each one if
//! `--dead-letter-topic` is also configured.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thoth_mesh_broker::{Broker, MessageStore};

use crate::dead_letter::{DeadLetterConfig, dead_letter};
use crate::metrics::Metrics;

/// How often the sweep runs - a fixed interval, not derived from the
/// TTL itself (unlike ADR-0041's ack-redelivery sweep, whose interval
/// scales with its own timeout): a TTL is typically hours or days, and
/// sweeping proportionally rarely at that scale would make expiry
/// unhelpfully imprecise. Not currently configurable via a CLI flag.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Spawns the background task that runs the TTL sweep every
/// [`SWEEP_INTERVAL`] until the process exits - there's no shutdown
/// signal to wait on, the same as every other long-running background
/// task this node spawns (e.g. `peering::spawn_discovery_dialer`).
pub fn spawn(
    store: Arc<dyn MessageStore>,
    broker: Arc<Broker>,
    ttl: Duration,
    dead_letter: DeadLetterConfig,
    metrics: Metrics,
) {
    spawn_with_interval(store, broker, ttl, dead_letter, metrics, SWEEP_INTERVAL);
}

/// Like [`spawn`], but with the sweep interval as a parameter - split
/// out purely so a test can use a short interval instead of waiting on
/// [`SWEEP_INTERVAL`]'s real 60 seconds.
fn spawn_with_interval(
    store: Arc<dyn MessageStore>,
    broker: Arc<Broker>,
    ttl: Duration,
    dead_letter: DeadLetterConfig,
    metrics: Metrics,
    sweep_interval: Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(sweep_interval);
        loop {
            interval.tick().await;
            sweep_once(&store, &broker, ttl, &dead_letter, &metrics).await;
        }
    });
}

/// One sweep: deletes every persisted message older than `ttl` (via
/// [`MessageStore::expire_before`]) and dead-letters each one via
/// `dead_letter` - a no-op per message if it has no topic configured,
/// the same "no dead-letter destination by default" posture every
/// other giving-up path in this project already has.
async fn sweep_once(
    store: &Arc<dyn MessageStore>,
    broker: &Broker,
    ttl: Duration,
    dead_letter_config: &DeadLetterConfig,
    metrics: &Metrics,
) {
    // A TTL longer than the time since the Unix epoch (only reachable
    // with a badly wrong system clock) has nothing to expire yet -
    // skip the sweep rather than underflow computing the cutoff.
    let Some(cutoff) = SystemTime::now().checked_sub(ttl) else {
        return;
    };
    let cutoff_ts = cutoff
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let store_for_sweep = Arc::clone(store);
    let expired =
        match tokio::task::spawn_blocking(move || store_for_sweep.expire_before(cutoff_ts)).await {
            Ok(Ok(expired)) => expired,
            Ok(Err(err)) => {
                tracing::error!(%err, "message TTL sweep failed");
                return;
            }
            Err(join_err) => {
                tracing::error!(%join_err, "message TTL sweep task panicked");
                return;
            }
        };
    if expired.is_empty() {
        return;
    }
    metrics.record_expired_messages(expired.len() as u64);
    tracing::info!(count = expired.len(), "expired messages past their TTL");

    let mut dead_lettered = 0u64;
    for envelope in &expired {
        if dead_letter(broker, dead_letter_config, envelope).await {
            dead_lettered += 1;
        }
    }
    if dead_lettered > 0 {
        metrics.record_dead_lettered_messages(dead_lettered);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use thoth_mesh_core::{Envelope, MessageKind, PeerId, Topic};

    fn publish(topic: &str, payload: &[u8]) -> Envelope {
        Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: Topic::from_str(topic).unwrap(),
                payload: payload.to_vec(),
                retain: false,
                content_type: None,
            },
        )
    }

    fn config_for(topic: &str) -> DeadLetterConfig {
        DeadLetterConfig {
            node_id: PeerId::new(),
            topic: Some(Topic::from_str(topic).unwrap()),
        }
    }

    async fn temp_store() -> (tempfile::TempDir, Arc<dyn MessageStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn MessageStore> =
            Arc::new(crate::persistence::SqliteStore::open(dir.path()).unwrap());
        (dir, store)
    }

    #[tokio::test]
    async fn sweep_once_expires_old_messages_and_dead_letters_them() {
        let (_dir, store) = temp_store().await;
        let published = publish("weather.updates", b"sunny");
        store.append(&published).unwrap();

        let broker = Broker::new();
        let config = config_for("dead-letter");
        let (_backlog, mut rx) = broker
            .subscribe(
                Topic::from_str("dead-letter.weather.updates")
                    .unwrap()
                    .into(),
            )
            .await;
        let metrics = Metrics::new();

        // A tiny sleep so "now" has genuinely moved past the append
        // above, then a tiny TTL so it counts as expired.
        tokio::time::sleep(Duration::from_millis(20)).await;
        sweep_once(&store, &broker, Duration::from_millis(1), &config, &metrics).await;

        let delivered = rx.try_recv().unwrap();
        match &delivered.kind {
            MessageKind::Publish { payload, .. } => assert_eq!(payload, b"sunny"),
            other => panic!("expected a Publish, got {other:?}"),
        }
        // Really gone from the store, not just dead-lettered on top.
        assert!(store.load_recent(10).unwrap().is_empty());
    }

    #[tokio::test]
    async fn sweep_once_drops_expired_messages_silently_with_no_dead_letter_topic_configured() {
        let (_dir, store) = temp_store().await;
        store.append(&publish("weather.updates", b"sunny")).unwrap();

        let broker = Broker::new();
        let config = DeadLetterConfig {
            node_id: PeerId::new(),
            topic: None,
        };
        let metrics = Metrics::new();
        tokio::time::sleep(Duration::from_millis(20)).await;
        sweep_once(&store, &broker, Duration::from_millis(1), &config, &metrics).await;

        assert!(store.load_recent(10).unwrap().is_empty());
    }

    #[tokio::test]
    async fn sweep_once_is_a_no_op_when_nothing_has_aged_past_the_ttl_yet() {
        let (_dir, store) = temp_store().await;
        let published = publish("weather.updates", b"sunny");
        store.append(&published).unwrap();

        let broker = Broker::new();
        let config = config_for("dead-letter");
        let metrics = Metrics::new();
        sweep_once(
            &store,
            &broker,
            Duration::from_secs(3600),
            &config,
            &metrics,
        )
        .await;

        let remaining = store.load_recent(10).unwrap();
        assert_eq!(remaining[0].1[0].id, published.id);
    }

    /// The full background loop, not just one sweep - proves `spawn`
    /// actually ticks and calls through, using a short interval so the
    /// test doesn't wait on the real 60s default.
    #[tokio::test]
    async fn spawn_runs_the_sweep_periodically() {
        let (_dir, store) = temp_store().await;
        store.append(&publish("weather.updates", b"sunny")).unwrap();

        let broker = Arc::new(Broker::new());
        let config = config_for("dead-letter");
        let (_backlog, mut rx) = broker
            .subscribe(
                Topic::from_str("dead-letter.weather.updates")
                    .unwrap()
                    .into(),
            )
            .await;

        tokio::time::sleep(Duration::from_millis(20)).await;
        spawn_with_interval(
            store,
            broker,
            Duration::from_millis(1),
            config,
            Metrics::new(),
            Duration::from_millis(10),
        );

        let delivered = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for the sweep to run")
            .unwrap();
        match &delivered.kind {
            MessageKind::Publish { payload, .. } => assert_eq!(payload, b"sunny"),
            other => panic!("expected a Publish, got {other:?}"),
        }
    }
}
