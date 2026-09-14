//! The always-on background sweep that reclaims or gives up on
//! work-queue consumer-group deliveries (ADR-0048): a group joined
//! with `ack: true` gets lease-based redelivery - reclaimable by
//! *any* live member, not just the one it was originally sent to -
//! instead of ADR-0041's same-connection redelivery.

use std::sync::Arc;
use std::time::{Duration, Instant};

use thoth_mesh_broker::Broker;

use crate::dead_letter::{DeadLetterConfig, dead_letter};
use crate::metrics::Metrics;
use crate::redelivery::{DEFAULT_ACK_TIMEOUT, DEFAULT_MAX_REDELIVERY_ATTEMPTS, sweep_interval};

/// Spawns the background task that sweeps every consumer group's
/// work-queue leases, unconditionally - the same "always running,
/// cheap when there's nothing to do" posture
/// `peering::spawn_discovery_dialer` already has. Unlike
/// `ttl::spawn` (opt-in behind `--data-dir`/`--persisted-message-ttl-secs`),
/// there's no flag gating this: a work-queue group can be created at
/// any time by any client's `Subscribe { ack: true, group: Some(_) }`,
/// so the sweeper has to already be running when the first one shows
/// up rather than being started lazily. Reuses ADR-0041's
/// `DEFAULT_ACK_TIMEOUT`/`DEFAULT_MAX_REDELIVERY_ATTEMPTS` - no
/// evidence yet that work-queue groups need different defaults than
/// ordinary `ack: true` delivery does.
pub fn spawn(broker: Arc<Broker>, dead_letter_config: DeadLetterConfig, metrics: Metrics) {
    spawn_with(
        broker,
        dead_letter_config,
        metrics,
        DEFAULT_ACK_TIMEOUT,
        DEFAULT_MAX_REDELIVERY_ATTEMPTS,
        sweep_interval(DEFAULT_ACK_TIMEOUT),
    );
}

/// Like [`spawn`], but with the lease timeout, max attempts, and sweep
/// interval all as parameters - split out purely so a test can use
/// small values instead of waiting on the real, multi-second defaults.
fn spawn_with(
    broker: Arc<Broker>,
    dead_letter_config: DeadLetterConfig,
    metrics: Metrics,
    ack_timeout: Duration,
    max_redelivery_attempts: u32,
    sweep_interval: Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(sweep_interval);
        loop {
            interval.tick().await;
            sweep_once(
                &broker,
                &dead_letter_config,
                &metrics,
                ack_timeout,
                max_redelivery_attempts,
            )
            .await;
        }
    });
}

/// One sweep: reclaims or gives up on every ack-tracked group's leases
/// older than `ack_timeout` (via [`Broker::sweep_group_leases`]).
/// Anything given up on is dead-lettered via `dead_letter_config` if
/// it has a topic configured (ADR-0047) - otherwise just dropped, the
/// same as an ordinary `ack: true` giveup with no `--dead-letter-topic`
/// set. Counted under the same metrics an ordinary `ack: true`
/// subscription's redelivery/giveup/dead-letter already use - no
/// group-specific metric.
async fn sweep_once(
    broker: &Broker,
    dead_letter_config: &DeadLetterConfig,
    metrics: &Metrics,
    ack_timeout: Duration,
    max_redelivery_attempts: u32,
) {
    let given_up_on =
        broker.sweep_group_leases(Instant::now(), ack_timeout, max_redelivery_attempts);
    if given_up_on.is_empty() {
        return;
    }
    metrics.record_delivery_ack_timeouts(given_up_on.len() as u64);
    let mut dead_lettered = 0u64;
    for envelope in &given_up_on {
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
    use tokio::sync::mpsc;

    fn publish(topic: &str, payload: &[u8]) -> Arc<Envelope> {
        Arc::new(Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: Topic::from_str(topic).unwrap(),
                payload: payload.to_vec(),
                retain: false,
                content_type: None,
            },
        ))
    }

    fn config_for(topic: &str) -> DeadLetterConfig {
        DeadLetterConfig {
            node_id: PeerId::new(),
            topic: Some(Topic::from_str(topic).unwrap()),
        }
    }

    fn no_dead_letter() -> DeadLetterConfig {
        DeadLetterConfig {
            node_id: PeerId::new(),
            topic: None,
        }
    }

    #[tokio::test]
    async fn sweep_once_redelivers_an_expired_lease_to_another_member() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let (tx_a, mut rx_a) = mpsc::channel(8);
        let (tx_b, mut rx_b) = mpsc::channel(8);
        broker.join_group(topic.clone().into(), "workers".to_owned(), tx_a, true);
        broker.join_group(topic.clone().into(), "workers".to_owned(), tx_b, true);

        let envelope = publish("weather.updates", b"sunny");
        broker.publish(&topic, envelope.clone()).await;
        rx_a.try_recv().unwrap();

        sweep_once(
            &broker,
            &no_dead_letter(),
            &Metrics::new(),
            Duration::ZERO,
            5,
        )
        .await;

        assert_eq!(rx_b.try_recv().unwrap().id, envelope.id);
    }

    #[tokio::test]
    async fn sweep_once_drops_a_giveup_silently_with_no_dead_letter_topic_configured() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let (tx, mut rx) = mpsc::channel(8);
        broker.join_group(topic.clone().into(), "workers".to_owned(), tx, true);

        broker
            .publish(&topic, publish("weather.updates", b"sunny"))
            .await;
        rx.try_recv().unwrap();

        // max_redelivery_attempts of 0 - the very first sweep past the
        // (zero) timeout gives up immediately.
        sweep_once(
            &broker,
            &no_dead_letter(),
            &Metrics::new(),
            Duration::ZERO,
            0,
        )
        .await;

        assert!(rx.try_recv().is_err(), "nothing more to redeliver");
    }

    #[tokio::test]
    async fn sweep_once_dead_letters_a_delivery_it_gives_up_on() {
        let broker = Arc::new(Broker::new());
        let topic = Topic::from_str("weather.updates").unwrap();
        let (tx, mut rx) = mpsc::channel(8);
        broker.join_group(topic.clone().into(), "workers".to_owned(), tx, true);

        let config = config_for("dead-letter");
        let (_backlog, mut dl_rx) = broker
            .subscribe(
                Topic::from_str("dead-letter.weather.updates")
                    .unwrap()
                    .into(),
            )
            .await;

        let envelope = publish("weather.updates", b"sunny");
        broker.publish(&topic, envelope.clone()).await;
        rx.try_recv().unwrap();

        sweep_once(&broker, &config, &Metrics::new(), Duration::ZERO, 0).await;

        let delivered = dl_rx.try_recv().unwrap();
        match &delivered.kind {
            MessageKind::Publish { payload, .. } => assert_eq!(payload, b"sunny"),
            other => panic!("expected a Publish, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sweep_once_is_a_no_op_when_no_lease_has_expired_yet() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let (tx, mut rx) = mpsc::channel(8);
        broker.join_group(topic.clone().into(), "workers".to_owned(), tx, true);

        broker
            .publish(&topic, publish("weather.updates", b"sunny"))
            .await;
        rx.try_recv().unwrap();

        sweep_once(
            &broker,
            &no_dead_letter(),
            &Metrics::new(),
            Duration::from_secs(3600),
            5,
        )
        .await;

        assert!(
            rx.try_recv().is_err(),
            "nothing should have been redelivered yet"
        );
    }

    /// The full background loop, not just one sweep - proves `spawn`
    /// actually ticks and calls through, using short parameters so the
    /// test doesn't wait on the real multi-second defaults.
    #[tokio::test]
    async fn spawn_runs_the_sweep_periodically_and_reclaims_across_members() {
        let broker = Arc::new(Broker::new());
        let topic = Topic::from_str("weather.updates").unwrap();
        let (tx_a, mut rx_a) = mpsc::channel(8);
        let (tx_b, mut rx_b) = mpsc::channel(8);
        broker.join_group(topic.clone().into(), "workers".to_owned(), tx_a, true);
        broker.join_group(topic.clone().into(), "workers".to_owned(), tx_b, true);

        let envelope = publish("weather.updates", b"sunny");
        broker.publish(&topic, envelope.clone()).await;
        assert_eq!(rx_a.try_recv().unwrap().id, envelope.id);

        spawn_with(
            Arc::clone(&broker),
            no_dead_letter(),
            Metrics::new(),
            Duration::from_millis(1),
            5,
            Duration::from_millis(10),
        );

        let reclaimed = tokio::time::timeout(Duration::from_secs(2), rx_b.recv())
            .await
            .expect("timed out waiting for the sweep to reclaim it")
            .unwrap();
        assert_eq!(reclaimed.id, envelope.id);
    }
}
