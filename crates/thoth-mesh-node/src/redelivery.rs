//! Pending-acknowledgement tracking for an `ack: true` subscription's
//! forwarder (ADR-0041): which deliveries are still waiting on an
//! ack, since when, and how many times each has already been resent.
//!
//! Deliberately not `Broker`'s concern - a resend is the forwarder
//! re-sending an `Arc<Envelope>` it already holds, never a second
//! call to [`thoth_mesh_broker::Broker::publish`], so deduplication
//! (ADR-0011) and the replay buffer (ADR-0021) are untouched by any
//! of this.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use thoth_mesh_core::{Envelope, MessageId};

/// How long an unacknowledged delivery waits before being resent.
/// Not currently configurable via a CLI/env flag - see ADR-0041.
pub const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// How many times a delivery is resent before this node gives up on
/// it ever being acknowledged (see ADR-0041's "never acks at all").
pub const DEFAULT_MAX_REDELIVERY_ATTEMPTS: u32 = 5;

struct PendingEntry {
    envelope: Arc<Envelope>,
    sent_at: Instant,
    attempts: u32,
}

/// Every delivery one ack-required forwarder has sent but not yet had
/// acknowledged, keyed by the envelope's own [`MessageId`] - the same
/// ID an `Ack { in_reply_to }` names back. See ADR-0041.
pub struct PendingAcks {
    timeout: Duration,
    max_attempts: u32,
    entries: HashMap<MessageId, PendingEntry>,
}

impl PendingAcks {
    pub fn new(timeout: Duration, max_attempts: u32) -> Self {
        Self {
            timeout,
            max_attempts,
            entries: HashMap::new(),
        }
    }

    /// Records that `envelope` was just sent, starting its
    /// redelivery clock at `now`.
    pub fn record(&mut self, envelope: Arc<Envelope>, now: Instant) {
        self.entries.insert(
            envelope.id,
            PendingEntry {
                envelope,
                sent_at: now,
                attempts: 0,
            },
        );
    }

    /// Acknowledges `id`, if it's still pending - a no-op otherwise
    /// (already acked, already given up on, or never one of this
    /// forwarder's own deliveries to begin with - see
    /// `ConnectionContext::handle_ack`, which routes every incoming
    /// `Ack` to every forwarder on the connection).
    pub fn ack(&mut self, id: MessageId) {
        self.entries.remove(&id);
    }

    /// Sweeps every entry that's gone `timeout` without an ack:
    /// resends it (bumping its attempt count and resetting its clock
    /// to `now`) if it hasn't yet hit `max_attempts`, or drops it -
    /// reported as given up on - if it has. Returns what to resend and
    /// what was given up on; neither is in any particular order. The
    /// given-up-on half carries the full envelope, not just its id -
    /// what the caller needs to dead-letter it (ADR-0047).
    pub fn sweep(&mut self, now: Instant) -> (Vec<Arc<Envelope>>, Vec<Arc<Envelope>>) {
        let expired: Vec<MessageId> = self
            .entries
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.sent_at) >= self.timeout)
            .map(|(id, _)| *id)
            .collect();

        let mut to_resend = Vec::new();
        let mut given_up_on = Vec::new();
        for id in expired {
            let entry = self
                .entries
                .get_mut(&id)
                .expect("id was just collected from entries above");
            if entry.attempts >= self.max_attempts {
                given_up_on.push(Arc::clone(&entry.envelope));
                self.entries.remove(&id);
                continue;
            }
            entry.attempts += 1;
            entry.sent_at = now;
            to_resend.push(Arc::clone(&entry.envelope));
        }
        (to_resend, given_up_on)
    }

    #[cfg(test)]
    fn pending_count(&self) -> usize {
        self.entries.len()
    }
}

/// How often a forwarder's redelivery sweep runs, given `timeout` -
/// frequent enough that an expired entry isn't kept waiting long past
/// its actual timeout, without polling far more often than the
/// timeout itself would ever need.
pub fn sweep_interval(timeout: Duration) -> Duration {
    (timeout / 4).max(Duration::from_millis(10))
}

#[cfg(test)]
mod tests {
    use super::*;
    use thoth_mesh_core::{MessageKind, PeerId, Topic};

    fn envelope() -> Arc<Envelope> {
        Arc::new(Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: "weather.updates".parse::<Topic>().unwrap(),
                payload: b"sunny".to_vec(),
                retain: false,
                content_type: None,
                reply_to: None,
                in_reply_to: None,
            },
        ))
    }

    #[test]
    fn a_fresh_entry_is_not_swept_before_its_timeout() {
        let mut pending = PendingAcks::new(Duration::from_secs(5), 3);
        let now = Instant::now();
        pending.record(envelope(), now);

        let (to_resend, given_up_on) = pending.sweep(now);
        assert!(to_resend.is_empty());
        assert!(given_up_on.is_empty());
        assert_eq!(pending.pending_count(), 1);
    }

    #[test]
    fn an_entry_past_its_timeout_is_resent_and_its_clock_resets() {
        let mut pending = PendingAcks::new(Duration::from_millis(10), 3);
        let sent_at = Instant::now();
        let sent = envelope();
        pending.record(Arc::clone(&sent), sent_at);

        let past_timeout = sent_at + Duration::from_millis(20);
        let (to_resend, given_up_on) = pending.sweep(past_timeout);
        assert_eq!(to_resend, vec![sent]);
        assert!(given_up_on.is_empty());

        // Immediately sweeping again at the same instant doesn't
        // re-resend - the clock was reset by the resend above.
        let (to_resend_again, _) = pending.sweep(past_timeout);
        assert!(to_resend_again.is_empty());
    }

    #[test]
    fn acking_a_pending_entry_removes_it_so_it_is_never_resent() {
        let mut pending = PendingAcks::new(Duration::from_millis(10), 3);
        let sent_at = Instant::now();
        let sent = envelope();
        pending.ack(sent.id); // acking before recording is a no-op
        pending.record(Arc::clone(&sent), sent_at);
        pending.ack(sent.id);

        let (to_resend, given_up_on) = pending.sweep(sent_at + Duration::from_secs(1));
        assert!(to_resend.is_empty());
        assert!(given_up_on.is_empty());
        assert_eq!(pending.pending_count(), 0);
    }

    #[test]
    fn an_entry_is_given_up_on_after_exhausting_every_redelivery_attempt() {
        let mut pending = PendingAcks::new(Duration::from_millis(10), 2);
        let mut now = Instant::now();
        let sent = envelope();
        pending.record(Arc::clone(&sent), now);

        // Two resends (max_attempts) still keep the entry alive...
        for _ in 0..2 {
            now += Duration::from_millis(20);
            let (to_resend, given_up_on) = pending.sweep(now);
            assert_eq!(to_resend, vec![Arc::clone(&sent)]);
            assert!(given_up_on.is_empty());
        }

        // ...but the third timeout gives up on it instead of resending
        // a third time.
        now += Duration::from_millis(20);
        let (to_resend, given_up_on) = pending.sweep(now);
        assert!(to_resend.is_empty());
        assert_eq!(given_up_on, vec![sent]);
        assert_eq!(pending.pending_count(), 0);
    }

    #[test]
    fn sweep_interval_is_a_quarter_of_the_timeout_with_a_floor() {
        assert_eq!(
            sweep_interval(Duration::from_secs(5)),
            Duration::from_millis(1250)
        );
        // A tiny timeout (as tests use) doesn't produce an
        // effectively-zero, spin-loop interval.
        assert_eq!(
            sweep_interval(Duration::from_millis(10)),
            Duration::from_millis(10)
        );
    }
}
