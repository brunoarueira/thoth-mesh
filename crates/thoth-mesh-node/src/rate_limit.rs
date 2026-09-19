//! Per-principal publish rate limiting via an in-memory token bucket -
//! `--publish-rate-limit-per-sec`/`--publish-rate-limit-burst`. See
//! ADR-0051.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::topic_acl::Principal;

/// How many distinct principals' buckets this node remembers at once -
/// same cap and reasoning as every other per-identity map in this
/// codebase (membership, peer directory, topic/pattern subscribers -
/// see ADR-0025). Not currently configurable via a flag.
pub const DEFAULT_RATE_LIMIT_PRINCIPAL_CAPACITY: usize = 4096;

/// `--publish-rate-limit-per-sec`/`--publish-rate-limit-burst`, parsed
/// once at startup. See ADR-0051.
#[derive(Debug, Default, Clone, Copy)]
pub struct RateLimitConfig {
    /// Sustained tokens refilled per second.
    pub per_sec: u32,
    /// Bucket capacity - how far ahead of the sustained rate a
    /// principal that's been idle can get before being throttled
    /// again.
    pub burst: u32,
}

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

/// `buckets` and `order` live under one lock, not two - avoids any
/// lock-ordering question between the map and its eviction queue (see
/// ADR-0025).
#[derive(Default)]
struct RateLimiterState {
    buckets: HashMap<Principal, Bucket>,
    /// FIFO of principals in first-seen order - the front is the
    /// longest-tracked (and therefore next-to-evict) principal.
    /// Deliberately not touch-refreshed like the peer directory's own
    /// eviction queue: this is checked on every single `Publish`, and
    /// an `O(n)` reorder-on-touch scan isn't worth paying for the
    /// extra precision on that hot a path.
    order: VecDeque<Principal>,
}

/// Tracks one token bucket per [`Principal`] that has published at
/// least once - shared across every connection from the same
/// principal (`Arc`-wrapped in `Shared`/`ConnectionContext`, same as
/// `TopicAcl`), since the whole point is catching a principal that
/// floods across more than one connection, not just one. See
/// ADR-0051.
pub struct RateLimiter {
    config: RateLimitConfig,
    state: Mutex<RateLimiterState>,
    principal_evictions: AtomicU64,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            state: Mutex::new(RateLimiterState::default()),
            principal_evictions: AtomicU64::new(0),
        }
    }

    /// Whether `principal` may publish right now - `true` consumes one
    /// token from its bucket, `false` leaves it untouched (nothing to
    /// "refund"; there was nothing to spend). A principal seen for the
    /// first time starts with a full bucket (`burst` tokens) - the
    /// same "innocent until it actually floods" posture every other
    /// rejection-based check in this codebase takes.
    pub fn allow(&self, principal: Principal) -> bool {
        let mut state = self.state.lock().expect("rate limiter mutex poisoned");
        let now = Instant::now();
        let is_new = !state.buckets.contains_key(&principal);
        let bucket = state.buckets.entry(principal).or_insert_with(|| Bucket {
            tokens: self.config.burst as f64,
            last_refill: now,
        });
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens =
            (bucket.tokens + elapsed * self.config.per_sec as f64).min(self.config.burst as f64);
        bucket.last_refill = now;
        let allowed = if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        };
        if is_new {
            state.order.push_back(principal);
            if state.order.len() > DEFAULT_RATE_LIMIT_PRINCIPAL_CAPACITY
                && let Some(oldest) = state.order.pop_front()
            {
                state.buckets.remove(&oldest);
                self.principal_evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        allowed
    }

    /// How many principals' buckets have been reclaimed for the table
    /// sitting over [`DEFAULT_RATE_LIMIT_PRINCIPAL_CAPACITY`] (see
    /// ADR-0025/ADR-0051).
    pub fn principal_evictions(&self) -> u64 {
        self.principal_evictions.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal() -> Principal {
        Principal::Fingerprint([1u8; 32])
    }

    #[test]
    fn allows_up_to_burst_then_rejects() {
        let limiter = RateLimiter::new(RateLimitConfig {
            per_sec: 1,
            burst: 3,
        });
        let p = principal();
        assert!(limiter.allow(p));
        assert!(limiter.allow(p));
        assert!(limiter.allow(p));
        assert!(!limiter.allow(p));
    }

    #[test]
    fn distinct_principals_have_independent_buckets() {
        let limiter = RateLimiter::new(RateLimitConfig {
            per_sec: 1,
            burst: 1,
        });
        let a = Principal::Fingerprint([1u8; 32]);
        let b = Principal::Fingerprint([2u8; 32]);
        assert!(limiter.allow(a));
        assert!(!limiter.allow(a));
        assert!(limiter.allow(b));
    }

    #[test]
    fn anonymous_connections_share_one_bucket() {
        let limiter = RateLimiter::new(RateLimitConfig {
            per_sec: 1,
            burst: 1,
        });
        assert!(limiter.allow(Principal::Anonymous));
        assert!(!limiter.allow(Principal::Anonymous));
    }

    #[test]
    fn refills_over_time_up_to_burst() {
        // Fast enough to have clearly refilled after a 20ms sleep (4
        // tokens' worth), slow enough that the near-instant gap
        // between the first two calls below can't itself refill a
        // whole token.
        let limiter = RateLimiter::new(RateLimitConfig {
            per_sec: 200,
            burst: 1,
        });
        let p = principal();
        assert!(limiter.allow(p));
        assert!(!limiter.allow(p));
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(limiter.allow(p));
    }

    #[test]
    fn over_capacity_evicts_the_longest_tracked_principal() {
        let limiter = RateLimiter::new(RateLimitConfig {
            per_sec: 1,
            burst: 1,
        });
        for i in 0..=DEFAULT_RATE_LIMIT_PRINCIPAL_CAPACITY as u16 {
            let mut fingerprint = [0u8; 32];
            fingerprint[..2].copy_from_slice(&i.to_le_bytes());
            limiter.allow(Principal::Fingerprint(fingerprint));
        }
        assert_eq!(limiter.principal_evictions(), 1);
    }
}
