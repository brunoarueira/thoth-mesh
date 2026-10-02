//! Whether this node is ready to accept connections - see ADR-0056.
//! A one-way latch: starts `false`, flipped to `true` once, never back
//! - see the ADR for why nothing today needs it to go the other way.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Cheaply `Clone` (an `Arc<AtomicBool>` under the hood), same pattern
/// as `Membership`/`Interest`/`PeerLinks` elsewhere in this crate -
/// every clone observes the same underlying flag.
#[derive(Debug, Clone, Default)]
pub struct Readiness(Arc<AtomicBool>);

impl Readiness {
    /// Starts not ready.
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Flips this node to ready - visible to every clone of this same
    /// handle immediately. Idempotent; calling it again is a no-op.
    pub fn mark_ready(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_readiness_is_not_ready() {
        assert!(!Readiness::new().is_ready());
    }

    #[test]
    fn mark_ready_is_visible_to_every_clone() {
        let readiness = Readiness::new();
        let cloned = readiness.clone();
        readiness.mark_ready();
        assert!(cloned.is_ready());
    }

    #[test]
    fn mark_ready_is_idempotent() {
        let readiness = Readiness::new();
        readiness.mark_ready();
        readiness.mark_ready();
        assert!(readiness.is_ready());
    }
}
