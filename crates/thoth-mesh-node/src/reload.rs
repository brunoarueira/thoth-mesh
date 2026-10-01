//! Live-reloadable node state - see ADR-0055. Deliberately knows
//! nothing about config files, `clap`, or `SIGHUP`; those stay
//! `main.rs`'s own concern (ADR-0054's same separation, extended).
//! This module is just the mechanism a caller uses to push a fresh
//! set of already-parsed values into a running node.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use crate::peer_topic_filter::PeerTopicFilter;
use crate::topic_acl::TopicAcl;

/// A field a connection reads fresh on every check, instead of
/// capturing once at connection start - the difference between a
/// reload actually reaching an already-established connection's very
/// next check, and only ever affecting connections made *after* the
/// reload (see ADR-0055's "Live state" section). Cheaply `Clone`
/// (just clones the inner `Arc`), same as `Membership`/`Interest`/
/// `PeerLinks` elsewhere in this codebase.
///
/// A plain `std::sync::RwLock`, not `tokio::sync` - every access is a
/// quick, synchronous, in-memory read or write, never held across an
/// `.await` point, the same reasoning `RateLimiter` already used
/// (ADR-0051).
#[derive(Debug)]
pub struct Reloadable<T> {
    current: Arc<RwLock<Option<Arc<T>>>>,
}

// Hand-rolled rather than derived: `#[derive(Clone)]` would add a
// `T: Clone` bound to the generated impl even though cloning a
// `Reloadable<T>` only ever clones the outer `Arc`, never `T` itself
// - the same reason `Arc<T>`'s own `Clone` impl isn't derived either.
impl<T> Clone for Reloadable<T> {
    fn clone(&self) -> Self {
        Self {
            current: Arc::clone(&self.current),
        }
    }
}

impl<T> Reloadable<T> {
    /// Starts holding `initial` - typically whatever `NodeOptions`
    /// supplied at startup (ADR-0018/ADR-0020/ADR-0049/ADR-0017), same
    /// as before this ADR.
    pub fn new(initial: Option<T>) -> Self {
        Self {
            current: Arc::new(RwLock::new(initial.map(Arc::new))),
        }
    }

    /// The current value, if one is set - cloning only the `Arc`, not
    /// `T` itself.
    pub fn get(&self) -> Option<Arc<T>> {
        self.current
            .read()
            .expect("Reloadable lock poisoned")
            .clone()
    }

    /// Replaces the current value outright - visible to the very next
    /// `.get()` call from any connection holding this same handle,
    /// including one already established before this call. The write
    /// lock is only ever held for the pointer swap itself: the
    /// previous value comes back out from under it and is dropped
    /// afterward, so dropping a large outgoing `T` (with no reader
    /// still holding its own `Arc` to it) can never block a
    /// concurrent `.get()`.
    pub fn set(&self, new: Option<T>) {
        let new = new.map(Arc::new);
        let old = {
            let mut current = self.current.write().expect("Reloadable lock poisoned");
            std::mem::replace(&mut *current, new)
        };
        drop(old);
    }
}

/// A fully-parsed, already-validated set of values for every field
/// ADR-0055 made reloadable - what a caller (`main.rs`'s `SIGHUP`
/// handler, or a test driving a `watch::Sender` directly) sends down
/// the channel `NodeOptions::reload` carries. Rejected wholesale
/// before ever reaching here if any one field failed to parse/
/// validate - never a partial subset of the four. Each field then
/// applies as its own independent `Reloadable::set` in quick
/// succession, not one combined cross-field transaction - see
/// ADR-0055 for why nothing actually needs that.
#[derive(Debug, Clone, Default)]
pub struct ReloadableAcls {
    pub topic_acl: Option<TopicAcl>,
    pub peer_topic_acl: Option<TopicAcl>,
    pub allow_peer: Option<HashSet<[u8; 32]>>,
    pub peer_topic_filter: Option<PeerTopicFilter>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_reloadable_holds_its_initial_value() {
        let r = Reloadable::new(Some(42));
        assert_eq!(r.get().as_deref(), Some(&42));
    }

    #[test]
    fn a_fresh_reloadable_with_none_holds_nothing() {
        let r: Reloadable<i32> = Reloadable::new(None);
        assert!(r.get().is_none());
    }

    #[test]
    fn set_replaces_the_value_visible_to_every_clone() {
        let r = Reloadable::new(Some(1));
        let cloned = r.clone();
        r.set(Some(2));
        assert_eq!(cloned.get().as_deref(), Some(&2));
    }

    #[test]
    fn set_to_none_clears_it() {
        let r = Reloadable::new(Some(1));
        r.set(None);
        assert!(r.get().is_none());
    }
}
