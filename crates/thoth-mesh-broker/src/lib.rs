//! In-process pub/sub broker: topic registry and subscriber dispatch.
//!
//! See ADR-0006 for the design rationale, ADR-0011 for the
//! duplicate-envelope dedup this crate now also does, ADR-0021 for the
//! per-topic replay buffer that lets a late subscriber catch up on
//! recent history, ADR-0022 for wildcard topic filters, ADR-0024 for
//! why the replay buffer is sized larger than the broadcast channel it
//! sits alongside, ADR-0025 for why `topics`/`patterns` are each
//! capped, never evicting an entry with a live subscriber, ADR-0042
//! for consumer groups - `publish`'s other delivery path, exactly one
//! member per message rather than fan-out to every subscriber - and
//! ADR-0043 for retained (last-value) messages, delivered to a later
//! subscriber even after they've fallen out of the replay window.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use thoth_mesh_core::{Envelope, MessageId, MessageKind, PeerId, Topic, TopicFilter};
use tokio::sync::{RwLock, broadcast, mpsc};

/// Default channel capacity for a topic's broadcast channel.
///
/// Bounds how many envelopes can be buffered for a subscriber before it
/// starts lagging (see [`tokio::sync::broadcast`]'s lag semantics).
pub const DEFAULT_TOPIC_CHANNEL_CAPACITY: usize = 256;

/// How many recently-published message IDs [`Broker`] remembers for
/// duplicate detection (see ADR-0011) - bounded so memory doesn't grow
/// without limit on a long-running node.
pub const DEFAULT_DEDUP_CAPACITY: usize = 4096;

/// How many recent envelopes each topic's replay buffer keeps for a
/// late subscriber to catch up on (see ADR-0021), and, since ADR-0024,
/// for a lagged forwarder to recover from too. Deliberately *larger*
/// than [`DEFAULT_TOPIC_CHANNEL_CAPACITY`] - see ADR-0024's Decision
/// for why this gap has to exist for lag recovery to ever find
/// anything: a `broadcast::Receiver` only reports `Lagged` once its
/// unread messages have already fallen outside the broadcast channel's
/// own `DEFAULT_TOPIC_CHANNEL_CAPACITY`-sized window, so a buffer sized
/// the same as that channel (as this constant originally was under
/// ADR-0021, before recovery existed) would have already evicted the
/// exact same range by the time recovery could look for it. Not
/// currently configurable via a CLI flag.
pub const DEFAULT_REPLAY_BUFFER_CAPACITY: usize = 1024;

/// How many distinct entries [`Broker`]'s `topics` map and `patterns`
/// map each keep, independently, before the least-recently-touched
/// entry with no live subscriber is reclaimed (see ADR-0025). A
/// `TopicChannel` with at least one live [`broadcast::Receiver`] is
/// never a candidate, no matter how long it's gone untouched - this
/// bounds accumulated cruft (topics or patterns nobody's listened to
/// in a long time), not currently-live subscriptions, which stay
/// unbounded on purpose. Not currently configurable via a CLI flag.
pub const DEFAULT_TOPIC_MAP_CAPACITY: usize = 4096;

/// A durable sink for published messages (ADR-0045). Implemented by
/// `thoth-mesh-node`'s SQLite-backed store; the broker only knows this
/// trait, never the storage engine.
///
/// Methods are synchronous - `Broker::publish` calls [`append`](Self::append)
/// from `tokio::task::spawn_blocking`, and [`load_recent`](Self::load_recent)/
/// [`load_retained`](Self::load_retained) run once at startup before
/// the node accepts connections.
pub trait MessageStore: std::fmt::Debug + Send + Sync + 'static {
    /// Durably record `envelope` (always a `Publish`). Called once per
    /// distinct, non-duplicate publish, before any in-memory delivery.
    /// A `retain: true` envelope also updates the topic's stored
    /// retained message (an empty payload clears it - see ADR-0043).
    fn append(&self, envelope: &Envelope) -> std::io::Result<()>;

    /// Every topic's most recent `per_topic` messages, oldest-first
    /// per topic - used to refill each topic's replay buffer
    /// (ADR-0021) on startup.
    fn load_recent(&self, per_topic: usize) -> std::io::Result<Vec<(Topic, Vec<Arc<Envelope>>)>>;

    /// Every topic's current retained message (ADR-0043), for
    /// refilling retained slots on startup.
    fn load_retained(&self) -> std::io::Result<Vec<(Topic, Arc<Envelope>)>>;

    /// Every message persisted for `topic` after `after` (exclusive),
    /// oldest first - the disk catch-up a durable subscription resumes
    /// from (ADR-0046). A message persisted before `msg_id` tracking
    /// existed is never returned, regardless of `after`.
    fn messages_since(
        &self,
        topic: &Topic,
        after: MessageId,
    ) -> std::io::Result<Vec<Arc<Envelope>>>;

    /// `subscriber`'s last recorded position for `topic`, if it has
    /// one - `None` means this `(subscriber, topic)` pair has never
    /// been durably tracked before (ADR-0046).
    fn load_offset(&self, subscriber: PeerId, topic: &Topic) -> std::io::Result<Option<MessageId>>;

    /// Records `message_id` as `subscriber`'s new position for
    /// `topic` (ADR-0046) - called after every durable delivery.
    fn record_offset(
        &self,
        subscriber: PeerId,
        topic: &Topic,
        message_id: MessageId,
    ) -> std::io::Result<()>;
}

/// Why [`Broker::subscribe_durable`] refused a request, before ever
/// touching the store (ADR-0046).
#[derive(Debug)]
pub enum DurableSubscribeError {
    /// No [`MessageStore`] is configured for this broker - durable
    /// subscriptions need somewhere to record a position.
    NoStore,
    /// `filter` is a wildcard pattern (ADR-0022), not a literal topic -
    /// one offset can't mean several topics.
    WildcardFilter,
    /// The store itself failed while reading the existing position or
    /// the catch-up history.
    Io(std::io::Error),
}

impl std::fmt::Display for DurableSubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoStore => write!(
                f,
                "no message store is configured for durable subscriptions"
            ),
            Self::WildcardFilter => write!(
                f,
                "durable subscriptions require a literal topic, not a wildcard filter"
            ),
            Self::Io(err) => write!(f, "durable subscribe store error: {err}"),
        }
    }
}

impl std::error::Error for DurableSubscribeError {}

/// An in-process pub/sub broker: routes published envelopes to the
/// subscribers registered for their topic.
///
/// The broker only understands topic-addressed delivery, not envelope
/// semantics - interpreting an incoming message's `MessageKind` and
/// calling [`subscribe`](Broker::subscribe)/[`publish`](Broker::publish)
/// accordingly is the caller's job. The one exception is duplicate
/// detection: every hop of a forwarded envelope keeps its original
/// `MessageId`, and every hop - including the original local publish -
/// already flows through [`publish`](Broker::publish), which makes this
/// the natural place to stop an envelope that's looped back around a
/// cyclic peer mesh from circulating forever (see ADR-0011).
#[derive(Debug)]
pub struct Broker {
    topics: RwLock<TopicMap<Topic>>,
    /// Wildcard filter subscriptions (ADR-0022) - kept separate from
    /// `topics` so the exact-match path above is completely unchanged
    /// (same type, same lookup, same cost) for the common
    /// non-wildcard case.
    patterns: RwLock<TopicMap<TopicFilter>>,
    seen: Mutex<SeenIds>,
    messages_published: AtomicU64,
    /// How many `topics` entries have been reclaimed for being over
    /// [`DEFAULT_TOPIC_MAP_CAPACITY`] with no live subscriber (see
    /// ADR-0025).
    topic_evictions: AtomicU64,
    /// Same as `topic_evictions`, for `patterns`.
    pattern_evictions: AtomicU64,
    /// Consumer groups (ADR-0042), keyed by `(filter, group name)`.
    /// Scanned linearly on every publish - same tradeoff `patterns`
    /// already accepts (ADR-0022) - to find every group whose filter
    /// matches the topic being published. A plain (sync) `Mutex`,
    /// not an `RwLock` like `topics`/`patterns`: every access either
    /// mutates the round-robin cursor or the member list, so there's
    /// no genuinely-shared-read case here to justify the extra
    /// complexity.
    groups: Mutex<HashMap<GroupKey, GroupMembers>>,
    /// Where each distinct publish is durably recorded (ADR-0045).
    /// `None` - the default - is fully in-memory, exactly as before
    /// this ADR; `Some` is wired by `thoth-mesh-node` when
    /// `--data-dir` is set.
    store: Option<Arc<dyn MessageStore>>,
    /// How many publishes the store failed to durably record
    /// (ADR-0045) - delivery still happened, but those messages
    /// won't survive a restart. Always 0 with no store configured.
    persist_failures: AtomicU64,
}

impl Default for Broker {
    fn default() -> Self {
        Self {
            topics: RwLock::default(),
            patterns: RwLock::default(),
            seen: Mutex::new(SeenIds::new(DEFAULT_DEDUP_CAPACITY)),
            messages_published: AtomicU64::new(0),
            topic_evictions: AtomicU64::new(0),
            pattern_evictions: AtomicU64::new(0),
            groups: Mutex::new(HashMap::new()),
            store: None,
            persist_failures: AtomicU64::new(0),
        }
    }
}

impl Broker {
    /// Creates a new, empty broker with no durable store - fully
    /// in-memory (ADR-0006/ADR-0021).
    pub fn new() -> Self {
        Self::default()
    }

    /// A broker that also durably records every distinct publish to
    /// `store` before delivering it, and whose replay buffers /
    /// retained slots can be refilled from it on startup (ADR-0045).
    pub fn with_store(store: Arc<dyn MessageStore>) -> Self {
        Self {
            store: Some(store),
            ..Self::default()
        }
    }

    /// Refills `topic`'s replay buffer with `envelopes` (oldest-first),
    /// for startup rehydration from a [`MessageStore`] (ADR-0045).
    /// Does not broadcast (there are no subscribers yet) and does not
    /// touch the `seen` dedup set (a restart is a legitimate reset of
    /// that bounded window - see ADR-0011). Caps at
    /// [`DEFAULT_REPLAY_BUFFER_CAPACITY`], keeping the newest.
    pub async fn rehydrate_buffer(&self, topic: &Topic, envelopes: Vec<Arc<Envelope>>) {
        if envelopes.is_empty() {
            return;
        }
        let channel = {
            let mut topics = self.topics.write().await;
            topics.get_or_insert(topic.clone(), &self.topic_evictions)
        };
        let mut state = channel.state.lock().unwrap();
        for envelope in envelopes {
            state.buffer.push_back(envelope);
        }
        while state.buffer.len() > DEFAULT_REPLAY_BUFFER_CAPACITY {
            state.buffer.pop_front();
        }
    }

    /// Sets `topic`'s retained message to `envelope`, for startup
    /// rehydration from a [`MessageStore`] (ADR-0045).
    pub async fn rehydrate_retained(&self, topic: &Topic, envelope: Arc<Envelope>) {
        let channel = {
            let mut topics = self.topics.write().await;
            topics.get_or_insert(topic.clone(), &self.topic_evictions)
        };
        channel.state.lock().unwrap().retained = Some(envelope);
    }

    /// Subscribes to `filter`, returning its current replay backlog
    /// (oldest first, see ADR-0021) alongside a receiver that yields
    /// every envelope published to a topic `filter` matches, from this
    /// point on.
    ///
    /// A literal `filter` (see [`TopicFilter::is_literal`]) is routed
    /// to the same exact-match registry as before ADR-0022, with no
    /// behavior change; a genuine pattern gets its own
    /// [`TopicChannel`], keyed on the filter itself.
    ///
    /// The backlog and the receiver together cover every matching
    /// envelope published from the moment this call resolves, with no
    /// gap and no duplicate - see [`TopicChannel::subscribe`] for why
    /// that's guaranteed even against a concurrent `publish`.
    ///
    /// Unsubscribing is just dropping the returned receiver.
    pub async fn subscribe(
        &self,
        filter: TopicFilter,
    ) -> (Vec<Arc<Envelope>>, broadcast::Receiver<Arc<Envelope>>) {
        match filter.as_topic() {
            // Exact topic: its own channel's `subscribe` already folds
            // in that topic's retained message (ADR-0043) under the
            // same lock as the buffer snapshot, so it's race-free.
            Some(topic) => {
                let mut topics = self.topics.write().await;
                topics
                    .get_or_insert(topic, &self.topic_evictions)
                    .subscribe()
            }
            // Wildcard: the pattern channel has no retained slot of its
            // own (ADR-0043) - gather each matching *exact* topic's
            // retained message by scanning `topics`, merge them into
            // the pattern's replay backlog deduplicated by `MessageId`,
            // and sort the whole thing by id (a UUIDv7, monotonic by
            // creation time) so retained values and replay history land
            // in a coherent order.
            None => {
                let (mut backlog, receiver) = {
                    let mut patterns = self.patterns.write().await;
                    patterns
                        .get_or_insert(filter.clone(), &self.pattern_evictions)
                        .subscribe()
                };
                let topics = self.topics.read().await;
                for (topic, channel) in topics.iter() {
                    if filter.matches(topic)
                        && let Some(retained) = channel.retained_snapshot()
                        && !backlog.iter().any(|e| e.id == retained.id)
                    {
                        backlog.push(retained);
                    }
                }
                backlog.sort_by_key(|e| e.id);
                (backlog, receiver)
            }
        }
    }

    /// Whether a [`MessageStore`] is configured - durable subscriptions
    /// (ADR-0046) need one; nothing else does.
    pub fn has_store(&self) -> bool {
        self.store.is_some()
    }

    /// Subscribes `subscriber` to `filter` *durably* (ADR-0046):
    /// `filter` must be a literal topic (a wildcard is refused - one
    /// offset can't mean several topics), and a [`MessageStore`] must
    /// be configured (refused otherwise, rather than silently
    /// downgrading to an ordinary subscribe).
    ///
    /// The returned backlog is `subscriber`'s disk catch-up since its
    /// last recorded position for this topic (everything, unbounded -
    /// or nothing, if this is the first time this `(subscriber, topic)`
    /// pair has ever been seen, in which case this behaves exactly
    /// like [`subscribe`](Self::subscribe)) merged with whatever part
    /// of the ordinary in-memory backlog (which already folds in the
    /// topic's retained message, ADR-0043) is newer than that recorded
    /// position, deduplicated by `MessageId` and sorted by it - a
    /// still-buffered copy of a message already delivered before this
    /// subscriber reconnected is not redelivered. The disk read happens
    /// *before* the in-memory snapshot and
    /// receiver registration, not under the same lock - see ADR-0046
    /// for why the resulting narrow race can only ever double-deliver,
    /// never lose, a message at the seam.
    ///
    /// Recording each delivery as `subscriber`'s new position is the
    /// caller's job (see `record_delivered`) - this call only reads.
    pub async fn subscribe_durable(
        &self,
        filter: TopicFilter,
        subscriber: PeerId,
    ) -> Result<(Vec<Arc<Envelope>>, broadcast::Receiver<Arc<Envelope>>), DurableSubscribeError>
    {
        let topic = filter
            .as_topic()
            .ok_or(DurableSubscribeError::WildcardFilter)?;
        let store = self.store.clone().ok_or(DurableSubscribeError::NoStore)?;

        let (last_id, catchup) = {
            let store = Arc::clone(&store);
            let topic = topic.clone();
            tokio::task::spawn_blocking(move || match store.load_offset(subscriber, &topic)? {
                Some(last_id) => Ok((Some(last_id), store.messages_since(&topic, last_id)?)),
                None => Ok((None, Vec::new())),
            })
            .await
            .map_err(|err| DurableSubscribeError::Io(std::io::Error::other(err)))?
            .map_err(DurableSubscribeError::Io)?
        };

        let (buffered, receiver) = self.subscribe(filter).await;
        let mut backlog = catchup;
        for envelope in buffered {
            // Already delivered and recorded before this subscriber
            // reconnected - the disk catch-up above is the source of
            // truth for "since my last position", so a still-buffered
            // copy of the same old message must not come back too.
            if last_id.is_some_and(|last| envelope.id <= last) {
                continue;
            }
            if !backlog.iter().any(|e| e.id == envelope.id) {
                backlog.push(envelope);
            }
        }
        backlog.sort_by_key(|e| e.id);
        Ok((backlog, receiver))
    }

    /// Records `message_id` as `subscriber`'s new durably-tracked
    /// position for `topic` (ADR-0046). Best-effort, like the persist
    /// path itself (ADR-0045): a failure is logged, not surfaced - it
    /// just means the next reconnect might replay a little more than
    /// strictly necessary, never less. A no-op if no store is
    /// configured (shouldn't happen in practice - `subscribe_durable`
    /// already requires one - but this is never the caller's problem
    /// to re-check).
    pub async fn record_delivered(&self, subscriber: PeerId, topic: Topic, message_id: MessageId) {
        let Some(store) = self.store.clone() else {
            return;
        };
        let result = tokio::task::spawn_blocking(move || {
            store.record_offset(subscriber, &topic, message_id)
        })
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!(%err, "failed to record durable subscriber offset"),
            Err(join_err) => tracing::error!(%join_err, "durable-offset task panicked"),
        }
    }

    /// Joins `sender` to the named consumer `group` for `filter`
    /// (ADR-0042): each `Publish` whose topic `filter` matches from
    /// this point on goes to exactly one *current* member of the
    /// group, round-robin, rather than every member - a fundamentally
    /// different delivery model than [`subscribe`](Self::subscribe)'s
    /// fan-out. Delivery is a direct push onto `sender` itself
    /// (`try_send`, see [`publish`](Self::publish)), not a
    /// `broadcast::Receiver` - there is no backlog replay (ADR-0021)
    /// or lag recovery (ADR-0024) for a group; a member only ever
    /// sees what's published while it's a live, keeping-up member.
    pub fn join_group(
        &self,
        filter: TopicFilter,
        group: String,
        sender: mpsc::Sender<Arc<Envelope>>,
    ) {
        self.groups
            .lock()
            .unwrap()
            .entry((filter, group))
            .or_default()
            .members
            .push(sender);
    }

    /// Removes `sender` from the named consumer `group` for `filter`,
    /// if it's still a member - matched by comparing the exact
    /// channel (`Sender::same_channel`), the same guard
    /// `thoth_mesh_node::PeerLinks::unregister` uses, so a stale
    /// teardown can't remove a different connection's still-live
    /// membership. A no-op if `sender` was never a member of this
    /// `(filter, group)` (or already removed) - safe to call
    /// unconditionally on disconnect.
    pub fn leave_group(
        &self,
        filter: TopicFilter,
        group: String,
        sender: &mpsc::Sender<Arc<Envelope>>,
    ) {
        let mut groups = self.groups.lock().unwrap();
        if let Some(members) = groups.get_mut(&(filter, group)) {
            members
                .members
                .retain(|member| !member.same_channel(sender));
        }
    }

    /// Publishes `envelope` to every subscriber currently registered
    /// for `topic` - exact-match subscribers and every currently
    /// registered pattern filter that matches `topic` (ADR-0022) -
    /// plus, for every consumer group whose filter matches `topic`
    /// (ADR-0042), exactly one of its current members - returning how
    /// many receivers got it live in total (each such group counts as
    /// at most one).
    ///
    /// `envelope` is also appended to each matching *fan-out* channel's
    /// replay buffer (ADR-0021) regardless of whether anyone is
    /// currently subscribed there - a topic (or pattern) with no
    /// subscribers yet still builds up a backlog for whoever
    /// subscribes later; a consumer group has no such backlog (see
    /// [`join_group`](Self::join_group)). A connection holding both an
    /// exact subscribe and an independently matching pattern subscribe
    /// receives the envelope twice, once per subscription - each is
    /// delivered through its own `TopicChannel`, same as two distinct
    /// clients would be. Returns `0` if there are no live subscribers
    /// right now - this is not an error, publishing to a topic nobody
    /// is listening to is normal - or if an envelope with this same
    /// `MessageId` has already been published here before, which is
    /// dropped rather than redelivered or re-buffered (see ADR-0011).
    pub async fn publish(&self, topic: &Topic, envelope: Arc<Envelope>) -> usize {
        let is_new = self.seen.lock().unwrap().record(envelope.id);
        if !is_new {
            return 0;
        }
        self.messages_published.fetch_add(1, Ordering::Relaxed);

        // Durably record the message before delivering it (ADR-0045),
        // on a blocking thread so the SQLite write doesn't stall the
        // runtime. A failure is logged, not fatal - in-memory delivery
        // still proceeds below, so a bad disk degrades durability
        // rather than taking the node down.
        if let Some(store) = &self.store {
            let store = Arc::clone(store);
            let envelope = Arc::clone(&envelope);
            let recorded = match tokio::task::spawn_blocking(move || store.append(&envelope)).await
            {
                Ok(Ok(())) => true,
                Ok(Err(err)) => {
                    tracing::error!(%err, "failed to persist published message");
                    false
                }
                Err(join_err) => {
                    tracing::error!(%join_err, "message-persistence task panicked");
                    false
                }
            };
            if !recorded {
                self.persist_failures.fetch_add(1, Ordering::Relaxed);
            }
        }

        // The retained effect (ADR-0043) is the exact topic's alone -
        // pattern channels never hold a retained value (see
        // `Broker::subscribe`), so they always get `retain: false`.
        let retain = matches!(&envelope.kind, MessageKind::Publish { retain: true, .. });

        let exact_channel = {
            let mut topics = self.topics.write().await;
            topics.get_or_insert(topic.clone(), &self.topic_evictions)
        };
        let mut delivered = exact_channel.publish(Arc::clone(&envelope), retain);

        // Every *currently registered* pattern is checked against
        // `topic` on each publish - O(number of distinct active
        // patterns), not O(subscribers), since same-pattern
        // subscribers already share one `TopicChannel`. A linear scan
        // is the deliberate v1 answer (see ADR-0022); a prefix index
        // is worth it only once this is shown to matter at scale. Kept
        // as a read lock deliberately (see ADR-0025) - a match here
        // doesn't refresh that pattern's eviction recency, so a burst
        // of publishes doesn't serialize behind pattern-map
        // contention the way it didn't before that ADR.
        let patterns = self.patterns.read().await;
        for (filter, channel) in patterns.iter() {
            if filter.matches(topic) {
                delivered += channel.publish(Arc::clone(&envelope), false);
            }
        }
        drop(patterns);

        // Consumer groups (ADR-0042): same linear-scan tradeoff as
        // patterns above, expected to matter even less at scale - a
        // handful of named groups per filter, not thousands of
        // individual subscribers.
        let mut groups = self.groups.lock().unwrap();
        for (key, members) in groups.iter_mut() {
            if key.0.matches(topic) && members.deliver(Arc::clone(&envelope)) {
                delivered += 1;
            }
        }
        delivered
    }

    /// How many distinct (non-duplicate) envelopes have been published
    /// through this broker since it was created - see ADR-0013.
    pub fn messages_published(&self) -> u64 {
        self.messages_published.load(Ordering::Relaxed)
    }

    /// How many `topics` entries have been reclaimed for sitting over
    /// [`DEFAULT_TOPIC_MAP_CAPACITY`] with no live subscriber (see
    /// ADR-0025).
    pub fn topic_evictions(&self) -> u64 {
        self.topic_evictions.load(Ordering::Relaxed)
    }

    /// Same as [`topic_evictions`](Self::topic_evictions), for
    /// `patterns`.
    pub fn pattern_evictions(&self) -> u64 {
        self.pattern_evictions.load(Ordering::Relaxed)
    }

    /// How many publishes the configured [`MessageStore`] failed to
    /// record (ADR-0045). Always 0 with no store.
    pub fn persist_failures(&self) -> u64 {
        self.persist_failures.load(Ordering::Relaxed)
    }
}

/// A `HashMap<K, Arc<TopicChannel>>` paired with a `VecDeque<K>`
/// tracking touch order, so the least-recently-touched entry can be
/// found when [`DEFAULT_TOPIC_MAP_CAPACITY`] is exceeded (see
/// ADR-0025). Both live under the map's own lock in [`Broker`], so
/// there's no separate lock-ordering question between the two.
#[derive(Debug)]
struct TopicMap<K> {
    map: HashMap<K, Arc<TopicChannel>>,
    order: VecDeque<K>,
}

impl<K> Default for TopicMap<K> {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }
}

impl<K: Eq + Hash + Clone> TopicMap<K> {
    /// Gets `key`'s channel, creating it if this is the first time
    /// `key` has ever been seen. Either way, `key` becomes the most
    /// recently touched entry; a fresh insert additionally enforces
    /// the capacity, reclaiming the least-recently-touched entry with
    /// no live subscriber if the map is now over it (see
    /// [`evict_over_capacity`](Self::evict_over_capacity)).
    fn get_or_insert(&mut self, key: K, evictions: &AtomicU64) -> Arc<TopicChannel> {
        if let Some(channel) = self.map.get(&key) {
            let channel = Arc::clone(channel);
            self.touch(&key);
            return channel;
        }
        let channel = Arc::new(TopicChannel::new());
        self.map.insert(key.clone(), Arc::clone(&channel));
        self.order.push_back(key);
        self.evict_over_capacity(evictions);
        channel
    }

    /// Moves `key` to the back of the touch-order queue, if present -
    /// a no-op otherwise. `O(capacity)` in the worst case (a linear
    /// scan of `order`), acceptable here since it only runs once per
    /// `subscribe`/`publish` call to an *existing* entry, not per
    /// envelope.
    fn touch(&mut self, key: &K) {
        if let Some(pos) = self.order.iter().position(|queued| queued == key)
            && let Some(entry) = self.order.remove(pos)
        {
            self.order.push_back(entry);
        }
    }

    /// Reclaims the least-recently-touched entry with no live
    /// subscriber, if the map is over [`DEFAULT_TOPIC_MAP_CAPACITY`]
    /// and such an entry exists. Scans forward from the front of
    /// `order` rather than assuming the front itself is evictable - an
    /// entry with a live [`broadcast::Receiver`] is never a candidate,
    /// even if that means staying over capacity this time (see
    /// ADR-0025). Evicts at most one entry per call - `get_or_insert`
    /// only ever grows the map by one at a time, so one eviction is
    /// enough to stay at capacity whenever an evictable entry exists
    /// at all.
    fn evict_over_capacity(&mut self, evictions: &AtomicU64) {
        if self.map.len() <= DEFAULT_TOPIC_MAP_CAPACITY {
            return;
        }
        let map = &self.map;
        let Some(pos) = self.order.iter().position(|key| {
            map.get(key)
                .is_some_and(|channel| channel.sender.receiver_count() == 0)
        }) else {
            return;
        };
        if let Some(key) = self.order.remove(pos) {
            self.map.remove(&key);
            evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&K, &Arc<TopicChannel>)> {
        self.map.iter()
    }
}

/// The part of a [`TopicChannel`] that has to move atomically with a
/// publish: the replay buffer (ADR-0021) and the retained/last-value
/// message (ADR-0043). Both live under one lock so a new subscriber's
/// snapshot of them and its receiver registration are one atomic step
/// relative to a concurrent publish.
#[derive(Debug, Default)]
struct ChannelState {
    buffer: VecDeque<Arc<Envelope>>,
    /// The topic's retained message, if one has been set and not
    /// cleared - delivered to any later subscriber even after it's
    /// fallen out of `buffer`'s window. See ADR-0043. Only ever set on
    /// an *exact-topic* channel, never a pattern one.
    retained: Option<Arc<Envelope>>,
}

/// One topic's live broadcast channel paired with its [`ChannelState`]
/// (replay buffer + retained message), guarded by one lock so a new
/// subscriber's snapshot and its receiver registration happen as one
/// atomic step relative to a concurrent
/// [`publish`](TopicChannel::publish).
#[derive(Debug)]
struct TopicChannel {
    sender: broadcast::Sender<Arc<Envelope>>,
    state: Mutex<ChannelState>,
}

impl TopicChannel {
    fn new() -> Self {
        Self {
            sender: broadcast::channel(DEFAULT_TOPIC_CHANNEL_CAPACITY).0,
            state: Mutex::new(ChannelState {
                buffer: VecDeque::with_capacity(DEFAULT_REPLAY_BUFFER_CAPACITY),
                retained: None,
            }),
        }
    }

    /// Registers a new receiver and snapshots the current replay
    /// buffer *and* retained message under the same lock, so a
    /// concurrent [`publish`](Self::publish) can never land in neither
    /// (a lost envelope) or both (a duplicate): the two are strictly
    /// ordered by the lock, so whichever runs first completes in full -
    /// buffer push, retained update *and* broadcast send - before the
    /// other starts. See ADR-0021 and ADR-0043.
    ///
    /// The returned backlog is the buffer contents, plus the retained
    /// message prepended *if* it's not already in the buffer (i.e.
    /// it's fallen out of the replay window) - "here's the current
    /// value, then recent history since".
    fn subscribe(&self) -> (Vec<Arc<Envelope>>, broadcast::Receiver<Arc<Envelope>>) {
        let state = self.state.lock().unwrap();
        let receiver = self.sender.subscribe();
        let mut backlog: Vec<Arc<Envelope>> = state.buffer.iter().cloned().collect();
        if let Some(retained) = &state.retained
            && !backlog.iter().any(|e| e.id == retained.id)
        {
            backlog.insert(0, Arc::clone(retained));
        }
        (backlog, receiver)
    }

    /// This channel's retained message right now, if any - a
    /// standalone snapshot for a *wildcard* subscribe, which has to
    /// gather retained values from every matching exact-topic channel
    /// (ADR-0043) rather than from one channel's own
    /// [`subscribe`](Self::subscribe).
    fn retained_snapshot(&self) -> Option<Arc<Envelope>> {
        self.state.lock().unwrap().retained.clone()
    }

    /// Appends `envelope` to the replay buffer (evicting the oldest
    /// entry once over [`DEFAULT_REPLAY_BUFFER_CAPACITY`]), applies its
    /// retained effect if `retain` is set (an empty payload *clears*
    /// the retained message, any other payload *sets* it - ADR-0043),
    /// and broadcasts it to every live receiver, as one critical
    /// section under the same lock [`subscribe`](Self::subscribe) uses.
    fn publish(&self, envelope: Arc<Envelope>, retain: bool) -> usize {
        let mut state = self.state.lock().unwrap();
        state.buffer.push_back(Arc::clone(&envelope));
        if state.buffer.len() > DEFAULT_REPLAY_BUFFER_CAPACITY {
            state.buffer.pop_front();
        }
        if retain {
            state.retained = match &envelope.kind {
                MessageKind::Publish { payload, .. } if payload.is_empty() => None,
                _ => Some(Arc::clone(&envelope)),
            };
        }
        self.sender.send(envelope).unwrap_or(0)
    }
}

/// Identifies one consumer group (ADR-0042): the filter it's
/// registered against, paired with its own name - two different
/// names on the same filter are two independent groups.
type GroupKey = (TopicFilter, String);

/// One consumer group's currently registered members, plus a
/// round-robin cursor into them. See [`Broker::join_group`].
#[derive(Debug, Default)]
struct GroupMembers {
    members: Vec<mpsc::Sender<Arc<Envelope>>>,
    /// Index into `members` to try first on the *next* delivery -
    /// always kept within bounds of whatever `members.len()` is at
    /// the time, even as membership changes.
    next: usize,
}

impl GroupMembers {
    /// Round-robins through this group's currently registered
    /// members, `try_send`ing to each in turn starting from `next`,
    /// until one accepts it or every member has been tried once.
    /// Returns whether *any* member accepted it - `false` only when
    /// the group is empty or every member's channel is currently full
    /// or already closed, the same "no live receiver right now" case
    /// [`Broker::publish`] already treats as normal, not an error.
    fn deliver(&mut self, envelope: Arc<Envelope>) -> bool {
        let len = self.members.len();
        if len == 0 {
            return false;
        }
        for offset in 0..len {
            let idx = (self.next + offset) % len;
            if self.members[idx].try_send(Arc::clone(&envelope)).is_ok() {
                self.next = (idx + 1) % len;
                return true;
            }
        }
        false
    }
}

/// A bounded FIFO record of recently-seen message IDs: a [`HashSet`]
/// for O(1) membership checks alongside a [`VecDeque`] tracking
/// insertion order, so the oldest ID can be evicted once `capacity` is
/// exceeded.
#[derive(Debug)]
struct SeenIds {
    capacity: usize,
    order: VecDeque<MessageId>,
    set: HashSet<MessageId>,
}

impl SeenIds {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            set: HashSet::with_capacity(capacity),
        }
    }

    /// Records `id` as seen, returning `true` if it hadn't been
    /// recorded before (the caller should proceed) or `false` if it's
    /// a repeat (the caller should drop whatever it was about to do).
    fn record(&mut self, id: MessageId) -> bool {
        if !self.set.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.set.remove(&oldest);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use thoth_mesh_core::{MessageKind, PeerId, TopicFilter};

    fn publish_envelope(topic: &Topic, payload: &[u8]) -> Arc<Envelope> {
        Arc::new(Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: topic.clone(),
                payload: payload.to_vec(),
                retain: false,
                content_type: None,
            },
        ))
    }

    /// Subscribes and discards the replay backlog, for tests that only
    /// care about live delivery - see ADR-0021 for the backlog itself.
    async fn subscribe_live(broker: &Broker, topic: Topic) -> broadcast::Receiver<Arc<Envelope>> {
        broker.subscribe(topic.into()).await.1
    }

    #[tokio::test]
    async fn subscribe_then_publish_delivers_to_subscriber() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut rx = subscribe_live(&broker, topic.clone()).await;

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 1);
        let received = rx.recv().await.unwrap();
        assert_eq!(received, envelope);
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut rx_a = subscribe_live(&broker, topic.clone()).await;
        let mut rx_b = subscribe_live(&broker, topic.clone()).await;

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 2);
        assert_eq!(rx_a.recv().await.unwrap(), envelope);
        assert_eq!(rx_b.recv().await.unwrap(), envelope);
    }

    #[tokio::test]
    async fn publish_with_no_subscribers_returns_zero() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        let envelope = publish_envelope(&topic, b"sunny");
        assert_eq!(broker.publish(&topic, envelope).await, 0);
    }

    #[tokio::test]
    async fn distinct_topics_do_not_cross_deliver() {
        let broker = Broker::new();
        let weather = Topic::from_str("weather.updates").unwrap();
        let traffic = Topic::from_str("traffic.updates").unwrap();
        let mut rx = subscribe_live(&broker, weather.clone()).await;

        let envelope = publish_envelope(&traffic, b"jam");
        let delivered = broker.publish(&traffic, envelope).await;

        assert_eq!(delivered, 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn dropped_receiver_does_not_affect_others() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let rx_a = subscribe_live(&broker, topic.clone()).await;
        let mut rx_b = subscribe_live(&broker, topic.clone()).await;
        drop(rx_a);

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 1);
        assert_eq!(rx_b.recv().await.unwrap(), envelope);
    }

    #[tokio::test]
    async fn publishing_the_same_envelope_twice_only_delivers_once() {
        // Simulates an envelope that's looped back around a cyclic
        // peer mesh and arrived at the same node again with the same
        // MessageId - see ADR-0011.
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut rx = subscribe_live(&broker, topic.clone()).await;

        let envelope = publish_envelope(&topic, b"sunny");
        assert_eq!(broker.publish(&topic, envelope.clone()).await, 1);
        assert_eq!(broker.publish(&topic, envelope.clone()).await, 0);

        assert_eq!(rx.recv().await.unwrap(), envelope);
        assert!(
            rx.try_recv().is_err(),
            "the duplicate should not have been redelivered"
        );
    }

    #[tokio::test]
    async fn messages_published_counts_only_new_envelopes() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        let first = publish_envelope(&topic, b"sunny");
        let second = publish_envelope(&topic, b"cloudy");
        broker.publish(&topic, first.clone()).await;
        broker.publish(&topic, second).await;
        // A duplicate of `first` shouldn't bump the counter again.
        broker.publish(&topic, first).await;

        assert_eq!(broker.messages_published(), 2);
    }

    #[tokio::test]
    async fn distinct_envelopes_on_the_same_topic_both_deliver() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut rx = subscribe_live(&broker, topic.clone()).await;

        let first = publish_envelope(&topic, b"sunny");
        let second = publish_envelope(&topic, b"cloudy");
        assert_eq!(broker.publish(&topic, first.clone()).await, 1);
        assert_eq!(broker.publish(&topic, second.clone()).await, 1);

        assert_eq!(rx.recv().await.unwrap(), first);
        assert_eq!(rx.recv().await.unwrap(), second);
    }

    #[tokio::test]
    async fn a_late_subscriber_is_replayed_a_publish_that_happened_before_it_subscribed() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        // Nobody is subscribed yet when this is published.
        let envelope = publish_envelope(&topic, b"sunny");
        assert_eq!(broker.publish(&topic, envelope.clone()).await, 0);

        // A late subscriber still gets it, via the backlog rather than
        // live delivery.
        let (backlog, mut rx) = broker.subscribe(topic.into()).await;
        assert_eq!(backlog, vec![envelope]);
        assert!(rx.try_recv().is_err(), "already delivered via backlog");
    }

    #[tokio::test]
    async fn backlog_and_live_delivery_never_double_deliver() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        let before = publish_envelope(&topic, b"sunny");
        broker.publish(&topic, before.clone()).await;

        let (backlog, mut rx) = broker.subscribe(topic.clone().into()).await;
        assert_eq!(backlog, vec![before]);

        let after = publish_envelope(&topic, b"cloudy");
        broker.publish(&topic, after.clone()).await;

        // Only the publish made *after* subscribing arrives live - the
        // earlier one was already handed over in the backlog above.
        assert_eq!(rx.recv().await.unwrap(), after);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn replay_buffer_returns_backlog_oldest_first() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        let first = publish_envelope(&topic, b"sunny");
        let second = publish_envelope(&topic, b"cloudy");
        broker.publish(&topic, first.clone()).await;
        broker.publish(&topic, second.clone()).await;

        let (backlog, _rx) = broker.subscribe(topic.into()).await;
        assert_eq!(backlog, vec![first, second]);
    }

    #[tokio::test]
    async fn replay_buffer_drops_the_oldest_once_over_capacity() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        // One more than the buffer holds - the very first publish
        // should have been evicted by the time anyone reads it back.
        let mut envelopes = Vec::with_capacity(DEFAULT_REPLAY_BUFFER_CAPACITY + 1);
        for i in 0..=DEFAULT_REPLAY_BUFFER_CAPACITY {
            let envelope = publish_envelope(&topic, format!("update {i}").as_bytes());
            broker.publish(&topic, envelope.clone()).await;
            envelopes.push(envelope);
        }

        let (backlog, _rx) = broker.subscribe(topic.into()).await;
        assert_eq!(backlog.len(), DEFAULT_REPLAY_BUFFER_CAPACITY);
        assert_eq!(backlog, &envelopes[1..]);
    }

    #[tokio::test]
    async fn a_duplicate_publish_is_not_replayed_twice() {
        // Same scenario as publishing_the_same_envelope_twice_only_delivers_once,
        // but for the backlog rather than live delivery - see ADR-0011.
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();

        let envelope = publish_envelope(&topic, b"sunny");
        broker.publish(&topic, envelope.clone()).await;
        broker.publish(&topic, envelope.clone()).await;

        let (backlog, _rx) = broker.subscribe(topic.into()).await;
        assert_eq!(backlog, vec![envelope]);
    }

    fn filter(s: &str) -> TopicFilter {
        TopicFilter::from_str(s).unwrap()
    }

    #[tokio::test]
    async fn a_pattern_subscriber_receives_a_matching_publish() {
        let broker = Broker::new();
        let (_backlog, mut rx) = broker.subscribe(filter("weather.+")).await;

        let topic = Topic::from_str("weather.updates").unwrap();
        let envelope = publish_envelope(&topic, b"sunny");
        assert_eq!(broker.publish(&topic, envelope.clone()).await, 1);
        assert_eq!(rx.recv().await.unwrap(), envelope);
    }

    #[tokio::test]
    async fn a_pattern_subscriber_does_not_receive_a_non_matching_publish() {
        let broker = Broker::new();
        let (_backlog, mut rx) = broker.subscribe(filter("weather.+")).await;

        let topic = Topic::from_str("traffic.updates").unwrap();
        let envelope = publish_envelope(&topic, b"jam");
        assert_eq!(broker.publish(&topic, envelope).await, 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn an_exact_and_a_matching_pattern_subscriber_both_independently_receive() {
        // Two distinct subscriptions - one exact, one a pattern that
        // happens to also match - each get their own delivery through
        // their own TopicChannel (see ADR-0022's Broker::publish doc).
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut exact_rx = subscribe_live(&broker, topic.clone()).await;
        let (_backlog, mut pattern_rx) = broker.subscribe(filter("weather.+")).await;

        let envelope = publish_envelope(&topic, b"sunny");
        assert_eq!(broker.publish(&topic, envelope.clone()).await, 2);
        assert_eq!(exact_rx.recv().await.unwrap(), envelope);
        assert_eq!(pattern_rx.recv().await.unwrap(), envelope);
    }

    #[tokio::test]
    async fn a_late_pattern_subscriber_is_replayed_a_matching_backlog() {
        // Patterns reuse TopicChannel, so a second subscriber to the
        // *same already-registered* pattern gets ADR-0021's replay
        // buffer for free, same as an exact-match topic does. Unlike
        // an exact topic, though, a pattern's buffer can only start
        // accumulating once something has actually subscribed to that
        // pattern string - there's no way to pre-create a buffer for
        // every pattern a future subscriber might use (see ADR-0022).
        let broker = Broker::new();
        let (_backlog, _first_rx) = broker.subscribe(filter("weather.+")).await;

        let topic = Topic::from_str("weather.updates").unwrap();
        let envelope = publish_envelope(&topic, b"sunny");
        broker.publish(&topic, envelope.clone()).await;

        let (backlog, _rx) = broker.subscribe(filter("weather.+")).await;
        assert_eq!(backlog, vec![envelope]);
    }

    #[tokio::test]
    async fn a_publish_before_any_subscriber_ever_used_a_pattern_is_not_retroactively_matched() {
        // The inverse of ADR-0021's "publish creates a topic's buffer
        // even with zero subscribers" - that only applies to the
        // exact-match map. A pattern nobody has subscribed to yet
        // doesn't exist in `patterns` at publish time, so there's
        // nothing to buffer into; a subscriber to that pattern later
        // only sees what's published *after* it first subscribes.
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        broker
            .publish(&topic, publish_envelope(&topic, b"sunny"))
            .await;

        let (backlog, _rx) = broker.subscribe(filter("weather.+")).await;
        assert!(backlog.is_empty());
    }

    #[tokio::test]
    async fn a_bare_hash_pattern_matches_every_topic() {
        let broker = Broker::new();
        let (_backlog, mut rx) = broker.subscribe(filter("#")).await;

        let weather = Topic::from_str("weather.updates").unwrap();
        let traffic = Topic::from_str("traffic.jam").unwrap();
        let sunny = publish_envelope(&weather, b"sunny");
        let heavy = publish_envelope(&traffic, b"heavy");
        broker.publish(&weather, sunny.clone()).await;
        broker.publish(&traffic, heavy.clone()).await;

        assert_eq!(rx.recv().await.unwrap(), sunny);
        assert_eq!(rx.recv().await.unwrap(), heavy);
        assert!(rx.try_recv().is_err());
    }

    /// Subscribes to `topic`, then immediately drops the receiver -
    /// leaves a `TopicChannel` behind with a live buffer but zero
    /// receivers, exactly the eviction-eligible shape ADR-0025 targets.
    async fn subscribe_then_abandon(broker: &Broker, topic: Topic) {
        drop(broker.subscribe(topic.into()).await);
    }

    #[tokio::test]
    async fn topics_over_capacity_evicts_the_oldest_entry_with_no_live_subscriber() {
        let broker = Broker::new();
        let evicted = Topic::from_str("topic.0").unwrap();
        subscribe_then_abandon(&broker, evicted.clone()).await;

        // Fill up to (and one past) capacity with distinct topics -
        // `evicted` is the least-recently-touched entry throughout,
        // and has no live receiver, so it's the one reclaimed.
        for i in 1..=DEFAULT_TOPIC_MAP_CAPACITY {
            subscribe_then_abandon(&broker, Topic::from_str(&format!("topic.{i}")).unwrap()).await;
        }

        assert_eq!(broker.topic_evictions(), 1);
        // A fresh subscribe to the evicted topic gets an empty
        // backlog, same as if it had never been touched before - its
        // prior (empty, in this test) history is gone along with it.
        let (backlog, _rx) = broker.subscribe(evicted.into()).await;
        assert!(backlog.is_empty());
    }

    #[tokio::test]
    async fn a_topic_with_a_live_subscriber_is_never_evicted_even_over_capacity() {
        let broker = Broker::new();
        let protected = Topic::from_str("topic.protected").unwrap();
        // Held for the rest of the test - this receiver is what makes
        // `protected` ineligible for eviction.
        let _rx = subscribe_live(&broker, protected.clone()).await;

        for i in 0..=DEFAULT_TOPIC_MAP_CAPACITY {
            subscribe_then_abandon(&broker, Topic::from_str(&format!("topic.{i}")).unwrap()).await;
        }

        // Nothing else was eligible either (every other entry in this
        // test is also abandoned) - eviction did happen, just never
        // against `protected`, which a non-empty backlog after a
        // publish (rather than a topic reset back to empty) confirms.
        assert!(broker.topic_evictions() >= 1);
        let envelope = publish_envelope(&protected, b"still here");
        assert_eq!(broker.publish(&protected, envelope).await, 1);
    }

    #[tokio::test]
    async fn re_touching_an_entry_protects_it_from_being_the_next_eviction() {
        let broker = Broker::new();
        let refreshed = Topic::from_str("topic.refreshed").unwrap();
        subscribe_then_abandon(&broker, refreshed.clone()).await;

        // Fill to just below capacity with other topics.
        for i in 1..DEFAULT_TOPIC_MAP_CAPACITY {
            subscribe_then_abandon(&broker, Topic::from_str(&format!("topic.{i}")).unwrap()).await;
        }
        assert_eq!(broker.topic_evictions(), 0);

        // Touch `refreshed` again, moving it to the back of the queue
        // - `topic.1` (untouched since its own insert) is now the
        // least-recently-touched entry instead.
        subscribe_then_abandon(&broker, refreshed.clone()).await;
        subscribe_then_abandon(&broker, Topic::from_str("topic.one_more").unwrap()).await;

        assert_eq!(broker.topic_evictions(), 1);
        let (backlog, _rx) = broker.subscribe(refreshed.into()).await;
        assert!(
            backlog.is_empty(),
            "refreshed should still exist (empty backlog, not evicted)"
        );
    }

    #[tokio::test]
    async fn patterns_are_capped_and_evicted_independently_of_topics() {
        let broker = Broker::new();
        let evicted = filter("evicted.+");
        drop(broker.subscribe(evicted.clone()).await);

        for i in 0..DEFAULT_TOPIC_MAP_CAPACITY {
            drop(broker.subscribe(filter(&format!("pattern.{i}.+"))).await);
        }

        assert_eq!(broker.pattern_evictions(), 1);
        assert_eq!(broker.topic_evictions(), 0, "topics is a separate cap");
        let (backlog, _rx) = broker.subscribe(evicted).await;
        assert!(backlog.is_empty());
    }

    /// Registers `count` fresh members for `(filter, group)`, returning
    /// their receivers in join order - what `deliver`'s round-robin
    /// cursor starts iterating from.
    async fn join_members(
        broker: &Broker,
        filter: TopicFilter,
        group: &str,
        count: usize,
    ) -> Vec<mpsc::Receiver<Arc<Envelope>>> {
        let mut receivers = Vec::with_capacity(count);
        for _ in 0..count {
            let (tx, rx) = mpsc::channel(8);
            broker.join_group(filter.clone(), group.to_owned(), tx);
            receivers.push(rx);
        }
        receivers
    }

    #[tokio::test]
    async fn a_publish_goes_to_exactly_one_group_member() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut members = join_members(&broker, topic.clone().into(), "workers", 2).await;

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 1, "exactly one group member, not both");
        let got: Vec<bool> = members.iter_mut().map(|rx| rx.try_recv().is_ok()).collect();
        assert_eq!(
            got.iter().filter(|&&got_it| got_it).count(),
            1,
            "exactly one member's channel actually received it: {got:?}"
        );
    }

    #[tokio::test]
    async fn group_delivery_round_robins_across_members() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut members = join_members(&broker, topic.clone().into(), "workers", 2).await;

        let mut published = Vec::with_capacity(4);
        for i in 0..4u32 {
            let envelope = publish_envelope(&topic, format!("update {i}").as_bytes());
            broker.publish(&topic, envelope.clone()).await;
            published.push(envelope);
        }

        // Each member got every other message, alternating starting
        // with the first member joined - not both getting everything,
        // and not one member starved.
        let member_0: Vec<_> = std::iter::from_fn(|| members[0].try_recv().ok())
            .map(|e| e.id)
            .collect();
        let member_1: Vec<_> = std::iter::from_fn(|| members[1].try_recv().ok())
            .map(|e| e.id)
            .collect();
        assert_eq!(member_0, vec![published[0].id, published[2].id]);
        assert_eq!(member_1, vec![published[1].id, published[3].id]);
    }

    #[tokio::test]
    async fn leave_group_removes_a_matching_member() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let filter: TopicFilter = topic.clone().into();
        let (tx_a, mut rx_a) = mpsc::channel(8);
        let (tx_b, mut rx_b) = mpsc::channel(8);
        broker.join_group(filter.clone(), "workers".to_owned(), tx_a.clone());
        broker.join_group(filter.clone(), "workers".to_owned(), tx_b);
        broker.leave_group(filter, "workers".to_owned(), &tx_a);

        // Two publishes - if `tx_a` were still a member, round-robin
        // would alternate; since only `tx_b` remains, both go to it.
        broker
            .publish(&topic, publish_envelope(&topic, b"one"))
            .await;
        broker
            .publish(&topic, publish_envelope(&topic, b"two"))
            .await;

        assert!(rx_a.try_recv().is_err(), "removed member got nothing");
        assert!(rx_b.try_recv().is_ok());
        assert!(rx_b.try_recv().is_ok());
    }

    #[tokio::test]
    async fn leave_group_is_a_no_op_for_a_non_member() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let filter: TopicFilter = topic.clone().into();
        let (tx, mut rx) = mpsc::channel(8);
        broker.join_group(filter.clone(), "workers".to_owned(), tx);

        let (never_joined, _never_joined_rx) = mpsc::channel(8);
        broker.leave_group(filter, "workers".to_owned(), &never_joined);

        broker
            .publish(&topic, publish_envelope(&topic, b"still here"))
            .await;
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn a_group_member_with_a_full_channel_is_skipped_in_favor_of_the_next() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let filter: TopicFilter = topic.clone().into();
        let (tx_full, rx_full) = mpsc::channel(1);
        // Fill tx_full's one slot without ever draining it.
        tx_full
            .try_send(publish_envelope(&topic, b"already queued"))
            .unwrap();
        let (tx_open, mut rx_open) = mpsc::channel(8);
        broker.join_group(filter, "workers".to_owned(), tx_full);
        broker.join_group(topic.clone().into(), "workers".to_owned(), tx_open);

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 1);
        assert_eq!(rx_open.try_recv().unwrap().id, envelope.id);
        drop(rx_full); // only ever held the one pre-queued message
    }

    #[tokio::test]
    async fn a_wildcard_group_filter_matches_a_publish_on_it() {
        let broker = Broker::new();
        let mut members = join_members(&broker, filter("weather.+"), "workers", 1).await;

        let topic = Topic::from_str("weather.updates").unwrap();
        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 1);
        assert_eq!(members[0].try_recv().unwrap().id, envelope.id);
    }

    #[tokio::test]
    async fn an_ordinary_subscriber_and_a_consumer_group_on_the_same_filter_both_independently_receive()
     {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut fanout_rx = subscribe_live(&broker, topic.clone()).await;
        let mut members = join_members(&broker, topic.clone().into(), "workers", 1).await;

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 2, "one fan-out subscriber plus one group member");
        assert_eq!(fanout_rx.recv().await.unwrap().id, envelope.id);
        assert_eq!(members[0].try_recv().unwrap().id, envelope.id);
    }

    #[tokio::test]
    async fn two_differently_named_groups_on_the_same_filter_are_independent() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut group_a = join_members(&broker, topic.clone().into(), "a", 1).await;
        let mut group_b = join_members(&broker, topic.clone().into(), "b", 1).await;

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 2, "each group gets its own copy");
        assert_eq!(group_a[0].try_recv().unwrap().id, envelope.id);
        assert_eq!(group_b[0].try_recv().unwrap().id, envelope.id);
    }

    #[tokio::test]
    async fn a_group_member_does_not_see_a_publish_that_happened_before_it_joined() {
        // Unlike an ordinary subscribe (ADR-0021's replay buffer), a
        // consumer group has no backlog - see ADR-0042.
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        broker
            .publish(&topic, publish_envelope(&topic, b"before"))
            .await;

        let mut members = join_members(&broker, topic.clone().into(), "workers", 1).await;
        let after = publish_envelope(&topic, b"after");
        broker.publish(&topic, after.clone()).await;

        assert_eq!(members[0].try_recv().unwrap().id, after.id);
        assert!(
            members[0].try_recv().is_err(),
            "nothing else should have arrived - no backlog for a group"
        );
    }

    #[tokio::test]
    async fn a_publish_to_a_group_with_no_current_members_is_not_an_error() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let filter: TopicFilter = topic.clone().into();
        let (tx, _rx) = mpsc::channel(8);
        broker.join_group(filter.clone(), "workers".to_owned(), tx.clone());
        broker.leave_group(filter, "workers".to_owned(), &tx);

        // The group entry still exists (now with zero members) -
        // publishing to it contributes nothing to `delivered`, same as
        // publishing to a topic nobody is subscribed to at all.
        let delivered = broker
            .publish(&topic, publish_envelope(&topic, b"anybody?"))
            .await;
        assert_eq!(delivered, 0);
    }

    fn retain_envelope(topic: &Topic, payload: &[u8]) -> Arc<Envelope> {
        Arc::new(Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: topic.clone(),
                payload: payload.to_vec(),
                retain: true,
                content_type: None,
            },
        ))
    }

    #[tokio::test]
    async fn a_retained_message_is_delivered_to_a_later_subscriber_exactly_once() {
        let broker = Broker::new();
        let topic = Topic::from_str("sensor.temp").unwrap();

        let retained = retain_envelope(&topic, b"21C");
        broker.publish(&topic, Arc::clone(&retained)).await;

        // Subscribing afterward gets the retained value in the backlog,
        // and only once (not also via the replay buffer entry it also
        // created, not also live).
        let (backlog, mut rx) = broker.subscribe(topic.clone().into()).await;
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog[0].id, retained.id);
        assert!(rx.try_recv().is_err(), "already delivered via the backlog");
    }

    #[tokio::test]
    async fn a_retained_message_survives_past_the_replay_window() {
        let broker = Broker::new();
        let topic = Topic::from_str("sensor.temp").unwrap();

        let retained = retain_envelope(&topic, b"21C");
        broker.publish(&topic, Arc::clone(&retained)).await;

        // Flood the topic with enough non-retained traffic to evict the
        // retained message from the bounded replay buffer entirely.
        for i in 0..=DEFAULT_REPLAY_BUFFER_CAPACITY {
            broker
                .publish(
                    &topic,
                    publish_envelope(&topic, format!("noise {i}").as_bytes()),
                )
                .await;
        }

        let (backlog, _rx) = broker.subscribe(topic.clone().into()).await;
        assert_eq!(
            backlog.len(),
            DEFAULT_REPLAY_BUFFER_CAPACITY + 1,
            "the full replay window plus the retained message prepended"
        );
        assert_eq!(
            backlog[0].id, retained.id,
            "the retained message is the oldest entry - 'current value, then history since'"
        );
    }

    #[tokio::test]
    async fn a_newer_retained_publish_replaces_the_previous_one() {
        let broker = Broker::new();
        let topic = Topic::from_str("sensor.temp").unwrap();

        broker
            .publish(&topic, retain_envelope(&topic, b"21C"))
            .await;
        let newer = retain_envelope(&topic, b"22C");
        broker.publish(&topic, Arc::clone(&newer)).await;

        // Evict both from the replay buffer so only the retained slot
        // can answer.
        for i in 0..=DEFAULT_REPLAY_BUFFER_CAPACITY {
            broker
                .publish(
                    &topic,
                    publish_envelope(&topic, format!("noise {i}").as_bytes()),
                )
                .await;
        }

        let (backlog, _rx) = broker.subscribe(topic.clone().into()).await;
        assert_eq!(backlog[0].id, newer.id);
        assert!(
            !backlog.iter().any(|e| {
                let MessageKind::Publish { payload, .. } = &e.kind else {
                    return false;
                };
                payload == b"21C"
            }),
            "the superseded retained value is gone"
        );
    }

    #[tokio::test]
    async fn a_retained_publish_with_an_empty_payload_clears_the_retained_message() {
        let broker = Broker::new();
        let topic = Topic::from_str("sensor.temp").unwrap();

        broker
            .publish(&topic, retain_envelope(&topic, b"21C"))
            .await;
        broker.publish(&topic, retain_envelope(&topic, b"")).await;

        for i in 0..=DEFAULT_REPLAY_BUFFER_CAPACITY {
            broker
                .publish(
                    &topic,
                    publish_envelope(&topic, format!("noise {i}").as_bytes()),
                )
                .await;
        }

        let (backlog, _rx) = broker.subscribe(topic.clone().into()).await;
        assert_eq!(
            backlog.len(),
            DEFAULT_REPLAY_BUFFER_CAPACITY,
            "no retained message prepended - it was cleared"
        );
    }

    #[tokio::test]
    async fn a_non_retained_publish_never_becomes_the_retained_message() {
        let broker = Broker::new();
        let topic = Topic::from_str("sensor.temp").unwrap();

        broker
            .publish(&topic, publish_envelope(&topic, b"not retained"))
            .await;
        for i in 0..=DEFAULT_REPLAY_BUFFER_CAPACITY {
            broker
                .publish(
                    &topic,
                    publish_envelope(&topic, format!("noise {i}").as_bytes()),
                )
                .await;
        }

        let (backlog, _rx) = broker.subscribe(topic.clone().into()).await;
        assert_eq!(backlog.len(), DEFAULT_REPLAY_BUFFER_CAPACITY);
    }

    #[tokio::test]
    async fn a_retained_message_and_a_live_publish_never_double_deliver() {
        let broker = Broker::new();
        let topic = Topic::from_str("sensor.temp").unwrap();

        let retained = retain_envelope(&topic, b"21C");
        broker.publish(&topic, Arc::clone(&retained)).await;

        let (backlog, mut rx) = broker.subscribe(topic.clone().into()).await;
        assert_eq!(backlog, vec![Arc::clone(&retained)]);

        let live = publish_envelope(&topic, b"live");
        broker.publish(&topic, Arc::clone(&live)).await;

        assert_eq!(rx.recv().await.unwrap().id, live.id);
        assert!(
            rx.try_recv().is_err(),
            "only the post-subscribe publish arrives live"
        );
    }

    #[tokio::test]
    async fn a_wildcard_subscriber_gets_retained_values_from_every_matching_concrete_topic() {
        let broker = Broker::new();
        let temp = Topic::from_str("sensor.temp").unwrap();
        let humidity = Topic::from_str("sensor.humidity").unwrap();

        let temp_retained = retain_envelope(&temp, b"21C");
        let humidity_retained = retain_envelope(&humidity, b"40%");
        broker.publish(&temp, Arc::clone(&temp_retained)).await;
        broker
            .publish(&humidity, Arc::clone(&humidity_retained))
            .await;

        let (backlog, _rx) = broker.subscribe(filter("sensor.+")).await;
        let ids: HashSet<_> = backlog.iter().map(|e| e.id).collect();
        assert_eq!(ids, HashSet::from([temp_retained.id, humidity_retained.id]));
        // Sorted by MessageId (UUIDv7), so the earlier publish comes
        // first.
        assert_eq!(backlog[0].id, temp_retained.id);
        assert_eq!(backlog[1].id, humidity_retained.id);
    }

    #[tokio::test]
    async fn a_consumer_group_does_not_receive_a_retained_message() {
        // A group gets no catch-up of any kind - see ADR-0042/ADR-0043.
        let broker = Broker::new();
        let topic = Topic::from_str("sensor.temp").unwrap();
        broker
            .publish(&topic, retain_envelope(&topic, b"21C"))
            .await;

        let mut members = join_members(&broker, topic.clone().into(), "workers", 1).await;
        assert!(
            members[0].try_recv().is_err(),
            "a group member joining after a retained publish gets nothing"
        );
    }

    /// A [`MessageStore`] that just records what `append` is handed,
    /// and can replay a caller-supplied history back - enough to test
    /// `Broker`'s side of ADR-0045 without a real database.
    #[derive(Debug, Default)]
    struct FakeStore {
        appended: Mutex<Vec<Arc<Envelope>>>,
        recent: Vec<(Topic, Vec<Arc<Envelope>>)>,
        retained: Vec<(Topic, Arc<Envelope>)>,
        fail: bool,
        // ADR-0046
        offsets: Mutex<HashMap<(PeerId, Topic), MessageId>>,
    }

    impl MessageStore for FakeStore {
        fn append(&self, envelope: &Envelope) -> std::io::Result<()> {
            if self.fail {
                return Err(std::io::Error::other("boom"));
            }
            self.appended
                .lock()
                .unwrap()
                .push(Arc::new(envelope.clone()));
            Ok(())
        }
        fn load_recent(
            &self,
            _per_topic: usize,
        ) -> std::io::Result<Vec<(Topic, Vec<Arc<Envelope>>)>> {
            Ok(self.recent.clone())
        }
        fn load_retained(&self) -> std::io::Result<Vec<(Topic, Arc<Envelope>)>> {
            Ok(self.retained.clone())
        }
        // ADR-0046: `appended` (already a growing log across every
        // topic) doubles as the fake's "on-disk" history to scan.
        fn messages_since(
            &self,
            topic: &Topic,
            after: MessageId,
        ) -> std::io::Result<Vec<Arc<Envelope>>> {
            let mut matches: Vec<Arc<Envelope>> = self
                .appended
                .lock()
                .unwrap()
                .iter()
                .filter(|e| {
                    matches!(&e.kind, MessageKind::Publish { topic: t, .. } if t == topic)
                        && e.id > after
                })
                .cloned()
                .collect();
            matches.sort_by_key(|e| e.id);
            Ok(matches)
        }
        fn load_offset(
            &self,
            subscriber: PeerId,
            topic: &Topic,
        ) -> std::io::Result<Option<MessageId>> {
            Ok(self
                .offsets
                .lock()
                .unwrap()
                .get(&(subscriber, topic.clone()))
                .copied())
        }
        fn record_offset(
            &self,
            subscriber: PeerId,
            topic: &Topic,
            message_id: MessageId,
        ) -> std::io::Result<()> {
            self.offsets
                .lock()
                .unwrap()
                .insert((subscriber, topic.clone()), message_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn publish_records_every_distinct_message_to_the_store() {
        let store = Arc::new(FakeStore::default());
        let broker = Broker::with_store(store.clone());
        let topic = Topic::from_str("weather.updates").unwrap();

        let first = publish_envelope(&topic, b"sunny");
        let second = publish_envelope(&topic, b"cloudy");
        broker.publish(&topic, first.clone()).await;
        broker.publish(&topic, second.clone()).await;
        // A duplicate (same MessageId) is not re-recorded - it's
        // dropped before delivery *and* before persistence (ADR-0011).
        broker.publish(&topic, first.clone()).await;

        let appended = store.appended.lock().unwrap();
        assert_eq!(appended.len(), 2);
        assert_eq!(appended[0].id, first.id);
        assert_eq!(appended[1].id, second.id);
        assert_eq!(broker.persist_failures(), 0);
    }

    #[tokio::test]
    async fn a_store_failure_is_counted_but_delivery_still_happens() {
        let store = Arc::new(FakeStore {
            fail: true,
            ..FakeStore::default()
        });
        let broker = Broker::with_store(store);
        let topic = Topic::from_str("weather.updates").unwrap();
        let mut rx = subscribe_live(&broker, topic.clone()).await;

        let envelope = publish_envelope(&topic, b"sunny");
        let delivered = broker.publish(&topic, envelope.clone()).await;

        assert_eq!(delivered, 1, "in-memory delivery still happens");
        assert_eq!(rx.recv().await.unwrap().id, envelope.id);
        assert_eq!(broker.persist_failures(), 1);
    }

    #[tokio::test]
    async fn rehydrate_refills_the_replay_buffer_and_retained_slot() {
        let topic = Topic::from_str("sensor.temp").unwrap();
        let history = vec![
            publish_envelope(&topic, b"20C"),
            publish_envelope(&topic, b"21C"),
        ];
        let retained = retain_envelope(&topic, b"21C");
        let store = Arc::new(FakeStore {
            recent: vec![(topic.clone(), history.clone())],
            retained: vec![(topic.clone(), retained.clone())],
            ..FakeStore::default()
        });
        let broker = Broker::with_store(store.clone());

        for (topic, envelopes) in store.load_recent(1024).unwrap() {
            broker.rehydrate_buffer(&topic, envelopes).await;
        }
        for (topic, envelope) in store.load_retained().unwrap() {
            broker.rehydrate_retained(&topic, envelope).await;
        }

        // A subscriber connecting after the rehydrate sees the
        // rehydrated history as its replay backlog, with the rehydrated
        // retained value folded in as the current value (ADR-0043's
        // literal-subscribe behavior) - exactly as it would before a
        // restart - and nothing was re-broadcast or re-persisted.
        let (backlog, mut rx) = broker.subscribe(topic.clone().into()).await;
        let ids: Vec<_> = backlog.iter().map(|e| e.id).collect();
        assert_eq!(ids, vec![retained.id, history[0].id, history[1].id]);
        assert!(rx.try_recv().is_err(), "rehydrate must not re-broadcast");
        assert!(
            store.appended.lock().unwrap().is_empty(),
            "rehydrate must not re-persist"
        );
    }

    #[test]
    fn has_store_reflects_whether_one_is_configured() {
        assert!(!Broker::new().has_store());
        assert!(Broker::with_store(Arc::new(FakeStore::default())).has_store());
    }

    #[tokio::test]
    async fn subscribe_durable_rejects_a_wildcard_filter() {
        let broker = Broker::with_store(Arc::new(FakeStore::default()));
        let err = broker
            .subscribe_durable(filter("weather.+"), PeerId::new())
            .await
            .unwrap_err();
        assert!(matches!(err, DurableSubscribeError::WildcardFilter));
    }

    #[tokio::test]
    async fn subscribe_durable_rejects_when_no_store_is_configured() {
        let broker = Broker::new();
        let topic = Topic::from_str("weather.updates").unwrap();
        let err = broker
            .subscribe_durable(topic.into(), PeerId::new())
            .await
            .unwrap_err();
        assert!(matches!(err, DurableSubscribeError::NoStore));
    }

    #[tokio::test]
    async fn a_first_time_durable_subscriber_gets_no_disk_catch_up() {
        // No recorded offset for this (subscriber, topic) - behaves
        // exactly like an ordinary subscribe, even though the store
        // has history for the topic (from some *other* subscriber, or
        // from before this one ever existed). See ADR-0046.
        let topic = Topic::from_str("weather.updates").unwrap();
        let store = Arc::new(FakeStore {
            appended: Mutex::new(vec![publish_envelope(&topic, b"old news")]),
            ..FakeStore::default()
        });
        let broker = Broker::with_store(store);

        let (backlog, _rx) = broker
            .subscribe_durable(topic.into(), PeerId::new())
            .await
            .unwrap();
        assert!(backlog.is_empty());
    }

    #[tokio::test]
    async fn a_returning_durable_subscriber_gets_everything_since_its_recorded_position() {
        let topic = Topic::from_str("weather.updates").unwrap();
        let subscriber = PeerId::new();
        let before = publish_envelope(&topic, b"before");
        let after_1 = publish_envelope(&topic, b"after 1");
        let after_2 = publish_envelope(&topic, b"after 2");
        let store = Arc::new(FakeStore {
            appended: Mutex::new(vec![before.clone(), after_1.clone(), after_2.clone()]),
            offsets: Mutex::new(HashMap::from([((subscriber, topic.clone()), before.id)])),
            ..FakeStore::default()
        });
        let broker = Broker::with_store(store);

        let (backlog, _rx) = broker
            .subscribe_durable(topic.into(), subscriber)
            .await
            .unwrap();
        let ids: Vec<_> = backlog.iter().map(|e| e.id).collect();
        assert_eq!(ids, vec![after_1.id, after_2.id]);
    }

    #[tokio::test]
    async fn record_delivered_then_resubscribing_durably_resumes_from_there() {
        // The full round trip: publish, durably subscribe, record what
        // was delivered, reconnect (a fresh subscribe_durable call) -
        // only what's genuinely new comes back.
        let store = Arc::new(FakeStore::default());
        let broker = Broker::with_store(store);
        let topic = Topic::from_str("weather.updates").unwrap();
        let subscriber = PeerId::new();

        let first = publish_envelope(&topic, b"sunny");
        broker.publish(&topic, first.clone()).await;
        let (backlog, _rx) = broker
            .subscribe_durable(topic.clone().into(), subscriber)
            .await
            .unwrap();
        assert_eq!(backlog, vec![first.clone()]);
        broker
            .record_delivered(subscriber, topic.clone(), first.id)
            .await;

        let second = publish_envelope(&topic, b"cloudy");
        broker.publish(&topic, second.clone()).await;
        let (backlog, _rx) = broker
            .subscribe_durable(topic.into(), subscriber)
            .await
            .unwrap();
        assert_eq!(
            backlog,
            vec![second],
            "only the message published after the recorded position comes back"
        );
    }

    #[test]
    fn seen_ids_evicts_the_oldest_once_over_capacity() {
        let mut seen = SeenIds::new(2);
        let a = MessageId::new();
        let b = MessageId::new();
        let c = MessageId::new();

        assert!(seen.record(a));
        assert!(seen.record(b));
        // `b` is still within capacity, so re-recording it is still a
        // duplicate.
        assert!(!seen.record(b));

        assert!(seen.record(c)); // over capacity - evicts `a`

        // `a` was evicted, so it looks new again.
        assert!(seen.record(a));
        // `c` is recent enough to still be remembered.
        assert!(!seen.record(c));
    }
}
