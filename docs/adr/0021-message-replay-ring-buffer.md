# 21. Message replay for late subscribers via a bounded per-topic ring buffer

## Status

Accepted

## Context

Issue #48 (Phase 8): today delivery is purely live - `Broker` holds no
history, it only fans out to whoever is already subscribed at publish
time (ADR-0006). A subscriber that connects after a publish has simply
missed it, with no way to catch up - explicitly called out as a known
gap in `PROTOCOL.md`'s Delivery semantics section ("no persistence...
nothing is stored for a subscriber that connects afterward").

The issue's "Known shape" flagged the widest fork up front: an
in-memory ring buffer (bounded, like the broadcast channel capacity
already is) vs. real cross-restart durability, with embedded SQLite
named as a candidate for the latter - and was explicit that these are
"very different scope, worth being explicit about which this phase is
committing to." It also left open replay semantics (whole-buffer vs. a
client-supplied cursor) and cross-node scope (does replay reach into
what peers saw, or only what this node itself observed).

## Decision

### Scope: the in-memory ring buffer, not cross-restart durability

This ADR commits to the in-memory half of the fork the issue raised,
not the SQLite-backed one:

- Every other bound in this codebase so far is a fixed-capacity,
  in-memory, non-persistent structure - the per-topic broadcast
  channel (ADR-0006) and the dedup `MessageId` cache (ADR-0011) both
  already work this way. A ring buffer is additive to that existing
  model, not a new one.
- A durable, cross-restart store is a materially different piece of
  infrastructure - a writer task/thread bridging sync `rusqlite` into
  the async hot path, WAL mode, an on-disk retention policy, a
  migration story - that deserves its own ADR if and when it's
  actually needed, not bundled into "can a late subscriber catch up on
  recent history at all." The issue's own text agrees these are very
  different scope.
- This doesn't foreclose durability later: a durable store would sit
  at the same seam this ADR introduces (`TopicChannel::publish`/
  `subscribe`, below), not require changing the wire protocol or the
  client-facing replay behavior decided here.

### Replay semantics: whole-buffer replay on `Subscribe`, no cursor, no wire protocol change

A newly-registered forwarder for a topic - client or peer link, see
below - is handed the entire current backlog, oldest first,
immediately after the usual `Subscribe`/`Ack` handshake. There's no
client-specified starting point: no new fields on `Subscribe`, no new
`MessageKind`. This resolves the issue's cursor/offset question in
favor of the simpler option, in the same spirit as ADR-0006's
"exact-match topics only for v1, revisit once there's a working
end-to-end system to justify more" - a cursor is real design work (is
it valid once the buffer's evicted past it? per-topic or global? does
it survive a reconnect?) that a wire-protocol change would lock in
indefinitely, better done once whole-buffer replay has actually been
used in practice, not speculated about up front.

Re-subscribing to a topic this connection is already being forwarded
is unchanged: still a no-op, still `Ack`'d, no re-replay - the
existing idempotent-forwarder behavior (`forwarders.entry(...).
or_insert_with`) already governs this, and replay only ever runs the
first time a forwarder is spawned for a (connection, topic) pair.

### Mechanism: `TopicChannel` pairs the broadcast `Sender` with a `Mutex`-guarded ring buffer, one lock for both

`Broker`'s per-topic map value changes from a bare
`broadcast::Sender<Arc<Envelope>>` to `Arc<TopicChannel>`:

```rust
struct TopicChannel {
    sender: broadcast::Sender<Arc<Envelope>>,
    buffer: Mutex<VecDeque<Arc<Envelope>>>,
}
```

`TopicChannel::subscribe` registers a new `broadcast::Receiver` and
snapshots the current buffer as a `Vec` for replay; `TopicChannel::
publish` appends to the buffer (evicting the oldest once over
capacity) and sends to the broadcast channel - both under the *same*
`buffer` lock, as one critical section each. This is what makes replay
exactly-once rather than racy: a concurrent `subscribe` and `publish`
are strictly ordered by the mutex, so whichever runs first completes
in full before the other starts. If `subscribe` wins the race, the
envelope isn't in its buffer snapshot yet, but its receiver is already
registered before `publish`'s `send` runs, so it arrives live. If
`publish` wins, the envelope is already in the buffer snapshot
`subscribe` reads, and its receiver is registered too late to also get
it live. Either way, exactly one delivery path fires, never zero or
both - no gap, no duplicate, with no new coordination beyond the lock
`Broker` already needed.

`std::sync::Mutex` rather than `tokio::sync::Mutex`: both operations
inside the critical section are synchronous, non-blocking, and brief
(a `VecDeque` push/pop and a non-blocking channel send), so there's
nothing worth `.await`-ing on and no risk of holding it across a
suspension point.

Buffer capacity is a new constant, `DEFAULT_REPLAY_BUFFER_CAPACITY`,
sized the same as `DEFAULT_TOPIC_CHANNEL_CAPACITY` (256) and, like it,
not configurable via a CLI flag in v1 - consistent with how the
existing broadcast-channel and dedup-cache capacities are already
fixed constants, not flags.

### Applies uniformly to clients and peer links - no special-casing

Peer links and clients already share the same `Subscribe` dispatch and
forwarder-spawn code path (ADR-0010). A peer catching up via
`register_peer_link`'s interest-snapshot `Subscribe`s (ADR-0011)
spawns a forwarder exactly like a client's `Subscribe` does, so it
gets the same replay, with no additional code needed to make that
happen.

This directly answers the issue's cross-node question: replay only
ever draws from what this node's own `Broker::publish` has seen -
which, by the time an envelope reaches there, has already survived
cross-node dedup (ADR-0011), whether it originated locally or arrived
from a peer. A freshly-linked peer's own buffer starts empty; this
node never reaches into *that* peer's history to backfill anything.
"Probably the former initially," which the issue guessed at, is what
falls out for free from reusing the existing dispatch path rather than
something requiring its own decision.

### `Broker::publish` creates a topic's buffer even with zero current subscribers

Before this ADR, `publish` to a topic nobody had ever subscribed to
was a pure no-op - no entry was ever created in the topics map. For a
publish that happens *before* the first subscriber ever shows up to
still be replayable, `publish` now get-or-inserts the topic's
`TopicChannel` the same way `subscribe` already does, rather than only
looking one up. This is what makes "late subscriber" mean "connects
after a publish, whether or not anyone was listening at the time," not
just "connects after every other subscriber has already come and
gone."

### New metric: `thothmesh_replayed_messages_total`

Counts envelopes delivered to a forwarder from the replay backlog
specifically, distinct from `thothmesh_messages_published_total`
(which already counts distinct publishes, not deliveries). Lets an
operator see replay actually happening - or confirm it isn't, on a
node where every subscriber connects before any publish.

## Consequences

`Broker::subscribe`'s return type changes from a bare
`broadcast::Receiver<Arc<Envelope>>` to `(Vec<Arc<Envelope>>,
broadcast::Receiver<Arc<Envelope>>)` - a breaking change to its one
caller (`connection.rs`'s `spawn_forwarder`) and to `thoth-mesh-broker`'s
own unit tests, both updated alongside this ADR.

A topic's buffer now persists as long as its `TopicChannel` does -
which, per the change above, can now be created by a publish alone,
with no subscriber ever having existed. Combined with the per-topic
map already never shrinking (ADR-0006's Consequences already flagged
this for zero-receiver `Sender`s), this is a genuine new contributor
to unbounded topic-map growth on a node fielding many distinct
short-lived topics - exactly the kind of structure issue #72 (bound
per-node memory footprint) is meant to catalogue and address; that
issue's own text already anticipated this tension explicitly. Not
addressed here - #72 is the right place to decide eviction policy
across every such structure at once, not one at a time as each is
introduced.

The buffer silently drops its oldest entry once a topic exceeds
`DEFAULT_REPLAY_BUFFER_CAPACITY` publishes since the last subscriber
history read it - a late subscriber to a very bursty topic may still
miss the earliest messages in that burst, with no wire-level signal
that anything was dropped (the same posture `PROTOCOL.md`'s existing
"a slow subscriber can miss messages" bullet already accepts for a
lagging live receiver; this extends the same tradeoff to the replay
path).

This closes #48. #49 (wildcard/pattern topic matching), the other
Phase 8 item, is unaffected - replay is keyed on the same exact-match
`Topic` the broker already dispatches on.
