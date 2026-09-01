# 25. Bounding per-node memory footprint at mesh scale

## Status

Accepted

## Context

Issue #72 (Phase 9): every piece of live mesh state is in-memory, and
several of the structures holding it have no eviction or cap tied to
mesh size - a real memory-growth risk on a large, long-running mesh,
not a hypothetical one. `PeerId` makes this concrete: it's a fresh
random UUID generated per process, "per CLI invocation and per node
startup" (`PROTOCOL.md`). Every node restart anywhere in the mesh is
therefore a *new* identity to every other node - a long-running mesh
that simply restarts its nodes occasionally accumulates distinct
`PeerId`s forever, independent of how many nodes are ever actually up
at once.

The issue's "Known shape" named five structures to audit one by one.
Auditing them first (rather than assuming the issue's list is
complete or that every named structure actually needs work) turned up
that it's a mix:

- **`Interest`** (ADR-0011) already removes a filter's entry the
  moment its count returns to zero (`unsubscribe`'s `if *count == 0 {
  counts.remove(filter); ... }`) - bounded by "currently interested
  filters," which is exactly the live data this structure exists to
  answer. Nothing to do here.
- **`PeerLinks`** (`thoth-mesh-node`) already removes a peer's sender
  on disconnect (`unregister`, guarded against a stale reconnect race)
  - bounded by "currently connected peers," a number the OS's own
    connection limits already cap. Nothing to do here.
- **The dedup `MessageId` cache** (`SeenIds`, ADR-0011) is already a
  fixed-capacity FIFO, exactly the pattern this ADR applies elsewhere
  - the issue's own text already said as much.
- **`Membership`** and **`PeerDirectory`** (ADR-0015) both genuinely
  never remove an entry - confirmed real, unbounded growth, and the
  `PeerId`-per-restart problem above makes it worse than "many nodes,"
  it's "many node restarts over time."
- **The broker's per-topic dispatch tables** (`Broker`'s `topics`/
  `patterns` maps) also genuinely never remove an entry -
  `TopicChannel`s persist forever once created, which ADR-0021 and
  ADR-0022 already flagged as feeding this exact issue when they
  introduced the growth.

So the real scope is three structures, not five, across two crates
(`thoth-mesh`, `thoth-mesh-broker`).

## Decision

### Policy: a fixed-capacity, self-contained bound per structure - no shared abstraction, no background sweeper

Every existing bounded structure in this codebase (the broadcast
channel capacity, `SeenIds`, the replay buffer) is a fixed-capacity,
in-memory structure with no CLI flag and no background timer - eviction
happens inline, as a side effect of the operation that would otherwise
grow it past capacity. This ADR extends the same shape to `Membership`,
`PeerDirectory`, and `Broker`'s topic maps, rather than introducing a
time-based expiry mechanism (a background sweep task, wall-clock
timestamps) or a shared generic cache abstraction in `thoth-mesh-core`.

Time-based expiry was the issue's other named option ("active eviction
... vs. ... cap and let hit a hard ceiling"). It's not needed: a fixed
capacity with the right eviction *candidate* selection (below) already
gets the useful property - a peer or topic that's still actually
active never gets evicted just because it's old - without a clock,
a sweep task, or a policy for how long is "long enough."

A shared generic LRU-cache type was considered and declined: the three
call sites have genuinely different eviction-eligibility rules (a
connected peer is never evictable at all; a topic or pattern with a
live subscriber is never evictable at all; a merely-known peer address
has no such exemption) - a shared abstraction expressive enough for
all three would need callback-based eligibility predicates threaded
through every operation, more machinery than three independent,
straightforward implementations following the same pattern by
convention. Consistent with this codebase's general preference for
minimal, purpose-built code over shared infrastructure built ahead of
a second real need.

### `Membership`: cap *disconnected* entries only, connected peers exempt

A connected peer's entry is never a candidate for eviction - that
count is already bounded by how many real sockets this node can hold
open, which is exactly the live data `connected_count` (ADR-0013)
exists to report. Only *disconnected* peers (kept today purely as
"this was our last-known address for them, in case they come back")
are capped, at `DEFAULT_MEMBERSHIP_DISCONNECTED_CAPACITY` (4096,
matching `DEFAULT_DEDUP_CAPACITY`'s existing order of magnitude).

Mechanism: a `VecDeque<PeerId>` alongside the existing `peers` map,
under the *same* lock (avoiding any two-lock ordering question) -
appended to on each connected-to-disconnected transition,
capacity-enforced immediately after (popping the front and removing
that peer's `peers` entry once over capacity - safe unconditionally,
since anything still in the queue is guaranteed still disconnected, by
the next rule). A reconnect (`mark_connected` for a peer with a queued
disconnected entry) removes that peer's entry from the queue - it's no
longer an eviction candidate at all, and this is what keeps the queue
itself from silently accumulating stale references across repeated
reconnect churn.

### `PeerDirectory`: a plain capacity-bounded FIFO, refreshed on re-record

No connected/disconnected distinction exists here at all (deliberately,
per its own existing doc comment) - every known peer is an eviction
candidate. Capped at `DEFAULT_PEER_DIRECTORY_CAPACITY` (4096), using
the same "companion `VecDeque` under the map's own lock" shape as
`Membership` above. `record`-ing an already-known peer (a repeat
gossip mention or handshake) moves its entry to the back of the
queue rather than leaving it in its original position - a peer that
keeps getting talked about stays fresh; one that stops aging toward
eviction is exactly the peers-that-are-actually-gone this bound exists
to reclaim.

### `Broker`: cap `topics`/`patterns` separately, never evicting an entry with a live receiver

The riskiest of the three to get wrong: a `TopicChannel` isn't just a
cache entry, it's the only path back to every currently-subscribed
connection's `broadcast::Receiver` for that topic or pattern. Evicting
one out from under a live subscriber would silently and permanently
cut it off - a future publish would get-or-insert a *new*,
disconnected `TopicChannel`, and the old receiver would just stop
receiving anything, ever, with no error. So eviction here is
conditional: only a `TopicChannel` whose `broadcast::Sender::
receiver_count()` is currently zero is ever a candidate, checked at
the moment of eviction, not cached. Each of `topics` and `patterns` is
capped independently at `DEFAULT_TOPIC_MAP_CAPACITY` (4096), using the
same companion-`VecDeque`-under-the-map's-lock shape. Recency is
refreshed whenever an entry is created or looked up through
`subscribe` (both maps) and, for the exact-match `topics` map only,
through `publish` too - `publish`'s *pattern* matching loop
deliberately keeps its existing `RwLock::read` scan rather than
upgrading to a write lock just to touch recency on every match, so a
burst of publishes doesn't serialize behind pattern-map contention the
way it didn't before this ADR. This only affects which zero-receiver
entry gets reclaimed first when the cap is hit, not whether one with
live receivers ever can be - that guarantee doesn't depend on recency
at all.

Enforcement is a scan forward from the front of the queue for the
first zero-receiver entry, evicting only that one - not a blind
pop-the-front, since the front might currently have live subscribers.
If every tracked entry happens to have a live receiver right now (the
whole mesh's active topic set has grown past the cap), the map is
simply allowed to exceed it rather than break a live subscriber's
delivery guarantee - a soft cap, not a hard ceiling, and the one place
in this ADR where that's true. This is the same trade this codebase
already makes elsewhere (correctness over a hard resource ceiling) and
is a deliberate, documented exception to "fixed capacity" being a hard
number everywhere else.

### Observability: an eviction counter per structure, read directly by `render_prometheus`

`Membership::disconnected_evictions()`, `PeerDirectory::evictions()`,
and two new `Broker` counters (`topic_evictions()`/
`pattern_evictions()`) - each a self-tracked `AtomicU64` on the type
itself, read directly by `thoth-mesh-node`'s `render_prometheus` the
same way `Broker::messages_published()` and `Membership::
connected_count()` already are, rather than duplicated into
`thoth-mesh-node`'s own `Metrics` struct. This answers the issue's own
"should this be observable via metrics" question - an operator sees
eviction actually happening (or not) well before hitting a case where
it matters.

## Consequences

Three independent, sequential PRs (one per structure) rather than one
large change touching two crates at once - each compiles and tests on
its own, and none of the three depends on either of the others being
done first.

`Membership::new()`/`PeerDirectory::new()`/`Broker::new()` keep their
existing zero-argument signatures - the new capacities are fixed
constants, not constructor parameters, consistent with every other
capacity in this codebase not being configurable via a flag in v1.
Every existing caller of all three keeps working unchanged.

The `Broker` bound is a soft cap under sustained load with a
persistently-huge live topic count - documented above, not a bug, but
worth knowing: this ADR bounds "topics with nobody currently listening
that a node keeps remembering," not "how many topics a mesh can have
subscribers to at once," which remains genuinely unbounded (the same
way connected-peer count is unbounded by `Membership`'s cap - both are
real, live, wanted state, not accumulated cruft).

None of these three changes anything about wire behavior or the
metrics already listed in `docs/OPERATIONS.md`'s table besides adding
new counters - existing tests for `Membership`, `PeerDirectory`, and
`Broker` keep passing unchanged; new tests cover the eviction paths
specifically (capacity exceeded, a reconnect/re-touch clearing
eviction-candidacy, a live receiver never getting evicted).

This closes #72.
