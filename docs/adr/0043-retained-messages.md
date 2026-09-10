# 43. Retained (last-value) messages per topic

## Status

Accepted

## Context

Filed as #130 (Phase 13). A late subscriber only ever gets what's
still inside the replay buffer's bounded window (ADR-0021, 1024
messages per topic). There's no "just give me the current value on
this topic" - the way MQTT's retained messages work, where a
subscriber gets the last retained payload immediately on subscribe,
however long ago it was published.

The issue's known shape asked two things: one retained payload per
topic (overwritten, not appended, distinct from the replay buffer);
and whether it's opt-in per `Publish` or per topic, and what the
default is.

## Decision

### Opt-in per `Publish`, default off

`MessageKind::Publish` gains a `retain: bool` field (`#[serde(default)]`,
so an older sender omitting it decodes as `false` - unchanged
behavior, the same additive-field rolling-upgrade pattern ADR-0041's
`ack` and ADR-0042's `group` established). `retain: true` makes that
publish *also* become the topic's retained value, replacing whatever
was there. `retain: false` (the default) is exactly today's behavior.

Per-publish, not per-topic: a per-topic setting would need a
topic-configuration mechanism this project doesn't have, and MQTT's
own model - the flag rides on each PUBLISH - has proven itself. A
publisher that wants every message on a topic retained just sets the
flag every time.

### Stored in the exact topic's `TopicChannel`, under the replay buffer's own lock

The retained slot lives inside `TopicChannel` (the per-exact-topic
structure that already holds the replay buffer), guarded by the *same*
lock the buffer snapshot and receiver registration already share
(ADR-0021). A `retain: true` publish sets the slot, pushes to the
buffer, and broadcasts as one critical section; a new subscriber
reads the slot, snapshots the buffer, and registers its receiver as
one critical section. So for a literal subscribe the retained value
is delivered exactly once - never also live, never dropped - the same
guarantee ADR-0021 gives the replay buffer, for the same reason.

Not a separate top-level `Broker` map: that would put the retained
write on a different lock from the buffer-push/broadcast, reopening
exactly the neither-or-both race ADR-0021 closed. Living in
`TopicChannel` also means a retained value is bounded by ADR-0025's
topic-map cap for free (see Consequences).

Pattern (`patterns`-map) channels do *not* get a retained slot - "the
last message matching `sensor.+`" is last-write-wins across every
`sensor.*` topic, which is meaningless. A wildcard subscriber's
retained values come from scanning the `topics` map for matching
concrete topics (see below).

### Delivery: merged into the subscribe backlog, deduplicated by `MessageId`

`Broker::subscribe` returns the retained value(s) as part of the same
`Vec<Arc<Envelope>>` backlog it already returns for the replay buffer
- a forwarder replays them exactly like any other backlog entry
(ADR-0021), and they're counted the same way
(`replayed_messages_total`). Rules:

- A retained envelope whose `MessageId` is already in the replay
  backlog (i.e. it was published recently enough to still be in the
  window) is *not* added again - it's already there.
- A retained envelope that's fallen out of the replay window is
  prepended as the oldest entry: "here's the current value, then
  here's recent history since."
- For a wildcard subscribe, the retained values from every matching
  concrete topic are collected, deduplicated against the pattern
  channel's own replay snapshot, and the whole backlog is sorted by
  `MessageId` (a UUIDv7, monotonic by creation time - ADR-0005) so
  retained values and replay history land in a coherent order.

### Clearing a retained value

`retain: true` with an empty payload clears the topic's retained slot
(MQTT's convention - a zero-length retained PUBLISH). The publish
itself is still delivered live and still enters the replay buffer
like any other; only the retained slot is affected. There is no other
way to un-retain a topic.

### What retained does *not* interact with

- **Consumer groups (ADR-0042):** a `group` subscribe gets no
  retained value, the same as it gets no replay backlog - retained is
  a form of catch-up, and a load-balanced group deliberately doesn't
  do catch-up.
- **At-least-once (ADR-0041):** a retained value delivered to an
  `ack: true` subscriber is held for acknowledgement exactly like any
  other backlog entry - no special case.
- **Federation:** `retain` rides along in the forwarded envelope, so
  a peer that receives a forwarded `retain: true` publish stores it
  as retained for that topic on *its* broker too. This is not a
  cross-mesh retained-state sync: a node only retains what it
  actually received, and a node that gains interest in a topic
  *after* a retain-publish elsewhere does not have that earlier value
  back-filled to it. Full mesh-wide retained sync is a bigger
  federation-routing problem, out of scope here.

## Consequences

- `MessageKind::Publish` grows a `retain` field; every construction
  and pattern-match site across the workspace is updated, the same
  mechanical footprint ADR-0041/ADR-0042 had.
- `TopicChannel`'s `buffer: Mutex<VecDeque<..>>` becomes
  `state: Mutex<ChannelState>` bundling the buffer and the retained
  `Option<Arc<Envelope>>`, so both live under one lock.
- A wildcard subscribe now scans the `topics` map for matching
  retained values - O(concrete topics), on subscribe only, the same
  linear-scan tradeoff ADR-0022 accepts for patterns on publish.
- A retained value is subject to ADR-0025's topic-map eviction: a
  retained topic with no live subscriber, on a node churning through
  more than `DEFAULT_TOPIC_MAP_CAPACITY` distinct topics, can lose
  its retained value along with its `TopicChannel`. Acceptable at v1
  scale (4096); retained topics could be made eviction-exempt later
  if it matters.
- No new metric - a retained delivery is a backlog delivery and is
  already counted as one.
- `thoth-mesh-cli publish` gains `--retain`.
