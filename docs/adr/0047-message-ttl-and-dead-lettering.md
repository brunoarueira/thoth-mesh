# 47. Message TTL and dead-lettering

## Status

Accepted

## Context

Filed as #135 (Phase 14), depends on #133/ADR-0045's on-disk log. The
on-disk log keeps the newest `DEFAULT_PERSISTED_MESSAGES_PER_TOPIC`
(100,000) rows per topic, pruned by count - but nothing expires by
*age*, so a node that has ever seen a topic keeps up to that many rows
for it forever (ADR-0045's own Consequences flagged this as the gap
"age-based expiry (#135) is where that gets a real answer"). Separately,
ADR-0041 left a message with nowhere to go once an `ack: true`
subscription's redelivery attempts are exhausted: "there is no
dead-letter destination in this phase (Phase 14's 'Message TTL /
dead-lettering' is explicitly where that belongs)."

The issue's known shape asked two open questions: per-topic or
per-publish TTL, and whether an expired message is just dropped or
moved somewhere inspectable.

## Decision

### One dead-letter mechanism for both sources

A message goes unconsumed in this codebase in exactly two ways today:
it ages out of the on-disk log before anyone durably needed it, or an
`ack: true` delivery exhausts every redelivery attempt without ever
being acked (ADR-0041). Rather than building disk-TTL dead-lettering
now and leaving ADR-0041's note unresolved, both feed the same
mechanism: an optional `--dead-letter-topic <topic>` - if configured,
either source republishes the original message, unchanged, as an
ordinary `Publish` to `<dead-letter-topic>.<original topic>` (e.g.
`dead-letter.weather.updates`), inspectable by subscribing to it
directly or to `<dead-letter-topic>.#` for everything. With no
`--dead-letter-topic` configured (the default), both sources behave
exactly as they do today: an expired row is just deleted, an
exhausted ack delivery is just dropped and counted.

The per-source prefix keeps provenance visible without any wire
change - a dead-lettered message on `dead-letter.weather.updates`
tells a subscriber exactly what topic it fell off of. A single flat
dead-letter topic losing that would make the feature far less useful
for its actual purpose (figuring out *what's* going unconsumed).

The republish is a **fresh envelope** - its own new `id` and this
node's own identity as `sender`, `retain: false` - carrying the
original payload and content-type hint forward unchanged. Reusing the
original envelope's `id` was considered and rejected: that `id` may
already be in this node's (or a downstream peer's) loop-prevention
`seen` set (ADR-0011) from its original delivery, which would make the
republish silently vanish as an apparent duplicate - exactly backward
for a mechanism whose entire point is making an otherwise-lost message
visible. A fresh `id` guarantees it's delivered as the genuinely new
event it is.

Both are counted under one new metric, `thothmesh_dead_lettered_messages_total`
- an operator watching it doesn't need to care which of the two
sources produced a given increment; `thothmesh_expired_messages_total`
(below) and `thothmesh_delivery_ack_timeouts_total` (ADR-0041,
already existed) separately show *why* the underlying message was
given up on, whether or not it was also dead-lettered.

### TTL: a single global `--message-ttl-secs`, not per-topic or per-publish

The issue's two named options were per-topic and per-publish; a third,
simpler one - one node-wide value - is what this ADR picks instead.
Per-topic would need a new config-file format and precedence rules for
overlapping wildcard patterns (mirroring `--topic-acl`'s real
complexity); per-publish would need a new `Publish` field, a wire
change every publisher has to know to set, and per-message bookkeeping
the on-disk sweep query doesn't otherwise need. Neither is justified
by evidence of actual need yet - the issue itself only asks for *a*
way to expire content, not a granular one. A single
`--message-ttl-secs <N>` (seconds; unset by default, meaning no
age-based expiry - only the existing count cap applies, unchanged from
before this ADR) is enough to solve the stated problem ("growing
forever") with no new wire surface at all. Requires `--data-dir`
(enforced by `clap`'s `requires`) - there's no disk log to expire
anything from otherwise.

If a real deployment later needs per-topic control, that's the point
to design it properly, on its own merits - the same "don't
speculatively build structure a single case doesn't need" posture
ADR-0046 already settled on for schema evolution.

### The sweep: a periodic background task, not append-triggered

`MessageStore` gains `expire_before(cutoff_ts) -> Vec<Arc<Envelope>>`:
deletes every `messages` row older than `cutoff_ts` and returns what
was removed, so the caller can dead-letter it before it's gone.
Unlike the existing count-based `prune()` (triggered every
`PRUNE_EVERY` appends - a natural fit for "count", which only changes
on an append), age has nothing to do with appends; a row can go stale
with zero further publishes on its topic. So the TTL sweep is instead
a periodic background task (`ttl.rs`, a fixed 60s interval, not
currently configurable), started only when both `--data-dir` and
`--message-ttl-secs` are set. `retained` rows are untouched by
`expire_before` - a retained value's entire point is staying current
indefinitely (ADR-0043), not expiring by age; TTL only ever prunes
`messages`.

### `PendingAcks::sweep` returns the envelope, not just the id

ADR-0041's `PendingAcks::sweep` reported a given-up-on delivery as
just its `MessageId` - enough to count and log, not enough to
dead-letter, since dead-lettering needs the payload. `sweep` now
returns `Vec<Arc<Envelope>>` for the given-up-on set instead (the
entry already holds the `Arc<Envelope>` right up until it's dropped);
`spawn_ack_forwarder` dead-letters each one, best-effort, if
`--dead-letter-topic` is configured.

## Consequences

- New CLI flags: `--message-ttl-secs <N>` (requires `--data-dir`) and
  `--dead-letter-topic <topic>` (standalone - useful for ack-giveup
  dead-lettering even with no `--data-dir`/TTL at all).
- `MessageStore` gains `expire_before`. `SqliteStore` implements it as
  a `SELECT` + `DELETE` pair in one transaction (no schema change -
  `messages.ts`, already there since ADR-0045, is exactly what this
  needs).
- `redelivery::PendingAcks::sweep`'s return type changes (`Vec<MessageId>`
  → `Vec<Arc<Envelope>>` for the given-up-on half) - an internal,
  not-`pub`-outside-the-crate type, no wire impact.
- New `thoth-mesh-node::dead_letter` module: one shared
  `dead_letter()` helper, used by both the TTL sweep task and
  `spawn_ack_forwarder`.
- Two new metrics (`thothmesh_expired_messages_total`,
  `thothmesh_dead_lettered_messages_total`), added to `MetricsSummary`
  (ADR-0037) and the Prometheus render.
- No wire-protocol change - a dead-lettered message is an ordinary
  `Publish`, indistinguishable on the wire from any other.
