# 22. Wildcard/pattern topic matching via MQTT-style topic filters

## Status

Accepted

## Context

Issue #49 (Phase 8, the other half alongside #48/ADR-0021): subscribing
to `weather.*` (or similar) should match any topic under that prefix,
instead of requiring one exact-string `Subscribe` per topic. ADR-0006
deliberately chose exact-match topics for v1, explicitly flagging it
as worth revisiting "once exercised" - it now has been, across a real
federated mesh (ADR-0011's interest propagation and dedup, ADR-0021's
replay).

The issue's "Known shape" flagged three open questions: which pattern
syntax to use, that `Broker`'s exact-match `HashMap<Topic, ...>`
doesn't generalize to pattern matching for free, and that ADR-0011's
interest propagation aggregates exact topics today - propagating
*patterns* across peer links changes what "does any peer want this"
means.

## Decision

### Pattern syntax: MQTT-style `+`/`#`, a new `TopicFilter` type distinct from `Topic`

Segments are `.`-delimited, matching this codebase's overwhelming
existing convention (`weather.updates`, `sensors.data`, `traffic.
updates`, ...) - `/` appears only incidentally (`weather.updates/v1`),
never as the primary hierarchy separator. Two wildcard tokens, each
occupying a whole segment by itself:

- `+` matches exactly one segment.
- `#` matches zero or more remaining segments, and only as the *last*
  segment of a filter.

This is MQTT's own convention (also close to NATS' `*`/`>`), chosen
over an unglobbed glob (`*`/`**` matching *within* a segment) because
segment-whole wildcards are a much easier matching problem to reason
about and implement correctly than arbitrary substring globbing, and
MQTT's semantics map onto this codebase's existing dot-segmented
topics without any translation.

A new type, `TopicFilter` (`thoth-mesh-core`), rather than widening
`Topic` itself: `Topic` keeps meaning exactly what it always has - a
concrete, publishable name - enforced by the type system rather than
a runtime check at the one place (`Publish`) where a wildcard would be
meaningless. Every string that's already a valid `Topic` is also a
valid, purely-literal `TopicFilter` (`impl From<Topic> for
TopicFilter` is infallible) - the wildcard segments are strictly
additional vocabulary, not a competing one, so this is fully backward
compatible with every existing exact-match subscribe.

### `Broker`: the existing exact-match map is untouched; a second pattern map sits alongside it

`Broker::topics: HashMap<Topic, Arc<TopicChannel>>` (ADR-0006,
extended by ADR-0021) is unchanged - same type, same lookup, same
replay buffer, zero behavior or performance change for the common
literal-subscribe case. A new `Broker::patterns: HashMap<TopicFilter,
Arc<TopicChannel>>` holds wildcard filters only, reusing `TopicChannel`
unmodified - a pattern subscription gets ADR-0021's replay buffer for
free, the same way peer links got it for free by reusing the shared
dispatch path.

`Broker::subscribe(filter: TopicFilter)` is now the one entry point
(`connection.rs` no longer calls a `Topic`-typed overload): a literal
filter (`filter.as_topic()` is `Some`) routes to `topics`, exactly as
before; a genuine pattern routes to `patterns`.

`Broker::publish(topic: &Topic, envelope)` is unchanged through the
dedup/counter step, then delivers to `topics.get(topic)` if present
(unchanged, O(1)) *and* iterates `patterns`, delivering to every entry
whose filter matches `topic` (O(number of distinct active patterns) -
not O(subscribers), since same-pattern subscribers already share one
`TopicChannel`). This mirrors ADR-0006's own "exact-match for v1,
revisit once exercised" precedent: a linear scan over active patterns
is the simple, correct v1 answer; a prefix index is worth building
once there's evidence this scan is actually a bottleneck (the kind of
question issue #71's scale-hardening work is already the right place
to ask).

A connection holding both an exact subscribe and an independently
matching pattern subscribe on the same topic (e.g. `weather.updates`
and `weather.+`) gets two independent deliveries, one per subscription
- each is its own `TopicChannel` with its own forwarder task
(`connection.rs`'s `forwarders` map is keyed by the raw `TopicFilter`,
so the two are distinct entries). This is ordinary, expected pub/sub
behavior (any MQTT broker does the same for two overlapping
subscriptions), not a bug worth de-duplicating.

### Interest propagation (ADR-0011) widens from `Topic` to `TopicFilter`, uniformly

`Interest`'s key type changes from `Topic` to `TopicFilter` - not a
parallel structure, since every literal topic is already a
`TopicFilter`. `MessageKind::Subscribe`/`Unsubscribe` carry a
`filter: TopicFilter` field (renamed from `topic`, to name the
subscribe-side/publish-side distinction MQTT itself draws between a
"topic filter" and a "topic name"); `register_peer_link`'s catch-up
loop and `propagate_interest` need no branching between the two cases
- they already just forward whatever's in `Interest::snapshot()`.

A peer link can therefore hold interest in a pattern; a publish
reaching a node with a peer link interested in a matching pattern is
forwarded to it exactly as an exact-topic interest already would be -
no special-casing, the same "falls out for free from the shared
dispatch path" result ADR-0021 got for replay.

### `Publish` stays `Topic`-typed

`MessageKind::Publish { topic: Topic, .. }` is unchanged. You can only
ever publish to a concrete topic - `Topic`'s charset has no `+`/`#`,
so this is a compile-time guarantee, not a runtime check.

### ACL interaction: a non-literal filter is rejected outright wherever a topic ACL is configured

Neither ADR-0018's `--topic-acl` nor ADR-0020's `--peer-topic-acl` is
pattern-aware - `TopicAcl::permits` matches an exact `(Principal,
Topic, Action)` tuple, and the issue didn't raise this interaction at
all. Making an ACL entry itself pattern-aware opens a genuinely hard
question with no obviously-correct answer (does an entry granting
`weather.updates` say anything about a subscribe to `weather.+`? What
about the reverse?) that this ADR isn't going to invent an answer to
just to unblock wildcard subscribes.

Instead: if a `--topic-acl`/`--peer-topic-acl` is configured at all for
a connection's role (client or peer, same distinction ADR-0018/0020
already draw), a `Subscribe` whose filter isn't literal
(`filter.as_topic()` is `None`) is refused with the same `Error` an
ACL already sends for a denied literal topic. A literal filter is
checked exactly as before, completely unchanged. Pattern-aware ACL
entries are left for a future issue if this restriction ever proves
too limiting in practice.

### No new metric

`thothmesh_messages_published_total` still counts distinct publishes,
unaffected by how many exact/pattern subscriptions a publish happens
to match. Per-path delivery counts (exact vs. pattern) aren't tracked,
the same as delivery counts were never broken out before ADR-0021
either - not worth a new counter until there's a concrete operational
question it would answer.

## Consequences

`MessageKind::Subscribe`/`Unsubscribe`'s field changes from `topic:
Topic` to `filter: TopicFilter` - a breaking wire-type change, same
category as ADR-0021's breaking `Broker::subscribe` signature change,
acceptable pre-1.0. Every construction site (production and test)
across `thoth-mesh-core`, `thoth-mesh`, `thoth-mesh-node`, and
`thoth-mesh-cli` is updated alongside this ADR.

`connection.rs`'s per-connection `forwarders` map changes key type
from `Topic` to `TopicFilter`; `Interest`'s counts map does the same.

The new `patterns` map has the same "never shrinks on its own" shape
`topics` already has (flagged for `topics` in ADR-0006/ADR-0021's
Consequences) - one more structure for issue #72 (bound per-node
memory footprint) to catalogue, not addressed here.

Unlike `topics`, `patterns` entries can't be created proactively by
`publish` - ADR-0021's "a publish creates a topic's buffer even with
zero subscribers" trick only works because `publish` already knows the
one concrete `Topic` it's addressed to; there's no way to guess in
advance which of the unbounded space of possible patterns a future
subscriber might use. A pattern's replay buffer therefore only starts
accumulating once something has actually subscribed to that exact
pattern string - a publish that happens before *any* subscriber has
ever used a given pattern is not retroactively matched into it once
someone finally does, even though the equivalent is true for an exact
topic. This is a genuine asymmetry between the two paths, not an
oversight; it falls directly out of patterns being an open-ended set
rather than a discoverable one.

Pattern matching is a linear scan over distinct active patterns on
every publish - fine at today's scale, a candidate for a prefix index
if issue #71's scale work ever shows it matters.

Wildcard subscribes are unconditionally refused on any connection
whose role has a topic ACL configured, regardless of what the pattern
would actually expand to - a deliberate, conservative v1 restriction,
not an oversight; see the ACL section above.

This closes #49, completing Phase 8 alongside #48/ADR-0021.
