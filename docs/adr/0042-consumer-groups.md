# 42. Consumer groups

## Status

Accepted

## Context

Filed as #129 (Phase 13). Every subscriber to a topic gets every
message today - `Broker::publish` fans out to every exact-match and
matching-pattern subscriber alike (ADR-0006/ADR-0022). Real workloads
often want to load-balance a topic across a group of workers instead:
register several connections under the same name, and each published
message goes to exactly one currently-live member, not all of them.

The issue's known shape asked for a group name on `Subscribe`, and
flagged one open question: how this interacts with at-least-once
redelivery (ADR-0041) - what "acked" means for a group, not just an
individual subscriber.

## Decision

### `group` is a third field on `Subscribe`, orthogonal to `ack`

```
Subscribe { filter: TopicFilter, ack: bool, group: Option<String> }
```

`#[serde(default)]`, same rolling-upgrade story as `ack` (ADR-0041):
an older sender omitting `group` decodes as `None`, unchanged
fan-out behavior. `group: Some(name)` joins the named group for
`filter` on this connection instead of subscribing to ordinary
fan-out; `filter` can be literal or a wildcard pattern (ADR-0022)
either way. Two different group names on the same filter are two
independent groups, each getting its own copy of every matching
publish - same relationship literal-vs-pattern subscriptions already
have to each other (ADR-0022's Consequences); *within* one group,
exactly one currently-live member gets each message.

### Round-robin, one member per message, decided in `Broker`

A new `Broker::join_group`/`leave_group` pair, mirroring
`thoth-mesh-node`'s `PeerLinks::register`/`unregister` (matched by
comparing the exact `mpsc::Sender`, so a stale disconnect can't
clobber a fresher registration for the same connection). Membership
lives in `Broker` itself, not `thoth-mesh-node`, because the matching
logic it needs - "does this group's filter match the topic being
published" - is exactly what `Broker::publish` already does for
`patterns`; keeping group delivery there reuses that matching
instead of a second implementation of it in the node crate.

Delivery is a direct push onto the group member's own connection
`outgoing_tx` (`try_send`, the same channel `handle_subscribe`'s
`Ack`/`Error` replies and `PeerLinks::broadcast`'s catch-up already
use) - **not** a `broadcast::Receiver` a spawned forwarder pulls from.
This is a deliberate departure from how every other subscription
works (ADR-0021's per-topic `TopicChannel`): a `broadcast::Sender`
fundamentally delivers to *every* receiver, which is the one thing
group delivery must not do. On publish, `Broker` picks the next
member in rotation (an `AtomicUsize` cursor) and `try_send`s to it;
if that member's channel is full or already closed, it tries the next
member in rotation instead of blocking or dropping outright, up to
once around the whole group - only if every current member's channel
is unavailable does the message go undelivered for that group, the
same "zero live receivers right now" case `Broker::publish` already
treats as normal, not an error.

### No replay buffer, no lag recovery, for a group - live delivery only

A newly-joined group member does not get ADR-0021's backlog replay,
and a group member's delivery is a direct `try_send`, not a
`broadcast::Receiver` - so ADR-0024's lag-recovery-from-the-replay-
buffer doesn't apply either. Both exist to solve "a *specific*
subscriber missed something," which doesn't translate cleanly to a
group: replaying the full backlog to every newly-joining member would
mean each one reprocesses the group's entire history independently,
defeating the point of load-balancing it in the first place. A real
answer (partitioned or offset-based catch-up, Kafka-consumer-group
style) is a durable-storage feature - Phase 14's job, not this one.
v1's guarantee for a group is exactly as strong as ordinary
fire-and-forget delivery (ADR-0005), applied once instead of fanned
out: whichever member is live and has room gets it, once, live only.

### `ack: true` + `group: Some(_)` is refused outright in v1

The issue's own open question - what "acked" means for a group, not
an individual subscriber - doesn't have an answer this ADR is willing
to guess at: does an unacknowledged group delivery redeliver to the
same member (simple, but defeats load-balancing on a slow member),
or to a different one (a real work-queue semantic, but needs the
message kept somewhere until *some* member acks it, materially closer
to Phase 14 territory than a small addition)? ADR-0041's redelivery
is also structurally tied to a spawned forwarder pulling from a
`broadcast::Receiver`, which group delivery deliberately doesn't use
(see above) - there is no forwarder task to hold a pending-ack table
for a group member's deliveries as things stand.

Rather than half-implement one interpretation silently, a `Subscribe`
carrying both `ack: true` and `group: Some(_)` gets an `Error` (the
same reply shape a `--topic-acl` rejection uses) naming the
combination as unsupported, and nothing is registered. Plain `ack:
true` (no group) and plain `group: Some(_)` (no ack) both work exactly
as their own ADRs describe, independently.

## Consequences

- `MessageKind::Subscribe` grows a `group: Option<String>` field;
  every existing construction/pattern site across the workspace is
  updated, same mechanical footprint ADR-0041's `ack` field addition
  had.
- New `Broker::join_group`/`leave_group`, and a `groups` registry
  scanned (linear, same tradeoff `patterns` already accepts per
  ADR-0022) on every publish - acceptable for the expected scale (a
  handful of named groups per filter, not thousands of individual
  subscribers).
- `handle_subscribe` gains a fourth outcome alongside "registered" /
  "ACL-refused": "refused, `ack`+`group` unsupported together" - a
  new `Error` case, tested the same way the ACL-rejection path is.
- No new metric for "every group member unavailable" - philosophically
  identical to the existing, deliberately uncounted "publish with zero
  subscribers" case (`Broker::publish`'s own doc comment).
- `thoth-mesh-cli subscribe --group <name>` joins a named group
  instead of ordinary fan-out; combining it with `--ack` is rejected
  by the node the same way any other client hitting that combination
  is, not specially prevented client-side first.
