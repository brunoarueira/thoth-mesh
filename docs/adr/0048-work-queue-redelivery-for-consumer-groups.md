# 48. Work-queue redelivery for consumer groups

## Status

Accepted

## Context

Filed as #158 (Phase 14). Consumer groups (#129/ADR-0042) load-balance
a topic across a group of workers, but delivery within a group is
exactly fire-and-forget: if the member a message was routed to dies
(or never finishes) before processing it, that message is simply gone.
ADR-0042 deliberately refused `ack: true` combined with `group:
Some(_)` rather than guess at what "acked" means for a group - its own
words: "does an unacknowledged group delivery redeliver to the same
member ..., or to a different one (a real work-queue semantic, but
needs the message kept somewhere until *some* member acks it,
materially closer to Phase 14 territory than a small addition)?" -
naming this exact gap as future work.

This is a different problem from #134/ADR-0046 (durable subscriptions):
that ADR is "let *the same* subscriber resume from where it left off
after reconnecting." This one is "let a *different* live group member
claim a message the original recipient never finished" - real
work-queue semantics (claim/lease/requeue), not resumption.

## Decision

### Reuse `ack` + `group`, not a new field

`ack: true` combined with `group: Some(name)` is no longer refused -
it now means work-queue delivery: each delivery is provisional until
*some* member of the group acks it, and reclaimable (redelivered,
possibly to a different member) if that doesn't happen in time. No new
`Subscribe` field, no wire change at all - this is exactly the
combination ADR-0042 reserved for this. `ack: true` alone (no group)
and `group: Some(_)` alone (no ack) are both unchanged. `durable`
combined with either `ack` or `group` remains refused, untouched by
this ADR.

A group's ack-tracking mode is decided once, by whichever `Subscribe`
first creates that `(filter, group name)` pair - the same "first
subscribe decides, a later mismatched request is logged and otherwise
ignored" convention `ack`/`group`/`durable` already follow
per-connection (ADR-0041/0042/0046), just enforced across every
member of a group instead of within one connection, since group state
already lives in `Broker`, shared by construction.

### Leases live in `Broker`, in-memory only - no new persistence

Each work-queue group's `GroupMembers` (already `Broker`-owned,
ADR-0042) gains its own `leases: HashMap<MessageId, GroupLease>` -
`{ envelope, leased_at, attempts }` - populated on every delivery
`Broker::publish` makes into an ack-tracked group, alongside the
existing round-robin `deliver()` call. This is deliberately **not**
persisted to disk. The issue's own framing suggested this "has to
survive somewhere beyond a single connection's forwarder" - true, but
that "somewhere" is satisfied by `Broker`'s already-shared, already-
central group state (visible to every member's connection, not tied
to whichever one received the original delivery) without needing disk
at all. A node restart already loses every other piece of in-flight
delivery state this system tracks - ADR-0041's own per-connection
pending-ack table included - and a work-queue lease is exactly that
category of state, not message history (which the on-disk log,
ADR-0045, already covers independently). Adding a persisted-lease
table would be real, non-trivial machinery (a new schema, restart-time
reconciliation) for a durability guarantee nothing else here offers
either; not worth it speculatively. If a real deployment needs
leases to survive a restart, that's the point to design it properly,
on its own merits - the same posture ADR-0046/ADR-0047 already took
on schema evolution and TTL granularity.

Keying leases by `MessageId` *within* each group's own `GroupMembers`
(rather than one broker-wide map) matters: two independently-named
groups on the same filter (ADR-0042 already allows this) both receive
the exact same envelope, same `MessageId` - a single global map
couldn't represent both leases at once, but one map per group can.

### Redelivery is a fresh round-robin call, not "retry the same member"

Reclaiming an expired lease calls the group's existing `deliver()`
(round-robin `try_send`) again - the same mechanism an ordinary
publish already uses, not a special "send back to the original
recipient" path. If that member is actually gone (disconnected), it's
no longer in `members` (removed by `leave_group` on disconnect) and
naturally can't be picked again; if it's alive but merely slow, it can
still be picked again by plain round-robin - no explicit avoidance,
the same limitation ADR-0041's own same-connection redelivery already
has for a slow-but-alive subscriber. Building "skip whoever last held
it" would need extra per-lease bookkeeping for a case not obviously
worth the complexity yet.

### One node-wide sweep, reusing ADR-0041's timeout/attempts and ADR-0047's dead-letter path

A new `Broker::sweep_group_leases(now, timeout, max_attempts) ->
Vec<Arc<Envelope>>` scans every group's leases: past `timeout` with
attempts remaining, it's redelivered (`deliver()` again, attempts
incremented, lease clock reset) exactly like above; past `timeout`
with attempts exhausted, the lease is removed and the envelope
returned to the caller. A member's channel being full/closed for
*every* current member on a given sweep pass is treated the same
"zero live receivers right now" non-error `Broker::publish` already
does - the lease is left untouched, retried again next sweep without
burning an attempt.

Reuses `redelivery::DEFAULT_ACK_TIMEOUT`/`DEFAULT_MAX_REDELIVERY_ATTEMPTS`
(5s / 5 attempts) rather than inventing group-specific ones - no
evidence yet that work-queue groups need a different default than
ordinary `ack: true` delivery does. A new `thoth-mesh-node::work_queue`
module spawns one always-on background task (unconditionally, at
every node startup, the same posture `peering::spawn_discovery_dialer`
already has - cheap to poll when no ack-tracked group exists yet)
that calls `sweep_group_leases` on `redelivery::sweep_interval`'s
existing cadence, and for anything given up on, dead-letters it via
the already-existing `dead_letter()` helper (ADR-0047) if
`--dead-letter-topic` is configured - standalone, no `--data-dir`
needed, the same as the ack-giveup source ADR-0047 already wired.

### Ack routing: scoped to the acking connection's own group memberships

`ConnectionContext::handle_ack` already loops over this connection's
own `forwarders` to find which pending-ack table (if any) an incoming
`Ack` belongs to (ADR-0041). It now also tries every
`Subscription::Group(name)` entry against a new
`Broker::ack_group_delivery(filter, group, message_id)`, which
removes that `(filter, group)`'s lease for `message_id` if present -
a no-op otherwise (never registered, already acked, already given up
on, or this group isn't ack-tracked at all). Scoping the lookup to
groups *this connection* actually belongs to - rather than a single
broker-wide "clear by `MessageId` alone" - is what correctly handles
two groups sharing a filter and therefore the same delivered
`MessageId` independently, and mirrors the existing per-forwarder
`ack_tx` loop's own shape exactly.

No new metric: a resend is counted the same as any other
(`thothmesh_redelivered_messages_total`), a give-up the same as any
other (`thothmesh_delivery_ack_timeouts_total`), and a dead-lettered
give-up the same as any other (`thothmesh_dead_lettered_messages_total`,
ADR-0047) - an operator doesn't need a separate counter to know these
came from a group specifically versus an ordinary `ack: true`
subscription; the existing three already answer "how much is being
redelivered / given up on / recovered somewhere inspectable" either
way.

## Consequences

- `MessageKind::Subscribe`'s wire shape is unchanged - `ack: true` +
  `group: Some(_)` simply stops being refused.
- `Broker::join_group` gains an `ack: bool` parameter. `GroupMembers`
  gains `ack_tracked: bool` and `leases: HashMap<MessageId,
  GroupLease>`. New `Broker::ack_group_delivery` and
  `Broker::sweep_group_leases`.
- `ConnectionContext::handle_ack` iterates `forwarders` by `(filter,
  subscription)` instead of `subscription` alone, to route a `Group`
  entry's ack correctly.
- New `thoth-mesh-node::work_queue` module: one always-on background
  sweep task, spawned unconditionally alongside the existing
  discovery-dialer task.
- `thoth-mesh-cli subscribe --group <name> --ack` now works instead of
  being rejected by the node - no CLI-side change needed, the
  combination was never prevented client-side to begin with (ADR-0042).
- Explicit "skip the original recipient on reclaim" and persisted,
  restart-surviving leases are both still out of scope, left for a
  future ADR if real usage shows either is needed.
