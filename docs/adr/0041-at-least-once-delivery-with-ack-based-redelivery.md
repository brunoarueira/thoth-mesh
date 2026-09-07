# 41. At-least-once delivery with ack-based redelivery

## Status

Accepted

## Context

Filed as #128 (Phase 13). Delivery today is fire-and-forget: a
`Publish` reaches whoever is currently subscribed, and PROTOCOL.md
says so explicitly ("No delivery confirmation for `Publish`"). A
subscriber that's momentarily behind gets ADR-0021's replay buffer and
ADR-0024's lag recovery, but neither of those helps a subscriber that
was live-connected and simply dropped a delivery (crashed mid-process,
closed without finishing work, etc.) - there's no signal the node
ever gets that a delivery *didn't* land, so nothing redelivers it.

The issue's known shape asked three questions:

- Build redelivery on the existing replay buffer, not new durable
  storage - full durable redelivery from arbitrary history is Phase
  14's job.
- Decide whether client acknowledgement of an individual delivery
  needs its own `MessageKind` or can extend an existing one.
- Decide the redelivery window/backoff, and what happens once a
  subscriber never acks at all.

## Decision

### Opt-in per subscription, not a new default

`MessageKind::Subscribe` gains a field: `ack: bool` (`#[serde(default)]`,
so an older sender omitting it decodes as `false` - unchanged
fire-and-forget behavior, the same additive-field rolling-upgrade
pattern ADR-0037's `StatusRequest`/`StatusReply` established). A
subscription made with `ack: false` (the default) behaves exactly as
before this ADR. `ack: true` opts that specific subscription into
holding each delivery until acknowledged, redelivering on a timeout.

Per-subscription rather than connection-wide or node-wide: a client
watching several filters over one connection (ADR-0033) may only want
the guarantee - and the redelivery traffic it implies - on some of
them.

### `Ack` is reused, in the other direction, rather than a new kind

`MessageKind::Ack { in_reply_to: MessageId }` already exists, sent by
a node to acknowledge a client's `Subscribe`/`Unsubscribe`. This ADR
reuses the exact same shape for a client acknowledging an individual
delivery, sent the other direction: `in_reply_to` names the `Publish`
envelope's own `MessageId`. The two are unambiguous by direction and
context - a node never receives a `Subscribe`/`Unsubscribe` to
acknowledge from a client, and nothing about the wire format needs to
distinguish "acking a request" from "acking a delivery"; both are
just "I received the message with this ID." A dedicated
`MessageKind::DeliveryAck` would say the same thing with a second
wire shape and a second thing for every implementation to handle,
for no behavioral difference.

Concretely: on the read loop, any `Ack` a node receives is now routed
to every forwarder on that connection with a pending-ack table (see
below); each forwarder discards it as a no-op if the ID isn't one of
its own. There's exactly one place `Ack` is interpreted as an incoming
delivery-acknowledgement (`ConnectionContext::handle_ack`), so this
costs no new dispatch complexity.

### Redelivery lives in the forwarder, keyed on the replay buffer's own `MessageId`s

A forwarder spawned for an `ack: true` subscription (see
`spawn_forwarder` in `connection.rs`) keeps a small in-memory table:
every `MessageId` it has sent but not yet had acknowledged, alongside
when it was last sent and how many times. A periodic sweep resends
any entry that's gone `DEFAULT_ACK_TIMEOUT` (5s) without an ack -
`outgoing_tx.send`ing the *same* `Arc<Envelope>` again, unchanged -
and gives up on an entry once it's been resent
`DEFAULT_MAX_REDELIVERY_ATTEMPTS` (5) times without an ack, dropping
it and logging a warning (counted by a new
`thothmesh_delivery_ack_timeouts_total` metric). No durable storage is
added - a node restart, or the forwarder task itself ending, loses
whatever was still pending, the same as every other in-memory piece of
state this node already accepts losing across a restart (Phase 14
is what closes that gap generally, not this one guarantee in
isolation).

Redelivery reuses the envelope, not the replay buffer directly, for
the *common* case, but a forwarder recovering from a broker lag
(ADR-0024) also has to fold recovered envelopes into the same pending
table if `ack` is set - resubscribing to the broker and only sending
once is table stakes: a lagged-and-recovering ack-forwarder still has
to track what it resent from the buffer the exact same way it tracks
a fresh live delivery.

Neither timeout nor attempt count is configurable via a CLI/env flag
yet - consistent with `DEFAULT_REPLAY_BUFFER_CAPACITY` and
`DEFAULT_TOPIC_MAP_CAPACITY`, both still hardcoded constants at this
point in the project. Worth revisiting once this has been exercised
for real.

### What "never acks at all" means in practice

Once `DEFAULT_MAX_REDELIVERY_ATTEMPTS` is exhausted, the message is
gone - there is no dead-letter destination in this phase (Phase 14's
"Message TTL / dead-lettering" is explicitly where that belongs). This
is a bounded, not unbounded, guarantee: "delivered, or redelivered up
to N times over roughly N × 5s, or given up on and counted" - not
"delivered eventually, however long that takes." A slow-but-alive
subscriber that acks *late* (after a resend already went out) is still
handled correctly: the late ack still removes the pending entry, it
just means one redundant resend already happened. At-least-once
allows exactly that - a subscriber can see the same message twice, and
the reference CLI's own auto-ack (below) accounts for this rather
than treating a duplicate as an error.

### Scope: one hop, not mesh-wide

Interest propagated across a peer link (ADR-0011) - the `Subscribe`
a peer sends another peer to say "forward me this filter" - always
uses `ack: false`, unconditionally, regardless of what any of this
node's own client subscriptions asked for. Propagating an
ack-requirement across peer links, so a multi-hop forward also
redelivers end-to-end, is a materially harder problem (which hop's
timeout applies? what does a partial ack across a chain even mean?)
that this ADR does not attempt. This closes the gap between a
`Publish` and the subscribers directly attached to the node that
delivers it - not for an arbitrary path across the mesh. Worth
revisiting from Phase 15 (federation-specific routing) once there's
a concrete need.

### CLI: `subscribe --ack`, always auto-acking

`thoth-mesh-cli subscribe` gains an `--ack` flag. With it set, every
`Subscribe` this invocation sends carries `ack: true`, and the CLI
sends an `Ack` for each delivery immediately after printing it. v1
doesn't expose "ack only after some external condition" - that's a
library concern (Phase 19), not this terminal tool's. The CLI is a
convenient way to *exercise* the guarantee (and to see redelivery
happen, e.g. by killing `thoth-mesh subscribe --ack` before it prints
a message and reconnecting isn't actually how redelivery is observed
here - only a still-connected, non-acking subscriber sees it, since
there's no durable state to resume from after a disconnect), not a
demonstration of a fully durable consumer.

## Consequences

- `MessageKind::Subscribe` grows an `ack` field; every existing
  construction site across the workspace (tests included) is updated
  to specify it, and every pattern match on the variant either reads
  it or is updated to ignore it explicitly (`..`).
- `MetricsSummary`/`render_prometheus` gain two counters:
  `redelivered_messages_total` (every resend, whether or not it's
  eventually acked) and `delivery_ack_timeouts_total` (every message
  given up on after exhausting redelivery attempts).
- Pending-ack table size is bounded only indirectly - by how much
  traffic accumulates before `DEFAULT_MAX_REDELIVERY_ATTEMPTS` gives
  up on each entry - not by an explicit cap the way `DEFAULT_TOPIC_MAP_CAPACITY`
  bounds the topic/pattern maps (ADR-0025). A publisher sustaining a
  high rate against a connected-but-silently-not-acking subscriber
  for the entire `DEFAULT_ACK_TIMEOUT × DEFAULT_MAX_REDELIVERY_ATTEMPTS`
  window accumulates a pending table proportional to that. Worth an
  explicit cap if this turns out to matter in practice; not added
  speculatively here.
- Does not touch `Broker` at all - deduplication (ADR-0011) and the
  replay buffer (ADR-0021) work exactly as before; a resend is just
  the forwarder re-sending an `Arc<Envelope>` it already had, never a
  second call to `Broker::publish`.
