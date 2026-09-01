# 24. Recovering a lagged forwarder from the replay buffer

## Status

Accepted

## Context

Issue #85: when a forwarder falls behind its `broadcast::Receiver` and
hits `RecvError::Lagged` (`connection.rs`'s `spawn_forwarder`), it
today just logs a warning, bumps `thothmesh_forwarder_lag_total`, and
continues from wherever the broadcast channel now stands - the
skipped envelopes are gone for that subscriber, permanently.
ADR-0021 already keeps each topic's recent history in a bounded
replay buffer for late subscribers; a lagged forwarder is, in effect,
a subscriber that's gone briefly "late" mid-stream, and the same
buffer can backfill what it missed instead of accepting silent loss
outright.

The issue's "Known shape" flagged the open question as *how* to
correlate a `Lagged` event against buffer contents, since the buffer
is a plain `VecDeque` with no notion of "where" a given forwarder
currently is, and asked whether `TopicChannel`'s shape needs to
change to support it.

## Decision

### Prerequisite: the replay buffer has to outlive the broadcast channel's own window

Before any position-tracking design matters, there's a sizing
constraint that has to hold or recovery can never find anything at
all: `tokio::sync::broadcast::Receiver::recv` only ever returns
`Lagged(missed)` once this receiver's next unread message has already
fallen outside the broadcast channel's own fixed-size ring (confirmed
against `tokio`'s actual implementation, not just its docs - `recv_ref`
computes the receiver's post-lag position as `tail.pos - capacity`,
i.e. exactly the oldest message the *channel itself* still retains).
Given `DEFAULT_REPLAY_BUFFER_CAPACITY` was set equal to
`DEFAULT_TOPIC_CHANNEL_CAPACITY` under ADR-0021, a replay buffer of
the same capacity has, by construction, *already evicted the exact
same range* by the time a `Lagged` error can even fire - there is
mathematically nothing left there to recover. ADR-0021 never hit this
because it only ever reads the buffer at `subscribe` time, never
correlates it against a live receiver's lag position.

`DEFAULT_REPLAY_BUFFER_CAPACITY` therefore changes from 256 (equal to
`DEFAULT_TOPIC_CHANNEL_CAPACITY`) to 1024 (4x) - the extra headroom
above the broadcast channel's own window is what a lagged forwarder
can actually recover into, bounded by that difference. This is a
`thoth-mesh-broker` constant change, not an API change: `Broker`'s
public surface (`subscribe`/`publish`) is untouched, so every existing
caller and test keeps working unchanged, just against a bigger number.
Late subscribers (ADR-0021's original use of this same buffer) get a
small side benefit too - more history available to catch up on - but
that's incidental, not why the number moved.

### Position: the forwarder remembers the last `MessageId` it delivered, nothing more

No new field on `TopicChannel`, no per-forwarder cursor stored in the
broker. `spawn_forwarder` already delivers every envelope through one
`outgoing_tx.send` call site per source (the initial backlog replay
loop, and the live `rx.recv()` loop); both are changed to record the
`MessageId` of the last envelope actually sent, in a local
`last_delivered: Option<MessageId>`. This is the same shape of
information ADR-0021 already uses to reason about ordering
(`MessageId` identifies an envelope end-to-end, ADR-0011's dedup
already relies on this), just kept by the forwarder instead of the
broker.

### Recovery: re-subscribe to the same filter and diff against `last_delivered`

On `Lagged`, instead of only logging and continuing on the existing
receiver, the forwarder calls `Broker::subscribe` again for the same
`filter` - the exact same call `spawn_forwarder` already makes once at
startup. This reuses ADR-0021's existing atomicity guarantee (a
buffer snapshot and a new receiver's registration happen under one
lock, so nothing published between them can be missed or duplicated)
instead of inventing a second, parallel mechanism for "peek the
buffer without registering a receiver." The forwarder never touches
`TopicChannel`'s internals directly - `Broker`'s public API is
unchanged by this ADR.

The fresh backlog is then diffed against `last_delivered`:

- **Found in the fresh backlog** (the common case now that the buffer
  outlives the broadcast window - see above): everything *after* that
  position is what was missed. Replayed in order, then the loop
  continues on the new receiver.
- **`last_delivered` is `None`** (a lag on the very first `recv()`,
  before anything was ever delivered live - possible if the topic
  overflows between `subscribe` and this forwarder's first poll):
  nothing has been delivered yet, so there's no risk of duplicating
  anything - the entire fresh backlog is replayed.
- **Not found, and `last_delivered` is `Some`** (the gap exceeds even
  the buffer's extra headroom by the time recovery runs): a genuinely
  unrecoverable gap, same as before this ADR. Nothing is replayed -
  guessing at a boundary here risks re-delivering an envelope this
  forwarder already got live before lagging, which the issue calls
  out as a hard correctness requirement. The forwarder still switches
  to the new receiver, so at least delivery going forward has a clean
  atomic starting point.

In every branch, the old receiver is discarded in favor of the new
one - there is no case where continuing to use the pre-lag receiver is
better than a receiver whose starting point is provably consistent
with whatever was just replayed.

### A new metric, distinct from both existing ones

`thothmesh_lag_recovered_total` counts envelopes recovered this way,
separate from:

- `thothmesh_forwarder_lag_total` - `tokio::sync::broadcast`'s own
  `skipped` count, a point-in-time measurement taken when the error
  fires.
- `thothmesh_replayed_messages_total` (ADR-0021) - backlog delivered
  to a *newly-spawned* forwarder, not a recovering one.

These three can legitimately disagree: `skipped` reflects the gap at
the moment `Lagged` fired, while the buffer snapshot used for recovery
is taken slightly later (once the re-subscribe call actually runs),
so it can hold more or fewer entries than `skipped` reported - more if
additional publishes landed in the interim, fewer if the gap already
exceeded even the buffer's extra headroom and some of what `skipped`
counted was already unrecoverable. An operator comparing
`lag_recovered_total` against `forwarder_lag_total` over time sees how
much reported lag is actually being absorbed by the replay buffer
versus genuinely lost.

### Applies uniformly to clients and peer links

`spawn_forwarder` is the one shared code path for both (ADR-0010),
unchanged by this ADR - no special-casing needed, same as ADR-0021.

## Consequences

Recovery costs one extra `Broker::subscribe` call (a brief write-lock
to get-or-insert the topic/pattern entry, then the channel's own
lock) per lag event, not per envelope. Lag events are already
warning-logged as a sign of a slow consumer, so this isn't a
hot-path cost.

Still bounded, not guaranteed, exactly as the issue anticipated: a
forwarder that fell behind by more than the buffer's headroom above
the broadcast channel's own capacity still has a genuine, permanent
gap for whatever fell off the buffer's oldest end before recovery
could run. This is the same tradeoff ADR-0021 already accepted for a
fresh late subscriber, extended to a forwarder that goes "late"
mid-stream rather than starting out that way.

Every topic's replay buffer now holds up to 1024 envelopes instead of
256 - a 4x increase in the existing per-topic memory footprint ADR-0021
already flagged as a #72 (bound per-node memory footprint) concern.
This ADR makes that number bigger for a real reason (without it,
recovery cannot work at all - see the sizing constraint above), but
it's still the same open question, now with a bigger number attached;
#72 remains the right place to decide on eviction/bounding policy
across every such structure at once.

`PROTOCOL.md`'s "a slow subscriber can miss messages" bullet is
updated to describe this partial recovery rather than unconditional
silent loss.

This closes #85.
