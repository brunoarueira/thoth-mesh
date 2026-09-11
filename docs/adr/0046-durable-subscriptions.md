# 46. Durable subscriptions via per-subscriber offset tracking

## Status

Accepted

## Context

Filed as #134 (Phase 14), depends on #133/ADR-0045's on-disk log. A
subscriber that reconnects after being gone longer than the replay
buffer's window (1024 messages, ADR-0021) has simply lost whatever it
missed - there's no way to resume from an exact position. The issue's
known shape asked for a subscriber-supplied offset/cursor and a way
to identify "the same subscriber" across reconnects, naming `PeerId`
(cryptographically meaningful for a TLS-identified connection since
ADR-0038) as the natural fit.

## Decision

### Identity-keyed, automatic resume - no offset numbers on the wire

`Subscribe` gains `durable: bool` (`#[serde(default)]`, same
rolling-upgrade pattern as `ack`/`group`/`retain`). A durable
subscription is keyed by `(PeerId, Topic)`: the node remembers, per
subscriber identity and topic, the last message delivered, and a
later `Subscribe { durable: true }` from the *same* identity resumes
from there automatically - no client-supplied numeric cursor, no
`seq` or offset exposed on the wire at all. The subscriber's identity
*is* the cursor key.

This is a deliberate departure from the issue's literal
"subscriber-supplied offset" phrasing. An opaque, server-remembered
position is simpler for a client to use correctly (nothing to store,
nothing to get wrong on reconnect), doesn't leak an internal
implementation detail onto the wire, and is what "durable
subscription" means in every system this project takes inspiration
from (Kafka consumer groups, MQTT persistent sessions) - you identify
yourself, the broker remembers where you were. An explicit seek/
rewind (start from a specific point, or from the beginning) is a
legitimate future addition but isn't needed for the issue's actual
goal and adds real complexity of its own; left out of v1.

### Requires a TLS-authenticated identity - refused otherwise

A resumable position is only meaningful if the identity it's keyed on
is stable across reconnects. `PeerId::new()` (no client certificate)
is fresh and random every connection (ADR-0038's own doc: "no
persistent identity across restarts") - a `durable: true` subscribe
with no TLS client certificate presented is refused with an `Error`,
the same "refuse outright rather than silently do something that
doesn't work" precedent ADR-0042 set for `ack`+`group` together. The
identity a durable subscription resumes under is always the
authenticated one (ADR-0039), never a self-reported claim.

### Literal topics only - a wildcard filter is refused

A wildcard `Subscribe` (ADR-0022) matches a *set* of concrete topics,
each with its own independent position - "resume this pattern" isn't
one offset. `durable: true` combined with a wildcard filter is
refused with an `Error`, the same wildcard-refused-outright precedent
`--topic-acl` (ADR-0018) and consumer groups already use. Watching
several concrete topics durably just means subscribing to each
individually (ADR-0033 already allows more than one filter per
connection).

### On-disk shape: one new column, one new table - no migration framework

Two changes to the store from ADR-0045:

- `messages` gains a `msg_id` column (the envelope's own `MessageId`,
  16 raw bytes) with an index on `(topic, msg_id)`, so "everything
  after position P for topic T" is an efficient range query -
  `MessageId` is a UUIDv7, monotonic by creation time (ADR-0005), so
  `msg_id > P ORDER BY msg_id` is both correct and cheap, the same
  "safe to sort/compare" property ADR-0043's retained-value merge
  already relies on.
- A new `subscriber_offsets(peer_id, topic, last_msg_id, updated_at)`
  table, one row per `(subscriber, topic)` pair ever durably watched.

Neither needs a general migration mechanism. A brand-new database's
`schema.sql` already creates `messages` with `msg_id` included and
`subscriber_offsets` as just another `CREATE TABLE IF NOT EXISTS` -
adding a table has always been free this way, migration or not. The
one genuine gap is a database that already exists from before this
ADR (#133-era): its `messages` table is missing the column, and
`CREATE TABLE IF NOT EXISTS` can't add a column to a table that
already exists. `SqliteStore::open` closes that gap with a single,
narrowly-scoped check before running `schema.sql`: if `messages`
exists and lacks `msg_id`, `ALTER TABLE messages ADD COLUMN msg_id
BLOB` first. That's the whole mechanism - one guarded statement for
the one thing idempotent `CREATE TABLE`/`CREATE INDEX ... IF NOT
EXISTS` genuinely can't express, not a directory of ordered migration
files or a version-tracking runner. Rows persisted before this
column existed have `msg_id = NULL` and are simply invisible to a
durable catch-up query - a durable subscription only ever resumes
into history persisted after it started being tracked, never
backfills the meaning of old rows.

If a future change needs something this narrow fix can't express
(reshaping existing data, not just adding new capability going
forward), that's the point to design a real migration mechanism - on
its own merits, once there's an actual second case to generalize
from, not speculatively here for a single column and a single new
table.

### A new forwarder variant, not a change to the existing ones

A `durable: true` subscribe gets its own forwarder
(`spawn_durable_forwarder`), parallel to the plain and `ack: true`
ones (ADR-0041) rather than a flag threaded through the existing one:

1. Look up the stored position for `(subscriber, topic)`.
   - **None** (never seen this subscriber+topic before): behaves
     exactly like an ordinary subscribe - the in-memory replay
     buffer's backlog, nothing from disk. A durable subscription's
     value is resuming across reconnects, not an unconditional full
     history replay for a first-time subscriber.
   - **Some(last_id)**: fetch every persisted message for that topic
     with `msg_id > last_id` from disk - unbounded, not capped the
     way the in-memory replay buffer or `load_recent` are, since
     "give back everything you missed" is the point. A subscriber
     offline long enough on a busy topic can trigger a large
     catch-up batch (bounded in practice only by
     `DEFAULT_PERSISTED_MESSAGES_PER_TOPIC`, ADR-0045); worth
     revisiting (streaming instead of materializing one `Vec`) if it
     ever matters at this project's scale.
2. Merge that disk catch-up with the ordinary in-memory backlog
   snapshot (`TopicChannel::subscribe`, still atomic with the live
   receiver registration exactly as ADR-0021 established), first
   dropping any buffered entry at or before `last_id` - it was already
   delivered and recorded before this subscriber reconnected, so a
   surviving copy of it in the small in-memory buffer must not come
   back a second time. What's left is deduplicated by `MessageId` and
   sorted by it - the same merge shape ADR-0043 uses for a wildcard
   subscriber's retained values, just with that one extra filter step.
   A narrow race is possible at the seam between the (unlocked, disk)
   catch-up read and the (locked, in-memory) buffer snapshot: a
   publish landing in that exact window can appear in both and, after
   dedup, still be followed by one genuine live delivery of it. This
   can only ever cause a harmless duplicate, never a loss - consistent
   with `PROTOCOL.md`'s standing "de-duplication, not exactly-once"
   stance - and avoiding it entirely would mean holding the topic's
   lock across a disk read, serializing every publish on that topic
   behind however long the catch-up query takes. Not worth it.
3. After every send - catch-up batch or live - record that envelope's
   own `id` as the new position for `(subscriber, topic)`, before
   moving on to the next. Best-effort like the persist path itself
   (ADR-0045): a failure to record a position is logged, not fatal,
   and just means the next reconnect might replay a little more than
   strictly necessary - never less.

Counted under the existing `replayed_messages_total` metric (a
durable catch-up *is* a replay, just from a different source and with
no cap) - no new metric.

## Consequences

- `MessageKind::Subscribe` grows a `durable` field - the same
  mechanical footprint the last four ADRs each had.
- `messages.msg_id` (+ index) and `subscriber_offsets` added to the
  store; `SqliteStore::open` gains one narrowly-scoped
  add-column-if-missing check, not a migration framework.
- `MessageStore` gains `messages_since`/`record_offset`/`load_offset`.
  `Broker` gains `subscribe_durable`, returning a typed error for the
  no-store/wildcard-filter cases rather than silently downgrading to
  an ordinary subscribe. `ConnectionContext::handle_subscribe` checks
  the remaining requirements - no TLS client certificate, `ack`/
  `group` also set - synchronously, before ever calling it, since
  those are per-connection facts the broker itself has no way to know.
- `thoth-mesh-cli subscribe --durable` (requires `--tls-cert`/
  `--tls-key`, same as the node-side requirement - not pre-validated
  client-side, the node's `Error` is the single source of truth,
  matching ADR-0042's precedent).
- Explicit seek/rewind, and durability for consumer-group members
  (#158, itself deferred pending this), are both out of scope here.
