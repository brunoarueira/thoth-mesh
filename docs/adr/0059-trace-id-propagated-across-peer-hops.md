# 59. A `trace_id` propagated unchanged across peer hops

## Status

Accepted

## Context

Issue #146 (Phase 17, [docs/ROADMAP.md](../ROADMAP.md)): `status`
(ADR-0037) shows a node's point-in-time state, but nothing shows the
path a single published message actually took across a multi-hop
mesh - essential for debugging routing/federation issues that only
show up in a real topology, not a single node. This ADR covers only
the foundational piece: a correlation id that actually makes the trip
hop to hop. Issue #147 (making that id observable - logged at each
hop at minimum, likely bridged into real OpenTelemetry spans via
`tracing-opentelemetry` later) is deliberately out of scope here.

### Neither existing identifier already does this

Two fields already travel with a forwarded `Publish`, and neither is
fit for purpose:

- **`MessageId` (`envelope.id`)** does stay unchanged hop to hop for a
  forwarded `Publish` - every hop's `Broker::publish` round-trips it
  verbatim, and that stability is exactly what loop-prevention/dedup
  (ADR-0011) keys on. But it identifies *this one message instance*,
  not its journey - two publishes with byte-identical payloads still
  get two different, unrelated `MessageId`s - and overloading it as
  the tracing key as well would
  couple a debugging concern to a correctness mechanism for no
  reason, and doesn't generalize to anything that mints a *new*
  `MessageId` per hop by design (the interest-propagation `Subscribe`/
  `Unsubscribe` flood-fill ADR-0011 also describes forwards a fresh
  envelope, and fresh `id`, at every hop).
- **`sender` (`envelope.sender`)** looks, at first glance, like it
  might at least tell you who originally published a message. It
  doesn't, for any connection with a known TLS client-certificate
  fingerprint: `ConnectionContext::authenticated_sender` (ADR-0039)
  runs unconditionally on *every* decoded frame on *every* such
  connection, and silently overwrites `sender` to the fingerprint's
  identity whenever it disagrees with the envelope's claimed value -
  which, for a forwarded `Publish` past the first hop over a
  certificate-backed connection, it always does (hop 2's connection to
  hop 1 is authenticated as hop 1, not as the original publisher
  several hops back). A connection with no client certificate at all
  (plaintext, or TLS with none configured) has no fingerprint to
  correct against, so `claimed` passes through unchanged there -
  `sender` is unreliable specifically wherever ADR-0039's correction
  is actually active, not universally. Confirmed live with a real
  3-node mTLS chain (A - B - C, A and C never directly peered): by the
  time C receives A's publish via B, `sender` reads B, not A - and the
  correction fires, and warns, on every such hop, for every message,
  as the normal shape of relaying rather than anything exceptional.
  Filed as #221 (not fixed here - a `sender` correction that assumes
  "I'm looking at this connection's own identity claim" doesn't hold
  once the thing crossing that connection is something it's relaying
  on someone else's behalf, which is a problem bigger than this ADR).

Given neither existing field can double as a stable per-journey
correlation key, this needs a field of its own.

## Decision

### A new `TraceId`, distinct from `MessageId`, on every `Envelope`

`thoth-mesh-core` gains a `TraceId` type (`message.rs`, next to
`MessageId`): the same wire shape (a UUIDv7 wrapper,
`#[serde(transparent)]`, encoded as raw 16 bytes) and the same
`new()`/`as_bytes()`/`from_bytes()`/`Default` surface, reusing the
"v7" `uuid` feature already enabled rather than pulling in a "v4" one
for no real benefit here. It's deliberately a smaller type than
`MessageId`, though: no `timestamp()` accessor and no `PartialOrd`/
`Ord` - nothing here needs a trace to be sortable or to expose when it
started, and ADR-0046's reason for `MessageId` needing that (an
indexed range query) doesn't apply to a field nothing persists.
`Envelope` gains a `trace_id: TraceId` field, set by `Envelope::new()`
the same way `id` already is
- every freshly constructed envelope mints its own fresh `TraceId`, no
exceptions and no cross-linking to whatever caused its creation (an
`Ack`/`Error` replying to a `Publish` gets its own new `TraceId`, same
as it already gets its own new `MessageId` - correlating a reply to
its request is what `in_reply_to` is for, ADR-0041/ADR-0050, and nothing
here changes that).

This means `trace_id` is present on every message kind uniformly
(simplest mechanism, no special-casing by `MessageKind`), but is only
ever meaningful as a *journey* for the one case that actually re-sends
the same envelope unchanged across hops: a forwarded `Publish`. A
`Subscribe`/`Unsubscribe` re-broadcast by interest propagation
(ADR-0011) mints a fresh envelope, and so a fresh `trace_id`, at every
hop by the same existing design that already gives it a fresh
`MessageId` - tracing *that* flood-fill, if ever wanted, is a
different, unscoped problem, not this one.

### Propagation needs no new plumbing at all

`connection.rs`'s forwarding path already carries a decoded `Envelope`
hop to hop as a single `Arc<Envelope>`, re-serialized unchanged by
`to_bytes()` at each outgoing hop (`id` and `sender` already travel
this way - see ADR-0011's "CBOR round-trips the `id` field unchanged"
and ADR-0039's correction of `sender` being the one and only mutation
ever applied to an in-flight envelope). Adding `trace_id` as an
ordinary struct field means it rides along for free through that exact
mechanism: nothing in `connection.rs`, `broker.rs`, or any forwarder
needs to change to make it cross a peer link. The entire implementation
of this ADR is the new type, the new field, and the wire/doc updates
below.

### Wire compatibility: the established `#[serde(default)]` pattern, no version bump needed

Every prior addition to an existing message shape (`retain`/
`content_type` ADR-0043/0044, `ack`/`group`/`durable` ADR-0041/0042/
0046, `reply_to`/`in_reply_to` ADR-0050) used `#[serde(default)]` so an
older build's envelope, missing the new field entirely, still decodes
- CBOR structs here are encoded as maps keyed by field name (not a
positional sequence), so decoding matches fields by name regardless of
what's present, missing, or reordered. `trace_id` follows the same
convention, but with a twist the boolean/`Option` precedents didn't
need: there's no "off"/`None` value that means anything for a trace
id, so the default has to be a freshly-minted `TraceId` (`TraceId`'s
own `Default` impl calls `TraceId::new()`, exactly mirroring
`MessageId`'s existing `Default`), not an empty/absent one. Decoding an
old-shaped envelope that lacks `trace_id` at all means "no visibility
further upstream than the oldest build touching it" - the trace
restarts right there - rather than a decode error. This needs no
protocol-version gate (Phase 18, issue #149/#150, isn't done yet, and
this doesn't wait on it) for the same reason none of the earlier
additive fields did.

### Interaction with loop prevention (ADR-0011)

A traced message re-entering an already-visited node is exactly what
loop prevention exists for, and needs no new handling here: a
duplicate (by `MessageId`) is dropped at the top of `Broker::publish`,
*before* it ever reaches a forwarder - so within the dedup cache's own
window (`SeenIds`, capacity `DEFAULT_DEDUP_CAPACITY`, oldest-evicted -
see ADR-0011), a cyclic mesh can't make a given `trace_id` bounce
around forever. That's a cache-window guarantee, not an absolute one:
the same eviction that already means a sufficiently delayed genuine
duplicate can be re-accepted after enough *other* messages have cycled
through (ADR-0011's own trade-off, not new here) applies equally to a
looped copy of a traced message - just at whatever low probability
that already implies today. There's nothing for this ADR to add here
beyond stating it explicitly, which issue #146 itself called out as
worth doing.

## Consequences

`PROTOCOL.md`'s `Envelope` table gains a `trace_id` row and a
`### TraceId` subsection (mirroring `### MessageId`'s), and its "exactly
these four keys" framing becomes five. No other renderer needs
updating: nothing in `thoth-mesh-node`/`thoth-mesh-cli` currently prints
individual envelope fields for a received `Publish` (checked - `print_if_publish`
only ever reads `topic`/`payload`/`content_type`), so there's no second
place for issue #147 to have to keep in sync when it adds the actual
logging.

A `trace_id` exists on control messages (`Hello`, `Ack`, `Error`,
`StatusRequest`/`Reply`, ...) where it's never read or useful - an
accepted, uniform-field cost rather than threading a second,
`Publish`-only wire shape through the protocol for one field.

This ADR does not fix #221 (the `sender`-rewrite/warn-spam behavior
discovered while confirming `sender` couldn't substitute for this) -
that's tracked separately. It also doesn't implement any
observability for the new field - that's #147.
