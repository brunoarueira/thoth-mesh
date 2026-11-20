# 60. Log `trace_id` at each hop; defer OpenTelemetry

## Status

Accepted

## Context

Issue #147 (Phase 17, [docs/ROADMAP.md](../ROADMAP.md)): ADR-0059 gave
a `Publish` a `trace_id` that survives forwarding unchanged, but
nothing yet makes it observable - a trace id nobody can see is just
dead weight on the wire. The roadmap names two possible bars:
"logged at each hop at minimum," and - as the "likely mechanism" for
going further - bridging this codebase's existing `tracing`
instrumentation (ADR-0007 and friends) into real OpenTelemetry spans
via `tracing-opentelemetry`, exportable to a backend like Jaeger or
Tempo, rather than a bespoke trace format.

### The OpenTelemetry bridge is a substantially bigger decision than "log it"

Before implementing, it's worth being explicit about why this ADR
doesn't just do both. Making an OTel bridge actually *useful* - one
connected distributed trace per message, spanning every node it
touched, rather than several disconnected single-span traces that
happen to share a tag - needs more than adding a layer to the
existing `tracing_subscriber::registry()`:

- Each hop's span would need to be constructed with our own
  `trace_id` as the OTel `SpanContext`'s own trace id (OTel normally
  mints its own, root-span-first), with the right remote-parent
  linking so an exporter actually groups hops into one trace instead
  of several.
- A real exporter (OTLP being the obvious choice) needs somewhere to
  actually send spans - a `--otel-endpoint`-shaped flag, off by
  default, matching this project's convention for every other
  optional external surface (`--metrics-addr`, `--data-dir`, ...).
- None of `opentelemetry`/`tracing-opentelemetry`/an OTLP exporter are
  dependencies anywhere in this workspace today - this would be a new
  dependency family, not an incremental addition to one already in
  use.
- Verifying it end-to-end needs something to actually export to
  (a real Jaeger/Tempo instance), not just unit tests.

That's a real, multi-part design decision on its own - deserving its
own ADR and its own PR when it's picked up, not a few extra lines
folded into "make the id observable." This ADR explicitly scopes
itself to the minimum bar only.

## Decision

### Log `trace_id` at the existing per-hop log sites, nothing else

`handle_publish`'s three existing log lines in `connection.rs` - the
unconditional `debug!("publish")` that already fires once per node for
every `Publish` it processes (client-originated or forwarded alike,
see ADR-0059's own Context section on why this is a true per-hop
site), the topic-ACL rejection `warn!`, and the rate-limit rejection
`debug!` - each gain a `trace_id = ?envelope.trace_id` field,
consistent with how `sender` is already logged on the same lines (`?`
for `Debug`, matching every other `PeerId`/`MessageId` logging site in
this codebase - no new `Display` impl for `TraceId` needed for this).

Rejections are included deliberately, not just the success path: an
operator asking "why didn't this reach node C" needs to see *where* a
traced message stopped and *why*, not just where it kept going. No
other log site changes - `spawn_forwarder`'s per-downstream-connection
sends aren't included, since the per-node "I processed this" line
already captures the one thing worth seeing at each hop a message
actually reaches; a separate per-downstream-fan-out line would mostly
just restate how many peer links were active at that node, not
anything about the message's own journey.

### `docs/OPERATIONS.md` gets a short how-to, not a dedicated query surface

A new "Tracing a message across peer hops" subsection (under
`## Logging`) explains the mechanism: run every node at `debug`,
collect each node's own logs, grep for the same `trace_id`. No
dedicated cross-node query/export surface is built - the roadmap
itself names that as "possibly later," and it's a different, larger
problem (aggregating logs - or spans - from several independently
running node processes) than this ADR's narrow scope.

## Consequences

A `trace_id` is now visible in plain-text logs wherever `RUST_LOG`/
`--log-level` is turned up to `debug`, with zero new dependencies and
zero new configuration surface - grep is the query mechanism, same as
every other piece of ad hoc debugging this project's logs already
support. Correlating a message across *several separately running
node processes* still means collecting their logs yourself first
(there's no single place today that already aggregates logs from
more than one node) - unchanged by this ADR, and not a regression it
introduces.

This deliberately leaves the roadmap's "likely mechanism" (the
OpenTelemetry bridge) and its own "dedicated query/export surface"
both undone. Picking either up later is a new ADR's decision to make,
not an extension of this one.
