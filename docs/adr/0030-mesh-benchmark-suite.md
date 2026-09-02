# 30. A benchmark suite for throughput and latency across N mesh hops

## Status

Accepted

## Context

Issue #51: there's a real federated implementation now (Phases 2-4),
but no measurement of how it actually performs - message throughput,
end-to-end latency, and how both degrade as a message crosses more
peer hops. The issue named three open questions to resolve here.

## Decision

### A hand-rolled load-generation binary, not `criterion`

`criterion` is built for microbenchmarks - repeatedly calling one
function in-process, with statistical warm-up/outlier handling around
that. What this issue actually needs is closer to a small load test: a
real multi-node mesh, real TCP sockets, a sustained burst of messages
crossing some number of real peer links. Coercing that into
`criterion`'s per-iteration closure model would mean either spinning up
a fresh mesh on every iteration (dominating the measurement with setup
cost) or reusing one mutable mesh across iterations (accumulating
state - subscriptions, dedup cache entries - `criterion` isn't designed
to reset between runs). Neither fits, and pulling in a new workspace
dependency for a fit that awkward isn't worth it, consistent with this
project's standing preference for minimal dependencies (the same
reasoning ADR-0008 already applied to framing).

Instead, a plain binary - `crates/thoth-mesh-node/examples/bench_mesh.rs`
- run via `cargo run --release --example bench_mesh`. No new
dependency: an example target already has full access to its crate's
own public API (`thoth_mesh_node::spawn`, same as an integration test)
and everything already in `[dependencies]`. `--release` matters here in
a way it doesn't for correctness tests - LTO and codegen-units=1 (this
workspace's release profile) meaningfully change throughput numbers a
debug build wouldn't represent honestly.

### What's measured: a hop-count sweep, reusing the existing chain topology

For `hops` in `[0, 1, 2, 4, 8]`:

- `hops = 0` is a single node - publisher and subscriber both connect
  to it directly, no peer link at all. This is the floor every other
  measurement is read against: broker dispatch and connection-handling
  overhead alone, with zero network hops to blame it on.
- `hops = N` chains `N + 1` nodes exactly the way `multi_hop_interest_
  propagates_across_a_chain_of_peers` (`tests/integration.rs`) already
  does - node `i` seeds only node `i - 1`'s address. The publisher
  connects to the chain's first node, the subscriber to its last.

For each hop count, the publisher writes `MESSAGE_COUNT` (5,000)
`Publish` envelopes back-to-back on one connection - `Publish` gets no
`Ack` in this protocol (`PROTOCOL.md`), so nothing but the connection's
own backpressure paces the burst, which is the real sustained-throughput
question this issue is actually asking. The subscriber reads until it's
seen all of them, recording each one's arrival instant.

Two numbers come out of that:

- **Throughput**: `MESSAGE_COUNT` divided by the wall-clock time from
  the first write to the last delivery.
- **Latency**: each `Envelope`'s `MessageId` is a UUIDv7, which already
  embeds the publish instant (`ADR-0005`) - no extra payload
  instrumentation needed, and it measures the exact metadata a real
  deployment already carries on the wire, not a benchmark-only
  addition. Latency is receipt time minus that embedded timestamp,
  reported as min/p50/p95/max. UUIDv7 embeds millisecond, not
  microsecond, resolution - coarse for a loopback single-hop number
  that may genuinely be sub-millisecond, but the hop-count *trend* this
  issue actually asks about still shows clearly at millisecond
  granularity. Finer-grained timing is a fine follow-up if this ever
  proves too coarse to be useful, not built speculatively now.

Payload size is fixed at 64 bytes (a small, representative event, not a
transfer-throughput test); sweeping payload size is a dimension left
for later if it turns out to matter.

### The subscriber tolerates loss instead of assuming exactly `MESSAGE_COUNT` arrives

An early version of this benchmark read exactly `MESSAGE_COUNT`
deliveries and hung indefinitely at higher hop counts: an unpaced
5,000-message burst can outrun the per-topic broadcast channel
(capacity 256) faster than a forwarder several hops downstream can
drain it, and if the resulting `Lagged` gap exceeds the replay buffer
(1,024, ADR-0024) some messages are unrecoverably lost - a real,
already-accepted consequence of ADR-0024's design, not a bug this
benchmark should paper over by assuming it can't happen.

The subscriber instead reads with a per-message idle timeout (2s -
generous relative to the worst per-message latencies actually observed
even at 8 hops without any loss). Once nothing arrives for that long,
the burst is considered over, whether every message got there or not.
`BenchResult` reports `delivered` alongside `sent`, and throughput is
computed against `delivered`, not the nominal `MESSAGE_COUNT` - so a
run that loses messages under saturating load shows that honestly (a
delivered/sent ratio under 100%) rather than hanging forever or
silently overstating throughput. This is itself a useful data point
the sweep surfaces: not just how latency degrades with hop count, but
the hop count past which sustained unpaced load starts actually
losing messages.

A lossy row's throughput number is more than just lower, though - it's
not *comparable* to a lossless row's at all, and printing one invites
reading it as though it were. Measuring elapsed time against the last
message that actually arrived (necessary - see the last-delivery-vs-
idle-timeout note in the code) means a lossy run's window only covers
however far the pipeline got before giving up on the rest, not the
full transit a complete run's window covers; recovery replays are
fast, in-memory catch-up bursts with no per-message network pacing, so
that truncated window can look *faster* than a complete one despite
delivering less overall - not evidence the mesh is actually faster at
that hop count, just an artifact of comparing a partial window to a
full one. `bench_mesh` prints `n/a*` for `msgs/sec` whenever `delivered
< sent`, with a footnote, rather than a number that would otherwise
invite exactly that misreading.

### Where results live: printed to stdout, not CI-tracked

Wiring this into CI as a regression-tracked benchmark needs a stored
baseline, a comparison threshold, and a policy for what happens when a
run trips it - real infrastructure this project doesn't have anywhere
yet, for a project at this stage still validating basic feasibility
numbers, not defending an established performance budget. `bench_mesh`
prints a plain results table to stdout when run by hand; a captured
example run is recorded in `docs/OPERATIONS.md`, labeled as a
point-in-time, this-machine number rather than an authoritative
baseline. CI-tracked regression benchmarking is a reasonable future
issue if this project ever reaches a point where performance
regressions are a real risk worth automatically guarding against - not
built speculatively ahead of that need.

## Consequences

A new `examples/bench_mesh.rs` in `thoth-mesh-node`, no new workspace
dependency, no CI change. Running it is a manual, occasional step, not
part of the normal `cargo test`/`clippy` verification loop - consistent
with how `cargo run --release --example bench_mesh` sits outside that
loop the same way `cargo run` (the daemon binary itself) already does.

This closes #51.
