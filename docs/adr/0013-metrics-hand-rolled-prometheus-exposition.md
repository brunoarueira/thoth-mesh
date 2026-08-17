# 13. Metrics: hand-rolled Prometheus text exposition, opt-in port

## Status

Accepted

## Context

Issue #18 (Phase 4) names metrics as the second resilience/operability
item, with a "known shape" of three things an operator should be able
to see: how many peers are currently connected, message throughput,
and how far a slow subscriber's forwarder is falling behind. Issue #34
left two questions open:

1. What exactly gets exposed, and in what shape (raw counters an
   operator computes rates from, or something that's already a rate)?
2. How is it exposed - a metrics crate and wire format, and where it's
   served from?

## Decision

### Three metrics, all counters or a live-read gauge - no new library

- `thothmesh_peers_connected` (gauge): the number of currently-reachable
  peers. Not a tracked counter - read live from `Membership` at render
  time (a new `Membership::connected_count`), so there's no second
  piece of state that could drift out of sync with what `Membership`
  already knows.
- `thothmesh_messages_published_total` (counter): incremented once per
  *new* (non-duplicate) envelope in `Broker::publish` - the same choke
  point ADR-0011's dedup already lives in, so every publish from any
  source (client or peer, direct or forwarded) is counted exactly
  once, matching what dedup already treats as "one real publish."
  "Messages/sec" from the known shape is this counter, not a
  pre-computed rate: that's normal Prometheus practice (`rate()` at
  query time), and computing a rate here would mean picking a window
  and storing more state for no benefit over letting the scraper do
  it.
- `thothmesh_forwarder_lag_total` (counter): total envelopes skipped
  across every per-connection forwarder's `RecvError::Lagged`
  (`connection.rs`'s `spawn_forwarder`, which already logs this at
  `warn` and continues). Summed across all forwarders on this node,
  not broken out per topic or connection - the known shape asks for
  "broadcast lag" as a signal something's falling behind, not a
  per-topic breakdown; that's a reasonable future refinement if it's
  ever needed, not something to speculatively build now.

No metrics crate (`metrics`, `prometheus`, etc.) is added. Three
counters/gauges and a hand-written render function is a handful of
lines; pulling in a registry/facade crate (plus, for an HTTP exporter,
transitively an HTTP server library this workspace has avoided so far)
buys abstraction this project doesn't need yet, and cuts against its
established style of hand-rolling the wire protocol, framing, and
broker rather than reaching for an off-the-shelf equivalent (ADR-0005,
ADR-0006, ADR-0009).

### Exposed as Prometheus text exposition format, over an opt-in TCP port

The render function writes the three metrics above as plain
Prometheus text exposition (`# TYPE` line plus `name value` per
metric) - the de facto standard scrape format, so this plugs into
Prometheus (or anything else that speaks it, e.g. Grafana Agent)
without a translation layer, while still just being a `String` this
project's existing raw-`TcpListener` style can serve without a real
HTTP crate.

Serving it needs just enough HTTP to be scrapeable: a minimal
handler reads and discards the incoming request up to the blank line
that ends its headers (so well-behaved clients see a clean response,
not a reset connection), then always writes back `200 OK` with the
current render as the body, regardless of method or path. This is
deliberately not a general HTTP implementation - a metrics scrape
endpoint has exactly one thing to serve.

This only runs if a `--metrics-addr` flag is given; there's no default
port. Unlike `--peer`, which only adds outbound connections nothing
else depends on, opening a second listening port is a change to what
the process exposes on the network by default - opt-in keeps that
decision with whoever's deploying the node rather than this ADR.
Only `thoth_mesh_node::run` (`main.rs`'s entry point) grows this
parameter; `serve`/`spawn`, used by tests and `thoth-mesh-cli`'s
embedded node, are unaffected; wiring metrics serving into them too is
straightforward later if a caller other than the daemon binary needs
it.

### Where the state lives

`Broker` (already the natural single choke point for every publish,
per ADR-0011) grows its own `AtomicU64` and a `messages_published()`
accessor - no new type needed for one counter with no cross-cutting
concerns. `thoth-mesh-node` gets a small `Metrics` struct, following
the same cheap-clone-over-`Arc` pattern as `Membership`/`Interest`/
`PeerLinks`, holding just the forwarder-lag counter (the one metric
that isn't naturally owned by something that already exists) and
bundled into `Shared`. Nothing new needs threading through
`Shared::new`'s signature - `Metrics::default()` inside it, same as
every other field there.

## Consequences

An operator pointed at `--metrics-addr` gets a standard Prometheus
scrape target for free. Nothing changes for a node run without that
flag - no new listening port, no behavior difference, no new
dependency compiled in for a feature not in use.

This doesn't cover request auth or TLS on the metrics port - same
trust model as the main protocol port has today (network-level access
control is the operator's problem, not this node's). It also doesn't
add labels, histograms, or per-topic/per-peer breakdowns; if a real
need for those shows up, revisiting whether a proper metrics crate
earns its keep at that point is more honest than guessing now.
