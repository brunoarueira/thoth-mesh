# 26. Bounding concurrent outbound peer dials

## Status

Accepted

## Context

Issue #71: ADR-0015 (dynamic peer discovery via gossip) is proven
correct at small scale (a 3-node chain integration test) but has no
analysis of what happens as mesh size grows toward anything realistic.
The issue named three specific worries. Auditing the actual code
against each, rather than assuming, turned up that only one is real:

- **Redundant `PeerAnnounce` traffic** - already adequately bounded.
  `learn_peers` (`connection.rs`) batches every newly-learned peer
  from one incoming `PeerAnnounce` into a single re-broadcast to each
  active peer link, not one message per peer; `PeerDirectory::
  record`'s first-time-only gate means a given peer is only ever
  propagated once per node, no matter how many times it's mentioned
  again. Total propagation traffic during convergence is bounded by
  mesh edges, not exponential in mesh size. Nothing to change here.
- **Unbounded growth with node count** - already resolved, as a side
  effect of separate work: `PeerDirectory` (the "known-peers map" the
  issue named) is now capacity-bounded (ADR-0025), and the
  de-duplication cache already was (ADR-0011). Nothing to change here
  either.
- **Concurrent dial storms** - real, and unaddressed. `spawn_discovery_
  dialer` (`peering.rs`) spawns a fully independent `tokio::spawn`
  task per discovered address, with no concurrency limit anywhere in
  the path; `dial_peer` immediately attempts a TCP connect, optional
  TLS handshake, and protocol handshake with no throttling. Right
  after a fresh N-node bootstrap, or a network partition healing and
  re-announcing many peers at once, a node can find itself opening
  dozens or hundreds of concurrent outbound connection attempts at
  the same instant - real, scaling-with-mesh-size resource pressure
  (file descriptors, TLS handshake CPU) the tie-break in
  `we_should_dial` does nothing to prevent, since it only ever
  resolves *which side* of a single pair dials, never how many
  *different* pairs dial concurrently.

So the real scope is one change, not three.

## Decision

### Bound concurrent *connect-phase* dial attempts via a shared semaphore, not established-connection count

A `tokio::sync::Semaphore`, one per node, added to `Shared` (already
cloned into every dial task) and sized
`DEFAULT_MAX_CONCURRENT_DIALS` (16, not configurable via a flag in
v1, consistent with this codebase's other fixed capacities). `dial_
peer` acquires a permit before `TcpStream::connect`, and drops it
once the attempt has either failed or succeeded through to a
completed handshake - *before* handing off to `connection::handle_
connection`, which is what actually runs for the connection's entire
lifetime. The permit is held only for the connecting phase, never for
an established link's lifetime.

This is deliberate, not an oversight: gating on established-connection
count instead would mean a mesh that's grown past the cap and stayed
up could never dial *anyone* new again, an availability bug worse
than the storm this ADR fixes. The problem #71 describes is concurrent
*handshake attempts*, not steady-state connection count - which stays
genuinely unbounded on purpose, the same posture ADR-0025 already
took for currently-connected peers in `Membership`.

Applied uniformly to both `spawn_seed_peers` and `spawn_discovery_
dialer`'s dial tasks - both already funnel through the same `dial_
peer_with_reconnect`/`dial_peer` pair and the same `Shared`, so this
needs no special-casing between an operator's configured `--peer` list
and a gossip-discovered address, consistent with how the rest of this
codebase already treats the two identically.

A dial queued behind the semaphore isn't "retrying" yet - it simply
hasn't started its first attempt. `next_backoff`'s reconnect delay is
unaffected: it's still driven by how long an attempt that actually ran
took, not by queueing time.

### Verification: the worst case directly, not a separate benchmark harness

The issue's "Known shape" suggested a load-shaped test/bench spinning
up tens of nodes to observe actual behavior before deciding on a fix.
Building new observability tooling just to decide whether *this*
mitigation works is more machinery than the question needs: this ADR
instead adds one integration test that constructs the single most
dial-storm-prone bootstrap shape this codebase can produce directly -
N nodes (20), each configured via `--peer` with every *other* node's
address, all started at once. Zero gossip is even needed to trigger
every possible pairwise dial - every node already knows to dial
everyone the moment it starts. The test asserts the mesh still fully
converges (every node reachable from every other) within a generous
timeout, which only holds if dials queued behind the semaphore
actually get their turn rather than starving. A dedicated benchmark
harness with finer-grained numbers (exact `PeerAnnounce` counts, dial
latency distributions) is what #51 is already the right home for, if
that's ever actually needed - not duplicated here for a question this
test already answers.

## Consequences

A very large simultaneous bootstrap now converges somewhat slower
(dials queue behind 16 permits rather than all firing at once) - the
explicit tradeoff this ADR makes. Acceptable since bootstrap is a
one-time event, not a steady-state cost, and the alternative (letting
every dial fire at once) is exactly the resource-pressure problem
#71 raised.

No wire-protocol change, no new metric - `spawn_discovery_dialer`'s
existing `tracing::info!` on each dial attempt already gives an
operator visibility into dial activity; a queued-vs-running-attempt
gauge is easy to add later (following the same pattern ADR-0013/
ADR-0025 already established) if this proves to matter in practice,
not added speculatively here.

This closes #71.
