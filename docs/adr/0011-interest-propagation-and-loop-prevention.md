# 11. Topic-interest propagation and loop prevention

## Status

Accepted

## Context

ADR-0010 made peer links full participants in local routing: either
side of a peer connection can `Subscribe`, `Unsubscribe`, or `Publish`
and have it handled exactly like a client. But nothing yet causes a
node to *originate* interest toward its peers - a peer link only
carries traffic for a topic once something (today, only a test
manually driving the wire protocol) sends `Subscribe` over it. Issue
#30's goal is the first real one: a publish on node A reaching a
subscriber connected to node B, without anyone doing that by hand.

Three design questions have to be answered before that's possible:

1. What counts as "this node is interested in a topic," and who gets
   told when that changes?
2. Given peer links share `connection.rs`'s dispatch loop
   (ADR-0010) and `Broker` re-publishes every incoming `Publish`
   locally, a connected mesh with a cycle can circulate the same
   envelope forever. Where does that get stopped?
3. `handle_connection`/`run_connection` and `dial_peer` already take
   five-plus parameters each (`Broker`, `Membership`, node identity,
   listen address, peer identity). This work adds two more
   (topic-interest state, a way to reach active peer links). Keep
   growing the parameter lists, or bundle them?

## Decision

### Interest is aggregated across every connection, not just clients

A new `thoth_mesh::Interest` registry counts, per topic, how many of
this node's connections - client *or* peer - currently want it
forwarded to them. This is deliberately not "only local clients count
as local interest": counting peer-originated `Subscribe`s the same as
client ones means a node that relays interest from one peer
automatically re-propagates that interest to its *other* peers too.
Three nodes in a line (A-B-C, no direct A-C link) get correct
multi-hop delivery for free, without a separate transitive-relay
mechanism - `Interest` plus the propagation rule below already behaves
like flood-fill gossip.

### Every transition broadcasts to every active peer link, unconditionally

When a topic goes from zero to one interested connection, its
`Subscribe` is sent to *every* currently active peer link - including,
when the transition was itself caused by a peer's `Subscribe`, back to
that same peer. This looks wasteful (why tell a peer about interest it
just told us about?) but is deliberately not special-cased, for two
reasons:

- The receiving side's existing per-connection forwarder map
  (`connection.rs`'s `forwarders: HashMap<Topic, JoinHandle<()>>`) is
  already idempotent - a repeat `Subscribe` for a topic that
  connection already forwards is a no-op, and per-connection
  idempotency is exactly what makes the flood-fill terminate. Every
  directed connection-leg in the mesh can trigger at most one real
  `Interest` transition per topic; once a leg has "seen" a topic, any
  further `Subscribe` over it is absorbed without re-propagating
  further. Excluding the originating peer from a given broadcast
  wouldn't change whether the flood terminates, just shave one
  redundant round-trip per edge.
- A peer link newly coming up gets caught up on the full current
  interest snapshot (see below) regardless, so there's no correctness
  gap to plug by being clever about exclusion.

`Unsubscribe` propagates the same way on a one-to-zero transition,
including when a connection (client or peer) disconnects while still
holding forwarders open - that's treated as an implicit unsubscribe
for each of its topics.

A newly-registered peer link (dial side: known before the dispatch
loop starts; accept side: known once its `Hello` arrives) is sent the
current `Interest` snapshot as a batch of `Subscribe` envelopes, so a
late-joining peer catches up instead of waiting for the next
transition. This reuses `Subscribe`/`Unsubscribe` - no new
`MessageKind`, consistent with ADR-0010.

Reaching "every active peer link" requires knowing what they are: a
new `thoth_mesh_node::PeerLinks` registry maps a connected peer's ID
to its connection's outgoing channel, kept in sync alongside
`Membership` (registered wherever `mark_connected` is called,
unregistered wherever `mark_disconnected` is). Delivery is best-effort
- `try_send`, skipping a link that's full or closed - the same
philosophy as the interest catch-up above: a badly-backed-up or
disconnecting link isn't worth blocking on, and it'll catch up from
scratch on its next connect.

### Loop prevention lives inside `Broker::publish`, not `connection.rs`

Every hop of a forwarded envelope keeps its original `MessageId`
(CBOR round-trips the `id` field unchanged), and every hop - the
original local publish included - already flows through
`Broker::publish`. That makes `Broker` itself, not
`thoth-mesh-node`'s per-connection code, the natural place to ask "has
this exact envelope already been published here before?": a bounded,
recently-seen set of `MessageId`s (a `HashSet` for lookup plus a
`VecDeque` recording insertion order, capped and evicting oldest-first
so memory doesn't grow without bound on a long-running node). A
duplicate is dropped - `publish` returns `0` delivered, same as
"nobody's subscribed," since from the caller's perspective both mean
"this call didn't reach anyone." Every call site keeps calling
`broker.publish(...)` exactly as before; the dedup is transparent.

This also needs no coordination with the interest-propagation flood
above: that flood is bounded by per-connection idempotency (previous
section), while envelope circulation is bounded by this dedup - they
solve different loop risks (repeated *control* messages vs. repeated
*data* messages) and don't need to share a mechanism.

### `thoth_mesh_node::Shared` bundles per-node services

`Broker`, `Membership`, the new `Interest`, and the new `PeerLinks`,
plus this node's own ID and advertised listen address, are now passed
around as one `#[derive(Clone)] struct Shared` instead of growing
`handle_connection`/`run_connection`/`dial_peer`'s parameter lists
further. `serve`/`spawn` construct one `Shared` and clone it into the
accept loop and every seed-peer dial, same as `Broker` alone was
threaded through before this issue.

## Consequences

A publish reaches every subscriber reachable through any path in the
mesh, not just directly-peered nodes - multi-hop forwarding falls out
of the interest-aggregation rule rather than needing its own
mechanism. A cyclic mesh no longer circulates messages forever.

Propagation is unconditional flood-fill, not shortest-path or
partial-mesh-aware - explicitly acceptable per issue #30's scope, and
consistent with this project's general bias toward the simplest
mechanism that's provably correct over a more efficient one.
Revisiting that trade-off, if the mesh grows large enough to matter,
is future work, not tracked yet.

Reconnect/backoff for a dropped peer link remains out of scope,
tracked under Phase 4 (issue #18) - a link that drops and reconnects
today simply repeats its catch-up handshake and interest snapshot from
scratch, which is correct but means a topic loses forwarding through
that link for the gap in between.
