# 37. `thoth-mesh-cli` status command

## Status

Accepted

## Context

An operator can only see a node's connected peers and activity by
scraping the opt-in Prometheus endpoint (`--metrics-addr`, ADR-0013)
or reading logs - there's no `thoth-mesh status`. Filed as #57 (Phase
11, docs/ROADMAP.md), which named two real options: (a) a new admin
request/reply pair over the existing wire protocol, or (b) the CLI
becomes a thin HTTP client for `--metrics-addr` and parses the
Prometheus text format back out.

(b) doesn't actually cover the ask. `--metrics-addr` is opt-in
(ADR-0013) - a status command that only works when a second port
happens to be enabled isn't a general admin command. More
fundamentally, the Prometheus endpoint only ever exposes
`thothmesh_peers_connected`, a *count* - not one specific peer's
identity or address, since a scalar counter has nowhere to put a list.
"See connected peers" (this issue's own goal) is impossible to build
on (b) at all, not just inconvenient.

## Decision

### A `StatusRequest`/`StatusReply` message pair, like everything else

`thoth-mesh status` sends a `StatusRequest` over the same connection
every other command already dials, and prints the `StatusReply` it
gets back. Same envelope/framing as `Publish`/`Subscribe`/etc - no new
port, no new transport, and it works identically whether or not
`--metrics-addr` is enabled on the node at all, over TLS or plaintext.

`StatusReply` carries:

- `node_id`, `listen_addr`: this node's own identity and (if any) the
  address it advertises to peers.
- `peers: Vec<PeerSummary>` (`peer_id`, `listen_addr`): every peer
  **currently connected**, sorted by `peer_id`. Deliberately not
  `Membership`'s full history, including disconnected entries capped
  at `DEFAULT_MEMBERSHIP_DISCONNECTED_CAPACITY` (ADR-0025) - the issue
  asks to "see connected peers," and thousands of stale disconnected
  entries by default would bury that under noise nobody asked for. A
  flag to include disconnected history is a natural, additive follow-up
  if it turns out to be wanted, not a redesign of this shape.
- `metrics: MetricsSummary`: every counter `render_prometheus` already
  reports, as typed `u64` fields instead of Prometheus text - see
  below.

Both new types (`PeerSummary`, `MetricsSummary`) live in
`thoth-mesh-core` next to `PeerAdvert` (`PeerAnnounce`'s payload type,
the closest existing precedent) - they're wire-protocol payload
shapes, not node-internal state, even though thoth-mesh-node is what
computes them.

### One status computation, two renderers

`thoth-mesh-node::metrics` gains `summary(membership, broker, discover,
metrics) -> thoth_mesh_core::MetricsSummary`, and `render_prometheus`
is rewritten to call it and format its fields as text, rather than
reading each counter itself. `StatusRequest`'s handler calls the same
`summary()` to build its reply's `metrics` field. One function computes
every number; Prometheus text and a `StatusReply` are just two ways of
presenting the same snapshot, which is also what keeps them from
silently drifting apart as new counters are added later.

### No new authorization

A `StatusRequest` is answered on any connection - client or peer link,
regardless of `--topic-acl`/`--peer-topic-acl` (ADR-0018/ADR-0020),
with no new flag gating it. The information involved is no more
sensitive than what's already available to anyone able to open a
connection at all (existing peer/topic behavior, the same connected-peer
count already in unauthenticated Prometheus text by default) - adding
a permission model for this one command, ahead of any real need for
one, isn't this ADR's problem to invent. `--metrics-token-file`
(ADR-0019) remains specific to the HTTP metrics endpoint; it has no
bearing on the client wire protocol and this doesn't change that.

## Consequences

`thoth-mesh status` works against any running node, with or without
`--metrics-addr`, and reports live peer identities `--metrics-addr`
never could. No new dependency - reuses the existing envelope/framing
and connection code path. `MessageKind` gains two variants, so the one
non-wildcard match over it
(`thoth-mesh-node::connection::run_connection`'s dispatch loop) gets a
new arm; every other `match ... MessageKind` in the codebase already
has a wildcard arm and needed no change.

Closes #57.
