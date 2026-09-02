# 28. Chaos/partition test coverage for reconnect, dedup, and loop prevention

## Status

Accepted

## Context

Issue #52: ADR-0012's reconnect-with-backoff and ADR-0011's loop
prevention/interest-dedup are each proven correct today by exactly one
lightweight integration test -
`peer_becomes_unreachable_once_its_connection_drops` (severs a link,
checks membership notices) and
`loop_prevention_stops_a_publish_from_bouncing_forever` (a two-node
cycle, checks a publish isn't delivered twice) respectively. Neither
comes close to the conditions a real long-running mesh eventually
hits: a node dying and coming back as a fresh identity mid-mesh, a
partition healing rather than just occurring, or a peer that was
*announced* via gossip (ADR-0015) but was never actually reachable by
the time this node tries to dial it.

Three scenarios, matching the issue's own breakdown:

1. **Node death and restart.** A peer disappears and later comes back
   - as a *new* identity, since `PeerId` is a fresh random value per
     process start (`PROTOCOL.md`, also the basis for ADR-0025's
     eviction caps). Reconnect must not get permanently stuck waiting
     for an identity that's never coming back, and once the new
     identity is reachable, delivery must resume cleanly - no
     duplicates left over from before the drop.
2. **Partition heals.** A link goes down and later comes back up
   *between the same two identities* (unlike scenario 1) - membership
   must converge back to the same view on both sides, not get stuck
   believing the other side is still gone.
3. **A gossip-discovered peer that's down.** `PeerAnnounce` (ADR-0015)
   tells a node about a peer that was up when the *announcer* learned
   it, but may be down, unreachable, or gone by the time this node
   tries to dial it. Auto-dial reuses `dial_peer_with_reconnect`
   verbatim, so this should already degrade to the same backoff/retry
   a configured `--peer` that never comes up gets - but that's an
   assumption from code reuse, not something tested. Also worth
   proving: the peer comes up later and the retry actually succeeds.

## Decision

### In-process chaos, not an external harness

Every scenario is driven by spawning, killing, and restarting real
`tokio` tasks and sockets within a single test binary - the same shape
`thoth_mesh_node::spawn`'s `Node { accept_loop, peer_dials, .. }`
handles already support (`peer_becomes_unreachable_once_its_connection_
drops` already aborts a `peer_dials` entry to simulate a dropped
peer). An external harness spawning and killing real separate
processes would exercise the same code paths but add process
orchestration this project doesn't otherwise need - the existing
integration-test style already reaches the code that matters (`dial_
peer`, `Membership`, `Broker::publish`'s dedup) without it.

"Killing" a node means aborting its `accept_loop` and every `peer_
dials` handle, which drops (and so closes) the listening socket - a
*listening* socket carries no `TIME_WAIT` state of its own (that only
applies to individual established connections the local side closed),
so a fresh `TcpListener` can rebind the identical address immediately
afterward with no artificial delay needed to dodge port reuse
flakiness. "Restarting" means spawning a brand new `Node` on that
freshly rebound listener - a new `PeerId` by construction, exactly
mirroring a real process restart.

### One new test per scenario, added as separate sequential PRs

Each scenario above becomes its own integration test in a new `tests/
chaos.rs` (parallel to how `tls.rs`/`topic_acl.rs` already split off
from `integration.rs` by concern), landing as its own PR - consistent
with this project's general preference for several small, reviewable
PRs over one large one, and letting each scenario's test infrastructure
(killing/restarting, severing/restoring, dialing a never-up address)
be reviewed on its own rather than all three arriving as one large
diff.

### What "holds up" means, concretely, per scenario

- **Node death and restart**: after the restart, `Membership` reports
  the *old* identity unreachable and the *new* identity reachable, and
  exactly one delivery is observed for a publish made after
  reconvergence (proving no leftover duplicate from the drop, and no
  permanently-stuck backoff - if it were stuck, reconvergence itself
  would never happen and the test would time out).
- **Partition heals**: after severing and restoring the same link,
  both sides' `Membership` report each other reachable again, matching
  the view from before the partition.
- **Gossip-discovered peer that's down**: after an address is gossiped
  for a peer that never accepts a connection, the discovering node's
  backoff loop keeps retrying rather than giving up (proven by later
  binding a real listener at that exact address and observing
  convergence) - the same behavior `dial_peer_with_reconnect_retries_
  until_the_seed_peer_comes_up` already proves for a configured `--
  peer`, just reached via gossip discovery instead.

No production code is expected to change for this issue - the point is
proving what ADR-0011/ADR-0012/ADR-0015 already built holds up under
these conditions. If a scenario's test reveals an actual gap, that
becomes its own follow-up issue/ADR (the same pattern #71's audit and
#98 already established), rather than silently expanding this ADR's
scope.

## Consequences

Three new integration tests, each slower and more involved than this
codebase's typical test (multiple real nodes, sockets bound and
rebound, deliberate delays for backoff to play out) - acceptable,
since proving these properties needs real concurrent tasks and real
sockets, not something a unit test can substitute for. No metric, no
wire-protocol change.

This closes #52.
