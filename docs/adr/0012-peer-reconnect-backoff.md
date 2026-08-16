# 12. Reconnect with exponential backoff for dropped peer links

## Status

Accepted

## Context

ADR-0011 explicitly left this out of scope: today, `peering::dial_peer`
makes exactly one attempt per configured seed peer. If the connect
fails, the handshake fails, or an established link later drops for any
reason, `dial_peer` just returns and nothing dials that address again
for the rest of the node's life. A node that starts before its seed
peer does, or loses a link to a network blip, stays partitioned from
that peer forever rather than recovering once the peer is reachable
again.

Phase 4's goal is a mesh that survives real-world flakiness. Issue #18
names this as the first piece: reconnect/backoff for dropped peer
links. Two questions need answering:

1. Retry forever, or give up after some number of attempts? A seed
   peer is a fixed piece of node configuration, not a one-off dial -
   there's no natural point at which "stop trying" is correct instead
   of just prolonging a partition.
2. How fast to retry? Immediately after every failure risks hammering
   a peer that's down for a while (or that's still starting up) with a
   tight reconnect loop; always waiting the same fixed interval either
   wastes time recovering from a single transient blip or, if that
   interval is short, has the same hammering problem for a
   longer-lived outage.

## Decision

### Retry forever, with exponential backoff between attempts

`peering::spawn_seed_peers` wraps each seed peer's existing
single-attempt `dial_peer` in a new outer loop,
`dial_peer_with_reconnect`, that never gives up: connect fails,
handshake fails, or the link drops after running for a while, the loop
waits and tries the same address again, indefinitely. `dial_peer`
itself is unchanged - it still makes exactly one attempt and returns,
which keeps its existing single-attempt tests (connection refused,
handshake failure, etc.) valid without modification.

The wait between attempts starts at a short interval and doubles on
each consecutive attempt that doesn't result in a link staying up for
a meaningful amount of time, capped at a maximum:

- Initial backoff: 500ms.
- Doubles each unsuccessful attempt: 500ms, 1s, 2s, 4s, ... capped at
  30s.
- Reset back to 500ms once an attempt's connection stays up for at
  least 5 seconds - treated as evidence the peer is actually reachable
  now, not just that the handshake happened to complete right before
  an immediate drop.

The 5-second "was this attempt actually healthy" threshold is measured
by the wrapper, not `dial_peer` - it just times how long each call to
`dial_peer` takes to return (`dial_peer` doesn't return until the
handshake fails, or the connection it established closes). No new
signal needs to be threaded out of `dial_peer` for this: a link that
never came up returns almost immediately, and a link that comes up and
runs for a while before dropping takes at least that long to return,
so wall-clock elapsed time already tells the two cases apart.

### No jitter

Per-seed-peer reconnect loops aren't synchronized with each other and
this project has no scenario yet where many nodes would reconnect to
the same peer at the same moment (a mesh this size doesn't have a
"thundering herd" problem) - jitter would add complexity (and a new
dependency, since nothing in the workspace pulls in `rand` today) to
guard against a failure mode that doesn't apply here. Worth adding if
a future phase makes synchronized mass-reconnects realistic.

### No CLI-configurable backoff parameters, yet

The initial delay, cap, and multiplier are constants in `peering.rs`.
Making them configurable is straightforward if a real need shows up,
but nothing today asks for it - keeping them fixed avoids growing
`thoth-mesh-cli`'s flag surface for a knob nobody's asked to turn.

## Consequences

A node recovers from a lost peer link, or a seed peer that wasn't up
yet when this node started, without operator intervention - the mesh
self-heals across restarts and transient network issues. `dial_peer`
and `spawn_seed_peers`'s existing division of labor (one dials and
hands off once, the other fans out across seed peers) stays intact;
the reconnect loop is a new layer wrapping the former, not a rewrite
of either.

Backoff state (the current delay) lives only in the reconnect loop's
stack, one per seed peer - it resets to nothing meaningful on process
restart, which is fine since there's nothing worth persisting across
restarts anyway.

This still doesn't cover a peer disappearing from the *accept* side
(a node that dialed *us* going away) - that side has no address to
redial, by design; the disappeared peer's own reconnect loop (if it's
configured with us as a seed peer) is what re-establishes the link.
Nor does it change anything about interest catch-up: a link that drops
and reconnects still repeats its full catch-up handshake from scratch
(ADR-0011), it just now happens automatically instead of requiring a
node restart.

Metrics (the other half of issue #18) remain out of scope here and
untouched by this decision.
