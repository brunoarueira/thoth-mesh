# 40. Membership and loop-prevention trust only the authenticated identity

## Status

Accepted

## Context

Filed as #122 (Phase 12), the last piece: "loop-prevention,
membership, and interest-dedup (ADR-0011) currently key off a peer's
self-reported `PeerId` alone, with nothing stopping two distinct peers
from claiming the same one." The issue asked to review every place
`Hello`'s `peer_id` is used, once #120/#121 land, for an assumption
that no longer holds.

That review is this ADR. It found the gap already closed - #121
(ADR-0039) didn't just fix `Hello` in isolation, it corrected
`envelope.sender` once, in `run_connection`'s read loop, before any
dispatch at all. Every handler downstream - `handle_hello` included -
already only ever sees the authenticated value. There was no separate
"loop-prevention trusts the claim" code path left to fix; it was the
same code path #121 already covers, and #120/#121 are what actually
closes this issue, not any change this ADR makes.

## Decision

### No additional correction needed - trace every consumer to confirm it

Every place `Hello`'s (or any envelope's) sender ends up mattering for
membership/loop-prevention/interest-dedup:

- `ConnectionContext::peer_identity`, `Membership::mark_connected`/
  `mark_disconnected`, `PeerLinks::register`/`unregister` - all set
  from `envelope.sender` (in `handle_hello`) or `peer_id` (in
  `admit_initial_peer`), both already run through
  `authenticated_sender` (ADR-0039) before either function does
  anything else with the value.
- `register_peer_link`'s interest catch-up, and `propagate_interest`/
  `propagate_peer`'s onward broadcasts - all take `node_id`
  (this node's own already-correct identity, ADR-0038) or a
  `peer_id`/`filter` that traces back to the same corrected value.
- `we_should_dial`'s tie-break and `learn_peers`' auto-dial decision
  use a gossiped `PeerAdvert.peer_id` - genuinely still unverified
  (see below), but this only ever decides *whether to attempt a dial*,
  never who ends up registered in `Membership` once one happens: an
  auto-dial ends up at `connect_and_handshake` → `admit_initial_peer`,
  the exact same authenticated path any other dial goes through.
  Whatever answers gets registered under *its own* authenticated
  identity, never the gossiped claim.

Concretely, two peers can no longer collide on one `Membership`/
`PeerLinks` entry: `PeerLinks::register(peer_id, ...)` "replacing any
previous one for the same ID" is only reachable with an authenticated
`peer_id` - and the only way to produce a specific authenticated
`PeerId` is to hold the certificate (and private key) it derives from.
An impersonator without a real peer's private key can register only
under their own distinct identity, alongside the real peer's
untouched entry, never in place of it.

### `PeerDirectory`/gossip stays intentionally unverified - it isn't a trust boundary

A gossiped `PeerAdvert { peer_id, listen_addr }` is never checked
against anything, and this ADR doesn't add a check. `PeerDirectory`'s
own doc comment already states its scope: "Deliberately has no concept
of 'currently connected' - that's `Membership`'s job. This just
answers 'have I ever recorded this peer before.'" A poisoned advert
(a fake `peer_id` paired with a real, attacker-controlled address, or
vice versa) can at most cause a wasted dial attempt to that address -
whoever actually answers still only ever gets registered under their
own authenticated identity, by the same path an ordinary seed-peer
dial does (already covered by ADR-0039's dial-side test). Verifying
gossip transitively - proving a claim about a peer *not* directly
connected to this node - is a fundamentally different, harder problem
than authenticating a direct connection, and nothing about mesh
correctness depends on it: `Membership` (the actual source of truth
for "is this peer reachable") never reads `PeerDirectory` at all.

## Consequences

Closes #122 with no production code change - the fix already shipped
in #121; this is the audit #122 asked for, confirming it actually
covers what #122 was worried about. A new integration test
(`an_impersonation_attempt_never_collides_with_the_real_peers_membership_entry`)
makes the "two peers can't collide on one `PeerId`" property concrete:
a real peer and a simultaneously-connected impersonation attempt (its
own valid certificate, but a `Hello` claiming the real peer's
identity) end up with two independent, correct `Membership` entries -
neither displaces the other, and both continue delivering traffic
under their own identity.

Phase 12 (#119) is done: #120 made a cryptographic `PeerId` possible,
#121 made it authoritative over any claim, and #122 confirms that
authority actually reaches everywhere mesh correctness depends on it.
