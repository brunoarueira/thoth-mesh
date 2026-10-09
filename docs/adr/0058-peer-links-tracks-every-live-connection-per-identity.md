# 58. `PeerLinks` tracks every live connection per identity, not just one

## Status

Accepted

## Context

Issue #194, filed against ADR-0057 part 1 (PR #192)'s own review:
`PeerLinks::register` keys its registry by `PeerId`, a plain
`HashMap::insert` that replaces whatever `Link` (sender, principal,
and the `disconnect: Arc<Notify>` handle ADR-0057 added) was
previously registered for that ID. If two connections for the same
peer ever briefly overlap, only the newer one's `disconnect` handle
stays reachable - the older entry simply vanishes from the registry.
Nothing scanning it (`disconnect_unless`, specifically - an
`--allow-peer` reload's own revocation sweep) can ever reach that
older connection again, even if it's the one actually still carrying
traffic and in violation of a freshly-tightened allowlist.

Two connections can overlap for the same peer because both sides of a
pair dial each other at the same moment - not a bug, a real race any
bidirectional link can hit. ADR-0015 already names this exact
scenario ("Simultaneous mutual auto-dial") for *gossip*-discovered
peers, and already fixes it there: before dialing a peer learned via
`PeerAnnounce`, a node checks `we_should_dial(node_id, peer_id)` (a
`PeerId` comparison both sides evaluate independently, with no
coordination) and only dials if it holds - the other side simply
waits for the inbound connection. That ADR explicitly scoped the fix
to auto-dial only: "an operator who points two nodes' `--peer` at
each other still hits the pre-existing theoretical race - unrelated
to gossip, and no worse than it was before this ADR." The reason it
couldn't cover explicit `--peer` too: `we_should_dial` needs the
peer's `PeerId` *before* deciding whether to dial, and gossip hands
that over via `PeerAnnounce` ahead of time - an explicit `--peer
<address>` entry is just a hostname/port; nobody knows the peer's
identity until a handshake, dial or accept, actually completes. There
is no "don't bother dialing" check to add on that path.

PR #192 tried fixing #194's actual registry gap directly: firing the
superseded entry's own `disconnect` right there in `register`,
tearing the old connection down whenever a new one replaces it.
Reverted - verified as an active regression, not just incomplete. The
20-node full-mesh bootstrap integration test (`tests/integration.rs`)
configures every node's `--peer` to dial every other node directly,
specifically to exercise ADR-0026's dial-concurrency bound - every
pair dials each other, mutually, right from startup. With the "kill
on supersede" fix in place, each side's own completing dial
immediately killed the other side's completing dial in response, back
and forth, and the mesh never converged.

## Decision

`PeerLinks` stops assuming one connection per identity at all.
`links: HashMap<PeerId, Link>` becomes `HashMap<PeerId, Vec<Link>>` -
`register` pushes onto a peer's list rather than overwriting it, and
`unregister` removes only the one entry whose sender matches (by
`same_channel`, same check as before), leaving the rest - including
removing the peer's entry from the outer map entirely once its list
empties, so a peer with no live connections leaves no trace, same as
today.

Every consumer - `broadcast`, `broadcast_interest`, `disconnect_unless`,
`reconcile_interest` (ADR-0057) - already only ever iterates the
registry broadly (send to everyone, or everyone a predicate accepts);
none of them do a single-entry lookup keyed by identity for some other
purpose. Changing what they iterate *over* (every link across every
identity, rather than one link per identity) is the entire change -
none of their own logic needs to know or care how many connections a
given peer happens to have right now.

This directly closes #194: `disconnect_unless` now reaches *every*
currently-registered connection for a revoked peer, not whichever one
happened to still be in the map. No "is this one stale or legitimate"
judgment call is needed at all - every registered entry is, by
construction, a connection that hasn't unregistered itself yet,
i.e. one that's actually still live.

### `register` returns a `Drop` guard, so an aborted connection still unregisters

`PeerLinks::unregister` is only ever called from `connection.rs`'s
`shut_down()`, which runs explicitly near the end of `run_connection` -
not from a `Drop` impl. A connection whose task is aborted outright
(ADR-0028's chaos tests, via `Node::accepted_connections`) skips
everything after the abort point, `shut_down()` included, so
`unregister` never runs for it.

Today that's invisible: the next `register()` for the same `PeerId`
overwrites the stale entry outright, so a leaked one is silently
discarded, capped at one per peer forever regardless of how many times
this happens. Switching to `Vec<Link>` turns that same gap into a real
one: `register` now *appends* rather than overwrites, so a peer
aborted-and-reconnected repeatedly (a long-running node under
chaos-like conditions) would accumulate one stale, never-removed entry
per cycle - unbounded growth where none existed before, caused
entirely by this ADR's own change from overwrite to append semantics.

Fixed the same way `ConnectionRegistry` fixed the identical problem in
ADR-0057 part 2: `register` returns a `PeerLinkRegistration` guard
(mirroring `ConnectionRegistration`) whose `Drop` calls `unregister` -
constructed once, in `register_peer_link`, and held in a new
`ConnectionContext` field (`Option`, since not every connection
becomes a peer) alongside the existing `_registration`. Guarantees
cleanup on every path a connection's task can end, abort included,
closing the growth risk at its root instead of capping the `Vec`'s
length as a band-aid - and as a side effect, fixes the exact same
dormant leak in *today's* code, invisible there only because overwrite
semantics happened to mask its consequence. `shut_down()`'s own
explicit `peer_links.unregister` call is removed as redundant, same
treatment `ConnectionRegistry`'s equivalent explicit call got once its
own guard existed.

### Alternative considered: a `PeerId`-ordering tie-break, like ADR-0015's, applied after the fact

Since explicit `--peer` can't skip a redundant dial *before* it
happens (no identity to compare yet), a tempting variant was to apply
`we_should_dial`'s same comparison *after* a new connection's identity
becomes known: if a newly-registering connection is the "wrong
direction" for this `node_id`/`peer_id` pair (e.g. we dialed, but our
`node_id` is the larger one, so the peer should have dialed us
instead), reject it outright instead of registering it; if it's the
"right direction," register it and tear down whatever's already there
for that peer, trusting the comparison to guarantee both sides
converge on the same single surviving connection without ping-ponging
each other's new connections to death.

Rejected: the dial that's permanently the "wrong direction" for a
given pair doesn't stop being configured just because one attempt got
rejected. `dial_peer_with_reconnect` doesn't know *why* a connection
it just established got closed - it just sees the link drop and
retries with backoff, forever, the exact shape of waste issue #193
already describes for a different cause. Every bidirectional `--peer`
pair would permanently carry one side's connection attempts never
succeeding, capped at a 30-second retry forever, for no operational
benefit over just letting both connections stand. Tracking every live
connection avoids manufacturing a second, permanent instance of #193
to fix #194.

## Consequences

- `PeerLinks`'s internal map changes shape
  (`HashMap<PeerId, Link>` -> `HashMap<PeerId, Vec<Link>>`);
  `broadcast`/`broadcast_interest`/`disconnect_unless`/
  `reconcile_interest` are otherwise unchanged. `register` now returns
  a `PeerLinkRegistration` guard instead of nothing, and the explicit
  `unregister` call `shut_down()` used to make is gone - cleanup is
  the guard's job now, on every path, not just the ones that reach
  that explicit call.
- A peer with two simultaneous connections gets interest-propagation
  and gossip messages twice, once per connection - already tolerated,
  not new: both are idempotent from the receiving side (ADR-0011,
  ADR-0057), and `PeerDirectory::record`'s own idempotency gate
  already denies crediting a redundant connection as a reason to
  gossip-dial again.
- Actual message delivery doesn't duplicate: a forwarder is set up
  per explicit `Subscribe` request on the specific connection it
  arrived on, not per registered identity - two connections to the
  same peer only ever deliver a topic twice if that peer itself
  explicitly subscribed on both, which is its own choice to make, not
  this node's.
- `Membership` itself still assumes one connection per peer -
  `mark_disconnected(peer_id)` flips a peer straight to unreachable
  the moment *any* one of its connections ends, even if another is
  still live, and nothing re-affirms reachability until the next
  fresh `mark_connected`. Pre-existing (not introduced here -
  `Membership` has never consulted `PeerLinks` and today's silent
  overwrite doesn't prevent the loser's own eventual, ordinary
  disconnect from doing exactly this already), but worth noting since
  this ADR makes a lasting overlap a normal, expected outcome rather
  than a fleeting one. Out of scope for #194 - `is_reachable`
  reference-counting connections, not just connections-for-revocation,
  is its own, separate change. Filed as issue #198.
- No mechanism converges two simultaneous connections for the same
  peer back down to one - they simply coexist for as long as both
  stay up. Revisiting that (if duplicate connections turn out to be
  common or costly enough in practice to be worth optimizing away) is
  future work, not part of closing #194.
- Closes #194.
