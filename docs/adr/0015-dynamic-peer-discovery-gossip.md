# 15. Dynamic peer discovery via gossip

## Status

Accepted

## Context

Message *routing* is already multi-hop: interest propagation and
forwarding cross the whole mesh (ADR-0011), so a publish on one node
reaches a subscriber on any other node reachable through *some* path.
But *topology* isn't dynamic - a node only ever dials the exact
addresses passed via `--peer`, and never learns about a peer-of-a-peer.
Phase 6's goal (issue #39) is closing that gap: a node discovers and
dials peers it was never directly configured with, by learning about
them from the peers it is configured with. Issue #45 leaves four
questions open:

1. What gets exchanged?
2. When is it exchanged?
3. How are loops/unbounded growth prevented, the way ADR-0011 had to
   solve for interest propagation?
4. Is a discovered peer dialed automatically, or just surfaced for an
   operator to act on?

A fifth question surfaced while working through the fourth, not
present in the original issue: `thoth_mesh::Membership` and
`thoth_mesh_node::PeerLinks` are both keyed by `PeerId` and both
assume at most one active connection per peer at a time - reasonable
when the only way a peer link forms is one side dialing a
hand-configured address. Auto-dialing discovered peers makes it
realistic for *both* sides of a pair to independently decide to dial
*each other* at close to the same time (each having learned about the
other from a third peer). Two concurrent connections for the same
`PeerId` would race `Membership`/`PeerLinks`' last-write-wins registration
against whichever connection happens to close first calling
`mark_disconnected`/`unregister` - marking the peer wrongly disconnected
even though its other connection is still live. This isn't new
(nothing stops two operators from pointing `--peer` at each other
today), but gossip-driven auto-dial turns it from a theoretical
misconfiguration into an expected occurrence for any mutually-reachable
pair, so it needs an answer here.

## Decision

### What's exchanged: `(PeerId, listen_addr)` pairs, only for dialable peers

A new `MessageKind::PeerAnnounce { peers: Vec<PeerAdvert> }`, where
`PeerAdvert { peer_id: PeerId, listen_addr: String }` - the same shape
`Membership` already tracks per peer, minus the peers with no
`listen_addr`. A peer with no `listen_addr` only ever dials out and
can't be dialed back by anyone, including a third party who heard
about it - there's nothing useful to gossip about it, so it's filtered
out at the source rather than gossiped as an unusable entry.

### A new `thoth_mesh::PeerDirectory` tracks every peer ever learned about, and gates everything else

```rust
pub struct PeerDirectory { known: Arc<Mutex<HashMap<PeerId, String>>> }

impl PeerDirectory {
    /// Records `peer_id` as dialable at `listen_addr`. Returns `true`
    /// only the first time this `peer_id` is recorded - every later
    /// call just refreshes the address and returns `false`.
    pub fn record(&self, peer_id: PeerId, listen_addr: String) -> bool;

    /// Every peer currently known, except `exclude`.
    pub fn snapshot_excluding(&self, exclude: PeerId) -> Vec<(PeerId, String)>;
}
```

This is a structural mirror of ADR-0011's `Interest::subscribe`'s
boolean-on-first-transition contract, and it's what answers questions
2-4 together:

- **Loop prevention (question 3)**: exactly like `Interest`, a
  `PeerAnnounce` is only re-broadcast onward when `record` reports the
  peer as genuinely new to this node. A peer mentioned again after
  that - by the same neighbor or a different one, chasing the same
  flood-fill topology ADR-0011 already established for interest - is
  silently absorbed. No separate seen-set or TTL is needed.
- **When it's exchanged (question 2)**: twice, both reusing the exact
  points ADR-0011 already added for interest catch-up:
  - **On every peer link's handshake completing** (dial or accept
    side, same as `register_peer_link` already does for interest):
    the new peer itself is recorded (if it has a `listen_addr`), and
    the link is sent one catch-up `PeerAnnounce` containing
    `PeerDirectory::snapshot_excluding(the new peer)` - unlike
    interest's per-topic catch-up messages, this is naturally a single
    batched message, since peer entries don't each need their own
    forwarder task the way topics do.
  - **On every genuinely-new peer**, whether learned via a direct
    `Hello` or via a received `PeerAnnounce`: broadcast onward to
    every other active peer link, unconditionally, same
    "don't special-case the sender, idempotency already bounds it"
    reasoning ADR-0011 used for interest.
- **Auto-dial, not just surfaced (question 4)**: `record` returning
  `true` is also the dial trigger, with one restriction (see the next
  section for why): a peer *directly Hello'd* is never auto-dialed
  (we're already connected to it), only a peer learned about
  *indirectly*, via `PeerAnnounce`, is. Because `record` already
  returns `false` for a peer the direct-handshake path recorded first,
  a peer that's both a live direct link and separately gossiped about
  never gets a redundant second dial - the same idempotency gate
  answers "propagate again?" and "dial again?" for free.

This means the mesh actively converges toward more of its nodes
holding direct links to each other, not just staying minimally
connected - the intended effect of Phase 6, not a side effect to
guard against.

### Simultaneous mutual auto-dial: broken by ordering `PeerId`s, not by changing `Membership`/`PeerLinks`

When deciding whether to auto-dial a peer learned via `PeerAnnounce`,
a node only proceeds if its own `node_id` sorts less than the
discovered peer's `PeerId` (`PeerId` already derives `Ord` over its
underlying UUID). Both sides of a pair evaluate this independently, on
their own local information, with no coordination - and since UUIDs
are distinct, exactly one side's comparison holds. That side dials;
the other doesn't, and simply waits for the inbound connection.

This was chosen over changing `Membership`/`PeerLinks` to track
multiple simultaneous connections per `PeerId` (e.g. a connection
count, or keying by a per-connection token instead of `PeerId`)
because it's a smaller, narrowly-scoped change that provably prevents
the *common* case this ADR introduces - two nodes independently
auto-dialing each other after learning about each other from a third
party - without touching two already well-tested modules whose
existing single-connection assumption has otherwise held up fine.
It's deliberately scoped to auto-dial only; explicit `--peer`
configuration stays unconditional and untouched, so an operator who
points two nodes' `--peer` at each other still hits the pre-existing
theoretical race - unrelated to gossip, and no worse than it was
before this ADR.

### Delivery plumbing: an unbounded channel, not a direct call from `connection.rs`

`peering.rs` already depends on `connection.rs` (a completed dial
hands off to `handle_connection`); having `connection.rs` call back
into `peering.rs` to spawn a new dial would create a cycle. Instead,
`Shared` gains a `discovered_tx: mpsc::UnboundedSender<String>` -
`connection.rs` only ever pushes an address onto it, never spawns a
dial itself. A new `peering::spawn_discovery_dialer` owns the
receiving end, looping `dial_peer_with_reconnect` (unchanged, reused
verbatim) over whatever addresses arrive. `run`/`serve`/`spawn` spawn
this task the same way they already spawn `spawn_seed_peers`.

Plain `Shared::new` (used throughout existing tests that don't
exercise discovery) creates the channel and drops the receiving half
immediately - a discovery send in that context fails silently, same
as sending into any channel with no reader, which is correct for
those tests' scope. A new `Shared::new_with_discovery` returns the
receiver alongside `Shared`, for `run`/`serve`/`spawn` and any test
that wants to observe auto-dial behavior.

## Consequences

A node started with a single `--peer` into an existing mesh
eventually holds direct links to every other node reachable through
it, not just the one it was configured with - `--peer` becomes an
entry point into the mesh rather than the complete topology
definition. This is unconditional flood-fill, same as ADR-0011's
interest propagation, and the same bias applies: simplest mechanism
that's provably correct, not shortest-path or partial-mesh-aware.

`PeerDirectory` has no eviction and no cap - unlike `Broker`'s
`SeenIds` (ADR-0011), which bounds itself because it can grow forever
on live message traffic, `PeerDirectory`'s size is bounded by the
mesh's actual peer count, which isn't expected to be large yet.

Nothing validates a `PeerAdvert` beyond what `MessageKind`'s CBOR
shape already requires, and nothing authenticates who sent it -
consistent with the rest of the wire protocol today (see
`PROTOCOL.md`), but worth naming plainly here: a node with no trust
boundary yet (Phase 7) will attempt an outbound connection to any
address a peer - malicious or just buggy - claims belongs to some
`PeerId`. This is an accepted limitation, tracked by Phase 7's peer
authentication/authorization work, not solved here.

`Membership`/`PeerLinks` are unchanged - still keyed by `PeerId`,
still assuming one active connection per peer. The ordering rule
above keeps that assumption holding for gossip-driven auto-dial; it
was already only ever an assumption, not an enforced invariant, for
manually-configured `--peer` pairs.
