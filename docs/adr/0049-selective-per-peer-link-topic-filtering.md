# 49. Selective per-peer-link topic filtering

## Status

Accepted

## Context

Filed as #137 (Phase 15). A peer link today forwards whatever this
node's aggregate subscriber interest already is (ADR-0011): once any
local connection (client or another peer) makes this node interested
in a filter, that interest is proactively announced to *every* active
peer link, unconditionally. Real federation topologies often want more
deliberate control - "only ever relay topics matching X to peer Y,"
independent of what interest this node happens to have accumulated for
other reasons.

## Decision

### Scope: gates proactive announcement only, not an explicit request

`--peer-topic-filter` restricts what this node *proactively tells* a
specific peer link about via interest propagation
([`register_peer_link`'s catch-up
loop](../../crates/thoth-mesh-node/src/connection.rs) and
[`propagate_interest`](../../crates/thoth-mesh-node/src/connection.rs))
- it does not additionally restrict an *explicit* `Subscribe` that
peer sends this node itself. That remains exactly `--peer-topic-acl`'s
job (ADR-0020), unchanged. A peer can still explicitly ask for, and
receive, anything `--peer-topic-acl` already permits, even a topic
this node would never have volunteered unasked.

This keeps the two mechanisms cleanly separated by what question each
answers - `--peer-topic-acl`: "is this peer *permitted* to
publish/subscribe to this topic if it asks?"; `--peer-topic-filter`:
"does this node *proactively announce* its own interest in this topic
to this peer?" - rather than one flag trying to answer both, and
avoids doubling the integration surface (checked only where interest
propagation already happens, not also threaded into
`handle_subscribe`'s request-handling path).

### Literal topics only, reusing `TopicAcl`'s conservative wildcard stance

An entry is `<fingerprint>|<topic>` - a literal `Topic`, not a
`TopicFilter`. `Interest` is keyed on `TopicFilter` and can itself hold
a wildcard pattern (ADR-0022); rather than invent pattern-vs-pattern
matching (which topic does a configured `weather.+` filter-of-filters
"cover"?) - a kind of logic this codebase has deliberately avoided
everywhere else ACLs meet wildcards - a wildcard filter is simply never
relayed to any peer link a `--peer-topic-filter` applies to at all,
once that peer has *any* entries. The same conservative default
`filter_acl_permits` already established for `--topic-acl`/
`--peer-topic-acl` (a wildcard `Subscribe` is refused outright wherever
either ACL is configured, regardless of what it would expand to).
Watching several concrete topics through a filtered link just means
listing each one.

### A new, purpose-built type - not `TopicAcl` reused with a new `Action`

A new `PeerTopicFilter` (`peer_topic_filter.rs`), structurally close to
`TopicAcl` (`HashSet<(Principal, Topic)>`, same `Principal`/fingerprint
parsing reused directly) but with no `Action` dimension at all - relay
is the only thing an entry ever grants, so `<fingerprint>|<topic>` (two
fields) rather than forcing every entry to spell out a no-op third
field (`<fingerprint>|relay|<topic>`) the way reusing `TopicAcl` with a
new `Action::Relay` variant would. Keeping it a distinct type also
keeps the two flags' *meanings* visibly distinct in code, not just in
the CLI - `peer_topic_acl.permits(principal, topic, Action::Subscribe)`
answering a materially different question than
`peer_topic_filter.permits(principal, topic)` reads oddly if they're
the same type doing double duty. Same "default-deny once configured
at all" semantics as `TopicAcl`: once `--peer-topic-filter` has *any*
entries, every peer link's proactive relay is restricted to exactly
what's listed for it - a peer link with no entries of its own gets
nothing proactively announced, not silently exempted. `None` (the
default, nothing configured) is unchanged behavior: every peer link
gets everything, as before this ADR.

### `PeerLinks` learns each link's identity, not just its channel

`PeerLinks` (`peer_links.rs`) already centrally tracks every active
peer link's outgoing channel (ADR-0011) - it now also stores each
link's `Principal` (the same fingerprint-or-anonymous identity
`--peer-topic-acl`/`--allow-peer` already key on), set once at
`register` time from the already-authenticated identity
`ConnectionContext` computes for every connection (ADR-0038/ADR-0039).
This is what makes filtering possible at `propagate_interest`'s single
broadcast site: a new `PeerLinks::broadcast_interest(envelope, filter,
peer_topic_filter)` checks each registered link's own `Principal`
against `peer_topic_filter` before sending, falling straight through to
the existing unfiltered `broadcast` when no `--peer-topic-filter` is
configured at all. `register_peer_link`'s own catch-up loop (a new
link's first payload - every currently-interested filter, sent
directly rather than through `PeerLinks`) applies the identical check
inline, once, for the one peer link it's populating.

`PeerLinks::broadcast` itself (`PeerAnnounce`/peer-discovery gossip,
ADR-0015) is untouched - gossiping about *other peers existing* is an
orthogonal concern from topic relay, not something `--peer-topic-filter`
has any reason to gate.

## Consequences

- New CLI flag `--peer-topic-filter <fingerprint>|<topic>` (repeatable).
  Independent of `--peer-topic-acl`/`--allow-peer` - none of the three
  require each other.
- New `thoth-mesh-node::peer_topic_filter` module: `PeerTopicFilter`,
  reusing `topic_acl::Principal`/its fingerprint parsing.
- `PeerLinks::register` gains a `Principal` parameter; its map's value
  becomes `(Sender, Principal)`. New `PeerLinks::broadcast_interest`,
  alongside the existing, still-unfiltered `broadcast`.
- `register_peer_link`/`propagate_interest` (`connection.rs`) each
  gain a `peer_topic_filter: Option<&PeerTopicFilter>` parameter and
  the newly-joining/target peer's own `Principal`.
- `NodeOptions` gains `peer_topic_filter: Option<PeerTopicFilter>`.
- An explicit `Subscribe` a peer sends this node is entirely
  unaffected - still governed only by `--peer-topic-acl`, as today.
  Layering `--peer-topic-filter` onto that request-handling path too
  is explicitly out of scope here; revisit only if real usage shows
  proactive-announcement-only isn't enough.
