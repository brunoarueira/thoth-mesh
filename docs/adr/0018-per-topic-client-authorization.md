# 18. Per-topic client authorization via TLS certificate fingerprint principals

## Status

Accepted

## Context

Issue #62 (Phase 7): today any connected client can `Subscribe` or
`Publish` to any topic. ADR-0017 gates who gets to become a *peer
link* at all, but says nothing about what a client already inside the
mesh is allowed to do once connected.

The issue originally also bundled metrics-endpoint auth and
peer-scoped topic restriction under the same umbrella. Both are split
out:

- **Metrics-endpoint auth** is tracked separately (#75) - the metrics
  port (`--metrics-addr`, ADR-0013) has no TLS layer at all
  (ADR-0016 explicitly scoped TLS to the client/peer port only), so it
  needs its own mechanism, not whatever this ADR lands on.
- **Peer-scoped topic restriction** - limiting which topics a *peer
  link* may propagate/forward, as opposed to a client's own
  subscriptions - is a materially different problem. A peer link
  relays interest and publishes on behalf of potentially many clients
  elsewhere in the mesh; restricting it per-topic is closer to a
  mesh-wide policy question than a per-connection ACL check, and isn't
  resolved here. Left for a future issue if it turns out to be needed.

This ADR is scoped to **client-scoped per-topic publish/subscribe
authorization only.**

## Decision

### Principal: the same TLS fingerprint primitive as ADR-0017, or "anonymous"

A client connection's principal is the SHA-256 fingerprint of its TLS
client certificate, if it presented one - reusing
`thoth-mesh-tls::fingerprint` exactly as ADR-0017 does for peers,
rather than inventing a second identity mechanism. ADR-0016 already
makes a client certificate optional on the accept side, so a client
that doesn't present one (including every connection when TLS is off
entirely) has the principal `anonymous`.

`--topic-acl` is **not** wired to require TLS the way `--allow-peer`
is. An `anonymous`-only ACL (e.g. "only `status.public` is readable,
full stop") is a reasonable, self-contained use case that doesn't need
TLS to be meaningful. A fingerprint-based entry, meanwhile, simply
never matches on a connection that never presented a certificate -
same "no identity never satisfies a rule naming one" logic ADR-0017
already established for `--allow-peer`, reused here rather than
re-litigated.

### Enforcement: `--topic-acl <principal>|<action>|<topic>`, opt-in, default-deny once active, client connections only

`--topic-acl` (repeatable) on `thoth-mesh-node`, each entry shaped
`<principal>|<action>|<topic>`:

- `<principal>`: a fingerprint (accepting the same tolerant formats as
  `--allow-peer`) or the literal `anonymous`.
- `<action>`: `pub`, `sub`, or `pubsub` (shorthand for both).
- `<topic>`: an exact topic name - no wildcard/prefix matching, since
  nothing else in the protocol has that concept either (topics are
  exact-match per ADR-0006).

**Absent entirely, behavior is unchanged** - the same opt-in posture
every security feature here has had. Given at least once, enforcement
is **default-deny**: any `(principal, topic, action)` not explicitly
listed is refused, the same shape `--allow-peer` already established
(list what's allowed; everything else is refused once the feature is
on at all).

Enforcement lives in `connection.rs`'s existing
`MessageKind::Subscribe`/`Publish` handling - the same choke point
ADR-0017 already added a check to for `Hello`. It only applies while
`peer_identity` is still `None` on that connection - i.e. a connection
not (yet) known to be a peer link. For a dialed peer link,
`peer_identity` is set before the dispatch loop starts (the handshake
already completed - see ADR-0010), so this check never runs against
genuine peer traffic; for an accepted connection, it applies to
whatever Subscribe/Publish activity happens before (or absent) a
`Hello`, and stops applying the moment one is processed and accepted.
This is what "client connections only" means concretely: no new way
to distinguish a client from a peer is introduced, the existing
`peer_identity` tracking already *is* that distinction.

`Unsubscribe` is deliberately not checked: undoing a subscription the
client was never allowed to have in the first place is already a
no-op in the existing code (`forwarders.remove(topic)` only acts on a
match), so there's nothing an ACL check there would add.

### Rejection: `MessageKind::Error`, but the connection stays open

Unlike ADR-0017's peer-link rejection, a topic ACL violation does
**not** close the connection - a client denied on one topic may be
completely entitled to others, so severing the whole session over one
disallowed request would make the feature nearly unusable. Instead:

- A rejected `Subscribe` gets `Error { in_reply_to: Some(subscribe.id), .. }`
  instead of the usual `Ack` - no forwarder spawned, no interest
  registered.
- A rejected `Publish` gets `Error { in_reply_to: Some(publish.id), .. }`
  (there's no `Ack` for `Publish` to withhold instead) - the broker
  never sees it.

Both reuse `MessageKind::Error`, whose only other caller is
ADR-0017's peer rejection - but this is a genuinely different shape of
use (doesn't close the connection), which is exactly the kind of
divergence ADR-0017 flagged as worth checking rather than assuming
covered. It still fits: a topic ACL violation is just as much a
"common, recoverable mistake" (an operator forgot an entry, a client
tried the wrong topic) as a rejected peer link was.

A new counter, `thothmesh_topic_acl_rejections_total`, is added
alongside ADR-0013's existing three metrics.

## Consequences

Enabling enforcement costs one new repeatable flag and a `TopicAcl`
field threaded through `Shared`, alongside `run_with_tls`/
`serve_with_tls`/`spawn_with_tls` gaining one more `Option` parameter
- consistent with how ADR-0016 added `tls` onto ADR-0013's existing
signature. Worth a second look if a *fourth* orthogonal knob shows up
here; three positional `Option`s stacked onto `run_with_tls` is still
readable, a fifth wouldn't be.

`--topic-acl` entries are exact-topic, not prefix/wildcard - matching
today's exact-match subscription model, but meaning an ACL covering
many related topics needs one entry per topic. Acceptable at this
project's scale (same tradeoff ADR-0017 already accepted for
`--allow-peer`); worth revisiting together if either grows unwieldy
enough to justify a config file (see docs/ROADMAP.md Phase 10).

Peer-scoped topic restriction and metrics-endpoint auth remain open
(the latter tracked as #75) - this ADR closes #62's client-scoped
half only.
