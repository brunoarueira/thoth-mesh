# 20. Peer-scoped topic restriction

## Status

Accepted

## Context

Issue #77 (Phase 7), split out of #62 / ADR-0018: today, once a peer
link is established (ADR-0017 gates *who* can become a peer at all),
it can propagate interest for and publish to *any* topic across the
mesh - there's no way to restrict what a given peer link is allowed
to carry.

ADR-0018's Context already flagged why this isn't just "reuse
`--topic-acl`": a peer link relays interest and publishes on behalf of
however many clients and further peers sit behind it elsewhere in the
mesh, not for itself. Restricting what topics may flow over a specific
link is a different question from restricting what one client may
do - but as worked out below, it turns out to be answerable with the
*same* underlying mechanism, aimed at a different enforcement point.

## Decision

### Reuse `TopicAcl`'s type, aimed at a new, independent `--peer-topic-acl` list

`Principal` (a TLS fingerprint, or `anonymous`), `Action` (`pub`,
`sub`, `pubsub`), and the `<principal>|<action>|<topic>` parsing
`TopicAcl` already provides (ADR-0018) are reused exactly as they
are - no new identity primitive, no new entry syntax. What's new is a
second, independent list: `--peer-topic-acl` (repeatable), parsed the
same way, but consulted at a different enforcement point and against
a different principal - the peer's own certificate fingerprint, not a
client's.

Peer links already share the client dispatch/enforcement choke point
(ADR-0010) - the same `Subscribe`/`Publish` arms in `connection.rs`.
ADR-0018 skips that check entirely once `peer_identity` is `Some`;
this ADR instead switches *which* ACL it consults there, rather than
adding a separate check elsewhere:

- `peer_identity.is_none()` (a client, or a connection not yet known
  to be a peer link) → `topic_acl`, unchanged from ADR-0018.
- `peer_identity.is_some()` (a peer link) → `peer_topic_acl`, new.

### What `Action` means for a peer link

This resolves the "needs to define what restricted means concretely"
question #77 left open:

- **`Subscribe`**: permits that peer link to be forwarded topic `T`
  from this node. Enforced against the peer's own inbound `Subscribe`
  for `T` - which is indistinguishable on the wire from a client's,
  whether it's the peer's own direct interest or interest it's
  relaying onward from further out in the mesh (ADR-0011's multi-hop
  propagation is itself just this same `Subscribe` arm, one hop over,
  so a denial composes across hops for free: a peer denied `T` can't
  register a forwarder for it, so it can never relay a downstream
  peer's interest in `T` to us either). Denied → no forwarder spawned
  on that link for `T`, the same consequence a denied client
  `Subscribe` already has.
- **`Publish`**: permits that peer link to send topic `T` *to* this
  node. Enforced against inbound `Publish` envelopes on that link,
  mirroring the client enforcement point exactly. Denied →
  `broker.publish` never runs, so `T` is neither delivered locally nor
  re-propagated onward - nothing extra is needed to stop propagation,
  since propagation only ever happens by publishing.

Together these answer all three shapes #77 called out: "interest this
node won't forward to that peer" is a denied `Subscribe`; "publishes
from that peer this node won't accept/re-propagate" is a denied
`Publish`; both is simply omitting a `pubsub` entry for that
`(principal, topic)`.

### No interaction needed with gossip catch-up (ADR-0011) or auto-dial (ADR-0015)

Enforcement is entirely local and receiver-side: each end of a link
independently judges the other's `Subscribe`/`Publish` against its own
configuration. This node's own outbound interest catch-up
(`register_peer_link`, sending our aggregate interest to a
newly-linked peer) is unchanged and unchecked here - it's a request
*to* the peer, which is the peer's own `peer_topic_acl` (on its side of
the same link) to honor or deny, the same way it judges a client's
`Subscribe` today. No coordination or wire-protocol change is needed,
and nothing beyond the existing certificate fingerprint is ever
exchanged about a peer's identity. Auto-dial and gossiped peer
discovery (ADR-0015) are unaffected - which peers exist and how to
reach them is orthogonal to what topics an already-established link
may carry.

### Principal: the same fingerprint identity as ADR-0017/ADR-0018

No new identity primitive. The `peer_fingerprint` already computed
once per connection (and already used for both `--allow-peer` and
`--topic-acl`) is reused unchanged. A peer connected without
presenting a certificate is `Principal::Anonymous`, the same
convention an anonymous client already gets - so a `--peer-topic-acl
anonymous|...` entry is meaningful even without TLS, consistent with
how `--topic-acl` already treats `anonymous`.

### Rejection: `MessageKind::Error`, connection stays open - same posture as ADR-0018

No new wire behavior. A denied peer `Subscribe`/`Publish` gets exactly
the `Error`-then-`continue` treatment a denied client one already
gets; the far end (itself a thoth-mesh-node, in the normal case)
already treats an unsolicited `Error` as a no-op (the existing
`Ack`/`Error` arm), so no protocol change is needed for a peer to
tolerate receiving one.

A new counter, `thothmesh_peer_topic_acl_rejections_total`, is added,
distinct from ADR-0018's `thothmesh_topic_acl_rejections_total` - so
an operator can tell a misbehaving/misconfigured peer apart from a
misbehaving/misconfigured client at a glance, without cross-referencing
logs.

### `NodeOptions`: replacing stacked positional `Option`s on `run_with_tls`/`serve_with_tls`/`spawn_with_tls`

ADR-0018 already flagged this exact moment: "worth a second look if a
*fourth* orthogonal knob shows up here; three positional `Option`s
stacked onto `run_with_tls` is still readable, a fifth wouldn't be."
`run_with_tls` is already at three (`tls`, `topic_acl`,
`metrics_token`, added across ADR-0016/ADR-0018/ADR-0019);
`peer_topic_acl` would make a fourth. Rather than push past the
threshold the prior ADR called out, this introduces:

```rust
pub struct NodeOptions {
    pub tls: Option<TlsConfig>,
    pub topic_acl: Option<TopicAcl>,
    pub peer_topic_acl: Option<TopicAcl>,
}
```

- `NodeOptions::default()` (all `None`) is exactly today's
  plaintext/unrestricted behavior - `run`/`serve`/`spawn` (the
  non-`_with_tls` entry points) pass it unchanged.
- `serve_with_tls`/`spawn_with_tls` take one `NodeOptions` in place of
  their two positional `Option`s.
- `run_with_tls` takes `NodeOptions` plus its existing, separate
  `metrics_addr`/`metrics_token` parameters. `metrics_token` stays
  *outside* `NodeOptions` deliberately: ADR-0019 already scoped it to
  `run_with_tls` alone (`metrics_server::serve_metrics` has no other
  caller), so folding it into a struct every `_with_tls` function
  receives would misrepresent it as something `serve_with_tls`/
  `spawn_with_tls` act on, when they don't bind a metrics port at all.
- `Shared` gains a matching `peer_topic_acl: Option<Arc<TopicAcl>>`
  field, threaded exactly like `topic_acl` (ADR-0018) and
  `allowed_peers` (ADR-0017) already are.

This is a one-time signature churn across every `_with_tls` call site
(this crate's own tests, `thoth-mesh-cli`'s tests, `main.rs`), but
stops the positional-`Option` growth pattern before a fifth knob would
have made it unreadable, per ADR-0018's own foresight.

## Consequences

`--peer-topic-acl` entries are exact-topic, not prefix/wildcard - the
same tradeoff `--topic-acl` and `--allow-peer` already accepted,
inherited here by reusing `TopicAcl` unchanged. Worth revisiting
together if any of the three grow unwieldy enough to justify a config
file (see docs/ROADMAP.md Phase 10).

`allowed_peers` (ADR-0017) still lives on `TlsConfig`, not
`NodeOptions` - it's TLS-specific (there's no meaningful allowlist
without a certificate to check it against), so folding it in would
blur `NodeOptions`' purpose rather than sharpen it. `NodeOptions` is
deliberately scoped to the three connection-dispatch-level knobs that
`serve_with_tls`/`spawn_with_tls`/`run_with_tls` all need identically.

A `sub`-only grant (no `pub`) for a peer link can still show an
occasional `Publish` rejection from that same peer under ordinary,
non-malicious operation - verified manually while testing this ADR.
ADR-0011's interest propagation echoes a `Subscribe` back to whichever
link's own `Subscribe` just moved local interest from 0 to 1
(broadcast goes to every active link, including the one that triggered
it); if the far end honors that echo (it has no `--peer-topic-acl` of
its own, or one that permits `sub`), it ends up with its own forwarder
relaying matching publishes back the way they came. A `sub`-only grant
correctly rejects that redundant hop rather than silently accepting
it - `Broker`'s `SeenIds` dedup (ADR-0011) would have dropped it too,
but the rejection is a more explicit signal, and it's what the ACL
entry's absence of `pub` actually means. Not a bug; worth knowing so
it isn't mistaken for one.

This closes #77 and, with it, the last item ADR-0018's Context left
open from #62's original scope (metrics-endpoint auth, the other
split-out half, closed separately by ADR-0019 / #75).
