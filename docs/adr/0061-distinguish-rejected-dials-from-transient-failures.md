# 61. Distinguish an explicit rejection from a transient dial failure

## Status

Accepted

## Context

Issue #193, noticed while verifying #192 (ADR-0057 part 1): when a
dialing node's handshake is rejected - today, only an `--allow-peer`
allowlist rejection (ADR-0017) can do this - `dial_peer_with_reconnect`
(ADR-0012) treats it exactly like any other failure. It backs off
exponentially (500ms doubling to a 30s cap) and retries indefinitely,
never inspecting *why* the attempt failed. A transient network blip
and a deliberate, persistent policy rejection look identical to this
loop, so a rejected seed peer gets hammered every few seconds
indefinitely - useless, since only an operator fixing the allowlist
(then reloading, ADR-0057) can ever change the outcome.

This applies to both a configured `--peer` (seed peer) and a
gossip-discovered one (ADR-0015) - `spawn_discovery_dialer` wraps both
in the same retry loop.

### Why this is worth doing *before* Phase 18 (protocol version negotiation)

Issue #149 (Phase 18) wants "a clear, specific error" when a decoded
envelope's version isn't understood. For a peer connection, that error
would flow through this exact same handshake-rejection path - a second
cause of the identical problem this ADR fixes, landing on top of an
already-unfixed one if this is deferred. Classifying *how* a dial
attempt failed, not just whether it did, is the reusable piece #149
will also need; doing it now means #149 has an existing mechanism to
extend rather than inventing its own and reconciling the two later.

## Decision

### `HandshakeError` gains a `Rejected(String)` variant, distinct from any other unexpected reply

`recv_hello` (`thoth-mesh::handshake`) already turns anything other
than a `Hello` reply into `HandshakeError::UnexpectedMessage`. An
`Error` reply specifically - the one `MessageKind` an accepting node
already sends to signal "I understood your `Hello` perfectly well,
and I'm deliberately refusing it" (ADR-0017's allowlist rejection
today; #149's future version-mismatch rejection, reusing the same
mechanism, tomorrow) - gets its own `HandshakeError::Rejected(String)`
carrying the rejection's own `message`. Everything else (a `Subscribe`,
a `StatusReply`, anything else that was never meant as a handshake
reply at all) stays `UnexpectedMessage` - that really is just
"confusing," not a deliberate decision by the far end.

### `dial_peer`'s one attempt now reports *how* it failed, not just whether

`dial_peer`/`dial_peer_with_timeout` changed from returning `()` to a
new `DialOutcome` (`Established`, `Rejected`, or `Other`), threaded up
from a new `DialFailure` (`Rejected`/`Other`) `connect_and_handshake`
now returns instead of `Option`. `dial_peer_with_reconnect`'s own
"was this attempt healthy" signal - `attempt_start.elapsed()`, timed
by the wrapper per ADR-0012, unchanged - still decides whether a
*successful* connection counts as healthy; `DialOutcome` is the
orthogonal signal for *why a failed attempt failed*, not a replacement
for the uptime check.

### A rejection gets a long, fixed backoff - not the normal exponential ramp

`next_backoff` gains a third parameter, the `DialOutcome` of the
attempt that just ended, and a new branch: a `Rejected` outcome always
waits `REJECTED_BACKOFF` (2 minutes) before the next attempt,
regardless of `previous` - no doubling, no cap computation, the same
fixed value every consecutive rejection. Two things this ADR
explicitly decided *against*, and why:

- **Giving up entirely** after a rejection would be a real regression,
  not just a missed improvement: seed peers aren't reloadable
  themselves (ADR-0055's scope is `topic_acl`/`peer_topic_acl`/
  `allow_peer`/`peer_topic_filter`, not the `--peer` list), but
  `allow_peer` *is* - today, an operator who fixes the allowlist and
  reloads gets the dial side reconnecting automatically on its next
  (infinitely-retried) attempt, with no restart needed. Giving up
  permanently on first rejection would silently remove that recovery
  path.
- **A moderate, still-doubling backoff** (e.g. just raising
  `MAX_BACKOFF`) was considered and rejected as under-committing: a
  policy rejection needs a human to notice and act, a timescale of
  minutes at the least - continuing to ramp through sub-second and
  single-digit-second delays on the way there is still mostly wasted
  effort for a condition that's already known, with certainty, not to
  be transient. A rejection isn't "maybe still flaky," it's a *known*
  state - the backoff should reflect that from the very first retry,
  not creep toward it.

Logging also distinguishes the two: `connect_and_handshake` logs a
rejection as `"seed peer explicitly rejected this connection"` with
the rejection's own message, instead of the generic `"handshake with
seed peer failed"` every other failure still uses - an operator
scanning logs can tell "this is deliberate, fix the allowlist" from
"something's actually broken" without reading the message content.

## Consequences

A rejected seed peer or gossip-discovered peer now retries at a slow,
steady 2-minute cadence instead of hammering the target every few
seconds indefinitely - the fix issue #193 asked for. Recovery once the
allowlist is fixed and reloaded still works automatically, just with
up to a 2-minute delay before the next attempt notices, instead of
however short the exponential backoff happened to have ramped down to.

This only distinguishes a rejection from *every other* failure
(refused, TLS failed, timed out, a confusing non-`Error` reply) -
it doesn't yet give #149's version-mismatch rejection anything beyond
what this ADR already built: when that lands, an accepting node
replying with `MessageKind::Error` for an unsupported version already
flows through the same `HandshakeError::Rejected` path this ADR adds,
with no further changes needed here.

Not attempted: distinguishing rejection *reasons* from each other (an
allowlist rejection vs. a future version-mismatch one) for a different
backoff or log treatment per reason - the message string is still free
text, not a structured code. Worth revisiting if a second rejection
reason (from #149) turns out to want different handling than this
ADR's single `Rejected` bucket gives it; not assumed here.
