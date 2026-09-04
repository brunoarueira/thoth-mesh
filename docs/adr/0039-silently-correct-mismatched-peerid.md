# 39. Silently correct a mismatched `PeerId` claim

## Status

Accepted

## Context

ADR-0038 (#120) made a `PeerId` derivable from a TLS certificate
fingerprint, and used it for a node/CLI's own identity - but a
connection's *claimed* identity (a peer's `Hello`, a client's
envelope `sender`) is still trusted as-is: nothing compares it
against the fingerprint actually authenticating that connection.
Filed as #121 (Phase 12), which named the decision this ADR makes:
reject a mismatch outright (like an `--allow-peer` allowlist
rejection, ADR-0017), or silently correct it and continue.

## Decision

### Silently correct, not reject

A connection with a known TLS fingerprint gets its claimed `PeerId` -
in a `Hello`, or any envelope's `sender` - replaced with
`PeerId::from_fingerprint(fingerprint)` whenever the two disagree,
rather than being rejected. A `tracing::warn!` notes the correction
(claimed and authenticated values both logged), but the connection
proceeds under the authenticated identity.

Rejecting was the more obvious-looking choice, matching how an
unlisted `--allow-peer` certificate is already handled - but a
mismatch here has a very different threat shape. Under normal
operation *after* ADR-0038, a node/CLI's own claimed identity and its
own certificate's derived identity are always the same value by
construction (both come from the same `PeerId::from_fingerprint` call
this node itself made) - so in practice, a mismatch only arises from
a peer or client that hasn't been upgraded to ADR-0038 yet (an older
build presenting a certificate but still self-reporting a random
`PeerId::new()`, exactly as every build did before it), or from a
genuine impersonation attempt. Rejecting outright can't distinguish
those two cases - the wire carries no version negotiation for this -
and would turn an ordinary rolling upgrade (some nodes/clients on the
old build, some on the new one, both presenting real certificates)
into a hard outage for every not-yet-upgraded participant still using
TLS identities at all. Silent correction closes the actual security
gap identically either way - an impersonator's claim is simply never
honored, gaining them nothing - without that collateral damage.

### One correction point covers every message kind

`ConnectionContext` gains `authenticated_sender(&self, claimed:
PeerId) -> PeerId`, applying the rule above (or `claimed` unchanged,
if `peer_fingerprint` is `None` - the same TLS-and-cert-only boundary
ADR-0038 already draws). It's called in exactly two places:

- Once per frame, in `run_connection`'s read loop, immediately after
  decoding an envelope and before any dispatch - so `Hello`,
  `Publish`, `Subscribe`, `Unsubscribe`, and `StatusRequest` (every
  message kind that carries a meaningful `sender`) are corrected by
  the same code path, not five separate call sites that could drift
  out of sync with each other. `handle_hello`/`handle_publish`/etc.
  need no changes at all - they already just read `envelope.sender`,
  which is authenticated before they ever see it.
- Once in `admit_initial_peer`, for the dial side's already-known
  identity (established before the read loop starts, see ADR-0010) -
  the one case a fresh-per-frame correction can't reach, since
  there's no frame to intercept.

A `Publish`'s corrected `sender` is also what gets broadcast to
subscribers (the same `Envelope` a forwarder delivers downstream) -
not a node-internal-only correction. A `PeerAnnounce`'s gossiped
`PeerAdvert` entries are a different thing entirely (third-party
claims about peers not directly connected to this node) and aren't
touched here; verifying those transitively is a much harder problem
this ADR doesn't attempt.

## Consequences

An impersonation attempt (claiming a `PeerId` that isn't backed by
the certificate presented) never succeeds at anything - the claim is
simply overwritten before it reaches membership, interest
propagation, or a delivered `Publish`. `OPERATIONS.md`'s "claimed
`sender`" limitation is narrowed accordingly: a *certificate-backed*
identity can no longer be spoofed by a differently-certificated
connection; a connection with no certificate at all still has nothing
stronger than an unverifiable self-report, unchanged.

Closes #121.
