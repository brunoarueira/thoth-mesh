# 17. Peer authentication via TLS certificate fingerprint allowlist

## Status

Accepted

## Context

Issue #47 (Phase 7): today `--peer` only controls who *this* node
dials out to. There's nothing stopping an arbitrary connection from
sending `Hello` and being treated as a peer on the accept side, and
ADR-0016's TLS accept-side policy deliberately doesn't help here on
its own — a client certificate is requested but never required, since
the shared client/peer port can't tell a peer link from a plain client
until `Hello` arrives. Even a connection presenting a CA-signed
certificate is, today, automatically trusted as a peer purely for
having reached the port.

#47 leaves three questions open:

- What identifies an allowed peer.
- How this interacts with TLS (#46/ADR-0016) — explicitly suggesting
  mutual TLS client certs could double as the mechanism.
- What happens to a rejected `Hello`.

### Why `PeerId` can't be the allowlist key

The obvious-looking option — an allowlist of `PeerId`s — doesn't
actually work: `PeerId` is a UUID generated fresh per node startup
(see ADR-0009), not a durable identity. A static allowlist keyed on it
would need editing on every restart of every node, on every other node
that allows it. Any durable allowlist has to key off something that
survives a restart — which, with TLS as the only source of verified
identity this project has, means the peer's certificate.

## Decision

### Identity: SHA-256 fingerprint of the peer's leaf TLS certificate

Rather than parsing a certificate's Subject/CN (which would pull in a
general X.509 parsing dependency — this project already weighed
`rustls`/`ring` as a non-trivial dependency cost in ADR-0016 and
shouldn't casually add another), a peer's identity for allowlisting
purposes is the SHA-256 digest of its leaf certificate's raw DER
bytes, computed via `ring::digest` — already present transitively
through `rustls`, so this adds no new dependency, only a direct
`Cargo.toml` entry for a crate already being compiled.

A fingerprint is unambiguous (no risk of two different keys sharing a
human-chosen CN) and cheap to obtain: `openssl x509 -in node-b-cert.pem
-noout -fingerprint -sha256` — a command an operator already has
reason to run once they're generating certs per ADR-0016's
`docs/OPERATIONS.md` walkthrough. `thoth-mesh-tls` gains
`fingerprint()`, `fingerprint_hex()`, and `parse_fingerprint()` (the
last tolerant of `openssl`'s own colon-separated, prefixed output
format, so a value can be pasted in verbatim) so the same logic isn't
duplicated between the node and any future consumer.

**Known limitation, accepted rather than solved here:** a fingerprint
doesn't survive cert rotation — reissuing a node's certificate (same
CA, same identity, new keypair) invalidates every allowlist entry
naming its old fingerprint until they're updated. For this project's
single-operator, small-mesh scale, that's an acceptable tradeoff
against not adding an X.509 parsing dependency; a future move to
SPKI-pinning or CN-based identity is a superset of this, not a
rewrite, if it's ever needed.

### Enforcement: opt-in, requires TLS, symmetric on both sides of a link

`--allow-peer <fingerprint>` (repeatable) on `thoth-mesh-node`.
**Absent entirely, behavior is unchanged from before this ADR** — the
same opt-in bridge posture as `--metrics-addr` (ADR-0013) and TLS
itself (ADR-0016). Given at least once, it requires
`--tls-cert`/`--tls-key`/`--tls-ca` to also be set (a startup error
otherwise, via `clap`'s `requires`, the same pattern already used
among the three TLS flags themselves) — there's no meaningful
enforcement without TLS providing a verifiable identity to check in
the first place.

The check applies to **every peer link, regardless of which side
dialed**, not just the accept side the issue named. Since ADR-0016
already makes node-to-node TLS mutual unconditionally (a peer dialing
a peer always presents its own identity, and the dialer always
verifies the far end's), both sides of any peer link have a
certificate available by the time `Hello` is exchanged — so one check,
applied wherever a `Hello` is processed (the accept-side match arm, and
the dial-side handoff in `run_connection`), covers both directions
with no special-casing. This closes a gap the issue didn't name but
follows directly from it: without this, an operator's `--peer` could
still dial into a since-untrusted node, or a since-untrusted node
could still complete a dial to a configured seed peer, even with an
allowlist configured — only the accept side would have been protected.

A peer link with no certificate at all (allowed for a plain client
under ADR-0016's optional-client-auth policy) fails the check the same
as a certificate with no matching fingerprint — "no identity" doesn't
bypass an allowlist, it just never matches one. Plain client
connections are untouched: they never send `Hello`, so this check
never runs against them, and client authentication/authorization stays
out of scope here (#62).

### Rejection: `MessageKind::Error`, then close

`MessageKind::Error { in_reply_to, message }` has existed since
ADR-0005 but the reference implementation has never sent one — every
other failure mode closes the connection outright (see `PROTOCOL.md`).
An allowlist rejection is different: it's a common, recoverable
operator mistake (a fingerprint typo, a forgotten `--allow-peer` entry
after rotating a cert), not a malformed frame or a hostile peer, and
telling the rejected side *why* is worth the one new code path. On
rejection, the checking side sends an `Error` referencing the `Hello`
it's rejecting, then closes without registering the connection as a
peer link (accept side: never replies with its own `Hello`; dial side:
never proceeds past the handoff into the connection's main loop).

## Consequences

Enabling enforcement costs one new repeatable flag and a `TlsConfig`
field carrying the parsed set through `Shared` — no wire-format
change (`Error` already existed), no change to anyone not opting in
via `--allow-peer`.

The forward cost: fingerprint pinning means cert rotation is an
allowlist-wide, multi-node update, not a one-node change — worth
watching if the mesh grows past "one operator remembers to update a
few flags." `MessageKind::Error`, unused since ADR-0005, now has
exactly one real caller — worth checking any future second use fits
the same "recoverable, worth explaining" shape rather than assuming
prior precedent covers every future error path.
