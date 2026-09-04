# 38. `PeerId` derived from a TLS certificate fingerprint

## Status

Accepted

## Context

`PeerId` has been a bare, self-reported random UUID since ADR-0005,
which flagged this as a deliberate, temporary gap: "expected to be
replaced by a cryptographic identity ... once federation/trust work
begins." That work has begun - TLS (ADR-0016) and a certificate
fingerprint used as an authorization `Principal` (ADR-0017/0018) both
exist now - but neither is connected to `PeerId` at all. Filed as
#120 (Phase 12, docs/ROADMAP.md), the foundational piece: a
connection's `PeerId`, whatever it claims in a `Hello` or an
envelope's `sender`, should be bound to the fingerprint already
authenticating it.

Phase 12 splits this into three issues on purpose. This ADR is #120's
alone: making a cryptographic `PeerId` possible to derive at all, and
using it for **a node or CLI's own identity**, when it has a
certificate to derive it from. Checking a claimed `PeerId` against the
identity its own certificate implies - and deciding what a mismatch
does - is #121; extending that trust to loop-prevention/membership
is #122. This ADR doesn't implement either.

## Decision

### `PeerId::from_fingerprint`: a version-8 (custom) UUID from the fingerprint's first 16 bytes

```rust
pub fn from_fingerprint(fingerprint: [u8; 32]) -> Self {
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&fingerprint[..16]);
    Self(uuid::Builder::from_custom_bytes(bytes).into_uuid())
}
```

Deterministic: the same certificate always derives the same `PeerId`.
Truncating the SHA-256 fingerprint to 16 bytes (from 32) to fit
`PeerId`'s existing `Uuid` representation costs collision resistance
in principle (128 bits of hash instead of 256), but 128 bits is
already far more than any realistic peer count could ever collide
on - not worth widening `PeerId` itself over. `uuid::Builder::
from_custom_bytes` packs those 16 bytes as RFC 9562's "version 8,
custom" UUID - the only version reserved for exactly this
(implementation-defined bytes wrapped in a valid UUID) - rather than
a version this project already gives its own meaning
(`PeerId::new()`/`MessageId` both use version 7, timestamp-ordered).
A cryptographically-derived `PeerId` is now visibly distinguishable
from a self-assigned one by its version nibble alone.

### A node's own `node_id` derives from its own `--tls-cert`, when given

`TlsConfig::build()` (thoth-mesh-node) now also returns this node's
own leaf certificate's fingerprint alongside the acceptor/connector it
already built - it was already loading that certificate to build them,
so this doesn't cost a second file read. `run_with_tls`/
`serve_with_tls`/`spawn_with_tls` use it to set `node_id` via
`PeerId::from_fingerprint` instead of `PeerId::new()`, whenever
`--tls-cert`/`--tls-key`/`--tls-ca` are configured.

A useful side effect, not just a stepping stone: a node's identity is
now stable across restarts as long as its certificate is - closing
OPERATIONS.md's "no persistent identity across restarts" note, for
any node that runs TLS with its own certificate. A node with no TLS
identity (plaintext, or TLS without presenting a client cert)
generates a random `PeerId::new()` exactly as before - nothing about
this changes for that case, the same boundary `Principal::Anonymous`
already draws for `--topic-acl`.

### `thoth-mesh-cli`'s `sender` derives from `--tls-cert`, the same way

`build_connector` (thoth-mesh-cli) similarly returns its own leaf
certificate's fingerprint when `--tls-cert`/`--tls-key` were given
alongside `--tls-ca`, and `run()` uses it for the `sender` on every
envelope this invocation sends, instead of a fresh `PeerId::new()`.
Same reasoning as the node side: an envelope's `sender` naming a
cryptographic identity is only possible when the client actually
presented one.

### Not implemented here: checking a *claimed* identity against a certificate

Everything above is about a node/CLI's own identity, derived from its
own certificate - there's no "claim" involved, nothing to disagree
with. What a *peer's* `Hello` or a *client's* envelope claims as its
`sender` is still trusted as-is after this ADR, exactly as before -
`ConnectionContext`'s already-captured `peer_fingerprint` (ADR-0017)
isn't yet compared against it anywhere. That comparison, and the
reject-vs-correct policy for a mismatch, is #121, deliberately kept
out of this ADR's scope.

## Consequences

`thoth-mesh status` (ADR-0037) on a TLS-configured node now reports
the same `node_id` across restarts, rather than a fresh one every
time. `thoth-mesh publish`/`subscribe --tls-cert` likewise sends a
stable `sender`. No wire-protocol change - `PeerId` is still a `Uuid`
newtype; this only changes how one gets generated. Two crates gain a
small amount of surface: `TlsConfig::build`'s return type grows a
third element, and `thoth-mesh-cli`'s internal `build_connector`
likewise - both crate-internal, no public API change outside
`PeerId::from_fingerprint` itself (new, additive) and `PeerId`'s own
doc comment (updated to reflect that a cryptographic identity now
exists, not just "expected").

Closes #120.
