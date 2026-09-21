# 53. Payload-level encryption

## Status

Accepted

## Context

Filed as #142 (Phase 16). Transport TLS (ADR-0016) protects a payload
in transit between two directly-connected parties, but a payload
relayed through an intermediate peer is readable by that peer - no
end-to-end option exists. The issue's own known shape already leaned
toward client-side encryption (opaque ciphertext bytes, nothing the
protocol needs to know about) over protocol-level support, and
explicitly scoped key distribution/management out as a separate,
much larger problem.

## Decision

### No first-party encryption code - this already composes today, for free

`publish <topic> -` reads stdin to EOF as exact raw bytes, and
`subscribe --output raw` writes every delivered payload as exact raw
bytes with no lossy decoding (ADR-0035). A `Publish` payload is
already fully opaque - `Vec<u8>` with no protocol-level interpretation
(PROTOCOL.md) - so piping a payload through any external encryption
tool on the way in, and the reverse on the way out, is *already*
end-to-end encryption through this mesh today, with zero protocol or
CLI changes:

```sh
age -e -r <recipient> secret.bin | thoth-mesh publish secure.topic -
thoth-mesh subscribe secure.topic --output raw | age -d -i key.txt
```

An intermediate relaying peer only ever sees ciphertext bytes - it has
no more visibility into this than it does into any other opaque
payload. Building a first-party encrypt/decrypt feature on top of this
would mean taking on real cryptographic surface area (key formats,
cipher choice, nonce handling, authentication) for a problem generic,
independently-audited tools already solve well - "don't roll your own
crypto" applies exactly as much to gluing a convenience flag over a
well-known primitive as it does to writing the primitive itself,
because the gluing is exactly where nonce-reuse and
missing-authentication bugs actually happen. This ADR's decision is
therefore to document the pattern, not build a feature - the issue's
own "likely the former" steer plus the already-solved composition
made this the shape of "picking this up," not a new capability to
design.

### The cookbook covers two tools, each in both its asymmetric and symmetric mode

Documented in [docs/OPERATIONS.md](../OPERATIONS.md#payload-level-encryption)
rather than repeated here, so it stays next to the rest of the
CLI-usage documentation instead of living only in an ADR nobody
re-reads day to day:

- **[`age`](https://age-encryption.org)** - modern, minimal,
  authenticated by default. `-r <recipient>`/`-i <identity>` for
  public-key (asymmetric) use; `-p` for a shared passphrase
  (symmetric) with nothing to generate or manage at all.
- **[GnuPG](https://gnupg.org)** (`gpg`) - the older, far more widely
  *already-deployed* option, useful specifically when an organization
  already has a PGP keyring/web-of-trust in place rather than wanting
  to introduce a second key format. `--encrypt --recipient` for
  asymmetric; `--symmetric`/`-c` for a shared passphrase.

Two tools, not one, deliberately: `age`'s own docs recommend it as the
simpler modern default, but "which tool" is exactly the kind of choice
this ADR shouldn't make unilaterally for every reader - an
organization with existing GPG key infrastructure gets no benefit from
being steered onto a second, unrelated key format. Each tool is shown
in both its asymmetric (a publisher encrypts to a subscriber's public
key) and symmetric (both sides already share a secret) mode, since
that axis - not "which tool" - is the actual decision a reader has to
make first, based on whether the two ends can practically exchange a
public key beforehand or already share a passphrase out of band.

`openssl enc` was deliberately left out of the cookbook despite being
close to universally installed already: its `enc` subcommand has a
long-standing history of poor support for authenticated (AEAD) cipher
modes - the tag isn't handled automatically the way `age`/`gpg` handle
it - making it easy to reach for the one flag combination (a
non-authenticated mode) this ADR most wants to steer a reader away
from. A footnote in the cookbook points this out rather than silently
omitting it, so a reader who already reaches for `openssl` understands
why it isn't one of the two recommended starting points.

### Key distribution stays entirely the reader's problem, on purpose

Exactly as the issue scoped it: `age -r`/`gpg --recipient` both assume
the publisher already has the subscriber's public key by whatever
means the reader's own organization already uses to exchange one (a
keyserver, an internal directory, a file dropped in a shared
location); the two `-p`/`-c` (symmetric) examples assume the same for
a shared passphrase. Nothing here attempts a discovery, registry, or
distribution mechanism - that remains the "large, separate problem"
the issue named, undiminished by this ADR.

### Continuous multi-message decryption on the subscribe side is a known, undocumented-as-solved gap

`--output raw` writes every delivered payload back-to-back with
*no delimiter between messages* (ADR-0035, deliberately - "the common
case is capturing exactly one payload"). Composing that with any of
the tools above works cleanly for **encrypting a `publish`** (always
exactly one payload in, one ciphertext out, no framing question at
all) and for **capturing and decrypting exactly one delivered
message**. It does *not* work for feeding a long-running
`subscribe --output raw` session's continuous stream into one
long-running decrypt process and expecting each message decrypted
independently - none of the ciphertext formats above are
self-delimiting when naively concatenated, so a decryptor fed the
whole stream sees one corrupt blob after the first message, not a
sequence of independent ones. The cookbook says this plainly rather
than implying a pattern that quietly breaks past the first message. A
reader who genuinely needs continuous per-message decryption has to
either re-invoke `subscribe` per message today, or this becomes a
real, separate follow-up (e.g. a delimited/length-prefixed
`--output` mode) if real usage shows it's wanted - not something this
ADR tries to solve by making the cookbook's advice vaguer than it
should be.

## Consequences

- No code changes anywhere in the workspace - `thoth-mesh-core`,
  `thoth-mesh-node`, and `thoth-mesh-cli` are all already sufficient,
  via ADR-0005 (opaque payload bytes) and ADR-0035 (raw stdin/stdout
  fidelity).
- New `docs/OPERATIONS.md` section: a cookbook with four concrete
  recipes (age/gpg × asymmetric/symmetric), the `openssl enc` caveat,
  and the continuous-multi-message limitation stated explicitly.
- Key distribution/management remains entirely out of scope, as the
  issue asked.
- A delimited/self-framing `--output` mode for continuous per-message
  decryption is an explicit, independent follow-up - not attempted
  here.
