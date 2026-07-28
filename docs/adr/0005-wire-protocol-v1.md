# 5. Wire protocol v1: envelope, framing, and CBOR serialization

## Status

Accepted

## Context

`thoth-mesh-core` exists specifically to hold the protocol types and wire
format shared by every other crate in the workspace (see
[0002](0002-cargo-workspace-with-layered-crates.md)). Before any
federation or networking code can be written, the shape of a message on
the wire needs to be settled: what a message looks like, how it's
identified, how it's serialized to bytes, and how those bytes are framed
on a byte stream.

Two sub-decisions dominate this:

1. **Serialization format.** thoth-mesh is federated — different peers
   will, over time, run different builds. A format that isn't
   self-describing (bincode, postcard) silently breaks compatibility the
   moment a field is added, removed, or reordered on one side and not
   the other, with no way for the receiver to detect it. Protobuf solves
   this well but requires a `.proto` schema and a codegen build step,
   which is a second schema language to maintain. JSON is self-describing
   and trivial to debug, but larger on the wire and slower to parse than
   a binary format.
2. **Identifiers.** Messages and peers both need identifiers. thoth-mesh
   is explicitly a recorder of events (see the project's naming — Thoth,
   scribe of the gods), so a time-ordered message ID is a natural fit.
   Peer identity will eventually need to be cryptographic (to prevent
   impersonation in a trustless federation), but that's a distinct,
   larger piece of work that shouldn't block getting a wire format in
   place.

## Decision

**Serialization:** CBOR via the `ciborium` crate. It's self-describing
(unknown/extra fields don't break decoding), pure Rust with no `unsafe`,
requires no codegen step, and is a reasonable size/speed compromise
between JSON and a non-self-describing binary format.

**Identifiers:** `MessageId` and `PeerId` are newtypes around `Uuid`.
`MessageId` specifically uses UUIDv7, which embeds a millisecond
timestamp and is monotonically sortable — so message ordering is
available for free without a redundant timestamp field on every
envelope. `PeerId` is an opaque UUID for now; it is expected to be
replaced by a cryptographic identity (e.g. a public key or its hash)
once federation/trust work begins, and nothing in this design assumes
otherwise.

**Envelope:** every message on the wire is an `Envelope { version: u8,
id: MessageId, sender: PeerId, kind: MessageKind }`. `MessageKind` is a
plain Rust enum with variants `Publish { topic, payload }`, `Subscribe {
topic }`, `Unsubscribe { topic }`, `Ack { in_reply_to }`, and `Error {
in_reply_to, message }`. Serde's default (externally tagged) enum
representation is used, which CBOR/JSON handle natively — this would
*not* work with bincode/postcard, reinforcing the serialization choice
above.

**Topic:** a validated newtype around `String` (non-empty, bounded
length, restricted character set) rather than a bare `String`, so
invalid topics are rejected at the boundary instead of propagating.

**Framing:** messages are framed on a byte stream with a 4-byte
big-endian length prefix followed by the CBOR-encoded envelope, capped
at a fixed maximum frame size to bound memory allocation from a
corrupt or hostile length prefix. Framing helpers operate on generic
`std::io::Read`/`Write`, not a specific transport — this crate has no
networking or async runtime dependency (see
[0002](0002-cargo-workspace-with-layered-crates.md)); actual socket I/O
is `thoth-mesh-node`'s job.

Explicitly out of scope for this ADR (tracked as future work, not
designed here): message authentication/signatures, payload compression,
and protocol version negotiation between peers running different
versions. The `version` field on `Envelope` exists so this can be added
later without a breaking change to the envelope shape itself.

## Consequences

Every message kind is self-describing and independently decodable even
if a future version adds fields elsewhere. Message ordering comes for
free from `MessageId`. The crate stays free of networking/async
dependencies, keeping it usable in contexts that don't need a full
runtime (tests, tooling, potentially other transports later). The cost
is CBOR's binary size/speed being worse than a non-self-describing
format — acceptable now, revisit if profiling ever shows it matters.
`PeerId` being a bare UUID is a known, deliberate gap: nothing in this
design should be built assuming a peer's identity is trustworthy.
