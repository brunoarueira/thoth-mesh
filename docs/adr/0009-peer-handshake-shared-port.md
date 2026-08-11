# 9. Peer handshake over the shared client port

## Status

Accepted

## Context

Phase 2's goal (see [docs/ROADMAP.md](../ROADMAP.md)) is nodes finding
each other and tracking liveness, without routing messages between
them yet. `thoth-mesh-node` currently has exactly one TCP listener,
and every accepted connection is assumed to be a client speaking
`Publish`/`Subscribe`/`Unsubscribe` (ADR-0007). Before any peer-to-peer
work can start, one question has to be settled: does peer traffic
share that same port and wire protocol, or does it get a dedicated
port and handshake of its own?

**Shared port + shared `Envelope`/`MessageKind`:**
`thoth-mesh-node`'s connection handling (`connection.rs`) already has
a proven per-connection task: accept, split the socket, frame with
`thoth-mesh-core::async_framing`, and dispatch on `Envelope.kind` in a
`tokio::select!` loop. A peer connection could ride that machinery
unchanged, distinguishing itself from a client purely by which
`MessageKind` variant it sends first. Reusing this means one listener,
one framing format, one dispatch loop, and a symmetric story for
outbound connections: dialing a peer looks exactly like the CLI
already connecting to a node (`TcpStream::connect` + the same
`Envelope`/`async_framing` types).

**Separate port + own handshake:** peers speak a dedicated protocol on
a second listener, fully decoupled from the client-facing one. This is
how several real distributed systems separate client and cluster
traffic (e.g. etcd's client vs. peer URLs, Kafka's separate
listeners), and it means the two protocols can evolve, and eventually
be secured (mTLS between nodes only, say), independently. The cost
today is doubling the listening/config surface — a second address to
bind, document, and firewall — for isolation neither protocol can
actually use yet, since neither client nor peer traffic has any
auth/TLS at all.

## Decision

Peer connections share the client port and wire format. A new
`MessageKind::Hello { listen_addr: Option<String> }` variant identifies
a connection as a peer link: the dialing node sends `Hello` as its
first message, `thoth-mesh-node`'s existing dispatch loop replies with
its own `Hello`, and both sides now know each other's identity (from
`Envelope.sender`, already carried on every message) and the address
other peers should use to dial them back. No separate handshake
protocol, port, or listener.

The handshake helpers themselves (`thoth_mesh::dial_handshake` and the
`PeerInfo`/`HandshakeError` types) live in the `thoth-mesh` crate,
generic over `futures_util::io::{AsyncRead, AsyncWrite}` exactly like
`thoth-mesh-core`'s `async_framing` (ADR-0008) — no tokio dependency,
so any runtime can dial a peer. `thoth-mesh-node` does the concrete
`tokio::net::TcpStream` dialing (one outbound task per configured seed
peer) and holds the connection open afterward; the accept-side reply
is one more arm in `connection.rs`'s existing match, requiring no
restructuring of the accept loop at all.

This is a decision for the project's current scale, not a permanent
one. If peer traffic later needs isolation the client protocol
shouldn't have (different auth, different rate limits, independent
versioning), splitting it onto its own port is a superset of what's
built here, not a rewrite: `thoth-mesh`'s handshake logic doesn't
assume a shared listener, only that it's handed something that
implements `AsyncRead + AsyncWrite`.

## Consequences

Implementing peer connections costs very little new surface: one new
`MessageKind` variant, one new match arm in `connection.rs`, and a
small dialer module in `thoth-mesh-node` reusing types and helpers
that already exist. There's exactly one port to bind, document, and
firewall for the whole project, which matters for a single-binary
learning-scale daemon.

The cost is that `MessageKind` now mixes client-facing operations
(`Publish`, `Subscribe`, `Unsubscribe`) with peer-internal ones
(`Hello`, and whatever Phase 3/4 add — topic-interest propagation,
heartbeats). That's acceptable while the set of peer message kinds is
small, but worth watching: if it grows enough to feel like two
protocols wearing one enum, revisiting the separate-port design (now
comparatively cheap, since the handshake logic doesn't hardcode a
shared listener) is the escape hatch. There's also no trust boundary
at the port level — a connection is only known to be a peer once it
sends `Hello`, not before — but nothing in this project authenticates
client connections either, so this doesn't regress anything that
existed.
