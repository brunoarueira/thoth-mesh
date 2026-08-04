# 7. thoth-mesh-node v1: TCP daemon wiring the broker to sockets

## Status

Accepted

## Context

`thoth-mesh-node`'s stated purpose (ADR-0002) is "wires the federation
layer to a network transport." But `thoth-mesh` (federation/gossip) is
still an empty placeholder — there's no federation layer to wire up
yet. `thoth-mesh-broker` (ADR-0006), on the other hand, is a complete
in-process pub/sub engine with nothing driving it from a real network
connection. The useful first milestone for this crate is wiring
`thoth-mesh-broker` to TCP sockets: accept connections, decode framed
envelopes, dispatch `Publish`/`Subscribe`/`Unsubscribe` to the broker,
and forward broadcast delivery back out over the wire. Federation joins
later, once `thoth-mesh` has something to wire in.

Three sub-decisions dominate this:

1. **Transport.** TCP is the obvious default to get a working system;
   anything else (QUIC, WebSocket) is premature before a single
   transport works end to end.
2. **Async I/O vs. `thoth-mesh-core`'s sync framing.**
   `read_frame`/`write_frame` in `thoth-mesh-core` operate on
   `std::io::Read`/`Write` by design (ADR-0002/0005 keep core free of
   async/networking dependencies). A TCP daemon needs non-blocking,
   concurrent I/O across many connections, which needs async framing.
3. **Concurrency shape of a connection.** A connection has two
   independent things happening: reading control messages
   (publish/subscribe/unsubscribe) from the client, and pushing
   broadcast deliveries from however many topics it's subscribed to
   back out to the client. These need to happen concurrently without
   one blocking the other.

## Decision

**Transport:** raw TCP, no TLS. Binds to `127.0.0.1:49500` by default
(a private/dynamic-range port, chosen to avoid colliding with commonly
registered services). Auth, TLS, and configurable bind addresses are
out of scope for v1 (see Consequences).

**Async framing:** a small hand-rolled async equivalent of
`thoth_mesh_core::framing`, living in `thoth-mesh-node` itself
(`tokio::io::AsyncReadExt`/`AsyncWriteExt` instead of
`std::io::Read`/`Write`, same 4-byte-BE-length-prefix format and
`MAX_FRAME_LEN` bound). This duplicates the read/write control flow
(~20 lines) but keeps `thoth-mesh-core` free of any async runtime
dependency, preserving the boundary ADR-0002 drew. The error type
(`FramingError`) is reused as-is from `thoth-mesh-core` — both sync and
async I/O in Rust/tokio report errors as `std::io::Error`, so only the
read/write functions need duplicating, not the error type.

**Connection task topology:** one task per accepted connection, split
into a socket read half and write half
(`tokio::net::TcpStream::into_split`). The task body is a
`tokio::select!` loop racing two things: the next framed envelope off
the socket, and the next envelope off an internal per-connection
`mpsc` channel. Subscribing to a topic spawns a small forwarder task
that owns the broker's `broadcast::Receiver` for that topic and relays
every message it receives into the connection's outgoing `mpsc`
channel; its `JoinHandle` is kept in a `HashMap<Topic, JoinHandle<()>>`
so `Unsubscribe` (or connection teardown) can `.abort()` it cleanly.

**Ack behavior:** the node acks `Subscribe` and `Unsubscribe` (the
client gets confirmation that its registration state actually changed
— the one thing about them that has a meaningful yes/no answer).
`Publish` stays fire-and-forget: broadcast delivery to N subscribers
doesn't have a single "did it work" answer to report, so no ack is
sent for it in v1. `MessageKind::Ack`/`Error` received *from* a client
are ignored — a client acking the server isn't meaningful in v1's
request/response shape.

**Error handling:** a malformed frame, oversized frame, or CBOR decode
failure logs and closes that connection; it never brings down the
listener or other connections, since each connection is an isolated
task.

## Consequences

`thoth-mesh-node` now has a real, testable end-to-end path: a client
can connect, subscribe to a topic, have another client publish to it,
and see the message arrive — entirely through `thoth-mesh-core`'s wire
format and `thoth-mesh-broker`'s dispatch, with no federation involved.
Federation integration later is additive: `thoth-mesh` slots in
alongside the broker rather than requiring a rework of the connection
handling built here.

The cost is a stated scope gap against ADR-0002's original description
of this crate ("wires the federation layer") — that's still true, just
not yet, and is explicitly deferred rather than abandoned. No
authentication, no TLS, and a hardcoded loopback-only bind address mean
this is unsafe to expose beyond local development; that's acceptable
for the current milestone but must be revisited before anything
resembling a real deployment. The lack of publish acks means delivery
is best-effort with no confirmation — consistent with ADR-0005's
existing stance that delivery guarantees are future work, not decided
here.
