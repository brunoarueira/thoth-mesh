# thoth-mesh wire protocol

This is a from-scratch description of the protocol every thoth-mesh
connection speaks — client-to-node and node-to-node alike — independent
of the Rust implementation. It's meant to be enough to implement a
compatible client or peer without reading `thoth-mesh-core`'s source.
For the reasoning behind these choices, see
[ADR-0005](docs/adr/0005-wire-protocol-v1.md) (envelope, framing,
CBOR), [ADR-0008](docs/adr/0008-generic-async-framing.md) (async
framing), [ADR-0009](docs/adr/0009-peer-handshake-shared-port.md) (the
peer handshake), [ADR-0011](docs/adr/0011-interest-propagation-and-loop-prevention.md)
(interest propagation and loop prevention),
[ADR-0015](docs/adr/0015-dynamic-peer-discovery-gossip.md) (peer
discovery via gossip), [ADR-0016](docs/adr/0016-tls-transport-security.md)
(TLS), [ADR-0017](docs/adr/0017-peer-allowlist-via-tls-fingerprint.md)
(peer certificate allowlisting), and
[ADR-0018](docs/adr/0018-per-topic-client-authorization.md) (per-topic
client authorization),
[ADR-0020](docs/adr/0020-peer-scoped-topic-restriction.md) (per-topic
peer-link authorization), and
[ADR-0021](docs/adr/0021-message-replay-ring-buffer.md) (replay for
late subscribers). For diagrams of several of these flows, see
[docs/FLOWS.md](docs/FLOWS.md).

**Status:** version 1, and explicitly unstable — see ADR-0014. Nothing
here should be assumed to hold across a breaking change; check
`PROTOCOL_VERSION` (currently `1`) and this file's git history.

## Transport

TCP. There is one listening port per node; the exact same port and
protocol serve both client connections and peer links (see
[Connections](#connections-clients-vs-peer-links) below) — there's no
separate cluster port. Connections are long-lived: a client opens one
connection and issues `Subscribe`/`Publish`/`Unsubscribe` on it for as
long as it wants to stay connected, and a peer link stays open
indefinitely once it's up.

TLS is optional (see [ADR-0016](docs/adr/0016-tls-transport-security.md))
and, when enabled, wraps the connection *underneath* everything
below — framing, the envelope, and every message kind are unchanged
either way, since a `MaybeTlsStream` looks like a plain byte stream to
everything above it. A peer link's TLS certificate can optionally be
checked against an `--allow-peer` allowlist (see
[ADR-0017](docs/adr/0017-peer-allowlist-via-tls-fingerprint.md)), and
a client's own certificate (or lack of one) can likewise gate which
topics it may `Subscribe`/`Publish` to, via `--topic-acl` (see
[ADR-0018](docs/adr/0018-per-topic-client-authorization.md)) — and,
independently, a peer link's own certificate can gate which topics
*it* may carry, via `--peer-topic-acl` (see
[ADR-0020](docs/adr/0020-peer-scoped-topic-restriction.md)); the two
lists never cross-apply, a peer is never checked against `--topic-acl`
and a client is never checked against `--peer-topic-acl`. None of
these authenticate what a `sender` value itself claims to be, though —
nothing ties an envelope's `sender` field to the connection's TLS
identity. The metrics endpoint (`--metrics-addr`) is unrelated to this
port and this TLS layer entirely — it's a separate, plain-HTTP port
with its own opt-in bearer-token authentication (see
[ADR-0019](docs/adr/0019-metrics-endpoint-authentication.md) and
`docs/OPERATIONS.md`).

## Framing

Every message on the wire is a single length-prefixed frame:

```
+----------------------+---------------------------+
| length (4 bytes, u32 | payload (`length` bytes,   |
| big-endian)          | CBOR-encoded Envelope)     |
+----------------------+---------------------------+
```

- The length prefix is the payload's byte length, **not including**
  the 4-byte prefix itself.
- The maximum allowed length is 16 MiB (`16 * 1024 * 1024`). A frame
  whose declared length exceeds this is rejected without reading the
  payload, and the connection is closed — this bounds how much a
  corrupt or hostile length prefix can make a reader allocate.
- There is no magic number, checksum, or other framing overhead beyond
  the 4-byte length. The payload is always exactly one CBOR-encoded
  `Envelope` (see below); nothing else is ever sent between the
  4-byte prefixes.

## Envelope

Every message is wrapped in an `Envelope`, CBOR-encoded as a map with
exactly these four keys, in this order:

| Key | CBOR type | Meaning |
| --- | --- | --- |
| `version` | unsigned integer | Protocol version. Always `1` today. **Not currently validated on receipt** by this implementation — an envelope claiming a different version is still processed. Reserved for future version negotiation. |
| `id` | byte string (16 bytes) | This message's [`MessageId`](#messageid). |
| `sender` | byte string (16 bytes) | The sending node/client's [`PeerId`](#peerid). |
| `kind` | map | The message payload — see [Message kinds](#message-kinds). |

### `MessageId`

A [UUIDv7](https://www.rfc-editor.org/rfc/rfc9562#name-uuid-version-7),
encoded as its raw 16 bytes (a CBOR byte string, **not** the
hyphenated text form UUIDs are usually printed as). UUIDv7 embeds a
millisecond timestamp and sorts monotonically by generation time, so
message ordering is available without a separate timestamp field.
Every envelope gets a freshly generated `MessageId` — including a
forwarded/re-published envelope crossing the mesh, which **keeps the
original `MessageId`** it was created with (this is what loop
prevention and de-duplication key on — see
[Delivery semantics](#delivery-semantics)).

### `PeerId`

Also a UUID, encoded the same way (16 raw bytes). Identifies a client
or a node. **Carries no cryptographic guarantee today** — nothing
about the protocol proves a `sender` value is who it claims to be; any
connection can claim any `PeerId`. A random one is generated fresh per
CLI invocation and per node startup.

### `Topic`

A UTF-8 string, encoded as a CBOR text string (not the raw-bytes
encoding `MessageId`/`PeerId` use). Validated on both construction and
deserialization — a malformed topic in a decoded envelope is a decode
error, not something that reaches application code:

- Non-empty.
- At most 256 bytes.
- Only ASCII alphanumerics plus `.`, `-`, `_`, and `/`.

There's no hierarchy or wildcard semantics — two topics either match
exactly or don't match at all (see
[docs/ROADMAP.md](docs/ROADMAP.md) Phase 8 for wildcard matching as
possible future work).

## Message kinds

`kind` is CBOR-encoded the way Rust/serde encodes an externally-tagged
enum by default: a one-entry map whose single key is the variant name,
and whose value is that variant's fields as a nested map. For example,
a `Publish` is `{"Publish": {"topic": "...", "payload": [...]}}`, not
a flat structure with a separate `"type"`-style discriminant field.

### `Publish`

```
{"Publish": {"topic": <Topic>, "payload": <bytes>}}
```

Publishes `payload` to `topic`. `payload` is an arbitrary byte string
with no interpretation at the protocol level — **note**: it's CBOR-
encoded as an *array of unsigned integers* (CBOR major type 4, one
element per byte), not as a CBOR byte string (major type 2) the way
`MessageId`/`PeerId` are. This is a consequence of how the reference
implementation's `Vec<u8>` field serializes by default, not a
deliberate format choice — a byte string would be considerably more
compact, but an implementation reading this protocol needs to accept
what's actually on the wire today.

No reply is sent for a `Publish` — it's fire-and-forget from the
sender's point of view, unless a `--topic-acl`
([ADR-0018](docs/adr/0018-per-topic-client-authorization.md)) or, for
a peer link, a `--peer-topic-acl`
([ADR-0020](docs/adr/0020-peer-scoped-topic-restriction.md)) refuses
it, in which case an `Error` takes the place of the (otherwise absent)
reply. See [Delivery semantics](#delivery-semantics) for what
"published" actually guarantees.

### `Subscribe`

```
{"Subscribe": {"topic": <Topic>}}
```

Registers interest in `topic` on this connection. The node replies
with an `Ack` once registered, or an `Error` instead if a
`--topic-acl` (or, for a peer link, a `--peer-topic-acl`) refuses it.
Sending `Subscribe` for a topic this connection is already subscribed
to is a no-op (still gets an `Ack`).

Immediately after the `Ack`, this node also delivers - as ordinary
`Publish` messages - whatever it currently holds in `topic`'s replay
buffer, oldest first, so a client connecting after the fact can still
catch up on recent history (see
[ADR-0021](docs/adr/0021-message-replay-ring-buffer.md)). This only
happens the first time a connection registers interest in `topic`; a
no-op re-`Subscribe` above doesn't replay anything again.

### `Unsubscribe`

```
{"Unsubscribe": {"topic": <Topic>}}
```

Removes interest in `topic` on this connection, acknowledged the same
way as `Subscribe`.

### `Ack`

```
{"Ack": {"in_reply_to": <MessageId>}}
```

Sent by a node in reply to a `Subscribe` or `Unsubscribe`,
referencing the `id` of the envelope it's acknowledging. A client
waiting on a `Subscribe`/`Unsubscribe` to take effect should wait for
the `Ack` whose `in_reply_to` matches the request's `id` — other
traffic (e.g. a `Publish` delivered on the same connection) can
legitimately arrive first and should be skipped over, not treated as
the reply.

### `Error`

```
{"Error": {"in_reply_to": <MessageId | null>, "message": <string>}}
```

Reserved for reporting a protocol-level error, optionally in response
to a specific message. A malformed frame or envelope still closes the
connection outright rather than replying with an `Error`. Two cases
the reference implementation does send one for:

- A peer link rejected by an `--allow-peer` allowlist
  ([ADR-0017](docs/adr/0017-peer-allowlist-via-tls-fingerprint.md)):
  `in_reply_to` names the `Hello` being rejected, and the connection
  closes right after, on either side of the link — whichever side is
  enforcing an allowlist and finds the far end's TLS certificate
  missing or unlisted.
- A `Subscribe` or `Publish` refused by a `--topic-acl`
  ([ADR-0018](docs/adr/0018-per-topic-client-authorization.md)), or,
  symmetrically for a peer link, by a `--peer-topic-acl`
  ([ADR-0020](docs/adr/0020-peer-scoped-topic-restriction.md)):
  `in_reply_to` names the refused message, and — unlike the peer-link
  *rejection* case above — **the connection stays open**. A client (or
  peer link) denied on one topic may be entitled to others; only a
  `Subscribe`/`Publish` actually rejected gets an `Error` in place of
  its usual `Ack`/delivery, nothing else about the connection changes.

A client should be able to decode and handle receiving one either
way.

### `Hello`

```
{"Hello": {"listen_addr": <string | null>}}
```

Identifies a connection as a peer link rather than a client
connection, and is the only message kind involved in the
[peer handshake](#peer-handshake). `listen_addr` is the address other
peers should dial to reach the sender back, if it accepts inbound
connections at all (a peer that only ever dials out can send `null`).

### `PeerAnnounce`

```
{"PeerAnnounce": {"peers": [{"peer_id": <PeerId>, "listen_addr": <string>}, ...]}}
```

Advertises peers the sender knows about, so the receiver can discover
and dial peers it was never directly configured with (see
[ADR-0015](docs/adr/0015-dynamic-peer-discovery-gossip.md)). Only
peers with a known `listen_addr` are worth advertising — unlike
`Hello`'s `listen_addr`, this one isn't nullable, since an entry with
nothing to dial wouldn't be useful to gossip in the first place. Sent
over a peer link only, never by/to a plain client connection: once as
a batch catch-up when the link comes up (every peer already known,
except the new link's own), and again, incrementally, whenever the
sender itself learns of a peer it didn't already know.

No reply is sent for a `PeerAnnounce`, the same as `Publish`.

## Connections: clients vs. peer links

There is exactly one kind of connection at the transport level; the
protocol doesn't distinguish a "client port" from a "peer port" (see
ADR-0009). What makes a connection a **peer link** rather than a
plain client connection is purely behavioral:

- It's the one that completed the [peer handshake](#peer-handshake)
  below (sent or received a `Hello`).
- Once it's a peer link, the two ends additionally propagate their
  aggregate topic interest to each other with the *exact same*
  `Subscribe`/`Unsubscribe` messages a client would send — a peer
  link is, from the wire's point of view, a client that also happens
  to forward its own subscribers' aggregate interest onward. See
  ADR-0011 for the loop-prevention/de-duplication this requires once
  a mesh can have cycles.
- Peer links are also the only connections `PeerAnnounce` is ever
  sent on — a plain client connection neither sends nor receives one
  (see ADR-0015).

A node's own set of "topics anything downstream wants" is what gets
propagated to peers, not each individual client subscription.

## Peer handshake

The dialing side sends `Hello` first, immediately after connecting —
before anything else on the connection. The accepting side, on
receiving a `Hello`, replies with its own `Hello`. After this
exchange, both sides know:

- The other side's `PeerId` (from the `Hello` envelope's `sender`).
- The address to dial the other side back at, if any (from
  `listen_addr`).

There is no explicit handshake acknowledgment beyond the `Hello`
exchange itself, and no timeout defined at the protocol level for how
long a dialing side should wait for the reply `Hello` before giving
up (the reference implementation ties this to the underlying TCP
connect/read behavior). Receiving anything other than a `Hello` as
the very first message on a freshly dialed connection is a handshake
failure.

If an `--allow-peer` allowlist is configured (ADR-0017), either side
may reject the other's `Hello` instead of replying with its own: it
sends `Error { in_reply_to: <the rejected Hello's id>, .. }` and
closes the connection without ever completing the handshake. This can
happen on the dialing side too — a dialed peer's `Hello` reply can
itself be rejected by the dialer's own allowlist — not only on the
accepting side.

## Delivery semantics

Worth being explicit about what thoth-mesh does **not** currently
guarantee:

- **Best-effort, in-memory only - bounded replay, not durability.**
  There is no persistence across a node restart. Live delivery still
  reaches whoever is subscribed *at that moment*, on that node or
  reachable through the mesh; a subscriber connecting afterward is
  additionally replayed each topic's recent backlog (a bounded
  in-memory ring buffer, capacity `DEFAULT_REPLAY_BUFFER_CAPACITY`,
  currently 256, per topic) - see
  [ADR-0021](docs/adr/0021-message-replay-ring-buffer.md). A subscriber
  connecting after a topic's backlog has rolled past that capacity
  still misses whatever fell off the oldest end.
- **A slow subscriber can miss messages.** Delivery to each
  subscriber goes through a bounded channel; a subscriber that falls
  too far behind gets some of its messages silently dropped rather
  than the sender blocking or the connection failing. There's no
  wire-level signal to a client when this happens.
- **De-duplication, not exactly-once.** An envelope crossing more
  than one hop of a cyclic mesh keeps its original `MessageId`
  end-to-end, and each node remembers a bounded number of recently
  seen `MessageId`s to drop a duplicate rather than deliver (or
  re-forward) it twice. This prevents loops and double-delivery
  within that memory window, not for the life of the mesh — a very
  old repeated `MessageId` after enough other traffic has gone by
  could in principle be treated as new again.
- **No delivery confirmation for `Publish`.** Unlike `Subscribe`/
  `Unsubscribe`, nothing acknowledges a `Publish` — not receipt by the
  node it was sent to, and not delivery to any subscriber.
