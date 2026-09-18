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
peer-link authorization),
[ADR-0021](docs/adr/0021-message-replay-ring-buffer.md) (replay for
late subscribers), and
[ADR-0022](docs/adr/0022-wildcard-topic-filters.md) (wildcard topic
filters), [ADR-0041](docs/adr/0041-at-least-once-delivery-with-ack-based-redelivery.md)
(at-least-once delivery with ack-based redelivery),
[ADR-0042](docs/adr/0042-consumer-groups.md) (consumer groups),
[ADR-0043](docs/adr/0043-retained-messages.md) (retained/last-value
messages), and
[ADR-0044](docs/adr/0044-content-type-hint-on-publish.md) (content-type
hint on `Publish`), and
[ADR-0046](docs/adr/0046-durable-subscriptions.md) (durable
subscriptions via per-subscriber offset tracking), and
[ADR-0047](docs/adr/0047-message-ttl-and-dead-lettering.md) (message
TTL and dead-lettering), and
[ADR-0048](docs/adr/0048-work-queue-redelivery-for-consumer-groups.md)
(work-queue redelivery for consumer groups), and
[ADR-0049](docs/adr/0049-selective-per-peer-link-topic-filtering.md)
(selective per-peer-link topic filtering), and
[ADR-0050](docs/adr/0050-request-reply-over-pubsub.md) (request/reply
over pub/sub), and
[ADR-0051](docs/adr/0051-per-principal-publish-rate-limiting.md)
(per-principal publish rate limiting). For diagrams of several of
these flows, see [docs/FLOWS.md](docs/FLOWS.md).

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
and a client is never checked against `--peer-topic-acl`. A third,
independent knob, `--peer-topic-filter` (see
[ADR-0049](docs/adr/0049-selective-per-peer-link-topic-filtering.md)),
restricts which of this node's own aggregate interest a specific peer
link is *proactively told about* - a different question from
`--peer-topic-acl`'s "is this peer permitted to ask for this," and
checked nowhere near the wire format itself: a `Subscribe`/
`Unsubscribe` this node decides to send a peer link looks exactly like
any other, it's only *whether* one gets sent to that particular link
at all that changes. None of these authenticate what a `sender` value
itself claims to be, though —
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
or a node. A random (version 7) one is generated fresh per CLI
invocation and per node startup by default - but given a TLS
certificate of its own (`--tls-cert`/`--tls-key`), a node or CLI
invocation instead derives one deterministically from that
certificate's SHA-256 fingerprint (its first 16 bytes, packed as a
version 8/"custom" UUID per RFC 9562 - visibly distinct from a
version-7 self-assigned one), stable across restarts as long as the
certificate is (see [ADR-0038](docs/adr/0038-peerid-from-tls-fingerprint.md)).

A claimed `sender`/`Hello` value that doesn't match the fingerprint
actually authenticating a connection is silently overridden with the
one that does, rather than trusted as claimed or rejected outright -
so an impersonation attempt against a *certificate-bearing* connection
never succeeds at anything (see
[ADR-0039](docs/adr/0039-silently-correct-mismatched-peerid.md)). A
connection presenting no certificate at all still has nothing stronger
to offer than an unverifiable self-report - not a gap this can close,
since there's no cryptographic material to check it against.

### `Topic`

A UTF-8 string, encoded as a CBOR text string (not the raw-bytes
encoding `MessageId`/`PeerId` use). Validated on both construction and
deserialization — a malformed topic in a decoded envelope is a decode
error, not something that reaches application code:

- Non-empty.
- At most 256 bytes.
- Only ASCII alphanumerics plus `.`, `-`, `_`, and `/`.

A `Topic` names one concrete destination — what a `Publish` addresses.
It carries no wildcard semantics of its own; matching against a
subscriber's filter is `TopicFilter`'s job (below).

### `TopicFilter`

A UTF-8 string, encoded and validated the same way as `Topic` -
`.`-delimited segments, same charset — except two segments carry
special meaning ([ADR-0022](docs/adr/0022-wildcard-topic-filters.md)):

- `+` matches exactly one segment in that position.
- `#` matches zero or more remaining segments, and is only valid as
  the filter's *last* segment.

Every valid `Topic` string is therefore also a valid, purely-literal
`TopicFilter` with identical matching behavior — `weather.updates`
matches only itself, `weather.+` matches `weather.updates` and
`weather.forecast` but not `weather` or `weather.updates.v2`, and
`weather.#` matches `weather`, `weather.updates`, and
`weather.updates.v2` alike. What a `Subscribe`/`Unsubscribe` carries;
`Publish` always carries a concrete `Topic`, never a filter.

## Message kinds

`kind` is CBOR-encoded the way Rust/serde encodes an externally-tagged
enum by default: for a variant with fields, a one-entry map whose
single key is the variant name and whose value is that variant's
fields as a nested map. For example, a `Publish` is `{"Publish":
{"topic": "...", "payload": [...]}}`, not a flat structure with a
separate `"type"`-style discriminant field. The one field-less variant
(`StatusRequest`, below) is the exception this representation carves
out: it's a bare string, not a one-entry map with a `null` value.

### `Publish`

```
{"Publish": {"topic": <Topic>, "payload": <bytes>, "retain": <bool>, "content_type": <string | null>, "reply_to": <Topic | null>, "in_reply_to": <MessageId | null>}}
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

`content_type` (`#[serde(default)]` — an older sender that omits it
means `null`) is an optional, **purely informational** hint at what
`payload` is
([ADR-0044](docs/adr/0044-content-type-hint-on-publish.md)): a MIME
type by convention (`application/json`, `text/plain; charset=utf-8`,
`application/octet-stream`), but a node never validates, normalizes,
rejects, or otherwise interprets it — it's carried through delivery,
replay, retention, and cross-peer forwarding verbatim, and a
subscriber does whatever it likes with it (or ignores it). `null` and
an empty string both just mean "no useful hint".

`retain` (`#[serde(default)]` — an older sender that omits it means
`false`) makes this publish *also* become the topic's retained
(last-value) message
([ADR-0043](docs/adr/0043-retained-messages.md)): it's delivered
immediately to any connection that subscribes to a matching filter
afterward, even long after it was published and after it's fallen out
of the replay window ([ADR-0021](docs/adr/0021-message-replay-ring-buffer.md)).
A `retain: true` publish with an **empty** `payload` *clears* the
topic's retained message instead of setting one. Retained delivery is
folded into the same backlog the replay buffer uses — a subscriber
sees it as an ordinary `Publish`, deduplicated by `MessageId` against
anything the replay window would also deliver. A retained value is
*not* delivered to a consumer-group (`group`) subscribe, and it rides
along in a forwarded envelope so a peer that receives a `retain: true`
publish retains it locally too (this is not a mesh-wide retained-state
sync — see ADR-0043).

`reply_to` and `in_reply_to` (both `#[serde(default)]` — an older
sender that omits either means `null`) are the request/reply
convention ([ADR-0050](docs/adr/0050-request-reply-over-pubsub.md)):
purely opaque, purely conventional metadata that a node never
validates or acts on, carried through delivery, persistence, replay,
and cross-peer forwarding exactly like `content_type`. A **request**
is an ordinary `Publish` with `reply_to` set to the `Topic` its answer
should be published to; a **reply** is an ordinary `Publish` (to that
topic) with `in_reply_to` set to the request envelope's own `id` — the
same by-`id` correlation `Ack` already uses. Nothing about either field
changes how this `Publish` is otherwise handled: it persists, replays,
retains, gets dead-lettered, and routes through consumer groups exactly
like any other `Publish`. A requester that wants at most one responder
gets it by making the request topic a consumer group (`group`,
[ADR-0042](docs/adr/0042-consumer-groups.md)) rather than an ordinary
fan-out subscription — a fan-out request topic with more than one
subscriber can produce more than one reply, all carrying the same
`in_reply_to`. There is no server-side timeout or redelivery for an
unanswered request; a requester that gives up waiting does so entirely
on its own.

No reply is sent for a `Publish` — it's fire-and-forget from the
sender's point of view, unless a `--topic-acl`
([ADR-0018](docs/adr/0018-per-topic-client-authorization.md)) or, for
a peer link, a `--peer-topic-acl`
([ADR-0020](docs/adr/0020-peer-scoped-topic-restriction.md)) refuses
it, or a client (never a peer link) exceeds a configured
`--publish-rate-limit-per-sec`
([ADR-0051](docs/adr/0051-per-principal-publish-rate-limiting.md)), in
which case an `Error` takes the place of the (otherwise absent) reply.
See [Delivery semantics](#delivery-semantics) for what "published"
actually guarantees.

### `Subscribe`

```
{"Subscribe": {"filter": <TopicFilter>, "ack": <bool>, "group": <string | null>, "durable": <bool>}}
```

Registers interest in `filter` on this connection - a literal topic
name or a wildcard pattern alike (ADR-0022). The node replies with an
`Ack` once registered, or an `Error` instead if a `--topic-acl` (or,
for a peer link, a `--peer-topic-acl`) refuses it, or if `durable` is
set and any of its own requirements aren't met (see `durable` below,
which does still refuse combining it with either `ack` or `group`). A
wildcard `filter` is refused outright wherever either ACL is
configured for this connection's role, regardless of what it would
actually expand to - neither ACL is pattern-aware, and this codebase
doesn't attempt to make one covering-pattern imply anything about
another. Sending `Subscribe` for a filter this connection is already
subscribed to is a no-op (still gets an `Ack`) - including for
`ack`/`group`/`durable`: all three are only read the first time a
filter is subscribed to, the same as everything else a no-op
re-`Subscribe` doesn't retroactively change.

`ack` opts this subscription into at-least-once delivery
([ADR-0041](docs/adr/0041-at-least-once-delivery-with-ack-based-redelivery.md)):
every `Publish` delivered for it is held until the receiver sends back
an `Ack` naming that delivery's own `id` (see [`Ack`](#ack) below), and
redelivered - the exact same envelope, `id` included, so a receiver
can't tell a redelivery apart from a first delivery except by having
already seen that `id` - if no `Ack` arrives in time. `#[serde(default)]`
on the implementation side: a sender that omits `ack` entirely (every
sender that predates ADR-0041) gets `false`, unchanged fire-and-forget
behavior. `ack: true` is a *per-subscription* opt-in - a client
watching several filters over one connection (ADR-0033) can request it
for only some of them - and applies only to the direct connection
between a node and its subscriber: a `Subscribe` a peer link sends to
propagate interest onward (ADR-0011) always carries `ack: false`,
regardless of what any client subscription behind it asked for. Once
redelivery attempts are exhausted, the message is dropped and counted
(`thothmesh_delivery_ack_timeouts_total`) - and, if the node is run
with `--dead-letter-topic`, also republished there instead of just
vanishing (see [ADR-0047](docs/adr/0047-message-ttl-and-dead-lettering.md)
and [OPERATIONS.md](docs/OPERATIONS.md)).

Immediately after the `Ack`, this node also delivers - as ordinary
`Publish` messages - whatever it currently holds in a matching replay
buffer, oldest first, so a client connecting after the fact can still
catch up on recent history (see
[ADR-0021](docs/adr/0021-message-replay-ring-buffer.md)). This only
happens the first time a connection registers interest in `filter`; a
no-op re-`Subscribe` above doesn't replay anything again. A wildcard
filter's replay buffer only starts accumulating once *that exact
filter string* has been subscribed to at least once - unlike a literal
topic, there's no way to pre-buffer for a pattern nobody has used yet
(see ADR-0022's Consequences). A replay-buffer delivery is held for
acknowledgement exactly like a live one when `ack: true`.

`group` joins the named consumer group for `filter` instead of
ordinary fan-out
([ADR-0042](docs/adr/0042-consumer-groups.md)): each `Publish`
matching `filter` goes to exactly one currently-live member of the
group, round-robin, rather than every subscriber. Two different
`group` names on the same `filter` are two independent groups, each
getting its own copy; *within* one group, only one member gets each
message. A group member gets no replay-buffer catch-up on join and no
lag recovery if it falls behind (unlike ordinary fan-out, ADR-0021/
ADR-0024) - live delivery only. `#[serde(default)]` on the
implementation side, same rolling-upgrade story as `ack`: a sender
that omits `group` entirely gets `None`, unchanged fan-out behavior.

Combined with `ack: true`, `group: Some(_)` means **work-queue**
delivery instead of plain fire-and-forget
([ADR-0048](docs/adr/0048-work-queue-redelivery-for-consumer-groups.md);
refused outright in earlier protocol versions, ADR-0042): a delivery
is provisional until *some* current member of the group - not
necessarily the one it was originally sent to - acks it, and
reclaimed (redelivered, via the same round-robin as any other
delivery) if that doesn't happen before the redelivery timeout,
exactly `DEFAULT_MAX_REDELIVERY_ATTEMPTS` times before it's given up
on. Whether a `(filter, group)` pair is work-queue or plain
fire-and-forget is decided once, by whichever `Subscribe` first
creates it - a later join with a different `ack` doesn't change it,
the same no-op-re-`Subscribe` rule above. `group: Some(_)` with no
`ack` remains exactly as strong a guarantee as ordinary
fire-and-forget, just applied once across the group instead of fanned
out to everyone.

`durable: true` makes this a durable subscription
([ADR-0046](docs/adr/0046-durable-subscriptions.md)): the node
remembers, per authenticated identity and topic, the `id` of the last
`Publish` delivered, so a later `durable: true` resubscribe from the
*same* identity resumes automatically from exactly that position -
catching up on everything published while it was gone, not just
whatever the in-memory replay buffer still happens to hold, and never
redelivering anything already delivered before it disconnected. The
very first `durable: true` subscribe for a given (identity, topic)
pair - nothing recorded yet - behaves exactly like an ordinary
subscribe: replay-buffer backlog only, no unconditional full history.
Four things must all hold, or the node refuses with an `Error`
instead of registering anything:

- This connection has a TLS client certificate (ADR-0038) - the
  authenticated identity it derives is what the position is keyed on.
  A plaintext connection, or TLS with no client certificate presented,
  has no such identity.
- `filter` is a literal topic, not a wildcard pattern - one recorded
  position can't stand in for several topics.
- `ack` and `group` are both unset - durable delivery is its own mode,
  not composable with either.
- The node was started with `--data-dir`
  ([ADR-0045](docs/adr/0045-on-disk-message-persistence-via-sqlite.md)) - durable
  subscriptions need the on-disk log both to record a position in and
  to catch up from.

`#[serde(default)]` on the implementation side, same rolling-upgrade
story as `ack`/`group`: a sender that omits `durable` entirely gets
`false`, unchanged behavior. The CLI's `subscribe --durable` requires
`--tls-cert`/`--tls-key` to be set, but does not pre-validate any of
the four requirements above client-side - the node's `Error` reply is
the single source of truth, exactly as `ack`/`group` already work.

### `Unsubscribe`

```
{"Unsubscribe": {"filter": <TopicFilter>}}
```

Removes interest in `filter` on this connection, acknowledged the same
way as `Subscribe`.

### `Ack`

```
{"Ack": {"in_reply_to": <MessageId>}}
```

Sent in either direction, unambiguous by which:

- **Node → client/peer**, in reply to a `Subscribe` or `Unsubscribe`,
  referencing the `id` of the request it's acknowledging. A client
  waiting on a `Subscribe`/`Unsubscribe` to take effect should wait
  for the `Ack` whose `in_reply_to` matches the request's `id` — other
  traffic (e.g. a `Publish` delivered on the same connection) can
  legitimately arrive first and should be skipped over, not treated as
  the reply.
- **Client → node**, acknowledging an individual `Publish` delivery on
  a subscription made with `ack: true` (see [`Subscribe`](#subscribe),
  [ADR-0041](docs/adr/0041-at-least-once-delivery-with-ack-based-redelivery.md)) -
  `in_reply_to` names the delivered `Publish` envelope's own `id`. A
  node never sends a `Subscribe`/`Unsubscribe` to a client to
  acknowledge, so there's no ambiguity between the two uses on either
  side of a connection.

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

### `StatusRequest` / `StatusReply`

```
"StatusRequest"
{"StatusReply": {
  "in_reply_to": <MessageId>,
  "node_id": <PeerId>,
  "listen_addr": <string | null>,
  "peers": [{"peer_id": <PeerId>, "listen_addr": <string | null>}, ...],
  "metrics": {
    "peers_connected": <u64>, "messages_published": <u64>,
    "forwarder_lag_total": <u64>, "topic_acl_rejections_total": <u64>,
    "metrics_auth_rejections_total": <u64>,
    "peer_topic_acl_rejections_total": <u64>,
    "replayed_messages_total": <u64>, "lag_recovered_total": <u64>,
    "topic_evictions_total": <u64>, "pattern_evictions_total": <u64>,
    "membership_evictions_total": <u64>,
    "peer_directory_evictions_total": <u64>
  }
}}
```

Requests the receiving node's current status. Answered on any
connection - client or peer link - with no ACL check
([ADR-0037](docs/adr/0037-status-command.md)): `peers` lists every
peer the node currently has an open link to (not the full history a
`--metrics-addr` scrape's `thothmesh_peers_connected` count is
capped-and-collapsed from), and `metrics` mirrors every counter that
endpoint's Prometheus text exposes (ADR-0013), just as typed fields
instead of text - the two field sets correspond 1:1, minus the
`thothmesh_` prefix. Unlike every other message kind, `StatusRequest` carries no fields at
all - a unit variant, which serde/ciborium's externally-tagged
representation encodes as a bare CBOR text string (`"StatusRequest"`),
not a one-entry map the way every field-carrying variant above is. An
implementation decoding the envelope's `kind` needs to accept a bare
string as well as a map with one entry.

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

- **Best-effort, in-memory only by default - bounded replay, not
  durability, unless `--data-dir` is configured.** Live delivery still
  reaches whoever is subscribed *at that moment*, on that node or
  reachable through the mesh; a subscriber connecting afterward is
  additionally replayed each topic's recent backlog (a bounded
  in-memory ring buffer, capacity `DEFAULT_REPLAY_BUFFER_CAPACITY`,
  currently 1024, per topic) - see
  [ADR-0021](docs/adr/0021-message-replay-ring-buffer.md). A subscriber
  connecting after a topic's backlog has rolled past that capacity
  still misses whatever fell off the oldest end - unless the node was
  started with `--data-dir`
  ([ADR-0045](docs/adr/0045-on-disk-message-persistence-via-sqlite.md)), in which
  case every publish also survives on disk across a restart, and a
  `durable: true` subscribe (see [`Subscribe`](#subscribe),
  [ADR-0046](docs/adr/0046-durable-subscriptions.md)) can catch up from
  disk past what the in-memory buffer alone would still hold.
- **A slow subscriber can miss messages - but recovers what it can.**
  Delivery to each subscriber goes through a bounded channel; a
  subscriber that falls too far behind has the gap recovered from the
  same per-topic replay buffer a late subscriber catches up from
  (ADR-0021), rather than the sender blocking or the connection
  failing - see [ADR-0024](docs/adr/0024-lagged-forwarder-recovery.md).
  This is bounded, not guaranteed: a gap wider than the buffer's
  headroom above the live channel's own capacity still drops the
  oldest messages in it silently. There's no wire-level signal to a
  client either way - recovered messages arrive as ordinary `Publish`
  messages, indistinguishable from a live delivery.
- **De-duplication, not exactly-once.** An envelope crossing more
  than one hop of a cyclic mesh keeps its original `MessageId`
  end-to-end, and each node remembers a bounded number of recently
  seen `MessageId`s to drop a duplicate rather than deliver (or
  re-forward) it twice. This prevents loops and double-delivery
  within that memory window, not for the life of the mesh — a very
  old repeated `MessageId` after enough other traffic has gone by
  could in principle be treated as new again.
- **No delivery confirmation for `Publish` by default - opt-in per
  subscription.** Nothing acknowledges receipt of a `Publish` by the
  node it was sent to - a publisher never learns whether anyone
  actually got it, with or without `ack`. What `ack: true` on a
  `Subscribe` adds is at-least-once delivery *to that one
  subscription*: the node holds each delivery until the subscriber
  sends back an `Ack`, redelivering (the same envelope, same `id`) on
  a timeout, up to a bounded number of attempts, after which it gives
  up (see [ADR-0041](docs/adr/0041-at-least-once-delivery-with-ack-based-redelivery.md)).
  This is a single-hop guarantee between a node and its direct
  subscriber - interest propagated across a peer link (ADR-0011) is
  always `ack: false`, so it doesn't extend across a multi-hop
  forward. Without `ack: true`, delivery remains exactly as before:
  fire-and-forget, once, best-effort.
- **A connection with overlapping subscriptions gets more than one
  delivery.** A literal `Subscribe` and a wildcard `Subscribe` that
  both happen to match the same published topic are independent
  subscriptions (ADR-0022) — a connection holding both receives the
  matching `Publish` once per subscription, not deduplicated down to
  one.
- **A consumer group has no replay buffer and no lag recovery.**
  `group` (ADR-0042) picks exactly one live member per message instead
  of fan-out, but that member gets it only if it's live and keeping up
  *at that moment* - no backlog on joining (ADR-0021 doesn't apply to
  a group), and a member that falls behind just misses what it missed,
  rather than recovering it the way an ordinary fan-out forwarder does
  (ADR-0024). A plain `group: Some(_)` (no `ack`) is exactly as strong
  a guarantee as ordinary fire-and-forget, applied once across the
  group rather than fanned out to everyone in it - `group` combined
  with `ack: true` is stronger (work-queue delivery, reclaimable by
  any live member until acked, see [`Subscribe`](#subscribe) and
  [ADR-0048](docs/adr/0048-work-queue-redelivery-for-consumer-groups.md)),
  but still has no backlog/lag-recovery of its own: a lease can only
  ever be reclaimed by a member that's live *right now*, the same as
  the very first delivery attempt.
- **A retained message is per node, not per mesh.** `retain: true`
  (ADR-0043) gives a topic a last-value message that a later
  subscriber gets immediately, but each node only retains what it
  actually received: a node that gains interest in a topic *after* a
  `retain: true` publish landed on another node does not have that
  earlier value back-filled to it. A retained value can also be lost
  to the same topic-map eviction the replay buffer is subject to
  (ADR-0025) - a retained topic with no live subscriber, on a node
  churning through more than 4096 distinct topics.
- **An unconsumed message is dropped by default - dead-lettering is
  opt-in.** A message aged past `--persisted-message-ttl-secs` (only meaningful
  alongside `--data-dir`), or an `ack: true` delivery that exhausts
  every redelivery attempt (ADR-0041), is simply gone unless the node
  is run with `--dead-letter-topic` - in which case it's republished,
  as an ordinary `Publish` with a fresh `id`, to
  `<dead-letter-topic>.<original topic>` (see
  [ADR-0047](docs/adr/0047-message-ttl-and-dead-lettering.md)). No
  wire-protocol change either way - a dead-lettered message is
  indistinguishable from any other `Publish`.
