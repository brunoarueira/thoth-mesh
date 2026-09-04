# Operating thoth-mesh

A walkthrough for running a small mesh end to end: one node, then two
federated nodes, publishing and subscribing across them, and scraping
metrics. This is the "follow these steps" doc; for *why* things work
this way, see the [README](../README.md), the [ADRs](adr/), and
[`PROTOCOL.md`](../PROTOCOL.md).

## Build / install

From a checkout of this repo:

```sh
cargo build --release --workspace
```

This produces two binaries under `target/release/`:

- `thoth-mesh-node` — the daemon that runs a mesh node.
- `thoth-mesh` — the CLI client, for publishing, subscribing, and
  (later) admin.

Or install the published releases directly, without a checkout:

```sh
cargo install thoth-mesh-node
cargo install thoth-mesh-cli   # installs the `thoth-mesh` binary
```

For a container image or a systemd service, see [Docker](#docker) and
[systemd](#systemd) below. The examples in this walkthrough use
`cargo run -p <crate> --` in place of the built binary path; drop the
`--release` flag while iterating, add it back for anything you're
leaving running for a while.

### Shell completions

`thoth-mesh completions <shell>` prints a tab-completion script for
`bash`, `zsh`, `fish`, `elvish`, or `powershell` to stdout - install it
the same way you would for any other CLI (see ADR-0036):

```sh
thoth-mesh completions bash | sudo tee /etc/bash_completion.d/thoth-mesh
thoth-mesh completions zsh > "${fpath[1]}/_thoth-mesh"
thoth-mesh completions fish > ~/.config/fish/completions/thoth-mesh.fish
```

## Docker

A multi-stage `Dockerfile` at the repo root builds both binaries into
a minimal, non-root runtime image (see
[ADR-0023](adr/0023-packaging-docker-and-systemd.md)):

```sh
docker build -t thoth-mesh .
```

Run it:

```sh
docker run -d --name thoth-mesh-node -p 49500:49500 thoth-mesh
```

The image's default `CMD` binds to `0.0.0.0:49500`, not
`127.0.0.1:49500` — a node bound to loopback inside the container
would be unreachable through `-p`, from outside the container's
network namespace entirely. Append flags to override or extend it
(this replaces `CMD` outright, same as any Docker image):

```sh
docker run -d --name thoth-mesh-node -p 49500:49500 -p 9090:9090 \
  -v "$(pwd)/certs:/certs:ro" \
  thoth-mesh \
  --addr 0.0.0.0:49500 --metrics-addr 0.0.0.0:9090 \
  --peer other-node:49500 \
  --tls-cert /certs/node-cert.pem --tls-key /certs/node-key.pem --tls-ca /certs/ca-cert.pem
```

The runtime image is `gcr.io/distroless/cc-debian12` — no shell, so
there's no `docker exec -it ... sh`, but the CLI is in the same image
and can still be invoked directly by path:

```sh
docker exec thoth-mesh-node /usr/local/bin/thoth-mesh \
  --addr 127.0.0.1:49500 publish demo.topic "hello from inside"
```

## systemd

An example unit, `packaging/thoth-mesh-node.service`, and an example
`EnvironmentFile`, `packaging/thoth-mesh-node.env.example` (see
[ADR-0023](adr/0023-packaging-docker-and-systemd.md)). Not installed
automatically — copy both, adjust the binary path in `ExecStart=` if
it isn't `/usr/local/bin`, and edit the env file for your actual
flags:

```sh
sudo cp packaging/thoth-mesh-node.service /etc/systemd/system/
sudo mkdir -p /etc/thoth-mesh
sudo cp packaging/thoth-mesh-node.env.example /etc/thoth-mesh/node.env
sudo "$EDITOR" /etc/thoth-mesh/node.env   # at minimum, set --addr

sudo systemctl daemon-reload
sudo systemctl enable --now thoth-mesh-node
```

`journalctl -u thoth-mesh-node -f` follows its logs, same `tracing`
output as running it directly (see [Logging](#logging)).

The unit runs as `DynamicUser=yes` — an ephemeral, unprivileged user
systemd allocates and tears down with the unit's lifetime, not a
manually-created account — plus a standard hardening bundle
(`ProtectSystem=strict`, `NoNewPrivileges=yes`, and friends; see the
unit file's own comments for the full list and why each is safe for
this daemon specifically). Since `thoth-mesh-node` takes CLI flags,
not environment variables, for its configuration, the env file sets a
single `NODE_ARGS` that `ExecStart=` word-splits the same way a shell
would — see the unit and env-file comments for why that specific
`$NODE_ARGS` (unquoted) syntax matters.

If you bind `--addr`/`--metrics-addr` to a port below 1024, the
default `CapabilityBoundingSet=` (empty) refuses to start — the unit
file comments where to add `CAP_NET_BIND_SERVICE` instead.

## Single-node quickstart

Start a node. By default it listens on `127.0.0.1:49500`
(`thoth_mesh_core::DEFAULT_ADDR`, shared by the node and the CLI so
their defaults can't drift apart):

```sh
cargo run -p thoth-mesh-node --
```

In a second terminal, subscribe to a topic:

```sh
cargo run -p thoth-mesh-cli -- subscribe demo.topic
```

In a third terminal, publish to it:

```sh
cargo run -p thoth-mesh-cli -- publish demo.topic "hello mesh"
```

The subscriber terminal prints:

```
Subscribed to demo.topic. Waiting for messages (Ctrl-C to stop)...
[demo.topic] hello mesh
```

`subscribe` runs until interrupted (Ctrl-C) and accepts more than one
filter in one invocation (`subscribe demo.topic other.topic`), sharing
a single connection - each printed line names the concrete topic it
arrived on, so it's clear which filter matched (see ADR-0033).
`publish` sends one payload and exits — there's no reply or delivery
confirmation (see [`PROTOCOL.md`](../PROTOCOL.md#delivery-semantics)).
The payload is UTF-8 text on the command line by default, or pass `-`
to read it as raw bytes from stdin instead - the way to send a binary
payload, or one too large for a CLI argument:

```sh
cat image.png | thoth-mesh publish images.new -
```

`subscribe --output raw` is the matching binary-safe read side: every
delivered payload's exact bytes go to stdout, with no topic label or
separator between messages, so it's safe to redirect straight to a
file. The usual `Subscribed to ...` banner and a per-message `[topic]
N bytes` note move to stderr in this mode, so they stay visible on the
terminal without corrupting the captured file (`--output text`, the
default, is unchanged - see ADR-0035).

Every node and CLI invocation picks a fresh random `PeerId` on
startup — there's no persistent identity across restarts.

`--addr` and the `--tls-*` flags don't have to be repeated on every
invocation: a TOML config file at the conventional per-OS location
(`~/.config/thoth-mesh/config.toml` on Linux) supplies defaults for
them, overridden by whichever of these are actually given as flags
(see ADR-0034):

```toml
addr = "127.0.0.1:49501"
tls_ca = "/path/to/ca-cert.pem"
```

Pass `--config <path>` to use a different file instead of the
conventional location - handy for more than one saved profile. A
config file is entirely optional either way: nothing changes for an
invocation that doesn't have one.

## Multi-node / federation quickstart

Start a second node, telling it about the first via `--peer`. Each
node needs its own listen address:

```sh
# node A (as above, still running)
cargo run -p thoth-mesh-node -- --addr 127.0.0.1:49500

# node B, dials node A on startup
cargo run -p thoth-mesh-node -- --addr 127.0.0.1:49501 --peer 127.0.0.1:49500
```

`--peer` is repeatable, for dialing more than one seed peer. Once B
has dialed A, subscribe on one node and publish on the other:

```sh
# subscribe against B
cargo run -p thoth-mesh-cli -- --addr 127.0.0.1:49501 subscribe demo.topic

# publish against A
cargo run -p thoth-mesh-cli -- --addr 127.0.0.1:49500 publish demo.topic "hello mesh"
```

The subscriber on B receives the message even though it was published
on A — B's aggregate topic interest was propagated to A when the peer
link came up (ADR-0011). This works transitively across more than two
nodes; the mesh doesn't need to be fully connected, only reachable.

If a peer link drops, the dialing side retries with backoff rather
than giving up — there's no need to manually reconnect. `--peer` is
only needed to join the mesh in the first place: once B has dialed A,
each side also gossips the other peers it knows about, so a node
learns about (and auto-dials) peers-of-peers it was never directly
configured with (see [ADR-0015](adr/0015-dynamic-peer-discovery-gossip.md)).
Node C, started with only `--peer` pointing at B, ends up directly
connected to A too, without ever being told A's address.

## Metrics

A node opens no metrics port by default. Pass `--metrics-addr` to
enable one:

```sh
cargo run -p thoth-mesh-node -- --metrics-addr 127.0.0.1:9090
```

Any HTTP request to that address, on any path, gets back the current
render — there's no routing, so `/metrics` is a convention, not a
requirement enforced by the server:

```sh
curl -s http://127.0.0.1:9090/metrics
```

```
# TYPE thothmesh_peers_connected gauge
thothmesh_peers_connected 1
# TYPE thothmesh_messages_published_total counter
thothmesh_messages_published_total 1
# TYPE thothmesh_forwarder_lag_total counter
thothmesh_forwarder_lag_total 0
# TYPE thothmesh_topic_acl_rejections_total counter
thothmesh_topic_acl_rejections_total 0
# TYPE thothmesh_metrics_auth_rejections_total counter
thothmesh_metrics_auth_rejections_total 0
# TYPE thothmesh_peer_topic_acl_rejections_total counter
thothmesh_peer_topic_acl_rejections_total 0
# TYPE thothmesh_replayed_messages_total counter
thothmesh_replayed_messages_total 0
# TYPE thothmesh_lag_recovered_total counter
thothmesh_lag_recovered_total 0
# TYPE thothmesh_topic_evictions_total counter
thothmesh_topic_evictions_total 0
# TYPE thothmesh_pattern_evictions_total counter
thothmesh_pattern_evictions_total 0
# TYPE thothmesh_membership_evictions_total counter
thothmesh_membership_evictions_total 0
# TYPE thothmesh_peer_directory_evictions_total counter
thothmesh_peer_directory_evictions_total 0
```

Twelve metrics today (ADR-0013, plus `topic_acl_rejections_total`
added by ADR-0018, `metrics_auth_rejections_total` added by ADR-0019,
`peer_topic_acl_rejections_total` added by ADR-0020,
`replayed_messages_total` added by ADR-0021, `lag_recovered_total`
added by ADR-0024, and `topic_evictions_total`/`pattern_evictions_total`/
`membership_evictions_total`/`peer_directory_evictions_total` added by
ADR-0025):

| Metric | Type | Meaning |
| --- | --- | --- |
| `thothmesh_peers_connected` | gauge | Peer links currently up (not client connections). |
| `thothmesh_messages_published_total` | counter | `Publish` messages this node has processed since startup. |
| `thothmesh_forwarder_lag_total` | counter | Envelopes a subscriber's delivery channel skipped because it fell behind (see [`PROTOCOL.md`](../PROTOCOL.md#delivery-semantics)). Nonzero here means a consumer is too slow, not that the node is misbehaving - compare against `lag_recovered_total` to see how much of this was actually recovered rather than lost. |
| `thothmesh_topic_acl_rejections_total` | counter | `Subscribe`/`Publish` attempts refused by a [`--topic-acl`](#per-topic-client-authorization). Zero unless one is configured. |
| `thothmesh_metrics_auth_rejections_total` | counter | Scrapes refused by a [`--metrics-token-file`](#metrics-authentication). Zero unless one is configured. |
| `thothmesh_peer_topic_acl_rejections_total` | counter | `Subscribe`/`Publish` attempts from a peer link refused by a [`--peer-topic-acl`](#peer-scoped-topic-authorization). Zero unless one is configured. |
| `thothmesh_replayed_messages_total` | counter | Envelopes delivered to a newly-subscribed connection from a topic's [replay buffer](#message-replay) rather than live. Zero on a node where every subscriber connects before any publish it cares about. |
| `thothmesh_lag_recovered_total` | counter | Envelopes recovered from a topic's replay buffer for a forwarder that fell behind mid-stream (see [Lagged-forwarder recovery](#lagged-forwarder-recovery)), rather than lost. |
| `thothmesh_topic_evictions_total` | counter | Exact-match topics reclaimed for sitting over capacity with no live subscriber (see [Bounded memory footprint](#bounded-memory-footprint)). Zero on a node whose distinct-topic count over its lifetime stays under the cap. |
| `thothmesh_pattern_evictions_total` | counter | Same as `topic_evictions_total`, for wildcard pattern subscriptions - tracked separately since they're two independent caps. |
| `thothmesh_membership_evictions_total` | counter | Disconnected peers this node stopped remembering an address for, once over the cap (see [Bounded memory footprint](#bounded-memory-footprint)). A currently-*connected* peer is never counted here. |
| `thothmesh_peer_directory_evictions_total` | counter | Peers this node stops remembering as dialable, once over the cap - distinct from `membership_evictions_total`: this is every peer ever learned about (gossip or handshake), not just ones this node itself connected to. |

Point a Prometheus `scrape_configs` target at `--metrics-addr` the
same way you would any other exporter; there's no special
`/metrics`-only handling to configure around.

### Metrics authentication

By default, anyone who can reach `--metrics-addr` gets the render —
this port is plain HTTP and deliberately outside the TLS layer
described [below](#tls) (ADR-0016 scoped TLS to the client/peer port
only). `--metrics-token-file` closes that gap with a shared-secret
bearer token (see [ADR-0019](adr/0019-metrics-endpoint-authentication.md)):

```sh
echo -n "a-long-random-secret" > metrics-token.txt
cargo run -p thoth-mesh-node -- --addr 127.0.0.1:49500 \
  --metrics-addr 127.0.0.1:9090 --metrics-token-file metrics-token.txt
```

A scrape now needs a matching `Authorization` header:

```sh
curl -s http://127.0.0.1:9090/metrics                                  # 401 Unauthorized
curl -s -H "Authorization: Bearer a-long-random-secret" \
  http://127.0.0.1:9090/metrics                                        # 200 OK, the render
```

Point Prometheus's `scrape_configs` at the same token with its
built-in `authorization`/`bearer_token` scrape option — no extra
client-side TLS setup needed for this port. Requires
`--metrics-addr` (a startup error otherwise, since a token with no
metrics port to guard is a no-op). Off by default — with no
`--metrics-token-file` given, any connection to `--metrics-addr` gets
the render, same as before this flag existed. Unlike TLS-based auth on
the main port, this is one shared secret, not a per-scraper identity —
anyone with the token can scrape, and there's no way to revoke just
one holder of it.

## TLS

Off by default — every connection (client or peer) is plaintext
unless you opt in. Enabling it needs a CA and a cert/key per node,
signed by that CA (see [ADR-0016](adr/0016-tls-transport-security.md)
for the trust model). thoth-mesh doesn't generate certs itself;
`openssl` does the job in a few commands:

```sh
# One CA for the whole mesh.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout ca-key.pem -out ca-cert.pem -days 3650 -subj "/CN=my-mesh-ca"

# One cert per node, signed by that CA. subjectAltName has to match
# how peers/clients will actually reach it (an IP here, since this
# quickstart stays on loopback - use DNS:your.hostname for a real
# deployment).
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout node-a-key.pem -out node-a.csr -subj "/CN=node-a"
openssl x509 -req -in node-a.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
  -out node-a-cert.pem -days 825 -extfile <(printf "subjectAltName=IP:127.0.0.1")

# Repeat for node-b (and any other node) with its own key/CSR/cert.
```

Start each node with all three flags — partial specification (e.g.
`--tls-cert` without `--tls-key`) is a startup error, not a silent
fallback to plaintext:

```sh
cargo run -p thoth-mesh-node -- --addr 127.0.0.1:49500 \
  --tls-cert node-a-cert.pem --tls-key node-a-key.pem --tls-ca ca-cert.pem

cargo run -p thoth-mesh-node -- --addr 127.0.0.1:49501 --peer 127.0.0.1:49500 \
  --tls-cert node-b-cert.pem --tls-key node-b-key.pem --tls-ca ca-cert.pem
```

A node dialing another node always presents its own cert (a peer
dialing a peer identifies itself) and always verifies the far end's,
so both sides above need real, CA-signed identities — there's no
"TLS with only one side configured" for a peer link.

The CLI only needs `--tls-ca` to talk to a TLS-enabled node — it
verifies the node's cert, same as any TLS client, but doesn't need to
prove its own identity unless a [topic ACL](#per-topic-client-authorization)
on the node it's talking to actually checks for one:

```sh
cargo run -p thoth-mesh-cli -- --addr 127.0.0.1:49500 --tls-ca ca-cert.pem \
  subscribe demo.topic
```

To also present a client certificate (so a `--topic-acl` entry can
name this specific client rather than falling back to `anonymous`),
add `--tls-cert`/`--tls-key`, the same flags the node itself uses:

```sh
cargo run -p thoth-mesh-cli -- --addr 127.0.0.1:49500 --tls-ca ca-cert.pem \
  --tls-cert client-cert.pem --tls-key client-key.pem \
  subscribe demo.topic
```

### Peer allowlist

TLS alone doesn't restrict *which* certificates get to link as a peer
— by default, any connection presenting a cert this node's CA
recognizes (or none at all, on the accept side) is accepted as a peer
the moment it sends `Hello`. `--allow-peer` closes that gap (see
[ADR-0017](adr/0017-peer-allowlist-via-tls-fingerprint.md)): repeat it
once per peer certificate this node should accept a link from,
identified by its SHA-256 fingerprint. Off by default — with no
`--allow-peer` given, every peer link is allowed, same as before this
flag existed.

```sh
# Get node B's fingerprint, to allow it on node A.
openssl x509 -in node-b-cert.pem -noout -fingerprint -sha256
# sha256 Fingerprint=3F:08:CA:D2:92:03:BB:AA:B8:DD:92:32:33:8B:BD:0E:F8:E6:D9:E4:70:27:87:4E:51:D3:24:6E:CC:1A:92:10
```

`--allow-peer` accepts that output verbatim, pasted in as-is:

```sh
cargo run -p thoth-mesh-node -- --addr 127.0.0.1:49500 \
  --tls-cert node-a-cert.pem --tls-key node-a-key.pem --tls-ca ca-cert.pem \
  --allow-peer "sha256 Fingerprint=3F:08:CA:D2:92:03:BB:AA:B8:DD:92:32:33:8B:BD:0E:F8:E6:D9:E4:70:27:87:4E:51:D3:24:6E:CC:1A:92:10"
```

Requires `--tls-cert`/`--tls-key`/`--tls-ca` too (a startup error
otherwise) — there's no verified identity to check without TLS.
Enforcement is symmetric: it applies whether this node dialed the
other side or accepted the connection, so a seed peer this node dials
via `--peer` also needs to be on the allowlist if one is configured.
A rejected link gets an `Error` envelope explaining why, then the
connection closes without completing the handshake — it never
appears in this node's membership. Note that a fingerprint is tied to
the exact certificate, not the CA that signed it — reissuing a node's
cert (even from the same CA) means updating every allowlist entry
that named its old fingerprint.

### Per-topic client authorization

`--allow-peer` controls who gets to link as a peer at all; it says
nothing about what a *client* already connected is allowed to
`Subscribe`/`Publish` to. `--topic-acl` closes that gap (see
[ADR-0018](adr/0018-per-topic-client-authorization.md)): repeat it
once per `<principal>|<action>|<topic>` permission this node should
grant. Off by default — with no `--topic-acl` given, any client can
publish/subscribe to anything, same as before this flag existed; given
at least once, only the combinations listed are allowed (everything
else on a connection not already known to be a peer link is refused).

- `<principal>`: a client certificate's SHA-256 fingerprint (same
  tolerant formats as `--allow-peer`), or the literal `anonymous` for
  a client that didn't present one — which is every client when TLS
  is off entirely, and still an option even with TLS on (ADR-0016
  never requires a client certificate).
- `<action>`: `pub`, `sub`, or `pubsub` for both.
- `<topic>`: an exact topic name — no wildcards. A client's wildcard
  `Subscribe` (see [Wildcard topic filters](#wildcard-topic-filters))
  is refused outright once any `--topic-acl` entry exists, regardless
  of `<topic>`.

```sh
# Anyone (no certificate needed) may subscribe to a public status
# topic; only a specific, identified client may publish sensor data.
cargo run -p thoth-mesh-node -- --addr 127.0.0.1:49500 \
  --tls-cert node-a-cert.pem --tls-key node-a-key.pem --tls-ca ca-cert.pem \
  --topic-acl "anonymous|sub|status.public" \
  --topic-acl "3F:08:CA:D2:92:03:BB:AA:B8:DD:92:32:33:8B:BD:0E:F8:E6:D9:E4:70:27:87:4E:51:D3:24:6E:CC:1A:92:10|pub|sensors.data"
```

Unlike a rejected peer link, a topic ACL rejection doesn't close the
connection — a client denied on one topic may be entitled to others.
It gets an `Error` in place of the `Subscribe`'s usual `Ack` (or, for
a `Publish`, in place of the silence a fire-and-forget message
normally gets), naming the rejected request, and nothing else about
the connection changes. Every rejection also bumps
`thothmesh_topic_acl_rejections_total` (see [Metrics](#metrics)).

Peer traffic (interest propagation over an established peer link) is
never checked against a `--topic-acl` — it only applies to connections
not (yet) known to be peer links. See
[Peer-scoped topic authorization](#peer-scoped-topic-authorization) for
the equivalent applying to a peer link itself.

### Peer-scoped topic authorization

`--topic-acl` never applies to a peer link (a peer relays on behalf of
however many clients and further peers sit behind it, not for itself);
`--peer-topic-acl` closes that gap instead (see
[ADR-0020](adr/0020-peer-scoped-topic-restriction.md)). Same shape,
same parser, same `anonymous`-or-fingerprint principal, checked
against a peer's own certificate instead of a client's — but a
completely independent list: a peer link is never checked against
`--topic-acl`, and a client is never checked against `--peer-topic-acl`.

- `<action>` means something slightly different here: `sub` permits
  the peer link to be forwarded the topic (its own `Subscribe`
  request for it is honored); `pub` permits it to send the topic to
  this node (its `Publish` is accepted rather than refused).
- Off by default — with no `--peer-topic-acl` given, every peer link
  can carry anything, same as before this flag existed.

```sh
# The peer link at this fingerprint may only carry sensor readings -
# it can neither ask for anything else forwarded to it, nor publish
# anything else through this node.
cargo run -p thoth-mesh-node -- --addr 127.0.0.1:49500 \
  --tls-cert node-a-cert.pem --tls-key node-a-key.pem --tls-ca ca-cert.pem \
  --peer-topic-acl "3F:08:CA:D2:92:03:BB:AA:B8:DD:92:32:33:8B:BD:0E:F8:E6:D9:E4:70:27:87:4E:51:D3:24:6E:CC:1A:92:10|pubsub|sensors.data"
```

Rejection has the same shape as `--topic-acl`'s: an `Error` in place
of the usual `Ack`/silence, the connection stays open, and every
rejection bumps `thothmesh_peer_topic_acl_rejections_total` (see
[Metrics](#metrics)) — kept separate from
`thothmesh_topic_acl_rejections_total` so a misbehaving peer is
distinguishable from a misbehaving client at a glance.

## Message replay

Every topic keeps a bounded, in-memory ring buffer of its most
recently published envelopes — 1024 by default, not currently
configurable via a flag (see
[ADR-0021](adr/0021-message-replay-ring-buffer.md)). A connection
subscribing to a topic for the first time is replayed that buffer,
oldest first, immediately after its `Subscribe` is acknowledged — no
extra step needed on the client's part, and no wire-protocol change:
replayed envelopes arrive as ordinary `Publish` messages. This applies
equally to an ordinary client and to a peer link catching up on
interest via `--peer` (a peer's own `Subscribe`, sent to catch it up on
this node's aggregate interest, spawns a forwarder exactly the way a
client's does).

This is **not** durability across a restart — the buffer is in-memory
only and empties on every node restart, the same as everything else
`Broker` tracks. A subscriber connecting after a topic's buffer has
rolled past its capacity still misses whatever fell off the oldest
end, silently — the same posture `PROTOCOL.md`'s
[Delivery semantics](../PROTOCOL.md#delivery-semantics) already
accepts for a live subscriber that falls behind.
`thothmesh_replayed_messages_total` (see [Metrics](#metrics)) counts
how many envelopes have gone out via replay rather than live delivery.

## Lagged-forwarder recovery

A subscriber that's already receiving live deliveries can still fall
behind mid-stream if it (or the network to it) is slow enough for long
enough — the node reports this to itself as a `Lagged` error on that
subscriber's internal channel. Rather than just dropping whatever was
missed, the node recovers as much of the gap as it can from the same
per-topic replay buffer described above (see
[ADR-0024](adr/0024-lagged-forwarder-recovery.md)) — no client-visible
difference from an ordinary `Publish` either way.

This only works because the replay buffer deliberately holds *more*
history than the live delivery channel's own capacity (256) - a
buffer sized the same as that channel would already have evicted
whatever a lag event skipped, by the time recovery could look for it.
The gap between the two (1024 vs. 256) is how much of a burst a
lagged subscriber can fully recover from; a gap wider than that still
permanently loses whatever fell off the buffer's oldest end, same as
before this existed. `thothmesh_lag_recovered_total` (see
[Metrics](#metrics)) counts how many envelopes were actually recovered
this way — comparing it against `thothmesh_forwarder_lag_total` shows
how much of a node's reported lag is being absorbed versus genuinely
lost.

## Bounded memory footprint

A node's live mesh state is entirely in-memory, with no persistence
layer (see [Message replay](#message-replay) above for the one form of
history that does get kept) - several of the structures holding it are
audited and capped in [ADR-0025](adr/0025-bound-per-node-memory-footprint.md)
so a long-running node's memory use doesn't grow without limit.
Currently-subscribed topics and patterns are never bounded themselves
- that's live, wanted state - but an exact-match topic or wildcard
pattern with **no subscriber left** is capped at 4096 tracked entries;
once over, the least-recently-touched one with nothing currently
listening is reclaimed. `thothmesh_topic_evictions_total`/
`thothmesh_pattern_evictions_total` (see [Metrics](#metrics)) count
this happening.

Peer membership is bounded the same way: a currently-*connected*
peer's entry is never a candidate - it's live, wanted state, already
bounded by how many real sockets this node can hold open - but a
**disconnected** peer (kept only as a last-known address in case it
reconnects) is capped at 4096 entries, oldest reclaimed first once
over. `thothmesh_membership_evictions_total` counts this happening.
This matters more than it might look: a peer's ID is a fresh random
value generated at every node startup (`PROTOCOL.md`), so a mesh whose
actual size never changes still sees a new distinct ID every time any
node in it restarts - without this cap, a long-running node would
remember every such identity forever.

The same shape covers every peer address this node has ever learned,
whether from a direct handshake or from gossip about a peer of a peer
(the `discover` registry behind `--peer` auto-dialing, see
[ADR-0015](adr/0015-dynamic-peer-discovery-gossip.md)) - also capped
at 4096, oldest reclaimed first, but refreshed every time a peer is
re-recorded (a repeat gossip mention or handshake): a peer that keeps
getting talked about stays fresh, one that's actually gone ages toward
eviction. `thothmesh_peer_directory_evictions_total` counts this
happening - distinct from `thothmesh_membership_evictions_total`,
since this registry has no connected/disconnected concept at all and
tracks every peer ever heard of, not just ones this node itself
connected to.

None of these caps are configurable via a flag in v1, consistent with
every other capacity in this codebase (the replay buffer, the
duplicate-message cache).

## Bounded dial concurrency

A node dials every seed peer (`--peer`) and every gossip-discovered
peer address it decides to auto-dial as an independent background
task, with no limit on how many run at once by default - a fresh
N-node bootstrap, or a partition healing and re-announcing many peers
at once, could otherwise open dozens or hundreds of TCP connect and
TLS handshake attempts in the same instant. [ADR-0026](adr/0026-bound-concurrent-peer-dials.md)
bounds this: at most 16 dials (the connect-and-handshake phase only,
never an established link's lifetime) run concurrently per node,
shared across seed peers and gossip-discovered addresses alike. A
dial beyond that simply waits its turn rather than firing immediately
- reconnect backoff is unaffected, since it's driven by how long an
attempt that actually ran took, not by time spent queued. This cap
isn't configurable via a flag in v1 either.

Bounding concurrency alone isn't enough if a stalled peer can hold its
dial slot forever: a peer that accepts the TCP connection but never
finishes TLS, or completes TLS but never sends its `Hello`, would
otherwise tie up one of the 16 slots indefinitely. [ADR-0027](adr/0027-dial-connect-and-handshake-timeout.md)
bounds the connect-and-handshake phase itself to 10 seconds - generous
for a real network path, but well short of the OS's own TCP connect
timeout - after which the attempt is abandoned and its slot freed, the
same as any other failed dial. Not configurable via a flag in v1.

## Wildcard topic filters

A `Subscribe` can name a pattern instead of an exact topic (see
[ADR-0022](adr/0022-wildcard-topic-filters.md)): `+` matches exactly
one `.`-delimited segment, and a trailing `#` matches zero or more
remaining segments. `weather.+` matches `weather.updates` and
`weather.forecast` but not `weather` itself; `weather.#` matches all
three. This applies equally to a client and to a peer link's own
interest.

```sh
# Subscribe to any single-segment child of weather.*
cargo run -p thoth-mesh-cli -- subscribe "weather.+"
# Subscribe to weather.* and everything under it, any number of levels deep
cargo run -p thoth-mesh-cli -- subscribe "weather.#"
```

A subscription holding both a literal and an overlapping wildcard
filter gets a publish delivered twice, once per subscription — they're
independent, not deduplicated against each other.

**Wildcard subscribes are refused wherever a topic ACL applies.** If
`--topic-acl` (or, for a peer link, `--peer-topic-acl`) is configured
at all, a wildcard `Subscribe` gets an `Error` unconditionally,
regardless of what it would actually expand to — neither ACL
mechanism understands patterns, and this codebase doesn't attempt to
infer whether a pattern's expansion would be covered by an ACL entry.
A literal subscribe is checked exactly as it always has been. If you
need both a topic ACL and wildcard subscriptions, the wildcard side
isn't available yet — subscribe to each concrete topic instead.

## Logging

Both binaries use `tracing`. Control verbosity with `RUST_LOG`
(a standard `tracing_subscriber::EnvFilter` directive) or, for the
node only, `--log-level`:

```sh
RUST_LOG=debug cargo run -p thoth-mesh-node --
# or, equivalently, when RUST_LOG isn't set:
cargo run -p thoth-mesh-node -- --log-level debug
```

`RUST_LOG` always wins when it's set, even if it fails to parse (the
node falls back to `--log-level` rather than silently ignoring the
environment). `--log-level` defaults to `info`. Both accept either a
bare level (`trace` / `debug` / `info` / `warn` / `error`) or a full
`EnvFilter` directive for finer control, e.g.:

```sh
RUST_LOG=thoth_mesh_node=debug,thoth_mesh=trace cargo run -p thoth-mesh-node --
```

The CLI (`thoth-mesh`) doesn't currently expose a log-level flag —
its output is just what it prints for the command you ran.

## Benchmarking

`bench_mesh` (see [ADR-0030](adr/0030-mesh-benchmark-suite.md)) measures
message throughput and end-to-end latency across a sweep of mesh hop
counts — 0 (a single node, no peer link at all) through 8, chaining
that many extra nodes the same way the multi-hop integration test
does. Run it with:

```sh
cargo run --release --example bench_mesh -p thoth-mesh-node
```

`--release` matters here — this workspace's release profile (LTO,
codegen-units = 1) meaningfully changes the numbers a debug build
wouldn't represent honestly. A captured run — Apple M2, 16GB RAM,
macOS 26.4.1, one sample per row, not an average (see the ADR for why
this stays a manually-run, point-in-time report rather than a
CI-tracked regression suite). Disclosed because it's the industry norm
for a benchmark result to say what it ran on — everything here is over
loopback, so the numbers are really measuring per-message overhead
(syscalls, scheduling, CBOR encode/decode) more than raw compute, and
that profile is sensitive to the OS's networking stack (kqueue here,
epoll on Linux — where CI, and most production deployments, actually
run) at least as much as to the hardware. Don't read these as
portable to other machines, operating systems, or even a quieter run
of this same one:

```
thoth-mesh bench_mesh - 5000 messages, 64-byte payload, per hop count

 hops   delivered    msgs/sec   min (ms)   p50 (ms)   p95 (ms)   max (ms)
    0   5000/5000      111997          0          1          2          3
    1   3976/5000        n/a*          0         17         20         21
    2   5000/5000       45373          2         22         25         26
    4   5000/5000       20540          2         56         99        101
    8   5000/5000        5980          4        185        348        369

* msgs/sec omitted: this row's elapsed window only covers messages that arrived
before the pipeline gave up on the rest - not a rate comparable to a row that
delivered everything.
```

`delivered` can come in under the 5,000 sent: the publisher writes as
fast as the connection allows with no pacing, and that can outrun a
downstream forwarder's per-topic broadcast channel badly enough that
the resulting lag exceeds the replay buffer (ADR-0024) — some messages
are then unrecoverably lost, a real, already-accepted consequence of
that design, not a bug in the benchmark. When that happens, `msgs/sec`
prints as `n/a*` rather than a number — a lossy row's elapsed window
only covers however far the pipeline got before giving up, and
recovery replays are fast in-memory catch-up bursts, so that truncated
window can look faster than a complete row's despite delivering less
overall (see the ADR for the full reasoning). Don't compare a `n/a*`
row's throughput to another row's; the `delivered`/`sent` ratio and the
latency columns are still meaningful. `RUST_LOG`-style filtering isn't
wired up for this tool; it always logs at `warn`, which is enough to
see `forwarder lagged` / `lag recovery gap exceeded` when loss
happens.

Numbers here are a rough shape, not a guarantee — one sample per row,
no repetition or statistical averaging.

## Current limitations

Worth knowing before running this anywhere that matters:

- **`sender` is still unverified.** TLS ([above](#tls)), a
  [peer allowlist](#peer-allowlist), and
  [per-topic client authorization](#per-topic-client-authorization)
  are all available but opt-in, and even with all three on, any
  connection can still claim any `PeerId` in an envelope's `sender`
  field — nothing ties it to the connection's TLS identity. Without
  TLS, every connection is plaintext TCP; don't expose a node's port
  beyond a trusted network either way.
- **The metrics endpoint's authentication, when on, is a single shared
  secret.** `--metrics-token-file` (see
  [Metrics authentication](#metrics-authentication)) is opt-in and
  doesn't go through TLS or either mechanism above — it's one bearer
  token for the whole endpoint, not a per-scraper identity, and with
  no token file configured, anyone who can reach the port still gets
  the current render, same as before ADR-0019.
- **No persistence across a restart.** [Message replay](#message-replay)
  (ADR-0021) lets a subscriber that connects after a publish catch up
  on recent history, but only within a bounded in-memory buffer that's
  gone the moment a node restarts — there's still no durable,
  on-disk store. See [docs/ROADMAP.md](ROADMAP.md) Phase 8.
None of these are hidden defaults — they're the honest current state
of a project still in early phases. See [docs/ROADMAP.md](ROADMAP.md)
for what's planned next.
