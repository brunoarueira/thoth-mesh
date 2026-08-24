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

There's no packaged distribution yet (see
[docs/ROADMAP.md](ROADMAP.md) Phase 9) — build from source for now.
The examples below use `cargo run -p <crate> --` in place of the
built binary path; drop the `--release` flag while iterating, add it
back for anything you're leaving running for a while.

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

`subscribe` runs until interrupted (Ctrl-C). `publish` sends one
payload and exits — there's no reply or delivery confirmation (see
[`PROTOCOL.md`](../PROTOCOL.md#delivery-semantics)). The payload is
taken as UTF-8 text on the command line; there's no way yet to send
binary payloads or read one from stdin (see
[docs/ROADMAP.md](ROADMAP.md) Phase 10).

Every node and CLI invocation picks a fresh random `PeerId` on
startup — there's no persistent identity across restarts.

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
```

Three metrics today (ADR-0013):

| Metric | Type | Meaning |
| --- | --- | --- |
| `thothmesh_peers_connected` | gauge | Peer links currently up (not client connections). |
| `thothmesh_messages_published_total` | counter | `Publish` messages this node has processed since startup. |
| `thothmesh_forwarder_lag_total` | counter | Envelopes silently dropped because a subscriber's delivery channel fell behind (see [`PROTOCOL.md`](../PROTOCOL.md#delivery-semantics)). Nonzero here means a consumer is too slow, not that the node is misbehaving. |

Point a Prometheus `scrape_configs` target at `--metrics-addr` the
same way you would any other exporter; there's no special
`/metrics`-only handling to configure around.

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
prove its own identity (nothing enforces client identity yet, see
[docs/ROADMAP.md](ROADMAP.md) Phase 7):

```sh
cargo run -p thoth-mesh-cli -- --addr 127.0.0.1:49500 --tls-ca ca-cert.pem \
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

## Current limitations

Worth knowing before running this anywhere that matters:

- **No client authentication or authorization.** TLS ([above](#tls))
  and a [peer allowlist](#peer-allowlist) are both available but
  opt-in, and even with both on, any connection can still claim any
  `PeerId` — nothing ties the `sender` field to a connection's TLS
  identity, and a client (as opposed to a peer link) is never checked
  against an allowlist or restricted to which topics it can use.
  Without TLS, every connection is plaintext TCP; don't expose a
  node's port beyond a trusted network either way. See
  [docs/ROADMAP.md](ROADMAP.md) Phase 7.
- **No persistence.** A `Publish` reaches whoever is subscribed at
  that moment; nothing is stored for a subscriber that connects
  later, and there's no message replay. See
  [docs/ROADMAP.md](ROADMAP.md) Phase 8.
- **No config file.** Every flag is set on the command line each
  time; there's no `thoth-mesh.toml` or similar yet. See
  [docs/ROADMAP.md](ROADMAP.md) Phase 10.

None of these are hidden defaults — they're the honest current state
of a project still in early phases. See [docs/ROADMAP.md](ROADMAP.md)
for what's planned next.
