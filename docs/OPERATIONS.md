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
than giving up — there's no need to manually reconnect. Peer topology
is static and one-directional to configure today: only the dialing
side is told where to connect via `--peer`, and there's no discovery
of peers beyond the ones explicitly listed (see
[docs/ROADMAP.md](ROADMAP.md) Phase 6).

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

- **No TLS, no authentication.** Every connection — client or peer —
  is plaintext TCP, and any connection can claim any `PeerId`. Don't
  expose a node's port beyond a trusted network. See
  [docs/ROADMAP.md](ROADMAP.md) Phase 7.
- **Static peer configuration.** `--peer` is the only way to join a
  mesh; there's no discovery, and a node doesn't learn about peers
  beyond the ones it or its peers were explicitly told about. See
  [docs/ROADMAP.md](ROADMAP.md) Phase 6.
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
