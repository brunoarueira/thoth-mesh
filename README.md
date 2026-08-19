# thoth-mesh

A federated publish/subscribe mesh, written in Rust.

Named for [Thoth](https://en.wikipedia.org/wiki/Thoth), the Egyptian god of
writing, magic, wisdom, and the moon — scribe of the gods, recorder of
events, keeper of balance. This project is a learning vehicle for going
deep on Rust (async networking, concurrency, protocol design, distributed
systems) beyond CLI-toy scope.

**Status:** early work in progress. A single node can broker pub/sub
traffic over TCP end to end, and nodes federate over peer links with
reconnect/backoff and basic metrics. See [the roadmap](docs/ROADMAP.md)
for what's next. All crates are 0.x — the wire protocol and public
APIs are not yet stable (see ADR-0014).

## Layout

This is a Cargo workspace:

| Crate | Kind | Purpose |
| --- | --- | --- |
| [`thoth-mesh-core`](crates/thoth-mesh-core) | lib | Core protocol types and wire format shared by everything else. |
| [`thoth-mesh-broker`](crates/thoth-mesh-broker) | lib | In-process pub/sub dispatch: per-topic broadcast to subscribers. |
| [`thoth-mesh`](crates/thoth-mesh) | lib | Federation/gossip layer: peer discovery, membership, replication. |
| [`thoth-mesh-tls`](crates/thoth-mesh-tls) | lib | TLS transport helpers: certificate loading, config, and a plaintext/TLS stream shim. |
| [`thoth-mesh-node`](crates/thoth-mesh-node) | bin | Daemon that runs a mesh node over a network transport. |
| [`thoth-mesh-cli`](crates/thoth-mesh-cli) | bin | Command-line client (`thoth-mesh` binary) for publishing, subscribing, and admin. |

## Running it

For a step-by-step walkthrough — build, single-node quickstart,
multi-node federation, metrics, logging, and current limitations —
see [`docs/OPERATIONS.md`](docs/OPERATIONS.md).

## Architecture decisions

Significant architecture decisions are recorded as ADRs in
[`docs/adr/`](docs/adr/). For a from-scratch description of the wire
protocol itself, independent of the Rust implementation, see
[`PROTOCOL.md`](PROTOCOL.md). For where the project is headed, see
[`docs/ROADMAP.md`](docs/ROADMAP.md).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the workflow: how work is
planned, when a change needs an ADR, and what to run locally before
opening a PR.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
