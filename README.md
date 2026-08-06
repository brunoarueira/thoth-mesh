# thoth-mesh

A federated publish/subscribe mesh, written in Rust.

Named for [Thoth](https://en.wikipedia.org/wiki/Thoth), the Egyptian god of
writing, magic, wisdom, and the moon — scribe of the gods, recorder of
events, keeper of balance. This project is a learning vehicle for going
deep on Rust (async networking, concurrency, protocol design, distributed
systems) beyond CLI-toy scope.

**Status:** early work in progress. A single node can broker pub/sub
traffic over TCP end to end; federation between nodes doesn't exist
yet. See [the roadmap](docs/ROADMAP.md) for what's next. Names are
reserved on crates.io ahead of a first real release.

## Layout

This is a Cargo workspace:

| Crate | Kind | Purpose |
| --- | --- | --- |
| [`thoth-mesh-core`](crates/thoth-mesh-core) | lib | Core protocol types and wire format shared by everything else. |
| [`thoth-mesh-broker`](crates/thoth-mesh-broker) | lib | In-process pub/sub dispatch: per-topic broadcast to subscribers. |
| [`thoth-mesh`](crates/thoth-mesh) | lib | Federation/gossip layer: peer discovery, membership, replication. |
| [`thoth-mesh-node`](crates/thoth-mesh-node) | bin | Daemon that runs a mesh node over a network transport. |
| [`thoth-mesh-cli`](crates/thoth-mesh-cli) | bin | Command-line client (`thoth-mesh` binary) for publishing, subscribing, and admin. |

## Architecture decisions

Significant architecture decisions are recorded as ADRs in
[`docs/adr/`](docs/adr/). For where the project is headed, see
[`docs/ROADMAP.md`](docs/ROADMAP.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
