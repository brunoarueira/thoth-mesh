# thoth-mesh

A federated publish/subscribe mesh, written in Rust.

Named for [Thoth](https://en.wikipedia.org/wiki/Thoth), the Egyptian god of
writing, magic, wisdom, and the moon — scribe of the gods, recorder of
events, keeper of balance. This project is a learning vehicle for going
deep on Rust (async networking, concurrency, protocol design, distributed
systems) beyond CLI-toy scope.

**Status:** early work in progress. Names are reserved on crates.io ahead
of a first real release; none of the crates below do anything yet.

## Layout

This is a Cargo workspace:

| Crate | Kind | Purpose |
| --- | --- | --- |
| [`thoth-mesh-core`](crates/thoth-mesh-core) | lib | Core protocol types and wire format shared by everything else. |
| [`thoth-mesh`](crates/thoth-mesh) | lib | Federation/gossip layer: peer discovery, membership, replication. |
| [`thoth-mesh-node`](crates/thoth-mesh-node) | bin | Daemon that runs a mesh node over a network transport. |
| [`thoth-mesh-cli`](crates/thoth-mesh-cli) | bin | Command-line client (`thoth-mesh` binary) for publishing, subscribing, and admin. |

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
