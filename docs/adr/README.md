# Architecture Decision Records

This directory records the significant architecture decisions made on
thoth-mesh, using the format described in
[0001](0001-record-architecture-decisions.md).

| ADR | Title |
| --- | --- |
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions |
| [0002](0002-cargo-workspace-with-layered-crates.md) | Cargo workspace with layered crates |
| [0003](0003-rename-crates-to-thoth-mesh-namespace.md) | Rename crates to the thoth-mesh namespace |
| [0004](0004-dual-mit-apache-license.md) | Dual MIT/Apache-2.0 license |
| [0005](0005-wire-protocol-v1.md) | Wire protocol v1: envelope, framing, and CBOR serialization |
| [0006](0006-pubsub-broker-crate.md) | In-process pub/sub broker: a new crate, tokio broadcast channels, exact-match topics |
| [0007](0007-node-v1-tcp-daemon.md) | thoth-mesh-node v1: TCP daemon wiring the broker to sockets |
| [0008](0008-generic-async-framing.md) | Generic async framing in thoth-mesh-core via futures-util |
| [0009](0009-peer-handshake-shared-port.md) | Peer handshake over the shared client port |
| [0010](0010-peer-links-share-client-dispatch.md) | Peer links share the client dispatch loop |
| [0011](0011-interest-propagation-and-loop-prevention.md) | Topic-interest propagation and loop prevention |
| [0012](0012-peer-reconnect-backoff.md) | Reconnect with exponential backoff for dropped peer links |
| [0013](0013-metrics-hand-rolled-prometheus-exposition.md) | Metrics: hand-rolled Prometheus text exposition, opt-in port |

To add a new one, copy the format of an existing ADR, number it
sequentially, and set its status to `Accepted` once the decision is
final.
