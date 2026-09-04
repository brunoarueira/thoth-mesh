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
| [0014](0014-release-readiness-versioning-and-republish.md) | Release readiness: lockstep versioning, unstable protocol, republish all five crates |
| [0015](0015-dynamic-peer-discovery-gossip.md) | Dynamic peer discovery via gossip |
| [0016](0016-tls-transport-security.md) | TLS on peer and client connections |
| [0017](0017-peer-allowlist-via-tls-fingerprint.md) | Peer authentication via TLS certificate fingerprint allowlist |
| [0018](0018-per-topic-client-authorization.md) | Per-topic client authorization via TLS certificate fingerprint principals |
| [0019](0019-metrics-endpoint-authentication.md) | Metrics endpoint authentication via shared-secret bearer token |
| [0020](0020-peer-scoped-topic-restriction.md) | Peer-scoped topic restriction via a second `--peer-topic-acl` list |
| [0021](0021-message-replay-ring-buffer.md) | Message replay for late subscribers via a bounded per-topic ring buffer |
| [0022](0022-wildcard-topic-filters.md) | Wildcard/pattern topic matching via MQTT-style topic filters |
| [0023](0023-packaging-docker-and-systemd.md) | Packaging: a multi-stage Docker image and a hardened systemd unit |
| [0024](0024-lagged-forwarder-recovery.md) | Recovering a lagged forwarder from the replay buffer |
| [0025](0025-bound-per-node-memory-footprint.md) | Bounding per-node memory footprint at mesh scale |
| [0026](0026-bound-concurrent-peer-dials.md) | Bounding concurrent outbound peer dials |
| [0027](0027-dial-connect-and-handshake-timeout.md) | Timing out the dial connect-and-handshake phase |
| [0028](0028-chaos-partition-test-coverage.md) | Chaos/partition test coverage for reconnect, dedup, and loop prevention |
| [0029](0029-split-connection-read-and-write-loops.md) | Splitting a connection's read and write loops into separate tasks |
| [0030](0030-mesh-benchmark-suite.md) | A benchmark suite for throughput and latency across N mesh hops |
| [0031](0031-connection-context-struct.md) | A `ConnectionContext` struct for `run_connection`'s dispatch loop |
| [0032](0032-git-tags-and-github-releases.md) | Git tags and GitHub Releases for workspace version bumps |
| [0033](0033-cli-subscribe-to-multiple-topics.md) | `thoth-mesh-cli subscribe` accepts more than one filter |
| [0034](0034-cli-config-file.md) | `thoth-mesh-cli` config file for connection options |
| [0035](0035-cli-payload-fidelity.md) | `thoth-mesh-cli` payload input/output fidelity |
| [0036](0036-cli-shell-completions.md) | `thoth-mesh-cli` shell completions |
| [0037](0037-status-command.md) | `thoth-mesh-cli` status command |
| [0038](0038-peerid-from-tls-fingerprint.md) | `PeerId` derived from a TLS certificate fingerprint |
| [0039](0039-silently-correct-mismatched-peerid.md) | Silently correct a mismatched `PeerId` claim |

To add a new one, copy the format of an existing ADR, number it
sequentially, and set its status to `Accepted` once the decision is
final.
