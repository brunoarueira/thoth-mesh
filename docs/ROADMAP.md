# Roadmap

thoth-mesh's stated goal is a *federated* publish/subscribe mesh — not
just a single node that brokers messages, but a set of nodes that find
each other and route messages between them. This document lays out the
phases between where the project stands today and that goal.

Each phase groups related work under a GitHub Milestone. Concrete,
ready-to-implement items become issues right away; later phases start
as a single tracking issue and get broken down (with an ADR where a
real design decision is needed) once the phase before it lands.

## Phase 0 — Foundations (done)

- ADR-0001–0004: recording ADRs, the Cargo workspace layout, crate
  naming, dual MIT/Apache-2.0 licensing.
- ADR-0005: wire protocol v1 (`thoth-mesh-core`) — envelope, topic,
  message types, sync + async (runtime-agnostic) framing.
- ADR-0006: in-process pub/sub broker (`thoth-mesh-broker`).
- ADR-0007/0008: `thoth-mesh-node` — a TCP daemon wiring the broker to
  real sockets.
- CI (test/fmt/clippy on push+PR), `cargo-audit`, Dependabot, and
  branch protection on `main`.

## Phase 1 — Usable single-node pub/sub

**Goal:** a person can run `thoth-mesh-node` and actually publish and
subscribe without writing Rust or hand-crafting CBOR frames.

- `thoth-mesh-cli`: `publish`, `subscribe`, and basic admin commands
  talking to a node over TCP.
- Node configuration: listen address, log level, etc. via CLI
  flags/env instead of the hardcoded `DEFAULT_ADDR`.
- Structured logging (`tracing`) in `thoth-mesh-node` — needed before
  federation multiplies the number of moving parts to debug.
- Fix the README's crate table, which is missing `thoth-mesh-broker`.

## Phase 2 — Federation groundwork (`thoth-mesh` crate)

**Goal:** two or more nodes can find each other and track who's alive,
without routing any messages between them yet.

- ADR: node-to-node protocol — does peer traffic share the client
  wire protocol/port, or get its own handshake?
- Static seed-peer list config + outbound peer connection management.
- Membership tracking (which configured peers are currently reachable).

## Phase 3 — Federated message routing

**Goal:** a publish on node A reaches a subscriber connected to node B.

- Topic-interest propagation between peers (so a node knows which
  remote peers currently want a given topic forwarded to them).
- Cross-node message forwarding through the broker.
- Loop prevention / de-dup for messages crossing more than one peer
  (envelopes already carry a `MessageId` usable for this).

## Phase 4 — Resilience & operability

**Goal:** the mesh survives real-world flakiness and is observable.

- Reconnect/backoff for dropped peer links.
- Metrics (connected peers, messages/sec, broadcast lag).

## Phase 5 — Release readiness

**Goal:** the crates are actually publishable and versioned like a
real project, not placeholders.

- Real crates.io publishes of `thoth-mesh-core` and
  `thoth-mesh-broker` (currently reserved but not buildable for a
  consumer — see the crates.io publish state note in project memory).
- Decide whether crates keep moving in version lockstep
  (`version.workspace = true`) or decouple.
- Declare the wire protocol stable (or explicitly mark it unstable).

## Non-goals (for now)

Things intentionally left out of this roadmap because they're not
blocking a working federated mesh: authn/authz, message persistence
and replay, wildcard/pattern topic matching (ADR-0006 chose exact
match for v1), and a hosted/managed deployment story.
