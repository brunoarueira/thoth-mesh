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

## Phase 5 — Release readiness (see ADR-0014)

**Goal:** the crates are actually publishable and versioned like a
real project, not placeholders.

All 5 crate names are reserved on crates.io, but every one of them is
a stale `0.0.1` placeholder — `thoth-mesh-core`, `thoth-mesh`,
`thoth-mesh-node`, and `thoth-mesh-cli` predate ADR-0005's wire
protocol, and `thoth-mesh-broker` 0.0.1 was published with `cargo
publish --no-verify` just to reserve the name. None of them build for
a consumer today.

- Versioning stays in lockstep (`version.workspace = true`) — the
  crates already move together and aren't independently useful.
- The wire protocol, and every crate's public API, stay explicitly
  unstable (0.x) — there are no external consumers yet and it's
  changed in nearly every phase so far.
- All 5 crates get a real `0.1.0` publish, in dependency order
  (`thoth-mesh-core` → `thoth-mesh`/`thoth-mesh-broker` →
  `thoth-mesh-node` → `thoth-mesh-cli`), verified for real this time
  instead of `--no-verify`.

## Phase 6 — Dynamic mesh topology

**Goal:** the mesh's topology grows on its own instead of needing
every edge hand-configured. Message *routing* is already multi-hop
(ADR-0011); topology isn't — a node only ever dials the exact
addresses passed via `--peer`.

- Peer exchange / gossip, so a node can discover and dial peers it was
  never directly configured with.

## Phase 7 — Trust & transport security

**Goal:** the mesh isn't wide open to anyone who can reach the port —
today there's no TLS and no authentication anywhere in the wire
protocol.

- TLS on peer and client connections.
- Peer authentication / allowlisting.
- Fine-grained authorization: per-topic publish/subscribe permissions,
  and access control on the metrics/admin surface (TLS alone doesn't
  imply this).

## Phase 8 — Message durability & topic model

**Goal:** broker semantics grow beyond in-memory, best-effort,
exact-match delivery.

- Message persistence and replay for late subscribers.
- Wildcard/pattern topic matching (ADR-0006 chose exact match for v1;
  revisit now that it's been exercised across a real federated mesh).

## Phase 9 — Deployment & operational hardening

**Goal:** the mesh can be run somewhere real, and its resilience
claims (reconnect/backoff, loop prevention, dedup) are tested under
actual failure conditions, not just asserted by one lightweight
integration test each.

- Packaging: a Dockerfile, a systemd unit, and install docs.
- A benchmark suite: throughput and latency across N mesh hops.
- Chaos/partition testing: kill/restart nodes mid-mesh, partition a
  peer, verify reconnect+backoff and loop prevention/dedup hold up.

## Phase 10 — CLI ergonomics

**Goal:** `thoth-mesh-cli` is pleasant to use for more than a demo —
v1 was deliberately minimal (see issue #13).

- Subscribe to multiple topics in one invocation.
- Config file support, so `--addr` doesn't need repeating.
- Payload input/output fidelity: read a publish payload from stdin,
  stop assuming payloads are UTF-8 text end to end.
- Shell completions.

## Phase 11 — CLI admin & observability

**Goal:** an operator can inspect a running mesh from the CLI —
connected peers, a metrics summary — not just by scraping the
Prometheus endpoint (ADR-0013) or reading logs.

- Admin/status commands (e.g. `thoth-mesh status`), depends on Phase 6
  for what "peers" means once topology isn't purely static-config-driven.

## Standalone work (not tied to a phase)

Some work is useful on its own regardless of which feature phase lands
next — tracked as plain issues rather than under a milestone:

- A written wire-protocol spec (`PROTOCOL.md`).
- An operator / getting-started guide (`docs/OPERATIONS.md`).
- `CONTRIBUTING.md` documenting the ADR/issue/branch/PR workflow.

## Non-goals (for now)

Things intentionally left out of this roadmap: a hosted/managed
deployment story (a control plane, a SaaS offering — distinct from
Phase 9's "runnable via Docker/systemd," which is in scope).
