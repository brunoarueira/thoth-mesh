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

## Phase 12 — Verified sender identity

**Goal:** `PeerId` actually means something — bound to the TLS
identity a connection already authenticates with, closing the gap
ADR-0005 flagged as deliberate-but-temporary ("expected to be
replaced by a cryptographic identity ... once federation/trust work
begins") and OPERATIONS.md's "`sender` is still unverified"
limitation names directly.

- Bind a connection's `PeerId` to its authenticated TLS certificate
  fingerprint (the same identity `--allow-peer`/`--topic-acl` already
  use), rather than trusting whatever it self-reports in `Hello`/an
  envelope's `sender`. Inherently TLS-and-client-cert-only — a
  plaintext or certificate-less connection has no cryptographic
  identity to bind to, the same boundary `Principal::Anonymous`
  already draws for topic ACLs.
- Decide what a mismatched claim does: reject the connection outright
  (like an unlisted peer today, ADR-0017) or silently correct it to
  the authenticated identity.
- Once a peer's identity is trustworthy, close the corresponding gap
  in loop-prevention/membership/interest-dedup (ADR-0011), which
  currently keys off `Hello`'s self-reported `PeerId` alone with
  nothing stopping two peers from claiming the same one.

## Phase 13 — Delivery semantics

**Goal:** pub/sub today is fire-and-forget fan-out to whoever's
currently subscribed — no delivery guarantee, no way to split load
across a group of workers, no way to just get "the current value,"
and no hint at what a payload even is.

- At-least-once delivery with ack-based redelivery, built on the
  existing replay buffer (ADR-0021/ADR-0024) rather than requiring
  durable storage first.
- Consumer groups — a named group load-balances a topic (one member
  gets each message) as an alternative to today's
  fan-out-to-everyone.
- Retained/last-value semantics per topic — subscribing gets you the
  current value immediately, distinct from the replay buffer's
  bounded catch-up window.
- An optional content-type hint on `Publish`, so a subscriber can
  know what it's looking at without a full schema registry.

## Phase 14 — Durability beyond the replay buffer

**Goal:** Phase 8's replay buffer is bounded, in-memory, and
short-lived — nothing survives a node restart, and a subscriber gone
too long loses everything in between.

- On-disk persistence for published messages, replacing or backing
  the in-memory broadcast channel (ADR-0006).
- Durable subscriptions / consumer offsets — reconnecting resumes
  exactly where a subscriber left off.
- Message TTL / dead-lettering for payloads nobody ever consumes.

## Phase 15 — Federation-specific routing

**Goal:** a peer link forwards whatever the far end's aggregate
subscriber interest already is (ADR-0011) — real federation
topologies often want more deliberate control over what crosses a
given link.

- Selective per-peer-link topic filtering — relay only matching
  topics to a specific peer, independent of that peer's own interest.
- Request/reply over pub/sub — a correlation-ID convention for
  RPC-style call/response.

## Phase 16 — Operability

**Goal:** running thoth-mesh somewhere that matters needs guardrails
and visibility beyond `status`/metrics.

- Rate limiting / quotas, per client or per topic.
- Topic discovery — "what topics currently have traffic."
- Payload-level encryption, independent of transport TLS.
- Dynamic config reload — `--topic-acl`/`--allow-peer`/etc. without a
  restart.
- Health/readiness endpoints for orchestration (k8s, systemd),
  distinct from metrics/status.

## Phase 17 — Per-message tracing across peer hops

**Goal:** `status` (ADR-0037) shows a node's point-in-time state, but
nothing shows the path a single message actually took across a
multi-hop mesh — essential for debugging routing/federation issues
that only show up in a real topology.

- A trace/correlation id propagated alongside a message as it crosses
  peer links, distinct from `MessageId` (which identifies the
  message, not its journey).
- A way to actually observe a trace — logged at each hop at minimum,
  a dedicated query surface possibly later.
- Worth documenting explicitly: a traced message re-entering an
  already-visited node is exactly the case loop-prevention
  (ADR-0011) exists for.

## Phase 18 — Protocol version negotiation

**Goal:** `Envelope.version` exists on the wire (ADR-0005) but
nothing reads or enforces it — a version mismatch between two builds
(or a future non-Rust implementation) fails unpredictably instead of
being detected and handled.

- Reject/handle an incompatible version explicitly, rather than
  attempting to decode it as understood.
- A documented compatibility policy — what "supported" means across
  versions, and how a breaking wire change would roll out.

## Phase 19 — Embeddable client library

**Goal:** every existing wire-protocol user, including this project's
own test suites, hand-rolls envelope construction and framing calls —
there's no ergonomic Rust API for embedding a thoth-mesh client in
another application, the way `thoth-mesh-cli` is one for a terminal.

- A `thoth-mesh-client`-shaped crate wrapping connect/publish/
  subscribe/status behind a proper async API.
- Decide the relationship to `thoth-mesh-cli`: does the CLI become a
  thin wrapper over this crate, or do they stay independent?

## Standalone work (not tied to a phase)

Some work is useful on its own regardless of which feature phase lands
next — tracked as plain issues rather than under a milestone:

- A written wire-protocol spec (`PROTOCOL.md`).
- An operator / getting-started guide (`docs/OPERATIONS.md`).
- `CONTRIBUTING.md` documenting the ADR/issue/branch/PR workflow.

## Non-goals (for now)

Things intentionally left out of this roadmap:

- A hosted/managed deployment story (a control plane, a SaaS offering
  — distinct from Phase 9's "runnable via Docker/systemd," which is
  in scope).
- Multi-tenancy / a schema registry — enterprise-shaped features that
  don't obviously match this project's minimalism so far; Phase 13's
  content-type hint covers the "what is this payload" need without
  either.
- Horizontal sharding within one logical node — contradicts the
  mesh's own design, where *nodes* are the scaling unit, not shards
  within one.
