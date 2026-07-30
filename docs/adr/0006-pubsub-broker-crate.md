# 6. In-process pub/sub broker: a new crate, tokio broadcast channels, exact-match topics

## Status

Accepted

## Context

`thoth-mesh-core` gives us the wire types (Envelope, Topic, MessageKind)
and `thoth-mesh` is scoped to federation/gossip — peer discovery and
replication (see [0002](0002-cargo-workspace-with-layered-crates.md)).
Neither owns the actual pub/sub dispatch engine: given a published
envelope on a topic, which local subscribers should receive it. That's
a distinct concern from both wire format and cross-peer federation, and
needs a home before `thoth-mesh-node` can do anything useful with a
connection.

Two sub-decisions dominate this:

1. **Where does this logic live?** Bolting it onto `thoth-mesh`
   (redefining that crate to mean "the whole pub/sub engine" instead of
   "federation/gossip") would blur a boundary ADR-0002 drew
   deliberately. Bolting it directly into `thoth-mesh-node` would work,
   but couples the dispatch engine to the daemon binary, making it
   harder to unit test or reuse independently of a running process.
2. **Concurrency model.** The broker holds live, shared, mutable state
   (which subscribers are registered to which topics) that will be
   touched concurrently by however many connection-handler tasks
   `thoth-mesh-node` runs. `thoth-mesh-core` deliberately avoids
   async/networking dependencies because it's pure data with nothing to
   block on — but the broker is the opposite: it's concurrent state
   that exists specifically to be shared across tasks. Async-native
   primitives avoid the friction of bridging a `std::sync::Mutex` into
   async handler code (blocking-in-async footguns, or a
   `spawn_blocking` hop on every registry access), and match what
   `thoth-mesh-node` will need for networking regardless.

## Decision

**Crate boundary:** a new crate, `thoth-mesh-broker`, depending only on
`thoth-mesh-core` and tokio's `sync` feature (no full tokio runtime
dependency — just the async-aware primitives). `thoth-mesh-node` will
depend on both `thoth-mesh` (federation) and `thoth-mesh-broker` (local
dispatch) once it exists, keeping "route to local subscribers" and
"replicate across peers" as separate, independently testable concerns.

**Concurrency:** `tokio::sync::broadcast` per topic, held in a
`HashMap<Topic, broadcast::Sender<Arc<Envelope>>>` guarded by a
`tokio::sync::RwLock`. Subscribing to a topic gets-or-creates the
topic's `Sender` and returns a fresh `broadcast::Receiver`.
Unsubscribing is just dropping the receiver — `broadcast` tracks
receiver count internally, so there's no manual subscriber bookkeeping
or explicit unsubscribe handle to manage. Envelopes are wrapped in `Arc`
before being sent, so fanning out to N subscribers is N reference-count
bumps, not N deep clones of the payload.

**API shape:** the broker only understands topic-addressed delivery,
not envelope semantics. `Broker::publish(&self, topic: &Topic, envelope:
Arc<Envelope>)` takes the topic explicitly rather than pattern-matching
on `envelope.kind` internally — interpreting an incoming
`MessageKind::Publish`/`Subscribe`/`Unsubscribe` and calling into the
broker accordingly is `thoth-mesh-node`'s job once it exists.

**Topic matching:** exact string match only for v1. Hierarchical
wildcard matching (MQTT/NATS-style `+`/`*` segments, which `Topic`'s
`/`-separated charset would support) is real design work — matching
algorithm, precedence rules, its own test suite — better done once
there's a working end-to-end system to justify it, not before.

## Consequences

The broker is unit-testable in isolation with no running daemon, no
network, and no federation logic in the picture — just
subscribe/publish/drop against `tokio::sync::broadcast`. Slow or
crashed subscribers can't block publishing to others or leak
registrations: a lagging receiver gets `RecvError::Lagged` on its next
`recv()` instead of the broker blocking or growing unbounded state, and
a dropped receiver is simply gone from the channel's internal count.

The cost is a topic that has had subscribers but currently has none
leaves a `Sender` with zero receivers sitting in the map — harmless
(a `send` with no receivers is cheap and simply reports zero
deliveries) but worth garbage-collecting later if the topic space grows
large and churns. Exact-match-only topics mean no wildcard subscriptions
in v1; broadening this later is additive (new matching logic layered on
the same registry) rather than a breaking change to the API shape
decided here.
