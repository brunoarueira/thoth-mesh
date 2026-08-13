# 10. Peer links share the client connection's broker-wired dispatch

## Status

Accepted

## Context

Phase 2 gave nodes a handshake (ADR-0009) and membership tracking
(issue #24), but no message routing over peer links yet - that's
Phase 3's goal (see [docs/ROADMAP.md](../ROADMAP.md)).

The two sides of a peer connection are, today, not symmetric.
`thoth-mesh-node`'s accept side (`connection.rs::run_connection`) is a
single loop matching on every incoming `Envelope.kind`, including
`Hello` as just one more arm alongside `Subscribe`/`Unsubscribe`/
`Publish`. Nothing about that loop is client-specific: if the peer on
the other end of an *accepted* connection sent `Subscribe` after its
`Hello`, it would already be handled correctly - a forwarder gets
spawned against the shared `Broker`, wired to that connection's
outgoing channel, exactly as for a client.

The dial side (`peering.rs::dial_peer`) has no such loop. After
`dial_handshake` completes, it holds the connection open by reading
frames and discarding every one:

```rust
// Hold the connection open until the peer disconnects.
// Nothing routes over peer links yet - that's Phase 3 - so
// any further frames are just logged and discarded.
loop {
    match async_framing::read_frame(&mut conn).await { ... }
}
```

It also never touches `Broker` at all - `spawn_seed_peers`/`dial_peer`
don't even have a reference to one. Before topic-interest propagation
or cross-node forwarding (the rest of Phase 3) can be built, this
asymmetry has to be resolved: does the dial side get its own
parallel implementation of `Subscribe`/`Unsubscribe`/`Publish`
handling, or does it reuse `connection.rs`'s existing loop?

## Decision

The dial side hands off to the same dispatch loop as the accept side,
once its handshake completes, rather than growing a second
implementation of it.

`dial_handshake` operates on a `Compat<TcpStream>` (it's generic over
`futures_util`'s async traits per ADR-0008, so it doesn't know about
`tokio` directly). `tokio_util::compat::Compat::into_inner()` recovers
the underlying `TcpStream` afterward with no data loss - the compat
wrapper adds no buffering of its own, it's a trait-adapter over the
same socket. `dial_peer` uses that to hand the raw stream straight
into `connection::handle_connection`, the same entry point
`accept_loop` uses.

`handle_connection`/`run_connection` gain one new parameter,
`initial_peer_identity: Option<PeerId>`:

- `None` on the accept side, which still only learns who it's talking
  to once it receives `Hello` mid-loop, exactly as before.
- `Some(info.peer_id)` on the dial side, whose peer identity is
  already known from `dial_handshake`'s result before the loop even
  starts.

This makes the disconnect bookkeeping (`membership.mark_disconnected`)
uniform regardless of which side dialed, and lets `peering.rs` drop
its own read loop and membership-teardown code entirely - dialing
now means: connect, handshake, hand off.

`spawn_seed_peers`/`dial_peer` gain a `broker: Arc<Broker>` parameter
so that handoff has something to wire into; `serve`/`spawn` in
`lib.rs` construct the `Broker` once and pass the same instance to
both the accept loop and the seed-peer dialer, rather than the accept
loop building its own as it does today.

No new `MessageKind` variants, and no separate peer protocol: reusing
`Subscribe`/`Unsubscribe`/`Publish` for peer-to-peer traffic is a
continuation of ADR-0009's shared-port decision, not a departure from
it. `Broker` becomes the single mechanism routing messages to local
subscribers *and* to peers, uniformly.

## Consequences

Once this lands, either side of a peer connection can subscribe,
unsubscribe, or publish and have it handled exactly as if it came
from an ordinary client - peer connections become full participants
in local routing, symmetric regardless of who dialed whom.

Nothing yet causes a node to *originate* a `Subscribe` toward its
peers based on its own local clients' interest - a node has to be
manually told (or, later, automatically triggered) to subscribe over
a peer link before anything flows across it. There is also no
protection yet against forwarding loops on a connected mesh with a
cycle (an envelope forwarded node A -> B -> C -> A would circulate
forever). Both are deliberately out of scope here and tracked as the
next Phase 3 issue: topic-interest propagation (propagate local
subscribe/unsubscribe transitions to every active peer link) and loop
prevention (dedup on `Envelope`'s existing `MessageId`).

Reconnect/backoff for a dropped dial-side link remains out of scope,
tracked under Phase 4 (issue #18).
