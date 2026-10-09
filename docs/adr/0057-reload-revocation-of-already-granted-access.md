# 57. Reload revocation of already-granted access

## Status

Accepted

## Context

Issue #187, filed against ADR-0055's own review: a reload only
changes what a *new* `Subscribe`/`Publish`/`Hello` is checked
against. It doesn't reconcile access already granted before the
reload landed:

1. `--topic-acl`/`--peer-topic-acl` tightening doesn't stop a
   forwarder a connection already has running for a now-denied topic
   - the ACL is only consulted at `Subscribe`/`Publish` time, never
   per delivery.
2. `--allow-peer` tightening doesn't disconnect a peer link whose
   fingerprint was just removed - `allowlist_permits` is only ever
   checked once, at the `Hello`/dial handshake (ADR-0017).
3. `--peer-topic-filter` changes don't reconcile interest already
   propagated to an active peer link under the old filter.

These are three genuinely different mechanisms, not one feature:
`--topic-acl`/`--peer-topic-acl` needs to reach into a live
connection's own forwarder state from outside it; `--allow-peer`
needs to actually close an established connection, not just stop a
piece of it; `--peer-topic-filter` needs no connection-level signal
at all, just a smarter push from the reload path itself. This ADR
designs all three, but ships them as three separate PRs - each is
independently valuable and independently testable, and bundling them
into one change would make for a review nobody could hold in their
head at once.

## Decision

### Why `--allow-peer` and `--topic-acl`/`--peer-topic-acl` can't share one signal

Both need the node-level reload task to tell a *specific already-
running connection* something - obvious to reach for one signal
mechanism for both. They can't share one, for a reason worth being
explicit about: `connection.rs`'s read loop is a single
`async_framing::read_frame(&mut reader).await` per iteration, and
ADR-0029 already established why that can never be raced in a
`tokio::select!` against anything else *if the loop is going to keep
reading afterward* - cancelling `read_frame` after it's consumed a
frame's length prefix but before its payload permanently desyncs
every frame after it, because those bytes can't be put back once
taken off the stream.

- **`--allow-peer` ends the connection outright.** Once the decision
  is "close this," never reading from `reader` again is exactly the
  point - racing `read_frame` in a `select!` is safe *here
  specifically* because whichever branch wins, this loop never calls
  `read_frame` on this reader again either way.
- **`--topic-acl`/`--peer-topic-acl` has to keep the connection
  alive** and keep reading from the same `reader` afterward - racing
  `read_frame` is exactly the unsafe case ADR-0029 describes.

So: `--allow-peer` gets a `select!`-based signal the read loop races
against `read_frame`, safe only because taking that branch means
abandoning the reader for good. `--topic-acl`/`--peer-topic-acl` gets
no read-loop signal at all - the reload task mutates a connection's
live forwarder state directly, through a handle it holds
independently of anything the read loop is doing.

### `--allow-peer`: a per-peer-link `Notify`, raced via `select!`

`PeerLinks`'s registered `Link` gains an `Arc<tokio::sync::Notify>`,
created once per connection (in `run_connection`, before the loop -
every connection gets one, client or peer alike, even though only a
confirmed peer link's ever actually used; cheap enough not to bother
special-casing). `run_connection`'s loop becomes:

```rust
loop {
    tokio::select! {
        result = async_framing::read_frame(&mut reader) => { /* existing body */ }
        _ = disconnect.notified() => {
            let error = Envelope::new(ctx.node_id, MessageKind::Error {
                in_reply_to: None,
                message: "disconnected: no longer on --allow-peer (config reload)".into(),
            });
            // try_send, not an awaited send: best-effort, and a
            // revoked peer has every incentive to let its own
            // outgoing queue back up (just stop reading) specifically
            // to delay its own eviction - awaiting queue capacity
            // here would hand it exactly that lever.
            let _ = ctx.outgoing_tx.try_send(Arc::new(error));
            break;
        }
    }
}
```

An explicit `Error` first, not a silent close - the same "tell the
other side why" convention every other rejection in this codebase
already follows (ADR-0017/0018/0019/0020/0038). `break` falls through
to the exact same `ctx.shut_down()` this loop already runs on any
other exit - membership/`peer_links`/interest cleanup is unchanged,
not duplicated or reimplemented for this new path.

`PeerLinks` grows `disconnect_unless(&self, still_allowed: impl Fn
(Principal) -> bool)`: notifies every registered link whose
`Principal` `still_allowed` rejects, leaves the rest alone.
`spawn_reload_applier` calls it right after applying a reloaded
`allow_peer` set, checking each link's already-known `Principal`
(which, being `Principal::Fingerprint([u8; 32])`, already carries the
fingerprint - no new field needed to look it up) against the fresh
set, through the exact same `allowlist_permits` `connection.rs`
itself checks at handshake time (made `pub(crate)` for this, rather
than growing a second copy of the same rule in `lib.rs`). `None` (no
`--allow-peer` configured at all, including a reload that removes it
entirely) never disconnects anyone - consistent with
`allowlist_permits`'s own "no list, no restriction" rule.

Two gaps worth being explicit about: the registration race is
closed; the overlapping-connection gap remains open.

- **The registration race.** An inbound `Hello`/dial handshake can
  pass the allowlist check against the value live *before* a reload,
  then - after an `await` (sending the Hello reply; the dial side's
  own earlier handshake step) - register as a peer link *after* that
  reload's `disconnect_unless` scan already ran, escaping it entirely
  until some future reload happens to catch it. Closed by
  re-checking `allowlist_permits` again immediately after
  `register_peer_link` returns, against whichever value is live *at
  that moment* - if a reload landed in the gap, this fires this
  connection's own `disconnect` itself, rather than waiting on
  another reload to.
- **An overlapping connection, left open.** `PeerLinks::register`
  replacing an existing entry for the same `PeerId` orphans the *old*
  entry's `disconnect` handle - once overwritten, nothing scanning
  the registry can reach it again, so a revoked old connection that
  loses this race stays up until it disconnects some other way. Tried
  closing this by firing the superseded entry's own `disconnect`
  right there in `register` - reverted: a full mesh isn't just a
  stale reconnect racing its own predecessor's teardown, two peers
  can legitimately dial each other at the same moment and both
  connections are real, not a stale one and a fresh one. Verified
  this isn't hypothetical - the single-line version of this fix was
  enough to stop a 20-node full-mesh bootstrap test from ever
  converging, each side kept killing the other's simultaneous dial
  outright. Left as a known, narrow gap rather than risk a similar
  regression again without a real way to tell "superseded" apart from
  "simultaneous" - tracked as #194.

### `--topic-acl`/`--peer-topic-acl`: forwarders reconciled by direct mutation

`ConnectionContext.forwarders` changes from a private
`HashMap<TopicFilter, Subscription>` to `Arc<Mutex<HashMap<TopicFilter,
Subscription>>>`, registered - alongside the connection's `Principal`
and whether it's (currently) a peer link - in a new
`ConnectionRegistry` every connection adds itself to on start and
removes itself from in `shut_down()` (mirroring `PeerLinks`'s own
register/unregister shape, but for every connection, not just peer
links). `handle_subscribe`/`handle_unsubscribe`/`shut_down` change to
lock the shared map instead of touching a plain field directly - the
connection's own behavior is otherwise unchanged.

On a `--topic-acl`/`--peer-topic-acl` reload, the reload task walks
every registered connection, locks its forwarders, and - using the
exact same `filter_acl_permits` check `handle_subscribe` itself uses,
against whichever reloaded ACL applies to that connection's own role
- stops (aborts the forwarder/leaves the group) and removes any
filter no longer permitted, propagating the resulting interest loss
exactly like an explicit `Unsubscribe` would if dropping to zero
local subscribers. No signal into the read loop at all; the
connection's own task keeps reading frames throughout, completely
unaware anything happened unless its *next* `Subscribe`/`Publish`
hits the (now-updated) ACL on its own.

### `--peer-topic-filter`: no connection-level mechanism needed at all

Unlike the other two, this doesn't require reaching into any
connection's own state - it only ever needs to re-run the *push* side
of interest propagation (ADR-0011/ADR-0049), which already lives
entirely in code the reload task itself can call directly. On a
`--peer-topic-filter` reload, for every currently registered peer
link (`PeerLinks` grows an iteration method exposing each link's
`Principal` and outgoing sender), take the current local
`Interest::snapshot()` - the same snapshot `register_peer_link`'s own
catch-up logic already uses - and, under the *new* filter, partition
it by whether that link's `Principal` is still permitted to hear
about each one. Send a `Subscribe`-shaped interest-announce for the
permitted set, an `Unsubscribe`-shaped one for the rejected set.

No history of "what did this link already get told" needs tracking:
both messages are idempotent from the receiving peer's own
perspective (duplicate interest propagation is possible by design
already, in the gossip-and-mesh reality `thoth-mesh` discovers peers
in; a peer being told to unsubscribe from something it was never told
to subscribe to is a no-op) - so recomputing the whole set fresh from
current `Interest` every reload, rather than diffing against a
remembered history, is both simpler and already covered by guarantees
the mesh relies on elsewhere.

## Consequences

- Three independent PRs against this one ADR, landed separately:
  1. `--allow-peer` disconnect (the `Notify`/`select!` mechanism).
  2. `--topic-acl`/`--peer-topic-acl` forwarder reconciliation (the
     `ConnectionRegistry`/shared-forwarders mechanism).
  3. `--peer-topic-filter` reconciliation (no new mechanism - reuses
     existing `Interest`/`PeerTopicFilter`/`PeerLinks` building
     blocks from the reload task directly).
- `connection.rs`'s read loop grows a `select!` for `--allow-peer`
  only; `--topic-acl`/`--peer-topic-acl` adds no new branch to it at
  all, by design.
- `PeerLinks` grows `disconnect_unless` (`--allow-peer`) and a
  link-iteration method (`--peer-topic-filter`).
- A new `ConnectionRegistry` (`--topic-acl`/`--peer-topic-acl`) -
  every connection registers, regardless of role, mirroring
  `PeerLinks`'s shape for the broader set of connections topic ACLs
  apply to.
- None of this changes what's reloadable - still exactly the four
  fields ADR-0055 named. It closes the gap between "reload changes
  what a new request sees" and "reload actually revokes what was
  already granted," for all four.

Closes #187 once all three PRs land.
