# 29. Splitting a connection's read and write loops into separate tasks

## Status

Accepted

## Context

Found while building #51's benchmark (`bench_mesh`, not yet merged),
filed as #104: sustaining a real burst of `Publish` traffic across a
peer link - something no existing test does; the lightest existing
multi-hop test sends exactly one message - reliably breaks the link
within the first few dozen to few hundred messages, with connections
logging:

```
closing connection: frame read error err=frame length 2758243941 exceeds the maximum of 16777216 bytes
```

`2758243941` decodes to the bytes `a4 67 76 65`. `0xa4` is CBOR's tag
for "map with 4 entries" - exactly how `ciborium` encodes `Envelope`
(`version`, `id`, `sender`, `kind`: 4 fields). These aren't corrupted
bytes; they're the *start of a real envelope's CBOR encoding*, being
misread as a 4-byte frame-length prefix. That's a frame-boundary
desync, not packet corruption - and it's fully reproducible under
sustained bidirectional load, not an occasional flake.

### Root cause: a cancellation-unsafe read inside `tokio::select!`

`run_connection`'s dispatch loop (`connection.rs`) reads and writes on
one task via:

```rust
loop {
    tokio::select! {
        frame = async_framing::read_frame(&mut reader) => { /* ... */ }
        Some(outgoing) = outgoing_rx.recv() => { /* ... */ }
    }
}
```

`async_framing::read_frame` does two sequential steps: `read_exact`
the 4-byte length prefix, then `read_exact` that many payload bytes.
`tokio::select!` polls every branch and, once one resolves, *drops*
the other branches' futures - including one that's mid-flight. If the
`outgoing_rx` branch becomes ready right after `read_frame` has
consumed the length prefix but before it finishes reading the payload,
`select!` drops the read future. The payload bytes are still sitting
unread in the socket - but the *length prefix bytes are already gone*,
consumed from the stream with no way to put them back. The next loop
iteration starts a brand new `read_frame` call, which reads the first
4 bytes of what was actually the *previous* frame's payload and
interprets them as a new length prefix. Every frame after that point
is misaligned, and the connection has no way to recover - which is
exactly the `frame length 2758243941` symptom above.

This is a standard `tokio::select!` pitfall (its own docs call this out
as "cancellation safety"): a branch used in `select!` must be safe to
drop mid-poll, and a naive multi-`.await` read is not, because the
underlying stream's read position has already moved past whatever was
consumed so far. Nothing about this is specific to the benchmark - any
peer link (or client connection) with enough concurrent inbound and
outbound traffic to make both `select!` branches contend can hit it.
Existing tests never sent enough messages back-to-back to expose it.

## Decision

### Read and write run as two independent tasks, not one via `select!`

`run_connection`'s read loop no longer touches the write half at all.
Every outgoing envelope - direct replies (`Ack`, `Hello`, `Error`) and
forwarded broadcasts alike - goes through `outgoing_tx`, unifying what
used to be two separate write paths (`send_envelope(&mut writer, ..)`
inline in the read loop, and the same helper called from the `select!`
branch draining `outgoing_rx`) into one. A new `write_loop` task, spawned
once at the top of `run_connection`, owns the write half exclusively and
drains `outgoing_rx` until it closes or a write fails:

```rust
async fn write_loop(
    mut writer: Compat<WriteHalf<MaybeTlsStream>>,
    mut outgoing_rx: mpsc::Receiver<Arc<Envelope>>,
) {
    while let Some(envelope) = outgoing_rx.recv().await {
        if !send_envelope(&mut writer, &envelope).await {
            tracing::warn!("closing connection: frame write error");
            break;
        }
    }
}
```

The read loop becomes a plain `loop { async_framing::read_frame(&mut
reader).await ... }` - no `select!`, so `read_frame` always runs to
completion once started. There's nothing left to cancel it mid-flight,
which is what actually closes the bug: cancellation safety isn't
something to carefully get right within one task, it's removed as a
concern entirely by construction.

`outgoing_tx.send(..).await` (backpressure-aware, not `try_send`) is
what a direct reply now goes through instead of writing straight to
the socket - `mpsc::Sender::send` is cancellation-safe in the relevant
sense: if its future is ever dropped mid-poll, nothing has been
partially transmitted, unlike a raw multi-step socket read.

### Shutdown: cleanup still runs once, in the read loop; the writer task is joined, not aborted

The read loop keeps ending the connection exactly as before: on a
frame error, on `outgoing_tx.send` returning `Err` (which now means
"the writer task ended," standing in for the old direct-write failure
check), it breaks out of the loop, then runs the same cleanup it always
did - aborting forwarders, unregistering the peer link, marking it
disconnected. That cleanup drops every `outgoing_tx` clone the
connection itself created (forwarders, `peer_links`); the read loop's
own local `outgoing_tx` is then dropped as `run_connection` returns.
Once every clone is gone, `write_loop`'s `outgoing_rx.recv()` returns
`None` on its own - no explicit signal or abort needed to stop it.
`run_connection` `.await`s the writer task's `JoinHandle` after
cleanup, so a connection is only considered fully done once its last
queued reply has actually been flushed (or the write side has already
failed on its own), rather than leaving it as an untracked background
task.

One accepted, minor asymmetry from this split: previously, a write
failure ended the read loop in the same instant (one task, one
`break`). Now, the read loop only notices a dead writer the next time
it tries to send something - if it's purely processing incoming
messages that need no reply for a while, it could keep reading briefly
after the write side has already failed. In practice a broken TCP
connection is observable from both directions close together, so the
read side's own `read_frame` typically fails on its own around the
same time anyway. Not worth complicating the shutdown signal for an
edge case (a connection broken in exactly one direction) this minor,
compared to the frame-desync bug being fixed.

### Two ordering assumptions that were only ever true by accident

Unifying every outgoing message onto one `outgoing_tx` queue surfaced
two places that had silently relied on *direct* writes reaching the
wire ahead of *queued* ones - true under the old two-path design (a
direct `send_envelope` call happened synchronously within the same
`select!` arm, while anything queued onto `outgoing_rx` waited for a
later loop iteration), never a real guarantee, and no longer true once
everything shares one FIFO queue and code order is what determines
wire order:

- **`Subscribe`/`Unsubscribe` acks vs. the interest-propagation echo**
  (ADR-0011): a subscribe from a peer link's own filter gets echoed
  back down that same link. The old code queued the echo (via
  `propagate_interest`) *before* constructing the ack, relying on the
  ack's direct write to still win the race to the wire. Fixed by
  actually sending the ack first in code, before propagating interest
  - a subscriber now deterministically sees a reply to its own request
  before a side effect of it, instead of by accident.
- **The `Hello` reply vs. `register_peer_link`'s catch-up traffic**
  (ADR-0015): accepting a `Hello` used to call `register_peer_link`
  (which can immediately queue a `PeerAnnounce` catching the new link
  up on already-known peers) *before* sending the `Hello` reply. Once
  both go through the same queue, the far end's `dial_handshake` -
  which expects the very first message it reads to be exactly a
  `Hello` - could receive that catch-up `PeerAnnounce` first instead,
  failing the handshake outright. Fixed the same way: send the reply
  first, register the link after. Safe to reorder - this task doesn't
  read the next incoming frame until the whole match arm returns, so
  nothing from the far end can be processed here before
  `register_peer_link` runs regardless of which order the two happen
  in.

### A dropped `JoinHandle` doesn't stop the task it names

`Node::accepted_connections` (ADR-0028) aborts a connection's
outermost task to simulate it dying in tests. That only cancels
`run_connection`'s own task - `write_loop` is a second, independently
spawned task, and a plain `JoinHandle` left to drop when
`run_connection`'s local variables go away *detaches* rather than
aborts, leaving the write half of the socket alive with nothing left
reading the other side. `AbortOnDrop<T>` wraps the writer task's
`JoinHandle` so dropping it - whether via normal cleanup or `run_
connection` itself being aborted - aborts the task it refers to.

## Consequences

No wire-protocol change, no new dependency. `bench_mesh` (#51, not yet
merged) is what actually verifies this: the same sustained-burst
scenario that reliably desynced a peer link within dozens to hundreds
of messages before this fix now runs cleanly at thousands of messages
across a hop-count sweep - a better regression check for this specific
bug than a hand-rolled unit test would be, since it only manifests
under genuine concurrent read/write pressure a small test wouldn't
generate. The existing chaos suite (ADR-0028) is what caught both the
ordering regressions and the dropped-task-handle regression above,
each surfacing as a real test failure rather than something reasoned
out in advance - direct evidence for why that suite exists.

Closes #104.
