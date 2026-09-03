# 31. A `ConnectionContext` struct for `run_connection`'s dispatch loop

## Status

Accepted

## Context

`run_connection` (`connection.rs`) reads framed envelopes off a socket
in a loop and dispatches each one via a single `match` over
`MessageKind` - six arms (`Subscribe`, `Unsubscribe`, `Publish`,
`Ack`/`Error`, `Hello`, `PeerAnnounce`), each written out inline in the
function body. By #105 (ADR-0029), that function had grown to roughly
350 lines, and each arm closes over on the order of ten pieces of
shared state - `broker`, `membership`, `peer_links`, `interest`,
`discover`, `outgoing_tx`, `node_id`, `metrics`, the two ACL configs,
plus the loop's own local `forwarders`/`peer_identity` - by capture,
not through any explicit, readable interface.

#105's diff (splitting the read and write loops into separate tasks)
touched nearly every line of this match block, even though the actual
logic change was small: removing one `tokio::select!` wrapper
unindents everything beneath it by a level, and a diff tool has no way
to represent "this code didn't change, it just moved one column to the
left" as anything other than every line being deleted and re-added.
That's a diff-tool artifact, not evidence of a bug on its own - but
it's also a real, recurring cost: a function this size means even a
change scoped to one arm tends to produce a large, hard-to-review
diff, because the arms have no boundaries a diff can respect. Filed as
#106 during review discussion on #105.

## Decision

Introduce a private `ConnectionContext` struct, constructed once near
the top of `run_connection` from exactly the same fields `Shared` used
to be destructured into, plus the loop's own per-connection state
(`outgoing_tx`, `forwarders`, `peer_identity`, `peer_fingerprint`,
`principal`). Each match arm's body becomes a method call on it:

```rust
let keep_going = match &envelope.kind {
    MessageKind::Subscribe { filter } => ctx.handle_subscribe(&envelope, filter.clone()).await,
    MessageKind::Unsubscribe { filter } => ctx.handle_unsubscribe(&envelope, filter.clone()).await,
    MessageKind::Publish { .. } => ctx.handle_publish(envelope).await,
    MessageKind::Ack { .. } | MessageKind::Error { .. } => true,
    MessageKind::Hello { listen_addr } => ctx.handle_hello(&envelope, listen_addr.clone()).await,
    MessageKind::PeerAnnounce { peers } => {
        ctx.handle_peer_announce(peers);
        true
    }
};
if !keep_going {
    break;
}
```

Every handler returns `bool` - `true` to keep reading, `false` to end
the connection - collapsing what used to be a mix of bare `break` and
`continue` statements scattered through each arm into one uniform
signal the loop checks in exactly one place. A `ctx.send(envelope)`
helper wraps `self.outgoing_tx.send(Arc::new(envelope)).await.is_ok()`,
replacing the repeated `if outgoing_tx.send(..).await.is_err() { break;
}` pattern every arm used.

The initial-peer admission before the loop (the dial side already
knows its peer identity going in - ADR-0010) and the trailing cleanup
after it (stopping forwarders, unregistering a peer link) become
`ctx.admit_initial_peer(..)` and `ctx.shut_down()`, for the same
reason: both also read and mutate the same context fields the loop
body does.

`register_peer_link`, `learn_peers`, `propagate_peer`,
`propagate_interest`, and the ACL-check helpers
(`allowlist_permits`/`topic_acl_permits`/`acl_permits`/
`filter_acl_permits`) stay exactly as they were: free functions with
small, explicit parameter lists, each already unit-tested in
isolation by constructing only the two or three collaborators they
actually need. They were never part of the problem #106 describes -
that was specifically the match arms' implicit captures - and turning
them into `ConnectionContext` methods would only make their existing
tests need a full context to construct, for no corresponding benefit.

### Why a struct instead of splitting into more free functions

`register_peer_link` already shows what the free-function alternative
looks like: `#[allow(clippy::too_many_arguments)]` and seven
positional parameters, because async's borrow-checker rules don't let
a closure over `&mut self`-style state easily straddle `.await` points
the way a synchronous closure could. A method on a struct sidesteps
that: `&mut self` is one parameter standing in for everything the
handler needs, and each field is named at the call site once, in the
struct literal, rather than re-threaded through every call.

## Consequences

Pure refactor - no intended behavior change. The existing test suite,
including the chaos suite (ADR-0028) and `bench_mesh` (ADR-0030),
passes unmodified and is what verifies that; nothing here is itself
new test surface, since `ConnectionContext`'s methods take the same
shared, real collaborators (`Broker`, `PeerLinks`, ...) the existing
integration-style tests already exercise through `handle_connection`.

A future arm-specific change (e.g. reworking how `Subscribe` is
authorized) now produces a diff scoped to one method, not one that
reflows the whole function - directly addressing the diff-review cost
#106 was filed over.

Closes #106.
