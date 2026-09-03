# 33. `thoth-mesh-cli subscribe` accepts more than one filter

## Status

Accepted

## Context

`thoth-mesh subscribe` took exactly one topic filter; watching more
than one meant running a separate process per filter. Filed as #53
(Phase 10, docs/ROADMAP.md), which already sketched the CLI surface:
a repeatable positional (`thoth-mesh subscribe weather.updates
traffic.updates`) rather than a new subcommand or a repeated `--topic`
flag - the least surprising shape for "subscribe to N things," and
consistent with most CLIs' pub/sub tooling.

## Decision

### CLI surface: `filters: Vec<String>`, `required = true`

`Command::Subscribe`'s `filter: String` becomes `filters: Vec<String>`,
a plain repeatable positional (`#[arg(required = true)]` so `thoth-mesh
subscribe` with no filter at all is still a usage error, same as
before). Every filter is parsed and validated up front, before ever
connecting - unchanged from the single-filter behavior, just looped
over N filters instead of one.

### Sending every `Subscribe` up front, not one at a time

The naive approach - call the existing `subscribe()` helper once per
filter, sequentially - has a real bug: `subscribe()`'s wait loop
discards any envelope that isn't the `Ack`/`Error` for the one request
it's currently waiting on. That's fine with exactly one filter in
flight (nothing else could legitimately arrive yet), but not with two
or more: if filter A is already acked and starts delivering live
`Publish` traffic while filter B's subscribe is still in flight, that
delivery arrives while the loop is waiting on B's ack alone and gets
silently thrown away - a real message loss, not a display glitch.

A new `subscribe_all(conn, sender, filters)` sends every filter's
`Subscribe` first, then drains responses in one loop until every
request's `Ack` (or the first `Error`) has been seen. It's deliberately
not responsible for printing anything itself - same division of
responsibility as `subscribe` (the existing single-filter helper)
never printing either - so any `Publish` that arrives before every ack
does is buffered and returned, in arrival order, rather than acted on
inline:

```rust
async fn subscribe_all(
    conn: &mut Compat<MaybeTlsStream>,
    sender: PeerId,
    filters: &[TopicFilter],
) -> std::io::Result<Vec<Envelope>> {
    let mut pending: HashSet<MessageId> = HashSet::new();
    for filter in filters {
        let envelope = Envelope::new(sender, MessageKind::Subscribe { filter: filter.clone() });
        send(conn, &envelope).await?;
        pending.insert(envelope.id);
    }
    let mut backlog = Vec::new();
    while !pending.is_empty() {
        let received = recv(conn).await?;
        match &received.kind {
            MessageKind::Ack { in_reply_to } if pending.remove(in_reply_to) => {}
            MessageKind::Error { in_reply_to: Some(id), message } if pending.remove(id) => {
                return Err(std::io::Error::new(ErrorKind::PermissionDenied, message.clone()));
            }
            MessageKind::Publish { .. } => backlog.push(received),
            _ => continue,
        }
    }
    Ok(backlog)
}
```

`subscribe_and_print` prints `backlog` (in order) right after the
"Subscribed to ..." line, before falling into its own live-delivery
loop - so nothing arriving during the handshake window is lost or
reordered relative to what arrives afterward, it's just held briefly.

The single-filter `subscribe()` helper (heavily used directly by
existing protocol-level tests) is untouched - it's still correct for
exactly one filter, and `subscribe_all` is only what
`subscribe_and_print`'s CLI-facing path now calls.

A rejection (`Error`) on any filter still fails the whole command
immediately, same as the single-filter behavior - not "subscribe to
whichever ones were permitted and silently skip the rest." Returning
`Err` this way also drops whatever had already accumulated in
`backlog` - acceptable, since a rejected batch never reaches the print
loop at all.

### Output stays per-message, unchanged

`print_if_publish`'s existing `[{topic}] {payload}` format already
names the concrete topic a message arrived on, not the filter that
matched it - already sufficient to disambiguate which of several
subscribed filters a given line came from, with no format change
needed.

## Consequences

`thoth-mesh subscribe weather.updates traffic.updates` watches both
over one connection. No wire-protocol change - this is purely how the
CLI drives the existing `Subscribe`/`Ack`/`Error` exchange.

Closes #53.
