# 50. Request/reply over pub/sub

## Status

Accepted

## Context

Filed as #138 (Phase 15). RPC-style call/response has to be hand-rolled
on top of `Publish`/`Subscribe` today - no correlation-id convention,
or protocol support for one, exists. The issue flagged the real
decision as client-side convention (a reply-to topic plus a
correlation id, entirely payload-encoded) versus actual wire-protocol
support (new fields, or a new `MessageKind`), plus how timeout/no-reply
is handled given pub/sub has no inherent request/response pairing.

## Decision

### Two new optional `Publish` fields, not a new `MessageKind`

```
Publish { topic, payload, retain, content_type, reply_to: Option<Topic>, in_reply_to: Option<MessageId> }
```

Both `#[serde(default)]`, same rolling-upgrade story as every prior
`Publish`/`Subscribe` field addition (ADR-0041/0042/0043/0044/0046). A
**request** is an ordinary `Publish` with `reply_to` set to the topic
its answer should land on; a **reply** is an ordinary `Publish` (to
that topic) with `in_reply_to` set to the original request envelope's
own `id` - the exact same `Ack`/`Error`-style correlation-by-`id`
convention this protocol already uses everywhere else (ADR-0041's
`Ack`, ADR-0044's error replies), reused rather than inventing a
second one. No new `MessageKind`: a request and a reply are both
completely ordinary publishes in every other respect - they persist
(ADR-0045), replay (ADR-0021), retain (ADR-0043), get dead-lettered
(ADR-0047), and route through consumer groups (ADR-0042) exactly like
any other `Publish`, for free, because they *are* one.

Rejected: encoding reply-to/correlation purely into the payload bytes
(the issue's "client-side convention" option). That isn't actually
*protocol* support - two independent implementations would still need
to agree on a payload envelope format out of band, which is exactly
the interoperability gap this issue exists to close. Also rejected: a
dedicated `MessageKind::Request`/`MessageKind::Reply` pair - it would
duplicate `Publish`'s entire shape (topic, payload, content-type) for
no behavioral difference, and would need its own special-casing
through persistence/replay/groups/dead-lettering that reusing `Publish`
gets automatically.

### The node does nothing special with either field

`reply_to`/`in_reply_to` are opaque metadata to every node-side code
path, exactly like `content_type` (ADR-0044) - carried, persisted, and
delivered unchanged, never validated, never interpreted, never acted
on. This is a deliberate, load-bearing choice: it keeps the entire
node-side footprint of this ADR to two struct fields and their
mechanical `#[serde(default)]` plumbing, with zero new logic in
`Broker`, `handle_publish`, any ACL, or any persistence/redelivery/
dead-letter path. It's also what makes composition with everything
else free (previous section) - the node has no notion of "this publish
expects a reply" to get subtly wrong.

One consequence worth being explicit about: if the request topic has
more than one ordinary (fan-out) subscriber, every one of them can see
the request and independently reply - the requester may get more than
one `Publish` carrying the same `in_reply_to`, and it's the
requester's own job to take the first, aggregate all of them, or
whatever else its use case wants. A requester that wants exactly one
responder gets it for free by making the request topic a consumer
group (ADR-0042) instead - no special-casing needed here either.

### Timeout/no-reply handling is entirely client-side

The node has no concept of "this publish is a request" and does
nothing if no reply ever arrives - there's no server-side timeout,
no redelivery of unanswered requests, nothing to configure. A
requester subscribes to its own `reply_to` topic *before* publishing
the request (so a fast responder can't possibly reply before it's
listening), then waits up to a caller-chosen duration for a `Publish`
there whose `in_reply_to` matches the request's own `id`, treating a
timeout as "no reply" - exactly the shape of a plain client-side RPC
timeout, no different in kind from how `ack: true`'s timeout is also
purely reactive on whichever side owns the clock (ADR-0041), just
without any redelivery machinery behind it.

### `thoth-mesh-cli`: a new `request` subcommand, no corresponding `respond`

```
thoth-mesh request <topic> <payload> [--timeout-secs <secs>]
```

Generates a fresh, per-invocation-unique reply topic (`reply.<hex of a
freshly generated MessageId>` - deliberately *not* derived from this
invocation's own `sender`, which is stable across invocations when a
TLS identity is configured and would otherwise collide between two
concurrent `request` calls using the same certificate), subscribes to
it, publishes the request with `reply_to` set to it, waits (bounded by
`--timeout-secs`, default matching `DEFAULT_ACK_TIMEOUT`'s 5s - no
particular reason to diverge) for a `Publish` there naming the
request's own `id` in `in_reply_to`, and prints it exactly like
`subscribe`'s own delivery printing (`--output text`/`raw`, ADR-0035).
A timeout is a non-zero exit with a message on stderr, not a hang.

No `thoth-mesh respond` (or similar auto-responder) command. Building
one well means deciding where a reply's payload actually comes from
per request (a fixed canned value? stdin, once? a shell command run
per request?) - a materially different, open-ended feature (closer to
a small RPC server framework) than "add a reply-to/correlation
convention," and not something the issue's known shape asked for. A
responder today is `subscribe`'s existing delivery loop plus a
hand-written reply `Publish` using the two new fields directly -
already fully possible with what this ADR adds, just not wrapped in a
dedicated command. Worth its own issue if real usage shows it's
wanted.

## Consequences

- `MessageKind::Publish` grows `reply_to: Option<Topic>` and
  `in_reply_to: Option<MessageId>` - every construction site across
  the workspace updated, same mechanical footprint the last several
  `Publish`/`Subscribe` field additions each had.
- No new `MessageKind`, no new node-side behavior, no new metric - a
  request/reply exchange is invisible to the node as anything other
  than two ordinary publishes.
- New `thoth-mesh-cli` `request` subcommand; no `respond` counterpart,
  deliberately out of scope for now.
- Composes for free with consumer groups (exactly-one-responder),
  durable subscriptions, persistence, and dead-lettering - none of
  them need to know this feature exists.
