# 52. Topic discovery

## Status

Accepted

## Context

Filed as #141 (Phase 16). There's no way to ask a node "what topics
currently have traffic" - a client can only subscribe to a filter it
already knows the name of. The issue flagged the real shape as a new
admin request/reply pair, following the same pattern
`StatusRequest`/`StatusReply` already established (ADR-0037), plus a
real decision this ADR has to make: what counts as "active" -
currently-subscribed-to, recently-published-to, or both.

## Decision

### A new `TopicsRequest`/`TopicsReply` pair, mirroring `StatusRequest`/`StatusReply`

```
"TopicsRequest"
{"TopicsReply": {
  "in_reply_to": <MessageId>,
  "topics": [{"topic": <Topic>, "subscribers": <u64>, "messages_buffered": <u64>}, ...]
}}
```

`TopicsRequest` is a field-less unit variant, encoded the same bare-
string way `StatusRequest` already is. Reused rather than folded into
`StatusReply` itself: a mesh with thousands of distinct topics would
make every plain status check pay for a payload most callers of it
never want, and the two answer genuinely different questions ("is
this node healthy/connected" vs. "what's flowing through it").

### "Active" is the union of both signals, not a single verdict

Each entry reports two raw numbers rather than the node collapsing
them into one boolean:

- `subscribers`: how many live connections are currently registered
  for this *exact* topic right now - an ordinary fan-out subscriber
  (`broadcast::Sender::receiver_count` on the topic's channel, the
  same count `Broker`'s own capacity-eviction logic already reads to
  decide whether an entry is reclaimable, see ADR-0025) *and* a
  consumer-group member (ADR-0042) both count. The two live in
  entirely separate `Broker` state (`join_group` never touches the
  topic's broadcast channel a fan-out subscriber does), so `topics()`
  reads both and sums them per topic - a group-only topic (published
  to via a literal-filter group, no ordinary subscriber at all) is
  still reported, not silently omitted. A wildcard-filter group's
  membership doesn't count toward any one topic's number, consistent
  with "exact topics only" below - it corresponds to an unknowable set
  of topics, not one.
- `messages_buffered`: how many envelopes currently sit in this
  topic's replay buffer (ADR-0021) - non-zero means it's been
  published to recently (within the replay window), zero doesn't
  necessarily mean *never* (an old publish can have aged out of the
  buffer's bounded capacity).

A topic is included in the reply at all only if at least one of the
two is non-zero - picking neither "subscribed" nor "published" alone
as *the* definition of active, since the issue itself posed them as
two candidate answers to the same question rather than obviously
preferring one. A caller that wants only "someone is listening right
now" filters on `subscribers > 0` client-side; one that wants only
"this has actually carried traffic" filters on `messages_buffered >
0`; one that wants either gets exactly this reply unfiltered further.
Cheaper to compute and more honest than inventing a single blended
"active" score this ADR would then have to justify.

### Exact topics only, not wildcard filter patterns

Only entries from the broker's exact-topic map are reported - never a
wildcard pattern (ADR-0022) some connection is subscribed with. Topic
discovery is about discovering concrete topic *names* actually in use;
a pattern isn't a topic, it's a filter over topics, and is already
fully documented as its own concept. Nothing stops a later ADR from
surfacing active patterns too if a real need for that shows up - kept
out of scope here to answer the one question the issue actually asked.

### No `--topic-acl` check - open to any connection, like `StatusRequest`

Answered identically on a client connection or a peer link, with no
authorization check, exactly mirroring `StatusRequest`'s own posture
(ADR-0037) rather than inventing a stricter one just for this. This
was weighed deliberately rather than assumed: a topic *name* can encode
more about what a system does than a peer count or a metrics counter
does (`customer.12345.orders` says more than `peers_connected: 3`
does) - but a node's own `--topic-acl` is a client-vs-topic
authorization control, not an information-disclosure control, and
teaching it to also gate *discovery* would be new scope for that flag,
not a natural extension of what it already promises. An operator whose
topic names themselves are sensitive is better served by not exposing
this port/connection to that caller at all (the same boundary that
already has to hold for `StatusRequest` and `--metrics-addr` today)
than by a partial, easy-to-misjudge filter here.

### Node-local, not mesh-aggregated

Exactly like `StatusReply`'s own `peers` list, this reports what *this*
node's own broker currently holds - which already includes traffic
forwarded in from peer links (an envelope arriving over a peer link
goes through the same `Broker::publish` a local client's does), but
never queries other nodes or aggregates a mesh-wide view. A caller
that wants the whole mesh's topic set queries every node it can reach,
the same way it would have to for `status` today.

## Consequences

- `thoth-mesh-core`: two new `MessageKind` variants
  (`TopicsRequest`/`TopicsReply`) and a new `TopicSummary` struct. No
  existing variant's fields change, so no `#[serde(default)]`
  concerns - an older peer that's never heard of `TopicsRequest`
  simply can't send one, the same as before `StatusRequest` existed.
- `thoth-mesh-broker`: a new `Broker::topics()` returning every exact-
  topic entry's name, live subscriber count, and buffered-message
  count - the same raw-getter pattern `messages_published`/
  `topic_evictions`/etc. already use, not a broker-side notion of
  "active."
- `thoth-mesh-node`: a new `handle_topics` dispatch arm, answering on
  any connection with no ACL check.
- `thoth-mesh-cli`: a new `topics` subcommand, printing the filtered,
  sorted list.
- Explicitly out of scope: wildcard-pattern discovery, mesh-wide
  aggregation, and any `--topic-acl`-based filtering of the reply -
  each a straightforward, independent follow-up if real usage shows
  it's wanted.
