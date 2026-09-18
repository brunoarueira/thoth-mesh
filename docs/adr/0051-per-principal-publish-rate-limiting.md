# 51. Per-principal publish rate limiting

## Status

Accepted

## Context

Filed as #140 (Phase 16). Nothing stops one client from overwhelming a
node or a topic today - no per-client or per-topic rate limit exists.
The issue flagged two real decisions: what identity a quota is keyed
by (per-connection, which a reconnect resets, vs. per-principal, the
TLS-fingerprint identity ADR-0018 already established, which doesn't),
and what happens once a limit is hit (reject vs. throttle/delay).

## Decision

### Per-principal (ADR-0018's `Principal`), not per-connection

A quota keyed by the live connection alone is trivially evaded by
reconnecting - defeats the point of a quota meant to survive a noisy
or hostile client's own choices. `Principal` (the TLS client
certificate's SHA-256 fingerprint, or `Anonymous` with none - the same
identity primitive `--topic-acl` already checks) is reused rather than
inventing a second one: it's already computed per connection, already
survives a reconnect for exactly the reason a quota needs to, and
every `Anonymous` connection already shares one identity for
authorization purposes, so it shares one quota bucket too, consistent
with the existing precedent rather than a new special case.

### Reject with an `Error`, not throttle/delay

A rejected `Publish` gets an `Error` reply, the same shape a
`--topic-acl` rejection already uses (ADR-0018) - the client is told
immediately and decides for itself whether/when to retry. Throttling
(holding the message and delivering it once back under quota) was
rejected: it needs per-connection timer/queueing state this node has
nowhere else, for a benefit (no client-visible error) that doesn't
obviously outweigh that - a client already has to handle a rejection
for every other reason a `Publish` can fail, and gets to control its
own retry/backoff instead of the node silently deciding for it.

### `Publish` only, node-wide across every topic, client connections only

- **`Publish` only, not `Subscribe`.** The issue's concern is a
  connection *overwhelming* the node - a `Publish` flood is the actual
  volume vector; a `Subscribe` flood is already a different, much
  smaller-surface problem (each one is a single registration, not a
  stream of messages) that this ADR leaves alone.
- **Node-wide per principal, not scoped per-topic.** The issue posed
  "per client or per topic" as two candidate identities for the same
  one quota, not two dimensions to cross - a principal gets one bucket
  covering everything it publishes, not one per topic it publishes to.
  A genuinely per-topic quota (e.g. "this principal may publish 10/s
  to `sensor.temp` specifically") is a materially bigger configuration
  surface (something shaped like `--topic-acl`'s
  `<principal>|<topic>|<rate>` entries) for a narrower problem than
  the node-wide case already covers; worth its own issue if a real
  need for it shows up.
- **Client connections only, not peer links.** Mirrors the existing
  `topic_acl`/`peer_topic_acl` split (ADR-0018/ADR-0020): a peer link
  is already gated by `--allow-peer` trust before it's ever linked at
  all, and legitimate inter-node replication traffic has a
  fundamentally different volume profile than a single client - lumping
  the two together risks throttling normal mesh traffic to punish a
  single noisy client. A `--peer-publish-rate-limit` counterpart is
  straightforward to add later, exactly the way `--peer-topic-acl` was
  split out from `--topic-acl`, if a noisy-peer scenario turns out to
  need it.

### Token bucket, in-memory only, two flags

```
--publish-rate-limit-per-sec <n>    # sustained refill rate
--publish-rate-limit-burst <n>      # bucket capacity; defaults to
                                     # --publish-rate-limit-per-sec's
                                     # own value if omitted
```

`--publish-rate-limit-per-sec` gates the whole feature - `None` (the
default) means unchanged behavior: no limiter is even constructed, no
`Publish` is ever rejected for rate. `--publish-rate-limit-burst`
requires it, same `requires` relationship `--persisted-message-ttl-secs`
has with `--data-dir`; omitting it (having opted in to rate limiting at
all) defaults the bucket capacity to the sustained rate itself - a flat
`N`/second with no extra burst allowance above it, the simplest useful
behavior from one flag alone.

Classic token bucket, refilled lazily on each check (no background
sweep task): a principal's bucket starts full (at `burst` capacity),
gains `per_sec` tokens per elapsed second up to `burst`, and a
`Publish` is allowed only if at least one token is available, which it
then spends. In-memory only, like `--peer-topic-filter`'s and
ADR-0048's work-queue leases - a restart resets every principal back to
a full bucket, which is fine: this is abuse mitigation, not a durable
allowance ledger.

### The bucket table is capacity-bounded, like every other per-identity map (ADR-0025)

A distinct bucket is allocated the first time each principal publishes
- an unbounded number of distinct principals (in practice, distinct
TLS client certificates a CA was ever willing to sign, or the single
shared `Anonymous` entry) would otherwise grow this table without
limit, the same risk ADR-0025 already catalogued for membership/peer-
directory/topic maps. Capped at 4096 entries; over capacity, the
longest-tracked principal's bucket is reclaimed first (insertion-order
FIFO, not touch-refreshed LRU - unlike the peer directory's refresh-on-
touch eviction, this table is checked on every single `Publish`, a hot
path where an `O(n)` reorder-on-touch scan isn't worth paying for the
extra precision). A reclaimed principal simply gets a fresh, full
bucket the next time it publishes - never a way to get *more* than
`burst` allows, only possibly an earlier-than-otherwise refill.
`thothmesh_rate_limit_principal_evictions_total` counts this
happening, mirroring `thothmesh_membership_evictions_total`/
`thothmesh_peer_directory_evictions_total`.

## Consequences

- New `thoth-mesh-node` module (`rate_limit.rs`): `RateLimiter`, an
  `Arc`-shared, capacity-bounded, per-`Principal` token bucket table -
  threaded through `Shared`/`ConnectionContext`/`NodeOptions` exactly
  like `TopicAcl`.
- Two new `thoth-mesh-node` flags: `--publish-rate-limit-per-sec`,
  `--publish-rate-limit-burst`. Neither given - the default - means
  unchanged behavior: no rate limiting at all.
- Two new metrics: `thothmesh_publish_rate_limit_rejections_total`
  (a `Publish` refused for exceeding its principal's quota) and
  `thothmesh_rate_limit_principal_evictions_total` (a bucket reclaimed
  for the table sitting over its 4096-entry cap). Both zero unless
  rate limiting is configured at all (the latter additionally needs
  over 4096 distinct principals to have ever published).
- `MetricsSummary` gains both new fields (`publish_rate_limit_rejections_total`,
  `rate_limit_principal_evictions_total`) with no `#[serde(default)]` -
  consistent with every prior counter added to it (ADR-0041 through
  ADR-0049): unlike `Publish`/`Subscribe`'s wire fields, `MetricsSummary`/
  `StatusReply` isn't part of this project's rolling-upgrade
  compatibility contract. `summary()`/`render_prometheus()` take an
  additional `Option<&RateLimiter>` parameter to read the latter from -
  the same shape `Membership`/`PeerDirectory` already use for their own
  eviction counters, rather than duplicating the count inside `Metrics`
  itself.
- Explicitly out of scope for now, same reasoning as the corresponding
  section above: per-topic quotas, peer-link rate limiting, and
  throttle/delay instead of rejection. Each is a straightforward,
  independent follow-up if real usage shows it's wanted.
