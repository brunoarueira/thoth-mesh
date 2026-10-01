# 55. Dynamic config reload

## Status

Accepted

## Context

Filed as #143 (Phase 16). `--topic-acl`/`--allow-peer`/
`--peer-topic-acl`/etc. all require a full restart to change - an
operator can't adjust authorization without dropping every
connection. The issue's own known shape flagged two decisions: a
reload trigger (a signal vs. an admin command extending the
`StatusRequest`/`TopicsRequest` admin surface, ADR-0037/ADR-0052), and
what's actually reloadable (TLS identity "probably isn't," ACLs/
allowlists "plausibly are").

Picked up deliberately after #167 (ADR-0054, the node's TOML config
file) rather than before it - discussed directly, not assumed: an
admin command would need its own authorization story (unlike
`StatusRequest`/`TopicsRequest`, a request that can rewrite this
node's own security policy can't reasonably answer on any unauthenticated
connection the way those do - an attacker who found a gap in the
*existing* authorization could use an unauthenticated reload command
to widen it further, a materially worse failure mode than either of
those two read-only requests has), while a signal has none of that
wire-protocol exposure at all - but a signal needs something on disk
to re-read, which didn't exist before ADR-0054.

## Decision

### `SIGHUP`, not an admin command

The conventional daemon reload signal (`kill -HUP`, `systemctl
reload`), triggering a re-read of the config file (ADR-0054) already
on disk - no wire-protocol exposure, no new authorization surface to
design or get wrong. Firmly rejected the admin-command alternative for
the reason above: `StatusRequest`/`TopicsRequest` are safe to answer
unauthenticated specifically *because* they're read-only introspection
- a reload request is a write that changes this node's own security
policy, and an unauthenticated (or under-authenticated) path to that
is a real attack vector, not a hypothetical one. Unix-only
(`tokio::signal::unix`) - no signal-based reload on Windows; the
process still runs, SIGHUP just isn't a concept there.

### Reloadable: `--topic-acl`, `--peer-topic-acl`, `--allow-peer`, `--peer-topic-filter` - nothing else

The issue's own three named examples, plus `--peer-topic-filter`
(ADR-0049) for the same reason `--peer-topic-acl` is in: all four are
repeated, list-shaped, purely-authorization-or-interest-scoping
config with no other node state depending on their *identity* (unlike,
say, `--data-dir`, where the on-disk store itself is tied to the path
chosen at startup). Explicitly **not** reloadable, and not attempted
here:

- **TLS identity** (`--tls-cert`/`--tls-key`/`--tls-ca`) - the issue's
  own steer. Swapping certificates live means rebuilding the
  acceptor/connector and re-deriving this node's own `PeerId`
  (ADR-0038) out from under every existing connection - a
  fundamentally different, riskier operation than swapping an ACL
  list, and worth its own ADR if it's ever actually wanted.
- **`--addr`/`--metrics-addr`** - reloading either would mean rebinding
  a listening socket, which is not meaningfully different from a
  restart for anything connected to it.
- **`--data-dir`, `--persisted-message-ttl-secs`, `--dead-letter-topic`,
  `--publish-rate-limit-per-sec`/`--publish-rate-limit-burst`** - real,
  independently useful candidates for a future ADR, but outside what
  this issue's own Goal (adjusting *authorization* without a restart)
  actually asked for. Scoped out deliberately rather than growing this
  ADR into "reload everything."

### Reload re-runs the *same* CLI-over-file merge (ADR-0054), against a freshly-read file - it never touches a CLI flag's value

A reloaded field only actually changes if it was sourced from the
config file at startup in the first place. If an operator set
`--topic-acl` as a CLI flag, `SIGHUP` re-reads the file, re-merges,
and - CLI still winning, per ADR-0054's own precedence rule - the
*live* value stays exactly what the CLI flag said, unaffected by
anything now in the file for that key. This was the deliberate
alternative to "reload always takes whatever's in the file now,
CLI-sourced or not": that version would mean a CLI-set ACL silently
reverting to whatever the file happens to hold (or nothing at all) on
the very first `SIGHUP`, an operator surprise with real security
consequences. "Reload replays the exact same precedence decision with
fresher input" has no such surprise, and needs no new precedence rule
this ADR would have to separately justify. Consequence worth being
explicit about: a reloadable ACL, to actually be reloadable in
practice, has to be configured via the file, not a flag.

Three of the four have no cross-flag constraint at all
(`--topic-acl`/`--peer-topic-acl`/`--peer-topic-filter` each parse
independently); `--allow-peer` still requires a TLS identity, exactly
as ADR-0054 established - since that identity itself isn't reloadable,
this collapses to "reject a reload that introduces a non-empty
`--allow-peer` on a node that was never started with TLS in the first
place," checked with the same code that already checks it at startup.
Every parse/validation failure rejects the *whole* reload outright -
logged, live state left completely unchanged, never a partial subset
of the four applied. *Within* a field, `Reloadable::set` (below) is a
single lock-guarded pointer swap - no reader ever sees a torn or
half-updated value for that one field. *Across* the four, each is its
own independent swap, applied in quick succession rather than as one
combined transaction - deliberately not something this ADR pays extra
complexity for, since no single message check ever reads more than one
of the four at once (a client `Publish` checks `topic_acl`, a peer's
checks `peer_topic_acl`, never both for the same message) - so nothing
actually depends on two of them changing in the exact same instant.

### Live state: a small `Reloadable<T>` handle, not a value each connection captures once

Today, `Shared`'s ACL fields are a plain `Option<Arc<T>>` -
`run_connection` destructures `Shared` once, at connection start, into
`ConnectionContext`'s own fields, and every subsequent ACL check reads
that connection's own copy for its entire lifetime. A reload that just
replaced `Shared.topic_acl` would be invisible to every connection
already established before the swap - only a *new* connection,
created after the reload, would ever see it. That's not "without
dropping every connection," it's "only new connections see the
change," a materially weaker promise than the issue actually asked
for.

`topic_acl`/`peer_topic_acl`/`peer_topic_filter`/`allowed_peers`
become `Reloadable<T>` - a small, cheaply-`Clone`able handle
(`Arc<std::sync::RwLock<Option<Arc<T>>>>` under the hood, the same
`std::sync` - not `tokio::sync` - choice `RateLimiter` already made
for the same reason: every read is a quick, synchronous, in-memory
check never held across an `.await` point, see ADR-0051) with
`.get()`/`.set()`. `ConnectionContext` still captures its own clone of
the handle once, same as today - what changes is that an ACL check now
calls `.get()` on it fresh, every time, instead of reading a value
captured at connection start. An already-established connection's very
next `Publish`/`Subscribe` after a reload sees the new rules; nothing
about its own lifecycle changes.

### The library stays config-file-and-signal-agnostic; `main.rs` owns the whole reload pipeline

`thoth-mesh-node`'s library code has never known about TOML files or
`clap` (`mod config` is declared directly in `main.rs`, ADR-0054); this
ADR doesn't change that. The library's own piece is a plain,
signal-agnostic capability: `NodeOptions` gains one new field,
`reload: Option<watch::Receiver<ReloadableAcls>>` - unlike
`metrics_token` (ADR-0019, kept as a separate parameter specifically
because only `run_with_tls` opens the metrics HTTP endpoint that flag
gates), applying a reload is ordinary `Shared` state every one of
`run_with_tls`/`serve_with_tls`/`spawn_with_tls` already sets up
identically from `NodeOptions` today - there's no structural reason to
withhold it from the other two, and real benefit in not doing so:
`spawn_with_tls` in a test can drive a reload by pushing a value
through a `watch::Sender` directly, with no real `SIGHUP` involved at
all. `ReloadableAcls` is a plain, already-*parsed* value type (the
same `TopicAcl`/`PeerTopicFilter`/`HashSet<[u8; 32]>` types
`NodeOptions` itself uses) - the library's own reload task just
applies whatever arrives, no parsing or validation of its own to
duplicate `main.rs`'s.

`main.rs` does everything file-and-signal-specific: installs the
`SIGHUP` listener, remembers the config file path and the original
`Cli`'s four reloadable fields from startup, and on each signal,
re-reads the file, re-runs the merge, re-validates (the same
functions `main` already calls once at startup), and sends the result
down the `watch` channel - or, on any failure, logs it and sends
nothing, leaving the live state untouched. `watch`, not `mpsc`: only
the *latest* desired state ever matters, never a queue of past reload
attempts to work through.

## Consequences

- New `crates/thoth-mesh-node/src/reload.rs`: `Reloadable<T>` and
  `ReloadableAcls`, both public.
- `Shared`/`ConnectionContext`'s four ACL-shaped fields change type
  from `Option<Arc<T>>` to `Reloadable<T>`; every read site adds a
  `.get()` call rather than reading a captured field directly.
- `NodeOptions` gains one new optional field, available uniformly to
  `run_with_tls`/`serve_with_tls`/`spawn_with_tls`; `Default` (`None`)
  is a no-op, unchanged from before this ADR.
- `main.rs`: a `#[cfg(unix)]` `SIGHUP`-triggered task feeding the
  `watch` channel, reusing the exact parsing/validation `main` already
  runs once at startup - the only signal-and-file-specific piece,
  entirely outside the library.
- Explicitly out of scope, each a real, independent follow-up if
  wanted later: reloading TLS identity, `--addr`/`--metrics-addr`,
  `--data-dir`-adjacent settings, or rate limiting; an admin-command
  reload trigger (rejected on its own security grounds above, not
  merely deferred).
- **Known limitation**: a reload only changes what a *new*
  `Subscribe`/`Publish`/`Hello` is checked against - it doesn't
  reconcile access already granted before the reload. Tightening
  `topic_acl`/`peer_topic_acl` doesn't stop a forwarder a connection
  already has running for a now-denied topic; tightening `allow_peer`
  doesn't disconnect a peer link whose fingerprint was just removed
  (`allowlist_permits` is only ever checked once, at the `Hello`/dial
  handshake); changing `peer_topic_filter` doesn't reconcile interest
  already propagated to an active peer link under the old filter.
  Reaching into live per-connection/per-peer-link state from the
  node-level reload task needs a real mechanism `PeerLinks` doesn't
  have today (a "force-disconnect by identity" primitive, plus a way
  to diff and reconcile each active link's already-propagated
  interest) - a materially larger feature than this ADR's own "swap a
  reference" mechanism, deliberately left to a follow-up rather than
  grown into this one. See #187.

Closes #143.
