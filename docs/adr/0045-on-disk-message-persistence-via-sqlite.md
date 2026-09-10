# 45. On-disk message persistence via SQLite

## Status

Accepted

## Context

Filed as #133 (Phase 14), the foundational piece. Every message lives
only in memory: a per-topic `broadcast` channel plus a bounded replay
buffer (ADR-0021), and a per-topic retained slot (ADR-0043). A node
restart loses all of it, and #134 (durable subscriptions / consumer
offsets) and #135 (message TTL / dead-lettering) both need a durable
log to have a position in, or an age to expire against, at all.

The issue flagged two decisions as ADR-worthy: the storage engine,
and the retention policy.

## Decision

### SQLite, embedded via `rusqlite` with the `bundled` feature

Over a pure-Rust embedded KV (redb) or a hand-rolled append-only
segmented log:

- **The backup and portability story is the point.** One `.db` file;
  `VACUUM INTO 'snapshot.db'` produces a consistent single-file
  backup while the node is running; any SQLite tool
  (`sqlite3`, DB Browser, every language binding) can open it to
  inspect or repair. That's exactly the "copy one file, push it
  somewhere" operator story this phase wants, and it's stronger than
  what redb offers.
- **It keeps this PR — and #134/#135 — small.** Retention is a
  `DELETE`, consumer offsets (#134) are another table, TTL (#135) is
  a `WHERE ts < ?` sweep. A query planner, secondary indexes, and
  transactions come for free; none of it has to be hand-built.
- **Crash safety is not a thing to get subtly wrong here.** SQLite in
  WAL mode is the most-exercised durable-storage code that exists.

The cost is a bundled C dependency in an otherwise all-Rust
workspace: `cargo build` compiles the SQLite amalgamation (`cc` is
already assumed), `cargo-audit` doesn't cover the vendored C, and
cross-compiling needs a C cross-toolchain for the target. Accepted -
SQLite is arguably a more trustworthy dependency than most crates,
and it's confined to `thoth-mesh-node` (see below).

### The `MessageStore` trait lives in `thoth-mesh-broker`; the SQLite impl lives in `thoth-mesh-node`

`thoth-mesh-broker` defines a small **synchronous** trait:

```rust
pub trait MessageStore: Send + Sync + 'static {
    fn append(&self, envelope: &Envelope) -> io::Result<()>;
    fn load_recent(&self, per_topic: usize) -> io::Result<Vec<(Topic, Vec<Arc<Envelope>>)>>;
    fn load_retained(&self) -> io::Result<Vec<(Topic, Arc<Envelope>)>>;
}
```

`Broker` gains an `Option<Arc<dyn MessageStore>>`, `None` for
`Broker::new()` (unchanged, fully in-memory - every existing test and
the whole in-memory path is untouched) and `Some` via
`Broker::with_store(..)`. `thoth-mesh-broker` gets **no new
dependency** beyond adding `rt` to its existing `tokio` features so
`publish` can call the store from `tokio::task::spawn_blocking` - the
SQLite crate, and all the SQL, are entirely in `thoth-mesh-node`,
which is a binary and already owns every other piece of I/O and
config.

### `publish` persists synchronously, before it delivers

Inside `Broker::publish`, once the message has passed the `seen`
dedup check (ADR-0011) and *before* any in-memory delivery, the
envelope is written to the store on a blocking thread and awaited -
so a message is durable before a subscriber ever sees it. `append`
does one transaction: `INSERT` into `messages`, and upsert-or-delete
the `retained` row when the envelope is a `retain: true` publish
(ADR-0043 - an empty payload clears it).

A persist **failure** is logged (and counted -
`thothmesh_persist_failures_total`, a new metric) but is **not**
fatal: in-memory delivery still proceeds, so a full or failing disk
degrades durability rather than taking the node down. Documented as
the v1 behavior.

Synchronous-before-delivery is the honest default for a first
persistence layer - the semantics are trivial to reason about and
test. A batched async writer thread (better throughput, at the cost
of a small "last few ms lost on power-loss" window) is a clear
follow-up with its own ADR if throughput ever matters.

### Retention: newest N per topic, pruned periodically

`DEFAULT_PERSISTED_MESSAGES_PER_TOPIC` (100_000) - far larger than
the in-memory replay buffer (1024, ADR-0021), which is the whole
point: the log is what survives *past* the in-memory window. A
`ROW_NUMBER() OVER (PARTITION BY topic ORDER BY seq DESC)` sweep runs
every `PRUNE_EVERY` (1000) appends and once at startup, deleting
everything beyond the cap per topic. `retained` rows are a separate
table and are never pruned by this.

This mirrors ADR-0025's bounded-memory decisions (a per-key cap,
enforced lazily) but for disk. Known gap, same as ADR-0025's own: the
total is per-topic-cap × number-of-distinct-topics-ever-seen, so a
node cycling through unboundedly many topics grows unboundedly on
disk. Age-based expiry (#135) is where that gets a real answer; a
byte cap could too. Not solved speculatively here.

### Startup rehydration makes a restart transparent

When `--data-dir` is set, node startup opens
`<data-dir>/messages.db`, runs the retention sweep, then repopulates
the broker from disk before accepting connections:
`load_recent(DEFAULT_REPLAY_BUFFER_CAPACITY)` fills each topic's
replay buffer, `load_retained()` fills each retained slot. A later
subscriber sees exactly what it would have before the restart.

Rehydrated messages are **not** re-broadcast (there are no
subscribers yet) and **not** re-entered into the `seen` dedup set - a
restart is a legitimate reset of the bounded dedup window (ADR-0011
already documents that window as best-effort and finite). Per-
subscriber durable offsets are explicitly out of scope - that's #134.

## Consequences

- New `--data-dir <path>` flag on `thoth-mesh-node`. Absent, the node
  is exactly as it is today: fully in-memory, nothing written.
- `thoth-mesh-broker` gains a `MessageStore` trait, an optional store
  field, `Broker::with_store`, and `rehydrate_buffer`/
  `rehydrate_retained` (startup only). Its `tokio` features gain
  `rt`. No other new dependency.
- `thoth-mesh-node` gains `rusqlite` (`bundled`), a `persistence`
  module with the `SqliteStore`, startup rehydration wiring, and one
  new metric.
- `docs/OPERATIONS.md` documents `--data-dir`, the on-disk format
  (a plain SQLite DB an operator can inspect/back up), the retention
  cap, and the "persist failure is logged, not fatal" behavior.
  No `PROTOCOL.md` change - this is node-local behavior, invisible on
  the wire.
- Schema carries a `meta(key, value)` row `schema_version = "1"` so a
  future format change has a migration hook.
