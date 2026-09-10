-- On-disk message store schema (ADR-0045).
--
-- `include_str!`'d and applied idempotently by `SqliteStore::open`.
-- There is one schema version; a real migration mechanism is deferred
-- until a second one is actually needed (the `meta.schema_version`
-- row below is the hook for it). Keep `SCHEMA_VERSION` in
-- `persistence.rs` in step with what this file creates.

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

-- Key/value scratch table; currently just `schema_version`.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per distinct (non-duplicate) publish. `envelope` is the
-- CBOR-encoded `Envelope`; `topic`/`ts` are extracted alongside it
-- for the retention sweep and future age-based expiry. Pruned to the
-- newest DEFAULT_PERSISTED_MESSAGES_PER_TOPIC rows per topic.
CREATE TABLE IF NOT EXISTS messages (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    topic    TEXT    NOT NULL,
    ts       INTEGER NOT NULL,
    envelope BLOB    NOT NULL
);

CREATE INDEX IF NOT EXISTS messages_topic_seq ON messages (topic, seq);

-- The current retained (last-value) message per topic (ADR-0043).
-- A separate table so retention pruning of `messages` never removes
-- a topic's retained value.
CREATE TABLE IF NOT EXISTS retained (
    topic    TEXT PRIMARY KEY,
    envelope BLOB NOT NULL
);
