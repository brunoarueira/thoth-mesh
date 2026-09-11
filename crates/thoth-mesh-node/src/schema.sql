-- On-disk message store schema (ADR-0045/ADR-0046).
--
-- `include_str!`'d and applied idempotently by `SqliteStore::open`.
-- Every statement here is safe to re-run against an existing database
-- (`IF NOT EXISTS` throughout) - the one thing that genuinely can't be
-- idempotent this way, adding `msg_id` to an already-existing
-- `messages` table from before ADR-0046, is handled separately in
-- Rust *before* this file runs (see `SqliteStore::open`). Keep
-- `SCHEMA_VERSION` in `persistence.rs` in step with what this file
-- creates - it's a human-readable marker for anyone inspecting the
-- database directly, not something any code branches on.

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
--
-- `msg_id` (ADR-0046) is the envelope's own `MessageId` (16 raw
-- bytes) - NULL for a row persisted before that ADR, which a durable
-- subscription's catch-up query (indexed below) simply never matches.
CREATE TABLE IF NOT EXISTS messages (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    topic    TEXT    NOT NULL,
    ts       INTEGER NOT NULL,
    envelope BLOB    NOT NULL,
    msg_id   BLOB
);

CREATE INDEX IF NOT EXISTS messages_topic_seq ON messages (topic, seq);
-- Partial: a row from before ADR-0046 has msg_id = NULL and is never
-- matched by messages_since's `msg_id IS NOT NULL AND msg_id > ?`
-- query - indexing it too would just be dead weight, permanently, on
-- a long-lived database with a lot of pre-ADR-0046 history.
CREATE INDEX IF NOT EXISTS messages_topic_msgid ON messages (topic, msg_id)
    WHERE msg_id IS NOT NULL;

-- The current retained (last-value) message per topic (ADR-0043).
-- A separate table so retention pruning of `messages` never removes
-- a topic's retained value.
CREATE TABLE IF NOT EXISTS retained (
    topic    TEXT PRIMARY KEY,
    envelope BLOB NOT NULL
);

-- The last message durably delivered to `peer_id` on `topic`
-- (ADR-0046) - what a later `Subscribe { durable: true }` from the
-- same authenticated identity resumes from.
CREATE TABLE IF NOT EXISTS subscriber_offsets (
    peer_id      BLOB    NOT NULL,
    topic        TEXT    NOT NULL,
    last_msg_id  BLOB    NOT NULL,
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (peer_id, topic)
);
