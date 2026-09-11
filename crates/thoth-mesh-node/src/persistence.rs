//! SQLite-backed [`MessageStore`] (ADR-0045): the on-disk durable log
//! every published message is written to when the node is run with
//! `--data-dir`. Also backs durable subscriptions (ADR-0046): each
//! row's `msg_id` and the `subscriber_offsets` table are what let a
//! returning `Subscribe { durable: true }` catch up from an exact
//! position instead of just whatever the in-memory replay buffer still
//! holds.
//!
//! The database is a plain SQLite file an operator can inspect with
//! any SQLite tool and back up with `VACUUM INTO`. The schema lives in
//! `schema.sql` (a real, reviewable SQL file, `include_str!`'d here
//! and applied idempotently on open) rather than inline string
//! literals; a real migration mechanism is deferred until there's an
//! actual second case to generalize from - see the note in that file
//! and [`ensure_msg_id_column`] for the one piece of evolution
//! `schema.sql`'s idempotent statements can't express on their own.

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
use thoth_mesh_broker::MessageStore;
use thoth_mesh_core::{Envelope, MessageId, MessageKind, PeerId, Topic};

/// How many messages per topic the on-disk log keeps before the
/// oldest are pruned (ADR-0045). Far larger than the in-memory replay
/// buffer ([`thoth_mesh_broker::DEFAULT_REPLAY_BUFFER_CAPACITY`],
/// 1024) - surviving *past* that window is the point. Not currently
/// configurable via a CLI flag.
pub const DEFAULT_PERSISTED_MESSAGES_PER_TOPIC: usize = 100_000;

/// The retention sweep runs once every this many `append`s (and once
/// at startup), rather than after every single one.
const PRUNE_EVERY: u64 = 1000;

/// The schema version this build's `schema.sql` creates. Written into
/// the `meta` table on first open; a future migration mechanism will
/// compare against it.
const SCHEMA_VERSION: &str = "1";

/// The DDL applied (idempotently) on every open. A real `.sql` file
/// so it gets syntax highlighting and proper review, not a Rust
/// string literal.
const SCHEMA: &str = include_str!("schema.sql");

/// A durable message log backed by a single SQLite file.
pub struct SqliteStore {
    conn: Mutex<Connection>,
    appends_since_prune: AtomicU64,
}

impl std::fmt::Debug for SqliteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStore").finish_non_exhaustive()
    }
}

impl SqliteStore {
    /// Opens (creating if needed) `<dir>/messages.db`, applies the
    /// schema, and runs one retention sweep.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let conn = Connection::open(dir.join("messages.db")).map_err(to_io)?;
        ensure_msg_id_column(&conn)?;
        conn.execute_batch(SCHEMA).map_err(to_io)?;
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', ?1)",
            [SCHEMA_VERSION],
        )
        .map_err(to_io)?;

        let store = Self {
            conn: Mutex::new(conn),
            appends_since_prune: AtomicU64::new(0),
        };
        store.prune()?;
        Ok(store)
    }

    /// Deletes everything beyond the newest
    /// [`DEFAULT_PERSISTED_MESSAGES_PER_TOPIC`] rows per topic.
    /// `retained` is a separate table and is untouched.
    fn prune(&self) -> std::io::Result<()> {
        self.conn
            .lock()
            .unwrap()
            .execute(
                "DELETE FROM messages WHERE seq IN (
                     SELECT seq FROM (
                         SELECT seq, ROW_NUMBER() OVER (
                             PARTITION BY topic ORDER BY seq DESC
                         ) AS rn
                         FROM messages
                     ) WHERE rn > ?1
                 )",
                [DEFAULT_PERSISTED_MESSAGES_PER_TOPIC],
            )
            .map(|_| ())
            .map_err(to_io)
    }
}

impl MessageStore for SqliteStore {
    fn append(&self, envelope: &Envelope) -> std::io::Result<()> {
        let MessageKind::Publish {
            topic,
            payload,
            retain,
            ..
        } = &envelope.kind
        else {
            return Err(std::io::Error::other(
                "MessageStore::append given a non-Publish envelope",
            ));
        };
        let topic = topic.as_str();
        let bytes = envelope.to_bytes().map_err(std::io::Error::other)?;
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        {
            let mut conn = self.conn.lock().unwrap();
            let tx = conn.transaction().map_err(to_io)?;
            tx.execute(
                "INSERT INTO messages (topic, ts, envelope, msg_id) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![topic, ts, bytes, envelope.id.as_bytes().to_vec()],
            )
            .map_err(to_io)?;
            if *retain {
                if payload.is_empty() {
                    tx.execute("DELETE FROM retained WHERE topic = ?1", [topic])
                        .map_err(to_io)?;
                } else {
                    tx.execute(
                        "INSERT INTO retained (topic, envelope) VALUES (?1, ?2)
                         ON CONFLICT(topic) DO UPDATE SET envelope = excluded.envelope",
                        rusqlite::params![topic, bytes],
                    )
                    .map_err(to_io)?;
                }
            }
            tx.commit().map_err(to_io)?;
        }

        if self.appends_since_prune.fetch_add(1, Ordering::Relaxed) + 1 >= PRUNE_EVERY {
            self.appends_since_prune.store(0, Ordering::Relaxed);
            self.prune()?;
        }
        Ok(())
    }

    fn load_recent(&self, per_topic: usize) -> std::io::Result<Vec<(Topic, Vec<Arc<Envelope>>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT topic, envelope FROM (
                     SELECT topic, envelope, seq, ROW_NUMBER() OVER (
                         PARTITION BY topic ORDER BY seq DESC
                     ) AS rn
                     FROM messages
                 ) WHERE rn <= ?1
                 ORDER BY topic ASC, seq ASC",
            )
            .map_err(to_io)?;
        let rows = stmt
            .query_map([per_topic], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(to_io)?;

        let mut out: Vec<(Topic, Vec<Arc<Envelope>>)> = Vec::new();
        for row in rows {
            let (topic, bytes) = row.map_err(to_io)?;
            let topic: Topic = topic.parse().map_err(std::io::Error::other)?;
            let envelope = Arc::new(Envelope::from_bytes(&bytes).map_err(std::io::Error::other)?);
            match out.last_mut() {
                Some((last_topic, envelopes)) if *last_topic == topic => envelopes.push(envelope),
                _ => out.push((topic, vec![envelope])),
            }
        }
        Ok(out)
    }

    fn load_retained(&self) -> std::io::Result<Vec<(Topic, Arc<Envelope>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT topic, envelope FROM retained")
            .map_err(to_io)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(to_io)?;

        let mut out = Vec::new();
        for row in rows {
            let (topic, bytes) = row.map_err(to_io)?;
            let topic: Topic = topic.parse().map_err(std::io::Error::other)?;
            let envelope = Arc::new(Envelope::from_bytes(&bytes).map_err(std::io::Error::other)?);
            out.push((topic, envelope));
        }
        Ok(out)
    }

    // ADR-0046
    fn messages_since(
        &self,
        topic: &Topic,
        after: MessageId,
    ) -> std::io::Result<Vec<Arc<Envelope>>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT envelope FROM messages
                 WHERE topic = ?1 AND msg_id IS NOT NULL AND msg_id > ?2
                 ORDER BY msg_id ASC",
            )
            .map_err(to_io)?;
        let rows = stmt
            .query_map(
                rusqlite::params![topic.as_str(), after.as_bytes().to_vec()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .map_err(to_io)?;

        let mut out = Vec::new();
        for row in rows {
            let bytes = row.map_err(to_io)?;
            out.push(Arc::new(
                Envelope::from_bytes(&bytes).map_err(std::io::Error::other)?,
            ));
        }
        Ok(out)
    }

    fn load_offset(&self, subscriber: PeerId, topic: &Topic) -> std::io::Result<Option<MessageId>> {
        let conn = self.conn.lock().unwrap();
        let bytes: Option<Vec<u8>> = conn
            .query_row(
                "SELECT last_msg_id FROM subscriber_offsets WHERE peer_id = ?1 AND topic = ?2",
                rusqlite::params![subscriber.as_bytes().to_vec(), topic.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(to_io)?;
        bytes
            .map(|bytes| {
                let raw: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| std::io::Error::other("stored last_msg_id is not 16 bytes"))?;
                Ok(MessageId::from_bytes(raw))
            })
            .transpose()
    }

    fn record_offset(
        &self,
        subscriber: PeerId,
        topic: &Topic,
        message_id: MessageId,
    ) -> std::io::Result<()> {
        let conn = self.conn.lock().unwrap();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        conn.execute(
            "INSERT INTO subscriber_offsets (peer_id, topic, last_msg_id, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(peer_id, topic) DO UPDATE SET
                 last_msg_id = excluded.last_msg_id,
                 updated_at = excluded.updated_at",
            rusqlite::params![
                subscriber.as_bytes().to_vec(),
                topic.as_str(),
                message_id.as_bytes().to_vec(),
                ts
            ],
        )
        .map(|_| ())
        .map_err(to_io)
    }

    // ADR-0047
    fn expire_before(&self, cutoff_ts: i64) -> std::io::Result<Vec<Arc<Envelope>>> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(to_io)?;
        let expired: Vec<Arc<Envelope>> = {
            let mut stmt = tx
                .prepare("SELECT envelope FROM messages WHERE ts < ?1")
                .map_err(to_io)?;
            let rows = stmt
                .query_map([cutoff_ts], |row| row.get::<_, Vec<u8>>(0))
                .map_err(to_io)?;
            let mut out = Vec::new();
            for row in rows {
                let bytes = row.map_err(to_io)?;
                out.push(Arc::new(
                    Envelope::from_bytes(&bytes).map_err(std::io::Error::other)?,
                ));
            }
            out
        };
        tx.execute("DELETE FROM messages WHERE ts < ?1", [cutoff_ts])
            .map_err(to_io)?;
        tx.commit().map_err(to_io)?;
        Ok(expired)
    }
}

/// Adds `messages.msg_id` if it's missing (ADR-0046): the one piece of
/// schema evolution `schema.sql`'s idempotent `CREATE TABLE`/
/// `CREATE INDEX ... IF NOT EXISTS` statements can't express on their
/// own - SQLite has no `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`. A
/// brand-new database never takes this path: `schema.sql` alone
/// already creates `messages` with `msg_id` present from the start, so
/// both checks below come back negative and this is a no-op. Must run
/// *before* `SCHEMA` is applied - `schema.sql`'s own `messages` DDL is
/// `CREATE TABLE IF NOT EXISTS`, so on an existing pre-ADR-0046
/// database it does nothing at all and the gap would otherwise never
/// close.
fn ensure_msg_id_column(conn: &Connection) -> std::io::Result<()> {
    let table_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'messages'",
            [],
            |row| row.get(0),
        )
        .map_err(to_io)?;
    if table_exists == 0 {
        return Ok(());
    }
    let has_msg_id: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name = 'msg_id'",
            [],
            |row| row.get(0),
        )
        .map_err(to_io)?;
    if has_msg_id == 0 {
        conn.execute("ALTER TABLE messages ADD COLUMN msg_id BLOB", [])
            .map_err(to_io)?;
    }
    Ok(())
}

fn to_io(err: rusqlite::Error) -> std::io::Error {
    std::io::Error::other(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use thoth_mesh_core::{MessageKind, PeerId};

    fn publish(topic: &str, payload: &[u8], retain: bool) -> Envelope {
        Envelope::new(
            PeerId::new(),
            MessageKind::Publish {
                topic: topic.parse().unwrap(),
                payload: payload.to_vec(),
                retain,
                content_type: None,
            },
        )
    }

    fn temp_store() -> (tempfile::TempDir, SqliteStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn append_then_load_recent_round_trips_oldest_first_per_topic() {
        let (_dir, store) = temp_store();
        let a1 = publish("weather.updates", b"sunny", false);
        let a2 = publish("weather.updates", b"cloudy", false);
        let b1 = publish("traffic.updates", b"jam", false);
        for e in [&a1, &a2, &b1] {
            store.append(e).unwrap();
        }

        let recent = store.load_recent(10).unwrap();
        let mut by_topic: std::collections::HashMap<String, Vec<_>> =
            std::collections::HashMap::new();
        for (topic, envelopes) in recent {
            by_topic.insert(
                topic.as_str().to_owned(),
                envelopes.iter().map(|e| e.id).collect(),
            );
        }
        assert_eq!(by_topic["weather.updates"], vec![a1.id, a2.id]);
        assert_eq!(by_topic["traffic.updates"], vec![b1.id]);
    }

    #[test]
    fn load_recent_caps_at_per_topic_keeping_the_newest() {
        let (_dir, store) = temp_store();
        let mut ids = Vec::new();
        for i in 0..5 {
            let e = publish("weather.updates", format!("{i}").as_bytes(), false);
            ids.push(e.id);
            store.append(&e).unwrap();
        }
        let recent = store.load_recent(2).unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(
            recent[0].1.iter().map(|e| e.id).collect::<Vec<_>>(),
            ids[3..]
        );
    }

    #[test]
    fn a_retained_publish_is_stored_and_an_empty_one_clears_it() {
        let (_dir, store) = temp_store();
        let set = publish("sensor.temp", b"21C", true);
        store.append(&set).unwrap();
        let retained = store.load_retained().unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].0.as_str(), "sensor.temp");
        assert_eq!(retained[0].1.id, set.id);

        store.append(&publish("sensor.temp", b"", true)).unwrap();
        assert!(store.load_retained().unwrap().is_empty());
    }

    #[test]
    fn a_retained_value_survives_message_retention_pruning() {
        let (_dir, store) = temp_store();
        let retained = publish("sensor.temp", b"21C", true);
        store.append(&retained).unwrap();
        // Push the retained message itself out of the `messages` cap.
        for i in 0..5 {
            store
                .append(&publish(
                    "sensor.temp",
                    format!("noise {i}").as_bytes(),
                    false,
                ))
                .unwrap();
        }
        store.prune().unwrap();

        // Gone from the message log...
        let recent = store.load_recent(2).unwrap();
        assert!(!recent[0].1.iter().any(|e| e.id == retained.id));
        // ...but still the retained value.
        assert_eq!(store.load_retained().unwrap()[0].1.id, retained.id);
    }

    #[test]
    fn data_survives_reopening_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let sent = {
            let store = SqliteStore::open(dir.path()).unwrap();
            let e = publish("weather.updates", b"sunny", true);
            store.append(&e).unwrap();
            e
        };
        let reopened = SqliteStore::open(dir.path()).unwrap();
        assert_eq!(reopened.load_recent(10).unwrap()[0].1[0].id, sent.id);
        assert_eq!(reopened.load_retained().unwrap()[0].1.id, sent.id);
    }

    // ADR-0046

    /// The zero `MessageId` - older than anything ever generated by
    /// [`MessageId::new`] (a UUIDv7 timestamped at creation), so
    /// `messages_since` against it returns every msg_id-tagged row for
    /// the topic.
    fn epoch() -> MessageId {
        MessageId::from_bytes([0u8; 16])
    }

    #[test]
    fn messages_since_returns_only_what_came_after_in_id_order() {
        let (_dir, store) = temp_store();
        let a = publish("weather.updates", b"1", false);
        let b = publish("weather.updates", b"2", false);
        let c = publish("weather.updates", b"3", false);
        let other_topic = publish("traffic.updates", b"jam", false);
        for e in [&a, &b, &c, &other_topic] {
            store.append(e).unwrap();
        }

        let topic: Topic = "weather.updates".parse().unwrap();
        let since_a = store.messages_since(&topic, a.id).unwrap();
        assert_eq!(
            since_a.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![b.id, c.id]
        );

        let since_epoch = store.messages_since(&topic, epoch()).unwrap();
        assert_eq!(
            since_epoch.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![a.id, b.id, c.id]
        );

        let since_c = store.messages_since(&topic, c.id).unwrap();
        assert!(since_c.is_empty());
    }

    #[test]
    fn load_offset_is_none_until_a_position_is_recorded_then_round_trips() {
        let (_dir, store) = temp_store();
        let topic: Topic = "weather.updates".parse().unwrap();
        let subscriber = PeerId::new();
        assert_eq!(store.load_offset(subscriber, &topic).unwrap(), None);

        let first = MessageId::new();
        store.record_offset(subscriber, &topic, first).unwrap();
        assert_eq!(store.load_offset(subscriber, &topic).unwrap(), Some(first));

        // Recording again overwrites rather than erroring or
        // accumulating a second row (PRIMARY KEY(peer_id, topic)).
        let second = MessageId::new();
        store.record_offset(subscriber, &topic, second).unwrap();
        assert_eq!(store.load_offset(subscriber, &topic).unwrap(), Some(second));
    }

    #[test]
    fn distinct_subscribers_and_topics_have_independent_offsets() {
        let (_dir, store) = temp_store();
        let weather: Topic = "weather.updates".parse().unwrap();
        let traffic: Topic = "traffic.updates".parse().unwrap();
        let alice = PeerId::new();
        let bob = PeerId::new();

        let alice_weather = MessageId::new();
        let alice_traffic = MessageId::new();
        let bob_weather = MessageId::new();
        store.record_offset(alice, &weather, alice_weather).unwrap();
        store.record_offset(alice, &traffic, alice_traffic).unwrap();
        store.record_offset(bob, &weather, bob_weather).unwrap();

        assert_eq!(
            store.load_offset(alice, &weather).unwrap(),
            Some(alice_weather)
        );
        assert_eq!(
            store.load_offset(alice, &traffic).unwrap(),
            Some(alice_traffic)
        );
        assert_eq!(store.load_offset(bob, &weather).unwrap(), Some(bob_weather));
        assert_eq!(store.load_offset(bob, &traffic).unwrap(), None);
    }

    /// A database from before ADR-0046 (a `messages` table with no
    /// `msg_id` column, built by hand here to simulate one) opens
    /// without error - `ensure_msg_id_column` adds the column - and its
    /// pre-existing rows, having no `msg_id`, are simply invisible to
    /// `messages_since`; only messages appended after the upgrade are.
    #[test]
    fn opening_a_pre_adr_0046_database_adds_the_msg_id_column_without_backfilling() {
        let dir = tempfile::tempdir().unwrap();
        {
            let conn = Connection::open(dir.path().join("messages.db")).unwrap();
            conn.execute_batch(
                "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 CREATE TABLE messages (
                     seq      INTEGER PRIMARY KEY AUTOINCREMENT,
                     topic    TEXT    NOT NULL,
                     ts       INTEGER NOT NULL,
                     envelope BLOB    NOT NULL
                 );
                 CREATE TABLE retained (topic TEXT PRIMARY KEY, envelope BLOB NOT NULL);",
            )
            .unwrap();
            let old = publish("weather.updates", b"before the upgrade", false);
            conn.execute(
                "INSERT INTO messages (topic, ts, envelope) VALUES (?1, ?2, ?3)",
                rusqlite::params!["weather.updates", 0i64, old.to_bytes().unwrap()],
            )
            .unwrap();
        }

        let store = SqliteStore::open(dir.path()).unwrap();
        let topic: Topic = "weather.updates".parse().unwrap();

        // The pre-existing row is still there for the ordinary
        // in-memory rehydration path...
        assert_eq!(store.load_recent(10).unwrap()[0].1.len(), 1);
        // ...but has no msg_id, so a durable catch-up query never sees
        // it, from any position - there's nothing to backfill it from.
        assert!(store.messages_since(&topic, epoch()).unwrap().is_empty());

        // A message appended after the upgrade does get a msg_id and
        // is visible to messages_since.
        let after = publish("weather.updates", b"after the upgrade", false);
        store.append(&after).unwrap();
        assert_eq!(
            store
                .messages_since(&topic, epoch())
                .unwrap()
                .iter()
                .map(|e| e.id)
                .collect::<Vec<_>>(),
            vec![after.id]
        );
    }

    /// Opening a database that already has `msg_id` (the ordinary,
    /// post-ADR-0046 case) doesn't re-run the `ALTER TABLE` - which
    /// would fail with a duplicate-column error - and is a plain no-op.
    #[test]
    fn opening_an_already_current_database_twice_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = SqliteStore::open(dir.path()).unwrap();
            store
                .append(&publish("weather.updates", b"sunny", false))
                .unwrap();
        }
        // Reopening must not error - ensure_msg_id_column sees the
        // column already present and does nothing further.
        let reopened = SqliteStore::open(dir.path()).unwrap();
        assert_eq!(reopened.load_recent(10).unwrap()[0].1.len(), 1);
    }

    // ADR-0047

    /// Inserts `envelope` directly at `ts`, bypassing `append`'s own
    /// `SystemTime::now()` - the point of every `expire_before` test
    /// below is controlling exactly how old a row is.
    fn insert_at(store: &SqliteStore, envelope: &Envelope, ts: i64) {
        let MessageKind::Publish { topic, .. } = &envelope.kind else {
            panic!("insert_at given a non-Publish envelope");
        };
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO messages (topic, ts, envelope, msg_id) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    topic.as_str(),
                    ts,
                    envelope.to_bytes().unwrap(),
                    envelope.id.as_bytes().to_vec()
                ],
            )
            .unwrap();
    }

    #[test]
    fn expire_before_deletes_and_returns_only_the_older_rows() {
        let (_dir, store) = temp_store();
        let old = publish("weather.updates", b"old", false);
        let recent = publish("weather.updates", b"recent", false);
        insert_at(&store, &old, 1_000);
        insert_at(&store, &recent, 2_000);

        let expired = store.expire_before(1_500).unwrap();
        assert_eq!(
            expired.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![old.id]
        );

        let remaining = store.load_recent(10).unwrap();
        assert_eq!(
            remaining[0].1.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![recent.id]
        );
    }

    #[test]
    fn expire_before_is_a_no_op_when_nothing_is_old_enough() {
        let (_dir, store) = temp_store();
        let recent = publish("weather.updates", b"recent", false);
        insert_at(&store, &recent, 2_000);

        assert!(store.expire_before(1_000).unwrap().is_empty());
        assert_eq!(store.load_recent(10).unwrap()[0].1[0].id, recent.id);
    }

    /// `retained` has no `ts` column at all - a retained value can
    /// never be touched by `expire_before`, regardless of how old the
    /// `messages` row that originally set it gets.
    #[test]
    fn expire_before_never_touches_a_retained_value() {
        let (_dir, store) = temp_store();
        let retained = publish("sensor.temp", b"21C", true);
        insert_at(&store, &retained, 1_000);
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO retained (topic, envelope) VALUES (?1, ?2)",
                rusqlite::params!["sensor.temp", retained.to_bytes().unwrap()],
            )
            .unwrap();

        let expired = store.expire_before(2_000).unwrap();
        assert_eq!(
            expired.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![retained.id]
        );
        assert_eq!(store.load_retained().unwrap()[0].1.id, retained.id);
    }
}
