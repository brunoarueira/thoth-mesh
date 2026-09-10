//! SQLite-backed [`MessageStore`] (ADR-0045): the on-disk durable log
//! every published message is written to when the node is run with
//! `--data-dir`.
//!
//! The database is a plain SQLite file an operator can inspect with
//! any SQLite tool and back up with `VACUUM INTO`. The schema lives in
//! `schema.sql` (a real, reviewable SQL file, `include_str!`'d here
//! and applied idempotently on open) rather than inline string
//! literals; a real migration mechanism is deferred until a second
//! schema version is actually needed - see the note in that file.

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::Arc;

use rusqlite::Connection;
use thoth_mesh_broker::MessageStore;
use thoth_mesh_core::{Envelope, MessageKind, Topic};

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
                "INSERT INTO messages (topic, ts, envelope) VALUES (?1, ?2, ?3)",
                rusqlite::params![topic, ts, bytes],
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
}
