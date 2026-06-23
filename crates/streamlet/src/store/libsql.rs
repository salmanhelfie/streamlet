//! A persistent [`EventStore`] + [`DocumentStore`] backed by libSQL (SQLite).
//!
//! The event table is strictly append-only: the store only ever `INSERT`s into
//! it, optimistic concurrency is enforced with a `UNIQUE(aggregate_type,
//! stream_id, version)` index, and a monotonic `global_position` (the
//! autoincrement rowid) gives projections a stable order to follow.

use async_trait::async_trait;
use libsql::{params, Builder, Connection, Database};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::error::StoreError;
use crate::event::{DomainEvent, ExpectedRevision, Metadata, Recorded};

use super::{decode, encode, now_millis, DocumentStore, EventStore};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS events (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT    NOT NULL,
    aggregate_type  TEXT    NOT NULL,
    stream_id       TEXT    NOT NULL,
    version         INTEGER NOT NULL,
    event_type      TEXT    NOT NULL,
    payload         TEXT    NOT NULL,
    metadata        TEXT    NOT NULL,
    recorded_at     INTEGER NOT NULL,
    UNIQUE (aggregate_type, stream_id, version)
);
CREATE INDEX IF NOT EXISTS idx_events_stream ON events (aggregate_type, stream_id, version);
CREATE INDEX IF NOT EXISTS idx_events_type   ON events (event_type, global_position);

CREATE TABLE IF NOT EXISTS documents (
    collection TEXT    NOT NULL,
    doc_key    TEXT    NOT NULL,
    body       TEXT    NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (collection, doc_key)
);
";

/// A SQLite-backed store. Cloneable handles are cheap; all access is serialised
/// through an async mutex so reads-then-writes stay consistent (SQLite is a
/// single-writer engine anyway).
pub struct SqliteStore {
    // Kept alive for the lifetime of the store; the connection borrows from it.
    _db: Database,
    conn: Mutex<Connection>,
}

impl SqliteStore {
    /// Open (creating if necessary) a file-backed database and ensure the schema.
    pub async fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_string_lossy().to_string();
        let db = Builder::new_local(path).build().await.map_err(backend)?;
        Self::from_database(db).await
    }

    /// Open a private in-memory database (handy for tests that still want to
    /// exercise the real SQL paths).
    pub async fn open_in_memory() -> Result<Self, StoreError> {
        let db = Builder::new_local(":memory:")
            .build()
            .await
            .map_err(backend)?;
        Self::from_database(db).await
    }

    async fn from_database(db: Database) -> Result<Self, StoreError> {
        let conn = db.connect().map_err(backend)?;
        // Pragmas first: WAL for read/write concurrency, NORMAL sync for a sane
        // durability/throughput trade-off, a busy timeout so brief writer
        // contention waits instead of erroring, and enforced foreign keys.
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = ON;",
        )
        .await
        .map_err(backend)?;
        conn.execute_batch(SCHEMA).await.map_err(backend)?;
        Ok(Self {
            _db: db,
            conn: Mutex::new(conn),
        })
    }
}

fn backend(e: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(e.to_string())
}

#[async_trait]
impl EventStore for SqliteStore {
    async fn append<E: DomainEvent>(
        &self,
        aggregate_type: &str,
        stream_id: &str,
        expected: ExpectedRevision,
        events: &[E],
        metadata: &Metadata,
    ) -> Result<Vec<Recorded<E>>, StoreError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let metadata_json = encode(metadata)?;
        let recorded_at = now_millis();

        let conn = self.conn.lock().await;
        let tx = conn.transaction().await.map_err(backend)?;

        // Current stream version.
        let current: u64 = {
            let mut rows = tx
                .query(
                    "SELECT COALESCE(MAX(version), 0) FROM events WHERE aggregate_type = ?1 AND stream_id = ?2",
                    params![aggregate_type, stream_id],
                )
                .await
                .map_err(backend)?;
            let row = rows.next().await.map_err(backend)?;
            row.map(|r| r.get::<i64>(0))
                .transpose()
                .map_err(backend)?
                .unwrap_or(0) as u64
        };

        expected
            .check(current)
            .map_err(|(expected, actual)| StoreError::Conflict {
                stream_id: stream_id.to_string(),
                expected,
                actual,
            })?;

        let mut recorded = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let version = current + offset as u64 + 1;
            let id = Uuid::now_v7().to_string();
            let event_type = event.event_type();
            let payload = encode(event)?;

            tx.execute(
                "INSERT INTO events \
                 (event_id, aggregate_type, stream_id, version, event_type, payload, metadata, recorded_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    id.clone(),
                    aggregate_type,
                    stream_id,
                    version as i64,
                    event_type,
                    payload,
                    metadata_json.clone(),
                    recorded_at
                ],
            )
            .await
            .map_err(backend)?;

            let global_position = tx.last_insert_rowid() as u64;

            recorded.push(Recorded {
                id,
                aggregate_type: aggregate_type.to_string(),
                stream_id: stream_id.to_string(),
                version,
                global_position,
                event_type: event_type.to_string(),
                payload: event.clone(),
                recorded_at,
                metadata: metadata.clone(),
            });
        }

        tx.commit().await.map_err(backend)?;
        Ok(recorded)
    }

    async fn load<E: DomainEvent>(
        &self,
        aggregate_type: &str,
        stream_id: &str,
    ) -> Result<Vec<Recorded<E>>, StoreError> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT global_position, event_id, aggregate_type, stream_id, version, event_type, payload, metadata, recorded_at \
                 FROM events WHERE aggregate_type = ?1 AND stream_id = ?2 ORDER BY version ASC",
                params![aggregate_type, stream_id],
            )
            .await
            .map_err(backend)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(backend)? {
            out.push(row_to_recorded::<E>(&row)?);
        }
        Ok(out)
    }

    async fn load_from<E: DomainEvent>(
        &self,
        aggregate_type: &str,
        stream_id: &str,
        after_version: u64,
    ) -> Result<Vec<Recorded<E>>, StoreError> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT global_position, event_id, aggregate_type, stream_id, version, event_type, payload, metadata, recorded_at \
                 FROM events WHERE aggregate_type = ?1 AND stream_id = ?2 AND version > ?3 ORDER BY version ASC",
                params![aggregate_type, stream_id, after_version as i64],
            )
            .await
            .map_err(backend)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(backend)? {
            out.push(row_to_recorded::<E>(&row)?);
        }
        Ok(out)
    }

    async fn read_all<E: DomainEvent>(
        &self,
        after_global_position: u64,
        limit: usize,
    ) -> Result<Vec<Recorded<E>>, StoreError> {
        let names = E::event_types();
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT global_position, event_id, aggregate_type, stream_id, version, event_type, payload, metadata, recorded_at \
                 FROM events WHERE global_position > ?1 ORDER BY global_position ASC",
                params![after_global_position as i64],
            )
            .await
            .map_err(backend)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(backend)? {
            let event_type: String = row.get(5).map_err(backend)?;
            if names.contains(&event_type.as_str()) {
                out.push(row_to_recorded::<E>(&row)?);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }
}

fn row_to_recorded<E: DeserializeOwned>(row: &libsql::Row) -> Result<Recorded<E>, StoreError> {
    let global_position: i64 = row.get(0).map_err(backend)?;
    let id: String = row.get(1).map_err(backend)?;
    let aggregate_type: String = row.get(2).map_err(backend)?;
    let stream_id: String = row.get(3).map_err(backend)?;
    let version: i64 = row.get(4).map_err(backend)?;
    let event_type: String = row.get(5).map_err(backend)?;
    let payload_raw: String = row.get(6).map_err(backend)?;
    let metadata_raw: String = row.get(7).map_err(backend)?;
    let recorded_at: i64 = row.get(8).map_err(backend)?;

    Ok(Recorded {
        id,
        aggregate_type,
        stream_id,
        version: version as u64,
        global_position: global_position as u64,
        event_type,
        payload: decode::<E>(&payload_raw)?,
        recorded_at,
        metadata: decode::<Metadata>(&metadata_raw)?,
    })
}

#[async_trait]
impl DocumentStore for SqliteStore {
    async fn save<T>(&self, collection: &str, key: &str, value: &T) -> Result<(), StoreError>
    where
        T: Serialize + Send + Sync,
    {
        let body = encode(value)?;
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO documents (collection, doc_key, body, updated_at) VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT (collection, doc_key) DO UPDATE SET body = excluded.body, updated_at = excluded.updated_at",
            params![collection, key, body, now_millis()],
        )
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn fetch<T>(&self, collection: &str, key: &str) -> Result<Option<T>, StoreError>
    where
        T: DeserializeOwned + Send,
    {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT body FROM documents WHERE collection = ?1 AND doc_key = ?2",
                params![collection, key],
            )
            .await
            .map_err(backend)?;
        match rows.next().await.map_err(backend)? {
            Some(row) => {
                let body: String = row.get(0).map_err(backend)?;
                Ok(Some(decode::<T>(&body)?))
            }
            None => Ok(None),
        }
    }

    async fn delete(&self, collection: &str, key: &str) -> Result<(), StoreError> {
        let conn = self.conn.lock().await;
        conn.execute(
            "DELETE FROM documents WHERE collection = ?1 AND doc_key = ?2",
            params![collection, key],
        )
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn list<T>(&self, collection: &str) -> Result<Vec<(String, T)>, StoreError>
    where
        T: DeserializeOwned + Send,
    {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT doc_key, body FROM documents WHERE collection = ?1 ORDER BY doc_key ASC",
                params![collection],
            )
            .await
            .map_err(backend)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(backend)? {
            let key: String = row.get(0).map_err(backend)?;
            let body: String = row.get(1).map_err(backend)?;
            out.push((key, decode::<T>(&body)?));
        }
        Ok(out)
    }
}
