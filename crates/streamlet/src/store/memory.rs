use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use crate::error::StoreError;
use crate::event::{DomainEvent, ExpectedRevision, Metadata, Recorded};

use super::{decode, encode, now_millis, DocumentStore, EventStore};

/// A thread-safe, in-process [`EventStore`] + [`DocumentStore`].
///
/// Everything lives in a `Vec`/`HashMap` behind a `Mutex`. It is the
/// recommended store for unit tests and quick experiments — fast, dependency
/// free, and behaviourally identical (concurrency checks, ordering) to the
/// persistent backends.
#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Globally-ordered log; index + 1 is the `global_position`.
    log: Vec<StoredEvent>,
    /// collection -> (key -> json document)
    documents: HashMap<String, HashMap<String, String>>,
}

#[derive(Clone)]
struct StoredEvent {
    id: String,
    aggregate_type: String,
    stream_id: String,
    version: u64,
    event_type: String,
    payload: String,
    recorded_at: i64,
    metadata: Metadata,
}

impl MemoryStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Total number of events across all streams (useful in tests).
    pub fn len(&self) -> usize {
        self.inner.lock().expect("memory store poisoned").log.len()
    }

    /// `true` if no events have been appended yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl StoredEvent {
    fn into_recorded<E>(self, global_position: u64) -> Result<Recorded<E>, StoreError>
    where
        E: DeserializeOwned,
    {
        Ok(Recorded {
            payload: decode::<E>(&self.payload)?,
            id: self.id,
            aggregate_type: self.aggregate_type,
            stream_id: self.stream_id,
            version: self.version,
            global_position,
            event_type: self.event_type,
            recorded_at: self.recorded_at,
            metadata: self.metadata,
        })
    }
}

#[async_trait]
impl EventStore for MemoryStore {
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

        let mut inner = self.inner.lock().expect("memory store poisoned");

        let current = inner
            .log
            .iter()
            .filter(|e| e.aggregate_type == aggregate_type && e.stream_id == stream_id)
            .map(|e| e.version)
            .max()
            .unwrap_or(0);

        expected
            .check(current)
            .map_err(|(expected, actual)| StoreError::Conflict {
                stream_id: stream_id.to_string(),
                expected,
                actual,
            })?;

        let recorded_at = now_millis();
        let mut recorded = Vec::with_capacity(events.len());

        for (offset, event) in events.iter().enumerate() {
            let version = current + offset as u64 + 1;
            let stored = StoredEvent {
                id: Uuid::now_v7().to_string(),
                aggregate_type: aggregate_type.to_string(),
                stream_id: stream_id.to_string(),
                version,
                event_type: event.event_type().to_string(),
                payload: encode(event)?,
                recorded_at,
                metadata: metadata.clone(),
            };
            inner.log.push(stored.clone());
            let global_position = inner.log.len() as u64;
            recorded.push(Recorded {
                id: stored.id,
                aggregate_type: stored.aggregate_type,
                stream_id: stored.stream_id,
                version,
                global_position,
                event_type: stored.event_type,
                payload: event.clone(),
                recorded_at,
                metadata: metadata.clone(),
            });
        }

        Ok(recorded)
    }

    async fn load<E: DomainEvent>(
        &self,
        aggregate_type: &str,
        stream_id: &str,
    ) -> Result<Vec<Recorded<E>>, StoreError> {
        let inner = self.inner.lock().expect("memory store poisoned");
        let mut out = Vec::new();
        for (idx, stored) in inner.log.iter().enumerate() {
            if stored.aggregate_type == aggregate_type && stored.stream_id == stream_id {
                out.push(stored.clone().into_recorded::<E>(idx as u64 + 1)?);
            }
        }
        out.sort_by_key(|e| e.version);
        Ok(out)
    }

    async fn load_from<E: DomainEvent>(
        &self,
        aggregate_type: &str,
        stream_id: &str,
        after_version: u64,
    ) -> Result<Vec<Recorded<E>>, StoreError> {
        let inner = self.inner.lock().expect("memory store poisoned");
        let mut out = Vec::new();
        for (idx, stored) in inner.log.iter().enumerate() {
            if stored.aggregate_type == aggregate_type
                && stored.stream_id == stream_id
                && stored.version > after_version
            {
                out.push(stored.clone().into_recorded::<E>(idx as u64 + 1)?);
            }
        }
        out.sort_by_key(|e| e.version);
        Ok(out)
    }

    async fn read_all<E: DomainEvent>(
        &self,
        after_global_position: u64,
        limit: usize,
    ) -> Result<Vec<Recorded<E>>, StoreError> {
        let names = E::event_types();
        let inner = self.inner.lock().expect("memory store poisoned");
        let mut out = Vec::new();
        for (idx, stored) in inner.log.iter().enumerate() {
            let position = idx as u64 + 1;
            if position <= after_global_position {
                continue;
            }
            if names.contains(&stored.event_type.as_str()) {
                out.push(stored.clone().into_recorded::<E>(position)?);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl DocumentStore for MemoryStore {
    async fn save<T>(&self, collection: &str, key: &str, value: &T) -> Result<(), StoreError>
    where
        T: Serialize + Send + Sync,
    {
        let json = encode(value)?;
        let mut inner = self.inner.lock().expect("memory store poisoned");
        inner
            .documents
            .entry(collection.to_string())
            .or_default()
            .insert(key.to_string(), json);
        Ok(())
    }

    async fn fetch<T>(&self, collection: &str, key: &str) -> Result<Option<T>, StoreError>
    where
        T: DeserializeOwned + Send,
    {
        let inner = self.inner.lock().expect("memory store poisoned");
        match inner.documents.get(collection).and_then(|c| c.get(key)) {
            Some(json) => Ok(Some(decode::<T>(json)?)),
            None => Ok(None),
        }
    }

    async fn delete(&self, collection: &str, key: &str) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().expect("memory store poisoned");
        if let Some(c) = inner.documents.get_mut(collection) {
            c.remove(key);
        }
        Ok(())
    }

    async fn list<T>(&self, collection: &str) -> Result<Vec<(String, T)>, StoreError>
    where
        T: DeserializeOwned + Send,
    {
        let inner = self.inner.lock().expect("memory store poisoned");
        let mut out = Vec::new();
        if let Some(c) = inner.documents.get(collection) {
            for (k, json) in c {
                out.push((k.clone(), decode::<T>(json)?));
            }
        }
        Ok(out)
    }
}
