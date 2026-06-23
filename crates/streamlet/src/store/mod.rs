//! Storage traits and implementations.
//!
//! The toolkit talks to persistence through two small traits:
//!
//! * [`EventStore`] — an append-only log of [`Recorded`] events, with optimistic
//!   concurrency and a global ordering that projections can follow.
//! * [`DocumentStore`] — a tiny key/value document store for read models and
//!   projection checkpoints.
//!
//! Two implementations ship in-box: [`memory::MemoryStore`] (feature `memory`,
//! on by default) and [`libsql::SqliteStore`] (feature `libsql`).

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::StoreError;
use crate::event::{DomainEvent, ExpectedRevision, Metadata, Recorded};

#[cfg(feature = "libsql")]
pub mod libsql;
#[cfg(feature = "memory")]
pub mod memory;

mod projection;
pub use projection::{catch_up_view, replay_view, Projection};

/// An append-only event log.
///
/// Implementations must guarantee that, within a single `(aggregate_type,
/// stream_id)` stream, versions are contiguous starting at 1, and that the
/// `global_position` is strictly increasing across every append.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Append `events` to a stream, enforcing `expected` for optimistic
    /// concurrency. Returns the events as stored (with ids, versions, positions
    /// and timestamps filled in). Appending an empty slice is a no-op.
    async fn append<E: DomainEvent>(
        &self,
        aggregate_type: &str,
        stream_id: &str,
        expected: ExpectedRevision,
        events: &[E],
        metadata: &Metadata,
    ) -> Result<Vec<Recorded<E>>, StoreError>;

    /// Load the full event stream for one aggregate instance, in order.
    async fn load<E: DomainEvent>(
        &self,
        aggregate_type: &str,
        stream_id: &str,
    ) -> Result<Vec<Recorded<E>>, StoreError>;

    /// Read events of type `E` across *all* streams, ordered by
    /// `global_position`, starting strictly after `after_global_position`.
    ///
    /// Only events whose stored name is in [`DomainEvent::event_types`] are
    /// returned, so this is safe to call with any event enum — it filters the
    /// log down to the events that enum understands. This is the primitive that
    /// powers projections.
    async fn read_all<E: DomainEvent>(
        &self,
        after_global_position: u64,
        limit: usize,
    ) -> Result<Vec<Recorded<E>>, StoreError>;
}

/// A minimal document store for read models / projections.
#[async_trait]
pub trait DocumentStore: Send + Sync {
    /// Insert or replace a document.
    async fn save<T>(&self, collection: &str, key: &str, value: &T) -> Result<(), StoreError>
    where
        T: Serialize + Send + Sync;

    /// Fetch a document, or `None` if absent.
    async fn fetch<T>(&self, collection: &str, key: &str) -> Result<Option<T>, StoreError>
    where
        T: DeserializeOwned + Send;

    /// Delete a document. Deleting a missing key is not an error.
    async fn delete(&self, collection: &str, key: &str) -> Result<(), StoreError>;

    /// List every `(key, document)` pair in a collection.
    async fn list<T>(&self, collection: &str) -> Result<Vec<(String, T)>, StoreError>
    where
        T: DeserializeOwned + Send;
}

/// Current epoch time in milliseconds; shared by store implementations.
pub(crate) fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Encode an event payload to JSON text, mapping failures to [`StoreError`].
pub(crate) fn encode<E: Serialize>(event: &E) -> Result<String, StoreError> {
    serde_json::to_string(event).map_err(|e| StoreError::Serialization(e.to_string()))
}

/// Decode an event payload from JSON text, mapping failures to [`StoreError`].
pub(crate) fn decode<E: DeserializeOwned>(raw: &str) -> Result<E, StoreError> {
    serde_json::from_str(raw).map_err(|e| StoreError::Serialization(e.to_string()))
}
