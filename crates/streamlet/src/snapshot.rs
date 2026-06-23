//! Snapshots: skip re-folding long streams on every load.
//!
//! Rendering an aggregate normally folds *every* event in its stream. For
//! long-lived streams that gets expensive. A snapshot stores the rendered state
//! at a known version in the [`DocumentStore`](crate::DocumentStore); a later
//! load reads the snapshot and folds only the events recorded *after* it (via
//! [`EventStore::load_from`](crate::EventStore::load_from)).
//!
//! Snapshots are a pure optimisation: deleting them only makes loads slower, and
//! the event log remains the single source of truth.

use serde::{Deserialize, Serialize};

/// A stored snapshot: the rendered aggregate state plus the stream version it
/// reflects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEnvelope<A> {
    /// The stream version this snapshot was taken at.
    pub version: u64,
    /// The rendered aggregate state at that version.
    pub state: A,
}

/// When the service should write a fresh snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotPolicy {
    /// Never snapshot automatically.
    Never,
    /// Snapshot whenever the stream version crosses a multiple of `n`.
    EveryNEvents(u64),
}

impl SnapshotPolicy {
    /// Whether a write that brought the stream to `new_version` (from
    /// `old_version`) should trigger a snapshot.
    pub(crate) fn should_snapshot(self, old_version: u64, new_version: u64) -> bool {
        match self {
            SnapshotPolicy::Never => false,
            SnapshotPolicy::EveryNEvents(0) => false,
            SnapshotPolicy::EveryNEvents(n) => old_version / n != new_version / n,
        }
    }
}

/// The document collection snapshots for aggregate type `ty` live in.
pub(crate) fn collection(ty: &str) -> String {
    format!("__snapshot.{ty}")
}
