use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::aggregate::View;
use crate::error::StoreError;

use super::{DocumentStore, EventStore};

/// How many events to pull from the log per round-trip while catching up.
const DEFAULT_BATCH: usize = 256;

/// A [`View`] together with the global log position it has consumed up to.
///
/// Keeping the position next to the view is what makes projections resumable:
/// persist the whole `Projection` and you can pick up exactly where you left off.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
pub struct Projection<V> {
    /// The accumulated read model.
    pub view: V,
    /// Global position of the last event folded in (0 means "nothing yet").
    pub position: u64,
}

/// Rebuild a [`View`] from scratch by streaming every relevant event out of the
/// store. Returns the populated [`Projection`] (view + final position).
pub async fn replay_view<V, S>(store: &S) -> Result<Projection<V>, StoreError>
where
    V: View,
    S: EventStore,
{
    let mut projection = Projection::<V>::default();
    loop {
        let batch = store
            .read_all::<V::Event>(projection.position, DEFAULT_BATCH)
            .await?;
        if batch.is_empty() {
            break;
        }
        for event in &batch {
            projection.view.apply(event);
            projection.position = event.global_position;
        }
    }
    Ok(projection)
}

/// Incrementally bring a persisted [`View`] up to date.
///
/// The projection state (view + checkpoint) is stored as a single document in
/// `collection` under `key`. On each call we load it, fold in any events that
/// have arrived since, and save it back. Returns the up-to-date projection.
///
/// This deliberately uses *both* store traits — the event log as the source of
/// truth and the document store for the materialised, queryable result.
pub async fn catch_up_view<V, S, D>(
    events: &S,
    documents: &D,
    collection: &str,
    key: &str,
) -> Result<Projection<V>, StoreError>
where
    V: View + Serialize + DeserializeOwned,
    S: EventStore,
    D: DocumentStore,
{
    let mut projection = documents
        .fetch::<Projection<V>>(collection, key)
        .await?
        .unwrap_or_default();

    let mut changed = false;
    loop {
        let batch = events
            .read_all::<V::Event>(projection.position, DEFAULT_BATCH)
            .await?;
        if batch.is_empty() {
            break;
        }
        for event in &batch {
            projection.view.apply(event);
            projection.position = event.global_position;
            changed = true;
        }
    }

    if changed {
        documents.save(collection, key, &projection).await?;
    }
    Ok(projection)
}
