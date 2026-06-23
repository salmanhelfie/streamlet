//! Catch-up subscriptions and the things built on them.
//!
//! Everything here rides on one primitive — [`pump`] — which walks the event
//! log from a persisted checkpoint, hands each event to a callback, and advances
//! the checkpoint as it goes. That single engine powers:
//!
//! * **projections / read models** (see also [`catch_up_view`](crate::catch_up_view)),
//! * a **transactional-style outbox** via [`EventPublisher`] + [`run_publisher`],
//! * **process managers / sagas** via [`Reactor`] + [`run_reactor`].
//!
//! Delivery is *at-least-once* and *in order*: the checkpoint is advanced after
//! each event, so a crash re-delivers at most the in-flight event. Pair it with
//! [`already_processed`] / [`mark_processed`] when a consumer needs explicit
//! idempotency.

use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::StoreError;
use crate::event::{DomainEvent, Recorded};
use crate::store::{DocumentStore, EventStore};

/// How many events to pull from the log per round-trip.
const DEFAULT_BATCH: usize = 256;

/// Document collection used to store subscription checkpoints.
const CHECKPOINTS: &str = "__checkpoint";

/// Document collection used to store idempotency markers.
const PROCESSED: &str = "__processed";

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct Checkpoint {
    position: u64,
}

async fn load_checkpoint<D: DocumentStore>(documents: &D, name: &str) -> Result<u64, StoreError> {
    Ok(documents
        .fetch::<Checkpoint>(CHECKPOINTS, name)
        .await?
        .map(|c| c.position)
        .unwrap_or(0))
}

async fn save_checkpoint<D: DocumentStore>(
    documents: &D,
    name: &str,
    position: u64,
) -> Result<(), StoreError> {
    documents
        .save(CHECKPOINTS, name, &Checkpoint { position })
        .await
}

/// A boxed, lifetime-bound future — the return type of a [`pump`] callback.
pub type Handled<'a> = Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + 'a>>;

/// Drive `handle` over every event of type `E` recorded after the named
/// subscription's stored checkpoint, advancing the checkpoint after each event.
///
/// Returns the number of events processed in this run. Call it on a timer (or in
/// a loop) to keep a consumer live; it resumes exactly where it left off. The
/// callback returns a boxed future (use `Box::pin(async move { .. })`) so it may
/// borrow the event it is handed.
pub async fn pump<E, S, D, F>(
    events: &S,
    documents: &D,
    name: &str,
    mut handle: F,
) -> Result<u64, StoreError>
where
    E: DomainEvent,
    S: EventStore,
    D: DocumentStore,
    F: for<'a> FnMut(&'a Recorded<E>) -> Handled<'a>,
{
    let mut position = load_checkpoint(documents, name).await?;
    let mut processed = 0u64;

    loop {
        let batch = events.read_all::<E>(position, DEFAULT_BATCH).await?;
        if batch.is_empty() {
            break;
        }
        for event in &batch {
            handle(event).await?;
            position = event.global_position;
            processed += 1;
            save_checkpoint(documents, name, position).await?;
        }
    }

    Ok(processed)
}

/// The current checkpoint position of a named subscription (0 if it has never
/// run).
pub async fn checkpoint_position<D: DocumentStore>(
    documents: &D,
    name: &str,
) -> Result<u64, StoreError> {
    load_checkpoint(documents, name).await
}

/// A destination events can be forwarded to — a broker, a webhook, a log.
///
/// Implement it for your transport; [`run_publisher`] turns it into a resumable
/// outbox by driving it from a checkpoint. The bundled [`InMemoryPublisher`] is
/// handy for tests.
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Forward one recorded event to the destination.
    async fn publish<E: DomainEvent>(&self, event: &Recorded<E>) -> Result<(), StoreError>;
}

/// Drive an [`EventPublisher`] over the log from a checkpoint — a resumable,
/// at-least-once outbox.
pub async fn run_publisher<E, S, D, P>(
    events: &S,
    documents: &D,
    name: &str,
    publisher: &P,
) -> Result<u64, StoreError>
where
    E: DomainEvent,
    S: EventStore,
    D: DocumentStore,
    P: EventPublisher,
{
    let mut position = load_checkpoint(documents, name).await?;
    let mut processed = 0u64;
    loop {
        let batch = events.read_all::<E>(position, DEFAULT_BATCH).await?;
        if batch.is_empty() {
            break;
        }
        for event in &batch {
            publisher.publish(event).await?;
            position = event.global_position;
            processed += 1;
            save_checkpoint(documents, name, position).await?;
        }
    }
    Ok(processed)
}

/// A captured event as seen by [`InMemoryPublisher`].
#[derive(Debug, Clone)]
pub struct PublishedEvent {
    /// The stable event name.
    pub event_type: String,
    /// The originating stream.
    pub stream_id: String,
    /// The event's global log position.
    pub global_position: u64,
    /// The JSON-encoded payload.
    pub payload: serde_json::Value,
}

/// An [`EventPublisher`] that just records what it was asked to publish.
#[derive(Default)]
pub struct InMemoryPublisher {
    published: std::sync::Mutex<Vec<PublishedEvent>>,
}

impl InMemoryPublisher {
    /// A fresh, empty publisher.
    pub fn new() -> Self {
        Self::default()
    }

    /// A snapshot of everything published so far, in order.
    pub fn published(&self) -> Vec<PublishedEvent> {
        self.published.lock().expect("publisher poisoned").clone()
    }
}

#[async_trait]
impl EventPublisher for InMemoryPublisher {
    async fn publish<E: DomainEvent>(&self, event: &Recorded<E>) -> Result<(), StoreError> {
        let payload = serde_json::to_value(&event.payload)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        self.published
            .lock()
            .expect("publisher poisoned")
            .push(PublishedEvent {
                event_type: event.event_type.clone(),
                stream_id: event.stream_id.clone(),
                global_position: event.global_position,
                payload,
            });
        Ok(())
    }
}

/// A process manager / saga: it observes events and reacts — typically by
/// issuing commands to other aggregates (call your services from `react`).
///
/// Drive it with [`run_reactor`], which keeps a checkpoint so reactions resume
/// after a restart.
#[async_trait]
pub trait Reactor: Send + Sync {
    /// The events this reactor consumes.
    type Event: DomainEvent;

    /// React to a single event. Errors are surfaced and stop the run *before*
    /// the checkpoint advances, so the event is retried next time.
    async fn react(&self, event: &Recorded<Self::Event>) -> Result<(), StoreError>;
}

/// Drive a [`Reactor`] over the log from a checkpoint.
pub async fn run_reactor<R, S, D>(
    reactor: &R,
    events: &S,
    documents: &D,
    name: &str,
) -> Result<u64, StoreError>
where
    R: Reactor,
    S: EventStore,
    D: DocumentStore,
{
    let mut position = load_checkpoint(documents, name).await?;
    let mut processed = 0u64;
    loop {
        let batch = events.read_all::<R::Event>(position, DEFAULT_BATCH).await?;
        if batch.is_empty() {
            break;
        }
        for event in &batch {
            reactor.react(event).await?;
            position = event.global_position;
            processed += 1;
            save_checkpoint(documents, name, position).await?;
        }
    }
    Ok(processed)
}

/// Has `event_id` already been processed within `scope`? (Idempotency helper.)
pub async fn already_processed<D: DocumentStore>(
    documents: &D,
    scope: &str,
    event_id: &str,
) -> Result<bool, StoreError> {
    let key = format!("{scope}:{event_id}");
    Ok(documents.fetch::<bool>(PROCESSED, &key).await?.is_some())
}

/// Mark `event_id` as processed within `scope`, so a later
/// [`already_processed`] returns `true`.
pub async fn mark_processed<D: DocumentStore>(
    documents: &D,
    scope: &str,
    event_id: &str,
) -> Result<(), StoreError> {
    let key = format!("{scope}:{event_id}");
    documents.save(PROCESSED, &key, &true).await
}
