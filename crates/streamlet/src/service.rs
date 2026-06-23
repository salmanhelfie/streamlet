use std::marker::PhantomData;

use async_trait::async_trait;

use crate::aggregate::{render_from, Aggregate};
use crate::command::Command;
use crate::error::ServiceError;
use crate::event::{ExpectedRevision, Metadata, Recorded};
use crate::store::EventStore;

/// A typed application service for a single [`Aggregate`].
///
/// You declare it once — `Service::<Counter, _>::new(store)` — and from then on
/// the type system only lets you feed it `Counter::Command` values. Loading,
/// rendering, deciding and appending are wired together for you, and the two
/// failure modes stay cleanly separated:
///
/// * a broken business rule comes back as [`ServiceError::Rejected`];
/// * a storage/plumbing failure comes back as [`ServiceError::Store`].
pub struct Service<A, S> {
    store: S,
    _aggregate: PhantomData<fn() -> A>,
}

impl<A, S> Service<A, S>
where
    A: Aggregate,
    S: EventStore,
{
    /// Build a service over the given event store.
    pub fn new(store: S) -> Self {
        Self {
            store,
            _aggregate: PhantomData,
        }
    }

    /// Borrow the underlying store (e.g. to also use it as a `DocumentStore`).
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The exact set of command names this service handles. This is the
    /// type-level promise made concrete: a service for aggregate `A` accepts
    /// precisely the commands `A::Command` declares, nothing more.
    pub fn handled_commands() -> &'static [&'static str] {
        <A::Command as Command>::command_types()
    }

    /// Load and render the current state of one aggregate instance, alongside
    /// the [`ExpectedRevision`] needed to safely append to it next.
    pub async fn load(
        &self,
        id: &str,
    ) -> Result<(A, ExpectedRevision), ServiceError<A::Rejection>> {
        let events = self.store.load::<A::Event>(A::TYPE, id).await?;
        let version = events.last().map(|e| e.version).unwrap_or(0);
        let state = render_from::<A>(&events);
        let expected = if version == 0 {
            ExpectedRevision::NoStream
        } else {
            ExpectedRevision::Exact(version)
        };
        Ok((state, expected))
    }

    /// Execute a command against an aggregate instance: load → render → decide →
    /// append. Returns the events that were actually written (empty if the
    /// aggregate chose to emit none).
    pub async fn execute(
        &self,
        id: &str,
        command: A::Command,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>> {
        self.execute_with(id, command, Metadata::new()).await
    }

    /// Like [`execute`](Self::execute) but attaches `metadata` to every event
    /// written by this command.
    pub async fn execute_with(
        &self,
        id: &str,
        command: A::Command,
        metadata: Metadata,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>> {
        let (state, expected) = self.load(id).await?;

        // Business decision. A rejection here is a domain outcome, not a bug or
        // an outage — keep it in its own arm.
        let new_events = state.handle(command).map_err(ServiceError::Rejected)?;
        if new_events.is_empty() {
            return Ok(Vec::new());
        }

        // Infrastructure. Anything that goes wrong here is a `StoreError`.
        let recorded = self
            .store
            .append::<A::Event>(A::TYPE, id, expected, &new_events, &metadata)
            .await?;
        Ok(recorded)
    }
}

/// Abstraction over *where* a command runs.
///
/// Both the plain in-process [`Service`] and a durable, Restate-backed executor
/// implement this, so the same call site can be pointed at either one without
/// touching your domain code.
#[async_trait]
pub trait Executor<A: Aggregate>: Send + Sync {
    /// Execute a command for the aggregate instance `id`.
    async fn execute(
        &self,
        id: &str,
        command: A::Command,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>>;
}

#[async_trait]
impl<A, S> Executor<A> for Service<A, S>
where
    A: Aggregate,
    S: EventStore,
    A::Command: 'static,
{
    async fn execute(
        &self,
        id: &str,
        command: A::Command,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>> {
        Service::execute(self, id, command).await
    }
}
