use std::marker::PhantomData;

use async_trait::async_trait;

use crate::aggregate::{render_from, Aggregate};
use crate::command::Command;
use crate::error::{ServiceError, StoreError};
use crate::event::{ExpectedRevision, Metadata, Recorded};
use crate::handler::{CommandKind, Handles};
use crate::ids::StreamId;
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

    /// Execute a single, strongly-typed [`CommandKind`] against an instance.
    ///
    /// This is the compile-time-checked counterpart to [`execute`](Self::execute):
    /// the bound `A: Handles<C>` means you can only submit commands the
    /// aggregate actually implements a handler for. Submitting anything else is
    /// a *compile error* rather than a runtime rejection.
    pub async fn submit<C>(
        &self,
        id: &str,
        command: C,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>>
    where
        A: Handles<C>,
        C: CommandKind,
    {
        self.submit_with(id, command, Metadata::new()).await
    }

    /// Like [`submit`](Self::submit) but attaches `metadata` to every event.
    pub async fn submit_with<C>(
        &self,
        id: &str,
        command: C,
        metadata: Metadata,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>>
    where
        A: Handles<C>,
        C: CommandKind,
    {
        let (state, expected) = self.load(id).await?;

        let new_events =
            <A as Handles<C>>::handle(&state, command).map_err(ServiceError::Rejected)?;
        if new_events.is_empty() {
            return Ok(Vec::new());
        }

        let recorded = self
            .store
            .append::<A::Event>(A::TYPE, id, expected, &new_events, &metadata)
            .await?;
        Ok(recorded)
    }

    /// Execute a command, transparently retrying on optimistic-concurrency
    /// conflicts.
    ///
    /// When another writer wins the race the stream is re-loaded and the command
    /// is decided again from the fresh state, up to `max_retries` extra attempts.
    /// Business [`Rejected`](ServiceError::Rejected) outcomes and non-conflict
    /// store errors are returned immediately — only a genuine
    /// [`StoreError::Conflict`] is retried.
    pub async fn execute_with_retry(
        &self,
        id: &str,
        command: A::Command,
        max_retries: u32,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>>
    where
        A::Command: Clone,
    {
        let mut attempts = 0;
        loop {
            match self.execute(id, command.clone()).await {
                Err(ServiceError::Store(StoreError::Conflict { .. })) if attempts < max_retries => {
                    attempts += 1;
                }
                other => return other,
            }
        }
    }

    /// Get a handle bound to a single aggregate instance, so you don't have to
    /// repeat the id on every call.
    pub fn entity(&self, id: impl Into<StreamId>) -> Entity<'_, A, S> {
        Entity {
            service: self,
            id: id.into(),
        }
    }
}

/// A convenience handle bound to one aggregate instance.
///
/// Created via [`Service::entity`]. It simply forwards to the underlying service
/// with the id pre-filled, which reads nicely when you issue several commands
/// against the same stream.
pub struct Entity<'a, A, S> {
    service: &'a Service<A, S>,
    id: StreamId,
}

impl<A, S> Entity<'_, A, S>
where
    A: Aggregate,
    S: EventStore,
{
    /// The id this handle is bound to.
    pub fn id(&self) -> &StreamId {
        &self.id
    }

    /// Render the current state of this instance.
    pub async fn state(&self) -> Result<A, ServiceError<A::Rejection>> {
        Ok(self.service.load(self.id.as_str()).await?.0)
    }

    /// Execute an enum command against this instance.
    pub async fn execute(
        &self,
        command: A::Command,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>> {
        self.service.execute(self.id.as_str(), command).await
    }

    /// Submit a strongly-typed [`CommandKind`] against this instance.
    pub async fn submit<C>(
        &self,
        command: C,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>>
    where
        A: Handles<C>,
        C: CommandKind,
    {
        self.service.submit(self.id.as_str(), command).await
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
