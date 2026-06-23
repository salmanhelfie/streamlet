use std::marker::PhantomData;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::aggregate::{render_from, Aggregate};
use crate::command::Command;
use crate::error::{ServiceError, StoreError};
use crate::event::{ExpectedRevision, Metadata, Recorded};
use crate::handler::{CommandKind, Handles, HandlesIn};
use crate::ids::StreamId;
use crate::snapshot::{collection as snapshot_collection, SnapshotEnvelope, SnapshotPolicy};
use crate::store::{DocumentStore, EventStore};

/// A typed application service for a single [`Aggregate`].
///
/// You declare it once — `Service::<Counter, _>::new(store)` — and from then on
/// the type system only lets you feed it `Counter`'s commands. Loading,
/// rendering, deciding and appending are wired together for you, and the two
/// failure modes stay cleanly separated:
///
/// * a broken business rule comes back as [`ServiceError::Rejected`];
/// * a storage/plumbing failure comes back as [`ServiceError::Store`].
///
/// The optional `Env` type parameter is an injected environment (dependencies
/// like clients, clocks or policies). It defaults to `()`; set it with
/// [`Service::with_env`] to drive dependency-aware handlers via
/// [`Service::dispatch`].
pub struct Service<A, S, Env = ()> {
    store: S,
    env: Env,
    _aggregate: PhantomData<fn() -> A>,
}

impl<A, S> Service<A, S, ()>
where
    A: Aggregate,
    S: EventStore,
{
    /// Build a service over the given event store, with no environment.
    pub fn new(store: S) -> Self {
        Self {
            store,
            env: (),
            _aggregate: PhantomData,
        }
    }
}

impl<A, S, Env> Service<A, S, Env>
where
    A: Aggregate,
    S: EventStore,
{
    /// Build a service over the given event store and an injected environment.
    ///
    /// The `env` is shared (by reference) with every dependency-aware handler
    /// dispatched through [`dispatch`](Self::dispatch).
    pub fn with_env(store: S, env: Env) -> Self {
        Self {
            store,
            env,
            _aggregate: PhantomData,
        }
    }

    /// Borrow the underlying store (e.g. to also use it as a `DocumentStore`).
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Borrow the injected environment.
    pub fn env(&self) -> &Env {
        &self.env
    }

    /// The exact set of command names this service handles via the enum path.
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

    /// Execute an enum command against an aggregate instance: load → render →
    /// decide → append. Returns the events that were actually written (empty if
    /// the aggregate chose to emit none).
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
        self.append(id, expected, new_events, metadata).await
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
        self.append(id, expected, new_events, metadata).await
    }

    /// Dispatch a strongly-typed [`CommandKind`] through a *dependency-aware*
    /// handler, passing the service's injected environment.
    ///
    /// The bound `A: HandlesIn<C, Env>` is satisfied automatically for any pure
    /// [`Handles<C>`] command, so `dispatch` works for both pure and
    /// environment-driven handlers on the same aggregate.
    pub async fn dispatch<C>(
        &self,
        id: &str,
        command: C,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>>
    where
        A: HandlesIn<C, Env>,
        C: CommandKind,
        Env: Send + Sync,
    {
        self.dispatch_with(id, command, Metadata::new()).await
    }

    /// Like [`dispatch`](Self::dispatch) but attaches `metadata` to every event.
    pub async fn dispatch_with<C>(
        &self,
        id: &str,
        command: C,
        metadata: Metadata,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>>
    where
        A: HandlesIn<C, Env>,
        C: CommandKind,
        Env: Send + Sync,
    {
        let (state, expected) = self.load(id).await?;
        let new_events = <A as HandlesIn<C, Env>>::handle(&state, command, &self.env)
            .await
            .map_err(ServiceError::Rejected)?;
        self.append(id, expected, new_events, metadata).await
    }

    /// Execute an enum command, transparently retrying on optimistic-concurrency
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
    pub fn entity(&self, id: impl Into<StreamId>) -> Entity<'_, A, S, Env> {
        Entity {
            service: self,
            id: id.into(),
        }
    }

    /// Shared tail of every write path: append non-empty events with the given
    /// metadata, or no-op on an empty decision.
    #[tracing::instrument(
        name = "streamlet.append",
        skip_all,
        fields(aggregate = A::TYPE, stream = id, events = new_events.len())
    )]
    async fn append(
        &self,
        id: &str,
        expected: ExpectedRevision,
        new_events: Vec<A::Event>,
        metadata: Metadata,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>> {
        if new_events.is_empty() {
            tracing::debug!("command produced no events");
            return Ok(Vec::new());
        }
        let recorded = self
            .store
            .append::<A::Event>(A::TYPE, id, expected, &new_events, &metadata)
            .await?;
        tracing::debug!(appended = recorded.len(), "events appended");
        Ok(recorded)
    }
}

impl<A, S, Env> Service<A, S, Env>
where
    A: Aggregate + Serialize + DeserializeOwned,
    S: EventStore + DocumentStore,
{
    /// Load an instance using a snapshot fast-path when one is available.
    ///
    /// Reads the latest [`SnapshotEnvelope`] (if any) from the document store,
    /// then folds only the events recorded *after* it. Falls back to a full
    /// render when there is no snapshot, so the result is always identical to
    /// [`load`](Self::load) — just potentially much cheaper.
    pub async fn load_snapshotted(
        &self,
        id: &str,
    ) -> Result<(A, ExpectedRevision), ServiceError<A::Rejection>> {
        let coll = snapshot_collection(A::TYPE);
        let snapshot: Option<SnapshotEnvelope<A>> = self.store.fetch(&coll, id).await?;
        let (mut state, from_version) = match snapshot {
            Some(s) => (s.state, s.version),
            None => (A::default(), 0),
        };

        let tail = self
            .store
            .load_from::<A::Event>(A::TYPE, id, from_version)
            .await?;
        let mut version = from_version;
        for event in &tail {
            state.apply(&event.payload);
            version = event.version;
        }

        let expected = if version == 0 {
            ExpectedRevision::NoStream
        } else {
            ExpectedRevision::Exact(version)
        };
        Ok((state, expected))
    }

    /// Persist a snapshot of `state` at `version` for instance `id`.
    pub async fn save_snapshot(
        &self,
        id: &str,
        state: &A,
        version: u64,
    ) -> Result<(), ServiceError<A::Rejection>> {
        // Serialize a borrowing envelope so we never have to clone the state.
        #[derive(Serialize)]
        struct SnapshotRef<'a, A> {
            version: u64,
            state: &'a A,
        }
        let coll = snapshot_collection(A::TYPE);
        self.store
            .save(&coll, id, &SnapshotRef { version, state })
            .await?;
        Ok(())
    }

    /// Execute an enum command, then write a snapshot if `policy` says it is due.
    ///
    /// The snapshot reflects the state *after* the command's events are applied,
    /// so subsequent [`load_snapshotted`](Self::load_snapshotted) calls fold
    /// fewer events.
    pub async fn execute_snapshotting(
        &self,
        id: &str,
        command: A::Command,
        policy: SnapshotPolicy,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>> {
        let (mut state, expected) = self.load_snapshotted(id).await?;
        let old_version = match expected {
            ExpectedRevision::Exact(v) => v,
            _ => 0,
        };
        let new_events = state.handle(command).map_err(ServiceError::Rejected)?;
        let recorded = self
            .append(id, expected, new_events, Metadata::new())
            .await?;

        if let Some(last) = recorded.last() {
            if policy.should_snapshot(old_version, last.version) {
                for event in &recorded {
                    state.apply(&event.payload);
                }
                self.save_snapshot(id, &state, last.version).await?;
            }
        }
        Ok(recorded)
    }
}

/// A convenience handle bound to one aggregate instance.
///
/// Created via [`Service::entity`]. It simply forwards to the underlying service
/// with the id pre-filled, which reads nicely when you issue several commands
/// against the same stream.
pub struct Entity<'a, A, S, Env = ()> {
    service: &'a Service<A, S, Env>,
    id: StreamId,
}

impl<A, S, Env> Entity<'_, A, S, Env>
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

    /// Dispatch a strongly-typed command through a dependency-aware handler.
    pub async fn dispatch<C>(
        &self,
        command: C,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>>
    where
        A: HandlesIn<C, Env>,
        C: CommandKind,
        Env: Send + Sync,
    {
        self.service.dispatch(self.id.as_str(), command).await
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
impl<A, S, Env> Executor<A> for Service<A, S, Env>
where
    A: Aggregate,
    S: EventStore,
    Env: Send + Sync,
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

/// Backend-agnostic, *strongly-typed* command submission.
///
/// This is the abstraction that lets business logic be written once and run
/// against any backend — an in-process [`Service`], a future durable executor,
/// a test double — without changing the call site:
///
/// ```ignore
/// async fn top_up<X>(exec: &X, id: &str) -> Result<(), ServiceError<AccountError>>
/// where
///     X: TypedExecutor<Account>,
///     Account: Handles<Deposit>,
/// {
///     exec.submit(id, Deposit(100)).await?;
///     Ok(())
/// }
/// ```
///
/// The `submit` method is generic over the command type and bounded on
/// `A: Handles<C>`, so submitting a command the aggregate doesn't handle is a
/// compile error regardless of which executor you target. (The trait is not
/// `dyn`-safe because of that generic method — use it as a generic bound, the
/// way `top_up` does above.)
#[async_trait]
pub trait TypedExecutor<A: Aggregate>: Send + Sync {
    /// Submit a strongly-typed command to the aggregate instance `id`.
    async fn submit<C>(
        &self,
        id: &str,
        command: C,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>>
    where
        A: Handles<C>,
        C: CommandKind + Send;
}

#[async_trait]
impl<A, S, Env> TypedExecutor<A> for Service<A, S, Env>
where
    A: Aggregate,
    S: EventStore,
    Env: Send + Sync,
{
    async fn submit<C>(
        &self,
        id: &str,
        command: C,
    ) -> Result<Vec<Recorded<A::Event>>, ServiceError<A::Rejection>>
    where
        A: Handles<C>,
        C: CommandKind + Send,
    {
        Service::submit(self, id, command).await
    }
}
