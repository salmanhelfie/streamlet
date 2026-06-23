//! Run `streamlet` business logic durably on [Restate](https://restate.dev).
//!
//! You write your [`Aggregate`] and [`Service`] exactly once, then choose
//! *where* a command runs:
//!
//! * **in-process** — call [`Service::execute`] directly (it implements
//!   [`Executor`](streamlet::Executor)); or
//! * **durably** — from inside a Restate handler, wrap the same `Service` call
//!   in `ctx.run(..)` using the journaled-action builders in this crate
//!   ([`execute_command`] and [`read_state`]). Restate then journals the result
//!   and replays it instead of re-running the side effect on retries.
//!
//! The two builders return `ctx.run`-ready futures, so a handler is a one-liner:
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use restate_sdk::prelude::*;
//! # use restate_sdk::serde::Json;
//! # use streamlet::{MemoryStore, Service};
//! # use streamlet_restate::{execute_command, read_state};
//! # use serde::{Serialize, Deserialize};
//! # use streamlet::prelude::*;
//! # #[derive(Clone, Serialize, Deserialize, DomainEvent)] enum E { Bumped }
//! # #[derive(Command)] enum C { Bump }
//! # #[derive(Default)] struct Counter { n: i64 }
//! # #[derive(Debug, thiserror::Error)] #[error("no")] struct R;
//! # impl Aggregate for Counter {
//! #   type Command = C; type Event = E; type Rejection = R;
//! #   const TYPE: &'static str = "counter";
//! #   fn handle(&self, _: C) -> Result<Vec<E>, R> { Ok(vec![E::Bumped]) }
//! #   fn apply(&mut self, _: &E) { self.n += 1; }
//! # }
//! #[restate_sdk::object]
//! trait CounterObject {
//!     async fn bump() -> Result<i64, HandlerError>;
//! }
//!
//! struct Impl { service: Arc<Service<Counter, MemoryStore>> }
//!
//! impl CounterObject for Impl {
//!     async fn bump(&self, ctx: ObjectContext<'_>) -> Result<i64, HandlerError> {
//!         let id = ctx.key().to_string();
//!         let service = self.service.clone();
//!         // Durable append — journaled and retried by Restate.
//!         ctx.run(|| execute_command(service.clone(), id.clone(), C::Bump)).await?;
//!         // Durable read of derived state.
//!         let Json(value) = ctx.run(|| read_state(service, id, |c: &Counter| c.n)).await?;
//!         Ok(value)
//!     }
//! }
//! ```
//!
//! Crucially, the rejection / infrastructure split is preserved: a
//! [`ServiceError::Rejected`] becomes a [`TerminalError`] (Restate will *not*
//! retry it), while a [`ServiceError::Store`] stays a retryable
//! [`HandlerError`] — exactly the failures a durable executor exists to retry.

use std::fmt::Display;
use std::sync::Arc;

use restate_sdk::errors::{HandlerError, HandlerResult, TerminalError};
use restate_sdk::serde::Json;

use streamlet::{Aggregate, EventStore, Recorded, Service, ServiceError};

/// Map a [`ServiceError`] onto a Restate [`HandlerError`].
///
/// Business rejections become *terminal* (no retries); store failures stay
/// *retryable*. This is the bridge that keeps the toolkit's two failure modes
/// distinct all the way through Restate's durable executor.
pub fn map_service_error<R: Display>(error: ServiceError<R>) -> HandlerError {
    match error {
        ServiceError::Rejected(rejection) => TerminalError::new(rejection.to_string()).into(),
        ServiceError::Store(store) => store.into(),
    }
}

/// A journaled action that executes a command against an aggregate instance.
///
/// Intended to be handed straight to `ctx.run`:
///
/// ```ignore
/// let Json(events) = ctx
///     .run(|| execute_command(service.clone(), id.clone(), command))
///     .await?;
/// ```
///
/// The whole load → render → decide → append cycle runs inside the journaled
/// closure, so Restate replays the recorded events on retries rather than
/// re-appending. The result is wrapped in [`Json`] because that is how the
/// Restate SDK serialises non-primitive journal values.
pub async fn execute_command<A, S>(
    service: Arc<Service<A, S>>,
    id: String,
    command: A::Command,
) -> HandlerResult<Json<Vec<Recorded<A::Event>>>>
where
    A: Aggregate + 'static,
    S: EventStore + 'static,
    A::Command: Send + 'static,
{
    service
        .execute(&id, command)
        .await
        .map(Json)
        .map_err(map_service_error)
}

/// A journaled action that reads derived state from an aggregate instance.
///
/// The aggregate itself is rarely `Serialize`, so you pass a `project` closure
/// that extracts a serializable summary (a value, a DTO, ...) from the rendered
/// state:
///
/// ```ignore
/// let Json(value) = ctx
///     .run(|| read_state(service.clone(), id.clone(), |c: &Counter| c.value))
///     .await?;
/// ```
pub async fn read_state<A, S, T, F>(
    service: Arc<Service<A, S>>,
    id: String,
    project: F,
) -> HandlerResult<Json<T>>
where
    A: Aggregate + 'static,
    S: EventStore + 'static,
    T: serde::Serialize + serde::de::DeserializeOwned + 'static,
    F: FnOnce(&A) -> T + Send + 'static,
{
    let (state, _revision) = service.load(&id).await.map_err(map_service_error)?;
    Ok(Json(project(&state)))
}
