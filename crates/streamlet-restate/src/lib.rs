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

use streamlet::{Aggregate, CommandKind, EventStore, Handles, Recorded, Service, ServiceError};

#[doc(hidden)]
pub use paste;

/// Re-export of [`streamlet::declare_service!`] so the unified [`service!`]
/// macro can emit the in-process service without the caller importing it.
#[doc(inline)]
pub use streamlet::declare_service;

/// Re-exports used by the [`durable_object!`] macro so callers don't have to
/// import streamlet's types directly. Not part of the stable API.
#[doc(hidden)]
pub mod reexport {
    pub use streamlet::{Aggregate, Recorded, Service};
}

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

/// A journaled action that submits a strongly-typed [`CommandKind`] against an
/// aggregate instance — the durable counterpart to
/// [`Service::submit`](streamlet::Service::submit).
///
/// This is what the [`durable_object!`] macro calls under the hood, but it is
/// also usable directly inside any `ctx.run(..)`:
///
/// ```ignore
/// let Json(events) = ctx
///     .run(|| submit_command(service.clone(), id.clone(), Deposit(100)))
///     .await?;
/// ```
pub async fn submit_command<A, S, C>(
    service: Arc<Service<A, S>>,
    id: String,
    command: C,
) -> HandlerResult<Json<Vec<Recorded<A::Event>>>>
where
    A: Aggregate + Handles<C> + 'static,
    S: EventStore + 'static,
    C: CommandKind + Send + 'static,
{
    service
        .submit(&id, command)
        .await
        .map(Json)
        .map_err(map_service_error)
}

/// Generate a Restate Virtual Object from an aggregate and a command list — the
/// durable sibling of [`streamlet::declare_service!`].
///
/// One declaration yields both the `#[restate_sdk::object]` trait and a ready
/// `<Name>Server` implementation. Each method takes its command as JSON input,
/// submits it through [`submit_command`] inside `ctx.run` (so Restate journals
/// the append and replays it on retry), and returns the recorded events. The
/// rejection / infrastructure split is preserved end to end: business
/// rejections become a `TerminalError`, store failures stay retryable.
///
/// Each command type must derive `CommandKind`, be `Clone`, and be
/// `serde::Serialize + serde::Deserialize` (Restate needs to (de)serialize the
/// handler input). The aggregate must implement `Handles<C>` for each command.
///
/// ```ignore
/// durable_object! {
///     /// One bank account per object key.
///     pub object AccountObject for Account, store = MemoryStore {
///         open     => Open,
///         deposit  => Deposit,
///         withdraw => Withdraw,
///     }
/// }
///
/// // Generates `trait AccountObject` and `struct AccountObjectServer`:
/// let server = AccountObjectServer::new(Arc::new(Service::new(MemoryStore::new())));
/// // HttpServer::new(Endpoint::builder().bind(server.serve()).build())...
/// ```
#[macro_export]
macro_rules! durable_object {
    (
        $(#[$meta:meta])*
        $vis:vis object $obj:ident for $agg:ty, store = $store:ty {
            $( $(#[$mmeta:meta])* $method:ident => $cmd:ty ),* $(,)?
        }
    ) => {
        #[::restate_sdk::object]
        $(#[$meta])*
        $vis trait $obj {
            $(
                $(#[$mmeta])*
                async fn $method(
                    command: ::restate_sdk::serde::Json<$cmd>,
                ) -> ::core::result::Result<
                    ::restate_sdk::serde::Json<
                        ::std::vec::Vec<
                            $crate::reexport::Recorded<<$agg as $crate::reexport::Aggregate>::Event>,
                        >,
                    >,
                    ::restate_sdk::errors::HandlerError,
                >;
            )*
        }

        $crate::paste::paste! {
            #[doc = concat!("Restate handler implementation for [`", stringify!($obj), "`].")]
            #[derive(::core::clone::Clone)]
            $vis struct [<$obj Server>] {
                service: ::std::sync::Arc<$crate::reexport::Service<$agg, $store>>,
            }

            impl [<$obj Server>] {
                /// Wrap a service so it can be served as a Restate object.
                $vis fn new(
                    service: ::std::sync::Arc<$crate::reexport::Service<$agg, $store>>,
                ) -> Self {
                    Self { service }
                }
            }

            impl $obj for [<$obj Server>] {
                $(
                    async fn $method(
                        &self,
                        ctx: ::restate_sdk::prelude::ObjectContext<'_>,
                        command: ::restate_sdk::serde::Json<$cmd>,
                    ) -> ::core::result::Result<
                        ::restate_sdk::serde::Json<
                            ::std::vec::Vec<
                                $crate::reexport::Recorded<<$agg as $crate::reexport::Aggregate>::Event>,
                            >,
                        >,
                        ::restate_sdk::errors::HandlerError,
                    > {
                        let id = ctx.key().to_string();
                        let service = ::core::clone::Clone::clone(&self.service);
                        let ::restate_sdk::serde::Json(command) = command;
                        let result = ctx
                            .run(|| $crate::submit_command(
                                ::core::clone::Clone::clone(&service),
                                id.clone(),
                                ::core::clone::Clone::clone(&command),
                            ))
                            .await?;
                        ::core::result::Result::Ok(result)
                    }
                )*
            }
        }
    };
}

/// One declaration, *both* backends — the unified sibling of wee-events'
/// `service!`.
///
/// From a single aggregate + command list this emits **both** the in-process
/// typed service (via [`streamlet::declare_service!`]) **and** the durable
/// Restate object (via [`durable_object!`]). Business code can then target the
/// in-process `Service`/[`TypedExecutor`](streamlet::TypedExecutor) for tests
/// and the generated Restate object for production from the same source of
/// truth.
///
/// ```ignore
/// streamlet_restate::service! {
///     /// A bank account, in-process and durable.
///     pub Account, store = MemoryStore {
///         service AccountService,   // -> struct AccountService<S>
///         object  AccountObject,    // -> trait AccountObject + struct AccountObjectServer
///         commands {
///             open     => Open,
///             deposit  => Deposit,
///             withdraw => Withdraw,
///         }
///     }
/// }
/// ```
///
/// The command list obeys the same rules as [`durable_object!`]: each command
/// derives `CommandKind`, is `Clone`, and is `serde::Serialize + Deserialize`,
/// and the aggregate implements `Handles<C>` for each.
#[macro_export]
macro_rules! service {
    (
        $(#[$meta:meta])*
        $vis:vis $agg:ty, store = $store:ty {
            service $svc:ident,
            object  $obj:ident,
            commands { $( $(#[$mmeta:meta])* $method:ident => $cmd:ty ),* $(,)? }
        }
    ) => {
        $crate::declare_service! {
            $(#[$meta])*
            $vis service $svc for $agg {
                $( $(#[$mmeta])* $method => $cmd ),*
            }
        }

        $crate::durable_object! {
            $(#[$meta])*
            $vis object $obj for $agg, store = $store {
                $( $(#[$mmeta])* $method => $cmd ),*
            }
        }
    };
}
