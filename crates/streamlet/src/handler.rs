//! Compile-time typed command handling.
//!
//! The [`Command`](crate::Command) enum names a *family* of intents and is
//! dispatched through [`Aggregate::handle`]. That is ergonomic, but the service
//! only knows the set of commands at runtime.
//!
//! This module adds the opposite, strongly-typed end of the spectrum: each
//! command is its *own type* ([`CommandKind`]), and an aggregate advertises the
//! commands it understands by implementing [`Handles<C>`] once per command type.
//! The payoff is that [`Service::submit`](crate::Service::submit) will only
//! accept a command the aggregate actually handles — anything else is a
//! *compile error*, not a runtime rejection.
//!
//! ```
//! use streamlet::prelude::*;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Clone, Serialize, Deserialize, DomainEvent)]
//! enum LightEvent { TurnedOn }
//!
//! // One command, one type.
//! #[derive(CommandKind)]
//! struct TurnOn;
//!
//! #[derive(Debug, thiserror::Error)]
//! #[error("already on")]
//! struct AlreadyOn;
//!
//! #[derive(Default)]
//! struct Light { on: bool }
//!
//! impl Aggregate for Light {
//!     type Command = NoCommand;       // typed-only: never uses the enum path
//!     type Event = LightEvent;
//!     type Rejection = AlreadyOn;
//!     const TYPE: &'static str = "light";
//!     fn handle(&self, command: NoCommand) -> Result<Vec<LightEvent>, AlreadyOn> { match command {} }
//!     fn apply(&mut self, event: &LightEvent) {
//!         self.on = matches!(event, LightEvent::TurnedOn);
//!     }
//! }
//!
//! impl Handles<TurnOn> for Light {
//!     fn handle(&self, _: TurnOn) -> Result<Vec<LightEvent>, AlreadyOn> {
//!         if self.on { Err(AlreadyOn) } else { Ok(vec![LightEvent::TurnedOn]) }
//!     }
//! }
//!
//! // `service.submit("kitchen", TurnOn)` now only compiles for commands the
//! // aggregate `Handles`.
//! ```

use async_trait::async_trait;

use crate::aggregate::Aggregate;

/// A single, strongly-typed command addressed at one aggregate.
///
/// Where the [`Command`](crate::Command) trait describes an enum of related
/// intents, a `CommandKind` is exactly one intent expressed as its own type
/// (typically a small struct). Derive it with `#[derive(CommandKind)]` to get a
/// stable [`NAME`](CommandKind::NAME) for free.
pub trait CommandKind: Send + Sync + 'static {
    /// Stable, unique name for this command (e.g. `"account.Deposit"`).
    const NAME: &'static str;

    /// The name of *this* command value. Mirrors [`NAME`](CommandKind::NAME);
    /// provided so a `&dyn` value can still report its name.
    fn command_name(&self) -> &'static str {
        Self::NAME
    }
}

/// An [`Aggregate`] that knows how to handle one specific [`CommandKind`].
///
/// Implement it once per command type. Because [`Service::submit`] is bound on
/// `A: Handles<C>`, the set of commands a service accepts is checked by the
/// compiler rather than at runtime.
///
/// [`Service::submit`]: crate::Service::submit
pub trait Handles<C: CommandKind>: Aggregate {
    /// Decide which events `command` produces against the current state.
    ///
    /// Like [`Aggregate::handle`], this takes `&self`: deciding never mutates
    /// state. Returning `Ok(vec![])` is a valid no-op.
    fn handle(&self, command: C) -> Result<Vec<Self::Event>, Self::Rejection>;
}

/// An [`Aggregate`] that handles a [`CommandKind`] *with access to an injected
/// environment* `Env`.
///
/// [`Handles<C>`] is a pure decider. When a command needs to consult
/// dependencies to decide — a pricing client, a clock, another service, a policy
/// object — implement `HandlesIn<C, Env>` instead. The service hands your async
/// handler a shared reference to its environment (set via
/// [`Service::with_env`](crate::Service::with_env)) and dispatches the command
/// through [`Service::dispatch`](crate::Service::dispatch).
///
/// Deciding stays write-light: you still return events or a rejection. The
/// environment is for *reads* (look something up to decide), keeping the
/// append-only, optimistic-concurrency guarantees intact.
///
/// Use [`Handles<C>`] for pure commands (dispatched with
/// [`Service::submit`](crate::Service::submit)) and `HandlesIn<C, Env>` for
/// dependency-driven ones (dispatched with
/// [`Service::dispatch`](crate::Service::dispatch)). The two can coexist on the
/// same aggregate for different commands.
#[async_trait]
pub trait HandlesIn<C: CommandKind, Env: Send + Sync>: Aggregate {
    /// Decide which events `command` produces, consulting `env` as needed.
    async fn handle(&self, command: C, env: &Env) -> Result<Vec<Self::Event>, Self::Rejection>;
}
