//! # streamlet
//!
//! A small, ergonomic event-sourcing toolkit. The whole model fits in your head:
//!
//! * [`DomainEvent`] — an enum of things that *happened*. Each variant gets a
//!   stable string name (via the [`derive@DomainEvent`] derive).
//! * [`Command`] — an enum of things someone *wants to happen*.
//! * [`Aggregate`] — folds events into state ("rendering") and decides which new
//!   events a command produces, rejecting it with a typed business error if a
//!   rule is broken.
//! * [`EventStore`] / [`DocumentStore`] — append-only event log plus a small
//!   document/projection store. Comes with an in-memory implementation and an
//!   optional libSQL (SQLite) implementation.
//! * [`Service`] — declared once for an aggregate; loads a stream, renders it,
//!   executes a command and appends the resulting events. Business rejections
//!   are kept separate from infrastructure failures via [`ServiceError`].
//!
//! ```
//! use streamlet::prelude::*;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Clone, Serialize, Deserialize, DomainEvent)]
//! #[domain_event(prefix = "counter.")]
//! enum CounterEvent { Incremented { by: i64 }, Reset }
//!
//! #[derive(Command)]
//! enum CounterCommand { Increment(i64), Reset }
//!
//! #[derive(Default)]
//! struct Counter { value: i64 }
//!
//! #[derive(Debug, thiserror::Error)]
//! #[error("counter would overflow")]
//! struct Overflow;
//!
//! impl Aggregate for Counter {
//!     type Command = CounterCommand;
//!     type Event = CounterEvent;
//!     type Rejection = Overflow;
//!     const TYPE: &'static str = "counter";
//!
//!     fn handle(&self, cmd: CounterCommand) -> Result<Vec<CounterEvent>, Overflow> {
//!         match cmd {
//!             CounterCommand::Increment(by) => {
//!                 self.value.checked_add(by).ok_or(Overflow)?;
//!                 Ok(vec![CounterEvent::Incremented { by }])
//!             }
//!             CounterCommand::Reset => Ok(vec![CounterEvent::Reset]),
//!         }
//!     }
//!
//!     fn apply(&mut self, event: &CounterEvent) {
//!         match event {
//!             CounterEvent::Incremented { by } => self.value += by,
//!             CounterEvent::Reset => self.value = 0,
//!         }
//!     }
//! }
//! ```

// Allow the derive macros (which expand to `::streamlet::...`) to work inside
// this very crate's own tests and doctests.
extern crate self as streamlet;

mod aggregate;
mod command;
mod error;
mod event;
mod service;
pub mod store;

pub use aggregate::{render, render_from, Aggregate, View};
pub use command::Command;
pub use error::{ServiceError, StoreError};
pub use event::{DomainEvent, ExpectedRevision, Metadata, Recorded};
pub use service::{Executor, Service};
pub use store::{catch_up_view, replay_view, DocumentStore, EventStore, Projection};

#[cfg(feature = "memory")]
pub use store::memory::MemoryStore;

#[cfg(feature = "libsql")]
pub use store::libsql::SqliteStore;

/// Re-exports of the derive macros so `use streamlet::prelude::*` brings both
/// the traits and their derives into scope.
pub use streamlet_derive::{Command, DomainEvent};

/// Everything you need for day-to-day use, in one import.
pub mod prelude {
    pub use crate::aggregate::{render, Aggregate, View};
    pub use crate::command::Command;
    pub use crate::error::{ServiceError, StoreError};
    pub use crate::event::{DomainEvent, ExpectedRevision, Metadata, Recorded};
    pub use crate::service::{Executor, Service};
    pub use crate::store::{catch_up_view, replay_view, DocumentStore, EventStore, Projection};
    pub use streamlet_derive::{Command, DomainEvent};

    #[cfg(feature = "memory")]
    pub use crate::store::memory::MemoryStore;

    #[cfg(feature = "libsql")]
    pub use crate::store::libsql::SqliteStore;
}
