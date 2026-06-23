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
mod handler;
mod ids;
mod macros;
mod service;
mod snapshot;
pub mod store;
pub mod subscription;
pub mod testing;
pub mod upcast;

pub use aggregate::{render, render_from, Aggregate, View};
pub use command::{Command, NoCommand};
pub use error::{ServiceError, StoreError};
pub use event::{
    meta_keys, DomainEvent, ExpectedRevision, Metadata, MetadataExt, RawEvent, Recorded,
};
pub use handler::{CommandKind, Handles, HandlesIn};
pub use ids::{AggregateType, EventId, Revision, StreamId};
pub use upcast::{Upcaster, Upcasters};

/// Re-export of [`async_trait::async_trait`], so implementors of
/// [`HandlesIn`] can write `#[streamlet::async_trait]` without adding the
/// `async-trait` crate to their own dependencies.
pub use async_trait::async_trait;
pub use service::{Entity, Executor, Service, TypedExecutor};
pub use snapshot::{SnapshotEnvelope, SnapshotPolicy};
pub use store::{catch_up_view, replay_view, DocumentStore, EventStore, Projection};
pub use subscription::{
    already_processed, checkpoint_position, mark_processed, pump, run_publisher, run_reactor,
    EventPublisher, InMemoryPublisher, PublishedEvent, Reactor,
};

#[cfg(feature = "memory")]
pub use store::memory::MemoryStore;

#[cfg(feature = "libsql")]
pub use store::libsql::SqliteStore;

/// Re-exports of the derive macros so `use streamlet::prelude::*` brings both
/// the traits and their derives into scope.
pub use streamlet_derive::{Command, CommandKind, DomainEvent};

/// Everything you need for day-to-day use, in one import.
pub mod prelude {
    pub use crate::aggregate::{render, Aggregate, View};
    pub use crate::command::{Command, NoCommand};
    pub use crate::declare_service;
    pub use crate::error::{ServiceError, StoreError};
    pub use crate::event::{
        meta_keys, DomainEvent, ExpectedRevision, Metadata, MetadataExt, Recorded,
    };
    pub use crate::handler::{CommandKind, Handles, HandlesIn};
    pub use crate::ids::{AggregateType, EventId, Revision, StreamId};
    pub use crate::service::{Entity, Executor, Service, TypedExecutor};
    pub use crate::snapshot::{SnapshotEnvelope, SnapshotPolicy};
    pub use crate::store::{catch_up_view, replay_view, DocumentStore, EventStore, Projection};
    pub use async_trait::async_trait;
    pub use streamlet_derive::{Command, CommandKind, DomainEvent};

    #[cfg(feature = "memory")]
    pub use crate::store::memory::MemoryStore;

    #[cfg(feature = "libsql")]
    pub use crate::store::libsql::SqliteStore;
}
