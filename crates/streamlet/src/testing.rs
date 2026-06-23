//! A tiny given/when/then harness for unit-testing aggregates.
//!
//! Aggregate decisions are pure: a starting set of events renders a state, a
//! command produces either new events or a rejection. That makes them a joy to
//! test without any store at all:
//!
//! ```
//! use streamlet::prelude::*;
//! use streamlet::testing::Scenario;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Clone, PartialEq, Debug, Serialize, Deserialize, DomainEvent)]
//! enum CounterEvent { Incremented { by: i64 } }
//!
//! #[derive(CommandKind)] struct Increment(i64);
//!
//! #[derive(Debug, thiserror::Error)]
//! #[error("overflow")] struct Overflow;
//!
//! #[derive(Default)] struct Counter { value: i64 }
//!
//! impl Aggregate for Counter {
//!     type Command = NoCommand;
//!     type Event = CounterEvent;
//!     type Rejection = Overflow;
//!     const TYPE: &'static str = "counter";
//!     fn handle(&self, c: NoCommand) -> Result<Vec<CounterEvent>, Overflow> { match c {} }
//!     fn apply(&mut self, e: &CounterEvent) { match e { CounterEvent::Incremented { by } => self.value += by } }
//! }
//! impl Handles<Increment> for Counter {
//!     fn handle(&self, Increment(by): Increment) -> Result<Vec<CounterEvent>, Overflow> {
//!         self.value.checked_add(by).ok_or(Overflow)?;
//!         Ok(vec![CounterEvent::Incremented { by }])
//!     }
//! }
//!
//! Scenario::<Counter>::given([CounterEvent::Incremented { by: 2 }])
//!     .when_typed(Increment(3))
//!     .then_events([CounterEvent::Incremented { by: 3 }]);
//! ```

use crate::aggregate::{render, Aggregate};
use crate::handler::{CommandKind, Handles};

/// The "given" stage: a set of historical events that define the starting state.
pub struct Scenario<A: Aggregate> {
    events: Vec<A::Event>,
}

impl<A: Aggregate> Default for Scenario<A> {
    fn default() -> Self {
        Self { events: Vec::new() }
    }
}

impl<A: Aggregate> Scenario<A> {
    /// Start from no history (a brand-new aggregate).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Start from the given historical events.
    pub fn given(events: impl IntoIterator<Item = A::Event>) -> Self {
        Self {
            events: events.into_iter().collect(),
        }
    }

    /// Append one more historical event.
    pub fn and(mut self, event: A::Event) -> Self {
        self.events.push(event);
        self
    }

    /// The "when" stage for an enum [`Command`](crate::Command).
    pub fn when(self, command: A::Command) -> Outcome<A> {
        let state = render::<A>(&self.events);
        Outcome {
            result: state.handle(command),
        }
    }

    /// The "when" stage for a strongly-typed [`CommandKind`].
    pub fn when_typed<C>(self, command: C) -> Outcome<A>
    where
        A: Handles<C>,
        C: CommandKind,
    {
        let state = render::<A>(&self.events);
        Outcome {
            result: <A as Handles<C>>::handle(&state, command),
        }
    }
}

/// The "then" stage: assertions over the decision the aggregate made.
pub struct Outcome<A: Aggregate> {
    result: Result<Vec<A::Event>, A::Rejection>,
}

impl<A: Aggregate> Outcome<A> {
    /// Get the raw decision, if you want to assert on it yourself.
    pub fn into_result(self) -> Result<Vec<A::Event>, A::Rejection> {
        self.result
    }

    /// Assert the command was rejected and return the rejection for further
    /// inspection.
    #[track_caller]
    pub fn then_rejected(self) -> A::Rejection
    where
        A::Event: std::fmt::Debug,
    {
        match self.result {
            Err(rejection) => rejection,
            Ok(events) => panic!("expected a rejection, but command emitted {events:?}"),
        }
    }
}

impl<A: Aggregate> Outcome<A>
where
    A::Event: PartialEq + std::fmt::Debug,
    A::Rejection: std::fmt::Debug,
{
    /// Assert exactly these events were emitted, in order.
    #[track_caller]
    pub fn then_events(self, expected: impl IntoIterator<Item = A::Event>) {
        let expected: Vec<A::Event> = expected.into_iter().collect();
        match self.result {
            Ok(actual) => assert_eq!(actual, expected, "emitted events did not match expectation"),
            Err(rejection) => {
                panic!("expected events {expected:?}, but command was rejected: {rejection:?}")
            }
        }
    }

    /// Assert the command was accepted but emitted no events (a valid no-op).
    #[track_caller]
    pub fn then_nothing(self) {
        match self.result {
            Ok(actual) if actual.is_empty() => {}
            Ok(actual) => panic!("expected no events, but got {actual:?}"),
            Err(rejection) => panic!("expected no events, but command was rejected: {rejection:?}"),
        }
    }
}

impl<A: Aggregate> Outcome<A>
where
    A::Event: std::fmt::Debug,
    A::Rejection: PartialEq + std::fmt::Debug,
{
    /// Assert the command was rejected with exactly this rejection.
    #[track_caller]
    pub fn then_rejected_with(self, expected: A::Rejection) {
        match self.result {
            Err(rejection) => {
                assert_eq!(rejection, expected, "rejection did not match expectation")
            }
            Ok(events) => panic!("expected rejection {expected:?}, but command emitted {events:?}"),
        }
    }
}
