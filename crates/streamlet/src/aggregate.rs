use crate::command::Command;
use crate::event::{DomainEvent, Recorded};

/// The heart of the domain model.
///
/// An aggregate is a small state machine that:
/// * **renders** itself from past events ([`apply`](Aggregate::apply)), and
/// * **decides** which new events a command produces ([`handle`](Aggregate::handle)),
///   rejecting the command with a typed business error when a rule is violated.
///
/// Note that `handle` takes `&self`: deciding never mutates state. State only
/// ever changes by applying events, which keeps replay and live execution
/// identical.
pub trait Aggregate: Default + Send + Sync {
    /// The command enum this aggregate accepts.
    type Command: Command;
    /// The event enum this aggregate emits.
    type Event: DomainEvent;
    /// The business-rule rejection type. This is intentionally *not* an
    /// infrastructure error — see [`crate::ServiceError`].
    type Rejection: std::error::Error + Send + Sync + 'static;

    /// Stable aggregate type name, used as the stream's `aggregate_type`.
    const TYPE: &'static str;

    /// Decide what happens. Returning `Ok(vec![])` is a valid no-op.
    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Rejection>;

    /// Fold a single event into the current state.
    fn apply(&mut self, event: &Self::Event);
}

/// Render an aggregate by folding decoded event payloads into a fresh value.
pub fn render<A: Aggregate>(events: &[A::Event]) -> A {
    let mut state = A::default();
    for event in events {
        state.apply(event);
    }
    state
}

/// Render an aggregate from stored [`Recorded`] events (the common case after a
/// `load`).
pub fn render_from<A: Aggregate>(events: &[Recorded<A::Event>]) -> A {
    let mut state = A::default();
    for event in events {
        state.apply(&event.payload);
    }
    state
}

/// A read model / projection: anything that can be rebuilt by folding a stream
/// of recorded events. Unlike an [`Aggregate`], a `View` never makes decisions —
/// it just accumulates a queryable shape of the data.
pub trait View: Default + Send + Sync {
    /// The events this view consumes.
    type Event: DomainEvent;

    /// Stable projection name (used as a checkpoint key / document collection).
    const NAME: &'static str;

    /// Fold one recorded event into the view.
    fn apply(&mut self, event: &Recorded<Self::Event>);
}
