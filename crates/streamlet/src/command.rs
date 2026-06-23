/// A command — something a caller wants an aggregate to do. Usually an `enum`
/// where each variant is one intent. Derive it with `#[derive(Command)]` so
/// every variant gets a stable string name automatically.
pub trait Command: Send + Sync {
    /// The stable name of *this* command value (e.g. `"counter.Increment"`).
    fn command_type(&self) -> &'static str;

    /// Every command name this type can produce. This is what powers the
    /// "a service only handles the commands it declares" guarantee: the set of
    /// names here is exactly the set a [`crate::Service`] for the owning
    /// aggregate will accept.
    fn command_types() -> &'static [&'static str]
    where
        Self: Sized;
}

/// An uninhabited command type for aggregates that drive everything through the
/// strongly-typed [`Handles<C>`](crate::Handles) / [`Service::submit`] API and
/// never use the enum [`Service::execute`] path.
///
/// Set `type Command = NoCommand` and implement `handle` as `match command {}`:
///
/// ```
/// # use streamlet::prelude::*;
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Clone, Serialize, Deserialize, DomainEvent)] enum E { Did }
/// # #[derive(Debug, thiserror::Error)] #[error("no")] struct R;
/// #[derive(Default)]
/// struct MyAggregate;
/// impl Aggregate for MyAggregate {
///     type Command = NoCommand;
///     type Event = E;
///     type Rejection = R;
///     const TYPE: &'static str = "my-aggregate";
///     fn handle(&self, command: NoCommand) -> Result<Vec<E>, R> { match command {} }
///     fn apply(&mut self, _: &E) {}
/// }
/// ```
///
/// [`Service::submit`]: crate::Service::submit
/// [`Service::execute`]: crate::Service::execute
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoCommand {}

impl Command for NoCommand {
    fn command_type(&self) -> &'static str {
        match *self {}
    }

    fn command_types() -> &'static [&'static str] {
        &[]
    }
}
