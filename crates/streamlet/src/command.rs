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
