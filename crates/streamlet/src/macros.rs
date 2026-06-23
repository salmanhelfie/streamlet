//! The [`declare_service!`] macro.

/// Declare a typed application service for an aggregate in one place.
///
/// You list the aggregate and, for each strongly-typed command it
/// [`Handles`](crate::Handles), a method name. The macro generates a small
/// wrapper struct whose methods are named after your domain operations and are
/// each bound to the matching [`CommandKind`](crate::CommandKind) — so the set of
/// operations a service exposes is fixed at compile time and reads like an API.
///
/// ```
/// use streamlet::prelude::*;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Clone, Serialize, Deserialize, DomainEvent)]
/// enum DoorEvent { Opened, Closed }
///
/// #[derive(CommandKind)] struct Open;
/// #[derive(CommandKind)] struct Close;
///
/// #[derive(Debug, thiserror::Error)]
/// #[error("door rule violated")]
/// struct DoorError;
///
/// #[derive(Default)]
/// struct Door { open: bool }
///
/// impl Aggregate for Door {
///     type Command = NoCommand;
///     type Event = DoorEvent;
///     type Rejection = DoorError;
///     const TYPE: &'static str = "door";
///     fn handle(&self, c: NoCommand) -> Result<Vec<DoorEvent>, DoorError> { match c {} }
///     fn apply(&mut self, e: &DoorEvent) { self.open = matches!(e, DoorEvent::Opened); }
/// }
/// impl Handles<Open> for Door {
///     fn handle(&self, _: Open) -> Result<Vec<DoorEvent>, DoorError> { Ok(vec![DoorEvent::Opened]) }
/// }
/// impl Handles<Close> for Door {
///     fn handle(&self, _: Close) -> Result<Vec<DoorEvent>, DoorError> { Ok(vec![DoorEvent::Closed]) }
/// }
///
/// declare_service! {
///     /// Operations available on a door.
///     pub service DoorService for Door {
///         open => Open,
///         close => Close,
///     }
/// }
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let doors = DoorService::new(MemoryStore::new());
/// doors.open("front", Open).await?;
/// doors.close("front", Close).await?;
/// # Ok(()) }
/// ```
#[macro_export]
macro_rules! declare_service {
    (
        $(#[$meta:meta])*
        $vis:vis service $name:ident for $agg:ty {
            $( $(#[$mmeta:meta])* $method:ident => $cmd:ty ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis struct $name<S> {
            inner: $crate::Service<$agg, S>,
        }

        impl<S> $name<S>
        where
            S: $crate::EventStore,
        {
            /// Build the service over the given event store.
            $vis fn new(store: S) -> Self {
                Self { inner: $crate::Service::new(store) }
            }

            /// Wrap an existing [`Service`](streamlet::Service).
            $vis fn from_service(inner: $crate::Service<$agg, S>) -> Self {
                Self { inner }
            }

            /// Borrow the underlying generic service.
            $vis fn service(&self) -> &$crate::Service<$agg, S> {
                &self.inner
            }

            /// Unwrap into the underlying generic service.
            $vis fn into_service(self) -> $crate::Service<$agg, S> {
                self.inner
            }

            $(
                $(#[$mmeta])*
                $vis async fn $method(
                    &self,
                    id: &str,
                    command: $cmd,
                ) -> ::core::result::Result<
                    ::std::vec::Vec<$crate::Recorded<<$agg as $crate::Aggregate>::Event>>,
                    $crate::ServiceError<<$agg as $crate::Aggregate>::Rejection>,
                >
                where
                    $agg: $crate::Handles<$cmd>,
                {
                    self.inner.submit(id, command).await
                }
            )*
        }
    };
}
