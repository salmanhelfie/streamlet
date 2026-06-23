//! Small typed identifiers.
//!
//! These are deliberately thin newtypes over `String`/`u64`. They cost nothing
//! at runtime but make signatures self-documenting and stop you from
//! accidentally swapping, say, a collection name for a stream id, or a global
//! position for a per-stream version.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Generate a thin, string-backed newtype id with the usual conversions.
macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        pub struct $name(String);

        impl $name {
            /// Wrap a string as this id.
            pub fn new(id: impl Into<String>) -> Self {
                Self(id.into())
            }

            /// Borrow the underlying string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consume the id, returning the owned `String`.
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl std::ops::Deref for $name {
            type Target = str;
            fn deref(&self) -> &str {
                &self.0
            }
        }
    };
}

string_id! {
    /// The identifier of a single aggregate instance / event stream.
    ///
    /// Construct one from anything string-like (`"order-42".into()`), and pass
    /// it wherever a stream id is expected — every store method accepts `&str`,
    /// and `StreamId` derefs to `str`.
    StreamId
}

string_id! {
    /// The stable, unique id of a single recorded event (a UUID v7 string).
    EventId
}

string_id! {
    /// The logical type/category of an aggregate (e.g. `"account"`), shared by
    /// every stream of that kind.
    AggregateType
}

/// A position within an event stream, or across the whole log.
///
/// `Revision` distinguishes the two monotonic counters the store maintains: a
/// per-stream [`version`](crate::Recorded::version) and a global
/// [`global_position`](crate::Recorded::global_position). Keeping them in one
/// type — rather than bare `u64`s — stops you from comparing a stream version
/// against a global position by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Revision(u64);

impl Revision {
    /// The revision before any event exists (`0`).
    pub const ZERO: Revision = Revision(0);

    /// Wrap a raw counter value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw counter value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next revision (saturating).
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for Revision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Revision {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Revision> for u64 {
    fn from(value: Revision) -> Self {
        value.0
    }
}
