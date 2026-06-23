//! Small typed identifiers.
//!
//! These are deliberately thin newtypes over `String`. They cost nothing at
//! runtime but make signatures self-documenting and stop you from accidentally
//! swapping, say, a collection name for a stream id.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The identifier of a single aggregate instance / event stream.
///
/// Construct one from anything string-like (`"order-42".into()`), and pass it
/// wherever a stream id is expected — every store method accepts `&str`, and
/// `StreamId` derefs to `str`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StreamId(String);

impl StreamId {
    /// Wrap a string as a [`StreamId`].
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

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for StreamId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for StreamId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<StreamId> for String {
    fn from(value: StreamId) -> Self {
        value.0
    }
}

impl AsRef<str> for StreamId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for StreamId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}
