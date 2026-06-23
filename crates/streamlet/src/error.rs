use thiserror::Error;

/// Infrastructure failures from a store. These are about *plumbing* (the disk,
/// the wire, serialization, concurrency) — never about domain rules.
#[derive(Debug, Error)]
pub enum StoreError {
    /// Optimistic concurrency check failed: someone else wrote to the stream.
    #[error(
        "concurrency conflict on stream `{stream_id}`: expected {expected}, found version {actual}"
    )]
    Conflict {
        stream_id: String,
        expected: String,
        actual: u64,
    },

    /// A payload could not be (de)serialized.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The requested item does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// Any other backend error (I/O, SQL, network, ...).
    #[error("store backend error: {0}")]
    Backend(String),
}

/// The result of asking a [`crate::Service`] to do something.
///
/// This is the key ergonomic split the toolkit promises: a command can fail for
/// two *fundamentally different* reasons, and they should never be confused:
///
/// * [`ServiceError::Rejected`] — the domain said "no" (a business rule). This
///   is a normal, expected outcome; retrying won't help.
/// * [`ServiceError::Store`] — the infrastructure failed. This may be transient
///   and is what a durable executor like Restate would retry.
#[derive(Debug, Error)]
pub enum ServiceError<R> {
    /// The aggregate rejected the command for a business reason.
    #[error("command rejected: {0}")]
    Rejected(R),

    /// The underlying store failed.
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl<R> ServiceError<R> {
    /// `true` if this was a business-rule rejection (not infrastructure).
    pub fn is_rejection(&self) -> bool {
        matches!(self, ServiceError::Rejected(_))
    }

    /// `true` if this was an infrastructure failure (potentially retryable).
    pub fn is_infrastructure(&self) -> bool {
        matches!(self, ServiceError::Store(_))
    }
}
