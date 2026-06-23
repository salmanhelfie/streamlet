use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// A domain event payload — usually an `enum` where each variant is a single
/// thing that happened. Derive it with `#[derive(DomainEvent)]` so every variant
/// gets a stable string name automatically.
pub trait DomainEvent: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    /// The stable name of *this* event value (e.g. `"counter.Incremented"`).
    fn event_type(&self) -> &'static str;

    /// Every event name this type can ever produce. Useful for subscriptions,
    /// routing and documentation.
    fn event_types() -> &'static [&'static str];
}

/// Free-form, string-keyed metadata carried alongside an event (correlation id,
/// causation id, actor, etc.). Kept deliberately simple.
pub type Metadata = BTreeMap<String, String>;

/// An event as it lives in the store: the payload plus the bookkeeping the store
/// assigned to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recorded<E> {
    /// Unique id of this event (UUID v7, time-ordered).
    pub id: String,
    /// The aggregate type that owns the stream, e.g. `"counter"`.
    pub aggregate_type: String,
    /// The stream / aggregate instance id.
    pub stream_id: String,
    /// 1-based position of this event *within its stream*.
    pub version: u64,
    /// Monotonic position of this event across *all* streams. Drives projections.
    pub global_position: u64,
    /// The stable event name (mirrors [`DomainEvent::event_type`]).
    pub event_type: String,
    /// The decoded payload.
    pub payload: E,
    /// Milliseconds since the Unix epoch when the event was recorded.
    pub recorded_at: i64,
    /// Caller-supplied metadata.
    pub metadata: Metadata,
}

impl<E> Recorded<E> {
    /// Map the payload while preserving all bookkeeping. Handy for turning a
    /// `Recorded<RawJson>` into a `Recorded<MyEvent>` and vice-versa.
    pub fn map_payload<T>(self, f: impl FnOnce(E) -> T) -> Recorded<T> {
        Recorded {
            id: self.id,
            aggregate_type: self.aggregate_type,
            stream_id: self.stream_id,
            version: self.version,
            global_position: self.global_position,
            event_type: self.event_type,
            payload: f(self.payload),
            recorded_at: self.recorded_at,
            metadata: self.metadata,
        }
    }
}

/// Optimistic-concurrency expectation when appending to a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRevision {
    /// Append regardless of the current stream version.
    Any,
    /// The stream must not exist yet (current version is 0).
    NoStream,
    /// The stream's current version must be exactly this value.
    Exact(u64),
}

impl ExpectedRevision {
    /// Validate `actual` (the stream's current version) against this
    /// expectation, returning the conflict description if it fails.
    pub(crate) fn check(self, actual: u64) -> Result<(), (String, u64)> {
        let ok = match self {
            ExpectedRevision::Any => true,
            ExpectedRevision::NoStream => actual == 0,
            ExpectedRevision::Exact(v) => actual == v,
        };
        if ok {
            Ok(())
        } else {
            Err((self.describe(), actual))
        }
    }

    fn describe(self) -> String {
        match self {
            ExpectedRevision::Any => "any".to_string(),
            ExpectedRevision::NoStream => "no stream".to_string(),
            ExpectedRevision::Exact(v) => format!("version {v}"),
        }
    }
}
