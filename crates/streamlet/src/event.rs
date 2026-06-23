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

/// Well-known metadata keys.
pub mod meta_keys {
    /// Ties together every event that stems from the same originating request.
    pub const CORRELATION_ID: &str = "correlation_id";
    /// The id of the event/command that directly caused this one.
    pub const CAUSATION_ID: &str = "causation_id";
    /// Who (user, service) triggered the change.
    pub const ACTOR: &str = "actor";
}

/// Ergonomic, type-checked access to the well-known [`Metadata`] keys.
///
/// `Metadata` stays a plain map (so anything goes), but correlation tracing is
/// common enough to deserve first-class helpers:
///
/// ```
/// use streamlet::{Metadata, MetadataExt};
///
/// let md = Metadata::new()
///     .with_correlation_id("req-1")
///     .with_actor("alice");
/// assert_eq!(md.correlation_id(), Some("req-1"));
/// assert_eq!(md.actor(), Some("alice"));
/// ```
pub trait MetadataExt {
    /// Set the correlation id (builder style).
    fn with_correlation_id(self, id: impl Into<String>) -> Self;
    /// Set the causation id (builder style).
    fn with_causation_id(self, id: impl Into<String>) -> Self;
    /// Set the actor (builder style).
    fn with_actor(self, actor: impl Into<String>) -> Self;
    /// The correlation id, if present.
    fn correlation_id(&self) -> Option<&str>;
    /// The causation id, if present.
    fn causation_id(&self) -> Option<&str>;
    /// The actor, if present.
    fn actor(&self) -> Option<&str>;
}

impl MetadataExt for Metadata {
    fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.insert(meta_keys::CORRELATION_ID.to_string(), id.into());
        self
    }
    fn with_causation_id(mut self, id: impl Into<String>) -> Self {
        self.insert(meta_keys::CAUSATION_ID.to_string(), id.into());
        self
    }
    fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.insert(meta_keys::ACTOR.to_string(), actor.into());
        self
    }
    fn correlation_id(&self) -> Option<&str> {
        self.get(meta_keys::CORRELATION_ID).map(String::as_str)
    }
    fn causation_id(&self) -> Option<&str> {
        self.get(meta_keys::CAUSATION_ID).map(String::as_str)
    }
    fn actor(&self) -> Option<&str> {
        self.get(meta_keys::ACTOR).map(String::as_str)
    }
}

/// A still-encoded event payload: the raw JSON exactly as it sits in the store.
///
/// This is the input to [`upcasting`](crate::upcast): you read events as
/// `Recorded<RawEvent>`, rewrite old shapes forward, then decode into the
/// current event type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RawEvent(pub serde_json::Value);

impl DomainEvent for RawEvent {
    fn event_type(&self) -> &'static str {
        // The authoritative name lives on the `Recorded` envelope (the store
        // column); a raw payload carries none of its own.
        ""
    }

    fn event_types() -> &'static [&'static str] {
        &[]
    }
}

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
    /// This event's unique id as a typed [`EventId`].
    pub fn event_id(&self) -> crate::ids::EventId {
        crate::ids::EventId::new(self.id.clone())
    }

    /// The owning aggregate type as a typed [`AggregateType`].
    pub fn aggregate(&self) -> crate::ids::AggregateType {
        crate::ids::AggregateType::new(self.aggregate_type.clone())
    }

    /// The owning stream as a typed [`StreamId`].
    pub fn stream(&self) -> crate::ids::StreamId {
        crate::ids::StreamId::new(self.stream_id.clone())
    }

    /// This event's per-stream position as a typed [`Revision`].
    pub fn revision(&self) -> crate::ids::Revision {
        crate::ids::Revision::new(self.version)
    }

    /// This event's global log position as a typed [`Revision`].
    pub fn position(&self) -> crate::ids::Revision {
        crate::ids::Revision::new(self.global_position)
    }

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
