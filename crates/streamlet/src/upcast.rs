//! Event upcasting: evolve persisted event shapes without rewriting history.
//!
//! Events are immutable once written, but your code keeps changing. When an old
//! event's JSON no longer matches the current Rust type, register an
//! [`Upcaster`] that rewrites the *old* shape into the *new* one at load time.
//! The log on disk is untouched; decoding simply runs the payload through the
//! relevant upcasters first.
//!
//! ```
//! use streamlet::upcast::{Upcaster, Upcasters};
//! use serde_json::{json, Value};
//!
//! // v1 stored `{"amount": 10}`; v2 needs `{"amount": 10, "currency": "USD"}`.
//! struct AddCurrency;
//! impl Upcaster for AddCurrency {
//!     fn event_type(&self) -> &str { "account.deposited" }
//!     fn upcast(&self, mut payload: Value) -> Value {
//!         if let Value::Object(ref mut map) = payload {
//!             map.entry("currency").or_insert(json!("USD"));
//!         }
//!         payload
//!     }
//! }
//!
//! let upcasters = Upcasters::new().with(AddCurrency);
//! let fixed = upcasters.apply("account.deposited", json!({"amount": 10}));
//! assert_eq!(fixed, json!({"amount": 10, "currency": "USD"}));
//! ```

use std::collections::HashMap;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::aggregate::Aggregate;
use crate::error::StoreError;
use crate::event::{RawEvent, Recorded};

/// Rewrites one event type's stored JSON into its current shape.
///
/// Register several for the same event type to chain migrations (v1→v2→v3);
/// they run in registration order.
pub trait Upcaster: Send + Sync {
    /// The stored event name this upcaster applies to (e.g. `"account.deposited"`).
    fn event_type(&self) -> &str;

    /// Transform the stored payload into the next shape.
    fn upcast(&self, payload: Value) -> Value;
}

/// A registry of [`Upcaster`]s, grouped by event type.
#[derive(Default)]
pub struct Upcasters {
    by_type: HashMap<String, Vec<Box<dyn Upcaster>>>,
}

impl Upcasters {
    /// An empty registry (a no-op: payloads pass through unchanged).
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an upcaster (builder style).
    pub fn with(mut self, upcaster: impl Upcaster + 'static) -> Self {
        self.register(upcaster);
        self
    }

    /// Register an upcaster.
    pub fn register(&mut self, upcaster: impl Upcaster + 'static) {
        self.by_type
            .entry(upcaster.event_type().to_string())
            .or_default()
            .push(Box::new(upcaster));
    }

    /// Apply every upcaster registered for `event_type`, in order.
    pub fn apply(&self, event_type: &str, payload: Value) -> Value {
        match self.by_type.get(event_type) {
            Some(chain) => chain.iter().fold(payload, |p, u| u.upcast(p)),
            None => payload,
        }
    }

    /// Decode a raw recorded event into `E`, upcasting its payload first.
    pub fn decode<E: DeserializeOwned>(&self, raw: &Recorded<RawEvent>) -> Result<E, StoreError> {
        let upcasted = self.apply(&raw.event_type, raw.payload.0.clone());
        serde_json::from_value(upcasted).map_err(|e| StoreError::Serialization(e.to_string()))
    }

    /// Render an aggregate by upcasting then folding a stream of raw events.
    ///
    /// Pair with [`EventStore::load_raw`](crate::EventStore::load_raw) to load a
    /// stream, migrate every old payload forward, and rebuild current state.
    pub fn render<A: Aggregate>(&self, raw: &[Recorded<RawEvent>]) -> Result<A, StoreError> {
        let mut state = A::default();
        for event in raw {
            let payload: A::Event = self.decode(event)?;
            state.apply(&payload);
        }
        Ok(state)
    }
}
