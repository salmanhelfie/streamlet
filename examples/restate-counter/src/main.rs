//! The same counter aggregate as the in-process example, exposed as a Restate
//! Virtual Object so every command runs through Restate's durable executor.
//!
//! Run it with:
//!
//! ```text
//! cargo run -p restate-counter-example --bin restate-counter
//! ```
//!
//! then register the endpoint with a running Restate server:
//!
//! ```text
//! restate deployments register http://localhost:9080
//! curl localhost:8080/CounterObject/my-counter/increment --json '5'
//! curl localhost:8080/CounterObject/my-counter/get
//! ```

use std::sync::Arc;

use restate_sdk::prelude::*;
use restate_sdk::serde::Json;
use serde::{Deserialize, Serialize};
use streamlet::prelude::*;
use streamlet_restate::{execute_command, read_state};

// ---------------------------------------------------------------------------
// Domain (identical business logic to the in-process counter example).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "counter.")]
pub enum CounterEvent {
    Incremented { by: i64 },
    Decremented { by: i64 },
    Reset,
}

#[derive(Debug, Command)]
#[command(prefix = "counter.")]
pub enum CounterCommand {
    Increment(i64),
    Decrement(i64),
    Reset,
}

#[derive(Debug, thiserror::Error)]
pub enum CounterError {
    #[error("counter cannot go below zero (would be {would_be})")]
    WouldGoNegative { would_be: i64 },
}

#[derive(Debug, Default)]
pub struct Counter {
    value: i64,
}

impl Aggregate for Counter {
    type Command = CounterCommand;
    type Event = CounterEvent;
    type Rejection = CounterError;
    const TYPE: &'static str = "counter";

    fn handle(&self, command: CounterCommand) -> Result<Vec<CounterEvent>, CounterError> {
        match command {
            CounterCommand::Increment(by) => Ok(vec![CounterEvent::Incremented { by }]),
            CounterCommand::Decrement(by) => {
                let would_be = self.value - by;
                if would_be < 0 {
                    return Err(CounterError::WouldGoNegative { would_be });
                }
                Ok(vec![CounterEvent::Decremented { by }])
            }
            CounterCommand::Reset => Ok(vec![CounterEvent::Reset]),
        }
    }

    fn apply(&mut self, event: &CounterEvent) {
        match event {
            CounterEvent::Incremented { by } => self.value += by,
            CounterEvent::Decremented { by } => self.value -= by,
            CounterEvent::Reset => self.value = 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Restate Virtual Object: one logical counter per object key.
// ---------------------------------------------------------------------------

#[restate_sdk::object]
trait CounterObject {
    async fn increment(amount: i64) -> Result<i64, HandlerError>;
    async fn decrement(amount: i64) -> Result<i64, HandlerError>;
    async fn reset() -> Result<i64, HandlerError>;
    #[shared]
    async fn get() -> Result<i64, HandlerError>;
}

#[derive(Clone)]
struct CounterObjectImpl {
    service: Arc<Service<Counter, MemoryStore>>,
}

impl CounterObject for CounterObjectImpl {
    async fn increment(&self, ctx: ObjectContext<'_>, amount: i64) -> Result<i64, HandlerError> {
        self.apply(ctx, CounterCommand::Increment(amount)).await
    }

    async fn decrement(&self, ctx: ObjectContext<'_>, amount: i64) -> Result<i64, HandlerError> {
        self.apply(ctx, CounterCommand::Decrement(amount)).await
    }

    async fn reset(&self, ctx: ObjectContext<'_>) -> Result<i64, HandlerError> {
        self.apply(ctx, CounterCommand::Reset).await
    }

    async fn get(&self, ctx: SharedObjectContext<'_>) -> Result<i64, HandlerError> {
        let id = ctx.key().to_string();
        let service = self.service.clone();
        let Json(value) = ctx
            .run(|| read_state(service, id, |c: &Counter| c.value))
            .await?;
        Ok(value)
    }
}

impl CounterObjectImpl {
    /// Shared body for the writing handlers: durably append, then durably read
    /// back the resulting value.
    async fn apply(
        &self,
        ctx: ObjectContext<'_>,
        command: CounterCommand,
    ) -> Result<i64, HandlerError> {
        let id = ctx.key().to_string();
        let service = self.service.clone();

        // Durable append — Restate journals the written events and replays them
        // on retry instead of appending twice.
        ctx.run(|| execute_command(service.clone(), id.clone(), command))
            .await?;

        // Durable read of derived state.
        let Json(value) = ctx
            .run(|| read_state(service, id, |c: &Counter| c.value))
            .await?;
        Ok(value)
    }
}

#[tokio::main]
async fn main() {
    // A real deployment would use a persistent store (e.g. SqliteStore); the
    // in-memory store keeps the example self-contained.
    let service = Arc::new(Service::<Counter, _>::new(MemoryStore::new()));
    let object = CounterObjectImpl { service };

    println!("serving CounterObject on http://0.0.0.0:9080 ...");
    HttpServer::new(Endpoint::builder().bind(object.serve()).build())
        .listen_and_serve("0.0.0.0:9080".parse().unwrap())
        .await;
}
