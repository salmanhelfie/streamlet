//! A runnable counter demo for the `streamlet` toolkit.
//!
//! Run it with:
//!
//! ```text
//! cargo run -p counter-example --bin counter
//! # or, to also exercise the persistent libSQL store:
//! cargo run -p counter-example --bin counter --features libsql
//! ```

use serde::{Deserialize, Serialize};
use streamlet::prelude::*;

// ---------------------------------------------------------------------------
// Domain: a counter that can be bumped up/down and reset, but refuses to go
// negative or to overflow. "Refuses" is a *business rule*, not an error.
// ---------------------------------------------------------------------------

/// Events: the derive gives every variant a stable, prefixed name automatically.
#[derive(Debug, Clone, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "counter.")]
pub enum CounterEvent {
    Incremented { by: i64 },
    Decremented { by: i64 },
    Reset,
}

/// Commands: likewise named automatically.
#[derive(Debug, Command)]
#[command(prefix = "counter.")]
pub enum CounterCommand {
    Increment(i64),
    Decrement(i64),
    Reset,
}

/// The business-rule rejection type — completely separate from store errors.
#[derive(Debug, thiserror::Error)]
pub enum CounterError {
    #[error("counter cannot go below zero (would be {would_be})")]
    WouldGoNegative { would_be: i64 },
    #[error("counter would overflow")]
    Overflow,
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
            CounterCommand::Increment(by) => {
                self.value.checked_add(by).ok_or(CounterError::Overflow)?;
                Ok(vec![CounterEvent::Incremented { by }])
            }
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
// A read-model / projection: total activity across *all* counters.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ActivityView {
    pub increments: u64,
    pub decrements: u64,
    pub resets: u64,
}

impl View for ActivityView {
    type Event = CounterEvent;
    const NAME: &'static str = "counter-activity";

    fn apply(&mut self, event: &Recorded<CounterEvent>) {
        match event.payload {
            CounterEvent::Incremented { .. } => self.increments += 1,
            CounterEvent::Decremented { .. } => self.decrements += 1,
            CounterEvent::Reset => self.resets += 1,
        }
    }
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("== streamlet counter demo ==\n");
    run(MemoryStore::new(), "in-memory").await?;

    #[cfg(feature = "libsql")]
    {
        println!();
        let store = SqliteStore::open_in_memory().await?;
        run(store, "libsql (sqlite)").await?;
    }

    Ok(())
}

async fn run<S>(store: S, label: &str) -> Result<(), Box<dyn std::error::Error>>
where
    S: EventStore + DocumentStore,
{
    println!("--- store: {label} ---");

    // Declare the service once; from here on it only accepts CounterCommands.
    let service = Service::<Counter, _>::new(store);
    println!(
        "this service handles exactly: {:?}",
        Service::<Counter, S>::handled_commands()
    );

    let id = "counter-1";

    service.execute(id, CounterCommand::Increment(5)).await?;
    service.execute(id, CounterCommand::Increment(3)).await?;
    service.execute(id, CounterCommand::Decrement(2)).await?;

    let (counter, revision) = service.load(id).await?;
    println!("state after 3 commands: value={} ({:?})", counter.value, revision);

    // A business-rule rejection — note how it is NOT an infrastructure error.
    match service.execute(id, CounterCommand::Decrement(100)).await {
        Err(ServiceError::Rejected(rejection)) => {
            println!("rejected (business rule): {rejection}");
        }
        Err(ServiceError::Store(err)) => {
            println!("unexpected infrastructure error: {err}");
        }
        Ok(_) => println!("(unexpectedly accepted)"),
    }

    // A second counter, so the projection spans multiple streams.
    service.execute("counter-2", CounterCommand::Increment(10)).await?;
    service.execute("counter-2", CounterCommand::Reset).await?;

    // Build the projection straight from the event log, then persist it as a
    // document and read it back.
    let projection = replay_view::<ActivityView, _>(service.store()).await?;
    println!(
        "activity projection @position {}: {:?}",
        projection.position, projection.view
    );

    let docs = service.store();
    catch_up_view::<ActivityView, _, _>(service.store(), docs, "projections", ActivityView::NAME)
        .await?;
    let persisted: Option<streamlet::store::Projection<ActivityView>> =
        docs.fetch("projections", ActivityView::NAME).await?;
    println!("persisted projection document: {:?}", persisted.map(|p| p.view));

    Ok(())
}
