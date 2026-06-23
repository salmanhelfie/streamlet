//! Exercises the strongly-typed command model: `CommandKind` + `Handles<C>`,
//! `Service::submit`, the `Entity` handle, `declare_service!`, the
//! `execute_with_retry` helper, and the `Scenario` test harness.

use serde::{Deserialize, Serialize};
use streamlet::prelude::*;
use streamlet::testing::Scenario;

// --- A typed-only aggregate -------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "toggle.", rename_all = "snake_case")]
enum ToggleEvent {
    TurnedOn,
    TurnedOff,
}

#[derive(Debug, Clone, CommandKind)]
struct TurnOn;

#[derive(Debug, Clone, CommandKind)]
struct TurnOff;

#[derive(Debug, PartialEq, thiserror::Error)]
enum ToggleError {
    #[error("already on")]
    AlreadyOn,
    #[error("already off")]
    AlreadyOff,
}

#[derive(Debug, Default)]
struct Toggle {
    on: bool,
}

impl Aggregate for Toggle {
    type Command = NoCommand;
    type Event = ToggleEvent;
    type Rejection = ToggleError;
    const TYPE: &'static str = "toggle";

    fn handle(&self, command: NoCommand) -> Result<Vec<ToggleEvent>, ToggleError> {
        match command {}
    }

    fn apply(&mut self, event: &ToggleEvent) {
        self.on = matches!(event, ToggleEvent::TurnedOn);
    }
}

impl Handles<TurnOn> for Toggle {
    fn handle(&self, _: TurnOn) -> Result<Vec<ToggleEvent>, ToggleError> {
        if self.on {
            Err(ToggleError::AlreadyOn)
        } else {
            Ok(vec![ToggleEvent::TurnedOn])
        }
    }
}

impl Handles<TurnOff> for Toggle {
    fn handle(&self, _: TurnOff) -> Result<Vec<ToggleEvent>, ToggleError> {
        if self.on {
            Ok(vec![ToggleEvent::TurnedOff])
        } else {
            Err(ToggleError::AlreadyOff)
        }
    }
}

declare_service! {
    pub service ToggleService for Toggle {
        turn_on => TurnOn,
        turn_off => TurnOff,
    }
}

#[tokio::test]
async fn submit_appends_typed_command_with_stable_name() {
    let service = Service::<Toggle, _>::new(MemoryStore::new());

    let events = service.submit("sw", TurnOn).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "toggle.turned_on");
}

#[tokio::test]
async fn submit_surfaces_business_rejection() {
    let service = Service::<Toggle, _>::new(MemoryStore::new());
    service.submit("sw", TurnOn).await.unwrap();

    let err = service.submit("sw", TurnOn).await.unwrap_err();
    assert!(matches!(
        err,
        ServiceError::Rejected(ToggleError::AlreadyOn)
    ));
}

#[tokio::test]
async fn entity_handle_pre_fills_the_id() {
    let service = Service::<Toggle, _>::new(MemoryStore::new());
    let sw = service.entity("sw");

    sw.submit(TurnOn).await.unwrap();
    assert!(sw.state().await.unwrap().on);

    sw.submit(TurnOff).await.unwrap();
    assert!(!sw.state().await.unwrap().on);
}

#[tokio::test]
async fn declared_service_exposes_named_methods() {
    let switches = ToggleService::new(MemoryStore::new());
    switches.turn_on("kitchen", TurnOn).await.unwrap();

    let state = switches.service().entity("kitchen").state().await.unwrap();
    assert!(state.on);
}

#[test]
fn scenario_asserts_emitted_events() {
    Scenario::<Toggle>::empty()
        .when_typed(TurnOn)
        .then_events([ToggleEvent::TurnedOn]);
}

#[test]
fn scenario_asserts_rejection() {
    Scenario::<Toggle>::given([ToggleEvent::TurnedOn])
        .when_typed(TurnOn)
        .then_rejected_with(ToggleError::AlreadyOn);
}

// --- An enum aggregate, to exercise execute_with_retry ----------------------

#[derive(Debug, Clone, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "c.")]
enum CounterEvent {
    Added { n: i64 },
}

#[derive(Debug, Clone, Command)]
#[command(prefix = "c.")]
enum CounterCommand {
    Add(i64),
}

#[derive(Debug, thiserror::Error)]
#[error("counter error")]
struct CounterError;

#[derive(Debug, Default)]
struct Counter {
    sum: i64,
}

impl Aggregate for Counter {
    type Command = CounterCommand;
    type Event = CounterEvent;
    type Rejection = CounterError;
    const TYPE: &'static str = "c";

    fn handle(&self, command: CounterCommand) -> Result<Vec<CounterEvent>, CounterError> {
        match command {
            CounterCommand::Add(n) => Ok(vec![CounterEvent::Added { n }]),
        }
    }

    fn apply(&mut self, event: &CounterEvent) {
        match event {
            CounterEvent::Added { n } => self.sum += n,
        }
    }
}

#[tokio::test]
async fn execute_with_retry_runs_the_command() {
    let service = Service::<Counter, _>::new(MemoryStore::new());
    service
        .execute_with_retry("x", CounterCommand::Add(5), 3)
        .await
        .unwrap();

    let (state, _) = service.load("x").await.unwrap();
    assert_eq!(state.sum, 5);
}
