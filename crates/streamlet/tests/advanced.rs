//! Exercises the production-oriented additions: snapshots, the checkpointed
//! subscription engine (publisher/outbox + reactor), idempotency helpers, and
//! the correlation metadata helpers.

use serde::{Deserialize, Serialize};
use streamlet::prelude::*;
use streamlet::{
    already_processed, checkpoint_position, mark_processed, run_publisher, run_reactor,
    InMemoryPublisher, Reactor, SnapshotPolicy,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "ctr.", rename_all = "snake_case")]
enum CounterEvent {
    Added { by: i64 },
}

#[derive(Debug, Clone, Command)]
enum CounterCommand {
    Add(i64),
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("counter error")]
struct CounterError;

#[derive(Debug, Default, Serialize, Deserialize)]
struct Counter {
    sum: i64,
    applied: u64,
}

impl Aggregate for Counter {
    type Command = CounterCommand;
    type Event = CounterEvent;
    type Rejection = CounterError;
    const TYPE: &'static str = "ctr";

    fn handle(&self, command: CounterCommand) -> Result<Vec<CounterEvent>, CounterError> {
        match command {
            CounterCommand::Add(by) => Ok(vec![CounterEvent::Added { by }]),
        }
    }

    fn apply(&mut self, event: &CounterEvent) {
        match event {
            CounterEvent::Added { by } => {
                self.sum += by;
                self.applied += 1;
            }
        }
    }
}

#[tokio::test]
async fn snapshot_load_matches_full_render() {
    let service = Service::<Counter, _>::new(MemoryStore::new());
    for i in 0..10 {
        service
            .execute_snapshotting(
                "c1",
                CounterCommand::Add(i),
                SnapshotPolicy::EveryNEvents(4),
            )
            .await
            .unwrap();
    }

    // Snapshot fast-path and full render must agree.
    let (snap_state, snap_rev) = service.load_snapshotted("c1").await.unwrap();
    let (full_state, full_rev) = service.load("c1").await.unwrap();
    assert_eq!(snap_state.sum, full_state.sum);
    assert_eq!(snap_state.applied, full_state.applied);
    assert_eq!(snap_rev, full_rev);
    assert_eq!(snap_state.sum, (0..10).sum::<i64>());
}

#[tokio::test]
async fn publisher_outbox_is_resumable() {
    let svc = Service::<Counter, _>::new(MemoryStore::new());
    svc.execute("c1", CounterCommand::Add(1)).await.unwrap();
    svc.execute("c1", CounterCommand::Add(2)).await.unwrap();

    let publisher = InMemoryPublisher::new();
    let n = run_publisher::<CounterEvent, _, _, _>(svc.store(), svc.store(), "pub", &publisher)
        .await
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(publisher.published().len(), 2);

    // A second run publishes nothing new (checkpoint advanced).
    svc.execute("c1", CounterCommand::Add(3)).await.unwrap();
    let n2 = run_publisher::<CounterEvent, _, _, _>(svc.store(), svc.store(), "pub", &publisher)
        .await
        .unwrap();
    assert_eq!(n2, 1);
    assert_eq!(publisher.published().len(), 3);
    assert_eq!(checkpoint_position(svc.store(), "pub").await.unwrap(), 3);
}

struct Totaller {
    total: std::sync::atomic::AtomicI64,
}

#[streamlet::async_trait]
impl Reactor for Totaller {
    type Event = CounterEvent;
    async fn react(&self, event: &Recorded<CounterEvent>) -> Result<(), StoreError> {
        let CounterEvent::Added { by } = &event.payload;
        self.total
            .fetch_add(*by, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn reactor_processes_each_event_once() {
    let svc = Service::<Counter, _>::new(MemoryStore::new());
    svc.execute("c1", CounterCommand::Add(5)).await.unwrap();
    svc.execute("c1", CounterCommand::Add(7)).await.unwrap();

    let reactor = Totaller {
        total: std::sync::atomic::AtomicI64::new(0),
    };
    run_reactor(&reactor, svc.store(), svc.store(), "saga")
        .await
        .unwrap();
    // Run again: checkpoint means no double counting.
    run_reactor(&reactor, svc.store(), svc.store(), "saga")
        .await
        .unwrap();
    assert_eq!(reactor.total.load(std::sync::atomic::Ordering::SeqCst), 12);
}

#[tokio::test]
async fn idempotency_markers_round_trip() {
    let store = MemoryStore::new();
    assert!(!already_processed(&store, "saga", "evt-1").await.unwrap());
    mark_processed(&store, "saga", "evt-1").await.unwrap();
    assert!(already_processed(&store, "saga", "evt-1").await.unwrap());
    assert!(!already_processed(&store, "saga", "evt-2").await.unwrap());
}

#[tokio::test]
async fn upcasting_renders_from_raw_events() {
    use streamlet::Upcasters;
    let svc = Service::<Counter, _>::new(MemoryStore::new());
    svc.execute("c1", CounterCommand::Add(3)).await.unwrap();
    svc.execute("c1", CounterCommand::Add(4)).await.unwrap();

    let raw = svc.store().load_raw("ctr", "c1").await.unwrap();
    assert_eq!(raw.len(), 2);

    // With no upcasters registered this is just a faithful re-render.
    let rebuilt: Counter = Upcasters::new().render(&raw).unwrap();
    let (direct, _) = svc.load("c1").await.unwrap();
    assert_eq!(rebuilt.sum, direct.sum);
    assert_eq!(rebuilt.sum, 7);
}

#[tokio::test]
async fn metadata_correlation_helpers() {
    let md = Metadata::new()
        .with_correlation_id("req-1")
        .with_causation_id("cmd-9")
        .with_actor("alice");
    assert_eq!(md.correlation_id(), Some("req-1"));
    assert_eq!(md.causation_id(), Some("cmd-9"));
    assert_eq!(md.actor(), Some("alice"));
}
