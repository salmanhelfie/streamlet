//! Behavioural conformance suite run against every store implementation.
//!
//! The same `*_suite` functions are executed against [`MemoryStore`] and (when
//! the `libsql` feature is on) [`SqliteStore`], so both backends are held to an
//! identical contract: contiguous versions, a monotonic global position,
//! optimistic-concurrency enforcement, type-filtered global reads, document
//! round-trips and projection replay/catch-up.

use serde::{Deserialize, Serialize};
use streamlet::prelude::*;
use streamlet::StoreError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "test.")]
enum Ev {
    Added { n: i64 },
    Removed { n: i64 },
    #[event(rename = "Cleared")]
    Reset,
}

#[derive(Debug, Clone, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "other.")]
enum OtherEv {
    Noise,
}

#[derive(Command)]
#[command(prefix = "test.")]
enum Cmd {
    Add(i64),
    Remove(i64),
}

#[derive(Debug, Default)]
struct Acc {
    total: i64,
}

#[derive(Debug, thiserror::Error)]
#[error("would go negative (to {to})")]
struct WouldGoNegative {
    to: i64,
}

impl Aggregate for Acc {
    type Command = Cmd;
    type Event = Ev;
    type Rejection = WouldGoNegative;
    const TYPE: &'static str = "acc";

    fn handle(&self, command: Cmd) -> Result<Vec<Ev>, WouldGoNegative> {
        match command {
            Cmd::Add(n) => Ok(vec![Ev::Added { n }]),
            Cmd::Remove(n) => {
                let to = self.total - n;
                if to < 0 {
                    Err(WouldGoNegative { to })
                } else {
                    Ok(vec![Ev::Removed { n }])
                }
            }
        }
    }

    fn apply(&mut self, event: &Ev) {
        match event {
            Ev::Added { n } => self.total += n,
            Ev::Removed { n } => self.total -= n,
            Ev::Reset => self.total = 0,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SumView {
    net: i64,
    events: u64,
}

impl View for SumView {
    type Event = Ev;
    const NAME: &'static str = "sum-view";

    fn apply(&mut self, event: &Recorded<Ev>) {
        self.events += 1;
        match event.payload {
            Ev::Added { n } => self.net += n,
            Ev::Removed { n } => self.net -= n,
            Ev::Reset => self.net = 0,
        }
    }
}

// --- the reusable suites ---------------------------------------------------

async fn append_and_load_suite<S: EventStore>(store: &S) {
    let md = Metadata::new();
    let written = store
        .append::<Ev>("acc", "a1", ExpectedRevision::NoStream, &[Ev::Added { n: 2 }, Ev::Added { n: 3 }], &md)
        .await
        .expect("append");

    assert_eq!(written.len(), 2);
    assert_eq!(written[0].version, 1);
    assert_eq!(written[1].version, 2);
    assert!(written[1].global_position > written[0].global_position);
    assert_eq!(written[0].event_type, "test.Added");
    assert!(!written[0].id.is_empty());

    let loaded = store.load::<Ev>("acc", "a1").await.expect("load");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].payload, Ev::Added { n: 2 });
    assert_eq!(loaded[1].payload, Ev::Added { n: 3 });

    // A second append continues the version sequence.
    let more = store
        .append::<Ev>("acc", "a1", ExpectedRevision::Exact(2), &[Ev::Reset], &md)
        .await
        .expect("second append");
    assert_eq!(more[0].version, 3);
    assert_eq!(more[0].event_type, "test.Cleared");

    // Empty append is a no-op.
    let none = store
        .append::<Ev>("acc", "a1", ExpectedRevision::Any, &[], &md)
        .await
        .expect("empty append");
    assert!(none.is_empty());

    // Unknown stream loads as empty.
    let empty = store.load::<Ev>("acc", "does-not-exist").await.expect("load empty");
    assert!(empty.is_empty());
}

async fn concurrency_suite<S: EventStore>(store: &S) {
    let md = Metadata::new();
    store
        .append::<Ev>("acc", "c1", ExpectedRevision::NoStream, &[Ev::Added { n: 1 }], &md)
        .await
        .expect("first");

    // NoStream on an existing stream must conflict.
    let err = store
        .append::<Ev>("acc", "c1", ExpectedRevision::NoStream, &[Ev::Added { n: 1 }], &md)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict { .. }), "got {err:?}");

    // Exact mismatch must conflict.
    let err = store
        .append::<Ev>("acc", "c1", ExpectedRevision::Exact(99), &[Ev::Added { n: 1 }], &md)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict { .. }), "got {err:?}");

    // Any always succeeds.
    store
        .append::<Ev>("acc", "c1", ExpectedRevision::Any, &[Ev::Added { n: 1 }], &md)
        .await
        .expect("any append");

    assert_eq!(store.load::<Ev>("acc", "c1").await.unwrap().len(), 2);
}

async fn read_all_suite<S: EventStore>(store: &S) {
    let md = Metadata::new();
    store
        .append::<Ev>("acc", "r1", ExpectedRevision::Any, &[Ev::Added { n: 1 }, Ev::Added { n: 2 }], &md)
        .await
        .unwrap();
    // Interleave an event of a different type / aggregate.
    store
        .append::<OtherEv>("other", "x", ExpectedRevision::Any, &[OtherEv::Noise], &md)
        .await
        .unwrap();
    store
        .append::<Ev>("acc", "r2", ExpectedRevision::Any, &[Ev::Removed { n: 1 }], &md)
        .await
        .unwrap();

    // read_all::<Ev> sees only test.* events, in global order.
    let all = store.read_all::<Ev>(0, 100).await.unwrap();
    assert_eq!(all.len(), 3);
    assert!(all.windows(2).all(|w| w[0].global_position < w[1].global_position));
    assert!(all.iter().all(|e| e.event_type.starts_with("test.")));

    // `after` + `limit` paginate.
    let first = store.read_all::<Ev>(0, 2).await.unwrap();
    assert_eq!(first.len(), 2);
    let rest = store
        .read_all::<Ev>(first.last().unwrap().global_position, 100)
        .await
        .unwrap();
    assert_eq!(rest.len(), 1);
}

async fn document_suite<D: DocumentStore>(store: &D) {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Doc {
        name: String,
        hits: u32,
    }

    assert!(store.fetch::<Doc>("c", "k").await.unwrap().is_none());

    store
        .save("c", "k", &Doc { name: "a".into(), hits: 1 })
        .await
        .unwrap();
    assert_eq!(
        store.fetch::<Doc>("c", "k").await.unwrap().unwrap(),
        Doc { name: "a".into(), hits: 1 }
    );

    // Save replaces.
    store
        .save("c", "k", &Doc { name: "a".into(), hits: 2 })
        .await
        .unwrap();
    store.save("c", "k2", &Doc { name: "b".into(), hits: 9 }).await.unwrap();

    let mut listed = store.list::<Doc>("c").await.unwrap();
    listed.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].0, "k");
    assert_eq!(listed[0].1.hits, 2);

    store.delete("c", "k").await.unwrap();
    assert!(store.fetch::<Doc>("c", "k").await.unwrap().is_none());
    // Deleting a missing key is fine.
    store.delete("c", "nope").await.unwrap();
}

async fn projection_suite<S: EventStore + DocumentStore>(store: &S) {
    let md = Metadata::new();
    store
        .append::<Ev>("acc", "p1", ExpectedRevision::Any, &[Ev::Added { n: 10 }, Ev::Removed { n: 4 }], &md)
        .await
        .unwrap();

    let replayed = replay_view::<SumView, _>(store).await.unwrap();
    assert_eq!(replayed.view.net, 6);
    assert_eq!(replayed.view.events, 2);

    // catch_up persists, then a follow-up only folds new events.
    let p1 = catch_up_view::<SumView, _, _>(store, store, "proj", SumView::NAME)
        .await
        .unwrap();
    assert_eq!(p1.view.net, 6);

    store
        .append::<Ev>("acc", "p1", ExpectedRevision::Any, &[Ev::Added { n: 5 }], &md)
        .await
        .unwrap();

    let p2 = catch_up_view::<SumView, _, _>(store, store, "proj", SumView::NAME)
        .await
        .unwrap();
    assert_eq!(p2.view.net, 11);
    assert_eq!(p2.view.events, 3);
    assert!(p2.position > p1.position);
}

async fn service_suite<S: EventStore>(store: S) {
    let service = Service::<Acc, _>::new(store);

    assert_eq!(
        Service::<Acc, S>::handled_commands(),
        &["test.Add", "test.Remove"]
    );

    service.execute("s1", Cmd::Add(10)).await.unwrap();
    service.execute("s1", Cmd::Remove(3)).await.unwrap();
    let (acc, rev) = service.load("s1").await.unwrap();
    assert_eq!(acc.total, 7);
    assert_eq!(rev, ExpectedRevision::Exact(2));

    // A business rejection is reported as Rejected, not Store.
    let err = service.execute("s1", Cmd::Remove(999)).await.unwrap_err();
    assert!(err.is_rejection());
    assert!(!err.is_infrastructure());
    match err {
        ServiceError::Rejected(WouldGoNegative { to }) => assert_eq!(to, -992),
        other => panic!("expected rejection, got {other:?}"),
    }

    // The rejected command wrote nothing.
    assert_eq!(service.load("s1").await.unwrap().0.total, 7);
}

// --- bind the suites to MemoryStore ---------------------------------------

#[tokio::test]
async fn memory_append_and_load() {
    append_and_load_suite(&MemoryStore::new()).await;
}

#[tokio::test]
async fn memory_concurrency() {
    concurrency_suite(&MemoryStore::new()).await;
}

#[tokio::test]
async fn memory_read_all() {
    read_all_suite(&MemoryStore::new()).await;
}

#[tokio::test]
async fn memory_documents() {
    document_suite(&MemoryStore::new()).await;
}

#[tokio::test]
async fn memory_projection() {
    projection_suite(&MemoryStore::new()).await;
}

#[tokio::test]
async fn memory_service() {
    service_suite(MemoryStore::new()).await;
}

// --- and to SqliteStore when the libsql feature is enabled -----------------

#[cfg(feature = "libsql")]
mod sqlite {
    use super::*;
    use streamlet::SqliteStore;

    async fn store() -> SqliteStore {
        SqliteStore::open_in_memory().await.expect("open sqlite")
    }

    #[tokio::test]
    async fn sqlite_append_and_load() {
        append_and_load_suite(&store().await).await;
    }

    #[tokio::test]
    async fn sqlite_concurrency() {
        concurrency_suite(&store().await).await;
    }

    #[tokio::test]
    async fn sqlite_read_all() {
        read_all_suite(&store().await).await;
    }

    #[tokio::test]
    async fn sqlite_documents() {
        document_suite(&store().await).await;
    }

    #[tokio::test]
    async fn sqlite_projection() {
        projection_suite(&store().await).await;
    }

    #[tokio::test]
    async fn sqlite_service() {
        service_suite(store().await).await;
    }
}
