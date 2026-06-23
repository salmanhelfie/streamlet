//! Micro-benchmarks for the hot store paths: append and full-stream load.
//!
//! Run with `cargo bench -p streamlet`.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use serde::{Deserialize, Serialize};
use streamlet::prelude::*;
use tokio::runtime::Runtime;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "bench.")]
enum BenchEvent {
    Tick { n: u64 },
}

fn append_one(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function("append_single_event", |b| {
        b.to_async(&rt).iter_batched(
            MemoryStore::new,
            |store| async move {
                store
                    .append::<BenchEvent>(
                        "bench",
                        "s1",
                        ExpectedRevision::Any,
                        &[BenchEvent::Tick { n: 1 }],
                        &Metadata::new(),
                    )
                    .await
                    .unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn load_stream(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let store = MemoryStore::new();
    rt.block_on(async {
        for n in 0..1000u64 {
            store
                .append::<BenchEvent>(
                    "bench",
                    "s1",
                    ExpectedRevision::Any,
                    &[BenchEvent::Tick { n }],
                    &Metadata::new(),
                )
                .await
                .unwrap();
        }
    });

    c.bench_function("load_1000_events", |b| {
        b.to_async(&rt).iter(|| async {
            let events = store.load::<BenchEvent>("bench", "s1").await.unwrap();
            assert_eq!(events.len(), 1000);
        });
    });
}

criterion_group!(benches, append_one, load_stream);
criterion_main!(benches);
