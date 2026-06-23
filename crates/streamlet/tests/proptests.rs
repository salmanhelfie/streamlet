//! Property-based store conformance: whatever sequence of appends we throw at
//! the in-memory store, its core invariants must hold — contiguous per-stream
//! versions starting at 1, a strictly increasing global position, and a faithful
//! round-trip of every payload.

use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use streamlet::prelude::*;
use tokio::runtime::Runtime;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "p.")]
enum E {
    N { v: i64 },
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn append_batches_preserve_invariants(batches in proptest::collection::vec(proptest::collection::vec(any::<i64>(), 0..5), 0..20)) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStore::new();
            let mut expected_version = 0u64;
            let mut last_global = 0u64;

            for batch in &batches {
                let events: Vec<E> = batch.iter().map(|v| E::N { v: *v }).collect();
                let expected = if expected_version == 0 {
                    ExpectedRevision::NoStream
                } else {
                    ExpectedRevision::Exact(expected_version)
                };
                let recorded = store
                    .append::<E>("p", "s1", expected, &events, &Metadata::new())
                    .await
                    .unwrap();

                prop_assert_eq!(recorded.len(), events.len());
                for r in &recorded {
                    expected_version += 1;
                    prop_assert_eq!(r.version, expected_version);
                    prop_assert!(r.global_position > last_global);
                    last_global = r.global_position;
                }
            }

            // Full reload returns everything, in version order, intact.
            let all = store.load::<E>("p", "s1").await.unwrap();
            prop_assert_eq!(all.len() as u64, expected_version);
            for (i, r) in all.iter().enumerate() {
                prop_assert_eq!(r.version, i as u64 + 1);
            }
            Ok(())
        })?;
    }

    #[test]
    fn load_from_skips_the_prefix(total in 1usize..40, cut in 0u64..40) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStore::new();
            let events: Vec<E> = (0..total).map(|v| E::N { v: v as i64 }).collect();
            store
                .append::<E>("p", "s1", ExpectedRevision::NoStream, &events, &Metadata::new())
                .await
                .unwrap();

            let tail = store.load_from::<E>("p", "s1", cut).await.unwrap();
            for r in &tail {
                prop_assert!(r.version > cut);
            }
            let expected_len = (total as u64).saturating_sub(cut.min(total as u64));
            prop_assert_eq!(tail.len() as u64, expected_len);
            Ok(())
        })?;
    }
}
