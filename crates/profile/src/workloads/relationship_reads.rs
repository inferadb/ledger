//! relationship-reads: pure check_relationship loop against a pre-seeded set.
//!
//! Seed phase writes 1,000 relationships (10 resources × 10 relations × 10
//! subjects). Measured phase cycles `client.check_relationship()` against
//! those tuples — all calls should return `exists=true`. Exercises the
//! SDK → gRPC → validation → slug-resolution → state-layer relationship-
//! index lookup hot path in isolation.
//!
//! Consistency: `Eventual` (linearizable routes through ReadIndex, which
//! profiles a different code path).

use std::time::{Duration, Instant};

use inferadb_ledger_sdk::{Operation, ReadConsistency};

use crate::harness::{Harness, Summary};

const RESOURCES: u64 = 10;
const RELATIONS: u64 = 10;
const SUBJECTS: u64 = 10;

pub async fn run(harness: &Harness, duration: Duration) -> Summary {
    if let Err(e) = seed(harness).await {
        tracing::warn!(error = %e, "seed failed; checks will miss and return exists=false");
    }

    let mut summary = Summary::default();
    let start = Instant::now();
    let mut counter: u64 = 0;
    while start.elapsed() < duration {
        let idx = counter;
        counter = counter.wrapping_add(1);

        let resource = format!("resource:{}", idx % RESOURCES);
        let relation = format!("rel:{}", (idx / RESOURCES) % RELATIONS);
        let subject = format!("subject:{}", (idx / (RESOURCES * RELATIONS)) % SUBJECTS);

        let op_start = Instant::now();
        let outcome = harness
            .client
            .check_relationship(
                harness.user,
                harness.organization,
                harness.vault,
                &resource,
                &relation,
                &subject,
                ReadConsistency::Eventual,
                None,
            )
            .await
            .map(|_| ());
        summary.record_timed(outcome, op_start.elapsed());
    }
    summary.elapsed = start.elapsed();
    summary
}

async fn seed(harness: &Harness) -> Result<(), inferadb_ledger_sdk::SdkError> {
    let mut ops: Vec<Operation> = Vec::with_capacity((RESOURCES * RELATIONS * SUBJECTS) as usize);
    for r in 0..RESOURCES {
        for rel in 0..RELATIONS {
            for s in 0..SUBJECTS {
                ops.push(Operation::create_relationship(
                    format!("resource:{r}"),
                    format!("rel:{rel}"),
                    format!("subject:{s}"),
                ));
            }
        }
    }
    harness
        .client
        .write(harness.user, harness.organization, Some(harness.vault), ops, None)
        .await
        .map(|_| ())
}
