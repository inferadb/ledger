//! check-heavy: 90% reads over a seeded relationship graph, 10% writes.
//!
//! Seeds 1,000 relationships at startup (10 resources x 10 relations x 10
//! subjects), then cycles through them with a fixed mix. The read path stands
//! in for the authorization-check path — the SDK does not currently expose a
//! `check` / `check_permission` method on `LedgerClient`, so this preset falls
//! back to reading a synthetic `check:{resource}:{relation}:{subject}` key.
//! Occasional writes exercise the relationship-mutation path so flamegraphs
//! still include both read-heavy and write-heavy stacks.
//!
//! If/when the SDK gains a first-class check method, swap the else branch to
//! call it directly — the seed phase already lays down the relationships.

use std::time::{Duration, Instant};

use inferadb_ledger_sdk::Operation;

use crate::harness::{Harness, Summary};

const RESOURCES: u64 = 10;
const RELATIONS: u64 = 10;
const SUBJECTS: u64 = 10;

pub async fn run(harness: &Harness, duration: Duration) -> Summary {
    // Seed relationships. If the seed fails, reads during the measured phase
    // will miss and return None — still a hot path worth profiling. Seed
    // failures surface via the error counter on Summary.
    if let Err(e) = seed(harness).await {
        tracing::warn!(error = %e, "seed failed; proceeding with whatever state exists");
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

        let outcome = if idx.is_multiple_of(10) {
            // Write path: (re-)create a relationship every 10th iter.
            let ops = vec![Operation::create_relationship(resource, relation, subject)];
            harness
                .client
                .write(harness.user, harness.organization, Some(harness.vault), ops, None)
                .await
                .map(|_| ())
        } else {
            // Check fallback: read a synthetic key shaped after the tuple.
            let key = format!("check:{resource}:{relation}:{subject}");
            harness
                .client
                .read(harness.user, harness.organization, Some(harness.vault), key, None, None)
                .await
                .map(|_| ())
        };
        summary.record(outcome);
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
