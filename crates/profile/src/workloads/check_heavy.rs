//! check-heavy: 90% relationship existence checks, 10% relationship writes.
//!
//! Seeds 1,000 relationships at startup (10 resources × 10 relations × 10
//! subjects), then cycles through them with a fixed 9:1 check:write mix.
//! The check path invokes `LedgerClient::check_relationship`, the
//! storage-primitive existence check the Engine layer hammers when serving
//! authorization decisions.

use std::time::{Duration, Instant};

use inferadb_ledger_sdk::{Operation, ReadConsistency};

use crate::harness::{Harness, Summary};

const RESOURCES: u64 = 10;
const RELATIONS: u64 = 10;
const SUBJECTS: u64 = 10;

pub async fn run(harness: &Harness, duration: Duration) -> Summary {
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
            let ops = vec![Operation::create_relationship(resource, relation, subject)];
            harness
                .client
                .write(harness.user, harness.organization, Some(harness.vault), ops, None)
                .await
                .map(|_| ())
        } else {
            harness
                .client
                .check_relationship(
                    harness.user,
                    harness.organization,
                    harness.vault,
                    resource,
                    relation,
                    subject,
                    ReadConsistency::Eventual,
                    None,
                )
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
