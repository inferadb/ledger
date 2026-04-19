//! relationship-writes: pure create_relationship loop.
//!
//! Cycles through a 10×10×10 tuple space (1,000 relationships), re-creating
//! each one repeatedly. Exercises the SDK → gRPC → Raft-propose → apply →
//! relationship-index-insert hot path in isolation. No seed phase; the first
//! cycle is insert-path, subsequent cycles are update-path.

use std::time::{Duration, Instant};

use inferadb_ledger_sdk::Operation;

use crate::harness::{Harness, Summary};

const RESOURCES: u64 = 10;
const RELATIONS: u64 = 10;
const SUBJECTS: u64 = 10;

pub async fn run(harness: &Harness, duration: Duration) -> Summary {
    let mut summary = Summary::default();
    let start = Instant::now();
    let mut counter: u64 = 0;
    while start.elapsed() < duration {
        let idx = counter;
        counter = counter.wrapping_add(1);

        let resource = format!("resource:{}", idx % RESOURCES);
        let relation = format!("rel:{}", (idx / RESOURCES) % RELATIONS);
        let subject = format!("subject:{}", (idx / (RESOURCES * RELATIONS)) % SUBJECTS);

        let ops = vec![Operation::create_relationship(resource, relation, subject)];

        let op_start = Instant::now();
        let outcome = harness
            .client
            .write(harness.user, harness.organization, Some(harness.vault), ops, None)
            .await
            .map(|_| ());
        summary.record_timed(outcome, op_start.elapsed());
    }
    summary.elapsed = start.elapsed();
    summary
}
