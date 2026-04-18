//! throughput-writes: tight write loop, single writer, fixed key-space.
//!
//! Shape: 10,000-entry key-space, each iteration writes one `Operation::SetEntity`
//! with a 32-byte value. The key cycles modulo the key-space size so the same
//! keys are rewritten repeatedly — exercises the update path in the B+ tree,
//! not the insert path. A production node's steady state is update-dominant.

use std::time::{Duration, Instant};

use inferadb_ledger_sdk::Operation;

use crate::harness::{Harness, Summary};

const KEY_SPACE: u64 = 10_000;
const VALUE_SIZE: usize = 32;

pub async fn run(harness: &Harness, duration: Duration) -> Summary {
    let mut summary = Summary::default();
    let start = Instant::now();
    let mut counter: u64 = 0;
    while start.elapsed() < duration {
        let key = format!("profile:write:{}", counter % KEY_SPACE);
        let value = vec![0xABu8; VALUE_SIZE];
        counter = counter.wrapping_add(1);

        let ops = vec![Operation::set_entity(key, value, None, None)];
        let outcome = harness
            .client
            .write(harness.user, harness.organization, Some(harness.vault), ops, None)
            .await
            .map(|_| ());
        summary.record(outcome);
    }
    summary.elapsed = start.elapsed();
    summary
}
