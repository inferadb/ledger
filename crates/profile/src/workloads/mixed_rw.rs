//! mixed-rw: 70/30 writes-to-reads mix against a pre-seeded vault.
//!
//! Reads use `read()` — a single-entity lookup. Writes are the same shape as
//! `throughput-writes`. The mix cycles deterministically (10 iterations = 7
//! writes, 3 reads) to hold the ratio fixed across runs.

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
        let key_idx = counter % KEY_SPACE;
        let is_write = (counter % 10) < 7;
        counter = counter.wrapping_add(1);

        let key = format!("profile:mixed:{key_idx}");
        let op_start = Instant::now();
        let outcome = if is_write {
            let ops = vec![Operation::set_entity(key, vec![0xCDu8; VALUE_SIZE], None, None)];
            harness
                .client
                .write(harness.user, harness.organization, Some(harness.vault), ops, None)
                .await
                .map(|_| ())
        } else {
            harness
                .client
                .read(harness.user, harness.organization, Some(harness.vault), key, None, None)
                .await
                .map(|_| ())
        };
        summary.record_timed(outcome, op_start.elapsed());
    }
    summary.elapsed = start.elapsed();
    summary
}
