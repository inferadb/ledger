//! entity-reads: pure read loop against a pre-seeded 10k-entry key-space.
//!
//! Seed phase writes 10,000 entities with keys `profile:ent-rd:{i}` and a
//! 32-byte value. Measured phase cycles `client.read()` against those keys,
//! recording per-op latency. Exercises the SDK → gRPC → validation →
//! slug-resolution → state-layer entity-lookup hot path in isolation.
//!
//! Consistency: `Eventual` (linearizable routes through ReadIndex, which
//! profiles a different code path — not what this preset is measuring).

use std::time::{Duration, Instant};

use inferadb_ledger_sdk::Operation;

use crate::harness::{Harness, Summary};

const KEY_SPACE: u64 = 10_000;
const VALUE_SIZE: usize = 32;
const SEED_BATCH: usize = 500;

pub async fn run(harness: &Harness, duration: Duration) -> Summary {
    if let Err(e) = seed(harness).await {
        tracing::warn!(error = %e, "seed failed; reads will miss and surface as errors");
    }

    let mut summary = Summary::default();
    let start = Instant::now();
    let mut counter: u64 = 0;
    while start.elapsed() < duration {
        let key = format!("profile:ent-rd:{}", counter % KEY_SPACE);
        counter = counter.wrapping_add(1);

        let op_start = Instant::now();
        let outcome = harness
            .client
            .read(harness.user, harness.organization, Some(harness.vault), key, None, None)
            .await
            .map(|_| ());
        summary.record_timed(outcome, op_start.elapsed());
    }
    summary.elapsed = start.elapsed();
    summary
}

/// Seed 10,000 entities in batches to avoid a server-side op-count limit on a
/// single write.
async fn seed(harness: &Harness) -> Result<(), inferadb_ledger_sdk::SdkError> {
    let mut batch: Vec<Operation> = Vec::with_capacity(SEED_BATCH);
    let value = vec![0xDEu8; VALUE_SIZE];
    for i in 0..KEY_SPACE {
        batch.push(Operation::set_entity(format!("profile:ent-rd:{i}"), value.clone(), None, None));
        if batch.len() >= SEED_BATCH {
            let ops = std::mem::take(&mut batch);
            harness
                .client
                .write(harness.user, harness.organization, Some(harness.vault), ops, None)
                .await
                .map(|_| ())?;
            batch = Vec::with_capacity(SEED_BATCH);
        }
    }
    if !batch.is_empty() {
        harness
            .client
            .write(harness.user, harness.organization, Some(harness.vault), batch, None)
            .await
            .map(|_| ())?;
    }
    Ok(())
}
