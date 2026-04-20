//! concurrent-reads: N concurrent readers, each doing a serial read loop
//! against a shared seeded key-space.
//!
//! Mirror of `concurrent_writes` for the read path. Seed phase writes a
//! fixed 1,000-key corpus under `profile:cr:seed:{i}`; measured phase spawns
//! N reader tasks that cycle over the same keys with `ReadConsistency::Eventual`
//! (lock-free page-cache path — `Linearizable` would route through ReadIndex
//! and profile a different hot path).
//!
//! Unlike `concurrent_writes`, readers do NOT partition the key-space: reads
//! don't contend on a row, so all tasks share one seeded corpus. The signal
//! this preset measures is read throughput under concurrent gRPC streams on
//! a single tonic HTTP/2 channel (the `LedgerClient` is `Clone` and
//! multiplexes).
//!
//! Concurrency: configurable via `--concurrency N` (default 32).
//! Key space: 1,000 seeded entries, shared across all tasks.
//!
//! If the seed phase fails, the measured phase still runs — reads miss and
//! count as errors, which the Summary surfaces.

use std::time::{Duration, Instant};

use inferadb_ledger_sdk::{Operation, ReadConsistency};

use crate::harness::{Harness, Summary};

const KEY_SPACE: u64 = 1_000;
const VALUE_SIZE: usize = 32;
const SEED_BATCH: usize = 500;

/// Seed a 1,000-key corpus, then spawn `concurrency` reader tasks, each
/// running a serial read loop for `duration`, and merge their results into
/// a single `Summary`.
pub async fn run(harness: &Harness, duration: Duration, concurrency: usize) -> Summary {
    if let Err(e) = seed(harness).await {
        tracing::warn!(error = %e, "seed failed; reads will miss and surface as errors");
    }

    // Guard against nonsense. `concurrency == 0` would produce no tasks and a
    // summary with zero ops; still valid, but easier to reason about if we
    // normalize to at least one reader.
    let concurrency = concurrency.max(1);

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let client = harness.client.clone();
        let user = harness.user;
        let organization = harness.organization;
        let vault = harness.vault;
        handles.push(tokio::spawn(async move {
            let mut local = Summary::default();
            let task_start = Instant::now();
            // Stagger per-task starting index so readers don't march in lock-step
            // over the same key on tick 0. Cheap, deterministic, keeps the
            // preset shape-stable across runs.
            let mut counter: u64 = task_id as u64;
            while task_start.elapsed() < duration {
                let key = format!("profile:cr:seed:{}", counter % KEY_SPACE);
                counter = counter.wrapping_add(1);

                let op_start = Instant::now();
                let outcome = client
                    .read(
                        user,
                        organization,
                        Some(vault),
                        key,
                        Some(ReadConsistency::Eventual),
                        None,
                    )
                    .await
                    .map(|_| ());
                local.record_timed(outcome, op_start.elapsed());
            }
            local
        }));
    }

    let mut summary = Summary::default();
    for handle in handles {
        match handle.await {
            Ok(local) => summary.merge(local),
            Err(join_err) => {
                // A panicked or cancelled task shows up here. We count it as
                // one error rather than aborting — the rest of the tasks'
                // work is still meaningful.
                tracing::debug!(error = %join_err, "reader task join error");
                summary.errors += 1;
            },
        }
    }
    summary.elapsed = start.elapsed();
    summary
}

/// Seed `KEY_SPACE` entities in batches to avoid a server-side op-count limit
/// on a single write. Mirrors `entity_reads::seed`.
async fn seed(harness: &Harness) -> Result<(), inferadb_ledger_sdk::SdkError> {
    let mut batch: Vec<Operation> = Vec::with_capacity(SEED_BATCH);
    let value = vec![0xCDu8; VALUE_SIZE];
    for i in 0..KEY_SPACE {
        batch.push(Operation::set_entity(
            format!("profile:cr:seed:{i}"),
            value.clone(),
            None,
            None,
        ));
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
