//! concurrent-writes: N concurrent writers, each doing a serial write loop.
//!
//! Tests whether Raft's BatchWriter amortizes WAL fsync under concurrent
//! load. Each of N tasks owns its own region of the key space to avoid
//! write-write contention on the same key — we're measuring fsync batching,
//! not CAS throughput.
//!
//! Concurrency: configurable via `--concurrency N` (default 32).
//! Key space: N × 10,000 (each task gets 10k keys in its own prefix).
//!
//! Each task constructs its own `LedgerClient` with a unique
//! `client_id="profile-task-{task_id}"`. This is test hygiene rather than a
//! perf optimization: it ensures server-side per-client metrics (request
//! rate, idempotency keys, sequence numbers, hot-key counters) partition
//! across the spawned tasks instead of all being credited to one synthetic
//! client. Mirrors how a real multi-tenant deployment would shape load.

use std::time::{Duration, Instant};

use inferadb_ledger_sdk::Operation;

use crate::harness::{Harness, Summary};

const KEY_SPACE_PER_TASK: u64 = 10_000;
const VALUE_SIZE: usize = 32;

/// Spawn `concurrency` writer tasks, each running a serial write loop for
/// `duration`, then merge their results into a single `Summary`.
pub async fn run(harness: &Harness, duration: Duration, concurrency: usize) -> Summary {
    // Guard against nonsense. `concurrency == 0` would produce no tasks and a
    // summary with zero ops; still valid, but easier to reason about if we
    // normalize to at least one writer.
    let concurrency = concurrency.max(1);

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let user = harness.user;
        let organization = harness.organization;
        let vault = harness.vault;
        let client = match harness.connect_task_client(task_id).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, task_id, "failed to connect per-task client");
                let mut local = Summary::default();
                local.errors += 1;
                handles.push(tokio::spawn(async move { local }));
                continue;
            },
        };
        handles.push(tokio::spawn(async move {
            let mut local = Summary::default();
            let task_start = Instant::now();
            let mut counter: u64 = 0;
            while task_start.elapsed() < duration {
                let key = format!("profile:cw:{task_id}:{}", counter % KEY_SPACE_PER_TASK);
                let value = vec![0xABu8; VALUE_SIZE];
                counter = counter.wrapping_add(1);

                let ops = vec![Operation::set_entity(key, value, None, None)];
                let op_start = Instant::now();
                let outcome =
                    client.write(user, organization, Some(vault), ops, None).await.map(|_| ());
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
                tracing::debug!(error = %join_err, "writer task join error");
                summary.errors += 1;
            },
        }
    }
    summary.elapsed = start.elapsed();
    summary
}
