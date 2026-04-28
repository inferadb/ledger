//! concurrent-writes-multivault: N concurrent writers fanned across M vaults.
//!
//! Like `concurrent-writes`, but rotates writes across `vaults` distinct
//! vaults so load is split across Raft shards / WAL streams. Exposes the
//! real multi-tenant write ceiling, where a single vault plateau (set by a
//! single WAL fsync stream + apply pipeline) gives way to multi-shard
//! scaling.
//!
//! Concurrency: configurable via `--concurrency N` (default 32).
//! Fan-out:     configurable via `--vaults M` (default 16).
//! Key space:   N × 10,000 keys per task, each task pinned to one vault
//!              (`task_id % M`) to avoid cross-vault interference.

use std::time::{Duration, Instant};

use inferadb_ledger_sdk::Operation;
use inferadb_ledger_types::VaultSlug;

use crate::harness::{Harness, Summary};

const KEY_SPACE_PER_TASK: u64 = 10_000;
const VALUE_SIZE: usize = 32;

/// Spawn `concurrency` writer tasks, each running a serial write loop for
/// `duration` against the vault at `vaults[task_id % vaults.len()]`.
///
/// `vaults` must be non-empty; callers should use
/// [`Harness::provision_vaults`] to create the target set before invoking.
pub async fn run(
    harness: &Harness,
    duration: Duration,
    concurrency: usize,
    vaults: Vec<VaultSlug>,
) -> Summary {
    let concurrency = concurrency.max(1);
    assert!(!vaults.is_empty(), "concurrent-writes-multivault requires at least one vault");

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let user = harness.user;
        let organization = harness.organization;
        let vault = vaults[task_id % vaults.len()];
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
                let key = format!("profile:cwmv:{task_id}:{}", counter % KEY_SPACE_PER_TASK);
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
                tracing::debug!(error = %join_err, "writer task join error");
                summary.errors += 1;
            },
        }
    }
    summary.elapsed = start.elapsed();
    summary
}
