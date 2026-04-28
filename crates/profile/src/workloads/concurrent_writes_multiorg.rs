//! concurrent-writes-multiorg: N concurrent writers fanned across M orgs.
//!
//! Validates **shard-based parallelism** — the architectural axis InferaDB
//! is designed to scale along. Each organization routes to a specific Raft
//! shard via `ShardManager`, so writes across distinct orgs land on
//! distinct Raft groups, each with its own apply pipeline (`ApplyWorker`,
//! WAL, commit path). Fanning writes across M orgs should scale throughput
//! roughly linearly in shard count, bounded by the host's CPU / disk
//! capacity.
//!
//! Contrast with `concurrent-writes-multivault`: that preset fans across
//! multiple vaults *within one organization* (same shard), so it measures
//! per-vault parallelism within a shard. This preset fans across *orgs*,
//! measuring the real cross-shard scaling the architecture promises.
//!
//! Concurrency: configurable via `--concurrency N` (default 32).
//! Fan-out:     configurable via `--orgs M` (default 4).
//! Key space:   10,000 keys per task, each task pinned to one org
//!              (`task_id % M`) to avoid cross-org write-write contention.

use std::time::{Duration, Instant};

use inferadb_ledger_sdk::Operation;
use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};

use crate::harness::{Harness, Summary};

const KEY_SPACE_PER_TASK: u64 = 10_000;
const VALUE_SIZE: usize = 32;

/// Spawn `concurrency` writer tasks, each running a serial write loop for
/// `duration` against the org at `targets[task_id % targets.len()]`.
///
/// `targets` must be non-empty; callers should use
/// [`Harness::provision_orgs`] to create the target set before invoking.
pub async fn run(
    harness: &Harness,
    duration: Duration,
    concurrency: usize,
    targets: Vec<(UserSlug, OrganizationSlug, VaultSlug)>,
) -> Summary {
    let concurrency = concurrency.max(1);
    assert!(!targets.is_empty(), "concurrent-writes-multiorg requires at least one org");

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let (user, organization, vault) = targets[task_id % targets.len()];
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
                let key = format!("profile:cwmo:{task_id}:{}", counter % KEY_SPACE_PER_TASK);
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
