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

use inferadb_ledger_sdk::{LedgerClient, Operation};
use inferadb_ledger_types::VaultSlug;

use crate::harness::{Harness, Summary};

const KEY_SPACE_PER_TASK: u64 = 10_000;
const VALUE_SIZE: usize = 32;

/// Spawn `concurrency` writer tasks, each running a serial write loop for
/// `duration` against the vault at `vaults[task_id % vaults.len()]`.
///
/// `vaults` must be non-empty; callers should use
/// [`Harness::provision_vaults`] to create the target set before invoking.
///
/// When `shared_client` is `true`, all tasks share a single
/// `LedgerClient` cloned from the harness. This is the diagnostic mode used
/// to measure the throughput gap between single-connection workloads (real
/// SDK consumers reusing one client) and per-task-connection workloads.
/// The shared client carries `client_id = "profile-workload"` for every
/// request; the per-task path uses `connect_task_client` and produces one
/// client (and thus one TCP+HTTP/2 connection) per task.
///
/// `pool_size` overrides `ClientConfig::connection_pool_size` for
/// the harness's bootstrap client (`shared_client=true`) and for every
/// per-task client (`shared_client=false`). A value of `0` falls back to
/// the SDK default (`1`). Higher values trade a linear increase in
/// TCP/HTTP-2 footprint for parallel tonic `Buffer` workers, which is
/// the diagnostic axis for the per-Channel dispatch ceiling described
/// on `ClientConfig::connection_pool_size`.
pub async fn run(
    harness: &Harness,
    duration: Duration,
    concurrency: usize,
    vaults: Vec<VaultSlug>,
    shared_client: bool,
    pool_size: u8,
) -> Summary {
    let concurrency = concurrency.max(1);
    assert!(!vaults.is_empty(), "concurrent-writes-multivault requires at least one vault");

    // Build a shared client up-front (if requested) so every task gets
    // a clone instead of one connect per task. With the pool override
    // applied, that clone propagates the pool-size config, so every
    // task draws from the same pooled-channel set.
    let shared = if shared_client {
        match harness.build_shared_client_with_pool(pool_size).await {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!(error = %e, "failed to build shared client with pool override");
                let mut summary = Summary::default();
                summary.errors += 1;
                summary.elapsed = Duration::ZERO;
                return summary;
            },
        }
    } else {
        None
    };

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let user = harness.user;
        let organization = harness.organization;
        let vault = vaults[task_id % vaults.len()];
        let client: LedgerClient = if let Some(ref c) = shared {
            c.clone()
        } else {
            match harness.connect_task_client_with_pool(task_id, pool_size).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, task_id, "failed to connect per-task client");
                    let mut local = Summary::default();
                    local.errors += 1;
                    handles.push(tokio::spawn(async move { local }));
                    continue;
                },
            }
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
