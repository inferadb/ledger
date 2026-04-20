//! Observability metrics exposed via Prometheus using the `metrics` crate.
//!
//! ## Metric Naming Conventions
//!
//! Metrics use the `ledger_` or `inferadb_ledger_raft_` prefix.
//!
//! - Counters: `_total` suffix
//! - Histograms: `_seconds` or `_bytes` suffix
//! - Gauges: no suffix

use std::time::Instant;

use inferadb_ledger_types::{OrganizationId, VaultId};
use metrics::{counter, gauge, histogram};

use crate::logging::fields;

/// Gate a metric emission through the cardinality tracker.
///
/// If the tracker has not been initialized (pre-bootstrap) or admits the
/// emission, execute `$body`. Otherwise the emission is silently dropped
/// (Enforce mode) or admitted with overflow counting (Observe mode).
macro_rules! gated {
    ($metric_name:expr, $labels:expr, $body:block) => {
        let admitted = match crate::cardinality::tracker() {
            Some(tracker) => {
                let fp = crate::cardinality::fingerprint_labels($labels);
                tracker.admit($metric_name, fp)
            },
            None => true,
        };
        if admitted {
            $body
        }
    };
}

// =============================================================================
// Metric Names (constants for consistency)
// =============================================================================

// Data residency violation metrics
const DATA_RESIDENCY_VIOLATION_TOTAL: &str = "ledger_data_residency_violation_total";

// Raft consensus metrics
const RAFT_PROPOSALS_TOTAL: &str = "inferadb_ledger_raft_proposals_total";
const RAFT_PROPOSAL_TIMEOUTS_TOTAL: &str = "inferadb_ledger_raft_proposal_timeouts_total";
const RAFT_PROPOSALS_PENDING: &str = "inferadb_ledger_raft_proposals_pending";
const RAFT_APPLY_LATENCY: &str = "inferadb_ledger_raft_apply_latency_seconds";
const RAFT_COMMIT_INDEX: &str = "inferadb_ledger_raft_commit_index";
const RAFT_TERM: &str = "inferadb_ledger_raft_term";
const RAFT_LEADER: &str = "inferadb_ledger_raft_is_leader";

// State machine metrics
const STATE_ROOT_COMPUTATIONS: &str = "inferadb_ledger_state_root_computations_total";
const STATE_ROOT_LATENCY: &str = "inferadb_ledger_state_root_latency_seconds";
const STATE_ROOT_VERIFICATIONS: &str = "inferadb_ledger_state_root_verifications_total";
const STATE_ROOT_DIVERGENCES: &str = "inferadb_ledger_state_root_divergences_total";
const DIRTY_BUCKETS: &str = "ledger_dirty_buckets";

// Storage metrics
const STORAGE_BYTES_WRITTEN: &str = "ledger_storage_bytes_written_total";
const STORAGE_BYTES_READ: &str = "ledger_storage_bytes_read_total";
const STORAGE_OPERATIONS: &str = "ledger_storage_operations_total";

// Snapshot metrics
const SNAPSHOTS_CREATED: &str = "ledger_snapshots_created_total";
const SNAPSHOT_SIZE_BYTES: &str = "ledger_snapshot_size_bytes";
const SNAPSHOT_RESTORE_DURATION: &str = "ledger_snapshot_restore_duration_seconds";

// Idempotency cache metrics
const IDEMPOTENCY_OPERATIONS: &str = "ledger_idempotency_operations_total";
const IDEMPOTENCY_SIZE: &str = "ledger_idempotency_cache_size";

// Connection metrics
const ACTIVE_CONNECTIONS: &str = "ledger_active_connections";
const GRPC_REQUESTS_TOTAL: &str = "ledger_grpc_requests_total";
const GRPC_REQUEST_LATENCY: &str = "ledger_grpc_request_latency_seconds";

// Batching metrics
const BATCH_COALESCE_TOTAL: &str = "ledger_batch_coalesce_total";
const BATCH_COALESCE_SIZE: &str = "ledger_batch_coalesce_size";
const BATCH_FLUSH_LATENCY: &str = "ledger_batch_flush_latency_seconds";

// Rate limiting metrics
const RATE_LIMIT_EXCEEDED: &str = "ledger_rate_limit_exceeded_total";
const RATE_LIMIT_REJECTED: &str = "ledger_rate_limit_rejected_total";

// Recovery metrics
const RECOVERY_SUCCESS_TOTAL: &str = "ledger_recovery_success_total";
const RECOVERY_FAILURE_TOTAL: &str = "ledger_recovery_failure_total";
const DETERMINISM_BUG_TOTAL: &str = "ledger_determinism_bug_total";
const RECOVERY_ATTEMPTS_TOTAL: &str = "ledger_divergence_recovery_attempts_total";
const VAULT_HEALTH: &str = "ledger_vault_health";

// Integrity scrubber metrics
const INTEGRITY_PAGES_CHECKED: &str = "ledger_integrity_pages_checked_total";
const INTEGRITY_ERRORS: &str = "ledger_integrity_errors_total";

// Learner refresh metrics
const LEARNER_REFRESH_TOTAL: &str = "ledger_learner_refresh_total";
const LEARNER_REFRESH_LATENCY: &str = "ledger_learner_refresh_latency_seconds";
const LEARNER_CACHE_STALENESS: &str = "ledger_learner_cache_stale_total";
const LEARNER_VOTER_ERRORS: &str = "ledger_learner_voter_errors_total";

// =============================================================================
// Write Service Metrics
// =============================================================================

/// Records a rate limit exceeded event.
///
/// `ledger_rate_limit_exceeded_total{level, reason}`.
#[inline]
pub fn record_rate_limit_exceeded(level: &str, reason: &str) {
    gated!(RATE_LIMIT_EXCEEDED, &[(fields::LEVEL, level), (fields::REASON, reason)], {
        counter!(RATE_LIMIT_EXCEEDED,
            fields::LEVEL => level.to_string(),
            fields::REASON => reason.to_string()
        )
        .increment(1);
    });
}

/// Records a rate limit rejection with level and reason labels.
///
/// `ledger_rate_limit_rejected_total{level, reason}`.
#[inline]
pub fn record_rate_limit_rejected(level: &str, reason: &str) {
    gated!(RATE_LIMIT_REJECTED, &[(fields::LEVEL, level), (fields::REASON, reason)], {
        counter!(RATE_LIMIT_REJECTED, fields::LEVEL => level.to_string(), fields::REASON => reason.to_string())
            .increment(1);
    });
}

// =============================================================================
// Read Service Metrics
// =============================================================================

/// Records a data residency violation attempt.
///
/// Incremented when a request arrives at a node outside the organization's
/// protected region. The request is forwarded, but the violation is tracked
/// for operational alerting.
#[inline]
pub fn record_data_residency_violation(region: &str) {
    gated!(DATA_RESIDENCY_VIOLATION_TOTAL, &[(fields::REGION, region)], {
        counter!(
            DATA_RESIDENCY_VIOLATION_TOTAL,
            fields::REGION => region.to_string()
        )
        .increment(1);
    });
}

// =============================================================================
// Raft Consensus Metrics
// =============================================================================

/// Records a Raft proposal submission.
#[inline]
pub fn record_raft_proposal() {
    counter!(RAFT_PROPOSALS_TOTAL).increment(1);
}

/// Records a Raft proposal that timed out before committing.
#[inline]
pub fn record_raft_proposal_timeout() {
    counter!(RAFT_PROPOSAL_TIMEOUTS_TOTAL).increment(1);
}

/// Sets the number of pending Raft proposals.
#[inline]
pub fn set_pending_proposals(count: usize) {
    gauge!(RAFT_PROPOSALS_PENDING).set(count as f64);
}

/// Records Raft apply latency.
#[inline]
pub fn record_raft_apply_latency(latency_secs: f64) {
    histogram!(RAFT_APPLY_LATENCY).record(latency_secs);
}

/// Sets the current Raft commit index.
#[inline]
pub fn set_raft_commit_index(index: u64) {
    gauge!(RAFT_COMMIT_INDEX).set(index as f64);
}

/// Sets the current Raft term.
#[inline]
pub fn set_raft_term(term: u64) {
    gauge!(RAFT_TERM).set(term as f64);
}

/// Sets whether this node is the Raft leader.
#[inline]
pub fn set_is_leader(is_leader: bool) {
    gauge!(RAFT_LEADER).set(if is_leader { 1.0 } else { 0.0 });
}

// =============================================================================
// State Machine Metrics
// =============================================================================

/// Records a state root computation.
#[inline]
pub fn record_state_root_computation(_vault: VaultId, latency_secs: f64) {
    counter!(STATE_ROOT_COMPUTATIONS).increment(1);
    histogram!(STATE_ROOT_LATENCY).record(latency_secs);
}

/// Records a successful state root verification (local matches leader commitment).
#[inline]
pub fn record_state_root_verification() {
    counter!(STATE_ROOT_VERIFICATIONS).increment(1);
}

/// Records a state root divergence (local state root differs from leader commitment).
///
/// This is a critical alert — it indicates potential Byzantine behavior or a
/// determinism bug in the state machine.
#[inline]
pub fn record_state_root_divergence(_organization: OrganizationId, _vault: VaultId) {
    counter!(STATE_ROOT_DIVERGENCES).increment(1);
}

/// Sets the number of dirty buckets for a vault.
#[inline]
pub fn set_dirty_buckets(_vault: VaultId, count: usize) {
    gauge!(DIRTY_BUCKETS).set(count as f64);
}

// =============================================================================
// Storage Metrics
// =============================================================================

/// Records bytes written to storage.
#[inline]
pub fn record_storage_write(bytes: usize) {
    counter!(STORAGE_BYTES_WRITTEN).increment(bytes as u64);
    counter!(STORAGE_OPERATIONS, fields::OP => "write").increment(1);
}

/// Records bytes read from storage.
#[inline]
pub fn record_storage_read(bytes: usize) {
    counter!(STORAGE_BYTES_READ).increment(bytes as u64);
    counter!(STORAGE_OPERATIONS, fields::OP => "read").increment(1);
}

// =============================================================================
// Snapshot Metrics
// =============================================================================

/// Records a snapshot creation.
#[inline]
pub fn record_snapshot_created(size_bytes: usize) {
    counter!(SNAPSHOTS_CREATED).increment(1);
    histogram!(SNAPSHOT_SIZE_BYTES).record(size_bytes as f64);
}

/// Records a snapshot restore.
#[inline]
pub fn record_snapshot_restore(latency_secs: f64) {
    histogram!(SNAPSHOT_RESTORE_DURATION).record(latency_secs);
}

// =============================================================================
// Idempotency Cache Metrics
// =============================================================================

/// Records an idempotency cache operation.
///
/// `result` must be one of `"hit"`, `"miss"`, or `"eviction"`.
///
/// `ledger_idempotency_operations_total{result}`.
#[inline]
pub fn record_idempotency_operation(result: &str) {
    gated!(IDEMPOTENCY_OPERATIONS, &[(fields::RESULT, result)], {
        counter!(IDEMPOTENCY_OPERATIONS, fields::RESULT => result.to_string()).increment(1);
    });
}

/// Sets the current idempotency cache size.
#[inline]
pub fn set_idempotency_cache_size(size: usize) {
    gauge!(IDEMPOTENCY_SIZE).set(size as f64);
}

// =============================================================================
// Connection Metrics
// =============================================================================

/// Increments the active connection gauge.
#[inline]
pub fn increment_connections() {
    gauge!(ACTIVE_CONNECTIONS).increment(1.0);
}

/// Decrements the active connection gauge.
#[inline]
pub fn decrement_connections() {
    gauge!(ACTIVE_CONNECTIONS).decrement(1.0);
}

/// Records a gRPC request.
///
/// # Arguments
///
/// * `service` - gRPC service name (e.g., `"WriteService"`, `"ReadService"`).
/// * `method` - RPC method name (e.g., `"write"`, `"read"`).
/// * `status` - gRPC status code as a string (e.g., `"OK"`, `"Internal"`).
/// * `latency_secs` - Request latency in seconds.
#[inline]
pub fn record_grpc_request(service: &str, method: &str, status: &str, latency_secs: f64) {
    gated!(
        GRPC_REQUESTS_TOTAL,
        &[(fields::SERVICE, service), (fields::METHOD, method), (fields::STATUS, status)],
        {
            counter!(GRPC_REQUESTS_TOTAL,
                fields::SERVICE => service.to_string(),
                fields::METHOD => method.to_string(),
                fields::STATUS => status.to_string()
            )
            .increment(1);
        }
    );
    gated!(GRPC_REQUEST_LATENCY, &[(fields::SERVICE, service), (fields::METHOD, method)], {
        histogram!(GRPC_REQUEST_LATENCY,
            fields::SERVICE => service.to_string(),
            fields::METHOD => method.to_string()
        )
        .record(latency_secs);
    });
}

// =============================================================================
// Batching Metrics
// =============================================================================

/// Records a batch coalesce event.
#[inline]
pub fn record_batch_coalesce(size: usize) {
    counter!(BATCH_COALESCE_TOTAL).increment(1);
    histogram!(BATCH_COALESCE_SIZE).record(size as f64);
}

/// Records batch flush latency.
#[inline]
pub fn record_batch_flush(latency_secs: f64) {
    histogram!(BATCH_FLUSH_LATENCY).record(latency_secs);
}

// =============================================================================
// Recovery Metrics
// =============================================================================

/// Records a successful vault recovery.
#[inline]
pub fn record_recovery_success(_organization: OrganizationId, _vault: VaultId) {
    counter!(RECOVERY_SUCCESS_TOTAL).increment(1);
}

/// Records a failed vault recovery attempt.
#[inline]
pub fn record_recovery_failure(_organization: OrganizationId, _vault: VaultId, reason: &str) {
    gated!(RECOVERY_FAILURE_TOTAL, &[(fields::REASON, reason)], {
        counter!(
            RECOVERY_FAILURE_TOTAL,
            fields::REASON => reason.to_string()
        )
        .increment(1);
    });
}

/// Records a determinism bug detection (critical alert).
#[inline]
pub fn record_determinism_bug(_organization: OrganizationId, _vault: VaultId) {
    counter!(DETERMINISM_BUG_TOTAL).increment(1);
}

/// Records a divergence recovery attempt with outcome.
#[inline]
pub fn record_recovery_attempt(
    _organization: OrganizationId,
    _vault: VaultId,
    _attempt: u8,
    outcome: &str,
) {
    gated!(RECOVERY_ATTEMPTS_TOTAL, &[(fields::OUTCOME, outcome)], {
        counter!(
            RECOVERY_ATTEMPTS_TOTAL,
            fields::OUTCOME => outcome.to_string()
        )
        .increment(1);
    });
}

/// Sets the vault health gauge.
///
/// State values: 0 = healthy, 1 = diverged, 2 = recovering.
#[inline]
pub fn set_vault_health(_organization: OrganizationId, _vault: VaultId, state: &str) {
    let value = match state {
        "healthy" => 0.0,
        "diverged" => 1.0,
        "recovering" => 2.0,
        _ => -1.0,
    };
    gated!(VAULT_HEALTH, &[(fields::STATE, state)], {
        gauge!(VAULT_HEALTH, fields::STATE => state.to_string()).set(value);
    });
}

// =============================================================================
// Learner Refresh Metrics
// =============================================================================

/// Records a learner refresh attempt.
#[inline]
pub fn record_learner_refresh(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    gated!(LEARNER_REFRESH_TOTAL, &[(fields::STATUS, status)], {
        counter!(LEARNER_REFRESH_TOTAL, fields::STATUS => status).increment(1);
        histogram!(LEARNER_REFRESH_LATENCY, fields::STATUS => status).record(latency_secs);
    });
}

/// Records a learner cache staleness event.
///
/// This is incremented when a learner's cached state becomes stale
/// and requires refresh from a voter.
#[inline]
pub fn record_learner_cache_stale() {
    counter!(LEARNER_CACHE_STALENESS).increment(1);
}

/// Records a voter connection error during learner refresh.
#[inline]
pub fn record_learner_voter_error(voter_id: u64, error_type: &str) {
    let voter_id_str = voter_id.to_string();
    gated!(
        LEARNER_VOTER_ERRORS,
        &[(fields::VOTER_ID, &voter_id_str), (fields::ERROR_TYPE, error_type)],
        {
            counter!(
                LEARNER_VOTER_ERRORS,
                fields::VOTER_ID => voter_id_str.clone(),
                fields::ERROR_TYPE => error_type.to_string()
            )
            .increment(1);
        }
    );
}

// B+ tree compaction metrics
const BTREE_COMPACTION_RUNS_TOTAL: &str = "ledger_btree_compaction_runs_total";
const BTREE_OPERATIONS: &str = "ledger_btree_operations_total";

/// Records a B+ tree compaction run.
///
/// Tracks the number of compaction cycles and the pages merged/freed.
#[inline]
pub fn record_btree_compaction(pages_merged: u64, pages_freed: u64) {
    counter!(BTREE_COMPACTION_RUNS_TOTAL).increment(1);
    counter!(BTREE_OPERATIONS, fields::KIND => "merge").increment(pages_merged);
    counter!(BTREE_OPERATIONS, fields::KIND => "free").increment(pages_freed);
}

/// Records a B+ tree operation.
///
/// `kind` must be one of `"merge"`, `"free"`, or `"split"`.
///
/// `ledger_btree_operations_total{kind}`.
#[inline]
pub fn record_btree_operation(kind: &str) {
    gated!(BTREE_OPERATIONS, &[(fields::KIND, kind)], {
        counter!(BTREE_OPERATIONS, fields::KIND => kind.to_string()).increment(1);
    });
}

// ─── Post-Erasure Compaction ──────────────────────────────────

/// Snapshots triggered by the post-erasure compaction job.
///
/// Labels: `region` = region name
const POST_ERASURE_COMPACTION_TRIGGERED_TOTAL: &str =
    "ledger_post_erasure_compaction_triggered_total";

/// Records a post-erasure compaction snapshot trigger.
pub fn record_post_erasure_compaction_triggered(region: &str) {
    gated!(POST_ERASURE_COMPACTION_TRIGGERED_TOTAL, &[(fields::REGION, region)], {
        counter!(
            POST_ERASURE_COMPACTION_TRIGGERED_TOTAL,
            fields::REGION => region.to_string(),
        )
        .increment(1);
    });
}

// ─── Organization Purge ──────────────────────────────────────

/// Organization purge failures (counter).
///
/// Labels: `tier` = global | regional, `exhausted` = true | false.
const ORG_PURGE_FAILURES_TOTAL: &str = "ledger_org_purge_failures_total";

/// Records an organization purge failure.
///
/// `tier` must be `"global"` or `"regional"`.
/// `exhausted` indicates whether all retry attempts were consumed.
///
/// `ledger_org_purge_failures_total{tier, exhausted}`.
pub fn record_org_purge_failure(tier: &str, exhausted: bool) {
    let exhausted_str = if exhausted { "true" } else { "false" };
    gated!(
        ORG_PURGE_FAILURES_TOTAL,
        &[(fields::TIER, tier), (fields::EXHAUSTED, exhausted_str)],
        {
            counter!(
                ORG_PURGE_FAILURES_TOTAL,
                fields::TIER => tier.to_string(),
                fields::EXHAUSTED => exhausted_str
            )
            .increment(1);
        }
    );
}

// ─── Proof Generation ─────────────────────────────────────────

/// Block proof generation failure counter.
///
/// Incremented when `generate_write_proof` fails after a successful write commit.
/// The write is not affected; only the post-commit proof enrichment failed.
const PROOF_GENERATION_FAILURES_TOTAL: &str = "ledger_proof_generation_failures_total";

/// Records a proof generation failure for a committed write.
///
/// `ledger_proof_generation_failures_total{reason}`.
///
/// `reason` is one of `"block_not_found"`, `"no_transactions"`,
/// `"region_unavailable"`, or `"internal_error"`.
#[inline]
pub fn record_proof_generation_failure(reason: &'static str) {
    gated!(PROOF_GENERATION_FAILURES_TOTAL, &[(fields::REASON, reason)], {
        counter!(
            PROOF_GENERATION_FAILURES_TOTAL,
            fields::REASON => reason
        )
        .increment(1);
    });
}

// ─── Hot Key Detection ────────────────────────────────────────

/// Hot key detection events.
const HOT_KEY_DETECTED_TOTAL: &str = "ledger_hot_key_detected_total";

/// Records a hot key detection event.
///
/// Called whenever a key's access rate exceeds the configured threshold.
/// The vault, key, and rate are captured in the wide event; this counter
/// is label-free to avoid cardinality explosion.
#[inline]
pub fn record_hot_key_detected(_vault: VaultId, _key: &str, _ops_per_sec: f64) {
    counter!(HOT_KEY_DETECTED_TOTAL).increment(1);
}

// ─── SLI/SLO Metrics ──────────────────────────────────────────

/// Batches writer queue depth gauge.
const BATCH_QUEUE_DEPTH: &str = "ledger_batch_queue_depth";

/// Rate limiter queue depth gauge (pending proposals tracked by backpressure tier).
const RATE_LIMIT_QUEUE_DEPTH: &str = "ledger_rate_limit_queue_depth";

/// Cluster quorum status gauge (1 = quorum, 0 = lost).
const CLUSTER_QUORUM_STATUS: &str = "ledger_cluster_quorum_status";

/// Leader election counter.
const LEADER_ELECTIONS_TOTAL: &str = "ledger_leader_elections_total";

/// Sets the current batch writer queue depth.
///
/// Tracks how many write operations are pending in the batch writer,
/// serving as a leading indicator of write saturation.
/// The `region` label identifies which region's batch writer is being measured.
#[inline]
pub fn set_batch_queue_depth(depth: usize, region: &str) {
    gated!(BATCH_QUEUE_DEPTH, &[(fields::REGION, region)], {
        gauge!(BATCH_QUEUE_DEPTH, fields::REGION => region.to_string()).set(depth as f64);
    });
}

/// Sets the current rate limiter queue depth.
///
/// Tracks the number of pending proposals seen by the rate limiter's
/// backpressure tier, indicating write pipeline saturation.
/// The `region` label identifies which region's rate limiter is being measured.
#[inline]
pub fn set_rate_limit_queue_depth(depth: u64, region: &str) {
    gated!(RATE_LIMIT_QUEUE_DEPTH, &[(fields::REGION, region)], {
        gauge!(RATE_LIMIT_QUEUE_DEPTH, fields::REGION => region.to_string()).set(depth as f64);
    });
}

/// Sets the cluster quorum status.
///
/// - `1.0` — a leader is elected and the cluster has quorum
/// - `0.0` — no leader, quorum lost
#[inline]
pub fn set_cluster_quorum_status(has_quorum: bool) {
    gauge!(CLUSTER_QUORUM_STATUS).set(if has_quorum { 1.0 } else { 0.0 });
}

/// Records a leader election event.
///
/// Should be called when a Raft term change is detected, indicating
/// a new leader election has occurred.
#[inline]
pub fn record_leader_election() {
    counter!(LEADER_ELECTIONS_TOTAL).increment(1);
}

// ─── Resource Saturation Metrics ──────────────────────────────

/// Disk space total bytes gauge.
const DISK_BYTES_TOTAL: &str = "ledger_disk_bytes_total";

/// Disk space free bytes gauge.
const DISK_BYTES_FREE: &str = "ledger_disk_bytes_free";

/// Disk space used bytes gauge.
const DISK_BYTES_USED: &str = "ledger_disk_bytes_used";

/// Page cache hit counter.
const PAGE_CACHE_HITS_TOTAL: &str = "ledger_page_cache_hits_total";

/// Page cache miss counter.
const PAGE_CACHE_MISSES_TOTAL: &str = "ledger_page_cache_misses_total";

/// Page cache current size gauge.
const PAGE_CACHE_SIZE: &str = "ledger_page_cache_size";

/// B-tree depth gauge (per-table label).
const BTREE_DEPTH: &str = "ledger_btree_depth";

// BTREE_PAGE_SPLITS_TOTAL removed — splits now emitted via BTREE_OPERATIONS with kind="split".

/// Compaction lag blocks gauge (free pages as a proxy for reclaimable space).
const COMPACTION_LAG_BLOCKS: &str = "ledger_compaction_lag_blocks";

/// Snapshot total disk bytes gauge.
const SNAPSHOT_DISK_BYTES: &str = "ledger_snapshot_disk_bytes";

/// Sets disk space metrics.
///
/// Updates total, free, and used disk bytes for the data directory's filesystem.
/// The `region` label identifies which region's storage is being measured.
#[inline]
pub fn set_disk_bytes(total: u64, free: u64, region: &str) {
    gated!(DISK_BYTES_TOTAL, &[(fields::REGION, region)], {
        gauge!(DISK_BYTES_TOTAL, fields::REGION => region.to_string()).set(total as f64);
        gauge!(DISK_BYTES_FREE, fields::REGION => region.to_string()).set(free as f64);
        gauge!(DISK_BYTES_USED, fields::REGION => region.to_string())
            .set((total.saturating_sub(free)) as f64);
    });
}

/// Sets page cache counters.
///
/// Reports cumulative cache hit/miss totals and current cache size.
/// The `region` label identifies which region's database is being measured.
#[inline]
pub fn set_page_cache_metrics(hits: u64, misses: u64, size: usize, region: &str) {
    gated!(PAGE_CACHE_HITS_TOTAL, &[(fields::REGION, region)], {
        counter!(PAGE_CACHE_HITS_TOTAL, fields::REGION => region.to_string()).absolute(hits);
        counter!(PAGE_CACHE_MISSES_TOTAL, fields::REGION => region.to_string()).absolute(misses);
        gauge!(PAGE_CACHE_SIZE, fields::REGION => region.to_string()).set(size as f64);
    });
}

/// Sets B-tree depth for a given table.
///
/// The `region` label identifies which region's database is being measured.
#[inline]
pub fn set_btree_depth(table: &str, depth: u32, region: &str) {
    gated!(BTREE_DEPTH, &[(fields::TABLE, table), (fields::REGION, region)], {
        gauge!(BTREE_DEPTH, fields::TABLE => table.to_string(), fields::REGION => region.to_string())
            .set(f64::from(depth));
    });
}

/// Sets B-tree page splits total (cumulative since startup).
///
/// Emits into `ledger_btree_operations_total{kind="split"}`.
/// The `region` label identifies which region's database is being measured.
#[inline]
pub fn set_btree_page_splits(total: u64, region: &str) {
    gated!(BTREE_OPERATIONS, &[(fields::KIND, "split"), (fields::REGION, region)], {
        counter!(BTREE_OPERATIONS, fields::KIND => "split", fields::REGION => region.to_string())
            .absolute(total);
    });
}

/// Sets compaction lag blocks gauge.
///
/// Tracks the number of free pages (reclaimable space) as a proxy for
/// compaction backlog. High values indicate that compaction is falling behind.
/// The `region` label identifies which region's database is being measured.
#[inline]
pub fn set_compaction_lag_blocks(blocks: usize, region: &str) {
    gated!(COMPACTION_LAG_BLOCKS, &[(fields::REGION, region)], {
        gauge!(COMPACTION_LAG_BLOCKS, fields::REGION => region.to_string()).set(blocks as f64);
    });
}

/// Sets snapshot total disk bytes.
///
/// Tracks the total disk space used by all snapshots in the snapshot directory.
/// The `region` label identifies which region's snapshots are being measured.
#[inline]
pub fn set_snapshot_disk_bytes(bytes: u64, region: &str) {
    gated!(SNAPSHOT_DISK_BYTES, &[(fields::REGION, region)], {
        gauge!(SNAPSHOT_DISK_BYTES, fields::REGION => region.to_string()).set(bytes as f64);
    });
}

// ─── State-DB Checkpointer ────────────────────────────────────

/// State checkpoint events (counter). Labels: `region`, `trigger`, `status`.
///
/// `trigger` is one of `time` | `applies` | `dirty` | `snapshot` | `backup` |
/// `shutdown`.
/// `status` is `ok` | `error`.
pub const LEDGER_STATE_CHECKPOINTS_TOTAL: &str = "ledger_state_checkpoints_total";

/// State checkpoint duration in seconds (histogram). Labels: `region`, `trigger`.
pub const LEDGER_STATE_CHECKPOINT_DURATION_SECONDS: &str =
    "ledger_state_checkpoint_duration_seconds";

/// Unix timestamp of the most recent successful state checkpoint (gauge).
/// Labels: `region`.
pub const LEDGER_STATE_CHECKPOINT_LAST_TIMESTAMP_SECONDS: &str =
    "ledger_state_checkpoint_last_timestamp_seconds";

/// Applies accumulated in memory since the last successful checkpoint (gauge).
/// Labels: `region`.
pub const LEDGER_STATE_APPLIES_SINCE_CHECKPOINT: &str = "ledger_state_applies_since_checkpoint";

/// Dirty page count observed at the most recent checkpointer wake-up (gauge).
/// Labels: `region`.
pub const LEDGER_STATE_DIRTY_PAGES: &str = "ledger_state_dirty_pages";

/// Total in-memory pages resident in the state-DB page cache (gauge).
/// Labels: `region`.
pub const LEDGER_STATE_PAGE_CACHE_LEN: &str = "ledger_state_page_cache_len";

/// Most recent snapshot id durably persisted to disk (gauge). Labels: `region`.
pub const LEDGER_STATE_LAST_SYNCED_SNAPSHOT_ID: &str = "ledger_state_last_synced_snapshot_id";

/// Number of WAL entries replayed during the post-open crash-recovery sweep
/// (counter). Fired exactly once per region on startup, with value 0 on
/// clean shutdown. Labels: `region`.
pub const LEDGER_STATE_RECOVERY_REPLAY_COUNT_TOTAL: &str =
    "ledger_state_recovery_replay_count_total";

/// Duration of the post-open crash-recovery sweep (histogram, seconds).
/// Fired exactly once per region on startup, regardless of whether any
/// entries were replayed — operators can watch p99 for pathological recovery
/// windows. Labels: `region`.
pub const LEDGER_STATE_RECOVERY_DURATION_SECONDS: &str = "ledger_state_recovery_duration_seconds";

/// Records a state checkpoint attempt.
///
/// `trigger` is one of `"time"`, `"applies"`, `"dirty"` (emitted by the
/// background checkpointer), plus `"snapshot"`, `"backup"`, `"shutdown"`
/// (emitted by their owning code paths in Tasks 2B/2C).
/// `status` is `"ok"` or `"error"`.
#[inline]
pub fn record_state_checkpoint(region: &str, trigger: &str, status: &str, duration_secs: f64) {
    gated!(
        LEDGER_STATE_CHECKPOINTS_TOTAL,
        &[(fields::REGION, region), (fields::TRIGGER, trigger), (fields::STATUS, status)],
        {
            counter!(
                LEDGER_STATE_CHECKPOINTS_TOTAL,
                fields::REGION => region.to_string(),
                fields::TRIGGER => trigger.to_string(),
                fields::STATUS => status.to_string(),
            )
            .increment(1);
        }
    );
    gated!(
        LEDGER_STATE_CHECKPOINT_DURATION_SECONDS,
        &[(fields::REGION, region), (fields::TRIGGER, trigger)],
        {
            histogram!(
                LEDGER_STATE_CHECKPOINT_DURATION_SECONDS,
                fields::REGION => region.to_string(),
                fields::TRIGGER => trigger.to_string(),
            )
            .record(duration_secs);
        }
    );
}

/// Updates the applies-since-last-checkpoint gauge.
#[inline]
pub fn set_state_applies_since_checkpoint(region: &str, applies: u64) {
    gated!(LEDGER_STATE_APPLIES_SINCE_CHECKPOINT, &[(fields::REGION, region)], {
        gauge!(LEDGER_STATE_APPLIES_SINCE_CHECKPOINT, fields::REGION => region.to_string())
            .set(applies as f64);
    });
}

/// Updates the dirty-pages gauge sampled at checkpointer wake-ups.
#[inline]
pub fn set_state_dirty_pages(region: &str, dirty_pages: u64) {
    gated!(LEDGER_STATE_DIRTY_PAGES, &[(fields::REGION, region)], {
        gauge!(LEDGER_STATE_DIRTY_PAGES, fields::REGION => region.to_string())
            .set(dirty_pages as f64);
    });
}

/// Updates the total page-cache-size gauge.
#[inline]
pub fn set_state_page_cache_len(region: &str, cache_len: u64) {
    gated!(LEDGER_STATE_PAGE_CACHE_LEN, &[(fields::REGION, region)], {
        gauge!(LEDGER_STATE_PAGE_CACHE_LEN, fields::REGION => region.to_string())
            .set(cache_len as f64);
    });
}

/// Updates the last-synced-snapshot-id gauge.
#[inline]
pub fn set_state_last_synced_snapshot_id(region: &str, snapshot_id: u64) {
    gated!(LEDGER_STATE_LAST_SYNCED_SNAPSHOT_ID, &[(fields::REGION, region)], {
        gauge!(LEDGER_STATE_LAST_SYNCED_SNAPSHOT_ID, fields::REGION => region.to_string())
            .set(snapshot_id as f64);
    });
}

/// Updates the last-checkpoint-timestamp gauge (Unix seconds).
#[inline]
pub fn set_state_checkpoint_last_timestamp(region: &str, unix_secs: f64) {
    gated!(LEDGER_STATE_CHECKPOINT_LAST_TIMESTAMP_SECONDS, &[(fields::REGION, region)], {
        gauge!(
            LEDGER_STATE_CHECKPOINT_LAST_TIMESTAMP_SECONDS,
            fields::REGION => region.to_string(),
        )
        .set(unix_secs);
    });
}

/// Records the number of WAL entries replayed by `RaftLogStore::replay_crash_gap`.
///
/// Called exactly once per region on startup, with `count = 0` on clean
/// shutdowns. Surfacing zero-count recoveries is deliberate: dashboards can
/// plot `rate()` to see restarts per deploy.
#[inline]
pub fn record_state_recovery_replay(region: &str, count: u64) {
    gated!(LEDGER_STATE_RECOVERY_REPLAY_COUNT_TOTAL, &[(fields::REGION, region)], {
        counter!(
            LEDGER_STATE_RECOVERY_REPLAY_COUNT_TOTAL,
            fields::REGION => region.to_string(),
        )
        .increment(count);
    });
}

/// Records the duration of the post-open crash-recovery sweep.
///
/// Fires unconditionally — even a zero-entry recovery did some work
/// (reading the last-applied sentinel + coalescing a no-op sync). Samples
/// land in `SLI_HISTOGRAM_BUCKETS`; pathological long recoveries alert on p99.
#[inline]
pub fn record_state_recovery_duration(region: &str, duration: std::time::Duration) {
    gated!(LEDGER_STATE_RECOVERY_DURATION_SECONDS, &[(fields::REGION, region)], {
        histogram!(
            LEDGER_STATE_RECOVERY_DURATION_SECONDS,
            fields::REGION => region.to_string(),
        )
        .record(duration.as_secs_f64());
    });
}

// ─── Handler-Phase Event Flusher ──────────────────────

/// Total handler-phase event flushes by trigger reason (counter).
/// Labels: `region`, `trigger = time | size | shutdown`.
pub const LEDGER_EVENT_FLUSH_TRIGGERS_TOTAL: &str = "ledger_event_flush_triggers_total";

/// Duration of a single handler-phase event flush (histogram, seconds).
/// Labels: `region`. Uses `SLI_HISTOGRAM_BUCKETS`.
pub const LEDGER_EVENT_FLUSH_DURATION_SECONDS: &str = "ledger_event_flush_duration_seconds";

/// Queue depth sampled at flush entry (gauge). Labels: `region`.
pub const LEDGER_EVENT_FLUSH_QUEUE_DEPTH: &str = "ledger_event_flush_queue_depth";

/// Entries drained per flush (histogram). Labels: `region`.
pub const LEDGER_EVENT_FLUSH_ENTRIES_PER_FLUSH: &str = "ledger_event_flush_entries_per_flush";

/// Handler-phase event flush failures (counter). Labels: `region`.
pub const LEDGER_EVENT_FLUSH_FAILURES_TOTAL: &str = "ledger_event_flush_failures_total";

/// Handler-phase event overflows by cause (counter).
/// Labels: `region`, `cause = queue_full | shutdown_timeout | channel_closed`.
pub const LEDGER_EVENT_OVERFLOW_TOTAL: &str = "ledger_event_overflow_total";

/// Records a successful handler-phase flush attempt.
///
/// `trigger` is one of `"time"`, `"size"`, `"shutdown"` — emitted by the
/// background flusher in `event_writer.rs`.
#[inline]
pub fn record_event_flush(region: &str, trigger: &str, duration_secs: f64, entries: u64) {
    gated!(
        LEDGER_EVENT_FLUSH_TRIGGERS_TOTAL,
        &[(fields::REGION, region), (fields::TRIGGER, trigger)],
        {
            counter!(
                LEDGER_EVENT_FLUSH_TRIGGERS_TOTAL,
                fields::REGION => region.to_string(),
                fields::TRIGGER => trigger.to_string(),
            )
            .increment(1);
        }
    );
    gated!(LEDGER_EVENT_FLUSH_DURATION_SECONDS, &[(fields::REGION, region)], {
        histogram!(
            LEDGER_EVENT_FLUSH_DURATION_SECONDS,
            fields::REGION => region.to_string(),
        )
        .record(duration_secs);
    });
    gated!(LEDGER_EVENT_FLUSH_ENTRIES_PER_FLUSH, &[(fields::REGION, region)], {
        histogram!(
            LEDGER_EVENT_FLUSH_ENTRIES_PER_FLUSH,
            fields::REGION => region.to_string(),
        )
        .record(entries as f64);
    });
}

/// Records a flush failure (events.db commit / write error).
#[inline]
pub fn record_event_flush_failure(region: &str) {
    gated!(LEDGER_EVENT_FLUSH_FAILURES_TOTAL, &[(fields::REGION, region)], {
        counter!(
            LEDGER_EVENT_FLUSH_FAILURES_TOTAL,
            fields::REGION => region.to_string(),
        )
        .increment(1);
    });
}

/// Updates the pre-drain queue-depth gauge sampled at flush entry.
#[inline]
pub fn set_event_flush_queue_depth(region: &str, depth: u64) {
    gated!(LEDGER_EVENT_FLUSH_QUEUE_DEPTH, &[(fields::REGION, region)], {
        gauge!(LEDGER_EVENT_FLUSH_QUEUE_DEPTH, fields::REGION => region.to_string())
            .set(depth as f64);
    });
}

/// Records a handler-phase event overflow.
///
/// `cause` is one of `"queue_full"` (producer tried to enqueue on a full
/// channel under `overflow_behavior = Drop`), `"shutdown_timeout"`
/// (flush-for-shutdown exited with queued entries remaining), or
/// `"channel_closed"` (bug signal — producer saw a closed channel).
#[inline]
pub fn record_event_overflow(region: &str, cause: &str, count: u64) {
    if count == 0 {
        return;
    }
    gated!(LEDGER_EVENT_OVERFLOW_TOTAL, &[(fields::REGION, region), (fields::CAUSE, cause)], {
        counter!(
            LEDGER_EVENT_OVERFLOW_TOTAL,
            fields::REGION => region.to_string(),
            fields::CAUSE => cause.to_string(),
        )
        .increment(count);
    });
}

/// SLI-aligned histogram bucket boundaries (in seconds).
///
/// These buckets are designed for latency SLI/SLO tracking:
/// - Sub-millisecond: 1ms (p50 target for reads)
/// - Low-latency: 5ms, 10ms, 25ms (p95/p99 read targets)
/// - Medium-latency: 50ms, 100ms, 250ms (p95/p99 write targets)
/// - High-latency: 500ms, 1s, 5s, 10s (tail latency / timeouts)
pub const SLI_HISTOGRAM_BUCKETS: [f64; 11] =
    [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0];

// =============================================================================
// Timer Helper
// =============================================================================

/// A timer that records latency on drop.
pub struct Timer {
    start: Instant,
    record_fn: Option<Box<dyn FnOnce(f64) + Send>>,
}

impl Timer {
    /// Creates a new timer.
    pub fn new<F: FnOnce(f64) + Send + 'static>(record_fn: F) -> Self {
        Self { start: Instant::now(), record_fn: Some(Box::new(record_fn)) }
    }

    /// Returns elapsed time in seconds.
    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    /// Cancels the on-drop recording and returns the elapsed time in seconds.
    ///
    /// Unlike dropping the timer (which records to the histogram), this
    /// consumes the timer without emitting a metric.
    pub fn stop(mut self) -> f64 {
        self.record_fn = None;
        self.elapsed_secs()
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(record_fn) = self.record_fn.take() {
            record_fn(self.elapsed_secs());
        }
    }
}

// ─── Integrity Scrubber Metrics ──────────────────────────────

/// Records the number of pages checked in a scrub cycle.
#[inline]
pub fn record_integrity_pages_checked(count: u64) {
    counter!(INTEGRITY_PAGES_CHECKED).increment(count);
}

/// Records the number of integrity errors detected in a scrub cycle.
///
/// Labels by error type: "checksum" for data corruption, "structural" for
/// B-tree invariant violations.
#[inline]
pub fn record_integrity_errors(error_type: &str, count: u64) {
    gated!(INTEGRITY_ERRORS, &[(fields::ERROR_TYPE, error_type)], {
        counter!(INTEGRITY_ERRORS, fields::ERROR_TYPE => error_type.to_string()).increment(count);
    });
}

// ─── Organization Resource Accounting Metrics ────────────────

/// Per-organization cumulative storage bytes (gauge).
const ORGANIZATION_STORAGE_BYTES: &str = "ledger_organization_storage_bytes";

/// Per-organization operation counter.
const ORGANIZATION_OPERATIONS_TOTAL: &str = "ledger_organization_operations_total";

/// Per-organization operation latency histogram.
const ORGANIZATION_LATENCY_SECONDS: &str = "ledger_organization_latency_seconds";

/// Sets the current cumulative storage bytes for an organization.
#[inline]
pub fn set_organization_storage_bytes(_organization: OrganizationId, bytes: u64) {
    gauge!(ORGANIZATION_STORAGE_BYTES).set(bytes as f64);
}

/// Records an organization-level operation (read, write, or admin).
///
/// Increments `ledger_organization_operations_total{operation}`.
#[inline]
pub fn record_organization_operation(_organization: OrganizationId, operation: &str) {
    gated!(ORGANIZATION_OPERATIONS_TOTAL, &[(fields::OPERATION, operation)], {
        counter!(
            ORGANIZATION_OPERATIONS_TOTAL,
            fields::OPERATION => operation.to_string()
        )
        .increment(1);
    });
}

/// Records per-organization operation latency.
///
/// Records into `ledger_organization_latency_seconds{operation}`.
#[inline]
pub fn record_organization_latency(
    _organization: OrganizationId,
    operation: &str,
    latency_secs: f64,
) {
    gated!(ORGANIZATION_LATENCY_SECONDS, &[(fields::OPERATION, operation)], {
        histogram!(
            ORGANIZATION_LATENCY_SECONDS,
            fields::OPERATION => operation.to_string()
        )
        .record(latency_secs);
    });
}

// ─── Background Job Observability Metrics ────────────────────

/// Duration of each background job cycle (histogram, seconds).
///
/// Labels: `job` = gc | compaction | integrity_scrub | auto_recovery | backup
const BACKGROUND_JOB_DURATION_SECONDS: &str = "ledger_background_job_duration_seconds";

/// Total number of background job cycle runs (counter).
///
/// Labels: `job`, `result` = success | failure
const BACKGROUND_JOB_RUNS_TOTAL: &str = "ledger_background_job_runs_total";

/// Total items processed by background jobs (counter).
///
/// Labels: `job`
///
/// Meaning per job:
/// - `gc`: blocks compacted
/// - `compaction`: pages merged
/// - `integrity_scrub`: pages checked
/// - `auto_recovery`: vaults recovered
/// - `backup`: backups created
const BACKGROUND_JOB_ITEMS_PROCESSED_TOTAL: &str = "ledger_background_job_items_processed_total";

/// Records the duration of a background job cycle.
#[inline]
pub fn record_background_job_duration(job: &str, duration_secs: f64) {
    gated!(BACKGROUND_JOB_DURATION_SECONDS, &[(fields::JOB, job)], {
        histogram!(
            BACKGROUND_JOB_DURATION_SECONDS,
            fields::JOB => job.to_string()
        )
        .record(duration_secs);
    });
}

/// Records a completed background job cycle.
///
/// `result` must be `"success"` or `"failure"` — bounded cardinality.
#[inline]
pub fn record_background_job_run(job: &str, result: &str) {
    gated!(BACKGROUND_JOB_RUNS_TOTAL, &[(fields::JOB, job), (fields::RESULT, result)], {
        counter!(
            BACKGROUND_JOB_RUNS_TOTAL,
            fields::JOB => job.to_string(),
            fields::RESULT => result.to_string()
        )
        .increment(1);
    });
}

/// Records items processed by a background job cycle.
#[inline]
pub fn record_background_job_items(job: &str, count: u64) {
    gated!(BACKGROUND_JOB_ITEMS_PROCESSED_TOTAL, &[(fields::JOB, job)], {
        counter!(
            BACKGROUND_JOB_ITEMS_PROCESSED_TOTAL,
            fields::JOB => job.to_string()
        )
        .increment(count);
    });
}

// ─── DEK Re-Wrapping Metrics ──────────────────────────────────

/// Total pages re-wrapped during RMK rotation (counter).
const REWRAP_PAGES_TOTAL: &str = "ledger_rewrap_pages_total";

/// Remaining pages to process during re-wrapping (gauge).
const REWRAP_PAGES_REMAINING: &str = "ledger_rewrap_pages_remaining";

/// Records pages re-wrapped during a batch cycle.
#[inline]
pub fn record_rewrap_pages(count: u64) {
    counter!(REWRAP_PAGES_TOTAL).increment(count);
}

/// Records remaining pages to re-wrap (gauge).
#[inline]
pub fn record_rewrap_remaining(remaining: u64) {
    gauge!(REWRAP_PAGES_REMAINING).set(remaining as f64);
}

// ─── Metric Cardinality Budget Metrics ────────────────────────

/// Total metric observations dropped due to cardinality budget overflow (counter).
///
/// Labels: `metric_name` — the metric family that exceeded `max_cardinality`.
/// Cardinality of this meta-metric is bounded by the number of distinct metric
/// families in the application (typically < 100).
const CARDINALITY_OVERFLOW_TOTAL: &str = "ledger_metrics_cardinality_overflow_total";

/// Records that a metric observation was dropped due to cardinality overflow.
#[inline]
pub fn record_cardinality_overflow(metric_name: &str) {
    counter!(
        CARDINALITY_OVERFLOW_TOTAL,
        fields::METRIC_NAME => metric_name.to_string()
    )
    .increment(1);
}

// ─── Events Ingestion Metrics ─────────────────────────────────

/// Total ingested events (counter).
///
/// Labels: `source_service` = engine | control, `outcome` = accepted | rejected.
const EVENTS_INGEST_TOTAL: &str = "ledger_events_ingest_total";

/// Ingested batch size (histogram).
///
/// Labels: `source_service` = engine | control.
const EVENTS_INGEST_BATCH_SIZE: &str = "ledger_events_ingest_batch_size";

/// Total ingestion requests rejected by rate limiter (counter).
///
/// Labels: `source_service` = engine | control.
const EVENTS_INGEST_RATE_LIMITED_TOTAL: &str = "ledger_events_ingest_rate_limited_total";

/// Ingestion request duration (histogram, seconds).
const EVENTS_INGEST_DURATION_SECONDS: &str = "ledger_events_ingest_duration_seconds";

/// Records ingested events by outcome.
///
/// Called by `IngestEvents` handler after processing a batch.
#[inline]
pub fn record_events_ingest(source_service: &str, outcome: &str, count: u32) {
    gated!(
        EVENTS_INGEST_TOTAL,
        &[(fields::SOURCE_SERVICE, source_service), (fields::OUTCOME, outcome)],
        {
            counter!(
                EVENTS_INGEST_TOTAL,
                fields::SOURCE_SERVICE => source_service.to_string(),
                fields::OUTCOME => outcome.to_string()
            )
            .increment(u64::from(count));
        }
    );
}

/// Records the batch size of an ingestion request.
#[inline]
pub fn record_events_ingest_batch_size(source_service: &str, size: usize) {
    gated!(EVENTS_INGEST_BATCH_SIZE, &[(fields::SOURCE_SERVICE, source_service)], {
        histogram!(
            EVENTS_INGEST_BATCH_SIZE,
            fields::SOURCE_SERVICE => source_service.to_string()
        )
        .record(size as f64);
    });
}

/// Records an ingestion rate limit rejection.
#[inline]
pub fn record_events_ingest_rate_limited(source_service: &str) {
    gated!(EVENTS_INGEST_RATE_LIMITED_TOTAL, &[(fields::SOURCE_SERVICE, source_service)], {
        counter!(
            EVENTS_INGEST_RATE_LIMITED_TOTAL,
            fields::SOURCE_SERVICE => source_service.to_string()
        )
        .increment(1);
    });
}

/// Records the duration of an ingestion request.
#[inline]
pub fn record_events_ingest_duration(duration_secs: f64) {
    histogram!(EVENTS_INGEST_DURATION_SECONDS).record(duration_secs);
}

// ─── Event Write Metrics ──────────────────────────────────────

/// Total event entries written to the events database (counter).
///
/// Labels: `scope` = system | organization, `action` = snake_case action string.
const EVENT_WRITES_TOTAL: &str = "ledger_event_writes_total";

/// Records an event write to Prometheus.
///
/// Called by [`EventWriter`](crate::event_writer::EventWriter) and
/// [`EventHandle`](crate::event_writer::EventHandle) after each successful
/// event persist. Labels: `emission` (apply_phase | handler_phase),
/// `scope` (system | organization), `action` (snake_case action string).
#[inline]
pub fn record_event_write(emission: &str, scope: &str, action: &str) {
    gated!(
        EVENT_WRITES_TOTAL,
        &[(fields::EMISSION, emission), (fields::SCOPE, scope), (fields::ACTION, action)],
        {
            counter!(
                EVENT_WRITES_TOTAL,
                fields::EMISSION => emission.to_string(),
                fields::SCOPE => scope.to_string(),
                fields::ACTION => action.to_string()
            )
            .increment(1);
        }
    );
}

/// Creates a timer for Raft apply operations.
pub fn raft_apply_timer() -> Timer {
    Timer::new(record_raft_apply_latency)
}

// ─── Events GC Metrics ──────────────────────────────────────
// Covered by ledger_background_job_* family (JobContext). No standalone constants needed.

// ---------------------------------------------------------------------------
// Leader Transfer
// ---------------------------------------------------------------------------

/// Total leader transfer attempts (counter).
///
/// Labels: `status` = success | failure.
const LEADER_TRANSFERS_TOTAL: &str = "ledger_leader_transfers_total";

/// Leader transfer latency in seconds (histogram).
///
/// Labels: `status` = success | failure.
const LEADER_TRANSFER_LATENCY: &str = "ledger_leader_transfer_latency_seconds";

/// Total trigger election requests received (counter).
///
/// Labels: `result` = accepted | rejected.
const TRIGGER_ELECTIONS_TOTAL: &str = "ledger_trigger_elections_total";

// ---------------------------------------------------------------------------
// Leader Transfer
// ---------------------------------------------------------------------------

/// Records a leader transfer attempt with its outcome and latency.
#[inline]
pub fn record_leader_transfer(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "failure" };
    gated!(LEADER_TRANSFERS_TOTAL, &[(fields::STATUS, status)], {
        counter!(LEADER_TRANSFERS_TOTAL, fields::STATUS => status).increment(1);
        histogram!(LEADER_TRANSFER_LATENCY, fields::STATUS => status).record(latency_secs);
    });
}

/// Records a trigger election request received by this node.
#[inline]
pub fn record_trigger_election(accepted: bool) {
    let result = if accepted { "accepted" } else { "rejected" };
    gated!(TRIGGER_ELECTIONS_TOTAL, &[(fields::RESULT, result)], {
        counter!(TRIGGER_ELECTIONS_TOTAL, fields::RESULT => result).increment(1);
    });
}

// ---------------------------------------------------------------------------
// Onboarding
// ---------------------------------------------------------------------------

/// Total onboarding requests (counter).
///
/// Labels: `stage` = initiate | verify | register, `status` = success | failure.
const ONBOARDING_REQUESTS_TOTAL: &str = "ledger_onboarding_requests_total";

/// Records an onboarding request by stage and status.
///
/// `stage` must be one of `"initiate"`, `"verify"`, or `"register"`.
/// `status` must be one of `"success"` or `"failure"`.
///
/// `ledger_onboarding_requests_total{stage, status}`.
#[inline]
pub fn record_onboarding_request(stage: &str, status: &str) {
    gated!(ONBOARDING_REQUESTS_TOTAL, &[(fields::STAGE, stage), (fields::STATUS, status)], {
        counter!(
            ONBOARDING_REQUESTS_TOTAL,
            fields::STAGE => stage.to_string(),
            fields::STATUS => status.to_string()
        )
        .increment(1);
    });
}

// =============================================================================
// Per-Peer Consensus Transport Metrics
// =============================================================================

/// Records a peer consensus-message send completion. Called by the per-peer
/// drain task in `consensus_transport::peer_sender`.
#[inline]
pub fn record_peer_send_latency(peer: u64, latency_secs: f64) {
    let peer_str = peer.to_string();
    gated!("ledger_peer_send_latency_seconds", &[(fields::PEER, &peer_str)], {
        histogram!("ledger_peer_send_latency_seconds", fields::PEER => peer_str)
            .record(latency_secs);
    });
}

/// Records a dropped outbound consensus message.
///
/// `reason` is one of:
/// - `"queue_full"` — push evicted the oldest message due to capacity overflow.
/// - `"task_shutdown"` — peer was removed while messages were still queued.
#[inline]
pub fn record_peer_send_drop(peer: u64, reason: &'static str) {
    let peer_str = peer.to_string();
    gated!(
        "ledger_peer_send_drops_total",
        &[(fields::PEER, &peer_str), (fields::REASON, reason)],
        {
            counter!(
                "ledger_peer_send_drops_total",
                fields::PEER => peer_str,
                fields::REASON => reason,
            )
            .increment(1);
        }
    );
}

/// Sets the per-peer send queue depth gauge.
#[inline]
pub fn record_peer_send_queue_depth(peer: u64, depth: usize) {
    #[allow(clippy::cast_precision_loss)]
    let depth_f64 = depth as f64;
    let peer_str = peer.to_string();
    gated!("ledger_peer_send_queue_depth", &[(fields::PEER, &peer_str)], {
        gauge!("ledger_peer_send_queue_depth", fields::PEER => peer_str).set(depth_f64);
    });
}

/// Records a consensus-stream reconnect attempt for a peer. Incremented
/// whenever the drain loop (re)enters the stream-open path after a prior
/// failure — either because the initial stream open failed or because a
/// previously healthy stream broke and we are retrying.
#[inline]
pub fn record_peer_stream_reconnect(peer: u64) {
    let peer_str = peer.to_string();
    gated!("ledger_peer_stream_reconnects_total", &[(fields::PEER, &peer_str)], {
        counter!("ledger_peer_stream_reconnects_total", fields::PEER => peer_str).increment(1);
    });
}

/// Records a node connection registry lifecycle event.
///
/// `event` is one of:
/// - `"registered"` — a new peer was added to the registry.
/// - `"unregistered"` — a peer was explicitly removed via `unregister`.
/// - `"pruned"` — a peer was removed by `on_membership_changed`.
/// - `"replaced"` — a peer's address changed; the old entry was evicted before re-registering under
///   the new address.
#[inline]
pub fn record_node_connection_event(peer: u64, event: &'static str) {
    let peer_str = peer.to_string();
    gated!(
        "ledger_node_connection_events_total",
        &[(fields::PEER, &peer_str), (fields::EVENT, event)],
        {
            counter!(
                "ledger_node_connection_events_total",
                fields::PEER => peer_str,
                fields::EVENT => event,
            )
            .increment(1);
        }
    );
}

/// Sets the active node-connection count gauge.
#[inline]
pub fn record_node_connections_active(count: usize) {
    #[allow(clippy::cast_precision_loss)]
    let count_f64 = count as f64;
    gauge!("ledger_node_connections_active").set(count_f64);
}

/// Increments the counter tracking invitation email-hash scans that exceeded the safety ceiling.
///
/// Emitted when the number of index entries for a given email hash reaches `SCAN_CEILING`,
/// indicating a potential throughput-amplification pattern or stale index entries.
pub fn record_invitation_scan_ceiling_breached() {
    counter!("ledger_invitation_scan_ceiling_breached_total").increment(1);
}

// ============================================================================
// Backup
// ============================================================================

const BACKUPS_CREATED_TOTAL: &str = "ledger_backups_created_total";
const BACKUP_LAST_HEIGHT: &str = "ledger_backup_last_height";
const BACKUP_LAST_SIZE_BYTES: &str = "ledger_backup_last_size_bytes";
const BACKUP_FAILURES_TOTAL: &str = "ledger_backup_failures_total";
const BACKUP_PRE_ERASURE_COUNT: &str = "ledger_backup_pre_erasure_count";

/// Records a successful backup: increments the created counter and updates the
/// last-height and last-size gauges.
pub fn record_backup_created(height: u64, size_bytes: u64) {
    #[allow(clippy::cast_precision_loss)]
    {
        counter!(BACKUPS_CREATED_TOTAL).increment(1);
        gauge!(BACKUP_LAST_HEIGHT).set(height as f64);
        gauge!(BACKUP_LAST_SIZE_BYTES).set(size_bytes as f64);
    }
}

/// Increments the backup failure counter.
pub fn record_backup_failed() {
    counter!(BACKUP_FAILURES_TOTAL).increment(1);
}

/// Sets the count of backups created before the most recent erasure event for
/// the given region.
///
/// Operators should monitor this gauge and retire pre-erasure backups within
/// the configured `max_backup_retention_days` window.
pub fn set_backup_pre_erasure_count(region: &str, count: u64) {
    #[allow(clippy::cast_precision_loss)]
    gauge!(BACKUP_PRE_ERASURE_COUNT, fields::REGION => region.to_string()).set(count as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer_elapsed() {
        let timer = Timer::new(|_| {});
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = timer.stop();
        assert!(elapsed >= 0.01);
    }

    #[test]
    fn test_sli_histogram_buckets() {
        // Verify bucket boundaries are sorted and within expected range
        for window in SLI_HISTOGRAM_BUCKETS.windows(2) {
            assert!(window[0] < window[1], "Buckets must be strictly increasing");
        }
        // First bucket is 1ms (p50 read target)
        assert!((SLI_HISTOGRAM_BUCKETS[0] - 0.001).abs() < f64::EPSILON);
        // Last bucket is 10s (timeout boundary)
        assert!((SLI_HISTOGRAM_BUCKETS[10] - 10.0).abs() < f64::EPSILON);
        // 11 buckets total
        assert_eq!(SLI_HISTOGRAM_BUCKETS.len(), 11);
    }

    #[test]
    fn test_background_job_metric_names() {
        // Verify metric name constants follow naming conventions
        assert!(BACKGROUND_JOB_DURATION_SECONDS.starts_with("ledger_"));
        assert!(BACKGROUND_JOB_DURATION_SECONDS.ends_with("_seconds"));
        assert!(BACKGROUND_JOB_RUNS_TOTAL.starts_with("ledger_"));
        assert!(BACKGROUND_JOB_RUNS_TOTAL.ends_with("_total"));
        assert!(BACKGROUND_JOB_ITEMS_PROCESSED_TOTAL.starts_with("ledger_"));
        assert!(BACKGROUND_JOB_ITEMS_PROCESSED_TOTAL.ends_with("_total"));
        assert!(POST_ERASURE_COMPACTION_TRIGGERED_TOTAL.starts_with("ledger_"));
        assert!(POST_ERASURE_COMPACTION_TRIGGERED_TOTAL.ends_with("_total"));
    }

    #[test]
    fn test_cardinality_overflow_metric_name() {
        assert!(CARDINALITY_OVERFLOW_TOTAL.starts_with("ledger_"));
        assert!(CARDINALITY_OVERFLOW_TOTAL.ends_with("_total"));
    }

    #[test]
    fn test_data_residency_violation_metric_name() {
        assert!(DATA_RESIDENCY_VIOLATION_TOTAL.starts_with("ledger_"));
        assert!(DATA_RESIDENCY_VIOLATION_TOTAL.ends_with("_total"));
    }

    #[test]
    fn test_onboarding_metric_names() {
        assert!(ONBOARDING_REQUESTS_TOTAL.starts_with("ledger_"));
        assert!(ONBOARDING_REQUESTS_TOTAL.ends_with("_total"));
    }

    // =========================================================================
    // Coverage tests for record_* and set_* functions
    // =========================================================================

    // --- Write service ---

    #[test]
    fn test_record_rate_limit_exceeded() {
        record_rate_limit_exceeded("global", "burst");
        record_rate_limit_exceeded("organization", "sustained");
    }

    #[test]
    fn test_record_rate_limit_rejected() {
        record_rate_limit_rejected("global", "burst");
        record_rate_limit_rejected("organization", "sustained");
    }

    // --- Data residency tracking ---

    #[test]
    fn test_record_data_residency_violation() {
        record_data_residency_violation("us-east-va");
    }

    // --- Raft consensus ---

    #[test]
    fn test_record_raft_proposal() {
        record_raft_proposal();
    }

    #[test]
    fn test_record_raft_proposal_timeout() {
        record_raft_proposal_timeout();
    }

    #[test]
    fn test_set_pending_proposals() {
        set_pending_proposals(42);
        set_pending_proposals(0);
    }

    #[test]
    fn test_record_raft_apply_latency() {
        record_raft_apply_latency(0.003);
    }

    #[test]
    fn test_set_raft_commit_index() {
        set_raft_commit_index(100);
    }

    #[test]
    fn test_set_raft_term() {
        set_raft_term(5);
    }

    #[test]
    fn test_set_is_leader_both_paths() {
        set_is_leader(true);
        set_is_leader(false);
    }

    // --- State machine ---

    #[test]
    fn test_record_state_root_computation() {
        record_state_root_computation(VaultId::new(1), 0.002);
        // vault parameter is intentionally ignored by the metric helper
    }

    #[test]
    fn test_record_state_root_verification() {
        record_state_root_verification();
    }

    #[test]
    fn test_record_state_root_divergence() {
        record_state_root_divergence(OrganizationId::new(1), VaultId::new(2));
    }

    #[test]
    fn test_set_dirty_buckets() {
        set_dirty_buckets(VaultId::new(1), 5);
    }

    // --- Storage ---

    #[test]
    fn test_record_storage_write() {
        record_storage_write(4096);
    }

    #[test]
    fn test_record_storage_read() {
        record_storage_read(2048);
    }

    // --- Snapshot ---

    #[test]
    fn test_record_snapshot_created() {
        record_snapshot_created(1_000_000);
    }

    #[test]
    fn test_record_snapshot_restore() {
        record_snapshot_restore(3.0);
    }

    // --- Idempotency cache ---

    #[test]
    fn test_record_idempotency_operation() {
        record_idempotency_operation("hit");
        record_idempotency_operation("miss");
        record_idempotency_operation("eviction");
    }

    #[test]
    fn test_set_idempotency_cache_size() {
        set_idempotency_cache_size(128);
    }

    // --- Connections ---

    #[test]
    fn test_increment_decrement_connections() {
        increment_connections();
        decrement_connections();
    }

    // --- gRPC request ---

    #[test]
    fn test_record_grpc_request() {
        record_grpc_request("WriteService", "write", "OK", 0.01);
        record_grpc_request("ReadService", "read", "Internal", 0.5);
    }

    // --- Batching ---

    #[test]
    fn test_record_batch_coalesce() {
        record_batch_coalesce(5);
    }

    #[test]
    fn test_record_batch_flush() {
        record_batch_flush(0.002);
    }

    // --- Recovery ---

    #[test]
    fn test_record_recovery_success() {
        record_recovery_success(OrganizationId::new(1), VaultId::new(2));
        // org and vault params are ignored by the metric helper
    }

    #[test]
    fn test_record_recovery_failure() {
        record_recovery_failure(OrganizationId::new(1), VaultId::new(2), "snapshot_mismatch");
    }

    #[test]
    fn test_record_determinism_bug() {
        record_determinism_bug(OrganizationId::new(1), VaultId::new(2));
        // org and vault params are ignored by the metric helper
    }

    #[test]
    fn test_record_recovery_attempt() {
        record_recovery_attempt(OrganizationId::new(1), VaultId::new(2), 1, "success");
        record_recovery_attempt(OrganizationId::new(1), VaultId::new(2), 3, "failure");
        // org, vault, and attempt params are ignored by the metric helper
    }

    #[test]
    fn test_set_vault_health_all_states() {
        let org = OrganizationId::new(1);
        let vault = VaultId::new(2);
        set_vault_health(org, vault, "healthy");
        set_vault_health(org, vault, "diverged");
        set_vault_health(org, vault, "recovering");
        // Unknown fallback
        set_vault_health(org, vault, "something_else");
        // org and vault params are ignored by the metric helper
    }

    // --- Learner refresh ---

    #[test]
    fn test_record_learner_refresh_both_paths() {
        record_learner_refresh(true, 0.01);
        record_learner_refresh(false, 0.5);
    }

    #[test]
    fn test_record_learner_cache_stale() {
        record_learner_cache_stale();
    }

    #[test]
    fn test_record_learner_voter_error() {
        record_learner_voter_error(42, "connection_refused");
    }

    // --- B+ tree compaction ---

    #[test]
    fn test_record_btree_compaction() {
        record_btree_compaction(10, 5);
    }

    // --- State-DB Checkpointer ---

    #[test]
    fn test_record_state_checkpoint_ok_paths() {
        record_state_checkpoint("us-east-va", "time", "ok", 0.003);
        record_state_checkpoint("us-east-va", "applies", "ok", 0.012);
        record_state_checkpoint("us-east-va", "dirty", "ok", 0.025);
    }

    #[test]
    fn test_record_state_checkpoint_error_path() {
        record_state_checkpoint("global", "time", "error", 0.5);
    }

    #[test]
    fn test_set_state_checkpoint_gauges() {
        set_state_applies_since_checkpoint("global", 1234);
        set_state_dirty_pages("global", 42);
        set_state_page_cache_len("global", 999);
        set_state_last_synced_snapshot_id("global", 5);
        set_state_checkpoint_last_timestamp("global", 1_700_000_000.0);
    }

    // --- Handler-phase event flusher ---

    #[test]
    fn test_record_event_flush_triggers() {
        record_event_flush("global", "time", 0.002, 5);
        record_event_flush("global", "size", 0.012, 500);
        record_event_flush("global", "shutdown", 0.050, 1_000);
    }

    #[test]
    fn test_record_event_flush_failure() {
        record_event_flush_failure("global");
    }

    #[test]
    fn test_set_event_flush_queue_depth() {
        set_event_flush_queue_depth("global", 0);
        set_event_flush_queue_depth("global", 1_234);
    }

    #[test]
    fn test_record_event_overflow_all_causes() {
        record_event_overflow("global", "queue_full", 1);
        record_event_overflow("global", "shutdown_timeout", 42);
        record_event_overflow("global", "channel_closed", 1);
    }

    #[test]
    fn test_record_event_overflow_zero_count_is_noop() {
        record_event_overflow("global", "queue_full", 0);
    }

    #[test]
    fn test_record_btree_operation() {
        record_btree_operation("merge");
        record_btree_operation("free");
        record_btree_operation("split");
    }

    // --- Post-erasure compaction ---

    #[test]
    fn test_record_post_erasure_compaction_triggered() {
        record_post_erasure_compaction_triggered("us-east-va");
    }

    // --- Organization purge ---

    #[test]
    fn test_record_org_purge_failure() {
        record_org_purge_failure("global", false);
        record_org_purge_failure("global", true);
        record_org_purge_failure("regional", false);
        record_org_purge_failure("regional", true);
    }

    // --- Hot key detection ---

    #[test]
    fn test_record_hot_key_detected() {
        record_hot_key_detected(VaultId::new(1), "users:123", 500.0);
        // vault, key, and ops_per_sec params are ignored by the metric helper
    }

    // --- SLI/SLO ---

    #[test]
    fn test_set_batch_queue_depth() {
        set_batch_queue_depth(10, "us-east-va");
    }

    #[test]
    fn test_set_rate_limit_queue_depth() {
        set_rate_limit_queue_depth(5, "global");
    }

    #[test]
    fn test_set_cluster_quorum_status_both_paths() {
        set_cluster_quorum_status(true);
        set_cluster_quorum_status(false);
    }

    #[test]
    fn test_record_leader_election() {
        record_leader_election();
    }

    // --- Resource saturation ---

    #[test]
    fn test_set_disk_bytes() {
        set_disk_bytes(1_000_000, 500_000, "us-east-va");
    }

    #[test]
    fn test_set_page_cache_metrics() {
        set_page_cache_metrics(1000, 50, 512, "global");
    }

    #[test]
    fn test_set_btree_depth() {
        set_btree_depth("entities", 4, "us-east-va");
    }

    #[test]
    fn test_set_btree_page_splits() {
        set_btree_page_splits(25, "global");
    }

    #[test]
    fn test_set_compaction_lag_blocks() {
        set_compaction_lag_blocks(100, "us-east-va");
    }

    #[test]
    fn test_set_snapshot_disk_bytes() {
        set_snapshot_disk_bytes(50_000_000, "global");
    }

    // --- Organization resource accounting ---

    #[test]
    fn test_set_organization_storage_bytes() {
        set_organization_storage_bytes(OrganizationId::new(1), 1_000_000);
        // org param is ignored by the metric helper
    }

    #[test]
    fn test_record_organization_operation() {
        record_organization_operation(OrganizationId::new(1), "write");
        record_organization_operation(OrganizationId::new(1), "read");
        // org param is ignored by the metric helper
    }

    #[test]
    fn test_record_organization_latency() {
        record_organization_latency(OrganizationId::new(1), "write", 0.01);
        // org param is ignored by the metric helper
    }

    // --- Background jobs ---

    #[test]
    fn test_record_background_job_duration() {
        record_background_job_duration("gc", 1.5);
    }

    #[test]
    fn test_record_background_job_run() {
        record_background_job_run("compaction", "success");
        record_background_job_run("compaction", "failure");
    }

    #[test]
    fn test_record_background_job_items() {
        record_background_job_items("integrity_scrub", 200);
    }

    // --- DEK re-wrapping ---

    #[test]
    fn test_record_rewrap_pages() {
        record_rewrap_pages(100);
    }

    #[test]
    fn test_record_rewrap_remaining() {
        record_rewrap_remaining(500);
    }

    // --- Cardinality overflow ---

    #[test]
    fn test_record_cardinality_overflow() {
        record_cardinality_overflow("ledger_writes_total");
    }

    // --- Events ingestion ---

    #[test]
    fn test_record_events_ingest() {
        record_events_ingest("engine", "accepted", 10);
        record_events_ingest("control", "rejected", 2);
    }

    #[test]
    fn test_record_events_ingest_batch_size() {
        record_events_ingest_batch_size("engine", 50);
    }

    #[test]
    fn test_record_events_ingest_rate_limited() {
        record_events_ingest_rate_limited("control");
    }

    #[test]
    fn test_record_events_ingest_duration() {
        record_events_ingest_duration(0.015);
    }

    // --- Event writes ---

    #[test]
    fn test_record_event_write() {
        record_event_write("apply_phase", "system", "user_created");
        record_event_write("handler_phase", "organization", "entity_updated");
    }

    // --- Timer factories ---

    #[test]
    fn test_raft_apply_timer() {
        let timer = raft_apply_timer();
        drop(timer);
    }

    // --- Integrity scrubber ---

    #[test]
    fn test_record_integrity_pages_checked() {
        record_integrity_pages_checked(1000);
    }

    #[test]
    fn test_record_integrity_errors() {
        record_integrity_errors("checksum", 2);
        record_integrity_errors("structural", 1);
    }

    // --- Events GC --- (covered by ledger_background_job_* via JobContext)

    // --- Leader transfer ---

    #[test]
    fn test_record_leader_transfer_both_paths() {
        record_leader_transfer(true, 0.1);
        record_leader_transfer(false, 5.0);
    }

    #[test]
    fn test_record_trigger_election_both_paths() {
        record_trigger_election(true);
        record_trigger_election(false);
    }

    // --- Onboarding ---

    #[test]
    fn test_record_onboarding_request() {
        record_onboarding_request("initiate", "success");
        record_onboarding_request("initiate", "failure");
        record_onboarding_request("verify", "success");
        record_onboarding_request("verify", "failure");
        record_onboarding_request("register", "success");
        record_onboarding_request("register", "failure");
    }
}
