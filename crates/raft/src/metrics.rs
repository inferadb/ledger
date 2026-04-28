//! Observability metrics exposed via Prometheus using the `metrics` crate.
//!
//! ## Metric Naming Conventions
//!
//! Metrics use the `ledger_` or `inferadb_ledger_raft_` prefix.
//!
//! - Counters: `_total` suffix
//! - Histograms: `_seconds` or `_bytes` suffix
//! - Gauges: no suffix

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

use inferadb_ledger_types::{OrganizationId, VaultId};
use metrics::{counter, gauge, histogram};

use crate::logging::fields;

// =============================================================================
// Per-Vault Metric Gating
// =============================================================================

/// Global gate for per-vault Prometheus label emission.
///
/// When `false` (default), helpers that emit `vault_id`-labeled series
/// fast-path to org-level rollups only. When `true`, the per-vault series
/// are emitted in addition to the rollups. Set once at startup from
/// [`ObservabilityConfig::vault_metrics_enabled`](inferadb_ledger_types::config::ObservabilityConfig);
/// flipping it at runtime would change the time-series shape mid-scrape,
/// confusing dashboards.
static VAULT_METRICS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Sentinel value emitted in place of an internal vault id when per-vault
/// metrics are disabled. Keeps dashboards stable across opt-in changes —
/// a panel that groups by `vault_id` always has a value.
pub const VAULT_ID_ROLLUP_LABEL: &str = "ROLLUP";

/// Sets the per-vault metric emission gate.
///
/// Call once during server bootstrap, before any metric is emitted. Reading
/// the gate is a single relaxed atomic load on the metric hot path.
#[inline]
pub fn set_vault_metrics_enabled(enabled: bool) {
    VAULT_METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Returns `true` if per-vault Prometheus series should be emitted.
#[inline]
pub fn vault_metrics_enabled() -> bool {
    VAULT_METRICS_ENABLED.load(Ordering::Relaxed)
}

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

// Apply-worker metrics — per `(region, organization_id)`.
//
// The apply worker is the per-OrganizationGroup sink that drains committed
// Raft batches into `StateLayer::apply_operations`. It is the ceiling on
// per-organization write throughput — measuring it is how the per-
// organization scaling hypothesis (N organizations × single-org ceiling ≈
// per-node throughput) becomes testable.
const APPLY_BATCHES_TOTAL: &str = "inferadb_ledger_raft_apply_batches_total";
const APPLY_BATCH_LATENCY: &str = "inferadb_ledger_raft_apply_batch_latency_seconds";
const APPLY_BATCH_SIZE: &str = "inferadb_ledger_raft_apply_batch_size";
const APPLY_ENTRIES_TOTAL: &str = "inferadb_ledger_raft_apply_entries_total";

// Apply-pipeline phase-level latency histograms.
//
// `APPLY_BATCH_LATENCY` above reports end-to-end apply-batch time. These
// per-phase histograms break down where that time is spent, so
// parallelism / pipelining work can target the dominant phase instead of
// optimising speculatively. Labelled by `(region, organization_id, phase)`
// so you can slice per-org to see which tier dominates.
const APPLY_PHASE_LATENCY: &str = "inferadb_ledger_raft_apply_phase_latency_seconds";

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

/// Records a single apply-worker batch.
///
/// Every committed Raft batch drained by the per-OrganizationGroup
/// `ApplyWorker` fires this exactly once. Labels cover the full
/// `(region, organization_id)` matrix plus a `status` of `"ok"` or
/// `"error"` so dashboards can alert on elevated apply-error rates
/// without losing the healthy cadence.
///
/// `batch_size` is the number of entries in the batch; the paired
/// histogram captures distribution so we can see whether Raft is
/// delivering large batches (good — WAL fsync amortized) or dribbles
/// (bad — fsync-per-batch dominates).
///
/// `latency_secs` is the end-to-end apply duration for the batch,
/// including `StateLayer::apply_operations`, response-map delivery, and
/// spillover insertion. A node's sustainable write rate for one
/// organization is `batch_size / latency`; summed across active
/// organizations it is the per-node ceiling, so the p50/p99 of this
/// histogram is the headline scaling number.
#[inline]
pub fn record_apply_batch(
    region: &str,
    organization_id: &str,
    status: &str,
    batch_size: usize,
    latency_secs: f64,
) {
    gated!(
        APPLY_BATCHES_TOTAL,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::STATUS, status),
        ],
        {
            counter!(
                APPLY_BATCHES_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::STATUS => status.to_string(),
            )
            .increment(1);
            counter!(
                APPLY_ENTRIES_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::STATUS => status.to_string(),
            )
            .increment(batch_size as u64);
            histogram!(
                APPLY_BATCH_SIZE,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .record(batch_size as f64);
            histogram!(
                APPLY_BATCH_LATENCY,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::STATUS => status.to_string(),
            )
            .record(latency_secs);
        }
    );
}

/// Records a per-phase latency within a single `apply_committed_entries`
/// call. `phase` should be a small fixed-cardinality label — see
/// [`ApplyPhase`] for the canonical set.
#[inline]
pub fn record_apply_phase(
    region: &str,
    organization_id: &str,
    phase: ApplyPhase,
    latency_secs: f64,
) {
    gated!(
        APPLY_PHASE_LATENCY,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            ("phase", phase.as_str()),
        ],
        {
            histogram!(
                APPLY_PHASE_LATENCY,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                "phase" => phase.as_str(),
            )
            .record(latency_secs);
        }
    );
}

/// Phases within a single `apply_committed_entries` call. The set is
/// intentionally small and fixed so Prometheus label cardinality stays
/// bounded (`#phases × #(region, org_id)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyPhase {
    /// Pre-decode of every `CommittedEntry`'s payload bytes into
    /// `RaftPayload<R>`. Single serial pass, parallelisable — low
    /// priority unless proven dominant.
    Decode,
    /// The main serial apply loop. Each entry calls `R::apply_on` against
    /// `AppliedState` and `StateLayer`. Raft-ordered, cannot be
    /// parallelised without intra-batch vault-group sharding.
    ApplyLoop,
    /// Per-unique-vault `StateLayer::compute_state_root`. Already
    /// parallelised across vaults via the bounded apply rayon pool.
    StateRoot,
    /// Per-`VaultEntry` block hash. Already parallelised.
    BlockHash,
    /// `BlockArchive::append_block` — page-cache commit to `blocks.db` +
    /// buffered segment-file append. No fsync on the apply path.
    BlockArchive,
    /// `block_announcements.send()` fan-out. Non-blocking broadcast
    /// channel send.
    Broadcast,
    /// Response delivery — partitioning into waiter-send vs spillover
    /// and the oneshot sends themselves.
    ResponseFanout,
}

impl ApplyPhase {
    /// Prometheus label value for this phase. Must stay ASCII / lowercase.
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::ApplyLoop => "apply_loop",
            Self::StateRoot => "state_root",
            Self::BlockHash => "block_hash",
            Self::BlockArchive => "block_archive",
            Self::Broadcast => "broadcast",
            Self::ResponseFanout => "response_fanout",
        }
    }
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
///
/// `region` and `organization_id` locate the originating BatchWriter so
/// dashboards can split coalesce volume across the
/// `(region, organization_id)` matrix. The data-region group's BatchWriter
/// emits `organization_id = "0"`; per-organization groups emit the new
/// organization's id.
#[inline]
pub fn record_batch_coalesce(size: usize, region: &str, organization_id: &str) {
    gated!(
        BATCH_COALESCE_TOTAL,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            counter!(
                BATCH_COALESCE_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .increment(1);
            histogram!(
                BATCH_COALESCE_SIZE,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .record(size as f64);
        }
    );
}

/// Records batch flush latency.
///
/// See [`record_batch_coalesce`] for the role of `region` and
/// `organization_id`.
#[inline]
pub fn record_batch_flush(latency_secs: f64, region: &str, organization_id: &str) {
    gated!(
        BATCH_FLUSH_LATENCY,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            histogram!(
                BATCH_FLUSH_LATENCY,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .record(latency_secs);
        }
    );
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

// ─── Vault Membership Cascade (Phase 5 / M5) ──────────────────

/// Vault conf-change stalled counter (Phase 5 / M5).
///
/// Incremented every time the
/// [`MembershipDispatcher`](crate::region_membership_watcher::MembershipDispatcher)
/// fires a per-vault conf-change and the call does not complete inside
/// the configured timeout (default 60s, tunable via
/// [`RaftManagerConfig::vault_conf_change_timeout_secs`](crate::raft_manager::RaftManagerConfig)).
/// A non-zero value means the vault's Raft proposal pipeline is
/// stalled — the cascade has been dropped to keep the dispatcher
/// moving, and DR replica state may be inconsistent until the next
/// region-state delta re-derives the change.
///
/// Labels:
/// * `region` — region name (e.g. `"us-west"`).
/// * `organization_id` — internal organization id (stringified `i64`).
/// * `vault_id` — internal vault id (stringified `i64`).
/// * `action` — `"AddLearner"` | `"PromoteVoter"` | `"Remove"`.
/// * `reason` — currently always `"timeout"`. Reserved label for future stall causes (e.g.
///   `"transport_error"`).
const VAULT_CONF_CHANGE_STALLED_TOTAL: &str = "ledger_vault_conf_change_stalled_total";

/// Records a stalled per-vault membership conf-change.
///
/// Called from
/// [`MembershipDispatcher`](crate::region_membership_watcher::MembershipDispatcher)
/// after the per-vault conf-change RPC exceeds the configured
/// timeout.
///
/// Labels:
/// * `region` — region name (e.g. `"us-west"`).
/// * `organization_id` — internal organization id (stringified `i64`).
/// * `vault_id` — internal vault id (stringified `i64`).
/// * `action` — `"AddLearner"` | `"PromoteVoter"` | `"Remove"`.
/// * `reason` — currently always `"timeout"`. Reserved for future stall causes.
///
/// `ledger_vault_conf_change_stalled_total{region, organization_id, vault_id, action, reason}`.
pub fn record_vault_conf_change_stalled(
    region: &str,
    organization_id: &str,
    vault_id: &str,
    action: &str,
    reason: &str,
) {
    gated!(
        VAULT_CONF_CHANGE_STALLED_TOTAL,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::VAULT_ID, vault_id),
            (fields::ACTION, action),
            (fields::REASON, reason),
        ],
        {
            counter!(
                VAULT_CONF_CHANGE_STALLED_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::VAULT_ID => vault_id.to_string(),
                fields::ACTION => action.to_string(),
                fields::REASON => reason.to_string()
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
pub fn set_batch_queue_depth(depth: usize, region: &str, organization_id: &str) {
    gated!(
        BATCH_QUEUE_DEPTH,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            gauge!(
                BATCH_QUEUE_DEPTH,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .set(depth as f64);
        }
    );
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
/// (emitted by their owning code paths).
/// `status` is `"ok"` or `"error"`.
/// `organization_id` identifies the OrganizationGroup within `region`;
/// the data-region group emits `"0"`, per-organization groups emit the
/// organization's id. Dashboards track checkpoint cadence per
/// `(region, organization_id)`.
#[inline]
pub fn record_state_checkpoint(
    region: &str,
    organization_id: &str,
    trigger: &str,
    status: &str,
    duration_secs: f64,
) {
    gated!(
        LEDGER_STATE_CHECKPOINTS_TOTAL,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::TRIGGER, trigger),
            (fields::STATUS, status),
        ],
        {
            counter!(
                LEDGER_STATE_CHECKPOINTS_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::TRIGGER => trigger.to_string(),
                fields::STATUS => status.to_string(),
            )
            .increment(1);
        }
    );
    gated!(
        LEDGER_STATE_CHECKPOINT_DURATION_SECONDS,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::TRIGGER, trigger),
        ],
        {
            histogram!(
                LEDGER_STATE_CHECKPOINT_DURATION_SECONDS,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::TRIGGER => trigger.to_string(),
            )
            .record(duration_secs);
        }
    );
}

/// Updates the applies-since-last-checkpoint gauge.
///
/// See [`record_state_checkpoint`] for the role of `organization_id`.
#[inline]
pub fn set_state_applies_since_checkpoint(region: &str, organization_id: &str, applies: u64) {
    gated!(
        LEDGER_STATE_APPLIES_SINCE_CHECKPOINT,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            gauge!(
                LEDGER_STATE_APPLIES_SINCE_CHECKPOINT,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .set(applies as f64);
        }
    );
}

/// Updates the dirty-pages gauge sampled at checkpointer wake-ups.
///
/// See [`record_state_checkpoint`] for the role of `organization_id`.
#[inline]
pub fn set_state_dirty_pages(region: &str, organization_id: &str, dirty_pages: u64) {
    gated!(
        LEDGER_STATE_DIRTY_PAGES,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            gauge!(
                LEDGER_STATE_DIRTY_PAGES,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .set(dirty_pages as f64);
        }
    );
}

/// Updates the total page-cache-size gauge.
///
/// See [`record_state_checkpoint`] for the role of `organization_id`.
#[inline]
pub fn set_state_page_cache_len(region: &str, organization_id: &str, cache_len: u64) {
    gated!(
        LEDGER_STATE_PAGE_CACHE_LEN,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            gauge!(
                LEDGER_STATE_PAGE_CACHE_LEN,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .set(cache_len as f64);
        }
    );
}

/// Updates the last-synced-snapshot-id gauge.
///
/// See [`record_state_checkpoint`] for the role of `organization_id`.
#[inline]
pub fn set_state_last_synced_snapshot_id(region: &str, organization_id: &str, snapshot_id: u64) {
    gated!(
        LEDGER_STATE_LAST_SYNCED_SNAPSHOT_ID,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            gauge!(
                LEDGER_STATE_LAST_SYNCED_SNAPSHOT_ID,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .set(snapshot_id as f64);
        }
    );
}

/// Updates the last-checkpoint-timestamp gauge (Unix seconds).
///
/// See [`record_state_checkpoint`] for the role of `organization_id`.
#[inline]
pub fn set_state_checkpoint_last_timestamp(region: &str, organization_id: &str, unix_secs: f64) {
    gated!(
        LEDGER_STATE_CHECKPOINT_LAST_TIMESTAMP_SECONDS,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            gauge!(
                LEDGER_STATE_CHECKPOINT_LAST_TIMESTAMP_SECONDS,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .set(unix_secs);
        }
    );
}

/// Records the number of WAL entries replayed by `RaftLogStore::replay_crash_gap`.
///
/// Called exactly once per region on startup, with `count = 0` on clean
/// shutdowns. Surfacing zero-count recoveries is deliberate: dashboards can
/// plot `rate()` to see restarts per deploy.
#[inline]
pub fn record_state_recovery_replay(region: &str, organization_id: &str, count: u64) {
    gated!(
        LEDGER_STATE_RECOVERY_REPLAY_COUNT_TOTAL,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            counter!(
                LEDGER_STATE_RECOVERY_REPLAY_COUNT_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .increment(count);
        }
    );
}

/// Records the duration of the post-open crash-recovery sweep.
///
/// Fires unconditionally — even a zero-entry recovery did some work
/// (reading the last-applied sentinel + coalescing a no-op sync). Samples
/// land in `SLI_HISTOGRAM_BUCKETS`; pathological long recoveries alert on p99.
#[inline]
pub fn record_state_recovery_duration(
    region: &str,
    organization_id: &str,
    duration: std::time::Duration,
) {
    gated!(
        LEDGER_STATE_RECOVERY_DURATION_SECONDS,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            histogram!(
                LEDGER_STATE_RECOVERY_DURATION_SECONDS,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .record(duration.as_secs_f64());
        }
    );
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
pub fn record_event_flush(
    region: &str,
    organization_id: &str,
    trigger: &str,
    duration_secs: f64,
    entries: u64,
) {
    gated!(
        LEDGER_EVENT_FLUSH_TRIGGERS_TOTAL,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::TRIGGER, trigger),
        ],
        {
            counter!(
                LEDGER_EVENT_FLUSH_TRIGGERS_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::TRIGGER => trigger.to_string(),
            )
            .increment(1);
        }
    );
    gated!(
        LEDGER_EVENT_FLUSH_DURATION_SECONDS,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            histogram!(
                LEDGER_EVENT_FLUSH_DURATION_SECONDS,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .record(duration_secs);
        }
    );
    gated!(
        LEDGER_EVENT_FLUSH_ENTRIES_PER_FLUSH,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            histogram!(
                LEDGER_EVENT_FLUSH_ENTRIES_PER_FLUSH,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .record(entries as f64);
        }
    );
}

/// Records a flush failure (events.db commit / write error).
#[inline]
pub fn record_event_flush_failure(region: &str, organization_id: &str) {
    gated!(
        LEDGER_EVENT_FLUSH_FAILURES_TOTAL,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            counter!(
                LEDGER_EVENT_FLUSH_FAILURES_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .increment(1);
        }
    );
}

/// Updates the pre-drain queue-depth gauge sampled at flush entry.
#[inline]
pub fn set_event_flush_queue_depth(region: &str, organization_id: &str, depth: u64) {
    gated!(
        LEDGER_EVENT_FLUSH_QUEUE_DEPTH,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            gauge!(
                LEDGER_EVENT_FLUSH_QUEUE_DEPTH,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .set(depth as f64);
        }
    );
}

/// Records a handler-phase event overflow.
///
/// `cause` is one of `"queue_full"` (producer tried to enqueue on a full
/// channel under `overflow_behavior = Drop`), `"shutdown_timeout"`
/// (flush-for-shutdown exited with queued entries remaining), or
/// `"channel_closed"` (bug signal — producer saw a closed channel).
#[inline]
pub fn record_event_overflow(region: &str, organization_id: &str, cause: &str, count: u64) {
    if count == 0 {
        return;
    }
    gated!(
        LEDGER_EVENT_OVERFLOW_TOTAL,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::CAUSE, cause),
        ],
        {
            counter!(
                LEDGER_EVENT_OVERFLOW_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::CAUSE => cause.to_string(),
            )
            .increment(count);
        }
    );
}

// =============================================================================
// Per-Vault Metrics (Phase 7 — opt-in via ObservabilityConfig::vault_metrics_enabled)
// =============================================================================
//
// These helpers always emit org-level rollups and conditionally emit
// per-vault series gated on `vault_metrics_enabled()`. Defaulting per-vault
// off keeps Prometheus cardinality bounded by `(region × organization)`
// instead of `(region × organization × vault)`.

/// Per-vault apply latency histogram (opt-in).
///
/// Labels: `region`, `organization_id`, `vault_id`, `status`.
const VAULT_APPLY_LATENCY: &str = "vault_apply_latency_seconds";

/// Per-vault state-root cache hits (opt-in).
///
/// Labels: `region`, `organization_id`, `vault_id`. Incremented when a
/// `compute_state_root` call short-circuits on a clean commitment instead
/// of recomputing bucket roots.
const VAULT_STATE_ROOT_CACHE_HIT_TOTAL: &str = "vault_state_root_cache_hit_total";

/// Per-vault gRPC request counter (opt-in).
///
/// Labels: `region`, `organization_id`, `vault_id`, `method`, `status`.
const VAULT_REQUEST_TOTAL: &str = "vault_request_total";

/// Per-organization apply throughput counter (always-on rollup).
///
/// Labels: `region`, `organization_id`. Sum across all per-vault apply
/// batches under the organization. Operators can divide by the
/// per-vault counter (when opt-in is on) to derive vault-level shares
/// without a high-cardinality time series for the rollup itself.
const ORG_APPLY_THROUGHPUT_OPS_TOTAL: &str = "org_apply_throughput_ops_total";

/// Per-organization active vault count gauge (always-on rollup).
///
/// Labels: `region`, `organization_id`, `status` ∈ {`active`, `dormant`,
/// `stalled`}. Today only `active` is populated — `dormant` and `stalled`
/// are reserved for the hibernation milestone (Phase 7 / O1).
const ORG_ACTIVE_VAULT_COUNT: &str = "org_active_vault_count";

/// Vault hibernation transition counter (always-on rollup).
///
/// Labels: `region`, `organization_id`, `direction` ∈ {`wake`, `sleep`}.
/// Reserved for the hibernation milestone (Phase 7 / O1) — the metric is
/// registered ahead of time so dashboards do not break when O1 ships.
const VAULT_HIBERNATION_TRANSITIONS_TOTAL: &str = "vault_hibernation_transitions_total";

/// Vault hibernation: number of completed sleeps (active → dormant) that
/// drove the page-cache eviction path. Always-on rollup, labelled by
/// `region`, `organization_id`. Counts vaults sent to sleep, not page-cache
/// eviction calls — eviction is best-effort and may be a no-op on
/// platforms without `posix_fadvise`.
const VAULT_HIBERNATION_EVICTED_TOTAL: &str = "vault_hibernation_evicted_total";

/// Vault hibernation: number of completed wakes (dormant → active) issued
/// by [`crate::raft_manager::RaftManager::wake_vault`]. Always-on rollup,
/// labelled by `region`, `organization_id`.
const VAULT_HIBERNATION_WOKEN_TOTAL: &str = "vault_hibernation_woken_total";

/// Vault hibernation: histogram of wake-side latency in seconds. Measured
/// from `wake_vault` entry to return; covers `engine.resume_shard` round-
/// trip plus the lifecycle transition. Bucketed via [`SLI_HISTOGRAM_BUCKETS`]
/// (p99 budget: < 100ms). Always-on rollup, labelled by `region`,
/// `organization_id`.
const VAULT_HIBERNATION_WAKE_DURATION_SECONDS: &str = "vault_hibernation_wake_duration_seconds";

/// Vault hibernation: counter of paused-shard peer messages whose
/// `ConsensusStateId` had no entry in [`crate::raft_manager::RaftManager`]'s
/// `vault_shard_index` reverse map. Always-on rollup with no labels —
/// the per-shard label would explode cardinality, and the counter's
/// purpose is "is the notifier ever firing without a registered vault?"
/// not "which shard". A non-zero value means a peer is replicating to a
/// shard that was unregistered between `start_vault_group` and the wake
/// event landing.
const VAULT_HIBERNATION_WAKE_UNMATCHED_TOTAL: &str = "vault_hibernation_wake_unmatched_total";

/// Returns the `vault_id` label value to emit for the given vault, taking
/// the global per-vault gate into account.
#[cfg(test)]
fn vault_label_value(vault: VaultId) -> String {
    if vault_metrics_enabled() {
        vault.value().to_string()
    } else {
        VAULT_ID_ROLLUP_LABEL.to_string()
    }
}

/// Records a per-vault apply batch.
///
/// Always increments the org-level rollup counter
/// `org_apply_throughput_ops_total`. When per-vault metrics are enabled,
/// also records the vault-labeled apply latency histogram
/// `vault_apply_latency_seconds` so operators can identify hot or
/// stalled vaults.
///
/// `vault` is the internal [`VaultId`] of the per-vault Raft group whose
/// apply pump just drained a batch; if the apply worker drives an
/// org-scoped group (no vault), pass `None` and the rollup is the only
/// emission.
#[inline]
pub fn record_vault_apply_batch(
    region: &str,
    organization_id: &str,
    vault: Option<VaultId>,
    status: &str,
    batch_size: usize,
    latency_secs: f64,
) {
    // Always-on org rollup. Cardinality: region × organization.
    gated!(
        ORG_APPLY_THROUGHPUT_OPS_TOTAL,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            counter!(
                ORG_APPLY_THROUGHPUT_OPS_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .increment(batch_size as u64);
        }
    );

    // Opt-in per-vault histogram. Skip emission entirely when off so the
    // series never registers — a hard cardinality cap rather than a
    // sentinel-label emission.
    if !vault_metrics_enabled() {
        return;
    }
    let vault_label =
        vault.map_or_else(|| VAULT_ID_ROLLUP_LABEL.to_string(), |v| v.value().to_string());
    gated!(
        VAULT_APPLY_LATENCY,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::VAULT_ID, &vault_label),
            (fields::STATUS, status),
        ],
        {
            histogram!(
                VAULT_APPLY_LATENCY,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::VAULT_ID => vault_label.clone(),
                fields::STATUS => status.to_string(),
            )
            .record(latency_secs);
        }
    );
}

/// Records a per-vault state-root cache hit.
///
/// Opt-in: emitted only when `vault_metrics_enabled()` is `true`. The
/// org-level cache-hit rate is already covered by
/// [`record_state_root_computation`]; this helper exists so operators
/// who opt in can identify per-vault outliers (e.g. a vault whose
/// commitment is constantly dirty under burst writes).
#[inline]
pub fn record_vault_state_root_cache_hit(region: &str, organization_id: &str, vault: VaultId) {
    if !vault_metrics_enabled() {
        return;
    }
    let vault_label = vault.value().to_string();
    gated!(
        VAULT_STATE_ROOT_CACHE_HIT_TOTAL,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::VAULT_ID, &vault_label),
        ],
        {
            counter!(
                VAULT_STATE_ROOT_CACHE_HIT_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::VAULT_ID => vault_label.clone(),
            )
            .increment(1);
        }
    );
}

/// Records a per-vault gRPC request.
///
/// Opt-in: emitted only when `vault_metrics_enabled()` is `true`. The
/// org-level rollup is provided by [`record_grpc_request`]; this helper
/// adds vault-grain visibility (e.g. for identifying which vault a
/// `Write` storm is targeting).
#[inline]
pub fn record_vault_request(
    region: &str,
    organization_id: &str,
    vault: VaultId,
    method: &str,
    status: &str,
) {
    if !vault_metrics_enabled() {
        return;
    }
    let vault_label = vault.value().to_string();
    gated!(
        VAULT_REQUEST_TOTAL,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::VAULT_ID, &vault_label),
            (fields::METHOD, method),
            (fields::STATUS, status),
        ],
        {
            counter!(
                VAULT_REQUEST_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::VAULT_ID => vault_label.clone(),
                fields::METHOD => method.to_string(),
                fields::STATUS => status.to_string(),
            )
            .increment(1);
        }
    );
}

/// Sets the per-organization active vault count gauge (always-on rollup).
///
/// `status` must be one of `"active"`, `"dormant"`, or `"stalled"`. Today
/// only `"active"` is populated — `"dormant"` and `"stalled"` are
/// reserved for the hibernation milestone (Phase 7 / O1).
#[inline]
pub fn set_org_active_vault_count(region: &str, organization_id: &str, status: &str, count: u64) {
    gated!(
        ORG_ACTIVE_VAULT_COUNT,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::STATUS, status),
        ],
        {
            #[allow(clippy::cast_precision_loss)]
            gauge!(
                ORG_ACTIVE_VAULT_COUNT,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::STATUS => status.to_string(),
            )
            .set(count as f64);
        }
    );
}

/// Records a vault hibernation transition (always-on rollup).
///
/// `direction` must be `"wake"` (dormant → active) or `"sleep"`
/// (active → dormant). Reserved for the hibernation milestone
/// (Phase 7 / O1) — the metric is registered ahead of time so dashboards
/// do not break when O1 ships.
#[inline]
pub fn record_vault_hibernation_transition(region: &str, organization_id: &str, direction: &str) {
    gated!(
        VAULT_HIBERNATION_TRANSITIONS_TOTAL,
        &[
            (fields::REGION, region),
            (fields::ORGANIZATION_ID, organization_id),
            (fields::DIRECTION, direction),
        ],
        {
            counter!(
                VAULT_HIBERNATION_TRANSITIONS_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
                fields::DIRECTION => direction.to_string(),
            )
            .increment(1);
        }
    );
}

/// Records a completed sleep (active → dormant) that drove the page-cache
/// eviction path. Always-on rollup labelled by `region`, `organization_id`.
/// Use [`record_vault_hibernation_transition`] for the broader Active ↔
/// Dormant ↔ Stalled transitions; this counter is specifically the
/// observability surface for the O6 Pass 2 sleep_vault path.
#[inline]
pub fn record_vault_hibernation_evicted(region: &str, organization_id: &str) {
    gated!(
        VAULT_HIBERNATION_EVICTED_TOTAL,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            counter!(
                VAULT_HIBERNATION_EVICTED_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .increment(1);
        }
    );
}

/// Records a completed wake (dormant → active) issued by
/// [`crate::raft_manager::RaftManager::wake_vault`] together with its
/// observed latency in seconds. The histogram uses [`SLI_HISTOGRAM_BUCKETS`]
/// so dashboards share buckets with the rest of the SLO surface.
#[inline]
pub fn record_vault_hibernation_woken(region: &str, organization_id: &str, latency_secs: f64) {
    gated!(
        VAULT_HIBERNATION_WOKEN_TOTAL,
        &[(fields::REGION, region), (fields::ORGANIZATION_ID, organization_id)],
        {
            counter!(
                VAULT_HIBERNATION_WOKEN_TOTAL,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .increment(1);
            histogram!(
                VAULT_HIBERNATION_WAKE_DURATION_SECONDS,
                fields::REGION => region.to_string(),
                fields::ORGANIZATION_ID => organization_id.to_string(),
            )
            .record(latency_secs);
        }
    );
}

/// Records a paused-shard peer-message wake whose `ConsensusStateId` was
/// not present in
/// [`crate::raft_manager::RaftManager`]'s `vault_shard_index`. No labels —
/// the metric's purpose is "is this happening?" not "which shard". The
/// `_shard_id` is taken so the call site can pass the value verbatim
/// without separate logging plumbing.
#[inline]
pub fn record_vault_hibernation_wake_unmatched(
    _shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
) {
    gated!(VAULT_HIBERNATION_WAKE_UNMATCHED_TOTAL, &[], {
        counter!(VAULT_HIBERNATION_WAKE_UNMATCHED_TOTAL).increment(1);
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
const RESTORE_TRASH_SWEPT_TOTAL: &str = "ledger_restore_trash_swept_total";
const RESTORE_TRASH_SWEEP_FAILURES_TOTAL: &str = "ledger_restore_trash_sweep_failures_total";

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

/// Increments the counter for restore-trash entries swept by the periodic
/// background job.
///
/// `count` is the number of timestamped trash directories removed in the most
/// recent sweep cycle.
pub fn record_restore_trash_swept(count: u64) {
    counter!(RESTORE_TRASH_SWEPT_TOTAL).increment(count);
}

/// Increments the counter for restore-trash sweep failures.
///
/// One increment per failed sweep cycle.
pub fn record_restore_trash_sweep_failed() {
    counter!(RESTORE_TRASH_SWEEP_FAILURES_TOTAL).increment(1);
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
        record_batch_coalesce(5, "us-east-va", "0");
    }

    #[test]
    fn test_record_batch_flush() {
        record_batch_flush(0.002, "us-east-va", "0");
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
        record_state_checkpoint("us-east-va", "0", "time", "ok", 0.003);
        record_state_checkpoint("us-east-va", "0", "applies", "ok", 0.012);
        record_state_checkpoint("us-east-va", "0", "dirty", "ok", 0.025);
    }

    #[test]
    fn test_record_state_checkpoint_error_path() {
        record_state_checkpoint("global", "0", "time", "error", 0.5);
    }

    #[test]
    fn test_set_state_checkpoint_gauges() {
        set_state_applies_since_checkpoint("global", "0", 1234);
        set_state_dirty_pages("global", "0", 42);
        set_state_page_cache_len("global", "0", 999);
        set_state_last_synced_snapshot_id("global", "0", 5);
        set_state_checkpoint_last_timestamp("global", "0", 1_700_000_000.0);
    }

    // --- Handler-phase event flusher ---

    #[test]
    fn test_record_event_flush_triggers() {
        record_event_flush("global", "0", "time", 0.002, 5);
        record_event_flush("global", "0", "size", 0.012, 500);
        record_event_flush("global", "0", "shutdown", 0.050, 1_000);
    }

    #[test]
    fn test_record_event_flush_failure() {
        record_event_flush_failure("global", "0");
    }

    #[test]
    fn test_set_event_flush_queue_depth() {
        set_event_flush_queue_depth("global", "0", 0);
        set_event_flush_queue_depth("global", "0", 1_234);
    }

    #[test]
    fn test_record_event_overflow_all_causes() {
        record_event_overflow("global", "0", "queue_full", 1);
        record_event_overflow("global", "0", "shutdown_timeout", 42);
        record_event_overflow("global", "0", "channel_closed", 1);
    }

    #[test]
    fn test_record_event_overflow_zero_count_is_noop() {
        record_event_overflow("global", "0", "queue_full", 0);
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
        set_batch_queue_depth(10, "us-east-va", "0");
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

    // --- Per-Vault Metrics (Phase 7 opt-in) ---

    #[test]
    fn test_vault_metrics_gate_default_off() {
        // Fresh process default — gate is off.
        // Use a separate gate read so this test stays valid even when
        // run after other tests have flipped the gate.
        let prior = vault_metrics_enabled();
        set_vault_metrics_enabled(false);
        assert!(!vault_metrics_enabled());
        set_vault_metrics_enabled(prior);
    }

    #[test]
    fn test_vault_metrics_gate_toggle() {
        let prior = vault_metrics_enabled();
        set_vault_metrics_enabled(true);
        assert!(vault_metrics_enabled());
        set_vault_metrics_enabled(false);
        assert!(!vault_metrics_enabled());
        set_vault_metrics_enabled(prior);
    }

    #[test]
    fn test_vault_label_value_respects_gate() {
        let prior = vault_metrics_enabled();
        set_vault_metrics_enabled(false);
        assert_eq!(vault_label_value(VaultId::new(42)), VAULT_ID_ROLLUP_LABEL);
        set_vault_metrics_enabled(true);
        assert_eq!(vault_label_value(VaultId::new(42)), "42");
        set_vault_metrics_enabled(prior);
    }

    #[test]
    fn test_record_vault_apply_batch_off_emits_rollup_only() {
        let prior = vault_metrics_enabled();
        set_vault_metrics_enabled(false);
        record_vault_apply_batch("us-east-va", "1", Some(VaultId::new(7)), "ok", 5, 0.003);
        record_vault_apply_batch("us-east-va", "1", None, "ok", 5, 0.003);
        set_vault_metrics_enabled(prior);
    }

    #[test]
    fn test_record_vault_apply_batch_on_emits_per_vault() {
        let prior = vault_metrics_enabled();
        set_vault_metrics_enabled(true);
        record_vault_apply_batch("us-east-va", "1", Some(VaultId::new(7)), "ok", 5, 0.003);
        record_vault_apply_batch("us-east-va", "1", Some(VaultId::new(8)), "error", 1, 0.5);
        record_vault_apply_batch("us-east-va", "1", None, "ok", 5, 0.003);
        set_vault_metrics_enabled(prior);
    }

    #[test]
    fn test_record_vault_state_root_cache_hit_off_is_noop() {
        let prior = vault_metrics_enabled();
        set_vault_metrics_enabled(false);
        record_vault_state_root_cache_hit("us-east-va", "1", VaultId::new(7));
        set_vault_metrics_enabled(prior);
    }

    #[test]
    fn test_record_vault_state_root_cache_hit_on_emits() {
        let prior = vault_metrics_enabled();
        set_vault_metrics_enabled(true);
        record_vault_state_root_cache_hit("us-east-va", "1", VaultId::new(7));
        set_vault_metrics_enabled(prior);
    }

    #[test]
    fn test_record_vault_request_off_is_noop() {
        let prior = vault_metrics_enabled();
        set_vault_metrics_enabled(false);
        record_vault_request("us-east-va", "1", VaultId::new(7), "Write", "OK");
        set_vault_metrics_enabled(prior);
    }

    #[test]
    fn test_record_vault_request_on_emits() {
        let prior = vault_metrics_enabled();
        set_vault_metrics_enabled(true);
        record_vault_request("us-east-va", "1", VaultId::new(7), "Write", "OK");
        record_vault_request("us-east-va", "1", VaultId::new(8), "Read", "Internal");
        set_vault_metrics_enabled(prior);
    }

    #[test]
    fn test_set_org_active_vault_count_always_on() {
        let prior = vault_metrics_enabled();
        // Always-on rollup — fires regardless of gate.
        set_vault_metrics_enabled(false);
        set_org_active_vault_count("us-east-va", "1", "active", 12);
        set_vault_metrics_enabled(true);
        set_org_active_vault_count("us-east-va", "1", "active", 12);
        set_org_active_vault_count("us-east-va", "1", "dormant", 3);
        set_org_active_vault_count("us-east-va", "1", "stalled", 0);
        set_vault_metrics_enabled(prior);
    }

    #[test]
    fn test_record_vault_hibernation_transition_always_on() {
        let prior = vault_metrics_enabled();
        set_vault_metrics_enabled(false);
        record_vault_hibernation_transition("us-east-va", "1", "wake");
        record_vault_hibernation_transition("us-east-va", "1", "sleep");
        set_vault_metrics_enabled(prior);
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
