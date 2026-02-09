//! Metrics for InferaDB Ledger.
//!
//! This module defines metrics for observability using the `metrics` crate.
//! Metrics are exposed via Prometheus when the metrics exporter is initialized.
//!
//! ## Metric Naming Conventions
//!
//! All metrics follow the pattern: `ledger_{subsystem}_{name}_{unit}`
//!
//! - Counters: `_total` suffix
//! - Histograms: `_seconds` or `_bytes` suffix
//! - Gauges: no suffix

use std::time::Instant;

use metrics::{counter, gauge, histogram};

// =============================================================================
// Metric Names (constants for consistency)
// =============================================================================

// Write service metrics
const WRITES_TOTAL: &str = "ledger_writes_total";
const WRITES_LATENCY: &str = "ledger_write_latency_seconds";
const BATCH_WRITES_TOTAL: &str = "ledger_batch_writes_total";
const BATCH_SIZE: &str = "ledger_batch_size";

// Read service metrics
const READS_TOTAL: &str = "ledger_reads_total";
const READS_LATENCY: &str = "ledger_read_latency_seconds";
const VERIFIED_READS_TOTAL: &str = "ledger_verified_reads_total";

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
const DIRTY_BUCKETS: &str = "ledger_dirty_buckets";

// Storage metrics
const STORAGE_BYTES_WRITTEN: &str = "ledger_storage_bytes_written_total";
const STORAGE_BYTES_READ: &str = "ledger_storage_bytes_read_total";
const STORAGE_OPERATIONS: &str = "ledger_storage_operations_total";

// Snapshot metrics
const SNAPSHOTS_CREATED: &str = "ledger_snapshots_created_total";
const SNAPSHOT_SIZE_BYTES: &str = "ledger_snapshot_size_bytes";
const SNAPSHOT_CREATE_LATENCY: &str = "ledger_snapshot_create_latency_seconds";
const SNAPSHOT_RESTORE_LATENCY: &str = "ledger_snapshot_restore_latency_seconds";

// Idempotency cache metrics
const IDEMPOTENCY_HITS: &str = "ledger_idempotency_cache_hits_total";
const IDEMPOTENCY_MISSES: &str = "ledger_idempotency_cache_misses_total";
const IDEMPOTENCY_SIZE: &str = "ledger_idempotency_cache_size";
const IDEMPOTENCY_EVICTIONS: &str = "ledger_idempotency_cache_evictions_total";

// Connection metrics
const ACTIVE_CONNECTIONS: &str = "ledger_active_connections";
const GRPC_REQUESTS_TOTAL: &str = "ledger_grpc_requests_total";
const GRPC_REQUEST_LATENCY: &str = "ledger_grpc_request_latency_seconds";

// Batching metrics
const BATCH_COALESCE_TOTAL: &str = "ledger_batch_coalesce_total";
const BATCH_COALESCE_SIZE: &str = "ledger_batch_coalesce_size";
const BATCH_FLUSH_LATENCY: &str = "ledger_batch_flush_latency_seconds";
const BATCH_EAGER_COMMITS_TOTAL: &str = "ledger_batch_eager_commits_total";
const BATCH_TIMEOUT_COMMITS_TOTAL: &str = "ledger_batch_timeout_commits_total";

// Rate limiting metrics
const RATE_LIMIT_EXCEEDED: &str = "ledger_rate_limit_exceeded_total";
const RATE_LIMIT_REJECTED: &str = "ledger_rate_limit_rejected_total";

// Recovery metrics
const RECOVERY_SUCCESS_TOTAL: &str = "ledger_recovery_success_total";
const RECOVERY_FAILURE_TOTAL: &str = "ledger_recovery_failure_total";
const DETERMINISM_BUG_TOTAL: &str = "ledger_determinism_bug_total";
const RECOVERY_ATTEMPTS_TOTAL: &str = "ledger_divergence_recovery_attempts_total";
const VAULT_HEALTH: &str = "ledger_vault_health";

// Learner refresh metrics
const LEARNER_REFRESH_TOTAL: &str = "ledger_learner_refresh_total";
const LEARNER_REFRESH_LATENCY: &str = "ledger_learner_refresh_latency_seconds";
const LEARNER_CACHE_STALENESS: &str = "ledger_learner_cache_stale_total";
const LEARNER_VOTER_ERRORS: &str = "ledger_learner_voter_errors_total";

// =============================================================================
// Write Service Metrics
// =============================================================================

/// Record a write operation.
#[inline]
pub fn record_write(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    counter!(WRITES_TOTAL, "status" => status).increment(1);
    histogram!(WRITES_LATENCY, "status" => status).record(latency_secs);
}

/// Record a batch write operation.
#[inline]
pub fn record_batch_write(success: bool, batch_size: usize, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    counter!(BATCH_WRITES_TOTAL, "status" => status).increment(1);
    histogram!(WRITES_LATENCY, "status" => status).record(latency_secs);
    histogram!(BATCH_SIZE).record(batch_size as f64);
}

/// Record a rate limit exceeded event (legacy, namespace-only).
#[inline]
pub fn record_rate_limit_exceeded(namespace_id: i64) {
    counter!(RATE_LIMIT_EXCEEDED, "namespace_id" => namespace_id.to_string()).increment(1);
}

/// Record a rate limit rejection with level and reason labels.
///
/// Per PRD Task 4: `ledger_rate_limit_rejected_total{level, reason}`.
#[inline]
pub fn record_rate_limit_rejected(level: &str, reason: &str) {
    counter!(RATE_LIMIT_REJECTED, "level" => level.to_string(), "reason" => reason.to_string())
        .increment(1);
}

// =============================================================================
// Read Service Metrics
// =============================================================================

/// Record a read operation.
#[inline]
pub fn record_read(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    counter!(READS_TOTAL, "status" => status).increment(1);
    histogram!(READS_LATENCY, "status" => status).record(latency_secs);
}

/// Record a verified read operation.
#[inline]
pub fn record_verified_read(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    counter!(VERIFIED_READS_TOTAL, "status" => status).increment(1);
    histogram!(READS_LATENCY, "status" => status, "verified" => "true").record(latency_secs);
}

// =============================================================================
// Raft Consensus Metrics
// =============================================================================

/// Record a Raft proposal submission.
#[inline]
pub fn record_raft_proposal() {
    counter!(RAFT_PROPOSALS_TOTAL).increment(1);
}

/// Record a Raft proposal that timed out before committing.
#[inline]
pub fn record_raft_proposal_timeout() {
    counter!(RAFT_PROPOSAL_TIMEOUTS_TOTAL).increment(1);
}

/// Set the number of pending Raft proposals.
#[inline]
pub fn set_pending_proposals(count: usize) {
    gauge!(RAFT_PROPOSALS_PENDING).set(count as f64);
}

/// Record Raft apply latency.
#[inline]
pub fn record_raft_apply_latency(latency_secs: f64) {
    histogram!(RAFT_APPLY_LATENCY).record(latency_secs);
}

/// Set the current Raft commit index.
#[inline]
pub fn set_raft_commit_index(index: u64) {
    gauge!(RAFT_COMMIT_INDEX).set(index as f64);
}

/// Set the current Raft term.
#[inline]
pub fn set_raft_term(term: u64) {
    gauge!(RAFT_TERM).set(term as f64);
}

/// Set whether this node is the Raft leader.
#[inline]
pub fn set_is_leader(is_leader: bool) {
    gauge!(RAFT_LEADER).set(if is_leader { 1.0 } else { 0.0 });
}

// =============================================================================
// State Machine Metrics
// =============================================================================

/// Record a state root computation.
#[inline]
pub fn record_state_root_computation(vault_id: i64, latency_secs: f64) {
    let vault_label = vault_id.to_string();
    counter!(STATE_ROOT_COMPUTATIONS, "vault_id" => vault_label.clone()).increment(1);
    histogram!(STATE_ROOT_LATENCY, "vault_id" => vault_label).record(latency_secs);
}

/// Set the number of dirty buckets for a vault.
#[inline]
pub fn set_dirty_buckets(vault_id: i64, count: usize) {
    let vault_label = vault_id.to_string();
    gauge!(DIRTY_BUCKETS, "vault_id" => vault_label).set(count as f64);
}

// =============================================================================
// Storage Metrics
// =============================================================================

/// Record bytes written to storage.
#[inline]
pub fn record_storage_write(bytes: usize) {
    counter!(STORAGE_BYTES_WRITTEN).increment(bytes as u64);
    counter!(STORAGE_OPERATIONS, "op" => "write").increment(1);
}

/// Record bytes read from storage.
#[inline]
pub fn record_storage_read(bytes: usize) {
    counter!(STORAGE_BYTES_READ).increment(bytes as u64);
    counter!(STORAGE_OPERATIONS, "op" => "read").increment(1);
}

// =============================================================================
// Snapshot Metrics
// =============================================================================

/// Record a snapshot creation.
#[inline]
pub fn record_snapshot_created(size_bytes: usize, latency_secs: f64) {
    counter!(SNAPSHOTS_CREATED).increment(1);
    histogram!(SNAPSHOT_SIZE_BYTES).record(size_bytes as f64);
    histogram!(SNAPSHOT_CREATE_LATENCY).record(latency_secs);
}

/// Record a snapshot restore.
#[inline]
pub fn record_snapshot_restore(latency_secs: f64) {
    histogram!(SNAPSHOT_RESTORE_LATENCY).record(latency_secs);
}

// =============================================================================
// Idempotency Cache Metrics
// =============================================================================

/// Record an idempotency cache hit.
#[inline]
pub fn record_idempotency_hit() {
    counter!(IDEMPOTENCY_HITS).increment(1);
}

/// Record an idempotency cache miss.
#[inline]
pub fn record_idempotency_miss() {
    counter!(IDEMPOTENCY_MISSES).increment(1);
}

/// Set the current idempotency cache size.
#[inline]
pub fn set_idempotency_cache_size(size: usize) {
    gauge!(IDEMPOTENCY_SIZE).set(size as f64);
}

/// Record idempotency cache evictions.
#[inline]
pub fn record_idempotency_evictions(count: usize) {
    counter!(IDEMPOTENCY_EVICTIONS).increment(count as u64);
}

// =============================================================================
// Connection Metrics
// =============================================================================

/// Increment active connections.
#[inline]
pub fn increment_connections() {
    gauge!(ACTIVE_CONNECTIONS).increment(1.0);
}

/// Decrement active connections.
#[inline]
pub fn decrement_connections() {
    gauge!(ACTIVE_CONNECTIONS).decrement(1.0);
}

/// Record a gRPC request.
///
/// The `error_class` label classifies errors by cause for error-budget tracking:
/// `"timeout"`, `"unavailable"`, `"permission_denied"`, `"validation"`, `"internal"`,
/// or `"none"` for successful requests.
#[inline]
pub fn record_grpc_request(
    service: &str,
    method: &str,
    status: &str,
    error_class: &str,
    latency_secs: f64,
) {
    counter!(GRPC_REQUESTS_TOTAL,
        "service" => service.to_string(),
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_class" => error_class.to_string()
    )
    .increment(1);
    histogram!(GRPC_REQUEST_LATENCY,
        "service" => service.to_string(),
        "method" => method.to_string()
    )
    .record(latency_secs);
}

/// Classify a gRPC status code into an error class label for metrics.
///
/// Returns one of: `"none"`, `"timeout"`, `"unavailable"`, `"permission_denied"`,
/// `"validation"`, `"rate_limited"`, `"internal"`.
pub fn error_class_from_grpc_code(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "none",
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => "timeout",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => "permission_denied",
        tonic::Code::InvalidArgument
        | tonic::Code::NotFound
        | tonic::Code::AlreadyExists
        | tonic::Code::FailedPrecondition
        | tonic::Code::OutOfRange => "validation",
        tonic::Code::ResourceExhausted => "rate_limited",
        _ => "internal",
    }
}

// =============================================================================
// Batching Metrics
// =============================================================================

/// Record a batch coalesce event.
#[inline]
pub fn record_batch_coalesce(size: usize) {
    counter!(BATCH_COALESCE_TOTAL).increment(1);
    histogram!(BATCH_COALESCE_SIZE).record(size as f64);
}

/// Record batch flush latency.
#[inline]
pub fn record_batch_flush(latency_secs: f64) {
    histogram!(BATCH_FLUSH_LATENCY).record(latency_secs);
}

/// Record an eager commit (batch flushed due to queue draining).
///
/// Per DESIGN.md §6.3: Eager commits occur when the incoming queue drains
/// and the batch is flushed immediately rather than waiting for timeout.
#[inline]
pub fn record_eager_commit() {
    counter!(BATCH_EAGER_COMMITS_TOTAL).increment(1);
}

/// Record a timeout commit (batch flushed due to deadline).
#[inline]
pub fn record_timeout_commit() {
    counter!(BATCH_TIMEOUT_COMMITS_TOTAL).increment(1);
}

// =============================================================================
// Recovery Metrics
// =============================================================================

/// Record a successful vault recovery.
#[inline]
pub fn record_recovery_success(namespace_id: i64, vault_id: i64) {
    counter!(
        RECOVERY_SUCCESS_TOTAL,
        "namespace_id" => namespace_id.to_string(),
        "vault_id" => vault_id.to_string()
    )
    .increment(1);
}

/// Record a failed vault recovery attempt.
#[inline]
pub fn record_recovery_failure(namespace_id: i64, vault_id: i64, reason: &str) {
    counter!(
        RECOVERY_FAILURE_TOTAL,
        "namespace_id" => namespace_id.to_string(),
        "vault_id" => vault_id.to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
}

/// Record a determinism bug detection (critical alert).
#[inline]
pub fn record_determinism_bug(namespace_id: i64, vault_id: i64) {
    counter!(
        DETERMINISM_BUG_TOTAL,
        "namespace_id" => namespace_id.to_string(),
        "vault_id" => vault_id.to_string()
    )
    .increment(1);
}

/// Record a divergence recovery attempt with outcome.
#[inline]
pub fn record_recovery_attempt(namespace_id: i64, vault_id: i64, attempt: u8, outcome: &str) {
    counter!(
        RECOVERY_ATTEMPTS_TOTAL,
        "namespace_id" => namespace_id.to_string(),
        "vault_id" => vault_id.to_string(),
        "attempt" => attempt.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

/// Set the vault health gauge for a specific vault.
///
/// State values: 0 = healthy, 1 = diverged, 2 = recovering.
#[inline]
pub fn set_vault_health(namespace_id: i64, vault_id: i64, state: &str) {
    let value = match state {
        "healthy" => 0.0,
        "diverged" => 1.0,
        "recovering" => 2.0,
        _ => -1.0,
    };
    gauge!(
        VAULT_HEALTH,
        "namespace_id" => namespace_id.to_string(),
        "vault_id" => vault_id.to_string(),
        "state" => state.to_string()
    )
    .set(value);
}

// =============================================================================
// Learner Refresh Metrics
// =============================================================================

/// Record a learner refresh attempt.
#[inline]
pub fn record_learner_refresh(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    counter!(LEARNER_REFRESH_TOTAL, "status" => status).increment(1);
    histogram!(LEARNER_REFRESH_LATENCY, "status" => status).record(latency_secs);
}

/// Record a learner cache staleness event.
///
/// This is incremented when a learner's cached state becomes stale
/// and requires refresh from a voter.
#[inline]
pub fn record_learner_cache_stale() {
    counter!(LEARNER_CACHE_STALENESS).increment(1);
}

/// Record a voter connection error during learner refresh.
#[inline]
pub fn record_learner_voter_error(voter_id: u64, error_type: &str) {
    counter!(
        LEARNER_VOTER_ERRORS,
        "voter_id" => voter_id.to_string(),
        "error_type" => error_type.to_string()
    )
    .increment(1);
}

// =============================================================================
// Serialization Metrics
// =============================================================================

// Metric names for serialization timing
const SERIALIZATION_PROTO_DECODE: &str = "ledger_serialization_proto_decode_seconds";
const SERIALIZATION_POSTCARD_ENCODE: &str = "ledger_serialization_postcard_encode_seconds";
const SERIALIZATION_POSTCARD_DECODE: &str = "ledger_serialization_postcard_decode_seconds";
const SERIALIZATION_BYTES: &str = "ledger_serialization_bytes";

/// Record proto decoding latency (gRPC request → internal types).
///
/// This measures the time to convert protobuf messages to internal Rust types,
/// which is part of the write path hot loop.
#[inline]
pub fn record_proto_decode(latency_secs: f64, operation: &str) {
    histogram!(SERIALIZATION_PROTO_DECODE, "operation" => operation.to_string())
        .record(latency_secs);
}

/// Record postcard encoding latency (internal types → Raft log).
///
/// This measures serialization time when appending entries to the Raft log.
/// Per DESIGN.md architecture: internal types are postcard-serialized for
/// efficient storage.
#[inline]
pub fn record_postcard_encode(latency_secs: f64, entry_type: &str) {
    histogram!(SERIALIZATION_POSTCARD_ENCODE, "entry_type" => entry_type.to_string())
        .record(latency_secs);
}

/// Record postcard decoding latency (Raft log → internal types).
///
/// This measures deserialization time when reading entries from the Raft log,
/// used during log replay and snapshot restoration.
#[inline]
pub fn record_postcard_decode(latency_secs: f64, entry_type: &str) {
    histogram!(SERIALIZATION_POSTCARD_DECODE, "entry_type" => entry_type.to_string())
        .record(latency_secs);
}

/// Record serialization size in bytes.
///
/// Useful for correlating latency with payload size and detecting
/// unexpectedly large serialized payloads.
#[inline]
pub fn record_serialization_bytes(bytes: usize, direction: &str, entry_type: &str) {
    histogram!(
        SERIALIZATION_BYTES,
        "direction" => direction.to_string(),
        "entry_type" => entry_type.to_string()
    )
    .record(bytes as f64);
}

// Audit logging metrics
const AUDIT_EVENTS_TOTAL: &str = "ledger_audit_events_total";

/// Record an audit event for Prometheus tracking.
///
/// Tracks the total number of audit events by action and outcome.
/// This is called by the audit integration layer after each operation.
#[inline]
pub fn record_audit_event(action: &str, outcome: &str) {
    counter!(
        AUDIT_EVENTS_TOTAL,
        "action" => action.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

// B+ tree compaction metrics
const BTREE_COMPACTION_RUNS_TOTAL: &str = "ledger_btree_compaction_runs_total";
const BTREE_COMPACTION_PAGES_MERGED: &str = "ledger_btree_compaction_pages_merged";
const BTREE_COMPACTION_PAGES_FREED: &str = "ledger_btree_compaction_pages_freed";

/// Record a B+ tree compaction run.
///
/// Tracks the number of compaction cycles and the pages merged/freed.
#[inline]
pub fn record_btree_compaction(pages_merged: u64, pages_freed: u64) {
    counter!(BTREE_COMPACTION_RUNS_TOTAL).increment(1);
    counter!(BTREE_COMPACTION_PAGES_MERGED).increment(pages_merged);
    counter!(BTREE_COMPACTION_PAGES_FREED).increment(pages_freed);
}

// ─── Hot Key Detection ────────────────────────────────────────

/// Hot key detection events.
const HOT_KEY_DETECTED_TOTAL: &str = "ledger_hot_key_detected_total";

/// Record a hot key detection event.
///
/// Called whenever a key's access rate exceeds the configured threshold.
/// Labels include vault_id and a hash of the key (not the key itself,
/// to avoid high-cardinality label explosion).
#[inline]
pub fn record_hot_key_detected(
    vault_id: inferadb_ledger_types::VaultId,
    key: &str,
    ops_per_sec: f64,
) {
    let key_hash = format!("{:016x}", seahash::hash(key.as_bytes()));
    counter!(
        HOT_KEY_DETECTED_TOTAL,
        "vault_id" => vault_id.value().to_string(),
        "key_hash" => key_hash,
        "ops_per_sec" => format!("{:.0}", ops_per_sec)
    )
    .increment(1);
}

// ─── SLI/SLO Metrics ──────────────────────────────────────────

/// Batch writer queue depth gauge.
const BATCH_QUEUE_DEPTH: &str = "ledger_batch_queue_depth";

/// Rate limiter queue depth gauge (pending proposals tracked by backpressure tier).
const RATE_LIMIT_QUEUE_DEPTH: &str = "ledger_rate_limit_queue_depth";

/// Cluster quorum status gauge (1 = quorum, 0 = lost).
const CLUSTER_QUORUM_STATUS: &str = "ledger_cluster_quorum_status";

/// Leader election counter.
const LEADER_ELECTIONS_TOTAL: &str = "ledger_leader_elections_total";

/// Set the current batch writer queue depth.
///
/// Tracks how many write operations are pending in the batch writer,
/// serving as a leading indicator of write saturation.
#[inline]
pub fn set_batch_queue_depth(depth: usize) {
    gauge!(BATCH_QUEUE_DEPTH).set(depth as f64);
}

/// Set the current rate limiter queue depth.
///
/// Tracks the number of pending proposals seen by the rate limiter's
/// backpressure tier, indicating write pipeline saturation.
#[inline]
pub fn set_rate_limit_queue_depth(depth: u64) {
    gauge!(RATE_LIMIT_QUEUE_DEPTH).set(depth as f64);
}

/// Set the cluster quorum status.
///
/// - `1.0` — a leader is elected and the cluster has quorum
/// - `0.0` — no leader, quorum lost
#[inline]
pub fn set_cluster_quorum_status(has_quorum: bool) {
    gauge!(CLUSTER_QUORUM_STATUS).set(if has_quorum { 1.0 } else { 0.0 });
}

/// Record a leader election event.
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

/// B-tree page splits counter.
const BTREE_PAGE_SPLITS_TOTAL: &str = "ledger_btree_page_splits_total";

/// Compaction lag blocks gauge (free pages as a proxy for reclaimable space).
const COMPACTION_LAG_BLOCKS: &str = "ledger_compaction_lag_blocks";

/// Snapshot total disk bytes gauge.
const SNAPSHOT_DISK_BYTES: &str = "ledger_snapshot_disk_bytes";

/// Set disk space metrics.
///
/// Updates total, free, and used disk bytes for the data directory's filesystem.
#[inline]
pub fn set_disk_bytes(total: u64, free: u64) {
    gauge!(DISK_BYTES_TOTAL).set(total as f64);
    gauge!(DISK_BYTES_FREE).set(free as f64);
    gauge!(DISK_BYTES_USED).set((total.saturating_sub(free)) as f64);
}

/// Set page cache counters.
///
/// Reports cumulative cache hit/miss totals and current cache size.
#[inline]
pub fn set_page_cache_metrics(hits: u64, misses: u64, size: usize) {
    counter!(PAGE_CACHE_HITS_TOTAL).absolute(hits);
    counter!(PAGE_CACHE_MISSES_TOTAL).absolute(misses);
    gauge!(PAGE_CACHE_SIZE).set(size as f64);
}

/// Set B-tree depth for a given table.
#[inline]
pub fn set_btree_depth(table: &str, depth: u32) {
    gauge!(BTREE_DEPTH, "table" => table.to_string()).set(f64::from(depth));
}

/// Set B-tree page splits total.
#[inline]
pub fn set_btree_page_splits(total: u64) {
    counter!(BTREE_PAGE_SPLITS_TOTAL).absolute(total);
}

/// Set compaction lag blocks gauge.
///
/// Tracks the number of free pages (reclaimable space) as a proxy for
/// compaction backlog. High values indicate that compaction is falling behind.
#[inline]
pub fn set_compaction_lag_blocks(blocks: usize) {
    gauge!(COMPACTION_LAG_BLOCKS).set(blocks as f64);
}

/// Set snapshot total disk bytes.
///
/// Tracks the total disk space used by all snapshots in the snapshot directory.
#[inline]
pub fn set_snapshot_disk_bytes(bytes: u64) {
    gauge!(SNAPSHOT_DISK_BYTES).set(bytes as f64);
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
    /// Create a new timer.
    pub fn new<F: FnOnce(f64) + Send + 'static>(record_fn: F) -> Self {
        Self { start: Instant::now(), record_fn: Some(Box::new(record_fn)) }
    }

    /// Get elapsed time in seconds.
    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    /// Stop the timer and return elapsed time without recording.
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

// ─── Namespace Resource Accounting Metrics ───────────────────

/// Per-namespace cumulative storage bytes (gauge).
const NAMESPACE_STORAGE_BYTES: &str = "ledger_namespace_storage_bytes";

/// Per-namespace operation counter.
const NAMESPACE_OPERATIONS_TOTAL: &str = "ledger_namespace_operations_total";

/// Per-namespace operation latency histogram.
const NAMESPACE_LATENCY_SECONDS: &str = "ledger_namespace_latency_seconds";

/// Set the current cumulative storage bytes for a namespace.
///
/// Cardinality is bounded by the number of namespaces, which is
/// operator-controlled (typically < 100 in production).
#[inline]
pub fn set_namespace_storage_bytes(namespace_id: i64, bytes: u64) {
    gauge!(NAMESPACE_STORAGE_BYTES, "namespace_id" => namespace_id.to_string())
        .set(bytes as f64);
}

/// Record a namespace-level operation (read, write, or admin).
///
/// Increments `ledger_namespace_operations_total{namespace_id, operation}`.
#[inline]
pub fn record_namespace_operation(namespace_id: i64, operation: &str) {
    counter!(
        NAMESPACE_OPERATIONS_TOTAL,
        "namespace_id" => namespace_id.to_string(),
        "operation" => operation.to_string()
    )
    .increment(1);
}

/// Record per-namespace operation latency.
///
/// Records into `ledger_namespace_latency_seconds{namespace_id, operation}`.
#[inline]
pub fn record_namespace_latency(namespace_id: i64, operation: &str, latency_secs: f64) {
    histogram!(
        NAMESPACE_LATENCY_SECONDS,
        "namespace_id" => namespace_id.to_string(),
        "operation" => operation.to_string()
    )
    .record(latency_secs);
}

/// Create a timer for write operations.
pub fn write_timer() -> Timer {
    Timer::new(|secs| record_write(true, secs))
}

/// Create a timer for read operations.
pub fn read_timer() -> Timer {
    Timer::new(|secs| record_read(true, secs))
}

/// Create a timer for Raft apply operations.
pub fn raft_apply_timer() -> Timer {
    Timer::new(record_raft_apply_latency)
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
    fn test_metrics_dont_panic() {
        // These should not panic even without a recorder installed
        record_write(true, 0.001);
        record_read(true, 0.001);
        record_raft_proposal();
        set_raft_commit_index(100);
        set_is_leader(true);
        record_idempotency_hit();
        record_grpc_request("WriteService", "write", "OK", "none", 0.001);
    }

    #[test]
    fn test_sli_metrics_dont_panic() {
        // SLI/SLO metrics should not panic without a recorder
        set_batch_queue_depth(42);
        set_rate_limit_queue_depth(10);
        set_cluster_quorum_status(true);
        set_cluster_quorum_status(false);
        record_leader_election();
        record_grpc_request("WriteService", "write", "Internal", "internal", 0.5);
        record_grpc_request("ReadService", "read", "DeadlineExceeded", "timeout", 1.0);
        record_grpc_request("AdminService", "create", "InvalidArgument", "validation", 0.01);
        record_grpc_request("WriteService", "write", "ResourceExhausted", "rate_limited", 0.1);
        record_grpc_request("WriteService", "write", "Unavailable", "unavailable", 0.05);
    }

    #[test]
    fn test_error_class_from_grpc_code() {
        assert_eq!(error_class_from_grpc_code(tonic::Code::Ok), "none");
        assert_eq!(error_class_from_grpc_code(tonic::Code::DeadlineExceeded), "timeout");
        assert_eq!(error_class_from_grpc_code(tonic::Code::Cancelled), "timeout");
        assert_eq!(error_class_from_grpc_code(tonic::Code::Unavailable), "unavailable");
        assert_eq!(error_class_from_grpc_code(tonic::Code::PermissionDenied), "permission_denied");
        assert_eq!(error_class_from_grpc_code(tonic::Code::Unauthenticated), "permission_denied");
        assert_eq!(error_class_from_grpc_code(tonic::Code::InvalidArgument), "validation");
        assert_eq!(error_class_from_grpc_code(tonic::Code::NotFound), "validation");
        assert_eq!(error_class_from_grpc_code(tonic::Code::AlreadyExists), "validation");
        assert_eq!(error_class_from_grpc_code(tonic::Code::FailedPrecondition), "validation");
        assert_eq!(error_class_from_grpc_code(tonic::Code::OutOfRange), "validation");
        assert_eq!(error_class_from_grpc_code(tonic::Code::ResourceExhausted), "rate_limited");
        assert_eq!(error_class_from_grpc_code(tonic::Code::Internal), "internal");
        assert_eq!(error_class_from_grpc_code(tonic::Code::DataLoss), "internal");
        assert_eq!(error_class_from_grpc_code(tonic::Code::Unknown), "internal");
        assert_eq!(error_class_from_grpc_code(tonic::Code::Unimplemented), "internal");
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
}
