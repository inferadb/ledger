//! Observability metrics exposed via Prometheus using the `metrics` crate.
//!
//! ## Metric Naming Conventions
//!
//! All metrics follow the pattern: `ledger_{subsystem}_{name}_{unit}`
//!
//! - Counters: `_total` suffix
//! - Histograms: `_seconds` or `_bytes` suffix
//! - Gauges: no suffix

use std::time::Instant;

use inferadb_ledger_state::system::MIN_NODES_PER_PROTECTED_REGION;
use inferadb_ledger_types::{OrganizationId, VaultId};
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
const READ_FORWARDS_TOTAL: &str = "ledger_read_forwards_total";

// Cross-region forwarding metrics
const CROSS_REGION_FORWARD_TOTAL: &str = "ledger_cross_region_forward_total";
const CROSS_REGION_FORWARD_LATENCY: &str = "ledger_cross_region_forward_latency_seconds";

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

// Integrity scrubber metrics
const INTEGRITY_PAGES_CHECKED: &str = "ledger_integrity_pages_checked_total";
const INTEGRITY_ERRORS: &str = "ledger_integrity_errors_total";
const INTEGRITY_SCAN_DURATION: &str = "ledger_integrity_scan_duration_seconds";

// Learner refresh metrics
const LEARNER_REFRESH_TOTAL: &str = "ledger_learner_refresh_total";
const LEARNER_REFRESH_LATENCY: &str = "ledger_learner_refresh_latency_seconds";
const LEARNER_CACHE_STALENESS: &str = "ledger_learner_cache_stale_total";
const LEARNER_VOTER_ERRORS: &str = "ledger_learner_voter_errors_total";

// =============================================================================
// Write Service Metrics
// =============================================================================

/// Records a write operation.
#[inline]
pub fn record_write(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    counter!(WRITES_TOTAL, "status" => status).increment(1);
    histogram!(WRITES_LATENCY, "status" => status).record(latency_secs);
}

/// Records a batch write operation.
#[inline]
pub fn record_batch_write(success: bool, batch_size: usize, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    counter!(BATCH_WRITES_TOTAL, "status" => status).increment(1);
    histogram!(WRITES_LATENCY, "status" => status).record(latency_secs);
    histogram!(BATCH_SIZE).record(batch_size as f64);
}

/// Records a rate limit exceeded event for a single organization.
#[inline]
pub fn record_rate_limit_exceeded(organization: OrganizationId) {
    counter!(RATE_LIMIT_EXCEEDED, "organization_id" => organization.value().to_string())
        .increment(1);
}

/// Records a rate limit rejection with level and reason labels.
///
/// `ledger_rate_limit_rejected_total{level, reason}`.
#[inline]
pub fn record_rate_limit_rejected(level: &str, reason: &str) {
    counter!(RATE_LIMIT_REJECTED, "level" => level.to_string(), "reason" => reason.to_string())
        .increment(1);
}

// =============================================================================
// Read Service Metrics
// =============================================================================

/// Records a read operation.
#[inline]
pub fn record_read(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    counter!(READS_TOTAL, "status" => status).increment(1);
    histogram!(READS_LATENCY, "status" => status).record(latency_secs);
}

/// Records a verified read operation.
#[inline]
pub fn record_verified_read(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    counter!(VERIFIED_READS_TOTAL, "status" => status).increment(1);
    histogram!(READS_LATENCY, "status" => status, "verified" => "true").record(latency_secs);
}

/// Records a read request forwarded to the leader due to Raft log lag.
///
/// `method` is the RPC method name (e.g., `"read"`, `"get_block"`, `"list_entities"`).
#[inline]
pub fn record_read_forward(method: &str) {
    counter!(READ_FORWARDS_TOTAL, "method" => method.to_string()).increment(1);
}

// =============================================================================
// Cross-Region Forwarding Metrics
// =============================================================================

/// Records a cross-region request forward.
///
/// Increments `ledger_cross_region_forward_total{method, source_region, target_region}`
/// and records latency in `ledger_cross_region_forward_latency_seconds`.
///
/// Emitted when a request for a protected-region organization arrives at an
/// out-of-region node and is forwarded to an in-region node.
#[inline]
pub fn record_cross_region_forward(
    method: &str,
    source_region: &str,
    target_region: &str,
    latency_secs: f64,
) {
    counter!(
        CROSS_REGION_FORWARD_TOTAL,
        "method" => method.to_string(),
        "source_region" => source_region.to_string(),
        "target_region" => target_region.to_string()
    )
    .increment(1);
    histogram!(
        CROSS_REGION_FORWARD_LATENCY,
        "source_region" => source_region.to_string(),
        "target_region" => target_region.to_string()
    )
    .record(latency_secs);
}

/// Records a data residency violation attempt.
///
/// Incremented when a request arrives at a node outside the organization's
/// protected region. The request is forwarded, but the violation is tracked
/// for operational alerting.
#[inline]
pub fn record_data_residency_violation(region: &str) {
    counter!(
        DATA_RESIDENCY_VIOLATION_TOTAL,
        "region" => region.to_string()
    )
    .increment(1);
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
pub fn record_state_root_computation(vault: VaultId, latency_secs: f64) {
    let vault_label = vault.value().to_string();
    counter!(STATE_ROOT_COMPUTATIONS, "vault_id" => vault_label.clone()).increment(1);
    histogram!(STATE_ROOT_LATENCY, "vault_id" => vault_label).record(latency_secs);
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
pub fn record_state_root_divergence(organization: OrganizationId, vault: VaultId) {
    counter!(
        STATE_ROOT_DIVERGENCES,
        "organization_id" => organization.value().to_string(),
        "vault_id" => vault.value().to_string(),
    )
    .increment(1);
}

/// Sets the number of dirty buckets for a vault.
#[inline]
pub fn set_dirty_buckets(vault: VaultId, count: usize) {
    let vault_label = vault.value().to_string();
    gauge!(DIRTY_BUCKETS, "vault_id" => vault_label).set(count as f64);
}

// =============================================================================
// Storage Metrics
// =============================================================================

/// Records bytes written to storage.
#[inline]
pub fn record_storage_write(bytes: usize) {
    counter!(STORAGE_BYTES_WRITTEN).increment(bytes as u64);
    counter!(STORAGE_OPERATIONS, "op" => "write").increment(1);
}

/// Records bytes read from storage.
#[inline]
pub fn record_storage_read(bytes: usize) {
    counter!(STORAGE_BYTES_READ).increment(bytes as u64);
    counter!(STORAGE_OPERATIONS, "op" => "read").increment(1);
}

// =============================================================================
// Snapshot Metrics
// =============================================================================

/// Records a snapshot creation.
#[inline]
pub fn record_snapshot_created(size_bytes: usize, latency_secs: f64) {
    counter!(SNAPSHOTS_CREATED).increment(1);
    histogram!(SNAPSHOT_SIZE_BYTES).record(size_bytes as f64);
    histogram!(SNAPSHOT_CREATE_LATENCY).record(latency_secs);
}

/// Records a snapshot restore.
#[inline]
pub fn record_snapshot_restore(latency_secs: f64) {
    histogram!(SNAPSHOT_RESTORE_LATENCY).record(latency_secs);
}

// =============================================================================
// Idempotency Cache Metrics
// =============================================================================

/// Records an idempotency cache hit.
#[inline]
pub fn record_idempotency_hit() {
    counter!(IDEMPOTENCY_HITS).increment(1);
}

/// Records an idempotency cache miss.
#[inline]
pub fn record_idempotency_miss() {
    counter!(IDEMPOTENCY_MISSES).increment(1);
}

/// Sets the current idempotency cache size.
#[inline]
pub fn set_idempotency_cache_size(size: usize) {
    gauge!(IDEMPOTENCY_SIZE).set(size as f64);
}

/// Records idempotency cache evictions.
#[inline]
pub fn record_idempotency_evictions(count: usize) {
    counter!(IDEMPOTENCY_EVICTIONS).increment(count as u64);
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
/// * `error_class` - Error classification for error-budget tracking: `"timeout"`, `"unavailable"`,
///   `"permission_denied"`, `"validation"`, `"rate_limited"`, `"internal"`, or `"none"` for
///   successful requests. See [`error_class_from_grpc_code`] for the mapping.
/// * `latency_secs` - Request latency in seconds.
/// * `region` - Region identifier string (e.g., `"us-east-va"`, `"global"`).
#[inline]
pub fn record_grpc_request(
    service: &str,
    method: &str,
    status: &str,
    error_class: &str,
    latency_secs: f64,
    region: &str,
) {
    counter!(GRPC_REQUESTS_TOTAL,
        "service" => service.to_string(),
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_class" => error_class.to_string(),
        "region" => region.to_string()
    )
    .increment(1);
    histogram!(GRPC_REQUEST_LATENCY,
        "service" => service.to_string(),
        "method" => method.to_string(),
        "region" => region.to_string()
    )
    .record(latency_secs);
}

/// Classifies a gRPC status code into an error class label for metrics.
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

/// Records an eager commit (batch flushed due to queue draining).
///
/// Eager commits occur when the incoming queue drains and the batch is
/// flushed immediately rather than waiting for timeout.
#[inline]
pub fn record_eager_commit() {
    counter!(BATCH_EAGER_COMMITS_TOTAL).increment(1);
}

/// Records a timeout commit (batch flushed due to deadline).
#[inline]
pub fn record_timeout_commit() {
    counter!(BATCH_TIMEOUT_COMMITS_TOTAL).increment(1);
}

// =============================================================================
// Recovery Metrics
// =============================================================================

/// Records a successful vault recovery.
#[inline]
pub fn record_recovery_success(organization: OrganizationId, vault: VaultId) {
    counter!(
        RECOVERY_SUCCESS_TOTAL,
        "organization_id" => organization.value().to_string(),
        "vault_id" => vault.value().to_string()
    )
    .increment(1);
}

/// Records a failed vault recovery attempt.
#[inline]
pub fn record_recovery_failure(organization: OrganizationId, vault: VaultId, reason: &str) {
    counter!(
        RECOVERY_FAILURE_TOTAL,
        "organization_id" => organization.value().to_string(),
        "vault_id" => vault.value().to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
}

/// Records a determinism bug detection (critical alert).
#[inline]
pub fn record_determinism_bug(organization: OrganizationId, vault: VaultId) {
    counter!(
        DETERMINISM_BUG_TOTAL,
        "organization_id" => organization.value().to_string(),
        "vault_id" => vault.value().to_string()
    )
    .increment(1);
}

/// Records a divergence recovery attempt with outcome.
#[inline]
pub fn record_recovery_attempt(
    organization: OrganizationId,
    vault: VaultId,
    attempt: u8,
    outcome: &str,
) {
    counter!(
        RECOVERY_ATTEMPTS_TOTAL,
        "organization_id" => organization.value().to_string(),
        "vault_id" => vault.value().to_string(),
        "attempt" => attempt.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

/// Sets the vault health gauge for a specific vault.
///
/// State values: 0 = healthy, 1 = diverged, 2 = recovering.
#[inline]
pub fn set_vault_health(organization: OrganizationId, vault: VaultId, state: &str) {
    let value = match state {
        "healthy" => 0.0,
        "diverged" => 1.0,
        "recovering" => 2.0,
        _ => -1.0,
    };
    gauge!(
        VAULT_HEALTH,
        "organization_id" => organization.value().to_string(),
        "vault_id" => vault.value().to_string(),
        "state" => state.to_string()
    )
    .set(value);
}

// =============================================================================
// Learner Refresh Metrics
// =============================================================================

/// Records a learner refresh attempt.
#[inline]
pub fn record_learner_refresh(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "error" };
    counter!(LEARNER_REFRESH_TOTAL, "status" => status).increment(1);
    histogram!(LEARNER_REFRESH_LATENCY, "status" => status).record(latency_secs);
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

/// Records proto decoding latency (gRPC request → internal types).
///
/// This measures the time to convert protobuf messages to internal Rust types,
/// which is part of the write path hot loop.
#[inline]
pub fn record_proto_decode(latency_secs: f64, operation: &str) {
    histogram!(SERIALIZATION_PROTO_DECODE, "operation" => operation.to_string())
        .record(latency_secs);
}

/// Records postcard encoding latency (internal types → Raft log).
///
/// This measures serialization time when appending entries to the Raft log.
/// Internal types are postcard-serialized for efficient storage.
#[inline]
pub fn record_postcard_encode(latency_secs: f64, entry_type: &str) {
    histogram!(SERIALIZATION_POSTCARD_ENCODE, "entry_type" => entry_type.to_string())
        .record(latency_secs);
}

/// Records postcard decoding latency (Raft log → internal types).
///
/// This measures deserialization time when reading entries from the Raft log,
/// used during log replay and snapshot restoration.
#[inline]
pub fn record_postcard_decode(latency_secs: f64, entry_type: &str) {
    histogram!(SERIALIZATION_POSTCARD_DECODE, "entry_type" => entry_type.to_string())
        .record(latency_secs);
}

/// Records serialization size in bytes.
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

// B+ tree compaction metrics
const BTREE_COMPACTION_RUNS_TOTAL: &str = "ledger_btree_compaction_runs_total";
const BTREE_COMPACTION_PAGES_MERGED: &str = "ledger_btree_compaction_pages_merged";
const BTREE_COMPACTION_PAGES_FREED: &str = "ledger_btree_compaction_pages_freed";

/// Records a B+ tree compaction run.
///
/// Tracks the number of compaction cycles and the pages merged/freed.
#[inline]
pub fn record_btree_compaction(pages_merged: u64, pages_freed: u64) {
    counter!(BTREE_COMPACTION_RUNS_TOTAL).increment(1);
    counter!(BTREE_COMPACTION_PAGES_MERGED).increment(pages_merged);
    counter!(BTREE_COMPACTION_PAGES_FREED).increment(pages_freed);
}

// ─── Post-Erasure Compaction ──────────────────────────────────

/// Snapshots triggered by the post-erasure compaction job.
///
/// Labels: `region` = region name, `trigger` = time_based | erasure_detected
const POST_ERASURE_COMPACTION_TRIGGERED_TOTAL: &str =
    "ledger_post_erasure_compaction_triggered_total";

/// Records a post-erasure compaction snapshot trigger.
pub fn record_post_erasure_compaction_triggered(region: &str) {
    counter!(
        POST_ERASURE_COMPACTION_TRIGGERED_TOTAL,
        "region" => region.to_string(),
    )
    .increment(1);
}

// ─── Organization Purge ──────────────────────────────────────

/// REGIONAL purge step failures.
const ORG_PURGE_REGIONAL_FAILURES_TOTAL: &str = "ledger_org_purge_regional_failures_total";

/// GLOBAL purge step failures.
const ORG_PURGE_GLOBAL_FAILURES_TOTAL: &str = "ledger_org_purge_global_failures_total";

/// Organizations that failed all retry attempts.
const ORG_PURGE_RETRY_EXHAUSTED_TOTAL: &str = "ledger_org_purge_retry_exhausted_total";

/// Records a REGIONAL purge step failure.
pub fn record_org_purge_regional_failure(region: &str) {
    counter!(ORG_PURGE_REGIONAL_FAILURES_TOTAL, "region" => region.to_string()).increment(1);
}

/// Records a GLOBAL purge step failure.
pub fn record_org_purge_global_failure() {
    counter!(ORG_PURGE_GLOBAL_FAILURES_TOTAL).increment(1);
}

/// Records an organization whose purge retries were exhausted.
pub fn record_org_purge_retry_exhausted() {
    counter!(ORG_PURGE_RETRY_EXHAUSTED_TOTAL).increment(1);
}

// ─── Hot Key Detection ────────────────────────────────────────

/// Hot key detection events.
const HOT_KEY_DETECTED_TOTAL: &str = "ledger_hot_key_detected_total";

/// Records a hot key detection event.
///
/// Called whenever a key's access rate exceeds the configured threshold.
/// Labels include vault_id and a hash of the key (not the key itself,
/// to avoid high-cardinality label explosion).
#[inline]
pub fn record_hot_key_detected(vault: VaultId, key: &str, ops_per_sec: f64) {
    let key_hash = format!("{:016x}", seahash::hash(key.as_bytes()));
    counter!(
        HOT_KEY_DETECTED_TOTAL,
        "vault_id" => vault.value().to_string(),
        "key_hash" => key_hash,
        "ops_per_sec" => format!("{:.0}", ops_per_sec)
    )
    .increment(1);
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
    gauge!(BATCH_QUEUE_DEPTH, "region" => region.to_string()).set(depth as f64);
}

/// Sets the current rate limiter queue depth.
///
/// Tracks the number of pending proposals seen by the rate limiter's
/// backpressure tier, indicating write pipeline saturation.
/// The `region` label identifies which region's rate limiter is being measured.
#[inline]
pub fn set_rate_limit_queue_depth(depth: u64, region: &str) {
    gauge!(RATE_LIMIT_QUEUE_DEPTH, "region" => region.to_string()).set(depth as f64);
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

/// B-tree page splits counter.
const BTREE_PAGE_SPLITS_TOTAL: &str = "ledger_btree_page_splits_total";

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
    gauge!(DISK_BYTES_TOTAL, "region" => region.to_string()).set(total as f64);
    gauge!(DISK_BYTES_FREE, "region" => region.to_string()).set(free as f64);
    gauge!(DISK_BYTES_USED, "region" => region.to_string())
        .set((total.saturating_sub(free)) as f64);
}

/// Sets page cache counters.
///
/// Reports cumulative cache hit/miss totals and current cache size.
/// The `region` label identifies which region's database is being measured.
#[inline]
pub fn set_page_cache_metrics(hits: u64, misses: u64, size: usize, region: &str) {
    counter!(PAGE_CACHE_HITS_TOTAL, "region" => region.to_string()).absolute(hits);
    counter!(PAGE_CACHE_MISSES_TOTAL, "region" => region.to_string()).absolute(misses);
    gauge!(PAGE_CACHE_SIZE, "region" => region.to_string()).set(size as f64);
}

/// Sets B-tree depth for a given table.
///
/// The `region` label identifies which region's database is being measured.
#[inline]
pub fn set_btree_depth(table: &str, depth: u32, region: &str) {
    gauge!(BTREE_DEPTH, "table" => table.to_string(), "region" => region.to_string())
        .set(f64::from(depth));
}

/// Sets B-tree page splits total.
///
/// The `region` label identifies which region's database is being measured.
#[inline]
pub fn set_btree_page_splits(total: u64, region: &str) {
    counter!(BTREE_PAGE_SPLITS_TOTAL, "region" => region.to_string()).absolute(total);
}

/// Sets compaction lag blocks gauge.
///
/// Tracks the number of free pages (reclaimable space) as a proxy for
/// compaction backlog. High values indicate that compaction is falling behind.
/// The `region` label identifies which region's database is being measured.
#[inline]
pub fn set_compaction_lag_blocks(blocks: usize, region: &str) {
    gauge!(COMPACTION_LAG_BLOCKS, "region" => region.to_string()).set(blocks as f64);
}

/// Sets snapshot total disk bytes.
///
/// Tracks the total disk space used by all snapshots in the snapshot directory.
/// The `region` label identifies which region's snapshots are being measured.
#[inline]
pub fn set_snapshot_disk_bytes(bytes: u64, region: &str) {
    gauge!(SNAPSHOT_DISK_BYTES, "region" => region.to_string()).set(bytes as f64);
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
    counter!(INTEGRITY_ERRORS, "error_type" => error_type.to_string()).increment(count);
}

/// Records the duration of a scrub cycle in seconds.
#[inline]
pub fn record_integrity_scan_duration(duration_secs: f64) {
    histogram!(INTEGRITY_SCAN_DURATION).record(duration_secs);
}

// ─── Organization Resource Accounting Metrics ────────────────

/// Per-organization cumulative storage bytes (gauge).
const ORGANIZATION_STORAGE_BYTES: &str = "ledger_organization_storage_bytes";

/// Per-organization operation counter.
const ORGANIZATION_OPERATIONS_TOTAL: &str = "ledger_organization_operations_total";

/// Per-organization operation latency histogram.
const ORGANIZATION_LATENCY_SECONDS: &str = "ledger_organization_latency_seconds";

/// Sets the current cumulative storage bytes for an organization.
///
/// Cardinality is bounded by the number of organizations, which is
/// operator-controlled (typically < 100 in production).
#[inline]
pub fn set_organization_storage_bytes(organization: OrganizationId, bytes: u64) {
    gauge!(ORGANIZATION_STORAGE_BYTES, "organization_id" => organization.value().to_string())
        .set(bytes as f64);
}

/// Records an organization-level operation (read, write, or admin).
///
/// Increments `ledger_organization_operations_total{organization_id, operation}`.
#[inline]
pub fn record_organization_operation(organization: OrganizationId, operation: &str) {
    counter!(
        ORGANIZATION_OPERATIONS_TOTAL,
        "organization_id" => organization.value().to_string(),
        "operation" => operation.to_string()
    )
    .increment(1);
}

/// Records per-organization operation latency.
///
/// Records into `ledger_organization_latency_seconds{organization_id, operation}`.
#[inline]
pub fn record_organization_latency(
    organization: OrganizationId,
    operation: &str,
    latency_secs: f64,
) {
    histogram!(
        ORGANIZATION_LATENCY_SECONDS,
        "organization_id" => organization.value().to_string(),
        "operation" => operation.to_string()
    )
    .record(latency_secs);
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
    histogram!(
        BACKGROUND_JOB_DURATION_SECONDS,
        "job" => job.to_string()
    )
    .record(duration_secs);
}

/// Records a completed background job cycle.
///
/// `result` must be `"success"` or `"failure"` — bounded cardinality.
#[inline]
pub fn record_background_job_run(job: &str, result: &str) {
    counter!(
        BACKGROUND_JOB_RUNS_TOTAL,
        "job" => job.to_string(),
        "result" => result.to_string()
    )
    .increment(1);
}

/// Records items processed by a background job cycle.
#[inline]
pub fn record_background_job_items(job: &str, count: u64) {
    counter!(
        BACKGROUND_JOB_ITEMS_PROCESSED_TOTAL,
        "job" => job.to_string()
    )
    .increment(count);
}

// ─── Saga PII Cache Metrics ───────────────────────────────────

/// In-memory user PII entries held by the saga orchestrator (gauge).
const SAGA_PII_CACHE_SIZE: &str = "ledger_saga_pii_cache_size";

/// In-memory organization PII entries held by the saga orchestrator (gauge).
const SAGA_ORG_PII_CACHE_SIZE: &str = "ledger_saga_org_pii_cache_size";

/// In-memory crypto material entries held by the saga orchestrator (gauge).
const SAGA_CRYPTO_CACHE_SIZE: &str = "ledger_saga_crypto_cache_size";

/// Records the current size of each in-memory PII cache in the saga
/// orchestrator. Called at the end of each `run_cycle()` for operational
/// visibility into memory-resident PII.
#[inline]
pub fn record_saga_pii_cache_sizes(pii: usize, org_pii: usize, crypto: usize) {
    gauge!(SAGA_PII_CACHE_SIZE).set(pii as f64);
    gauge!(SAGA_ORG_PII_CACHE_SIZE).set(org_pii as f64);
    gauge!(SAGA_CRYPTO_CACHE_SIZE).set(crypto as f64);
}

// ─── DEK Re-Wrapping Metrics ──────────────────────────────────

/// Total pages re-wrapped during RMK rotation (counter).
const REWRAP_PAGES_TOTAL: &str = "ledger_rewrap_pages_total";

/// Remaining pages to process during re-wrapping (gauge).
const REWRAP_PAGES_REMAINING: &str = "ledger_rewrap_pages_remaining";

/// Duration of a single re-wrapping batch in seconds (histogram).
const REWRAP_DURATION_SECONDS: &str = "ledger_rewrap_duration_seconds";

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

/// Records re-wrapping batch duration.
#[inline]
pub fn record_rewrap_duration(duration_secs: f64) {
    histogram!(REWRAP_DURATION_SECONDS).record(duration_secs);
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
        "metric_name" => metric_name.to_string()
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
    counter!(
        EVENTS_INGEST_TOTAL,
        "source_service" => source_service.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(u64::from(count));
}

/// Records the batch size of an ingestion request.
#[inline]
pub fn record_events_ingest_batch_size(source_service: &str, size: usize) {
    histogram!(
        EVENTS_INGEST_BATCH_SIZE,
        "source_service" => source_service.to_string()
    )
    .record(size as f64);
}

/// Records an ingestion rate limit rejection.
#[inline]
pub fn record_events_ingest_rate_limited(source_service: &str) {
    counter!(
        EVENTS_INGEST_RATE_LIMITED_TOTAL,
        "source_service" => source_service.to_string()
    )
    .increment(1);
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
    counter!(
        EVENT_WRITES_TOTAL,
        "emission" => emission.to_string(),
        "scope" => scope.to_string(),
        "action" => action.to_string()
    )
    .increment(1);
}

/// Creates a timer for write operations.
pub fn write_timer() -> Timer {
    Timer::new(|secs| record_write(true, secs))
}

/// Creates a timer for read operations.
pub fn read_timer() -> Timer {
    Timer::new(|secs| record_read(true, secs))
}

/// Creates a timer for Raft apply operations.
pub fn raft_apply_timer() -> Timer {
    Timer::new(record_raft_apply_latency)
}

// ─── Events GC Metrics ──────────────────────────────────────

/// Total event entries deleted by garbage collection (counter).
const EVENTS_GC_ENTRIES_DELETED_TOTAL: &str = "ledger_events_gc_entries_deleted_total";

/// Duration of each GC cycle in seconds (histogram).
const EVENTS_GC_CYCLE_DURATION_SECONDS: &str = "ledger_events_gc_cycle_duration_seconds";

/// Total GC cycles executed (counter).
///
/// Labels: `result` = success | failure.
const EVENTS_GC_CYCLES_TOTAL: &str = "ledger_events_gc_cycles_total";

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

/// Number of nodes in each region (gauge).
///
/// Labels: `region` = region identifier string.
/// Emitted when membership changes. Protected regions emit a warning when
/// the count drops below
/// [`MIN_NODES_PER_PROTECTED_REGION`](inferadb_ledger_state::system::MIN_NODES_PER_PROTECTED_REGION)
/// + 1.
const REGION_NODE_COUNT: &str = "ledger_region_node_count";

/// Records the number of expired event entries deleted in a GC cycle.
#[inline]
pub fn record_events_gc_entries_deleted(count: u64) {
    counter!(EVENTS_GC_ENTRIES_DELETED_TOTAL).increment(count);
}

/// Records the duration of an events GC cycle.
#[inline]
pub fn record_events_gc_cycle_duration(duration_secs: f64) {
    histogram!(EVENTS_GC_CYCLE_DURATION_SECONDS).record(duration_secs);
}

/// Records a completed events GC cycle.
#[inline]
pub fn record_events_gc_cycle(result: &str) {
    counter!(
        EVENTS_GC_CYCLES_TOTAL,
        "result" => result.to_string()
    )
    .increment(1);
}

// ---------------------------------------------------------------------------
// Leader Transfer
// ---------------------------------------------------------------------------

/// Records a leader transfer attempt with its outcome and latency.
#[inline]
pub fn record_leader_transfer(success: bool, latency_secs: f64) {
    let status = if success { "success" } else { "failure" };
    counter!(LEADER_TRANSFERS_TOTAL, "status" => status).increment(1);
    histogram!(LEADER_TRANSFER_LATENCY, "status" => status).record(latency_secs);
}

/// Records a trigger election request received by this node.
#[inline]
pub fn record_trigger_election(accepted: bool) {
    let result = if accepted { "accepted" } else { "rejected" };
    counter!(TRIGGER_ELECTIONS_TOTAL, "result" => result).increment(1);
}

/// Records the current node count for a region and emits a warning if a
/// protected region is critically low (below `min_threshold + 1`).
#[inline]
pub fn record_region_node_count(region: &str, count: usize, is_protected: bool) {
    gauge!(REGION_NODE_COUNT, "region" => region.to_owned()).set(count as f64);
    if is_protected && count < MIN_NODES_PER_PROTECTED_REGION + 1 {
        tracing::warn!(
            region,
            count,
            min_required = MIN_NODES_PER_PROTECTED_REGION,
            "Protected region node count critically low"
        );
    }
}

// ---------------------------------------------------------------------------
// Onboarding
// ---------------------------------------------------------------------------

/// Total email verification initiations (counter).
///
/// Labels: `status` = success | failure.
const ONBOARDING_INITIATION_TOTAL: &str = "ledger_onboarding_initiation_total";

/// Total email verification code attempts (counter).
///
/// Labels: `status` = success | failure.
const ONBOARDING_VERIFICATION_TOTAL: &str = "ledger_onboarding_verification_total";

/// Total registration completions (counter).
///
/// Labels: `status` = success | failure.
const ONBOARDING_REGISTRATION_TOTAL: &str = "ledger_onboarding_registration_total";

/// Records an email verification initiation attempt.
#[inline]
pub fn record_onboarding_initiation(status: &str) {
    counter!(
        ONBOARDING_INITIATION_TOTAL,
        "status" => status.to_string()
    )
    .increment(1);
}

/// Records an email verification code attempt.
#[inline]
pub fn record_onboarding_verification(status: &str) {
    counter!(
        ONBOARDING_VERIFICATION_TOTAL,
        "status" => status.to_string()
    )
    .increment(1);
}

/// Records a registration completion attempt.
#[inline]
pub fn record_onboarding_registration(status: &str) {
    counter!(
        ONBOARDING_REGISTRATION_TOTAL,
        "status" => status.to_string()
    )
    .increment(1);
}

/// Total expired onboarding verification codes cleaned up by background GC (counter).
const ONBOARDING_VERIFICATION_CODES_GC_TOTAL: &str =
    "ledger_onboarding_verification_codes_gc_total";

/// Total expired onboarding accounts cleaned up by background GC (counter).
const ONBOARDING_ACCOUNTS_GC_TOTAL: &str = "ledger_onboarding_accounts_gc_total";

/// Records expired onboarding verification codes deleted.
#[inline]
pub fn record_onboarding_gc_codes(count: u64) {
    counter!(ONBOARDING_VERIFICATION_CODES_GC_TOTAL).increment(count);
}

/// Records expired onboarding accounts deleted.
#[inline]
pub fn record_onboarding_gc_accounts(count: u64) {
    counter!(ONBOARDING_ACCOUNTS_GC_TOTAL).increment(count);
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
    fn test_cross_region_metric_names() {
        assert!(CROSS_REGION_FORWARD_TOTAL.starts_with("ledger_"));
        assert!(CROSS_REGION_FORWARD_TOTAL.ends_with("_total"));
        assert!(CROSS_REGION_FORWARD_LATENCY.starts_with("ledger_"));
        assert!(CROSS_REGION_FORWARD_LATENCY.ends_with("_seconds"));
        assert!(DATA_RESIDENCY_VIOLATION_TOTAL.starts_with("ledger_"));
        assert!(DATA_RESIDENCY_VIOLATION_TOTAL.ends_with("_total"));
    }

    #[test]
    fn test_onboarding_metric_names() {
        assert!(ONBOARDING_INITIATION_TOTAL.starts_with("ledger_"));
        assert!(ONBOARDING_INITIATION_TOTAL.ends_with("_total"));
        assert!(ONBOARDING_VERIFICATION_TOTAL.starts_with("ledger_"));
        assert!(ONBOARDING_VERIFICATION_TOTAL.ends_with("_total"));
        assert!(ONBOARDING_REGISTRATION_TOTAL.starts_with("ledger_"));
        assert!(ONBOARDING_REGISTRATION_TOTAL.ends_with("_total"));
        assert!(ONBOARDING_VERIFICATION_CODES_GC_TOTAL.starts_with("ledger_"));
        assert!(ONBOARDING_VERIFICATION_CODES_GC_TOTAL.ends_with("_total"));
        assert!(ONBOARDING_ACCOUNTS_GC_TOTAL.starts_with("ledger_"));
        assert!(ONBOARDING_ACCOUNTS_GC_TOTAL.ends_with("_total"));
    }
}
