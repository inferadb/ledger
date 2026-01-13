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
const RAFT_PROPOSALS_TOTAL: &str = "ledger_raft_proposals_total";
const RAFT_PROPOSALS_PENDING: &str = "ledger_raft_proposals_pending";
const RAFT_APPLY_LATENCY: &str = "ledger_raft_apply_latency_seconds";
const RAFT_COMMIT_INDEX: &str = "ledger_raft_commit_index";
const RAFT_TERM: &str = "ledger_raft_term";
const RAFT_LEADER: &str = "ledger_raft_is_leader";

// State machine metrics
const STATE_ROOT_COMPUTATIONS: &str = "ledger_state_root_computations_total";
const STATE_ROOT_LATENCY: &str = "ledger_state_root_latency_seconds";
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

// Recovery metrics
const RECOVERY_SUCCESS_TOTAL: &str = "ledger_recovery_success_total";
const RECOVERY_FAILURE_TOTAL: &str = "ledger_recovery_failure_total";
const DETERMINISM_BUG_TOTAL: &str = "ledger_determinism_bug_total";

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

/// Record a rate limit exceeded event.
#[inline]
pub fn record_rate_limit_exceeded(namespace_id: i64) {
    counter!(RATE_LIMIT_EXCEEDED, "namespace_id" => namespace_id.to_string()).increment(1);
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
#[inline]
pub fn record_grpc_request(service: &str, method: &str, status: &str, latency_secs: f64) {
    counter!(GRPC_REQUESTS_TOTAL,
        "service" => service.to_string(),
        "method" => method.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(GRPC_REQUEST_LATENCY,
        "service" => service.to_string(),
        "method" => method.to_string()
    )
    .record(latency_secs);
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
        Self {
            start: Instant::now(),
            record_fn: Some(Box::new(record_fn)),
        }
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
        record_grpc_request("WriteService", "write", "OK", 0.001);
    }
}
