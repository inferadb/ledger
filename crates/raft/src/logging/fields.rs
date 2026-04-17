//! Shared field-name constants for telemetry emission.
//!
//! All canonical log line fields, metric labels, and event detail keys
//! reference these constants instead of hardcoding string literals.
//! This prevents naming drift between telemetry backends.

// ── Identity ─────────────────────────────────────────────────────

/// Unique request identifier (UUID v4).
pub const REQUEST_ID: &str = "request_id";

/// gRPC service name (e.g. `"WriteService"`).
pub const SERVICE: &str = "service";

/// gRPC method name (e.g. `"write"`).
pub const METHOD: &str = "method";

/// Idempotency client identifier.
pub const CLIENT_ID: &str = "client_id";

/// Per-client sequence number.
pub const SEQUENCE: &str = "sequence";

// ── Scope ────────────────────────────────────────────────────────

/// Organization slug (external Snowflake ID).
pub const ORGANIZATION: &str = "organization";

/// Vault slug (external Snowflake ID).
pub const VAULT: &str = "vault";

/// Caller user slug (external Snowflake ID).
pub const CALLER: &str = "caller";

// ── System ───────────────────────────────────────────────────────

/// Raft node identifier.
pub const NODE_ID: &str = "node_id";

/// Whether this node is the current Raft leader.
pub const IS_LEADER: &str = "is_leader";

/// Current Raft term.
pub const RAFT_TERM: &str = "raft_term";

/// Data region identifier.
pub const REGION: &str = "region";

// ── Outcome ──────────────────────────────────────────────────────

/// Request outcome (success, error, cached, rate_limited, precondition_failed).
pub const OUTCOME: &str = "outcome";

/// Error code string.
pub const ERROR_CODE: &str = "error_code";

/// Human-readable error message.
pub const ERROR_MESSAGE: &str = "error_message";

/// Key that caused a precondition failure.
pub const ERROR_KEY: &str = "error_key";

// ── Timing ───────────────────────────────────────────────────────

/// Total request duration in milliseconds.
pub const DURATION_MS: &str = "duration_ms";

/// Time spent in Raft consensus in milliseconds.
pub const RAFT_LATENCY_MS: &str = "raft_latency_ms";

/// Time spent in the storage layer in milliseconds.
pub const STORAGE_LATENCY_MS: &str = "storage_latency_ms";

// ── Tracing (W3C Trace Context) ──────────────────────────────────

/// W3C trace ID (32 hex characters).
pub const TRACE_ID: &str = "trace_id";

/// Span ID (16 hex characters).
pub const SPAN_ID: &str = "span_id";

/// Parent span ID (16 hex characters).
pub const PARENT_SPAN_ID: &str = "parent_span_id";

/// W3C trace flags.
pub const TRACE_FLAGS: &str = "trace_flags";

// ── Write operation ──────────────────────────────────────────────

/// Number of operations in a write request.
pub const OPERATIONS_COUNT: &str = "operations_count";

/// Types of operations in a write request.
pub const OPERATION_TYPES: &str = "operation_types";

/// Whether a transaction proof was requested.
pub const INCLUDE_TX_PROOF: &str = "include_tx_proof";

/// Whether the request was served from idempotency cache.
pub const IDEMPOTENCY_HIT: &str = "idempotency_hit";

/// Whether the write was batch-coalesced.
pub const BATCH_COALESCED: &str = "batch_coalesced";

/// Number of operations in a coalesced batch.
pub const BATCH_SIZE: &str = "batch_size";

/// Condition type for CAS operations.
pub const CONDITION_TYPE: &str = "condition_type";

/// Committed block height.
pub const BLOCK_HEIGHT: &str = "block_height";

/// Block hash (truncated hex).
pub const BLOCK_HASH: &str = "block_hash";

/// Merkle state root (truncated hex).
pub const STATE_ROOT: &str = "state_root";

// ── Read operation ───────────────────────────────────────────────

/// Key being read.
pub const KEY: &str = "key";

/// Number of keys in a batch read.
pub const KEYS_COUNT: &str = "keys_count";

/// Number of keys found in a batch read.
pub const FOUND_COUNT: &str = "found_count";

/// Consistency level for reads.
pub const CONSISTENCY: &str = "consistency";

/// Historical read height.
pub const AT_HEIGHT: &str = "at_height";

/// Whether a Merkle proof was requested.
pub const INCLUDE_PROOF: &str = "include_proof";

/// Size of the Merkle proof in bytes.
pub const PROOF_SIZE_BYTES: &str = "proof_size_bytes";

/// Whether the key was found.
pub const FOUND: &str = "found";

/// Size of the value in bytes.
pub const VALUE_SIZE_BYTES: &str = "value_size_bytes";

// ── Admin operation ──────────────────────────────────────────────

/// Admin action name (e.g. `"create_organization"`).
pub const ADMIN_ACTION: &str = "admin_action";

/// Retention mode for vault creation.
pub const RETENTION_MODE: &str = "retention_mode";

/// Whether force mode was used for recovery.
pub const RECOVERY_FORCE: &str = "recovery_force";

// ── I/O metrics ──────────────────────────────────────────────────

/// Bytes read during this request.
pub const BYTES_READ: &str = "bytes_read";

/// Bytes written during this request.
pub const BYTES_WRITTEN: &str = "bytes_written";

/// Number of Raft round trips.
pub const RAFT_ROUND_TRIPS: &str = "raft_round_trips";

// ── Client transport metadata ────────────────────────────────────

/// SDK version string reported by the client.
pub const SDK_VERSION: &str = "sdk_version";

/// Source IP address of the client connection.
pub const SOURCE_IP: &str = "source_ip";

// ── Metric-only labels ───────────────────────────────────────────

/// gRPC status code string (e.g. `"OK"`, `"Internal"`).
pub const STATUS: &str = "status";

/// Error classification for SLI tracking.
pub const ERROR_CLASS: &str = "error_class";

/// Internal organization ID (for metric labels).
pub const ORGANIZATION_ID: &str = "organization_id";

/// Internal vault ID (for metric labels).
pub const VAULT_ID: &str = "vault_id";

/// Storage operation type (`"read"` or `"write"`).
pub const OP: &str = "op";

/// Verified read flag.
pub const VERIFIED: &str = "verified";

/// Background job name.
pub const JOB: &str = "job";

/// Job cycle result (`"success"` or `"failure"`).
pub const RESULT: &str = "result";

/// Rate limit level.
pub const LEVEL: &str = "level";

/// Reason for an event (e.g. rate limit reason).
pub const REASON: &str = "reason";

/// Recovery attempt number.
pub const ATTEMPT: &str = "attempt";

/// Vault health state string.
pub const STATE: &str = "state";

/// Voter ID for learner refresh errors.
pub const VOTER_ID: &str = "voter_id";

/// Error type classification.
pub const ERROR_TYPE: &str = "error_type";

/// Serialization operation label.
pub const OPERATION: &str = "operation";

/// Raft log entry type.
pub const ENTRY_TYPE: &str = "entry_type";

/// Serialization direction (`"encode"` or `"decode"`).
pub const DIRECTION: &str = "direction";

/// Hot key hash (hex-encoded).
pub const KEY_HASH: &str = "key_hash";

/// Operations per second rate.
pub const OPS_PER_SEC: &str = "ops_per_sec";

/// B-tree table name label.
pub const TABLE: &str = "table";

/// Metric name label (for cardinality overflow tracking).
pub const METRIC_NAME: &str = "metric_name";

/// Event source service label.
pub const SOURCE_SERVICE: &str = "source_service";

/// Event emission phase label.
pub const EMISSION: &str = "emission";

/// Event scope label (`"system"` or `"organization"`).
pub const SCOPE: &str = "scope";

/// Event action label.
pub const ACTION: &str = "action";

/// Peer node ID label.
pub const PEER: &str = "peer";

/// Connection registry event type.
pub const EVENT: &str = "event";

/// Operation kind label (e.g. `"merge"`, `"free"`, `"split"` for B-tree ops).
pub const KIND: &str = "kind";

/// Purge tier label (`"global"` or `"regional"`).
pub const TIER: &str = "tier";

/// Whether retry attempts were exhausted (`"true"` or `"false"`).
pub const EXHAUSTED: &str = "exhausted";

/// Onboarding stage label (`"initiate"`, `"verify"`, `"register"`).
pub const STAGE: &str = "stage";

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashSet;

    /// Collects all field constant values defined in the parent module.
    fn all_field_values() -> Vec<(&'static str, &'static str)> {
        vec![
            ("REQUEST_ID", super::REQUEST_ID),
            ("SERVICE", super::SERVICE),
            ("METHOD", super::METHOD),
            ("CLIENT_ID", super::CLIENT_ID),
            ("SEQUENCE", super::SEQUENCE),
            ("ORGANIZATION", super::ORGANIZATION),
            ("VAULT", super::VAULT),
            ("CALLER", super::CALLER),
            ("NODE_ID", super::NODE_ID),
            ("IS_LEADER", super::IS_LEADER),
            ("RAFT_TERM", super::RAFT_TERM),
            ("REGION", super::REGION),
            ("OUTCOME", super::OUTCOME),
            ("ERROR_CODE", super::ERROR_CODE),
            ("ERROR_MESSAGE", super::ERROR_MESSAGE),
            ("ERROR_KEY", super::ERROR_KEY),
            ("DURATION_MS", super::DURATION_MS),
            ("RAFT_LATENCY_MS", super::RAFT_LATENCY_MS),
            ("STORAGE_LATENCY_MS", super::STORAGE_LATENCY_MS),
            ("TRACE_ID", super::TRACE_ID),
            ("SPAN_ID", super::SPAN_ID),
            ("PARENT_SPAN_ID", super::PARENT_SPAN_ID),
            ("TRACE_FLAGS", super::TRACE_FLAGS),
            ("OPERATIONS_COUNT", super::OPERATIONS_COUNT),
            ("OPERATION_TYPES", super::OPERATION_TYPES),
            ("INCLUDE_TX_PROOF", super::INCLUDE_TX_PROOF),
            ("IDEMPOTENCY_HIT", super::IDEMPOTENCY_HIT),
            ("BATCH_COALESCED", super::BATCH_COALESCED),
            ("BATCH_SIZE", super::BATCH_SIZE),
            ("CONDITION_TYPE", super::CONDITION_TYPE),
            ("BLOCK_HEIGHT", super::BLOCK_HEIGHT),
            ("BLOCK_HASH", super::BLOCK_HASH),
            ("STATE_ROOT", super::STATE_ROOT),
            ("KEY", super::KEY),
            ("KEYS_COUNT", super::KEYS_COUNT),
            ("FOUND_COUNT", super::FOUND_COUNT),
            ("CONSISTENCY", super::CONSISTENCY),
            ("AT_HEIGHT", super::AT_HEIGHT),
            ("INCLUDE_PROOF", super::INCLUDE_PROOF),
            ("PROOF_SIZE_BYTES", super::PROOF_SIZE_BYTES),
            ("FOUND", super::FOUND),
            ("VALUE_SIZE_BYTES", super::VALUE_SIZE_BYTES),
            ("ADMIN_ACTION", super::ADMIN_ACTION),
            ("RETENTION_MODE", super::RETENTION_MODE),
            ("RECOVERY_FORCE", super::RECOVERY_FORCE),
            ("BYTES_READ", super::BYTES_READ),
            ("BYTES_WRITTEN", super::BYTES_WRITTEN),
            ("RAFT_ROUND_TRIPS", super::RAFT_ROUND_TRIPS),
            ("SDK_VERSION", super::SDK_VERSION),
            ("SOURCE_IP", super::SOURCE_IP),
            ("STATUS", super::STATUS),
            ("ERROR_CLASS", super::ERROR_CLASS),
            ("ORGANIZATION_ID", super::ORGANIZATION_ID),
            ("VAULT_ID", super::VAULT_ID),
            ("OP", super::OP),
            ("VERIFIED", super::VERIFIED),
            ("JOB", super::JOB),
            ("RESULT", super::RESULT),
            ("LEVEL", super::LEVEL),
            ("REASON", super::REASON),
            ("ATTEMPT", super::ATTEMPT),
            ("STATE", super::STATE),
            ("VOTER_ID", super::VOTER_ID),
            ("ERROR_TYPE", super::ERROR_TYPE),
            ("OPERATION", super::OPERATION),
            ("ENTRY_TYPE", super::ENTRY_TYPE),
            ("DIRECTION", super::DIRECTION),
            ("KEY_HASH", super::KEY_HASH),
            ("OPS_PER_SEC", super::OPS_PER_SEC),
            ("TABLE", super::TABLE),
            ("METRIC_NAME", super::METRIC_NAME),
            ("SOURCE_SERVICE", super::SOURCE_SERVICE),
            ("EMISSION", super::EMISSION),
            ("SCOPE", super::SCOPE),
            ("ACTION", super::ACTION),
            ("PEER", super::PEER),
            ("EVENT", super::EVENT),
            ("KIND", super::KIND),
            ("TIER", super::TIER),
            ("EXHAUSTED", super::EXHAUSTED),
            ("STAGE", super::STAGE),
        ]
    }

    #[test]
    fn all_constants_are_non_empty() {
        for (name, value) in all_field_values() {
            assert!(!value.is_empty(), "Field constant {name} must not be empty");
        }
    }

    #[test]
    fn all_constants_are_snake_case() {
        for (name, value) in all_field_values() {
            assert!(
                !value.contains(|c: char| c.is_uppercase()),
                "Field constant {name} = {value:?} contains uppercase characters"
            );
            assert!(
                !value.contains('-'),
                "Field constant {name} = {value:?} contains hyphens (use snake_case)"
            );
        }
    }

    #[test]
    fn no_duplicate_values() {
        let fields = all_field_values();
        let mut seen = HashSet::new();
        for (name, value) in &fields {
            assert!(seen.insert(value), "Duplicate field value {value:?} found in constant {name}");
        }
    }
}
