//! Event logging domain types for organization-scoped audit trails.
//!
//! Provides the core types for the two-tier event logging system:
//! - [`EventEntry`] — a structured audit event following the canonical log line pattern
//! - [`EventScope`] — system or organization level scoping
//! - [`EventAction`] — enumeration of all trackable actions with compile-time scope routing
//! - [`EventOutcome`] — success, failure, or denial outcomes
//! - [`EventEmission`] — apply-phase (deterministic) or handler-phase (node-local)
//! - [`EventConfig`] — configuration for event retention, limits, and feature flags
//!
//! Each [`EventAction`] variant maps to exactly one [`EventScope`] via
//! [`EventAction::scope()`], preventing dual-writing at compile time.
//! The match is exhaustive — adding a new variant requires assigning a scope.

// The schemars `JsonSchema` derive macro internally uses `.unwrap()` in its
// `json_schema!` and `json_internal!` expansions. Allow `disallowed_methods`
// at the module level since the derive expansion is uncontrollable.
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::types::OrganizationId;

/// A structured audit event for persistent, queryable audit trails.
///
/// Follows the canonical log line ("wide event") pattern — a single structured
/// record with rich contextual fields. Events are written to the `Events` B+ tree
/// table and queryable via the `EventsService` gRPC API.
///
/// # Field Ordering
///
/// Fields are ordered for thin deserialization efficiency (postcard serializes
/// sequentially by declaration order):
///
/// 1. `emission` — [`EmissionMeta`] reads only this field to filter handler-phase events in
///    [`EventStore::scan_apply_phase`] without full deserialization.
/// 2. `expires_at` — [`EventMeta`] reads `emission` + `expires_at` for GC checks.
///
/// **Do not reorder fields** without updating `EmissionMeta`, `EventMeta`, and
/// the `event_entry_serialization_byte_pinning` test.
///
/// # Serialization
///
/// Uses postcard for storage (compact binary, position-dependent) and serde
/// for JSON API responses. Optional fields have `#[serde(default)]` for
/// forward-compatible JSON deserialization. Postcard serialization order
/// matches struct declaration order — field reorders are breaking changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEntry {
    /// `ApplyPhase` or `HandlerPhase { node_id }`.
    ///
    /// **Must be field 1** — [`EmissionMeta`] thin-deserializes only this field
    /// to filter handler-phase events in snapshot collection.
    pub emission: EventEmission,

    /// Unix timestamp for TTL expiry (0 = no expiry).
    ///
    /// **Must be field 2** — [`EventMeta`] thin-deserializes `emission` +
    /// `expires_at` for GC expiry checks.
    pub expires_at: u64,

    /// Deterministic UUID v5 (apply-phase) or random UUID v4 (handler-phase).
    pub event_id: [u8; 16],

    /// Originating service (`"ledger"`, `"engine"`, or `"control"`).
    ///
    /// Must be `String` (not `&'static str`) for postcard deserialization
    /// round-trip compatibility.
    #[serde(default)]
    pub source_service: String,

    /// Hierarchical dot-separated type (e.g., `ledger.vault.created`).
    #[serde(default)]
    pub event_type: String,

    /// Block timestamp (apply-phase) or wall clock (handler-phase).
    pub timestamp: DateTime<Utc>,

    /// System or organization scope.
    pub scope: EventScope,

    /// What happened — used for internal routing and scope enforcement.
    pub action: EventAction,

    /// Who performed the action (server-assigned actor, never client-controlled).
    pub principal: String,

    /// Owning organization (0 for system events).
    pub organization_id: OrganizationId,

    /// External organization slug (for API responses).
    #[serde(default)]
    pub organization_slug: Option<u64>,

    /// Vault context (when applicable).
    #[serde(default)]
    pub vault_slug: Option<u64>,

    /// Success, failure, or denial.
    pub outcome: EventOutcome,

    /// Action-specific key-value context (bounded by config).
    #[serde(default)]
    pub details: BTreeMap<String, String>,

    /// Reference to blockchain block (for committed writes).
    #[serde(default)]
    pub block_height: Option<u64>,

    /// Distributed tracing correlation (W3C Trace Context).
    #[serde(default)]
    pub trace_id: Option<String>,

    /// Business-level correlation for multi-step operations.
    #[serde(default)]
    pub correlation_id: Option<String>,

    /// Number of operations (for write actions).
    #[serde(default)]
    pub operations_count: Option<u32>,
}

/// Thin wrapper for GC-efficient expiry checks.
///
/// Deserializes the first two fields of [`EventEntry`] (postcard sequential
/// deserialization): `emission` then `expires_at`. The GC reads `expires_at`
/// to check expiry without full decode overhead.
///
/// See also [`EmissionMeta`] which reads only `emission` (field 1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMeta {
    /// Emission phase (field 1 of `EventEntry`).
    pub emission: EventEmission,

    /// Unix timestamp for TTL expiry (0 = no expiry, field 2 of `EventEntry`).
    pub expires_at: u64,
}

/// Thin deserialization target for [`EventStore::scan_apply_phase`].
///
/// Reads only the `emission` field (first in serialization order) to filter
/// handler-phase events without full [`EventEntry`] deserialization.
/// Follows the same pattern as [`EventMeta`] used by GC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmissionMeta {
    /// Emission phase (field 1 of `EventEntry`).
    pub emission: EventEmission,
}

/// Event scope — determines which log an event belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventScope {
    /// Cluster-wide administrative events (org_id = 0).
    System,
    /// Per-organization tenant events.
    Organization,
}

impl EventScope {
    /// Returns a snake_case string for metrics labels.
    pub const fn as_str(&self) -> &'static str {
        match self {
            EventScope::System => "system",
            EventScope::Organization => "organization",
        }
    }
}

/// Emission path indicating the consistency model of an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventEmission {
    /// Deterministic, Raft-replicated — identical on all nodes.
    ApplyPhase,
    /// Node-local, best-effort — exists only on the handling node.
    HandlerPhase {
        /// The node that generated this event.
        node_id: u64,
    },
}

/// Outcome of an audited operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventOutcome {
    /// Operation completed successfully.
    Success,
    /// Operation failed with an error.
    Failed {
        /// Error code or category.
        code: String,
        /// Human-readable error description.
        detail: String,
    },
    /// Operation was denied (rate limited, unauthorized, etc.).
    Denied {
        /// Reason for denial.
        reason: String,
    },
}

/// All trackable audit actions.
///
/// Each variant maps to exactly one [`EventScope`] via [`EventAction::scope()`].
/// The match is exhaustive — adding a new variant is a compile error that
/// forces you to assign a scope, event type, and metrics label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventAction {
    // ── System scope ────────────────────────────────────────
    /// Organization created.
    OrganizationCreated,
    /// Organization deleted.
    OrganizationDeleted,
    /// Organization suspended.
    OrganizationSuspended,
    /// Organization resumed.
    OrganizationResumed,
    /// User account created.
    UserCreated,
    /// User account deleted.
    UserDeleted,
    /// Data migration started.
    MigrationStarted,
    /// Data migration completed.
    MigrationCompleted,
    /// Node joined the cluster.
    NodeJoinedCluster,
    /// Node left the cluster.
    NodeLeftCluster,
    /// Organization routing updated.
    RoutingUpdated,
    /// Server configuration changed.
    ConfigurationChanged,
    /// Backup created.
    BackupCreated,
    /// Backup restored.
    BackupRestored,
    /// Snapshot created.
    SnapshotCreated,

    // ── Organization scope ──────────────────────────────────
    /// Vault created.
    VaultCreated,
    /// Vault deleted.
    VaultDeleted,
    /// Single write transaction committed.
    WriteCommitted,
    /// Batch write committed.
    BatchWriteCommitted,
    /// Entity expired via TTL GC.
    EntityExpired,
    /// Vault health status updated.
    VaultHealthUpdated,
    /// Integrity check completed.
    IntegrityChecked,
    /// Vault recovery completed.
    VaultRecovered,
    /// Request rate limited.
    RequestRateLimited,
    /// Request validation failed.
    RequestValidationFailed,
    /// Quota exceeded.
    QuotaExceeded,
}

impl EventAction {
    /// Returns the fixed scope for this action variant.
    ///
    /// Exhaustive match — adding a new variant without a scope assignment
    /// produces a compile error.
    pub const fn scope(&self) -> EventScope {
        match self {
            // System scope
            EventAction::OrganizationCreated
            | EventAction::OrganizationDeleted
            | EventAction::OrganizationSuspended
            | EventAction::OrganizationResumed
            | EventAction::UserCreated
            | EventAction::UserDeleted
            | EventAction::MigrationStarted
            | EventAction::MigrationCompleted
            | EventAction::NodeJoinedCluster
            | EventAction::NodeLeftCluster
            | EventAction::RoutingUpdated
            | EventAction::ConfigurationChanged
            | EventAction::BackupCreated
            | EventAction::BackupRestored
            | EventAction::SnapshotCreated => EventScope::System,

            // Organization scope
            EventAction::VaultCreated
            | EventAction::VaultDeleted
            | EventAction::WriteCommitted
            | EventAction::BatchWriteCommitted
            | EventAction::EntityExpired
            | EventAction::VaultHealthUpdated
            | EventAction::IntegrityChecked
            | EventAction::VaultRecovered
            | EventAction::RequestRateLimited
            | EventAction::RequestValidationFailed
            | EventAction::QuotaExceeded => EventScope::Organization,
        }
    }

    /// Returns the hierarchical dot-separated event type string.
    ///
    /// Format: `ledger.<domain>.<action>`. Exhaustive match — adding a new
    /// variant without a type string produces a compile error.
    pub const fn event_type(&self) -> &'static str {
        match self {
            // System scope
            EventAction::OrganizationCreated => "ledger.organization.created",
            EventAction::OrganizationDeleted => "ledger.organization.deleted",
            EventAction::OrganizationSuspended => "ledger.organization.suspended",
            EventAction::OrganizationResumed => "ledger.organization.resumed",
            EventAction::UserCreated => "ledger.user.created",
            EventAction::UserDeleted => "ledger.user.deleted",
            EventAction::MigrationStarted => "ledger.migration.started",
            EventAction::MigrationCompleted => "ledger.migration.completed",
            EventAction::NodeJoinedCluster => "ledger.node.joined",
            EventAction::NodeLeftCluster => "ledger.node.left",
            EventAction::RoutingUpdated => "ledger.routing.updated",
            EventAction::ConfigurationChanged => "ledger.config.changed",
            EventAction::BackupCreated => "ledger.backup.created",
            EventAction::BackupRestored => "ledger.backup.restored",
            EventAction::SnapshotCreated => "ledger.snapshot.created",

            // Organization scope
            EventAction::VaultCreated => "ledger.vault.created",
            EventAction::VaultDeleted => "ledger.vault.deleted",
            EventAction::WriteCommitted => "ledger.write.committed",
            EventAction::BatchWriteCommitted => "ledger.write.batch_committed",
            EventAction::EntityExpired => "ledger.entity.expired",
            EventAction::VaultHealthUpdated => "ledger.vault.health_updated",
            EventAction::IntegrityChecked => "ledger.integrity.checked",
            EventAction::VaultRecovered => "ledger.vault.recovered",
            EventAction::RequestRateLimited => "ledger.request.rate_limited",
            EventAction::RequestValidationFailed => "ledger.request.validation_failed",
            EventAction::QuotaExceeded => "ledger.request.quota_exceeded",
        }
    }

    /// Returns a snake_case string for metrics labels.
    ///
    /// Exhaustive match — adding a new variant without a label produces
    /// a compile error.
    pub const fn as_str(&self) -> &'static str {
        match self {
            EventAction::OrganizationCreated => "organization_created",
            EventAction::OrganizationDeleted => "organization_deleted",
            EventAction::OrganizationSuspended => "organization_suspended",
            EventAction::OrganizationResumed => "organization_resumed",
            EventAction::UserCreated => "user_created",
            EventAction::UserDeleted => "user_deleted",
            EventAction::MigrationStarted => "migration_started",
            EventAction::MigrationCompleted => "migration_completed",
            EventAction::NodeJoinedCluster => "node_joined_cluster",
            EventAction::NodeLeftCluster => "node_left_cluster",
            EventAction::RoutingUpdated => "routing_updated",
            EventAction::ConfigurationChanged => "configuration_changed",
            EventAction::BackupCreated => "backup_created",
            EventAction::BackupRestored => "backup_restored",
            EventAction::SnapshotCreated => "snapshot_created",
            EventAction::VaultCreated => "vault_created",
            EventAction::VaultDeleted => "vault_deleted",
            EventAction::WriteCommitted => "write_committed",
            EventAction::BatchWriteCommitted => "batch_write_committed",
            EventAction::EntityExpired => "entity_expired",
            EventAction::VaultHealthUpdated => "vault_health_updated",
            EventAction::IntegrityChecked => "integrity_checked",
            EventAction::VaultRecovered => "vault_recovered",
            EventAction::RequestRateLimited => "request_rate_limited",
            EventAction::RequestValidationFailed => "request_validation_failed",
            EventAction::QuotaExceeded => "quota_exceeded",
        }
    }

    /// Returns all variants of `EventAction` for exhaustive testing.
    pub const ALL: &'static [EventAction] = &[
        EventAction::OrganizationCreated,
        EventAction::OrganizationDeleted,
        EventAction::OrganizationSuspended,
        EventAction::OrganizationResumed,
        EventAction::UserCreated,
        EventAction::UserDeleted,
        EventAction::MigrationStarted,
        EventAction::MigrationCompleted,
        EventAction::NodeJoinedCluster,
        EventAction::NodeLeftCluster,
        EventAction::RoutingUpdated,
        EventAction::ConfigurationChanged,
        EventAction::BackupCreated,
        EventAction::BackupRestored,
        EventAction::SnapshotCreated,
        EventAction::VaultCreated,
        EventAction::VaultDeleted,
        EventAction::WriteCommitted,
        EventAction::BatchWriteCommitted,
        EventAction::EntityExpired,
        EventAction::VaultHealthUpdated,
        EventAction::IntegrityChecked,
        EventAction::VaultRecovered,
        EventAction::RequestRateLimited,
        EventAction::RequestValidationFailed,
        EventAction::QuotaExceeded,
    ];
}

impl std::str::FromStr for EventAction {
    type Err = String;

    /// Parses an `EventAction` from its snake_case string representation.
    ///
    /// Inverse of [`EventAction::as_str()`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for action in Self::ALL {
            if action.as_str() == s {
                return Ok(*action);
            }
        }
        Err(format!("unknown event action: {s}"))
    }
}

/// Configuration for external event ingestion via `IngestEvents` RPC.
///
/// Controls which external services can write events into the events table,
/// batch size limits, and per-source rate limiting.
///
/// # Defaults
///
/// - `ingest_enabled`: `true`
/// - `allowed_sources`: `["engine", "control"]`
/// - `max_ingest_batch_size`: `500`
/// - `ingest_rate_limit_per_source`: `10_000`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct IngestionConfig {
    /// Master switch for external event ingestion.
    #[serde(default = "default_enabled")]
    pub ingest_enabled: bool,

    /// Allow-list of `source_service` values accepted by `IngestEvents`.
    #[serde(default = "default_allowed_sources")]
    pub allowed_sources: Vec<String>,

    /// Maximum events per `IngestEvents` call (1..=10_000).
    #[serde(default = "default_max_ingest_batch_size")]
    pub max_ingest_batch_size: u32,

    /// Events per second per source service (1..=1_000_000).
    #[serde(default = "default_ingest_rate_limit_per_source")]
    pub ingest_rate_limit_per_source: u32,
}

fn default_allowed_sources() -> Vec<String> {
    vec!["engine".to_string(), "control".to_string()]
}

fn default_max_ingest_batch_size() -> u32 {
    500
}

fn default_ingest_rate_limit_per_source() -> u32 {
    10_000
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            ingest_enabled: default_enabled(),
            allowed_sources: default_allowed_sources(),
            max_ingest_batch_size: default_max_ingest_batch_size(),
            ingest_rate_limit_per_source: default_ingest_rate_limit_per_source(),
        }
    }
}

impl IngestionConfig {
    /// Validates ingestion configuration values.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`](crate::config::ConfigError) if:
    /// - `max_ingest_batch_size` is outside 1..=10_000
    /// - `ingest_rate_limit_per_source` is 0
    /// - `allowed_sources` contains empty strings
    pub fn validate(&self) -> Result<(), crate::config::ConfigError> {
        use crate::config::ConfigError;

        if !(1..=10_000).contains(&self.max_ingest_batch_size) {
            return Err(ConfigError::Validation {
                message: format!(
                    "max_ingest_batch_size must be 1..=10_000, got {}",
                    self.max_ingest_batch_size
                ),
            });
        }
        if !(1..=1_000_000).contains(&self.ingest_rate_limit_per_source) {
            return Err(ConfigError::Validation {
                message: format!(
                    "ingest_rate_limit_per_source must be 1..=1_000_000, got {}",
                    self.ingest_rate_limit_per_source
                ),
            });
        }
        for source in &self.allowed_sources {
            if source.is_empty() {
                return Err(ConfigError::Validation {
                    message: "allowed_sources must not contain empty strings".to_string(),
                });
            }
        }
        Ok(())
    }
}

/// Configuration for the event logging system.
///
/// Controls retention, size limits, per-scope enable flags, snapshot limits,
/// and external ingestion settings.
///
/// # Defaults
///
/// - `enabled`: `true`
/// - `default_ttl_days`: `90`
/// - `max_details_size_bytes`: `4096`
/// - `system_log_enabled`: `true`
/// - `organization_log_enabled`: `true`
/// - `max_snapshot_events`: `100_000`
/// - `ingestion`: default `IngestionConfig`
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::events::EventConfig;
/// let config = EventConfig::builder()
///     .default_ttl_days(30)
///     .max_details_size_bytes(8192)
///     .build()
///     .expect("valid event config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EventConfig {
    /// Master switch for event logging.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Default TTL in days for event entries (1..=3650).
    #[serde(default = "default_ttl_days")]
    pub default_ttl_days: u32,

    /// Maximum size of the details map in bytes (256..=65536).
    #[serde(default = "default_max_details_size_bytes")]
    pub max_details_size_bytes: usize,

    /// Enable system-level event logging.
    #[serde(default = "default_enabled")]
    pub system_log_enabled: bool,

    /// Enable organization-level event logging.
    #[serde(default = "default_enabled")]
    pub organization_log_enabled: bool,

    /// Maximum apply-phase events included in Raft snapshots.
    ///
    /// Prevents unbounded snapshot growth for high-throughput clusters.
    /// When exceeded, oldest events are omitted (they would be expired
    /// by GC shortly after the new follower joins).
    #[serde(default = "default_max_snapshot_events")]
    pub max_snapshot_events: usize,

    /// External event ingestion configuration.
    #[serde(default)]
    pub ingestion: IngestionConfig,
}

fn default_enabled() -> bool {
    true
}

fn default_ttl_days() -> u32 {
    90
}

fn default_max_details_size_bytes() -> usize {
    4096
}

fn default_max_snapshot_events() -> usize {
    100_000
}

impl Default for EventConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            default_ttl_days: default_ttl_days(),
            max_details_size_bytes: default_max_details_size_bytes(),
            system_log_enabled: default_enabled(),
            organization_log_enabled: default_enabled(),
            max_snapshot_events: default_max_snapshot_events(),
            ingestion: IngestionConfig::default(),
        }
    }
}

#[bon::bon]
impl EventConfig {
    /// Creates a new `EventConfig` with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`](crate::config::ConfigError) if:
    /// - `default_ttl_days` is outside 1..=3650
    /// - `max_details_size_bytes` is outside 256..=65536
    /// - `max_snapshot_events` is 0
    /// - `ingestion` config is invalid
    #[builder]
    pub fn new(
        enabled: Option<bool>,
        default_ttl_days: Option<u32>,
        max_details_size_bytes: Option<usize>,
        system_log_enabled: Option<bool>,
        organization_log_enabled: Option<bool>,
        max_snapshot_events: Option<usize>,
        ingestion: Option<IngestionConfig>,
    ) -> Result<Self, crate::config::ConfigError> {
        let config = Self {
            enabled: enabled.unwrap_or_else(default_enabled),
            default_ttl_days: default_ttl_days.unwrap_or_else(self::default_ttl_days),
            max_details_size_bytes: max_details_size_bytes
                .unwrap_or_else(self::default_max_details_size_bytes),
            system_log_enabled: system_log_enabled.unwrap_or_else(default_enabled),
            organization_log_enabled: organization_log_enabled.unwrap_or_else(default_enabled),
            max_snapshot_events: max_snapshot_events
                .unwrap_or_else(self::default_max_snapshot_events),
            ingestion: ingestion.unwrap_or_default(),
        };
        config.validate()?;
        Ok(config)
    }
}

impl EventConfig {
    /// Validates configuration values are within acceptable ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`](crate::config::ConfigError) if any value is invalid.
    pub fn validate(&self) -> Result<(), crate::config::ConfigError> {
        use crate::config::ConfigError;

        if !(1..=3650).contains(&self.default_ttl_days) {
            return Err(ConfigError::Validation {
                message: format!(
                    "default_ttl_days must be 1..=3650, got {}",
                    self.default_ttl_days
                ),
            });
        }
        if !(256..=65536).contains(&self.max_details_size_bytes) {
            return Err(ConfigError::Validation {
                message: format!(
                    "max_details_size_bytes must be 256..=65536, got {}",
                    self.max_details_size_bytes
                ),
            });
        }
        if self.max_snapshot_events == 0 {
            return Err(ConfigError::Validation {
                message: "max_snapshot_events must be at least 1".to_string(),
            });
        }
        self.ingestion.validate()?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // ── EventScope ──────────────────────────────────────────

    #[test]
    fn event_scope_serde_roundtrip() {
        for scope in [EventScope::System, EventScope::Organization] {
            let json = serde_json::to_string(&scope).expect("serialize");
            let back: EventScope = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(scope, back);
        }
    }

    // ── EventAction ─────────────────────────────────────────

    #[test]
    fn event_action_scope_covers_all_variants() {
        // Verifies exhaustive match — if a variant is missing, this
        // won't compile (the match in scope() would fail first).
        for action in EventAction::ALL {
            let _scope = action.scope();
        }
    }

    #[test]
    fn event_action_event_type_covers_all_variants() {
        for action in EventAction::ALL {
            let et = action.event_type();
            assert!(et.starts_with("ledger."), "event type must start with 'ledger.': {et}");
            // Must have at least 3 segments: ledger.domain.action
            assert!(et.split('.').count() >= 3, "event type must have >= 3 segments: {et}");
        }
    }

    #[test]
    fn event_action_as_str_covers_all_variants() {
        for action in EventAction::ALL {
            let s = action.as_str();
            assert!(!s.is_empty(), "as_str must not be empty");
            assert!(
                s.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "as_str must be snake_case: {s}"
            );
        }
    }

    #[test]
    fn event_action_scope_system_variants() {
        let system_actions = [
            EventAction::OrganizationCreated,
            EventAction::OrganizationDeleted,
            EventAction::OrganizationSuspended,
            EventAction::OrganizationResumed,
            EventAction::UserCreated,
            EventAction::UserDeleted,
            EventAction::MigrationStarted,
            EventAction::MigrationCompleted,
            EventAction::NodeJoinedCluster,
            EventAction::NodeLeftCluster,
            EventAction::RoutingUpdated,
            EventAction::ConfigurationChanged,
            EventAction::BackupCreated,
            EventAction::BackupRestored,
            EventAction::SnapshotCreated,
        ];
        for action in system_actions {
            assert_eq!(action.scope(), EventScope::System, "{:?} should be System scope", action);
        }
    }

    #[test]
    fn event_action_scope_organization_variants() {
        let org_actions = [
            EventAction::VaultCreated,
            EventAction::VaultDeleted,
            EventAction::WriteCommitted,
            EventAction::BatchWriteCommitted,
            EventAction::EntityExpired,
            EventAction::VaultHealthUpdated,
            EventAction::IntegrityChecked,
            EventAction::VaultRecovered,
            EventAction::RequestRateLimited,
            EventAction::RequestValidationFailed,
            EventAction::QuotaExceeded,
        ];
        for action in org_actions {
            assert_eq!(
                action.scope(),
                EventScope::Organization,
                "{:?} should be Organization scope",
                action
            );
        }
    }

    #[test]
    fn event_action_event_type_mapping() {
        assert_eq!(EventAction::OrganizationCreated.event_type(), "ledger.organization.created");
        assert_eq!(EventAction::VaultCreated.event_type(), "ledger.vault.created");
        assert_eq!(EventAction::WriteCommitted.event_type(), "ledger.write.committed");
        assert_eq!(EventAction::BatchWriteCommitted.event_type(), "ledger.write.batch_committed");
        assert_eq!(EventAction::RequestRateLimited.event_type(), "ledger.request.rate_limited");
        assert_eq!(EventAction::NodeJoinedCluster.event_type(), "ledger.node.joined");
    }

    #[test]
    fn event_action_as_str_mapping() {
        assert_eq!(EventAction::OrganizationCreated.as_str(), "organization_created");
        assert_eq!(EventAction::VaultCreated.as_str(), "vault_created");
        assert_eq!(EventAction::WriteCommitted.as_str(), "write_committed");
        assert_eq!(EventAction::BatchWriteCommitted.as_str(), "batch_write_committed");
    }

    #[test]
    fn event_action_serde_roundtrip() {
        for action in EventAction::ALL {
            let json = serde_json::to_string(action).expect("serialize");
            let back: EventAction = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(*action, back, "failed roundtrip for {json}");
        }
    }

    #[test]
    fn event_action_all_count() {
        // 15 system + 11 organization = 26 total
        assert_eq!(EventAction::ALL.len(), 26);
    }

    // ── EventOutcome ────────────────────────────────────────

    #[test]
    fn event_outcome_success_serde() {
        let outcome = EventOutcome::Success;
        let json = serde_json::to_string(&outcome).expect("serialize");
        let back: EventOutcome = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(outcome, back);
    }

    #[test]
    fn event_outcome_failed_serde() {
        let outcome = EventOutcome::Failed {
            code: "NOT_FOUND".to_string(),
            detail: "vault not found".to_string(),
        };
        let json = serde_json::to_string(&outcome).expect("serialize");
        let back: EventOutcome = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(outcome, back);
    }

    #[test]
    fn event_outcome_denied_serde() {
        let outcome = EventOutcome::Denied { reason: "rate limited".to_string() };
        let json = serde_json::to_string(&outcome).expect("serialize");
        let back: EventOutcome = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(outcome, back);
    }

    // ── EventEmission ───────────────────────────────────────

    #[test]
    fn event_emission_serde_roundtrip() {
        let emissions = [EventEmission::ApplyPhase, EventEmission::HandlerPhase { node_id: 42 }];
        for emission in emissions {
            let json = serde_json::to_string(&emission).expect("serialize");
            let back: EventEmission = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(emission, back);
        }
    }

    // ── EventEntry ──────────────────────────────────────────

    fn sample_event_entry() -> EventEntry {
        EventEntry {
            expires_at: 1_700_000_000,
            event_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            source_service: "ledger".to_string(),
            event_type: "ledger.vault.created".to_string(),
            timestamp: Utc::now(),
            scope: EventScope::Organization,
            action: EventAction::VaultCreated,
            emission: EventEmission::ApplyPhase,
            principal: "user:alice".to_string(),
            organization_id: OrganizationId::new(42),
            organization_slug: Some(12345),
            vault_slug: Some(67890),
            outcome: EventOutcome::Success,
            details: BTreeMap::from([("vault_name".to_string(), "my-vault".to_string())]),
            block_height: Some(100),
            trace_id: Some("abc123".to_string()),
            correlation_id: Some("batch-job-1".to_string()),
            operations_count: Some(5),
        }
    }

    #[test]
    fn event_entry_json_serde_roundtrip() {
        let entry = sample_event_entry();
        let json = serde_json::to_string(&entry).expect("serialize");
        let back: EventEntry = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(entry, back);
    }

    #[test]
    fn event_entry_postcard_roundtrip() {
        let entry = sample_event_entry();
        let bytes = postcard::to_allocvec(&entry).expect("encode");
        let back: EventEntry = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(entry, back);
    }

    #[test]
    fn event_entry_postcard_roundtrip_minimal() {
        let entry = EventEntry {
            expires_at: 0,
            event_id: [0; 16],
            source_service: String::new(),
            event_type: String::new(),
            timestamp: DateTime::from_timestamp(0, 0).expect("epoch"),
            scope: EventScope::System,
            action: EventAction::OrganizationCreated,
            emission: EventEmission::ApplyPhase,
            principal: String::new(),
            organization_id: OrganizationId::new(0),
            organization_slug: None,
            vault_slug: None,
            outcome: EventOutcome::Success,
            details: BTreeMap::new(),
            block_height: None,
            trace_id: None,
            correlation_id: None,
            operations_count: None,
        };
        let bytes = postcard::to_allocvec(&entry).expect("encode");
        let back: EventEntry = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(entry, back);
    }

    #[test]
    fn event_entry_postcard_roundtrip_all_outcomes() {
        for outcome in [
            EventOutcome::Success,
            EventOutcome::Failed { code: "ERR".to_string(), detail: "fail".to_string() },
            EventOutcome::Denied { reason: "nope".to_string() },
        ] {
            let mut entry = sample_event_entry();
            entry.outcome = outcome;
            let bytes = postcard::to_allocvec(&entry).expect("encode");
            let back: EventEntry = postcard::from_bytes(&bytes).expect("decode");
            assert_eq!(entry, back);
        }
    }

    #[test]
    fn event_entry_postcard_roundtrip_all_emissions() {
        for emission in [
            EventEmission::ApplyPhase,
            EventEmission::HandlerPhase { node_id: 0 },
            EventEmission::HandlerPhase { node_id: u64::MAX },
        ] {
            let mut entry = sample_event_entry();
            entry.emission = emission;
            let bytes = postcard::to_allocvec(&entry).expect("encode");
            let back: EventEntry = postcard::from_bytes(&bytes).expect("decode");
            assert_eq!(entry, back);
        }
    }

    #[test]
    fn event_meta_deserializes_first_field_only() {
        let entry = sample_event_entry();
        let bytes = postcard::to_allocvec(&entry).expect("encode");
        // EventMeta reads emission (field 1) + expires_at (field 2)
        let meta: EventMeta = postcard::from_bytes(&bytes).expect("decode meta");
        assert_eq!(meta.emission, entry.emission);
        assert_eq!(meta.expires_at, entry.expires_at);
    }

    #[test]
    fn emission_meta_thin_deserializes_apply_phase() {
        let entry = sample_event_entry();
        assert!(matches!(entry.emission, EventEmission::ApplyPhase));
        let bytes = postcard::to_allocvec(&entry).expect("encode");
        let meta: EmissionMeta = postcard::from_bytes(&bytes).expect("decode emission meta");
        assert_eq!(meta.emission, EventEmission::ApplyPhase);
    }

    #[test]
    fn emission_meta_thin_deserializes_handler_phase() {
        let mut entry = sample_event_entry();
        entry.emission = EventEmission::HandlerPhase { node_id: 42 };
        let bytes = postcard::to_allocvec(&entry).expect("encode");
        let meta: EmissionMeta = postcard::from_bytes(&bytes).expect("decode emission meta");
        assert_eq!(meta.emission, EventEmission::HandlerPhase { node_id: 42 });
    }

    #[test]
    fn event_meta_reads_emission_and_expires_at_after_reorder() {
        // Verify EventMeta works for both ApplyPhase and HandlerPhase variants
        let mut entry = sample_event_entry();
        entry.emission = EventEmission::HandlerPhase { node_id: 99 };
        entry.expires_at = 2_000_000_000;
        let bytes = postcard::to_allocvec(&entry).expect("encode");
        let meta: EventMeta = postcard::from_bytes(&bytes).expect("decode meta");
        assert_eq!(meta.emission, EventEmission::HandlerPhase { node_id: 99 });
        assert_eq!(meta.expires_at, 2_000_000_000);
    }

    #[test]
    fn event_entry_full_serde_roundtrip_after_reorder() {
        let entry = sample_event_entry();
        let bytes = postcard::to_allocvec(&entry).expect("encode");
        let decoded: EventEntry = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, entry);
    }

    /// Byte-pinning test: verifies `emission` is serialized first.
    ///
    /// Postcard encodes enums as a varint discriminant. `ApplyPhase` is variant 0,
    /// so the first byte of a serialized `EventEntry` with `ApplyPhase` emission
    /// must be `0x00`. `HandlerPhase` is variant 1 (`0x01`), followed by the
    /// varint-encoded `node_id`.
    ///
    /// This test prevents accidental field reorders that would break thin
    /// deserialization via `EmissionMeta` and `EventMeta`.
    #[test]
    fn event_entry_serialization_byte_pinning() {
        // ApplyPhase: first byte is 0x00 (variant 0)
        let mut entry = sample_event_entry();
        entry.emission = EventEmission::ApplyPhase;
        let bytes = postcard::to_allocvec(&entry).expect("encode apply");
        assert_eq!(bytes[0], 0x00, "first byte must be ApplyPhase discriminant (0x00)");

        // HandlerPhase: first byte is 0x01 (variant 1), followed by node_id varint
        entry.emission = EventEmission::HandlerPhase { node_id: 7 };
        let bytes = postcard::to_allocvec(&entry).expect("encode handler");
        assert_eq!(bytes[0], 0x01, "first byte must be HandlerPhase discriminant (0x01)");
        assert_eq!(bytes[1], 7, "second byte must be node_id varint (7)");
    }

    #[test]
    fn event_entry_default_fields_deserialize() {
        // Simulate an older entry missing new fields (default handling)
        let json = r#"{
            "expires_at": 0,
            "event_id": [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            "timestamp": "1970-01-01T00:00:00Z",
            "scope": "system",
            "action": "organization_created",
            "emission": "apply_phase",
            "principal": "",
            "organization_id": 0,
            "outcome": "success"
        }"#;
        let entry: EventEntry = serde_json::from_str(json).expect("deserialize with defaults");
        assert!(entry.source_service.is_empty());
        assert!(entry.event_type.is_empty());
        assert!(entry.organization_slug.is_none());
        assert!(entry.vault_slug.is_none());
        assert!(entry.details.is_empty());
        assert!(entry.block_height.is_none());
        assert!(entry.trace_id.is_none());
        assert!(entry.correlation_id.is_none());
        assert!(entry.operations_count.is_none());
    }

    // ── EventConfig ─────────────────────────────────────────

    #[test]
    fn event_config_defaults() {
        let config = EventConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_ttl_days, 90);
        assert_eq!(config.max_details_size_bytes, 4096);
        assert!(config.system_log_enabled);
        assert!(config.organization_log_enabled);
    }

    #[test]
    fn event_config_builder_defaults() {
        let config = EventConfig::builder().build().expect("valid defaults");
        assert_eq!(config, EventConfig::default());
    }

    #[test]
    fn event_config_builder_custom() {
        let config = EventConfig::builder()
            .enabled(false)
            .default_ttl_days(30)
            .max_details_size_bytes(8192)
            .system_log_enabled(false)
            .organization_log_enabled(true)
            .build()
            .expect("valid config");
        assert!(!config.enabled);
        assert_eq!(config.default_ttl_days, 30);
        assert_eq!(config.max_details_size_bytes, 8192);
        assert!(!config.system_log_enabled);
        assert!(config.organization_log_enabled);
    }

    #[test]
    fn event_config_ttl_days_too_low() {
        let result = EventConfig::builder().default_ttl_days(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn event_config_ttl_days_too_high() {
        let result = EventConfig::builder().default_ttl_days(3651).build();
        assert!(result.is_err());
    }

    #[test]
    fn event_config_ttl_days_boundary_low() {
        let config =
            EventConfig::builder().default_ttl_days(1).build().expect("ttl_days=1 is valid");
        assert_eq!(config.default_ttl_days, 1);
    }

    #[test]
    fn event_config_ttl_days_boundary_high() {
        let config =
            EventConfig::builder().default_ttl_days(3650).build().expect("ttl_days=3650 is valid");
        assert_eq!(config.default_ttl_days, 3650);
    }

    #[test]
    fn event_config_max_details_too_small() {
        let result = EventConfig::builder().max_details_size_bytes(255).build();
        assert!(result.is_err());
    }

    #[test]
    fn event_config_max_details_too_large() {
        let result = EventConfig::builder().max_details_size_bytes(65537).build();
        assert!(result.is_err());
    }

    #[test]
    fn event_config_max_details_boundary_low() {
        let config =
            EventConfig::builder().max_details_size_bytes(256).build().expect("256 is valid");
        assert_eq!(config.max_details_size_bytes, 256);
    }

    #[test]
    fn event_config_max_details_boundary_high() {
        let config =
            EventConfig::builder().max_details_size_bytes(65536).build().expect("65536 is valid");
        assert_eq!(config.max_details_size_bytes, 65536);
    }

    #[test]
    fn event_config_validate_method() {
        let mut config = EventConfig::default();
        assert!(config.validate().is_ok());
        config.default_ttl_days = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn event_config_serde_roundtrip() {
        let config = EventConfig::default();
        let json = serde_json::to_string(&config).expect("serialize");
        let back: EventConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(config, back);
    }

    #[test]
    fn event_config_serde_defaults() {
        // Empty JSON should use all defaults
        let config: EventConfig = serde_json::from_str("{}").expect("defaults");
        assert_eq!(config, EventConfig::default());
    }

    #[test]
    fn event_config_json_schema_is_valid() {
        let schema = schemars::schema_for!(EventConfig);
        let json = serde_json::to_string_pretty(&schema).expect("schema to json");
        assert!(json.contains("default_ttl_days"));
        assert!(json.contains("max_details_size_bytes"));
        assert!(json.contains("max_snapshot_events"));
        assert!(json.contains("ingestion"));
    }

    // ── EventConfig — max_snapshot_events ────────────────────

    #[test]
    fn event_config_defaults_include_new_fields() {
        let config = EventConfig::default();
        assert_eq!(config.max_snapshot_events, 100_000);
        assert!(config.ingestion.ingest_enabled);
        assert_eq!(config.ingestion.allowed_sources, vec!["engine", "control"]);
        assert_eq!(config.ingestion.max_ingest_batch_size, 500);
        assert_eq!(config.ingestion.ingest_rate_limit_per_source, 10_000);
    }

    #[test]
    fn event_config_max_snapshot_events_zero_rejected() {
        let result = EventConfig::builder().max_snapshot_events(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn event_config_max_snapshot_events_one_accepted() {
        let config = EventConfig::builder()
            .max_snapshot_events(1)
            .build()
            .expect("max_snapshot_events=1 is valid");
        assert_eq!(config.max_snapshot_events, 1);
    }

    #[test]
    fn event_config_builder_with_custom_snapshot_and_ingestion() {
        let ingestion = IngestionConfig {
            ingest_enabled: false,
            allowed_sources: vec!["my_service".to_string()],
            max_ingest_batch_size: 100,
            ingest_rate_limit_per_source: 5_000,
        };
        let config = EventConfig::builder()
            .max_snapshot_events(50_000)
            .ingestion(ingestion.clone())
            .build()
            .expect("valid config");
        assert_eq!(config.max_snapshot_events, 50_000);
        assert_eq!(config.ingestion, ingestion);
    }

    // ── IngestionConfig ─────────────────────────────────────

    #[test]
    fn ingestion_config_defaults() {
        let config = IngestionConfig::default();
        assert!(config.ingest_enabled);
        assert_eq!(config.allowed_sources, vec!["engine", "control"]);
        assert_eq!(config.max_ingest_batch_size, 500);
        assert_eq!(config.ingest_rate_limit_per_source, 10_000);
    }

    #[test]
    fn ingestion_config_validate_batch_size_too_low() {
        let config = IngestionConfig { max_ingest_batch_size: 0, ..IngestionConfig::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn ingestion_config_validate_batch_size_too_high() {
        let config =
            IngestionConfig { max_ingest_batch_size: 10_001, ..IngestionConfig::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn ingestion_config_validate_batch_size_boundaries() {
        let config = IngestionConfig { max_ingest_batch_size: 1, ..IngestionConfig::default() };
        assert!(config.validate().is_ok());
        let config =
            IngestionConfig { max_ingest_batch_size: 10_000, ..IngestionConfig::default() };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn ingestion_config_validate_rate_limit_zero() {
        let config =
            IngestionConfig { ingest_rate_limit_per_source: 0, ..IngestionConfig::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn ingestion_config_validate_empty_source() {
        let config = IngestionConfig {
            allowed_sources: vec!["engine".to_string(), String::new()],
            ..IngestionConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn ingestion_config_serde_roundtrip() {
        let config = IngestionConfig::default();
        let json = serde_json::to_string(&config).expect("serialize");
        let back: IngestionConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(config, back);
    }

    #[test]
    fn ingestion_config_serde_defaults() {
        let config: IngestionConfig = serde_json::from_str("{}").expect("defaults");
        assert_eq!(config, IngestionConfig::default());
    }

    #[test]
    fn event_config_cascading_validation() {
        // Invalid ingestion config should fail EventConfig validation
        let ingestion = IngestionConfig { max_ingest_batch_size: 0, ..IngestionConfig::default() };
        let result = EventConfig::builder().ingestion(ingestion).build();
        assert!(result.is_err());
    }

    #[test]
    fn ingestion_config_json_schema_is_valid() {
        let schema = schemars::schema_for!(IngestionConfig);
        let json = serde_json::to_string_pretty(&schema).expect("schema to json");
        assert!(json.contains("ingest_enabled"));
        assert!(json.contains("allowed_sources"));
        assert!(json.contains("max_ingest_batch_size"));
        assert!(json.contains("ingest_rate_limit_per_source"));
    }
}
