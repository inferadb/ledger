//! Event writers for deterministic and handler-phase audit trail generation.
//!
//! This module provides:
//!
//! - [`EventWriter`] for persisting batched events to `events.db`
//! - [`ApplyPhaseEmitter`] for constructing deterministic [`EventEntry`] records during Raft state
//!   machine apply (UUID v5, block timestamps, identical on all replicas)
//! - [`HandlerPhaseEmitter`] for constructing node-local [`EventEntry`] records at the gRPC handler
//!   level (UUID v4, wall-clock timestamps, node-specific)
//! - [`EventHandle`] — a cheaply cloneable handle for best-effort handler-phase event writes,
//!   injected into gRPC services

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use inferadb_ledger_state::{EventStore, EventsDatabase};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    OrganizationId, OrganizationSlug, VaultSlug,
    events::{EventAction, EventConfig, EventEmission, EventEntry, EventOutcome, EventScope},
};
use snafu::{ResultExt, Snafu};
use uuid::Uuid;

use crate::metrics;

/// Fixed UUID v4 namespace for deterministic UUID v5 event ID generation.
///
/// All Raft replicas use this same namespace to derive identical event IDs
/// from the same `(block_height, op_index, action)` tuple.
///
/// Generated once via `uuid::Uuid::new_v4()` and hardcoded.
pub const EVENTS_UUID_NAMESPACE: Uuid = Uuid::from_bytes([
    0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x47, 0x08, 0x89, 0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60,
]);

/// Errors returned by [`EventWriter`] operations.
#[derive(Debug, Snafu)]
pub enum EventWriterError {
    /// Failed to open a write transaction on the events database.
    #[snafu(display("Events database write transaction failed: {source}"))]
    Transaction {
        /// The underlying store error.
        source: inferadb_ledger_store::Error,
        /// Source code location for debugging.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Failed to write an event entry to storage.
    #[snafu(display("Event write failed: {source}"))]
    Write {
        /// The underlying event store error.
        source: inferadb_ledger_state::EventStoreError,
        /// Source code location for debugging.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Failed to commit the events write transaction.
    #[snafu(display("Events transaction commit failed: {source}"))]
    Commit {
        /// The underlying store error.
        source: inferadb_ledger_store::Error,
        /// Source code location for debugging.
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Persists event entries to the dedicated events database.
///
/// Wraps an [`EventsDatabase`] and [`EventConfig`], providing a high-level
/// API that respects the config's `enabled` and scope flags. All event
/// writes go through this type so metrics and filtering are centralized.
pub struct EventWriter<B: StorageBackend> {
    events_db: Arc<EventsDatabase<B>>,
    config: EventConfig,
}

impl<B: StorageBackend> EventWriter<B> {
    /// Creates a new `EventWriter`.
    pub fn new(events_db: Arc<EventsDatabase<B>>, config: EventConfig) -> Self {
        Self { events_db, config }
    }

    /// Returns `true` if event logging is enabled for the given scope.
    fn is_scope_enabled(&self, scope: EventScope) -> bool {
        if !self.config.enabled {
            return false;
        }
        match scope {
            EventScope::System => self.config.system_log_enabled,
            EventScope::Organization => self.config.organization_log_enabled,
        }
    }

    /// Writes a batch of events to the events database.
    ///
    /// Filters out events whose scope is disabled by config, opens a single
    /// write transaction, writes all enabled events, commits, and emits
    /// Prometheus metrics.
    ///
    /// Returns the number of events actually written (after scope filtering).
    ///
    /// # Errors
    ///
    /// Returns [`EventWriterError`] if the transaction, write, or commit fails.
    pub fn write_events(&self, entries: &[EventEntry]) -> Result<usize, EventWriterError> {
        if !self.config.enabled {
            return Ok(0);
        }

        let enabled_entries: Vec<&EventEntry> =
            entries.iter().filter(|e| self.is_scope_enabled(e.action.scope())).collect();

        if enabled_entries.is_empty() {
            return Ok(0);
        }

        let mut txn = self.events_db.write().context(TransactionSnafu)?;

        for entry in &enabled_entries {
            EventStore::write(&mut txn, entry).context(WriteSnafu)?;
        }

        txn.commit().context(CommitSnafu)?;

        let count = enabled_entries.len();

        // Emit Prometheus metrics per event
        for entry in &enabled_entries {
            metrics::record_event_write(
                "apply_phase",
                entry.action.scope().as_str(),
                entry.action.as_str(),
            );
        }

        Ok(count)
    }

    /// Returns a reference to the underlying events database.
    pub fn events_db(&self) -> &Arc<EventsDatabase<B>> {
        &self.events_db
    }

    /// Returns a reference to the event config.
    pub fn config(&self) -> &EventConfig {
        &self.config
    }
}

/// Builder for deterministic apply-phase event entries.
///
/// Accumulates context fields (organization, vault, principal, outcome, etc.)
/// and produces an [`EventEntry`] with a deterministic UUID v5 event ID
/// derived from `(block_height, op_index, action)`.
///
/// # Usage
///
/// ```no_run
/// # use inferadb_ledger_raft::event_writer::ApplyPhaseEmitter;
/// # use inferadb_ledger_types::events::{EventAction, EventOutcome};
/// # use inferadb_ledger_types::{OrganizationId, OrganizationSlug};
/// # use chrono::Utc;
/// let entry = ApplyPhaseEmitter::for_organization(
///         EventAction::WriteCommitted,
///         OrganizationId::new(1),
///         Some(OrganizationSlug::new(12345)),
///     )
///     .principal("user:alice")
///     .outcome(EventOutcome::Success)
///     .operations_count(5)
///     .build(100, 0, Utc::now(), 90);
/// ```
pub struct ApplyPhaseEmitter {
    action: EventAction,
    organization_id: OrganizationId,
    organization: Option<OrganizationSlug>,
    vault: Option<VaultSlug>,
    principal: String,
    outcome: EventOutcome,
    details: BTreeMap<String, String>,
    trace_id: Option<String>,
    correlation_id: Option<String>,
    operations_count: Option<u32>,
}

impl ApplyPhaseEmitter {
    /// Creates an emitter for a system-scope action (organization_id = 0).
    pub fn for_system(action: EventAction) -> Self {
        Self {
            action,
            organization_id: OrganizationId::new(0),
            organization: None,
            vault: None,
            principal: String::new(),
            outcome: EventOutcome::Success,
            details: BTreeMap::new(),
            trace_id: None,
            correlation_id: None,
            operations_count: None,
        }
    }

    /// Creates an emitter for an organization-scope action.
    pub fn for_organization(
        action: EventAction,
        org_id: OrganizationId,
        organization: Option<OrganizationSlug>,
    ) -> Self {
        Self {
            action,
            organization_id: org_id,
            organization,
            vault: None,
            principal: String::new(),
            outcome: EventOutcome::Success,
            details: BTreeMap::new(),
            trace_id: None,
            correlation_id: None,
            operations_count: None,
        }
    }

    /// Sets the vault slug (external identifier).
    pub fn vault(mut self, slug: VaultSlug) -> Self {
        self.vault = Some(slug);
        self
    }

    /// Sets the principal (actor) for this event.
    pub fn principal(mut self, principal: &str) -> Self {
        self.principal = principal.to_string();
        self
    }

    /// Sets the outcome of the operation.
    pub fn outcome(mut self, outcome: EventOutcome) -> Self {
        self.outcome = outcome;
        self
    }

    /// Adds a key-value detail to the event context.
    pub fn detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }

    /// Sets the trace ID for distributed tracing correlation.
    pub fn trace_id(mut self, id: &str) -> Self {
        self.trace_id = Some(id.to_string());
        self
    }

    /// Sets the correlation ID for multi-step operation tracking.
    pub fn correlation_id(mut self, id: &str) -> Self {
        self.correlation_id = Some(id.to_string());
        self
    }

    /// Sets the operations count (for write/batch actions).
    pub fn operations_count(mut self, count: u32) -> Self {
        self.operations_count = Some(count);
        self
    }

    /// Builds the [`EventEntry`] with deterministic UUID v5 and block timestamp.
    ///
    /// The event ID is derived from `EVENTS_UUID_NAMESPACE` and the string
    /// `"{block_height}:{op_index}:{action_str}"`, ensuring all replicas
    /// produce identical event IDs for the same log entry.
    ///
    /// `ttl_days` controls the `expires_at` field: `0` means no expiry,
    /// otherwise `block_timestamp + ttl_days` converted to Unix timestamp.
    pub fn build(
        self,
        block_height: u64,
        op_index: u32,
        block_timestamp: DateTime<Utc>,
        ttl_days: u32,
    ) -> EventEntry {
        // Deterministic event ID: UUID v5 from namespace + "{block_height}:{op_index}:{action}"
        let id_input = format!("{}:{}:{}", block_height, op_index, self.action.as_str());
        let event_id = *Uuid::new_v5(&EVENTS_UUID_NAMESPACE, id_input.as_bytes()).as_bytes();

        // TTL: 0 means no expiry, otherwise block_timestamp + ttl_days
        let expires_at = if ttl_days == 0 {
            0
        } else {
            let duration = chrono::Duration::days(i64::from(ttl_days));
            let expiry = block_timestamp + duration;
            expiry.timestamp() as u64
        };

        EventEntry {
            expires_at,
            event_id,
            source_service: "ledger".to_string(),
            event_type: self.action.event_type().to_string(),
            timestamp: block_timestamp,
            scope: self.action.scope(),
            action: self.action,
            emission: EventEmission::ApplyPhase,
            principal: self.principal,
            organization_id: self.organization_id,
            organization: self.organization,
            vault: self.vault,
            outcome: self.outcome,
            details: self.details,
            block_height: Some(block_height),
            trace_id: self.trace_id,
            correlation_id: self.correlation_id,
            operations_count: self.operations_count,
        }
    }
}

/// Builder for node-local handler-phase event entries.
///
/// Accumulates context fields and produces an [`EventEntry`] with a random
/// UUID v4 event ID and wall-clock timestamp. Used for pre-Raft rejections
/// (rate limits, validation failures, quota exceeded) and admin operations
/// that don't go through Raft consensus.
///
/// # Usage
///
/// ```no_run
/// # use inferadb_ledger_raft::event_writer::HandlerPhaseEmitter;
/// # use inferadb_ledger_types::events::{EventAction, EventOutcome};
/// # use inferadb_ledger_types::{OrganizationId, OrganizationSlug};
/// let entry = HandlerPhaseEmitter::for_organization(
///         EventAction::RequestRateLimited,
///         OrganizationId::new(1),
///         Some(OrganizationSlug::new(12345)),
///         42,
///     )
///     .principal("user:alice")
///     .outcome(EventOutcome::Denied { reason: "rate_limited".to_string() })
///     .detail("level", "organization")
///     .build(90);
/// ```
pub struct HandlerPhaseEmitter {
    action: EventAction,
    organization_id: OrganizationId,
    organization: Option<OrganizationSlug>,
    vault: Option<VaultSlug>,
    node_id: u64,
    principal: String,
    outcome: EventOutcome,
    details: BTreeMap<String, String>,
    trace_id: Option<String>,
    correlation_id: Option<String>,
    operations_count: Option<u32>,
}

impl HandlerPhaseEmitter {
    /// Creates an emitter for a system-scope handler-phase action.
    pub fn for_system(action: EventAction, node_id: u64) -> Self {
        Self {
            action,
            organization_id: OrganizationId::new(0),
            organization: None,
            vault: None,
            node_id,
            principal: String::new(),
            outcome: EventOutcome::Success,
            details: BTreeMap::new(),
            trace_id: None,
            correlation_id: None,
            operations_count: None,
        }
    }

    /// Creates an emitter for an organization-scope handler-phase action.
    pub fn for_organization(
        action: EventAction,
        org_id: OrganizationId,
        organization: Option<OrganizationSlug>,
        node_id: u64,
    ) -> Self {
        Self {
            action,
            organization_id: org_id,
            organization,
            vault: None,
            node_id,
            principal: String::new(),
            outcome: EventOutcome::Success,
            details: BTreeMap::new(),
            trace_id: None,
            correlation_id: None,
            operations_count: None,
        }
    }

    /// Sets the vault slug (external identifier).
    pub fn vault(mut self, slug: VaultSlug) -> Self {
        self.vault = Some(slug);
        self
    }

    /// Sets the principal (actor) for this event.
    pub fn principal(mut self, principal: &str) -> Self {
        self.principal = principal.to_string();
        self
    }

    /// Sets the outcome of the operation.
    pub fn outcome(mut self, outcome: EventOutcome) -> Self {
        self.outcome = outcome;
        self
    }

    /// Adds a key-value detail to the event context.
    pub fn detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }

    /// Sets the trace ID for distributed tracing correlation.
    pub fn trace_id(mut self, id: &str) -> Self {
        self.trace_id = Some(id.to_string());
        self
    }

    /// Sets the correlation ID for multi-step operation tracking.
    pub fn correlation_id(mut self, id: &str) -> Self {
        self.correlation_id = Some(id.to_string());
        self
    }

    /// Sets the operations count (for write/batch actions).
    pub fn operations_count(mut self, count: u32) -> Self {
        self.operations_count = Some(count);
        self
    }

    /// Builds the [`EventEntry`] with random UUID v4 and wall-clock timestamp.
    ///
    /// `ttl_days` controls the `expires_at` field: `0` means no expiry,
    /// otherwise `Utc::now() + ttl_days` converted to Unix timestamp.
    pub fn build(self, ttl_days: u32) -> EventEntry {
        let event_id = *Uuid::new_v4().as_bytes();
        let now = Utc::now();

        let expires_at = if ttl_days == 0 {
            0
        } else {
            let duration = chrono::Duration::days(i64::from(ttl_days));
            let expiry = now + duration;
            expiry.timestamp() as u64
        };

        EventEntry {
            expires_at,
            event_id,
            source_service: "ledger".to_string(),
            event_type: self.action.event_type().to_string(),
            timestamp: now,
            scope: self.action.scope(),
            action: self.action,
            emission: EventEmission::HandlerPhase { node_id: self.node_id },
            principal: self.principal,
            organization_id: self.organization_id,
            organization: self.organization,
            vault: self.vault,
            outcome: self.outcome,
            details: self.details,
            block_height: None,
            trace_id: self.trace_id,
            correlation_id: self.correlation_id,
            operations_count: self.operations_count,
        }
    }
}

/// Cheaply cloneable handle for best-effort handler-phase event writes.
///
/// Injected into gRPC service implementations (`WriteServiceImpl`,
/// `AdminServiceImpl`, `ReadServiceImpl`) to record denial and admin events.
/// Write failures are logged as warnings but never propagate to the caller —
/// the primary RPC must not be affected by event persistence issues.
///
/// Uses the dedicated `events.db` database, separate from `state.db`, so
/// handler-phase writes do not contend with Raft apply-phase state mutations.
pub struct EventHandle<B: StorageBackend> {
    events_db: Arc<EventsDatabase<B>>,
    config: Arc<EventConfig>,
    node_id: u64,
}

impl<B: StorageBackend> Clone for EventHandle<B> {
    fn clone(&self) -> Self {
        Self {
            events_db: Arc::clone(&self.events_db),
            config: Arc::clone(&self.config),
            node_id: self.node_id,
        }
    }
}

impl<B: StorageBackend> EventHandle<B> {
    /// Creates a new event handle.
    pub fn new(events_db: Arc<EventsDatabase<B>>, config: Arc<EventConfig>, node_id: u64) -> Self {
        Self { events_db, config, node_id }
    }

    /// Records a handler-phase event (best-effort).
    ///
    /// Opens a write transaction on `events.db`, writes the entry, commits,
    /// and emits Prometheus metrics. If any step fails, logs a warning and
    /// returns `Ok(())` — handler events must never affect the primary RPC.
    pub fn record_handler_event(&self, entry: EventEntry) {
        if !self.config.enabled {
            return;
        }

        let scope = entry.action.scope();
        let is_enabled = match scope {
            EventScope::System => self.config.system_log_enabled,
            EventScope::Organization => self.config.organization_log_enabled,
        };
        if !is_enabled {
            return;
        }

        let action_str = entry.action.as_str().to_string();
        let scope_str = scope.as_str().to_string();

        match self.write_entry(&entry) {
            Ok(()) => {
                metrics::record_event_write("handler_phase", &scope_str, &action_str);
            },
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    action = action_str,
                    scope = scope_str,
                    "Handler-phase event write failed (best-effort, dropping)"
                );
            },
        }
    }

    /// Performs the actual write transaction.
    fn write_entry(&self, entry: &EventEntry) -> Result<(), EventWriterError> {
        let mut txn = self.events_db.write().context(TransactionSnafu)?;
        EventStore::write(&mut txn, entry).context(WriteSnafu)?;
        txn.commit().context(CommitSnafu)?;
        Ok(())
    }

    /// Returns the node ID for this handle.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Returns a reference to the event config.
    pub fn config(&self) -> &EventConfig {
        &self.config
    }

    /// Returns a shared reference to the event config `Arc`.
    pub fn config_arc(&self) -> &Arc<EventConfig> {
        &self.config
    }

    /// Returns a reference to the underlying events database.
    pub fn events_db(&self) -> &Arc<EventsDatabase<B>> {
        &self.events_db
    }
}

/// Per-source token bucket rate limiter for external event ingestion.
///
/// Maintains one `TokenBucket` per `source_service` value (e.g., `"engine"`,
/// `"control"`). Each bucket is sized to the configured
/// `ingest_rate_limit_per_source` events/sec.
///
/// The rate limit is runtime-updatable via `AtomicU64` — no restart required
/// when the operator changes `IngestionConfig.ingest_rate_limit_per_source`
/// through `UpdateConfig` RPC.
pub struct IngestionRateLimiter {
    /// Per-source token buckets. Key = `source_service`.
    buckets: parking_lot::Mutex<std::collections::HashMap<String, IngestionTokenBucket>>,

    /// Runtime-updatable rate limit (events/sec per source).
    /// Stored as `u32.to_bits()` via `f64::to_bits()` pattern — but since
    /// ingestion rate is a whole number, we just store the raw `u32` value.
    rate_limit: std::sync::atomic::AtomicU64,
}

/// A simple token bucket for ingestion rate limiting.
#[derive(Debug)]
struct IngestionTokenBucket {
    /// Available tokens (scaled ×1000 for sub-token precision).
    tokens_millis: u64,
    /// Maximum capacity (scaled ×1000).
    capacity_millis: u64,
    /// Refill rate (tokens per second).
    refill_rate: f64,
    /// Last refill timestamp.
    last_refill: std::time::Instant,
}

impl IngestionTokenBucket {
    /// Creates a new bucket at full capacity.
    fn new(rate: u32) -> Self {
        let capacity = u64::from(rate);
        Self {
            tokens_millis: capacity * 1000,
            capacity_millis: capacity * 1000,
            refill_rate: f64::from(rate),
            last_refill: std::time::Instant::now(),
        }
    }

    /// Updates the bucket's rate limit (capacity and refill rate).
    fn update_rate(&mut self, rate: u32) {
        let new_capacity_millis = u64::from(rate) * 1000;
        // Scale tokens proportionally if capacity changed
        if self.capacity_millis > 0 && self.capacity_millis != new_capacity_millis {
            let ratio = new_capacity_millis as f64 / self.capacity_millis as f64;
            self.tokens_millis =
                ((self.tokens_millis as f64 * ratio) as u64).min(new_capacity_millis);
        }
        self.capacity_millis = new_capacity_millis;
        self.refill_rate = f64::from(rate);
    }

    /// Tries to acquire `count` tokens. Returns `true` if allowed.
    fn try_acquire(&mut self, count: u32) -> bool {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_refill);

        // Refill based on elapsed time
        let refill = (elapsed.as_secs_f64() * self.refill_rate * 1000.0) as u64;
        if refill > 0 {
            self.tokens_millis = (self.tokens_millis + refill).min(self.capacity_millis);
            self.last_refill = now;
        }

        let needed = u64::from(count) * 1000;
        if self.tokens_millis >= needed {
            self.tokens_millis -= needed;
            true
        } else {
            false
        }
    }
}

impl IngestionRateLimiter {
    /// Creates a new ingestion rate limiter.
    pub fn new(rate_limit_per_source: u32) -> Self {
        Self {
            buckets: parking_lot::Mutex::new(std::collections::HashMap::new()),
            rate_limit: std::sync::atomic::AtomicU64::new(u64::from(rate_limit_per_source)),
        }
    }

    /// Updates the per-source rate limit at runtime.
    ///
    /// Existing buckets will pick up the new rate on their next `check()` call.
    pub fn update_rate_limit(&self, rate_limit_per_source: u32) {
        self.rate_limit
            .store(u64::from(rate_limit_per_source), std::sync::atomic::Ordering::Relaxed);
    }

    /// Checks whether `count` events from `source_service` are allowed.
    ///
    /// Returns `true` if the events are within the rate limit, `false` if
    /// the source has exceeded its budget.
    pub fn check(&self, source_service: &str, count: u32) -> bool {
        let current_rate = self.rate_limit.load(std::sync::atomic::Ordering::Relaxed) as u32;

        let mut buckets = self.buckets.lock();
        let bucket = buckets
            .entry(source_service.to_string())
            .or_insert_with(|| IngestionTokenBucket::new(current_rate));

        // Update rate if it changed since bucket creation
        #[allow(clippy::cast_possible_truncation)]
        let expected_rate = current_rate;
        if (bucket.refill_rate - f64::from(expected_rate)).abs() > f64::EPSILON {
            bucket.update_rate(expected_rate);
        }

        bucket.try_acquire(count)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use chrono::TimeZone;
    use inferadb_ledger_state::EventsDatabase;

    use super::*;

    fn test_config() -> EventConfig {
        EventConfig::default()
    }

    fn disabled_config() -> EventConfig {
        EventConfig { enabled: false, ..EventConfig::default() }
    }

    fn system_disabled_config() -> EventConfig {
        EventConfig { system_log_enabled: false, ..EventConfig::default() }
    }

    fn org_disabled_config() -> EventConfig {
        EventConfig { organization_log_enabled: false, ..EventConfig::default() }
    }

    fn sample_timestamp() -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000, 0).unwrap()
    }

    // ── EVENTS_UUID_NAMESPACE ──────────────────────────────────

    #[test]
    fn namespace_is_valid_uuid() {
        // Must be a valid UUID (not nil)
        assert_ne!(EVENTS_UUID_NAMESPACE, Uuid::nil());
        // Version 4 bytes check: version nibble is 4
        assert_eq!(EVENTS_UUID_NAMESPACE.get_version_num(), 4);
    }

    // ── ApplyPhaseEmitter ──────────────────────────────────────

    #[test]
    fn emitter_system_event_defaults() {
        let entry = ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(
            1,
            0,
            sample_timestamp(),
            90,
        );

        assert_eq!(entry.action, EventAction::OrganizationCreated);
        assert_eq!(entry.scope, EventScope::System);
        assert_eq!(entry.emission, EventEmission::ApplyPhase);
        assert_eq!(entry.organization_id, OrganizationId::new(0));
        assert_eq!(entry.source_service, "ledger");
        assert_eq!(entry.event_type, "ledger.organization.created");
        assert!(entry.principal.is_empty());
        assert_eq!(entry.outcome, EventOutcome::Success);
        assert_eq!(entry.block_height, Some(1));
    }

    #[test]
    fn emitter_organization_event() {
        let entry = ApplyPhaseEmitter::for_organization(
            EventAction::WriteCommitted,
            OrganizationId::new(42),
            Some(OrganizationSlug::new(12345)),
        )
        .vault(VaultSlug::new(67890))
        .principal("user:alice")
        .outcome(EventOutcome::Success)
        .operations_count(5)
        .detail("vault_name", "my-vault")
        .trace_id("trace-abc")
        .correlation_id("corr-xyz")
        .build(100, 3, sample_timestamp(), 90);

        assert_eq!(entry.action, EventAction::WriteCommitted);
        assert_eq!(entry.scope, EventScope::Organization);
        assert_eq!(entry.emission, EventEmission::ApplyPhase);
        assert_eq!(entry.organization_id, OrganizationId::new(42));
        assert_eq!(entry.organization, Some(OrganizationSlug::new(12345)));
        assert_eq!(entry.vault, Some(VaultSlug::new(67890)));
        assert_eq!(entry.principal, "user:alice");
        assert_eq!(entry.operations_count, Some(5));
        assert_eq!(entry.details.get("vault_name").unwrap(), "my-vault");
        assert_eq!(entry.trace_id, Some("trace-abc".to_string()));
        assert_eq!(entry.correlation_id, Some("corr-xyz".to_string()));
        assert_eq!(entry.block_height, Some(100));
    }

    #[test]
    fn emitter_deterministic_uuid_v5() {
        let ts = sample_timestamp();

        // Same inputs → same event ID
        let a =
            ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(10, 0, ts, 90);
        let b =
            ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(10, 0, ts, 90);
        assert_eq!(a.event_id, b.event_id);

        // Different block_height → different event ID
        let c =
            ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(11, 0, ts, 90);
        assert_ne!(a.event_id, c.event_id);

        // Different op_index → different event ID
        let d =
            ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(10, 1, ts, 90);
        assert_ne!(a.event_id, d.event_id);

        // Different action → different event ID
        let e =
            ApplyPhaseEmitter::for_system(EventAction::OrganizationDeleted).build(10, 0, ts, 90);
        assert_ne!(a.event_id, e.event_id);
    }

    #[test]
    fn emitter_uuid_is_v5() {
        let entry = ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(
            1,
            0,
            sample_timestamp(),
            90,
        );
        let uuid = Uuid::from_bytes(entry.event_id);
        assert_eq!(uuid.get_version_num(), 5);
    }

    #[test]
    fn emitter_ttl_zero_means_no_expiry() {
        let entry = ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(
            1,
            0,
            sample_timestamp(),
            0,
        );
        assert_eq!(entry.expires_at, 0);
    }

    #[test]
    fn emitter_ttl_computes_expires_at() {
        let ts = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let entry =
            ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(1, 0, ts, 30);

        // 30 days = 30 * 86400 = 2_592_000 seconds
        let expected = 1_700_000_000u64 + 30 * 86400;
        assert_eq!(entry.expires_at, expected);
    }

    #[test]
    fn emitter_failed_outcome() {
        let entry = ApplyPhaseEmitter::for_organization(
            EventAction::WriteCommitted,
            OrganizationId::new(1),
            None,
        )
        .outcome(EventOutcome::Failed {
            code: "CONFLICT".to_string(),
            detail: "conditional write failed".to_string(),
        })
        .build(50, 0, sample_timestamp(), 90);

        match &entry.outcome {
            EventOutcome::Failed { code, detail } => {
                assert_eq!(code, "CONFLICT");
                assert_eq!(detail, "conditional write failed");
            },
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    // ── EventWriter ────────────────────────────────────────────

    #[test]
    fn writer_disabled_config_writes_nothing() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let writer = EventWriter::new(Arc::new(db), disabled_config());

        let entry = ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(
            1,
            0,
            sample_timestamp(),
            90,
        );

        let count = writer.write_events(&[entry]).expect("write");
        assert_eq!(count, 0);
    }

    #[test]
    fn writer_system_scope_disabled_filters_system_events() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let writer = EventWriter::new(Arc::new(db.clone()), system_disabled_config());

        let system_entry = ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(
            1,
            0,
            sample_timestamp(),
            90,
        );
        let org_entry = ApplyPhaseEmitter::for_organization(
            EventAction::WriteCommitted,
            OrganizationId::new(1),
            None,
        )
        .build(1, 1, sample_timestamp(), 90);

        let count = writer.write_events(&[system_entry, org_entry]).expect("write");
        assert_eq!(count, 1, "only org event should be written");

        // Verify via direct read
        let txn = db.read().expect("read");
        let org_count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(org_count, 1);
        let sys_count = EventStore::count(&txn, OrganizationId::new(0)).expect("count");
        assert_eq!(sys_count, 0);
    }

    #[test]
    fn writer_org_scope_disabled_filters_org_events() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let writer = EventWriter::new(Arc::new(db.clone()), org_disabled_config());

        let system_entry = ApplyPhaseEmitter::for_system(EventAction::OrganizationCreated).build(
            1,
            0,
            sample_timestamp(),
            90,
        );
        let org_entry = ApplyPhaseEmitter::for_organization(
            EventAction::WriteCommitted,
            OrganizationId::new(1),
            None,
        )
        .build(1, 1, sample_timestamp(), 90);

        let count = writer.write_events(&[system_entry, org_entry]).expect("write");
        assert_eq!(count, 1, "only system event should be written");

        let txn = db.read().expect("read");
        let sys_count = EventStore::count(&txn, OrganizationId::new(0)).expect("count");
        assert_eq!(sys_count, 1);
        let org_count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(org_count, 0);
    }

    #[test]
    fn writer_enabled_writes_all() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let writer = EventWriter::new(Arc::new(db.clone()), test_config());

        let entries: Vec<EventEntry> = (0..5u32)
            .map(|i| {
                ApplyPhaseEmitter::for_organization(
                    EventAction::WriteCommitted,
                    OrganizationId::new(1),
                    None,
                )
                .build(1, i, sample_timestamp(), 90)
            })
            .collect();

        let count = writer.write_events(&entries).expect("write");
        assert_eq!(count, 5);

        let txn = db.read().expect("read");
        let stored = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(stored, 5);
    }

    #[test]
    fn writer_empty_batch_is_noop() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let writer = EventWriter::new(Arc::new(db), test_config());

        let count = writer.write_events(&[]).expect("write");
        assert_eq!(count, 0);
    }

    #[test]
    fn writer_accessors() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let db_arc = Arc::new(db);
        let config = test_config();
        let writer = EventWriter::new(Arc::clone(&db_arc), config.clone());

        assert_eq!(*writer.config(), config);
        assert!(Arc::ptr_eq(writer.events_db(), &db_arc));
    }

    // ── Determinism integration ────────────────────────────────

    #[test]
    fn determinism_across_writers() {
        // Two separate EventWriters (simulating two replicas) produce identical events
        let db1 = EventsDatabase::open_in_memory().expect("open");
        let db2 = EventsDatabase::open_in_memory().expect("open");
        let writer1 = EventWriter::new(Arc::new(db1), test_config());
        let writer2 = EventWriter::new(Arc::new(db2), test_config());

        let ts = sample_timestamp();

        let entry1 = ApplyPhaseEmitter::for_organization(
            EventAction::WriteCommitted,
            OrganizationId::new(1),
            Some(OrganizationSlug::new(100)),
        )
        .principal("user:alice")
        .operations_count(3)
        .build(50, 0, ts, 90);

        let entry2 = ApplyPhaseEmitter::for_organization(
            EventAction::WriteCommitted,
            OrganizationId::new(1),
            Some(OrganizationSlug::new(100)),
        )
        .principal("user:alice")
        .operations_count(3)
        .build(50, 0, ts, 90);

        // Event IDs must match
        assert_eq!(entry1.event_id, entry2.event_id);
        // Timestamps must match
        assert_eq!(entry1.timestamp, entry2.timestamp);
        // Expires at must match
        assert_eq!(entry1.expires_at, entry2.expires_at);

        // Both writes succeed
        assert_eq!(writer1.write_events(&[entry1]).expect("write1"), 1);
        assert_eq!(writer2.write_events(&[entry2]).expect("write2"), 1);
    }

    // ── HandlerPhaseEmitter ──────────────────────────────────

    #[test]
    fn handler_emitter_system_event_defaults() {
        let entry =
            HandlerPhaseEmitter::for_system(EventAction::ConfigurationChanged, 42).build(90);

        assert_eq!(entry.action, EventAction::ConfigurationChanged);
        assert_eq!(entry.scope, EventScope::System);
        assert_eq!(entry.emission, EventEmission::HandlerPhase { node_id: 42 });
        assert_eq!(entry.organization_id, OrganizationId::new(0));
        assert_eq!(entry.source_service, "ledger");
        assert_eq!(entry.event_type, "ledger.config.changed");
        assert!(entry.principal.is_empty());
        assert_eq!(entry.outcome, EventOutcome::Success);
        assert_eq!(entry.block_height, None);
    }

    #[test]
    fn handler_emitter_organization_event() {
        let entry = HandlerPhaseEmitter::for_organization(
            EventAction::RequestRateLimited,
            OrganizationId::new(7),
            Some(OrganizationSlug::new(99999)),
            10,
        )
        .principal("client:bot")
        .vault(VaultSlug::new(55555))
        .outcome(EventOutcome::Denied { reason: "rate_limited".to_string() })
        .detail("level", "organization")
        .trace_id("trace-123")
        .correlation_id("corr-456")
        .operations_count(12)
        .build(90);

        assert_eq!(entry.action, EventAction::RequestRateLimited);
        assert_eq!(entry.scope, EventScope::Organization);
        assert_eq!(entry.emission, EventEmission::HandlerPhase { node_id: 10 });
        assert_eq!(entry.organization_id, OrganizationId::new(7));
        assert_eq!(entry.organization, Some(OrganizationSlug::new(99999)));
        assert_eq!(entry.vault, Some(VaultSlug::new(55555)));
        assert_eq!(entry.principal, "client:bot");
        assert_eq!(entry.event_type, "ledger.request.rate_limited");
        assert!(
            matches!(entry.outcome, EventOutcome::Denied { ref reason } if reason == "rate_limited")
        );
        assert_eq!(entry.details.get("level").unwrap(), "organization");
        assert_eq!(entry.trace_id, Some("trace-123".to_string()));
        assert_eq!(entry.correlation_id, Some("corr-456".to_string()));
        assert_eq!(entry.operations_count, Some(12));
        assert_eq!(entry.block_height, None);
    }

    #[test]
    fn handler_emitter_uuid_v4() {
        let entry1 =
            HandlerPhaseEmitter::for_system(EventAction::ConfigurationChanged, 1).build(90);
        let entry2 =
            HandlerPhaseEmitter::for_system(EventAction::ConfigurationChanged, 1).build(90);

        // UUID v4 means random → different IDs each time
        assert_ne!(entry1.event_id, entry2.event_id);

        // Verify it's actually UUID v4
        let uuid1 = Uuid::from_bytes(entry1.event_id);
        assert_eq!(uuid1.get_version_num(), 4);
    }

    #[test]
    fn handler_emitter_wall_clock_timestamp() {
        let before = Utc::now();
        let entry = HandlerPhaseEmitter::for_system(EventAction::ConfigurationChanged, 1).build(90);
        let after = Utc::now();

        assert!(entry.timestamp >= before);
        assert!(entry.timestamp <= after);
    }

    #[test]
    fn handler_emitter_ttl_zero_means_no_expiry() {
        let entry = HandlerPhaseEmitter::for_system(EventAction::ConfigurationChanged, 1).build(0);
        assert_eq!(entry.expires_at, 0);
    }

    #[test]
    fn handler_emitter_ttl_computes_expires_at() {
        let entry = HandlerPhaseEmitter::for_system(EventAction::ConfigurationChanged, 1).build(30);

        // 30 days ≈ 2_592_000 seconds from now
        let now_ts = Utc::now().timestamp() as u64;
        let expected_min = now_ts + 30 * 86400 - 5; // 5s tolerance
        let expected_max = now_ts + 30 * 86400 + 5;
        assert!(
            entry.expires_at >= expected_min && entry.expires_at <= expected_max,
            "expires_at {} not in range [{}, {}]",
            entry.expires_at,
            expected_min,
            expected_max
        );
    }

    // ── EventHandle ──────────────────────────────────────────

    #[test]
    fn event_handle_write_and_read() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let db_arc = Arc::new(db);
        let config = Arc::new(test_config());
        let handle = EventHandle::new(Arc::clone(&db_arc), config, 42);

        let entry = HandlerPhaseEmitter::for_organization(
            EventAction::RequestRateLimited,
            OrganizationId::new(5),
            None,
            42,
        )
        .outcome(EventOutcome::Denied { reason: "rate_limited".to_string() })
        .build(90);

        handle.record_handler_event(entry);

        // Verify it was written
        let txn = db_arc.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(5)).expect("count");
        assert_eq!(count, 1);
    }

    #[test]
    fn event_handle_disabled_config_writes_nothing() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let db_arc = Arc::new(db);
        let config = Arc::new(disabled_config());
        let handle = EventHandle::new(Arc::clone(&db_arc), config, 1);

        let entry = HandlerPhaseEmitter::for_organization(
            EventAction::RequestRateLimited,
            OrganizationId::new(1),
            None,
            1,
        )
        .build(90);

        handle.record_handler_event(entry);

        let txn = db_arc.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 0);
    }

    #[test]
    fn event_handle_org_disabled_filters_org_events() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let db_arc = Arc::new(db);
        let config = Arc::new(org_disabled_config());
        let handle = EventHandle::new(Arc::clone(&db_arc), config, 1);

        let entry = HandlerPhaseEmitter::for_organization(
            EventAction::RequestRateLimited,
            OrganizationId::new(1),
            None,
            1,
        )
        .build(90);

        handle.record_handler_event(entry);

        let txn = db_arc.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 0);
    }

    #[test]
    fn event_handle_system_disabled_filters_system_events() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let db_arc = Arc::new(db);
        let config = Arc::new(system_disabled_config());
        let handle = EventHandle::new(Arc::clone(&db_arc), config, 1);

        let entry = HandlerPhaseEmitter::for_system(EventAction::ConfigurationChanged, 1).build(90);

        handle.record_handler_event(entry);

        let txn = db_arc.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(0)).expect("count");
        assert_eq!(count, 0);
    }

    #[test]
    fn event_handle_clones_share_state() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let db_arc = Arc::new(db);
        let config = Arc::new(test_config());
        let handle1 = EventHandle::new(Arc::clone(&db_arc), Arc::clone(&config), 1);
        let handle2 = handle1.clone();

        // Write via handle1
        let entry = HandlerPhaseEmitter::for_organization(
            EventAction::RequestRateLimited,
            OrganizationId::new(3),
            None,
            1,
        )
        .build(90);
        handle1.record_handler_event(entry);

        // Read via handle2's events_db
        let txn = handle2.events_db().read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(3)).expect("count");
        assert_eq!(count, 1);
    }

    #[test]
    fn event_handle_concurrent_writes() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let db_arc = Arc::new(db);
        let config = Arc::new(test_config());
        let handle = EventHandle::new(Arc::clone(&db_arc), Arc::clone(&config), 1);

        // Simulate concurrent writes from multiple "services"
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let h = handle.clone();
                std::thread::spawn(move || {
                    for j in 0..25u32 {
                        let entry = HandlerPhaseEmitter::for_organization(
                            EventAction::RequestRateLimited,
                            OrganizationId::new(1),
                            None,
                            1,
                        )
                        .detail("thread", &i.to_string())
                        .detail("index", &j.to_string())
                        .build(90);
                        h.record_handler_event(entry);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread join");
        }

        // 4 threads × 25 events = 100 events
        let txn = db_arc.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 100);
    }

    #[test]
    fn event_handle_accessors() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let db_arc = Arc::new(db);
        let config = Arc::new(test_config());
        let handle = EventHandle::new(Arc::clone(&db_arc), Arc::clone(&config), 42);

        assert_eq!(handle.node_id(), 42);
        assert_eq!(*handle.config(), test_config());
        assert!(Arc::ptr_eq(handle.events_db(), &db_arc));
    }

    #[test]
    fn event_handle_stress_rapid_denials() {
        // Simulates denial storm: 10k rapid denials
        let db = EventsDatabase::open_in_memory().expect("open");
        let db_arc = Arc::new(db);
        let config = Arc::new(test_config());
        let handle = EventHandle::new(Arc::clone(&db_arc), Arc::clone(&config), 1);

        for i in 0..10_000u32 {
            let entry = HandlerPhaseEmitter::for_organization(
                EventAction::RequestRateLimited,
                OrganizationId::new(1),
                None,
                1,
            )
            .principal("bot:spammer")
            .outcome(EventOutcome::Denied { reason: "rate_limited".to_string() })
            .detail("request_index", &i.to_string())
            .build(90);
            handle.record_handler_event(entry);
        }

        let txn = db_arc.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 10_000);
    }
}
