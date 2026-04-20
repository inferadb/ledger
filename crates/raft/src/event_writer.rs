//! Event writers for deterministic and handler-phase audit trail generation.
//!
//! Provides:
//!
//! - [`EventWriter`] for persisting batched events to `events.db`
//! - [`ApplyPhaseEmitter`] for constructing deterministic [`EventEntry`] records during Raft state
//!   machine apply (UUID v5, block timestamps, identical on all replicas)
//! - [`HandlerPhaseEmitter`] for constructing node-local [`EventEntry`] records at the gRPC handler
//!   level (UUID v4, wall-clock timestamps, node-specific)
//! - [`EventHandle`] — a cheaply cloneable handle for best-effort handler-phase event writes,
//!   injected into gRPC services
//! - [`EventEmitter`] — trait-erased event emission, enabling
//!   [`RequestContext`](crate::logging::RequestContext) to carry an event handle without being
//!   generic over the storage backend
//! - A background flusher (private `EventFlusher`) that drains the in-memory queue into `events.db`
//!   on a time / size / shutdown trigger — see [`EventHandle::with_batching`]

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use inferadb_ledger_state::{EventStore, EventsDatabase};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    OrganizationId, OrganizationSlug, VaultSlug,
    config::{EventOverflowBehavior, EventWriterBatchConfig},
    events::{EventAction, EventConfig, EventEmission, EventEntry, EventOutcome, EventScope},
};
use snafu::{ResultExt, Snafu};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{metrics, runtime_config::RuntimeConfigHandle};

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

impl<B: StorageBackend> Clone for EventWriter<B> {
    fn clone(&self) -> Self {
        Self { events_db: Arc::clone(&self.events_db), config: self.config.clone() }
    }
}

impl<B: StorageBackend> std::fmt::Debug for EventWriter<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventWriter").field("config", &self.config).finish_non_exhaustive()
    }
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
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(entry_count = entries.len())
    )]
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

        // The events.db apply-path batched write uses `commit_in_memory`.
        // Every EventEntry's event_id is deterministic
        // (UUID v5 over {block_height, op_index, action} — see ApplyPhaseEmitter
        // at event_writer.rs:~292-332), so replay produces byte-identical rows.
        // EventStore::write is idempotent via B-tree upsert semantics; duplicate
        // upsert of identical bytes is safe. Durability realized by
        // StateCheckpointer / snapshot / backup / graceful-shutdown sync.
        txn.commit_in_memory().context(CommitSnafu)?;

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

/// Trait-erased event emission.
///
/// Implemented by [`EventHandle<B>`] to allow [`RequestContext`](crate::logging::RequestContext)
/// to carry an event handle without being generic over the storage backend.
/// This decouples the logging/metrics layer from the concrete storage
/// implementation.
pub trait EventEmitter: Send + Sync {
    /// Records a handler-phase business event (best-effort).
    ///
    /// Implementations should log warnings on failure but never propagate
    /// errors — event persistence must not affect the primary RPC.
    fn record_event(&self, entry: EventEntry);
}

impl<B: StorageBackend + 'static> EventEmitter for EventHandle<B> {
    fn record_event(&self, entry: EventEntry) {
        self.record_handler_event(entry);
    }
}

// =============================================================================
// Handler-phase event batching primitives
// =============================================================================
//
// `record_handler_event` used to fsync events.db once per emission. Under
// concurrent handlers that cost dominated the RPC critical path (42% of
// active CPU in the earlier flamegraph). A bounded in-memory channel +
// background `EventFlusher` now drains the queue on a time / size / shutdown
// trigger and commits once per batch — one fsync per flush window instead of
// one per event.
//
// The contract change: a successful RPC no longer implies the emitted event
// is fsync'd. Events become durable within one flush cadence (default 100ms).
// `enabled = false` bypasses the queue and restores the synchronous per-event
// fsync path — the strict-durability escape hatch for compliance deployments.
// See `docs/superpowers/specs/2026-04-19-sprint-1b4-handler-event-batching-design.md`.

/// Shutdown command sent to the flusher.
///
/// Carries a `oneshot::Sender` on which the flusher replies with the final
/// [`DrainResult`] once it has committed all reachable queued entries (or
/// timed out).
struct ShutdownCommand {
    /// Maximum wall-clock time the flusher may spend draining the queue.
    timeout: Duration,
    /// Reply channel carrying the final drain outcome.
    ack: oneshot::Sender<DrainResult>,
}

/// Outcome of a shutdown drain.
///
/// Returned by [`EventHandle::flush_for_shutdown`] so the caller can emit a
/// lifecycle log line showing how many events made it to disk vs. were
/// dropped to the shutdown deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainResult {
    /// Entries successfully committed to `events.db` during the drain.
    pub drained: u64,
    /// Entries abandoned because the timeout elapsed, the channel closed
    /// early, or a commit error aborted the batch.
    pub lost: u64,
    /// Wall-clock duration of the drain.
    pub duration: Duration,
}

/// Minimum interval at which the flusher polls for time / size triggers.
///
/// Floors the `interval_ms / 4` live-config clamp so sub-4ms flush intervals
/// don't spin the task at the tokio timer's minimum resolution.
const MIN_FLUSHER_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Maximum interval at which the flusher polls for config changes.
///
/// Caps the `interval_ms / 4` clamp so live `UpdateConfig` RPCs that shorten
/// the flush interval take effect within ~1s of the swap landing.
const MAX_FLUSHER_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Emit the overflow warn at most once every this often (per handle).
const OVERFLOW_LOG_THROTTLE: Duration = Duration::from_secs(5);

/// Trigger label for a time-tick flush.
const FLUSH_TRIGGER_TIME: &str = "time";
/// Trigger label for a size-threshold flush.
const FLUSH_TRIGGER_SIZE: &str = "size";
/// Trigger label for a shutdown-drain flush.
const FLUSH_TRIGGER_SHUTDOWN: &str = "shutdown";

/// Overflow cause label for producer-side `try_send` failures.
const OVERFLOW_CAUSE_QUEUE_FULL: &str = "queue_full";
/// Overflow cause label for entries remaining after `flush_for_shutdown` timed out.
const OVERFLOW_CAUSE_SHUTDOWN_TIMEOUT: &str = "shutdown_timeout";
/// Overflow cause label for unexpected channel closure observed by a producer.
const OVERFLOW_CAUSE_CHANNEL_CLOSED: &str = "channel_closed";

/// Bounded in-memory queue between handler-phase emitters and the flusher.
///
/// The sender side is cloned into every [`EventHandle`] produced from the
/// same construction call so all handles feed one flusher. The size-hint
/// counter is maintained by both ends for approximate depth gauges (exact
/// depth is `capacity - sender.capacity()`; the atomic is used for the
/// flusher's size trigger and metrics).
struct FlushQueue {
    /// Producer-side channel handle.
    sender: mpsc::Sender<EventEntry>,
    /// Atomic producer-side drop counter.
    ///
    /// Incremented on every `try_send` failure under
    /// [`EventOverflowBehavior::Drop`]. The counter is sampled by the
    /// flusher on each tick and pushed into the
    /// `ledger_event_overflow_total{cause=queue_full}` counter so operators
    /// see drop trends without per-drop cardinality cost.
    drop_count: Arc<AtomicU64>,
    /// Most recent `Instant` at which a drop-warn was emitted, in
    /// nanoseconds since a fixed epoch. Used with
    /// `OVERFLOW_LOG_THROTTLE` to cap drop-warn spam.
    last_warn_ns: Arc<AtomicU64>,
    /// Approximate queue depth observed by the producer.
    ///
    /// Bumped on successful enqueue, decremented as the flusher drains.
    /// Cheap enough to read on every producer call (single atomic load) for
    /// the size-trigger notification.
    size_hint: Arc<AtomicUsize>,
    /// Notify fired whenever the producer crosses `flush_size_threshold`.
    /// The flusher awaits this alongside the time tick.
    size_notify: Arc<tokio::sync::Notify>,
    /// Shutdown-command sender. The oneshot is consumed on first
    /// `flush_for_shutdown`; subsequent calls see `None`.
    shutdown_tx: parking_lot::Mutex<Option<mpsc::Sender<ShutdownCommand>>>,
    /// Overflow behaviour captured at construction.
    ///
    /// Runtime-reconfigurable via the `RuntimeConfigHandle` the flusher
    /// reads on each tick — but the producer captures the value at
    /// construction time for the fast `try_send` path. A future task can
    /// upgrade this to an `AtomicU8` if hot-swapping overflow semantics
    /// becomes a requirement.
    overflow: EventOverflowBehavior,
    /// Region label for emitted metrics.
    region: String,
}

impl FlushQueue {
    /// Returns the approximate current queue depth.
    fn depth(&self) -> usize {
        self.size_hint.load(Ordering::Relaxed)
    }

    /// Throttled drop-warn. Returns `true` if the log line was emitted.
    fn maybe_warn_drop(&self, total_drops: u64) -> bool {
        // Use a coarse "nanoseconds since process start" clock by leveraging
        // std::time::Instant's monotonic property relative to its elapsed value.
        // Store the last-warn time as u64 nanos from a fixed reference
        // instant captured once per process.
        let now_ns = start_ref().elapsed().as_nanos() as u64;
        let last = self.last_warn_ns.load(Ordering::Relaxed);
        if now_ns.saturating_sub(last) < OVERFLOW_LOG_THROTTLE.as_nanos() as u64 {
            return false;
        }
        // CAS to ensure exactly one warn per interval under contention.
        if self
            .last_warn_ns
            .compare_exchange(last, now_ns, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        warn!(
            region = %self.region,
            drops = total_drops,
            "Handler-phase event queue full — dropping event (best-effort, throttled)"
        );
        true
    }
}

/// Process-wide monotonic reference `Instant` for throttled-warn timekeeping.
///
/// `OnceLock` initialised lazily at first access. Used instead of
/// `SystemTime::now()` so the comparison stays clock-jump-immune.
fn start_ref() -> Instant {
    use std::sync::OnceLock;
    static REF: OnceLock<Instant> = OnceLock::new();
    *REF.get_or_init(Instant::now)
}

/// Cheaply cloneable handle for best-effort handler-phase event writes.
///
/// Injected into gRPC service implementations (`WriteService`,
/// `AdminService`, `ReadService`) to record denial and admin events.
/// Write failures are logged as warnings but never propagate to the caller —
/// the primary RPC must not be affected by event persistence issues.
///
/// Uses the dedicated `events.db` database, separate from `state.db`, so
/// handler-phase writes do not contend with Raft apply-phase state mutations.
///
/// # Batched fsyncs
///
/// Production construction via [`EventHandle::with_batching`] wires in a
/// bounded in-memory queue plus a background flusher task;
/// `record_handler_event` enqueues into the queue instead of opening a
/// write txn per event. The flusher drains on a time / size / shutdown
/// trigger and commits once per batch. Test / legacy constructors (via
/// [`EventHandle::new`]) attach no queue, which restores the synchronous
/// `write_entry` path — byte-identical semantics.
pub struct EventHandle<B: StorageBackend> {
    events_db: Arc<EventsDatabase<B>>,
    config: Arc<EventConfig>,
    node_id: u64,
    /// `None` → fall back to synchronous `write_entry` per emission
    /// (used by tests and strict-durability callers).
    ///
    /// `Some(_)` → enqueue into the flusher queue; one fsync per flush
    /// window instead of one per event.
    queue: Option<Arc<FlushQueue>>,
    /// Handle to the live runtime configuration.
    ///
    /// `None` → construction via [`EventHandle::new`]; there is no
    /// flusher to consult and `record_handler_event` unconditionally uses
    /// the synchronous fallback.
    ///
    /// `Some(_)` → production construction via [`EventHandle::with_batching`].
    /// `record_handler_event` reads `event_writer_batch.enabled` on every
    /// emission — when `false`, the queue is bypassed and the entry is
    /// written through `write_entry` with per-emission fsync
    /// (strict-durability escape hatch).
    runtime_config: Option<RuntimeConfigHandle>,
}

impl<B: StorageBackend> Clone for EventHandle<B> {
    fn clone(&self) -> Self {
        Self {
            events_db: Arc::clone(&self.events_db),
            config: Arc::clone(&self.config),
            node_id: self.node_id,
            queue: self.queue.clone(),
            runtime_config: self.runtime_config.clone(),
        }
    }
}

impl<B: StorageBackend + 'static> EventHandle<B> {
    /// Creates a new event handle with no batching queue attached.
    ///
    /// `record_handler_event` performs a synchronous per-event fsync.
    /// Used by tests and legacy construction sites; production code should
    /// construct via [`EventHandle::with_batching`] instead.
    pub fn new(events_db: Arc<EventsDatabase<B>>, config: Arc<EventConfig>, node_id: u64) -> Self {
        Self { events_db, config, node_id, queue: None, runtime_config: None }
    }

    /// Creates a new event handle wired into a background flusher.
    ///
    /// Spawns an [`EventFlusher`] that drains the returned handle's queue
    /// into `events.db` on a time / size / shutdown trigger. Clones of the
    /// returned handle share the same queue. The handle's
    /// [`flush_for_shutdown`](Self::flush_for_shutdown) stops the flusher
    /// and returns a [`DrainResult`].
    ///
    /// Returns the handle plus the spawned flusher's `JoinHandle` so the
    /// caller can await final termination.
    pub fn with_batching(
        events_db: Arc<EventsDatabase<B>>,
        config: Arc<EventConfig>,
        node_id: u64,
        batch_config: EventWriterBatchConfig,
        runtime_config: RuntimeConfigHandle,
        region: impl Into<String>,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let region: String = region.into();
        let (entry_tx, entry_rx) = mpsc::channel::<EventEntry>(batch_config.queue_capacity);
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<ShutdownCommand>(1);

        let drop_count = Arc::new(AtomicU64::new(0));
        let last_warn_ns = Arc::new(AtomicU64::new(0));
        let size_hint = Arc::new(AtomicUsize::new(0));
        let size_notify = Arc::new(tokio::sync::Notify::new());

        let queue = Arc::new(FlushQueue {
            sender: entry_tx,
            drop_count: Arc::clone(&drop_count),
            last_warn_ns,
            size_hint: Arc::clone(&size_hint),
            size_notify: Arc::clone(&size_notify),
            shutdown_tx: parking_lot::Mutex::new(Some(shutdown_tx)),
            overflow: batch_config.overflow_behavior,
            region: region.clone(),
        });

        info!(
            region = %region,
            flush_interval_ms = batch_config.flush_interval_ms,
            flush_size_threshold = batch_config.flush_size_threshold,
            queue_capacity = batch_config.queue_capacity,
            overflow_behavior = ?batch_config.overflow_behavior,
            "EventFlusher starting"
        );

        let flusher = EventFlusher::<B> {
            events_db: Arc::clone(&events_db),
            config: Arc::clone(&config),
            receiver: entry_rx,
            shutdown_rx,
            size_hint,
            size_notify,
            drop_count,
            runtime_config: runtime_config.clone(),
            region,
            last_announced_capacity: batch_config.queue_capacity,
        };

        let join = tokio::spawn(async move {
            flusher.run().await;
        });

        let handle = Self {
            events_db,
            config,
            node_id,
            queue: Some(queue),
            runtime_config: Some(runtime_config),
        };
        (handle, join)
    }

    /// Records a handler-phase event (best-effort).
    ///
    /// Routing decision:
    ///
    /// - When the handle has a flusher attached ([`Self::with_batching`]) **and**
    ///   `RuntimeConfigHandle::load().event_writer_batch.enabled` is `true` (the default when the
    ///   section is unset), the entry is enqueued into the bounded [`FlushQueue`] and the method
    ///   returns without fsync'ing — the flusher commits on its next time / size / shutdown cycle.
    /// - When `event_writer_batch.enabled = false` is set via the `UpdateConfig` RPC, the queue is
    ///   **bypassed** and the entry is written synchronously through [`Self::write_entry`] with one
    ///   fsync per emission. This is the strict-durability escape hatch for compliance deployments
    ///   (see the design doc § "enabled = false as escape hatch"). Entries already in the queue
    ///   when the flag flips continue to drain on the next flush cycle — no data loss.
    /// - When no flusher is attached ([`Self::new`]), the same synchronous path is taken
    ///   unconditionally.
    ///
    /// Best-effort: write / enqueue failures are logged and swallowed;
    /// callers never observe an error.
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

        // Strict-durability escape hatch: operators can toggle
        // `event_writer_batch.enabled = false` at runtime via `UpdateConfig`
        // RPC to bypass the queue and restore per-emission fsync semantics.
        // Default is `true` (batching is the shipped default); an unset
        // `event_writer_batch` section is also treated as batched,
        // matching `EventWriterBatchConfig::default().enabled`.
        let batch_enabled = self
            .runtime_config
            .as_ref()
            .is_none_or(|rc| rc.load().event_writer_batch.as_ref().is_none_or(|b| b.enabled));

        match (&self.queue, batch_enabled) {
            (Some(queue), true) => self.enqueue_or_fallback(queue, entry, &scope_str, &action_str),
            _ => self.write_sync(entry, &scope_str, &action_str),
        }
    }

    /// Fast-path: enqueue the entry into the flusher queue.
    ///
    /// On `try_send` failure the action depends on
    /// [`EventOverflowBehavior`]: `Drop` records the drop + emits a throttled
    /// warn and returns without writing; `Block` falls back to a
    /// backpressured loop that yields until space is available, bounded so a
    /// jammed flusher can't wedge a producer forever.
    fn enqueue_or_fallback(
        &self,
        queue: &FlushQueue,
        entry: EventEntry,
        scope_str: &str,
        action_str: &str,
    ) {
        match queue.sender.try_send(entry) {
            Ok(()) => {
                queue.size_hint.fetch_add(1, Ordering::Relaxed);
                queue.size_notify.notify_one();
                // Metric is emitted at enqueue time: the queue is the new
                // "accepted by the system" boundary (see design § Metrics).
                metrics::record_event_write("handler_phase", scope_str, action_str);
            },
            Err(mpsc::error::TrySendError::Full(entry)) => match queue.overflow {
                EventOverflowBehavior::Drop => {
                    let total = queue.drop_count.fetch_add(1, Ordering::Relaxed) + 1;
                    metrics::record_event_overflow(&queue.region, OVERFLOW_CAUSE_QUEUE_FULL, 1);
                    queue.maybe_warn_drop(total);
                    // Entry dropped — action/scope strings are left unused here
                    // deliberately: under sustained overflow, emitting one
                    // write-counter increment per drop would mask the overflow
                    // signal. The throttled warn + overflow counter is the
                    // full observability.
                    let _ = (scope_str, action_str);
                    let _ = entry;
                },
                EventOverflowBehavior::Block => {
                    // Sync producer — we cannot `await`. Fall back to a
                    // blocking loop with short sleeps; the producer thread
                    // yields while the flusher drains. If the channel is
                    // closed mid-loop, treat it as an overflow under
                    // `channel_closed`.
                    let scope_owned = scope_str.to_string();
                    let action_owned = action_str.to_string();
                    let mut payload = Some(entry);
                    while let Some(next) = payload.take() {
                        match queue.sender.try_send(next) {
                            Ok(()) => {
                                queue.size_hint.fetch_add(1, Ordering::Relaxed);
                                queue.size_notify.notify_one();
                                metrics::record_event_write(
                                    "handler_phase",
                                    &scope_owned,
                                    &action_owned,
                                );
                                break;
                            },
                            Err(mpsc::error::TrySendError::Full(e)) => {
                                payload = Some(e);
                                std::thread::sleep(Duration::from_millis(1));
                            },
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                metrics::record_event_overflow(
                                    &queue.region,
                                    OVERFLOW_CAUSE_CHANNEL_CLOSED,
                                    1,
                                );
                                warn!(
                                    region = %queue.region,
                                    action = action_owned,
                                    scope = scope_owned,
                                    "Handler-phase event channel closed (best-effort, dropping)"
                                );
                                break;
                            },
                        }
                    }
                },
            },
            Err(mpsc::error::TrySendError::Closed(_)) => {
                metrics::record_event_overflow(&queue.region, OVERFLOW_CAUSE_CHANNEL_CLOSED, 1);
                warn!(
                    region = %queue.region,
                    action = action_str,
                    scope = scope_str,
                    "Handler-phase event channel closed (best-effort, dropping)"
                );
            },
        }
    }

    /// Synchronous fallback path: one fsync per entry.
    fn write_sync(&self, entry: EventEntry, scope_str: &str, action_str: &str) {
        match self.write_entry(&entry) {
            Ok(()) => {
                metrics::record_event_write("handler_phase", scope_str, action_str);
            },
            Err(e) => {
                warn!(
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
        // DO NOT flip to `commit_in_memory` — handler-phase events have no WAL
        // replay backstop (per design § "STAYS on commit" + consensus review
        // Critical finding). The background flusher amortises the fsync cost;
        // this path is the escape-hatch / fallback only (queue = None or
        // enabled = false).
        txn.commit().context(CommitSnafu)?;
        Ok(())
    }

    /// Drains the flusher queue and stops the background task.
    ///
    /// Call sites: `pre_shutdown` at graceful-shutdown Phase 5b (see the
    /// design doc; wired up in `server/src/main.rs`). Returns a
    /// [`DrainResult`] so the caller can
    /// emit a final lifecycle log line.
    ///
    /// On a handle with no batching queue attached, returns a zeroed
    /// `DrainResult` immediately. Subsequent calls after the first
    /// shutdown see a closed channel and also return zeroed results.
    pub async fn flush_for_shutdown(&self, timeout: Duration) -> DrainResult {
        let queue = match &self.queue {
            Some(q) => q,
            None => {
                return DrainResult { drained: 0, lost: 0, duration: Duration::from_secs(0) };
            },
        };

        let sender = match queue.shutdown_tx.lock().take() {
            Some(tx) => tx,
            None => {
                return DrainResult { drained: 0, lost: 0, duration: Duration::from_secs(0) };
            },
        };

        let (ack_tx, ack_rx) = oneshot::channel();
        let cmd = ShutdownCommand { timeout, ack: ack_tx };
        // Mirror the shutdown signal to the flusher. Use `send().await` on
        // the shutdown channel (capacity 1) so we don't race a second
        // caller. If the flusher has already exited (dropped its receiver)
        // we surface that as a zeroed result.
        if sender.send(cmd).await.is_err() {
            return DrainResult { drained: 0, lost: 0, duration: Duration::from_secs(0) };
        }
        match ack_rx.await {
            Ok(result) => result,
            Err(_) => DrainResult {
                drained: 0,
                lost: queue.depth() as u64,
                duration: Duration::from_secs(0),
            },
        }
    }

    /// Returns the current approximate queue depth. `0` if no queue is
    /// attached (test / strict-durability constructors).
    #[must_use]
    pub fn queue_depth(&self) -> usize {
        self.queue.as_ref().map(|q| q.depth()).unwrap_or(0)
    }

    /// Returns the total number of enqueue-drop events observed by
    /// producers using this handle, across all clones. `0` if no queue.
    ///
    /// Primarily a test affordance; operational monitoring uses
    /// `ledger_event_overflow_total{cause=queue_full}`.
    #[must_use]
    pub fn dropped_count(&self) -> u64 {
        self.queue.as_ref().map(|q| q.drop_count.load(Ordering::Relaxed)).unwrap_or(0)
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

/// Background task that drains [`FlushQueue`] into `events.db`.
///
/// One flusher per `EventHandle` construction. Runs until it receives a
/// shutdown command via the oneshot `shutdown_rx`; does NOT register with
/// any `CancellationToken` — the flusher must keep draining through the
/// connection-drain phase of graceful shutdown so handler-phase emissions
/// that land during in-flight RPC completion still commit on cadence.
///
/// Fire triggers (any one activates the tick):
/// - **Time**: `flush_interval_ms` elapsed since the last successful drain.
/// - **Size**: queue depth >= `flush_size_threshold`.
/// - **Shutdown**: `flush_for_shutdown` called (drain-then-exit).
///
/// **Durability class.** Each tick commits the batch with
/// `commit_in_memory` — the per-flush dual-slot fsync was removed to unblock
/// throughput under concurrent-write workloads. Durability for handler-phase
/// events is realised by
/// [`StateCheckpointer`](crate::state_checkpointer::StateCheckpointer),
/// which syncs `events.db` alongside the other state DBs on every
/// `checkpoint_interval_ms` tick (default 500ms). Clean-shutdown zero-loss
/// is preserved via Phase 5b (`flush_for_shutdown` drains the queue) →
/// Phase 5c (`sync_all_state_dbs` syncs `events.db`) in the server's
/// `pre_shutdown` closure. See
/// `docs/superpowers/specs/2026-04-19-commit-durability-audit.md`
/// for the full audit entry.
struct EventFlusher<B: StorageBackend> {
    events_db: Arc<EventsDatabase<B>>,
    config: Arc<EventConfig>,
    receiver: mpsc::Receiver<EventEntry>,
    shutdown_rx: mpsc::Receiver<ShutdownCommand>,
    size_hint: Arc<AtomicUsize>,
    size_notify: Arc<tokio::sync::Notify>,
    /// Shared producer-side drop counter. The flusher samples this once per
    /// tick into a local accumulator so dashboards see a monotonically
    /// non-decreasing counter (see § Metrics in the design doc).
    #[allow(dead_code)]
    drop_count: Arc<AtomicU64>,
    runtime_config: RuntimeConfigHandle,
    region: String,
    /// Most recently-observed `queue_capacity` — used to detect
    /// restart-only updates and warn operators once.
    last_announced_capacity: usize,
}

impl<B: StorageBackend + 'static> EventFlusher<B> {
    /// Reads the current batch config off the runtime handle.
    ///
    /// Falls back to `EventWriterBatchConfig::default()` if the section is
    /// unset — same graceful-degradation pattern as
    /// [`StateCheckpointer::current_config`](crate::state_checkpointer).
    fn current_config(&self) -> EventWriterBatchConfig {
        self.runtime_config.load().event_writer_batch.clone().unwrap_or_default()
    }

    /// Runs the flusher until `shutdown_rx` delivers a command.
    async fn run(mut self) {
        let mut last_flush_at = Instant::now();

        loop {
            let config = self.current_config();

            if config.queue_capacity != self.last_announced_capacity {
                warn!(
                    region = %self.region,
                    old = self.last_announced_capacity,
                    new = config.queue_capacity,
                    "queue_capacity change detected; restart required to take effect"
                );
                self.last_announced_capacity = config.queue_capacity;
            }

            let interval = Duration::from_millis(config.flush_interval_ms.max(1));
            let poll_interval =
                (interval / 4).max(MIN_FLUSHER_POLL_INTERVAL).min(MAX_FLUSHER_POLL_INTERVAL);

            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {
                    // Time / size tick — handled below.
                },
                _ = self.size_notify.notified() => {
                    // Producer signalled a possible size threshold crossing;
                    // fall through to tick() and re-check against config.
                },
                cmd = self.shutdown_rx.recv() => {
                    let Some(cmd) = cmd else {
                        // Queue's shutdown sender dropped without signalling.
                        // Exit quietly — nothing we can do about it.
                        info!(region = %self.region, "EventFlusher exiting (shutdown channel closed)");
                        return;
                    };
                    let result = self.final_drain(&config, cmd.timeout).await;
                    let _ = cmd.ack.send(result);
                    info!(
                        region = %self.region,
                        drained = result.drained,
                        lost = result.lost,
                        duration_ms = result.duration.as_millis() as u64,
                        "EventFlusher shutting down"
                    );
                    return;
                },
            }

            // Decide whether to flush. OR-triggered: time elapsed OR size
            // threshold crossed.
            let depth = self.size_hint.load(Ordering::Relaxed);
            let time_fired = last_flush_at.elapsed() >= interval;
            let size_fired = depth >= config.flush_size_threshold;
            if !time_fired && !size_fired {
                continue;
            }

            let trigger = if size_fired { FLUSH_TRIGGER_SIZE } else { FLUSH_TRIGGER_TIME };
            self.tick(&config, trigger).await;
            last_flush_at = Instant::now();
        }
    }

    /// Drains up to `drain_batch_max` entries from the channel and commits
    /// them in a single events.db write txn.
    ///
    /// Returns `(drained, duration)`. On commit / write failure, the
    /// drained entries are lost (see design § Error handling) —
    /// retries risk partial duplication. The failure counter + warn is the
    /// operator's signal.
    async fn tick(&mut self, config: &EventWriterBatchConfig, trigger: &'static str) {
        let depth_before = self.size_hint.load(Ordering::Relaxed) as u64;
        metrics::set_event_flush_queue_depth(&self.region, depth_before);

        let start = Instant::now();
        let mut drained = Vec::with_capacity(config.drain_batch_max.min(1024));

        for _ in 0..config.drain_batch_max {
            match self.receiver.try_recv() {
                Ok(entry) => {
                    drained.push(entry);
                },
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // Producers gone (should only happen at handle-drop in
                    // tests). Fall through with what we've collected.
                    break;
                },
            }
        }

        let drained_count = drained.len() as u64;
        if drained.is_empty() {
            return;
        }

        // Commit in one txn — the whole point of the sprint.
        match self.commit_batch(&drained).await {
            Ok(()) => {
                self.size_hint.fetch_sub(drained.len(), Ordering::Relaxed);
                let duration = start.elapsed();
                metrics::record_event_flush(
                    &self.region,
                    trigger,
                    duration.as_secs_f64(),
                    drained_count,
                );
                debug!(
                    region = %self.region,
                    trigger,
                    drained = drained_count,
                    duration_ms = duration.as_millis() as u64,
                    queue_depth = depth_before,
                    "event-flush complete"
                );
            },
            Err(e) => {
                // Drained entries are gone from the channel; we lose them.
                // The metric + warn is the signal.
                self.size_hint.fetch_sub(drained.len(), Ordering::Relaxed);
                metrics::record_event_flush_failure(&self.region);
                warn!(
                    error = %e,
                    region = %self.region,
                    trigger,
                    dropped = drained_count,
                    "event-flush failure — dropping batch"
                );
            },
        }
    }

    /// Opens a single events.db write txn, writes each entry, commits
    /// in-memory only.
    ///
    /// Commits via `commit_in_memory()` rather than strict-durable `commit()`.
    /// The per-flush dual-slot fsync was the dominant cost under
    /// `concurrent-writes @ 32` (~12% of wall-clock). Durability is realised
    /// by `StateCheckpointer`, which syncs `events.db` on
    /// every tick (default `checkpoint_interval_ms = 500ms` — see
    /// `crates/raft/src/state_checkpointer.rs` and
    /// `crates/types/src/config/storage.rs` `default_checkpoint_interval_ms`).
    /// The crash-loss window widens from "last flusher tick" (up to
    /// `flush_interval_ms`, default 100ms) to "last checkpointer tick" (up
    /// to `checkpoint_interval_ms`, default 500ms). Graceful shutdown still
    /// preserves zero-loss via Phase 5b (`flush_for_shutdown` drains the
    /// queue with `commit_in_memory`) immediately followed by Phase 5c
    /// (`RaftManager::sync_all_state_dbs` syncs `events.db`); see
    /// `crates/server/src/main.rs` `pre_shutdown` closure.
    ///
    /// Operators who require the narrower window set
    /// `EventWriterBatchConfig::enabled = false` (see `write_entry` escape
    /// hatch — unchanged, still strict-durable per invocation).
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(entry_count = entries.len(), region = %self.region)
    )]
    async fn commit_batch(&self, entries: &[EventEntry]) -> Result<(), EventWriterError> {
        let mut txn = self.events_db.write().context(TransactionSnafu)?;
        for entry in entries {
            // Scope filtering is enforced at enqueue time in
            // `record_handler_event`. Once an entry is in the queue, the
            // flusher trusts the scope gate and writes without re-checking
            // — the scope flags are effectively immutable for the duration
            // of a node.
            let _ = self.config.enabled;
            EventStore::write(&mut txn, entry).context(WriteSnafu)?;
        }
        // `commit_in_memory` skips the dual-slot persist.
        // `StateCheckpointer::do_checkpoint` invokes `sync_state` on
        // events.db alongside state.db / raft.db / blocks.db on every tick;
        // graceful shutdown's Phase 5c sweep (`sync_all_state_dbs`) closes
        // the window on clean exit.
        txn.commit_in_memory().context(CommitSnafu)?;
        Ok(())
    }

    /// Drains every remaining entry (subject to `timeout` and `drain_batch_max`
    /// chunking) and returns the final [`DrainResult`].
    async fn final_drain(
        &mut self,
        config: &EventWriterBatchConfig,
        timeout: Duration,
    ) -> DrainResult {
        let start = Instant::now();
        let mut drained_total: u64 = 0;
        let mut lost_total: u64 = 0;

        loop {
            if start.elapsed() >= timeout {
                // Remaining entries are lost. Surface via the overflow
                // counter so dashboards see shutdown-timeout drops.
                let remaining = self.size_hint.load(Ordering::Relaxed) as u64;
                if remaining > 0 {
                    metrics::record_event_overflow(
                        &self.region,
                        OVERFLOW_CAUSE_SHUTDOWN_TIMEOUT,
                        remaining,
                    );
                    warn!(
                        region = %self.region,
                        dropped = remaining,
                        timeout_ms = timeout.as_millis() as u64,
                        "flush_for_shutdown exceeded timeout — dropping remaining events"
                    );
                    lost_total = lost_total.saturating_add(remaining);
                }
                break;
            }

            let mut batch = Vec::with_capacity(config.drain_batch_max.min(1024));
            for _ in 0..config.drain_batch_max {
                match self.receiver.try_recv() {
                    Ok(entry) => batch.push(entry),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            if batch.is_empty() {
                break;
            }

            let drained_this_batch = batch.len() as u64;
            match self.commit_batch(&batch).await {
                Ok(()) => {
                    self.size_hint.fetch_sub(batch.len(), Ordering::Relaxed);
                    drained_total = drained_total.saturating_add(drained_this_batch);
                },
                Err(e) => {
                    self.size_hint.fetch_sub(batch.len(), Ordering::Relaxed);
                    metrics::record_event_flush_failure(&self.region);
                    warn!(
                        error = %e,
                        region = %self.region,
                        trigger = FLUSH_TRIGGER_SHUTDOWN,
                        dropped = drained_this_batch,
                        "event-flush failure during shutdown drain — dropping batch"
                    );
                    lost_total = lost_total.saturating_add(drained_this_batch);
                },
            }
        }

        let duration = start.elapsed();
        let duration_secs = duration.as_secs_f64();
        if drained_total > 0 {
            metrics::record_event_flush(
                &self.region,
                FLUSH_TRIGGER_SHUTDOWN,
                duration_secs,
                drained_total,
            );
        }
        DrainResult { drained: drained_total, lost: lost_total, duration }
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

    // ── commit-in-memory semantics ─────────

    /// `write_events` commits in-memory only; `sync_state` advances
    /// `last_synced_snapshot_id`. Mirrors the
    /// `save_state_core_commits_in_memory_only_then_sync_advances_snapshot`
    /// test pattern.
    #[tokio::test]
    async fn write_events_commits_in_memory_only_until_sync() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = EventsDatabase::open(dir.path()).expect("open events db");
        let db_arc = Arc::new(events_db);
        let writer = EventWriter::new(Arc::clone(&db_arc), test_config());

        let synced_before = db_arc.db().last_synced_snapshot_id();

        // Deterministically constructed entries (identical to what an apply
        // pipeline would produce on any replica).
        let entries: Vec<EventEntry> = (0..3u32)
            .map(|i| {
                ApplyPhaseEmitter::for_organization(
                    EventAction::WriteCommitted,
                    OrganizationId::new(1),
                    Some(OrganizationSlug::new(100)),
                )
                .principal("user:alice")
                .operations_count(1)
                .build(42, i, sample_timestamp(), 90)
            })
            .collect();

        let count = writer.write_events(&entries).expect("write_events");
        assert_eq!(count, 3);

        // In-process read-your-own-writes: events are visible immediately.
        let txn = db_arc.read().expect("read");
        let stored = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(stored, 3, "events must be readable in-process after write_events");
        drop(txn);

        // `last_synced_snapshot_id` has NOT advanced — `commit_in_memory`
        // skips the dual-slot persist.
        let synced_after_commit = db_arc.db().last_synced_snapshot_id();
        assert_eq!(
            synced_after_commit, synced_before,
            "write_events must not advance last_synced_snapshot_id \
             (commit_in_memory skips the dual-slot persist)"
        );

        // Forcing a sync advances the synced id.
        Arc::clone(db_arc.db()).sync_state().await.expect("sync_state");
        let synced_after_sync = db_arc.db().last_synced_snapshot_id();
        assert!(
            synced_after_sync > synced_before,
            "sync_state must advance last_synced_snapshot_id past the \
             in-memory commit (before={synced_before} after={synced_after_sync})"
        );

        // Drop the writer + db handle so the file lock releases, then reopen
        // and assert the events survived the sync.
        drop(writer);
        drop(db_arc);

        let reopened = EventsDatabase::open(dir.path()).expect("reopen events db");
        let txn = reopened.read().expect("read reopened");
        let stored_after_reopen =
            EventStore::count(&txn, OrganizationId::new(1)).expect("count after reopen");
        assert_eq!(
            stored_after_reopen, 3,
            "synced events must survive a reopen of the events database"
        );
    }

    /// Guard: `write_entry` (handler-phase) STAYS strict-durable. Mirrors
    /// the `save_vote_is_durable_before_returning` test. Handler-phase events
    /// have no WAL replay backstop; losing them on crash is the audit finding
    /// the DO-NOT-FLIP comment cites.
    #[tokio::test]
    async fn write_entry_stays_durable_per_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = EventsDatabase::open(dir.path()).expect("open events db");
        let db_arc = Arc::new(events_db);
        let handle = EventHandle::new(Arc::clone(&db_arc), Arc::new(test_config()), 1);

        let synced_before = db_arc.db().last_synced_snapshot_id();

        let entry = HandlerPhaseEmitter::for_organization(
            EventAction::RequestRateLimited,
            OrganizationId::new(1),
            None,
            1,
        )
        .principal("user:alice")
        .outcome(EventOutcome::Denied { reason: "rate_limited".to_string() })
        .build(90);

        // Call the private `write_entry` path directly (what
        // `record_handler_event` wraps).
        handle.write_entry(&entry).expect("write_entry");

        let synced_after = db_arc.db().last_synced_snapshot_id();
        assert!(
            synced_after > synced_before,
            "write_entry MUST advance last_synced_snapshot_id before returning \
             (handler-phase has no WAL replay backstop): \
             before={synced_before} after={synced_after}"
        );

        // And the entry is readable after the call.
        let txn = db_arc.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 1);
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

    // =========================================================================
    // Handler-phase event batching
    // =========================================================================

    use inferadb_ledger_types::config::{
        EventOverflowBehavior, EventWriterBatchConfig, RuntimeConfig,
    };

    use crate::runtime_config::RuntimeConfigHandle;

    fn sample_handler_entry(index: u32) -> EventEntry {
        HandlerPhaseEmitter::for_organization(
            EventAction::RequestRateLimited,
            OrganizationId::new(1),
            None,
            1,
        )
        .principal("user:alice")
        .outcome(EventOutcome::Denied { reason: "rate_limited".to_string() })
        .detail("index", &index.to_string())
        .build(90)
    }

    fn runtime_handle_with_batch(batch: EventWriterBatchConfig) -> RuntimeConfigHandle {
        RuntimeConfigHandle::new(RuntimeConfig {
            event_writer_batch: Some(batch),
            ..RuntimeConfig::default()
        })
    }

    /// Test 1: enqueue path is taken when a flusher is wired and the config
    /// has `enabled = true`. The event is not yet visible via the events.db
    /// read path immediately after `record_handler_event` returns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn record_handler_event_enqueues_when_enabled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = Arc::new(EventsDatabase::open(dir.path()).expect("open"));
        let config = Arc::new(test_config());

        // Huge intervals / capacity so the flusher WON'T drain on its own
        // before we assert.
        let batch = EventWriterBatchConfig::builder()
            .flush_interval_ms(60_000)
            .flush_size_threshold(10_000)
            .queue_capacity(100)
            .build()
            .unwrap();
        let runtime = runtime_handle_with_batch(batch.clone());

        let (handle, join) = EventHandle::with_batching(
            Arc::clone(&events_db),
            Arc::clone(&config),
            1,
            batch,
            runtime,
            "test-region",
        );

        handle.record_handler_event(sample_handler_entry(1));
        // Enqueue bumps the size hint.
        assert_eq!(handle.queue_depth(), 1);

        // Stop the flusher before scope ends.
        let result = handle.flush_for_shutdown(Duration::from_secs(5)).await;
        assert_eq!(result.drained, 1);
        assert_eq!(result.lost, 0);
        join.await.expect("flusher task exit");

        // Visible after the shutdown drain.
        let txn = events_db.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 1);
    }

    /// Test 2: a handle constructed via `EventHandle::new` has no queue
    /// attached, so `record_handler_event` always routes through the
    /// synchronous fallback and makes the event visible immediately.
    /// The `enabled = false` runtime-config escape hatch on a batched
    /// handle is covered by the `proptest_flush_correctness` module's
    /// `disabled_flag_matches_batched_path_plus_flush`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn record_handler_event_falls_back_when_no_queue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = Arc::new(EventsDatabase::open(dir.path()).expect("open"));
        let config = Arc::new(test_config());

        // No flusher — this exercises the synchronous fallback path.
        let handle = EventHandle::new(Arc::clone(&events_db), config, 1);
        handle.record_handler_event(sample_handler_entry(1));

        // Synchronous write + fsync — visible IMMEDIATELY after record
        // returns (this is the whole point of the fallback path).
        let txn = events_db.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 1);
    }

    /// Guard: `EventFlusher::commit_batch` commits in-memory only. Mirrors
    /// `write_events_commits_in_memory_only_until_sync` — enqueue events,
    /// trigger a flush via `flush_for_shutdown`, assert the entries are
    /// visible via in-process reads but `last_synced_snapshot_id` has NOT
    /// advanced. Forcing `sync_state` then advances it.
    ///
    /// Under a strict-durable contract, `commit_batch` would call `commit()`
    /// and `last_synced_snapshot_id` would advance as part of the flush.
    /// This test fires exactly when that contract is restored by accident.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_batch_commits_in_memory_only_until_sync() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = Arc::new(EventsDatabase::open(dir.path()).expect("open"));
        let config = Arc::new(test_config());

        // Default flusher cadence is fine — we drive the drain ourselves via
        // `flush_for_shutdown` so timing jitter doesn't affect the assertion.
        let batch = EventWriterBatchConfig::builder()
            .flush_interval_ms(60_000)
            .flush_size_threshold(10_000)
            .queue_capacity(100)
            .drain_batch_max(100)
            .build()
            .expect("valid batch cfg");
        let runtime = runtime_handle_with_batch(batch.clone());

        let (handle, join) = EventHandle::with_batching(
            Arc::clone(&events_db),
            Arc::clone(&config),
            1,
            batch,
            runtime,
            "test-region",
        );

        let synced_before = events_db.db().last_synced_snapshot_id();

        // Enqueue three events — these sit in the mpsc queue until the
        // shutdown drain fires.
        for i in 0..3 {
            handle.record_handler_event(sample_handler_entry(i));
        }
        assert_eq!(handle.queue_depth(), 3);

        // Drain via the shutdown path. `commit_batch` uses `commit_in_memory`,
        // so the dual-slot persist is skipped.
        let drain = handle.flush_for_shutdown(Duration::from_secs(5)).await;
        assert_eq!(drain.drained, 3, "shutdown drain must commit every queued entry");
        assert_eq!(drain.lost, 0);
        join.await.expect("flusher task exit");

        // Read-your-own-writes through an in-process txn works immediately
        // after `commit_in_memory` — dirty pages are visible to any reader
        // on the same process.
        let txn = events_db.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(
            count, 3,
            "events must be readable in-process after commit_batch (in-memory commit)"
        );
        drop(txn);

        // `last_synced_snapshot_id` MUST NOT have advanced — this is the
        // commit-in-memory contract. `commit_in_memory` skips the dual-slot
        // persist; durability is the StateCheckpointer's job.
        let synced_after_commit = events_db.db().last_synced_snapshot_id();
        assert_eq!(
            synced_after_commit, synced_before,
            "commit_batch must not advance last_synced_snapshot_id \
             (commit_in_memory skips the dual-slot persist)"
        );

        // Forcing a sync (what the StateCheckpointer does on every tick)
        // advances the synced id, making the events durable across crashes.
        Arc::clone(events_db.db()).sync_state().await.expect("sync_state");
        let synced_after_sync = events_db.db().last_synced_snapshot_id();
        assert!(
            synced_after_sync > synced_before,
            "sync_state must advance last_synced_snapshot_id past the \
             in-memory flusher commit (before={synced_before} after={synced_after_sync})"
        );

        // Reopen the DB and confirm the events survived the sync. This is
        // the end-to-end proof that commit_in_memory + sync_state yields a
        // durable outcome.
        drop(handle);
        let db_path = dir.path().to_path_buf();
        drop(events_db);

        let reopened = EventsDatabase::open(&db_path).expect("reopen events db");
        let txn = reopened.read().expect("read reopened");
        let stored_after_reopen =
            EventStore::count(&txn, OrganizationId::new(1)).expect("count after reopen");
        assert_eq!(
            stored_after_reopen, 3,
            "synced flusher events must survive a reopen of the events database"
        );
    }

    /// Test 3: time trigger drains the queue after `flush_interval_ms`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flusher_time_trigger_fires_after_interval() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = Arc::new(EventsDatabase::open(dir.path()).expect("open"));
        let config = Arc::new(test_config());

        let batch = EventWriterBatchConfig::builder()
            .flush_interval_ms(50)
            .flush_size_threshold(10_000)
            .queue_capacity(100)
            .drain_batch_max(100)
            .build()
            .unwrap();
        let runtime = runtime_handle_with_batch(batch.clone());

        let (handle, join) = EventHandle::with_batching(
            Arc::clone(&events_db),
            Arc::clone(&config),
            1,
            batch,
            runtime,
            "test-region",
        );

        for i in 0..3 {
            handle.record_handler_event(sample_handler_entry(i));
        }

        // Wait for time trigger to fire at least once.
        let mut observed = 0;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let txn = events_db.read().expect("read");
            observed = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
            if observed >= 3 {
                break;
            }
        }
        assert_eq!(observed, 3, "time trigger should have drained all 3 entries");

        let _ = handle.flush_for_shutdown(Duration::from_secs(5)).await;
        join.await.expect("flusher task exit");
    }

    /// Test 4: size trigger fires before time trigger when the queue
    /// crosses `flush_size_threshold`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flusher_size_trigger_fires_at_threshold() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = Arc::new(EventsDatabase::open(dir.path()).expect("open"));
        let config = Arc::new(test_config());

        // 5s flush interval + size threshold 5 — size trigger must fire
        // first.
        let batch = EventWriterBatchConfig::builder()
            .flush_interval_ms(5_000)
            .flush_size_threshold(5)
            .queue_capacity(100)
            .drain_batch_max(100)
            .build()
            .unwrap();
        let runtime = runtime_handle_with_batch(batch.clone());

        let (handle, join) = EventHandle::with_batching(
            Arc::clone(&events_db),
            Arc::clone(&config),
            1,
            batch,
            runtime,
            "test-region",
        );

        for i in 0..5 {
            handle.record_handler_event(sample_handler_entry(i));
        }

        // Wait up to 1s — size trigger should fire well before that.
        let mut observed = 0;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let txn = events_db.read().expect("read");
            observed = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
            if observed >= 5 {
                break;
            }
        }
        assert_eq!(observed, 5, "size trigger should have drained all 5 entries quickly");

        let _ = handle.flush_for_shutdown(Duration::from_secs(5)).await;
        join.await.expect("flusher task exit");
    }

    /// Test 5: under `overflow_behavior = Drop` + tiny capacity, enqueue
    /// failures increment the drop counter and the `ledger_event_overflow_total`
    /// metric. Expected behaviour: producer loses events, RPC never fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overflow_drops_when_queue_full() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = Arc::new(EventsDatabase::open(dir.path()).expect("open"));
        let config = Arc::new(test_config());

        // Capacity 2, huge intervals — we must exceed capacity before the
        // flusher gets a chance to drain.
        let batch = EventWriterBatchConfig::builder()
            .flush_interval_ms(60_000)
            .flush_size_threshold(10_000)
            .queue_capacity(2)
            .overflow_behavior(EventOverflowBehavior::Drop)
            .build()
            .unwrap();
        let runtime = runtime_handle_with_batch(batch.clone());

        let (handle, join) = EventHandle::with_batching(
            Arc::clone(&events_db),
            Arc::clone(&config),
            1,
            batch,
            runtime,
            "test-region",
        );

        // 5 entries — capacity 2 → at least 3 drops.
        for i in 0..5 {
            handle.record_handler_event(sample_handler_entry(i));
        }
        assert!(
            handle.dropped_count() >= 3,
            "expected at least 3 drops, got {}",
            handle.dropped_count()
        );

        let _ = handle.flush_for_shutdown(Duration::from_secs(5)).await;
        join.await.expect("flusher task exit");
    }

    /// Test 6: under `overflow_behavior = Block`, a producer trying to
    /// enqueue into a full queue blocks. Bound the test with
    /// `tokio::time::timeout` so a broken implementation can't hang
    /// indefinitely.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overflow_blocks_when_block_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = Arc::new(EventsDatabase::open(dir.path()).expect("open"));
        let config = Arc::new(test_config());

        // Capacity 2 + Block mode + huge intervals — the 3rd producer must
        // block. We pre-fill, then launch the 3rd on a blocking thread so
        // it loops waiting for space.
        let batch = EventWriterBatchConfig::builder()
            .flush_interval_ms(60_000)
            .flush_size_threshold(10_000)
            .queue_capacity(2)
            .overflow_behavior(EventOverflowBehavior::Block)
            .build()
            .unwrap();
        let runtime = runtime_handle_with_batch(batch.clone());

        let (handle, join) = EventHandle::with_batching(
            Arc::clone(&events_db),
            Arc::clone(&config),
            1,
            batch,
            runtime,
            "test-region",
        );

        // Fill the queue to capacity.
        handle.record_handler_event(sample_handler_entry(0));
        handle.record_handler_event(sample_handler_entry(1));
        assert_eq!(handle.queue_depth(), 2);

        // Third producer must block. Run in a blocking task so we can
        // observe the block + timeout.
        let handle_for_producer = handle.clone();
        let block_task = tokio::task::spawn_blocking(move || {
            handle_for_producer.record_handler_event(sample_handler_entry(2));
        });

        // Verify the producer is still blocked after a reasonable wait.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!block_task.is_finished(), "producer should be blocked waiting for queue space");

        // Free the block by running a shutdown drain — the drained slot
        // lets the blocked producer's try_send succeed.
        let shutdown_handle = handle.clone();
        let drain_task = tokio::spawn(async move {
            shutdown_handle.flush_for_shutdown(Duration::from_secs(2)).await
        });

        // The blocked producer eventually unblocks once space is available.
        // Allow up to 2s — generous, but caps a broken implementation.
        let join_result = tokio::time::timeout(Duration::from_secs(2), block_task).await;
        assert!(join_result.is_ok(), "blocked producer did not unblock within timeout after drain");
        join_result.expect("timed").expect("producer join");

        let _ = drain_task.await.expect("drain join");
        join.await.expect("flusher task exit");
    }

    /// Test 7: updating `flush_interval_ms` via `RuntimeConfigHandle` is
    /// picked up on the flusher's next tick.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_config_update_takes_effect_on_next_tick() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = Arc::new(EventsDatabase::open(dir.path()).expect("open"));
        let config = Arc::new(test_config());

        // Start with huge intervals — nothing fires on its own.
        let initial = EventWriterBatchConfig::builder()
            .flush_interval_ms(60_000)
            .flush_size_threshold(10_000)
            .queue_capacity(100)
            .drain_batch_max(100)
            .build()
            .unwrap();
        let runtime = runtime_handle_with_batch(initial.clone());

        let (handle, join) = EventHandle::with_batching(
            Arc::clone(&events_db),
            Arc::clone(&config),
            1,
            initial,
            runtime.clone(),
            "test-region",
        );

        handle.record_handler_event(sample_handler_entry(1));
        // Under the initial (huge) interval nothing should drain.
        tokio::time::sleep(Duration::from_millis(250)).await;
        {
            let txn = events_db.read().expect("read");
            let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
            assert_eq!(count, 0, "no drain should have fired with flush_interval_ms=60_000");
        }

        // Swap in a tight interval. The flusher picks up the new value on
        // the next tick (bounded by MAX_FLUSHER_POLL_INTERVAL).
        let tight = EventWriterBatchConfig::builder()
            .flush_interval_ms(50)
            .flush_size_threshold(10_000)
            .queue_capacity(100)
            .drain_batch_max(100)
            .build()
            .unwrap();
        runtime
            .store(RuntimeConfig { event_writer_batch: Some(tight), ..RuntimeConfig::default() });

        let mut observed = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let txn = events_db.read().expect("read");
            let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
            if count >= 1 {
                observed = true;
                break;
            }
        }
        assert!(observed, "next tick should pick up the tightened flush interval");

        let _ = handle.flush_for_shutdown(Duration::from_secs(5)).await;
        join.await.expect("flusher task exit");
    }

    /// Test 8: shutdown drain commits every queued entry when the
    /// timeout is generous, returning a `DrainResult` reflecting the
    /// work done.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_for_shutdown_drains_queue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = Arc::new(EventsDatabase::open(dir.path()).expect("open"));
        let config = Arc::new(test_config());

        let batch = EventWriterBatchConfig::builder()
            .flush_interval_ms(60_000)
            .flush_size_threshold(10_000)
            .queue_capacity(100)
            .drain_batch_max(50)
            .build()
            .unwrap();
        let runtime = runtime_handle_with_batch(batch.clone());

        let (handle, join) = EventHandle::with_batching(
            Arc::clone(&events_db),
            Arc::clone(&config),
            1,
            batch,
            runtime,
            "test-region",
        );

        for i in 0..42 {
            handle.record_handler_event(sample_handler_entry(i));
        }

        let result = handle.flush_for_shutdown(Duration::from_secs(5)).await;
        assert_eq!(result.drained, 42);
        assert_eq!(result.lost, 0);
        join.await.expect("flusher task exit");

        let txn = events_db.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 42);
    }

    /// Handle clones share the same queue — both producers feed the
    /// single flusher and both observe the post-drain state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handle_clones_share_queue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let events_db = Arc::new(EventsDatabase::open(dir.path()).expect("open"));
        let config = Arc::new(test_config());

        let batch = EventWriterBatchConfig::builder()
            .flush_interval_ms(60_000)
            .flush_size_threshold(10_000)
            .queue_capacity(100)
            .drain_batch_max(50)
            .build()
            .unwrap();
        let runtime = runtime_handle_with_batch(batch.clone());

        let (handle_a, join) = EventHandle::with_batching(
            Arc::clone(&events_db),
            Arc::clone(&config),
            1,
            batch,
            runtime,
            "test-region",
        );
        let handle_b = handle_a.clone();

        handle_a.record_handler_event(sample_handler_entry(1));
        handle_b.record_handler_event(sample_handler_entry(2));
        assert_eq!(handle_a.queue_depth(), 2);
        assert_eq!(handle_b.queue_depth(), 2);

        let _ = handle_a.flush_for_shutdown(Duration::from_secs(5)).await;
        join.await.expect("flusher task exit");

        let txn = events_db.read().expect("read");
        let count = EventStore::count(&txn, OrganizationId::new(1)).expect("count");
        assert_eq!(count, 2);
    }

    /// `flush_for_shutdown` on a handle with no queue returns a zeroed
    /// result immediately — used by bootstrap paths that conditionally
    /// wire a flusher.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_for_shutdown_no_queue_is_noop() {
        let db = EventsDatabase::open_in_memory().expect("open");
        let db_arc = Arc::new(db);
        let config = Arc::new(test_config());
        let handle = EventHandle::new(Arc::clone(&db_arc), config, 1);

        let result = handle.flush_for_shutdown(Duration::from_secs(1)).await;
        assert_eq!(result.drained, 0);
        assert_eq!(result.lost, 0);
    }

    // =========================================================================
    // Proptests for queue ordering + flush correctness
    // =========================================================================
    //
    // Two invariants drive these proptests:
    //
    // 1. **FIFO ordering per-producer is preserved** — a single producer serially emitting N
    //    entries sees them land in `events.db` in the same order after a forced flush.
    // 2. **Enabled=false == Enabled=true + forced flush** — the strict- durable escape hatch
    //    produces byte-identical `events.db` state to the batched path once a shutdown-drain
    //    completes. Proves the escape hatch is a true drop-in equivalent for compliance
    //    deployments.
    //
    // Plus one cheap extra invariant:
    //
    // 3. **N concurrent producers each emitting M entries all commit** — count + set equality of
    //    event_ids after a forced flush. No ordering claim across producers — the mpsc channel only
    //    guarantees per-sender order.
    //
    // All three tests feed entries that are fully deterministic
    // (construction uses `Utc.timestamp_opt(...)` with monotonic
    // nanoseconds, NOT `HandlerPhaseEmitter` which derives `Utc::now()` +
    // random UUID v4 bytes). That lets us assert strict B-tree read
    // order — the storage primary key is `(org_id, timestamp_ns,
    // event_id)`, so monotonic timestamps force the B-tree iteration
    // order to match emission order.
    //
    // Case counts: 32 for the single-producer tests, 16 for the
    // concurrent one. Each case opens a fresh tempdir-backed
    // `FileBackend` events database, spawns the flusher, enqueues 1..N
    // entries, and runs `flush_for_shutdown` — ~20-40ms per case.
    // PROPTEST_CASES=256 would blow the lib-test wall clock; these are
    // still enough to shrink any per-producer FIFO or enabled-false
    // equivalence counterexample into a minimal repro.
    mod proptest_flush_correctness {
        use inferadb_ledger_state::Events;
        use inferadb_ledger_store::FileBackend;
        use proptest::prelude::*;

        use super::*;

        /// Constructs a deterministic `EventEntry` at emission sequence
        /// `idx`. Timestamps are strictly monotonic in `idx` (nanosecond
        /// granularity), event_ids encode `idx` as big-endian bytes, and
        /// no random / wall-clock fields are used — so an
        /// `enabled=false` vs `enabled=true` run that feeds the same
        /// sequence produces byte-identical `events.db` state.
        ///
        /// # Determinism
        /// - `timestamp` = `1_700_000_000s + idx ns` (strictly monotonic).
        /// - `event_id` = `idx.to_be_bytes() ++ [0u8; 12]` (unique per emission index).
        /// - `expires_at` = 0 (no GC interference).
        fn deterministic_entry(idx: u32) -> EventEntry {
            use chrono::TimeZone;

            let ts = Utc.timestamp_opt(1_700_000_000, idx).single().expect("valid timestamp");

            let mut event_id = [0u8; 16];
            event_id[..4].copy_from_slice(&idx.to_be_bytes());

            EventEntry {
                emission: EventEmission::HandlerPhase { node_id: 1 },
                expires_at: 0,
                event_id,
                source_service: "ledger".to_string(),
                event_type: EventAction::RequestRateLimited.event_type().to_string(),
                timestamp: ts,
                scope: EventScope::Organization,
                action: EventAction::RequestRateLimited,
                principal: "user:alice".to_string(),
                organization_id: OrganizationId::new(1),
                organization: None,
                vault: None,
                outcome: EventOutcome::Denied { reason: "rate_limited".to_string() },
                details: BTreeMap::new(),
                block_height: None,
                trace_id: None,
                correlation_id: None,
                operations_count: None,
            }
        }

        /// Lists every event for org 1 in B-tree key order. With
        /// monotonic `deterministic_entry` timestamps this is also the
        /// emission order.
        fn list_all_for_org1<B: StorageBackend>(events_db: &EventsDatabase<B>) -> Vec<EventEntry> {
            let txn = events_db.read().expect("read");
            let (entries, cursor) =
                EventStore::list(&txn, OrganizationId::new(1), 0, u64::MAX, 10_000, None)
                    .expect("list");
            assert!(cursor.is_none(), "test sequences are bounded below pagination limit");
            entries
        }

        /// Raw (key, value) snapshot of the `Events` primary table in
        /// B-tree iteration order. Used for byte-identical equality
        /// between the enabled=false and enabled=true+flush paths.
        fn snapshot_events_table<B: StorageBackend>(
            events_db: &EventsDatabase<B>,
        ) -> Vec<(Vec<u8>, Vec<u8>)> {
            let txn = events_db.read().expect("read");
            txn.iter::<Events>().expect("iter events").collect()
        }

        /// Build a small-capacity batched handle backed by a tempdir
        /// events.db. Caller owns the returned `TempDir` so the db
        /// survives until the assertions run.
        fn make_batched_handle(
            flush_interval_ms: u64,
            queue_capacity: usize,
        ) -> (
            tempfile::TempDir,
            Arc<EventsDatabase<FileBackend>>,
            EventHandle<FileBackend>,
            tokio::task::JoinHandle<()>,
        ) {
            let dir = tempfile::tempdir().expect("tempdir");
            let events_db =
                Arc::new(EventsDatabase::<FileBackend>::open(dir.path()).expect("open"));
            let config = Arc::new(test_config());

            let batch = EventWriterBatchConfig::builder()
                .flush_interval_ms(flush_interval_ms)
                .flush_size_threshold(10_000)
                .queue_capacity(queue_capacity)
                .drain_batch_max(1_000)
                .build()
                .unwrap();
            let runtime = runtime_handle_with_batch(batch.clone());

            let (handle, join) = EventHandle::with_batching(
                Arc::clone(&events_db),
                config,
                1,
                batch,
                runtime,
                "test-region",
            );
            (dir, events_db, handle, join)
        }

        proptest! {
            // 32 cases: each case opens a tempdir-backed FileBackend
            // events.db, spawns a flusher, enqueues up to 40 entries,
            // and runs `flush_for_shutdown`. Heavier than the
            // block-archive proptest; 32 keeps the total runtime
            // bounded while still shrinking any FIFO-violation
            // counterexample down to a minimal repro.
            #![proptest_config(ProptestConfig::with_cases(32))]

            /// Property 1: FIFO ordering per-producer is preserved.
            ///
            /// A single producer serially emitting N entries via
            /// `record_handler_event`, followed by a `flush_for_shutdown`,
            /// results in `events.db` holding the entries in the same
            /// order. Uses `deterministic_entry(idx)` which produces
            /// strictly-monotonic timestamps, so the B-tree primary key
            /// `(org_id, timestamp_ns, event_id)` orders rows by
            /// emission index.
            #[test]
            fn flusher_preserves_fifo_order_per_producer(n in 1u32..=40u32) {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("tokio runtime");

                runtime.block_on(async move {
                    // Use a generous flush_interval (won't fire mid-run);
                    // `flush_for_shutdown` is the forced-drain primitive.
                    let (_dir, events_db, handle, join) =
                        make_batched_handle(60_000, 10_000);

                    for idx in 0..n {
                        handle.record_handler_event(deterministic_entry(idx));
                    }

                    let result = handle.flush_for_shutdown(Duration::from_secs(5)).await;
                    prop_assert_eq!(result.drained, u64::from(n));
                    prop_assert_eq!(result.lost, 0);
                    join.await.expect("flusher task exit");

                    // B-tree order == emission order under monotonic timestamps.
                    let got = list_all_for_org1(&events_db);
                    prop_assert_eq!(got.len(), n as usize);
                    for (i, entry) in got.iter().enumerate() {
                        let expected_id = (i as u32).to_be_bytes();
                        prop_assert_eq!(
                            &entry.event_id[..4],
                            &expected_id[..],
                            "event at position {} has wrong event_id prefix — \
                             FIFO ordering violated",
                            i
                        );
                    }

                    Ok(())
                })?;
            }

            /// Property 2: `enabled = false` escape hatch produces
            /// byte-identical `events.db` state vs the batched path.
            ///
            /// The design doc
            /// (`docs/superpowers/specs/2026-04-19-sprint-1b4-handler-event-batching-design.md`
            /// § "enabled = false is the strict-durability escape
            /// hatch") specifies that operators can disable batching at
            /// runtime via the `UpdateConfig` RPC; in that mode,
            /// `record_handler_event` must bypass the queue and route
            /// through the synchronous per-emission fsync path. This
            /// property tests the exact production code path, including
            /// the runtime-config read inside `record_handler_event`.
            ///
            /// Two handles, two fresh tempdir-backed events databases,
            /// identical input sequence (fully deterministic, so no
            /// wall-clock or random UUIDs to perturb the bytes).
            ///
            /// - Handle A: constructed via [`EventHandle::with_batching`]
            ///   with `event_writer_batch.enabled = false` in the
            ///   runtime config. `record_handler_event` consults the
            ///   flag and falls through to `write_sync` (one fsync per
            ///   event; strict-synchronous semantics).
            /// - Handle B: constructed via [`EventHandle::with_batching`]
            ///   with `enabled = true`; every emission enqueues into
            ///   the flusher, and `flush_for_shutdown` drains the
            ///   queue to events.db in one batched commit.
            ///
            /// After both paths complete, the `Events` table must be
            /// byte-identical at the B-tree (key, value) level. Proves
            /// the escape hatch is a true drop-in equivalent for
            /// compliance deployments.
            #[test]
            fn disabled_flag_matches_batched_path_plus_flush(n in 1u32..=40u32) {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("tokio runtime");

                runtime.block_on(async move {
                    // Handle A: batched constructor, but the runtime
                    // config has `enabled = false` — exercises the
                    // strict-durability escape hatch on the production
                    // code path.
                    let dir_a = tempfile::tempdir().expect("tempdir A");
                    let events_db_a =
                        Arc::new(EventsDatabase::<FileBackend>::open(dir_a.path())
                            .expect("open A"));
                    let batch_off = EventWriterBatchConfig::builder()
                        .enabled(false)
                        .flush_interval_ms(60_000)
                        .flush_size_threshold(10_000)
                        .queue_capacity(10_000)
                        .drain_batch_max(1_000)
                        .build()
                        .unwrap();
                    let runtime_a = runtime_handle_with_batch(batch_off.clone());
                    let (handle_a, join_a) = EventHandle::with_batching(
                        Arc::clone(&events_db_a),
                        Arc::new(test_config()),
                        1,
                        batch_off,
                        runtime_a,
                        "test-region-a",
                    );

                    // Handle B: batched path with batching enabled.
                    let (_dir_b, events_db_b, handle_b, join_b) =
                        make_batched_handle(60_000, 10_000);

                    for idx in 0..n {
                        let entry = deterministic_entry(idx);
                        handle_a.record_handler_event(entry.clone());
                        handle_b.record_handler_event(entry);
                    }

                    // Handle A wrote synchronously on every emission —
                    // the queue should be empty, but we still drain to
                    // stop the spawned flusher task cleanly.
                    let result_a = handle_a.flush_for_shutdown(Duration::from_secs(5)).await;
                    prop_assert_eq!(
                        result_a.drained, 0,
                        "escape-hatch handle must not have enqueued any entries"
                    );
                    prop_assert_eq!(result_a.lost, 0);
                    join_a.await.expect("flusher A exit");

                    let result_b = handle_b.flush_for_shutdown(Duration::from_secs(5)).await;
                    prop_assert_eq!(result_b.drained, u64::from(n));
                    prop_assert_eq!(result_b.lost, 0);
                    join_b.await.expect("flusher B exit");

                    let snap_a = snapshot_events_table(&events_db_a);
                    let snap_b = snapshot_events_table(&events_db_b);

                    prop_assert_eq!(
                        snap_a.len(),
                        snap_b.len(),
                        "row counts diverged between enabled=false and batched+flush"
                    );
                    prop_assert_eq!(
                        snap_a,
                        snap_b,
                        "Events table bytes diverged between enabled=false and \
                         batched+flush — escape hatch is NOT a drop-in equivalent"
                    );

                    // Keep dir_a alive through the assertions.
                    drop(dir_a);

                    Ok(())
                })?;
            }
        }

        proptest! {
            // 16 cases for the concurrent producers test: ~N*M tasks
            // spawned per case plus flush latency. Heavier than the
            // single-producer tests; 16 keeps the runtime bounded.
            #![proptest_config(ProptestConfig::with_cases(16))]

            /// Property 3: N concurrent producers each emitting M
            /// entries all land in `events.db` after a forced flush.
            ///
            /// Asserts count == N*M, all event_ids are present
            /// (set-equality), and no duplicates. No claim about order
            /// — the mpsc channel only guarantees per-sender FIFO, and
            /// producers race each other. The per-producer FIFO
            /// property is covered by
            /// [`flusher_preserves_fifo_order_per_producer`].
            ///
            /// Each producer's entries use `event_id` slots reserved
            /// via `(producer_idx, entry_idx)` encoding — unique across
            /// all producers.
            #[test]
            fn flusher_under_concurrent_emitters_preserves_all(
                producers in 2u32..=8u32,
                per_producer in 1u32..=10u32,
            ) {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .enable_all()
                    .build()
                    .expect("tokio runtime");

                runtime.block_on(async move {
                    let (_dir, events_db, handle, join) =
                        make_batched_handle(60_000, 10_000);

                    let mut tasks = Vec::with_capacity(producers as usize);
                    for p in 0..producers {
                        let h = handle.clone();
                        let m = per_producer;
                        tasks.push(tokio::spawn(async move {
                            for i in 0..m {
                                // Unique event_id per (producer, sequence):
                                // first 4 bytes = producer, next 4 = seq.
                                let mut entry = deterministic_entry(i);
                                entry.event_id[..4].copy_from_slice(&p.to_be_bytes());
                                entry.event_id[4..8].copy_from_slice(&i.to_be_bytes());
                                h.record_handler_event(entry);
                                // Yield between emissions so the multi-
                                // thread runtime interleaves producers.
                                tokio::task::yield_now().await;
                            }
                        }));
                    }
                    for t in tasks {
                        t.await.expect("producer task exit");
                    }

                    let expected_total = u64::from(producers) * u64::from(per_producer);
                    let result = handle.flush_for_shutdown(Duration::from_secs(5)).await;
                    prop_assert_eq!(result.drained, expected_total);
                    prop_assert_eq!(result.lost, 0);
                    join.await.expect("flusher task exit");

                    let got = list_all_for_org1(&events_db);
                    prop_assert_eq!(got.len() as u64, expected_total);

                    // Set equality: every (producer, seq) pair appears
                    // exactly once.
                    use std::collections::HashSet;
                    let ids: HashSet<[u8; 16]> =
                        got.iter().map(|e| e.event_id).collect();
                    prop_assert_eq!(
                        ids.len() as u64,
                        expected_total,
                        "duplicate event_ids — flusher committed the same entry twice"
                    );
                    for p in 0..producers {
                        for i in 0..per_producer {
                            let mut expected = [0u8; 16];
                            expected[..4].copy_from_slice(&p.to_be_bytes());
                            expected[4..8].copy_from_slice(&i.to_be_bytes());
                            prop_assert!(
                                ids.contains(&expected),
                                "missing event_id for producer={} seq={}",
                                p,
                                i
                            );
                        }
                    }

                    Ok(())
                })?;
            }
        }
    }
}
