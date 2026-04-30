//! Audit event types for querying and ingesting events.

use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use inferadb_ledger_wire::services::events as wire_events;

/// Identifies which InferaDB component is the source of ingested events.
///
/// Used with [`LedgerClient::ingest_events`](crate::LedgerClient::ingest_events) to specify the
/// originating component. Only external components (Engine and Control) may ingest
/// events — the Ledger itself generates its own events internally during
/// apply-phase processing.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, EventSource, OrganizationSlug, UserSlug};
/// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
/// # let organization = OrganizationSlug::new(1);
/// let result = client.ingest_events(UserSlug::new(42), organization, EventSource::Engine, vec![]).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum EventSource {
    /// The authorization engine component.
    Engine,
    /// The control plane component.
    Control,
}

impl EventSource {
    /// Returns the wire-format string sent over gRPC.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Engine => "engine",
            Self::Control => "control",
        }
    }
}

impl std::fmt::Display for EventSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Scope of an event (system-wide or organization-scoped).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum EventScope {
    /// Cluster-wide administrative event.
    System,
    /// Per-organization tenant event.
    Organization,
}

impl EventScope {
    /// Converts from the wire-protocol enum.
    ///
    /// Unknown / `Unspecified` collapses to [`Self::System`].
    pub(crate) fn from_wire(value: wire_events::EventScope) -> Self {
        match value {
            wire_events::EventScope::System => Self::System,
            wire_events::EventScope::Organization => Self::Organization,
            wire_events::EventScope::Unspecified => Self::System,
        }
    }
}

/// Outcome of an audited operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum EventOutcome {
    /// Operation completed successfully.
    Success,
    /// Operation failed with an error.
    Failed {
        /// Error code.
        code: String,
        /// Error details.
        detail: String,
    },
    /// Operation was denied (rate limited, unauthorized, etc.).
    Denied {
        /// Denial reason.
        reason: String,
    },
}

impl EventOutcome {
    /// Converts from the wire-protocol enum + the per-outcome auxiliary
    /// fields the wire `EventEntry` carries alongside it.
    ///
    /// `Unspecified` collapses to [`Self::Success`] as the safe default.
    pub(crate) fn from_wire(
        value: wire_events::EventOutcome,
        error_code: Option<String>,
        error_detail: Option<String>,
        denial_reason: Option<String>,
    ) -> Self {
        match value {
            wire_events::EventOutcome::Success => Self::Success,
            wire_events::EventOutcome::Failed => Self::Failed {
                code: error_code.unwrap_or_default(),
                detail: error_detail.unwrap_or_default(),
            },
            wire_events::EventOutcome::Denied => {
                Self::Denied { reason: denial_reason.unwrap_or_default() }
            },
            wire_events::EventOutcome::Unspecified => Self::Success,
        }
    }

    /// Converts to the wire-protocol enum and its per-outcome auxiliary
    /// fields. Mirrors [`Self::to_proto`] for the wire transport.
    pub(crate) fn to_wire(
        &self,
    ) -> (wire_events::EventOutcome, Option<String>, Option<String>, Option<String>) {
        match self {
            Self::Success => (wire_events::EventOutcome::Success, None, None, None),
            Self::Failed { code, detail } => {
                (wire_events::EventOutcome::Failed, Some(code.clone()), Some(detail.clone()), None)
            },
            Self::Denied { reason } => {
                (wire_events::EventOutcome::Denied, None, None, Some(reason.clone()))
            },
        }
    }
}

/// Emission path of an event (how it was generated).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum EventEmissionPath {
    /// Deterministic, Raft-replicated — identical on all nodes.
    ApplyPhase,
    /// Emitted outside the apply pipeline: either ingested externally
    /// through [`LedgerClient::ingest_events`](crate::LedgerClient::ingest_events)
    /// (Raft-replicated) or recorded locally during RPC handling.
    /// Locally-recorded handler-phase events are batched through an
    /// in-memory flush queue and fsynced within a ~100 ms default window,
    /// so a query issued immediately after the emitting RPC may not yet
    /// observe them.
    HandlerPhase,
}

impl EventEmissionPath {
    /// Converts from the wire-protocol enum.
    ///
    /// `Unspecified` collapses to [`Self::ApplyPhase`] as the safe default.
    pub(crate) fn from_wire(value: wire_events::EventEmissionPath) -> Self {
        match value {
            wire_events::EventEmissionPath::ApplyPhase => Self::ApplyPhase,
            wire_events::EventEmissionPath::HandlerPhase => Self::HandlerPhase,
            wire_events::EventEmissionPath::Unspecified => Self::ApplyPhase,
        }
    }
}

/// An audit event entry from the events system.
///
/// Represents a single auditable action — a write, admin operation, denial,
/// or system event. Events follow the canonical log line pattern with rich
/// contextual fields for compliance and debugging.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SdkEventEntry {
    /// Unique event identifier (UUID, 16 bytes).
    pub event_id: Vec<u8>,
    /// Originating service (`"ledger"`, `"engine"`, or `"control"`).
    pub source_service: String,
    /// Hierarchical dot-separated type (e.g., `"ledger.vault.created"`).
    pub event_type: String,
    /// When the event occurred.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Event scope (system or organization).
    pub scope: EventScope,
    /// Action name (snake_case, e.g., `"vault_created"`).
    pub action: String,
    /// Emission path (apply-phase or handler-phase).
    pub emission_path: EventEmissionPath,
    /// Who performed the action.
    pub principal: String,
    /// Owning organization (0 for system events).
    pub organization: OrganizationSlug,
    /// Vault context (when applicable).
    pub vault: Option<VaultSlug>,
    /// Outcome of the operation.
    pub outcome: EventOutcome,
    /// Action-specific key-value context.
    pub details: std::collections::HashMap<String, String>,
    /// Reference to blockchain block (for committed writes).
    pub block_height: Option<u64>,
    /// Node that generated the event (for handler-phase events).
    pub node_id: Option<u64>,
    /// Distributed tracing correlation (W3C Trace Context).
    pub trace_id: Option<String>,
    /// Business-level correlation for multi-step operations.
    pub correlation_id: Option<String>,
    /// Number of operations (for write actions).
    pub operations_count: Option<u32>,
}

impl SdkEventEntry {
    /// Creates from a wire-protocol response.
    ///
    /// Mirrors [`Self::from_proto`] for the wire transport. The wire
    /// `EventEntry` carries `timestamp` as UNIX nanoseconds (a single
    /// `u64`, not a split seconds/nanos `Timestamp`); we project it back
    /// into a `chrono::DateTime<Utc>` via `from_timestamp_nanos`. Wire
    /// `details` is a `BTreeMap` and widens to the domain type's
    /// `HashMap`. The wire-only `expires_at` field is intentionally
    /// dropped — the SDK domain type does not surface it.
    pub(crate) fn from_wire(entry: wire_events::EventEntry) -> Self {
        // Wire `timestamp` is u64 UNIX nanoseconds; chrono's nanos
        // constructor takes i64 (capped at ~year 2262), so any value
        // outside i64 range collapses to UNIX_EPOCH rather than panicking.
        let timestamp = i64::try_from(entry.timestamp)
            .map(chrono::DateTime::from_timestamp_nanos)
            .unwrap_or(chrono::DateTime::UNIX_EPOCH);

        Self {
            event_id: entry.event_id.to_vec(),
            source_service: entry.source_service,
            event_type: entry.event_type,
            timestamp,
            scope: EventScope::from_wire(entry.scope),
            action: entry.action,
            emission_path: EventEmissionPath::from_wire(entry.emission_path),
            principal: entry.principal,
            organization: entry.organization.unwrap_or_else(|| OrganizationSlug::new(0)),
            vault: entry.vault,
            outcome: EventOutcome::from_wire(
                entry.outcome,
                entry.error_code,
                entry.error_detail,
                entry.denial_reason,
            ),
            details: entry.details.into_iter().collect(),
            block_height: entry.block_height,
            node_id: entry.node_id,
            trace_id: entry.trace_id,
            correlation_id: entry.correlation_id,
            operations_count: entry.operations_count,
        }
    }

    /// Returns the event ID formatted as a UUID string.
    ///
    /// Falls back to hex encoding if the ID is not exactly 16 bytes.
    pub fn event_id_string(&self) -> String {
        if let Ok(bytes) = <[u8; 16]>::try_from(self.event_id.as_slice()) {
            uuid::Uuid::from_bytes(bytes).to_string()
        } else {
            self.event_id.iter().fold(String::new(), |mut s, b| {
                use std::fmt::Write;
                let _ = write!(s, "{b:02x}");
                s
            })
        }
    }
}

/// Paginated result from event queries.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EventPage {
    /// Matching events in chronological order.
    pub entries: Vec<SdkEventEntry>,
    /// Opaque cursor for next page; `None` if no more results.
    pub next_page_token: Option<String>,
    /// Estimated total count (may be approximate for large datasets).
    pub total_estimate: Option<u64>,
}

impl EventPage {
    /// Returns `true` if there are more pages available.
    pub fn has_next_page(&self) -> bool {
        self.next_page_token.is_some()
    }
}

/// Filter criteria for event queries.
///
/// Use the builder methods to construct a filter. An empty filter matches
/// all events in the organization.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::EventFilter;
/// let filter = EventFilter::new()
///     .event_type_prefix("ledger.vault")
///     .outcome_success();
/// ```
#[derive(Debug, Clone, Default)]
pub struct EventFilter {
    start_time: Option<chrono::DateTime<chrono::Utc>>,
    end_time: Option<chrono::DateTime<chrono::Utc>>,
    actions: Vec<String>,
    event_type_prefix: Option<String>,
    principal: Option<String>,
    outcome: Option<wire_events::EventOutcome>,
    emission_path: Option<wire_events::EventEmissionPath>,
    correlation_id: Option<String>,
    vault: Option<VaultSlug>,
}

impl EventFilter {
    /// Creates an empty filter that matches all events.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filters events from this time forward (inclusive).
    pub fn start_time(mut self, time: chrono::DateTime<chrono::Utc>) -> Self {
        self.start_time = Some(time);
        self
    }

    /// Filters events before this time (exclusive).
    pub fn end_time(mut self, time: chrono::DateTime<chrono::Utc>) -> Self {
        self.end_time = Some(time);
        self
    }

    /// Filters by action names (snake_case). Multiple actions are OR'd.
    pub fn actions(mut self, actions: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.actions = actions.into_iter().map(Into::into).collect();
        self
    }

    /// Filters by event type prefix (e.g., `"ledger.vault"` matches `"ledger.vault.created"`).
    pub fn event_type_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.event_type_prefix = Some(prefix.into());
        self
    }

    /// Filters by principal (who performed the action).
    pub fn principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Filters to successful events only.
    pub fn outcome_success(mut self) -> Self {
        self.outcome = Some(wire_events::EventOutcome::Success);
        self
    }

    /// Filters to failed events only.
    pub fn outcome_failed(mut self) -> Self {
        self.outcome = Some(wire_events::EventOutcome::Failed);
        self
    }

    /// Filters to denied events only.
    pub fn outcome_denied(mut self) -> Self {
        self.outcome = Some(wire_events::EventOutcome::Denied);
        self
    }

    /// Filters to apply-phase events only (deterministic, replicated).
    pub fn apply_phase_only(mut self) -> Self {
        self.emission_path = Some(wire_events::EventEmissionPath::ApplyPhase);
        self
    }

    /// Filters to handler-phase events only (emitted outside the apply pipeline).
    ///
    /// Handler-phase events cover both externally-ingested events (via
    /// [`LedgerClient::ingest_events`](crate::LedgerClient::ingest_events),
    /// which are Raft-replicated) and events recorded locally inside RPC
    /// handlers. Locally-recorded handler-phase events carry a ~100 ms
    /// default durability window: the server batches them
    /// through an in-memory flush queue and fsyncs them on a background
    /// tick, so a query issued immediately after the emitting RPC may not
    /// yet observe them. See
    /// [`LedgerClient::list_events`](crate::LedgerClient::list_events)
    /// for the read-after-write caveat.
    pub fn handler_phase_only(mut self) -> Self {
        self.emission_path = Some(wire_events::EventEmissionPath::HandlerPhase);
        self
    }

    /// Filters by business-level correlation ID.
    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Filters to events emitted within a specific vault.
    ///
    /// When set, the query routes to that vault's per-vault events store
    /// and skips org-level events. Without a vault filter, the query fans
    /// out across the org-level store and every per-vault store under the
    /// organization, merging by timestamp.
    pub fn vault(mut self, vault: VaultSlug) -> Self {
        self.vault = Some(vault);
        self
    }

    /// Projects the filter into the wire-protocol shape.
    ///
    /// Wire timestamps are single-`u64` UNIX nanoseconds (not split
    /// seconds/nanos `Timestamp`s), so we collapse via
    /// [`chrono::DateTime::timestamp_nanos_opt`]; `None` from chrono (only
    /// for far-future / pre-epoch dates outside i64 nanos range) drops the
    /// bound silently.
    ///
    /// `outcome` / `emission_path` use the wire enums directly; an absent
    /// builder selection ("match any") collapses to the `Unspecified`
    /// variant on the wire side.
    pub(crate) fn to_wire(&self) -> wire_events::EventFilter {
        let start_time = self
            .start_time
            .and_then(|dt| dt.timestamp_nanos_opt())
            .and_then(|n| u64::try_from(n).ok());
        let end_time = self
            .end_time
            .and_then(|dt| dt.timestamp_nanos_opt())
            .and_then(|n| u64::try_from(n).ok());
        wire_events::EventFilter {
            start_time,
            end_time,
            actions: self.actions.clone(),
            event_type_prefix: self.event_type_prefix.clone(),
            principal: self.principal.clone(),
            outcome: self.outcome.unwrap_or(wire_events::EventOutcome::Unspecified),
            emission_path: self
                .emission_path
                .unwrap_or(wire_events::EventEmissionPath::Unspecified),
            correlation_id: self.correlation_id.clone(),
            vault: self.vault,
        }
    }
}

/// A single event for external ingestion (from Engine or Control).
///
/// Use the builder methods to construct an event entry for ingestion.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::SdkIngestEventEntry;
/// let event = SdkIngestEventEntry::new(
///     "engine.authorization.checked",
///     "user:alice",
///     inferadb_ledger_sdk::EventOutcome::Success,
/// )
/// .correlation_id("batch-job-42");
/// ```
#[derive(Debug, Clone)]
pub struct SdkIngestEventEntry {
    event_type: String,
    principal: String,
    outcome: EventOutcome,
    details: std::collections::HashMap<String, String>,
    trace_id: Option<String>,
    correlation_id: Option<String>,
    vault: Option<VaultSlug>,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

impl SdkIngestEventEntry {
    /// Creates a new event entry with required fields.
    pub fn new(
        event_type: impl Into<String>,
        principal: impl Into<String>,
        outcome: EventOutcome,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            principal: principal.into(),
            outcome,
            details: std::collections::HashMap::new(),
            trace_id: None,
            correlation_id: None,
            vault: None,
            timestamp: None,
        }
    }

    /// Adds action-specific key-value context.
    pub fn details(mut self, details: std::collections::HashMap<String, String>) -> Self {
        self.details = details;
        self
    }

    /// Adds a single detail key-value pair.
    pub fn detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    /// Sets the distributed tracing correlation ID.
    pub fn trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Sets the business-level correlation ID.
    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Sets the vault context.
    pub fn vault(mut self, vault: VaultSlug) -> Self {
        self.vault = Some(vault);
        self
    }

    /// Sets a custom timestamp (defaults to server receive time if omitted).
    pub fn timestamp(mut self, timestamp: chrono::DateTime<chrono::Utc>) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Projects the entry into the wire-protocol shape.
    ///
    /// Wire details are stored as `BTreeMap` (sorted, postcard-friendly);
    /// we narrow from the domain `HashMap` via `collect`. The wire
    /// `timestamp` is `Option<u64>` UNIX nanoseconds — same caveat as
    /// [`EventFilter::to_wire`] for far-future dates outside i64 nanos
    /// range.
    pub(crate) fn into_wire(self) -> wire_events::IngestEventEntry {
        let (outcome, error_code, error_detail, denial_reason) = self.outcome.to_wire();
        let timestamp = self
            .timestamp
            .and_then(|dt| dt.timestamp_nanos_opt())
            .and_then(|n| u64::try_from(n).ok());
        wire_events::IngestEventEntry {
            event_type: self.event_type,
            principal: self.principal,
            outcome,
            details: self.details.into_iter().collect(),
            trace_id: self.trace_id,
            correlation_id: self.correlation_id,
            vault: self.vault,
            timestamp,
            error_code,
            error_detail,
            denial_reason,
        }
    }
}

/// Result of an event ingestion request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IngestResult {
    /// Number of events accepted and written.
    pub accepted_count: u32,
    /// Number of events rejected.
    pub rejected_count: u32,
    /// Per-event rejection details.
    pub rejections: Vec<IngestRejection>,
}

/// A single rejected event from an ingestion batch.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IngestRejection {
    /// Zero-based index into the request's entries array.
    pub index: u32,
    /// Human-readable rejection reason.
    pub reason: String,
}
