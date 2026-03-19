//! Audit event types for querying and ingesting events.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

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
/// # use inferadb_ledger_sdk::{LedgerClient, EventSource, OrganizationSlug};
/// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
/// # let organization = OrganizationSlug::new(1);
/// let result = client.ingest_events(organization, EventSource::Engine, vec![]).await?;
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
    pub(crate) fn from_proto(value: i32) -> Self {
        match proto::EventScope::try_from(value) {
            Ok(proto::EventScope::System) => Self::System,
            Ok(proto::EventScope::Organization) => Self::Organization,
            _ => Self::System,
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
    pub(crate) fn from_proto(
        value: i32,
        error_code: Option<String>,
        error_detail: Option<String>,
        denial_reason: Option<String>,
    ) -> Self {
        match proto::EventOutcome::try_from(value) {
            Ok(proto::EventOutcome::Success) => Self::Success,
            Ok(proto::EventOutcome::Failed) => Self::Failed {
                code: error_code.unwrap_or_default(),
                detail: error_detail.unwrap_or_default(),
            },
            Ok(proto::EventOutcome::Denied) => {
                Self::Denied { reason: denial_reason.unwrap_or_default() }
            },
            _ => Self::Success,
        }
    }

    pub(crate) fn to_proto(&self) -> (i32, Option<String>, Option<String>, Option<String>) {
        match self {
            Self::Success => (proto::EventOutcome::Success as i32, None, None, None),
            Self::Failed { code, detail } => {
                (proto::EventOutcome::Failed as i32, Some(code.clone()), Some(detail.clone()), None)
            },
            Self::Denied { reason } => {
                (proto::EventOutcome::Denied as i32, None, None, Some(reason.clone()))
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
    /// Node-local, best-effort — exists only on the handling node.
    HandlerPhase,
}

impl EventEmissionPath {
    pub(crate) fn from_proto(value: i32) -> Self {
        match proto::EventEmissionPath::try_from(value) {
            Ok(proto::EventEmissionPath::EmissionPathApplyPhase) => Self::ApplyPhase,
            Ok(proto::EventEmissionPath::EmissionPathHandlerPhase) => Self::HandlerPhase,
            _ => Self::ApplyPhase,
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
    /// Creates from protobuf response.
    pub(crate) fn from_proto(proto: proto::EventEntry) -> Self {
        let timestamp = proto
            .timestamp
            .map(|ts| {
                chrono::DateTime::from_timestamp(ts.seconds, u32::try_from(ts.nanos).unwrap_or(0))
                    .unwrap_or(chrono::DateTime::UNIX_EPOCH)
            })
            .unwrap_or(chrono::DateTime::UNIX_EPOCH);

        Self {
            event_id: proto.event_id,
            source_service: proto.source_service,
            event_type: proto.event_type,
            timestamp,
            scope: EventScope::from_proto(proto.scope),
            action: proto.action,
            emission_path: EventEmissionPath::from_proto(proto.emission_path),
            principal: proto.principal,
            organization: OrganizationSlug::new(proto.organization.map_or(0, |o| o.slug)),
            vault: proto.vault.map(|v| VaultSlug::new(v.slug)),
            outcome: EventOutcome::from_proto(
                proto.outcome,
                proto.error_code,
                proto.error_detail,
                proto.denial_reason,
            ),
            details: proto.details,
            block_height: proto.block_height,
            node_id: proto.node_id,
            trace_id: proto.trace_id,
            correlation_id: proto.correlation_id,
            operations_count: proto.operations_count,
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
    outcome: Option<proto::EventOutcome>,
    emission_path: Option<proto::EventEmissionPath>,
    correlation_id: Option<String>,
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
        self.outcome = Some(proto::EventOutcome::Success);
        self
    }

    /// Filters to failed events only.
    pub fn outcome_failed(mut self) -> Self {
        self.outcome = Some(proto::EventOutcome::Failed);
        self
    }

    /// Filters to denied events only.
    pub fn outcome_denied(mut self) -> Self {
        self.outcome = Some(proto::EventOutcome::Denied);
        self
    }

    /// Filters to apply-phase events only (deterministic, replicated).
    pub fn apply_phase_only(mut self) -> Self {
        self.emission_path = Some(proto::EventEmissionPath::EmissionPathApplyPhase);
        self
    }

    /// Filters to handler-phase events only (node-local).
    pub fn handler_phase_only(mut self) -> Self {
        self.emission_path = Some(proto::EventEmissionPath::EmissionPathHandlerPhase);
        self
    }

    /// Filters by business-level correlation ID.
    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    pub(crate) fn to_proto(&self) -> proto::EventFilter {
        proto::EventFilter {
            start_time: self.start_time.map(|dt| prost_types::Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }),
            end_time: self.end_time.map(|dt| prost_types::Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }),
            actions: self.actions.clone(),
            event_type_prefix: self.event_type_prefix.clone(),
            principal: self.principal.clone(),
            outcome: self.outcome.map_or(0, |o| o as i32),
            emission_path: self.emission_path.map_or(0, |e| e as i32),
            correlation_id: self.correlation_id.clone(),
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

    pub(crate) fn into_proto(self) -> proto::IngestEventEntry {
        let (outcome, error_code, error_detail, denial_reason) = self.outcome.to_proto();
        proto::IngestEventEntry {
            event_type: self.event_type,
            principal: self.principal,
            outcome,
            details: self.details,
            trace_id: self.trace_id,
            correlation_id: self.correlation_id,
            vault: self.vault.map(|v| proto::VaultSlug { slug: v.value() }),
            timestamp: self.timestamp.map(|dt| prost_types::Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }),
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
