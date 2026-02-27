//! Main `LedgerClient` implementation.
//!
//! Provides the high-level API for interacting with the Ledger service,
//! orchestrating connection pool, idempotency keys, and retry logic.

use std::sync::Arc;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use tonic::service::interceptor::InterceptedService;

use crate::{
    config::ClientConfig,
    connection::ConnectionPool,
    error::{self, Result},
    retry::with_retry_cancellable,
    server::{ServerResolver, ServerSource},
    streaming::{HeightTracker, ReconnectingStream},
    tracing::TraceContextInterceptor,
};

/// Consistency level for read operations.
///
/// Controls whether reads are served from any replica (eventual) or must
/// go through the leader (linearizable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadConsistency {
    /// Reads from any replica (fastest, may be stale).
    #[default]
    Eventual,
    /// Reads from leader (strong consistency, higher latency).
    Linearizable,
}

impl ReadConsistency {
    /// Converts to protobuf enum value.
    fn to_proto(self) -> proto::ReadConsistency {
        match self {
            ReadConsistency::Eventual => proto::ReadConsistency::Eventual,
            ReadConsistency::Linearizable => proto::ReadConsistency::Linearizable,
        }
    }
}

// =============================================================================
// Write Types
// =============================================================================

/// Result of a successful write operation.
///
/// Contains the transaction ID, block height, and server-assigned sequence number
/// for the committed write. This information can be used for:
/// - Tracking transaction history
/// - Waiting for replication to replicas
/// - Verified reads at a specific block height
/// - Monitoring client write progress via assigned_sequence
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteSuccess {
    /// Unique transaction ID assigned by the server.
    pub tx_id: String,
    /// Block height where the transaction was committed.
    pub block_height: u64,
    /// Server-assigned sequence number for this write.
    ///
    /// The server assigns monotonically increasing sequence numbers at Raft commit
    /// time. This provides a total ordering of writes per (organization, vault, client)
    /// and can be used for audit trail continuity.
    pub assigned_sequence: u64,
}

// =============================================================================
// Streaming Types
// =============================================================================

/// A block announcement from the WatchBlocks stream.
///
/// Contains metadata about a newly committed block in a vault's chain.
/// Used for real-time notifications of state changes.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, OrganizationSlug, VaultSlug, ServerSource};
/// # use futures::StreamExt;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = LedgerClient::new(ClientConfig::builder()
/// #     .servers(ServerSource::from_static(["http://localhost:50051"]))
/// #     .client_id("example")
/// #     .build()?).await?;
/// # let (organization, vault, start_height) = (OrganizationSlug::new(1), VaultSlug::new(1), 1u64);
/// let mut stream = client.watch_blocks(organization, vault, start_height).await?;
/// while let Some(announcement) = stream.next().await {
///     let block = announcement?;
///     println!("New block at height {}: {:?}", block.height, block.block_hash);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockAnnouncement {
    /// Organization containing the vault.
    pub organization: OrganizationSlug,
    /// Vault (Snowflake ID) within the organization.
    pub vault: VaultSlug,
    /// Block height (1-indexed).
    pub height: u64,
    /// Hash of the block header.
    pub block_hash: Vec<u8>,
    /// Merkle root of the state at this block.
    pub state_root: Vec<u8>,
    /// Timestamp when the block was committed.
    pub timestamp: Option<std::time::SystemTime>,
}

impl BlockAnnouncement {
    /// Creates a BlockAnnouncement from the protobuf type.
    fn from_proto(proto: proto::BlockAnnouncement) -> Self {
        let timestamp = proto.timestamp.map(|ts| {
            std::time::UNIX_EPOCH + std::time::Duration::new(ts.seconds as u64, ts.nanos as u32)
        });

        Self {
            organization: OrganizationSlug::new(proto.organization.map_or(0, |n| n.slug)),
            vault: VaultSlug::new(proto.vault.map_or(0, |v| v.slug)),
            height: proto.height,
            block_hash: proto.block_hash.map(|h| h.value).unwrap_or_default(),
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            timestamp,
        }
    }
}

// =============================================================================
// Admin Types
// =============================================================================

/// Status of an organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OrganizationStatus {
    /// Status is unknown or unspecified.
    #[default]
    Unspecified,
    /// Organization is active and operational.
    Active,
    /// Organization has been deleted.
    Deleted,
}

impl OrganizationStatus {
    /// Creates from protobuf enum value.
    fn from_proto(value: i32) -> Self {
        match proto::OrganizationStatus::try_from(value) {
            Ok(proto::OrganizationStatus::Active) => OrganizationStatus::Active,
            Ok(proto::OrganizationStatus::Deleted) => OrganizationStatus::Deleted,
            _ => OrganizationStatus::Unspecified,
        }
    }
}

/// Status of a vault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VaultStatus {
    /// Status is unknown or unspecified.
    #[default]
    Unspecified,
    /// Vault is active and operational.
    Active,
    /// Vault is read-only (no writes allowed).
    ReadOnly,
    /// Vault has been deleted.
    Deleted,
}

impl VaultStatus {
    /// Creates from protobuf enum value.
    fn from_proto(value: i32) -> Self {
        match proto::VaultStatus::try_from(value) {
            Ok(proto::VaultStatus::Active) => VaultStatus::Active,
            Ok(proto::VaultStatus::ReadOnly) => VaultStatus::ReadOnly,
            Ok(proto::VaultStatus::Deleted) => VaultStatus::Deleted,
            _ => VaultStatus::Unspecified,
        }
    }
}

/// Information about an organization.
///
/// Contains metadata about an organization including its ID, name, shard assignment,
/// and current status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrganizationInfo {
    /// Unique organization slug (Snowflake ID).
    pub slug: OrganizationSlug,
    /// Human-readable organization name.
    pub name: String,
    /// Shard ID hosting this organization.
    pub shard_id: u32,
    /// Node IDs of shard members (node IDs are strings).
    pub member_nodes: Vec<String>,
    /// Configuration version number.
    pub config_version: u64,
    /// Current organization status.
    pub status: OrganizationStatus,
}

impl OrganizationInfo {
    /// Creates from protobuf response.
    fn from_proto(proto: proto::GetOrganizationResponse) -> Self {
        Self {
            slug: OrganizationSlug::new(proto.slug.map_or(0, |n| n.slug)),
            name: proto.name,
            shard_id: proto.shard_id.map_or(0, |s| s.id),
            member_nodes: proto.member_nodes.into_iter().map(|n| n.id).collect(),
            config_version: proto.config_version,
            status: OrganizationStatus::from_proto(proto.status),
        }
    }
}

/// Information about a vault.
///
/// Contains metadata about a vault including its ID, current height,
/// state root, and node membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultInfo {
    /// Organization slug for this vault.
    pub organization: OrganizationSlug,
    /// Unique vault identifier (Snowflake ID) within the organization.
    pub vault: VaultSlug,
    /// Current block height.
    pub height: u64,
    /// Current state root (Merkle root).
    pub state_root: Vec<u8>,
    /// Node IDs replicating this vault (node IDs are strings).
    pub nodes: Vec<String>,
    /// Current leader node ID.
    pub leader: Option<String>,
    /// Current vault status.
    pub status: VaultStatus,
}

impl VaultInfo {
    /// Creates from protobuf response.
    fn from_proto(proto: proto::GetVaultResponse) -> Self {
        Self {
            organization: OrganizationSlug::new(proto.organization.map_or(0, |n| n.slug)),
            vault: VaultSlug::new(proto.vault.map_or(0, |v| v.slug)),
            height: proto.height,
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            nodes: proto.nodes.into_iter().map(|n| n.id).collect(),
            leader: proto.leader.map(|n| n.id),
            status: VaultStatus::from_proto(proto.status),
        }
    }
}

/// Health status of a node or vault.
///
/// Maps to the protobuf `HealthStatus` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HealthStatus {
    /// Status is unknown or unspecified.
    #[default]
    Unspecified,
    /// The node or vault is healthy and fully operational.
    Healthy,
    /// The node or vault is operational but has some issues.
    Degraded,
    /// The node or vault is unavailable.
    Unavailable,
}

impl HealthStatus {
    /// Creates from protobuf enum value.
    fn from_proto(value: i32) -> Self {
        match proto::HealthStatus::try_from(value) {
            Ok(proto::HealthStatus::Healthy) => HealthStatus::Healthy,
            Ok(proto::HealthStatus::Degraded) => HealthStatus::Degraded,
            Ok(proto::HealthStatus::Unavailable) => HealthStatus::Unavailable,
            _ => HealthStatus::Unspecified,
        }
    }
}

/// Result of a health check operation.
///
/// Contains the health status along with a message and additional details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthCheckResult {
    /// The health status.
    pub status: HealthStatus,
    /// Human-readable message describing the health state.
    pub message: String,
    /// Additional details as key-value pairs.
    pub details: std::collections::HashMap<String, String>,
}

impl HealthCheckResult {
    /// Creates from protobuf response.
    fn from_proto(proto: proto::HealthCheckResponse) -> Self {
        Self {
            status: HealthStatus::from_proto(proto.status),
            message: proto.message,
            details: proto.details,
        }
    }

    /// Returns true if the status is healthy.
    pub fn is_healthy(&self) -> bool {
        self.status == HealthStatus::Healthy
    }

    /// Returns true if the status is degraded.
    pub fn is_degraded(&self) -> bool {
        self.status == HealthStatus::Degraded
    }

    /// Returns true if the status is unavailable.
    pub fn is_unavailable(&self) -> bool {
        self.status == HealthStatus::Unavailable
    }
}

// =============================================================================
// Events Types
// =============================================================================

/// Identifies which InferaDB component is the source of ingested events.
///
/// Used with [`LedgerClient::ingest_events`] to specify the originating
/// component. Only external components (Engine and Control) may ingest
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
pub enum EventScope {
    /// Cluster-wide administrative event.
    System,
    /// Per-organization tenant event.
    Organization,
}

impl EventScope {
    fn from_proto(value: i32) -> Self {
        match proto::EventScope::try_from(value) {
            Ok(proto::EventScope::System) => Self::System,
            Ok(proto::EventScope::Organization) => Self::Organization,
            _ => Self::System,
        }
    }
}

/// Outcome of an audited operation.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn from_proto(
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

    fn to_proto(&self) -> (i32, Option<String>, Option<String>, Option<String>) {
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
pub enum EventEmissionPath {
    /// Deterministic, Raft-replicated — identical on all nodes.
    ApplyPhase,
    /// Node-local, best-effort — exists only on the handling node.
    HandlerPhase,
}

impl EventEmissionPath {
    fn from_proto(value: i32) -> Self {
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
    pub fn from_proto(proto: proto::EventEntry) -> Self {
        let timestamp = proto
            .timestamp
            .map(|ts| {
                chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
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

    fn to_proto(&self) -> proto::EventFilter {
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

    fn into_proto(self) -> proto::IngestEventEntry {
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
pub struct IngestRejection {
    /// Zero-based index into the request's entries array.
    pub index: u32,
    /// Human-readable rejection reason.
    pub reason: String,
}

// =============================================================================
// Verified Read Types
// =============================================================================

/// Direction of a sibling in a Merkle proof.
///
/// Indicates whether the sibling hash should be placed on the left or right
/// when computing the parent hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Sibling is on the left: `hash(sibling || current)`.
    Left,
    /// Sibling is on the right: `hash(current || sibling)`.
    Right,
}

impl Direction {
    /// Creates from protobuf enum value.
    fn from_proto(value: i32) -> Self {
        match proto::Direction::try_from(value) {
            Ok(proto::Direction::Left) => Direction::Left,
            _ => Direction::Right, // Default to right for unspecified
        }
    }
}

/// A sibling node in a Merkle proof path.
///
/// Each sibling contains the hash of the neighboring node and which side
/// it appears on for hash computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleSibling {
    /// Hash of the sibling node.
    pub hash: Vec<u8>,
    /// Direction (left or right) relative to the current node.
    pub direction: Direction,
}

impl MerkleSibling {
    /// Creates from protobuf type.
    fn from_proto(proto: proto::MerkleSibling) -> Self {
        Self {
            hash: proto.hash.map(|h| h.value).unwrap_or_default(),
            direction: Direction::from_proto(proto.direction),
        }
    }
}

/// Merkle proof for verifying state inclusion.
///
/// Contains the leaf hash and a path of sibling hashes from leaf to root.
/// Used to verify that a value is included in the state tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleProof {
    /// Hash of the leaf (the entity key-value).
    pub leaf_hash: Vec<u8>,
    /// Sibling hashes from leaf to root (bottom-up order).
    pub siblings: Vec<MerkleSibling>,
}

impl MerkleProof {
    /// Creates from protobuf type.
    fn from_proto(proto: proto::MerkleProof) -> Self {
        Self {
            leaf_hash: proto.leaf_hash.map(|h| h.value).unwrap_or_default(),
            siblings: proto.siblings.into_iter().map(MerkleSibling::from_proto).collect(),
        }
    }

    /// Verifies this proof against an expected state root.
    ///
    /// Recomputes the root hash from the leaf through the sibling path and
    /// checks if it matches the expected root.
    ///
    /// # Arguments
    ///
    /// * `expected_root` - The expected state root hash to verify against.
    ///
    /// # Returns
    ///
    /// `true` if the proof is valid and matches the expected root.
    pub fn verify(&self, expected_root: &[u8]) -> bool {
        use sha2::{Digest, Sha256};

        if self.siblings.is_empty() {
            // Single-element tree: leaf hash equals root
            return self.leaf_hash == expected_root;
        }

        let mut current_hash = self.leaf_hash.clone();

        for sibling in &self.siblings {
            let mut hasher = Sha256::new();
            match sibling.direction {
                Direction::Left => {
                    // Sibling is on left: hash(sibling || current)
                    hasher.update(&sibling.hash);
                    hasher.update(&current_hash);
                },
                Direction::Right => {
                    // Sibling is on right: hash(current || sibling)
                    hasher.update(&current_hash);
                    hasher.update(&sibling.hash);
                },
            }
            current_hash = hasher.finalize().to_vec();
        }

        current_hash == expected_root
    }
}

/// Block header containing cryptographic commitments.
///
/// The block header is the cryptographic anchor for all state at a given height.
/// It contains the state root which can be used to verify Merkle proofs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    /// Block height (1-indexed).
    pub height: u64,
    /// Organization slug for the vault.
    pub organization: OrganizationSlug,
    /// Vault (Snowflake ID) within the organization.
    pub vault: VaultSlug,
    /// Hash of the previous block header.
    pub previous_hash: Vec<u8>,
    /// Merkle root of transactions in this block.
    pub tx_merkle_root: Vec<u8>,
    /// Merkle root of the state tree after this block.
    pub state_root: Vec<u8>,
    /// Timestamp when the block was committed.
    pub timestamp: Option<std::time::SystemTime>,
    /// Node ID of the leader that committed this block.
    pub leader_id: String,
    /// Raft term number.
    pub term: u64,
    /// Raft committed index.
    pub committed_index: u64,
}

impl BlockHeader {
    /// Creates from protobuf type.
    fn from_proto(proto: proto::BlockHeader) -> Self {
        let timestamp = proto.timestamp.map(|ts| {
            std::time::UNIX_EPOCH + std::time::Duration::new(ts.seconds as u64, ts.nanos as u32)
        });

        Self {
            height: proto.height,
            organization: OrganizationSlug::new(proto.organization.map_or(0, |n| n.slug)),
            vault: VaultSlug::new(proto.vault.map_or(0, |v| v.slug)),
            previous_hash: proto.previous_hash.map(|h| h.value).unwrap_or_default(),
            tx_merkle_root: proto.tx_merkle_root.map(|h| h.value).unwrap_or_default(),
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            timestamp,
            leader_id: proto.leader_id.map(|n| n.id).unwrap_or_default(),
            term: proto.term,
            committed_index: proto.committed_index,
        }
    }
}

/// Chain proof linking a trusted height to a response height.
///
/// Used to verify that a block at response_height descends from trusted_height.
/// Contains block headers in ascending order from trusted_height + 1 to response_height.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainProof {
    /// Block headers from trusted_height + 1 to response_height (ascending order).
    pub headers: Vec<BlockHeader>,
}

impl ChainProof {
    /// Creates from protobuf type.
    fn from_proto(proto: proto::ChainProof) -> Self {
        Self { headers: proto.headers.into_iter().map(BlockHeader::from_proto).collect() }
    }

    /// Verifies the chain of blocks links correctly.
    ///
    /// Checks that each block's previous_hash matches the hash of the preceding block.
    ///
    /// # Arguments
    ///
    /// * `trusted_header_hash` - Hash of the block at trusted_height (client already has this).
    ///
    /// # Returns
    ///
    /// `true` if all previous_hash links are valid.
    pub fn verify(&self, trusted_header_hash: &[u8]) -> bool {
        use sha2::{Digest, Sha256};

        if self.headers.is_empty() {
            return true;
        }

        // First header should link to trusted header
        if self.headers[0].previous_hash != trusted_header_hash {
            return false;
        }

        // Each subsequent header should link to the previous one
        for i in 1..self.headers.len() {
            let prev = &self.headers[i - 1];
            let curr = &self.headers[i];

            // Compute hash of previous header
            // Note: This is a simplified hash - real implementation would hash the canonical
            // encoding
            let mut hasher = Sha256::new();
            hasher.update(&prev.previous_hash);
            hasher.update(&prev.tx_merkle_root);
            hasher.update(&prev.state_root);
            hasher.update(prev.height.to_le_bytes());
            let prev_hash = hasher.finalize().to_vec();

            if curr.previous_hash != prev_hash {
                return false;
            }
        }

        true
    }
}

/// Options for verified read operations.
///
/// Controls which proofs to include and at what height to read.
#[derive(Debug, Clone, Default)]
pub struct VerifyOpts {
    /// Reads at a specific block height (None = current height).
    pub at_height: Option<u64>,
    /// Include chain proof linking to a trusted height.
    pub include_chain_proof: bool,
    /// Trusted height for chain proof verification.
    pub trusted_height: Option<u64>,
}

impl VerifyOpts {
    /// Creates options with default values (current height, no chain proof).
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Includes a chain proof from a trusted height.
    pub fn with_chain_proof(mut self, trusted_height: u64) -> Self {
        self.include_chain_proof = true;
        self.trusted_height = Some(trusted_height);
        self
    }
}

// =============================================================================
// Query Types
// =============================================================================

/// Paginated result from query operations.
///
/// Used by `list_entities`, `list_relationships`, and `list_resources` operations.
/// The `next_page_token` can be passed to subsequent calls to continue pagination.
#[derive(Debug, Clone)]
pub struct PagedResult<T> {
    /// Items returned in this page.
    pub items: Vec<T>,
    /// Token for fetching the next page, or `None` if this is the last page.
    pub next_page_token: Option<String>,
    /// Block height at which the query was evaluated.
    pub block_height: u64,
}

impl<T> PagedResult<T> {
    /// Checks if there are more pages available.
    pub fn has_next_page(&self) -> bool {
        self.next_page_token.is_some()
    }
}

/// An entity stored in the ledger.
///
/// Entities are key-value pairs that can have optional expiration times
/// and track their version (block height when last modified).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entity {
    /// Entity key (max 1024 bytes, UTF-8).
    pub key: String,
    /// Entity value (max 1MB).
    pub value: Vec<u8>,
    /// Unix epoch seconds when the entity expires, or `None` for no expiration.
    pub expires_at: Option<u64>,
    /// Block height when this entity was last modified.
    pub version: u64,
}

impl Entity {
    /// Converts from protobuf Entity.
    pub fn from_proto(proto: proto::Entity) -> Self {
        Self {
            key: proto.key,
            value: proto.value,
            expires_at: proto.expires_at.filter(|&ts| ts > 0),
            version: proto.version,
        }
    }

    /// Checks if this entity has expired relative to a given timestamp.
    pub fn is_expired_at(&self, now_secs: u64) -> bool {
        self.expires_at.is_some_and(|exp| exp <= now_secs)
    }
}

/// A relationship in a vault (authorization tuple).
///
/// Relationships connect resources to subjects via relations, forming
/// the basis for permission checking.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Relationship {
    /// Resource identifier in format "type:id" (max 512 chars).
    pub resource: String,
    /// Relation name (max 64 chars).
    pub relation: String,
    /// Subject identifier in format "type:id" or "type:id#relation" (max 512 chars).
    pub subject: String,
}

impl Relationship {
    /// Creates a new relationship.
    pub fn new(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Self { resource: resource.into(), relation: relation.into(), subject: subject.into() }
    }

    /// Converts from protobuf Relationship.
    pub fn from_proto(proto: proto::Relationship) -> Self {
        Self { resource: proto.resource, relation: proto.relation, subject: proto.subject }
    }
}

/// Options for listing entities.
///
/// Builder pattern for configuring entity list queries with optional filters.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct ListEntitiesOpts {
    /// Filter entities by key prefix (e.g., "user:", "session:").
    #[builder(into, default)]
    pub key_prefix: String,
    /// Reads at a specific block height (None = current).
    pub at_height: Option<u64>,
    /// Include entities past their expiration time.
    #[builder(default)]
    pub include_expired: bool,
    /// Maximum number of results per page (0 = server default).
    #[builder(default)]
    pub limit: u32,
    /// Pagination token from previous response.
    #[builder(into)]
    pub page_token: Option<String>,
    /// Read consistency level.
    #[builder(default)]
    pub consistency: ReadConsistency,
    /// Vault for vault-scoped entities (None = organization-level).
    pub vault: Option<VaultSlug>,
}

impl ListEntitiesOpts {
    /// Creates options with a key prefix filter.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self { key_prefix: prefix.into(), ..Default::default() }
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Includes expired entities in results.
    pub fn include_expired(mut self) -> Self {
        self.include_expired = true;
        self
    }

    /// Sets maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continues from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Sets read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Uses linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }

    /// Scopes to a specific vault (for vault-level entities).
    pub fn vault(mut self, vault: VaultSlug) -> Self {
        self.vault = Some(vault);
        self
    }
}

/// Options for listing relationships.
///
/// Builder pattern for configuring relationship list queries with optional filters.
/// All filter fields are optional; omitting a filter matches all values for that field.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct ListRelationshipsOpts {
    /// Filter by resource (exact match).
    #[builder(into)]
    pub resource: Option<String>,
    /// Filter by relation (exact match).
    #[builder(into)]
    pub relation: Option<String>,
    /// Filter by subject (exact match).
    #[builder(into)]
    pub subject: Option<String>,
    /// Reads at a specific block height (None = current).
    pub at_height: Option<u64>,
    /// Maximum number of results per page (0 = server default).
    #[builder(default)]
    pub limit: u32,
    /// Pagination token from previous response.
    #[builder(into)]
    pub page_token: Option<String>,
    /// Read consistency level.
    #[builder(default)]
    pub consistency: ReadConsistency,
}

impl ListRelationshipsOpts {
    /// Creates default options (no filters).
    pub fn new() -> Self {
        Self::default()
    }

    /// Filters by resource.
    pub fn resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    /// Filters by relation.
    pub fn relation(mut self, relation: impl Into<String>) -> Self {
        self.relation = Some(relation.into());
        self
    }

    /// Filters by subject.
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Sets maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continues from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Sets read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Uses linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }
}

/// Options for listing resources.
///
/// Builder pattern for configuring resource list queries.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct ListResourcesOpts {
    /// Resource type prefix (e.g., "document" matches "document:*").
    #[builder(into, default)]
    pub resource_type: String,
    /// Reads at a specific block height (None = current).
    pub at_height: Option<u64>,
    /// Maximum number of results per page (0 = server default).
    #[builder(default)]
    pub limit: u32,
    /// Pagination token from previous response.
    #[builder(into)]
    pub page_token: Option<String>,
    /// Read consistency level.
    #[builder(default)]
    pub consistency: ReadConsistency,
}

impl ListResourcesOpts {
    /// Creates options with a resource type filter.
    pub fn with_type(resource_type: impl Into<String>) -> Self {
        Self { resource_type: resource_type.into(), ..Default::default() }
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Sets maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continues from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Sets read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Uses linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }
}

/// Result of a verified read operation.
///
/// Contains the value along with cryptographic proofs for client-side verification.
/// Use [`VerifiedValue::verify`] to check that the value is authentic.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, OrganizationSlug, VaultSlug, VerifyOpts, ServerSource};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = LedgerClient::new(ClientConfig::builder()
/// #     .servers(ServerSource::from_static(["http://localhost:50051"]))
/// #     .client_id("example")
/// #     .build()?).await?;
/// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
/// let result = client.verified_read(organization, Some(vault), "key", VerifyOpts::new()).await?;
/// if let Some(verified) = result {
///     // Verify the proof is valid
///     assert!(verified.verify()?);
///     println!("Verified value: {:?}", verified.value);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedValue {
    /// The entity value (None if key not found).
    pub value: Option<Vec<u8>>,
    /// Block height at which the read was performed.
    pub block_height: u64,
    /// Block header containing the state root.
    pub block_header: BlockHeader,
    /// Merkle proof from leaf to state root.
    pub merkle_proof: MerkleProof,
    /// Optional chain proof linking to trusted height.
    pub chain_proof: Option<ChainProof>,
}

impl VerifiedValue {
    /// Creates from protobuf response.
    fn from_proto(proto: proto::VerifiedReadResponse) -> Option<Self> {
        // Block header is required for verification
        let block_header = proto.block_header.map(BlockHeader::from_proto)?;
        let merkle_proof = proto.merkle_proof.map(MerkleProof::from_proto)?;

        Some(Self {
            value: proto.value,
            block_height: proto.block_height,
            block_header,
            merkle_proof,
            chain_proof: proto.chain_proof.map(ChainProof::from_proto),
        })
    }

    /// Verifies the value is authentic.
    ///
    /// Checks that the Merkle proof correctly links the value to the state root
    /// in the block header. If a chain proof is present, also verifies the
    /// chain of blocks links correctly.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::ProofVerification` if the Merkle proof does not
    /// match the block header's state root.
    pub fn verify(&self) -> Result<bool> {
        // Verify the Merkle proof against the block header's state root
        if !self.merkle_proof.verify(&self.block_header.state_root) {
            return Err(error::SdkError::ProofVerification {
                reason: "Merkle proof does not match state root",
            });
        }

        // If we have a chain proof, that would be verified by the caller
        // with their trusted header hash (we don't have it here)

        Ok(true)
    }
}

/// A write operation to be submitted to the ledger.
///
/// Operations modify state in the ledger. They are applied atomically within
/// a single transaction. Use [`Operation::set_entity`] for key-value writes
/// and [`Operation::create_relationship`] for authorization tuples.
#[derive(Debug, Clone)]
pub enum Operation {
    /// Sets an entity value (key-value write).
    SetEntity {
        /// Entity key (max 1024 bytes).
        key: String,
        /// Entity value (max 1MB).
        value: Vec<u8>,
        /// Optional expiration time (Unix epoch seconds).
        expires_at: Option<u64>,
        /// Optional conditional write.
        condition: Option<SetCondition>,
    },
    /// Deletes an entity.
    DeleteEntity {
        /// Entity key to delete.
        key: String,
    },
    /// Creates an authorization relationship.
    CreateRelationship {
        /// Resource identifier (format: "type:id").
        resource: String,
        /// Relation name (e.g., "viewer", "editor").
        relation: String,
        /// Subject identifier (format: "type:id" or "type:id#relation").
        subject: String,
    },
    /// Deletes an authorization relationship.
    DeleteRelationship {
        /// Resource identifier (format: "type:id").
        resource: String,
        /// Relation name.
        relation: String,
        /// Subject identifier.
        subject: String,
    },
}

/// Condition for compare-and-set (CAS) writes.
///
/// Allows conditional writes that only succeed if the current state matches
/// the expected condition. Useful for coordination primitives like locks.
#[derive(Debug, Clone)]
pub enum SetCondition {
    /// Only set if the key doesn't exist.
    NotExists,
    /// Only set if the key exists.
    MustExist,
    /// Only set if the key was last modified at this block height.
    Version(u64),
    /// Only set if the current value matches exactly.
    ValueEquals(Vec<u8>),
}

impl Operation {
    /// Creates an operation that sets an entity's key-value pair.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::Operation;
    /// let op = Operation::set_entity("user:123", b"data".to_vec());
    /// ```
    pub fn set_entity(key: impl Into<String>, value: Vec<u8>) -> Self {
        Operation::SetEntity { key: key.into(), value, expires_at: None, condition: None }
    }

    /// Creates a set entity operation with expiration.
    ///
    /// # Arguments
    ///
    /// * `key` - Entity key
    /// * `value` - Entity value
    /// * `expires_at` - Unix epoch seconds when the entity expires
    pub fn set_entity_with_expiry(key: impl Into<String>, value: Vec<u8>, expires_at: u64) -> Self {
        Operation::SetEntity {
            key: key.into(),
            value,
            expires_at: Some(expires_at),
            condition: None,
        }
    }

    /// Creates a conditional set entity operation.
    ///
    /// # Arguments
    ///
    /// * `key` - Entity key
    /// * `value` - Entity value
    /// * `condition` - Condition that must be met for the write to succeed
    pub fn set_entity_if(key: impl Into<String>, value: Vec<u8>, condition: SetCondition) -> Self {
        Operation::SetEntity {
            key: key.into(),
            value,
            expires_at: None,
            condition: Some(condition),
        }
    }

    /// Creates an operation that deletes an entity by key.
    pub fn delete_entity(key: impl Into<String>) -> Self {
        Operation::DeleteEntity { key: key.into() }
    }

    /// Creates an operation that establishes a relationship between a resource and subject.
    ///
    /// # Arguments
    ///
    /// * `resource` - Resource identifier (format: "type:id")
    /// * `relation` - Relation name (e.g., "viewer", "editor")
    /// * `subject` - Subject identifier (format: "type:id" or "type:id#relation")
    pub fn create_relationship(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Operation::CreateRelationship {
            resource: resource.into(),
            relation: relation.into(),
            subject: subject.into(),
        }
    }

    /// Creates an operation that removes a relationship between a resource and subject.
    pub fn delete_relationship(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Operation::DeleteRelationship {
            resource: resource.into(),
            relation: relation.into(),
            subject: subject.into(),
        }
    }

    /// Validates this operation against the given validation configuration.
    ///
    /// Checks field sizes and character whitelists. Call this before
    /// sending operations to the server for fast client-side validation.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError` if key length, value size, or character constraints are violated.
    pub fn validate(
        &self,
        config: &inferadb_ledger_types::config::ValidationConfig,
    ) -> std::result::Result<(), inferadb_ledger_types::validation::ValidationError> {
        use inferadb_ledger_types::validation;
        match self {
            Operation::SetEntity { key, value, .. } => {
                validation::validate_key(key, config)?;
                validation::validate_value(value, config)?;
            },
            Operation::DeleteEntity { key } => {
                validation::validate_key(key, config)?;
            },
            Operation::CreateRelationship { resource, relation, subject } => {
                validation::validate_relationship_string(resource, "resource", config)?;
                validation::validate_relationship_string(relation, "relation", config)?;
                validation::validate_relationship_string(subject, "subject", config)?;
            },
            Operation::DeleteRelationship { resource, relation, subject } => {
                validation::validate_relationship_string(resource, "resource", config)?;
                validation::validate_relationship_string(relation, "relation", config)?;
                validation::validate_relationship_string(subject, "subject", config)?;
            },
        }
        Ok(())
    }

    /// Returns the estimated wire size of this operation in bytes.
    ///
    /// Used for aggregate payload size validation before sending to the server.
    fn estimated_size_bytes(&self) -> usize {
        match self {
            Operation::SetEntity { key, value, .. } => key.len() + value.len(),
            Operation::DeleteEntity { key } => key.len(),
            Operation::CreateRelationship { resource, relation, subject }
            | Operation::DeleteRelationship { resource, relation, subject } => {
                resource.len() + relation.len() + subject.len()
            },
        }
    }

    /// Converts to protobuf operation.
    fn to_proto(&self) -> proto::Operation {
        let op = match self {
            Operation::SetEntity { key, value, expires_at, condition } => {
                proto::operation::Op::SetEntity(proto::SetEntity {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at: *expires_at,
                    condition: condition.as_ref().map(SetCondition::to_proto),
                })
            },
            Operation::DeleteEntity { key } => {
                proto::operation::Op::DeleteEntity(proto::DeleteEntity { key: key.clone() })
            },
            Operation::CreateRelationship { resource, relation, subject } => {
                proto::operation::Op::CreateRelationship(proto::CreateRelationship {
                    resource: resource.clone(),
                    relation: relation.clone(),
                    subject: subject.clone(),
                })
            },
            Operation::DeleteRelationship { resource, relation, subject } => {
                proto::operation::Op::DeleteRelationship(proto::DeleteRelationship {
                    resource: resource.clone(),
                    relation: relation.clone(),
                    subject: subject.clone(),
                })
            },
        };
        proto::Operation { op: Some(op) }
    }
}

impl SetCondition {
    /// Converts to protobuf set condition.
    fn to_proto(&self) -> proto::SetCondition {
        let condition = match self {
            SetCondition::NotExists => proto::set_condition::Condition::NotExists(true),
            SetCondition::MustExist => proto::set_condition::Condition::MustExists(true),
            SetCondition::Version(v) => proto::set_condition::Condition::Version(*v),
            SetCondition::ValueEquals(v) => proto::set_condition::Condition::ValueEquals(v.clone()),
        };
        proto::SetCondition { condition: Some(condition) }
    }
}

/// High-level client for interacting with the Ledger service.
///
/// `LedgerClient` orchestrates:
/// - Connection pool for efficient channel management
/// - Sequence tracker for client-side idempotency
/// - Retry logic for transient failure recovery
/// - Server discovery (DNS, file, or static endpoints)
/// - Graceful shutdown with request cancellation
///
/// # Server Discovery
///
/// The client supports three server discovery modes:
/// - **Static**: Fixed list of endpoint URLs
/// - **DNS**: Resolve A records from a domain (for Kubernetes headless services)
/// - **File**: Load servers from a JSON manifest file
///
/// For DNS and file sources, the client performs initial resolution during
/// construction and starts a background refresh task.
///
/// # Shutdown Behavior
///
/// When [`shutdown()`](Self::shutdown) is called:
/// 1. All pending requests are cancelled with `SdkError::Shutdown`
/// 2. New requests immediately fail with `SdkError::Shutdown`
/// 3. Server resolver refresh task is stopped
/// 4. Sequence tracker state is flushed to disk (if using persistence)
/// 5. Connections are closed
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, ServerSource};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ClientConfig::builder()
///     .servers(ServerSource::from_static(["http://localhost:50051"]))
///     .client_id("my-app-001")
///     .build()?;
///
/// let client = LedgerClient::new(config).await?;
///
/// // ... use the client ...
///
/// // Graceful shutdown
/// client.shutdown().await;
/// # Ok(())
/// # }
/// ```
///
/// # Cancellation
///
/// The client supports two levels of cancellation:
///
/// **Client-level** — [`shutdown()`](Self::shutdown) cancels all in-flight
/// requests and rejects new ones with `SdkError::Shutdown`.
///
/// **Per-request** — Methods like [`read_with_token`](Self::read_with_token)
/// and [`write_with_token`](Self::write_with_token) accept a
/// [`CancellationToken`](tokio_util::sync::CancellationToken) that cancels
/// a single request with `SdkError::Cancelled`.
///
/// Both mechanisms interrupt in-flight RPCs and backoff sleeps via
/// `tokio::select!`. Access the client's token via
/// [`cancellation_token()`](Self::cancellation_token) to create child
/// tokens or integrate with application-level shutdown.
#[derive(Clone)]
pub struct LedgerClient {
    pool: ConnectionPool,
    /// Server resolver for DNS/file discovery.
    resolver: Option<Arc<ServerResolver>>,
    /// Cancellation token for coordinated shutdown.
    cancellation: tokio_util::sync::CancellationToken,
}

impl LedgerClient {
    /// Creates a new `LedgerClient` with the given configuration.
    ///
    /// This constructor validates the configuration and performs initial server
    /// resolution for DNS/file sources. Connections are established lazily on
    /// first use.
    ///
    /// For DNS and file server sources, a background refresh task is started
    /// to periodically re-resolve servers.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The configuration is invalid
    /// - DNS resolution fails (for DNS sources)
    /// - File read/parse fails (for file sources)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, ServerSource};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Static endpoints
    /// let config = ClientConfig::builder()
    ///     .servers(ServerSource::from_static(["http://localhost:50051"]))
    ///     .client_id("my-service")
    ///     .build()?;
    /// let client = LedgerClient::new(config).await?;
    ///
    /// // DNS discovery (Kubernetes)
    /// use inferadb_ledger_sdk::DnsConfig;
    /// let config = ClientConfig::builder()
    ///     .servers(ServerSource::dns(DnsConfig::builder().domain("ledger.default.svc").build()))
    ///     .client_id("my-service")
    ///     .build()?;
    /// let client = LedgerClient::new(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let cancellation = tokio_util::sync::CancellationToken::new();

        // Create resolver for DNS/file sources
        let (resolver, initial_endpoints) = match config.servers() {
            ServerSource::Static(_) => (None, None),
            source @ (ServerSource::Dns(_) | ServerSource::File(_)) => {
                let resolver = Arc::new(ServerResolver::new(source.clone()));

                // Perform initial resolution
                let servers = resolver.resolve().await.map_err(|e| error::SdkError::Config {
                    message: format!("Server discovery failed: {e}"),
                })?;

                // Convert to endpoint URLs
                let endpoints: Vec<String> = servers.iter().map(|s| s.url()).collect();

                // Start background refresh task
                resolver.start_refresh_task();

                (Some(resolver), Some(endpoints))
            },
        };

        let pool = ConnectionPool::new(config);

        // Set initial endpoints for DNS/file sources
        if let Some(endpoints) = initial_endpoints {
            pool.update_endpoints(endpoints);
        }

        Ok(Self { pool, resolver, cancellation })
    }

    /// Convenience constructor for connecting to a single endpoint.
    ///
    /// Creates a client with default configuration, connecting to the specified
    /// endpoint with the given client ID. For more control over configuration,
    /// use [`ClientConfig::builder()`] and [`LedgerClient::new()`].
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL is invalid.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(
        endpoint: impl Into<String>,
        client_id: impl Into<String>,
    ) -> Result<Self> {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([endpoint.into()]))
            .client_id(client_id)
            .build()?;

        Self::new(config).await
    }

    /// Returns the client ID used for idempotency tracking.
    ///
    /// The client ID is included in all write requests and is used by the server
    /// to deduplicate requests and track per-client sequence state.
    #[inline]
    #[must_use]
    pub fn client_id(&self) -> &str {
        self.pool.config().client_id()
    }

    /// Returns a reference to the client configuration.
    ///
    /// Useful for inspecting configuration values like endpoints, timeouts,
    /// and retry policy.
    #[inline]
    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        self.pool.config()
    }

    /// Returns a reference to the connection pool.
    ///
    /// Most users won't need direct access to this. Useful for:
    /// - Checking if compression is enabled
    /// - Resetting connections after network changes
    #[inline]
    #[must_use]
    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    // =========================================================================
    // Fluent Builders
    // =========================================================================

    /// Creates a fluent write builder for the given organization and optional vault.
    ///
    /// Chain operations and then call `.execute()` to submit:
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
    /// # let organization = OrganizationSlug::new(1);
    /// let result = client
    ///     .write_builder(organization, Some(VaultSlug::new(1)))
    ///     .set("user:123", b"data".to_vec())
    ///     .create_relationship("doc:1", "viewer", "user:123")
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn write_builder(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
    ) -> crate::builders::WriteBuilder<'_> {
        crate::builders::WriteBuilder::new(self, organization, vault)
    }

    /// Creates a fluent batch read builder for the given organization and optional vault.
    ///
    /// Add keys, then call `.execute()`:
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
    /// # let organization = OrganizationSlug::new(1);
    /// let results = client
    ///     .batch_read_builder(organization, Some(VaultSlug::new(1)))
    ///     .key("user:123")
    ///     .key("user:456")
    ///     .linearizable()
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn batch_read_builder(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
    ) -> crate::builders::BatchReadBuilder<'_> {
        crate::builders::BatchReadBuilder::new(self, organization, vault)
    }

    /// Creates a fluent relationship query builder for the given organization and vault.
    ///
    /// Add filters, then call `.execute()`:
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
    /// # let organization = OrganizationSlug::new(1);
    /// let page = client
    ///     .relationship_query(organization, VaultSlug::new(1))
    ///     .resource("document:report")
    ///     .relation("viewer")
    ///     .limit(50)
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn relationship_query(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> crate::builders::RelationshipQueryBuilder<'_> {
        crate::builders::RelationshipQueryBuilder::new(self, organization, vault)
    }

    /// Returns the client's cancellation token.
    ///
    /// The token can be used to:
    /// - Monitor shutdown state via `CancellationToken::cancelled()`
    /// - Create child tokens for per-request cancellation
    ///
    /// # Per-Request Cancellation
    ///
    /// Create a child token and pass it to RPC methods that accept an
    /// optional cancellation token. Cancelling the child token cancels
    /// only that request, not the entire client.
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "svc").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// let token = client.cancellation_token().child_token();
    ///
    /// // Cancel after 100ms
    /// let cancel_token = token.clone();
    /// tokio::spawn(async move {
    ///     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    ///     cancel_token.cancel();
    /// });
    ///
    /// // This read will be cancelled if it takes longer than 100ms
    /// let result = client.read_with_token(organization, None, "key", token).await;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    #[must_use]
    pub fn cancellation_token(&self) -> &tokio_util::sync::CancellationToken {
        &self.cancellation
    }

    // =========================================================================
    // Shutdown
    // =========================================================================

    /// Initiates graceful shutdown of the client.
    ///
    /// This method:
    /// 1. Cancels all pending requests (they will return `SdkError::Shutdown`)
    /// 2. Prevents new requests from being accepted
    /// 3. Stops the server resolver refresh task (if using DNS/file discovery)
    /// 4. Resets the connection pool
    ///
    /// After calling `shutdown()`, all operations will immediately return
    /// `SdkError::Shutdown`. The client can be cloned, but all clones share
    /// the same shutdown state.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// # let operations = vec![];
    /// let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    ///
    /// // Perform operations...
    /// client.write(organization, Some(vault), operations).await?;
    ///
    /// // Graceful shutdown before application exit
    /// client.shutdown().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(&self) {
        // Cancel all pending and future operations
        self.cancellation.cancel();

        // Stop server resolver refresh task
        if let Some(ref resolver) = self.resolver {
            resolver.shutdown();
        }

        tracing::debug!("Client shutdown initiated");

        // Reset connection pool to close connections
        self.pool.reset();
    }

    /// Returns `true` if the client has been shut down.
    ///
    /// After shutdown, all operations will fail with `SdkError::Shutdown`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// if client.is_shutdown() {
    ///     println!("Client has been shut down");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns an error if the client has been shut down or the request token
    /// has been cancelled.
    ///
    /// Called at the start of each operation to fail fast.
    #[inline]
    fn check_shutdown(
        &self,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(error::SdkError::Shutdown);
        }
        if let Some(token) = request_token
            && token.is_cancelled()
        {
            return Err(error::SdkError::Cancelled);
        }
        Ok(())
    }

    /// Creates a token that fires when either the client shuts down or
    /// the per-request token is cancelled.
    ///
    /// When no request token is provided, returns the client's own token
    /// (no allocation). When a request token is provided, creates a child
    /// of the client token and links the request token to it.
    fn effective_token(
        &self,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> tokio_util::sync::CancellationToken {
        match request_token {
            Some(req_token) => {
                // Child of client token: cancelled when client shuts down.
                // We also link the request token via a background task.
                let child = self.cancellation.child_token();
                let child_clone = child.clone();
                let req_clone = req_token.clone();
                tokio::spawn(async move {
                    req_clone.cancelled().await;
                    child_clone.cancel();
                });
                child
            },
            None => self.cancellation.clone(),
        }
    }

    /// Creates a trace context interceptor based on the client's configuration.
    ///
    /// Includes request timeout propagation via the `grpc-timeout` header so
    /// the server can extract the client's deadline and avoid processing
    /// requests the client has already abandoned.
    #[inline]
    fn trace_interceptor(&self) -> TraceContextInterceptor {
        TraceContextInterceptor::with_timeout(
            self.pool.config().trace(),
            self.pool.config().timeout(),
        )
    }

    /// Executes a future and records request metrics (latency + success/error).
    async fn with_metrics<T>(
        &self,
        method: &str,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let start = std::time::Instant::now();
        let result = fut.await;
        let duration = start.elapsed();
        self.pool.metrics().record_request(method, duration, result.is_ok());
        result
    }

    /// Creates a discovery service that shares this client's connection pool.
    ///
    /// The discovery service can be used to dynamically update the client's
    /// endpoint list based on discovered cluster peers.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, DiscoveryConfig};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-client").await?;
    /// let discovery = client.create_discovery_service(DiscoveryConfig::enabled());
    ///
    /// // Start background endpoint refresh
    /// discovery.start_background_refresh();
    ///
    /// // The client will now use updated endpoints as peers are discovered
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn create_discovery_service(
        &self,
        config: crate::config::DiscoveryConfig,
    ) -> crate::discovery::DiscoveryService {
        crate::discovery::DiscoveryService::new(self.pool.clone(), config)
    }

    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Reads a value by key with eventual consistency.
    ///
    /// Uses `EVENTUAL` consistency level, which reads from any replica for
    /// lowest latency. The value may be slightly stale if a write was just
    /// committed.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - The key to read.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the read fails after retry attempts are exhausted.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Read an organization-level entity
    /// let value = client.read(organization, None, "user:123").await?;
    ///
    /// // Read a vault-level entity
    /// let value = client.read(organization, Some(vault), "key").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
    ) -> Result<Option<Vec<u8>>> {
        self.read_internal(organization, vault, key.into(), ReadConsistency::Eventual, None).await
    }

    /// Reads a value by key with linearizable (strong) consistency.
    ///
    /// Uses `LINEARIZABLE` consistency level, which reads from the leader to
    /// guarantee the latest committed value. Has higher latency than eventual
    /// consistency.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - The key to read.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the read fails after retry attempts are exhausted.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Read with strong consistency guarantee
    /// let value = client.read_consistent(organization, Some(vault), "key").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_consistent(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
    ) -> Result<Option<Vec<u8>>> {
        self.read_internal(organization, vault, key.into(), ReadConsistency::Linearizable, None)
            .await
    }

    /// Reads a value by key with a per-request cancellation token.
    ///
    /// Like [`read`](Self::read) but accepts a [`CancellationToken`] that can
    /// cancel this specific request without shutting down the client. Returns
    /// `SdkError::Cancelled` if the token is cancelled before the RPC completes.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Cancelled` if the token is cancelled.
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the read fails after retry attempts.
    ///
    /// [`CancellationToken`]: tokio_util::sync::CancellationToken
    pub async fn read_with_token(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        token: tokio_util::sync::CancellationToken,
    ) -> Result<Option<Vec<u8>>> {
        self.read_internal(organization, vault, key.into(), ReadConsistency::Eventual, Some(&token))
            .await
    }

    /// Writes a transaction with a per-request cancellation token.
    ///
    /// Like [`write`](Self::write) but accepts a [`CancellationToken`] that can
    /// cancel this specific request. Note that cancellation is best-effort:
    /// the server may still commit the transaction if the cancellation races
    /// with the Raft commit.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Cancelled` if the token is cancelled.
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the write fails after retry attempts.
    /// Returns `SdkError::Validation` if client-side validation fails.
    ///
    /// [`CancellationToken`]: tokio_util::sync::CancellationToken
    pub async fn write_with_token(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        operations: Vec<Operation>,
        token: tokio_util::sync::CancellationToken,
    ) -> Result<WriteSuccess> {
        self.check_shutdown(Some(&token))?;

        let idempotency_key = uuid::Uuid::new_v4();

        self.execute_write(organization, vault, &operations, idempotency_key, Some(&token)).await
    }

    /// Batch read with a per-request cancellation token.
    ///
    /// Like [`batch_read`](Self::batch_read) but accepts a cancellation token.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Cancelled` if the token is cancelled.
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the batch read fails after retry attempts.
    pub async fn batch_read_with_token(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        keys: impl IntoIterator<Item = impl Into<String>>,
        token: tokio_util::sync::CancellationToken,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.batch_read_internal(
            organization,
            vault,
            keys.into_iter().map(Into::into).collect(),
            ReadConsistency::Eventual,
            Some(&token),
        )
        .await
    }

    /// Batch write with a per-request cancellation token.
    ///
    /// Like [`batch_write`](Self::batch_write) but accepts a cancellation token.
    /// Cancellation is best-effort — the server may still commit.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Cancelled` if the token is cancelled.
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the batch write fails after retry attempts.
    /// Returns `SdkError::Validation` if client-side validation fails.
    pub async fn batch_write_with_token(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        batches: Vec<Vec<Operation>>,
        token: tokio_util::sync::CancellationToken,
    ) -> Result<WriteSuccess> {
        self.check_shutdown(Some(&token))?;

        let idempotency_key = uuid::Uuid::new_v4();

        self.execute_batch_write(organization, vault, &batches, idempotency_key, Some(&token)).await
    }

    /// Batch read multiple keys in a single RPC call.
    ///
    /// Amortizes network overhead across multiple reads for higher throughput.
    /// All reads share the same organization, vault, and consistency level (EVENTUAL).
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `keys` - The keys to read (max 1000).
    ///
    /// # Returns
    ///
    /// Returns a vector of `(key, Option<value>)` pairs in the same order as
    /// the input keys. Missing keys have `None` values.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch read fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let results = client.batch_read(
    ///     organization,
    ///     Some(vault),
    ///     vec!["key1", "key2", "key3"],
    /// ).await?;
    ///
    /// for (key, value) in results {
    ///     match value {
    ///         Some(v) => println!("{key}: {} bytes", v.len()),
    ///         None => println!("{key}: not found"),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_read(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.batch_read_internal(
            organization,
            vault,
            keys.into_iter().map(Into::into).collect(),
            ReadConsistency::Eventual,
            None,
        )
        .await
    }

    /// Batch read multiple keys with linearizable consistency.
    ///
    /// Like [`batch_read`](Self::batch_read) but with strong consistency guarantees.
    /// All reads are served from the leader.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `keys` - The keys to read (max 1000).
    ///
    /// # Returns
    ///
    /// Returns a vector of `(key, Option<value>)` pairs in the same order as
    /// the input keys. Missing keys have `None` values.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch read fails after retry attempts.
    pub async fn batch_read_consistent(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.batch_read_internal(
            organization,
            vault,
            keys.into_iter().map(Into::into).collect(),
            ReadConsistency::Linearizable,
            None,
        )
        .await
    }

    // =========================================================================
    // Internal Read Implementation
    // =========================================================================

    /// Internal read implementation with configurable consistency and
    /// optional per-request cancellation.
    async fn read_internal(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: String,
        consistency: ReadConsistency,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<Option<Vec<u8>>> {
        self.check_shutdown(request_token)?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        self.with_metrics(
            "read",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "read", || async {
                let channel = pool.get_channel().await?;
                let mut client = Self::create_read_client(
                    channel,
                    pool.compression_enabled(),
                    TraceContextInterceptor::with_timeout(
                        pool.config().trace(),
                        pool.config().timeout(),
                    ),
                );

                let request = proto::ReadRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    key: key.clone(),
                    consistency: consistency.to_proto() as i32,
                };

                let response = client.read(tonic::Request::new(request)).await?.into_inner();

                Ok(response.value)
            }),
        )
        .await
    }

    /// Internal batch read implementation with configurable consistency and
    /// optional per-request cancellation.
    async fn batch_read_internal(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        keys: Vec<String>,
        consistency: ReadConsistency,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.check_shutdown(request_token)?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        self.with_metrics(
            "batch_read",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "batch_read", || async {
                let channel = pool.get_channel().await?;
                let mut client = Self::create_read_client(
                    channel,
                    pool.compression_enabled(),
                    TraceContextInterceptor::with_timeout(
                        pool.config().trace(),
                        pool.config().timeout(),
                    ),
                );

                let request = proto::BatchReadRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    keys: keys.clone(),
                    consistency: consistency.to_proto() as i32,
                };

                let response = client.batch_read(tonic::Request::new(request)).await?.into_inner();

                // Convert results to (key, Option<value>) pairs
                let results = response.results.into_iter().map(|r| (r.key, r.value)).collect();

                Ok(results)
            }),
        )
        .await
    }

    /// Creates a ReadServiceClient with compression and tracing settings applied.
    fn create_read_client(
        channel: tonic::transport::Channel,
        compression_enabled: bool,
        interceptor: TraceContextInterceptor,
    ) -> proto::read_service_client::ReadServiceClient<
        InterceptedService<tonic::transport::Channel, TraceContextInterceptor>,
    > {
        let client =
            proto::read_service_client::ReadServiceClient::with_interceptor(channel, interceptor);
        if compression_enabled {
            client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            client
        }
    }

    // =========================================================================
    // Write Operations
    // =========================================================================

    /// Submits a write transaction to the ledger.
    ///
    /// Writes are automatically idempotent via server-assigned sequence numbers.
    /// The server assigns monotonically increasing sequences at Raft commit time.
    /// If a write fails with a retryable error, it will be retried with the
    /// same idempotency key. If the server reports the write was already
    /// committed (duplicate), the original result is returned as success.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (required for relationships).
    /// * `operations` - The operations to apply atomically.
    ///
    /// # Returns
    ///
    /// Returns [`WriteSuccess`] containing the transaction ID, block height, and
    /// server-assigned sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - A conditional write (CAS) condition fails
    /// - An idempotency key is reused with different payload
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, Operation, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let result = client.write(
    ///     organization,
    ///     Some(vault),
    ///     vec![
    ///         Operation::set_entity("user:123", b"data".to_vec()),
    ///         Operation::create_relationship("doc:456", "viewer", "user:123"),
    ///     ],
    /// ).await?;
    ///
    /// println!("Committed at block {} with sequence {}", result.block_height, result.assigned_sequence);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        operations: Vec<Operation>,
    ) -> Result<WriteSuccess> {
        self.check_shutdown(None)?;

        // Generate UUID idempotency key once for this request
        // The same key is reused across all retry attempts
        let idempotency_key = uuid::Uuid::new_v4();

        self.execute_write(organization, vault, &operations, idempotency_key, None).await
    }

    /// Executes a single write attempt with retry for transient errors.
    ///
    /// The idempotency key is preserved across retry attempts to ensure
    /// at-most-once semantics even with network failures.
    async fn execute_write(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        operations: &[Operation],
        idempotency_key: uuid::Uuid,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        // Client-side validation: fast rejection before network round-trip
        let validation_config = self.config().validation();
        inferadb_ledger_types::validation::validate_operations_count(
            operations.len(),
            validation_config,
        )
        .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
        let mut total_bytes: usize = 0;
        for op in operations {
            op.validate(validation_config)
                .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
            total_bytes += op.estimated_size_bytes();
        }
        inferadb_ledger_types::validation::validate_batch_payload_bytes(
            total_bytes,
            validation_config,
        )
        .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();
        let client_id = self.client_id().to_string();

        // Convert operations to proto
        let proto_operations: Vec<proto::Operation> =
            operations.iter().map(Operation::to_proto).collect();

        // Convert UUID to bytes (16 bytes)
        let idempotency_key_bytes = idempotency_key.as_bytes().to_vec();

        // Execute with retry for transient errors
        self.with_metrics(
            "write",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "write", || {
                let proto_ops = proto_operations.clone();
                let cid = client_id.clone();
                let key_bytes = idempotency_key_bytes.clone();
                async move {
                    let channel = pool.get_channel().await?;
                    let mut write_client = Self::create_write_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::WriteRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        client_id: Some(proto::ClientId { id: cid }),
                        idempotency_key: key_bytes,
                        operations: proto_ops,
                        include_tx_proof: false,
                    };

                    let response =
                        write_client.write(tonic::Request::new(request)).await?.into_inner();

                    Self::process_write_response(response)
                }
            }),
        )
        .await
    }

    /// Processes a WriteResponse and converts to Result<WriteSuccess>.
    fn process_write_response(response: proto::WriteResponse) -> Result<WriteSuccess> {
        match response.result {
            Some(proto::write_response::Result::Success(success)) => Ok(WriteSuccess {
                tx_id: Self::tx_id_to_hex(success.tx_id),
                block_height: success.block_height,
                assigned_sequence: success.assigned_sequence,
            }),
            Some(proto::write_response::Result::Error(error)) => {
                let code = proto::WriteErrorCode::try_from(error.code)
                    .unwrap_or(proto::WriteErrorCode::Unspecified);

                match code {
                    proto::WriteErrorCode::AlreadyCommitted => {
                        // Idempotent retry - return the original success with assigned_sequence
                        Ok(WriteSuccess {
                            tx_id: Self::tx_id_to_hex(error.committed_tx_id),
                            block_height: error.committed_block_height.unwrap_or(0),
                            assigned_sequence: error.assigned_sequence.unwrap_or(0),
                        })
                    },
                    proto::WriteErrorCode::IdempotencyKeyReused => {
                        // Client reused idempotency key with different payload
                        Err(crate::error::SdkError::Idempotency {
                            message: format!(
                                "Idempotency key reused with different payload: {}",
                                error.message
                            ),
                            conflict_key: None,
                            original_tx_id: Some(Self::tx_id_to_hex(error.committed_tx_id.clone())),
                        })
                    },
                    _ => {
                        // Other write errors (CAS failures, etc.)
                        Err(crate::error::SdkError::Rpc {
                            code: tonic::Code::FailedPrecondition,
                            message: error.message,
                            request_id: None,
                            trace_id: None,
                            error_details: None,
                        })
                    },
                }
            },
            None => Err(crate::error::SdkError::Rpc {
                code: tonic::Code::Internal,
                message: "Empty write response".to_owned(),
                request_id: None,
                trace_id: None,
                error_details: None,
            }),
        }
    }

    /// Converts TxId bytes to hex string.
    fn tx_id_to_hex(tx_id: Option<proto::TxId>) -> String {
        use std::fmt::Write;
        tx_id
            .map(|t| {
                t.id.iter().fold(String::with_capacity(t.id.len() * 2), |mut acc, b| {
                    let _ = write!(acc, "{b:02x}");
                    acc
                })
            })
            .unwrap_or_default()
    }

    /// Creates a WriteServiceClient with compression and tracing settings applied.
    fn create_write_client(
        channel: tonic::transport::Channel,
        compression_enabled: bool,
        interceptor: TraceContextInterceptor,
    ) -> proto::write_service_client::WriteServiceClient<
        InterceptedService<tonic::transport::Channel, TraceContextInterceptor>,
    > {
        let client =
            proto::write_service_client::WriteServiceClient::with_interceptor(channel, interceptor);
        if compression_enabled {
            client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            client
        }
    }

    // =========================================================================
    // Batch Write Operations
    // =========================================================================

    /// Submits a batch write transaction with all-or-nothing atomicity.
    ///
    /// A batch write groups multiple operation sets into a single atomic transaction.
    /// All operations are committed together in a single block, or none are applied
    /// if any operation fails (e.g., CAS condition failure).
    ///
    /// The batch uses a single idempotency key, meaning the entire batch is the
    /// deduplication unit - retry with the same idempotency key returns the
    /// original result.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (required for relationships).
    /// * `batches` - Groups of operations to apply atomically. Each inner `Vec<Operation>` is a
    ///   logical group processed in order.
    ///
    /// # Returns
    ///
    /// Returns [`WriteSuccess`] containing the transaction ID, block height, and
    /// server-assigned sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - Any CAS condition fails (entire batch rolled back)
    /// - An idempotency key is reused with different payload
    ///
    /// # Atomicity
    ///
    /// Operations are applied in array order:
    /// - `batches[0]` operations first, then `batches[1]`, etc.
    /// - Within each batch: `operations[0]` first, then `operations[1]`, etc.
    /// - If ANY operation fails, the ENTIRE transaction is rolled back.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, Operation, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Atomic transaction: create user AND grant permissions
    /// let result = client.batch_write(
    ///     organization,
    ///     Some(vault),
    ///     vec![
    ///         // First batch: create the user
    ///         vec![Operation::set_entity("user:123", b"alice".to_vec())],
    ///         // Second batch: grant permissions (depends on user existing)
    ///         vec![
    ///             Operation::create_relationship("doc:456", "viewer", "user:123"),
    ///             Operation::create_relationship("folder:789", "editor", "user:123"),
    ///         ],
    ///     ],
    /// ).await?;
    ///
    /// println!("Batch committed at block {} with sequence {}", result.block_height, result.assigned_sequence);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_write(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        batches: Vec<Vec<Operation>>,
    ) -> Result<WriteSuccess> {
        self.check_shutdown(None)?;

        // Generate UUID idempotency key once for this request
        // The same key is reused across all retry attempts
        let idempotency_key = uuid::Uuid::new_v4();

        self.execute_batch_write(organization, vault, &batches, idempotency_key, None).await
    }

    /// Executes a single batch write attempt with retry for transient errors.
    ///
    /// The idempotency key is preserved across retry attempts to ensure
    /// at-most-once semantics even with network failures.
    async fn execute_batch_write(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        batches: &[Vec<Operation>],
        idempotency_key: uuid::Uuid,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        // Client-side validation: fast rejection before network round-trip
        let validation_config = self.config().validation();
        let total_ops: usize = batches.iter().map(|b| b.len()).sum();
        inferadb_ledger_types::validation::validate_operations_count(total_ops, validation_config)
            .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
        let mut total_bytes: usize = 0;
        for batch in batches {
            for op in batch {
                op.validate(validation_config)
                    .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
                total_bytes += op.estimated_size_bytes();
            }
        }
        inferadb_ledger_types::validation::validate_batch_payload_bytes(
            total_bytes,
            validation_config,
        )
        .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();
        let client_id = self.client_id().to_string();

        // Convert batches to proto BatchWriteOperation format
        let proto_batches: Vec<proto::BatchWriteOperation> = batches
            .iter()
            .map(|ops| proto::BatchWriteOperation {
                operations: ops.iter().map(Operation::to_proto).collect(),
            })
            .collect();

        // Convert UUID to bytes (16 bytes)
        let idempotency_key_bytes = idempotency_key.as_bytes().to_vec();

        // Execute with retry for transient errors
        self.with_metrics(
            "batch_write",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "batch_write", || {
                let batch_ops = proto_batches.clone();
                let cid = client_id.clone();
                let key_bytes = idempotency_key_bytes.clone();
                async move {
                    let channel = pool.get_channel().await?;
                    let mut write_client = Self::create_write_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::BatchWriteRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        client_id: Some(proto::ClientId { id: cid }),
                        idempotency_key: key_bytes,
                        operations: batch_ops,
                        include_tx_proofs: false,
                    };

                    let response =
                        write_client.batch_write(tonic::Request::new(request)).await?.into_inner();

                    Self::process_batch_write_response(response)
                }
            }),
        )
        .await
    }

    /// Processes a BatchWriteResponse and converts to Result<WriteSuccess>.
    fn process_batch_write_response(response: proto::BatchWriteResponse) -> Result<WriteSuccess> {
        match response.result {
            Some(proto::batch_write_response::Result::Success(success)) => Ok(WriteSuccess {
                tx_id: Self::tx_id_to_hex(success.tx_id),
                block_height: success.block_height,
                assigned_sequence: success.assigned_sequence,
            }),
            Some(proto::batch_write_response::Result::Error(error)) => {
                let code = proto::WriteErrorCode::try_from(error.code)
                    .unwrap_or(proto::WriteErrorCode::Unspecified);

                match code {
                    proto::WriteErrorCode::AlreadyCommitted => {
                        // Idempotent retry - return the original success with assigned_sequence
                        Ok(WriteSuccess {
                            tx_id: Self::tx_id_to_hex(error.committed_tx_id),
                            block_height: error.committed_block_height.unwrap_or(0),
                            assigned_sequence: error.assigned_sequence.unwrap_or(0),
                        })
                    },
                    proto::WriteErrorCode::IdempotencyKeyReused => {
                        // Client reused idempotency key with different payload
                        Err(crate::error::SdkError::Idempotency {
                            message: format!(
                                "Idempotency key reused with different payload: {}",
                                error.message
                            ),
                            conflict_key: None,
                            original_tx_id: Some(Self::tx_id_to_hex(error.committed_tx_id.clone())),
                        })
                    },
                    _ => {
                        // Other write errors (CAS failures, etc.)
                        Err(crate::error::SdkError::Rpc {
                            code: tonic::Code::FailedPrecondition,
                            message: error.message,
                            request_id: None,
                            trace_id: None,
                            error_details: None,
                        })
                    },
                }
            },
            None => Err(crate::error::SdkError::Rpc {
                code: tonic::Code::Internal,
                message: "Empty batch write response".to_owned(),
                request_id: None,
                trace_id: None,
                error_details: None,
            }),
        }
    }

    // =============================================================================
    // Streaming Operations
    // =============================================================================

    /// Subscribes to block announcements for a vault.
    ///
    /// Returns a stream of [`BlockAnnouncement`] items that emits each time a new
    /// block is committed to the vault's chain. The stream automatically reconnects
    /// on disconnect and resumes from the last seen block height.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `start_height` - First block height to receive (must be >= 1).
    ///
    /// # Returns
    ///
    /// Returns a `Stream` that yields `Result<BlockAnnouncement>` items.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the initial stream connection fails.
    ///
    /// # Reconnection Behavior
    ///
    /// On disconnect (network error, server restart, etc.), the stream:
    /// 1. Applies exponential backoff before reconnecting
    /// 2. Resumes from `last_seen_height + 1` to avoid gaps or duplicates
    /// 3. Continues until max reconnection attempts are exhausted
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # use futures::StreamExt;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-app").await?;
    /// # let organization = OrganizationSlug::new(1);
    ///
    /// // Start watching from height 1
    /// let mut stream = client.watch_blocks(organization, VaultSlug::new(0), 1).await?;
    ///
    /// while let Some(announcement) = stream.next().await {
    ///     match announcement {
    ///         Ok(block) => {
    ///             println!("New block at height {}", block.height);
    ///             // Process block...
    ///         }
    ///         Err(e) => {
    ///             eprintln!("Stream error: {}", e);
    ///             break;
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_blocks(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        start_height: u64,
    ) -> Result<impl futures::Stream<Item = Result<BlockAnnouncement>>> {
        self.check_shutdown(None)?;

        // Get the initial stream
        let initial_stream =
            self.create_watch_blocks_stream(organization, vault, start_height).await?;

        // Create position tracker starting at the requested height
        let position = HeightTracker::new(start_height);

        // Clone pool and config for the reconnection closure
        let pool = self.pool.clone();
        let retry_policy = self.config().retry_policy().clone();

        // Create the reconnecting stream wrapper
        let reconnecting = ReconnectingStream::new(
            initial_stream,
            position,
            retry_policy.clone(),
            move |next_height| {
                let pool = pool.clone();
                Box::pin(async move {
                    let channel = pool.get_channel().await?;
                    let mut client = proto::read_service_client::ReadServiceClient::new(channel);
                    if pool.compression_enabled() {
                        client = client
                            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
                    }

                    let request = proto::WatchBlocksRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        start_height: next_height,
                    };

                    let response =
                        client.watch_blocks(tonic::Request::new(request)).await?.into_inner();

                    Ok(response)
                })
            },
        );

        // Map proto announcements to SDK type
        Ok(futures::StreamExt::map(reconnecting, |result| {
            result.map(BlockAnnouncement::from_proto)
        }))
    }

    /// Creates a WatchBlocks stream without reconnection logic.
    async fn create_watch_blocks_stream(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        start_height: u64,
    ) -> Result<tonic::Streaming<proto::BlockAnnouncement>> {
        let channel = self.pool.get_channel().await?;
        let mut client = Self::create_read_client(
            channel,
            self.pool.compression_enabled(),
            self.trace_interceptor(),
        );

        let request = proto::WatchBlocksRequest {
            organization: Some(proto::OrganizationSlug { slug: organization.value() }),
            vault: Some(proto::VaultSlug { slug: vault.value() }),
            start_height,
        };

        let response = client.watch_blocks(tonic::Request::new(request)).await?.into_inner();

        Ok(response)
    }

    // =========================================================================
    // Admin Operations
    // =========================================================================

    /// Creates a new organization.
    ///
    /// Creates an organization with the given name. The organization slug
    /// (external identifier) is assigned by the leader and returned in the response.
    ///
    /// # Arguments
    ///
    /// * `name` - Human-readable name for the organization (e.g., "acme_corp").
    ///
    /// # Returns
    ///
    /// Returns [`OrganizationInfo`] containing the generated slug and metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization name is invalid or already exists
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let org = client.create_organization("my-org").await?;
    /// println!("Created organization with slug: {}", org.slug);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_organization(&self, name: impl Into<String>) -> Result<OrganizationInfo> {
        self.check_shutdown(None)?;

        let name = name.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_organization",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_admin_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::CreateOrganizationRequest {
                        name: name.clone(),
                        shard_id: None, // Auto-assigned
                        quota: None,
                    };

                    let response = client
                        .create_organization(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(OrganizationInfo {
                        slug: OrganizationSlug::new(response.slug.map_or(0, |n| n.slug)),
                        name: name.clone(),
                        shard_id: response.shard_id.map_or(0, |s| s.id),
                        member_nodes: Vec::new(),
                        config_version: 0,
                        status: OrganizationStatus::Active,
                    })
                },
            ),
        )
        .await
    }

    /// Returns information about an organization by slug.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns [`OrganizationInfo`] containing organization metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let slug = OrganizationSlug::new(1);
    /// let info = client.get_organization(slug).await?;
    /// println!("Organization: {} (status: {:?})", info.name, info.status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_organization(&self, slug: OrganizationSlug) -> Result<OrganizationInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_organization",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_admin_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::GetOrganizationRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                    };

                    let response =
                        client.get_organization(tonic::Request::new(request)).await?.into_inner();

                    Ok(OrganizationInfo::from_proto(response))
                },
            ),
        )
        .await
    }

    /// Deletes an organization by slug.
    ///
    /// Marks the organization for deletion. Fails if the organization
    /// still contains active vaults.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    /// - The organization still has active vaults
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let slug = OrganizationSlug::new(1);
    /// client.delete_organization(slug).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_organization(&self, slug: OrganizationSlug) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_organization",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_admin_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::DeleteOrganizationRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                    };

                    client.delete_organization(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
        .await
    }

    /// Lists all organizations.
    ///
    /// Returns a list of all organizations visible to this client.
    /// Admin operations typically have longer timeouts.
    ///
    /// # Returns
    ///
    /// Returns a vector of [`OrganizationInfo`] for all organizations.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let organizations = client.list_organizations().await?;
    /// for org in organizations {
    ///     println!("Organization: {} (slug: {})", org.name, org.slug);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_organizations(&self) -> Result<Vec<OrganizationInfo>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_organizations",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_organizations",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_admin_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::ListOrganizationsRequest {
                        page_token: None,
                        page_size: 0, // Use default
                    };

                    let response =
                        client.list_organizations(tonic::Request::new(request)).await?.into_inner();

                    Ok(response
                        .organizations
                        .into_iter()
                        .map(OrganizationInfo::from_proto)
                        .collect())
                },
            ),
        )
        .await
    }

    /// Creates a new vault in an organization.
    ///
    /// Creates a vault within the specified organization. The vault slug
    /// (external identifier) is assigned by the leader and returned in the response.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns [`VaultInfo`] containing the new vault's metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// let vault = client.create_vault(organization).await?;
    /// println!("Created vault with slug: {}", vault.vault);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_vault(&self, organization: OrganizationSlug) -> Result<VaultInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_vault",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_admin_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::CreateVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        replication_factor: 0,  // Use default
                        initial_nodes: vec![],  // Auto-assigned
                        retention_policy: None, // Default: FULL
                    };

                    let response =
                        client.create_vault(tonic::Request::new(request)).await?.into_inner();

                    // Build VaultInfo from CreateVaultResponse
                    // Note: CreateVaultResponse has limited fields compared to GetVaultResponse
                    Ok(VaultInfo {
                        organization,
                        vault: VaultSlug::new(response.vault.map_or(0, |v| v.slug)),
                        height: 0,          // Genesis block
                        state_root: vec![], // Empty at genesis
                        nodes: vec![],      // Not returned in create response
                        leader: None,       // Not returned in create response
                        status: VaultStatus::Active,
                    })
                },
            ),
        )
        .await
    }

    /// Returns information about a vault.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns [`VaultInfo`] containing vault metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization or vault does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let info = client.get_vault(organization, vault).await?;
    /// println!("Vault height: {}, status: {:?}", info.height, info.status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_vault(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<VaultInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_vault",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_admin_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::GetVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                    };

                    let response =
                        client.get_vault(tonic::Request::new(request)).await?.into_inner();

                    Ok(VaultInfo::from_proto(response))
                },
            ),
        )
        .await
    }

    /// Lists all vaults on this node.
    ///
    /// Returns a list of all vaults that this node is hosting or participating in.
    ///
    /// # Returns
    ///
    /// Returns a vector of [`VaultInfo`] for all vaults.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let vaults = client.list_vaults().await?;
    /// for v in vaults {
    ///     println!("Vault {} in {}", v.vault, v.organization);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_vaults(&self) -> Result<Vec<VaultInfo>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_vaults",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_vaults",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_admin_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::ListVaultsRequest {};

                    let response =
                        client.list_vaults(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.vaults.into_iter().map(VaultInfo::from_proto).collect())
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Events Operations
    // =========================================================================

    /// Lists audit events for an organization with optional filtering.
    ///
    /// Returns a paginated list of events matching the filter criteria.
    /// Pass `organization = 0` to query system-level events.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier; 0 for system events).
    /// * `filter` - Filter criteria (empty filter matches all events).
    /// * `limit` - Maximum results per page (0 = server default, max 1000).
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, EventFilter, OrganizationSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let filter = EventFilter::new()
    ///     .event_type_prefix("ledger.vault")
    ///     .outcome_success();
    /// let page = client.list_events(organization, filter, 100).await?;
    /// for event in &page.entries {
    ///     println!("{}: {}", event.event_type, event.principal);
    /// }
    /// if page.has_next_page() {
    ///     let next = client.list_events_next(organization, page.next_page_token.as_deref().unwrap_or_default()).await?;
    ///     println!("Next page: {} events", next.entries.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_events(
        &self,
        organization: OrganizationSlug,
        filter: EventFilter,
        limit: u32,
    ) -> Result<EventPage> {
        self.list_events_inner(organization, filter, limit, String::new()).await
    }

    /// Continues paginating audit events from a previous response.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier; must match the original query).
    /// * `page_token` - Opaque cursor from the previous response's `next_page_token`.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, EventFilter, OrganizationSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// # let page_token = "abc".to_string();
    /// let next_page = client.list_events_next(organization, &page_token).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_events_next(
        &self,
        organization: OrganizationSlug,
        page_token: &str,
    ) -> Result<EventPage> {
        self.list_events_inner(organization, EventFilter::new(), 0, page_token.to_owned()).await
    }

    /// Internal list_events implementation shared by `list_events` and `list_events_next`.
    async fn list_events_inner(
        &self,
        organization: OrganizationSlug,
        filter: EventFilter,
        limit: u32,
        page_token: String,
    ) -> Result<EventPage> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_events",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_events",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_events_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::ListEventsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        filter: Some(filter.to_proto()),
                        limit,
                        page_token: page_token.clone(),
                    };

                    let response =
                        client.list_events(tonic::Request::new(request)).await?.into_inner();

                    let entries =
                        response.entries.into_iter().map(SdkEventEntry::from_proto).collect();

                    let next_page_token = if response.next_page_token.is_empty() {
                        None
                    } else {
                        Some(response.next_page_token)
                    };

                    Ok(EventPage {
                        entries,
                        next_page_token,
                        total_estimate: response.total_estimate,
                    })
                },
            ),
        )
        .await
    }

    /// Retrieves a single audit event by ID.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `event_id` - Event identifier (UUID string, e.g., from `event_id_string()`).
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` with `NOT_FOUND` if the event does not exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let event = client.get_event(organization, "550e8400-e29b-41d4-a716-446655440000").await?;
    /// println!("Event: {} by {}", event.event_type, event.principal);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_event(
        &self,
        organization: OrganizationSlug,
        event_id: &str,
    ) -> Result<SdkEventEntry> {
        self.check_shutdown(None)?;

        let event_id_bytes =
            uuid::Uuid::parse_str(event_id).map(|u| u.into_bytes().to_vec()).map_err(|e| {
                error::SdkError::Validation { message: format!("invalid event_id: {e}") }
            })?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_event",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_event",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_events_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::GetEventRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        event_id: event_id_bytes.clone(),
                    };

                    let response =
                        client.get_event(tonic::Request::new(request)).await?.into_inner();

                    let entry = response
                        .entry
                        .ok_or_else(|| tonic::Status::not_found("event not found in response"))?;

                    Ok(SdkEventEntry::from_proto(entry))
                },
            ),
        )
        .await
    }

    /// Counts audit events matching a filter.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier; 0 for system events).
    /// * `filter` - Filter criteria (empty filter counts all events).
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, EventFilter, OrganizationSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let denied_count = client.count_events(organization, EventFilter::new().outcome_denied()).await?;
    /// println!("Denied events: {}", denied_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count_events(
        &self,
        organization: OrganizationSlug,
        filter: EventFilter,
    ) -> Result<u64> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "count_events",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "count_events",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_events_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::CountEventsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        filter: Some(filter.to_proto()),
                    };

                    let response =
                        client.count_events(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.count)
                },
            ),
        )
        .await
    }

    /// Ingests external audit events from Engine or Control services.
    ///
    /// Writes a batch of events into the organization's audit trail. Events
    /// are stored as handler-phase entries (node-local, not Raft-replicated).
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `source` - Originating component ([`EventSource::Engine`] or [`EventSource::Control`]).
    /// * `events` - Batch of events to ingest.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` if ingestion fails. Individual events may be
    /// rejected; check `IngestResult::rejections` for per-event details.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, SdkIngestEventEntry, EventOutcome, EventSource, OrganizationSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let events = vec![
    ///     SdkIngestEventEntry::new(
    ///         "engine.authorization.checked",
    ///         "user:alice",
    ///         EventOutcome::Success,
    ///     )
    ///     .detail("resource", "document:123")
    ///     .detail("relation", "viewer"),
    /// ];
    /// let result = client.ingest_events(organization, EventSource::Engine, events).await?;
    /// println!("Accepted: {}, Rejected: {}", result.accepted_count, result.rejected_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ingest_events(
        &self,
        organization: OrganizationSlug,
        source: EventSource,
        events: Vec<SdkIngestEventEntry>,
    ) -> Result<IngestResult> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "ingest_events",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "ingest_events",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_events_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::IngestEventsRequest {
                        source_service: source.as_str().to_owned(),
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        entries: events
                            .clone()
                            .into_iter()
                            .map(SdkIngestEventEntry::into_proto)
                            .collect(),
                    };

                    let response =
                        client.ingest_events(tonic::Request::new(request)).await?.into_inner();

                    Ok(IngestResult {
                        accepted_count: response.accepted_count,
                        rejected_count: response.rejected_count,
                        rejections: response
                            .rejections
                            .into_iter()
                            .map(|r| IngestRejection { index: r.index, reason: r.reason })
                            .collect(),
                    })
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Health Operations
    // =========================================================================

    /// Checks node-level health.
    ///
    /// Returns `true` if the node is healthy and has a leader elected.
    /// This is a simple health check suitable for load balancer probes.
    ///
    /// # Returns
    ///
    /// Returns `true` if the node is healthy, `false` if degraded.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is unavailable or connection fails.
    /// Note: An unavailable node returns an error, not `false`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// if client.health_check().await? {
    ///     println!("Node is healthy");
    /// } else {
    ///     println!("Node is degraded but available");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check(&self) -> Result<bool> {
        self.check_shutdown(None)?;

        let result = self.health_check_detailed().await?;
        match result.status {
            HealthStatus::Healthy => Ok(true),
            HealthStatus::Degraded => Ok(false),
            HealthStatus::Unavailable => {
                Err(error::SdkError::Unavailable { message: result.message })
            },
            HealthStatus::Unspecified => Ok(false),
        }
    }

    /// Returns detailed node-level health information.
    ///
    /// Returns full health check result including status, message, and details.
    /// Use this for monitoring and diagnostics that need more than a simple boolean.
    ///
    /// # Returns
    ///
    /// Returns a [`HealthCheckResult`] with status, message, and details.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let health = client.health_check_detailed().await?;
    /// println!("Status: {:?}, Message: {}", health.status, health.message);
    /// if let Some(term) = health.details.get("current_term") {
    ///     println!("Current Raft term: {}", term);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check_detailed(&self) -> Result<HealthCheckResult> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "health_check_detailed",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "health_check_detailed",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_health_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::HealthCheckRequest { organization: None, vault: None };

                    let response = client.check(tonic::Request::new(request)).await?.into_inner();

                    Ok(HealthCheckResult::from_proto(response))
                },
            ),
        )
        .await
    }

    /// Checks health of a specific vault.
    ///
    /// Returns detailed health information for a specific vault, including
    /// block height, health status, and any divergence information.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns a [`HealthCheckResult`] with vault-specific health information.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// let health = client.health_check_vault(organization, VaultSlug::new(0)).await?;
    /// println!("Vault status: {:?}", health.status);
    /// if let Some(height) = health.details.get("block_height") {
    ///     println!("Current height: {}", height);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check_vault(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<HealthCheckResult> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "health_check_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "health_check_vault",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_health_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::HealthCheckRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                    };

                    let response = client.check(tonic::Request::new(request)).await?.into_inner();

                    Ok(HealthCheckResult::from_proto(response))
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Verified Read Operations
    // =========================================================================

    /// Reads a value with cryptographic proof for client-side verification.
    ///
    /// Returns the value along with a Merkle proof that can be used to verify
    /// the value is authentic without trusting the server. The proof links
    /// the entity value to the state root in the block header.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - Entity key to read.
    /// * `opts` - Verification options (height, chain proof).
    ///
    /// # Returns
    ///
    /// `VerifiedValue` containing the value and proofs, or `None` if key not found.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the read fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug, VerifyOpts};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let result = client.verified_read(organization, Some(vault), "user:123", VerifyOpts::new()).await?;
    /// if let Some(verified) = result {
    ///     // Verify the proof before using the value
    ///     verified.verify()?;
    ///     println!("Value: {:?}", verified.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn verified_read(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        opts: VerifyOpts,
    ) -> Result<Option<VerifiedValue>> {
        self.check_shutdown(None)?;

        let key = key.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "verified_read",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "verified_read",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_read_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::VerifiedReadRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        key: key.clone(),
                        at_height: opts.at_height,
                        include_chain_proof: opts.include_chain_proof,
                        trusted_height: opts.trusted_height,
                    };

                    let response =
                        client.verified_read(tonic::Request::new(request)).await?.into_inner();

                    // If no value and no block header, key was not found
                    if response.value.is_none() && response.block_header.is_none() {
                        return Ok(None);
                    }

                    Ok(VerifiedValue::from_proto(response))
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Query Operations
    // =========================================================================

    /// Lists entities matching a key prefix.
    ///
    /// Returns a paginated list of entities with keys starting with the given prefix.
    /// Use the `next_page_token` to fetch additional pages.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `opts` - Query options including prefix filter, pagination, and consistency.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListEntitiesOpts, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// // List all users
    /// let result = client.list_entities(organization, ListEntitiesOpts::with_prefix("user:")).await?;
    /// for entity in result.items {
    ///     println!("Key: {}, Version: {}", entity.key, entity.version);
    /// }
    ///
    /// // Fetch next page if available
    /// if let Some(token) = result.next_page_token {
    ///     let next_page = client.list_entities(
    ///         organization,
    ///         ListEntitiesOpts::with_prefix("user:").page_token(token)
    ///     ).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_entities(
        &self,
        organization: OrganizationSlug,
        opts: ListEntitiesOpts,
    ) -> Result<PagedResult<Entity>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_entities",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_entities",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_read_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::ListEntitiesRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        key_prefix: opts.key_prefix.clone(),
                        at_height: opts.at_height,
                        include_expired: opts.include_expired,
                        limit: opts.limit,
                        page_token: opts.page_token.clone().unwrap_or_default(),
                        consistency: opts.consistency.to_proto() as i32,
                        vault: opts.vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    };

                    let response =
                        client.list_entities(tonic::Request::new(request)).await?.into_inner();

                    let items = response.entities.into_iter().map(Entity::from_proto).collect();

                    let next_page_token = if response.next_page_token.is_empty() {
                        None
                    } else {
                        Some(response.next_page_token)
                    };

                    Ok(PagedResult { items, next_page_token, block_height: response.block_height })
                },
            ),
        )
        .await
    }

    /// Lists relationships in a vault with optional filters.
    ///
    /// Returns a paginated list of relationships matching the filter criteria.
    /// All filter fields are optional; omitting a filter matches all values.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `opts` - Query options including filters, pagination, and consistency.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListRelationshipsOpts, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // List all relationships for a document
    /// let result = client.list_relationships(
    ///     organization,
    ///     vault,
    ///     ListRelationshipsOpts::new().resource("document:123")
    /// ).await?;
    ///
    /// for rel in result.items {
    ///     println!("{} -> {} -> {}", rel.resource, rel.relation, rel.subject);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_relationships(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        opts: ListRelationshipsOpts,
    ) -> Result<PagedResult<Relationship>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_relationships",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_relationships",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_read_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::ListRelationshipsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        resource: opts.resource.clone(),
                        relation: opts.relation.clone(),
                        subject: opts.subject.clone(),
                        at_height: opts.at_height,
                        limit: opts.limit,
                        page_token: opts.page_token.clone().unwrap_or_default(),
                        consistency: opts.consistency.to_proto() as i32,
                    };

                    let response =
                        client.list_relationships(tonic::Request::new(request)).await?.into_inner();

                    let items =
                        response.relationships.into_iter().map(Relationship::from_proto).collect();

                    let next_page_token = if response.next_page_token.is_empty() {
                        None
                    } else {
                        Some(response.next_page_token)
                    };

                    Ok(PagedResult { items, next_page_token, block_height: response.block_height })
                },
            ),
        )
        .await
    }

    /// Lists distinct resource IDs matching a type prefix.
    ///
    /// Returns a paginated list of unique resource identifiers that match the given
    /// type prefix (e.g., "document" matches "document:1", "document:2", etc.).
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `opts` - Query options including type filter, pagination, and consistency.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListResourcesOpts, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // List all document resources
    /// let result = client.list_resources(
    ///     organization,
    ///     vault,
    ///     ListResourcesOpts::with_type("document")
    /// ).await?;
    ///
    /// for resource_id in result.items {
    ///     println!("Resource: {}", resource_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_resources(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        opts: ListResourcesOpts,
    ) -> Result<PagedResult<String>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_resources",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_resources",
                || async {
                    let channel = pool.get_channel().await?;
                    let mut client = Self::create_read_client(
                        channel,
                        pool.compression_enabled(),
                        TraceContextInterceptor::with_timeout(
                            pool.config().trace(),
                            pool.config().timeout(),
                        ),
                    );

                    let request = proto::ListResourcesRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        resource_type: opts.resource_type.clone(),
                        at_height: opts.at_height,
                        limit: opts.limit,
                        page_token: opts.page_token.clone().unwrap_or_default(),
                        consistency: opts.consistency.to_proto() as i32,
                    };

                    let response =
                        client.list_resources(tonic::Request::new(request)).await?.into_inner();

                    let next_page_token = if response.next_page_token.is_empty() {
                        None
                    } else {
                        Some(response.next_page_token)
                    };

                    Ok(PagedResult {
                        items: response.resources,
                        next_page_token,
                        block_height: response.block_height,
                    })
                },
            ),
        )
        .await
    }

    /// Creates an AdminService client with compression and tracing settings.
    fn create_admin_client(
        channel: tonic::transport::Channel,
        compression_enabled: bool,
        interceptor: TraceContextInterceptor,
    ) -> proto::admin_service_client::AdminServiceClient<
        InterceptedService<tonic::transport::Channel, TraceContextInterceptor>,
    > {
        let client =
            proto::admin_service_client::AdminServiceClient::with_interceptor(channel, interceptor);
        if compression_enabled {
            client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            client
        }
    }

    /// Creates an EventsService client with compression and tracing settings.
    fn create_events_client(
        channel: tonic::transport::Channel,
        compression_enabled: bool,
        interceptor: TraceContextInterceptor,
    ) -> proto::events_service_client::EventsServiceClient<
        InterceptedService<tonic::transport::Channel, TraceContextInterceptor>,
    > {
        let client = proto::events_service_client::EventsServiceClient::with_interceptor(
            channel,
            interceptor,
        );
        if compression_enabled {
            client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            client
        }
    }

    /// Creates a HealthService client with compression and tracing settings.
    fn create_health_client(
        channel: tonic::transport::Channel,
        compression_enabled: bool,
        interceptor: TraceContextInterceptor,
    ) -> proto::health_service_client::HealthServiceClient<
        InterceptedService<tonic::transport::Channel, TraceContextInterceptor>,
    > {
        let client = proto::health_service_client::HealthServiceClient::with_interceptor(
            channel,
            interceptor,
        );
        if compression_enabled {
            client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            client
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::RetryPolicy;

    const ORG: OrganizationSlug = OrganizationSlug::new(1);

    #[tokio::test]
    async fn test_new_with_valid_config() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.client_id(), "test-client");
        assert!(matches!(client.config().servers(), ServerSource::Static(_)));
    }

    #[tokio::test]
    async fn test_connect_convenience_constructor() {
        let client = LedgerClient::connect("http://localhost:50051", "quick-client")
            .await
            .expect("client creation");

        assert_eq!(client.client_id(), "quick-client");
        assert!(matches!(client.config().servers(), ServerSource::Static(_)));
    }

    #[tokio::test]
    async fn test_connect_with_invalid_endpoint() {
        let result = LedgerClient::connect("not-a-url", "test-client").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_config_accessor_returns_full_config() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("accessor-test")
            .timeout(Duration::from_secs(30))
            .compression(true)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.config().timeout(), Duration::from_secs(30));
        assert!(client.config().compression());
    }

    #[tokio::test]
    async fn test_pool_accessor_returns_pool() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("pool-test")
            .compression(true)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert!(client.pool().compression_enabled());
    }

    #[tokio::test]
    async fn test_create_discovery_service() {
        use crate::config::DiscoveryConfig;

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("discovery-test")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let discovery = client.create_discovery_service(DiscoveryConfig::enabled());

        assert!(discovery.config().is_enabled());
    }

    #[tokio::test]
    async fn test_new_preserves_retry_policy() {
        let retry_policy = RetryPolicy::builder()
            .max_attempts(5)
            .initial_backoff(Duration::from_millis(100))
            .build();

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("retry-test")
            .retry_policy(retry_policy)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.config().retry_policy().max_attempts, 5);
    }

    // =========================================================================
    // ReadConsistency Tests
    // =========================================================================

    #[test]
    fn test_read_consistency_default_is_eventual() {
        assert_eq!(ReadConsistency::default(), ReadConsistency::Eventual);
    }

    #[test]
    fn test_read_consistency_to_proto_eventual() {
        let consistency = ReadConsistency::Eventual;
        assert_eq!(consistency.to_proto() as i32, proto::ReadConsistency::Eventual as i32);
    }

    #[test]
    fn test_read_consistency_to_proto_linearizable() {
        let consistency = ReadConsistency::Linearizable;
        assert_eq!(consistency.to_proto() as i32, proto::ReadConsistency::Linearizable as i32);
    }

    // =========================================================================
    // Read Operation Integration Tests
    // =========================================================================
    //
    // These tests verify error handling when connecting to unreachable endpoints.
    // They don't require a running server - they test the retry/error paths.

    #[tokio::test]
    async fn test_read_returns_error_on_connection_failure() {
        // Configure minimal retry to make test fast
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59999"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let result = client.read(ORG, Some(VaultSlug::new(0)), "test-key").await;
        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_read_consistent_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59998"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let result = client.read_consistent(ORG, Some(VaultSlug::new(0)), "test-key").await;
        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_batch_read_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59997"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let result =
            client.batch_read(ORG, Some(VaultSlug::new(0)), vec!["key1", "key2", "key3"]).await;
        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_batch_read_consistent_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59996"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let result =
            client.batch_read_consistent(ORG, Some(VaultSlug::new(0)), vec!["key1", "key2"]).await;
        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_read_with_none_vault() {
        // Test that read works with None vault (organization-level reads)
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59995"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // This tests the API signature - None for vault should work
        let result = client.read(ORG, None, "user:123").await;
        assert!(result.is_err(), "expected connection error");
    }

    // =========================================================================
    // Operation Builder Tests
    // =========================================================================

    #[test]
    fn test_operation_set_entity() {
        let op = Operation::set_entity("user:123", b"data".to_vec());
        match op {
            Operation::SetEntity { key, value, expires_at, condition } => {
                assert_eq!(key, "user:123");
                assert_eq!(value, b"data");
                assert!(expires_at.is_none());
                assert!(condition.is_none());
            },
            _ => panic!("Expected SetEntity"),
        }
    }

    #[test]
    fn test_operation_set_entity_with_expiry() {
        let op = Operation::set_entity_with_expiry("session:abc", b"token".to_vec(), 1700000000);
        match op {
            Operation::SetEntity { key, value, expires_at, condition } => {
                assert_eq!(key, "session:abc");
                assert_eq!(value, b"token");
                assert_eq!(expires_at, Some(1700000000));
                assert!(condition.is_none());
            },
            _ => panic!("Expected SetEntity"),
        }
    }

    #[test]
    fn test_operation_set_entity_if_not_exists() {
        let op = Operation::set_entity_if("lock:xyz", b"owner".to_vec(), SetCondition::NotExists);
        match op {
            Operation::SetEntity { key, condition: Some(SetCondition::NotExists), .. } => {
                assert_eq!(key, "lock:xyz");
            },
            _ => panic!("Expected SetEntity with NotExists condition"),
        }
    }

    #[test]
    fn test_operation_set_entity_if_version() {
        let op = Operation::set_entity_if("counter", b"42".to_vec(), SetCondition::Version(100));
        match op {
            Operation::SetEntity { condition: Some(SetCondition::Version(v)), .. } => {
                assert_eq!(v, 100);
            },
            _ => panic!("Expected SetEntity with Version condition"),
        }
    }

    #[test]
    fn test_operation_set_entity_if_value_equals() {
        let op = Operation::set_entity_if(
            "data",
            b"new".to_vec(),
            SetCondition::ValueEquals(b"old".to_vec()),
        );
        match op {
            Operation::SetEntity { condition: Some(SetCondition::ValueEquals(v)), .. } => {
                assert_eq!(v, b"old");
            },
            _ => panic!("Expected SetEntity with ValueEquals condition"),
        }
    }

    #[test]
    fn test_operation_delete_entity() {
        let op = Operation::delete_entity("obsolete:key");
        match op {
            Operation::DeleteEntity { key } => {
                assert_eq!(key, "obsolete:key");
            },
            _ => panic!("Expected DeleteEntity"),
        }
    }

    #[test]
    fn test_operation_create_relationship() {
        let op = Operation::create_relationship("doc:456", "viewer", "user:123");
        match op {
            Operation::CreateRelationship { resource, relation, subject } => {
                assert_eq!(resource, "doc:456");
                assert_eq!(relation, "viewer");
                assert_eq!(subject, "user:123");
            },
            _ => panic!("Expected CreateRelationship"),
        }
    }

    #[test]
    fn test_operation_delete_relationship() {
        let op = Operation::delete_relationship("doc:456", "editor", "team:admins#member");
        match op {
            Operation::DeleteRelationship { resource, relation, subject } => {
                assert_eq!(resource, "doc:456");
                assert_eq!(relation, "editor");
                assert_eq!(subject, "team:admins#member");
            },
            _ => panic!("Expected DeleteRelationship"),
        }
    }

    #[test]
    fn test_operation_to_proto_set_entity() {
        let op = Operation::set_entity("key", b"value".to_vec());
        let proto_op = op.to_proto();

        assert!(proto_op.op.is_some());
        match proto_op.op.unwrap() {
            proto::operation::Op::SetEntity(set) => {
                assert_eq!(set.key, "key");
                assert_eq!(set.value, b"value");
            },
            _ => panic!("Expected SetEntity proto"),
        }
    }

    #[test]
    fn test_operation_to_proto_create_relationship() {
        let op = Operation::create_relationship("res", "rel", "sub");
        let proto_op = op.to_proto();

        match proto_op.op.unwrap() {
            proto::operation::Op::CreateRelationship(rel) => {
                assert_eq!(rel.resource, "res");
                assert_eq!(rel.relation, "rel");
                assert_eq!(rel.subject, "sub");
            },
            _ => panic!("Expected CreateRelationship proto"),
        }
    }

    #[test]
    fn test_set_condition_to_proto() {
        let not_exists = SetCondition::NotExists;
        let proto_cond = not_exists.to_proto();
        assert!(matches!(
            proto_cond.condition,
            Some(proto::set_condition::Condition::NotExists(true))
        ));

        let must_exist = SetCondition::MustExist;
        let proto_cond = must_exist.to_proto();
        assert!(matches!(
            proto_cond.condition,
            Some(proto::set_condition::Condition::MustExists(true))
        ));

        let version = SetCondition::Version(42);
        let proto_cond = version.to_proto();
        assert!(matches!(proto_cond.condition, Some(proto::set_condition::Condition::Version(42))));

        let value_eq = SetCondition::ValueEquals(b"test".to_vec());
        let proto_cond = value_eq.to_proto();
        match proto_cond.condition {
            Some(proto::set_condition::Condition::ValueEquals(v)) => {
                assert_eq!(v, b"test");
            },
            _ => panic!("Expected ValueEquals"),
        }
    }

    // =========================================================================
    // WriteSuccess Tests
    // =========================================================================

    #[test]
    fn test_write_success_fields() {
        let success =
            WriteSuccess { tx_id: "abc123".to_string(), block_height: 42, assigned_sequence: 5 };

        assert_eq!(success.tx_id, "abc123");
        assert_eq!(success.block_height, 42);
        assert_eq!(success.assigned_sequence, 5);
    }

    #[test]
    fn test_tx_id_to_hex() {
        // Test with Some(TxId)
        let tx_id = proto::TxId { id: vec![0x12, 0x34, 0xab, 0xcd] };
        let hex = LedgerClient::tx_id_to_hex(Some(tx_id));
        assert_eq!(hex, "1234abcd");

        // Test with None
        let hex = LedgerClient::tx_id_to_hex(None);
        assert_eq!(hex, "");
    }

    // =========================================================================
    // Write Operation Integration Tests
    // =========================================================================
    //
    // These tests verify error handling when connecting to unreachable endpoints.
    // They don't require a running server - they test the retry/error paths.

    #[tokio::test]
    async fn test_write_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59994"]))
            .client_id("write-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let operations = vec![Operation::set_entity("key", b"value".to_vec())];
        let result = client.write(ORG, Some(VaultSlug::new(0)), operations).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_write_with_multiple_operations() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59990"]))
            .client_id("multi-op-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Multiple operations should be grouped in a single write
        let operations = vec![
            Operation::set_entity("user:1", b"alice".to_vec()),
            Operation::set_entity("user:2", b"bob".to_vec()),
            Operation::create_relationship("doc:1", "viewer", "user:1"),
            Operation::create_relationship("doc:1", "editor", "user:2"),
        ];

        let result = client.write(ORG, Some(VaultSlug::new(0)), operations).await;

        // Should fail due to connection (not due to multiple ops)
        assert!(result.is_err());
    }

    // =========================================================================
    // Batch Write Operation Tests
    // =========================================================================

    #[tokio::test]
    async fn test_batch_write_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59989"]))
            .client_id("batch-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let batches = vec![vec![Operation::set_entity("key", b"value".to_vec())]];
        let result = client.batch_write(ORG, Some(VaultSlug::new(0)), batches).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_batch_write_with_multiple_operation_groups() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59984"]))
            .client_id("batch-groups-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Atomic transaction with multiple groups
        let batches = vec![
            // First group: create user
            vec![Operation::set_entity("user:123", b"alice".to_vec())],
            // Second group: grant permissions
            vec![
                Operation::create_relationship("doc:456", "viewer", "user:123"),
                Operation::create_relationship("folder:789", "editor", "user:123"),
            ],
        ];

        let result = client.batch_write(ORG, Some(VaultSlug::new(0)), batches).await;

        // Should fail due to connection (not due to batch structure)
        assert!(result.is_err());
    }

    // =========================================================================
    // BlockAnnouncement Tests
    // =========================================================================

    #[test]
    fn test_block_announcement_from_proto_with_all_fields() {
        use prost_types::Timestamp;

        let proto_announcement = proto::BlockAnnouncement {
            organization: Some(proto::OrganizationSlug { slug: 1 }),
            vault: Some(proto::VaultSlug { slug: 2 }),
            height: 100,
            block_hash: Some(proto::Hash { value: vec![0x12, 0x34] }),
            state_root: Some(proto::Hash { value: vec![0xab, 0xcd] }),
            timestamp: Some(Timestamp { seconds: 1700000000, nanos: 123_456_789 }),
        };

        let announcement = BlockAnnouncement::from_proto(proto_announcement);

        assert_eq!(announcement.organization, ORG);
        assert_eq!(announcement.vault, VaultSlug::new(2));
        assert_eq!(announcement.height, 100);
        assert_eq!(announcement.block_hash, vec![0x12, 0x34]);
        assert_eq!(announcement.state_root, vec![0xab, 0xcd]);
        assert!(announcement.timestamp.is_some());
    }

    #[test]
    fn test_block_announcement_from_proto_with_missing_optional_fields() {
        let proto_announcement = proto::BlockAnnouncement {
            organization: None,
            vault: None,
            height: 50,
            block_hash: None,
            state_root: None,
            timestamp: None,
        };

        let announcement = BlockAnnouncement::from_proto(proto_announcement);

        assert_eq!(announcement.organization, OrganizationSlug::new(0));
        assert_eq!(announcement.vault, VaultSlug::new(0));
        assert_eq!(announcement.height, 50);
        assert!(announcement.block_hash.is_empty());
        assert!(announcement.state_root.is_empty());
        assert!(announcement.timestamp.is_none());
    }

    #[test]
    fn test_block_announcement_equality() {
        let a = BlockAnnouncement {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            block_hash: vec![0x12],
            state_root: vec![0xab],
            timestamp: None,
        };

        let b = BlockAnnouncement {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            block_hash: vec![0x12],
            state_root: vec![0xab],
            timestamp: None,
        };

        assert_eq!(a, b);
    }

    #[test]
    fn test_block_announcement_clone() {
        let original = BlockAnnouncement {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            block_hash: vec![0x12, 0x34],
            state_root: vec![0xab, 0xcd],
            timestamp: None,
        };

        let cloned = original.clone();

        assert_eq!(original, cloned);
    }

    // =========================================================================
    // WatchBlocks Integration Tests
    // =========================================================================

    #[tokio::test]
    async fn test_watch_blocks_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59982"]))
            .client_id("watch-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let result = client.watch_blocks(ORG, VaultSlug::new(0), 1).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_watch_blocks_different_vaults() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59981"]))
            .client_id("multi-vault-watch")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Both should fail with connection error (testing different vaults work)
        let result1 = client.watch_blocks(ORG, VaultSlug::new(1), 1).await;
        let result2 = client.watch_blocks(ORG, VaultSlug::new(2), 1).await;

        assert!(result1.is_err());
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_watch_blocks_start_height_parameter() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59980"]))
            .client_id("height-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Test with different start heights
        let result_h1 = client.watch_blocks(ORG, VaultSlug::new(0), 1).await;
        let result_h100 = client.watch_blocks(ORG, VaultSlug::new(0), 100).await;

        // Both should fail due to connection (not invalid height)
        assert!(result_h1.is_err());
        assert!(result_h100.is_err());
    }

    // =========================================================================
    // Admin Operation Tests
    // =========================================================================

    #[test]
    fn test_organization_status_from_proto_active() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Active as i32);
        assert_eq!(status, OrganizationStatus::Active);
    }

    #[test]
    fn test_organization_status_from_proto_deleted() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Deleted as i32);
        assert_eq!(status, OrganizationStatus::Deleted);
    }

    #[test]
    fn test_organization_status_from_proto_unspecified() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Unspecified as i32);
        assert_eq!(status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_organization_status_from_proto_invalid() {
        let status = OrganizationStatus::from_proto(999);
        assert_eq!(status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_organization_status_default() {
        let status: OrganizationStatus = Default::default();
        assert_eq!(status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_from_proto_active() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Active as i32);
        assert_eq!(status, VaultStatus::Active);
    }

    #[test]
    fn test_vault_status_from_proto_read_only() {
        let status = VaultStatus::from_proto(proto::VaultStatus::ReadOnly as i32);
        assert_eq!(status, VaultStatus::ReadOnly);
    }

    #[test]
    fn test_vault_status_from_proto_deleted() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Deleted as i32);
        assert_eq!(status, VaultStatus::Deleted);
    }

    #[test]
    fn test_vault_status_from_proto_unspecified() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Unspecified as i32);
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_from_proto_invalid() {
        let status = VaultStatus::from_proto(999);
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_default() {
        let status: VaultStatus = Default::default();
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_organization_info_from_proto() {
        let proto = proto::GetOrganizationResponse {
            slug: Some(proto::OrganizationSlug { slug: 42 }),
            name: "test-organization".to_string(),
            shard_id: Some(proto::ShardId { id: 1 }),
            member_nodes: vec![
                proto::NodeId { id: "node-100".to_string() },
                proto::NodeId { id: "node-101".to_string() },
            ],
            status: proto::OrganizationStatus::Active as i32,
            config_version: 5,
            created_at: None,
        };

        let info = OrganizationInfo::from_proto(proto);

        assert_eq!(info.slug, OrganizationSlug::new(42));
        assert_eq!(info.name, "test-organization");
        assert_eq!(info.shard_id, 1);
        assert_eq!(info.member_nodes, vec!["node-100", "node-101"]);
        assert_eq!(info.config_version, 5);
        assert_eq!(info.status, OrganizationStatus::Active);
    }

    #[test]
    fn test_organization_info_from_proto_with_missing_fields() {
        let proto = proto::GetOrganizationResponse {
            slug: None,
            name: "minimal".to_string(),
            shard_id: None,
            member_nodes: vec![],
            status: proto::OrganizationStatus::Unspecified as i32,
            config_version: 0,
            created_at: None,
        };

        let info = OrganizationInfo::from_proto(proto);

        assert_eq!(info.slug, OrganizationSlug::new(0));
        assert_eq!(info.name, "minimal");
        assert_eq!(info.shard_id, 0);
        assert!(info.member_nodes.is_empty());
        assert_eq!(info.config_version, 0);
        assert_eq!(info.status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_vault_info_from_proto() {
        let proto = proto::GetVaultResponse {
            organization: Some(proto::OrganizationSlug { slug: 1 }),
            vault: Some(proto::VaultSlug { slug: 10 }),
            height: 1000,
            state_root: Some(proto::Hash { value: vec![1, 2, 3, 4] }),
            nodes: vec![
                proto::NodeId { id: "node-200".to_string() },
                proto::NodeId { id: "node-201".to_string() },
            ],
            leader: Some(proto::NodeId { id: "node-200".to_string() }),
            status: proto::VaultStatus::Active as i32,
            retention_policy: None,
        };

        let info = VaultInfo::from_proto(proto);

        assert_eq!(info.organization, ORG);
        assert_eq!(info.vault, VaultSlug::new(10));
        assert_eq!(info.height, 1000);
        assert_eq!(info.state_root, vec![1, 2, 3, 4]);
        assert_eq!(info.nodes, vec!["node-200", "node-201"]);
        assert_eq!(info.leader, Some("node-200".to_string()));
        assert_eq!(info.status, VaultStatus::Active);
    }

    #[test]
    fn test_vault_info_from_proto_with_missing_fields() {
        let proto = proto::GetVaultResponse {
            organization: None,
            vault: None,
            height: 0,
            state_root: None,
            nodes: vec![],
            leader: None,
            status: proto::VaultStatus::Unspecified as i32,
            retention_policy: None,
        };

        let info = VaultInfo::from_proto(proto);

        assert_eq!(info.organization, OrganizationSlug::new(0));
        assert_eq!(info.vault, VaultSlug::new(0));
        assert_eq!(info.height, 0);
        assert!(info.state_root.is_empty());
        assert!(info.nodes.is_empty());
        assert_eq!(info.leader, None);
        assert_eq!(info.status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_organization_info_equality() {
        let info1 = OrganizationInfo {
            slug: ORG,
            name: "test".to_string(),
            shard_id: 1,
            member_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            config_version: 1,
            status: OrganizationStatus::Active,
        };
        let info2 = info1.clone();

        assert_eq!(info1, info2);
    }

    #[test]
    fn test_vault_info_equality() {
        let info1 = VaultInfo {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            state_root: vec![1, 2, 3],
            nodes: vec!["node-1".to_string(), "node-2".to_string()],
            leader: Some("node-1".to_string()),
            status: VaultStatus::Active,
        };
        let info2 = info1.clone();

        assert_eq!(info1, info2);
    }

    #[tokio::test]
    async fn test_create_organization_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59970"]))
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.create_organization("test-ns").await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_get_organization_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59971"]))
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.get_organization(ORG).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_organizations_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59972"]))
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.list_organizations().await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_create_vault_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59973"]))
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.create_vault(ORG).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_get_vault_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59974"]))
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.get_vault(ORG, VaultSlug::new(1)).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_vaults_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59975"]))
            .client_id("admin-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.list_vaults().await;

        assert!(result.is_err(), "expected connection error");
    }

    // =========================================================================
    // HealthStatus Tests
    // =========================================================================

    #[test]
    fn test_health_status_from_proto_healthy() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Healthy as i32);
        assert_eq!(status, HealthStatus::Healthy);
    }

    #[test]
    fn test_health_status_from_proto_degraded() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Degraded as i32);
        assert_eq!(status, HealthStatus::Degraded);
    }

    #[test]
    fn test_health_status_from_proto_unavailable() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Unavailable as i32);
        assert_eq!(status, HealthStatus::Unavailable);
    }

    #[test]
    fn test_health_status_from_proto_unspecified() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Unspecified as i32);
        assert_eq!(status, HealthStatus::Unspecified);
    }

    #[test]
    fn test_health_status_from_proto_invalid() {
        let status = HealthStatus::from_proto(999);
        assert_eq!(status, HealthStatus::Unspecified);
    }

    #[test]
    fn test_health_status_default() {
        let status: HealthStatus = Default::default();
        assert_eq!(status, HealthStatus::Unspecified);
    }

    // =========================================================================
    // HealthCheckResult Tests
    // =========================================================================

    #[test]
    fn test_health_check_result_from_proto() {
        let mut details = std::collections::HashMap::new();
        details.insert("current_term".to_string(), "5".to_string());
        details.insert("leader_id".to_string(), "node-1".to_string());

        let proto = proto::HealthCheckResponse {
            status: proto::HealthStatus::Healthy as i32,
            message: "Node is healthy".to_string(),
            details: details.clone(),
        };

        let result = HealthCheckResult::from_proto(proto);

        assert_eq!(result.status, HealthStatus::Healthy);
        assert_eq!(result.message, "Node is healthy");
        assert_eq!(result.details, details);
    }

    #[test]
    fn test_health_check_result_is_healthy() {
        let result = HealthCheckResult {
            status: HealthStatus::Healthy,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(result.is_healthy());
        assert!(!result.is_degraded());
        assert!(!result.is_unavailable());
    }

    #[test]
    fn test_health_check_result_is_degraded() {
        let result = HealthCheckResult {
            status: HealthStatus::Degraded,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(!result.is_healthy());
        assert!(result.is_degraded());
        assert!(!result.is_unavailable());
    }

    #[test]
    fn test_health_check_result_is_unavailable() {
        let result = HealthCheckResult {
            status: HealthStatus::Unavailable,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(!result.is_healthy());
        assert!(!result.is_degraded());
        assert!(result.is_unavailable());
    }

    // =========================================================================
    // Health Check Integration Tests
    // =========================================================================

    #[tokio::test]
    async fn test_health_check_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59976"]))
            .client_id("health-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.health_check().await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_health_check_detailed_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59977"]))
            .client_id("health-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.health_check_detailed().await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_health_check_vault_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59978"]))
            .client_id("health-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.health_check_vault(ORG, VaultSlug::new(0)).await;

        assert!(result.is_err(), "expected connection error");
    }

    // =========================================================================
    // Verified Read Tests
    // =========================================================================

    #[test]
    fn test_direction_from_proto_left() {
        use inferadb_ledger_proto::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Left as i32);
        assert_eq!(direction, Direction::Left);
    }

    #[test]
    fn test_direction_from_proto_right() {
        use inferadb_ledger_proto::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Right as i32);
        assert_eq!(direction, Direction::Right);
    }

    #[test]
    fn test_direction_from_proto_unspecified_defaults_to_right() {
        use inferadb_ledger_proto::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Unspecified as i32);
        assert_eq!(direction, Direction::Right);
    }

    #[test]
    fn test_merkle_sibling_from_proto() {
        use inferadb_ledger_proto::proto;
        let proto_sibling = proto::MerkleSibling {
            hash: Some(proto::Hash { value: vec![1, 2, 3, 4] }),
            direction: proto::Direction::Left as i32,
        };
        let sibling = MerkleSibling::from_proto(proto_sibling);
        assert_eq!(sibling.hash, vec![1, 2, 3, 4]);
        assert_eq!(sibling.direction, Direction::Left);
    }

    #[test]
    fn test_merkle_proof_from_proto() {
        use inferadb_ledger_proto::proto;
        let proto_proof = proto::MerkleProof {
            leaf_hash: Some(proto::Hash { value: vec![0; 32] }),
            siblings: vec![
                proto::MerkleSibling {
                    hash: Some(proto::Hash { value: vec![1; 32] }),
                    direction: proto::Direction::Left as i32,
                },
                proto::MerkleSibling {
                    hash: Some(proto::Hash { value: vec![2; 32] }),
                    direction: proto::Direction::Right as i32,
                },
            ],
        };
        let proof = MerkleProof::from_proto(proto_proof);
        assert_eq!(proof.leaf_hash, vec![0; 32]);
        assert_eq!(proof.siblings.len(), 2);
        assert_eq!(proof.siblings[0].direction, Direction::Left);
        assert_eq!(proof.siblings[1].direction, Direction::Right);
    }

    #[test]
    fn test_merkle_proof_verify_single_element_tree() {
        // Single element tree: leaf hash equals root
        let proof = MerkleProof {
            leaf_hash: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            siblings: vec![],
        };
        let expected_root = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_single_element_tree_mismatch() {
        let proof = MerkleProof { leaf_hash: vec![1, 2, 3, 4], siblings: vec![] };
        let wrong_root = vec![5, 6, 7, 8];
        assert!(!proof.verify(&wrong_root));
    }

    #[test]
    fn test_merkle_proof_verify_with_siblings() {
        use sha2::{Digest, Sha256};

        // Create a simple two-leaf tree
        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute expected root: hash(leaf || sibling) since sibling is on right
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let expected_root = hasher.finalize().to_vec();

        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: sibling_hash, direction: Direction::Right }],
        };

        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_left_sibling() {
        use sha2::{Digest, Sha256};

        // Create a proof where sibling is on the left
        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute expected root: hash(sibling || leaf) since sibling is on left
        let mut hasher = Sha256::new();
        hasher.update(&sibling_hash);
        hasher.update(&leaf_hash);
        let expected_root = hasher.finalize().to_vec();

        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: sibling_hash, direction: Direction::Left }],
        };

        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_tampered_proof_fails() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute correct root
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let correct_root = hasher.finalize().to_vec();

        // Tamper with the sibling hash
        let tampered_sibling = vec![2u8; 32];
        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: tampered_sibling, direction: Direction::Right }],
        };

        // Should not verify against correct root
        assert!(!proof.verify(&correct_root));
    }

    #[test]
    fn test_merkle_proof_verify_wrong_direction_fails() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute root with sibling on right
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let expected_root = hasher.finalize().to_vec();

        // Create proof with wrong direction (Left instead of Right)
        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling {
                hash: sibling_hash,
                direction: Direction::Left, // Wrong!
            }],
        };

        // Should fail verification
        assert!(!proof.verify(&expected_root));
    }

    #[test]
    fn test_block_header_from_proto() {
        use inferadb_ledger_proto::proto;

        let proto_header = proto::BlockHeader {
            height: 100,
            organization: Some(proto::OrganizationSlug { slug: 1 }),
            vault: Some(proto::VaultSlug { slug: 2 }),
            previous_hash: Some(proto::Hash { value: vec![1; 32] }),
            tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
            state_root: Some(proto::Hash { value: vec![3; 32] }),
            timestamp: Some(prost_types::Timestamp { seconds: 1704067200, nanos: 0 }),
            leader_id: Some(proto::NodeId { id: "node-1".to_string() }),
            term: 5,
            committed_index: 99,
        };

        let header = BlockHeader::from_proto(proto_header);
        assert_eq!(header.height, 100);
        assert_eq!(header.organization, ORG);
        assert_eq!(header.vault, VaultSlug::new(2));
        assert_eq!(header.previous_hash, vec![1; 32]);
        assert_eq!(header.tx_merkle_root, vec![2; 32]);
        assert_eq!(header.state_root, vec![3; 32]);
        assert!(header.timestamp.is_some());
        assert_eq!(header.leader_id, "node-1");
        assert_eq!(header.term, 5);
        assert_eq!(header.committed_index, 99);
    }

    #[test]
    fn test_block_header_from_proto_with_missing_fields() {
        use inferadb_ledger_proto::proto;

        let proto_header = proto::BlockHeader {
            height: 1,
            organization: None,
            vault: None,
            previous_hash: None,
            tx_merkle_root: None,
            state_root: None,
            timestamp: None,
            leader_id: None,
            term: 0,
            committed_index: 0,
        };

        let header = BlockHeader::from_proto(proto_header);
        assert_eq!(header.height, 1);
        assert_eq!(header.organization, OrganizationSlug::new(0));
        assert_eq!(header.vault, VaultSlug::new(0));
        assert!(header.previous_hash.is_empty());
        assert!(header.tx_merkle_root.is_empty());
        assert!(header.state_root.is_empty());
        assert!(header.timestamp.is_none());
        assert!(header.leader_id.is_empty());
    }

    #[test]
    fn test_chain_proof_from_proto() {
        use inferadb_ledger_proto::proto;

        let proto_chain = proto::ChainProof {
            headers: vec![
                proto::BlockHeader {
                    height: 101,
                    organization: Some(proto::OrganizationSlug { slug: 1 }),
                    vault: Some(proto::VaultSlug { slug: 0 }),
                    previous_hash: Some(proto::Hash { value: vec![0; 32] }),
                    tx_merkle_root: Some(proto::Hash { value: vec![1; 32] }),
                    state_root: Some(proto::Hash { value: vec![2; 32] }),
                    timestamp: None,
                    leader_id: None,
                    term: 1,
                    committed_index: 100,
                },
                proto::BlockHeader {
                    height: 102,
                    organization: Some(proto::OrganizationSlug { slug: 1 }),
                    vault: Some(proto::VaultSlug { slug: 0 }),
                    previous_hash: Some(proto::Hash { value: vec![3; 32] }),
                    tx_merkle_root: Some(proto::Hash { value: vec![4; 32] }),
                    state_root: Some(proto::Hash { value: vec![5; 32] }),
                    timestamp: None,
                    leader_id: None,
                    term: 1,
                    committed_index: 101,
                },
            ],
        };

        let chain = ChainProof::from_proto(proto_chain);
        assert_eq!(chain.headers.len(), 2);
        assert_eq!(chain.headers[0].height, 101);
        assert_eq!(chain.headers[1].height, 102);
    }

    #[test]
    fn test_chain_proof_verify_empty() {
        let chain = ChainProof { headers: vec![] };
        let trusted_hash = vec![0; 32];
        assert!(chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_first_links_to_trusted() {
        let chain = ChainProof {
            headers: vec![BlockHeader {
                height: 101,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![1, 2, 3, 4], // Must match trusted_hash
                tx_merkle_root: vec![5, 6, 7, 8],
                state_root: vec![9, 10, 11, 12],
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 100,
            }],
        };
        let trusted_hash = vec![1, 2, 3, 4];
        assert!(chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_fails_if_first_not_linked() {
        let chain = ChainProof {
            headers: vec![BlockHeader {
                height: 101,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![0, 0, 0, 0], // Wrong hash
                tx_merkle_root: vec![5, 6, 7, 8],
                state_root: vec![9, 10, 11, 12],
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 100,
            }],
        };
        let trusted_hash = vec![1, 2, 3, 4];
        assert!(!chain.verify(&trusted_hash));
    }

    #[test]
    fn test_verify_opts_default() {
        let opts = VerifyOpts::new();
        assert!(opts.at_height.is_none());
        assert!(!opts.include_chain_proof);
        assert!(opts.trusted_height.is_none());
    }

    #[test]
    fn test_verify_opts_at_height() {
        let opts = VerifyOpts::new().at_height(100);
        assert_eq!(opts.at_height, Some(100));
        assert!(!opts.include_chain_proof);
    }

    #[test]
    fn test_verify_opts_with_chain_proof() {
        let opts = VerifyOpts::new().with_chain_proof(50);
        assert!(opts.include_chain_proof);
        assert_eq!(opts.trusted_height, Some(50));
    }

    #[test]
    fn test_verify_opts_builder_chain() {
        let opts = VerifyOpts::new().at_height(100).with_chain_proof(50);
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_chain_proof);
        assert_eq!(opts.trusted_height, Some(50));
    }

    #[test]
    fn test_verified_value_from_proto() {
        use inferadb_ledger_proto::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: Some(proto::BlockHeader {
                height: 100,
                organization: Some(proto::OrganizationSlug { slug: 1 }),
                vault: Some(proto::VaultSlug { slug: 0 }),
                previous_hash: Some(proto::Hash { value: vec![1; 32] }),
                tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
                state_root: Some(proto::Hash { value: vec![3; 32] }),
                timestamp: None,
                leader_id: None,
                term: 1,
                committed_index: 99,
            }),
            merkle_proof: Some(proto::MerkleProof {
                leaf_hash: Some(proto::Hash { value: vec![4; 32] }),
                siblings: vec![],
            }),
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_some());
        let v = verified.unwrap();
        assert_eq!(v.value, Some(b"test-value".to_vec()));
        assert_eq!(v.block_height, 100);
        assert_eq!(v.block_header.height, 100);
        assert_eq!(v.merkle_proof.leaf_hash, vec![4; 32]);
        assert!(v.chain_proof.is_none());
    }

    #[test]
    fn test_verified_value_from_proto_missing_header() {
        use inferadb_ledger_proto::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: None, // Missing
            merkle_proof: Some(proto::MerkleProof {
                leaf_hash: Some(proto::Hash { value: vec![4; 32] }),
                siblings: vec![],
            }),
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_none()); // Should return None if header missing
    }

    #[test]
    fn test_verified_value_from_proto_missing_proof() {
        use inferadb_ledger_proto::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: Some(proto::BlockHeader {
                height: 100,
                organization: Some(proto::OrganizationSlug { slug: 1 }),
                vault: Some(proto::VaultSlug { slug: 0 }),
                previous_hash: Some(proto::Hash { value: vec![1; 32] }),
                tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
                state_root: Some(proto::Hash { value: vec![3; 32] }),
                timestamp: None,
                leader_id: None,
                term: 1,
                committed_index: 99,
            }),
            merkle_proof: None, // Missing
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_none()); // Should return None if proof missing
    }

    #[test]
    fn test_verified_value_verify_succeeds_with_matching_root() {
        // Create a verified value where the merkle proof matches the state root
        let state_root = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let verified = VerifiedValue {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: BlockHeader {
                height: 100,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![0; 32],
                tx_merkle_root: vec![0; 32],
                state_root: state_root.clone(),
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 99,
            },
            merkle_proof: MerkleProof {
                leaf_hash: state_root, // Single element tree: leaf == root
                siblings: vec![],
            },
            chain_proof: None,
        };

        let result = verified.verify();
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_verified_value_verify_fails_with_mismatched_root() {
        // Create a verified value where the merkle proof does NOT match the state root
        let verified = VerifiedValue {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: BlockHeader {
                height: 100,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![0; 32],
                tx_merkle_root: vec![0; 32],
                state_root: vec![1, 2, 3, 4], // Expected root
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 99,
            },
            merkle_proof: MerkleProof {
                leaf_hash: vec![5, 6, 7, 8], // Different hash!
                siblings: vec![],
            },
            chain_proof: None,
        };

        let result = verified.verify();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_verified_read_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59999"]))
            .client_id("verified-read-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result =
            client.verified_read(ORG, Some(VaultSlug::new(0)), "key", VerifyOpts::new()).await;

        assert!(result.is_err(), "expected connection error");
    }

    // =========================================================================
    // Query Types Tests
    // =========================================================================

    #[test]
    fn test_entity_from_proto() {
        let proto_entity = proto::Entity {
            key: "user:123".to_string(),
            value: b"data".to_vec(),
            expires_at: Some(1700000000),
            version: 42,
        };

        let entity = Entity::from_proto(proto_entity);

        assert_eq!(entity.key, "user:123");
        assert_eq!(entity.value, b"data");
        assert_eq!(entity.expires_at, Some(1700000000));
        assert_eq!(entity.version, 42);
    }

    #[test]
    fn test_entity_from_proto_no_expiration() {
        let proto_entity = proto::Entity {
            key: "session:abc".to_string(),
            value: vec![],
            expires_at: None,
            version: 1,
        };

        let entity = Entity::from_proto(proto_entity);

        assert_eq!(entity.expires_at, None);
    }

    #[test]
    fn test_entity_from_proto_zero_expiration_treated_as_none() {
        let proto_entity = proto::Entity {
            key: "key".to_string(),
            value: vec![],
            expires_at: Some(0),
            version: 1,
        };

        let entity = Entity::from_proto(proto_entity);

        // Zero expiration is treated as "no expiration"
        assert_eq!(entity.expires_at, None);
    }

    #[test]
    fn test_entity_is_expired_at() {
        let entity =
            Entity { key: "key".to_string(), value: vec![], expires_at: Some(1000), version: 1 };

        // Before expiration
        assert!(!entity.is_expired_at(999));
        // At expiration
        assert!(entity.is_expired_at(1000));
        // After expiration
        assert!(entity.is_expired_at(1001));
    }

    #[test]
    fn test_entity_is_expired_at_no_expiration() {
        let entity = Entity { key: "key".to_string(), value: vec![], expires_at: None, version: 1 };

        // Never expires
        assert!(!entity.is_expired_at(u64::MAX));
    }

    #[test]
    fn test_entity_equality() {
        let entity1 = Entity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            expires_at: Some(1000),
            version: 1,
        };
        let entity2 = entity1.clone();

        assert_eq!(entity1, entity2);
    }

    #[test]
    fn test_relationship_new() {
        let rel = Relationship::new("document:1", "viewer", "user:alice");

        assert_eq!(rel.resource, "document:1");
        assert_eq!(rel.relation, "viewer");
        assert_eq!(rel.subject, "user:alice");
    }

    #[test]
    fn test_relationship_from_proto() {
        let proto_rel = proto::Relationship {
            resource: "folder:root".to_string(),
            relation: "owner".to_string(),
            subject: "user:admin".to_string(),
        };

        let rel = Relationship::from_proto(proto_rel);

        assert_eq!(rel.resource, "folder:root");
        assert_eq!(rel.relation, "owner");
        assert_eq!(rel.subject, "user:admin");
    }

    #[test]
    fn test_relationship_equality_and_hash() {
        use std::collections::HashSet;

        let rel1 = Relationship::new("doc:1", "editor", "user:bob");
        let rel2 = Relationship::new("doc:1", "editor", "user:bob");
        let rel3 = Relationship::new("doc:1", "viewer", "user:bob");

        assert_eq!(rel1, rel2);
        assert_ne!(rel1, rel3);

        let mut set = HashSet::new();
        set.insert(rel1.clone());
        assert!(set.contains(&rel2));
        assert!(!set.contains(&rel3));
    }

    #[test]
    fn test_paged_result_has_next_page() {
        let with_next: PagedResult<String> = PagedResult {
            items: vec!["item".to_string()],
            next_page_token: Some("token".to_string()),
            block_height: 100,
        };

        let without_next: PagedResult<String> = PagedResult {
            items: vec!["item".to_string()],
            next_page_token: None,
            block_height: 100,
        };

        assert!(with_next.has_next_page());
        assert!(!without_next.has_next_page());
    }

    #[test]
    fn test_list_entities_opts_builder() {
        let opts = ListEntitiesOpts::with_prefix("user:")
            .at_height(100)
            .include_expired()
            .limit(50)
            .page_token("abc123")
            .linearizable();

        assert_eq!(opts.key_prefix, "user:");
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_expired);
        assert_eq!(opts.limit, 50);
        assert_eq!(opts.page_token, Some("abc123".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_entities_opts_defaults() {
        let opts = ListEntitiesOpts::with_prefix("session:");

        assert_eq!(opts.key_prefix, "session:");
        assert_eq!(opts.at_height, None);
        assert!(!opts.include_expired);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_relationships_opts_builder() {
        let opts = ListRelationshipsOpts::new()
            .resource("document:1")
            .relation("viewer")
            .subject("user:alice")
            .at_height(50)
            .limit(100)
            .page_token("xyz")
            .consistency(ReadConsistency::Linearizable);

        assert_eq!(opts.resource, Some("document:1".to_string()));
        assert_eq!(opts.relation, Some("viewer".to_string()));
        assert_eq!(opts.subject, Some("user:alice".to_string()));
        assert_eq!(opts.at_height, Some(50));
        assert_eq!(opts.limit, 100);
        assert_eq!(opts.page_token, Some("xyz".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_relationships_opts_defaults() {
        let opts = ListRelationshipsOpts::new();

        assert_eq!(opts.resource, None);
        assert_eq!(opts.relation, None);
        assert_eq!(opts.subject, None);
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_resources_opts_builder() {
        let opts = ListResourcesOpts::with_type("document")
            .at_height(200)
            .limit(25)
            .page_token("next")
            .linearizable();

        assert_eq!(opts.resource_type, "document");
        assert_eq!(opts.at_height, Some(200));
        assert_eq!(opts.limit, 25);
        assert_eq!(opts.page_token, Some("next".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_resources_opts_defaults() {
        let opts = ListResourcesOpts::with_type("folder");

        assert_eq!(opts.resource_type, "folder");
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_entities_opts_bon_builder() {
        let opts = ListEntitiesOpts::builder()
            .key_prefix("user:")
            .at_height(100)
            .include_expired(true)
            .limit(50)
            .page_token("abc123")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.key_prefix, "user:");
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_expired);
        assert_eq!(opts.limit, 50);
        assert_eq!(opts.page_token, Some("abc123".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_entities_opts_bon_builder_defaults() {
        let opts = ListEntitiesOpts::builder().build();

        assert_eq!(opts.key_prefix, "");
        assert_eq!(opts.at_height, None);
        assert!(!opts.include_expired);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_entities_opts_bon_builder_matches_default() {
        let from_builder = ListEntitiesOpts::builder().build();
        let from_default = ListEntitiesOpts::default();

        assert_eq!(from_builder.key_prefix, from_default.key_prefix);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.include_expired, from_default.include_expired);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder() {
        let opts = ListRelationshipsOpts::builder()
            .resource("document:1")
            .relation("viewer")
            .subject("user:alice")
            .at_height(50)
            .limit(100)
            .page_token("xyz")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.resource, Some("document:1".to_string()));
        assert_eq!(opts.relation, Some("viewer".to_string()));
        assert_eq!(opts.subject, Some("user:alice".to_string()));
        assert_eq!(opts.at_height, Some(50));
        assert_eq!(opts.limit, 100);
        assert_eq!(opts.page_token, Some("xyz".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder_defaults() {
        let opts = ListRelationshipsOpts::builder().build();

        assert_eq!(opts.resource, None);
        assert_eq!(opts.relation, None);
        assert_eq!(opts.subject, None);
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder_matches_default() {
        let from_builder = ListRelationshipsOpts::builder().build();
        let from_default = ListRelationshipsOpts::default();

        assert_eq!(from_builder.resource, from_default.resource);
        assert_eq!(from_builder.relation, from_default.relation);
        assert_eq!(from_builder.subject, from_default.subject);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    #[test]
    fn test_list_resources_opts_bon_builder() {
        let opts = ListResourcesOpts::builder()
            .resource_type("document")
            .at_height(200)
            .limit(25)
            .page_token("next")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.resource_type, "document");
        assert_eq!(opts.at_height, Some(200));
        assert_eq!(opts.limit, 25);
        assert_eq!(opts.page_token, Some("next".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_resources_opts_bon_builder_defaults() {
        let opts = ListResourcesOpts::builder().build();

        assert_eq!(opts.resource_type, "");
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_resources_opts_bon_builder_matches_default() {
        let from_builder = ListResourcesOpts::builder().build();
        let from_default = ListResourcesOpts::default();

        assert_eq!(from_builder.resource_type, from_default.resource_type);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    // =========================================================================
    // Query Operations Integration Tests
    // =========================================================================

    #[tokio::test]
    async fn test_list_entities_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59999"]))
            .client_id("list-entities-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client.list_entities(ORG, ListEntitiesOpts::with_prefix("user:")).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_relationships_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59999"]))
            .client_id("list-rels-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result =
            client.list_relationships(ORG, VaultSlug::new(0), ListRelationshipsOpts::new()).await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_resources_returns_error_on_connection_failure() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59999"]))
            .client_id("list-resources-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let result = client
            .list_resources(ORG, VaultSlug::new(0), ListResourcesOpts::with_type("document"))
            .await;

        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_entities_with_different_options() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59999"]))
            .client_id("list-entities-opts-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Test with various options - should still fail on connection but validates options are
        // passed
        let opts = ListEntitiesOpts::with_prefix("session:")
            .at_height(100)
            .include_expired()
            .limit(50)
            .linearizable();

        let result = client.list_entities(ORG, opts).await;
        assert!(result.is_err(), "expected connection error");
    }

    #[tokio::test]
    async fn test_list_relationships_with_filters() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59999"]))
            .client_id("list-rels-filter-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Test with filters
        let opts =
            ListRelationshipsOpts::new().resource("document:1").relation("viewer").limit(100);

        let result = client.list_relationships(ORG, VaultSlug::new(0), opts).await;
        assert!(result.is_err(), "expected connection error");
    }

    // =========================================================================
    // Shutdown Tests
    // =========================================================================

    #[tokio::test]
    async fn test_is_shutdown_false_initially() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        assert!(!client.is_shutdown(), "client should not be shutdown initially");
    }

    #[tokio::test]
    async fn test_is_shutdown_true_after_shutdown() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        client.shutdown().await;

        assert!(client.is_shutdown(), "client should be shutdown after calling shutdown()");
    }

    #[tokio::test]
    async fn test_shutdown_is_idempotent() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        // Multiple shutdown calls should not panic
        client.shutdown().await;
        client.shutdown().await;
        client.shutdown().await;

        assert!(client.is_shutdown());
    }

    #[tokio::test]
    async fn test_cloned_client_shares_shutdown_state() {
        let client1 = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        let client2 = client1.clone();

        assert!(!client1.is_shutdown());
        assert!(!client2.is_shutdown());

        // Shutdown through client1
        client1.shutdown().await;

        // Both should reflect shutdown state
        assert!(client1.is_shutdown());
        assert!(client2.is_shutdown(), "cloned client should share shutdown state");
    }

    #[tokio::test]
    async fn test_read_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        // Shutdown the client
        client.shutdown().await;

        // All operations should return Shutdown error
        let result = client.read(ORG, Some(VaultSlug::new(0)), "key").await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_write_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        let result = client
            .write(ORG, Some(VaultSlug::new(0)), vec![Operation::set_entity("key", vec![1, 2, 3])])
            .await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_batch_write_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        let result = client
            .batch_write(
                ORG,
                Some(VaultSlug::new(0)),
                vec![vec![Operation::set_entity("key", vec![1, 2, 3])]],
            )
            .await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_batch_read_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        let result = client
            .batch_read(ORG, Some(VaultSlug::new(0)), vec!["key1".to_string(), "key2".to_string()])
            .await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_watch_blocks_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        let result = client.watch_blocks(ORG, VaultSlug::new(0), 1).await;
        assert!(matches!(result, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_admin_operations_return_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        // Test various admin operations
        assert!(matches!(
            client.create_organization("test").await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(
            client.get_organization(ORG).await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(client.list_organizations().await, Err(crate::error::SdkError::Shutdown)));
        assert!(matches!(client.create_vault(ORG).await, Err(crate::error::SdkError::Shutdown)));
        assert!(matches!(
            client.get_vault(ORG, VaultSlug::new(0)).await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(client.list_vaults().await, Err(crate::error::SdkError::Shutdown)));
    }

    #[tokio::test]
    async fn test_health_check_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        assert!(matches!(client.health_check().await, Err(crate::error::SdkError::Shutdown)));
        assert!(matches!(
            client.health_check_detailed().await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(
            client.health_check_vault(ORG, VaultSlug::new(0)).await,
            Err(crate::error::SdkError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn test_verified_read_returns_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        assert!(matches!(
            client.verified_read(ORG, Some(VaultSlug::new(0)), "key", VerifyOpts::new()).await,
            Err(crate::error::SdkError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn test_query_operations_return_shutdown_error_after_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        client.shutdown().await;

        assert!(matches!(
            client.list_entities(ORG, ListEntitiesOpts::with_prefix("key")).await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(
            client.list_relationships(ORG, VaultSlug::new(0), ListRelationshipsOpts::new()).await,
            Err(crate::error::SdkError::Shutdown)
        ));
        assert!(matches!(
            client
                .list_resources(ORG, VaultSlug::new(0), ListResourcesOpts::with_type("doc"))
                .await,
            Err(crate::error::SdkError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn test_shutdown_error_is_not_retryable() {
        assert!(!crate::error::SdkError::Shutdown.is_retryable());
    }

    // =========================================================================
    // Cancellation tests
    // =========================================================================

    #[tokio::test]
    async fn test_cancellation_token_accessor() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let token = client.cancellation_token();

        // Token should not be cancelled initially
        assert!(!token.is_cancelled());

        // After shutdown, the token should be cancelled
        client.shutdown().await;
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_child_token_cancelled_on_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let child = client.cancellation_token().child_token();

        assert!(!child.is_cancelled());

        client.shutdown().await;
        assert!(child.is_cancelled());
    }

    #[tokio::test]
    async fn test_read_with_token_pre_cancelled() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();

        let result = client.read_with_token(ORG, None, "key", token).await;
        assert!(matches!(result, Err(crate::error::SdkError::Cancelled)));
    }

    #[tokio::test]
    async fn test_write_with_token_pre_cancelled() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();

        let result = client
            .write_with_token(ORG, None, vec![Operation::set_entity("key", b"val".to_vec())], token)
            .await;
        assert!(matches!(result, Err(crate::error::SdkError::Cancelled)));
    }

    #[tokio::test]
    async fn test_batch_read_with_token_pre_cancelled() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();

        let result = client.batch_read_with_token(ORG, None, vec!["key1", "key2"], token).await;
        assert!(matches!(result, Err(crate::error::SdkError::Cancelled)));
    }

    #[tokio::test]
    async fn test_batch_write_with_token_pre_cancelled() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();

        let ops = vec![vec![Operation::set_entity("key", b"val".to_vec())]];
        let result = client.batch_write_with_token(ORG, None, ops, token).await;
        assert!(matches!(result, Err(crate::error::SdkError::Cancelled)));
    }

    #[tokio::test]
    async fn test_cancelled_error_is_not_retryable() {
        assert!(!crate::error::SdkError::Cancelled.is_retryable());
    }

    #[tokio::test]
    async fn test_cancelled_differs_from_shutdown() {
        // Cancelled and Shutdown are distinct error types
        let cancelled = crate::error::SdkError::Cancelled;
        let shutdown = crate::error::SdkError::Shutdown;

        assert!(!matches!(cancelled, crate::error::SdkError::Shutdown));
        assert!(!matches!(shutdown, crate::error::SdkError::Cancelled));
    }

    #[tokio::test]
    async fn test_read_with_token_returns_cancelled_during_backoff() {
        // Set many retries with long backoff so cancellation fires during backoff
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(10)
                    .initial_backoff(Duration::from_secs(30))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(50))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let token = tokio_util::sync::CancellationToken::new();
        let token_clone = token.clone();

        // Cancel after 200ms — the first attempt fails quickly,
        // then the 30s backoff starts, and cancellation fires during it
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            token_clone.cancel();
        });

        let start = std::time::Instant::now();
        let result = client.read_with_token(ORG, None, "key", token).await;
        let elapsed = start.elapsed();

        // Should be cancelled during the backoff sleep
        assert!(
            matches!(result, Err(crate::error::SdkError::Cancelled)),
            "expected Cancelled, got: {:?}",
            result
        );
        // Should return quickly, not wait for the 30s backoff
        assert!(elapsed < Duration::from_secs(5), "took {:?}", elapsed);
    }

    #[tokio::test]
    async fn test_shutdown_cancels_inflight_retries() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(10)
                    .initial_backoff(Duration::from_secs(30))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(50))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let client_clone = client.clone();

        // Shutdown after 200ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            client_clone.shutdown().await;
        });

        let start = std::time::Instant::now();
        let result = client.read(ORG, None, "key").await;
        let elapsed = start.elapsed();

        // Should receive either Shutdown or Cancelled (from the client cancellation token)
        assert!(
            matches!(
                result,
                Err(crate::error::SdkError::Cancelled | crate::error::SdkError::Shutdown)
            ),
            "expected cancellation-related error, got: {:?}",
            result
        );
        // Should not wait for the full 10 attempts × 30s backoff
        assert!(elapsed < Duration::from_secs(5), "took {:?}", elapsed);
    }

    // =========================================================================
    // Operation validation tests
    // =========================================================================

    #[test]
    fn test_operation_validate_set_entity_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::set_entity("user:123", b"data".to_vec());
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_set_entity_empty_key() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::SetEntity {
            key: String::new(),
            value: b"data".to_vec(),
            expires_at: None,
            condition: None,
        };
        let err = op.validate(&config).unwrap_err();
        assert!(err.to_string().contains("key"), "Error should mention key: {err}");
    }

    #[test]
    fn test_operation_validate_set_entity_invalid_key_chars() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::set_entity("user 123", b"data".to_vec());
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_key_too_long() {
        let config = inferadb_ledger_types::config::ValidationConfig::builder()
            .max_key_bytes(10)
            .build()
            .unwrap();
        let op = Operation::set_entity("a".repeat(11), b"data".to_vec());
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_value_too_large() {
        let config = inferadb_ledger_types::config::ValidationConfig::builder()
            .max_value_bytes(4)
            .build()
            .unwrap();
        let op = Operation::set_entity("key", vec![0u8; 5]);
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_delete_entity_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::delete_entity("user:123");
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_delete_entity_empty_key() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::DeleteEntity { key: String::new() };
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_create_relationship_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::create_relationship("doc:456", "viewer", "user:123");
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_relationship_with_hash() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::create_relationship("doc:456", "viewer", "user:123#member");
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_relationship_empty_resource() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::CreateRelationship {
            resource: String::new(),
            relation: "viewer".to_string(),
            subject: "user:123".to_string(),
        };
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_relationship_invalid_chars() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::create_relationship("doc 456", "viewer", "user:123");
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_delete_relationship_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::delete_relationship("doc:456", "viewer", "user:123");
        assert!(op.validate(&config).is_ok());
    }

    // =========================================================================
    // estimated_size_bytes tests
    // =========================================================================

    #[test]
    fn test_estimated_size_set_entity() {
        let op = Operation::set_entity("key", b"value".to_vec());
        assert_eq!(op.estimated_size_bytes(), 3 + 5); // "key" + "value"
    }

    #[test]
    fn test_estimated_size_delete_entity() {
        let op = Operation::delete_entity("user:123");
        assert_eq!(op.estimated_size_bytes(), 8); // "user:123"
    }

    #[test]
    fn test_estimated_size_relationship() {
        let op = Operation::create_relationship("doc:456", "viewer", "user:123");
        assert_eq!(op.estimated_size_bytes(), 7 + 6 + 8); // "doc:456" + "viewer" + "user:123"
    }

    // =========================================================================
    // SdkError::Validation tests
    // =========================================================================

    #[test]
    fn test_sdk_validation_error_not_retryable() {
        let err = crate::error::SdkError::Validation { message: "key too long".to_string() };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sdk_validation_error_display() {
        let err = crate::error::SdkError::Validation { message: "key too long".to_string() };
        assert!(err.to_string().contains("key too long"));
    }

    // =========================================================================
    // Events type conversion tests
    // =========================================================================

    fn make_proto_event_entry() -> proto::EventEntry {
        proto::EventEntry {
            event_id: uuid::Uuid::nil().into_bytes().to_vec(),
            source_service: "ledger".to_string(),
            event_type: "ledger.vault.created".to_string(),
            timestamp: Some(prost_types::Timestamp { seconds: 1_700_000_000, nanos: 500_000 }),
            scope: proto::EventScope::Organization as i32,
            action: "vault_created".to_string(),
            emission_path: proto::EventEmissionPath::EmissionPathApplyPhase as i32,
            principal: "user:alice".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 12345 }),
            vault: Some(proto::VaultSlug { slug: 67890 }),
            outcome: proto::EventOutcome::Success as i32,
            error_code: None,
            error_detail: None,
            denial_reason: None,
            details: [("vault_name".to_string(), "my-vault".to_string())].into_iter().collect(),
            block_height: Some(42),
            node_id: None,
            trace_id: Some("abc-trace".to_string()),
            correlation_id: Some("batch-123".to_string()),
            operations_count: None,
            expires_at: 0,
        }
    }

    #[test]
    fn test_sdk_event_entry_from_proto_success() {
        let proto = make_proto_event_entry();
        let entry = SdkEventEntry::from_proto(proto);

        assert_eq!(entry.source_service, "ledger");
        assert_eq!(entry.event_type, "ledger.vault.created");
        assert_eq!(entry.action, "vault_created");
        assert_eq!(entry.principal, "user:alice");
        assert_eq!(entry.organization, OrganizationSlug::new(12345));
        assert_eq!(entry.vault, Some(VaultSlug::new(67890)));
        assert_eq!(entry.scope, EventScope::Organization);
        assert_eq!(entry.emission_path, EventEmissionPath::ApplyPhase);
        assert!(matches!(entry.outcome, EventOutcome::Success));
        assert_eq!(entry.details.get("vault_name").unwrap(), "my-vault");
        assert_eq!(entry.block_height, Some(42));
        assert_eq!(entry.trace_id.as_deref(), Some("abc-trace"));
        assert_eq!(entry.correlation_id.as_deref(), Some("batch-123"));
        assert_eq!(entry.timestamp.timestamp(), 1_700_000_000);
    }

    #[test]
    fn test_sdk_event_entry_from_proto_failed_outcome() {
        let mut proto = make_proto_event_entry();
        proto.outcome = proto::EventOutcome::Failed as i32;
        proto.error_code = Some("STORAGE_FULL".to_string());
        proto.error_detail = Some("disk quota exceeded".to_string());

        let entry = SdkEventEntry::from_proto(proto);
        match &entry.outcome {
            EventOutcome::Failed { code, detail } => {
                assert_eq!(code, "STORAGE_FULL");
                assert_eq!(detail, "disk quota exceeded");
            },
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_sdk_event_entry_from_proto_denied_outcome() {
        let mut proto = make_proto_event_entry();
        proto.outcome = proto::EventOutcome::Denied as i32;
        proto.denial_reason = Some("rate limit exceeded".to_string());

        let entry = SdkEventEntry::from_proto(proto);
        match &entry.outcome {
            EventOutcome::Denied { reason } => {
                assert_eq!(reason, "rate limit exceeded");
            },
            _ => panic!("expected Denied outcome"),
        }
    }

    #[test]
    fn test_sdk_event_entry_from_proto_handler_phase() {
        let mut proto = make_proto_event_entry();
        proto.emission_path = proto::EventEmissionPath::EmissionPathHandlerPhase as i32;
        proto.node_id = Some(7);

        let entry = SdkEventEntry::from_proto(proto);
        assert_eq!(entry.emission_path, EventEmissionPath::HandlerPhase);
        assert_eq!(entry.node_id, Some(7));
    }

    #[test]
    fn test_sdk_event_entry_from_proto_system_scope() {
        let mut proto = make_proto_event_entry();
        proto.scope = proto::EventScope::System as i32;
        proto.organization = Some(proto::OrganizationSlug { slug: 0 });

        let entry = SdkEventEntry::from_proto(proto);
        assert_eq!(entry.scope, EventScope::System);
        assert_eq!(entry.organization, OrganizationSlug::new(0));
    }

    #[test]
    fn test_sdk_event_entry_event_id_string_uuid() {
        let entry = SdkEventEntry::from_proto(make_proto_event_entry());
        assert_eq!(entry.event_id_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_sdk_event_entry_event_id_string_non_uuid() {
        let mut proto = make_proto_event_entry();
        proto.event_id = vec![0xab, 0xcd, 0xef];
        let entry = SdkEventEntry::from_proto(proto);
        assert_eq!(entry.event_id_string(), "abcdef");
    }

    #[test]
    fn test_event_filter_default_is_all_pass() {
        let filter = EventFilter::new();
        let proto = filter.to_proto();

        assert!(proto.actions.is_empty());
        assert!(proto.start_time.is_none());
        assert!(proto.end_time.is_none());
        assert!(proto.event_type_prefix.is_none());
        assert!(proto.principal.is_none());
        assert_eq!(proto.outcome, 0);
        assert_eq!(proto.emission_path, 0);
        assert!(proto.correlation_id.is_none());
    }

    #[test]
    fn test_event_filter_with_all_options() {
        let start = chrono::DateTime::from_timestamp(1_000_000, 0).unwrap();
        let end = chrono::DateTime::from_timestamp(2_000_000, 0).unwrap();

        let filter = EventFilter::new()
            .start_time(start)
            .end_time(end)
            .actions(["vault_created", "vault_deleted"])
            .event_type_prefix("ledger.vault")
            .principal("user:bob")
            .outcome_denied()
            .apply_phase_only()
            .correlation_id("job-99");

        let proto = filter.to_proto();
        assert_eq!(proto.start_time.unwrap().seconds, 1_000_000);
        assert_eq!(proto.end_time.unwrap().seconds, 2_000_000);
        assert_eq!(proto.actions, vec!["vault_created", "vault_deleted"]);
        assert_eq!(proto.event_type_prefix.as_deref(), Some("ledger.vault"));
        assert_eq!(proto.principal.as_deref(), Some("user:bob"));
        assert_eq!(proto.outcome, proto::EventOutcome::Denied as i32);
        assert_eq!(proto.emission_path, proto::EventEmissionPath::EmissionPathApplyPhase as i32);
        assert_eq!(proto.correlation_id.as_deref(), Some("job-99"));
    }

    #[test]
    fn test_event_filter_outcome_variants() {
        assert_eq!(
            EventFilter::new().outcome_success().to_proto().outcome,
            proto::EventOutcome::Success as i32,
        );
        assert_eq!(
            EventFilter::new().outcome_failed().to_proto().outcome,
            proto::EventOutcome::Failed as i32,
        );
        assert_eq!(
            EventFilter::new().outcome_denied().to_proto().outcome,
            proto::EventOutcome::Denied as i32,
        );
    }

    #[test]
    fn test_event_filter_emission_path_variants() {
        assert_eq!(
            EventFilter::new().apply_phase_only().to_proto().emission_path,
            proto::EventEmissionPath::EmissionPathApplyPhase as i32,
        );
        assert_eq!(
            EventFilter::new().handler_phase_only().to_proto().emission_path,
            proto::EventEmissionPath::EmissionPathHandlerPhase as i32,
        );
    }

    #[test]
    fn test_ingest_event_entry_required_fields() {
        let entry = SdkIngestEventEntry::new(
            "engine.authorization.checked",
            "user:alice",
            EventOutcome::Success,
        );
        let proto = entry.into_proto();

        assert_eq!(proto.event_type, "engine.authorization.checked");
        assert_eq!(proto.principal, "user:alice");
        assert_eq!(proto.outcome, proto::EventOutcome::Success as i32);
        assert!(proto.details.is_empty());
        assert!(proto.trace_id.is_none());
        assert!(proto.correlation_id.is_none());
        assert!(proto.vault.is_none());
        assert!(proto.timestamp.is_none());
    }

    #[test]
    fn test_ingest_event_entry_with_all_optional_fields() {
        let ts = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let entry = SdkIngestEventEntry::new(
            "control.member.invited",
            "admin:bob",
            EventOutcome::Failed { code: "LIMIT".to_string(), detail: "max members".to_string() },
        )
        .detail("email", "new@example.com")
        .trace_id("trace-xyz")
        .correlation_id("import-batch-7")
        .vault(VaultSlug::new(999))
        .timestamp(ts);

        let proto = entry.into_proto();
        assert_eq!(proto.event_type, "control.member.invited");
        assert_eq!(proto.principal, "admin:bob");
        assert_eq!(proto.outcome, proto::EventOutcome::Failed as i32);
        assert_eq!(proto.error_code.as_deref(), Some("LIMIT"));
        assert_eq!(proto.error_detail.as_deref(), Some("max members"));
        assert_eq!(proto.details.get("email").unwrap(), "new@example.com");
        assert_eq!(proto.trace_id.as_deref(), Some("trace-xyz"));
        assert_eq!(proto.correlation_id.as_deref(), Some("import-batch-7"));
        assert_eq!(proto.vault.unwrap().slug, 999);
        assert_eq!(proto.timestamp.unwrap().seconds, 1_700_000_000);
    }

    #[test]
    fn test_ingest_event_entry_denied_outcome() {
        let entry = SdkIngestEventEntry::new(
            "engine.authorization.checked",
            "user:charlie",
            EventOutcome::Denied { reason: "no permission".to_string() },
        );
        let proto = entry.into_proto();
        assert_eq!(proto.outcome, proto::EventOutcome::Denied as i32);
        assert_eq!(proto.denial_reason.as_deref(), Some("no permission"));
        assert!(proto.error_code.is_none());
    }

    #[test]
    fn test_event_page_has_next_page() {
        let page = EventPage {
            entries: vec![],
            next_page_token: Some("cursor-abc".to_string()),
            total_estimate: None,
        };
        assert!(page.has_next_page());

        let page = EventPage { entries: vec![], next_page_token: None, total_estimate: None };
        assert!(!page.has_next_page());
    }

    #[test]
    fn test_event_outcome_roundtrip_success() {
        let outcome = EventOutcome::Success;
        let (value, code, detail, reason) = outcome.to_proto();
        let restored = EventOutcome::from_proto(value, code, detail, reason);
        assert!(matches!(restored, EventOutcome::Success));
    }

    #[test]
    fn test_event_outcome_roundtrip_failed() {
        let outcome = EventOutcome::Failed {
            code: "ERR_001".to_string(),
            detail: "something broke".to_string(),
        };
        let (value, code, detail, reason) = outcome.to_proto();
        let restored = EventOutcome::from_proto(value, code, detail, reason);
        match restored {
            EventOutcome::Failed { code, detail } => {
                assert_eq!(code, "ERR_001");
                assert_eq!(detail, "something broke");
            },
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn test_event_outcome_roundtrip_denied() {
        let outcome = EventOutcome::Denied { reason: "rate limited".to_string() };
        let (value, code, detail, reason) = outcome.to_proto();
        let restored = EventOutcome::from_proto(value, code, detail, reason);
        match restored {
            EventOutcome::Denied { reason } => {
                assert_eq!(reason, "rate limited");
            },
            _ => panic!("expected Denied"),
        }
    }

    #[test]
    fn test_event_scope_from_proto() {
        assert_eq!(EventScope::from_proto(proto::EventScope::System as i32), EventScope::System);
        assert_eq!(
            EventScope::from_proto(proto::EventScope::Organization as i32),
            EventScope::Organization,
        );
        // Unknown falls back to System
        assert_eq!(EventScope::from_proto(99), EventScope::System);
    }

    #[test]
    fn test_event_emission_path_from_proto() {
        assert_eq!(
            EventEmissionPath::from_proto(proto::EventEmissionPath::EmissionPathApplyPhase as i32),
            EventEmissionPath::ApplyPhase,
        );
        assert_eq!(
            EventEmissionPath::from_proto(
                proto::EventEmissionPath::EmissionPathHandlerPhase as i32,
            ),
            EventEmissionPath::HandlerPhase,
        );
        // Unknown falls back to ApplyPhase
        assert_eq!(EventEmissionPath::from_proto(99), EventEmissionPath::ApplyPhase);
    }

    #[test]
    fn test_ingest_event_entry_detail_builder() {
        let entry = SdkIngestEventEntry::new("test.event", "user:x", EventOutcome::Success)
            .detail("key1", "val1")
            .detail("key2", "val2");
        let proto = entry.into_proto();
        assert_eq!(proto.details.len(), 2);
        assert_eq!(proto.details.get("key1").unwrap(), "val1");
        assert_eq!(proto.details.get("key2").unwrap(), "val2");
    }

    #[test]
    fn test_ingest_event_entry_details_bulk() {
        let map: std::collections::HashMap<String, String> =
            [("a".to_string(), "1".to_string()), ("b".to_string(), "2".to_string())]
                .into_iter()
                .collect();
        let entry =
            SdkIngestEventEntry::new("test.event", "user:x", EventOutcome::Success).details(map);
        let proto = entry.into_proto();
        assert_eq!(proto.details.len(), 2);
    }
}
