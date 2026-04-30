//! Events query and ingestion service implementation.
//!
//! Provides [`EventsService`], the gRPC service that reads from the
//! local `Events` table with filtering, pagination, and organization-scoped
//! access control. This is the unified query API — it returns events from
//! all sources (Ledger, Engine, Control) stored in the same Events table.
//!
//! ## Consistency model
//!
//! Apply-phase events are identical across all nodes (replicated by state
//! machine determinism). Handler-phase events exist only on the node that
//! generated them. Clients can filter by `emission_path` for deterministic
//! cross-node results.

use std::{sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    event_writer::IngestionRateLimiter, log_storage::AppliedStateAccessor,
    pagination::PageTokenCodec, raft_manager::RaftManager,
};
use inferadb_ledger_state::EventsDatabase;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{OrganizationId, VaultId, events::EventConfig};

use crate::proposal::ProposalService;

/// Default limit for `ListEvents` when the request specifies 0.
pub(super) const DEFAULT_LIMIT: usize = 100;

/// Maximum limit for `ListEvents`.
pub(super) const MAX_LIMIT: usize = 1000;

/// Maximum number of entries to scan in `CountEvents` to prevent
/// unbounded scans on large datasets.
pub(super) const COUNT_SCAN_LIMIT: usize = 100_000;

/// Resolver for per-vault `EventsDatabase` handles owned by the local node.
///
/// `ListEvents` fans out across the org-level events store and every
/// per-vault events store registered for the target organization. The
/// resolver returns one entry per local vault, plus its `(org_id, vault_id)`
/// for cursor disambiguation. The org-level events store is supplied
/// separately via the service's `events_db` field.
///
/// In production this is wired from
/// [`RaftManager::list_vault_groups`](inferadb_ledger_raft::raft_manager::RaftManager::list_vault_groups);
/// tests inject in-memory handles directly.
pub type VaultEventSources<B> =
    Arc<dyn Fn(OrganizationId) -> Vec<VaultEventSource<B>> + Send + Sync>;

/// One per-vault events source returned by [`VaultEventSources`].
///
/// `events_db` is the vault's own events store; `vault_id` identifies the
/// vault within its parent organization.
pub struct VaultEventSource<B: StorageBackend> {
    /// Vault identifier within its parent organization.
    pub vault_id: VaultId,
    /// The vault's events database handle.
    pub events_db: EventsDatabase<B>,
}

/// Events query and ingestion service.
///
/// Reads from the local `Events` B+ tree — no Raft proposal needed. The
/// service supports organization-scoped queries with filtering by time range,
/// action, principal, outcome, emission path, and `event_type` prefix.
///
/// When ingestion fields (`event_config`, `node_id`, `ingestion_rate_limiter`)
/// are set, the service also handles `IngestEvents` RPCs from external services
/// (Engine, Control) — writing their audit events to the local Events table
/// using handler-phase semantics.
#[derive(bon::Builder)]
pub struct EventsService<B: StorageBackend> {
    /// Events database (local to this node).
    #[allow(dead_code)] // reserved for read paths + test fixtures
    pub(super) events_db: EventsDatabase<B>,

    /// Applied state for slug resolution.
    pub(super) applied_state: AppliedStateAccessor,

    /// HMAC codec for page token signing/verification.
    pub(super) page_token_codec: PageTokenCodec,

    /// Event configuration (for ingestion validation: TTL, details size, source allow-list).
    pub(super) event_config: Option<Arc<EventConfig>>,

    /// Node ID for handler-phase event emission.
    pub(super) node_id: Option<u64>,

    /// Per-source rate limiter for ingestion.
    pub(super) ingestion_rate_limiter: Option<Arc<IngestionRateLimiter>>,

    /// Proposal service for routing `IngestExternalEvents` through Raft.
    ///
    /// When set, `ingest_events` proposes the accepted
    /// batch as [`OrganizationRequest::IngestExternalEvents`] against the REGIONAL
    /// Raft group owning the organization, instead of writing directly to
    /// `events.db`. When `None`, ingestion is disabled (the RPC returns
    /// `UNAVAILABLE`).
    pub(super) proposer: Option<Arc<dyn ProposalService>>,

    /// Multi-Raft manager, used to resolve an organization to its REGIONAL
    /// Raft group for `IngestExternalEvents` proposal routing.
    pub(super) manager: Option<Arc<RaftManager>>,

    /// Resolver for per-vault events stores registered on the local node.
    ///
    /// Powers [`list_events`](Self::list_events) fan-out across the
    /// org-level and per-vault events stores. When unset, `ListEvents`
    /// reads only the org-level store (`events_db`).
    pub(super) vault_event_sources: Option<VaultEventSources<B>>,

    /// Maximum time to wait for an `IngestExternalEvents` Raft proposal to commit.
    #[builder(default = Duration::from_secs(30))]
    pub(super) proposal_timeout: Duration,
}

/// Computes a deterministic query hash from filter parameters.
///
/// Used by the wire transport's `list_events` / `count_events` handlers to
/// sign and verify page tokens. The same input must always produce the same
/// hash so cursors remain valid across leader changes and process restarts.
pub(super) fn compute_filter_hash_wire(
    filter: &Option<inferadb_ledger_wire::services::events::EventFilter>,
) -> [u8; 8] {
    let mut params = String::with_capacity(256);
    if let Some(f) = filter {
        if let Some(ns) = f.start_time {
            // Mirrors `ns_to_proto_timestamp(ns)` -> `(seconds, nanos)`.
            let secs = ns / 1_000_000_000;
            let nanos = ns % 1_000_000_000;
            params.push_str(&format!("start:{secs}.{nanos},"));
        }
        if let Some(ns) = f.end_time {
            let secs = ns / 1_000_000_000;
            let nanos = ns % 1_000_000_000;
            params.push_str(&format!("end:{secs}.{nanos},"));
        }
        if !f.actions.is_empty() {
            params.push_str(&format!("actions:{},", f.actions.join(";")));
        }
        if let Some(prefix) = &f.event_type_prefix {
            params.push_str(&format!("type_prefix:{prefix},"));
        }
        if let Some(principal) = &f.principal {
            params.push_str(&format!("principal:{principal},"));
        }
        // Wire enums are `#[repr(i32)]` with the same numeric tags as the
        // proto enums (see `crates/wire/src/services/events.rs`).
        let outcome_tag = f.outcome as i32;
        if outcome_tag != 0 {
            params.push_str(&format!("outcome:{outcome_tag},"));
        }
        let emission_tag = f.emission_path as i32;
        if emission_tag != 0 {
            params.push_str(&format!("emission:{emission_tag},"));
        }
        if let Some(cid) = &f.correlation_id {
            params.push_str(&format!("correlation:{cid},"));
        }
        if let Some(vault) = &f.vault {
            params.push_str(&format!("vault:{},", vault.value()));
        }
    }
    PageTokenCodec::compute_query_hash(params.as_bytes())
}

/// Checks whether an event entry matches the given filter criteria (in-memory).
pub(super) fn matches_filter_wire(
    entry: &inferadb_ledger_types::events::EventEntry,
    filter: &Option<inferadb_ledger_wire::services::events::EventFilter>,
) -> bool {
    let Some(f) = filter else {
        return true;
    };

    if !f.actions.is_empty() {
        let action_str = entry.action.as_str();
        if !f.actions.iter().any(|a| a == action_str) {
            return false;
        }
    }

    if let Some(prefix) = &f.event_type_prefix
        && !prefix.is_empty()
        && !entry.event_type.starts_with(prefix.as_str())
    {
        return false;
    }

    if let Some(principal) = &f.principal
        && !principal.is_empty()
        && entry.principal != *principal
    {
        return false;
    }

    let outcome_tag = f.outcome as i32;
    if outcome_tag != 0 {
        // Wire enum numeric tags: Unspecified=0, Success=1, Failed=2, Denied=3.
        // Mirrors the legacy `proto::EventOutcome::from(&entry.outcome)` mapping.
        let entry_outcome: i32 = match entry.outcome {
            inferadb_ledger_types::events::EventOutcome::Success => {
                inferadb_ledger_wire::services::events::EventOutcome::Success as i32
            },
            inferadb_ledger_types::events::EventOutcome::Failed { .. } => {
                inferadb_ledger_wire::services::events::EventOutcome::Failed as i32
            },
            inferadb_ledger_types::events::EventOutcome::Denied { .. } => {
                inferadb_ledger_wire::services::events::EventOutcome::Denied as i32
            },
        };
        if entry_outcome != outcome_tag {
            return false;
        }
    }

    let emission_tag = f.emission_path as i32;
    if emission_tag != 0 {
        // Wire enum numeric tags: Unspecified=0, ApplyPhase=1, HandlerPhase=2.
        // Mirrors the legacy `proto::EventEmissionPath::from(&entry.emission)` mapping.
        let entry_emission: i32 = match entry.emission {
            inferadb_ledger_types::events::EventEmission::ApplyPhase => {
                inferadb_ledger_wire::services::events::EventEmissionPath::ApplyPhase as i32
            },
            inferadb_ledger_types::events::EventEmission::HandlerPhase { .. } => {
                inferadb_ledger_wire::services::events::EventEmissionPath::HandlerPhase as i32
            },
        };
        if entry_emission != emission_tag {
            return false;
        }
    }

    if let Some(cid) = &f.correlation_id
        && !cid.is_empty()
    {
        match &entry.correlation_id {
            Some(entry_cid) if entry_cid == cid => {},
            _ => return false,
        }
    }

    true
}
