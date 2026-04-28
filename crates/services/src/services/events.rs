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

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::Utc;
use inferadb_ledger_proto::proto;
use inferadb_ledger_raft::{
    event_writer::IngestionRateLimiter,
    log_storage::AppliedStateAccessor,
    metrics,
    pagination::{EventPageToken, PageTokenCodec},
    raft_manager::RaftManager,
    types::{LedgerResponse, OrganizationRequest},
};
use inferadb_ledger_state::{EventStore, EventsDatabase};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    OrganizationId, OrganizationSlug, VaultId, VaultSlug,
    events::{EventAction, EventConfig, EventEmission, EventEntry, EventOutcome, EventScope},
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use super::{error_classify, metadata::status_with_correlation, slug_resolver::SlugResolver};
use crate::proposal::ProposalService;

/// Default limit for `ListEvents` when the request specifies 0.
const DEFAULT_LIMIT: usize = 100;

/// Maximum limit for `ListEvents`.
const MAX_LIMIT: usize = 1000;

/// Maximum number of entries to scan in `CountEvents` to prevent
/// unbounded scans on large datasets.
const COUNT_SCAN_LIMIT: usize = 100_000;

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
    events_db: EventsDatabase<B>,

    /// Applied state for slug resolution.
    applied_state: AppliedStateAccessor,

    /// HMAC codec for page token signing/verification.
    page_token_codec: PageTokenCodec,

    /// Event configuration (for ingestion validation: TTL, details size, source allow-list).
    event_config: Option<Arc<EventConfig>>,

    /// Node ID for handler-phase event emission.
    node_id: Option<u64>,

    /// Per-source rate limiter for ingestion.
    ingestion_rate_limiter: Option<Arc<IngestionRateLimiter>>,

    /// Proposal service for routing `IngestExternalEvents` through Raft.
    ///
    /// When set, `ingest_events` proposes the accepted
    /// batch as [`OrganizationRequest::IngestExternalEvents`] against the REGIONAL
    /// Raft group owning the organization, instead of writing directly to
    /// `events.db`. When `None`, ingestion is disabled (the RPC returns
    /// `UNAVAILABLE`).
    proposer: Option<Arc<dyn ProposalService>>,

    /// Multi-Raft manager, used to resolve an organization to its REGIONAL
    /// Raft group for `IngestExternalEvents` proposal routing.
    manager: Option<Arc<RaftManager>>,

    /// Resolver for per-vault events stores registered on the local node.
    ///
    /// Powers [`list_events`](Self::list_events) fan-out across the
    /// org-level and per-vault events stores. When unset, `ListEvents`
    /// reads only the org-level store (`events_db`).
    vault_event_sources: Option<VaultEventSources<B>>,

    /// Maximum time to wait for an `IngestExternalEvents` Raft proposal to commit.
    #[builder(default = Duration::from_secs(30))]
    proposal_timeout: Duration,
}

impl<B: StorageBackend + 'static> EventsService<B> {
    /// Returns the page token codec reference (for tests).
    #[cfg(test)]
    pub fn codec(&self) -> &PageTokenCodec {
        &self.page_token_codec
    }
}

/// Converts optional proto `Timestamp` to nanoseconds, or returns a boundary
/// default (0 for start, `u64::MAX` for end).
fn timestamp_to_ns(ts: &Option<prost_types::Timestamp>, default: u64) -> u64 {
    match ts {
        Some(t) => {
            let secs = t.seconds.max(0) as u64;
            let nanos = t.nanos.max(0) as u64;
            secs.saturating_mul(1_000_000_000).saturating_add(nanos)
        },
        None => default,
    }
}

/// Computes a deterministic query hash from filter parameters.
fn compute_filter_hash(filter: &Option<proto::EventFilter>) -> [u8; 8] {
    let mut params = String::with_capacity(256);
    if let Some(f) = filter {
        if let Some(st) = &f.start_time {
            params.push_str(&format!("start:{}.{},", st.seconds, st.nanos));
        }
        if let Some(et) = &f.end_time {
            params.push_str(&format!("end:{}.{},", et.seconds, et.nanos));
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
        if f.outcome != 0 {
            params.push_str(&format!("outcome:{},", f.outcome));
        }
        if f.emission_path != 0 {
            params.push_str(&format!("emission:{},", f.emission_path));
        }
        if let Some(cid) = &f.correlation_id {
            params.push_str(&format!("correlation:{cid},"));
        }
        if let Some(vault) = &f.vault {
            params.push_str(&format!("vault:{},", vault.slug));
        }
    }
    PageTokenCodec::compute_query_hash(params.as_bytes())
}

/// Checks whether an event entry matches the given filter criteria (in-memory).
fn matches_filter(
    entry: &inferadb_ledger_types::events::EventEntry,
    filter: &Option<proto::EventFilter>,
) -> bool {
    let Some(f) = filter else {
        return true;
    };

    // Action filter: if non-empty, entry's action must be in the list
    if !f.actions.is_empty() {
        let action_str = entry.action.as_str();
        if !f.actions.iter().any(|a| a == action_str) {
            return false;
        }
    }

    // Event type prefix filter: case-sensitive starts_with
    if let Some(prefix) = &f.event_type_prefix
        && !prefix.is_empty()
        && !entry.event_type.starts_with(prefix.as_str())
    {
        return false;
    }

    // Principal filter
    if let Some(principal) = &f.principal
        && !principal.is_empty()
        && entry.principal != *principal
    {
        return false;
    }

    // Outcome filter (0 = UNSPECIFIED = match all)
    if f.outcome != 0 {
        let entry_outcome: i32 = proto::EventOutcome::from(&entry.outcome).into();
        if entry_outcome != f.outcome {
            return false;
        }
    }

    // Emission path filter (0 = UNSPECIFIED = match all)
    if f.emission_path != 0 {
        let entry_emission: i32 = proto::EventEmissionPath::from(&entry.emission).into();
        if entry_emission != f.emission_path {
            return false;
        }
    }

    // Correlation ID filter
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

#[tonic::async_trait]
impl<B: StorageBackend + 'static> proto::events_service_server::EventsService for EventsService<B> {
    /// Lists events with pagination and optional filtering by action, type, or time range.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    ///
    /// ## Fan-out
    ///
    /// Events for an organization live in two kinds of stores:
    ///
    /// 1. The **org-level** events store ([`Self::events_db`]) — written by apply paths that emit
    ///    org-scoped events (e.g. `CreateVault`, `AddOrganizationMember`).
    /// 2. **Per-vault** events stores — one per vault registered on this node, written by apply
    ///    paths that emit vault-scoped events (e.g. `Write`, `IngestEvents`).
    ///
    /// When [`EventFilter::vault`](proto::EventFilter::vault) is set the
    /// query routes to that vault's per-vault store and skips the org-level
    /// store. Otherwise it fans out across the org-level store and every
    /// per-vault store registered for the organization, merges by
    /// `(timestamp_ns, event_id)`, and slices to the requested page size.
    /// Pagination uses a global cursor (the storage key includes
    /// `org_id || timestamp_ns || event_hash` so a single cursor advances
    /// every source consistently).
    async fn list_events(
        &self,
        request: Request<proto::ListEventsRequest>,
    ) -> Result<Response<proto::ListEventsResponse>, Status> {
        let req = request.into_inner();

        // Resolve organization (with system bypass for slug=0)
        let resolver = SlugResolver::new(self.applied_state.clone());
        let org_id = resolver.extract_and_resolve_for_events(&req.organization)?;

        // Compute limit
        let limit =
            if req.limit == 0 { DEFAULT_LIMIT } else { (req.limit as usize).min(MAX_LIMIT) };

        // Convert time filter to nanosecond bounds
        let start_ns = timestamp_to_ns(&req.filter.as_ref().and_then(|f| f.start_time), 0);
        let end_ns = timestamp_to_ns(&req.filter.as_ref().and_then(|f| f.end_time), u64::MAX);

        // Compute query hash for token validation. Includes the vault filter
        // so a switch from "all vaults" to a specific vault rejects a stale
        // page token.
        let query_hash = compute_filter_hash(&req.filter);

        // Decode and validate page token if provided
        let resume_key = if req.page_token.is_empty() {
            None
        } else {
            let token = self
                .page_token_codec
                .decode_event(&req.page_token)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            self.page_token_codec
                .validate_event_context(&token, org_id, query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            Some(token.last_key)
        };

        // Build the list of events stores to read from.
        //
        // Vault-filtered queries route to a single per-vault store (Case A);
        // org-scoped queries fan out across the org-level store and every
        // per-vault store registered for the organization (Case B). When no
        // `vault_event_sources` resolver is wired (test fixtures, legacy
        // single-store deployments), the handler reads only the org-level
        // store — the existing org-level results are preserved.
        let sources: Vec<EventsDatabase<B>> = match (
            req.filter.as_ref().and_then(|f| f.vault.as_ref()),
            self.vault_event_sources.as_ref(),
        ) {
            (Some(vault_proto), Some(resolver_fn)) => {
                let vault_slug = SlugResolver::extract_vault_slug(&Some(*vault_proto))?;
                let (owning_org, vault_id) = resolver.resolve_vault_pair(vault_slug)?;
                if owning_org != org_id {
                    return Err(Status::not_found(format!(
                        "Vault {} does not belong to organization {}",
                        vault_slug.value(),
                        org_id.value(),
                    )));
                }
                let mut entries = resolver_fn(org_id);
                let matched = entries.iter().position(|s| s.vault_id == vault_id);
                match matched {
                    Some(idx) => vec![entries.swap_remove(idx).events_db],
                    None => {
                        // Vault is not registered on this node. Returning an
                        // empty page (rather than 404) matches the read-path
                        // contract: any node may serve `ListEvents`, but only
                        // nodes hosting the vault group's per-vault events.db
                        // can return its events. The SDK's region-leader cache
                        // will redirect callers to the right node.
                        Vec::new()
                    },
                }
            },
            (Some(_vault_proto), None) => {
                // Vault filter set but no fan-out resolver wired. The legacy
                // single-store path cannot honour a vault filter.
                return Err(Status::failed_precondition(
                    "vault filter is unsupported on this deployment",
                ));
            },
            (None, Some(resolver_fn)) => {
                let mut all = Vec::with_capacity(8);
                all.push(self.events_db.clone());
                for src in resolver_fn(org_id) {
                    all.push(src.events_db);
                }
                all
            },
            (None, None) => vec![self.events_db.clone()],
        };

        // Read from every source. We over-fetch each source by `limit + 1`
        // entries past the cursor — enough to populate one full output page
        // after merge + filter, with one extra entry to detect "more
        // available". Per-source over-fetch beyond `limit` would only matter
        // if a single source dominates the merged output; in the common case
        // (events sprinkled across sources) this bound is loose enough.
        let mut merged: Vec<EventEntry> = Vec::with_capacity(limit.saturating_add(8));
        let mut any_source_has_more = false;
        let batch_size = limit.saturating_mul(4).max(256);

        for source in &sources {
            let txn = source.read().map_err(|e| error_classify::storage_error(&e))?;
            let mut cursor = resume_key.clone();
            let mut filtered_from_source = 0usize;
            let mut source_exhausted = true;

            loop {
                let (batch, next_cursor) =
                    EventStore::list(&txn, org_id, start_ns, end_ns, batch_size, cursor.as_deref())
                        .map_err(|e| error_classify::storage_error(&e))?;

                if batch.is_empty() {
                    break;
                }

                for entry in batch {
                    if matches_filter(&entry, &req.filter) {
                        merged.push(entry);
                        filtered_from_source += 1;
                    }
                }

                // Stop once this source has produced enough post-filter
                // entries to be sure it can't displace any of the first
                // `limit` events in the merged output. If the source still
                // has more rows after that, we don't know whether they'd
                // change the merge order, so we mark "more available".
                if filtered_from_source >= limit {
                    source_exhausted = next_cursor.is_none();
                    break;
                }

                if next_cursor.is_none() {
                    break;
                }
                cursor = next_cursor;
            }

            if !source_exhausted {
                any_source_has_more = true;
            }
        }

        // Sort merged entries by storage-key order, which is
        // `(org_id, timestamp_ns, event_hash)`. All events share the same
        // org_id within a single query, so this is effectively a
        // chronological sort with a stable tie-breaker on `event_hash`.
        merged.sort_by(|a, b| {
            let a_ts = a.timestamp.timestamp_nanos_opt().unwrap_or(i64::MAX) as u64;
            let b_ts = b.timestamp.timestamp_nanos_opt().unwrap_or(i64::MAX) as u64;
            (a_ts, seahash::hash(&a.event_id)).cmp(&(b_ts, seahash::hash(&b.event_id)))
        });

        let has_more = merged.len() > limit || any_source_has_more;
        merged.truncate(limit);

        // Build next page token from the last returned entry. Per-source
        // `EventStore::list` accepts the same cursor unchanged because the
        // storage key incorporates `org_id || timestamp_ns || event_hash`,
        // and every source for this query shares the same `org_id`.
        let next_page_token = if has_more {
            merged.last().map(|entry| {
                let ts_ns = entry.timestamp.timestamp_nanos_opt().unwrap_or_else(|| {
                    tracing::warn!(timestamp = ?entry.timestamp, "Timestamp nanos overflow — using max sentinel");
                    i64::MAX
                }) as u64;
                let key = inferadb_ledger_state::encode_event_key(
                    entry.organization_id,
                    ts_ns,
                    &entry.event_id,
                );
                let token =
                    EventPageToken { version: 1, organization: org_id, last_key: key, query_hash };
                self.page_token_codec.encode_event(&token)
            })
        } else {
            None
        };

        // Convert domain entries to proto
        let entries: Vec<proto::EventEntry> = merged.iter().map(proto::EventEntry::from).collect();

        tracing::info!(
            service = "EventsService",
            method = "ListEvents",
            org_id = org_id.value(),
            returned = entries.len(),
            sources = sources.len(),
            has_more = has_more,
            "Events query completed"
        );

        Ok(Response::new(proto::ListEventsResponse {
            entries,
            next_page_token: next_page_token.unwrap_or_default(),
            total_estimate: None,
        }))
    }

    /// Retrieves a single event by its 16-byte event ID.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn get_event(
        &self,
        request: Request<proto::GetEventRequest>,
    ) -> Result<Response<proto::GetEventResponse>, Status> {
        let req = request.into_inner();

        // Resolve organization (with system bypass)
        let resolver = SlugResolver::new(self.applied_state.clone());
        let org_id = resolver.extract_and_resolve_for_events(&req.organization)?;

        // Validate event_id length
        if req.event_id.len() != 16 {
            return Err(Status::invalid_argument(format!(
                "event_id must be 16 bytes, got {}",
                req.event_id.len()
            )));
        }

        // O(log n) index lookup: (org_id, event_id) → primary key → entry.
        let txn = self.events_db.read().map_err(|e| error_classify::storage_error(&e))?;

        let mut event_id_arr = [0u8; 16];
        event_id_arr.copy_from_slice(&req.event_id);

        let found = EventStore::get_by_id(&txn, org_id, &event_id_arr)
            .map_err(|e| error_classify::storage_error(&e))?;

        match found {
            Some(entry) => {
                tracing::info!(
                    service = "EventsService",
                    method = "GetEvent",
                    org_id = org_id.value(),
                    found = true,
                    "Event lookup completed"
                );
                Ok(Response::new(proto::GetEventResponse {
                    entry: Some(proto::EventEntry::from(&entry)),
                }))
            },
            None => Err(Status::not_found("Event not found")),
        }
    }

    /// Counts events matching optional filter criteria (action, type, time range).
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn count_events(
        &self,
        request: Request<proto::CountEventsRequest>,
    ) -> Result<Response<proto::CountEventsResponse>, Status> {
        let req = request.into_inner();

        // Resolve organization (with system bypass)
        let resolver = SlugResolver::new(self.applied_state.clone());
        let org_id = resolver.extract_and_resolve_for_events(&req.organization)?;

        // Convert time filter to nanosecond bounds
        let start_ns = timestamp_to_ns(&req.filter.as_ref().and_then(|f| f.start_time), 0);
        let end_ns = timestamp_to_ns(&req.filter.as_ref().and_then(|f| f.end_time), u64::MAX);

        let txn = self.events_db.read().map_err(|e| error_classify::storage_error(&e))?;

        // If no filters beyond time range, use the fast count path
        let has_memory_filters = req.filter.as_ref().is_some_and(|f| {
            !f.actions.is_empty()
                || f.event_type_prefix.as_ref().is_some_and(|p| !p.is_empty())
                || f.principal.as_ref().is_some_and(|p| !p.is_empty())
                || f.outcome != 0
                || f.emission_path != 0
                || f.correlation_id.as_ref().is_some_and(|c| !c.is_empty())
        });

        let count = if !has_memory_filters && start_ns == 0 && end_ns == u64::MAX {
            // Fast path: count all events for the org
            EventStore::count(&txn, org_id).map_err(|e| error_classify::storage_error(&e))?
        } else {
            // Slow path: scan with filters
            let mut count = 0u64;
            let mut cursor: Option<Vec<u8>> = None;
            let batch_size = 1000;

            loop {
                let (batch, next_cursor) =
                    EventStore::list(&txn, org_id, start_ns, end_ns, batch_size, cursor.as_deref())
                        .map_err(|e| error_classify::storage_error(&e))?;

                if batch.is_empty() {
                    break;
                }

                for entry in &batch {
                    if matches_filter(entry, &req.filter) {
                        count += 1;
                    }
                }

                if next_cursor.is_none() || count >= COUNT_SCAN_LIMIT as u64 {
                    break;
                }

                cursor = next_cursor;
            }

            count
        };

        tracing::info!(
            service = "EventsService",
            method = "CountEvents",
            org_id = org_id.value(),
            count = count,
            "Event count completed"
        );

        Ok(Response::new(proto::CountEventsResponse { count }))
    }

    /// Ingests a batch of external events from an authorized source service.
    ///
    /// Validates the source against the configured allow-list and enforces batch size limits.
    async fn ingest_events(
        &self,
        request: Request<proto::IngestEventsRequest>,
    ) -> Result<Response<proto::IngestEventsResponse>, Status> {
        let start = std::time::Instant::now();
        let req = request.into_inner();

        // Require ingestion infrastructure to be wired
        let config = self.event_config.as_ref().ok_or_else(|| {
            Status::unavailable("Events ingestion is not configured on this node")
        })?;
        let node_id = self.node_id.ok_or_else(|| {
            Status::unavailable("Events ingestion is not configured on this node")
        })?;

        let source = &req.source_service;

        // 1. Check master switch
        if !config.ingestion.ingest_enabled {
            tracing::info!(
                service = "EventsService",
                method = "IngestEvents",
                source_service = %source,
                "Ingestion disabled"
            );
            return Err(Status::unavailable("Event ingestion is currently disabled"));
        }

        // 2. Validate source_service against allow-list
        if !config.ingestion.allowed_sources.iter().any(|s| s == source) {
            tracing::warn!(
                service = "EventsService",
                method = "IngestEvents",
                source_service = %source,
                "Unknown source service rejected"
            );
            return Err(Status::permission_denied(format!(
                "Source service '{source}' is not in the allowed sources list"
            )));
        }

        // 3. Validate batch size
        let batch_size = req.entries.len();
        if batch_size > config.ingestion.max_ingest_batch_size as usize {
            return Err(Status::invalid_argument(format!(
                "Batch size {batch_size} exceeds maximum {}",
                config.ingestion.max_ingest_batch_size
            )));
        }

        // Record batch size metric
        metrics::record_events_ingest_batch_size(source, batch_size);

        // 4. Empty batch is valid — return immediately
        if batch_size == 0 {
            let duration = start.elapsed().as_secs_f64();
            metrics::record_events_ingest_duration(duration);
            tracing::info!(
                service = "EventsService",
                method = "IngestEvents",
                source_service = %source,
                batch_size = 0,
                accepted = 0,
                rejected = 0,
                duration_ms = format!("{:.2}", duration * 1000.0),
                "IngestEvents completed (empty batch)"
            );
            return Ok(Response::new(proto::IngestEventsResponse {
                accepted_count: 0,
                rejected_count: 0,
                rejections: vec![],
            }));
        }

        // 5. Per-source rate limiting
        if let Some(limiter) = &self.ingestion_rate_limiter
            && !limiter.check(source, batch_size as u32)
        {
            metrics::record_events_ingest_rate_limited(source);
            let duration = start.elapsed().as_secs_f64();
            metrics::record_events_ingest_duration(duration);
            tracing::warn!(
                service = "EventsService",
                method = "IngestEvents",
                source_service = %source,
                batch_size = batch_size,
                "Rate limited"
            );
            return Err(Status::resource_exhausted(format!(
                "Rate limit exceeded for source service '{source}'"
            )));
        }

        // 6. Resolve organization
        let resolver = SlugResolver::new(self.applied_state.clone());
        let org_id = resolver.extract_and_resolve_for_events(&req.organization)?;
        let organization = req.organization.as_ref().map(|o| OrganizationSlug::new(o.slug));

        // 7. Process each entry — validate and convert to EventEntry.
        //
        // Each accepted entry is paired with its target `Option<VaultId>`
        // routing key so the post-loop bucketing step (#8) can split the
        // batch into one per-shard proposal per distinct vault. `None`
        // means the entry has no vault (org-level) and routes through the
        // standard org propose path; `Some(vault_id)` routes through the
        // per-vault shard.
        let now = Utc::now();
        let ttl_days = config.default_ttl_days;
        let max_details_bytes = config.max_details_size_bytes;
        // Routing key carries both the internal `VaultId` (used for per-vault
        // group lookup) and the external `VaultSlug` (forwarded into the
        // `NotLeader` `LeaderHint` so the SDK's `VaultLeaderCache` — keyed
        // on `(OrganizationSlug, VaultSlug)` — populates without a resolve
        // round-trip).
        let mut accepted_entries: Vec<(Option<(VaultId, VaultSlug)>, EventEntry)> =
            Vec::with_capacity(batch_size);
        let mut rejections: Vec<proto::RejectedEvent> = Vec::new();

        for (idx, proto_entry) in req.entries.iter().enumerate() {
            // Validate required fields first (before prefix check, so empty
            // event_type gets a clear "is required" message instead of the
            // confusing "must start with 'engine.'" error).
            if proto_entry.event_type.is_empty() {
                rejections.push(proto::RejectedEvent {
                    index: idx as u32,
                    reason: "event_type is required".to_string(),
                });
                continue;
            }
            if proto_entry.principal.is_empty() {
                rejections.push(proto::RejectedEvent {
                    index: idx as u32,
                    reason: "principal is required".to_string(),
                });
                continue;
            }

            // Validate event_type prefix
            let expected_prefix = format!("{}.", source);
            if !proto_entry.event_type.starts_with(&expected_prefix) {
                rejections.push(proto::RejectedEvent {
                    index: idx as u32,
                    reason: format!(
                        "event_type '{}' must start with '{}'",
                        proto_entry.event_type, expected_prefix
                    ),
                });
                continue;
            }

            // Validate details size
            let details_size: usize =
                proto_entry.details.iter().map(|(k, v)| k.len() + v.len()).sum();
            if details_size > max_details_bytes {
                rejections.push(proto::RejectedEvent {
                    index: idx as u32,
                    reason: format!(
                        "details map size {details_size} bytes exceeds maximum {max_details_bytes}"
                    ),
                });
                continue;
            }

            // Convert proto outcome to domain outcome
            let outcome = match proto::EventOutcome::try_from(proto_entry.outcome) {
                Ok(proto::EventOutcome::Success) => EventOutcome::Success,
                Ok(proto::EventOutcome::Failed) => EventOutcome::Failed {
                    code: proto_entry.error_code.clone().unwrap_or_default(),
                    detail: proto_entry.error_detail.clone().unwrap_or_default(),
                },
                Ok(proto::EventOutcome::Denied) => EventOutcome::Denied {
                    reason: proto_entry.denial_reason.clone().unwrap_or_default(),
                },
                _ => {
                    rejections.push(proto::RejectedEvent {
                        index: idx as u32,
                        reason: format!("invalid outcome value: {}", proto_entry.outcome),
                    });
                    continue;
                },
            };

            // Determine timestamp
            let timestamp = proto_entry
                .timestamp
                .as_ref()
                .and_then(|ts| chrono::DateTime::from_timestamp(ts.seconds, ts.nanos.max(0) as u32))
                .unwrap_or(now);

            // Compute expiry
            let expires_at = if ttl_days == 0 {
                0
            } else {
                let duration = chrono::Duration::days(i64::from(ttl_days));
                let expiry = timestamp + duration;
                expiry.timestamp() as u64
            };

            // Generate random UUID v4
            let event_id = *Uuid::new_v4().as_bytes();

            // Convert details from HashMap to BTreeMap
            let details: BTreeMap<String, String> =
                proto_entry.details.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

            // Determine scope — ingested events are always organization-scope
            // (external services don't emit system events)
            let scope = EventScope::Organization;

            // Use a generic action for ingested events — they carry their
            // semantics in event_type, not in EventAction. We use the closest
            // match: WriteCommitted for success, RequestValidationFailed for
            // failures, RequestRateLimited for denials.
            let action = match &outcome {
                EventOutcome::Success => EventAction::WriteCommitted,
                EventOutcome::Failed { .. } => EventAction::RequestValidationFailed,
                EventOutcome::Denied { .. } => EventAction::RequestRateLimited,
            };

            // Resolve the optional vault slug to a routing key. A non-zero
            // slug must (a) exist in the slug index and (b) belong to the
            // resolved organization — accepting a vault from a different
            // org would route the entry to a foreign per-vault shard and
            // leak cross-org data through Raft replication.
            let vault_slug_opt = proto_entry.vault.as_ref().map(|v| VaultSlug::new(v.slug));
            let vault_routing = match vault_slug_opt {
                None => None,
                Some(slug) if slug.value() == 0 => {
                    rejections.push(proto::RejectedEvent {
                        index: idx as u32,
                        reason: "vault.slug must be non-zero".to_string(),
                    });
                    continue;
                },
                Some(slug) => match resolver.resolve_vault_pair(slug) {
                    Ok((owning_org, vault_id)) if owning_org == org_id => Some((vault_id, slug)),
                    Ok((owning_org, _)) => {
                        rejections.push(proto::RejectedEvent {
                            index: idx as u32,
                            reason: format!(
                                "vault {} belongs to organization {} not {}",
                                slug.value(),
                                owning_org,
                                org_id
                            ),
                        });
                        continue;
                    },
                    Err(_) => {
                        rejections.push(proto::RejectedEvent {
                            index: idx as u32,
                            reason: format!("vault with slug {} not found", slug.value()),
                        });
                        continue;
                    },
                },
            };

            let entry = EventEntry {
                expires_at,
                event_id,
                source_service: source.clone(),
                event_type: proto_entry.event_type.clone(),
                timestamp,
                scope,
                action,
                emission: EventEmission::HandlerPhase { node_id },
                principal: proto_entry.principal.clone(),
                organization_id: org_id,
                organization,
                vault: vault_slug_opt,
                outcome,
                details,
                block_height: None,
                trace_id: proto_entry.trace_id.clone(),
                correlation_id: proto_entry.correlation_id.clone(),
                operations_count: None,
            };

            accepted_entries.push((vault_routing, entry));
        }

        // 8. Route the accepted batch through one Raft proposal per distinct target shard.
        //    Org-level entries (no `vault_id`) route through the per-org group via
        //    `propose_to_organization_bytes`; entries with a `vault_id` route through the per-vault
        //    shard via `propose_to_vault_bytes`. The apply-side classifier
        //    (`raft_manager::classify_request`) treats `IngestExternalEvents` as `VaultScoped`, so
        //    vault-scoped batches must arrive pre-bucketed at the per-vault apply pipeline.
        //
        //    Rejections + accepted count are known locally — apply
        //    responses are just acks.
        //
        //    Bucketing uses a `BTreeMap<Option<VaultId>, _>` so per-bucket
        //    proposal ordering is deterministic across retries — a key
        //    requirement for idempotency, since each per-vault propose
        //    has its own idempotency cache. The serialized payload bytes
        //    for a given `(source, vault_routing)` pair are identical
        //    on retry because (a) the bucket order is fixed and (b)
        //    entries within a bucket preserve original-request order.
        let accepted_count = accepted_entries.len() as u32;
        let rejected_count = rejections.len() as u32;
        let caller = req.caller.as_ref().map_or(0, |c| c.slug);
        let correlation_request_id = uuid::Uuid::new_v4();
        let correlation_trace_id = String::new();

        if !accepted_entries.is_empty() {
            let proposer = self.proposer.as_ref().cloned().ok_or_else(|| {
                Status::unavailable("Event ingestion proposer is not configured on this node")
            })?;

            // Resolve the REGIONAL Raft group for this organization. External
            // EventEntry payloads carry user-controlled strings (principal,
            // event_type, details map) that are PII and must stay within the
            // owning region's Raft log + events.db. Never route to GLOBAL.
            //
            // In production, `manager` is always wired. Tests that inject
            // only a `proposer` (no `manager`) fall back to a stub region —
            // the `Region` value is inspected by the test mock for routing
            // assertions, but the mock does not consult a real routing
            // table.
            let region = match self.manager.as_ref() {
                Some(manager) => manager.get_organization_region(org_id).ok_or_else(|| {
                    Status::not_found(format!("Organization {} not found in routing table", org_id))
                })?,
                None => inferadb_ledger_types::Region::GLOBAL,
            };

            // Bucket accepted entries by routing key. `BTreeMap` gives
            // deterministic iteration order: `None` (org-level) first,
            // then `Some((vault_id, _))` ascending by vault id — stable
            // across retries. The slug travels alongside the id so the
            // post-loop proposal can forward it into `NotLeader` hints.
            let mut buckets: BTreeMap<Option<(VaultId, VaultSlug)>, Vec<EventEntry>> =
                BTreeMap::new();
            for (vault_routing, entry) in accepted_entries {
                buckets.entry(vault_routing).or_default().push(entry);
            }

            let timeout = self.proposal_timeout;

            // Sequential per-bucket propose. First-error-wins matches the
            // pre-refactor "all-or-nothing" semantics — partial success on
            // mixed-vault batches is not exposed in the response shape.
            // Concurrent proposals are a possible future optimization;
            // they need careful idempotency / partial-failure handling
            // that is out of scope for this slice.
            for (vault_routing, events) in buckets {
                let payload = crate::proposal::serialize_payload(
                    inferadb_ledger_raft::types::RaftPayload::new(
                        OrganizationRequest::IngestExternalEvents {
                            source: source.clone(),
                            events,
                        },
                        caller,
                    ),
                )
                .map_err(|status| {
                    status_with_correlation(status, &correlation_request_id, &correlation_trace_id)
                })?;

                let response = match vault_routing {
                    Some((vault_id, vault_slug)) => proposer
                        .propose_to_vault_bytes(
                            region,
                            org_id,
                            vault_id,
                            // org slug comes from the inbound request (already validated above).
                            req.organization.as_ref().map(|o| o.slug),
                            Some(vault_slug.value()),
                            payload,
                            timeout,
                        )
                        .await
                        .map_err(|status| {
                            status_with_correlation(
                                status,
                                &correlation_request_id,
                                &correlation_trace_id,
                            )
                        })?,
                    None => proposer
                        .propose_to_organization_bytes(region, org_id, payload, timeout)
                        .await
                        .map_err(|status| {
                            status_with_correlation(
                                status,
                                &correlation_request_id,
                                &correlation_trace_id,
                            )
                        })?,
                };

                match response {
                    LedgerResponse::Empty => {},
                    LedgerResponse::Error { code, message } => {
                        tracing::error!(
                            service = "EventsService",
                            method = "IngestEvents",
                            source_service = %source,
                            org_id = org_id.value(),
                            vault_id = vault_routing.map(|v| v.0.value()).unwrap_or(0),
                            error_code = ?code,
                            "IngestExternalEvents apply returned error"
                        );
                        return Err(status_with_correlation(
                            super::helpers::error_code_to_status(code, message),
                            &correlation_request_id,
                            &correlation_trace_id,
                        ));
                    },
                    other => {
                        tracing::error!(
                            service = "EventsService",
                            method = "IngestEvents",
                            vault_id = vault_routing.map(|v| v.0.value()).unwrap_or(0),
                            response = ?std::mem::discriminant(&other),
                            "IngestExternalEvents returned unexpected response variant"
                        );
                        return Err(status_with_correlation(
                            Status::internal(
                                "Unexpected response type from IngestExternalEvents apply handler",
                            ),
                            &correlation_request_id,
                            &correlation_trace_id,
                        ));
                    },
                }
            }
        }

        // 9. Record ingestion metrics (RPC-handler site, unchanged). Per-event
        //    `ledger_event_writes_total` now fires on the apply side (see design § Observability).
        metrics::record_events_ingest(source, "accepted", accepted_count);
        if rejected_count > 0 {
            metrics::record_events_ingest(source, "rejected", rejected_count);
        }
        let duration = start.elapsed().as_secs_f64();
        metrics::record_events_ingest_duration(duration);

        // 10. Canonical log line
        tracing::info!(
            service = "EventsService",
            method = "IngestEvents",
            source_service = %source,
            org_id = org_id.value(),
            batch_size = batch_size,
            accepted = accepted_count,
            rejected = rejected_count,
            duration_ms = format!("{:.2}", duration * 1000.0),
            "IngestEvents completed"
        );

        Ok(Response::new(proto::IngestEventsResponse {
            accepted_count,
            rejected_count,
            rejections,
        }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::Arc,
        time::Duration,
    };

    use arc_swap::ArcSwap;
    use chrono::{DateTime, TimeZone, Utc};
    use inferadb_ledger_proto::proto::events_service_server::EventsService as EventsServiceProto;
    use inferadb_ledger_raft::{
        event_writer::IngestionRateLimiter,
        log_storage::{AppliedState, AppliedStateAccessor},
    };
    use inferadb_ledger_state::{EventStore, EventsDatabase};
    use inferadb_ledger_store::InMemoryBackend;
    use inferadb_ledger_types::{
        OrganizationId, Region,
        events::{
            EventAction, EventConfig, EventEmission, EventEntry, EventOutcome, EventScope,
            IngestionConfig,
        },
    };
    use parking_lot::Mutex;

    use super::*;

    /// Test double that stands in for the apply-handler side of the
    /// `IngestExternalEvents` flow.
    ///
    /// - Captures every `propose_to_organization_bytes` and `propose_to_vault_bytes` call for
    ///   assertion. Each captured entry records the routing target (`Option<VaultId>`) so tests can
    ///   assert on per-vault bucketing.
    /// - Simulates the apply handler's write-through-and-ack by deserializing the proposed bytes
    ///   back to `OrganizationRequest`, writing the `EventEntry` batch into the shared `events_db`
    ///   handle, and returning `Ok(LedgerResponse::Empty)`.
    /// - Optionally returns a canned error response (`set_next_error`) so tests can exercise the
    ///   apply-error → gRPC-error propagation path. Errors are consumed in propose-call order
    ///   across both org and vault routing arms.
    struct TestIngestProposer {
        events_db: EventsDatabase<InMemoryBackend>,
        /// Captured `(region, vault_routing, request_bytes)` tuples for
        /// post-call assertions. `vault_routing == None` means the call
        /// went through `propose_to_organization_bytes`; `Some(vault_id)`
        /// means it went through `propose_to_vault_bytes`.
        proposals: Mutex<Vec<(Region, Option<inferadb_ledger_types::VaultId>, Vec<u8>)>>,
        next_error: Mutex<VecDeque<Status>>,
    }

    impl TestIngestProposer {
        fn new(events_db: EventsDatabase<InMemoryBackend>) -> Self {
            Self {
                events_db,
                proposals: Mutex::new(Vec::new()),
                next_error: Mutex::new(VecDeque::new()),
            }
        }

        fn proposals(&self) -> Vec<(Region, Option<inferadb_ledger_types::VaultId>, Vec<u8>)> {
            self.proposals.lock().clone()
        }

        /// Deserializes captured proposal bytes to `OrganizationRequest` for assertion.
        fn decode_proposals(
            &self,
        ) -> Vec<(Region, Option<inferadb_ledger_types::VaultId>, OrganizationRequest)> {
            self.proposals
                .lock()
                .iter()
                .map(|(region, vault_routing, bytes)| {
                    let payload: inferadb_ledger_raft::types::RaftPayload<OrganizationRequest> =
                        postcard::from_bytes(bytes).expect("decode proposal bytes");
                    (*region, *vault_routing, payload.request)
                })
                .collect()
        }

        /// Enqueues a `tonic::Status` that the next propose call (org or
        /// vault arm) will return instead of simulating a successful apply.
        fn set_next_error(&self, status: Status) {
            self.next_error.lock().push_back(status);
        }

        /// Apply-side simulation shared between the org and vault arms.
        ///
        /// Decodes the payload as `OrganizationRequest::IngestExternalEvents`
        /// and writes its `EventEntry` batch through to the shared
        /// `events_db`, mirroring what the real apply pipeline does on
        /// commit. Returns `Ok(LedgerResponse::Empty)` to ack the propose.
        fn simulate_apply(&self, bytes: &[u8]) -> Result<LedgerResponse, Status> {
            let payload: inferadb_ledger_raft::types::RaftPayload<OrganizationRequest> =
                postcard::from_bytes(bytes).expect("decode proposal bytes in test mock");
            if let OrganizationRequest::IngestExternalEvents { ref events, .. } = payload.request {
                let mut txn = self.events_db.write().expect("test write txn");
                for entry in events.iter() {
                    EventStore::write(&mut txn, entry).expect("test write event");
                }
                txn.commit().expect("test commit");
            }
            Ok(LedgerResponse::Empty)
        }
    }

    #[tonic::async_trait]
    impl ProposalService for TestIngestProposer {
        async fn propose_bytes(
            &self,
            _bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, Status> {
            Err(Status::unimplemented("TestIngestProposer only supports propose_to_region_bytes"))
        }

        async fn propose_to_region_bytes(
            &self,
            _region: Region,
            _bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, Status> {
            Err(Status::unimplemented(
                "TestIngestProposer only supports propose_to_organization_bytes",
            ))
        }

        async fn propose_to_organization_bytes(
            &self,
            region: Region,
            _organization: OrganizationId,
            bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, Status> {
            if let Some(err) = self.next_error.lock().pop_front() {
                self.proposals.lock().push((region, None, bytes));
                return Err(err);
            }
            let response = self.simulate_apply(&bytes);
            self.proposals.lock().push((region, None, bytes));
            response
        }

        async fn propose_to_vault_bytes(
            &self,
            region: Region,
            _organization: OrganizationId,
            vault_id: inferadb_ledger_types::VaultId,
            _organization_slug: Option<u64>,
            _vault_slug: Option<u64>,
            bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, Status> {
            if let Some(err) = self.next_error.lock().pop_front() {
                self.proposals.lock().push((region, Some(vault_id), bytes));
                return Err(err);
            }
            let response = self.simulate_apply(&bytes);
            self.proposals.lock().push((region, Some(vault_id), bytes));
            response
        }

        async fn propose_organization_request_to_vault(
            &self,
            _region: Region,
            _organization: OrganizationId,
            _vault_id: inferadb_ledger_types::VaultId,
            _organization_slug: Option<u64>,
            _vault_slug: Option<u64>,
            _request: OrganizationRequest,
            _caller: u64,
            _timeout: Duration,
        ) -> Result<LedgerResponse, Status> {
            Err(Status::unimplemented("TestIngestProposer does not support typed vault proposals"))
        }

        fn regional_state(
            &self,
            _region: Region,
        ) -> Result<
            Arc<inferadb_ledger_state::StateLayer<inferadb_ledger_store::FileBackend>>,
            Status,
        > {
            Err(Status::unimplemented("TestIngestProposer does not expose a regional state layer"))
        }
    }

    fn make_test_service() -> EventsService<InMemoryBackend> {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let state = AppliedState::default();
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));

        EventsService::builder()
            .events_db(events_db)
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .build()
    }

    fn make_service_with_org(slug: u64, org_id: i64) -> EventsService<InMemoryBackend> {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let mut state = AppliedState::default();
        state.slug_index.insert(
            inferadb_ledger_types::OrganizationSlug::new(slug),
            OrganizationId::new(org_id),
        );
        state.id_to_slug.insert(
            OrganizationId::new(org_id),
            inferadb_ledger_types::OrganizationSlug::new(slug),
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));

        EventsService::builder()
            .events_db(events_db)
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .build()
    }

    /// Builds a service wired for ingestion tests with default `IngestionConfig`.
    fn make_ingest_service(slug: u64, org_id: i64) -> EventsService<InMemoryBackend> {
        make_ingest_service_with_config(slug, org_id, IngestionConfig::default()).0
    }

    /// Builds a service + proposer pair for ingestion tests that need to
    /// inspect the captured proposals or inject an apply-error.
    fn make_ingest_service_with_proposer(
        slug: u64,
        org_id: i64,
    ) -> (EventsService<InMemoryBackend>, Arc<TestIngestProposer>) {
        make_ingest_service_with_config(slug, org_id, IngestionConfig::default())
    }

    /// Like [`make_ingest_service_with_proposer`] but additionally seeds the
    /// applied state's vault slug index with `vaults`, each tuple
    /// `(vault_slug, owning_org_id, vault_id)`. Tests that exercise the
    /// per-vault bucketing path use this to make `resolve_vault_pair`
    /// succeed against known slugs.
    fn make_ingest_service_with_vaults(
        slug: u64,
        org_id: i64,
        vaults: &[(u64, i64, i64)],
    ) -> (EventsService<InMemoryBackend>, Arc<TestIngestProposer>) {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let mut state = AppliedState::default();
        state.slug_index.insert(
            inferadb_ledger_types::OrganizationSlug::new(slug),
            OrganizationId::new(org_id),
        );
        state.id_to_slug.insert(
            OrganizationId::new(org_id),
            inferadb_ledger_types::OrganizationSlug::new(slug),
        );
        for &(vslug, owning_org, vid) in vaults {
            let key_slug = inferadb_ledger_types::VaultSlug::new(vslug);
            let pair = (OrganizationId::new(owning_org), inferadb_ledger_types::VaultId::new(vid));
            state.vault_slug_index.insert(key_slug, pair);
            state.vault_id_to_slug.insert(pair, key_slug);
        }
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));
        let ingestion = IngestionConfig::default();
        let rate_limit = ingestion.ingest_rate_limit_per_source;
        let config = EventConfig { ingestion, ..EventConfig::default() };

        let proposer = Arc::new(TestIngestProposer::new(events_db.clone()));
        let proposer_trait: Arc<dyn ProposalService> = proposer.clone();

        let service = EventsService::builder()
            .events_db(events_db)
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .event_config(Arc::new(config))
            .node_id(1_u64)
            .ingestion_rate_limiter(Arc::new(IngestionRateLimiter::new(rate_limit)))
            .proposer(proposer_trait)
            .proposal_timeout(Duration::from_secs(5))
            .build();
        (service, proposer)
    }

    /// Helper to build an `IngestEventEntry` carrying a `VaultSlug`.
    fn make_ingest_entry_with_vault(
        event_type: &str,
        principal: &str,
        vault_slug: u64,
    ) -> proto::IngestEventEntry {
        proto::IngestEventEntry {
            vault: Some(proto::VaultSlug { slug: vault_slug }),
            ..make_ingest_entry(event_type, principal)
        }
    }

    /// Builds a service with custom `IngestionConfig` for testing edge cases.
    /// Returns the service plus the test proposer so tests can assert on
    /// the captured proposal stream or inject an apply-handler error.
    fn make_ingest_service_with_config(
        slug: u64,
        org_id: i64,
        ingestion: IngestionConfig,
    ) -> (EventsService<InMemoryBackend>, Arc<TestIngestProposer>) {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let mut state = AppliedState::default();
        state.slug_index.insert(
            inferadb_ledger_types::OrganizationSlug::new(slug),
            OrganizationId::new(org_id),
        );
        state.id_to_slug.insert(
            OrganizationId::new(org_id),
            inferadb_ledger_types::OrganizationSlug::new(slug),
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));
        let rate_limit = ingestion.ingest_rate_limit_per_source;
        let config = EventConfig { ingestion, ..EventConfig::default() };

        let proposer = Arc::new(TestIngestProposer::new(events_db.clone()));
        let proposer_trait: Arc<dyn ProposalService> = proposer.clone();

        let service = EventsService::builder()
            .events_db(events_db)
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .event_config(Arc::new(config))
            .node_id(1_u64)
            .ingestion_rate_limiter(Arc::new(IngestionRateLimiter::new(rate_limit)))
            .proposer(proposer_trait)
            .proposal_timeout(Duration::from_secs(5))
            .build();
        (service, proposer)
    }

    /// Helper to build a simple `IngestEventEntry` with required fields.
    fn make_ingest_entry(event_type: &str, principal: &str) -> proto::IngestEventEntry {
        proto::IngestEventEntry {
            event_type: event_type.to_string(),
            principal: principal.to_string(),
            outcome: proto::EventOutcome::Success.into(),
            details: std::collections::HashMap::new(),
            trace_id: None,
            correlation_id: None,
            vault: None,
            timestamp: None,
            error_code: None,
            error_detail: None,
            denial_reason: None,
        }
    }

    fn make_entry(
        org_id: i64,
        event_id: [u8; 16],
        timestamp_secs: i64,
        action: EventAction,
    ) -> EventEntry {
        EventEntry {
            expires_at: 0,
            event_id,
            source_service: "ledger".to_string(),
            event_type: format!("ledger.{}", action.as_str()),
            timestamp: Utc
                .timestamp_opt(timestamp_secs, 0)
                .single()
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            scope: if org_id == 0 { EventScope::System } else { EventScope::Organization },
            action,
            emission: EventEmission::ApplyPhase,
            principal: "test-user".to_string(),
            organization_id: OrganizationId::new(org_id),
            organization: None,
            vault: None,
            outcome: EventOutcome::Success,
            details: BTreeMap::new(),
            block_height: Some(1),
            trace_id: None,
            correlation_id: None,
            operations_count: None,
        }
    }

    fn write_entries(service: &EventsService<InMemoryBackend>, entries: &[EventEntry]) {
        let mut txn = service.events_db.write().expect("write txn");
        for entry in entries {
            EventStore::write(&mut txn, entry).expect("write");
        }
        txn.commit().expect("commit");
    }

    // --- ListEvents tests ---

    #[tokio::test]
    async fn list_events_empty() {
        let service = make_service_with_org(100, 1);
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });

        let resp = service.list_events(req).await.unwrap();
        assert!(resp.get_ref().entries.is_empty());
        assert!(resp.get_ref().next_page_token.is_empty());
    }

    #[tokio::test]
    async fn list_events_returns_entries() {
        let service = make_service_with_org(100, 1);
        let entries = vec![
            make_entry(1, [1u8; 16], 1_700_000_001, EventAction::WriteCommitted),
            make_entry(1, [2u8; 16], 1_700_000_002, EventAction::VaultCreated),
        ];
        write_entries(&service, &entries);

        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });

        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 2);
        assert!(resp.get_ref().next_page_token.is_empty());
    }

    #[tokio::test]
    async fn list_events_pagination() {
        let service = make_service_with_org(100, 1);
        let mut entries = Vec::new();
        for i in 0..5u8 {
            entries.push(make_entry(
                1,
                [i; 16],
                1_700_000_000 + i64::from(i),
                EventAction::WriteCommitted,
            ));
        }
        write_entries(&service, &entries);

        // Page 1
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 2,
            page_token: String::new(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 2);
        assert!(!resp.get_ref().next_page_token.is_empty());

        // Page 2
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 2,
            page_token: resp.get_ref().next_page_token.clone(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 2);
        assert!(!resp.get_ref().next_page_token.is_empty());

        // Page 3 (last entry)
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 2,
            page_token: resp.get_ref().next_page_token.clone(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 1);
        assert!(resp.get_ref().next_page_token.is_empty());
    }

    #[tokio::test]
    async fn list_events_action_filter() {
        let service = make_service_with_org(100, 1);
        let entries = vec![
            make_entry(1, [1u8; 16], 1_700_000_001, EventAction::WriteCommitted),
            make_entry(1, [2u8; 16], 1_700_000_002, EventAction::VaultCreated),
            make_entry(1, [3u8; 16], 1_700_000_003, EventAction::WriteCommitted),
        ];
        write_entries(&service, &entries);

        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                actions: vec!["vault_created".to_string()],
                ..Default::default()
            }),
            limit: 10,
            page_token: String::new(),
            caller: None,
        });

        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 1);
        assert_eq!(resp.get_ref().entries[0].action, "vault_created");
    }

    #[tokio::test]
    async fn list_events_event_type_prefix_filter() {
        let service = make_service_with_org(100, 1);
        let entries = vec![
            make_entry(1, [1u8; 16], 1_700_000_001, EventAction::VaultCreated),
            make_entry(1, [2u8; 16], 1_700_000_002, EventAction::VaultDeleted),
            make_entry(1, [3u8; 16], 1_700_000_003, EventAction::WriteCommitted),
        ];
        write_entries(&service, &entries);

        // "ledger.vault" should match "ledger.vault_created" and "ledger.vault_deleted"
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                event_type_prefix: Some("ledger.vault".to_string()),
                ..Default::default()
            }),
            limit: 10,
            page_token: String::new(),
            caller: None,
        });

        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 2);
        for entry in &resp.get_ref().entries {
            assert!(entry.event_type.starts_with("ledger.vault"));
        }
    }

    #[tokio::test]
    async fn list_events_event_type_prefix_no_false_positives() {
        let service = make_service_with_org(100, 1);
        let mut entry = make_entry(1, [1u8; 16], 1_700_000_001, EventAction::VaultCreated);
        entry.event_type = "ledger.vault.created".to_string();
        let mut entry2 = make_entry(1, [2u8; 16], 1_700_000_002, EventAction::VaultCreated);
        entry2.event_type = "ledger.vaultx.created".to_string();
        write_entries(&service, &[entry, entry2]);

        // "ledger.vault." (with trailing dot) should match "ledger.vault.created"
        // but NOT "ledger.vaultx.created"
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                event_type_prefix: Some("ledger.vault.".to_string()),
                ..Default::default()
            }),
            limit: 10,
            page_token: String::new(),
            caller: None,
        });

        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 1);
        assert_eq!(resp.get_ref().entries[0].event_type, "ledger.vault.created");
    }

    #[tokio::test]
    async fn list_events_org_isolation() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let mut state = AppliedState::default();
        // Register two orgs
        state
            .slug_index
            .insert(inferadb_ledger_types::OrganizationSlug::new(100), OrganizationId::new(1));
        state
            .id_to_slug
            .insert(OrganizationId::new(1), inferadb_ledger_types::OrganizationSlug::new(100));
        state
            .slug_index
            .insert(inferadb_ledger_types::OrganizationSlug::new(200), OrganizationId::new(2));
        state
            .id_to_slug
            .insert(OrganizationId::new(2), inferadb_ledger_types::OrganizationSlug::new(200));
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));

        let service = EventsService::builder()
            .events_db(events_db)
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .build();

        // Write events for both orgs
        let entries = vec![
            make_entry(1, [1u8; 16], 1_700_000_001, EventAction::WriteCommitted),
            make_entry(2, [2u8; 16], 1_700_000_002, EventAction::VaultCreated),
        ];
        write_entries(&service, &entries);

        // Query org 1 — should not see org 2's events
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 1);
        assert_eq!(resp.get_ref().entries[0].action, "write_committed");
    }

    #[tokio::test]
    async fn list_events_system_org_accessible() {
        let service = make_test_service();
        let entry = make_entry(0, [1u8; 16], 1_700_000_001, EventAction::OrganizationCreated);
        write_entries(&service, &[entry]);

        // System events via slug=0
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 0 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 1);
        assert_eq!(resp.get_ref().entries[0].action, "organization_created");
    }

    #[tokio::test]
    async fn list_events_missing_org_returns_error() {
        let service = make_test_service();
        let req = Request::new(proto::ListEventsRequest {
            organization: None,
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let err = service.list_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // --- Fan-out tests (cross-vault aggregation) ---
    //
    // Phase 4.4: vault-scoped events live in per-vault events.db files; the
    // org's events.db only carries org-scoped events. `ListEvents` must fan
    // out across the org-level and per-vault stores when no vault filter is
    // set, and route to a single per-vault store when a filter is set.

    /// Builds a service wired with two per-vault events.db instances plus the
    /// org-level events.db. Returns the service alongside the three databases
    /// so tests can write directly into the appropriate store. The vault slugs
    /// `(11, 22)` resolve to `(VaultId(11), VaultId(22))` under organization
    /// id `1` (slug `100`).
    fn make_fanout_service() -> (
        EventsService<InMemoryBackend>,
        EventsDatabase<InMemoryBackend>,
        EventsDatabase<InMemoryBackend>,
        EventsDatabase<InMemoryBackend>,
    ) {
        let org_events_db = EventsDatabase::open_in_memory().expect("open");
        let vault_a_db = EventsDatabase::open_in_memory().expect("open");
        let vault_b_db = EventsDatabase::open_in_memory().expect("open");

        let mut state = AppliedState::default();
        state
            .slug_index
            .insert(inferadb_ledger_types::OrganizationSlug::new(100), OrganizationId::new(1));
        state
            .id_to_slug
            .insert(OrganizationId::new(1), inferadb_ledger_types::OrganizationSlug::new(100));
        // Vault A: slug 11 -> VaultId 11
        state.vault_slug_index.insert(
            inferadb_ledger_types::VaultSlug::new(11),
            (OrganizationId::new(1), inferadb_ledger_types::VaultId::new(11)),
        );
        state.vault_id_to_slug.insert(
            (OrganizationId::new(1), inferadb_ledger_types::VaultId::new(11)),
            inferadb_ledger_types::VaultSlug::new(11),
        );
        // Vault B: slug 22 -> VaultId 22
        state.vault_slug_index.insert(
            inferadb_ledger_types::VaultSlug::new(22),
            (OrganizationId::new(1), inferadb_ledger_types::VaultId::new(22)),
        );
        state.vault_id_to_slug.insert(
            (OrganizationId::new(1), inferadb_ledger_types::VaultId::new(22)),
            inferadb_ledger_types::VaultSlug::new(22),
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));

        let vault_a_clone = vault_a_db.clone();
        let vault_b_clone = vault_b_db.clone();
        let resolver: VaultEventSources<InMemoryBackend> = Arc::new(move |org| {
            if org == OrganizationId::new(1) {
                vec![
                    VaultEventSource {
                        vault_id: inferadb_ledger_types::VaultId::new(11),
                        events_db: vault_a_clone.clone(),
                    },
                    VaultEventSource {
                        vault_id: inferadb_ledger_types::VaultId::new(22),
                        events_db: vault_b_clone.clone(),
                    },
                ]
            } else {
                Vec::new()
            }
        });

        let service = EventsService::builder()
            .events_db(org_events_db.clone())
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .vault_event_sources(resolver)
            .build();

        (service, org_events_db, vault_a_db, vault_b_db)
    }

    fn write_to_db(events_db: &EventsDatabase<InMemoryBackend>, entries: &[EventEntry]) {
        let mut txn = events_db.write().expect("write txn");
        for entry in entries {
            EventStore::write(&mut txn, entry).expect("write");
        }
        txn.commit().expect("commit");
    }

    #[tokio::test]
    async fn list_events_fanout_no_vault_filter_merges_all_sources() {
        let (service, org_db, vault_a_db, vault_b_db) = make_fanout_service();

        // Org-level: single event at t=1
        write_to_db(&org_db, &[make_entry(1, [1u8; 16], 1_700_000_001, EventAction::VaultCreated)]);
        // Vault A: events at t=2 and t=4
        write_to_db(
            &vault_a_db,
            &[
                make_entry(1, [2u8; 16], 1_700_000_002, EventAction::WriteCommitted),
                make_entry(1, [4u8; 16], 1_700_000_004, EventAction::WriteCommitted),
            ],
        );
        // Vault B: events at t=3 and t=5
        write_to_db(
            &vault_b_db,
            &[
                make_entry(1, [3u8; 16], 1_700_000_003, EventAction::WriteCommitted),
                make_entry(1, [5u8; 16], 1_700_000_005, EventAction::WriteCommitted),
            ],
        );

        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 100,
            page_token: String::new(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        let entries = &resp.get_ref().entries;

        assert_eq!(entries.len(), 5, "all 5 events from 3 sources should merge");
        // Verify chronological ordering (1, 2, 3, 4, 5).
        let timestamps: Vec<i64> =
            entries.iter().map(|e| e.timestamp.as_ref().expect("ts").seconds).collect();
        assert_eq!(
            timestamps,
            vec![1_700_000_001, 1_700_000_002, 1_700_000_003, 1_700_000_004, 1_700_000_005]
        );
    }

    #[tokio::test]
    async fn list_events_fanout_vault_filter_routes_to_single_vault() {
        let (service, org_db, vault_a_db, vault_b_db) = make_fanout_service();

        write_to_db(&org_db, &[make_entry(1, [1u8; 16], 1_700_000_001, EventAction::VaultCreated)]);
        write_to_db(
            &vault_a_db,
            &[
                make_entry(1, [10u8; 16], 1_700_000_010, EventAction::WriteCommitted),
                make_entry(1, [11u8; 16], 1_700_000_011, EventAction::WriteCommitted),
            ],
        );
        write_to_db(
            &vault_b_db,
            &[make_entry(1, [20u8; 16], 1_700_000_020, EventAction::WriteCommitted)],
        );

        // Filter on Vault A (slug 11): expect exactly the two vault A events.
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                vault: Some(proto::VaultSlug { slug: 11 }),
                ..Default::default()
            }),
            limit: 100,
            page_token: String::new(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        let entries = &resp.get_ref().entries;
        assert_eq!(entries.len(), 2);
        let timestamps: Vec<i64> =
            entries.iter().map(|e| e.timestamp.as_ref().expect("ts").seconds).collect();
        assert_eq!(timestamps, vec![1_700_000_010, 1_700_000_011]);

        // Filter on Vault B (slug 22): expect only the single vault B event.
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                vault: Some(proto::VaultSlug { slug: 22 }),
                ..Default::default()
            }),
            limit: 100,
            page_token: String::new(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        let entries = &resp.get_ref().entries;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].timestamp.as_ref().expect("ts").seconds, 1_700_000_020);
    }

    #[tokio::test]
    async fn list_events_fanout_pagination_across_sources() {
        // 12 events spread across vault A (4), vault B (4), org-level (4),
        // interleaved by timestamp. With page_size = 5 we expect three pages
        // covering all 12 entries with no duplicates and chronological order.
        let (service, org_db, vault_a_db, vault_b_db) = make_fanout_service();

        let mut org_entries = Vec::new();
        let mut vault_a_entries = Vec::new();
        let mut vault_b_entries = Vec::new();
        for i in 0..12u8 {
            let entry =
                make_entry(1, [i; 16], 1_700_000_000 + i64::from(i), EventAction::WriteCommitted);
            match i % 3 {
                0 => org_entries.push(entry),
                1 => vault_a_entries.push(entry),
                _ => vault_b_entries.push(entry),
            }
        }
        write_to_db(&org_db, &org_entries);
        write_to_db(&vault_a_db, &vault_a_entries);
        write_to_db(&vault_b_db, &vault_b_entries);

        let mut all_seen: Vec<i64> = Vec::new();
        let mut page_token = String::new();
        let mut pages = 0usize;
        loop {
            let req = Request::new(proto::ListEventsRequest {
                organization: Some(proto::OrganizationSlug { slug: 100 }),
                filter: None,
                limit: 5,
                page_token: page_token.clone(),
                caller: None,
            });
            let resp = service.list_events(req).await.unwrap();
            for e in &resp.get_ref().entries {
                all_seen.push(e.timestamp.as_ref().expect("ts").seconds);
            }
            pages += 1;
            page_token = resp.get_ref().next_page_token.clone();
            if page_token.is_empty() || pages > 10 {
                break;
            }
        }

        assert!(pages <= 3, "expected at most 3 pages, got {pages}");
        assert_eq!(all_seen.len(), 12, "every event observed exactly once");
        // Strictly ascending timestamps prove no duplicates and chronological order.
        let mut sorted = all_seen.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted, all_seen);
    }

    #[tokio::test]
    async fn list_events_fanout_unknown_vault_returns_empty() {
        let (_service, _org_db, vault_a_db, _vault_b_db) = make_fanout_service();

        // Seed vault A so we know the request path is reachable, but query
        // for a vault not registered on this node (slug 99 does not appear
        // in the resolver's output).
        write_to_db(
            &vault_a_db,
            &[make_entry(1, [1u8; 16], 1_700_000_001, EventAction::WriteCommitted)],
        );

        // Add an unknown slug to applied state so resolver returns Ok, but
        // the vault is not in our `vault_event_sources` resolver.
        let mut state = AppliedState::default();
        state
            .slug_index
            .insert(inferadb_ledger_types::OrganizationSlug::new(100), OrganizationId::new(1));
        state
            .id_to_slug
            .insert(OrganizationId::new(1), inferadb_ledger_types::OrganizationSlug::new(100));
        state.vault_slug_index.insert(
            inferadb_ledger_types::VaultSlug::new(99),
            (OrganizationId::new(1), inferadb_ledger_types::VaultId::new(99)),
        );
        state.vault_id_to_slug.insert(
            (OrganizationId::new(1), inferadb_ledger_types::VaultId::new(99)),
            inferadb_ledger_types::VaultSlug::new(99),
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));
        let vault_a_clone = vault_a_db.clone();
        let resolver: VaultEventSources<InMemoryBackend> = Arc::new(move |_org| {
            // Resolver returns vault 11 but request asks for vault 99.
            vec![VaultEventSource {
                vault_id: inferadb_ledger_types::VaultId::new(11),
                events_db: vault_a_clone.clone(),
            }]
        });
        let service = EventsService::builder()
            .events_db(EventsDatabase::open_in_memory().expect("open"))
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .vault_event_sources(resolver)
            .build();

        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                vault: Some(proto::VaultSlug { slug: 99 }),
                ..Default::default()
            }),
            limit: 100,
            page_token: String::new(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 0);
        assert!(resp.get_ref().next_page_token.is_empty());
    }

    #[tokio::test]
    async fn list_events_fanout_vault_filter_cross_org_rejected() {
        // Build a fan-out service for org 1, then ask for a vault that
        // belongs to org 2. The handler must reject with NotFound rather
        // than silently returning org 1's events.
        let (service, _org_db, _a, _b) = make_fanout_service();

        // Manually inject a vault slug that resolves to a different org via
        // a fresh service with an extended applied state.
        let mut state = AppliedState::default();
        state
            .slug_index
            .insert(inferadb_ledger_types::OrganizationSlug::new(100), OrganizationId::new(1));
        state
            .id_to_slug
            .insert(OrganizationId::new(1), inferadb_ledger_types::OrganizationSlug::new(100));
        // Vault 77 belongs to org 7 (different org).
        state.vault_slug_index.insert(
            inferadb_ledger_types::VaultSlug::new(77),
            (OrganizationId::new(7), inferadb_ledger_types::VaultId::new(77)),
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));
        let resolver: VaultEventSources<InMemoryBackend> = Arc::new(|_org| Vec::new());
        let cross_service = EventsService::builder()
            .events_db(EventsDatabase::open_in_memory().expect("open"))
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .vault_event_sources(resolver)
            .build();

        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                vault: Some(proto::VaultSlug { slug: 77 }),
                ..Default::default()
            }),
            limit: 100,
            page_token: String::new(),
            caller: None,
        });
        let err = cross_service.list_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);

        // Original fanout service unaffected.
        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 100,
            page_token: String::new(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 0);
    }

    #[tokio::test]
    async fn list_events_fanout_filters_apply_after_merge() {
        // Filters (action, principal, etc.) must apply to every source —
        // a vault-scoped event matching the filter must appear, and an
        // org-scoped event not matching must not.
        let (service, org_db, vault_a_db, _vault_b_db) = make_fanout_service();

        write_to_db(
            &org_db,
            &[
                make_entry(1, [1u8; 16], 1_700_000_001, EventAction::VaultCreated),
                make_entry(1, [2u8; 16], 1_700_000_002, EventAction::WriteCommitted),
            ],
        );
        write_to_db(
            &vault_a_db,
            &[
                make_entry(1, [3u8; 16], 1_700_000_003, EventAction::WriteCommitted),
                make_entry(1, [4u8; 16], 1_700_000_004, EventAction::VaultCreated),
            ],
        );

        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                actions: vec!["write_committed".to_string()],
                ..Default::default()
            }),
            limit: 100,
            page_token: String::new(),
            caller: None,
        });
        let resp = service.list_events(req).await.unwrap();
        let entries = &resp.get_ref().entries;
        assert_eq!(entries.len(), 2);
        for e in entries {
            assert_eq!(e.action, "write_committed");
        }
    }

    // --- GetEvent tests ---

    #[tokio::test]
    async fn get_event_found() {
        let service = make_service_with_org(100, 1);
        let entry = make_entry(1, [42u8; 16], 1_700_000_001, EventAction::VaultCreated);
        write_entries(&service, &[entry]);

        let req = Request::new(proto::GetEventRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            event_id: vec![42u8; 16],
            caller: None,
        });
        let resp = service.get_event(req).await.unwrap();
        let got = resp.get_ref().entry.as_ref().unwrap();
        assert_eq!(got.action, "vault_created");
    }

    #[tokio::test]
    async fn get_event_not_found() {
        let service = make_service_with_org(100, 1);

        let req = Request::new(proto::GetEventRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            event_id: vec![99u8; 16],
            caller: None,
        });
        let err = service.get_event(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_event_wrong_org() {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let mut state = AppliedState::default();
        state
            .slug_index
            .insert(inferadb_ledger_types::OrganizationSlug::new(100), OrganizationId::new(1));
        state
            .id_to_slug
            .insert(OrganizationId::new(1), inferadb_ledger_types::OrganizationSlug::new(100));
        state
            .slug_index
            .insert(inferadb_ledger_types::OrganizationSlug::new(200), OrganizationId::new(2));
        state
            .id_to_slug
            .insert(OrganizationId::new(2), inferadb_ledger_types::OrganizationSlug::new(200));
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));

        let service = EventsService::builder()
            .events_db(events_db)
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .build();

        // Write event for org 1
        let entry = make_entry(1, [42u8; 16], 1_700_000_001, EventAction::VaultCreated);
        write_entries(&service, &[entry]);

        // Query as org 2 — should not find it
        let req = Request::new(proto::GetEventRequest {
            organization: Some(proto::OrganizationSlug { slug: 200 }),
            event_id: vec![42u8; 16],
            caller: None,
        });
        let err = service.get_event(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_event_invalid_id_length() {
        let service = make_service_with_org(100, 1);

        let req = Request::new(proto::GetEventRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            event_id: vec![1, 2, 3], // wrong length
            caller: None,
        });
        let err = service.get_event(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // --- CountEvents tests ---

    #[tokio::test]
    async fn count_events_total() {
        let service = make_service_with_org(100, 1);
        let mut entries = Vec::new();
        for i in 0..7u8 {
            entries.push(make_entry(
                1,
                [i; 16],
                1_700_000_000 + i64::from(i),
                EventAction::WriteCommitted,
            ));
        }
        write_entries(&service, &entries);

        let req = Request::new(proto::CountEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            caller: None,
        });
        let resp = service.count_events(req).await.unwrap();
        assert_eq!(resp.get_ref().count, 7);
    }

    #[tokio::test]
    async fn count_events_with_filter() {
        let service = make_service_with_org(100, 1);
        let entries = vec![
            make_entry(1, [1u8; 16], 1_700_000_001, EventAction::WriteCommitted),
            make_entry(1, [2u8; 16], 1_700_000_002, EventAction::VaultCreated),
            make_entry(1, [3u8; 16], 1_700_000_003, EventAction::WriteCommitted),
        ];
        write_entries(&service, &entries);

        let req = Request::new(proto::CountEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                actions: vec!["write_committed".to_string()],
                ..Default::default()
            }),
            caller: None,
        });
        let resp = service.count_events(req).await.unwrap();
        assert_eq!(resp.get_ref().count, 2);
    }

    #[tokio::test]
    async fn count_events_system_org() {
        let service = make_test_service();
        let entries = vec![
            make_entry(0, [1u8; 16], 1_700_000_001, EventAction::OrganizationCreated),
            make_entry(0, [2u8; 16], 1_700_000_002, EventAction::UserCreated),
        ];
        write_entries(&service, &entries);

        let req = Request::new(proto::CountEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 0 }),
            filter: None,
            caller: None,
        });
        let resp = service.count_events(req).await.unwrap();
        assert_eq!(resp.get_ref().count, 2);
    }

    // --- IngestEvents tests ---

    #[tokio::test]
    async fn ingest_events_no_config_returns_unavailable() {
        // Service built without ingestion config → UNAVAILABLE
        let service = make_test_service();
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 0 }),
            entries: vec![],
            caller: None,
        });
        let err = service.ingest_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn ingest_events_disabled_returns_unavailable() {
        let config = IngestionConfig { ingest_enabled: false, ..IngestionConfig::default() };
        let (service, _proposer) = make_ingest_service_with_config(100, 1, config);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("engine.task_completed", "user-1")],
            caller: None,
        });
        let err = service.ingest_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("disabled"));
    }

    #[tokio::test]
    async fn ingest_events_unknown_source_rejected() {
        let service = make_ingest_service(100, 1);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "rogue".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("rogue.evil", "user-1")],
            caller: None,
        });
        let err = service.ingest_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("rogue"));
    }

    #[tokio::test]
    async fn ingest_events_batch_size_exceeded() {
        let config = IngestionConfig { max_ingest_batch_size: 2, ..IngestionConfig::default() };
        let (service, _proposer) = make_ingest_service_with_config(100, 1, config);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry("engine.a", "u1"),
                make_ingest_entry("engine.b", "u2"),
                make_ingest_entry("engine.c", "u3"),
            ],
            caller: None,
        });
        let err = service.ingest_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("exceeds maximum"));
    }

    #[tokio::test]
    async fn ingest_events_empty_batch_succeeds() {
        let service = make_ingest_service(100, 1);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        let body = resp.get_ref();
        assert_eq!(body.accepted_count, 0);
        assert_eq!(body.rejected_count, 0);
        assert!(body.rejections.is_empty());
    }

    #[tokio::test]
    async fn ingest_events_success_and_query_back() {
        let service = make_ingest_service(100, 1);

        // Ingest two events
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry("engine.task_completed", "alice"),
                make_ingest_entry("engine.task_started", "bob"),
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 2);
        assert_eq!(resp.get_ref().rejected_count, 0);

        // Query them back via ListEvents
        let list_req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let list_resp = service.list_events(list_req).await.unwrap();
        assert_eq!(list_resp.get_ref().entries.len(), 2);

        // Verify source_service and event_type are preserved
        for e in &list_resp.get_ref().entries {
            assert_eq!(e.source_service, "engine");
            assert!(e.event_type.starts_with("engine."));
        }
    }

    #[tokio::test]
    async fn ingest_events_partial_success_mixed_valid_invalid() {
        let service = make_ingest_service(100, 1);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                // Valid
                make_ingest_entry("engine.ok", "alice"),
                // Invalid: wrong event_type prefix
                make_ingest_entry("control.wrong_prefix", "bob"),
                // Invalid: missing principal
                proto::IngestEventEntry {
                    event_type: "engine.task".to_string(),
                    principal: String::new(),
                    outcome: proto::EventOutcome::Success.into(),
                    details: std::collections::HashMap::new(),
                    trace_id: None,
                    correlation_id: None,
                    vault: None,
                    timestamp: None,
                    error_code: None,
                    error_detail: None,
                    denial_reason: None,
                },
                // Valid
                make_ingest_entry("engine.done", "charlie"),
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 2);
        assert_eq!(resp.get_ref().rejected_count, 2);
        assert_eq!(resp.get_ref().rejections.len(), 2);

        // Check rejection indices
        assert_eq!(resp.get_ref().rejections[0].index, 1);
        assert!(resp.get_ref().rejections[0].reason.contains("must start with"));
        assert_eq!(resp.get_ref().rejections[1].index, 2);
        assert!(resp.get_ref().rejections[1].reason.contains("principal"));

        // Valid entries persisted
        let list_req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let list_resp = service.list_events(list_req).await.unwrap();
        assert_eq!(list_resp.get_ref().entries.len(), 2);
    }

    #[tokio::test]
    async fn ingest_events_all_invalid_batch() {
        let service = make_ingest_service(100, 1);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry("control.wrong", "alice"),
                make_ingest_entry("bad.prefix", "bob"),
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 0);
        assert_eq!(resp.get_ref().rejected_count, 2);

        // Nothing persisted
        let list_req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let list_resp = service.list_events(list_req).await.unwrap();
        assert!(list_resp.get_ref().entries.is_empty());
    }

    #[tokio::test]
    async fn ingest_events_event_type_prefix_filter_isolates_sources() {
        let service = make_ingest_service(100, 1);

        // Write a native ledger event directly
        let ledger_entry = make_entry(1, [1u8; 16], 1_700_000_001, EventAction::WriteCommitted);
        write_entries(&service, &[ledger_entry]);

        // Ingest an engine event
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("engine.task_done", "alice")],
            caller: None,
        });
        service.ingest_events(req).await.unwrap();

        // Filter by "engine." prefix — should only see the ingested event
        let list_req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                event_type_prefix: Some("engine.".to_string()),
                ..Default::default()
            }),
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let list_resp = service.list_events(list_req).await.unwrap();
        assert_eq!(list_resp.get_ref().entries.len(), 1);
        assert!(list_resp.get_ref().entries[0].event_type.starts_with("engine."));

        // Filter by "ledger." — should only see the native event
        let list_req2 = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                event_type_prefix: Some("ledger.".to_string()),
                ..Default::default()
            }),
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let list_resp2 = service.list_events(list_req2).await.unwrap();
        assert_eq!(list_resp2.get_ref().entries.len(), 1);
        assert!(list_resp2.get_ref().entries[0].event_type.starts_with("ledger."));

        // No filter — both events visible
        let list_req3 = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let list_resp3 = service.list_events(list_req3).await.unwrap();
        assert_eq!(list_resp3.get_ref().entries.len(), 2);
    }

    #[tokio::test]
    async fn ingest_events_rate_limit_enforcement() {
        // Very low rate limit: 1 event/sec
        let config =
            IngestionConfig { ingest_rate_limit_per_source: 1, ..IngestionConfig::default() };
        let (service, _proposer) = make_ingest_service_with_config(100, 1, config);

        // First request: 1 event — should succeed (bucket starts full)
        let req1 = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("engine.a", "u1")],
            caller: None,
        });
        let resp1 = service.ingest_events(req1).await.unwrap();
        assert_eq!(resp1.get_ref().accepted_count, 1);

        // Second immediate request: should be rate limited (bucket drained)
        let req2 = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("engine.b", "u2")],
            caller: None,
        });
        let err = service.ingest_events(req2).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert!(err.message().contains("Rate limit"));
    }

    #[tokio::test]
    async fn ingest_events_rate_limit_independent_source_buckets() {
        // Low rate limit to trigger it quickly
        let config =
            IngestionConfig { ingest_rate_limit_per_source: 1, ..IngestionConfig::default() };
        let (service, _proposer) = make_ingest_service_with_config(100, 1, config);

        // "engine" drains its bucket
        let req1 = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("engine.a", "u1")],
            caller: None,
        });
        service.ingest_events(req1).await.unwrap();

        // "control" has its own bucket — should still succeed
        let req2 = Request::new(proto::IngestEventsRequest {
            source_service: "control".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("control.b", "u1")],
            caller: None,
        });
        let resp2 = service.ingest_events(req2).await.unwrap();
        assert_eq!(resp2.get_ref().accepted_count, 1);
    }

    #[tokio::test]
    async fn ingest_events_details_size_exceeds_limit() {
        let service = make_ingest_service(100, 1);
        let mut details = std::collections::HashMap::new();
        // Default max_details_size_bytes is 4096 — exceed it
        details.insert("big_key".to_string(), "x".repeat(5000));
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![proto::IngestEventEntry {
                event_type: "engine.heavy".to_string(),
                principal: "user".to_string(),
                outcome: proto::EventOutcome::Success.into(),
                details,
                trace_id: None,
                correlation_id: None,
                vault: None,
                timestamp: None,
                error_code: None,
                error_detail: None,
                denial_reason: None,
            }],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 0);
        assert_eq!(resp.get_ref().rejected_count, 1);
        assert!(resp.get_ref().rejections[0].reason.contains("details map size"));
    }

    #[tokio::test]
    async fn ingest_events_invalid_outcome_rejected() {
        let service = make_ingest_service(100, 1);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![proto::IngestEventEntry {
                event_type: "engine.task".to_string(),
                principal: "user".to_string(),
                outcome: 99, // invalid enum variant
                details: std::collections::HashMap::new(),
                trace_id: None,
                correlation_id: None,
                vault: None,
                timestamp: None,
                error_code: None,
                error_detail: None,
                denial_reason: None,
            }],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 0);
        assert_eq!(resp.get_ref().rejected_count, 1);
        assert!(resp.get_ref().rejections[0].reason.contains("invalid outcome"));
    }

    #[tokio::test]
    async fn ingest_events_outcome_variants_stored_correctly() {
        let service = make_ingest_service(100, 1);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                // Success
                make_ingest_entry("engine.success_op", "alice"),
                // Failed
                proto::IngestEventEntry {
                    event_type: "engine.failed_op".to_string(),
                    principal: "bob".to_string(),
                    outcome: proto::EventOutcome::Failed.into(),
                    details: std::collections::HashMap::new(),
                    trace_id: None,
                    correlation_id: None,
                    vault: None,
                    timestamp: None,
                    error_code: Some("ERR_42".to_string()),
                    error_detail: Some("something broke".to_string()),
                    denial_reason: None,
                },
                // Denied
                proto::IngestEventEntry {
                    event_type: "engine.denied_op".to_string(),
                    principal: "charlie".to_string(),
                    outcome: proto::EventOutcome::Denied.into(),
                    details: std::collections::HashMap::new(),
                    trace_id: None,
                    correlation_id: None,
                    vault: None,
                    timestamp: None,
                    error_code: None,
                    error_detail: None,
                    denial_reason: Some("quota exceeded".to_string()),
                },
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 3);

        // Query back and verify outcomes
        let list_req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let list_resp = service.list_events(list_req).await.unwrap();
        assert_eq!(list_resp.get_ref().entries.len(), 3);
    }

    #[tokio::test]
    async fn ingest_events_preserves_metadata() {
        // Seed vault slug 42 → (org=1, vault_id=3) so the per-vault routing
        // resolver accepts the entry. Without a registered slug, the
        // pre-propose vault validation would reject the event with
        // "vault with slug 42 not found".
        let (service, _proposer) = make_ingest_service_with_vaults(100, 1, &[(42, 1, 3)]);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![proto::IngestEventEntry {
                event_type: "engine.task_done".to_string(),
                principal: "alice".to_string(),
                outcome: proto::EventOutcome::Success.into(),
                details: {
                    let mut m = std::collections::HashMap::new();
                    m.insert("key".to_string(), "value".to_string());
                    m
                },
                trace_id: Some("trace-abc".to_string()),
                correlation_id: Some("corr-xyz".to_string()),
                vault: Some(proto::VaultSlug { slug: 42 }),
                timestamp: None,
                error_code: None,
                error_detail: None,
                denial_reason: None,
            }],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 1);

        // Query back and verify metadata preserved
        let list_req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
            caller: None,
        });
        let list_resp = service.list_events(list_req).await.unwrap();
        let entry = &list_resp.get_ref().entries[0];
        assert_eq!(entry.source_service, "engine");
        assert_eq!(entry.event_type, "engine.task_done");
        assert_eq!(entry.principal, "alice");
        assert_eq!(entry.trace_id.as_deref(), Some("trace-abc"));
        assert_eq!(entry.correlation_id.as_deref(), Some("corr-xyz"));
        assert_eq!(entry.details.get("key").map(String::as_str), Some("value"));
    }

    #[tokio::test]
    async fn ingest_events_control_source_accepted() {
        let service = make_ingest_service(100, 1);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "control".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("control.deploy_started", "ci-bot")],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 1);
    }

    // --- Correlation ID filter test ---

    #[tokio::test]
    async fn list_events_correlation_id_filter() {
        let service = make_service_with_org(100, 1);
        let mut e1 = make_entry(1, [1u8; 16], 1_700_000_001, EventAction::WriteCommitted);
        e1.correlation_id = Some("req-123".to_string());
        let mut e2 = make_entry(1, [2u8; 16], 1_700_000_002, EventAction::VaultCreated);
        e2.correlation_id = Some("req-456".to_string());
        let e3 = make_entry(1, [3u8; 16], 1_700_000_003, EventAction::WriteCommitted);
        write_entries(&service, &[e1, e2, e3]);

        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                correlation_id: Some("req-123".to_string()),
                ..Default::default()
            }),
            limit: 10,
            page_token: String::new(),
            caller: None,
        });

        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 1);
        assert_eq!(resp.get_ref().entries[0].correlation_id.as_deref(), Some("req-123"));
    }

    // --- Principal filter test ---

    #[tokio::test]
    async fn list_events_principal_filter() {
        let service = make_service_with_org(100, 1);
        let mut e1 = make_entry(1, [1u8; 16], 1_700_000_001, EventAction::WriteCommitted);
        e1.principal = "alice".to_string();
        let mut e2 = make_entry(1, [2u8; 16], 1_700_000_002, EventAction::VaultCreated);
        e2.principal = "bob".to_string();
        write_entries(&service, &[e1, e2]);

        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                principal: Some("alice".to_string()),
                ..Default::default()
            }),
            limit: 10,
            page_token: String::new(),
            caller: None,
        });

        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 1);
        assert_eq!(resp.get_ref().entries[0].principal, "alice");
    }

    // --- Outcome filter test ---

    #[tokio::test]
    async fn list_events_outcome_filter() {
        let service = make_service_with_org(100, 1);
        let e1 = make_entry(1, [1u8; 16], 1_700_000_001, EventAction::WriteCommitted);
        let mut e2 = make_entry(1, [2u8; 16], 1_700_000_002, EventAction::RequestRateLimited);
        e2.outcome = EventOutcome::Denied { reason: "rate limited".to_string() };
        write_entries(&service, &[e1, e2]);

        let req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: Some(proto::EventFilter {
                outcome: proto::EventOutcome::Denied.into(),
                ..Default::default()
            }),
            limit: 10,
            page_token: String::new(),
            caller: None,
        });

        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 1);
    }

    // ── IngestExternalEvents Raft-routing tests ──

    /// A valid batch must produce exactly one `OrganizationRequest::IngestExternalEvents`
    /// proposal via `propose_to_region_bytes`. The captured proposal's event list
    /// must match the RPC's accepted entries so WAL replay is byte-identical.
    #[tokio::test]
    async fn ingest_events_proposes_to_regional_raft() {
        let (service, proposer) = make_ingest_service_with_proposer(100, 1);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry("engine.task_completed", "alice"),
                make_ingest_entry("engine.task_failed", "bob"),
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 2);
        assert_eq!(resp.get_ref().rejected_count, 0);

        // Exactly one proposal submitted; it is IngestExternalEvents with 2 events.
        let decoded = proposer.decode_proposals();
        assert_eq!(decoded.len(), 1, "expected exactly one Raft proposal");
        let (_region, vault_routing, request) = &decoded[0];
        assert_eq!(*vault_routing, None, "no entry has a vault — must route org-level");
        match request {
            OrganizationRequest::IngestExternalEvents { source, events } => {
                assert_eq!(source, "engine");
                assert_eq!(events.len(), 2);
                // Pre-proposal validation assigns UUID v4 event_ids + source service.
                for entry in events {
                    assert_eq!(entry.source_service, "engine");
                    assert_ne!(entry.event_id, [0u8; 16], "event_id must be assigned pre-proposal");
                }
            },
            other => panic!("expected IngestExternalEvents, got {other:?}"),
        }
    }

    /// When every entry is rejected by validation, no Raft proposal is
    /// submitted — the RPC short-circuits with the rejection list. Validates
    /// that failed validations don't consume Raft bandwidth.
    #[tokio::test]
    async fn ingest_events_empty_batch_skips_raft() {
        let (service, proposer) = make_ingest_service_with_proposer(100, 1);
        // Every entry has wrong prefix → all rejected, accepted_entries empty.
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry("control.wrong_prefix_a", "u1"),
                make_ingest_entry("control.wrong_prefix_b", "u2"),
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 0);
        assert_eq!(resp.get_ref().rejected_count, 2);
        // No proposal submitted — rejected batches never reach Raft.
        assert!(proposer.proposals().is_empty(), "empty accepted batch must not propose to Raft");
    }

    /// When the apply handler returns an error (simulated via
    /// `TestIngestProposer::set_next_error`), the RPC must propagate the
    /// error to the gRPC client through `status_with_correlation`. This
    /// validates the Option A error-propagation contract.
    #[tokio::test]
    async fn ingest_events_apply_error_propagates_to_caller() {
        let (service, proposer) = make_ingest_service_with_proposer(100, 1);
        // Inject an apply-error the proposer will surface on the next call.
        proposer.set_next_error(Status::internal("simulated events.db commit_in_memory failure"));

        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("engine.task_completed", "alice")],
            caller: None,
        });
        let err = service.ingest_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(
            err.message().contains("simulated events.db commit_in_memory failure"),
            "unexpected error message: {}",
            err.message()
        );
        // The proposal was submitted (captured) before the mock error fired,
        // confirming the proposer was actually invoked.
        assert_eq!(proposer.proposals().len(), 1);
    }

    // ── Per-vault bucketing tests (Task #161) ──

    /// All entries carrying the same `vault` slug must produce exactly one
    /// `propose_to_vault_bytes` call — and zero `propose_to_organization_bytes`
    /// calls — because the vault-routed apply pipeline expects pre-bucketed
    /// `VaultScoped` payloads.
    #[tokio::test]
    async fn ingest_events_single_vault_routes_to_vault_shard() {
        let (service, proposer) = make_ingest_service_with_vaults(100, 1, &[(200, 1, 5)]);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry_with_vault("engine.task_completed", "alice", 200),
                make_ingest_entry_with_vault("engine.task_failed", "bob", 200),
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 2);

        let decoded = proposer.decode_proposals();
        assert_eq!(decoded.len(), 1, "expected exactly one vault-scoped proposal");
        let (_region, vault_routing, request) = &decoded[0];
        assert_eq!(*vault_routing, Some(inferadb_ledger_types::VaultId::new(5)));
        match request {
            OrganizationRequest::IngestExternalEvents { events, .. } => {
                assert_eq!(events.len(), 2);
                for entry in events {
                    assert_eq!(entry.vault, Some(inferadb_ledger_types::VaultSlug::new(200)));
                }
            },
            other => panic!("expected IngestExternalEvents, got {other:?}"),
        }
    }

    /// A multi-vault batch must produce one proposal per distinct vault.
    /// Within each bucket, entries preserve their original-request order so
    /// retries serialize to byte-identical payloads (idempotency invariant).
    #[tokio::test]
    async fn ingest_events_multi_vault_buckets_and_routes() {
        let (service, proposer) =
            make_ingest_service_with_vaults(100, 1, &[(201, 1, 5), (202, 1, 7)]);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry_with_vault("engine.a", "alice", 201),
                make_ingest_entry_with_vault("engine.b", "bob", 202),
                make_ingest_entry_with_vault("engine.c", "carol", 201),
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 3);
        assert_eq!(resp.get_ref().rejected_count, 0);

        let decoded = proposer.decode_proposals();
        assert_eq!(decoded.len(), 2, "expected one proposal per distinct vault");

        // Buckets iterate in `BTreeMap<Option<VaultId>, _>` order: ascending
        // VaultId after the `None` org-bucket. Here both are `Some` so the
        // ordering is VaultId(5) before VaultId(7).
        let (_region_0, vault_0, req_0) = &decoded[0];
        let (_region_1, vault_1, req_1) = &decoded[1];
        assert_eq!(*vault_0, Some(inferadb_ledger_types::VaultId::new(5)));
        assert_eq!(*vault_1, Some(inferadb_ledger_types::VaultId::new(7)));

        match req_0 {
            OrganizationRequest::IngestExternalEvents { events, .. } => {
                assert_eq!(events.len(), 2, "vault 5 bucket holds entries a and c");
                assert_eq!(events[0].event_type, "engine.a");
                assert_eq!(events[1].event_type, "engine.c");
            },
            other => panic!("expected IngestExternalEvents, got {other:?}"),
        }
        match req_1 {
            OrganizationRequest::IngestExternalEvents { events, .. } => {
                assert_eq!(events.len(), 1, "vault 7 bucket holds entry b");
                assert_eq!(events[0].event_type, "engine.b");
            },
            other => panic!("expected IngestExternalEvents, got {other:?}"),
        }
    }

    /// A mixed batch — some entries with a vault, some without — produces
    /// one `propose_to_organization_bytes` call (for the org-level entries)
    /// plus one `propose_to_vault_bytes` call per distinct vault.
    #[tokio::test]
    async fn ingest_events_mixed_batch_splits_org_and_vault_proposals() {
        let (service, proposer) = make_ingest_service_with_vaults(100, 1, &[(201, 1, 5)]);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry("engine.org_level", "alice"),
                make_ingest_entry_with_vault("engine.vault_level", "bob", 201),
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 2);

        let decoded = proposer.decode_proposals();
        assert_eq!(decoded.len(), 2);

        // BTreeMap iteration: `None` (org-level) first, then `Some(_)`.
        let (_, routing_0, req_0) = &decoded[0];
        let (_, routing_1, req_1) = &decoded[1];
        assert_eq!(*routing_0, None, "org-level bucket must come first");
        assert_eq!(*routing_1, Some(inferadb_ledger_types::VaultId::new(5)));

        match req_0 {
            OrganizationRequest::IngestExternalEvents { events, .. } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].event_type, "engine.org_level");
                assert!(events[0].vault.is_none());
            },
            other => panic!("expected IngestExternalEvents, got {other:?}"),
        }
        match req_1 {
            OrganizationRequest::IngestExternalEvents { events, .. } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].event_type, "engine.vault_level");
                assert_eq!(events[0].vault, Some(inferadb_ledger_types::VaultSlug::new(201)));
            },
            other => panic!("expected IngestExternalEvents, got {other:?}"),
        }
    }

    /// An entry that targets a vault belonging to a different organization
    /// must be rejected per-event with a clear error — never proposed to
    /// the foreign vault's shard. The other entries in the batch proceed.
    #[tokio::test]
    async fn ingest_events_rejects_vault_from_other_org() {
        // org=1 owns vault slug 201; vault slug 999 belongs to org=2.
        let (service, proposer) =
            make_ingest_service_with_vaults(100, 1, &[(201, 1, 5), (999, 2, 11)]);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry_with_vault("engine.ok", "alice", 201),
                make_ingest_entry_with_vault("engine.foreign", "bob", 999),
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 1);
        assert_eq!(resp.get_ref().rejected_count, 1);
        assert_eq!(resp.get_ref().rejections[0].index, 1);
        assert!(
            resp.get_ref().rejections[0].reason.contains("belongs to organization"),
            "unexpected rejection reason: {}",
            resp.get_ref().rejections[0].reason
        );

        // Only the in-org vault was proposed.
        let decoded = proposer.decode_proposals();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1, Some(inferadb_ledger_types::VaultId::new(5)));
    }

    /// An entry referencing an unknown vault slug must be rejected per-event
    /// without short-circuiting the rest of the batch.
    #[tokio::test]
    async fn ingest_events_rejects_unknown_vault_slug() {
        let (service, proposer) = make_ingest_service_with_vaults(100, 1, &[(201, 1, 5)]);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry_with_vault("engine.bogus", "alice", 8888),
                make_ingest_entry_with_vault("engine.ok", "bob", 201),
            ],
            caller: None,
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 1);
        assert_eq!(resp.get_ref().rejected_count, 1);
        assert_eq!(resp.get_ref().rejections[0].index, 0);
        assert!(
            resp.get_ref().rejections[0].reason.contains("not found"),
            "unexpected rejection reason: {}",
            resp.get_ref().rejections[0].reason
        );

        let decoded = proposer.decode_proposals();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1, Some(inferadb_ledger_types::VaultId::new(5)));
    }

    /// A multi-bucket batch where the FIRST per-bucket proposal fails must
    /// surface the error to the caller without continuing to the next
    /// bucket. Validates the "first error wins" partial-failure contract.
    #[tokio::test]
    async fn ingest_events_multi_bucket_first_error_short_circuits() {
        let (service, proposer) =
            make_ingest_service_with_vaults(100, 1, &[(201, 1, 5), (202, 1, 7)]);
        // Inject an error on the first per-bucket propose. With BTreeMap
        // ordering (ascending VaultId), the vault-5 bucket is dispatched
        // first, so it consumes the error.
        proposer.set_next_error(Status::internal("simulated propose failure"));

        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry_with_vault("engine.a", "alice", 201),
                make_ingest_entry_with_vault("engine.b", "bob", 202),
            ],
            caller: None,
        });
        let err = service.ingest_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("simulated propose failure"));

        // Exactly one bucket was attempted before the short-circuit; the
        // vault-7 bucket never ran.
        let decoded = proposer.decode_proposals();
        assert_eq!(decoded.len(), 1, "later buckets must not propose after first error");
        assert_eq!(decoded[0].1, Some(inferadb_ledger_types::VaultId::new(5)));
    }

    /// Bucketing must be deterministic across retries so each per-vault
    /// proposal serializes to byte-identical payload bytes — a prerequisite
    /// for the per-vault idempotency cache to dedupe replayed batches.
    /// Two identical requests must produce identical (region, vault, bytes)
    /// streams.
    #[tokio::test]
    async fn ingest_events_per_vault_payload_is_deterministic() {
        let (service_a, proposer_a) =
            make_ingest_service_with_vaults(100, 1, &[(201, 1, 5), (202, 1, 7)]);
        let (service_b, proposer_b) =
            make_ingest_service_with_vaults(100, 1, &[(201, 1, 5), (202, 1, 7)]);

        // Use deterministic timestamps so the EventEntry payloads don't
        // diverge on `now`. event_id is per-call random (UUID v4) — that is
        // a known idempotency boundary owned by the apply pipeline's
        // per-vault cache, not the bytes themselves; assert on bucket
        // structure + ordering instead.
        let make_req = || {
            Request::new(proto::IngestEventsRequest {
                source_service: "engine".to_string(),
                organization: Some(proto::OrganizationSlug { slug: 100 }),
                entries: vec![
                    make_ingest_entry_with_vault("engine.a", "alice", 202),
                    make_ingest_entry_with_vault("engine.b", "bob", 201),
                    make_ingest_entry_with_vault("engine.c", "carol", 202),
                ],
                caller: None,
            })
        };
        service_a.ingest_events(make_req()).await.unwrap();
        service_b.ingest_events(make_req()).await.unwrap();

        let decoded_a = proposer_a.decode_proposals();
        let decoded_b = proposer_b.decode_proposals();
        assert_eq!(decoded_a.len(), 2);
        assert_eq!(decoded_b.len(), 2);
        // Bucket ordering must match between the two runs.
        assert_eq!(decoded_a[0].1, decoded_b[0].1);
        assert_eq!(decoded_a[1].1, decoded_b[1].1);
        // Each bucket's per-event ordering must match.
        for (i, ((_, _, req_a), (_, _, req_b))) in
            decoded_a.iter().zip(decoded_b.iter()).enumerate()
        {
            match (req_a, req_b) {
                (
                    OrganizationRequest::IngestExternalEvents { events: events_a, .. },
                    OrganizationRequest::IngestExternalEvents { events: events_b, .. },
                ) => {
                    assert_eq!(events_a.len(), events_b.len(), "bucket {i} length mismatch");
                    for (a, b) in events_a.iter().zip(events_b.iter()) {
                        assert_eq!(a.event_type, b.event_type);
                        assert_eq!(a.principal, b.principal);
                        assert_eq!(a.vault, b.vault);
                    }
                },
                _ => panic!("expected IngestExternalEvents in both decoded streams"),
            }
        }
    }
}
