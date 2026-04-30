//! Wire-protocol implementation for `EventsService` (Phase F.1.f.1.10).
//!
//! Provides the [`inferadb_ledger_wire_services::EventsService`] impl on
//! [`super::events::EventsService`]. The wire impl operates on wire types
//! end-to-end — no proto round-trip, no UFCS-delegation through the tonic
//! impl. The two paths share only the underlying `SlugResolver`,
//! `PageTokenCodec`, `EventStore`, and `ProposalService` primitives.
//!
//! Page-token hash determinism across transports is preserved by
//! [`compute_filter_hash_wire`](super::events::compute_filter_hash_wire),
//! which produces a bit-identical hash to the proto-shaped
//! [`compute_filter_hash`](super::events::compute_filter_hash) used by the
//! tonic path. Domain → wire `EventEntry` translation is done via
//! [`domain_to_wire_event_entry`] without round-tripping through proto.

use std::collections::BTreeMap;

use bytes::Bytes;
use chrono::Utc;
use inferadb_ledger_raft::{
    pagination::EventPageToken,
    types::{LedgerResponse, OrganizationRequest},
};
use inferadb_ledger_state::EventStore;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    OrganizationSlug, VaultId, VaultSlug,
    events::{
        EventAction, EventEmission, EventEntry, EventOutcome, EventScope as DomainEventScope,
    },
};
use inferadb_ledger_wire::{
    ErrorCode, RequestContext, WireError,
    services::{events as w, shared as ws},
};
use uuid::Uuid;

use super::{
    error_classify,
    events::{
        COUNT_SCAN_LIMIT, DEFAULT_LIMIT, EventsService, MAX_LIMIT, compute_filter_hash_wire,
        matches_filter_wire,
    },
    slug_resolver::SlugResolver,
};
use crate::proposal::serialize_payload_wire;

// ---------------------------------------------------------------------------
// Enum conversions (numeric-tag mappings).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// EventEntry conversion (direct domain → wire bridge).
//
// `domain_to_wire_event_entry` consumes a `domain::EventEntry` from
// `EventStore::list`/`get` and produces the wire `EventEntry` directly,
// without round-tripping through proto. The `EventOutcome` /
// `EventEmission` rich enums are flattened the same way the previous
// `proto::EventEntry::From<&domain::EventEntry>` impl did:
// - `EventOutcome::Failed { code, detail }` → `outcome=Failed` + `error_code` + `error_detail`
// - `EventOutcome::Denied { reason }`       → `outcome=Denied` + `denial_reason`
// - `EventEmission::HandlerPhase { node_id }` → `emission_path=HandlerPhase` + `node_id`
// ---------------------------------------------------------------------------

/// Convert a domain `EventEntry` reference to its wire representation.
fn domain_to_wire_event_entry(entry: &EventEntry) -> w::EventEntry {
    let (outcome, error_code, error_detail, denial_reason) = match &entry.outcome {
        EventOutcome::Success => (w::EventOutcome::Success, None, None, None),
        EventOutcome::Failed { code, detail } => {
            (w::EventOutcome::Failed, Some(code.clone()), Some(detail.clone()), None)
        },
        EventOutcome::Denied { reason } => {
            (w::EventOutcome::Denied, None, None, Some(reason.clone()))
        },
    };

    let (emission_path, node_id) = match &entry.emission {
        EventEmission::ApplyPhase => (w::EventEmissionPath::ApplyPhase, None),
        EventEmission::HandlerPhase { node_id } => {
            (w::EventEmissionPath::HandlerPhase, Some(*node_id))
        },
    };

    let scope = match entry.scope {
        DomainEventScope::System => w::EventScope::System,
        DomainEventScope::Organization => w::EventScope::Organization,
    };

    // The proto bridge fell back to `organization_id.value() as u64` when the
    // external slug was absent — preserve that to keep wire output bit-
    // compatible with the previous domain → proto → wire path.
    let organization_slug =
        entry.organization.map(|s| s.value()).unwrap_or(entry.organization_id.value() as u64);

    w::EventEntry {
        event_id: Bytes::copy_from_slice(&entry.event_id),
        source_service: entry.source_service.clone(),
        event_type: entry.event_type.clone(),
        timestamp: u64::try_from(entry.timestamp.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0),
        scope,
        action: entry.action.as_str().to_string(),
        emission_path,
        principal: entry.principal.clone(),
        organization: Some(ws::OrganizationSlug::new(organization_slug)),
        vault: entry.vault.map(|s| ws::VaultSlug::new(s.value())),
        outcome,
        error_code,
        error_detail,
        denial_reason,
        details: entry.details.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        block_height: entry.block_height,
        node_id,
        trace_id: entry.trace_id.clone(),
        correlation_id: entry.correlation_id.clone(),
        operations_count: entry.operations_count,
        expires_at: entry.expires_at,
    }
}

// ---------------------------------------------------------------------------
// Wire-trait implementation for `EventsService`.
//
// Inlined directly on the wire types — no proto round-trip, no UFCS
// dispatch through the tonic impl. Each handler body mirrors the
// corresponding tonic [`EventsService`] method in [`super::events`]
// one-for-one; both impls access the same `EventsService` private fields
// (made `pub(super)` for this purpose) and call into the same
// `SlugResolver` / `EventStore` / `ProposalService` primitives. The two
// paths share NO code.
//
// `SlugResolver::resolve_*`, `error_classify::*`, the
// `ProposalService::propose_*` family, and `serialize_payload_wire` all
// return `WireError` directly (F.1.f.0) — no adapter required on the
// wire path.
// ---------------------------------------------------------------------------

impl<B: StorageBackend + 'static> inferadb_ledger_wire_services::EventsService
    for EventsService<B>
{
    /// Lists events with pagination and optional filtering. Mirrors the
    /// tonic [`list_events`](super::events::EventsService) handler:
    /// resolves the organization (with system bypass for `slug=0`),
    /// validates the page token, routes the read across the org-level
    /// events store and (when present) per-vault events stores, merges
    /// the results in `(timestamp_ns, event_id)` order, and returns at
    /// most `limit` entries plus a continuation token.
    async fn list_events(
        &self,
        request: w::ListEventsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListEventsResponse, WireError> {
        let req = request;

        // Resolve organization (with system bypass for slug=0). The wire
        // `OrganizationSlug` is the same `inferadb_ledger_types`
        // newtype — pass through directly.
        let resolver = SlugResolver::new(self.applied_state.clone());
        let org_id = resolver.extract_and_resolve_for_events(req.organization)?;

        // Compute limit
        let limit =
            if req.limit == 0 { DEFAULT_LIMIT } else { (req.limit as usize).min(MAX_LIMIT) };

        // Wire timestamps are already UNIX nanoseconds (`Option<u64>`);
        // collapse to boundary defaults the same way the tonic impl's
        // `timestamp_to_ns` did.
        let start_ns = req.filter.as_ref().and_then(|f| f.start_time).unwrap_or(0);
        let end_ns = req.filter.as_ref().and_then(|f| f.end_time).unwrap_or(u64::MAX);

        // `compute_filter_hash_wire` operates on wire types directly, producing
        // a byte-identical hash to the proto-shaped `compute_filter_hash`. Page
        // tokens remain interchangeable across transports.
        let query_hash = compute_filter_hash_wire(&req.filter);

        // Decode and validate page token if provided
        let resume_key = if req.page_token.is_empty() {
            None
        } else {
            let token = self
                .page_token_codec
                .decode_event(&req.page_token)
                .map_err(|e| WireError::new(ErrorCode::InvalidArgument, e.to_string()))?;

            self.page_token_codec
                .validate_event_context(&token, org_id, query_hash)
                .map_err(|e| WireError::new(ErrorCode::InvalidArgument, e.to_string()))?;

            Some(token.last_key)
        };

        // Build the list of events stores to read from.
        let sources: Vec<inferadb_ledger_state::EventsDatabase<B>> = match (
            req.filter.as_ref().and_then(|f| f.vault.as_ref()),
            self.vault_event_sources.as_ref(),
        ) {
            (Some(vault_slug_wire), Some(resolver_fn)) => {
                let vault_slug = SlugResolver::extract_vault_slug(Some(*vault_slug_wire))?;
                let (owning_org, vault_id) = resolver.resolve_vault_pair(vault_slug)?;
                if owning_org != org_id {
                    return Err(WireError::new(
                        ErrorCode::NotFound,
                        format!(
                            "Vault {} does not belong to organization {}",
                            vault_slug.value(),
                            org_id.value(),
                        ),
                    ));
                }
                let mut entries = resolver_fn(org_id);
                let matched = entries.iter().position(|s| s.vault_id == vault_id);
                match matched {
                    Some(idx) => vec![entries.swap_remove(idx).events_db],
                    None => Vec::new(),
                }
            },
            (Some(_vault_slug_wire), None) => {
                return Err(WireError::new(
                    ErrorCode::FailedPrecondition,
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

        // Read from every source.
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
                    if matches_filter_wire(&entry, &req.filter) {
                        merged.push(entry);
                        filtered_from_source += 1;
                    }
                }

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

        // Sort merged entries by storage-key order.
        merged.sort_by(|a, b| {
            let a_ts = a.timestamp.timestamp_nanos_opt().unwrap_or(i64::MAX) as u64;
            let b_ts = b.timestamp.timestamp_nanos_opt().unwrap_or(i64::MAX) as u64;
            (a_ts, seahash::hash(&a.event_id)).cmp(&(b_ts, seahash::hash(&b.event_id)))
        });

        let has_more = merged.len() > limit || any_source_has_more;
        merged.truncate(limit);

        // Build next page token from the last returned entry.
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

        // Convert each domain `EventEntry` directly to the wire shape via
        // [`domain_to_wire_event_entry`] — no proto round-trip.
        let entries: Vec<w::EventEntry> = merged.iter().map(domain_to_wire_event_entry).collect();

        tracing::info!(
            service = "EventsService",
            method = "ListEvents",
            org_id = org_id.value(),
            returned = entries.len(),
            sources = sources.len(),
            has_more = has_more,
            "Events query completed"
        );

        Ok(w::ListEventsResponse {
            entries,
            next_page_token: next_page_token.unwrap_or_default(),
            total_estimate: None,
        })
    }

    /// Retrieves a single event by its 16-byte event ID.
    async fn get_event(
        &self,
        request: w::GetEventRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetEventResponse, WireError> {
        let req = request;

        let resolver = SlugResolver::new(self.applied_state.clone());
        let org_id = resolver.extract_and_resolve_for_events(req.organization)?;

        if req.event_id.len() != 16 {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                format!("event_id must be 16 bytes, got {}", req.event_id.len()),
            ));
        }

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
                Ok(w::GetEventResponse { entry: Some(domain_to_wire_event_entry(&entry)) })
            },
            None => Err(WireError::new(ErrorCode::NotFound, "Event not found")),
        }
    }

    /// Counts events matching optional filter criteria.
    async fn count_events(
        &self,
        request: w::CountEventsRequest,
        _ctx: RequestContext,
    ) -> Result<w::CountEventsResponse, WireError> {
        let req = request;

        let resolver = SlugResolver::new(self.applied_state.clone());
        let org_id = resolver.extract_and_resolve_for_events(req.organization)?;

        let start_ns = req.filter.as_ref().and_then(|f| f.start_time).unwrap_or(0);
        let end_ns = req.filter.as_ref().and_then(|f| f.end_time).unwrap_or(u64::MAX);

        let txn = self.events_db.read().map_err(|e| error_classify::storage_error(&e))?;

        let has_memory_filters = req.filter.as_ref().is_some_and(|f| {
            !f.actions.is_empty()
                || f.event_type_prefix.as_ref().is_some_and(|p| !p.is_empty())
                || f.principal.as_ref().is_some_and(|p| !p.is_empty())
                || (f.outcome as i32) != 0
                || (f.emission_path as i32) != 0
                || f.correlation_id.as_ref().is_some_and(|c| !c.is_empty())
        });

        let count = if !has_memory_filters && start_ns == 0 && end_ns == u64::MAX {
            EventStore::count(&txn, org_id).map_err(|e| error_classify::storage_error(&e))?
        } else {
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
                    if matches_filter_wire(entry, &req.filter) {
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

        Ok(w::CountEventsResponse { count })
    }

    /// Ingests a batch of external events from an authorized source service.
    #[allow(clippy::too_many_lines)]
    async fn ingest_events(
        &self,
        request: w::IngestEventsRequest,
        _ctx: RequestContext,
    ) -> Result<w::IngestEventsResponse, WireError> {
        let start = std::time::Instant::now();
        let req = request;

        let config = self.event_config.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::StaleRouting,
                "Events ingestion is not configured on this node",
            )
        })?;
        let node_id = self.node_id.ok_or_else(|| {
            WireError::new(
                ErrorCode::StaleRouting,
                "Events ingestion is not configured on this node",
            )
        })?;

        let source = &req.source_service;

        if !config.ingestion.ingest_enabled {
            tracing::info!(
                service = "EventsService",
                method = "IngestEvents",
                source_service = %source,
                "Ingestion disabled"
            );
            return Err(WireError::new(
                ErrorCode::StaleRouting,
                "Event ingestion is currently disabled",
            ));
        }

        if !config.ingestion.allowed_sources.iter().any(|s| s == source) {
            tracing::warn!(
                service = "EventsService",
                method = "IngestEvents",
                source_service = %source,
                "Unknown source service rejected"
            );
            return Err(WireError::new(
                ErrorCode::PermissionDenied,
                format!("Source service '{source}' is not in the allowed sources list"),
            ));
        }

        let batch_size = req.entries.len();
        if batch_size > config.ingestion.max_ingest_batch_size as usize {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Batch size {batch_size} exceeds maximum {}",
                    config.ingestion.max_ingest_batch_size
                ),
            ));
        }

        inferadb_ledger_raft::metrics::record_events_ingest_batch_size(source, batch_size);

        if batch_size == 0 {
            let duration = start.elapsed().as_secs_f64();
            inferadb_ledger_raft::metrics::record_events_ingest_duration(duration);
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
            return Ok(w::IngestEventsResponse {
                accepted_count: 0,
                rejected_count: 0,
                rejections: vec![],
            });
        }

        if let Some(limiter) = &self.ingestion_rate_limiter
            && !limiter.check(source, batch_size as u32)
        {
            inferadb_ledger_raft::metrics::record_events_ingest_rate_limited(source);
            let duration = start.elapsed().as_secs_f64();
            inferadb_ledger_raft::metrics::record_events_ingest_duration(duration);
            tracing::warn!(
                service = "EventsService",
                method = "IngestEvents",
                source_service = %source,
                batch_size = batch_size,
                "Rate limited"
            );
            return Err(WireError::new(
                ErrorCode::RateLimited,
                format!("Rate limit exceeded for source service '{source}'"),
            ));
        }

        let resolver = SlugResolver::new(self.applied_state.clone());
        let org_id = resolver.extract_and_resolve_for_events(req.organization)?;
        let organization = req.organization.map(|o| OrganizationSlug::new(o.value()));

        let now = Utc::now();
        let ttl_days = config.default_ttl_days;
        let max_details_bytes = config.max_details_size_bytes;
        let mut accepted_entries: Vec<(Option<(VaultId, VaultSlug)>, EventEntry)> =
            Vec::with_capacity(batch_size);
        let mut rejections: Vec<w::RejectedEvent> = Vec::new();

        for (idx, wire_entry) in req.entries.iter().enumerate() {
            if wire_entry.event_type.is_empty() {
                rejections.push(w::RejectedEvent {
                    index: idx as u32,
                    reason: "event_type is required".to_string(),
                });
                continue;
            }
            if wire_entry.principal.is_empty() {
                rejections.push(w::RejectedEvent {
                    index: idx as u32,
                    reason: "principal is required".to_string(),
                });
                continue;
            }

            let expected_prefix = format!("{}.", source);
            if !wire_entry.event_type.starts_with(&expected_prefix) {
                rejections.push(w::RejectedEvent {
                    index: idx as u32,
                    reason: format!(
                        "event_type '{}' must start with '{}'",
                        wire_entry.event_type, expected_prefix
                    ),
                });
                continue;
            }

            let details_size: usize =
                wire_entry.details.iter().map(|(k, v)| k.len() + v.len()).sum();
            if details_size > max_details_bytes {
                rejections.push(w::RejectedEvent {
                    index: idx as u32,
                    reason: format!(
                        "details map size {details_size} bytes exceeds maximum {max_details_bytes}"
                    ),
                });
                continue;
            }

            // Convert wire outcome to domain outcome. `Unspecified` falls
            // through to the rejection arm — same semantics as the tonic
            // path's `_` arm against `proto::EventOutcome::try_from`.
            let outcome = match wire_entry.outcome {
                w::EventOutcome::Success => EventOutcome::Success,
                w::EventOutcome::Failed => EventOutcome::Failed {
                    code: wire_entry.error_code.clone().unwrap_or_default(),
                    detail: wire_entry.error_detail.clone().unwrap_or_default(),
                },
                w::EventOutcome::Denied => EventOutcome::Denied {
                    reason: wire_entry.denial_reason.clone().unwrap_or_default(),
                },
                _ => {
                    rejections.push(w::RejectedEvent {
                        index: idx as u32,
                        reason: format!("invalid outcome value: {}", wire_entry.outcome as i32),
                    });
                    continue;
                },
            };

            // Wire `timestamp` is `Option<u64>` UNIX nanoseconds.
            let timestamp = wire_entry
                .timestamp
                .and_then(|ns| {
                    let secs = (ns / 1_000_000_000) as i64;
                    let nanos = (ns % 1_000_000_000) as u32;
                    chrono::DateTime::from_timestamp(secs, nanos)
                })
                .unwrap_or(now);

            let expires_at = if ttl_days == 0 {
                0
            } else {
                let duration = chrono::Duration::days(i64::from(ttl_days));
                let expiry = timestamp + duration;
                expiry.timestamp() as u64
            };

            let event_id = *Uuid::new_v4().as_bytes();

            let details: BTreeMap<String, String> = wire_entry.details.clone();

            let scope = DomainEventScope::Organization;

            let action = match &outcome {
                EventOutcome::Success => EventAction::WriteCommitted,
                EventOutcome::Failed { .. } => EventAction::RequestValidationFailed,
                EventOutcome::Denied { .. } => EventAction::RequestRateLimited,
            };

            let vault_slug_opt = wire_entry.vault.map(|v| VaultSlug::new(v.value()));
            let vault_routing = match vault_slug_opt {
                None => None,
                Some(slug) if slug.value() == 0 => {
                    rejections.push(w::RejectedEvent {
                        index: idx as u32,
                        reason: "vault.slug must be non-zero".to_string(),
                    });
                    continue;
                },
                Some(slug) => match resolver.resolve_vault_pair(slug) {
                    Ok((owning_org, vault_id)) if owning_org == org_id => Some((vault_id, slug)),
                    Ok((owning_org, _)) => {
                        rejections.push(w::RejectedEvent {
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
                        rejections.push(w::RejectedEvent {
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
                event_type: wire_entry.event_type.clone(),
                timestamp,
                scope,
                action,
                emission: EventEmission::HandlerPhase { node_id },
                principal: wire_entry.principal.clone(),
                organization_id: org_id,
                organization,
                vault: vault_slug_opt,
                outcome,
                details,
                block_height: None,
                trace_id: wire_entry.trace_id.clone(),
                correlation_id: wire_entry.correlation_id.clone(),
                operations_count: None,
            };

            accepted_entries.push((vault_routing, entry));
        }

        let accepted_count = accepted_entries.len() as u32;
        let rejected_count = rejections.len() as u32;
        let caller = req.caller.as_ref().map_or(0, |c| c.value());

        if !accepted_entries.is_empty() {
            let proposer = self.proposer.as_ref().cloned().ok_or_else(|| {
                WireError::new(
                    ErrorCode::StaleRouting,
                    "Event ingestion proposer is not configured on this node",
                )
            })?;

            let region = match self.manager.as_ref() {
                Some(manager) => manager.get_organization_region(org_id).ok_or_else(|| {
                    WireError::new(
                        ErrorCode::NotFound,
                        format!("Organization {} not found in routing table", org_id),
                    )
                })?,
                None => inferadb_ledger_types::Region::GLOBAL,
            };

            let mut buckets: BTreeMap<Option<(VaultId, VaultSlug)>, Vec<EventEntry>> =
                BTreeMap::new();
            for (vault_routing, entry) in accepted_entries {
                buckets.entry(vault_routing).or_default().push(entry);
            }

            let timeout = self.proposal_timeout;
            let inbound_org_slug = req.organization.map(|o| o.value());

            for (vault_routing, events) in buckets {
                let payload =
                    serialize_payload_wire(inferadb_ledger_raft::types::RaftPayload::new(
                        OrganizationRequest::IngestExternalEvents {
                            source: source.clone(),
                            events,
                        },
                        caller,
                    ))?;

                let response = match vault_routing {
                    Some((vault_id, vault_slug)) => {
                        proposer
                            .propose_to_vault_bytes(
                                region,
                                org_id,
                                vault_id,
                                inbound_org_slug,
                                Some(vault_slug.value()),
                                payload,
                                timeout,
                            )
                            .await?
                    },
                    None => {
                        proposer
                            .propose_to_organization_bytes(region, org_id, payload, timeout)
                            .await?
                    },
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
                        return Err(super::helpers::error_code_to_wire_error(code, message));
                    },
                    other => {
                        tracing::error!(
                            service = "EventsService",
                            method = "IngestEvents",
                            vault_id = vault_routing.map(|v| v.0.value()).unwrap_or(0),
                            response = ?std::mem::discriminant(&other),
                            "IngestExternalEvents returned unexpected response variant"
                        );
                        return Err(WireError::new(
                            ErrorCode::Internal,
                            "Unexpected response type from IngestExternalEvents apply handler",
                        ));
                    },
                }
            }
        }

        inferadb_ledger_raft::metrics::record_events_ingest(source, "accepted", accepted_count);
        if rejected_count > 0 {
            inferadb_ledger_raft::metrics::record_events_ingest(source, "rejected", rejected_count);
        }
        let duration = start.elapsed().as_secs_f64();
        inferadb_ledger_raft::metrics::record_events_ingest_duration(duration);

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

        Ok(w::IngestEventsResponse { accepted_count, rejected_count, rejections })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;

    use super::*;

    fn populated_event_entry() -> w::EventEntry {
        let mut details = BTreeMap::new();
        details.insert("vault".to_owned(), "vault-7".to_owned());
        details.insert("source".to_owned(), "engine".to_owned());

        w::EventEntry {
            event_id: Bytes::from_static(&[0xAA; 16]),
            source_service: "engine".to_owned(),
            event_type: "engine.write.committed".to_owned(),
            timestamp: 1_700_000_000_123_456_789,
            scope: w::EventScope::Organization,
            action: "write_committed".to_owned(),
            emission_path: w::EventEmissionPath::ApplyPhase,
            principal: "user:42".to_owned(),
            organization: Some(ws::OrganizationSlug::new(0xDEAD_BEEF)),
            vault: Some(ws::VaultSlug::new(0xCAFE_F00D)),
            outcome: w::EventOutcome::Success,
            error_code: None,
            error_detail: None,
            denial_reason: None,
            details,
            block_height: Some(99),
            node_id: Some(7),
            trace_id: Some("trace-1".to_owned()),
            correlation_id: Some("corr-1".to_owned()),
            operations_count: Some(3),
            expires_at: 1_800_000_000,
        }
    }

    fn populated_event_filter() -> w::EventFilter {
        w::EventFilter {
            start_time: Some(1_700_000_000_000_000_000),
            end_time: Some(1_800_000_000_000_000_000),
            actions: vec!["vault_created".to_owned(), "vault_deleted".to_owned()],
            event_type_prefix: Some("ledger.vault".to_owned()),
            principal: Some("user:42".to_owned()),
            outcome: w::EventOutcome::Success,
            emission_path: w::EventEmissionPath::ApplyPhase,
            correlation_id: Some("corr-7".to_owned()),
            vault: Some(ws::VaultSlug::new(7)),
        }
    }

    // -----------------------------------------------------------------------
    // Reference fixtures retained as documentation of the wire shape used
    // by `domain_to_wire_event_entry`. The previous wire ↔ proto ↔ wire
    // round-trips are gone with the proto crate; serde_json round-trips
    // verify the shapes are legal on the wire-frame format.
    // -----------------------------------------------------------------------
    #[test]
    fn populated_fixtures_are_legal_wire_shapes() {
        let entry = populated_event_entry();
        let filter = populated_event_filter();
        let entry_json = serde_json::to_vec(&entry).unwrap();
        let entry_back: w::EventEntry = serde_json::from_slice(&entry_json).unwrap();
        assert_eq!(entry, entry_back);
        let filter_json = serde_json::to_vec(&filter).unwrap();
        let filter_back: w::EventFilter = serde_json::from_slice(&filter_json).unwrap();
        assert_eq!(filter, filter_back);
    }
}
