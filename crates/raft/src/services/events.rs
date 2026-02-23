//! Events query and ingestion service implementation.
//!
//! Provides [`EventsServiceImpl`], the gRPC service that reads from the
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

use std::{collections::BTreeMap, sync::Arc};

use chrono::Utc;
use inferadb_ledger_proto::{proto, proto::events_service_server::EventsService};
use inferadb_ledger_state::{EventStore, EventsDatabase};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::events::{
    EventAction, EventConfig, EventEmission, EventEntry, EventOutcome, EventScope,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::{
    event_writer::IngestionRateLimiter,
    log_storage::AppliedStateAccessor,
    metrics,
    pagination::{EventPageToken, PageTokenCodec},
    services::slug_resolver::SlugResolver,
};

/// Default limit for `ListEvents` when the request specifies 0.
const DEFAULT_LIMIT: usize = 100;

/// Maximum limit for `ListEvents`.
const MAX_LIMIT: usize = 1000;

/// Maximum number of entries to scan in `CountEvents` to prevent
/// unbounded scans on large datasets.
const COUNT_SCAN_LIMIT: usize = 100_000;

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
pub struct EventsServiceImpl<B: StorageBackend> {
    /// Events database (local to this node).
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
}

impl<B: StorageBackend + 'static> EventsServiceImpl<B> {
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
    let mut params = String::new();
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
impl<B: StorageBackend + 'static> EventsService for EventsServiceImpl<B> {
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

        // Compute query hash for token validation
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
                .validate_event_context(&token, org_id.value(), query_hash)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            Some(token.last_key)
        };

        // Read from events database
        let txn = self
            .events_db
            .read()
            .map_err(|e| Status::internal(format!("Events database error: {e}")))?;

        // We need to over-fetch because in-memory filtering may discard some entries.
        // Fetch in batches until we have enough matching entries or run out.
        let mut result_entries = Vec::with_capacity(limit);
        let mut cursor = resume_key.clone();
        let mut has_more = false;
        let batch_size = limit.saturating_mul(4).max(256);

        loop {
            let (batch, next_cursor) =
                EventStore::list(&txn, org_id, start_ns, end_ns, batch_size, cursor.as_deref())
                    .map_err(|e| Status::internal(format!("Events query error: {e}")))?;

            if batch.is_empty() {
                break;
            }

            for entry in batch {
                if result_entries.len() >= limit {
                    has_more = true;
                    break;
                }
                if matches_filter(&entry, &req.filter) {
                    result_entries.push(entry);
                }
            }

            if result_entries.len() >= limit || next_cursor.is_none() {
                if result_entries.len() >= limit {
                    has_more = true;
                }
                break;
            }

            cursor = next_cursor;
        }

        // Truncate to limit if we collected more
        result_entries.truncate(limit);

        // Build next page token from the last returned entry
        let next_page_token = if has_more {
            result_entries.last().map(|entry| {
                let ts_ns = entry.timestamp.timestamp_nanos_opt().unwrap_or(0) as u64;
                let key = inferadb_ledger_state::encode_event_key(
                    entry.organization_id,
                    ts_ns,
                    &entry.event_id,
                );
                let token = EventPageToken {
                    version: 1,
                    organization_id: org_id.value(),
                    last_key: key,
                    query_hash,
                };
                self.page_token_codec.encode_event(&token)
            })
        } else {
            None
        };

        // Convert domain entries to proto
        let entries: Vec<proto::EventEntry> =
            result_entries.iter().map(proto::EventEntry::from).collect();

        tracing::info!(
            service = "EventsService",
            method = "ListEvents",
            org_id = org_id.value(),
            returned = entries.len(),
            has_more = has_more,
            "Events query completed"
        );

        Ok(Response::new(proto::ListEventsResponse {
            entries,
            next_page_token: next_page_token.unwrap_or_default(),
            total_estimate: None,
        }))
    }

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
        let txn = self
            .events_db
            .read()
            .map_err(|e| Status::internal(format!("Events database error: {e}")))?;

        let mut event_id_arr = [0u8; 16];
        event_id_arr.copy_from_slice(&req.event_id);

        let found = EventStore::get_by_id(&txn, org_id, &event_id_arr)
            .map_err(|e| Status::internal(format!("Events query error: {e}")))?;

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

        let txn = self
            .events_db
            .read()
            .map_err(|e| Status::internal(format!("Events database error: {e}")))?;

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
            EventStore::count(&txn, org_id)
                .map_err(|e| Status::internal(format!("Events count error: {e}")))?
        } else {
            // Slow path: scan with filters
            let mut count = 0u64;
            let mut cursor: Option<Vec<u8>> = None;
            let batch_size = 1000;

            loop {
                let (batch, next_cursor) =
                    EventStore::list(&txn, org_id, start_ns, end_ns, batch_size, cursor.as_deref())
                        .map_err(|e| Status::internal(format!("Events count error: {e}")))?;

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
        let org_slug = req.organization.as_ref().map(|o| o.slug);

        // 7. Process each entry — validate and convert to EventEntry
        let now = Utc::now();
        let ttl_days = config.default_ttl_days;
        let max_details_bytes = config.max_details_size_bytes;
        let mut accepted_entries: Vec<EventEntry> = Vec::with_capacity(batch_size);
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
                organization_slug: org_slug,
                vault_slug: proto_entry.vault.as_ref().map(|v| v.slug),
                outcome,
                details,
                block_height: None,
                trace_id: proto_entry.trace_id.clone(),
                correlation_id: proto_entry.correlation_id.clone(),
                operations_count: None,
            };

            accepted_entries.push(entry);
        }

        // 8. Write accepted events to events.db
        let accepted_count = accepted_entries.len() as u32;
        let rejected_count = rejections.len() as u32;

        if !accepted_entries.is_empty() {
            let mut txn = self
                .events_db
                .write()
                .map_err(|e| Status::internal(format!("Events write transaction failed: {e}")))?;

            for entry in &accepted_entries {
                EventStore::write(&mut txn, entry)
                    .map_err(|e| Status::internal(format!("Event write failed: {e}")))?;
            }

            txn.commit()
                .map_err(|e| Status::internal(format!("Events transaction commit failed: {e}")))?;

            // Emit per-event metrics
            for entry in &accepted_entries {
                metrics::record_event_write(
                    "handler_phase",
                    entry.action.scope().as_str(),
                    entry.action.as_str(),
                );
            }
        }

        // 9. Record ingestion metrics
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
    use std::{collections::BTreeMap, sync::Arc};

    use chrono::{TimeZone, Utc};
    use inferadb_ledger_state::EventsDatabase;
    use inferadb_ledger_store::InMemoryBackend;
    use inferadb_ledger_types::{
        OrganizationId,
        events::{
            EventAction, EventConfig, EventEmission, EventEntry, EventOutcome, EventScope,
            IngestionConfig,
        },
    };
    use parking_lot::RwLock;

    use super::*;
    use crate::{
        event_writer::IngestionRateLimiter,
        log_storage::{AppliedState, AppliedStateAccessor},
    };

    fn make_test_service() -> EventsServiceImpl<InMemoryBackend> {
        let events_db = EventsDatabase::open_in_memory().expect("open");
        let state = AppliedState::default();
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));

        EventsServiceImpl::builder()
            .events_db(events_db)
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .build()
    }

    fn make_service_with_org(slug: u64, org_id: i64) -> EventsServiceImpl<InMemoryBackend> {
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
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));

        EventsServiceImpl::builder()
            .events_db(events_db)
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .build()
    }

    /// Builds a service wired for ingestion tests with default `IngestionConfig`.
    fn make_ingest_service(slug: u64, org_id: i64) -> EventsServiceImpl<InMemoryBackend> {
        make_ingest_service_with_config(slug, org_id, IngestionConfig::default())
    }

    /// Builds a service with custom `IngestionConfig` for testing edge cases.
    fn make_ingest_service_with_config(
        slug: u64,
        org_id: i64,
        ingestion: IngestionConfig,
    ) -> EventsServiceImpl<InMemoryBackend> {
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
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        let rate_limit = ingestion.ingest_rate_limit_per_source;
        let config = EventConfig { ingestion, ..EventConfig::default() };

        EventsServiceImpl::builder()
            .events_db(events_db)
            .applied_state(accessor)
            .page_token_codec(PageTokenCodec::with_random_key())
            .event_config(Arc::new(config))
            .node_id(1_u64)
            .ingestion_rate_limiter(Arc::new(IngestionRateLimiter::new(rate_limit)))
            .build()
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
            timestamp: Utc.timestamp_opt(timestamp_secs, 0).unwrap(),
            scope: if org_id == 0 { EventScope::System } else { EventScope::Organization },
            action,
            emission: EventEmission::ApplyPhase,
            principal: "test-user".to_string(),
            organization_id: OrganizationId::new(org_id),
            organization_slug: None,
            vault_slug: None,
            outcome: EventOutcome::Success,
            details: BTreeMap::new(),
            block_height: Some(1),
            trace_id: None,
            correlation_id: None,
            operations_count: None,
        }
    }

    fn write_entries(service: &EventsServiceImpl<InMemoryBackend>, entries: &[EventEntry]) {
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
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));

        let service = EventsServiceImpl::builder()
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
        });
        let err = service.list_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
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
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));

        let service = EventsServiceImpl::builder()
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
        });
        let err = service.ingest_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn ingest_events_disabled_returns_unavailable() {
        let config = IngestionConfig { ingest_enabled: false, ..IngestionConfig::default() };
        let service = make_ingest_service_with_config(100, 1, config);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("engine.task_completed", "user-1")],
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
        });
        let err = service.ingest_events(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("rogue"));
    }

    #[tokio::test]
    async fn ingest_events_batch_size_exceeded() {
        let config = IngestionConfig { max_ingest_batch_size: 2, ..IngestionConfig::default() };
        let service = make_ingest_service_with_config(100, 1, config);
        let req = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![
                make_ingest_entry("engine.a", "u1"),
                make_ingest_entry("engine.b", "u2"),
                make_ingest_entry("engine.c", "u3"),
            ],
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
        });
        let list_resp3 = service.list_events(list_req3).await.unwrap();
        assert_eq!(list_resp3.get_ref().entries.len(), 2);
    }

    #[tokio::test]
    async fn ingest_events_rate_limit_enforcement() {
        // Very low rate limit: 1 event/sec
        let config =
            IngestionConfig { ingest_rate_limit_per_source: 1, ..IngestionConfig::default() };
        let service = make_ingest_service_with_config(100, 1, config);

        // First request: 1 event — should succeed (bucket starts full)
        let req1 = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("engine.a", "u1")],
        });
        let resp1 = service.ingest_events(req1).await.unwrap();
        assert_eq!(resp1.get_ref().accepted_count, 1);

        // Second immediate request: should be rate limited (bucket drained)
        let req2 = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("engine.b", "u2")],
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
        let service = make_ingest_service_with_config(100, 1, config);

        // "engine" drains its bucket
        let req1 = Request::new(proto::IngestEventsRequest {
            source_service: "engine".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("engine.a", "u1")],
        });
        service.ingest_events(req1).await.unwrap();

        // "control" has its own bucket — should still succeed
        let req2 = Request::new(proto::IngestEventsRequest {
            source_service: "control".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            entries: vec![make_ingest_entry("control.b", "u1")],
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
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 3);

        // Query back and verify outcomes
        let list_req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
        });
        let list_resp = service.list_events(list_req).await.unwrap();
        assert_eq!(list_resp.get_ref().entries.len(), 3);
    }

    #[tokio::test]
    async fn ingest_events_preserves_metadata() {
        let service = make_ingest_service(100, 1);
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
        });
        let resp = service.ingest_events(req).await.unwrap();
        assert_eq!(resp.get_ref().accepted_count, 1);

        // Query back and verify metadata preserved
        let list_req = Request::new(proto::ListEventsRequest {
            organization: Some(proto::OrganizationSlug { slug: 100 }),
            filter: None,
            limit: 10,
            page_token: String::new(),
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
        });

        let resp = service.list_events(req).await.unwrap();
        assert_eq!(resp.get_ref().entries.len(), 1);
    }
}
