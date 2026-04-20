//! Audit event listing, counting, and ingestion operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, UserSlug};

use crate::{
    LedgerClient,
    error::{self, Result},
    proto_util::non_empty,
    types::events::{
        EventFilter, EventPage, EventSource, IngestRejection, IngestResult, SdkEventEntry,
        SdkIngestEventEntry,
    },
};

impl LedgerClient {
    // =========================================================================
    // Events Operations
    // =========================================================================

    /// Lists audit events for an organization with optional filtering.
    ///
    /// Returns a paginated list of events matching the filter criteria.
    /// Pass `organization = 0` to query system-level events.
    ///
    /// See [`ingest_events`](Self::ingest_events) for durability and event
    /// ID stability semantics across crash recovery.
    ///
    /// # Read-after-write visibility for handler-phase events
    ///
    /// As of Sprint 1B4, handler-phase audit events (events emitted as a
    /// side-effect of RPCs such as admin mutations or authorization checks)
    /// are batched through an in-memory flush queue on the server and fsynced
    /// within a ~100 ms default window. A `list_events` (or `count_events` /
    /// `get_event`) call issued immediately after such an RPC may not yet
    /// observe those events — they can still be in the flush queue. Tests and
    /// callers that require strict read-after-write audit visibility should
    /// either wait past the configured flush interval or run against a
    /// deployment configured with the handler-phase flush queue disabled.
    /// Apply-phase events (committed entity writes) and ingested events are
    /// not affected and remain immediately visible after a successful RPC.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
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
    /// # use inferadb_ledger_sdk::{LedgerClient, EventFilter, OrganizationSlug, UserSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let filter = EventFilter::new()
    ///     .event_type_prefix("ledger.vault")
    ///     .outcome_success();
    /// let page = client.list_events(UserSlug::new(42), organization, filter, 100).await?;
    /// for event in &page.entries {
    ///     println!("{}: {}", event.event_type, event.principal);
    /// }
    /// if page.has_next_page() {
    ///     let next = client.list_events_next(UserSlug::new(42), organization, page.next_page_token.as_deref().unwrap_or_default()).await?;
    ///     println!("Next page: {} events", next.entries.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_events(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        filter: EventFilter,
        limit: u32,
    ) -> Result<EventPage> {
        self.list_events_inner(caller, organization, filter, limit, String::new()).await
    }

    /// Continues paginating audit events from a previous response.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
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
    /// # use inferadb_ledger_sdk::{LedgerClient, EventFilter, OrganizationSlug, UserSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// # let page_token = "abc".to_string();
    /// let next_page = client.list_events_next(UserSlug::new(42), organization, &page_token).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_events_next(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        page_token: &str,
    ) -> Result<EventPage> {
        self.list_events_inner(caller, organization, EventFilter::new(), 0, page_token.to_owned())
            .await
    }

    /// Internal list_events implementation shared by `list_events` and `list_events_next`.
    async fn list_events_inner(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        filter: EventFilter,
        limit: u32,
        page_token: String,
    ) -> Result<EventPage> {
        let pool = self.pool.clone();
        self.call_with_retry("list_events", || {
            let pool = pool.clone();
            let filter = filter.clone();
            let page_token = page_token.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_events_client);

                let request = proto::ListEventsRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    filter: Some(filter.to_proto()),
                    limit,
                    page_token: page_token.clone(),
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                let response = client.list_events(tonic::Request::new(request)).await?.into_inner();

                let entries = response.entries.into_iter().map(SdkEventEntry::from_proto).collect();

                let next_page_token = non_empty(response.next_page_token);

                Ok(EventPage { entries, next_page_token, total_estimate: response.total_estimate })
            }
        })
        .await
    }

    /// Retrieves a single audit event by ID.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
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
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let event = client.get_event(UserSlug::new(42), organization, "550e8400-e29b-41d4-a716-446655440000").await?;
    /// println!("Event: {} by {}", event.event_type, event.principal);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_event(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        event_id: &str,
    ) -> Result<SdkEventEntry> {
        let event_id_bytes =
            uuid::Uuid::parse_str(event_id).map(|u| u.into_bytes().to_vec()).map_err(|e| {
                error::SdkError::Validation { message: format!("invalid event_id: {e}") }
            })?;

        let pool = self.pool.clone();
        self.call_with_retry("get_event", || {
            let pool = pool.clone();
            let event_id_bytes = event_id_bytes.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_events_client);

                let request = proto::GetEventRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    event_id: event_id_bytes.clone(),
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                let response = client.get_event(tonic::Request::new(request)).await?.into_inner();

                let entry = response
                    .entry
                    .ok_or_else(|| tonic::Status::not_found("event not found in response"))?;

                Ok(SdkEventEntry::from_proto(entry))
            }
        })
        .await
    }

    /// Counts audit events matching a filter.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
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
    /// # use inferadb_ledger_sdk::{LedgerClient, EventFilter, OrganizationSlug, UserSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let denied_count = client.count_events(UserSlug::new(42), organization, EventFilter::new().outcome_denied()).await?;
    /// println!("Denied events: {}", denied_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count_events(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        filter: EventFilter,
    ) -> Result<u64> {
        let pool = self.pool.clone();
        self.call_with_retry("count_events", || {
            let pool = pool.clone();
            let filter = filter.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_events_client);

                let request = proto::CountEventsRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    filter: Some(filter.to_proto()),
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                let response =
                    client.count_events(tonic::Request::new(request)).await?.into_inner();

                Ok(response.count)
            }
        })
        .await
    }

    /// Ingests external audit events from Engine or Control services.
    ///
    /// Writes a batch of events into the organization's audit trail as
    /// handler-phase entries routed through the region's Raft log.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    /// * `organization` - Organization slug (external identifier).
    /// * `source` - Originating component ([`EventSource::Engine`] or [`EventSource::Control`]).
    /// * `events` - Batch of events to ingest.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` if ingestion fails. Individual events may be
    /// rejected; check `IngestResult::rejections` for per-event details.
    ///
    /// # Durability
    ///
    /// A successful response indicates the ingested events are **WAL-durable**
    /// on the receiving region's Raft log — the proposal committed through
    /// consensus and applied in-memory to the region's events.db. State-DB
    /// materialization (the events.db dual-slot persist) lands on the next
    /// `StateCheckpointer` tick (~500ms default) or immediately on a snapshot,
    /// backup, or graceful shutdown. On crash, the events are re-applied from
    /// the WAL during region recovery — no events are lost.
    ///
    /// # Event ID stability across recovery
    ///
    /// The `event_id` generated for each ingested event (UUID v4) is frozen
    /// into the Raft proposal at RPC entry time — every replica and every
    /// post-recovery replay produces byte-identical event IDs. External
    /// consumers may rely on this ID stability for deduplication and
    /// correlation.
    ///
    /// Note: this stability property applies to events submitted via this
    /// RPC. Internal apply-phase audit events (generated as a side-effect
    /// of entity writes) use deterministic UUID v5s keyed on apply-batch
    /// layout, which can differ across crash+recovery — do not rely on
    /// apply-phase event IDs for long-term external persistence.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, SdkIngestEventEntry, EventOutcome, EventSource, OrganizationSlug, UserSlug};
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
    /// let result = client.ingest_events(UserSlug::new(42), organization, EventSource::Engine, events).await?;
    /// println!("Accepted: {}, Rejected: {}", result.accepted_count, result.rejected_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ingest_events(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        source: EventSource,
        events: Vec<SdkIngestEventEntry>,
    ) -> Result<IngestResult> {
        let pool = self.pool.clone();
        self.call_with_retry("ingest_events", || {
            let pool = pool.clone();
            let events = events.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_events_client);

                let request = proto::IngestEventsRequest {
                    source_service: source.as_str().to_owned(),
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    entries: events
                        .clone()
                        .into_iter()
                        .map(SdkIngestEventEntry::into_proto)
                        .collect(),
                    caller: Some(proto::UserSlug { slug: caller.value() }),
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
            }
        })
        .await
    }
}
