//! Audit event listing, counting, and ingestion operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, UserSlug};

use crate::{
    LedgerClient,
    error::{self, Result},
    proto_util::non_empty,
    retry::with_retry_cancellable,
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
                    let mut client = crate::connected_client!(pool, create_events_client);

                    let request = proto::ListEventsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        filter: Some(filter.to_proto()),
                        limit,
                        page_token: page_token.clone(),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response =
                        client.list_events(tonic::Request::new(request)).await?.into_inner();

                    let entries =
                        response.entries.into_iter().map(SdkEventEntry::from_proto).collect();

                    let next_page_token = non_empty(response.next_page_token);

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
                    let mut client = crate::connected_client!(pool, create_events_client);

                    let request = proto::GetEventRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        event_id: event_id_bytes.clone(),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
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
                    let mut client = crate::connected_client!(pool, create_events_client);

                    let request = proto::CountEventsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        filter: Some(filter.to_proto()),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
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
                },
            ),
        )
        .await
    }
}
