//! Audit event listing, counting, and ingestion operations.

use inferadb_ledger_types::{OrganizationSlug, UserSlug};

use crate::{
    LedgerClient,
    error::{self, Result},
    types::events::{
        EventFilter, EventPage, EventSource, IngestResult, SdkEventEntry, SdkIngestEventEntry,
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
    /// Handler-phase audit events (events emitted as a side-effect of RPCs
    /// such as admin mutations or authorization checks) are batched through
    /// an in-memory flush queue on the server. The flusher commits into
    /// the events.db page cache (not per-flush fsync); the
    /// events become visible to `list_events` / `count_events` / `get_event`
    /// on the same node within a ~100 ms default flush interval, and reach
    /// disk on the `StateCheckpointer` cadence (~500 ms default). A query
    /// issued immediately after the RPC may not yet observe newly emitted
    /// events — they can still be in the flush queue, or waiting on the next
    /// checkpoint on a reader that goes through the raw on-disk layer. Tests
    /// and callers that require strict read-after-write audit visibility
    /// should either wait past the configured flush interval or run against
    /// a deployment configured with the handler-phase flush queue disabled
    /// (`event_writer_batch.enabled = false` via `UpdateConfig`). Apply-phase
    /// events (committed entity writes) and ingested events are not affected
    /// and remain immediately visible after a successful RPC.
    ///
    /// Pass `organization = 0` to query system-level events. `filter` controls
    /// which events are returned; an empty filter matches all events. `limit`
    /// sets the maximum results per page (0 = server default, max 1000).
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::Rpc`] if the query fails after retry attempts.
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
    /// `organization` must match the original query. `page_token` is the
    /// opaque cursor from the previous response's `next_page_token`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::Rpc`] if the query fails after retry attempts.
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
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::events::list_events(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    filter,
                    limit,
                    page_token,
                )
                .await
            }
        })
        .await
    }

    /// Retrieves a single audit event by ID.
    ///
    /// `event_id` must be a UUID string (e.g., as returned by `event_id_string()`).
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::Rpc`] with `NOT_FOUND` if the event does not
    /// exist, or if the query fails after retry attempts.
    pub async fn get_event(
        &self,
        caller: UserSlug,
        organization: OrganizationSlug,
        event_id: &str,
    ) -> Result<SdkEventEntry> {
        // Validate the UUID format up-front so an obviously bad caller input
        // returns a Validation error without a network round-trip.
        uuid::Uuid::parse_str(event_id).map_err(|e| error::SdkError::Validation {
            message: format!("invalid event_id: {e}"),
        })?;

        let pool = self.pool.clone();
        let event_id_owned = event_id.to_owned();
        self.call_with_retry("get_event", || {
            let pool = pool.clone();
            let event_id_owned = event_id_owned.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::events::get_event(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    &event_id_owned,
                )
                .await
            }
        })
        .await
    }

    /// Counts audit events matching a filter.
    ///
    /// Pass `organization = 0` to count system-level events. An empty `filter`
    /// counts all events.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::Rpc`] if the query fails after retry attempts.
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
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::events::count_events(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    filter,
                )
                .await
            }
        })
        .await
    }

    /// Ingests external audit events from Engine or Control services.
    ///
    /// Writes a batch of events into the organization's audit trail as
    /// handler-phase entries routed through the region's Raft log. `source`
    /// identifies the originating component
    /// ([`EventSource::Engine`](crate::EventSource::Engine) or
    /// [`EventSource::Control`](crate::EventSource::Control)). Individual
    /// events may be rejected; check `IngestResult::rejections` for per-event
    /// details.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SdkError::Rpc`] if ingestion fails.
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
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::events::ingest_events(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                    source,
                    events,
                )
                .await
            }
        })
        .await
    }
}
