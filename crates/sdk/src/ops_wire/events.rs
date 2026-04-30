//! Wire-based events ops.
//!
//! Mirrors [`super::super::ops::events`]: same returned domain types, same
//! `Result<_, SdkError>` shape on the consumer side. Internal dispatch goes
//! through the wire-services-generated
//! [`EventsServiceClient`](inferadb_ledger_wire_services::EventsServiceClient)
//! instead of the proto-generated tonic client.
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests in this file cover the
//! [`RpcError`] → [`SdkError`] conversion (re-used from `ops_wire::health`)
//! and the wire-response → domain-type mapping in isolation. The four entry
//! points themselves are not exercised yet — [`LedgerClient`](crate::LedgerClient)
//! keeps dispatching through the tonic `ops::events` path until F.1 flips
//! the connection pool.

use std::sync::Arc;

use bytes::Bytes;
use inferadb_ledger_types::{OrganizationSlug, UserSlug};
use inferadb_ledger_wire::services::events as w;
use inferadb_ledger_wire_services::EventsServiceClient;
use inferadb_ledger_wire_transport::WireClient;

use crate::{
    error::{Result, SdkError},
    ops_wire::rpc_error_to_sdk_error,
    types::events::{
        EventFilter, EventPage, EventSource, IngestRejection, IngestResult, SdkEventEntry,
        SdkIngestEventEntry,
    },
};

/// Lists audit events for an organization with optional filtering.
///
/// Mirrors [`LedgerClient::list_events`](crate::LedgerClient::list_events) at
/// the dispatch layer. The call goes out as
/// [`EventsService::list_events`](inferadb_ledger_wire_services::EventsService)
/// — the server returns a paginated batch of events plus an opaque
/// `next_page_token` for follow-up calls.
///
/// `request_id` is the 128-bit frame-header correlation ID; the SDK's retry
/// loop is responsible for rotating it across attempts.
///
/// # Errors
///
/// Returns [`SdkError::Connection`] for transport / protocol failures and
/// the appropriate [`SdkError`] variant for server-returned [`WireError`]s
/// (rate-limit family, migration codes, all others land in `Rpc`).
pub(crate) async fn list_events(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    filter: EventFilter,
    limit: u32,
    page_token: String,
) -> Result<EventPage> {
    let client = EventsServiceClient::new(wire_client);
    let request = w::ListEventsRequest {
        organization: Some(organization),
        filter: Some(filter.to_wire()),
        limit,
        page_token,
        caller: Some(caller),
    };
    let response = client.list_events(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(domain_event_page_from_wire(response))
}

/// Retrieves a single audit event by ID.
///
/// Mirrors [`LedgerClient::get_event`](crate::LedgerClient::get_event) at the
/// dispatch layer. `event_id` must be a UUID string (e.g., as returned by
/// [`SdkEventEntry::event_id_string`](crate::SdkEventEntry::event_id_string));
/// invalid input maps to [`SdkError::Validation`] before the call goes out.
///
/// # Errors
///
/// Returns [`SdkError::Validation`] for malformed UUIDs,
/// [`SdkError::Connection`] for transport / protocol failures, and the
/// appropriate [`SdkError`] variant for server-returned [`WireError`]s
/// (`NOT_FOUND` for missing events lands in `Rpc`).
pub(crate) async fn get_event(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    event_id: &str,
) -> Result<SdkEventEntry> {
    let event_id_bytes = uuid::Uuid::parse_str(event_id)
        .map(|u| Bytes::copy_from_slice(&u.into_bytes()))
        .map_err(|e| SdkError::Validation { message: format!("invalid event_id: {e}") })?;

    let client = EventsServiceClient::new(wire_client);
    let request = w::GetEventRequest {
        organization: Some(organization),
        event_id: event_id_bytes,
        caller: Some(caller),
    };
    let response = client.get_event(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    let entry = response.entry.ok_or_else(|| SdkError::Rpc {
        code: inferadb_ledger_wire::ErrorCode::NotFound,
        message: "event not found in response".to_owned(),
        request_id: None,
        trace_id: None,
        error_details: None,
    })?;
    Ok(SdkEventEntry::from_wire(entry))
}

/// Counts audit events matching a filter.
///
/// Mirrors [`LedgerClient::count_events`](crate::LedgerClient::count_events)
/// at the dispatch layer. Returns the server's count of events matching
/// `filter` within `organization`.
///
/// # Errors
///
/// Same shape as [`list_events`] — transport failures become
/// [`SdkError::Connection`]; server-returned [`WireError`]s flow through
/// the shared [`rpc_error_to_sdk_error`] shim.
pub(crate) async fn count_events(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    filter: EventFilter,
) -> Result<u64> {
    let client = EventsServiceClient::new(wire_client);
    let request = w::CountEventsRequest {
        organization: Some(organization),
        filter: Some(filter.to_wire()),
        caller: Some(caller),
    };
    let response =
        client.count_events(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(response.count)
}

/// Ingests external audit events from Engine or Control services.
///
/// Mirrors [`LedgerClient::ingest_events`](crate::LedgerClient::ingest_events)
/// at the dispatch layer. Writes the supplied batch into the organization's
/// audit trail as handler-phase entries routed through the region's Raft
/// log. Individual events may be rejected; check `IngestResult::rejections`
/// for per-event details.
///
/// # Errors
///
/// Returns [`SdkError::Connection`] for transport / protocol failures and
/// the appropriate [`SdkError`] variant for server-returned [`WireError`]s.
pub(crate) async fn ingest_events(
    wire_client: Arc<WireClient>,
    request_id: u128,
    caller: UserSlug,
    organization: OrganizationSlug,
    source: EventSource,
    events: Vec<SdkIngestEventEntry>,
) -> Result<IngestResult> {
    let client = EventsServiceClient::new(wire_client);
    let request = w::IngestEventsRequest {
        source_service: source.as_str().to_owned(),
        organization: Some(organization),
        entries: events.into_iter().map(SdkIngestEventEntry::into_wire).collect(),
        caller: Some(caller),
    };
    let response =
        client.ingest_events(request, request_id).await.map_err(rpc_error_to_sdk_error)?;
    Ok(domain_ingest_result_from_wire(response))
}

/// Maps a wire `ListEventsResponse` into the SDK's domain [`EventPage`].
///
/// `next_page_token` projects the `String`-typed wire field through the
/// SDK's `Option<String>` convention (an empty string means "no more
/// pages"). `total_estimate` carries through unchanged.
fn domain_event_page_from_wire(resp: w::ListEventsResponse) -> EventPage {
    EventPage {
        entries: resp.entries.into_iter().map(SdkEventEntry::from_wire).collect(),
        next_page_token: if resp.next_page_token.is_empty() {
            None
        } else {
            Some(resp.next_page_token)
        },
        total_estimate: resp.total_estimate,
    }
}

/// Maps a wire `IngestEventsResponse` into the SDK's domain
/// [`IngestResult`].
fn domain_ingest_result_from_wire(resp: w::IngestEventsResponse) -> IngestResult {
    IngestResult {
        accepted_count: resp.accepted_count,
        rejected_count: resp.rejected_count,
        rejections: resp
            .rejections
            .into_iter()
            .map(|r| IngestRejection { index: r.index, reason: r.reason })
            .collect(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Unit coverage for the wire-response → domain-type mapping. End-to-end
    //! tests against a real `WireClient` / `WireServer` are deferred to
    //! E.7 (TestCluster migration) — at that point the four entry points
    //! get exercised in full.

    use std::collections::BTreeMap;

    use bytes::Bytes;
    use inferadb_ledger_types::VaultSlug;
    use inferadb_ledger_wire::services::{events as w, shared as ws};

    use super::*;

    fn sample_wire_entry() -> w::EventEntry {
        let mut details = BTreeMap::new();
        details.insert("resource".to_owned(), "doc:42".to_owned());
        details.insert("relation".to_owned(), "viewer".to_owned());

        w::EventEntry {
            event_id: Bytes::from(vec![0u8; 16]),
            source_service: "engine".to_owned(),
            event_type: "engine.authorization.checked".to_owned(),
            timestamp: 1_700_000_000_000_000_000,
            scope: w::EventScope::Organization,
            action: "check".to_owned(),
            emission_path: w::EventEmissionPath::HandlerPhase,
            principal: "user:alice".to_owned(),
            organization: Some(ws::OrganizationSlug::new(12345)),
            vault: Some(ws::VaultSlug::new(7)),
            outcome: w::EventOutcome::Success,
            error_code: None,
            error_detail: None,
            denial_reason: None,
            details,
            block_height: Some(42),
            node_id: Some(1),
            trace_id: Some("00-trace-span-01".to_owned()),
            correlation_id: Some("batch-7".to_owned()),
            operations_count: Some(3),
            expires_at: 0,
        }
    }

    #[test]
    fn wire_event_scope_each_variant_maps_correctly() {
        // Pin the per-variant mapping so a future addition to the wire
        // enum doesn't silently fall through to the System fallback
        // without a matching domain variant.
        use crate::types::events::EventScope;
        assert_eq!(EventScope::from_wire(w::EventScope::System), EventScope::System);
        assert_eq!(EventScope::from_wire(w::EventScope::Organization), EventScope::Organization);
        // Unspecified collapses to System — same fallback as the proto path.
        assert_eq!(EventScope::from_wire(w::EventScope::Unspecified), EventScope::System);
    }

    #[test]
    fn wire_event_emission_path_each_variant_maps_correctly() {
        use crate::types::events::EventEmissionPath;
        assert_eq!(
            EventEmissionPath::from_wire(w::EventEmissionPath::ApplyPhase),
            EventEmissionPath::ApplyPhase
        );
        assert_eq!(
            EventEmissionPath::from_wire(w::EventEmissionPath::HandlerPhase),
            EventEmissionPath::HandlerPhase
        );
        // Unspecified collapses to ApplyPhase — same fallback as the proto path.
        assert_eq!(
            EventEmissionPath::from_wire(w::EventEmissionPath::Unspecified),
            EventEmissionPath::ApplyPhase
        );
    }

    #[test]
    fn wire_event_outcome_success_maps_to_success() {
        use crate::types::events::EventOutcome;
        let outcome = EventOutcome::from_wire(w::EventOutcome::Success, None, None, None);
        assert_eq!(outcome, EventOutcome::Success);
    }

    #[test]
    fn wire_event_outcome_failed_carries_code_and_detail() {
        use crate::types::events::EventOutcome;
        let outcome = EventOutcome::from_wire(
            w::EventOutcome::Failed,
            Some("INTERNAL".to_owned()),
            Some("disk write failed".to_owned()),
            None,
        );
        assert_eq!(
            outcome,
            EventOutcome::Failed {
                code: "INTERNAL".to_owned(),
                detail: "disk write failed".to_owned(),
            }
        );
    }

    #[test]
    fn wire_event_outcome_denied_carries_reason() {
        use crate::types::events::EventOutcome;
        let outcome = EventOutcome::from_wire(
            w::EventOutcome::Denied,
            None,
            None,
            Some("rate limited".to_owned()),
        );
        assert_eq!(outcome, EventOutcome::Denied { reason: "rate limited".to_owned() });
    }

    #[test]
    fn wire_event_outcome_failed_with_missing_aux_fields_defaults() {
        // Server may produce a Failed outcome without the auxiliary
        // fields; we substitute empty strings rather than panic — same
        // fallback as the proto path.
        use crate::types::events::EventOutcome;
        let outcome = EventOutcome::from_wire(w::EventOutcome::Failed, None, None, None);
        assert_eq!(outcome, EventOutcome::Failed { code: String::new(), detail: String::new() });
    }

    #[test]
    fn wire_event_outcome_round_trip_through_to_wire() {
        // to_wire → from_wire reconstructs the same outcome for each
        // variant. Pins the symmetry of the two mappings.
        use crate::types::events::EventOutcome;
        for outcome in [
            EventOutcome::Success,
            EventOutcome::Failed {
                code: "PERMISSION_DENIED".to_owned(),
                detail: "no read".to_owned(),
            },
            EventOutcome::Denied { reason: "quota exceeded".to_owned() },
        ] {
            let (wire_kind, code, detail, reason) = outcome.to_wire();
            let round_trip = EventOutcome::from_wire(wire_kind, code, detail, reason);
            assert_eq!(round_trip, outcome);
        }
    }

    #[test]
    fn sdk_event_entry_from_wire_round_trips_all_fields() {
        // Project a fully-populated wire entry into the domain type and
        // assert each surfaced field. The wire `expires_at` field is
        // intentionally not surfaced by the domain type, so it doesn't
        // appear in this assertion.
        let wire = sample_wire_entry();
        let domain = SdkEventEntry::from_wire(wire);

        assert_eq!(domain.event_id, vec![0u8; 16]);
        assert_eq!(domain.source_service, "engine");
        assert_eq!(domain.event_type, "engine.authorization.checked");
        assert_eq!(domain.timestamp.timestamp_nanos_opt(), Some(1_700_000_000_000_000_000));
        assert_eq!(domain.action, "check");
        assert_eq!(domain.principal, "user:alice");
        assert_eq!(domain.organization.value(), 12345);
        assert_eq!(domain.vault.map(VaultSlug::value), Some(7));
        assert_eq!(domain.block_height, Some(42));
        assert_eq!(domain.node_id, Some(1));
        assert_eq!(domain.trace_id.as_deref(), Some("00-trace-span-01"));
        assert_eq!(domain.correlation_id.as_deref(), Some("batch-7"));
        assert_eq!(domain.operations_count, Some(3));
        assert_eq!(domain.details.len(), 2);
        assert_eq!(domain.details.get("resource").map(String::as_str), Some("doc:42"));
        assert_eq!(domain.details.get("relation").map(String::as_str), Some("viewer"));
    }

    #[test]
    fn sdk_event_entry_from_wire_missing_organization_defaults_to_zero() {
        // Mirrors the proto path: `organization.unwrap_or(0)`. The wire
        // type carries `Option<OrganizationSlug>`; system-level events
        // arrive with `None`.
        let mut wire = sample_wire_entry();
        wire.organization = None;
        let domain = SdkEventEntry::from_wire(wire);
        assert_eq!(domain.organization.value(), 0);
    }

    #[test]
    fn sdk_event_entry_from_wire_far_future_timestamp_collapses_to_epoch() {
        // u64 nanoseconds beyond i64::MAX (~year 2262) cannot be
        // represented by chrono::DateTime; we collapse to UNIX_EPOCH
        // rather than panicking.
        let mut wire = sample_wire_entry();
        wire.timestamp = u64::MAX;
        let domain = SdkEventEntry::from_wire(wire);
        assert_eq!(domain.timestamp, chrono::DateTime::UNIX_EPOCH);
    }

    #[test]
    fn list_events_response_to_event_page_maps_pagination() {
        let resp = w::ListEventsResponse {
            entries: vec![sample_wire_entry(), sample_wire_entry()],
            next_page_token: "cursor-abc".to_owned(),
            total_estimate: Some(42),
        };
        let page = domain_event_page_from_wire(resp);
        assert_eq!(page.entries.len(), 2);
        assert_eq!(page.next_page_token.as_deref(), Some("cursor-abc"));
        assert_eq!(page.total_estimate, Some(42));
        assert!(page.has_next_page());
    }

    #[test]
    fn list_events_response_empty_token_collapses_to_none() {
        // Wire `next_page_token: String` uses empty-string for "no more
        // pages"; the SDK domain type uses `Option<String>::None`.
        let resp = w::ListEventsResponse {
            entries: vec![],
            next_page_token: String::new(),
            total_estimate: None,
        };
        let page = domain_event_page_from_wire(resp);
        assert!(page.next_page_token.is_none());
        assert!(!page.has_next_page());
    }

    #[test]
    fn ingest_events_response_to_ingest_result_maps_rejections() {
        let resp = w::IngestEventsResponse {
            accepted_count: 8,
            rejected_count: 2,
            rejections: vec![
                w::RejectedEvent { index: 3, reason: "invalid principal".to_owned() },
                w::RejectedEvent { index: 7, reason: "vault not found".to_owned() },
            ],
        };
        let result = domain_ingest_result_from_wire(resp);
        assert_eq!(result.accepted_count, 8);
        assert_eq!(result.rejected_count, 2);
        assert_eq!(result.rejections.len(), 2);
        assert_eq!(result.rejections[0].index, 3);
        assert_eq!(result.rejections[0].reason, "invalid principal");
        assert_eq!(result.rejections[1].index, 7);
        assert_eq!(result.rejections[1].reason, "vault not found");
    }

    #[test]
    fn event_filter_to_wire_round_trip_via_outcome_builders() {
        // The SDK domain `EventFilter` builder methods (outcome_*,
        // *_phase_only) flip private fields whose wire projection is
        // non-trivial; pin each branch explicitly.
        let dt_start = chrono::DateTime::from_timestamp_nanos(1_700_000_000_000_000_000);
        let dt_end = chrono::DateTime::from_timestamp_nanos(1_800_000_000_000_000_000);
        let filter = EventFilter::new()
            .start_time(dt_start)
            .end_time(dt_end)
            .actions(["vault_created", "vault_deleted"])
            .event_type_prefix("ledger.vault")
            .principal("user:alice")
            .outcome_failed()
            .handler_phase_only()
            .correlation_id("batch-7")
            .vault(VaultSlug::new(99));

        let wire = filter.to_wire();
        assert_eq!(wire.start_time, Some(1_700_000_000_000_000_000));
        assert_eq!(wire.end_time, Some(1_800_000_000_000_000_000));
        assert_eq!(wire.actions, vec!["vault_created".to_owned(), "vault_deleted".to_owned()]);
        assert_eq!(wire.event_type_prefix.as_deref(), Some("ledger.vault"));
        assert_eq!(wire.principal.as_deref(), Some("user:alice"));
        assert_eq!(wire.outcome, w::EventOutcome::Failed);
        assert_eq!(wire.emission_path, w::EventEmissionPath::HandlerPhase);
        assert_eq!(wire.correlation_id.as_deref(), Some("batch-7"));
        assert_eq!(wire.vault.map(VaultSlug::value), Some(99));
    }

    #[test]
    fn event_filter_default_to_wire_uses_unspecified_outcome_and_path() {
        // An empty filter (no outcome / emission-path builder called)
        // must serialize to the wire `Unspecified` variants — the
        // server interprets these as "match any".
        let wire = EventFilter::new().to_wire();
        assert_eq!(wire.outcome, w::EventOutcome::Unspecified);
        assert_eq!(wire.emission_path, w::EventEmissionPath::Unspecified);
        assert_eq!(wire.start_time, None);
        assert_eq!(wire.end_time, None);
        assert!(wire.actions.is_empty());
        assert_eq!(wire.event_type_prefix, None);
        assert_eq!(wire.principal, None);
        assert_eq!(wire.correlation_id, None);
        assert_eq!(wire.vault, None);
    }

    #[test]
    fn event_filter_outcome_success_maps_to_wire_success() {
        let wire = EventFilter::new().outcome_success().to_wire();
        assert_eq!(wire.outcome, w::EventOutcome::Success);
    }

    #[test]
    fn event_filter_outcome_denied_maps_to_wire_denied() {
        let wire = EventFilter::new().outcome_denied().to_wire();
        assert_eq!(wire.outcome, w::EventOutcome::Denied);
    }

    #[test]
    fn event_filter_apply_phase_only_maps_to_wire_apply_phase() {
        let wire = EventFilter::new().apply_phase_only().to_wire();
        assert_eq!(wire.emission_path, w::EventEmissionPath::ApplyPhase);
    }

    #[test]
    fn ingest_event_entry_into_wire_carries_all_fields() {
        use crate::types::events::EventOutcome;
        let dt = chrono::DateTime::from_timestamp_nanos(1_700_000_000_000_000_000);
        let entry = SdkIngestEventEntry::new(
            "engine.authorization.checked",
            "user:alice",
            EventOutcome::Failed {
                code: "PERMISSION_DENIED".to_owned(),
                detail: "no read".to_owned(),
            },
        )
        .detail("resource", "doc:42")
        .detail("relation", "viewer")
        .trace_id("00-trace-span-01")
        .correlation_id("batch-7")
        .vault(VaultSlug::new(99))
        .timestamp(dt);

        let wire = entry.into_wire();
        assert_eq!(wire.event_type, "engine.authorization.checked");
        assert_eq!(wire.principal, "user:alice");
        assert_eq!(wire.outcome, w::EventOutcome::Failed);
        assert_eq!(wire.error_code.as_deref(), Some("PERMISSION_DENIED"));
        assert_eq!(wire.error_detail.as_deref(), Some("no read"));
        assert_eq!(wire.denial_reason, None);
        assert_eq!(wire.details.len(), 2);
        assert_eq!(wire.details.get("resource").map(String::as_str), Some("doc:42"));
        assert_eq!(wire.trace_id.as_deref(), Some("00-trace-span-01"));
        assert_eq!(wire.correlation_id.as_deref(), Some("batch-7"));
        assert_eq!(wire.vault.map(VaultSlug::value), Some(99));
        assert_eq!(wire.timestamp, Some(1_700_000_000_000_000_000));
    }

    #[test]
    fn ingest_event_entry_into_wire_denied_carries_reason() {
        use crate::types::events::EventOutcome;
        let entry = SdkIngestEventEntry::new(
            "engine.authorization.checked",
            "user:alice",
            EventOutcome::Denied { reason: "rate limited".to_owned() },
        );
        let wire = entry.into_wire();
        assert_eq!(wire.outcome, w::EventOutcome::Denied);
        assert_eq!(wire.error_code, None);
        assert_eq!(wire.error_detail, None);
        assert_eq!(wire.denial_reason.as_deref(), Some("rate limited"));
    }

    #[test]
    fn ingest_event_entry_into_wire_minimal_omits_optional_fields() {
        use crate::types::events::EventOutcome;
        let entry = SdkIngestEventEntry::new(
            "engine.authorization.checked",
            "user:alice",
            EventOutcome::Success,
        );
        let wire = entry.into_wire();
        assert_eq!(wire.outcome, w::EventOutcome::Success);
        assert!(wire.details.is_empty());
        assert_eq!(wire.trace_id, None);
        assert_eq!(wire.correlation_id, None);
        assert_eq!(wire.vault, None);
        assert_eq!(wire.timestamp, None);
        assert_eq!(wire.error_code, None);
        assert_eq!(wire.error_detail, None);
        assert_eq!(wire.denial_reason, None);
    }
}
