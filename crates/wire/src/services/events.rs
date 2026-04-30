//! Wire types for `EventsService`.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{OrganizationSlug, UserSlug, VaultSlug};

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventScope {
    Unspecified = 0,
    System = 1,
    Organization = 2,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventOutcome {
    Unspecified = 0,
    Success = 1,
    Failed = 2,
    Denied = 3,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventEmissionPath {
    Unspecified = 0,
    ApplyPhase = 1,
    HandlerPhase = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventEntry {
    /// UUID, 16 bytes.
    pub event_id: Bytes,
    pub source_service: String,
    pub event_type: String,
    /// UNIX nanoseconds.
    pub timestamp: u64,
    pub scope: EventScope,
    pub action: String,
    pub emission_path: EventEmissionPath,
    pub principal: String,
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub outcome: EventOutcome,
    pub error_code: Option<String>,
    pub error_detail: Option<String>,
    pub denial_reason: Option<String>,
    pub details: BTreeMap<String, String>,
    pub block_height: Option<u64>,
    pub node_id: Option<u64>,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub operations_count: Option<u32>,
    pub expires_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventFilter {
    /// UNIX nanoseconds.
    pub start_time: Option<u64>,
    /// UNIX nanoseconds.
    pub end_time: Option<u64>,
    pub actions: Vec<String>,
    pub event_type_prefix: Option<String>,
    pub principal: Option<String>,
    pub outcome: EventOutcome,
    pub emission_path: EventEmissionPath,
    pub correlation_id: Option<String>,
    pub vault: Option<VaultSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListEventsRequest {
    pub organization: Option<OrganizationSlug>,
    pub filter: Option<EventFilter>,
    pub limit: u32,
    pub page_token: String,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListEventsResponse {
    pub entries: Vec<EventEntry>,
    pub next_page_token: String,
    pub total_estimate: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetEventRequest {
    pub organization: Option<OrganizationSlug>,
    pub event_id: Bytes,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetEventResponse {
    pub entry: Option<EventEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CountEventsRequest {
    pub organization: Option<OrganizationSlug>,
    pub filter: Option<EventFilter>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CountEventsResponse {
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IngestEventEntry {
    pub event_type: String,
    pub principal: String,
    pub outcome: EventOutcome,
    pub details: BTreeMap<String, String>,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub vault: Option<VaultSlug>,
    /// UNIX nanoseconds.
    pub timestamp: Option<u64>,
    pub error_code: Option<String>,
    pub error_detail: Option<String>,
    pub denial_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IngestEventsRequest {
    pub source_service: String,
    pub organization: Option<OrganizationSlug>,
    pub entries: Vec<IngestEventEntry>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RejectedEvent {
    pub index: u32,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IngestEventsResponse {
    pub accepted_count: u32,
    pub rejected_count: u32,
    pub rejections: Vec<RejectedEvent>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn list_events_request_with_filter_roundtrips() {
        let mut details = BTreeMap::new();
        details.insert("vault".to_owned(), "vault-7".to_owned());

        let req = ListEventsRequest {
            organization: Some(OrganizationSlug::new(1)),
            filter: Some(EventFilter {
                start_time: Some(1_700_000_000_000_000_000),
                end_time: Some(1_800_000_000_000_000_000),
                actions: vec!["vault_created".to_owned(), "vault_deleted".to_owned()],
                event_type_prefix: Some("ledger.vault".to_owned()),
                principal: None,
                outcome: EventOutcome::Success,
                emission_path: EventEmissionPath::ApplyPhase,
                correlation_id: None,
                vault: Some(VaultSlug::new(7)),
            }),
            limit: 100,
            page_token: String::new(),
            caller: Some(UserSlug::new(42)),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ListEventsRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }
}
