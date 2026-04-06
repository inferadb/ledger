//! Mock implementation of the events gRPC service.

use std::sync::Arc;

use inferadb_ledger_proto::proto;
use tonic::{Request, Response, Status};

use super::MockState;

pub(super) struct MockEventsService {
    pub(super) state: Arc<MockState>,
}

impl MockEventsService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }

    fn now_timestamp() -> prost_types::Timestamp {
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        prost_types::Timestamp { seconds: now.as_secs() as i64, nanos: now.subsec_nanos() as i32 }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::events_service_server::EventsService for MockEventsService {
    async fn list_events(
        &self,
        request: Request<proto::ListEventsRequest>,
    ) -> Result<Response<proto::ListEventsResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let org_slug = req.organization.map_or(0, |o| o.slug);

        let events = self.state.events.read();
        let org_events: Vec<proto::EventEntry> = events
            .iter()
            .filter(|e| e.organization.as_ref().is_some_and(|o| o.slug == org_slug))
            .cloned()
            .collect();

        Ok(Response::new(proto::ListEventsResponse {
            entries: org_events,
            next_page_token: String::new(),
            total_estimate: None,
        }))
    }

    async fn get_event(
        &self,
        request: Request<proto::GetEventRequest>,
    ) -> Result<Response<proto::GetEventResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();

        let events = self.state.events.read();
        let entry = events
            .iter()
            .find(|e| e.event_id == req.event_id)
            .cloned()
            .ok_or_else(|| Status::not_found("Event not found"))?;

        Ok(Response::new(proto::GetEventResponse { entry: Some(entry) }))
    }

    async fn count_events(
        &self,
        request: Request<proto::CountEventsRequest>,
    ) -> Result<Response<proto::CountEventsResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let org_slug = req.organization.map_or(0, |o| o.slug);

        let events = self.state.events.read();
        let count = events
            .iter()
            .filter(|e| e.organization.as_ref().is_some_and(|o| o.slug == org_slug))
            .count() as u64;

        Ok(Response::new(proto::CountEventsResponse { count }))
    }

    async fn ingest_events(
        &self,
        request: Request<proto::IngestEventsRequest>,
    ) -> Result<Response<proto::IngestEventsResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let org_slug = req.organization.map_or(0, |o| o.slug);
        let accepted_count = req.entries.len() as u32;

        // Store ingested events
        let mut events = self.state.events.write();
        for entry in &req.entries {
            let event_id = uuid::Uuid::new_v4().into_bytes().to_vec();
            let (outcome, error_code, error_detail, denial_reason) = (
                entry.outcome,
                entry.error_code.clone(),
                entry.error_detail.clone(),
                entry.denial_reason.clone(),
            );

            events.push(proto::EventEntry {
                event_id,
                source_service: req.source_service.clone(),
                event_type: entry.event_type.clone(),
                timestamp: Some(entry.timestamp.unwrap_or_else(Self::now_timestamp)),
                scope: proto::EventScope::Organization as i32,
                action: entry.event_type.clone(),
                emission_path: proto::EventEmissionPath::EmissionPathHandlerPhase as i32,
                principal: entry.principal.clone(),
                organization: Some(proto::OrganizationSlug { slug: org_slug }),
                vault: entry.vault,
                outcome,
                error_code,
                error_detail,
                denial_reason,
                details: entry.details.clone(),
                block_height: None,
                node_id: None,
                trace_id: entry.trace_id.clone(),
                correlation_id: entry.correlation_id.clone(),
                operations_count: None,
                expires_at: 0,
            });
        }

        Ok(Response::new(proto::IngestEventsResponse {
            accepted_count,
            rejected_count: 0,
            rejections: vec![],
        }))
    }
}
