use std::sync::Arc;

use inferadb_ledger_proto::proto;
use tonic::{Request, Response, Status};

use super::MockState;

pub(super) struct MockHealthService {
    pub(super) state: Arc<MockState>,
}

impl MockHealthService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::health_service_server::HealthService for MockHealthService {
    async fn check(
        &self,
        _request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        self.state.check_injection().await?;

        Ok(Response::new(proto::HealthCheckResponse {
            status: proto::HealthStatus::Healthy as i32,
            message: "Mock server healthy".to_string(),
            details: std::collections::HashMap::new(),
        }))
    }
}
