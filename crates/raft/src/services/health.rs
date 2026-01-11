//! Health check service implementation.
//!
//! Provides health status for the node and individual vaults.

use std::sync::Arc;

use openraft::Raft;
use parking_lot::RwLock;
use tonic::{Request, Response, Status};

use crate::proto::health_service_server::HealthService;
use crate::proto::{HealthCheckRequest, HealthCheckResponse, HealthStatus};
use crate::types::LedgerTypeConfig;

use ledger_storage::StateLayer;

/// Health service implementation.
pub struct HealthServiceImpl {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The state layer.
    #[allow(dead_code)]
    state: Arc<RwLock<StateLayer>>,
}

impl HealthServiceImpl {
    /// Create a new health service.
    pub fn new(raft: Arc<Raft<LedgerTypeConfig>>, state: Arc<RwLock<StateLayer>>) -> Self {
        Self { raft, state }
    }
}

#[tonic::async_trait]
impl HealthService for HealthServiceImpl {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let req = request.into_inner();

        // If a vault_id is specified, check vault health
        if let Some(_vault_id) = req.vault_id {
            // TODO: Check specific vault health from state layer
            return Ok(Response::new(HealthCheckResponse {
                status: HealthStatus::Healthy.into(),
                message: "Vault is healthy".to_string(),
                details: std::collections::HashMap::new(),
            }));
        }

        // Check overall node health
        let metrics = self.raft.metrics().borrow().clone();
        let mut details = std::collections::HashMap::new();

        // Add Raft metrics to details
        details.insert("current_term".to_string(), metrics.current_term.to_string());
        if let Some(leader) = metrics.current_leader {
            details.insert("leader_id".to_string(), leader.to_string());
        }
        details.insert(
            "member_count".to_string(),
            metrics
                .membership_config
                .membership()
                .nodes()
                .count()
                .to_string(),
        );

        // Determine health status based on Raft state
        let (status, message) = if metrics.current_leader.is_some() {
            (HealthStatus::Healthy, "Node is healthy and has a leader")
        } else if metrics.vote.leader_id.node_id == 0 {
            (HealthStatus::Degraded, "Node is starting up")
        } else {
            (HealthStatus::Degraded, "No leader elected")
        };

        Ok(Response::new(HealthCheckResponse {
            status: status.into(),
            message: message.to_string(),
            details,
        }))
    }
}
