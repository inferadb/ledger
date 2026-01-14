//! Health check service implementation.
//!
//! Provides health status for the node and individual vaults.

use std::sync::Arc;

use openraft::Raft;
use tonic::{Request, Response, Status};

use crate::log_storage::{AppliedStateAccessor, VaultHealthStatus};
use crate::proto::health_service_server::HealthService;
use crate::proto::{HealthCheckRequest, HealthCheckResponse, HealthStatus};
use crate::types::LedgerTypeConfig;

use inkwell::FileBackend;
use ledger_storage::StateLayer;

/// Health service implementation.
pub struct HealthServiceImpl {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The state layer.
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
}

impl HealthServiceImpl {
    /// Create a new health service.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<StateLayer<FileBackend>>,
        applied_state: AppliedStateAccessor,
    ) -> Self {
        Self {
            raft,
            state,
            applied_state,
        }
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
        if let Some(vault_id_proto) = req.vault_id {
            let vault_id = vault_id_proto.id;
            // Get namespace_id from request, default to 0 if not provided
            let namespace_id = req.namespace_id.map(|n| n.id).unwrap_or(0);

            let mut details = std::collections::HashMap::new();

            // Get vault height
            let height = self.applied_state.vault_height(namespace_id, vault_id);
            details.insert("block_height".to_string(), height.to_string());

            // Get vault health status
            let health_status = self.applied_state.vault_health(namespace_id, vault_id);
            let (status, message) = match &health_status {
                VaultHealthStatus::Healthy => {
                    details.insert("health_status".to_string(), "healthy".to_string());
                    (HealthStatus::Healthy, "Vault is healthy")
                }
                VaultHealthStatus::Diverged {
                    expected,
                    computed,
                    at_height,
                } => {
                    details.insert("health_status".to_string(), "diverged".to_string());
                    details.insert("diverged_at_height".to_string(), at_height.to_string());
                    details.insert(
                        "expected_root".to_string(),
                        format!("{:x?}", &expected[..8]),
                    );
                    details.insert(
                        "computed_root".to_string(),
                        format!("{:x?}", &computed[..8]),
                    );
                    (HealthStatus::Unavailable, "Vault has diverged state")
                }
                VaultHealthStatus::Recovering {
                    started_at,
                    attempt,
                } => {
                    details.insert("health_status".to_string(), "recovering".to_string());
                    details.insert("recovery_started_at".to_string(), started_at.to_string());
                    details.insert("recovery_attempt".to_string(), attempt.to_string());
                    (
                        HealthStatus::Degraded,
                        "Vault is recovering from divergence",
                    )
                }
            };

            // Get vault metadata (last write timestamp)
            if let Some(vault_meta) = self.applied_state.get_vault(namespace_id, vault_id) {
                if vault_meta.last_write_timestamp > 0 {
                    details.insert(
                        "last_write_timestamp".to_string(),
                        vault_meta.last_write_timestamp.to_string(),
                    );
                }
                if let Some(name) = &vault_meta.name {
                    details.insert("vault_name".to_string(), name.clone());
                }
            }

            // Get entity count from state layer (StateLayer is internally thread-safe)
            if let Ok(entities) = self.state.list_entities(vault_id, None, None, 1) {
                // We fetch 1 to check if any exist, actual count needs full scan
                // For now, indicate whether vault has entities
                details.insert(
                    "has_entities".to_string(),
                    (!entities.is_empty()).to_string(),
                );
            }

            return Ok(Response::new(HealthCheckResponse {
                status: status.into(),
                message: message.to_string(),
                details,
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
