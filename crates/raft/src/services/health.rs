//! Health check service implementation.
//!
//! Provides health status for the node and individual vaults, with
//! dependency validation for disk writability, peer reachability,
//! and Raft log lag detection.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use inferadb_ledger_proto::proto::{
    HealthCheckRequest, HealthCheckResponse, HealthStatus, health_service_server::HealthService,
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use openraft::Raft;
use tonic::{Request, Response, Status};

use crate::{
    dependency_health::DependencyHealthChecker,
    log_storage::{AppliedStateAccessor, VaultHealthStatus},
    types::LedgerTypeConfig,
};

/// Health check service with three-probe Kubernetes support and dependency validation.
///
/// Provides three-probe health checking for Kubernetes orchestration:
/// - **Startup**: passes once initialization is complete and data directory is valid
/// - **Liveness**: passes when the event loop is responsive
/// - **Readiness**: passes when the node can serve traffic and dependencies are healthy
///
/// Dependency checks (disk, peer, Raft lag) are cached with a configurable TTL
/// to prevent I/O storms from aggressive probe intervals.
pub struct HealthServiceImpl {
    /// Raft consensus handle for leadership and term queries.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The state layer.
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
    /// Shared node health state for lifecycle probes.
    health_state: crate::graceful_shutdown::HealthState,
    /// Last observed Raft term for leader election detection.
    last_observed_term: AtomicU64,
    /// Dependency health checker for disk/peer/raft-lag validation.
    dependency_checker: Option<DependencyHealthChecker>,
}

impl HealthServiceImpl {
    /// Creates a new health service.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<StateLayer<FileBackend>>,
        applied_state: AppliedStateAccessor,
        health_state: crate::graceful_shutdown::HealthState,
    ) -> Self {
        Self {
            raft,
            state,
            applied_state,
            health_state,
            last_observed_term: AtomicU64::new(0),
            dependency_checker: None,
        }
    }

    /// Attaches a dependency health checker for enhanced probe validation.
    #[must_use]
    pub fn with_dependency_checker(mut self, checker: DependencyHealthChecker) -> Self {
        self.dependency_checker = Some(checker);
        self
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
            let vault_id = inferadb_ledger_types::VaultId::new(vault_id_proto.id);
            // Get namespace_id from request, default to 0 if not provided
            let namespace_id =
                inferadb_ledger_types::NamespaceId::new(req.namespace_id.map_or(0, |n| n.id));

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
                },
                VaultHealthStatus::Diverged { expected, computed, at_height } => {
                    details.insert("health_status".to_string(), "diverged".to_string());
                    details.insert("diverged_at_height".to_string(), at_height.to_string());
                    details.insert("expected_root".to_string(), format!("{:x?}", &expected[..8]));
                    details.insert("computed_root".to_string(), format!("{:x?}", &computed[..8]));
                    (HealthStatus::Unavailable, "Vault has diverged state")
                },
                VaultHealthStatus::Recovering { started_at, attempt } => {
                    details.insert("health_status".to_string(), "recovering".to_string());
                    details.insert("recovery_started_at".to_string(), started_at.to_string());
                    details.insert("recovery_attempt".to_string(), attempt.to_string());
                    (HealthStatus::Degraded, "Vault is recovering from divergence")
                },
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
                details.insert("has_entities".to_string(), (!entities.is_empty()).to_string());
            }

            return Ok(Response::new(HealthCheckResponse {
                status: status.into(),
                message: message.to_string(),
                details,
            }));
        }

        // Check overall node health using three-probe pattern
        let metrics = self.raft.metrics().borrow().clone();
        let mut details = std::collections::HashMap::new();

        // Emit SLI metrics: quorum status and leader election detection
        let has_quorum = metrics.current_leader.is_some();
        crate::metrics::set_cluster_quorum_status(has_quorum);

        // Detect leader elections by observing Raft term changes
        let prev_term = self.last_observed_term.swap(metrics.current_term, Ordering::Relaxed);
        if prev_term > 0 && metrics.current_term > prev_term {
            // Term increased — a leader election occurred (possibly multiple)
            for _ in 0..(metrics.current_term - prev_term) {
                crate::metrics::record_leader_election();
            }
        }

        // Add Raft metrics to details
        details.insert("current_term".to_string(), metrics.current_term.to_string());
        if let Some(leader) = metrics.current_leader {
            details.insert("leader_id".to_string(), leader.to_string());
        }
        details.insert(
            "member_count".to_string(),
            metrics.membership_config.membership().nodes().count().to_string(),
        );

        // Include Kubernetes probe results in details
        details.insert("startup".to_string(), self.health_state.startup_check().to_string());
        details.insert("liveness".to_string(), self.health_state.liveness_check().to_string());
        details.insert("readiness".to_string(), self.health_state.readiness_check().to_string());
        details.insert("phase".to_string(), format!("{:?}", self.health_state.phase()));

        // Run dependency health checks if configured
        let deps_healthy = if let Some(checker) = &self.dependency_checker {
            let dep_health = checker.check_all().await;
            for (name, result) in &dep_health.details {
                details.insert(
                    format!("dep:{name}"),
                    if result.healthy {
                        format!("ok: {}", result.detail)
                    } else {
                        format!("FAIL: {}", result.detail)
                    },
                );
            }
            dep_health.all_healthy
        } else {
            true
        };

        // Determine health status based on node phase, Raft state, and dependencies
        let phase = self.health_state.phase();
        let (status, message) = match phase {
            crate::graceful_shutdown::NodePhase::Starting => {
                // Run startup validation if checker is available
                if let Some(checker) = &self.dependency_checker {
                    let startup_health = checker.check_startup();
                    for (name, result) in &startup_health.details {
                        details.insert(
                            format!("startup:{name}"),
                            if result.healthy {
                                format!("ok: {}", result.detail)
                            } else {
                                format!("FAIL: {}", result.detail)
                            },
                        );
                    }
                }
                (HealthStatus::Degraded, "Node is starting up")
            },
            crate::graceful_shutdown::NodePhase::ShuttingDown => {
                (HealthStatus::Unavailable, "Node is shutting down")
            },
            crate::graceful_shutdown::NodePhase::Ready => {
                if !deps_healthy {
                    (HealthStatus::Degraded, "Dependencies unhealthy")
                } else if metrics.current_leader.is_some() {
                    (HealthStatus::Healthy, "Node is healthy and has a leader")
                } else {
                    (HealthStatus::Degraded, "No leader elected")
                }
            },
        };

        Ok(Response::new(HealthCheckResponse {
            status: status.into(),
            message: message.to_string(),
            details,
        }))
    }
}
