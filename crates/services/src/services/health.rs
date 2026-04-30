//! Health check service implementation.
//!
//! Provides health status for the node and individual vaults, with
//! dependency validation for disk writability, peer reachability,
//! and Raft log lag detection.
//!
//! The wire-protocol implementation lives in [`super::health_wire`]; this
//! module owns the [`HealthService`] type and its constructors only.

use std::sync::{Arc, atomic::AtomicU64};

use inferadb_ledger_raft::{
    ConsensusHandle, dependency_health::DependencyHealthChecker, log_storage::AppliedStateAccessor,
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;

/// Health check service with three-probe Kubernetes support and dependency validation.
///
/// Provides three-probe health checking for Kubernetes orchestration:
/// - **Startup**: passes once initialization is complete and data directory is valid
/// - **Liveness**: passes when the event loop is responsive
/// - **Readiness**: passes when the node can serve traffic and dependencies are healthy
///
/// Dependency checks (disk, peer, Raft lag) are cached with a configurable TTL
/// to prevent I/O storms from aggressive probe intervals.
pub struct HealthService {
    /// Consensus handle for leadership and term queries.
    pub(super) handle: Arc<ConsensusHandle>,
    /// State layer for vault health and height queries.
    pub(super) state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    pub(super) applied_state: AppliedStateAccessor,
    /// Shared node health state for lifecycle probes.
    pub(super) health_state: inferadb_ledger_raft::graceful_shutdown::HealthState,
    /// Last observed Raft term for leader election detection.
    pub(super) last_observed_term: AtomicU64,
    /// Dependency health checker for disk/peer/raft-lag validation.
    pub(super) dependency_checker: Option<DependencyHealthChecker>,
    /// Multi-Raft manager for resolving vault health from regional applied state.
    pub(super) manager: Option<Arc<inferadb_ledger_raft::RaftManager>>,
}

impl HealthService {
    /// Creates a new health service.
    pub fn new(
        handle: Arc<ConsensusHandle>,
        state: Arc<StateLayer<FileBackend>>,
        applied_state: AppliedStateAccessor,
        health_state: inferadb_ledger_raft::graceful_shutdown::HealthState,
    ) -> Self {
        Self {
            handle,
            state,
            applied_state,
            health_state,
            last_observed_term: AtomicU64::new(0),
            dependency_checker: None,
            manager: None,
        }
    }

    /// Attaches the RaftManager for resolving vault health from regional state.
    pub fn with_manager(mut self, manager: Arc<inferadb_ledger_raft::RaftManager>) -> Self {
        self.manager = Some(manager);
        self
    }

    /// Attaches a dependency health checker for enhanced probe validation.
    #[must_use]
    pub fn with_dependency_checker(mut self, checker: DependencyHealthChecker) -> Self {
        self.dependency_checker = Some(checker);
        self
    }
}
