//! Proposal service abstraction for Raft consensus.
//!
//! Decouples gRPC service handlers from the concrete `openraft::Raft` type,
//! enabling unit testing with [`MockProposalService`] while production code
//! uses [`RaftProposalService`].

use std::{sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    error::classify_raft_error,
    raft_manager::RaftManager,
    types::{LedgerRequest, LedgerResponse, LedgerTypeConfig, RaftPayload},
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::Region;
use openraft::{BasicNode, Raft, RaftMetrics};
use tonic::Status;

/// Raft metrics type alias using our node ID and node types.
pub(crate) type LedgerRaftMetrics = RaftMetrics<inferadb_ledger_types::LedgerNodeId, BasicNode>;

/// Abstraction over Raft proposal submission.
///
/// Production code uses [`RaftProposalService`], which wraps `openraft::Raft`
/// and [`RaftManager`]. Test code uses [`MockProposalService`], which returns
/// canned responses and captures proposals for assertion.
#[tonic::async_trait]
pub(crate) trait ProposalService: Send + Sync {
    /// Proposes a [`LedgerRequest`] to the default (GLOBAL) Raft group.
    ///
    /// Wraps `request` + `caller` into a [`RaftPayload`] and submits it
    /// through Raft with the given timeout. Returns the committed response
    /// or a gRPC status error.
    async fn propose(
        &self,
        request: LedgerRequest,
        caller: u64,
        timeout: Duration,
    ) -> Result<LedgerResponse, Status>;

    /// Proposes a [`LedgerRequest`] to a specific region's Raft group.
    ///
    /// Resolves `region` to a [`RegionGroup`](inferadb_ledger_raft::raft_manager::RegionGroup)
    /// via the [`RaftManager`], then proposes through that group's Raft instance.
    async fn propose_to_region(
        &self,
        region: Region,
        request: LedgerRequest,
        caller: u64,
        timeout: Duration,
    ) -> Result<LedgerResponse, Status>;

    /// Returns the state layer for a specific region's Raft group.
    ///
    /// Enables direct reads from a region's state without proposing through
    /// Raft. Used by handlers that read onboarding accounts, user profiles,
    /// or other regional data.
    fn regional_state(&self, region: Region) -> Result<Arc<StateLayer<FileBackend>>, Status>;

    /// Returns current Raft metrics for the GLOBAL group.
    ///
    /// Used by vault creation to populate genesis block headers with leader ID
    /// and term. Returns `None` when metrics are unavailable (e.g., in tests).
    fn raft_metrics(&self) -> Option<LedgerRaftMetrics> {
        None
    }
}

/// Production [`ProposalService`] backed by `openraft::Raft` and [`RaftManager`].
///
/// Owns the Raft handle and manager reference, delegating proposal submission
/// to the appropriate region's Raft group.
pub(crate) struct RaftProposalService {
    raft: Arc<Raft<LedgerTypeConfig>>,
    manager: Option<Arc<RaftManager>>,
}

impl RaftProposalService {
    /// Creates a new `RaftProposalService`.
    ///
    /// `manager` is required for regional proposals. Pass `None` only
    /// in single-region setups where `propose_to_region` is never called.
    pub(crate) fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        manager: Option<Arc<RaftManager>>,
    ) -> Self {
        Self { raft, manager }
    }
}

#[tonic::async_trait]
impl ProposalService for RaftProposalService {
    async fn propose(
        &self,
        request: LedgerRequest,
        caller: u64,
        timeout: Duration,
    ) -> Result<LedgerResponse, Status> {
        let payload = RaftPayload::new(request, caller);

        let result = tokio::time::timeout(timeout, self.raft.client_write(payload)).await;

        match result {
            Ok(Ok(resp)) => Ok(resp.data),
            Ok(Err(e)) => Err(classify_raft_error(&e.to_string())),
            Err(_elapsed) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                Err(Status::deadline_exceeded(format!(
                    "Raft proposal timed out after {}ms",
                    timeout.as_millis()
                )))
            },
        }
    }

    async fn propose_to_region(
        &self,
        region: Region,
        request: LedgerRequest,
        caller: u64,
        timeout: Duration,
    ) -> Result<LedgerResponse, Status> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            Status::failed_precondition("Regional proposals require RaftManager configuration")
        })?;

        let region_group = manager.get_region_group(region).map_err(|e| {
            Status::unavailable(format!("Region {region} is not active on this node: {e}"))
        })?;

        let payload = RaftPayload::new(request, caller);

        let result = tokio::time::timeout(timeout, region_group.raft().client_write(payload)).await;

        match result {
            Ok(Ok(resp)) => Ok(resp.data),
            Ok(Err(e)) => Err(classify_raft_error(&e.to_string())),
            Err(_elapsed) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                Err(Status::deadline_exceeded(format!(
                    "Regional Raft proposal timed out after {}ms (region: {region})",
                    timeout.as_millis()
                )))
            },
        }
    }

    fn regional_state(&self, region: Region) -> Result<Arc<StateLayer<FileBackend>>, Status> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            Status::failed_precondition("Regional state access requires RaftManager configuration")
        })?;

        let region_group = manager.get_region_group(region).map_err(|e| {
            Status::unavailable(format!("Region {region} is not active on this node: {e}"))
        })?;

        Ok(region_group.state().clone())
    }

    fn raft_metrics(&self) -> Option<LedgerRaftMetrics> {
        Some(self.raft.metrics().borrow().clone())
    }
}

#[cfg(test)]
pub(crate) mod mock {
    //! Mock [`ProposalService`] for unit testing gRPC handlers.
    //!
    //! Captures proposals for assertion and returns pre-enqueued responses.

    use std::{collections::VecDeque, sync::Arc, time::Duration};

    use inferadb_ledger_raft::types::{LedgerRequest, LedgerResponse};
    use inferadb_ledger_state::StateLayer;
    use inferadb_ledger_store::FileBackend;
    use inferadb_ledger_types::Region;
    use parking_lot::Mutex;
    use tonic::Status;

    use super::ProposalService;

    /// Test double for [`ProposalService`] that captures proposals and returns
    /// pre-configured responses.
    ///
    /// Thread-safe via `parking_lot::Mutex` on all interior state.
    pub(crate) struct MockProposalService {
        responses: Mutex<VecDeque<Result<LedgerResponse, Status>>>,
        regional_responses: Mutex<VecDeque<Result<LedgerResponse, Status>>>,
        proposals: Mutex<Vec<(LedgerRequest, u64)>>,
        regional_proposals: Mutex<Vec<(Region, LedgerRequest, u64)>>,
        regional_state_layer: Mutex<Option<Arc<StateLayer<FileBackend>>>>,
    }

    impl MockProposalService {
        /// Creates a new mock with empty queues.
        pub(crate) fn new() -> Self {
            Self {
                responses: Mutex::new(VecDeque::new()),
                regional_responses: Mutex::new(VecDeque::new()),
                proposals: Mutex::new(Vec::new()),
                regional_proposals: Mutex::new(Vec::new()),
                regional_state_layer: Mutex::new(None),
            }
        }

        /// Enqueues a response for the next `propose()` call.
        pub(crate) fn enqueue(&self, response: Result<LedgerResponse, Status>) {
            self.responses.lock().push_back(response);
        }

        /// Enqueues a response for the next `propose_to_region()` call.
        #[allow(dead_code)]
        pub(crate) fn enqueue_regional(&self, response: Result<LedgerResponse, Status>) {
            self.regional_responses.lock().push_back(response);
        }

        /// Sets the state layer returned by `regional_state()`.
        #[allow(dead_code)]
        pub(crate) fn set_regional_state(&self, state: Arc<StateLayer<FileBackend>>) {
            *self.regional_state_layer.lock() = Some(state);
        }

        /// Returns a snapshot of all captured `propose()` calls.
        pub(crate) fn proposals(&self) -> Vec<(LedgerRequest, u64)> {
            self.proposals.lock().clone()
        }

        /// Returns a snapshot of all captured `propose_to_region()` calls.
        #[allow(dead_code)]
        pub(crate) fn regional_proposals(&self) -> Vec<(Region, LedgerRequest, u64)> {
            self.regional_proposals.lock().clone()
        }
    }

    #[tonic::async_trait]
    impl ProposalService for MockProposalService {
        async fn propose(
            &self,
            request: LedgerRequest,
            caller: u64,
            _timeout: Duration,
        ) -> Result<LedgerResponse, Status> {
            self.proposals.lock().push((request, caller));
            self.responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(Status::internal("no mock response enqueued")))
        }

        async fn propose_to_region(
            &self,
            region: Region,
            request: LedgerRequest,
            caller: u64,
            _timeout: Duration,
        ) -> Result<LedgerResponse, Status> {
            self.regional_proposals.lock().push((region, request, caller));
            self.regional_responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(Status::internal("no mock regional response enqueued")))
        }

        fn regional_state(&self, region: Region) -> Result<Arc<StateLayer<FileBackend>>, Status> {
            self.regional_state_layer.lock().clone().ok_or_else(|| {
                Status::failed_precondition(format!(
                    "MockProposalService: no regional state configured for {region}"
                ))
            })
        }

        fn raft_metrics(&self) -> Option<super::LedgerRaftMetrics> {
            None
        }
    }

    #[cfg(test)]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn mock_propose_returns_enqueued_response() {
            let mock = MockProposalService::new();
            mock.enqueue(Ok(LedgerResponse::Empty));

            let result = mock
                .propose(
                    LedgerRequest::System(
                        inferadb_ledger_raft::types::SystemRequest::VerifyUserEmail {
                            email_id: inferadb_ledger_types::UserEmailId::new(1),
                        },
                    ),
                    42,
                    Duration::from_secs(5),
                )
                .await;

            assert!(result.is_ok());
            let proposals = mock.proposals();
            assert_eq!(proposals.len(), 1);
            assert_eq!(proposals[0].1, 42);
        }

        #[tokio::test]
        async fn mock_propose_returns_error_when_empty() {
            let mock = MockProposalService::new();

            let result = mock
                .propose(
                    LedgerRequest::System(
                        inferadb_ledger_raft::types::SystemRequest::VerifyUserEmail {
                            email_id: inferadb_ledger_types::UserEmailId::new(1),
                        },
                    ),
                    0,
                    Duration::from_secs(5),
                )
                .await;

            assert!(result.is_err());
            assert_eq!(result.unwrap_err().code(), tonic::Code::Internal);
        }

        #[tokio::test]
        async fn mock_regional_state_returns_error_when_unconfigured() {
            let mock = MockProposalService::new();
            let result = mock.regional_state(Region::GLOBAL);
            let err = result.err().expect("expected error");
            assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        }

        #[test]
        fn mock_raft_metrics_returns_none() {
            let mock = MockProposalService::new();
            assert!(mock.raft_metrics().is_none());
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_raft::raft_manager::{RaftManagerConfig, RegionConfig};
    use inferadb_ledger_test_utils::TestDir;

    use super::*;

    async fn create_raft_proposal_service() -> (Arc<RaftProposalService>, Arc<RaftManager>, TestDir)
    {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), node_id, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(config));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let svc = Arc::new(RaftProposalService::new(system.raft().clone(), Some(manager.clone())));
        (svc, manager, temp)
    }

    #[tokio::test]
    async fn regional_state_returns_state_layer_for_valid_region() {
        let (svc, _manager, _temp) = create_raft_proposal_service().await;

        let state = svc.regional_state(Region::GLOBAL).expect("GLOBAL should be available");
        assert!(Arc::strong_count(&state) >= 1);
    }

    #[tokio::test]
    async fn regional_state_returns_unavailable_for_unknown_region() {
        let (svc, _manager, _temp) = create_raft_proposal_service().await;

        match svc.regional_state(Region::US_EAST_VA) {
            Err(err) => assert_eq!(err.code(), tonic::Code::Unavailable),
            Ok(_) => panic!("Expected UNAVAILABLE error"),
        }
    }

    #[tokio::test]
    async fn regional_state_without_manager_returns_failed_precondition() {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), node_id, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(config));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let svc = RaftProposalService::new(system.raft().clone(), None);

        match svc.regional_state(Region::GLOBAL) {
            Err(err) => assert_eq!(err.code(), tonic::Code::FailedPrecondition),
            Ok(_) => panic!("Expected FAILED_PRECONDITION error"),
        }
    }

    #[tokio::test]
    async fn propose_to_region_without_manager_returns_failed_precondition() {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = RaftManagerConfig::new(temp.path().to_path_buf(), node_id, Region::GLOBAL);
        let manager = Arc::new(RaftManager::new(config));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let svc = RaftProposalService::new(system.raft().clone(), None);

        let err = svc
            .propose_to_region(
                Region::US_EAST_VA,
                LedgerRequest::System(
                    inferadb_ledger_raft::types::SystemRequest::VerifyUserEmail {
                        email_id: inferadb_ledger_types::UserEmailId::new(1),
                    },
                ),
                0,
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn propose_to_region_unknown_region_returns_unavailable() {
        let (svc, _manager, _temp) = create_raft_proposal_service().await;

        let err = svc
            .propose_to_region(
                Region::US_EAST_VA,
                LedgerRequest::System(
                    inferadb_ledger_raft::types::SystemRequest::VerifyUserEmail {
                        email_id: inferadb_ledger_types::UserEmailId::new(1),
                    },
                ),
                0,
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn raft_metrics_returns_some() {
        let (svc, _manager, _temp) = create_raft_proposal_service().await;
        assert!(svc.raft_metrics().is_some());
    }
}
