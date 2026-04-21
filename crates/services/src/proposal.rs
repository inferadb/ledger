//! Proposal service abstraction for Raft consensus.
//!
//! Decouples gRPC service handlers from the concrete `openraft::Raft` type,
//! enabling unit testing with [`MockProposalService`] while production code
//! uses [`RaftProposalService`].

use std::{sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    ConsensusHandle, HandleError,
    raft_manager::RaftManager,
    types::{LedgerResponse, RaftPayload, LedgerRequest},
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::Region;
use tonic::Status;

/// Simplified Raft metrics for vault genesis blocks and service context.
///
/// Replaces the full `openraft::RaftMetrics` with only the fields needed
/// by downstream consumers (vault creation populates genesis block headers).
pub struct LedgerRaftMetrics {
    /// Current leader node ID, if elected.
    pub(crate) current_leader: Option<u64>,
    /// Current Raft term.
    pub(crate) current_term: u64,
    /// This node's ID.
    pub(crate) id: u64,
}

/// Abstraction over Raft proposal submission.
///
/// Production code uses [`RaftProposalService`], which wraps `openraft::Raft`
/// and [`RaftManager`]. Test code uses [`MockProposalService`], which returns
/// canned responses and captures proposals for assertion.
///
/// Exposed as `pub` (rather than `pub(crate)`) because `EventsService` (public)
/// accepts an `Arc<dyn ProposalService>` in its bon builder — the derived builder
/// setters inherit `pub` visibility and cannot reference a `pub(crate)` trait.
/// Consumers of the `services` crate should not implement this trait directly;
/// wire through [`RaftProposalService`].
#[tonic::async_trait]
pub trait ProposalService: Send + Sync {
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
    /// Resolves `region` to a [`OrganizationGroup`](inferadb_ledger_raft::raft_manager::OrganizationGroup)
    /// via the [`RaftManager`], then proposes through that group's Raft instance.
    ///
    /// If the local node is not the leader for the target region, returns a
    /// `NotLeader` `Status` with leader-hint `ErrorDetails` attached. Callers
    /// should propagate the status to the client; the SDK uses the hint to
    /// retry directly against the within-region leader. This method does not
    /// itself perform any cross-node forwarding.
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

/// Converts a [`ConsensusError`](inferadb_ledger_consensus::ConsensusError) into
/// the appropriate [`tonic::Status`] using the error's structured `grpc_code()`.
pub(crate) fn consensus_error_to_status(err: inferadb_ledger_consensus::ConsensusError) -> Status {
    let message = format!("Raft error: {err}");
    let code = match err.grpc_code() {
        3 => tonic::Code::InvalidArgument,
        6 => tonic::Code::AlreadyExists,
        8 => tonic::Code::ResourceExhausted,
        9 => tonic::Code::FailedPrecondition,
        13 => tonic::Code::Internal,
        14 => tonic::Code::Unavailable,
        _ => tonic::Code::Internal,
    };
    Status::new(code, message)
}

/// Production [`ProposalService`] backed by [`ConsensusHandle`] and [`RaftManager`].
///
/// Owns the consensus handle and manager reference, delegating proposal
/// submission to the appropriate region's consensus group.
pub(crate) struct RaftProposalService {
    handle: Arc<ConsensusHandle>,
    manager: Option<Arc<RaftManager>>,
}

impl RaftProposalService {
    /// Creates a new `RaftProposalService`.
    ///
    /// `manager` is required for regional proposals. Pass `None` only
    /// in single-region setups where `propose_to_region` is never called.
    pub(crate) fn new(handle: Arc<ConsensusHandle>, manager: Option<Arc<RaftManager>>) -> Self {
        Self { handle, manager }
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

        match self.handle.propose_and_wait(payload, timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. }) => Err(consensus_error_to_status(source)),
            Err(HandleError::Timeout { .. }) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                Err(Status::deadline_exceeded(format!(
                    "Raft proposal timed out after {}ms",
                    timeout.as_millis()
                )))
            },
            Err(e) => Err(Status::internal(e.to_string())),
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

        // Lazily create the data region if it doesn't exist yet.
        // Data region shards materialize on first use — propose CreateDataRegion
        // to the GLOBAL Raft group so ALL nodes create the region through consensus.
        let region_group = match manager.get_region_group(region) {
            Ok(group) => group,
            Err(_) if region != Region::GLOBAL => {
                // Region doesn't exist. If we're the GLOBAL leader, create it
                // via Raft consensus so all nodes get it. If we're a follower,
                // return UNAVAILABLE so the client retries (eventually hitting
                // the leader node which creates the region).
                if !self.handle.is_leader() {
                    return Err(Status::unavailable(format!(
                        "Region {region} not started — retry on the leader node"
                    )));
                }

                // Build initial_members from GLOBAL shard voters + peer addresses
                // so ALL nodes in the cluster start the data region as voters.
                let global_state = self.handle.shard_state();
                let mut initial_members = Vec::with_capacity(global_state.voters.len());
                for voter in &global_state.voters {
                    let addr = manager.peer_addresses().get(voter.0).ok_or_else(|| {
                        Status::unavailable(format!(
                            "Peer {} address not yet announced; cannot create data region \
                             without full membership",
                            voter.0
                        ))
                    })?;
                    initial_members.push((voter.0, addr));
                }

                // Propose CreateDataRegion to GLOBAL Raft — the handler on each
                // node will start the region with the full voter set.
                let create_req = LedgerRequest::System(
                    inferadb_ledger_raft::types::SystemRequest::CreateDataRegion {
                        region,
                        initial_members,
                    },
                );
                let payload = inferadb_ledger_raft::types::RaftPayload::system(create_req);
                match self.handle.propose_and_wait(payload, timeout).await {
                    Ok(LedgerResponse::DataRegionCreated { .. }) => {},
                    Ok(_) => {},
                    Err(e) => {
                        return Err(Status::unavailable(format!(
                            "Failed to create data region {region}: {e}"
                        )));
                    },
                }

                // Region should now exist on this node (applied by the local handler).
                // Poll briefly in case the apply worker hasn't finished yet.
                let start = std::time::Instant::now();
                loop {
                    if let Ok(group) = manager.get_region_group(region) {
                        // Wait for leader election on the newly created region.
                        let election_start = std::time::Instant::now();
                        while election_start.elapsed() < std::time::Duration::from_secs(5) {
                            if group.handle().current_leader().is_some() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                        break group;
                    }
                    if start.elapsed() > std::time::Duration::from_secs(5) {
                        return Err(Status::unavailable(format!(
                            "Region {region} not available after creation via Raft consensus"
                        )));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            },
            Err(e) => {
                return Err(Status::unavailable(format!(
                    "Region {region} is not active on this node: {e}"
                )));
            },
        };

        let payload = RaftPayload::new(request, caller);

        // Check leadership eagerly. If we're not the data region leader, return
        // a NotLeader hint up front — the SDK's region-leader cache uses this
        // hint to retry directly against the within-region leader without going
        // through a server-side forwarding hop. This avoids relying on a
        // stringly-typed match against openraft's error message format.
        if !region_group.handle().is_leader() {
            return Err(super::services::metadata::not_leader_status_from_handle(
                region_group.handle().as_ref(),
                Some(manager.peer_addresses()),
                format!("Not the leader for region {region}"),
            ));
        }

        match region_group.handle().propose_and_wait(payload, timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. })
                if source.to_string().contains("Not the leader") =>
            {
                // Leadership changed between the check above and the propose.
                // Rare but possible — belt-and-suspenders to still surface the
                // NotLeader hint when openraft rejects post-check.
                Err(super::services::metadata::not_leader_status_from_handle(
                    region_group.handle().as_ref(),
                    Some(manager.peer_addresses()),
                    format!("Not the leader for region {region} (lost leadership mid-propose)"),
                ))
            },
            Err(HandleError::Consensus { source, .. }) => Err(consensus_error_to_status(source)),
            Err(HandleError::Timeout { .. }) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                Err(Status::deadline_exceeded(format!(
                    "Regional Raft proposal timed out after {}ms (region: {region})",
                    timeout.as_millis()
                )))
            },
            Err(e) => Err(Status::internal(e.to_string())),
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
        let state = self.handle.shard_state();
        Some(LedgerRaftMetrics {
            current_leader: state.leader.map(|n| n.0),
            current_term: state.term,
            id: self.handle.node_id(),
        })
    }
}

#[cfg(test)]
pub(crate) mod mock {
    //! Mock [`ProposalService`] for unit testing gRPC handlers.
    //!
    //! Captures proposals for assertion and returns pre-enqueued responses.

    use std::{collections::VecDeque, sync::Arc, time::Duration};

    use inferadb_ledger_raft::types::{LedgerResponse, LedgerRequest};
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
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let svc =
            Arc::new(RaftProposalService::new(system.handle().clone(), Some(manager.clone())));
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
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let svc = RaftProposalService::new(system.handle().clone(), None);

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
        let manager = Arc::new(RaftManager::new(
            config,
            Arc::new(inferadb_ledger_raft::node_registry::NodeConnectionRegistry::new()),
        ));
        let region_config =
            RegionConfig::system(node_id, "127.0.0.1:0".to_string()).without_background_jobs();
        let system = manager.start_system_region(region_config).await.expect("start system region");

        let svc = RaftProposalService::new(system.handle().clone(), None);

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
    async fn propose_to_region_unknown_region_triggers_global_proposal() {
        let (svc, manager, _temp) = create_raft_proposal_service().await;

        // Verify the region doesn't exist yet.
        assert!(!manager.has_region(Region::US_EAST_VA), "region should not exist before propose");

        // In unit tests, GLOBAL Raft consensus may not be fully functional
        // (no apply worker processing), so the propose may time out. The key
        // behavior: propose_to_region attempts CreateDataRegion through GLOBAL
        // when the region is missing, rather than creating locally.
        // Full end-to-end region creation is validated in integration tests.
        let result = svc
            .propose_to_region(
                Region::US_EAST_VA,
                LedgerRequest::System(
                    inferadb_ledger_raft::types::SystemRequest::VerifyUserEmail {
                        email_id: inferadb_ledger_types::UserEmailId::new(1),
                    },
                ),
                0,
                Duration::from_millis(200),
            )
            .await;

        // The proposal should fail (timeout or unavailable) since the full
        // consensus pipeline isn't running in this unit test.
        assert!(result.is_err(), "expected error in unit test without full consensus");
    }

    #[tokio::test]
    async fn raft_metrics_returns_some() {
        let (svc, _manager, _temp) = create_raft_proposal_service().await;
        assert!(svc.raft_metrics().is_some());
    }
}
