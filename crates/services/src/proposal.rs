//! Proposal service abstraction for Raft consensus.
//!
//! Decouples gRPC service handlers from the concrete `ConsensusHandle`
//! transport, enabling unit testing with [`MockProposalService`] while
//! production code uses [`RaftProposalService`].

use std::{sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    ConsensusHandle, HandleError,
    raft_manager::RaftManager,
    types::{LedgerResponse, RaftPayload},
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{OrganizationId, Region};
use tonic::Status;

/// Serializes a `RaftPayload<R>` to postcard bytes.
///
/// Used by the typed helpers in [`ServiceContext`] to serialize
/// tier-specific payloads for proposal.
pub(crate) fn serialize_payload<R: serde::Serialize>(
    payload: RaftPayload<R>,
) -> Result<Vec<u8>, Status> {
    postcard::to_allocvec(&payload)
        .map_err(|e| Status::internal(format!("payload serialization failed: {e}")))
}

/// Simplified Raft metrics for vault genesis blocks and service context.
///
/// Exposes only the fields needed by downstream consumers (vault creation
/// populates genesis block headers).
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
/// Production code uses [`RaftProposalService`], which wraps the custom Raft
/// engine and [`RaftManager`]. Test code uses [`MockProposalService`], which
/// returns canned responses and captures proposals for assertion.
///
/// Exposed as `pub` (rather than `pub(crate)`) because `EventsService` (public)
/// accepts an `Arc<dyn ProposalService>` in its bon builder — the derived builder
/// setters inherit `pub` visibility and cannot reference a `pub(crate)` trait.
/// Consumers of the `services` crate should not implement this trait directly;
/// wire through [`RaftProposalService`].
///
/// ## Bytes-oriented interface
///
/// The primary proposal methods accept pre-serialized `postcard(RaftPayload<R>)`
/// bytes. Typed service helpers call [`serialize_payload`] to produce bytes from
/// a `RaftPayload<SystemRequest>` / `RaftPayload<OrganizationRequest>` etc.,
/// then call these methods. This decouples the serialization type from the trait
/// and avoids generic methods (which are incompatible with `dyn` trait objects).
#[tonic::async_trait]
pub trait ProposalService: Send + Sync {
    /// Proposes pre-serialized `postcard(RaftPayload<R>)` bytes to the default
    /// (GLOBAL) Raft group.
    ///
    /// Submits bytes through Raft with the given `caller` stamped into the
    /// payload at serialization time by the caller (via [`serialize_payload`]).
    /// Returns the committed response or a gRPC status error.
    async fn propose_bytes(
        &self,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, Status>;

    /// Proposes pre-serialized `postcard(RaftPayload<R>)` bytes to a specific
    /// region's Raft group.
    ///
    /// Resolves `region` to an
    /// [`OrganizationGroup`](inferadb_ledger_raft::raft_manager::OrganizationGroup)
    /// via the [`RaftManager`], then proposes through that group's Raft instance.
    ///
    /// If the local node is not the leader for the target region, returns a
    /// `NotLeader` `Status` with leader-hint `ErrorDetails` attached. Callers
    /// should propagate the status to the client; the SDK uses the hint to
    /// retry directly against the within-region leader. This method does not
    /// itself perform any cross-node forwarding.
    async fn propose_to_region_bytes(
        &self,
        region: Region,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, Status>;

    /// Proposes pre-serialized `postcard(RaftPayload<OrganizationRequest>)` bytes
    /// to a specific organization's per-org Raft group.
    ///
    /// Resolves `organization` to its `OrganizationGroup` via
    /// [`RaftManager::route_organization`], then proposes through that group's
    /// consensus handle.
    ///
    /// Returns `UNAVAILABLE` when the organization is not yet placed on this
    /// node (transitional state between `CreateOrganization` apply and
    /// `start_organization_group`). The caller should propagate this status
    /// to the client so the SDK can retry on the correct leader.
    async fn propose_to_organization_bytes(
        &self,
        region: Region,
        organization: OrganizationId,
        bytes: Vec<u8>,
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
    async fn propose_bytes(
        &self,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, Status> {
        match self.handle.propose_bytes_and_wait(bytes, timeout).await {
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

    async fn propose_to_region_bytes(
        &self,
        region: Region,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, Status> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            Status::failed_precondition("Regional proposals require RaftManager configuration")
        })?;

        // Look up the region group. If the region doesn't exist, return
        // FAILED_PRECONDITION — region creation is an explicit admin operation
        // (AdminService::create_data_region proposes CreateDataRegion to GLOBAL
        // Raft), not a lazy side-effect of the first data write.
        let region_group = match manager.get_region_group(region) {
            Ok(group) => group,
            Err(_) => {
                return Err(Status::failed_precondition(format!(
                    "Region {region} is not active on this node; create it explicitly \
                     via AdminService::create_data_region before proposing writes"
                )));
            },
        };

        // Check leadership eagerly. If we're not the data region leader, return
        // a NotLeader hint up front — the SDK's region-leader cache uses this
        // hint to retry directly against the within-region leader without going
        // through a server-side forwarding hop.
        if !region_group.handle().is_leader() {
            return Err(super::services::metadata::not_leader_status_from_handle(
                region_group.handle().as_ref(),
                Some(manager.peer_addresses()),
                format!("Not the leader for region {region}"),
            ));
        }

        match region_group.handle().propose_bytes_and_wait(bytes, timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. })
                if source.to_string().contains("Not the leader") =>
            {
                // Leadership changed between the check above and the propose.
                // Rare but possible — belt-and-suspenders to still surface the
                // NotLeader hint when the consensus engine rejects post-check.
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

    async fn propose_to_organization_bytes(
        &self,
        region: Region,
        organization: OrganizationId,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<LedgerResponse, Status> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "Per-organization proposals require RaftManager configuration",
            )
        })?;

        let org_group = manager.route_organization(organization).ok_or_else(|| {
            Status::unavailable(format!(
                "Organization {organization} is not active on this node in region {region}"
            ))
        })?;

        if !org_group.handle().is_leader() {
            return Err(super::services::metadata::not_leader_status_from_handle(
                org_group.handle().as_ref(),
                Some(manager.peer_addresses()),
                format!(
                    "Not the leader for organization {organization} in region {region}"
                ),
            ));
        }

        match org_group.handle().propose_bytes_and_wait(bytes, timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. })
                if source.to_string().contains("Not the leader") =>
            {
                Err(super::services::metadata::not_leader_status_from_handle(
                    org_group.handle().as_ref(),
                    Some(manager.peer_addresses()),
                    format!(
                        "Not the leader for organization {organization} in region {region} (lost leadership mid-propose)"
                    ),
                ))
            },
            Err(HandleError::Consensus { source, .. }) => Err(consensus_error_to_status(source)),
            Err(HandleError::Timeout { .. }) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                Err(Status::deadline_exceeded(format!(
                    "Org Raft proposal timed out after {}ms (org: {organization}, region: {region})",
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
    //! Proposals are stored as raw `postcard(RaftPayload<R>)` bytes.
    //! Use [`MockProposalService::decode_proposals`] to deserialize them for
    //! assertions in tests.

    use std::{collections::VecDeque, sync::Arc, time::Duration};

    use inferadb_ledger_raft::types::{LedgerResponse, RaftPayload};
    use inferadb_ledger_state::StateLayer;
    use inferadb_ledger_store::FileBackend;
    use inferadb_ledger_types::{OrganizationId, Region};
    use parking_lot::Mutex;
    use tonic::Status;

    use super::ProposalService;

    /// Test double for [`ProposalService`] that captures proposals and returns
    /// pre-configured responses.
    ///
    /// Thread-safe via `parking_lot::Mutex` on all interior state.
    /// Proposals are stored as raw bytes (`postcard(RaftPayload<R>)`).
    /// Use [`decode_proposals`](Self::decode_proposals) to deserialize.
    pub(crate) struct MockProposalService {
        responses: Mutex<VecDeque<Result<LedgerResponse, Status>>>,
        regional_responses: Mutex<VecDeque<Result<LedgerResponse, Status>>>,
        organization_responses: Mutex<VecDeque<Result<LedgerResponse, Status>>>,
        /// Captured global proposals as raw `postcard(RaftPayload<R>)` bytes.
        proposals: Mutex<Vec<Vec<u8>>>,
        /// Captured regional proposals as raw bytes with (region, bytes).
        regional_proposals: Mutex<Vec<(Region, Vec<u8>)>>,
        /// Captured organization proposals as raw bytes with (region, organization_id, bytes).
        organization_proposals: Mutex<Vec<(Region, OrganizationId, Vec<u8>)>>,
        regional_state_layer: Mutex<Option<Arc<StateLayer<FileBackend>>>>,
    }

    impl MockProposalService {
        /// Creates a new mock with empty queues.
        pub(crate) fn new() -> Self {
            Self {
                responses: Mutex::new(VecDeque::new()),
                regional_responses: Mutex::new(VecDeque::new()),
                organization_responses: Mutex::new(VecDeque::new()),
                proposals: Mutex::new(Vec::new()),
                regional_proposals: Mutex::new(Vec::new()),
                organization_proposals: Mutex::new(Vec::new()),
                regional_state_layer: Mutex::new(None),
            }
        }

        /// Enqueues a response for the next `propose_bytes()` call.
        pub(crate) fn enqueue(&self, response: Result<LedgerResponse, Status>) {
            self.responses.lock().push_back(response);
        }

        /// Enqueues a response for the next `propose_to_region_bytes()` call.
        #[allow(dead_code)]
        pub(crate) fn enqueue_regional(&self, response: Result<LedgerResponse, Status>) {
            self.regional_responses.lock().push_back(response);
        }

        /// Enqueues a response for the next `propose_to_organization_bytes()` call.
        #[allow(dead_code)]
        pub(crate) fn enqueue_organization(&self, response: Result<LedgerResponse, Status>) {
            self.organization_responses.lock().push_back(response);
        }

        /// Sets the state layer returned by `regional_state()`.
        #[allow(dead_code)]
        pub(crate) fn set_regional_state(&self, state: Arc<StateLayer<FileBackend>>) {
            *self.regional_state_layer.lock() = Some(state);
        }

        /// Returns the raw proposal bytes captured from `propose_bytes()` calls.
        pub(crate) fn raw_proposals(&self) -> Vec<Vec<u8>> {
            self.proposals.lock().clone()
        }

        /// Returns the raw regional proposal bytes captured from
        /// `propose_to_region_bytes()` calls.
        #[allow(dead_code)]
        pub(crate) fn raw_regional_proposals(&self) -> Vec<(Region, Vec<u8>)> {
            self.regional_proposals.lock().clone()
        }

        /// Returns the raw organization proposal bytes captured from
        /// `propose_to_organization_bytes()` calls.
        #[allow(dead_code)]
        pub(crate) fn raw_organization_proposals(&self) -> Vec<(Region, OrganizationId, Vec<u8>)> {
            self.organization_proposals.lock().clone()
        }

        /// Decodes captured `propose_bytes` calls into typed `RaftPayload<R>`.
        ///
        /// Panics if any captured bytes cannot be deserialized as `RaftPayload<R>`.
        /// Use this in tests to assert on the typed request after a service call:
        ///
        /// ```no_run
        /// # use inferadb_ledger_raft::types::{SystemRequest, RaftPayload};
        /// # let mock = crate::proposal::mock::MockProposalService::new();
        /// let decoded = mock.decode_proposals::<SystemRequest>();
        /// assert!(matches!(decoded[0].request, SystemRequest::UpdateUser { .. }));
        /// ```
        #[allow(dead_code, clippy::panic)]
        pub(crate) fn decode_proposals<R: serde::de::DeserializeOwned + std::fmt::Debug>(
            &self,
        ) -> Vec<RaftPayload<R>> {
            self.proposals
                .lock()
                .iter()
                .map(|bytes| {
                    postcard::from_bytes::<RaftPayload<R>>(bytes).unwrap_or_else(|e| {
                        panic!("failed to decode proposal bytes as RaftPayload<R>: {e}")
                    })
                })
                .collect()
        }

        /// Decodes captured `propose_to_region_bytes` calls into typed `RaftPayload<R>`.
        ///
        /// Panics if any captured bytes cannot be deserialized as `RaftPayload<R>`.
        #[allow(dead_code, clippy::panic)]
        pub(crate) fn decode_regional_proposals<
            R: serde::de::DeserializeOwned + std::fmt::Debug,
        >(
            &self,
        ) -> Vec<(Region, RaftPayload<R>)> {
            self.regional_proposals
                .lock()
                .iter()
                .map(|(region, bytes)| {
                    let payload =
                        postcard::from_bytes::<RaftPayload<R>>(bytes).unwrap_or_else(|e| {
                            panic!(
                                "failed to decode regional proposal bytes as RaftPayload<R>: {e}"
                            )
                        });
                    (*region, payload)
                })
                .collect()
        }

        /// Returns the number of captured global proposals.
        pub(crate) fn proposals_len(&self) -> usize {
            self.proposals.lock().len()
        }

        /// Returns `true` if no global proposals have been captured.
        #[allow(dead_code)]
        pub(crate) fn proposals_is_empty(&self) -> bool {
            self.proposals.lock().is_empty()
        }
    }

    #[tonic::async_trait]
    impl ProposalService for MockProposalService {
        async fn propose_bytes(
            &self,
            bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, Status> {
            self.proposals.lock().push(bytes);
            self.responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(Status::internal("no mock response enqueued")))
        }

        async fn propose_to_region_bytes(
            &self,
            region: Region,
            bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, Status> {
            self.regional_proposals.lock().push((region, bytes));
            self.regional_responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(Status::internal("no mock regional response enqueued")))
        }

        async fn propose_to_organization_bytes(
            &self,
            region: Region,
            organization: OrganizationId,
            bytes: Vec<u8>,
            _timeout: Duration,
        ) -> Result<LedgerResponse, Status> {
            self.organization_proposals.lock().push((region, organization, bytes));
            self.organization_responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(Status::internal("no mock organization response enqueued")))
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
        use inferadb_ledger_raft::types::SystemRequest;

        use super::*;

        #[tokio::test]
        async fn mock_propose_returns_enqueued_response() {
            let mock = MockProposalService::new();
            mock.enqueue(Ok(LedgerResponse::Empty));

            let payload = inferadb_ledger_raft::types::RaftPayload::new(
                SystemRequest::VerifyUserEmail {
                    email_id: inferadb_ledger_types::UserEmailId::new(1),
                },
                42,
            );
            let bytes = postcard::to_allocvec(&payload).unwrap();

            let result = mock.propose_bytes(bytes, Duration::from_secs(5)).await;

            assert!(result.is_ok());
            assert_eq!(mock.proposals_len(), 1);

            // Decode and verify caller is preserved.
            let decoded = mock.decode_proposals::<SystemRequest>();
            assert_eq!(decoded[0].caller, 42);
        }

        #[tokio::test]
        async fn mock_propose_returns_error_when_empty() {
            let mock = MockProposalService::new();

            let payload =
                inferadb_ledger_raft::types::RaftPayload::system(SystemRequest::VerifyUserEmail {
                    email_id: inferadb_ledger_types::UserEmailId::new(1),
                });
            let bytes = postcard::to_allocvec(&payload).unwrap();

            let result = mock.propose_bytes(bytes, Duration::from_secs(5)).await;

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

    fn make_system_bytes() -> Vec<u8> {
        let payload = inferadb_ledger_raft::types::RaftPayload::new(
            inferadb_ledger_raft::types::SystemRequest::VerifyUserEmail {
                email_id: inferadb_ledger_types::UserEmailId::new(1),
            },
            0,
        );
        postcard::to_allocvec(&payload).expect("serialize payload")
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
            .propose_to_region_bytes(
                Region::US_EAST_VA,
                make_system_bytes(),
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn propose_to_region_unknown_region_returns_failed_precondition() {
        let (svc, manager, _temp) = create_raft_proposal_service().await;

        // Verify the region doesn't exist yet.
        assert!(!manager.has_region(Region::US_EAST_VA), "region should not exist before propose");

        // Unknown regions now return FAILED_PRECONDITION immediately — region
        // creation is an explicit admin operation, not a lazy side-effect.
        let err = svc
            .propose_to_region_bytes(
                Region::US_EAST_VA,
                make_system_bytes(),
                Duration::from_millis(200),
            )
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn raft_metrics_returns_some() {
        let (svc, _manager, _temp) = create_raft_proposal_service().await;
        assert!(svc.raft_metrics().is_some());
    }
}
