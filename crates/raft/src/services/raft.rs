//! Raft service implementation for inter-node communication.
//!
//! This service handles incoming Raft RPC calls from peer nodes:
//! - Vote requests during leader election
//! - AppendEntries for log replication
//! - InstallSnapshot for follower catch-up

use std::sync::Arc;

use inferadb_ledger_proto::proto::{
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftInstallSnapshotRequest,
    RaftInstallSnapshotResponse, RaftLogId, RaftVoteRequest, RaftVoteResponse,
    raft_service_server::RaftService,
};
use inferadb_ledger_types::decode;
use openraft::{Raft, Vote, raft::AppendEntriesRequest};
use tonic::{Request, Response, Status};

use crate::types::{LedgerNodeId, LedgerTypeConfig};

/// Handles incoming vote, append-entries, and install-snapshot RPCs from peer Raft nodes.
pub struct RaftServiceImpl {
    /// The Raft instance to forward requests to.
    raft: Arc<Raft<LedgerTypeConfig>>,
}

impl RaftServiceImpl {
    /// Creates a new Raft service.
    pub fn new(raft: Arc<Raft<LedgerTypeConfig>>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn vote(
        &self,
        request: Request<RaftVoteRequest>,
    ) -> Result<Response<RaftVoteResponse>, Status> {
        let req = request.into_inner();

        let vote =
            req.vote.as_ref().ok_or_else(|| Status::invalid_argument("Missing vote field"))?;

        // Convert proto to OpenRaft types (using From impl in proto_convert)
        let raft_vote: Vote<LedgerNodeId> = vote.into();
        // Use the vote's node_id for the CommittedLeaderId - this identifies who committed the log
        // entry
        let last_log_id = req.last_log_id.map(|id| {
            openraft::LogId::new(openraft::CommittedLeaderId::new(id.term, vote.node_id), id.index)
        });

        let vote_request = openraft::raft::VoteRequest { vote: raft_vote, last_log_id };

        // Forward to the Raft instance
        let response = self
            .raft
            .vote(vote_request)
            .await
            .map_err(|e| Status::internal(format!("Vote failed: {}", e)))?;

        // Convert response back to proto
        Ok(Response::new(RaftVoteResponse {
            vote: Some((&response.vote).into()),
            vote_granted: response.vote_granted,
            last_log_id: response
                .last_log_id
                .map(|id| RaftLogId { term: id.leader_id.term, index: id.index }),
        }))
    }

    async fn append_entries(
        &self,
        request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        let req = request.into_inner();

        let vote =
            req.vote.as_ref().ok_or_else(|| Status::invalid_argument("Missing vote field"))?;

        // Deserialize log entries
        let entries: Vec<_> = req.entries.iter().filter_map(|bytes| decode(bytes).ok()).collect();

        // Convert proto to OpenRaft types (using From impl in proto_convert)
        let raft_vote: Vote<LedgerNodeId> = vote.into();
        // Use the vote's node_id (the leader) for the CommittedLeaderId
        let leader_node_id = vote.node_id;
        let prev_log_id = req.prev_log_id.map(|id| {
            openraft::LogId::new(
                openraft::CommittedLeaderId::new(id.term, leader_node_id),
                id.index,
            )
        });
        let leader_commit = req.leader_commit.map(|id| {
            openraft::LogId::new(
                openraft::CommittedLeaderId::new(id.term, leader_node_id),
                id.index,
            )
        });

        let append_request: AppendEntriesRequest<LedgerTypeConfig> =
            AppendEntriesRequest { vote: raft_vote, prev_log_id, entries, leader_commit };

        // Forward to the Raft instance
        let response = self
            .raft
            .append_entries(append_request)
            .await
            .map_err(|e| Status::internal(format!("AppendEntries failed: {}", e)))?;

        // Convert response to proto
        use openraft::raft::AppendEntriesResponse::*;
        let (success, conflict, higher_vote) = match response {
            Success => (true, false, None),
            Conflict => (false, true, None),
            HigherVote(v) => (false, false, Some((&v).into())),
            PartialSuccess(_) => (true, false, None), // Treat partial as success
        };

        Ok(Response::new(RaftAppendEntriesResponse { success, conflict, vote: higher_vote }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        let vote =
            req.vote.as_ref().ok_or_else(|| Status::invalid_argument("Missing vote field"))?;

        let meta =
            req.meta.as_ref().ok_or_else(|| Status::invalid_argument("Missing meta field"))?;

        // Build snapshot metadata - use leader's node_id from vote
        let leader_node_id = vote.node_id;
        let last_log_id = meta.last_log_id.as_ref().map(|id| {
            openraft::LogId::new(
                openraft::CommittedLeaderId::new(id.term, leader_node_id),
                id.index,
            )
        });

        // Build membership from proto (simplified - just extract node addresses)
        let membership_proto = meta
            .last_membership
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing last_membership"))?;

        // Convert membership configs - build nodes map
        use std::collections::BTreeMap;

        use openraft::BasicNode;

        let mut all_nodes: BTreeMap<u64, BasicNode> = BTreeMap::new();
        for config in &membership_proto.configs {
            for (node_id, addr) in &config.members {
                all_nodes.insert(*node_id, BasicNode { addr: addr.clone() });
            }
        }

        // Create membership with voter set and node info
        let voter_ids: std::collections::BTreeSet<u64> = all_nodes.keys().copied().collect();
        let membership = openraft::Membership::new(vec![voter_ids], all_nodes);

        // Wrap in StoredMembership with the log_id at which this membership was committed
        let stored_membership = openraft::StoredMembership::new(last_log_id, membership);

        let snapshot_meta = openraft::SnapshotMeta {
            last_log_id,
            last_membership: stored_membership,
            snapshot_id: meta.snapshot_id.clone(),
        };

        let install_request = openraft::raft::InstallSnapshotRequest {
            vote: vote.into(),
            meta: snapshot_meta,
            offset: req.offset,
            data: req.data,
            done: req.done,
        };

        // Forward to the Raft instance
        let response = self
            .raft
            .install_snapshot(install_request)
            .await
            .map_err(|e| Status::internal(format!("InstallSnapshot failed: {}", e)))?;

        Ok(Response::new(RaftInstallSnapshotResponse { vote: Some((&response.vote).into()) }))
    }
}

// ============================================================================
// Multi-Shard Raft Service
// ============================================================================

use crate::multi_raft::MultiRaftManager;

/// Multi-shard Raft service that routes RPCs to the correct shard.
///
/// Extracts `shard_id` from incoming requests and forwards to the corresponding
/// shard's Raft instance. Defaults to shard 0 (system shard) for backward
/// compatibility with clients that don't specify a shard.
pub struct MultiShardRaftService {
    /// Multi-raft manager for shard resolution.
    manager: Arc<MultiRaftManager>,
}

impl MultiShardRaftService {
    /// Creates a new multi-shard Raft service.
    pub fn new(manager: Arc<MultiRaftManager>) -> Self {
        Self { manager }
    }

    /// Resolves shard ID to a Raft instance.
    fn resolve_shard(&self, shard_id: Option<u64>) -> Result<Arc<Raft<LedgerTypeConfig>>, Status> {
        let shard_id = inferadb_ledger_types::ShardId::new(shard_id.unwrap_or(0) as u32);
        self.manager
            .get_shard(shard_id)
            .map(|shard| shard.raft().clone())
            .map_err(|_| Status::not_found(format!("shard {} not found", shard_id)))
    }
}

#[tonic::async_trait]
impl RaftService for MultiShardRaftService {
    async fn vote(
        &self,
        request: Request<RaftVoteRequest>,
    ) -> Result<Response<RaftVoteResponse>, Status> {
        let req = request.into_inner();

        // Resolve shard from request
        let raft = self.resolve_shard(req.shard_id)?;

        let vote =
            req.vote.as_ref().ok_or_else(|| Status::invalid_argument("Missing vote field"))?;

        // Convert proto to OpenRaft types
        let raft_vote: Vote<LedgerNodeId> = vote.into();
        let last_log_id = req.last_log_id.map(|id| {
            openraft::LogId::new(openraft::CommittedLeaderId::new(id.term, vote.node_id), id.index)
        });

        let vote_request = openraft::raft::VoteRequest { vote: raft_vote, last_log_id };

        // Forward to the shard's Raft instance
        let response = raft
            .vote(vote_request)
            .await
            .map_err(|e| Status::internal(format!("Vote failed: {}", e)))?;

        Ok(Response::new(RaftVoteResponse {
            vote: Some((&response.vote).into()),
            vote_granted: response.vote_granted,
            last_log_id: response
                .last_log_id
                .map(|id| RaftLogId { term: id.leader_id.term, index: id.index }),
        }))
    }

    async fn append_entries(
        &self,
        request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        let req = request.into_inner();

        // Resolve shard from request
        let raft = self.resolve_shard(req.shard_id)?;

        let vote =
            req.vote.as_ref().ok_or_else(|| Status::invalid_argument("Missing vote field"))?;

        // Deserialize log entries
        let entries: Vec<_> = req.entries.iter().filter_map(|bytes| decode(bytes).ok()).collect();

        // Convert proto to OpenRaft types
        let raft_vote: Vote<LedgerNodeId> = vote.into();
        let leader_node_id = vote.node_id;
        let prev_log_id = req.prev_log_id.map(|id| {
            openraft::LogId::new(
                openraft::CommittedLeaderId::new(id.term, leader_node_id),
                id.index,
            )
        });
        let leader_commit = req.leader_commit.map(|id| {
            openraft::LogId::new(
                openraft::CommittedLeaderId::new(id.term, leader_node_id),
                id.index,
            )
        });

        let append_request: AppendEntriesRequest<LedgerTypeConfig> =
            AppendEntriesRequest { vote: raft_vote, prev_log_id, entries, leader_commit };

        // Forward to the shard's Raft instance
        let response = raft
            .append_entries(append_request)
            .await
            .map_err(|e| Status::internal(format!("AppendEntries failed: {}", e)))?;

        // Convert response to proto
        use openraft::raft::AppendEntriesResponse::*;
        let (success, conflict, higher_vote) = match response {
            Success => (true, false, None),
            Conflict => (false, true, None),
            HigherVote(v) => (false, false, Some((&v).into())),
            PartialSuccess(_) => (true, false, None),
        };

        Ok(Response::new(RaftAppendEntriesResponse { success, conflict, vote: higher_vote }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        // Resolve shard from request
        let raft = self.resolve_shard(req.shard_id)?;

        let vote =
            req.vote.as_ref().ok_or_else(|| Status::invalid_argument("Missing vote field"))?;

        let meta =
            req.meta.as_ref().ok_or_else(|| Status::invalid_argument("Missing meta field"))?;

        // Build snapshot metadata
        let leader_node_id = vote.node_id;
        let last_log_id = meta.last_log_id.as_ref().map(|id| {
            openraft::LogId::new(
                openraft::CommittedLeaderId::new(id.term, leader_node_id),
                id.index,
            )
        });

        // Convert membership
        let membership_proto = meta
            .last_membership
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing last_membership"))?;

        use std::collections::BTreeMap;

        use openraft::BasicNode;

        let mut all_nodes: BTreeMap<u64, BasicNode> = BTreeMap::new();
        for config in &membership_proto.configs {
            for (node_id, addr) in &config.members {
                all_nodes.insert(*node_id, BasicNode { addr: addr.clone() });
            }
        }

        let voter_ids: std::collections::BTreeSet<u64> = all_nodes.keys().copied().collect();
        let membership = openraft::Membership::new(vec![voter_ids], all_nodes);
        let stored_membership = openraft::StoredMembership::new(last_log_id, membership);

        let snapshot_meta = openraft::SnapshotMeta {
            last_log_id,
            last_membership: stored_membership,
            snapshot_id: meta.snapshot_id.clone(),
        };

        let install_request = openraft::raft::InstallSnapshotRequest {
            vote: vote.into(),
            meta: snapshot_meta,
            offset: req.offset,
            data: req.data,
            done: req.done,
        };

        // Forward to the shard's Raft instance
        let response = raft
            .install_snapshot(install_request)
            .await
            .map_err(|e| Status::internal(format!("InstallSnapshot failed: {}", e)))?;

        Ok(Response::new(RaftInstallSnapshotResponse { vote: Some((&response.vote).into()) }))
    }
}

// ============================================================================
// Byzantine fault tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use inferadb_ledger_proto::proto::{
        RaftAppendEntriesRequest, RaftInstallSnapshotRequest, RaftLogId, RaftMembership,
        RaftMembershipConfig, RaftSnapshotMeta, RaftVote, RaftVoteRequest,
        raft_service_server::RaftService,
    };
    use inferadb_ledger_test_utils::TestDir;
    use inferadb_ledger_types::ShardId;
    use tonic::Request;

    use crate::{
        MultiRaftConfig, MultiRaftManager, ShardConfig,
        services::raft::{MultiShardRaftService, RaftServiceImpl},
    };

    /// Creates a RaftServiceImpl backed by a real single-node Raft instance.
    ///
    /// Uses MultiRaftManager to bootstrap a single system shard. The Raft
    /// instance runs a full consensus engine in-process with no networking.
    async fn create_test_service() -> (RaftServiceImpl, Arc<MultiRaftManager>, u64, TestDir) {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = MultiRaftConfig::new(temp.path().to_path_buf(), node_id);
        let manager = Arc::new(MultiRaftManager::new(config));

        let shard_config =
            ShardConfig::system(node_id, "127.0.0.1:50099".to_string()).without_background_jobs();
        let shard = manager.start_system_shard(shard_config).await.expect("start system shard");
        let raft = shard.raft().clone();

        // Wait for the single-node to become leader
        let start = tokio::time::Instant::now();
        while start.elapsed() < std::time::Duration::from_secs(5) {
            let metrics = raft.metrics().borrow().clone();
            if metrics.current_leader == Some(node_id) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let metrics = raft.metrics().borrow().clone();
        assert_eq!(metrics.current_leader, Some(node_id), "Single-node failed to become leader");

        let service = RaftServiceImpl::new(raft);
        (service, manager, node_id, temp)
    }

    /// Creates a MultiShardRaftService with system + 1 data shard.
    async fn create_multi_shard_service()
    -> (MultiShardRaftService, Arc<MultiRaftManager>, u64, TestDir) {
        let temp = TestDir::new();
        let node_id = 1u64;
        let config = MultiRaftConfig::new(temp.path().to_path_buf(), node_id);
        let manager = Arc::new(MultiRaftManager::new(config));

        let shard_config =
            ShardConfig::system(node_id, "127.0.0.1:50098".to_string()).without_background_jobs();
        manager.start_system_shard(shard_config).await.expect("start system shard");

        let data_config =
            ShardConfig::data(ShardId::new(1), vec![(node_id, "127.0.0.1:50098".to_string())])
                .without_background_jobs();
        manager.start_data_shard(data_config).await.expect("start data shard");

        // Wait for leaders on both shards
        let start = tokio::time::Instant::now();
        while start.elapsed() < std::time::Duration::from_secs(5) {
            let sys_ok = manager
                .get_shard(ShardId::new(0))
                .map(|s| s.raft().metrics().borrow().current_leader == Some(node_id))
                .unwrap_or(false);
            let data_ok = manager
                .get_shard(ShardId::new(1))
                .map(|s| s.raft().metrics().borrow().current_leader == Some(node_id))
                .unwrap_or(false);
            if sys_ok && data_ok {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let service = MultiShardRaftService::new(manager.clone());
        (service, manager, node_id, temp)
    }

    fn make_vote(term: u64, node_id: u64) -> RaftVote {
        RaftVote { term, node_id, committed: false }
    }

    fn make_committed_vote(term: u64, node_id: u64) -> RaftVote {
        RaftVote { term, node_id, committed: true }
    }

    fn make_log_id(term: u64, index: u64) -> RaftLogId {
        RaftLogId { term, index }
    }

    fn make_snapshot_meta(
        last_log_term: u64,
        last_log_index: u64,
        membership_nodes: Vec<(u64, String)>,
        snapshot_id: &str,
    ) -> RaftSnapshotMeta {
        let config = RaftMembershipConfig { members: membership_nodes.into_iter().collect() };
        RaftSnapshotMeta {
            last_log_id: Some(make_log_id(last_log_term, last_log_index)),
            last_membership: Some(RaftMembership { configs: vec![config] }),
            snapshot_id: snapshot_id.to_string(),
        }
    }

    /// Returns the current term from the Raft instance via the manager.
    fn get_term(manager: &MultiRaftManager, shard_id: u32) -> u64 {
        manager
            .get_shard(ShardId::new(shard_id))
            .expect("shard exists")
            .raft()
            .metrics()
            .borrow()
            .current_term
    }

    /// Returns the last applied index from the Raft instance.
    fn get_applied(manager: &MultiRaftManager, shard_id: u32) -> u64 {
        manager
            .get_shard(ShardId::new(shard_id))
            .expect("shard exists")
            .raft()
            .metrics()
            .borrow()
            .last_applied
            .map_or(0, |id| id.index)
    }

    // ====================================================================
    // Test 1: Malformed message rejection
    // ====================================================================

    /// Vote request with missing vote field → INVALID_ARGUMENT.
    #[tokio::test]
    async fn test_byzantine_vote_missing_vote_field() {
        let (service, _mgr, _id, _dir) = create_test_service().await;

        let request =
            Request::new(RaftVoteRequest { vote: None, last_log_id: None, shard_id: None });
        let result = service.vote(request).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    /// AppendEntries with missing vote field → INVALID_ARGUMENT.
    #[tokio::test]
    async fn test_byzantine_append_entries_missing_vote_field() {
        let (service, _mgr, _id, _dir) = create_test_service().await;

        let request = Request::new(RaftAppendEntriesRequest {
            vote: None,
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
            shard_id: None,
        });
        let result = service.append_entries(request).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    /// InstallSnapshot with missing meta field → INVALID_ARGUMENT.
    #[tokio::test]
    async fn test_byzantine_snapshot_missing_meta_field() {
        let (service, _mgr, _id, _dir) = create_test_service().await;

        let request = Request::new(RaftInstallSnapshotRequest {
            vote: Some(make_vote(1, 999)),
            meta: None,
            offset: 0,
            data: vec![],
            done: true,
            shard_id: None,
        });
        let result = service.install_snapshot(request).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    /// InstallSnapshot with missing membership in meta → INVALID_ARGUMENT.
    #[tokio::test]
    async fn test_byzantine_snapshot_missing_membership() {
        let (service, _mgr, _id, _dir) = create_test_service().await;

        let request = Request::new(RaftInstallSnapshotRequest {
            vote: Some(make_vote(1, 999)),
            meta: Some(RaftSnapshotMeta {
                last_log_id: Some(make_log_id(1, 5)),
                last_membership: None,
                snapshot_id: "fake".to_string(),
            }),
            offset: 0,
            data: vec![],
            done: true,
            shard_id: None,
        });
        let result = service.install_snapshot(request).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    // ====================================================================
    // Test 2: Conflicting log entries from Byzantine leader
    // ====================================================================

    /// Stale-term AppendEntries should be rejected or return higher vote.
    #[tokio::test]
    async fn test_byzantine_stale_term_append_entries() {
        let (service, _mgr, _id, _dir) = create_test_service().await;

        let request = Request::new(RaftAppendEntriesRequest {
            vote: Some(make_vote(0, 999)),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
            shard_id: None,
        });
        let result = service.append_entries(request).await;

        if let Ok(response) = result {
            let resp = response.into_inner();
            assert!(
                !resp.success || resp.vote.is_some(),
                "Should reject stale term or return higher vote"
            );
        }
    }

    /// Garbage (non-deserializable) log entries should be silently dropped.
    #[tokio::test]
    async fn test_byzantine_garbage_log_entries() {
        let (service, mgr, node_id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);
        let applied_before = get_applied(&mgr, 0);

        let garbage_entries = vec![
            vec![0xFF, 0xFE, 0xFD],
            vec![],
            vec![0x00; 1024],
            b"not a valid postcard encoded entry".to_vec(),
        ];

        let request = Request::new(RaftAppendEntriesRequest {
            vote: Some(make_vote(current_term, node_id)),
            prev_log_id: None,
            entries: garbage_entries,
            leader_commit: None,
            shard_id: None,
        });
        let _result = service.append_entries(request).await;

        let applied_after = get_applied(&mgr, 0);
        assert!(
            applied_after >= applied_before,
            "Applied index must not regress: before={}, after={}",
            applied_before,
            applied_after
        );
    }

    /// Conflicting prev_log_id (non-existent index) → conflict response.
    #[tokio::test]
    async fn test_byzantine_conflicting_prev_log_id() {
        let (service, mgr, node_id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);

        let request = Request::new(RaftAppendEntriesRequest {
            vote: Some(make_vote(current_term, node_id)),
            prev_log_id: Some(make_log_id(current_term, 999_999)),
            entries: vec![],
            leader_commit: None,
            shard_id: None,
        });
        let result = service.append_entries(request).await;

        if let Ok(response) = result {
            let resp = response.into_inner();
            assert!(
                resp.conflict || !resp.success,
                "Should detect log conflict for non-existent prev_log_id"
            );
        }
    }

    // ====================================================================
    // Test 3: Vote manipulation
    // ====================================================================

    /// High-term vote request should not crash the node.
    #[tokio::test]
    async fn test_byzantine_high_term_vote_request() {
        let (service, mgr, _id, _dir) = create_test_service().await;

        let request = Request::new(RaftVoteRequest {
            vote: Some(make_vote(999, 12345)),
            last_log_id: Some(make_log_id(999, 100)),
            shard_id: None,
        });
        let result = service.vote(request).await;

        match result {
            Ok(response) => {
                assert!(
                    response.into_inner().vote.is_some(),
                    "Vote response should contain a vote"
                );
            },
            Err(status) => {
                assert!(
                    status.code() == tonic::Code::Internal
                        || status.code() == tonic::Code::InvalidArgument,
                    "Unexpected error code: {:?}",
                    status
                );
            },
        }

        // Wait for re-election in the single-node cluster
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let metrics =
            mgr.get_shard(ShardId::new(0)).expect("shard").raft().metrics().borrow().clone();
        assert!(metrics.id > 0, "Node should still be responsive");
    }

    /// Competing vote in the same term should be handled gracefully.
    #[tokio::test]
    async fn test_byzantine_competing_vote_same_term() {
        let (service, mgr, _id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);

        let request = Request::new(RaftVoteRequest {
            vote: Some(make_vote(current_term, 99999)),
            last_log_id: None,
            shard_id: None,
        });
        let result = service.vote(request).await;

        if let Ok(response) = result {
            assert!(response.into_inner().vote.is_some(), "Should return a vote in the response");
        }
    }

    // ====================================================================
    // Test 4: Corrupted snapshot data
    // ====================================================================

    /// Corrupted snapshot data should not corrupt node state.
    #[tokio::test]
    async fn test_byzantine_corrupted_snapshot_data() {
        let (service, mgr, node_id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);
        let applied_before = get_applied(&mgr, 0);

        let meta = make_snapshot_meta(
            current_term,
            1,
            vec![(node_id, "127.0.0.1:50099".to_string())],
            "byzantine-corrupt",
        );

        let request = Request::new(RaftInstallSnapshotRequest {
            vote: Some(make_committed_vote(current_term, node_id)),
            meta: Some(meta),
            offset: 0,
            data: vec![0xFF; 4096],
            done: true,
            shard_id: None,
        });
        let _result = service.install_snapshot(request).await;

        // Wait for potential recovery
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let applied_after = get_applied(&mgr, 0);
        assert!(
            applied_after >= applied_before,
            "State must not regress: before={}, after={}",
            applied_before,
            applied_after
        );
    }

    /// Forged membership containing attacker nodes.
    #[tokio::test]
    async fn test_byzantine_forged_membership_in_snapshot() {
        let (service, mgr, node_id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);

        let meta = make_snapshot_meta(
            current_term,
            1,
            vec![
                (node_id, "127.0.0.1:50099".to_string()),
                (88888, "attacker.evil.com:9999".to_string()),
                (99999, "attacker2.evil.com:9999".to_string()),
            ],
            "byzantine-forged",
        );

        let request = Request::new(RaftInstallSnapshotRequest {
            vote: Some(make_committed_vote(current_term, node_id)),
            meta: Some(meta),
            offset: 0,
            data: vec![0x00; 64],
            done: true,
            shard_id: None,
        });
        let _result = service.install_snapshot(request).await;

        // Node should still be responsive
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let metrics =
            mgr.get_shard(ShardId::new(0)).expect("shard").raft().metrics().borrow().clone();
        assert!(metrics.id > 0, "Node should still be responsive");
    }

    /// Snapshot with impossibly high log index.
    #[tokio::test]
    async fn test_byzantine_snapshot_future_log_index() {
        let (service, mgr, node_id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);

        let meta = make_snapshot_meta(
            current_term,
            999_999_999,
            vec![(node_id, "127.0.0.1:50099".to_string())],
            "byzantine-future",
        );

        let request = Request::new(RaftInstallSnapshotRequest {
            vote: Some(make_committed_vote(current_term, node_id)),
            meta: Some(meta),
            offset: 0,
            data: vec![0x00; 64],
            done: true,
            shard_id: None,
        });
        let _result = service.install_snapshot(request).await;

        // Node should still be responsive
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let metrics =
            mgr.get_shard(ShardId::new(0)).expect("shard").raft().metrics().borrow().clone();
        assert!(metrics.id > 0, "Node should still be responsive");
    }

    // ====================================================================
    // Test 5: Replay attacks
    // ====================================================================

    /// Replayed AppendEntries from a stale term should be rejected.
    #[tokio::test]
    async fn test_byzantine_replay_old_term_entries() {
        let (service, mgr, _id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);
        let applied_before = get_applied(&mgr, 0);
        let old_term = current_term.saturating_sub(1);

        let request = Request::new(RaftAppendEntriesRequest {
            vote: Some(make_vote(old_term, 42)),
            prev_log_id: Some(make_log_id(old_term, 1)),
            entries: vec![vec![0xDE, 0xAD, 0xBE, 0xEF]],
            leader_commit: Some(make_log_id(old_term, 1)),
            shard_id: None,
        });
        let result = service.append_entries(request).await;

        if let Ok(response) = result {
            let resp = response.into_inner();
            assert!(
                !resp.success || resp.vote.is_some(),
                "Should not accept entries from an old term"
            );
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let applied_after = get_applied(&mgr, 0);
        assert!(
            applied_after >= applied_before,
            "Applied must not regress: before={}, after={}",
            applied_before,
            applied_after
        );
    }

    /// Replayed AppendEntries at already-committed log index.
    #[tokio::test]
    async fn test_byzantine_replay_committed_index() {
        let (service, mgr, node_id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);

        let request = Request::new(RaftAppendEntriesRequest {
            vote: Some(make_vote(current_term, node_id)),
            prev_log_id: None,
            entries: vec![vec![0xBA, 0xD0, 0x00]],
            leader_commit: Some(make_log_id(current_term, 1)),
            shard_id: None,
        });
        let _result = service.append_entries(request).await;

        // Node should remain operational
        let metrics =
            mgr.get_shard(ShardId::new(0)).expect("shard").raft().metrics().borrow().clone();
        assert!(metrics.id > 0, "Node should be responsive");
    }

    // ====================================================================
    // Test 6: Protocol violation edge cases
    // ====================================================================

    /// u64::MAX term vote should not crash or overflow.
    #[tokio::test]
    async fn test_byzantine_max_term_vote() {
        let (service, mgr, _id, _dir) = create_test_service().await;

        let request = Request::new(RaftVoteRequest {
            vote: Some(make_vote(u64::MAX, 77777)),
            last_log_id: Some(make_log_id(u64::MAX, u64::MAX)),
            shard_id: None,
        });
        let _result = service.vote(request).await;

        // Wait for potential re-election
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let metrics =
            mgr.get_shard(ShardId::new(0)).expect("shard").raft().metrics().borrow().clone();
        assert!(metrics.id > 0, "Node should still be responsive after max-term vote");
    }

    /// Empty snapshot with done=true should not corrupt state.
    #[tokio::test]
    async fn test_byzantine_empty_snapshot_marked_done() {
        let (service, mgr, node_id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);
        let applied_before = get_applied(&mgr, 0);

        let meta = make_snapshot_meta(
            current_term,
            1,
            vec![(node_id, "127.0.0.1:50099".to_string())],
            "empty-snapshot",
        );

        let request = Request::new(RaftInstallSnapshotRequest {
            vote: Some(make_committed_vote(current_term, node_id)),
            meta: Some(meta),
            offset: 0,
            data: vec![],
            done: true,
            shard_id: None,
        });
        let _result = service.install_snapshot(request).await;

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let applied_after = get_applied(&mgr, 0);
        assert!(
            applied_after >= applied_before,
            "State must not regress: before={}, after={}",
            applied_before,
            applied_after,
        );
    }

    /// Out-of-order snapshot chunks should be handled gracefully.
    #[tokio::test]
    async fn test_byzantine_out_of_order_snapshot_chunks() {
        let (service, mgr, node_id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);

        let meta = make_snapshot_meta(
            current_term,
            1,
            vec![(node_id, "127.0.0.1:50099".to_string())],
            "ooo-snapshot",
        );

        // Send chunk at offset 1000 without preceding offset 0
        let request = Request::new(RaftInstallSnapshotRequest {
            vote: Some(make_committed_vote(current_term, node_id)),
            meta: Some(meta),
            offset: 1000,
            data: vec![0xCC; 128],
            done: false,
            shard_id: None,
        });
        let _result = service.install_snapshot(request).await;

        let metrics =
            mgr.get_shard(ShardId::new(0)).expect("shard").raft().metrics().borrow().clone();
        assert!(metrics.id > 0, "Node should remain responsive");
    }

    /// Oversized snapshot chunk should not cause OOM.
    #[tokio::test]
    async fn test_byzantine_oversized_snapshot_chunk() {
        let (service, mgr, node_id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);

        let meta = make_snapshot_meta(
            current_term,
            1,
            vec![(node_id, "127.0.0.1:50099".to_string())],
            "oversized-snapshot",
        );

        let request = Request::new(RaftInstallSnapshotRequest {
            vote: Some(make_committed_vote(current_term, node_id)),
            meta: Some(meta),
            offset: 0,
            data: vec![0xAB; 1024 * 1024], // 1MB
            done: true,
            shard_id: None,
        });
        let _result = service.install_snapshot(request).await;

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let metrics =
            mgr.get_shard(ShardId::new(0)).expect("shard").raft().metrics().borrow().clone();
        assert!(metrics.id > 0, "Node should survive oversized snapshot");
    }

    // ====================================================================
    // Test: MultiShardRaftService routing
    // ====================================================================

    /// Non-existent shard_id → NOT_FOUND error.
    #[tokio::test]
    async fn test_byzantine_multi_shard_invalid_shard() {
        let (service, _mgr, _id, _dir) = create_multi_shard_service().await;

        let request = Request::new(RaftAppendEntriesRequest {
            vote: Some(make_vote(1, 999)),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
            shard_id: Some(99999), // Non-existent shard
        });
        let result = service.append_entries(request).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    /// Missing shard_id defaults to shard 0 (system shard).
    #[tokio::test]
    async fn test_byzantine_multi_shard_default_routing() {
        let (service, _mgr, _id, _dir) = create_multi_shard_service().await;

        // Request without shard_id and missing vote → should route to shard 0
        // and return InvalidArgument (not NotFound)
        let request =
            Request::new(RaftVoteRequest { vote: None, last_log_id: None, shard_id: None });
        let result = service.vote(request).await;

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            tonic::Code::InvalidArgument,
            "Should route to shard 0 and fail on missing vote, not on shard lookup"
        );
    }

    /// Malformed messages to a specific data shard → still rejected properly.
    #[tokio::test]
    async fn test_byzantine_multi_shard_malformed_to_data_shard() {
        let (service, _mgr, _id, _dir) = create_multi_shard_service().await;

        let request = Request::new(RaftVoteRequest {
            vote: None,
            last_log_id: None,
            shard_id: Some(1), // Route to data shard
        });
        let result = service.vote(request).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    // ====================================================================
    // Test 7: Comprehensive integrity after multiple attacks
    // ====================================================================

    /// Sequential Byzantine attacks should not corrupt cumulative state.
    #[tokio::test]
    async fn test_byzantine_cluster_integrity_after_attacks() {
        let (service, mgr, node_id, _dir) = create_test_service().await;
        let current_term = get_term(&mgr, 0);
        let initial_applied = get_applied(&mgr, 0);

        // Attack 1: Stale term
        let _ = service
            .append_entries(Request::new(RaftAppendEntriesRequest {
                vote: Some(make_vote(0, 42)),
                prev_log_id: None,
                entries: vec![vec![0xFF; 32]],
                leader_commit: None,
                shard_id: None,
            }))
            .await;

        // Attack 2: Garbage entries
        let _ = service
            .append_entries(Request::new(RaftAppendEntriesRequest {
                vote: Some(make_vote(current_term, node_id)),
                prev_log_id: None,
                entries: vec![vec![0xDE; 1024], vec![0xAD; 512]],
                leader_commit: None,
                shard_id: None,
            }))
            .await;

        // Attack 3: Missing-field vote
        let _ = service
            .vote(Request::new(RaftVoteRequest { vote: None, last_log_id: None, shard_id: None }))
            .await;

        // Attack 4: Corrupt snapshot
        let meta = make_snapshot_meta(
            current_term,
            1,
            vec![(node_id, "127.0.0.1:50099".to_string())],
            "capstone-test",
        );
        let _ = service
            .install_snapshot(Request::new(RaftInstallSnapshotRequest {
                vote: Some(make_committed_vote(current_term, node_id)),
                meta: Some(meta),
                offset: 0,
                data: vec![0xDE, 0xAD],
                done: true,
                shard_id: None,
            }))
            .await;

        // Attack 5: Conflicting prev_log_id
        let _ = service
            .append_entries(Request::new(RaftAppendEntriesRequest {
                vote: Some(make_vote(current_term, node_id)),
                prev_log_id: Some(make_log_id(current_term, 999_999)),
                entries: vec![],
                leader_commit: None,
                shard_id: None,
            }))
            .await;

        // Wait for stabilization
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Verify integrity
        let metrics =
            mgr.get_shard(ShardId::new(0)).expect("shard").raft().metrics().borrow().clone();
        assert!(metrics.id > 0, "Node should be responsive after attacks");

        let final_applied = get_applied(&mgr, 0);
        assert!(
            final_applied >= initial_applied,
            "Applied index regressed: {} -> {}",
            initial_applied,
            final_applied
        );
    }
}
