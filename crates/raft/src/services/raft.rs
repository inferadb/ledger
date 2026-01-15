//! Raft service implementation for inter-node communication.
//!
//! This service handles incoming Raft RPC calls from peer nodes:
//! - Vote requests during leader election
//! - AppendEntries for log replication
//! - InstallSnapshot for follower catch-up

use std::sync::Arc;

use ledger_types::decode;
use openraft::raft::AppendEntriesRequest;
use openraft::{Raft, Vote};
use tonic::{Request, Response, Status};

use crate::proto::raft_service_server::RaftService;
use crate::proto::{
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftInstallSnapshotRequest,
    RaftInstallSnapshotResponse, RaftLogId, RaftVoteRequest, RaftVoteResponse,
};
use crate::types::{LedgerNodeId, LedgerTypeConfig};

/// Raft service implementation for handling inter-node Raft RPCs.
pub struct RaftServiceImpl {
    /// The Raft instance to forward requests to.
    raft: Arc<Raft<LedgerTypeConfig>>,
}

impl RaftServiceImpl {
    /// Create a new Raft service.
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

        let vote = req
            .vote
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing vote field"))?;

        // Convert proto to OpenRaft types (using From impl in proto_convert)
        let raft_vote: Vote<LedgerNodeId> = vote.into();
        // Use the vote's node_id for the CommittedLeaderId - this identifies who committed the log entry
        let last_log_id = req.last_log_id.map(|id| {
            openraft::LogId::new(
                openraft::CommittedLeaderId::new(id.term, vote.node_id),
                id.index,
            )
        });

        let vote_request = openraft::raft::VoteRequest {
            vote: raft_vote,
            last_log_id,
        };

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
            last_log_id: response.last_log_id.map(|id| RaftLogId {
                term: id.leader_id.term,
                index: id.index,
            }),
        }))
    }

    async fn append_entries(
        &self,
        request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        let req = request.into_inner();

        let vote = req
            .vote
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing vote field"))?;

        // Deserialize log entries
        let entries: Vec<_> = req
            .entries
            .iter()
            .filter_map(|bytes| decode(bytes).ok())
            .collect();

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

        let append_request: AppendEntriesRequest<LedgerTypeConfig> = AppendEntriesRequest {
            vote: raft_vote,
            prev_log_id,
            entries,
            leader_commit,
        };

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

        Ok(Response::new(RaftAppendEntriesResponse {
            success,
            conflict,
            vote: higher_vote,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        let vote = req
            .vote
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing vote field"))?;

        let meta = req
            .meta
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing meta field"))?;

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
        use openraft::BasicNode;
        use std::collections::BTreeMap;

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

        Ok(Response::new(RaftInstallSnapshotResponse {
            vote: Some((&response.vote).into()),
        }))
    }
}
