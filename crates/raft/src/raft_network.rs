//! Raft network transport using gRPC.
//!
//! This module implements the `RaftNetwork` trait for OpenRaft, enabling
//! inter-node communication for vote requests, log replication, and snapshots.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use ledger_types::encode;
use openraft::error::{Fatal, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use parking_lot::RwLock;
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Snapshot, Vote};
use tonic::transport::Channel;

use crate::proto::raft_service_client::RaftServiceClient;
use crate::types::{LedgerNodeId, LedgerTypeConfig};

/// Error type for network operations.
#[derive(Debug, Clone)]
pub struct NetworkError(String);

impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NetworkError: {}", self.0)
    }
}

impl std::error::Error for NetworkError {}

/// gRPC-based Raft network implementation.
///
/// Maintains a pool of gRPC clients to peer nodes, creating connections
/// on demand and caching them for reuse.
#[derive(Clone)]
pub struct GrpcRaftNetwork {
    /// Cached gRPC clients for peer nodes.
    clients: Arc<RwLock<HashMap<LedgerNodeId, RaftServiceClient<Channel>>>>,
}

impl GrpcRaftNetwork {
    /// Create a new gRPC Raft network.
    pub fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or create a client connection to a peer node.
    async fn get_client(
        &self,
        target: LedgerNodeId,
        node: &BasicNode,
    ) -> Result<RaftServiceClient<Channel>, NetworkError> {
        // Check cache first (brief read lock)
        if let Some(client) = self.clients.read().get(&target).cloned() {
            return Ok(client);
        }

        // Create new connection (outside lock)
        let endpoint = format!("http://{}", node.addr);
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| NetworkError(format!("Invalid endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| NetworkError(format!("Connection failed: {}", e)))?;

        let client = RaftServiceClient::new(channel);
        self.clients.write().insert(target, client.clone());
        Ok(client)
    }
}

impl Default for GrpcRaftNetwork {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for creating network connections to Raft peers.
pub struct GrpcRaftNetworkFactory {
    network: GrpcRaftNetwork,
}

impl GrpcRaftNetworkFactory {
    /// Create a new network factory.
    pub fn new() -> Self {
        Self {
            network: GrpcRaftNetwork::new(),
        }
    }
}

impl Default for GrpcRaftNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<LedgerTypeConfig> for GrpcRaftNetworkFactory {
    type Network = GrpcRaftNetworkConnection;

    async fn new_client(&mut self, target: LedgerNodeId, node: &BasicNode) -> Self::Network {
        GrpcRaftNetworkConnection {
            target,
            node: node.clone(),
            network: self.network.clone(),
        }
    }
}

/// A connection to a specific Raft peer.
pub struct GrpcRaftNetworkConnection {
    target: LedgerNodeId,
    node: BasicNode,
    network: GrpcRaftNetwork,
}

/// Convert proto vote to OpenRaft Vote.
fn proto_to_vote(proto: &crate::proto::RaftVote) -> Vote<LedgerNodeId> {
    if proto.committed {
        Vote::new_committed(proto.term, proto.node_id)
    } else {
        Vote::new(proto.term, proto.node_id)
    }
}

impl RaftNetwork<LedgerTypeConfig> for GrpcRaftNetworkConnection {
    async fn vote(
        &mut self,
        rpc: VoteRequest<LedgerNodeId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<LedgerNodeId>,
        RPCError<LedgerNodeId, BasicNode, RaftError<LedgerNodeId>>,
    > {
        let mut client = self
            .network
            .get_client(self.target, &self.node)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        // Convert to proto types
        let request = crate::proto::RaftVoteRequest {
            vote: Some(crate::proto::RaftVote {
                term: rpc.vote.leader_id.term,
                node_id: rpc.vote.leader_id.node_id,
                committed: rpc.vote.committed,
            }),
            last_log_id: rpc.last_log_id.map(|id| crate::proto::RaftLogId {
                term: id.leader_id.term,
                index: id.index,
            }),
        };

        let response = client
            .vote(request)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError(e.to_string()))))?
            .into_inner();

        // Convert back from proto types
        let vote = response.vote.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&NetworkError(
                "Missing vote in response".to_string(),
            )))
        })?;

        Ok(VoteResponse {
            vote: proto_to_vote(&vote),
            vote_granted: response.vote_granted,
            last_log_id: response.last_log_id.map(|id| {
                openraft::LogId::new(openraft::CommittedLeaderId::new(id.term, 0), id.index)
            }),
        })
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<LedgerTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<LedgerNodeId>,
        RPCError<LedgerNodeId, BasicNode, RaftError<LedgerNodeId>>,
    > {
        let mut client = self
            .network
            .get_client(self.target, &self.node)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        // Serialize entries to bytes for transport
        let entries: Vec<Vec<u8>> = rpc
            .entries
            .iter()
            .map(|e| encode(e).unwrap_or_default())
            .collect();

        let request = crate::proto::RaftAppendEntriesRequest {
            vote: Some(crate::proto::RaftVote {
                term: rpc.vote.leader_id.term,
                node_id: rpc.vote.leader_id.node_id,
                committed: rpc.vote.committed,
            }),
            prev_log_id: rpc.prev_log_id.map(|id| crate::proto::RaftLogId {
                term: id.leader_id.term,
                index: id.index,
            }),
            entries,
            leader_commit: rpc.leader_commit.map(|id| crate::proto::RaftLogId {
                term: id.leader_id.term,
                index: id.index,
            }),
        };

        let response = client
            .append_entries(request)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError(e.to_string()))))?
            .into_inner();

        // Convert response: the proto has success/conflict booleans
        if response.success {
            Ok(AppendEntriesResponse::Success)
        } else if response.conflict {
            Ok(AppendEntriesResponse::Conflict)
        } else if let Some(vote) = response.vote {
            // Higher vote received
            Ok(AppendEntriesResponse::HigherVote(proto_to_vote(&vote)))
        } else {
            Ok(AppendEntriesResponse::Conflict)
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<LedgerTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<LedgerNodeId>,
        RPCError<
            LedgerNodeId,
            BasicNode,
            RaftError<LedgerNodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let mut client = self
            .network
            .get_client(self.target, &self.node)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let request = crate::proto::RaftInstallSnapshotRequest {
            vote: Some(crate::proto::RaftVote {
                term: rpc.vote.leader_id.term,
                node_id: rpc.vote.leader_id.node_id,
                committed: rpc.vote.committed,
            }),
            meta: Some(crate::proto::RaftSnapshotMeta {
                last_log_id: rpc.meta.last_log_id.map(|id| crate::proto::RaftLogId {
                    term: id.leader_id.term,
                    index: id.index,
                }),
                last_membership: Some(crate::proto::RaftMembership {
                    configs: {
                        let members: std::collections::HashMap<u64, String> = rpc
                            .meta
                            .last_membership
                            .nodes()
                            .map(|(id, node)| (*id, node.addr.clone()))
                            .collect();
                        vec![crate::proto::RaftMembershipConfig { members }]
                    },
                }),
                snapshot_id: rpc.meta.snapshot_id.clone(),
            }),
            offset: rpc.offset,
            data: rpc.data.clone(),
            done: rpc.done,
        };

        let response = client
            .install_snapshot(request)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError(e.to_string()))))?
            .into_inner();

        let vote = response.vote.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&NetworkError(
                "Missing vote in response".to_string(),
            )))
        })?;

        Ok(InstallSnapshotResponse {
            vote: proto_to_vote(&vote),
        })
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<LedgerNodeId>,
        snapshot: Snapshot<LedgerTypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<LedgerNodeId>, StreamingError<LedgerTypeConfig, Fatal<LedgerNodeId>>>
    {
        // Use the default chunked implementation
        use openraft::network::snapshot_transport::Chunked;
        use openraft::network::snapshot_transport::SnapshotTransport;

        Chunked::send_snapshot(self, vote, snapshot, cancel, option).await
    }
}
