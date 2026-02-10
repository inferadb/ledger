//! Raft network transport using gRPC.
//!
//! This module implements the `RaftNetwork` trait for OpenRaft, enabling
//! inter-node communication for vote requests, log replication, and snapshots.

use std::{collections::HashMap, future::Future, sync::Arc};

use inferadb_ledger_proto::proto::raft_service_client::RaftServiceClient;
use inferadb_ledger_types::encode;
use openraft::{
    BasicNode, Snapshot, Vote,
    error::{Fatal, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
};
use parking_lot::RwLock;
use tonic::{Request, transport::Channel};

use crate::{
    trace_context::{self, TraceContext},
    types::{LedgerNodeId, LedgerTypeConfig},
};

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
    /// Creates a new gRPC Raft network.
    pub fn new() -> Self {
        Self { clients: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Returns or create a client connection to a peer node.
    async fn get_client(
        &self,
        target: LedgerNodeId,
        node: &BasicNode,
    ) -> Result<RaftServiceClient<Channel>, NetworkError> {
        if let Some(client) = self.clients.read().get(&target).cloned() {
            return Ok(client);
        }

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
    /// Whether to inject trace context into outgoing RPCs.
    trace_raft_rpcs: bool,
}

impl GrpcRaftNetworkFactory {
    /// Creates a new network factory.
    pub fn new() -> Self {
        Self { network: GrpcRaftNetwork::new(), trace_raft_rpcs: true }
    }

    /// Creates a new network factory with trace context injection configured.
    pub fn with_trace_config(trace_raft_rpcs: bool) -> Self {
        Self { network: GrpcRaftNetwork::new(), trace_raft_rpcs }
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
            trace_raft_rpcs: self.trace_raft_rpcs,
        }
    }
}

/// A connection to a specific Raft peer.
pub struct GrpcRaftNetworkConnection {
    target: LedgerNodeId,
    node: BasicNode,
    network: GrpcRaftNetwork,
    /// Whether to inject trace context into outgoing RPCs.
    trace_raft_rpcs: bool,
}

impl GrpcRaftNetworkConnection {
    /// Creates a gRPC request with optional trace context injection.
    fn make_request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if self.trace_raft_rpcs {
            let trace_ctx = TraceContext::new();
            trace_context::inject_into_metadata(request.metadata_mut(), &trace_ctx);
        }
        request
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

        let proto_request = inferadb_ledger_proto::proto::RaftVoteRequest {
            vote: Some((&rpc.vote).into()),
            last_log_id: rpc.last_log_id.map(|id| inferadb_ledger_proto::proto::RaftLogId {
                term: id.leader_id.term,
                index: id.index,
            }),
            shard_id: None, // Default to system shard (0)
        };

        let response = client
            .vote(self.make_request(proto_request))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError(e.to_string()))))?
            .into_inner();

        let vote = response.vote.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&NetworkError(
                "Missing vote in response".to_string(),
            )))
        })?;

        Ok(VoteResponse {
            vote: (&vote).into(),
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

        let entries: Vec<Vec<u8>> =
            rpc.entries.iter().map(|e| encode(e).unwrap_or_default()).collect();

        let proto_request = inferadb_ledger_proto::proto::RaftAppendEntriesRequest {
            vote: Some((&rpc.vote).into()),
            prev_log_id: rpc.prev_log_id.map(|id| inferadb_ledger_proto::proto::RaftLogId {
                term: id.leader_id.term,
                index: id.index,
            }),
            entries,
            leader_commit: rpc.leader_commit.map(|id| inferadb_ledger_proto::proto::RaftLogId {
                term: id.leader_id.term,
                index: id.index,
            }),
            shard_id: None, // Default to system shard (0)
        };

        let response = client
            .append_entries(self.make_request(proto_request))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError(e.to_string()))))?
            .into_inner();

        if response.success {
            Ok(AppendEntriesResponse::Success)
        } else if response.conflict {
            Ok(AppendEntriesResponse::Conflict)
        } else if let Some(vote) = response.vote {
            // Higher vote received
            Ok(AppendEntriesResponse::HigherVote((&vote).into()))
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

        let proto_request = inferadb_ledger_proto::proto::RaftInstallSnapshotRequest {
            vote: Some((&rpc.vote).into()),
            meta: Some(inferadb_ledger_proto::proto::RaftSnapshotMeta {
                last_log_id: rpc.meta.last_log_id.map(|id| {
                    inferadb_ledger_proto::proto::RaftLogId {
                        term: id.leader_id.term,
                        index: id.index,
                    }
                }),
                last_membership: Some(inferadb_ledger_proto::proto::RaftMembership {
                    configs: {
                        let members: std::collections::HashMap<u64, String> = rpc
                            .meta
                            .last_membership
                            .nodes()
                            .map(|(id, node)| (*id, node.addr.clone()))
                            .collect();
                        vec![inferadb_ledger_proto::proto::RaftMembershipConfig { members }]
                    },
                }),
                snapshot_id: rpc.meta.snapshot_id.clone(),
            }),
            offset: rpc.offset,
            data: rpc.data.clone(),
            done: rpc.done,
            shard_id: None, // Default to system shard (0)
        };

        let response = client
            .install_snapshot(self.make_request(proto_request))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&NetworkError(e.to_string()))))?
            .into_inner();

        let vote = response.vote.ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&NetworkError(
                "Missing vote in response".to_string(),
            )))
        })?;

        Ok(InstallSnapshotResponse { vote: (&vote).into() })
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
        use openraft::network::snapshot_transport::{Chunked, SnapshotTransport};

        Chunked::send_snapshot(self, vote, snapshot, cancel, option).await
    }
}
