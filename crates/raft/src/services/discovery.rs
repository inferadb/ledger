//! System discovery service implementation.
//!
//! Provides peer discovery and system state information for cluster coordination.

use std::sync::Arc;

use openraft::Raft;
use parking_lot::RwLock;
use tonic::{Request, Response, Status};

use crate::log_storage::AppliedStateAccessor;
use crate::proto::system_discovery_service_server::SystemDiscoveryService;
use crate::proto::{
    AnnouncePeerRequest, AnnouncePeerResponse, GetPeersRequest, GetPeersResponse,
    GetSystemStateRequest, GetSystemStateResponse, NamespaceId, NamespaceRegistry,
    NodeCapabilities, NodeId, NodeInfo, PeerInfo, ShardId,
};
use crate::types::LedgerTypeConfig;

use ledger_storage::StateLayer;

/// Discovery service implementation.
pub struct DiscoveryServiceImpl {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The state layer.
    #[allow(dead_code)]
    state: Arc<RwLock<StateLayer>>,
    /// Accessor for applied state (namespace registry).
    applied_state: AppliedStateAccessor,
}

impl DiscoveryServiceImpl {
    /// Create a new discovery service.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<RwLock<StateLayer>>,
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
impl SystemDiscoveryService for DiscoveryServiceImpl {
    async fn get_peers(
        &self,
        request: Request<GetPeersRequest>,
    ) -> Result<Response<GetPeersResponse>, Status> {
        let req = request.into_inner();
        let max_peers = if req.max_peers == 0 {
            100
        } else {
            req.max_peers as usize
        };

        // Get peers from Raft membership
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();

        let peers: Vec<PeerInfo> = membership
            .nodes()
            .take(max_peers)
            .map(|(id, node)| PeerInfo {
                node_id: Some(NodeId { id: id.to_string() }),
                addresses: vec![node.addr.clone()],
                grpc_port: 5000, // Default port
                last_seen: None, // TODO: Track last seen times
            })
            .collect();

        Ok(Response::new(GetPeersResponse {
            peers,
            system_version: metrics.current_term,
        }))
    }

    async fn announce_peer(
        &self,
        request: Request<AnnouncePeerRequest>,
    ) -> Result<Response<AnnouncePeerResponse>, Status> {
        let req = request.into_inner();

        // Validate the peer info
        let peer = req
            .peer
            .ok_or_else(|| Status::invalid_argument("Missing peer info"))?;

        if peer.node_id.is_none() {
            return Err(Status::invalid_argument("Missing node_id"));
        }

        // TODO: In a full implementation, this would:
        // 1. Validate the peer's addresses are WireGuard IPs
        // 2. Add the peer to the local peer cache
        // 3. Propagate to other nodes if needed

        tracing::info!("Peer announced: {:?}", peer);

        Ok(Response::new(AnnouncePeerResponse { accepted: true }))
    }

    async fn get_system_state(
        &self,
        request: Request<GetSystemStateRequest>,
    ) -> Result<Response<GetSystemStateResponse>, Status> {
        let req = request.into_inner();

        // Get current system version (Raft term)
        let metrics = self.raft.metrics().borrow().clone();
        let current_version = metrics.current_term;

        // If client already has current version, return empty response
        if req.if_version_greater_than >= current_version {
            return Ok(Response::new(GetSystemStateResponse {
                version: current_version,
                nodes: vec![],
                namespaces: vec![],
            }));
        }

        // Build node info from Raft membership
        let membership = metrics.membership_config.membership();
        let nodes: Vec<NodeInfo> = membership
            .nodes()
            .map(|(id, node)| NodeInfo {
                node_id: Some(NodeId { id: id.to_string() }),
                addresses: vec![node.addr.clone()],
                grpc_port: 5000,
                capabilities: Some(NodeCapabilities {
                    can_lead: true,
                    max_vaults: 1000,
                }),
                last_heartbeat: None,
            })
            .collect();

        // Build namespace registry from applied state
        let member_nodes: Vec<NodeId> = membership
            .nodes()
            .map(|(id, _)| NodeId { id: id.to_string() })
            .collect();

        let leader_hint = metrics.current_leader.map(|id| NodeId { id: id.to_string() });

        let namespaces: Vec<NamespaceRegistry> = self
            .applied_state
            .list_namespaces()
            .into_iter()
            .map(|ns| NamespaceRegistry {
                namespace_id: Some(NamespaceId { id: ns.namespace_id }),
                shard_id: Some(ShardId { id: ns.shard_id }),
                members: member_nodes.clone(),
                leader_hint: leader_hint.clone(),
                config_version: current_version,
            })
            .collect();

        Ok(Response::new(GetSystemStateResponse {
            version: current_version,
            nodes,
            namespaces,
        }))
    }
}
