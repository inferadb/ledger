//! System discovery service implementation.
//!
//! Provides peer discovery and system state information for cluster coordination.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use openraft::Raft;
use parking_lot::RwLock;
use prost_types::Timestamp;
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

/// Tracks when peers were last seen for health monitoring.
#[derive(Debug, Default)]
struct PeerTracker {
    /// Map of node ID string to last seen instant.
    last_seen: HashMap<String, Instant>,
}

impl PeerTracker {
    fn new() -> Self {
        Self {
            last_seen: HashMap::new(),
        }
    }

    /// Record that a peer was seen now.
    fn record_seen(&mut self, node_id: &str) {
        self.last_seen.insert(node_id.to_string(), Instant::now());
    }

    /// Get the last seen timestamp for a peer as a proto Timestamp.
    /// Returns None if the peer has never been seen.
    fn get_last_seen(&self, node_id: &str) -> Option<Timestamp> {
        self.last_seen.get(node_id).map(|instant| {
            // Convert Instant to wall-clock time
            let elapsed_since_seen = instant.elapsed();
            let now = std::time::SystemTime::now();
            let seen_time = now - elapsed_since_seen;

            // Convert to proto Timestamp
            match seen_time.duration_since(std::time::UNIX_EPOCH) {
                Ok(duration) => Timestamp {
                    seconds: duration.as_secs() as i64,
                    nanos: duration.subsec_nanos() as i32,
                },
                Err(_) => Timestamp {
                    seconds: 0,
                    nanos: 0,
                },
            }
        })
    }
}

/// Discovery service implementation.
pub struct DiscoveryServiceImpl {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The state layer.
    #[allow(dead_code)]
    state: Arc<RwLock<StateLayer>>,
    /// Accessor for applied state (namespace registry).
    applied_state: AppliedStateAccessor,
    /// Tracks when peers were last seen.
    peer_tracker: RwLock<PeerTracker>,
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
            peer_tracker: RwLock::new(PeerTracker::new()),
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

        let tracker = self.peer_tracker.read();
        let peers: Vec<PeerInfo> = membership
            .nodes()
            .take(max_peers)
            .map(|(id, node)| {
                let node_id_str = id.to_string();
                PeerInfo {
                    node_id: Some(NodeId {
                        id: node_id_str.clone(),
                    }),
                    addresses: vec![node.addr.clone()],
                    grpc_port: 5000, // Default port
                    last_seen: tracker.get_last_seen(&node_id_str),
                }
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

        let node_id = peer
            .node_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing node_id"))?;

        // Record that we've seen this peer
        {
            let mut tracker = self.peer_tracker.write();
            tracker.record_seen(&node_id.id);
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

        let leader_hint = metrics
            .current_leader
            .map(|id| NodeId { id: id.to_string() });

        let namespaces: Vec<NamespaceRegistry> = self
            .applied_state
            .list_namespaces()
            .into_iter()
            .map(|ns| NamespaceRegistry {
                namespace_id: Some(NamespaceId {
                    id: ns.namespace_id,
                }),
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    clippy::panic
)]
mod tests {
    use super::*;

    // =========================================================================
    // PeerTracker Tests
    // =========================================================================

    #[test]
    fn test_peer_tracker_new() {
        let tracker = PeerTracker::new();
        assert!(tracker.last_seen.is_empty());
    }

    #[test]
    fn test_peer_tracker_record_seen() {
        let mut tracker = PeerTracker::new();

        tracker.record_seen("node-1");
        assert!(tracker.last_seen.contains_key("node-1"));
        assert!(tracker.get_last_seen("node-1").is_some());
    }

    #[test]
    fn test_peer_tracker_unknown_peer_returns_none() {
        let tracker = PeerTracker::new();
        assert!(tracker.get_last_seen("unknown").is_none());
    }

    #[test]
    fn test_peer_tracker_updates_timestamp() {
        let mut tracker = PeerTracker::new();

        // Record first sighting
        tracker.record_seen("node-1");
        let first_seen = tracker.get_last_seen("node-1").unwrap();

        // Wait a bit
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Record again
        tracker.record_seen("node-1");
        let second_seen = tracker.get_last_seen("node-1").unwrap();

        // Second timestamp should be later (or at least equal due to resolution)
        assert!(
            second_seen.seconds >= first_seen.seconds
                || (second_seen.seconds == first_seen.seconds
                    && second_seen.nanos >= first_seen.nanos),
            "Second sighting should have later or equal timestamp"
        );
    }

    #[test]
    fn test_peer_tracker_multiple_peers() {
        let mut tracker = PeerTracker::new();

        tracker.record_seen("node-1");
        tracker.record_seen("node-2");
        tracker.record_seen("node-3");

        assert!(tracker.get_last_seen("node-1").is_some());
        assert!(tracker.get_last_seen("node-2").is_some());
        assert!(tracker.get_last_seen("node-3").is_some());
        assert!(tracker.get_last_seen("node-4").is_none());
    }

    #[test]
    fn test_peer_tracker_timestamp_is_reasonable() {
        let mut tracker = PeerTracker::new();

        tracker.record_seen("node-1");
        let ts = tracker.get_last_seen("node-1").unwrap();

        // Timestamp should be recent (within last minute)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        assert!(
            ts.seconds >= now - 60 && ts.seconds <= now + 1,
            "Timestamp should be within last minute: got {}, now {}",
            ts.seconds,
            now
        );
    }
}
