//! Wire types for `SystemDiscoveryService`.

use serde::{Deserialize, Serialize};

use crate::services::shared::{NodeInfo, OrganizationRegistry, PeerInfo};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetPeersRequest {
    pub max_peers: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetPeersResponse {
    pub peers: Vec<PeerInfo>,
    pub system_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AnnouncePeerRequest {
    pub peer: Option<PeerInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AnnouncePeerResponse {
    pub accepted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetSystemStateRequest {
    pub if_version_greater_than: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetSystemStateResponse {
    pub version: u64,
    pub nodes: Vec<NodeInfo>,
    pub organizations: Vec<OrganizationRegistry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResolveRegionLeaderRequest {
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResolveRegionLeaderResponse {
    pub endpoint: String,
    pub raft_term: u64,
    pub ttl_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WatchLeaderRequest {
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaderUpdate {
    pub endpoint: String,
    pub raft_term: u64,
    pub leader_node_id: u64,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::services::shared::NodeId;

    #[test]
    fn get_peers_response_roundtrips_via_postcard() {
        let resp = GetPeersResponse {
            peers: vec![PeerInfo {
                node_id: Some(NodeId::new("node-1")),
                addresses: vec!["10.0.0.1".to_owned(), "fd00::1".to_owned()],
                grpc_port: 5000,
                last_seen: 1_700_000_000_000_000_000,
            }],
            system_version: 42,
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: GetPeersResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }
}
