//! System discovery service implementation.
//!
//! Provides peer discovery and system state information for cluster coordination.
//!
//! Per DESIGN.md, peer addresses must be private/WireGuard IPs. Public IPs are
//! rejected to ensure cluster traffic stays within the private network.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
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

// =============================================================================
// IP Address Validation
// =============================================================================

/// Check if an IPv4 address is a private/internal address.
///
/// Private ranges per RFC 1918:
/// - 10.0.0.0/8 (Class A)
/// - 172.16.0.0/12 (Class B)
/// - 192.168.0.0/16 (Class C)
///
/// Also allows:
/// - 127.0.0.0/8 (loopback, for testing)
/// - 169.254.0.0/16 (link-local)
fn is_private_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();

    // 10.0.0.0/8
    if octets[0] == 10 {
        return true;
    }

    // 172.16.0.0/12 (172.16.x.x - 172.31.x.x)
    if octets[0] == 172 && (16..=31).contains(&octets[1]) {
        return true;
    }

    // 192.168.0.0/16
    if octets[0] == 192 && octets[1] == 168 {
        return true;
    }

    // 127.0.0.0/8 (loopback)
    if octets[0] == 127 {
        return true;
    }

    // 169.254.0.0/16 (link-local)
    if octets[0] == 169 && octets[1] == 254 {
        return true;
    }

    false
}

/// Check if an IPv6 address is a private/internal address.
///
/// Private ranges:
/// - fd00::/8 (Unique Local Addresses - typical for WireGuard)
/// - fe80::/10 (Link-Local)
/// - ::1 (loopback)
fn is_private_ipv6(ip: Ipv6Addr) -> bool {
    // Loopback
    if ip.is_loopback() {
        return true;
    }

    let segments = ip.segments();

    // fd00::/8 (Unique Local - WireGuard typically uses this)
    if (segments[0] >> 8) == 0xfd {
        return true;
    }

    // fc00::/7 (broader ULA range)
    if (segments[0] >> 9) == 0x7e {
        return true;
    }

    // fe80::/10 (Link-Local)
    if (segments[0] >> 6) == 0x3fa {
        return true;
    }

    // Check using std methods if available
    // fe80::/10 via manual check
    if segments[0] & 0xffc0 == 0xfe80 {
        return true;
    }

    false
}

/// Validate that an IP address string is a private/WireGuard address.
///
/// Returns Ok(()) if valid, Err with reason if invalid.
fn validate_private_ip(addr: &str) -> Result<(), String> {
    // Parse as IP address (strips any port)
    let ip: IpAddr = addr
        .parse()
        .map_err(|_| format!("Invalid IP address: {}", addr))?;

    match ip {
        IpAddr::V4(ipv4) => {
            if is_private_ipv4(ipv4) {
                Ok(())
            } else {
                Err(format!(
                    "Public IPv4 address not allowed: {}. Use private ranges (10.x, 172.16-31.x, 192.168.x)",
                    addr
                ))
            }
        }
        IpAddr::V6(ipv6) => {
            if is_private_ipv6(ipv6) {
                Ok(())
            } else {
                Err(format!(
                    "Public IPv6 address not allowed: {}. Use fd00::/8 (WireGuard) or fe80::/10 (link-local)",
                    addr
                ))
            }
        }
    }
}

/// Validate all addresses in a peer announcement.
///
/// All addresses must be private/WireGuard IPs.
#[allow(clippy::result_large_err)] // tonic::Status is external, can't box it
fn validate_peer_addresses(addresses: &[String]) -> Result<(), Status> {
    if addresses.is_empty() {
        return Err(Status::invalid_argument(
            "Peer must have at least one address",
        ));
    }

    for addr in addresses {
        if let Err(reason) = validate_private_ip(addr) {
            return Err(Status::invalid_argument(reason));
        }
    }

    Ok(())
}

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

        // Validate gRPC port
        if peer.grpc_port == 0 || peer.grpc_port > 65535 {
            return Err(Status::invalid_argument(format!(
                "Invalid gRPC port: {}. Must be 1-65535",
                peer.grpc_port
            )));
        }

        // Validate addresses are private/WireGuard IPs (per DESIGN.md)
        validate_peer_addresses(&peer.addresses)?;

        // Record that we've seen this peer
        {
            let mut tracker = self.peer_tracker.write();
            tracker.record_seen(&node_id.id);
        }

        tracing::info!(
            node_id = %node_id.id,
            addresses = ?peer.addresses,
            grpc_port = peer.grpc_port,
            "Peer announced"
        );

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

    // =========================================================================
    // IP Validation Tests
    // =========================================================================

    #[test]
    fn test_private_ipv4_class_a() {
        // 10.0.0.0/8
        assert!(is_private_ipv4("10.0.0.1".parse().unwrap()));
        assert!(is_private_ipv4("10.255.255.255".parse().unwrap()));
        assert!(is_private_ipv4("10.0.1.1".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_class_b() {
        // 172.16.0.0/12 (172.16.x.x - 172.31.x.x)
        assert!(is_private_ipv4("172.16.0.1".parse().unwrap()));
        assert!(is_private_ipv4("172.31.255.255".parse().unwrap()));
        assert!(is_private_ipv4("172.20.1.1".parse().unwrap()));

        // Outside range
        assert!(!is_private_ipv4("172.15.0.1".parse().unwrap()));
        assert!(!is_private_ipv4("172.32.0.1".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_class_c() {
        // 192.168.0.0/16
        assert!(is_private_ipv4("192.168.0.1".parse().unwrap()));
        assert!(is_private_ipv4("192.168.255.255".parse().unwrap()));
        assert!(is_private_ipv4("192.168.1.100".parse().unwrap()));

        // Outside range
        assert!(!is_private_ipv4("192.167.1.1".parse().unwrap()));
        assert!(!is_private_ipv4("192.169.1.1".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_loopback() {
        // 127.0.0.0/8
        assert!(is_private_ipv4("127.0.0.1".parse().unwrap()));
        assert!(is_private_ipv4("127.255.255.255".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_link_local() {
        // 169.254.0.0/16
        assert!(is_private_ipv4("169.254.0.1".parse().unwrap()));
        assert!(is_private_ipv4("169.254.255.255".parse().unwrap()));
    }

    #[test]
    fn test_public_ipv4_rejected() {
        // Public IPs should not be private
        assert!(!is_private_ipv4("8.8.8.8".parse().unwrap())); // Google DNS
        assert!(!is_private_ipv4("1.1.1.1".parse().unwrap())); // Cloudflare
        assert!(!is_private_ipv4("142.250.72.14".parse().unwrap())); // Google
        assert!(!is_private_ipv4("13.107.42.14".parse().unwrap())); // Microsoft
    }

    #[test]
    fn test_private_ipv6_loopback() {
        assert!(is_private_ipv6("::1".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv6_ula_wireguard() {
        // fd00::/8 - Unique Local Addresses (WireGuard typical)
        assert!(is_private_ipv6("fd00::1".parse().unwrap()));
        assert!(is_private_ipv6("fd12:3456:789a::1".parse().unwrap()));
        assert!(is_private_ipv6(
            "fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap()
        ));
    }

    #[test]
    fn test_private_ipv6_link_local() {
        // fe80::/10
        assert!(is_private_ipv6("fe80::1".parse().unwrap()));
        assert!(is_private_ipv6(
            "fe80::1234:5678:abcd:ef01".parse().unwrap()
        ));
    }

    #[test]
    fn test_public_ipv6_rejected() {
        // Public IPv6 should not be private
        assert!(!is_private_ipv6("2001:4860:4860::8888".parse().unwrap())); // Google DNS
        assert!(!is_private_ipv6("2606:4700:4700::1111".parse().unwrap())); // Cloudflare
    }

    #[test]
    fn test_validate_private_ip_accepts_valid() {
        // IPv4 private
        assert!(validate_private_ip("10.0.0.1").is_ok());
        assert!(validate_private_ip("192.168.1.1").is_ok());
        assert!(validate_private_ip("172.16.0.1").is_ok());

        // IPv6 private
        assert!(validate_private_ip("fd00::1").is_ok());
        assert!(validate_private_ip("::1").is_ok());
    }

    #[test]
    fn test_validate_private_ip_rejects_public() {
        // Public IPv4
        let result = validate_private_ip("8.8.8.8");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Public IPv4"));

        // Public IPv6
        let result = validate_private_ip("2001:4860:4860::8888");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Public IPv6"));
    }

    #[test]
    fn test_validate_private_ip_rejects_invalid() {
        let result = validate_private_ip("not-an-ip");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid IP address"));
    }

    #[test]
    fn test_validate_peer_addresses_empty() {
        let result = validate_peer_addresses(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_peer_addresses_valid() {
        let addrs = vec!["10.0.0.1".to_string(), "fd00::1".to_string()];
        assert!(validate_peer_addresses(&addrs).is_ok());
    }

    #[test]
    fn test_validate_peer_addresses_mixed_rejects() {
        // One valid, one public - should reject
        let addrs = vec!["10.0.0.1".to_string(), "8.8.8.8".to_string()];
        assert!(validate_peer_addresses(&addrs).is_err());
    }
}
