//! System discovery service implementation.
//!
//! Provides peer discovery and system state information for cluster coordination.
//!
//! Peer addresses must be private/WireGuard IPs. Public IPs are rejected to
//! ensure cluster traffic stays within the private network.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::Arc,
};

use inferadb_ledger_proto::proto::{
    AnnouncePeerRequest, AnnouncePeerResponse, GetPeersRequest, GetPeersResponse,
    GetSystemStateRequest, GetSystemStateResponse, NodeId, NodeInfo, NodeRole,
    OrganizationRegistry, OrganizationSlug, PeerInfo, ShardId,
    system_discovery_service_server::SystemDiscoveryService,
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use openraft::Raft;
use parking_lot::RwLock;
use tonic::{Request, Response, Status};

use crate::{
    log_storage::AppliedStateAccessor, peer_tracker::PeerTracker, types::LedgerTypeConfig,
};

// =============================================================================
// IP Address Validation
// =============================================================================

/// Checks if an IPv4 address is a private/internal address.
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

/// Checks if an IPv6 address is a private/internal address.
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

/// Validates that an IP address string is a private/WireGuard address.
///
/// Returns Ok(()) if valid, Err with reason if invalid.
fn validate_private_ip(addr: &str) -> Result<(), String> {
    // Parse as IP address (strips any port)
    let ip: IpAddr = addr.parse().map_err(|_| format!("Invalid IP address: {}", addr))?;

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
        },
        IpAddr::V6(ipv6) => {
            if is_private_ipv6(ipv6) {
                Ok(())
            } else {
                Err(format!(
                    "Public IPv6 address not allowed: {}. Use fd00::/8 (WireGuard) or fe80::/10 (link-local)",
                    addr
                ))
            }
        },
    }
}

/// Validates all addresses in a peer announcement.
///
/// All addresses must be private/WireGuard IPs.
#[allow(clippy::result_large_err)] // tonic::Status is external, can't box it
fn validate_peer_addresses(addresses: &[String]) -> Result<(), Status> {
    if addresses.is_empty() {
        return Err(Status::invalid_argument("Peer must have at least one address"));
    }

    for addr in addresses {
        if let Err(reason) = validate_private_ip(addr) {
            return Err(Status::invalid_argument(reason));
        }
    }

    Ok(())
}

/// Default learner cache TTL (5 seconds).
const DEFAULT_LEARNER_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// Provides peer discovery and system state for cluster coordination.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct DiscoveryServiceImpl {
    /// Raft consensus handle for membership and leader queries.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// State layer for organization registry access.
    #[allow(dead_code)] // retained to maintain Arc reference count
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (organization registry).
    applied_state: AppliedStateAccessor,
    /// Tracks when peers were last seen.
    #[builder(default = RwLock::new(PeerTracker::new()))]
    peer_tracker: RwLock<PeerTracker>,
    /// This node's ID for voter/learner checks (optional for backward compatibility).
    #[builder(default)]
    node_id: Option<crate::types::LedgerNodeId>,
    /// Timestamp of last state update (for learner staleness checks).
    #[builder(default = RwLock::new(std::time::Instant::now()))]
    last_state_update: RwLock<std::time::Instant>,
    /// Learner cache TTL (stale after this duration).
    #[builder(default = DEFAULT_LEARNER_CACHE_TTL)]
    learner_cache_ttl: std::time::Duration,
}

impl DiscoveryServiceImpl {
    /// Updates the last state update timestamp.
    ///
    /// Should be called after applying Raft entries.
    pub fn mark_state_updated(&self) {
        *self.last_state_update.write() = std::time::Instant::now();
    }

    /// Checks if this node is a voter (participates in consensus).
    ///
    /// Returns true if this node is in the voter set, or if node_id is not
    /// configured (assume voter behavior as fallback).
    fn is_voter(&self) -> bool {
        let Some(node_id) = self.node_id else {
            // No node_id configured, assume voter (skip staleness checks)
            return true;
        };

        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();

        // Check if this node is in the voter set
        membership.voter_ids().any(|id| id == node_id)
    }

    /// Checks if the local state cache is stale (for learners).
    ///
    /// Voters always have fresh state; learners check time since last update.
    fn is_cache_stale(&self) -> bool {
        if self.is_voter() {
            // Voters always have authoritative state
            return false;
        }

        // Learners check cache age
        let last_update = *self.last_state_update.read();
        last_update.elapsed() > self.learner_cache_ttl
    }

    /// Returns the current leader's node ID (for forwarding from stale learners).
    fn get_leader_hint(&self) -> Option<String> {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader.map(|id| id.to_string())
    }

    /// Prunes stale peers that haven't been seen within the staleness threshold.
    ///
    /// Peers not seen in >1 hour are pruned. This method should be called
    /// periodically (every 5 minutes).
    ///
    /// Returns the number of peers pruned.
    pub fn prune_stale_peers(&self) -> usize {
        let mut tracker = self.peer_tracker.write();
        tracker.prune_stale()
    }

    /// Returns the number of tracked peers.
    pub fn peer_count(&self) -> usize {
        let tracker = self.peer_tracker.read();
        tracker.peer_count()
    }

    /// Returns the maintenance interval from the peer tracker config.
    pub fn maintenance_interval(&self) -> std::time::Duration {
        let tracker = self.peer_tracker.read();
        tracker.maintenance_interval()
    }
}

#[tonic::async_trait]
impl SystemDiscoveryService for DiscoveryServiceImpl {
    async fn get_peers(
        &self,
        request: Request<GetPeersRequest>,
    ) -> Result<Response<GetPeersResponse>, Status> {
        let req = request.into_inner();
        let max_peers = if req.max_peers == 0 { 100 } else { req.max_peers as usize };

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
                    node_id: Some(NodeId { id: node_id_str.clone() }),
                    addresses: vec![node.addr.clone()],
                    grpc_port: 5000, // Default port
                    last_seen: tracker.get_last_seen(&node_id_str),
                }
            })
            .collect();

        Ok(Response::new(GetPeersResponse { peers, system_version: metrics.current_term }))
    }

    async fn announce_peer(
        &self,
        request: Request<AnnouncePeerRequest>,
    ) -> Result<Response<AnnouncePeerResponse>, Status> {
        let req = request.into_inner();

        // Validate the peer info
        let peer = req.peer.ok_or_else(|| Status::invalid_argument("Missing peer info"))?;

        let node_id =
            peer.node_id.as_ref().ok_or_else(|| Status::invalid_argument("Missing node_id"))?;

        // Validate gRPC port
        if peer.grpc_port == 0 || peer.grpc_port > 65535 {
            return Err(Status::invalid_argument(format!(
                "Invalid gRPC port: {}. Must be 1-65535",
                peer.grpc_port
            )));
        }

        // Validate addresses are private/WireGuard IPs
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

        // Check learner staleness.
        // If this is a learner with stale cache, return error with leader hint.
        if self.is_cache_stale() {
            let leader_hint = self.get_leader_hint();
            return Err(Status::unavailable(format!(
                "Learner cache is stale. Retry with leader: {}",
                leader_hint.unwrap_or_else(|| "unknown".to_string())
            )));
        }

        // Get current system version (Raft term)
        let metrics = self.raft.metrics().borrow().clone();
        let current_version = metrics.current_term;

        // If client already has current version, return empty response
        if req.if_version_greater_than >= current_version {
            return Ok(Response::new(GetSystemStateResponse {
                version: current_version,
                nodes: vec![],
                organizations: vec![],
            }));
        }

        // Build node info from Raft membership
        let membership = metrics.membership_config.membership();
        let voter_ids: std::collections::HashSet<_> = membership.voter_ids().collect();

        let nodes: Vec<NodeInfo> = membership
            .nodes()
            .map(|(id, node)| {
                let role = if voter_ids.contains(id) { NodeRole::Voter } else { NodeRole::Learner };
                NodeInfo {
                    node_id: Some(NodeId { id: id.to_string() }),
                    addresses: vec![node.addr.clone()],
                    grpc_port: 5000,
                    role: role.into(),
                    last_heartbeat: None,
                    joined_at: None,
                }
            })
            .collect();

        // Build organization registry from applied state
        let member_nodes: Vec<NodeId> =
            membership.nodes().map(|(id, _)| NodeId { id: id.to_string() }).collect();

        let organizations: Vec<OrganizationRegistry> = self
            .applied_state
            .list_organizations()
            .into_iter()
            .map(|ns| {
                let status = crate::proto_compat::organization_status_to_proto(ns.status);
                OrganizationRegistry {
                    organization_slug: Some(OrganizationSlug {
                        slug: self
                            .applied_state
                            .resolve_id_to_slug(ns.organization_id)
                            .map_or(ns.organization_id.value() as u64, |s| s.value()),
                    }),
                    name: ns.name,
                    shard_id: Some(ShardId { id: ns.shard_id.value() }),
                    members: member_nodes.clone(),
                    status: status.into(),
                    config_version: current_version,
                    created_at: None,
                }
            })
            .collect();

        Ok(Response::new(GetSystemStateResponse { version: current_version, nodes, organizations }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

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
        assert!(is_private_ipv6("fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv6_link_local() {
        // fe80::/10
        assert!(is_private_ipv6("fe80::1".parse().unwrap()));
        assert!(is_private_ipv6("fe80::1234:5678:abcd:ef01".parse().unwrap()));
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
