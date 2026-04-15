//! Peer discovery and system state for cluster coordination.
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
    GetSystemStateRequest, GetSystemStateResponse, LeaderUpdate, NodeId, NodeInfo,
    OrganizationRegistry, OrganizationSlug, PeerInfo, Region as ProtoRegion,
    ResolveRegionLeaderRequest, ResolveRegionLeaderResponse, WatchLeaderRequest,
    system_discovery_service_server::SystemDiscoveryService,
};
use inferadb_ledger_raft::{
    ConsensusHandle, log_storage::AppliedStateAccessor, peer_tracker::PeerTracker,
    raft_manager::RaftManager,
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use parking_lot::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

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
/// - fc00::/7 (Unique Local Addresses)
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
pub struct DiscoveryService {
    /// Consensus handle for leadership and term queries.
    handle: Arc<ConsensusHandle>,
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
    node_id: Option<inferadb_ledger_raft::types::LedgerNodeId>,
    /// Timestamp of last state update (for learner staleness checks).
    #[builder(default = RwLock::new(std::time::Instant::now()))]
    last_state_update: RwLock<std::time::Instant>,
    /// Learner cache TTL (stale after this duration).
    #[builder(default = DEFAULT_LEARNER_CACHE_TTL)]
    learner_cache_ttl: std::time::Duration,
    /// Geographic region this node belongs to.
    ///
    /// Included in system state responses for peer region awareness.
    #[builder(default = inferadb_ledger_types::Region::GLOBAL)]
    #[allow(dead_code)]
    region: inferadb_ledger_types::Region,
    /// Raft manager for membership queries that need node addresses.
    #[builder(default)]
    #[allow(dead_code)]
    raft_manager: Option<Arc<RaftManager>>,
    /// Shared peer address map for resolving peer network addresses.
    ///
    /// Updated by `announce_peer` and used by discovery RPCs.
    #[builder(default)]
    peer_addresses: Option<inferadb_ledger_raft::PeerAddressMap>,
}

impl DiscoveryService {
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

        // Check if this node is a voter by comparing its ID against the
        // shard state voter set. We compare the raw u64 values since
        // inferadb_ledger_consensus::types::NodeId is not accessible from
        // this crate.
        self.handle.shard_state().voters.iter().any(|v| v.0 == node_id)
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
        self.handle.current_leader().map(|id| id.to_string())
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

    /// Resolves the leader endpoint for a data residency region.
    ///
    /// Returns the leader's endpoint URL, the current Raft term, and a
    /// recommended cache TTL in seconds. Currently all regions share a
    /// single Raft group, so the resolved leader is the same regardless
    /// of region; the region parameter is validated and reserved for
    /// future multi-Raft routing.
    fn resolve_region_leader_impl(
        &self,
        _region: inferadb_ledger_types::Region,
    ) -> std::result::Result<(String, u64, u32), Status> {
        let state = self.handle.shard_state();
        let current_term = self.handle.current_term();

        let leader_id = state.leader.ok_or_else(|| {
            super::metadata::status_with_not_leader_hint(
                "No leader elected in this region",
                None,
                None,
                Some(current_term),
            )
        })?;

        let endpoint =
            self.peer_addresses.as_ref().and_then(|m| m.get(leader_id.0)).ok_or_else(|| {
                Status::unavailable(format!(
                    "Leader {} address not yet known — peer has not announced itself",
                    leader_id.0
                ))
            })?;

        // Recommend a short TTL since leadership can change on election.
        Ok((endpoint, current_term, 10))
    }
}

#[tonic::async_trait]
impl SystemDiscoveryService for DiscoveryService {
    async fn get_peers(
        &self,
        request: Request<GetPeersRequest>,
    ) -> Result<Response<GetPeersResponse>, Status> {
        let _req = request.into_inner();

        let current_term = self.handle.current_term();
        let state = self.handle.shard_state();

        let peers: Vec<PeerInfo> = state
            .voters
            .iter()
            .chain(state.learners.iter())
            .filter_map(|node_id| {
                let addr_with_port = self.peer_addresses.as_ref()?.get(node_id.0)?;
                // Split "host:port" into address and port components.
                let (host, port_str) = addr_with_port.rsplit_once(':')?;
                let grpc_port: u32 = port_str.parse().ok()?;
                Some(PeerInfo {
                    node_id: Some(NodeId { id: node_id.0.to_string() }),
                    addresses: vec![host.to_string()],
                    grpc_port,
                    last_seen: None,
                })
            })
            .collect();

        Ok(Response::new(GetPeersResponse { peers, system_version: current_term }))
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

        // Update the shared peer address map so other services can resolve
        // this peer's address for forwarding and health checks.
        if let Some(ref peer_addresses) = self.peer_addresses
            && let (Ok(parsed_id), Some(first_addr)) =
                (node_id.id.parse::<u64>(), peer.addresses.first())
        {
            let address = format!("{first_addr}:{}", peer.grpc_port);
            peer_addresses.insert(parsed_id, address);
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
        let current_version = self.handle.current_term();

        // If client already has current version, return empty response
        if req.if_version_greater_than >= current_version {
            return Ok(Response::new(GetSystemStateResponse {
                version: current_version,
                nodes: vec![],
                organizations: vec![],
            }));
        }

        let state = self.handle.shard_state();

        // Build node list from voters that have announced their addresses.
        let nodes: Vec<NodeInfo> = state
            .voters
            .iter()
            .chain(state.learners.iter())
            .filter_map(|node_id| {
                let addr_with_port = self.peer_addresses.as_ref()?.get(node_id.0)?;
                let (host, port_str) = addr_with_port.rsplit_once(':')?;
                let grpc_port: u32 = port_str.parse().ok()?;
                Some(NodeInfo {
                    node_id: Some(NodeId { id: node_id.0.to_string() }),
                    addresses: vec![host.to_string()],
                    grpc_port,
                    last_heartbeat: None,
                    joined_at: None,
                    region: ProtoRegion::Global.into(),
                })
            })
            .collect();

        // Member node IDs for the organization registry entries.
        let member_nodes: Vec<NodeId> =
            state.voters.iter().map(|n| NodeId { id: n.0.to_string() }).collect();

        let organizations: Vec<OrganizationRegistry> = self
            .applied_state
            .list_organizations()
            .into_iter()
            .map(|ns| {
                let status = crate::proto_compat::organization_status_to_proto(ns.status);
                OrganizationRegistry {
                    slug: Some(OrganizationSlug {
                        slug: self
                            .applied_state
                            .resolve_id_to_slug(ns.organization)
                            .map_or(ns.organization.value() as u64, |s| s.value()),
                    }),
                    name: String::new(),
                    region: ProtoRegion::Global.into(),
                    members: member_nodes.clone(),
                    status: status.into(),
                    config_version: current_version,
                    created_at: None,
                }
            })
            .collect();

        Ok(Response::new(GetSystemStateResponse { version: current_version, nodes, organizations }))
    }

    async fn resolve_region_leader(
        &self,
        request: Request<ResolveRegionLeaderRequest>,
    ) -> Result<Response<ResolveRegionLeaderResponse>, Status> {
        let req = request.into_inner();

        let proto_region = ProtoRegion::try_from(req.region).unwrap_or(ProtoRegion::Unspecified);
        let region = inferadb_ledger_types::Region::try_from(proto_region)?;

        let (endpoint, raft_term, ttl_seconds) = self.resolve_region_leader_impl(region)?;

        Ok(Response::new(ResolveRegionLeaderResponse { endpoint, raft_term, ttl_seconds }))
    }

    type WatchLeaderStream = ReceiverStream<Result<LeaderUpdate, Status>>;

    async fn watch_leader(
        &self,
        request: Request<WatchLeaderRequest>,
    ) -> Result<Response<Self::WatchLeaderStream>, Status> {
        let req = request.into_inner();

        // Validate region — reject unrecognized values.
        let proto_region = ProtoRegion::try_from(req.region).unwrap_or(ProtoRegion::Unspecified);
        let _region = inferadb_ledger_types::Region::try_from(proto_region)?;

        // Currently all regions share a single consensus handle. The region
        // parameter is validated and reserved for future multi-Raft routing,
        // mirroring `resolve_region_leader`.
        let mut state_rx = self.handle.state_watch();
        let peer_addresses = self.peer_addresses.clone();

        // Bounded channel — if the client cannot keep up we drop the stream
        // and let the client reconnect. Capacity of 16 is generous for a
        // push feed whose expected rate is at most a few updates per second
        // even during rapid re-elections.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<LeaderUpdate, Status>>(16);

        tokio::spawn(async move {
            // Emit initial state immediately.
            let initial = make_leader_update(&state_rx.borrow(), peer_addresses.as_ref());
            if tx.send(Ok(initial)).await.is_err() {
                return;
            }

            // Stream subsequent leader changes.
            while state_rx.changed().await.is_ok() {
                let update = make_leader_update(&state_rx.borrow(), peer_addresses.as_ref());
                if tx.send(Ok(update)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Builds a `LeaderUpdate` message from a shard state snapshot and the
/// shared peer address map.
fn make_leader_update(
    state: &inferadb_ledger_consensus::leadership::ShardState,
    peer_addresses: Option<&inferadb_ledger_raft::PeerAddressMap>,
) -> LeaderUpdate {
    match state.leader {
        Some(leader) => {
            let endpoint = peer_addresses
                .and_then(|m| m.get(leader.0))
                .map(|addr| format!("http://{addr}"))
                .unwrap_or_default();
            let leader_node_id = if endpoint.is_empty() { 0 } else { leader.0 };
            LeaderUpdate { endpoint, raft_term: state.term, leader_node_id }
        },
        None => LeaderUpdate { endpoint: String::new(), raft_term: state.term, leader_node_id: 0 },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    // =========================================================================
    // is_private_ipv4
    // =========================================================================

    #[test]
    fn private_ipv4_class_a_range_start() {
        assert!(is_private_ipv4("10.0.0.1".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_class_a_range_end() {
        assert!(is_private_ipv4("10.255.255.255".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_class_b_range_start() {
        assert!(is_private_ipv4("172.16.0.1".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_class_b_range_end() {
        assert!(is_private_ipv4("172.31.255.255".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_class_b_below_range_rejected() {
        assert!(!is_private_ipv4("172.15.0.1".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_class_b_above_range_rejected() {
        assert!(!is_private_ipv4("172.32.0.1".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_class_c_range_start() {
        assert!(is_private_ipv4("192.168.0.1".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_class_c_range_end() {
        assert!(is_private_ipv4("192.168.255.255".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_class_c_below_range_rejected() {
        assert!(!is_private_ipv4("192.167.1.1".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_class_c_above_range_rejected() {
        assert!(!is_private_ipv4("192.169.1.1".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_loopback() {
        assert!(is_private_ipv4("127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_loopback_range_end() {
        assert!(is_private_ipv4("127.255.255.255".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_link_local_range_start() {
        assert!(is_private_ipv4("169.254.0.1".parse().unwrap()));
    }

    #[test]
    fn private_ipv4_link_local_range_end() {
        assert!(is_private_ipv4("169.254.255.255".parse().unwrap()));
    }

    #[test]
    fn public_ipv4_rejected() {
        assert!(!is_private_ipv4("8.8.8.8".parse().unwrap()));
    }

    // =========================================================================
    // is_private_ipv6
    // =========================================================================

    #[test]
    fn private_ipv6_loopback() {
        assert!(is_private_ipv6("::1".parse().unwrap()));
    }

    #[test]
    fn private_ipv6_ula_range_start() {
        assert!(is_private_ipv6("fd00::1".parse().unwrap()));
    }

    #[test]
    fn private_ipv6_ula_range_end() {
        assert!(is_private_ipv6("fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap()));
    }

    #[test]
    fn private_ipv6_link_local() {
        assert!(is_private_ipv6("fe80::1".parse().unwrap()));
    }

    #[test]
    fn public_ipv6_rejected() {
        assert!(!is_private_ipv6("2001:4860:4860::8888".parse().unwrap()));
    }

    // =========================================================================
    // validate_private_ip
    // =========================================================================

    #[test]
    fn validate_private_ip_accepts_ipv4_private() {
        assert!(validate_private_ip("10.0.0.1").is_ok());
    }

    #[test]
    fn validate_private_ip_accepts_ipv6_private() {
        assert!(validate_private_ip("fd00::1").is_ok());
    }

    #[test]
    fn validate_private_ip_rejects_public_ipv4() {
        let result = validate_private_ip("8.8.8.8");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Public IPv4"));
    }

    #[test]
    fn validate_private_ip_rejects_public_ipv6() {
        let result = validate_private_ip("2001:4860:4860::8888");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Public IPv6"));
    }

    #[test]
    fn validate_private_ip_rejects_invalid_string() {
        let result = validate_private_ip("not-an-ip");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid IP address"));
    }

    // =========================================================================
    // validate_peer_addresses
    // =========================================================================

    #[test]
    fn validate_peer_addresses_empty_rejected() {
        let result = validate_peer_addresses(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn validate_peer_addresses_all_private_accepted() {
        let addrs = vec!["10.0.0.1".to_string(), "fd00::1".to_string()];
        assert!(validate_peer_addresses(&addrs).is_ok());
    }

    #[test]
    fn validate_peer_addresses_mixed_public_rejected() {
        let addrs = vec!["10.0.0.1".to_string(), "8.8.8.8".to_string()];
        assert!(validate_peer_addresses(&addrs).is_err());
    }
}
