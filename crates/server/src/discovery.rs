//! Peer discovery via DNS SRV records.
//!
//! Per DESIGN.md §3.6: DNS SRV records enable dynamic bootstrap node management
//! without client reconfiguration. Nodes query `_ledger._tcp.<domain>` to discover
//! cluster entry points.
//!
//! Discovery order:
//! 1. Cached peers (from previous successful connections)
//! 2. Static config (from peers configuration)
//! 3. DNS SRV lookup (if srv_domain is configured)
//!
//! # Security
//!
//! The [`discover_node_info`] function currently uses plaintext HTTP for gRPC
//! connections during bootstrap coordination. This is acceptable for:
//! - Private networks with network-level isolation
//! - Development and testing environments
//!
//! For production deployments requiring transport encryption:
//! - Use network-level TLS termination (e.g., service mesh, load balancer)
//! - Deploy nodes on private networks with firewall rules
//! - Consider implementing server-side TLS configuration in a future release
//!
//! The `GetNodeInfo` RPC returns only non-sensitive coordination metadata,
//! minimizing exposure risk even without transport encryption.

use std::{net::SocketAddr, path::Path, time::Duration};

use hickory_resolver::{
    TokioAsyncResolver,
    config::{ResolverConfig, ResolverOpts},
};
use inferadb_ledger_raft::proto::{GetNodeInfoRequest, admin_service_client::AdminServiceClient};
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;
use tracing::{debug, info, warn};

// Re-export DiscoveryConfig from config for convenience
pub use crate::config::DiscoveryConfig;

/// A discovered peer from DNS SRV lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredPeer {
    /// Peer address (host:port).
    pub addr: String,
    /// SRV record priority (lower = preferred).
    pub priority: u16,
    /// SRV record weight (for load balancing among same priority).
    pub weight: u16,
}

/// A discovered node with identity information from GetNodeInfo RPC.
///
/// Used during bootstrap coordination to determine which node should
/// bootstrap the cluster (lowest Snowflake ID wins).
// Used by coordinator module in Task 5 (PRD.md)
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DiscoveredNode {
    /// Node's Snowflake ID (auto-generated, persisted).
    pub node_id: u64,
    /// Node's gRPC address.
    pub addr: SocketAddr,
    /// True if node is already part of a cluster.
    pub is_cluster_member: bool,
    /// Current Raft term (0 if not in cluster).
    pub term: u64,
}

/// Cached peers file format.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedPeers {
    /// Unix timestamp when cache was written.
    cached_at: u64,
    /// Discovered peers.
    peers: Vec<DiscoveredPeer>,
}

/// Resolve bootstrap peers using discovery methods.
///
/// Tries in order:
/// 1. Cached peers (if valid and not expired)
/// 2. DNS SRV lookup (if configured)
///
/// Returns addresses for cluster discovery.
pub async fn resolve_bootstrap_peers(discovery: &DiscoveryConfig) -> Vec<SocketAddr> {
    let mut addresses = Vec::new();

    if let Some(cached_path) = &discovery.cached_peers_path {
        match load_cached_peers(cached_path, discovery.cache_ttl_secs) {
            Ok(cached) => {
                debug!(count = cached.len(), "Loaded cached peers");
                for peer in cached {
                    if let Ok(addr) = peer.addr.parse::<SocketAddr>() {
                        addresses.push(addr);
                    }
                }
            },
            Err(e) => {
                debug!(error = %e, "No valid cached peers");
            },
        }
    }

    if let Some(domain) = &discovery.srv_domain {
        match dns_srv_lookup(domain).await {
            Ok(srv_peers) => {
                info!(count = srv_peers.len(), domain = %domain, "Discovered peers via DNS SRV");

                // Cache the discovered peers
                if let Some(cached_path) = &discovery.cached_peers_path {
                    if let Err(e) = save_cached_peers(cached_path, &srv_peers) {
                        warn!(error = %e, "Failed to cache discovered peers");
                    }
                }

                let mut sorted_peers = srv_peers;
                sorted_peers.sort_by(|a, b| {
                    a.priority.cmp(&b.priority).then_with(|| b.weight.cmp(&a.weight))
                });

                for peer in sorted_peers {
                    if let Ok(addr) = peer.addr.parse::<SocketAddr>() {
                        addresses.push(addr);
                    }
                }
            },
            Err(e) => {
                warn!(error = %e, domain = %domain, "DNS SRV lookup failed");
            },
        }
    }

    // Remove duplicates while preserving order
    let mut seen = std::collections::HashSet::new();
    addresses.retain(|addr| seen.insert(*addr));

    addresses
}

/// Query a peer for its node identity information via GetNodeInfo RPC.
///
/// This function connects to a peer and retrieves its Snowflake ID, cluster
/// membership status, and current Raft term. Used during bootstrap coordination
/// to determine which node should bootstrap the cluster.
///
/// Returns `None` if the connection fails, the RPC times out, or the address
/// is invalid, allowing callers to skip unreachable/invalid peers gracefully.
///
/// # Arguments
///
/// * `addr` - The peer's gRPC address
/// * `timeout` - Maximum time to wait for connection and RPC completion
///
/// # Security
///
/// This function validates the peer address before attempting connection:
/// - Rejects port 0 (unassigned/ephemeral)
/// - Rejects unspecified addresses (0.0.0.0, ::)
///
/// Network-level controls (firewalls, VPNs) should further restrict which
/// peers can be contacted in production environments.
pub async fn discover_node_info(addr: SocketAddr, timeout: Duration) -> Option<DiscoveredNode> {
    // Validate peer address before attempting connection
    if addr.port() == 0 {
        debug!(peer = %addr, "Rejecting peer with port 0");
        return None;
    }

    if addr.ip().is_unspecified() {
        debug!(peer = %addr, "Rejecting unspecified peer address");
        return None;
    }

    debug!(peer = %addr, "Querying node info");

    let endpoint = match Channel::from_shared(format!("http://{}", addr)) {
        Ok(ep) => ep.connect_timeout(timeout),
        Err(e) => {
            debug!(peer = %addr, error = %e, "Invalid peer address");
            return None;
        },
    };

    let channel = match endpoint.connect().await {
        Ok(ch) => ch,
        Err(e) => {
            debug!(peer = %addr, error = %e, "Failed to connect to peer");
            return None;
        },
    };

    let mut client = AdminServiceClient::new(channel);

    match tokio::time::timeout(timeout, client.get_node_info(GetNodeInfoRequest {})).await {
        Ok(Ok(response)) => {
            let info = response.into_inner();
            debug!(
                peer = %addr,
                node_id = info.node_id,
                is_cluster_member = info.is_cluster_member,
                "Got node info"
            );
            Some(DiscoveredNode {
                node_id: info.node_id,
                addr,
                is_cluster_member: info.is_cluster_member,
                term: info.term,
            })
        },
        Ok(Err(e)) => {
            debug!(peer = %addr, error = %e, "GetNodeInfo RPC failed");
            None
        },
        Err(_) => {
            debug!(peer = %addr, "GetNodeInfo RPC timed out");
            None
        },
    }
}

/// Perform DNS SRV lookup for `_ledger._tcp.<domain>`.
async fn dns_srv_lookup(domain: &str) -> Result<Vec<DiscoveredPeer>, DiscoveryError> {
    let srv_name = format!("_ledger._tcp.{}", domain);
    debug!(name = %srv_name, "Performing DNS SRV lookup");

    let resolver = TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());

    let srv_records = resolver
        .srv_lookup(&srv_name)
        .await
        .map_err(|e| DiscoveryError::DnsLookup(e.to_string()))?;

    let mut peers = Vec::new();

    for record in srv_records.iter() {
        let target = record.target().to_string();
        // Remove trailing dot from DNS name if present
        let host = target.trim_end_matches('.');

        match resolver.lookup_ip(host).await {
            Ok(ips) => {
                for ip in ips.iter() {
                    peers.push(DiscoveredPeer {
                        addr: SocketAddr::new(ip, record.port()).to_string(),
                        priority: record.priority(),
                        weight: record.weight(),
                    });
                }
            },
            Err(e) => {
                warn!(target = %host, error = %e, "Failed to resolve SRV target");
            },
        }
    }

    Ok(peers)
}

/// Load cached peers from file.
fn load_cached_peers(path: &str, ttl_secs: u64) -> Result<Vec<DiscoveredPeer>, DiscoveryError> {
    let content =
        std::fs::read_to_string(path).map_err(|e| DiscoveryError::CacheRead(e.to_string()))?;

    let cached: CachedPeers =
        serde_json::from_str(&content).map_err(|e| DiscoveryError::CacheParse(e.to_string()))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    if now.saturating_sub(cached.cached_at) > ttl_secs {
        return Err(DiscoveryError::CacheExpired);
    }

    Ok(cached.peers)
}

/// Save discovered peers to cache file.
fn save_cached_peers(path: &str, peers: &[DiscoveredPeer]) -> Result<(), DiscoveryError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let cached = CachedPeers { cached_at: now, peers: peers.to_vec() };

    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent).map_err(|e| DiscoveryError::CacheWrite(e.to_string()))?;
    }

    let content = serde_json::to_string_pretty(&cached)
        .map_err(|e| DiscoveryError::CacheWrite(e.to_string()))?;

    std::fs::write(path, content).map_err(|e| DiscoveryError::CacheWrite(e.to_string()))?;

    debug!(path, "Cached discovered peers");
    Ok(())
}

/// Discovery error types.
#[derive(Debug)]
pub enum DiscoveryError {
    /// DNS SRV lookup failed.
    DnsLookup(String),
    /// Failed to read cache file.
    CacheRead(String),
    /// Failed to parse cache file.
    CacheParse(String),
    /// Cache has expired.
    CacheExpired,
    /// Failed to write cache file.
    CacheWrite(String),
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiscoveryError::DnsLookup(msg) => write!(f, "DNS lookup failed: {}", msg),
            DiscoveryError::CacheRead(msg) => write!(f, "cache read error: {}", msg),
            DiscoveryError::CacheParse(msg) => write!(f, "cache parse error: {}", msg),
            DiscoveryError::CacheExpired => write!(f, "cached peers expired"),
            DiscoveryError::CacheWrite(msg) => write!(f, "cache write error: {}", msg),
        }
    }
}

impl std::error::Error for DiscoveryError {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_cache_roundtrip() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("peers.json");
        let path_str = path.to_str().expect("path to string");

        let peers = vec![
            DiscoveredPeer { addr: "192.168.1.1:50051".to_string(), priority: 10, weight: 100 },
            DiscoveredPeer { addr: "192.168.1.2:50051".to_string(), priority: 20, weight: 50 },
        ];

        // Save
        save_cached_peers(path_str, &peers).expect("save cache");

        // Load
        let loaded = load_cached_peers(path_str, 3600).expect("load cache");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].addr, "192.168.1.1:50051");
        assert_eq!(loaded[0].priority, 10);
    }

    #[test]
    fn test_cache_expiry() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("peers.json");
        let path_str = path.to_str().expect("path to string");

        // Create cache with timestamp in the past
        let cached = CachedPeers {
            cached_at: 0, // Very old
            peers: vec![],
        };
        let content = serde_json::to_string(&cached).expect("serialize");
        std::fs::write(&path, content).expect("write");

        // Should fail due to expiry
        let result = load_cached_peers(path_str, 3600);
        assert!(matches!(result, Err(DiscoveryError::CacheExpired)));
    }

    #[test]
    fn test_discovery_config_defaults() {
        let config = DiscoveryConfig::default();
        assert!(config.srv_domain.is_none());
        assert!(config.cached_peers_path.is_none());
        assert_eq!(config.cache_ttl_secs, 3600);
    }

    #[tokio::test]
    async fn test_resolve_with_no_discovery_configured() {
        let discovery = DiscoveryConfig::default();

        // With no discovery sources configured, should return empty
        let addresses = resolve_bootstrap_peers(&discovery).await;
        assert!(addresses.is_empty());
    }

    #[test]
    fn test_discovered_node_struct() {
        let node = DiscoveredNode {
            node_id: 12345,
            addr: "192.168.1.1:50051".parse().expect("valid addr"),
            is_cluster_member: false,
            term: 0,
        };

        assert_eq!(node.node_id, 12345);
        assert_eq!(node.addr.port(), 50051);
        assert!(!node.is_cluster_member);
        assert_eq!(node.term, 0);
    }

    #[test]
    fn test_discovered_node_cluster_member() {
        let node = DiscoveredNode {
            node_id: 67890,
            addr: "10.0.0.1:8080".parse().expect("valid addr"),
            is_cluster_member: true,
            term: 42,
        };

        assert_eq!(node.node_id, 67890);
        assert!(node.is_cluster_member);
        assert_eq!(node.term, 42);
    }

    #[tokio::test]
    async fn test_discover_node_info_unreachable_peer() {
        // Try to connect to a non-existent address
        let addr: SocketAddr = "127.0.0.1:59999".parse().expect("valid addr");
        let result = discover_node_info(addr, Duration::from_millis(100)).await;

        // Should return None for unreachable peer
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_discover_node_info_rejects_port_zero() {
        // Port 0 should be rejected for security
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid addr");
        let result = discover_node_info(addr, Duration::from_millis(100)).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_discover_node_info_rejects_unspecified_ipv4() {
        // 0.0.0.0 should be rejected
        let addr: SocketAddr = "0.0.0.0:50051".parse().expect("valid addr");
        let result = discover_node_info(addr, Duration::from_millis(100)).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_discover_node_info_rejects_unspecified_ipv6() {
        // [::] should be rejected
        let addr: SocketAddr = "[::]:50051".parse().expect("valid addr");
        let result = discover_node_info(addr, Duration::from_millis(100)).await;

        assert!(result.is_none());
    }
}
