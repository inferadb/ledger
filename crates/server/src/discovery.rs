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

use std::{net::SocketAddr, path::Path};

use hickory_resolver::{
    TokioAsyncResolver,
    config::{ResolverConfig, ResolverOpts},
};
use serde::{Deserialize, Serialize};
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

/// Cached peers file format.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedPeers {
    /// Unix timestamp when cache was written.
    cached_at: u64,
    /// Discovered peers.
    peers: Vec<DiscoveredPeer>,
}

/// Resolve bootstrap peers using multiple discovery methods.
///
/// Tries in order:
/// 1. Cached peers (if valid and not expired)
/// 2. Static peers from configuration
/// 3. DNS SRV lookup (if configured)
///
/// Returns addresses for load distribution.
pub async fn resolve_bootstrap_peers(
    static_peer_addrs: &[String],
    discovery: &DiscoveryConfig,
) -> Vec<SocketAddr> {
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

    for peer_addr in static_peer_addrs {
        if let Ok(addr) = peer_addr.parse::<SocketAddr>() {
            addresses.push(addr);
        } else {
            warn!(addr = %peer_addr, "Invalid static peer address");
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
    async fn test_resolve_with_static_peers_only() {
        let static_peers = vec!["127.0.0.1:50051".to_string(), "127.0.0.1:50052".to_string()];
        let discovery = DiscoveryConfig::default();

        let addresses = resolve_bootstrap_peers(&static_peers, &discovery).await;
        assert_eq!(addresses.len(), 2);
    }
}
