//! Peer discovery for cluster bootstrap.
//!
//! Provides the `discover_node_info` function for querying a peer's identity
//! via the `GetNodeInfo` RPC, and `parse_seed_addresses` for parsing the
//! `--join` flag into socket addresses.
//!
//! The discovery RPC is dialed over the in-house wire transport. Bootstrap
//! threads through the shared `NodeConnectionRegistry` (which already
//! holds the cluster's `WireClientTemplate`) so discovery dials reuse the
//! same TLS material as inter-node Raft. Because the dialed peer's node id
//! is not yet known, the discovery client is constructed via
//! `NodeConnectionRegistry::build_anonymous_wire_client` — the resulting
//! `Arc<WireClient>` is dropped at the end of the call rather than cached.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use inferadb_ledger_raft::node_registry::NodeConnectionRegistry;
use inferadb_ledger_wire::services::admin as wadmin;
use inferadb_ledger_wire_services::AdminServiceClient;
use tracing::debug;

/// A discovered node with identity information from GetNodeInfo RPC.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used by integration tests and lib consumers
pub struct DiscoveredNode {
    /// Node's Snowflake ID (auto-generated, persisted).
    pub node_id: u64,
    /// Node's wire address.
    pub addr: SocketAddr,
    /// True if node is already part of a cluster.
    pub is_cluster_member: bool,
    /// Current Raft term (0 if not in cluster).
    pub term: u64,
    /// Cluster ID (0 if uninitialized).
    pub cluster_id: u64,
    /// Node lifecycle state ("uninitialized" or "running").
    pub state: String,
}

/// Queries a peer for its node identity information via GetNodeInfo RPC.
///
/// Connects to a peer over the in-house wire transport and retrieves its
/// Snowflake ID, cluster membership status, current Raft term, and cluster
/// ID. The wire client is constructed from the shared registry's
/// [`inferadb_ledger_raft::node_registry::WireClientTemplate`] (so the same
/// TLS material that backs Raft replication backs discovery) and dropped
/// after the call — there is no per-peer cache for discovery dials.
///
/// Returns `None` if the registry has no wire template configured, the
/// QUIC connection fails, the RPC times out, or the address is invalid,
/// allowing callers to skip unreachable / misconfigured peers gracefully.
pub async fn discover_node_info(
    registry: &Arc<NodeConnectionRegistry>,
    addr: SocketAddr,
    timeout: Duration,
) -> Option<DiscoveredNode> {
    if addr.port() == 0 {
        debug!(peer = %addr, "Rejecting peer with port 0");
        return None;
    }

    if addr.ip().is_unspecified() {
        debug!(peer = %addr, "Rejecting unspecified peer address");
        return None;
    }

    debug!(peer = %addr, "Querying node info");

    let wire_client = match registry.build_anonymous_wire_client(&addr.to_string()) {
        Ok(c) => c,
        Err(e) => {
            debug!(peer = %addr, error = %e, "Failed to build wire client for discovery");
            return None;
        },
    };

    let client = AdminServiceClient::new(wire_client);
    // request_id is purely an SDK-side correlation token for retries; for a
    // one-shot discovery call we can synthesise one from the address bytes.
    let request_id: u128 = (u128::from(addr.port()) << 32) | u128::from(addr.ip_to_canonical_u64());

    match tokio::time::timeout(
        timeout,
        client.get_node_info(wadmin::GetNodeInfoRequest {}, request_id),
    )
    .await
    {
        Ok(Ok(info)) => {
            debug!(
                peer = %addr,
                node_id = info.node_id,
                is_cluster_member = info.is_cluster_member,
                cluster_id = info.cluster_id,
                state = %info.state,
                "Got node info"
            );
            Some(DiscoveredNode {
                node_id: info.node_id,
                addr,
                is_cluster_member: info.is_cluster_member,
                term: info.term,
                cluster_id: info.cluster_id,
                state: info.state,
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

/// Helper trait providing a stable canonical-IP encoding for the synthetic
/// `request_id`. Newtype wrapper avoids depending on unstable `IpAddr`
/// helpers.
trait CanonicalIp {
    fn ip_to_canonical_u64(&self) -> u64;
}

impl CanonicalIp for SocketAddr {
    fn ip_to_canonical_u64(&self) -> u64 {
        match self {
            SocketAddr::V4(v4) => u64::from(u32::from(*v4.ip())),
            SocketAddr::V6(v6) => {
                let octets = v6.ip().octets();
                let mut acc = 0u64;
                // Fold the 16-byte address into a u64; collisions are
                // acceptable because request_id is only an SDK-side
                // correlation token, not a security boundary.
                for chunk in octets.chunks(8) {
                    let mut bytes = [0u8; 8];
                    bytes[..chunk.len()].copy_from_slice(chunk);
                    acc ^= u64::from_le_bytes(bytes);
                }
                acc
            },
        }
    }
}

/// Parses seed addresses from string values into socket addresses.
///
/// Skips entries that cannot be parsed, logging a warning for each.
pub fn parse_seed_addresses(seeds: &[String]) -> Vec<SocketAddr> {
    seeds
        .iter()
        .filter_map(|s| {
            s.parse::<SocketAddr>().ok().or_else(|| {
                tracing::warn!(seed = %s, "Failed to parse seed address (expected host:port)");
                None
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_seed_addresses_valid() {
        let seeds = vec!["127.0.0.1:9090".to_string(), "192.168.1.1:50051".to_string()];
        let addrs = parse_seed_addresses(&seeds);
        assert_eq!(addrs.len(), 2);
    }

    #[test]
    fn test_parse_seed_addresses_skips_invalid() {
        let seeds = vec![
            "127.0.0.1:9090".to_string(),
            "not-a-valid-addr".to_string(),
            "192.168.1.1:50051".to_string(),
        ];
        let addrs = parse_seed_addresses(&seeds);
        assert_eq!(addrs.len(), 2);
    }

    #[test]
    fn test_parse_seed_addresses_empty() {
        let addrs = parse_seed_addresses(&[]);
        assert!(addrs.is_empty());
    }
}
