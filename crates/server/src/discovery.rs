//! Peer discovery for cluster bootstrap.
//!
//! Provides the `discover_node_info` function for querying a peer's identity
//! via the `GetNodeInfo` RPC, and `parse_seed_addresses` for parsing the
//! `--join` flag into socket addresses.

use std::{net::SocketAddr, time::Duration};

use inferadb_ledger_proto::proto::{GetNodeInfoRequest, admin_service_client::AdminServiceClient};
use tonic::transport::Channel;
use tracing::debug;

/// A discovered node with identity information from GetNodeInfo RPC.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used by integration tests and lib consumers
pub struct DiscoveredNode {
    /// Node's Snowflake ID (auto-generated, persisted).
    pub node_id: u64,
    /// Node's gRPC address.
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
/// Connects to a peer and retrieves its Snowflake ID, cluster
/// membership status, current Raft term, and cluster ID.
///
/// Returns `None` if the connection fails, the RPC times out, or the address
/// is invalid, allowing callers to skip unreachable/invalid peers gracefully.
pub async fn discover_node_info(addr: SocketAddr, timeout: Duration) -> Option<DiscoveredNode> {
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
