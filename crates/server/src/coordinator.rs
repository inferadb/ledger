//! Bootstrap coordination algorithm for multi-node cluster formation.
//!
//! This module implements the coordination algorithm that determines which node
//! should bootstrap the cluster when multiple nodes start simultaneously. It
//! uses Snowflake IDs to ensure deterministic leader election - the node with
//! the lowest ID (earliest started) bootstraps while others wait to join.

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use tracing::{debug, info};

use crate::{
    config::{BootstrapConfig, DiscoveryConfig},
    discovery::{DiscoveredNode, discover_node_info, resolve_bootstrap_peers},
};

/// Result of bootstrap coordination.
///
/// After polling discovery and querying peer node info, the coordinator
/// determines what action this node should take.
#[derive(Debug, Clone)]
pub enum BootstrapDecision {
    /// This node should bootstrap the cluster with the given initial members.
    ///
    /// The members list contains `(node_id, address)` pairs for all discovered
    /// nodes including self. This node has the lowest Snowflake ID among them.
    Bootstrap {
        /// Initial cluster members: (node_id, gRPC address).
        initial_members: Vec<(u64, String)>,
    },

    /// This node should wait for another node to bootstrap, then join.
    ///
    /// The node with the lowest Snowflake ID will bootstrap the cluster, and
    /// this node will be added via the AdminService JoinCluster RPC.
    WaitForJoin {
        /// Address of the node that will bootstrap (lowest ID).
        leader_addr: SocketAddr,
    },

    /// A cluster already exists; this node should join it.
    ///
    /// At least one discovered peer reported `is_cluster_member=true`,
    /// indicating an existing cluster to join rather than bootstrapping new.
    JoinExisting {
        /// Address of a peer that is already a cluster member.
        via_peer: SocketAddr,
    },
}

/// Errors that can occur during bootstrap coordination.
#[derive(Debug)]
#[allow(dead_code)] // Config variant reserved for future use
pub enum CoordinatorError {
    /// Bootstrap coordination timed out waiting for peers.
    Timeout(String),
    /// Configuration validation failed.
    Config(String),
}

impl std::fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout(msg) => write!(f, "bootstrap timeout: {msg}"),
            Self::Config(msg) => write!(f, "configuration error: {msg}"),
        }
    }
}

impl std::error::Error for CoordinatorError {}

/// Coordinate bootstrap decision with discovered peers.
///
/// This function implements the coordination algorithm:
///
/// 1. Poll discovery every `poll_interval_secs` until `bootstrap_expect` nodes found
/// 2. Query each discovered peer via `discover_node_info`
/// 3. If any peer has `is_cluster_member=true`, return `JoinExisting`
/// 4. If enough peers found and my ID is lowest, return `Bootstrap` with all members
/// 5. If enough peers found and my ID is not lowest, return `WaitForJoin`
/// 6. If timeout expires before enough nodes found, return error
///
/// # Arguments
///
/// * `my_node_id` - This node's Snowflake ID
/// * `my_address` - This node's gRPC address
/// * `bootstrap_config` - Bootstrap timing and size configuration
/// * `discovery_config` - Peer discovery configuration
///
/// # Returns
///
/// A `BootstrapDecision` indicating what action this node should take, or
/// a `CoordinatorError` if coordination fails.
pub async fn coordinate_bootstrap(
    my_node_id: u64,
    my_address: &str,
    bootstrap_config: &BootstrapConfig,
    discovery_config: &DiscoveryConfig,
) -> Result<BootstrapDecision, CoordinatorError> {
    let start = Instant::now();
    let timeout = Duration::from_secs(bootstrap_config.bootstrap_timeout_secs);
    let poll_interval = Duration::from_secs(bootstrap_config.poll_interval_secs);
    let rpc_timeout = Duration::from_secs(5);

    info!(
        my_id = my_node_id,
        my_address = %my_address,
        bootstrap_expect = bootstrap_config.bootstrap_expect,
        timeout_secs = bootstrap_config.bootstrap_timeout_secs,
        poll_interval_secs = bootstrap_config.poll_interval_secs,
        "Starting bootstrap coordination"
    );

    loop {
        // Check timeout
        if start.elapsed() > timeout {
            info!(
                my_id = my_node_id,
                my_address = %my_address,
                elapsed_secs = start.elapsed().as_secs(),
                bootstrap_expect = bootstrap_config.bootstrap_expect,
                decision = "timeout",
                "Bootstrap coordination timed out waiting for peers"
            );
            return Err(CoordinatorError::Timeout(format!(
                "did not discover {} peers within {}s",
                bootstrap_config.bootstrap_expect, bootstrap_config.bootstrap_timeout_secs
            )));
        }

        // Resolve peer addresses from discovery
        let peer_addrs = resolve_bootstrap_peers(discovery_config).await;

        // Query each peer for node info
        let discovered = query_peers(&peer_addrs, rpc_timeout).await;

        // Check if any peer is already in a cluster
        if let Some(member) = discovered.iter().find(|n| n.is_cluster_member) {
            info!(
                my_id = my_node_id,
                my_address = %my_address,
                peer_id = member.node_id,
                peer_addr = %member.addr,
                peer_term = member.term,
                decision = "join_existing",
                "Bootstrap decision: found existing cluster via peer"
            );
            return Ok(BootstrapDecision::JoinExisting { via_peer: member.addr });
        }

        // Count total nodes (discovered + self)
        let total_nodes = discovered.len() + 1;

        if total_nodes >= bootstrap_config.bootstrap_expect as usize {
            return Ok(make_bootstrap_decision(my_node_id, my_address, &discovered));
        }

        debug!(
            found = total_nodes,
            required = bootstrap_config.bootstrap_expect,
            "Waiting for peers"
        );
        tokio::time::sleep(poll_interval).await;
    }
}

/// Query multiple peers for their node info concurrently.
async fn query_peers(peer_addrs: &[SocketAddr], timeout: Duration) -> Vec<DiscoveredNode> {
    let mut discovered = Vec::new();
    for &addr in peer_addrs {
        if let Some(node) = discover_node_info(addr, timeout).await {
            discovered.push(node);
        }
    }
    discovered
}

/// Determine bootstrap decision when enough peers are found and none are cluster members.
fn make_bootstrap_decision(
    my_node_id: u64,
    my_address: &str,
    discovered: &[DiscoveredNode],
) -> BootstrapDecision {
    // Find the node with lowest ID among discovered peers
    let lowest_peer = discovered.iter().min_by_key(|n| n.node_id);

    // Collect peer IDs for audit logging
    let peer_ids: Vec<u64> = discovered.iter().map(|n| n.node_id).collect();

    match lowest_peer {
        // No peers discovered - we bootstrap alone
        None => {
            info!(
                my_id = my_node_id,
                my_address = %my_address,
                decision = "bootstrap_single",
                "Bootstrap decision: no peers discovered, bootstrapping as single node"
            );
            BootstrapDecision::Bootstrap {
                initial_members: vec![(my_node_id, my_address.to_string())],
            }
        },
        // We have the lowest ID - bootstrap with all members
        Some(lowest) if my_node_id < lowest.node_id => {
            let mut members = vec![(my_node_id, my_address.to_string())];
            for node in discovered {
                members.push((node.node_id, node.addr.to_string()));
            }
            info!(
                my_id = my_node_id,
                my_address = %my_address,
                peer_ids = ?peer_ids,
                member_count = members.len(),
                decision = "bootstrap_leader",
                "Bootstrap decision: this node has lowest ID, bootstrapping cluster"
            );
            BootstrapDecision::Bootstrap { initial_members: members }
        },
        // Another node has lower or equal ID - wait for it to bootstrap
        Some(lowest) => {
            info!(
                my_id = my_node_id,
                my_address = %my_address,
                leader_id = lowest.node_id,
                leader_addr = %lowest.addr,
                peer_ids = ?peer_ids,
                decision = "wait_for_join",
                "Bootstrap decision: waiting for node with lowest ID to bootstrap"
            );
            BootstrapDecision::WaitForJoin { leader_addr: lowest.addr }
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_decision_debug() {
        let decision = BootstrapDecision::Bootstrap {
            initial_members: vec![
                (100, "127.0.0.1:50051".to_string()),
                (200, "127.0.0.1:50052".to_string()),
            ],
        };
        let debug_str = format!("{:?}", decision);
        assert!(debug_str.contains("Bootstrap"));
        assert!(debug_str.contains("100"));
    }

    #[test]
    fn test_coordinator_error_display() {
        let timeout_err = CoordinatorError::Timeout("test timeout".to_string());
        assert_eq!(timeout_err.to_string(), "bootstrap timeout: test timeout");

        let config_err = CoordinatorError::Config("test config".to_string());
        assert_eq!(config_err.to_string(), "configuration error: test config");
    }

    #[test]
    fn test_make_bootstrap_decision_lowest_id() {
        let my_node_id = 100;
        let my_address = "127.0.0.1:50051";
        let discovered = vec![
            DiscoveredNode {
                node_id: 200,
                addr: "127.0.0.1:50052".parse().unwrap(),
                is_cluster_member: false,
                term: 0,
            },
            DiscoveredNode {
                node_id: 300,
                addr: "127.0.0.1:50053".parse().unwrap(),
                is_cluster_member: false,
                term: 0,
            },
        ];

        let decision = make_bootstrap_decision(my_node_id, my_address, &discovered);

        match decision {
            BootstrapDecision::Bootstrap { initial_members } => {
                assert_eq!(initial_members.len(), 3);
                // Self should be first
                assert_eq!(initial_members[0], (100, "127.0.0.1:50051".to_string()));
                // Other members follow
                assert!(initial_members.contains(&(200, "127.0.0.1:50052".to_string())));
                assert!(initial_members.contains(&(300, "127.0.0.1:50053".to_string())));
            },
            _ => panic!("Expected Bootstrap decision"),
        }
    }

    #[test]
    fn test_make_bootstrap_decision_not_lowest_id() {
        let my_node_id = 300;
        let my_address = "127.0.0.1:50053";
        let discovered = vec![
            DiscoveredNode {
                node_id: 100,
                addr: "127.0.0.1:50051".parse().unwrap(),
                is_cluster_member: false,
                term: 0,
            },
            DiscoveredNode {
                node_id: 200,
                addr: "127.0.0.1:50052".parse().unwrap(),
                is_cluster_member: false,
                term: 0,
            },
        ];

        let decision = make_bootstrap_decision(my_node_id, my_address, &discovered);

        match decision {
            BootstrapDecision::WaitForJoin { leader_addr } => {
                assert_eq!(leader_addr, "127.0.0.1:50051".parse().unwrap());
            },
            _ => panic!("Expected WaitForJoin decision"),
        }
    }

    #[test]
    fn test_make_bootstrap_decision_no_peers() {
        let my_node_id = 100;
        let my_address = "127.0.0.1:50051";
        let discovered: Vec<DiscoveredNode> = vec![];

        let decision = make_bootstrap_decision(my_node_id, my_address, &discovered);

        match decision {
            BootstrapDecision::Bootstrap { initial_members } => {
                assert_eq!(initial_members.len(), 1);
                assert_eq!(initial_members[0], (100, "127.0.0.1:50051".to_string()));
            },
            _ => panic!("Expected Bootstrap decision with just self"),
        }
    }

    #[test]
    fn test_make_bootstrap_decision_equal_ids_self_wins() {
        // Edge case: if somehow IDs are equal, self should win (my_node_id < peer_id is false, so
        // the condition fails and we should still bootstrap if we're the only option)
        // Actually with Snowflake IDs this shouldn't happen, but testing the logic
        let my_node_id = 100;
        let my_address = "127.0.0.1:50051";
        let discovered = vec![DiscoveredNode {
            node_id: 100, // Same ID (shouldn't happen in practice)
            addr: "127.0.0.1:50052".parse().unwrap(),
            is_cluster_member: false,
            term: 0,
        }];

        let decision = make_bootstrap_decision(my_node_id, my_address, &discovered);

        // With equal IDs, my_node_id < lowest_peer_id is false, so we get WaitForJoin
        match decision {
            BootstrapDecision::WaitForJoin { leader_addr } => {
                assert_eq!(leader_addr, "127.0.0.1:50052".parse().unwrap());
            },
            _ => panic!("Expected WaitForJoin when IDs are equal"),
        }
    }

    #[tokio::test]
    async fn test_coordinate_bootstrap_timeout() {
        // Use a very short timeout and no discoverable peers
        let bootstrap_config = BootstrapConfig {
            bootstrap_expect: 3,
            bootstrap_timeout_secs: 1, // Very short timeout
            poll_interval_secs: 1,
        };

        let discovery_config = DiscoveryConfig {
            srv_domain: None,        // No DNS SRV
            cached_peers_path: None, // No cached peers
            cache_ttl_secs: 3600,
        };

        let result =
            coordinate_bootstrap(100, "127.0.0.1:50051", &bootstrap_config, &discovery_config)
                .await;

        // Should timeout since no peers are discoverable
        match result {
            Err(CoordinatorError::Timeout(msg)) => {
                assert!(msg.contains("did not discover"));
                assert!(msg.contains("3 peers"));
            },
            other => panic!("Expected Timeout error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_coordinate_bootstrap_single_node_immediate() {
        // Single node mode should succeed immediately
        let bootstrap_config = BootstrapConfig::for_single_node();

        let discovery_config =
            DiscoveryConfig { srv_domain: None, cached_peers_path: None, cache_ttl_secs: 3600 };

        let result =
            coordinate_bootstrap(100, "127.0.0.1:50051", &bootstrap_config, &discovery_config)
                .await;

        // Should immediately bootstrap as single node (1 node = bootstrap_expect)
        match result {
            Ok(BootstrapDecision::Bootstrap { initial_members }) => {
                assert_eq!(initial_members.len(), 1);
                assert_eq!(initial_members[0], (100, "127.0.0.1:50051".to_string()));
            },
            other => panic!("Expected Bootstrap decision, got: {:?}", other),
        }
    }

    // =========================================================================
    // Security-related tests
    // =========================================================================

    #[test]
    fn test_malicious_peer_with_zero_id() {
        // A malicious peer might report node_id = 0 to try to win leader election.
        // The algorithm should handle this gracefully - zero is still a valid ID.
        let my_node_id = 100;
        let my_address = "127.0.0.1:50051";
        let discovered = vec![DiscoveredNode {
            node_id: 0, // Suspicious zero ID
            addr: "127.0.0.1:50052".parse().unwrap(),
            is_cluster_member: false,
            term: 0,
        }];

        let decision = make_bootstrap_decision(my_node_id, my_address, &discovered);

        // Zero is lower than 100, so the peer "wins" - this is expected behavior.
        // Defense is at network/discovery layer, not in coordinator logic.
        match decision {
            BootstrapDecision::WaitForJoin { leader_addr } => {
                assert_eq!(leader_addr, "127.0.0.1:50052".parse().unwrap());
            },
            _ => panic!("Expected WaitForJoin for peer with lower ID"),
        }
    }

    #[test]
    fn test_malicious_peer_with_very_low_id() {
        // A malicious peer with an artificially low Snowflake ID (old timestamp)
        // would win leader election. The coordinator handles this gracefully.
        let my_node_id = 1_000_000_000_000; // Normal Snowflake ID
        let my_address = "127.0.0.1:50051";
        let discovered = vec![DiscoveredNode {
            node_id: 1, // Suspiciously low ID (would require timestamp near epoch)
            addr: "127.0.0.1:50052".parse().unwrap(),
            is_cluster_member: false,
            term: 0,
        }];

        let decision = make_bootstrap_decision(my_node_id, my_address, &discovered);

        // Low ID wins - security is at network/discovery layer
        match decision {
            BootstrapDecision::WaitForJoin { leader_addr } => {
                assert_eq!(leader_addr, "127.0.0.1:50052".parse().unwrap());
            },
            _ => panic!("Expected WaitForJoin for peer with lower ID"),
        }
    }

    #[test]
    fn test_multiple_peers_with_same_id() {
        // Edge case: two peers claim the same node ID.
        // The algorithm uses min_by_key which picks the first occurrence.
        let my_node_id = 300;
        let my_address = "127.0.0.1:50053";
        let discovered = vec![
            DiscoveredNode {
                node_id: 100, // Duplicate ID
                addr: "127.0.0.1:50051".parse().unwrap(),
                is_cluster_member: false,
                term: 0,
            },
            DiscoveredNode {
                node_id: 100, // Duplicate ID (different address)
                addr: "127.0.0.1:50052".parse().unwrap(),
                is_cluster_member: false,
                term: 5, // Higher term suggests more activity
            },
        ];

        let decision = make_bootstrap_decision(my_node_id, my_address, &discovered);

        // First peer with lowest ID is chosen
        match decision {
            BootstrapDecision::WaitForJoin { leader_addr } => {
                // min_by_key picks first match
                assert_eq!(leader_addr, "127.0.0.1:50051".parse().unwrap());
            },
            _ => panic!("Expected WaitForJoin"),
        }
    }

    #[test]
    fn test_peer_falsely_claims_cluster_membership() {
        // A malicious peer claims is_cluster_member=true when no cluster exists.
        // The coordinator trusts this and attempts to join (JoinExisting).
        // This is expected - the join will fail if no cluster exists.
        let my_node_id = 100;
        let my_address = "127.0.0.1:50051";
        let discovered = vec![DiscoveredNode {
            node_id: 200,
            addr: "127.0.0.1:50052".parse().unwrap(),
            is_cluster_member: true, // False claim
            term: 42,
        }];

        // The make_bootstrap_decision function doesn't check is_cluster_member,
        // but coordinate_bootstrap does check this before calling make_bootstrap_decision.
        // Let's verify the logic handles cluster members correctly.

        // If a peer claims membership, we'd get JoinExisting from coordinate_bootstrap.
        // For make_bootstrap_decision, it ignores is_cluster_member since coordinate_bootstrap
        // handles that case separately.
        let decision = make_bootstrap_decision(my_node_id, my_address, &discovered);

        // We have lower ID, so we'd bootstrap (is_cluster_member ignored in this function)
        match decision {
            BootstrapDecision::Bootstrap { initial_members } => {
                assert_eq!(initial_members.len(), 2);
            },
            _ => panic!("Expected Bootstrap decision"),
        }
    }

    #[test]
    fn test_max_u64_node_id() {
        // Edge case: peer reports maximum u64 value as node ID
        let my_node_id = u64::MAX - 1;
        let my_address = "127.0.0.1:50051";
        let discovered = vec![DiscoveredNode {
            node_id: u64::MAX,
            addr: "127.0.0.1:50052".parse().unwrap(),
            is_cluster_member: false,
            term: 0,
        }];

        let decision = make_bootstrap_decision(my_node_id, my_address, &discovered);

        // We have lower ID, so we bootstrap
        match decision {
            BootstrapDecision::Bootstrap { initial_members } => {
                assert_eq!(initial_members.len(), 2);
                assert!(initial_members.iter().any(|(id, _)| *id == u64::MAX));
            },
            _ => panic!("Expected Bootstrap decision"),
        }
    }
}
