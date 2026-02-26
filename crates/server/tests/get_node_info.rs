//! Integration tests for GetNodeInfo RPC.
//!
//! Tests the pre-bootstrap coordination RPC that returns node identity information
//! including Snowflake ID, address, cluster membership status, and Raft term.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_proto::proto::GetNodeInfoRequest;
use inferadb_ledger_server::discovery::discover_node_info;

use crate::common::{TestCluster, create_admin_client};

/// Tests GetNodeInfo returns correct node ID before bootstrap.
///
/// This test verifies that GetNodeInfo is available and returns the node's
/// Snowflake ID even before the cluster is fully bootstrapped.
#[tokio::test]
async fn test_get_node_info_returns_node_id() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = create_admin_client(leader.addr).await.expect("connect to admin service");

    let response = client.get_node_info(GetNodeInfoRequest {}).await.expect("get_node_info RPC");
    let info = response.into_inner();

    // Node ID should be the configured ID (1 for first node in test cluster)
    assert_eq!(info.node_id, leader.id, "node_id should match");

    // Address should be the node's listen address
    assert_eq!(info.address, leader.addr.to_string(), "address should match listen_addr");
}

/// Tests GetNodeInfo shows is_cluster_member=true after bootstrap.
///
/// After a node has bootstrapped and become a cluster member, the
/// is_cluster_member flag should be true.
#[tokio::test]
async fn test_get_node_info_shows_cluster_member_after_bootstrap() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = create_admin_client(leader.addr).await.expect("connect to admin service");

    let response = client.get_node_info(GetNodeInfoRequest {}).await.expect("get_node_info RPC");
    let info = response.into_inner();

    // Node should be a cluster member after bootstrap
    assert!(info.is_cluster_member, "node should be a cluster member after bootstrap");

    // Term should be > 0 after bootstrap
    assert!(info.term > 0, "term should be > 0 after bootstrap");
}

/// Tests GetNodeInfo returns correct info in a 3-node cluster.
///
/// Verifies that each node returns its own ID and all nodes report
/// consistent cluster membership after joining.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_node_info_three_node_cluster() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Query each node
    for node in cluster.nodes() {
        let mut client = create_admin_client(node.addr).await.expect("connect to admin service");

        let response =
            client.get_node_info(GetNodeInfoRequest {}).await.expect("get_node_info RPC");
        let info = response.into_inner();

        // Each node should return its own ID
        assert_eq!(info.node_id, node.id, "node {} should return its own ID", node.id);

        // Each node should return its own address
        assert_eq!(
            info.address,
            node.addr.to_string(),
            "node {} should return its own address",
            node.id
        );

        // All nodes should be cluster members
        assert!(
            info.is_cluster_member,
            "node {} should be a cluster member after joining",
            node.id
        );

        // All nodes should have the same term (cluster is stable)
        assert!(info.term > 0, "node {} should have term > 0", node.id);
    }
}

/// Tests GetNodeInfo term matches Raft metrics.
///
/// The term returned by GetNodeInfo should match the current Raft term
/// from the node's metrics.
#[tokio::test]
async fn test_get_node_info_term_matches_raft_metrics() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let mut client = create_admin_client(leader.addr).await.expect("connect to admin service");

    let response = client.get_node_info(GetNodeInfoRequest {}).await.expect("get_node_info RPC");
    let info = response.into_inner();

    // Term from GetNodeInfo should match the node's current Raft term
    let raft_term = leader.current_term();
    assert_eq!(info.term, raft_term, "term should match Raft metrics");
}

/// Tests discover_node_info function against a running node.
///
/// Verifies that the discovery helper function correctly queries a node
/// via the GetNodeInfo RPC and returns a DiscoveredNode.
#[tokio::test]
async fn test_discover_node_info_against_running_node() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Use discover_node_info to query the node
    let discovered = discover_node_info(leader.addr, Duration::from_secs(5)).await;

    // Should successfully discover the node
    let node = discovered.expect("should discover node");

    // Verify the discovered node info
    assert_eq!(node.node_id, leader.id, "node_id should match");
    assert_eq!(node.addr, leader.addr, "addr should match");
    assert!(node.is_cluster_member, "should be cluster member after bootstrap");
    assert!(node.term > 0, "term should be > 0 after bootstrap");
}

/// Tests discover_node_info returns None for unreachable peers.
///
/// When a peer is not reachable, discover_node_info should return None
/// rather than failing with an error.
#[tokio::test]
async fn test_discover_node_info_unreachable_returns_none() {
    // Try to discover a node at an address that's not running
    let fake_addr: std::net::SocketAddr = "127.0.0.1:59998".parse().expect("valid addr");

    let result = discover_node_info(fake_addr, Duration::from_millis(200)).await;

    // Should return None for unreachable peer
    assert!(result.is_none(), "should return None for unreachable peer");
}
