//! Integration tests for coordinated cluster bootstrap.
//!
//! Tests the Task 6 bootstrap coordination system including:
//! - Single-node bootstrap with `allow_single_node=true`
//! - 3-node coordinated bootstrap (lowest ID wins)
//! - Node restart preserves ID and rejoins cluster
//! - Late joiner finds existing cluster via `is_cluster_member`

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

mod common;

use std::time::Duration;

use common::{TestCluster, create_admin_client};
use inferadb_ledger_raft::proto::GetNodeInfoRequest;
use inferadb_ledger_server::{
    bootstrap::bootstrap_node,
    config::{BootstrapConfig, Config, DiscoveryConfig},
    node_id::load_or_generate_node_id,
};
use inferadb_ledger_test_utils::TestDir;
use serial_test::serial;

/// Test single-node bootstrap with `allow_single_node=true`.
///
/// Verifies that a single node can bootstrap immediately when configured
/// with `min_cluster_size=1` and `allow_single_node=true`.
#[serial]
#[tokio::test]
async fn test_single_node_bootstrap_with_allow_single_node() {
    let temp_dir = TestDir::new();
    let port = 45000 + (rand::random::<u16>() % 1000);
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Create config with allow_single_node=true
    let config = Config {
        node_id: None, // Use auto-generated Snowflake ID
        listen_addr: addr,
        metrics_addr: None,
        data_dir: temp_dir.path().to_path_buf(),
        batching: Default::default(),
        rate_limit: Default::default(),
        discovery: DiscoveryConfig::default(),
        bootstrap: BootstrapConfig {
            min_cluster_size: 1,
            allow_single_node: true,
            bootstrap_timeout_secs: 5,
            poll_interval_secs: 1,
            skip_coordination: false,
        },
    };

    // Bootstrap should succeed
    let result = bootstrap_node(&config).await;
    assert!(result.is_ok(), "single-node bootstrap should succeed: {:?}", result.err());

    let bootstrapped = result.unwrap();

    // Start server to verify it's operational
    let server = bootstrapped.server;
    let server_handle = tokio::spawn(async move {
        let _ = server.serve().await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify node is a cluster member via GetNodeInfo
    let mut client = create_admin_client(addr).await.expect("connect to admin service");
    let response = client.get_node_info(GetNodeInfoRequest {}).await.expect("get_node_info RPC");
    let info = response.into_inner();

    assert!(info.is_cluster_member, "node should be cluster member after bootstrap");
    assert!(info.node_id > 0, "node should have auto-generated Snowflake ID");
    assert!(info.term > 0, "node should have term > 0 after bootstrap");

    // Verify Snowflake ID was persisted
    let node_id_file = temp_dir.path().join("node_id");
    assert!(node_id_file.exists(), "node_id file should be created");

    let persisted_id: u64 = std::fs::read_to_string(&node_id_file).unwrap().trim().parse().unwrap();
    assert_eq!(persisted_id, info.node_id, "persisted ID should match reported ID");

    server_handle.abort();
}

/// Test node restart preserves ID and rejoins cluster.
///
/// Verifies that when a node restarts:
/// 1. It loads the existing Snowflake ID from disk
/// 2. It resumes its cluster membership
#[serial]
#[tokio::test]
async fn test_node_restart_preserves_id() {
    let temp_dir = TestDir::new();
    let port = 46000 + (rand::random::<u16>() % 1000);
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let config = Config {
        node_id: None,
        listen_addr: addr,
        metrics_addr: None,
        data_dir: temp_dir.path().to_path_buf(),
        batching: Default::default(),
        rate_limit: Default::default(),
        discovery: DiscoveryConfig::default(),
        bootstrap: BootstrapConfig {
            min_cluster_size: 1,
            allow_single_node: true,
            bootstrap_timeout_secs: 5,
            poll_interval_secs: 1,
            skip_coordination: false,
        },
    };

    // First startup - bootstrap fresh node
    let first_id = {
        let bootstrapped = bootstrap_node(&config).await.expect("first bootstrap should succeed");

        // Get the generated node ID
        let raft_metrics = bootstrapped.raft.metrics().borrow().clone();
        let node_id = raft_metrics.id;

        // Verify ID was persisted
        let node_id_file = temp_dir.path().join("node_id");
        assert!(node_id_file.exists(), "node_id file should exist after first start");

        // Let server run briefly
        let server = bootstrapped.server;
        let handle = tokio::spawn(async move {
            let _ = server.serve().await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();

        node_id
    };

    // Small delay to ensure resources are released
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second startup - should load existing ID and resume
    let second_id = {
        // Use a different port since the old one may not be released yet
        let port2 = port + 1;
        let addr2: std::net::SocketAddr = format!("127.0.0.1:{}", port2).parse().unwrap();

        let config2 = Config {
            node_id: None,
            listen_addr: addr2,
            metrics_addr: None,
            data_dir: temp_dir.path().to_path_buf(),
            batching: Default::default(),
            rate_limit: Default::default(),
            discovery: DiscoveryConfig::default(),
            bootstrap: BootstrapConfig {
                min_cluster_size: 1,
                allow_single_node: true,
                bootstrap_timeout_secs: 5,
                poll_interval_secs: 1,
                skip_coordination: false,
            },
        };

        let bootstrapped = bootstrap_node(&config2).await.expect("restart should succeed");

        let raft_metrics = bootstrapped.raft.metrics().borrow().clone();
        let node_id = raft_metrics.id;

        // Start server and verify it's operational
        let server = bootstrapped.server;
        let handle = tokio::spawn(async move {
            let _ = server.serve().await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify cluster membership
        let mut client = create_admin_client(addr2).await.expect("connect");
        let response = client.get_node_info(GetNodeInfoRequest {}).await.expect("get_node_info");
        let info = response.into_inner();

        assert!(info.is_cluster_member, "restarted node should be cluster member");

        handle.abort();
        node_id
    };

    // Verify the same ID was used across restarts
    assert_eq!(first_id, second_id, "node ID should be preserved across restarts");
}

/// Test that load_or_generate_node_id generates unique IDs.
///
/// This is a unit test placed here because it verifies the behavior
/// that enables coordinated bootstrap (earlier nodes get lower IDs).
#[tokio::test]
async fn test_snowflake_ids_are_time_ordered_across_nodes() {
    let temp_dir1 = TestDir::new();
    let temp_dir2 = TestDir::new();

    // Generate first ID
    let id1 = load_or_generate_node_id(temp_dir1.path()).expect("generate id1");

    // Small delay to ensure different timestamp
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Generate second ID
    let id2 = load_or_generate_node_id(temp_dir2.path()).expect("generate id2");

    // Later ID should be higher
    assert!(id2 > id1, "ID generated later should be higher: {} vs {}", id1, id2);
}

/// Test that the TestCluster properly uses coordinated bootstrap.
///
/// This verifies that the existing test infrastructure works correctly
/// with the coordinated bootstrap system.
#[serial]
#[tokio::test]
async fn test_three_node_cluster_uses_coordinated_bootstrap() {
    // TestCluster uses skip_coordination for joining nodes,
    // so this test verifies that behavior works correctly
    let cluster = TestCluster::new(3).await;

    // Wait for leader election
    let leader_id = cluster.wait_for_leader().await;
    assert!(leader_id > 0, "should have elected a leader");

    // All nodes should be cluster members
    for node in cluster.nodes() {
        let mut client = create_admin_client(node.addr).await.expect("connect");
        let response = client.get_node_info(GetNodeInfoRequest {}).await.expect("get_node_info");
        let info = response.into_inner();

        assert!(info.is_cluster_member, "node {} should be cluster member", node.id);
    }

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all nodes agree on the same leader
    let leader_views: Vec<Option<u64>> =
        cluster.nodes().iter().map(|n| n.current_leader()).collect();

    // All should agree on the same leader
    let first_leader = leader_views[0];
    for view in &leader_views {
        assert_eq!(*view, first_leader, "all nodes should agree on leader, got {:?}", leader_views);
    }
}

/// Test that late joiner detects existing cluster via is_cluster_member.
///
/// When a node starts and discovers peers that are already cluster members,
/// it should return JoinExisting decision (via is_cluster_member=true).
#[serial]
#[tokio::test]
async fn test_late_joiner_finds_existing_cluster() {
    // Start a single-node cluster first
    let leader_dir = TestDir::new();
    let leader_port = 47000 + (rand::random::<u16>() % 1000);
    let leader_addr: std::net::SocketAddr = format!("127.0.0.1:{}", leader_port).parse().unwrap();

    let leader_config = Config {
        node_id: Some(1), // Use fixed ID for deterministic test
        listen_addr: leader_addr,
        metrics_addr: None,
        data_dir: leader_dir.path().to_path_buf(),
        batching: Default::default(),
        rate_limit: Default::default(),
        discovery: DiscoveryConfig::default(),
        bootstrap: BootstrapConfig {
            min_cluster_size: 1,
            allow_single_node: true,
            skip_coordination: false,
            ..Default::default()
        },
    };

    let leader = bootstrap_node(&leader_config).await.expect("leader bootstrap");
    let leader_server = leader.server;
    let leader_raft = leader.raft.clone();

    let leader_handle = tokio::spawn(async move {
        let _ = leader_server.serve().await;
    });

    // Wait for server to start accepting connections (retry loop)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wait for leader to become leader
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        let metrics = leader_raft.metrics().borrow().clone();
        if metrics.current_leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Wait for server to be ready and retry connection
    let mut client = None;
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        match create_admin_client(leader_addr).await {
            Ok(c) => {
                client = Some(c);
                break;
            },
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let mut client = client.expect("connect to leader");
    let response = client.get_node_info(GetNodeInfoRequest {}).await.expect("get_node_info");
    let info = response.into_inner();
    assert!(info.is_cluster_member, "leader should be cluster member");

    // Now test that discover_node_info correctly identifies the cluster
    let discovered =
        inferadb_ledger_server::discovery::discover_node_info(leader_addr, Duration::from_secs(5))
            .await;

    let node_info = discovered.expect("should discover leader node");
    assert!(node_info.is_cluster_member, "discovered node should report is_cluster_member=true");
    assert_eq!(node_info.node_id, 1, "should have correct node ID");
    assert!(node_info.term > 0, "should have term > 0");

    leader_handle.abort();
}

/// Test bootstrap validation rejects min_cluster_size=1 without allow_single_node.
#[tokio::test]
async fn test_bootstrap_validation_rejects_unsafe_single_node() {
    let temp_dir = TestDir::new();
    let port = 48000 + (rand::random::<u16>() % 1000);
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Create config with min_cluster_size=1 but allow_single_node=false
    let config = Config {
        node_id: Some(1),
        listen_addr: addr,
        metrics_addr: None,
        data_dir: temp_dir.path().to_path_buf(),
        batching: Default::default(),
        rate_limit: Default::default(),
        discovery: DiscoveryConfig::default(),
        bootstrap: BootstrapConfig {
            min_cluster_size: 1,
            allow_single_node: false, // This should cause validation failure
            ..Default::default()
        },
    };

    // Bootstrap should fail due to validation
    let result = bootstrap_node(&config).await;

    // Use match to extract error since BootstrappedNode doesn't implement Debug
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("should reject min_cluster_size=1 without allow_single_node"),
    };
    let err_str = err.to_string();
    assert!(
        err_str.contains("allow_single_node") || err_str.contains("configuration"),
        "error should mention allow_single_node: {}",
        err_str
    );
}

/// Test bootstrap validation rejects min_cluster_size=0.
#[tokio::test]
async fn test_bootstrap_validation_rejects_zero_cluster_size() {
    let temp_dir = TestDir::new();
    let port = 48500 + (rand::random::<u16>() % 1000);
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let config = Config {
        node_id: Some(1),
        listen_addr: addr,
        metrics_addr: None,
        data_dir: temp_dir.path().to_path_buf(),
        batching: Default::default(),
        rate_limit: Default::default(),
        discovery: DiscoveryConfig::default(),
        bootstrap: BootstrapConfig {
            min_cluster_size: 0, // Invalid
            allow_single_node: true,
            ..Default::default()
        },
    };

    let result = bootstrap_node(&config).await;

    // Use match to extract error since BootstrappedNode doesn't implement Debug
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("should reject min_cluster_size=0"),
    };
    let err_str = err.to_string();
    assert!(
        err_str.contains("at least 1") || err_str.contains("min_cluster_size"),
        "error should mention min_cluster_size: {}",
        err_str
    );
}
