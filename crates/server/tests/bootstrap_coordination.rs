//! Integration tests for cluster bootstrap.
//!
//! Tests the bootstrap system including:
//! - Single-node bootstrap with cluster_id pre-written
//! - 3-node cluster via dynamic join
//! - Node restart preserves ID and rejoins cluster
//! - Late joiner finds existing cluster via `is_cluster_member`
//! - Fresh node (no cluster_id) waits for initialization

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_proto::proto::GetNodeInfoRequest;
use inferadb_ledger_server::{
    bootstrap::bootstrap_node, config::Config, node_id::load_or_generate_node_id,
};
use inferadb_ledger_test_utils::TestDir;
use serial_test::serial;

use crate::common::{TestCluster, create_admin_client};

/// Tests single-node bootstrap with pre-written cluster_id.
///
/// Verifies that a single node can bootstrap immediately when a cluster_id
/// file exists (restart path).
#[tokio::test]
async fn test_single_node_bootstrap() {
    let socket_dir = TestDir::new();
    let socket_path = socket_dir.path().join("bootstrap-single.sock");
    let temp_dir = TestDir::new();
    let data_dir = temp_dir.path().to_path_buf();

    // Write cluster_id so bootstrap_node takes the restart path (immediate startup)
    inferadb_ledger_server::cluster_id::write_cluster_id(&data_dir, 1).expect("write cluster_id");

    let config = Config {
        socket: Some(socket_path.clone()),
        metrics_addr: None,
        data_dir: Some(data_dir.clone()),
        ..Config::default()
    };

    // Bootstrap should succeed
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let result = bootstrap_node(
        &config,
        &data_dir,
        inferadb_ledger_raft::HealthState::new(),
        shutdown_rx,
        None,
    )
    .await;
    assert!(result.is_ok(), "single-node bootstrap should succeed: {:?}", result.err());

    let bootstrapped = result.unwrap();
    let addr = socket_path.to_string_lossy().to_string();

    // Server is already running and accepting connections from bootstrap_node()

    // Verify node is a cluster member via GetNodeInfo
    let mut client = create_admin_client(&addr).await.expect("connect to admin service");
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

    bootstrapped.server_handle.abort();
}

/// Tests node restart preserves ID and rejoins cluster.
///
/// Verifies that when a node restarts:
/// 1. It loads the existing Snowflake ID from disk
/// 2. It resumes its cluster membership
#[tokio::test]
async fn test_node_restart_preserves_id() {
    let socket_dir = TestDir::new();
    let socket_path = socket_dir.path().join("restart-test.sock");
    let temp_dir = TestDir::new();
    let data_dir = temp_dir.path().to_path_buf();

    // Write cluster_id so bootstrap_node takes the restart path
    inferadb_ledger_server::cluster_id::write_cluster_id(&data_dir, 1).expect("write cluster_id");

    let config = Config {
        socket: Some(socket_path.clone()),
        metrics_addr: None,
        data_dir: Some(data_dir.clone()),
        ..Config::default()
    };

    // First startup - bootstrap fresh node
    let first_id = {
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let bootstrapped = bootstrap_node(
            &config,
            &data_dir,
            inferadb_ledger_raft::HealthState::new(),
            shutdown_rx,
            None,
        )
        .await
        .expect("first bootstrap should succeed");

        // Get the generated node ID
        let node_id = bootstrapped.handle.node_id();

        // Verify ID was persisted
        let node_id_file = temp_dir.path().join("node_id");
        assert!(node_id_file.exists(), "node_id file should exist after first start");

        // Server is already running from bootstrap_node(), let it run briefly
        tokio::time::sleep(Duration::from_millis(200)).await;
        bootstrapped.server_handle.abort();

        node_id
    };

    // Small delay to ensure resources are released
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second startup - should load existing ID and resume
    let second_id = {
        // Use a different socket path since the old one may still be in use
        let socket_path2 = socket_dir.path().join("restart-test-2.sock");

        let config2 = Config {
            socket: Some(socket_path2.clone()),
            metrics_addr: None,
            data_dir: Some(data_dir.clone()),
            ..Config::default()
        };
        let addr2 = socket_path2.to_string_lossy().to_string();

        let (_shutdown_tx2, shutdown_rx2) = tokio::sync::watch::channel(false);
        let bootstrapped = bootstrap_node(
            &config2,
            &data_dir,
            inferadb_ledger_raft::HealthState::new(),
            shutdown_rx2,
            None,
        )
        .await
        .expect("restart should succeed");

        let node_id = bootstrapped.handle.node_id();

        // Server is already running and accepting TCP connections from bootstrap_node()

        // Verify cluster membership
        let mut client = create_admin_client(&addr2).await.expect("connect");
        let response = client.get_node_info(GetNodeInfoRequest {}).await.expect("get_node_info");
        let info = response.into_inner();

        assert!(info.is_cluster_member, "restarted node should be cluster member");

        bootstrapped.server_handle.abort();
        node_id
    };

    // Verify the same ID was used across restarts
    assert_eq!(first_id, second_id, "node ID should be preserved across restarts");
}

/// Tests that load_or_generate_node_id generates unique IDs.
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

/// Tests that the TestCluster properly uses coordinated bootstrap.
///
/// This verifies that the existing test infrastructure works correctly
/// with the coordinated bootstrap system.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_three_node_cluster_uses_coordinated_bootstrap() {
    // TestCluster uses single-node mode for the first node and dynamic
    // join for subsequent nodes via AdminService
    let cluster = TestCluster::new(3).await;

    // Wait for leader election
    let leader_id = cluster.wait_for_leader().await;
    assert!(leader_id > 0, "should have elected a leader");

    // All nodes should be cluster members
    for node in cluster.nodes() {
        let mut client = create_admin_client(&node.addr).await.expect("connect");
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

/// Tests that late joiner detects existing cluster via is_cluster_member.
///
/// When a node starts and discovers peers that are already cluster members,
/// it should return JoinExisting decision (via is_cluster_member=true).
#[tokio::test]
async fn test_late_joiner_finds_existing_cluster() {
    // This test uses discover_node_info which requires a TCP SocketAddr,
    // so we keep TCP (no socket) for this specific test.
    let leader_dir = TestDir::new();
    let leader_data_dir = leader_dir.path().to_path_buf();
    // Bind TCP on port 0 to get an ephemeral port
    let tcp_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let leader_addr: std::net::SocketAddr = tcp_listener.local_addr().expect("local addr");
    drop(tcp_listener);

    // Pre-write node_id for deterministic test behavior
    std::fs::write(leader_dir.path().join("node_id"), "1").expect("write node_id");

    // Write cluster_id so bootstrap_node takes the restart path
    inferadb_ledger_server::cluster_id::write_cluster_id(&leader_data_dir, 1)
        .expect("write cluster_id");

    let leader_config = Config {
        listen: Some(leader_addr),
        metrics_addr: None,
        data_dir: Some(leader_data_dir.clone()),
        ..Config::default()
    };
    let leader_addr_str = leader_addr.to_string();

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let leader = bootstrap_node(
        &leader_config,
        &leader_data_dir,
        inferadb_ledger_raft::HealthState::new(),
        shutdown_rx,
        None,
    )
    .await
    .expect("leader bootstrap");
    let leader_handle = leader.handle.clone();

    // Server is already running and accepting TCP connections from bootstrap_node()

    // Wait for leader to become leader
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if leader_handle.current_leader() == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait for server to be ready by retrying the RPC (lazy channels always
    // succeed on creation, so we must probe with an actual request).
    let mut response = None;
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        let mut client = create_admin_client(&leader_addr_str).await.unwrap();
        match client.get_node_info(GetNodeInfoRequest {}).await {
            Ok(r) => {
                response = Some(r);
                break;
            },
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let info = response.expect("get_node_info RPC").into_inner();
    assert!(info.is_cluster_member, "leader should be cluster member");

    // Now test that discover_node_info correctly identifies the cluster
    let discovered =
        inferadb_ledger_server::discovery::discover_node_info(leader_addr, Duration::from_secs(5))
            .await;

    let node_info = discovered.expect("should discover leader node");
    assert!(node_info.is_cluster_member, "discovered node should report is_cluster_member=true");
    assert_eq!(node_info.node_id, 1, "should have correct node ID");
    assert!(node_info.term > 0, "should have term > 0");

    leader.server_handle.abort();
}

/// Tests that a fresh node (no cluster_id) waits for initialization.
///
/// Verifies that a node without a cluster_id file starts its gRPC server
/// but does not initialize a Raft cluster — it waits for InitCluster RPC.
#[tokio::test]
async fn test_fresh_node_waits_for_init() {
    let socket_dir = TestDir::new();
    let socket_path = socket_dir.path().join("fresh-node.sock");
    let temp_dir = TestDir::new();
    let data_dir = temp_dir.path().to_path_buf();
    let addr = socket_path.to_string_lossy().to_string();

    // No cluster_id written — node enters "fresh" path and blocks waiting
    // for InitCluster RPC. The gRPC server is running during this wait.
    let config = Config {
        socket: Some(socket_path),
        metrics_addr: None,
        data_dir: Some(data_dir.clone()),
        ..Config::default()
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn bootstrap_node in background — it blocks on the fresh path.
    let bootstrap_handle = tokio::spawn(async move {
        bootstrap_node(
            &config,
            &data_dir,
            inferadb_ledger_raft::HealthState::new(),
            shutdown_rx,
            None,
        )
        .await
    });

    // Wait for the gRPC server to be reachable via a probe RPC.
    let start = tokio::time::Instant::now();
    let mut response = None;
    while start.elapsed() < Duration::from_secs(10) {
        let mut client = create_admin_client(&addr).await.unwrap();
        match client.get_node_info(GetNodeInfoRequest {}).await {
            Ok(r) => {
                response = Some(r);
                break;
            },
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let response = response.expect("get_node_info RPC");
    let info = response.into_inner();

    assert!(!info.is_cluster_member, "fresh node should NOT be cluster member");

    // Send shutdown to unblock the fresh path
    let _ = shutdown_tx.send(true);

    // Wait for bootstrap_node to finish (it returns after shutdown signal).
    // On the fresh path, shutdown before InitCluster returns an error — that's
    // expected behavior, not a failure. The key assertion above (is_cluster_member
    // == false) already validated the test invariant.
    let result = tokio::time::timeout(Duration::from_secs(5), bootstrap_handle)
        .await
        .expect("bootstrap should finish after shutdown")
        .expect("bootstrap task should not panic");

    match result {
        Ok(bootstrapped) => bootstrapped.server_handle.abort(),
        Err(_) => {
            // Fresh node received shutdown before initialization — expected.
        },
    }
}
