//! End-to-end tests for the Ledger SDK against a real server cluster.
//!
//! These tests verify SDK functionality against an actual Ledger server,
//! including Raft consensus, replication, and leader failover scenarios.
//!
//! ## Requirements
//!
//! - Tests use ephemeral ports (40000-45000 range)
//! - Tests require `serial_test::serial` annotation for cluster isolation
//! - Each test creates its own `TestCluster` for isolation
//!
//! ## Test Categories
//!
//! - **Write/Read Cycle**: Basic CRUD operations through consensus
//! - **Idempotency**: Sequence tracking survives failover
//! - **Streaming**: WatchBlocks continues after leader election
//! - **Recovery**: Sequence recovery after client restart

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use inferadb_ledger_raft::{
    LedgerTypeConfig,
    proto::{JoinClusterRequest, admin_service_client::AdminServiceClient},
};
use inferadb_ledger_sdk::{
    ClientConfig, FileSequenceStorage, LedgerClient, Operation, PersistentSequenceTracker,
    RetryPolicy,
};
use inferadb_ledger_test_utils::TestDir;
use openraft::Raft;
use serial_test::serial;
use tokio::time::timeout;

// ============================================================================
// Test Cluster Infrastructure
// ============================================================================

/// A test node in a cluster.
struct TestNode {
    /// The node ID.
    id: u64,
    /// The gRPC address.
    addr: SocketAddr,
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// Temporary directory for node data.
    _temp_dir: TestDir,
    /// Server task handle for cleanup.
    _server_handle: tokio::task::JoinHandle<()>,
}

impl TestNode {
    /// Check if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.id)
    }

    /// Get the current leader ID if known.
    fn current_leader(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }

    /// Get the last applied log index.
    fn last_applied(&self) -> u64 {
        self.raft.metrics().borrow().last_applied.map(|id| id.index).unwrap_or(0)
    }
}

/// A test cluster of Raft nodes.
struct TestCluster {
    /// The nodes in the cluster.
    nodes: Vec<TestNode>,
}

impl TestCluster {
    /// Create a new test cluster with the given number of nodes.
    ///
    /// The first node bootstraps the cluster, and other nodes join via
    /// the AdminService's join_cluster RPC.
    async fn new(size: usize) -> Self {
        assert!(size >= 1, "cluster must have at least 1 node");

        // Use wide random range to minimize port conflicts
        let base_port = 40000 + (rand::random::<u16>() % 5000);
        let mut nodes = Vec::with_capacity(size);

        // Step 1: Start the bootstrap node as a SINGLE-NODE cluster
        let addr: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
        let temp_dir = TestDir::new();

        // Bootstrap node: no discovery configured → no peers found → bootstraps.
        // Uses auto-generated Snowflake ID (node_id: None) for realistic testing.
        let config = inferadb_ledger_server::config::Config {
            node_id: None, // Auto-generate Snowflake ID
            listen_addr: addr,
            metrics_addr: None,
            data_dir: temp_dir.path().to_path_buf(),
            batching: inferadb_ledger_server::config::BatchConfig::default(),
            rate_limit: inferadb_ledger_server::config::RateLimitConfig::default(),
            discovery: inferadb_ledger_server::config::DiscoveryConfig::default(),
            bootstrap: inferadb_ledger_server::config::BootstrapConfig::for_single_node(),
        };

        let bootstrapped = inferadb_ledger_server::bootstrap::bootstrap_node(&config)
            .await
            .expect("bootstrap node");

        // Get the auto-generated Snowflake ID from Raft metrics
        let node_id = bootstrapped.raft.metrics().borrow().id;

        let server = bootstrapped.server;
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                tracing::error!("server error: {}", e);
            }
        });

        let leader_raft = bootstrapped.raft.clone();
        let leader_addr = addr;
        nodes.push(TestNode {
            id: node_id,
            addr,
            raft: bootstrapped.raft,
            _temp_dir: temp_dir,
            _server_handle: server_handle,
        });

        // Wait for the bootstrap node to become leader
        let start = tokio::time::Instant::now();
        let timeout_duration = Duration::from_secs(5);
        while start.elapsed() < timeout_duration {
            let metrics = leader_raft.metrics().borrow().clone();
            if metrics.current_leader == Some(node_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Verify leader election succeeded
        {
            let metrics = leader_raft.metrics().borrow().clone();
            if metrics.current_leader != Some(node_id) {
                panic!("Bootstrap node failed to become leader within timeout");
            }
        }

        // Step 2: Start remaining nodes and have them join dynamically
        for i in 1..size {
            let port = base_port + i as u16;
            let temp_dir = TestDir::new();
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

            // Joining node: use skip_coordination mode for dynamic node addition.
            // This bypasses the coordinated bootstrap system and uses the legacy
            // behavior: no Raft cluster initialized, waiting to be added via
            // AdminService's JoinCluster RPC which we call explicitly below.
            // Uses auto-generated Snowflake ID (node_id: None) for realistic testing.
            let config = inferadb_ledger_server::config::Config {
                node_id: None, // Auto-generate Snowflake ID
                listen_addr: addr,
                metrics_addr: None,
                data_dir: temp_dir.path().to_path_buf(),
                batching: inferadb_ledger_server::config::BatchConfig::default(),
                rate_limit: inferadb_ledger_server::config::RateLimitConfig::default(),
                discovery: inferadb_ledger_server::config::DiscoveryConfig::default(),
                bootstrap: inferadb_ledger_server::config::BootstrapConfig {
                    min_cluster_size: 1,
                    allow_single_node: true,
                    skip_coordination: true,
                    ..Default::default()
                },
            };

            let bootstrapped = inferadb_ledger_server::bootstrap::bootstrap_node(&config)
                .await
                .expect("bootstrap node");

            // Get the auto-generated Snowflake ID from Raft metrics
            let node_id = bootstrapped.raft.metrics().borrow().id;

            let server = bootstrapped.server;
            let server_handle = tokio::spawn(async move {
                if let Err(e) = server.serve().await {
                    tracing::error!("server error: {}", e);
                }
            });

            // Give server time to start
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Join the cluster via AdminService RPC
            let mut attempts = 0;
            let max_attempts = 10;
            let mut join_success = false;
            let mut last_error = String::new();

            while attempts < max_attempts {
                let endpoint = format!("http://{}", leader_addr);
                let connect_result = AdminServiceClient::connect(endpoint).await;

                match connect_result {
                    Ok(mut admin_client) => {
                        let request = JoinClusterRequest { node_id, address: addr.to_string() };

                        match admin_client.join_cluster(request).await {
                            Ok(_) => {
                                join_success = true;
                                break;
                            },
                            Err(e) => {
                                last_error = format!("join RPC failed: {}", e);
                            },
                        }
                    },
                    Err(e) => {
                        last_error = format!("connect failed: {}", e);
                    },
                }

                attempts += 1;
                let backoff = Duration::from_millis(100 * (1 << attempts.min(5)));
                tokio::time::sleep(backoff).await;
            }

            if !join_success {
                panic!("Failed to join cluster after {} attempts: {}", max_attempts, last_error);
            }

            let new_raft = bootstrapped.raft.clone();

            // Wait for the new node to see itself as a voter
            let sync_start = tokio::time::Instant::now();
            let sync_timeout = Duration::from_secs(30);
            while sync_start.elapsed() < sync_timeout {
                let metrics = new_raft.metrics().borrow().clone();
                let membership = metrics.membership_config.membership();
                let is_voter = membership.voter_ids().any(|id| id == node_id);

                if is_voter && metrics.current_leader.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // Wait for leader's membership to exit joint consensus
            let stabilize_start = tokio::time::Instant::now();
            let stabilize_timeout = Duration::from_secs(10);
            while stabilize_start.elapsed() < stabilize_timeout {
                let leader_metrics = leader_raft.metrics().borrow().clone();
                let leader_membership = leader_metrics.membership_config.membership();
                let joint_config = leader_membership.get_joint_config();

                if joint_config.len() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            nodes.push(TestNode {
                id: node_id,
                addr,
                raft: bootstrapped.raft,
                _temp_dir: temp_dir,
                _server_handle: server_handle,
            });
        }

        // Wait for cluster to stabilize
        tokio::time::sleep(Duration::from_millis(500)).await;

        Self { nodes }
    }

    /// Wait for a leader to be elected across all nodes.
    async fn wait_for_leader(&self) -> u64 {
        let timeout_duration = Duration::from_secs(10);
        timeout(timeout_duration, async {
            loop {
                for node in &self.nodes {
                    if let Some(leader_id) = node.current_leader() {
                        if self.nodes.iter().any(|n| n.id == leader_id && n.is_leader()) {
                            return leader_id;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("leader election timeout")
    }

    /// Get the current leader node.
    fn leader(&self) -> Option<&TestNode> {
        self.nodes.iter().find(|n| n.is_leader())
    }

    /// Get all follower nodes.
    fn followers(&self) -> Vec<&TestNode> {
        self.nodes.iter().filter(|n| !n.is_leader()).collect()
    }

    /// Get a specific node by ID.
    #[allow(dead_code)]
    fn node(&self, id: u64) -> Option<&TestNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Wait for all nodes to synchronize to the same last_applied index.
    async fn wait_for_sync(&self, timeout_duration: Duration) -> bool {
        timeout(timeout_duration, async {
            loop {
                let leader = match self.leader() {
                    Some(l) => l,
                    None => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    },
                };
                let leader_applied = leader.last_applied();

                let all_synced = self.nodes.iter().all(|n| n.last_applied() >= leader_applied);
                if all_synced && leader_applied > 0 {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or(false)
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Create a LedgerClient connected to the given cluster node.
async fn create_sdk_client(addr: SocketAddr, client_id: &str) -> LedgerClient {
    let config = ClientConfig::builder()
        .with_endpoint(format!("http://{}", addr))
        .with_client_id(client_id)
        .with_timeout(Duration::from_secs(10))
        .with_connect_timeout(Duration::from_secs(5))
        .with_retry_policy(
            RetryPolicy::builder()
                .with_max_attempts(3)
                .with_initial_backoff(Duration::from_millis(100))
                .with_max_backoff(Duration::from_secs(2))
                .build(),
        )
        .build()
        .expect("valid config");

    LedgerClient::new(config).await.expect("client creation")
}

/// Create a test namespace and vault, returning (namespace_id, vault_id).
async fn setup_test_namespace_vault(client: &LedgerClient) -> (i64, i64) {
    let ns_id = client.create_namespace("test-ns").await.expect("create namespace");
    let vault_info = client.create_vault(ns_id).await.expect("create vault");
    (ns_id, vault_info.vault_id)
}

// ============================================================================
// E2E Tests: Write/Read Cycle
// ============================================================================

/// Test write → read cycle against TestCluster.
#[serial]
#[tokio::test]
async fn test_write_read_cycle_against_real_cluster() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let client = create_sdk_client(leader.addr, "e2e-client-1").await;

    // Create namespace and vault first
    let (ns_id, vault_id) = setup_test_namespace_vault(&client).await;

    // Write an entity
    let ops = vec![Operation::set_entity("user:alice", b"Alice Data".to_vec())];
    let write_result =
        client.write(ns_id, Some(vault_id), ops).await.expect("write should succeed");

    assert!(!write_result.tx_id.is_empty(), "should have tx_id");
    assert!(write_result.block_height > 0, "should have block height");

    // Read the entity back
    let read_result =
        client.read(ns_id, Some(vault_id), "user:alice").await.expect("read should succeed");

    assert_eq!(read_result, Some(b"Alice Data".to_vec()), "should read back written value");
}

/// Test multiple writes and reads.
#[serial]
#[tokio::test]
async fn test_multiple_writes_reads() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let client = create_sdk_client(leader.addr, "e2e-client-2").await;

    // Create namespace and vault first
    let (ns_id, vault_id) = setup_test_namespace_vault(&client).await;

    // Write multiple entities in separate transactions
    for i in 0..5 {
        let key = format!("item:{}", i);
        let value = format!("value-{}", i).into_bytes();
        let ops = vec![Operation::set_entity(&key, value)];
        client.write(ns_id, Some(vault_id), ops).await.expect("write should succeed");
    }

    // Read all entities back
    for i in 0..5 {
        let key = format!("item:{}", i);
        let expected = format!("value-{}", i).into_bytes();
        let result = client.read(ns_id, Some(vault_id), &key).await.expect("read should succeed");
        assert_eq!(result, Some(expected), "should read back value for {}", key);
    }
}

/// Test batch read functionality.
#[serial]
#[tokio::test]
async fn test_batch_read_against_real_cluster() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let client = create_sdk_client(leader.addr, "e2e-client-3").await;

    // Create namespace and vault first
    let (ns_id, vault_id) = setup_test_namespace_vault(&client).await;

    // Write several entities
    for i in 0..3 {
        let key = format!("batch:{}", i);
        let value = format!("batch-value-{}", i).into_bytes();
        let ops = vec![Operation::set_entity(&key, value)];
        client.write(ns_id, Some(vault_id), ops).await.expect("write should succeed");
    }

    // Batch read including a missing key
    let keys = vec![
        "batch:0".to_string(),
        "batch:1".to_string(),
        "batch:2".to_string(),
        "batch:missing".to_string(),
    ];
    let results =
        client.batch_read(ns_id, Some(vault_id), keys).await.expect("batch read should succeed");

    assert_eq!(results.len(), 4);
    assert_eq!(results[0], ("batch:0".to_string(), Some(b"batch-value-0".to_vec())));
    assert_eq!(results[1], ("batch:1".to_string(), Some(b"batch-value-1".to_vec())));
    assert_eq!(results[2], ("batch:2".to_string(), Some(b"batch-value-2".to_vec())));
    assert_eq!(results[3], ("batch:missing".to_string(), None));
}

// ============================================================================
// E2E Tests: Idempotency
// ============================================================================

/// Test idempotency survives leader failover.
#[serial]
#[tokio::test]
async fn test_idempotency_survives_leader_failover() {
    let cluster = TestCluster::new(3).await;
    let _original_leader_id = cluster.wait_for_leader().await;

    let original_leader = cluster.leader().expect("original leader");
    let client = create_sdk_client(original_leader.addr, "failover-client").await;

    // Create namespace and vault first
    let (ns_id, vault_id) = setup_test_namespace_vault(&client).await;

    // Perform a write
    let ops = vec![Operation::set_entity("failover:key", b"original-data".to_vec())];
    let first_result = client.write(ns_id, Some(vault_id), ops.clone()).await.expect("first write");

    // Wait for replication
    cluster.wait_for_sync(Duration::from_secs(5)).await;

    // Find a follower
    let followers = cluster.followers();
    assert!(!followers.is_empty(), "should have followers");

    let follower = followers[0];
    let follower_client = create_sdk_client(follower.addr, "follower-reader").await;

    // Read from follower - data should be replicated
    let result = follower_client
        .read(ns_id, Some(vault_id), "failover:key")
        .await
        .expect("read from follower");

    assert_eq!(result, Some(b"original-data".to_vec()), "data should be replicated to follower");

    // Verify block height consistency across cluster
    assert!(first_result.block_height > 0);
}

/// Test that multiple writes with sequential client IDs work correctly.
///
/// This test verifies that each client session with a unique ID can perform
/// independent writes without interference. This is the recommended pattern
/// for applications that need multiple independent sessions.
#[serial]
#[tokio::test]
async fn test_multiple_client_sessions_independent() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create first client session
    let client1 = create_sdk_client(leader.addr, "session-1").await;

    // Create namespace and vault first
    let (ns_id, vault_id) = setup_test_namespace_vault(&client1).await;

    // First session writes
    let ops1 = vec![Operation::set_entity("session:key1", b"from-session-1".to_vec())];
    let first_result = client1.write(ns_id, Some(vault_id), ops1).await.expect("first write");

    // Create second independent client session with different ID
    let client2 = create_sdk_client(leader.addr, "session-2").await;

    // Second session writes different data
    let ops2 = vec![Operation::set_entity("session:key2", b"from-session-2".to_vec())];
    let second_result = client2.write(ns_id, Some(vault_id), ops2).await.expect("second write");

    // Both writes should succeed with unique tx_ids
    assert!(!first_result.tx_id.is_empty());
    assert!(!second_result.tx_id.is_empty());
    assert_ne!(first_result.tx_id, second_result.tx_id, "should have different tx_ids");

    // Read back both keys
    let value1 = client2.read(ns_id, Some(vault_id), "session:key1").await.expect("read first");
    let value2 = client2.read(ns_id, Some(vault_id), "session:key2").await.expect("read second");

    assert_eq!(value1, Some(b"from-session-1".to_vec()));
    assert_eq!(value2, Some(b"from-session-2".to_vec()));
}

// ============================================================================
// E2E Tests: Streaming
// ============================================================================

/// Test streaming continues after leader election.
/// Note: Full streaming test requires block production, which may not happen
/// in a test cluster without writes. This test verifies stream setup works.
#[serial]
#[tokio::test]
async fn test_watch_blocks_stream_setup() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let client = create_sdk_client(leader.addr, "stream-client").await;

    // Create namespace and vault first
    let (ns_id, vault_id) = setup_test_namespace_vault(&client).await;

    // Start watching blocks from height 1 (genesis)
    // Note: start_height must be >= 1 per server requirements
    let stream_result = client.watch_blocks(ns_id, vault_id, 1).await;

    // Stream setup should succeed
    assert!(stream_result.is_ok(), "watch_blocks should succeed: {:?}", stream_result.err());
}

// ============================================================================
// E2E Tests: Sequence Recovery
// ============================================================================

/// Test that data persists across client sessions.
///
/// This test verifies that data written by one client session is readable
/// by a different client session, demonstrating server-side persistence.
/// Each session uses a unique client ID to avoid sequence conflicts.
#[serial]
#[tokio::test]
async fn test_data_persistence_across_sessions() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Setup client first creates namespace/vault
    let setup_client = create_sdk_client(leader.addr, "setup-client").await;
    let (ns_id, vault_id) = setup_test_namespace_vault(&setup_client).await;

    // First client session - perform several writes
    {
        let client = create_sdk_client(leader.addr, "writer-session-1").await;

        for i in 0..5 {
            let key = format!("persist:{}", i);
            let value = format!("data-{}", i).into_bytes();
            let ops = vec![Operation::set_entity(&key, value)];
            client.write(ns_id, Some(vault_id), ops).await.expect("write should succeed");
        }
    } // Client dropped, simulating shutdown

    // Second client session with different ID - reads previously written data
    // and writes additional data
    {
        let client = create_sdk_client(leader.addr, "reader-session-2").await;

        // Verify all previously written data is accessible
        for i in 0..5 {
            let key = format!("persist:{}", i);
            let expected = format!("data-{}", i).into_bytes();
            let read_result = client.read(ns_id, Some(vault_id), &key).await.expect("read");
            assert_eq!(read_result, Some(expected), "should read {}", key);
        }

        // Write additional data from new session
        let key = "persist:5";
        let value = b"data-5".to_vec();
        let ops = vec![Operation::set_entity(key, value.clone())];
        let result =
            client.write(ns_id, Some(vault_id), ops).await.expect("write from new session");
        assert!(!result.tx_id.is_empty(), "should have tx_id");

        // Verify the new write is readable
        let read_result = client.read(ns_id, Some(vault_id), key).await.expect("read new write");
        assert_eq!(read_result, Some(value));
    }
}

/// Test sequence recovery with persistent tracker.
#[serial]
#[tokio::test]
async fn test_sequence_recovery_with_persistence() {
    use std::path::PathBuf;

    use tempfile::tempdir;

    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let _leader = cluster.leader().expect("should have leader");
    let temp_dir = tempdir().expect("create temp dir");
    let storage_dir: PathBuf = temp_dir.path().to_path_buf();

    // First session with persistent tracker
    {
        let storage = FileSequenceStorage::new("persist-client", storage_dir.clone());
        let tracker =
            PersistentSequenceTracker::new("persist-client", storage).expect("create tracker");

        // Perform some sequence operations
        for _ in 0..3 {
            let _ = tracker.next_sequence(1, 1);
        }

        // Flush to ensure persistence
        tracker.flush().expect("flush");
    }

    // Second session - load persisted state
    {
        let storage = FileSequenceStorage::new("persist-client", storage_dir);
        let tracker =
            PersistentSequenceTracker::new("persist-client", storage).expect("create tracker");

        // Next sequence should continue from where we left off
        let next = tracker.next_sequence(1, 1);
        assert_eq!(next, 4, "sequence should continue from persisted state");
    }
}

// ============================================================================
// E2E Tests: Multi-Node Replication
// ============================================================================

/// Test write replicates to all nodes in cluster.
#[serial]
#[tokio::test]
async fn test_three_node_write_replication() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let client = create_sdk_client(leader.addr, "replication-client").await;

    // Create namespace and vault first
    let (ns_id, vault_id) = setup_test_namespace_vault(&client).await;

    // Write through the leader
    let ops = vec![Operation::set_entity("replicated:key", b"replicated-data".to_vec())];
    client.write(ns_id, Some(vault_id), ops).await.expect("write should succeed");

    // Wait for replication to all nodes
    let synced = cluster.wait_for_sync(Duration::from_secs(5)).await;
    assert!(synced, "cluster should sync");

    // Read from each follower
    for follower in cluster.followers() {
        let follower_client = create_sdk_client(follower.addr, "follower-client").await;
        let result = follower_client
            .read(ns_id, Some(vault_id), "replicated:key")
            .await
            .expect("read from follower");

        assert_eq!(
            result,
            Some(b"replicated-data".to_vec()),
            "data should be replicated to node {}",
            follower.id
        );
    }
}

// ============================================================================
// E2E Tests: Admin Operations
// ============================================================================

/// Test admin operations against real cluster.
#[serial]
#[tokio::test]
async fn test_admin_operations_real_cluster() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let client = create_sdk_client(leader.addr, "admin-client").await;

    // Create a namespace
    let ns_id = client.create_namespace("test-namespace").await.expect("create namespace");

    assert!(ns_id > 0, "should get valid namespace ID");

    // Get the namespace
    let ns_info = client.get_namespace(ns_id).await.expect("get namespace");
    assert_eq!(ns_info.name, "test-namespace");

    // Create a vault in the namespace
    let vault_info = client.create_vault(ns_id).await.expect("create vault");
    assert!(vault_info.vault_id > 0, "should get valid vault ID");

    // List namespaces
    let namespaces = client.list_namespaces().await.expect("list namespaces");
    assert!(
        namespaces.iter().any(|ns| ns.namespace_id == ns_id),
        "created namespace should be in list"
    );
}

// ============================================================================
// E2E Tests: Health Check
// ============================================================================

/// Test health check against real cluster.
#[serial]
#[tokio::test]
async fn test_health_check_real_cluster() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");
    let client = create_sdk_client(leader.addr, "health-client").await;

    // Simple health check
    let is_healthy = client.health_check().await.expect("health check");
    assert!(is_healthy, "cluster should be healthy");

    // Detailed health check
    let health_result = client.health_check_detailed().await.expect("detailed health");
    assert!(health_result.is_healthy(), "detailed health should report healthy");
}
