//! End-to-end tests for the Ledger SDK against a real server cluster.
//!
//! These tests run against an external Ledger cluster started by
//! `scripts/run-sdk-integration-tests.sh`. The cluster endpoints are
//! provided via the `LEDGER_ENDPOINTS` environment variable.
//!
//! When `LEDGER_ENDPOINTS` is not set, all tests skip gracefully — this
//! allows `cargo test --workspace` to pass without a running cluster.
//!
//! ## Test Categories
//!
//! - **Write/Read Cycle**: Basic CRUD operations through consensus
//! - **Replication**: Write to leader, read from followers
//! - **Streaming**: WatchBlocks stream setup
//! - **Admin**: Namespace/vault lifecycle
//! - **Health**: Health check endpoints

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_raft::proto::{
    GetClusterInfoRequest, admin_service_client::AdminServiceClient,
};
use inferadb_ledger_sdk::{ClientConfig, LedgerClient, Operation, RetryPolicy, ServerSource};

// ============================================================================
// External Cluster Helpers
// ============================================================================

/// Read `LEDGER_ENDPOINTS` env var. Returns `None` if not set.
fn require_external_cluster() -> Option<Vec<String>> {
    let raw = std::env::var("LEDGER_ENDPOINTS").ok()?;
    let endpoints: Vec<String> =
        raw.split(',').map(|e| e.trim().to_string()).filter(|e| !e.is_empty()).collect();

    if endpoints.is_empty() {
        return None;
    }

    Some(endpoints)
}

/// Skip macro: returns early if no external cluster is available.
macro_rules! require_cluster {
    () => {
        match require_external_cluster() {
            Some(eps) => eps,
            None => {
                eprintln!(
                    "LEDGER_ENDPOINTS not set — skipping SDK e2e test. \
                     Run via scripts/run-sdk-integration-tests.sh"
                );
                return;
            },
        }
    };
}

/// Create a `LedgerClient` connected to all cluster endpoints.
async fn create_sdk_client(endpoints: &[String], client_id: &str) -> LedgerClient {
    let config = ClientConfig::builder()
        .servers(ServerSource::from_static(endpoints.iter().cloned()))
        .client_id(client_id)
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .initial_backoff(Duration::from_millis(100))
                .max_backoff(Duration::from_secs(2))
                .build(),
        )
        .build()
        .expect("valid config");

    LedgerClient::new(config).await.expect("client creation")
}

/// Create a `LedgerClient` connected to a single specific endpoint.
async fn create_single_endpoint_client(endpoint: &str, client_id: &str) -> LedgerClient {
    let config = ClientConfig::builder()
        .servers(ServerSource::from_static([endpoint.to_string()]))
        .client_id(client_id)
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .initial_backoff(Duration::from_millis(100))
                .max_backoff(Duration::from_secs(2))
                .build(),
        )
        .build()
        .expect("valid config");

    LedgerClient::new(config).await.expect("client creation")
}

/// Create a test namespace and vault, returning (namespace_id, vault_id).
async fn setup_test_namespace_vault(client: &LedgerClient) -> (i64, i64) {
    let ns_name = format!("test-ns-{}", uuid::Uuid::new_v4());
    let ns_id = client.create_namespace(&ns_name).await.expect("create namespace");
    let vault_info = client.create_vault(ns_id).await.expect("create vault");
    (ns_id, vault_info.vault_id)
}

/// Find the leader endpoint via `GetClusterInfo`.
async fn find_leader_endpoint(endpoints: &[String]) -> String {
    for endpoint in endpoints {
        let mut client =
            AdminServiceClient::connect(endpoint.clone()).await.expect("connect to admin");

        let response =
            client.get_cluster_info(GetClusterInfoRequest {}).await.expect("get cluster info");

        let info = response.into_inner();
        if info.leader_id > 0
            && let Some(leader) = info.members.iter().find(|m| m.is_leader)
            && let Some(ep) = endpoints
                .iter()
                .find(|ep| ep.ends_with(&leader.address) || ep.contains(&leader.address))
        {
            return ep.clone();
        }
    }
    panic!("no leader found in cluster");
}

/// Find non-leader endpoints via `GetClusterInfo`.
async fn find_non_leader_endpoints(endpoints: &[String]) -> Vec<String> {
    for endpoint in endpoints {
        if let Ok(mut client) = AdminServiceClient::connect(endpoint.clone()).await
            && let Ok(response) = client.get_cluster_info(GetClusterInfoRequest {}).await
        {
            let info = response.into_inner();
            let leader_addr = info
                .members
                .iter()
                .find(|m| m.is_leader)
                .map(|m| m.address.clone())
                .unwrap_or_default();

            return endpoints
                .iter()
                .filter(|ep| !ep.ends_with(&leader_addr) && !ep.contains(&leader_addr))
                .cloned()
                .collect();
        }
    }
    vec![]
}

// ============================================================================
// E2E Tests: Write/Read Cycle
// ============================================================================

/// Test write → read cycle.
#[tokio::test]
async fn test_write_read_cycle() {
    let endpoints = require_cluster!();

    let client = create_sdk_client(&endpoints, "e2e-write-read").await;
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
#[tokio::test]
async fn test_multiple_writes_reads() {
    let endpoints = require_cluster!();

    let client = create_sdk_client(&endpoints, "e2e-multi-rw").await;
    let (ns_id, vault_id) = setup_test_namespace_vault(&client).await;

    // Write multiple entities
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
#[tokio::test]
async fn test_batch_read() {
    let endpoints = require_cluster!();

    let client = create_sdk_client(&endpoints, "e2e-batch-read").await;
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
// E2E Tests: Replication
// ============================================================================

/// Test that writes to the leader replicate to followers.
///
/// Writes via a leader-connected client, then reads from each non-leader
/// endpoint to verify Raft replication is working.
#[tokio::test]
async fn test_write_replication_to_followers() {
    let endpoints = require_cluster!();

    if endpoints.len() < 3 {
        eprintln!("Skipping replication test: need >= 3 nodes, got {}", endpoints.len());
        return;
    }

    let leader_ep = find_leader_endpoint(&endpoints).await;
    let leader_client = create_single_endpoint_client(&leader_ep, "repl-leader").await;

    let (ns_id, vault_id) = setup_test_namespace_vault(&leader_client).await;

    // Write through the leader
    let ops = vec![Operation::set_entity("repl:key", b"replicated-data".to_vec())];
    let result =
        leader_client.write(ns_id, Some(vault_id), ops).await.expect("write should succeed");
    assert!(result.block_height > 0);

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Read from each non-leader endpoint
    let non_leaders = find_non_leader_endpoints(&endpoints).await;
    assert!(!non_leaders.is_empty(), "should have non-leader endpoints");

    for (i, ep) in non_leaders.iter().enumerate() {
        let follower_client =
            create_single_endpoint_client(ep, &format!("repl-follower-{}", i)).await;

        let read_result = follower_client
            .read(ns_id, Some(vault_id), "repl:key")
            .await
            .expect("read from follower should succeed");

        assert_eq!(
            read_result,
            Some(b"replicated-data".to_vec()),
            "data should be replicated to follower at {}",
            ep
        );
    }
}

/// Test write replicates to all nodes.
#[tokio::test]
async fn test_three_node_write_replication() {
    let endpoints = require_cluster!();

    if endpoints.len() < 3 {
        eprintln!("Skipping 3-node test: need >= 3 nodes, got {}", endpoints.len());
        return;
    }

    let leader_ep = find_leader_endpoint(&endpoints).await;
    let client = create_single_endpoint_client(&leader_ep, "repl-3node").await;

    let (ns_id, vault_id) = setup_test_namespace_vault(&client).await;

    // Write through the leader
    let ops = vec![Operation::set_entity("replicated:key", b"replicated-data".to_vec())];
    client.write(ns_id, Some(vault_id), ops).await.expect("write should succeed");

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Read from every endpoint (including leader)
    for (i, ep) in endpoints.iter().enumerate() {
        let reader = create_single_endpoint_client(ep, &format!("repl-reader-{}", i)).await;

        let result =
            reader.read(ns_id, Some(vault_id), "replicated:key").await.expect("read from node");

        assert_eq!(
            result,
            Some(b"replicated-data".to_vec()),
            "data should be replicated to node at {}",
            ep
        );
    }
}

// ============================================================================
// E2E Tests: Multiple Sessions
// ============================================================================

/// Test multiple client sessions with independent IDs.
#[tokio::test]
async fn test_multiple_client_sessions() {
    let endpoints = require_cluster!();

    let client1 = create_sdk_client(&endpoints, "session-1").await;
    let (ns_id, vault_id) = setup_test_namespace_vault(&client1).await;

    // First session writes
    let ops1 = vec![Operation::set_entity("session:key1", b"from-session-1".to_vec())];
    let first_result = client1.write(ns_id, Some(vault_id), ops1).await.expect("first write");

    // Second independent session
    let client2 = create_sdk_client(&endpoints, "session-2").await;

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

/// Test watch_blocks stream setup.
#[tokio::test]
async fn test_watch_blocks_stream_setup() {
    let endpoints = require_cluster!();

    let client = create_sdk_client(&endpoints, "stream-client").await;
    let (ns_id, vault_id) = setup_test_namespace_vault(&client).await;

    // Start watching blocks from height 1 (genesis)
    let stream_result = client.watch_blocks(ns_id, vault_id, 1).await;

    assert!(stream_result.is_ok(), "watch_blocks should succeed: {:?}", stream_result.err());
}

// ============================================================================
// E2E Tests: Persistence
// ============================================================================

/// Test that data persists across client sessions.
#[tokio::test]
async fn test_data_persistence_across_sessions() {
    let endpoints = require_cluster!();

    // Setup: create namespace/vault
    let setup_client = create_sdk_client(&endpoints, "setup-client").await;
    let (ns_id, vault_id) = setup_test_namespace_vault(&setup_client).await;

    // First client session — write data
    {
        let client = create_sdk_client(&endpoints, "writer-session-1").await;

        for i in 0..5 {
            let key = format!("persist:{}", i);
            let value = format!("data-{}", i).into_bytes();
            let ops = vec![Operation::set_entity(&key, value)];
            client.write(ns_id, Some(vault_id), ops).await.expect("write should succeed");
        }
    } // Client dropped

    // Second client session — read previously written data
    {
        let client = create_sdk_client(&endpoints, "reader-session-2").await;

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

        let read_result = client.read(ns_id, Some(vault_id), key).await.expect("read new write");
        assert_eq!(read_result, Some(value));
    }
}

// ============================================================================
// E2E Tests: Admin Operations
// ============================================================================

/// Test admin operations (namespace/vault lifecycle).
#[tokio::test]
async fn test_admin_operations() {
    let endpoints = require_cluster!();

    let client = create_sdk_client(&endpoints, "admin-client").await;

    // Create a namespace
    let ns_name = format!("test-admin-{}", uuid::Uuid::new_v4());
    let ns_id = client.create_namespace(&ns_name).await.expect("create namespace");
    assert!(ns_id > 0, "should get valid namespace ID");

    // Get the namespace
    let ns_info = client.get_namespace(ns_id).await.expect("get namespace");
    assert_eq!(ns_info.name, ns_name);

    // Create a vault
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

/// Test health check endpoints.
#[tokio::test]
async fn test_health_check() {
    let endpoints = require_cluster!();

    let client = create_sdk_client(&endpoints, "health-client").await;

    // Simple health check
    let is_healthy = client.health_check().await.expect("health check");
    assert!(is_healthy, "cluster should be healthy");

    // Detailed health check
    let health_result = client.health_check_detailed().await.expect("detailed health");
    assert!(health_result.is_healthy(), "detailed health should report healthy");
}
