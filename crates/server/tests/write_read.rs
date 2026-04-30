//! Write/read integration tests.
//!
//! Tests single-node and multi-node write/read cycles through Raft consensus.
//!
//! F.1.f.2.Stage1e Wave 3: migrated from the legacy tonic helpers
//! (`create_read_client` / `create_write_client` /
//! `create_test_organization` / `create_test_vault`) to their
//! wire-protocol siblings (`wire_read_client` / `wire_write_client` /
//! `wire_create_test_organization` / `wire_create_test_vault`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

use std::time::Duration;

use bytes::Bytes;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use inferadb_ledger_wire::services::{read as wr, shared as ws, write as ww};

use crate::common::{
    TestCluster, resolve_org_id, wait_for_vault_group_live_on_all_voters,
    wire_create_test_organization, wire_create_test_vault, wire_read_client, wire_write_client,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization and returns its slug.
async fn create_organization(
    cluster: &TestCluster,
    node_id: u64,
    name: &str,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let (slug, _admin) = wire_create_test_organization(cluster, node_id, name).await?;
    Ok(slug)
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    wire_create_test_vault(cluster, node_id, organization).await
}

// ============================================================================
// Write/Read Tests
// ============================================================================

/// Tests single-node write and read cycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_single_node_write_read() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "write-read-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Create a write client
    let client = wire_write_client(&cluster, leader.id);

    // Submit a write request using SetEntity operation
    let request = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "test-client".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "test-key".to_string(),
                value: Bytes::from_static(b"test-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request, rand::random::<u128>()).await;
    assert!(response.is_ok(), "write should succeed: {:?}", response.err());

    let response = response.unwrap();
    match response.result {
        Some(ww::WriteResponseResult::Success(success)) => {
            assert!(success.tx_id.is_some(), "should have tx_id");
            assert!(success.block_height > 0, "should have non-zero block height");
        },
        Some(ww::WriteResponseResult::Error(err)) => {
            panic!("write failed: {:?}", err);
        },
        None => {
            panic!("no result in response");
        },
    }
}

/// Tests write idempotency - same client_id + idempotency_key should return cached result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_write_idempotency() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "idempotency-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    let client = wire_write_client(&cluster, leader.id);

    // Use a fixed idempotency key for both writes to test deduplication
    let idempotency_key = Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes());

    let request = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "idempotent-client".to_string() }),
        idempotency_key: idempotency_key.clone(),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "idempotent-key".to_string(),
                value: Bytes::from_static(b"idempotent-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    // First write
    let response1 = client
        .write(request.clone(), rand::random::<u128>())
        .await
        .expect("first write should succeed");

    // Second write with same client_id + idempotency_key
    let response2 =
        client.write(request, rand::random::<u128>()).await.expect("second write should succeed");

    // Both should return the same result
    match (response1.result, response2.result) {
        (
            Some(ww::WriteResponseResult::Success(s1)),
            Some(ww::WriteResponseResult::Success(s2)),
        ) => {
            assert_eq!(s1.tx_id, s2.tx_id, "tx_id should match");
            assert_eq!(s1.block_height, s2.block_height, "block_height should match");
        },
        _ => panic!("both writes should succeed"),
    }
}

/// Tests that writes create blocks that can be retrieved via GetBlock.
///
/// State root is computed after applying transactions.
/// GetBlock returns stored block with header and transactions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_write_creates_retrievable_block() {
    let cluster = TestCluster::with_wire_transport(1).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "block-test-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Create write and read clients
    let write_client = wire_write_client(&cluster, leader.id);
    let read_client = wire_read_client(&cluster, leader.id);

    // Submit a write
    let request = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "block-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "block-key".to_string(),
                value: Bytes::from_static(b"block-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response =
        write_client.write(request, rand::random::<u128>()).await.expect("write should succeed");

    let block_height = match response.result {
        Some(ww::WriteResponseResult::Success(s)) => s.block_height,
        _ => panic!("write should succeed"),
    };

    assert!(block_height > 0, "block_height should be > 0");

    // Retrieve the block via GetBlock
    let get_block_request = wr::GetBlockRequest {
        organization: Some(organization),
        vault: Some(vault),
        height: block_height,
    };

    let block_response = read_client
        .get_block(get_block_request, rand::random::<u128>())
        .await
        .expect("get_block should succeed");

    let block = block_response.block.expect("should have block");
    let header = block.header.expect("block should have header");

    // Verify block header
    assert_eq!(header.height, block_height, "block height should match");
    assert!(header.state_root.is_some(), "block should have state_root");
    assert!(header.tx_merkle_root.is_some(), "block should have tx_merkle_root");
    assert!(header.previous_hash.is_some(), "block should have previous_hash");

    // Verify state_root is non-zero (we applied operations, so state changed)
    let state_root = header.state_root.unwrap();
    assert!(
        !state_root.value.iter().all(|&b| b == 0),
        "state_root should not be all zeros after write"
    );

    // Verify block contains the transaction
    assert_eq!(block.transactions.len(), 1, "block should have 1 transaction");
    let tx = &block.transactions[0];
    assert_eq!(tx.operations.len(), 1, "transaction should have 1 operation");
}

/// Tests write to leader replicates to followers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_three_node_write_replication() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&cluster, leader.id, "replication-ns")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, leader.id, organization).await.expect("create vault");

    // Wait for the per-vault group to be live on every voter before writing.
    // `wire_create_test_vault` returns when Raft commits CreateVault, but the local
    // watcher's `start_vault_group` fires fire-and-forget — without this wait,
    // an immediate write can race the registration and hit
    // "Vault is not active on this node".
    let region = inferadb_ledger_types::Region::US_EAST_VA;
    let org_id = resolve_org_id(leader, organization);
    let _vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(30))
            .await;

    let client = wire_write_client(&cluster, leader.id);

    // Submit a write
    let request = ww::WriteRequest {
        client_id: Some(ws::ClientIdMessage { id: "replication-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        organization: Some(organization),
        vault: Some(vault),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "replicated-key".to_string(),
                value: Bytes::from_static(b"replicated-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response =
        client.write(request, rand::random::<u128>()).await.expect("write should succeed");

    // Verify write succeeded
    match response.result {
        Some(ww::WriteResponseResult::Success(_)) => {},
        _ => panic!("write should succeed"),
    }

    // Wait for replication to complete
    let synced = cluster.wait_for_sync(Duration::from_secs(5)).await;
    assert!(synced, "all nodes should sync");

    // Verify all nodes have the same last applied index
    let leader_applied = leader.last_applied();
    for follower in cluster.followers() {
        assert_eq!(
            follower.last_applied(),
            leader_applied,
            "follower {} should have same last_applied as leader",
            follower.id
        );
    }
}
