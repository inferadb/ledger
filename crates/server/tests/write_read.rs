//! Write/read integration tests.
//!
//! Tests single-node and multi-node write/read cycles through Raft consensus.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

use std::time::Duration;

use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::common::{
    TestCluster, TestNode, create_read_client, create_write_client, resolve_org_id,
    wait_for_vault_group_live_on_all_voters,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization and returns its slug.
async fn create_organization(
    addr: &str,
    name: &str,
    node: &TestNode,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let (slug, _admin) = crate::common::create_test_organization(addr, name, node).await?;
    Ok(slug)
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    addr: &str,
    organization: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    crate::common::create_test_vault(addr, organization).await
}

// ============================================================================
// Write/Read Tests
// ============================================================================

/// Tests single-node write and read cycle.
#[tokio::test]
async fn test_single_node_write_read() {
    let cluster = TestCluster::new(1).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&leader.addr, "write-read-ns", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, organization).await.expect("create vault");

    // Create a write client
    let mut client = create_write_client(&leader.addr).await.expect("connect to leader");

    // Submit a write request using SetEntity operation
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "test-client".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "test-key".to_string(),
                    value: b"test-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request).await;
    assert!(response.is_ok(), "write should succeed: {:?}", response.err());

    let response = response.unwrap().into_inner();
    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(success)) => {
            assert!(success.tx_id.is_some(), "should have tx_id");
            assert!(success.block_height > 0, "should have non-zero block height");
        },
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(err)) => {
            panic!("write failed: {:?}", err);
        },
        None => {
            panic!("no result in response");
        },
    }
}

/// Tests write idempotency - same client_id + idempotency_key should return cached result.
#[tokio::test]
async fn test_write_idempotency() {
    let cluster = TestCluster::new(1).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&leader.addr, "idempotency-ns", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, organization).await.expect("create vault");

    let mut client = create_write_client(&leader.addr).await.expect("connect to leader");

    // Use a fixed idempotency key for both writes to test deduplication
    let idempotency_key = uuid::Uuid::new_v4().as_bytes().to_vec();

    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "idempotent-client".to_string(),
        }),
        idempotency_key: idempotency_key.clone(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "idempotent-key".to_string(),
                    value: b"idempotent-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    // First write
    let response1 =
        client.write(request.clone()).await.expect("first write should succeed").into_inner();

    // Second write with same client_id + idempotency_key
    let response2 = client.write(request).await.expect("second write should succeed").into_inner();

    // Both should return the same result
    match (response1.result, response2.result) {
        (
            Some(inferadb_ledger_proto::proto::write_response::Result::Success(s1)),
            Some(inferadb_ledger_proto::proto::write_response::Result::Success(s2)),
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
#[tokio::test]
async fn test_write_creates_retrievable_block() {
    let cluster = TestCluster::new(1).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&leader.addr, "block-test-ns", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, organization).await.expect("create vault");

    // Create write and read clients
    let mut write_client = create_write_client(&leader.addr).await.expect("connect to leader");
    let mut read_client =
        create_read_client(&leader.addr).await.expect("connect to leader for reads");

    // Submit a write
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "block-test".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "block-key".to_string(),
                    value: b"block-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = write_client.write(request).await.expect("write should succeed").into_inner();

    let block_height = match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => s.block_height,
        _ => panic!("write should succeed"),
    };

    assert!(block_height > 0, "block_height should be > 0");

    // Retrieve the block via GetBlock
    let get_block_request = inferadb_ledger_proto::proto::GetBlockRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        height: block_height,
    };

    let block_response = read_client
        .get_block(get_block_request)
        .await
        .expect("get_block should succeed")
        .into_inner();

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
    let cluster = TestCluster::new(3).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;

    let leader = cluster.leader().expect("should have leader");

    // Create organization and vault
    let organization = create_organization(&leader.addr, "replication-ns", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, organization).await.expect("create vault");

    // Wait for the per-vault group to be live on every voter before writing.
    // `create_test_vault` returns when Raft commits CreateVault, but the local
    // watcher's `start_vault_group` fires fire-and-forget — without this wait,
    // an immediate write can race the registration and hit
    // "Vault is not active on this node".
    let region = inferadb_ledger_types::Region::US_EAST_VA;
    let org_id = resolve_org_id(leader, organization);
    let _vault_id =
        wait_for_vault_group_live_on_all_voters(&cluster, region, org_id, Duration::from_secs(15))
            .await;

    let mut client = create_write_client(&leader.addr).await.expect("connect to leader");

    // Submit a write
    let request = inferadb_ledger_proto::proto::WriteRequest {
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "replication-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "replicated-key".to_string(),
                    value: b"replicated-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request).await.expect("write should succeed");
    let response = response.into_inner();

    // Verify write succeeded
    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {},
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
