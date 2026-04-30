//! Multi-region integration tests.
//!
//! Tests write forwarding, read consistency, batch writes, and cross-region
//! operations using the `RegionTestCluster` infrastructure.
//!
//! These tests exercise the full wire-protocol path through `WriteService`
//! and `ReadService` with multi-region routing, validating that
//! organization→region routing, idempotency, and error handling work
//! correctly across region boundaries.
//!
//! F.1.f.2.Stage1e Wave 5: migrated from the legacy tonic helpers
//! (`create_read_client` / `create_write_client` /
//! `create_test_organization` / `create_test_vault`) to their wire-protocol
//! siblings (`wire_read_client` / `wire_write_client` /
//! `wire_create_test_organization` / `wire_create_test_vault`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use bytes::Bytes;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
use inferadb_ledger_wire::{
    error::ErrorCode,
    services::{read as wr, shared as ws, write as ww},
};
use inferadb_ledger_wire_transport::RpcError;

use crate::common::{
    TestCluster, wire_create_test_organization, wire_create_test_vault, wire_read_client,
    wire_write_client,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization on a multi-region cluster and returns its slug.
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

/// Writes an entity and return the block height.
async fn write_entity(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
    value: &[u8],
) -> Result<u64, Box<dyn std::error::Error>> {
    let client = wire_write_client(cluster, node_id);

    let request = ww::WriteRequest {
        organization: Some(organization),
        vault: Some(vault),
        client_id: Some(ws::ClientIdMessage { id: "multi-region-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let response = client.write(request, rand::random::<u128>()).await?;
    match response.result {
        Some(ww::WriteResponseResult::Success(s)) => Ok(s.block_height),
        Some(ww::WriteResponseResult::Error(e)) => Err(format!("Write error: {:?}", e).into()),
        None => Err("No result in write response".into()),
    }
}

/// Reads an entity from a vault.
async fn read_entity(
    cluster: &TestCluster,
    node_id: u64,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let client = wire_read_client(cluster, node_id);

    let request = wr::ReadRequest {
        organization: Some(organization),
        vault: Some(vault),
        key: key.to_string(),
        consistency: ws::ReadConsistency::Eventual,
        caller: None,
    };

    let response = client.read(request, rand::random::<u128>()).await?;
    Ok(response.value.map(|b| b.to_vec()))
}

// ============================================================================
// Multi-Region Integration Tests
// ============================================================================

/// Tests that writes to an organization are routed to the correct region and readable.
///
/// Exercises the full write→route→Raft→apply→read path through wire transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_write_and_read() {
    let cluster = TestCluster::with_wire_transport(2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();

    // Create organization (gets assigned to a data region)
    let ns_id =
        create_organization(&cluster, node.id, "ms-write-read").await.expect("create organization");
    let vault = create_vault(&cluster, node.id, ns_id).await.expect("create vault");

    // Write entity
    let height = write_entity(&cluster, node.id, ns_id, vault, "key1", b"value1")
        .await
        .expect("write entity");
    assert!(height > 0, "block height should be positive");

    // Read it back
    let value = read_entity(&cluster, node.id, ns_id, vault, "key1").await.expect("read entity");
    assert_eq!(value, Some(b"value1".to_vec()), "should read back written value");
}

/// Tests that multiple organizations in different regions are isolated.
///
/// Writes to organization A should not be visible in organization B, even if both
/// are served by the same node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_organization_isolation() {
    let cluster = TestCluster::with_wire_transport(2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();

    // Create two organizations (may land in different regions)
    let ns_a = create_organization(&cluster, node.id, "isolated-a").await.expect("create ns A");
    let ns_b = create_organization(&cluster, node.id, "isolated-b").await.expect("create ns B");

    let vault_a = create_vault(&cluster, node.id, ns_a).await.expect("create vault A");
    let vault_b = create_vault(&cluster, node.id, ns_b).await.expect("create vault B");

    // Write to organization A
    write_entity(&cluster, node.id, ns_a, vault_a, "shared-key", b"value-a")
        .await
        .expect("write to ns A");

    // Write different value to organization B with same key
    write_entity(&cluster, node.id, ns_b, vault_b, "shared-key", b"value-b")
        .await
        .expect("write to ns B");

    // Read from both — values should be independent
    let val_a =
        read_entity(&cluster, node.id, ns_a, vault_a, "shared-key").await.expect("read from ns A");
    let val_b =
        read_entity(&cluster, node.id, ns_b, vault_b, "shared-key").await.expect("read from ns B");

    assert_eq!(val_a, Some(b"value-a".to_vec()), "ns A should have its own value");
    assert_eq!(val_b, Some(b"value-b".to_vec()), "ns B should have its own value");
}

/// Tests sequential per-vault writes through the multi-region service.
///
/// Originally exercised `BatchWrite`; migrated to per-vault `Write` after
/// cross-vault batches were deprecated. The atomicity invariant is sacrificed
/// (each `Write` is its own Raft proposal) but the read-after-write contract
/// still holds — both keys must be readable after the loop completes, and block
/// heights must be strictly monotonic in commit order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_batch_write() {
    let cluster = TestCluster::with_wire_transport(2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();

    let ns_id =
        create_organization(&cluster, node.id, "ms-batch").await.expect("create organization");
    let vault = create_vault(&cluster, node.id, ns_id).await.expect("create vault");

    // Submit per-vault writes sequentially. The block height must strictly
    // increase across calls because each `Write` is its own Raft proposal.
    let entries: &[(&str, &[u8])] =
        &[("batch-key-1", b"batch-val-1"), ("batch-key-2", b"batch-val-2")];

    let mut last_block_height: u64 = 0;
    for (key, value) in entries {
        let height =
            write_entity(&cluster, node.id, ns_id, vault, key, value).await.expect("write");
        assert!(
            height > last_block_height,
            "block height should be monotonically increasing: got {height} after {last_block_height}"
        );
        last_block_height = height;
    }

    // Verify both keys are readable
    let val1 = read_entity(&cluster, node.id, ns_id, vault, "batch-key-1")
        .await
        .expect("read batch key 1");
    let val2 = read_entity(&cluster, node.id, ns_id, vault, "batch-key-2")
        .await
        .expect("read batch key 2");

    assert_eq!(val1, Some(b"batch-val-1".to_vec()), "first batch key should be readable");
    assert_eq!(val2, Some(b"batch-val-2".to_vec()), "second batch key should be readable");
}

/// Tests idempotency across multi-region writes.
///
/// Same client_id + idempotency_key should return cached result on retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_write_idempotency() {
    let cluster = TestCluster::with_wire_transport(2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();

    let ns_id =
        create_organization(&cluster, node.id, "ms-idempotent").await.expect("create organization");
    let vault = create_vault(&cluster, node.id, ns_id).await.expect("create vault");

    let client = wire_write_client(&cluster, node.id);
    let idempotency_key = Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes());

    let request = ww::WriteRequest {
        organization: Some(ns_id),
        vault: Some(vault),
        client_id: Some(ws::ClientIdMessage { id: "idempotent-ms".to_string() }),
        idempotency_key,
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "idem-key".to_string(),
                value: Bytes::from_static(b"idem-value"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    // First write
    let resp1 = client.write(request.clone(), rand::random::<u128>()).await.expect("first write");

    // Second write with same idempotency key
    let resp2 = client.write(request, rand::random::<u128>()).await.expect("second write");

    // Both should return identical results
    match (resp1.result, resp2.result) {
        (
            Some(ww::WriteResponseResult::Success(s1)),
            Some(ww::WriteResponseResult::Success(s2)),
        ) => {
            assert_eq!(s1.tx_id, s2.tx_id, "idempotent writes should return same tx_id");
            assert_eq!(
                s1.block_height, s2.block_height,
                "idempotent writes should return same block_height"
            );
        },
        _ => panic!("both writes should succeed"),
    }
}

/// Tests that writes to a non-existent organization return an appropriate error.
///
/// The multi-region service should reject writes for organizations that haven't
/// been created, rather than silently dropping them or panicking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_write_nonexistent_organization() {
    let cluster = TestCluster::with_wire_transport(2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();
    let client = wire_write_client(&cluster, node.id);

    // Write to organization 99999 which doesn't exist
    let request = ww::WriteRequest {
        organization: Some(inferadb_ledger_types::OrganizationSlug::new(99999)),
        vault: Some(inferadb_ledger_types::VaultSlug::new(1)),
        client_id: Some(ws::ClientIdMessage { id: "nonexistent-ns".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: "test".to_string(),
                value: Bytes::from_static(b"test"),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };

    let result = client.write(request, rand::random::<u128>()).await;

    // Should get an error (either wire-level error or WriteResponse error)
    match result {
        Ok(response) => {
            match response.result {
                Some(ww::WriteResponseResult::Error(_)) => {
                    // Expected: write error for nonexistent organization
                },
                Some(ww::WriteResponseResult::Success(_)) => {
                    panic!("write to nonexistent organization should not succeed");
                },
                None => {
                    panic!("expected error result for nonexistent organization");
                },
            }
        },
        Err(RpcError::WireError(wire_err)) => {
            // Wire-level error is also acceptable
            assert!(
                matches!(
                    wire_err.code,
                    ErrorCode::NotFound
                        | ErrorCode::Internal
                        | ErrorCode::FailedPrecondition
                        | ErrorCode::InvalidArgument
                ),
                "expected NotFound, Internal, FailedPrecondition, or InvalidArgument for nonexistent organization, got: {:?}",
                wire_err.code
            );
        },
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

/// Tests concurrent writes to multiple organizations across regions.
///
/// Verifies that writes to different organizations can proceed in parallel
/// without interfering with each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_region_concurrent_writes() {
    let cluster = TestCluster::with_wire_transport(2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();
    let node_id = node.id;

    // Create 3 organizations
    let mut organizations = Vec::new();
    for i in 0..3 {
        let ns_id = create_organization(&cluster, node_id, &format!("concurrent-{}", i))
            .await
            .expect("create organization");
        let vault = create_vault(&cluster, node_id, ns_id).await.expect("create vault");
        organizations.push((ns_id, vault));
    }

    // Spawn concurrent writes to all organizations
    let mut handles = Vec::new();

    for (i, &(ns_id, vault)) in organizations.iter().enumerate() {
        // Per-task wire write client (no &cluster reference across tasks)
        let client = wire_write_client(&cluster, node_id);
        let handle = tokio::spawn(async move {
            for j in 0..5 {
                let key = format!("concurrent-{}-{}", i, j);
                let value = format!("value-{}-{}", i, j);
                let request = ww::WriteRequest {
                    organization: Some(ns_id),
                    vault: Some(vault),
                    client_id: Some(ws::ClientIdMessage { id: "multi-region-test".to_string() }),
                    idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
                    operations: vec![ws::Operation {
                        op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                            key,
                            value: Bytes::from(value.into_bytes()),
                            expires_at: None,
                            condition: None,
                        })),
                    }],
                    include_tx_proof: false,
                    caller: None,
                };
                let response =
                    client.write(request, rand::random::<u128>()).await.expect("concurrent write");
                match response.result {
                    Some(ww::WriteResponseResult::Success(_)) => {},
                    Some(ww::WriteResponseResult::Error(e)) => {
                        panic!("concurrent write error: {:?}", e);
                    },
                    None => panic!("concurrent write returned no result"),
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all writes to complete
    for handle in handles {
        handle.await.expect("task should not panic");
    }

    // Verify all writes are readable
    for (i, &(ns_id, vault)) in organizations.iter().enumerate() {
        for j in 0..5 {
            let key = format!("concurrent-{}-{}", i, j);
            let expected = format!("value-{}-{}", i, j);
            let value = read_entity(&cluster, node_id, ns_id, vault, &key)
                .await
                .expect("read after concurrent writes");
            assert_eq!(
                value,
                Some(expected.into_bytes()),
                "concurrent write {}-{} should be readable",
                i,
                j
            );
        }
    }
}

// ============================================================================
// Write Forwarding Integration Tests
// ============================================================================

/// Tests that writes to a local-region follower node succeed.
///
/// In a 3-node cluster where all nodes host all regions, writing to any node
/// should succeed — the `resolve_with_redirect()` returns `Local` and the
/// Raft layer handles leader election internally. This verifies the new
/// forwarding code path is a transparent no-op for local organizations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_write_forwarding_local_region_all_nodes() {
    // Single-node cluster with 2 data regions still uses RegionResolverService
    // (supports_forwarding=true) so the forwarding code path is exercised.
    let cluster = TestCluster::with_wire_transport(2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(15)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();
    let ns_id =
        create_organization(&cluster, node.id, "fwd-local-all").await.expect("create organization");
    let vault = create_vault(&cluster, node.id, ns_id).await.expect("create vault");

    // Write through the forwarding-enabled resolver — resolve_with_redirect
    // returns Local because the single node hosts every region.
    let height = write_entity(&cluster, node.id, ns_id, vault, "fwd-key-0", b"fwd-val-0")
        .await
        .expect("write should succeed through forwarding path");
    assert!(height > 0, "block height should be positive");

    // Verify readable
    tokio::time::sleep(Duration::from_millis(200)).await;
    let value =
        read_entity(&cluster, node.id, ns_id, vault, "fwd-key-0").await.expect("read after write");
    assert_eq!(value, Some(b"fwd-val-0".to_vec()));
}

/// Tests sequential per-vault writes through the forwarding-enabled write service.
///
/// Originally exercised `BatchWrite` against a non-leader; migrated to
/// per-vault `Write` after cross-vault batches were deprecated. Each `Write`
/// may itself be forwarded or redirected to the per-vault leader; the test
/// asserts the entries are readable after the writes commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_batch_write_forwarding_local_region() {
    let cluster = TestCluster::with_wire_transport(2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(15)).await,
        "all regions should elect leaders"
    );

    let node = cluster.any_node();
    let ns_id = create_organization(&cluster, node.id, "fwd-batch-local")
        .await
        .expect("create organization");
    let vault = create_vault(&cluster, node.id, ns_id).await.expect("create vault");

    let entries: &[(&str, &[u8])] =
        &[("batch-fwd-1", b"batch-fwd-val-1"), ("batch-fwd-2", b"batch-fwd-val-2")];

    for (key, value) in entries {
        let height = write_entity(&cluster, node.id, ns_id, vault, key, value)
            .await
            .expect("write to non-leader");
        assert!(height > 0, "write should produce a block");
    }

    // Verify readable
    tokio::time::sleep(Duration::from_millis(200)).await;
    let val1 = read_entity(&cluster, node.id, ns_id, vault, "batch-fwd-1")
        .await
        .expect("read batch key 1");
    let val2 = read_entity(&cluster, node.id, ns_id, vault, "batch-fwd-2")
        .await
        .expect("read batch key 2");

    assert_eq!(val1, Some(b"batch-fwd-val-1".to_vec()));
    assert_eq!(val2, Some(b"batch-fwd-val-2".to_vec()));
}
