//! Multi-shard integration tests.
//!
//! Tests write forwarding, read consistency, batch writes, and cross-shard
//! operations using the `MultiShardTestCluster` infrastructure.
//!
//! These tests exercise the full gRPC path through `MultiShardWriteServiceImpl`
//! and `MultiShardReadServiceImpl`, validating that organization→shard routing,
//! idempotency, and error handling work correctly across shard boundaries.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::common::{
    MultiShardTestCluster, create_admin_client, create_read_client, create_write_client,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization on a multi-shard cluster and returns its slug.
async fn create_organization(
    addr: std::net::SocketAddr,
    name: &str,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_organization(inferadb_ledger_proto::proto::CreateOrganizationRequest {
            name: name.to_string(),
            shard_id: None,
            quota: None,
        })
        .await?;

    let organization = response
        .into_inner()
        .slug
        .map(|n| OrganizationSlug::new(n.slug))
        .ok_or("No organization slug in response")?;

    Ok(organization)
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    addr: std::net::SocketAddr,
    organization: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_vault(inferadb_ledger_proto::proto::CreateVaultRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization.value(),
            }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
        })
        .await?;

    let vault = response
        .into_inner()
        .vault
        .map(|v| VaultSlug::new(v.slug))
        .ok_or("No vault in response")?;
    Ok(vault)
}

/// Writes an entity and return the block height.
async fn write_entity(
    addr: std::net::SocketAddr,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
    value: &[u8],
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_write_client(addr).await?;

    let request = inferadb_ledger_proto::proto::WriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "multi-shard-test".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: key.to_string(),
                    value: value.to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response = client.write(request).await?.into_inner();
    match response.result {
        Some(inferadb_ledger_proto::proto::write_response::Result::Success(s)) => {
            Ok(s.block_height)
        },
        Some(inferadb_ledger_proto::proto::write_response::Result::Error(e)) => {
            Err(format!("Write error: {:?}", e).into())
        },
        None => Err("No result in write response".into()),
    }
}

/// Reads an entity from a vault.
async fn read_entity(
    addr: std::net::SocketAddr,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut client = create_read_client(addr).await?;

    let request = inferadb_ledger_proto::proto::ReadRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        key: key.to_string(),
        consistency: 0, // EVENTUAL
    };

    let response = client.read(request).await?.into_inner();
    Ok(response.value)
}

// ============================================================================
// Multi-Shard Integration Tests
// ============================================================================

/// Tests that writes to an organization are routed to the correct shard and readable.
///
/// Exercises the full write→route→Raft→apply→read path through gRPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_shard_write_and_read() {
    let cluster = MultiShardTestCluster::new(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all shards should elect leaders"
    );

    let node = cluster.any_node();

    // Create organization (gets assigned to a data shard)
    let ns_id = create_organization(node.addr, "ms-write-read").await.expect("create organization");
    let vault = create_vault(node.addr, ns_id).await.expect("create vault");

    // Write entity
    let height =
        write_entity(node.addr, ns_id, vault, "key1", b"value1").await.expect("write entity");
    assert!(height > 0, "block height should be positive");

    // Read it back
    let value = read_entity(node.addr, ns_id, vault, "key1").await.expect("read entity");
    assert_eq!(value, Some(b"value1".to_vec()), "should read back written value");
}

/// Tests that multiple organizations on different shards are isolated.
///
/// Writes to organization A should not be visible in organization B, even if both
/// are served by the same node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_shard_organization_isolation() {
    let cluster = MultiShardTestCluster::new(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all shards should elect leaders"
    );

    let node = cluster.any_node();

    // Create two organizations (may land on different shards)
    let ns_a = create_organization(node.addr, "isolated-a").await.expect("create ns A");
    let ns_b = create_organization(node.addr, "isolated-b").await.expect("create ns B");

    let vault_a = create_vault(node.addr, ns_a).await.expect("create vault A");
    let vault_b = create_vault(node.addr, ns_b).await.expect("create vault B");

    // Write to organization A
    write_entity(node.addr, ns_a, vault_a, "shared-key", b"value-a").await.expect("write to ns A");

    // Write different value to organization B with same key
    write_entity(node.addr, ns_b, vault_b, "shared-key", b"value-b").await.expect("write to ns B");

    // Read from both — values should be independent
    let val_a = read_entity(node.addr, ns_a, vault_a, "shared-key").await.expect("read from ns A");
    let val_b = read_entity(node.addr, ns_b, vault_b, "shared-key").await.expect("read from ns B");

    assert_eq!(val_a, Some(b"value-a".to_vec()), "ns A should have its own value");
    assert_eq!(val_b, Some(b"value-b".to_vec()), "ns B should have its own value");
}

/// Tests batch writes through the multi-shard service.
///
/// Verifies that BatchWrite routes correctly and applies atomically.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_shard_batch_write() {
    let cluster = MultiShardTestCluster::new(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all shards should elect leaders"
    );

    let node = cluster.any_node();

    let ns_id = create_organization(node.addr, "ms-batch").await.expect("create organization");
    let vault = create_vault(node.addr, ns_id).await.expect("create vault");

    // Submit a batch write with multiple operations
    let mut client = create_write_client(node.addr).await.expect("connect");

    let request = inferadb_ledger_proto::proto::BatchWriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: ns_id.value() }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "batch-client".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::BatchWriteOperation {
            operations: vec![
                inferadb_ledger_proto::proto::Operation {
                    op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                        inferadb_ledger_proto::proto::SetEntity {
                            key: "batch-key-1".to_string(),
                            value: b"batch-val-1".to_vec(),
                            expires_at: None,
                            condition: None,
                        },
                    )),
                },
                inferadb_ledger_proto::proto::Operation {
                    op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                        inferadb_ledger_proto::proto::SetEntity {
                            key: "batch-key-2".to_string(),
                            value: b"batch-val-2".to_vec(),
                            expires_at: None,
                            condition: None,
                        },
                    )),
                },
            ],
        }],
        include_tx_proofs: false,
    };

    let response = client.batch_write(request).await.expect("batch write").into_inner();
    match response.result {
        Some(inferadb_ledger_proto::proto::batch_write_response::Result::Success(s)) => {
            assert!(s.block_height > 0, "batch should produce a block");
            assert!(s.tx_id.is_some(), "batch should have a tx_id");
        },
        Some(inferadb_ledger_proto::proto::batch_write_response::Result::Error(e)) => {
            panic!("batch write failed: {:?}", e);
        },
        None => panic!("no result in batch write response"),
    }

    // Verify both keys are readable
    let val1 = read_entity(node.addr, ns_id, vault, "batch-key-1").await.expect("read batch key 1");
    let val2 = read_entity(node.addr, ns_id, vault, "batch-key-2").await.expect("read batch key 2");

    assert_eq!(val1, Some(b"batch-val-1".to_vec()), "first batch key should be readable");
    assert_eq!(val2, Some(b"batch-val-2".to_vec()), "second batch key should be readable");
}

/// Tests idempotency across multi-shard writes.
///
/// Same client_id + idempotency_key should return cached result on retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_shard_write_idempotency() {
    let cluster = MultiShardTestCluster::new(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all shards should elect leaders"
    );

    let node = cluster.any_node();

    let ns_id = create_organization(node.addr, "ms-idempotent").await.expect("create organization");
    let vault = create_vault(node.addr, ns_id).await.expect("create vault");

    let mut client = create_write_client(node.addr).await.expect("connect");
    let idempotency_key = uuid::Uuid::new_v4().as_bytes().to_vec();

    let request = inferadb_ledger_proto::proto::WriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: ns_id.value() }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId { id: "idempotent-ms".to_string() }),
        idempotency_key: idempotency_key.clone(),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "idem-key".to_string(),
                    value: b"idem-value".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    // First write
    let resp1 = client.write(request.clone()).await.expect("first write").into_inner();

    // Second write with same idempotency key
    let resp2 = client.write(request).await.expect("second write").into_inner();

    // Both should return identical results
    match (resp1.result, resp2.result) {
        (
            Some(inferadb_ledger_proto::proto::write_response::Result::Success(s1)),
            Some(inferadb_ledger_proto::proto::write_response::Result::Success(s2)),
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
/// The multi-shard service should reject writes for organizations that haven't
/// been created, rather than silently dropping them or panicking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_shard_write_nonexistent_organization() {
    let cluster = MultiShardTestCluster::new(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all shards should elect leaders"
    );

    let node = cluster.any_node();
    let mut client = create_write_client(node.addr).await.expect("connect");

    // Write to organization 99999 which doesn't exist
    let request = inferadb_ledger_proto::proto::WriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: 99999 }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: 1 }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "nonexistent-ns".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::Operation {
            op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                inferadb_ledger_proto::proto::SetEntity {
                    key: "test".to_string(),
                    value: b"test".to_vec(),
                    expires_at: None,
                    condition: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let result = client.write(request).await;

    // Should get an error (either gRPC status error or WriteResponse error)
    match result {
        Ok(response) => {
            let inner = response.into_inner();
            match inner.result {
                Some(inferadb_ledger_proto::proto::write_response::Result::Error(_)) => {
                    // Expected: write error for nonexistent organization
                },
                Some(inferadb_ledger_proto::proto::write_response::Result::Success(_)) => {
                    panic!("write to nonexistent organization should not succeed");
                },
                None => {
                    panic!("expected error result for nonexistent organization");
                },
            }
        },
        Err(status) => {
            // gRPC-level error is also acceptable
            assert!(
                status.code() == tonic::Code::NotFound
                    || status.code() == tonic::Code::Internal
                    || status.code() == tonic::Code::FailedPrecondition
                    || status.code() == tonic::Code::InvalidArgument,
                "expected NOT_FOUND, INTERNAL, FAILED_PRECONDITION, or INVALID_ARGUMENT for nonexistent organization, got: {:?}",
                status.code()
            );
        },
    }
}

/// Tests concurrent writes to multiple organizations across shards.
///
/// Verifies that writes to different organizations can proceed in parallel
/// without interfering with each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_shard_concurrent_writes() {
    let cluster = MultiShardTestCluster::new(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(10)).await,
        "all shards should elect leaders"
    );

    let node = cluster.any_node();

    // Create 3 organizations
    let mut organizations = Vec::new();
    for i in 0..3 {
        let ns_id = create_organization(node.addr, &format!("concurrent-{}", i))
            .await
            .expect("create organization");
        let vault = create_vault(node.addr, ns_id).await.expect("create vault");
        organizations.push((ns_id, vault));
    }

    // Spawn concurrent writes to all organizations
    let addr = node.addr;
    let mut handles = Vec::new();

    for (i, &(ns_id, vault)) in organizations.iter().enumerate() {
        let handle = tokio::spawn(async move {
            for j in 0..5 {
                let key = format!("concurrent-{}-{}", i, j);
                let value = format!("value-{}-{}", i, j);
                write_entity(addr, ns_id, vault, &key, value.as_bytes())
                    .await
                    .expect("concurrent write");
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
            let value =
                read_entity(addr, ns_id, vault, &key).await.expect("read after concurrent writes");
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

/// Tests that writes to a local-shard follower node succeed.
///
/// In a 3-node cluster where all nodes host all shards, writing to any node
/// should succeed — the `resolve_with_forward()` returns `Local` and the
/// Raft layer handles leader election internally. This verifies the new
/// forwarding code path is a transparent no-op for local organizations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_write_forwarding_local_shard_all_nodes() {
    // Single-node cluster with 2 data shards still uses MultiShardResolver
    // (supports_forwarding=true) so the forwarding code path is exercised.
    let cluster = MultiShardTestCluster::new(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(15)).await,
        "all shards should elect leaders"
    );

    let node = cluster.any_node();
    let ns_id = create_organization(node.addr, "fwd-local-all").await.expect("create organization");
    let vault = create_vault(node.addr, ns_id).await.expect("create vault");

    // Write through the forwarding-enabled resolver — resolve_with_forward
    // returns Local because the single node hosts every shard.
    let height = write_entity(node.addr, ns_id, vault, "fwd-key-0", b"fwd-val-0")
        .await
        .expect("write should succeed through forwarding path");
    assert!(height > 0, "block height should be positive");

    // Verify readable
    tokio::time::sleep(Duration::from_millis(200)).await;
    let value = read_entity(node.addr, ns_id, vault, "fwd-key-0").await.expect("read after write");
    assert_eq!(value, Some(b"fwd-val-0".to_vec()));
}

/// Tests batch write through the forwarding-enabled `MultiShardWriteService`.
///
/// Verifies that `batch_write()` through `MultiShardWriteService` with the
/// forwarding-enabled `MultiShardResolver` works correctly for local shards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_batch_write_forwarding_local_shard() {
    let cluster = MultiShardTestCluster::new(1, 2).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(15)).await,
        "all shards should elect leaders"
    );

    let node = cluster.any_node();
    let ns_id =
        create_organization(node.addr, "fwd-batch-local").await.expect("create organization");
    let vault = create_vault(node.addr, ns_id).await.expect("create vault");

    // Send batch write through the forwarding-enabled resolver
    let mut client = create_write_client(node.addr).await.expect("connect");

    let request = inferadb_ledger_proto::proto::BatchWriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug { slug: ns_id.value() }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "batch-fwd-client".to_string(),
        }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_proto::proto::BatchWriteOperation {
            operations: vec![
                inferadb_ledger_proto::proto::Operation {
                    op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                        inferadb_ledger_proto::proto::SetEntity {
                            key: "batch-fwd-1".to_string(),
                            value: b"batch-fwd-val-1".to_vec(),
                            expires_at: None,
                            condition: None,
                        },
                    )),
                },
                inferadb_ledger_proto::proto::Operation {
                    op: Some(inferadb_ledger_proto::proto::operation::Op::SetEntity(
                        inferadb_ledger_proto::proto::SetEntity {
                            key: "batch-fwd-2".to_string(),
                            value: b"batch-fwd-val-2".to_vec(),
                            expires_at: None,
                            condition: None,
                        },
                    )),
                },
            ],
        }],
        include_tx_proofs: false,
    };

    let response =
        client.batch_write(request).await.expect("batch write to non-leader").into_inner();
    match response.result {
        Some(inferadb_ledger_proto::proto::batch_write_response::Result::Success(s)) => {
            assert!(s.block_height > 0, "batch should produce a block");
        },
        Some(inferadb_ledger_proto::proto::batch_write_response::Result::Error(e)) => {
            panic!("batch write failed: {:?}", e);
        },
        None => panic!("no result in batch write response"),
    }

    // Verify readable
    tokio::time::sleep(Duration::from_millis(200)).await;
    let val1 = read_entity(node.addr, ns_id, vault, "batch-fwd-1").await.expect("read batch key 1");
    let val2 = read_entity(node.addr, ns_id, vault, "batch-fwd-2").await.expect("read batch key 2");

    assert_eq!(val1, Some(b"batch-fwd-val-1".to_vec()));
    assert_eq!(val2, Some(b"batch-fwd-val-2".to_vec()));
}
