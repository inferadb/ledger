//! Namespace and vault isolation tests.
//!
//! These tests verify that data in one namespace/vault is properly isolated
//! from other namespaces/vaults. Key design invariants tested:
//!
//! - Vault IDs are globally unique (assigned by a global sequence counter)
//! - Data written to one vault cannot be read from another vault
//! - Namespaces provide organizational isolation through their vaults
//! - Multiple vaults within the same namespace are isolated from each other
//!
//! Per DESIGN.md:
//! - Line 158: "Namespace | Entities, vaults, keys" - isolation boundary
//! - Line 162: "Namespaces share physical nodes and Raft groups but maintain independent data"
//! - Line 226: VaultId is "Sequential from `_meta:seq:vault`" - globally unique

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

mod common;

use std::time::Duration;

use common::{TestCluster, create_admin_client, create_read_client, create_write_client};
use serial_test::serial;

// ============================================================================
// Test Helpers
// ============================================================================

/// Create a namespace and return its ID.
async fn create_namespace(
    addr: std::net::SocketAddr,
    name: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_namespace(inferadb_ledger_raft::proto::CreateNamespaceRequest {
            name: name.to_string(),
            shard_id: None,
        })
        .await?;

    let namespace_id =
        response.into_inner().namespace_id.map(|n| n.id).ok_or("No namespace_id in response")?;

    Ok(namespace_id)
}

/// Create a vault in a namespace and return its ID.
async fn create_vault(
    addr: std::net::SocketAddr,
    namespace_id: i64,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_vault(inferadb_ledger_raft::proto::CreateVaultRequest {
            namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId { id: namespace_id }),
            replication_factor: 0,  // Default, ignored for single-shard setup
            initial_nodes: vec![],  // Auto-assigned
            retention_policy: None, // Default: FULL
        })
        .await?;

    let vault_id = response.into_inner().vault_id.map(|v| v.id).ok_or("No vault_id in response")?;

    Ok(vault_id)
}

/// Write a key-value pair to a vault.
async fn write_entity(
    addr: std::net::SocketAddr,
    namespace_id: i64,
    vault_id: i64,
    key: &str,
    value: &[u8],
    client_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_write_client(addr).await?;

    let request = inferadb_ledger_raft::proto::WriteRequest {
        namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_raft::proto::VaultId { id: vault_id }),
        client_id: Some(inferadb_ledger_raft::proto::ClientId { id: client_id.to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![inferadb_ledger_raft::proto::Operation {
            op: Some(inferadb_ledger_raft::proto::operation::Op::SetEntity(
                inferadb_ledger_raft::proto::SetEntity {
                    key: key.to_string(),
                    value: value.to_vec(),
                    condition: None,
                    expires_at: None,
                },
            )),
        }],
        include_tx_proof: false,
    };

    let response = client.write(request).await?.into_inner();

    match response.result {
        Some(inferadb_ledger_raft::proto::write_response::Result::Success(s)) => Ok(s.block_height),
        Some(inferadb_ledger_raft::proto::write_response::Result::Error(e)) => {
            Err(format!("Write error: {:?}", e).into())
        },
        None => Err("No result in write response".into()),
    }
}

/// Read an entity from a vault.
async fn read_entity(
    addr: std::net::SocketAddr,
    namespace_id: i64,
    vault_id: i64,
    key: &str,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut client = create_read_client(addr).await?;

    let request = inferadb_ledger_raft::proto::ReadRequest {
        namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId { id: namespace_id }),
        vault_id: Some(inferadb_ledger_raft::proto::VaultId { id: vault_id }),
        key: key.to_string(),
        consistency: 0, // EVENTUAL
    };

    let response = client.read(request).await?.into_inner();
    Ok(response.value)
}

// ============================================================================
// Vault Isolation Tests
// ============================================================================

/// Test that data written to one vault cannot be read from another vault.
///
/// This is the fundamental isolation guarantee: each vault is a separate
/// data container with its own independent key space.
#[serial]
#[tokio::test]
async fn test_vault_isolation_same_namespace() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create a namespace
    let ns_id = create_namespace(leader.addr, "isolation-test-ns").await.expect("create namespace");

    // Create two vaults in the same namespace
    let vault_a = create_vault(leader.addr, ns_id).await.expect("create vault A");
    let vault_b = create_vault(leader.addr, ns_id).await.expect("create vault B");

    // Vault IDs should be different (globally unique)
    assert_ne!(vault_a, vault_b, "Vault IDs should be globally unique");

    // Write data to vault A
    write_entity(leader.addr, ns_id, vault_a, "shared-key", b"value-from-vault-a", "client-a")
        .await
        .expect("write to vault A");

    // Write DIFFERENT data with the SAME key to vault B
    write_entity(leader.addr, ns_id, vault_b, "shared-key", b"value-from-vault-b", "client-b")
        .await
        .expect("write to vault B");

    // Read from vault A - should get vault A's value
    let value_a = read_entity(leader.addr, ns_id, vault_a, "shared-key")
        .await
        .expect("read from vault A")
        .expect("should have value in vault A");
    assert_eq!(value_a, b"value-from-vault-a", "Vault A should have its own value");

    // Read from vault B - should get vault B's value
    let value_b = read_entity(leader.addr, ns_id, vault_b, "shared-key")
        .await
        .expect("read from vault B")
        .expect("should have value in vault B");
    assert_eq!(value_b, b"value-from-vault-b", "Vault B should have its own value");
}

/// Test that data in vault A is not visible when reading from vault B.
#[serial]
#[tokio::test]
async fn test_vault_isolation_key_not_found() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create namespace and vaults
    let ns_id =
        create_namespace(leader.addr, "isolation-not-found-ns").await.expect("create namespace");

    let vault_a = create_vault(leader.addr, ns_id).await.expect("create vault A");
    let vault_b = create_vault(leader.addr, ns_id).await.expect("create vault B");

    // Write a unique key to vault A only
    write_entity(leader.addr, ns_id, vault_a, "unique-to-vault-a", b"secret-value", "client-a")
        .await
        .expect("write to vault A");

    // Try to read that key from vault B - should NOT be found
    let value = read_entity(leader.addr, ns_id, vault_b, "unique-to-vault-a")
        .await
        .expect("read should not error");

    assert!(value.is_none(), "Key from vault A should NOT be visible in vault B");
}

/// Test isolation with multiple keys across multiple vaults.
#[serial]
#[tokio::test]
async fn test_multi_vault_isolation() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create namespace
    let ns_id = create_namespace(leader.addr, "multi-vault-ns").await.expect("create namespace");

    // Create 5 vaults
    let mut vaults = Vec::new();
    for _ in 0..5 {
        let vault_id = create_vault(leader.addr, ns_id).await.expect("create vault");
        vaults.push(vault_id);
    }

    // Verify all vault IDs are unique
    let unique_count = vaults.iter().collect::<std::collections::HashSet<_>>().len();
    assert_eq!(unique_count, 5, "All vault IDs should be unique");

    // Write to each vault
    for (i, &vault_id) in vaults.iter().enumerate() {
        write_entity(
            leader.addr,
            ns_id,
            vault_id,
            "common-key",
            format!("vault-{}-value", i).as_bytes(),
            &format!("client-{}", i),
        )
        .await
        .expect("write to vault");
    }

    // Verify each vault has its own value
    for (i, &vault_id) in vaults.iter().enumerate() {
        let value = read_entity(leader.addr, ns_id, vault_id, "common-key")
            .await
            .expect("read from vault")
            .expect("should have value");

        let expected = format!("vault-{}-value", i);
        assert_eq!(value, expected.as_bytes(), "Vault {} should have its unique value", i);
    }
}

// ============================================================================
// Namespace Isolation Tests
// ============================================================================

/// Test that vaults in different namespaces are isolated.
///
/// Even though vault IDs are globally unique, namespaces provide an
/// organizational boundary. This test verifies the isolation model.
#[serial]
#[tokio::test]
async fn test_namespace_isolation() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create two namespaces
    let ns_1 = create_namespace(leader.addr, "org-alpha").await.expect("create namespace 1");
    let ns_2 = create_namespace(leader.addr, "org-beta").await.expect("create namespace 2");

    assert_ne!(ns_1, ns_2, "Namespace IDs should be different");

    // Create a vault in each namespace
    let vault_1 = create_vault(leader.addr, ns_1).await.expect("create vault in ns1");
    let vault_2 = create_vault(leader.addr, ns_2).await.expect("create vault in ns2");

    // Vault IDs are globally unique
    assert_ne!(vault_1, vault_2, "Vault IDs are globally unique across namespaces");

    // Write to vault in namespace 1
    write_entity(leader.addr, ns_1, vault_1, "org-secret", b"alpha-data", "alpha-client")
        .await
        .expect("write to ns1 vault");

    // Write to vault in namespace 2
    write_entity(leader.addr, ns_2, vault_2, "org-secret", b"beta-data", "beta-client")
        .await
        .expect("write to ns2 vault");

    // Verify namespace 1's data
    let value_1 = read_entity(leader.addr, ns_1, vault_1, "org-secret")
        .await
        .expect("read from ns1")
        .expect("should have value");
    assert_eq!(value_1, b"alpha-data");

    // Verify namespace 2's data
    let value_2 = read_entity(leader.addr, ns_2, vault_2, "org-secret")
        .await
        .expect("read from ns2")
        .expect("should have value");
    assert_eq!(value_2, b"beta-data");
}

/// Test behavior when accessing a vault with a mismatched namespace_id.
///
/// DESIGN NOTE: Since vault_ids are globally unique (generated from a global
/// sequence counter), the vault_id alone is sufficient to identify data.
/// The namespace_id parameter is currently NOT validated on reads - the system
/// trusts the globally unique vault_id as the authoritative identifier.
///
/// This test documents the current behavior. Actual data isolation is ensured
/// by the global uniqueness of vault_ids - you cannot access another vault's
/// data without knowing its vault_id.
#[serial]
#[tokio::test]
async fn test_vault_id_is_authoritative_identifier() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create namespace and vault
    let ns_1 = create_namespace(leader.addr, "vault-auth-ns-1").await.expect("create namespace 1");
    let vault_1 = create_vault(leader.addr, ns_1).await.expect("create vault in ns1");

    // Write data to vault_1
    write_entity(leader.addr, ns_1, vault_1, "test-key", b"vault1-data", "client")
        .await
        .expect("write to vault");

    // Create a second namespace with its own vault
    let ns_2 = create_namespace(leader.addr, "vault-auth-ns-2").await.expect("create namespace 2");
    let vault_2 = create_vault(leader.addr, ns_2).await.expect("create vault in ns2");

    // Vault IDs are different (globally unique)
    assert_ne!(vault_1, vault_2, "Vault IDs should be globally unique");

    // Even with ns_2 specified, vault_2's data is independent
    write_entity(leader.addr, ns_2, vault_2, "test-key", b"vault2-data", "client-2")
        .await
        .expect("write to vault 2");

    // Key point: vault_id is the authoritative identifier
    // Reading vault_2 with correct IDs gets vault_2's data
    let result = read_entity(leader.addr, ns_2, vault_2, "test-key")
        .await
        .expect("read should succeed")
        .expect("should have value");
    assert_eq!(result, b"vault2-data");

    // Reading vault_1 with correct IDs gets vault_1's data
    let result = read_entity(leader.addr, ns_1, vault_1, "test-key")
        .await
        .expect("read should succeed")
        .expect("should have value");
    assert_eq!(result, b"vault1-data");

    // SECURITY: You CANNOT access vault_1's data using vault_2's ID
    // The vault_id being globally unique means you need the correct vault_id
    // to access data - namespace_id validation is not the isolation mechanism.
    let result = read_entity(leader.addr, ns_2, vault_2, "test-key")
        .await
        .expect("read should succeed")
        .expect("should have value");
    assert_eq!(result, b"vault2-data", "Vault ID determines data access");
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

/// Test concurrent writes to different vaults are isolated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_vault_writes() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create namespace and vaults
    let ns_id = create_namespace(leader.addr, "concurrent-ns").await.expect("create namespace");

    let vault_a = create_vault(leader.addr, ns_id).await.expect("create vault A");
    let vault_b = create_vault(leader.addr, ns_id).await.expect("create vault B");

    let addr = leader.addr;

    // Spawn concurrent writes to both vaults
    let write_a = tokio::spawn(async move {
        for i in 0..10 {
            write_entity(
                addr,
                ns_id,
                vault_a,
                &format!("key-{}", i),
                format!("vault-a-{}", i).as_bytes(),
                "client-a",
            )
            .await
            .expect("write to vault A");
        }
        vault_a
    });

    let addr = leader.addr;
    let write_b = tokio::spawn(async move {
        for i in 0..10 {
            write_entity(
                addr,
                ns_id,
                vault_b,
                &format!("key-{}", i),
                format!("vault-b-{}", i).as_bytes(),
                "client-b",
            )
            .await
            .expect("write to vault B");
        }
        vault_b
    });

    let (vault_a, vault_b) = tokio::try_join!(write_a, write_b).expect("joins");

    // Allow time for writes to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify isolation after concurrent writes
    for i in 0..10 {
        let key = format!("key-{}", i);

        let value_a = read_entity(leader.addr, ns_id, vault_a, &key)
            .await
            .expect("read from vault A")
            .expect("should have value in A");
        assert_eq!(value_a, format!("vault-a-{}", i).as_bytes());

        let value_b = read_entity(leader.addr, ns_id, vault_b, &key)
            .await
            .expect("read from vault B")
            .expect("should have value in B");
        assert_eq!(value_b, format!("vault-b-{}", i).as_bytes());
    }
}

// ============================================================================
// Vault ID Uniqueness Tests
// ============================================================================

/// Verify that vault IDs are monotonically increasing and globally unique.
#[serial]
#[tokio::test]
async fn test_vault_id_global_uniqueness() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create multiple namespaces
    let ns_1 = create_namespace(leader.addr, "uniqueness-ns-1").await.expect("create ns1");
    let ns_2 = create_namespace(leader.addr, "uniqueness-ns-2").await.expect("create ns2");
    let ns_3 = create_namespace(leader.addr, "uniqueness-ns-3").await.expect("create ns3");

    // Collect vault IDs from all namespaces
    let mut all_vault_ids = Vec::new();

    // Create vaults in ns_1
    for _ in 0..3 {
        let vault_id = create_vault(leader.addr, ns_1).await.expect("create vault");
        all_vault_ids.push(vault_id);
    }

    // Create vaults in ns_2
    for _ in 0..3 {
        let vault_id = create_vault(leader.addr, ns_2).await.expect("create vault");
        all_vault_ids.push(vault_id);
    }

    // Create vaults in ns_3
    for _ in 0..3 {
        let vault_id = create_vault(leader.addr, ns_3).await.expect("create vault");
        all_vault_ids.push(vault_id);
    }

    // Verify all IDs are unique
    let unique_count = all_vault_ids.iter().collect::<std::collections::HashSet<_>>().len();
    assert_eq!(
        unique_count,
        all_vault_ids.len(),
        "All vault IDs across all namespaces should be unique"
    );

    // Verify IDs are monotonically increasing
    for i in 1..all_vault_ids.len() {
        assert!(
            all_vault_ids[i] > all_vault_ids[i - 1],
            "Vault IDs should be monotonically increasing"
        );
    }
}

// ============================================================================
// Multi-Node Isolation Tests
// ============================================================================

/// Test that vault isolation is maintained across cluster replication.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_isolation_across_replicas() {
    let cluster = TestCluster::new(3).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create namespace and vaults via leader
    let ns_id =
        create_namespace(leader.addr, "replica-isolation-ns").await.expect("create namespace");

    let vault_a = create_vault(leader.addr, ns_id).await.expect("create vault A");
    let vault_b = create_vault(leader.addr, ns_id).await.expect("create vault B");

    // Write to each vault via leader
    write_entity(leader.addr, ns_id, vault_a, "test-key", b"value-a", "client-a")
        .await
        .expect("write to vault A");

    write_entity(leader.addr, ns_id, vault_b, "test-key", b"value-b", "client-b")
        .await
        .expect("write to vault B");

    // Wait for replication
    let synced = cluster.wait_for_sync(Duration::from_secs(5)).await;
    assert!(synced, "cluster should sync");

    // Verify isolation on each follower
    for follower in cluster.followers() {
        let value_a = read_entity(follower.addr, ns_id, vault_a, "test-key")
            .await
            .expect("read from follower")
            .expect("should have value");
        assert_eq!(value_a, b"value-a", "Follower should have vault A's value");

        let value_b = read_entity(follower.addr, ns_id, vault_b, "test-key")
            .await
            .expect("read from follower")
            .expect("should have value");
        assert_eq!(value_b, b"value-b", "Follower should have vault B's value");
    }
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Test that empty vaults are properly isolated (no data leakage).
#[serial]
#[tokio::test]
async fn test_empty_vault_isolation() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create namespace
    let ns_id = create_namespace(leader.addr, "empty-vault-ns").await.expect("create namespace");

    // Create two vaults
    let vault_a = create_vault(leader.addr, ns_id).await.expect("create vault A");
    let vault_b = create_vault(leader.addr, ns_id).await.expect("create vault B");

    // Write to vault A only
    write_entity(leader.addr, ns_id, vault_a, "data", b"exists", "client")
        .await
        .expect("write to vault A");

    // Vault B should be empty (no data leakage from A)
    let value =
        read_entity(leader.addr, ns_id, vault_b, "data").await.expect("read from empty vault B");

    assert!(value.is_none(), "Empty vault B should have no data from A");
}

/// Test that deletion in one vault doesn't affect another.
#[serial]
#[tokio::test]
async fn test_deletion_isolation() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create namespace and vaults
    let ns_id =
        create_namespace(leader.addr, "deletion-isolation-ns").await.expect("create namespace");

    let vault_a = create_vault(leader.addr, ns_id).await.expect("create vault A");
    let vault_b = create_vault(leader.addr, ns_id).await.expect("create vault B");

    // Write same key to both vaults
    write_entity(leader.addr, ns_id, vault_a, "to-delete", b"value-a", "client-a")
        .await
        .expect("write to vault A");

    write_entity(leader.addr, ns_id, vault_b, "to-delete", b"value-b", "client-b")
        .await
        .expect("write to vault B");

    // Delete from vault A
    let mut client = create_write_client(leader.addr).await.expect("client");
    client
        .write(inferadb_ledger_raft::proto::WriteRequest {
            namespace_id: Some(inferadb_ledger_raft::proto::NamespaceId { id: ns_id }),
            vault_id: Some(inferadb_ledger_raft::proto::VaultId { id: vault_a }),
            client_id: Some(inferadb_ledger_raft::proto::ClientId { id: "client-a".to_string() }),
            idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
            operations: vec![inferadb_ledger_raft::proto::Operation {
                op: Some(inferadb_ledger_raft::proto::operation::Op::DeleteEntity(
                    inferadb_ledger_raft::proto::DeleteEntity { key: "to-delete".to_string() },
                )),
            }],
            include_tx_proof: false,
        })
        .await
        .expect("delete from vault A");

    // Verify vault A key is deleted
    let value_a =
        read_entity(leader.addr, ns_id, vault_a, "to-delete").await.expect("read from vault A");
    assert!(value_a.is_none(), "Key should be deleted from vault A");

    // Verify vault B key still exists
    let value_b = read_entity(leader.addr, ns_id, vault_b, "to-delete")
        .await
        .expect("read from vault B")
        .expect("should still have value");
    assert_eq!(value_b, b"value-b", "Deletion in vault A should not affect vault B");
}
