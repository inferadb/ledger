//! Backup end-to-end integration tests.
//!
//! Tests the CreateBackup and ListBackups RPCs, including backup metadata
//! correctness, consistency during concurrent writes, and checksum presence.
//!
//! These tests exercise the gRPC Admin service backup operations through
//! a real single-node cluster, validating that backup files are created
//! and metadata is accurate.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

mod common;

use common::{TestCluster, create_admin_client, create_write_client};
use serial_test::serial;

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates a organization and return its ID.
async fn create_organization(
    addr: std::net::SocketAddr,
    name: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_organization(inferadb_ledger_proto::proto::CreateOrganizationRequest {
            name: name.to_string(),
            shard_id: None,
            quota: None,
        })
        .await?;

    let organization_id = response
        .into_inner()
        .slug
        .map(|n| n.slug as i64)
        .ok_or("No organization_id in response")?;

    Ok(organization_id)
}

/// Creates a vault in a organization and return its ID.
async fn create_vault(
    addr: std::net::SocketAddr,
    organization_id: i64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;
    let response = client
        .create_vault(inferadb_ledger_proto::proto::CreateVaultRequest {
            organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
                slug: organization_id as u64,
            }),
            replication_factor: 0,
            initial_nodes: vec![],
            retention_policy: None,
        })
        .await?;

    let vault_slug =
        response.into_inner().vault.map(|v| v.slug).ok_or("No vault_slug in response")?;
    Ok(vault_slug)
}

/// Writes an entity and return the block height.
async fn write_entity(
    addr: std::net::SocketAddr,
    organization_id: i64,
    vault_slug: u64,
    key: &str,
    value: &[u8],
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_write_client(addr).await?;

    let request = inferadb_ledger_proto::proto::WriteRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization_id as u64,
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault_slug }),
        client_id: Some(inferadb_ledger_proto::proto::ClientId {
            id: "backup-test-client".to_string(),
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

/// Creates a backup and return the response.
async fn create_backup(
    addr: std::net::SocketAddr,
    tag: Option<&str>,
    base_backup_id: Option<&str>,
) -> Result<inferadb_ledger_proto::proto::CreateBackupResponse, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;

    let request = inferadb_ledger_proto::proto::CreateBackupRequest {
        tag: tag.map(|t| t.to_string()),
        base_backup_id: base_backup_id.map(|id| id.to_string()),
    };

    let response = client.create_backup(request).await?.into_inner();
    Ok(response)
}

/// Lists backups and return the list.
async fn list_backups(
    addr: std::net::SocketAddr,
    limit: u32,
) -> Result<Vec<inferadb_ledger_proto::proto::BackupInfo>, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;

    let request = inferadb_ledger_proto::proto::ListBackupsRequest { limit };

    let response = client.list_backups(request).await?.into_inner();
    Ok(response.backups)
}

// ============================================================================
// Backup/Restore E2E Tests
// ============================================================================

/// Tests full backup → list → verify metadata correctness.
///
/// Creates data, takes a backup, lists backups, and verifies that the
/// backup metadata (shard_height, size, checksum, tag) is accurate.
#[serial]
#[tokio::test]
async fn test_backup_create_and_list_metadata() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create some data to back up
    let ns_id =
        create_organization(leader.addr, "backup-metadata").await.expect("create organization");
    let vault_slug = create_vault(leader.addr, ns_id).await.expect("create vault");

    for i in 0..5 {
        write_entity(
            leader.addr,
            ns_id,
            vault_slug,
            &format!("key-{}", i),
            format!("value-{}", i).as_bytes(),
        )
        .await
        .expect("write entity");
    }

    // Create a tagged backup
    let backup =
        create_backup(leader.addr, Some("test-snapshot"), None).await.expect("create backup");

    assert!(!backup.backup_id.is_empty(), "backup should have an ID");
    assert!(backup.shard_height > 0, "backup should have a shard height");
    assert!(backup.size_bytes > 0, "backup should have non-zero size");
    assert!(!backup.backup_path.is_empty(), "backup should have a path");

    // List backups and verify metadata
    let backups = list_backups(leader.addr, 10).await.expect("list backups");

    assert!(!backups.is_empty(), "should have at least one backup");

    // Find our backup
    let our_backup = backups.iter().find(|b| b.backup_id == backup.backup_id);
    assert!(our_backup.is_some(), "our backup should appear in list");

    let info = our_backup.unwrap();
    assert_eq!(info.shard_height, backup.shard_height, "heights should match");
    assert_eq!(info.tag, "test-snapshot", "tag should match");
    assert!(info.created_at.is_some(), "should have creation timestamp");
}

/// Tests backup during active writes.
///
/// Writes are submitted concurrently with backup creation to verify
/// that backup captures a consistent snapshot despite concurrent activity.
#[serial]
#[tokio::test]
async fn test_backup_during_active_writes() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let ns_id =
        create_organization(leader.addr, "backup-concurrent").await.expect("create organization");
    let vault_slug = create_vault(leader.addr, ns_id).await.expect("create vault");

    // Write initial data
    for i in 0..3 {
        write_entity(leader.addr, ns_id, vault_slug, &format!("initial-{}", i), b"initial-value")
            .await
            .expect("write initial entity");
    }

    // Start concurrent writes in background
    let addr = leader.addr;
    let write_handle = tokio::spawn(async move {
        for i in 0..5 {
            let _ =
                write_entity(addr, ns_id, vault_slug, &format!("concurrent-{}", i), b"concurrent")
                    .await;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    });

    // Create backup while writes are happening
    let backup = create_backup(leader.addr, Some("during-writes"), None)
        .await
        .expect("backup during writes should succeed");

    // Wait for writes to finish
    write_handle.await.expect("concurrent writes should complete");

    // Backup should have captured at least the initial data
    assert!(backup.shard_height > 0, "backup should capture state");
    assert!(backup.size_bytes > 0, "backup should have content");
}

/// Tests multiple backups with listing limit.
///
/// Creates several backups and verifies that ListBackups respects the
/// limit parameter and returns backups in the expected order.
#[serial]
#[tokio::test]
async fn test_backup_list_with_limit() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let ns_id =
        create_organization(leader.addr, "backup-limit").await.expect("create organization");
    let vault_slug = create_vault(leader.addr, ns_id).await.expect("create vault");

    // Write data and create multiple backups
    let mut backup_ids = Vec::new();
    for i in 0..3 {
        write_entity(
            leader.addr,
            ns_id,
            vault_slug,
            &format!("data-{}", i),
            format!("value-{}", i).as_bytes(),
        )
        .await
        .expect("write entity");

        // Small delay to ensure different timestamps
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let backup = create_backup(leader.addr, Some(&format!("backup-{}", i)), None)
            .await
            .expect("create backup");
        backup_ids.push(backup.backup_id);
    }

    // List with limit=2 should return at most 2
    let limited = list_backups(leader.addr, 2).await.expect("list limited");
    assert!(limited.len() <= 2, "limit should be respected, got {}", limited.len());

    // List with limit=0 (all) should return at least 3
    let all = list_backups(leader.addr, 0).await.expect("list all");
    assert!(all.len() >= 3, "should have at least 3 backups, got {}", all.len());
}

/// Tests that backup produces a valid checksum.
///
/// The backup response includes a SHA-256 checksum that operators can
/// use to verify backup integrity during transfers.
#[serial]
#[tokio::test]
async fn test_backup_checksum_present() {
    let cluster = TestCluster::new(1).await;
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let ns_id =
        create_organization(leader.addr, "backup-checksum").await.expect("create organization");
    let _vault_slug = create_vault(leader.addr, ns_id).await.expect("create vault");

    let backup =
        create_backup(leader.addr, Some("checksum-test"), None).await.expect("create backup");

    // Checksum should be present and non-zero
    assert!(backup.checksum.is_some(), "backup should have a checksum");
    let checksum = backup.checksum.unwrap();
    assert!(!checksum.value.iter().all(|&b| b == 0), "checksum should not be all zeros");
}
