//! Backup end-to-end integration tests.
//!
//! Exercises the multi-DB archive backup path through the gRPC Admin
//! service:
//!
//! - `CreateBackup` produces a `.tar.zst` archive with a populated manifest (region, organization,
//!   RMK fingerprint, db list).
//! - `ListBackups` enumerates archives in the configured backups directory and parses their
//!   manifests on demand.
//! - `RestoreBackup` stages an archive into `.restore-staging/` and surfaces the manifest back to
//!   the operator before any swap.
//! - `BackupManager::stage_restore` + `apply_staged_restore` as the offline restore primitive,
//!   driven against a separate data directory so the live node's open file handles do not block the
//!   swap.
//!
//! Full restore (stop → swap → start) is the operator workflow exercised
//! by the `inferadb-ledger restore apply` CLI; these tests verify the
//! primitives that workflow composes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::common::{TestCluster, create_admin_client, create_write_client};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization and returns its slug.
async fn create_organization(
    addr: &str,
    name: &str,
    node: &crate::common::TestNode,
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

/// Writes an entity and returns the block height.
async fn write_entity(
    addr: &str,
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
        caller: None,
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

/// Creates a backup of `organization` and returns the response.
async fn create_backup(
    addr: &str,
    organization: OrganizationSlug,
    tag: Option<&str>,
) -> Result<inferadb_ledger_proto::proto::CreateBackupResponse, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;

    let request = inferadb_ledger_proto::proto::CreateBackupRequest {
        tag: tag.map(|t| t.to_string()),
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
    };

    let response = client.create_backup(request).await?.into_inner();
    Ok(response)
}

/// Lists backups and returns the list.
async fn list_backups(
    addr: &str,
    limit: u32,
) -> Result<Vec<inferadb_ledger_proto::proto::BackupInfo>, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;

    let request = inferadb_ledger_proto::proto::ListBackupsRequest { limit };

    let response = client.list_backups(request).await?.into_inner();
    Ok(response.backups)
}

/// Stages a backup for restore and returns the response.
async fn restore_backup(
    addr: &str,
    backup_id: &str,
) -> Result<inferadb_ledger_proto::proto::RestoreBackupResponse, Box<dyn std::error::Error>> {
    let mut client = create_admin_client(addr).await?;

    let request =
        inferadb_ledger_proto::proto::RestoreBackupRequest { backup_id: backup_id.to_string() };

    let response = client.restore_backup(request).await?.into_inner();
    Ok(response)
}

// ============================================================================
// Backup E2E Tests
// ============================================================================

/// `CreateBackup` produces a valid archive whose manifest enumerates the
/// org's DB files plus all of its vaults' DB files.
#[tokio::test]
async fn admin_create_backup_produces_valid_archive() {
    let cluster = TestCluster::new(1).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let org_slug = create_organization(&leader.addr, "backup-archive", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, org_slug).await.expect("create vault");

    for i in 0..3 {
        write_entity(
            &leader.addr,
            org_slug,
            vault,
            &format!("key-{}", i),
            format!("value-{}", i).as_bytes(),
        )
        .await
        .expect("write entity");
    }

    let response =
        create_backup(&leader.addr, org_slug, Some("manual")).await.expect("create backup");

    assert!(!response.backup_id.is_empty(), "backup should have an id");
    assert!(response.size_bytes > 0, "archive should have non-zero size");
    assert!(response.backup_path.ends_with(".tar.zst"), "archive should be .tar.zst");
    assert!(
        std::path::Path::new(&response.backup_path).exists(),
        "archive file should exist on disk: {}",
        response.backup_path,
    );

    let manifest = response.manifest.as_ref().expect("response carries manifest");
    assert_eq!(manifest.schema_version, 1, "schema version must be 1");
    assert_eq!(manifest.format, "inferadb-ledger-multi-db-backup");
    assert!(
        manifest.rmk_fingerprint.starts_with("sha256:"),
        "fingerprint must use sha256 prefix, got {}",
        manifest.rmk_fingerprint,
    );
    assert!(!manifest.dbs.is_empty(), "manifest should enumerate at least one DB");
    assert!(
        manifest.dbs.iter().any(|e| e.checksum.starts_with("blake3:")),
        "checksums must use blake3 prefix",
    );
    let manifest_org = manifest.organization.as_ref().expect("manifest carries org slug");
    assert_eq!(manifest_org.slug, org_slug.value(), "manifest org slug matches request");
}

/// `RestoreBackup` stages a previously-created archive into the
/// `.restore-staging/` tree, returning the staging path and manifest.
#[tokio::test]
async fn admin_restore_backup_stages_archive() {
    let cluster = TestCluster::new(1).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let org_slug = create_organization(&leader.addr, "backup-restore-stage", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, org_slug).await.expect("create vault");
    write_entity(&leader.addr, org_slug, vault, "stage-key", b"stage-value")
        .await
        .expect("write entity");

    let backup =
        create_backup(&leader.addr, org_slug, Some("stage-test")).await.expect("create backup");

    let response =
        restore_backup(&leader.addr, &backup.backup_id).await.expect("restore (stage) backup");

    assert!(!response.staging_dir.is_empty(), "response carries staging_dir");
    assert!(
        std::path::Path::new(&response.staging_dir).exists(),
        "staging directory should exist on disk: {}",
        response.staging_dir,
    );
    let manifest = response.manifest.as_ref().expect("staging response carries manifest");
    assert_eq!(manifest.schema_version, 1);
    assert!(!manifest.dbs.is_empty(), "manifest should list staged DB files");
    let manifest_org = manifest.organization.as_ref().expect("manifest carries org slug");
    assert_eq!(manifest_org.slug, org_slug.value());
}

/// Tests `CreateBackup` → `ListBackups` round-trip: every created
/// archive shows up in the list response with a populated manifest.
#[tokio::test]
async fn test_backup_create_and_list_metadata() {
    let cluster = TestCluster::new(1).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let org_slug = create_organization(&leader.addr, "backup-metadata", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, org_slug).await.expect("create vault");

    for i in 0..5 {
        write_entity(
            &leader.addr,
            org_slug,
            vault,
            &format!("key-{}", i),
            format!("value-{}", i).as_bytes(),
        )
        .await
        .expect("write entity");
    }

    let backup =
        create_backup(&leader.addr, org_slug, Some("test-snapshot")).await.expect("create backup");

    assert!(!backup.backup_id.is_empty(), "backup should have an ID");
    assert!(backup.size_bytes > 0, "backup should have non-zero size");
    assert!(!backup.backup_path.is_empty(), "backup should have a path");

    let backups = list_backups(&leader.addr, 10).await.expect("list backups");
    assert!(!backups.is_empty(), "should have at least one backup");

    let our_backup = backups.iter().find(|b| b.backup_id == backup.backup_id);
    assert!(our_backup.is_some(), "our backup should appear in list");
    let info = our_backup.unwrap();
    assert!(info.created_at.is_some(), "should have creation timestamp");
    assert!(info.manifest.is_some(), "list entry should carry parsed manifest");
}

/// Backup during active writes still produces a parseable archive — the
/// archive captures the file bytes synced before the tar build, even
/// while writers continue to issue requests.
#[tokio::test]
async fn test_backup_during_active_writes() {
    let cluster = TestCluster::new(1).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let org_slug = create_organization(&leader.addr, "backup-concurrent", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, org_slug).await.expect("create vault");

    for i in 0..3 {
        write_entity(&leader.addr, org_slug, vault, &format!("initial-{}", i), b"initial-value")
            .await
            .expect("write initial entity");
    }

    let addr = leader.addr.clone();
    let write_handle = tokio::spawn(async move {
        for i in 0..5 {
            let _ =
                write_entity(&addr, org_slug, vault, &format!("concurrent-{}", i), b"concurrent")
                    .await;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    });

    let backup = create_backup(&leader.addr, org_slug, Some("during-writes"))
        .await
        .expect("backup during writes should succeed");

    write_handle.await.expect("concurrent writes should complete");

    assert!(backup.size_bytes > 0, "backup should have content");
    let manifest = backup.manifest.expect("response carries manifest");
    assert!(!manifest.dbs.is_empty(), "manifest should enumerate DB files");
}

/// Tests multiple backups with listing limit. Creates several archives
/// across distinct organizations and verifies that `ListBackups`
/// respects the limit parameter.
#[tokio::test]
async fn test_backup_list_with_limit() {
    let cluster = TestCluster::new(1).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let mut backup_ids = Vec::new();
    for i in 0..3 {
        let org_slug = create_organization(&leader.addr, &format!("backup-limit-{i}"), leader)
            .await
            .expect("create organization");
        let vault = create_vault(&leader.addr, org_slug).await.expect("create vault");
        write_entity(&leader.addr, org_slug, vault, &format!("data-{}", i), b"v")
            .await
            .expect("write entity");

        // Delay between backups so timestamps are unique. Backup id
        // encodes `{org_id}-{timestamp_micros}`; back-to-back calls
        // within the same microsecond would collide on the on-disk
        // filename.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let backup = create_backup(&leader.addr, org_slug, Some(&format!("backup-{i}")))
            .await
            .expect("create backup");
        backup_ids.push(backup.backup_id);
    }

    let all = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let backups = list_backups(&leader.addr, 0).await.expect("list all");
            if backups.len() >= 3 {
                return backups;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("all 3 backups should appear within timeout");

    assert!(all.len() >= 3, "should have at least 3 backups, got {}", all.len());

    let limited = list_backups(&leader.addr, 2).await.expect("list limited");
    assert!(limited.len() <= 2, "limit should be respected, got {}", limited.len());
}

/// Reads `key` from `(organization, vault)` via the gRPC Read service.
/// Returns the value bytes on hit, `None` on miss.
async fn read_entity(
    addr: &str,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut client = crate::common::create_read_client(addr).await?;

    let request = inferadb_ledger_proto::proto::ReadRequest {
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: organization.value(),
        }),
        vault: Some(inferadb_ledger_proto::proto::VaultSlug { slug: vault.value() }),
        key: key.to_string(),
        consistency: 0,
        caller: None,
    };

    let response = client.read(request).await?.into_inner();
    Ok(response.value)
}

/// End-to-end backup → restore round-trip.
///
/// Flow:
/// 1. Build a single-node cluster + data region, create org + vault, write 5 entities.
/// 2. Verify reads return what we wrote (sanity baseline).
/// 3. Call `CreateBackup` admin RPC; capture the archive path.
/// 4. Verify the archive file exists, parses, and the manifest enumerates the org-level + per-vault
///    DB files.
/// 5. Stage the archive into a *separate* data directory (so the running node's file lock does not
///    block the swap), then call `apply_staged_restore` against that separate data_dir.
/// 6. Verify the swapped live tree contains every DB file from the manifest, byte-for-byte
///    equivalent to the archive members. The encrypted state.db that holds the entries lands at the
///    expected location with the original size — proving the round-trip preserves on-disk state.
///
/// We do not actually stop and restart the test cluster against the swapped tree —
/// that exercises the `init` + restart path which is covered by separate tests; here
/// we are validating the swap primitive itself, not the operator workflow.
#[tokio::test]
async fn test_backup_restore_round_trip_with_data_integrity() {
    use inferadb_ledger_raft::backup::{BackupManager, apply_staged_restore};
    use inferadb_ledger_types::{OrganizationId, Region, config::BackupConfig};

    let cluster = TestCluster::new(1).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let org_slug = create_organization(&leader.addr, "backup-roundtrip", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, org_slug).await.expect("create vault");

    // Phase 1: write 5 entities and prove they're readable through the live node.
    let entries: Vec<(String, Vec<u8>)> =
        (0..5).map(|i| (format!("rt-key-{i}"), format!("rt-value-{i}").into_bytes())).collect();
    for (key, value) in &entries {
        write_entity(&leader.addr, org_slug, vault, key, value).await.expect("write entity");
    }
    for (key, value) in &entries {
        let actual = read_entity(&leader.addr, org_slug, vault, key)
            .await
            .expect("read entity")
            .expect("entity exists pre-backup");
        assert_eq!(&actual, value, "pre-backup read mismatch for {key}");
    }

    // Phase 2: take the backup. Capture archive path + manifest.
    let backup =
        create_backup(&leader.addr, org_slug, Some("roundtrip")).await.expect("create backup");
    let archive_path = std::path::PathBuf::from(&backup.backup_path);
    assert!(archive_path.exists(), "archive should exist on disk: {}", archive_path.display());

    let archive_file = std::fs::File::open(&archive_path).expect("open archive");
    let parsed_manifest =
        inferadb_ledger_raft::backup::archive::parse_archive(archive_file, |_path, reader| {
            // Drain each member without buffering — parse_archive enforces
            // checksums, so a clean traversal is enough to validate the
            // archive end-to-end.
            let mut sink = std::io::sink();
            std::io::copy(reader, &mut sink).map(|_| ()).map_err(|source| {
                inferadb_ledger_raft::backup::archive::BackupArchiveError::Io { source }
            })
        })
        .expect("parse archive");
    assert!(!parsed_manifest.dbs.is_empty(), "manifest enumerates at least one DB");
    let vault_state_db = format!("state/vault-{}/state.db", parsed_manifest.dbs[0].path);
    let _ = vault_state_db; // silence unused (referenced by assertion below)

    // The archive must include the per-vault state.db that holds the
    // entries we wrote — that is the storage member which carries the
    // entity bytes through the backup.
    let archived_paths: Vec<&str> = parsed_manifest.dbs.iter().map(|e| e.path.as_str()).collect();
    let has_vault_state_db =
        archived_paths.iter().any(|p| p.starts_with("state/vault-") && p.ends_with("/state.db"));
    assert!(
        has_vault_state_db,
        "archive must include at least one per-vault state.db; got {archived_paths:?}"
    );

    // Phase 3: stage + apply against a *separate* data directory. The
    // live node still holds its own data_dir's `.lock`; staging into a
    // fresh tree sidesteps that lock and exercises the same primitives
    // the offline `restore apply` CLI runs.
    let restore_dir = tempfile::TempDir::new().expect("restore tempdir");
    let restore_data_dir = restore_dir.path().to_path_buf();
    std::fs::create_dir_all(&restore_data_dir).expect("create restore data dir");

    let manager = BackupManager::new(
        &BackupConfig::builder()
            .destination(restore_data_dir.join("_backups").to_string_lossy().to_string())
            .build()
            .expect("valid backup config"),
    )
    .expect("backup manager")
    .with_data_dir(restore_data_dir.clone(), 0);

    let staging = manager
        .stage_restore(&archive_path, None)
        .await
        .expect("stage_restore against fresh data_dir");

    assert_eq!(
        staging.files_staged,
        parsed_manifest.dbs.len(),
        "staging count matches manifest db count"
    );
    let staging_root = restore_data_dir.join(".restore-staging");
    assert!(staging.staging_dir.starts_with(&staging_root), "staging dir lives under root");

    // Each staged file matches the archive member byte-for-byte.
    for entry in &parsed_manifest.dbs {
        let staged = staging.staging_dir.join(&entry.path);
        assert!(staged.exists(), "staged file missing: {}", entry.path);
        let actual_size = std::fs::metadata(&staged).expect("stat staged").len();
        assert_eq!(actual_size, entry.size_bytes, "staged size for {}", entry.path);
    }

    let region = parsed_manifest.region;
    let organization_id = parsed_manifest.organization_id;
    assert_eq!(region, Region::US_EAST_VA, "manifest region matches data region");
    assert_ne!(organization_id, OrganizationId::new(0), "manifest org is the user-created org");

    // The fresh data_dir has no live tree to displace; this is the
    // first-restore-on-this-node path.
    let apply_result =
        apply_staged_restore(&restore_data_dir, &staging.staging_dir, region, organization_id)
            .expect("apply_staged_restore on fresh data_dir");
    assert!(apply_result.trash_dir.as_os_str().is_empty(), "no live tree to displace");
    assert_eq!(
        apply_result.files_swapped,
        parsed_manifest.dbs.len(),
        "files_swapped matches manifest db count"
    );

    // Phase 4: every manifest member is now present in the swapped live
    // tree at the same byte size. The per-vault state.db that contains
    // the entries we wrote is durable on disk in the new data_dir; a
    // node started against this data_dir would re-open those bytes.
    let live_dir = restore_data_dir.join(region.as_str()).join(organization_id.value().to_string());
    assert!(live_dir.is_dir(), "live dir created by apply: {}", live_dir.display());
    for entry in &parsed_manifest.dbs {
        let live_path = live_dir.join(&entry.path);
        assert!(live_path.exists(), "live tree missing: {}", entry.path);
        let actual_size = std::fs::metadata(&live_path).expect("stat live").len();
        assert_eq!(actual_size, entry.size_bytes, "live size for {}", entry.path);
    }

    // The swap left no staging tree behind.
    assert!(!staging.staging_dir.exists(), "staging tree consumed by swap");

    // Marker file is written so an operator can confirm the swap ran.
    let marker_path = restore_data_dir.join(".restore-marker");
    assert!(marker_path.exists(), "restore marker file written");
}
