//! Wire types for `AdminService`.
//!
//! Covers cluster membership, maintenance ops, backup/restore, blinding key
//! rotation, region key rotation, user migration, region provisioning,
//! per-vault inspection, and runtime config.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::services::shared::{
    ClusterMember, Hash, OrganizationSlug, UserSlug, VaultHealthProto, VaultSlug,
};

// ---------------------------------------------------------------------------
// Cluster membership
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinClusterRequest {
    pub node_id: u64,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinClusterResponse {
    pub success: bool,
    pub message: String,
    pub leader_id: u64,
    pub leader_address: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaveClusterRequest {
    pub node_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaveClusterResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetDecommissionStatusRequest {
    pub node_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataRegionReplica {
    pub region: String,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetDecommissionStatusResponse {
    pub status: String,
    pub remaining: Vec<DataRegionReplica>,
    pub global_removed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckPeerLivenessRequest {
    pub target_node_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckPeerLivenessResponse {
    pub reachable: bool,
    pub last_seen_ago_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetClusterInfoRequest {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetClusterInfoResponse {
    pub members: Vec<ClusterMember>,
    pub leader_id: u64,
    pub term: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetNodeInfoRequest {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetNodeInfoResponse {
    pub node_id: u64,
    pub address: String,
    pub is_cluster_member: bool,
    pub term: u64,
    pub cluster_id: u64,
    pub state: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InitClusterRequest {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InitClusterResponse {
    pub initialized: bool,
    pub cluster_id: u64,
    pub already_initialized: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransferLeadershipRequest {
    pub target_node_id: u64,
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransferLeadershipResponse {
    pub success: bool,
    pub new_leader_id: u64,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Maintenance operations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateSnapshotRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateSnapshotResponse {
    pub block_height: u64,
    pub state_root: Option<Hash>,
    pub snapshot_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckIntegrityRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub full_check: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckIntegrityResponse {
    pub healthy: bool,
    pub issues: Vec<IntegrityIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IntegrityIssue {
    pub block_height: u64,
    pub issue_type: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecoverVaultRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecoverVaultResponse {
    pub success: bool,
    pub message: String,
    pub health_status: VaultHealthProto,
    pub final_height: u64,
    pub final_state_root: Option<Hash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SimulateDivergenceRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub expected_state_root: Option<Hash>,
    pub computed_state_root: Option<Hash>,
    pub at_height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SimulateDivergenceResponse {
    pub success: bool,
    pub message: String,
    pub health_status: VaultHealthProto,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ForceGcRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ForceGcResponse {
    pub success: bool,
    pub message: String,
    pub expired_count: u64,
    pub vaults_scanned: u64,
}

// ---------------------------------------------------------------------------
// Runtime configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateConfigRequest {
    pub config_json: String,
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateConfigResponse {
    pub applied: bool,
    pub message: String,
    pub current_config_json: String,
    pub changed_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetConfigRequest {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetConfigResponse {
    pub config_json: String,
}

// ---------------------------------------------------------------------------
// Backup / Restore
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BackupDbEntry {
    pub path: String,
    pub size_bytes: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BackupManifest {
    pub schema_version: u32,
    pub format: String,
    pub region: String,
    pub organization: Option<OrganizationSlug>,
    pub organization_id: i64,
    /// Microseconds since UNIX epoch (mirrors the proto field literally).
    pub timestamp_micros: i64,
    pub rmk_fingerprint: String,
    pub node_id_at_creation: u64,
    pub dbs: Vec<BackupDbEntry>,
    pub vault_count: u32,
    pub created_by_app_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateBackupRequest {
    pub tag: Option<String>,
    pub organization: Option<OrganizationSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateBackupResponse {
    pub backup_id: String,
    pub backup_path: String,
    pub size_bytes: u64,
    pub manifest: Option<BackupManifest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListBackupsRequest {
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListBackupsResponse {
    pub backups: Vec<BackupInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BackupInfo {
    pub backup_id: String,
    pub backup_path: String,
    pub size_bytes: u64,
    /// UNIX nanoseconds.
    pub created_at: u64,
    pub tag: String,
    pub manifest: Option<BackupManifest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RestoreBackupRequest {
    pub backup_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RestoreBackupResponse {
    pub staging_dir: String,
    pub manifest: Option<BackupManifest>,
}

// ---------------------------------------------------------------------------
// Blinding key rotation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RotateBlindingKeyRequest {
    pub new_key_version: u32,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RotateBlindingKeyResponse {
    pub total_entries: u64,
    pub entries_rehashed: u64,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetBlindingKeyRehashStatusRequest {
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetBlindingKeyRehashStatusResponse {
    pub total_entries: u64,
    pub entries_rehashed: u64,
    pub complete: bool,
    pub per_region_progress: BTreeMap<String, u64>,
    pub active_key_version: u32,
}

// ---------------------------------------------------------------------------
// Region key rotation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RotateRegionKeyRequest {
    pub region: String,
    pub target_version: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RotateRegionKeyResponse {
    pub new_version: u32,
    pub total_pages: u64,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetRewrapStatusRequest {
    pub region: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetRewrapStatusResponse {
    pub total_pages: u64,
    pub pages_processed: u64,
    pub pages_rewrapped: u64,
    pub complete: bool,
    pub target_version: u32,
    pub estimated_remaining_secs: u64,
}

// ---------------------------------------------------------------------------
// User migration / Region provisioning
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MigrateExistingUsersRequest {
    pub default_region: String,
    pub email_blinding_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrateExistingUsersResponse {
    pub users: u64,
    pub migrated: u64,
    pub skipped: u64,
    pub errors: u64,
    pub elapsed_secs: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProvisionRegionRequest {
    pub name: String,
    pub protected: bool,
    pub requires_residency: bool,
    pub retention_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProvisionRegionResponse {
    pub created: bool,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RegionDirectoryEntry {
    pub name: String,
    pub protected: bool,
    pub conf_epoch: u64,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    /// UNIX nanoseconds.
    pub created_at: u64,
    pub requires_residency: bool,
    pub retention_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SetRegionResidencyRequest {
    pub name: String,
    pub requires_residency: bool,
    pub retention_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SetRegionResidencyResponse {
    pub name: String,
    pub requires_residency: bool,
    pub retention_days: u32,
}

// ---------------------------------------------------------------------------
// Per-vault inspection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AdminVaultInfo {
    pub slug: Option<VaultSlug>,
    pub status: String,
    pub leader_node_id: u64,
    pub voter_count: u64,
    pub learner_count: u64,
    pub last_applied_index: u64,
    /// UNIX nanoseconds.
    pub last_activity: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AdminListVaultsRequest {
    pub organization: Option<OrganizationSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AdminListVaultsResponse {
    pub vaults: Vec<AdminVaultInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShowVaultRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShowVaultResponse {
    pub info: Option<AdminVaultInfo>,
    pub region: String,
    pub organization_id: u64,
    pub vault_id: u64,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub conf_epoch: u64,
    pub lifecycle_state: String,
    /// UNIX nanoseconds.
    pub last_activity_at: Option<u64>,
    /// UNIX nanoseconds.
    pub pending_membership_started_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RepairVaultRequest {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RepairVaultResponse {
    pub status: String,
    pub message: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn create_backup_response_with_manifest_roundtrips() {
        let manifest = BackupManifest {
            schema_version: 1,
            format: "ledger.backup.v1".to_owned(),
            region: "us-east-va".to_owned(),
            organization: Some(OrganizationSlug::new(42)),
            organization_id: 7,
            timestamp_micros: 1_700_000_000_000_000,
            rmk_fingerprint: "sha256:abcdef".to_owned(),
            node_id_at_creation: 1,
            dbs: vec![BackupDbEntry {
                path: "_meta.db".to_owned(),
                size_bytes: 4096,
                checksum: "blake3:deadbeef".to_owned(),
            }],
            vault_count: 0,
            created_by_app_version: "1.0.0".to_owned(),
        };
        let resp = CreateBackupResponse {
            backup_id: "backup-123".to_owned(),
            backup_path: "/var/backups/backup-123.tar.zst".to_owned(),
            size_bytes: 8192,
            manifest: Some(manifest),
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: CreateBackupResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }
}
