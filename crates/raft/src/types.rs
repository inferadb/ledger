//! Core types for OpenRaft integration.
//!
//! This module defines the type configuration for OpenRaft, including:
//! - Node identification
//! - Log entry format
//! - Response types
//! - Snapshot data format

use std::fmt;

use chrono::{DateTime, Utc};
// Re-export domain types that originated here but now live in types crate.
pub use inferadb_ledger_types::{BlockRetentionMode, BlockRetentionPolicy, LedgerNodeId};
use inferadb_ledger_types::{
    Hash, OrganizationId, OrganizationSlug, Region, SetCondition, Transaction, UserId, UserSlug,
    VaultId, VaultSlug,
};
use openraft::{BasicNode, impls::OneshotResponder};
use serde::{Deserialize, Serialize};

// Use the declare_raft_types macro for type configuration.
// This macro generates a `LedgerTypeConfig` struct that implements `RaftTypeConfig`.
//
// Type parameters:
// - `D`: Application data (LedgerRequest)
// - `R`: Application response (LedgerResponse)
// - `NodeId`: Node identifier type (u64)
// - `Node`: Node metadata (BasicNode with address info)
// - `Entry`: Log entry format (default Entry)
// - `SnapshotData`: Snapshot format (file-based streaming with zstd compression)
// - `AsyncRuntime`: Tokio runtime
// - `Responder`: One-shot channel responder
// ============================================================================
// State Root Commitment
// ============================================================================

/// A leader's computed state root for a vault at a specific height.
///
/// After applying an entry, each node records the state root it computed.
/// The leader attaches its commitments to the *next* `RaftPayload`
/// (piggybacking on entry N+1 for entry N's roots). All nodes verify
/// the commitment against their own archived state root during apply,
/// detecting divergence without extra RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRootCommitment {
    /// Organization owning the vault.
    pub organization: OrganizationId,
    /// Vault whose state root was computed.
    pub vault: VaultId,
    /// Block height at which the state root was computed.
    pub vault_height: u64,
    /// SHA-256 state root the leader computed after applying this block.
    pub state_root: Hash,
}

/// A detected state root divergence between the local node and the leader.
///
/// Sent through a channel from the apply path to the divergence handler,
/// which proposes `UpdateVaultHealth { healthy: false }` via Raft to halt
/// the vault cluster-wide.
#[derive(Debug, Clone)]
pub struct StateRootDivergence {
    /// Organization containing the diverged vault.
    pub organization: OrganizationId,
    /// Vault that diverged.
    pub vault: VaultId,
    /// Block height at which divergence was detected.
    pub vault_height: u64,
    /// State root the local node computed.
    pub local_state_root: Hash,
    /// State root the leader committed.
    pub leader_state_root: Hash,
}

// ============================================================================
// Raft Payload Wrapper
// ============================================================================

/// Wraps a [`LedgerRequest`] with a leader-assigned wall-clock timestamp.
///
/// The leader stamps `proposed_at` at proposal time (`client_write`), and all
/// replicas use this value during apply — guaranteeing byte-identical event
/// timestamps, B+ tree keys, and pagination cursors across the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftPayload {
    /// The application-level request.
    pub request: LedgerRequest,
    /// Leader-assigned wall-clock timestamp at proposal time.
    pub proposed_at: DateTime<Utc>,
    /// Leader's state root commitments from the previous apply batch.
    ///
    /// Piggybacked on entry N+1 for entry N's vault state roots.
    /// Followers verify these against their locally computed roots to
    /// detect state machine divergence.
    #[serde(default)]
    pub state_root_commitments: Vec<StateRootCommitment>,
}

impl RaftPayload {
    /// Creates a payload with no state root commitments.
    ///
    /// Used by all proposal sites except the leader's write path, which
    /// drains the commitment buffer via [`Self::with_commitments`].
    pub fn new(request: LedgerRequest) -> Self {
        Self { request, proposed_at: chrono::Utc::now(), state_root_commitments: vec![] }
    }

    /// Creates a payload carrying piggybacked state root commitments.
    ///
    /// The leader drains its commitment buffer and attaches the results
    /// to the next `RaftPayload` so followers can verify state roots
    /// without extra RPCs.
    pub fn with_commitments(
        request: LedgerRequest,
        state_root_commitments: Vec<StateRootCommitment>,
    ) -> Self {
        Self { request, proposed_at: chrono::Utc::now(), state_root_commitments }
    }
}

openraft::declare_raft_types!(
    /// Ledger Raft type configuration.
    pub LedgerTypeConfig:
        D = RaftPayload,
        R = LedgerResponse,
        NodeId = LedgerNodeId,
        Node = BasicNode,
        Entry = openraft::Entry<LedgerTypeConfig>,
        SnapshotData = tokio::fs::File,
        AsyncRuntime = openraft::TokioRuntime,
        Responder = OneshotResponder<LedgerTypeConfig>
);

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request to the Raft state machine.
///
/// This is the "D" (data) type in OpenRaft's type configuration.
/// Each request targets a specific organization and vault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgerRequest {
    /// Writes transactions to a vault.
    Write {
        /// Target organization.
        organization: OrganizationId,
        /// Target vault within the organization.
        vault: VaultId,
        /// Transactions to apply atomically.
        transactions: Vec<Transaction>,
        /// Idempotency key (16-byte UUID) for cross-failover deduplication.
        /// Stored in the replicated `ClientSequenceEntry` so new leaders can
        /// detect retries without the moka cache.
        #[serde(default)]
        idempotency_key: [u8; 16],
        /// Hash of the request payload (seahash) for detecting key reuse
        /// with different payloads after failover.
        #[serde(default)]
        request_hash: u64,
    },

    /// Creates a new organization (applied to `_system`).
    CreateOrganization {
        /// Requested organization name.
        name: String,
        /// External slug for API lookups (generated before Raft proposal).
        slug: OrganizationSlug,
        /// Target data residency region (required, must not be GLOBAL).
        region: Region,
        /// Billing tier (defaults to Free).
        #[serde(default)]
        tier: inferadb_ledger_state::system::OrganizationTier,
    },

    /// Creates a new vault within an organization.
    CreateVault {
        /// Organization to create the vault in.
        organization: OrganizationId,
        /// External slug for API lookups (generated before Raft proposal).
        slug: VaultSlug,
        /// Optional vault name (for display).
        name: Option<String>,
        /// Block retention policy for this vault.
        /// Defaults to Full retention if not specified.
        retention_policy: Option<BlockRetentionPolicy>,
    },

    /// Deletes an organization.
    DeleteOrganization {
        /// Organization ID to delete.
        organization: OrganizationId,
    },

    /// Deletes a vault.
    DeleteVault {
        /// Organization containing the vault.
        organization: OrganizationId,
        /// Vault ID to delete.
        vault: VaultId,
    },

    /// Suspends an organization (billing hold or policy violation).
    /// Suspended organizations reject writes but allow reads.
    SuspendOrganization {
        /// Organization to suspend.
        organization: OrganizationId,
        /// Optional reason for suspension (e.g., "Payment overdue", "TOS violation").
        reason: Option<String>,
    },

    /// Resumes a suspended organization.
    ResumeOrganization {
        /// Organization to resume.
        organization: OrganizationId,
    },

    /// Starts organization migration to a new region.
    /// Sets status to Migrating, blocking writes until CompleteMigration.
    StartMigration {
        /// Organization to migrate.
        organization: OrganizationId,
        /// Target region for migration.
        target_region_group: Region,
    },

    /// Completes a pending organization migration.
    /// Updates region and returns status to Active.
    CompleteMigration {
        /// Organization being migrated.
        organization: OrganizationId,
    },

    /// Updates vault health status (used during recovery).
    UpdateVaultHealth {
        /// Organization containing the vault.
        organization: OrganizationId,
        /// Vault ID to update.
        vault: VaultId,
        /// New health status: true = Healthy, false = Diverged/Recovering.
        healthy: bool,
        /// If diverged, the expected state root.
        expected_root: Option<Hash>,
        /// If diverged, the computed state root.
        computed_root: Option<Hash>,
        /// If diverged, the height at which divergence was detected.
        diverged_at_height: Option<u64>,
        /// If recovering, the recovery attempt number (1-based).
        recovery_attempt: Option<u8>,
        /// If recovering, the start timestamp (Unix seconds).
        recovery_started_at: Option<i64>,
    },

    /// System operation (user management, node membership, etc.).
    System(SystemRequest),

    /// Batches of requests to apply atomically in a single Raft entry.
    ///
    /// Application-level batching coalesces multiple write requests into a
    /// single Raft proposal to reduce consensus round-trips and improve
    /// throughput.
    ///
    /// Each inner request is processed sequentially, and responses are
    /// returned in the same order via `LedgerResponse::BatchWrite`.
    BatchWrite {
        /// The requests to process.
        requests: Vec<LedgerRequest>,
    },
}

/// System-level requests that modify `_system` organization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemRequest {
    /// Creates a new user (global control-plane entry only).
    ///
    /// PII fields (name, email) are excluded — the global control plane must
    /// not contain plaintext personal data. User PII is written directly to
    /// the regional store.
    CreateUser {
        /// Pre-allocated user ID from the saga orchestrator.
        ///
        /// The saga allocates this ID via CAS sequence in step 0 and writes it
        /// to the email-hash index. The state machine must use this exact ID
        /// (not allocate a new one) to keep the slug index consistent with
        /// the email-hash index.
        user: UserId,
        /// Whether this user is a global service administrator.
        admin: bool,
        /// External Snowflake identifier.
        slug: UserSlug,
        /// Data residency region for the user's PII.
        region: Region,
    },

    /// Adds a node to the cluster.
    AddNode {
        /// Numeric node ID.
        node_id: LedgerNodeId,
        /// Node's gRPC address.
        address: String,
    },

    /// Removes a node from the cluster.
    RemoveNode {
        /// Node ID to remove.
        node_id: LedgerNodeId,
    },

    /// Updates organization-to-region mapping.
    UpdateOrganizationRouting {
        /// Organization to update.
        organization: OrganizationId,
        /// New region assignment.
        region: Region,
    },

    /// Registers an email HMAC hash in the global control plane.
    /// Uses CAS (`MustNotExist`) for uniqueness enforcement.
    RegisterEmailHash {
        /// Hex-encoded HMAC-SHA256 of the normalized email.
        hmac_hex: String,
        /// User ID to associate with this email hash.
        user_id: UserId,
    },

    /// Removes an email HMAC hash from the global control plane.
    RemoveEmailHash {
        /// Hex-encoded HMAC-SHA256 to remove.
        hmac_hex: String,
    },

    /// Sets the active email blinding key version.
    SetBlindingKeyVersion {
        /// New active key version number.
        version: u32,
    },

    /// Updates rehash progress for a region during blinding key rotation.
    UpdateRehashProgress {
        /// Region whose progress is being updated.
        region: Region,
        /// Number of entries rehashed so far.
        entries_rehashed: u64,
    },

    /// Clears rehash progress for a region (rotation complete for that region).
    ClearRehashProgress {
        /// Region whose progress is being cleared.
        region: Region,
    },

    /// Updates a user's directory entry status and optionally their region.
    /// Used during user region migration (mark Migrating, update region, revert).
    UpdateUserDirectoryStatus {
        /// User whose directory entry to update.
        user_id: UserId,
        /// New directory status.
        status: inferadb_ledger_state::system::UserDirectoryStatus,
        /// If `Some`, update the region. If `None`, keep current region.
        region: Option<Region>,
    },

    /// Erases a user's PII via crypto-shredding.
    ///
    /// Forward-only finalization: destroys the per-subject encryption key,
    /// scrubs the directory entry, records an erasure audit trail, and
    /// marks the user for snapshot tombstoning. Each step is idempotent
    /// but irreversible.
    EraseUser {
        /// User whose data to erase.
        user_id: UserId,
        /// Identity of the actor requesting erasure (audit trail).
        erased_by: String,
        /// Region where the user's PII resides.
        region: Region,
    },

    /// One-time migration of existing users from flat `_system` store to
    /// regional directory structure.
    ///
    /// Each entry contains pre-computed data (email HMAC, subject key) so
    /// the blinding key never enters the Raft log. The state machine creates
    /// directory entries, slug indexes, email hash indexes, subject keys,
    /// and removes old plaintext email indexes atomically.
    MigrateExistingUsers {
        /// Pre-computed migration entries (one per user).
        entries: Vec<inferadb_ledger_state::system::UserMigrationEntry>,
    },
}

/// Response from the Raft state machine.
///
/// This is the "R" (response) type in OpenRaft's type configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LedgerResponse {
    /// Empty response (for operations that don't return data).
    #[default]
    Empty,

    /// Writes operation completed.
    Write {
        /// Block height where the write was committed.
        block_height: u64,
        /// Block hash.
        block_hash: Hash,
        /// Server-assigned sequence number for this write.
        assigned_sequence: u64,
    },

    /// Organization created.
    OrganizationCreated {
        /// Assigned organization ID.
        organization: OrganizationId,
        /// Assigned region.
        region: Region,
    },

    /// Vault created.
    VaultCreated {
        /// Assigned internal vault ID.
        vault: VaultId,
        /// External Snowflake slug for API lookups.
        slug: VaultSlug,
    },

    /// Organization deleted.
    OrganizationDeleted {
        /// Whether the deletion was successful.
        /// If false, `blocking_vault_ids` contains the vaults that must be deleted first.
        success: bool,
        /// Vault IDs that are blocking deletion (only set when success=false).
        /// Clients should delete these vaults before retrying organization deletion.
        blocking_vault_ids: Vec<VaultId>,
    },

    /// Organization migrated to a new region.
    OrganizationMigrated {
        /// Organization that was migrated.
        organization: OrganizationId,
        /// Previous region assignment.
        old_region: Region,
        /// New region assignment.
        new_region: Region,
    },

    /// Organization suspended.
    OrganizationSuspended {
        /// Organization that was suspended.
        organization: OrganizationId,
    },

    /// Organization resumed (suspension lifted).
    OrganizationResumed {
        /// Organization that was resumed.
        organization: OrganizationId,
    },

    /// Organization migration started.
    MigrationStarted {
        /// Organization entering migration.
        organization: OrganizationId,
        /// Target region for migration.
        target_region_group: Region,
    },

    /// Organization migration completed.
    MigrationCompleted {
        /// Organization that was migrated.
        organization: OrganizationId,
        /// Previous region assignment.
        old_region: Region,
        /// New region assignment.
        new_region: Region,
    },

    /// Organization marked for deletion (has active vaults).
    /// Transitions to Deleted once all vaults are deleted.
    OrganizationDeleting {
        /// Organization marked for deletion.
        organization: OrganizationId,
        /// Vault IDs that must be deleted first.
        blocking_vault_ids: Vec<VaultId>,
    },

    /// Vault deleted.
    VaultDeleted {
        /// Whether the deletion was successful.
        success: bool,
    },

    /// Vault health updated.
    VaultHealthUpdated {
        /// Whether the update was successful.
        success: bool,
    },

    /// User created.
    UserCreated {
        /// Assigned user ID.
        user_id: UserId,
        /// External Snowflake identifier.
        slug: UserSlug,
    },

    /// User data erased via crypto-shredding.
    UserErased {
        /// User whose data was erased.
        user_id: UserId,
    },

    /// Flat-to-regional user migration completed.
    UsersMigrated {
        /// User records processed.
        users: u64,
        /// Users successfully migrated.
        migrated: u64,
        /// Users skipped (already migrated).
        skipped: u64,
        /// Users that failed migration.
        errors: u64,
    },

    /// Error response.
    Error {
        /// Error message.
        message: String,
    },

    /// Precondition failed for conditional write.
    /// Returns current state for client-side conflict resolution.
    PreconditionFailed {
        /// Key that failed the condition.
        key: String,
        /// Current version of the entity (block height when last modified).
        current_version: Option<u64>,
        /// Current value of the entity.
        current_value: Option<Vec<u8>>,
        /// The condition that failed (for specific error code mapping).
        failed_condition: Option<SetCondition>,
    },

    /// Batches of responses from a BatchWrite request.
    ///
    /// Responses are in the same order as the requests in the corresponding
    /// `LedgerRequest::BatchWrite`.
    BatchWrite {
        /// Responses for each request in the batch.
        responses: Vec<LedgerResponse>,
    },
}

impl fmt::Display for LedgerResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LedgerResponse::Empty => write!(f, "Empty"),
            LedgerResponse::Write { block_height, .. } => {
                write!(f, "Write(height={})", block_height)
            },
            LedgerResponse::OrganizationCreated { organization, region } => {
                write!(f, "OrganizationCreated(id={}, region={})", organization, region)
            },
            LedgerResponse::VaultCreated { vault, slug } => {
                write!(f, "VaultCreated(id={}, slug={})", vault, slug)
            },
            LedgerResponse::UserCreated { user_id, slug } => {
                write!(f, "UserCreated(id={}, slug={})", user_id, slug)
            },
            LedgerResponse::OrganizationDeleted { success, blocking_vault_ids } => {
                if *success {
                    write!(f, "OrganizationDeleted(success=true)")
                } else {
                    write!(
                        f,
                        "OrganizationDeleted(success=false, blocking_vaults={:?})",
                        blocking_vault_ids
                    )
                }
            },
            LedgerResponse::OrganizationMigrated { organization, old_region, new_region } => {
                write!(
                    f,
                    "OrganizationMigrated(id={}, {}->{})",
                    organization, old_region, new_region
                )
            },
            LedgerResponse::OrganizationSuspended { organization } => {
                write!(f, "OrganizationSuspended(id={})", organization)
            },
            LedgerResponse::OrganizationResumed { organization } => {
                write!(f, "OrganizationResumed(id={})", organization)
            },
            LedgerResponse::MigrationStarted { organization, target_region_group } => {
                write!(f, "MigrationStarted(id={}, target={})", organization, target_region_group)
            },
            LedgerResponse::MigrationCompleted { organization, old_region, new_region } => {
                write!(f, "MigrationCompleted(id={}, {}->{})", organization, old_region, new_region)
            },
            LedgerResponse::OrganizationDeleting { organization, blocking_vault_ids } => {
                write!(
                    f,
                    "OrganizationDeleting(id={}, blocking_vaults={:?})",
                    organization, blocking_vault_ids
                )
            },
            LedgerResponse::VaultDeleted { success } => {
                write!(f, "VaultDeleted(success={})", success)
            },
            LedgerResponse::VaultHealthUpdated { success } => {
                write!(f, "VaultHealthUpdated(success={})", success)
            },
            LedgerResponse::UserErased { user_id } => {
                write!(f, "UserErased(id={})", user_id)
            },
            LedgerResponse::UsersMigrated { users, migrated, skipped, errors } => {
                write!(
                    f,
                    "UsersMigrated(total={}, migrated={}, skipped={}, errors={})",
                    users, migrated, skipped, errors
                )
            },
            LedgerResponse::Error { message } => {
                write!(f, "Error({})", message)
            },
            LedgerResponse::PreconditionFailed { key, .. } => {
                write!(f, "PreconditionFailed(key={})", key)
            },
            LedgerResponse::BatchWrite { responses } => {
                write!(f, "BatchWrite(count={})", responses.len())
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_ledger_request_serialization() {
        let request = LedgerRequest::CreateOrganization {
            name: "test-org".to_string(),
            slug: OrganizationSlug::new(12345),
            region: Region::US_EAST_VA,
            tier: Default::default(),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::CreateOrganization { name, slug, region, .. } => {
                assert_eq!(name, "test-org");
                assert_eq!(slug, OrganizationSlug::new(12345));
                assert_eq!(region, Region::US_EAST_VA);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_ledger_response_display() {
        let response =
            LedgerResponse::Write { block_height: 42, block_hash: [0u8; 32], assigned_sequence: 1 };
        assert_eq!(format!("{}", response), "Write(height=42)");
    }

    #[test]
    fn test_system_request_serialization() {
        let request = SystemRequest::CreateUser {
            user: UserId::new(42),
            admin: false,
            slug: UserSlug::new(12345),
            region: Region::US_EAST_VA,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::CreateUser { user, admin, slug, region } => {
                assert_eq!(user, UserId::new(42));
                assert!(!admin);
                assert_eq!(slug, UserSlug::new(12345));
                assert_eq!(region, Region::US_EAST_VA);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_register_email_hash_serialization() {
        let request = SystemRequest::RegisterEmailHash {
            hmac_hex: "a1b2c3".to_string(),
            user_id: UserId::new(42),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::RegisterEmailHash { hmac_hex, user_id } => {
                assert_eq!(hmac_hex, "a1b2c3");
                assert_eq!(user_id, UserId::new(42));
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_remove_email_hash_serialization() {
        let request = SystemRequest::RemoveEmailHash { hmac_hex: "deadbeef".to_string() };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::RemoveEmailHash { hmac_hex } => {
                assert_eq!(hmac_hex, "deadbeef");
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_set_blinding_key_version_serialization() {
        let request = SystemRequest::SetBlindingKeyVersion { version: 3 };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::SetBlindingKeyVersion { version } => {
                assert_eq!(version, 3);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_update_rehash_progress_serialization() {
        let request = SystemRequest::UpdateRehashProgress {
            region: Region::IE_EAST_DUBLIN,
            entries_rehashed: 1500,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::UpdateRehashProgress { region, entries_rehashed } => {
                assert_eq!(region, Region::IE_EAST_DUBLIN);
                assert_eq!(entries_rehashed, 1500);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_clear_rehash_progress_serialization() {
        let request = SystemRequest::ClearRehashProgress { region: Region::IN_WEST_MUMBAI };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::ClearRehashProgress { region } => {
                assert_eq!(region, Region::IN_WEST_MUMBAI);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_raft_payload_serde_roundtrip() {
        use chrono::TimeZone;

        let payload = RaftPayload {
            request: LedgerRequest::CreateOrganization {
                name: "test-org".to_string(),
                slug: OrganizationSlug::new(999),
                region: Region::US_EAST_VA,
                tier: Default::default(),
            },
            proposed_at: Utc.with_ymd_and_hms(2099, 6, 15, 12, 30, 0).unwrap(),
            state_root_commitments: vec![],
        };

        let bytes = postcard::to_allocvec(&payload).expect("serialize");
        let deserialized: RaftPayload = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(payload, deserialized);
        assert_eq!(deserialized.proposed_at, Utc.with_ymd_and_hms(2099, 6, 15, 12, 30, 0).unwrap());
        match &deserialized.request {
            LedgerRequest::CreateOrganization { name, slug, .. } => {
                assert_eq!(name, "test-org");
                assert_eq!(*slug, OrganizationSlug::new(999));
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_raft_payload_preserves_proposed_at_across_reserialize() {
        use chrono::TimeZone;

        let ts = Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap();
        let payload = RaftPayload {
            request: LedgerRequest::Write {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0; 16],
                request_hash: 0,
            },
            proposed_at: ts,
            state_root_commitments: vec![],
        };

        let bytes1 = postcard::to_allocvec(&payload).expect("serialize");
        let decoded: RaftPayload = postcard::from_bytes(&bytes1).expect("deserialize");
        let bytes2 = postcard::to_allocvec(&decoded).expect("re-serialize");

        assert_eq!(bytes1, bytes2, "re-serialization should produce identical bytes");
    }

    // ============================================
    // State Root Commitment Serialization Tests
    // ============================================

    #[test]
    fn test_state_root_commitment_serialization_roundtrip() {
        let commitment = StateRootCommitment {
            organization: OrganizationId::new(42),
            vault: VaultId::new(7),
            vault_height: 100,
            state_root: [0xAB; 32],
        };

        let bytes = postcard::to_allocvec(&commitment).expect("serialize");
        let deserialized: StateRootCommitment = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(commitment, deserialized);
        assert_eq!(deserialized.organization, OrganizationId::new(42));
        assert_eq!(deserialized.vault, VaultId::new(7));
        assert_eq!(deserialized.vault_height, 100);
        assert_eq!(deserialized.state_root, [0xAB; 32]);
    }

    #[test]
    fn test_state_root_commitment_zero_hash() {
        let commitment = StateRootCommitment {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            vault_height: 0,
            state_root: [0u8; 32],
        };

        let bytes = postcard::to_allocvec(&commitment).expect("serialize");
        let deserialized: StateRootCommitment = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(commitment, deserialized);
        assert_eq!(deserialized.state_root, [0u8; 32]);
    }

    #[test]
    fn test_raft_payload_with_commitments_roundtrip() {
        use chrono::TimeZone;

        let commitments = vec![
            StateRootCommitment {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                vault_height: 10,
                state_root: [0xAA; 32],
            },
            StateRootCommitment {
                organization: OrganizationId::new(2),
                vault: VaultId::new(3),
                vault_height: 20,
                state_root: [0xBB; 32],
            },
        ];

        let payload = RaftPayload {
            request: LedgerRequest::Write {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0; 16],
                request_hash: 0,
            },
            proposed_at: Utc.with_ymd_and_hms(2099, 6, 15, 12, 0, 0).unwrap(),
            state_root_commitments: commitments.clone(),
        };

        let bytes = postcard::to_allocvec(&payload).expect("serialize");
        let deserialized: RaftPayload = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(deserialized.state_root_commitments.len(), 2);
        assert_eq!(deserialized.state_root_commitments[0].organization, OrganizationId::new(1));
        assert_eq!(deserialized.state_root_commitments[0].vault_height, 10);
        assert_eq!(deserialized.state_root_commitments[0].state_root, [0xAA; 32]);
        assert_eq!(deserialized.state_root_commitments[1].organization, OrganizationId::new(2));
        assert_eq!(deserialized.state_root_commitments[1].vault_height, 20);
        assert_eq!(deserialized.state_root_commitments[1].state_root, [0xBB; 32]);
    }

    #[test]
    fn test_raft_payload_backward_compat_empty_commitments() {
        // Payloads serialized before the commitments field was added should
        // deserialize with an empty vec (thanks to #[serde(default)]).
        use chrono::TimeZone;

        let payload_without = RaftPayload {
            request: LedgerRequest::CreateOrganization {
                name: "test".to_string(),
                slug: OrganizationSlug::new(1),
                region: Region::US_EAST_VA,
                tier: Default::default(),
            },
            proposed_at: Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap(),
            state_root_commitments: vec![],
        };

        let bytes = postcard::to_allocvec(&payload_without).expect("serialize");
        let deserialized: RaftPayload = postcard::from_bytes(&bytes).expect("deserialize");

        assert!(deserialized.state_root_commitments.is_empty());
    }

    #[test]
    fn test_raft_payload_with_commitments_preserves_bytes_across_reserialize() {
        use chrono::TimeZone;

        let payload = RaftPayload {
            request: LedgerRequest::Write {
                organization: OrganizationId::new(5),
                vault: VaultId::new(3),
                transactions: vec![],
                idempotency_key: [42; 16],
                request_hash: 12345,
            },
            proposed_at: Utc.with_ymd_and_hms(2099, 3, 1, 0, 0, 0).unwrap(),
            state_root_commitments: vec![StateRootCommitment {
                organization: OrganizationId::new(5),
                vault: VaultId::new(3),
                vault_height: 99,
                state_root: [0xFF; 32],
            }],
        };

        let bytes1 = postcard::to_allocvec(&payload).expect("serialize");
        let decoded: RaftPayload = postcard::from_bytes(&bytes1).expect("deserialize");
        let bytes2 = postcard::to_allocvec(&decoded).expect("re-serialize");

        assert_eq!(bytes1, bytes2, "re-serialization with commitments should be stable");
    }

    // ============================================
    // Property-based Raft log invariant tests
    // ============================================

    mod proptest_raft_log {
        use inferadb_ledger_types::{OrganizationId, VaultId, VaultSlug};
        use openraft::{CommittedLeaderId, LogId};
        use proptest::prelude::*;

        use crate::types::{LedgerNodeId, LedgerRequest};

        /// Helper to create a LogId from term and index.
        fn make_log_id(term: u64, index: u64) -> LogId<LedgerNodeId> {
            LogId::new(CommittedLeaderId::new(term, 0), index)
        }

        /// Represents a Raft log entry with term and index.
        #[derive(Debug, Clone)]
        struct LogEntry {
            term: u64,
            index: u64,
        }

        /// Generates a valid Raft log sequence with monotonic indices and
        /// non-decreasing terms. Optionally includes term changes (leader elections).
        fn arb_valid_log(max_entries: usize) -> impl Strategy<Value = Vec<LogEntry>> {
            proptest::collection::vec(
                (1u64..100, prop::bool::ANY), // (term_increment, is_election)
                1..max_entries,
            )
            .prop_map(|decisions| {
                let mut entries = Vec::new();
                let mut current_term = 1u64;
                let mut current_index = 1u64;

                for (term_inc, is_election) in decisions {
                    if is_election {
                        current_term += term_inc;
                    }
                    entries.push(LogEntry { term: current_term, index: current_index });
                    current_index += 1;
                }
                entries
            })
        }

        proptest! {
            /// Logs indices must be strictly monotonic (sequential, no gaps).
            #[test]
            fn prop_log_indices_strictly_monotonic(log in arb_valid_log(200)) {
                for window in log.windows(2) {
                    prop_assert_eq!(
                        window[1].index,
                        window[0].index + 1,
                        "indices not sequential: {} -> {}",
                        window[0].index,
                        window[1].index
                    );
                }
            }

            /// Logs terms must be non-decreasing (can stay same or increase, never decrease).
            #[test]
            fn prop_log_terms_nondecreasing(log in arb_valid_log(200)) {
                for window in log.windows(2) {
                    prop_assert!(
                        window[1].term >= window[0].term,
                        "term decreased: {} -> {} at indices {}-{}",
                        window[0].term,
                        window[1].term,
                        window[0].index,
                        window[1].index
                    );
                }
            }

            /// LogId ordering: later entries have greater or equal LogId.
            /// This verifies that openraft's LogId ordering matches our expectations.
            #[test]
            fn prop_logid_ordering_consistent(log in arb_valid_log(200)) {
                let log_ids: Vec<LogId<LedgerNodeId>> = log
                    .iter()
                    .map(|e| make_log_id(e.term, e.index))
                    .collect();

                for window in log_ids.windows(2) {
                    prop_assert!(
                        window[1] >= window[0],
                        "LogId ordering violated: {:?} > {:?}",
                        window[0],
                        window[1]
                    );
                }
            }

            /// First entry always has index >= 1 (0 is reserved for initial state).
            #[test]
            fn prop_first_index_nonzero(log in arb_valid_log(50)) {
                if let Some(first) = log.first() {
                    prop_assert!(
                        first.index >= 1,
                        "first index should be >= 1, got {}",
                        first.index
                    );
                }
            }

            /// Term changes represent leader elections: within the same term,
            /// indices must be contiguous (no gaps within a term).
            #[test]
            fn prop_no_index_gaps_within_term(log in arb_valid_log(200)) {
                // Group consecutive entries by term
                let mut term_groups: Vec<Vec<u64>> = Vec::new();
                let mut current_term = 0u64;

                for entry in &log {
                    if entry.term != current_term {
                        term_groups.push(Vec::new());
                        current_term = entry.term;
                    }
                    if let Some(group) = term_groups.last_mut() {
                        group.push(entry.index);
                    }
                }

                // Within each term group, indices must be contiguous
                for group in &term_groups {
                    for window in group.windows(2) {
                        prop_assert_eq!(
                            window[1],
                            window[0] + 1,
                            "gap within term: indices {} -> {}",
                            window[0],
                            window[1]
                        );
                    }
                }
            }

            /// LedgerRequest serialization roundtrip preserves all variants.
            #[test]
            fn prop_ledger_request_roundtrip(
                variant_idx in 0u8..4,
                name in "[a-z]{1,16}",
                organization in (1i64..10_000).prop_map(OrganizationId::new),
                vault in (1i64..10_000).prop_map(VaultId::new),
                region_idx in 0usize..inferadb_ledger_types::ALL_REGIONS.len(),
            ) {
                let region = inferadb_ledger_types::ALL_REGIONS[region_idx];
                let request = match variant_idx {
                    0 => LedgerRequest::CreateOrganization {
                        name: name.clone(),
                        slug: inferadb_ledger_types::OrganizationSlug::new(42),
                        region,
                        tier: Default::default(),
                    },
                    1 => LedgerRequest::CreateVault {
                        organization,
                        slug: VaultSlug::new(42),
                        name: Some(name.clone()),
                        retention_policy: None,
                    },
                    2 => LedgerRequest::DeleteOrganization { organization },
                    _ => LedgerRequest::DeleteVault { organization, vault },
                };

                let bytes = postcard::to_allocvec(&request).expect("serialize");
                let decoded: super::LedgerRequest =
                    postcard::from_bytes(&bytes).expect("deserialize");
                prop_assert_eq!(
                    postcard::to_allocvec(&decoded).expect("re-serialize"),
                    bytes,
                    "roundtrip changed encoding"
                );
            }
        }
    }
}
