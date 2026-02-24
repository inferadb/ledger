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
    Hash, OrganizationId, OrganizationSlug, SetCondition, ShardId, Transaction, VaultId, VaultSlug,
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
        organization_id: OrganizationId,
        /// Target vault within the organization.
        vault_id: VaultId,
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
        /// Target shard ID (None = auto-assign to shard 0).
        shard_id: Option<ShardId>,
        /// Optional resource quota for this organization.
        /// When `None`, server-wide default quota applies.
        quota: Option<inferadb_ledger_types::config::OrganizationQuota>,
    },

    /// Creates a new vault within an organization.
    CreateVault {
        /// Organization to create the vault in.
        organization_id: OrganizationId,
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
        organization_id: OrganizationId,
    },

    /// Deletes a vault.
    DeleteVault {
        /// Organization containing the vault.
        organization_id: OrganizationId,
        /// Vault ID to delete.
        vault_id: VaultId,
    },

    /// Suspends an organization (billing hold or policy violation).
    /// Suspended organizations reject writes but allow reads.
    SuspendOrganization {
        /// Organization to suspend.
        organization_id: OrganizationId,
        /// Optional reason for suspension (e.g., "Payment overdue", "TOS violation").
        reason: Option<String>,
    },

    /// Resumes a suspended organization.
    ResumeOrganization {
        /// Organization to resume.
        organization_id: OrganizationId,
    },

    /// Starts organization migration to a new shard.
    /// Sets status to Migrating, blocking writes until CompleteMigration.
    StartMigration {
        /// Organization to migrate.
        organization_id: OrganizationId,
        /// Target shard ID for migration.
        target_shard_id: ShardId,
    },

    /// Completes a pending organization migration.
    /// Updates shard_id and returns status to Active.
    CompleteMigration {
        /// Organization being migrated.
        organization_id: OrganizationId,
    },

    /// Updates vault health status (used during recovery).
    UpdateVaultHealth {
        /// Organization containing the vault.
        organization_id: OrganizationId,
        /// Vault ID to update.
        vault_id: VaultId,
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
    /// Creates a new user.
    CreateUser {
        /// User's display name.
        name: String,
        /// User's email address.
        email: String,
        /// Whether this user is a global service administrator.
        admin: bool,
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

    /// Updates organization-to-shard mapping.
    UpdateOrganizationRouting {
        /// Organization to update.
        organization_id: OrganizationId,
        /// New shard assignment.
        shard_id: i32,
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
        organization_id: OrganizationId,
        /// Assigned shard ID.
        shard_id: ShardId,
    },

    /// Vault created.
    VaultCreated {
        /// Assigned internal vault ID.
        vault_id: VaultId,
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

    /// Organization migrated to a new shard.
    OrganizationMigrated {
        /// Organization that was migrated.
        organization_id: OrganizationId,
        /// Previous shard assignment.
        old_shard_id: ShardId,
        /// New shard assignment.
        new_shard_id: ShardId,
    },

    /// Organization suspended.
    OrganizationSuspended {
        /// Organization that was suspended.
        organization_id: OrganizationId,
    },

    /// Organization resumed (suspension lifted).
    OrganizationResumed {
        /// Organization that was resumed.
        organization_id: OrganizationId,
    },

    /// Organization migration started.
    MigrationStarted {
        /// Organization entering migration.
        organization_id: OrganizationId,
        /// Target shard for migration.
        target_shard_id: ShardId,
    },

    /// Organization migration completed.
    MigrationCompleted {
        /// Organization that was migrated.
        organization_id: OrganizationId,
        /// Previous shard assignment.
        old_shard_id: ShardId,
        /// New shard assignment.
        new_shard_id: ShardId,
    },

    /// Organization marked for deletion (has active vaults).
    /// Transitions to Deleted once all vaults are deleted.
    OrganizationDeleting {
        /// Organization marked for deletion.
        organization_id: OrganizationId,
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
        user_id: i64,
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
            LedgerResponse::OrganizationCreated { organization_id, shard_id } => {
                write!(f, "OrganizationCreated(id={}, shard={})", organization_id, shard_id)
            },
            LedgerResponse::VaultCreated { vault_id, slug } => {
                write!(f, "VaultCreated(id={}, slug={})", vault_id, slug)
            },
            LedgerResponse::UserCreated { user_id } => {
                write!(f, "UserCreated(id={})", user_id)
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
            LedgerResponse::OrganizationMigrated {
                organization_id,
                old_shard_id,
                new_shard_id,
            } => {
                write!(
                    f,
                    "OrganizationMigrated(id={}, {}->{})",
                    organization_id, old_shard_id, new_shard_id
                )
            },
            LedgerResponse::OrganizationSuspended { organization_id } => {
                write!(f, "OrganizationSuspended(id={})", organization_id)
            },
            LedgerResponse::OrganizationResumed { organization_id } => {
                write!(f, "OrganizationResumed(id={})", organization_id)
            },
            LedgerResponse::MigrationStarted { organization_id, target_shard_id } => {
                write!(f, "MigrationStarted(id={}, target={})", organization_id, target_shard_id)
            },
            LedgerResponse::MigrationCompleted { organization_id, old_shard_id, new_shard_id } => {
                write!(
                    f,
                    "MigrationCompleted(id={}, {}->{})",
                    organization_id, old_shard_id, new_shard_id
                )
            },
            LedgerResponse::OrganizationDeleting { organization_id, blocking_vault_ids } => {
                write!(
                    f,
                    "OrganizationDeleting(id={}, blocking_vaults={:?})",
                    organization_id, blocking_vault_ids
                )
            },
            LedgerResponse::VaultDeleted { success } => {
                write!(f, "VaultDeleted(success={})", success)
            },
            LedgerResponse::VaultHealthUpdated { success } => {
                write!(f, "VaultHealthUpdated(success={})", success)
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
            shard_id: Some(ShardId::new(1)),
            quota: None,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::CreateOrganization { name, slug, shard_id, quota: _ } => {
                assert_eq!(name, "test-org");
                assert_eq!(slug, OrganizationSlug::new(12345));
                assert_eq!(shard_id, Some(ShardId::new(1)));
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
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            admin: false,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::CreateUser { name, email, admin } => {
                assert_eq!(name, "Alice");
                assert_eq!(email, "alice@example.com");
                assert!(!admin);
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
                shard_id: Some(ShardId::new(1)),
                quota: None,
            },
            proposed_at: Utc.with_ymd_and_hms(2099, 6, 15, 12, 30, 0).unwrap(),
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
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0; 16],
                request_hash: 0,
            },
            proposed_at: ts,
        };

        let bytes1 = postcard::to_allocvec(&payload).expect("serialize");
        let decoded: RaftPayload = postcard::from_bytes(&bytes1).expect("deserialize");
        let bytes2 = postcard::to_allocvec(&decoded).expect("re-serialize");

        assert_eq!(bytes1, bytes2, "re-serialization should produce identical bytes");
    }

    // ============================================
    // Property-based Raft log invariant tests
    // ============================================

    mod proptest_raft_log {
        use inferadb_ledger_types::{OrganizationId, ShardId, VaultId, VaultSlug};
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
                organization_id in (1i64..10_000).prop_map(OrganizationId::new),
                vault_id in (1i64..10_000).prop_map(VaultId::new),
                shard_id in (1u32..1_000).prop_map(ShardId::new),
            ) {
                let request = match variant_idx {
                    0 => LedgerRequest::CreateOrganization {
                        name: name.clone(),
                        slug: inferadb_ledger_types::OrganizationSlug::new(42),
                        shard_id: Some(shard_id),
                        quota: None,
                    },
                    1 => LedgerRequest::CreateVault {
                        organization_id,
                        slug: VaultSlug::new(42),
                        name: Some(name.clone()),
                        retention_policy: None,
                    },
                    2 => LedgerRequest::DeleteOrganization { organization_id },
                    _ => LedgerRequest::DeleteVault { organization_id, vault_id },
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
