//! Raft storage implementation using inferadb-ledger-store.
//!
//! Persistent storage for Raft log entries,
//! vote state, committed log tracking, and state machine state.
//!
//! We use the deprecated but non-sealed `RaftStorage` trait which combines
//! log storage and state machine into one implementation. The v2 traits
//! (`RaftLogStorage`, `RaftStateMachine`) are sealed in OpenRaft 0.9.
//!
//! Each region group has its own storage located at:
//! `regions/{region}/raft/log.db`
//!
//! # Lock Ordering Convention
//!
//! To prevent deadlocks, locks in this module must be acquired in the following order:
//!
//! 1. `applied_state` - Raft state machine state
//! 2. `region_chain` - Region chain tracking (height + previous hash)
//! 3. `vote_cache`, `last_purged_cache` - Caches (independent, no ordering requirement)
//!
//! The `region_chain` lock consolidates `region_height` and `previous_region_hash`
//! into a single lock to eliminate internal ordering issues.

mod accessor;
pub mod operations;
mod raft_impl;
mod store;
mod types;

pub use accessor::*;
use inferadb_ledger_types::Hash;
pub use operations::ApplyableRequest;
pub use raft_impl::{LedgerSnapshotBuilder, RecoveryStats};
pub use store::*;
pub use types::*;

// ============================================================================
// Metadata Keys
// ============================================================================

// Metadata keys for RaftState table
const KEY_VOTE: &str = "vote";
#[allow(dead_code)] // Used by OpenRaft's save_committed/read_committed trait methods
const KEY_COMMITTED: &str = "committed";
const KEY_LAST_PURGED: &str = "last_purged";
const KEY_APPLIED_STATE: &str = "applied_state";

// ============================================================================
// Region Chain State (Lock Consolidated)
// ============================================================================

/// Region chain tracking state.
///
/// These fields are grouped into a single lock to avoid lock ordering issues.
/// They track the region-level blockchain state for creating RegionBlocks.
#[derive(Debug, Clone, Copy, Default)]
pub struct RegionChainState {
    /// Current region height for block creation.
    pub height: u64,
    /// Previous region hash for chain continuity.
    pub previous_hash: Hash,
}

/// Test-only helpers for synchronously applying requests without the full apply pipeline.
#[cfg(test)]
impl<B: inferadb_ledger_store::StorageBackend> store::RaftLogStore<B> {
    /// Apply a `SystemRequest` synchronously with default aux parameters.
    ///
    /// Convenience shim for unit tests — avoids spelling out all the boilerplate
    /// parameters that the production apply pipeline fills in.
    pub(crate) fn apply_system(
        &self,
        request: &crate::types::SystemRequest,
        state: &mut types::AppliedState,
    ) -> (crate::types::LedgerResponse, Option<inferadb_ledger_types::VaultEntry>) {
        self.apply_system_request_with_events(
            request,
            state,
            chrono::Utc::now(),
            &mut 0u32,
            &mut vec![],
            0,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        )
    }

    /// Apply an `OrganizationRequest` synchronously with default aux parameters.
    ///
    /// Convenience shim for unit tests — avoids spelling out all the boilerplate
    /// parameters that the production apply pipeline fills in.
    pub(crate) fn apply_org(
        &self,
        request: &crate::types::OrganizationRequest,
        state: &mut types::AppliedState,
    ) -> (crate::types::LedgerResponse, Option<inferadb_ledger_types::VaultEntry>) {
        self.apply_organization_request_with_events(
            request,
            state,
            chrono::Utc::now(),
            &mut 0u32,
            &mut vec![],
            0,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, Duration, Utc};
    use inferadb_ledger_consensus::{committed::CommittedEntry, types::EntryKind};
    use inferadb_ledger_state::{
        EventStore, EventsDatabase,
        system::{OrganizationMemberRole, OrganizationStatus, OrganizationTier},
    };
    use inferadb_ledger_store::{FileBackend, tables};
    use inferadb_ledger_types::{
        ClientId, CredentialData, CredentialType, EmailVerifyTokenId, ErrorCode, InvitationStatus,
        InviteId, InviteSlug, Operation, OrganizationId, PasskeyCredential, PrimaryAuthMethod,
        RecoveryCodeCredential, Region, SigningKeyScope, TeamId, TeamSlug, TokenSubject,
        TotpCredential, Transaction, UserCredentialId, UserEmailId, UserId, UserSlug, VaultId,
        VaultSlug,
        events::{EventAction, EventConfig, EventEntry, EventScope},
    };
    use tempfile::tempdir;
    use tokio::sync::broadcast;

    use super::{types::estimate_write_storage_delta, *};
    use crate::{
        log_storage::types::{LogId, Vote},
        types::{
            BlockRetentionPolicy, LedgerResponse, OrganizationRequest, RaftPayload, SystemRequest,
        },
    };

    /// Helper to create log IDs for tests.
    #[cfg(test)]
    fn make_log_id(term: u64, index: u64) -> LogId {
        LogId::new(term, 0, index)
    }

    /// Converts a `RaftPayload<R>` to a `CommittedEntry` for test use.
    fn make_committed_entry<R: serde::Serialize>(
        term: u64,
        index: u64,
        payload: RaftPayload<R>,
    ) -> CommittedEntry {
        let data = inferadb_ledger_types::encode(&payload).expect("encode payload");
        CommittedEntry { index, term, data: data.into(), kind: EntryKind::Normal }
    }

    /// Wraps a `SystemRequest` into a `RaftPayload` for test entries.
    fn wrap_system_payload(request: SystemRequest) -> RaftPayload<SystemRequest> {
        RaftPayload::system(request)
    }

    /// Wraps an `OrganizationRequest` into a `RaftPayload` for test entries.
    fn wrap_org_payload(request: OrganizationRequest) -> RaftPayload<OrganizationRequest> {
        RaftPayload::system(request)
    }

    /// Creates an organization directory and immediately activates it.
    ///
    /// `CreateOrganization` sets status to `Provisioning`. Tests that
    /// need the org to accept writes must activate it via
    /// `UpdateOrganizationStatus`.
    fn create_active_organization(
        store: &RaftLogStore<FileBackend>,
        state: &mut AppliedState,
        slug: inferadb_ledger_types::OrganizationSlug,
        region: Region,
    ) -> OrganizationId {
        let (response, _) = store.apply_system(
            &SystemRequest::CreateOrganization {
                slug,
                region,
                tier: Default::default(),
                admin: UserId::new(1),
            },
            state,
        );
        let org_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };
        store.apply_system(
            &SystemRequest::UpdateOrganizationStatus {
                organization: org_id,
                status: OrganizationStatus::Active,
            },
            state,
        );
        org_id
    }

    #[tokio::test]
    async fn test_log_store_open() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        // Verify database can be read (tables exist in inferadb-ledger-store by default)
        let read_txn = store.db.read().expect("begin read");
        // Tables are fixed in inferadb-ledger-store - just verify we can get a transaction
        let _ = read_txn.get::<tables::RaftLog>(&0u64).expect("query RaftLog");
        let _ = read_txn.get::<tables::RaftState>(&"test".to_string()).expect("query RaftState");
    }

    #[tokio::test]
    async fn test_save_and_read_vote() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        let vote = Vote { term: 1, node_id: 42, committed: false };
        store.save_vote(&vote).await.expect("save vote");

        let read_vote = store.read_vote().await.expect("read vote");
        assert_eq!(read_vote, Some(vote));
    }

    #[tokio::test]
    async fn test_sequence_counters() {
        let mut counters = SequenceCounters::new();

        assert_eq!(counters.next_organization(), OrganizationId::new(1));
        assert_eq!(counters.next_organization(), OrganizationId::new(2));
        assert_eq!(counters.next_vault(), VaultId::new(1));
        assert_eq!(counters.next_vault(), VaultId::new(2));
        assert_eq!(counters.next_user(), UserId::new(1));
        assert_eq!(counters.next_user(), UserId::new(2));
        assert_eq!(counters.next_user_email(), UserEmailId::new(1));
        assert_eq!(counters.next_user_email(), UserEmailId::new(2));
        assert_eq!(counters.next_email_verify(), EmailVerifyTokenId::new(1));
        assert_eq!(counters.next_email_verify(), EmailVerifyTokenId::new(2));
    }

    #[tokio::test]
    async fn test_apply_create_organization() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        let request = SystemRequest::CreateOrganization {
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            region: Region::US_EAST_VA,
            tier: Default::default(),
            admin: UserId::new(1),
        };

        let (response, _vault_entry) = store.apply_system(&request, &mut state);

        match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => {
                assert_eq!(organization_id, OrganizationId::new(1));

                // Bare directory creation starts in Provisioning (saga will activate later)
                assert_eq!(
                    state.organizations.get(&organization_id).unwrap().status,
                    OrganizationStatus::Provisioning
                );
            },
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_apply_create_organization_uses_provided_region() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        state.sequences.organization = OrganizationId::new(1);

        store.applied_state.store(Arc::new(state));

        let mut state = (*store.applied_state.load_full()).clone();
        let request = SystemRequest::CreateOrganization {
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            region: Region::US_EAST_VA,
            tier: Default::default(),
            admin: UserId::new(1),
        };

        let (response, _vault_entry) = store.apply_system(&request, &mut state);

        match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => {
                assert_eq!(organization_id, OrganizationId::new(1));
                let meta = state.organizations.get(&organization_id).expect("org exists");
                assert_eq!(meta.region, Region::US_EAST_VA, "Should use provided region");
            },
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_apply_create_organization_explicit_region_overrides() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Pre-populate: region 0 has fewer organizations
        state.organizations.insert(
            OrganizationId::new(1),
            OrganizationMeta {
                organization: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                region: Region::GLOBAL,
                status: OrganizationStatus::Active,
                tier: OrganizationTier::Free,
                pending_region: None,
                storage_bytes: 0,
            },
        );
        state.sequences.organization = OrganizationId::new(2);

        // Create organization with explicit IE_EAST_DUBLIN region
        let request = SystemRequest::CreateOrganization {
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            region: Region::IE_EAST_DUBLIN,
            tier: Default::default(),
            admin: UserId::new(1),
        };

        let (response, _vault_entry) = store.apply_system(&request, &mut state);

        match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => {
                assert_eq!(organization_id, OrganizationId::new(2));
                // Explicit region should override load balancing
                let meta = state.organizations.get(&organization_id).expect("org exists");
                assert_eq!(
                    meta.region,
                    Region::IE_EAST_DUBLIN,
                    "Explicit region should override load balancing"
                );
            },
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_apply_create_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let request = OrganizationRequest::CreateVault {
            organization: org_id,
            slug: VaultSlug::new(1),
            name: Some("test-vault".to_string()),
            retention_policy: None,
        };

        let (response, _vault_entry) = store.apply_org(&request, &mut state);

        match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => {
                assert_eq!(vault_id, VaultId::new(1));
                assert_eq!(state.vault_heights.get(&(org_id, VaultId::new(1))), Some(&0));
            },
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_diverged_vault_rejects_writes() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Mark vault as diverged
        state.vault_health.insert(
            (org_id, VaultId::new(1)),
            VaultHealthStatus::Diverged { expected: [1u8; 32], computed: [2u8; 32], at_height: 10 },
        );

        let request = OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let (response, _vault_entry) = store.apply_org(&request, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("diverged"));
            },
            _ => panic!("expected error response"),
        }
    }

    #[tokio::test]
    async fn test_update_vault_health_to_healthy() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Start with a diverged vault
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Diverged { expected: [1u8; 32], computed: [2u8; 32], at_height: 10 },
        );

        // Update to healthy
        let request = OrganizationRequest::UpdateVaultHealth {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            healthy: true,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: None,
            recovery_started_at: None,
        };

        let (response, _vault_entry) = store.apply_org(&request, &mut state);

        match response {
            LedgerResponse::VaultHealthUpdated { success } => {
                assert!(success);
            },
            _ => panic!("expected VaultHealthUpdated response"),
        }

        // Verify vault is now healthy
        assert_eq!(
            state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(&VaultHealthStatus::Healthy)
        );
    }

    #[tokio::test]
    async fn test_update_vault_health_to_diverged() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Start healthy
        state
            .vault_health
            .insert((OrganizationId::new(1), VaultId::new(1)), VaultHealthStatus::Healthy);

        // Update to diverged
        let request = OrganizationRequest::UpdateVaultHealth {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            healthy: false,
            expected_root: Some([0xAA; 32]),
            computed_root: Some([0xBB; 32]),
            diverged_at_height: Some(42),
            recovery_attempt: None,
            recovery_started_at: None,
        };

        let (response, _vault_entry) = store.apply_org(&request, &mut state);

        match response {
            LedgerResponse::VaultHealthUpdated { success } => {
                assert!(success);
            },
            _ => panic!("expected VaultHealthUpdated response"),
        }

        // Verify vault is now diverged with correct values
        match state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))) {
            Some(VaultHealthStatus::Diverged { expected, computed, at_height }) => {
                assert_eq!(*expected, [0xAA; 32]);
                assert_eq!(*computed, [0xBB; 32]);
                assert_eq!(*at_height, 42);
            },
            _ => panic!("expected Diverged health status"),
        }
    }

    #[tokio::test]
    async fn test_update_vault_health_to_recovering() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Start with a diverged vault
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Diverged { expected: [1u8; 32], computed: [2u8; 32], at_height: 10 },
        );

        // Update to recovering
        let request = OrganizationRequest::UpdateVaultHealth {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            healthy: false,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: Some(1),
            recovery_started_at: Some(chrono::Utc::now().timestamp()),
        };

        let (response, _vault_entry) = store.apply_org(&request, &mut state);

        match response {
            LedgerResponse::VaultHealthUpdated { success } => {
                assert!(success);
            },
            _ => panic!("expected VaultHealthUpdated response"),
        }

        // Verify vault is now recovering
        match state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))) {
            Some(VaultHealthStatus::Recovering { attempt, .. }) => {
                assert_eq!(*attempt, 1);
            },
            _ => panic!("expected Recovering health status"),
        }
    }

    #[tokio::test]
    async fn test_update_vault_health_recovery_escalates_attempt_count() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Start with a vault already in recovery attempt 1
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Recovering {
                attempt: 1,
                started_at: chrono::Utc::now().timestamp(),
            },
        );

        // Escalate to recovery attempt 2
        let request = OrganizationRequest::UpdateVaultHealth {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            healthy: false,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: Some(2),
            recovery_started_at: Some(chrono::Utc::now().timestamp()),
        };

        let (response, _vault_entry) = store.apply_org(&request, &mut state);

        match response {
            LedgerResponse::VaultHealthUpdated { success } => {
                assert!(success);
            },
            _ => panic!("expected VaultHealthUpdated response"),
        }

        match state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))) {
            Some(VaultHealthStatus::Recovering { attempt, .. }) => {
                assert_eq!(*attempt, 2);
            },
            _ => panic!("expected Recovering health status with attempt 2"),
        }
    }

    // ========================================================================
    // Deletion Cascade Tests
    // ========================================================================
    //
    // These tests verify the organization deletion behavior with blocking vaults.
    // Organizations with active vaults cannot be deleted until all vaults are
    // deleted first. The response includes blocking vault IDs.

    #[tokio::test]
    async fn test_delete_organization_blocked_by_active_vaults() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Create two vaults
        let create_vault1 = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault1".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault1, &mut state);
        let vault1_id = match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        let create_vault2 = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault2".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault2, &mut state);
        let vault2_id = match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Delete organization — should soft-delete regardless of active vaults
        let delete_ns = SystemRequest::DeleteOrganization { organization: organization_id };
        let (response, _) = store.apply_system(&delete_ns, &mut state);

        match response {
            LedgerResponse::OrganizationDeleted {
                organization_id: deleted_id,
                retention_days,
                ..
            } => {
                assert_eq!(deleted_id, organization_id);
                assert!(retention_days > 0);
            },
            _ => panic!("expected OrganizationDeleted, got {:?}", response),
        }

        // Verify organization is now in Deleted state
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleted
        );
        // Vaults are NOT cleaned up at delete time — that's deferred to PurgeOrganization
        let _ = vault1_id;
        let _ = vault2_id;
    }

    #[tokio::test]
    async fn test_delete_organization_succeeds_after_vaults_deleted() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Create vault
        let create_vault = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault, &mut state);
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Delete vault first
        let delete_vault =
            OrganizationRequest::DeleteVault { organization: organization_id, vault: vault_id };
        let (response, _) = store.apply_org(&delete_vault, &mut state);
        match response {
            LedgerResponse::VaultDeleted { success } => assert!(success),
            _ => panic!("expected VaultDeleted"),
        }

        // Now delete organization - should succeed (soft-delete)
        let delete_ns = SystemRequest::DeleteOrganization { organization: organization_id };
        let (response, _) = store.apply_system(&delete_ns, &mut state);

        match response {
            LedgerResponse::OrganizationDeleted { organization_id: deleted_id, .. } => {
                assert_eq!(deleted_id, organization_id);
            },
            _ => panic!("expected OrganizationDeleted, got {:?}", response),
        }

        // Verify organization is marked as deleted
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleted
        );
    }

    #[tokio::test]
    async fn test_delete_organization_empty_succeeds() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization with no vaults
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Delete organization immediately - should succeed (soft-delete, no vaults)
        let delete_ns = SystemRequest::DeleteOrganization { organization: organization_id };
        let (response, _) = store.apply_system(&delete_ns, &mut state);

        match response {
            LedgerResponse::OrganizationDeleted { organization_id: deleted_id, .. } => {
                assert_eq!(deleted_id, organization_id);
            },
            _ => panic!("expected OrganizationDeleted, got {:?}", response),
        }
    }

    #[tokio::test]
    async fn test_delete_organization_not_found() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Try to delete non-existent organization
        let delete_ns =
            SystemRequest::DeleteOrganization { organization: OrganizationId::new(999) };
        let (response, _) = store.apply_system(&delete_ns, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, message } => {
                assert!(message.contains("999"));
                assert!(message.contains("not found"));
            },
            _ => panic!("expected Error response"),
        }
    }

    #[tokio::test]
    async fn test_delete_organization_ignores_deleted_vaults() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Create two vaults
        let create_vault1 = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault1".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault1, &mut state);
        let vault1_id = match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        let create_vault2 = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault2".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault2, &mut state);
        let vault2_id = match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Delete vault1
        let delete_vault =
            OrganizationRequest::DeleteVault { organization: organization_id, vault: vault1_id };
        let (response, _) = store.apply_org(&delete_vault, &mut state);
        match response {
            LedgerResponse::VaultDeleted { success } => assert!(success),
            _ => panic!("expected VaultDeleted"),
        }

        // Delete organization — should soft-delete regardless of active vaults
        let delete_ns = SystemRequest::DeleteOrganization { organization: organization_id };
        let (response, _) = store.apply_system(&delete_ns, &mut state);

        match response {
            LedgerResponse::OrganizationDeleted { organization_id: deleted_id, .. } => {
                assert_eq!(deleted_id, organization_id);
            },
            _ => panic!("expected OrganizationDeleted, got {:?}", response),
        }

        // Verify organization is now in Deleted state
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleted
        );
        let _ = vault2_id;
    }

    // ========================================================================
    // Organization Migration Tests
    // ========================================================================
    //
    // These tests verify organization migration behavior via
    // SystemRequest::UpdateOrganizationRouting. Migration changes the region for a
    // organization, updating routing without data movement.

    #[tokio::test]
    async fn test_migrate_organization_success() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization on US_EAST_VA
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Verify initial state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.region, Region::US_EAST_VA);
        assert_eq!(meta.status, OrganizationStatus::Active);

        // Migrate to IE_EAST_DUBLIN
        let migrate = SystemRequest::UpdateOrganizationRouting {
            organization: organization_id,
            region: Region::IE_EAST_DUBLIN,
        };
        let (response, _) = store.apply_system(&migrate, &mut state);

        match response {
            LedgerResponse::OrganizationMigrated {
                organization: ns_id,
                old_region,
                new_region,
            } => {
                assert_eq!(ns_id, organization_id);
                assert_eq!(old_region, Region::US_EAST_VA);
                assert_eq!(new_region, Region::IE_EAST_DUBLIN);
            },
            _ => panic!("expected OrganizationMigrated, got {:?}", response),
        }

        // Verify updated state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.region, Region::IE_EAST_DUBLIN, "region should be updated");
        assert_eq!(meta.status, OrganizationStatus::Active, "status should remain unchanged");
    }

    #[tokio::test]
    async fn test_migrate_organization_not_found() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Try to migrate non-existent organization
        let migrate = SystemRequest::UpdateOrganizationRouting {
            organization: OrganizationId::new(999),
            region: Region::US_EAST_VA,
        };
        let (response, _) = store.apply_system(&migrate, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, message } => {
                assert!(message.contains("999"), "error should mention organization ID");
                assert!(message.contains("not found"), "error should indicate not found");
            },
            _ => panic!("expected Error response, got {:?}", response),
        }
    }

    #[tokio::test]
    async fn test_migrate_organization_deleted() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create and delete organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Delete the organization
        let delete_ns = SystemRequest::DeleteOrganization { organization: organization_id };
        let (response, _) = store.apply_system(&delete_ns, &mut state);
        match response {
            LedgerResponse::OrganizationDeleted { .. } => {},
            _ => panic!("expected OrganizationDeleted, got {:?}", response),
        }

        // Verify organization is deleted
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.status, OrganizationStatus::Deleted);

        // Try to migrate deleted organization
        let migrate = SystemRequest::UpdateOrganizationRouting {
            organization: organization_id,
            region: Region::US_EAST_VA,
        };
        let (response, _) = store.apply_system(&migrate, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("deleted"), "error should mention deleted organization");
            },
            _ => panic!("expected Error response, got {:?}", response),
        }
    }

    #[tokio::test]
    async fn test_migrate_organization_idempotent_same_region() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization on US_EAST_VA
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Migrate to same region (idempotent case)
        let migrate = SystemRequest::UpdateOrganizationRouting {
            organization: organization_id,
            region: Region::US_EAST_VA,
        };
        let (response, _) = store.apply_system(&migrate, &mut state);

        match response {
            LedgerResponse::OrganizationMigrated {
                organization: ns_id,
                old_region,
                new_region,
            } => {
                assert_eq!(ns_id, organization_id);
                assert_eq!(old_region, Region::US_EAST_VA);
                assert_eq!(new_region, Region::US_EAST_VA, "should be idempotent - same region");
            },
            _ => panic!("expected OrganizationMigrated (idempotent), got {:?}", response),
        }

        // Verify state remains consistent
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.region, Region::US_EAST_VA);
        assert_eq!(meta.status, OrganizationStatus::Active);
    }

    #[tokio::test]
    async fn test_migrate_organization_multiple_times() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Migration 1: US_EAST_VA -> IE_EAST_DUBLIN
        let migrate1 = SystemRequest::UpdateOrganizationRouting {
            organization: organization_id,
            region: Region::IE_EAST_DUBLIN,
        };
        let (response, _) = store.apply_system(&migrate1, &mut state);
        match response {
            LedgerResponse::OrganizationMigrated { old_region, new_region, .. } => {
                assert_eq!(old_region, Region::US_EAST_VA);
                assert_eq!(new_region, Region::IE_EAST_DUBLIN);
            },
            _ => panic!("expected OrganizationMigrated"),
        }

        // Migration 2: IE_EAST_DUBLIN -> US_WEST_OR
        let migrate2 = SystemRequest::UpdateOrganizationRouting {
            organization: organization_id,
            region: Region::US_WEST_OR,
        };
        let (response, _) = store.apply_system(&migrate2, &mut state);
        match response {
            LedgerResponse::OrganizationMigrated { old_region, new_region, .. } => {
                assert_eq!(old_region, Region::IE_EAST_DUBLIN);
                assert_eq!(new_region, Region::US_WEST_OR);
            },
            _ => panic!("expected OrganizationMigrated"),
        }

        // Migration 3: US_WEST_OR -> US_EAST_VA (back to original)
        let migrate3 = SystemRequest::UpdateOrganizationRouting {
            organization: organization_id,
            region: Region::US_EAST_VA,
        };
        let (response, _) = store.apply_system(&migrate3, &mut state);
        match response {
            LedgerResponse::OrganizationMigrated { old_region, new_region, .. } => {
                assert_eq!(old_region, Region::US_WEST_OR);
                assert_eq!(new_region, Region::US_EAST_VA);
            },
            _ => panic!("expected OrganizationMigrated"),
        }

        // Verify final state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.region, Region::US_EAST_VA);
    }

    // ========================================================================
    // Organization Suspension Tests
    // ========================================================================
    //
    // These tests verify organization suspension behavior for billing/policy holds.
    // Suspended organizations reject writes but allow reads.

    #[tokio::test]
    async fn test_suspend_organization_success() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Verify initial state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.status, OrganizationStatus::Active);

        // Suspend the organization
        let suspend = SystemRequest::SuspendOrganization {
            organization: organization_id,
            reason: Some("Payment overdue".to_string()),
        };
        let (response, _) = store.apply_system(&suspend, &mut state);

        match response {
            LedgerResponse::OrganizationSuspended { organization: ns_id } => {
                assert_eq!(ns_id, organization_id);
            },
            _ => panic!("expected OrganizationSuspended, got {:?}", response),
        }

        // Verify suspended state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.status, OrganizationStatus::Suspended);
    }

    #[tokio::test]
    async fn test_resume_organization_success() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create and suspend organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        let suspend =
            SystemRequest::SuspendOrganization { organization: organization_id, reason: None };
        let (response, _) = store.apply_system(&suspend, &mut state);
        match response {
            LedgerResponse::OrganizationSuspended { .. } => {},
            _ => panic!("expected OrganizationSuspended"),
        }

        // Resume the organization
        let resume = SystemRequest::ResumeOrganization { organization: organization_id };
        let (response, _) = store.apply_system(&resume, &mut state);

        match response {
            LedgerResponse::OrganizationResumed { organization: ns_id } => {
                assert_eq!(ns_id, organization_id);
            },
            _ => panic!("expected OrganizationResumed, got {:?}", response),
        }

        // Verify resumed state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.status, OrganizationStatus::Active);
    }

    // ========================================================================
    // Migration Tests
    // ========================================================================

    #[tokio::test]
    async fn test_start_migration_success() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization on region 0
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Start migration to IE_EAST_DUBLIN
        let start_migration = SystemRequest::StartMigration {
            organization: organization_id,
            target_region_group: Region::IE_EAST_DUBLIN,
        };
        let (response, _) = store.apply_system(&start_migration, &mut state);

        match response {
            LedgerResponse::MigrationStarted { organization: ns_id, target_region_group } => {
                assert_eq!(ns_id, organization_id);
                assert_eq!(target_region_group, Region::IE_EAST_DUBLIN);
            },
            _ => panic!("expected MigrationStarted"),
        }

        // Verify organization is in Migrating state with pending region
        let ns = state.organizations.get(&organization_id).unwrap();
        assert_eq!(ns.status, OrganizationStatus::Migrating);
        assert_eq!(ns.region, Region::US_EAST_VA); // Still on old region
        assert_eq!(ns.pending_region, Some(Region::IE_EAST_DUBLIN)); // Target region stored
    }

    #[tokio::test]
    async fn test_start_migration_not_found() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        let start_migration = SystemRequest::StartMigration {
            organization: OrganizationId::new(999),
            target_region_group: Region::US_EAST_VA,
        };
        let (response, _) = store.apply_system(&start_migration, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, message } => {
                assert!(message.contains("not found"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_start_migration_already_migrating() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Start migration
        let start_migration = SystemRequest::StartMigration {
            organization: organization_id,
            target_region_group: Region::IE_EAST_DUBLIN,
        };
        let (response, _) = store.apply_system(&start_migration, &mut state);
        assert!(matches!(response, LedgerResponse::MigrationStarted { .. }));

        // Try to start another migration - should fail
        let start_migration2 = SystemRequest::StartMigration {
            organization: organization_id,
            target_region_group: Region::US_WEST_OR,
        };
        let (response, _) = store.apply_system(&start_migration2, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("already migrating"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_start_migration_suspended_organization() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create and suspend organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        let suspend =
            SystemRequest::SuspendOrganization { organization: organization_id, reason: None };
        store.apply_system(&suspend, &mut state);

        // Try to start migration - should fail
        let start_migration = SystemRequest::StartMigration {
            organization: organization_id,
            target_region_group: Region::IE_EAST_DUBLIN,
        };
        let (response, _) = store.apply_system(&start_migration, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("suspended"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_complete_migration_success() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization on region 0
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Start migration to IE_EAST_DUBLIN
        let start_migration = SystemRequest::StartMigration {
            organization: organization_id,
            target_region_group: Region::IE_EAST_DUBLIN,
        };
        store.apply_system(&start_migration, &mut state);

        // Complete migration
        let complete_migration = SystemRequest::CompleteMigration { organization: organization_id };
        let (response, _) = store.apply_system(&complete_migration, &mut state);

        match response {
            LedgerResponse::MigrationCompleted { organization: ns_id, old_region, new_region } => {
                assert_eq!(ns_id, organization_id);
                assert_eq!(old_region, Region::US_EAST_VA);
                assert_eq!(new_region, Region::IE_EAST_DUBLIN);
            },
            _ => panic!("expected MigrationCompleted"),
        }

        // Verify organization is back to Active on new region
        let ns = state.organizations.get(&organization_id).unwrap();
        assert_eq!(ns.status, OrganizationStatus::Active);
        assert_eq!(ns.region, Region::IE_EAST_DUBLIN);
        assert_eq!(ns.pending_region, None);
    }

    #[tokio::test]
    async fn test_complete_migration_not_migrating() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization (Active state)
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Try to complete migration without starting - should fail
        let complete_migration = SystemRequest::CompleteMigration { organization: organization_id };
        let (response, _) = store.apply_system(&complete_migration, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("not migrating"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_migrating_organization_blocks_writes() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization and vault
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        let create_vault = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault, &mut state);
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Start migration
        let start_migration = SystemRequest::StartMigration {
            organization: organization_id,
            target_region_group: Region::IE_EAST_DUBLIN,
        };
        store.apply_system(&start_migration, &mut state);

        // Try to write - should be blocked
        let write = OrganizationRequest::Write {
            vault: vault_id,
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };
        let (response, _) = store.apply_org(&write, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("migrating"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_migrating_organization_blocks_create_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Start migration
        let start_migration = SystemRequest::StartMigration {
            organization: organization_id,
            target_region_group: Region::IE_EAST_DUBLIN,
        };
        store.apply_system(&start_migration, &mut state);

        // Try to create vault - should be blocked
        let create_vault = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("migrating"));
            },
            _ => panic!("expected Error"),
        }
    }

    // ========================================================================
    // Deleting State Tests
    // ========================================================================

    #[tokio::test]
    async fn test_deleted_organization_vault_delete_does_not_affect_org_status() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Create two vaults
        let create_vault1 = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault1".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault1, &mut state);
        let vault1_id = match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        let create_vault2 = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(2),
            name: Some("vault2".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault2, &mut state);
        let vault2_id = match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Soft-delete the organization
        let delete_ns = SystemRequest::DeleteOrganization { organization: organization_id };
        let (response, _) = store.apply_system(&delete_ns, &mut state);
        assert!(matches!(response, LedgerResponse::OrganizationDeleted { .. }));
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleted
        );

        // Deleting vaults does NOT change the org status (no auto-cascade)
        let delete_vault1 =
            OrganizationRequest::DeleteVault { organization: organization_id, vault: vault1_id };
        let (response, _) = store.apply_org(&delete_vault1, &mut state);
        assert!(matches!(response, LedgerResponse::VaultDeleted { success: true }));
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleted
        );

        let delete_vault2 =
            OrganizationRequest::DeleteVault { organization: organization_id, vault: vault2_id };
        let (response, _) = store.apply_org(&delete_vault2, &mut state);
        assert!(matches!(response, LedgerResponse::VaultDeleted { success: true }));
        // Still Deleted — PurgeOrganization would remove the org entirely
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleted
        );
    }

    #[tokio::test]
    async fn test_deleting_organization_blocks_writes() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization and vault
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        let create_vault = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault, &mut state);
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Mark organization for deletion
        let delete_ns = SystemRequest::DeleteOrganization { organization: organization_id };
        store.apply_system(&delete_ns, &mut state);

        // Try to write - should be blocked
        let write = OrganizationRequest::Write {
            vault: vault_id,
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };
        let (response, _) = store.apply_org(&write, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("deleted") || message.contains("deleting"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_deleting_organization_blocks_create_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization with a vault
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        let create_vault = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("existing-vault".to_string()),
            retention_policy: None,
        };
        store.apply_org(&create_vault, &mut state);

        // Mark organization for deletion
        let delete_ns = SystemRequest::DeleteOrganization { organization: organization_id };
        store.apply_system(&delete_ns, &mut state);

        // Try to create another vault - should be blocked
        let create_vault2 = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("new-vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault2, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("deleted") || message.contains("deleting"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_suspend_organization_write_rejected() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create and suspend organization with a vault
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Create vault before suspending
        let create_vault = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("test-vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault, &mut state);
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Suspend the organization
        let suspend =
            SystemRequest::SuspendOrganization { organization: organization_id, reason: None };
        let (response, _) = store.apply_system(&suspend, &mut state);
        match response {
            LedgerResponse::OrganizationSuspended { .. } => {},
            _ => panic!("expected OrganizationSuspended"),
        }

        // Try to write to suspended organization - should fail
        let write = OrganizationRequest::Write {
            vault: vault_id,
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };
        let (response, _) = store.apply_org(&write, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("suspended"), "error should mention suspended");
            },
            _ => panic!("expected Error for write to suspended organization, got {:?}", response),
        }
    }

    #[tokio::test]
    async fn test_suspend_organization_create_vault_rejected() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create and suspend organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        let suspend =
            SystemRequest::SuspendOrganization { organization: organization_id, reason: None };
        let (response, _) = store.apply_system(&suspend, &mut state);
        match response {
            LedgerResponse::OrganizationSuspended { .. } => {},
            _ => panic!("expected OrganizationSuspended"),
        }

        // Try to create vault in suspended organization - should fail
        let create_vault = OrganizationRequest::CreateVault {
            organization: organization_id,
            slug: VaultSlug::new(1),
            name: Some("test-vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&create_vault, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("suspended"), "error should mention suspended");
            },
            _ => {
                panic!(
                    "expected Error for create vault in suspended organization, got {:?}",
                    response
                )
            },
        }
    }

    #[tokio::test]
    async fn test_suspend_already_suspended_fails() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create and suspend organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        let suspend =
            SystemRequest::SuspendOrganization { organization: organization_id, reason: None };
        let (response, _) = store.apply_system(&suspend, &mut state);
        match response {
            LedgerResponse::OrganizationSuspended { .. } => {},
            _ => panic!("expected OrganizationSuspended"),
        }

        // Try to suspend again - should fail
        let suspend2 =
            SystemRequest::SuspendOrganization { organization: organization_id, reason: None };
        let (response, _) = store.apply_system(&suspend2, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(
                    message.contains("already suspended"),
                    "error should mention already suspended"
                );
            },
            _ => panic!(
                "expected Error for suspending already suspended organization, got {:?}",
                response
            ),
        }
    }

    #[tokio::test]
    async fn test_resume_active_organization_fails() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization (active after activation)
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        // Try to resume active organization - should fail
        let resume = SystemRequest::ResumeOrganization { organization: organization_id };
        let (response, _) = store.apply_system(&resume, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("not suspended"), "error should mention not suspended");
            },
            _ => panic!("expected Error for resuming active organization, got {:?}", response),
        }
    }

    #[tokio::test]
    async fn test_suspend_deleted_organization_fails() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create and delete organization
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        let delete_ns = SystemRequest::DeleteOrganization { organization: organization_id };
        let (response, _) = store.apply_system(&delete_ns, &mut state);
        match response {
            LedgerResponse::OrganizationDeleted { .. } => {},
            _ => panic!("expected OrganizationDeleted, got {:?}", response),
        }

        // Try to suspend deleted organization - should fail
        let suspend =
            SystemRequest::SuspendOrganization { organization: organization_id, reason: None };
        let (response, _) = store.apply_system(&suspend, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("deleted"), "error should mention deleted organization");
            },
            _ => panic!("expected Error for suspending deleted organization, got {:?}", response),
        }
    }

    #[tokio::test]
    async fn test_suspend_organization_not_found() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Try to suspend non-existent organization
        let suspend = SystemRequest::SuspendOrganization {
            organization: OrganizationId::new(999),
            reason: None,
        };
        let (response, _) = store.apply_system(&suspend, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, message } => {
                assert!(message.contains("999"), "error should mention organization ID");
                assert!(message.contains("not found"), "error should mention not found");
            },
            _ => panic!(
                "expected Error for suspending non-existent organization, got {:?}",
                response
            ),
        }
    }

    #[tokio::test]
    async fn test_resume_organization_not_found() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Try to resume non-existent organization
        let resume = SystemRequest::ResumeOrganization { organization: OrganizationId::new(999) };
        let (response, _) = store.apply_system(&resume, &mut state);

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, message } => {
                assert!(message.contains("999"), "error should mention organization ID");
                assert!(message.contains("not found"), "error should mention not found");
            },
            _ => {
                panic!("expected Error for resuming non-existent organization, got {:?}", response)
            },
        }
    }

    #[tokio::test]
    async fn test_delete_suspended_organization_succeeds() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create and suspend organization (no vaults)
        let organization_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );

        let suspend =
            SystemRequest::SuspendOrganization { organization: organization_id, reason: None };
        let (response, _) = store.apply_system(&suspend, &mut state);
        match response {
            LedgerResponse::OrganizationSuspended { .. } => {},
            _ => panic!("expected OrganizationSuspended"),
        }

        // Delete suspended organization - should succeed
        let delete_ns = SystemRequest::DeleteOrganization { organization: organization_id };
        let (response, _) = store.apply_system(&delete_ns, &mut state);

        match response {
            LedgerResponse::OrganizationDeleted { .. } => {},
            _ => panic!("expected OrganizationDeleted, got {:?}", response),
        }

        // Verify deleted state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.status, OrganizationStatus::Deleted);
    }

    // These tests verify that the state machine is deterministic - a critical
    // requirement for Raft consensus. All nodes must produce identical state
    // when applying the same log entries.
    //
    // CRITICAL: The state machine must NEVER use:
    // - rand::random() or any RNG
    // - SystemTime::now() for state (only logging)
    // - HashMap iteration order (use BTreeMap for deterministic ordering)
    // - Floating point operations that vary by platform
    // - Any external I/O that could vary between nodes

    /// Verifies same input sequence produces identical outputs on independent state machines.
    ///
    /// This is the fundamental Raft invariant: if two nodes apply the same log entries
    /// in the same order, they MUST produce identical state.
    #[tokio::test]
    async fn test_deterministic_apply() {
        // Create two independent state machines (simulating two Raft nodes)
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        // Apply to both nodes A and B
        let mut state_a = (*store_a.applied_state.load_full()).clone();
        let mut state_b = (*store_b.applied_state.load_full()).clone();
        let mut results_a: Vec<LedgerResponse> = Vec::new();
        let mut results_b: Vec<LedgerResponse> = Vec::new();

        // Helper: apply same sequence to two stores, collect responses
        macro_rules! apply_both_system {
            ($req:expr) => {{
                let req = $req;
                let ra = store_a.apply_system(&req, &mut state_a).0;
                let rb = store_b.apply_system(&req, &mut state_b).0;
                results_a.push(ra);
                results_b.push(rb);
            }};
        }
        macro_rules! apply_both_org {
            ($req:expr) => {{
                let req = $req;
                let ra = store_a.apply_org(&req, &mut state_a).0;
                let rb = store_b.apply_org(&req, &mut state_b).0;
                results_a.push(ra);
                results_b.push(rb);
            }};
        }

        apply_both_system!(SystemRequest::CreateOrganization {
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            region: Region::US_EAST_VA,
            tier: Default::default(),
            admin: UserId::new(1),
        });
        apply_both_system!(SystemRequest::UpdateOrganizationStatus {
            organization: OrganizationId::new(1),
            status: OrganizationStatus::Active,
        });
        apply_both_system!(SystemRequest::CreateOrganization {
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            region: Region::US_EAST_VA,
            tier: Default::default(),
            admin: UserId::new(1),
        });
        apply_both_system!(SystemRequest::UpdateOrganizationStatus {
            organization: OrganizationId::new(2),
            status: OrganizationStatus::Active,
        });
        apply_both_org!(OrganizationRequest::CreateVault {
            organization: OrganizationId::new(1),
            slug: VaultSlug::new(1),
            name: Some("production".to_string()),
            retention_policy: None,
        });
        apply_both_org!(OrganizationRequest::CreateVault {
            organization: OrganizationId::new(1),
            slug: VaultSlug::new(1),
            name: Some("staging".to_string()),
            retention_policy: None,
        });
        apply_both_org!(OrganizationRequest::CreateVault {
            organization: OrganizationId::new(2),
            slug: VaultSlug::new(1),
            name: Some("main".to_string()),
            retention_policy: None,
        });
        apply_both_org!(OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        });
        apply_both_org!(OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        });
        apply_both_org!(OrganizationRequest::Write {
            vault: VaultId::new(3),
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        });
        apply_both_system!(SystemRequest::CreateUser {
            user: UserId::new(1),
            admin: false,
            slug: UserSlug::new(111),
            region: Region::US_EAST_VA,
        });
        apply_both_system!(SystemRequest::CreateUser {
            user: UserId::new(2),
            admin: false,
            slug: UserSlug::new(222),
            region: Region::US_EAST_VA,
        });

        store_a.applied_state.store(Arc::new(state_a));
        store_b.applied_state.store(Arc::new(state_b));

        // Results must be identical
        assert_eq!(results_a, results_b, "Same inputs must produce identical results on all nodes");

        // Final state must be identical
        let final_state_a = store_a.applied_state.load();
        let final_state_b = store_b.applied_state.load();

        assert_eq!(
            final_state_a.sequences, final_state_b.sequences,
            "Sequence counters must match"
        );
        assert_eq!(
            final_state_a.vault_heights, final_state_b.vault_heights,
            "Vault heights must match"
        );
        assert_eq!(
            final_state_a.vault_health, final_state_b.vault_health,
            "Vault health must match"
        );
    }

    /// Verifies ID generation is deterministic across state machines.
    ///
    /// IDs are assigned by the leader during log application. All nodes must
    /// generate the same IDs for the same sequence of requests.
    #[tokio::test]
    async fn test_deterministic_id_generation() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        // Apply same sequence on both nodes
        let mut state_a = (*store_a.applied_state.load_full()).clone();
        let mut state_b = (*store_b.applied_state.load_full()).clone();

        // Organization IDs
        let ns_id_a1 = state_a.sequences.next_organization();
        let ns_id_a2 = state_a.sequences.next_organization();
        let ns_id_b1 = state_b.sequences.next_organization();
        let ns_id_b2 = state_b.sequences.next_organization();

        assert_eq!(ns_id_a1, ns_id_b1, "First organization ID must match");
        assert_eq!(ns_id_a2, ns_id_b2, "Second organization ID must match");

        // Vault IDs
        let vault_id_a1 = state_a.sequences.next_vault();
        let vault_id_a2 = state_a.sequences.next_vault();
        let vault_id_b1 = state_b.sequences.next_vault();
        let vault_id_b2 = state_b.sequences.next_vault();

        assert_eq!(vault_id_a1, vault_id_b1, "First vault ID must match");
        assert_eq!(vault_id_a2, vault_id_b2, "Second vault ID must match");

        // User IDs
        let user_id_a1 = state_a.sequences.next_user();
        let user_id_a2 = state_a.sequences.next_user();
        let user_id_b1 = state_b.sequences.next_user();
        let user_id_b2 = state_b.sequences.next_user();

        assert_eq!(user_id_a1, user_id_b1, "First user ID must match");
        assert_eq!(user_id_a2, user_id_b2, "Second user ID must match");
    }

    /// Verifies vault height tracking is deterministic.
    ///
    /// Writes to the same vault must increment height consistently across nodes.
    #[tokio::test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    async fn test_deterministic_vault_heights() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        let mut state_a = (*store_a.applied_state.load_full()).clone();
        let mut state_b = (*store_b.applied_state.load_full()).clone();

        // Register active org on both nodes
        let org_slug = inferadb_ledger_types::OrganizationSlug::new(100);
        let org_id_a =
            create_active_organization(&store_a, &mut state_a, org_slug, Region::US_EAST_VA);
        let org_id_b =
            create_active_organization(&store_b, &mut state_b, org_slug, Region::US_EAST_VA);
        assert_eq!(org_id_a, org_id_b, "Both stores should assign the same org ID");

        // Create vault on both nodes
        let create_vault = OrganizationRequest::CreateVault {
            organization: org_id_a,
            slug: VaultSlug::new(1),
            name: Some("test".to_string()),
            retention_policy: None,
        };
        store_a.apply_org(&create_vault, &mut state_a);
        store_b.apply_org(&create_vault, &mut state_b);

        // Apply multiple writes
        for _ in 0..5 {
            let write = OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            };
            store_a.apply_org(&write, &mut state_a);
            store_b.apply_org(&write, &mut state_b);
        }

        // Heights must match
        assert_eq!(
            state_a.vault_heights.get(&(org_id_a, VaultId::new(1))),
            state_b.vault_heights.get(&(org_id_b, VaultId::new(1))),
            "Vault heights must be identical after same operations"
        );
        assert_eq!(
            state_a.vault_heights.get(&(org_id_a, VaultId::new(1))),
            Some(&5),
            "Height should be 5 after 5 writes"
        );
    }

    /// Verifies interleaved operations across multiple vaults are deterministic.
    ///
    /// Real workloads have writes to multiple vaults interleaved. The state
    /// machine must handle this deterministically.
    #[tokio::test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    async fn test_deterministic_interleaved_operations() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        let mut state_a = (*store_a.applied_state.load_full()).clone();
        let mut state_b = (*store_b.applied_state.load_full()).clone();

        // Create organization and vaults (applied to both stores)
        for req in &[
            SystemRequest::CreateOrganization {
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                region: Region::US_EAST_VA,
                tier: Default::default(),
                admin: UserId::new(1),
            },
            SystemRequest::UpdateOrganizationStatus {
                organization: OrganizationId::new(1),
                status: OrganizationStatus::Active,
            },
        ] {
            store_a.apply_system(req, &mut state_a);
            store_b.apply_system(req, &mut state_b);
        }
        for req in &[
            OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("vault-a".to_string()),
                retention_policy: None,
            },
            OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("vault-b".to_string()),
                retention_policy: None,
            },
        ] {
            store_a.apply_org(req, &mut state_a);
            store_b.apply_org(req, &mut state_b);
        }

        // Interleaved writes to different vaults
        let interleaved = vec![
            OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            OrganizationRequest::Write {
                vault: VaultId::new(2),
                transactions: vec![],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            OrganizationRequest::Write {
                vault: VaultId::new(2),
                transactions: vec![],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
        ];

        let mut results_a = Vec::new();
        let mut results_b = Vec::new();

        for req in &interleaved {
            let (response_a, _) = store_a.apply_org(req, &mut state_a);
            let (response_b, _) = store_b.apply_org(req, &mut state_b);
            results_a.push(response_a);
            results_b.push(response_b);
        }

        // Results must match
        assert_eq!(results_a, results_b, "Interleaved operation results must match");

        // Vault 1: 3 writes, Vault 2: 2 writes
        assert_eq!(state_a.vault_heights.get(&(OrganizationId::new(1), VaultId::new(1))), Some(&3));
        assert_eq!(state_a.vault_heights.get(&(OrganizationId::new(1), VaultId::new(2))), Some(&2));
        assert_eq!(state_a.vault_heights, state_b.vault_heights);
    }

    /// Verifies state can be serialized and deserialized deterministically.
    ///
    /// Snapshots must serialize to the same bytes on all nodes for the same state.
    #[tokio::test]
    async fn test_deterministic_state_serialization() {
        let mut state_a = AppliedState { sequences: SequenceCounters::new(), ..Default::default() };
        let mut state_b = AppliedState { sequences: SequenceCounters::new(), ..Default::default() };

        // Apply same mutations
        state_a.sequences.next_organization();
        state_a.sequences.next_vault();
        state_a.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 42);

        state_b.sequences.next_organization();
        state_b.sequences.next_vault();
        state_b.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 42);

        // Serialize both
        let bytes_a = postcard::to_allocvec(&state_a).expect("serialize a");
        let bytes_b = postcard::to_allocvec(&state_b).expect("serialize b");

        assert_eq!(bytes_a, bytes_b, "Serialized state must be identical");

        // Deserialize and verify
        let restored_a: AppliedState = postcard::from_bytes(&bytes_a).expect("deserialize a");
        let restored_b: AppliedState = postcard::from_bytes(&bytes_b).expect("deserialize b");

        assert_eq!(restored_a.sequences, restored_b.sequences);
        assert_eq!(restored_a.vault_heights, restored_b.vault_heights);
    }

    /// Verifies that sequence counters start at well-defined values.
    ///
    /// All nodes must start with the same initial counter values.
    #[test]
    fn test_sequence_counters_initial_values() {
        let counters = SequenceCounters::new();

        // Verify initial values:
        // - organization 0 is reserved for _system
        // - IDs start at 1
        assert_eq!(
            counters.organization,
            OrganizationId::new(1),
            "Organization counter should start at 1"
        );
        assert_eq!(counters.vault, VaultId::new(1), "Vault counter should start at 1");
        assert_eq!(counters.user, UserId::new(1), "User counter should start at 1");
        assert_eq!(
            counters.user_email,
            UserEmailId::new(1),
            "User email counter should start at 1"
        );
        assert_eq!(
            counters.email_verify,
            EmailVerifyTokenId::new(1),
            "Email verify counter should start at 1"
        );
    }

    // ========================================================================
    // State Machine Integration Tests
    // ========================================================================
    //
    // These tests verify the full state machine flow including StateLayer
    // integration, block creation, and snapshot persistence.

    /// Test that Write with transactions produces a VaultEntry with proper fields.
    ///
    /// This verifies the critical path: Write → apply → VaultEntry creation.
    #[tokio::test]
    async fn test_write_produces_vault_entry() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Setup: create organization and vault
        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );
        store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("vault1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Apply a write with transactions
        let tx = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: ClientId::new("test-client"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "key1".to_string(),
                value: b"value1".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };

        let request = OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![tx],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let (response, vault_entry) = store.apply_org(&request, &mut state);

        // Verify response
        match response {
            LedgerResponse::Write { block_height, .. } => {
                assert_eq!(block_height, 1, "First write should be height 1");
            },
            _ => panic!("Expected Write response"),
        }

        // Verify VaultEntry was created
        let entry = vault_entry.expect("VaultEntry should be created");
        assert_eq!(entry.organization, OrganizationId::new(1));
        assert_eq!(entry.vault, VaultId::new(1));
        assert_eq!(entry.vault_height, 1);
        assert_eq!(entry.transactions.len(), 1);
        // state_root and tx_merkle_root will be ZERO_HASH without StateLayer configured
        // but the structure should be correct
    }

    /// Test that region_height is tracked in AppliedState for snapshot persistence.
    #[tokio::test]
    async fn test_region_height_tracked_in_applied_state() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        // Initial region height should be 0
        assert_eq!(store.current_region_height(), 0);

        // After applying entries, region height should increment
        // Note: full region height increment requires apply_to_state_machine
        // which creates RegionBlocks. This test verifies the accessor.
        let state = store.applied_state.load();
        assert_eq!(state.region_height, 0, "Initial region height should be 0");
    }

    /// Test that AppliedState serialization preserves all fields including region tracking.
    #[tokio::test]
    async fn test_applied_state_snapshot_round_trip() {
        let mut original = AppliedState {
            last_applied: Some(make_log_id(1, 10)),
            region_height: 42,
            previous_region_hash: [0xAB; 32],
            ..Default::default()
        };

        // Add some data
        original.sequences.next_organization();
        original.sequences.next_vault();
        original.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 100);
        original.vault_heights.insert((OrganizationId::new(1), VaultId::new(2)), 50);
        original.vault_health.insert(
            (OrganizationId::new(2), VaultId::new(1)),
            VaultHealthStatus::Diverged { expected: [1u8; 32], computed: [2u8; 32], at_height: 10 },
        );
        original
            .previous_vault_hashes
            .insert((OrganizationId::new(1), VaultId::new(1)), [0xCD; 32]);
        original.organizations.insert(
            OrganizationId::new(1),
            OrganizationMeta {
                organization: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                region: Region::GLOBAL,
                status: OrganizationStatus::Active,
                tier: OrganizationTier::Free,
                pending_region: None,
                storage_bytes: 0,
            },
        );
        original.vaults.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultMeta {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                deleted: false,
                last_write_timestamp: 1234567899,
                retention_policy: BlockRetentionPolicy::default(),
            },
        );

        // Serialize and deserialize
        let bytes = postcard::to_allocvec(&original).expect("serialize");
        let restored: AppliedState = postcard::from_bytes(&bytes).expect("deserialize");

        // Verify key fields restored
        assert_eq!(restored.sequences, original.sequences);
        assert_eq!(restored.vault_heights, original.vault_heights);
        assert_eq!(restored.vault_health, original.vault_health);
        assert_eq!(restored.previous_vault_hashes, original.previous_vault_hashes);
        assert_eq!(restored.region_height, 42, "region_height must be preserved");
        assert_eq!(
            restored.previous_region_hash, [0xAB; 32],
            "previous_region_hash must be preserved"
        );
        // Verify organization and vault counts (HashMaps don't implement PartialEq for complex
        // types)
        assert_eq!(restored.organizations.len(), 1);
        assert_eq!(restored.vaults.len(), 1);
        assert!(restored.organizations.contains_key(&OrganizationId::new(1)));
        assert!(restored.vaults.contains_key(&(OrganizationId::new(1), VaultId::new(1))));
    }

    /// Test that AppliedStateAccessor provides correct data.
    #[tokio::test]
    async fn test_applied_state_accessor() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let accessor = store.accessor();

        // Setup some state
        {
            let mut state = (*store.applied_state.load_full()).clone();
            state.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 42);
            state.vault_heights.insert((OrganizationId::new(1), VaultId::new(2)), 100);
            state.region_height = 99;
            state.organizations.insert(
                OrganizationId::new(1),
                OrganizationMeta {
                    organization: OrganizationId::new(1),
                    slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    region: Region::GLOBAL,
                    status: OrganizationStatus::Active,
                    tier: OrganizationTier::Free,
                    pending_region: None,
                    storage_bytes: 0,
                },
            );
            store.applied_state.store(Arc::new(state));
        }

        // Test accessor methods
        assert_eq!(accessor.vault_height(OrganizationId::new(1), VaultId::new(1)), 42);
        assert_eq!(accessor.vault_height(OrganizationId::new(1), VaultId::new(2)), 100);
        assert_eq!(accessor.vault_height(OrganizationId::new(1), VaultId::new(99)), 0); // Non-existent returns 0
        assert_eq!(accessor.region_height(), 99);

        assert_eq!(accessor.vault_height_count(), 2);
        assert_eq!(accessor.vault_height(OrganizationId::new(1), VaultId::new(1)), 42);

        assert!(accessor.get_organization(OrganizationId::new(1)).is_some());
        assert!(accessor.get_organization(OrganizationId::new(99)).is_none());
    }

    #[tokio::test]
    async fn test_block_announcements_sender_stored() {
        use inferadb_ledger_proto::proto::{
            BlockAnnouncement, Hash, OrganizationSlug as ProtoOrganizationSlug,
            VaultSlug as ProtoVaultSlug,
        };

        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        // Open store without sender - should be None
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        assert!(store.block_announcements().is_none());

        // Create new store with sender
        let (sender, mut receiver) = broadcast::channel::<BlockAnnouncement>(16);
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1))
            .with_block_announcements(sender);

        // Verify sender is stored and accessible
        assert!(store.block_announcements().is_some());

        // Verify we can send through the stored sender
        let announcement = BlockAnnouncement {
            organization: Some(ProtoOrganizationSlug { slug: 1 }),
            vault: Some(ProtoVaultSlug { slug: 2 }),
            height: 3,
            block_hash: Some(Hash { value: vec![0u8; 32] }),
            state_root: Some(Hash { value: vec![0u8; 32] }),
            timestamp: None, // Optional field
        };

        store.block_announcements().unwrap().send(announcement.clone()).expect("send");

        // Verify receiver gets the announcement
        let received = receiver.recv().await.expect("receive");
        assert_eq!(received.organization, announcement.organization);
        assert_eq!(received.vault, announcement.vault);
        assert_eq!(received.height, announcement.height);
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_broadcasts_block_announcements() {
        use std::time::{Duration, Instant};

        use inferadb_ledger_proto::proto::{
            BlockAnnouncement, OrganizationSlug as ProtoOrganizationSlug,
            VaultSlug as ProtoVaultSlug,
        };
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        // Create store with broadcast sender
        let (sender, mut receiver) = broadcast::channel::<BlockAnnouncement>(16);
        let mut store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1))
            .with_block_announcements(sender);

        // First, create organization and vault using apply_request (sets up state)
        {
            let mut state = (*store.applied_state.load_full()).clone();

            let organization_id = create_active_organization(
                &store,
                &mut state,
                inferadb_ledger_types::OrganizationSlug::new(42),
                Region::US_EAST_VA,
            );
            assert_eq!(organization_id, OrganizationId::new(1));

            let create_vault = OrganizationRequest::CreateVault {
                organization: organization_id,
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            };
            let (response, _) = store.apply_org(&create_vault, &mut state);
            let vault_id = match response {
                LedgerResponse::VaultCreated { vault: vault_id, .. } => vault_id,
                _ => panic!("expected VaultCreated"),
            };
            assert_eq!(vault_id, VaultId::new(1));
            store.applied_state.store(Arc::new(state));
        }

        // Now call apply_to_state_machine with a Write entry
        // This should broadcast a BlockAnnouncement.
        // γ Phase 3a: the announcement path reads external slugs from the
        // stamped `VaultEntry` (not from `state.id_to_slug`). The write-path
        // service layer lifts these from the incoming gRPC request; this
        // test exercises the apply handler directly so we stamp the same
        // slugs the service layer would.
        let write_request = OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![], // Empty transactions still create a block
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(42),
            vault_slug: inferadb_ledger_types::VaultSlug::new(1),
        };

        let entry = make_committed_entry(1, 1, wrap_org_payload(write_request));

        let _start = Instant::now();
        let responses = store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[entry], None)
            .await
            .expect("apply");

        // Verify response is WriteCompleted
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            LedgerResponse::Write { block_height: height, .. } => {
                assert_eq!(*height, 1, "Expected height 1 for first block");
            },
            other => panic!("expected WriteCompleted, got {:?}", other),
        }

        // Verify announcement was broadcast (should be near-instant, but allow
        // headroom for parallel CI where file-backed B+ tree I/O competes for disk)
        let timeout = Duration::from_secs(2);
        let received = tokio::time::timeout(timeout, receiver.recv())
            .await
            .expect("announcement should arrive within timeout")
            .expect("should receive announcement");

        // Verify announcement contents
        assert_eq!(received.organization, Some(ProtoOrganizationSlug { slug: 42 }));
        assert_eq!(received.vault, Some(ProtoVaultSlug { slug: 1 }));
        assert_eq!(received.height, 1);
        assert!(received.block_hash.is_some(), "block_hash should be set");
        assert!(received.state_root.is_some(), "state_root should be set");
        assert!(received.timestamp.is_some(), "timestamp should be set");
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_no_broadcast_without_sender() {
        // Verify that without a sender, apply_to_state_machine still works
        // (graceful handling of None sender)
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        // Store without broadcast sender
        let mut store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        // Create organization and vault
        {
            let mut state = (*store.applied_state.load_full()).clone();

            create_active_organization(
                &store,
                &mut state,
                inferadb_ledger_types::OrganizationSlug::new(0),
                Region::US_EAST_VA,
            );

            let create_vault = OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            };
            store.apply_org(&create_vault, &mut state);
            store.applied_state.store(Arc::new(state));
        }

        // Apply write - should not panic even without sender
        let write_request = OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let entry = make_committed_entry(1, 1, wrap_org_payload(write_request));

        let responses = store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[entry], None)
            .await
            .expect("apply");
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            LedgerResponse::Write { .. } => {},
            other => panic!("expected WriteCompleted, got {:?}", other),
        }
    }

    // ========================================================================
    // Deterministic Timestamp Tests
    // ========================================================================
    //
    // These tests verify that all replicas produce byte-identical event storage
    // when applying the same Raft log entries, by using leader-assigned
    // timestamps from `RaftPayload.proposed_at` instead of local `Utc::now()`.

    #[tokio::test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    async fn test_apply_uses_proposed_at_not_utc_now() {
        // Critical test: apply with proposed_at set to year 2099 — if any code
        // path still calls Utc::now(), timestamps will be ~2026 instead of 2099.
        use chrono::{Datelike, TimeZone};
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let mut store = store_with_events(dir.path());

        // Set up org and vault
        {
            let mut state = (*store.applied_state.load_full()).clone();
            setup_org_and_vault(&mut state);
            store.applied_state.store(Arc::new(state));
        }

        let far_future = Utc.with_ymd_and_hms(2099, 6, 15, 12, 0, 0).unwrap();

        let write_request = OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![Transaction {
                id: [42u8; 16],
                client_id: ClientId::new("test"),
                sequence: 0,
                operations: vec![Operation::SetEntity {
                    key: "k1".to_string(),
                    value: b"v1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                timestamp: far_future,
            }],
            idempotency_key: [0u8; 16],
            request_hash: 0,

            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let payload = RaftPayload {
            request: write_request,
            proposed_at: far_future,
            state_root_commitments: vec![],
            caller: 0,
        };

        let entry = make_committed_entry(1, 1, payload);

        let responses = store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[entry], None)
            .await
            .expect("apply");
        assert_eq!(responses.len(), 1);

        // Read events from the events DB
        let ew = store.event_writer().expect("event writer");
        let events_db = ew.events_db();
        let txn = events_db.read().expect("read txn");
        let (events, _) = EventStore::list(&txn, OrganizationId::new(1), 0, u64::MAX, 1000, None)
            .expect("list events");

        // ALL event timestamps must be in 2099 (the proposed_at year)
        assert!(!events.is_empty(), "should have events");
        for event in &events {
            assert_eq!(
                event.timestamp.year(),
                2099,
                "event timestamp should use proposed_at (2099), not Utc::now(). Got year {}",
                event.timestamp.year()
            );
        }
    }

    #[tokio::test]
    async fn test_region_block_uses_proposed_at_timestamp() {
        // Verify that RegionBlock timestamp also comes from proposed_at,
        // not a separate Utc::now() call.
        use chrono::TimeZone;
        use inferadb_ledger_proto::proto::BlockAnnouncement;
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let (sender, mut receiver) = broadcast::channel::<BlockAnnouncement>(16);
        let mut store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1))
            .with_block_announcements(sender);

        // Create org + vault
        {
            let mut state = (*store.applied_state.load_full()).clone();
            create_active_organization(
                &store,
                &mut state,
                inferadb_ledger_types::OrganizationSlug::new(99),
                Region::US_EAST_VA,
            );

            let create_vault = OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            };
            store.apply_org(&create_vault, &mut state);
            store.applied_state.store(Arc::new(state));
        }

        let far_future = Utc.with_ymd_and_hms(2099, 12, 31, 23, 59, 59).unwrap();

        let write_request = OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let payload = RaftPayload {
            request: write_request,
            proposed_at: far_future,
            state_root_commitments: vec![],
            caller: 0,
        };
        let entry = make_committed_entry(1, 1, payload);

        store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[entry], None)
            .await
            .expect("apply");

        // Check the broadcast announcement timestamp
        let announcement = receiver.try_recv().expect("should have received block announcement");

        let ts = announcement.timestamp.expect("timestamp should be set");
        assert_eq!(
            ts.seconds,
            far_future.timestamp(),
            "RegionBlock timestamp should use proposed_at (2099), not Utc::now()"
        );
    }

    #[tokio::test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    async fn test_two_state_machines_produce_identical_events() {
        // Apply the same request on two independent state machines.
        // With deterministic timestamps, resulting EventEntry records must be
        // byte-identical.
        use chrono::TimeZone;
        // openraft trait removed — tests use apply_committed_entries directly

        let far_future = Utc.with_ymd_and_hms(2099, 3, 14, 15, 9, 26).unwrap();

        let write_request = OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![Transaction {
                id: [7u8; 16],
                client_id: ClientId::new("client"),
                sequence: 0,
                operations: vec![Operation::SetEntity {
                    key: "key".to_string(),
                    value: b"value".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                timestamp: far_future,
            }],
            idempotency_key: [0u8; 16],
            request_hash: 0,

            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        // Helper to create a store, set up state, and apply
        let apply_on_fresh_store = |dir: &std::path::Path| {
            let store = store_with_events(dir);
            {
                let mut state = (*store.applied_state.load_full()).clone();
                setup_org_and_vault(&mut state);
                store.applied_state.store(Arc::new(state));
            }
            let payload = RaftPayload {
                request: write_request.clone(),
                proposed_at: far_future,
                state_root_commitments: vec![],
                caller: 0,
            };
            let entry = make_committed_entry(1, 1, payload);
            (store, entry)
        };

        let dir1 = tempdir().expect("dir1");
        let dir2 = tempdir().expect("dir2");

        let (mut store1, entry1) = apply_on_fresh_store(dir1.path());
        let (mut store2, entry2) = apply_on_fresh_store(dir2.path());

        store1
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[entry1], None)
            .await
            .expect("apply1");
        store2
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[entry2], None)
            .await
            .expect("apply2");

        // Read events from both stores
        let read_events = |store: &RaftLogStore<FileBackend>| {
            let ew = store.event_writer().expect("event writer");
            let db = ew.events_db();
            let txn = db.read().expect("read");
            EventStore::list(&txn, OrganizationId::new(1), 0, u64::MAX, 1000, None).expect("list").0
        };

        let events1 = read_events(&store1);
        let events2 = read_events(&store2);

        assert_eq!(events1.len(), events2.len(), "event count should match");
        assert!(!events1.is_empty(), "should have events");

        for (e1, e2) in events1.iter().zip(events2.iter()) {
            assert_eq!(e1.event_id, e2.event_id, "event IDs should match");
            assert_eq!(e1.timestamp, e2.timestamp, "timestamps should match");
            // Byte-identical serialization
            let bytes1 = inferadb_ledger_types::encode(e1).expect("encode1");
            let bytes2 = inferadb_ledger_types::encode(e2).expect("encode2");
            assert_eq!(bytes1, bytes2, "serialized events should be byte-identical");
        }
    }

    // ========================================================================
    // Divergence Recovery Lifecycle Tests
    // ========================================================================
    //
    // These tests verify the complete recovery lifecycle through the state
    // machine: Healthy → Diverged → Recovering (attempts 1..N) → Healthy,
    // including circuit breaker behavior at MAX_RECOVERY_ATTEMPTS.

    #[tokio::test]
    async fn test_recovery_lifecycle_healthy_diverged_recovering_healthy() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // 1. Start healthy
        state
            .vault_health
            .insert((OrganizationId::new(1), VaultId::new(1)), VaultHealthStatus::Healthy);
        assert_eq!(
            state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(&VaultHealthStatus::Healthy)
        );

        // 2. Transition to Diverged (detected by auto-recovery scanner)
        let diverge = OrganizationRequest::UpdateVaultHealth {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            healthy: false,
            expected_root: Some([0xAA; 32]),
            computed_root: Some([0xBB; 32]),
            diverged_at_height: Some(100),
            recovery_attempt: None,
            recovery_started_at: None,
        };
        let (response, _) = store.apply_org(&diverge, &mut state);
        assert!(matches!(response, LedgerResponse::VaultHealthUpdated { success: true }));
        assert!(matches!(
            state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(VaultHealthStatus::Diverged { at_height: 100, .. })
        ));

        // 3. Transition to Recovering attempt 1
        let now = chrono::Utc::now().timestamp();
        let recover1 = OrganizationRequest::UpdateVaultHealth {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            healthy: false,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: Some(1),
            recovery_started_at: Some(now),
        };
        let (response, _) = store.apply_org(&recover1, &mut state);
        assert!(matches!(response, LedgerResponse::VaultHealthUpdated { success: true }));
        assert!(matches!(
            state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(VaultHealthStatus::Recovering { attempt: 1, .. })
        ));

        // Recovering vaults do NOT block writes (only Diverged does)
        // Recovery happens in the background via replay, not inline

        // 4. Recovery succeeds → Healthy
        let healthy = OrganizationRequest::UpdateVaultHealth {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            healthy: true,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: None,
            recovery_started_at: None,
        };
        let (response, _) = store.apply_org(&healthy, &mut state);
        assert!(matches!(response, LedgerResponse::VaultHealthUpdated { success: true }));
        assert_eq!(
            state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(&VaultHealthStatus::Healthy)
        );
    }

    #[tokio::test]
    async fn test_recovery_repeated_failure_escalating_attempts() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Start diverged
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Diverged {
                expected: [0xAA; 32],
                computed: [0xBB; 32],
                at_height: 50,
            },
        );

        let base_time = chrono::Utc::now().timestamp();

        // Simulate escalating recovery attempts (1, 2, 3)
        for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
            let recover = OrganizationRequest::UpdateVaultHealth {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                healthy: false,
                expected_root: None,
                computed_root: None,
                diverged_at_height: None,
                recovery_attempt: Some(attempt),
                recovery_started_at: Some(base_time + i64::from(attempt) * 10),
            };
            let (response, _) = store.apply_org(&recover, &mut state);
            assert!(matches!(response, LedgerResponse::VaultHealthUpdated { success: true }));

            match state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))) {
                Some(VaultHealthStatus::Recovering { attempt: a, .. }) => {
                    assert_eq!(*a, attempt);
                },
                other => panic!("expected Recovering with attempt {attempt}, got {:?}", other),
            }
        }
    }

    #[tokio::test]
    async fn test_recovery_circuit_breaker_max_attempts() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Start diverged
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Diverged {
                expected: [0xAA; 32],
                computed: [0xBB; 32],
                at_height: 50,
            },
        );

        // Exhaust all attempts
        let now = chrono::Utc::now().timestamp();
        for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
            let recover = OrganizationRequest::UpdateVaultHealth {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                healthy: false,
                expected_root: None,
                computed_root: None,
                diverged_at_height: None,
                recovery_attempt: Some(attempt),
                recovery_started_at: Some(now),
            };
            let _ = store.apply_org(&recover, &mut state);
        }

        // Verify vault is at max recovery attempt
        match state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))) {
            Some(VaultHealthStatus::Recovering { attempt, .. }) => {
                assert_eq!(*attempt, MAX_RECOVERY_ATTEMPTS);
            },
            other => panic!("expected Recovering at max attempt, got {:?}", other),
        }

        // Writes should still go through when in Recovering state
        // (only Diverged blocks writes)
        // The circuit breaker is enforced by AutoRecoveryJob not attempting
        // recovery beyond MAX_RECOVERY_ATTEMPTS
    }

    #[tokio::test]
    async fn test_diverged_vault_blocks_writes_recovering_does_not() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization so writes don't fail for missing organization
        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );
        let create_vault = OrganizationRequest::CreateVault {
            organization: OrganizationId::new(1),
            slug: VaultSlug::new(1),
            name: Some("vault".to_string()),
            retention_policy: None,
        };
        store.apply_org(&create_vault, &mut state);

        // Diverged blocks writes
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Diverged { expected: [1; 32], computed: [2; 32], at_height: 1 },
        );
        let write = OrganizationRequest::Write {
            vault: VaultId::new(1),
            transactions: vec![],
            idempotency_key: [0u8; 16],
            request_hash: 0,
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };
        let (response, _) = store.apply_org(&write, &mut state);
        assert!(matches!(response, LedgerResponse::Error { .. }));

        // Recovering does NOT block writes (recovery is background replay)
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Recovering {
                started_at: chrono::Utc::now().timestamp(),
                attempt: 1,
            },
        );
        let (response, _) = store.apply_org(&write, &mut state);
        // Should succeed (Write response, not Error)
        assert!(
            matches!(response, LedgerResponse::Write { .. }),
            "expected Write response during Recovering, got {:?}",
            response
        );
    }

    #[tokio::test]
    async fn test_vault_health_transition_idempotent() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Mark healthy twice
        let healthy = OrganizationRequest::UpdateVaultHealth {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            healthy: true,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: None,
            recovery_started_at: None,
        };
        let (r1, _) = store.apply_org(&healthy, &mut state);
        let (r2, _) = store.apply_org(&healthy, &mut state);
        assert!(matches!(r1, LedgerResponse::VaultHealthUpdated { success: true }));
        assert!(matches!(r2, LedgerResponse::VaultHealthUpdated { success: true }));
        assert_eq!(
            state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(&VaultHealthStatus::Healthy)
        );
    }

    // ─── Organization Resource Accounting Tests ───────────────────────────────────

    #[test]
    fn test_estimate_delta_set_entity() {
        let tx = inferadb_ledger_types::Transaction {
            id: [0u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "hello".to_string(),   // 5 bytes
                value: b"world!!".to_vec(), // 7 bytes
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        assert_eq!(estimate_write_storage_delta(&[tx]), 12);
    }

    #[test]
    fn test_estimate_delta_delete_entity() {
        let tx = inferadb_ledger_types::Transaction {
            id: [0u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::DeleteEntity {
                key: "hello".to_string(), // -5 bytes
            }],
            timestamp: chrono::Utc::now(),
        };
        assert_eq!(estimate_write_storage_delta(&[tx]), -5);
    }

    #[test]
    fn test_estimate_delta_create_relationship() {
        let tx = inferadb_ledger_types::Transaction {
            id: [0u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::CreateRelationship {
                resource: "doc:1".to_string(),  // 5
                relation: "viewer".to_string(), // 6
                subject: "user:2".to_string(),  // 6
            }],
            timestamp: chrono::Utc::now(),
        };
        assert_eq!(estimate_write_storage_delta(&[tx]), 17);
    }

    #[test]
    fn test_estimate_delta_delete_relationship() {
        let tx = inferadb_ledger_types::Transaction {
            id: [0u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::DeleteRelationship {
                resource: "doc:1".to_string(),
                relation: "viewer".to_string(),
                subject: "user:2".to_string(),
            }],
            timestamp: chrono::Utc::now(),
        };
        assert_eq!(estimate_write_storage_delta(&[tx]), -17);
    }

    #[test]
    fn test_estimate_delta_expire_entity() {
        let tx = inferadb_ledger_types::Transaction {
            id: [0u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::ExpireEntity {
                key: "mykey".to_string(), // -5
                expired_at: 12345,
            }],
            timestamp: chrono::Utc::now(),
        };
        assert_eq!(estimate_write_storage_delta(&[tx]), -5);
    }

    #[test]
    fn test_estimate_delta_mixed_operations() {
        let tx = inferadb_ledger_types::Transaction {
            id: [0u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![
                inferadb_ledger_types::Operation::SetEntity {
                    key: "k1".to_string(),  // +2
                    value: b"val".to_vec(), // +3
                    condition: None,
                    expires_at: None,
                },
                inferadb_ledger_types::Operation::DeleteEntity {
                    key: "k2".to_string(),        // -2
                },
            ],
            timestamp: chrono::Utc::now(),
        };
        // +5 - 2 = 3
        assert_eq!(estimate_write_storage_delta(&[tx]), 3);
    }

    #[test]
    fn test_estimate_delta_multiple_transactions() {
        let tx1 = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "aa".to_string(),
                value: b"bb".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        let tx2 = inferadb_ledger_types::Transaction {
            id: [2u8; 16],
            client_id: ClientId::new("c"),
            sequence: 2,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "cc".to_string(),
                value: b"dd".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        // tx1: 2+2=4, tx2: 2+2=4 → total 8
        assert_eq!(estimate_write_storage_delta(&[tx1, tx2]), 8);
    }

    #[test]
    fn test_estimate_delta_empty() {
        assert_eq!(estimate_write_storage_delta(&[]), 0);
    }

    #[tokio::test]
    async fn test_organization_storage_bytes_accumulate() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Setup organization + vault
        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );
        store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        assert_eq!(
            state.organization_storage_bytes.get(&OrganizationId::new(1)).copied().unwrap_or(0),
            0,
            "Storage starts at zero"
        );

        // Write 1: key=4 bytes, value=6 bytes → +10
        let tx1 = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: ClientId::new("client1"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "key1".to_string(),
                value: b"value1".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_org(
            &OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![tx1],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
        );
        assert_eq!(
            state.organization_storage_bytes.get(&OrganizationId::new(1)).copied().unwrap_or(0),
            10,
            "First write adds 4+6=10 bytes"
        );

        // Write 2: key=4 bytes, value=10 bytes → +14
        let tx2 = inferadb_ledger_types::Transaction {
            id: [2u8; 16],
            client_id: ClientId::new("client1"),
            sequence: 2,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "key2".to_string(),
                value: b"longerval!".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_org(
            &OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![tx2],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
        );
        assert_eq!(
            state.organization_storage_bytes.get(&OrganizationId::new(1)).copied().unwrap_or(0),
            24,
            "Second write accumulates: 10+14=24 bytes"
        );
    }

    #[tokio::test]
    async fn test_organization_storage_bytes_decrease_on_delete() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Setup
        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );
        store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Write: key="abcde" (5), value="fghij" (5) → +10
        let tx_set = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "abcde".to_string(),
                value: b"fghij".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_org(
            &OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![tx_set],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
        );
        assert_eq!(state.organization_storage_bytes[&OrganizationId::new(1)], 10);

        // Delete: key="abcde" (5) → -5 (conservative: doesn't know value size)
        let tx_del = inferadb_ledger_types::Transaction {
            id: [2u8; 16],
            client_id: ClientId::new("c"),
            sequence: 2,
            operations: vec![inferadb_ledger_types::Operation::DeleteEntity {
                key: "abcde".to_string(),
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_org(
            &OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![tx_del],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
        );
        assert_eq!(
            state.organization_storage_bytes[&OrganizationId::new(1)],
            5,
            "Delete subtracts key bytes only: 10-5=5"
        );
    }

    #[tokio::test]
    async fn test_organization_storage_bytes_floor_at_zero() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );
        store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Delete without prior write — should floor at 0 via saturating_sub
        let tx = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::DeleteEntity {
                key: "missing_key".to_string(),
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_org(
            &OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![tx],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
        );
        assert_eq!(
            state.organization_storage_bytes.get(&OrganizationId::new(1)).copied().unwrap_or(0),
            0,
            "Storage bytes must never go negative — saturating_sub floors at 0"
        );
    }

    #[tokio::test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    async fn test_organization_storage_bytes_independent_organizations() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create two organizations, each with a vault
        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );
        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );
        store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: OrganizationId::new(2),
                slug: VaultSlug::new(1),
                name: Some("v2".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Write to organization 1: "aa" + "bb" = 4 bytes
        let tx1 = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "aa".to_string(),
                value: b"bb".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_org(
            &OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![tx1],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
        );

        // Write to organization 2: "cccccc" + "dddddddd" = 14 bytes
        let tx2 = inferadb_ledger_types::Transaction {
            id: [2u8; 16],
            client_id: ClientId::new("c"),
            sequence: 2,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "cccccc".to_string(),
                value: b"dddddddd".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_org(
            &OrganizationRequest::Write {
                vault: VaultId::new(2),
                transactions: vec![tx2],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
        );

        assert_eq!(state.organization_storage_bytes[&OrganizationId::new(1)], 4);
        assert_eq!(state.organization_storage_bytes[&OrganizationId::new(2)], 14);
    }

    #[tokio::test]
    async fn test_organization_storage_accessor() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        // Before any writes, accessor returns 0
        assert_eq!(store.accessor().organization_storage_bytes(OrganizationId::new(1)), 0);

        // Write some data
        let mut state = (*store.applied_state.load_full()).clone();
        state.organization_storage_bytes.insert(OrganizationId::new(1), 42);
        store.applied_state.store(Arc::new(state));

        assert_eq!(store.accessor().organization_storage_bytes(OrganizationId::new(1)), 42);
        assert_eq!(
            store.accessor().organization_storage_bytes(OrganizationId::new(999)),
            0,
            "Unknown organization returns 0"
        );
    }

    #[tokio::test]
    async fn test_organization_usage_combines_storage_and_vaults() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Create organization with two vaults
        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );
        store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(2),
                name: Some("v2".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Write some data
        let tx = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: ClientId::new("c"),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "key".to_string(),
                value: b"value".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_org(
            &OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![tx],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
        );
        store.applied_state.store(Arc::new(state));

        let usage = store.accessor().organization_usage(OrganizationId::new(1));
        assert_eq!(usage.storage_bytes, 8, "key(3) + value(5) = 8 bytes");
        assert_eq!(usage.vault_count, 2, "Two active vaults");

        // Unknown organization returns zeroes
        let empty = store.accessor().organization_usage(OrganizationId::new(999));
        assert_eq!(empty.storage_bytes, 0);
        assert_eq!(empty.vault_count, 0);
    }

    #[tokio::test]
    async fn test_organization_usage_excludes_deleted_vaults() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(0),
            Region::US_EAST_VA,
        );
        store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(2),
                name: Some("v2".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        store.applied_state.store(Arc::new(state));
        assert_eq!(store.accessor().organization_usage(OrganizationId::new(1)).vault_count, 2);

        // Delete one vault
        let mut state = (*store.applied_state.load_full()).clone();
        store.apply_org(
            &OrganizationRequest::DeleteVault {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
            },
            &mut state,
        );
        store.applied_state.store(Arc::new(state));

        assert_eq!(
            store.accessor().organization_usage(OrganizationId::new(1)).vault_count,
            1,
            "Deleted vaults excluded from count"
        );
    }

    #[tokio::test]
    async fn test_organization_storage_bytes_concurrent_reads() {
        use std::sync::Arc;

        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = Arc::new(
            RaftLogStore::<FileBackend>::open(&path)
                .expect("open store")
                .with_organization_id(OrganizationId::new(1)),
        );

        // Set up state with known storage bytes
        {
            let mut state = (*store.applied_state.load_full()).clone();
            create_active_organization(
                &store,
                &mut state,
                inferadb_ledger_types::OrganizationSlug::new(0),
                Region::US_EAST_VA,
            );
            store.apply_org(
                &OrganizationRequest::CreateVault {
                    organization: OrganizationId::new(1),
                    slug: VaultSlug::new(1),
                    name: Some("v1".to_string()),
                    retention_policy: None,
                },
                &mut state,
            );
            // Write data: "hello" (5) + "world!" (6) = 11 bytes
            let tx = inferadb_ledger_types::Transaction {
                id: [1u8; 16],
                client_id: ClientId::new("c"),
                sequence: 1,
                operations: vec![inferadb_ledger_types::Operation::SetEntity {
                    key: "hello".to_string(),
                    value: b"world!".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                timestamp: chrono::Utc::now(),
            };
            store.apply_org(
                &OrganizationRequest::Write {
                    vault: VaultId::new(1),
                    transactions: vec![tx],
                    idempotency_key: [0u8; 16],
                    request_hash: 0,
                    organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    vault_slug: inferadb_ledger_types::VaultSlug::new(0),
                },
                &mut state,
            );
            store.applied_state.store(Arc::new(state));
        }

        // Spawn 50 concurrent readers — all must see consistent state
        let mut handles = Vec::new();
        for _ in 0..50 {
            let store_clone = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                let usage = store_clone.accessor().organization_usage(OrganizationId::new(1));
                assert_eq!(usage.storage_bytes, 11);
                assert_eq!(usage.vault_count, 1);
                usage
            }));
        }

        for handle in handles {
            let usage = handle.await.expect("task panicked");
            assert_eq!(usage.storage_bytes, 11);
            assert_eq!(usage.vault_count, 1);
        }
    }

    // =========================================================================
    // Apply-phase event integration tests
    // =========================================================================

    /// Helper: creates a RaftLogStore with an EventWriter attached.
    fn store_with_events(dir: &std::path::Path) -> RaftLogStore<FileBackend> {
        let store = RaftLogStore::<FileBackend>::open(dir.join("raft_log.db")).expect("open store");
        let events_db = EventsDatabase::open(dir).expect("open events db");
        let config = EventConfig::default();
        let writer = crate::event_writer::EventWriter::new(Arc::new(events_db), config);
        store.with_event_writer(writer)
    }

    /// Helper: fixed timestamp for deterministic event tests.
    fn fixed_timestamp() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2025-01-15T12:00:00Z").expect("parse timestamp").to_utc()
    }

    /// Helper: creates a simple Write request with one set operation.
    fn simple_write_request(_organization: OrganizationId, vault: VaultId) -> OrganizationRequest {
        OrganizationRequest::Write {
            vault,
            transactions: vec![Transaction {
                id: [1u8; 16],
                client_id: ClientId::new("test-client"),
                sequence: 0,
                operations: vec![Operation::SetEntity {
                    key: "key1".to_string(),
                    value: b"value1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                timestamp: fixed_timestamp(),
            }],
            idempotency_key: [0u8; 16],
            request_hash: 0,

            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        }
    }

    /// Helper: sets up an organization and vault in applied state.
    fn setup_org_and_vault(state: &mut AppliedState) -> (OrganizationId, VaultId) {
        let org_id = state.sequences.next_organization();
        let vault_id = state.sequences.next_vault();
        let org_slug = inferadb_ledger_types::OrganizationSlug::new(1000);
        let vault_slug = VaultSlug::new(2000);

        state.organizations.insert(
            org_id,
            OrganizationMeta {
                organization: org_id,
                slug: org_slug,
                region: Region::GLOBAL,
                status: OrganizationStatus::Active,
                tier: OrganizationTier::Free,
                pending_region: None,
                storage_bytes: 0,
            },
        );
        state.slug_index.insert(org_slug, org_id);
        state.id_to_slug.insert(org_id, org_slug);

        let key = (org_id, vault_id);
        state.vault_heights.insert(key, 0);
        state.vaults.insert(
            key,
            VaultMeta {
                organization: org_id,
                vault: vault_id,
                slug: vault_slug,
                name: Some("test-vault".to_string()),
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: BlockRetentionPolicy::default(),
            },
        );
        state.vault_slug_index.insert(vault_slug, (org_id, vault_id));
        state.vault_id_to_slug.insert((org_id, vault_id), vault_slug);
        state.vault_health.insert(key, VaultHealthStatus::Healthy);

        (org_id, vault_id)
    }

    #[test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    fn event_write_committed_has_correct_block_height() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        let request = simple_write_request(org_id, vault_id);
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        let (response, _) = store.apply_organization_request_with_events(
            &request,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        // Verify response is a successful write
        match &response {
            LedgerResponse::Write { block_height, .. } => {
                assert_eq!(*block_height, 1, "first write should be block 1");
            },
            other => panic!("expected Write response, got {:?}", other),
        }

        // Verify WriteCommitted event
        let write_event = events
            .iter()
            .find(|e| e.action == EventAction::WriteCommitted)
            .expect("WriteCommitted event should be emitted");

        assert_eq!(
            write_event.block_height,
            Some(1),
            "event block_height should match region chain height + 1"
        );
        assert_eq!(write_event.organization_id, org_id);
        assert!(
            write_event.operations_count.is_some(),
            "WriteCommitted should include operations_count"
        );
    }

    #[test]
    fn event_determinism_across_stores() {
        // Create two independent stores with events
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");
        let store_a = store_with_events(dir_a.path());
        let store_b = store_with_events(dir_b.path());

        // Set up identical state in both
        let mut state_a = (*store_a.applied_state.load_full()).clone();
        let (org_id_a, vault_id_a) = setup_org_and_vault(&mut state_a);

        let mut state_b = (*store_b.applied_state.load_full()).clone();
        let (org_id_b, vault_id_b) = setup_org_and_vault(&mut state_b);

        // Both should have assigned the same IDs (sequential from zero)
        assert_eq!(org_id_a, org_id_b);
        assert_eq!(vault_id_a, vault_id_b);

        let ts = fixed_timestamp();

        // Apply identical write request to both stores
        let write_request = simple_write_request(org_id_a, vault_id_a);

        let mut events_a: Vec<EventEntry> = Vec::new();
        let mut events_b: Vec<EventEntry> = Vec::new();
        let mut idx_a = 0u32;
        let mut idx_b = 0u32;

        store_a.apply_organization_request_with_events(
            &write_request,
            &mut state_a,
            ts,
            &mut idx_a,
            &mut events_a,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );
        store_b.apply_organization_request_with_events(
            &write_request,
            &mut state_b,
            ts,
            &mut idx_b,
            &mut events_b,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        // Both must produce the same number of events
        assert_eq!(events_a.len(), events_b.len(), "event count must match across replicas");

        // Each event must have identical IDs and content
        for (a, b) in events_a.iter().zip(events_b.iter()) {
            assert_eq!(a.event_id, b.event_id, "event IDs must be deterministic");
            assert_eq!(a.action, b.action);
            assert_eq!(a.block_height, b.block_height);
            assert_eq!(a.timestamp, b.timestamp);
            assert_eq!(a.details, b.details);
        }
    }

    #[test]
    fn event_entity_expired_emits_per_key() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();
        let (_org_id, vault_id) = setup_org_and_vault(&mut state);

        // Write request with two ExpireEntity operations
        let request = OrganizationRequest::Write {
            vault: vault_id,
            transactions: vec![Transaction {
                id: [2u8; 16],
                client_id: ClientId::new("gc-client"),
                sequence: 0,
                operations: vec![
                    Operation::ExpireEntity { key: "expired-key-1".to_string(), expired_at: 100 },
                    Operation::ExpireEntity { key: "expired-key-2".to_string(), expired_at: 200 },
                ],
                timestamp: fixed_timestamp(),
            }],
            idempotency_key: [0u8; 16],
            request_hash: 0,

            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_organization_request_with_events(
            &request,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        // Should have: 1 WriteCommitted + 2 EntityExpired = 3 events
        let expired_events: Vec<&EventEntry> =
            events.iter().filter(|e| e.action == EventAction::EntityExpired).collect();

        assert_eq!(expired_events.len(), 2, "should emit one EntityExpired per ExpireEntity op");

        // Verify each has the correct key in details
        let keys: Vec<&str> = expired_events
            .iter()
            .map(|e| e.details.get("key").expect("key detail").as_str())
            .collect();
        assert!(keys.contains(&"expired-key-1"));
        assert!(keys.contains(&"expired-key-2"));

        // Verify each has a distinct event_id (different op_index)
        assert_ne!(
            expired_events[0].event_id, expired_events[1].event_id,
            "each EntityExpired must have a unique event_id"
        );
    }

    #[test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    fn event_writer_integration_persists_to_events_db() {
        let dir = tempdir().expect("create temp dir");
        let events_db = EventsDatabase::open(dir.path()).expect("open events db");
        let events_db_arc = Arc::new(events_db);
        let config = EventConfig::default();
        let writer = crate::event_writer::EventWriter::new(Arc::clone(&events_db_arc), config);
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
            .expect("open store")
            .with_event_writer(writer);

        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        let request = simple_write_request(org_id, vault_id);
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_organization_request_with_events(
            &request,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        // Write events through the EventWriter (simulating what apply_to_state_machine does)
        let ew = store.event_writer().expect("event writer configured");
        let written = ew.write_events(&events).expect("write events");
        assert!(written > 0, "at least one event should be written");

        // Read back from events_db and verify
        let read_txn = events_db_arc.read().expect("read txn");
        let (stored, _cursor) =
            EventStore::list(&read_txn, org_id, 0, u64::MAX, 100, None).expect("list events");

        // System-scoped events (OrganizationCreated etc.) won't appear under org_id query,
        // but WriteCommitted is organization-scoped so it should be there
        let write_committed: Vec<_> =
            stored.iter().filter(|e| e.action == EventAction::WriteCommitted).collect();
        assert_eq!(
            write_committed.len(),
            1,
            "WriteCommitted event should be persisted in events.db"
        );
        assert_eq!(write_committed[0].organization_id, org_id);
    }

    // ── IngestExternalEvents apply handler tests ──

    /// Build a HandlerPhase `EventEntry` shaped like the ones the RPC handler
    /// constructs (UUID v4 event_id, fixed timestamp, no block_height).
    fn make_external_event_entry(
        org_id: OrganizationId,
        source: &str,
        event_type: &str,
        principal: &str,
        event_id: [u8; 16],
    ) -> EventEntry {
        EventEntry {
            expires_at: 0,
            event_id,
            source_service: source.to_string(),
            event_type: event_type.to_string(),
            timestamp: fixed_timestamp(),
            scope: inferadb_ledger_types::events::EventScope::Organization,
            action: EventAction::WriteCommitted,
            emission: inferadb_ledger_types::events::EventEmission::HandlerPhase { node_id: 1 },
            principal: principal.to_string(),
            organization_id: org_id,
            organization: None,
            vault: None,
            outcome: inferadb_ledger_types::events::EventOutcome::Success,
            details: std::collections::BTreeMap::new(),
            block_height: None,
            trace_id: None,
            correlation_id: None,
            operations_count: None,
        }
    }

    #[test]
    fn apply_ingest_external_events_writes_events_to_db() {
        let dir = tempdir().expect("create temp dir");
        let events_db = EventsDatabase::open(dir.path()).expect("open events db");
        let events_db_arc = Arc::new(events_db);
        let config = EventConfig::default();
        let writer = crate::event_writer::EventWriter::new(Arc::clone(&events_db_arc), config);
        // Residency debug_assert in the apply handler requires a REGIONAL
        // region — external events must never apply on GLOBAL.
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
            .expect("open store")
            .with_region_config(
                Region::US_EAST_VA,
                inferadb_ledger_types::NodeId::new("node-test"),
                1,
            )
            .with_event_writer(writer);

        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, _vault_id) = setup_org_and_vault(&mut state);

        let batch = vec![
            make_external_event_entry(org_id, "engine", "engine.task_a", "alice", [1u8; 16]),
            make_external_event_entry(org_id, "engine", "engine.task_b", "bob", [2u8; 16]),
        ];
        let request = OrganizationRequest::IngestExternalEvents {
            source: "engine".to_string(),
            events: batch.clone(),
        };

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();
        let (response, vault_entry) = store.apply_organization_request_with_events(
            &request,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        assert_eq!(
            response,
            LedgerResponse::Empty,
            "IngestExternalEvents must return Empty on success"
        );
        assert!(vault_entry.is_none(), "external events produce no vault entry");
        assert!(events.is_empty(), "external events don't emit apply-phase events");

        // Read back from events_db and verify both entries are present.
        let read_txn = events_db_arc.read().expect("read txn");
        let (stored, _cursor) =
            EventStore::list(&read_txn, org_id, 0, u64::MAX, 100, None).expect("list events");
        assert_eq!(stored.len(), 2, "both external events must be persisted");
        // Verify bytes match the proposed entries byte-for-byte (event_id is
        // the discriminator; timestamps + source + principal round-trip).
        for entry in &batch {
            assert!(
                stored.iter().any(|e| e.event_id == entry.event_id
                    && e.source_service == entry.source_service
                    && e.principal == entry.principal),
                "proposed event_id {:?} not found in events.db",
                entry.event_id
            );
        }

        // commit_in_memory was used: the on-disk dual-slot has NOT advanced.
        // last_synced_snapshot_id stays at 0 until StateCheckpointer fires.
        // We can't call a public accessor here without wider surface changes,
        // but the write path has a compile-time check that the txn is
        // commit_in_memory'd (see apply handler). A dedicated
        // checkpointer/sync integration test covers the durability boundary.
    }

    #[test]
    fn apply_ingest_external_events_replay_idempotency() {
        let dir = tempdir().expect("create temp dir");
        let events_db = EventsDatabase::open(dir.path()).expect("open events db");
        let events_db_arc = Arc::new(events_db);
        let config = EventConfig::default();
        let writer = crate::event_writer::EventWriter::new(Arc::clone(&events_db_arc), config);
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
            .expect("open store")
            .with_region_config(
                Region::US_EAST_VA,
                inferadb_ledger_types::NodeId::new("node-test"),
                1,
            )
            .with_event_writer(writer);

        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, _vault_id) = setup_org_and_vault(&mut state);

        let batch = vec![
            make_external_event_entry(org_id, "engine", "engine.x", "alice", [10u8; 16]),
            make_external_event_entry(org_id, "engine", "engine.y", "bob", [20u8; 16]),
        ];
        let request = OrganizationRequest::IngestExternalEvents {
            source: "engine".to_string(),
            events: batch.clone(),
        };

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        // Apply twice — simulating WAL replay re-driving the same proposal.
        for _ in 0..2 {
            let (response, _) = store.apply_organization_request_with_events(
                &request,
                &mut state,
                ts,
                &mut op_index,
                &mut events,
                90,
                &mut PendingExternalWrites::default(),
                None,
                false,
                0,
                false,
            );
            assert_eq!(response, LedgerResponse::Empty);
        }

        // B-tree upsert idempotency: identical bytes re-applied produce the
        // same row count (not duplicated).
        let read_txn = events_db_arc.read().expect("read txn");
        let (stored, _cursor) =
            EventStore::list(&read_txn, org_id, 0, u64::MAX, 100, None).expect("list events");
        assert_eq!(
            stored.len(),
            2,
            "replay of IngestExternalEvents must upsert, not duplicate (got {})",
            stored.len()
        );
    }

    // ── System-Level Event Hook Tests ─────────────────────────────

    #[test]
    fn event_organization_created_has_slug_detail() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let request = SystemRequest::CreateOrganization {
            slug: inferadb_ledger_types::OrganizationSlug::new(42_000),
            region: Region::US_EAST_VA,
            tier: Default::default(),
            admin: UserId::new(1),
        };

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_system_request_with_events(
            &request,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        let org_event = events
            .iter()
            .find(|e| e.action == EventAction::OrganizationCreated)
            .expect("OrganizationCreated event should be emitted");

        assert_eq!(
            org_event.details.get("organization_slug").map(|s| s.as_str()),
            Some("42000"),
            "OrganizationCreated should include slug in details"
        );
        assert_eq!(org_event.organization_id, OrganizationId::new(0), "system events use org_id 0");
    }

    #[test]
    fn event_organization_deleted_captures_vault_count() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        // Mark the vault as deleted so DeleteOrganization goes straight to Deleted
        let key = (org_id, vault_id);
        {
            let mut vault = state.vaults.get(&key).expect("vault exists").clone();
            vault.deleted = true;
            state.vaults.insert(key, vault);
        }

        let request = SystemRequest::DeleteOrganization { organization: org_id };

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_system_request_with_events(
            &request,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        let del_event = events
            .iter()
            .find(|e| e.action == EventAction::OrganizationDeleted)
            .expect("OrganizationDeleted event should be emitted");

        // vault_count captures active (non-deleted) vaults at deletion time
        assert_eq!(
            del_event.details.get("vault_count").map(|s| s.as_str()),
            Some("0"),
            "OrganizationDeleted should count active vaults (deleted ones excluded)"
        );
        assert_eq!(
            del_event.details.get("organization_slug").map(|s| s.as_str()),
            Some("1000"),
            "OrganizationDeleted should capture slug before deletion"
        );
    }

    #[test]
    fn event_user_created_has_details() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let request = SystemRequest::CreateUser {
            user: UserId::new(1),
            admin: true,
            slug: UserSlug::new(333),
            region: Region::US_EAST_VA,
        };

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_system_request_with_events(
            &request,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        let user_event = events
            .iter()
            .find(|e| e.action == EventAction::UserCreated)
            .expect("UserCreated event should be emitted");

        assert!(
            user_event.details.contains_key("user_id"),
            "UserCreated should have user_id detail"
        );
        assert_eq!(user_event.details.get("admin").map(|s| s.as_str()), Some("true"));
        assert_eq!(user_event.organization_id, OrganizationId::new(0));
    }

    #[test]
    fn event_suspend_resume_org_has_details() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, _vault_id) = setup_org_and_vault(&mut state);

        // Suspend with reason
        let suspend = SystemRequest::SuspendOrganization {
            organization: org_id,
            reason: Some("billing overdue".to_string()),
        };
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_system_request_with_events(
            &suspend,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        let suspend_event = events
            .iter()
            .find(|e| e.action == EventAction::OrganizationSuspended)
            .expect("OrganizationSuspended event should be emitted");

        assert_eq!(
            suspend_event.details.get("organization_slug").map(|s| s.as_str()),
            Some("1000")
        );
        assert_eq!(
            suspend_event.details.get("reason").map(|s| s.as_str()),
            Some("billing overdue")
        );

        // Resume
        let resume = SystemRequest::ResumeOrganization { organization: org_id };
        let mut resume_events: Vec<EventEntry> = Vec::new();
        let mut resume_op_index = 0u32;

        store.apply_system_request_with_events(
            &resume,
            &mut state,
            ts,
            &mut resume_op_index,
            &mut resume_events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        let resume_event = resume_events
            .iter()
            .find(|e| e.action == EventAction::OrganizationResumed)
            .expect("OrganizationResumed event should be emitted");

        assert_eq!(resume_event.details.get("organization_slug").map(|s| s.as_str()), Some("1000"));
    }

    #[test]
    fn event_system_scope_not_in_org_query() {
        let dir = tempdir().expect("create temp dir");
        let events_db = EventsDatabase::open(dir.path()).expect("open events db");
        let events_db_arc = Arc::new(events_db);
        let config = EventConfig::default();
        let writer = crate::event_writer::EventWriter::new(Arc::clone(&events_db_arc), config);
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
            .expect("open store")
            .with_event_writer(writer);

        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        // Emit a system event (OrganizationCreated via CreateOrganization)
        let create_org = SystemRequest::CreateOrganization {
            slug: inferadb_ledger_types::OrganizationSlug::new(9999),
            region: Region::US_EAST_VA,
            tier: Default::default(),
            admin: UserId::new(1),
        };
        let mut all_events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_system_request_with_events(
            &create_org,
            &mut state,
            ts,
            &mut op_index,
            &mut all_events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        // Emit an org-scoped event (WriteCommitted)
        let write_req = simple_write_request(org_id, vault_id);
        store.apply_organization_request_with_events(
            &write_req,
            &mut state,
            ts,
            &mut op_index,
            &mut all_events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        // Persist all events
        let ew = store.event_writer().expect("event writer configured");
        let written = ew.write_events(&all_events).expect("write events");
        assert!(written >= 2, "at least system + org events written");

        // Query under org_id — should NOT find the system event
        let read_txn = events_db_arc.read().expect("read txn");
        let (org_events, _) =
            EventStore::list(&read_txn, org_id, 0, u64::MAX, 100, None).expect("list org events");

        let system_in_org: Vec<_> =
            org_events.iter().filter(|e| e.action == EventAction::OrganizationCreated).collect();
        assert!(
            system_in_org.is_empty(),
            "system events (org_id=0) must not appear in org-scoped queries"
        );

        // Query under org_id=0 — system events should be there
        let (sys_events, _) =
            EventStore::list(&read_txn, OrganizationId::new(0), 0, u64::MAX, 100, None)
                .expect("list system events");

        let org_created: Vec<_> =
            sys_events.iter().filter(|e| e.action == EventAction::OrganizationCreated).collect();
        assert!(!org_created.is_empty(), "system events should be found under org_id=0");
    }

    #[test]
    fn event_vault_created_has_org_scope() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        // Create an organization first
        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(5000),
            Region::US_EAST_VA,
        );
        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        // Create a vault
        let create_vault = OrganizationRequest::CreateVault {
            organization: org_id,
            slug: VaultSlug::new(6000),
            name: Some("audit-vault".to_string()),
            retention_policy: None,
        };
        let (resp, _) = store.apply_organization_request_with_events(
            &create_vault,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );
        assert!(
            matches!(resp, LedgerResponse::VaultCreated { .. }),
            "expected VaultCreated response"
        );

        let vault_event = events
            .iter()
            .find(|e| e.action == EventAction::VaultCreated)
            .expect("VaultCreated event should be emitted");

        assert_eq!(vault_event.scope, EventScope::Organization);
        assert_eq!(vault_event.organization_id, org_id);
        assert_eq!(vault_event.vault, Some(VaultSlug::new(6000)));
        assert_eq!(vault_event.details.get("vault_name").map(|s| s.as_str()), Some("audit-vault"));
    }

    #[test]
    fn event_vault_deleted_has_org_scope() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        let delete_vault =
            OrganizationRequest::DeleteVault { organization: org_id, vault: vault_id };
        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        let (resp, _) = store.apply_organization_request_with_events(
            &delete_vault,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );
        assert!(
            matches!(resp, LedgerResponse::VaultDeleted { success: true }),
            "expected VaultDeleted response"
        );

        let delete_event = events
            .iter()
            .find(|e| e.action == EventAction::VaultDeleted)
            .expect("VaultDeleted event should be emitted");

        assert_eq!(delete_event.scope, EventScope::Organization);
        assert_eq!(delete_event.organization_id, org_id);
        assert_eq!(
            delete_event.details.get("vault_id").map(|s| s.as_str()),
            Some(&vault_id.to_string() as &str)
        );
    }

    #[test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    fn event_batch_write_committed_emitted() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        // Create a batch of two writes
        let batch = OrganizationRequest::BatchWrite {
            requests: vec![
                simple_write_request(org_id, vault_id),
                OrganizationRequest::Write {
                    vault: vault_id,
                    transactions: vec![Transaction {
                        id: [2u8; 16],
                        client_id: ClientId::new("test-client"),
                        sequence: 0,
                        operations: vec![Operation::SetEntity {
                            key: "key2".to_string(),
                            value: b"value2".to_vec(),
                            condition: None,
                            expires_at: None,
                        }],
                        timestamp: fixed_timestamp(),
                    }],
                    idempotency_key: [0u8; 16],
                    request_hash: 0,

                    organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    vault_slug: inferadb_ledger_types::VaultSlug::new(0),
                },
            ],
        };

        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        let (resp, _) = store.apply_organization_request_with_events(
            &batch,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );
        assert!(matches!(resp, LedgerResponse::BatchWrite { .. }), "expected BatchWrite response");

        // Should have: two WriteCommitted events (one per inner write) + one BatchWriteCommitted
        let write_events: Vec<_> =
            events.iter().filter(|e| e.action == EventAction::WriteCommitted).collect();
        assert_eq!(write_events.len(), 2, "each inner write should emit WriteCommitted");

        let batch_event = events
            .iter()
            .find(|e| e.action == EventAction::BatchWriteCommitted)
            .expect("BatchWriteCommitted event should be emitted");

        assert_eq!(batch_event.scope, EventScope::Organization);
        assert_eq!(batch_event.organization_id, org_id);
        assert_eq!(batch_event.operations_count, Some(2), "batch has 2 requests");
    }

    #[test]
    fn event_vault_health_updated_emitted() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        // Mark vault as diverged
        let health_request = OrganizationRequest::UpdateVaultHealth {
            organization: org_id,
            vault: vault_id,
            healthy: false,
            expected_root: None,
            computed_root: None,
            diverged_at_height: Some(5),
            recovery_attempt: None,
            recovery_started_at: None,
        };

        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        store.apply_organization_request_with_events(
            &health_request,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        let health_event = events
            .iter()
            .find(|e| e.action == EventAction::VaultHealthUpdated)
            .expect("VaultHealthUpdated event should be emitted");

        assert_eq!(health_event.scope, EventScope::Organization);
        assert_eq!(health_event.organization_id, org_id);
        assert_eq!(health_event.details.get("health_status").map(|s| s.as_str()), Some("diverged"));
    }

    #[test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    fn event_org_isolation_across_organizations() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        // Create two organizations with vaults
        let (org_a, vault_a) = setup_org_and_vault(&mut state);

        // Create second org manually
        let org_b = state.sequences.next_organization();
        let vault_b = state.sequences.next_vault();
        let org_b_slug = inferadb_ledger_types::OrganizationSlug::new(3000);
        let vault_b_slug = VaultSlug::new(4000);
        state.organizations.insert(
            org_b,
            OrganizationMeta {
                organization: org_b,
                slug: org_b_slug,
                region: Region::GLOBAL,
                status: OrganizationStatus::Active,
                tier: OrganizationTier::Free,
                pending_region: None,
                storage_bytes: 0,
            },
        );
        state.slug_index.insert(org_b_slug, org_b);
        state.id_to_slug.insert(org_b, org_b_slug);
        let key_b = (org_b, vault_b);
        state.vault_heights.insert(key_b, 0);
        state.vaults.insert(
            key_b,
            VaultMeta {
                organization: org_b,
                vault: vault_b,
                slug: vault_b_slug,
                name: Some("vault-b".to_string()),
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: BlockRetentionPolicy::default(),
            },
        );
        state.vault_slug_index.insert(vault_b_slug, (org_b, vault_b));
        state.vault_id_to_slug.insert((org_b, vault_b), vault_b_slug);
        state.vault_health.insert(key_b, VaultHealthStatus::Healthy);

        let ts = fixed_timestamp();
        let mut all_events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        // Write to org A
        store.apply_organization_request_with_events(
            &simple_write_request(org_a, vault_a),
            &mut state,
            ts,
            &mut op_index,
            &mut all_events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        // Write to org B
        store.apply_organization_request_with_events(
            &OrganizationRequest::Write {
                vault: vault_b,
                transactions: vec![Transaction {
                    id: [9u8; 16],
                    client_id: ClientId::new("client-b"),
                    sequence: 0,
                    operations: vec![Operation::SetEntity {
                        key: "key-b".to_string(),
                        value: b"value-b".to_vec(),
                        condition: None,
                        expires_at: None,
                    }],
                    timestamp: ts,
                }],
                idempotency_key: [0u8; 16],
                request_hash: 0,

                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
            ts,
            &mut op_index,
            &mut all_events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        // Verify org isolation: filter events by org_id
        let org_a_events: Vec<_> =
            all_events.iter().filter(|e| e.organization_id == org_a).collect();
        let org_b_events: Vec<_> =
            all_events.iter().filter(|e| e.organization_id == org_b).collect();

        assert!(!org_a_events.is_empty(), "org A should have events");
        assert!(!org_b_events.is_empty(), "org B should have events");

        // No cross-contamination
        for event in &org_a_events {
            assert_eq!(event.organization_id, org_a, "org A event should belong to org A");
        }
        for event in &org_b_events {
            assert_eq!(event.organization_id, org_b, "org B event should belong to org B");
        }
    }

    #[test]
    fn event_org_log_disabled_suppresses_org_events() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let events_db = EventsDatabase::open(dir.path()).expect("open events db");
        let config = EventConfig {
            enabled: true,
            organization_log_enabled: false, // Suppress org events
            ..EventConfig::default()
        };
        let events_db_arc = Arc::new(events_db);
        let writer = crate::event_writer::EventWriter::new(Arc::clone(&events_db_arc), config);
        let store = store.with_event_writer(writer);

        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        // Write to vault — should produce WriteCommitted event in the accumulator
        store.apply_organization_request_with_events(
            &simple_write_request(org_id, vault_id),
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        // Events are accumulated but the writer will filter org events on write.
        // The apply path emits events into the vector regardless of config;
        // filtering happens in EventWriter::write_events().
        assert!(
            events.iter().any(|e| e.action == EventAction::WriteCommitted),
            "apply path accumulates events regardless of config"
        );

        // Write via EventWriter — org events should be filtered out
        let event_writer = store.event_writer().expect("event writer configured");
        let written = event_writer.write_events(&events).expect("write events");
        assert_eq!(
            written, 0,
            "org events should be filtered by write_events when organization_log_enabled=false"
        );

        // Verify nothing persisted
        let rtxn = events_db_arc.read().expect("begin read");
        let count = EventStore::count(&rtxn, org_id).expect("count");
        assert_eq!(count, 0, "no org events persisted when org log disabled");
    }

    #[test]
    fn event_write_committed_includes_block_height_reference() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        // First write — block_height should reference the region chain height + 1
        store.apply_organization_request_with_events(
            &simple_write_request(org_id, vault_id),
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        let write_event = events
            .iter()
            .find(|e| e.action == EventAction::WriteCommitted)
            .expect("WriteCommitted event emitted");

        assert_eq!(
            write_event.block_height,
            Some(1),
            "WriteCommitted event should reference block_height for blockchain drill-down"
        );

        // Second write to same vault
        events.clear();
        op_index = 0;
        store.apply_organization_request_with_events(
            &OrganizationRequest::Write {
                vault: vault_id,
                transactions: vec![Transaction {
                    id: [3u8; 16],
                    client_id: ClientId::new("test-client"),
                    sequence: 0,
                    operations: vec![Operation::SetEntity {
                        key: "key2".to_string(),
                        value: b"value2".to_vec(),
                        condition: None,
                        expires_at: None,
                    }],
                    timestamp: ts,
                }],
                idempotency_key: [0u8; 16],
                request_hash: 0,

                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
            &mut PendingExternalWrites::default(),
            None,
            false,
            0,
            false,
        );

        let second_write = events
            .iter()
            .find(|e| e.action == EventAction::WriteCommitted)
            .expect("second WriteCommitted event emitted");

        // block_height references the region chain height, which is still 1
        // (region chain only increments on actual block archival)
        assert!(
            second_write.block_height.is_some(),
            "second WriteCommitted should also have block_height reference"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn event_snapshot_includes_apply_phase_entries() {
        use std::collections::BTreeMap;

        use chrono::{TimeZone, Utc};
        use inferadb_ledger_types::events::{EventEmission, EventOutcome};

        use crate::log_storage::types::SnapshotMeta;

        let source_dir = tempdir().expect("create source dir");
        let target_dir = tempdir().expect("create target dir");

        // Create source store with events and minimal Raft state
        let mut source_store = store_with_events(source_dir.path());
        {
            let mut state = (*source_store.applied_state.load_full()).clone();
            state.last_applied = Some(make_log_id(1, 10));
            source_store.applied_state.store(Arc::new(state));
        }
        // Insert OrganizationMeta so event collection can find org IDs
        {
            let org_meta = super::types::OrganizationMeta {
                organization: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(100),
                region: Region::GLOBAL,
                status: OrganizationStatus::Active,
                tier: OrganizationTier::Free,
                pending_region: None,
                storage_bytes: 0,
            };
            let encoded = postcard::to_allocvec(&org_meta).expect("encode org meta");
            let mut write_txn = source_store.db.write().expect("write txn");
            write_txn.insert::<tables::OrganizationMeta>(&1i64, &encoded).expect("insert org meta");
            write_txn.commit().expect("commit org meta");
        }
        // Persist core to database — snapshot builder reads from DB, not in-memory state
        {
            let state = source_store.applied_state.load();
            source_store
                .save_state_core(&state, &PendingExternalWrites::default())
                .expect("persist core");
        }

        // Write apply-phase events directly to the events database
        let events_db_arc =
            source_store.event_writer().expect("should have event_writer").events_db().clone();
        {
            let mut txn = events_db_arc.write().expect("write txn");
            let apply_entry = EventEntry {
                expires_at: 0,
                event_id: [1u8; 16],
                source_service: "ledger".to_string(),
                event_type: "ledger.vault.created".to_string(),
                timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                scope: EventScope::Organization,
                action: EventAction::VaultCreated,
                emission: EventEmission::ApplyPhase,
                principal: "admin".to_string(),
                organization_id: OrganizationId::new(1),
                organization: None,
                vault: None,
                outcome: EventOutcome::Success,
                details: BTreeMap::new(),
                block_height: Some(1),
                trace_id: None,
                correlation_id: None,
                operations_count: None,
            };
            EventStore::write(&mut txn, &apply_entry).expect("write apply event");
            txn.commit().expect("commit");
        }

        // Get snapshot from source
        let snapshot = source_store
            .get_current_snapshot()
            .await
            .expect("get snapshot")
            .expect("snapshot should exist");

        // Copy snapshot file for install on target
        let snapshot_path = target_dir.path().join("snapshot_copy.bin");
        {
            let mut src_file = snapshot.0;
            let mut dst_file =
                tokio::fs::File::create(&snapshot_path).await.expect("create snapshot copy");
            tokio::io::copy(&mut src_file, &mut dst_file).await.expect("copy snapshot data");
        }

        // Install snapshot on target
        let mut target_store = store_with_events(target_dir.path());
        let meta = SnapshotMeta {
            last_log_id: snapshot.1.last_log_id,
            last_membership: snapshot.1.last_membership,
            snapshot_id: "test-snapshot".to_string(),
        };
        let install_file = tokio::fs::File::open(&snapshot_path).await.expect("open snapshot copy");
        target_store
            .install_snapshot(&meta, Box::new(install_file))
            .await
            .expect("install snapshot");

        // Verify apply-phase events are present on target
        let target_events_db = target_store
            .event_writer()
            .expect("target should have event_writer")
            .events_db()
            .clone();
        let txn = target_events_db.read().expect("read txn");
        let events = EventStore::scan_apply_phase(&txn, 1000).expect("scan events");
        assert!(!events.is_empty(), "apply-phase events should be transferred via snapshot");
        assert_eq!(events[0].event_id, [1u8; 16]);
        assert!(matches!(events[0].emission, EventEmission::ApplyPhase));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn event_snapshot_excludes_handler_phase_entries() {
        use std::collections::BTreeMap;

        use chrono::{TimeZone, Utc};
        use inferadb_ledger_types::events::{EventEmission, EventOutcome};

        use crate::log_storage::types::SnapshotMeta;

        let source_dir = tempdir().expect("create source dir");
        let target_dir = tempdir().expect("create target dir");

        let mut source_store = store_with_events(source_dir.path());
        {
            let mut state = (*source_store.applied_state.load_full()).clone();
            state.last_applied = Some(make_log_id(1, 10));
            source_store.applied_state.store(Arc::new(state));
        }
        // Insert OrganizationMeta so event collection can find org IDs
        {
            let org_meta = super::types::OrganizationMeta {
                organization: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(100),
                region: Region::GLOBAL,
                status: OrganizationStatus::Active,
                tier: OrganizationTier::Free,
                pending_region: None,
                storage_bytes: 0,
            };
            let encoded = postcard::to_allocvec(&org_meta).expect("encode org meta");
            let mut write_txn = source_store.db.write().expect("write txn");
            write_txn.insert::<tables::OrganizationMeta>(&1i64, &encoded).expect("insert org meta");
            write_txn.commit().expect("commit org meta");
        }
        // Persist core to database — snapshot builder reads from DB, not in-memory state
        {
            let state = source_store.applied_state.load();
            source_store
                .save_state_core(&state, &PendingExternalWrites::default())
                .expect("persist core");
        }

        // Write both apply-phase and handler-phase events
        let events_db_arc =
            source_store.event_writer().expect("should have event_writer").events_db().clone();
        {
            let mut txn = events_db_arc.write().expect("write txn");

            // Apply-phase event (should be included)
            let apply_entry = EventEntry {
                expires_at: 0,
                event_id: [1u8; 16],
                source_service: "ledger".to_string(),
                event_type: "ledger.vault.created".to_string(),
                timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                scope: EventScope::Organization,
                action: EventAction::VaultCreated,
                emission: EventEmission::ApplyPhase,
                principal: "admin".to_string(),
                organization_id: OrganizationId::new(1),
                organization: None,
                vault: None,
                outcome: EventOutcome::Success,
                details: BTreeMap::new(),
                block_height: Some(1),
                trace_id: None,
                correlation_id: None,
                operations_count: None,
            };
            // Handler-phase event (should NOT be included in snapshot)
            let handler_entry = EventEntry {
                expires_at: 0,
                event_id: [2u8; 16],
                source_service: "ledger".to_string(),
                event_type: "ledger.request.rate_limited".to_string(),
                timestamp: Utc.timestamp_opt(1_700_000_001, 0).unwrap(),
                scope: EventScope::Organization,
                action: EventAction::RequestRateLimited,
                emission: EventEmission::HandlerPhase { node_id: 42 },
                principal: "test-user".to_string(),
                organization_id: OrganizationId::new(1),
                organization: None,
                vault: None,
                outcome: EventOutcome::Denied { reason: "rate limit exceeded".to_string() },
                details: BTreeMap::new(),
                block_height: None,
                trace_id: None,
                correlation_id: None,
                operations_count: None,
            };
            EventStore::write(&mut txn, &apply_entry).expect("write apply event");
            EventStore::write(&mut txn, &handler_entry).expect("write handler event");
            txn.commit().expect("commit");
        }

        // Get snapshot — should only contain apply-phase events
        let snapshot = source_store
            .get_current_snapshot()
            .await
            .expect("get snapshot")
            .expect("snapshot should exist");

        // Copy snapshot file for install on target
        let snapshot_path = target_dir.path().join("snapshot_copy.bin");
        {
            let mut src_file = snapshot.0;
            let mut dst_file =
                tokio::fs::File::create(&snapshot_path).await.expect("create snapshot copy");
            tokio::io::copy(&mut src_file, &mut dst_file).await.expect("copy snapshot data");
        }

        // Install snapshot on target
        let mut target_store = store_with_events(target_dir.path());
        let meta = SnapshotMeta {
            last_log_id: snapshot.1.last_log_id,
            last_membership: snapshot.1.last_membership,
            snapshot_id: "test-snapshot".to_string(),
        };
        let install_file = tokio::fs::File::open(&snapshot_path).await.expect("open snapshot copy");
        target_store
            .install_snapshot(&meta, Box::new(install_file))
            .await
            .expect("install snapshot");

        // Verify only apply-phase events transferred
        let target_events_db = target_store
            .event_writer()
            .expect("target should have event_writer")
            .events_db()
            .clone();
        let txn = target_events_db.read().expect("read txn");
        let events = EventStore::scan_apply_phase(&txn, 1000).expect("scan events");
        assert_eq!(events.len(), 1, "only apply-phase event should be transferred");
        assert_eq!(events[0].event_id, [1u8; 16]);

        // List ALL events on target — handler-phase should not be present
        let (all_events, _cursor) =
            EventStore::list(&txn, OrganizationId::new(1), 0, u64::MAX, 100, None)
                .expect("list all events");
        assert_eq!(
            all_events.len(),
            1,
            "handler-phase event should not be transferred via snapshot"
        );
    }

    // ========================================================================
    // Client Sequence TTL Eviction Tests
    // ========================================================================

    /// Helper: run eviction logic on state + pending, matching the
    /// implementation in `apply_to_state_machine`.
    fn run_eviction(
        state: &mut AppliedState,
        pending: &mut PendingExternalWrites,
        proposed_at_secs: i64,
        ttl_seconds: i64,
    ) {
        let mut expired_keys: Vec<(
            (
                inferadb_ledger_types::OrganizationId,
                inferadb_ledger_types::VaultId,
                inferadb_ledger_types::ClientId,
            ),
            Vec<u8>,
        )> = state
            .client_sequences
            .iter()
            .filter(|(_, entry)| proposed_at_secs.saturating_sub(entry.last_seen) > ttl_seconds)
            .map(|(key, _)| {
                let bytes_key =
                    PendingExternalWrites::client_sequence_key(key.0, key.1, key.2.as_bytes());
                (key.clone(), bytes_key)
            })
            .collect();

        expired_keys.sort_by(|a, b| a.1.cmp(&b.1));

        for (map_key, bytes_key) in expired_keys {
            state.client_sequences.remove(&map_key);
            pending.client_sequences_deleted.push(bytes_key);
        }
    }

    #[test]
    fn test_eviction_removes_expired_entries() {
        let mut state = AppliedState::default();
        let org = OrganizationId::new(1);
        let vault = VaultId::new(1);

        // Insert entry that is 2 days old
        state.client_sequences.insert(
            (org, vault, ClientId::new("old-client")),
            ClientSequenceEntry {
                sequence: 1,
                last_seen: 1_000_000,
                last_idempotency_key: [1u8; 16],
                last_request_hash: 42,
            },
        );

        let mut pending = PendingExternalWrites::default();
        // proposed_at is 2 days later (ttl = 86400 = 1 day)
        run_eviction(&mut state, &mut pending, 1_000_000 + 200_000, 86400);

        assert!(state.client_sequences.is_empty(), "expired entry should be removed");
        assert_eq!(pending.client_sequences_deleted.len(), 1, "one B+ tree delete expected");
    }

    #[test]
    fn test_eviction_retains_entries_within_ttl() {
        let mut state = AppliedState::default();
        let org = OrganizationId::new(1);
        let vault = VaultId::new(1);

        // Insert entry that is 1 hour old (well within 24h TTL)
        state.client_sequences.insert(
            (org, vault, ClientId::new("fresh-client")),
            ClientSequenceEntry {
                sequence: 5,
                last_seen: 1_000_000,
                last_idempotency_key: [2u8; 16],
                last_request_hash: 99,
            },
        );

        let mut pending = PendingExternalWrites::default();
        run_eviction(&mut state, &mut pending, 1_000_000 + 3600, 86400);

        assert_eq!(state.client_sequences.len(), 1, "fresh entry should be retained");
        assert!(pending.client_sequences_deleted.is_empty(), "no B+ tree deletes expected");
    }

    #[test]
    fn test_eviction_zero_expired_entries() {
        let mut state = AppliedState::default();
        let org = OrganizationId::new(1);
        let vault = VaultId::new(1);

        // All entries are recent
        for i in 0..5 {
            state.client_sequences.insert(
                (org, vault, ClientId::new(format!("client-{i}"))),
                ClientSequenceEntry {
                    sequence: i + 1,
                    last_seen: 1_000_000,
                    last_idempotency_key: [0u8; 16],
                    last_request_hash: 0,
                },
            );
        }

        let mut pending = PendingExternalWrites::default();
        run_eviction(&mut state, &mut pending, 1_000_000 + 100, 86400);

        assert_eq!(state.client_sequences.len(), 5, "all entries retained");
        assert!(pending.client_sequences_deleted.is_empty(), "no deletes");
    }

    #[test]
    fn test_eviction_all_entries_expired() {
        let mut state = AppliedState::default();
        let org = OrganizationId::new(1);

        // Insert 3 entries, all very old
        for i in 0..3u64 {
            state.client_sequences.insert(
                (org, VaultId::new(i as i64 + 1), ClientId::new(format!("client-{i}"))),
                ClientSequenceEntry {
                    sequence: i + 1,
                    last_seen: 100,
                    last_idempotency_key: [0u8; 16],
                    last_request_hash: 0,
                },
            );
        }

        let mut pending = PendingExternalWrites::default();
        run_eviction(&mut state, &mut pending, 100 + 200_000, 86400);

        assert!(state.client_sequences.is_empty(), "all entries evicted");
        assert_eq!(pending.client_sequences_deleted.len(), 3, "three B+ tree deletes expected");
    }

    #[test]
    fn test_eviction_candidates_sorted_by_key() {
        let mut state = AppliedState::default();

        // Insert entries with IDs that sort in a specific order when encoded
        // as big-endian bytes. Org 2 > Org 1 in byte ordering.
        state.client_sequences.insert(
            (OrganizationId::new(2), VaultId::new(1), ClientId::new("b-client")),
            ClientSequenceEntry { sequence: 1, last_seen: 100, ..ClientSequenceEntry::default() },
        );
        state.client_sequences.insert(
            (OrganizationId::new(1), VaultId::new(1), ClientId::new("a-client")),
            ClientSequenceEntry { sequence: 2, last_seen: 100, ..ClientSequenceEntry::default() },
        );
        state.client_sequences.insert(
            (OrganizationId::new(1), VaultId::new(2), ClientId::new("c-client")),
            ClientSequenceEntry { sequence: 3, last_seen: 100, ..ClientSequenceEntry::default() },
        );

        let mut pending = PendingExternalWrites::default();
        run_eviction(&mut state, &mut pending, 100 + 200_000, 86400);

        // Verify deletion order: sorted by big-endian composite key
        assert_eq!(pending.client_sequences_deleted.len(), 3);

        // Keys should be sorted: (org=1,vault=1,"a-client") < (org=1,vault=2,"c-client") <
        // (org=2,vault=1,"b-client")
        let key_1_1_a = PendingExternalWrites::client_sequence_key(
            OrganizationId::new(1),
            VaultId::new(1),
            b"a-client",
        );
        let key_1_2_c = PendingExternalWrites::client_sequence_key(
            OrganizationId::new(1),
            VaultId::new(2),
            b"c-client",
        );
        let key_2_1_b = PendingExternalWrites::client_sequence_key(
            OrganizationId::new(2),
            VaultId::new(1),
            b"b-client",
        );
        assert_eq!(pending.client_sequences_deleted[0], key_1_1_a);
        assert_eq!(pending.client_sequences_deleted[1], key_1_2_c);
        assert_eq!(pending.client_sequences_deleted[2], key_2_1_b);
    }

    #[test]
    fn test_eviction_deterministic_across_replicas() {
        // Two independent state machines with identical state must produce
        // identical eviction results.
        let mut state_a = AppliedState::default();
        let mut state_b = AppliedState::default();

        for i in 0..10u64 {
            let entry = ClientSequenceEntry {
                sequence: i + 1,
                last_seen: 100 + i as i64,
                last_idempotency_key: [0u8; 16],
                last_request_hash: 0,
            };
            let key =
                (OrganizationId::new(1), VaultId::new(1), ClientId::new(format!("client-{i}")));
            state_a.client_sequences.insert(key.clone(), entry.clone());
            state_b.client_sequences.insert(key, entry);
        }

        let mut pending_a = PendingExternalWrites::default();
        let mut pending_b = PendingExternalWrites::default();

        run_eviction(&mut state_a, &mut pending_a, 100 + 200_000, 86400);
        run_eviction(&mut state_b, &mut pending_b, 100 + 200_000, 86400);

        assert_eq!(
            pending_a.client_sequences_deleted, pending_b.client_sequences_deleted,
            "eviction must be deterministic across replicas"
        );
        assert_eq!(
            state_a.client_sequences.len(),
            state_b.client_sequences.len(),
            "remaining entry count must match"
        );
    }

    #[test]
    fn test_eviction_clock_regression_no_panic() {
        // Simulate proposed_at going backward (clock regression after leader change).
        // With i64 saturating_sub, negative deltas become 0 — no eviction.
        let mut state = AppliedState::default();
        state.client_sequences.insert(
            (OrganizationId::new(1), VaultId::new(1), ClientId::new("client")),
            ClientSequenceEntry {
                sequence: 1,
                last_seen: 1_000_000,
                last_idempotency_key: [0u8; 16],
                last_request_hash: 0,
            },
        );

        let mut pending = PendingExternalWrites::default();
        // proposed_at is BEFORE last_seen — clock went backward
        run_eviction(&mut state, &mut pending, 500_000, 86400);

        assert_eq!(state.client_sequences.len(), 1, "no eviction on clock regression");
        assert!(pending.client_sequences_deleted.is_empty());
    }

    #[test]
    fn test_eviction_backward_by_more_than_ttl_no_overflow() {
        // proposed_at backward by more than TTL — verify no signed subtraction overflow
        let mut state = AppliedState::default();
        state.client_sequences.insert(
            (OrganizationId::new(1), VaultId::new(1), ClientId::new("client")),
            ClientSequenceEntry {
                sequence: 1,
                last_seen: i64::MAX - 100,
                last_idempotency_key: [0u8; 16],
                last_request_hash: 0,
            },
        );

        let mut pending = PendingExternalWrites::default();
        // proposed_at is 0 — massive backward step
        run_eviction(&mut state, &mut pending, 0, 86400);

        assert_eq!(state.client_sequences.len(), 1, "saturating_sub prevents overflow");
        assert!(pending.client_sequences_deleted.is_empty());
    }

    #[test]
    fn test_eviction_mixed_expired_and_fresh() {
        let mut state = AppliedState::default();
        let org = OrganizationId::new(1);
        let vault = VaultId::new(1);

        // Old entry (expired)
        state.client_sequences.insert(
            (org, vault, ClientId::new("old-client")),
            ClientSequenceEntry {
                sequence: 1,
                last_seen: 100,
                last_idempotency_key: [1u8; 16],
                last_request_hash: 42,
            },
        );

        // Fresh entry (within TTL)
        state.client_sequences.insert(
            (org, vault, ClientId::new("new-client")),
            ClientSequenceEntry {
                sequence: 2,
                last_seen: 100_000,
                last_idempotency_key: [2u8; 16],
                last_request_hash: 99,
            },
        );

        let mut pending = PendingExternalWrites::default();
        run_eviction(&mut state, &mut pending, 100_000 + 50_000, 86400);

        assert_eq!(state.client_sequences.len(), 1, "only fresh entry retained");
        assert!(
            state.client_sequences.contains_key(&(org, vault, ClientId::new("new-client"))),
            "new-client should survive eviction"
        );
        assert_eq!(pending.client_sequences_deleted.len(), 1, "one delete for old-client");
    }

    /// Verifies that `build_snapshot()` and `get_current_snapshot()` produce byte-identical
    /// snapshot files when the apply loop is quiesced. No events DB is configured, eliminating
    /// the only non-deterministic section.
    ///
    /// Both methods call the shared `write_snapshot_to_file()` function which reads from
    /// a `ReadTransaction` (deterministic B-tree iteration order), compresses via zstd
    /// (deterministic at the same compression level), and appends a SHA-256 checksum
    /// (pure function of compressed bytes).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_snapshot_determinism_build_and_get_current_produce_identical_files() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let dir = tempdir().expect("create temp dir");
        let mut store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");

        // Apply a batch of Raft entries that create organizations, vaults, and writes.
        // apply_to_state_machine persists state via save_state_core + flush_external_writes.
        //
        // System-tier setup (org creation, activation) is applied directly via apply_system;
        // the Raft-entry path is exercised for the org-tier entries (vault creation, writes).
        {
            let mut state = (*store.applied_state.load_full()).clone();
            store.apply_system(
                &SystemRequest::CreateOrganization {
                    slug: inferadb_ledger_types::OrganizationSlug::new(1000),
                    region: Region::US_EAST_VA,
                    tier: Default::default(),
                    admin: UserId::new(1),
                },
                &mut state,
            );
            store.apply_system(
                &SystemRequest::UpdateOrganizationStatus {
                    organization: OrganizationId::new(1),
                    status: OrganizationStatus::Active,
                },
                &mut state,
            );
            store.apply_system(
                &SystemRequest::CreateOrganization {
                    slug: inferadb_ledger_types::OrganizationSlug::new(2000),
                    region: Region::US_EAST_VA,
                    tier: Default::default(),
                    admin: UserId::new(1),
                },
                &mut state,
            );
            store.apply_system(
                &SystemRequest::UpdateOrganizationStatus {
                    organization: OrganizationId::new(2),
                    status: OrganizationStatus::Active,
                },
                &mut state,
            );
            store.applied_state.store(Arc::new(state));
        }

        let entries = vec![
            // Entry 1: Create vault in org 1
            make_committed_entry(
                1,
                1,
                wrap_org_payload(OrganizationRequest::CreateVault {
                    organization: OrganizationId::new(1),
                    slug: VaultSlug::new(100),
                    name: Some("vault-1".to_string()),
                    retention_policy: None,
                }),
            ),
            // Entry 2: Write to vault (populates vault_heights, vault_hashes, client_sequences)
            make_committed_entry(
                1,
                2,
                wrap_org_payload(OrganizationRequest::Write {
                    vault: VaultId::new(1),
                    transactions: vec![],
                    idempotency_key: [1u8; 16],
                    request_hash: 42,
                    organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    vault_slug: inferadb_ledger_types::VaultSlug::new(0),
                }),
            ),
            // Entry 3: Create vault in org 2
            make_committed_entry(
                1,
                3,
                wrap_org_payload(OrganizationRequest::CreateVault {
                    organization: OrganizationId::new(2),
                    slug: VaultSlug::new(200),
                    name: Some("vault-2".to_string()),
                    retention_policy: None,
                }),
            ),
            // Entry 4: Write to second vault
            make_committed_entry(
                1,
                4,
                wrap_org_payload(OrganizationRequest::Write {
                    vault: VaultId::new(2),
                    transactions: vec![],
                    idempotency_key: [2u8; 16],
                    request_hash: 99,
                    organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    vault_slug: inferadb_ledger_types::VaultSlug::new(0),
                }),
            ),
        ];

        store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&entries, None)
            .await
            .expect("apply batch");

        // Now take two snapshots via different code paths.
        // No events DB configured, so event section is deterministically empty.

        // Snapshot A via get_current_snapshot() (on RaftLogStore)
        let snapshot_a = store
            .get_current_snapshot()
            .await
            .expect("get_current_snapshot")
            .expect("snapshot should exist");

        // Snapshot B via build_snapshot() (on LedgerSnapshotBuilder)
        let mut builder = store.get_snapshot_builder();
        let snapshot_b = builder.build_snapshot().await.expect("build_snapshot");

        // Read both files completely
        let mut bytes_a = Vec::new();
        let mut file_a = snapshot_a.0;
        file_a.seek(std::io::SeekFrom::Start(0)).await.expect("seek a");
        file_a.read_to_end(&mut bytes_a).await.expect("read a");

        let mut bytes_b = Vec::new();
        let mut file_b = snapshot_b.0;
        file_b.seek(std::io::SeekFrom::Start(0)).await.expect("seek b");
        file_b.read_to_end(&mut bytes_b).await.expect("read b");

        // The files must be byte-identical
        assert_eq!(bytes_a.len(), bytes_b.len(), "snapshot files should have identical length");
        assert_eq!(
            bytes_a, bytes_b,
            "get_current_snapshot() and build_snapshot() should produce byte-identical files \
             when the apply loop is quiesced and no events are present"
        );

        // Sanity: file should be non-trivial
        assert!(
            bytes_a.len() > 32,
            "snapshot file should be non-trivial ({} bytes)",
            bytes_a.len()
        );

        // Verify both snapshots report the same last_applied
        assert_eq!(
            snapshot_a.1.last_log_id, snapshot_b.1.last_log_id,
            "both snapshots should report the same last_applied"
        );
    }

    // ========================================================================
    // State Root Commitment Buffer Tests
    // ========================================================================

    #[test]
    fn test_commitment_buffer_drain_returns_all_entries() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");

        // Push some commitments directly into the buffer
        {
            let mut buf = store.state_root_commitments.lock().unwrap();
            buf.push(crate::types::StateRootCommitment {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                vault_height: 10,
                state_root: [0xAA; 32],
            });
            buf.push(crate::types::StateRootCommitment {
                organization: OrganizationId::new(2),
                vault: VaultId::new(2),
                vault_height: 20,
                state_root: [0xBB; 32],
            });
        }

        // Drain should return all entries
        let drained = store.drain_state_root_commitments();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].organization, OrganizationId::new(1));
        assert_eq!(drained[0].vault_height, 10);
        assert_eq!(drained[1].organization, OrganizationId::new(2));
        assert_eq!(drained[1].vault_height, 20);

        // Buffer should be empty after drain
        let again = store.drain_state_root_commitments();
        assert!(again.is_empty());
    }

    #[test]
    fn test_commitment_buffer_shared_via_arc() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");

        // Get a shared handle (same pattern as OrganizationGroup)
        let buffer = store.commitment_buffer();

        // Push via the shared handle
        {
            let mut buf = buffer.lock().unwrap();
            buf.push(crate::types::StateRootCommitment {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                vault_height: 5,
                state_root: [0xCC; 32],
            });
        }

        // Drain via the store — should see the same data
        let drained = store.drain_state_root_commitments();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].state_root, [0xCC; 32]);

        // Both should now be empty
        assert!(buffer.lock().unwrap().is_empty());
    }

    #[test]
    fn test_commitment_buffer_cap_prevents_unbounded_growth() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");

        // Fill buffer to capacity
        {
            let mut buf = store.state_root_commitments.lock().unwrap();
            for i in 0..10_000u64 {
                buf.push(crate::types::StateRootCommitment {
                    organization: OrganizationId::new(1),
                    vault: VaultId::new(1),
                    vault_height: i,
                    state_root: [0u8; 32],
                });
            }
            assert_eq!(buf.len(), 10_000);
        }

        // The buffer is at cap but drain still works
        let drained = store.drain_state_root_commitments();
        assert_eq!(drained.len(), 10_000);
        assert!(store.state_root_commitments.lock().unwrap().is_empty());
    }

    #[test]
    fn test_commitment_buffer_empty_drain() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");

        let drained = store.drain_state_root_commitments();
        assert!(drained.is_empty());
    }

    // ========================================================================
    // State Root Verification Tests (via apply_to_state_machine)
    // ========================================================================

    /// Creates a store with a block archive wired up for verification tests.
    fn store_with_archive(
        dir: &std::path::Path,
    ) -> (RaftLogStore<FileBackend>, Arc<inferadb_ledger_state::BlockArchive<FileBackend>>) {
        // Archive needs its own database (separate from raft log)
        let archive_db = Arc::new(
            inferadb_ledger_store::Database::create(dir.join("archive.db"))
                .expect("create archive db"),
        );
        let archive = Arc::new(inferadb_ledger_state::BlockArchive::new(archive_db));

        let store = RaftLogStore::<FileBackend>::open(dir.join("raft_log.db"))
            .expect("open store")
            .with_block_archive(Arc::clone(&archive));

        (store, archive)
    }

    /// Creates a store with block archive and divergence channel.
    fn store_with_archive_and_divergence(
        dir: &std::path::Path,
    ) -> (
        RaftLogStore<FileBackend>,
        Arc<inferadb_ledger_state::BlockArchive<FileBackend>>,
        tokio::sync::mpsc::UnboundedReceiver<crate::types::StateRootDivergence>,
    ) {
        let (store, archive) = store_with_archive(dir);
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let store = store.with_divergence_sender(sender);
        (store, archive, receiver)
    }

    #[tokio::test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    async fn test_apply_emits_commitments_to_buffer() {
        // When apply_to_state_machine processes a Write that creates vault entries,
        // it should buffer StateRootCommitments.
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let (mut store, _archive) = store_with_archive(dir.path());

        // Set up org and vault
        {
            let mut state = (*store.applied_state.load_full()).clone();
            setup_org_and_vault(&mut state);
            store.applied_state.store(Arc::new(state));
        }

        // Apply a write entry
        let entry = make_committed_entry(
            1,
            1,
            wrap_org_payload(simple_write_request(OrganizationId::new(1), VaultId::new(1))),
        );

        store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[entry], None)
            .await
            .expect("apply");

        // Buffer should contain a commitment for this vault
        let commitments = store.drain_state_root_commitments();
        assert_eq!(commitments.len(), 1, "should have one commitment after write");
        assert_eq!(commitments[0].organization, OrganizationId::new(1));
        assert_eq!(commitments[0].vault, VaultId::new(1));
        assert_eq!(commitments[0].vault_height, 1);
        // State root should be non-zero (actual value depends on state layer)
        // Without a state layer, it will be the zero hash
        assert_eq!(commitments[0].state_root.len(), 32);
    }

    #[tokio::test]
    async fn test_apply_no_commitments_for_non_write_entries() {
        // CreateOrganization / CreateVault don't produce vault entries,
        // so no commitments should be buffered.
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let (mut store, _archive) = store_with_archive(dir.path());

        let entry = make_committed_entry(
            1,
            1,
            wrap_system_payload(SystemRequest::CreateOrganization {
                slug: inferadb_ledger_types::OrganizationSlug::new(999),
                region: Region::US_EAST_VA,
                tier: Default::default(),
                admin: UserId::new(1),
            }),
        );

        store
            .apply_committed_entries::<crate::types::SystemRequest>(&[entry], None)
            .await
            .expect("apply");

        let commitments = store.drain_state_root_commitments();
        assert!(commitments.is_empty(), "admin ops should not produce commitments");
    }

    #[tokio::test]
    async fn test_apply_multiple_writes_emits_multiple_commitments() {
        // Two writes in one batch → two commitments.
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let (mut store, _archive) = store_with_archive(dir.path());

        // Set up org with two vaults
        {
            let mut state = (*store.applied_state.load_full()).clone();
            let (org_id, _vault1) = setup_org_and_vault(&mut state);

            // Add second vault
            let vault2 = state.sequences.next_vault();
            let vault2_slug = VaultSlug::new(3000);
            state.vault_heights.insert((org_id, vault2), 0);
            state.vaults.insert(
                (org_id, vault2),
                VaultMeta {
                    organization: org_id,
                    vault: vault2,
                    slug: vault2_slug,
                    name: Some("vault-2".to_string()),
                    deleted: false,
                    last_write_timestamp: 0,
                    retention_policy: BlockRetentionPolicy::default(),
                },
            );
            state.vault_slug_index.insert(vault2_slug, (org_id, vault2));
            state.vault_id_to_slug.insert((org_id, vault2), vault2_slug);
            state.vault_health.insert((org_id, vault2), VaultHealthStatus::Healthy);
        }

        let entries = vec![
            make_committed_entry(
                1,
                1,
                wrap_org_payload(simple_write_request(OrganizationId::new(1), VaultId::new(1))),
            ),
            make_committed_entry(
                1,
                2,
                wrap_org_payload(OrganizationRequest::Write {
                    vault: VaultId::new(2),
                    transactions: vec![Transaction {
                        id: [2u8; 16],
                        client_id: ClientId::new("test-client"),
                        sequence: 0,
                        operations: vec![Operation::SetEntity {
                            key: "key2".to_string(),
                            value: b"value2".to_vec(),
                            condition: None,
                            expires_at: None,
                        }],
                        timestamp: fixed_timestamp(),
                    }],
                    idempotency_key: [0u8; 16],
                    request_hash: 0,

                    organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    vault_slug: inferadb_ledger_types::VaultSlug::new(0),
                }),
            ),
        ];

        store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&entries, None)
            .await
            .expect("apply");

        let commitments = store.drain_state_root_commitments();
        assert_eq!(commitments.len(), 2, "two writes → two commitments");
        assert_eq!(commitments[0].vault, VaultId::new(1));
        assert_eq!(commitments[0].vault_height, 1);
        assert_eq!(commitments[1].vault, VaultId::new(2));
        assert_eq!(commitments[1].vault_height, 1);
    }

    #[tokio::test]
    async fn test_verify_commitment_matching_state_root() {
        // Full end-to-end: apply a write (which archives the block), then apply
        // a second entry piggybacking the correct commitment. Verification should
        // succeed silently (no divergence event).
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let (mut store, _archive, mut divergence_rx) =
            store_with_archive_and_divergence(dir.path());

        {
            let mut state = (*store.applied_state.load_full()).clone();
            setup_org_and_vault(&mut state);
            store.applied_state.store(Arc::new(state));
        }

        // Step 1: Apply a write → archives block, buffers commitment
        let write_entry = make_committed_entry(
            1,
            1,
            wrap_org_payload(simple_write_request(OrganizationId::new(1), VaultId::new(1))),
        );
        store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[write_entry], None)
            .await
            .expect("apply write");

        // Drain the commitment (simulating leader draining for next payload)
        let commitments = store.drain_state_root_commitments();
        assert_eq!(commitments.len(), 1);

        // Step 2: Apply a second entry carrying the commitment from step 1.
        // The commitment's state_root matches the archived block's state_root.
        let second_entry = make_committed_entry(
            1,
            2,
            RaftPayload {
                request: SystemRequest::CreateOrganization {
                    slug: inferadb_ledger_types::OrganizationSlug::new(9999),
                    region: Region::US_EAST_VA,
                    tier: Default::default(),
                    admin: UserId::new(1),
                },
                proposed_at: Utc::now(),
                state_root_commitments: commitments,
                caller: 0,
            },
        );
        store
            .apply_committed_entries::<crate::types::SystemRequest>(&[second_entry], None)
            .await
            .expect("apply verify");

        // No divergence should have been sent
        assert!(
            matches!(divergence_rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "matching state root should not trigger divergence"
        );
    }

    #[tokio::test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    async fn test_verify_commitment_mismatching_state_root_sends_divergence() {
        // Piggyback a commitment with a deliberately wrong state_root.
        // The verification should detect the mismatch and send a divergence event.
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let (mut store, _archive, mut divergence_rx) =
            store_with_archive_and_divergence(dir.path());

        {
            let mut state = (*store.applied_state.load_full()).clone();
            setup_org_and_vault(&mut state);
            store.applied_state.store(Arc::new(state));
        }

        // Apply a write to archive a block
        let write_entry = make_committed_entry(
            1,
            1,
            wrap_org_payload(simple_write_request(OrganizationId::new(1), VaultId::new(1))),
        );
        store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[write_entry], None)
            .await
            .expect("apply write");

        // Drain and tamper with the state root
        let mut commitments = store.drain_state_root_commitments();
        assert_eq!(commitments.len(), 1);
        commitments[0].state_root = [0xFF; 32]; // Wrong state root

        // Apply entry carrying the tampered commitment
        let verify_entry = make_committed_entry(
            1,
            2,
            RaftPayload {
                request: SystemRequest::CreateOrganization {
                    slug: inferadb_ledger_types::OrganizationSlug::new(8888),
                    region: Region::US_EAST_VA,
                    tier: Default::default(),
                    admin: UserId::new(1),
                },
                proposed_at: Utc::now(),
                state_root_commitments: commitments,
                caller: 0,
            },
        );
        store
            .apply_committed_entries::<crate::types::SystemRequest>(&[verify_entry], None)
            .await
            .expect("apply verify");

        // Should have received a divergence event
        let divergence = divergence_rx.try_recv().expect("should receive divergence event");
        assert_eq!(divergence.organization, OrganizationId::new(1));
        assert_eq!(divergence.vault, VaultId::new(1));
        assert_eq!(divergence.vault_height, 1);
        assert_eq!(divergence.leader_state_root, [0xFF; 32]);
        // The local state root should be whatever the store computed (not 0xFF)
        assert_ne!(divergence.local_state_root, [0xFF; 32]);
    }

    #[tokio::test]
    async fn test_verify_commitment_missing_block_skips_silently() {
        // If the commitment references a vault height not yet archived
        // (e.g., after snapshot install), verification should skip without error.
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let (mut store, _archive, mut divergence_rx) =
            store_with_archive_and_divergence(dir.path());

        {
            let mut state = (*store.applied_state.load_full()).clone();
            setup_org_and_vault(&mut state);
            store.applied_state.store(Arc::new(state));
        }

        // Don't apply any writes — no blocks exist in the archive.
        // Piggyback a commitment referencing a non-existent vault height.
        let entry = make_committed_entry(
            1,
            1,
            RaftPayload {
                request: SystemRequest::CreateOrganization {
                    slug: inferadb_ledger_types::OrganizationSlug::new(7777),
                    region: Region::US_EAST_VA,
                    tier: Default::default(),
                    admin: UserId::new(1),
                },
                proposed_at: Utc::now(),
                state_root_commitments: vec![crate::types::StateRootCommitment {
                    organization: OrganizationId::new(1),
                    vault: VaultId::new(1),
                    vault_height: 999, // Never archived
                    state_root: [0xDD; 32],
                }],
                caller: 0,
            },
        );
        store
            .apply_committed_entries::<crate::types::SystemRequest>(&[entry], None)
            .await
            .expect("apply");

        // No divergence — just skipped
        assert!(
            matches!(divergence_rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "missing block should not trigger divergence"
        );
    }

    #[tokio::test]
    async fn test_verify_commitment_no_archive_skips() {
        // Without a block archive configured, verification is a no-op.
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let mut store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");

        {
            let mut state = (*store.applied_state.load_full()).clone();
            setup_org_and_vault(&mut state);
            store.applied_state.store(Arc::new(state));
        }

        // Apply entry with commitments but no archive — should not panic
        let entry = make_committed_entry(
            1,
            1,
            RaftPayload {
                request: SystemRequest::CreateOrganization {
                    slug: inferadb_ledger_types::OrganizationSlug::new(6666),
                    region: Region::US_EAST_VA,
                    tier: Default::default(),
                    admin: UserId::new(1),
                },
                proposed_at: Utc::now(),
                state_root_commitments: vec![crate::types::StateRootCommitment {
                    organization: OrganizationId::new(1),
                    vault: VaultId::new(1),
                    vault_height: 1,
                    state_root: [0xEE; 32],
                }],
                caller: 0,
            },
        );
        store
            .apply_committed_entries::<crate::types::SystemRequest>(&[entry], None)
            .await
            .expect("apply should succeed without archive");
    }

    #[tokio::test]
    async fn test_commitment_buffer_cap_during_apply() {
        // Verify the 10K cap works during apply_to_state_machine.
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let (mut store, _archive) = store_with_archive(dir.path());

        {
            let mut state = (*store.applied_state.load_full()).clone();
            setup_org_and_vault(&mut state);
            store.applied_state.store(Arc::new(state));
        }

        // Pre-fill buffer to capacity so the next apply triggers overflow
        {
            let mut buf = store.state_root_commitments.lock().unwrap();
            for i in 0..10_000u64 {
                buf.push(crate::types::StateRootCommitment {
                    organization: OrganizationId::new(1),
                    vault: VaultId::new(1),
                    vault_height: i,
                    state_root: [0u8; 32],
                });
            }
        }

        // Apply a write — should add one commitment and drop the oldest
        let entry = make_committed_entry(
            1,
            1,
            wrap_org_payload(simple_write_request(OrganizationId::new(1), VaultId::new(1))),
        );
        store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[entry], None)
            .await
            .expect("apply");

        let buf = store.state_root_commitments.lock().unwrap();
        assert!(buf.len() <= 10_000, "buffer should not exceed 10K cap, got {}", buf.len());
    }

    #[tokio::test]
    async fn test_divergence_sender_none_does_not_panic() {
        // When no divergence sender is configured, mismatches should be
        // logged but not panic.
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let (mut store, _archive) = store_with_archive(dir.path());
        // Note: no with_divergence_sender — sender is None

        {
            let mut state = (*store.applied_state.load_full()).clone();
            setup_org_and_vault(&mut state);
            store.applied_state.store(Arc::new(state));
        }

        // Apply a write to archive a block
        let write_entry = make_committed_entry(
            1,
            1,
            wrap_org_payload(simple_write_request(OrganizationId::new(1), VaultId::new(1))),
        );
        store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&[write_entry], None)
            .await
            .expect("apply");

        // Drain and tamper
        let mut commitments = store.drain_state_root_commitments();
        commitments[0].state_root = [0xFF; 32];

        // Apply with tampered commitment — should not panic despite no sender
        let verify_entry = make_committed_entry(
            1,
            2,
            RaftPayload {
                request: SystemRequest::CreateOrganization {
                    slug: inferadb_ledger_types::OrganizationSlug::new(5555),
                    region: Region::US_EAST_VA,
                    tier: Default::default(),
                    admin: UserId::new(1),
                },
                proposed_at: Utc::now(),
                state_root_commitments: commitments,
                caller: 0,
            },
        );
        store
            .apply_committed_entries::<crate::types::SystemRequest>(&[verify_entry], None)
            .await
            .expect("should not panic without divergence sender");
    }

    #[tokio::test]
    async fn test_piggybacking_end_to_end() {
        // Full piggybacking cycle: write → buffer → drain → piggyback → verify.
        // Simulates two consecutive apply batches.
        // openraft trait removed — tests use apply_committed_entries directly

        let dir = tempdir().expect("create temp dir");
        let (mut store, _archive, mut divergence_rx) =
            store_with_archive_and_divergence(dir.path());

        {
            let mut state = (*store.applied_state.load_full()).clone();
            setup_org_and_vault(&mut state);
            store.applied_state.store(Arc::new(state));
        }

        // Batch 1: Write to vault → buffers commitment
        let batch1 = vec![make_committed_entry(
            1,
            1,
            wrap_org_payload(simple_write_request(OrganizationId::new(1), VaultId::new(1))),
        )];
        store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&batch1, None)
            .await
            .expect("apply batch 1");

        let commitments_from_batch1 = store.drain_state_root_commitments();
        assert_eq!(commitments_from_batch1.len(), 1);

        // Batch 2: Another write, piggybacking batch 1's commitments
        let batch2 = vec![make_committed_entry(
            1,
            2,
            RaftPayload {
                request: simple_write_request(OrganizationId::new(1), VaultId::new(1)),
                proposed_at: Utc::now(),
                state_root_commitments: commitments_from_batch1,
                caller: 0,
            },
        )];
        store
            .apply_committed_entries::<crate::types::OrganizationRequest>(&batch2, None)
            .await
            .expect("apply batch 2");

        // Verification passed — no divergence
        assert!(
            matches!(divergence_rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "correctly piggybacked commitments should not trigger divergence"
        );

        // Batch 2 also produced commitments for its own writes
        let commitments_from_batch2 = store.drain_state_root_commitments();
        assert_eq!(commitments_from_batch2.len(), 1);
        assert_eq!(commitments_from_batch2[0].vault_height, 2);
    }

    #[test]
    fn test_activate_onboarding_user_persists_org_meta_to_btree() {
        let dir = tempdir().expect("create temp dir");

        // Build store with state layer so sys_service is available
        let state_db = Arc::new(
            inferadb_ledger_store::Database::create(dir.path().join("state.db"))
                .expect("create state db"),
        );
        let state_layer = Arc::new(inferadb_ledger_state::StateLayer::new(state_db));
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
            .expect("open store")
            .with_state_layer(state_layer);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_slug = inferadb_ledger_types::OrganizationSlug::new(5000);
        let user_slug = UserSlug::new(6000);
        let email_hmac = "test-hmac-activate".to_string();

        // Step 0: CreateOnboardingUser — allocates user+org in Provisioning
        let create_request = SystemRequest::CreateOnboardingUser {
            email_hmac: email_hmac.clone(),
            user_slug,
            organization_slug: org_slug,
            region: Region::US_EAST_VA,
        };
        let (response, _) = store.apply_system(&create_request, &mut state);
        let (user_id, organization_id) = match response {
            LedgerResponse::OnboardingUserCreated { user_id, organization_id } => {
                (user_id, organization_id)
            },
            other => panic!("expected OnboardingUserCreated, got {other:?}"),
        };

        // Verify org starts in Provisioning
        assert_eq!(
            state.organizations.get(&organization_id).expect("org meta exists").status,
            OrganizationStatus::Provisioning,
        );

        // Step 2: ActivateOnboardingUser — should persist Active to B+ tree
        let activate_request = SystemRequest::ActivateOnboardingUser {
            user_id,
            user_slug,
            organization_id,
            organization_slug: org_slug,
            email_hmac,
        };
        let mut pending = PendingExternalWrites::default();
        let mut events = Vec::new();
        let mut op_index = 0u32;
        let (response, _) = store.apply_system_request_with_events(
            &activate_request,
            &mut state,
            fixed_timestamp(),
            &mut op_index,
            &mut events,
            0,
            &mut pending,
            None,
            false,
            0,
            false,
        );
        assert!(
            matches!(response, LedgerResponse::OnboardingUserActivated),
            "expected OnboardingUserActivated, got {response:?}",
        );

        // Verify in-memory status is Active
        assert_eq!(
            state.organizations.get(&organization_id).expect("org meta exists").status,
            OrganizationStatus::Active,
        );

        // Verify pending.organizations contains the persistence entry (the bug fix)
        let org_entry = pending.organizations.iter().find(|(id, _)| *id == organization_id);
        assert!(
            org_entry.is_some(),
            "ActivateOnboardingUser must push org_meta to pending.organizations for B+ tree persistence",
        );

        // Decode the blob and verify the persisted status is Active
        let (_, blob) = org_entry.expect("checked above");
        let persisted_meta: super::types::OrganizationMeta =
            postcard::from_bytes(blob).expect("decode org_meta blob");
        assert_eq!(persisted_meta.status, OrganizationStatus::Active);
        assert_eq!(persisted_meta.organization, organization_id);
    }

    #[test]
    fn test_update_organization_status_syncs_registry() {
        let dir = tempdir().expect("create temp dir");

        let state_db = Arc::new(
            inferadb_ledger_store::Database::create(dir.path().join("state.db"))
                .expect("create state db"),
        );
        let state_layer = Arc::new(inferadb_ledger_state::StateLayer::new(state_db));
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
            .expect("open store")
            .with_state_layer(state_layer.clone());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_slug = inferadb_ledger_types::OrganizationSlug::new(7000);

        // Create org — starts in Provisioning
        let (response, _) = store.apply_system(
            &SystemRequest::CreateOrganization {
                slug: org_slug,
                region: Region::GLOBAL,
                tier: Default::default(),
                admin: UserId::new(1),
            },
            &mut state,
        );
        let org_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            other => panic!("expected OrganizationCreated, got {other:?}"),
        };

        // Verify registry starts at Provisioning
        let sys =
            inferadb_ledger_state::system::SystemOrganizationService::new(state_layer.clone());
        let registry =
            sys.get_organization(org_id).expect("read registry").expect("registry exists");
        assert_eq!(registry.status, OrganizationStatus::Provisioning);
        let initial_config_version = registry.config_version;

        // UpdateOrganizationStatus(Active) — should sync registry
        store.apply_system(
            &SystemRequest::UpdateOrganizationStatus {
                organization: org_id,
                status: OrganizationStatus::Active,
            },
            &mut state,
        );

        let registry =
            sys.get_organization(org_id).expect("read registry").expect("registry exists");
        assert_eq!(
            registry.status,
            OrganizationStatus::Active,
            "registry status should be Active after UpdateOrganizationStatus"
        );
        assert_eq!(
            registry.config_version,
            initial_config_version + 1,
            "config_version should increment"
        );
        assert!(registry.deleted_at.is_none(), "deleted_at should not be set for Active");

        // UpdateOrganizationStatus(Deleted) — should sync registry + set deleted_at
        store.apply_system(
            &SystemRequest::UpdateOrganizationStatus {
                organization: org_id,
                status: OrganizationStatus::Deleted,
            },
            &mut state,
        );

        let registry =
            sys.get_organization(org_id).expect("read registry").expect("registry exists");
        assert_eq!(
            registry.status,
            OrganizationStatus::Deleted,
            "registry status should be Deleted"
        );
        assert_eq!(
            registry.config_version,
            initial_config_version + 2,
            "config_version should increment again"
        );
        assert!(registry.deleted_at.is_some(), "deleted_at should be set for Deleted status");
    }

    #[test]
    fn test_saga_compensation_marks_org_deleted_in_meta_and_registry() {
        let dir = tempdir().expect("create temp dir");

        let state_db = Arc::new(
            inferadb_ledger_store::Database::create(dir.path().join("state.db"))
                .expect("create state db"),
        );
        let state_layer = Arc::new(inferadb_ledger_state::StateLayer::new(state_db));
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
            .expect("open store")
            .with_state_layer(state_layer.clone());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_slug = inferadb_ledger_types::OrganizationSlug::new(8000);

        // Simulate saga step 0: CreateOrganization (org starts in Provisioning)
        let (response, _) = store.apply_system(
            &SystemRequest::CreateOrganization {
                slug: org_slug,
                region: Region::GLOBAL,
                tier: Default::default(),
                admin: UserId::new(1),
            },
            &mut state,
        );
        let org_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            other => panic!("expected OrganizationCreated, got {other:?}"),
        };

        // Verify both meta and registry start at Provisioning
        assert_eq!(
            state.organizations.get(&org_id).expect("org meta exists").status,
            OrganizationStatus::Provisioning,
        );
        let sys =
            inferadb_ledger_state::system::SystemOrganizationService::new(state_layer.clone());
        let registry =
            sys.get_organization(org_id).expect("read registry").expect("registry exists");
        assert_eq!(registry.status, OrganizationStatus::Provisioning);

        // Simulate saga compensation: UpdateOrganizationStatus(Deleted)
        // This is what the saga orchestrator sends on permanent failure.
        store.apply_system(
            &SystemRequest::UpdateOrganizationStatus {
                organization: org_id,
                status: OrganizationStatus::Deleted,
            },
            &mut state,
        );

        // Verify meta is Deleted
        assert_eq!(
            state.organizations.get(&org_id).expect("org meta exists").status,
            OrganizationStatus::Deleted,
            "OrganizationMeta should be Deleted after compensation"
        );

        // Verify registry is Deleted with deleted_at (enables PurgeJob discovery)
        let registry =
            sys.get_organization(org_id).expect("read registry").expect("registry exists");
        assert_eq!(
            registry.status,
            OrganizationStatus::Deleted,
            "OrganizationRegistry should be Deleted after compensation (fixes zombie org bug)"
        );
        assert!(
            registry.deleted_at.is_some(),
            "deleted_at must be set so PurgeJob can compute retention window"
        );
    }

    // =========================================================================
    // Credential CRUD apply handler tests
    // =========================================================================

    /// Creates a `RaftLogStore` backed by a real B+ tree with a state layer.
    ///
    /// Credential operations require `sys_service` (state layer) to be present.
    fn create_store_with_state(dir: &std::path::Path) -> RaftLogStore<FileBackend> {
        let state_db = Arc::new(
            inferadb_ledger_store::Database::create(dir.join("state.db")).expect("create state db"),
        );
        let state_layer = Arc::new(inferadb_ledger_state::StateLayer::new(state_db));
        RaftLogStore::<FileBackend>::open(dir.join("raft_log.db"))
            .expect("open store")
            .with_state_layer(state_layer)
    }

    /// Creates a user in the state machine so credential operations can reference it.
    fn create_test_user(
        store: &RaftLogStore<FileBackend>,
        state: &mut AppliedState,
        slug: UserSlug,
        region: Region,
    ) -> UserId {
        let (response, _) = store.apply_system(
            &SystemRequest::CreateUser {
                user: UserId::new(0), // Ignored — state machine assigns
                admin: false,
                slug,
                region,
            },
            state,
        );
        match response {
            LedgerResponse::UserCreated { user_id, .. } => user_id,
            other => panic!("expected UserCreated, got {other:?}"),
        }
    }

    /// Creates a passkey credential for a user via the apply handler.
    ///
    /// Uses `seed` to generate distinct `credential_id`/`public_key` bytes
    /// so multiple passkeys per user don't collide.
    fn create_passkey_credential_with_seed(
        store: &RaftLogStore<FileBackend>,
        state: &mut AppliedState,
        user_id: UserId,
        seed: u8,
    ) -> UserCredentialId {
        let (response, _) = store.apply_system(
            &SystemRequest::CreateUserCredential {
                user_id,
                credential_type: CredentialType::Passkey,
                credential_data: CredentialData::Passkey(PasskeyCredential {
                    credential_id: vec![seed, seed + 1, seed + 2],
                    public_key: vec![seed + 3, seed + 4, seed + 5],
                    sign_count: 0,
                    transports: vec!["internal".to_string()],
                    backup_eligible: true,
                    backup_state: false,
                    attestation_format: Some("packed".to_string()),
                    aaguid: None,
                }),
                name: format!("Passkey {seed}"),
            },
            state,
        );
        match response {
            LedgerResponse::UserCredentialCreated { credential_id } => credential_id,
            other => panic!("expected UserCredentialCreated, got {other:?}"),
        }
    }

    /// Creates a passkey credential with default seed (1).
    fn create_passkey_credential(
        store: &RaftLogStore<FileBackend>,
        state: &mut AppliedState,
        user_id: UserId,
    ) -> UserCredentialId {
        create_passkey_credential_with_seed(store, state, user_id, 1)
    }

    /// Creates a recovery code credential for a user via the apply handler.
    fn create_recovery_credential(
        store: &RaftLogStore<FileBackend>,
        state: &mut AppliedState,
        user_id: UserId,
        code_hashes: Vec<[u8; 32]>,
    ) -> UserCredentialId {
        let total = code_hashes.len() as u8;
        let (response, _) = store.apply_system(
            &SystemRequest::CreateUserCredential {
                user_id,
                credential_type: CredentialType::RecoveryCode,
                credential_data: CredentialData::RecoveryCode(RecoveryCodeCredential {
                    code_hashes,
                    total_generated: total,
                }),
                name: "Recovery codes".to_string(),
            },
            state,
        );
        match response {
            LedgerResponse::UserCredentialCreated { credential_id } => credential_id,
            other => panic!("expected UserCredentialCreated, got {other:?}"),
        }
    }

    /// Applies a request at `fixed_timestamp()`, discarding events and pending writes.
    fn apply_at_fixed_time<R: ApplyableRequest>(
        store: &RaftLogStore<FileBackend>,
        request: &R,
        state: &mut AppliedState,
    ) -> LedgerResponse {
        let mut events = Vec::new();
        let mut op_index = 0u32;
        let mut pending = PendingExternalWrites::default();
        let (response, _) = R::apply_on(
            store,
            request,
            state,
            fixed_timestamp(),
            &mut op_index,
            &mut events,
            0,
            &mut pending,
            None,
            false,
            0,
            false,
        );
        response
    }

    /// Creates a TOTP challenge via the apply handler.
    fn create_test_totp_challenge(
        store: &RaftLogStore<FileBackend>,
        state: &mut AppliedState,
        user_id: UserId,
        user_slug: UserSlug,
        nonce: [u8; 32],
        expires_at: DateTime<Utc>,
    ) {
        let (response, _) = store.apply_system(
            &SystemRequest::CreateTotpChallenge {
                user_id,
                user_slug,
                nonce,
                expires_at,
                primary_method: PrimaryAuthMethod::EmailCode,
            },
            state,
        );
        match response {
            LedgerResponse::TotpChallengeCreated { nonce: n } => {
                assert_eq!(n, nonce);
            },
            other => panic!("expected TotpChallengeCreated, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_create_user_credential_passkey() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);
        let cred_id = create_passkey_credential(&store, &mut state, user_id);

        assert_eq!(cred_id, UserCredentialId::new(1), "first credential gets ID 1");
    }

    #[test]
    fn test_apply_create_user_credential_totp() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);

        let (response, _) = store.apply_system(
            &SystemRequest::CreateUserCredential {
                user_id,
                credential_type: CredentialType::Totp,
                credential_data: CredentialData::Totp(TotpCredential {
                    secret: zeroize::Zeroizing::new(vec![0xAA; 20]),
                    algorithm: inferadb_ledger_types::TotpAlgorithm::Sha1,
                    digits: 6,
                    period: 30,
                }),
                name: "Authenticator app".to_string(),
            },
            &mut state,
        );

        match response {
            LedgerResponse::UserCredentialCreated { credential_id } => {
                assert_eq!(credential_id, UserCredentialId::new(1));
            },
            other => panic!("expected UserCredentialCreated, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_create_user_credential_recovery_codes() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);

        let (response, _) = store.apply_system(
            &SystemRequest::CreateUserCredential {
                user_id,
                credential_type: CredentialType::RecoveryCode,
                credential_data: CredentialData::RecoveryCode(RecoveryCodeCredential {
                    code_hashes: vec![[0xAA; 32], [0xBB; 32], [0xCC; 32]],
                    total_generated: 3,
                }),
                name: "Recovery codes".to_string(),
            },
            &mut state,
        );

        match response {
            LedgerResponse::UserCredentialCreated { credential_id } => {
                assert_eq!(credential_id, UserCredentialId::new(1));
            },
            other => panic!("expected UserCredentialCreated, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_create_user_credential_duplicate_totp_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);

        let totp_request = SystemRequest::CreateUserCredential {
            user_id,
            credential_type: CredentialType::Totp,
            credential_data: CredentialData::Totp(TotpCredential {
                secret: zeroize::Zeroizing::new(vec![0xAA; 20]),
                algorithm: inferadb_ledger_types::TotpAlgorithm::Sha1,
                digits: 6,
                period: 30,
            }),
            name: "TOTP 1".to_string(),
        };

        // First TOTP succeeds
        let (r1, _) = store.apply_system(&totp_request, &mut state);
        assert!(matches!(r1, LedgerResponse::UserCredentialCreated { .. }));

        // Second TOTP rejected (one TOTP per user invariant)
        let totp_request_2 = SystemRequest::CreateUserCredential {
            user_id,
            credential_type: CredentialType::Totp,
            credential_data: CredentialData::Totp(TotpCredential {
                secret: zeroize::Zeroizing::new(vec![0xBB; 20]),
                algorithm: inferadb_ledger_types::TotpAlgorithm::Sha1,
                digits: 6,
                period: 30,
            }),
            name: "TOTP 2".to_string(),
        };
        let (r2, _) = store.apply_system(&totp_request_2, &mut state);
        match r2 {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::AlreadyExists);
            },
            other => panic!("expected Error(AlreadyExists), got {other:?}"),
        }
    }

    #[test]
    fn test_apply_update_user_credential() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);
        let cred_id = create_passkey_credential(&store, &mut state, user_id);

        let (response, _) = store.apply_system(
            &SystemRequest::UpdateUserCredential {
                user_id,
                credential_id: cred_id,
                name: Some("Renamed Key".to_string()),
                enabled: Some(false),
                passkey_update: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::UserCredentialUpdated { credential_id } => {
                assert_eq!(credential_id, cred_id);
            },
            other => panic!("expected UserCredentialUpdated, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_update_user_credential_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);

        let (response, _) = store.apply_system(
            &SystemRequest::UpdateUserCredential {
                user_id,
                credential_id: UserCredentialId::new(999),
                name: Some("Nonexistent".to_string()),
                enabled: None,
                passkey_update: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => assert_eq!(code, ErrorCode::NotFound),
            other => panic!("expected Error(NotFound), got {other:?}"),
        }
    }

    #[test]
    fn test_apply_delete_user_credential() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);

        // Create two credentials so deletion of one is allowed
        let cred1 = create_passkey_credential(&store, &mut state, user_id);
        create_passkey_credential_with_seed(&store, &mut state, user_id, 7);

        // Delete first credential
        let (response, _) = store.apply_system(
            &SystemRequest::DeleteUserCredential { user_id, credential_id: cred1 },
            &mut state,
        );

        match response {
            LedgerResponse::UserCredentialDeleted { credential_id } => {
                assert_eq!(credential_id, cred1);
            },
            other => panic!("expected UserCredentialDeleted, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_delete_last_credential_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);
        let cred_id = create_passkey_credential(&store, &mut state, user_id);

        // Attempt to delete the only credential
        let (response, _) = store.apply_system(
            &SystemRequest::DeleteUserCredential { user_id, credential_id: cred_id },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::FailedPrecondition);
            },
            other => panic!("expected Error(FailedPrecondition), got {other:?}"),
        }
    }

    #[test]
    fn test_apply_sequential_credential_ids() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);

        let id1 = create_passkey_credential(&store, &mut state, user_id);
        let id2 = create_passkey_credential_with_seed(&store, &mut state, user_id, 7);

        assert_eq!(id1, UserCredentialId::new(1));
        assert_eq!(id2, UserCredentialId::new(2));
    }

    #[test]
    fn test_apply_create_duplicate_recovery_code_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);

        // First recovery code set succeeds
        create_recovery_credential(&store, &mut state, user_id, vec![[0xAA; 32]]);

        // Second recovery code set rejected (one per user invariant)
        let (response, _) = store.apply_system(
            &SystemRequest::CreateUserCredential {
                user_id,
                credential_type: CredentialType::RecoveryCode,
                credential_data: CredentialData::RecoveryCode(RecoveryCodeCredential {
                    code_hashes: vec![[0xBB; 32]],
                    total_generated: 1,
                }),
                name: "Recovery codes 2".to_string(),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::AlreadyExists);
            },
            other => panic!("expected Error(AlreadyExists), got {other:?}"),
        }
    }

    #[test]
    fn test_apply_create_duplicate_passkey_credential_id_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);

        // First passkey succeeds
        create_passkey_credential(&store, &mut state, user_id);

        // Second passkey with same credential_id bytes rejected
        let (response, _) = store.apply_system(
            &SystemRequest::CreateUserCredential {
                user_id,
                credential_type: CredentialType::Passkey,
                credential_data: CredentialData::Passkey(PasskeyCredential {
                    credential_id: vec![1, 2, 3], // Same as seed=1 helper
                    public_key: vec![99, 99, 99], // Different key, same credential_id
                    sign_count: 0,
                    transports: vec![],
                    backup_eligible: false,
                    backup_state: false,
                    attestation_format: None,
                    aaguid: None,
                }),
                name: "Duplicate passkey".to_string(),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::AlreadyExists);
            },
            other => panic!("expected Error(AlreadyExists), got {other:?}"),
        }
    }

    // =========================================================================
    // TOTP Challenge Lifecycle apply handler tests
    // =========================================================================

    #[test]
    fn test_apply_create_totp_challenge() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);
        let nonce = [0xAA; 32];
        let expires_at = Utc::now() + Duration::minutes(5);

        create_test_totp_challenge(
            &store,
            &mut state,
            user_id,
            UserSlug::new(100),
            nonce,
            expires_at,
        );
    }

    #[test]
    fn test_apply_create_totp_challenge_rate_limited() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);
        let expires_at = Utc::now() + Duration::minutes(5);

        // Create 3 challenges (max allowed)
        for i in 0..3u8 {
            let mut nonce = [0u8; 32];
            nonce[0] = i;
            create_test_totp_challenge(
                &store,
                &mut state,
                user_id,
                UserSlug::new(100),
                nonce,
                expires_at,
            );
        }

        // 4th challenge should be rate limited
        let mut nonce_4 = [0u8; 32];
        nonce_4[0] = 3;
        let (response, _) = store.apply_system(
            &SystemRequest::CreateTotpChallenge {
                user_id,
                user_slug: UserSlug::new(100),
                nonce: nonce_4,
                expires_at,
                primary_method: PrimaryAuthMethod::EmailCode,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::RateLimited);
            },
            other => panic!("expected Error(RateLimited), got {other:?}"),
        }
    }

    #[test]
    fn test_apply_consume_totp_and_create_session() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(100);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        let nonce = [0xAA; 32];
        let expires_at = fixed_timestamp() + Duration::minutes(5);
        create_test_totp_challenge(&store, &mut state, user_id, user_slug, nonce, expires_at);

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::ConsumeTotpAndCreateSession {
                user_id,
                nonce,
                token_hash: [0xCC; 32],
                family: [0xDD; 16],
                kid: "key-001".to_string(),
                ttl_secs: 3600,
            },
            &mut state,
        );

        match response {
            LedgerResponse::TotpVerified { refresh_token_id } => {
                assert_eq!(refresh_token_id.value(), 1, "first refresh token gets ID 1");
            },
            other => panic!("expected TotpVerified, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_consume_totp_expired_challenge() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(100);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        let nonce = [0xAA; 32];
        // Challenge expires at fixed_timestamp (2025-01-15 12:00:00)
        let expires_at = fixed_timestamp();
        create_test_totp_challenge(&store, &mut state, user_id, user_slug, nonce, expires_at);

        // Apply at block_timestamp == expires_at (expired: uses <=)
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::ConsumeTotpAndCreateSession {
                user_id,
                nonce,
                token_hash: [0xCC; 32],
                family: [0xDD; 16],
                kid: "key-001".to_string(),
                ttl_secs: 3600,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::FailedPrecondition);
            },
            other => {
                panic!("expected Error(FailedPrecondition) for expired challenge, got {other:?}")
            },
        }
    }

    #[test]
    fn test_apply_consume_totp_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);

        // Try to consume a non-existent challenge
        let (response, _) = store.apply_system(
            &SystemRequest::ConsumeTotpAndCreateSession {
                user_id,
                nonce: [0xFF; 32],
                token_hash: [0xCC; 32],
                family: [0xDD; 16],
                kid: "key-001".to_string(),
                ttl_secs: 3600,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::NotFound);
            },
            other => panic!("expected Error(NotFound), got {other:?}"),
        }
    }

    #[test]
    fn test_apply_consume_totp_max_attempts_exceeded() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(100);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        let nonce = [0xAA; 32];
        let expires_at = fixed_timestamp() + Duration::minutes(5);
        create_test_totp_challenge(&store, &mut state, user_id, user_slug, nonce, expires_at);

        // Increment 3 times (0→1, 1→2, 2→3). Check >= 3 is BEFORE increment,
        // so all 3 succeed. The 4th would be rejected.
        for expected in 1..=3u8 {
            let (response, _) = store
                .apply_system(&SystemRequest::IncrementTotpAttempt { user_id, nonce }, &mut state);
            match response {
                LedgerResponse::TotpAttemptIncremented { attempts } => {
                    assert_eq!(attempts, expected);
                },
                other => panic!("expected TotpAttemptIncremented({expected}), got {other:?}"),
            }
        }

        // Consumption should be rejected (defense-in-depth: attempts >= 3)
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::ConsumeTotpAndCreateSession {
                user_id,
                nonce,
                token_hash: [0xCC; 32],
                family: [0xDD; 16],
                kid: "key-001".to_string(),
                ttl_secs: 3600,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::TooManyAttempts);
            },
            other => panic!("expected Error(TooManyAttempts), got {other:?}"),
        }
    }

    #[test]
    fn test_apply_increment_totp_attempt() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(100);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        let nonce = [0xAA; 32];
        let expires_at = Utc::now() + Duration::minutes(5);
        create_test_totp_challenge(&store, &mut state, user_id, user_slug, nonce, expires_at);

        // First increment: 0 → 1
        let (response, _) =
            store.apply_system(&SystemRequest::IncrementTotpAttempt { user_id, nonce }, &mut state);
        match response {
            LedgerResponse::TotpAttemptIncremented { attempts } => {
                assert_eq!(attempts, 1);
            },
            other => panic!("expected TotpAttemptIncremented, got {other:?}"),
        }

        // Second increment: 1 → 2
        let (response, _) =
            store.apply_system(&SystemRequest::IncrementTotpAttempt { user_id, nonce }, &mut state);
        match response {
            LedgerResponse::TotpAttemptIncremented { attempts } => {
                assert_eq!(attempts, 2);
            },
            other => panic!("expected TotpAttemptIncremented, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_increment_totp_attempt_max_reached() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(100);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        let nonce = [0xAA; 32];
        let expires_at = Utc::now() + Duration::minutes(5);
        create_test_totp_challenge(&store, &mut state, user_id, user_slug, nonce, expires_at);

        // Increment 3 times (0→1, 1→2, 2→3) — all succeed because check is >= 3 BEFORE increment
        for _ in 0..3 {
            store.apply_system(&SystemRequest::IncrementTotpAttempt { user_id, nonce }, &mut state);
        }

        // 4th increment: attempts == 3 >= MAX_TOTP_ATTEMPTS → rejected
        let (response, _) =
            store.apply_system(&SystemRequest::IncrementTotpAttempt { user_id, nonce }, &mut state);

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::TooManyAttempts);
            },
            other => panic!("expected TooManyAttempts on 4th attempt, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_increment_totp_attempt_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = create_test_user(&store, &mut state, UserSlug::new(100), Region::US_EAST_VA);

        let (response, _) = store.apply_system(
            &SystemRequest::IncrementTotpAttempt { user_id, nonce: [0xFF; 32] },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::NotFound);
            },
            other => panic!("expected Error(NotFound), got {other:?}"),
        }
    }

    // =========================================================================
    // Recovery Code Consumption + Direct Session Creation
    // =========================================================================

    #[test]
    fn test_apply_consume_recovery_and_create_session() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(100);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        let code_hash = [0xEE; 32];
        let recovery_cred_id =
            create_recovery_credential(&store, &mut state, user_id, vec![code_hash, [0xFF; 32]]);

        // Create a TOTP challenge (required for recovery code consumption)
        let nonce = [0xAA; 32];
        let expires_at = fixed_timestamp() + Duration::minutes(5);
        create_test_totp_challenge(&store, &mut state, user_id, user_slug, nonce, expires_at);

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::ConsumeRecoveryAndCreateSession {
                user_id,
                nonce,
                code_hash,
                credential_id: recovery_cred_id,
                token_hash: [0xCC; 32],
                family: [0xDD; 16],
                kid: "key-001".to_string(),
                ttl_secs: 3600,
            },
            &mut state,
        );

        match response {
            LedgerResponse::RecoveryCodeConsumed { refresh_token_id, remaining_codes } => {
                assert_eq!(refresh_token_id.value(), 1);
                assert_eq!(remaining_codes, 1, "one of two codes consumed");
            },
            other => panic!("expected RecoveryCodeConsumed, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_consume_recovery_wrong_code() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(100);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        let recovery_cred_id =
            create_recovery_credential(&store, &mut state, user_id, vec![[0xEE; 32]]);

        let nonce = [0xAA; 32];
        let expires_at = fixed_timestamp() + Duration::minutes(5);
        create_test_totp_challenge(&store, &mut state, user_id, user_slug, nonce, expires_at);

        // Try with wrong code hash
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::ConsumeRecoveryAndCreateSession {
                user_id,
                nonce,
                code_hash: [0x00; 32], // Wrong hash!
                credential_id: recovery_cred_id,
                token_hash: [0xCC; 32],
                family: [0xDD; 16],
                kid: "key-001".to_string(),
                ttl_secs: 3600,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, message } => {
                assert_eq!(code, ErrorCode::Unauthenticated);
                // Verify generic error message (no entity detail)
                assert_eq!(message, "Verification failed");
            },
            other => panic!("expected Error(Unauthenticated), got {other:?}"),
        }
    }

    #[test]
    fn test_apply_consume_recovery_expired_challenge() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(100);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        let recovery_cred_id =
            create_recovery_credential(&store, &mut state, user_id, vec![[0xEE; 32]]);

        // Challenge expires at fixed_timestamp
        let nonce = [0xAA; 32];
        let expires_at = fixed_timestamp();
        create_test_totp_challenge(&store, &mut state, user_id, user_slug, nonce, expires_at);

        // Consume at fixed_timestamp == expires_at (expired: <=)
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::ConsumeRecoveryAndCreateSession {
                user_id,
                nonce,
                code_hash: [0xEE; 32],
                credential_id: recovery_cred_id,
                token_hash: [0xCC; 32],
                family: [0xDD; 16],
                kid: "key-001".to_string(),
                ttl_secs: 3600,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::FailedPrecondition);
            },
            other => panic!("expected Error(FailedPrecondition), got {other:?}"),
        }
    }

    #[test]
    fn test_apply_consume_totp_challenge_one_time_use() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(100);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        let nonce = [0xAA; 32];
        let expires_at = fixed_timestamp() + Duration::minutes(5);
        create_test_totp_challenge(&store, &mut state, user_id, user_slug, nonce, expires_at);

        // First consumption succeeds
        let r1 = apply_at_fixed_time(
            &store,
            &SystemRequest::ConsumeTotpAndCreateSession {
                user_id,
                nonce,
                token_hash: [0xCC; 32],
                family: [0xDD; 16],
                kid: "key-001".to_string(),
                ttl_secs: 3600,
            },
            &mut state,
        );
        assert!(matches!(r1, LedgerResponse::TotpVerified { .. }));

        // Second consumption with same nonce fails (challenge deleted)
        let r2 = apply_at_fixed_time(
            &store,
            &SystemRequest::ConsumeTotpAndCreateSession {
                user_id,
                nonce,
                token_hash: [0xEE; 32],
                family: [0xFF; 16],
                kid: "key-002".to_string(),
                ttl_secs: 3600,
            },
            &mut state,
        );
        match r2 {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::NotFound);
            },
            other => panic!("expected Error(NotFound) for replayed nonce, got {other:?}"),
        }
    }

    // ========================================================================
    // AddOrganizationMember Tests
    // ========================================================================

    /// Creates a store with state layer configured (required for member operations).
    fn create_store_with_state_layer(dir: &tempfile::TempDir) -> RaftLogStore<FileBackend> {
        let state_db = Arc::new(
            inferadb_ledger_store::Database::create(dir.path().join("state.db"))
                .expect("create state db"),
        );
        let state_layer = Arc::new(inferadb_ledger_state::StateLayer::new(state_db));
        RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
            .expect("open store")
            .with_state_layer(state_layer)
    }

    #[tokio::test]
    async fn test_add_organization_member() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let new_user = UserId::new(42);
        let new_user_slug = UserSlug::new(4200);

        let request = OrganizationRequest::AddOrganizationMember {
            organization: org_id,
            user: new_user,
            user_slug: new_user_slug,
            role: OrganizationMemberRole::Member,
        };
        let (response, _) = store.apply_org(&request, &mut state);

        match response {
            LedgerResponse::OrganizationMemberAdded { organization_id, already_member } => {
                assert_eq!(organization_id, org_id);
                assert!(!already_member);
            },
            other => panic!("expected OrganizationMemberAdded, got {other}"),
        }

        // Verify the member was added to the user→org index
        assert!(state.user_org_index.get(&new_user).is_some_and(|orgs| orgs.contains(&org_id)));
    }

    #[tokio::test]
    async fn test_add_organization_member_idempotent() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(101),
            Region::US_EAST_VA,
        );

        let new_user = UserId::new(43);
        let new_user_slug = UserSlug::new(4300);

        let request = OrganizationRequest::AddOrganizationMember {
            organization: org_id,
            user: new_user,
            user_slug: new_user_slug,
            role: OrganizationMemberRole::Member,
        };

        // First add
        let (response, _) = store.apply_org(&request, &mut state);
        assert!(matches!(
            response,
            LedgerResponse::OrganizationMemberAdded { already_member: false, .. }
        ));

        // Second add — idempotent, returns already_member=true
        let (response, _) = store.apply_org(&request, &mut state);
        match response {
            LedgerResponse::OrganizationMemberAdded { organization_id, already_member } => {
                assert_eq!(organization_id, org_id);
                assert!(already_member);
            },
            other => panic!("expected OrganizationMemberAdded, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_add_member_to_nonexistent_organization() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let request = OrganizationRequest::AddOrganizationMember {
            organization: OrganizationId::new(9999),
            user: UserId::new(1),
            user_slug: UserSlug::new(100),
            role: OrganizationMemberRole::Member,
        };
        let (response, _) = store.apply_org(&request, &mut state);

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::NotFound);
            },
            other => panic!("expected Error(NotFound), got {other}"),
        }
    }

    // ========================================================================
    // CreateOrganizationInvite / ResolveOrganizationInvite Tests
    // ========================================================================

    #[tokio::test]
    async fn test_create_organization_invite() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let request = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug: InviteSlug::new(9999),
            token_hash: [0xAB; 32],
            invitee_email_hmac: "deadbeef".to_string(),
            ttl_hours: 168,
        };
        let (response, _) = store.apply_org(&request, &mut state);

        match response {
            LedgerResponse::OrganizationInviteCreated { invite_id, invite_slug, expires_at } => {
                assert_eq!(invite_id, InviteId::new(1));
                assert_eq!(invite_slug, InviteSlug::new(9999));
                // expires_at should be roughly block_timestamp + 168 hours
                // block_timestamp is close to Utc::now() in tests
                let expected_duration = chrono::Duration::hours(168);
                let diff = expires_at - Utc::now();
                // Allow 10-second tolerance for test execution time
                assert!(
                    diff > expected_duration - chrono::Duration::seconds(10),
                    "expires_at too early: diff={diff}"
                );
            },
            other => panic!("expected OrganizationInviteCreated, got {other}"),
        }

        // Second invite should get InviteId(2)
        let request2 = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug: InviteSlug::new(8888),
            token_hash: [0xCD; 32],
            invitee_email_hmac: "cafe0123".to_string(),
            ttl_hours: 24,
        };
        let (response2, _) = store.apply_org(&request2, &mut state);

        match response2 {
            LedgerResponse::OrganizationInviteCreated { invite_id, .. } => {
                assert_eq!(invite_id, InviteId::new(2));
            },
            other => panic!("expected OrganizationInviteCreated, got {other}"),
        }
    }

    /// Retry of `CreateOrganizationInvite` with the same slug is idempotent:
    /// returns the existing `(invite_id, expires_at)` without allocating a
    /// new invite_id, advancing the invite sequence counter, or writing
    /// duplicate token_hash/email_hash index entries. Pairs with the
    /// client-side slug stabilisation in `sdk/src/ops/invitation.rs`.
    #[tokio::test]
    async fn test_create_organization_invite_is_idempotent_by_slug() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let slug = InviteSlug::new(4242);
        let token_hash_first = [0xAA; 32];
        let token_hash_retry = [0xBB; 32]; // different hash on retry (server regenerates)

        let request = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug,
            token_hash: token_hash_first,
            invitee_email_hmac: "abc123".to_string(),
            ttl_hours: 72,
        };
        let (response, _) = store.apply_org(&request, &mut state);
        let (first_invite_id, first_expires_at) = match response {
            LedgerResponse::OrganizationInviteCreated {
                invite_id,
                invite_slug,
                expires_at,
            } => {
                assert_eq!(invite_slug, slug);
                (invite_id, expires_at)
            },
            other => panic!("expected OrganizationInviteCreated, got {other}"),
        };

        let sequence_after_first = state.sequences.clone();

        // Retry with same slug but different token_hash + email_hmac
        // (simulating server regenerating them on client retry).
        let retry = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug,
            token_hash: token_hash_retry,
            invitee_email_hmac: "different".to_string(),
            ttl_hours: 72,
        };
        let (retry_response, _) = store.apply_org(&retry, &mut state);
        match retry_response {
            LedgerResponse::OrganizationInviteCreated {
                invite_id,
                invite_slug,
                expires_at,
            } => {
                assert_eq!(
                    invite_id, first_invite_id,
                    "retry must return the same invite_id (idempotent by slug)"
                );
                assert_eq!(invite_slug, slug);
                assert_eq!(
                    expires_at, first_expires_at,
                    "retry must return the same expires_at from the stored index entry"
                );
            },
            other => panic!("expected OrganizationInviteCreated on retry, got {other}"),
        }

        // Invite sequence counter NOT advanced — no new invite_id allocated.
        assert_eq!(
            state.sequences, sequence_after_first,
            "retry must not advance state.sequences.invite"
        );
    }

    #[tokio::test]
    async fn test_create_invite_nonexistent_org() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let request = OrganizationRequest::CreateOrganizationInvite {
            organization: OrganizationId::new(9999),
            slug: InviteSlug::new(100),
            token_hash: [0; 32],
            invitee_email_hmac: "hmac".to_string(),
            ttl_hours: 168,
        };
        let (response, _) = store.apply_org(&request, &mut state);

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::NotFound);
            },
            other => panic!("expected Error(NotFound), got {other}"),
        }
    }

    #[tokio::test]
    async fn test_resolve_organization_invite_accept() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create an invitation first
        let create_req = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug: InviteSlug::new(9999),
            token_hash: [0xAB; 32],
            invitee_email_hmac: "deadbeef".to_string(),
            ttl_hours: 168,
        };
        let (create_resp, _) = store.apply_org(&create_req, &mut state);
        let invite_id = match create_resp {
            LedgerResponse::OrganizationInviteCreated { invite_id, .. } => invite_id,
            other => panic!("expected OrganizationInviteCreated, got {other}"),
        };

        // Resolve it as Accepted
        let resolve_req = OrganizationRequest::ResolveOrganizationInvite {
            invite: invite_id,
            organization: org_id,
            status: InvitationStatus::Accepted,
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: [0xAB; 32],
        };
        let (response, _) = store.apply_org(&resolve_req, &mut state);

        match response {
            LedgerResponse::OrganizationInviteResolved { invite_id: resolved_id } => {
                assert_eq!(resolved_id, invite_id);
            },
            other => panic!("expected OrganizationInviteResolved, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_resolve_invite_cas_already_resolved() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create an invitation
        let create_req = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug: InviteSlug::new(9999),
            token_hash: [0xAB; 32],
            invitee_email_hmac: "deadbeef".to_string(),
            ttl_hours: 168,
        };
        let (create_resp, _) = store.apply_org(&create_req, &mut state);
        let invite_id = match create_resp {
            LedgerResponse::OrganizationInviteCreated { invite_id, .. } => invite_id,
            other => panic!("expected OrganizationInviteCreated, got {other}"),
        };

        // Resolve as Accepted (first time — succeeds)
        let resolve_req = OrganizationRequest::ResolveOrganizationInvite {
            invite: invite_id,
            organization: org_id,
            status: InvitationStatus::Accepted,
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: [0xAB; 32],
        };
        let (resp1, _) = store.apply_org(&resolve_req, &mut state);
        assert!(matches!(resp1, LedgerResponse::OrganizationInviteResolved { .. }));

        // Try to resolve again (CAS failure — already resolved)
        let resolve_again = OrganizationRequest::ResolveOrganizationInvite {
            invite: invite_id,
            organization: org_id,
            status: InvitationStatus::Revoked,
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: [0xAB; 32],
        };
        let (resp2, _) = store.apply_org(&resolve_again, &mut state);

        match resp2 {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::InvitationAlreadyResolved);
            },
            other => panic!("expected Error(InvitationAlreadyResolved), got {other}"),
        }
    }

    #[tokio::test]
    async fn test_resolve_invite_accept_after_revoke_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create invitation
        let create_req = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug: InviteSlug::new(9999),
            token_hash: [0xAB; 32],
            invitee_email_hmac: "deadbeef".to_string(),
            ttl_hours: 168,
        };
        let (create_resp, _) = store.apply_org(&create_req, &mut state);
        let invite_id = match create_resp {
            LedgerResponse::OrganizationInviteCreated { invite_id, .. } => invite_id,
            other => panic!("expected OrganizationInviteCreated, got {other}"),
        };

        // Revoke first
        let revoke_req = OrganizationRequest::ResolveOrganizationInvite {
            invite: invite_id,
            organization: org_id,
            status: InvitationStatus::Revoked,
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: [0xAB; 32],
        };
        let (resp1, _) = store.apply_org(&revoke_req, &mut state);
        assert!(matches!(resp1, LedgerResponse::OrganizationInviteResolved { .. }));

        // Try to accept after revoke — should fail
        let accept_req = OrganizationRequest::ResolveOrganizationInvite {
            invite: invite_id,
            organization: org_id,
            status: InvitationStatus::Accepted,
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: [0xAB; 32],
        };
        let (resp2, _) = store.apply_org(&accept_req, &mut state);

        match resp2 {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::InvitationAlreadyResolved);
            },
            other => panic!("expected Error(InvitationAlreadyResolved), got {other}"),
        }
    }

    #[tokio::test]
    async fn test_resolve_invite_expire_after_accept_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create invitation
        let create_req = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug: InviteSlug::new(9999),
            token_hash: [0xAB; 32],
            invitee_email_hmac: "deadbeef".to_string(),
            ttl_hours: 168,
        };
        let (create_resp, _) = store.apply_org(&create_req, &mut state);
        let invite_id = match create_resp {
            LedgerResponse::OrganizationInviteCreated { invite_id, .. } => invite_id,
            other => panic!("expected OrganizationInviteCreated, got {other}"),
        };

        // Accept first
        let accept_req = OrganizationRequest::ResolveOrganizationInvite {
            invite: invite_id,
            organization: org_id,
            status: InvitationStatus::Accepted,
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: [0xAB; 32],
        };
        let (resp1, _) = store.apply_org(&accept_req, &mut state);
        assert!(matches!(resp1, LedgerResponse::OrganizationInviteResolved { .. }));

        // Try to expire after accept — should fail (CAS rejects non-Pending)
        let expire_req = OrganizationRequest::ResolveOrganizationInvite {
            invite: invite_id,
            organization: org_id,
            status: InvitationStatus::Expired,
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: [0xAB; 32],
        };
        let (resp2, _) = store.apply_org(&expire_req, &mut state);

        match resp2 {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::InvitationAlreadyResolved);
            },
            other => panic!("expected Error(InvitationAlreadyResolved), got {other}"),
        }
    }

    #[tokio::test]
    async fn test_resolve_invite_decline_after_expire_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create invitation
        let create_req = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug: InviteSlug::new(9999),
            token_hash: [0xAB; 32],
            invitee_email_hmac: "deadbeef".to_string(),
            ttl_hours: 168,
        };
        let (create_resp, _) = store.apply_org(&create_req, &mut state);
        let invite_id = match create_resp {
            LedgerResponse::OrganizationInviteCreated { invite_id, .. } => invite_id,
            other => panic!("expected OrganizationInviteCreated, got {other}"),
        };

        // Expire first
        let expire_req = OrganizationRequest::ResolveOrganizationInvite {
            invite: invite_id,
            organization: org_id,
            status: InvitationStatus::Expired,
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: [0xAB; 32],
        };
        let (resp1, _) = store.apply_org(&expire_req, &mut state);
        assert!(matches!(resp1, LedgerResponse::OrganizationInviteResolved { .. }));

        // Try to decline after expire — should fail
        let decline_req = OrganizationRequest::ResolveOrganizationInvite {
            invite: invite_id,
            organization: org_id,
            status: InvitationStatus::Declined,
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: [0xAB; 32],
        };
        let (resp2, _) = store.apply_org(&decline_req, &mut state);

        match resp2 {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::InvitationAlreadyResolved);
            },
            other => panic!("expected Error(InvitationAlreadyResolved), got {other}"),
        }
    }

    #[tokio::test]
    async fn test_rehash_invite_email_index_replaces_old_with_new() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create an invitation (writes email hash index with "deadbeef" HMAC)
        let create_req = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug: InviteSlug::new(9999),
            token_hash: [0xAB; 32],
            invitee_email_hmac: "deadbeef".to_string(),
            ttl_hours: 168,
        };
        let (create_resp, _) = store.apply_org(&create_req, &mut state);
        let invite_id = match create_resp {
            LedgerResponse::OrganizationInviteCreated { invite_id, .. } => invite_id,
            other => panic!("expected OrganizationInviteCreated, got {other}"),
        };

        // Verify old HMAC index key exists
        let state_layer = store.state_layer.as_ref().unwrap();
        let old_key = inferadb_ledger_state::system::SystemKeys::invite_email_hash_index_key(
            "deadbeef", invite_id,
        );
        let old_entry = state_layer.get_entity(VaultId::new(0), old_key.as_bytes()).unwrap();
        assert!(old_entry.is_some(), "old HMAC index entry should exist");

        // Rehash: old_hmac="deadbeef" → new_hmac="cafebabe"
        let rehash_req = OrganizationRequest::RehashInviteEmailIndex {
            invite: invite_id,
            old_hmac: "deadbeef".to_string(),
            new_hmac: "cafebabe".to_string(),
            organization: org_id,
            status: InvitationStatus::Pending,
        };
        let (rehash_resp, _) = store.apply_org(&rehash_req, &mut state);
        assert!(
            matches!(rehash_resp, LedgerResponse::InviteEmailIndexRehashed { .. }),
            "expected InviteEmailIndexRehashed, got {rehash_resp}"
        );

        // Verify old HMAC index key is deleted
        let old_entry_after = state_layer.get_entity(VaultId::new(0), old_key.as_bytes()).unwrap();
        assert!(old_entry_after.is_none(), "old HMAC index entry should be deleted after rehash");

        // Verify new HMAC index key exists
        let new_key = inferadb_ledger_state::system::SystemKeys::invite_email_hash_index_key(
            "cafebabe", invite_id,
        );
        let new_entry = state_layer.get_entity(VaultId::new(0), new_key.as_bytes()).unwrap();
        assert!(new_entry.is_some(), "new HMAC index entry should exist after rehash");

        // Verify the new entry deserializes with correct org_id and status
        let decoded: inferadb_ledger_types::InviteEmailEntry =
            inferadb_ledger_types::decode(&new_entry.unwrap().value).unwrap();
        assert_eq!(decoded.organization, org_id);
        assert_eq!(decoded.status, InvitationStatus::Pending);
    }

    /// Tests the full acceptance partial-failure recovery scenario at the Raft level:
    /// 1. Create invitation → 2. Resolve as Accepted → 3. AddOrganizationMember
    /// If step 3 is retried (simulating partial failure), it should be idempotent.
    #[tokio::test]
    async fn test_accept_then_add_member_partial_failure_recovery() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create invitation
        let create_req = OrganizationRequest::CreateOrganizationInvite {
            organization: org_id,
            slug: InviteSlug::new(9999),
            token_hash: [0xAB; 32],
            invitee_email_hmac: "deadbeef".to_string(),
            ttl_hours: 168,
        };
        let (create_resp, _) = store.apply_org(&create_req, &mut state);
        let invite_id = match create_resp {
            LedgerResponse::OrganizationInviteCreated { invite_id, .. } => invite_id,
            other => panic!("expected OrganizationInviteCreated, got {other}"),
        };

        // Resolve as Accepted (simulates step 7 in accept flow succeeding)
        let resolve_req = OrganizationRequest::ResolveOrganizationInvite {
            invite: invite_id,
            organization: org_id,
            status: InvitationStatus::Accepted,
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: [0xAB; 32],
        };
        let (resolve_resp, _) = store.apply_org(&resolve_req, &mut state);
        assert!(matches!(resolve_resp, LedgerResponse::OrganizationInviteResolved { .. }));

        // Retry resolve — CAS rejects (already Accepted), simulating duplicate call
        let (retry_resolve, _) = store.apply_org(&resolve_req, &mut state);
        assert!(matches!(
            retry_resolve,
            LedgerResponse::Error { code: ErrorCode::InvitationAlreadyResolved, .. }
        ));

        // AddOrganizationMember — first time
        let add_req = OrganizationRequest::AddOrganizationMember {
            organization: org_id,
            user: UserId::new(42),
            user_slug: UserSlug::new(4200),
            role: inferadb_ledger_state::system::OrganizationMemberRole::Member,
        };
        let (add_resp, _) = store.apply_org(&add_req, &mut state);
        match &add_resp {
            LedgerResponse::OrganizationMemberAdded { already_member, .. } => {
                assert!(!already_member);
            },
            other => panic!("expected OrganizationMemberAdded, got {other}"),
        }

        // Retry AddOrganizationMember — idempotent (simulating partial failure recovery)
        let (add_retry, _) = store.apply_org(&add_req, &mut state);
        match &add_retry {
            LedgerResponse::OrganizationMemberAdded { already_member, .. } => {
                assert!(already_member, "retry should report already_member");
            },
            other => panic!("expected OrganizationMemberAdded, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_create_invite_proposed_at_based_expiry() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Test with different TTL values
        for ttl in [1, 24, 168, 720] {
            let request = OrganizationRequest::CreateOrganizationInvite {
                organization: org_id,
                slug: InviteSlug::new(1000 + u64::from(ttl)),
                token_hash: [ttl as u8; 32],
                invitee_email_hmac: format!("hmac_{ttl}"),
                ttl_hours: ttl,
            };
            let (response, _) = store.apply_org(&request, &mut state);

            match response {
                LedgerResponse::OrganizationInviteCreated { expires_at, .. } => {
                    let now = Utc::now();
                    let expected_min = now + chrono::Duration::hours(i64::from(ttl))
                        - chrono::Duration::seconds(10);
                    let expected_max = now
                        + chrono::Duration::hours(i64::from(ttl))
                        + chrono::Duration::seconds(10);
                    assert!(
                        expires_at >= expected_min && expires_at <= expected_max,
                        "expires_at={expires_at} not within expected range for ttl={ttl}h"
                    );
                },
                other => panic!("expected OrganizationInviteCreated, got {other}"),
            }
        }
    }

    // ========================================================================
    // Vault CRUD operations
    // ========================================================================

    #[tokio::test]
    async fn test_delete_vault_success() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create a vault first
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("doomed-vault".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault, .. } => vault,
            other => panic!("expected VaultCreated, got {other}"),
        };

        // Delete it
        let (response, _) = store.apply_org(
            &OrganizationRequest::DeleteVault { organization: org_id, vault: vault_id },
            &mut state,
        );

        match response {
            LedgerResponse::VaultDeleted { success } => assert!(success),
            other => panic!("expected VaultDeleted, got {other}"),
        }

        // Verify vault is marked as deleted in state
        let key = (org_id, vault_id);
        let vault_meta = state.vaults.get(&key).expect("vault meta exists");
        assert!(vault_meta.deleted, "vault should be marked deleted");
    }

    #[tokio::test]
    async fn test_delete_vault_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_org(
            &OrganizationRequest::DeleteVault {
                organization: OrganizationId::new(999),
                vault: VaultId::new(999),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, .. } => {},
            other => panic!("expected NotFound error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_update_vault_retention_policy() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create vault
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault, .. } => vault,
            other => panic!("expected VaultCreated, got {other}"),
        };

        // Update retention policy
        let new_policy = BlockRetentionPolicy {
            mode: inferadb_ledger_types::BlockRetentionMode::Compacted,
            retention_blocks: 100,
        };
        let (response, _) = store.apply_org(
            &OrganizationRequest::UpdateVault {
                organization: org_id,
                vault: vault_id,
                retention_policy: Some(new_policy),
            },
            &mut state,
        );

        match response {
            LedgerResponse::VaultUpdated { success } => assert!(success),
            other => panic!("expected VaultUpdated, got {other}"),
        }

        let key = (org_id, vault_id);
        let vault_meta = state.vaults.get(&key).expect("vault meta exists");
        assert_eq!(
            vault_meta.retention_policy.mode,
            inferadb_ledger_types::BlockRetentionMode::Compacted
        );
        assert_eq!(vault_meta.retention_policy.retention_blocks, 100);
    }

    #[tokio::test]
    async fn test_update_vault_no_fields_still_succeeds() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault, .. } => vault,
            other => panic!("expected VaultCreated, got {other}"),
        };

        // Update with no fields
        let (response, _) = store.apply_org(
            &OrganizationRequest::UpdateVault {
                organization: org_id,
                vault: vault_id,
                retention_policy: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::VaultUpdated { success } => assert!(success),
            other => panic!("expected VaultUpdated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_update_vault_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_org(
            &OrganizationRequest::UpdateVault {
                organization: OrganizationId::new(999),
                vault: VaultId::new(999),
                retention_policy: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, .. } => {},
            other => panic!("expected NotFound error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_update_deleted_vault_returns_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create and delete a vault
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault, .. } => vault,
            other => panic!("expected VaultCreated, got {other}"),
        };

        store.apply_org(
            &OrganizationRequest::DeleteVault { organization: org_id, vault: vault_id },
            &mut state,
        );

        // Try to update deleted vault
        let (response, _) = store.apply_org(
            &OrganizationRequest::UpdateVault {
                organization: org_id,
                vault: vault_id,
                retention_policy: Some(BlockRetentionPolicy::default()),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, message } => {
                assert!(message.contains("deleted"));
            },
            other => panic!("expected NotFound error for deleted vault, got {other}"),
        }
    }

    // ========================================================================
    // Team operations
    // ========================================================================

    #[tokio::test]
    async fn test_create_organization_team_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let team_slug = TeamSlug::new(500);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateOrganizationTeam { organization: org_id, slug: team_slug },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationTeamCreated { team_id, team_slug: slug } => {
                assert_eq!(team_id, TeamId::new(1));
                assert_eq!(slug, team_slug);
                // Verify slug index
                assert_eq!(state.team_slug_index.get(&team_slug), Some(&(org_id, team_id)));
                assert_eq!(state.team_id_to_slug.get(&team_id), Some(&team_slug));
            },
            other => panic!("expected OrganizationTeamCreated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_create_organization_team_duplicate_slug_cross_org() {
        // Cross-organization slug collision is still rejected with
        // `AlreadyExists` — only same-org retries are idempotent (covered by
        // `create_organization_team_is_idempotent_by_slug`).
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_a = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );
        let org_b = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(101),
            Region::US_EAST_VA,
        );

        let team_slug = TeamSlug::new(500);

        // Create first team in org A
        store.apply_org(
            &OrganizationRequest::CreateOrganizationTeam { organization: org_a, slug: team_slug },
            &mut state,
        );

        // Same slug in a different organization is rejected.
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateOrganizationTeam { organization: org_b, slug: team_slug },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::AlreadyExists, message } => {
                assert!(message.contains("already exists"));
            },
            other => panic!("expected AlreadyExists error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_delete_organization_team_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let team_slug = TeamSlug::new(500);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateOrganizationTeam { organization: org_id, slug: team_slug },
            &mut state,
        );
        let team_id = match response {
            LedgerResponse::OrganizationTeamCreated { team_id, .. } => team_id,
            other => panic!("expected OrganizationTeamCreated, got {other}"),
        };

        // Delete the team
        let (response, _) = store.apply_org(
            &OrganizationRequest::DeleteOrganizationTeam { organization: org_id, team: team_id },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationTeamDeleted { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationTeamDeleted, got {other}"),
        }

        // Verify slug index is cleaned up
        assert!(!state.team_slug_index.contains_key(&team_slug));
        assert!(!state.team_id_to_slug.contains_key(&team_id));
    }

    #[tokio::test]
    async fn test_delete_organization_team_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let (response, _) = store.apply_org(
            &OrganizationRequest::DeleteOrganizationTeam {
                organization: org_id,
                team: TeamId::new(999),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, .. } => {},
            other => panic!("expected NotFound error, got {other}"),
        }
    }

    // ========================================================================
    // App operations
    // ========================================================================

    #[tokio::test]
    async fn test_create_app_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );

        match response {
            LedgerResponse::AppCreated { app_id, app_slug: slug } => {
                assert_eq!(app_id, inferadb_ledger_types::AppId::new(1));
                assert_eq!(slug, app_slug);
                // Verify slug index
                assert_eq!(state.app_slug_index.get(&app_slug), Some(&(org_id, app_id)));
                assert_eq!(state.app_id_to_slug.get(&app_id), Some(&app_slug));
            },
            other => panic!("expected AppCreated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_create_app_duplicate_slug_cross_org() {
        // Cross-organization slug collision is still rejected with
        // `AlreadyExists` — only same-org retries are idempotent (covered by
        // `create_app_is_idempotent_by_slug`).
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_a = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );
        let org_b = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(101),
            Region::US_EAST_VA,
        );

        let app_slug = inferadb_ledger_types::AppSlug::new(600);

        // Create first app in org A
        store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_a, slug: app_slug },
            &mut state,
        );

        // Same slug in a different organization is rejected.
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_b, slug: app_slug },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::AlreadyExists, message } => {
                assert!(message.contains("already exists"));
            },
            other => panic!("expected AlreadyExists error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_delete_app_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        // Delete the app
        let (response, _) = store.apply_org(
            &OrganizationRequest::DeleteApp { organization: org_id, app: app_id },
            &mut state,
        );

        match response {
            LedgerResponse::AppDeleted { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected AppDeleted, got {other}"),
        }

        // Verify slug index is cleaned up
        assert!(!state.app_slug_index.contains_key(&app_slug));
        assert!(!state.app_id_to_slug.contains_key(&app_id));
    }

    #[tokio::test]
    async fn test_delete_app_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let (response, _) = store.apply_org(
            &OrganizationRequest::DeleteApp {
                organization: org_id,
                app: inferadb_ledger_types::AppId::new(999),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, .. } => {},
            other => panic!("expected NotFound error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_set_app_enabled() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        // Enable the app
        let (response, _) = store.apply_org(
            &OrganizationRequest::SetAppEnabled {
                organization: org_id,
                app: app_id,
                enabled: true,
            },
            &mut state,
        );

        match response {
            LedgerResponse::AppToggled { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected AppToggled, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_set_app_credential_enabled() {
        use inferadb_ledger_state::system::AppCredentialType;

        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        // Enable client_secret credential
        let (response, _) = store.apply_org(
            &OrganizationRequest::SetAppCredentialEnabled {
                organization: org_id,
                app: app_id,
                credential_type: AppCredentialType::ClientSecret,
                enabled: true,
            },
            &mut state,
        );

        match response {
            LedgerResponse::AppCredentialToggled { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected AppCredentialToggled, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_rotate_app_client_secret() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        let (response, _) = store.apply_org(
            &OrganizationRequest::RotateAppClientSecret {
                organization: org_id,
                app: app_id,
                new_secret_hash: "$2b$12$test_hash".to_string(),
            },
            &mut state,
        );

        match response {
            LedgerResponse::AppClientSecretRotated { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected AppClientSecretRotated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_create_app_client_assertion() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateAppClientAssertion {
                organization: org_id,
                app: app_id,
                expires_at: Utc::now() + chrono::Duration::days(365),
                public_key_bytes: vec![1u8; 32],
            },
            &mut state,
        );

        match response {
            LedgerResponse::AppClientAssertionCreated { assertion_id } => {
                assert_eq!(assertion_id, inferadb_ledger_types::ClientAssertionId::new(1));
            },
            other => panic!("expected AppClientAssertionCreated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_delete_app_client_assertion() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        // Create an assertion
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateAppClientAssertion {
                organization: org_id,
                app: app_id,
                expires_at: Utc::now() + chrono::Duration::days(365),
                public_key_bytes: vec![1u8; 32],
            },
            &mut state,
        );
        let assertion_id = match response {
            LedgerResponse::AppClientAssertionCreated { assertion_id } => assertion_id,
            other => panic!("expected AppClientAssertionCreated, got {other}"),
        };

        // Delete it
        let (response, _) = store.apply_org(
            &OrganizationRequest::DeleteAppClientAssertion {
                organization: org_id,
                app: app_id,
                assertion: assertion_id,
            },
            &mut state,
        );

        match response {
            LedgerResponse::AppClientAssertionDeleted { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected AppClientAssertionDeleted, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_set_app_client_assertion_enabled() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        // Create an assertion
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateAppClientAssertion {
                organization: org_id,
                app: app_id,
                expires_at: Utc::now() + chrono::Duration::days(365),
                public_key_bytes: vec![1u8; 32],
            },
            &mut state,
        );
        let assertion_id = match response {
            LedgerResponse::AppClientAssertionCreated { assertion_id } => assertion_id,
            other => panic!("expected AppClientAssertionCreated, got {other}"),
        };

        // Toggle enabled
        let (response, _) = store.apply_org(
            &OrganizationRequest::SetAppClientAssertionEnabled {
                organization: org_id,
                app: app_id,
                assertion: assertion_id,
                enabled: true,
            },
            &mut state,
        );

        match response {
            LedgerResponse::AppClientAssertionToggled { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected AppClientAssertionToggled, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_add_app_vault_connection() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create vault
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault, .. } => vault,
            other => panic!("expected VaultCreated, got {other}"),
        };

        // Create app
        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        // Add vault connection
        let (response, _) = store.apply_org(
            &OrganizationRequest::AddAppVault {
                organization: org_id,
                app: app_id,
                vault: vault_id,
                vault_slug: VaultSlug::new(1),
                allowed_scopes: vec!["read".to_string(), "write".to_string()],
            },
            &mut state,
        );

        match response {
            LedgerResponse::AppVaultAdded { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected AppVaultAdded, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_add_app_vault_duplicate() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create vault
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault, .. } => vault,
            other => panic!("expected VaultCreated, got {other}"),
        };

        // Create app
        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        // Add vault connection
        store.apply_org(
            &OrganizationRequest::AddAppVault {
                organization: org_id,
                app: app_id,
                vault: vault_id,
                vault_slug: VaultSlug::new(1),
                allowed_scopes: vec!["read".to_string()],
            },
            &mut state,
        );

        // Try duplicate connection
        let (response, _) = store.apply_org(
            &OrganizationRequest::AddAppVault {
                organization: org_id,
                app: app_id,
                vault: vault_id,
                vault_slug: VaultSlug::new(1),
                allowed_scopes: vec!["read".to_string()],
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::AlreadyExists, .. } => {},
            other => panic!("expected AlreadyExists error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_remove_app_vault_connection() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create vault
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault, .. } => vault,
            other => panic!("expected VaultCreated, got {other}"),
        };

        // Create app
        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        // Add vault connection
        store.apply_org(
            &OrganizationRequest::AddAppVault {
                organization: org_id,
                app: app_id,
                vault: vault_id,
                vault_slug: VaultSlug::new(1),
                allowed_scopes: vec!["read".to_string()],
            },
            &mut state,
        );

        // Remove it
        let (response, _) = store.apply_org(
            &OrganizationRequest::RemoveAppVault {
                organization: org_id,
                app: app_id,
                vault: vault_id,
            },
            &mut state,
        );

        match response {
            LedgerResponse::AppVaultRemoved { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected AppVaultRemoved, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_update_app_vault_scopes() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create vault
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault, .. } => vault,
            other => panic!("expected VaultCreated, got {other}"),
        };

        // Create app
        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        // Add vault connection
        store.apply_org(
            &OrganizationRequest::AddAppVault {
                organization: org_id,
                app: app_id,
                vault: vault_id,
                vault_slug: VaultSlug::new(1),
                allowed_scopes: vec!["read".to_string()],
            },
            &mut state,
        );

        // Update scopes
        let (response, _) = store.apply_org(
            &OrganizationRequest::UpdateAppVault {
                organization: org_id,
                app: app_id,
                vault: vault_id,
                allowed_scopes: vec!["read".to_string(), "write".to_string(), "admin".to_string()],
            },
            &mut state,
        );

        match response {
            LedgerResponse::AppVaultUpdated { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected AppVaultUpdated, got {other}"),
        }
    }

    // ========================================================================
    // Signing key operations
    // ========================================================================

    #[tokio::test]
    async fn test_create_signing_key_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "test-kid-1".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        match response {
            LedgerResponse::SigningKeyCreated { id, kid } => {
                assert_eq!(id, inferadb_ledger_types::SigningKeyId::new(1));
                assert_eq!(kid, "test-kid-1");
            },
            other => panic!("expected SigningKeyCreated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_create_signing_key_idempotent() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        // Create initial key
        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "test-kid-1".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        // Same kid => idempotent return
        let (response, _) = store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "test-kid-1".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        match response {
            LedgerResponse::SigningKeyCreated { kid, .. } => {
                assert_eq!(kid, "test-kid-1");
            },
            other => panic!("expected idempotent SigningKeyCreated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_create_signing_key_active_exists_fails() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        // Create initial key
        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "test-kid-1".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        // Different kid, same scope => fails (active key exists)
        let (response, _) = store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "test-kid-2".to_string(),
                public_key_bytes: vec![3u8; 32],
                encrypted_private_key: vec![4u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("Active signing key already exists"));
            },
            other => panic!("expected FailedPrecondition error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_rotate_signing_key_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        // Create initial key
        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "old-kid".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        // Rotate
        let (response, _) = store.apply_system(
            &SystemRequest::RotateSigningKey {
                old_kid: "old-kid".to_string(),
                new_kid: "new-kid".to_string(),
                new_public_key_bytes: vec![3u8; 32],
                new_encrypted_private_key: vec![4u8; 100],
                rmk_version: 1,
                grace_period_secs: 3600,
            },
            &mut state,
        );

        match response {
            LedgerResponse::SigningKeyRotated { old_kid, new_kid } => {
                assert_eq!(old_kid, "old-kid");
                assert_eq!(new_kid, "new-kid");
            },
            other => panic!("expected SigningKeyRotated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_rotate_signing_key_immediate_revocation() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "old-kid".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        // Rotate with grace_period_secs = 0 (immediate revocation)
        let (response, _) = store.apply_system(
            &SystemRequest::RotateSigningKey {
                old_kid: "old-kid".to_string(),
                new_kid: "new-kid".to_string(),
                new_public_key_bytes: vec![3u8; 32],
                new_encrypted_private_key: vec![4u8; 100],
                rmk_version: 1,
                grace_period_secs: 0,
            },
            &mut state,
        );

        match response {
            LedgerResponse::SigningKeyRotated { old_kid, new_kid } => {
                assert_eq!(old_kid, "old-kid");
                assert_eq!(new_kid, "new-kid");
            },
            other => panic!("expected SigningKeyRotated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_revoke_signing_key() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "test-kid".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        let (response, _) = store.apply_system(
            &SystemRequest::RevokeSigningKey { kid: "test-kid".to_string() },
            &mut state,
        );

        match response {
            LedgerResponse::SigningKeyRevoked { kid } => {
                assert_eq!(kid, "test-kid");
            },
            other => panic!("expected SigningKeyRevoked, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_revoke_signing_key_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_system(
            &SystemRequest::RevokeSigningKey { kid: "nonexistent".to_string() },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::NotFound, .. } => {},
            other => panic!("expected NotFound error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_transition_signing_key_revoked() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        // Create and rotate (with grace period) to get a Rotated key
        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "old-kid".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );
        store.apply_system(
            &SystemRequest::RotateSigningKey {
                old_kid: "old-kid".to_string(),
                new_kid: "new-kid".to_string(),
                new_public_key_bytes: vec![3u8; 32],
                new_encrypted_private_key: vec![4u8; 100],
                rmk_version: 1,
                grace_period_secs: 3600,
            },
            &mut state,
        );

        // Transition rotated key to revoked
        let (response, _) = store.apply_system(
            &SystemRequest::TransitionSigningKeyRevoked { kid: "old-kid".to_string() },
            &mut state,
        );

        match response {
            LedgerResponse::SigningKeyTransitioned { kid } => {
                assert_eq!(kid, "old-kid");
            },
            other => panic!("expected SigningKeyTransitioned, got {other}"),
        }
    }

    // ========================================================================
    // Refresh token operations
    // ========================================================================

    #[tokio::test]
    async fn test_create_refresh_token() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        // Need a signing key for token creation
        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "signing-kid".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        let (response, _) = store.apply_system(
            &SystemRequest::CreateRefreshToken {
                token_hash: [0xAA; 32],
                family: [0xBB; 16],
                token_type: inferadb_ledger_types::TokenType::UserSession,
                subject: TokenSubject::User(UserSlug::new(42)),
                organization: None,
                vault: None,
                kid: "signing-kid".to_string(),
                ttl_secs: 86400,
            },
            &mut state,
        );

        match response {
            LedgerResponse::RefreshTokenCreated { id } => {
                assert_eq!(id, inferadb_ledger_types::RefreshTokenId::new(1));
            },
            other => panic!("expected RefreshTokenCreated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_revoke_all_user_sessions_no_state_layer() {
        // Without a state layer, this falls through to the else branch
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_system(
            &SystemRequest::RevokeAllUserSessions { user: UserId::new(42) },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::Internal, message } => {
                assert!(message.contains("State layer not available"));
            },
            other => panic!("expected Internal error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_revoke_all_app_sessions() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create app
        let app_slug = inferadb_ledger_types::AppSlug::new(600);
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateApp { organization: org_id, slug: app_slug },
            &mut state,
        );
        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            other => panic!("expected AppCreated, got {other}"),
        };

        let (response, _) = store.apply_system(
            &SystemRequest::RevokeAllAppSessions { organization: org_id, app: app_id },
            &mut state,
        );

        match response {
            LedgerResponse::AllAppSessionsRevoked { count, version } => {
                assert_eq!(count, 0);
                assert_eq!(version, inferadb_ledger_types::TokenVersion::new(1));
            },
            other => panic!("expected AllAppSessionsRevoked, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_revoke_token_family() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store
            .apply_system(&SystemRequest::RevokeTokenFamily { family: [0xBB; 16] }, &mut state);

        match response {
            LedgerResponse::TokenFamilyRevoked { count } => {
                // No tokens in that family
                assert_eq!(count, 0);
            },
            other => panic!("expected TokenFamilyRevoked, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_delete_expired_refresh_tokens() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) =
            store.apply_system(&SystemRequest::DeleteExpiredRefreshTokens, &mut state);

        match response {
            LedgerResponse::ExpiredRefreshTokensDeleted { count } => {
                assert_eq!(count, 0);
            },
            other => panic!("expected ExpiredRefreshTokensDeleted, got {other}"),
        }
    }

    // ========================================================================
    // System request: User operations
    // ========================================================================

    #[tokio::test]
    async fn test_create_user() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = UserId::new(42);
        let user_slug = UserSlug::new(4200);
        let (response, _) = store.apply_system(
            &SystemRequest::CreateUser {
                user: user_id,
                slug: user_slug,
                admin: false,
                region: Region::US_EAST_VA,
            },
            &mut state,
        );

        match response {
            LedgerResponse::UserCreated { user_id: uid, slug } => {
                assert_eq!(uid, user_id);
                assert_eq!(slug, user_slug);
            },
            other => panic!("expected UserCreated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_create_user_updates_slug_indexes() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = UserId::new(42);
        let user_slug = UserSlug::new(4200);
        store.apply_system(
            &SystemRequest::CreateUser {
                user: user_id,
                slug: user_slug,
                admin: false,
                region: Region::US_EAST_VA,
            },
            &mut state,
        );

        // Verify slug index was populated
        assert_eq!(state.user_slug_index.get(&user_slug), Some(&user_id));
        assert_eq!(state.user_id_to_slug.get(&user_id), Some(&user_slug));
    }

    #[tokio::test]
    async fn test_update_user_without_state_layer_returns_empty() {
        // Without a state layer, UpdateUser degrades to Empty
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_system(
            &SystemRequest::UpdateUser {
                user_id: UserId::new(42),
                role: Some(inferadb_ledger_types::UserRole::Admin),
                primary_email: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Empty => {},
            other => panic!("expected Empty (no state layer), got {other}"),
        }
    }

    #[tokio::test]
    async fn test_delete_user_without_state_layer_returns_empty() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) =
            store.apply_system(&SystemRequest::DeleteUser { user_id: UserId::new(42) }, &mut state);

        match response {
            LedgerResponse::Empty => {},
            other => panic!("expected Empty (no state layer), got {other}"),
        }
    }

    // ========================================================================
    // System request: email hash, blinding key, directory status
    // ========================================================================

    #[tokio::test]
    async fn test_register_email_hash() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = UserId::new(42);
        store.apply_system(
            &SystemRequest::CreateUser {
                user: user_id,
                slug: UserSlug::new(4200),
                admin: false,
                region: Region::US_EAST_VA,
            },
            &mut state,
        );

        let (response, _) = store.apply_system(
            &SystemRequest::RegisterEmailHash { hmac_hex: "abcdef0123456789".to_string(), user_id },
            &mut state,
        );

        // RegisterEmailHash returns Empty on success
        match response {
            LedgerResponse::Empty => {},
            LedgerResponse::Error { code, message } => {
                panic!("expected Empty, got Error({code:?}): {message}");
            },
            other => panic!("expected Empty, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_remove_email_hash() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let user_id = UserId::new(42);
        store.apply_system(
            &SystemRequest::CreateUser {
                user: user_id,
                slug: UserSlug::new(4200),
                admin: false,
                region: Region::US_EAST_VA,
            },
            &mut state,
        );

        // Register first
        store.apply_system(
            &SystemRequest::RegisterEmailHash { hmac_hex: "abcdef0123456789".to_string(), user_id },
            &mut state,
        );

        // Remove
        let (response, _) = store.apply_system(
            &SystemRequest::RemoveEmailHash { hmac_hex: "abcdef0123456789".to_string() },
            &mut state,
        );

        match response {
            LedgerResponse::Empty => {},
            other => panic!("expected Empty, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_set_blinding_key_version() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) =
            store.apply_system(&SystemRequest::SetBlindingKeyVersion { version: 2 }, &mut state);

        // State is stored in the state layer (sys_service), not in AppliedState
        match response {
            LedgerResponse::Empty => {},
            other => panic!("expected Empty, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_update_rehash_progress() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_system(
            &SystemRequest::UpdateRehashProgress {
                region: Region::US_EAST_VA,
                entries_rehashed: 500,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Empty => {},
            other => panic!("expected Empty, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_clear_rehash_progress() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        // Set progress first
        store.apply_system(
            &SystemRequest::UpdateRehashProgress {
                region: Region::US_EAST_VA,
                entries_rehashed: 500,
            },
            &mut state,
        );

        // Clear it
        let (response, _) = store.apply_system(
            &SystemRequest::ClearRehashProgress { region: Region::US_EAST_VA },
            &mut state,
        );

        match response {
            LedgerResponse::Empty => {},
            other => panic!("expected Empty, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_update_organization_routing() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let (response, _) = store.apply_system(
            &SystemRequest::UpdateOrganizationRouting {
                organization: org_id,
                region: Region::IE_EAST_DUBLIN,
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationMigrated { organization, old_region, new_region } => {
                assert_eq!(organization, org_id);
                assert_eq!(old_region, Region::US_EAST_VA);
                assert_eq!(new_region, Region::IE_EAST_DUBLIN);
            },
            other => panic!("expected OrganizationMigrated, got {other}"),
        }
    }

    // ========================================================================
    // Organization status operations
    // ========================================================================

    #[tokio::test]
    async fn test_update_organization_status() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Verify it starts active (create_active_organization sets Active)
        let org_meta = state.organizations.get(&org_id).expect("org exists");
        assert_eq!(org_meta.status, OrganizationStatus::Active);

        // Transition to Suspended via status update
        let (response, _) = store.apply_system(
            &SystemRequest::UpdateOrganizationStatus {
                organization: org_id,
                status: OrganizationStatus::Suspended,
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationStatusUpdated { organization_id } => {
                assert_eq!(organization_id, org_id);
                let updated_meta = state.organizations.get(&org_id).expect("org exists");
                assert_eq!(updated_meta.status, OrganizationStatus::Suspended);
            },
            other => panic!("expected OrganizationStatusUpdated, got {other}"),
        }
    }

    // ========================================================================
    // BatchWrite
    // ========================================================================

    #[tokio::test]
    async fn test_batch_write_multiple_vault_operations() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Batch: create two vaults
        let requests = vec![
            OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("vault-1".to_string()),
                retention_policy: None,
            },
            OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(2),
                name: Some("vault-2".to_string()),
                retention_policy: None,
            },
        ];

        let (response, _) =
            store.apply_org(&OrganizationRequest::BatchWrite { requests }, &mut state);

        match response {
            LedgerResponse::BatchWrite { responses } => {
                assert_eq!(responses.len(), 2);
                assert!(matches!(
                    &responses[0],
                    LedgerResponse::VaultCreated { vault, .. } if *vault == VaultId::new(1)
                ));
                assert!(matches!(
                    &responses[1],
                    LedgerResponse::VaultCreated { vault, .. } if *vault == VaultId::new(2)
                ));
            },
            other => panic!("expected BatchWrite, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_batch_write_empty() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, vault_entry) =
            store.apply_org(&OrganizationRequest::BatchWrite { requests: vec![] }, &mut state);

        match response {
            LedgerResponse::BatchWrite { responses } => {
                assert!(responses.is_empty());
            },
            other => panic!("expected BatchWrite, got {other}"),
        }
        assert!(vault_entry.is_none());
    }

    // ========================================================================
    // Write operations (entity writes)
    // ========================================================================

    #[tokio::test]
    // Obsolete under B.1.13: `OrganizationRequest::Write` no longer
    // carries an `organization:` field — a single `RaftLogStore`
    // owns writes for exactly one `OrganizationId`. Test rewrite
    // (one store per org) pending; three_tier_consensus integration
    // tests cover the new model end-to-end.
    #[ignore]
    async fn test_write_to_deleted_org_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Delete the org
        store.apply_system(&SystemRequest::DeleteOrganization { organization: org_id }, &mut state);

        // Try to write to deleted org
        let (response, _) = store.apply_org(
            &OrganizationRequest::Write {
                vault: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("deleted"));
            },
            other => panic!("expected FailedPrecondition error for deleted org, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_write_empty_transactions() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Create a vault
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault, .. } => vault,
            other => panic!("expected VaultCreated, got {other}"),
        };

        // Write with empty transactions
        let (response, vault_entry) = store.apply_org(
            &OrganizationRequest::Write {
                vault: vault_id,
                transactions: vec![],
                idempotency_key: [0u8; 16],
                request_hash: 0,
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                vault_slug: inferadb_ledger_types::VaultSlug::new(0),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Write { block_height, .. } => {
                assert_eq!(block_height, 1);
            },
            other => panic!("expected Write, got {other}"),
        }
        assert!(vault_entry.is_some(), "write should produce a vault entry");
    }

    // ========================================================================
    // Organization member operations
    // ========================================================================

    #[tokio::test]
    async fn test_update_organization_member_role() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Add a member as Member
        let new_user = UserId::new(42);
        store.apply_org(
            &OrganizationRequest::AddOrganizationMember {
                organization: org_id,
                user: new_user,
                user_slug: UserSlug::new(4200),
                role: OrganizationMemberRole::Member,
            },
            &mut state,
        );

        // Promote to Admin
        let (response, _) = store.apply_org(
            &OrganizationRequest::UpdateOrganizationMemberRole {
                organization: org_id,
                target: new_user,
                role: OrganizationMemberRole::Admin,
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationMemberRoleUpdated { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationMemberRoleUpdated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_remove_organization_member() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Add a second admin so we can remove one
        let second_admin = UserId::new(42);
        store.apply_org(
            &OrganizationRequest::AddOrganizationMember {
                organization: org_id,
                user: second_admin,
                user_slug: UserSlug::new(4200),
                role: OrganizationMemberRole::Admin,
            },
            &mut state,
        );

        // Remove the second admin
        let (response, _) = store.apply_org(
            &OrganizationRequest::RemoveOrganizationMember {
                organization: org_id,
                target: second_admin,
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationMemberRemoved { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationMemberRemoved, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_remove_last_admin_blocked() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // The admin user (UserId::new(1)) from create_active_organization
        let (response, _) = store.apply_org(
            &OrganizationRequest::RemoveOrganizationMember {
                organization: org_id,
                target: UserId::new(1),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code: ErrorCode::FailedPrecondition, message } => {
                assert!(message.contains("last administrator"));
            },
            other => panic!("expected FailedPrecondition error, got {other}"),
        }
    }

    // ========================================================================
    // PurgeOrganization
    // ========================================================================

    #[tokio::test]
    async fn test_purge_organization() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Delete the org first (purge requires Deleted/Deleting status)
        store.apply_system(&SystemRequest::DeleteOrganization { organization: org_id }, &mut state);

        // Purge
        let (response, _) = store
            .apply_system(&SystemRequest::PurgeOrganization { organization: org_id }, &mut state);

        match response {
            LedgerResponse::OrganizationPurged { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationPurged, got {other}"),
        }
    }

    // ========================================================================
    // UpdateUserDirectoryStatus
    // ========================================================================

    #[tokio::test]
    async fn test_update_user_directory_status_without_state_layer() {
        // Without a state layer, UpdateUserDirectoryStatus returns Empty
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_system(
            &SystemRequest::UpdateUserDirectoryStatus {
                user_id: UserId::new(42),
                status: inferadb_ledger_state::system::UserDirectoryStatus::Migrating,
                region: Some(Region::IE_EAST_DUBLIN),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Empty => {},
            other => panic!("expected Empty (no state layer), got {other}"),
        }
    }

    // ========================================================================
    // Write to suspended org rejected
    // ========================================================================

    #[tokio::test]
    async fn test_create_vault_in_provisioning_org_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        // Create org but DON'T activate it (stays in Provisioning)
        let (response, _) = store.apply_system(
            &SystemRequest::CreateOrganization {
                slug: inferadb_ledger_types::OrganizationSlug::new(100),
                region: Region::US_EAST_VA,
                tier: Default::default(),
                admin: UserId::new(1),
            },
            &mut state,
        );
        let org_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            other => panic!("expected OrganizationCreated, got {other}"),
        };

        // Try to create vault in Provisioning org
        let (response, _) = store.apply_org(
            &OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(1),
                name: Some("test".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert!(
                    code == ErrorCode::FailedPrecondition || code == ErrorCode::NotFound,
                    "expected FailedPrecondition or NotFound, got {code:?}"
                );
            },
            other => panic!("expected Error, got {other}"),
        }
    }

    // ========================================================================
    // UseRefreshToken lifecycle tests
    // ========================================================================

    #[tokio::test]
    async fn test_use_refresh_token_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        // Create a signing key (required for token operations)
        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "sk-1".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        // Create a user so we have a valid slug in the index
        let user_slug = UserSlug::new(500);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        // Create a refresh token via the state layer
        let (response, _) = store.apply_system(
            &SystemRequest::CreateRefreshToken {
                token_hash: [0xAA; 32],
                family: [0xBB; 16],
                token_type: inferadb_ledger_types::TokenType::UserSession,
                subject: TokenSubject::User(user_slug),
                organization: None,
                vault: None,
                kid: "sk-1".to_string(),
                ttl_secs: 86400,
            },
            &mut state,
        );
        match &response {
            LedgerResponse::RefreshTokenCreated { id } => {
                assert_eq!(id.value(), 1);
            },
            other => panic!("expected RefreshTokenCreated, got {other}"),
        }

        // Now write a User record to the state layer so UseRefreshToken can read it
        // (UseRefreshToken with expected_version needs the user entity)
        if let Some(state_layer) = &store.state_layer {
            let user = inferadb_ledger_state::system::User {
                id: user_id,
                slug: user_slug,
                region: Region::US_EAST_VA,
                name: "Test".to_string(),
                email: UserEmailId::new(1),
                status: inferadb_ledger_types::UserStatus::Active,
                role: inferadb_ledger_types::UserRole::default(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                deleted_at: None,
                version: inferadb_ledger_types::TokenVersion::new(0),
            };
            let key = inferadb_ledger_state::system::SystemKeys::user_key(user_id);
            let value = inferadb_ledger_types::encode(&user).expect("encode user");
            let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
            state_layer
                .apply_operations(inferadb_ledger_state::system::SYSTEM_VAULT_ID, &ops, 0)
                .expect("write user to state layer");
        }

        // Use the refresh token (rotate it)
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::UseRefreshToken {
                old_token_hash: [0xAA; 32],
                new_token_hash: [0xCC; 32],
                new_kid: "sk-1".to_string(),
                ttl_secs: 86400,
                expected_version: Some(inferadb_ledger_types::TokenVersion::new(0)),
                max_family_lifetime_secs: 604800,
            },
            &mut state,
        );

        match response {
            LedgerResponse::RefreshTokenRotated { new_id, token_version, .. } => {
                assert_eq!(new_id.value(), 2);
                assert_eq!(token_version, Some(inferadb_ledger_types::TokenVersion::new(0)));
            },
            other => panic!("expected RefreshTokenRotated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_use_refresh_token_reuse_detected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "sk-1".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        let user_slug = UserSlug::new(500);
        create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        // Create token
        store.apply_system(
            &SystemRequest::CreateRefreshToken {
                token_hash: [0xAA; 32],
                family: [0xBB; 16],
                token_type: inferadb_ledger_types::TokenType::UserSession,
                subject: TokenSubject::User(user_slug),
                organization: None,
                vault: None,
                kid: "sk-1".to_string(),
                ttl_secs: 86400,
            },
            &mut state,
        );

        // Use it once (no version check)
        let r1 = apply_at_fixed_time(
            &store,
            &SystemRequest::UseRefreshToken {
                old_token_hash: [0xAA; 32],
                new_token_hash: [0xCC; 32],
                new_kid: "sk-1".to_string(),
                ttl_secs: 86400,
                expected_version: None,
                max_family_lifetime_secs: 604800,
            },
            &mut state,
        );
        assert!(
            matches!(r1, LedgerResponse::RefreshTokenRotated { .. }),
            "first use should succeed"
        );

        // Try to reuse the old token hash (already used)
        let r2 = apply_at_fixed_time(
            &store,
            &SystemRequest::UseRefreshToken {
                old_token_hash: [0xAA; 32],
                new_token_hash: [0xDD; 32],
                new_kid: "sk-1".to_string(),
                ttl_secs: 86400,
                expected_version: None,
                max_family_lifetime_secs: 604800,
            },
            &mut state,
        );

        match r2 {
            LedgerResponse::Error { code, message } => {
                assert_eq!(code, ErrorCode::Unauthenticated);
                assert!(message.contains("reuse"));
            },
            other => panic!("expected Unauthenticated reuse error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_use_refresh_token_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::UseRefreshToken {
                old_token_hash: [0xFF; 32],
                new_token_hash: [0xCC; 32],
                new_kid: "sk-1".to_string(),
                ttl_secs: 86400,
                expected_version: None,
                max_family_lifetime_secs: 604800,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::Unauthenticated);
            },
            other => panic!("expected Unauthenticated, got {other}"),
        }
    }

    // ========================================================================
    // RevokeTokenFamily with actual tokens
    // ========================================================================

    #[tokio::test]
    async fn test_revoke_token_family_with_tokens() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "sk-1".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        let family = [0xBB; 16];

        // Create a token in that family
        store.apply_system(
            &SystemRequest::CreateRefreshToken {
                token_hash: [0xAA; 32],
                family,
                token_type: inferadb_ledger_types::TokenType::UserSession,
                subject: TokenSubject::User(UserSlug::new(42)),
                organization: None,
                vault: None,
                kid: "sk-1".to_string(),
                ttl_secs: 86400,
            },
            &mut state,
        );

        // Revoke the family
        let (response, _) =
            store.apply_system(&SystemRequest::RevokeTokenFamily { family }, &mut state);

        match response {
            LedgerResponse::TokenFamilyRevoked { count } => {
                assert!(count >= 1, "should have revoked at least 1 token");
            },
            other => panic!("expected TokenFamilyRevoked, got {other}"),
        }
    }

    // ========================================================================
    // RevokeAllUserSessions with actual user
    // ========================================================================

    #[tokio::test]
    async fn test_revoke_all_user_sessions_with_user() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(500);
        let user_id = create_test_user(&store, &mut state, user_slug, Region::US_EAST_VA);

        // Write user entity to state layer
        if let Some(state_layer) = &store.state_layer {
            let user = inferadb_ledger_state::system::User {
                id: user_id,
                slug: user_slug,
                region: Region::US_EAST_VA,
                name: "Test".to_string(),
                email: UserEmailId::new(1),
                status: inferadb_ledger_types::UserStatus::Active,
                role: inferadb_ledger_types::UserRole::default(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                deleted_at: None,
                version: inferadb_ledger_types::TokenVersion::new(0),
            };
            let key = inferadb_ledger_state::system::SystemKeys::user_key(user_id);
            let value = inferadb_ledger_types::encode(&user).expect("encode user");
            let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
            state_layer
                .apply_operations(inferadb_ledger_state::system::SYSTEM_VAULT_ID, &ops, 0)
                .expect("write user");
        }

        let (response, _) =
            store.apply_system(&SystemRequest::RevokeAllUserSessions { user: user_id }, &mut state);

        match response {
            LedgerResponse::AllUserSessionsRevoked { count, version } => {
                assert_eq!(count, 0, "no sessions created yet");
                assert_eq!(version, inferadb_ledger_types::TokenVersion::new(1));
            },
            other => panic!("expected AllUserSessionsRevoked, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_revoke_all_user_sessions_user_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        // User slug not in the index
        let (response, _) = store.apply_system(
            &SystemRequest::RevokeAllUserSessions { user: UserId::new(9999) },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::NotFound);
            },
            other => panic!("expected NotFound error, got {other}"),
        }
    }

    // ========================================================================
    // Signing key: revoke already-revoked is idempotent
    // ========================================================================

    #[tokio::test]
    async fn test_revoke_signing_key_idempotent() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "test-kid".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        // First revoke
        store.apply_system(
            &SystemRequest::RevokeSigningKey { kid: "test-kid".to_string() },
            &mut state,
        );

        // Second revoke (idempotent)
        let (response, _) = store.apply_system(
            &SystemRequest::RevokeSigningKey { kid: "test-kid".to_string() },
            &mut state,
        );

        match response {
            LedgerResponse::SigningKeyRevoked { kid } => {
                assert_eq!(kid, "test-kid");
            },
            other => panic!("expected SigningKeyRevoked (idempotent), got {other}"),
        }
    }

    // ========================================================================
    // TransitionSigningKeyRevoked: non-rotated key is a no-op
    // ========================================================================

    #[tokio::test]
    async fn test_transition_signing_key_active_is_noop() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        store.apply_system(
            &SystemRequest::CreateSigningKey {
                scope: SigningKeyScope::Global,
                kid: "active-kid".to_string(),
                public_key_bytes: vec![1u8; 32],
                encrypted_private_key: vec![2u8; 100],
                rmk_version: 1,
            },
            &mut state,
        );

        // Transition an Active key -> no-op
        let (response, _) = store.apply_system(
            &SystemRequest::TransitionSigningKeyRevoked { kid: "active-kid".to_string() },
            &mut state,
        );

        match response {
            LedgerResponse::SigningKeyTransitioned { kid } => {
                assert_eq!(kid, "active-kid");
            },
            other => panic!("expected SigningKeyTransitioned (no-op), got {other}"),
        }
    }

    #[tokio::test]
    async fn test_transition_signing_key_not_found_is_noop() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_system(
            &SystemRequest::TransitionSigningKeyRevoked { kid: "nonexistent".to_string() },
            &mut state,
        );

        match response {
            LedgerResponse::SigningKeyTransitioned { kid } => {
                assert_eq!(kid, "nonexistent");
            },
            other => panic!("expected SigningKeyTransitioned (idempotent no-op), got {other}"),
        }
    }

    // ========================================================================
    // Email verification lifecycle
    // ========================================================================

    #[tokio::test]
    async fn test_create_email_verification() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_system(
            &SystemRequest::CreateEmailVerification {
                email_hmac: "hmac-test-001".to_string(),
                code_hash: [0xAA; 32],
                region: Region::US_EAST_VA,
                expires_at: Utc::now() + Duration::minutes(10),
            },
            &mut state,
        );

        match response {
            LedgerResponse::EmailVerificationCreated => {},
            other => panic!("expected EmailVerificationCreated, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_verify_email_code_new_user() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let email_hmac = "hmac-new-user".to_string();
        let code_hash = [0xBB; 32];
        let expires_at = fixed_timestamp() + Duration::minutes(10);

        // Create verification
        store.apply_system(
            &SystemRequest::CreateEmailVerification {
                email_hmac: email_hmac.clone(),
                code_hash,
                region: Region::US_EAST_VA,
                expires_at,
            },
            &mut state,
        );

        // Verify with correct code
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::VerifyEmailCode {
                email_hmac: email_hmac.clone(),
                code_hash,
                region: Region::US_EAST_VA,
                existing_user_hmac_hit: false,
                onboarding_token_hash: [0xCC; 32],
                onboarding_expires_at: expires_at + Duration::hours(24),
                totp: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::EmailCodeVerified { result } => {
                assert!(
                    matches!(result, crate::types::EmailCodeVerifiedResult::NewUser),
                    "expected NewUser result"
                );
            },
            other => panic!("expected EmailCodeVerified, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_verify_email_code_wrong_code() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let email_hmac = "hmac-wrong-code".to_string();
        let code_hash = [0xBB; 32];
        let expires_at = fixed_timestamp() + Duration::minutes(10);

        store.apply_system(
            &SystemRequest::CreateEmailVerification {
                email_hmac: email_hmac.clone(),
                code_hash,
                region: Region::US_EAST_VA,
                expires_at,
            },
            &mut state,
        );

        // Verify with wrong code
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::VerifyEmailCode {
                email_hmac: email_hmac.clone(),
                code_hash: [0xFF; 32], // wrong!
                region: Region::US_EAST_VA,
                existing_user_hmac_hit: false,
                onboarding_token_hash: [0xCC; 32],
                onboarding_expires_at: expires_at + Duration::hours(24),
                totp: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::InvalidArgument);
            },
            other => panic!("expected InvalidArgument error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_verify_email_code_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::VerifyEmailCode {
                email_hmac: "nonexistent".to_string(),
                code_hash: [0xAA; 32],
                region: Region::US_EAST_VA,
                existing_user_hmac_hit: false,
                onboarding_token_hash: [0xCC; 32],
                onboarding_expires_at: fixed_timestamp() + Duration::hours(24),
                totp: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::NotFound);
            },
            other => panic!("expected NotFound error, got {other}"),
        }
    }

    // ========================================================================
    // CleanupExpiredOnboarding
    // ========================================================================

    #[tokio::test]
    async fn test_cleanup_expired_onboarding_empty() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let response =
            apply_at_fixed_time(&store, &SystemRequest::CleanupExpiredOnboarding, &mut state);

        match response {
            LedgerResponse::OnboardingCleanedUp {
                verification_codes_deleted,
                onboarding_accounts_deleted,
                totp_challenges_deleted,
            } => {
                assert_eq!(verification_codes_deleted, 0);
                assert_eq!(onboarding_accounts_deleted, 0);
                assert_eq!(totp_challenges_deleted, 0);
            },
            other => panic!("expected OnboardingCleanedUp, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_cleanup_expired_onboarding_without_state_layer() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) =
            store.apply_system(&SystemRequest::CleanupExpiredOnboarding, &mut state);

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::Internal);
            },
            other => panic!("expected Internal error (no state layer), got {other}"),
        }
    }

    // ========================================================================
    // UpdateOrganizationRouting
    // ========================================================================

    #[tokio::test]
    async fn test_update_organization_routing_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let (response, _) = store.apply_system(
            &SystemRequest::UpdateOrganizationRouting {
                organization: OrganizationId::new(9999),
                region: Region::US_WEST_OR,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::NotFound);
            },
            other => panic!("expected NotFound error, got {other}"),
        }
    }

    #[tokio::test]
    async fn test_update_organization_routing_deleted_org() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        // Delete the org
        store.apply_system(&SystemRequest::DeleteOrganization { organization: org_id }, &mut state);

        // Try to route a deleted org
        let (response, _) = store.apply_system(
            &SystemRequest::UpdateOrganizationRouting {
                organization: org_id,
                region: Region::US_WEST_OR,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::FailedPrecondition);
            },
            other => panic!("expected FailedPrecondition error, got {other}"),
        }
    }

    // ========================================================================
    // WriteTeam (REGIONAL SystemRequest)
    // ========================================================================

    #[test]
    fn test_write_team_creates_new_team() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(200),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);
        let team_slug = TeamSlug::new(5001);

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: team_slug,
                name: "Engineering".to_string(),
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationUpdated { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationUpdated, got {other:?}"),
        }

        // Verify name index is populated
        assert_eq!(state.team_name_index.get(&(org_id, "Engineering".to_string())), Some(&team_id),);
    }

    #[test]
    fn test_write_team_rename_updates_index() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(201),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);
        let team_slug = TeamSlug::new(5002);

        // Create team
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: team_slug,
                name: "Old Name".to_string(),
            },
            &mut state,
        );

        // Rename team
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: team_slug,
                name: "New Name".to_string(),
            },
            &mut state,
        );

        // Old name removed, new name present
        assert!(!state.team_name_index.contains_key(&(org_id, "Old Name".to_string())));
        assert_eq!(state.team_name_index.get(&(org_id, "New Name".to_string())), Some(&team_id),);
    }

    #[test]
    fn test_write_team_duplicate_name_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(202),
            Region::US_EAST_VA,
        );

        // Create first team
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: TeamId::new(1),
                slug: TeamSlug::new(5003),
                name: "Duplicate".to_string(),
            },
            &mut state,
        );

        // Second team with same name
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: TeamId::new(2),
                slug: TeamSlug::new(5004),
                name: "Duplicate".to_string(),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::AlreadyExists);
            },
            other => panic!("expected AlreadyExists error, got {other:?}"),
        }
    }

    // ========================================================================
    // AddTeamMember / RemoveTeamMember / UpdateTeamMemberRole (REGIONAL)
    // ========================================================================

    #[test]
    fn test_add_team_member_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(210),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);

        // Create team first
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: TeamSlug::new(5010),
                name: "Dev Team".to_string(),
            },
            &mut state,
        );

        // Add member
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::AddTeamMember {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(42),
                role: inferadb_ledger_state::system::TeamMemberRole::Member,
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationUpdated { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationUpdated, got {other:?}"),
        }
    }

    #[test]
    fn test_add_team_member_duplicate_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(211),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);

        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: TeamSlug::new(5011),
                name: "Dev Team".to_string(),
            },
            &mut state,
        );

        apply_at_fixed_time(
            &store,
            &SystemRequest::AddTeamMember {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(42),
                role: inferadb_ledger_state::system::TeamMemberRole::Member,
            },
            &mut state,
        );

        // Duplicate add
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::AddTeamMember {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(42),
                role: inferadb_ledger_state::system::TeamMemberRole::Member,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::AlreadyExists);
            },
            other => panic!("expected AlreadyExists error, got {other:?}"),
        }
    }

    #[test]
    fn test_remove_team_member_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(212),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);

        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: TeamSlug::new(5012),
                name: "Dev Team".to_string(),
            },
            &mut state,
        );

        // Add two members: one Manager, one Member
        apply_at_fixed_time(
            &store,
            &SystemRequest::AddTeamMember {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(10),
                role: inferadb_ledger_state::system::TeamMemberRole::Manager,
            },
            &mut state,
        );
        apply_at_fixed_time(
            &store,
            &SystemRequest::AddTeamMember {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(11),
                role: inferadb_ledger_state::system::TeamMemberRole::Member,
            },
            &mut state,
        );

        // Remove the regular member
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::RemoveTeamMember {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(11),
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationUpdated { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationUpdated, got {other:?}"),
        }
    }

    #[test]
    fn test_remove_last_team_manager_blocked() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(213),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);

        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: TeamSlug::new(5013),
                name: "Dev Team".to_string(),
            },
            &mut state,
        );

        // Add single manager
        apply_at_fixed_time(
            &store,
            &SystemRequest::AddTeamMember {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(10),
                role: inferadb_ledger_state::system::TeamMemberRole::Manager,
            },
            &mut state,
        );

        // Try to remove the only manager
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::RemoveTeamMember {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(10),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::FailedPrecondition);
            },
            other => panic!("expected FailedPrecondition error, got {other:?}"),
        }
    }

    #[test]
    fn test_remove_team_member_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(214),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);

        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: TeamSlug::new(5014),
                name: "Dev Team".to_string(),
            },
            &mut state,
        );

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::RemoveTeamMember {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(999),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::NotFound);
            },
            other => panic!("expected NotFound error, got {other:?}"),
        }
    }

    #[test]
    fn test_update_team_member_role() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(215),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);

        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: TeamSlug::new(5015),
                name: "Dev Team".to_string(),
            },
            &mut state,
        );

        apply_at_fixed_time(
            &store,
            &SystemRequest::AddTeamMember {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(42),
                role: inferadb_ledger_state::system::TeamMemberRole::Member,
            },
            &mut state,
        );

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::UpdateTeamMemberRole {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(42),
                role: inferadb_ledger_state::system::TeamMemberRole::Manager,
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationUpdated { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationUpdated, got {other:?}"),
        }
    }

    #[test]
    fn test_update_team_member_role_not_found() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(216),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);

        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: TeamSlug::new(5016),
                name: "Dev Team".to_string(),
            },
            &mut state,
        );

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::UpdateTeamMemberRole {
                organization: org_id,
                team: team_id,
                user_id: UserId::new(999),
                role: inferadb_ledger_state::system::TeamMemberRole::Manager,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::NotFound);
            },
            other => panic!("expected NotFound error, got {other:?}"),
        }
    }

    // ========================================================================
    // WriteAppProfile / DeleteAppProfile (REGIONAL)
    // ========================================================================

    #[test]
    fn test_write_app_profile_creates_new() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(220),
            Region::US_EAST_VA,
        );

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::WriteAppProfile {
                organization: org_id,
                app: inferadb_ledger_types::AppId::new(1),
                name: "My App".to_string(),
                description: Some("A test app".to_string()),
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationUpdated { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationUpdated, got {other:?}"),
        }

        assert_eq!(
            state.app_name_index.get(&(org_id, "My App".to_string())),
            Some(&inferadb_ledger_types::AppId::new(1)),
        );
    }

    #[test]
    fn test_write_app_profile_duplicate_name_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(221),
            Region::US_EAST_VA,
        );

        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteAppProfile {
                organization: org_id,
                app: inferadb_ledger_types::AppId::new(1),
                name: "Same Name".to_string(),
                description: Some(String::new()),
            },
            &mut state,
        );

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::WriteAppProfile {
                organization: org_id,
                app: inferadb_ledger_types::AppId::new(2),
                name: "Same Name".to_string(),
                description: Some(String::new()),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::AlreadyExists);
            },
            other => panic!("expected AlreadyExists error, got {other:?}"),
        }
    }

    #[test]
    fn test_delete_app_profile_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(222),
            Region::US_EAST_VA,
        );
        let app_id = inferadb_ledger_types::AppId::new(1);

        // Create profile first
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteAppProfile {
                organization: org_id,
                app: app_id,
                name: "Deletable App".to_string(),
                description: Some(String::new()),
            },
            &mut state,
        );

        // Delete it
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::DeleteAppProfile { organization: org_id, app: app_id },
            &mut state,
        );

        match response {
            LedgerResponse::AppDeleted { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected AppDeleted, got {other:?}"),
        }

        // Name index cleaned up
        assert!(!state.app_name_index.contains_key(&(org_id, "Deletable App".to_string())));
    }

    // ========================================================================
    // WriteClientAssertionName / DeleteClientAssertionName (REGIONAL)
    // ========================================================================

    #[test]
    fn test_write_client_assertion_name() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::WriteClientAssertionName {
                organization: OrganizationId::new(1),
                app: inferadb_ledger_types::AppId::new(1),
                assertion: inferadb_ledger_types::ClientAssertionId::new(1),
                name: "My Assertion".to_string(),
            },
            &mut state,
        );

        assert!(matches!(response, LedgerResponse::Empty), "expected Empty, got {response:?}");
    }

    #[test]
    fn test_delete_client_assertion_name() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        // Write first
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteClientAssertionName {
                organization: OrganizationId::new(1),
                app: inferadb_ledger_types::AppId::new(1),
                assertion: inferadb_ledger_types::ClientAssertionId::new(1),
                name: "My Assertion".to_string(),
            },
            &mut state,
        );

        // Delete
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::DeleteClientAssertionName {
                organization: OrganizationId::new(1),
                app: inferadb_ledger_types::AppId::new(1),
                assertion: inferadb_ledger_types::ClientAssertionId::new(1),
            },
            &mut state,
        );

        assert!(matches!(response, LedgerResponse::Empty), "expected Empty, got {response:?}");
    }

    // ========================================================================
    // DeleteTeam (REGIONAL)
    // ========================================================================

    #[test]
    fn test_delete_team_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(230),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);

        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: TeamSlug::new(5030),
                name: "Deletable Team".to_string(),
            },
            &mut state,
        );

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::DeleteTeam {
                organization: org_id,
                team: team_id,
                move_members_to: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationTeamDeleted { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationTeamDeleted, got {other:?}"),
        }

        // Name index cleaned up
        assert!(!state.team_name_index.contains_key(&(org_id, "Deletable Team".to_string())));
    }

    #[test]
    fn test_delete_team_not_found_idempotent() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(231),
            Region::US_EAST_VA,
        );

        // Delete nonexistent team should succeed idempotently
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::DeleteTeam {
                organization: org_id,
                team: TeamId::new(999),
                move_members_to: None,
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationTeamDeleted { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationTeamDeleted, got {other:?}"),
        }
    }

    #[test]
    fn test_delete_team_move_to_same_team_rejected() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(232),
            Region::US_EAST_VA,
        );
        let team_id = TeamId::new(1);

        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: team_id,
                slug: TeamSlug::new(5032),
                name: "Self Move".to_string(),
            },
            &mut state,
        );

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::DeleteTeam {
                organization: org_id,
                team: team_id,
                move_members_to: Some(team_id),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::FailedPrecondition);
            },
            other => panic!("expected FailedPrecondition error, got {other:?}"),
        }
    }

    // ========================================================================
    // PurgeOrganizationRegional (REGIONAL)
    // ========================================================================

    #[test]
    fn test_purge_organization_regional() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(240),
            Region::US_EAST_VA,
        );

        // Create team + app profile to be purged
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteTeam {
                organization: org_id,
                team: TeamId::new(1),
                slug: TeamSlug::new(5040),
                name: "Team To Purge".to_string(),
            },
            &mut state,
        );
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteAppProfile {
                organization: org_id,
                app: inferadb_ledger_types::AppId::new(1),
                name: "App To Purge".to_string(),
                description: Some(String::new()),
            },
            &mut state,
        );

        assert!(!state.team_name_index.is_empty());
        assert!(!state.app_name_index.is_empty());

        // Purge
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::PurgeOrganizationRegional { organization: org_id },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationPurged { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationPurged, got {other:?}"),
        }

        // Indices cleaned
        assert!(
            state.team_name_index.iter().all(|((oid, _), _)| *oid != org_id),
            "team indices should be purged",
        );
        assert!(
            state.app_name_index.iter().all(|((oid, _), _)| *oid != org_id),
            "app indices should be purged",
        );
    }

    // ========================================================================
    // WriteOrganizationInvite / UpdateOrganizationInviteStatus /
    // DeleteOrganizationInvite / RehashInvitationEmailHmac (REGIONAL)
    // ========================================================================

    #[test]
    fn test_write_organization_invite_success() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(250),
            Region::US_EAST_VA,
        );

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::WriteOrganizationInvite {
                organization: org_id,
                invite: InviteId::new(1),
                slug: InviteSlug::new(9001),
                token_hash: [0u8; 32],
                inviter: UserId::new(1),
                invitee_email_hmac: "hmac-invite-1".to_string(),
                invitee_email: "user@example.com".to_string(),
                role: inferadb_ledger_state::system::OrganizationMemberRole::Member,
                team: None,
                expires_at: fixed_timestamp() + Duration::days(7),
            },
            &mut state,
        );

        assert!(matches!(response, LedgerResponse::Empty), "expected Empty, got {response:?}");
    }

    #[test]
    fn test_update_organization_invite_status() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(251),
            Region::US_EAST_VA,
        );
        let invite_id = InviteId::new(1);

        // Write invite first
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteOrganizationInvite {
                organization: org_id,
                invite: invite_id,
                slug: InviteSlug::new(9002),
                token_hash: [0u8; 32],
                inviter: UserId::new(1),
                invitee_email_hmac: "hmac-invite-2".to_string(),
                invitee_email: "user@example.com".to_string(),
                role: inferadb_ledger_state::system::OrganizationMemberRole::Member,
                team: None,
                expires_at: fixed_timestamp() + Duration::days(7),
            },
            &mut state,
        );

        // Accept invite
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::UpdateOrganizationInviteStatus {
                organization: org_id,
                invite: invite_id,
                status: InvitationStatus::Accepted,
            },
            &mut state,
        );

        assert!(matches!(response, LedgerResponse::Empty), "expected Empty, got {response:?}");
    }

    #[test]
    fn test_delete_organization_invite() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(252),
            Region::US_EAST_VA,
        );
        let invite_id = InviteId::new(1);

        // Write invite first
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteOrganizationInvite {
                organization: org_id,
                invite: invite_id,
                slug: InviteSlug::new(9003),
                token_hash: [0u8; 32],
                inviter: UserId::new(1),
                invitee_email_hmac: "hmac-invite-3".to_string(),
                invitee_email: "user@example.com".to_string(),
                role: inferadb_ledger_state::system::OrganizationMemberRole::Member,
                team: None,
                expires_at: fixed_timestamp() + Duration::days(7),
            },
            &mut state,
        );

        // Delete
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::DeleteOrganizationInvite { organization: org_id, invite: invite_id },
            &mut state,
        );

        assert!(matches!(response, LedgerResponse::Empty), "expected Empty, got {response:?}");
    }

    #[test]
    fn test_rehash_invitation_email_hmac() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(253),
            Region::US_EAST_VA,
        );
        let invite_id = InviteId::new(1);

        // Write invite first
        apply_at_fixed_time(
            &store,
            &SystemRequest::WriteOrganizationInvite {
                organization: org_id,
                invite: invite_id,
                slug: InviteSlug::new(9004),
                token_hash: [0u8; 32],
                inviter: UserId::new(1),
                invitee_email_hmac: "old-hmac".to_string(),
                invitee_email: "user@example.com".to_string(),
                role: inferadb_ledger_state::system::OrganizationMemberRole::Member,
                team: None,
                expires_at: fixed_timestamp() + Duration::days(7),
            },
            &mut state,
        );

        // Rehash
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::RehashInvitationEmailHmac {
                organization: org_id,
                invite: invite_id,
                new_hmac: "new-hmac".to_string(),
            },
            &mut state,
        );

        assert!(matches!(response, LedgerResponse::Empty), "expected Empty, got {response:?}");
    }

    // ========================================================================
    // WriteOrganizationProfile / UpdateOrganizationProfile (REGIONAL)
    // ========================================================================

    #[test]
    fn test_update_organization_profile() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(260),
            Region::US_EAST_VA,
        );

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::UpdateOrganizationProfile {
                organization: org_id,
                name: "Updated Org Name".to_string(),
            },
            &mut state,
        );

        match response {
            LedgerResponse::OrganizationUpdated { organization_id } => {
                assert_eq!(organization_id, org_id);
            },
            other => panic!("expected OrganizationUpdated, got {other:?}"),
        }
    }

    // ========================================================================
    // EraseUser (GLOBAL SystemRequest)
    // ========================================================================

    #[test]
    fn test_erase_user_without_state_layer_returns_empty() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::EraseUser { user_id: UserId::new(1), region: Region::US_EAST_VA },
            &mut state,
        );

        assert!(matches!(response, LedgerResponse::Empty), "expected Empty, got {response:?}");
    }

    // ========================================================================
    // MigrateExistingUsers (GLOBAL SystemRequest)
    // ========================================================================

    #[test]
    fn test_migrate_existing_users_without_state_layer_returns_empty() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::MigrateExistingUsers { entries: vec![] },
            &mut state,
        );

        assert!(matches!(response, LedgerResponse::Empty), "expected Empty, got {response:?}");
    }

    // ========================================================================
    // CreateOnboardingUser (GLOBAL SystemRequest)
    // ========================================================================

    #[test]
    fn test_create_onboarding_user_without_state_layer_returns_error() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::CreateOnboardingUser {
                email_hmac: "test-hmac".to_string(),
                user_slug: UserSlug::new(1),
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(1),
                region: Region::US_EAST_VA,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::Internal);
            },
            other => panic!("expected Internal error, got {other:?}"),
        }
    }

    #[test]
    fn test_create_onboarding_user_email_already_active() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        // Register an active email hash
        apply_at_fixed_time(
            &store,
            &SystemRequest::RegisterEmailHash {
                hmac_hex: "occupied-hmac".to_string(),
                user_id: UserId::new(42),
            },
            &mut state,
        );

        // Try to onboard with same HMAC
        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::CreateOnboardingUser {
                email_hmac: "occupied-hmac".to_string(),
                user_slug: UserSlug::new(1),
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(1),
                region: Region::US_EAST_VA,
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::AlreadyExists);
            },
            other => panic!("expected AlreadyExists error, got {other:?}"),
        }
    }

    #[test]
    fn test_create_onboarding_user_idempotent_retry() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state(dir.path());
        let mut state = (*store.applied_state.load_full()).clone();

        let user_slug = UserSlug::new(7001);
        let org_slug = inferadb_ledger_types::OrganizationSlug::new(7001);

        // First call
        let response1 = apply_at_fixed_time(
            &store,
            &SystemRequest::CreateOnboardingUser {
                email_hmac: "idempotent-hmac".to_string(),
                user_slug,
                organization_slug: org_slug,
                region: Region::US_EAST_VA,
            },
            &mut state,
        );

        let (user_id1, org_id1) = match response1 {
            LedgerResponse::OnboardingUserCreated { user_id, organization_id } => {
                (user_id, organization_id)
            },
            other => panic!("expected OnboardingUserCreated, got {other:?}"),
        };

        // Retry with same parameters (idempotent)
        let response2 = apply_at_fixed_time(
            &store,
            &SystemRequest::CreateOnboardingUser {
                email_hmac: "idempotent-hmac".to_string(),
                user_slug,
                organization_slug: org_slug,
                region: Region::US_EAST_VA,
            },
            &mut state,
        );

        match response2 {
            LedgerResponse::OnboardingUserCreated { user_id, organization_id } => {
                assert_eq!(user_id, user_id1);
                assert_eq!(organization_id, org_id1);
            },
            other => panic!("expected idempotent OnboardingUserCreated, got {other:?}"),
        }
    }

    // ========================================================================
    // ActivateOnboardingUser — no state layer
    // ========================================================================

    #[test]
    fn test_activate_onboarding_without_state_layer_returns_error() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        let response = apply_at_fixed_time(
            &store,
            &SystemRequest::ActivateOnboardingUser {
                user_id: UserId::new(1),
                user_slug: UserSlug::new(1),
                organization_id: OrganizationId::new(1),
                organization_slug: inferadb_ledger_types::OrganizationSlug::new(1),
                email_hmac: "hmac".to_string(),
            },
            &mut state,
        );

        match response {
            LedgerResponse::Error { code, .. } => {
                assert_eq!(code, ErrorCode::Internal);
            },
            other => panic!("expected Internal error, got {other:?}"),
        }
    }

    // ========================================================================
    // BatchWrite containing system requests
    // ========================================================================

    #[test]
    fn test_batch_write_with_org_requests() {
        let dir = tempdir().expect("create temp dir");
        let store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db")).expect("open store");
        let mut state = (*store.applied_state.load_full()).clone();

        // Set up org and vault for the batch write
        let org_id = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(5000),
            Region::US_EAST_VA,
        );

        let requests = vec![
            OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(8001),
                name: Some("vault-batch-1".to_string()),
                retention_policy: None,
            },
            OrganizationRequest::CreateVault {
                organization: org_id,
                slug: VaultSlug::new(8002),
                name: Some("vault-batch-2".to_string()),
                retention_policy: None,
            },
        ];

        let response =
            apply_at_fixed_time(&store, &OrganizationRequest::BatchWrite { requests }, &mut state);

        match response {
            LedgerResponse::BatchWrite { responses } => {
                assert_eq!(responses.len(), 2);
                assert!(
                    matches!(responses[0], LedgerResponse::VaultCreated { .. }),
                    "first should be VaultCreated",
                );
                assert!(
                    matches!(responses[1], LedgerResponse::VaultCreated { .. }),
                    "second should be VaultCreated",
                );
            },
            other => panic!("expected BatchWrite, got {other:?}"),
        }
    }

    // ========================================================================
    // Raft log append, purge, delete conflict, and get_log_state
    // ========================================================================

    #[tokio::test]
    async fn test_last_applied_state_initial() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        let (last_applied, _membership) = store.last_applied_state().await.expect("last applied");
        assert!(last_applied.is_none());
    }

    #[tokio::test]
    async fn test_get_current_snapshot_empty_returns_none() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let mut store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        let snapshot = store.get_current_snapshot().await.expect("get snapshot");
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn test_vote_round_trip_overwrites() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        let vote1 = Vote { term: 1, node_id: 42, committed: false };
        store.save_vote(&vote1).await.expect("save vote1");
        assert_eq!(store.read_vote().await.expect("read"), Some(vote1));

        // Overwrite with new vote
        let vote2 = Vote { term: 2, node_id: 42, committed: false };
        store.save_vote(&vote2).await.expect("save vote2");
        assert_eq!(store.read_vote().await.expect("read"), Some(vote2));
    }

    // ========================================================================
    // snapshot-build sync hook test
    // ========================================================================

    /// `LedgerSnapshotBuilder::build_snapshot` (via `get_current_snapshot`)
    /// must force `sync_state` at the top so the snapshot captures durable
    /// state. Proved by: in-memory-commit a write directly to the state DB
    /// bypassing `save_vote`/etc., confirm the synced id is stale, call
    /// `get_current_snapshot`, and observe the synced id advanced.
    ///
    /// The `save_state_core` FLIP + `save_vote` KEEP classification are
    /// covered by dedicated tests in `log_storage::store::tests` (see
    /// `save_state_core_commits_in_memory_only_then_sync_advances_snapshot`
    /// and `save_vote_is_durable_before_returning`).
    #[tokio::test]
    async fn build_snapshot_forces_sync_state() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let mut store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        // Directly commit_in_memory a single write against the state DB so
        // `committed_state` is ahead of the durable dual-slot. Bypasses the
        // apply pipeline; we only care about exercising the sync hook here.
        {
            let mut txn = store.db.write().expect("begin write txn");
            txn.insert::<tables::RaftState>(
                &"task_2c_sync_hook_probe".to_string(),
                &b"probe-value".to_vec(),
            )
            .expect("insert");
            txn.commit_in_memory().expect("commit_in_memory");
        }

        // `last_synced_snapshot_id` is still whatever it was on open — the
        // commit_in_memory above did not advance it.
        let synced_before = store.db.last_synced_snapshot_id();

        // Trigger a snapshot build. Must call `sync_state` at the top.
        let _snapshot = store.get_current_snapshot().await.expect("get snapshot");

        let synced_after = store.db.last_synced_snapshot_id();
        assert!(
            synced_after > synced_before,
            "build_snapshot must advance last_synced_snapshot_id \
             (sync hook): before={synced_before} after={synced_after}"
        );
    }

    // ========================================================================
    // replay_crash_gap
    // ========================================================================
    //
    // These tests drive `RaftLogStore::replay_crash_gap` directly against an
    // `InMemoryWalBackend`. The apply path they invoke uses `EntryKind::Normal`
    // with empty payload (a Raft §5.4.2 no-op) so `apply_committed_entries`
    // exercises the replay wiring (state.last_applied advance, post-replay
    // sync_state) without depending on state_layer / block_archive machinery.

    /// Helper: produce a WAL frame for the given shard + index (no-op entry).
    fn replay_wal_frame(
        shard_id: inferadb_ledger_consensus::types::ConsensusStateId,
        index: u64,
        term: u64,
    ) -> inferadb_ledger_consensus::wal_backend::WalFrame {
        inferadb_ledger_consensus::wal_backend::WalFrame {
            shard_id,
            index,
            term,
            // Empty data => EntryKind::Normal no-op in apply_committed_entries
            data: std::sync::Arc::from(Vec::new().as_slice()),
        }
    }

    #[tokio::test]
    async fn replay_crash_gap_empty_log_is_noop() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let mut store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        // Fresh WAL with no checkpoint and no frames.
        let wal = inferadb_ledger_consensus::wal::InMemoryWalBackend::new();
        let shard_id = inferadb_ledger_consensus::types::ConsensusStateId(42);

        let synced_before = store.db.last_synced_snapshot_id();
        let stats = store.replay_crash_gap::<inferadb_ledger_consensus::wal::InMemoryWalBackend, OrganizationRequest>(&wal, shard_id).await.expect("replay");

        assert_eq!(stats.replayed_entries, 0, "empty WAL must replay zero entries");
        assert_eq!(stats.applied_durable, 0, "fresh store has applied_durable = 0");
        assert_eq!(stats.last_committed, 0, "no checkpoint means last_committed = 0");
        let synced_after = store.db.last_synced_snapshot_id();
        assert_eq!(
            synced_after, synced_before,
            "empty-log fast-path must NOT fire sync_state \
             (before={synced_before}, after={synced_after})",
        );
    }

    #[tokio::test]
    async fn replay_crash_gap_caught_up_is_noop() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let mut store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        let shard_id = inferadb_ledger_consensus::types::ConsensusStateId(7);

        // Pre-populate the WAL with frames 1..=3 and a checkpoint at
        // committed_index=3. Simulate "state DB already caught up" by
        // setting `applied_state.last_applied.index = 3` directly.
        let mut wal = inferadb_ledger_consensus::wal::InMemoryWalBackend::new();
        use inferadb_ledger_consensus::wal_backend::WalBackend;
        wal.append(&[
            replay_wal_frame(shard_id, 1, 1),
            replay_wal_frame(shard_id, 2, 1),
            replay_wal_frame(shard_id, 3, 1),
        ])
        .expect("append frames");
        wal.write_checkpoint(&inferadb_ledger_consensus::wal_backend::CheckpointFrame {
            committed_index: 3,
            term: 1,
            voted_for: None,
        })
        .expect("write checkpoint");
        wal.sync().expect("sync");

        // Seed applied_state.last_applied.index to match the checkpoint —
        // simulates a clean shutdown where `sync_all_state_dbs` drove the
        // gap to zero.
        {
            let current = store.applied_state.load_full();
            let mut new_state = (*current).clone();
            new_state.last_applied = Some(LogId::new(1, 0, 3));
            store.applied_state.store(Arc::new(new_state));
        }

        let stats = store.replay_crash_gap::<inferadb_ledger_consensus::wal::InMemoryWalBackend, OrganizationRequest>(&wal, shard_id).await.expect("replay");

        assert_eq!(
            stats.replayed_entries, 0,
            "applied_durable == last_committed must replay zero entries",
        );
        assert_eq!(stats.applied_durable, 3);
        assert_eq!(stats.last_committed, 3);
    }

    #[tokio::test]
    async fn replay_crash_gap_replays_tail_and_syncs() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let mut store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        let shard_id = inferadb_ledger_consensus::types::ConsensusStateId(11);

        // WAL has frames 1..=5 committed. State DB simulates a crash mid-way:
        // applied_durable = 2, so indexes 3..=5 must be replayed.
        let mut wal = inferadb_ledger_consensus::wal::InMemoryWalBackend::new();
        use inferadb_ledger_consensus::wal_backend::WalBackend;
        wal.append(&[
            replay_wal_frame(shard_id, 1, 1),
            replay_wal_frame(shard_id, 2, 1),
            replay_wal_frame(shard_id, 3, 1),
            replay_wal_frame(shard_id, 4, 1),
            replay_wal_frame(shard_id, 5, 1),
        ])
        .expect("append frames");
        wal.write_checkpoint(&inferadb_ledger_consensus::wal_backend::CheckpointFrame {
            committed_index: 5,
            term: 1,
            voted_for: None,
        })
        .expect("write checkpoint");
        wal.sync().expect("sync");

        // Seed a partial recovery: applied_durable = 2.
        {
            let current = store.applied_state.load_full();
            let mut new_state = (*current).clone();
            new_state.last_applied = Some(LogId::new(1, 0, 2));
            store.applied_state.store(Arc::new(new_state));
        }

        let synced_before = store.db.last_synced_snapshot_id();
        let stats = store.replay_crash_gap::<inferadb_ledger_consensus::wal::InMemoryWalBackend, OrganizationRequest>(&wal, shard_id).await.expect("replay");

        assert_eq!(stats.replayed_entries, 3, "must replay 3..=5");
        assert_eq!(stats.applied_durable, 2);
        assert_eq!(stats.last_committed, 5);

        // State DB now reflects index 5.
        let state_after = store.applied_state.load();
        assert_eq!(
            state_after.last_applied.as_ref().map(|id| id.index),
            Some(5),
            "applied_state.last_applied must advance past the replay window",
        );

        // Post-replay sync_state must have advanced the durable dual-slot.
        let synced_after = store.db.last_synced_snapshot_id();
        assert!(
            synced_after > synced_before,
            "post-replay sync_state must advance last_synced_snapshot_id \
             (before={synced_before} after={synced_after})",
        );
    }

    /// Post-replay sync must advance `last_synced_snapshot_id` on EVERY
    /// configured domain DB (state.db, blocks.db, events.db) in addition
    /// to raft.db. If only state.db + raft.db were synced, any dirty
    /// pages in blocks.db or events.db (produced by the replayed
    /// commit-in-memory applies) would accumulate in memory until the next
    /// checkpointer tick — a second crash during startup would lose them.
    #[tokio::test]
    async fn replay_crash_gap_post_replay_syncs_all_four_dbs() {
        let dir = tempdir().expect("create temp dir");

        // Build state.db + blocks.db + events.db alongside raft.db so all
        // three optional accessors on RaftLogStore are populated.
        let state_db = Arc::new(
            inferadb_ledger_store::Database::create(dir.path().join("state.db"))
                .expect("create state db"),
        );
        let blocks_db = Arc::new(
            inferadb_ledger_store::Database::create(dir.path().join("blocks.db"))
                .expect("create blocks db"),
        );
        // `EventsDatabase::open(&data_dir)` appends `/events.db` itself;
        // create a dedicated subdir so the events file doesn't collide
        // with the other DBs sharing the tempdir.
        let events_subdir = dir.path().join("events");
        std::fs::create_dir_all(&events_subdir).expect("create events dir");
        let events_db = Arc::new(EventsDatabase::open(&events_subdir).expect("open"));

        let state_layer = Arc::new(inferadb_ledger_state::StateLayer::new(Arc::clone(&state_db)));
        let block_archive =
            Arc::new(inferadb_ledger_state::BlockArchive::new(Arc::clone(&blocks_db)));
        let event_writer =
            crate::event_writer::EventWriter::new(Arc::clone(&events_db), EventConfig::default());

        let mut store = RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
            .expect("open store")
            .with_state_layer(state_layer)
            .with_block_archive(block_archive)
            .with_event_writer(event_writer);

        let shard_id = inferadb_ledger_consensus::types::ConsensusStateId(91);

        // Set up a WAL with frames 1..=3 committed and a state DB that
        // has only applied through index 1 — indexes 2..=3 get replayed.
        let mut wal = inferadb_ledger_consensus::wal::InMemoryWalBackend::new();
        use inferadb_ledger_consensus::wal_backend::WalBackend;
        wal.append(&[
            replay_wal_frame(shard_id, 1, 1),
            replay_wal_frame(shard_id, 2, 1),
            replay_wal_frame(shard_id, 3, 1),
        ])
        .expect("append frames");
        wal.write_checkpoint(&inferadb_ledger_consensus::wal_backend::CheckpointFrame {
            committed_index: 3,
            term: 1,
            voted_for: None,
        })
        .expect("write checkpoint");
        wal.sync().expect("sync");

        {
            let current = store.applied_state.load_full();
            let mut new_state = (*current).clone();
            new_state.last_applied = Some(LogId::new(1, 0, 1));
            store.applied_state.store(Arc::new(new_state));
        }

        // Dirty a page on each domain DB via `commit_in_memory` to force
        // `sync_state` on that DB to advance its `last_synced_snapshot_id`.
        // The no-op WAL entries don't touch these DBs themselves, so
        // without the pre-dirtied page the post-replay sync would be a
        // clean-cache no-op and the assertion would be vacuous.
        for db in [&state_db, &blocks_db] {
            let mut txn = db.write().expect("open write txn");
            txn.insert::<inferadb_ledger_store::tables::Entities>(
                &b"pre-replay-dirty".to_vec(),
                &b"v".to_vec(),
            )
            .expect("insert");
            txn.commit_in_memory().expect("commit_in_memory");
        }
        // events.db uses its own table types; dirty it via the Events
        // table if available, otherwise via Entities which is present in
        // every DB's schema.
        {
            let mut txn = events_db.write().expect("open events write txn");
            txn.insert::<inferadb_ledger_store::tables::Entities>(
                &b"pre-replay-dirty".to_vec(),
                &b"v".to_vec(),
            )
            .expect("insert");
            txn.commit_in_memory().expect("commit_in_memory");
        }

        let state_synced_before = state_db.last_synced_snapshot_id();
        let raft_synced_before = store.db.last_synced_snapshot_id();
        let blocks_synced_before = blocks_db.last_synced_snapshot_id();
        let events_synced_before = events_db.db().last_synced_snapshot_id();

        let stats = store.replay_crash_gap::<inferadb_ledger_consensus::wal::InMemoryWalBackend, OrganizationRequest>(&wal, shard_id).await.expect("replay");

        assert_eq!(stats.replayed_entries, 2, "expected to replay indexes 2..=3");

        // All four DBs must advance their last_synced_snapshot_id post-replay.
        assert!(
            store.db.last_synced_snapshot_id() > raft_synced_before,
            "raft.db last_synced_snapshot_id must advance on post-replay sync"
        );
        assert!(
            state_db.last_synced_snapshot_id() > state_synced_before,
            "state.db last_synced_snapshot_id must advance on post-replay sync"
        );
        assert!(
            blocks_db.last_synced_snapshot_id() > blocks_synced_before,
            "blocks.db last_synced_snapshot_id must advance on post-replay sync"
        );
        assert!(
            events_db.db().last_synced_snapshot_id() > events_synced_before,
            "events.db last_synced_snapshot_id must advance on post-replay sync"
        );
    }

    // =========================================================================
    // γ Phase 3b: per-organization vault lifecycle routing
    // =========================================================================

    /// `SystemRequest::RegisterVaultDirectoryEntry` inserts into GLOBAL's
    /// bidirectional slug index (`vault_slug_index` + `vault_id_to_slug`)
    /// and is idempotent — re-applying the same `(slug, org, vault_id)`
    /// tuple is a no-op at the index level.
    #[tokio::test]
    async fn register_vault_directory_entry_populates_slug_index_and_is_idempotent() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(0));
        let mut state = (*store.applied_state.load_full()).clone();

        let organization = OrganizationId::new(7);
        let vault = VaultId::new(42);
        let slug = VaultSlug::new(0xABCD_EF00);

        let req = SystemRequest::RegisterVaultDirectoryEntry { organization, vault, slug };

        let (resp1, _) = store.apply_system(&req, &mut state);
        assert!(matches!(resp1, LedgerResponse::Empty));
        assert_eq!(state.vault_slug_index.get(&slug).copied(), Some((organization, vault)));
        assert_eq!(state.vault_id_to_slug.get(&(organization, vault)).copied(), Some(slug));

        // Idempotent re-apply — index values identical.
        let (resp2, _) = store.apply_system(&req, &mut state);
        assert!(matches!(resp2, LedgerResponse::Empty));
        assert_eq!(state.vault_slug_index.get(&slug).copied(), Some((organization, vault)));
        assert_eq!(state.vault_id_to_slug.get(&(organization, vault)).copied(), Some(slug));
    }

    /// `SystemRequest::UnregisterVaultDirectoryEntry` removes both sides of
    /// the index and is idempotent on a missing entry.
    #[tokio::test]
    async fn unregister_vault_directory_entry_removes_both_sides_and_is_idempotent() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(0));
        let mut state = (*store.applied_state.load_full()).clone();

        let organization = OrganizationId::new(9);
        let vault = VaultId::new(21);
        let slug = VaultSlug::new(0xDEAD_BEEF);

        let register = SystemRequest::RegisterVaultDirectoryEntry { organization, vault, slug };
        store.apply_system(&register, &mut state);
        assert!(state.vault_slug_index.contains_key(&slug));
        assert!(state.vault_id_to_slug.contains_key(&(organization, vault)));

        let unregister = SystemRequest::UnregisterVaultDirectoryEntry { organization, vault };
        let (resp1, _) = store.apply_system(&unregister, &mut state);
        assert!(matches!(resp1, LedgerResponse::Empty));
        assert!(!state.vault_slug_index.contains_key(&slug));
        assert!(!state.vault_id_to_slug.contains_key(&(organization, vault)));

        // Idempotent when the entry is already absent.
        let (resp2, _) = store.apply_system(&unregister, &mut state);
        assert!(matches!(resp2, LedgerResponse::Empty));
    }

    /// `SystemRequest::RegisterVaultDirectoryEntry` rejects collisions: a second
    /// register with the same `(org, vault)` but a different slug, or the same
    /// slug but a different `(org, vault)`, returns a `FailedPrecondition` error
    /// instead of silently overwriting one side of the bidirectional index.
    ///
    /// In practice Snowflake slug generation prevents collisions, but
    /// defence-in-depth matters — a bug elsewhere that sends inconsistent
    /// register entries should surface loudly rather than desync the index.
    #[tokio::test]
    async fn register_vault_directory_entry_rejects_collisions() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(0));
        let mut state = (*store.applied_state.load_full()).clone();

        let organization = OrganizationId::new(3);
        let vault = VaultId::new(11);
        let slug = VaultSlug::new(0x1234_5678);

        let ok = SystemRequest::RegisterVaultDirectoryEntry { organization, vault, slug };
        let (resp, _) = store.apply_system(&ok, &mut state);
        assert!(matches!(resp, LedgerResponse::Empty));

        // Collision: same (org, vault), different slug.
        let slug_conflict = SystemRequest::RegisterVaultDirectoryEntry {
            organization,
            vault,
            slug: VaultSlug::new(0x9999_9999),
        };
        let (resp, _) = store.apply_system(&slug_conflict, &mut state);
        assert!(matches!(resp, LedgerResponse::Error { code, .. }
            if code == ErrorCode::FailedPrecondition));

        // Collision: same slug, different (org, vault).
        let vault_conflict = SystemRequest::RegisterVaultDirectoryEntry {
            organization: OrganizationId::new(5),
            vault: VaultId::new(99),
            slug,
        };
        let (resp, _) = store.apply_system(&vault_conflict, &mut state);
        assert!(matches!(resp, LedgerResponse::Error { code, .. }
            if code == ErrorCode::FailedPrecondition));

        // Original entry remains untouched.
        assert_eq!(state.vault_slug_index.get(&slug).copied(), Some((organization, vault)));
        assert_eq!(state.vault_id_to_slug.get(&(organization, vault)).copied(), Some(slug));
    }

    /// `OrganizationRequest::CreateVault` routed through the
    /// `SystemRequest::OrganizationMetadata` GLOBAL shim is rejected with a
    /// tier-violation error. Post-γ-3b the vault body must land on the
    /// per-organization group, and the slug-index is maintained via
    /// `RegisterVaultDirectoryEntry` — routing through the metadata shim
    /// would put the body on GLOBAL and skip the directory proposal.
    #[tokio::test]
    async fn organization_metadata_shim_rejects_create_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(0));
        let mut state = (*store.applied_state.load_full()).clone();

        let request =
            SystemRequest::OrganizationMetadata(Box::new(OrganizationRequest::CreateVault {
                organization: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("rejected".to_string()),
                retention_policy: None,
            }));
        let (response, _) = store.apply_system(&request, &mut state);
        match response {
            LedgerResponse::Error { code, message } => {
                assert_eq!(code, ErrorCode::InvalidArgument);
                assert!(
                    message.contains("CreateVault"),
                    "expected message to reference CreateVault, got: {message}"
                );
                assert!(
                    message.contains("tier"),
                    "expected message to mention tier violation, got: {message}"
                );
            },
            other => panic!("expected Error, got {other:?}"),
        }
    }

    /// Same shim rejection for `UpdateVault`.
    #[tokio::test]
    async fn organization_metadata_shim_rejects_update_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(0));
        let mut state = (*store.applied_state.load_full()).clone();

        let request =
            SystemRequest::OrganizationMetadata(Box::new(OrganizationRequest::UpdateVault {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                retention_policy: None,
            }));
        let (response, _) = store.apply_system(&request, &mut state);
        assert!(
            matches!(response, LedgerResponse::Error { code: ErrorCode::InvalidArgument, .. }),
            "expected InvalidArgument, got {response:?}"
        );
    }

    /// Same shim rejection for `DeleteVault`.
    #[tokio::test]
    async fn organization_metadata_shim_rejects_delete_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(0));
        let mut state = (*store.applied_state.load_full()).clone();

        let request =
            SystemRequest::OrganizationMetadata(Box::new(OrganizationRequest::DeleteVault {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
            }));
        let (response, _) = store.apply_system(&request, &mut state);
        assert!(
            matches!(response, LedgerResponse::Error { code: ErrorCode::InvalidArgument, .. }),
            "expected InvalidArgument, got {response:?}"
        );
    }

    /// Per-organization `CreateVault` apply does NOT mutate the slug-index
    /// maps (`vault_slug_index` / `vault_id_to_slug`) — those live on
    /// GLOBAL and are maintained via `RegisterVaultDirectoryEntry`.
    /// Mutating them from per-org apply changed state-root identity in
    /// prior flip attempts and broke block-announcement delivery.
    #[tokio::test]
    async fn per_org_create_vault_does_not_touch_slug_index() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        // Per-organization group: organization_id != 0.
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        // Seed the organization as Active in the per-org state so
        // `require_fully_active_org` passes.
        let organization = OrganizationId::new(1);
        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(1),
            Region::US_EAST_VA,
        );

        // CreateVault through the per-org apply path.
        let slug = VaultSlug::new(0xCAFE_BEEF);
        let req = OrganizationRequest::CreateVault {
            organization,
            slug,
            name: Some("per-org".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_org(&req, &mut state);
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault, slug: out_slug } => {
                assert_eq!(out_slug, slug);
                vault
            },
            other => panic!("expected VaultCreated, got {other:?}"),
        };

        // Vault body lives in per-org state.
        assert!(state.vaults.contains_key(&(organization, vault_id)));
        assert!(state.vault_heights.contains_key(&(organization, vault_id)));
        assert!(state.vault_health.contains_key(&(organization, vault_id)));

        // Slug-index maps are untouched by per-org apply — they are
        // populated separately via `SystemRequest::RegisterVaultDirectoryEntry`
        // on GLOBAL.
        assert!(
            !state.vault_slug_index.contains_key(&slug),
            "per-org CreateVault must not insert into vault_slug_index \
             (state-root invariant — prior flip attempts regressed \
             watch_blocks_realtime by doing this)"
        );
        assert!(
            !state.vault_id_to_slug.contains_key(&(organization, vault_id)),
            "per-org CreateVault must not insert into vault_id_to_slug \
             (state-root invariant)"
        );
    }

    /// Per-organization CreateVault is idempotent by `(organization, slug)`.
    ///
    /// γ Phase 3b's dual-propose path can fail between step (a)
    /// per-org CreateVault and step (b) GLOBAL RegisterVaultDirectoryEntry.
    /// The client retry then re-issues CreateVault with the same slug.
    /// Without idempotency the per-org apply would allocate a new `VaultId`
    /// each retry, leaving orphan vault bodies on per-org state. This
    /// test pins the idempotency contract: same slug → same vault_id,
    /// sequence counter not advanced.
    #[tokio::test]
    async fn per_org_create_vault_is_idempotent_by_slug() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));
        let mut state = (*store.applied_state.load_full()).clone();

        let organization = OrganizationId::new(1);
        create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(1),
            Region::US_EAST_VA,
        );

        let slug = VaultSlug::new(0x1111_2222);
        let req = OrganizationRequest::CreateVault {
            organization,
            slug,
            name: Some("first".to_string()),
            retention_policy: None,
        };

        let (resp1, _) = store.apply_org(&req, &mut state);
        let vault_id_1 = match resp1 {
            LedgerResponse::VaultCreated { vault, slug: out } => {
                assert_eq!(out, slug);
                vault
            },
            other => panic!("expected VaultCreated, got {other:?}"),
        };
        let sequence_after_first = state.sequences.clone();

        // Retry with the same slug → returns the existing vault_id, sequence
        // counter does NOT advance, no orphan body created.
        let (resp2, _) = store.apply_org(&req, &mut state);
        let vault_id_2 = match resp2 {
            LedgerResponse::VaultCreated { vault, slug: out } => {
                assert_eq!(out, slug);
                vault
            },
            other => panic!("expected VaultCreated on retry, got {other:?}"),
        };

        assert_eq!(
            vault_id_1, vault_id_2,
            "retry must return the same vault_id (idempotent by slug)"
        );
        assert_eq!(
            state.sequences, sequence_after_first,
            "retry must not advance the per-org sequence counter"
        );

        // Only one entry in `state.vaults` under this slug.
        let slug_entries = state
            .vaults
            .iter()
            .filter(|((org, _), meta)| *org == organization && meta.slug == slug)
            .count();
        assert_eq!(slug_entries, 1, "retry must not create a duplicate body");
    }

    /// CreateOrganization is idempotent by `slug`.
    ///
    /// The SDK's `call_with_retry` on a lost response re-issues the same
    /// `CreateOrganizationRequest` with the same client-generated slug; the
    /// handler threads that slug into the saga input, so a re-submitted
    /// `SystemRequest::CreateOrganization` proposal must return the existing
    /// `OrganizationId` instead of allocating a new one. Without idempotency
    /// the GLOBAL apply would allocate a fresh `OrganizationId` each retry,
    /// leaving orphan `OrganizationMeta` rows and skewing the sequence
    /// counter. This test pins the idempotency contract: same slug → same
    /// `OrganizationId`, `slug_index` holds a single entry, sequence counter
    /// not advanced.
    #[tokio::test]
    async fn create_organization_is_idempotent_by_slug() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let slug = inferadb_ledger_types::OrganizationSlug::new(0xAAAA_BBBB);
        let req = SystemRequest::CreateOrganization {
            slug,
            region: Region::US_EAST_VA,
            tier: Default::default(),
            admin: UserId::new(1),
        };

        let (resp1, _) = store.apply_system(&req, &mut state);
        let org_id_1 = match resp1 {
            LedgerResponse::OrganizationCreated { organization_id, organization_slug } => {
                assert_eq!(organization_slug, slug);
                organization_id
            },
            other => panic!("expected OrganizationCreated, got {other:?}"),
        };
        let sequence_after_first = state.sequences.clone();

        // Retry with the same slug → same OrganizationId, sequence counter
        // does NOT advance, no orphan body.
        let (resp2, _) = store.apply_system(&req, &mut state);
        let org_id_2 = match resp2 {
            LedgerResponse::OrganizationCreated { organization_id, organization_slug } => {
                assert_eq!(organization_slug, slug);
                organization_id
            },
            other => panic!("expected OrganizationCreated on retry, got {other:?}"),
        };

        assert_eq!(org_id_1, org_id_2, "retry must return the same organization_id");
        assert_eq!(
            state.sequences, sequence_after_first,
            "retry must not advance the sequence counter"
        );
        assert_eq!(state.slug_index.get(&slug), Some(&org_id_1));
        let slug_hits = state.organizations.iter().filter(|(_, meta)| meta.slug == slug).count();
        assert_eq!(slug_hits, 1, "retry must not create a duplicate organization body");
    }

    /// CreateOrganizationTeam is idempotent by `(organization, slug)`.
    ///
    /// The SDK's `call_with_retry` on a lost response re-issues
    /// `CreateOrganizationTeamRequest` with the same client-generated slug.
    /// Without idempotency the apply arm would report `AlreadyExists` on the
    /// retry and the allocation from the first (successful-but-response-
    /// lost) call would be invisible to the client. Pinning the contract:
    /// same slug → same `TeamId`, `team_slug_index` holds a single entry,
    /// sequence counter not advanced.
    #[tokio::test]
    async fn create_organization_team_is_idempotent_by_slug() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let organization = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let slug = TeamSlug::new(0x1234_5678);
        let req = OrganizationRequest::CreateOrganizationTeam { organization, slug };

        let (resp1, _) = store.apply_org(&req, &mut state);
        let team_id_1 = match resp1 {
            LedgerResponse::OrganizationTeamCreated { team_id, team_slug } => {
                assert_eq!(team_slug, slug);
                team_id
            },
            other => panic!("expected OrganizationTeamCreated, got {other:?}"),
        };
        let sequence_after_first = state.sequences.clone();

        // Retry with the same slug → same TeamId, sequence counter does NOT
        // advance, no orphan directory entry.
        let (resp2, _) = store.apply_org(&req, &mut state);
        let team_id_2 = match resp2 {
            LedgerResponse::OrganizationTeamCreated { team_id, team_slug } => {
                assert_eq!(team_slug, slug);
                team_id
            },
            other => panic!("expected OrganizationTeamCreated on retry, got {other:?}"),
        };

        assert_eq!(team_id_1, team_id_2, "retry must return the same team_id");
        assert_eq!(
            state.sequences, sequence_after_first,
            "retry must not advance the per-org sequence counter"
        );
        assert_eq!(state.team_slug_index.get(&slug), Some(&(organization, team_id_1)));
    }

    /// CreateApp is idempotent by `(organization, slug)`.
    ///
    /// The SDK's `call_with_retry` on a lost response re-issues
    /// `CreateAppRequest` with the same client-generated slug. Without
    /// idempotency the apply arm would report `AlreadyExists` on the retry
    /// and the allocation from the first (successful-but-response-lost) call
    /// would be invisible to the client. Pinning the contract: same slug →
    /// same `AppId`, `app_slug_index` holds a single entry, sequence counter
    /// not advanced.
    #[tokio::test]
    async fn create_app_is_idempotent_by_slug() {
        let dir = tempdir().expect("create temp dir");
        let store = create_store_with_state_layer(&dir);
        let mut state = (*store.applied_state.load_full()).clone();

        let organization = create_active_organization(
            &store,
            &mut state,
            inferadb_ledger_types::OrganizationSlug::new(100),
            Region::US_EAST_VA,
        );

        let slug = inferadb_ledger_types::AppSlug::new(0xDEAD_BEEF);
        let req = OrganizationRequest::CreateApp { organization, slug };

        let (resp1, _) = store.apply_org(&req, &mut state);
        let app_id_1 = match resp1 {
            LedgerResponse::AppCreated { app_id, app_slug } => {
                assert_eq!(app_slug, slug);
                app_id
            },
            other => panic!("expected AppCreated, got {other:?}"),
        };
        let sequence_after_first = state.sequences.clone();

        // Retry with the same slug → same AppId, sequence counter does NOT
        // advance, no orphan directory entry.
        let (resp2, _) = store.apply_org(&req, &mut state);
        let app_id_2 = match resp2 {
            LedgerResponse::AppCreated { app_id, app_slug } => {
                assert_eq!(app_slug, slug);
                app_id
            },
            other => panic!("expected AppCreated on retry, got {other:?}"),
        };

        assert_eq!(app_id_1, app_id_2, "retry must return the same app_id");
        assert_eq!(
            state.sequences, sequence_after_first,
            "retry must not advance the per-org sequence counter"
        );
        assert_eq!(state.app_slug_index.get(&slug), Some(&(organization, app_id_1)));
    }

    #[tokio::test]
    async fn replay_crash_gap_is_idempotent_across_double_call() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");
        let mut store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_organization_id(OrganizationId::new(1));

        let shard_id = inferadb_ledger_consensus::types::ConsensusStateId(99);

        let mut wal = inferadb_ledger_consensus::wal::InMemoryWalBackend::new();
        use inferadb_ledger_consensus::wal_backend::WalBackend;
        wal.append(&[replay_wal_frame(shard_id, 1, 1), replay_wal_frame(shard_id, 2, 1)])
            .expect("append frames");
        wal.write_checkpoint(&inferadb_ledger_consensus::wal_backend::CheckpointFrame {
            committed_index: 2,
            term: 1,
            voted_for: None,
        })
        .expect("write checkpoint");
        wal.sync().expect("sync");

        // First call replays everything.
        let stats1 = store.replay_crash_gap::<inferadb_ledger_consensus::wal::InMemoryWalBackend, OrganizationRequest>(&wal, shard_id).await.expect("replay #1");
        assert_eq!(stats1.replayed_entries, 2, "first call replays both entries");

        // Second call: applied_durable is now 2, so nothing to replay.
        let stats2 = store.replay_crash_gap::<inferadb_ledger_consensus::wal::InMemoryWalBackend, OrganizationRequest>(&wal, shard_id).await.expect("replay #2");
        assert_eq!(stats2.replayed_entries, 0, "idempotent: second call must replay zero entries",);
        assert_eq!(stats2.applied_durable, 2);
        assert_eq!(stats2.last_committed, 2);
    }

    // =========================================================================
    // Property-based idempotency for IngestExternalEvents
    // =========================================================================

    /// Arbitrary sequences of `IngestExternalEvents` proposals are replay-safe:
    /// applying the same sequence of proposals twice produces byte-identical
    /// `events.db` state. Sweeps the scalar property covered by
    /// [`apply_ingest_external_events_replay_idempotency`] across arbitrary
    /// proposal counts, batch sizes, and entry payloads.
    ///
    /// `EventStore::write` upserts via `BTree::insert`, so re-applying an
    /// identical `(source, Vec<EventEntry>)` proposal must be a no-op at the
    /// b-tree level. A counterexample would expose a real idempotency bug in
    /// the apply path.
    mod proptest_ingest_replay_idempotency {
        use inferadb_ledger_store::tables::{Entities, Relationships};
        use proptest::prelude::*;

        use super::*;

        /// (primary-table rows, secondary-index rows) — byte-level snapshot
        /// of an `EventsDatabase` used for equality assertions.
        type EventsSnapshot = (Vec<(Vec<u8>, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>)>);

        /// Reads every (key, value) pair from both events-db tables and
        /// returns them as owned byte vectors. Since `events.db` re-uses
        /// `TableId::Entities` for the primary Events table and
        /// `TableId::Relationships` for the EventIndex secondary, the
        /// store-level `tables::Entities` / `tables::Relationships`
        /// accessors give us the full byte-level snapshot.
        fn snapshot_events_db(db: &EventsDatabase<FileBackend>) -> EventsSnapshot {
            let txn = db.read().expect("read txn");
            let events: Vec<(Vec<u8>, Vec<u8>)> =
                txn.iter::<Entities>().expect("iter events").collect();
            let indices: Vec<(Vec<u8>, Vec<u8>)> =
                txn.iter::<Relationships>().expect("iter index").collect();
            (events, indices)
        }

        /// Coerces an arbitrary `EventEntry` into the shape the RPC handler
        /// produces: `Organization` scope, `HandlerPhase` emission. Leaves
        /// every other field untouched so proptest still explores the full
        /// value-space exposed by `arb_event_entry`. Residency-backstop
        /// `debug_assert` in the apply handler requires a REGIONAL shard,
        /// which the fixture already satisfies.
        fn coerce_external(entry: EventEntry) -> EventEntry {
            let mut e = entry;
            e.scope = EventScope::Organization;
            e.emission = inferadb_ledger_types::events::EventEmission::HandlerPhase { node_id: 1 };
            e
        }

        /// Arbitrary proposal list: 1..20 proposals, each carrying
        /// 1..15 external events. `organization_id` is overwritten
        /// post-generation so every entry targets the fixture's shared
        /// org — maximising key-space overlap across proposals, which
        /// is the stress case for b-tree upsert idempotency.
        fn arb_proposal_list() -> impl Strategy<Value = Vec<(String, Vec<EventEntry>)>> {
            let entries = proptest::collection::vec(
                inferadb_ledger_test_utils::strategies::arb_event_entry(),
                1..15usize,
            )
            .prop_map(|batch| batch.into_iter().map(coerce_external).collect::<Vec<_>>());
            let proposal = ("[a-z]{3,10}", entries);
            proptest::collection::vec(proposal, 1..20usize)
        }

        proptest! {
            // 32 cases: each case opens a tempdir-backed RaftLogStore +
            // EventsDatabase and applies up to ~300 events twice. Default
            // 256 would dominate the raft-crate lib-test wall clock.
            #![proptest_config(ProptestConfig::with_cases(32))]

            /// Applying a proposal sequence once vs. twice produces byte-
            /// identical events.db state (both `Events` and `EventIndex`
            /// tables). Exercises the `IngestExternalEvents` apply handler
            /// over a WAL-replay-shaped repeat.
            #[test]
            fn ingest_external_events_proposal_replay_safe(
                mut proposals in arb_proposal_list(),
            ) {
                // Reuses the existing fixture helpers (`fixed_timestamp`,
                // `setup_org_and_vault`, `make_external_event_entry`) to
                // stay aligned with the unit test that this proptest
                // generalises.
                let dir = tempdir().expect("create temp dir");
                let events_db = EventsDatabase::open(dir.path()).expect("open events db");
                let events_db_arc = Arc::new(events_db);
                let config = EventConfig::default();
                let writer = crate::event_writer::EventWriter::new(
                    Arc::clone(&events_db_arc),
                    config,
                );
                let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
                    .expect("open store")
                    .with_region_config(
                        Region::US_EAST_VA,
                        inferadb_ledger_types::NodeId::new("node-test"),
                        1,
                    )
                    .with_event_writer(writer);

                let mut state = (*store.applied_state.load_full()).clone();
                let (org_id, _vault_id) = setup_org_and_vault(&mut state);

                // Route every generated event at the fixture's shared org.
                for (_, batch) in proposals.iter_mut() {
                    for entry in batch.iter_mut() {
                        entry.organization_id = org_id;
                    }
                }

                let ts = fixed_timestamp();

                // Phase 1: apply every proposal once.
                for (source, batch) in &proposals {
                    let request = OrganizationRequest::IngestExternalEvents {
                        source: source.clone(),
                        events: batch.clone(),
                    };
                    let mut events: Vec<EventEntry> = Vec::new();
                    let mut op_index = 0u32;
                    let (response, vault_entry) = store.apply_organization_request_with_events(
                        &request,
                        &mut state,
                        ts,
                        &mut op_index,
                        &mut events,
                        90,
                        &mut PendingExternalWrites::default(),
                        None,
                        false,
                        0,
                        false,
                    );
                    prop_assert_eq!(
                        response,
                        LedgerResponse::Empty,
                        "phase 1: IngestExternalEvents must return Empty on success"
                    );
                    prop_assert!(
                        vault_entry.is_none(),
                        "phase 1: external events produce no vault entry"
                    );
                    prop_assert!(
                        events.is_empty(),
                        "phase 1: external events must not emit apply-phase events"
                    );
                }
                let snap_phase1 = snapshot_events_db(&events_db_arc);

                // Phase 2: re-apply every proposal in the same order. Upsert
                // semantics make this a logical no-op — the b-tree layout
                // must be byte-identical after the replay pass.
                for (source, batch) in &proposals {
                    let request = OrganizationRequest::IngestExternalEvents {
                        source: source.clone(),
                        events: batch.clone(),
                    };
                    let mut events: Vec<EventEntry> = Vec::new();
                    let mut op_index = 0u32;
                    let (response, _vault_entry) = store.apply_organization_request_with_events(
                        &request,
                        &mut state,
                        ts,
                        &mut op_index,
                        &mut events,
                        90,
                        &mut PendingExternalWrites::default(),
                        None,
                        false,
                        0,
                        false,
                    );
                    prop_assert_eq!(
                        response,
                        LedgerResponse::Empty,
                        "phase 2: replay must also return Empty"
                    );
                }
                let snap_phase2 = snapshot_events_db(&events_db_arc);

                prop_assert_eq!(
                    snap_phase1.0.len(),
                    snap_phase2.0.len(),
                    "Events table row count must match after proposal replay"
                );
                prop_assert_eq!(
                    &snap_phase1.0,
                    &snap_phase2.0,
                    "Events table must be byte-identical after proposal replay"
                );
                prop_assert_eq!(
                    snap_phase1.1.len(),
                    snap_phase2.1.len(),
                    "EventIndex table row count must match after proposal replay"
                );
                prop_assert_eq!(
                    &snap_phase1.1,
                    &snap_phase2.1,
                    "EventIndex table must be byte-identical after proposal replay"
                );
            }
        }
    }
}
