//! Raft storage implementation using inferadb-ledger-store.
//!
//! This module provides the persistent storage for Raft log entries,
//! vote state, committed log tracking, and state machine state.
//!
//! We use the deprecated but non-sealed `RaftStorage` trait which combines
//! log storage and state machine into one implementation. The v2 traits
//! (`RaftLogStorage`, `RaftStateMachine`) are sealed in OpenRaft 0.9.
//!
//! Each shard group has its own storage located at:
//! `shards/{shard_id}/raft/log.db`
//!
//! # Lock Ordering Convention
//!
//! To prevent deadlocks, locks in this module must be acquired in the following order:
//!
//! 1. `applied_state` - Raft state machine state
//! 2. `shard_chain` - Shard chain tracking (height + previous hash)
//! 3. `vote_cache`, `last_purged_cache` - Caches (independent, no ordering requirement)
//!
//! The `shard_chain` lock consolidates `shard_height` and `previous_shard_hash`
//! into a single lock to eliminate internal ordering issues.

mod accessor;
mod operations;
mod raft_impl;
mod store;
mod types;

pub use accessor::*;
use inferadb_ledger_types::Hash;
pub use raft_impl::LedgerSnapshotBuilder;
pub use store::*;
pub use types::*;

// ============================================================================
// Metadata Keys
// ============================================================================

// Metadata keys for RaftState table
const KEY_VOTE: &str = "vote";
#[allow(dead_code)] // reserved for save_committed/read_committed
const KEY_COMMITTED: &str = "committed";
const KEY_LAST_PURGED: &str = "last_purged";
const KEY_APPLIED_STATE: &str = "applied_state";

// ============================================================================
// Shard Chain State (Lock Consolidated)
// ============================================================================

/// Shard chain tracking state.
///
/// These fields are grouped into a single lock to avoid lock ordering issues.
/// They track the shard-level blockchain state for creating ShardBlocks.
#[derive(Debug, Clone, Copy, Default)]
pub struct ShardChainState {
    /// Current shard height for block creation.
    pub height: u64,
    /// Previous shard hash for chain continuity.
    pub previous_hash: Hash,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use chrono::{DateTime, Utc};
    use inferadb_ledger_state::{EventStore, EventsDatabase, system::OrganizationStatus};
    use inferadb_ledger_store::{FileBackend, tables};
    use inferadb_ledger_types::{
        Operation, OrganizationId, ShardId, Transaction, VaultId, VaultSlug,
        events::{EventAction, EventConfig, EventEntry, EventScope},
    };
    use openraft::{
        CommittedLeaderId, Entry, EntryPayload, LogId, RaftStorage, Vote, storage::RaftLogReader,
    };
    use tempfile::tempdir;
    use tokio::sync::broadcast;

    use super::{types::estimate_write_storage_delta, *};
    use crate::types::{
        BlockRetentionPolicy, LedgerNodeId, LedgerRequest, LedgerResponse, LedgerTypeConfig,
        SystemRequest,
    };

    /// Helper to create log IDs for tests.
    #[cfg(test)]
    fn make_log_id(term: u64, index: u64) -> LogId<LedgerNodeId> {
        LogId::new(CommittedLeaderId::new(term, 0), index)
    }

    #[tokio::test]
    async fn test_log_store_open() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");

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

        let mut store = RaftLogStore::<FileBackend>::open(&path).expect("open store");

        let vote = Vote::new(1, 42);
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
        assert_eq!(counters.next_user(), 1);
        assert_eq!(counters.next_user(), 2);
        assert_eq!(counters.next_user_email(), 1);
        assert_eq!(counters.next_user_email(), 2);
        assert_eq!(counters.next_email_verify(), 1);
        assert_eq!(counters.next_email_verify(), 2);
    }

    #[tokio::test]
    async fn test_apply_create_organization() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        let request = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: None,
            quota: None,
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => {
                assert_eq!(organization_id, OrganizationId::new(1));
            },
            _ => panic!("unexpected response"),
        }
    }

    #[test]
    fn test_select_least_loaded_shard_empty() {
        let organizations = HashMap::new();
        assert_eq!(select_least_loaded_shard(&organizations), ShardId::new(0));
    }

    #[test]
    fn test_select_least_loaded_shard_single_shard() {
        let mut organizations = HashMap::new();
        organizations.insert(
            OrganizationId::new(1),
            OrganizationMeta {
                organization_id: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns1".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        organizations.insert(
            OrganizationId::new(2),
            OrganizationMeta {
                organization_id: OrganizationId::new(2),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns2".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        // Only shard 0 exists, so it should be selected
        assert_eq!(select_least_loaded_shard(&organizations), ShardId::new(0));
    }

    #[test]
    fn test_select_least_loaded_shard_equal_load_prefers_lower_id() {
        let mut organizations = HashMap::new();
        // Shard 0: 2 organizations
        organizations.insert(
            OrganizationId::new(1),
            OrganizationMeta {
                organization_id: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns1".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        organizations.insert(
            OrganizationId::new(2),
            OrganizationMeta {
                organization_id: OrganizationId::new(2),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns2".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        // Shard 1: 2 organizations (equal load)
        organizations.insert(
            OrganizationId::new(3),
            OrganizationMeta {
                organization_id: OrganizationId::new(3),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns3".to_string(),
                shard_id: ShardId::new(1),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        organizations.insert(
            OrganizationId::new(4),
            OrganizationMeta {
                organization_id: OrganizationId::new(4),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns4".to_string(),
                shard_id: ShardId::new(1),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        // Tie-breaker: lower shard_id wins
        assert_eq!(select_least_loaded_shard(&organizations), ShardId::new(0));
    }

    #[test]
    fn test_select_least_loaded_shard_unequal_load() {
        let mut organizations = HashMap::new();
        // Shard 0: 3 organizations
        organizations.insert(
            OrganizationId::new(1),
            OrganizationMeta {
                organization_id: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns1".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        organizations.insert(
            OrganizationId::new(2),
            OrganizationMeta {
                organization_id: OrganizationId::new(2),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns2".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        organizations.insert(
            OrganizationId::new(3),
            OrganizationMeta {
                organization_id: OrganizationId::new(3),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns3".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        // Shard 1: 1 organization (lighter)
        organizations.insert(
            OrganizationId::new(4),
            OrganizationMeta {
                organization_id: OrganizationId::new(4),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns4".to_string(),
                shard_id: ShardId::new(1),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        // Shard 1 has fewer organizations
        assert_eq!(select_least_loaded_shard(&organizations), ShardId::new(1));
    }

    #[test]
    fn test_select_least_loaded_shard_ignores_deleted() {
        let mut organizations = HashMap::new();
        // Shard 0: 1 active, 2 deleted
        organizations.insert(
            OrganizationId::new(1),
            OrganizationMeta {
                organization_id: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns1".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        organizations.insert(
            OrganizationId::new(2),
            OrganizationMeta {
                organization_id: OrganizationId::new(2),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns2".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Deleted,
                pending_shard_id: None,
                quota: None,
            },
        );
        organizations.insert(
            OrganizationId::new(3),
            OrganizationMeta {
                organization_id: OrganizationId::new(3),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns3".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Deleted,
                pending_shard_id: None,
                quota: None,
            },
        );
        // Shard 1: 2 active
        organizations.insert(
            OrganizationId::new(4),
            OrganizationMeta {
                organization_id: OrganizationId::new(4),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns4".to_string(),
                shard_id: ShardId::new(1),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        organizations.insert(
            OrganizationId::new(5),
            OrganizationMeta {
                organization_id: OrganizationId::new(5),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "ns5".to_string(),
                shard_id: ShardId::new(1),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        // Shard 0 has only 1 active organization (deleted don't count)
        assert_eq!(select_least_loaded_shard(&organizations), ShardId::new(0));
    }

    #[test]
    fn test_select_least_loaded_shard_many_shards() {
        let mut organizations = HashMap::new();
        // Shard 0: 5 organizations
        for i in 1..=5i64 {
            organizations.insert(
                OrganizationId::new(i),
                OrganizationMeta {
                    organization_id: OrganizationId::new(i),
                    slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    name: format!("ns{}", i),
                    shard_id: ShardId::new(0),
                    status: OrganizationStatus::Active,
                    pending_shard_id: None,
                    quota: None,
                },
            );
        }
        // Shard 1: 3 organizations
        for i in 6..=8i64 {
            organizations.insert(
                OrganizationId::new(i),
                OrganizationMeta {
                    organization_id: OrganizationId::new(i),
                    slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    name: format!("ns{}", i),
                    shard_id: ShardId::new(1),
                    status: OrganizationStatus::Active,
                    pending_shard_id: None,
                    quota: None,
                },
            );
        }
        // Shard 2: 2 organizations (minimum)
        for i in 9..=10i64 {
            organizations.insert(
                OrganizationId::new(i),
                OrganizationMeta {
                    organization_id: OrganizationId::new(i),
                    slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    name: format!("ns{}", i),
                    shard_id: ShardId::new(2),
                    status: OrganizationStatus::Active,
                    pending_shard_id: None,
                    quota: None,
                },
            );
        }
        // Shard 2 has the fewest organizations
        assert_eq!(select_least_loaded_shard(&organizations), ShardId::new(2));
    }

    #[tokio::test]
    async fn test_apply_create_organization_load_balanced() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Pre-populate with organizations on different shards
        // Shard 0: 3 organizations
        for i in 1..=3i64 {
            state.organizations.insert(
                OrganizationId::new(i),
                OrganizationMeta {
                    organization_id: OrganizationId::new(i),
                    slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    name: format!("existing-ns-{}", i),
                    shard_id: ShardId::new(0),
                    status: OrganizationStatus::Active,
                    pending_shard_id: None,
                    quota: None,
                },
            );
        }
        // Shard 1: 1 organization
        state.organizations.insert(
            OrganizationId::new(4),
            OrganizationMeta {
                organization_id: OrganizationId::new(4),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "existing-ns-4".to_string(),
                shard_id: ShardId::new(1),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        state.sequences.organization = OrganizationId::new(5); // Next ID is 5

        drop(state);

        let mut state = store.applied_state.write();
        let request = LedgerRequest::CreateOrganization {
            name: "new-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: None,
            quota: None,
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::OrganizationCreated { organization_id, shard_id } => {
                assert_eq!(organization_id, OrganizationId::new(5));
                // Should be assigned to shard 1 (fewer organizations)
                assert_eq!(shard_id, ShardId::new(1), "Should assign to least-loaded shard");
            },
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_apply_create_organization_explicit_shard_overrides() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Pre-populate: shard 0 has fewer organizations
        state.organizations.insert(
            OrganizationId::new(1),
            OrganizationMeta {
                organization_id: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                name: "existing".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        state.sequences.organization = OrganizationId::new(2);

        // Request explicit shard 5 (even though it would be "heavy" if it existed)
        let request = LedgerRequest::CreateOrganization {
            name: "new-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(5)),
            quota: None,
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::OrganizationCreated { organization_id, shard_id } => {
                assert_eq!(organization_id, OrganizationId::new(2));
                // Explicit shard_id should override load balancing
                assert_eq!(
                    shard_id,
                    ShardId::new(5),
                    "Explicit shard_id should override load balancing"
                );
            },
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_apply_create_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        let request = LedgerRequest::CreateVault {
            organization_id: OrganizationId::new(1),
            slug: VaultSlug::new(1),
            name: Some("test-vault".to_string()),
            retention_policy: None,
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::VaultCreated { vault_id, .. } => {
                assert_eq!(vault_id, VaultId::new(1));
                assert_eq!(
                    state.vault_heights.get(&(OrganizationId::new(1), VaultId::new(1))),
                    Some(&0)
                );
            },
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_diverged_vault_rejects_writes() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Mark vault as diverged
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Diverged { expected: [1u8; 32], computed: [2u8; 32], at_height: 10 },
        );

        let request = LedgerRequest::Write {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            transactions: vec![],
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("diverged"));
            },
            _ => panic!("expected error response"),
        }
    }

    #[tokio::test]
    async fn test_update_vault_health_to_healthy() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Start with a diverged vault
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Diverged { expected: [1u8; 32], computed: [2u8; 32], at_height: 10 },
        );

        // Update to healthy
        let request = LedgerRequest::UpdateVaultHealth {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            healthy: true,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: None,
            recovery_started_at: None,
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Start healthy
        state
            .vault_health
            .insert((OrganizationId::new(1), VaultId::new(1)), VaultHealthStatus::Healthy);

        // Update to diverged
        let request = LedgerRequest::UpdateVaultHealth {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            healthy: false,
            expected_root: Some([0xAA; 32]),
            computed_root: Some([0xBB; 32]),
            diverged_at_height: Some(42),
            recovery_attempt: None,
            recovery_started_at: None,
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Start with a diverged vault
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Diverged { expected: [1u8; 32], computed: [2u8; 32], at_height: 10 },
        );

        // Update to recovering
        let request = LedgerRequest::UpdateVaultHealth {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            healthy: false,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: Some(1),
            recovery_started_at: Some(chrono::Utc::now().timestamp()),
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

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

        // Test recovery attempt 2 (circuit breaker)
        let request = LedgerRequest::UpdateVaultHealth {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            healthy: false,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: Some(2),
            recovery_started_at: Some(chrono::Utc::now().timestamp()),
        };

        let (response, _vault_entry) = store.apply_request(&request, &mut state);

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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Create two vaults
        let create_vault1 = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault1".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault1, &mut state);
        let vault1_id = match response {
            LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        let create_vault2 = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault2".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault2, &mut state);
        let vault2_id = match response {
            LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Try to delete organization - should transition to Deleting with blocking vault IDs
        let delete_ns = LedgerRequest::DeleteOrganization { organization_id };
        let (response, _) = store.apply_request(&delete_ns, &mut state);

        match response {
            LedgerResponse::OrganizationDeleting { organization_id: ns_id, blocking_vault_ids } => {
                assert_eq!(ns_id, organization_id);
                assert_eq!(blocking_vault_ids.len(), 2);
                assert!(blocking_vault_ids.contains(&vault1_id));
                assert!(blocking_vault_ids.contains(&vault2_id));
            },
            _ => panic!("expected OrganizationDeleting"),
        }

        // Verify organization is now in Deleting state
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleting
        );
    }

    #[tokio::test]
    async fn test_delete_organization_succeeds_after_vaults_deleted() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Create vault
        let create_vault = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault, &mut state);
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Delete vault first
        let delete_vault = LedgerRequest::DeleteVault { organization_id, vault_id };
        let (response, _) = store.apply_request(&delete_vault, &mut state);
        match response {
            LedgerResponse::VaultDeleted { success } => assert!(success),
            _ => panic!("expected VaultDeleted"),
        }

        // Now delete organization - should succeed
        let delete_ns = LedgerRequest::DeleteOrganization { organization_id };
        let (response, _) = store.apply_request(&delete_ns, &mut state);

        match response {
            LedgerResponse::OrganizationDeleted { success, blocking_vault_ids } => {
                assert!(success);
                assert!(blocking_vault_ids.is_empty());
            },
            _ => panic!("expected OrganizationDeleted"),
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization with no vaults
        let create_ns = LedgerRequest::CreateOrganization {
            name: "empty-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Delete organization immediately - should succeed (no vaults)
        let delete_ns = LedgerRequest::DeleteOrganization { organization_id };
        let (response, _) = store.apply_request(&delete_ns, &mut state);

        match response {
            LedgerResponse::OrganizationDeleted { success, blocking_vault_ids } => {
                assert!(success);
                assert!(blocking_vault_ids.is_empty());
            },
            _ => panic!("expected OrganizationDeleted"),
        }
    }

    #[tokio::test]
    async fn test_delete_organization_not_found() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Try to delete non-existent organization
        let delete_ns =
            LedgerRequest::DeleteOrganization { organization_id: OrganizationId::new(999) };
        let (response, _) = store.apply_request(&delete_ns, &mut state);

        match response {
            LedgerResponse::Error { message } => {
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Create two vaults
        let create_vault1 = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault1".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault1, &mut state);
        let vault1_id = match response {
            LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        let create_vault2 = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault2".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault2, &mut state);
        let vault2_id = match response {
            LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Delete vault1
        let delete_vault = LedgerRequest::DeleteVault { organization_id, vault_id: vault1_id };
        let (response, _) = store.apply_request(&delete_vault, &mut state);
        match response {
            LedgerResponse::VaultDeleted { success } => assert!(success),
            _ => panic!("expected VaultDeleted"),
        }

        // Try to delete organization - should transition to Deleting, but only vault2 should be in
        // blocking list
        let delete_ns = LedgerRequest::DeleteOrganization { organization_id };
        let (response, _) = store.apply_request(&delete_ns, &mut state);

        match response {
            LedgerResponse::OrganizationDeleting { organization_id: ns_id, blocking_vault_ids } => {
                assert_eq!(ns_id, organization_id);
                assert_eq!(blocking_vault_ids.len(), 1);
                assert!(blocking_vault_ids.contains(&vault2_id));
                // vault1 was deleted, so it should NOT be in blocking list
                assert!(!blocking_vault_ids.contains(&vault1_id));
            },
            _ => panic!("expected OrganizationDeleting"),
        }

        // Verify organization is now in Deleting state
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleting
        );
    }

    // ========================================================================
    // Organization Migration Tests
    // ========================================================================
    //
    // These tests verify organization migration behavior via
    // SystemRequest::UpdateOrganizationRouting. Migration changes the shard_id for a
    // organization, updating routing without data movement.

    #[tokio::test]
    async fn test_migrate_organization_success() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization on shard 0
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, shard_id } => {
                assert_eq!(shard_id, ShardId::new(0));
                organization_id
            },
            _ => panic!("expected OrganizationCreated"),
        };

        // Verify initial state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.shard_id, ShardId::new(0));
        assert_eq!(meta.status, OrganizationStatus::Active);

        // Migrate to shard 1
        let migrate = LedgerRequest::System(SystemRequest::UpdateOrganizationRouting {
            organization_id,
            shard_id: 1,
        });
        let (response, _) = store.apply_request(&migrate, &mut state);

        match response {
            LedgerResponse::OrganizationMigrated {
                organization_id: ns_id,
                old_shard_id,
                new_shard_id,
            } => {
                assert_eq!(ns_id, organization_id);
                assert_eq!(old_shard_id, ShardId::new(0));
                assert_eq!(new_shard_id, ShardId::new(1));
            },
            _ => panic!("expected OrganizationMigrated, got {:?}", response),
        }

        // Verify updated state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.shard_id, ShardId::new(1), "shard_id should be updated");
        assert_eq!(meta.status, OrganizationStatus::Active, "status should remain unchanged");
    }

    #[tokio::test]
    async fn test_migrate_organization_not_found() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Try to migrate non-existent organization
        let migrate = LedgerRequest::System(SystemRequest::UpdateOrganizationRouting {
            organization_id: OrganizationId::new(999),
            shard_id: 1,
        });
        let (response, _) = store.apply_request(&migrate, &mut state);

        match response {
            LedgerResponse::Error { message } => {
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create and delete organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "deleted-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Delete the organization
        let delete_ns = LedgerRequest::DeleteOrganization { organization_id };
        let (response, _) = store.apply_request(&delete_ns, &mut state);
        match response {
            LedgerResponse::OrganizationDeleted { success, .. } => assert!(success),
            _ => panic!("expected OrganizationDeleted"),
        }

        // Verify organization is deleted
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.status, OrganizationStatus::Deleted);

        // Try to migrate deleted organization
        let migrate = LedgerRequest::System(SystemRequest::UpdateOrganizationRouting {
            organization_id,
            shard_id: 1,
        });
        let (response, _) = store.apply_request(&migrate, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("deleted"), "error should mention deleted organization");
            },
            _ => panic!("expected Error response, got {:?}", response),
        }
    }

    #[tokio::test]
    async fn test_migrate_organization_negative_shard_id() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Try to migrate to invalid negative shard_id
        let migrate = LedgerRequest::System(SystemRequest::UpdateOrganizationRouting {
            organization_id,
            shard_id: -1,
        });
        let (response, _) = store.apply_request(&migrate, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("-1"), "error should mention the invalid shard_id");
            },
            _ => panic!("expected Error response, got {:?}", response),
        }

        // Verify shard_id remains unchanged
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.shard_id, ShardId::new(0), "shard_id should remain unchanged after error");
    }

    #[tokio::test]
    async fn test_migrate_organization_idempotent_same_shard() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization on shard 0
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, shard_id } => {
                assert_eq!(shard_id, ShardId::new(0));
                organization_id
            },
            _ => panic!("expected OrganizationCreated"),
        };

        // Migrate to same shard (idempotent case)
        let migrate = LedgerRequest::System(SystemRequest::UpdateOrganizationRouting {
            organization_id,
            shard_id: 0,
        });
        let (response, _) = store.apply_request(&migrate, &mut state);

        match response {
            LedgerResponse::OrganizationMigrated {
                organization_id: ns_id,
                old_shard_id,
                new_shard_id,
            } => {
                assert_eq!(ns_id, organization_id);
                assert_eq!(old_shard_id, ShardId::new(0));
                assert_eq!(new_shard_id, ShardId::new(0), "should be idempotent - same shard");
            },
            _ => panic!("expected OrganizationMigrated (idempotent), got {:?}", response),
        }

        // Verify state remains consistent
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.shard_id, ShardId::new(0));
        assert_eq!(meta.status, OrganizationStatus::Active);
    }

    #[tokio::test]
    async fn test_migrate_organization_multiple_times() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "migrating-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Migration 1: shard 0 -> shard 1
        let migrate1 = LedgerRequest::System(SystemRequest::UpdateOrganizationRouting {
            organization_id,
            shard_id: 1,
        });
        let (response, _) = store.apply_request(&migrate1, &mut state);
        match response {
            LedgerResponse::OrganizationMigrated { old_shard_id, new_shard_id, .. } => {
                assert_eq!(old_shard_id, ShardId::new(0));
                assert_eq!(new_shard_id, ShardId::new(1));
            },
            _ => panic!("expected OrganizationMigrated"),
        }

        // Migration 2: shard 1 -> shard 2
        let migrate2 = LedgerRequest::System(SystemRequest::UpdateOrganizationRouting {
            organization_id,
            shard_id: 2,
        });
        let (response, _) = store.apply_request(&migrate2, &mut state);
        match response {
            LedgerResponse::OrganizationMigrated { old_shard_id, new_shard_id, .. } => {
                assert_eq!(old_shard_id, ShardId::new(1));
                assert_eq!(new_shard_id, ShardId::new(2));
            },
            _ => panic!("expected OrganizationMigrated"),
        }

        // Migration 3: shard 2 -> shard 0 (back to original)
        let migrate3 = LedgerRequest::System(SystemRequest::UpdateOrganizationRouting {
            organization_id,
            shard_id: 0,
        });
        let (response, _) = store.apply_request(&migrate3, &mut state);
        match response {
            LedgerResponse::OrganizationMigrated { old_shard_id, new_shard_id, .. } => {
                assert_eq!(old_shard_id, ShardId::new(2));
                assert_eq!(new_shard_id, ShardId::new(0));
            },
            _ => panic!("expected OrganizationMigrated"),
        }

        // Verify final state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.shard_id, ShardId::new(0));
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Verify initial state
        let meta = state.organizations.get(&organization_id).expect("organization should exist");
        assert_eq!(meta.status, OrganizationStatus::Active);

        // Suspend the organization
        let suspend = LedgerRequest::SuspendOrganization {
            organization_id,
            reason: Some("Payment overdue".to_string()),
        };
        let (response, _) = store.apply_request(&suspend, &mut state);

        match response {
            LedgerResponse::OrganizationSuspended { organization_id: ns_id } => {
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create and suspend organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        let suspend = LedgerRequest::SuspendOrganization { organization_id, reason: None };
        let (response, _) = store.apply_request(&suspend, &mut state);
        match response {
            LedgerResponse::OrganizationSuspended { .. } => {},
            _ => panic!("expected OrganizationSuspended"),
        }

        // Resume the organization
        let resume = LedgerRequest::ResumeOrganization { organization_id };
        let (response, _) = store.apply_request(&resume, &mut state);

        match response {
            LedgerResponse::OrganizationResumed { organization_id: ns_id } => {
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization on shard 0
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Start migration to shard 1
        let start_migration =
            LedgerRequest::StartMigration { organization_id, target_shard_id: ShardId::new(1) };
        let (response, _) = store.apply_request(&start_migration, &mut state);

        match response {
            LedgerResponse::MigrationStarted { organization_id: ns_id, target_shard_id } => {
                assert_eq!(ns_id, organization_id);
                assert_eq!(target_shard_id, ShardId::new(1));
            },
            _ => panic!("expected MigrationStarted"),
        }

        // Verify organization is in Migrating state with pending shard
        let ns = state.organizations.get(&organization_id).unwrap();
        assert_eq!(ns.status, OrganizationStatus::Migrating);
        assert_eq!(ns.shard_id, ShardId::new(0)); // Still on old shard
        assert_eq!(ns.pending_shard_id, Some(ShardId::new(1))); // Target shard stored
    }

    #[tokio::test]
    async fn test_start_migration_not_found() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        let start_migration = LedgerRequest::StartMigration {
            organization_id: OrganizationId::new(999),
            target_shard_id: ShardId::new(1),
        };
        let (response, _) = store.apply_request(&start_migration, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("not found"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_start_migration_already_migrating() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Start migration
        let start_migration =
            LedgerRequest::StartMigration { organization_id, target_shard_id: ShardId::new(1) };
        let (response, _) = store.apply_request(&start_migration, &mut state);
        assert!(matches!(response, LedgerResponse::MigrationStarted { .. }));

        // Try to start another migration - should fail
        let start_migration2 =
            LedgerRequest::StartMigration { organization_id, target_shard_id: ShardId::new(2) };
        let (response, _) = store.apply_request(&start_migration2, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("already migrating"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_start_migration_suspended_organization() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create and suspend organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        let suspend = LedgerRequest::SuspendOrganization { organization_id, reason: None };
        store.apply_request(&suspend, &mut state);

        // Try to start migration - should fail
        let start_migration =
            LedgerRequest::StartMigration { organization_id, target_shard_id: ShardId::new(1) };
        let (response, _) = store.apply_request(&start_migration, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("suspended"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_complete_migration_success() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization on shard 0
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Start migration to shard 1
        let start_migration =
            LedgerRequest::StartMigration { organization_id, target_shard_id: ShardId::new(1) };
        store.apply_request(&start_migration, &mut state);

        // Complete migration
        let complete_migration = LedgerRequest::CompleteMigration { organization_id };
        let (response, _) = store.apply_request(&complete_migration, &mut state);

        match response {
            LedgerResponse::MigrationCompleted {
                organization_id: ns_id,
                old_shard_id,
                new_shard_id,
            } => {
                assert_eq!(ns_id, organization_id);
                assert_eq!(old_shard_id, ShardId::new(0));
                assert_eq!(new_shard_id, ShardId::new(1));
            },
            _ => panic!("expected MigrationCompleted"),
        }

        // Verify organization is back to Active on new shard
        let ns = state.organizations.get(&organization_id).unwrap();
        assert_eq!(ns.status, OrganizationStatus::Active);
        assert_eq!(ns.shard_id, ShardId::new(1));
        assert_eq!(ns.pending_shard_id, None);
    }

    #[tokio::test]
    async fn test_complete_migration_not_migrating() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization (Active state)
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Try to complete migration without starting - should fail
        let complete_migration = LedgerRequest::CompleteMigration { organization_id };
        let (response, _) = store.apply_request(&complete_migration, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("not migrating"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_migrating_organization_blocks_writes() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization and vault
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        let create_vault = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault, &mut state);
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Start migration
        let start_migration =
            LedgerRequest::StartMigration { organization_id, target_shard_id: ShardId::new(1) };
        store.apply_request(&start_migration, &mut state);

        // Try to write - should be blocked
        let write = LedgerRequest::Write { organization_id, vault_id, transactions: vec![] };
        let (response, _) = store.apply_request(&write, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("migrating"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_migrating_organization_blocks_create_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Start migration
        let start_migration =
            LedgerRequest::StartMigration { organization_id, target_shard_id: ShardId::new(1) };
        store.apply_request(&start_migration, &mut state);

        // Try to create vault - should be blocked
        let create_vault = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("migrating"));
            },
            _ => panic!("expected Error"),
        }
    }

    // ========================================================================
    // Deleting State Tests
    // ========================================================================

    #[tokio::test]
    async fn test_deleting_organization_auto_transitions_on_last_vault_delete() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Create two vaults
        let create_vault1 = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault1".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault1, &mut state);
        let vault1_id = match response {
            LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        let create_vault2 = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault2".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault2, &mut state);
        let vault2_id = match response {
            LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Mark organization for deletion (transitions to Deleting)
        let delete_ns = LedgerRequest::DeleteOrganization { organization_id };
        let (response, _) = store.apply_request(&delete_ns, &mut state);
        assert!(matches!(response, LedgerResponse::OrganizationDeleting { .. }));
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleting
        );

        // Delete first vault - organization should still be Deleting
        let delete_vault1 = LedgerRequest::DeleteVault { organization_id, vault_id: vault1_id };
        let (response, _) = store.apply_request(&delete_vault1, &mut state);
        assert!(matches!(response, LedgerResponse::VaultDeleted { success: true }));
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleting
        );

        // Delete second (last) vault - organization should auto-transition to Deleted
        let delete_vault2 = LedgerRequest::DeleteVault { organization_id, vault_id: vault2_id };
        let (response, _) = store.apply_request(&delete_vault2, &mut state);
        assert!(matches!(response, LedgerResponse::VaultDeleted { success: true }));
        assert_eq!(
            state.organizations.get(&organization_id).unwrap().status,
            OrganizationStatus::Deleted
        );
    }

    #[tokio::test]
    async fn test_deleting_organization_blocks_writes() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization and vault
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        let create_vault = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault, &mut state);
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Mark organization for deletion
        let delete_ns = LedgerRequest::DeleteOrganization { organization_id };
        store.apply_request(&delete_ns, &mut state);

        // Try to write - should be blocked
        let write = LedgerRequest::Write { organization_id, vault_id, transactions: vec![] };
        let (response, _) = store.apply_request(&write, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("deleted") || message.contains("deleting"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_deleting_organization_blocks_create_vault() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization with a vault
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        let create_vault = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("existing-vault".to_string()),
            retention_policy: None,
        };
        store.apply_request(&create_vault, &mut state);

        // Mark organization for deletion
        let delete_ns = LedgerRequest::DeleteOrganization { organization_id };
        store.apply_request(&delete_ns, &mut state);

        // Try to create another vault - should be blocked
        let create_vault2 = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("new-vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault2, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("deleted") || message.contains("deleting"));
            },
            _ => panic!("expected Error"),
        }
    }

    #[tokio::test]
    async fn test_suspend_organization_write_rejected() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create and suspend organization with a vault
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Create vault before suspending
        let create_vault = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("test-vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault, &mut state);
        let vault_id = match response {
            LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
            _ => panic!("expected VaultCreated"),
        };

        // Suspend the organization
        let suspend = LedgerRequest::SuspendOrganization { organization_id, reason: None };
        let (response, _) = store.apply_request(&suspend, &mut state);
        match response {
            LedgerResponse::OrganizationSuspended { .. } => {},
            _ => panic!("expected OrganizationSuspended"),
        }

        // Try to write to suspended organization - should fail
        let write = LedgerRequest::Write { organization_id, vault_id, transactions: vec![] };
        let (response, _) = store.apply_request(&write, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("suspended"), "error should mention suspended");
            },
            _ => panic!("expected Error for write to suspended organization, got {:?}", response),
        }
    }

    #[tokio::test]
    async fn test_suspend_organization_create_vault_rejected() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create and suspend organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        let suspend = LedgerRequest::SuspendOrganization { organization_id, reason: None };
        let (response, _) = store.apply_request(&suspend, &mut state);
        match response {
            LedgerResponse::OrganizationSuspended { .. } => {},
            _ => panic!("expected OrganizationSuspended"),
        }

        // Try to create vault in suspended organization - should fail
        let create_vault = LedgerRequest::CreateVault {
            organization_id,
            slug: VaultSlug::new(1),
            name: Some("test-vault".to_string()),
            retention_policy: None,
        };
        let (response, _) = store.apply_request(&create_vault, &mut state);

        match response {
            LedgerResponse::Error { message } => {
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create and suspend organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        let suspend = LedgerRequest::SuspendOrganization { organization_id, reason: None };
        let (response, _) = store.apply_request(&suspend, &mut state);
        match response {
            LedgerResponse::OrganizationSuspended { .. } => {},
            _ => panic!("expected OrganizationSuspended"),
        }

        // Try to suspend again - should fail
        let suspend2 = LedgerRequest::SuspendOrganization { organization_id, reason: None };
        let (response, _) = store.apply_request(&suspend2, &mut state);

        match response {
            LedgerResponse::Error { message } => {
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization (active by default)
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        // Try to resume active organization - should fail
        let resume = LedgerRequest::ResumeOrganization { organization_id };
        let (response, _) = store.apply_request(&resume, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("not suspended"), "error should mention not suspended");
            },
            _ => panic!("expected Error for resuming active organization, got {:?}", response),
        }
    }

    #[tokio::test]
    async fn test_suspend_deleted_organization_fails() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create and delete organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        let delete_ns = LedgerRequest::DeleteOrganization { organization_id };
        let (response, _) = store.apply_request(&delete_ns, &mut state);
        match response {
            LedgerResponse::OrganizationDeleted { success, .. } => assert!(success),
            _ => panic!("expected OrganizationDeleted"),
        }

        // Try to suspend deleted organization - should fail
        let suspend = LedgerRequest::SuspendOrganization { organization_id, reason: None };
        let (response, _) = store.apply_request(&suspend, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("deleted"), "error should mention deleted organization");
            },
            _ => panic!("expected Error for suspending deleted organization, got {:?}", response),
        }
    }

    #[tokio::test]
    async fn test_suspend_organization_not_found() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Try to suspend non-existent organization
        let suspend = LedgerRequest::SuspendOrganization {
            organization_id: OrganizationId::new(999),
            reason: None,
        };
        let (response, _) = store.apply_request(&suspend, &mut state);

        match response {
            LedgerResponse::Error { message } => {
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Try to resume non-existent organization
        let resume =
            LedgerRequest::ResumeOrganization { organization_id: OrganizationId::new(999) };
        let (response, _) = store.apply_request(&resume, &mut state);

        match response {
            LedgerResponse::Error { message } => {
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create and suspend organization (no vaults)
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test-ns".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        let (response, _) = store.apply_request(&create_ns, &mut state);
        let organization_id = match response {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            _ => panic!("expected OrganizationCreated"),
        };

        let suspend = LedgerRequest::SuspendOrganization { organization_id, reason: None };
        let (response, _) = store.apply_request(&suspend, &mut state);
        match response {
            LedgerResponse::OrganizationSuspended { .. } => {},
            _ => panic!("expected OrganizationSuspended"),
        }

        // Delete suspended organization - should succeed
        let delete_ns = LedgerRequest::DeleteOrganization { organization_id };
        let (response, _) = store.apply_request(&delete_ns, &mut state);

        match response {
            LedgerResponse::OrganizationDeleted { success, .. } => {
                assert!(success, "deletion should succeed for suspended organization");
            },
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

        // Same sequence of requests to apply
        let requests = vec![
            LedgerRequest::CreateOrganization {
                name: "acme-corp".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            LedgerRequest::CreateOrganization {
                name: "startup-inc".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("production".to_string()),
                retention_policy: None,
            },
            LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("staging".to_string()),
                retention_policy: None,
            },
            LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(2),
                slug: VaultSlug::new(1),
                name: Some("main".to_string()),
                retention_policy: None,
            },
            LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![],
            },
            LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![],
            },
            LedgerRequest::Write {
                organization_id: OrganizationId::new(2),
                vault_id: VaultId::new(3),
                transactions: vec![],
            },
            LedgerRequest::System(SystemRequest::CreateUser {
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                admin: false,
            }),
            LedgerRequest::System(SystemRequest::CreateUser {
                name: "Bob".to_string(),
                email: "bob@example.com".to_string(),
                admin: false,
            }),
        ];

        // Apply to node A
        let mut state_a = store_a.applied_state.write();
        let mut results_a = Vec::new();
        for request in &requests {
            let (response, _) = store_a.apply_request(request, &mut state_a);
            results_a.push(response);
        }
        drop(state_a);

        // Apply to node B
        let mut state_b = store_b.applied_state.write();
        let mut results_b = Vec::new();
        for request in &requests {
            let (response, _) = store_b.apply_request(request, &mut state_b);
            results_b.push(response);
        }
        drop(state_b);

        // Results must be identical
        assert_eq!(results_a, results_b, "Same inputs must produce identical results on all nodes");

        // Final state must be identical
        let final_state_a = store_a.applied_state.read();
        let final_state_b = store_b.applied_state.read();

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
        let mut state_a = store_a.applied_state.write();
        let mut state_b = store_b.applied_state.write();

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

    /// Verifies block hashes are deterministic.
    ///
    /// The same (organization, vault, height) must always produce the same block hash.
    #[tokio::test]
    async fn test_deterministic_block_hash() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        // Same inputs must produce same hash
        let hash_a = store_a.compute_block_hash(OrganizationId::new(1), VaultId::new(2), 3);
        let hash_b = store_b.compute_block_hash(OrganizationId::new(1), VaultId::new(2), 3);

        assert_eq!(hash_a, hash_b, "Block hashes must be deterministic");

        // Different inputs must produce different hashes
        let hash_c = store_a.compute_block_hash(OrganizationId::new(1), VaultId::new(2), 4);
        assert_ne!(hash_a, hash_c, "Different inputs should produce different hashes");
    }

    /// Verifies vault height tracking is deterministic.
    ///
    /// Writes to the same vault must increment height consistently across nodes.
    #[tokio::test]
    async fn test_deterministic_vault_heights() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        let mut state_a = store_a.applied_state.write();
        let mut state_b = store_b.applied_state.write();

        // Create vault on both nodes
        let create_vault = LedgerRequest::CreateVault {
            organization_id: OrganizationId::new(1),
            slug: VaultSlug::new(1),
            name: Some("test".to_string()),
            retention_policy: None,
        };
        store_a.apply_request(&create_vault, &mut state_a);
        store_b.apply_request(&create_vault, &mut state_b);

        // Apply multiple writes
        for _ in 0..5 {
            let write = LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![],
            };
            store_a.apply_request(&write, &mut state_a);
            store_b.apply_request(&write, &mut state_b);
        }

        // Heights must match
        assert_eq!(
            state_a.vault_heights.get(&(OrganizationId::new(1), VaultId::new(1))),
            state_b.vault_heights.get(&(OrganizationId::new(1), VaultId::new(1))),
            "Vault heights must be identical after same operations"
        );
        assert_eq!(
            state_a.vault_heights.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(&5),
            "Height should be 5 after 5 writes"
        );
    }

    /// Verifies interleaved operations across multiple vaults are deterministic.
    ///
    /// Real workloads have writes to multiple vaults interleaved. The state
    /// machine must handle this deterministically.
    #[tokio::test]
    async fn test_deterministic_interleaved_operations() {
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");

        let store_a = RaftLogStore::<FileBackend>::open(dir_a.path().join("raft_log.db"))
            .expect("open store a");
        let store_b = RaftLogStore::<FileBackend>::open(dir_b.path().join("raft_log.db"))
            .expect("open store b");

        let mut state_a = store_a.applied_state.write();
        let mut state_b = store_b.applied_state.write();

        // Create organization and vaults
        let requests: Vec<LedgerRequest> = vec![
            LedgerRequest::CreateOrganization {
                name: "ns1".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("vault-a".to_string()),
                retention_policy: None,
            },
            LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("vault-b".to_string()),
                retention_policy: None,
            },
        ];

        for req in &requests {
            store_a.apply_request(req, &mut state_a);
            store_b.apply_request(req, &mut state_b);
        }

        // Interleaved writes to different vaults
        let interleaved: Vec<LedgerRequest> = vec![
            LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![],
            },
            LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(2),
                transactions: vec![],
            },
            LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![],
            },
            LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(2),
                transactions: vec![],
            },
            LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![],
            },
        ];

        let mut results_a = Vec::new();
        let mut results_b = Vec::new();

        for req in &interleaved {
            let (response_a, _) = store_a.apply_request(req, &mut state_a);
            let (response_b, _) = store_b.apply_request(req, &mut state_b);
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
        assert_eq!(counters.user, 1, "User counter should start at 1");
        assert_eq!(counters.user_email, 1, "User email counter should start at 1");
        assert_eq!(counters.email_verify, 1, "Email verify counter should start at 1");
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Setup: create organization and vault
        store.apply_request(
            &LedgerRequest::CreateOrganization {
                name: "test".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("vault1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Apply a write with transactions
        let tx = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: "test-client".to_string(),
            sequence: 1,
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "key1".to_string(),
                value: b"value1".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
            actor: "test-actor".to_string(),
        };

        let request = LedgerRequest::Write {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            transactions: vec![tx],
        };

        let (response, vault_entry) = store.apply_request(&request, &mut state);

        // Verify response
        match response {
            LedgerResponse::Write { block_height, .. } => {
                assert_eq!(block_height, 1, "First write should be height 1");
            },
            _ => panic!("Expected Write response"),
        }

        // Verify VaultEntry was created
        let entry = vault_entry.expect("VaultEntry should be created");
        assert_eq!(entry.organization_id, OrganizationId::new(1));
        assert_eq!(entry.vault_id, VaultId::new(1));
        assert_eq!(entry.vault_height, 1);
        assert_eq!(entry.transactions.len(), 1);
        // state_root and tx_merkle_root will be ZERO_HASH without StateLayer configured
        // but the structure should be correct
    }

    /// Test that shard_height is tracked in AppliedState for snapshot persistence.
    #[tokio::test]
    async fn test_shard_height_tracked_in_applied_state() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");

        // Initial shard height should be 0
        assert_eq!(store.current_shard_height(), 0);

        // After applying entries, shard height should increment
        // Note: full shard height increment requires apply_to_state_machine
        // which creates ShardBlocks. This test verifies the accessor.
        let state = store.applied_state.read();
        assert_eq!(state.shard_height, 0, "Initial shard height should be 0");
    }

    /// Test that AppliedState serialization preserves all fields including shard tracking.
    #[tokio::test]
    async fn test_applied_state_snapshot_round_trip() {
        use openraft::StoredMembership;

        let mut original = AppliedState {
            last_applied: Some(make_log_id(1, 10)),
            membership: StoredMembership::default(),
            sequences: SequenceCounters::new(),
            vault_heights: HashMap::new(),
            vault_health: HashMap::new(),
            previous_vault_hashes: HashMap::new(),
            organizations: HashMap::new(),
            vaults: HashMap::new(),
            shard_height: 42,
            previous_shard_hash: [0xAB; 32],
            client_sequences: HashMap::new(),
            organization_storage_bytes: HashMap::new(),
            slug_index: HashMap::new(),
            id_to_slug: HashMap::new(),
            vault_slug_index: HashMap::new(),
            vault_id_to_slug: HashMap::new(),
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
                organization_id: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: ShardId::new(0),
                name: "test-ns".to_string(),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        original.vaults.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultMeta {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
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
        assert_eq!(restored.shard_height, 42, "shard_height must be preserved");
        assert_eq!(
            restored.previous_shard_hash, [0xAB; 32],
            "previous_shard_hash must be preserved"
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let accessor = store.accessor();

        // Setup some state
        {
            let mut state = store.applied_state.write();
            state.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 42);
            state.vault_heights.insert((OrganizationId::new(1), VaultId::new(2)), 100);
            state.shard_height = 99;
            state.organizations.insert(
                OrganizationId::new(1),
                OrganizationMeta {
                    organization_id: OrganizationId::new(1),
                    slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    shard_id: ShardId::new(0),
                    name: "test".to_string(),
                    status: OrganizationStatus::Active,
                    pending_shard_id: None,
                    quota: None,
                },
            );
        }

        // Test accessor methods
        assert_eq!(accessor.vault_height(OrganizationId::new(1), VaultId::new(1)), 42);
        assert_eq!(accessor.vault_height(OrganizationId::new(1), VaultId::new(2)), 100);
        assert_eq!(accessor.vault_height(OrganizationId::new(1), VaultId::new(99)), 0); // Non-existent returns 0
        assert_eq!(accessor.shard_height(), 99);

        let all_heights = accessor.all_vault_heights();
        assert_eq!(all_heights.len(), 2);
        assert_eq!(all_heights.get(&(OrganizationId::new(1), VaultId::new(1))), Some(&42));

        assert!(accessor.get_organization(OrganizationId::new(1)).is_some());
        assert!(accessor.get_organization(OrganizationId::new(99)).is_none());
    }

    // ========================================================================
    // Snapshot Install Tests
    // ========================================================================
    //
    // These tests verify that snapshot installation correctly restores state,
    // which is critical for follower catch-up and cluster recovery.

    /// Test that snapshot install restores all AppliedState fields.
    ///
    /// This test directly creates a CombinedSnapshot and verifies install_snapshot
    /// correctly restores all state including shard tracking.
    #[tokio::test]
    async fn test_snapshot_install_restores_state() {
        use std::io::Cursor;

        use openraft::{SnapshotMeta, StoredMembership};

        // Build a CombinedSnapshot with realistic data
        let mut applied_state = AppliedState {
            last_applied: Some(make_log_id(1, 100)),
            membership: StoredMembership::default(),
            sequences: SequenceCounters::new(),
            vault_heights: HashMap::new(),
            vault_health: HashMap::new(),
            previous_vault_hashes: HashMap::new(),
            organizations: HashMap::new(),
            vaults: HashMap::new(),
            shard_height: 55,
            previous_shard_hash: [0xBE; 32],
            client_sequences: HashMap::new(),
            organization_storage_bytes: HashMap::new(),
            slug_index: HashMap::new(),
            id_to_slug: HashMap::new(),
            vault_slug_index: HashMap::new(),
            vault_id_to_slug: HashMap::new(),
        };

        // Add state data
        applied_state.sequences.next_organization();
        applied_state.sequences.next_organization();
        applied_state.sequences.next_vault();
        applied_state.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 42);
        applied_state.vault_heights.insert((OrganizationId::new(1), VaultId::new(2)), 100);
        applied_state.organizations.insert(
            OrganizationId::new(1),
            OrganizationMeta {
                organization_id: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: ShardId::new(0),
                name: "production".to_string(),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        applied_state.organizations.insert(
            OrganizationId::new(2),
            OrganizationMeta {
                organization_id: OrganizationId::new(2),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: ShardId::new(0),
                name: "staging".to_string(),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        applied_state.vaults.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultMeta {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                slug: VaultSlug::new(1),
                name: Some("main-vault".to_string()),
                deleted: false,
                last_write_timestamp: 1234567890,
                retention_policy: BlockRetentionPolicy::default(),
            },
        );

        let combined = CombinedSnapshot {
            applied_state,
            vault_entities: HashMap::new(),
            event_entries: Vec::new(),
        };

        let snapshot_data = postcard::to_allocvec(&combined).expect("serialize snapshot");

        // Create target store (simulating a new follower)
        let target_dir = tempdir().expect("create target dir");
        let mut target_store = RaftLogStore::<FileBackend>::open(target_dir.path().join("raft.db"))
            .expect("open target");

        // Verify initial state is empty
        assert_eq!(target_store.current_shard_height(), 0);
        assert!(target_store.applied_state.read().vault_heights.is_empty());

        // Install snapshot on target
        let meta = SnapshotMeta {
            last_log_id: Some(make_log_id(1, 100)),
            last_membership: StoredMembership::default(),
            snapshot_id: "test-snapshot".to_string(),
        };
        target_store
            .install_snapshot(&meta, Box::new(Cursor::new(snapshot_data)))
            .await
            .expect("install snapshot");

        // Verify state was restored
        let restored = target_store.applied_state.read();

        // Check sequence counters
        assert_eq!(
            restored.sequences.organization,
            OrganizationId::new(3),
            "organization counter should be restored"
        );
        assert_eq!(restored.sequences.vault, VaultId::new(2), "vault counter should be restored");

        // Check vault heights
        assert_eq!(
            restored.vault_heights.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(&42)
        );
        assert_eq!(
            restored.vault_heights.get(&(OrganizationId::new(1), VaultId::new(2))),
            Some(&100)
        );

        // Check shard tracking
        assert_eq!(restored.shard_height, 55, "shard_height should be restored");
        assert_eq!(
            restored.previous_shard_hash, [0xBE; 32],
            "previous_shard_hash should be restored"
        );

        // Check organization registry
        assert_eq!(restored.organizations.len(), 2);
        let ns1 = restored
            .organizations
            .get(&OrganizationId::new(1))
            .expect("organization 1 should exist");
        assert_eq!(ns1.name, "production");

        // Check vault registry
        assert_eq!(restored.vaults.len(), 1);
        let v1 = restored
            .vaults
            .get(&(OrganizationId::new(1), VaultId::new(1)))
            .expect("vault (1,1) should exist");
        assert_eq!(v1.name, Some("main-vault".to_string()));

        // Verify the target store's runtime fields are also updated
        drop(restored);
        assert_eq!(target_store.current_shard_height(), 55);
    }

    /// Test that snapshot install on empty store works correctly.
    #[tokio::test]
    async fn test_snapshot_install_on_fresh_node() {
        use std::io::Cursor;

        use openraft::{SnapshotMeta, StoredMembership};

        // Create a minimal CombinedSnapshot
        let combined = CombinedSnapshot {
            applied_state: AppliedState {
                last_applied: Some(make_log_id(2, 50)),
                membership: StoredMembership::default(),
                sequences: SequenceCounters {
                    organization: OrganizationId::new(5),
                    vault: VaultId::new(10),
                    user: 3,
                    user_email: 7,
                    email_verify: 12,
                },
                vault_heights: {
                    let mut h = HashMap::new();
                    h.insert((OrganizationId::new(1), VaultId::new(1)), 25);
                    h
                },
                vault_health: HashMap::new(),
                previous_vault_hashes: HashMap::new(),
                organizations: HashMap::new(),
                vaults: HashMap::new(),
                shard_height: 30,
                previous_shard_hash: [0xAA; 32],
                client_sequences: HashMap::new(),
                organization_storage_bytes: HashMap::new(),
                slug_index: HashMap::new(),
                id_to_slug: HashMap::new(),
                vault_slug_index: HashMap::new(),
                vault_id_to_slug: HashMap::new(),
            },
            vault_entities: HashMap::new(),
            event_entries: Vec::new(),
        };

        let snapshot_data = postcard::to_allocvec(&combined).expect("serialize snapshot");

        // Fresh node
        let dir = tempdir().expect("create dir");
        let mut store =
            RaftLogStore::<FileBackend>::open(dir.path().join("raft.db")).expect("open");

        // Verify initial state is empty
        assert_eq!(store.current_shard_height(), 0);
        assert!(store.applied_state.read().vault_heights.is_empty());

        // Install snapshot
        let meta = SnapshotMeta {
            last_log_id: Some(make_log_id(2, 50)),
            last_membership: StoredMembership::default(),
            snapshot_id: "fresh-install".to_string(),
        };
        store.install_snapshot(&meta, Box::new(Cursor::new(snapshot_data))).await.expect("install");

        // Verify state
        assert_eq!(store.current_shard_height(), 30);
        assert_eq!(store.applied_state.read().sequences.organization, OrganizationId::new(5));
        assert_eq!(store.applied_state.read().sequences.vault, VaultId::new(10));
        assert_eq!(store.applied_state.read().sequences.user_email, 7);
        assert_eq!(store.applied_state.read().sequences.email_verify, 12);
        assert_eq!(
            store
                .applied_state
                .read()
                .vault_heights
                .get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(&25)
        );
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
        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        assert!(store.block_announcements().is_none());

        // Create new store with sender
        let (sender, mut receiver) = broadcast::channel::<BlockAnnouncement>(16);
        let store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
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
    async fn test_append_and_read_log_entries() {
        // This test simulates what openraft does during replication
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let mut store = RaftLogStore::<FileBackend>::open(&path).expect("open store");

        // Create 100 log entries (enough to cause multiple leaf nodes in the B-tree)
        let entries: Vec<Entry<LedgerTypeConfig>> = (1..=100u64)
            .map(|i| Entry {
                log_id: make_log_id(1, i),
                payload: EntryPayload::Normal(LedgerRequest::CreateOrganization {
                    name: format!("ns-{}", i),
                    slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    shard_id: None,
                    quota: None,
                }),
            })
            .collect();

        // Append entries
        store.append_to_log(entries).await.expect("append entries");

        // Get log state
        let log_state = store.get_log_state().await.expect("get log state");
        assert_eq!(log_state.last_log_id.map(|id| id.index), Some(100));

        // Read all entries back (what openraft does during replication)
        let read_entries = store.try_get_log_entries(1u64..=100u64).await.expect("read entries");

        assert_eq!(read_entries.len(), 100, "Expected 100 entries, got {}", read_entries.len());

        // Verify each entry exists and has correct index
        for (i, entry) in read_entries.iter().enumerate() {
            let expected_index = (i + 1) as u64;
            assert_eq!(
                entry.log_id.index, expected_index,
                "Entry at position {} has wrong index: expected {}, got {}",
                i, expected_index, entry.log_id.index
            );
        }

        // Test partial range (what openraft does when replicating to a follower)
        let partial = store.try_get_log_entries(50u64..=75u64).await.expect("read partial");
        assert_eq!(partial.len(), 26, "Expected 26 entries, got {}", partial.len());
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_broadcasts_block_announcements() {
        use std::time::{Duration, Instant};

        use inferadb_ledger_proto::proto::{
            BlockAnnouncement, OrganizationSlug as ProtoOrganizationSlug,
            VaultSlug as ProtoVaultSlug,
        };
        use openraft::RaftStorage;

        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        // Create store with broadcast sender
        let (sender, mut receiver) = broadcast::channel::<BlockAnnouncement>(16);
        let mut store = RaftLogStore::<FileBackend>::open(&path)
            .expect("open store")
            .with_block_announcements(sender);

        // First, create organization and vault using apply_request (sets up state)
        {
            let mut state = store.applied_state.write();

            let create_ns = LedgerRequest::CreateOrganization {
                name: "test-ns".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(42),
                shard_id: Some(ShardId::new(0)),
                quota: None,
            };
            let (response, _) = store.apply_request(&create_ns, &mut state);
            let organization_id = match response {
                LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
                _ => panic!("expected OrganizationCreated"),
            };
            assert_eq!(organization_id, OrganizationId::new(1));

            let create_vault = LedgerRequest::CreateVault {
                organization_id,
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            };
            let (response, _) = store.apply_request(&create_vault, &mut state);
            let vault_id = match response {
                LedgerResponse::VaultCreated { vault_id, .. } => vault_id,
                _ => panic!("expected VaultCreated"),
            };
            assert_eq!(vault_id, VaultId::new(1));
        }

        // Now call apply_to_state_machine with a Write entry
        // This should broadcast a BlockAnnouncement
        let write_request = LedgerRequest::Write {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            transactions: vec![], // Empty transactions still create a block
        };

        let entry =
            Entry { log_id: make_log_id(1, 1), payload: EntryPayload::Normal(write_request) };

        let start = Instant::now();
        let responses = store.apply_to_state_machine(&[entry]).await.expect("apply");

        // Verify response is WriteCompleted
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            LedgerResponse::Write { block_height: height, .. } => {
                assert_eq!(*height, 1, "Expected height 1 for first block");
            },
            other => panic!("expected WriteCompleted, got {:?}", other),
        }

        // Verify announcement was broadcast (should be near-instant)
        let timeout = Duration::from_millis(100);
        let received = tokio::time::timeout(timeout, receiver.recv())
            .await
            .expect("announcement should arrive within 100ms")
            .expect("should receive announcement");

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "Announcement should be received within 100ms, took {:?}",
            elapsed
        );

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
        use openraft::RaftStorage;

        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        // Store without broadcast sender
        let mut store = RaftLogStore::<FileBackend>::open(&path).expect("open store");

        // Create organization and vault
        {
            let mut state = store.applied_state.write();

            let create_ns = LedgerRequest::CreateOrganization {
                name: "test-ns".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: Some(ShardId::new(0)),
                quota: None,
            };
            store.apply_request(&create_ns, &mut state);

            let create_vault = LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("test-vault".to_string()),
                retention_policy: None,
            };
            store.apply_request(&create_vault, &mut state);
        }

        // Apply write - should not panic even without sender
        let write_request = LedgerRequest::Write {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            transactions: vec![],
        };

        let entry =
            Entry { log_id: make_log_id(1, 1), payload: EntryPayload::Normal(write_request) };

        let responses = store.apply_to_state_machine(&[entry]).await.expect("apply");
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            LedgerResponse::Write { .. } => {},
            other => panic!("expected WriteCompleted, got {:?}", other),
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // 1. Start healthy
        state
            .vault_health
            .insert((OrganizationId::new(1), VaultId::new(1)), VaultHealthStatus::Healthy);
        assert_eq!(
            state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(&VaultHealthStatus::Healthy)
        );

        // 2. Transition to Diverged (detected by auto-recovery scanner)
        let diverge = LedgerRequest::UpdateVaultHealth {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            healthy: false,
            expected_root: Some([0xAA; 32]),
            computed_root: Some([0xBB; 32]),
            diverged_at_height: Some(100),
            recovery_attempt: None,
            recovery_started_at: None,
        };
        let (response, _) = store.apply_request(&diverge, &mut state);
        assert!(matches!(response, LedgerResponse::VaultHealthUpdated { success: true }));
        assert!(matches!(
            state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(VaultHealthStatus::Diverged { at_height: 100, .. })
        ));

        // 3. Transition to Recovering attempt 1
        let now = chrono::Utc::now().timestamp();
        let recover1 = LedgerRequest::UpdateVaultHealth {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            healthy: false,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: Some(1),
            recovery_started_at: Some(now),
        };
        let (response, _) = store.apply_request(&recover1, &mut state);
        assert!(matches!(response, LedgerResponse::VaultHealthUpdated { success: true }));
        assert!(matches!(
            state.vault_health.get(&(OrganizationId::new(1), VaultId::new(1))),
            Some(VaultHealthStatus::Recovering { attempt: 1, .. })
        ));

        // Recovering vaults do NOT block writes (only Diverged does)
        // Recovery happens in the background via replay, not inline

        // 4. Recovery succeeds → Healthy
        let healthy = LedgerRequest::UpdateVaultHealth {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            healthy: true,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: None,
            recovery_started_at: None,
        };
        let (response, _) = store.apply_request(&healthy, &mut state);
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

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
            let recover = LedgerRequest::UpdateVaultHealth {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                healthy: false,
                expected_root: None,
                computed_root: None,
                diverged_at_height: None,
                recovery_attempt: Some(attempt),
                recovery_started_at: Some(base_time + i64::from(attempt) * 10),
            };
            let (response, _) = store.apply_request(&recover, &mut state);
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

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
            let recover = LedgerRequest::UpdateVaultHealth {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                healthy: false,
                expected_root: None,
                computed_root: None,
                diverged_at_height: None,
                recovery_attempt: Some(attempt),
                recovery_started_at: Some(now),
            };
            let _ = store.apply_request(&recover, &mut state);
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization so writes don't fail for missing organization
        let create_ns = LedgerRequest::CreateOrganization {
            name: "test".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(0),
            shard_id: Some(ShardId::new(0)),
            quota: None,
        };
        store.apply_request(&create_ns, &mut state);
        let create_vault = LedgerRequest::CreateVault {
            organization_id: OrganizationId::new(1),
            slug: VaultSlug::new(1),
            name: Some("vault".to_string()),
            retention_policy: None,
        };
        store.apply_request(&create_vault, &mut state);

        // Diverged blocks writes
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Diverged { expected: [1; 32], computed: [2; 32], at_height: 1 },
        );
        let write = LedgerRequest::Write {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            transactions: vec![],
        };
        let (response, _) = store.apply_request(&write, &mut state);
        assert!(matches!(response, LedgerResponse::Error { .. }));

        // Recovering does NOT block writes (recovery is background replay)
        state.vault_health.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            VaultHealthStatus::Recovering {
                started_at: chrono::Utc::now().timestamp(),
                attempt: 1,
            },
        );
        let (response, _) = store.apply_request(&write, &mut state);
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Mark healthy twice
        let healthy = LedgerRequest::UpdateVaultHealth {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(1),
            healthy: true,
            expected_root: None,
            computed_root: None,
            diverged_at_height: None,
            recovery_attempt: None,
            recovery_started_at: None,
        };
        let (r1, _) = store.apply_request(&healthy, &mut state);
        let (r2, _) = store.apply_request(&healthy, &mut state);
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
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
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
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
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
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
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
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
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
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
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
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
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
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
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
            client_id: "c".to_string(),
            sequence: 2,
            actor: "a".to_string(),
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Setup organization + vault
        store.apply_request(
            &LedgerRequest::CreateOrganization {
                name: "test-ns".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
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
            client_id: "client1".to_string(),
            sequence: 1,
            actor: "test".to_string(),
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "key1".to_string(),
                value: b"value1".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_request(
            &LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![tx1],
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
            client_id: "client1".to_string(),
            sequence: 2,
            actor: "test".to_string(),
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "key2".to_string(),
                value: b"longerval!".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_request(
            &LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![tx2],
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Setup
        store.apply_request(
            &LedgerRequest::CreateOrganization {
                name: "ns".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Write: key="abcde" (5), value="fghij" (5) → +10
        let tx_set = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "abcde".to_string(),
                value: b"fghij".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_request(
            &LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![tx_set],
            },
            &mut state,
        );
        assert_eq!(state.organization_storage_bytes[&OrganizationId::new(1)], 10);

        // Delete: key="abcde" (5) → -5 (conservative: doesn't know value size)
        let tx_del = inferadb_ledger_types::Transaction {
            id: [2u8; 16],
            client_id: "c".to_string(),
            sequence: 2,
            actor: "a".to_string(),
            operations: vec![inferadb_ledger_types::Operation::DeleteEntity {
                key: "abcde".to_string(),
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_request(
            &LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![tx_del],
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        store.apply_request(
            &LedgerRequest::CreateOrganization {
                name: "ns".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Delete without prior write — should floor at 0 via saturating_sub
        let tx = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
            operations: vec![inferadb_ledger_types::Operation::DeleteEntity {
                key: "missing_key".to_string(),
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_request(
            &LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![tx],
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
    async fn test_organization_storage_bytes_independent_organizations() {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("raft_log.db");

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create two organizations, each with a vault
        store.apply_request(
            &LedgerRequest::CreateOrganization {
                name: "ns-alpha".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateOrganization {
                name: "ns-beta".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(2),
                slug: VaultSlug::new(1),
                name: Some("v2".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Write to organization 1: "aa" + "bb" = 4 bytes
        let tx1 = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "aa".to_string(),
                value: b"bb".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_request(
            &LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![tx1],
            },
            &mut state,
        );

        // Write to organization 2: "cccccc" + "dddddddd" = 14 bytes
        let tx2 = inferadb_ledger_types::Transaction {
            id: [2u8; 16],
            client_id: "c".to_string(),
            sequence: 2,
            actor: "a".to_string(),
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "cccccc".to_string(),
                value: b"dddddddd".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_request(
            &LedgerRequest::Write {
                organization_id: OrganizationId::new(2),
                vault_id: VaultId::new(2),
                transactions: vec![tx2],
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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");

        // Before any writes, accessor returns 0
        assert_eq!(store.accessor().organization_storage_bytes(OrganizationId::new(1)), 0);

        // Write some data
        let mut state = store.applied_state.write();
        state.organization_storage_bytes.insert(OrganizationId::new(1), 42);
        drop(state);

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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        // Create organization with two vaults
        store.apply_request(
            &LedgerRequest::CreateOrganization {
                name: "usage-ns".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v2".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        // Write some data
        let tx = inferadb_ledger_types::Transaction {
            id: [1u8; 16],
            client_id: "c".to_string(),
            sequence: 1,
            actor: "a".to_string(),
            operations: vec![inferadb_ledger_types::Operation::SetEntity {
                key: "key".to_string(),
                value: b"value".to_vec(),
                condition: None,
                expires_at: None,
            }],
            timestamp: chrono::Utc::now(),
        };
        store.apply_request(
            &LedgerRequest::Write {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
                transactions: vec![tx],
            },
            &mut state,
        );
        drop(state);

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

        let store = RaftLogStore::<FileBackend>::open(&path).expect("open store");
        let mut state = store.applied_state.write();

        store.apply_request(
            &LedgerRequest::CreateOrganization {
                name: "del-ns".to_string(),
                slug: inferadb_ledger_types::OrganizationSlug::new(0),
                shard_id: None,
                quota: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v1".to_string()),
                retention_policy: None,
            },
            &mut state,
        );
        store.apply_request(
            &LedgerRequest::CreateVault {
                organization_id: OrganizationId::new(1),
                slug: VaultSlug::new(1),
                name: Some("v2".to_string()),
                retention_policy: None,
            },
            &mut state,
        );

        drop(state);
        assert_eq!(store.accessor().organization_usage(OrganizationId::new(1)).vault_count, 2);

        // Delete one vault
        let mut state = store.applied_state.write();
        store.apply_request(
            &LedgerRequest::DeleteVault {
                organization_id: OrganizationId::new(1),
                vault_id: VaultId::new(1),
            },
            &mut state,
        );
        drop(state);

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

        let store = Arc::new(RaftLogStore::<FileBackend>::open(&path).expect("open store"));

        // Set up state with known storage bytes
        {
            let mut state = store.applied_state.write();
            store.apply_request(
                &LedgerRequest::CreateOrganization {
                    name: "conc-ns".to_string(),
                    slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    shard_id: None,
                    quota: None,
                },
                &mut state,
            );
            store.apply_request(
                &LedgerRequest::CreateVault {
                    organization_id: OrganizationId::new(1),
                    slug: VaultSlug::new(1),
                    name: Some("v1".to_string()),
                    retention_policy: None,
                },
                &mut state,
            );
            // Write data: "hello" (5) + "world!" (6) = 11 bytes
            let tx = inferadb_ledger_types::Transaction {
                id: [1u8; 16],
                client_id: "c".to_string(),
                sequence: 1,
                actor: "a".to_string(),
                operations: vec![inferadb_ledger_types::Operation::SetEntity {
                    key: "hello".to_string(),
                    value: b"world!".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                timestamp: chrono::Utc::now(),
            };
            store.apply_request(
                &LedgerRequest::Write {
                    organization_id: OrganizationId::new(1),
                    vault_id: VaultId::new(1),
                    transactions: vec![tx],
                },
                &mut state,
            );
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
    fn simple_write_request(org_id: OrganizationId, vault_id: VaultId) -> LedgerRequest {
        LedgerRequest::Write {
            organization_id: org_id,
            vault_id,
            transactions: vec![Transaction {
                id: [1u8; 16],
                client_id: "test-client".to_string(),
                sequence: 0,
                actor: "test-actor".to_string(),
                operations: vec![Operation::SetEntity {
                    key: "key1".to_string(),
                    value: b"value1".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                timestamp: fixed_timestamp(),
            }],
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
                organization_id: org_id,
                slug: org_slug,
                name: "test-org".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        state.slug_index.insert(org_slug, org_id);
        state.id_to_slug.insert(org_id, org_slug);

        let key = (org_id, vault_id);
        state.vault_heights.insert(key, 0);
        state.vaults.insert(
            key,
            VaultMeta {
                organization_id: org_id,
                vault_id,
                slug: vault_slug,
                name: Some("test-vault".to_string()),
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: BlockRetentionPolicy::default(),
            },
        );
        state.vault_slug_index.insert(vault_slug, vault_id);
        state.vault_id_to_slug.insert(vault_id, vault_slug);
        state.vault_health.insert(key, VaultHealthStatus::Healthy);

        (org_id, vault_id)
    }

    #[test]
    fn event_write_committed_has_correct_block_height() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = store.applied_state.write();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        let request = simple_write_request(org_id, vault_id);
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        let (response, _) = store.apply_request_with_events(
            &request,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
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
            "event block_height should match shard chain height + 1"
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
        let mut state_a = store_a.applied_state.write();
        let (org_id_a, vault_id_a) = setup_org_and_vault(&mut state_a);

        let mut state_b = store_b.applied_state.write();
        let (org_id_b, vault_id_b) = setup_org_and_vault(&mut state_b);

        // Both should have assigned the same IDs (sequential from zero)
        assert_eq!(org_id_a, org_id_b);
        assert_eq!(vault_id_a, vault_id_b);

        let ts = fixed_timestamp();

        // Apply identical sequences to both: CreateOrganization + Write
        let write_request = simple_write_request(org_id_a, vault_id_a);

        let mut events_a: Vec<EventEntry> = Vec::new();
        let mut events_b: Vec<EventEntry> = Vec::new();
        let mut idx_a = 0u32;
        let mut idx_b = 0u32;

        store_a.apply_request_with_events(
            &write_request,
            &mut state_a,
            ts,
            &mut idx_a,
            &mut events_a,
            90,
        );
        store_b.apply_request_with_events(
            &write_request,
            &mut state_b,
            ts,
            &mut idx_b,
            &mut events_b,
            90,
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
        let mut state = store.applied_state.write();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        // Write request with two ExpireEntity operations
        let request = LedgerRequest::Write {
            organization_id: org_id,
            vault_id,
            transactions: vec![Transaction {
                id: [2u8; 16],
                client_id: "gc-client".to_string(),
                sequence: 0,
                actor: "gc-actor".to_string(),
                operations: vec![
                    Operation::ExpireEntity { key: "expired-key-1".to_string(), expired_at: 100 },
                    Operation::ExpireEntity { key: "expired-key-2".to_string(), expired_at: 200 },
                ],
                timestamp: fixed_timestamp(),
            }],
        };

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_request_with_events(&request, &mut state, ts, &mut op_index, &mut events, 90);

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
    fn event_writer_integration_persists_to_events_db() {
        let dir = tempdir().expect("create temp dir");
        let events_db = EventsDatabase::open(dir.path()).expect("open events db");
        let events_db_arc = Arc::new(events_db);
        let config = EventConfig::default();
        let writer = crate::event_writer::EventWriter::new(Arc::clone(&events_db_arc), config);
        let store = RaftLogStore::<FileBackend>::open(dir.path().join("raft_log.db"))
            .expect("open store")
            .with_event_writer(writer);

        let mut state = store.applied_state.write();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        let request = simple_write_request(org_id, vault_id);
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_request_with_events(&request, &mut state, ts, &mut op_index, &mut events, 90);

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

    // ── Task 5: System-Level Event Hook Tests ──────────────────────

    #[test]
    fn event_organization_created_has_slug_detail() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = store.applied_state.write();

        let request = LedgerRequest::CreateOrganization {
            name: "slug-test-org".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(42_000),
            shard_id: None,
            quota: None,
        };

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_request_with_events(&request, &mut state, ts, &mut op_index, &mut events, 90);

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
        let mut state = store.applied_state.write();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        // Mark the vault as deleted so DeleteOrganization goes straight to Deleted
        let key = (org_id, vault_id);
        state.vaults.get_mut(&key).expect("vault exists").deleted = true;

        let request = LedgerRequest::DeleteOrganization { organization_id: org_id };

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_request_with_events(&request, &mut state, ts, &mut op_index, &mut events, 90);

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
    fn event_node_joined_cluster_emitted() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = store.applied_state.write();

        let request = LedgerRequest::System(SystemRequest::AddNode {
            node_id: 7,
            address: "10.0.0.7:9090".to_string(),
        });

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_request_with_events(&request, &mut state, ts, &mut op_index, &mut events, 90);

        let join_event = events
            .iter()
            .find(|e| e.action == EventAction::NodeJoinedCluster)
            .expect("NodeJoinedCluster event should be emitted");

        assert_eq!(join_event.details.get("node_id").map(|s| s.as_str()), Some("7"));
        assert_eq!(join_event.details.get("address").map(|s| s.as_str()), Some("10.0.0.7:9090"));
        assert_eq!(
            join_event.organization_id,
            OrganizationId::new(0),
            "NodeJoinedCluster is system-scoped"
        );
    }

    #[test]
    fn event_node_left_cluster_emitted() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = store.applied_state.write();

        let request = LedgerRequest::System(SystemRequest::RemoveNode { node_id: 3 });

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_request_with_events(&request, &mut state, ts, &mut op_index, &mut events, 90);

        let leave_event = events
            .iter()
            .find(|e| e.action == EventAction::NodeLeftCluster)
            .expect("NodeLeftCluster event should be emitted");

        assert_eq!(leave_event.details.get("node_id").map(|s| s.as_str()), Some("3"));
        assert_eq!(leave_event.organization_id, OrganizationId::new(0));
    }

    #[test]
    fn event_user_created_has_details() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = store.applied_state.write();

        let request = LedgerRequest::System(SystemRequest::CreateUser {
            name: "Alice Admin".to_string(),
            email: "alice@example.com".to_string(),
            admin: true,
        });

        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_request_with_events(&request, &mut state, ts, &mut op_index, &mut events, 90);

        let user_event = events
            .iter()
            .find(|e| e.action == EventAction::UserCreated)
            .expect("UserCreated event should be emitted");

        assert!(
            user_event.details.contains_key("user_id"),
            "UserCreated should have user_id detail"
        );
        assert_eq!(user_event.details.get("name").map(|s| s.as_str()), Some("Alice Admin"));
        assert_eq!(user_event.details.get("email").map(|s| s.as_str()), Some("alice@example.com"));
        assert_eq!(user_event.details.get("admin").map(|s| s.as_str()), Some("true"));
        assert_eq!(user_event.organization_id, OrganizationId::new(0));
    }

    #[test]
    fn event_suspend_resume_org_has_details() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = store.applied_state.write();
        let (org_id, _vault_id) = setup_org_and_vault(&mut state);

        // Suspend with reason
        let suspend = LedgerRequest::SuspendOrganization {
            organization_id: org_id,
            reason: Some("billing overdue".to_string()),
        };
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_request_with_events(&suspend, &mut state, ts, &mut op_index, &mut events, 90);

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
        let resume = LedgerRequest::ResumeOrganization { organization_id: org_id };
        let mut resume_events: Vec<EventEntry> = Vec::new();
        let mut resume_op_index = 0u32;

        store.apply_request_with_events(
            &resume,
            &mut state,
            ts,
            &mut resume_op_index,
            &mut resume_events,
            90,
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

        let mut state = store.applied_state.write();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        // Emit a system event (OrganizationCreated via CreateOrganization)
        let create_org = LedgerRequest::CreateOrganization {
            name: "second-org".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(9999),
            shard_id: None,
            quota: None,
        };
        let mut all_events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;
        let ts = fixed_timestamp();

        store.apply_request_with_events(
            &create_org,
            &mut state,
            ts,
            &mut op_index,
            &mut all_events,
            90,
        );

        // Emit an org-scoped event (WriteCommitted)
        let write_req = simple_write_request(org_id, vault_id);
        store.apply_request_with_events(
            &write_req,
            &mut state,
            ts,
            &mut op_index,
            &mut all_events,
            90,
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
    fn event_node_join_leave_determinism() {
        // Two independent stores applying the same AddNode should produce identical events
        let dir_a = tempdir().expect("create temp dir a");
        let dir_b = tempdir().expect("create temp dir b");
        let store_a = store_with_events(dir_a.path());
        let store_b = store_with_events(dir_b.path());

        let mut state_a = store_a.applied_state.write();
        let mut state_b = store_b.applied_state.write();

        let request = LedgerRequest::System(SystemRequest::AddNode {
            node_id: 5,
            address: "10.0.0.5:8080".to_string(),
        });

        let ts = fixed_timestamp();
        let mut events_a: Vec<EventEntry> = Vec::new();
        let mut events_b: Vec<EventEntry> = Vec::new();
        let mut idx_a = 0u32;
        let mut idx_b = 0u32;

        store_a.apply_request_with_events(
            &request,
            &mut state_a,
            ts,
            &mut idx_a,
            &mut events_a,
            90,
        );
        store_b.apply_request_with_events(
            &request,
            &mut state_b,
            ts,
            &mut idx_b,
            &mut events_b,
            90,
        );

        assert_eq!(events_a.len(), events_b.len());
        for (a, b) in events_a.iter().zip(events_b.iter()) {
            assert_eq!(a.event_id, b.event_id, "node join events must be deterministic");
            assert_eq!(a.action, b.action);
            assert_eq!(a.details, b.details);
        }
    }

    #[test]
    fn event_vault_created_has_org_scope() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = store.applied_state.write();

        // Create an organization first
        let create_org = LedgerRequest::CreateOrganization {
            name: "events-test-org".to_string(),
            slug: inferadb_ledger_types::OrganizationSlug::new(5000),
            shard_id: None,
            quota: None,
        };
        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        let (resp, _) = store.apply_request_with_events(
            &create_org,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
        );
        let org_id = match resp {
            LedgerResponse::OrganizationCreated { organization_id, .. } => organization_id,
            other => panic!("expected OrganizationCreated, got {:?}", other),
        };
        events.clear();
        op_index = 0;

        // Create a vault
        let create_vault = LedgerRequest::CreateVault {
            organization_id: org_id,
            slug: VaultSlug::new(6000),
            name: Some("audit-vault".to_string()),
            retention_policy: None,
        };
        let (resp, _) = store.apply_request_with_events(
            &create_vault,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
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
        assert_eq!(vault_event.vault_slug, Some(6000));
        assert_eq!(vault_event.details.get("vault_name").map(|s| s.as_str()), Some("audit-vault"));
    }

    #[test]
    fn event_vault_deleted_has_org_scope() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = store.applied_state.write();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        let delete_vault = LedgerRequest::DeleteVault { organization_id: org_id, vault_id };
        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        let (resp, _) = store.apply_request_with_events(
            &delete_vault,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
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
    fn event_batch_write_committed_emitted() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = store.applied_state.write();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        // Create a batch of two writes
        let batch = LedgerRequest::BatchWrite {
            requests: vec![
                simple_write_request(org_id, vault_id),
                LedgerRequest::Write {
                    organization_id: org_id,
                    vault_id,
                    transactions: vec![Transaction {
                        id: [2u8; 16],
                        client_id: "test-client".to_string(),
                        sequence: 0,
                        actor: "test-actor".to_string(),
                        operations: vec![Operation::SetEntity {
                            key: "key2".to_string(),
                            value: b"value2".to_vec(),
                            condition: None,
                            expires_at: None,
                        }],
                        timestamp: fixed_timestamp(),
                    }],
                },
            ],
        };

        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        let (resp, _) =
            store.apply_request_with_events(&batch, &mut state, ts, &mut op_index, &mut events, 90);
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
        let mut state = store.applied_state.write();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        // Mark vault as diverged
        let health_request = LedgerRequest::UpdateVaultHealth {
            organization_id: org_id,
            vault_id,
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

        store.apply_request_with_events(
            &health_request,
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
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
    fn event_org_isolation_across_organizations() {
        let dir = tempdir().expect("create temp dir");
        let store = store_with_events(dir.path());
        let mut state = store.applied_state.write();

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
                organization_id: org_b,
                slug: org_b_slug,
                name: "org-b".to_string(),
                shard_id: ShardId::new(0),
                status: OrganizationStatus::Active,
                pending_shard_id: None,
                quota: None,
            },
        );
        state.slug_index.insert(org_b_slug, org_b);
        state.id_to_slug.insert(org_b, org_b_slug);
        let key_b = (org_b, vault_b);
        state.vault_heights.insert(key_b, 0);
        state.vaults.insert(
            key_b,
            VaultMeta {
                organization_id: org_b,
                vault_id: vault_b,
                slug: vault_b_slug,
                name: Some("vault-b".to_string()),
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: BlockRetentionPolicy::default(),
            },
        );
        state.vault_slug_index.insert(vault_b_slug, vault_b);
        state.vault_id_to_slug.insert(vault_b, vault_b_slug);
        state.vault_health.insert(key_b, VaultHealthStatus::Healthy);

        let ts = fixed_timestamp();
        let mut all_events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        // Write to org A
        store.apply_request_with_events(
            &simple_write_request(org_a, vault_a),
            &mut state,
            ts,
            &mut op_index,
            &mut all_events,
            90,
        );

        // Write to org B
        store.apply_request_with_events(
            &LedgerRequest::Write {
                organization_id: org_b,
                vault_id: vault_b,
                transactions: vec![Transaction {
                    id: [9u8; 16],
                    client_id: "client-b".to_string(),
                    sequence: 0,
                    actor: "actor-b".to_string(),
                    operations: vec![Operation::SetEntity {
                        key: "key-b".to_string(),
                        value: b"value-b".to_vec(),
                        condition: None,
                        expires_at: None,
                    }],
                    timestamp: ts,
                }],
            },
            &mut state,
            ts,
            &mut op_index,
            &mut all_events,
            90,
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

        let mut state = store.applied_state.write();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        // Write to vault — should produce WriteCommitted event in the accumulator
        store.apply_request_with_events(
            &simple_write_request(org_id, vault_id),
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
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
        let mut state = store.applied_state.write();
        let (org_id, vault_id) = setup_org_and_vault(&mut state);

        let ts = fixed_timestamp();
        let mut events: Vec<EventEntry> = Vec::new();
        let mut op_index = 0u32;

        // First write — block_height should reference the shard chain height + 1
        store.apply_request_with_events(
            &simple_write_request(org_id, vault_id),
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
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
        store.apply_request_with_events(
            &LedgerRequest::Write {
                organization_id: org_id,
                vault_id,
                transactions: vec![Transaction {
                    id: [3u8; 16],
                    client_id: "test-client".to_string(),
                    sequence: 0,
                    actor: "test-actor".to_string(),
                    operations: vec![Operation::SetEntity {
                        key: "key2".to_string(),
                        value: b"value2".to_vec(),
                        condition: None,
                        expires_at: None,
                    }],
                    timestamp: ts,
                }],
            },
            &mut state,
            ts,
            &mut op_index,
            &mut events,
            90,
        );

        let second_write = events
            .iter()
            .find(|e| e.action == EventAction::WriteCommitted)
            .expect("second WriteCommitted event emitted");

        // block_height references the shard chain height, which is still 1
        // (shard chain only increments on actual block archival)
        assert!(
            second_write.block_height.is_some(),
            "second WriteCommitted should also have block_height reference"
        );
    }

    #[tokio::test]
    async fn event_snapshot_includes_apply_phase_entries() {
        use std::{collections::BTreeMap, io::Cursor};

        use chrono::{TimeZone, Utc};
        use inferadb_ledger_types::events::{EventEmission, EventOutcome};
        use openraft::SnapshotMeta;

        let source_dir = tempdir().expect("create source dir");
        let target_dir = tempdir().expect("create target dir");

        // Create source store with events and minimal Raft state
        let mut source_store = store_with_events(source_dir.path());
        {
            let mut state = source_store.applied_state.write();
            state.last_applied = Some(make_log_id(1, 10));
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
                organization_slug: None,
                vault_slug: None,
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
        let mut snapshot = source_store
            .get_current_snapshot()
            .await
            .expect("get snapshot")
            .expect("snapshot should exist");
        let snapshot_data = {
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut *snapshot.snapshot, &mut buf)
                .expect("read snapshot data");
            buf
        };

        // Install snapshot on target
        let mut target_store = store_with_events(target_dir.path());
        let meta = SnapshotMeta {
            last_log_id: snapshot.meta.last_log_id,
            last_membership: snapshot.meta.last_membership,
            snapshot_id: "test-snapshot".to_string(),
        };
        target_store
            .install_snapshot(&meta, Box::new(Cursor::new(snapshot_data)))
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

    #[tokio::test]
    async fn event_snapshot_excludes_handler_phase_entries() {
        use std::{collections::BTreeMap, io::Cursor};

        use chrono::{TimeZone, Utc};
        use inferadb_ledger_types::events::{EventEmission, EventOutcome};
        use openraft::SnapshotMeta;

        let source_dir = tempdir().expect("create source dir");
        let target_dir = tempdir().expect("create target dir");

        let mut source_store = store_with_events(source_dir.path());
        {
            let mut state = source_store.applied_state.write();
            state.last_applied = Some(make_log_id(1, 10));
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
                organization_slug: None,
                vault_slug: None,
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
                organization_slug: None,
                vault_slug: None,
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
        let mut snapshot = source_store
            .get_current_snapshot()
            .await
            .expect("get snapshot")
            .expect("snapshot should exist");
        let snapshot_data = {
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut *snapshot.snapshot, &mut buf)
                .expect("read snapshot data");
            buf
        };

        // Install snapshot on target
        let mut target_store = store_with_events(target_dir.path());
        let meta = SnapshotMeta {
            last_log_id: snapshot.meta.last_log_id,
            last_membership: snapshot.meta.last_membership,
            snapshot_id: "test-snapshot".to_string(),
        };
        target_store
            .install_snapshot(&meta, Box::new(Cursor::new(snapshot_data)))
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
}
