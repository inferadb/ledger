//! Raft state machine implementation for InferaDB Ledger.
//!
//! This module implements the `RaftStateMachine` trait, applying committed
//! log entries to the ledger storage layer.
//!
//! Key responsibilities:
//! - Apply transactions to vaults (entities and relationships)
//! - Manage namespace and vault creation
//! - Handle system operations (user management, cluster membership)
//! - Generate deterministic IDs for new resources
//! - Build and install snapshots

use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{Entry, EntryPayload, LogId, StorageError, StoredMembership};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use ledger_storage::StorageEngine;
use ledger_types::{Hash, NamespaceId, VaultId, ZERO_HASH};

use crate::types::{
    LedgerNodeId, LedgerRequest, LedgerResponse, LedgerTypeConfig, SystemRequest,
};

// ============================================================================
// State Machine State
// ============================================================================

/// Applied state that is tracked for snapshots.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppliedState {
    /// Last applied log ID.
    pub last_applied: Option<LogId<LedgerNodeId>>,
    /// Stored membership configuration.
    pub membership: Option<StoredMembership<LedgerNodeId, openraft::BasicNode>>,
    /// Sequence counters for ID generation.
    pub sequences: SequenceCounters,
    /// Per-vault heights for deterministic block heights.
    pub vault_heights: HashMap<(NamespaceId, VaultId), u64>,
    /// Vault health status.
    pub vault_health: HashMap<(NamespaceId, VaultId), VaultHealthStatus>,
}

/// Sequence counters for deterministic ID generation.
///
/// Per DESIGN.md lines 222-252, the state machine leader generates IDs
/// sequentially to ensure determinism across replicas.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SequenceCounters {
    /// Next namespace ID (starts at 1, 0 = _system).
    pub namespace: NamespaceId,
    /// Next vault ID.
    pub vault: VaultId,
    /// Next user ID.
    pub user: i64,
    /// Next user email ID.
    pub user_email: i64,
    /// Next email verification token ID.
    pub email_verify: i64,
}

impl SequenceCounters {
    /// Create new counters with initial values.
    pub fn new() -> Self {
        Self {
            namespace: 1, // 0 is reserved for _system
            vault: 1,
            user: 1,
            user_email: 1,
            email_verify: 1,
        }
    }

    /// Get and increment the next namespace ID.
    pub fn next_namespace(&mut self) -> NamespaceId {
        let id = self.namespace;
        self.namespace += 1;
        id
    }

    /// Get and increment the next vault ID.
    pub fn next_vault(&mut self) -> VaultId {
        let id = self.vault;
        self.vault += 1;
        id
    }

    /// Get and increment the next user ID.
    pub fn next_user(&mut self) -> i64 {
        let id = self.user;
        self.user += 1;
        id
    }
}

/// Health status for a vault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultHealthStatus {
    /// Vault is healthy and accepting writes.
    Healthy,
    /// Vault has diverged and is in recovery.
    Diverged {
        /// Expected state root.
        expected: Hash,
        /// Actual computed state root.
        computed: Hash,
        /// Height at which divergence was detected.
        at_height: u64,
    },
}

impl Default for VaultHealthStatus {
    fn default() -> Self {
        Self::Healthy
    }
}

// ============================================================================
// State Machine
// ============================================================================

/// Ledger state machine for Raft consensus.
///
/// This state machine applies committed log entries to the storage layer
/// and maintains the applied state for snapshots.
pub struct LedgerStateMachine {
    /// Storage engine for persisting state.
    storage: Arc<StorageEngine>,
    /// Applied state (for snapshots).
    state: RwLock<AppliedState>,
}

impl LedgerStateMachine {
    /// Create a new state machine with the given storage engine.
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            state: RwLock::new(AppliedState {
                sequences: SequenceCounters::new(),
                ..Default::default()
            }),
        }
    }

    /// Apply a single request and return the response.
    fn apply_request(
        &self,
        request: &LedgerRequest,
        state: &mut AppliedState,
    ) -> LedgerResponse {
        match request {
            LedgerRequest::Write {
                namespace_id,
                vault_id,
                transactions,
            } => {
                // Check vault health
                let key = (*namespace_id, *vault_id);
                if let Some(VaultHealthStatus::Diverged { .. }) = state.vault_health.get(&key) {
                    return LedgerResponse::Error {
                        message: format!("Vault {}:{} is diverged and not accepting writes", namespace_id, vault_id),
                    };
                }

                // Get current vault height
                let current_height = state.vault_heights.get(&key).copied().unwrap_or(0);
                let new_height = current_height + 1;

                // Apply transactions to storage
                // For MVP, we compute a simple hash - production would use proper Merkle tree
                let block_hash = self.compute_block_hash(*namespace_id, *vault_id, new_height);

                // Update vault height
                state.vault_heights.insert(key, new_height);

                LedgerResponse::Write {
                    block_height: new_height,
                    block_hash,
                }
            }

            LedgerRequest::CreateNamespace { name } => {
                let namespace_id = state.sequences.next_namespace();

                // In a full implementation, we would:
                // 1. Create namespace entry in _system
                // 2. Assign to a shard
                // For MVP, just return the ID

                LedgerResponse::NamespaceCreated { namespace_id }
            }

            LedgerRequest::CreateVault {
                namespace_id,
                name: _,
            } => {
                let vault_id = state.sequences.next_vault();

                // Initialize vault height
                let key = (*namespace_id, vault_id);
                state.vault_heights.insert(key, 0);
                state.vault_health.insert(key, VaultHealthStatus::Healthy);

                LedgerResponse::VaultCreated { vault_id }
            }

            LedgerRequest::System(system_request) => {
                self.apply_system_request(system_request, state)
            }
        }
    }

    /// Apply a system request.
    fn apply_system_request(
        &self,
        request: &SystemRequest,
        state: &mut AppliedState,
    ) -> LedgerResponse {
        match request {
            SystemRequest::CreateUser { name, email } => {
                let user_id = state.sequences.next_user();

                // In a full implementation, we would:
                // 1. Create user entity in _system
                // 2. Create user_email entity
                // 3. Create email index entry
                // For MVP, just return the ID

                LedgerResponse::UserCreated { user_id }
            }

            SystemRequest::AddNode { node_id, address } => {
                // Node membership is handled by Raft membership changes
                LedgerResponse::Empty
            }

            SystemRequest::RemoveNode { node_id } => {
                // Node membership is handled by Raft membership changes
                LedgerResponse::Empty
            }

            SystemRequest::UpdateNamespaceRouting {
                namespace_id,
                shard_id,
            } => {
                // Update namespace routing table in _system
                LedgerResponse::Empty
            }
        }
    }

    /// Compute a block hash for a vault.
    ///
    /// This is a simplified implementation for MVP.
    /// Production would compute proper Merkle tree roots.
    fn compute_block_hash(&self, namespace_id: NamespaceId, vault_id: VaultId, height: u64) -> Hash {
        use ledger_types::sha256;

        let mut data = Vec::new();
        data.extend_from_slice(&namespace_id.to_le_bytes());
        data.extend_from_slice(&vault_id.to_le_bytes());
        data.extend_from_slice(&height.to_le_bytes());
        sha256(&data)
    }

    /// Mark a vault as diverged.
    pub fn mark_vault_diverged(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        expected: Hash,
        computed: Hash,
        at_height: u64,
    ) {
        let key = (namespace_id, vault_id);
        self.state.write().vault_health.insert(
            key,
            VaultHealthStatus::Diverged {
                expected,
                computed,
                at_height,
            },
        );
    }

    /// Check if a vault is healthy.
    pub fn is_vault_healthy(&self, namespace_id: NamespaceId, vault_id: VaultId) -> bool {
        let key = (namespace_id, vault_id);
        self.state
            .read()
            .vault_health
            .get(&key)
            .map(|s| matches!(s, VaultHealthStatus::Healthy))
            .unwrap_or(true)
    }
}

// ============================================================================
// RaftStateMachine Implementation
// ============================================================================

impl RaftStateMachine<LedgerTypeConfig> for LedgerStateMachine {
    type SnapshotBuilder = LedgerSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<LedgerNodeId>>,
            StoredMembership<LedgerNodeId, openraft::BasicNode>,
        ),
        StorageError<LedgerNodeId>,
    > {
        let state = self.state.read();
        Ok((
            state.last_applied,
            state
                .membership
                .clone()
                .unwrap_or_else(StoredMembership::default),
        ))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<LedgerResponse>, StorageError<LedgerNodeId>>
    where
        I: IntoIterator<Item = Entry<LedgerTypeConfig>> + Send,
    {
        let mut responses = Vec::new();
        let mut state = self.state.write();

        for entry in entries {
            // Update last applied
            state.last_applied = Some(*entry.get_log_id());

            // Handle entry payload
            let response = match entry.payload {
                EntryPayload::Blank => LedgerResponse::Empty,

                EntryPayload::Normal(request) => self.apply_request(&request, &mut state),

                EntryPayload::Membership(membership) => {
                    state.membership = Some(membership);
                    LedgerResponse::Empty
                }
            };

            responses.push(response);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        LedgerSnapshotBuilder {
            state: self.state.read().clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<LedgerNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<LedgerNodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<LedgerNodeId>> {
        let data = snapshot.into_inner();

        // Deserialize state from snapshot
        let new_state: AppliedState = bincode::deserialize(&data).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
            )
        })?;

        *self.state.write() = new_state;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<Snapshot<LedgerTypeConfig>>,
        StorageError<LedgerNodeId>,
    > {
        let state = self.state.read();

        if state.last_applied.is_none() {
            return Ok(None);
        }

        let data = bincode::serialize(&*state).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            )
        })?;

        let snapshot_id = format!(
            "snapshot-{}-{}",
            state.last_applied.as_ref().map(|l| l.index).unwrap_or(0),
            chrono::Utc::now().timestamp()
        );

        Ok(Some(Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: state.last_applied,
                last_membership: state.membership.clone().unwrap_or_default(),
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

// ============================================================================
// Snapshot Builder
// ============================================================================

/// Snapshot builder for the ledger state machine.
pub struct LedgerSnapshotBuilder {
    /// State to snapshot.
    state: AppliedState,
}

impl RaftSnapshotBuilder<LedgerTypeConfig> for LedgerSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<LedgerTypeConfig>, StorageError<LedgerNodeId>> {
        let data = bincode::serialize(&self.state).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            )
        })?;

        let snapshot_id = format!(
            "snapshot-{}-{}",
            self.state.last_applied.as_ref().map(|l| l.index).unwrap_or(0),
            chrono::Utc::now().timestamp()
        );

        Ok(Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: self.state.last_applied,
                last_membership: self.state.membership.clone().unwrap_or_default(),
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_types::Transaction;
    use tempfile::tempdir;

    fn create_test_storage() -> Arc<StorageEngine> {
        let dir = tempdir().expect("create temp dir");
        Arc::new(StorageEngine::open(dir.path().join("storage.redb")).expect("open storage"))
    }

    #[test]
    fn test_sequence_counters() {
        let mut counters = SequenceCounters::new();

        assert_eq!(counters.next_namespace(), 1);
        assert_eq!(counters.next_namespace(), 2);
        assert_eq!(counters.next_vault(), 1);
        assert_eq!(counters.next_vault(), 2);
        assert_eq!(counters.next_user(), 1);
        assert_eq!(counters.next_user(), 2);
    }

    #[tokio::test]
    async fn test_create_namespace() {
        let storage = create_test_storage();
        let sm = LedgerStateMachine::new(storage);
        let mut state = sm.state.write();

        let request = LedgerRequest::CreateNamespace {
            name: "test-namespace".to_string(),
        };

        let response = sm.apply_request(&request, &mut state);

        match response {
            LedgerResponse::NamespaceCreated { namespace_id } => {
                assert_eq!(namespace_id, 1);
            }
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_create_vault() {
        let storage = create_test_storage();
        let sm = LedgerStateMachine::new(storage);
        let mut state = sm.state.write();

        let request = LedgerRequest::CreateVault {
            namespace_id: 1,
            name: Some("test-vault".to_string()),
        };

        let response = sm.apply_request(&request, &mut state);

        match response {
            LedgerResponse::VaultCreated { vault_id } => {
                assert_eq!(vault_id, 1);
                // Verify vault was initialized
                assert_eq!(state.vault_heights.get(&(1, 1)), Some(&0));
                assert!(matches!(
                    state.vault_health.get(&(1, 1)),
                    Some(VaultHealthStatus::Healthy)
                ));
            }
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_write_to_vault() {
        let storage = create_test_storage();
        let sm = LedgerStateMachine::new(storage);
        let mut state = sm.state.write();

        // First create the vault
        let create_request = LedgerRequest::CreateVault {
            namespace_id: 1,
            name: None,
        };
        sm.apply_request(&create_request, &mut state);

        // Now write to it
        let write_request = LedgerRequest::Write {
            namespace_id: 1,
            vault_id: 1,
            transactions: vec![],
        };

        let response = sm.apply_request(&write_request, &mut state);

        match response {
            LedgerResponse::Write { block_height, .. } => {
                assert_eq!(block_height, 1);
            }
            _ => panic!("unexpected response"),
        }

        // Write again
        let response2 = sm.apply_request(&write_request, &mut state);
        match response2 {
            LedgerResponse::Write { block_height, .. } => {
                assert_eq!(block_height, 2);
            }
            _ => panic!("unexpected response"),
        }
    }

    #[tokio::test]
    async fn test_diverged_vault_rejects_writes() {
        let storage = create_test_storage();
        let sm = LedgerStateMachine::new(storage);

        // Mark vault as diverged
        sm.mark_vault_diverged(1, 1, [1u8; 32], [2u8; 32], 10);

        let mut state = sm.state.write();
        let write_request = LedgerRequest::Write {
            namespace_id: 1,
            vault_id: 1,
            transactions: vec![],
        };

        let response = sm.apply_request(&write_request, &mut state);

        match response {
            LedgerResponse::Error { message } => {
                assert!(message.contains("diverged"));
            }
            _ => panic!("expected error response"),
        }
    }

    #[test]
    fn test_vault_health_check() {
        let storage = create_test_storage();
        let sm = LedgerStateMachine::new(storage);

        // Initially healthy (no entry = healthy)
        assert!(sm.is_vault_healthy(1, 1));

        // Mark as diverged
        sm.mark_vault_diverged(1, 1, [1u8; 32], [2u8; 32], 10);
        assert!(!sm.is_vault_healthy(1, 1));
    }
}
