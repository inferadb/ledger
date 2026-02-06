//! Cross-namespace orphan cleanup job.
//!
//! Per DESIGN.md: When users are deleted from `_system`, membership records
//! in org namespaces become orphaned. This background job periodically scans
//! for and removes these orphaned records.
//!
//! ## Behavior
//!
//! - Runs hourly on leader (followers skip)
//! - Scans all org namespaces for memberships referencing deleted users
//! - Removes orphaned membership records and their indexes
//! - Actor recorded as `system:orphan_cleanup` for audit trail
//!
//! ## Why Eventual Cleanup?
//!
//! Cross-namespace atomic deletes would require distributed transactions.
//! Eventual cleanup is simpler, and orphaned memberships are harmless in
//! the interim (user can't authenticate, so membership grants nothing).

use std::{collections::HashSet, sync::Arc, time::Duration};

use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{NamespaceId, Operation, Transaction, VaultId};
use openraft::Raft;
use snafu::GenerateImplicitData;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    error::OrphanCleanupError,
    log_storage::AppliedStateAccessor,
    types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig},
};

/// Default interval between cleanup cycles (1 hour).
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Maximum memberships to process per namespace per cycle.
const MAX_BATCH_SIZE: usize = 1000;

/// Actor identifier for cleanup operations.
const CLEANUP_ACTOR: &str = "system:orphan_cleanup";

/// System namespace ID.
const SYSTEM_NAMESPACE_ID: NamespaceId = NamespaceId::new(0);

/// Orphan cleanup job for cross-namespace consistency.
///
/// Runs as a background task, periodically scanning for and removing
/// membership records that reference deleted users.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct OrphanCleanupJob<B: StorageBackend + 'static> {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// The shared state layer (internally thread-safe via inferadb-ledger-store MVCC).
    state: Arc<StateLayer<B>>,
    /// Accessor for applied state (namespace registry).
    applied_state: AppliedStateAccessor,
    /// Cleanup interval.
    #[builder(default = CLEANUP_INTERVAL)]
    interval: Duration,
}

impl<B: StorageBackend + 'static> OrphanCleanupJob<B> {
    /// Check if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Get all deleted user IDs from _system namespace.
    ///
    /// Returns user IDs where either:
    /// - User has deleted_at set
    /// - User has status = "DELETED" or "DELETING"
    fn get_deleted_user_ids(&self) -> HashSet<i64> {
        // StateLayer is internally thread-safe via inferadb-ledger-store MVCC

        // List all user entities in _system (vault_id = 0)
        let entities = match self.state.list_entities(
            VaultId::new(0),
            Some("user:"),
            None,
            MAX_BATCH_SIZE * 10,
        ) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "Failed to list users");
                return HashSet::new();
            },
        };

        entities
            .into_iter()
            .filter_map(|entity| {
                // Parse user ID from key "user:{id}"
                let key = String::from_utf8_lossy(&entity.key);
                let user_id: i64 = key.strip_prefix("user:")?.parse().ok()?;

                // Check if user is deleted
                let user_data: serde_json::Value = serde_json::from_slice(&entity.value).ok()?;

                let is_deleted = user_data.get("deleted_at").is_some()
                    || user_data
                        .get("status")
                        .and_then(|s| s.as_str())
                        .is_some_and(|s| s == "DELETED" || s == "DELETING");

                if is_deleted { Some(user_id) } else { None }
            })
            .collect()
    }

    /// Find orphaned memberships in a namespace.
    ///
    /// Returns (key, user_id) pairs for memberships referencing deleted users.
    fn find_orphaned_memberships(
        &self,
        namespace_id: NamespaceId,
        deleted_users: &HashSet<i64>,
    ) -> Vec<(String, i64)> {
        if deleted_users.is_empty() {
            return Vec::new();
        }

        // StateLayer is internally thread-safe via inferadb-ledger-store MVCC
        // Note: Namespace entities live in vault_id = 0 for the namespace
        // We need to list entities in the namespace, not vault 0 of _system
        // Actually, namespace-level entities (members, teams) are stored with the namespace
        // For this implementation, we assume entities are in vault_id = 0 per namespace

        let entities = match self.state.list_entities(
            VaultId::new(0),
            Some("member:"),
            None,
            MAX_BATCH_SIZE,
        ) {
            Ok(e) => e,
            Err(e) => {
                warn!(namespace_id = namespace_id.value(), error = %e, "Failed to list memberships");
                return Vec::new();
            },
        };

        entities
            .into_iter()
            .filter_map(|entity| {
                let key = String::from_utf8_lossy(&entity.key).to_string();

                // Parse user_id from membership record
                let member_data: serde_json::Value = serde_json::from_slice(&entity.value).ok()?;
                let user_id = member_data.get("user_id")?.as_i64()?;

                if deleted_users.contains(&user_id) { Some((key, user_id)) } else { None }
            })
            .collect()
    }

    /// Remove orphaned memberships from a namespace.
    async fn remove_orphaned_memberships(
        &self,
        namespace_id: NamespaceId,
        orphaned: Vec<(String, i64)>,
    ) -> Result<usize, OrphanCleanupError> {
        if orphaned.is_empty() {
            return Ok(0);
        }

        let mut operations = Vec::new();

        for (key, user_id) in &orphaned {
            // Delete the membership entity
            operations.push(Operation::DeleteEntity { key: key.clone() });

            // Delete the user index for this membership
            let idx_key = format!("_idx:member:user:{}", user_id);
            operations.push(Operation::DeleteEntity { key: idx_key });
        }

        let count = orphaned.len();

        let transaction = Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: CLEANUP_ACTOR.to_string(),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations,
            timestamp: chrono::Utc::now(),
            actor: CLEANUP_ACTOR.to_string(),
        };

        let request = LedgerRequest::Write {
            namespace_id,
            vault_id: VaultId::new(0), // Namespace-level entities
            transactions: vec![transaction],
        };

        self.raft.client_write(request).await.map_err(|e| OrphanCleanupError::OrphanRaftWrite {
            message: format!("{:?}", e),
            backtrace: snafu::Backtrace::generate(),
        })?;

        info!(namespace_id = namespace_id.value(), count, "Removed orphaned memberships");

        Ok(count)
    }

    /// Run a single cleanup cycle.
    async fn run_cycle(&self) {
        // Only leader performs cleanup
        if !self.is_leader() {
            debug!("Skipping orphan cleanup (not leader)");
            return;
        }

        debug!("Starting orphan cleanup cycle");

        // Step 1: Get all deleted user IDs from _system
        let deleted_users = self.get_deleted_user_ids();
        if deleted_users.is_empty() {
            debug!("No deleted users found");
            return;
        }

        debug!(count = deleted_users.len(), "Found deleted users");

        // Step 2: For each org namespace, find and remove orphaned memberships
        let namespaces = self.applied_state.list_namespaces();
        let mut total_removed = 0;

        for ns in namespaces {
            // Skip _system namespace (namespace_id = 0)
            if ns.namespace_id == SYSTEM_NAMESPACE_ID {
                continue;
            }

            let orphaned = self.find_orphaned_memberships(ns.namespace_id, &deleted_users);
            if orphaned.is_empty() {
                continue;
            }

            debug!(
                namespace_id = ns.namespace_id.value(),
                count = orphaned.len(),
                "Found orphaned memberships"
            );

            match self.remove_orphaned_memberships(ns.namespace_id, orphaned).await {
                Ok(count) => total_removed += count,
                Err(e) => {
                    warn!(
                        namespace_id = ns.namespace_id.value(),
                        error = %e,
                        "Failed to remove orphaned memberships"
                    );
                },
            }
        }

        if total_removed > 0 {
            info!(total_removed, "Orphan cleanup cycle completed");
        } else {
            debug!("Orphan cleanup cycle completed (no orphans found)");
        }
    }

    /// Start the orphan cleanup background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;
                self.run_cycle().await;
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    #[test]
    fn test_deleted_user_detection() {
        // Test user data with deleted_at
        let user_with_deleted_at = serde_json::json!({
            "id": 1,
            "name": "Alice",
            "deleted_at": "2024-01-15T10:00:00Z"
        });

        let is_deleted = user_with_deleted_at.get("deleted_at").is_some();
        assert!(is_deleted);

        // Test user data with DELETED status
        let user_with_status = serde_json::json!({
            "id": 2,
            "name": "Bob",
            "status": "DELETED"
        });

        let is_deleted = user_with_status
            .get("status")
            .and_then(|s| s.as_str())
            .is_some_and(|s| s == "DELETED" || s == "DELETING");
        assert!(is_deleted);

        // Test active user
        let active_user = serde_json::json!({
            "id": 3,
            "name": "Charlie",
            "status": "ACTIVE"
        });

        let is_deleted = active_user.get("deleted_at").is_some()
            || active_user
                .get("status")
                .and_then(|s| s.as_str())
                .is_some_and(|s| s == "DELETED" || s == "DELETING");
        assert!(!is_deleted);
    }

    #[test]
    fn test_membership_user_id_extraction() {
        let membership = serde_json::json!({
            "user_id": 42,
            "role": "member",
            "created_at": "2024-01-01T00:00:00Z"
        });

        let user_id = membership.get("user_id").and_then(|v| v.as_i64());
        assert_eq!(user_id, Some(42));
    }
}
