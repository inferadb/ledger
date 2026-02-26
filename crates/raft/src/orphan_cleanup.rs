//! Cross-organization orphan cleanup job.
//!
//! When users are deleted from `_system`, membership records
//! in org organizations become orphaned. This background job periodically scans
//! for and removes these orphaned records.
//!
//! ## Behavior
//!
//! - Runs hourly on leader (followers skip)
//! - Scans each organization's vaults for memberships referencing deleted users
//! - Removes orphaned membership records and their indexes
//! - Yields between organization scans to avoid I/O bursts
//! - Actor recorded as `system:orphan_cleanup` for audit trail
//!
//! ## Why Eventual Cleanup?
//!
//! Cross-organization atomic deletes would require distributed transactions.
//! Eventual cleanup is simpler, and orphaned memberships are harmless in
//! the interim (user can't authenticate, so membership grants nothing).

use std::{
    collections::HashSet,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{Operation, OrganizationId, Transaction, VaultId};
use openraft::Raft;
use snafu::GenerateImplicitData;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    error::OrphanCleanupError,
    log_storage::AppliedStateAccessor,
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
    },
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig, RaftPayload},
};

/// Default interval between cleanup cycles (1 hour).
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Maximum memberships to process per batch.
const MAX_BATCH_SIZE: usize = 1000;

/// Actor identifier for cleanup operations.
const CLEANUP_ACTOR: &str = "system:orphan_cleanup";

/// System organization ID.
const SYSTEM_ORGANIZATION_ID: OrganizationId = OrganizationId::new(0);

/// System vault ID (vault 0 — shared across organizations for system-level entities).
const SYSTEM_VAULT_ID: VaultId = VaultId::new(0);

/// Orphan cleanup job for cross-organization consistency.
///
/// Runs as a background task, periodically scanning for and removing
/// membership records that reference deleted users.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct OrphanCleanupJob<B: StorageBackend + 'static> {
    /// Raft consensus handle for proposing cleanup operations.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// The shared state layer (internally thread-safe via inferadb-ledger-store MVCC).
    state: Arc<StateLayer<B>>,
    /// Accessor for applied state (organization registry).
    applied_state: AppliedStateAccessor,
    /// Cleanup interval.
    #[builder(default = CLEANUP_INTERVAL)]
    interval: Duration,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl<B: StorageBackend + 'static> OrphanCleanupJob<B> {
    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Returns all deleted user IDs from `_system` organization.
    ///
    /// Paginates through all user entities to avoid truncation.
    /// Returns user IDs where either:
    /// - User has `deleted_at` set
    /// - User has status = `"DELETED"` or `"DELETING"`
    fn get_deleted_user_ids(&self) -> HashSet<i64> {
        let mut deleted = HashSet::new();
        let mut start_after: Option<String> = None;

        loop {
            let entities = match self.state.list_entities(
                SYSTEM_VAULT_ID,
                Some("user:"),
                start_after.as_deref(),
                MAX_BATCH_SIZE,
            ) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "Failed to list users");
                    return deleted;
                },
            };

            if entities.is_empty() {
                break;
            }

            let batch_len = entities.len();

            for entity in entities {
                let key = String::from_utf8_lossy(&entity.key);

                // Parse user ID from key "user:{id}"
                let Some(user_id) = key.strip_prefix("user:").and_then(|s| s.parse::<i64>().ok())
                else {
                    continue;
                };

                // Check if user is deleted
                let Ok(user_data) = serde_json::from_slice::<serde_json::Value>(&entity.value)
                else {
                    continue;
                };

                let is_deleted = user_data.get("deleted_at").is_some()
                    || user_data
                        .get("status")
                        .and_then(|s| s.as_str())
                        .is_some_and(|s| s == "DELETED" || s == "DELETING");

                if is_deleted {
                    deleted.insert(user_id);
                }

                // Track last key for pagination
                start_after = Some(key.into_owned());
            }

            // If we got fewer than the batch size, we've exhausted all results
            if batch_len < MAX_BATCH_SIZE {
                break;
            }
        }

        deleted
    }

    /// Finds orphaned memberships in an organization.
    ///
    /// Scans the organization's vaults for membership entities that reference
    /// deleted users. Returns `(key, user_id)` pairs for orphaned memberships.
    fn find_orphaned_memberships(
        &self,
        organization_id: OrganizationId,
        deleted_users: &HashSet<i64>,
    ) -> Vec<(String, i64)> {
        if deleted_users.is_empty() {
            return Vec::new();
        }

        // Find the organization's vaults. Membership entities are stored
        // in the organization's vaults, not in the _system vault.
        let vaults = self.applied_state.list_vaults(organization_id);

        let mut orphaned = Vec::new();

        // Scan each vault in the organization for membership entities
        for vault in &vaults {
            let entities = match self.state.list_entities(
                vault.vault_id,
                Some("member:"),
                None,
                MAX_BATCH_SIZE,
            ) {
                Ok(e) => e,
                Err(e) => {
                    warn!(
                        organization_id = organization_id.value(),
                        vault_id = vault.vault_id.value(),
                        error = %e,
                        "Failed to list memberships"
                    );
                    continue;
                },
            };

            for entity in entities {
                let key = String::from_utf8_lossy(&entity.key).to_string();

                let Some(member_data) =
                    serde_json::from_slice::<serde_json::Value>(&entity.value).ok()
                else {
                    continue;
                };

                let Some(user_id) = member_data.get("user_id").and_then(|v| v.as_i64()) else {
                    continue;
                };

                if deleted_users.contains(&user_id) {
                    orphaned.push((key, user_id));
                }
            }
        }

        orphaned
    }

    /// Removes orphaned memberships from an organization.
    async fn remove_orphaned_memberships(
        &self,
        organization_id: OrganizationId,
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
            organization: organization_id,
            vault: SYSTEM_VAULT_ID,
            transactions: vec![transaction],
            idempotency_key: [0; 16],
            request_hash: 0,
        };

        self.raft
            .client_write(RaftPayload { request, proposed_at: chrono::Utc::now() })
            .await
            .map_err(|e| OrphanCleanupError::OrphanRaftWrite {
                message: format!("{:?}", e),
                backtrace: snafu::Backtrace::generate(),
            })?;

        info!(organization_id = organization_id.value(), count, "Removed orphaned memberships");

        Ok(count)
    }

    /// Runs a single cleanup cycle.
    async fn run_cycle(&self) {
        // Only leader performs cleanup
        if !self.is_leader() {
            debug!("Skipping orphan cleanup (not leader)");
            return;
        }

        let _trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();

        debug!("Starting orphan cleanup cycle");

        // Step 1: Get all deleted user IDs from _system (paginated)
        let deleted_users = self.get_deleted_user_ids();
        if deleted_users.is_empty() {
            debug!("No deleted users found");

            let duration = cycle_start.elapsed().as_secs_f64();
            record_background_job_duration("orphan_cleanup", duration);
            record_background_job_run("orphan_cleanup", "success");
            record_background_job_items("orphan_cleanup", 0);

            if let Some(ref handle) = self.watchdog_handle {
                handle.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                    Ordering::Relaxed,
                );
            }

            return;
        }

        debug!(count = deleted_users.len(), "Found deleted users");

        // Step 2: For each org organization, find and remove orphaned memberships
        let organizations = self.applied_state.list_organizations();
        let mut total_removed: u64 = 0;

        for ns in organizations {
            // Skip _system organization (organization_id = 0)
            if ns.organization_id == SYSTEM_ORGANIZATION_ID {
                continue;
            }

            let orphaned = self.find_orphaned_memberships(ns.organization_id, &deleted_users);
            if orphaned.is_empty() {
                // Yield between organizations to avoid I/O bursts
                tokio::task::yield_now().await;
                continue;
            }

            debug!(
                organization_id = ns.organization_id.value(),
                count = orphaned.len(),
                "Found orphaned memberships"
            );

            match self.remove_orphaned_memberships(ns.organization_id, orphaned).await {
                Ok(count) => total_removed += count as u64,
                Err(e) => {
                    warn!(
                        organization_id = ns.organization_id.value(),
                        error = %e,
                        "Failed to remove orphaned memberships"
                    );
                },
            }

            // Yield between organizations to avoid burst reads that spike
            // authorization check latency
            tokio::task::yield_now().await;
        }

        if total_removed > 0 {
            info!(total_removed, "Orphan cleanup cycle completed");
        } else {
            debug!("Orphan cleanup cycle completed (no orphans found)");
        }

        // Record cycle metrics
        let duration = cycle_start.elapsed().as_secs_f64();
        record_background_job_duration("orphan_cleanup", duration);
        record_background_job_run("orphan_cleanup", "success");
        record_background_job_items("orphan_cleanup", total_removed);

        // Update watchdog heartbeat
        if let Some(ref handle) = self.watchdog_handle {
            handle.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                Ordering::Relaxed,
            );
        }
    }

    /// Starts the orphan cleanup background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            info!(interval_secs = self.interval.as_secs(), "Orphan cleanup job started");

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
    use std::{collections::HashSet, sync::Arc};

    use inferadb_ledger_state::StateLayer;
    use inferadb_ledger_store::InMemoryBackend;
    use inferadb_ledger_types::{
        Operation, OrganizationId, OrganizationSlug, ShardId, VaultId, VaultSlug,
    };
    use parking_lot::RwLock;

    use super::*;
    use crate::log_storage::{AppliedState, AppliedStateAccessor, OrganizationMeta, VaultMeta};

    /// Creates an in-memory StateLayer for testing.
    fn create_test_state() -> StateLayer<InMemoryBackend> {
        let db = inferadb_ledger_store::Database::open_in_memory().expect("open in-memory db");
        StateLayer::new(Arc::new(db))
    }

    /// Creates an AppliedStateAccessor with org and vault metadata populated.
    fn create_test_accessor(
        orgs: Vec<OrganizationMeta>,
        vaults: Vec<VaultMeta>,
    ) -> AppliedStateAccessor {
        let mut state = AppliedState::default();
        for org in orgs {
            state.organizations.insert(org.organization_id, org);
        }
        for vault in vaults {
            state.vaults.insert((vault.organization_id, vault.vault_id), vault);
        }
        AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)))
    }

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

    /// Verifies that orphan detection correctly identifies deleted users
    /// from the system vault and finds orphaned memberships only in the
    /// target organization's vaults (not the system vault).
    #[test]
    fn test_orphan_detection_with_real_state_layer() {
        let state = create_test_state();
        let system_vault = SYSTEM_VAULT_ID;
        let org_vault = VaultId::new(1);
        // Write users to system vault: user:1 (active), user:2 (deleted)
        let active_user = serde_json::json!({
            "id": 1,
            "name": "Alice",
            "status": "ACTIVE"
        });
        let deleted_user = serde_json::json!({
            "id": 2,
            "name": "Bob",
            "status": "DELETED"
        });

        state
            .apply_operations(
                system_vault,
                &[
                    Operation::SetEntity {
                        key: "user:1".to_string(),
                        value: serde_json::to_vec(&active_user).unwrap(),
                        condition: None,
                        expires_at: None,
                    },
                    Operation::SetEntity {
                        key: "user:2".to_string(),
                        value: serde_json::to_vec(&deleted_user).unwrap(),
                        condition: None,
                        expires_at: None,
                    },
                ],
                1,
            )
            .expect("apply user entities");

        // Write memberships to org vault:
        // member:alice (references active user 1) — should NOT be orphaned
        // member:bob (references deleted user 2) — SHOULD be orphaned
        let membership_alice = serde_json::json!({
            "user_id": 1,
            "role": "admin"
        });
        let membership_bob = serde_json::json!({
            "user_id": 2,
            "role": "member"
        });

        state
            .apply_operations(
                org_vault,
                &[
                    Operation::SetEntity {
                        key: "member:alice".to_string(),
                        value: serde_json::to_vec(&membership_alice).unwrap(),
                        condition: None,
                        expires_at: None,
                    },
                    Operation::SetEntity {
                        key: "member:bob".to_string(),
                        value: serde_json::to_vec(&membership_bob).unwrap(),
                        condition: None,
                        expires_at: None,
                    },
                ],
                1,
            )
            .expect("apply membership entities");

        // Verify deleted user detection via list_entities on system vault
        let users =
            state.list_entities(system_vault, Some("user:"), None, 1000).expect("list users");
        assert_eq!(users.len(), 2, "should find both users in system vault");

        let mut deleted_users = HashSet::new();
        for entity in &users {
            let key = String::from_utf8_lossy(&entity.key);
            let Some(user_id) = key.strip_prefix("user:").and_then(|s| s.parse::<i64>().ok())
            else {
                continue;
            };
            let Ok(user_data) = serde_json::from_slice::<serde_json::Value>(&entity.value) else {
                continue;
            };
            let is_deleted = user_data.get("deleted_at").is_some()
                || user_data
                    .get("status")
                    .and_then(|s| s.as_str())
                    .is_some_and(|s| s == "DELETED" || s == "DELETING");
            if is_deleted {
                deleted_users.insert(user_id);
            }
        }

        assert_eq!(deleted_users.len(), 1);
        assert!(deleted_users.contains(&2));

        // Verify membership detection in org vault
        let memberships =
            state.list_entities(org_vault, Some("member:"), None, 1000).expect("list memberships");
        assert_eq!(memberships.len(), 2, "should find both memberships in org vault");

        // Check orphan detection logic
        let mut orphaned = Vec::new();
        for entity in &memberships {
            let key = String::from_utf8_lossy(&entity.key).to_string();
            let Some(member_data) = serde_json::from_slice::<serde_json::Value>(&entity.value).ok()
            else {
                continue;
            };
            let Some(user_id) = member_data.get("user_id").and_then(|v| v.as_i64()) else {
                continue;
            };
            if deleted_users.contains(&user_id) {
                orphaned.push((key, user_id));
            }
        }

        assert_eq!(orphaned.len(), 1, "should find exactly one orphaned membership");
        assert_eq!(orphaned[0].0, "member:bob");
        assert_eq!(orphaned[0].1, 2);
    }

    /// Verifies that memberships in a different organization's vault are NOT
    /// picked up when scanning a specific organization.
    #[test]
    fn test_orphan_detection_scopes_to_correct_organization() {
        let state = create_test_state();
        let org1_vault = VaultId::new(1);
        let org2_vault = VaultId::new(2);

        // Set up deleted user set
        let deleted_users: HashSet<i64> = HashSet::from([42]);

        // Write orphaned membership to org1's vault
        let membership = serde_json::json!({
            "user_id": 42,
            "role": "member"
        });

        state
            .apply_operations(
                org1_vault,
                &[Operation::SetEntity {
                    key: "member:orphan".to_string(),
                    value: serde_json::to_vec(&membership).unwrap(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .expect("apply to org1 vault");

        // Write non-orphaned membership to org2's vault
        let non_orphan = serde_json::json!({
            "user_id": 99,
            "role": "admin"
        });

        state
            .apply_operations(
                org2_vault,
                &[Operation::SetEntity {
                    key: "member:safe".to_string(),
                    value: serde_json::to_vec(&non_orphan).unwrap(),
                    condition: None,
                    expires_at: None,
                }],
                1,
            )
            .expect("apply to org2 vault");

        // Set up accessor so org1 has vault 1, org2 has vault 2
        let accessor = create_test_accessor(
            vec![
                OrganizationMeta {
                    organization_id: OrganizationId::new(1),
                    slug: OrganizationSlug::new(100),
                    name: "Org1".to_string(),
                    shard_id: ShardId::new(0),
                    status: Default::default(),
                    pending_shard_id: None,
                    quota: None,
                    storage_bytes: 0,
                },
                OrganizationMeta {
                    organization_id: OrganizationId::new(2),
                    slug: OrganizationSlug::new(200),
                    name: "Org2".to_string(),
                    shard_id: ShardId::new(0),
                    status: Default::default(),
                    pending_shard_id: None,
                    quota: None,
                    storage_bytes: 0,
                },
            ],
            vec![
                VaultMeta {
                    organization_id: OrganizationId::new(1),
                    vault_id: org1_vault,
                    slug: VaultSlug::new(1000),
                    name: Some("vault1".to_string()),
                    deleted: false,
                    last_write_timestamp: 0,
                    retention_policy: Default::default(),
                },
                VaultMeta {
                    organization_id: OrganizationId::new(2),
                    vault_id: org2_vault,
                    slug: VaultSlug::new(2000),
                    name: Some("vault2".to_string()),
                    deleted: false,
                    last_write_timestamp: 0,
                    retention_policy: Default::default(),
                },
            ],
        );

        // Scan org1's vaults — should find the orphan
        let org1_vaults = accessor.list_vaults(OrganizationId::new(1));
        assert_eq!(org1_vaults.len(), 1);

        let mut org1_orphans = Vec::new();
        for vault in &org1_vaults {
            let entities = state
                .list_entities(vault.vault_id, Some("member:"), None, MAX_BATCH_SIZE)
                .expect("list memberships");
            for entity in entities {
                let key = String::from_utf8_lossy(&entity.key).to_string();
                let Some(member_data) =
                    serde_json::from_slice::<serde_json::Value>(&entity.value).ok()
                else {
                    continue;
                };
                let Some(user_id) = member_data.get("user_id").and_then(|v| v.as_i64()) else {
                    continue;
                };
                if deleted_users.contains(&user_id) {
                    org1_orphans.push((key, user_id));
                }
            }
        }

        assert_eq!(org1_orphans.len(), 1, "org1 should have one orphaned membership");
        assert_eq!(org1_orphans[0].0, "member:orphan");

        // Scan org2's vaults — should NOT find any orphans (user 99 is not deleted)
        let org2_vaults = accessor.list_vaults(OrganizationId::new(2));
        assert_eq!(org2_vaults.len(), 1);

        let mut org2_orphans = Vec::new();
        for vault in &org2_vaults {
            let entities = state
                .list_entities(vault.vault_id, Some("member:"), None, MAX_BATCH_SIZE)
                .expect("list memberships");
            for entity in entities {
                let Some(member_data) =
                    serde_json::from_slice::<serde_json::Value>(&entity.value).ok()
                else {
                    continue;
                };
                let Some(user_id) = member_data.get("user_id").and_then(|v| v.as_i64()) else {
                    continue;
                };
                if deleted_users.contains(&user_id) {
                    let key = String::from_utf8_lossy(&entity.key).to_string();
                    org2_orphans.push((key, user_id));
                }
            }
        }

        assert!(org2_orphans.is_empty(), "org2 should have no orphaned memberships");
    }

    /// Verifies pagination in user listing handles large datasets correctly.
    #[test]
    fn test_user_listing_pagination() {
        let state = create_test_state();
        let system_vault = VaultId::new(0);

        // Create more users than MAX_BATCH_SIZE to exercise pagination
        let batch_count = MAX_BATCH_SIZE + 50;
        let mut ops = Vec::with_capacity(batch_count);
        for i in 0..batch_count {
            let status = if i % 3 == 0 { "DELETED" } else { "ACTIVE" };
            let user = serde_json::json!({
                "id": i,
                "name": format!("user-{}", i),
                "status": status
            });
            ops.push(Operation::SetEntity {
                key: format!("user:{}", i),
                value: serde_json::to_vec(&user).unwrap(),
                condition: None,
                expires_at: None,
            });
        }

        state.apply_operations(system_vault, &ops, 1).expect("apply batch entities");

        // Paginate through all users using the same logic as get_deleted_user_ids
        let mut deleted = HashSet::new();
        let mut start_after: Option<String> = None;
        let mut total_seen = 0usize;

        loop {
            let entities = state
                .list_entities(system_vault, Some("user:"), start_after.as_deref(), MAX_BATCH_SIZE)
                .expect("list users");

            if entities.is_empty() {
                break;
            }

            let batch_len = entities.len();
            total_seen += batch_len;

            for entity in entities {
                let key = String::from_utf8_lossy(&entity.key);
                let Some(user_id) = key.strip_prefix("user:").and_then(|s| s.parse::<i64>().ok())
                else {
                    continue;
                };
                let Ok(user_data) = serde_json::from_slice::<serde_json::Value>(&entity.value)
                else {
                    continue;
                };
                let is_deleted = user_data
                    .get("status")
                    .and_then(|s| s.as_str())
                    .is_some_and(|s| s == "DELETED" || s == "DELETING");
                if is_deleted {
                    deleted.insert(user_id);
                }
                start_after = Some(key.into_owned());
            }

            if batch_len < MAX_BATCH_SIZE {
                break;
            }
        }

        assert_eq!(total_seen, batch_count, "pagination should see all users");

        // Every 3rd user (i % 3 == 0) is deleted: 0, 3, 6, ..., up to batch_count
        let expected_deleted = (0..batch_count).filter(|i| i % 3 == 0).count();
        assert_eq!(deleted.len(), expected_deleted, "should detect all deleted users across pages");
    }
}
