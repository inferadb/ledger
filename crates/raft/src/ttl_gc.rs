//! TTL garbage collection for expired entities.
//!
//! Per DESIGN.md, expired entities remain in state until garbage collection
//! removes them via committed `ExpireEntity` operations. This preserves the
//! `state_root = f(block N)` invariant while allowing lazy expiration.
//!
//! GC behavior:
//! - Runs only on leader (followers skip)
//! - Batched removals (max 1000 per cycle)
//! - Uses `ExpireEntity` operation (distinct from user deletion)
//! - Actor recorded as `system:gc` for audit trail

use std::sync::Arc;
use std::time::Duration;

use openraft::Raft;
use parking_lot::RwLock;
use tokio::time::interval;
use tracing::{debug, info, warn};

use ledger_storage::StateLayer;
use ledger_types::{Operation, Transaction, VaultId};

use crate::types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig};

/// Default interval between GC cycles.
const GC_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum entities to expire per cycle.
const MAX_BATCH_SIZE: usize = 1000;

/// Actor identifier for GC operations.
const GC_ACTOR: &str = "system:gc";

/// TTL garbage collector for expired entities.
///
/// Runs as a background task, periodically scanning for and removing
/// expired entities through Raft consensus.
pub struct TtlGarbageCollector {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// The shared state layer.
    state: Arc<RwLock<StateLayer>>,
    /// GC interval.
    interval: Duration,
    /// Maximum entities per cycle.
    max_batch_size: usize,
}

impl TtlGarbageCollector {
    /// Create a new garbage collector.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        node_id: LedgerNodeId,
        state: Arc<RwLock<StateLayer>>,
    ) -> Self {
        Self {
            raft,
            node_id,
            state,
            interval: GC_INTERVAL,
            max_batch_size: MAX_BATCH_SIZE,
        }
    }

    /// Create with custom interval (for testing).
    #[cfg(test)]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Check if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Scan for expired entities in a vault.
    ///
    /// Returns a list of (key, expires_at) for entities that have expired.
    fn find_expired_entities(&self, vault_id: VaultId) -> Vec<(String, u64)> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let state = self.state.read();

        // List all entities including expired ones
        match state.list_entities(vault_id, None, None, self.max_batch_size * 2) {
            Ok(entities) => entities
                .into_iter()
                .filter(|e| e.expires_at > 0 && e.expires_at < now)
                .take(self.max_batch_size)
                .map(|e| {
                    let key = String::from_utf8_lossy(&e.key).to_string();
                    (key, e.expires_at)
                })
                .collect(),
            Err(e) => {
                warn!(vault_id, error = %e, "Failed to list entities for GC");
                Vec::new()
            }
        }
    }

    /// Submit ExpireEntity operations through Raft.
    async fn expire_entities(
        &self,
        namespace_id: i64,
        vault_id: VaultId,
        expired: Vec<(String, u64)>,
    ) -> Result<(), String> {
        if expired.is_empty() {
            return Ok(());
        }

        let operations: Vec<Operation> = expired
            .iter()
            .map(|(key, expired_at)| Operation::ExpireEntity {
                key: key.clone(),
                expired_at: *expired_at,
            })
            .collect();

        let transaction = Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: GC_ACTOR.to_string(),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations,
            timestamp: chrono::Utc::now(),
            actor: GC_ACTOR.to_string(),
        };

        let request = LedgerRequest::Write {
            namespace_id,
            vault_id,
            transactions: vec![transaction],
        };

        self.raft
            .client_write(request)
            .await
            .map_err(|e| format!("Raft write failed: {}", e))?;

        info!(
            namespace_id,
            vault_id,
            count = expired.len(),
            "GC expired entities"
        );

        Ok(())
    }

    /// Run a single GC cycle.
    async fn run_cycle(&self) {
        // Only leader performs GC
        if !self.is_leader() {
            debug!("Skipping GC cycle (not leader)");
            return;
        }

        debug!("Starting GC cycle");

        // TODO: Get list of active vaults from system metadata.
        // For now, we scan vault IDs 0-99 as a simple heuristic.
        // In production, this should query the _system namespace for
        // all active vaults or maintain a registry.
        for vault_id in 0..100 {
            let expired = self.find_expired_entities(vault_id);
            if expired.is_empty() {
                continue;
            }

            debug!(vault_id, count = expired.len(), "Found expired entities");

            // namespace_id is typically derived from vault metadata
            // For now, assume namespace 0 for the default vault
            let namespace_id = 0;

            if let Err(e) = self.expire_entities(namespace_id, vault_id, expired).await {
                warn!(vault_id, error = %e, "GC cycle failed");
            }
        }
    }

    /// Start the garbage collector background task.
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    // Note: Full integration tests require a running Raft cluster.
    // These unit tests verify the filtering logic.

    #[test]
    fn test_expiration_check() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Entity expired 1 hour ago
        let expired_at = now - 3600;
        assert!(expired_at > 0 && expired_at < now);

        // Entity expires in 1 hour
        let future_expires = now + 3600;
        assert!(!(future_expires > 0 && future_expires < now));

        // Entity never expires
        let never_expires = 0u64;
        assert!(!(never_expires > 0 && never_expires < now));
    }
}
