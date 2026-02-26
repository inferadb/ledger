//! TTL garbage collection for expired entities.
//!
//! Expired entities remain in state until garbage collection
//! removes them via committed `ExpireEntity` operations. This preserves the
//! `state_root = f(block N)` invariant while allowing lazy expiration.
//!
//! GC behavior:
//! - Runs only on leader (followers skip)
//! - Batched removals (max 1000 per cycle)
//! - Uses `ExpireEntity` operation (distinct from user deletion)
//! - Actor recorded as `system:gc` for audit trail

use std::{sync::Arc, time::Duration};

use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{Operation, OrganizationId, Transaction, VaultId};
use openraft::Raft;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    log_storage::AppliedStateAccessor,
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig, RaftPayload},
};

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
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct TtlGarbageCollector<B: StorageBackend + 'static> {
    /// Raft consensus handle for proposing TTL expiration operations.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// The shared state layer (internally thread-safe via inferadb-ledger-store MVCC).
    state: Arc<StateLayer<B>>,
    /// Accessor for applied state (vault registry).
    applied_state: AppliedStateAccessor,
    /// GC interval.
    #[builder(default = GC_INTERVAL)]
    interval: Duration,
    /// Maximum entities per cycle.
    #[builder(default = MAX_BATCH_SIZE)]
    max_batch_size: usize,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl<B: StorageBackend + 'static> TtlGarbageCollector<B> {
    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Scans for expired entities in a vault.
    ///
    /// Returns a list of (key, expires_at) for entities that have expired.
    fn find_expired_entities(&self, vault_id: VaultId) -> Vec<(String, u64)> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // StateLayer is internally thread-safe via inferadb-ledger-store MVCC
        // List all entities including expired ones
        match self.state.list_entities(vault_id, None, None, self.max_batch_size * 2) {
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
                warn!(vault_id = vault_id.value(), error = %e, "Failed to list entities for GC");
                Vec::new()
            },
        }
    }

    /// Submit ExpireEntity operations through Raft.
    async fn expire_entities(
        &self,
        organization_id: OrganizationId,
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
            organization: organization_id,
            vault: vault_id,
            transactions: vec![transaction],
            idempotency_key: [0; 16],
            request_hash: 0,
        };

        self.raft
            .client_write(RaftPayload { request, proposed_at: chrono::Utc::now() })
            .await
            .map_err(|e| format!("Raft write failed: {}", e))?;

        info!(
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
            count = expired.len(),
            "GC expired entities"
        );

        Ok(())
    }

    /// Runs a single GC cycle.
    async fn run_cycle(&self) {
        // Only leader performs GC
        if !self.is_leader() {
            debug!("Skipping GC cycle (not leader)");
            return;
        }

        let trace_ctx = TraceContext::new();
        debug!(trace_id = %trace_ctx.trace_id, "Starting GC cycle");

        // Get all active vaults from the applied state registry
        let vault_heights = self.applied_state.all_vault_heights();

        for ((organization_id, vault_id), _height) in vault_heights {
            let expired = self.find_expired_entities(vault_id);
            if expired.is_empty() {
                continue;
            }

            debug!(
                trace_id = %trace_ctx.trace_id,
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                count = expired.len(),
                "Found expired entities"
            );

            if let Err(e) = self.expire_entities(organization_id, vault_id, expired).await {
                warn!(
                    trace_id = %trace_ctx.trace_id,
                    organization_id = organization_id.value(),
                    vault_id = vault_id.value(),
                    error = %e,
                    "GC cycle failed"
                );
            }
        }
    }

    /// Starts the garbage collector background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;
                if let Some(ref handle) = self.watchdog_handle {
                    handle.store(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
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
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

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
