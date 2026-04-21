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
use inferadb_ledger_types::{
    ClientId, Operation, OrganizationId, Transaction, VaultId, trace_context::TraceContext,
};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    consensus_handle::ConsensusHandle,
    log_storage::AppliedStateAccessor,
    types::{RaftPayload, LedgerRequest, OrganizationRequest},
};

/// Default interval between GC cycles.
const GC_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum entities to expire per cycle.
const MAX_BATCH_SIZE: usize = 1000;

/// Client ID for GC operations.
const GC_CLIENT_ID: &str = "system:gc";

/// TTL garbage collector for expired entities.
///
/// Runs as a background task, periodically scanning for and removing
/// expired entities through Raft consensus.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct TtlGarbageCollector<B: StorageBackend + 'static> {
    /// Consensus handle for proposing TTL expiration operations.
    handle: Arc<ConsensusHandle>,
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
    /// Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
}

impl<B: StorageBackend + 'static> TtlGarbageCollector<B> {
    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        self.handle.is_leader()
    }

    /// Scans for expired entities in a vault.
    ///
    /// Returns a list of (key, expires_at) for entities that have expired.
    fn find_expired_entities(&self, vault: VaultId) -> Vec<(String, u64)> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // StateLayer is internally thread-safe via inferadb-ledger-store MVCC
        // List all entities including expired ones
        match self.state.list_entities(vault, None, None, self.max_batch_size * 2) {
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
                warn!(vault = vault.value(), error = %e, "Failed to list entities for GC");
                Vec::new()
            },
        }
    }

    /// Submit ExpireEntity operations through Raft.
    async fn expire_entities(
        &self,
        organization: OrganizationId,
        vault: VaultId,
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
            client_id: ClientId::new(GC_CLIENT_ID),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations,
            timestamp: chrono::Utc::now(),
        };

        let request = LedgerRequest::Organization(OrganizationRequest::Write {            vault,
            transactions: vec![transaction],
            idempotency_key: [0; 16],
            request_hash: 0,
        });

        self.handle
            .propose(RaftPayload::system(request))
            .await
            .map_err(|e| format!("Raft write failed: {}", e))?;

        info!(
            organization = organization.value(),
            vault = vault.value(),
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

        let mut job = crate::logging::JobContext::new("ttl_gc", None);
        let trace_ctx = TraceContext::new();
        debug!(trace_id = %trace_ctx.trace_id, "Starting GC cycle");

        // Get all active vaults from the applied state registry
        let mut vault_keys = Vec::new();
        self.applied_state.for_each_vault_height(|org, vault, _| vault_keys.push((org, vault)));

        for (organization, vault) in vault_keys {
            let expired = self.find_expired_entities(vault);
            if expired.is_empty() {
                continue;
            }

            debug!(
                trace_id = %trace_ctx.trace_id,
                organization = organization.value(),
                vault = vault.value(),
                count = expired.len(),
                "Found expired entities"
            );

            let expired_count = expired.len() as u64;
            if let Err(e) = self.expire_entities(organization, vault, expired).await {
                warn!(
                    trace_id = %trace_ctx.trace_id,
                    organization = organization.value(),
                    vault = vault.value(),
                    error = %e,
                    "GC cycle failed"
                );
                job.set_failure();
            } else {
                job.record_items(expired_count);
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
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Some(ref handle) = self.watchdog_handle {
                            handle.store(
                                crate::graceful_shutdown::watchdog_now_nanos(),
                                std::sync::atomic::Ordering::Relaxed,
                            );
                        }
                        self.run_cycle().await;
                    }
                    _ = self.cancellation_token.cancelled() => {
                        info!("TtlGarbageCollector shutting down");
                        break;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn constants_are_reasonable() {
        assert_eq!(GC_INTERVAL, Duration::from_secs(60));
        assert_eq!(MAX_BATCH_SIZE, 1000);
        assert_eq!(GC_CLIENT_ID, "system:gc");
    }

    #[test]
    fn gc_client_id_round_trip() {
        let client_id = ClientId::new(GC_CLIENT_ID);
        let s: &str = client_id.as_ref();
        assert_eq!(s, "system:gc");
    }

    #[test]
    fn expire_entity_operation_construction() {
        let expired = [("entity:1".to_string(), 1000u64), ("entity:2".to_string(), 2000u64)];

        let operations: Vec<Operation> = expired
            .iter()
            .map(|(key, expired_at)| Operation::ExpireEntity {
                key: key.clone(),
                expired_at: *expired_at,
            })
            .collect();

        assert_eq!(operations.len(), 2);
        match &operations[0] {
            Operation::ExpireEntity { key, expired_at } => {
                assert_eq!(key, "entity:1");
                assert_eq!(*expired_at, 1000);
            },
            _ => unreachable!("expected ExpireEntity"),
        }
    }

    #[test]
    fn expiry_filtering_logic() {
        // Simulate the filtering from find_expired_entities
        let now = 5000u64;
        let entities = [
            (b"active".to_vec(), 0u64),      // no TTL (expires_at = 0)
            (b"expired".to_vec(), 3000u64),  // expired (3000 < 5000)
            (b"future".to_vec(), 10000u64),  // not expired yet
            (b"boundary".to_vec(), 5000u64), // exactly at now (not expired: < not <=)
        ];

        let expired: Vec<_> = entities
            .iter()
            .filter(|(_, expires_at)| *expires_at > 0 && *expires_at < now)
            .collect();

        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, b"expired".to_vec());
    }

    #[test]
    fn transaction_construction() {
        let ops = vec![Operation::ExpireEntity { key: "test:key".to_string(), expired_at: 12345 }];

        let transaction = Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: ClientId::new(GC_CLIENT_ID),
            sequence: 0,
            operations: ops,
            timestamp: chrono::Utc::now(),
        };

        let s: &str = transaction.client_id.as_ref();
        assert_eq!(s, GC_CLIENT_ID);
        assert_eq!(transaction.operations.len(), 1);
        assert_eq!(transaction.id.len(), 16);
    }
}
