//! Learner state refresh from voters.
//!
//! Per DESIGN.md §9.3: Learner nodes maintain a cache of system state (namespace
//! registry, routing info) that they periodically refresh from voters. This ensures
//! learners can serve read requests with reasonably fresh data without participating
//! in consensus.
//!
//! ## Refresh Strategy
//!
//! 1. **Passive refresh**: When cache TTL expires, next read triggers refresh
//! 2. **Active refresh**: Background task periodically polls voters
//! 3. **On-demand refresh**: Explicit refresh when stale data detected
//!
//! ## Voter Selection
//!
//! When refreshing, the learner picks a random voter from the current membership.
//! This distributes load across voters and provides resilience against individual
//! voter failures.

use std::{sync::Arc, time::Duration};

use inferadb_ledger_proto::proto::{
    GetSystemStateRequest, system_discovery_service_client::SystemDiscoveryServiceClient,
};
use openraft::Raft;
use parking_lot::RwLock;
use tokio::{sync::mpsc, time::interval};
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::{
    log_storage::AppliedStateAccessor,
    types::{LedgerNodeId, LedgerTypeConfig},
};

/// Default refresh interval for learner background task.
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Default timeout for RPC calls to voters.
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Learner refresh configuration.
#[derive(Debug, Clone)]
pub struct LearnerRefreshConfig {
    /// Interval between background refresh attempts.
    pub refresh_interval: Duration,
    /// Timeout for RPC calls to voters.
    pub rpc_timeout: Duration,
    /// Whether background refresh is enabled.
    pub enabled: bool,
}

impl Default for LearnerRefreshConfig {
    fn default() -> Self {
        Self {
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            enabled: true,
        }
    }
}

/// Cached system state from voters.
///
/// This represents the data that learners cache from the cluster state.
#[derive(Debug, Clone)]
pub struct CachedSystemState {
    /// Version number for cache comparison.
    pub version: u64,
    /// Number of namespaces in the cache.
    pub namespace_count: usize,
    /// Number of vaults in the cache.
    pub vault_count: usize,
    /// Timestamp when this cache was last updated.
    pub last_updated: std::time::Instant,
}

impl Default for CachedSystemState {
    fn default() -> Self {
        Self {
            version: 0,
            namespace_count: 0,
            vault_count: 0,
            last_updated: std::time::Instant::now(),
        }
    }
}

impl CachedSystemState {
    /// Check if the cache is considered fresh.
    pub fn is_fresh(&self, ttl: Duration) -> bool {
        self.last_updated.elapsed() < ttl
    }
}

/// Learner refresh job for keeping learner state synchronized with voters.
///
/// This runs as a background task on learner nodes, periodically fetching
/// fresh state from voters to keep the local cache up to date.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct LearnerRefreshJob {
    /// The Raft instance for membership info.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// Applied state accessor for updating local state.
    /// Reserved for future use when we want to apply state updates locally.
    #[allow(dead_code)] // retained for state access in refresh operations
    applied_state: AppliedStateAccessor,
    /// Cache of system state from voters.
    #[builder(default = Arc::new(RwLock::new(CachedSystemState::default())))]
    cached_state: Arc<RwLock<CachedSystemState>>,
    /// Configuration.
    #[builder(default)]
    config: LearnerRefreshConfig,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl LearnerRefreshJob {
    /// Get a reference to the cached state.
    pub fn cached_state(&self) -> Arc<RwLock<CachedSystemState>> {
        self.cached_state.clone()
    }

    /// Check if this node is a learner (not a voter).
    fn is_learner(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();

        // Check if this node is NOT in the voter set
        !membership.voter_ids().any(|id| id == self.node_id)
    }

    /// Get list of voter addresses from Raft membership.
    fn get_voter_addresses(&self) -> Vec<(LedgerNodeId, String)> {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();

        membership
            .voter_ids()
            .filter_map(|id| membership.get_node(&id).map(|node| (id, node.addr.clone())))
            .collect()
    }

    /// Refresh state from a voter.
    ///
    /// This fetches the latest system state from the specified voter
    /// and updates the local cache if the voter's state is newer.
    pub async fn refresh_from_voter(&self, voter_addr: &str) -> Result<bool, String> {
        debug!(voter_addr, "Attempting to refresh state from voter");

        // Connect to the voter
        let endpoint = format!("http://{}", voter_addr);
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| format!("Invalid endpoint: {}", e))?
            .timeout(self.config.rpc_timeout)
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to voter: {}", e))?;

        let mut client = SystemDiscoveryServiceClient::new(channel);

        // Request system state from voter
        // Use if_version_greater_than=0 to always get full state
        let request = tonic::Request::new(GetSystemStateRequest { if_version_greater_than: 0 });

        let response = client
            .get_system_state(request)
            .await
            .map_err(|e| format!("RPC failed: {}", e))?
            .into_inner();

        // Update cache if voter's state is newer
        let mut cache = self.cached_state.write();
        let voter_version = response.version;

        if voter_version > cache.version {
            cache.version = voter_version;
            cache.namespace_count = response.namespaces.len();
            cache.vault_count = response.namespaces.len(); // One entry per namespace
            cache.last_updated = std::time::Instant::now();

            debug!(
                voter_version,
                namespace_count = cache.namespace_count,
                node_count = response.nodes.len(),
                "Updated learner cache from voter"
            );

            Ok(true) // Cache was updated
        } else {
            cache.last_updated = std::time::Instant::now();
            debug!(
                voter_version,
                local_version = cache.version,
                "Voter state not newer, cache TTL refreshed"
            );
            Ok(false) // No update needed
        }
    }

    /// Attempt to refresh from any available voter.
    ///
    /// Tries to refresh from a randomly selected voter. If that fails,
    /// tries other voters until one succeeds or all fail.
    pub async fn try_refresh(&self) -> Result<bool, String> {
        let voters = self.get_voter_addresses();
        if voters.is_empty() {
            return Err("No voters available for refresh".to_string());
        }

        // Shuffle voters for random selection
        let mut voters = voters;
        use rand::seq::SliceRandom;
        voters.shuffle(&mut rand::rng());

        // Try each voter until one succeeds
        let mut last_error = None;
        for (voter_id, voter_addr) in voters {
            match self.refresh_from_voter(&voter_addr).await {
                Ok(updated) => {
                    return Ok(updated);
                },
                Err(e) => {
                    warn!(voter_id, voter_addr, error = %e, "Failed to refresh from voter");
                    // Classify error type for metrics
                    let error_type = if e.contains("connect") {
                        "connection"
                    } else if e.contains("RPC") {
                        "rpc"
                    } else {
                        "other"
                    };
                    crate::metrics::record_learner_voter_error(voter_id, error_type);
                    last_error = Some(e);
                },
            }
        }

        Err(last_error.unwrap_or_else(|| "No voters available".to_string()))
    }

    /// Run the learner refresh job.
    ///
    /// This should be spawned as a background task on learner nodes.
    /// It will periodically refresh state from voters until shutdown.
    pub async fn run(self, mut shutdown: mpsc::Receiver<()>) {
        if !self.config.enabled {
            info!("Learner refresh is disabled");
            return;
        }

        let mut ticker = interval(self.config.refresh_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            interval_secs = self.config.refresh_interval.as_secs(),
            "Learner refresh job started"
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Some(ref handle) = self.watchdog_handle {
                        handle.store(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    // Only refresh if this node is a learner
                    if !self.is_learner() {
                        debug!("Node is a voter, skipping learner refresh");
                        continue;
                    }

                    // Check if cache is still fresh (scope ensures guard is dropped)
                    let should_skip = {
                        let cache = self.cached_state.read();
                        cache.is_fresh(self.config.refresh_interval * 2)
                    };
                    if should_skip {
                        debug!("Cache still fresh, skipping refresh");
                        continue;
                    }

                    // Cache is stale, record metric and attempt refresh
                    crate::metrics::record_learner_cache_stale();

                    // Attempt refresh with timing
                    let start = std::time::Instant::now();
                    match self.try_refresh().await {
                        Ok(updated) => {
                            let latency = start.elapsed().as_secs_f64();
                            crate::metrics::record_learner_refresh(true, latency);
                            if updated {
                                debug!("Learner cache updated from voter");
                            }
                        }
                        Err(e) => {
                            let latency = start.elapsed().as_secs_f64();
                            crate::metrics::record_learner_refresh(false, latency);
                            warn!(error = %e, "Failed to refresh learner cache from any voter");
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("Learner refresh job shutting down");
                    break;
                }
            }
        }
    }

    /// Start the learner refresh job as a background task.
    ///
    /// Returns a handle to the spawned task. The task runs until dropped.
    /// For graceful shutdown, use `run()` with a shutdown channel instead.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            self.run(shutdown_rx).await;
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LearnerRefreshConfig::default();
        assert_eq!(config.refresh_interval, Duration::from_secs(5));
        assert_eq!(config.rpc_timeout, Duration::from_secs(10));
        assert!(config.enabled);
    }

    #[test]
    fn test_cached_state_freshness() {
        let state =
            CachedSystemState { last_updated: std::time::Instant::now(), ..Default::default() };

        // Should be fresh with 10 second TTL
        assert!(state.is_fresh(Duration::from_secs(10)));
    }

    #[test]
    fn test_cached_state_stale() {
        // Set last_updated to some time in the past
        let state = CachedSystemState {
            last_updated: std::time::Instant::now() - Duration::from_secs(60),
            ..Default::default()
        };

        // Should be stale with 10 second TTL
        assert!(!state.is_fresh(Duration::from_secs(10)));
    }
}
