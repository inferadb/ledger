//! Learner state refresh from voters.
//!
//! Learner nodes maintain a cache of system state (organization
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
use parking_lot::RwLock;
use tokio::{sync::mpsc, time::interval};
use tracing::{debug, info, warn};

use crate::{
    consensus_handle::ConsensusHandle, log_storage::AppliedStateAccessor,
    peer_address_map::PeerAddressMap, types::LedgerNodeId,
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
/// Data that learners cache from the cluster state.
#[derive(Debug, Clone)]
pub struct CachedSystemState {
    /// Version number for cache comparison.
    pub version: u64,
    /// Number of organizations in the cache.
    pub organization_count: usize,
    /// Timestamp when this cache was last updated.
    pub last_updated: std::time::Instant,
}

impl Default for CachedSystemState {
    fn default() -> Self {
        Self { version: 0, organization_count: 0, last_updated: std::time::Instant::now() }
    }
}

impl CachedSystemState {
    /// Checks if the cache is considered fresh.
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
    /// Consensus handle for querying cluster state.
    handle: Arc<ConsensusHandle>,
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
    /// Peer address map for resolving voter network addresses.
    #[builder(default)]
    peer_addresses: Option<PeerAddressMap>,
    /// Shared per-node connection registry. Single source of truth for
    /// inter-node HTTP/2 channels — reused across subsystems so each
    /// refresh cycle shares the existing peer connection.
    registry: Arc<crate::node_registry::NodeConnectionRegistry>,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl LearnerRefreshJob {
    /// Returns a reference to the cached state.
    pub fn cached_state(&self) -> Arc<RwLock<CachedSystemState>> {
        self.cached_state.clone()
    }

    /// Checks if this node is a learner (not a voter).
    fn is_learner(&self) -> bool {
        let state = self.handle.shard_state();
        let my_id = self.handle.node_id();
        // A node is a learner if it is not in the voter set
        !state.voters.iter().any(|id| id.0 == my_id)
    }

    /// Returns list of voter addresses from shard state and peer address map.
    fn get_voter_addresses(&self) -> Vec<(LedgerNodeId, String)> {
        let state = self.handle.shard_state();
        let my_id = self.handle.node_id();
        state
            .voters
            .iter()
            .filter(|&&id| id.0 != my_id)
            .filter_map(|id| {
                let addr = self.peer_addresses.as_ref()?.get(id.0)?;
                Some((id.0, addr))
            })
            .collect()
    }

    /// Refreshes state from a voter.
    ///
    /// This fetches the latest system state from the specified voter
    /// and updates the local cache if the voter's state is newer.
    ///
    /// # Errors
    ///
    /// Returns a description string if the endpoint is invalid, the gRPC
    /// connection fails, or the `GetSystemState` RPC returns an error.
    pub async fn refresh_from_voter(
        &self,
        voter_id: LedgerNodeId,
        voter_addr: &str,
    ) -> Result<bool, String> {
        debug!(voter_id, voter_addr, "Attempting to refresh state from voter");

        // Obtain the voter's peer connection from the shared registry.
        // HTTP/2 multiplexes all subsystems over one channel per peer.
        let peer = self
            .registry
            .get_or_register(voter_id, voter_addr)
            .await
            .map_err(|e| format!("register voter {voter_id} ({voter_addr}): {e}"))?;
        let mut client = SystemDiscoveryServiceClient::new(peer.channel());

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
            cache.organization_count = response.organizations.len();
            cache.last_updated = std::time::Instant::now();

            debug!(
                voter_version,
                organization_count = cache.organization_count,
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

    /// Attempts to refresh from any available voter.
    ///
    /// Tries to refresh from a randomly selected voter. If that fails,
    /// tries other voters until one succeeds or all fail.
    ///
    /// # Errors
    ///
    /// Returns a description string if no voters are available or all
    /// voter refresh attempts fail.
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
            match self.refresh_from_voter(voter_id, &voter_addr).await {
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

    /// Runs the learner refresh job.
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

    /// Starts the learner refresh job as a background task.
    ///
    /// Returns a handle to the spawned task. The task runs until dropped.
    /// For graceful shutdown, use `run()` with a shutdown channel instead.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            // Keep the sender alive for the task's lifetime so the shutdown
            // branch in `run()` does not immediately fire when the sender is
            // dropped at the caller (which would cause `recv()` to return
            // `None` on the first tick). Callers needing graceful shutdown
            // should call `run()` directly with a sender they retain.
            let _shutdown_tx_keep_alive = shutdown_tx;
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

    #[test]
    fn test_cached_state_default() {
        let state = CachedSystemState::default();
        assert_eq!(state.version, 0);
        assert_eq!(state.organization_count, 0);
        // Default state should be fresh (just created)
        assert!(state.is_fresh(Duration::from_secs(1)));
    }

    #[test]
    fn test_error_classification_logic() {
        // Simulates the error_type classification from try_refresh
        let connection_error = "Failed to connect to voter: connection refused";
        let rpc_error = "RPC failed: status UNAVAILABLE";
        let other_error = "Invalid endpoint: bad address";

        let classify = |e: &str| -> &str {
            if e.contains("connect") {
                "connection"
            } else if e.contains("RPC") {
                "rpc"
            } else {
                "other"
            }
        };

        assert_eq!(classify(connection_error), "connection");
        assert_eq!(classify(rpc_error), "rpc");
        assert_eq!(classify(other_error), "other");
    }

    #[test]
    fn test_refresh_interval_doubles_as_staleness_threshold() {
        // The run loop uses refresh_interval * 2 as the staleness threshold
        let config = LearnerRefreshConfig::default();
        let staleness_threshold = config.refresh_interval * 2;
        assert_eq!(staleness_threshold, Duration::from_secs(10));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_REFRESH_INTERVAL, Duration::from_secs(5));
        assert_eq!(DEFAULT_RPC_TIMEOUT, Duration::from_secs(10));
    }

    #[test]
    fn test_cache_update_simulation() {
        // Simulate the full cache update logic from refresh_from_voter
        let cache = Arc::new(RwLock::new(CachedSystemState::default()));

        // Initial state
        assert_eq!(cache.read().version, 0);

        // Voter with higher version updates cache
        {
            let mut c = cache.write();
            let voter_version = 5u64;
            if voter_version > c.version {
                c.version = voter_version;
                c.organization_count = 10;
                c.last_updated = std::time::Instant::now();
            }
        }
        assert_eq!(cache.read().version, 5);
        assert_eq!(cache.read().organization_count, 10);

        // Voter with same version only refreshes TTL
        {
            let mut c = cache.write();
            let voter_version = 5u64;
            if voter_version > c.version {
                c.version = voter_version;
            } else {
                c.last_updated = std::time::Instant::now();
            }
        }
        // Version unchanged
        assert_eq!(cache.read().version, 5);
    }

    #[test]
    fn test_cached_state_is_fresh_zero_ttl() {
        let state = CachedSystemState::default();
        // Zero TTL should never be fresh (unless exactly now)
        assert!(!state.is_fresh(Duration::from_secs(0)));
    }

    #[test]
    fn test_cached_state_is_fresh_very_large_ttl() {
        let state = CachedSystemState::default();
        // Very large TTL should always be fresh
        assert!(state.is_fresh(Duration::from_secs(86400 * 365)));
    }

    #[test]
    fn test_error_classification_edge_cases() {
        let classify = |e: &str| -> &str {
            if e.contains("connect") {
                "connection"
            } else if e.contains("RPC") {
                "rpc"
            } else {
                "other"
            }
        };

        // Edge: contains both "connect" and "RPC" — "connect" wins (checked first)
        assert_eq!(classify("RPC connect failure"), "connection");
        // Case-sensitive: "rpc" lowercase doesn't match
        assert_eq!(classify("rpc failed"), "other");
        // Empty string
        assert_eq!(classify(""), "other");
    }

    #[test]
    fn test_config_disabled() {
        let config = LearnerRefreshConfig {
            refresh_interval: Duration::from_secs(5),
            rpc_timeout: Duration::from_secs(10),
            enabled: false,
        };
        assert!(!config.enabled);
    }
}
