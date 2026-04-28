//! Dependency health checking for readiness and startup probes.
//!
//! Validates external dependencies beyond internal Raft state:
//! - **Disk writability**: touch + delete a temp file in the data directory
//! - **Peer reachability**: gRPC connectivity check to cluster peers
//! - **Raft log lag**: ensures the node isn't too far behind the leader
//! - **RMK (Region Master Key) provisioning**: reports loaded RMK versions per region
//!
//! Results are cached with a configurable TTL (default 5s) to prevent I/O
//! storms from aggressive Kubernetes probe intervals.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_store::crypto::{RegionKeyManager, rmk_versions_for_health};
use inferadb_ledger_types::{config::HealthCheckConfig, types::Region};
use parking_lot::RwLock;

use crate::{consensus_handle::ConsensusHandle, peer_address_map::PeerAddressMap};

/// Result of a single dependency health check.
#[derive(Debug, Clone)]
pub struct DependencyCheckResult {
    /// Whether the check passed.
    pub healthy: bool,
    /// Human-readable detail message.
    pub detail: String,
}

/// Cached result of all dependency checks.
#[derive(Debug, Clone)]
struct CachedResult {
    /// Individual check results keyed by check name.
    results: HashMap<String, DependencyCheckResult>,
    /// When this cache entry was created.
    timestamp: Instant,
}

/// Aggregated dependency health result.
#[derive(Debug, Clone)]
pub struct DependencyHealth {
    /// Whether all dependency checks passed.
    pub all_healthy: bool,
    /// Individual check results keyed by check name.
    pub details: HashMap<String, DependencyCheckResult>,
}

/// Validates external dependencies for health probes.
///
/// Runs disk, peer, Raft lag, and RMK provisioning checks with
/// per-check timeouts and caches results to avoid I/O storms from
/// aggressive Kubernetes probe intervals.
#[derive(Clone)]
pub struct DependencyHealthChecker {
    /// Consensus handle for shard state access.
    handle: Arc<ConsensusHandle>,
    /// Data directory for disk writability checks.
    data_dir: PathBuf,
    /// Configuration for timeouts, cache TTL, and thresholds.
    config: HealthCheckConfig,
    /// Cached check results.
    cache: Arc<RwLock<Option<CachedResult>>>,
    /// Key manager for RMK version reporting.
    key_manager: Option<Arc<dyn RegionKeyManager>>,
    /// Node's region for determining required RMK regions.
    node_region: Option<Region>,
    /// Whether the node's region requires residency. Resolved from the
    /// GLOBAL region directory by the caller.
    node_requires_residency: bool,
    /// Snapshot of known data regions and their `requires_residency` flags
    /// (resolved from the GLOBAL region directory by the caller). Empty
    /// when the registry is unavailable; the disciplined defaults in
    /// `required_regions` keep `Region::GLOBAL` and the node's own region
    /// in the required set regardless.
    known_regions: Vec<(Region, bool)>,
    /// Peer address map for TCP reachability checks.
    peer_addresses: Option<PeerAddressMap>,
}

impl DependencyHealthChecker {
    /// Creates a new dependency health checker.
    pub fn new(handle: Arc<ConsensusHandle>, data_dir: PathBuf, config: HealthCheckConfig) -> Self {
        Self {
            handle,
            data_dir,
            config,
            cache: Arc::new(RwLock::new(None)),
            key_manager: None,
            node_region: None,
            node_requires_residency: inferadb_ledger_state::system::default_requires_residency(),
            known_regions: Vec::new(),
            peer_addresses: None,
        }
    }

    /// Attaches the resolved residency contract for the node's region and
    /// the known set of provisioned data regions, derived from the GLOBAL
    /// region directory.
    pub fn with_region_residency(
        mut self,
        node_requires_residency: bool,
        known_regions: Vec<(Region, bool)>,
    ) -> Self {
        self.node_requires_residency = node_requires_residency;
        self.known_regions = known_regions;
        self
    }

    /// Attaches the peer address map for TCP reachability checks.
    pub fn with_peer_addresses(mut self, peer_addresses: PeerAddressMap) -> Self {
        self.peer_addresses = Some(peer_addresses);
        self
    }

    /// Sets the key manager and node region for RMK health reporting.
    pub fn with_key_manager(
        mut self,
        key_manager: Arc<dyn RegionKeyManager>,
        node_region: Region,
    ) -> Self {
        self.key_manager = Some(key_manager);
        self.node_region = Some(node_region);
        self
    }

    /// Runs all dependency checks, returning cached results if within TTL.
    pub async fn check_all(&self) -> DependencyHealth {
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(cached) = cache.as_ref() {
                let ttl = Duration::from_secs(self.config.health_cache_ttl_secs);
                if cached.timestamp.elapsed() < ttl {
                    let all_healthy = cached.results.values().all(|r| r.healthy);
                    return DependencyHealth { all_healthy, details: cached.results.clone() };
                }
            }
        }

        // Cache miss or expired — run checks
        let mut results = HashMap::new();

        let disk_result = check_disk(&self.data_dir);
        let raft_lag_result = check_raft_lag(&self.handle, self.config.max_raft_lag);
        let default_map = PeerAddressMap::new();
        let peer_addresses = self.peer_addresses.as_ref().unwrap_or(&default_map);
        let peer_result = check_peer_reachability_handle(
            &self.handle,
            peer_addresses,
            Duration::from_secs(self.config.dependency_check_timeout_secs),
        )
        .await;

        results.insert("disk_writable".to_string(), disk_result);
        results.insert("raft_log_lag".to_string(), raft_lag_result);
        results.insert("peer_reachable".to_string(), peer_result);

        // RMK provisioning check
        if let (Some(km), Some(region)) = (&self.key_manager, self.node_region) {
            let rmk_result = check_rmk_provisioning(
                km.as_ref(),
                region,
                self.node_requires_residency,
                self.known_regions.clone(),
            );
            results.insert("rmk_provisioning".to_string(), rmk_result);
        }

        let all_healthy = results.values().all(|r| r.healthy);

        // Update cache
        {
            let mut cache = self.cache.write();
            *cache = Some(CachedResult { results: results.clone(), timestamp: Instant::now() });
        }

        DependencyHealth { all_healthy, details: results }
    }

    /// Checks whether the startup environment is valid.
    ///
    /// Validates that the data directory exists and is writable. This is a
    /// lighter check than `check_all()` suitable for the startup probe.
    pub fn check_startup(&self) -> DependencyHealth {
        let mut results = HashMap::new();

        // Data directory must exist
        let dir_exists = self.data_dir.exists() && self.data_dir.is_dir();
        results.insert(
            "data_dir_exists".to_string(),
            DependencyCheckResult {
                healthy: dir_exists,
                detail: if dir_exists {
                    format!("data directory exists: {}", self.data_dir.display())
                } else {
                    format!(
                        "data directory missing or not a directory: {}",
                        self.data_dir.display()
                    )
                },
            },
        );

        // Data directory must be writable
        if dir_exists {
            let disk_result = check_disk(&self.data_dir);
            results.insert("data_dir_writable".to_string(), disk_result);
        }

        let all_healthy = results.values().all(|r| r.healthy);
        DependencyHealth { all_healthy, details: results }
    }
}

/// Checks disk writability by creating and deleting a probe file.
pub(crate) fn check_disk(data_dir: &std::path::Path) -> DependencyCheckResult {
    let probe_path = data_dir.join(".health_probe");
    match std::fs::write(&probe_path, b"ok") {
        Ok(()) => {
            let _ = std::fs::remove_file(&probe_path);
            DependencyCheckResult { healthy: true, detail: "disk is writable".to_string() }
        },
        Err(e) => {
            DependencyCheckResult { healthy: false, detail: format!("disk write failed: {e}") }
        },
    }
}

/// Checks Raft log lag using the consensus handle's shard state.
///
/// Computes lag as `last_log_index - commit_index`. A large gap indicates
/// entries have been appended but not yet committed, either due to a slow
/// leader or a partitioned follower.
pub(crate) fn check_raft_lag(handle: &ConsensusHandle, max_lag: u64) -> DependencyCheckResult {
    let state = handle.shard_state();
    let lag = state.last_log_index.saturating_sub(state.commit_index);
    let commit_index = state.commit_index;

    if lag <= max_lag {
        DependencyCheckResult {
            healthy: true,
            detail: format!("raft log lag: {lag} (max: {max_lag}, commit_index: {commit_index})"),
        }
    } else {
        DependencyCheckResult {
            healthy: false,
            detail: format!("raft log lag too high: {lag} > {max_lag}"),
        }
    }
}

/// Checks TCP reachability for cluster peers.
///
/// Resolves each voter's address from `peer_addresses` and attempts a TCP
/// connection within `timeout`. Reports healthy when a majority of peers
/// are reachable.
pub(crate) async fn check_peer_reachability_handle(
    handle: &ConsensusHandle,
    peer_addresses: &PeerAddressMap,
    timeout: Duration,
) -> DependencyCheckResult {
    let state = handle.shard_state();
    let my_id = handle.node_id();

    let voter_ids: Vec<u64> =
        state.voters.iter().filter(|&&id| id.0 != my_id).map(|id| id.0).collect();

    if voter_ids.is_empty() {
        return DependencyCheckResult {
            healthy: true,
            detail: "single-node cluster, no peers to check".to_string(),
        };
    }

    let mut reachable = 0u32;
    let mut unreachable = 0u32;

    for &peer_id in &voter_ids {
        let addr = match peer_addresses.get(peer_id) {
            Some(a) => a,
            None => {
                unreachable += 1;
                continue;
            },
        };

        match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&addr)).await {
            Ok(Ok(_)) => reachable += 1,
            _ => unreachable += 1,
        }
    }

    let other_voters = voter_ids.len() as u32;
    let total_voters = other_voters + 1; // include self
    let healthy = (reachable + 1) > total_voters / 2; // +1 for self

    DependencyCheckResult {
        healthy,
        detail: format!(
            "{reachable}/{other_voters} peers reachable ({unreachable} unreachable, self counts toward {total_voters}-node quorum)"
        ),
    }
}

/// Checks RMK provisioning for the node's required regions.
///
/// Reports how many versions are loaded per region. Healthy if all
/// required regions have at least one active version.
pub(crate) fn check_rmk_provisioning(
    key_manager: &dyn RegionKeyManager,
    node_region: Region,
    node_requires_residency: bool,
    known_regions: Vec<(Region, bool)>,
) -> DependencyCheckResult {
    let versions_map =
        rmk_versions_for_health(key_manager, node_region, node_requires_residency, known_regions);

    let mut region_summaries = Vec::new();
    let mut all_ok = true;

    for (region, versions) in &versions_map {
        let has_active =
            versions.iter().any(|v| v.status == inferadb_ledger_types::config::RmkStatus::Active);
        if !has_active {
            all_ok = false;
        }
        let version_nums: Vec<String> =
            versions.iter().map(|v| format!("v{}", v.version)).collect();
        region_summaries.push(format!("{region}: [{}]", version_nums.join(", ")));
    }

    region_summaries.sort();

    DependencyCheckResult {
        healthy: all_ok,
        detail: format!("rmk_versions: {{{}}}", region_summaries.join(", ")),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    // ─── DependencyCheckResult Tests ────────────────────────────

    // ─── Disk Check Tests ───────────────────────────────────────

    #[test]
    fn test_disk_check_writable_directory() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let result = check_disk(temp_dir.path());
        assert!(result.healthy);
        assert_eq!(result.detail, "disk is writable");
        // Probe file should be cleaned up
        assert!(!temp_dir.path().join(".health_probe").exists());
    }

    #[test]
    fn test_disk_check_nonexistent_directory() {
        let result = check_disk(std::path::Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(!result.healthy);
        assert!(result.detail.contains("disk write failed"));
    }

    #[test]
    fn test_disk_check_probe_cleanup_on_success() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let probe_path = temp_dir.path().join(".health_probe");

        // Run disk check twice
        let r1 = check_disk(temp_dir.path());
        assert!(r1.healthy);
        assert!(!probe_path.exists());

        let r2 = check_disk(temp_dir.path());
        assert!(r2.healthy);
        assert!(!probe_path.exists());
    }

    // ─── DependencyHealth Aggregation Tests ─────────────────────

    #[test]
    fn test_dependency_health_all_healthy() {
        let mut details = HashMap::new();
        details.insert(
            "disk".to_string(),
            DependencyCheckResult { healthy: true, detail: "ok".to_string() },
        );
        details.insert(
            "peer".to_string(),
            DependencyCheckResult { healthy: true, detail: "ok".to_string() },
        );
        let all_healthy = details.values().all(|r| r.healthy);
        let health = DependencyHealth { all_healthy, details };
        assert!(health.all_healthy);
    }

    #[test]
    fn test_dependency_health_one_unhealthy_fails() {
        let mut details = HashMap::new();
        details.insert(
            "disk".to_string(),
            DependencyCheckResult { healthy: true, detail: "ok".to_string() },
        );
        details.insert(
            "peer".to_string(),
            DependencyCheckResult { healthy: false, detail: "unreachable".to_string() },
        );
        let all_healthy = details.values().all(|r| r.healthy);
        let health = DependencyHealth { all_healthy, details };
        assert!(!health.all_healthy);
    }

    // ─── Config Validation Tests ────────────────────────────────

    #[test]
    fn test_health_check_config_defaults() {
        let config = HealthCheckConfig::default();
        assert_eq!(config.dependency_check_timeout_secs, 2);
        assert_eq!(config.health_cache_ttl_secs, 5);
        assert_eq!(config.max_raft_lag, 1000);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_health_check_config_zero_timeout_rejected() {
        let config =
            HealthCheckConfig { dependency_check_timeout_secs: 0, ..HealthCheckConfig::default() };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("dependency_check_timeout_secs"));
    }

    #[test]
    fn test_health_check_config_zero_ttl_rejected() {
        let config = HealthCheckConfig { health_cache_ttl_secs: 0, ..HealthCheckConfig::default() };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("health_cache_ttl_secs"));
    }

    #[test]
    fn test_health_check_config_zero_max_lag_rejected() {
        let config = HealthCheckConfig { max_raft_lag: 0, ..HealthCheckConfig::default() };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_raft_lag"));
    }

    #[test]
    fn test_health_check_config_valid_custom() {
        let config = HealthCheckConfig {
            dependency_check_timeout_secs: 5,
            health_cache_ttl_secs: 10,
            max_raft_lag: 500,
        };
        assert!(config.validate().is_ok());
    }

    // ─── Cache Logic Tests ──────────────────────────────────────

    #[test]
    fn test_cached_result_within_ttl() {
        let mut results = HashMap::new();
        results.insert(
            "disk_writable".to_string(),
            DependencyCheckResult { healthy: true, detail: "ok".to_string() },
        );
        let cached = CachedResult { results: results.clone(), timestamp: Instant::now() };

        // Within 5s TTL
        let ttl = Duration::from_secs(5);
        assert!(cached.timestamp.elapsed() < ttl);
    }

    #[test]
    fn test_cached_result_expired() {
        let mut results = HashMap::new();
        results.insert(
            "disk_writable".to_string(),
            DependencyCheckResult { healthy: true, detail: "ok".to_string() },
        );
        // Create a cache entry that was created 10s ago
        let cached = CachedResult {
            results: results.clone(),
            timestamp: Instant::now() - Duration::from_secs(10),
        };

        let ttl = Duration::from_secs(5);
        assert!(cached.timestamp.elapsed() >= ttl);
    }

    // ─── RMK Provisioning Check Tests ─────────────────────────

    #[test]
    fn test_rmk_check_all_regions_provisioned() {
        use inferadb_ledger_store::crypto::FileKeyManager;
        use inferadb_ledger_types::types::Region;

        let dir = tempfile::tempdir().expect("create temp dir");
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::GLOBAL).expect("rotate GLOBAL");
        manager.rotate_rmk(Region::US_EAST_VA).expect("rotate US_EAST_VA");
        manager.rotate_rmk(Region::US_WEST_OR).expect("rotate US_WEST_OR");

        let result = check_rmk_provisioning(
            &manager,
            Region::US_EAST_VA,
            false,
            vec![(Region::US_EAST_VA, false), (Region::US_WEST_OR, false)],
        );
        assert!(result.healthy);
        assert!(result.detail.contains("rmk_versions"));
    }

    #[test]
    fn test_rmk_check_missing_region_unhealthy() {
        use inferadb_ledger_store::crypto::FileKeyManager;
        use inferadb_ledger_types::types::Region;

        let dir = tempfile::tempdir().expect("create temp dir");
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        // Only provision GLOBAL
        manager.rotate_rmk(Region::GLOBAL).expect("rotate GLOBAL");

        let result = check_rmk_provisioning(
            &manager,
            Region::US_EAST_VA,
            false,
            vec![(Region::US_EAST_VA, false), (Region::US_WEST_OR, false)],
        );
        assert!(!result.healthy);
    }

    #[test]
    fn test_rmk_check_version_numbers_in_detail() {
        use inferadb_ledger_store::crypto::FileKeyManager;
        use inferadb_ledger_types::types::Region;

        let dir = tempfile::tempdir().expect("create temp dir");
        let manager = FileKeyManager::new(dir.path().to_path_buf());

        manager.rotate_rmk(Region::GLOBAL).expect("rotate GLOBAL v1");
        manager.rotate_rmk(Region::GLOBAL).expect("rotate GLOBAL v2");
        manager.rotate_rmk(Region::US_EAST_VA).expect("rotate US_EAST_VA");
        manager.rotate_rmk(Region::US_WEST_OR).expect("rotate US_WEST_OR");

        let result = check_rmk_provisioning(
            &manager,
            Region::US_EAST_VA,
            false,
            vec![(Region::US_EAST_VA, false), (Region::US_WEST_OR, false)],
        );
        assert!(result.healthy);
        assert!(result.detail.contains("v1"));
        assert!(result.detail.contains("v2"));
    }

    // ─── Disk Check Idempotency ──────────────────────────────────

    #[test]
    fn test_disk_check_idempotent() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        // Running multiple times should always succeed
        for _ in 0..5 {
            let result = check_disk(temp_dir.path());
            assert!(result.healthy);
        }
    }

    // ─── Config Boundary Tests ───────────────────────────────────

    #[test]
    fn test_health_check_config_boundary_values() {
        let config = HealthCheckConfig {
            dependency_check_timeout_secs: 1,
            health_cache_ttl_secs: 1,
            max_raft_lag: 1,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_health_check_config_large_values() {
        let config = HealthCheckConfig {
            dependency_check_timeout_secs: 60,
            health_cache_ttl_secs: 300,
            max_raft_lag: 100_000,
        };
        assert!(config.validate().is_ok());
    }

    // ─── Disk Check with Files Present ───────────────────────────

    #[test]
    fn test_disk_check_existing_probe_overwritten() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let probe_path = temp_dir.path().join(".health_probe");
        // Write something to the probe file location
        std::fs::write(&probe_path, b"old data").expect("write existing probe");

        let result = check_disk(temp_dir.path());
        assert!(result.healthy);
        // Probe file should be cleaned up
        assert!(!probe_path.exists());
    }
}
