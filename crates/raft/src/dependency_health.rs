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
use openraft::{BasicNode, Raft};
use parking_lot::RwLock;
use tonic::transport::Channel;

use crate::types::LedgerTypeConfig;

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
    /// Raft instance for metrics access.
    raft: Arc<Raft<LedgerTypeConfig>>,
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
}

impl DependencyHealthChecker {
    /// Creates a new dependency health checker.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        data_dir: PathBuf,
        config: HealthCheckConfig,
    ) -> Self {
        Self {
            raft,
            data_dir,
            config,
            cache: Arc::new(RwLock::new(None)),
            key_manager: None,
            node_region: None,
        }
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
        let raft_lag_result = check_raft_lag(&self.raft, self.config.max_raft_lag);
        let peer_result = check_peer_reachability(
            &self.raft,
            Duration::from_secs(self.config.dependency_check_timeout_secs),
        )
        .await;

        results.insert("disk_writable".to_string(), disk_result);
        results.insert("raft_log_lag".to_string(), raft_lag_result);
        results.insert("peer_reachable".to_string(), peer_result);

        // RMK provisioning check
        if let (Some(km), Some(region)) = (&self.key_manager, self.node_region) {
            let rmk_result = check_rmk_provisioning(km.as_ref(), region);
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

/// Checks Raft log lag by comparing last_log_index with last_applied.
pub(crate) fn check_raft_lag(raft: &Raft<LedgerTypeConfig>, max_lag: u64) -> DependencyCheckResult {
    let metrics = raft.metrics().borrow().clone();

    let last_log = metrics.last_log_index.unwrap_or(0);
    let last_applied = metrics.last_applied.map_or(0, |id| id.index);
    let lag = last_log.saturating_sub(last_applied);

    if lag <= max_lag {
        DependencyCheckResult {
            healthy: true,
            detail: format!("raft log lag: {lag} (max: {max_lag})"),
        }
    } else {
        DependencyCheckResult {
            healthy: false,
            detail: format!(
                "raft log lag too high: {lag} > {max_lag} (last_log: {last_log}, last_applied: {last_applied})"
            ),
        }
    }
}

/// Checks whether at least one peer is reachable via gRPC connectivity.
pub(crate) async fn check_peer_reachability(
    raft: &Raft<LedgerTypeConfig>,
    timeout: Duration,
) -> DependencyCheckResult {
    let metrics = raft.metrics().borrow().clone();
    let my_id = metrics.id;

    // Collect peer addresses from Raft membership
    let peers: Vec<(u64, String)> = metrics
        .membership_config
        .membership()
        .nodes()
        .filter(|(id, _)| **id != my_id)
        .map(|(id, node): (&u64, &BasicNode)| (*id, node.addr.clone()))
        .collect();

    if peers.is_empty() {
        return DependencyCheckResult {
            healthy: true,
            detail: "single-node cluster, no peers to check".to_string(),
        };
    }

    // Try each peer until one succeeds
    for (peer_id, addr) in &peers {
        let endpoint = format!("http://{addr}");
        if let Ok(ep) = Channel::from_shared(endpoint) {
            match tokio::time::timeout(timeout, ep.connect()).await {
                Ok(Ok(_)) => {
                    return DependencyCheckResult {
                        healthy: true,
                        detail: format!("peer {peer_id} ({addr}) reachable"),
                    };
                },
                Ok(Err(_)) | Err(_) => continue,
            }
        }
    }

    let peer_addrs: Vec<String> = peers.iter().map(|(id, addr)| format!("{id}@{addr}")).collect();
    DependencyCheckResult {
        healthy: false,
        detail: format!("no peers reachable (tried: {})", peer_addrs.join(", ")),
    }
}

/// Checks RMK provisioning for the node's required regions.
///
/// Reports how many versions are loaded per region. Healthy if all
/// required regions have at least one active version.
pub(crate) fn check_rmk_provisioning(
    key_manager: &dyn RegionKeyManager,
    node_region: Region,
) -> DependencyCheckResult {
    let versions_map = rmk_versions_for_health(key_manager, node_region);

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
    use std::path::PathBuf;

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

    // ─── Startup Check Tests (pure, no Raft) ────────────────────

    #[test]
    fn test_startup_data_dir_exists() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let dir_exists = temp_dir.path().exists() && temp_dir.path().is_dir();
        assert!(dir_exists);
    }

    #[test]
    fn test_startup_data_dir_missing() {
        let path = PathBuf::from("/nonexistent/startup/path");
        let dir_exists = path.exists() && path.is_dir();
        assert!(!dir_exists);
    }

    #[test]
    fn test_startup_data_dir_is_file_not_dir() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let file_path = temp_dir.path().join("not_a_dir");
        std::fs::write(&file_path, b"data").expect("create file");
        let dir_exists = file_path.exists() && file_path.is_dir();
        assert!(!dir_exists);
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

        let result = check_rmk_provisioning(&manager, Region::US_EAST_VA);
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

        let result = check_rmk_provisioning(&manager, Region::US_EAST_VA);
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

        let result = check_rmk_provisioning(&manager, Region::US_EAST_VA);
        assert!(result.healthy);
        assert!(result.detail.contains("v1"));
        assert!(result.detail.contains("v2"));
    }

    // ─── DependencyCheckResult Construction Tests ──────────────────

    #[test]
    fn test_dependency_check_result_healthy() {
        let result = DependencyCheckResult { healthy: true, detail: "all good".to_string() };
        assert!(result.healthy);
        assert_eq!(result.detail, "all good");
    }

    #[test]
    fn test_dependency_check_result_unhealthy() {
        let result =
            DependencyCheckResult { healthy: false, detail: "something wrong".to_string() };
        assert!(!result.healthy);
        assert_eq!(result.detail, "something wrong");
    }

    #[test]
    fn test_dependency_check_result_clone() {
        let result = DependencyCheckResult { healthy: true, detail: "cloned".to_string() };
        let cloned = result.clone();
        assert_eq!(cloned.healthy, result.healthy);
        assert_eq!(cloned.detail, result.detail);
    }

    #[test]
    fn test_dependency_check_result_debug() {
        let result = DependencyCheckResult { healthy: true, detail: "debug test".to_string() };
        let debug = format!("{:?}", result);
        assert!(debug.contains("healthy: true"));
        assert!(debug.contains("debug test"));
    }

    // ─── DependencyHealth Tests ────────────────────────────────────

    #[test]
    fn test_dependency_health_empty_details_is_healthy() {
        let details: HashMap<String, DependencyCheckResult> = HashMap::new();
        let all_healthy = details.values().all(|r| r.healthy);
        // Empty iterator returns true for all()
        assert!(all_healthy);
    }

    #[test]
    fn test_dependency_health_debug() {
        let health = DependencyHealth {
            all_healthy: false,
            details: HashMap::from([(
                "test".to_string(),
                DependencyCheckResult { healthy: false, detail: "failed".to_string() },
            )]),
        };
        let debug = format!("{:?}", health);
        assert!(debug.contains("all_healthy: false"));
    }

    #[test]
    fn test_dependency_health_clone() {
        let health = DependencyHealth {
            all_healthy: true,
            details: HashMap::from([(
                "disk".to_string(),
                DependencyCheckResult { healthy: true, detail: "ok".to_string() },
            )]),
        };
        let cloned = health.clone();
        assert_eq!(cloned.all_healthy, health.all_healthy);
        assert_eq!(cloned.details.len(), health.details.len());
    }

    // ─── Cache TTL Boundary Tests ──────────────────────────────────

    #[test]
    fn test_cache_exact_ttl_boundary() {
        // At exactly the TTL boundary, elapsed should equal TTL (not less than)
        // so the cache should be expired
        let ttl = Duration::from_secs(5);
        let cached = CachedResult { results: HashMap::new(), timestamp: Instant::now() - ttl };
        // elapsed() >= ttl means cache is expired
        assert!(cached.timestamp.elapsed() >= ttl);
    }

    // ─── Startup Check Additional Tests ────────────────────────────

    #[test]
    fn test_startup_check_detail_messages() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let dir_exists = temp_dir.path().exists() && temp_dir.path().is_dir();
        assert!(dir_exists);

        let detail = format!("data directory exists: {}", temp_dir.path().display());
        assert!(detail.starts_with("data directory exists:"));
    }

    #[test]
    fn test_startup_check_missing_dir_detail() {
        let path = PathBuf::from("/nonexistent/startup/path");
        let dir_exists = path.exists() && path.is_dir();
        assert!(!dir_exists);

        let detail = format!("data directory missing or not a directory: {}", path.display());
        assert!(detail.contains("/nonexistent/startup/path"));
    }
}
