//! Configuration types for InferaDB Ledger.
//!
//! Configuration is loaded from TOML files and environment variables.
//! All config structs validate their values at construction time via
//! fallible builders. Post-deserialization validation is available via
//! the [`validate`](StorageConfig::validate) method on each struct.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};
use snafu::Snafu;

/// Configuration validation error.
///
/// Returned when a configuration value is outside its valid range or
/// violates a cross-field constraint.
#[derive(Debug, Snafu)]
pub enum ConfigError {
    /// A configuration value is invalid.
    #[snafu(display("invalid config: {message}"))]
    Validation {
        /// Description of the validation failure.
        message: String,
    },
}

/// Minimum cache size: 1 MB.
const MIN_CACHE_SIZE_BYTES: usize = 1024 * 1024;

/// Maximum zstd compression level.
const MAX_COMPRESSION_LEVEL: i32 = 22;

/// Minimum zstd compression level.
const MIN_COMPRESSION_LEVEL: i32 = 1;

/// Main configuration for a ledger node.
#[derive(Debug, Clone, bon::Builder, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique identifier for this node.
    #[builder(into)]
    pub node_id: String,
    /// Address to listen for gRPC connections.
    pub listen_addr: SocketAddr,
    /// Directory for persistent data storage.
    #[builder(into)]
    pub data_dir: PathBuf,
    /// Peer nodes in the cluster.
    #[serde(default)]
    #[builder(default)]
    pub peers: Vec<PeerConfig>,
    /// Storage configuration.
    #[serde(default)]
    #[builder(default)]
    pub storage: StorageConfig,
    /// Raft consensus configuration.
    #[serde(default)]
    #[builder(default)]
    pub raft: RaftConfig,
    /// Batching configuration.
    #[serde(default)]
    #[builder(default)]
    pub batching: BatchConfig,
}

/// Configuration for a peer node.
#[derive(Debug, Clone, PartialEq, Eq, bon::Builder, Serialize, Deserialize)]
pub struct PeerConfig {
    /// Peer node identifier.
    #[builder(into)]
    pub node_id: String,
    /// Peer gRPC address.
    #[builder(into)]
    pub addr: String,
}

/// Storage layer configuration.
///
/// # Validation Rules
///
/// - `cache_size_bytes` must be >= 1 MB (1,048,576 bytes)
/// - `compression_level` must be 1-22 (zstd valid range)
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::StorageConfig;
/// let config = StorageConfig::builder()
///     .cache_size_bytes(128 * 1024 * 1024)
///     .compression_level(6)
///     .build()
///     .expect("valid storage config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Maximum size of the inferadb-ledger-store cache in bytes.
    ///
    /// Must be >= 1 MB (1,048,576 bytes) for reasonable operation.
    #[serde(default = "default_cache_size")]
    pub cache_size_bytes: usize,
    /// Number of snapshots to keep in hot cache.
    #[serde(default = "default_hot_cache_size")]
    pub hot_cache_snapshots: usize,
    /// Interval between automatic snapshots.
    #[serde(default = "default_snapshot_interval")]
    #[serde(with = "humantime_serde")]
    pub snapshot_interval: Duration,
    /// Zstd compression level for snapshots (1-22, 3 recommended).
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,
}

#[bon::bon]
impl StorageConfig {
    /// Creates a new storage configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `cache_size_bytes` < 1 MB
    /// - `compression_level` outside 1-22
    #[builder]
    pub fn new(
        #[builder(default = default_cache_size())] cache_size_bytes: usize,
        #[builder(default = default_hot_cache_size())] hot_cache_snapshots: usize,
        #[builder(default = default_snapshot_interval())] snapshot_interval: Duration,
        #[builder(default = default_compression_level())] compression_level: i32,
    ) -> Result<Self, ConfigError> {
        let config =
            Self { cache_size_bytes, hot_cache_snapshots, snapshot_interval, compression_level };
        config.validate()?;
        Ok(config)
    }
}

impl StorageConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.cache_size_bytes < MIN_CACHE_SIZE_BYTES {
            return Err(ConfigError::Validation {
                message: format!(
                    "cache_size_bytes must be >= {} (1 MB), got {}",
                    MIN_CACHE_SIZE_BYTES, self.cache_size_bytes
                ),
            });
        }
        if self.compression_level < MIN_COMPRESSION_LEVEL
            || self.compression_level > MAX_COMPRESSION_LEVEL
        {
            return Err(ConfigError::Validation {
                message: format!(
                    "compression_level must be {}-{}, got {}",
                    MIN_COMPRESSION_LEVEL, MAX_COMPRESSION_LEVEL, self.compression_level
                ),
            });
        }
        Ok(())
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            cache_size_bytes: default_cache_size(),
            hot_cache_snapshots: default_hot_cache_size(),
            snapshot_interval: default_snapshot_interval(),
            compression_level: default_compression_level(),
        }
    }
}

/// Raft consensus configuration.
///
/// # Validation Rules
///
/// - `election_timeout_min` must be < `election_timeout_max`
/// - `heartbeat_interval` must be < `election_timeout_min` / 2 (per Raft spec)
/// - `max_entries_per_rpc` must be > 0
/// - `snapshot_threshold` must be > 0
///
/// # Example
///
/// ```no_run
/// # use std::time::Duration;
/// # use inferadb_ledger_types::config::RaftConfig;
/// let config = RaftConfig::builder()
///     .heartbeat_interval(Duration::from_millis(100))
///     .election_timeout_min(Duration::from_millis(300))
///     .election_timeout_max(Duration::from_millis(500))
///     .build()
///     .expect("valid raft config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Heartbeat interval.
    ///
    /// Must be less than `election_timeout_min / 2` per Raft specification
    /// to prevent spurious leader elections.
    #[serde(default = "default_heartbeat_interval")]
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    /// Election timeout range (min).
    ///
    /// Must be less than `election_timeout_max`.
    #[serde(default = "default_election_timeout_min")]
    #[serde(with = "humantime_serde")]
    pub election_timeout_min: Duration,
    /// Election timeout range (max).
    ///
    /// Must be greater than `election_timeout_min`.
    #[serde(default = "default_election_timeout_max")]
    #[serde(with = "humantime_serde")]
    pub election_timeout_max: Duration,
    /// Maximum entries per append_entries RPC.
    ///
    /// Must be > 0.
    #[serde(default = "default_max_entries_per_rpc")]
    pub max_entries_per_rpc: u64,
    /// Snapshot threshold (entries since last snapshot).
    ///
    /// Must be > 0.
    #[serde(default = "default_snapshot_threshold")]
    pub snapshot_threshold: u64,
}

#[bon::bon]
impl RaftConfig {
    /// Creates a new Raft configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `election_timeout_min` >= `election_timeout_max`
    /// - `heartbeat_interval` >= `election_timeout_min / 2`
    /// - `max_entries_per_rpc` is 0
    /// - `snapshot_threshold` is 0
    #[builder]
    pub fn new(
        #[builder(default = default_heartbeat_interval())] heartbeat_interval: Duration,
        #[builder(default = default_election_timeout_min())] election_timeout_min: Duration,
        #[builder(default = default_election_timeout_max())] election_timeout_max: Duration,
        #[builder(default = default_max_entries_per_rpc())] max_entries_per_rpc: u64,
        #[builder(default = default_snapshot_threshold())] snapshot_threshold: u64,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            heartbeat_interval,
            election_timeout_min,
            election_timeout_max,
            max_entries_per_rpc,
            snapshot_threshold,
        };
        config.validate()?;
        Ok(config)
    }
}

impl RaftConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(ConfigError::Validation {
                message: format!(
                    "election_timeout_min ({:?}) must be less than election_timeout_max ({:?})",
                    self.election_timeout_min, self.election_timeout_max
                ),
            });
        }
        let half_election_min = self.election_timeout_min / 2;
        if self.heartbeat_interval >= half_election_min {
            return Err(ConfigError::Validation {
                message: format!(
                    "heartbeat_interval ({:?}) must be less than election_timeout_min / 2 ({:?})",
                    self.heartbeat_interval, half_election_min
                ),
            });
        }
        if self.max_entries_per_rpc == 0 {
            return Err(ConfigError::Validation {
                message: "max_entries_per_rpc must be > 0".to_string(),
            });
        }
        if self.snapshot_threshold == 0 {
            return Err(ConfigError::Validation {
                message: "snapshot_threshold must be > 0".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: default_heartbeat_interval(),
            election_timeout_min: default_election_timeout_min(),
            election_timeout_max: default_election_timeout_max(),
            max_entries_per_rpc: default_max_entries_per_rpc(),
            snapshot_threshold: default_snapshot_threshold(),
        }
    }
}

/// Transaction batching configuration.
///
/// # Validation Rules
///
/// - `max_batch_size` must be > 0
///
/// # Example
///
/// ```no_run
/// # use std::time::Duration;
/// # use inferadb_ledger_types::config::BatchConfig;
/// let config = BatchConfig::builder()
///     .max_batch_size(50)
///     .batch_timeout(Duration::from_millis(10))
///     .build()
///     .expect("valid batch config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum transactions per batch.
    ///
    /// Must be > 0.
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Maximum wait time before flushing a partial batch.
    #[serde(default = "default_batch_timeout")]
    #[serde(with = "humantime_serde")]
    pub batch_timeout: Duration,
    /// Enable batch coalescing for higher throughput.
    #[serde(default = "default_coalesce_enabled")]
    pub coalesce_enabled: bool,
}

#[bon::bon]
impl BatchConfig {
    /// Creates a new batch configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if `max_batch_size` is 0.
    #[builder]
    pub fn new(
        #[builder(default = default_max_batch_size())] max_batch_size: usize,
        #[builder(default = default_batch_timeout())] batch_timeout: Duration,
        #[builder(default = default_coalesce_enabled())] coalesce_enabled: bool,
    ) -> Result<Self, ConfigError> {
        let config = Self { max_batch_size, batch_timeout, coalesce_enabled };
        config.validate()?;
        Ok(config)
    }
}

impl BatchConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_batch_size == 0 {
            return Err(ConfigError::Validation {
                message: "max_batch_size must be > 0".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: default_max_batch_size(),
            batch_timeout: default_batch_timeout(),
            coalesce_enabled: default_coalesce_enabled(),
        }
    }
}

// Default value functions
fn default_cache_size() -> usize {
    256 * 1024 * 1024 // 256 MB
}

fn default_hot_cache_size() -> usize {
    3 // Last 3 snapshots
}

fn default_snapshot_interval() -> Duration {
    Duration::from_secs(300) // 5 minutes
}

fn default_compression_level() -> i32 {
    3 // Good balance of speed/ratio
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_millis(100)
}

fn default_election_timeout_min() -> Duration {
    Duration::from_millis(300)
}

fn default_election_timeout_max() -> Duration {
    Duration::from_millis(500)
}

fn default_max_entries_per_rpc() -> u64 {
    100
}

fn default_snapshot_threshold() -> u64 {
    10_000
}

fn default_max_batch_size() -> usize {
    100
}

fn default_batch_timeout() -> Duration {
    Duration::from_millis(5)
}

fn default_coalesce_enabled() -> bool {
    true
}

/// Duration serialization using humantime format.
mod humantime_serde {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&humantime::format_duration(*duration).to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        humantime::parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // =========================================================================
    // StorageConfig validation tests
    // =========================================================================

    #[test]
    fn test_storage_config_defaults_are_valid() {
        let config = StorageConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.cache_size_bytes, default_cache_size());
        assert_eq!(config.hot_cache_snapshots, default_hot_cache_size());
        assert_eq!(config.snapshot_interval, default_snapshot_interval());
        assert_eq!(config.compression_level, default_compression_level());
    }

    #[test]
    fn test_storage_config_builder_with_custom_values() {
        let config = StorageConfig::builder()
            .cache_size_bytes(2 * 1024 * 1024)
            .hot_cache_snapshots(5)
            .snapshot_interval(Duration::from_secs(600))
            .compression_level(10)
            .build()
            .expect("valid custom config");
        assert_eq!(config.cache_size_bytes, 2 * 1024 * 1024);
        assert_eq!(config.hot_cache_snapshots, 5);
        assert_eq!(config.snapshot_interval, Duration::from_secs(600));
        assert_eq!(config.compression_level, 10);
    }

    #[test]
    fn test_storage_config_cache_size_minimum() {
        // Exactly 1 MB is valid
        let result = StorageConfig::builder().cache_size_bytes(MIN_CACHE_SIZE_BYTES).build();
        assert!(result.is_ok());

        // Below 1 MB is invalid
        let result = StorageConfig::builder().cache_size_bytes(MIN_CACHE_SIZE_BYTES - 1).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("cache_size_bytes"));
        assert!(err.to_string().contains("1 MB"));
    }

    #[test]
    fn test_storage_config_cache_size_zero() {
        let result = StorageConfig::builder().cache_size_bytes(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_storage_config_compression_level_valid_range() {
        // Min boundary
        let result = StorageConfig::builder().compression_level(1).build();
        assert!(result.is_ok());

        // Max boundary
        let result = StorageConfig::builder().compression_level(22).build();
        assert!(result.is_ok());

        // Mid-range
        let result = StorageConfig::builder().compression_level(10).build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_storage_config_compression_level_too_low() {
        let result = StorageConfig::builder().compression_level(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("compression_level"));
        assert!(err.to_string().contains("1-22"));
    }

    #[test]
    fn test_storage_config_compression_level_too_high() {
        let result = StorageConfig::builder().compression_level(23).build();
        assert!(result.is_err());

        let result = StorageConfig::builder().compression_level(100).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_storage_config_compression_level_negative() {
        let result = StorageConfig::builder().compression_level(-1).build();
        assert!(result.is_err());
    }

    // =========================================================================
    // RaftConfig validation tests
    // =========================================================================

    #[test]
    fn test_raft_config_defaults_are_valid() {
        let config = RaftConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.heartbeat_interval, default_heartbeat_interval());
        assert_eq!(config.election_timeout_min, default_election_timeout_min());
        assert_eq!(config.election_timeout_max, default_election_timeout_max());
        assert_eq!(config.max_entries_per_rpc, default_max_entries_per_rpc());
        assert_eq!(config.snapshot_threshold, default_snapshot_threshold());
    }

    #[test]
    fn test_raft_config_builder_with_custom_values() {
        let config = RaftConfig::builder()
            .heartbeat_interval(Duration::from_millis(200))
            .election_timeout_min(Duration::from_millis(500))
            .election_timeout_max(Duration::from_millis(1000))
            .max_entries_per_rpc(50)
            .snapshot_threshold(5000)
            .build()
            .expect("valid custom config");
        assert_eq!(config.heartbeat_interval, Duration::from_millis(200));
        assert_eq!(config.election_timeout_min, Duration::from_millis(500));
        assert_eq!(config.election_timeout_max, Duration::from_millis(1000));
        assert_eq!(config.max_entries_per_rpc, 50);
        assert_eq!(config.snapshot_threshold, 5000);
    }

    #[test]
    fn test_raft_config_election_timeout_min_must_be_less_than_max() {
        // Equal is invalid
        let result = RaftConfig::builder()
            .election_timeout_min(Duration::from_millis(500))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("election_timeout_min"));
        assert!(err.to_string().contains("less than"));

        // Min > max is invalid
        let result = RaftConfig::builder()
            .election_timeout_min(Duration::from_millis(600))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_raft_config_heartbeat_must_be_less_than_half_election_min() {
        // heartbeat == election_min / 2 is invalid (must be strictly less)
        let result = RaftConfig::builder()
            .heartbeat_interval(Duration::from_millis(150))
            .election_timeout_min(Duration::from_millis(300))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("heartbeat_interval"));
        assert!(err.to_string().contains("election_timeout_min / 2"));

        // heartbeat > election_min / 2 is invalid
        let result = RaftConfig::builder()
            .heartbeat_interval(Duration::from_millis(200))
            .election_timeout_min(Duration::from_millis(300))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_raft_config_heartbeat_just_under_half_election_min() {
        // heartbeat < election_min / 2 is valid
        let result = RaftConfig::builder()
            .heartbeat_interval(Duration::from_millis(149))
            .election_timeout_min(Duration::from_millis(300))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_raft_config_max_entries_per_rpc_zero() {
        let result = RaftConfig::builder().max_entries_per_rpc(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("max_entries_per_rpc"));
    }

    #[test]
    fn test_raft_config_snapshot_threshold_zero() {
        let result = RaftConfig::builder().snapshot_threshold(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("snapshot_threshold"));
    }

    #[test]
    fn test_raft_config_max_entries_per_rpc_one() {
        let result = RaftConfig::builder().max_entries_per_rpc(1).build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_raft_config_snapshot_threshold_one() {
        let result = RaftConfig::builder().snapshot_threshold(1).build();
        assert!(result.is_ok());
    }

    // =========================================================================
    // BatchConfig validation tests
    // =========================================================================

    #[test]
    fn test_batch_config_defaults_are_valid() {
        let config = BatchConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.max_batch_size, default_max_batch_size());
        assert_eq!(config.batch_timeout, default_batch_timeout());
        assert_eq!(config.coalesce_enabled, default_coalesce_enabled());
    }

    #[test]
    fn test_batch_config_builder_with_custom_values() {
        let config = BatchConfig::builder()
            .max_batch_size(50)
            .batch_timeout(Duration::from_millis(10))
            .coalesce_enabled(false)
            .build()
            .expect("valid custom config");
        assert_eq!(config.max_batch_size, 50);
        assert_eq!(config.batch_timeout, Duration::from_millis(10));
        assert!(!config.coalesce_enabled);
    }

    #[test]
    fn test_batch_config_max_batch_size_zero() {
        let result = BatchConfig::builder().max_batch_size(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("max_batch_size"));
    }

    #[test]
    fn test_batch_config_max_batch_size_one() {
        let result = BatchConfig::builder().max_batch_size(1).build();
        assert!(result.is_ok());
    }

    // =========================================================================
    // Default impl tests
    // =========================================================================

    #[test]
    fn test_default_configs() {
        let storage = StorageConfig::default();
        assert_eq!(storage.hot_cache_snapshots, 3);
        assert_eq!(storage.compression_level, 3);

        let raft = RaftConfig::default();
        assert_eq!(raft.heartbeat_interval, Duration::from_millis(100));

        let batch = BatchConfig::default();
        assert_eq!(batch.max_batch_size, 100);
        assert!(batch.coalesce_enabled);
    }

    #[test]
    fn test_defaults_pass_validation() {
        assert!(StorageConfig::default().validate().is_ok());
        assert!(RaftConfig::default().validate().is_ok());
        assert!(BatchConfig::default().validate().is_ok());
    }

    #[test]
    fn test_builder_matches_default() {
        assert_eq!(StorageConfig::builder().build().expect("valid"), StorageConfig::default());
        assert_eq!(RaftConfig::builder().build().expect("valid"), RaftConfig::default());
        assert_eq!(BatchConfig::builder().build().expect("valid"), BatchConfig::default());
    }

    // =========================================================================
    // Validate method tests (for post-deserialization)
    // =========================================================================

    #[test]
    fn test_storage_config_validate_method() {
        let mut config = StorageConfig::default();
        assert!(config.validate().is_ok());

        config.compression_level = 100;
        assert!(config.validate().is_err());

        config.compression_level = 3;
        config.cache_size_bytes = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_raft_config_validate_method() {
        let mut config = RaftConfig::default();
        assert!(config.validate().is_ok());

        config.election_timeout_min = Duration::from_millis(600);
        config.election_timeout_max = Duration::from_millis(500);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_batch_config_validate_method() {
        let mut config = BatchConfig::default();
        assert!(config.validate().is_ok());

        config.max_batch_size = 0;
        assert!(config.validate().is_err());
    }

    // =========================================================================
    // Node config and integration tests
    // =========================================================================

    #[test]
    fn test_peer_config_builder() {
        let peer = PeerConfig::builder().node_id("node-2").addr("127.0.0.1:50052").build();
        assert_eq!(peer.node_id, "node-2");
        assert_eq!(peer.addr, "127.0.0.1:50052");
    }

    #[test]
    fn test_node_config_builder_nested() {
        let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let config = NodeConfig::builder()
            .node_id("node-1")
            .listen_addr(addr)
            .data_dir("/tmp/ledger")
            .peers(vec![PeerConfig::builder().node_id("node-2").addr("127.0.0.1:50052").build()])
            .storage(StorageConfig::builder().cache_size_bytes(1024 * 1024).build().expect("valid"))
            .raft(
                RaftConfig::builder()
                    .heartbeat_interval(Duration::from_millis(200))
                    .election_timeout_min(Duration::from_millis(500))
                    .election_timeout_max(Duration::from_millis(1000))
                    .build()
                    .expect("valid"),
            )
            .batching(BatchConfig::builder().max_batch_size(50).build().expect("valid"))
            .build();

        assert_eq!(config.node_id, "node-1");
        assert_eq!(config.listen_addr, addr);
        assert_eq!(config.data_dir, PathBuf::from("/tmp/ledger"));
        assert_eq!(config.peers.len(), 1);
        assert_eq!(config.peers[0].node_id, "node-2");
        assert_eq!(config.storage.cache_size_bytes, 1024 * 1024);
        assert_eq!(config.raft.heartbeat_interval, Duration::from_millis(200));
        assert_eq!(config.batching.max_batch_size, 50);
    }

    #[test]
    fn test_node_config_builder_with_defaults() {
        let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let config = NodeConfig::builder()
            .node_id("node-1")
            .listen_addr(addr)
            .data_dir("/tmp/ledger")
            .build();

        assert_eq!(config.node_id, "node-1");
        assert!(config.peers.is_empty());
        assert_eq!(config.storage, StorageConfig::default());
        assert_eq!(config.raft, RaftConfig::default());
        assert_eq!(config.batching, BatchConfig::default());
    }

    #[test]
    fn test_node_config_serde_roundtrip() {
        let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let config = NodeConfig::builder()
            .node_id("node-1")
            .listen_addr(addr)
            .data_dir("/tmp/ledger")
            .peers(vec![PeerConfig::builder().node_id("node-2").addr("127.0.0.1:50052").build()])
            .storage(
                StorageConfig::builder()
                    .cache_size_bytes(512 * 1024 * 1024)
                    .hot_cache_snapshots(5)
                    .build()
                    .expect("valid"),
            )
            .raft(
                RaftConfig::builder()
                    .heartbeat_interval(Duration::from_millis(150))
                    .election_timeout_min(Duration::from_millis(400))
                    .election_timeout_max(Duration::from_millis(600))
                    .max_entries_per_rpc(200)
                    .build()
                    .expect("valid"),
            )
            .batching(
                BatchConfig::builder()
                    .max_batch_size(200)
                    .coalesce_enabled(false)
                    .build()
                    .expect("valid"),
            )
            .build();

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: NodeConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.node_id, deserialized.node_id);
        assert_eq!(config.listen_addr, deserialized.listen_addr);
        assert_eq!(config.data_dir, deserialized.data_dir);
        assert_eq!(config.peers.len(), deserialized.peers.len());
        assert_eq!(config.peers[0].node_id, deserialized.peers[0].node_id);
        assert_eq!(config.storage, deserialized.storage);
        assert_eq!(config.raft, deserialized.raft);
        assert_eq!(config.batching, deserialized.batching);
    }

    // =========================================================================
    // ConfigError display tests
    // =========================================================================

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::Validation { message: "test error".to_string() };
        assert_eq!(err.to_string(), "invalid config: test error");
    }
}
