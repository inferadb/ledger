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
/// - `proposal_timeout` must be >= 1s
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
    /// Maximum time to wait for a Raft proposal to commit.
    ///
    /// If a gRPC deadline is present and shorter, the deadline takes precedence.
    /// Must be >= 1s.
    #[serde(default = "default_proposal_timeout")]
    #[serde(with = "humantime_serde")]
    pub proposal_timeout: Duration,
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
    /// - `proposal_timeout` < 1s
    #[builder]
    pub fn new(
        #[builder(default = default_heartbeat_interval())] heartbeat_interval: Duration,
        #[builder(default = default_election_timeout_min())] election_timeout_min: Duration,
        #[builder(default = default_election_timeout_max())] election_timeout_max: Duration,
        #[builder(default = default_max_entries_per_rpc())] max_entries_per_rpc: u64,
        #[builder(default = default_snapshot_threshold())] snapshot_threshold: u64,
        #[builder(default = default_proposal_timeout())] proposal_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            heartbeat_interval,
            election_timeout_min,
            election_timeout_max,
            max_entries_per_rpc,
            snapshot_threshold,
            proposal_timeout,
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
        if self.proposal_timeout < Duration::from_secs(1) {
            return Err(ConfigError::Validation {
                message: format!("proposal_timeout must be >= 1s, got {:?}", self.proposal_timeout),
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
            proposal_timeout: default_proposal_timeout(),
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

fn default_proposal_timeout() -> Duration {
    Duration::from_secs(30)
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

// =============================================================================
// Rate Limiting Configuration
// =============================================================================

/// Default per-client token bucket capacity (max burst).
fn default_client_burst() -> u64 {
    100
}

/// Default per-client sustained rate (requests/sec).
fn default_client_rate() -> f64 {
    50.0
}

/// Default per-namespace token bucket capacity (max burst).
fn default_namespace_burst() -> u64 {
    1000
}

/// Default per-namespace sustained rate (requests/sec).
fn default_namespace_rate() -> f64 {
    500.0
}

/// Default backpressure threshold (pending Raft proposals).
fn default_backpressure_threshold() -> u64 {
    100
}

/// Configuration for multi-level token bucket rate limiting.
///
/// Controls three tiers of admission control:
///
/// 1. **Per-client** — prevents one bad actor from monopolizing the system. `client_burst` sets the
///    max burst, `client_rate` sets sustained requests/sec.
///
/// 2. **Per-namespace** — ensures fair sharing across tenants in a multi-tenant shard.
///    `namespace_burst` sets the max burst, `namespace_rate` sets sustained requests/sec.
///
/// 3. **Global backpressure** — throttles all requests when Raft consensus is saturated.
///    `backpressure_threshold` is the pending proposal count above which requests are rejected.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::RateLimitConfig;
/// let config = RateLimitConfig::builder()
///     .client_burst(200)
///     .client_rate(100.0)
///     .namespace_burst(2000)
///     .namespace_rate(1000.0)
///     .backpressure_threshold(200)
///     .build()
///     .expect("valid rate limit config");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum burst size per client (token bucket capacity).
    ///
    /// Must be > 0.
    #[serde(default = "default_client_burst")]
    pub client_burst: u64,
    /// Sustained requests per second per client (token refill rate).
    ///
    /// Must be > 0.
    #[serde(default = "default_client_rate")]
    pub client_rate: f64,
    /// Maximum burst size per namespace (token bucket capacity).
    ///
    /// Must be > 0.
    #[serde(default = "default_namespace_burst")]
    pub namespace_burst: u64,
    /// Sustained requests per second per namespace (token refill rate).
    ///
    /// Must be > 0.
    #[serde(default = "default_namespace_rate")]
    pub namespace_rate: f64,
    /// Pending Raft proposal count above which global backpressure activates.
    ///
    /// Must be > 0.
    #[serde(default = "default_backpressure_threshold")]
    pub backpressure_threshold: u64,
}

#[bon::bon]
impl RateLimitConfig {
    /// Creates a new rate limit configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is zero or negative.
    #[builder]
    pub fn new(
        #[builder(default = default_client_burst())] client_burst: u64,
        #[builder(default = default_client_rate())] client_rate: f64,
        #[builder(default = default_namespace_burst())] namespace_burst: u64,
        #[builder(default = default_namespace_rate())] namespace_rate: f64,
        #[builder(default = default_backpressure_threshold())] backpressure_threshold: u64,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            client_burst,
            client_rate,
            namespace_burst,
            namespace_rate,
            backpressure_threshold,
        };
        config.validate()?;
        Ok(config)
    }
}

impl RateLimitConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.client_burst == 0 {
            return Err(ConfigError::Validation {
                message: "client_burst must be > 0".to_string(),
            });
        }
        if self.client_rate <= 0.0 {
            return Err(ConfigError::Validation { message: "client_rate must be > 0".to_string() });
        }
        if self.namespace_burst == 0 {
            return Err(ConfigError::Validation {
                message: "namespace_burst must be > 0".to_string(),
            });
        }
        if self.namespace_rate <= 0.0 {
            return Err(ConfigError::Validation {
                message: "namespace_rate must be > 0".to_string(),
            });
        }
        if self.backpressure_threshold == 0 {
            return Err(ConfigError::Validation {
                message: "backpressure_threshold must be > 0".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            client_burst: default_client_burst(),
            client_rate: default_client_rate(),
            namespace_burst: default_namespace_burst(),
            namespace_rate: default_namespace_rate(),
            backpressure_threshold: default_backpressure_threshold(),
        }
    }
}

// =========================================================================
// AuditConfig defaults
// =========================================================================

/// Default max audit log file size: 100 MB.
fn default_max_file_size_bytes() -> u64 {
    100 * 1024 * 1024
}

/// Default max rotated audit log files to retain.
fn default_max_rotated_files() -> u32 {
    10
}

/// Audit logging configuration.
///
/// Controls the file-based audit logger for compliance (SOC2, HIPAA).
/// Audit logs capture security-sensitive operations with durable writes.
///
/// # Log Rotation
///
/// When the active log file exceeds `max_file_size_bytes`, it is rotated
/// to `{path}.1`, `{path}.2`, etc. Files beyond `max_rotated_files` are
/// deleted. This prevents unbounded disk usage.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::AuditConfig;
/// let config = AuditConfig::builder()
///     .path("/var/log/inferadb/audit.jsonl")
///     .max_file_size_bytes(50 * 1024 * 1024)
///     .max_rotated_files(20)
///     .build()
///     .expect("valid audit config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Path to the audit log file (JSON Lines format).
    ///
    /// Must be a non-empty string. Parent directory must exist at runtime.
    pub path: String,
    /// Maximum audit log file size in bytes before rotation.
    ///
    /// Must be >= 1 MB (1_048_576 bytes). Default: 100 MB.
    #[serde(default = "default_max_file_size_bytes")]
    pub max_file_size_bytes: u64,
    /// Maximum number of rotated log files to retain.
    ///
    /// Must be >= 1. Default: 10. Oldest files are deleted when exceeded.
    #[serde(default = "default_max_rotated_files")]
    pub max_rotated_files: u32,
}

/// Minimum audit log file size: 1 MB.
pub const MIN_AUDIT_FILE_SIZE: u64 = 1024 * 1024;

#[bon::bon]
impl AuditConfig {
    /// Creates a new audit configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `path` is empty
    /// - `max_file_size_bytes` < 1 MB
    /// - `max_rotated_files` == 0
    #[builder]
    pub fn new(
        #[builder(into)] path: String,
        #[builder(default = default_max_file_size_bytes())] max_file_size_bytes: u64,
        #[builder(default = default_max_rotated_files())] max_rotated_files: u32,
    ) -> Result<Self, ConfigError> {
        let config = Self { path, max_file_size_bytes, max_rotated_files };
        config.validate()?;
        Ok(config)
    }
}

impl AuditConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.path.is_empty() {
            return Err(ConfigError::Validation {
                message: "audit path must not be empty".to_string(),
            });
        }
        if self.max_file_size_bytes < MIN_AUDIT_FILE_SIZE {
            return Err(ConfigError::Validation {
                message: format!(
                    "max_file_size_bytes must be >= {} (1 MB), got {}",
                    MIN_AUDIT_FILE_SIZE, self.max_file_size_bytes
                ),
            });
        }
        if self.max_rotated_files == 0 {
            return Err(ConfigError::Validation {
                message: "max_rotated_files must be >= 1".to_string(),
            });
        }
        Ok(())
    }
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

// =========================================================================
// BTreeCompactionConfig
// =========================================================================

fn default_min_fill_factor() -> f64 {
    0.4
}

fn default_compaction_interval_secs() -> u64 {
    3600
}

/// B+ tree compaction configuration.
///
/// Controls the background compaction job that merges underfull leaf nodes
/// after deletions. Without compaction, deleted entries leave sparse leaf
/// pages that waste disk space and cache memory.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::BTreeCompactionConfig;
/// let config = BTreeCompactionConfig::builder()
///     .min_fill_factor(0.5)
///     .interval_secs(1800)
///     .build()
///     .expect("valid compaction config");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BTreeCompactionConfig {
    /// Minimum fill factor threshold (0.0 to 1.0).
    ///
    /// Leaf nodes with a fill factor below this value are candidates for
    /// merging with a sibling. Must be in range (0.0, 1.0).
    /// Default: 0.4 (40%).
    #[serde(default = "default_min_fill_factor")]
    pub min_fill_factor: f64,
    /// Interval in seconds between compaction cycles.
    ///
    /// Must be >= 60 seconds. Default: 3600 (1 hour).
    #[serde(default = "default_compaction_interval_secs")]
    pub interval_secs: u64,
}

impl Default for BTreeCompactionConfig {
    fn default() -> Self {
        Self {
            min_fill_factor: default_min_fill_factor(),
            interval_secs: default_compaction_interval_secs(),
        }
    }
}

#[bon::bon]
impl BTreeCompactionConfig {
    /// Creates a new B+ tree compaction configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `min_fill_factor` is not in (0.0, 1.0)
    /// - `interval_secs` < 60
    #[builder]
    pub fn new(
        #[builder(default = default_min_fill_factor())] min_fill_factor: f64,
        #[builder(default = default_compaction_interval_secs())] interval_secs: u64,
    ) -> Result<Self, ConfigError> {
        let config = Self { min_fill_factor, interval_secs };
        config.validate()?;
        Ok(config)
    }
}

impl BTreeCompactionConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.min_fill_factor <= 0.0 || self.min_fill_factor >= 1.0 {
            return Err(ConfigError::Validation {
                message: format!(
                    "min_fill_factor must be in (0.0, 1.0), got {}",
                    self.min_fill_factor
                ),
            });
        }
        if self.interval_secs < 60 {
            return Err(ConfigError::Validation {
                message: format!("interval_secs must be >= 60, got {}", self.interval_secs),
            });
        }
        Ok(())
    }
}

/// Default grace period in seconds (15s).
const fn default_grace_period_secs() -> u64 {
    15
}

/// Default drain timeout in seconds (30s).
const fn default_drain_timeout_secs() -> u64 {
    30
}

/// Default pre-stop delay in seconds (5s).
const fn default_pre_stop_delay_secs() -> u64 {
    5
}

/// Default pre-shutdown timeout in seconds (60s).
const fn default_pre_shutdown_timeout_secs() -> u64 {
    60
}

/// Default watchdog interval multiplier (2x expected job interval).
const fn default_watchdog_multiplier() -> u64 {
    2
}

/// Minimum grace period in seconds.
const MIN_GRACE_PERIOD_SECS: u64 = 1;

/// Minimum drain timeout in seconds.
const MIN_DRAIN_TIMEOUT_SECS: u64 = 5;

/// Minimum pre-shutdown timeout in seconds.
const MIN_PRE_SHUTDOWN_TIMEOUT_SECS: u64 = 5;

/// Graceful shutdown configuration.
///
/// Controls how the server shuts down when receiving a termination signal.
/// The shutdown sequence is:
///
/// 1. Mark readiness probe as failing (stops new traffic from load balancer)
/// 2. Wait `pre_stop_delay_secs` for K8s to remove pod from endpoints
/// 3. Wait `grace_period_secs` for load balancer to drain existing connections
/// 4. Wait up to `drain_timeout_secs` for in-flight requests to complete
/// 5. Run pre-shutdown tasks (snapshots, Raft shutdown) with `pre_shutdown_timeout_secs` limit
/// 6. Signal the gRPC server to stop
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::ShutdownConfig;
/// let config = ShutdownConfig::builder()
///     .grace_period_secs(10)
///     .drain_timeout_secs(60)
///     .build()
///     .expect("valid shutdown config");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShutdownConfig {
    /// Seconds to wait after marking readiness as failing before draining.
    ///
    /// This gives load balancers time to stop sending traffic. Must be >= 1.
    /// Default: 15 seconds.
    #[serde(default = "default_grace_period_secs")]
    pub grace_period_secs: u64,
    /// Maximum seconds to wait for in-flight requests to complete.
    ///
    /// After the grace period, the server waits up to this duration for
    /// active requests to finish. Must be >= 5. Default: 30 seconds.
    #[serde(default = "default_drain_timeout_secs")]
    pub drain_timeout_secs: u64,
    /// Seconds to delay between readiness failure and connection drain.
    ///
    /// Kubernetes `preStop` hook equivalent. Allows the Kubernetes endpoints
    /// controller to remove this pod from Service endpoints before we start
    /// refusing connections. Default: 5 seconds.
    #[serde(default = "default_pre_stop_delay_secs")]
    pub pre_stop_delay_secs: u64,
    /// Maximum seconds for pre-shutdown tasks (snapshots, Raft shutdown).
    ///
    /// If Raft shutdown or snapshot creation hangs, this timeout ensures
    /// the process eventually exits. Must be >= 5. Default: 60 seconds.
    #[serde(default = "default_pre_shutdown_timeout_secs")]
    pub pre_shutdown_timeout_secs: u64,
    /// Multiplier for background job watchdog interval.
    ///
    /// Liveness probe fails if any background job has not heartbeated
    /// within `watchdog_multiplier` times its expected interval.
    /// Default: 2 (2x the job's expected cycle time).
    #[serde(default = "default_watchdog_multiplier")]
    pub watchdog_multiplier: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            grace_period_secs: default_grace_period_secs(),
            drain_timeout_secs: default_drain_timeout_secs(),
            pre_stop_delay_secs: default_pre_stop_delay_secs(),
            pre_shutdown_timeout_secs: default_pre_shutdown_timeout_secs(),
            watchdog_multiplier: default_watchdog_multiplier(),
        }
    }
}

#[bon::bon]
impl ShutdownConfig {
    /// Creates a new shutdown configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `grace_period_secs` < 1
    /// - `drain_timeout_secs` < 5
    /// - `pre_shutdown_timeout_secs` < 5
    #[builder]
    pub fn new(
        #[builder(default = default_grace_period_secs())] grace_period_secs: u64,
        #[builder(default = default_drain_timeout_secs())] drain_timeout_secs: u64,
        #[builder(default = default_pre_stop_delay_secs())] pre_stop_delay_secs: u64,
        #[builder(default = default_pre_shutdown_timeout_secs())] pre_shutdown_timeout_secs: u64,
        #[builder(default = default_watchdog_multiplier())] watchdog_multiplier: u64,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            grace_period_secs,
            drain_timeout_secs,
            pre_stop_delay_secs,
            pre_shutdown_timeout_secs,
            watchdog_multiplier,
        };
        config.validate()?;
        Ok(config)
    }
}

impl ShutdownConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.grace_period_secs < MIN_GRACE_PERIOD_SECS {
            return Err(ConfigError::Validation {
                message: format!(
                    "grace_period_secs must be >= {}, got {}",
                    MIN_GRACE_PERIOD_SECS, self.grace_period_secs
                ),
            });
        }
        if self.drain_timeout_secs < MIN_DRAIN_TIMEOUT_SECS {
            return Err(ConfigError::Validation {
                message: format!(
                    "drain_timeout_secs must be >= {}, got {}",
                    MIN_DRAIN_TIMEOUT_SECS, self.drain_timeout_secs
                ),
            });
        }
        if self.pre_shutdown_timeout_secs < MIN_PRE_SHUTDOWN_TIMEOUT_SECS {
            return Err(ConfigError::Validation {
                message: format!(
                    "pre_shutdown_timeout_secs must be >= {}, got {}",
                    MIN_PRE_SHUTDOWN_TIMEOUT_SECS, self.pre_shutdown_timeout_secs
                ),
            });
        }
        if self.watchdog_multiplier == 0 {
            return Err(ConfigError::Validation {
                message: "watchdog_multiplier must be >= 1".to_string(),
            });
        }
        Ok(())
    }
}

/// Minimum detection window in seconds.
const MIN_WINDOW_SECS: u64 = 1;

/// Minimum hot key threshold (operations per second).
const MIN_HOT_KEY_THRESHOLD: u64 = 1;

/// Default detection window in seconds (60s).
const fn default_window_secs() -> u64 {
    60
}

/// Default hot key threshold (100 ops/sec).
const fn default_hot_key_threshold() -> u64 {
    100
}

/// Default Count-Min Sketch width (number of counters per row).
const fn default_cms_width() -> usize {
    1024
}

/// Default Count-Min Sketch depth (number of hash functions).
const fn default_cms_depth() -> usize {
    4
}

/// Default top-k keys to track for operational visibility.
const fn default_top_k() -> usize {
    10
}

/// Hot key detection configuration.
///
/// Controls the background hot key detector that identifies frequently
/// accessed keys using a Count-Min Sketch for space-efficient frequency
/// estimation. Hot key warnings help operators identify contention points
/// before they cause performance issues.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::HotKeyConfig;
/// let config = HotKeyConfig::builder()
///     .window_secs(30)
///     .threshold(200)
///     .build()
///     .expect("valid hot key config");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HotKeyConfig {
    /// Detection window in seconds.
    ///
    /// Access counts are accumulated over this sliding window. Must be >= 1.
    /// Default: 60 seconds.
    #[serde(default = "default_window_secs")]
    pub window_secs: u64,
    /// Threshold for "hot" classification (operations per second).
    ///
    /// Keys exceeding this rate within the window are reported as hot.
    /// Must be >= 1. Default: 100 ops/sec.
    #[serde(default = "default_hot_key_threshold")]
    pub threshold: u64,
    /// Count-Min Sketch width (number of counters per row).
    ///
    /// Larger values reduce over-estimation error at the cost of memory.
    /// Must be >= 64. Default: 1024.
    #[serde(default = "default_cms_width")]
    pub cms_width: usize,
    /// Count-Min Sketch depth (number of hash functions).
    ///
    /// More hash functions reduce error probability at the cost of
    /// per-increment CPU. Must be >= 2. Default: 4.
    #[serde(default = "default_cms_depth")]
    pub cms_depth: usize,
    /// Maximum number of hot keys to track for `get_top_hot_keys()`.
    ///
    /// Only the top-k hottest keys are retained for operational visibility.
    /// Must be >= 1. Default: 10.
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

impl Default for HotKeyConfig {
    fn default() -> Self {
        Self {
            window_secs: default_window_secs(),
            threshold: default_hot_key_threshold(),
            cms_width: default_cms_width(),
            cms_depth: default_cms_depth(),
            top_k: default_top_k(),
        }
    }
}

#[bon::bon]
impl HotKeyConfig {
    /// Creates a new hot key detection configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `window_secs` < 1
    /// - `threshold` < 1
    /// - `cms_width` < 64
    /// - `cms_depth` < 2
    /// - `top_k` < 1
    #[builder]
    pub fn new(
        #[builder(default = default_window_secs())] window_secs: u64,
        #[builder(default = default_hot_key_threshold())] threshold: u64,
        #[builder(default = default_cms_width())] cms_width: usize,
        #[builder(default = default_cms_depth())] cms_depth: usize,
        #[builder(default = default_top_k())] top_k: usize,
    ) -> Result<Self, ConfigError> {
        let config = Self { window_secs, threshold, cms_width, cms_depth, top_k };
        config.validate()?;
        Ok(config)
    }
}

impl HotKeyConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.window_secs < MIN_WINDOW_SECS {
            return Err(ConfigError::Validation {
                message: format!(
                    "window_secs must be >= {}, got {}",
                    MIN_WINDOW_SECS, self.window_secs
                ),
            });
        }
        if self.threshold < MIN_HOT_KEY_THRESHOLD {
            return Err(ConfigError::Validation {
                message: format!(
                    "threshold must be >= {}, got {}",
                    MIN_HOT_KEY_THRESHOLD, self.threshold
                ),
            });
        }
        if self.cms_width < 64 {
            return Err(ConfigError::Validation {
                message: format!("cms_width must be >= 64, got {}", self.cms_width),
            });
        }
        if self.cms_depth < 2 {
            return Err(ConfigError::Validation {
                message: format!("cms_depth must be >= 2, got {}", self.cms_depth),
            });
        }
        if self.top_k < 1 {
            return Err(ConfigError::Validation {
                message: format!("top_k must be >= 1, got {}", self.top_k),
            });
        }
        Ok(())
    }
}

// ============================================================================
// Validation Configuration
// ============================================================================

/// Default maximum entity key size in bytes.
const fn default_max_key_bytes() -> usize {
    1024 // 1 KB
}

/// Default maximum entity value size in bytes.
const fn default_max_value_bytes() -> usize {
    10 * 1024 * 1024 // 10 MB
}

/// Default maximum operations per write request.
const fn default_max_operations_per_write() -> usize {
    1000
}

/// Default maximum aggregate payload size for a batch of operations.
const fn default_max_batch_payload_bytes() -> usize {
    100 * 1024 * 1024 // 100 MB
}

/// Default maximum namespace name length in bytes.
const fn default_max_namespace_name_bytes() -> usize {
    256
}

/// Default maximum relationship string length in bytes.
const fn default_max_relationship_string_bytes() -> usize {
    1024 // 1 KB
}

/// Input validation configuration for gRPC request fields.
///
/// Controls maximum sizes for entity keys, values, namespace names,
/// relationship strings, and batch limits. Applied at both the gRPC
/// service boundary and in SDK operation constructors.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::ValidationConfig;
/// let config = ValidationConfig::builder()
///     .max_key_bytes(2048)
///     .max_operations_per_write(500)
///     .build()
///     .expect("valid validation config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Maximum entity key size in bytes.
    ///
    /// Keys exceeding this limit are rejected with `INVALID_ARGUMENT`.
    /// Must be >= 1. Default: 1024 (1 KB).
    #[serde(default = "default_max_key_bytes")]
    pub max_key_bytes: usize,
    /// Maximum entity value size in bytes.
    ///
    /// Values exceeding this limit are rejected with `INVALID_ARGUMENT`.
    /// Must be >= 1. Default: 10 MB.
    #[serde(default = "default_max_value_bytes")]
    pub max_value_bytes: usize,
    /// Maximum number of operations per write request.
    ///
    /// Applies to both `Write` and `BatchWrite` RPCs.
    /// Must be >= 1. Default: 1000.
    #[serde(default = "default_max_operations_per_write")]
    pub max_operations_per_write: usize,
    /// Maximum aggregate payload size for a batch of operations in bytes.
    ///
    /// Prevents memory exhaustion from large batches.
    /// Must be >= 1. Default: 100 MB.
    #[serde(default = "default_max_batch_payload_bytes")]
    pub max_batch_payload_bytes: usize,
    /// Maximum namespace name length in bytes.
    ///
    /// Namespace names must also match `[a-z0-9-]{1,N}`.
    /// Must be >= 1. Default: 256.
    #[serde(default = "default_max_namespace_name_bytes")]
    pub max_namespace_name_bytes: usize,
    /// Maximum relationship string length in bytes.
    ///
    /// Applies to resource, relation, and subject fields.
    /// Must be >= 1. Default: 1024 (1 KB).
    #[serde(default = "default_max_relationship_string_bytes")]
    pub max_relationship_string_bytes: usize,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            max_key_bytes: default_max_key_bytes(),
            max_value_bytes: default_max_value_bytes(),
            max_operations_per_write: default_max_operations_per_write(),
            max_batch_payload_bytes: default_max_batch_payload_bytes(),
            max_namespace_name_bytes: default_max_namespace_name_bytes(),
            max_relationship_string_bytes: default_max_relationship_string_bytes(),
        }
    }
}

#[bon::bon]
impl ValidationConfig {
    /// Creates a new validation configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any limit is zero.
    #[builder]
    pub fn new(
        #[builder(default = default_max_key_bytes())] max_key_bytes: usize,
        #[builder(default = default_max_value_bytes())] max_value_bytes: usize,
        #[builder(default = default_max_operations_per_write())] max_operations_per_write: usize,
        #[builder(default = default_max_batch_payload_bytes())] max_batch_payload_bytes: usize,
        #[builder(default = default_max_namespace_name_bytes())] max_namespace_name_bytes: usize,
        #[builder(default = default_max_relationship_string_bytes())]
        max_relationship_string_bytes: usize,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            max_key_bytes,
            max_value_bytes,
            max_operations_per_write,
            max_batch_payload_bytes,
            max_namespace_name_bytes,
            max_relationship_string_bytes,
        };
        config.validate()?;
        Ok(config)
    }
}

impl ValidationConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure all limits are positive.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any limit is zero.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_key_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_key_bytes must be >= 1".to_string(),
            });
        }
        if self.max_value_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_value_bytes must be >= 1".to_string(),
            });
        }
        if self.max_operations_per_write == 0 {
            return Err(ConfigError::Validation {
                message: "max_operations_per_write must be >= 1".to_string(),
            });
        }
        if self.max_batch_payload_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_batch_payload_bytes must be >= 1".to_string(),
            });
        }
        if self.max_namespace_name_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_namespace_name_bytes must be >= 1".to_string(),
            });
        }
        if self.max_relationship_string_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_relationship_string_bytes must be >= 1".to_string(),
            });
        }
        Ok(())
    }
}

/// Default backup retention count.
fn default_backup_retention_count() -> usize {
    7
}

/// Default backup schedule interval (24 hours).
fn default_backup_interval_secs() -> u64 {
    86400
}

fn default_dependency_check_timeout_secs() -> u64 {
    2
}

fn default_health_cache_ttl_secs() -> u64 {
    5
}

fn default_max_raft_lag() -> u64 {
    1000
}

/// Configuration for dependency health checks.
///
/// Controls timeouts and thresholds for external dependency validation
/// (disk writability, peer reachability, Raft log lag). Each check has
/// an independent timeout to prevent a slow dependency from blocking
/// the entire health probe.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::HealthCheckConfig;
/// let config = HealthCheckConfig::default();
/// assert_eq!(config.dependency_check_timeout_secs, 2);
/// assert_eq!(config.health_cache_ttl_secs, 5);
/// assert_eq!(config.max_raft_lag, 1000);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Timeout in seconds for each individual dependency check (disk, peer).
    ///
    /// Default: 2 seconds. Must be >= 1.
    #[serde(default = "default_dependency_check_timeout_secs")]
    pub dependency_check_timeout_secs: u64,

    /// Time-to-live in seconds for cached health check results.
    ///
    /// Prevents I/O storms from aggressive probe intervals. Subsequent
    /// probes within the TTL window return the cached result.
    ///
    /// Default: 5 seconds. Must be >= 1.
    #[serde(default = "default_health_cache_ttl_secs")]
    pub health_cache_ttl_secs: u64,

    /// Maximum Raft log entries behind the leader before readiness fails.
    ///
    /// Compares `last_log_index - last_applied.index` from Raft metrics.
    /// When the lag exceeds this threshold, the node is considered too far
    /// behind to serve consistent reads.
    ///
    /// Default: 1000. Must be >= 1.
    #[serde(default = "default_max_raft_lag")]
    pub max_raft_lag: u64,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            dependency_check_timeout_secs: default_dependency_check_timeout_secs(),
            health_cache_ttl_secs: default_health_cache_ttl_secs(),
            max_raft_lag: default_max_raft_lag(),
        }
    }
}

impl HealthCheckConfig {
    /// Validate an existing config (e.g., after deserialization).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.dependency_check_timeout_secs == 0 {
            return Err(ConfigError::Validation {
                message: "dependency_check_timeout_secs must be >= 1".to_string(),
            });
        }
        if self.health_cache_ttl_secs == 0 {
            return Err(ConfigError::Validation {
                message: "health_cache_ttl_secs must be >= 1".to_string(),
            });
        }
        if self.max_raft_lag == 0 {
            return Err(ConfigError::Validation {
                message: "max_raft_lag must be >= 1".to_string(),
            });
        }
        Ok(())
    }
}

/// Backup and restore configuration.
///
/// Controls where backups are stored, how many to retain, and whether
/// automated backups are enabled. Backups build on the existing snapshot
/// infrastructure, adding a configurable destination and retention policy.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::BackupConfig;
/// let config = BackupConfig::builder()
///     .destination("/var/backups/ledger")
///     .build()
///     .expect("valid backup config");
/// assert_eq!(config.retention_count, 7);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackupConfig {
    /// Backup destination path (local directory or object store URL).
    ///
    /// For local storage, this is an absolute path to the backup directory.
    /// The directory is created automatically if it does not exist.
    pub destination: String,

    /// Maximum number of backups to retain before pruning oldest.
    ///
    /// Must be >= 1. Default: 7.
    #[serde(default = "default_backup_retention_count")]
    pub retention_count: usize,

    /// Enable automated periodic backups. Default: false.
    #[serde(default)]
    pub enabled: bool,

    /// Interval between automated backups in seconds.
    ///
    /// Only used when `enabled` is true. Must be >= 60. Default: 86400 (24 hours).
    #[serde(default = "default_backup_interval_secs")]
    pub interval_secs: u64,
}

#[bon::bon]
impl BackupConfig {
    /// Create a new backup configuration with validation.
    #[builder]
    pub fn new(
        #[builder(into)] destination: String,
        #[builder(default = default_backup_retention_count())] retention_count: usize,
        #[builder(default)] enabled: bool,
        #[builder(default = default_backup_interval_secs())] interval_secs: u64,
    ) -> Result<Self, ConfigError> {
        if destination.is_empty() {
            return Err(ConfigError::Validation {
                message: "backup destination must not be empty".to_string(),
            });
        }
        if retention_count == 0 {
            return Err(ConfigError::Validation {
                message: "backup retention_count must be >= 1".to_string(),
            });
        }
        if enabled && interval_secs < 60 {
            return Err(ConfigError::Validation {
                message: "backup interval_secs must be >= 60 when enabled".to_string(),
            });
        }
        Ok(Self { destination, retention_count, enabled, interval_secs })
    }

    /// Validate an existing backup configuration (e.g., after deserialization).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.destination.is_empty() {
            return Err(ConfigError::Validation {
                message: "backup destination must not be empty".to_string(),
            });
        }
        if self.retention_count == 0 {
            return Err(ConfigError::Validation {
                message: "backup retention_count must be >= 1".to_string(),
            });
        }
        if self.enabled && self.interval_secs < 60 {
            return Err(ConfigError::Validation {
                message: "backup interval_secs must be >= 60 when enabled".to_string(),
            });
        }
        Ok(())
    }
}

/// Runtime-reconfigurable configuration subset.
///
/// Contains only parameters that can be safely changed without a server restart.
/// Stored behind `Arc<ArcSwap<RuntimeConfig>>` for lock-free reads on every RPC.
///
/// # Reconfigurable vs Non-Reconfigurable
///
/// **Reconfigurable** (this struct): Operational knobs that affect behavior
/// without changing server identity or storage layout.
///
/// **Non-reconfigurable** (require restart): Listen address, data directory,
/// Raft topology, storage engine settings.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::RuntimeConfig;
/// // All fields default to None (disabled) when not set.
/// let config = RuntimeConfig::builder().build();
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, bon::Builder)]
pub struct RuntimeConfig {
    /// Rate limiting thresholds. `None` disables rate limiting.
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
    /// Hot key detection thresholds. `None` disables hot key detection.
    #[serde(default)]
    pub hot_key: Option<HotKeyConfig>,
    /// B+ tree compaction parameters.
    #[serde(default)]
    pub compaction: Option<BTreeCompactionConfig>,
    /// Input validation limits.
    #[serde(default)]
    pub validation: Option<ValidationConfig>,
}

/// Identifies a non-reconfigurable parameter that was included in an update request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonReconfigurableField {
    /// The field name (e.g. "listen_addr", "data_dir").
    pub name: String,
    /// Human-readable reason why this field cannot be changed at runtime.
    pub reason: String,
}

impl RuntimeConfig {
    /// Validate all present config sections.
    ///
    /// Returns `Ok(())` if all sections pass validation,
    /// or `Err(ConfigError)` with the first validation failure.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(ref rl) = self.rate_limit {
            rl.validate()?;
        }
        if let Some(ref hk) = self.hot_key {
            hk.validate()?;
        }
        if let Some(ref c) = self.compaction {
            c.validate()?;
        }
        if let Some(ref v) = self.validation {
            v.validate()?;
        }
        Ok(())
    }

    /// Compute the list of field paths that differ between two runtime configs.
    ///
    /// Returns human-readable strings like `"rate_limit.client_burst"` for
    /// audit logging and operator feedback.
    #[must_use]
    pub fn diff(&self, other: &RuntimeConfig) -> Vec<String> {
        let mut changes = Vec::new();
        if self.rate_limit != other.rate_limit {
            changes.push("rate_limit".to_string());
        }
        if self.hot_key != other.hot_key {
            changes.push("hot_key".to_string());
        }
        if self.compaction != other.compaction {
            changes.push("compaction".to_string());
        }
        if self.validation != other.validation {
            changes.push("validation".to_string());
        }
        changes
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
        assert!(RateLimitConfig::default().validate().is_ok());
    }

    #[test]
    fn test_builder_matches_default() {
        assert_eq!(StorageConfig::builder().build().expect("valid"), StorageConfig::default());
        assert_eq!(RaftConfig::builder().build().expect("valid"), RaftConfig::default());
        assert_eq!(BatchConfig::builder().build().expect("valid"), BatchConfig::default());
        assert_eq!(RateLimitConfig::builder().build().expect("valid"), RateLimitConfig::default());
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

    // =========================================================================
    // RateLimitConfig validation tests
    // =========================================================================

    #[test]
    fn test_rate_limit_config_defaults_are_valid() {
        let config = RateLimitConfig::builder().build().expect("defaults should be valid");
        assert_eq!(config.client_burst, default_client_burst());
        assert_eq!(config.client_rate, default_client_rate());
        assert_eq!(config.namespace_burst, default_namespace_burst());
        assert_eq!(config.namespace_rate, default_namespace_rate());
        assert_eq!(config.backpressure_threshold, default_backpressure_threshold());
    }

    #[test]
    fn test_rate_limit_config_builder_with_custom_values() {
        let config = RateLimitConfig::builder()
            .client_burst(200)
            .client_rate(100.0)
            .namespace_burst(5000)
            .namespace_rate(2000.0)
            .backpressure_threshold(200)
            .build()
            .expect("valid custom config");
        assert_eq!(config.client_burst, 200);
        assert_eq!(config.client_rate, 100.0);
        assert_eq!(config.namespace_burst, 5000);
        assert_eq!(config.namespace_rate, 2000.0);
        assert_eq!(config.backpressure_threshold, 200);
    }

    #[test]
    fn test_rate_limit_config_client_burst_zero() {
        let result = RateLimitConfig::builder().client_burst(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("client_burst"));
    }

    #[test]
    fn test_rate_limit_config_client_rate_zero() {
        let result = RateLimitConfig::builder().client_rate(0.0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("client_rate"));
    }

    #[test]
    fn test_rate_limit_config_client_rate_negative() {
        let result = RateLimitConfig::builder().client_rate(-1.0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_rate_limit_config_namespace_burst_zero() {
        let result = RateLimitConfig::builder().namespace_burst(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("namespace_burst"));
    }

    #[test]
    fn test_rate_limit_config_namespace_rate_zero() {
        let result = RateLimitConfig::builder().namespace_rate(0.0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("namespace_rate"));
    }

    #[test]
    fn test_rate_limit_config_backpressure_threshold_zero() {
        let result = RateLimitConfig::builder().backpressure_threshold(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("backpressure_threshold"));
    }

    #[test]
    fn test_rate_limit_config_boundary_values() {
        // Minimum valid values
        let result = RateLimitConfig::builder()
            .client_burst(1)
            .client_rate(0.001)
            .namespace_burst(1)
            .namespace_rate(0.001)
            .backpressure_threshold(1)
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_rate_limit_config_default_impl() {
        let config = RateLimitConfig::default();
        assert_eq!(config.client_burst, 100);
        assert_eq!(config.client_rate, 50.0);
        assert_eq!(config.namespace_burst, 1000);
        assert_eq!(config.namespace_rate, 500.0);
        assert_eq!(config.backpressure_threshold, 100);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_rate_limit_config_validate_method() {
        let mut config = RateLimitConfig::default();
        assert!(config.validate().is_ok());

        config.client_burst = 0;
        assert!(config.validate().is_err());

        config.client_burst = 100;
        config.namespace_rate = 0.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_rate_limit_config_serde_roundtrip() {
        let config = RateLimitConfig::builder()
            .client_burst(200)
            .client_rate(100.0)
            .namespace_burst(3000)
            .namespace_rate(1500.0)
            .backpressure_threshold(150)
            .build()
            .expect("valid");

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RateLimitConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_rate_limit_config_builder_matches_default() {
        let from_builder = RateLimitConfig::builder().build().expect("valid");
        let from_default = RateLimitConfig::default();
        assert_eq!(from_builder, from_default);
    }

    // =========================================================================
    // AuditConfig validation tests
    // =========================================================================

    #[test]
    fn test_audit_config_builder_with_valid_values() {
        let config = AuditConfig::builder()
            .path("/var/log/audit.jsonl")
            .max_file_size_bytes(50 * 1024 * 1024)
            .max_rotated_files(20)
            .build()
            .expect("valid config");
        assert_eq!(config.path, "/var/log/audit.jsonl");
        assert_eq!(config.max_file_size_bytes, 50 * 1024 * 1024);
        assert_eq!(config.max_rotated_files, 20);
    }

    #[test]
    fn test_audit_config_builder_with_defaults() {
        let config = AuditConfig::builder()
            .path("/var/log/audit.jsonl")
            .build()
            .expect("valid config with defaults");
        assert_eq!(config.max_file_size_bytes, 100 * 1024 * 1024);
        assert_eq!(config.max_rotated_files, 10);
    }

    #[test]
    fn test_audit_config_empty_path() {
        let result = AuditConfig::builder().path("").build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("path"));
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_audit_config_file_size_too_small() {
        let result = AuditConfig::builder()
            .path("/var/log/audit.jsonl")
            .max_file_size_bytes(MIN_AUDIT_FILE_SIZE - 1)
            .build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("max_file_size_bytes"));
        assert!(err.to_string().contains("1 MB"));
    }

    #[test]
    fn test_audit_config_file_size_minimum() {
        let result = AuditConfig::builder()
            .path("/var/log/audit.jsonl")
            .max_file_size_bytes(MIN_AUDIT_FILE_SIZE)
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_audit_config_max_rotated_files_zero() {
        let result =
            AuditConfig::builder().path("/var/log/audit.jsonl").max_rotated_files(0).build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("max_rotated_files"));
    }

    #[test]
    fn test_audit_config_max_rotated_files_one() {
        let result =
            AuditConfig::builder().path("/var/log/audit.jsonl").max_rotated_files(1).build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_audit_config_validate_method() {
        let mut config = AuditConfig {
            path: "/var/log/audit.jsonl".to_string(),
            max_file_size_bytes: 100 * 1024 * 1024,
            max_rotated_files: 10,
        };
        assert!(config.validate().is_ok());

        config.path = String::new();
        assert!(config.validate().is_err());

        config.path = "/var/log/audit.jsonl".to_string();
        config.max_file_size_bytes = 0;
        assert!(config.validate().is_err());

        config.max_file_size_bytes = 100 * 1024 * 1024;
        config.max_rotated_files = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_audit_config_serde_roundtrip() {
        let config = AuditConfig::builder()
            .path("/var/log/audit.jsonl")
            .max_file_size_bytes(50 * 1024 * 1024)
            .max_rotated_files(5)
            .build()
            .expect("valid");

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AuditConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_audit_config_into_string() {
        // Test that #[builder(into)] works for path
        let config = AuditConfig::builder()
            .path("/var/log/audit.jsonl") // &str should work via Into<String>
            .build()
            .expect("valid");
        assert_eq!(config.path, "/var/log/audit.jsonl");
    }

    // =========================================================================
    // BTreeCompactionConfig tests
    // =========================================================================

    #[test]
    fn test_btree_compaction_config_defaults() {
        let config = BTreeCompactionConfig::default();
        assert!((config.min_fill_factor - 0.4).abs() < f64::EPSILON);
        assert_eq!(config.interval_secs, 3600);
    }

    #[test]
    fn test_btree_compaction_config_builder_defaults() {
        let config = BTreeCompactionConfig::builder().build().expect("valid");
        assert!((config.min_fill_factor - 0.4).abs() < f64::EPSILON);
        assert_eq!(config.interval_secs, 3600);
    }

    #[test]
    fn test_btree_compaction_config_builder_custom() {
        let config = BTreeCompactionConfig::builder()
            .min_fill_factor(0.6)
            .interval_secs(1800)
            .build()
            .expect("valid");
        assert!((config.min_fill_factor - 0.6).abs() < f64::EPSILON);
        assert_eq!(config.interval_secs, 1800);
    }

    #[test]
    fn test_btree_compaction_config_fill_factor_zero() {
        let result = BTreeCompactionConfig::builder().min_fill_factor(0.0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_compaction_config_fill_factor_one() {
        let result = BTreeCompactionConfig::builder().min_fill_factor(1.0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_compaction_config_fill_factor_negative() {
        let result = BTreeCompactionConfig::builder().min_fill_factor(-0.1).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_compaction_config_interval_too_short() {
        let result = BTreeCompactionConfig::builder().interval_secs(59).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_compaction_config_interval_minimum() {
        let config =
            BTreeCompactionConfig::builder().interval_secs(60).build().expect("valid at minimum");
        assert_eq!(config.interval_secs, 60);
    }

    #[test]
    fn test_btree_compaction_config_serde_roundtrip() {
        let config = BTreeCompactionConfig::builder()
            .min_fill_factor(0.5)
            .interval_secs(7200)
            .build()
            .expect("valid");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: BTreeCompactionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_btree_compaction_config_validate_after_deserialize() {
        // Simulate deserializing invalid config
        let config = BTreeCompactionConfig { min_fill_factor: 1.5, interval_secs: 3600 };
        assert!(config.validate().is_err());
    }

    // =========================================================================
    // ShutdownConfig validation tests
    // =========================================================================

    #[test]
    fn test_shutdown_config_defaults_are_valid() {
        let config = ShutdownConfig::default();
        assert_eq!(config.grace_period_secs, 15);
        assert_eq!(config.drain_timeout_secs, 30);
        assert_eq!(config.pre_stop_delay_secs, 5);
        assert_eq!(config.pre_shutdown_timeout_secs, 60);
        assert_eq!(config.watchdog_multiplier, 2);
        config.validate().expect("defaults should be valid");
    }

    #[test]
    fn test_shutdown_config_builder_defaults() {
        let config = ShutdownConfig::builder().build().expect("valid");
        assert_eq!(config.grace_period_secs, 15);
        assert_eq!(config.drain_timeout_secs, 30);
        assert_eq!(config.pre_stop_delay_secs, 5);
        assert_eq!(config.pre_shutdown_timeout_secs, 60);
        assert_eq!(config.watchdog_multiplier, 2);
    }

    #[test]
    fn test_shutdown_config_builder_custom() {
        let config = ShutdownConfig::builder()
            .grace_period_secs(10)
            .drain_timeout_secs(60)
            .pre_stop_delay_secs(3)
            .pre_shutdown_timeout_secs(120)
            .watchdog_multiplier(3)
            .build()
            .expect("valid custom config");
        assert_eq!(config.grace_period_secs, 10);
        assert_eq!(config.drain_timeout_secs, 60);
        assert_eq!(config.pre_stop_delay_secs, 3);
        assert_eq!(config.pre_shutdown_timeout_secs, 120);
        assert_eq!(config.watchdog_multiplier, 3);
    }

    #[test]
    fn test_shutdown_config_grace_period_zero() {
        let result = ShutdownConfig::builder().grace_period_secs(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_shutdown_config_grace_period_minimum() {
        let config =
            ShutdownConfig::builder().grace_period_secs(1).build().expect("valid at minimum");
        assert_eq!(config.grace_period_secs, 1);
    }

    #[test]
    fn test_shutdown_config_drain_timeout_too_short() {
        let result = ShutdownConfig::builder().drain_timeout_secs(4).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_shutdown_config_drain_timeout_minimum() {
        let config =
            ShutdownConfig::builder().drain_timeout_secs(5).build().expect("valid at minimum");
        assert_eq!(config.drain_timeout_secs, 5);
    }

    #[test]
    fn test_shutdown_config_pre_shutdown_timeout_too_short() {
        let result = ShutdownConfig::builder().pre_shutdown_timeout_secs(4).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_shutdown_config_pre_shutdown_timeout_minimum() {
        let config = ShutdownConfig::builder()
            .pre_shutdown_timeout_secs(5)
            .build()
            .expect("valid at minimum");
        assert_eq!(config.pre_shutdown_timeout_secs, 5);
    }

    #[test]
    fn test_shutdown_config_watchdog_multiplier_zero() {
        let result = ShutdownConfig::builder().watchdog_multiplier(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_shutdown_config_pre_stop_delay_zero_allowed() {
        let config =
            ShutdownConfig::builder().pre_stop_delay_secs(0).build().expect("zero is valid");
        assert_eq!(config.pre_stop_delay_secs, 0);
    }

    #[test]
    fn test_shutdown_config_serde_roundtrip() {
        let config = ShutdownConfig::builder()
            .grace_period_secs(20)
            .drain_timeout_secs(45)
            .pre_stop_delay_secs(10)
            .pre_shutdown_timeout_secs(90)
            .watchdog_multiplier(4)
            .build()
            .expect("valid");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ShutdownConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_shutdown_config_validate_after_deserialize() {
        let config = ShutdownConfig {
            grace_period_secs: 0,
            drain_timeout_secs: 30,
            pre_stop_delay_secs: 5,
            pre_shutdown_timeout_secs: 60,
            watchdog_multiplier: 2,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_shutdown_config_serde_defaults() {
        let json = "{}";
        let config: ShutdownConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.grace_period_secs, 15);
        assert_eq!(config.drain_timeout_secs, 30);
        assert_eq!(config.pre_stop_delay_secs, 5);
        assert_eq!(config.pre_shutdown_timeout_secs, 60);
        assert_eq!(config.watchdog_multiplier, 2);
    }

    // ─── HotKeyConfig Tests ───────────────────────────────────

    #[test]
    fn test_hot_key_config_defaults_are_valid() {
        let config = HotKeyConfig::default();
        assert_eq!(config.window_secs, 60);
        assert_eq!(config.threshold, 100);
        assert_eq!(config.cms_width, 1024);
        assert_eq!(config.cms_depth, 4);
        assert_eq!(config.top_k, 10);
        config.validate().unwrap();
    }

    #[test]
    fn test_hot_key_config_builder_defaults() {
        let config = HotKeyConfig::builder().build().unwrap();
        assert_eq!(config, HotKeyConfig::default());
    }

    #[test]
    fn test_hot_key_config_builder_custom() {
        let config = HotKeyConfig::builder()
            .window_secs(30)
            .threshold(200)
            .cms_width(2048)
            .cms_depth(6)
            .top_k(20)
            .build()
            .unwrap();
        assert_eq!(config.window_secs, 30);
        assert_eq!(config.threshold, 200);
        assert_eq!(config.cms_width, 2048);
        assert_eq!(config.cms_depth, 6);
        assert_eq!(config.top_k, 20);
    }

    #[test]
    fn test_hot_key_config_window_secs_zero() {
        assert!(HotKeyConfig::builder().window_secs(0).build().is_err());
    }

    #[test]
    fn test_hot_key_config_window_secs_minimum() {
        let config = HotKeyConfig::builder().window_secs(1).build().unwrap();
        assert_eq!(config.window_secs, 1);
    }

    #[test]
    fn test_hot_key_config_threshold_zero() {
        assert!(HotKeyConfig::builder().threshold(0).build().is_err());
    }

    #[test]
    fn test_hot_key_config_threshold_minimum() {
        let config = HotKeyConfig::builder().threshold(1).build().unwrap();
        assert_eq!(config.threshold, 1);
    }

    #[test]
    fn test_hot_key_config_cms_width_too_small() {
        assert!(HotKeyConfig::builder().cms_width(63).build().is_err());
    }

    #[test]
    fn test_hot_key_config_cms_width_minimum() {
        let config = HotKeyConfig::builder().cms_width(64).build().unwrap();
        assert_eq!(config.cms_width, 64);
    }

    #[test]
    fn test_hot_key_config_cms_depth_too_small() {
        assert!(HotKeyConfig::builder().cms_depth(1).build().is_err());
    }

    #[test]
    fn test_hot_key_config_cms_depth_minimum() {
        let config = HotKeyConfig::builder().cms_depth(2).build().unwrap();
        assert_eq!(config.cms_depth, 2);
    }

    #[test]
    fn test_hot_key_config_top_k_zero() {
        assert!(HotKeyConfig::builder().top_k(0).build().is_err());
    }

    #[test]
    fn test_hot_key_config_top_k_minimum() {
        let config = HotKeyConfig::builder().top_k(1).build().unwrap();
        assert_eq!(config.top_k, 1);
    }

    #[test]
    fn test_hot_key_config_serde_roundtrip() {
        let config = HotKeyConfig::builder()
            .window_secs(30)
            .threshold(200)
            .cms_width(512)
            .cms_depth(3)
            .top_k(5)
            .build()
            .unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: HotKeyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_hot_key_config_validate_after_deserialize() {
        let config = HotKeyConfig::default();
        config.validate().unwrap();
    }

    #[test]
    fn test_hot_key_config_serde_defaults() {
        let json = "{}";
        let config: HotKeyConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.window_secs, 60);
        assert_eq!(config.threshold, 100);
        assert_eq!(config.cms_width, 1024);
        assert_eq!(config.cms_depth, 4);
        assert_eq!(config.top_k, 10);
    }

    // =========================================================================
    // ValidationConfig tests
    // =========================================================================

    #[test]
    fn test_validation_config_defaults_are_valid() {
        let config = ValidationConfig::default();
        config.validate().unwrap();
    }

    #[test]
    fn test_validation_config_builder_defaults() {
        let config = ValidationConfig::builder().build().unwrap();
        assert_eq!(config, ValidationConfig::default());
    }

    #[test]
    fn test_validation_config_builder_custom() {
        let config = ValidationConfig::builder()
            .max_key_bytes(2048)
            .max_value_bytes(1024)
            .max_operations_per_write(500)
            .max_batch_payload_bytes(50 * 1024 * 1024)
            .max_namespace_name_bytes(128)
            .max_relationship_string_bytes(512)
            .build()
            .unwrap();
        assert_eq!(config.max_key_bytes, 2048);
        assert_eq!(config.max_value_bytes, 1024);
        assert_eq!(config.max_operations_per_write, 500);
        assert_eq!(config.max_batch_payload_bytes, 50 * 1024 * 1024);
        assert_eq!(config.max_namespace_name_bytes, 128);
        assert_eq!(config.max_relationship_string_bytes, 512);
    }

    #[test]
    fn test_validation_config_max_key_bytes_zero() {
        let result = ValidationConfig::builder().max_key_bytes(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_max_value_bytes_zero() {
        let result = ValidationConfig::builder().max_value_bytes(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_max_operations_per_write_zero() {
        let result = ValidationConfig::builder().max_operations_per_write(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_max_batch_payload_bytes_zero() {
        let result = ValidationConfig::builder().max_batch_payload_bytes(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_max_namespace_name_bytes_zero() {
        let result = ValidationConfig::builder().max_namespace_name_bytes(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_max_relationship_string_bytes_zero() {
        let result = ValidationConfig::builder().max_relationship_string_bytes(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_config_serde_roundtrip() {
        let config = ValidationConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ValidationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_validation_config_serde_defaults() {
        let json = "{}";
        let config: ValidationConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config, ValidationConfig::default());
    }

    #[test]
    fn test_validation_config_validate_method() {
        let config = ValidationConfig { max_key_bytes: 0, ..ValidationConfig::default() };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_key_bytes"));
    }
}
