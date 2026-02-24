//! Raft consensus and batch writer configuration.

use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ConfigError;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RaftConfig {
    /// Heartbeat interval.
    ///
    /// Must be less than `election_timeout_min / 2` per Raft specification
    /// to prevent spurious leader elections.
    #[serde(default = "default_heartbeat_interval")]
    #[serde(with = "super::humantime_serde")]
    #[schemars(with = "String")]
    pub heartbeat_interval: Duration,
    /// Election timeout range (min).
    ///
    /// Must be less than `election_timeout_max`.
    #[serde(default = "default_election_timeout_min")]
    #[serde(with = "super::humantime_serde")]
    #[schemars(with = "String")]
    pub election_timeout_min: Duration,
    /// Election timeout range (max).
    ///
    /// Must be greater than `election_timeout_min`.
    #[serde(default = "default_election_timeout_max")]
    #[serde(with = "super::humantime_serde")]
    #[schemars(with = "String")]
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
    #[serde(with = "super::humantime_serde")]
    #[schemars(with = "String")]
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

// =========================================================================
// BatchConfig
// =========================================================================

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BatchConfig {
    /// Maximum transactions per batch.
    ///
    /// Must be > 0.
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Maximum wait time before flushing a partial batch.
    #[serde(default = "default_batch_timeout")]
    #[serde(with = "super::humantime_serde")]
    #[schemars(with = "String")]
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

fn default_max_batch_size() -> usize {
    100
}

fn default_batch_timeout() -> Duration {
    Duration::from_millis(5)
}

fn default_coalesce_enabled() -> bool {
    true
}

/// Configuration for client sequence TTL eviction.
///
/// Controls how frequently expired client sequence entries are purged
/// from the replicated state. Eviction is deterministic from the Raft
/// log index, ensuring all replicas evict identical entries.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::ClientSequenceEvictionConfig;
/// let config = ClientSequenceEvictionConfig::builder()
///     .eviction_interval(500)
///     .ttl_seconds(3600)
///     .build()
///     .expect("valid eviction config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ClientSequenceEvictionConfig {
    /// Eviction triggers when `last_applied.index % eviction_interval == 0`.
    ///
    /// Lower values mean more frequent eviction checks (more CPU per apply cycle).
    /// Higher values mean less frequent checks (entries may live slightly beyond TTL).
    #[serde(default = "default_eviction_interval")]
    pub eviction_interval: u64,
    /// Time-to-live in seconds for client sequence entries.
    ///
    /// Entries where `proposed_at - last_seen > ttl_seconds` are evicted.
    /// This bounds the cross-failover deduplication window.
    #[serde(default = "default_ttl_seconds")]
    pub ttl_seconds: i64,
}

#[bon::bon]
impl ClientSequenceEvictionConfig {
    /// Creates a new eviction configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if values are out of range.
    #[builder]
    pub fn new(
        #[builder(default = default_eviction_interval())] eviction_interval: u64,
        #[builder(default = default_ttl_seconds())] ttl_seconds: i64,
    ) -> Result<Self, ConfigError> {
        let config = Self { eviction_interval, ttl_seconds };
        config.validate()?;
        Ok(config)
    }
}

impl ClientSequenceEvictionConfig {
    /// Validates the configuration values.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.eviction_interval == 0 {
            return Err(ConfigError::Validation {
                message: "eviction_interval must be > 0".to_string(),
            });
        }
        if self.ttl_seconds <= 0 {
            return Err(ConfigError::Validation { message: "ttl_seconds must be > 0".to_string() });
        }
        Ok(())
    }
}

impl Default for ClientSequenceEvictionConfig {
    fn default() -> Self {
        Self { eviction_interval: default_eviction_interval(), ttl_seconds: default_ttl_seconds() }
    }
}

fn default_eviction_interval() -> u64 {
    1_000
}

fn default_ttl_seconds() -> i64 {
    86_400 // 24 hours
}
