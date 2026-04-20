//! Raft consensus, batch writer, sequence eviction, and post-erasure compaction configuration.

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
    #[serde(with = "humantime_serde")]
    #[schemars(with = "String")]
    pub heartbeat_interval: Duration,
    /// Election timeout range (min).
    ///
    /// Must be less than `election_timeout_max`.
    #[serde(default = "default_election_timeout_min")]
    #[serde(with = "humantime_serde")]
    #[schemars(with = "String")]
    pub election_timeout_min: Duration,
    /// Election timeout range (max).
    ///
    /// Must be greater than `election_timeout_min`.
    #[serde(default = "default_election_timeout_max")]
    #[serde(with = "humantime_serde")]
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
    #[serde(with = "humantime_serde")]
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
// PostErasureCompactionConfig
// =========================================================================

/// Default maximum log retention in seconds (1 hour).
///
/// After this duration without a snapshot, the compaction job triggers one
/// to ensure Raft log entries (which may contain encrypted PII) are purged.
const fn default_max_log_retention_secs() -> u64 {
    3600
}

/// Default interval between compaction check cycles in seconds (5 minutes).
const fn default_compaction_check_interval_secs() -> u64 {
    300
}

/// Configuration for post-erasure Raft log compaction.
///
/// After user erasure or organization purge, encrypted PII entries remain
/// in the Raft log until the next snapshot triggers log compaction. This
/// job enforces a maximum log retention period by triggering proactive
/// snapshots when the time since last snapshot exceeds the threshold.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::PostErasureCompactionConfig;
/// let config = PostErasureCompactionConfig::builder()
///     .max_log_retention_secs(1800)
///     .check_interval_secs(120)
///     .build()
///     .expect("valid compaction config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PostErasureCompactionConfig {
    /// Maximum seconds a Raft log may retain entries before a snapshot
    /// is triggered to compact them.
    ///
    /// Must be >= 300 (5 minutes). Default: 3600 (1 hour).
    #[serde(default = "default_max_log_retention_secs")]
    pub max_log_retention_secs: u64,
    /// Interval between compaction check cycles in seconds.
    ///
    /// Must be >= 60. Default: 300 (5 minutes).
    #[serde(default = "default_compaction_check_interval_secs")]
    pub check_interval_secs: u64,
}

impl Default for PostErasureCompactionConfig {
    fn default() -> Self {
        Self {
            max_log_retention_secs: default_max_log_retention_secs(),
            check_interval_secs: default_compaction_check_interval_secs(),
        }
    }
}

#[bon::bon]
impl PostErasureCompactionConfig {
    /// Creates a new post-erasure compaction configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `max_log_retention_secs` < 300
    /// - `check_interval_secs` < 60
    #[builder]
    pub fn new(
        #[builder(default = default_max_log_retention_secs())] max_log_retention_secs: u64,
        #[builder(default = default_compaction_check_interval_secs())] check_interval_secs: u64,
    ) -> Result<Self, ConfigError> {
        let config = Self { max_log_retention_secs, check_interval_secs };
        config.validate()?;
        Ok(config)
    }
}

impl PostErasureCompactionConfig {
    /// Validates the configuration values.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_log_retention_secs < 300 {
            return Err(ConfigError::Validation {
                message: format!(
                    "max_log_retention_secs must be >= 300, got {}",
                    self.max_log_retention_secs
                ),
            });
        }
        if self.check_interval_secs < 60 {
            return Err(ConfigError::Validation {
                message: format!(
                    "check_interval_secs must be >= 60, got {}",
                    self.check_interval_secs
                ),
            });
        }
        Ok(())
    }
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
    /// Upper bound on a single coalesced batch. Under concurrent load, a
    /// larger cap lets more proposals amortize a single WAL fsync. Must be
    /// > 0. Default: 500.
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Maximum wait time before flushing a partial batch.
    ///
    /// Caps added latency when proposals arrive one at a time. Under
    /// concurrent load `max_batch_size` usually triggers first. Default:
    /// 10ms.
    #[serde(default = "default_batch_timeout")]
    #[serde(with = "humantime_serde")]
    #[schemars(with = "String")]
    pub batch_timeout: Duration,
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
    ) -> Result<Self, ConfigError> {
        let config = Self { max_batch_size, batch_timeout };
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
        Self { max_batch_size: default_max_batch_size(), batch_timeout: default_batch_timeout() }
    }
}

fn default_max_batch_size() -> usize {
    // Under concurrent load, a larger cap lets more proposals amortize a
    // single WAL fsync. With `wal_sync_mode = barrier` the fsync itself is
    // ~2-5ms on APFS SSDs, so the batch cap — not fsync latency — becomes
    // the binding constraint; 2000 keeps the cap well above realistic
    // per-region burst rates while `batch_timeout` (10ms) still bounds
    // per-op latency at low load.
    2000
}

fn default_batch_timeout() -> Duration {
    // Give proposals more time to accumulate; caps added latency under
    // single-client load.
    Duration::from_millis(10)
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use super::*;

    // =========================================================================
    // RaftConfig tests
    // =========================================================================

    #[test]
    fn raft_config_election_timeout_min_gte_max_fails() {
        let result = RaftConfig::builder()
            .election_timeout_min(Duration::from_millis(500))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_err());

        let result = RaftConfig::builder()
            .election_timeout_min(Duration::from_millis(600))
            .election_timeout_max(Duration::from_millis(500))
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn raft_config_heartbeat_too_large_fails() {
        // heartbeat must be < election_timeout_min / 2
        // min=300ms, so heartbeat must be < 150ms
        let result = RaftConfig::builder()
            .heartbeat_interval(Duration::from_millis(150))
            .election_timeout_min(Duration::from_millis(300))
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn raft_config_zero_max_entries_per_rpc_fails() {
        let result = RaftConfig::builder().max_entries_per_rpc(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn raft_config_zero_snapshot_threshold_fails() {
        let result = RaftConfig::builder().snapshot_threshold(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn raft_config_proposal_timeout_below_1s_fails() {
        let result = RaftConfig::builder().proposal_timeout(Duration::from_millis(999)).build();
        assert!(result.is_err());
    }

    #[test]
    fn raft_config_proposal_timeout_exactly_1s_succeeds() {
        let result = RaftConfig::builder().proposal_timeout(Duration::from_secs(1)).build();
        assert!(result.is_ok());
    }

    #[test]
    fn raft_config_serde_with_defaults() {
        // Empty JSON should deserialize to defaults
        let config: RaftConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config, RaftConfig::default());
    }

    // =========================================================================
    // PostErasureCompactionConfig tests
    // =========================================================================

    #[test]
    fn post_erasure_compaction_config_retention_below_300_fails() {
        let result = PostErasureCompactionConfig::builder().max_log_retention_secs(299).build();
        assert!(result.is_err());
    }

    #[test]
    fn post_erasure_compaction_config_retention_exactly_300_succeeds() {
        let result = PostErasureCompactionConfig::builder().max_log_retention_secs(300).build();
        assert!(result.is_ok());
    }

    #[test]
    fn post_erasure_compaction_config_interval_below_60_fails() {
        let result = PostErasureCompactionConfig::builder().check_interval_secs(59).build();
        assert!(result.is_err());
    }

    #[test]
    fn post_erasure_compaction_config_interval_exactly_60_succeeds() {
        let result = PostErasureCompactionConfig::builder().check_interval_secs(60).build();
        assert!(result.is_ok());
    }

    // =========================================================================
    // BatchConfig tests
    // =========================================================================

    #[test]
    fn batch_config_zero_batch_size_fails() {
        let result = BatchConfig::builder().max_batch_size(0).build();
        assert!(result.is_err());
    }

    // =========================================================================
    // ClientSequenceEvictionConfig tests
    // =========================================================================

    #[test]
    fn eviction_config_zero_interval_fails() {
        let result = ClientSequenceEvictionConfig::builder().eviction_interval(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn eviction_config_zero_ttl_fails() {
        let result = ClientSequenceEvictionConfig::builder().ttl_seconds(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn eviction_config_negative_ttl_fails() {
        let result = ClientSequenceEvictionConfig::builder().ttl_seconds(-1).build();
        assert!(result.is_err());
    }

    #[test]
    fn eviction_config_serde_defaults_from_empty() {
        let config: ClientSequenceEvictionConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config, ClientSequenceEvictionConfig::default());
    }
}
