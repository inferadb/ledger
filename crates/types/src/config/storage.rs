//! Storage engine configuration for B+ tree, compaction, and integrity.

use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ConfigError;

/// Minimum cache size: 1 MB.
const MIN_CACHE_SIZE_BYTES: usize = 1024 * 1024;

/// Maximum zstd compression level.
const MAX_COMPRESSION_LEVEL: i32 = 22;

/// Minimum zstd compression level.
const MIN_COMPRESSION_LEVEL: i32 = 1;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
    #[serde(with = "super::humantime_serde")]
    #[schemars(with = "String")]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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

// =========================================================================
// IntegrityConfig
// =========================================================================

/// Default scrub interval in seconds (1 hour).
const fn default_scrub_interval_secs() -> u64 {
    3600
}

/// Default percentage of pages to scrub per cycle.
const fn default_pages_per_cycle_percent() -> f64 {
    1.0
}

/// Default full scan period in seconds (4 days).
const fn default_full_scan_period_secs() -> u64 {
    345_600
}

/// Configuration for the background integrity scrubber.
///
/// The integrity scrubber periodically verifies page checksums and B-tree
/// structural invariants to detect silent data corruption (bit rot). Each
/// cycle scrubs a percentage of total pages, progressing through the entire
/// database over `full_scan_period_secs`.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::IntegrityConfig;
/// let config = IntegrityConfig::builder()
///     .scrub_interval_secs(1800)
///     .pages_per_cycle_percent(2.0)
///     .build()
///     .expect("valid integrity config");
/// assert_eq!(config.full_scan_period_secs, 345_600);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct IntegrityConfig {
    /// Interval between scrub cycles in seconds.
    ///
    /// Must be >= 60. Default: 3600 (1 hour).
    #[serde(default = "default_scrub_interval_secs")]
    pub scrub_interval_secs: u64,

    /// Percentage of total pages to check per cycle (0.0–100.0).
    ///
    /// Must be > 0.0 and <= 100.0. Default: 1.0.
    #[serde(default = "default_pages_per_cycle_percent")]
    pub pages_per_cycle_percent: f64,

    /// Target period for a full database scan in seconds.
    ///
    /// Used for progress tracking and alerting (stale scan detection).
    /// Must be >= scrub_interval_secs. Default: 345600 (4 days).
    #[serde(default = "default_full_scan_period_secs")]
    pub full_scan_period_secs: u64,
}

impl Default for IntegrityConfig {
    fn default() -> Self {
        Self {
            scrub_interval_secs: default_scrub_interval_secs(),
            pages_per_cycle_percent: default_pages_per_cycle_percent(),
            full_scan_period_secs: default_full_scan_period_secs(),
        }
    }
}

#[bon::bon]
impl IntegrityConfig {
    /// Create a new integrity scrubber configuration with validation.
    #[builder]
    pub fn new(
        #[builder(default = default_scrub_interval_secs())] scrub_interval_secs: u64,
        #[builder(default = default_pages_per_cycle_percent())] pages_per_cycle_percent: f64,
        #[builder(default = default_full_scan_period_secs())] full_scan_period_secs: u64,
    ) -> Result<Self, ConfigError> {
        let config = Self { scrub_interval_secs, pages_per_cycle_percent, full_scan_period_secs };
        config.validate()?;
        Ok(config)
    }

    /// Validate an existing configuration (e.g., after deserialization).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.scrub_interval_secs < 60 {
            return Err(ConfigError::Validation {
                message: "integrity scrub_interval_secs must be >= 60".to_string(),
            });
        }
        if self.pages_per_cycle_percent <= 0.0 || self.pages_per_cycle_percent > 100.0 {
            return Err(ConfigError::Validation {
                message: "integrity pages_per_cycle_percent must be > 0.0 and <= 100.0".to_string(),
            });
        }
        if self.full_scan_period_secs < self.scrub_interval_secs {
            return Err(ConfigError::Validation {
                message: "integrity full_scan_period_secs must be >= scrub_interval_secs"
                    .to_string(),
            });
        }
        Ok(())
    }
}

// =========================================================================
// BackupConfig
// =========================================================================

/// Default backup retention count.
fn default_backup_retention_count() -> usize {
    7
}

/// Default backup schedule interval (24 hours).
fn default_backup_interval_secs() -> u64 {
    86400
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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
