//! Storage engine, compaction, integrity, backup, tiered storage, and lifecycle purge
//! configuration.

use std::{collections::HashMap, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ConfigError;
use crate::Region;

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
    #[serde(with = "humantime_serde")]
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
// CheckpointConfig
// =========================================================================

/// Default checkpoint interval in milliseconds.
const fn default_checkpoint_interval_ms() -> u64 {
    500
}

/// Default apply-count threshold before forcing a checkpoint.
const fn default_checkpoint_applies_threshold() -> u64 {
    5_000
}

/// Default dirty-page threshold before forcing a checkpoint.
const fn default_checkpoint_dirty_pages_threshold() -> u64 {
    10_000
}

/// Minimum checkpoint interval (50 ms floor keeps tight polling cheap).
const MIN_CHECKPOINT_INTERVAL_MS: u64 = 50;

/// Maximum checkpoint interval (60 s keeps recovery replay bounded).
const MAX_CHECKPOINT_INTERVAL_MS: u64 = 60_000;

/// State-DB checkpoint scheduling configuration.
///
/// Controls the background `StateCheckpointer` task that periodically drives
/// `Database::sync_state`, amortizing the dual-slot `persist_state_to_disk`
/// fsync cost across many in-memory commits. A checkpoint fires when any of
/// the three thresholds is crossed:
///
/// 1. **Time** — more than `interval_ms` has elapsed since the last successful checkpoint.
/// 2. **Apply count** — `applies_threshold` or more applies have landed in memory since the last
///    successful checkpoint.
/// 3. **Dirty pages** — the page cache is holding more than `dirty_pages_threshold` dirty pages
///    (backpressure against unbounded cache growth under sustained write load).
///
/// Values are runtime-reconfigurable via `UpdateConfig`; the checkpointer
/// re-reads from `RuntimeConfigHandle` on each wake-up so changes take
/// effect on the next tick.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::CheckpointConfig;
/// let config = CheckpointConfig::builder()
///     .interval_ms(250)
///     .applies_threshold(2_500)
///     .dirty_pages_threshold(5_000)
///     .build()
///     .expect("valid checkpoint config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CheckpointConfig {
    /// Time between checkpoint wake-ups in milliseconds.
    ///
    /// Must be in `[50, 60_000]`. Default: 500.
    #[serde(default = "default_checkpoint_interval_ms")]
    pub interval_ms: u64,
    /// Number of applies accumulated in memory that will force a checkpoint.
    ///
    /// Must be >= 1. Default: 5_000.
    #[serde(default = "default_checkpoint_applies_threshold")]
    pub applies_threshold: u64,
    /// Number of dirty pages in the page cache that will force a checkpoint.
    ///
    /// Must be >= 1. Default: 10_000 (~40MB at a 4KB page size).
    #[serde(default = "default_checkpoint_dirty_pages_threshold")]
    pub dirty_pages_threshold: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval_ms: default_checkpoint_interval_ms(),
            applies_threshold: default_checkpoint_applies_threshold(),
            dirty_pages_threshold: default_checkpoint_dirty_pages_threshold(),
        }
    }
}

#[bon::bon]
impl CheckpointConfig {
    /// Creates a new checkpoint configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `interval_ms` is outside `[50, 60_000]`
    /// - `applies_threshold` is 0
    /// - `dirty_pages_threshold` is 0
    #[builder]
    pub fn new(
        #[builder(default = default_checkpoint_interval_ms())] interval_ms: u64,
        #[builder(default = default_checkpoint_applies_threshold())] applies_threshold: u64,
        #[builder(default = default_checkpoint_dirty_pages_threshold())] dirty_pages_threshold: u64,
    ) -> Result<Self, ConfigError> {
        let config = Self { interval_ms, applies_threshold, dirty_pages_threshold };
        config.validate()?;
        Ok(config)
    }
}

impl CheckpointConfig {
    /// Validates the configuration values.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any field is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.interval_ms < MIN_CHECKPOINT_INTERVAL_MS
            || self.interval_ms > MAX_CHECKPOINT_INTERVAL_MS
        {
            return Err(ConfigError::Validation {
                message: format!(
                    "interval_ms must be in [{}, {}], got {}",
                    MIN_CHECKPOINT_INTERVAL_MS, MAX_CHECKPOINT_INTERVAL_MS, self.interval_ms
                ),
            });
        }
        if self.applies_threshold == 0 {
            return Err(ConfigError::Validation {
                message: "applies_threshold must be >= 1".to_string(),
            });
        }
        if self.dirty_pages_threshold == 0 {
            return Err(ConfigError::Validation {
                message: "dirty_pages_threshold must be >= 1".to_string(),
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
    /// Creates a new integrity scrubber configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `scrub_interval_secs` < 60
    /// - `pages_per_cycle_percent` is not in (0.0, 100.0]
    /// - `full_scan_period_secs` < `scrub_interval_secs`
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

    /// Validates an existing configuration (e.g., after deserialization).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
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

/// Default maximum backup retention in days for GDPR compliance.
fn default_max_backup_retention_days() -> u32 {
    90
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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

    /// Maximum backup retention period in days for GDPR erasure compliance.
    ///
    /// Pre-erasure backups contain subject encryption keys. After a user
    /// exercises right to erasure, operators should create a fresh backup
    /// and retire pre-erasure backups within this window. The system emits
    /// `ledger_backup_pre_erasure_count` gauge for monitoring.
    ///
    /// Must be >= 1. Default: 90.
    #[serde(default = "default_max_backup_retention_days")]
    pub max_backup_retention_days: u32,
}

#[bon::bon]
impl BackupConfig {
    /// Creates a new backup configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `destination` is empty
    /// - `retention_count` is 0
    /// - `interval_secs` < 60 when `enabled` is true
    #[builder]
    pub fn new(
        #[builder(into)] destination: String,
        #[builder(default = default_backup_retention_count())] retention_count: usize,
        #[builder(default)] enabled: bool,
        #[builder(default = default_backup_interval_secs())] interval_secs: u64,
        #[builder(default = default_max_backup_retention_days())] max_backup_retention_days: u32,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            destination,
            retention_count,
            enabled,
            interval_secs,
            max_backup_retention_days,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates an existing backup configuration (e.g., after deserialization).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
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
        if self.max_backup_retention_days == 0 {
            return Err(ConfigError::Validation {
                message: "backup max_backup_retention_days must be >= 1".to_string(),
            });
        }
        Ok(())
    }
}

// =========================================================================
// TieredStorageConfig
// =========================================================================

/// Default number of snapshots to keep in hot tier.
fn default_hot_count() -> usize {
    3
}

/// Default days to keep in warm tier before cold demotion.
fn default_warm_days() -> u32 {
    30
}

/// Default demotion interval in seconds (1 hour).
fn default_demote_interval_secs() -> u64 {
    3600
}

/// Default multipart upload threshold (50 MB).
fn default_multipart_threshold_bytes() -> usize {
    50 * 1024 * 1024
}

/// Tiered snapshot storage configuration.
///
/// Controls how snapshots are distributed across storage tiers:
/// - **Hot**: Local SSD for fast access (most recent N snapshots)
/// - **Warm**: Object storage (S3/GCS/Azure) for older snapshots
/// - **Cold**: Archive storage (future, not yet implemented)
///
/// When `warm_url` is `None`, operates in local-only mode with zero overhead.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::TieredStorageConfig;
/// let config = TieredStorageConfig::builder()
///     .hot_count(5)
///     .warm_url("s3://my-bucket/snapshots".to_string())
///     .build()
///     .expect("valid tiered storage config");
/// assert_eq!(config.demote_interval_secs, 3600);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TieredStorageConfig {
    /// Number of snapshots to keep in hot tier (local SSD).
    ///
    /// Must be >= 1. Default: 3.
    #[serde(default = "default_hot_count")]
    pub hot_count: usize,

    /// Object storage URL for warm tier.
    ///
    /// Supported schemes: `s3://`, `gs://`, `az://`, `file://`.
    /// Credentials are read from environment variables.
    /// `None` means local-only mode (no warm tier, zero overhead).
    #[serde(default)]
    pub warm_url: Option<String>,

    /// Days to keep snapshots in warm tier before cold demotion.
    ///
    /// Only relevant when cold tier is enabled. Default: 30.
    #[serde(default = "default_warm_days")]
    pub warm_days: u32,

    /// Whether cold tier archival is enabled.
    ///
    /// Cold tier support is not yet implemented. Default: false.
    #[serde(default)]
    pub cold_enabled: bool,

    /// Interval between demotion cycles in seconds.
    ///
    /// Controls how often old snapshots are moved from hot to warm tier.
    /// Must be >= 60. Default: 3600 (1 hour).
    #[serde(default = "default_demote_interval_secs")]
    pub demote_interval_secs: u64,

    /// Size threshold for multipart uploads in bytes.
    ///
    /// Snapshots larger than this are uploaded using multipart upload
    /// for reliability and S3 compatibility (single PUTs limited to 5 GB).
    /// Must be >= 5 MB. Default: 50 MB.
    #[serde(default = "default_multipart_threshold_bytes")]
    pub multipart_threshold_bytes: usize,

    /// Per-region warm tier URL overrides.
    ///
    /// Protected regions require in-region object storage buckets for data
    /// residency compliance. This map overrides the default `warm_url` for
    /// specific regions (e.g., `CA_CENTRAL_QC` → `s3://ca-central-snapshots/`).
    ///
    /// Regions not listed here use the default `warm_url`.
    #[serde(default)]
    pub region_overrides: HashMap<Region, RegionTieredStorageOverride>,
}

/// Per-region override for tiered storage configuration.
///
/// Allows protected regions to use in-region object storage buckets
/// for data residency compliance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RegionTieredStorageOverride {
    /// Object storage URL for this region's warm tier.
    ///
    /// Overrides the default `warm_url` from `TieredStorageConfig`.
    /// Must point to storage within the region's jurisdiction.
    pub warm_url: String,
}

impl TieredStorageConfig {
    /// Returns the warm tier URL for a specific region.
    ///
    /// Checks `region_overrides` first, then falls back to the default `warm_url`.
    /// Returns `None` if neither is configured (local-only mode for this region).
    pub fn warm_url_for_region(&self, region: Region) -> Option<&str> {
        if let Some(override_config) = self.region_overrides.get(&region) {
            Some(&override_config.warm_url)
        } else {
            self.warm_url.as_deref()
        }
    }
}

impl Default for TieredStorageConfig {
    fn default() -> Self {
        Self {
            hot_count: default_hot_count(),
            warm_url: None,
            warm_days: default_warm_days(),
            cold_enabled: false,
            demote_interval_secs: default_demote_interval_secs(),
            multipart_threshold_bytes: default_multipart_threshold_bytes(),
            region_overrides: HashMap::new(),
        }
    }
}

/// Minimum multipart threshold: 5 MB (S3 minimum part size).
const MIN_MULTIPART_THRESHOLD: usize = 5 * 1024 * 1024;

#[bon::bon]
impl TieredStorageConfig {
    /// Creates a new tiered storage configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `hot_count` is 0
    /// - `demote_interval_secs` < 60
    /// - `multipart_threshold_bytes` < 5 MB
    #[builder]
    pub fn new(
        #[builder(default = default_hot_count())] hot_count: usize,
        warm_url: Option<String>,
        #[builder(default = default_warm_days())] warm_days: u32,
        #[builder(default)] cold_enabled: bool,
        #[builder(default = default_demote_interval_secs())] demote_interval_secs: u64,
        #[builder(default = default_multipart_threshold_bytes())] multipart_threshold_bytes: usize,
        #[builder(default)] region_overrides: HashMap<Region, RegionTieredStorageOverride>,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            hot_count,
            warm_url,
            warm_days,
            cold_enabled,
            demote_interval_secs,
            multipart_threshold_bytes,
            region_overrides,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates an existing configuration (e.g., after deserialization).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.hot_count == 0 {
            return Err(ConfigError::Validation {
                message: "tiered storage hot_count must be >= 1".to_string(),
            });
        }
        if self.demote_interval_secs < 60 {
            return Err(ConfigError::Validation {
                message: "tiered storage demote_interval_secs must be >= 60".to_string(),
            });
        }
        if self.multipart_threshold_bytes < MIN_MULTIPART_THRESHOLD {
            return Err(ConfigError::Validation {
                message: format!(
                    "tiered storage multipart_threshold_bytes must be >= {} (5 MB)",
                    MIN_MULTIPART_THRESHOLD
                ),
            });
        }
        for (region, override_config) in &self.region_overrides {
            if override_config.warm_url.is_empty() {
                return Err(ConfigError::Validation {
                    message: format!(
                        "tiered storage region override for {} has empty warm_url",
                        region
                    ),
                });
            }
        }
        Ok(())
    }
}

// =========================================================================
// UserRetentionConfig
// =========================================================================

/// Default reaper scan interval in seconds (1 hour).
const fn default_reaper_interval_secs() -> u64 {
    3600
}

/// Default batch size for retention reaper scans.
const fn default_reaper_batch_size() -> usize {
    100
}

/// Configuration for the user retention reaper background job.
///
/// The reaper periodically scans for users in `Deleting` status whose
/// retention period has elapsed, and submits `EraseUser` Raft proposals
/// to finalize deletion. Only runs on the leader node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct UserRetentionConfig {
    /// Interval between reaper scan cycles in seconds.
    ///
    /// Must be >= 60. Default: 3600 (1 hour).
    #[serde(default = "default_reaper_interval_secs")]
    pub interval_secs: u64,
    /// Maximum number of users to process per cycle.
    ///
    /// Must be >= 1. Default: 100.
    #[serde(default = "default_reaper_batch_size")]
    pub batch_size: usize,
}

impl Default for UserRetentionConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_reaper_interval_secs(),
            batch_size: default_reaper_batch_size(),
        }
    }
}

#[bon::bon]
impl UserRetentionConfig {
    /// Creates a new retention reaper configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `interval_secs` < 60
    /// - `batch_size` < 1
    #[builder]
    pub fn new(
        #[builder(default = default_reaper_interval_secs())] interval_secs: u64,
        #[builder(default = default_reaper_batch_size())] batch_size: usize,
    ) -> Result<Self, ConfigError> {
        let config = Self { interval_secs, batch_size };
        config.validate()?;
        Ok(config)
    }
}

impl UserRetentionConfig {
    /// Validates the configuration values.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.interval_secs < 60 {
            return Err(ConfigError::Validation {
                message: format!("interval_secs must be >= 60, got {}", self.interval_secs),
            });
        }
        if self.batch_size == 0 {
            return Err(ConfigError::Validation { message: "batch_size must be >= 1".to_string() });
        }
        Ok(())
    }
}

// =========================================================================
// OrganizationPurgeConfig
// =========================================================================

/// Default purge scan interval in seconds (1 hour).
const fn default_purge_interval_secs() -> u64 {
    3600
}

/// Default batch size for organization purge scans.
const fn default_purge_batch_size() -> usize {
    50
}

/// Configuration for the organization purge background job.
///
/// The purge job periodically scans for organizations in `Deleted` status
/// whose retention cooldown has elapsed, and submits `PurgeOrganization`
/// Raft proposals to finalize removal. Only runs on the leader node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OrganizationPurgeConfig {
    /// Interval between purge scan cycles in seconds.
    ///
    /// Must be >= 60. Default: 3600 (1 hour).
    #[serde(default = "default_purge_interval_secs")]
    pub interval_secs: u64,
    /// Maximum number of organizations to process per cycle.
    ///
    /// Must be >= 1. Default: 50.
    #[serde(default = "default_purge_batch_size")]
    pub batch_size: usize,
}

impl Default for OrganizationPurgeConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_purge_interval_secs(),
            batch_size: default_purge_batch_size(),
        }
    }
}

#[bon::bon]
impl OrganizationPurgeConfig {
    /// Creates a new organization purge configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `interval_secs` < 60
    /// - `batch_size` < 1
    #[builder]
    pub fn new(
        #[builder(default = default_purge_interval_secs())] interval_secs: u64,
        #[builder(default = default_purge_batch_size())] batch_size: usize,
    ) -> Result<Self, ConfigError> {
        let config = Self { interval_secs, batch_size };
        config.validate()?;
        Ok(config)
    }
}

impl OrganizationPurgeConfig {
    /// Validates the configuration values.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.interval_secs < 60 {
            return Err(ConfigError::Validation {
                message: format!("interval_secs must be >= 60, got {}", self.interval_secs),
            });
        }
        if self.batch_size == 0 {
            return Err(ConfigError::Validation { message: "batch_size must be >= 1".to_string() });
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::field_reassign_with_default)]
mod tiered_storage_tests {
    use super::*;

    #[test]
    fn test_warm_url_for_region_default() {
        let config = TieredStorageConfig {
            warm_url: Some("s3://default-bucket/snapshots".to_string()),
            ..Default::default()
        };
        // Region with no override → uses default
        assert_eq!(
            config.warm_url_for_region(Region::US_EAST_VA),
            Some("s3://default-bucket/snapshots")
        );
    }

    #[test]
    fn test_warm_url_for_region_override() {
        let mut overrides = HashMap::new();
        overrides.insert(
            Region::CA_CENTRAL_QC,
            RegionTieredStorageOverride {
                warm_url: "s3://ca-central-bucket/snapshots".to_string(),
            },
        );
        let config = TieredStorageConfig {
            warm_url: Some("s3://default-bucket/snapshots".to_string()),
            region_overrides: overrides,
            ..Default::default()
        };
        // Overridden region → uses override
        assert_eq!(
            config.warm_url_for_region(Region::CA_CENTRAL_QC),
            Some("s3://ca-central-bucket/snapshots")
        );
        // Non-overridden → uses default
        assert_eq!(
            config.warm_url_for_region(Region::US_EAST_VA),
            Some("s3://default-bucket/snapshots")
        );
    }

    #[test]
    fn test_warm_url_for_region_no_default_no_override() {
        let config = TieredStorageConfig::default();
        // No warm_url and no override → None (local-only)
        assert_eq!(config.warm_url_for_region(Region::GLOBAL), None);
    }

    #[test]
    fn test_warm_url_for_region_override_takes_precedence() {
        let mut overrides = HashMap::new();
        overrides.insert(
            Region::IE_EAST_DUBLIN,
            RegionTieredStorageOverride { warm_url: "s3://eu-west-bucket/snapshots".to_string() },
        );
        let config = TieredStorageConfig {
            warm_url: Some("s3://us-default/snapshots".to_string()),
            region_overrides: overrides,
            ..Default::default()
        };
        assert_eq!(
            config.warm_url_for_region(Region::IE_EAST_DUBLIN),
            Some("s3://eu-west-bucket/snapshots")
        );
    }

    #[test]
    fn test_region_override_empty_url_rejected() {
        let mut overrides = HashMap::new();
        overrides
            .insert(Region::CA_CENTRAL_QC, RegionTieredStorageOverride { warm_url: String::new() });
        let config = TieredStorageConfig { region_overrides: overrides, ..Default::default() };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("empty warm_url"));
    }

    // StorageConfig tests

    #[test]
    fn storage_config_default_valid() {
        let config = StorageConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.cache_size_bytes, 256 * 1024 * 1024);
        assert_eq!(config.compression_level, 3);
    }

    #[test]
    fn storage_config_builder_defaults_valid() {
        let config = StorageConfig::builder().build().unwrap();
        assert_eq!(config.cache_size_bytes, 256 * 1024 * 1024);
    }

    #[test]
    fn storage_config_small_cache_fails() {
        let mut config = StorageConfig::default();
        config.cache_size_bytes = 1024; // 1 KB, below 1 MB minimum
        assert!(config.validate().is_err());
    }

    #[test]
    fn storage_config_compression_below_range_fails() {
        let mut config = StorageConfig::default();
        config.compression_level = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn storage_config_compression_above_range_fails() {
        let mut config = StorageConfig::default();
        config.compression_level = 23;
        assert!(config.validate().is_err());
    }

    #[test]
    fn storage_config_compression_at_boundaries() {
        let mut config = StorageConfig::default();
        config.compression_level = 1;
        assert!(config.validate().is_ok());
        config.compression_level = 22;
        assert!(config.validate().is_ok());
    }

    // BTreeCompactionConfig tests

    #[test]
    fn compaction_config_default_valid() {
        let config = BTreeCompactionConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.min_fill_factor, 0.4);
        assert_eq!(config.interval_secs, 3600);
    }

    #[test]
    fn compaction_config_builder_defaults_valid() {
        let config = BTreeCompactionConfig::builder().build().unwrap();
        assert_eq!(config.min_fill_factor, 0.4);
    }

    #[test]
    fn compaction_config_zero_fill_factor_fails() {
        let mut config = BTreeCompactionConfig::default();
        config.min_fill_factor = 0.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn compaction_config_fill_factor_one_fails() {
        let mut config = BTreeCompactionConfig::default();
        config.min_fill_factor = 1.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn compaction_config_negative_fill_factor_fails() {
        let mut config = BTreeCompactionConfig::default();
        config.min_fill_factor = -0.1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn compaction_config_low_interval_fails() {
        let mut config = BTreeCompactionConfig::default();
        config.interval_secs = 59;
        assert!(config.validate().is_err());
    }

    // CheckpointConfig tests

    #[test]
    fn checkpoint_config_default_valid() {
        let config = CheckpointConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.interval_ms, 500);
        assert_eq!(config.applies_threshold, 5_000);
        assert_eq!(config.dirty_pages_threshold, 10_000);
    }

    #[test]
    fn checkpoint_config_builder_defaults_valid() {
        let config = CheckpointConfig::builder().build().unwrap();
        assert_eq!(config.interval_ms, 500);
        assert_eq!(config.applies_threshold, 5_000);
        assert_eq!(config.dirty_pages_threshold, 10_000);
    }

    #[test]
    fn checkpoint_config_interval_too_low_fails() {
        let mut config = CheckpointConfig::default();
        config.interval_ms = 49;
        assert!(config.validate().is_err());
    }

    #[test]
    fn checkpoint_config_interval_too_high_fails() {
        let mut config = CheckpointConfig::default();
        config.interval_ms = 60_001;
        assert!(config.validate().is_err());
    }

    #[test]
    fn checkpoint_config_interval_at_bounds_ok() {
        let mut config = CheckpointConfig::default();
        config.interval_ms = 50;
        assert!(config.validate().is_ok());
        config.interval_ms = 60_000;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn checkpoint_config_zero_applies_threshold_fails() {
        let mut config = CheckpointConfig::default();
        config.applies_threshold = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn checkpoint_config_zero_dirty_pages_threshold_fails() {
        let mut config = CheckpointConfig::default();
        config.dirty_pages_threshold = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn checkpoint_config_builder_custom_values() {
        let config = CheckpointConfig::builder()
            .interval_ms(250)
            .applies_threshold(2_500)
            .dirty_pages_threshold(5_000)
            .build()
            .unwrap();
        assert_eq!(config.interval_ms, 250);
        assert_eq!(config.applies_threshold, 2_500);
        assert_eq!(config.dirty_pages_threshold, 5_000);
    }

    #[test]
    fn checkpoint_config_serde_roundtrip() {
        let config = CheckpointConfig::builder()
            .interval_ms(1000)
            .applies_threshold(1)
            .dirty_pages_threshold(100)
            .build()
            .unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: CheckpointConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn checkpoint_config_serde_defaults() {
        let config: CheckpointConfig = serde_json::from_str("{}").unwrap();
        config.validate().unwrap();
        assert_eq!(config.interval_ms, 500);
        assert_eq!(config.applies_threshold, 5_000);
        assert_eq!(config.dirty_pages_threshold, 10_000);
    }

    #[test]
    fn checkpoint_config_json_schema() {
        let schema = schemars::schema_for!(CheckpointConfig);
        let json = serde_json::to_string(&schema).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let props = value.get("properties").and_then(|v| v.as_object()).unwrap();
        assert!(props.contains_key("interval_ms"));
        assert!(props.contains_key("applies_threshold"));
        assert!(props.contains_key("dirty_pages_threshold"));
    }

    // IntegrityConfig tests

    #[test]
    fn integrity_config_default_valid() {
        let config = IntegrityConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.scrub_interval_secs, 3600);
        assert_eq!(config.pages_per_cycle_percent, 1.0);
        assert_eq!(config.full_scan_period_secs, 345_600);
    }

    #[test]
    fn integrity_config_builder_defaults_valid() {
        let config = IntegrityConfig::builder().build().unwrap();
        assert_eq!(config.scrub_interval_secs, 3600);
    }

    #[test]
    fn integrity_config_low_scrub_interval_fails() {
        let mut config = IntegrityConfig::default();
        config.scrub_interval_secs = 59;
        assert!(config.validate().is_err());
    }

    #[test]
    fn integrity_config_zero_pages_percent_fails() {
        let mut config = IntegrityConfig::default();
        config.pages_per_cycle_percent = 0.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn integrity_config_over_100_pages_percent_fails() {
        let mut config = IntegrityConfig::default();
        config.pages_per_cycle_percent = 100.1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn integrity_config_full_scan_less_than_scrub_fails() {
        let mut config = IntegrityConfig::default();
        config.full_scan_period_secs = 59;
        config.scrub_interval_secs = 60;
        assert!(config.validate().is_err());
    }

    // BackupConfig tests

    #[test]
    fn backup_config_builder_valid() {
        let config = BackupConfig::builder().destination("/var/backups").build().unwrap();
        assert_eq!(config.destination, "/var/backups");
        assert_eq!(config.retention_count, 7);
        assert!(!config.enabled);
    }

    #[test]
    fn backup_config_empty_destination_fails() {
        let result = BackupConfig::builder().destination("").build();
        assert!(result.is_err());
    }

    #[test]
    fn backup_config_zero_retention_fails() {
        let result = BackupConfig::builder().destination("/backups").retention_count(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn backup_config_enabled_low_interval_fails() {
        let result =
            BackupConfig::builder().destination("/backups").enabled(true).interval_secs(59).build();
        assert!(result.is_err());
    }

    #[test]
    fn backup_config_disabled_low_interval_ok() {
        let config = BackupConfig::builder()
            .destination("/backups")
            .enabled(false)
            .interval_secs(1)
            .build()
            .unwrap();
        assert!(!config.enabled);
    }

    #[test]
    fn backup_config_zero_retention_days_fails() {
        let result =
            BackupConfig::builder().destination("/backups").max_backup_retention_days(0).build();
        assert!(result.is_err());
    }
}
