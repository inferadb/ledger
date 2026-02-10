//! Hot key detection, audit logging, and metrics cardinality configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ConfigError;

// =========================================================================
// HotKeyConfig
// =========================================================================

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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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

// =========================================================================
// AuditConfig
// =========================================================================

/// Default max audit log file size: 100 MB.
fn default_max_file_size_bytes() -> u64 {
    100 * 1024 * 1024
}

/// Default max rotated audit log files to retain.
fn default_max_rotated_files() -> u32 {
    10
}

/// Minimum audit log file size: 1 MB.
pub const MIN_AUDIT_FILE_SIZE: u64 = 1024 * 1024;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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

// =========================================================================
// MetricsCardinalityConfig
// =========================================================================

/// Default warning threshold for metric cardinality per family.
const fn default_warn_cardinality() -> u32 {
    5000
}

/// Default maximum cardinality before metric observations are dropped.
const fn default_max_cardinality() -> u32 {
    10_000
}

/// Controls cardinality limits for Prometheus metrics.
///
/// Tracks distinct label combinations per metric family using HyperLogLog
/// estimation. When a metric family's estimated cardinality exceeds
/// `warn_cardinality`, a warning is emitted. When it exceeds
/// `max_cardinality`, new observations are dropped and an overflow
/// counter increments.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::MetricsCardinalityConfig;
/// let config = MetricsCardinalityConfig::builder()
///     .warn_cardinality(3000)
///     .max_cardinality(8000)
///     .build()
///     .expect("valid config");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MetricsCardinalityConfig {
    /// Emit a WARN log when estimated distinct label combinations reach this count.
    #[serde(default = "default_warn_cardinality")]
    pub warn_cardinality: u32,
    /// Drop metric observations when estimated cardinality exceeds this count.
    #[serde(default = "default_max_cardinality")]
    pub max_cardinality: u32,
}

impl Default for MetricsCardinalityConfig {
    fn default() -> Self {
        Self {
            warn_cardinality: default_warn_cardinality(),
            max_cardinality: default_max_cardinality(),
        }
    }
}

#[bon::bon]
impl MetricsCardinalityConfig {
    /// Creates a new cardinality config with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if `warn_cardinality >= max_cardinality`
    /// or either value is zero.
    #[builder]
    pub fn new(
        #[builder(default = default_warn_cardinality())] warn_cardinality: u32,
        #[builder(default = default_max_cardinality())] max_cardinality: u32,
    ) -> Result<Self, ConfigError> {
        let config = Self { warn_cardinality, max_cardinality };
        config.validate()?;
        Ok(config)
    }
}

impl MetricsCardinalityConfig {
    /// Validates an existing configuration (e.g., after deserialization).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if either value is zero or
    /// `warn_cardinality >= max_cardinality`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.warn_cardinality == 0 {
            return Err(ConfigError::Validation {
                message: "warn_cardinality must be > 0".to_string(),
            });
        }
        if self.max_cardinality == 0 {
            return Err(ConfigError::Validation {
                message: "max_cardinality must be > 0".to_string(),
            });
        }
        if self.warn_cardinality >= self.max_cardinality {
            return Err(ConfigError::Validation {
                message: format!(
                    "warn_cardinality ({}) must be less than max_cardinality ({})",
                    self.warn_cardinality, self.max_cardinality
                ),
            });
        }
        Ok(())
    }
}
