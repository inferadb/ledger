//! Encryption at rest configuration.
//!
//! Controls envelope encryption for all persisted data (B+ tree pages,
//! snapshots, Raft log entries). Each artifact gets a unique Data
//! Encryption Key (DEK) encrypted by the Region Master Key (RMK).

use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ConfigError;

/// Minimum DEK cache capacity.
const MIN_DEK_CACHE_CAPACITY: usize = 64;

/// Maximum DEK cache capacity (256K entries ≈ 8 MB of cached keys).
const MAX_DEK_CACHE_CAPACITY: usize = 262_144;

/// Default DEK cache capacity (8,192 entries ≈ 256 KB).
const DEFAULT_DEK_CACHE_CAPACITY: usize = 8_192;

/// Encryption at rest configuration.
///
/// When enabled, all data written to disk uses envelope encryption:
/// each artifact (page, snapshot chunk, log entry) gets a random
/// Data Encryption Key (DEK) that encrypts the data. The DEK is
/// wrapped by the Region Master Key (RMK) and stored alongside
/// the artifact. RMK rotation never requires re-encrypting data —
/// only re-wrapping DEK headers.
///
/// # Validation Rules
///
/// - `dek_cache_capacity` must be in \[64, 262\_144\]
/// - When `enabled`, `key_source` must be specified
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::EncryptionConfig;
/// let config = EncryptionConfig::builder()
///     .enabled(true)
///     .key_source(inferadb_ledger_types::config::KeySource::Env("LEDGER_RMK".to_string()))
///     .build()
///     .expect("valid encryption config");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EncryptionConfig {
    /// Whether encryption at rest is enabled.
    ///
    /// When false, the `EncryptedBackend` passes through to the inner
    /// backend without any crypto overhead.
    #[serde(default)]
    pub enabled: bool,

    /// Source of the Region Master Key material.
    ///
    /// Required when `enabled` is true. Ignored when disabled.
    #[serde(default)]
    pub key_source: Option<KeySource>,

    /// Encryption algorithm for data encryption (DEK → data).
    ///
    /// Currently only AES-256-GCM is supported.
    #[serde(default)]
    pub algorithm: EncryptionAlgorithm,

    /// Maximum number of unwrapped DEKs cached in memory.
    ///
    /// Higher values reduce AES-KWP unwrap operations on cache misses
    /// at the cost of memory (32 bytes per cached DEK). Must be in
    /// \[64, 262\_144\]. Default: 8,192 entries (≈ 256 KB).
    #[serde(default = "default_dek_cache_capacity")]
    pub dek_cache_capacity: usize,

    /// Whether `mlock` failure is fatal at startup.
    ///
    /// When true (default), the process exits if `mlock` cannot pin
    /// key material in RAM (prevents swap exposure). Set to false
    /// only for development/testing environments.
    #[serde(default = "default_true")]
    pub strict_memory_protection: bool,
}

#[bon::bon]
impl EncryptionConfig {
    /// Creates a new encryption configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if enabled without key source,
    /// or if DEK cache capacity is out of range.
    #[builder]
    pub fn new(
        #[builder(default)] enabled: bool,
        key_source: Option<KeySource>,
        #[builder(default)] algorithm: EncryptionAlgorithm,
        #[builder(default = default_dek_cache_capacity())] dek_cache_capacity: usize,
        #[builder(default = true)] strict_memory_protection: bool,
    ) -> Result<Self, ConfigError> {
        let config =
            Self { enabled, key_source, algorithm, dek_cache_capacity, strict_memory_protection };
        config.validate()?;
        Ok(config)
    }
}

impl EncryptionConfig {
    /// Validates the encryption configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any field is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled && self.key_source.is_none() {
            return Err(ConfigError::Validation {
                message: "encryption enabled but no key_source specified".to_string(),
            });
        }
        if self.dek_cache_capacity < MIN_DEK_CACHE_CAPACITY {
            return Err(ConfigError::Validation {
                message: format!(
                    "dek_cache_capacity must be >= {MIN_DEK_CACHE_CAPACITY}, got {}",
                    self.dek_cache_capacity
                ),
            });
        }
        if self.dek_cache_capacity > MAX_DEK_CACHE_CAPACITY {
            return Err(ConfigError::Validation {
                message: format!(
                    "dek_cache_capacity must be <= {MAX_DEK_CACHE_CAPACITY}, got {}",
                    self.dek_cache_capacity
                ),
            });
        }
        Ok(())
    }
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            key_source: None,
            algorithm: EncryptionAlgorithm::default(),
            dek_cache_capacity: DEFAULT_DEK_CACHE_CAPACITY,
            strict_memory_protection: true,
        }
    }
}

/// Source of Region Master Key material.
///
/// The RMK is used to wrap/unwrap per-artifact DEKs. Multiple sources
/// are supported for different deployment environments.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "value")]
pub enum KeySource {
    /// Load RMK from an environment variable (hex-encoded 32 bytes).
    ///
    /// Suitable for container deployments where secrets are injected
    /// via environment variables.
    Env(String),

    /// Load RMK from a file path (raw 32 bytes).
    ///
    /// Suitable for local development and testing. Production
    /// deployments should prefer `Env` with secrets manager injection.
    File(PathBuf),
}

/// Data encryption algorithm for DEK → data encryption.
///
/// Determines the symmetric cipher used to encrypt page content,
/// snapshot chunks, and log entry payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM (AEAD with 12-byte nonce, 16-byte auth tag).
    ///
    /// Industry-standard authenticated encryption. Hardware-accelerated
    /// via AES-NI on modern x86 processors.
    #[default]
    Aes256Gcm,
}

/// Default re-wrapping batch size (pages per iteration).
const DEFAULT_REWRAP_BATCH_SIZE: usize = 1_000;

/// Maximum re-wrapping batch size.
const MAX_REWRAP_BATCH_SIZE: usize = 50_000;

/// Default check interval for the re-wrapping job (seconds).
const DEFAULT_REWRAP_INTERVAL_SECS: u64 = 300;

/// Configuration for the background DEK re-wrapping job.
///
/// After RMK rotation, old artifacts still have DEKs wrapped by
/// the previous version. The re-wrapping job iterates all artifacts,
/// unwraps each DEK with the old RMK, re-wraps with the new RMK,
/// and updates the sidecar metadata. The encrypted body is
/// untouched — only the wrapping header changes.
///
/// # Validation Rules
///
/// - `batch_size` must be in \[1, 50\_000\]
/// - `interval_secs` must be > 0
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RewrapConfig {
    /// Whether the re-wrapping job is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Pages processed per iteration.
    ///
    /// Higher values re-wrap faster but generate more I/O load.
    #[serde(default = "default_rewrap_batch_size")]
    pub batch_size: usize,

    /// Seconds between re-wrapping check cycles.
    #[serde(default = "default_rewrap_interval_secs")]
    pub interval_secs: u64,

    /// Target RMK version to re-wrap to.
    ///
    /// If `None`, re-wraps to the current latest version.
    #[serde(default)]
    pub target_rmk_version: Option<u32>,
}

#[bon::bon]
impl RewrapConfig {
    /// Creates a new re-wrapping configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if batch size or interval is invalid.
    #[builder]
    pub fn new(
        #[builder(default = true)] enabled: bool,
        #[builder(default = DEFAULT_REWRAP_BATCH_SIZE)] batch_size: usize,
        #[builder(default = DEFAULT_REWRAP_INTERVAL_SECS)] interval_secs: u64,
        target_rmk_version: Option<u32>,
    ) -> Result<Self, ConfigError> {
        let config = Self { enabled, batch_size, interval_secs, target_rmk_version };
        config.validate()?;
        Ok(config)
    }
}

impl RewrapConfig {
    /// Validates the re-wrapping configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if batch size or interval is invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.batch_size == 0 || self.batch_size > MAX_REWRAP_BATCH_SIZE {
            return Err(ConfigError::Validation {
                message: format!(
                    "batch_size must be in [1, {MAX_REWRAP_BATCH_SIZE}], got {}",
                    self.batch_size
                ),
            });
        }
        if self.interval_secs == 0 {
            return Err(ConfigError::Validation {
                message: "interval_secs must be > 0".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for RewrapConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_size: DEFAULT_REWRAP_BATCH_SIZE,
            interval_secs: DEFAULT_REWRAP_INTERVAL_SECS,
            target_rmk_version: None,
        }
    }
}

fn default_dek_cache_capacity() -> usize {
    DEFAULT_DEK_CACHE_CAPACITY
}

fn default_true() -> bool {
    true
}

fn default_rewrap_batch_size() -> usize {
    DEFAULT_REWRAP_BATCH_SIZE
}

fn default_rewrap_interval_secs() -> u64 {
    DEFAULT_REWRAP_INTERVAL_SECS
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_disabled() {
        let config = EncryptionConfig::default();
        assert!(!config.enabled);
        assert!(config.key_source.is_none());
        assert_eq!(config.dek_cache_capacity, 8192);
        assert!(config.strict_memory_protection);
    }

    #[test]
    fn test_enabled_without_key_source_fails_validation() {
        let err = EncryptionConfig::builder().enabled(true).build().unwrap_err();
        assert!(err.to_string().contains("key_source"));
    }

    #[test]
    fn test_enabled_with_key_source_passes_validation() {
        let config = EncryptionConfig::builder()
            .enabled(true)
            .key_source(KeySource::Env("RMK".to_string()))
            .build()
            .unwrap();
        assert!(config.enabled);
    }

    #[test]
    fn test_disabled_without_key_source_passes_validation() {
        let config = EncryptionConfig { enabled: false, key_source: None, ..Default::default() };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_dek_cache_too_small() {
        let config = EncryptionConfig { dek_cache_capacity: 10, ..Default::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_dek_cache_too_large() {
        let config = EncryptionConfig { dek_cache_capacity: 500_000, ..Default::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let config = EncryptionConfig::builder()
            .enabled(true)
            .key_source(KeySource::File("/tmp/rmk.key".into()))
            .dek_cache_capacity(4096_usize)
            .build()
            .unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let restored: EncryptionConfig = serde_json::from_str(&json).unwrap();
        assert!(restored.enabled);
        assert_eq!(restored.dek_cache_capacity, 4096);
    }

    // --- RewrapConfig tests ---

    #[test]
    fn test_rewrap_config_defaults() {
        let config = RewrapConfig::default();
        assert!(config.enabled);
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.interval_secs, 300);
        assert!(config.target_rmk_version.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_rewrap_config_builder() {
        let config = RewrapConfig::builder()
            .batch_size(500_usize)
            .interval_secs(60_u64)
            .target_rmk_version(2_u32)
            .build()
            .unwrap();
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.interval_secs, 60);
        assert_eq!(config.target_rmk_version, Some(2));
    }

    #[test]
    fn test_rewrap_config_zero_batch_size_fails() {
        let config = RewrapConfig { batch_size: 0, ..Default::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_rewrap_config_batch_size_too_large() {
        let config = RewrapConfig { batch_size: 100_000, ..Default::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_rewrap_config_zero_interval_fails() {
        let config = RewrapConfig { interval_secs: 0, ..Default::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_rewrap_config_serialization() {
        let config = RewrapConfig::builder().batch_size(2000_usize).build().unwrap();
        let json = serde_json::to_string(&config).unwrap();
        let restored: RewrapConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.batch_size, 2000);
    }
}
