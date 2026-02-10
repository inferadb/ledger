use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    ConfigError,
    observability::{HotKeyConfig, MetricsCardinalityConfig},
    resilience::{RateLimitConfig, ValidationConfig},
    storage::{BTreeCompactionConfig, IntegrityConfig},
};

/// Default maximum storage bytes per namespace (10 GiB).
const fn default_max_storage_bytes() -> u64 {
    10 * 1024 * 1024 * 1024
}

/// Default maximum vault count per namespace.
const fn default_max_vaults() -> u32 {
    1000
}

/// Default maximum write operations per second per namespace.
const fn default_max_write_ops_per_sec() -> u32 {
    10_000
}

/// Default maximum read operations per second per namespace.
const fn default_max_read_ops_per_sec() -> u32 {
    50_000
}

/// Per-namespace resource quota configuration.
///
/// Enforces hard resource limits per namespace to prevent any single tenant
/// from exhausting shared infrastructure. Quotas are checked at the service
/// layer before operations reach the storage engine.
///
/// # Fields
///
/// - `max_storage_bytes`: Maximum cumulative storage bytes (estimated from payload sizes).
/// - `max_vaults`: Maximum number of vaults within the namespace.
/// - `max_write_ops_per_sec`: Maximum write operations per second (separate from global rate
///   limits).
/// - `max_read_ops_per_sec`: Maximum read operations per second (separate from global rate limits).
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::NamespaceQuota;
/// let quota = NamespaceQuota::builder()
///     .max_storage_bytes(1024 * 1024 * 1024) // 1 GiB
///     .max_vaults(100)
///     .build()
///     .expect("valid quota");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NamespaceQuota {
    /// Maximum cumulative storage bytes for the namespace.
    #[serde(default = "default_max_storage_bytes")]
    pub max_storage_bytes: u64,
    /// Maximum number of vaults within the namespace.
    #[serde(default = "default_max_vaults")]
    pub max_vaults: u32,
    /// Maximum write operations per second (namespace-level rate).
    #[serde(default = "default_max_write_ops_per_sec")]
    pub max_write_ops_per_sec: u32,
    /// Maximum read operations per second (namespace-level rate).
    #[serde(default = "default_max_read_ops_per_sec")]
    pub max_read_ops_per_sec: u32,
}

impl Default for NamespaceQuota {
    fn default() -> Self {
        Self {
            max_storage_bytes: default_max_storage_bytes(),
            max_vaults: default_max_vaults(),
            max_write_ops_per_sec: default_max_write_ops_per_sec(),
            max_read_ops_per_sec: default_max_read_ops_per_sec(),
        }
    }
}

#[bon::bon]
impl NamespaceQuota {
    /// Creates a new namespace quota with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is zero.
    #[builder]
    pub fn new(
        #[builder(default = default_max_storage_bytes())] max_storage_bytes: u64,
        #[builder(default = default_max_vaults())] max_vaults: u32,
        #[builder(default = default_max_write_ops_per_sec())] max_write_ops_per_sec: u32,
        #[builder(default = default_max_read_ops_per_sec())] max_read_ops_per_sec: u32,
    ) -> Result<Self, ConfigError> {
        let config =
            Self { max_storage_bytes, max_vaults, max_write_ops_per_sec, max_read_ops_per_sec };
        config.validate()?;
        Ok(config)
    }
}

impl NamespaceQuota {
    /// Validate an existing quota configuration (e.g., after deserialization).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_storage_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_storage_bytes must be > 0".to_string(),
            });
        }
        if self.max_vaults == 0 {
            return Err(ConfigError::Validation { message: "max_vaults must be > 0".to_string() });
        }
        if self.max_write_ops_per_sec == 0 {
            return Err(ConfigError::Validation {
                message: "max_write_ops_per_sec must be > 0".to_string(),
            });
        }
        if self.max_read_ops_per_sec == 0 {
            return Err(ConfigError::Validation {
                message: "max_read_ops_per_sec must be > 0".to_string(),
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema, bon::Builder)]
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
    /// Default namespace quota applied to new namespaces without explicit quotas.
    #[serde(default)]
    pub default_quota: Option<NamespaceQuota>,
    /// Integrity scrubber parameters.
    #[serde(default)]
    pub integrity: Option<IntegrityConfig>,
    /// Metric cardinality budgets. `None` disables cardinality tracking.
    #[serde(default)]
    pub metrics_cardinality: Option<MetricsCardinalityConfig>,
}

/// Identifies a non-reconfigurable parameter that was included in an update request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonReconfigurableField {
    /// The field name (e.g. "listen_addr", "data_dir").
    pub name: String,
    /// Human-readable reason why this field cannot be changed at runtime.
    pub reason: String,
}

/// A single field-level change detected during config diff.
///
/// Captures the full dotted path (e.g. "rate_limit.client_burst"),
/// the old value as a JSON string, and the new value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigChange {
    /// Dotted field path (e.g. "rate_limit.client_burst").
    pub field: String,
    /// Previous value serialized as JSON.
    pub old: String,
    /// New value serialized as JSON.
    pub new: String,
}

impl std::fmt::Display for ConfigChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} → {}", self.field, self.old, self.new)
    }
}

/// Recursively compare two JSON values and collect leaf-level differences.
fn collect_json_diffs(
    prefix: &str,
    old: &serde_json::Value,
    new: &serde_json::Value,
    out: &mut Vec<ConfigChange>,
) {
    match (old, new) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            // Check all keys in both maps.
            let mut all_keys: Vec<&String> = a.keys().chain(b.keys()).collect();
            all_keys.sort();
            all_keys.dedup();
            for key in all_keys {
                let path = if prefix.is_empty() { key.clone() } else { format!("{prefix}.{key}") };
                match (a.get(key), b.get(key)) {
                    (Some(av), Some(bv)) => collect_json_diffs(&path, av, bv, out),
                    (Some(av), None) => out.push(ConfigChange {
                        field: path,
                        old: av.to_string(),
                        new: "null".to_string(),
                    }),
                    (None, Some(bv)) => out.push(ConfigChange {
                        field: path,
                        old: "null".to_string(),
                        new: bv.to_string(),
                    }),
                    (None, None) => {},
                }
            }
        },
        _ => {
            if old != new {
                out.push(ConfigChange {
                    field: prefix.to_string(),
                    old: old.to_string(),
                    new: new.to_string(),
                });
            }
        },
    }
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
        if let Some(ref q) = self.default_quota {
            q.validate()?;
        }
        if let Some(ref i) = self.integrity {
            i.validate()?;
        }
        if let Some(ref mc) = self.metrics_cardinality {
            mc.validate()?;
        }
        Ok(())
    }

    /// Compute field-level differences between two runtime configs.
    ///
    /// Returns a list of [`ConfigChange`] with dotted field paths and
    /// old/new values serialized as JSON strings. Uses JSON-based recursive
    /// comparison so new fields automatically participate in diff reporting.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_types::config::RuntimeConfig;
    /// let old = RuntimeConfig::default();
    /// let new = RuntimeConfig::builder().build();
    /// let changes = old.detailed_diff(&new);
    /// for change in &changes {
    ///     println!("{change}");
    /// }
    /// ```
    #[must_use]
    pub fn detailed_diff(&self, other: &RuntimeConfig) -> Vec<ConfigChange> {
        // Serialize both to JSON values for recursive comparison.
        // Serialization cannot fail for these types (all fields are serde-compatible).
        let old_json = serde_json::to_value(self).unwrap_or_default();
        let new_json = serde_json::to_value(other).unwrap_or_default();
        let mut changes = Vec::new();
        collect_json_diffs("", &old_json, &new_json, &mut changes);
        changes
    }

    /// Compute the list of top-level section names that differ.
    ///
    /// Returns human-readable strings like `"rate_limit"` for backward
    /// compatibility with existing callers.
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
        if self.default_quota != other.default_quota {
            changes.push("default_quota".to_string());
        }
        if self.integrity != other.integrity {
            changes.push("integrity".to_string());
        }
        if self.metrics_cardinality != other.metrics_cardinality {
            changes.push("metrics_cardinality".to_string());
        }
        changes
    }
}
