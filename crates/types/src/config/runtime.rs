//! Runtime-reconfigurable configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    ConfigError,
    observability::{HotKeyConfig, MetricsCardinalityConfig},
    resilience::{RateLimitConfig, ValidationConfig},
    storage::{BTreeCompactionConfig, IntegrityConfig},
};

/// Runtime-reconfigurable configuration subset.
///
/// Contains only parameters that can be safely changed without a server restart.
/// Designed for lock-free reads on every RPC.
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
    /// Integrity scrubber parameters.
    #[serde(default)]
    pub integrity: Option<IntegrityConfig>,
    /// Metric cardinality budgets. `None` disables cardinality tracking.
    #[serde(default)]
    pub metrics_cardinality: Option<MetricsCardinalityConfig>,
    /// Event logging configuration (hot-reloadable subset).
    #[serde(default)]
    pub events: Option<RuntimeEventsConfig>,
}

/// Runtime-reconfigurable subset of event logging configuration.
///
/// Contains only the event parameters that can be safely changed without
/// restart. Structural parameters (`max_snapshot_events`, `max_details_size_bytes`,
/// `allowed_sources`, `max_ingest_batch_size`) require restart.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RuntimeEventsConfig {
    /// Master switch for event logging.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Default TTL in days for new event entries.
    #[serde(default)]
    pub default_ttl_days: Option<u32>,
    /// Enable system-level event logging.
    #[serde(default)]
    pub system_log_enabled: Option<bool>,
    /// Enable organization-level event logging.
    #[serde(default)]
    pub organization_log_enabled: Option<bool>,
    /// Master switch for external event ingestion.
    #[serde(default)]
    pub ingest_enabled: Option<bool>,
    /// Events per second per source service.
    #[serde(default)]
    pub ingest_rate_limit_per_source: Option<u32>,
}

impl RuntimeEventsConfig {
    /// Validates the runtime events configuration values.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(ttl) = self.default_ttl_days
            && !(1..=3650).contains(&ttl)
        {
            return Err(ConfigError::Validation {
                message: format!("events.default_ttl_days must be 1..=3650, got {ttl}"),
            });
        }
        if let Some(rate) = self.ingest_rate_limit_per_source
            && rate == 0
        {
            return Err(ConfigError::Validation {
                message: "events.ingest_rate_limit_per_source must be at least 1".to_string(),
            });
        }
        Ok(())
    }
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

/// Collects leaf-level differences between two JSON values recursively.
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
    /// Validates all present config sections.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] with the first subsection
    /// validation failure encountered.
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
        if let Some(ref i) = self.integrity {
            i.validate()?;
        }
        if let Some(ref mc) = self.metrics_cardinality {
            mc.validate()?;
        }
        if let Some(ref ev) = self.events {
            ev.validate()?;
        }
        Ok(())
    }

    /// Computes field-level differences between two runtime configs.
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

    /// Computes the list of top-level section names that differ.
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
        if self.integrity != other.integrity {
            changes.push("integrity".to_string());
        }
        if self.metrics_cardinality != other.metrics_cardinality {
            changes.push("metrics_cardinality".to_string());
        }
        if self.events != other.events {
            changes.push("events".to_string());
        }
        changes
    }
}
