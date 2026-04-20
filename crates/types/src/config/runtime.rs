//! Runtime-reconfigurable configuration and field-level change tracking.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    ConfigError,
    jwt::JwtConfig,
    observability::{HotKeyConfig, MetricsCardinalityConfig},
    resilience::{RateLimitConfig, ValidationConfig},
    storage::{BTreeCompactionConfig, CheckpointConfig, EventWriterBatchConfig, IntegrityConfig},
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
    /// State-DB checkpoint scheduling thresholds (time, applies, dirty pages).
    #[serde(default)]
    pub state_checkpoint: Option<CheckpointConfig>,
    /// Handler-phase event batching parameters.
    ///
    /// Controls the background `EventFlusher` that amortizes `events.db`
    /// fsyncs for handler-phase emissions. `queue_capacity` is
    /// restart-only; all other fields take effect on the next flush cycle.
    #[serde(default)]
    pub event_writer_batch: Option<EventWriterBatchConfig>,
    /// Metric cardinality budgets. `None` disables cardinality tracking.
    #[serde(default)]
    pub metrics_cardinality: Option<MetricsCardinalityConfig>,
    /// Event logging configuration (hot-reloadable subset).
    #[serde(default)]
    pub events: Option<RuntimeEventsConfig>,
    /// JWT token configuration (TTLs, clock skew, issuer).
    #[serde(default)]
    pub jwt: Option<JwtConfig>,
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
        if let Some(ref cp) = self.state_checkpoint {
            cp.validate()?;
        }
        if let Some(ref eb) = self.event_writer_batch {
            eb.validate()?;
        }
        if let Some(ref mc) = self.metrics_cardinality {
            mc.validate()?;
        }
        if let Some(ref ev) = self.events {
            ev.validate()?;
        }
        if let Some(ref j) = self.jwt {
            j.validate()?;
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
        // Serialization is infallible: all fields are primitive types (integers, bools, enums).
        #[expect(
            clippy::expect_used,
            reason = "infallible: RuntimeConfig fields are all serde-primitive"
        )]
        let old_json = serde_json::to_value(self).expect("RuntimeConfig serialization");
        #[expect(
            clippy::expect_used,
            reason = "infallible: RuntimeConfig fields are all serde-primitive"
        )]
        let new_json = serde_json::to_value(other).expect("RuntimeConfig serialization");
        let mut changes = Vec::new();
        collect_json_diffs("", &old_json, &new_json, &mut changes);
        changes
    }

    /// Computes the list of top-level section names that differ.
    ///
    /// Derives section names from [`detailed_diff`](Self::detailed_diff) so
    /// new fields are automatically included without manual enumeration.
    #[must_use]
    pub fn diff(&self, other: &RuntimeConfig) -> Vec<String> {
        let changes = self.detailed_diff(other);
        let sections: Vec<String> = changes
            .iter()
            .filter_map(|c| c.field.split('.').next().map(String::from))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        sections
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── RuntimeEventsConfig::validate ───────────────────────────────

    #[test]
    fn events_validate_accepts_valid_config() {
        let cfg = RuntimeEventsConfig {
            default_ttl_days: Some(30),
            ingest_rate_limit_per_source: Some(100),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn events_validate_rejects_ttl_out_of_range() {
        let cfg = RuntimeEventsConfig { default_ttl_days: Some(0), ..Default::default() };
        let err = cfg.validate().expect_err("should fail");
        assert!(err.to_string().contains("default_ttl_days"));

        let cfg = RuntimeEventsConfig { default_ttl_days: Some(3651), ..Default::default() };
        let err = cfg.validate().expect_err("should fail");
        assert!(err.to_string().contains("default_ttl_days"));
    }

    #[test]
    fn events_validate_rejects_zero_rate_limit() {
        let cfg =
            RuntimeEventsConfig { ingest_rate_limit_per_source: Some(0), ..Default::default() };
        let err = cfg.validate().expect_err("should fail");
        assert!(err.to_string().contains("ingest_rate_limit_per_source"));
    }

    #[test]
    fn events_validate_accepts_none_fields() {
        let cfg = RuntimeEventsConfig::default();
        assert!(cfg.validate().is_ok());
    }

    // ── RuntimeConfig::validate ─────────────────────────────────────

    #[test]
    fn runtime_config_validate_propagates_events_error() {
        let cfg = RuntimeConfig {
            events: Some(RuntimeEventsConfig { default_ttl_days: Some(0), ..Default::default() }),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn runtime_config_validate_empty_is_ok() {
        let cfg = RuntimeConfig::default();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn runtime_config_validate_propagates_state_checkpoint_error() {
        let bad = CheckpointConfig { interval_ms: 10, ..CheckpointConfig::default() };
        let cfg = RuntimeConfig { state_checkpoint: Some(bad), ..Default::default() };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn runtime_config_state_checkpoint_diff_detects_addition() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig {
            state_checkpoint: Some(CheckpointConfig::default()),
            ..Default::default()
        };
        let sections = a.diff(&b);
        assert!(sections.contains(&"state_checkpoint".to_string()));
    }

    // ── event_writer_batch passthrough ─────────────────────────────

    #[test]
    fn runtime_config_validate_propagates_event_writer_batch_error() {
        let bad =
            EventWriterBatchConfig { flush_interval_ms: 0, ..EventWriterBatchConfig::default() };
        let cfg = RuntimeConfig { event_writer_batch: Some(bad), ..Default::default() };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn runtime_config_event_writer_batch_diff_detects_addition() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig {
            event_writer_batch: Some(EventWriterBatchConfig::default()),
            ..Default::default()
        };
        let sections = a.diff(&b);
        assert!(sections.contains(&"event_writer_batch".to_string()));
    }

    #[test]
    fn runtime_config_event_writer_batch_detailed_diff_detects_field() {
        let a = RuntimeConfig {
            event_writer_batch: Some(EventWriterBatchConfig::default()),
            ..Default::default()
        };
        let b = RuntimeConfig {
            event_writer_batch: Some(EventWriterBatchConfig {
                flush_interval_ms: 250,
                ..EventWriterBatchConfig::default()
            }),
            ..Default::default()
        };
        let changes = a.detailed_diff(&b);
        assert!(changes.iter().any(|c| c.field == "event_writer_batch.flush_interval_ms"));
    }

    #[test]
    fn runtime_config_state_checkpoint_detailed_diff_detects_field() {
        let a = RuntimeConfig {
            state_checkpoint: Some(CheckpointConfig::default()),
            ..Default::default()
        };
        let b = RuntimeConfig {
            state_checkpoint: Some(CheckpointConfig {
                interval_ms: 1000,
                ..CheckpointConfig::default()
            }),
            ..Default::default()
        };
        let changes = a.detailed_diff(&b);
        assert!(changes.iter().any(|c| c.field == "state_checkpoint.interval_ms"));
    }

    // ── ConfigChange Display ────────────────────────────────────────

    #[test]
    fn config_change_display() {
        let change = ConfigChange {
            field: "rate_limit.client_burst".to_string(),
            old: "100".to_string(),
            new: "200".to_string(),
        };
        assert_eq!(change.to_string(), "rate_limit.client_burst: 100 → 200");
    }

    // ── detailed_diff ───────────────────────────────────────────────

    #[test]
    fn detailed_diff_no_changes_for_identical_configs() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig::default();
        assert!(a.detailed_diff(&b).is_empty());
    }

    #[test]
    fn detailed_diff_detects_events_change() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig {
            events: Some(RuntimeEventsConfig { enabled: Some(true), ..Default::default() }),
            ..Default::default()
        };
        let changes = a.detailed_diff(&b);
        assert!(!changes.is_empty());
        assert!(changes.iter().any(|c| c.field.contains("events")));
    }

    // ── diff ────────────────────────────────────────────────────────

    #[test]
    fn diff_returns_top_level_section_names() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig {
            events: Some(RuntimeEventsConfig { enabled: Some(true), ..Default::default() }),
            ..Default::default()
        };
        let sections = a.diff(&b);
        assert!(sections.contains(&"events".to_string()));
    }

    #[test]
    fn diff_empty_for_identical() {
        let a = RuntimeConfig::default();
        let b = RuntimeConfig::default();
        assert!(a.diff(&b).is_empty());
    }

    // ── collect_json_diffs ──────────────────────────────────────────

    #[test]
    fn collect_json_diffs_nested_objects() {
        let old: serde_json::Value = serde_json::json!({"a": {"b": 1, "c": 2}});
        let new: serde_json::Value = serde_json::json!({"a": {"b": 1, "c": 3}});
        let mut out = Vec::new();
        collect_json_diffs("", &old, &new, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].field, "a.c");
        assert_eq!(out[0].old, "2");
        assert_eq!(out[0].new, "3");
    }

    #[test]
    fn collect_json_diffs_addition_and_removal() {
        let old: serde_json::Value = serde_json::json!({"x": 1});
        let new: serde_json::Value = serde_json::json!({"y": 2});
        let mut out = Vec::new();
        collect_json_diffs("", &old, &new, &mut out);
        assert_eq!(out.len(), 2);
        let fields: Vec<&str> = out.iter().map(|c| c.field.as_str()).collect();
        assert!(fields.contains(&"x"));
        assert!(fields.contains(&"y"));
    }

    #[test]
    fn collect_json_diffs_equal_values_produce_nothing() {
        let v: serde_json::Value = serde_json::json!({"a": 1});
        let mut out = Vec::new();
        collect_json_diffs("", &v, &v, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn collect_json_diffs_scalar_change() {
        let old = serde_json::json!(42);
        let new = serde_json::json!(99);
        let mut out = Vec::new();
        collect_json_diffs("top", &old, &new, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].field, "top");
    }
}
