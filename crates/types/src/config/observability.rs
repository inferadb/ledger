//! Hot key detection, metrics cardinality, and OpenTelemetry configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ConfigError;

// =========================================================================
// OtelTransport
// =========================================================================

/// Transport protocol for OTLP trace export.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OtelTransport {
    /// gRPC transport (default, recommended for high-throughput).
    #[default]
    Grpc,
    /// HTTP transport (for environments where gRPC is blocked).
    Http,
}

// =========================================================================
// OtelConfig
// =========================================================================

/// Default batch size for span export (512 spans).
const fn default_otel_batch_size() -> usize {
    512
}

/// Default batch interval in milliseconds (5000ms).
const fn default_otel_batch_interval_ms() -> u64 {
    5000
}

/// Default export timeout in milliseconds (10000ms).
const fn default_otel_timeout_ms() -> u64 {
    10000
}

/// Default shutdown timeout in milliseconds (15000ms).
const fn default_otel_shutdown_timeout_ms() -> u64 {
    15000
}

/// Default trace Raft RPCs setting (true).
const fn default_trace_raft_rpcs() -> bool {
    true
}

/// Configuration for OpenTelemetry/OTLP trace export.
///
/// Enables exporting request logs as OpenTelemetry traces to observability
/// backends such as Jaeger, Honeycomb, or Datadog via the OTLP protocol.
///
/// # Environment Variables
///
/// ```bash
/// INFERADB__LEDGER__LOGGING__OTEL__ENABLED=true
/// INFERADB__LEDGER__LOGGING__OTEL__ENDPOINT=http://localhost:4317
/// INFERADB__LEDGER__LOGGING__OTEL__TRANSPORT=grpc
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct OtelConfig {
    /// Whether OTLP export is enabled. Default: false.
    #[serde(default)]
    pub enabled: bool,

    /// OTLP endpoint URL (e.g., "http://localhost:4317" for gRPC).
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Transport protocol. Default: gRPC.
    #[serde(default)]
    pub transport: OtelTransport,

    /// Batch size (flush when reached). Default: 512 spans.
    #[serde(default = "default_otel_batch_size")]
    pub batch_size: usize,

    /// Batch interval in milliseconds (flush when elapsed). Default: 5000ms.
    #[serde(default = "default_otel_batch_interval_ms")]
    pub batch_interval_ms: u64,

    /// Export timeout in milliseconds. Default: 10000ms.
    #[serde(default = "default_otel_timeout_ms")]
    pub timeout_ms: u64,

    /// Graceful shutdown timeout in milliseconds. Default: 15000ms.
    #[serde(default = "default_otel_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,

    /// Whether to propagate trace context in Raft RPCs. Default: true.
    ///
    /// When enabled, trace context is injected into AppendEntries, Vote, and
    /// InstallSnapshot RPCs, enabling end-to-end distributed tracing across
    /// the Raft cluster. Disable for performance-critical deployments where
    /// the ~100 bytes overhead per RPC is unacceptable.
    #[serde(default = "default_trace_raft_rpcs")]
    pub trace_raft_rpcs: bool,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            transport: OtelTransport::default(),
            batch_size: default_otel_batch_size(),
            batch_interval_ms: default_otel_batch_interval_ms(),
            timeout_ms: default_otel_timeout_ms(),
            shutdown_timeout_ms: default_otel_shutdown_timeout_ms(),
            trace_raft_rpcs: default_trace_raft_rpcs(),
        }
    }
}

#[bon::bon]
impl OtelConfig {
    /// Creates a new OTEL configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if enabled without endpoint,
    /// or if batch/timeout values are zero.
    #[builder]
    pub fn new(
        #[builder(default)] enabled: bool,
        #[builder(into)] endpoint: Option<String>,
        #[builder(default)] transport: OtelTransport,
        #[builder(default = default_otel_batch_size())] batch_size: usize,
        #[builder(default = default_otel_batch_interval_ms())] batch_interval_ms: u64,
        #[builder(default = default_otel_timeout_ms())] timeout_ms: u64,
        #[builder(default = default_otel_shutdown_timeout_ms())] shutdown_timeout_ms: u64,
        #[builder(default = default_trace_raft_rpcs())] trace_raft_rpcs: bool,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            enabled,
            endpoint,
            transport,
            batch_size,
            batch_interval_ms,
            timeout_ms,
            shutdown_timeout_ms,
            trace_raft_rpcs,
        };
        config.validate()?;
        Ok(config)
    }
}

impl OtelConfig {
    /// Returns `true` if gRPC transport is configured.
    pub fn use_grpc(&self) -> bool {
        matches!(self.transport, OtelTransport::Grpc)
    }

    /// Returns the endpoint URL, or an empty string if not set.
    pub fn endpoint_or_default(&self) -> &str {
        self.endpoint.as_deref().unwrap_or("")
    }

    /// Creates a configuration with test-suitable values (OTEL disabled).
    pub fn for_test() -> Self {
        Self::default()
    }

    /// Validates OTEL configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if OTEL is enabled without an endpoint,
    /// or if batch/timeout values are zero.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled && self.endpoint.is_none() {
            return Err(ConfigError::Validation {
                message: "logging.otel.endpoint is required when OTEL is enabled".to_string(),
            });
        }

        if self.batch_size == 0 {
            return Err(ConfigError::Validation {
                message: "logging.otel.batch_size must be positive".to_string(),
            });
        }

        if self.batch_interval_ms == 0 {
            return Err(ConfigError::Validation {
                message: "logging.otel.batch_interval_ms must be positive".to_string(),
            });
        }

        if self.timeout_ms == 0 {
            return Err(ConfigError::Validation {
                message: "logging.otel.timeout_ms must be positive".to_string(),
            });
        }

        if self.shutdown_timeout_ms == 0 {
            return Err(ConfigError::Validation {
                message: "logging.otel.shutdown_timeout_ms must be positive".to_string(),
            });
        }

        Ok(())
    }
}

// =========================================================================
// DogStatsdConfig
// =========================================================================

/// Default value for `use_distributions` (true).
const fn default_use_distributions() -> bool {
    true
}

/// DogStatsD exporter configuration.
///
/// When enabled, histograms are emitted as Datadog distributions in parallel
/// with the Prometheus exporter. Distributions are 1 custom metric per
/// `(name, tag_set)` vs. 8 for legacy histograms — an 8× cost reduction.
///
/// # Single-recorder constraint
///
/// The `metrics` crate permits only one global recorder. When this is enabled,
/// the DogStatsD recorder replaces the Prometheus recorder. Deploy a Prometheus
/// sidecar (e.g. `dogstatsd_exporter`) if you need both simultaneously, or use
/// the OTLP pipeline to forward traces to Datadog alongside DogStatsD metrics.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::DogStatsdConfig;
/// let config = DogStatsdConfig::default();
/// assert_eq!(config.endpoint, None);
/// assert!(config.use_distributions);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DogStatsdConfig {
    /// UDP endpoint for the DogStatsD agent (e.g., `"127.0.0.1:8125"`).
    ///
    /// When `None`, the exporter is disabled and no recorder is installed.
    /// The default DogStatsD port is 8125.
    pub endpoint: Option<std::net::SocketAddr>,

    /// Emit histograms as Datadog distributions (default: `true`).
    ///
    /// Distributions are computed server-side by the Datadog agent and cost
    /// 1 custom metric per `(name, tag_set)` instead of 8 for legacy
    /// histogram buckets.
    #[serde(default = "default_use_distributions")]
    pub use_distributions: bool,
}

impl Default for DogStatsdConfig {
    fn default() -> Self {
        Self { endpoint: None, use_distributions: default_use_distributions() }
    }
}

// =========================================================================
// ObservabilityConfig
// =========================================================================

/// Default value for `vault_metrics_enabled` (false — cardinality control).
const fn default_vault_metrics_enabled() -> bool {
    false
}

/// Per-vault and per-organization metric emission controls.
///
/// Per-vault metrics are opt-in by default because every vault adds a fresh
/// Prometheus label set, multiplying series count and time-series database
/// cost. Operators can opt in for environments where the cardinality is
/// acceptable (e.g. staging or small clusters) and stay rolled-up at the
/// organization level otherwise. The org-level rollups are always-on.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::ObservabilityConfig;
/// let config = ObservabilityConfig::builder()
///     .vault_metrics_enabled(true)
///     .build();
/// assert!(config.vault_metrics_enabled);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, bon::Builder)]
pub struct ObservabilityConfig {
    /// Emit per-vault Prometheus series. Default: `false`.
    ///
    /// When `false`, per-vault metrics are suppressed entirely; only
    /// organization-level rollups (always-on) are emitted. This keeps the
    /// label cardinality bounded by `(region × organization)` rather than
    /// `(region × organization × vault)`.
    ///
    /// When `true`, every per-vault metric gains a `vault_id` label,
    /// multiplying the time-series count by the number of vaults per
    /// organization. Use only when the operator is willing to budget the
    /// extra cardinality.
    #[serde(default = "default_vault_metrics_enabled")]
    #[builder(default = default_vault_metrics_enabled())]
    pub vault_metrics_enabled: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self { vault_metrics_enabled: default_vault_metrics_enabled() }
    }
}

impl ObservabilityConfig {
    /// Resolves the effective `vault_metrics_enabled` value from a CLI
    /// override and the configured inner field.
    ///
    /// Mirrors [`RateLimitConfig::resolve_enabled`](super::RateLimitConfig::resolve_enabled).
    /// The inner field is the canonical source of truth; the CLI override
    /// (`--vault-metrics`) lets operators flip the per-vault Prometheus
    /// emission gate without a YAML edit. Used at server startup;
    /// per-vault metrics are intentionally restart-only — flipping
    /// the gate at runtime would change the time-series shape mid-scrape
    /// and confuse dashboards.
    #[inline]
    #[must_use]
    pub fn resolve_enabled(cli_override: Option<bool>, inner: bool) -> bool {
        cli_override.unwrap_or(inner)
    }
}

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
    /// Emits a WARN log when estimated distinct label combinations reach this count.
    #[serde(default = "default_warn_cardinality")]
    pub warn_cardinality: u32,
    /// Drops metric observations when estimated cardinality exceeds this count.
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    // OtelConfig tests

    #[test]
    fn otel_config_enabled_without_endpoint_fails() {
        let result = OtelConfig::builder().enabled(true).build();
        assert!(result.is_err());
    }

    #[test]
    fn otel_config_enabled_with_endpoint_succeeds() {
        let config = OtelConfig::builder()
            .enabled(true)
            .endpoint("http://localhost:4317".to_string())
            .build()
            .unwrap();
        assert!(config.enabled);
        assert_eq!(config.endpoint.as_deref(), Some("http://localhost:4317"));
    }

    #[test]
    fn otel_config_zero_batch_size_fails() {
        let mut config = OtelConfig::default();
        config.batch_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn otel_config_zero_batch_interval_fails() {
        let mut config = OtelConfig::default();
        config.batch_interval_ms = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn otel_config_zero_timeout_fails() {
        let mut config = OtelConfig::default();
        config.timeout_ms = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn otel_config_zero_shutdown_timeout_fails() {
        let mut config = OtelConfig::default();
        config.shutdown_timeout_ms = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn otel_config_use_grpc() {
        let config = OtelConfig::default();
        assert!(config.use_grpc());
    }

    #[test]
    fn otel_config_endpoint_or_default_none() {
        let config = OtelConfig::default();
        assert_eq!(config.endpoint_or_default(), "");
    }

    #[test]
    fn otel_config_endpoint_or_default_some() {
        let mut config = OtelConfig::default();
        config.endpoint = Some("http://example.com".to_string());
        assert_eq!(config.endpoint_or_default(), "http://example.com");
    }

    #[test]
    fn otel_config_for_test() {
        let config = OtelConfig::for_test();
        assert!(!config.enabled);
    }

    // ObservabilityConfig tests

    #[test]
    fn observability_config_defaults_off() {
        let config = ObservabilityConfig::default();
        assert!(!config.vault_metrics_enabled);
    }

    #[test]
    fn observability_config_builder_opts_in() {
        let config = ObservabilityConfig::builder().vault_metrics_enabled(true).build();
        assert!(config.vault_metrics_enabled);
    }

    #[test]
    fn observability_config_serde_roundtrip_default() {
        let config = ObservabilityConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let decoded: ObservabilityConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, decoded);
    }

    #[test]
    fn observability_config_serde_omits_default_field() {
        // Empty JSON should deserialize to default (vault_metrics_enabled = false).
        let config: ObservabilityConfig = serde_json::from_str("{}").unwrap();
        assert!(!config.vault_metrics_enabled);
    }

    #[test]
    fn observability_resolve_enabled_returns_inner_when_cli_unset() {
        assert!(!ObservabilityConfig::resolve_enabled(None, false));
        assert!(ObservabilityConfig::resolve_enabled(None, true));
    }

    #[test]
    fn observability_resolve_enabled_cli_overrides_inner() {
        assert!(ObservabilityConfig::resolve_enabled(Some(true), false));
        assert!(!ObservabilityConfig::resolve_enabled(Some(false), true));
    }

    // HotKeyConfig tests

    #[test]
    fn hot_key_config_zero_window_fails() {
        let mut config = HotKeyConfig::default();
        config.window_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn hot_key_config_zero_threshold_fails() {
        let mut config = HotKeyConfig::default();
        config.threshold = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn hot_key_config_small_cms_width_fails() {
        let mut config = HotKeyConfig::default();
        config.cms_width = 63;
        assert!(config.validate().is_err());
    }

    #[test]
    fn hot_key_config_small_cms_depth_fails() {
        let mut config = HotKeyConfig::default();
        config.cms_depth = 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn hot_key_config_zero_top_k_fails() {
        let mut config = HotKeyConfig::default();
        config.top_k = 0;
        assert!(config.validate().is_err());
    }

    // MetricsCardinalityConfig tests

    #[test]
    fn metrics_cardinality_zero_warn_fails() {
        let mut config = MetricsCardinalityConfig::default();
        config.warn_cardinality = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn metrics_cardinality_zero_max_fails() {
        let mut config = MetricsCardinalityConfig::default();
        config.max_cardinality = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn metrics_cardinality_warn_ge_max_fails() {
        let mut config = MetricsCardinalityConfig::default();
        config.warn_cardinality = 10_000;
        config.max_cardinality = 10_000;
        assert!(config.validate().is_err());
    }

    #[test]
    fn metrics_cardinality_warn_exceeds_max_fails() {
        let mut config = MetricsCardinalityConfig::default();
        config.warn_cardinality = 15_000;
        config.max_cardinality = 10_000;
        assert!(config.validate().is_err());
    }
}
