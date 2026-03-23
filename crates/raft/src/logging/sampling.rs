//! Tail sampling for canonical log lines.
//!
//! Provides configurable tail sampling that reduces log volume while ensuring
//! errors, slow requests, and admin operations are never dropped.

use serde::Serialize;

/// Outcome of a request.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Request completed successfully.
    Success,
    /// Request failed with an error.
    Error {
        /// Error code (gRPC status or domain-specific).
        code: String,
        /// Human-readable error message.
        message: String,
    },
    /// Request returned a cached result (idempotency hit).
    Cached,
    /// Request was rate limited.
    RateLimited,
    /// Request failed due to precondition (CAS failure).
    PreconditionFailed {
        /// Key that failed the precondition.
        key: Option<String>,
    },
}

impl Outcome {
    /// Returns the outcome type as a string for logging.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error { .. } => "error",
            Self::Cached => "cached",
            Self::RateLimited => "rate_limited",
            Self::PreconditionFailed { .. } => "precondition_failed",
        }
    }
}

/// Operation type for sampling decisions.
///
/// Different operation types have different default sampling rates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// Read operations (lowest default sampling rate).
    Read,
    /// Write operations (medium default sampling rate).
    Write,
    /// Admin operations (always 100% sampled).
    Admin,
}

/// Configuration for log sampling.
///
/// Tail sampling samples events at emission time based on outcome and latency.
/// This ensures errors and slow requests are never dropped while reducing log
/// volume for healthy traffic.
///
/// # Example
///
/// ```no_run
/// use inferadb_ledger_raft::logging::SamplingConfig;
///
/// // Production: aggressive sampling
/// let config = SamplingConfig::builder()
///     .read_rate(0.01)   // 1% of successful reads
///     .write_rate(0.1)   // 10% of successful writes
///     .build();
///
/// // Development: no sampling
/// let config = SamplingConfig::disabled();
/// ```
#[derive(Debug, Clone, bon::Builder)]
pub struct SamplingConfig {
    /// Sample rate for error outcomes (0.0-1.0). Default: 1.0 (100%).
    #[builder(default = 1.0)]
    pub error_rate: f64,

    /// Sample rate for slow requests (0.0-1.0). Default: 1.0 (100%).
    #[builder(default = 1.0)]
    pub slow_rate: f64,

    /// Sample rate for VIP organizations (0.0-1.0). Default: 0.5 (50%).
    #[builder(default = 0.5)]
    pub vip_rate: f64,

    /// Sample rate for successful write operations (0.0-1.0). Default: 0.1 (10%).
    #[builder(default = 0.1)]
    pub write_rate: f64,

    /// Sample rate for successful read operations (0.0-1.0). Default: 0.01 (1%).
    #[builder(default = 0.01)]
    pub read_rate: f64,

    /// Threshold for slow read operations, in milliseconds. Default: 10.0.
    #[builder(default = 10.0)]
    pub slow_threshold_read_ms: f64,

    /// Threshold for slow write operations, in milliseconds. Default: 100.0.
    #[builder(default = 100.0)]
    pub slow_threshold_write_ms: f64,

    /// Threshold for slow admin operations, in milliseconds. Default: 1000.0.
    #[builder(default = 1000.0)]
    pub slow_threshold_admin_ms: f64,

    /// List of VIP organization IDs with elevated sampling.
    #[builder(default)]
    pub vip_organizations: Vec<u64>,

    /// Whether sampling is enabled. Default: true.
    #[builder(default = true)]
    pub enabled: bool,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl SamplingConfig {
    /// Creates a config with sampling disabled (100% sample rate for all events).
    pub fn disabled() -> Self {
        Self::builder().enabled(false).build()
    }

    /// Creates a config suitable for testing (no sampling, all events emitted).
    pub fn for_test() -> Self {
        Self::disabled()
    }
}

/// Sampler for canonical log lines using deterministic tail sampling.
///
/// Sampling decisions are made at event emission time based on:
/// 1. Outcome (errors always sampled at 100%)
/// 2. Latency (slow requests always sampled at 100%)
/// 3. Organization VIP status
/// 4. Operation type
///
/// The decision is deterministic: `seahash(request_id) % 10000 < (rate * 10000)`.
/// This ensures the same request_id always produces the same sampling decision.
#[derive(Debug, Clone)]
pub struct Sampler {
    config: SamplingConfig,
}

impl Default for Sampler {
    fn default() -> Self {
        Self::new(SamplingConfig::default())
    }
}

impl Sampler {
    /// Creates a new sampler with the given configuration.
    pub fn new(config: SamplingConfig) -> Self {
        Self { config }
    }

    /// Creates a sampler with sampling disabled.
    pub fn disabled() -> Self {
        Self::new(SamplingConfig::disabled())
    }

    /// Determines whether an event should be sampled (emitted).
    ///
    /// Returns true if the event should be emitted, false if it should be dropped.
    pub fn should_sample(
        &self,
        request_id: &uuid::Uuid,
        outcome: Option<&Outcome>,
        duration_ms: f64,
        operation_type: OperationType,
        organization: Option<u64>,
    ) -> bool {
        // If sampling is disabled, always emit
        if !self.config.enabled {
            return true;
        }

        // Rule 1: Errors are always sampled at 100%
        if matches!(outcome, Some(Outcome::Error { .. })) {
            return self.sample_at_rate(request_id, self.config.error_rate);
        }

        // Rule 2: Slow requests are always sampled at 100%
        let slow_threshold = match operation_type {
            OperationType::Read => self.config.slow_threshold_read_ms,
            OperationType::Write => self.config.slow_threshold_write_ms,
            OperationType::Admin => self.config.slow_threshold_admin_ms,
        };
        if duration_ms > slow_threshold {
            return self.sample_at_rate(request_id, self.config.slow_rate);
        }

        // Rule 3: Admin operations are always sampled at 100%
        if operation_type == OperationType::Admin {
            return true;
        }

        // Rule 4: VIP organizations get elevated sampling
        if organization.is_some_and(|org_id| self.config.vip_organizations.contains(&org_id)) {
            return self.sample_at_rate(request_id, self.config.vip_rate);
        }

        // Rule 5: Normal sampling based on operation type
        let rate = match operation_type {
            OperationType::Read => self.config.read_rate,
            OperationType::Write => self.config.write_rate,
            OperationType::Admin => 1.0, // Already handled above, but for completeness
        };

        self.sample_at_rate(request_id, rate)
    }

    /// Performs deterministic sampling using seahash.
    ///
    /// `seahash(request_id) % 10000 < (rate * 10000)`
    fn sample_at_rate(&self, request_id: &uuid::Uuid, rate: f64) -> bool {
        if rate >= 1.0 {
            return true;
        }
        if rate <= 0.0 {
            return false;
        }

        let hash = seahash::hash(request_id.as_bytes());
        let threshold = (rate * 10000.0) as u64;
        (hash % 10000) < threshold
    }
}
