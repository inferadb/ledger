//! Rate limiting, shutdown, health check, and validation configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::ConfigError;

// =============================================================================
// Rate Limiting Configuration
// =============================================================================

/// Default per-client token bucket capacity (max burst).
fn default_client_burst() -> u64 {
    100
}

/// Default per-client sustained rate (requests/sec).
fn default_client_rate() -> f64 {
    50.0
}

/// Default per-organization token bucket capacity (max burst).
fn default_organization_burst() -> u64 {
    1000
}

/// Default per-organization sustained rate (requests/sec).
fn default_organization_rate() -> f64 {
    500.0
}

/// Default backpressure threshold (pending Raft proposals).
fn default_backpressure_threshold() -> u64 {
    100
}

/// Configuration for multi-level token bucket rate limiting.
///
/// Controls three tiers of admission control:
///
/// 1. **Per-client** — prevents one bad actor from monopolizing the system. `client_burst` sets the
///    max burst, `client_rate` sets sustained requests/sec.
///
/// 2. **Per-organization** — ensures fair sharing across tenants in a multi-tenant shard.
///    `organization_burst` sets the max burst, `organization_rate` sets sustained requests/sec.
///
/// 3. **Global backpressure** — throttles all requests when Raft consensus is saturated.
///    `backpressure_threshold` is the pending proposal count above which requests are rejected.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::RateLimitConfig;
/// let config = RateLimitConfig::builder()
///     .client_burst(200)
///     .client_rate(100.0)
///     .organization_burst(2000)
///     .organization_rate(1000.0)
///     .backpressure_threshold(200)
///     .build()
///     .expect("valid rate limit config");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RateLimitConfig {
    /// Maximum burst size per client (token bucket capacity).
    ///
    /// Must be > 0.
    #[serde(default = "default_client_burst")]
    pub client_burst: u64,
    /// Sustained requests per second per client (token refill rate).
    ///
    /// Must be > 0.
    #[serde(default = "default_client_rate")]
    pub client_rate: f64,
    /// Maximum burst size per organization (token bucket capacity).
    ///
    /// Must be > 0.
    #[serde(default = "default_organization_burst")]
    pub organization_burst: u64,
    /// Sustained requests per second per organization (token refill rate).
    ///
    /// Must be > 0.
    #[serde(default = "default_organization_rate")]
    pub organization_rate: f64,
    /// Pending Raft proposal count above which global backpressure activates.
    ///
    /// Must be > 0.
    #[serde(default = "default_backpressure_threshold")]
    pub backpressure_threshold: u64,
}

#[bon::bon]
impl RateLimitConfig {
    /// Creates a new rate limit configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is zero or negative.
    #[builder]
    pub fn new(
        #[builder(default = default_client_burst())] client_burst: u64,
        #[builder(default = default_client_rate())] client_rate: f64,
        #[builder(default = default_organization_burst())] organization_burst: u64,
        #[builder(default = default_organization_rate())] organization_rate: f64,
        #[builder(default = default_backpressure_threshold())] backpressure_threshold: u64,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            client_burst,
            client_rate,
            organization_burst,
            organization_rate,
            backpressure_threshold,
        };
        config.validate()?;
        Ok(config)
    }
}

impl RateLimitConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.client_burst == 0 {
            return Err(ConfigError::Validation {
                message: "client_burst must be > 0".to_string(),
            });
        }
        if self.client_rate <= 0.0 {
            return Err(ConfigError::Validation { message: "client_rate must be > 0".to_string() });
        }
        if self.organization_burst == 0 {
            return Err(ConfigError::Validation {
                message: "organization_burst must be > 0".to_string(),
            });
        }
        if self.organization_rate <= 0.0 {
            return Err(ConfigError::Validation {
                message: "organization_rate must be > 0".to_string(),
            });
        }
        if self.backpressure_threshold == 0 {
            return Err(ConfigError::Validation {
                message: "backpressure_threshold must be > 0".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            client_burst: default_client_burst(),
            client_rate: default_client_rate(),
            organization_burst: default_organization_burst(),
            organization_rate: default_organization_rate(),
            backpressure_threshold: default_backpressure_threshold(),
        }
    }
}

// =============================================================================
// Shutdown Configuration
// =============================================================================

/// Default grace period in seconds (15s).
const fn default_grace_period_secs() -> u64 {
    15
}

/// Default drain timeout in seconds (30s).
const fn default_drain_timeout_secs() -> u64 {
    30
}

/// Default pre-stop delay in seconds (5s).
const fn default_pre_stop_delay_secs() -> u64 {
    5
}

/// Default pre-shutdown timeout in seconds (60s).
const fn default_pre_shutdown_timeout_secs() -> u64 {
    60
}

/// Default watchdog interval multiplier (2x expected job interval).
const fn default_watchdog_multiplier() -> u64 {
    2
}

/// Minimum grace period in seconds.
const MIN_GRACE_PERIOD_SECS: u64 = 1;

/// Minimum drain timeout in seconds.
const MIN_DRAIN_TIMEOUT_SECS: u64 = 5;

/// Minimum pre-shutdown timeout in seconds.
const MIN_PRE_SHUTDOWN_TIMEOUT_SECS: u64 = 5;

/// Graceful shutdown configuration.
///
/// Controls how the server shuts down when receiving a termination signal.
/// The shutdown sequence is:
///
/// 1. Mark readiness probe as failing (stops new traffic from load balancer)
/// 2. Wait `pre_stop_delay_secs` for K8s to remove pod from endpoints
/// 3. Wait `grace_period_secs` for load balancer to drain existing connections
/// 4. Wait up to `drain_timeout_secs` for in-flight requests to complete
/// 5. Run pre-shutdown tasks (snapshots, Raft shutdown) with `pre_shutdown_timeout_secs` limit
/// 6. Signal the gRPC server to stop
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::ShutdownConfig;
/// let config = ShutdownConfig::builder()
///     .grace_period_secs(10)
///     .drain_timeout_secs(60)
///     .build()
///     .expect("valid shutdown config");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ShutdownConfig {
    /// Seconds to wait after marking readiness as failing before draining.
    ///
    /// This gives load balancers time to stop sending traffic. Must be >= 1.
    /// Default: 15 seconds.
    #[serde(default = "default_grace_period_secs")]
    pub grace_period_secs: u64,
    /// Maximum seconds to wait for in-flight requests to complete.
    ///
    /// After the grace period, the server waits up to this duration for
    /// active requests to finish. Must be >= 5. Default: 30 seconds.
    #[serde(default = "default_drain_timeout_secs")]
    pub drain_timeout_secs: u64,
    /// Seconds to delay between readiness failure and connection drain.
    ///
    /// Kubernetes `preStop` hook equivalent. Allows the Kubernetes endpoints
    /// controller to remove this pod from Service endpoints before we start
    /// refusing connections. Default: 5 seconds.
    #[serde(default = "default_pre_stop_delay_secs")]
    pub pre_stop_delay_secs: u64,
    /// Maximum seconds for pre-shutdown tasks (snapshots, Raft shutdown).
    ///
    /// If Raft shutdown or snapshot creation hangs, this timeout ensures
    /// the process eventually exits. Must be >= 5. Default: 60 seconds.
    #[serde(default = "default_pre_shutdown_timeout_secs")]
    pub pre_shutdown_timeout_secs: u64,
    /// Multiplier for background job watchdog interval.
    ///
    /// Liveness probe fails if any background job has not heartbeated
    /// within `watchdog_multiplier` times its expected interval.
    /// Default: 2 (2x the job's expected cycle time).
    #[serde(default = "default_watchdog_multiplier")]
    pub watchdog_multiplier: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            grace_period_secs: default_grace_period_secs(),
            drain_timeout_secs: default_drain_timeout_secs(),
            pre_stop_delay_secs: default_pre_stop_delay_secs(),
            pre_shutdown_timeout_secs: default_pre_shutdown_timeout_secs(),
            watchdog_multiplier: default_watchdog_multiplier(),
        }
    }
}

#[bon::bon]
impl ShutdownConfig {
    /// Creates a new shutdown configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if:
    /// - `grace_period_secs` < 1
    /// - `drain_timeout_secs` < 5
    /// - `pre_shutdown_timeout_secs` < 5
    #[builder]
    pub fn new(
        #[builder(default = default_grace_period_secs())] grace_period_secs: u64,
        #[builder(default = default_drain_timeout_secs())] drain_timeout_secs: u64,
        #[builder(default = default_pre_stop_delay_secs())] pre_stop_delay_secs: u64,
        #[builder(default = default_pre_shutdown_timeout_secs())] pre_shutdown_timeout_secs: u64,
        #[builder(default = default_watchdog_multiplier())] watchdog_multiplier: u64,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            grace_period_secs,
            drain_timeout_secs,
            pre_stop_delay_secs,
            pre_shutdown_timeout_secs,
            watchdog_multiplier,
        };
        config.validate()?;
        Ok(config)
    }
}

impl ShutdownConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure values are within valid ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is out of range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.grace_period_secs < MIN_GRACE_PERIOD_SECS {
            return Err(ConfigError::Validation {
                message: format!(
                    "grace_period_secs must be >= {}, got {}",
                    MIN_GRACE_PERIOD_SECS, self.grace_period_secs
                ),
            });
        }
        if self.drain_timeout_secs < MIN_DRAIN_TIMEOUT_SECS {
            return Err(ConfigError::Validation {
                message: format!(
                    "drain_timeout_secs must be >= {}, got {}",
                    MIN_DRAIN_TIMEOUT_SECS, self.drain_timeout_secs
                ),
            });
        }
        if self.pre_shutdown_timeout_secs < MIN_PRE_SHUTDOWN_TIMEOUT_SECS {
            return Err(ConfigError::Validation {
                message: format!(
                    "pre_shutdown_timeout_secs must be >= {}, got {}",
                    MIN_PRE_SHUTDOWN_TIMEOUT_SECS, self.pre_shutdown_timeout_secs
                ),
            });
        }
        if self.watchdog_multiplier == 0 {
            return Err(ConfigError::Validation {
                message: "watchdog_multiplier must be >= 1".to_string(),
            });
        }
        Ok(())
    }
}

// =============================================================================
// Health Check Configuration
// =============================================================================

fn default_dependency_check_timeout_secs() -> u64 {
    2
}

fn default_health_cache_ttl_secs() -> u64 {
    5
}

fn default_max_raft_lag() -> u64 {
    1000
}

/// Configuration for dependency health checks.
///
/// Controls timeouts and thresholds for external dependency validation
/// (disk writability, peer reachability, Raft log lag). Each check has
/// an independent timeout to prevent a slow dependency from blocking
/// the entire health probe.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::HealthCheckConfig;
/// let config = HealthCheckConfig::default();
/// assert_eq!(config.dependency_check_timeout_secs, 2);
/// assert_eq!(config.health_cache_ttl_secs, 5);
/// assert_eq!(config.max_raft_lag, 1000);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HealthCheckConfig {
    /// Timeout in seconds for each individual dependency check (disk, peer).
    ///
    /// Default: 2 seconds. Must be >= 1.
    #[serde(default = "default_dependency_check_timeout_secs")]
    pub dependency_check_timeout_secs: u64,

    /// Time-to-live in seconds for cached health check results.
    ///
    /// Prevents I/O storms from aggressive probe intervals. Subsequent
    /// probes within the TTL window return the cached result.
    ///
    /// Default: 5 seconds. Must be >= 1.
    #[serde(default = "default_health_cache_ttl_secs")]
    pub health_cache_ttl_secs: u64,

    /// Maximum Raft log entries behind the leader before readiness fails.
    ///
    /// Compares `last_log_index - last_applied.index` from Raft metrics.
    /// When the lag exceeds this threshold, the node is considered too far
    /// behind to serve consistent reads.
    ///
    /// Default: 1000. Must be >= 1.
    #[serde(default = "default_max_raft_lag")]
    pub max_raft_lag: u64,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            dependency_check_timeout_secs: default_dependency_check_timeout_secs(),
            health_cache_ttl_secs: default_health_cache_ttl_secs(),
            max_raft_lag: default_max_raft_lag(),
        }
    }
}

impl HealthCheckConfig {
    /// Validates an existing config (e.g., after deserialization).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any value is zero.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.dependency_check_timeout_secs == 0 {
            return Err(ConfigError::Validation {
                message: "dependency_check_timeout_secs must be >= 1".to_string(),
            });
        }
        if self.health_cache_ttl_secs == 0 {
            return Err(ConfigError::Validation {
                message: "health_cache_ttl_secs must be >= 1".to_string(),
            });
        }
        if self.max_raft_lag == 0 {
            return Err(ConfigError::Validation {
                message: "max_raft_lag must be >= 1".to_string(),
            });
        }
        Ok(())
    }
}

// =============================================================================
// Validation Configuration
// =============================================================================

/// Default maximum entity key size in bytes.
const fn default_max_key_bytes() -> usize {
    1024 // 1 KB
}

/// Default maximum entity value size in bytes.
const fn default_max_value_bytes() -> usize {
    10 * 1024 * 1024 // 10 MB
}

/// Default maximum operations per write request.
const fn default_max_operations_per_write() -> usize {
    1000
}

/// Default maximum aggregate payload size for a batch of operations.
const fn default_max_batch_payload_bytes() -> usize {
    100 * 1024 * 1024 // 100 MB
}

/// Default maximum organization name length in bytes.
const fn default_max_organization_name_bytes() -> usize {
    256
}

/// Default maximum relationship string length in bytes.
const fn default_max_relationship_string_bytes() -> usize {
    1024 // 1 KB
}

/// Input validation configuration for gRPC request fields.
///
/// Controls maximum sizes for entity keys, values, organization names,
/// relationship strings, and batch limits. Applied at both the gRPC
/// service boundary and in SDK operation constructors.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_types::config::ValidationConfig;
/// let config = ValidationConfig::builder()
///     .max_key_bytes(2048)
///     .max_operations_per_write(500)
///     .build()
///     .expect("valid validation config");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ValidationConfig {
    /// Maximum entity key size in bytes.
    ///
    /// Keys exceeding this limit are rejected with `INVALID_ARGUMENT`.
    /// Must be >= 1. Default: 1024 (1 KB).
    #[serde(default = "default_max_key_bytes")]
    pub max_key_bytes: usize,
    /// Maximum entity value size in bytes.
    ///
    /// Values exceeding this limit are rejected with `INVALID_ARGUMENT`.
    /// Must be >= 1. Default: 10 MB.
    #[serde(default = "default_max_value_bytes")]
    pub max_value_bytes: usize,
    /// Maximum number of operations per write request.
    ///
    /// Applies to both `Write` and `BatchWrite` RPCs.
    /// Must be >= 1. Default: 1000.
    #[serde(default = "default_max_operations_per_write")]
    pub max_operations_per_write: usize,
    /// Maximum aggregate payload size for a batch of operations in bytes.
    ///
    /// Prevents memory exhaustion from large batches.
    /// Must be >= 1. Default: 100 MB.
    #[serde(default = "default_max_batch_payload_bytes")]
    pub max_batch_payload_bytes: usize,
    /// Maximum organization name length in bytes.
    ///
    /// Organization names must also match `[a-z0-9-]{1,N}`.
    /// Must be >= 1. Default: 256.
    #[serde(default = "default_max_organization_name_bytes")]
    pub max_organization_name_bytes: usize,
    /// Maximum relationship string length in bytes.
    ///
    /// Applies to resource, relation, and subject fields.
    /// Must be >= 1. Default: 1024 (1 KB).
    #[serde(default = "default_max_relationship_string_bytes")]
    pub max_relationship_string_bytes: usize,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            max_key_bytes: default_max_key_bytes(),
            max_value_bytes: default_max_value_bytes(),
            max_operations_per_write: default_max_operations_per_write(),
            max_batch_payload_bytes: default_max_batch_payload_bytes(),
            max_organization_name_bytes: default_max_organization_name_bytes(),
            max_relationship_string_bytes: default_max_relationship_string_bytes(),
        }
    }
}

#[bon::bon]
impl ValidationConfig {
    /// Creates a new validation configuration, verifying all limits are positive.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any limit is zero.
    #[builder]
    pub fn new(
        #[builder(default = default_max_key_bytes())] max_key_bytes: usize,
        #[builder(default = default_max_value_bytes())] max_value_bytes: usize,
        #[builder(default = default_max_operations_per_write())] max_operations_per_write: usize,
        #[builder(default = default_max_batch_payload_bytes())] max_batch_payload_bytes: usize,
        #[builder(default = default_max_organization_name_bytes())]
        max_organization_name_bytes: usize,
        #[builder(default = default_max_relationship_string_bytes())]
        max_relationship_string_bytes: usize,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            max_key_bytes,
            max_value_bytes,
            max_operations_per_write,
            max_batch_payload_bytes,
            max_organization_name_bytes,
            max_relationship_string_bytes,
        };
        config.validate()?;
        Ok(config)
    }
}

impl ValidationConfig {
    /// Validates the configuration values.
    ///
    /// Call after deserialization to ensure all limits are positive.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if any limit is zero.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_key_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_key_bytes must be >= 1".to_string(),
            });
        }
        if self.max_value_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_value_bytes must be >= 1".to_string(),
            });
        }
        if self.max_operations_per_write == 0 {
            return Err(ConfigError::Validation {
                message: "max_operations_per_write must be >= 1".to_string(),
            });
        }
        if self.max_batch_payload_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_batch_payload_bytes must be >= 1".to_string(),
            });
        }
        if self.max_organization_name_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_organization_name_bytes must be >= 1".to_string(),
            });
        }
        if self.max_relationship_string_bytes == 0 {
            return Err(ConfigError::Validation {
                message: "max_relationship_string_bytes must be >= 1".to_string(),
            });
        }
        Ok(())
    }
}
