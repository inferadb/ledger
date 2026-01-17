//! Server configuration.
//!
//! Provides configuration loading from files and environment variables.

use std::{net::SocketAddr, path::PathBuf};

use serde::Deserialize;

use crate::node_id;

/// Server configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Unique node identifier (deprecated - use auto-generated Snowflake IDs).
    ///
    /// When set, a deprecation warning is logged. Prefer removing this field
    /// and letting the server auto-generate a unique Snowflake ID on first start.
    #[serde(default)]
    pub node_id: Option<u64>,
    /// Address to listen on for gRPC.
    pub listen_addr: SocketAddr,
    /// Address to expose Prometheus metrics (e.g., "0.0.0.0:9090").
    /// If not set, metrics endpoint is disabled.
    #[serde(default)]
    pub metrics_addr: Option<SocketAddr>,
    /// Data directory for Raft logs and snapshots.
    pub data_dir: PathBuf,
    /// Batching configuration.
    #[serde(default)]
    #[allow(dead_code)]
    pub batching: BatchConfig,
    /// Rate limiting configuration.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    /// Peer discovery configuration (DNS SRV, caching).
    ///
    /// Used to determine cluster membership on startup:
    /// - If discovery finds existing peers, this node joins the cluster
    /// - If no peers are discovered, this node bootstraps a new cluster
    #[serde(default)]
    pub discovery: DiscoveryConfig,
    /// Bootstrap coordination configuration.
    ///
    /// Controls how nodes discover each other and coordinate to form a cluster.
    /// See [`BootstrapConfig`] for details on each setting.
    #[serde(default)]
    #[allow(dead_code)] // Used in Task 5/6 (coordinator integration)
    pub bootstrap: BootstrapConfig,
}

/// Transaction batching configuration.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct BatchConfig {
    /// Maximum number of transactions per batch.
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Maximum time to wait for a batch to fill (milliseconds).
    #[serde(default = "default_max_batch_delay_ms")]
    pub max_batch_delay_ms: u64,
}

/// Rate limiting configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum requests per second per connection.
    /// Reserved for future use - not currently implemented due to tonic Clone requirements.
    #[serde(default = "default_requests_per_second")]
    #[allow(dead_code)]
    pub requests_per_second: u64,
    /// Maximum concurrent requests per connection.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    /// Request timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: default_requests_per_second(),
            max_concurrent: default_max_concurrent(),
            timeout_secs: default_timeout_secs(),
        }
    }
}

fn default_requests_per_second() -> u64 {
    1000 // 1000 requests per second by default
}

fn default_max_concurrent() -> usize {
    100 // Max 100 concurrent requests per connection
}

fn default_timeout_secs() -> u64 {
    30 // 30 second timeout
}

/// Peer discovery configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DiscoveryConfig {
    /// DNS SRV domain for bootstrap discovery.
    /// When set, the server will query `_ledger._tcp.<domain>` to discover peers.
    #[serde(default)]
    pub srv_domain: Option<String>,

    /// Path to cache discovered peers for faster subsequent startups.
    #[serde(default)]
    pub cached_peers_path: Option<String>,

    /// TTL for cached peers in seconds (default: 3600 = 1 hour).
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,
}

/// Bootstrap configuration for coordinated cluster formation.
///
/// Controls how nodes discover each other and coordinate to form a cluster.
/// The `bootstrap_expect` value determines behavior:
/// - `0`: Join mode. Node waits to be added to an existing cluster via AdminService.
/// - `1`: Single-node mode. Node bootstraps immediately without coordination.
/// - `2+`: Coordinated mode. Nodes discover peers and the lowest-ID node bootstraps.
///
/// Default is 3 to ensure production clusters have proper quorum.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // Fields used in Task 5/6 (coordinator integration)
pub struct BootstrapConfig {
    /// Expected number of nodes for bootstrap (default: 3).
    ///
    /// - `0`: Join mode. Waits to be added to existing cluster via AdminService.
    /// - `1`: Single-node deployment. Bootstraps immediately, no coordination.
    /// - `2+`: Waits for this many nodes before coordinated bootstrap.
    #[serde(default = "default_bootstrap_expect")]
    pub bootstrap_expect: u32,

    /// Timeout waiting for peers in seconds (default: 60).
    ///
    /// If the expected node count is not reached within this timeout,
    /// the node will fail to start with a timeout error.
    /// Ignored when `bootstrap_expect <= 1`.
    #[serde(default = "default_bootstrap_timeout")]
    pub bootstrap_timeout_secs: u64,

    /// Discovery polling interval in seconds (default: 2).
    ///
    /// How frequently the node polls for new peers during the bootstrap
    /// coordination phase. Ignored when `bootstrap_expect <= 1`.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
}

fn default_bootstrap_expect() -> u32 {
    3
}

fn default_bootstrap_timeout() -> u64 {
    60
}

fn default_poll_interval() -> u64 {
    2
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            bootstrap_expect: default_bootstrap_expect(),
            bootstrap_timeout_secs: default_bootstrap_timeout(),
            poll_interval_secs: default_poll_interval(),
        }
    }
}

#[allow(dead_code)] // Used in Task 5/6 (coordinator integration)
impl BootstrapConfig {
    /// Create a bootstrap configuration for single-node deployments.
    ///
    /// This is primarily for testing or development scenarios where
    /// a single-node cluster is acceptable.
    pub fn for_single_node() -> Self {
        Self { bootstrap_expect: 1, ..Self::default() }
    }

    /// Validate the bootstrap configuration.
    ///
    /// Always succeeds - all `bootstrap_expect` values are valid:
    /// - `0`: Join existing cluster (no bootstrap)
    /// - `1`: Single-node deployment
    /// - `2+`: Coordinated multi-node bootstrap
    pub fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }

    /// Returns true if this is a single-node deployment (no coordination needed).
    pub fn is_single_node(&self) -> bool {
        self.bootstrap_expect == 1
    }

    /// Returns true if this node should wait to join an existing cluster.
    ///
    /// When `bootstrap_expect=0`, the node starts without initializing a Raft
    /// cluster and waits to be added via AdminService's JoinCluster RPC.
    pub fn is_join_mode(&self) -> bool {
        self.bootstrap_expect == 0
    }
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self { srv_domain: None, cached_peers_path: None, cache_ttl_secs: default_cache_ttl() }
    }
}

fn default_cache_ttl() -> u64 {
    3600 // 1 hour
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: default_max_batch_size(),
            max_batch_delay_ms: default_max_batch_delay_ms(),
        }
    }
}

fn default_max_batch_size() -> usize {
    100
}

fn default_max_batch_delay_ms() -> u64 {
    10
}

impl Config {
    /// Load configuration from a file.
    ///
    /// Supports TOML format. Environment variables can override config values
    /// using the `LEDGER_` prefix (e.g., `LEDGER_NODE_ID=1`).
    pub fn load(path: Option<&str>) -> Result<Self, ConfigError> {
        let builder = config::Config::builder();

        // Add config file if provided
        let builder = if let Some(path) = path {
            builder.add_source(config::File::with_name(path))
        } else {
            // Try default locations
            builder
                .add_source(config::File::with_name("inferadb-ledger").required(false))
                .add_source(config::File::with_name("/etc/inferadb-ledger/config").required(false))
        };

        // Add environment variables with INFERADB__LEDGER__ prefix.
        // Use "__" separator for nesting (e.g., INFERADB__LEDGER__BATCHING__MAX_BATCH_SIZE).
        // Single underscores in field names are preserved (e.g., INFERADB__LEDGER__NODE_ID →
        // node_id).
        let builder = builder.add_source(
            config::Environment::with_prefix("INFERADB__LEDGER").separator("__").try_parsing(true),
        );

        let config = builder.build().map_err(|e| ConfigError::Load(e.to_string()))?;

        config.try_deserialize().map_err(|e| ConfigError::Parse(e.to_string()))
    }

    /// Create a configuration for testing.
    ///
    /// Uses `bootstrap_expect=1` (single-node mode) by default for simple
    /// test scenarios. The `node_id` is explicitly set to allow tests to
    /// use deterministic, predictable node IDs.
    #[allow(clippy::unwrap_used, clippy::disallowed_methods, dead_code)]
    pub fn for_test(node_id: u64, port: u16, data_dir: PathBuf) -> Self {
        Self {
            node_id: Some(node_id),
            listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            metrics_addr: None,
            data_dir,
            batching: BatchConfig::default(),
            rate_limit: RateLimitConfig::default(),
            discovery: DiscoveryConfig::default(),
            bootstrap: BootstrapConfig::for_single_node(),
        }
    }

    /// Get the effective node ID, logging a deprecation warning if manually configured.
    ///
    /// This method resolves the node ID to use:
    /// - If `node_id` is explicitly set in config, use it (with deprecation warning)
    /// - If not set, auto-generate a Snowflake ID and persist to `{data_dir}/node_id`
    ///
    /// The generated ID is persisted to disk and reused on subsequent startups to
    /// maintain cluster identity across restarts.
    pub fn effective_node_id(&self) -> Result<u64, ConfigError> {
        match self.node_id {
            Some(id) => {
                tracing::warn!(
                    node_id = id,
                    "node_id in config is deprecated; remove it to use auto-generated Snowflake IDs"
                );
                Ok(id)
            },
            None => node_id::load_or_generate_node_id(&self.data_dir).map_err(|e| {
                ConfigError::Validation(format!("failed to load or generate node ID: {}", e))
            }),
        }
    }
}

/// Configuration error.
#[derive(Debug)]
pub enum ConfigError {
    /// Failed to load configuration.
    Load(String),
    /// Failed to parse configuration.
    Parse(String),
    /// Configuration validation failed.
    Validation(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Load(msg) => write!(f, "failed to load config: {}", msg),
            ConfigError::Parse(msg) => write!(f, "failed to parse config: {}", msg),
            ConfigError::Validation(msg) => write!(f, "invalid config: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_default_batch_config() {
        let config = BatchConfig::default();
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.max_batch_delay_ms, 10);
    }

    #[test]
    fn test_config_for_test() {
        let config = Config::for_test(1, 50051, PathBuf::from("/tmp/ledger-test"));
        assert_eq!(config.node_id, Some(1));
        assert_eq!(config.listen_addr.port(), 50051);
        // Verify bootstrap config for tests uses single-node mode
        assert_eq!(config.bootstrap.bootstrap_expect, 1);
        assert!(config.bootstrap.is_single_node());
    }

    #[test]
    fn test_bootstrap_config_defaults() {
        let config = BootstrapConfig::default();
        assert_eq!(config.bootstrap_expect, 3);
        assert_eq!(config.bootstrap_timeout_secs, 60);
        assert_eq!(config.poll_interval_secs, 2);
        assert!(!config.is_single_node());
    }

    #[test]
    fn test_bootstrap_config_validate_success() {
        let config = BootstrapConfig { bootstrap_expect: 3, ..Default::default() };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_bootstrap_config_single_node_valid() {
        let config = BootstrapConfig::for_single_node();
        assert!(config.validate().is_ok());
        assert!(config.is_single_node());
    }

    #[test]
    fn test_bootstrap_config_join_mode() {
        let config = BootstrapConfig { bootstrap_expect: 0, ..Default::default() };
        assert!(config.validate().is_ok());
        assert!(config.is_join_mode());
        assert!(!config.is_single_node());
    }
}
