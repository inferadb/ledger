//! Server configuration.
//!
//! Provides configuration loading from files and environment variables.
//! All configuration options are flat (no nested sections) for simpler
//! environment variable mapping.

use std::{net::SocketAddr, path::PathBuf};

use serde::Deserialize;

use crate::node_id;

/// Server configuration.
///
/// All fields are at the top level for simple environment variable mapping.
/// Environment variables use the `INFERADB__LEDGER__` prefix with field names
/// in SCREAMING_SNAKE_CASE (e.g., `INFERADB__LEDGER__BOOTSTRAP_EXPECT`).
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Unique node identifier (deprecated - use auto-generated Snowflake IDs).
    ///
    /// When set, a deprecation warning is logged. Prefer removing this field
    /// and letting the server auto-generate a unique Snowflake ID on first start.
    #[serde(default)]
    pub node_id: Option<u64>,

    /// Address to listen on for gRPC (e.g., "0.0.0.0:50051").
    pub listen_addr: SocketAddr,

    /// Address to expose Prometheus metrics (e.g., "0.0.0.0:9090").
    /// If not set, metrics endpoint is disabled.
    #[serde(default)]
    pub metrics_addr: Option<SocketAddr>,

    /// Data directory for Raft logs and snapshots.
    pub data_dir: PathBuf,

    // === Bootstrap ===
    /// Expected number of nodes for bootstrap (default: 3).
    ///
    /// Controls how nodes discover each other and coordinate to form a cluster:
    /// - `0`: Join mode. Waits to be added to existing cluster via AdminService.
    /// - `1`: Single-node deployment. Bootstraps immediately, no coordination.
    /// - `2+`: Coordinated mode. Waits for this many nodes before bootstrap.
    #[serde(default = "default_bootstrap_expect")]
    pub bootstrap_expect: u32,

    /// Timeout waiting for peers in seconds (default: 60).
    ///
    /// If the expected node count is not reached within this timeout,
    /// the node will fail to start with a timeout error.
    /// Ignored when `bootstrap_expect <= 1`.
    #[serde(default = "default_bootstrap_timeout_secs")]
    pub bootstrap_timeout_secs: u64,

    /// Discovery polling interval in seconds (default: 2).
    ///
    /// How frequently the node polls for new peers during the bootstrap
    /// coordination phase. Ignored when `bootstrap_expect <= 1`.
    #[serde(default = "default_bootstrap_poll_secs")]
    pub bootstrap_poll_secs: u64,

    // === Batching ===
    /// Maximum number of transactions per batch (default: 100).
    #[serde(default = "default_batch_max_size")]
    #[allow(dead_code)] // Reserved for LedgerServer batching integration
    pub batch_max_size: usize,

    /// Maximum time to wait for a batch to fill in milliseconds (default: 10).
    #[serde(default = "default_batch_max_delay_ms")]
    #[allow(dead_code)] // Reserved for LedgerServer batching integration
    pub batch_max_delay_ms: u64,

    // === Request Limits ===
    /// Maximum concurrent requests per connection (default: 100).
    #[serde(default = "default_requests_max_concurrent")]
    pub requests_max_concurrent: usize,

    /// Request timeout in seconds (default: 30).
    #[serde(default = "default_requests_timeout_secs")]
    pub requests_timeout_secs: u64,

    // === Discovery ===
    /// DNS SRV domain for bootstrap discovery.
    /// When set, the server will query `_ledger._tcp.<domain>` to discover peers.
    #[serde(default)]
    pub discovery_domain: Option<String>,

    /// Path to cache discovered peers for faster subsequent startups.
    #[serde(default)]
    pub discovery_cache_path: Option<String>,

    /// TTL for cached peers in seconds (default: 3600 = 1 hour).
    #[serde(default = "default_discovery_cache_ttl_secs")]
    pub discovery_cache_ttl_secs: u64,
}

// Default value functions for serde
fn default_bootstrap_expect() -> u32 {
    3
}
fn default_bootstrap_timeout_secs() -> u64 {
    60
}
fn default_bootstrap_poll_secs() -> u64 {
    2
}
fn default_batch_max_size() -> usize {
    100
}
fn default_batch_max_delay_ms() -> u64 {
    10
}
fn default_requests_max_concurrent() -> usize {
    100
}
fn default_requests_timeout_secs() -> u64 {
    30
}
fn default_discovery_cache_ttl_secs() -> u64 {
    3600 // 1 hour
}

impl Default for Config {
    #[allow(clippy::expect_used)] // Infallible: parsing a constant valid address
    fn default() -> Self {
        Self {
            node_id: None,
            listen_addr: "0.0.0.0:50051".parse().expect("valid default address"),
            metrics_addr: None,
            data_dir: PathBuf::from("/var/lib/ledger"),
            bootstrap_expect: default_bootstrap_expect(),
            bootstrap_timeout_secs: default_bootstrap_timeout_secs(),
            bootstrap_poll_secs: default_bootstrap_poll_secs(),
            batch_max_size: default_batch_max_size(),
            batch_max_delay_ms: default_batch_max_delay_ms(),
            requests_max_concurrent: default_requests_max_concurrent(),
            requests_timeout_secs: default_requests_timeout_secs(),
            discovery_domain: None,
            discovery_cache_path: None,
            discovery_cache_ttl_secs: default_discovery_cache_ttl_secs(),
        }
    }
}

impl Config {
    /// Load configuration from a file.
    ///
    /// Supports TOML format. Environment variables can override config values
    /// using the `INFERADB__LEDGER__` prefix (e.g., `INFERADB__LEDGER__BOOTSTRAP_EXPECT=3`).
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
        // Use "__" separator for nesting (though config is now flat).
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
            bootstrap_expect: 1, // Single-node mode for tests
            ..Self::default()
        }
    }

    /// Create a configuration for single-node deployments.
    ///
    /// Sets `bootstrap_expect=1` for immediate bootstrap without coordination.
    /// Primarily for testing or development scenarios.
    #[allow(dead_code)]
    pub fn for_single_node() -> Self {
        Self { bootstrap_expect: 1, ..Self::default() }
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

    /// Validate the configuration.
    ///
    /// Always succeeds - all `bootstrap_expect` values are valid:
    /// - `0`: Join existing cluster (no bootstrap)
    /// - `1`: Single-node deployment
    /// - `2+`: Coordinated multi-node bootstrap
    #[allow(dead_code)]
    pub fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }

    /// Returns true if this is a single-node deployment (no coordination needed).
    #[allow(dead_code)]
    pub fn is_single_node(&self) -> bool {
        self.bootstrap_expect == 1
    }

    /// Returns true if this node should wait to join an existing cluster.
    ///
    /// When `bootstrap_expect=0`, the node starts without initializing a Raft
    /// cluster and waits to be added via AdminService's JoinCluster RPC.
    #[allow(dead_code)]
    pub fn is_join_mode(&self) -> bool {
        self.bootstrap_expect == 0
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
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.bootstrap_expect, 3);
        assert_eq!(config.bootstrap_timeout_secs, 60);
        assert_eq!(config.bootstrap_poll_secs, 2);
        assert_eq!(config.batch_max_size, 100);
        assert_eq!(config.batch_max_delay_ms, 10);
        assert_eq!(config.requests_max_concurrent, 100);
        assert_eq!(config.requests_timeout_secs, 30);
        assert_eq!(config.discovery_cache_ttl_secs, 3600);
        assert!(!config.is_single_node());
    }

    #[test]
    fn test_config_for_test() {
        let config = Config::for_test(1, 50051, PathBuf::from("/tmp/ledger-test"));
        assert_eq!(config.node_id, Some(1));
        assert_eq!(config.listen_addr.port(), 50051);
        assert_eq!(config.bootstrap_expect, 1);
        assert!(config.is_single_node());
    }

    #[test]
    fn test_config_for_single_node() {
        let config = Config::for_single_node();
        assert_eq!(config.bootstrap_expect, 1);
        assert!(config.is_single_node());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_success() {
        let config = Config { bootstrap_expect: 3, ..Config::default() };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_join_mode() {
        let config = Config { bootstrap_expect: 0, ..Config::default() };
        assert!(config.validate().is_ok());
        assert!(config.is_join_mode());
        assert!(!config.is_single_node());
    }
}
