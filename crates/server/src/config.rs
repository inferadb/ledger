//! Server configuration.
//!
//! Provides configuration loading from files and environment variables.

use std::{net::SocketAddr, path::PathBuf};

use serde::Deserialize;

/// Server configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Unique node identifier.
    pub node_id: u64,
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
/// The `min_cluster_size` determines the minimum number of nodes required
/// before bootstrapping can proceed. A default of 3 ensures production clusters
/// have proper quorum.
#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapConfig {
    /// Minimum cluster size before bootstrapping (default: 3).
    ///
    /// The node will wait until this many nodes are discovered before
    /// proceeding with cluster formation. A value of 1 requires
    /// `allow_single_node=true` to prevent accidental single-node deployments.
    #[serde(default = "default_min_cluster_size")]
    pub min_cluster_size: u32,

    /// Timeout waiting for peers in seconds (default: 60).
    ///
    /// If the minimum cluster size is not reached within this timeout,
    /// the node will fail to start with a timeout error.
    #[serde(default = "default_bootstrap_timeout")]
    pub bootstrap_timeout_secs: u64,

    /// Discovery polling interval in seconds (default: 2).
    ///
    /// How frequently the node polls for new peers during the bootstrap
    /// coordination phase.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,

    /// Allow single-node mode (required when min_cluster_size=1).
    ///
    /// Must be explicitly set to `true` to allow single-node deployments.
    /// This prevents accidental misconfiguration in production.
    #[serde(default)]
    pub allow_single_node: bool,
}

fn default_min_cluster_size() -> u32 {
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
            min_cluster_size: default_min_cluster_size(),
            bootstrap_timeout_secs: default_bootstrap_timeout(),
            poll_interval_secs: default_poll_interval(),
            allow_single_node: false,
        }
    }
}

impl BootstrapConfig {
    /// Validate the bootstrap configuration.
    ///
    /// Returns an error if:
    /// - `min_cluster_size` is 0 (must be at least 1)
    /// - `min_cluster_size` is 1 without `allow_single_node=true`
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.min_cluster_size == 0 {
            return Err(ConfigError::Validation(
                "min_cluster_size must be at least 1".to_string(),
            ));
        }

        if self.min_cluster_size == 1 && !self.allow_single_node {
            return Err(ConfigError::Validation(
                "min_cluster_size=1 requires allow_single_node=true".to_string(),
            ));
        }

        Ok(())
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
    #[allow(clippy::unwrap_used, clippy::disallowed_methods, dead_code)]
    pub fn for_test(node_id: u64, port: u16, data_dir: PathBuf) -> Self {
        Self {
            node_id,
            listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            metrics_addr: None,
            data_dir,
            batching: BatchConfig::default(),
            rate_limit: RateLimitConfig::default(),
            discovery: DiscoveryConfig::default(),
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
        assert_eq!(config.node_id, 1);
        assert_eq!(config.listen_addr.port(), 50051);
    }


    #[test]
    fn test_bootstrap_config_defaults() {
        let config = BootstrapConfig::default();
        assert_eq!(config.min_cluster_size, 3);
        assert_eq!(config.bootstrap_timeout_secs, 60);
        assert_eq!(config.poll_interval_secs, 2);
        assert!(!config.allow_single_node);
    }

    #[test]
    fn test_bootstrap_config_validate_success() {
        // min_cluster_size=3 doesn't require allow_single_node
        let config = BootstrapConfig {
            min_cluster_size: 3,
            bootstrap_timeout_secs: 60,
            poll_interval_secs: 2,
            allow_single_node: false,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_bootstrap_config_single_node_requires_flag() {
        // min_cluster_size=1 requires allow_single_node=true
        let config = BootstrapConfig {
            min_cluster_size: 1,
            bootstrap_timeout_secs: 60,
            poll_interval_secs: 2,
            allow_single_node: false,
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, ConfigError::Validation(_)));
    }

    #[test]
    fn test_bootstrap_config_single_node_with_flag() {
        // min_cluster_size=1 with allow_single_node=true is valid
        let config = BootstrapConfig {
            min_cluster_size: 1,
            bootstrap_timeout_secs: 60,
            poll_interval_secs: 2,
            allow_single_node: true,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_bootstrap_config_zero_cluster_size_invalid() {
        // min_cluster_size must be >= 1
        let config = BootstrapConfig {
            min_cluster_size: 0,
            bootstrap_timeout_secs: 60,
            poll_interval_secs: 2,
            allow_single_node: true,
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, ConfigError::Validation(_)));
    }
}
