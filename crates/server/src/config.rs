//! Server configuration.
//!
//! Provides configuration loading from files and environment variables.

use std::net::SocketAddr;
use std::path::PathBuf;

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
    /// Peer nodes in the cluster.
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
    /// Batching configuration.
    #[serde(default)]
    #[allow(dead_code)]
    pub batching: BatchConfig,
    /// Rate limiting configuration.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    /// Peer discovery configuration (DNS SRV, caching).
    #[serde(default)]
    pub discovery: DiscoveryConfig,
    /// Whether this node should bootstrap a new cluster.
    #[serde(default)]
    pub bootstrap: bool,
}

/// Peer node configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct PeerConfig {
    /// Peer node ID.
    pub node_id: u64,
    /// Peer address.
    pub addr: String,
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

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            srv_domain: None,
            cached_peers_path: None,
            cache_ttl_secs: default_cache_ttl(),
        }
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
                .add_source(config::File::with_name("ledger").required(false))
                .add_source(config::File::with_name("/etc/ledger/config").required(false))
        };

        // Add environment variables with INFERADB__LEDGER__ prefix.
        // Use "__" separator for nesting (e.g., INFERADB__LEDGER__BATCHING__MAX_BATCH_SIZE).
        // Single underscores in field names are preserved (e.g., INFERADB__LEDGER__NODE_ID → node_id).
        let builder = builder.add_source(
            config::Environment::with_prefix("INFERADB__LEDGER")
                .separator("__")
                .try_parsing(true),
        );

        let config = builder
            .build()
            .map_err(|e| ConfigError::Load(e.to_string()))?;

        config
            .try_deserialize()
            .map_err(|e| ConfigError::Parse(e.to_string()))
    }

    /// Create a configuration for testing.
    #[allow(clippy::unwrap_used, clippy::disallowed_methods, dead_code)]
    pub fn for_test(node_id: u64, port: u16, data_dir: PathBuf) -> Self {
        Self {
            node_id,
            listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            metrics_addr: None,
            data_dir,
            peers: vec![],
            batching: BatchConfig::default(),
            rate_limit: RateLimitConfig::default(),
            discovery: DiscoveryConfig::default(),
            bootstrap: true,
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
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Load(msg) => write!(f, "failed to load config: {}", msg),
            ConfigError::Parse(msg) => write!(f, "failed to parse config: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
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
        assert!(config.bootstrap);
    }
}
