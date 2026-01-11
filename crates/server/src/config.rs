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
    /// Data directory for Raft logs and snapshots.
    pub data_dir: PathBuf,
    /// Peer nodes in the cluster.
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
    /// Batching configuration.
    #[serde(default)]
    pub batching: BatchConfig,
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
pub struct BatchConfig {
    /// Maximum number of transactions per batch.
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Maximum time to wait for a batch to fill (milliseconds).
    #[serde(default = "default_max_batch_delay_ms")]
    pub max_batch_delay_ms: u64,
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
    pub fn for_test(node_id: u64, port: u16, data_dir: PathBuf) -> Self {
        Self {
            node_id,
            listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            data_dir,
            peers: vec![],
            batching: BatchConfig::default(),
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
