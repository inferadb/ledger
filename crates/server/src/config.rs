//! Server configuration.
//!
//! Configuration is loaded from command-line arguments or environment variables.
//! CLI arguments take precedence over environment variables.
//!
//! # Bootstrap Modes
//!
//! Use one of `--single`, `--join`, or `--cluster N` to control cluster formation:
//!
//! - `--single`: Bootstrap as a single-node cluster (no coordination)
//! - `--join`: Wait to be added to an existing cluster via AdminService
//! - `--cluster N`: Coordinated bootstrap with N nodes (default: 3)
//!
//! Environment variable `INFERADB__LEDGER__CLUSTER` sets the numeric value directly:
//! `0` = join, `1` = single, `N` = cluster with N nodes.

use std::{net::SocketAddr, path::PathBuf};

use bon::Builder;
use clap::Parser;
use serde::Deserialize;
use snafu::Snafu;

use crate::node_id;

/// Default listen address for the gRPC server (localhost only for security).
const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:50051";

/// Server configuration.
///
/// Configuration can be provided via command-line arguments or environment variables.
/// CLI arguments take precedence over environment variables.
///
/// # CLI Example
///
/// ```bash
/// # Single-node cluster
/// inferadb-ledger --single --data /data
///
/// # Join existing cluster
/// inferadb-ledger --join --data /data --peers ledger.example.com
///
/// # Coordinated 3-node bootstrap (default)
/// inferadb-ledger --cluster 3 --data /data --peers ledger.example.com
/// ```
///
/// # Environment Variables
///
/// All options can be set via environment variables with the `INFERADB__LEDGER__` prefix:
///
/// ```bash
/// INFERADB__LEDGER__LISTEN=0.0.0.0:9000 \
/// INFERADB__LEDGER__DATA=/data/ledger \
/// INFERADB__LEDGER__CLUSTER=3 \
/// inferadb-ledger
/// ```
///
/// # Ephemeral Mode
///
/// When `--data` is not specified, the server runs in ephemeral mode using a
/// temporary directory. All data is lost on shutdown. This is useful for
/// development and testing.
///
/// # Builder Example
///
/// ```no_run
/// use std::path::PathBuf;
/// use inferadb_ledger_server::config::Config;
///
/// let config = Config::builder()
///     .listen_addr("0.0.0.0:9000".parse().unwrap())
///     .data_dir(PathBuf::from("/data/ledger"))
///     .cluster(3)
///     .build();
/// ```
#[derive(Debug, Clone, Deserialize, Builder, Parser)]
#[command(name = "inferadb-ledger")]
#[command(about = "InferaDB Ledger - Distributed consensus ledger")]
#[command(version)]
#[builder(derive(Debug))]
pub struct Config {
    /// Address to listen on for gRPC.
    ///
    /// Defaults to 127.0.0.1:50051 (localhost only) for security.
    /// Set to 0.0.0.0:50051 or a specific IP to accept remote connections.
    #[arg(long = "listen", env = "INFERADB__LEDGER__LISTEN", default_value = DEFAULT_LISTEN_ADDR)]
    #[builder(default = default_listen_addr())]
    pub listen_addr: SocketAddr,

    /// Address to expose Prometheus metrics. Disabled if not set.
    #[arg(long = "metrics", env = "INFERADB__LEDGER__METRICS")]
    #[serde(default)]
    pub metrics_addr: Option<SocketAddr>,

    /// Data directory for Raft logs and snapshots.
    ///
    /// If not specified, the server runs in ephemeral mode using a temporary
    /// directory. All data is lost on shutdown.
    #[arg(long = "data", env = "INFERADB__LEDGER__DATA")]
    #[serde(default)]
    pub data_dir: Option<PathBuf>,

    // === Bootstrap Mode ===
    /// Run as a single-node cluster (no coordination needed).
    /// Mutually exclusive with --join and --cluster.
    #[arg(long, group = "bootstrap_mode")]
    #[serde(skip)]
    #[builder(skip)]
    pub single: bool,

    /// Join an existing cluster (wait to be added via AdminService).
    /// Mutually exclusive with --single and --cluster.
    #[arg(long, group = "bootstrap_mode")]
    #[serde(skip)]
    #[builder(skip)]
    pub join: bool,

    /// Coordinated bootstrap: wait for N nodes, then lowest-ID bootstraps.
    /// Mutually exclusive with --single and --join. Defaults to 3 if no mode specified.
    #[arg(long, env = "INFERADB__LEDGER__CLUSTER", group = "bootstrap_mode", value_name = "N")]
    #[serde(default = "default_cluster")]
    pub cluster: Option<u32>,

    // === Discovery ===
    /// How to find peer nodes: DNS domain or file path.
    ///
    /// Automatically detected based on the value:
    /// - Contains `/` or `\` or ends with `.json` → file path (JSON peer list)
    /// - Otherwise → DNS domain for A record lookup
    ///
    /// Examples:
    /// - `ledger.default.svc.cluster.local` → DNS lookup
    /// - `/var/lib/ledger/peers.json` → file path
    #[arg(long = "peers", env = "INFERADB__LEDGER__PEERS")]
    #[serde(default)]
    pub peers: Option<String>,

    /// TTL for cached peers, in seconds.
    #[arg(long = "peers-ttl", env = "INFERADB__LEDGER__PEERS_TTL", default_value_t = 3600)]
    #[serde(default = "default_peers_ttl_secs")]
    #[builder(default = default_peers_ttl_secs())]
    pub peers_ttl_secs: u64,

    /// Timeout waiting for peers, in seconds.
    ///
    /// If the expected node count is not reached within this timeout,
    /// the node will fail to start. Ignored when --expect <= 1.
    #[arg(long = "peers-timeout", env = "INFERADB__LEDGER__PEERS_TIMEOUT", default_value_t = 60)]
    #[serde(default = "default_peers_timeout_secs")]
    #[builder(default = default_peers_timeout_secs())]
    pub peers_timeout_secs: u64,

    /// Peer discovery polling interval, in seconds.
    ///
    /// How frequently the node polls for new peers during the bootstrap
    /// coordination phase. Ignored when --expect <= 1.
    #[arg(long = "peers-poll", env = "INFERADB__LEDGER__PEERS_POLL", default_value_t = 2)]
    #[serde(default = "default_peers_poll_secs")]
    #[builder(default = default_peers_poll_secs())]
    pub peers_poll_secs: u64,

    // === Batching ===
    /// Maximum transactions per batch.
    #[arg(long = "batch-size", env = "INFERADB__LEDGER__BATCH_SIZE", default_value_t = 100)]
    #[serde(default = "default_batch_max_size")]
    #[builder(default = default_batch_max_size())]
    #[allow(dead_code)] // reserved for LedgerServer batching integration
    pub batch_max_size: usize,

    /// Maximum batch fill wait time, in seconds (supports fractions, e.g., 0.01 = 10ms).
    #[arg(long = "batch-delay", env = "INFERADB__LEDGER__BATCH_DELAY", default_value_t = 0.01)]
    #[serde(default = "default_batch_max_delay_secs")]
    #[builder(default = default_batch_max_delay_secs())]
    #[allow(dead_code)] // reserved for LedgerServer batching integration
    pub batch_max_delay_secs: f64,

    // === Request Limits ===
    /// Maximum concurrent requests.
    #[arg(long = "concurrent", env = "INFERADB__LEDGER__MAX_CONCURRENT", default_value_t = 100)]
    #[serde(default = "default_max_concurrent")]
    #[builder(default = default_max_concurrent())]
    pub max_concurrent: usize,

    /// Request timeout, in seconds.
    #[arg(long = "timeout", env = "INFERADB__LEDGER__TIMEOUT", default_value_t = 30)]
    #[serde(default = "default_timeout_secs")]
    #[builder(default = default_timeout_secs())]
    pub timeout_secs: u64,
}

// Default value functions
#[expect(clippy::expect_used, reason = "infallible: parsing constant valid address")]
fn default_listen_addr() -> SocketAddr {
    DEFAULT_LISTEN_ADDR.parse().expect("valid default address")
}

fn default_cluster() -> Option<u32> {
    Some(3)
}
fn default_peers_ttl_secs() -> u64 {
    3600 // 1 hour
}
fn default_peers_timeout_secs() -> u64 {
    60
}
fn default_peers_poll_secs() -> u64 {
    2
}
fn default_batch_max_size() -> usize {
    100
}
fn default_batch_max_delay_secs() -> f64 {
    0.01 // 10ms
}
fn default_max_concurrent() -> usize {
    100
}
fn default_timeout_secs() -> u64 {
    30
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            metrics_addr: None,
            data_dir: None,
            single: false,
            join: false,
            cluster: default_cluster(),
            peers: None,
            peers_ttl_secs: default_peers_ttl_secs(),
            peers_timeout_secs: default_peers_timeout_secs(),
            peers_poll_secs: default_peers_poll_secs(),
            batch_max_size: default_batch_max_size(),
            batch_max_delay_secs: default_batch_max_delay_secs(),
            max_concurrent: default_max_concurrent(),
            timeout_secs: default_timeout_secs(),
        }
    }
}

impl Config {
    /// Parse configuration from command-line arguments and environment variables.
    ///
    /// CLI arguments take precedence over environment variables.
    /// Fields not set via either use their default values.
    pub fn parse_args() -> Self {
        Self::parse()
    }

    /// Create a configuration for testing.
    ///
    /// Uses single-node mode by default for simple test scenarios.
    /// Writes the given node_id to the data directory for deterministic,
    /// predictable node IDs in tests.
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, dead_code)]
    pub fn for_test(node_id: u64, port: u16, data_dir: PathBuf) -> Self {
        // Write the node_id file for deterministic test behavior
        node_id::write_node_id(&data_dir, node_id).expect("failed to write test node_id");

        Self {
            listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            metrics_addr: None,
            data_dir: Some(data_dir),
            single: true,
            join: false,
            cluster: None,
            ..Self::default()
        }
    }

    /// Create a configuration for single-node deployments.
    ///
    /// Sets single-node mode for immediate bootstrap without coordination.
    /// Primarily for testing or development scenarios.
    #[allow(dead_code)] // convenience constructor for single-node deployments
    pub fn for_single_node() -> Self {
        Self { single: true, join: false, cluster: None, ..Self::default() }
    }

    /// Get the effective bootstrap_expect value.
    ///
    /// Computed from the bootstrap mode flags:
    /// - `--single` → 1
    /// - `--join` → 0
    /// - `--cluster N` → N
    /// - default → 3
    pub fn bootstrap_expect(&self) -> u32 {
        if self.single {
            1
        } else if self.join {
            0
        } else {
            self.cluster.unwrap_or(3)
        }
    }

    /// Returns true if the server is listening only on localhost.
    ///
    /// When true, only local connections are accepted. Remote clients
    /// will not be able to connect.
    pub fn is_localhost_only(&self) -> bool {
        self.listen_addr.ip().is_loopback()
    }

    /// Returns true if the server is running in ephemeral mode.
    ///
    /// In ephemeral mode, data is stored in a temporary directory and
    /// will be lost when the server shuts down.
    pub fn is_ephemeral(&self) -> bool {
        self.data_dir.is_none()
    }

    /// Resolve the data directory, creating an ephemeral one if needed.
    ///
    /// If `data_dir` is configured, returns it directly. Otherwise, creates
    /// a unique temporary directory using a Snowflake ID for uniqueness.
    ///
    /// # Errors
    ///
    /// Returns an error if the ephemeral directory cannot be created.
    pub fn resolve_data_dir(&self) -> Result<PathBuf, ConfigError> {
        match &self.data_dir {
            Some(dir) => Ok(dir.clone()),
            None => {
                let id = node_id::generate_snowflake_id().map_err(|e| ConfigError::Validation {
                    message: format!("failed to generate ephemeral directory ID: {}", e),
                })?;
                let path = std::env::temp_dir().join(format!("ledger-{}", id));
                std::fs::create_dir_all(&path).map_err(|e| ConfigError::Validation {
                    message: format!(
                        "failed to create ephemeral directory {}: {}",
                        path.display(),
                        e
                    ),
                })?;
                Ok(path)
            },
        }
    }

    /// Get the node ID for this server.
    ///
    /// Loads an existing node ID from `{data_dir}/node_id`, or generates a new
    /// Snowflake ID and persists it. The ID is reused across restarts to maintain
    /// cluster identity.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - The resolved data directory (from `resolve_data_dir()`)
    pub fn node_id(&self, data_dir: &std::path::Path) -> Result<u64, ConfigError> {
        node_id::load_or_generate_node_id(data_dir).map_err(|e| ConfigError::Validation {
            message: format!("failed to load or generate node ID: {}", e),
        })
    }

    /// Validate the configuration.
    ///
    /// Validates bootstrap mode settings:
    /// - `--single`: Single-node deployment
    /// - `--join`: Join existing cluster
    /// - `--cluster N`: Coordinated bootstrap (N must be >= 2)
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(n) = self.cluster
            && n < 2
        {
            return Err(ConfigError::Validation {
                message: format!(
                    "--cluster requires at least 2 nodes, got {}. Use --single for single-node or --join to join existing cluster",
                    n
                ),
            });
        }
        Ok(())
    }

    /// Returns true if this is a single-node deployment (no coordination needed).
    pub fn is_single_node(&self) -> bool {
        self.bootstrap_expect() == 1
    }

    /// Returns true if this node should wait to join an existing cluster.
    ///
    /// When in join mode, the node starts without initializing a Raft
    /// cluster and waits to be added via AdminService's JoinCluster RPC.
    pub fn is_join_mode(&self) -> bool {
        self.bootstrap_expect() == 0
    }

    /// Returns the peers value as a DNS domain if it looks like a domain.
    ///
    /// A value is treated as a DNS domain if it does NOT:
    /// - Contain `/` or `\` (path separators)
    /// - End with `.json`
    pub fn peers_as_dns_domain(&self) -> Option<&str> {
        self.peers
            .as_ref()
            .and_then(|p| if Self::is_file_path(p) { None } else { Some(p.as_str()) })
    }

    /// Returns the peers value as a file path if it looks like a file path.
    ///
    /// A value is treated as a file path if it:
    /// - Contains `/` or `\` (path separators), OR
    /// - Ends with `.json`
    pub fn peers_as_file_path(&self) -> Option<&str> {
        self.peers
            .as_ref()
            .and_then(|p| if Self::is_file_path(p) { Some(p.as_str()) } else { None })
    }

    /// Detects whether a peers value looks like a file path.
    fn is_file_path(value: &str) -> bool {
        value.contains('/') || value.contains('\\') || value.ends_with(".json")
    }
}

/// Configuration error.
#[derive(Debug, Snafu)]
pub enum ConfigError {
    /// Configuration validation failed.
    #[snafu(display("invalid config: {message}"))]
    Validation {
        /// Error description.
        message: String,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.bootstrap_expect(), 3);
        assert_eq!(config.cluster, Some(3));
        assert_eq!(config.peers_timeout_secs, 60);
        assert_eq!(config.peers_poll_secs, 2);
        assert_eq!(config.batch_max_size, 100);
        assert!((config.batch_max_delay_secs - 0.01).abs() < f64::EPSILON);
        assert_eq!(config.max_concurrent, 100);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.peers_ttl_secs, 3600);
        assert!(config.peers.is_none());
        assert!(!config.is_single_node());
        assert!(!config.is_join_mode());
        // Default is ephemeral (no data_dir) and localhost-only
        assert!(config.is_ephemeral());
        assert!(config.is_localhost_only());
        assert!(config.data_dir.is_none());
    }

    #[test]
    fn test_config_for_test() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();
        let config = Config::for_test(1, 50051, data_dir.clone());
        assert_eq!(config.listen_addr.port(), 50051);
        assert_eq!(config.bootstrap_expect(), 1);
        assert!(config.is_single_node());
        assert!(!config.is_ephemeral());

        // Verify node_id was written to file
        let node_id = config.node_id(&data_dir).expect("load node_id");
        assert_eq!(node_id, 1);
    }

    #[test]
    fn test_config_for_single_node() {
        let config = Config::for_single_node();
        assert_eq!(config.bootstrap_expect(), 1);
        assert!(config.is_single_node());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_cluster_size() {
        // Valid: cluster >= 2
        let config = Config { cluster: Some(3), ..Config::default() };
        assert!(config.validate().is_ok());

        let config = Config { cluster: Some(5), ..Config::default() };
        assert!(config.validate().is_ok());

        // Invalid: cluster < 2
        let config = Config { cluster: Some(1), ..Config::default() };
        assert!(config.validate().is_err());

        let config = Config { cluster: Some(0), ..Config::default() };
        assert!(config.validate().is_err());

        // Valid: no cluster specified (uses default 3)
        let config = Config { cluster: None, ..Config::default() };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_join_mode() {
        let config = Config { join: true, cluster: None, ..Config::default() };
        assert!(config.validate().is_ok());
        assert!(config.is_join_mode());
        assert!(!config.is_single_node());
        assert_eq!(config.bootstrap_expect(), 0);
    }

    #[test]
    fn test_config_single_mode() {
        let config = Config { single: true, cluster: None, ..Config::default() };
        assert!(config.validate().is_ok());
        assert!(config.is_single_node());
        assert!(!config.is_join_mode());
        assert_eq!(config.bootstrap_expect(), 1);
    }

    #[test]
    fn test_is_localhost_only() {
        // Localhost addresses
        let config =
            Config { listen_addr: "127.0.0.1:50051".parse().unwrap(), ..Config::default() };
        assert!(config.is_localhost_only());

        let config = Config { listen_addr: "[::1]:50051".parse().unwrap(), ..Config::default() };
        assert!(config.is_localhost_only());

        // Non-localhost addresses
        let config = Config { listen_addr: "0.0.0.0:50051".parse().unwrap(), ..Config::default() };
        assert!(!config.is_localhost_only());

        let config =
            Config { listen_addr: "192.168.1.1:50051".parse().unwrap(), ..Config::default() };
        assert!(!config.is_localhost_only());
    }

    #[test]
    fn test_is_ephemeral() {
        // Ephemeral when data_dir is None
        let config = Config::default();
        assert!(config.is_ephemeral());

        // Not ephemeral when data_dir is set
        let config = Config { data_dir: Some(PathBuf::from("/data")), ..Config::default() };
        assert!(!config.is_ephemeral());
    }

    #[test]
    fn test_resolve_data_dir_with_configured_path() {
        let config = Config { data_dir: Some(PathBuf::from("/data/ledger")), ..Config::default() };
        let resolved = config.resolve_data_dir().expect("resolve data_dir");
        assert_eq!(resolved, PathBuf::from("/data/ledger"));
    }

    #[test]
    fn test_resolve_data_dir_ephemeral() {
        let config = Config::default();
        assert!(config.is_ephemeral());

        let resolved = config.resolve_data_dir().expect("resolve data_dir");

        // Should be in temp directory with ledger- prefix
        assert!(resolved.starts_with(std::env::temp_dir()));
        assert!(resolved.file_name().unwrap().to_str().unwrap().starts_with("ledger-"));
        assert!(resolved.exists());

        // Cleanup
        std::fs::remove_dir_all(&resolved).ok();
    }

    // === Builder API Tests (TDD) ===

    #[test]
    fn test_config_builder_with_defaults() {
        // Builder with all defaults should have same behavior as Default::default()
        let from_builder = Config::builder().build();
        let from_default = Config::default();

        assert_eq!(from_builder.listen_addr, from_default.listen_addr);
        assert_eq!(from_builder.metrics_addr, from_default.metrics_addr);
        assert_eq!(from_builder.data_dir, from_default.data_dir);
        // Note: cluster field differs (builder=None, default=Some(3)), but bootstrap_expect() is
        // same
        assert_eq!(from_builder.bootstrap_expect(), from_default.bootstrap_expect());
        assert_eq!(from_builder.bootstrap_expect(), 3); // Both default to 3-node cluster
        assert_eq!(from_builder.peers, from_default.peers);
        assert_eq!(from_builder.peers_ttl_secs, from_default.peers_ttl_secs);
        assert_eq!(from_builder.peers_timeout_secs, from_default.peers_timeout_secs);
        assert_eq!(from_builder.peers_poll_secs, from_default.peers_poll_secs);
        assert_eq!(from_builder.batch_max_size, from_default.batch_max_size);
        assert!(
            (from_builder.batch_max_delay_secs - from_default.batch_max_delay_secs).abs()
                < f64::EPSILON
        );
        assert_eq!(from_builder.max_concurrent, from_default.max_concurrent);
        assert_eq!(from_builder.timeout_secs, from_default.timeout_secs);
    }

    #[test]
    fn test_config_builder_with_custom_values() {
        let config = Config::builder()
            .listen_addr("127.0.0.1:9999".parse().unwrap())
            .metrics_addr("127.0.0.1:9090".parse().unwrap())
            .data_dir(PathBuf::from("/custom/data"))
            .cluster(5)
            .peers("ledger.example.com".to_string())
            .peers_ttl_secs(7200)
            .peers_timeout_secs(120)
            .peers_poll_secs(5)
            .batch_max_size(500)
            .batch_max_delay_secs(0.05)
            .max_concurrent(200)
            .timeout_secs(60)
            .build();

        assert_eq!(config.listen_addr.port(), 9999);
        assert!(config.metrics_addr.is_some());
        assert_eq!(config.data_dir, Some(PathBuf::from("/custom/data")));
        assert_eq!(config.cluster, Some(5));
        assert_eq!(config.bootstrap_expect(), 5);
        assert_eq!(config.peers, Some("ledger.example.com".to_string()));
        assert_eq!(config.peers_ttl_secs, 7200);
        assert_eq!(config.peers_timeout_secs, 120);
        assert_eq!(config.peers_poll_secs, 5);
        assert_eq!(config.batch_max_size, 500);
        assert!((config.batch_max_delay_secs - 0.05).abs() < f64::EPSILON);
        assert_eq!(config.max_concurrent, 200);
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn test_config_builder_cluster_mode() {
        // Builder can create cluster configs
        let config = Config::builder().cluster(5).build();

        assert!(!config.is_single_node());
        assert!(!config.is_join_mode());
        assert_eq!(config.bootstrap_expect(), 5);
        assert!(config.validate().is_ok());
    }

    // === Peers Detection Tests ===

    #[test]
    fn test_peers_as_dns_domain() {
        // DNS domains (no path separators, not .json)
        let config = Config { peers: Some("ledger.example.com".to_string()), ..Config::default() };
        assert_eq!(config.peers_as_dns_domain(), Some("ledger.example.com"));
        assert!(config.peers_as_file_path().is_none());

        let config = Config {
            peers: Some("ledger.default.svc.cluster.local".to_string()),
            ..Config::default()
        };
        assert_eq!(config.peers_as_dns_domain(), Some("ledger.default.svc.cluster.local"));
        assert!(config.peers_as_file_path().is_none());
    }

    #[test]
    fn test_peers_as_file_path_with_slash() {
        // File paths with forward slash
        let config =
            Config { peers: Some("/var/lib/ledger/peers.json".to_string()), ..Config::default() };
        assert_eq!(config.peers_as_file_path(), Some("/var/lib/ledger/peers.json"));
        assert!(config.peers_as_dns_domain().is_none());

        // Relative path
        let config = Config { peers: Some("./peers.json".to_string()), ..Config::default() };
        assert_eq!(config.peers_as_file_path(), Some("./peers.json"));
        assert!(config.peers_as_dns_domain().is_none());
    }

    #[test]
    fn test_peers_as_file_path_with_backslash() {
        // Windows-style paths
        let config =
            Config { peers: Some("C:\\ledger\\peers.json".to_string()), ..Config::default() };
        assert_eq!(config.peers_as_file_path(), Some("C:\\ledger\\peers.json"));
        assert!(config.peers_as_dns_domain().is_none());
    }

    #[test]
    fn test_peers_as_file_path_json_extension() {
        // Just filename with .json extension (treated as file)
        let config = Config { peers: Some("peers.json".to_string()), ..Config::default() };
        assert_eq!(config.peers_as_file_path(), Some("peers.json"));
        assert!(config.peers_as_dns_domain().is_none());
    }

    #[test]
    fn test_peers_none() {
        let config = Config::default();
        assert!(config.peers_as_dns_domain().is_none());
        assert!(config.peers_as_file_path().is_none());
    }
}
