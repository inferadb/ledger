//! Server configuration.
//!
//! Configuration is loaded from command-line arguments or environment variables.
//! CLI arguments take precedence over environment variables.

// The schemars `JsonSchema` derive macro internally uses `.unwrap()`.
#![allow(clippy::disallowed_methods)]

use std::{net::SocketAddr, path::PathBuf};

use bon::Builder;
use clap::Parser;
pub use inferadb_ledger_types::config::OtelConfig;
use schemars::JsonSchema;
use serde::Deserialize;
use snafu::Snafu;

use crate::node_id;

/// Configuration for request logging.
///
/// # Environment Variables
///
/// ```bash
/// INFERADB__LEDGER__LOGGING__OTEL__ENDPOINT=http://localhost:4317
/// ```
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, bon::Builder)]
#[builder(derive(Debug))]
pub struct LoggingConfig {
    /// OpenTelemetry/OTLP export configuration.
    #[serde(default)]
    #[builder(default)]
    pub otel: OtelConfig,
}

impl LoggingConfig {
    /// Creates a configuration with test-suitable values.
    #[cfg(test)]
    pub fn for_test() -> Self {
        Self { otel: OtelConfig::for_test() }
    }

    /// Validates logging configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if OTEL configuration is invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.otel.validate()?;
        Ok(())
    }
}

/// Log output format.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable text format (default for development).
    #[default]
    Text,
    /// JSON structured logging (recommended for production).
    Json,
    /// Automatically detect: JSON for non-TTY stdout, text otherwise.
    Auto,
}

/// Configuration for the InferaDB Ledger server.
///
/// Configuration can be provided via command-line arguments or environment variables.
/// CLI arguments take precedence over environment variables.
///
/// # CLI Example
///
/// ```bash
/// # Start a node (listens for connections)
/// inferadb-ledger --data /data
///
/// # Start a node with seed peers
/// inferadb-ledger --data /data --join node1:9090,node2:9090
///
/// # Initialize the cluster (one-time, after nodes are running)
/// inferadb-ledger init --host node1:9090
/// ```
///
/// # Environment Variables
///
/// All options can be set via environment variables with the `INFERADB__LEDGER__` prefix:
///
/// ```bash
/// INFERADB__LEDGER__LISTEN=0.0.0.0:9000 \
/// INFERADB__LEDGER__DATA=/data/ledger \
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
///     .listen("0.0.0.0:9000".parse().unwrap())
///     .data_dir(PathBuf::from("/data/ledger"))
///     .build();
/// ```
#[derive(Debug, Clone, Deserialize, JsonSchema, Builder, Parser)]
#[builder(derive(Debug))]
pub struct Config {
    /// TCP address to listen on for gRPC.
    ///
    /// When set, the server binds a TCP listener on this address.
    /// Set to `0.0.0.0:50051` to accept remote connections.
    /// At least one of `--listen` or `--socket` must be specified.
    #[arg(long = "listen", env = "INFERADB__LEDGER__LISTEN")]
    #[serde(default)]
    pub listen: Option<SocketAddr>,

    /// Address to expose Prometheus metrics. Disabled if not set.
    #[arg(long = "metrics", env = "INFERADB__LEDGER__METRICS")]
    #[serde(default)]
    pub metrics_addr: Option<SocketAddr>,

    /// Log output format.
    ///
    /// - `text`: Human-readable format (default for development)
    /// - `json`: JSON structured logging (recommended for production)
    /// - `auto`: JSON for non-TTY stdout, text otherwise
    #[arg(
        long = "log-format",
        env = "INFERADB__LEDGER__LOG_FORMAT",
        value_enum,
        default_value = "auto"
    )]
    #[serde(default)]
    #[builder(default)]
    pub log_format: LogFormat,

    /// Data directory for Raft logs and snapshots.
    ///
    /// If not specified, the server runs in ephemeral mode using a temporary
    /// directory. All data is lost on shutdown.
    #[arg(long = "data", env = "INFERADB__LEDGER__DATA")]
    #[serde(default)]
    pub data_dir: Option<PathBuf>,

    // === Region ===
    /// Geographic region this node belongs to.
    ///
    /// Determines which Raft groups this node participates in:
    /// - Non-protected regions (global, us-east-va, us-west-or): all nodes join.
    /// - Protected regions: only nodes tagged with that region join.
    ///
    /// Use lowercase-hyphen format: `us-east-va`, `ie-east-dublin`, `global`.
    /// Region is immutable after registration.
    #[arg(long = "region", env = "INFERADB__LEDGER__REGION", default_value = "global")]
    #[serde(default = "default_region")]
    #[builder(default = default_region())]
    pub region: inferadb_ledger_types::Region,

    // === Networking ===
    /// Address other nodes should use to reach this node.
    ///
    /// Overrides `--listen` for inter-node communication. Required in
    /// NAT/container environments where the listen address (e.g., `0.0.0.0`)
    /// isn't routable from other nodes. When unset, defaults to `--listen`.
    #[arg(long = "advertise", env = "INFERADB__LEDGER__ADVERTISE")]
    #[serde(default)]
    pub advertise: Option<String>,

    // === Seed Discovery ===
    /// Comma-separated list of seed addresses for cluster discovery.
    ///
    /// Used during initial bootstrap to find other nodes. NOT an exhaustive
    /// peer list — just "introductions." One address is sufficient (e.g.,
    /// `--join=node1:9090`). On restart, persisted Raft membership is used;
    /// `--join` is ignored unless all persisted peers are unreachable.
    /// Optional — if omitted, the node listens for incoming connections.
    #[arg(long = "join", env = "INFERADB__LEDGER__JOIN", value_delimiter = ',')]
    #[serde(default)]
    pub join: Option<Vec<String>>,

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

    // === Raft Consensus ===
    /// Raft consensus timing and tuning configuration.
    ///
    /// Controls heartbeat interval, election timeouts, snapshot threshold,
    /// and other Raft protocol parameters. When absent, uses production defaults
    /// (100ms heartbeat, 300-500ms election timeout).
    #[arg(skip)]
    #[serde(default)]
    pub raft: Option<inferadb_ledger_types::config::RaftConfig>,

    // === Logging ===
    /// Logging configuration for comprehensive request logging.
    ///
    /// Emits a single JSON log line per request with 50+ contextual
    /// fields for debugging and observability.
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub logging: LoggingConfig,

    // === Backup ===
    /// Backup configuration for automated and on-demand backups.
    ///
    /// When configured, enables `CreateBackup`, `ListBackups`, and
    /// `RestoreBackup` RPCs on the admin service, plus an automated
    /// backup job that runs on the leader node.
    #[arg(skip)]
    #[serde(default)]
    pub backup: Option<inferadb_ledger_types::config::BackupConfig>,

    // === Events ===
    /// Event logging configuration for organization-scoped audit trails.
    ///
    /// Controls retention TTL, detail size limits, per-scope enable flags,
    /// snapshot limits, and external ingestion settings.
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub events: inferadb_ledger_types::events::EventConfig,

    // === JWT ===
    /// JWT token configuration (TTLs, issuer, clock skew, key rotation grace).
    ///
    /// Controls session and vault token lifetimes, signing key rotation
    /// grace periods, and clock skew tolerance.
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub jwt: inferadb_ledger_types::config::JwtConfig,

    // === Email Blinding Key ===
    /// Hex-encoded 32-byte key for HMAC-based email hashing.
    ///
    /// Enables the onboarding flow (email verification, registration).
    /// When absent, onboarding RPCs return FAILED_PRECONDITION.
    ///
    /// Generate with: `openssl rand -hex 32`
    #[arg(long = "email-blinding-key", env = "INFERADB__LEDGER__EMAIL_BLINDING_KEY")]
    #[serde(default)]
    pub email_blinding_key: Option<String>,

    // === Saga Orchestrator ===
    /// Saga orchestrator configuration for cross-organization operations.
    ///
    /// Controls the poll interval for pending saga discovery and execution.
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub saga: inferadb_ledger_types::config::SagaConfig,

    // === Orphan Cleanup ===
    /// Orphan cleanup configuration for removing stale membership records.
    ///
    /// Controls the interval for scanning and removing memberships that
    /// reference deleted users.
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub cleanup: inferadb_ledger_types::config::CleanupConfig,

    // === Integrity Scrubber ===
    /// Integrity scrubber configuration for background page checksum verification.
    ///
    /// Controls the scrub interval, percentage of pages per cycle, and
    /// full scan target period.
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub integrity: inferadb_ledger_types::config::IntegrityConfig,

    /// Tiered snapshot storage configuration.
    ///
    /// Controls snapshot distribution across hot (local SSD) and warm (S3/GCS/Azure) tiers.
    /// When `warm_url` is absent, operates in local-only mode with zero overhead.
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub tiered_storage: inferadb_ledger_types::config::TieredStorageConfig,

    // === Health Checks ===
    /// Dependency health check configuration for readiness probes.
    ///
    /// Controls timeouts, cache TTL, and Raft lag thresholds for disk
    /// writability, peer reachability, and consensus lag checks.
    #[arg(skip)]
    #[serde(default)]
    pub health_check: Option<inferadb_ledger_types::config::HealthCheckConfig>,

    // === gRPC Reflection ===
    /// Enable gRPC server reflection for service discovery.
    ///
    /// When enabled, tools like `grpcurl` can introspect the server's services
    /// without requiring proto files on the client side. Disabled by default
    /// to reduce the attack surface in production.
    #[arg(long = "enable-grpc-reflection", env = "INFERADB__LEDGER__ENABLE_GRPC_REFLECTION")]
    #[serde(default)]
    #[builder(default)]
    pub enable_grpc_reflection: bool,

    // === Rate Limiting ===
    /// Rate limit configuration for per-client and per-organization request throttling.
    ///
    /// Controls token bucket capacities and refill rates. When absent, uses
    /// hardcoded defaults (client burst=100, org burst=1000).
    #[arg(skip)]
    #[serde(default)]
    pub rate_limit: Option<inferadb_ledger_types::config::RateLimitConfig>,

    // === Token Maintenance ===
    /// Token maintenance job interval in seconds (default: 300 = 5 minutes).
    ///
    /// Controls how often expired refresh tokens are cleaned up and rotated
    /// signing keys past their grace period are transitioned to revoked.
    #[arg(skip = default_token_maintenance_interval_secs())]
    #[serde(default = "default_token_maintenance_interval_secs")]
    #[builder(default = default_token_maintenance_interval_secs())]
    pub token_maintenance_interval_secs: u64,

    // === Unix Domain Socket ===
    /// Path to a Unix domain socket for gRPC connections.
    ///
    /// When set alongside `--listen`, the server binds both TCP and UDS
    /// simultaneously. When set alone, the server listens on UDS only.
    /// At least one of `--listen` or `--socket` must be specified.
    #[arg(long = "socket", env = "INFERADB__LEDGER__SOCKET")]
    #[serde(default)]
    pub socket: Option<PathBuf>,
}

// Default value functions
fn default_region() -> inferadb_ledger_types::Region {
    inferadb_ledger_types::Region::GLOBAL
}

fn default_token_maintenance_interval_secs() -> u64 {
    300 // 5 minutes
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
            listen: None,
            metrics_addr: None,
            log_format: LogFormat::default(),
            data_dir: None,
            region: default_region(),
            advertise: None,
            join: None,
            max_concurrent: default_max_concurrent(),
            timeout_secs: default_timeout_secs(),
            raft: None,
            logging: LoggingConfig::default(),
            backup: None,
            events: inferadb_ledger_types::events::EventConfig::default(),
            jwt: inferadb_ledger_types::config::JwtConfig::default(),
            saga: inferadb_ledger_types::config::SagaConfig::default(),
            cleanup: inferadb_ledger_types::config::CleanupConfig::default(),
            integrity: inferadb_ledger_types::config::IntegrityConfig::default(),
            tiered_storage: inferadb_ledger_types::config::TieredStorageConfig::default(),
            health_check: None,
            enable_grpc_reflection: false,
            rate_limit: None,
            token_maintenance_interval_secs: default_token_maintenance_interval_secs(),
            email_blinding_key: None,
            socket: None,
        }
    }
}

impl Config {
    /// Creates a configuration with test-suitable values (deterministic ID).
    ///
    /// Writes the given `node_id` to the data directory for deterministic
    /// node IDs in tests.
    #[cfg(test)]
    #[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
    pub fn for_test(node_id: u64, port: u16, data_dir: PathBuf) -> Self {
        // Write the node_id file for deterministic test behavior
        node_id::write_node_id(&data_dir, node_id).expect("failed to write test node_id");

        Self {
            listen: Some(format!("127.0.0.1:{}", port).parse().unwrap()),
            metrics_addr: None,
            data_dir: Some(data_dir),
            logging: LoggingConfig::for_test(),
            ..Self::default()
        }
    }

    /// Returns true if the server is listening only on localhost.
    ///
    /// When true, only local connections are accepted. Remote clients
    /// will not be able to connect.
    pub fn is_localhost_only(&self) -> bool {
        self.listen.is_some_and(|a| a.ip().is_loopback())
    }

    /// Returns the address other nodes should use to reach this node.
    ///
    /// Uses `--advertise` when set, then falls back to the Unix socket path
    /// (if configured), and finally to the TCP listen address.
    pub fn advertise_addr(&self) -> String {
        if let Some(ref addr) = self.advertise {
            return addr.clone();
        }
        if let Some(ref path) = self.socket {
            return path.display().to_string();
        }
        self.listen.map_or_else(String::new, |a| a.to_string())
    }

    /// Returns true if the server is running in ephemeral mode.
    ///
    /// In ephemeral mode, data is stored in a temporary directory and
    /// will be lost when the server shuts down.
    pub fn is_ephemeral(&self) -> bool {
        self.data_dir.is_none()
    }

    /// Resolves the data directory, creating an ephemeral one if needed.
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

    /// Returns the node ID for this server.
    ///
    /// Loads an existing node ID from `{data_dir}/node_id`, or generates a new
    /// Snowflake ID and persists it. The ID is reused across restarts to maintain
    /// cluster identity.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - The resolved data directory (from `resolve_data_dir()`)
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if the node ID file cannot be read, parsed,
    /// or a new ID cannot be generated or written.
    pub fn node_id(&self, data_dir: &std::path::Path) -> Result<u64, ConfigError> {
        node_id::load_or_generate_node_id(data_dir).map_err(|e| ConfigError::Validation {
            message: format!("failed to load or generate node ID: {}", e),
        })
    }

    /// Validates the configuration.
    ///
    /// Validates logging, events, saga, cleanup, integrity, and tiered storage
    /// configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if any sub-configuration is invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.logging.validate()?;
        self.events.validate()?;
        self.saga.validate().map_err(|e| ConfigError::Validation { message: e.to_string() })?;
        self.cleanup.validate().map_err(|e| ConfigError::Validation { message: e.to_string() })?;
        self.integrity
            .validate()
            .map_err(|e| ConfigError::Validation { message: e.to_string() })?;
        self.tiered_storage
            .validate()
            .map_err(|e| ConfigError::Validation { message: e.to_string() })?;
        Ok(())
    }
}

/// Errors from configuration validation.
#[derive(Debug, Snafu)]
pub enum ConfigError {
    /// Configuration validation failed.
    #[snafu(display("invalid config: {message}"))]
    Validation {
        /// Error description.
        message: String,
    },
}

impl From<inferadb_ledger_types::config::ConfigError> for ConfigError {
    fn from(e: inferadb_ledger_types::config::ConfigError) -> Self {
        Self::Validation { message: e.to_string() }
    }
}

/// Command-line arguments for the ledger server.
#[derive(Debug, Parser)]
#[command(name = "inferadb-ledger")]
#[command(version)]
pub struct Cli {
    /// Subcommand to run. If omitted, starts the server.
    #[command(subcommand)]
    pub command: Option<CliCommand>,

    /// Server configuration (flattened so flags appear at top level).
    #[command(flatten)]
    pub config: Config,
}

/// CLI subcommands for configuration management.
#[derive(Debug, clap::Subcommand)]
pub enum CliCommand {
    /// Configuration management utilities.
    Config {
        /// Configuration action to perform.
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Initialize a new cluster (one-time operation per cluster lifetime).
    ///
    /// Sends a Bootstrap RPC to the target node, creating the GLOBAL shard
    /// with all discovered peers as voters. Returns the cluster ID.
    Init {
        /// Address of the node to bootstrap (e.g., "node1:9090").
        #[arg(long = "host")]
        host: String,
    },
}

/// Configuration subcommand actions.
#[derive(Debug, clap::Subcommand)]
pub enum ConfigAction {
    /// Output JSON Schema for the runtime configuration.
    ///
    /// The schema describes the structure of `RuntimeConfig`, which is
    /// used by the `UpdateConfig` RPC. Use this schema for IDE
    /// autocomplete and external validation of config payloads.
    Schema,
}

/// Generates JSON Schema for the runtime configuration.
///
/// Uses schemars to derive the schema from `RuntimeConfig` and its
/// nested types. All fields that derive `JsonSchema` are included.
#[must_use]
pub fn generate_runtime_config_schema() -> String {
    let schema = schemars::schema_for!(inferadb_ledger_types::config::RuntimeConfig);
    serde_json::to_string_pretty(&schema).unwrap_or_else(|e| format!("{{\"error\": \"{e}\"}}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_types::config::OtelTransport;

    use super::*;

    #[test]
    fn default_config_has_expected_field_values() {
        let config = Config::default();
        assert_eq!(config.max_concurrent, 100);
        assert_eq!(config.timeout_secs, 30);
        assert!(config.join.is_none());
        assert!(config.data_dir.is_none());
    }

    #[test]
    fn default_config_is_ephemeral_and_not_localhost() {
        let config = Config::default();
        assert!(config.is_ephemeral());
        // listen is None by default, so is_localhost_only returns false
        assert!(!config.is_localhost_only());
    }

    #[test]
    fn test_config_for_test() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();
        let config = Config::for_test(1, 50051, data_dir.clone());
        assert_eq!(config.listen.unwrap().port(), 50051);
        assert!(!config.is_ephemeral());

        // Verify node_id was written to file
        let node_id = config.node_id(&data_dir).expect("load node_id");
        assert_eq!(node_id, 1);
    }

    #[test]
    fn validate_accepts_default_config() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn is_localhost_only_true_for_loopback_addresses() {
        let config =
            Config { listen: Some("127.0.0.1:50051".parse().unwrap()), ..Config::default() };
        assert!(config.is_localhost_only());

        let config = Config { listen: Some("[::1]:50051".parse().unwrap()), ..Config::default() };
        assert!(config.is_localhost_only());
    }

    #[test]
    fn is_localhost_only_false_for_non_loopback_addresses() {
        let config = Config { listen: Some("0.0.0.0:50051".parse().unwrap()), ..Config::default() };
        assert!(!config.is_localhost_only());

        let config =
            Config { listen: Some("192.168.1.1:50051".parse().unwrap()), ..Config::default() };
        assert!(!config.is_localhost_only());
    }

    #[test]
    fn is_localhost_only_false_when_listen_is_none() {
        let config = Config { listen: None, ..Config::default() };
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

        assert_eq!(from_builder.listen, from_default.listen);
        assert_eq!(from_builder.metrics_addr, from_default.metrics_addr);
        assert_eq!(from_builder.data_dir, from_default.data_dir);
        assert_eq!(from_builder.join, from_default.join);
        assert_eq!(from_builder.max_concurrent, from_default.max_concurrent);
        assert_eq!(from_builder.timeout_secs, from_default.timeout_secs);
    }

    #[test]
    fn test_config_builder_with_custom_values() {
        let config = Config::builder()
            .listen("127.0.0.1:9999".parse().unwrap())
            .metrics_addr("127.0.0.1:9090".parse().unwrap())
            .data_dir(PathBuf::from("/custom/data"))
            .join(vec!["node1:9090".to_string(), "node2:9090".to_string()])
            .max_concurrent(200)
            .timeout_secs(60)
            .build();

        assert_eq!(config.listen.unwrap().port(), 9999);
        assert!(config.metrics_addr.is_some());
        assert_eq!(config.data_dir, Some(PathBuf::from("/custom/data")));
        assert_eq!(config.join, Some(vec!["node1:9090".to_string(), "node2:9090".to_string()]));
        assert_eq!(config.max_concurrent, 200);
        assert_eq!(config.timeout_secs, 60);
    }

    // === Logging Config Tests ===

    #[test]
    fn test_logging_config_defaults() {
        let config = LoggingConfig::default();
        assert!(!config.otel.enabled);
    }

    #[test]
    fn test_logging_config_for_test() {
        let config = LoggingConfig::for_test();
        // for_test uses OtelConfig::for_test() defaults (disabled for unit tests)
        assert!(!config.otel.enabled);
    }

    #[test]
    fn test_logging_config_validate() {
        let config = LoggingConfig::default();
        assert!(config.validate().is_ok());
    }

    // === OTEL Config Tests ===

    #[test]
    fn test_otel_config_defaults() {
        let config = OtelConfig::default();
        assert!(!config.enabled);
        assert!(config.endpoint.is_none());
        assert_eq!(config.transport, OtelTransport::Grpc);
        assert_eq!(config.batch_size, 512);
        assert_eq!(config.batch_interval_ms, 5000);
        assert_eq!(config.timeout_ms, 10000);
        assert_eq!(config.shutdown_timeout_ms, 15000);
        assert!(config.trace_raft_rpcs); // default is true
    }

    #[test]
    fn test_otel_config_for_test() {
        let config = OtelConfig::for_test();
        assert!(!config.enabled); // Disabled by default for tests
    }

    #[test]
    fn test_otel_config_validate_disabled() {
        // Disabled config is valid even without endpoint
        let config = OtelConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_otel_config_validate_enabled_requires_endpoint() {
        let err = OtelConfig::builder().enabled(true).build().unwrap_err();
        assert!(err.to_string().contains("endpoint"));
        assert!(err.to_string().contains("required"));
    }

    #[test]
    fn test_otel_config_validate_enabled_with_endpoint() {
        let config =
            OtelConfig::builder().enabled(true).endpoint("http://localhost:4317").build().unwrap();
        assert!(config.enabled);
    }

    #[test]
    fn test_otel_config_validate_batch_size() {
        let err = OtelConfig::builder().batch_size(0).build().unwrap_err();
        assert!(err.to_string().contains("batch_size"));
        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn test_otel_config_validate_batch_interval() {
        let err = OtelConfig::builder().batch_interval_ms(0).build().unwrap_err();
        assert!(err.to_string().contains("batch_interval_ms"));
    }

    #[test]
    fn test_otel_config_validate_timeout() {
        let err = OtelConfig::builder().timeout_ms(0).build().unwrap_err();
        assert!(err.to_string().contains("timeout_ms"));
    }

    #[test]
    fn test_otel_config_validate_shutdown_timeout() {
        let err = OtelConfig::builder().shutdown_timeout_ms(0).build().unwrap_err();
        assert!(err.to_string().contains("shutdown_timeout_ms"));
    }

    #[test]
    fn test_otel_config_builder() {
        let config = OtelConfig::builder()
            .enabled(true)
            .endpoint("http://localhost:4317")
            .transport(OtelTransport::Http)
            .batch_size(256)
            .batch_interval_ms(2500)
            .timeout_ms(5000)
            .shutdown_timeout_ms(7500)
            .trace_raft_rpcs(false)
            .build()
            .unwrap();

        assert!(config.enabled);
        assert_eq!(config.endpoint, Some("http://localhost:4317".to_string()));
        assert_eq!(config.transport, OtelTransport::Http);
        assert_eq!(config.batch_size, 256);
        assert_eq!(config.batch_interval_ms, 2500);
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.shutdown_timeout_ms, 7500);
        assert!(!config.trace_raft_rpcs);
    }

    #[test]
    fn test_otel_transport_default() {
        assert_eq!(OtelTransport::default(), OtelTransport::Grpc);
    }

    #[test]
    fn test_logging_config_validate_includes_otel() {
        // Invalid OTEL config (enabled without endpoint) should fail validation
        let config = LoggingConfig { otel: OtelConfig { enabled: true, ..OtelConfig::default() } };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("endpoint"));
    }

    // === VIP Config Tests ===

    // =========================================================================
    // Schema & example generation tests
    // =========================================================================

    #[test]
    fn test_generate_runtime_config_schema_is_valid_json() {
        let schema_json = generate_runtime_config_schema();
        let value: serde_json::Value = serde_json::from_str(&schema_json).unwrap();
        assert!(value.get("$schema").is_some());
        assert_eq!(value.get("title").and_then(|v| v.as_str()), Some("RuntimeConfig"));
    }

    #[test]
    fn test_generate_runtime_config_schema_has_definitions() {
        let schema_json = generate_runtime_config_schema();
        let value: serde_json::Value = serde_json::from_str(&schema_json).unwrap();
        // Should have $defs for nested config types.
        let defs = value.get("$defs").and_then(|v| v.as_object()).unwrap();
        assert!(defs.contains_key("RateLimitConfig"), "Missing RateLimitConfig definition");
        assert!(defs.contains_key("HotKeyConfig"), "Missing HotKeyConfig definition");
        assert!(defs.contains_key("ValidationConfig"), "Missing ValidationConfig definition");
    }

    #[test]
    fn test_default_config_region_is_global() {
        let config = Config::default();
        assert_eq!(config.region, inferadb_ledger_types::Region::GLOBAL);
    }

    #[test]
    fn test_config_builder_region() {
        let config =
            Config::builder().region(inferadb_ledger_types::Region::IE_EAST_DUBLIN).build();
        assert_eq!(config.region, inferadb_ledger_types::Region::IE_EAST_DUBLIN);
    }

    #[test]
    fn test_config_region_from_serde() {
        let json = r#"{"listen": "127.0.0.1:50051", "region": "ie-east-dublin"}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.region, inferadb_ledger_types::Region::IE_EAST_DUBLIN);
    }

    #[test]
    fn test_config_region_serde_default_is_global() {
        let json = r#"{"listen": "127.0.0.1:50051"}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.region, inferadb_ledger_types::Region::GLOBAL);
    }
}
