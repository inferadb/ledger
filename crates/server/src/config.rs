//! Server configuration.
//!
//! Configuration is loaded from command-line arguments or environment variables.
//! CLI arguments take precedence over environment variables.

use std::{net::SocketAddr, path::PathBuf};

use bon::Builder;
use clap::Parser;
use serde::Deserialize;
use snafu::Snafu;

use crate::node_id;

/// Configuration for wide events sampling.
///
/// Controls tail sampling behavior: which events are logged based on
/// outcome, latency, and namespace priority.
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[builder(derive(Debug))]
pub struct WideEventsSamplingConfig {
    /// Sample rate for error outcomes (0.0-1.0). Default: 1.0 (100%).
    #[serde(default = "default_error_rate")]
    #[builder(default = default_error_rate())]
    pub error_rate: f64,

    /// Sample rate for slow requests (0.0-1.0). Default: 1.0 (100%).
    #[serde(default = "default_slow_rate")]
    #[builder(default = default_slow_rate())]
    pub slow_rate: f64,

    /// Sample rate for VIP namespaces (0.0-1.0). Default: 0.5 (50%).
    #[serde(default = "default_vip_rate")]
    #[builder(default = default_vip_rate())]
    pub vip_rate: f64,

    /// Sample rate for successful write operations (0.0-1.0). Default: 0.1 (10%).
    #[serde(default = "default_write_rate")]
    #[builder(default = default_write_rate())]
    pub write_rate: f64,

    /// Sample rate for successful read operations (0.0-1.0). Default: 0.01 (1%).
    #[serde(default = "default_read_rate")]
    #[builder(default = default_read_rate())]
    pub read_rate: f64,

    /// Threshold for slow read operations, in milliseconds. Default: 10.0.
    #[serde(default = "default_slow_threshold_read_ms")]
    #[builder(default = default_slow_threshold_read_ms())]
    pub slow_threshold_read_ms: f64,

    /// Threshold for slow write operations, in milliseconds. Default: 100.0.
    #[serde(default = "default_slow_threshold_write_ms")]
    #[builder(default = default_slow_threshold_write_ms())]
    pub slow_threshold_write_ms: f64,

    /// Threshold for slow admin operations, in milliseconds. Default: 1000.0.
    #[serde(default = "default_slow_threshold_admin_ms")]
    #[builder(default = default_slow_threshold_admin_ms())]
    pub slow_threshold_admin_ms: f64,
}

impl Default for WideEventsSamplingConfig {
    fn default() -> Self {
        Self {
            error_rate: default_error_rate(),
            slow_rate: default_slow_rate(),
            vip_rate: default_vip_rate(),
            write_rate: default_write_rate(),
            read_rate: default_read_rate(),
            slow_threshold_read_ms: default_slow_threshold_read_ms(),
            slow_threshold_write_ms: default_slow_threshold_write_ms(),
            slow_threshold_admin_ms: default_slow_threshold_admin_ms(),
        }
    }
}

impl WideEventsSamplingConfig {
    /// Create a disabled sampling config (samples nothing except errors).
    #[allow(dead_code)] // reserved for future use when wide events can be selectively disabled
    pub fn disabled() -> Self {
        Self {
            error_rate: 1.0,
            slow_rate: 0.0,
            vip_rate: 0.0,
            write_rate: 0.0,
            read_rate: 0.0,
            slow_threshold_read_ms: f64::MAX,
            slow_threshold_write_ms: f64::MAX,
            slow_threshold_admin_ms: f64::MAX,
        }
    }

    /// Create a config for testing (samples everything).
    pub fn for_test() -> Self {
        Self {
            error_rate: 1.0,
            slow_rate: 1.0,
            vip_rate: 1.0,
            write_rate: 1.0,
            read_rate: 1.0,
            slow_threshold_read_ms: default_slow_threshold_read_ms(),
            slow_threshold_write_ms: default_slow_threshold_write_ms(),
            slow_threshold_admin_ms: default_slow_threshold_admin_ms(),
        }
    }

    /// Validate sampling configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Validate rates are in range 0.0-1.0
        let rates = [
            ("error_rate", self.error_rate),
            ("slow_rate", self.slow_rate),
            ("vip_rate", self.vip_rate),
            ("write_rate", self.write_rate),
            ("read_rate", self.read_rate),
        ];

        for (name, rate) in rates {
            if !(0.0..=1.0).contains(&rate) {
                return Err(ConfigError::Validation {
                    message: format!(
                        "wide_events.sampling.{} must be between 0.0 and 1.0, got {}",
                        name, rate
                    ),
                });
            }
        }

        // Validate thresholds are positive
        let thresholds = [
            ("slow_threshold_read_ms", self.slow_threshold_read_ms),
            ("slow_threshold_write_ms", self.slow_threshold_write_ms),
            ("slow_threshold_admin_ms", self.slow_threshold_admin_ms),
        ];

        for (name, threshold) in thresholds {
            if threshold <= 0.0 {
                return Err(ConfigError::Validation {
                    message: format!(
                        "wide_events.sampling.{} must be positive, got {}",
                        name, threshold
                    ),
                });
            }
        }

        Ok(())
    }
}

/// OTLP transport protocol.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtelTransport {
    /// gRPC transport (default, recommended for high-throughput).
    #[default]
    Grpc,
    /// HTTP transport (for environments where gRPC is blocked).
    Http,
}

/// Configuration for OpenTelemetry/OTLP trace export.
///
/// Enables exporting wide events as OpenTelemetry traces to observability
/// backends like Jaeger, Tempo, or Honeycomb.
///
/// # Environment Variables
///
/// ```bash
/// INFERADB__LEDGER__WIDE_EVENTS__OTEL__ENABLED=true
/// INFERADB__LEDGER__WIDE_EVENTS__OTEL__ENDPOINT=http://localhost:4317
/// INFERADB__LEDGER__WIDE_EVENTS__OTEL__TRANSPORT=grpc
/// ```
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[builder(derive(Debug))]
pub struct OtelConfig {
    /// Whether OTLP export is enabled. Default: false.
    #[serde(default)]
    #[builder(default)]
    pub enabled: bool,

    /// OTLP endpoint URL (e.g., "http://localhost:4317" for gRPC).
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Transport protocol. Default: gRPC.
    #[serde(default)]
    #[builder(default)]
    #[allow(dead_code)] // used in OTEL tracer provider initialization
    pub transport: OtelTransport,

    /// Batch size (flush when reached). Default: 512 spans.
    #[serde(default = "default_otel_batch_size")]
    #[builder(default = default_otel_batch_size())]
    pub batch_size: usize,

    /// Batch interval in milliseconds (flush when elapsed). Default: 5000ms.
    #[serde(default = "default_otel_batch_interval_ms")]
    #[builder(default = default_otel_batch_interval_ms())]
    pub batch_interval_ms: u64,

    /// Export timeout in milliseconds. Default: 10000ms.
    #[serde(default = "default_otel_timeout_ms")]
    #[builder(default = default_otel_timeout_ms())]
    pub timeout_ms: u64,

    /// Graceful shutdown timeout in milliseconds. Default: 15000ms.
    #[serde(default = "default_otel_shutdown_timeout_ms")]
    #[builder(default = default_otel_shutdown_timeout_ms())]
    pub shutdown_timeout_ms: u64,

    /// Whether to propagate trace context in Raft RPCs. Default: true.
    ///
    /// When enabled, trace context is injected into AppendEntries, Vote, and
    /// InstallSnapshot RPCs, enabling end-to-end distributed tracing across
    /// the Raft cluster. Disable for performance-critical deployments where
    /// the ~100 bytes overhead per RPC is unacceptable.
    #[serde(default = "default_trace_raft_rpcs")]
    #[builder(default = default_trace_raft_rpcs())]
    #[allow(dead_code)] // used in Raft network for trace context injection
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

impl OtelConfig {
    /// Create a config for testing with OTEL disabled.
    pub fn for_test() -> Self {
        Self::default()
    }

    /// Validate OTEL configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled && self.endpoint.is_none() {
            return Err(ConfigError::Validation {
                message: "wide_events.otel.endpoint is required when OTEL is enabled".to_string(),
            });
        }

        if self.batch_size == 0 {
            return Err(ConfigError::Validation {
                message: "wide_events.otel.batch_size must be positive".to_string(),
            });
        }

        if self.batch_interval_ms == 0 {
            return Err(ConfigError::Validation {
                message: "wide_events.otel.batch_interval_ms must be positive".to_string(),
            });
        }

        if self.timeout_ms == 0 {
            return Err(ConfigError::Validation {
                message: "wide_events.otel.timeout_ms must be positive".to_string(),
            });
        }

        if self.shutdown_timeout_ms == 0 {
            return Err(ConfigError::Validation {
                message: "wide_events.otel.shutdown_timeout_ms must be positive".to_string(),
            });
        }

        Ok(())
    }
}

fn default_otel_batch_size() -> usize {
    512
}
fn default_otel_batch_interval_ms() -> u64 {
    5000
}
fn default_otel_timeout_ms() -> u64 {
    10000
}
fn default_otel_shutdown_timeout_ms() -> u64 {
    15000
}

fn default_trace_raft_rpcs() -> bool {
    true
}

/// Configuration for dynamic VIP namespace discovery.
///
/// VIP namespaces receive elevated sampling rates. VIP status can be configured
/// statically via `vip_namespaces` list or dynamically discovered from the
/// `_system` namespace metadata.
///
/// # Environment Variables
///
/// ```bash
/// INFERADB__LEDGER__WIDE_EVENTS__VIP__DISCOVERY_ENABLED=true
/// INFERADB__LEDGER__WIDE_EVENTS__VIP__CACHE_TTL_SECS=60
/// INFERADB__LEDGER__WIDE_EVENTS__VIP__TAG_NAME=vip
/// ```
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[builder(derive(Debug))]
pub struct VipConfig {
    /// Whether dynamic VIP discovery from `_system` is enabled. Default: true.
    ///
    /// When enabled, the system queries `_system` namespace for entities with
    /// keys matching `vip:namespace:{namespace_id}` to determine VIP status.
    #[serde(default = "default_vip_discovery_enabled")]
    #[builder(default = default_vip_discovery_enabled())]
    #[allow(dead_code)] // used by VipCache for dynamic discovery
    pub discovery_enabled: bool,

    /// Cache TTL for VIP status lookups, in seconds. Default: 60.
    ///
    /// VIP status is cached locally to avoid querying `_system` on every request.
    /// The cache is refreshed asynchronously after TTL expires.
    #[serde(default = "default_vip_cache_ttl_secs")]
    #[builder(default = default_vip_cache_ttl_secs())]
    pub cache_ttl_secs: u64,

    /// Name of the metadata tag used to mark VIP namespaces. Default: "vip".
    ///
    /// VIP tags are stored as entities in `_system` with key format
    /// `{tag_name}:namespace:{namespace_id}`.
    #[serde(default = "default_vip_tag_name")]
    #[builder(default = default_vip_tag_name())]
    pub tag_name: String,
}

impl Default for VipConfig {
    fn default() -> Self {
        Self {
            discovery_enabled: default_vip_discovery_enabled(),
            cache_ttl_secs: default_vip_cache_ttl_secs(),
            tag_name: default_vip_tag_name(),
        }
    }
}

impl VipConfig {
    /// Create a config for testing with discovery disabled.
    pub fn for_test() -> Self {
        Self { discovery_enabled: false, cache_ttl_secs: 60, tag_name: "vip".to_string() }
    }

    /// Validate VIP configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.cache_ttl_secs == 0 {
            return Err(ConfigError::Validation {
                message: "wide_events.vip.cache_ttl_secs must be positive".to_string(),
            });
        }

        if self.tag_name.is_empty() {
            return Err(ConfigError::Validation {
                message: "wide_events.vip.tag_name cannot be empty".to_string(),
            });
        }

        Ok(())
    }
}

fn default_vip_discovery_enabled() -> bool {
    true
}
fn default_vip_cache_ttl_secs() -> u64 {
    60
}
fn default_vip_tag_name() -> String {
    "vip".to_string()
}

/// Configuration for wide events logging.
///
/// Wide events provide comprehensive request-level logging with 50+ contextual
/// fields for debugging and observability.
///
/// # Environment Variables
///
/// Configure via environment variables with the `INFERADB__LEDGER__WIDE_EVENTS__` prefix:
///
/// ```bash
/// INFERADB__LEDGER__WIDE_EVENTS__ENABLED=true
/// INFERADB__LEDGER__WIDE_EVENTS__SAMPLING__WRITE_RATE=0.1
/// INFERADB__LEDGER__WIDE_EVENTS__VIP_NAMESPACES=1,2,3
/// ```
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[builder(derive(Debug))]
pub struct WideEventsConfig {
    /// Whether wide events logging is enabled. Default: true.
    #[serde(default = "default_wide_events_enabled")]
    #[builder(default = default_wide_events_enabled())]
    #[allow(dead_code)] // reserved for when wide events can be disabled
    pub enabled: bool,

    /// Sampling configuration for wide events.
    #[serde(default)]
    #[builder(default)]
    pub sampling: WideEventsSamplingConfig,

    /// List of VIP namespace IDs with elevated sampling rates.
    /// These are static overrides that always receive VIP treatment.
    #[serde(default)]
    #[builder(default)]
    #[allow(dead_code)] // used by VipCache for static VIP override
    pub vip_namespaces: Vec<i64>,

    /// Dynamic VIP namespace discovery configuration.
    #[serde(default)]
    #[builder(default)]
    pub vip: VipConfig,

    /// OpenTelemetry/OTLP export configuration.
    #[serde(default)]
    #[builder(default)]
    pub otel: OtelConfig,
}

impl Default for WideEventsConfig {
    fn default() -> Self {
        Self {
            enabled: default_wide_events_enabled(),
            sampling: WideEventsSamplingConfig::default(),
            vip_namespaces: Vec::new(),
            vip: VipConfig::default(),
            otel: OtelConfig::default(),
        }
    }
}

impl WideEventsConfig {
    /// Create a config for testing with all sampling enabled.
    pub fn for_test() -> Self {
        Self {
            enabled: true,
            sampling: WideEventsSamplingConfig::for_test(),
            vip_namespaces: Vec::new(),
            vip: VipConfig::for_test(),
            otel: OtelConfig::for_test(),
        }
    }

    /// Validate wide events configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.sampling.validate()?;
        self.vip.validate()?;
        self.otel.validate()
    }
}

// Wide events sampling default value functions
fn default_error_rate() -> f64 {
    1.0
}
fn default_slow_rate() -> f64 {
    1.0
}
fn default_vip_rate() -> f64 {
    0.5
}
fn default_write_rate() -> f64 {
    0.1
}
fn default_read_rate() -> f64 {
    0.01
}
fn default_slow_threshold_read_ms() -> f64 {
    10.0
}
fn default_slow_threshold_write_ms() -> f64 {
    100.0
}
fn default_slow_threshold_admin_ms() -> f64 {
    1000.0
}
fn default_wide_events_enabled() -> bool {
    true
}

/// Log output format.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, clap::ValueEnum)]
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

    // === Wide Events ===
    /// Wide events configuration for comprehensive request logging.
    ///
    /// Wide events emit a single JSON log line per request with 50+ contextual
    /// fields for debugging and observability.
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub wide_events: WideEventsConfig,
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
            log_format: LogFormat::default(),
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
            wide_events: WideEventsConfig::default(),
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
            wide_events: WideEventsConfig::for_test(),
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
    /// Validates bootstrap mode settings and wide events configuration:
    /// - `--single`: Single-node deployment
    /// - `--join`: Join existing cluster
    /// - `--cluster N`: Coordinated bootstrap (N must be >= 2)
    /// - Wide events sampling rates must be 0.0-1.0
    /// - Wide events thresholds must be positive
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
        self.wide_events.validate()?;
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

    // === Wide Events Config Tests ===

    #[test]
    fn test_wide_events_sampling_config_defaults() {
        let config = WideEventsSamplingConfig::default();
        assert!((config.error_rate - 1.0).abs() < f64::EPSILON);
        assert!((config.slow_rate - 1.0).abs() < f64::EPSILON);
        assert!((config.vip_rate - 0.5).abs() < f64::EPSILON);
        assert!((config.write_rate - 0.1).abs() < f64::EPSILON);
        assert!((config.read_rate - 0.01).abs() < f64::EPSILON);
        assert!((config.slow_threshold_read_ms - 10.0).abs() < f64::EPSILON);
        assert!((config.slow_threshold_write_ms - 100.0).abs() < f64::EPSILON);
        assert!((config.slow_threshold_admin_ms - 1000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_wide_events_sampling_config_disabled() {
        let config = WideEventsSamplingConfig::disabled();
        assert!((config.error_rate - 1.0).abs() < f64::EPSILON); // Errors always sampled
        assert!((config.slow_rate - 0.0).abs() < f64::EPSILON);
        assert!((config.vip_rate - 0.0).abs() < f64::EPSILON);
        assert!((config.write_rate - 0.0).abs() < f64::EPSILON);
        assert!((config.read_rate - 0.0).abs() < f64::EPSILON);
        assert!(config.slow_threshold_read_ms > 1_000_000.0); // Effectively disabled
    }

    #[test]
    fn test_wide_events_sampling_config_for_test() {
        let config = WideEventsSamplingConfig::for_test();
        // Test config samples everything
        assert!((config.error_rate - 1.0).abs() < f64::EPSILON);
        assert!((config.slow_rate - 1.0).abs() < f64::EPSILON);
        assert!((config.vip_rate - 1.0).abs() < f64::EPSILON);
        assert!((config.write_rate - 1.0).abs() < f64::EPSILON);
        assert!((config.read_rate - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_wide_events_sampling_config_validate_rates() {
        // Valid rates
        let config = WideEventsSamplingConfig::builder()
            .error_rate(0.0)
            .slow_rate(0.5)
            .vip_rate(1.0)
            .write_rate(0.1)
            .read_rate(0.01)
            .build();
        assert!(config.validate().is_ok());

        // Invalid: rate > 1.0
        let config = WideEventsSamplingConfig::builder().error_rate(1.5).build();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("error_rate"));
        assert!(err.to_string().contains("0.0 and 1.0"));

        // Invalid: rate < 0.0
        let config = WideEventsSamplingConfig::builder().write_rate(-0.1).build();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("write_rate"));
    }

    #[test]
    fn test_wide_events_sampling_config_validate_thresholds() {
        // Valid thresholds
        let config = WideEventsSamplingConfig::builder()
            .slow_threshold_read_ms(5.0)
            .slow_threshold_write_ms(50.0)
            .slow_threshold_admin_ms(500.0)
            .build();
        assert!(config.validate().is_ok());

        // Invalid: threshold <= 0
        let config = WideEventsSamplingConfig::builder().slow_threshold_read_ms(0.0).build();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("slow_threshold_read_ms"));
        assert!(err.to_string().contains("positive"));

        let config = WideEventsSamplingConfig::builder().slow_threshold_write_ms(-10.0).build();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("slow_threshold_write_ms"));
    }

    #[test]
    fn test_wide_events_config_defaults() {
        let config = WideEventsConfig::default();
        assert!(config.enabled);
        assert!(config.vip_namespaces.is_empty());
        // Sampling defaults are covered by sampling config tests
    }

    #[test]
    fn test_wide_events_config_for_test() {
        let config = WideEventsConfig::for_test();
        assert!(config.enabled);
        assert!(config.vip_namespaces.is_empty());
        // Test config samples everything
        assert!((config.sampling.read_rate - 1.0).abs() < f64::EPSILON);
        assert!((config.sampling.write_rate - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_wide_events_config_validate() {
        // Valid config
        let config = WideEventsConfig::default();
        assert!(config.validate().is_ok());

        // Invalid sampling config propagates error
        let config = WideEventsConfig {
            sampling: WideEventsSamplingConfig::builder().error_rate(2.0).build(),
            ..WideEventsConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_includes_wide_events() {
        let config = Config::default();
        assert!(config.wide_events.enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_for_test_uses_test_wide_events() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();
        let config = Config::for_test(1, 50051, data_dir);

        // for_test uses WideEventsConfig::for_test() which samples everything
        assert!(config.wide_events.enabled);
        assert!((config.wide_events.sampling.read_rate - 1.0).abs() < f64::EPSILON);
        assert!((config.wide_events.sampling.write_rate - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_config_validate_includes_wide_events() {
        // Config with invalid wide_events should fail validation
        let config = Config {
            wide_events: WideEventsConfig {
                sampling: WideEventsSamplingConfig::builder().vip_rate(-0.5).build(),
                ..WideEventsConfig::default()
            },
            ..Config::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("vip_rate"));
    }

    #[test]
    fn test_wide_events_sampling_config_builder() {
        let config = WideEventsSamplingConfig::builder()
            .error_rate(0.9)
            .slow_rate(0.8)
            .vip_rate(0.7)
            .write_rate(0.5)
            .read_rate(0.1)
            .slow_threshold_read_ms(20.0)
            .slow_threshold_write_ms(200.0)
            .slow_threshold_admin_ms(2000.0)
            .build();

        assert!((config.error_rate - 0.9).abs() < f64::EPSILON);
        assert!((config.slow_rate - 0.8).abs() < f64::EPSILON);
        assert!((config.vip_rate - 0.7).abs() < f64::EPSILON);
        assert!((config.write_rate - 0.5).abs() < f64::EPSILON);
        assert!((config.read_rate - 0.1).abs() < f64::EPSILON);
        assert!((config.slow_threshold_read_ms - 20.0).abs() < f64::EPSILON);
        assert!((config.slow_threshold_write_ms - 200.0).abs() < f64::EPSILON);
        assert!((config.slow_threshold_admin_ms - 2000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_wide_events_config_builder() {
        let sampling = WideEventsSamplingConfig::builder().read_rate(0.05).build();
        let config = WideEventsConfig::builder()
            .enabled(false)
            .sampling(sampling)
            .vip_namespaces(vec![1, 2, 3])
            .build();

        assert!(!config.enabled);
        assert!((config.sampling.read_rate - 0.05).abs() < f64::EPSILON);
        assert_eq!(config.vip_namespaces, vec![1, 2, 3]);
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
        // Enabled without endpoint should fail
        let config = OtelConfig::builder().enabled(true).build();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("endpoint"));
        assert!(err.to_string().contains("required"));
    }

    #[test]
    fn test_otel_config_validate_enabled_with_endpoint() {
        let config = OtelConfig::builder()
            .enabled(true)
            .endpoint("http://localhost:4317".to_string())
            .build();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_otel_config_validate_batch_size() {
        // Zero batch size is invalid
        let config = OtelConfig::builder().batch_size(0).build();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("batch_size"));
        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn test_otel_config_validate_batch_interval() {
        // Zero interval is invalid
        let config = OtelConfig::builder().batch_interval_ms(0).build();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("batch_interval_ms"));
    }

    #[test]
    fn test_otel_config_validate_timeout() {
        // Zero timeout is invalid
        let config = OtelConfig::builder().timeout_ms(0).build();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("timeout_ms"));
    }

    #[test]
    fn test_otel_config_validate_shutdown_timeout() {
        // Zero shutdown timeout is invalid
        let config = OtelConfig::builder().shutdown_timeout_ms(0).build();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("shutdown_timeout_ms"));
    }

    #[test]
    fn test_otel_config_builder() {
        let config = OtelConfig::builder()
            .enabled(true)
            .endpoint("http://localhost:4317".to_string())
            .transport(OtelTransport::Http)
            .batch_size(256)
            .batch_interval_ms(2500)
            .timeout_ms(5000)
            .shutdown_timeout_ms(7500)
            .trace_raft_rpcs(false)
            .build();

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
    fn test_wide_events_config_includes_otel() {
        let config = WideEventsConfig::default();
        assert!(!config.otel.enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_wide_events_config_validate_includes_otel() {
        // Invalid OTEL config should fail validation
        let config = WideEventsConfig {
            otel: OtelConfig::builder().enabled(true).build(), // Missing endpoint
            ..WideEventsConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("endpoint"));
    }

    // === VIP Config Tests ===

    #[test]
    fn test_vip_config_defaults() {
        let config = VipConfig::default();
        assert!(config.discovery_enabled);
        assert_eq!(config.cache_ttl_secs, 60);
        assert_eq!(config.tag_name, "vip");
    }

    #[test]
    fn test_vip_config_for_test() {
        let config = VipConfig::for_test();
        assert!(!config.discovery_enabled);
        assert_eq!(config.cache_ttl_secs, 60);
        assert_eq!(config.tag_name, "vip");
    }

    #[test]
    fn test_vip_config_validate_cache_ttl() {
        let config = VipConfig { cache_ttl_secs: 0, ..VipConfig::default() };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("cache_ttl_secs"));
    }

    #[test]
    fn test_vip_config_validate_tag_name() {
        let config = VipConfig { tag_name: String::new(), ..VipConfig::default() };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("tag_name"));
    }

    #[test]
    fn test_vip_config_builder() {
        let config = VipConfig::builder()
            .discovery_enabled(false)
            .cache_ttl_secs(120)
            .tag_name("priority".to_string())
            .build();

        assert!(!config.discovery_enabled);
        assert_eq!(config.cache_ttl_secs, 120);
        assert_eq!(config.tag_name, "priority");
    }

    #[test]
    fn test_wide_events_config_includes_vip() {
        let config = WideEventsConfig::default();
        assert!(config.vip.discovery_enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_wide_events_config_validate_includes_vip() {
        // Invalid VIP config should fail validation
        let config = WideEventsConfig {
            vip: VipConfig { cache_ttl_secs: 0, ..VipConfig::default() },
            ..WideEventsConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("cache_ttl_secs"));
    }
}
