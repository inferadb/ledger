//! Server configuration.
//!
//! Configuration is loaded from command-line arguments or environment variables.
//! CLI arguments take precedence over environment variables.

// The schemars `JsonSchema` derive macro internally uses `.unwrap()`.
#![allow(clippy::disallowed_methods)]

use std::{net::SocketAddr, path::PathBuf};

use bon::Builder;
use clap::{ArgGroup, Parser};
pub use inferadb_ledger_types::config::{OtelConfig, StorageMode};
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
/// # Persistent storage (production)
/// inferadb-ledger --data /data
///
/// # Persistent storage with seed peers
/// inferadb-ledger --data /data --join node1:9090,node2:9090
///
/// # Ephemeral storage (development / one-off testing)
/// inferadb-ledger --dev
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
/// # Storage Mode
///
/// Exactly one of `--data <path>` or `--dev` must be supplied for the
/// server-launch invocation (i.e. running `inferadb-ledger` with no
/// subcommand). Supplying both produces a CLI parse error; supplying
/// neither produces a runtime [`ConfigError::NoStorageMode`] at the
/// start of `main` before any I/O. Client subcommands (`init`,
/// `vaults`, `config schema`, `restore apply`) do **not** require a
/// storage flag — they either talk to a running server over gRPC or
/// take their own `--data-dir` argument. `--data` selects persistent
/// file-system storage at the given path. `--dev` selects ephemeral
/// file-backed storage at an auto-generated tempdir under
/// `std::env::temp_dir()`; the path changes on every restart, so the
/// previous run's data is orphaned. `--dev` is **not** an in-memory mode —
/// the storage backend is still `FileBackend`; only the path lifetime
/// differs. Use `--dev` for development and testing only; production
/// deployments must specify `--data <path>`.
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
#[command(group(
    // Mutually exclusive but not required at parse time. The
    // server-launch path (no subcommand) enforces "exactly one of
    // --data / --dev" via `Config::storage_mode()` in `main.rs`,
    // which produces `ConfigError::NoStorageMode` when neither flag
    // is supplied. Client subcommands (`init`, `vaults`,
    // `config schema`, `restore apply`) skip the check entirely —
    // local storage is meaningless for a gRPC client.
    ArgGroup::new("storage")
        .required(false)
        .multiple(false)
        .args(["data_dir", "dev"]),
))]
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

    /// Optional override for `observability.vault_metrics_enabled` at startup.
    ///
    /// `None` (the default — flag absent) honors the canonical
    /// [`ObservabilityConfig::vault_metrics_enabled`](inferadb_ledger_types::config::ObservabilityConfig)
    /// field on [`Config::observability`]. `Some(true)` (`--vault-metrics`,
    /// `--vault-metrics=true`, `INFERADB__LEDGER__VAULT_METRICS=true`)
    /// forces per-vault Prometheus series on regardless of the inner
    /// field; `Some(false)` (`--vault-metrics=false`) forces them off.
    ///
    /// Per-vault metrics are restart-only: every vault adds a fresh
    /// `vault_id` label set, multiplying time-series count by the number
    /// of vaults per organization. Flipping the gate at runtime would
    /// reshape the time-series mid-scrape, so the resolved value is set
    /// exactly once at startup. Organization-level rollups
    /// (`org_apply_throughput_ops_total`, `org_active_vault_count`) are
    /// always-on regardless of this flag.
    #[arg(
        long = "vault-metrics",
        env = "INFERADB__LEDGER__VAULT_METRICS",
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = false,
    )]
    #[serde(default)]
    pub vault_metrics: Option<bool>,

    /// Optional override for `hibernation.enabled` at startup.
    ///
    /// `None` (the default — flag absent) honors the canonical
    /// [`HibernationConfig::enabled`](inferadb_ledger_types::config::HibernationConfig)
    /// field on [`Config::hibernation`]. `Some(true)` (`--vault-hibernation`,
    /// `--vault-hibernation=true`, `INFERADB__LEDGER__VAULT_HIBERNATION=true`)
    /// activates the idle detector regardless of the inner field;
    /// `Some(false)` (`--vault-hibernation=false`) forces it off.
    ///
    /// When the resolved value is `true`, the
    /// [`RaftManager`](inferadb_ledger_raft::RaftManager) runs an
    /// idle-detector task that transitions per-vault Raft groups to
    /// `Dormant` after they have not seen activity in `idle_secs`
    /// (default 5 min, see
    /// [`HibernationConfig`](inferadb_ledger_types::config::HibernationConfig)).
    /// The next request to a dormant vault wakes it back to `Active`.
    /// Hibernation is opt-in to keep the disciplined "no surprise
    /// throttling" default model (operators must accept the wake-time
    /// budget consciously).
    #[arg(
        long = "vault-hibernation",
        env = "INFERADB__LEDGER__VAULT_HIBERNATION",
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = false,
    )]
    #[serde(default)]
    pub vault_hibernation: Option<bool>,

    /// Vault hibernation policy. The `vault_hibernation` CLI flag is an
    /// optional override on
    /// [`HibernationConfig::enabled`](inferadb_ledger_types::config::HibernationConfig);
    /// this struct carries the canonical setting plus the tuning knobs
    /// (`idle_secs`, `max_warm`, `wake_threshold_ms`, `max_stall_secs`,
    /// `scan_interval_secs`). When `vault_hibernation` is unset, the
    /// `enabled` field here is honored as-is.
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub hibernation: inferadb_ledger_types::config::HibernationConfig,

    /// Observability configuration. The `vault_metrics` CLI flag is an
    /// optional override on
    /// [`ObservabilityConfig::vault_metrics_enabled`](inferadb_ledger_types::config::ObservabilityConfig);
    /// this struct carries the canonical setting and any future
    /// per-emitter knobs. Fields here take effect at startup only —
    /// flipping cardinality at runtime would reshape the time-series
    /// mid-scrape, so observability stays restart-only (mirrors the
    /// existing `RuntimeConfig` exclusion).
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub observability: inferadb_ledger_types::config::ObservabilityConfig,

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

    /// Persistent data directory for Raft logs and snapshots.
    ///
    /// Required for the server-launch invocation (i.e. running
    /// `inferadb-ledger` with no subcommand): exactly one of `--data`
    /// or `--dev` must be specified, otherwise `main` aborts with
    /// `ConfigError::NoStorageMode`. Client subcommands (`init`,
    /// `vaults`, `config schema`, `restore apply`) ignore the flag
    /// entirely. When `--data` is set, the server uses
    /// [`StorageMode::File`] and the path is honored verbatim. For
    /// development / one-off testing without a path, pass `--dev`
    /// instead.
    #[arg(long = "data", env = "INFERADB__LEDGER__DATA")]
    #[serde(default)]
    pub data_dir: Option<PathBuf>,

    /// Run with ephemeral file-backed storage at an auto-generated tempdir.
    ///
    /// Selects [`StorageMode::EphemeralTempDir`]. The storage backend is
    /// still `FileBackend` — this is **not** an in-memory mode. The path
    /// lives under `std::env::temp_dir()` and changes on every restart;
    /// the previous run's data is orphaned. Use only for development and
    /// testing — production deployments must specify `--data <path>`.
    /// Mutually exclusive with `--data`.
    #[arg(long = "dev", env = "INFERADB__LEDGER__DEV")]
    #[serde(default)]
    #[builder(default)]
    pub dev: bool,

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

    /// Comma-separated list of *protected* regions this node opts into hosting.
    ///
    /// Joining nodes consult the GLOBAL region directory at boot and apply
    /// the following policy for every entry:
    ///
    /// - **Unprotected regions** (`protected = false`): auto-join. Default behavior.
    /// - **Protected regions** (`protected = true`): only join when the region's name appears in
    ///   this list.
    ///
    /// Names are matched case-sensitively against the directory entry's
    /// `name` field (e.g. `us-east-va`, `eu-west-de`). The CLI uses
    /// `--regions us-east-va,eu-west-de`; the env var
    /// `INFERADB__LEDGER__REGIONS=us-east-va,eu-west-de` is equivalent.
    /// Defaults to the empty list — protected regions are never joined
    /// without explicit operator opt-in.
    #[arg(long = "regions", value_delimiter = ',', env = "INFERADB__LEDGER__REGIONS")]
    #[serde(default)]
    #[builder(default)]
    pub regions: Vec<String>,

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
    #[arg(long = "concurrent", env = "INFERADB__LEDGER__MAX_CONCURRENT", default_value_t = 10_000)]
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

    // === Write Batching ===
    /// Per-region write-batching configuration.
    ///
    /// Each started region spawns a `BatchWriter` that coalesces concurrent
    /// `Write` RPCs into a single Raft proposal, amortizing one WAL `fsync`
    /// across the batch. Controls the per-batch cap and the time bound before
    /// a partial batch flushes. Production defaults (500 / 10ms) are tuned for
    /// throughput over single-client latency — see
    /// [`BatchConfig`](inferadb_ledger_types::config::BatchConfig).
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub batching: inferadb_ledger_types::config::BatchConfig,

    // === HTTP/2 Transport ===
    /// HTTP/2 transport tuning for the gRPC server (flow-control windows,
    /// max concurrent streams).
    ///
    /// Restart-only: applied to the `tonic::transport::Server` builder at
    /// startup. Defaults (2 MiB stream window, 8 MiB connection window,
    /// 2048 max concurrent streams) are tuned for high-concurrency RPC
    /// workloads. The matching client-side knobs live on the SDK
    /// `ClientConfig`; both ends should be raised together to take effect.
    #[arg(skip)]
    #[serde(default)]
    #[builder(default)]
    pub http2: inferadb_ledger_types::config::Http2Config,

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

    // === Rate Limiting ===
    /// Optional override for `rate_limit.enabled` at startup.
    ///
    /// `None` (the default — flag absent) honors the canonical
    /// [`RateLimitConfig::enabled`](inferadb_ledger_types::config::RateLimitConfig)
    /// field on [`Config::rate_limit`] (which itself defaults to `false`,
    /// keeping rate limiting disabled out-of-the-box). `Some(true)`
    /// (`--ratelimit`, `--ratelimit=true`,
    /// `INFERADB__LEDGER__RATELIMIT=true`) forces the limiter on
    /// regardless of the inner field; `Some(false)` (`--ratelimit=false`)
    /// forces it off.
    ///
    /// Rate limiting defaults to disabled because surprise default
    /// throttling combined with SDK silent retry hides the rate limit
    /// behind retry backoff — a debugging trap that has historically
    /// cost more time than the protection saved. Production deployments
    /// that need DoS protection or per-tenant SLO enforcement opt in
    /// deliberately. The runtime-reconfig path (`UpdateConfig`) reads the
    /// inner field directly and is unaffected by this override.
    #[arg(
        long = "ratelimit",
        env = "INFERADB__LEDGER__RATELIMIT",
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = false,
    )]
    #[serde(default)]
    pub ratelimit: Option<bool>,

    /// Rate limit configuration for per-client and per-organization request throttling.
    ///
    /// Controls token bucket capacities and refill rates. The `enabled`
    /// field of this struct is the canonical master switch; the
    /// top-level `ratelimit` CLI flag is an optional startup override.
    /// Set `enabled = true` here (or in YAML / env config) to activate
    /// the limiter; `--ratelimit=false` at startup overrides it off, and
    /// `--ratelimit` overrides it on. Tune thresholds via this struct or
    /// the runtime `UpdateConfig` RPC.
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

    // === Bootstrap Initialization Timeout ===
    /// How long a fresh node waits for an `InitCluster` RPC before returning an error, in seconds.
    ///
    /// Prevents a misconfigured or isolated fresh node from waiting forever. After the timeout
    /// elapses with no initialization signal, `bootstrap_node` returns
    /// `BootstrapError::InitTimeout`. Defaults to 600 seconds (10 minutes). Set to 0 to
    /// disable the timeout (wait indefinitely).
    #[arg(
        long = "init-wait-timeout",
        env = "INFERADB__LEDGER__INIT_WAIT_TIMEOUT",
        default_value_t = default_init_wait_timeout_secs()
    )]
    #[serde(default = "default_init_wait_timeout_secs")]
    #[builder(default = default_init_wait_timeout_secs())]
    pub init_wait_timeout_secs: u64,
}

// Default value functions
fn default_region() -> inferadb_ledger_types::Region {
    inferadb_ledger_types::Region::GLOBAL
}

fn default_token_maintenance_interval_secs() -> u64 {
    300 // 5 minutes
}
fn default_max_concurrent() -> usize {
    10_000
}
fn default_timeout_secs() -> u64 {
    30
}
fn default_init_wait_timeout_secs() -> u64 {
    600 // 10 minutes
}
impl Default for Config {
    fn default() -> Self {
        Self {
            listen: None,
            metrics_addr: None,
            vault_metrics: None,
            vault_hibernation: None,
            hibernation: inferadb_ledger_types::config::HibernationConfig::default(),
            observability: inferadb_ledger_types::config::ObservabilityConfig::default(),
            log_format: LogFormat::default(),
            data_dir: None,
            dev: false,
            region: default_region(),
            regions: Vec::new(),
            advertise: None,
            join: None,
            max_concurrent: default_max_concurrent(),
            timeout_secs: default_timeout_secs(),
            raft: None,
            batching: inferadb_ledger_types::config::BatchConfig::default(),
            http2: inferadb_ledger_types::config::Http2Config::default(),
            logging: LoggingConfig::default(),
            backup: None,
            events: inferadb_ledger_types::events::EventConfig::default(),
            jwt: inferadb_ledger_types::config::JwtConfig::default(),
            saga: inferadb_ledger_types::config::SagaConfig::default(),
            cleanup: inferadb_ledger_types::config::CleanupConfig::default(),
            integrity: inferadb_ledger_types::config::IntegrityConfig::default(),
            tiered_storage: inferadb_ledger_types::config::TieredStorageConfig::default(),
            health_check: None,
            ratelimit: None,
            rate_limit: None,
            token_maintenance_interval_secs: default_token_maintenance_interval_secs(),
            email_blinding_key: None,
            socket: None,
            init_wait_timeout_secs: default_init_wait_timeout_secs(),
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
            vault_metrics: None,
            vault_hibernation: None,
            hibernation: inferadb_ledger_types::config::HibernationConfig::default(),
            observability: inferadb_ledger_types::config::ObservabilityConfig::default(),
            data_dir: Some(data_dir),
            dev: false,
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

    /// Returns true if the server is running in ephemeral mode (`--dev`).
    ///
    /// In ephemeral mode, data is stored in a temporary directory and is
    /// lost on process exit, on tempdir purge, and on every restart (each
    /// fresh launch picks a new tempdir suffix). Equivalent to
    /// `self.storage_mode().map_or(false, |m| m.is_ephemeral())`, except
    /// that this method is infallible: it reports the ephemeral choice
    /// directly without re-validating the mutual-exclusion invariant.
    pub fn is_ephemeral(&self) -> bool {
        self.dev
    }

    /// Resolves the storage mode declared by the operator.
    ///
    /// The CLI surface enforces mutual exclusion of `--data` / `--dev` at
    /// parse time via clap's `ArgGroup`. This method is the programmatic
    /// equivalent — callers that construct `Config` directly (notably
    /// tests) get the same invariant check here.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::NoStorageMode`] when neither `--data` nor
    /// `--dev` is set; [`ConfigError::ConflictingStorageModes`] when
    /// both are set.
    pub fn storage_mode(&self) -> Result<StorageMode, ConfigError> {
        match (&self.data_dir, self.dev) {
            (Some(path), false) => Ok(StorageMode::File { data_dir: path.clone() }),
            (None, true) => Ok(StorageMode::EphemeralTempDir),
            (None, false) => NoStorageModeSnafu.fail(),
            (Some(_), true) => ConflictingStorageModesSnafu.fail(),
        }
    }

    /// Resolves the data directory on disk for the current storage mode.
    ///
    /// For [`StorageMode::File`], returns the configured path verbatim.
    /// For [`StorageMode::EphemeralTempDir`], generates a Snowflake-suffixed
    /// path under [`std::env::temp_dir`] and `mkdir -p`s it.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::NoStorageMode`] / [`ConfigError::ConflictingStorageModes`]
    /// if the storage-mode invariant is violated, or
    /// [`ConfigError::Validation`] if the ephemeral tempdir cannot be created.
    pub fn resolve_data_dir(&self) -> Result<PathBuf, ConfigError> {
        let mode = self.storage_mode()?;
        mode.resolve_data_dir().map_err(Into::into)
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
    /// Neither `--data` nor `--dev` was supplied at startup.
    ///
    /// The server-launch path (no subcommand) requires exactly one
    /// storage flag; `main` calls [`Config::storage_mode`] before any
    /// I/O and aborts with this variant when neither is set. Client
    /// subcommands (`init`, `vaults`, `config schema`,
    /// `restore apply`) skip the check entirely.
    #[snafu(display(
        "exactly one of `--data <path>` or `--dev` is required at startup; neither was supplied"
    ))]
    NoStorageMode,
    /// Both `--data` and `--dev` were supplied at startup.
    ///
    /// clap's `ArgGroup` rejects this at parse time for every
    /// invocation (server-launch and subcommands alike). End users
    /// hit this variant only when constructing [`Config`]
    /// programmatically (notably tests).
    #[snafu(display("`--data` and `--dev` are mutually exclusive"))]
    ConflictingStorageModes,
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

    /// Path to write tracing-flame folded-stack output for span-based profiling.
    ///
    /// Only present when the `profiling` feature is enabled. When set, the server
    /// installs a `tracing_flame::FlameLayer` into the tracing subscriber and
    /// flushes the folded-stack file at graceful shutdown. Consumed by
    /// `scripts/profile-server.sh spans`.
    #[cfg(feature = "profiling")]
    #[arg(long, value_name = "PATH")]
    pub flamegraph_spans: Option<PathBuf>,
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
    /// Restore-related offline operations (run with the node stopped).
    Restore {
        /// Restore action to perform.
        #[command(subcommand)]
        action: RestoreAction,
    },
    /// Per-vault inspection and repair commands (Phase 7 / O4).
    ///
    /// Connects to a running node and queries its `AdminService` for
    /// per-vault Raft group state. Hit a region voter for the org to
    /// get an authoritative view — non-voter nodes have no per-vault
    /// groups for organizations they do not host.
    Vaults {
        /// Vault action to perform.
        #[command(subcommand)]
        action: VaultsAction,
        /// Address of the node to query (e.g., "node1:9090").
        #[arg(long = "host", global = true, default_value = "127.0.0.1:9090")]
        host: String,
    },
}

/// Restore subcommand actions.
///
/// The `apply` action is the offline half of the two-phase restore
/// designed in `docs/superpowers/specs/2026-04-24-multi-db-backup-archive-format.md`:
/// the online `RestoreBackup` RPC stages an archive into
/// `{data_dir}/.restore-staging/...`, and `restore apply` swaps that
/// staging tree onto the live data directory while the node is stopped.
/// Run after `SIGTERM` has fully drained the node — the subcommand
/// refuses to run if the data directory's `.lock` file is held by
/// another process.
#[derive(Debug, clap::Subcommand)]
pub enum RestoreAction {
    /// Swap a staged restore tree into the live data directory.
    ///
    /// Resolves `{data_dir}/{region}/{organization_id}/`, moves the
    /// existing tree (if any) to `{data_dir}/.restore-trash/{ts}/{org_id}/`,
    /// then renames `staging` onto the live path. Drops a
    /// `{data_dir}/.restore-marker` JSON file so the operator can
    /// confirm the swap before restarting the node.
    Apply {
        /// Root data directory of the (stopped) node.
        #[arg(long = "data-dir")]
        data_dir: PathBuf,
        /// Staging directory produced by `RestoreBackup` RPC.
        ///
        /// Typically `{data_dir}/.restore-staging/{org_id}-{timestamp}`.
        #[arg(long = "staging")]
        staging_dir: PathBuf,
        /// Region the restored organization lives in.
        ///
        /// Must match the `region` field of the archive's manifest.
        /// Use the canonical region slug (e.g. `us-east-va`,
        /// `de-central-frankfurt`).
        #[arg(long = "region")]
        region: String,
        /// Internal organization ID to restore (matches the
        /// archive manifest's `organization_id`).
        #[arg(long = "organization-id")]
        organization_id: i64,
    },
}

/// Per-vault inspection actions exposed by the `vaults` CLI subcommand.
///
/// All variants connect to the `--host` node's `AdminService` and surface
/// per-vault Raft-group state to the operator. Output formats:
///
/// - `list`: human-readable table of `(slug, status, leader, voters, learners, last-applied)`.
/// - `show`: JSON dump of the full `ShowVaultResponse`.
/// - `repair`: plain-text status line.
#[derive(Debug, clap::Subcommand)]
pub enum VaultsAction {
    /// List all vaults registered on the target node for an organization,
    /// with their lifecycle state, leadership, and apply progress.
    List {
        /// External organization slug (Snowflake u64). Required.
        org: u64,
    },
    /// Show detailed status for a single vault: voters, learners,
    /// hibernation lifecycle state, last-activity / pending-membership
    /// timestamps, conf epoch.
    Show {
        /// External organization slug. Required.
        org: u64,
        /// External vault slug. Required.
        vault: u64,
    },
    /// Trigger an operator-initiated vault repair. Today this is a stub:
    /// the underlying consensus engine does not expose a "force snapshot"
    /// or "force re-replication" entrypoint, so the RPC validates the
    /// vault exists and logs operator intent.
    Repair {
        /// External organization slug. Required.
        org: u64,
        /// External vault slug. Required.
        vault: u64,
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
        assert_eq!(config.max_concurrent, 10_000);
        assert_eq!(config.timeout_secs, 30);
        assert!(config.join.is_none());
        assert!(config.data_dir.is_none());
        assert!(!config.dev, "default Config has dev: false");
    }

    #[test]
    fn default_config_has_no_storage_mode_and_not_localhost() {
        // Neither --data nor --dev is set on the bare default — exactly
        // the configuration the operator must explicitly choose between.
        let config = Config::default();
        assert!(matches!(config.storage_mode(), Err(ConfigError::NoStorageMode)));
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
        // Ephemeral only when --dev is explicitly set
        let config = Config { dev: true, ..Config::default() };
        assert!(config.is_ephemeral());

        // Not ephemeral when --data is set
        let config = Config { data_dir: Some(PathBuf::from("/data")), ..Config::default() };
        assert!(!config.is_ephemeral());

        // Not ephemeral on the bare default (neither --data nor --dev) —
        // storage_mode() will reject this configuration anyway.
        let config = Config::default();
        assert!(!config.is_ephemeral());
    }

    #[test]
    fn test_storage_mode_file_when_only_data_dir_set() {
        let config = Config { data_dir: Some(PathBuf::from("/data/ledger")), ..Config::default() };
        let mode = config.storage_mode().expect("data_dir alone is a valid mode");
        assert!(
            matches!(mode, StorageMode::File { ref data_dir } if data_dir == &PathBuf::from("/data/ledger")),
            "expected File {{ data_dir: /data/ledger }}, got {mode:?}"
        );
    }

    #[test]
    fn test_storage_mode_ephemeral_when_only_dev_set() {
        let config = Config { dev: true, ..Config::default() };
        let mode = config.storage_mode().expect("dev alone is a valid mode");
        assert!(matches!(mode, StorageMode::EphemeralTempDir));
    }

    #[test]
    fn test_storage_mode_errors_when_neither_set() {
        let config = Config::default();
        let err = config.storage_mode().expect_err("neither --data nor --dev is invalid");
        assert!(matches!(err, ConfigError::NoStorageMode));
        // Error message names both flags so operators know what to do.
        let msg = err.to_string();
        assert!(msg.contains("--data"), "error mentions --data: {msg}");
        assert!(msg.contains("--dev"), "error mentions --dev: {msg}");
    }

    #[test]
    fn test_storage_mode_errors_when_both_set() {
        let config =
            Config { data_dir: Some(PathBuf::from("/data")), dev: true, ..Config::default() };
        let err = config.storage_mode().expect_err("--data + --dev is invalid");
        assert!(matches!(err, ConfigError::ConflictingStorageModes));
        let msg = err.to_string();
        assert!(msg.contains("--data"), "error mentions --data: {msg}");
        assert!(msg.contains("--dev"), "error mentions --dev: {msg}");
    }

    #[test]
    fn test_resolve_data_dir_with_configured_path() {
        let config = Config { data_dir: Some(PathBuf::from("/data/ledger")), ..Config::default() };
        let resolved = config.resolve_data_dir().expect("resolve data_dir");
        assert_eq!(resolved, PathBuf::from("/data/ledger"));
    }

    #[test]
    fn test_resolve_data_dir_ephemeral() {
        let config = Config { dev: true, ..Config::default() };
        assert!(config.is_ephemeral());

        let resolved = config.resolve_data_dir().expect("resolve data_dir");

        // Should be in temp directory with ledger- prefix
        assert!(resolved.starts_with(std::env::temp_dir()));
        assert!(resolved.file_name().unwrap().to_str().unwrap().starts_with("ledger-"));
        assert!(resolved.exists());

        // Cleanup
        std::fs::remove_dir_all(&resolved).ok();
    }

    #[test]
    fn test_resolve_data_dir_rejects_no_storage_mode() {
        let config = Config::default();
        let err = config.resolve_data_dir().expect_err("default has no storage mode");
        assert!(matches!(err, ConfigError::NoStorageMode));
    }

    #[test]
    fn test_resolve_data_dir_rejects_conflicting_storage_modes() {
        let config =
            Config { data_dir: Some(PathBuf::from("/data")), dev: true, ..Config::default() };
        let err = config.resolve_data_dir().expect_err("--data + --dev is invalid");
        assert!(matches!(err, ConfigError::ConflictingStorageModes));
    }

    #[test]
    fn cli_parses_invocation_without_storage_mode() {
        // Bare `inferadb-ledger` (no subcommand, no storage flag) must
        // *parse* cleanly — the storage requirement is enforced at
        // runtime in `main` via `Config::storage_mode()` so client
        // subcommands (`init`, `vaults`, etc.) don't inherit the
        // requirement when they have no use for local storage.
        let cli = Cli::try_parse_from(["inferadb-ledger"])
            .expect("bare invocation should parse; runtime check in main enforces storage flag");
        assert!(cli.command.is_none());
        assert!(cli.config.data_dir.is_none());
        assert!(!cli.config.dev);

        // The runtime check produces the canonical error text.
        let err = cli.config.storage_mode().expect_err("storage_mode rejects missing storage flag");
        assert!(matches!(err, ConfigError::NoStorageMode));
        let msg = err.to_string();
        assert!(msg.contains("--data"), "error mentions --data: {msg}");
        assert!(msg.contains("--dev"), "error mentions --dev: {msg}");
    }

    #[test]
    fn cli_rejects_both_data_and_dev() {
        // Mutual exclusion is still a parse-time check (clap
        // `ArgGroup` with `multiple = false`), so `--data /path --dev`
        // fails before any runtime logic runs.
        let err = Cli::try_parse_from(["inferadb-ledger", "--data", "/tmp/ledger", "--dev"])
            .expect_err("clap should reject --data + --dev");
        let msg = err.to_string();
        assert!(
            msg.contains("--dev") || msg.contains("--data"),
            "clap error mentions one of the storage flags: {msg}"
        );
    }

    #[test]
    fn cli_accepts_data_alone() {
        let cli = Cli::try_parse_from(["inferadb-ledger", "--data", "/tmp/ledger"])
            .expect("--data alone is valid");
        assert_eq!(cli.config.data_dir.as_deref(), Some(std::path::Path::new("/tmp/ledger")));
        assert!(!cli.config.dev);
    }

    #[test]
    fn cli_accepts_dev_alone() {
        let cli = Cli::try_parse_from(["inferadb-ledger", "--dev"]).expect("--dev alone is valid");
        assert!(cli.config.data_dir.is_none());
        assert!(cli.config.dev);
    }

    #[test]
    fn cli_init_subcommand_does_not_require_storage_flag() {
        // `init` is a gRPC client subcommand — it talks to a running
        // server over `--host` and has no use for local storage.
        // Before #245 the top-level `--data` / `--dev` requirement
        // forced operators to pass an irrelevant placeholder.
        let cli = Cli::try_parse_from(["inferadb-ledger", "init", "--host", "127.0.0.1:9090"])
            .expect("init parses without --data / --dev");
        assert!(matches!(cli.command, Some(CliCommand::Init { .. })));
        assert!(cli.config.data_dir.is_none());
        assert!(!cli.config.dev);
    }

    #[test]
    fn cli_vaults_subcommand_does_not_require_storage_flag() {
        let cli = Cli::try_parse_from(["inferadb-ledger", "vaults", "list", "42"])
            .expect("vaults parses without --data / --dev");
        assert!(matches!(cli.command, Some(CliCommand::Vaults { .. })));
    }

    #[test]
    fn cli_config_schema_subcommand_does_not_require_storage_flag() {
        let cli = Cli::try_parse_from(["inferadb-ledger", "config", "schema"])
            .expect("config schema parses without --data / --dev");
        assert!(matches!(cli.command, Some(CliCommand::Config { action: ConfigAction::Schema })));
    }

    #[test]
    fn cli_restore_apply_subcommand_does_not_require_top_level_storage_flag() {
        // `restore apply` carries its own `--data-dir` argument
        // (offline operation against a stopped node's data tree); the
        // top-level `--data` / `--dev` flag is meaningless and must
        // not be required.
        let cli = Cli::try_parse_from([
            "inferadb-ledger",
            "restore",
            "apply",
            "--data-dir",
            "/var/lib/ledger",
            "--staging",
            "/var/lib/ledger/.restore-staging/42",
            "--region",
            "us-east-va",
            "--organization-id",
            "42",
        ])
        .expect("restore apply parses without top-level --data / --dev");
        assert!(matches!(cli.command, Some(CliCommand::Restore { .. })));
        assert!(cli.config.data_dir.is_none());
        assert!(!cli.config.dev);
    }

    // === Logging Config Tests ===

    #[test]
    fn test_logging_config_for_test() {
        let config = LoggingConfig::for_test();
        // for_test uses OtelConfig::for_test() defaults (disabled for unit tests)
        assert!(!config.otel.enabled);
    }

    // === OTEL Config Tests ===

    #[test]
    fn test_otel_config_for_test() {
        let config = OtelConfig::for_test();
        assert!(!config.enabled); // Disabled by default for tests
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
            .build()
            .unwrap();

        assert!(config.enabled);
        assert_eq!(config.endpoint, Some("http://localhost:4317".to_string()));
        assert_eq!(config.transport, OtelTransport::Http);
        assert_eq!(config.batch_size, 256);
        assert_eq!(config.batch_interval_ms, 2500);
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.shutdown_timeout_ms, 7500);
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

    #[test]
    fn test_default_regions_is_empty() {
        let config = Config::default();
        assert!(config.regions.is_empty());
    }

    #[test]
    fn test_regions_cli_parses_comma_list() {
        use clap::Parser;
        // `--dev` satisfies the required storage-mode group so the parse
        // exercises just the `--regions` knob.
        let cli = Cli::try_parse_from([
            "inferadb-ledger",
            "--listen",
            "127.0.0.1:9090",
            "--dev",
            "--regions",
            "us-east-va,eu-west-de",
        ])
        .unwrap();
        assert_eq!(cli.config.regions, vec!["us-east-va".to_string(), "eu-west-de".to_string()]);
    }

    #[test]
    fn test_regions_cli_default_empty() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["inferadb-ledger", "--listen", "127.0.0.1:9090", "--dev"])
            .unwrap();
        assert!(cli.config.regions.is_empty());
    }

    #[test]
    fn test_regions_serde_default() {
        let json = r#"{"listen": "127.0.0.1:50051"}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert!(config.regions.is_empty());
    }

    #[test]
    fn test_regions_serde_explicit() {
        let json = r#"{"listen": "127.0.0.1:50051", "regions": ["us-east-va", "eu-west-de"]}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.regions, vec!["us-east-va".to_string(), "eu-west-de".to_string()]);
    }

    // =========================================================================
    // Option<bool> CLI flag parse tests (DSoT migration: --ratelimit,
    // --vault-hibernation, --vault-metrics).
    //
    // Each flag uses `num_args = 0..=1` + `default_missing_value = "true"`
    // + `require_equals = false`. Operator UX expectations:
    //
    //   flag absent           → None  (honor inner config field)
    //   --flag                → Some(true)
    //   --flag=true           → Some(true)
    //   --flag true           → Some(true)
    //   --flag=false          → Some(false)
    //
    // These tests guard against regressions on `default_missing_value`
    // and `require_equals`.
    // =========================================================================

    /// Helper: parse a CLI invocation with `--dev` to satisfy the required
    /// storage-mode group, returning the constructed `Config`.
    fn parse_cli(extra_args: &[&str]) -> Config {
        use clap::Parser;
        let mut argv = vec!["inferadb-ledger", "--dev"];
        argv.extend_from_slice(extra_args);
        Cli::try_parse_from(argv).unwrap().config
    }

    // --- ratelimit ---

    #[test]
    fn cli_ratelimit_absent_is_none() {
        let config = parse_cli(&[]);
        assert_eq!(config.ratelimit, None);
    }

    #[test]
    fn cli_ratelimit_bare_is_some_true() {
        let config = parse_cli(&["--ratelimit"]);
        assert_eq!(config.ratelimit, Some(true));
    }

    #[test]
    fn cli_ratelimit_eq_true_is_some_true() {
        let config = parse_cli(&["--ratelimit=true"]);
        assert_eq!(config.ratelimit, Some(true));
    }

    #[test]
    fn cli_ratelimit_space_true_is_some_true() {
        // `require_equals = false` accepts the space-separated form.
        let config = parse_cli(&["--ratelimit", "true"]);
        assert_eq!(config.ratelimit, Some(true));
    }

    #[test]
    fn cli_ratelimit_eq_false_is_some_false() {
        let config = parse_cli(&["--ratelimit=false"]);
        assert_eq!(config.ratelimit, Some(false));
    }

    // --- vault-hibernation ---

    #[test]
    fn cli_vault_hibernation_absent_is_none() {
        let config = parse_cli(&[]);
        assert_eq!(config.vault_hibernation, None);
    }

    #[test]
    fn cli_vault_hibernation_bare_is_some_true() {
        let config = parse_cli(&["--vault-hibernation"]);
        assert_eq!(config.vault_hibernation, Some(true));
    }

    #[test]
    fn cli_vault_hibernation_eq_true_is_some_true() {
        let config = parse_cli(&["--vault-hibernation=true"]);
        assert_eq!(config.vault_hibernation, Some(true));
    }

    #[test]
    fn cli_vault_hibernation_space_true_is_some_true() {
        let config = parse_cli(&["--vault-hibernation", "true"]);
        assert_eq!(config.vault_hibernation, Some(true));
    }

    #[test]
    fn cli_vault_hibernation_eq_false_is_some_false() {
        let config = parse_cli(&["--vault-hibernation=false"]);
        assert_eq!(config.vault_hibernation, Some(false));
    }

    // --- vault-metrics ---

    #[test]
    fn cli_vault_metrics_absent_is_none() {
        let config = parse_cli(&[]);
        assert_eq!(config.vault_metrics, None);
    }

    #[test]
    fn cli_vault_metrics_bare_is_some_true() {
        let config = parse_cli(&["--vault-metrics"]);
        assert_eq!(config.vault_metrics, Some(true));
    }

    #[test]
    fn cli_vault_metrics_eq_true_is_some_true() {
        let config = parse_cli(&["--vault-metrics=true"]);
        assert_eq!(config.vault_metrics, Some(true));
    }

    #[test]
    fn cli_vault_metrics_space_true_is_some_true() {
        let config = parse_cli(&["--vault-metrics", "true"]);
        assert_eq!(config.vault_metrics, Some(true));
    }

    #[test]
    fn cli_vault_metrics_eq_false_is_some_false() {
        let config = parse_cli(&["--vault-metrics=false"]);
        assert_eq!(config.vault_metrics, Some(false));
    }

    // --- defaults preserved ---

    #[test]
    fn default_config_preserves_rate_limiting_disabled() {
        // Central guarantee: default Config must leave rate limiting off
        // (CLI override `None` + inner default `false`). Resolved to
        // `false` by `RateLimitConfig::resolve_enabled`.
        let config = Config::default();
        assert_eq!(config.ratelimit, None);
        let resolved = inferadb_ledger_types::config::RateLimitConfig::resolve_enabled(
            config.ratelimit,
            false,
        );
        assert!(!resolved, "rate limiting must default to disabled");
    }

    #[test]
    fn default_config_preserves_hibernation_disabled() {
        let config = Config::default();
        assert_eq!(config.vault_hibernation, None);
        assert!(!config.hibernation.enabled);
    }

    #[test]
    fn default_config_preserves_observability_disabled() {
        let config = Config::default();
        assert_eq!(config.vault_metrics, None);
        assert!(!config.observability.vault_metrics_enabled);
    }
}
