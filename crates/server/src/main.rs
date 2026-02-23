//! InferaDB Ledger server binary.
//!
//! Launches a ledger node with gRPC services, Raft consensus, and background jobs.
//!
//! # Usage
//!
//! ```bash
//! # Start single-node cluster
//! inferadb-ledger --listen 0.0.0.0:50051 --data /tmp/ledger --single
//!
//! # Start with environment variables
//! INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
//! INFERADB__LEDGER__DATA=/tmp/ledger \
//! INFERADB__LEDGER__CLUSTER=1 \
//! inferadb-ledger
//!
//! # CLI arguments override environment variables
//! INFERADB__LEDGER__CLUSTER=3 inferadb-ledger --single
//! ```

mod bootstrap;
mod config;
mod config_reload;
mod coordinator;
mod discovery;
mod node_id;
mod shutdown;

use std::{io::IsTerminal, net::SocketAddr};

use clap::Parser;
use config::{Cli, CliCommand, Config, ConfigAction, LogFormat, OtelTransport};
use inferadb_ledger_raft::otel::{self, OtelConfig};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Top-level error type for the server binary, wrapping bootstrap and runtime failures.
#[derive(Debug)]
enum ServerError {
    Bootstrap(bootstrap::BootstrapError),
    Server(Box<dyn std::error::Error>),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::Bootstrap(e) => write!(f, "bootstrap error: {}", e),
            ServerError::Server(e) => write!(f, "server error: {}", e),
        }
    }
}

impl std::error::Error for ServerError {}

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    // Parse CLI args and env vars (clap handles --help and --version)
    let cli = Cli::parse();

    // Handle subcommands before starting the server.
    if let Some(command) = cli.command {
        match command {
            CliCommand::Config { action } => match action {
                ConfigAction::Schema => {
                    print!("{}", config::generate_runtime_config_schema());
                    return Ok(());
                },
                ConfigAction::Example => {
                    print!("{}", config::generate_runtime_config_example());
                    return Ok(());
                },
            },
        }
    }

    let config = cli.config;

    // Initialize logging based on config
    init_logging(&config);

    // Initialize OpenTelemetry if configured
    init_otel(&config)?;

    // Resolve data directory (creates ephemeral temp directory if not configured)
    let data_dir = config.resolve_data_dir().map_err(|e| {
        ServerError::Server(Box::new(std::io::Error::other(format!(
            "Failed to resolve data directory: {}",
            e
        ))))
    })?;

    tracing::info!(
        listen_addr = %config.listen_addr,
        data_dir = %data_dir.display(),
        "Starting InferaDB Ledger"
    );

    // Warn if listening only on localhost
    if config.is_localhost_only() {
        tracing::warn!(
            "Listening on localhost only. Remote connections will be rejected. \
             Set --listen or INFERADB__LEDGER__LISTEN to accept remote connections."
        );
    }

    // Warn if running in ephemeral mode
    if config.is_ephemeral() {
        tracing::warn!(
            data_dir = %data_dir.display(),
            "Running in ephemeral mode. All data will be lost on shutdown. \
             Set --data or INFERADB__LEDGER__DATA for persistent storage."
        );
    }

    if let Some(metrics_addr) = config.metrics_addr {
        init_metrics_exporter(metrics_addr)?;
    }

    // Set up graceful shutdown before bootstrap so we can wire the signal
    let shutdown_config = inferadb_ledger_types::config::ShutdownConfig::default();
    let watchdog =
        inferadb_ledger_raft::BackgroundJobWatchdog::new(shutdown_config.watchdog_multiplier);
    let health_state = inferadb_ledger_raft::HealthState::new().with_watchdog(watchdog);
    let (graceful_shutdown, shutdown_rx) =
        inferadb_ledger_raft::GracefulShutdown::new(shutdown_config, health_state.clone());

    let node = bootstrap::bootstrap_node(&config, &data_dir, health_state.clone(), shutdown_rx)
        .await
        .map_err(ServerError::Bootstrap)?;

    // Mark node as ready now that bootstrap is complete
    health_state.mark_ready();

    // Spawn SIGHUP config reload handler if a config file is specified
    #[cfg(unix)]
    if let Some(ref config_path) = config.config_file {
        config_reload::spawn_sighup_handler(
            config_path.clone(),
            node.runtime_config.clone(),
            None, // rate_limiter propagated via RuntimeConfigHandle::update()
            None, // hot_key_detector propagated via RuntimeConfigHandle::update()
        );
        tracing::info!(config_file = %config_path.display(), "SIGHUP config reload enabled");
    }

    // Spawn shutdown handler
    let raft_for_shutdown = node.raft.clone();
    let shutdown_handle = tokio::spawn(async move {
        shutdown::shutdown_signal().await;
        graceful_shutdown
            .execute(|| async move {
                // Trigger final snapshot if leader
                let _ = raft_for_shutdown.trigger().snapshot().await;
                if let Err(e) = raft_for_shutdown.shutdown().await {
                    tracing::warn!(error = %e, "Error during Raft shutdown");
                }
            })
            .await;
    });

    tracing::info!("Server ready, accepting connections");
    let server_result = node.server.serve().await;
    let _ = shutdown_handle.await;

    // Shutdown OTEL tracer provider gracefully
    otel::shutdown_otel();

    server_result.map_err(ServerError::Server)?;

    tracing::info!("Server shutdown complete");
    Ok(())
}

/// Initializes the logging system based on configuration.
///
/// Supports three formats:
/// - `Text`: Human-readable format (development)
/// - `Json`: JSON structured logging (production)
/// - `Auto`: JSON for non-TTY stdout, text otherwise
fn init_logging(config: &Config) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let use_json = match config.log_format {
        LogFormat::Json => true,
        LogFormat::Text => false,
        LogFormat::Auto => !std::io::stdout().is_terminal(),
    };

    if use_json {
        // JSON format for production / log aggregation
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt::layer().json().flatten_event(true).with_current_span(false))
            .init();
    } else {
        // Human-readable text format for development
        tracing_subscriber::registry().with(env_filter).with(fmt::layer()).init();
    }
}

/// Initializes OpenTelemetry/OTLP export if configured.
///
/// Creates a tracer provider with batch span processor for exporting traces
/// to observability backends like Jaeger, Tempo, or Honeycomb.
fn init_otel(config: &Config) -> Result<(), ServerError> {
    let otel_config = &config.logging.otel;

    if !otel_config.enabled {
        return Ok(());
    }

    // Convert server config to raft crate's OtelConfig
    let raft_otel_config = OtelConfig {
        enabled: otel_config.enabled,
        endpoint: otel_config.endpoint.clone().unwrap_or_default(),
        use_grpc: matches!(otel_config.transport, OtelTransport::Grpc),
        timeout_ms: otel_config.timeout_ms,
        shutdown_timeout_ms: otel_config.shutdown_timeout_ms,
        trace_raft_rpcs: otel_config.trace_raft_rpcs,
    };

    otel::init_otel(&raft_otel_config).map_err(|e| {
        ServerError::Server(Box::new(std::io::Error::other(format!(
            "Failed to initialize OTEL: {}",
            e
        ))))
    })
}

/// Initializes the Prometheus metrics exporter.
///
/// Starts an HTTP server that exposes metrics at `/metrics`.
/// Configures histogram buckets aligned with SLI targets for latency tracking.
fn init_metrics_exporter(addr: SocketAddr) -> Result<(), ServerError> {
    let builder = PrometheusBuilder::new()
        .with_http_listener(addr)
        .set_buckets(&inferadb_ledger_raft::metrics::SLI_HISTOGRAM_BUCKETS)
        .map_err(|e| {
            ServerError::Server(Box::new(std::io::Error::other(format!(
                "Failed to configure histogram buckets: {}",
                e
            ))))
        })?;

    builder.install().map_err(|e| {
        ServerError::Server(Box::new(std::io::Error::other(format!(
            "Failed to install Prometheus exporter: {}",
            e
        ))))
    })?;

    tracing::info!(metrics_addr = %addr, "Prometheus metrics exporter started");
    Ok(())
}
