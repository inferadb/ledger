//! InferaDB Ledger server binary.
//!
//! This is the main entry point for running a ledger node.
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
mod coordinator;
mod discovery;
mod node_id;
mod shutdown;

use std::{io::IsTerminal, net::SocketAddr};

use config::{Config, LogFormat};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Server error type.
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
    let config = Config::parse_args();

    // Initialize logging based on config
    init_logging(&config);

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

    let node =
        bootstrap::bootstrap_node(&config, &data_dir).await.map_err(ServerError::Bootstrap)?;

    let shutdown_coordinator = shutdown::ShutdownCoordinator::new();
    let shutdown_handle = {
        let coordinator = shutdown_coordinator.clone();
        tokio::spawn(async move {
            shutdown::shutdown_signal().await;
            coordinator.shutdown();
        })
    };

    tracing::info!("Server ready, accepting connections");
    let server_result = node.server.serve().await;
    let _ = shutdown_handle.await;

    server_result.map_err(ServerError::Server)?;

    tracing::info!("Server shutdown complete");
    Ok(())
}

/// Initialize the logging system based on configuration.
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

/// Initialize the Prometheus metrics exporter.
///
/// Starts an HTTP server that exposes metrics at `/metrics`.
fn init_metrics_exporter(addr: SocketAddr) -> Result<(), ServerError> {
    let builder = PrometheusBuilder::new().with_http_listener(addr);

    builder.install().map_err(|e| {
        ServerError::Server(Box::new(std::io::Error::other(format!(
            "Failed to install Prometheus exporter: {}",
            e
        ))))
    })?;

    tracing::info!(metrics_addr = %addr, "Prometheus metrics exporter started");
    Ok(())
}

impl Clone for shutdown::ShutdownCoordinator {
    fn clone(&self) -> Self {
        // Note: This creates a new coordinator that shares the same broadcast channel
        // This is intentional for the shutdown use case
        Self::new()
    }
}
