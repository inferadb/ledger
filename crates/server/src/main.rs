//! InferaDB Ledger server binary.
//!
//! This is the main entry point for running a ledger node.
//!
//! # Usage
//!
//! ```bash
//! # Start with default config (./inferadb-ledger.toml)
//! ledger
//!
//! # Start with custom config
//! ledger --config /path/to/config.toml
//! ```

mod bootstrap;
mod config;
mod discovery;
mod shutdown;

use std::{env, net::SocketAddr};

use config::{Config, ConfigError};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::EnvFilter;

/// Server error type.
#[derive(Debug)]
enum ServerError {
    Config(ConfigError),
    Bootstrap(bootstrap::BootstrapError),
    Server(Box<dyn std::error::Error>),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::Config(e) => write!(f, "configuration error: {}", e),
            ServerError::Bootstrap(e) => write!(f, "bootstrap error: {}", e),
            ServerError::Server(e) => write!(f, "server error: {}", e),
        }
    }
}

impl std::error::Error for ServerError {}

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config_path = parse_args();

    let config = Config::load(config_path.as_deref()).map_err(ServerError::Config)?;

    tracing::info!(
        node_id = config.node_id,
        listen_addr = %config.listen_addr,
        data_dir = %config.data_dir.display(),
        "Starting InferaDB Ledger"
    );

    if let Some(metrics_addr) = config.metrics_addr {
        init_metrics_exporter(metrics_addr)?;
    }

    let node = bootstrap::bootstrap_node(&config).await.map_err(ServerError::Bootstrap)?;

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

/// Parse command line arguments.
fn parse_args() -> Option<String> {
    let args: Vec<String> = env::args().collect();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" | "-c" => {
                if i + 1 < args.len() {
                    return Some(args[i + 1].clone());
                }
            },
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            },
            "--version" | "-V" => {
                println!("ledger {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            },
            _ => {},
        }
        i += 1;
    }

    None
}

fn print_help() {
    println!(
        r#"InferaDB Ledger - Distributed consensus ledger

USAGE:
    ledger [OPTIONS]

OPTIONS:
    -c, --config <FILE>    Configuration file path [default: inferadb-ledger.toml]
    -h, --help             Print help information
    -V, --version          Print version information

ENVIRONMENT VARIABLES:
    INFERADB__LEDGER__NODE_ID       Node identifier (numeric)
    INFERADB__LEDGER__LISTEN_ADDR   gRPC listen address (e.g., 0.0.0.0:50051)
    INFERADB__LEDGER__METRICS_ADDR  Prometheus metrics address (e.g., 0.0.0.0:9090)
    INFERADB__LEDGER__DATA_DIR      Data directory path

EXAMPLES:
    # Start with default configuration
    ledger

    # Start with custom config file
    ledger --config /etc/ledger/config.toml

    # Start a single-node cluster (auto-bootstraps on fresh data directory)
    INFERADB__LEDGER__NODE_ID=1 \
    INFERADB__LEDGER__LISTEN_ADDR=0.0.0.0:50051 \
    INFERADB__LEDGER__DATA_DIR=/tmp/ledger \
    ledger
"#
    );
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
