//! InferaDB Ledger server binary.
//!
//! Launches a ledger node with gRPC services, Raft consensus, and background jobs.
//!
//! # Usage
//!
//! ```bash
//! # Persistent storage (production)
//! inferadb-ledger --listen 0.0.0.0:50051 --data /tmp/ledger
//!
//! # Ephemeral storage (development / one-off testing)
//! inferadb-ledger --listen 0.0.0.0:50051 --dev
//!
//! # Start with environment variables
//! INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
//! INFERADB__LEDGER__DATA=/tmp/ledger \
//! inferadb-ledger
//!
//! # Initialize the cluster (one-time, after nodes are running)
//! inferadb-ledger init --host node1:9090
//! ```

mod bootstrap;
mod cluster_id;
mod config;
mod coordinator;
mod discovery;
mod dr_scheduler;
mod node_id;
mod placement;
mod shutdown;
mod tls;
mod wire_client;

use std::{io::IsTerminal, net::SocketAddr};

use clap::Parser;
use config::{
    AdminAction, Cli, CliCommand, Config, ConfigAction, LogFormat, RestoreAction, VaultsAction,
};
use inferadb_ledger_raft::otel;
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
            },
            CliCommand::Init { host } => {
                // Connect to the target node over the wire transport and send
                // InitCluster RPC.
                let tls = cli.config.tls();
                let client = wire_client::build_admin_client(&host, tls).map_err(|e| {
                    ServerError::Server(Box::new(std::io::Error::other(e.to_string())))
                })?;
                let resp = client
                    .init_cluster(
                        inferadb_ledger_wire::services::admin::InitClusterRequest {},
                        // request_id: any non-zero value the server treats as
                        // an idempotency token. `1` is fine — InitCluster is a
                        // one-shot per cluster lifetime and the server's
                        // already_initialized flag covers retries.
                        1,
                    )
                    .await
                    .map_err(|e| {
                        ServerError::Server(Box::new(std::io::Error::other(format!(
                            "InitCluster RPC failed: {}",
                            e
                        ))))
                    })?;

                if resp.already_initialized {
                    println!("Cluster already initialized. cluster_id={}", resp.cluster_id);
                } else {
                    println!("Cluster initialized. cluster_id={}", resp.cluster_id);
                }
                return Ok(());
            },
            CliCommand::Restore { action } => match action {
                RestoreAction::Apply { data_dir, staging_dir, region, organization_id } => {
                    return handle_restore_apply(data_dir, staging_dir, region, organization_id);
                },
            },
            CliCommand::Vaults { action, host } => {
                let tls = cli.config.tls();
                return handle_vaults_command(action, host, tls).await;
            },
            CliCommand::Admin { action } => {
                let tls = cli.config.tls();
                return handle_admin_command(action, tls).await;
            },
        }
    }

    // Capture the flamegraph-spans path BEFORE moving `cli.config`, then init
    // logging. On the `profiling` feature, `init_logging` returns a
    // `FlushGuard` whose `Drop` flushes pending span events to the
    // folded-stack file — it must outlive `serve_with_shutdown().await`.
    // Kept in `main`'s scope with a leading underscore to silence unused
    // warnings without triggering early drop.
    #[cfg(feature = "profiling")]
    let flamegraph_spans = cli.flamegraph_spans.clone();

    let config = cli.config;

    // Fail fast on the server-launch path when no storage flag was
    // supplied. clap's `ArgGroup("storage")` enforces mutual exclusion
    // (`multiple = false`) so `--data /path --dev` is rejected at parse
    // time, but the group is no longer `required` — that requirement
    // applied to every invocation including client subcommands like
    // `init`, which made the flag mandatory for gRPC clients that have
    // no use for local storage. The check lives here so the operator
    // sees the same well-formatted error text whether they hit it via
    // CLI parse (no subcommand) or via programmatic construction.
    config
        .storage_mode()
        .map_err(|e| ServerError::Server(Box::new(std::io::Error::other(e.to_string()))))?;

    // Initialize logging based on config
    #[cfg(feature = "profiling")]
    let _flame_guard = init_logging(&config, flamegraph_spans.as_deref())?;
    #[cfg(not(feature = "profiling"))]
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
        listen = ?config.listen,
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

    // Warn if running with --dev (ephemeral file-backed storage)
    if config.is_ephemeral() {
        tracing::warn!(
            data_dir = %data_dir.display(),
            "Running with --dev: ephemeral file-backed storage. \
             Data will be lost on restart, on tempdir purge, and on every fresh \
             launch (each restart picks a new tempdir suffix). \
             Use --data <path> for persistent storage in production."
        );
    }

    if let Some(metrics_addr) = config.metrics_addr {
        init_metrics_exporter(metrics_addr)?;
    }

    // Phase 7 / O3: wire the per-vault Prometheus emission gate before any
    // metric is sampled. `ObservabilityConfig::vault_metrics_enabled` is
    // the canonical setting (defaults to `false` for cardinality control);
    // the `--vault-metrics` CLI flag is an optional `Option<bool>` startup
    // override. Per-vault metrics are restart-only — flipping the gate at
    // runtime would change the time-series shape mid-scrape, so it is set
    // exactly once here.
    let vault_metrics_enabled = inferadb_ledger_types::config::ObservabilityConfig::resolve_enabled(
        config.vault_metrics,
        config.observability.vault_metrics_enabled,
    );
    inferadb_ledger_raft::metrics::set_vault_metrics_enabled(vault_metrics_enabled);
    if vault_metrics_enabled {
        tracing::info!(
            "Per-vault Prometheus metrics enabled (vault_id labels active). \
             Cardinality scales with the number of vaults per organization."
        );
    }

    // Set up graceful shutdown before bootstrap so we can wire the signal
    let shutdown_config = inferadb_ledger_types::config::ShutdownConfig::default();
    let watchdog =
        inferadb_ledger_raft::BackgroundJobWatchdog::new(shutdown_config.watchdog_multiplier);
    let health_state = inferadb_ledger_raft::HealthState::new().with_watchdog(watchdog);
    let (graceful_shutdown, shutdown_rx) =
        inferadb_ledger_raft::GracefulShutdown::new(shutdown_config, health_state.clone());

    // Create in-memory key manager for DEK/signing key encryption.
    // In production, this would be backed by a KMS. For now, generate
    // ephemeral keys that cover all regions.
    let key_manager: std::sync::Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager> =
        std::sync::Arc::new(
            inferadb_ledger_store::crypto::InMemoryKeyManager::generate_for_regions(
                &inferadb_ledger_types::ALL_REGIONS,
            ),
        );

    let node = bootstrap::bootstrap_node(
        &config,
        &data_dir,
        health_state.clone(),
        shutdown_rx,
        Some(key_manager),
    )
    .await
    .map_err(ServerError::Bootstrap)?;

    // Mark node as ready now that bootstrap is complete
    health_state.mark_ready();

    // Spawn shutdown handler
    let consensus_handle = node.handle.clone();
    let coordinator = node.coordinator.clone();
    // Clone the RaftManager so the `pre_shutdown` closure can force a final
    // `sync_state` on every region's state DB AFTER the WAL flush. This
    // narrows the post-restart WAL replay to zero entries on clean shutdown.
    let manager_for_shutdown = node.manager.clone();
    // Clone the batching `EventHandle` so the `pre_shutdown` closure can
    // drain the handler-phase event flusher at Phase 5b — between the WAL
    // flush (5a) and the state-DB sync (5c). The
    // flusher keeps running through coordinator shutdown so in-flight RPCs
    // in Phases 1-4 can still enqueue events; `flush_for_shutdown` is the
    // only signal that stops it.
    let event_handle_for_shutdown = node.event_handle.clone();
    // The flusher `JoinHandle` is moved (not cloned) into the shutdown
    // closure so the runtime doesn't tear down while the tokio task is
    // technically still scheduled after `flush_for_shutdown` returns.
    let event_flusher_join_handle = node.event_flusher_handle;
    let graceful_shutdown = graceful_shutdown.with_handle(node.handle.clone());
    let shutdown_handle = tokio::spawn(async move {
        shutdown::shutdown_signal().await;

        // Spawn a watchdog THE MOMENT SIGTERM is received. This forces
        // process exit after 30 seconds regardless of what happens during
        // the graceful shutdown sequence. The 30-second budget covers:
        //   5s  Kubernetes endpoint removal wait
        //  15s  Load balancer drain
        //  10s  Grace for pre-shutdown tasks + WAL flush
        // Background tasks (compactor, GC, orphan cleanup) lack explicit
        // shutdown signals and can keep the tokio runtime alive
        // indefinitely; the watchdog is the hard backstop.
        // Uses eprintln (not tracing) so the message survives even if
        // the tracing subscriber is broken.
        tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            eprintln!("Shutdown watchdog: forcing process exit after 30s grace period");
            std::process::exit(0);
        });

        // Drain background jobs BEFORE the WAL flush. Cancelling the
        // coordinator's root token signals all child tokens, and
        // `shutdown()` awaits each registered handle with a per-handle
        // deadline so in-flight work completes cleanly.
        coordinator.shutdown().await;

        graceful_shutdown
            .execute(|| async move {
                // Phase 5a: explicitly flush the WAL before the
                // consensus engine handle is dropped. This ensures all
                // committed proposals are durable even if a crash occurs
                // between gRPC server stop and handle drop.
                let flush_timeout = std::time::Duration::from_secs(5);
                if let Err(e) = consensus_handle.flush_for_shutdown(flush_timeout).await {
                    tracing::warn!(error = %e, "WAL flush during shutdown failed");
                } else {
                    tracing::info!("WAL flushed successfully during shutdown");
                }

                // Phase 5b: drain the handler-phase event
                // flusher before the final state-DB sync. The flusher keeps
                // accepting events through Phases 1-4 (in-flight RPCs still
                // emit), so this is the single point where the queue is
                // fully drained + the flusher exits cleanly. Strict-durable
                // per-window commits here — any events still queued after
                // the timeout are logged as lost + counted in
                // `ledger_event_overflow_total{cause=shutdown_timeout}`.
                let drain_timeout = std::time::Duration::from_secs(5);
                let drain_result =
                    event_handle_for_shutdown.flush_for_shutdown(drain_timeout).await;
                tracing::info!(
                    drained = drain_result.drained,
                    lost = drain_result.lost,
                    duration_ms = drain_result.duration.as_millis() as u64,
                    "Event flusher drain complete"
                );

                // Defensive: await the flusher task handle so its tokio task
                // fully terminates before the runtime shuts down.
                // `flush_for_shutdown` already waits for the drain ack, so
                // this should resolve almost immediately. A missed exit here
                // would otherwise race the watchdog's `std::process::exit(0)`
                // and leak the task mid-fsync.
                let join_timeout = std::time::Duration::from_secs(1);
                match tokio::time::timeout(join_timeout, event_flusher_join_handle).await {
                    Ok(Ok(())) => tracing::debug!("Event flusher task exited cleanly"),
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "Event flusher task panicked on exit")
                    },
                    Err(_) => tracing::warn!(
                        "Event flusher task did not exit within 1s of flush_for_shutdown"
                    ),
                }

                // Phase 5c: force a final `sync_state` on every region's
                // state DB AFTER the
                // WAL flush so the dual-slot god byte captures every apply
                // that happened between the last `StateCheckpointer` tick
                // and now. On clean shutdown this drives post-restart WAL
                // replay to zero entries. Proceed even if the WAL flush
                // above failed — the god byte may still succeed on a prefix
                // that the WAL flush missed, narrowing the crash gap.
                // `sync_all_state_dbs` logs per-region success/failure
                // internally and never aborts the sweep on a single
                // region's error. For `events.db`, Phase 5b already drained
                // pending writes, so this sweep is effectively a no-op for
                // that DB — harmless but redundant.
                manager_for_shutdown.sync_all_state_dbs(std::time::Duration::from_secs(5)).await;
            })
            .await;
    });

    tracing::info!("Server ready, accepting connections");
    let server_result = node.server_handle.await;
    let _ = shutdown_handle.await;

    // Shutdown OTEL tracer provider gracefully
    otel::shutdown_otel();

    match server_result {
        Ok(Ok(())) => {},
        Ok(Err(e)) => return Err(ServerError::Server(e.into())),
        Err(e) => return Err(ServerError::Server(Box::new(e))),
    }

    tracing::info!("Server shutdown complete");
    Ok(())
}

/// Initializes the logging system based on configuration.
///
/// Supports three formats:
/// - `Text`: Human-readable format (development)
/// - `Json`: JSON structured logging (production)
/// - `Auto`: JSON for non-TTY stdout, text otherwise
///
/// When the `profiling` feature is enabled and `flamegraph_spans` is `Some`, a
/// `tracing_flame::FlameLayer` is composed into the subscriber chain. The
/// returned `FlushGuard` (wrapped in `Option`) flushes pending span events to
/// the folded-stack file on `Drop`; the caller must keep it alive for the
/// lifetime of the server.
#[cfg(feature = "profiling")]
fn init_logging(
    config: &Config,
    flamegraph_spans: Option<&std::path::Path>,
) -> Result<Option<tracing_flame::FlushGuard<std::io::BufWriter<std::fs::File>>>, ServerError> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let use_json = match config.log_format {
        LogFormat::Json => true,
        LogFormat::Text => false,
        LogFormat::Auto => !std::io::stdout().is_terminal(),
    };

    // Inline `FlameLayer::with_file` in each branch so the layer's subscriber
    // type parameter is inferred independently per stack shape (json vs text).
    // Lifting it into a shared helper would unify the types across branches
    // and fail to compile.
    let flame_guard = match (flamegraph_spans, use_json) {
        (Some(path), true) => {
            let (flame_layer, guard) = tracing_flame::FlameLayer::with_file(path)
                .map_err(|e| flame_open_error(path, e))?;
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().json().flatten_event(true).with_current_span(false))
                .with(flame_layer)
                .init();
            tracing::info!(path = %path.display(), "tracing-flame enabled");
            Some(guard)
        },
        (Some(path), false) => {
            let (flame_layer, guard) = tracing_flame::FlameLayer::with_file(path)
                .map_err(|e| flame_open_error(path, e))?;
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer())
                .with(flame_layer)
                .init();
            tracing::info!(path = %path.display(), "tracing-flame enabled");
            Some(guard)
        },
        (None, true) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().json().flatten_event(true).with_current_span(false))
                .init();
            None
        },
        (None, false) => {
            tracing_subscriber::registry().with(env_filter).with(fmt::layer()).init();
            None
        },
    };

    Ok(flame_guard)
}

/// Wraps a tracing-flame file-open failure in a `ServerError`.
#[cfg(feature = "profiling")]
fn flame_open_error(path: &std::path::Path, e: tracing_flame::Error) -> ServerError {
    ServerError::Server(Box::new(std::io::Error::other(format!(
        "failed to open flamegraph-spans file '{}': {}",
        path.display(),
        e
    ))))
}

/// Initializes the logging system based on configuration.
///
/// Supports three formats:
/// - `Text`: Human-readable format (development)
/// - `Json`: JSON structured logging (production)
/// - `Auto`: JSON for non-TTY stdout, text otherwise
#[cfg(not(feature = "profiling"))]
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

    otel::init_otel(otel_config).map_err(|e| {
        ServerError::Server(Box::new(std::io::Error::other(format!(
            "Failed to initialize OTEL: {}",
            e
        ))))
    })
}

/// Runs the offline `restore apply` subcommand.
///
/// Resolves the live per-org tree under `data_dir`, swaps the staging
/// tree onto it (moving the displaced live tree to
/// `{data_dir}/.restore-trash/{ts}/{org_id}/`), and prints a summary +
/// restart instructions to stderr. Refuses to run if the data
/// directory's `.lock` file is held by another process — the operator
/// must stop the node first.
///
/// Extracted into a free function so it remains unit-testable without
/// going through `main`'s subcommand dispatch.
fn handle_restore_apply(
    data_dir: std::path::PathBuf,
    staging_dir: std::path::PathBuf,
    region: String,
    organization_id: i64,
) -> Result<(), ServerError> {
    use std::str::FromStr;

    let parsed_region = inferadb_ledger_types::Region::from_str(&region).map_err(|e| {
        ServerError::Server(Box::new(std::io::Error::other(format!(
            "invalid --region '{region}': {e}"
        ))))
    })?;
    let parsed_org_id = inferadb_ledger_types::OrganizationId::new(organization_id);

    let result = inferadb_ledger_raft::backup::apply_staged_restore(
        &data_dir,
        &staging_dir,
        parsed_region,
        parsed_org_id,
    )
    .map_err(|e| ServerError::Server(Box::new(std::io::Error::other(e.to_string()))))?;

    eprintln!("restore apply: success");
    eprintln!(
        "  swapped {} files into {}",
        result.files_swapped,
        data_dir.join(parsed_region.as_str()).join(parsed_org_id.value().to_string()).display()
    );
    if result.trash_dir.as_os_str().is_empty() {
        eprintln!("  no pre-existing live tree was displaced");
    } else {
        eprintln!("  displaced tree moved to {} (24h retention)", result.trash_dir.display());
    }
    eprintln!("  marker file: {}", data_dir.join(".restore-marker").display());
    eprintln!();
    eprintln!("Restart the node to bring the restored organization online.");
    Ok(())
}

/// Dispatches a `vaults` subcommand to the appropriate `AdminService` RPC
/// over the wire transport.
///
/// All variants connect to the `--host` node and surface per-vault Raft
/// group state to the operator. The CLI is intentionally thin — formatting
/// happens here, the server-side handlers in
/// `crates/services/src/services/admin.rs` do the work.
async fn handle_vaults_command(
    action: VaultsAction,
    host: String,
    tls: config::TlsConfig,
) -> Result<(), ServerError> {
    use inferadb_ledger_types::{OrganizationSlug, VaultSlug};
    use inferadb_ledger_wire::services::admin as wadmin;

    let client = wire_client::build_admin_client(&host, tls)
        .map_err(|e| ServerError::Server(Box::new(std::io::Error::other(e.to_string()))))?;

    match action {
        VaultsAction::List { org } => {
            let resp = client
                .admin_list_vaults(
                    wadmin::AdminListVaultsRequest {
                        organization: Some(OrganizationSlug::new(org)),
                    },
                    // request_id: synthesise from the org slug so retries
                    // dedupe at the server.
                    u128::from(org),
                )
                .await
                .map_err(|e| {
                    ServerError::Server(Box::new(std::io::Error::other(format!(
                        "AdminListVaults RPC failed: {}",
                        e
                    ))))
                })?;
            print_vaults_list(&resp);
        },
        VaultsAction::Show { org, vault } => {
            let resp = client
                .show_vault(
                    wadmin::ShowVaultRequest {
                        organization: Some(OrganizationSlug::new(org)),
                        vault: Some(VaultSlug::new(vault)),
                    },
                    u128::from(vault),
                )
                .await
                .map_err(|e| {
                    ServerError::Server(Box::new(std::io::Error::other(format!(
                        "ShowVault RPC failed: {}",
                        e
                    ))))
                })?;
            print_vault_show(&resp);
        },
        VaultsAction::Repair { org, vault } => {
            let resp = client
                .repair_vault(
                    wadmin::RepairVaultRequest {
                        organization: Some(OrganizationSlug::new(org)),
                        vault: Some(VaultSlug::new(vault)),
                    },
                    u128::from(vault),
                )
                .await
                .map_err(|e| {
                    ServerError::Server(Box::new(std::io::Error::other(format!(
                        "RepairVault RPC failed: {}",
                        e
                    ))))
                })?;
            println!("status:  {}", resp.status);
            println!("message: {}", resp.message);
        },
    }

    Ok(())
}

/// Renders an `AdminListVaultsResponse` as a human-readable table.
fn print_vaults_list(resp: &inferadb_ledger_wire::services::admin::AdminListVaultsResponse) {
    if resp.vaults.is_empty() {
        println!(
            "No vaults found on this node for the requested organization. Hit a region voter \
             for the org for an authoritative view."
        );
        return;
    }
    println!(
        "{:<22} {:<8} {:<10} {:<8} {:<8} {:<14} LAST_ACTIVITY",
        "VAULT_SLUG", "STATUS", "LEADER", "VOTERS", "LEARN", "LAST_APPLIED"
    );
    for v in &resp.vaults {
        let slug = v.slug.map_or(0, |s| s.value());
        // `last_activity` is UNIX nanoseconds on the wire; the legacy proto
        // surface emitted seconds. Convert to seconds for parity with the
        // historical CLI output, falling back to `-` when unset.
        let last_activity = v
            .last_activity
            .map(|nanos| (nanos / 1_000_000_000).to_string())
            .unwrap_or_else(|| "-".to_owned());
        println!(
            "{:<22} {:<8} {:<10} {:<8} {:<8} {:<14} {}",
            slug,
            v.status,
            v.leader_node_id,
            v.voter_count,
            v.learner_count,
            v.last_applied_index,
            last_activity,
        );
    }
}

/// Renders a `ShowVaultResponse` as JSON for tooling-friendly consumption.
fn print_vault_show(resp: &inferadb_ledger_wire::services::admin::ShowVaultResponse) {
    let info = resp.info.as_ref();
    let slug = info.and_then(|i| i.slug).map_or(0, |s| s.value());
    let status = info.map_or("", |i| i.status.as_str());
    let leader = info.map_or(0, |i| i.leader_node_id);
    let voter_count = info.map_or(0, |i| i.voter_count);
    let learner_count = info.map_or(0, |i| i.learner_count);
    let last_applied = info.map_or(0, |i| i.last_applied_index);
    // Wire types use UNIX nanoseconds; CLI output preserves the historical
    // seconds-resolution rendering.
    let last_activity = info
        .and_then(|i| i.last_activity)
        .map(|nanos| (nanos / 1_000_000_000).to_string())
        .unwrap_or_else(|| "null".to_owned());
    let pending_membership = resp
        .pending_membership_started_at
        .map(|nanos| (nanos / 1_000_000_000).to_string())
        .unwrap_or_else(|| "null".to_owned());

    let voters: Vec<String> = resp.voters.iter().map(|v| v.to_string()).collect();
    let learners: Vec<String> = resp.learners.iter().map(|v| v.to_string()).collect();

    println!("{{");
    println!("  \"vault_slug\": {},", slug);
    println!("  \"region\": \"{}\",", resp.region);
    println!("  \"organization_id\": {},", resp.organization_id);
    println!("  \"vault_id\": {},", resp.vault_id);
    println!("  \"status\": \"{}\",", status);
    println!("  \"lifecycle_state\": \"{}\",", resp.lifecycle_state);
    println!("  \"leader_node_id\": {},", leader);
    println!("  \"voter_count\": {},", voter_count);
    println!("  \"learner_count\": {},", learner_count);
    println!("  \"voters\": [{}],", voters.join(", "));
    println!("  \"learners\": [{}],", learners.join(", "));
    println!("  \"conf_epoch\": {},", resp.conf_epoch);
    println!("  \"last_applied_index\": {},", last_applied);
    println!("  \"last_activity_unix_secs\": {},", last_activity);
    println!("  \"pending_membership_started_unix_secs\": {}", pending_membership);
    println!("}}");
}

/// Dispatches an `admin` subcommand to the appropriate wire-protocol RPC.
///
/// Each variant connects to the operator-supplied `--host` over QUIC + TLS,
/// issues a single RPC against one of `AdminService`, `UserService`,
/// `VaultService`, `WriteService`, or `ReadService`, and prints the
/// response as pretty-printed JSON on stdout. RPC failures surface as
/// free-form text on stderr via `ServerError::Server`.
///
/// The CLI is intentionally thin: server-side handlers in
/// `crates/services/src/services/` do all validation, authorization,
/// and Raft routing.
async fn handle_admin_command(
    action: AdminAction,
    tls: config::TlsConfig,
) -> Result<(), ServerError> {
    use inferadb_ledger_types::{OrganizationSlug, UserSlug, VaultSlug};
    use inferadb_ledger_wire::services::{
        admin as wadmin, read as wread, shared as wshared, user as wuser, vault as wvault,
        write as wwrite,
    };

    match action {
        AdminAction::ClusterInfo { host } => {
            let client = wire_client::build_admin_client(&host, tls).map_err(io_other)?;
            let resp = client
                .get_cluster_info(wadmin::GetClusterInfoRequest {}, request_id())
                .await
                .map_err(|e| rpc_failure("GetClusterInfo", e))?;
            print_json(&resp)
        },
        AdminAction::NodeInfo { host } => {
            let client = wire_client::build_admin_client(&host, tls).map_err(io_other)?;
            let resp = client
                .get_node_info(wadmin::GetNodeInfoRequest {}, request_id())
                .await
                .map_err(|e| rpc_failure("GetNodeInfo", e))?;
            print_json(&resp)
        },
        AdminAction::JoinCluster { host, node_id, address } => {
            let client = wire_client::build_admin_client(&host, tls).map_err(io_other)?;
            let resp = client
                .join_cluster(wadmin::JoinClusterRequest { node_id, address }, request_id())
                .await
                .map_err(|e| rpc_failure("JoinCluster", e))?;
            print_json(&resp)
        },
        AdminAction::LeaveCluster { host, node_id } => {
            let client = wire_client::build_admin_client(&host, tls).map_err(io_other)?;
            let resp = client
                .leave_cluster(wadmin::LeaveClusterRequest { node_id }, request_id())
                .await
                .map_err(|e| rpc_failure("LeaveCluster", e))?;
            print_json(&resp)
        },
        AdminAction::TransferLeadership { host, target_node_id, timeout_ms } => {
            let client = wire_client::build_admin_client(&host, tls).map_err(io_other)?;
            let resp = client
                .transfer_leadership(
                    wadmin::TransferLeadershipRequest { target_node_id, timeout_ms },
                    request_id(),
                )
                .await
                .map_err(|e| rpc_failure("TransferLeadership", e))?;
            print_json(&resp)
        },
        AdminAction::ProvisionRegion {
            host,
            name,
            protected,
            requires_residency,
            retention_days,
        } => {
            let client = wire_client::build_admin_client(&host, tls).map_err(io_other)?;
            let resp = client
                .provision_region(
                    wadmin::ProvisionRegionRequest {
                        name,
                        protected,
                        requires_residency,
                        retention_days,
                    },
                    request_id(),
                )
                .await
                .map_err(|e| rpc_failure("ProvisionRegion", e))?;
            print_json(&resp)
        },
        AdminAction::InitiateEmailVerification { host, email, region } => {
            let client = wire_client::build_user_client(&host, tls).map_err(io_other)?;
            let resp = client
                .initiate_email_verification(
                    wuser::InitiateEmailVerificationRequest { email, region },
                    request_id(),
                )
                .await
                .map_err(|e| rpc_failure("InitiateEmailVerification", e))?;
            print_json(&resp)
        },
        AdminAction::VerifyEmailCode { host, email, code, region } => {
            let client = wire_client::build_user_client(&host, tls).map_err(io_other)?;
            let resp = client
                .verify_email_code(
                    wuser::VerifyEmailCodeRequest { email, code, region },
                    request_id(),
                )
                .await
                .map_err(|e| rpc_failure("VerifyEmailCode", e))?;
            print_json(&resp)
        },
        AdminAction::CompleteRegistration {
            host,
            onboarding_token,
            email,
            region,
            name,
            organization_name,
        } => {
            let client = wire_client::build_user_client(&host, tls).map_err(io_other)?;
            let resp = client
                .complete_registration(
                    wuser::CompleteRegistrationRequest {
                        onboarding_token,
                        email,
                        region,
                        name,
                        organization_name,
                    },
                    request_id(),
                )
                .await
                .map_err(|e| rpc_failure("CompleteRegistration", e))?;
            print_json(&resp)
        },
        AdminAction::CreateVault { host, organization, caller, slug, replication_factor } => {
            let client = wire_client::build_vault_client(&host, tls).map_err(io_other)?;
            let resp = client
                .create_vault(
                    wvault::CreateVaultRequest {
                        organization: Some(OrganizationSlug::new(organization)),
                        replication_factor,
                        initial_nodes: Vec::new(),
                        retention_policy: None,
                        caller: Some(UserSlug::new(caller)),
                        slug: slug.map(VaultSlug::new),
                    },
                    request_id(),
                )
                .await
                .map_err(|e| rpc_failure("CreateVault", e))?;
            print_json(&resp)
        },
        AdminAction::Write { host, organization, vault, caller, key, value, idempotency_key } => {
            let idempotency_bytes = parse_idempotency_key(idempotency_key.as_deref())?;
            let client = wire_client::build_write_client(&host, tls).map_err(io_other)?;
            let resp = client
                .write(
                    wwrite::WriteRequest {
                        organization: Some(OrganizationSlug::new(organization)),
                        vault: Some(VaultSlug::new(vault)),
                        client_id: None,
                        idempotency_key: idempotency_bytes,
                        operations: vec![wshared::Operation {
                            op: Some(wshared::OperationKind::SetEntity(wshared::SetEntity {
                                key,
                                value: bytes::Bytes::from(value.into_bytes()),
                                expires_at: None,
                                condition: None,
                            })),
                        }],
                        include_tx_proof: false,
                        caller: Some(UserSlug::new(caller)),
                    },
                    request_id(),
                )
                .await
                .map_err(|e| rpc_failure("Write", e))?;
            print_json(&resp)
        },
        AdminAction::Read { host, organization, vault, caller, key, consistency } => {
            let consistency = parse_consistency(&consistency)?;
            let client = wire_client::build_read_client(&host, tls).map_err(io_other)?;
            let resp = client
                .read(
                    wread::ReadRequest {
                        organization: Some(OrganizationSlug::new(organization)),
                        vault: Some(VaultSlug::new(vault)),
                        key,
                        consistency,
                        caller: Some(UserSlug::new(caller)),
                    },
                    request_id(),
                )
                .await
                .map_err(|e| rpc_failure("Read", e))?;
            print_json(&resp)
        },
        AdminAction::ListEntities { host, organization, vault, caller, key_prefix, limit } => {
            let client = wire_client::build_read_client(&host, tls).map_err(io_other)?;
            let resp = client
                .list_entities(
                    wread::ListEntitiesRequest {
                        organization: Some(OrganizationSlug::new(organization)),
                        key_prefix,
                        at_height: None,
                        include_expired: false,
                        limit,
                        page_token: String::new(),
                        consistency: wshared::ReadConsistency::Eventual,
                        vault: Some(VaultSlug::new(vault)),
                        caller: Some(UserSlug::new(caller)),
                    },
                    request_id(),
                )
                .await
                .map_err(|e| rpc_failure("ListEntities", e))?;
            print_json(&resp)
        },
        AdminAction::GetTip { host, organization, vault } => {
            let client = wire_client::build_read_client(&host, tls).map_err(io_other)?;
            let resp = client
                .get_tip(
                    wread::GetTipRequest {
                        organization: organization.map(OrganizationSlug::new),
                        vault: vault.map(VaultSlug::new),
                    },
                    request_id(),
                )
                .await
                .map_err(|e| rpc_failure("GetTip", e))?;
            print_json(&resp)
        },
    }
}

/// Generate a fresh `request_id` for a CLI-issued RPC.
///
/// The wire macro requires every RPC to carry a `u128` request id (used
/// server-side as an idempotency token in dispatch). The CLI is one-shot
/// per invocation, so a random id per call is sufficient — repeated CLI
/// invocations are independent operator actions and should not dedupe.
fn request_id() -> u128 {
    let bytes: [u8; 16] = rand::random();
    u128::from_le_bytes(bytes)
}

/// Convert a `CliWireClientError` into the binary's top-level
/// `ServerError::Server` variant.
fn io_other(e: wire_client::CliWireClientError) -> ServerError {
    ServerError::Server(Box::new(std::io::Error::other(e.to_string())))
}

/// Wrap an `RpcError` from a wire-protocol client call with operator-friendly
/// context naming the RPC that failed.
fn rpc_failure(rpc: &str, err: inferadb_ledger_wire_transport::RpcError) -> ServerError {
    ServerError::Server(Box::new(std::io::Error::other(format!("{rpc} RPC failed: {err}"))))
}

/// Print a serializable response as pretty JSON on stdout.
fn print_json<T: serde::Serialize>(value: &T) -> Result<(), ServerError> {
    let s = serde_json::to_string_pretty(value).map_err(|e| {
        ServerError::Server(Box::new(std::io::Error::other(format!(
            "failed to serialize response as JSON: {e}"
        ))))
    })?;
    println!("{s}");
    Ok(())
}

/// Parse an optional hex-encoded 16-byte idempotency key, falling back to
/// a fresh random UUID v4 when omitted.
fn parse_idempotency_key(input: Option<&str>) -> Result<bytes::Bytes, ServerError> {
    match input {
        None => {
            let uuid = uuid::Uuid::new_v4();
            Ok(bytes::Bytes::copy_from_slice(uuid.as_bytes()))
        },
        Some(s) => {
            let trimmed = s.trim_start_matches("0x");
            if trimmed.len() != 32 {
                return Err(ServerError::Server(Box::new(std::io::Error::other(format!(
                    "--idempotency-key must be 32 hex chars (16 bytes); got {} chars",
                    trimmed.len()
                )))));
            }
            let mut buf = [0u8; 16];
            for (i, byte) in buf.iter_mut().enumerate() {
                let pair = &trimmed[i * 2..i * 2 + 2];
                *byte = u8::from_str_radix(pair, 16).map_err(|e| {
                    ServerError::Server(Box::new(std::io::Error::other(format!(
                        "--idempotency-key contains non-hex characters: {e}"
                    ))))
                })?;
            }
            Ok(bytes::Bytes::copy_from_slice(&buf))
        },
    }
}

/// Map a CLI consistency string ("eventual" / "linearizable") to the
/// wire `ReadConsistency` enum.
fn parse_consistency(
    input: &str,
) -> Result<inferadb_ledger_wire::services::shared::ReadConsistency, ServerError> {
    use inferadb_ledger_wire::services::shared::ReadConsistency;
    match input.to_ascii_lowercase().as_str() {
        "eventual" => Ok(ReadConsistency::Eventual),
        "linearizable" | "linear" => Ok(ReadConsistency::Linearizable),
        other => Err(ServerError::Server(Box::new(std::io::Error::other(format!(
            "--consistency must be 'eventual' or 'linearizable'; got '{other}'"
        ))))),
    }
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
