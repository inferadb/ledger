//! InferaDB Ledger profiling workload driver.
//!
//! Drives reproducible load against a running ledger node for flamegraph capture.
//! See `docs/how-to/profiling.md` for usage.

use clap::Parser;
use snafu::{IntoError, ResultExt, Snafu};

mod harness;
mod workloads;

/// Top-level error for the profile binary.
///
/// The enum is extended as subsystems are introduced; Task 2 seeded it empty,
/// Task 6 adds the harness-bootstrap variant, Suite B adds the metrics-JSON
/// emission path.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum MainError {
    #[snafu(display("harness bootstrap failed: {source}"))]
    Harness {
        /// Boxed to keep `Result<(), MainError>` small — `HarnessError` wraps
        /// `SdkError`, which is ~200 bytes, and main's error return tripped
        /// clippy `result_large_err` unboxed. Precedent:
        /// `crates/state/src/system/service/mod.rs`.
        source: Box<harness::HarnessError>,
        #[snafu(implicit)]
        location: snafu::Location,
    },
    #[snafu(display("failed to write metrics JSON to {}: {source}", path.display()))]
    WriteMetrics {
        path: std::path::PathBuf,
        source: std::io::Error,
        #[snafu(implicit)]
        location: snafu::Location,
    },
    #[snafu(display("failed to serialize metrics report: {source}"))]
    SerializeMetrics {
        source: serde_json::Error,
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

#[derive(Parser, Debug)]
#[command(name = "inferadb-ledger-profile", version, about = "Profiling workload driver")]
struct Cli {
    #[command(subcommand)]
    preset: Preset,
}

#[derive(clap::Subcommand, Debug)]
enum Preset {
    /// Tight write loop — one concurrent writer, fixed key-space.
    ThroughputWrites(PresetArgs),
    /// 70/30 writes-to-reads mix against a pre-seeded vault.
    MixedRw(PresetArgs),
    /// 90% authorization checks, 10% writes against a pre-seeded vault.
    CheckHeavy(PresetArgs),
    /// Pure read loop against 10k pre-seeded entities.
    EntityReads(PresetArgs),
    /// Pure create_relationship loop through a 10×10×10 tuple space.
    RelationshipWrites(PresetArgs),
    /// Pure check_relationship loop against 1k pre-seeded relationships.
    RelationshipReads(PresetArgs),
    /// N concurrent writers. Tests whether Raft's BatchWriter amortizes WAL
    /// fsync under concurrent load. Each task owns its own key-prefix (no
    /// write-write contention).
    ConcurrentWrites(ConcurrentWritesArgs),
    /// N concurrent readers against a pre-seeded 1k-entry corpus. Measures
    /// read throughput under concurrent gRPC streams on one HTTP/2 channel.
    /// Uses `ReadConsistency::Eventual` (lock-free page-cache path).
    ConcurrentReads(ConcurrentReadsArgs),
    /// N concurrent writers fanned across M vaults. Exposes multi-shard
    /// scaling past the single-vault WAL-fsync ceiling. Task `i` writes to
    /// `vaults[i % M]`.
    ConcurrentWritesMultivault(ConcurrentWritesMultivaultArgs),
    /// N concurrent writers fanned across M organizations. Validates
    /// shard-based parallelism — each org routes to its own Raft shard via
    /// `ShardManager`, so writes land on distinct apply pipelines. Task `i`
    /// writes to `orgs[i % M]`.
    ConcurrentWritesMultiorg(ConcurrentWritesMultiorgArgs),
}

#[derive(clap::Args, Debug, Clone)]
struct PresetArgs {
    /// gRPC endpoint of the ledger node (e.g. http://127.0.0.1:50051).
    #[arg(long)]
    endpoint: String,
    /// Measured-phase duration in seconds.
    #[arg(long, default_value_t = 60)]
    duration: u64,
    /// Write a structured MetricsReport to this path on completion. Consumed
    /// by scripts/profile-suite.sh to aggregate across workloads.
    #[arg(long, value_name = "PATH")]
    metrics_json: Option<std::path::PathBuf>,
}

/// Args for the `concurrent-writes` preset — the base preset args plus a
/// `--concurrency` knob for the number of writer tasks to spawn.
#[derive(clap::Args, Debug, Clone)]
struct ConcurrentWritesArgs {
    #[command(flatten)]
    base: PresetArgs,

    /// Number of concurrent writer tasks. Each task runs its own serial write
    /// loop against its own key-prefix.
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
}

/// Args for the `concurrent-reads` preset — the base preset args plus a
/// `--concurrency` knob for the number of reader tasks to spawn.
#[derive(clap::Args, Debug, Clone)]
struct ConcurrentReadsArgs {
    #[command(flatten)]
    base: PresetArgs,

    /// Number of concurrent reader tasks. All tasks share the seeded
    /// key-space — reads don't contend, so no per-task partitioning.
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
}

/// Args for the `concurrent-writes-multivault` preset — base preset args
/// plus `--concurrency N` and `--vaults M`. Task `i` is pinned to
/// `vaults[i % M]`.
#[derive(clap::Args, Debug, Clone)]
struct ConcurrentWritesMultivaultArgs {
    #[command(flatten)]
    base: PresetArgs,

    /// Number of concurrent writer tasks.
    #[arg(long, default_value_t = 32)]
    concurrency: usize,

    /// Number of vaults to fan writes across. Provisioned at bootstrap.
    #[arg(long, default_value_t = 16)]
    vaults: usize,

    /// When set, every concurrent task reuses the harness's bootstrap
    /// `LedgerClient` (cloned). The `LedgerClient` wraps a single tonic
    /// `Channel`, so a shared client funnels every request through one
    /// TCP/HTTP-2 connection — the production-realistic shape for a
    /// service that reuses one SDK instance. When unset (default), each
    /// task constructs its own `LedgerClient` via `connect_task_client`,
    /// which produces one TCP/HTTP-2 connection per task.
    ///
    /// Used to diagnose the per-`client_id` contention ceiling: the
    /// asymmetry between shared and per-task at the same total request
    /// rate isolates connection-pinned bottlenecks (h2 frame dispatch,
    /// tower `concurrency_limit`, etc.) from per-`client_id` server
    /// state.
    #[arg(long)]
    shared_client: bool,

    /// Override `ClientConfig::connection_pool_size` for every
    /// `LedgerClient` created by the workload — applies to the shared
    /// client (when `--shared-client` is set) and to per-task clients
    /// otherwise. Default `0` falls back to the SDK default of `1`,
    /// preserving the original single-channel behavior. Higher values
    /// let one client drive multiple parallel tonic `Buffer` workers,
    /// trading TCP/HTTP-2 footprint for dispatch parallelism.
    #[arg(long, default_value_t = 0)]
    pool_size: u8,
}

/// Args for the `concurrent-writes-multiorg` preset — base preset args
/// plus `--concurrency N` and `--orgs M`. Task `i` is pinned to
/// `orgs[i % M]`. Each org provisions as a fully-onboarded user with one
/// vault; orgs route to distinct Raft shards via the cluster's
/// `ShardManager`.
#[derive(clap::Args, Debug, Clone)]
struct ConcurrentWritesMultiorgArgs {
    #[command(flatten)]
    base: PresetArgs,

    /// Number of concurrent writer tasks.
    #[arg(long, default_value_t = 32)]
    concurrency: usize,

    /// Number of organizations to fan writes across. Provisioned at
    /// bootstrap via N full onboarding flows. Setup cost scales linearly
    /// with M but is paid once per run.
    #[arg(long, default_value_t = 4)]
    orgs: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), MainError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let (args, preset_name) = match &cli.preset {
        Preset::ThroughputWrites(a) => (a, "throughput-writes"),
        Preset::MixedRw(a) => (a, "mixed-rw"),
        Preset::CheckHeavy(a) => (a, "check-heavy"),
        Preset::EntityReads(a) => (a, "entity-reads"),
        Preset::RelationshipWrites(a) => (a, "relationship-writes"),
        Preset::RelationshipReads(a) => (a, "relationship-reads"),
        Preset::ConcurrentWrites(a) => (&a.base, "concurrent-writes"),
        Preset::ConcurrentReads(a) => (&a.base, "concurrent-reads"),
        Preset::ConcurrentWritesMultivault(a) => (&a.base, "concurrent-writes-multivault"),
        Preset::ConcurrentWritesMultiorg(a) => (&a.base, "concurrent-writes-multiorg"),
    };

    let duration = std::time::Duration::from_secs(args.duration);
    tracing::info!(preset = preset_name, endpoint = %args.endpoint, ?duration, "starting");

    let harness = harness::Harness::bootstrap(&args.endpoint)
        .await
        .map_err(|e| HarnessSnafu.into_error(Box::new(e)))?;

    let summary = match &cli.preset {
        Preset::ThroughputWrites(_) => workloads::throughput_writes::run(&harness, duration).await,
        Preset::MixedRw(_) => workloads::mixed_rw::run(&harness, duration).await,
        Preset::CheckHeavy(_) => workloads::check_heavy::run(&harness, duration).await,
        Preset::EntityReads(_) => workloads::entity_reads::run(&harness, duration).await,
        Preset::RelationshipWrites(_) => {
            workloads::relationship_writes::run(&harness, duration).await
        },
        Preset::RelationshipReads(_) => {
            workloads::relationship_reads::run(&harness, duration).await
        },
        Preset::ConcurrentWrites(a) => {
            workloads::concurrent_writes::run(&harness, duration, a.concurrency).await
        },
        Preset::ConcurrentReads(a) => {
            workloads::concurrent_reads::run(&harness, duration, a.concurrency).await
        },
        Preset::ConcurrentWritesMultivault(a) => {
            let vaults = harness
                .provision_vaults(a.vaults.max(1))
                .await
                .map_err(|e| HarnessSnafu.into_error(Box::new(e)))?;
            tracing::info!(
                vaults = vaults.len(),
                concurrency = a.concurrency,
                "concurrent-writes-multivault: provisioned vaults"
            );
            workloads::concurrent_writes_multivault::run(
                &harness,
                duration,
                a.concurrency,
                vaults,
                a.shared_client,
                a.pool_size,
            )
            .await
        },
        Preset::ConcurrentWritesMultiorg(a) => {
            let mut targets = harness
                .provision_orgs(a.orgs.saturating_sub(1).max(0))
                .await
                .map_err(|e| HarnessSnafu.into_error(Box::new(e)))?;
            // Include the harness's own org as the first target so `--orgs 1`
            // is a valid single-shard baseline.
            targets.insert(0, (harness.user, harness.organization, harness.vault));
            tracing::info!(
                orgs = targets.len(),
                concurrency = a.concurrency,
                "concurrent-writes-multiorg: provisioned orgs"
            );
            workloads::concurrent_writes_multiorg::run(&harness, duration, a.concurrency, targets)
                .await
        },
    };

    summary.print(preset_name);

    if let Some(path) = args.metrics_json.as_ref() {
        let report = summary.to_report(preset_name, &args.endpoint, args.duration);
        let json = serde_json::to_string_pretty(&report).context(SerializeMetricsSnafu)?;
        std::fs::write(path, json).with_context(|_| WriteMetricsSnafu { path: path.clone() })?;
        tracing::info!(path = %path.display(), "wrote metrics report");
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn concurrent_writes_parses_with_explicit_concurrency() {
        let cli = Cli::try_parse_from([
            "inferadb-ledger-profile",
            "concurrent-writes",
            "--endpoint",
            "http://127.0.0.1:50051",
            "--duration",
            "30",
            "--concurrency",
            "16",
        ])
        .unwrap();
        match cli.preset {
            Preset::ConcurrentWrites(a) => {
                assert_eq!(a.base.endpoint, "http://127.0.0.1:50051");
                assert_eq!(a.base.duration, 30);
                assert_eq!(a.concurrency, 16);
                assert!(a.base.metrics_json.is_none());
            },
            other => panic!("expected ConcurrentWrites, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_writes_defaults_concurrency_to_32() {
        let cli = Cli::try_parse_from([
            "inferadb-ledger-profile",
            "concurrent-writes",
            "--endpoint",
            "http://127.0.0.1:50051",
        ])
        .unwrap();
        match cli.preset {
            Preset::ConcurrentWrites(a) => assert_eq!(a.concurrency, 32),
            other => panic!("expected ConcurrentWrites, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_reads_parses_with_explicit_concurrency() {
        let cli = Cli::try_parse_from([
            "inferadb-ledger-profile",
            "concurrent-reads",
            "--endpoint",
            "http://127.0.0.1:50051",
            "--duration",
            "30",
            "--concurrency",
            "16",
        ])
        .unwrap();
        match cli.preset {
            Preset::ConcurrentReads(a) => {
                assert_eq!(a.base.endpoint, "http://127.0.0.1:50051");
                assert_eq!(a.base.duration, 30);
                assert_eq!(a.concurrency, 16);
                assert!(a.base.metrics_json.is_none());
            },
            other => panic!("expected ConcurrentReads, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_reads_defaults_concurrency_to_32() {
        let cli = Cli::try_parse_from([
            "inferadb-ledger-profile",
            "concurrent-reads",
            "--endpoint",
            "http://127.0.0.1:50051",
        ])
        .unwrap();
        match cli.preset {
            Preset::ConcurrentReads(a) => assert_eq!(a.concurrency, 32),
            other => panic!("expected ConcurrentReads, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_writes_multivault_parses_with_both_knobs() {
        let cli = Cli::try_parse_from([
            "inferadb-ledger-profile",
            "concurrent-writes-multivault",
            "--endpoint",
            "http://127.0.0.1:50051",
            "--duration",
            "30",
            "--concurrency",
            "64",
            "--vaults",
            "8",
        ])
        .unwrap();
        match cli.preset {
            Preset::ConcurrentWritesMultivault(a) => {
                assert_eq!(a.base.endpoint, "http://127.0.0.1:50051");
                assert_eq!(a.base.duration, 30);
                assert_eq!(a.concurrency, 64);
                assert_eq!(a.vaults, 8);
                assert!(!a.shared_client);
                assert_eq!(a.pool_size, 0);
            },
            other => panic!("expected ConcurrentWritesMultivault, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_writes_multivault_defaults() {
        let cli = Cli::try_parse_from([
            "inferadb-ledger-profile",
            "concurrent-writes-multivault",
            "--endpoint",
            "http://127.0.0.1:50051",
        ])
        .unwrap();
        match cli.preset {
            Preset::ConcurrentWritesMultivault(a) => {
                assert_eq!(a.concurrency, 32);
                assert_eq!(a.vaults, 16);
                assert!(!a.shared_client);
                assert_eq!(a.pool_size, 0);
            },
            other => panic!("expected ConcurrentWritesMultivault, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_writes_multiorg_parses_with_both_knobs() {
        let cli = Cli::try_parse_from([
            "inferadb-ledger-profile",
            "concurrent-writes-multiorg",
            "--endpoint",
            "http://127.0.0.1:50051",
            "--duration",
            "30",
            "--concurrency",
            "128",
            "--orgs",
            "8",
        ])
        .unwrap();
        match cli.preset {
            Preset::ConcurrentWritesMultiorg(a) => {
                assert_eq!(a.base.endpoint, "http://127.0.0.1:50051");
                assert_eq!(a.base.duration, 30);
                assert_eq!(a.concurrency, 128);
                assert_eq!(a.orgs, 8);
            },
            other => panic!("expected ConcurrentWritesMultiorg, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_writes_multiorg_defaults() {
        let cli = Cli::try_parse_from([
            "inferadb-ledger-profile",
            "concurrent-writes-multiorg",
            "--endpoint",
            "http://127.0.0.1:50051",
        ])
        .unwrap();
        match cli.preset {
            Preset::ConcurrentWritesMultiorg(a) => {
                assert_eq!(a.concurrency, 32);
                assert_eq!(a.orgs, 4);
            },
            other => panic!("expected ConcurrentWritesMultiorg, got {other:?}"),
        }
    }
}
