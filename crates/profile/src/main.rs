//! InferaDB Ledger profiling workload driver.
//!
//! Drives reproducible load against a running ledger node for flamegraph capture.
//! See `docs/operations/profiling.md` for usage.

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
}
