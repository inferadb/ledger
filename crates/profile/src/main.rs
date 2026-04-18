//! InferaDB Ledger profiling workload driver.
//!
//! Drives reproducible load against a running ledger node for flamegraph capture.
//! See `docs/operations/profiling.md` for usage.

use clap::Parser;
use snafu::{IntoError, Snafu};

mod harness;
mod workloads;

/// Top-level error for the profile binary.
///
/// The enum is extended as subsystems are introduced; Task 2 seeded it empty,
/// Task 6 adds the harness-bootstrap variant.
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
    /// 70/30 writes-to-checks mix against a pre-seeded vault.
    MixedRw(PresetArgs),
    /// 90% authorization checks, 10% writes against a pre-seeded vault.
    CheckHeavy(PresetArgs),
}

#[derive(clap::Args, Debug, Clone)]
struct PresetArgs {
    /// gRPC endpoint of the ledger node (e.g. http://127.0.0.1:50051).
    #[arg(long)]
    endpoint: String,
    /// Measured-phase duration in seconds.
    #[arg(long, default_value_t = 60)]
    duration: u64,
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
    };

    summary.print(preset_name);
    Ok(())
}
