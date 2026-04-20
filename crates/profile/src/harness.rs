//! Shared harness: connection, user+org+vault bootstrap, summary struct.
//!
//! The bootstrap flow mirrors the integration-test pattern in
//! `crates/server/tests/onboarding.rs`:
//!   1. `initiate_email_verification(email, region)` → `EmailVerificationCode { code }`.
//!   2. `verify_email_code(email, code, region)` → `EmailVerificationResult::NewUser {
//!      onboarding_token }`.
//!   3. `complete_registration(token, email, region, name, org_name)` — creates the user AND
//!      organization in one RPC.
//!   4. `create_vault(caller, organization)` — creates the vault we'll write to.
//!
//! Workload modules own their own timed loops. `Summary::record_timed` is the
//! shared bookkeeping helper so each loop stays ~5 lines AND records per-op
//! latency for percentile computation.

use std::time::Duration;

use inferadb_ledger_sdk::{EmailVerificationResult, LedgerClient};
use inferadb_ledger_types::{OrganizationSlug, Region, UserSlug, VaultSlug};
use serde::Serialize;
use snafu::{ResultExt, Snafu};

/// A bootstrapped profiling session: a connected client plus the slugs needed
/// to drive writes and checks.
pub struct Harness {
    pub client: LedgerClient,
    pub user: UserSlug,
    pub organization: OrganizationSlug,
    pub vault: VaultSlug,
}

/// Latency percentiles over the set of recorded successful operations, in
/// nanoseconds. All zero when no operations have been recorded.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct Percentiles {
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

/// Summary of a completed workload run. Printed to stderr so humans can sanity-
/// check against the flamegraph without polluting any stdout pipe the caller
/// might want clean.
#[derive(Debug, Default)]
pub struct Summary {
    pub operations: u64,
    pub errors: u64,
    pub elapsed: Duration,
    /// Nanoseconds per successful op. Internal — callers consume
    /// `percentiles()` / `MetricsReport` instead.
    latencies: Vec<u64>,
}

impl Summary {
    pub fn throughput(&self) -> f64 {
        if self.elapsed.is_zero() {
            0.0
        } else {
            self.operations as f64 / self.elapsed.as_secs_f64()
        }
    }

    /// Record the outcome of a single operation along with how long it took.
    /// Workload loops call this after each SDK call. Errors are counted, not
    /// propagated — a transient RPC failure shouldn't abort a 60s profile run.
    /// Successful op latencies feed `Summary::percentiles`.
    pub fn record_timed(
        &mut self,
        outcome: Result<(), inferadb_ledger_sdk::SdkError>,
        duration: Duration,
    ) {
        match outcome {
            Ok(()) => {
                self.operations += 1;
                let ns = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
                self.latencies.push(ns);
            },
            Err(e) => {
                tracing::debug!(error = %e, "workload op error");
                self.errors += 1;
            },
        }
    }

    /// Merge another summary's operation counts, error counts, and per-op
    /// latencies into this one. Used by the `concurrent-writes` workload to
    /// combine per-task results into one run-wide summary. Intentionally does
    /// not touch `elapsed` — the concurrent driver sets wall-clock elapsed
    /// from its outer `start.elapsed()`, not the sum of per-task durations.
    pub fn merge(&mut self, other: Summary) {
        self.operations += other.operations;
        self.errors += other.errors;
        self.latencies.extend(other.latencies);
    }

    /// Compute percentiles from the recorded successful-op latencies. Returns
    /// zeros if no latencies have been recorded.
    pub fn percentiles(&self) -> Percentiles {
        if self.latencies.is_empty() {
            return Percentiles::default();
        }
        let mut sorted = self.latencies.clone();
        sorted.sort_unstable();
        let last = sorted.len() - 1;
        let pct = |p: f64| -> u64 {
            let idx = ((sorted.len() as f64 * p) as usize).min(last);
            sorted[idx]
        };
        Percentiles {
            p50_ns: pct(0.5),
            p95_ns: pct(0.95),
            p99_ns: pct(0.99),
            p999_ns: pct(0.999),
            max_ns: sorted[last],
        }
    }

    pub fn print(&self, preset: &str) {
        let pct = self.percentiles();
        eprintln!("==================== profile summary ====================");
        eprintln!("preset:      {preset}");
        eprintln!("operations:  {}", self.operations);
        eprintln!("errors:      {}", self.errors);
        eprintln!("elapsed:     {:.2}s", self.elapsed.as_secs_f64());
        eprintln!("throughput:  {:.1} ops/s", self.throughput());
        eprintln!(
            "latency ns:  p50={}  p95={}  p99={}  p999={}  max={}",
            pct.p50_ns, pct.p95_ns, pct.p99_ns, pct.p999_ns, pct.max_ns
        );
        eprintln!("=========================================================");
    }

    /// Bundle the summary + percentiles into a serializable report for
    /// `--metrics-json` output. Consumed by `main.rs` when the flag is set.
    pub fn to_report(
        &self,
        preset: &str,
        endpoint: &str,
        target_duration_secs: u64,
    ) -> MetricsReport {
        MetricsReport {
            preset: preset.to_string(),
            endpoint: endpoint.to_string(),
            target_duration_secs,
            operations: self.operations,
            errors: self.errors,
            elapsed_secs: self.elapsed.as_secs_f64(),
            throughput_ops_per_sec: self.throughput(),
            latency_ns: self.percentiles(),
        }
    }
}

/// Structured, machine-readable report for a completed workload run. Emitted
/// via `--metrics-json <path>` and aggregated by `scripts/profile-suite-report.sh`.
#[derive(Debug, Serialize)]
pub struct MetricsReport {
    pub preset: String,
    pub endpoint: String,
    pub target_duration_secs: u64,
    pub operations: u64,
    pub errors: u64,
    pub elapsed_secs: f64,
    pub throughput_ops_per_sec: f64,
    pub latency_ns: Percentiles,
}

/// Errors from harness setup. Only fatal failures — workload loops use
/// `Summary::record_timed` to count transient errors.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum HarnessError {
    #[snafu(display("failed to connect to {endpoint}: {source}"))]
    Connect {
        endpoint: String,
        source: inferadb_ledger_sdk::SdkError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
    #[snafu(display("failed to initiate email verification: {source}"))]
    InitiateEmail {
        source: inferadb_ledger_sdk::SdkError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
    #[snafu(display("failed to verify email code: {source}"))]
    VerifyEmail {
        source: inferadb_ledger_sdk::SdkError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
    #[snafu(display("failed to complete registration: {source}"))]
    CompleteRegistration {
        source: inferadb_ledger_sdk::SdkError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
    #[snafu(display("failed to create vault: {source}"))]
    CreateVault {
        source: inferadb_ledger_sdk::SdkError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
    #[snafu(display("complete_registration did not return an organization slug"))]
    MissingOrganization {
        #[snafu(implicit)]
        location: snafu::Location,
    },
    #[snafu(display("verify_email_code did not return a new-user onboarding token"))]
    NotNewUser {
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

impl Harness {
    /// Connect to `endpoint`, onboard a synthetic user, create an org and vault.
    ///
    /// The email is synthesized from a nanosecond-precision timestamp so repeat
    /// runs against the same server don't collide on user uniqueness.
    pub async fn bootstrap(endpoint: &str) -> Result<Self, HarnessError> {
        tracing::info!(endpoint, "connecting");
        let client = LedgerClient::connect(endpoint, "profile-workload")
            .await
            .with_context(|_| ConnectSnafu { endpoint: endpoint.to_string() })?;

        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let email = format!("profile-{suffix}@example.com");
        let name = "Profile Workload";
        let org_name = format!("profile-{suffix}");
        let region = Region::US_EAST_VA;

        tracing::info!(%email, "initiating email verification");
        let verification =
            client.initiate_email_verification(&email, region).await.context(InitiateEmailSnafu)?;

        tracing::info!("verifying email code");
        let verify_result = client
            .verify_email_code(&email, verification.code, region)
            .await
            .context(VerifyEmailSnafu)?;

        let onboarding_token = match verify_result {
            EmailVerificationResult::NewUser { onboarding_token } => onboarding_token,
            EmailVerificationResult::ExistingUser { .. }
            | EmailVerificationResult::TotpRequired { .. } => {
                return NotNewUserSnafu.fail();
            },
        };

        tracing::info!("completing registration");
        let registration = client
            .complete_registration(onboarding_token, &email, region, name, &org_name)
            .await
            .context(CompleteRegistrationSnafu)?;

        let user = registration.user;
        let organization =
            registration.organization.ok_or_else(|| MissingOrganizationSnafu.build())?;

        tracing::info!(user = %user.value(), organization = %organization.value(), "creating vault");
        let vault_info = client.create_vault(user, organization).await.context(CreateVaultSnafu)?;

        Ok(Self { client, user, organization, vault: vault_info.vault })
    }

    /// Provisions `n` additional vaults under the harness's existing org +
    /// user, returning their slugs. Used by multi-vault workloads that need
    /// to fan writes across multiple Raft shards / WAL streams.
    ///
    /// The harness's own `vault` is unaffected — callers typically discard
    /// it or use it as one of the N targets.
    pub async fn provision_vaults(&self, n: usize) -> Result<Vec<VaultSlug>, HarnessError> {
        let mut vaults = Vec::with_capacity(n);
        for _ in 0..n {
            let info = self
                .client
                .create_vault(self.user, self.organization)
                .await
                .context(CreateVaultSnafu)?;
            vaults.push(info.vault);
        }
        Ok(vaults)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn throughput_reports_ops_per_second() {
        let summary = Summary {
            operations: 100,
            errors: 0,
            elapsed: Duration::from_secs(2),
            latencies: vec![],
        };
        assert!((summary.throughput() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn throughput_is_zero_for_zero_elapsed() {
        let summary =
            Summary { operations: 100, errors: 0, elapsed: Duration::ZERO, latencies: vec![] };
        assert_eq!(summary.throughput(), 0.0);
    }

    #[test]
    fn percentiles_zero_when_empty() {
        let summary = Summary::default();
        let p = summary.percentiles();
        assert_eq!(p.p50_ns, 0);
        assert_eq!(p.p95_ns, 0);
        assert_eq!(p.p99_ns, 0);
        assert_eq!(p.p999_ns, 0);
        assert_eq!(p.max_ns, 0);
    }

    #[test]
    fn percentiles_single_value() {
        let mut summary = Summary::default();
        summary.record_timed(Ok(()), Duration::from_nanos(42));
        let p = summary.percentiles();
        assert_eq!(p.p50_ns, 42);
        assert_eq!(p.max_ns, 42);
    }

    #[test]
    fn percentiles_interpolate_expected_buckets() {
        let mut summary = Summary::default();
        for ns in 1u64..=1000 {
            summary.record_timed(Ok(()), Duration::from_nanos(ns));
        }
        let p = summary.percentiles();
        // ((1000.0 * 0.5) as usize).min(999) = 500, sorted[500] = 501
        assert_eq!(p.p50_ns, 501);
        assert_eq!(p.p95_ns, 951);
        assert_eq!(p.p99_ns, 991);
        assert_eq!(p.p999_ns, 1000);
        assert_eq!(p.max_ns, 1000);
    }

    #[test]
    fn record_timed_separates_ok_and_err() {
        let mut summary = Summary::default();
        summary.record_timed(Ok(()), Duration::from_nanos(10));
        summary.record_timed(
            Err(inferadb_ledger_sdk::SdkError::Connection { message: "synthetic".to_string() }),
            Duration::from_nanos(999),
        );
        assert_eq!(summary.operations, 1);
        assert_eq!(summary.errors, 1);
        assert_eq!(summary.latencies, vec![10]); // errors don't contribute latency
    }

    #[test]
    fn metrics_report_round_trips_via_serde_json() {
        let mut summary = Summary::default();
        summary.record_timed(Ok(()), Duration::from_nanos(100));
        summary.elapsed = Duration::from_secs(1);
        let report = summary.to_report("test-preset", "http://x:50051", 30);
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"preset\":\"test-preset\""));
        assert!(json.contains("\"p50_ns\":100"));
        assert!(json.contains("\"operations\":1"));
    }
}
