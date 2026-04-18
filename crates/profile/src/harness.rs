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
//! Workload modules own their own timed loops. `Summary::record` is the shared
//! bookkeeping helper so each loop stays ~5 lines.

use std::time::Duration;

use inferadb_ledger_sdk::{EmailVerificationResult, LedgerClient};
use inferadb_ledger_types::{OrganizationSlug, Region, UserSlug, VaultSlug};
use snafu::{ResultExt, Snafu};

/// A bootstrapped profiling session: a connected client plus the slugs needed
/// to drive writes and checks.
pub struct Harness {
    pub client: LedgerClient,
    pub user: UserSlug,
    pub organization: OrganizationSlug,
    pub vault: VaultSlug,
}

/// Summary of a completed workload run. Printed to stderr so humans can sanity-
/// check against the flamegraph without polluting any stdout pipe the caller
/// might want clean.
#[derive(Debug, Default)]
pub struct Summary {
    pub operations: u64,
    pub errors: u64,
    pub elapsed: Duration,
}

impl Summary {
    pub fn throughput(&self) -> f64 {
        if self.elapsed.is_zero() {
            0.0
        } else {
            self.operations as f64 / self.elapsed.as_secs_f64()
        }
    }

    /// Record the outcome of a single operation. Workload loops call this after
    /// each SDK call. Errors are counted, not propagated — a transient RPC
    /// failure shouldn't abort a 60s profile run.
    pub fn record(&mut self, outcome: Result<(), inferadb_ledger_sdk::SdkError>) {
        match outcome {
            Ok(()) => self.operations += 1,
            Err(e) => {
                tracing::debug!(error = %e, "workload op error");
                self.errors += 1;
            },
        }
    }

    pub fn print(&self, preset: &str) {
        eprintln!("==================== profile summary ====================");
        eprintln!("preset:      {preset}");
        eprintln!("operations:  {}", self.operations);
        eprintln!("errors:      {}", self.errors);
        eprintln!("elapsed:     {:.2}s", self.elapsed.as_secs_f64());
        eprintln!("throughput:  {:.1} ops/s", self.throughput());
        eprintln!("=========================================================");
    }
}

/// Errors from harness setup. Only fatal failures — workload loops use
/// `Summary::record` to count transient errors.
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn throughput_reports_ops_per_second() {
        let summary = Summary { operations: 100, errors: 0, elapsed: Duration::from_secs(2) };
        assert!((summary.throughput() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn throughput_is_zero_for_zero_elapsed() {
        let summary = Summary { operations: 100, errors: 0, elapsed: Duration::ZERO };
        assert_eq!(summary.throughput(), 0.0);
    }

    #[test]
    fn record_counts_ok_and_err_separately() {
        let mut summary = Summary::default();
        summary.record(Ok(()));
        summary.record(Ok(()));
        summary.record(Err(inferadb_ledger_sdk::SdkError::Connection {
            message: "synthetic".to_string(),
        }));
        assert_eq!(summary.operations, 2);
        assert_eq!(summary.errors, 1);
    }
}
