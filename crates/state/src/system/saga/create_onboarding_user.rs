//! Create Onboarding User saga state machine and step definitions.

use std::time::Duration;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{OrganizationId, Region, UserId, UserSlug};
use serde::{Deserialize, Serialize};

use super::{MAX_BACKOFF, MAX_RETRIES, OrganizationSlug, SagaId};

/// State machine for the Create Onboarding User saga.
///
/// A 3-step saga modeled on `CreateUserSaga` and `CreateOrganizationSaga`:
/// 1. GLOBAL (step 0): Allocate IDs, reserve HMAC as `Provisioning`, create directory entries
/// 2. Regional (step 1): Write all PII, `UserEmail`, `UserShredKey`, `RefreshToken`, org profile
/// 3. GLOBAL (step 2): Activate directories, update HMAC from `Provisioning` to `Active`
///
/// PII is NOT stored in the saga state — it is passed in-memory via
/// `SagaSubmission.pii` from the service handler. If the leader crashes,
/// PII is lost and the saga compensates step 0 (rolls back reservations).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CreateOnboardingUserSagaState {
    /// Initial state: about to allocate IDs and reserve HMAC in GLOBAL.
    Pending,
    /// Step 0 complete: IDs allocated, HMAC reserved, directories created in GLOBAL.
    IdsAllocated {
        /// Allocated internal user ID.
        user_id: UserId,
        /// Allocated internal organization ID.
        organization_id: OrganizationId,
    },
    /// Step 1 complete: PII and session material written to regional store.
    RegionalDataWritten {
        /// User's internal ID.
        user_id: UserId,
        /// Organization's internal ID.
        organization_id: OrganizationId,
        /// Refresh token ID assigned in step 1.
        refresh_token_id: inferadb_ledger_types::RefreshTokenId,
    },
    /// Step 2 complete: directories activated, HMAC updated to Active.
    Completed {
        /// Created user's internal ID.
        user_id: UserId,
        /// Created organization's internal ID.
        organization_id: OrganizationId,
    },
    /// Saga failed after exhausting retries.
    Failed {
        /// The step that failed (0-indexed).
        step: u8,
        /// Error description.
        error: String,
    },
    /// Saga failed and compensation completed.
    Compensated {
        /// The step that failed.
        step: u8,
        /// Summary of compensation actions taken.
        cleanup_summary: String,
    },
    /// Saga exceeded the configured timeout and was auto-compensated.
    TimedOut,
}

/// Input parameters for Create Onboarding User saga.
///
/// NO PII — only IDs, HMACs, and pre-generated slugs. PII (email, name,
/// organization_name) is held in-memory by the service handler and passed
/// to the saga orchestrator via the submit channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOnboardingUserInput {
    /// HMAC of the email address (deterministic key for uniqueness).
    pub email_hmac: String,
    /// Data residency region.
    pub region: Region,
    /// Pre-generated external Snowflake slug for the user.
    pub user_slug: UserSlug,
    /// Pre-generated external Snowflake slug for the organization.
    pub organization_slug: OrganizationSlug,
}

/// Record for the Create Onboarding User saga, tracking state and retry progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOnboardingUserSaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: CreateOnboardingUserSagaState,
    /// Input parameters.
    pub input: CreateOnboardingUserInput,
    /// When the saga was created.
    pub created_at: DateTime<Utc>,
    /// When the saga was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of retry attempts.
    pub retries: u8,
    /// Next retry time (for exponential backoff).
    pub next_retry_at: Option<DateTime<Utc>>,
    /// W3C `traceparent` of the originating request, propagated into
    /// downstream regional proposals so distributed traces stay linked.
    pub traceparent: Option<String>,
}

impl CreateOnboardingUserSaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: CreateOnboardingUserInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: CreateOnboardingUserSagaState::Pending,
            input,
            created_at: now,
            updated_at: now,
            retries: 0,
            next_retry_at: None,
            traceparent: None,
        }
    }

    /// Attaches a W3C `traceparent` header value to this saga.
    #[must_use]
    pub fn with_traceparent(mut self, traceparent: Option<String>) -> Self {
        self.traceparent = traceparent;
        self
    }

    /// Checks if the saga is complete (success or permanently failed).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            CreateOnboardingUserSagaState::Completed { .. }
                | CreateOnboardingUserSagaState::Failed { .. }
                | CreateOnboardingUserSagaState::Compensated { .. }
                | CreateOnboardingUserSagaState::TimedOut
        )
    }

    /// Checks if the saga is ready for retry.
    pub fn is_ready_for_retry(&self) -> bool {
        if self.is_terminal() {
            return false;
        }
        match self.next_retry_at {
            Some(retry_at) => Utc::now() >= retry_at,
            None => true,
        }
    }

    /// Checks if the saga has exceeded the given timeout.
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        let elapsed = Utc::now() - self.created_at;
        elapsed > chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::MAX)
    }

    /// Calculates next backoff duration using exponential backoff.
    pub fn next_backoff(&self) -> Duration {
        let base = Duration::from_secs(1);
        let backoff = base * 2u32.saturating_pow(self.retries as u32);
        std::cmp::min(backoff, MAX_BACKOFF)
    }

    /// Schedules a retry with exponential backoff.
    pub fn schedule_retry(&mut self) {
        self.retries = self.retries.saturating_add(1);
        let backoff = self.next_backoff();
        self.next_retry_at =
            Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        self.updated_at = Utc::now();
    }

    /// Transitions to a new state.
    pub fn transition(&mut self, new_state: CreateOnboardingUserSagaState) {
        self.state = new_state;
        self.updated_at = Utc::now();
        self.next_retry_at = None;
    }

    /// Marks as failed with error.
    ///
    /// After `MAX_RETRIES` failures, transitions to terminal Failed state.
    pub fn fail(&mut self, step: u8, error: String) {
        self.retries = self.retries.saturating_add(1);
        if self.retries >= MAX_RETRIES {
            self.state = CreateOnboardingUserSagaState::Failed { step, error };
        } else {
            let backoff = self.next_backoff();
            self.next_retry_at =
                Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        }
        self.updated_at = Utc::now();
    }

    /// Immediately transitions to `Failed` without consuming retries.
    ///
    /// Used for unrecoverable errors like `PiiLost` where retrying
    /// cannot succeed. Skips the retry counter and backoff entirely.
    pub fn force_fail(&mut self, step: u8, error: String) {
        self.state = CreateOnboardingUserSagaState::Failed { step, error };
        self.updated_at = Utc::now();
    }

    /// Returns the step number for the current state.
    pub fn current_step(&self) -> u8 {
        match &self.state {
            CreateOnboardingUserSagaState::Pending => 0,
            CreateOnboardingUserSagaState::IdsAllocated { .. } => 1,
            CreateOnboardingUserSagaState::RegionalDataWritten { .. } => 2,
            CreateOnboardingUserSagaState::Completed { .. }
            | CreateOnboardingUserSagaState::Failed { .. }
            | CreateOnboardingUserSagaState::Compensated { .. }
            | CreateOnboardingUserSagaState::TimedOut => 0,
        }
    }

    /// Returns the target region for the current step.
    ///
    /// Steps 0, 2 target GLOBAL; step 1 targets the user's declared region.
    pub fn target_region(&self) -> Region {
        match &self.state {
            CreateOnboardingUserSagaState::Pending => Region::GLOBAL,
            CreateOnboardingUserSagaState::IdsAllocated { .. } => self.input.region,
            CreateOnboardingUserSagaState::RegionalDataWritten { .. } => Region::GLOBAL,
            CreateOnboardingUserSagaState::Completed { .. }
            | CreateOnboardingUserSagaState::Failed { .. }
            | CreateOnboardingUserSagaState::Compensated { .. }
            | CreateOnboardingUserSagaState::TimedOut => Region::GLOBAL,
        }
    }
}
