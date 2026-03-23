//! Create User saga state machine and step definitions.

use std::time::Duration;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{Region, UserId, UserSlug};
use serde::{Deserialize, Serialize};

use super::{MAX_BACKOFF, MAX_RETRIES, OrganizationTier, SagaId};

/// State machine for the Create User saga.
///
/// Coordinates writes across GLOBAL + regional Raft groups:
/// 1. GLOBAL: allocate UserId/UserSlug, CAS email HMAC (reserves uniqueness)
/// 2. Regional: create User, UserEmail, UserShredKey
/// 3. GLOBAL: create UserDirectoryEntry + slug index
///
/// Compensation in reverse: step 3 → delete directory/slug index,
/// step 2 → delete User/UserEmail/UserShredKey from regional store,
/// step 1 → delete email HMAC from GLOBAL (releases reservation).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CreateUserSagaState {
    /// Initial state: about to allocate IDs and reserve email in GLOBAL.
    Pending,
    /// Step 1 complete: UserId, UserSlug allocated, email HMAC reserved in GLOBAL.
    EmailReserved {
        /// Allocated internal user ID.
        user_id: UserId,
        /// Allocated external Snowflake slug.
        user_slug: UserSlug,
        /// Hex-encoded HMAC used for email uniqueness.
        hmac_hex: String,
    },
    /// Step 2 complete: User, UserEmail, UserShredKey created in regional store.
    RegionalDataWritten {
        /// The user's internal ID.
        user_id: UserId,
        /// The user's external slug.
        user_slug: UserSlug,
        /// HMAC hex for compensation reference.
        hmac_hex: String,
    },
    /// Step 3 complete: directory entry and slug index created in GLOBAL.
    Completed {
        /// Created user's internal ID.
        user_id: UserId,
        /// Created user's external slug.
        user_slug: UserSlug,
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

/// Input parameters for Create User saga.
///
/// PII (name, email) is intentionally excluded — the saga is stored in the
/// global control plane, and only pseudonymous identifiers are permitted.
/// Plaintext PII is written directly to the regional store, not through the
/// saga's serialized state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUserInput {
    /// Hex-encoded HMAC-SHA256 of the normalized email (for global uniqueness).
    pub hmac: String,
    /// Data residency region for the user's PII.
    pub region: Region,
    /// Whether this user is a global service administrator.
    pub admin: bool,
    /// Organization display name for the user's default organization.
    #[serde(default)]
    pub organization_name: String,
    /// Billing tier for the default organization created with this user.
    #[serde(default)]
    pub default_org_tier: OrganizationTier,
}

/// Record for the Create User saga, tracking state and retry progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUserSaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: CreateUserSagaState,
    /// Input parameters.
    pub input: CreateUserInput,
    /// When the saga was created.
    pub created_at: DateTime<Utc>,
    /// When the saga was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of retry attempts.
    pub retries: u8,
    /// Next retry time (for exponential backoff).
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl CreateUserSaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: CreateUserInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: CreateUserSagaState::Pending,
            input,
            created_at: now,
            updated_at: now,
            retries: 0,
            next_retry_at: None,
        }
    }

    /// Checks if the saga is complete (success or permanently failed).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            CreateUserSagaState::Completed { .. }
                | CreateUserSagaState::Failed { .. }
                | CreateUserSagaState::Compensated { .. }
                | CreateUserSagaState::TimedOut
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

    /// Transitions to a new state.
    pub fn transition(&mut self, new_state: CreateUserSagaState) {
        self.state = new_state;
        self.updated_at = Utc::now();
        self.next_retry_at = None;
    }

    /// Marks as failed with error.
    ///
    /// After MAX_RETRIES failures, transitions to terminal Failed state.
    pub fn fail(&mut self, step: u8, error: String) {
        self.retries = self.retries.saturating_add(1);
        if self.retries >= MAX_RETRIES {
            self.state = CreateUserSagaState::Failed { step, error };
        } else {
            let backoff = self.next_backoff();
            self.next_retry_at =
                Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        }
        self.updated_at = Utc::now();
    }

    /// Returns the step number for the current state.
    pub fn current_step(&self) -> u8 {
        match &self.state {
            CreateUserSagaState::Pending => 0,
            CreateUserSagaState::EmailReserved { .. } => 1,
            CreateUserSagaState::RegionalDataWritten { .. } => 2,
            CreateUserSagaState::Completed { .. }
            | CreateUserSagaState::Failed { .. }
            | CreateUserSagaState::Compensated { .. }
            | CreateUserSagaState::TimedOut => 0,
        }
    }

    /// Returns the target region for the current step.
    ///
    /// Steps 0, 2 target GLOBAL; step 1 targets the user's declared region.
    pub fn target_region(&self) -> Region {
        match &self.state {
            CreateUserSagaState::Pending => Region::GLOBAL,
            CreateUserSagaState::EmailReserved { .. } => self.input.region,
            CreateUserSagaState::RegionalDataWritten { .. } => Region::GLOBAL,
            CreateUserSagaState::Completed { .. }
            | CreateUserSagaState::Failed { .. }
            | CreateUserSagaState::Compensated { .. }
            | CreateUserSagaState::TimedOut => Region::GLOBAL,
        }
    }
}
