//! User region migration saga state machine and step definitions.

use std::time::Duration;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{Region, UserId};
use serde::{Deserialize, Serialize};

use super::{MAX_BACKOFF, MAX_RETRIES, SagaId};

/// State machine for the Migrate User Region saga.
///
/// Moves user PII between regional Raft groups and updates the GLOBAL directory.
/// Steps: mark migrating → read source → write target → update directory → delete source.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MigrateUserSagaState {
    /// Initial state: about to mark user directory as Migrating.
    Pending,
    /// Directory entry updated to Migrating in GLOBAL control plane.
    DirectoryMarkedMigrating,
    /// User data read from source regional store.
    UserDataRead,
    /// User data written to target regional store.
    UserDataWritten,
    /// Directory entry updated: region = target, status = Active.
    DirectoryUpdated,
    /// Source regional data deleted.
    SourceDeleted,
    /// Migration completed successfully.
    Completed,
    /// Saga failed after exhausting retries.
    Failed {
        /// The step that failed (0-indexed).
        step: u8,
        /// Error description.
        error: String,
    },
    /// Migration rolled back due to failure. User exclusively in source region.
    Compensated {
        /// Summary of compensation actions taken.
        reason: String,
    },
    /// Migration exceeded the configured timeout and was auto-rolled back.
    TimedOut,
}

/// Input parameters for the Migrate User Region saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrateUserInput {
    /// Internal user identifier.
    pub user: UserId,
    /// Region the user's PII is migrating from.
    pub source_region: Region,
    /// Region the user's PII is migrating to.
    pub target_region: Region,
}

/// Record for the Migrate User Region saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrateUserSaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: MigrateUserSagaState,
    /// Input parameters.
    pub input: MigrateUserInput,
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

impl MigrateUserSaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: MigrateUserInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: MigrateUserSagaState::Pending,
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

    /// Checks if the saga is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            MigrateUserSagaState::Completed
                | MigrateUserSagaState::Failed { .. }
                | MigrateUserSagaState::Compensated { .. }
                | MigrateUserSagaState::TimedOut
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

    /// Checks if the migration has exceeded the given timeout.
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
    pub fn transition(&mut self, new_state: MigrateUserSagaState) {
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
            self.state = MigrateUserSagaState::Failed { step, error };
        } else {
            let backoff = self.next_backoff();
            self.next_retry_at =
                Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        }
        self.updated_at = Utc::now();
    }

    /// Returns the step number for the current state (used in fail tracking).
    pub fn current_step(&self) -> u8 {
        match &self.state {
            MigrateUserSagaState::Pending => 0,
            MigrateUserSagaState::DirectoryMarkedMigrating => 1,
            MigrateUserSagaState::UserDataRead => 2,
            MigrateUserSagaState::UserDataWritten => 3,
            MigrateUserSagaState::DirectoryUpdated => 4,
            MigrateUserSagaState::SourceDeleted => 5,
            MigrateUserSagaState::Completed
            | MigrateUserSagaState::Failed { .. }
            | MigrateUserSagaState::Compensated { .. }
            | MigrateUserSagaState::TimedOut => 0,
        }
    }
}
