use chrono::{DateTime, Utc};
use inferadb_ledger_types::UserId;
use serde::{Deserialize, Serialize};

use super::{MAX_BACKOFF, MAX_RETRIES, OrganizationId, SagaId};

/// State machine for the Delete User saga.
///
/// Removes a user and all their memberships across organizations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeleteUserSagaState {
    /// Initial state: about to mark user as deleting.
    Pending,
    /// User marked as DELETING, removing memberships.
    MarkingDeleted {
        /// The user being deleted.
        user_id: UserId,
        /// Organizations with memberships to remove.
        remaining_organizations: Vec<OrganizationId>,
    },
    /// All memberships removed, ready to delete user record.
    MembershipsRemoved {
        /// The user being deleted.
        user_id: UserId,
    },
    /// Saga completed successfully.
    Completed {
        /// The deleted user's ID.
        user_id: UserId,
    },
    /// Saga failed.
    Failed {
        /// The step that failed.
        step: u8,
        /// Error description.
        error: String,
    },
}

/// Input parameters for Delete User saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteUserInput {
    /// User to delete.
    pub user: UserId,
    /// Lists of organization IDs where user has memberships.
    pub organization_ids: Vec<OrganizationId>,
}

/// Record for the Delete User saga, tracking state and retry progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteUserSaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: DeleteUserSagaState,
    /// Input parameters.
    pub input: DeleteUserInput,
    /// When the saga was created.
    pub created_at: DateTime<Utc>,
    /// When the saga was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of retry attempts.
    pub retries: u8,
    /// Next retry time.
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl DeleteUserSaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: DeleteUserInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: DeleteUserSagaState::Pending,
            input,
            created_at: now,
            updated_at: now,
            retries: 0,
            next_retry_at: None,
        }
    }

    /// Checks if the saga is complete.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            DeleteUserSagaState::Completed { .. } | DeleteUserSagaState::Failed { .. }
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

    /// Calculates the next backoff duration using exponential backoff.
    pub fn next_backoff(&self) -> Duration {
        let base = Duration::from_secs(1);
        let backoff = base * 2u32.saturating_pow(self.retries as u32);
        std::cmp::min(backoff, MAX_BACKOFF)
    }

    /// Schedules the next retry with exponential backoff.
    pub fn schedule_retry(&mut self) {
        self.retries = self.retries.saturating_add(1);
        let backoff = self.next_backoff();
        self.next_retry_at =
            Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        self.updated_at = Utc::now();
    }

    /// Transitions to a new state.
    pub fn transition(&mut self, new_state: DeleteUserSagaState) {
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
            self.state = DeleteUserSagaState::Failed { step, error };
        } else {
            // Schedule next retry with exponential backoff
            let backoff = self.next_backoff();
            self.next_retry_at =
                Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        }
        self.updated_at = Utc::now();
    }

    /// Returns the step number for the current state (used in fail tracking).
    pub fn current_step(&self) -> u8 {
        match &self.state {
            DeleteUserSagaState::Pending => 0,
            DeleteUserSagaState::MarkingDeleted { .. } => 1,
            DeleteUserSagaState::MembershipsRemoved { .. } => 2,
            DeleteUserSagaState::Completed { .. } | DeleteUserSagaState::Failed { .. } => 0,
        }
    }
}

use std::time::Duration;
