//! Organization region migration saga state machine and step definitions.

use std::time::Duration;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{OrganizationId, OrganizationSlug, Region};
use serde::{Deserialize, Serialize};

use super::{MAX_BACKOFF, MAX_RETRIES, SagaId};

/// State machine for the Migrate Organization saga.
///
/// Coordinates region migration with data transfer and integrity verification.
/// Non-protected to non-protected migrations skip data movement steps.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MigrateOrgSagaState {
    /// Initial state: about to propose StartMigration to Raft.
    Pending,
    /// Organization status set to Migrating, pending_region recorded.
    MigrationStarted,
    /// Data snapshot taken from source region (skipped for metadata-only).
    DataSnapshotTaken {
        /// State root hash of the source data for integrity verification.
        source_state_root: Vec<u8>,
    },
    /// Data written to target region (skipped for metadata-only).
    DataWritten,
    /// State root in target matches source (skipped for metadata-only).
    IntegrityVerified,
    /// Routing updated to point to target region.
    RoutingUpdated,
    /// Source data deleted (skipped for metadata-only).
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
    /// Migration rolled back due to failure or timeout.
    RolledBack {
        /// Reason for rollback.
        reason: String,
    },
    /// Migration exceeded the configured timeout and was auto-rolled back.
    TimedOut,
}

/// Input parameters for Migrate Organization saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrateOrgInput {
    /// Internal organization ID.
    pub organization_id: OrganizationId,
    /// External organization slug.
    pub organization_slug: OrganizationSlug,
    /// Region the organization is migrating from.
    pub source_region: Region,
    /// Region the organization is migrating to.
    pub target_region: Region,
    /// Whether the user acknowledged residency downgrade (protected → non-protected).
    pub acknowledge_residency_downgrade: bool,
    /// Whether this is a metadata-only migration (non-protected → non-protected).
    pub metadata_only: bool,
}

/// Record for the Migrate Organization saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrateOrgSaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: MigrateOrgSagaState,
    /// Input parameters.
    pub input: MigrateOrgInput,
    /// When the saga was created.
    pub created_at: DateTime<Utc>,
    /// When the saga was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of retry attempts.
    pub retries: u8,
    /// Next retry time (for exponential backoff).
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl MigrateOrgSaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: MigrateOrgInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: MigrateOrgSagaState::Pending,
            input,
            created_at: now,
            updated_at: now,
            retries: 0,
            next_retry_at: None,
        }
    }

    /// Checks if the saga is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            MigrateOrgSagaState::Completed
                | MigrateOrgSagaState::Failed { .. }
                | MigrateOrgSagaState::RolledBack { .. }
                | MigrateOrgSagaState::TimedOut
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
    pub fn transition(&mut self, new_state: MigrateOrgSagaState) {
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
            self.state = MigrateOrgSagaState::Failed { step, error };
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
            MigrateOrgSagaState::Pending => 0,
            MigrateOrgSagaState::MigrationStarted => 1,
            MigrateOrgSagaState::DataSnapshotTaken { .. } => 2,
            MigrateOrgSagaState::DataWritten => 3,
            MigrateOrgSagaState::IntegrityVerified => 4,
            MigrateOrgSagaState::RoutingUpdated => 5,
            MigrateOrgSagaState::SourceDeleted => 6,
            MigrateOrgSagaState::Completed
            | MigrateOrgSagaState::Failed { .. }
            | MigrateOrgSagaState::RolledBack { .. }
            | MigrateOrgSagaState::TimedOut => 0,
        }
    }
}
