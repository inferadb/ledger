//! Cross-organization saga support.
//!
//! Sagas coordinate operations spanning multiple organizations using eventual
//! consistency. Each saga step is idempotent for crash recovery.
//!
//! ## Saga Patterns
//!
//! - **CreateOrg**: Creates user in `_system`, then organization with membership
//! - **DeleteUser**: Marks user as deleting, removes memberships, then deletes user
//!
//! ## Storage
//!
//! Sagas are stored in `_system` organization under `saga:{saga_id}` keys.
//! The leader polls for incomplete sagas every 30 seconds.

use std::time::Duration;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{OrganizationId, OrganizationSlug, Region, UserId};
use serde::{Deserialize, Serialize};

/// Unique identifier for a saga.
pub type SagaId = String;

/// Maximum number of retry attempts before marking a saga as failed.
pub const MAX_RETRIES: u8 = 10;

/// Interval between saga poll cycles (30 seconds).
pub const SAGA_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum backoff duration for retries (5 minutes).
pub const MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);

// =============================================================================
// Create Organization Saga
// =============================================================================

/// State machine for the Create Organization saga.
///
/// Creates a new user (if needed) and a new organization.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CreateOrgSagaState {
    /// Initial state: about to create user.
    Pending,
    /// User created in `_system`, waiting for organization creation.
    UserCreated {
        /// The created user's ID.
        user_id: UserId,
    },
    /// Organization created, waiting for finalization.
    OrganizationCreated {
        /// The user's ID.
        user_id: UserId,
        /// The created organization's ID.
        organization_id: OrganizationId,
    },
    /// Saga completed successfully.
    Completed {
        /// The user's ID.
        user_id: UserId,
        /// The created organization's ID.
        organization_id: OrganizationId,
    },
    /// Saga failed and compensation was attempted.
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
        /// What was cleaned up.
        cleanup_summary: String,
    },
}

/// Input parameters for Create Organization saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrgInput {
    /// User's display name.
    pub user_name: String,
    /// User's email address.
    pub user_email: String,
    /// Organization name.
    pub org_name: String,
    /// Optional existing user ID (if user already exists).
    pub existing_user_id: Option<UserId>,
}

/// Record for the Create Organization saga, tracking state and retry progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrgSaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: CreateOrgSagaState,
    /// Input parameters.
    pub input: CreateOrgInput,
    /// When the saga was created.
    pub created_at: DateTime<Utc>,
    /// When the saga was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of retry attempts.
    pub retries: u8,
    /// Next retry time (for exponential backoff).
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl CreateOrgSaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: CreateOrgInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: CreateOrgSagaState::Pending,
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
            CreateOrgSagaState::Completed { .. }
                | CreateOrgSagaState::Failed { .. }
                | CreateOrgSagaState::Compensated { .. }
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

    /// Calculates next backoff duration using exponential backoff.
    pub fn next_backoff(&self) -> Duration {
        let base = Duration::from_secs(1);
        let backoff = base * 2u32.saturating_pow(self.retries as u32);
        std::cmp::min(backoff, MAX_BACKOFF)
    }

    /// Increments the retry count and sets the next retry time.
    pub fn schedule_retry(&mut self) {
        self.retries = self.retries.saturating_add(1);
        let backoff = self.next_backoff();
        self.next_retry_at =
            Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        self.updated_at = Utc::now();
    }

    /// Transitions to a new state.
    pub fn transition(&mut self, new_state: CreateOrgSagaState) {
        self.state = new_state;
        self.updated_at = Utc::now();
        self.next_retry_at = None; // Clear retry on successful transition
    }

    /// Marks as failed with error.
    ///
    /// After MAX_RETRIES failures, transitions to terminal Failed state.
    pub fn fail(&mut self, step: u8, error: String) {
        self.retries = self.retries.saturating_add(1);
        if self.retries >= MAX_RETRIES {
            self.state = CreateOrgSagaState::Failed { step, error };
        } else {
            // Schedule next retry with exponential backoff
            let backoff = self.next_backoff();
            self.next_retry_at =
                Some(Utc::now() + chrono::Duration::from_std(backoff).unwrap_or_default());
        }
        self.updated_at = Utc::now();
    }
}

// =============================================================================
// Delete User Saga
// =============================================================================

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
    pub user_id: UserId,
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
}

// =============================================================================
// Migrate Organization Saga
// =============================================================================

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

// =============================================================================
// Migrate User Region Saga
// =============================================================================

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
    /// Internal user ID.
    pub user_id: UserId,
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
        }
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

// =============================================================================
// Generic Saga Wrapper
// =============================================================================

/// Type of saga.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SagaType {
    /// Create Organization saga type.
    CreateOrg,
    /// Delete User saga type.
    DeleteUser,
    /// Migrate Organization region saga type.
    MigrateOrg,
    /// Migrate User Region saga type.
    MigrateUser,
}

/// Generic saga record that wraps specific saga types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Saga {
    /// Create Organization saga.
    CreateOrg(CreateOrgSaga),
    /// Delete User saga.
    DeleteUser(DeleteUserSaga),
    /// Migrate Organization region saga.
    MigrateOrg(MigrateOrgSaga),
    /// Migrate User Region saga.
    MigrateUser(MigrateUserSaga),
}

impl Saga {
    /// Returns the saga ID.
    pub fn id(&self) -> &str {
        match self {
            Saga::CreateOrg(s) => &s.id,
            Saga::DeleteUser(s) => &s.id,
            Saga::MigrateOrg(s) => &s.id,
            Saga::MigrateUser(s) => &s.id,
        }
    }

    /// Returns the saga type.
    pub fn saga_type(&self) -> SagaType {
        match self {
            Saga::CreateOrg(_) => SagaType::CreateOrg,
            Saga::DeleteUser(_) => SagaType::DeleteUser,
            Saga::MigrateOrg(_) => SagaType::MigrateOrg,
            Saga::MigrateUser(_) => SagaType::MigrateUser,
        }
    }

    /// Checks if the saga is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        match self {
            Saga::CreateOrg(s) => s.is_terminal(),
            Saga::DeleteUser(s) => s.is_terminal(),
            Saga::MigrateOrg(s) => s.is_terminal(),
            Saga::MigrateUser(s) => s.is_terminal(),
        }
    }

    /// Checks if the saga is ready for retry.
    pub fn is_ready_for_retry(&self) -> bool {
        match self {
            Saga::CreateOrg(s) => s.is_ready_for_retry(),
            Saga::DeleteUser(s) => s.is_ready_for_retry(),
            Saga::MigrateOrg(s) => s.is_ready_for_retry(),
            Saga::MigrateUser(s) => s.is_ready_for_retry(),
        }
    }

    /// Returns the creation timestamp.
    pub fn created_at(&self) -> DateTime<Utc> {
        match self {
            Saga::CreateOrg(s) => s.created_at,
            Saga::DeleteUser(s) => s.created_at,
            Saga::MigrateOrg(s) => s.created_at,
            Saga::MigrateUser(s) => s.created_at,
        }
    }

    /// Returns the last-updated timestamp.
    pub fn updated_at(&self) -> DateTime<Utc> {
        match self {
            Saga::CreateOrg(s) => s.updated_at,
            Saga::DeleteUser(s) => s.updated_at,
            Saga::MigrateOrg(s) => s.updated_at,
            Saga::MigrateUser(s) => s.updated_at,
        }
    }

    /// Returns the retry count.
    pub fn retries(&self) -> u8 {
        match self {
            Saga::CreateOrg(s) => s.retries,
            Saga::DeleteUser(s) => s.retries,
            Saga::MigrateOrg(s) => s.retries,
            Saga::MigrateUser(s) => s.retries,
        }
    }

    /// Serializes to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserializes from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_create_org_saga_new() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let saga = CreateOrgSaga::new("saga-123".to_string(), input);

        assert_eq!(saga.id, "saga-123");
        assert_eq!(saga.state, CreateOrgSagaState::Pending);
        assert_eq!(saga.retries, 0);
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
    }

    #[test]
    fn test_create_org_saga_transitions() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let mut saga = CreateOrgSaga::new("saga-123".to_string(), input);

        // Transition to UserCreated
        saga.transition(CreateOrgSagaState::UserCreated { user_id: UserId::new(1) });
        assert!(matches!(
            saga.state,
            CreateOrgSagaState::UserCreated { user_id } if user_id == UserId::new(1)
        ));
        assert!(!saga.is_terminal());

        // Transition to OrganizationCreated
        saga.transition(CreateOrgSagaState::OrganizationCreated {
            user_id: UserId::new(1),
            organization_id: OrganizationId::new(100),
        });
        assert!(!saga.is_terminal());

        // Transition to Completed
        saga.transition(CreateOrgSagaState::Completed {
            user_id: UserId::new(1),
            organization_id: OrganizationId::new(100),
        });
        assert!(saga.is_terminal());
    }

    #[test]
    fn test_exponential_backoff() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let mut saga = CreateOrgSaga::new("saga-123".to_string(), input);

        // First backoff: 1s
        assert_eq!(saga.next_backoff(), Duration::from_secs(1));

        saga.schedule_retry();
        // Second backoff: 2s
        assert_eq!(saga.next_backoff(), Duration::from_secs(2));

        saga.schedule_retry();
        // Third backoff: 4s
        assert_eq!(saga.next_backoff(), Duration::from_secs(4));
    }

    #[test]
    fn test_max_backoff() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let mut saga = CreateOrgSaga::new("saga-123".to_string(), input);

        // Simulate many retries
        for _ in 0..20 {
            saga.schedule_retry();
        }

        // Should cap at MAX_BACKOFF (5 minutes)
        assert_eq!(saga.next_backoff(), MAX_BACKOFF);
    }

    #[test]
    fn test_fail_after_max_retries() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let mut saga = CreateOrgSaga::new("saga-123".to_string(), input);

        // Fail MAX_RETRIES times
        for _ in 0..MAX_RETRIES {
            saga.fail(1, "test error".to_string());
        }

        // Should now be in Failed state
        assert!(saga.is_terminal());
        assert!(matches!(saga.state, CreateOrgSagaState::Failed { step: 1, .. }));
    }

    #[test]
    fn test_delete_user_saga() {
        let input = DeleteUserInput {
            user_id: UserId::new(1),
            organization_ids: vec![OrganizationId::new(100), OrganizationId::new(101)],
        };
        let mut saga = DeleteUserSaga::new("delete-123".to_string(), input);

        assert!(!saga.is_terminal());

        saga.transition(DeleteUserSagaState::MarkingDeleted {
            user_id: UserId::new(1),
            remaining_organizations: vec![OrganizationId::new(100), OrganizationId::new(101)],
        });
        assert!(!saga.is_terminal());

        saga.transition(DeleteUserSagaState::MembershipsRemoved { user_id: UserId::new(1) });
        assert!(!saga.is_terminal());

        saga.transition(DeleteUserSagaState::Completed { user_id: UserId::new(1) });
        assert!(saga.is_terminal());
    }

    #[test]
    fn test_saga_serialization() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let saga = Saga::CreateOrg(CreateOrgSaga::new("saga-123".to_string(), input));

        let bytes = saga.to_bytes().unwrap();
        let restored = Saga::from_bytes(&bytes).unwrap();

        assert_eq!(saga.id(), restored.id());
        assert_eq!(saga.saga_type(), restored.saga_type());
    }

    #[test]
    fn test_saga_wrapper() {
        let input = CreateOrgInput {
            user_name: "Alice".to_string(),
            user_email: "alice@example.com".to_string(),
            org_name: "Acme Corp".to_string(),
            existing_user_id: None,
        };
        let saga = Saga::CreateOrg(CreateOrgSaga::new("saga-123".to_string(), input));

        assert_eq!(saga.id(), "saga-123");
        assert_eq!(saga.saga_type(), SagaType::CreateOrg);
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
        assert_eq!(saga.retries(), 0);
    }

    fn make_migrate_org_input() -> MigrateOrgInput {
        MigrateOrgInput {
            organization_id: OrganizationId::new(42),
            organization_slug: OrganizationSlug::new(9_001_000_000_000_000_000),
            source_region: Region::US_EAST_VA,
            target_region: Region::IE_EAST_DUBLIN,
            acknowledge_residency_downgrade: false,
            metadata_only: false,
        }
    }

    #[test]
    fn test_migrate_org_saga_new() {
        let saga = MigrateOrgSaga::new("migrate-1".to_string(), make_migrate_org_input());

        assert_eq!(saga.id, "migrate-1");
        assert_eq!(saga.state, MigrateOrgSagaState::Pending);
        assert_eq!(saga.retries, 0);
        assert!(saga.next_retry_at.is_none());
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
    }

    #[test]
    fn test_migrate_org_saga_transitions() {
        let mut saga = MigrateOrgSaga::new("migrate-2".to_string(), make_migrate_org_input());

        // Pending → MigrationStarted
        saga.transition(MigrateOrgSagaState::MigrationStarted);
        assert_eq!(saga.state, MigrateOrgSagaState::MigrationStarted);
        assert!(!saga.is_terminal());

        // MigrationStarted → DataSnapshotTaken
        saga.transition(MigrateOrgSagaState::DataSnapshotTaken {
            source_state_root: vec![0xde, 0xad, 0xbe, 0xef],
        });
        assert!(matches!(
            saga.state,
            MigrateOrgSagaState::DataSnapshotTaken { ref source_state_root }
                if source_state_root == &[0xde, 0xad, 0xbe, 0xef]
        ));
        assert!(!saga.is_terminal());

        // DataSnapshotTaken → DataWritten
        saga.transition(MigrateOrgSagaState::DataWritten);
        assert_eq!(saga.state, MigrateOrgSagaState::DataWritten);
        assert!(!saga.is_terminal());

        // DataWritten → IntegrityVerified
        saga.transition(MigrateOrgSagaState::IntegrityVerified);
        assert_eq!(saga.state, MigrateOrgSagaState::IntegrityVerified);
        assert!(!saga.is_terminal());

        // IntegrityVerified → RoutingUpdated
        saga.transition(MigrateOrgSagaState::RoutingUpdated);
        assert_eq!(saga.state, MigrateOrgSagaState::RoutingUpdated);
        assert!(!saga.is_terminal());

        // RoutingUpdated → SourceDeleted
        saga.transition(MigrateOrgSagaState::SourceDeleted);
        assert_eq!(saga.state, MigrateOrgSagaState::SourceDeleted);
        assert!(!saga.is_terminal());

        // SourceDeleted → Completed
        saga.transition(MigrateOrgSagaState::Completed);
        assert_eq!(saga.state, MigrateOrgSagaState::Completed);
        assert!(saga.is_terminal());
        assert!(!saga.is_ready_for_retry());
    }

    #[test]
    fn test_migrate_org_saga_is_terminal() {
        let base = MigrateOrgSaga::new("migrate-3".to_string(), make_migrate_org_input());

        let completed = {
            let mut s = base.clone();
            s.state = MigrateOrgSagaState::Completed;
            s
        };
        assert!(completed.is_terminal());

        let failed = {
            let mut s = base.clone();
            s.state = MigrateOrgSagaState::Failed { step: 2, error: "disk full".to_string() };
            s
        };
        assert!(failed.is_terminal());

        let rolled_back = {
            let mut s = base.clone();
            s.state = MigrateOrgSagaState::RolledBack { reason: "operator abort".to_string() };
            s
        };
        assert!(rolled_back.is_terminal());

        let timed_out = {
            let mut s = base.clone();
            s.state = MigrateOrgSagaState::TimedOut;
            s
        };
        assert!(timed_out.is_terminal());

        // Non-terminal states
        let mut in_progress = base.clone();
        in_progress.state = MigrateOrgSagaState::MigrationStarted;
        assert!(!in_progress.is_terminal());
    }

    #[test]
    fn test_migrate_org_saga_fail() {
        let mut saga = MigrateOrgSaga::new("migrate-4".to_string(), make_migrate_org_input());

        // First MAX_RETRIES - 1 failures schedule retries, not terminal.
        for _ in 0..(MAX_RETRIES - 1) {
            saga.fail(1, "transient error".to_string());
            assert!(!saga.is_terminal(), "should not be terminal before exhausting retries");
            assert!(saga.next_retry_at.is_some(), "should schedule a retry");
        }

        // Final failure tips over into terminal Failed state.
        saga.fail(1, "permanent error".to_string());
        assert!(saga.is_terminal());
        assert!(matches!(
            saga.state,
            MigrateOrgSagaState::Failed { step: 1, ref error } if error == "permanent error"
        ));
        assert_eq!(saga.retries, MAX_RETRIES);
        assert!(!saga.is_ready_for_retry());
    }

    #[test]
    fn test_migrate_org_saga_is_timed_out() {
        let mut saga = MigrateOrgSaga::new("migrate-5".to_string(), make_migrate_org_input());

        // Back-date creation so the saga appears old.
        saga.created_at = Utc::now() - chrono::Duration::hours(2);

        assert!(saga.is_timed_out(Duration::from_secs(3600))); // 1-hour timeout exceeded
        assert!(!saga.is_timed_out(Duration::from_secs(7200 + 60))); // 2-hour+ timeout not yet exceeded
    }

    #[test]
    fn test_migrate_org_saga_current_step() {
        let mut saga = MigrateOrgSaga::new("migrate-6".to_string(), make_migrate_org_input());

        assert_eq!(saga.current_step(), 0); // Pending

        saga.state = MigrateOrgSagaState::MigrationStarted;
        assert_eq!(saga.current_step(), 1);

        saga.state = MigrateOrgSagaState::DataSnapshotTaken { source_state_root: vec![0x01] };
        assert_eq!(saga.current_step(), 2);

        saga.state = MigrateOrgSagaState::DataWritten;
        assert_eq!(saga.current_step(), 3);

        saga.state = MigrateOrgSagaState::IntegrityVerified;
        assert_eq!(saga.current_step(), 4);

        saga.state = MigrateOrgSagaState::RoutingUpdated;
        assert_eq!(saga.current_step(), 5);

        saga.state = MigrateOrgSagaState::SourceDeleted;
        assert_eq!(saga.current_step(), 6);

        // Terminal states return 0
        saga.state = MigrateOrgSagaState::Completed;
        assert_eq!(saga.current_step(), 0);

        saga.state = MigrateOrgSagaState::Failed { step: 3, error: "oops".to_string() };
        assert_eq!(saga.current_step(), 0);

        saga.state = MigrateOrgSagaState::RolledBack { reason: "timeout".to_string() };
        assert_eq!(saga.current_step(), 0);

        saga.state = MigrateOrgSagaState::TimedOut;
        assert_eq!(saga.current_step(), 0);
    }

    #[test]
    fn test_migrate_org_saga_serialization() {
        let inner = MigrateOrgSaga::new("migrate-7".to_string(), make_migrate_org_input());
        let saga = Saga::MigrateOrg(inner);

        let bytes = saga.to_bytes().unwrap();
        let restored = Saga::from_bytes(&bytes).unwrap();

        assert_eq!(saga.id(), restored.id());
        assert_eq!(saga.saga_type(), SagaType::MigrateOrg);
        assert_eq!(restored.saga_type(), SagaType::MigrateOrg);
        assert_eq!(saga.retries(), restored.retries());
        assert_eq!(saga.created_at(), restored.created_at());

        // Verify inner fields survive the round-trip.
        let Saga::MigrateOrg(ref original) = saga else { panic!("expected MigrateOrg") };
        let Saga::MigrateOrg(ref deserialized) = restored else { panic!("expected MigrateOrg") };
        assert_eq!(original.input.organization_id, deserialized.input.organization_id);
        assert_eq!(original.input.organization_slug, deserialized.input.organization_slug);
        assert_eq!(original.input.source_region, deserialized.input.source_region);
        assert_eq!(original.input.target_region, deserialized.input.target_region);
        assert_eq!(
            original.input.acknowledge_residency_downgrade,
            deserialized.input.acknowledge_residency_downgrade
        );
        assert_eq!(original.input.metadata_only, deserialized.input.metadata_only);
        assert_eq!(original.state, deserialized.state);
    }

    // =========================================================================
    // MigrateUserSaga tests
    // =========================================================================

    fn make_migrate_user_input() -> MigrateUserInput {
        MigrateUserInput {
            user_id: UserId::new(7),
            source_region: Region::US_EAST_VA,
            target_region: Region::IE_EAST_DUBLIN,
        }
    }

    #[test]
    fn test_migrate_user_saga_new() {
        let saga = MigrateUserSaga::new("user-migrate-1".to_string(), make_migrate_user_input());

        assert_eq!(saga.id, "user-migrate-1");
        assert_eq!(saga.state, MigrateUserSagaState::Pending);
        assert_eq!(saga.retries, 0);
        assert!(saga.next_retry_at.is_none());
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
    }

    #[test]
    fn test_migrate_user_saga_transitions() {
        let mut saga =
            MigrateUserSaga::new("user-migrate-2".to_string(), make_migrate_user_input());

        // Step 1: Mark directory as migrating
        saga.transition(MigrateUserSagaState::DirectoryMarkedMigrating);
        assert_eq!(saga.state, MigrateUserSagaState::DirectoryMarkedMigrating);
        assert!(!saga.is_terminal());

        // Step 2: User data read from source region
        saga.transition(MigrateUserSagaState::UserDataRead);
        assert_eq!(saga.state, MigrateUserSagaState::UserDataRead);
        assert!(!saga.is_terminal());

        // Step 3: User data written to target region
        saga.transition(MigrateUserSagaState::UserDataWritten);
        assert_eq!(saga.state, MigrateUserSagaState::UserDataWritten);
        assert!(!saga.is_terminal());

        // Step 4: Directory updated to Active with new region
        saga.transition(MigrateUserSagaState::DirectoryUpdated);
        assert_eq!(saga.state, MigrateUserSagaState::DirectoryUpdated);
        assert!(!saga.is_terminal());

        // Step 5: Source data deleted
        saga.transition(MigrateUserSagaState::SourceDeleted);
        assert_eq!(saga.state, MigrateUserSagaState::SourceDeleted);
        assert!(!saga.is_terminal());

        // Step 6: Completed
        saga.transition(MigrateUserSagaState::Completed);
        assert_eq!(saga.state, MigrateUserSagaState::Completed);
        assert!(saga.is_terminal());
    }

    #[test]
    fn test_migrate_user_saga_is_terminal() {
        let base = MigrateUserSaga::new("user-migrate-3".to_string(), make_migrate_user_input());

        // Completed is terminal
        let mut s = base.clone();
        s.state = MigrateUserSagaState::Completed;
        assert!(s.is_terminal());

        // Failed is terminal
        let mut s = base.clone();
        s.state = MigrateUserSagaState::Failed { step: 2, error: "disk full".to_string() };
        assert!(s.is_terminal());

        // Compensated is terminal
        let mut s = base.clone();
        s.state = MigrateUserSagaState::Compensated { reason: "operator abort".to_string() };
        assert!(s.is_terminal());

        // TimedOut is terminal
        let mut s = base.clone();
        s.state = MigrateUserSagaState::TimedOut;
        assert!(s.is_terminal());

        // In-progress states are NOT terminal
        let mut in_progress = base;
        in_progress.state = MigrateUserSagaState::DirectoryMarkedMigrating;
        assert!(!in_progress.is_terminal());
    }

    #[test]
    fn test_migrate_user_saga_fail() {
        let mut saga =
            MigrateUserSaga::new("user-migrate-4".to_string(), make_migrate_user_input());

        // Fail multiple times with retries
        for i in 0..MAX_RETRIES.saturating_sub(1) {
            saga.fail(1, "transient error".to_string());
            assert!(!saga.is_terminal(), "should not be terminal after {} retries", i + 1);
            assert!(saga.next_retry_at.is_some());
        }

        // Final fail transitions to terminal Failed
        saga.fail(1, "permanent error".to_string());
        assert!(saga.is_terminal());
        assert!(matches!(
            saga.state,
            MigrateUserSagaState::Failed { step: 1, ref error } if error == "permanent error"
        ));
    }

    #[test]
    fn test_migrate_user_saga_is_timed_out() {
        let mut saga =
            MigrateUserSaga::new("user-migrate-5".to_string(), make_migrate_user_input());
        saga.created_at = Utc::now() - chrono::Duration::minutes(10);

        assert!(saga.is_timed_out(Duration::from_secs(300))); // 5 min timeout
        assert!(!saga.is_timed_out(Duration::from_secs(3600))); // 1 hour timeout
    }

    #[test]
    fn test_migrate_user_saga_current_step() {
        let mut saga =
            MigrateUserSaga::new("user-migrate-6".to_string(), make_migrate_user_input());

        assert_eq!(saga.current_step(), 0); // Pending

        saga.state = MigrateUserSagaState::DirectoryMarkedMigrating;
        assert_eq!(saga.current_step(), 1);

        saga.state = MigrateUserSagaState::UserDataRead;
        assert_eq!(saga.current_step(), 2);

        saga.state = MigrateUserSagaState::UserDataWritten;
        assert_eq!(saga.current_step(), 3);

        saga.state = MigrateUserSagaState::DirectoryUpdated;
        assert_eq!(saga.current_step(), 4);

        saga.state = MigrateUserSagaState::SourceDeleted;
        assert_eq!(saga.current_step(), 5);

        // Terminal states return 0
        saga.state = MigrateUserSagaState::Completed;
        assert_eq!(saga.current_step(), 0);

        saga.state = MigrateUserSagaState::Failed { step: 3, error: "oops".to_string() };
        assert_eq!(saga.current_step(), 0);

        saga.state = MigrateUserSagaState::Compensated { reason: "timeout".to_string() };
        assert_eq!(saga.current_step(), 0);

        saga.state = MigrateUserSagaState::TimedOut;
        assert_eq!(saga.current_step(), 0);
    }

    #[test]
    fn test_migrate_user_saga_serialization() {
        let inner = MigrateUserSaga::new("user-migrate-7".to_string(), make_migrate_user_input());
        let saga = Saga::MigrateUser(inner);

        let bytes = saga.to_bytes().unwrap();
        let restored = Saga::from_bytes(&bytes).unwrap();

        assert_eq!(saga.id(), restored.id());
        assert_eq!(saga.saga_type(), SagaType::MigrateUser);
        assert_eq!(restored.saga_type(), SagaType::MigrateUser);
        assert_eq!(saga.retries(), restored.retries());
        assert_eq!(saga.created_at(), restored.created_at());

        // Verify inner fields survive the round-trip.
        let Saga::MigrateUser(ref original) = saga else { panic!("expected MigrateUser") };
        let Saga::MigrateUser(ref deserialized) = restored else { panic!("expected MigrateUser") };
        assert_eq!(original.input.user_id, deserialized.input.user_id);
        assert_eq!(original.input.source_region, deserialized.input.source_region);
        assert_eq!(original.input.target_region, deserialized.input.target_region);
        assert_eq!(original.state, deserialized.state);
    }

    #[test]
    fn test_migrate_user_saga_wrapper_integration() {
        let inner = MigrateUserSaga::new("user-migrate-8".to_string(), make_migrate_user_input());
        let saga = Saga::MigrateUser(inner);

        assert_eq!(saga.id(), "user-migrate-8");
        assert_eq!(saga.saga_type(), SagaType::MigrateUser);
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
        assert_eq!(saga.retries(), 0);
    }
}
