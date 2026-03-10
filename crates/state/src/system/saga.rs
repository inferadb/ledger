//! Cross-organization saga support.
//!
//! Sagas coordinate operations spanning multiple organizations using eventual
//! consistency. Each saga step is idempotent for crash recovery.
//!
//! ## Saga Patterns
//!
//! - **CreateOrganization**: Multi-group saga creating org directory entry + profile
//! - **CreateSigningKey**: Single-group saga generating + encrypting + proposing a signing key
//! - **CreateUser**: Multi-group saga allocating IDs, reserving email, writing regional data
//! - **DeleteUser**: Marks user as deleting, removes memberships, then deletes user
//! - **MigrateOrg**: Region migration with data transfer and integrity verification
//! - **MigrateUser**: Moves user PII between regional Raft groups
//!
//! ## Storage
//!
//! Sagas are stored in `_system` organization under `saga:{saga_id}` keys.
//! The leader polls for incomplete sagas every 30 seconds.

use std::time::Duration;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{OrganizationId, OrganizationSlug, Region, UserId, UserSlug};
use serde::{Deserialize, Serialize};

use super::types::{OrganizationTier, SigningKeyScope};

/// Unique identifier for a saga.
///
/// Wraps a `String` with compile-time type safety to prevent mixing
/// with other string identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SagaId(String);

impl SagaId {
    /// Creates a new saga identifier.
    #[inline]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the string value as a slice.
    #[inline]
    pub fn value(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SagaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for SagaId {
    #[inline]
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl PartialEq<str> for SagaId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<String> for SagaId {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<&str> for SagaId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

/// Maximum number of retry attempts before marking a saga as failed.
pub const MAX_RETRIES: u8 = 10;

/// Interval between saga poll cycles (30 seconds).
pub const SAGA_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum backoff duration for retries (5 minutes).
pub const MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);

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
// Multi-Group Saga Infrastructure
// =============================================================================

/// Status of an individual saga step within a multi-group saga.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    /// Step has not yet been executed.
    Pending,
    /// Step completed successfully (Raft commit confirmed).
    Completed,
    /// Step execution failed.
    Failed,
    /// Step was compensated (rollback action executed).
    Compensated,
}

/// A single step in a multi-group saga, targeting a specific region's Raft group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaStep {
    /// Sequential step identifier within the saga (0-indexed).
    pub step_id: u32,
    /// The region whose Raft group this step targets.
    pub target_region: Region,
    /// Describes what action this step performs.
    pub action: String,
    /// Describes the compensation action if rollback is needed.
    pub compensate: String,
    /// Current execution status.
    pub status: StepStatus,
}

/// Identifies the entity locked by an active saga to prevent concurrent sagas.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SagaLockKey {
    /// Lock on a user (prevents concurrent user creation/migration/erasure).
    User(UserId),
    /// Lock on a user email (prevents concurrent creation with the same email).
    Email(String),
    /// Lock on an organization (prevents concurrent migration).
    Organization(OrganizationId),
    /// Lock on a signing key scope (prevents concurrent key creation for same scope).
    SigningKeyScope(SigningKeyScope),
}

impl std::fmt::Display for SagaLockKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SagaLockKey::User(id) => write!(f, "user:{}", id.value()),
            SagaLockKey::Email(email) => write!(f, "email:{email}"),
            SagaLockKey::Organization(id) => write!(f, "org:{}", id.value()),
            SagaLockKey::SigningKeyScope(scope) => match scope {
                SigningKeyScope::Global => write!(f, "signing_key:global"),
                SigningKeyScope::Organization(id) => {
                    write!(f, "signing_key:org:{}", id.value())
                },
            },
        }
    }
}

// =============================================================================
// Create User Saga
// =============================================================================

/// State machine for the Create User saga.
///
/// Coordinates writes across GLOBAL + regional Raft groups:
/// 1. GLOBAL: allocate UserId/UserSlug, CAS email HMAC (reserves uniqueness)
/// 2. Regional: create User, UserEmail, SubjectKey
/// 3. GLOBAL: create UserDirectoryEntry + slug index
///
/// Compensation in reverse: step 3 → delete directory/slug index,
/// step 2 → delete User/UserEmail/SubjectKey from regional store,
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
    /// Step 2 complete: User, UserEmail, SubjectKey created in regional store.
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
    /// Saga ID reference for the pending org profile in regional store.
    /// Format: `_sys:pending_org_profile:{saga_id}`
    #[serde(default)]
    pub pending_org_profile_key: String,
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

// =============================================================================
// Create Organization Saga (Multi-Group)
// =============================================================================

/// State machine for the Create Organization saga.
///
/// Coordinates writes across GLOBAL + regional stores:
/// 1. GLOBAL: allocate OrganizationId/OrganizationSlug, create directory entry
/// 2. Regional: finalize OrganizationProfile + write ownership membership
/// 3. GLOBAL: update directory status to Active
///
/// Compensation in reverse: step 3 → revert directory status,
/// step 2 → delete profile + membership from regional store,
/// step 1 → delete directory entry + slug index from GLOBAL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CreateOrganizationSagaState {
    /// Initial state: about to allocate IDs and create directory entry in GLOBAL.
    Pending,
    /// Step 1 complete: directory entry created in GLOBAL with Provisioning status.
    DirectoryCreated {
        /// Allocated internal organization ID.
        organization_id: OrganizationId,
        /// Allocated external Snowflake slug.
        organization_slug: OrganizationSlug,
    },
    /// Step 2 complete: profile finalized and ownership membership written in regional store.
    ProfileWritten {
        /// The organization's internal ID.
        organization_id: OrganizationId,
        /// The organization's external slug.
        organization_slug: OrganizationSlug,
    },
    /// Step 3 complete: directory entry marked Active in GLOBAL.
    Completed {
        /// Created organization's internal ID.
        organization_id: OrganizationId,
        /// Created organization's external slug.
        organization_slug: OrganizationSlug,
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
    /// Saga exceeded timeout and was auto-compensated.
    TimedOut,
}

/// Input parameters for Create Organization saga.
///
/// PII (organization name) is intentionally excluded — the saga is stored in
/// the global control plane, and only pseudonymous identifiers are permitted.
/// The organization name is written directly to the regional store by the
/// gRPC handler, not through the saga's serialized state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrganizationInput {
    /// Data residency region for the organization.
    pub region: Region,
    /// Billing tier for the organization.
    pub tier: OrganizationTier,
    /// Initial administrator for this organization.
    pub admin: UserId,
    /// Key in regional store where the handler wrote the pending org profile.
    /// The orchestrator reads this during the regional step to get the org name.
    /// Not PII — just a storage key reference.
    pub pending_profile_key: String,
}

/// Record for the Create Organization saga, tracking state and retry progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrganizationSaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: CreateOrganizationSagaState,
    /// Input parameters (PII-free).
    pub input: CreateOrganizationInput,
    /// When the saga was created.
    pub created_at: DateTime<Utc>,
    /// When the saga was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of retry attempts.
    pub retries: u8,
    /// Next retry time (for exponential backoff).
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl CreateOrganizationSaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: CreateOrganizationInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: CreateOrganizationSagaState::Pending,
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
            CreateOrganizationSagaState::Completed { .. }
                | CreateOrganizationSagaState::Failed { .. }
                | CreateOrganizationSagaState::Compensated { .. }
                | CreateOrganizationSagaState::TimedOut
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
    pub fn transition(&mut self, new_state: CreateOrganizationSagaState) {
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
            self.state = CreateOrganizationSagaState::Failed { step, error };
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
            CreateOrganizationSagaState::Pending => 0,
            CreateOrganizationSagaState::DirectoryCreated { .. } => 1,
            CreateOrganizationSagaState::ProfileWritten { .. } => 2,
            CreateOrganizationSagaState::Completed { .. }
            | CreateOrganizationSagaState::Failed { .. }
            | CreateOrganizationSagaState::Compensated { .. }
            | CreateOrganizationSagaState::TimedOut => 0,
        }
    }

    /// Returns the target region for the current step.
    ///
    /// Steps 0, 2 target GLOBAL; step 1 targets the organization's declared region.
    pub fn target_region(&self) -> Region {
        match &self.state {
            CreateOrganizationSagaState::Pending => Region::GLOBAL,
            CreateOrganizationSagaState::DirectoryCreated { .. } => self.input.region,
            CreateOrganizationSagaState::ProfileWritten { .. } => Region::GLOBAL,
            CreateOrganizationSagaState::Completed { .. }
            | CreateOrganizationSagaState::Failed { .. }
            | CreateOrganizationSagaState::Compensated { .. }
            | CreateOrganizationSagaState::TimedOut => Region::GLOBAL,
        }
    }
}

// =============================================================================
// Create Signing Key Saga
// =============================================================================

/// Input parameters for Create Signing Key saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSigningKeyInput {
    /// Scope of the signing key to create.
    pub scope: SigningKeyScope,
}

/// State machine for the Create Signing Key saga.
///
/// Coordinates signing key creation on the leader:
/// 1. Generate Ed25519 keypair and encrypt private key with RMK
/// 2. Propose `CreateSigningKey` through Raft
///
/// Idempotent: if an active key already exists for the scope, the saga
/// completes immediately without creating a new key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CreateSigningKeySagaState {
    /// Initial state: about to generate keypair and encrypt.
    Pending,
    /// Step 1 complete: keypair generated, encrypted, ready to propose through Raft.
    /// Stores the encrypted material for retry idempotency.
    KeyGenerated {
        /// UUID-format key identifier.
        kid: String,
        /// 32-byte Ed25519 public key.
        public_key_bytes: Vec<u8>,
        /// `SigningKeyEnvelope` serialized bytes.
        encrypted_private_key: Vec<u8>,
        /// RMK version used to wrap the DEK.
        rmk_version: u32,
    },
    /// Step 2 complete: signing key committed through Raft.
    Completed {
        /// The created key's identifier.
        kid: String,
    },
    /// Saga failed after exhausting retries.
    Failed {
        /// The step that failed (0-indexed).
        step: u8,
        /// Error description.
        error: String,
    },
    /// Saga exceeded timeout and was auto-cancelled.
    TimedOut,
}

/// Record for the Create Signing Key saga, tracking state and retry progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSigningKeySaga {
    /// Unique saga identifier.
    pub id: SagaId,
    /// Current state.
    pub state: CreateSigningKeySagaState,
    /// Input parameters.
    pub input: CreateSigningKeyInput,
    /// When the saga was created.
    pub created_at: DateTime<Utc>,
    /// When the saga was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of retry attempts.
    pub retries: u8,
    /// Next retry time (for exponential backoff).
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl CreateSigningKeySaga {
    /// Creates a new saga in Pending state.
    pub fn new(id: SagaId, input: CreateSigningKeyInput) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: CreateSigningKeySagaState::Pending,
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
            CreateSigningKeySagaState::Completed { .. }
                | CreateSigningKeySagaState::Failed { .. }
                | CreateSigningKeySagaState::TimedOut
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
    pub fn transition(&mut self, new_state: CreateSigningKeySagaState) {
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
            self.state = CreateSigningKeySagaState::Failed { step, error };
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
            CreateSigningKeySagaState::Pending => 0,
            CreateSigningKeySagaState::KeyGenerated { .. } => 1,
            CreateSigningKeySagaState::Completed { .. }
            | CreateSigningKeySagaState::Failed { .. }
            | CreateSigningKeySagaState::TimedOut => 0,
        }
    }
}

// =============================================================================
// Generic Saga Wrapper
// =============================================================================

/// Type of saga.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SagaType {
    /// Delete User saga type.
    DeleteUser,
    /// Migrate Organization region saga type.
    MigrateOrg,
    /// Migrate User Region saga type.
    MigrateUser,
    /// Create User saga type (multi-group: GLOBAL + regional).
    CreateUser,
    /// Create Organization saga type (multi-group: GLOBAL + regional).
    CreateOrganization,
    /// Create Signing Key saga type (single-group: generates + encrypts + proposes).
    CreateSigningKey,
}

/// Generic saga record that wraps specific saga types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Saga {
    /// Delete User saga.
    DeleteUser(DeleteUserSaga),
    /// Migrate Organization region saga.
    MigrateOrg(MigrateOrgSaga),
    /// Migrate User Region saga.
    MigrateUser(MigrateUserSaga),
    /// Create User saga (multi-group: GLOBAL + regional).
    CreateUser(CreateUserSaga),
    /// Create Organization saga (multi-group: GLOBAL + regional).
    CreateOrganization(CreateOrganizationSaga),
    /// Create Signing Key saga (single-group: generates + encrypts + proposes).
    CreateSigningKey(CreateSigningKeySaga),
}

impl Saga {
    /// Returns the saga ID.
    pub fn id(&self) -> &SagaId {
        match self {
            Saga::DeleteUser(s) => &s.id,
            Saga::MigrateOrg(s) => &s.id,
            Saga::MigrateUser(s) => &s.id,
            Saga::CreateUser(s) => &s.id,
            Saga::CreateOrganization(s) => &s.id,
            Saga::CreateSigningKey(s) => &s.id,
        }
    }

    /// Returns the saga type.
    pub fn saga_type(&self) -> SagaType {
        match self {
            Saga::DeleteUser(_) => SagaType::DeleteUser,
            Saga::MigrateOrg(_) => SagaType::MigrateOrg,
            Saga::MigrateUser(_) => SagaType::MigrateUser,
            Saga::CreateUser(_) => SagaType::CreateUser,
            Saga::CreateOrganization(_) => SagaType::CreateOrganization,
            Saga::CreateSigningKey(_) => SagaType::CreateSigningKey,
        }
    }

    /// Checks if the saga is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        match self {
            Saga::DeleteUser(s) => s.is_terminal(),
            Saga::MigrateOrg(s) => s.is_terminal(),
            Saga::MigrateUser(s) => s.is_terminal(),
            Saga::CreateUser(s) => s.is_terminal(),
            Saga::CreateOrganization(s) => s.is_terminal(),
            Saga::CreateSigningKey(s) => s.is_terminal(),
        }
    }

    /// Checks if the saga is ready for retry.
    pub fn is_ready_for_retry(&self) -> bool {
        match self {
            Saga::DeleteUser(s) => s.is_ready_for_retry(),
            Saga::MigrateOrg(s) => s.is_ready_for_retry(),
            Saga::MigrateUser(s) => s.is_ready_for_retry(),
            Saga::CreateUser(s) => s.is_ready_for_retry(),
            Saga::CreateOrganization(s) => s.is_ready_for_retry(),
            Saga::CreateSigningKey(s) => s.is_ready_for_retry(),
        }
    }

    /// Returns the creation timestamp.
    pub fn created_at(&self) -> DateTime<Utc> {
        match self {
            Saga::DeleteUser(s) => s.created_at,
            Saga::MigrateOrg(s) => s.created_at,
            Saga::MigrateUser(s) => s.created_at,
            Saga::CreateUser(s) => s.created_at,
            Saga::CreateOrganization(s) => s.created_at,
            Saga::CreateSigningKey(s) => s.created_at,
        }
    }

    /// Returns the last-updated timestamp.
    pub fn updated_at(&self) -> DateTime<Utc> {
        match self {
            Saga::DeleteUser(s) => s.updated_at,
            Saga::MigrateOrg(s) => s.updated_at,
            Saga::MigrateUser(s) => s.updated_at,
            Saga::CreateUser(s) => s.updated_at,
            Saga::CreateOrganization(s) => s.updated_at,
            Saga::CreateSigningKey(s) => s.updated_at,
        }
    }

    /// Returns the retry count.
    pub fn retries(&self) -> u8 {
        match self {
            Saga::DeleteUser(s) => s.retries,
            Saga::MigrateOrg(s) => s.retries,
            Saga::MigrateUser(s) => s.retries,
            Saga::CreateUser(s) => s.retries,
            Saga::CreateOrganization(s) => s.retries,
            Saga::CreateSigningKey(s) => s.retries,
        }
    }

    /// Returns the entity lock keys held by this saga.
    ///
    /// Used to reject concurrent sagas targeting the same entity.
    pub fn lock_keys(&self) -> Vec<SagaLockKey> {
        match self {
            Saga::DeleteUser(s) => vec![SagaLockKey::User(s.input.user)],
            Saga::MigrateOrg(s) => {
                vec![SagaLockKey::Organization(s.input.organization_id)]
            },
            Saga::MigrateUser(s) => vec![SagaLockKey::User(s.input.user)],
            Saga::CreateUser(s) => {
                vec![SagaLockKey::Email(s.input.hmac.clone())]
            },
            Saga::CreateOrganization(s) => match &s.state {
                CreateOrganizationSagaState::DirectoryCreated { organization_id, .. }
                | CreateOrganizationSagaState::ProfileWritten { organization_id, .. }
                | CreateOrganizationSagaState::Completed { organization_id, .. } => {
                    vec![SagaLockKey::Organization(*organization_id)]
                },
                _ => Vec::new(),
            },
            Saga::CreateSigningKey(s) => {
                vec![SagaLockKey::SigningKeyScope(s.input.scope)]
            },
        }
    }

    /// Returns the step number for the current state (used in fail tracking).
    pub fn current_step(&self) -> u8 {
        match self {
            Saga::DeleteUser(s) => s.current_step(),
            Saga::MigrateOrg(s) => s.current_step(),
            Saga::MigrateUser(s) => s.current_step(),
            Saga::CreateUser(s) => s.current_step(),
            Saga::CreateOrganization(s) => s.current_step(),
            Saga::CreateSigningKey(s) => s.current_step(),
        }
    }

    /// Marks the saga as failed at the current step.
    ///
    /// After `MAX_RETRIES` failures, transitions to terminal Failed state.
    pub fn fail(&mut self, error: String) {
        let step = self.current_step();
        match self {
            Saga::DeleteUser(s) => s.fail(step, error),
            Saga::MigrateOrg(s) => s.fail(step, error),
            Saga::MigrateUser(s) => s.fail(step, error),
            Saga::CreateUser(s) => s.fail(step, error),
            Saga::CreateOrganization(s) => s.fail(step, error),
            Saga::CreateSigningKey(s) => s.fail(step, error),
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
    fn test_exponential_backoff() {
        let input = DeleteUserInput {
            user: UserId::new(1),
            organization_ids: vec![OrganizationId::new(100)],
        };
        let mut saga = DeleteUserSaga::new(SagaId::new("saga-123"), input);

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
        let input = DeleteUserInput {
            user: UserId::new(1),
            organization_ids: vec![OrganizationId::new(100)],
        };
        let mut saga = DeleteUserSaga::new(SagaId::new("saga-123"), input);

        // Simulate many retries
        for _ in 0..20 {
            saga.schedule_retry();
        }

        // Should cap at MAX_BACKOFF (5 minutes)
        assert_eq!(saga.next_backoff(), MAX_BACKOFF);
    }

    #[test]
    fn test_fail_after_max_retries() {
        let input = DeleteUserInput {
            user: UserId::new(1),
            organization_ids: vec![OrganizationId::new(100)],
        };
        let mut saga = DeleteUserSaga::new(SagaId::new("saga-123"), input);

        // Fail MAX_RETRIES times
        for _ in 0..MAX_RETRIES {
            saga.fail(1, "test error".to_string());
        }

        // Should now be in Failed state
        assert!(saga.is_terminal());
        assert!(matches!(saga.state, DeleteUserSagaState::Failed { step: 1, .. }));
    }

    #[test]
    fn test_delete_user_saga() {
        let input = DeleteUserInput {
            user: UserId::new(1),
            organization_ids: vec![OrganizationId::new(100), OrganizationId::new(101)],
        };
        let mut saga = DeleteUserSaga::new(SagaId::new("delete-123"), input);

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
        let input = DeleteUserInput {
            user: UserId::new(42),
            organization_ids: vec![OrganizationId::new(1)],
        };
        let saga = Saga::DeleteUser(DeleteUserSaga::new(SagaId::new("saga-123"), input));

        let bytes = saga.to_bytes().unwrap();
        let restored = Saga::from_bytes(&bytes).unwrap();

        assert_eq!(saga.id(), restored.id());
        assert_eq!(saga.saga_type(), restored.saga_type());
    }

    #[test]
    fn test_saga_wrapper() {
        let input = DeleteUserInput {
            user: UserId::new(42),
            organization_ids: vec![OrganizationId::new(1)],
        };
        let saga = Saga::DeleteUser(DeleteUserSaga::new(SagaId::new("saga-123"), input));

        assert_eq!(saga.id(), "saga-123");
        assert_eq!(saga.saga_type(), SagaType::DeleteUser);
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
        let saga = MigrateOrgSaga::new(SagaId::new("migrate-1"), make_migrate_org_input());

        assert_eq!(saga.id, "migrate-1");
        assert_eq!(saga.state, MigrateOrgSagaState::Pending);
        assert_eq!(saga.retries, 0);
        assert!(saga.next_retry_at.is_none());
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
    }

    #[test]
    fn test_migrate_org_saga_transitions() {
        let mut saga = MigrateOrgSaga::new(SagaId::new("migrate-2"), make_migrate_org_input());

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
        let base = MigrateOrgSaga::new(SagaId::new("migrate-3"), make_migrate_org_input());

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
        let mut saga = MigrateOrgSaga::new(SagaId::new("migrate-4"), make_migrate_org_input());

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
        let mut saga = MigrateOrgSaga::new(SagaId::new("migrate-5"), make_migrate_org_input());

        // Back-date creation so the saga appears old.
        saga.created_at = Utc::now() - chrono::Duration::hours(2);

        assert!(saga.is_timed_out(Duration::from_secs(3600))); // 1-hour timeout exceeded
        assert!(!saga.is_timed_out(Duration::from_secs(7200 + 60))); // 2-hour+ timeout not yet exceeded
    }

    #[test]
    fn test_migrate_org_saga_current_step() {
        let mut saga = MigrateOrgSaga::new(SagaId::new("migrate-6"), make_migrate_org_input());

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
        let inner = MigrateOrgSaga::new(SagaId::new("migrate-7"), make_migrate_org_input());
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
            user: UserId::new(7),
            source_region: Region::US_EAST_VA,
            target_region: Region::IE_EAST_DUBLIN,
        }
    }

    #[test]
    fn test_migrate_user_saga_new() {
        let saga = MigrateUserSaga::new(SagaId::new("user-migrate-1"), make_migrate_user_input());

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
            MigrateUserSaga::new(SagaId::new("user-migrate-2"), make_migrate_user_input());

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
        let base = MigrateUserSaga::new(SagaId::new("user-migrate-3"), make_migrate_user_input());

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
            MigrateUserSaga::new(SagaId::new("user-migrate-4"), make_migrate_user_input());

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
            MigrateUserSaga::new(SagaId::new("user-migrate-5"), make_migrate_user_input());
        saga.created_at = Utc::now() - chrono::Duration::minutes(10);

        assert!(saga.is_timed_out(Duration::from_secs(300))); // 5 min timeout
        assert!(!saga.is_timed_out(Duration::from_secs(3600))); // 1 hour timeout
    }

    #[test]
    fn test_migrate_user_saga_current_step() {
        let mut saga =
            MigrateUserSaga::new(SagaId::new("user-migrate-6"), make_migrate_user_input());

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
        let inner = MigrateUserSaga::new(SagaId::new("user-migrate-7"), make_migrate_user_input());
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
        assert_eq!(original.input.user, deserialized.input.user);
        assert_eq!(original.input.source_region, deserialized.input.source_region);
        assert_eq!(original.input.target_region, deserialized.input.target_region);
        assert_eq!(original.state, deserialized.state);
    }

    #[test]
    fn test_migrate_user_saga_wrapper_integration() {
        let inner = MigrateUserSaga::new(SagaId::new("user-migrate-8"), make_migrate_user_input());
        let saga = Saga::MigrateUser(inner);

        assert_eq!(saga.id(), "user-migrate-8");
        assert_eq!(saga.saga_type(), SagaType::MigrateUser);
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
        assert_eq!(saga.retries(), 0);
    }

    // =========================================================================
    // SagaStep and StepStatus tests
    // =========================================================================

    #[test]
    fn test_step_status_serialization() {
        let statuses = [
            (StepStatus::Pending, "\"pending\""),
            (StepStatus::Completed, "\"completed\""),
            (StepStatus::Failed, "\"failed\""),
            (StepStatus::Compensated, "\"compensated\""),
        ];

        for (status, expected_json) in &statuses {
            let json = serde_json::to_string(status).unwrap();
            assert_eq!(&json, expected_json);

            let deserialized: StepStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(*status, deserialized);
        }
    }

    #[test]
    fn test_saga_step_construction_and_serialization() {
        let step = SagaStep {
            step_id: 0,
            target_region: Region::GLOBAL,
            action: "reserve email HMAC".to_string(),
            compensate: "delete email HMAC".to_string(),
            status: StepStatus::Pending,
        };

        assert_eq!(step.step_id, 0);
        assert_eq!(step.target_region, Region::GLOBAL);
        assert_eq!(step.status, StepStatus::Pending);

        let bytes = serde_json::to_vec(&step).unwrap();
        let restored: SagaStep = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(step, restored);
    }

    // =========================================================================
    // SagaLockKey tests
    // =========================================================================

    #[test]
    fn test_saga_lock_key_display() {
        assert_eq!(SagaLockKey::User(UserId::new(42)).to_string(), "user:42");
        assert_eq!(SagaLockKey::Email("abc123".to_string()).to_string(), "email:abc123");
        assert_eq!(SagaLockKey::Organization(OrganizationId::new(7)).to_string(), "org:7");
    }

    #[test]
    fn test_saga_lock_key_equality_and_hash() {
        use std::collections::HashSet;

        let key1 = SagaLockKey::User(UserId::new(1));
        let key2 = SagaLockKey::User(UserId::new(1));
        let key3 = SagaLockKey::User(UserId::new(2));

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);

        let mut set = HashSet::new();
        set.insert(key1.clone());
        assert!(set.contains(&key2));
        assert!(!set.contains(&key3));
    }

    #[test]
    fn test_saga_lock_key_serialization() {
        let keys = vec![
            SagaLockKey::User(UserId::new(42)),
            SagaLockKey::Email("hmac_hex".to_string()),
            SagaLockKey::Organization(OrganizationId::new(7)),
        ];

        for key in &keys {
            let json = serde_json::to_vec(key).unwrap();
            let restored: SagaLockKey = serde_json::from_slice(&json).unwrap();
            assert_eq!(*key, restored);
        }
    }

    // =========================================================================
    // CreateUserSaga tests
    // =========================================================================

    fn make_create_user_input() -> CreateUserInput {
        CreateUserInput {
            hmac: "deadbeef0123456789abcdef".to_string(),
            region: Region::IE_EAST_DUBLIN,
            admin: false,
            pending_org_profile_key: "_sys:pending_org_profile:test-saga".to_string(),
            default_org_tier: OrganizationTier::Free,
        }
    }

    #[test]
    fn test_create_user_saga_new() {
        let saga = CreateUserSaga::new(SagaId::new("cu-1"), make_create_user_input());

        assert_eq!(saga.id, "cu-1");
        assert_eq!(saga.state, CreateUserSagaState::Pending);
        assert_eq!(saga.retries, 0);
        assert!(saga.next_retry_at.is_none());
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
    }

    #[test]
    fn test_create_user_saga_transitions() {
        let mut saga = CreateUserSaga::new(SagaId::new("cu-2"), make_create_user_input());

        // Step 0 → EmailReserved
        saga.transition(CreateUserSagaState::EmailReserved {
            user_id: UserId::new(10),
            user_slug: UserSlug::new(1_000_000),
            hmac_hex: "deadbeef".to_string(),
        });
        assert!(matches!(saga.state, CreateUserSagaState::EmailReserved { .. }));
        assert!(!saga.is_terminal());

        // Step 1 → RegionalDataWritten
        saga.transition(CreateUserSagaState::RegionalDataWritten {
            user_id: UserId::new(10),
            user_slug: UserSlug::new(1_000_000),
            hmac_hex: "deadbeef".to_string(),
        });
        assert!(matches!(saga.state, CreateUserSagaState::RegionalDataWritten { .. }));
        assert!(!saga.is_terminal());

        // Step 2 → Completed
        saga.transition(CreateUserSagaState::Completed {
            user_id: UserId::new(10),
            user_slug: UserSlug::new(1_000_000),
        });
        assert!(saga.is_terminal());
        assert!(!saga.is_ready_for_retry());
    }

    #[test]
    fn test_create_user_saga_is_terminal() {
        let base = CreateUserSaga::new(SagaId::new("cu-3"), make_create_user_input());

        let mut s = base.clone();
        s.state =
            CreateUserSagaState::Completed { user_id: UserId::new(1), user_slug: UserSlug::new(1) };
        assert!(s.is_terminal());

        let mut s = base.clone();
        s.state = CreateUserSagaState::Failed { step: 1, error: "err".to_string() };
        assert!(s.is_terminal());

        let mut s = base.clone();
        s.state =
            CreateUserSagaState::Compensated { step: 0, cleanup_summary: "cleaned".to_string() };
        assert!(s.is_terminal());

        let mut s = base.clone();
        s.state = CreateUserSagaState::TimedOut;
        assert!(s.is_terminal());

        // Non-terminal
        let mut s = base;
        s.state = CreateUserSagaState::EmailReserved {
            user_id: UserId::new(1),
            user_slug: UserSlug::new(1),
            hmac_hex: "abc".to_string(),
        };
        assert!(!s.is_terminal());
    }

    #[test]
    fn test_create_user_saga_fail_and_retry() {
        let mut saga = CreateUserSaga::new(SagaId::new("cu-4"), make_create_user_input());

        for _ in 0..MAX_RETRIES.saturating_sub(1) {
            saga.fail(0, "transient".to_string());
            assert!(!saga.is_terminal());
            assert!(saga.next_retry_at.is_some());
        }

        saga.fail(0, "permanent".to_string());
        assert!(saga.is_terminal());
        assert!(matches!(
            saga.state,
            CreateUserSagaState::Failed { step: 0, ref error } if error == "permanent"
        ));
    }

    #[test]
    fn test_create_user_saga_is_timed_out() {
        let mut saga = CreateUserSaga::new(SagaId::new("cu-5"), make_create_user_input());
        saga.created_at = Utc::now() - chrono::Duration::minutes(5);

        assert!(saga.is_timed_out(Duration::from_secs(60))); // 1 min timeout
        assert!(!saga.is_timed_out(Duration::from_secs(600))); // 10 min timeout
    }

    #[test]
    fn test_create_user_saga_current_step() {
        let mut saga = CreateUserSaga::new(SagaId::new("cu-6"), make_create_user_input());

        assert_eq!(saga.current_step(), 0); // Pending

        saga.state = CreateUserSagaState::EmailReserved {
            user_id: UserId::new(1),
            user_slug: UserSlug::new(1),
            hmac_hex: "abc".to_string(),
        };
        assert_eq!(saga.current_step(), 1);

        saga.state = CreateUserSagaState::RegionalDataWritten {
            user_id: UserId::new(1),
            user_slug: UserSlug::new(1),
            hmac_hex: "abc".to_string(),
        };
        assert_eq!(saga.current_step(), 2);

        // Terminal states return 0
        saga.state =
            CreateUserSagaState::Completed { user_id: UserId::new(1), user_slug: UserSlug::new(1) };
        assert_eq!(saga.current_step(), 0);
    }

    #[test]
    fn test_create_user_saga_target_region() {
        let mut saga = CreateUserSaga::new(SagaId::new("cu-7"), make_create_user_input());
        assert_eq!(saga.input.region, Region::IE_EAST_DUBLIN);

        // Pending → GLOBAL (step 0 targets GLOBAL for ID allocation + email CAS)
        assert_eq!(saga.target_region(), Region::GLOBAL);

        // EmailReserved → user's region (step 1 targets regional store)
        saga.state = CreateUserSagaState::EmailReserved {
            user_id: UserId::new(1),
            user_slug: UserSlug::new(1),
            hmac_hex: "abc".to_string(),
        };
        assert_eq!(saga.target_region(), Region::IE_EAST_DUBLIN);

        // RegionalDataWritten → GLOBAL (step 2 targets GLOBAL for directory entry)
        saga.state = CreateUserSagaState::RegionalDataWritten {
            user_id: UserId::new(1),
            user_slug: UserSlug::new(1),
            hmac_hex: "abc".to_string(),
        };
        assert_eq!(saga.target_region(), Region::GLOBAL);
    }

    #[test]
    fn test_create_user_saga_serialization() {
        let inner = CreateUserSaga::new(SagaId::new("cu-8"), make_create_user_input());
        let saga = Saga::CreateUser(inner);

        let bytes = saga.to_bytes().unwrap();
        let restored = Saga::from_bytes(&bytes).unwrap();

        assert_eq!(saga.id(), restored.id());
        assert_eq!(saga.saga_type(), SagaType::CreateUser);
        assert_eq!(restored.saga_type(), SagaType::CreateUser);
        assert_eq!(saga.retries(), restored.retries());
        assert_eq!(saga.created_at(), restored.created_at());

        let Saga::CreateUser(ref original) = saga else { panic!("expected CreateUser") };
        let Saga::CreateUser(ref deserialized) = restored else { panic!("expected CreateUser") };
        assert_eq!(original.input.hmac, deserialized.input.hmac);
        assert_eq!(original.input.region, deserialized.input.region);
        assert_eq!(original.input.admin, deserialized.input.admin);
        assert_eq!(original.state, deserialized.state);
    }

    #[test]
    fn test_create_user_saga_wrapper_integration() {
        let inner = CreateUserSaga::new(SagaId::new("cu-9"), make_create_user_input());
        let saga = Saga::CreateUser(inner);

        assert_eq!(saga.id(), "cu-9");
        assert_eq!(saga.saga_type(), SagaType::CreateUser);
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
        assert_eq!(saga.retries(), 0);
    }

    // =========================================================================
    // Lock key integration tests
    // =========================================================================

    #[test]
    fn test_saga_lock_keys_create_user() {
        let inner = CreateUserSaga::new(SagaId::new("cu-lock"), make_create_user_input());
        let saga = Saga::CreateUser(inner);

        let keys = saga.lock_keys();
        assert_eq!(keys.len(), 1);
        assert!(matches!(&keys[0], SagaLockKey::Email(h) if h == "deadbeef0123456789abcdef"));
    }

    #[test]
    fn test_saga_lock_keys_migrate_org() {
        let inner = MigrateOrgSaga::new(SagaId::new("mo-lock"), make_migrate_org_input());
        let saga = Saga::MigrateOrg(inner);

        let keys = saga.lock_keys();
        assert_eq!(keys.len(), 1);
        assert!(matches!(keys[0], SagaLockKey::Organization(id) if id == OrganizationId::new(42)));
    }

    #[test]
    fn test_saga_lock_keys_migrate_user() {
        let inner = MigrateUserSaga::new(SagaId::new("mu-lock"), make_migrate_user_input());
        let saga = Saga::MigrateUser(inner);

        let keys = saga.lock_keys();
        assert_eq!(keys.len(), 1);
        assert!(matches!(keys[0], SagaLockKey::User(id) if id == UserId::new(7)));
    }

    // =========================================================================
    // CreateOrganizationSaga tests
    // =========================================================================

    fn make_create_organization_input() -> CreateOrganizationInput {
        CreateOrganizationInput {
            region: Region::US_EAST_VA,
            tier: OrganizationTier::Free,
            admin: UserId::new(1),
            pending_profile_key: "_sys:pending_org_profile:test-saga-123".to_string(),
        }
    }

    #[test]
    fn test_create_organization_saga_new() {
        let saga = CreateOrganizationSaga::new(
            SagaId::new("org-saga-1"),
            make_create_organization_input(),
        );
        assert_eq!(saga.id, "org-saga-1");
        assert_eq!(saga.state, CreateOrganizationSagaState::Pending);
        assert_eq!(saga.retries, 0);
        assert!(saga.next_retry_at.is_none());
        assert!(!saga.is_terminal());
        assert!(saga.is_ready_for_retry());
    }

    #[test]
    fn test_create_organization_saga_transitions() {
        let input = CreateOrganizationInput {
            region: Region::US_EAST_VA,
            tier: OrganizationTier::Pro,
            admin: UserId::new(1),
            pending_profile_key: "_sys:pending_org_profile:test".to_string(),
        };
        let mut saga = CreateOrganizationSaga::new(SagaId::new("org-saga-2"), input);

        let org_id = OrganizationId::new(42);
        let org_slug = OrganizationSlug::new(9999);

        // Pending → DirectoryCreated
        saga.transition(CreateOrganizationSagaState::DirectoryCreated {
            organization_id: org_id,
            organization_slug: org_slug,
        });
        assert_eq!(saga.current_step(), 1);
        assert!(!saga.is_terminal());

        // DirectoryCreated → ProfileWritten
        saga.transition(CreateOrganizationSagaState::ProfileWritten {
            organization_id: org_id,
            organization_slug: org_slug,
        });
        assert_eq!(saga.current_step(), 2);
        assert!(!saga.is_terminal());

        // ProfileWritten → Completed
        saga.transition(CreateOrganizationSagaState::Completed {
            organization_id: org_id,
            organization_slug: org_slug,
        });
        assert!(saga.is_terminal());
    }

    #[test]
    fn test_create_organization_saga_target_region() {
        let input = CreateOrganizationInput {
            region: Region::IE_EAST_DUBLIN,
            tier: OrganizationTier::Free,
            admin: UserId::new(1),
            pending_profile_key: "_sys:pending_org_profile:test".to_string(),
        };
        let mut saga = CreateOrganizationSaga::new(SagaId::new("org-saga-3"), input);

        // Step 0 targets GLOBAL
        assert_eq!(saga.target_region(), Region::GLOBAL);

        // Step 1 targets regional
        saga.transition(CreateOrganizationSagaState::DirectoryCreated {
            organization_id: OrganizationId::new(1),
            organization_slug: OrganizationSlug::new(1),
        });
        assert_eq!(saga.target_region(), Region::IE_EAST_DUBLIN);

        // Step 2 targets GLOBAL
        saga.transition(CreateOrganizationSagaState::ProfileWritten {
            organization_id: OrganizationId::new(1),
            organization_slug: OrganizationSlug::new(1),
        });
        assert_eq!(saga.target_region(), Region::GLOBAL);
    }

    #[test]
    fn test_create_organization_saga_is_terminal() {
        let base = CreateOrganizationSaga::new(
            SagaId::new("org-saga-4"),
            make_create_organization_input(),
        );

        let mut s = base.clone();
        s.state = CreateOrganizationSagaState::Completed {
            organization_id: OrganizationId::new(1),
            organization_slug: OrganizationSlug::new(1),
        };
        assert!(s.is_terminal());

        let mut s = base.clone();
        s.state = CreateOrganizationSagaState::Failed { step: 1, error: "err".to_string() };
        assert!(s.is_terminal());

        let mut s = base.clone();
        s.state = CreateOrganizationSagaState::Compensated {
            step: 0,
            cleanup_summary: "cleaned".to_string(),
        };
        assert!(s.is_terminal());

        let mut s = base.clone();
        s.state = CreateOrganizationSagaState::TimedOut;
        assert!(s.is_terminal());

        // Non-terminal
        let mut s = base;
        s.state = CreateOrganizationSagaState::DirectoryCreated {
            organization_id: OrganizationId::new(1),
            organization_slug: OrganizationSlug::new(1),
        };
        assert!(!s.is_terminal());
    }

    #[test]
    fn test_create_organization_saga_fail_and_retry() {
        let mut saga = CreateOrganizationSaga::new(
            SagaId::new("org-saga-5"),
            make_create_organization_input(),
        );

        for _ in 0..MAX_RETRIES.saturating_sub(1) {
            saga.fail(0, "transient".to_string());
            assert!(!saga.is_terminal());
            assert!(saga.next_retry_at.is_some());
        }

        saga.fail(0, "permanent".to_string());
        assert!(saga.is_terminal());
        assert!(matches!(
            saga.state,
            CreateOrganizationSagaState::Failed { step: 0, ref error } if error == "permanent"
        ));
    }

    #[test]
    fn test_create_organization_saga_is_timed_out() {
        let mut saga = CreateOrganizationSaga::new(
            SagaId::new("org-saga-6"),
            make_create_organization_input(),
        );
        saga.created_at = Utc::now() - chrono::Duration::minutes(5);

        assert!(saga.is_timed_out(Duration::from_secs(60))); // 1 min timeout
        assert!(!saga.is_timed_out(Duration::from_secs(600))); // 10 min timeout
    }

    #[test]
    fn test_create_organization_saga_current_step() {
        let mut saga = CreateOrganizationSaga::new(
            SagaId::new("org-saga-7"),
            make_create_organization_input(),
        );

        assert_eq!(saga.current_step(), 0); // Pending

        saga.state = CreateOrganizationSagaState::DirectoryCreated {
            organization_id: OrganizationId::new(1),
            organization_slug: OrganizationSlug::new(1),
        };
        assert_eq!(saga.current_step(), 1);

        saga.state = CreateOrganizationSagaState::ProfileWritten {
            organization_id: OrganizationId::new(1),
            organization_slug: OrganizationSlug::new(1),
        };
        assert_eq!(saga.current_step(), 2);

        // Terminal states return 0
        saga.state = CreateOrganizationSagaState::Completed {
            organization_id: OrganizationId::new(1),
            organization_slug: OrganizationSlug::new(1),
        };
        assert_eq!(saga.current_step(), 0);

        saga.state = CreateOrganizationSagaState::Failed { step: 1, error: "err".to_string() };
        assert_eq!(saga.current_step(), 0);

        saga.state = CreateOrganizationSagaState::TimedOut;
        assert_eq!(saga.current_step(), 0);
    }

    #[test]
    fn test_create_organization_saga_serialization() {
        let input = CreateOrganizationInput {
            region: Region::US_EAST_VA,
            tier: OrganizationTier::Enterprise,
            admin: UserId::new(7),
            pending_profile_key: "_sys:pending_org_profile:abc".to_string(),
        };
        let saga = CreateOrganizationSaga::new(SagaId::new("org-saga-ser"), input);
        let wrapped = Saga::CreateOrganization(saga);

        let json = serde_json::to_string(&wrapped).unwrap();
        let deserialized: Saga = serde_json::from_str(&json).unwrap();

        match deserialized {
            Saga::CreateOrganization(s) => {
                assert_eq!(s.id, "org-saga-ser");
                assert_eq!(s.input.region, Region::US_EAST_VA);
                assert_eq!(s.input.tier, OrganizationTier::Enterprise);
                assert_eq!(s.input.admin, UserId::new(7));
                assert_eq!(s.input.pending_profile_key, "_sys:pending_org_profile:abc");
            },
            _ => panic!("wrong saga variant"),
        }
    }

    #[test]
    fn test_create_organization_saga_wrapper() {
        let saga = CreateOrganizationSaga::new(
            SagaId::new("org-saga-wrap"),
            make_create_organization_input(),
        );
        let wrapped = Saga::CreateOrganization(saga);

        assert_eq!(wrapped.id(), "org-saga-wrap");
        assert_eq!(wrapped.saga_type(), SagaType::CreateOrganization);
        assert!(!wrapped.is_terminal());
        assert!(wrapped.is_ready_for_retry());
        assert_eq!(wrapped.retries(), 0);
        assert_eq!(wrapped.current_step(), 0);
    }

    #[test]
    fn test_create_organization_saga_lock_keys() {
        // Pending state has no lock keys (no org ID allocated yet)
        let saga = CreateOrganizationSaga::new(
            SagaId::new("org-saga-lock-1"),
            make_create_organization_input(),
        );
        let wrapped = Saga::CreateOrganization(saga);
        assert!(wrapped.lock_keys().is_empty());

        // DirectoryCreated state locks the organization
        let mut saga = CreateOrganizationSaga::new(
            SagaId::new("org-saga-lock-2"),
            make_create_organization_input(),
        );
        saga.transition(CreateOrganizationSagaState::DirectoryCreated {
            organization_id: OrganizationId::new(42),
            organization_slug: OrganizationSlug::new(9999),
        });
        let wrapped = Saga::CreateOrganization(saga);
        let keys = wrapped.lock_keys();
        assert_eq!(keys.len(), 1);
        assert!(matches!(keys[0], SagaLockKey::Organization(id) if id == OrganizationId::new(42)));

        // ProfileWritten state also locks the organization
        let mut saga = CreateOrganizationSaga::new(
            SagaId::new("org-saga-lock-3"),
            make_create_organization_input(),
        );
        saga.state = CreateOrganizationSagaState::ProfileWritten {
            organization_id: OrganizationId::new(42),
            organization_slug: OrganizationSlug::new(9999),
        };
        let wrapped = Saga::CreateOrganization(saga);
        let keys = wrapped.lock_keys();
        assert_eq!(keys.len(), 1);
        assert!(matches!(keys[0], SagaLockKey::Organization(id) if id == OrganizationId::new(42)));

        // Completed state also locks the organization
        let mut saga = CreateOrganizationSaga::new(
            SagaId::new("org-saga-lock-4"),
            make_create_organization_input(),
        );
        saga.state = CreateOrganizationSagaState::Completed {
            organization_id: OrganizationId::new(42),
            organization_slug: OrganizationSlug::new(9999),
        };
        let wrapped = Saga::CreateOrganization(saga);
        let keys = wrapped.lock_keys();
        assert_eq!(keys.len(), 1);
        assert!(matches!(keys[0], SagaLockKey::Organization(id) if id == OrganizationId::new(42)));

        // Failed state has no lock keys
        let mut saga = CreateOrganizationSaga::new(
            SagaId::new("org-saga-lock-5"),
            make_create_organization_input(),
        );
        saga.state = CreateOrganizationSagaState::Failed { step: 0, error: "err".to_string() };
        let wrapped = Saga::CreateOrganization(saga);
        assert!(wrapped.lock_keys().is_empty());
    }

    // =========================================================================
    // CreateSigningKeySaga tests
    // =========================================================================

    fn make_create_signing_key_input() -> CreateSigningKeyInput {
        CreateSigningKeyInput { scope: SigningKeyScope::Global }
    }

    #[test]
    fn test_create_signing_key_saga_new() {
        let saga = CreateSigningKeySaga::new(SagaId::new("sk-1"), make_create_signing_key_input());
        assert_eq!(saga.id, "sk-1");
        assert_eq!(saga.state, CreateSigningKeySagaState::Pending);
        assert_eq!(saga.retries, 0);
        assert!(saga.next_retry_at.is_none());
        assert_eq!(saga.input.scope, SigningKeyScope::Global);
    }

    #[test]
    fn test_create_signing_key_saga_full_lifecycle() {
        let mut saga =
            CreateSigningKeySaga::new(SagaId::new("sk-2"), make_create_signing_key_input());

        // Step 1: key generated
        saga.transition(CreateSigningKeySagaState::KeyGenerated {
            kid: "kid-uuid-1".to_string(),
            public_key_bytes: vec![0x01; 32],
            encrypted_private_key: vec![0x02; 100],
            rmk_version: 1,
        });
        assert!(matches!(saga.state, CreateSigningKeySagaState::KeyGenerated { .. }));
        assert_eq!(saga.current_step(), 1);

        // Step 2: completed
        saga.transition(CreateSigningKeySagaState::Completed { kid: "kid-uuid-1".to_string() });
        assert!(
            matches!(saga.state, CreateSigningKeySagaState::Completed { ref kid } if kid == "kid-uuid-1")
        );
        assert!(saga.is_terminal());
    }

    #[test]
    fn test_create_signing_key_saga_terminal_states() {
        let base = CreateSigningKeySaga::new(SagaId::new("sk-3"), make_create_signing_key_input());

        let mut s = base.clone();
        s.state = CreateSigningKeySagaState::Completed { kid: "k1".to_string() };
        assert!(s.is_terminal());

        let mut s = base.clone();
        s.state = CreateSigningKeySagaState::Failed { step: 0, error: "err".to_string() };
        assert!(s.is_terminal());

        let mut s = base.clone();
        s.state = CreateSigningKeySagaState::TimedOut;
        assert!(s.is_terminal());

        // Non-terminal: Pending
        let s = base.clone();
        assert!(!s.is_terminal());

        // Non-terminal: KeyGenerated
        let mut s = base;
        s.state = CreateSigningKeySagaState::KeyGenerated {
            kid: "k2".to_string(),
            public_key_bytes: vec![],
            encrypted_private_key: vec![],
            rmk_version: 1,
        };
        assert!(!s.is_terminal());
    }

    #[test]
    fn test_create_signing_key_saga_fail_and_retry() {
        let mut saga =
            CreateSigningKeySaga::new(SagaId::new("sk-4"), make_create_signing_key_input());

        // First failure: stays in Pending, gets retry backoff
        saga.fail(0, "transient error".to_string());
        assert_eq!(saga.retries, 1);
        assert!(!saga.is_terminal());
        assert!(saga.next_retry_at.is_some());

        // Exhaust retries
        for _ in 1..MAX_RETRIES {
            saga.fail(0, "permanent".to_string());
        }
        assert!(matches!(
            saga.state,
            CreateSigningKeySagaState::Failed { step: 0, ref error } if error == "permanent"
        ));
        assert!(saga.is_terminal());
    }

    #[test]
    fn test_create_signing_key_saga_retry_readiness() {
        let mut saga =
            CreateSigningKeySaga::new(SagaId::new("sk-5"), make_create_signing_key_input());
        assert!(saga.is_ready_for_retry());

        // After failure, has a future retry_at
        saga.fail(0, "err".to_string());
        assert!(!saga.is_ready_for_retry());
    }

    #[test]
    fn test_create_signing_key_saga_is_timed_out() {
        let mut saga =
            CreateSigningKeySaga::new(SagaId::new("sk-timeout"), make_create_signing_key_input());
        saga.created_at = Utc::now() - chrono::Duration::minutes(5);

        assert!(saga.is_timed_out(Duration::from_secs(60))); // 1 min timeout
        assert!(!saga.is_timed_out(Duration::from_secs(600))); // 10 min timeout
    }

    #[test]
    fn test_create_signing_key_saga_current_step() {
        let mut saga =
            CreateSigningKeySaga::new(SagaId::new("sk-6"), make_create_signing_key_input());
        assert_eq!(saga.current_step(), 0);

        saga.state = CreateSigningKeySagaState::KeyGenerated {
            kid: "k".to_string(),
            public_key_bytes: vec![],
            encrypted_private_key: vec![],
            rmk_version: 1,
        };
        assert_eq!(saga.current_step(), 1);

        saga.state = CreateSigningKeySagaState::Completed { kid: "k".to_string() };
        assert_eq!(saga.current_step(), 0);

        saga.state = CreateSigningKeySagaState::Failed { step: 1, error: "err".to_string() };
        assert_eq!(saga.current_step(), 0);

        saga.state = CreateSigningKeySagaState::TimedOut;
        assert_eq!(saga.current_step(), 0);
    }

    #[test]
    fn test_create_signing_key_saga_serialization() {
        let inner =
            CreateSigningKeySaga::new(SagaId::new("sk-ser-1"), make_create_signing_key_input());
        let saga = Saga::CreateSigningKey(inner);
        let bytes = saga.to_bytes().unwrap();
        let restored = Saga::from_bytes(&bytes).unwrap();
        assert_eq!(saga.id(), restored.id());
        assert_eq!(saga.saga_type(), SagaType::CreateSigningKey);
        assert_eq!(restored.saga_type(), SagaType::CreateSigningKey);

        let Saga::CreateSigningKey(ref original) = saga else {
            panic!("expected CreateSigningKey")
        };
        let Saga::CreateSigningKey(ref deserialized) = restored else {
            panic!("expected CreateSigningKey")
        };
        assert_eq!(original.id, deserialized.id);
        assert_eq!(original.input.scope, deserialized.input.scope);
    }

    #[test]
    fn test_create_signing_key_saga_wrapper_delegation() {
        let inner =
            CreateSigningKeySaga::new(SagaId::new("sk-wrap-1"), make_create_signing_key_input());
        let saga = Saga::CreateSigningKey(inner);

        assert_eq!(saga.id(), "sk-wrap-1");
        assert_eq!(saga.saga_type(), SagaType::CreateSigningKey);
        assert!(!saga.is_terminal());
        assert_eq!(saga.retries(), 0);
        assert_eq!(saga.current_step(), 0);
    }

    #[test]
    fn test_create_signing_key_saga_lock_keys() {
        // Global scope
        let inner = CreateSigningKeySaga::new(
            SagaId::new("sk-lock-1"),
            CreateSigningKeyInput { scope: SigningKeyScope::Global },
        );
        let saga = Saga::CreateSigningKey(inner);
        let keys = saga.lock_keys();
        assert_eq!(keys.len(), 1);
        assert!(matches!(keys[0], SagaLockKey::SigningKeyScope(SigningKeyScope::Global)));

        // Organization scope
        let inner = CreateSigningKeySaga::new(
            SagaId::new("sk-lock-2"),
            CreateSigningKeyInput { scope: SigningKeyScope::Organization(OrganizationId::new(42)) },
        );
        let saga = Saga::CreateSigningKey(inner);
        let keys = saga.lock_keys();
        assert_eq!(keys.len(), 1);
        assert!(matches!(
            keys[0],
            SagaLockKey::SigningKeyScope(SigningKeyScope::Organization(id)) if id == OrganizationId::new(42)
        ));
    }

    #[test]
    fn test_signing_key_scope_lock_key_display() {
        assert_eq!(
            SagaLockKey::SigningKeyScope(SigningKeyScope::Global).to_string(),
            "signing_key:global"
        );
        assert_eq!(
            SagaLockKey::SigningKeyScope(SigningKeyScope::Organization(OrganizationId::new(7)))
                .to_string(),
            "signing_key:org:7"
        );
    }

    #[test]
    fn test_signing_key_scope_lock_key_serde() {
        let keys = vec![
            SagaLockKey::SigningKeyScope(SigningKeyScope::Global),
            SagaLockKey::SigningKeyScope(SigningKeyScope::Organization(OrganizationId::new(42))),
        ];
        for key in keys {
            let json = serde_json::to_vec(&key).unwrap();
            let restored: SagaLockKey = serde_json::from_slice(&json).unwrap();
            assert_eq!(key, restored);
        }
    }

    #[test]
    fn test_create_signing_key_saga_org_scope() {
        let saga = CreateSigningKeySaga::new(
            SagaId::new("sk-org-1"),
            CreateSigningKeyInput { scope: SigningKeyScope::Organization(OrganizationId::new(99)) },
        );
        assert_eq!(saga.input.scope, SigningKeyScope::Organization(OrganizationId::new(99)));

        // Roundtrip through Saga wrapper
        let wrapped = Saga::CreateSigningKey(saga);
        let bytes = wrapped.to_bytes().unwrap();
        let restored = Saga::from_bytes(&bytes).unwrap();
        let Saga::CreateSigningKey(ref s) = restored else { panic!("expected CreateSigningKey") };
        assert_eq!(s.input.scope, SigningKeyScope::Organization(OrganizationId::new(99)));
    }
}
