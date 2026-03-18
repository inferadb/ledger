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
//! Sagas are stored in `_system` organization under `_meta:saga:{saga_id}` keys.
//! The leader polls for incomplete sagas every 30 seconds.

mod create_onboarding_user;
mod create_organization;
mod create_signing_key;
mod create_user;
mod delete_user;
mod migrate_org;
mod migrate_user;

use std::time::Duration;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{OrganizationId, OrganizationSlug, Region, SigningKeyScope, UserId};
use serde::{Deserialize, Serialize};

pub use self::{
    create_onboarding_user::{
        CreateOnboardingUserInput, CreateOnboardingUserSaga, CreateOnboardingUserSagaState,
    },
    create_organization::{
        CreateOrganizationInput, CreateOrganizationSaga, CreateOrganizationSagaState,
    },
    create_signing_key::{CreateSigningKeyInput, CreateSigningKeySaga, CreateSigningKeySagaState},
    create_user::{CreateUserInput, CreateUserSaga, CreateUserSagaState},
    delete_user::{DeleteUserInput, DeleteUserSaga, DeleteUserSagaState},
    migrate_org::{MigrateOrgInput, MigrateOrgSaga, MigrateOrgSagaState},
    migrate_user::{MigrateUserInput, MigrateUserSaga, MigrateUserSagaState},
};
use super::types::OrganizationTier;

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
    /// Create Onboarding User saga type (multi-group: GLOBAL → regional → GLOBAL).
    CreateOnboardingUser,
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
    /// Create Onboarding User saga (multi-group: GLOBAL → regional → GLOBAL).
    CreateOnboardingUser(CreateOnboardingUserSaga),
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
            Saga::CreateOnboardingUser(s) => &s.id,
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
            Saga::CreateOnboardingUser(_) => SagaType::CreateOnboardingUser,
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
            Saga::CreateOnboardingUser(s) => s.is_terminal(),
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
            Saga::CreateOnboardingUser(s) => s.is_ready_for_retry(),
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
            Saga::CreateOnboardingUser(s) => s.created_at,
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
            Saga::CreateOnboardingUser(s) => s.updated_at,
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
            Saga::CreateOnboardingUser(s) => s.retries,
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
            Saga::CreateOnboardingUser(s) => {
                vec![SagaLockKey::Email(s.input.email_hmac.clone())]
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
            Saga::CreateOnboardingUser(s) => s.current_step(),
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
            Saga::CreateOnboardingUser(s) => s.fail(step, error),
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
mod tests;
