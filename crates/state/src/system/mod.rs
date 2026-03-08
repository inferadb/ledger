//! System organization (`_system`) for global data.
//!
//! The `_system` organization stores global entities that span all organizations:
//! - User accounts (global identity)
//! - Email addresses (with uniqueness enforcement)
//! - Organization routing table
//! - Cluster node membership
//! - Cross-organization sagas

mod cluster;
mod keys;
mod saga;
mod service;
mod types;

pub use cluster::{
    ClusterMembership, GroupMembership, LearnerCacheConfig, MAX_VOTERS,
    MIN_NODES_PER_PROTECTED_REGION, SystemRole,
};
pub use inferadb_ledger_types::{UserRole, UserStatus};
pub use keys::SystemKeys;
pub use saga::{
    CreateOrganizationInput, CreateOrganizationSaga, CreateOrganizationSagaState, CreateUserInput,
    CreateUserSaga, CreateUserSagaState, DeleteUserInput, DeleteUserSaga, DeleteUserSagaState,
    MAX_RETRIES, MigrateOrgInput, MigrateOrgSaga, MigrateOrgSagaState, MigrateUserInput,
    MigrateUserSaga, MigrateUserSagaState, SAGA_POLL_INTERVAL, Saga, SagaId, SagaLockKey, SagaStep,
    SagaType, StepStatus,
};
pub use service::{
    SYSTEM_ORGANIZATION_ID, SYSTEM_VAULT_ID, SystemError, SystemOrganizationService,
};
pub use types::{
    App, AppCredentialType, AppCredentials, AppVaultConnection, ClientAssertionCredentialConfig,
    ClientAssertionEntry, ClientSecretCredential, EmailVerificationToken, ErasureAuditRecord,
    MigrationSummary, MtlsCredential, NodeInfo, NodeRole, OrganizationDirectoryEntry,
    OrganizationDirectoryStatus, OrganizationMember, OrganizationMemberRole, OrganizationProfile,
    OrganizationRegistry, OrganizationStatus, OrganizationTier, PendingOrganizationProfile,
    SubjectKey, TeamMember, TeamMemberRole, TeamProfile, User, UserDirectoryEntry,
    UserDirectoryStatus, UserEmail, UserMigrationEntry,
};
