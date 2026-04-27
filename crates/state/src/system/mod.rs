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
pub mod token;
mod types;

pub use cluster::{
    ClusterMembership, GroupMembership, MAX_VOTERS, MIN_NODES_PER_PROTECTED_REGION, SystemRole,
};
pub use inferadb_ledger_types::{
    AppCredentialType, OrganizationMemberRole, SigningKeyScope, SigningKeyStatus, UserRole,
    UserStatus,
};
pub use keys::{KeyFamily, KeyRegistryEntry, KeyTier, SystemKeys};
pub use saga::{
    CreateOnboardingUserInput, CreateOnboardingUserSaga, CreateOnboardingUserSagaState,
    CreateOrganizationInput, CreateOrganizationSaga, CreateOrganizationSagaState,
    CreateSigningKeyInput, CreateSigningKeySaga, CreateSigningKeySagaState, CreateUserInput,
    CreateUserSaga, CreateUserSagaState, DeleteUserInput, DeleteUserSaga, DeleteUserSagaState,
    MAX_RETRIES, MigrateOrgInput, MigrateOrgSaga, MigrateOrgSagaState, MigrateUserInput,
    MigrateUserSaga, MigrateUserSagaState, SAGA_POLL_INTERVAL, Saga, SagaId, SagaLockKey, SagaStep,
    SagaType, StepStatus,
};
pub use service::{
    SYSTEM_ORGANIZATION_ID, SYSTEM_VAULT_ID, SystemError, SystemOrganizationService,
    audit::{AuditKeys, AuditRecord, write_audit_record},
};
pub use token::{
    AllAppSessionsRevocationResult, AllUserSessionsRevocationResult, ExpiredTokenCleanupResult,
    RevocationResult,
};
pub use types::{
    App, AppCredentials, AppProfile, AppVaultConnection, ClientAssertionCredentialConfig,
    ClientAssertionEntry, ClientSecretCredential, EmailHashEntry, EmailVerificationToken,
    ErasureAuditRecord, MigrationSummary, MtlsCredential, NodeInfo, NodeRole, OnboardingAccount,
    OrgShredKey, Organization, OrganizationMember, OrganizationProfile, OrganizationRegistry,
    OrganizationStatus, OrganizationTier, PendingEmailVerification, ProvisioningReservation,
    RefreshToken, RegionDirectoryEntry, SigningKey, Team, TeamMember, TeamMemberRole, User,
    UserDirectoryEntry, UserDirectoryStatus, UserEmail, UserMigrationEntry, UserShredKey,
};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
pub(crate) fn create_test_service()
-> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
    use std::sync::Arc;

    let engine = crate::engine::InMemoryStorageEngine::open().unwrap();
    let meta_engine = crate::engine::InMemoryStorageEngine::open().unwrap();
    let state = Arc::new(
        crate::state::new_state_layer_shared(engine.db(), meta_engine.db())
            .expect("build shared StateLayer for system test service"),
    );
    SystemOrganizationService::new(state)
}
