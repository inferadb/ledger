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
use inferadb_ledger_store::StorageBackend;
pub use inferadb_ledger_types::{
    AppCredentialType, OrganizationMemberRole, SigningKeyScope, SigningKeyStatus, UserRole,
    UserStatus,
};
use inferadb_ledger_types::{Region, decode};
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
    default_requires_residency, default_retention_days,
};

use crate::state::StateLayer;

/// Region residency contract resolved from the GLOBAL region directory.
///
/// Returned by [`lookup_region_residency`] — every authoritative residency
/// decision should consult the registry rather than the previously hardcoded
/// `Region::requires_residency()` / `Region::retention_days()` methods (which
/// have been removed). For unknown / pre-R6 regions the returned values fall
/// back to the disciplined defaults ([`default_requires_residency`] = `true`,
/// [`default_retention_days`] = 90).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionResidency {
    /// Whether PII / state for this region must stay in-region.
    pub requires_residency: bool,
    /// Soft-delete retention period, in days.
    pub retention_days: u32,
}

impl Default for RegionResidency {
    fn default() -> Self {
        Self {
            requires_residency: default_requires_residency(),
            retention_days: default_retention_days(),
        }
    }
}

impl From<&RegionDirectoryEntry> for RegionResidency {
    fn from(entry: &RegionDirectoryEntry) -> Self {
        Self { requires_residency: entry.requires_residency, retention_days: entry.retention_days }
    }
}

/// Looks up a region's residency contract from the GLOBAL region directory.
///
/// Reads `_dir:region:{name}` from the system vault on the supplied
/// [`StateLayer`]. Returns:
/// - `Ok(Some(RegionResidency))` for a registered region,
/// - `Ok(None)` for `Region::GLOBAL` (the cluster control plane is never subject to data-residency
///   enforcement) and for any region whose directory entry has not yet been written (caller decides
///   the safe-default behaviour),
/// - `Err(_)` only when the underlying state read itself fails.
///
/// Callers that need a non-`Option` answer should treat `None` as
/// [`RegionResidency::default()`] (default-deny: `requires_residency=true`,
/// `retention_days=90`).
pub fn lookup_region_residency<B: StorageBackend>(
    state: &StateLayer<B>,
    region: Region,
) -> Result<Option<RegionResidency>, crate::state::StateError> {
    if region.is_global() {
        return Ok(None);
    }
    let key = SystemKeys::region_directory_entry(region.as_str());
    let Some(entity) = state.get_entity(SYSTEM_VAULT_ID, key.as_bytes())? else {
        return Ok(None);
    };
    match decode::<RegionDirectoryEntry>(&entity.value) {
        Ok(entry) => Ok(Some(RegionResidency::from(&entry))),
        Err(e) => {
            tracing::warn!(
                region = region.as_str(),
                error = %e,
                "RegionDirectoryEntry failed to decode — falling back to disciplined defaults"
            );
            Ok(None)
        },
    }
}

/// Same as [`lookup_region_residency`] but collapses missing / unreadable
/// entries into [`RegionResidency::default()`] — the disciplined-default
/// path used at every site that previously called the hardcoded
/// `Region::requires_residency()` / `Region::retention_days()` methods.
pub fn region_residency_or_default<B: StorageBackend>(
    state: &StateLayer<B>,
    region: Region,
) -> RegionResidency {
    if region.is_global() {
        // GLOBAL is the control plane: no PII, no residency, no retention
        // soft-delete cycle — use a 0-day retention as a tripwire (any
        // attempt to soft-delete in GLOBAL state is a bug).
        return RegionResidency { requires_residency: false, retention_days: 0 };
    }
    match lookup_region_residency(state, region) {
        Ok(Some(r)) => r,
        Ok(None) => RegionResidency::default(),
        Err(e) => {
            tracing::warn!(
                region = region.as_str(),
                error = %e,
                "Region directory lookup failed — falling back to disciplined defaults"
            );
            RegionResidency::default()
        },
    }
}

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
