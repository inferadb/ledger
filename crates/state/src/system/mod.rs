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

pub use cluster::{ClusterMembership, LearnerCacheConfig, MAX_VOTERS, SystemRole};
pub use keys::SystemKeys;
pub use saga::{
    CreateOrgInput, CreateOrgSaga, CreateOrgSagaState, DeleteUserInput, DeleteUserSaga,
    DeleteUserSagaState, MAX_RETRIES, SAGA_POLL_INTERVAL, Saga, SagaId, SagaType,
};
pub use service::{
    SYSTEM_ORGANIZATION_ID, SYSTEM_VAULT_ID, SystemError, SystemOrganizationService,
};
pub use types::{
    EmailVerificationToken, NodeInfo, NodeRole, OrganizationRegistry, OrganizationStatus, User,
    UserEmail, UserRole, UserStatus,
};
