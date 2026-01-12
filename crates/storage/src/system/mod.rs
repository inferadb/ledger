//! System namespace (`_system`) for global data.
//!
//! The `_system` namespace stores global entities that span all namespaces:
//! - User accounts (global identity)
//! - Email addresses (with uniqueness enforcement)
//! - Namespace routing table
//! - Cluster node membership
//! - Cross-namespace sagas
//!
//! Per DESIGN.md lines 1858-1996 and §4.6.

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
pub use service::{SYSTEM_NAMESPACE_ID, SYSTEM_VAULT_ID, SystemError, SystemNamespaceService};
pub use types::{
    EmailVerificationToken, NamespaceRegistry, NamespaceStatus, NodeInfo, NodeRole, User,
    UserEmail, UserStatus,
};
