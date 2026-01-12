//! System namespace (`_system`) for global data.
//!
//! The `_system` namespace stores global entities that span all namespaces:
//! - User accounts (global identity)
//! - Email addresses (with uniqueness enforcement)
//! - Namespace routing table
//! - Cluster node membership
//!
//! Per DESIGN.md lines 1858-1996.

mod cluster;
mod keys;
mod service;
mod types;

pub use cluster::{ClusterMembership, MAX_VOTERS, SystemRole};
pub use keys::SystemKeys;
pub use service::{SystemError, SystemNamespaceService, SYSTEM_NAMESPACE_ID, SYSTEM_VAULT_ID};
pub use types::{
    EmailVerificationToken, NamespaceRegistry, NamespaceStatus, NodeInfo, NodeRole, User,
    UserEmail, UserStatus,
};
