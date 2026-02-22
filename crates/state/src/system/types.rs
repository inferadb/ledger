//! Data model types for the `_system` organization.

use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{NodeId, OrganizationId, ShardId, UserId};
use serde::{Deserialize, Serialize};

// ============================================================================
// User Types
// ============================================================================

/// Global user account.
///
/// Users exist globally in `_system`, independent of organizations.
/// Organizations reference users by their global `UserId`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// Unique user identifier (globally unique).
    pub id: UserId,
    /// User's display name.
    pub name: String,
    /// ID of the user's primary email address.
    pub primary_email_id: i64,
    /// Current user status.
    pub status: UserStatus,
    /// Authorization role (regular user or service admin).
    pub role: UserRole,
    /// Account creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp.
    pub updated_at: DateTime<Utc>,
}

/// User account status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserStatus {
    /// User is active and can authenticate.
    #[default]
    Active,
    /// User is pending organization creation (saga in progress).
    PendingOrg,
    /// User is suspended and cannot authenticate.
    Suspended,
    /// User is being deleted (cascade in progress).
    Deleting,
    /// User has been deleted (tombstone for audit).
    Deleted,
}

/// User authorization role.
///
/// Determines what operations a user can perform at the global service level.
/// Organization-level permissions are separate (derived from membership records).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    /// Regular user with standard permissions.
    #[default]
    User,
    /// Global service administrator.
    Admin,
}

// ============================================================================
// Email Types
// ============================================================================

/// User email address.
///
/// Users can have multiple email addresses. One is marked as primary.
/// Global email uniqueness is enforced via the `_idx:email:{email}` index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserEmail {
    /// Unique email record identifier.
    pub id: i64,
    /// User who owns this email.
    pub user_id: UserId,
    /// Email address (lowercase normalized).
    pub email: String,
    /// Whether this email has been verified.
    pub verified: bool,
    /// Whether this is the user's primary email.
    pub primary: bool,
    /// When this email was added.
    pub created_at: DateTime<Utc>,
    /// When this email was verified (if verified).
    pub verified_at: Option<DateTime<Utc>>,
}

/// Email verification token.
///
/// Tokens are stored with their hash, not plaintext, for security.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmailVerificationToken {
    /// Unique token identifier.
    pub id: i64,
    /// Email record this token is for.
    pub email_id: i64,
    /// SHA-256 hash of the token (not the plaintext token).
    pub token_hash: [u8; 32],
    /// When this token expires.
    pub expires_at: DateTime<Utc>,
    /// When this token was used (if used).
    pub used_at: Option<DateTime<Utc>>,
}

// ============================================================================
// Organization Routing
// ============================================================================

/// Organization routing table entry.
///
/// Maps an organization to its shard group for request routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationRegistry {
    /// Organization identifier.
    pub organization_id: OrganizationId,
    /// Human-readable organization name.
    pub name: String,
    /// Shard group hosting this organization.
    pub shard_id: ShardId,
    /// Nodes in the shard group.
    pub member_nodes: Vec<NodeId>,
    /// Current organization status.
    pub status: OrganizationStatus,
    /// Configuration version for cache invalidation.
    pub config_version: u64,
    /// When this organization was created.
    pub created_at: DateTime<Utc>,
}

/// Organization lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationStatus {
    /// Organization is active and accepting requests.
    #[default]
    Active,
    /// Organization is being migrated to another shard.
    Migrating,
    /// Organization is suspended (billing, policy, etc.).
    Suspended,
    /// Organization is being deleted.
    Deleting,
    /// Organization has been deleted (tombstone).
    Deleted,
}

// ============================================================================
// Cluster Membership
// ============================================================================

/// Cluster node information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique node identifier.
    pub node_id: NodeId,
    /// Node's WireGuard addresses.
    pub addresses: Vec<SocketAddr>,
    /// gRPC port for client connections.
    pub grpc_port: u16,
    /// Node's role in the cluster.
    pub role: NodeRole,
    /// Last heartbeat timestamp.
    pub last_heartbeat: DateTime<Utc>,
    /// When this node joined the cluster.
    pub joined_at: DateTime<Utc>,
}

/// Node role in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Voter: Participates in Raft elections (max 5 per cluster).
    Voter,
    /// Learner: Replicates data but doesn't vote (for scaling).
    #[default]
    Learner,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn test_user_status_default() {
        assert_eq!(UserStatus::default(), UserStatus::Active);
    }

    #[test]
    fn test_organization_status_default() {
        assert_eq!(OrganizationStatus::default(), OrganizationStatus::Active);
    }

    #[test]
    fn test_node_role_default() {
        assert_eq!(NodeRole::default(), NodeRole::Learner);
    }

    #[test]
    fn test_user_role_default() {
        assert_eq!(UserRole::default(), UserRole::User);
    }

    #[test]
    fn test_user_serialization() {
        let user = User {
            id: UserId::new(1),
            name: "Alice".to_string(),
            primary_email_id: 1,
            status: UserStatus::Active,
            role: UserRole::User,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let bytes = postcard::to_allocvec(&user).unwrap();
        let deserialized: User = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(user.id, deserialized.id);
        assert_eq!(user.name, deserialized.name);
        assert_eq!(deserialized.role, UserRole::User);
    }

    #[test]
    fn test_user_admin_serialization() {
        let user = User {
            id: UserId::new(2),
            name: "Bob".to_string(),
            primary_email_id: 2,
            status: UserStatus::Active,
            role: UserRole::Admin,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let bytes = postcard::to_allocvec(&user).unwrap();
        let deserialized: User = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.role, UserRole::Admin);
    }

    #[test]
    fn test_organization_registry_serialization() {
        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(1),
            name: "acme-corp".to_string(),
            shard_id: ShardId::new(1),
            member_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
        };

        let bytes = postcard::to_allocvec(&registry).unwrap();
        let deserialized: OrganizationRegistry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(registry.organization_id, deserialized.organization_id);
        assert_eq!(registry.name, deserialized.name);
    }
}
