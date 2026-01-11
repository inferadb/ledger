//! Data model types for the `_system` namespace.
//!
//! Per DESIGN.md lines 1860-1930.

use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use ledger_types::{NamespaceId, NodeId, ShardId, UserId};

// ============================================================================
// User Types
// ============================================================================

/// Global user account per DESIGN.md lines 1860-1875.
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

// ============================================================================
// Email Types
// ============================================================================

/// User email address per DESIGN.md lines 1877-1892.
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

/// Email verification token per DESIGN.md lines 1894-1905.
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
// Namespace Routing
// ============================================================================

/// Namespace routing table entry per DESIGN.md lines 1916-1922.
///
/// Maps a namespace to its shard group for request routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceRegistry {
    /// Namespace identifier.
    pub namespace_id: NamespaceId,
    /// Human-readable namespace name.
    pub name: String,
    /// Shard group hosting this namespace.
    pub shard_id: ShardId,
    /// Nodes in the shard group.
    pub member_nodes: Vec<NodeId>,
    /// Current namespace status.
    pub status: NamespaceStatus,
    /// Configuration version for cache invalidation.
    pub config_version: u64,
    /// When this namespace was created.
    pub created_at: DateTime<Utc>,
}

/// Namespace lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceStatus {
    /// Namespace is active and accepting requests.
    #[default]
    Active,
    /// Namespace is being migrated to another shard.
    Migrating,
    /// Namespace is suspended (billing, policy, etc.).
    Suspended,
    /// Namespace is being deleted.
    Deleting,
    /// Namespace has been deleted (tombstone).
    Deleted,
}

// ============================================================================
// Cluster Membership
// ============================================================================

/// Cluster node information per DESIGN.md lines 1924-1930.
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

/// Node role in the cluster per DESIGN.md lines 1970-1996.
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
    fn test_namespace_status_default() {
        assert_eq!(NamespaceStatus::default(), NamespaceStatus::Active);
    }

    #[test]
    fn test_node_role_default() {
        assert_eq!(NodeRole::default(), NodeRole::Learner);
    }

    #[test]
    fn test_user_serialization() {
        let user = User {
            id: 1,
            name: "Alice".to_string(),
            primary_email_id: 1,
            status: UserStatus::Active,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let bytes = bincode::serialize(&user).unwrap();
        let deserialized: User = bincode::deserialize(&bytes).unwrap();
        assert_eq!(user.id, deserialized.id);
        assert_eq!(user.name, deserialized.name);
    }

    #[test]
    fn test_namespace_registry_serialization() {
        let registry = NamespaceRegistry {
            namespace_id: 1,
            name: "acme-corp".to_string(),
            shard_id: 1,
            member_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            status: NamespaceStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
        };

        let bytes = bincode::serialize(&registry).unwrap();
        let deserialized: NamespaceRegistry = bincode::deserialize(&bytes).unwrap();
        assert_eq!(registry.namespace_id, deserialized.namespace_id);
        assert_eq!(registry.name, deserialized.name);
    }
}
