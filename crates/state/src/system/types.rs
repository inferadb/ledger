//! Data model types for the `_system` organization.

use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{
    EmailVerifyTokenId, NodeId, OrganizationId, Region, UserEmailId, UserId, UserSlug,
};
use serde::{Deserialize, Serialize};

// ============================================================================
// User Types
// ============================================================================

/// User account record stored in a regional store.
///
/// User PII (name, email) resides in the region declared at registration.
/// The GLOBAL control plane holds a non-PII [`UserDirectoryEntry`] for
/// cross-region resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// Unique user identifier (globally unique).
    pub id: UserId,
    /// External Snowflake identifier for API consumers.
    pub slug: UserSlug,
    /// Data residency region where this record is stored.
    pub region: Region,
    /// User's display name.
    pub name: String,
    /// ID of the user's primary email address.
    pub email: UserEmailId,
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
/// Users can have multiple email addresses. The primary email is whichever
/// email the [`User::email`] field references. Verification status is derived
/// from `verified_at` — if present, the email is verified.
///
/// Global email uniqueness is enforced via the `_idx:email:{email}` index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserEmail {
    /// Unique email record identifier.
    pub id: UserEmailId,
    /// User who owns this email.
    pub user: UserId,
    /// Email address (lowercase normalized).
    pub email: String,
    /// When this email was added.
    pub created_at: DateTime<Utc>,
    /// When this email was verified (`None` if unverified).
    pub verified_at: Option<DateTime<Utc>>,
}

/// Email verification token.
///
/// Tokens are stored with their hash, not plaintext, for security.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmailVerificationToken {
    /// Unique token identifier.
    pub id: EmailVerifyTokenId,
    /// Email record this token is for.
    pub email_id: UserEmailId,
    /// SHA-256 hash of the token (not the plaintext token).
    pub token_hash: [u8; 32],
    /// When this token expires.
    pub expires_at: DateTime<Utc>,
    /// When this token was used (if used).
    pub used_at: Option<DateTime<Utc>>,
}

// ============================================================================
// Subject Key Types (per-user encryption for crypto-shredding)
// ============================================================================

/// Per-subject encryption key for GDPR Article 17 crypto-shredding.
///
/// Each user's PII is encrypted with a unique subject key. To exercise right
/// to erasure, destroy the subject key — encrypted PII in Raft log and
/// snapshots becomes cryptographically unrecoverable.
///
/// Subject keys are the sole exception to the "no key material in Raft"
/// principle. Unlike infrastructure keys (RMKs), subject keys are
/// application-level data stored inside Ledger's regional stores, encrypted
/// at rest under the region's RMK (via `EncryptedBackend`). This is
/// intentional: the key must be destroyable via a single Raft write.
///
/// Key pattern: `_key:user:{user_id}` in the regional store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubjectKey {
    /// User this key belongs to.
    pub user_id: UserId,
    /// 256-bit AES key material (encrypted at rest by EncryptedBackend).
    pub key: [u8; 32],
    /// When this key was generated.
    pub created_at: DateTime<Utc>,
}

/// Non-PII audit record for user erasure (GDPR Article 17(2) accountability).
///
/// Stored in the GLOBAL control plane. Retains only opaque identifiers and
/// metadata required for regulatory compliance. The `region` field is
/// intentionally retained despite being cleared from the directory tombstone —
/// demonstrating in which jurisdiction erasure occurred takes precedence over
/// metadata minimization for audit records.
///
/// Key pattern: `_audit:erasure:{user_id}` in system vault.
/// Uses insert-if-absent for idempotent crash-resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureAuditRecord {
    /// User that was erased.
    pub user_id: UserId,
    /// When erasure was performed.
    pub erased_at: DateTime<Utc>,
    /// Principal (admin user or system) who initiated the erasure.
    pub erased_by: String,
    /// Region where the user's PII was stored at time of erasure.
    pub region: Region,
}

// ============================================================================
// User Directory Types (GLOBAL control plane)
// ============================================================================

/// Lifecycle status for a user directory entry in the GLOBAL control plane.
///
/// Distinct from [`UserStatus`] which tracks richer regional-level lifecycle.
/// The directory only needs to route and gate:
/// - `Active` → PII in declared region
/// - `Migrating` → PII being moved between regions (Task 15)
/// - `Deleted` → user erased via crypto-shredding (Task 20)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserDirectoryStatus {
    /// User is active — PII in declared region.
    #[default]
    Active,
    /// User is migrating between regions.
    Migrating,
    /// User has been erased. Permanent tombstone.
    Deleted,
}

/// Non-PII user directory record in the GLOBAL control plane.
///
/// Enables any node to resolve a [`UserId`] to its data region without
/// touching regional stores. Contains no personally identifiable information
/// — only opaque identifiers, enums, and timestamps.
///
/// Key pattern: `_sys:user:{user_id}` → postcard-serialized entry.
///
/// Optional fields are set to `None` after erasure (tombstone minimization):
/// only `user` and `status = Deleted` survive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserDirectoryEntry {
    /// Internal user identifier (globally unique). Always present, even after erasure.
    pub user: UserId,
    /// External Snowflake identifier. `None` after erasure.
    pub slug: Option<UserSlug>,
    /// Region where user's PII is stored. `None` after erasure.
    pub region: Option<Region>,
    /// Lifecycle status visible at the global level.
    pub status: UserDirectoryStatus,
    /// Last modification timestamp. `None` after erasure.
    pub updated_at: Option<DateTime<Utc>>,
}

// ============================================================================
// Organization Routing
// ============================================================================

/// Organization routing table entry.
///
/// Maps an organization to its region for request routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationRegistry {
    /// Organization identifier.
    pub organization_id: OrganizationId,
    /// Human-readable organization name.
    pub name: String,
    /// Region hosting this organization.
    pub region: Region,
    /// Nodes in the region group.
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
    /// Organization is being migrated to another region.
    Migrating,
    /// Organization is suspended (billing, policy, etc.).
    Suspended,
    /// Organization is being deleted.
    Deleting,
    /// Organization has been deleted (tombstone).
    Deleted,
}

/// Organization billing tier.
///
/// Determines runtime behavior such as quota presets and feature gating.
/// Billing details (Stripe, payment info) are managed externally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationTier {
    /// Free tier (default for new organizations).
    #[default]
    Free,
    /// Professional tier with higher limits.
    Pro,
    /// Enterprise tier with custom limits and SLA.
    Enterprise,
}

// ============================================================================
// Cluster Membership
// ============================================================================

/// Cluster node information.
///
/// Physical node properties only — no global role. A node's voter/learner status
/// is per-Raft-group, tracked by [`GroupMembership`](super::cluster::GroupMembership).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique node identifier.
    pub node_id: NodeId,
    /// Node's WireGuard addresses.
    pub addresses: Vec<SocketAddr>,
    /// gRPC port for client connections.
    pub grpc_port: u16,
    /// Geographic region this node belongs to.
    ///
    /// Determines which Raft groups the node participates in:
    /// - `GLOBAL`: all nodes join (control plane, replicated everywhere).
    /// - Non-protected (`requires_residency() == false`): all nodes join.
    /// - Protected (`requires_residency() == true`): only nodes tagged with that exact region
    ///   join.
    ///
    /// Region is immutable after registration — moving requires decommission
    /// and re-register.
    pub region: Region,
    /// Last heartbeat timestamp.
    pub last_heartbeat: DateTime<Utc>,
    /// When this node joined the cluster.
    pub joined_at: DateTime<Utc>,
}

/// Node role within a specific Raft group.
///
/// A node's role is per-group, not global: the same node can be a Voter in
/// `GLOBAL`, a Learner in `US_EAST_VA`, and not a member of `CN_NORTH_BEIJING`.
/// Tracked by [`GroupMembership`](super::cluster::GroupMembership).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Voter: Participates in Raft elections (max 5 per group).
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
    fn test_organization_tier_default() {
        assert_eq!(OrganizationTier::default(), OrganizationTier::Free);
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
            slug: UserSlug::new(100),
            region: Region::US_EAST_VA,
            name: "Alice".to_string(),
            email: UserEmailId::new(1),
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
            slug: UserSlug::new(200),
            region: Region::IE_EAST_DUBLIN,
            name: "Bob".to_string(),
            email: UserEmailId::new(2),
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
    fn test_user_region_field_serialization() {
        let user = User {
            id: UserId::new(3),
            slug: UserSlug::new(300),
            region: Region::JP_EAST_TOKYO,
            name: "Charlie".to_string(),
            email: UserEmailId::new(3),
            status: UserStatus::Active,
            role: UserRole::User,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let bytes = postcard::to_allocvec(&user).unwrap();
        let deserialized: User = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.region, Region::JP_EAST_TOKYO);
        assert_eq!(deserialized.name, "Charlie");
    }

    #[test]
    fn test_user_directory_status_default() {
        assert_eq!(UserDirectoryStatus::default(), UserDirectoryStatus::Active);
    }

    #[test]
    fn test_user_directory_entry_serialization() {
        let entry = UserDirectoryEntry {
            user: UserId::new(42),
            slug: Some(UserSlug::new(9999)),
            region: Some(Region::IE_EAST_DUBLIN),
            status: UserDirectoryStatus::Active,
            updated_at: Some(Utc::now()),
        };

        let bytes = postcard::to_allocvec(&entry).unwrap();
        let deserialized: UserDirectoryEntry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.user, UserId::new(42));
        assert_eq!(deserialized.slug, Some(UserSlug::new(9999)));
        assert_eq!(deserialized.region, Some(Region::IE_EAST_DUBLIN));
        assert_eq!(deserialized.status, UserDirectoryStatus::Active);
    }

    #[test]
    fn test_user_directory_entry_tombstone() {
        // After erasure, optional fields are None
        let tombstone = UserDirectoryEntry {
            user: UserId::new(42),
            slug: None,
            region: None,
            status: UserDirectoryStatus::Deleted,
            updated_at: None,
        };

        let bytes = postcard::to_allocvec(&tombstone).unwrap();
        let deserialized: UserDirectoryEntry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.user, UserId::new(42));
        assert_eq!(deserialized.slug, None);
        assert_eq!(deserialized.region, None);
        assert_eq!(deserialized.status, UserDirectoryStatus::Deleted);
        assert_eq!(deserialized.updated_at, None);
    }

    #[test]
    fn test_user_directory_status_serde_json() {
        // Verify snake_case rename
        let json = serde_json::to_string(&UserDirectoryStatus::Active).unwrap();
        assert_eq!(json, r#""active""#);

        let json = serde_json::to_string(&UserDirectoryStatus::Migrating).unwrap();
        assert_eq!(json, r#""migrating""#);

        let json = serde_json::to_string(&UserDirectoryStatus::Deleted).unwrap();
        assert_eq!(json, r#""deleted""#);
    }

    #[test]
    fn test_organization_registry_serialization() {
        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(1),
            name: "acme-corp".to_string(),
            region: Region::GLOBAL,
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

    #[test]
    fn test_node_info_serialization_with_region() {
        let node = NodeInfo {
            node_id: "node-42".to_string(),
            addresses: vec!["10.0.0.1:5000".parse::<SocketAddr>().unwrap()],
            grpc_port: 5001,
            region: Region::IE_EAST_DUBLIN,
            last_heartbeat: Utc::now(),
            joined_at: Utc::now(),
        };

        let bytes = postcard::to_allocvec(&node).unwrap();
        let deserialized: NodeInfo = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.node_id, "node-42");
        assert_eq!(deserialized.region, Region::IE_EAST_DUBLIN);
    }

    #[test]
    fn test_node_info_region_round_trip_all_variants() {
        for region in inferadb_ledger_types::ALL_REGIONS {
            let node = NodeInfo {
                node_id: "node-1".to_string(),
                addresses: vec!["10.0.0.1:5000".parse::<SocketAddr>().unwrap()],
                grpc_port: 5001,
                region,
                last_heartbeat: Utc::now(),
                joined_at: Utc::now(),
            };

            let bytes = postcard::to_allocvec(&node).unwrap();
            let deserialized: NodeInfo = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(deserialized.region, region, "Region round-trip failed for {region}");
        }
    }
}
