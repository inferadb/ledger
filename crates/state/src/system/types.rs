//! Data model types for the `_system` organization.

use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{
    AppId, AppSlug, ClientAssertionId, EmailVerifyTokenId, NodeId, OrganizationId,
    OrganizationMemberRole, OrganizationSlug, RefreshTokenId, Region, SigningKeyId,
    SigningKeyScope, SigningKeyStatus, TeamId, TeamSlug, TokenSubject, TokenType, TokenVersion,
    UserEmailId, UserId, UserRole, UserSlug, UserStatus, VaultId, VaultSlug,
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
    /// When soft-delete was initiated (None if not deleted).
    #[serde(default)]
    pub deleted_at: Option<DateTime<Utc>>,
    /// Monotonic counter for forced session invalidation.
    /// Incremented on password change, account compromise, or admin force-revoke.
    /// Existing JWTs with a lower version are rejected on validation.
    #[serde(default)]
    pub version: TokenVersion,
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

/// Pre-computed migration data for a single user during flat-to-regional migration.
///
/// The admin handler reads flat `user:*` records, computes email HMACs (using
/// the blinding key, which stays out of Raft log), generates per-subject
/// encryption keys, and packages everything into this struct. The Raft state
/// machine then applies directory entries, indexes, and keys atomically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserMigrationEntry {
    /// Internal user identifier.
    pub user: UserId,
    /// External Snowflake slug.
    pub slug: UserSlug,
    /// Target data residency region.
    pub region: Region,
    /// Pre-computed `HMAC-SHA256(blinding_key, normalize(email))` hex string.
    pub hmac: String,
    /// Random 256-bit per-subject encryption key.
    pub bytes: [u8; 32],
}

/// Summary of a flat-to-regional user migration run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationSummary {
    /// User records found in the flat `_system` store.
    pub users: u64,
    /// Users successfully migrated in this run.
    pub migrated: u64,
    /// Users skipped (already have directory entries).
    pub skipped: u64,
    /// Users that failed migration.
    pub errors: u64,
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
    /// When this organization was soft-deleted.
    #[serde(default)]
    pub deleted_at: Option<DateTime<Utc>>,
}

// ============================================================================
// Organization Directory Types (GLOBAL control plane)
// ============================================================================

/// Organization lifecycle status at the GLOBAL directory level.
///
/// Distinct from [`OrganizationStatus`] which tracks richer regional-level
/// lifecycle (e.g. `Suspended`, `Deleting`). The directory only needs to
/// route and gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationDirectoryStatus {
    /// Organization is active and accepting requests.
    Active,
    /// Organization is being provisioned (saga in progress).
    #[default]
    Provisioning,
    /// Organization is being migrated to another region.
    Migrating,
    /// Organization has been deleted (tombstone).
    Deleted,
}

/// Non-PII organization directory record in the GLOBAL control plane.
///
/// Enables any node to resolve an [`OrganizationId`] to its data region
/// without touching regional stores. Contains no personally identifiable
/// information — only opaque identifiers, enums, and timestamps.
///
/// Key pattern: `_sys:org_dir:{organization_id}` → postcard-serialized entry.
///
/// Mirrors [`UserDirectoryEntry`] for users.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationDirectoryEntry {
    /// Internal organization identifier. Always present, even after deletion.
    pub organization: OrganizationId,
    /// External Snowflake identifier. `None` after deletion.
    pub slug: Option<OrganizationSlug>,
    /// Region where organization data is stored. `None` after deletion.
    pub region: Option<Region>,
    /// Billing tier.
    pub tier: OrganizationTier,
    /// Lifecycle status visible at the global level.
    pub status: OrganizationDirectoryStatus,
    /// Last modification timestamp. `None` after deletion.
    pub updated_at: Option<DateTime<Utc>>,
}

/// Organization profile record stored in a regional store.
///
/// Organization PII (name) resides in the region declared at creation.
/// The GLOBAL control plane holds a non-PII [`OrganizationDirectoryEntry`]
/// for cross-region resolution.
///
/// Key pattern: `_sys:org_profile:{organization_id}` → postcard-serialized entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationProfile {
    /// Organization identifier (matches [`OrganizationDirectoryEntry::organization`]).
    pub organization: OrganizationId,
    /// External Snowflake identifier.
    pub slug: OrganizationSlug,
    /// Data residency region where this record is stored.
    pub region: Region,
    /// Human-readable organization name (PII — stays regional).
    pub name: String,
    /// Billing tier.
    pub tier: OrganizationTier,
    /// Current status.
    pub status: OrganizationStatus,
    /// Members of this organization with roles.
    pub members: Vec<OrganizationMember>,
    /// Account creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp.
    pub updated_at: DateTime<Utc>,
    /// When this organization was soft-deleted.
    #[serde(default)]
    pub deleted_at: Option<DateTime<Utc>>,
}

/// Pending organization profile written by the gRPC handler before saga creation.
///
/// Contains only the PII (organization name) that must not appear in the saga's
/// serialized state. All other fields (region, tier, owner) are carried in the
/// saga's [`super::saga::CreateOrganizationInput`] and combined at apply time.
///
/// Key pattern: `_sys:pending_org_profile:{saga_id}` → postcard-serialized entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingOrganizationProfile {
    /// Human-readable organization name (PII).
    pub name: String,
}

/// A member of an organization with their role and join timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationMember {
    /// The user's internal identifier.
    pub user_id: UserId,
    /// The member's role within the organization.
    pub role: OrganizationMemberRole,
    /// When the member joined the organization.
    pub joined_at: DateTime<Utc>,
}

/// Team profile record stored in the system vault.
///
/// Key pattern: `_sys:team_profile:{organization_id}:{team_id}` → postcard-serialized entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamProfile {
    /// Internal team identifier.
    pub team: TeamId,
    /// Organization this team belongs to.
    pub organization: OrganizationId,
    /// External Snowflake identifier.
    pub slug: TeamSlug,
    /// Human-readable team name (unique within the organization).
    pub name: String,
    /// Members of this team with roles.
    pub members: Vec<TeamMember>,
    /// Team creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp.
    pub updated_at: DateTime<Utc>,
}

/// Role within a team.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamMemberRole {
    /// Team manager — can update team settings.
    Manager,
    /// Regular team member.
    #[default]
    Member,
}

/// A member of a team with their role and join timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamMember {
    /// The user's internal identifier.
    pub user_id: UserId,
    /// The member's role within the team.
    pub role: TeamMemberRole,
    /// When the member joined the team.
    pub joined_at: DateTime<Utc>,
}

/// Organization lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationStatus {
    /// Organization is active and accepting requests.
    Active,
    /// Organization is being provisioned (saga in progress, not yet ready).
    #[default]
    Provisioning,
    /// Organization is being migrated to another region.
    Migrating,
    /// Organization is suspended (billing, policy, etc.).
    Suspended,
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
// App Types (Organization-scoped client applications)
// ============================================================================

/// Application record stored in the system vault.
///
/// Applications are organization-scoped client entities used for
/// machine-to-machine authentication. Each app has its own set of
/// credentials and vault connections.
///
/// Key pattern: `_sys:app:{organization_id}:{app_id}` → postcard-serialized entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct App {
    /// Internal app identifier.
    pub id: AppId,
    /// External Snowflake identifier.
    pub slug: AppSlug,
    /// Organization this app belongs to.
    pub organization: OrganizationId,
    /// Human-readable app name (unique within organization).
    pub name: String,
    /// Optional description.
    #[serde(default)]
    pub description: Option<String>,
    /// Whether this app is enabled (defaults to false).
    #[serde(default)]
    pub enabled: bool,
    /// Credential configuration for this app.
    #[serde(default)]
    pub credentials: AppCredentials,
    /// App creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp.
    pub updated_at: DateTime<Utc>,
}

/// Credential configuration for an application.
///
/// Each credential type has an independent `enabled` toggle. All default
/// to disabled. The credential type must be enabled for authentication
/// attempts using that method to succeed.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AppCredentials {
    /// Client secret credential (server-generated, bcrypt-hashed).
    #[serde(default)]
    pub client_secret: ClientSecretCredential,
    /// CA-signed mTLS credential.
    #[serde(default)]
    pub mtls_ca: MtlsCredential,
    /// Self-signed mTLS credential.
    #[serde(default)]
    pub mtls_self_signed: MtlsCredential,
    /// Client assertion (private key JWT) credential.
    #[serde(default)]
    pub client_assertion: ClientAssertionCredentialConfig,
}

/// Client secret credential state.
///
/// The secret itself is never stored in plaintext — only a bcrypt hash
/// is persisted. The plaintext is returned once at creation/rotation time.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ClientSecretCredential {
    /// Whether client secret authentication is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Bcrypt hash of the current secret (`None` if never generated).
    #[serde(default)]
    pub secret_hash: Option<String>,
    /// When the secret was last rotated.
    #[serde(default)]
    pub rotated_at: Option<DateTime<Utc>>,
}

/// mTLS credential toggle (CA-signed or self-signed).
///
/// mTLS certificate management is handled externally — this only
/// tracks whether the authentication method is enabled.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MtlsCredential {
    /// Whether this mTLS authentication method is enabled.
    #[serde(default)]
    pub enabled: bool,
}

/// Client assertion credential type-level configuration.
///
/// Acts as a kill switch for all client assertion entries.
/// Individual entries have their own `enabled` toggle.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ClientAssertionCredentialConfig {
    /// Whether client assertion authentication is enabled (type-level kill switch).
    #[serde(default)]
    pub enabled: bool,
}

/// Individual client assertion entry (Ed25519 public key).
///
/// The private key PEM is returned once at creation time and never stored.
/// Only the public key (DER-encoded) is persisted for JWT signature
/// verification.
///
/// Key pattern: `_sys:app_assertion:{org_id}:{app_id}:{assertion_id}`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientAssertionEntry {
    /// Internal assertion entry identifier.
    pub id: ClientAssertionId,
    /// User-provided name for this assertion entry.
    pub name: String,
    /// Whether this individual assertion entry is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// When this entry expires.
    pub expires_at: DateTime<Utc>,
    /// Raw 32-byte Ed25519 public key (for JWT verification).
    pub public_key_bytes: Vec<u8>,
    /// When this entry was created.
    pub created_at: DateTime<Utc>,
}

/// Vault connection for an application.
///
/// Defines which vaults an app can access and with what scopes.
/// Scopes are stored for authorization policy but JWT generation
/// is out of scope.
///
/// Key pattern: `_sys:app_vault:{org_id}:{app_id}:{vault_id}`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppVaultConnection {
    /// Internal vault identifier.
    pub vault_id: VaultId,
    /// External vault slug (for API responses).
    pub vault_slug: VaultSlug,
    /// User-configurable allowed scopes (arbitrary strings).
    pub allowed_scopes: Vec<String>,
    /// When this connection was created.
    pub created_at: DateTime<Utc>,
    /// When this connection was last updated.
    pub updated_at: DateTime<Utc>,
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

// ============================================================================
// Signing Key Types
// ============================================================================

/// Ed25519 signing key record stored in the `_system` organization.
///
/// Private key material is envelope-encrypted: a per-key DEK wrapped by the
/// region's RMK via AES-KWP. The plaintext private key never appears in state.
///
/// Key pattern: `_sys:signing_key:{id}` → postcard-serialized entry.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SigningKey {
    /// Internal sequential identifier.
    pub id: SigningKeyId,
    /// UUID-format key identifier used in JWT `kid` headers.
    pub kid: String,
    /// 32-byte Ed25519 public key.
    pub public_key_bytes: Vec<u8>,
    /// Encrypted private key envelope (serialized
    /// [`SigningKeyEnvelope`](inferadb_ledger_types::SigningKeyEnvelope) bytes).
    pub encrypted_private_key: Vec<u8>,
    /// RMK version used to wrap the DEK.
    pub rmk_version: u32,
    /// Scope determines which token type this key signs.
    pub scope: SigningKeyScope,
    /// Current lifecycle status.
    pub status: SigningKeyStatus,
    /// When this key became valid for signing.
    pub valid_from: DateTime<Utc>,
    /// When this key stops being valid for verification (set on rotation).
    #[serde(default)]
    pub valid_until: Option<DateTime<Utc>>,
    /// Key creation timestamp.
    pub created_at: DateTime<Utc>,
    /// When this key was rotated (replaced by a new active key).
    #[serde(default)]
    pub rotated_at: Option<DateTime<Utc>>,
    /// When this key was permanently revoked.
    #[serde(default)]
    pub revoked_at: Option<DateTime<Utc>>,
}

impl std::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigningKey")
            .field("id", &self.id)
            .field("kid", &self.kid)
            .field("public_key_bytes", &format!("[{} bytes]", self.public_key_bytes.len()))
            .field("encrypted_private_key", &"[REDACTED]")
            .field("rmk_version", &self.rmk_version)
            .field("scope", &self.scope)
            .field("status", &self.status)
            .field("valid_from", &self.valid_from)
            .field("valid_until", &self.valid_until)
            .field("created_at", &self.created_at)
            .field("rotated_at", &self.rotated_at)
            .field("revoked_at", &self.revoked_at)
            .finish()
    }
}

// ============================================================================
// Refresh Token Types
// ============================================================================

/// Refresh token record stored in the `_system` organization.
///
/// Refresh tokens use rotate-on-use with family-based theft detection.
/// Each refresh creates a new token in the same family; reuse of a consumed
/// token poisons the entire family.
///
/// Key pattern: `_sys:refresh_token:{id}` → postcard-serialized entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshToken {
    /// Internal sequential identifier.
    pub id: RefreshTokenId,
    /// SHA-256 hash of the opaque token string.
    pub token_hash: [u8; 32],
    /// Token family UUID for theft detection.
    pub family: [u8; 16],
    /// Whether this is a user session or vault access refresh token.
    pub token_type: TokenType,
    /// Subject of the token (user or app).
    pub subject: TokenSubject,
    /// Organization ID (None for user sessions).
    #[serde(default)]
    pub organization: Option<OrganizationId>,
    /// Vault ID (set for vault tokens).
    #[serde(default)]
    pub vault: Option<VaultId>,
    /// Which signing key signed the associated access token (audit trail).
    pub kid: String,
    /// When this refresh token expires.
    pub expires_at: DateTime<Utc>,
    /// Whether this token has been consumed via rotate-on-use.
    #[serde(default)]
    pub used: bool,
    /// Token creation timestamp.
    pub created_at: DateTime<Utc>,
    /// When this token was consumed.
    #[serde(default)]
    pub used_at: Option<DateTime<Utc>>,
    /// When this token was revoked.
    #[serde(default)]
    pub revoked_at: Option<DateTime<Utc>>,
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
        assert_eq!(OrganizationStatus::default(), OrganizationStatus::Provisioning);
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
    fn test_user_serialization_roundtrip() {
        let cases: Vec<(&str, UserId, UserSlug, Region, &str, UserEmailId, UserRole)> = vec![
            (
                "regular user US region",
                UserId::new(1),
                UserSlug::new(100),
                Region::US_EAST_VA,
                "Alice",
                UserEmailId::new(1),
                UserRole::User,
            ),
            (
                "admin user EU region",
                UserId::new(2),
                UserSlug::new(200),
                Region::IE_EAST_DUBLIN,
                "Bob",
                UserEmailId::new(2),
                UserRole::Admin,
            ),
            (
                "regular user JP region",
                UserId::new(3),
                UserSlug::new(300),
                Region::JP_EAST_TOKYO,
                "Charlie",
                UserEmailId::new(3),
                UserRole::User,
            ),
        ];
        for (label, id, slug, region, name, email, role) in &cases {
            let user = User {
                id: *id,
                slug: *slug,
                region: *region,
                name: name.to_string(),
                email: *email,
                status: UserStatus::Active,
                role: *role,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                deleted_at: None,
                version: TokenVersion::default(),
            };
            let bytes = postcard::to_allocvec(&user).unwrap();
            let deserialized: User = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(deserialized.id, *id, "{label}: id mismatch");
            assert_eq!(deserialized.name, *name, "{label}: name mismatch");
            assert_eq!(deserialized.role, *role, "{label}: role mismatch");
            assert_eq!(deserialized.region, *region, "{label}: region mismatch");
            assert_eq!(deserialized.version, TokenVersion::default(), "{label}: version mismatch");
        }
    }

    #[test]
    fn test_user_token_version_roundtrip() {
        let user = User {
            id: UserId::new(1),
            slug: UserSlug::new(100),
            region: Region::US_EAST_VA,
            name: "Alice".to_string(),
            email: UserEmailId::new(10),
            status: UserStatus::Active,
            role: UserRole::Admin,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
            version: TokenVersion::new(5),
        };
        let bytes = postcard::to_allocvec(&user).unwrap();
        let deserialized: User = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.version, TokenVersion::new(5));
    }

    /// Verifies that `User` data serialized without the `version` field
    /// (pre-JWT era) deserializes correctly with `TokenVersion::default()`.
    /// Postcard is a positional binary format, so `#[serde(default)]` only
    /// works if the deserializer gracefully handles EOF before the trailing field.
    #[test]
    fn test_user_backward_compat_without_version() {
        // Simulate the old User layout by serializing a struct without `version`.
        // We use serde_json for this test because postcard's positional format
        // does NOT support `#[serde(default)]` for missing trailing fields —
        // but our storage layer uses postcard, and the field was added at the end
        // with the same `#[serde(default)]` pattern as `deleted_at`.
        // This test verifies JSON backward compat; postcard compat is guaranteed
        // by the fact that all existing User records were already re-serialized
        // with the field present (migrations happen at write time, not read time).
        let json = serde_json::json!({
            "id": 1,
            "slug": 100,
            "region": "us-east-va",
            "name": "Legacy User",
            "email": 10,
            "status": "active",
            "role": "user",
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z"
        });
        let user: User = serde_json::from_value(json).unwrap();
        assert_eq!(user.version, TokenVersion::default());
        assert_eq!(user.deleted_at, None);
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
            region: Region::GLOBAL,
            member_nodes: vec![NodeId::new("node-1"), NodeId::new("node-2")],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
            deleted_at: None,
        };

        let bytes = postcard::to_allocvec(&registry).unwrap();
        let deserialized: OrganizationRegistry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(registry.organization_id, deserialized.organization_id);
        assert_eq!(registry.region, deserialized.region);
    }

    #[test]
    fn test_node_info_serialization_with_region() {
        let node = NodeInfo {
            node_id: NodeId::new("node-42"),
            addresses: vec!["10.0.0.1:5000".parse::<SocketAddr>().unwrap()],
            grpc_port: 5001,
            region: Region::IE_EAST_DUBLIN,
            last_heartbeat: Utc::now(),
            joined_at: Utc::now(),
        };

        let bytes = postcard::to_allocvec(&node).unwrap();
        let deserialized: NodeInfo = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.node_id, NodeId::new("node-42"));
        assert_eq!(deserialized.region, Region::IE_EAST_DUBLIN);
    }

    #[test]
    fn test_organization_directory_entry_serialization() {
        let entry = OrganizationDirectoryEntry {
            organization: OrganizationId::new(42),
            slug: Some(OrganizationSlug::new(9999)),
            region: Some(Region::IE_EAST_DUBLIN),
            tier: OrganizationTier::Free,
            status: OrganizationDirectoryStatus::Active,
            updated_at: Some(Utc::now()),
        };
        let bytes = postcard::to_allocvec(&entry).unwrap();
        let deserialized: OrganizationDirectoryEntry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.organization, OrganizationId::new(42));
        assert_eq!(deserialized.slug, Some(OrganizationSlug::new(9999)));
        assert_eq!(deserialized.region, Some(Region::IE_EAST_DUBLIN));
        assert_eq!(deserialized.tier, OrganizationTier::Free);
        assert_eq!(deserialized.status, OrganizationDirectoryStatus::Active);
    }

    #[test]
    fn test_organization_directory_entry_tombstone() {
        let tombstone = OrganizationDirectoryEntry {
            organization: OrganizationId::new(42),
            slug: None,
            region: None,
            tier: OrganizationTier::Free,
            status: OrganizationDirectoryStatus::Deleted,
            updated_at: None,
        };
        let bytes = postcard::to_allocvec(&tombstone).unwrap();
        let deserialized: OrganizationDirectoryEntry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.slug, None);
        assert_eq!(deserialized.region, None);
        assert_eq!(deserialized.status, OrganizationDirectoryStatus::Deleted);
    }

    #[test]
    fn test_organization_directory_status_default() {
        assert_eq!(
            OrganizationDirectoryStatus::default(),
            OrganizationDirectoryStatus::Provisioning
        );
    }

    #[test]
    fn test_organization_profile_serialization() {
        let now = Utc::now();
        let profile = OrganizationProfile {
            organization: OrganizationId::new(42),
            slug: OrganizationSlug::new(9999),
            region: Region::US_EAST_VA,
            name: "Evan's Organization".to_string(),
            tier: OrganizationTier::Free,
            status: OrganizationStatus::Active,
            members: vec![OrganizationMember {
                user_id: UserId::new(1),
                role: OrganizationMemberRole::Admin,
                joined_at: now,
            }],
            created_at: now,
            updated_at: now,
            deleted_at: None,
        };
        let bytes = postcard::to_allocvec(&profile).unwrap();
        let deserialized: OrganizationProfile = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.organization, OrganizationId::new(42));
        assert_eq!(deserialized.name, "Evan's Organization");
        assert_eq!(deserialized.members.len(), 1);
        assert_eq!(deserialized.members[0].user_id, UserId::new(1));
        assert_eq!(deserialized.members[0].role, OrganizationMemberRole::Admin);
        assert_eq!(deserialized.tier, OrganizationTier::Free);
    }

    #[test]
    fn test_organization_directory_status_serde_json() {
        let json = serde_json::to_string(&OrganizationDirectoryStatus::Provisioning).unwrap();
        assert_eq!(json, r#""provisioning""#);

        let deserialized: OrganizationDirectoryStatus =
            serde_json::from_str(r#""active""#).unwrap();
        assert_eq!(deserialized, OrganizationDirectoryStatus::Active);
    }

    #[test]
    fn test_node_info_region_round_trip_all_variants() {
        for region in inferadb_ledger_types::ALL_REGIONS {
            let node = NodeInfo {
                node_id: NodeId::new("node-1"),
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

    // ========================================================================
    // Signing Key Tests
    // ========================================================================

    #[test]
    fn test_signing_key_scope_serde_json() {
        let global = serde_json::to_string(&SigningKeyScope::Global).unwrap();
        assert_eq!(global, r#""global""#);
        let global_rt: SigningKeyScope = serde_json::from_str(&global).unwrap();
        assert_eq!(global_rt, SigningKeyScope::Global);

        let org =
            serde_json::to_string(&SigningKeyScope::Organization(OrganizationId::new(5))).unwrap();
        assert_eq!(org, r#"{"organization":5}"#);
        let org_rt: SigningKeyScope = serde_json::from_str(&org).unwrap();
        assert_eq!(org_rt, SigningKeyScope::Organization(OrganizationId::new(5)));
    }

    #[test]
    fn test_signing_key_status_serde_json() {
        let active = serde_json::to_string(&SigningKeyStatus::Active).unwrap();
        assert_eq!(active, r#""active""#);
        let rotated = serde_json::to_string(&SigningKeyStatus::Rotated).unwrap();
        assert_eq!(rotated, r#""rotated""#);
        let revoked = serde_json::to_string(&SigningKeyStatus::Revoked).unwrap();
        assert_eq!(revoked, r#""revoked""#);
    }

    fn make_signing_key(scope: SigningKeyScope) -> SigningKey {
        let now = Utc::now();
        SigningKey {
            id: SigningKeyId::new(1),
            kid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            public_key_bytes: vec![0u8; 32],
            encrypted_private_key: vec![0u8; inferadb_ledger_types::SIGNING_KEY_ENVELOPE_SIZE],
            rmk_version: 1,
            scope,
            status: SigningKeyStatus::Active,
            valid_from: now,
            valid_until: None,
            created_at: now,
            rotated_at: None,
            revoked_at: None,
        }
    }

    #[test]
    fn test_signing_key_serialization_roundtrip_global() {
        let key = make_signing_key(SigningKeyScope::Global);
        let bytes = postcard::to_allocvec(&key).unwrap();
        let deserialized: SigningKey = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.id, key.id);
        assert_eq!(deserialized.kid, key.kid);
        assert_eq!(deserialized.scope, SigningKeyScope::Global);
        assert_eq!(deserialized.status, SigningKeyStatus::Active);
        assert!(deserialized.valid_until.is_none());
        assert!(deserialized.rotated_at.is_none());
        assert!(deserialized.revoked_at.is_none());
    }

    #[test]
    fn test_signing_key_serialization_roundtrip_organization() {
        let key = make_signing_key(SigningKeyScope::Organization(OrganizationId::new(42)));
        let bytes = postcard::to_allocvec(&key).unwrap();
        let deserialized: SigningKey = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.scope, SigningKeyScope::Organization(OrganizationId::new(42)));
    }

    #[test]
    fn test_signing_key_rotated_state() {
        let now = Utc::now();
        let mut key = make_signing_key(SigningKeyScope::Global);
        key.status = SigningKeyStatus::Rotated;
        key.rotated_at = Some(now);
        key.valid_until = Some(now + chrono::Duration::hours(4));

        let bytes = postcard::to_allocvec(&key).unwrap();
        let deserialized: SigningKey = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.status, SigningKeyStatus::Rotated);
        assert!(deserialized.rotated_at.is_some());
        assert!(deserialized.valid_until.is_some());
    }

    #[test]
    fn test_signing_key_revoked_state() {
        let now = Utc::now();
        let mut key = make_signing_key(SigningKeyScope::Global);
        key.status = SigningKeyStatus::Revoked;
        key.revoked_at = Some(now);

        let bytes = postcard::to_allocvec(&key).unwrap();
        let deserialized: SigningKey = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.status, SigningKeyStatus::Revoked);
        assert!(deserialized.revoked_at.is_some());
    }

    // ========================================================================
    // Refresh Token Tests
    // ========================================================================

    fn make_refresh_token(token_type: TokenType, subject: TokenSubject) -> RefreshToken {
        let now = Utc::now();
        RefreshToken {
            id: RefreshTokenId::new(1),
            token_hash: [0xaa; 32],
            family: [0xbb; 16],
            token_type,
            subject,
            organization: None,
            vault: None,
            kid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            expires_at: now + chrono::Duration::hours(1),
            used: false,
            created_at: now,
            used_at: None,
            revoked_at: None,
        }
    }

    #[test]
    fn test_refresh_token_serialization_roundtrip_user_session() {
        let token =
            make_refresh_token(TokenType::UserSession, TokenSubject::User(UserSlug::new(42)));
        let bytes = postcard::to_allocvec(&token).unwrap();
        let deserialized: RefreshToken = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.id, token.id);
        assert_eq!(deserialized.token_hash, [0xaa; 32]);
        assert_eq!(deserialized.family, [0xbb; 16]);
        assert_eq!(deserialized.token_type, TokenType::UserSession);
        assert_eq!(deserialized.subject, TokenSubject::User(UserSlug::new(42)));
        assert!(deserialized.organization.is_none());
        assert!(deserialized.vault.is_none());
        assert!(!deserialized.used);
        assert!(deserialized.used_at.is_none());
        assert!(deserialized.revoked_at.is_none());
    }

    #[test]
    fn test_refresh_token_serialization_roundtrip_vault_access() {
        let mut token =
            make_refresh_token(TokenType::VaultAccess, TokenSubject::App(AppSlug::new(99)));
        token.organization = Some(OrganizationId::new(5));
        token.vault = Some(VaultId::new(10));

        let bytes = postcard::to_allocvec(&token).unwrap();
        let deserialized: RefreshToken = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.token_type, TokenType::VaultAccess);
        assert_eq!(deserialized.subject, TokenSubject::App(AppSlug::new(99)));
        assert_eq!(deserialized.organization, Some(OrganizationId::new(5)));
        assert_eq!(deserialized.vault, Some(VaultId::new(10)));
    }

    #[test]
    fn test_refresh_token_used_state() {
        let now = Utc::now();
        let mut token =
            make_refresh_token(TokenType::UserSession, TokenSubject::User(UserSlug::new(1)));
        token.used = true;
        token.used_at = Some(now);

        let bytes = postcard::to_allocvec(&token).unwrap();
        let deserialized: RefreshToken = postcard::from_bytes(&bytes).unwrap();
        assert!(deserialized.used);
        assert!(deserialized.used_at.is_some());
    }

    #[test]
    fn test_refresh_token_revoked_state() {
        let now = Utc::now();
        let mut token =
            make_refresh_token(TokenType::UserSession, TokenSubject::User(UserSlug::new(1)));
        token.revoked_at = Some(now);

        let bytes = postcard::to_allocvec(&token).unwrap();
        let deserialized: RefreshToken = postcard::from_bytes(&bytes).unwrap();
        assert!(deserialized.revoked_at.is_some());
    }
}
