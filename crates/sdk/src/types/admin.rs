//! Administrative domain types: organizations, vaults, users, teams, health.

use std::fmt;

use inferadb_ledger_types::{
    OrganizationSlug, Region, TeamSlug, UserEmailId, UserRole, UserSlug, UserStatus, VaultSlug,
};

/// Status of an organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum OrganizationStatus {
    /// Status is unknown or unspecified.
    #[default]
    Unspecified,
    /// Organization is active and operational.
    Active,
    /// Organization is being provisioned (saga in progress).
    Provisioning,
    /// Organization is being migrated to another region.
    Migrating,
    /// Organization is suspended (billing or policy).
    Suspended,
    /// Organization has been deleted.
    Deleted,
}

impl OrganizationStatus {
    /// Returns the variant as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Active => "active",
            Self::Provisioning => "provisioning",
            Self::Migrating => "migrating",
            Self::Suspended => "suspended",
            Self::Deleted => "deleted",
        }
    }
}

impl fmt::Display for OrganizationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Billing tier for an organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum OrganizationTier {
    /// Free tier with basic features.
    #[default]
    Free,
    /// Launch tier with enhanced features.
    Launch,
    /// Scale tier with full features.
    Scale,
}

impl OrganizationTier {
    /// Returns the variant as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Free => "free",
            Self::Launch => "launch",
            Self::Scale => "scale",
        }
    }
}

impl fmt::Display for OrganizationTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Status of a vault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum VaultStatus {
    /// Status is unknown or unspecified.
    #[default]
    Unspecified,
    /// Vault is active and operational.
    Active,
    /// Vault is read-only (no writes allowed).
    ReadOnly,
    /// Vault has been deleted.
    Deleted,
}

impl VaultStatus {
    /// Returns the variant as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Active => "active",
            Self::ReadOnly => "read_only",
            Self::Deleted => "deleted",
        }
    }
}

impl fmt::Display for VaultStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Information about an in-progress organization migration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MigrationInfo {
    /// Organization slug (Snowflake ID).
    pub slug: OrganizationSlug,
    /// Source region for the migration.
    pub source_region: Region,
    /// Target region for the migration.
    pub target_region: Region,
    /// Current organization status (should be `Migrating`).
    pub status: OrganizationStatus,
}

/// SDK representation of a user record.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserInfo {
    /// External Snowflake slug.
    pub slug: UserSlug,
    /// Display name.
    pub name: String,
    /// Primary email.
    pub email: UserEmailId,
    /// Current status.
    pub status: UserStatus,
    /// Authorization role.
    pub role: UserRole,
    /// When the user was created.
    pub created_at: Option<std::time::SystemTime>,
    /// When the user was last updated.
    pub updated_at: Option<std::time::SystemTime>,
    /// When the user was soft-deleted (if deleted).
    pub deleted_at: Option<std::time::SystemTime>,
    /// Retention period in days (populated on delete responses).
    pub retention_days: Option<u32>,
}

/// SDK representation of a user email record.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserEmailInfo {
    /// Email record ID.
    pub id: UserEmailId,
    /// Email address.
    pub email: String,
    /// Whether this email is verified.
    pub verified: bool,
    /// When the email was created.
    pub created_at: Option<std::time::SystemTime>,
    /// When the email was verified (if verified).
    pub verified_at: Option<std::time::SystemTime>,
}

/// Information about a user region migration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserMigrationInfo {
    /// User slug.
    pub slug: UserSlug,
    /// Source region for the migration.
    pub source_region: Region,
    /// Target region for the migration.
    pub target_region: Region,
    /// Current directory status.
    pub directory_status: String,
}

/// Status of a blinding key rotation initiated by
/// [`LedgerClient::rotate_blinding_key`](crate::LedgerClient::rotate_blinding_key).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BlindingKeyRotationStatus {
    /// Total email hash entries to re-hash.
    pub total_entries: u64,
    /// Entries re-hashed so far (0 on initial response).
    pub entries_rehashed: u64,
    /// Whether the rotation is already complete (true if zero entries).
    pub complete: bool,
}

/// Verification code returned by
/// [`LedgerClient::initiate_email_verification`](crate::LedgerClient::initiate_email_verification).
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EmailVerificationCode {
    /// 6-character verification code (A-Z, 0-9).
    pub code: String,
}

impl std::fmt::Debug for EmailVerificationCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmailVerificationCode").field("code", &"<redacted>").finish()
    }
}

/// Result of [`LedgerClient::verify_email_code`](crate::LedgerClient::verify_email_code).
///
/// Either a session for an existing user, or an onboarding token for
/// a new user who must complete registration.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum EmailVerificationResult {
    /// The email belongs to an existing user; a session was created.
    ExistingUser {
        /// Existing user's external slug.
        user: UserSlug,
        /// Session tokens.
        session: crate::token::TokenPair,
    },
    /// The email is new; the user must complete registration.
    NewUser {
        /// Opaque onboarding token (single-use, 12-hour TTL).
        onboarding_token: String,
    },
    /// The user has TOTP enabled; second-factor verification is required.
    TotpRequired {
        /// 32-byte challenge nonce for the TOTP verification step.
        challenge_nonce: Vec<u8>,
    },
}

impl std::fmt::Debug for EmailVerificationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExistingUser { user, .. } => f
                .debug_struct("ExistingUser")
                .field("user", user)
                .field("session", &"<redacted>")
                .finish(),
            Self::NewUser { .. } => {
                f.debug_struct("NewUser").field("onboarding_token", &"<redacted>").finish()
            },
            Self::TotpRequired { .. } => {
                f.debug_struct("TotpRequired").field("challenge_nonce", &"<redacted>").finish()
            },
        }
    }
}

/// Result of [`LedgerClient::complete_registration`](crate::LedgerClient::complete_registration).
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RegistrationResult {
    /// The newly created user's external slug.
    pub user: UserSlug,
    /// Session tokens for the new user.
    pub session: crate::token::TokenPair,
    /// The auto-created organization's external slug (if any).
    pub organization: Option<OrganizationSlug>,
}

impl std::fmt::Debug for RegistrationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationResult")
            .field("user", &self.user)
            .field("session", &"<redacted>")
            .field("organization", &self.organization)
            .finish()
    }
}

/// Role of a member within an organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum OrganizationMemberRole {
    /// Organization administrator — can manage members and settings.
    Admin,
    /// Regular organization member.
    Member,
}

impl OrganizationMemberRole {
    /// Returns the variant as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Member => "member",
        }
    }
}

impl fmt::Display for OrganizationMemberRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Information about an organization member.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OrganizationMemberInfo {
    /// User identifier.
    pub user: UserSlug,
    /// Role within the organization.
    pub role: OrganizationMemberRole,
    /// When the member joined the organization.
    pub joined_at: Option<std::time::SystemTime>,
}

/// Role within a team.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum TeamMemberRole {
    /// Team manager — can update team settings.
    Manager,
    /// Regular team member.
    Member,
}

impl TeamMemberRole {
    /// Returns the variant as a static string slice.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Manager => "manager",
            Self::Member => "member",
        }
    }
}

impl fmt::Display for TeamMemberRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A member of a team.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TeamMemberInfo {
    /// User identifier.
    pub user: UserSlug,
    /// Role within the team.
    pub role: TeamMemberRole,
    /// When the member joined the team.
    pub joined_at: Option<std::time::SystemTime>,
}

/// Information about an organization team.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TeamInfo {
    /// Team identifier.
    pub slug: TeamSlug,
    /// Organization this team belongs to.
    pub organization: OrganizationSlug,
    /// Team name.
    pub name: String,
    /// Team members.
    pub members: Vec<TeamMemberInfo>,
    /// When the team was created.
    pub created_at: Option<std::time::SystemTime>,
    /// When the team was last updated.
    pub updated_at: Option<std::time::SystemTime>,
}

/// Information about an organization.
///
/// Contains metadata about an organization including its ID, name, region assignment,
/// and current status.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OrganizationInfo {
    /// Unique organization slug (Snowflake ID).
    pub slug: OrganizationSlug,
    /// Human-readable organization name.
    pub name: String,
    /// Data residency region for this organization.
    pub region: Region,
    /// Node IDs in the region's Raft group (node IDs are strings).
    pub member_nodes: Vec<String>,
    /// Configuration version number.
    pub config_version: u64,
    /// Current organization status.
    pub status: OrganizationStatus,
    /// Billing tier.
    pub tier: OrganizationTier,
    /// Organization members with roles.
    pub members: Vec<OrganizationMemberInfo>,
}

/// Information returned when an organization is soft-deleted.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OrganizationDeleteInfo {
    /// When the soft-delete was initiated (absent if server omitted timestamp).
    pub deleted_at: Option<std::time::SystemTime>,
    /// Region-derived cooldown in days before data is purged.
    pub retention_days: u32,
}

/// Information about a vault.
///
/// Contains metadata about a vault including its ID, current height,
/// state root, and node membership.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VaultInfo {
    /// Organization slug for this vault.
    pub organization: OrganizationSlug,
    /// Unique vault identifier (Snowflake ID) within the organization.
    pub vault: VaultSlug,
    /// Current block height.
    pub height: u64,
    /// Current state root (Merkle root).
    pub state_root: Vec<u8>,
    /// Node IDs replicating this vault (node IDs are strings).
    pub nodes: Vec<String>,
    /// Current leader node ID.
    pub leader: Option<String>,
    /// Current vault status.
    pub status: VaultStatus,
}

/// Health status of a node or vault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum HealthStatus {
    /// Status is unknown or unspecified.
    #[default]
    Unspecified,
    /// The node or vault is healthy and fully operational.
    Healthy,
    /// The node or vault is operational but has some issues.
    Degraded,
    /// The node or vault is unavailable.
    Unavailable,
}

/// Result of a health check operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HealthCheckResult {
    /// The health status.
    pub status: HealthStatus,
    /// Human-readable message describing the health state.
    pub message: String,
    /// Additional details as key-value pairs.
    pub details: std::collections::HashMap<String, String>,
}

impl HealthCheckResult {
    /// Returns true if the status is healthy.
    pub fn is_healthy(&self) -> bool {
        self.status == HealthStatus::Healthy
    }

    /// Returns true if the status is degraded.
    pub fn is_degraded(&self) -> bool {
        self.status == HealthStatus::Degraded
    }

    /// Returns true if the status is unavailable.
    pub fn is_unavailable(&self) -> bool {
        self.status == HealthStatus::Unavailable
    }
}
