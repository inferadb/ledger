//! Administrative domain types: organizations, vaults, users, teams, health.

use std::fmt;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{
    OrganizationSlug, Region, TeamSlug, UserEmailId, UserRole, UserSlug, UserStatus, VaultSlug,
};

use crate::proto_util::{proto_timestamp_to_system_time, region_from_proto_i32};

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

impl fmt::Display for OrganizationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unspecified => write!(f, "unspecified"),
            Self::Active => write!(f, "active"),
            Self::Provisioning => write!(f, "provisioning"),
            Self::Migrating => write!(f, "migrating"),
            Self::Suspended => write!(f, "suspended"),
            Self::Deleted => write!(f, "deleted"),
        }
    }
}

impl OrganizationStatus {
    /// Converts from protobuf enum value.
    pub(crate) fn from_proto(value: i32) -> Self {
        match proto::OrganizationStatus::try_from(value) {
            Ok(proto::OrganizationStatus::Active) => OrganizationStatus::Active,
            Ok(proto::OrganizationStatus::Provisioning) => OrganizationStatus::Provisioning,
            Ok(proto::OrganizationStatus::Migrating) => OrganizationStatus::Migrating,
            Ok(proto::OrganizationStatus::Suspended) => OrganizationStatus::Suspended,
            Ok(proto::OrganizationStatus::Deleted) => OrganizationStatus::Deleted,
            _ => OrganizationStatus::Unspecified,
        }
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
    /// Pro tier with enhanced features.
    Pro,
    /// Enterprise tier with full features.
    Enterprise,
}

impl fmt::Display for OrganizationTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Free => write!(f, "free"),
            Self::Pro => write!(f, "pro"),
            Self::Enterprise => write!(f, "enterprise"),
        }
    }
}

impl OrganizationTier {
    /// Converts from protobuf enum value.
    pub(crate) fn from_proto(value: i32) -> Self {
        match proto::OrganizationTier::try_from(value) {
            Ok(proto::OrganizationTier::Pro) => OrganizationTier::Pro,
            Ok(proto::OrganizationTier::Enterprise) => OrganizationTier::Enterprise,
            _ => OrganizationTier::Free,
        }
    }

    /// Converts to protobuf enum value.
    pub(crate) fn to_proto(self) -> i32 {
        match self {
            OrganizationTier::Free => proto::OrganizationTier::Free.into(),
            OrganizationTier::Pro => proto::OrganizationTier::Pro.into(),
            OrganizationTier::Enterprise => proto::OrganizationTier::Enterprise.into(),
        }
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

impl fmt::Display for VaultStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unspecified => write!(f, "unspecified"),
            Self::Active => write!(f, "active"),
            Self::ReadOnly => write!(f, "read_only"),
            Self::Deleted => write!(f, "deleted"),
        }
    }
}

impl VaultStatus {
    /// Converts from protobuf enum value.
    pub(crate) fn from_proto(value: i32) -> Self {
        match proto::VaultStatus::try_from(value) {
            Ok(proto::VaultStatus::Active) => VaultStatus::Active,
            Ok(proto::VaultStatus::ReadOnly) => VaultStatus::ReadOnly,
            Ok(proto::VaultStatus::Deleted) => VaultStatus::Deleted,
            _ => VaultStatus::Unspecified,
        }
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

/// Status of a blinding key rehash operation.
///
/// Returned by
/// [`LedgerClient::get_blinding_key_rehash_status`](crate::LedgerClient::get_blinding_key_rehash_status).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BlindingKeyRehashStatus {
    /// Total email hash entries to re-hash.
    pub total_entries: u64,
    /// Entries re-hashed across all regions.
    pub entries_rehashed: u64,
    /// Whether the rotation is fully complete.
    pub complete: bool,
    /// Per-region progress: region name to entries re-hashed in that region.
    pub per_region_progress: std::collections::HashMap<String, u64>,
    /// Currently active blinding key version.
    pub active_key_version: u32,
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

impl fmt::Display for OrganizationMemberRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admin => write!(f, "admin"),
            Self::Member => write!(f, "member"),
        }
    }
}

impl OrganizationMemberRole {
    /// Converts from protobuf enum value.
    pub(crate) fn from_proto(value: i32) -> Self {
        match proto::OrganizationMemberRole::try_from(value) {
            Ok(proto::OrganizationMemberRole::Admin) => OrganizationMemberRole::Admin,
            _ => OrganizationMemberRole::Member,
        }
    }

    /// Converts to protobuf enum value.
    pub(crate) fn to_proto(self) -> i32 {
        match self {
            OrganizationMemberRole::Admin => proto::OrganizationMemberRole::Admin.into(),
            OrganizationMemberRole::Member => proto::OrganizationMemberRole::Member.into(),
        }
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

impl OrganizationMemberInfo {
    /// Converts from protobuf message.
    pub(crate) fn from_proto(member: &proto::OrganizationMember) -> Self {
        Self {
            user: UserSlug::new(member.user.map_or(0, |u| u.slug)),
            role: OrganizationMemberRole::from_proto(member.role),
            joined_at: member.joined_at.as_ref().and_then(proto_timestamp_to_system_time),
        }
    }
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

impl fmt::Display for TeamMemberRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Manager => write!(f, "manager"),
            Self::Member => write!(f, "member"),
        }
    }
}

impl TeamMemberRole {
    /// Converts from protobuf enum value.
    pub(crate) fn from_proto(value: i32) -> Self {
        match proto::OrganizationTeamMemberRole::try_from(value) {
            Ok(proto::OrganizationTeamMemberRole::Manager) => TeamMemberRole::Manager,
            _ => TeamMemberRole::Member,
        }
    }

    /// Converts to protobuf enum value.
    pub(crate) fn to_proto(self) -> proto::OrganizationTeamMemberRole {
        match self {
            TeamMemberRole::Manager => proto::OrganizationTeamMemberRole::Manager,
            TeamMemberRole::Member => proto::OrganizationTeamMemberRole::Member,
        }
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

impl TeamMemberInfo {
    /// Converts from protobuf message.
    pub(crate) fn from_proto(member: &proto::OrganizationTeamMember) -> Self {
        Self {
            user: UserSlug::new(member.user.map_or(0, |u| u.slug)),
            role: TeamMemberRole::from_proto(member.role),
            joined_at: member.joined_at.as_ref().and_then(proto_timestamp_to_system_time),
        }
    }
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

impl TeamInfo {
    /// Converts from protobuf message.
    pub(crate) fn from_proto(team: &proto::OrganizationTeam) -> Self {
        Self {
            slug: TeamSlug::new(team.slug.map_or(0, |s| s.slug)),
            organization: OrganizationSlug::new(team.organization.map_or(0, |n| n.slug)),
            name: team.name.clone(),
            members: team.members.iter().map(TeamMemberInfo::from_proto).collect(),
            created_at: team.created_at.as_ref().and_then(proto_timestamp_to_system_time),
            updated_at: team.updated_at.as_ref().and_then(proto_timestamp_to_system_time),
        }
    }
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

impl OrganizationInfo {
    /// Constructs from the fields shared by all organization response protos.
    pub(crate) fn from_fields(
        slug: Option<proto::OrganizationSlug>,
        name: String,
        region: i32,
        member_nodes: Vec<proto::NodeId>,
        config_version: u64,
        status: i32,
        tier: i32,
        members: &[proto::OrganizationMember],
    ) -> Self {
        Self {
            slug: OrganizationSlug::new(slug.map_or(0, |n| n.slug)),
            name,
            region: region_from_proto_i32(region).unwrap_or(Region::GLOBAL),
            member_nodes: member_nodes.into_iter().map(|n| n.id).collect(),
            config_version,
            status: OrganizationStatus::from_proto(status),
            tier: OrganizationTier::from_proto(tier),
            members: members.iter().map(OrganizationMemberInfo::from_proto).collect(),
        }
    }

    /// Creates from protobuf get response.
    pub(crate) fn from_proto(r: proto::GetOrganizationResponse) -> Self {
        Self::from_fields(
            r.slug,
            r.name,
            r.region,
            r.member_nodes,
            r.config_version,
            r.status,
            r.tier,
            &r.members,
        )
    }

    /// Creates from protobuf update response.
    pub(crate) fn from_update_proto(r: proto::UpdateOrganizationResponse) -> Self {
        Self::from_fields(
            r.slug,
            r.name,
            r.region,
            r.member_nodes,
            r.config_version,
            r.status,
            r.tier,
            &r.members,
        )
    }
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

impl VaultInfo {
    /// Creates from protobuf response.
    pub(crate) fn from_proto(proto: proto::GetVaultResponse) -> Self {
        Self {
            organization: OrganizationSlug::new(proto.organization.map_or(0, |n| n.slug)),
            vault: VaultSlug::new(proto.vault.map_or(0, |v| v.slug)),
            height: proto.height,
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            nodes: proto.nodes.into_iter().map(|n| n.id).collect(),
            leader: proto.leader.map(|n| n.id),
            status: VaultStatus::from_proto(proto.status),
        }
    }
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

impl HealthStatus {
    /// Converts from protobuf enum value.
    pub(crate) fn from_proto(value: i32) -> Self {
        match proto::HealthStatus::try_from(value) {
            Ok(proto::HealthStatus::Healthy) => HealthStatus::Healthy,
            Ok(proto::HealthStatus::Degraded) => HealthStatus::Degraded,
            Ok(proto::HealthStatus::Unavailable) => HealthStatus::Unavailable,
            _ => HealthStatus::Unspecified,
        }
    }
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
    /// Creates from protobuf response.
    pub(crate) fn from_proto(proto: proto::HealthCheckResponse) -> Self {
        Self {
            status: HealthStatus::from_proto(proto.status),
            message: proto.message,
            details: proto.details,
        }
    }

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
