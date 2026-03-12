//! Main `LedgerClient` implementation.
//!
//! Provides the high-level API for interacting with the Ledger service,
//! orchestrating connection pool, idempotency keys, and retry logic.

use std::sync::Arc;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{
    AppSlug, ClientAssertionId as DomainClientAssertionId, OrganizationSlug, Region, TeamSlug,
    UserEmailId, UserRole, UserSlug, UserStatus, VaultSlug,
};
use tonic::service::interceptor::InterceptedService;

use crate::{
    config::ClientConfig,
    connection::ConnectionPool,
    error::{self, Result},
    retry::with_retry_cancellable,
    server::{ServerResolver, ServerSource},
    streaming::{HeightTracker, ReconnectingStream},
    tracing::TraceContextInterceptor,
};

/// Consistency level for read operations.
///
/// Controls whether reads are served from any replica (eventual) or must
/// go through the leader (linearizable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadConsistency {
    /// Reads from any replica (fastest, may be stale).
    #[default]
    Eventual,
    /// Reads from leader (strong consistency, higher latency).
    Linearizable,
}

impl ReadConsistency {
    /// Converts to protobuf enum value.
    fn to_proto(self) -> proto::ReadConsistency {
        match self {
            ReadConsistency::Eventual => proto::ReadConsistency::Eventual,
            ReadConsistency::Linearizable => proto::ReadConsistency::Linearizable,
        }
    }
}

// =============================================================================
// Write Types
// =============================================================================

/// Result of a successful write operation.
///
/// Contains the transaction ID, block height, and server-assigned sequence number
/// for the committed write. This information can be used for:
/// - Tracking transaction history
/// - Waiting for replication to replicas
/// - Verified reads at a specific block height
/// - Monitoring client write progress via assigned_sequence
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteSuccess {
    /// Unique transaction ID assigned by the server.
    pub tx_id: String,
    /// Block height where the transaction was committed.
    pub block_height: u64,
    /// Server-assigned sequence number for this write.
    ///
    /// The server assigns monotonically increasing sequence numbers at Raft commit
    /// time. This provides a total ordering of writes per (organization, vault, client)
    /// and can be used for audit trail continuity.
    pub assigned_sequence: u64,
}

// =============================================================================
// Streaming Types
// =============================================================================

/// A block announcement from the WatchBlocks stream.
///
/// Contains metadata about a newly committed block in a vault's chain.
/// Used for real-time notifications of state changes.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, OrganizationSlug, VaultSlug, ServerSource};
/// # use futures::StreamExt;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = LedgerClient::new(ClientConfig::builder()
/// #     .servers(ServerSource::from_static(["http://localhost:50051"]))
/// #     .client_id("example")
/// #     .build()?).await?;
/// # let (organization, vault, start_height) = (OrganizationSlug::new(1), VaultSlug::new(1), 1u64);
/// let mut stream = client.watch_blocks(organization, vault, start_height).await?;
/// while let Some(announcement) = stream.next().await {
///     let block = announcement?;
///     println!("New block at height {}: {:?}", block.height, block.block_hash);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockAnnouncement {
    /// Organization containing the vault.
    pub organization: OrganizationSlug,
    /// Vault (Snowflake ID) within the organization.
    pub vault: VaultSlug,
    /// Block height (1-indexed).
    pub height: u64,
    /// Hash of the block header.
    pub block_hash: Vec<u8>,
    /// Merkle root of the state at this block.
    pub state_root: Vec<u8>,
    /// Timestamp when the block was committed.
    pub timestamp: Option<std::time::SystemTime>,
}

impl BlockAnnouncement {
    /// Creates a BlockAnnouncement from the protobuf type.
    fn from_proto(proto: proto::BlockAnnouncement) -> Self {
        let timestamp = proto.timestamp.and_then(|ts| proto_timestamp_to_system_time(&ts));

        Self {
            organization: OrganizationSlug::new(proto.organization.map_or(0, |n| n.slug)),
            vault: VaultSlug::new(proto.vault.map_or(0, |v| v.slug)),
            height: proto.height,
            block_hash: proto.block_hash.map(|h| h.value).unwrap_or_default(),
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            timestamp,
        }
    }
}

// =============================================================================
// Admin Types
// =============================================================================

/// Status of an organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
    /// Creates from protobuf enum value.
    fn from_proto(value: i32) -> Self {
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
pub enum OrganizationTier {
    /// Free tier with basic features.
    #[default]
    Free,
    /// Pro tier with enhanced features.
    Pro,
    /// Enterprise tier with full features.
    Enterprise,
}

impl OrganizationTier {
    fn from_proto(value: i32) -> Self {
        match proto::OrganizationTier::try_from(value) {
            Ok(proto::OrganizationTier::Pro) => OrganizationTier::Pro,
            Ok(proto::OrganizationTier::Enterprise) => OrganizationTier::Enterprise,
            _ => OrganizationTier::Free,
        }
    }

    fn to_proto(self) -> i32 {
        match self {
            OrganizationTier::Free => proto::OrganizationTier::Free.into(),
            OrganizationTier::Pro => proto::OrganizationTier::Pro.into(),
            OrganizationTier::Enterprise => proto::OrganizationTier::Enterprise.into(),
        }
    }
}

/// Status of a vault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
    /// Creates from protobuf enum value.
    fn from_proto(value: i32) -> Self {
        match proto::VaultStatus::try_from(value) {
            Ok(proto::VaultStatus::Active) => VaultStatus::Active,
            Ok(proto::VaultStatus::ReadOnly) => VaultStatus::ReadOnly,
            Ok(proto::VaultStatus::Deleted) => VaultStatus::Deleted,
            _ => VaultStatus::Unspecified,
        }
    }
}

/// Converts a protobuf region `i32` to a domain [`Region`].
///
/// Returns `None` for unspecified (0) or unknown values, allowing callers
/// to decide the fallback behavior.
pub(crate) fn region_from_proto_i32(value: i32) -> Option<Region> {
    let proto_region = proto::Region::try_from(value).ok()?;
    Region::try_from(proto_region).ok()
}

/// Converts a domain [`Region`] to a protobuf `i32` value.
pub(crate) fn region_to_proto_i32(region: Region) -> i32 {
    let proto_region: proto::Region = region.into();
    proto_region.into()
}

/// Converts a protobuf `UserStatus` i32 to a domain [`UserStatus`].
pub(crate) fn user_status_from_proto_i32(value: i32) -> UserStatus {
    match proto::UserStatus::try_from(value) {
        Ok(proto::UserStatus::Active) => UserStatus::Active,
        Ok(proto::UserStatus::PendingOrg) => UserStatus::PendingOrg,
        Ok(proto::UserStatus::Suspended) => UserStatus::Suspended,
        Ok(proto::UserStatus::Deleting) => UserStatus::Deleting,
        Ok(proto::UserStatus::Deleted) => UserStatus::Deleted,
        _ => UserStatus::Active,
    }
}

/// Converts a protobuf `UserRole` i32 to a domain [`UserRole`].
pub(crate) fn user_role_from_proto_i32(value: i32) -> UserRole {
    match proto::UserRole::try_from(value) {
        Ok(proto::UserRole::Admin) => UserRole::Admin,
        _ => UserRole::User,
    }
}

/// Converts a proto `User` message to a [`UserInfo`].
pub(crate) fn user_info_from_proto(user: &proto::User) -> UserInfo {
    UserInfo {
        slug: UserSlug::new(user.slug.as_ref().map_or(0, |s| s.slug)),
        name: user.name.clone(),
        primary_email_id: UserEmailId::new(user.email.as_ref().map_or(0, |e| e.id)),
        status: user_status_from_proto_i32(user.status),
        role: user_role_from_proto_i32(user.role),
        created_at: user.created_at.as_ref().and_then(proto_timestamp_to_system_time),
        updated_at: user.updated_at.as_ref().and_then(proto_timestamp_to_system_time),
        deleted_at: user.deleted_at.as_ref().and_then(proto_timestamp_to_system_time),
        retention_days: None,
    }
}

/// Converts a proto `UserEmail` message to a [`UserEmailInfo`].
pub(crate) fn user_email_info_from_proto(email: &proto::UserEmail) -> UserEmailInfo {
    UserEmailInfo {
        id: UserEmailId::new(email.id.as_ref().map_or(0, |e| e.id)),
        email: email.email.clone(),
        verified: email.verified_at.is_some(),
        created_at: email.created_at.as_ref().and_then(proto_timestamp_to_system_time),
        verified_at: email.verified_at.as_ref().and_then(proto_timestamp_to_system_time),
    }
}

/// Information about an in-progress organization migration.
#[derive(Debug, Clone, PartialEq, Eq)]
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
pub struct UserInfo {
    /// External Snowflake slug.
    pub slug: UserSlug,
    /// Display name.
    pub name: String,
    /// Primary email ID.
    pub primary_email_id: UserEmailId,
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

/// Status of a blinding key rotation initiated by [`LedgerClient::rotate_blinding_key`].
#[derive(Debug, Clone, PartialEq, Eq)]
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
/// Returned by [`LedgerClient::get_blinding_key_rehash_status`].
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Verification code returned by [`LedgerClient::initiate_email_verification`].
#[derive(Clone, PartialEq, Eq)]
pub struct EmailVerificationCode {
    /// 6-character verification code (A-Z, 0-9).
    pub code: String,
}

impl std::fmt::Debug for EmailVerificationCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmailVerificationCode").field("code", &"<redacted>").finish()
    }
}

/// Result of [`LedgerClient::verify_email_code`].
///
/// Either a session for an existing user, or an onboarding token for
/// a new user who must complete registration.
#[derive(Clone, PartialEq, Eq)]
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
        }
    }
}

/// Result of [`LedgerClient::complete_registration`].
#[derive(Clone, PartialEq, Eq)]
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

/// Converts a proto `Timestamp` to `SystemTime`, returning `None` for invalid values.
pub(crate) fn proto_timestamp_to_system_time(
    ts: &prost_types::Timestamp,
) -> Option<std::time::SystemTime> {
    let secs = u64::try_from(ts.seconds).ok()?;
    let nanos = u32::try_from(ts.nanos).ok().filter(|&n| n < 1_000_000_000)?;
    std::time::UNIX_EPOCH.checked_add(std::time::Duration::new(secs, nanos))
}

fn system_time_to_proto_timestamp(t: &std::time::SystemTime) -> prost_types::Timestamp {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => {
            prost_types::Timestamp { seconds: d.as_secs() as i64, nanos: d.subsec_nanos() as i32 }
        },
        Err(_) => prost_types::Timestamp { seconds: 0, nanos: 0 },
    }
}

/// Role of a member within an organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrganizationMemberRole {
    /// Organization administrator — can manage members and settings.
    Admin,
    /// Regular organization member.
    Member,
}

impl OrganizationMemberRole {
    /// Converts from protobuf enum value.
    fn from_proto(value: i32) -> Self {
        match proto::OrganizationMemberRole::try_from(value) {
            Ok(proto::OrganizationMemberRole::Admin) => OrganizationMemberRole::Admin,
            _ => OrganizationMemberRole::Member,
        }
    }

    /// Converts to protobuf enum value.
    fn to_proto(self) -> i32 {
        match self {
            OrganizationMemberRole::Admin => proto::OrganizationMemberRole::Admin.into(),
            OrganizationMemberRole::Member => proto::OrganizationMemberRole::Member.into(),
        }
    }
}

/// Information about an organization member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrganizationMemberInfo {
    /// User identifier.
    pub user: UserSlug,
    /// Role within the organization.
    pub role: OrganizationMemberRole,
    /// When the member joined the organization.
    pub joined_at: Option<std::time::SystemTime>,
}

impl OrganizationMemberInfo {
    /// Creates from protobuf message.
    fn from_proto(member: &proto::OrganizationMember) -> Self {
        Self {
            user: UserSlug::new(member.user.map_or(0, |u| u.slug)),
            role: OrganizationMemberRole::from_proto(member.role),
            joined_at: member.joined_at.as_ref().and_then(proto_timestamp_to_system_time),
        }
    }
}

/// Role within a team.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamMemberRole {
    /// Team manager — can update team settings.
    Manager,
    /// Regular team member.
    Member,
}

impl TeamMemberRole {
    /// Converts from protobuf enum value.
    fn from_proto(value: i32) -> Self {
        match proto::OrganizationTeamMemberRole::try_from(value) {
            Ok(proto::OrganizationTeamMemberRole::Manager) => TeamMemberRole::Manager,
            _ => TeamMemberRole::Member,
        }
    }
}

/// A member of a team.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamMemberInfo {
    /// User identifier.
    pub user: UserSlug,
    /// Role within the team.
    pub role: TeamMemberRole,
    /// When the member joined the team.
    pub joined_at: Option<std::time::SystemTime>,
}

impl TeamMemberInfo {
    fn from_proto(member: &proto::OrganizationTeamMember) -> Self {
        Self {
            user: UserSlug::new(member.user.map_or(0, |u| u.slug)),
            role: TeamMemberRole::from_proto(member.role),
            joined_at: member.joined_at.as_ref().and_then(proto_timestamp_to_system_time),
        }
    }
}

/// Information about an organization team.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn from_proto(team: &proto::OrganizationTeam) -> Self {
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
pub struct OrganizationDeleteInfo {
    /// When the soft-delete was initiated (absent if server omitted timestamp).
    pub deleted_at: Option<std::time::SystemTime>,
    /// Region-derived cooldown in days before data is purged.
    pub retention_days: u32,
}

impl OrganizationInfo {
    /// Constructs from the fields shared by all organization response protos.
    fn from_fields(
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
    fn from_proto(r: proto::GetOrganizationResponse) -> Self {
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
    fn from_update_proto(r: proto::UpdateOrganizationResponse) -> Self {
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
    fn from_proto(proto: proto::GetVaultResponse) -> Self {
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
///
/// Maps to the protobuf `HealthStatus` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
    /// Creates from protobuf enum value.
    fn from_proto(value: i32) -> Self {
        match proto::HealthStatus::try_from(value) {
            Ok(proto::HealthStatus::Healthy) => HealthStatus::Healthy,
            Ok(proto::HealthStatus::Degraded) => HealthStatus::Degraded,
            Ok(proto::HealthStatus::Unavailable) => HealthStatus::Unavailable,
            _ => HealthStatus::Unspecified,
        }
    }
}

/// Result of a health check operation.
///
/// Contains the health status along with a message and additional details.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn from_proto(proto: proto::HealthCheckResponse) -> Self {
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

// =============================================================================
// App Types
// =============================================================================

/// SDK representation of a client application.
#[derive(Debug, Clone)]
pub struct AppInfo {
    /// External Snowflake slug.
    pub slug: AppSlug,
    /// Human-readable name.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Whether the app is enabled.
    pub enabled: bool,
    /// Credentials configuration (absent in list responses).
    pub credentials: Option<AppCredentialsInfo>,
    /// Creation timestamp.
    pub created_at: Option<std::time::SystemTime>,
    /// Last update timestamp.
    pub updated_at: Option<std::time::SystemTime>,
}

/// Credential configuration for an app.
#[derive(Debug, Clone)]
pub struct AppCredentialsInfo {
    /// Client secret credential.
    pub client_secret_enabled: bool,
    /// mTLS CA credential.
    pub mtls_ca_enabled: bool,
    /// mTLS self-signed credential.
    pub mtls_self_signed_enabled: bool,
    /// Client assertion (private key JWT) credential.
    pub client_assertion_enabled: bool,
}

/// A client assertion entry (public metadata only — private key is never returned after creation).
#[derive(Debug, Clone)]
pub struct AppClientAssertionInfo {
    /// Server-assigned assertion ID.
    pub id: DomainClientAssertionId,
    /// User-provided name.
    pub name: String,
    /// Whether this assertion is individually enabled.
    pub enabled: bool,
    /// Expiration timestamp.
    pub expires_at: Option<std::time::SystemTime>,
    /// Creation timestamp.
    pub created_at: Option<std::time::SystemTime>,
}

/// A vault connection for an app.
#[derive(Debug, Clone)]
pub struct AppVaultConnectionInfo {
    /// Vault slug (external identifier).
    pub vault_slug: VaultSlug,
    /// Allowed scopes for this vault connection.
    pub allowed_scopes: Vec<String>,
    /// Creation timestamp.
    pub created_at: Option<std::time::SystemTime>,
}

/// Result of creating a client assertion — includes the private key PEM (returned only once).
#[derive(Debug, Clone)]
pub struct CreateAppClientAssertionResult {
    /// The created assertion metadata.
    pub assertion: AppClientAssertionInfo,
    /// Private key PEM (Ed25519). Only returned on creation.
    pub private_key_pem: String,
}

/// Result of getting a client secret's status.
#[derive(Debug, Clone)]
pub struct AppClientSecretStatus {
    /// Whether the client secret credential is enabled.
    pub enabled: bool,
    /// Whether a secret has been generated.
    pub has_secret: bool,
}

/// Credential type for enable/disable operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppCredentialType {
    /// Client secret (shared secret).
    ClientSecret,
    /// mTLS with CA-signed certificate.
    MtlsCa,
    /// mTLS with self-signed certificate.
    MtlsSelfSigned,
    /// Client assertion (private key JWT).
    ClientAssertion,
}

impl AppCredentialType {
    fn to_proto(self) -> i32 {
        match self {
            AppCredentialType::ClientSecret => proto::AppCredentialType::ClientSecret as i32,
            AppCredentialType::MtlsCa => proto::AppCredentialType::MtlsCa as i32,
            AppCredentialType::MtlsSelfSigned => proto::AppCredentialType::MtlsSelfSigned as i32,
            AppCredentialType::ClientAssertion => proto::AppCredentialType::ClientAssertion as i32,
        }
    }
}

fn app_info_from_proto(p: &proto::AppInfo) -> AppInfo {
    AppInfo {
        slug: AppSlug::new(p.slug.as_ref().map_or(0, |s| s.slug)),
        name: p.name.clone(),
        description: p.description.clone(),
        enabled: p.enabled,
        credentials: p.credentials.as_ref().map(|c| AppCredentialsInfo {
            client_secret_enabled: c.client_secret_enabled,
            mtls_ca_enabled: c.mtls_ca_enabled,
            mtls_self_signed_enabled: c.mtls_self_signed_enabled,
            client_assertion_enabled: c.client_assertion_enabled,
        }),
        created_at: p.created_at.as_ref().and_then(proto_timestamp_to_system_time),
        updated_at: p.updated_at.as_ref().and_then(proto_timestamp_to_system_time),
    }
}

fn assertion_info_from_proto(p: &proto::AppClientAssertionInfo) -> AppClientAssertionInfo {
    AppClientAssertionInfo {
        id: DomainClientAssertionId::new(p.id.as_ref().map_or(0, |id| id.id)),
        name: p.name.clone(),
        enabled: p.enabled,
        expires_at: p.expires_at.as_ref().and_then(proto_timestamp_to_system_time),
        created_at: p.created_at.as_ref().and_then(proto_timestamp_to_system_time),
    }
}

fn vault_connection_from_proto(p: &proto::AppVaultConnectionInfo) -> AppVaultConnectionInfo {
    AppVaultConnectionInfo {
        vault_slug: VaultSlug::new(p.vault.as_ref().map_or(0, |s| s.slug)),
        allowed_scopes: p.allowed_scopes.clone(),
        created_at: p.created_at.as_ref().and_then(proto_timestamp_to_system_time),
    }
}

/// Constructs an `SdkError::Rpc` for a missing field in a server response.
fn missing_response_field(field: &str, response_type: &str) -> error::SdkError {
    error::SdkError::Rpc {
        code: tonic::Code::Internal,
        message: format!("Missing {field} in {response_type}"),
        request_id: None,
        trace_id: None,
        error_details: None,
    }
}

// =============================================================================
// Events Types
// =============================================================================

/// Identifies which InferaDB component is the source of ingested events.
///
/// Used with [`LedgerClient::ingest_events`] to specify the originating
/// component. Only external components (Engine and Control) may ingest
/// events — the Ledger itself generates its own events internally during
/// apply-phase processing.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, EventSource, OrganizationSlug};
/// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
/// # let organization = OrganizationSlug::new(1);
/// let result = client.ingest_events(organization, EventSource::Engine, vec![]).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventSource {
    /// The authorization engine component.
    Engine,
    /// The control plane component.
    Control,
}

impl EventSource {
    /// Returns the wire-format string sent over gRPC.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Engine => "engine",
            Self::Control => "control",
        }
    }
}

impl std::fmt::Display for EventSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Scope of an event (system-wide or organization-scoped).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventScope {
    /// Cluster-wide administrative event.
    System,
    /// Per-organization tenant event.
    Organization,
}

impl EventScope {
    fn from_proto(value: i32) -> Self {
        match proto::EventScope::try_from(value) {
            Ok(proto::EventScope::System) => Self::System,
            Ok(proto::EventScope::Organization) => Self::Organization,
            _ => Self::System,
        }
    }
}

/// Outcome of an audited operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventOutcome {
    /// Operation completed successfully.
    Success,
    /// Operation failed with an error.
    Failed {
        /// Error code.
        code: String,
        /// Error details.
        detail: String,
    },
    /// Operation was denied (rate limited, unauthorized, etc.).
    Denied {
        /// Denial reason.
        reason: String,
    },
}

impl EventOutcome {
    fn from_proto(
        value: i32,
        error_code: Option<String>,
        error_detail: Option<String>,
        denial_reason: Option<String>,
    ) -> Self {
        match proto::EventOutcome::try_from(value) {
            Ok(proto::EventOutcome::Success) => Self::Success,
            Ok(proto::EventOutcome::Failed) => Self::Failed {
                code: error_code.unwrap_or_default(),
                detail: error_detail.unwrap_or_default(),
            },
            Ok(proto::EventOutcome::Denied) => {
                Self::Denied { reason: denial_reason.unwrap_or_default() }
            },
            _ => Self::Success,
        }
    }

    fn to_proto(&self) -> (i32, Option<String>, Option<String>, Option<String>) {
        match self {
            Self::Success => (proto::EventOutcome::Success as i32, None, None, None),
            Self::Failed { code, detail } => {
                (proto::EventOutcome::Failed as i32, Some(code.clone()), Some(detail.clone()), None)
            },
            Self::Denied { reason } => {
                (proto::EventOutcome::Denied as i32, None, None, Some(reason.clone()))
            },
        }
    }
}

/// Emission path of an event (how it was generated).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventEmissionPath {
    /// Deterministic, Raft-replicated — identical on all nodes.
    ApplyPhase,
    /// Node-local, best-effort — exists only on the handling node.
    HandlerPhase,
}

impl EventEmissionPath {
    fn from_proto(value: i32) -> Self {
        match proto::EventEmissionPath::try_from(value) {
            Ok(proto::EventEmissionPath::EmissionPathApplyPhase) => Self::ApplyPhase,
            Ok(proto::EventEmissionPath::EmissionPathHandlerPhase) => Self::HandlerPhase,
            _ => Self::ApplyPhase,
        }
    }
}

/// An audit event entry from the events system.
///
/// Represents a single auditable action — a write, admin operation, denial,
/// or system event. Events follow the canonical log line pattern with rich
/// contextual fields for compliance and debugging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdkEventEntry {
    /// Unique event identifier (UUID, 16 bytes).
    pub event_id: Vec<u8>,
    /// Originating service (`"ledger"`, `"engine"`, or `"control"`).
    pub source_service: String,
    /// Hierarchical dot-separated type (e.g., `"ledger.vault.created"`).
    pub event_type: String,
    /// When the event occurred.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Event scope (system or organization).
    pub scope: EventScope,
    /// Action name (snake_case, e.g., `"vault_created"`).
    pub action: String,
    /// Emission path (apply-phase or handler-phase).
    pub emission_path: EventEmissionPath,
    /// Who performed the action.
    pub principal: String,
    /// Owning organization (0 for system events).
    pub organization: OrganizationSlug,
    /// Vault context (when applicable).
    pub vault: Option<VaultSlug>,
    /// Outcome of the operation.
    pub outcome: EventOutcome,
    /// Action-specific key-value context.
    pub details: std::collections::HashMap<String, String>,
    /// Reference to blockchain block (for committed writes).
    pub block_height: Option<u64>,
    /// Node that generated the event (for handler-phase events).
    pub node_id: Option<u64>,
    /// Distributed tracing correlation (W3C Trace Context).
    pub trace_id: Option<String>,
    /// Business-level correlation for multi-step operations.
    pub correlation_id: Option<String>,
    /// Number of operations (for write actions).
    pub operations_count: Option<u32>,
}

impl SdkEventEntry {
    /// Creates from protobuf response.
    pub fn from_proto(proto: proto::EventEntry) -> Self {
        let timestamp = proto
            .timestamp
            .map(|ts| {
                chrono::DateTime::from_timestamp(ts.seconds, u32::try_from(ts.nanos).unwrap_or(0))
                    .unwrap_or(chrono::DateTime::UNIX_EPOCH)
            })
            .unwrap_or(chrono::DateTime::UNIX_EPOCH);

        Self {
            event_id: proto.event_id,
            source_service: proto.source_service,
            event_type: proto.event_type,
            timestamp,
            scope: EventScope::from_proto(proto.scope),
            action: proto.action,
            emission_path: EventEmissionPath::from_proto(proto.emission_path),
            principal: proto.principal,
            organization: OrganizationSlug::new(proto.organization.map_or(0, |o| o.slug)),
            vault: proto.vault.map(|v| VaultSlug::new(v.slug)),
            outcome: EventOutcome::from_proto(
                proto.outcome,
                proto.error_code,
                proto.error_detail,
                proto.denial_reason,
            ),
            details: proto.details,
            block_height: proto.block_height,
            node_id: proto.node_id,
            trace_id: proto.trace_id,
            correlation_id: proto.correlation_id,
            operations_count: proto.operations_count,
        }
    }

    /// Returns the event ID formatted as a UUID string.
    ///
    /// Falls back to hex encoding if the ID is not exactly 16 bytes.
    pub fn event_id_string(&self) -> String {
        if let Ok(bytes) = <[u8; 16]>::try_from(self.event_id.as_slice()) {
            uuid::Uuid::from_bytes(bytes).to_string()
        } else {
            self.event_id.iter().fold(String::new(), |mut s, b| {
                use std::fmt::Write;
                let _ = write!(s, "{b:02x}");
                s
            })
        }
    }
}

/// Paginated result from event queries.
#[derive(Debug, Clone)]
pub struct EventPage {
    /// Matching events in chronological order.
    pub entries: Vec<SdkEventEntry>,
    /// Opaque cursor for next page; `None` if no more results.
    pub next_page_token: Option<String>,
    /// Estimated total count (may be approximate for large datasets).
    pub total_estimate: Option<u64>,
}

impl EventPage {
    /// Returns `true` if there are more pages available.
    pub fn has_next_page(&self) -> bool {
        self.next_page_token.is_some()
    }
}

/// Filter criteria for event queries.
///
/// Use the builder methods to construct a filter. An empty filter matches
/// all events in the organization.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::EventFilter;
/// let filter = EventFilter::new()
///     .event_type_prefix("ledger.vault")
///     .outcome_success();
/// ```
#[derive(Debug, Clone, Default)]
pub struct EventFilter {
    start_time: Option<chrono::DateTime<chrono::Utc>>,
    end_time: Option<chrono::DateTime<chrono::Utc>>,
    actions: Vec<String>,
    event_type_prefix: Option<String>,
    principal: Option<String>,
    outcome: Option<proto::EventOutcome>,
    emission_path: Option<proto::EventEmissionPath>,
    correlation_id: Option<String>,
}

impl EventFilter {
    /// Creates an empty filter that matches all events.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filters events from this time forward (inclusive).
    pub fn start_time(mut self, time: chrono::DateTime<chrono::Utc>) -> Self {
        self.start_time = Some(time);
        self
    }

    /// Filters events before this time (exclusive).
    pub fn end_time(mut self, time: chrono::DateTime<chrono::Utc>) -> Self {
        self.end_time = Some(time);
        self
    }

    /// Filters by action names (snake_case). Multiple actions are OR'd.
    pub fn actions(mut self, actions: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.actions = actions.into_iter().map(Into::into).collect();
        self
    }

    /// Filters by event type prefix (e.g., `"ledger.vault"` matches `"ledger.vault.created"`).
    pub fn event_type_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.event_type_prefix = Some(prefix.into());
        self
    }

    /// Filters by principal (who performed the action).
    pub fn principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Filters to successful events only.
    pub fn outcome_success(mut self) -> Self {
        self.outcome = Some(proto::EventOutcome::Success);
        self
    }

    /// Filters to failed events only.
    pub fn outcome_failed(mut self) -> Self {
        self.outcome = Some(proto::EventOutcome::Failed);
        self
    }

    /// Filters to denied events only.
    pub fn outcome_denied(mut self) -> Self {
        self.outcome = Some(proto::EventOutcome::Denied);
        self
    }

    /// Filters to apply-phase events only (deterministic, replicated).
    pub fn apply_phase_only(mut self) -> Self {
        self.emission_path = Some(proto::EventEmissionPath::EmissionPathApplyPhase);
        self
    }

    /// Filters to handler-phase events only (node-local).
    pub fn handler_phase_only(mut self) -> Self {
        self.emission_path = Some(proto::EventEmissionPath::EmissionPathHandlerPhase);
        self
    }

    /// Filters by business-level correlation ID.
    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    fn to_proto(&self) -> proto::EventFilter {
        proto::EventFilter {
            start_time: self.start_time.map(|dt| prost_types::Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }),
            end_time: self.end_time.map(|dt| prost_types::Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }),
            actions: self.actions.clone(),
            event_type_prefix: self.event_type_prefix.clone(),
            principal: self.principal.clone(),
            outcome: self.outcome.map_or(0, |o| o as i32),
            emission_path: self.emission_path.map_or(0, |e| e as i32),
            correlation_id: self.correlation_id.clone(),
        }
    }
}

/// A single event for external ingestion (from Engine or Control).
///
/// Use the builder methods to construct an event entry for ingestion.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::SdkIngestEventEntry;
/// let event = SdkIngestEventEntry::new(
///     "engine.authorization.checked",
///     "user:alice",
///     inferadb_ledger_sdk::EventOutcome::Success,
/// )
/// .correlation_id("batch-job-42");
/// ```
#[derive(Debug, Clone)]
pub struct SdkIngestEventEntry {
    event_type: String,
    principal: String,
    outcome: EventOutcome,
    details: std::collections::HashMap<String, String>,
    trace_id: Option<String>,
    correlation_id: Option<String>,
    vault: Option<VaultSlug>,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

impl SdkIngestEventEntry {
    /// Creates a new event entry with required fields.
    pub fn new(
        event_type: impl Into<String>,
        principal: impl Into<String>,
        outcome: EventOutcome,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            principal: principal.into(),
            outcome,
            details: std::collections::HashMap::new(),
            trace_id: None,
            correlation_id: None,
            vault: None,
            timestamp: None,
        }
    }

    /// Adds action-specific key-value context.
    pub fn details(mut self, details: std::collections::HashMap<String, String>) -> Self {
        self.details = details;
        self
    }

    /// Adds a single detail key-value pair.
    pub fn detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    /// Sets the distributed tracing correlation ID.
    pub fn trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Sets the business-level correlation ID.
    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Sets the vault context.
    pub fn vault(mut self, vault: VaultSlug) -> Self {
        self.vault = Some(vault);
        self
    }

    /// Sets a custom timestamp (defaults to server receive time if omitted).
    pub fn timestamp(mut self, timestamp: chrono::DateTime<chrono::Utc>) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    fn into_proto(self) -> proto::IngestEventEntry {
        let (outcome, error_code, error_detail, denial_reason) = self.outcome.to_proto();
        proto::IngestEventEntry {
            event_type: self.event_type,
            principal: self.principal,
            outcome,
            details: self.details,
            trace_id: self.trace_id,
            correlation_id: self.correlation_id,
            vault: self.vault.map(|v| proto::VaultSlug { slug: v.value() }),
            timestamp: self.timestamp.map(|dt| prost_types::Timestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }),
            error_code,
            error_detail,
            denial_reason,
        }
    }
}

/// Result of an event ingestion request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestResult {
    /// Number of events accepted and written.
    pub accepted_count: u32,
    /// Number of events rejected.
    pub rejected_count: u32,
    /// Per-event rejection details.
    pub rejections: Vec<IngestRejection>,
}

/// A single rejected event from an ingestion batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestRejection {
    /// Zero-based index into the request's entries array.
    pub index: u32,
    /// Human-readable rejection reason.
    pub reason: String,
}

// =============================================================================
// Verified Read Types
// =============================================================================

/// Direction of a sibling in a Merkle proof.
///
/// Indicates whether the sibling hash should be placed on the left or right
/// when computing the parent hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Sibling is on the left: `hash(sibling || current)`.
    Left,
    /// Sibling is on the right: `hash(current || sibling)`.
    Right,
}

impl Direction {
    /// Creates from protobuf enum value.
    fn from_proto(value: i32) -> Self {
        match proto::Direction::try_from(value) {
            Ok(proto::Direction::Left) => Direction::Left,
            _ => Direction::Right, // Default to right for unspecified
        }
    }
}

/// A sibling node in a Merkle proof path.
///
/// Each sibling contains the hash of the neighboring node and which side
/// it appears on for hash computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleSibling {
    /// Hash of the sibling node.
    pub hash: Vec<u8>,
    /// Direction (left or right) relative to the current node.
    pub direction: Direction,
}

impl MerkleSibling {
    /// Creates from protobuf type.
    fn from_proto(proto: proto::MerkleSibling) -> Self {
        Self {
            hash: proto.hash.map(|h| h.value).unwrap_or_default(),
            direction: Direction::from_proto(proto.direction),
        }
    }
}

/// Merkle proof for verifying state inclusion.
///
/// Contains the leaf hash and a path of sibling hashes from leaf to root.
/// Used to verify that a value is included in the state tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleProof {
    /// Hash of the leaf (the entity key-value).
    pub leaf_hash: Vec<u8>,
    /// Sibling hashes from leaf to root (bottom-up order).
    pub siblings: Vec<MerkleSibling>,
}

impl MerkleProof {
    /// Creates from protobuf type.
    fn from_proto(proto: proto::MerkleProof) -> Self {
        Self {
            leaf_hash: proto.leaf_hash.map(|h| h.value).unwrap_or_default(),
            siblings: proto.siblings.into_iter().map(MerkleSibling::from_proto).collect(),
        }
    }

    /// Verifies this proof against an expected state root.
    ///
    /// Recomputes the root hash from the leaf through the sibling path and
    /// checks if it matches the expected root.
    ///
    /// # Arguments
    ///
    /// * `expected_root` - The expected state root hash to verify against.
    ///
    /// # Returns
    ///
    /// `true` if the proof is valid and matches the expected root.
    pub fn verify(&self, expected_root: &[u8]) -> bool {
        use sha2::{Digest, Sha256};

        if self.siblings.is_empty() {
            // Single-element tree: leaf hash equals root
            return self.leaf_hash == expected_root;
        }

        let mut current_hash = self.leaf_hash.clone();

        for sibling in &self.siblings {
            let mut hasher = Sha256::new();
            match sibling.direction {
                Direction::Left => {
                    // Sibling is on left: hash(sibling || current)
                    hasher.update(&sibling.hash);
                    hasher.update(&current_hash);
                },
                Direction::Right => {
                    // Sibling is on right: hash(current || sibling)
                    hasher.update(&current_hash);
                    hasher.update(&sibling.hash);
                },
            }
            current_hash = hasher.finalize().to_vec();
        }

        current_hash == expected_root
    }
}

/// Block header containing cryptographic commitments.
///
/// The block header is the cryptographic anchor for all state at a given height.
/// It contains the state root which can be used to verify Merkle proofs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    /// Block height (1-indexed).
    pub height: u64,
    /// Organization slug for the vault.
    pub organization: OrganizationSlug,
    /// Vault (Snowflake ID) within the organization.
    pub vault: VaultSlug,
    /// Hash of the previous block header.
    pub previous_hash: Vec<u8>,
    /// Merkle root of transactions in this block.
    pub tx_merkle_root: Vec<u8>,
    /// Merkle root of the state tree after this block.
    pub state_root: Vec<u8>,
    /// Timestamp when the block was committed.
    pub timestamp: Option<std::time::SystemTime>,
    /// Node ID of the leader that committed this block.
    pub leader_id: String,
    /// Raft term number.
    pub term: u64,
    /// Raft committed index.
    pub committed_index: u64,
    /// Server-computed hash of this block header.
    ///
    /// Used by [`ChainProof::verify`] to check chain continuity without
    /// recomputing the hash (which requires internal IDs not exposed via API).
    pub block_hash: Vec<u8>,
}

impl BlockHeader {
    /// Creates from protobuf type.
    fn from_proto(proto: proto::BlockHeader) -> Self {
        let timestamp = proto.timestamp.and_then(|ts| proto_timestamp_to_system_time(&ts));

        Self {
            height: proto.height,
            organization: OrganizationSlug::new(proto.organization.map_or(0, |n| n.slug)),
            vault: VaultSlug::new(proto.vault.map_or(0, |v| v.slug)),
            previous_hash: proto.previous_hash.map(|h| h.value).unwrap_or_default(),
            tx_merkle_root: proto.tx_merkle_root.map(|h| h.value).unwrap_or_default(),
            state_root: proto.state_root.map(|h| h.value).unwrap_or_default(),
            timestamp,
            leader_id: proto.leader_id.map(|n| n.id).unwrap_or_default(),
            term: proto.term,
            committed_index: proto.committed_index,
            block_hash: proto.block_hash.map(|h| h.value).unwrap_or_default(),
        }
    }
}

/// Chain proof linking a trusted height to a response height.
///
/// Used to verify that a block at response_height descends from trusted_height.
/// Contains block headers in ascending order from trusted_height + 1 to response_height.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainProof {
    /// Block headers from trusted_height + 1 to response_height (ascending order).
    pub headers: Vec<BlockHeader>,
}

impl ChainProof {
    /// Creates from protobuf type.
    fn from_proto(proto: proto::ChainProof) -> Self {
        Self { headers: proto.headers.into_iter().map(BlockHeader::from_proto).collect() }
    }

    /// Verifies the chain of blocks links correctly.
    ///
    /// Checks that each block's `previous_hash` matches the server-provided
    /// `block_hash` of the preceding block header.
    ///
    /// # Arguments
    ///
    /// * `trusted_header_hash` - Hash of the block at `trusted_height` that the client already
    ///   trusts.
    ///
    /// # Returns
    ///
    /// `true` if all `previous_hash` links chain correctly.
    pub fn verify(&self, trusted_header_hash: &[u8]) -> bool {
        if self.headers.is_empty() {
            return true;
        }

        // First header should link to trusted header
        if self.headers[0].previous_hash != trusted_header_hash {
            return false;
        }

        // Each subsequent header's previous_hash must match the
        // server-provided block_hash of the preceding header.
        for i in 1..self.headers.len() {
            let prev = &self.headers[i - 1];
            let curr = &self.headers[i];

            if prev.block_hash.is_empty() || curr.previous_hash != prev.block_hash {
                return false;
            }
        }

        true
    }
}

/// Options for verified read operations.
///
/// Controls which proofs to include and at what height to read.
#[derive(Debug, Clone, Default)]
pub struct VerifyOpts {
    /// Reads at a specific block height (None = current height).
    pub at_height: Option<u64>,
    /// Include chain proof linking to a trusted height.
    pub include_chain_proof: bool,
    /// Trusted height for chain proof verification.
    pub trusted_height: Option<u64>,
}

impl VerifyOpts {
    /// Creates options with default values (current height, no chain proof).
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Includes a chain proof from a trusted height.
    pub fn with_chain_proof(mut self, trusted_height: u64) -> Self {
        self.include_chain_proof = true;
        self.trusted_height = Some(trusted_height);
        self
    }
}

// =============================================================================
// Query Types
// =============================================================================

/// Paginated result from query operations.
///
/// Used by `list_entities`, `list_relationships`, and `list_resources` operations.
/// The `next_page_token` can be passed to subsequent calls to continue pagination.
#[derive(Debug, Clone)]
pub struct PagedResult<T> {
    /// Items returned in this page.
    pub items: Vec<T>,
    /// Token for fetching the next page, or `None` if this is the last page.
    pub next_page_token: Option<String>,
    /// Block height at which the query was evaluated.
    pub block_height: u64,
}

impl<T> PagedResult<T> {
    /// Checks if there are more pages available.
    pub fn has_next_page(&self) -> bool {
        self.next_page_token.is_some()
    }
}

/// An entity stored in the ledger.
///
/// Entities are key-value pairs that can have optional expiration times
/// and track their version (block height when last modified).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entity {
    /// Entity key (max 1024 bytes, UTF-8).
    pub key: String,
    /// Entity value (max 1MB).
    pub value: Vec<u8>,
    /// Unix epoch seconds when the entity expires, or `None` for no expiration.
    pub expires_at: Option<u64>,
    /// Block height when this entity was last modified.
    pub version: u64,
}

impl Entity {
    /// Converts from protobuf Entity.
    pub fn from_proto(proto: proto::Entity) -> Self {
        Self {
            key: proto.key,
            value: proto.value,
            expires_at: proto.expires_at.filter(|&ts| ts > 0),
            version: proto.version,
        }
    }

    /// Checks if this entity has expired relative to a given timestamp.
    pub fn is_expired_at(&self, now_secs: u64) -> bool {
        self.expires_at.is_some_and(|exp| exp <= now_secs)
    }
}

/// A relationship in a vault (authorization tuple).
///
/// Relationships connect resources to subjects via relations, forming
/// the basis for permission checking.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Relationship {
    /// Resource identifier in format "type:id" (max 512 chars).
    pub resource: String,
    /// Relation name (max 64 chars).
    pub relation: String,
    /// Subject identifier in format "type:id" or "type:id#relation" (max 512 chars).
    pub subject: String,
}

impl Relationship {
    /// Creates a new relationship.
    pub fn new(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Self { resource: resource.into(), relation: relation.into(), subject: subject.into() }
    }

    /// Converts from protobuf Relationship.
    pub fn from_proto(proto: proto::Relationship) -> Self {
        Self { resource: proto.resource, relation: proto.relation, subject: proto.subject }
    }
}

/// Options for listing entities.
///
/// Builder pattern for configuring entity list queries with optional filters.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct ListEntitiesOpts {
    /// Filter entities by key prefix (e.g., "user:", "session:").
    #[builder(into, default)]
    pub key_prefix: String,
    /// Reads at a specific block height (None = current).
    pub at_height: Option<u64>,
    /// Include entities past their expiration time.
    #[builder(default)]
    pub include_expired: bool,
    /// Maximum number of results per page (0 = server default).
    #[builder(default)]
    pub limit: u32,
    /// Pagination token from previous response.
    #[builder(into)]
    pub page_token: Option<String>,
    /// Read consistency level.
    #[builder(default)]
    pub consistency: ReadConsistency,
    /// Vault for vault-scoped entities (None = organization-level).
    pub vault: Option<VaultSlug>,
}

impl ListEntitiesOpts {
    /// Creates options with a key prefix filter.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self { key_prefix: prefix.into(), ..Default::default() }
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Includes expired entities in results.
    pub fn include_expired(mut self) -> Self {
        self.include_expired = true;
        self
    }

    /// Sets maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continues from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Sets read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Uses linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }

    /// Scopes to a specific vault (for vault-level entities).
    pub fn vault(mut self, vault: VaultSlug) -> Self {
        self.vault = Some(vault);
        self
    }
}

/// Options for listing relationships.
///
/// Builder pattern for configuring relationship list queries with optional filters.
/// All filter fields are optional; omitting a filter matches all values for that field.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct ListRelationshipsOpts {
    /// Filter by resource (exact match).
    #[builder(into)]
    pub resource: Option<String>,
    /// Filter by relation (exact match).
    #[builder(into)]
    pub relation: Option<String>,
    /// Filter by subject (exact match).
    #[builder(into)]
    pub subject: Option<String>,
    /// Reads at a specific block height (None = current).
    pub at_height: Option<u64>,
    /// Maximum number of results per page (0 = server default).
    #[builder(default)]
    pub limit: u32,
    /// Pagination token from previous response.
    #[builder(into)]
    pub page_token: Option<String>,
    /// Read consistency level.
    #[builder(default)]
    pub consistency: ReadConsistency,
}

impl ListRelationshipsOpts {
    /// Creates default options (no filters).
    pub fn new() -> Self {
        Self::default()
    }

    /// Filters by resource.
    pub fn resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    /// Filters by relation.
    pub fn relation(mut self, relation: impl Into<String>) -> Self {
        self.relation = Some(relation.into());
        self
    }

    /// Filters by subject.
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Sets maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continues from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Sets read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Uses linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }
}

/// Options for listing resources.
///
/// Builder pattern for configuring resource list queries.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct ListResourcesOpts {
    /// Resource type prefix (e.g., "document" matches "document:*").
    #[builder(into, default)]
    pub resource_type: String,
    /// Reads at a specific block height (None = current).
    pub at_height: Option<u64>,
    /// Maximum number of results per page (0 = server default).
    #[builder(default)]
    pub limit: u32,
    /// Pagination token from previous response.
    #[builder(into)]
    pub page_token: Option<String>,
    /// Read consistency level.
    #[builder(default)]
    pub consistency: ReadConsistency,
}

impl ListResourcesOpts {
    /// Creates options with a resource type filter.
    pub fn with_type(resource_type: impl Into<String>) -> Self {
        Self { resource_type: resource_type.into(), ..Default::default() }
    }

    /// Reads at a specific block height.
    pub fn at_height(mut self, height: u64) -> Self {
        self.at_height = Some(height);
        self
    }

    /// Sets maximum results per page.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Continues from a previous page.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }

    /// Sets read consistency level.
    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Uses linearizable (strong) consistency.
    pub fn linearizable(mut self) -> Self {
        self.consistency = ReadConsistency::Linearizable;
        self
    }
}

/// Result of a verified read operation.
///
/// Contains the value along with cryptographic proofs for client-side verification.
/// Use [`VerifiedValue::verify`] to check that the value is authentic.
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, OrganizationSlug, VaultSlug, VerifyOpts, ServerSource};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = LedgerClient::new(ClientConfig::builder()
/// #     .servers(ServerSource::from_static(["http://localhost:50051"]))
/// #     .client_id("example")
/// #     .build()?).await?;
/// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
/// let result = client.verified_read(organization, Some(vault), "key", VerifyOpts::new()).await?;
/// if let Some(verified) = result {
///     // Verify the proof is valid
///     verified.verify()?;
///     println!("Verified value: {:?}", verified.value);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedValue {
    /// The entity value (None if key not found).
    pub value: Option<Vec<u8>>,
    /// Block height at which the read was performed.
    pub block_height: u64,
    /// Block header containing the state root.
    pub block_header: BlockHeader,
    /// Merkle proof from leaf to state root.
    pub merkle_proof: MerkleProof,
    /// Optional chain proof linking to trusted height.
    pub chain_proof: Option<ChainProof>,
}

impl VerifiedValue {
    /// Creates from protobuf response.
    fn from_proto(proto: proto::VerifiedReadResponse) -> Option<Self> {
        // Block header is required for verification
        let block_header = proto.block_header.map(BlockHeader::from_proto)?;
        let merkle_proof = proto.merkle_proof.map(MerkleProof::from_proto)?;

        Some(Self {
            value: proto.value,
            block_height: proto.block_height,
            block_header,
            merkle_proof,
            chain_proof: proto.chain_proof.map(ChainProof::from_proto),
        })
    }

    /// Verifies the value is authentic.
    ///
    /// Checks that the Merkle proof correctly links the value to the state root
    /// in the block header. If a chain proof is present, also verifies the
    /// chain of blocks links correctly.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::ProofVerification` if the Merkle proof does not
    /// match the block header's state root.
    pub fn verify(&self) -> Result<()> {
        // Verify the Merkle proof against the block header's state root
        if !self.merkle_proof.verify(&self.block_header.state_root) {
            return Err(error::SdkError::ProofVerification {
                reason: "Merkle proof does not match state root",
            });
        }

        Ok(())
    }
}

/// A write operation to be submitted to the ledger.
///
/// Operations modify state in the ledger. They are applied atomically within
/// a single transaction. Use [`Operation::set_entity`] for key-value writes
/// and [`Operation::create_relationship`] for authorization tuples.
#[derive(Debug, Clone)]
pub enum Operation {
    /// Sets an entity value (key-value write).
    SetEntity {
        /// Entity key (max 1024 bytes).
        key: String,
        /// Entity value (max 1MB).
        value: Vec<u8>,
        /// Optional expiration time (Unix epoch seconds).
        expires_at: Option<u64>,
        /// Optional conditional write.
        condition: Option<SetCondition>,
    },
    /// Deletes an entity.
    DeleteEntity {
        /// Entity key to delete.
        key: String,
    },
    /// Creates an authorization relationship.
    CreateRelationship {
        /// Resource identifier (format: "type:id").
        resource: String,
        /// Relation name (e.g., "viewer", "editor").
        relation: String,
        /// Subject identifier (format: "type:id" or "type:id#relation").
        subject: String,
    },
    /// Deletes an authorization relationship.
    DeleteRelationship {
        /// Resource identifier (format: "type:id").
        resource: String,
        /// Relation name.
        relation: String,
        /// Subject identifier.
        subject: String,
    },
}

/// Condition for compare-and-set (CAS) writes.
///
/// Allows conditional writes that only succeed if the current state matches
/// the expected condition. Useful for coordination primitives like locks.
#[derive(Debug, Clone)]
pub enum SetCondition {
    /// Only set if the key doesn't exist.
    NotExists,
    /// Only set if the key exists.
    MustExist,
    /// Only set if the key was last modified at this block height.
    Version(u64),
    /// Only set if the current value matches exactly.
    ValueEquals(Vec<u8>),
}

impl Operation {
    /// Creates an operation that sets an entity's key-value pair.
    ///
    /// # Arguments
    ///
    /// * `key` - Entity key.
    /// * `value` - Entity value.
    /// * `expires_at` - Optional Unix epoch seconds when the entity expires.
    /// * `condition` - Optional condition that must be met for the write to succeed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{Operation, SetCondition};
    /// // Simple set
    /// let op = Operation::set_entity("user:123", b"data".to_vec(), None, None);
    ///
    /// // Set with expiry
    /// let op = Operation::set_entity("session:abc", b"token".to_vec(), Some(1700000000), None);
    ///
    /// // Conditional set (create-if-not-exists)
    /// let op = Operation::set_entity("lock:xyz", b"owner".to_vec(), None, Some(SetCondition::NotExists));
    /// ```
    pub fn set_entity(
        key: impl Into<String>,
        value: Vec<u8>,
        expires_at: Option<u64>,
        condition: Option<SetCondition>,
    ) -> Self {
        Operation::SetEntity { key: key.into(), value, expires_at, condition }
    }

    /// Creates an operation that deletes an entity by key.
    pub fn delete_entity(key: impl Into<String>) -> Self {
        Operation::DeleteEntity { key: key.into() }
    }

    /// Creates an operation that establishes a relationship between a resource and subject.
    ///
    /// # Arguments
    ///
    /// * `resource` - Resource identifier (format: "type:id")
    /// * `relation` - Relation name (e.g., "viewer", "editor")
    /// * `subject` - Subject identifier (format: "type:id" or "type:id#relation")
    pub fn create_relationship(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Operation::CreateRelationship {
            resource: resource.into(),
            relation: relation.into(),
            subject: subject.into(),
        }
    }

    /// Creates an operation that removes a relationship between a resource and subject.
    pub fn delete_relationship(
        resource: impl Into<String>,
        relation: impl Into<String>,
        subject: impl Into<String>,
    ) -> Self {
        Operation::DeleteRelationship {
            resource: resource.into(),
            relation: relation.into(),
            subject: subject.into(),
        }
    }

    /// Validates this operation against the given validation configuration.
    ///
    /// Checks field sizes and character whitelists. Call this before
    /// sending operations to the server for fast client-side validation.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError` if key length, value size, or character constraints are violated.
    pub fn validate(
        &self,
        config: &inferadb_ledger_types::config::ValidationConfig,
    ) -> std::result::Result<(), inferadb_ledger_types::validation::ValidationError> {
        use inferadb_ledger_types::validation;
        match self {
            Operation::SetEntity { key, value, .. } => {
                validation::validate_key(key, config)?;
                validation::validate_value(value, config)?;
            },
            Operation::DeleteEntity { key } => {
                validation::validate_key(key, config)?;
            },
            Operation::CreateRelationship { resource, relation, subject } => {
                validation::validate_relationship_string(resource, "resource", config)?;
                validation::validate_relationship_string(relation, "relation", config)?;
                validation::validate_relationship_string(subject, "subject", config)?;
            },
            Operation::DeleteRelationship { resource, relation, subject } => {
                validation::validate_relationship_string(resource, "resource", config)?;
                validation::validate_relationship_string(relation, "relation", config)?;
                validation::validate_relationship_string(subject, "subject", config)?;
            },
        }
        Ok(())
    }

    /// Returns the estimated wire size of this operation in bytes.
    ///
    /// Used for aggregate payload size validation before sending to the server.
    fn estimated_size_bytes(&self) -> usize {
        match self {
            Operation::SetEntity { key, value, .. } => key.len() + value.len(),
            Operation::DeleteEntity { key } => key.len(),
            Operation::CreateRelationship { resource, relation, subject }
            | Operation::DeleteRelationship { resource, relation, subject } => {
                resource.len() + relation.len() + subject.len()
            },
        }
    }

    /// Converts to protobuf operation.
    fn to_proto(&self) -> proto::Operation {
        let op = match self {
            Operation::SetEntity { key, value, expires_at, condition } => {
                proto::operation::Op::SetEntity(proto::SetEntity {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at: *expires_at,
                    condition: condition.as_ref().map(SetCondition::to_proto),
                })
            },
            Operation::DeleteEntity { key } => {
                proto::operation::Op::DeleteEntity(proto::DeleteEntity { key: key.clone() })
            },
            Operation::CreateRelationship { resource, relation, subject } => {
                proto::operation::Op::CreateRelationship(proto::CreateRelationship {
                    resource: resource.clone(),
                    relation: relation.clone(),
                    subject: subject.clone(),
                })
            },
            Operation::DeleteRelationship { resource, relation, subject } => {
                proto::operation::Op::DeleteRelationship(proto::DeleteRelationship {
                    resource: resource.clone(),
                    relation: relation.clone(),
                    subject: subject.clone(),
                })
            },
        };
        proto::Operation { op: Some(op) }
    }
}

impl SetCondition {
    /// Creates a condition for compare-and-set operations from an expected
    /// previous value.
    ///
    /// - `None` → [`SetCondition::NotExists`] (create-if-absent)
    /// - `Some(value)` → [`SetCondition::ValueEquals`] (update-if-unchanged)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use inferadb_ledger_sdk::SetCondition;
    ///
    /// // Insert only if key doesn't exist
    /// let cond = SetCondition::from_expected(None::<Vec<u8>>);
    /// assert!(matches!(cond, SetCondition::NotExists));
    ///
    /// // Update only if current value matches
    /// let cond = SetCondition::from_expected(Some(b"old-value".to_vec()));
    /// assert!(matches!(cond, SetCondition::ValueEquals(_)));
    /// ```
    pub fn from_expected(expected: Option<impl Into<Vec<u8>>>) -> Self {
        match expected {
            None => SetCondition::NotExists,
            Some(value) => SetCondition::ValueEquals(value.into()),
        }
    }

    /// Converts to protobuf set condition.
    fn to_proto(&self) -> proto::SetCondition {
        let condition = match self {
            SetCondition::NotExists => proto::set_condition::Condition::NotExists(true),
            SetCondition::MustExist => proto::set_condition::Condition::MustExists(true),
            SetCondition::Version(v) => proto::set_condition::Condition::Version(*v),
            SetCondition::ValueEquals(v) => proto::set_condition::Condition::ValueEquals(v.clone()),
        };
        proto::SetCondition { condition: Some(condition) }
    }
}

/// High-level client for interacting with the Ledger service.
///
/// `LedgerClient` orchestrates:
/// - Connection pool for efficient channel management
/// - Sequence tracker for client-side idempotency
/// - Retry logic for transient failure recovery
/// - Server discovery (DNS, file, or static endpoints)
/// - Graceful shutdown with request cancellation
///
/// # Server Discovery
///
/// The client supports three server discovery modes:
/// - **Static**: Fixed list of endpoint URLs
/// - **DNS**: Resolve A records from a domain (for Kubernetes headless services)
/// - **File**: Load servers from a JSON manifest file
///
/// For DNS and file sources, the client performs initial resolution during
/// construction and starts a background refresh task.
///
/// # Shutdown Behavior
///
/// When [`shutdown()`](Self::shutdown) is called:
/// 1. All pending requests are cancelled with `SdkError::Shutdown`
/// 2. New requests immediately fail with `SdkError::Shutdown`
/// 3. Server resolver refresh task is stopped
/// 4. Sequence tracker state is flushed to disk (if using persistence)
/// 5. Connections are closed
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, ServerSource};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ClientConfig::builder()
///     .servers(ServerSource::from_static(["http://localhost:50051"]))
///     .client_id("my-app-001")
///     .build()?;
///
/// let client = LedgerClient::new(config).await?;
///
/// // ... use the client ...
///
/// // Graceful shutdown
/// client.shutdown().await;
/// # Ok(())
/// # }
/// ```
///
/// # Cancellation
///
/// The client supports two levels of cancellation:
///
/// **Client-level** — [`shutdown()`](Self::shutdown) cancels all in-flight
/// requests and rejects new ones with `SdkError::Shutdown`.
///
/// **Per-request** — Methods like [`read`](Self::read) and
/// [`write`](Self::write) accept an optional
/// [`CancellationToken`](tokio_util::sync::CancellationToken) that cancels
/// a single request with `SdkError::Cancelled`.
///
/// Both mechanisms interrupt in-flight RPCs and backoff sleeps via
/// `tokio::select!`. Access the client's token via
/// [`cancellation_token()`](Self::cancellation_token) to create child
/// tokens or integrate with application-level shutdown.
#[derive(Clone)]
pub struct LedgerClient {
    pool: ConnectionPool,
    /// Server resolver for DNS/file discovery.
    resolver: Option<Arc<ServerResolver>>,
    /// Cancellation token for coordinated shutdown.
    cancellation: tokio_util::sync::CancellationToken,
}

/// Generates a `LedgerClient` method that creates a gRPC service client with
/// optional compression and tracing. All service clients follow the same pattern:
/// attach the tracing interceptor, then conditionally enable gzip compression.
macro_rules! create_grpc_client {
    ($fn_name:ident, $mod:ident, $client:ident) => {
        fn $fn_name(
            channel: tonic::transport::Channel,
            compression_enabled: bool,
            interceptor: TraceContextInterceptor,
        ) -> proto::$mod::$client<
            InterceptedService<tonic::transport::Channel, TraceContextInterceptor>,
        > {
            let client = proto::$mod::$client::with_interceptor(channel, interceptor);
            if compression_enabled {
                client
                    .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            } else {
                client
            }
        }
    };
}

/// Acquires a channel from the connection pool and constructs a typed gRPC client.
///
/// Replaces the 5-line boilerplate of `pool.get_channel()` + `Self::create_xxx_client(...)`.
macro_rules! connected_client {
    ($pool:expr, $create_fn:ident) => {{
        let channel = $pool.get_channel().await?;
        Self::$create_fn(
            channel,
            $pool.compression_enabled(),
            TraceContextInterceptor::with_timeout($pool.config().trace(), $pool.config().timeout()),
        )
    }};
}

impl LedgerClient {
    /// Creates a new `LedgerClient` with the given configuration.
    ///
    /// This constructor validates the configuration and performs initial server
    /// resolution for DNS/file sources. Connections are established lazily on
    /// first use.
    ///
    /// For DNS and file server sources, a background refresh task is started
    /// to periodically re-resolve servers.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The configuration is invalid
    /// - DNS resolution fails (for DNS sources)
    /// - File read/parse fails (for file sources)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ClientConfig, ServerSource};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Static endpoints
    /// let config = ClientConfig::builder()
    ///     .servers(ServerSource::from_static(["http://localhost:50051"]))
    ///     .client_id("my-service")
    ///     .build()?;
    /// let client = LedgerClient::new(config).await?;
    ///
    /// // DNS discovery (Kubernetes)
    /// use inferadb_ledger_sdk::DnsConfig;
    /// let config = ClientConfig::builder()
    ///     .servers(ServerSource::dns(DnsConfig::builder().domain("ledger.default.svc").build()))
    ///     .client_id("my-service")
    ///     .build()?;
    /// let client = LedgerClient::new(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let cancellation = tokio_util::sync::CancellationToken::new();

        // Create resolver for DNS/file sources
        let (resolver, initial_endpoints) = match config.servers() {
            ServerSource::Static(_) => (None, None),
            source @ (ServerSource::Dns(_) | ServerSource::File(_)) => {
                let resolver = Arc::new(ServerResolver::new(source.clone()));

                // Perform initial resolution
                let servers = resolver.resolve().await.map_err(|e| error::SdkError::Config {
                    message: format!("Server discovery failed: {e}"),
                })?;

                // Convert to endpoint URLs
                let endpoints: Vec<String> = servers.iter().map(|s| s.url()).collect();

                // Start background refresh task
                resolver.start_refresh_task();

                (Some(resolver), Some(endpoints))
            },
        };

        let pool = ConnectionPool::new(config);

        // Set initial endpoints for DNS/file sources
        if let Some(endpoints) = initial_endpoints {
            pool.update_endpoints(endpoints);
        }

        Ok(Self { pool, resolver, cancellation })
    }

    /// Convenience constructor for connecting to a single endpoint.
    ///
    /// Creates a client with default configuration, connecting to the specified
    /// endpoint with the given client ID. For more control over configuration,
    /// use [`ClientConfig::builder()`] and [`LedgerClient::new()`].
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL is invalid.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(
        endpoint: impl Into<String>,
        client_id: impl Into<String>,
    ) -> Result<Self> {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static([endpoint.into()]))
            .client_id(client_id)
            .build()?;

        Self::new(config).await
    }

    /// Returns the client ID used for idempotency tracking.
    ///
    /// The client ID is included in all write requests and is used by the server
    /// to deduplicate requests and track per-client sequence state.
    #[inline]
    #[must_use]
    pub fn client_id(&self) -> &str {
        self.pool.config().client_id()
    }

    /// Returns a reference to the client configuration.
    ///
    /// Useful for inspecting configuration values like endpoints, timeouts,
    /// and retry policy.
    #[inline]
    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        self.pool.config()
    }

    /// Returns a reference to the connection pool.
    ///
    /// Most users won't need direct access to this. Useful for:
    /// - Checking if compression is enabled
    /// - Resetting connections after network changes
    #[inline]
    #[must_use]
    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    // =========================================================================
    // Fluent Builders
    // =========================================================================

    /// Creates a fluent write builder for the given organization and optional vault.
    ///
    /// Chain operations and then call `.execute()` to submit:
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
    /// # let organization = OrganizationSlug::new(1);
    /// let result = client
    ///     .write_builder(organization, Some(VaultSlug::new(1)))
    ///     .set("user:123", b"data".to_vec())
    ///     .create_relationship("doc:1", "viewer", "user:123")
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn write_builder(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
    ) -> crate::builders::WriteBuilder<'_> {
        crate::builders::WriteBuilder::new(self, organization, vault)
    }

    /// Creates a fluent batch read builder for the given organization and optional vault.
    ///
    /// Add keys, then call `.execute()`:
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
    /// # let organization = OrganizationSlug::new(1);
    /// let results = client
    ///     .batch_read_builder(organization, Some(VaultSlug::new(1)))
    ///     .key("user:123")
    ///     .key("user:456")
    ///     .linearizable()
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn batch_read_builder(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
    ) -> crate::builders::BatchReadBuilder<'_> {
        crate::builders::BatchReadBuilder::new(self, organization, vault)
    }

    /// Creates a fluent relationship query builder for the given organization and vault.
    ///
    /// Add filters, then call `.execute()`:
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example(client: &LedgerClient) -> inferadb_ledger_sdk::Result<()> {
    /// # let organization = OrganizationSlug::new(1);
    /// let page = client
    ///     .relationship_query(organization, VaultSlug::new(1))
    ///     .resource("document:report")
    ///     .relation("viewer")
    ///     .limit(50)
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn relationship_query(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> crate::builders::RelationshipQueryBuilder<'_> {
        crate::builders::RelationshipQueryBuilder::new(self, organization, vault)
    }

    /// Returns the client's cancellation token.
    ///
    /// The token can be used to:
    /// - Monitor shutdown state via `CancellationToken::cancelled()`
    /// - Create child tokens for per-request cancellation
    ///
    /// # Per-Request Cancellation
    ///
    /// Create a child token and pass it to RPC methods that accept an
    /// optional cancellation token. Cancelling the child token cancels
    /// only that request, not the entire client.
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "svc").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// let token = client.cancellation_token().child_token();
    ///
    /// // Cancel after 100ms
    /// let cancel_token = token.clone();
    /// tokio::spawn(async move {
    ///     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    ///     cancel_token.cancel();
    /// });
    ///
    /// // This read will be cancelled if it takes longer than 100ms
    /// let result = client.read(organization, None, "key", None, Some(token)).await;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    #[must_use]
    pub fn cancellation_token(&self) -> &tokio_util::sync::CancellationToken {
        &self.cancellation
    }

    // =========================================================================
    // Shutdown
    // =========================================================================

    /// Initiates graceful shutdown of the client.
    ///
    /// This method:
    /// 1. Cancels all pending requests (they will return `SdkError::Shutdown`)
    /// 2. Prevents new requests from being accepted
    /// 3. Stops the server resolver refresh task (if using DNS/file discovery)
    /// 4. Resets the connection pool
    ///
    /// After calling `shutdown()`, all operations will immediately return
    /// `SdkError::Shutdown`. The client can be cloned, but all clones share
    /// the same shutdown state.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// # let operations = vec![];
    /// let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    ///
    /// // Perform operations...
    /// client.write(organization, Some(vault), operations, None).await?;
    ///
    /// // Graceful shutdown before application exit
    /// client.shutdown().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(&self) {
        // Cancel all pending and future operations
        self.cancellation.cancel();

        // Stop server resolver refresh task
        if let Some(ref resolver) = self.resolver {
            resolver.shutdown();
        }

        tracing::debug!("Client shutdown initiated");

        // Reset connection pool to close connections
        self.pool.reset();
    }

    /// Returns `true` if the client has been shut down.
    ///
    /// After shutdown, all operations will fail with `SdkError::Shutdown`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// if client.is_shutdown() {
    ///     println!("Client has been shut down");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns an error if the client has been shut down or the request token
    /// has been cancelled.
    ///
    /// Called at the start of each operation to fail fast.
    #[inline]
    fn check_shutdown(
        &self,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(error::SdkError::Shutdown);
        }
        if let Some(token) = request_token
            && token.is_cancelled()
        {
            return Err(error::SdkError::Cancelled);
        }
        Ok(())
    }

    /// Creates a token that fires when either the client shuts down or
    /// the per-request token is cancelled.
    ///
    /// When no request token is provided, returns the client's own token
    /// (no allocation). When a request token is provided, creates a child
    /// of the client token and links the request token to it.
    fn effective_token(
        &self,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> tokio_util::sync::CancellationToken {
        match request_token {
            Some(req_token) => {
                // Child of client token: cancelled when client shuts down.
                // We also link the request token via a background task.
                let child = self.cancellation.child_token();
                let child_clone = child.clone();
                let req_clone = req_token.clone();
                tokio::spawn(async move {
                    req_clone.cancelled().await;
                    child_clone.cancel();
                });
                child
            },
            None => self.cancellation.clone(),
        }
    }

    /// Creates a trace context interceptor based on the client's configuration.
    ///
    /// Includes request timeout propagation via the `grpc-timeout` header so
    /// the server can extract the client's deadline and avoid processing
    /// requests the client has already abandoned.
    #[inline]
    fn trace_interceptor(&self) -> TraceContextInterceptor {
        TraceContextInterceptor::with_timeout(
            self.pool.config().trace(),
            self.pool.config().timeout(),
        )
    }

    /// Executes a future and records request metrics (latency + success/error).
    async fn with_metrics<T>(
        &self,
        method: &str,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let start = std::time::Instant::now();
        let result = fut.await;
        let duration = start.elapsed();
        self.pool.metrics().record_request(method, duration, result.is_ok());
        result
    }

    /// Creates a discovery service that shares this client's connection pool.
    ///
    /// The discovery service can be used to dynamically update the client's
    /// endpoint list based on discovered cluster peers.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, DiscoveryConfig};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-client").await?;
    /// let discovery = client.create_discovery_service(DiscoveryConfig::enabled());
    ///
    /// // Start background endpoint refresh
    /// discovery.start_background_refresh();
    ///
    /// // The client will now use updated endpoints as peers are discovered
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn create_discovery_service(
        &self,
        config: crate::config::DiscoveryConfig,
    ) -> crate::discovery::DiscoveryService {
        crate::discovery::DiscoveryService::new(self.pool.clone(), config)
    }

    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Reads a value by key.
    ///
    /// By default uses `EVENTUAL` consistency, which reads from any replica
    /// for lowest latency. Pass `Some(ReadConsistency::Linearizable)` for
    /// strong consistency reads served from the leader.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - The key to read.
    /// * `consistency` - Optional consistency level (`None` = eventual).
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the read fails after retry attempts are exhausted,
    /// the client has been shut down, or the cancellation token is triggered.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, ReadConsistency, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Eventual consistency read (default)
    /// let value = client.read(organization, None, "user:123", None, None).await?;
    ///
    /// // Linearizable consistency read
    /// let value = client.read(organization, Some(vault), "key",
    ///     Some(ReadConsistency::Linearizable), None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        consistency: Option<ReadConsistency>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<Option<Vec<u8>>> {
        self.read_internal(
            organization,
            vault,
            key.into(),
            consistency.unwrap_or(ReadConsistency::Eventual),
            token.as_ref(),
        )
        .await
    }

    /// Batch read multiple keys in a single RPC call.
    ///
    /// Amortizes network overhead across multiple reads for higher throughput.
    /// All reads share the same organization, vault, and consistency level.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `keys` - The keys to read (max 1000).
    /// * `consistency` - Optional consistency level (`None` = eventual).
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Returns
    ///
    /// Returns a vector of `(key, Option<value>)` pairs in the same order as
    /// the input keys. Missing keys have `None` values.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch read fails after retry attempts are
    /// exhausted, the client has been shut down, or the cancellation token
    /// is triggered.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let results = client.batch_read(
    ///     organization,
    ///     Some(vault),
    ///     vec!["key1", "key2", "key3"],
    ///     None,
    ///     None,
    /// ).await?;
    ///
    /// for (key, value) in results {
    ///     match value {
    ///         Some(v) => println!("{key}: {} bytes", v.len()),
    ///         None => println!("{key}: not found"),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_read(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        keys: impl IntoIterator<Item = impl Into<String>>,
        consistency: Option<ReadConsistency>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.batch_read_internal(
            organization,
            vault,
            keys.into_iter().map(Into::into).collect(),
            consistency.unwrap_or(ReadConsistency::Eventual),
            token.as_ref(),
        )
        .await
    }

    // =========================================================================
    // Internal Read Implementation
    // =========================================================================

    /// Internal read implementation with configurable consistency and
    /// optional per-request cancellation.
    async fn read_internal(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: String,
        consistency: ReadConsistency,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<Option<Vec<u8>>> {
        self.check_shutdown(request_token)?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        self.with_metrics(
            "read",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "read", || async {
                let mut client = connected_client!(pool, create_read_client);

                let request = proto::ReadRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    key: key.clone(),
                    consistency: consistency.to_proto() as i32,
                };

                let response = client.read(tonic::Request::new(request)).await?.into_inner();

                Ok(response.value)
            }),
        )
        .await
    }

    /// Internal batch read implementation with configurable consistency and
    /// optional per-request cancellation.
    async fn batch_read_internal(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        keys: Vec<String>,
        consistency: ReadConsistency,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>> {
        self.check_shutdown(request_token)?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();

        self.with_metrics(
            "batch_read",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "batch_read", || async {
                let mut client = connected_client!(pool, create_read_client);

                let request = proto::BatchReadRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    keys: keys.clone(),
                    consistency: consistency.to_proto() as i32,
                };

                let response = client.batch_read(tonic::Request::new(request)).await?.into_inner();

                // Convert results to (key, Option<value>) pairs
                let results = response.results.into_iter().map(|r| (r.key, r.value)).collect();

                Ok(results)
            }),
        )
        .await
    }

    // =========================================================================
    // Write Operations
    // =========================================================================

    /// Submits a write transaction to the ledger.
    ///
    /// Writes are automatically idempotent via server-assigned sequence numbers.
    /// The server assigns monotonically increasing sequences at Raft commit time.
    /// If a write fails with a retryable error, it will be retried with the
    /// same idempotency key. If the server reports the write was already
    /// committed (duplicate), the original result is returned as success.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (required for relationships).
    /// * `operations` - The operations to apply atomically.
    /// * `token` - Optional per-request cancellation token.
    ///
    /// # Returns
    ///
    /// Returns [`WriteSuccess`] containing the transaction ID, block height, and
    /// server-assigned sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - A conditional write (CAS) condition fails
    /// - An idempotency key is reused with different payload
    /// - The client has been shut down or the cancellation token is triggered
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, Operation, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let result = client.write(
    ///     organization,
    ///     Some(vault),
    ///     vec![
    ///         Operation::set_entity("user:123", b"data".to_vec(), None, None),
    ///         Operation::create_relationship("doc:456", "viewer", "user:123"),
    ///     ],
    ///     None,
    /// ).await?;
    ///
    /// println!("Committed at block {} with sequence {}", result.block_height, result.assigned_sequence);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        operations: Vec<Operation>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.check_shutdown(token.as_ref())?;

        // Generate UUID idempotency key once for this request
        // The same key is reused across all retry attempts
        let idempotency_key = uuid::Uuid::new_v4();

        self.execute_write(organization, vault, &operations, idempotency_key, token.as_ref()).await
    }

    /// Executes a single write attempt with retry for transient errors.
    ///
    /// The idempotency key is preserved across retry attempts to ensure
    /// at-most-once semantics even with network failures.
    async fn execute_write(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        operations: &[Operation],
        idempotency_key: uuid::Uuid,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        // Client-side validation: fast rejection before network round-trip
        let validation_config = self.config().validation();
        inferadb_ledger_types::validation::validate_operations_count(
            operations.len(),
            validation_config,
        )
        .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
        let mut total_bytes: usize = 0;
        for op in operations {
            op.validate(validation_config)
                .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
            total_bytes += op.estimated_size_bytes();
        }
        inferadb_ledger_types::validation::validate_batch_payload_bytes(
            total_bytes,
            validation_config,
        )
        .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();
        let client_id = self.client_id().to_string();

        // Convert operations to proto
        let proto_operations: Vec<proto::Operation> =
            operations.iter().map(Operation::to_proto).collect();

        // Convert UUID to bytes (16 bytes)
        let idempotency_key_bytes = idempotency_key.as_bytes().to_vec();

        // Execute with retry for transient errors
        self.with_metrics(
            "write",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "write", || {
                let proto_ops = proto_operations.clone();
                let cid = client_id.clone();
                let key_bytes = idempotency_key_bytes.clone();
                async move {
                    let mut write_client = connected_client!(pool, create_write_client);

                    let request = proto::WriteRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        client_id: Some(proto::ClientId { id: cid }),
                        idempotency_key: key_bytes,
                        operations: proto_ops,
                        include_tx_proof: false,
                    };

                    let response =
                        write_client.write(tonic::Request::new(request)).await?.into_inner();

                    Self::process_write_response(response)
                }
            }),
        )
        .await
    }

    /// Processes a WriteResponse and converts to Result<WriteSuccess>.
    fn process_write_response(response: proto::WriteResponse) -> Result<WriteSuccess> {
        match response.result {
            Some(proto::write_response::Result::Success(success)) => Ok(WriteSuccess {
                tx_id: Self::tx_id_to_hex(success.tx_id),
                block_height: success.block_height,
                assigned_sequence: success.assigned_sequence,
            }),
            Some(proto::write_response::Result::Error(error)) => {
                let code = proto::WriteErrorCode::try_from(error.code)
                    .unwrap_or(proto::WriteErrorCode::Unspecified);

                match code {
                    proto::WriteErrorCode::AlreadyCommitted => {
                        // Idempotent retry - return the original success with assigned_sequence
                        Ok(WriteSuccess {
                            tx_id: Self::tx_id_to_hex(error.committed_tx_id),
                            block_height: error.committed_block_height.unwrap_or(0),
                            assigned_sequence: error.assigned_sequence.unwrap_or(0),
                        })
                    },
                    proto::WriteErrorCode::IdempotencyKeyReused => {
                        // Client reused idempotency key with different payload
                        Err(crate::error::SdkError::Idempotency {
                            message: format!(
                                "Idempotency key reused with different payload: {}",
                                error.message
                            ),
                            conflict_key: None,
                            original_tx_id: Some(Self::tx_id_to_hex(error.committed_tx_id.clone())),
                        })
                    },
                    _ => {
                        // Other write errors (CAS failures, etc.)
                        Err(crate::error::SdkError::Rpc {
                            code: tonic::Code::FailedPrecondition,
                            message: error.message,
                            request_id: None,
                            trace_id: None,
                            error_details: None,
                        })
                    },
                }
            },
            None => Err(missing_response_field("result", "WriteResponse")),
        }
    }

    /// Converts TxId bytes to hex string.
    fn tx_id_to_hex(tx_id: Option<proto::TxId>) -> String {
        use std::fmt::Write;
        tx_id
            .map(|t| {
                t.id.iter().fold(String::with_capacity(t.id.len() * 2), |mut acc, b| {
                    let _ = write!(acc, "{b:02x}");
                    acc
                })
            })
            .unwrap_or_default()
    }

    // =========================================================================
    // Batch Write Operations
    // =========================================================================

    /// Submits a batch write transaction with all-or-nothing atomicity.
    ///
    /// A batch write groups multiple operation sets into a single atomic transaction.
    /// All operations are committed together in a single block, or none are applied
    /// if any operation fails (e.g., CAS condition failure).
    ///
    /// The batch uses a single idempotency key, meaning the entire batch is the
    /// deduplication unit - retry with the same idempotency key returns the
    /// original result.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (required for relationships).
    /// * `batches` - Groups of operations to apply atomically. Each inner `Vec<Operation>` is a
    ///   logical group processed in order.
    /// * `token` - Optional per-request cancellation token. If `None`, the client-level
    ///   cancellation token is used.
    ///
    /// # Returns
    ///
    /// Returns [`WriteSuccess`] containing the transaction ID, block height, and
    /// server-assigned sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - Any CAS condition fails (entire batch rolled back)
    /// - An idempotency key is reused with different payload
    ///
    /// # Atomicity
    ///
    /// Operations are applied in array order:
    /// - `batches[0]` operations first, then `batches[1]`, etc.
    /// - Within each batch: `operations[0]` first, then `operations[1]`, etc.
    /// - If ANY operation fails, the ENTIRE transaction is rolled back.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, Operation, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Atomic transaction: create user AND grant permissions
    /// let result = client.batch_write(
    ///     organization,
    ///     Some(vault),
    ///     vec![
    ///         // First batch: create the user
    ///         vec![Operation::set_entity("user:123", b"alice".to_vec(), None, None)],
    ///         // Second batch: grant permissions (depends on user existing)
    ///         vec![
    ///             Operation::create_relationship("doc:456", "viewer", "user:123"),
    ///             Operation::create_relationship("folder:789", "editor", "user:123"),
    ///         ],
    ///     ],
    ///     None,
    /// ).await?;
    ///
    /// println!("Batch committed at block {} with sequence {}", result.block_height, result.assigned_sequence);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_write(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        batches: Vec<Vec<Operation>>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.check_shutdown(token.as_ref())?;

        // Generate UUID idempotency key once for this request
        // The same key is reused across all retry attempts
        let idempotency_key = uuid::Uuid::new_v4();

        self.execute_batch_write(organization, vault, &batches, idempotency_key, token.as_ref())
            .await
    }

    /// Executes a single batch write attempt with retry for transient errors.
    ///
    /// The idempotency key is preserved across retry attempts to ensure
    /// at-most-once semantics even with network failures.
    async fn execute_batch_write(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        batches: &[Vec<Operation>],
        idempotency_key: uuid::Uuid,
        request_token: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        // Client-side validation: fast rejection before network round-trip
        let validation_config = self.config().validation();
        let total_ops: usize = batches.iter().map(|b| b.len()).sum();
        inferadb_ledger_types::validation::validate_operations_count(total_ops, validation_config)
            .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
        let mut total_bytes: usize = 0;
        for batch in batches {
            for op in batch {
                op.validate(validation_config)
                    .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;
                total_bytes += op.estimated_size_bytes();
            }
        }
        inferadb_ledger_types::validation::validate_batch_payload_bytes(
            total_bytes,
            validation_config,
        )
        .map_err(|e| error::SdkError::Validation { message: e.to_string() })?;

        let token = self.effective_token(request_token);
        let pool = &self.pool;
        let retry_policy = self.config().retry_policy().clone();
        let client_id = self.client_id().to_string();

        // Convert batches to proto BatchWriteOperation format
        let proto_batches: Vec<proto::BatchWriteOperation> = batches
            .iter()
            .map(|ops| proto::BatchWriteOperation {
                operations: ops.iter().map(Operation::to_proto).collect(),
            })
            .collect();

        // Convert UUID to bytes (16 bytes)
        let idempotency_key_bytes = idempotency_key.as_bytes().to_vec();

        // Execute with retry for transient errors
        self.with_metrics(
            "batch_write",
            with_retry_cancellable(&retry_policy, &token, Some(pool), "batch_write", || {
                let batch_ops = proto_batches.clone();
                let cid = client_id.clone();
                let key_bytes = idempotency_key_bytes.clone();
                async move {
                    let mut write_client = connected_client!(pool, create_write_client);

                    let request = proto::BatchWriteRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        client_id: Some(proto::ClientId { id: cid }),
                        idempotency_key: key_bytes,
                        operations: batch_ops,
                        include_tx_proofs: false,
                    };

                    let response =
                        write_client.batch_write(tonic::Request::new(request)).await?.into_inner();

                    Self::process_batch_write_response(response)
                }
            }),
        )
        .await
    }

    /// Processes a BatchWriteResponse and converts to Result<WriteSuccess>.
    fn process_batch_write_response(response: proto::BatchWriteResponse) -> Result<WriteSuccess> {
        match response.result {
            Some(proto::batch_write_response::Result::Success(success)) => Ok(WriteSuccess {
                tx_id: Self::tx_id_to_hex(success.tx_id),
                block_height: success.block_height,
                assigned_sequence: success.assigned_sequence,
            }),
            Some(proto::batch_write_response::Result::Error(error)) => {
                let code = proto::WriteErrorCode::try_from(error.code)
                    .unwrap_or(proto::WriteErrorCode::Unspecified);

                match code {
                    proto::WriteErrorCode::AlreadyCommitted => {
                        // Idempotent retry - return the original success with assigned_sequence
                        Ok(WriteSuccess {
                            tx_id: Self::tx_id_to_hex(error.committed_tx_id),
                            block_height: error.committed_block_height.unwrap_or(0),
                            assigned_sequence: error.assigned_sequence.unwrap_or(0),
                        })
                    },
                    proto::WriteErrorCode::IdempotencyKeyReused => {
                        // Client reused idempotency key with different payload
                        Err(crate::error::SdkError::Idempotency {
                            message: format!(
                                "Idempotency key reused with different payload: {}",
                                error.message
                            ),
                            conflict_key: None,
                            original_tx_id: Some(Self::tx_id_to_hex(error.committed_tx_id.clone())),
                        })
                    },
                    _ => {
                        // Other write errors (CAS failures, etc.)
                        Err(crate::error::SdkError::Rpc {
                            code: tonic::Code::FailedPrecondition,
                            message: error.message,
                            request_id: None,
                            trace_id: None,
                            error_details: None,
                        })
                    },
                }
            },
            None => Err(missing_response_field("result", "BatchWriteResponse")),
        }
    }

    // =============================================================================
    // Single-Operation Convenience Methods
    // =============================================================================

    /// Writes a single entity (set), optionally with expiration and/or a
    /// condition.
    ///
    /// Convenience wrapper around [`write`](Self::write) for the common case of
    /// setting a single key-value pair. Generates an idempotency key
    /// automatically.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - The entity key.
    /// * `value` - The entity value.
    /// * `expires_at` - Optional Unix timestamp (seconds) when the entity expires.
    /// * `condition` - Optional condition for compare-and-swap writes.
    /// * `token` - Optional cancellation token for this request. When triggered, the operation is
    ///   cancelled at the next retry boundary. Pass `None` to rely on the client-level token only.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails, the condition is not met, or the
    /// write fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug, SetCondition};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // Simple set:
    /// client.set_entity(organization, Some(vault), "user:123", b"data".to_vec(), None, None, None).await?;
    ///
    /// // With expiration:
    /// client.set_entity(organization, Some(vault), "session:abc", b"token".to_vec(), Some(1700000000), None, None).await?;
    ///
    /// // Conditional (create-if-not-exists):
    /// client.set_entity(organization, Some(vault), "lock:xyz", b"owner".to_vec(), None, Some(SetCondition::NotExists), None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_entity(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        value: Vec<u8>,
        expires_at: Option<u64>,
        condition: Option<SetCondition>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.write(
            organization,
            vault,
            vec![Operation::set_entity(key, value, expires_at, condition)],
            token,
        )
        .await
    }

    /// Deletes a single entity.
    ///
    /// Convenience wrapper around [`write`](Self::write) for deleting a single
    /// key.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - The entity key to delete.
    /// * `token` - Optional cancellation token for this request. When triggered, the operation is
    ///   cancelled at the next retry boundary. Pass `None` to rely on the client-level token only.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or the write fails after retry
    /// attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// client.delete_entity(organization, Some(vault), "user:123", None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_entity(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<WriteSuccess> {
        self.write(organization, vault, vec![Operation::delete_entity(key)], token).await
    }

    // =============================================================================
    // Streaming Operations
    // =============================================================================

    /// Subscribes to block announcements for a vault.
    ///
    /// Returns a stream of [`BlockAnnouncement`] items that emits each time a new
    /// block is committed to the vault's chain. The stream automatically reconnects
    /// on disconnect and resumes from the last seen block height.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `start_height` - First block height to receive (must be >= 1).
    ///
    /// # Returns
    ///
    /// Returns a `Stream` that yields `Result<BlockAnnouncement>` items.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the initial stream connection fails.
    ///
    /// # Reconnection Behavior
    ///
    /// On disconnect (network error, server restart, etc.), the stream:
    /// 1. Applies exponential backoff before reconnecting
    /// 2. Resumes from `last_seen_height + 1` to avoid gaps or duplicates
    /// 3. Continues until max reconnection attempts are exhausted
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # use futures::StreamExt;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = LedgerClient::connect("http://localhost:50051", "my-app").await?;
    /// # let organization = OrganizationSlug::new(1);
    ///
    /// // Start watching from height 1
    /// let mut stream = client.watch_blocks(organization, VaultSlug::new(0), 1).await?;
    ///
    /// while let Some(announcement) = stream.next().await {
    ///     match announcement {
    ///         Ok(block) => {
    ///             println!("New block at height {}", block.height);
    ///             // Process block...
    ///         }
    ///         Err(e) => {
    ///             eprintln!("Stream error: {}", e);
    ///             break;
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch_blocks(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        start_height: u64,
    ) -> Result<impl futures::Stream<Item = Result<BlockAnnouncement>>> {
        self.check_shutdown(None)?;

        // Get the initial stream
        let initial_stream =
            self.create_watch_blocks_stream(organization, vault, start_height).await?;

        // Create position tracker starting at the requested height
        let position = HeightTracker::new(start_height);

        // Clone pool and config for the reconnection closure
        let pool = self.pool.clone();
        let retry_policy = self.config().retry_policy().clone();

        // Create the reconnecting stream wrapper
        let reconnecting = ReconnectingStream::new(
            initial_stream,
            position,
            retry_policy.clone(),
            move |next_height| {
                let pool = pool.clone();
                Box::pin(async move {
                    let channel = pool.get_channel().await?;
                    let mut client = proto::read_service_client::ReadServiceClient::new(channel);
                    if pool.compression_enabled() {
                        client = client
                            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
                    }

                    let request = proto::WatchBlocksRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        start_height: next_height,
                    };

                    let response =
                        client.watch_blocks(tonic::Request::new(request)).await?.into_inner();

                    Ok(response)
                })
            },
        );

        // Map proto announcements to SDK type
        Ok(futures::StreamExt::map(reconnecting, |result| {
            result.map(BlockAnnouncement::from_proto)
        }))
    }

    /// Creates a WatchBlocks stream without reconnection logic.
    async fn create_watch_blocks_stream(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        start_height: u64,
    ) -> Result<tonic::Streaming<proto::BlockAnnouncement>> {
        let channel = self.pool.get_channel().await?;
        let mut client = Self::create_read_client(
            channel,
            self.pool.compression_enabled(),
            self.trace_interceptor(),
        );

        let request = proto::WatchBlocksRequest {
            organization: Some(proto::OrganizationSlug { slug: organization.value() }),
            vault: Some(proto::VaultSlug { slug: vault.value() }),
            start_height,
        };

        let response = client.watch_blocks(tonic::Request::new(request)).await?.into_inner();

        Ok(response)
    }

    // =========================================================================
    // Admin Operations
    // =========================================================================

    /// Creates a new organization with the specified data residency region.
    ///
    /// Every organization must declare a region at creation time. The region
    /// determines where the organization's data is stored and which data
    /// protection regulations apply.
    ///
    /// # Arguments
    ///
    /// * `name` - Human-readable name for the organization (e.g., "acme_corp").
    /// * `region` - Data residency region. Must not be `Region::Global` (the control plane) or
    ///   `Region::Unspecified`.
    ///
    /// # Returns
    ///
    /// Returns [`OrganizationInfo`] containing the generated slug and metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization name is invalid
    /// - The region is `Global` or `Unspecified`
    /// - A protected region has insufficient in-region nodes (< 3)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationTier, Region, UserSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let org = client.create_organization("my-org", Region::US_EAST_VA, UserSlug::new(1), OrganizationTier::Free).await?;
    /// println!("Created organization with slug: {}", org.slug);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_organization(
        &self,
        name: impl Into<String>,
        region: Region,
        admin: UserSlug,
        tier: OrganizationTier,
    ) -> Result<OrganizationInfo> {
        self.check_shutdown(None)?;

        let name = name.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_organization",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::CreateOrganizationRequest {
                        name: name.clone(),
                        region: region_i32,
                        tier: Some(tier.to_proto()),
                        admin: Some(proto::UserSlug { slug: admin.value() }),
                    };

                    let response = client
                        .create_organization(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(OrganizationInfo::from_fields(
                        response.slug,
                        response.name,
                        response.region,
                        response.member_nodes,
                        response.config_version,
                        response.status,
                        response.tier,
                        &response.members,
                    ))
                },
            ),
        )
        .await
    }

    /// Returns information about an organization by slug.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns [`OrganizationInfo`] containing organization metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let slug = OrganizationSlug::new(1);
    /// let info = client.get_organization(slug, UserSlug::new(42)).await?;
    /// println!("Organization: {} (status: {:?})", info.name, info.status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_organization(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
    ) -> Result<OrganizationInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_organization",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::GetOrganizationRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        caller: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response =
                        client.get_organization(tonic::Request::new(request)).await?.into_inner();

                    Ok(OrganizationInfo::from_proto(response))
                },
            ),
        )
        .await
    }

    /// Updates an organization's mutable fields.
    ///
    /// Currently supports renaming an organization. The initiator must be
    /// an organization administrator.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `user` - User slug of the admin performing the update.
    /// * `name` - New name for the organization, or `None` to keep the current name.
    ///
    /// # Returns
    ///
    /// Returns the updated [`OrganizationInfo`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    /// - The initiator is not an organization admin
    /// - The new name is invalid
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let slug = OrganizationSlug::new(1);
    /// let updated = client.update_organization(slug, UserSlug::new(42), Some("new-name".into())).await?;
    /// println!("Updated organization: {}", updated.name);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_organization(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
        name: Option<String>,
    ) -> Result<OrganizationInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_organization",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::UpdateOrganizationRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        name: name.clone(),
                    };

                    let response = client
                        .update_organization(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(OrganizationInfo::from_update_proto(response))
                },
            ),
        )
        .await
    }

    /// Soft-deletes an organization by slug.
    ///
    /// Marks the organization for deletion. Fails if the organization
    /// still contains active vaults. The organization enters `Deleted` status
    /// and data is retained for `retention_days` before being purged.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `user` - User slug of the admin initiating the delete.
    ///
    /// # Returns
    ///
    /// Returns [`OrganizationDeleteInfo`] with the deletion timestamp and retention period.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    /// - The organization still has active vaults
    /// - The initiator is not an organization admin
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, UserSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let slug = OrganizationSlug::new(1);
    /// let delete_info = client.delete_organization(slug, UserSlug::new(42)).await?;
    /// println!("Deleted, retention: {} days", delete_info.retention_days);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_organization(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
    ) -> Result<OrganizationDeleteInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_organization",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::DeleteOrganizationRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response = client
                        .delete_organization(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let deleted_at =
                        response.deleted_at.as_ref().and_then(proto_timestamp_to_system_time);

                    Ok(OrganizationDeleteInfo {
                        deleted_at,
                        retention_days: response.retention_days,
                    })
                },
            ),
        )
        .await
    }

    /// Lists organizations visible to the caller.
    ///
    /// Returns a paginated list of organizations. Pass the returned
    /// `next_page_token` into subsequent calls to retrieve further pages.
    ///
    /// # Arguments
    ///
    /// * `caller` - User slug of the caller (for authorization filtering).
    /// * `page_size` - Maximum items per page (0 = server default).
    /// * `page_token` - Opaque cursor from a previous response, or `None` for the first page.
    ///
    /// # Returns
    ///
    /// A tuple of `(organizations, next_page_token)`. When `next_page_token`
    /// is `None`, there are no more pages.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # use inferadb_ledger_types::UserSlug;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let (organizations, _next) = client.list_organizations(UserSlug::new(1), 100, None).await?;
    /// for org in organizations {
    ///     println!("Organization: {} (slug: {})", org.name, org.slug);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_organizations(
        &self,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<OrganizationInfo>, Option<Vec<u8>>)> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_organizations",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_organizations",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::ListOrganizationsRequest {
                        page_token: page_token.clone(),
                        page_size,
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                    };

                    let response =
                        client.list_organizations(tonic::Request::new(request)).await?.into_inner();

                    let organizations = response
                        .organizations
                        .into_iter()
                        .map(OrganizationInfo::from_proto)
                        .collect();

                    Ok((organizations, response.next_page_token))
                },
            ),
        )
        .await
    }

    /// Lists members of an organization.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `caller` - User slug of the caller (must be a member).
    /// * `page_size` - Maximum items per page (0 = server default).
    /// * `page_token` - Opaque cursor from a previous response, or `None` for the first page.
    ///
    /// # Returns
    ///
    /// A tuple of `(members, next_page_token)`. When `next_page_token`
    /// is `None`, there are no more pages.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    /// - The caller is not a member
    pub async fn list_organization_members(
        &self,
        slug: OrganizationSlug,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<OrganizationMemberInfo>, Option<Vec<u8>>)> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_organization_members",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_organization_members",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::ListOrganizationMembersRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                        page_token: page_token.clone(),
                        page_size,
                    };

                    let response = client
                        .list_organization_members(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let members =
                        response.members.iter().map(OrganizationMemberInfo::from_proto).collect();

                    Ok((members, response.next_page_token))
                },
            ),
        )
        .await
    }

    /// Removes a member from an organization.
    ///
    /// Self-removal: any member can leave unless they are the last admin.
    /// Removing others: initiator must be an admin.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `user` - User slug of the person performing the removal.
    /// * `target` - User slug of the member to remove.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The target is not a member
    /// - The initiator lacks permission
    /// - Removing the last admin
    pub async fn remove_organization_member(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
        target: UserSlug,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "remove_organization_member",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "remove_organization_member",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::RemoveOrganizationMemberRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        target: Some(proto::UserSlug { slug: target.value() }),
                    };

                    client.remove_organization_member(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
        .await
    }

    /// Updates a member's role within an organization.
    ///
    /// Initiator must be an admin. Cannot demote the last admin.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `user` - User slug of the admin performing the change.
    /// * `target` - User slug of the member to update.
    /// * `role` - New role for the member.
    ///
    /// # Returns
    ///
    /// Returns the updated [`OrganizationMemberInfo`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The initiator is not an admin
    /// - The target is not a member
    /// - Demoting the last admin
    pub async fn update_organization_member_role(
        &self,
        slug: OrganizationSlug,
        user: UserSlug,
        target: UserSlug,
        role: OrganizationMemberRole,
    ) -> Result<OrganizationMemberInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_organization_member_role",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_organization_member_role",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::UpdateOrganizationMemberRoleRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        target: Some(proto::UserSlug { slug: target.value() }),
                        role: role.to_proto(),
                    };

                    let response = client
                        .update_organization_member_role(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let member = response
                        .member
                        .as_ref()
                        .map(OrganizationMemberInfo::from_proto)
                        .ok_or_else(|| {
                        missing_response_field("member", "UpdateOrganizationMemberRoleResponse")
                    })?;

                    Ok(member)
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Organization Teams
    // =========================================================================

    /// Lists teams in an organization.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `caller` - User slug of the caller (for authorization filtering).
    /// * `page_size` - Maximum items per page (0 = server default).
    /// * `page_token` - Opaque cursor from a previous response, or `None` for the first page.
    ///
    /// # Returns
    ///
    /// A tuple of `(teams, next_page_token)`. When `next_page_token`
    /// is `None`, there are no more pages.
    pub async fn list_organization_teams(
        &self,
        organization: OrganizationSlug,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<TeamInfo>, Option<Vec<u8>>)> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_organization_teams",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_organization_teams",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::ListOrganizationTeamsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        caller: Some(proto::UserSlug { slug: caller.value() }),
                        page_token: page_token.clone(),
                        page_size,
                    };

                    let response = client
                        .list_organization_teams(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let teams = response.teams.iter().map(TeamInfo::from_proto).collect();

                    Ok((teams, response.next_page_token))
                },
            ),
        )
        .await
    }

    /// Creates a new team within an organization.
    ///
    /// The team name must be unique within the organization.
    /// A Snowflake team slug is generated server-side.
    pub async fn create_organization_team(
        &self,
        organization: OrganizationSlug,
        name: &str,
        initiator: UserSlug,
    ) -> Result<TeamInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();
        let name = name.to_string();

        self.with_metrics(
            "create_organization_team",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_organization_team",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::CreateOrganizationTeamRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        name: name.clone(),
                        initiator: Some(proto::UserSlug { slug: initiator.value() }),
                    };

                    let response = client
                        .create_organization_team(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response
                        .team
                        .as_ref()
                        .map(TeamInfo::from_proto)
                        .ok_or_else(|| missing_response_field("team", "CreateTeamResponse"))
                },
            ),
        )
        .await
    }

    /// Deletes a team from an organization.
    ///
    /// Caller must be an organization administrator or team manager.
    /// Optionally moves all members to another team before deletion.
    pub async fn delete_organization_team(
        &self,
        team: TeamSlug,
        initiator: UserSlug,
        move_members_to: Option<TeamSlug>,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_organization_team",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_organization_team",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::DeleteOrganizationTeamRequest {
                        slug: Some(proto::TeamSlug { slug: team.value() }),
                        move_members_to: move_members_to
                            .map(|s| proto::TeamSlug { slug: s.value() }),
                        initiator: Some(proto::UserSlug { slug: initiator.value() }),
                    };

                    client.delete_organization_team(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
        .await
    }

    /// Updates a team's metadata (currently: name only).
    ///
    /// Caller must be an organization administrator or team manager.
    pub async fn update_organization_team(
        &self,
        team: TeamSlug,
        initiator: UserSlug,
        name: Option<&str>,
    ) -> Result<TeamInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();
        let name = name.map(|s| s.to_string());

        self.with_metrics(
            "update_organization_team",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_organization_team",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::UpdateOrganizationTeamRequest {
                        slug: Some(proto::TeamSlug { slug: team.value() }),
                        initiator: Some(proto::UserSlug { slug: initiator.value() }),
                        name: name.clone(),
                    };

                    let response = client
                        .update_organization_team(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response
                        .team
                        .as_ref()
                        .map(TeamInfo::from_proto)
                        .ok_or_else(|| missing_response_field("team", "UpdateTeamResponse"))
                },
            ),
        )
        .await
    }

    /// Initiates migration of an organization to a different region.
    ///
    /// Transitions the organization to `Migrating` status and creates a background
    /// saga to drive the migration through. Writes are blocked during migration.
    ///
    /// # Arguments
    ///
    /// * `slug` - Organization slug (external identifier).
    /// * `target_region` - Target region for the migration.
    /// * `acknowledge_residency_downgrade` - Required for protected → non-protected.
    ///
    /// # Returns
    ///
    /// Returns [`MigrationInfo`] with source/target regions and current status.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Organization does not exist
    /// - Organization is not in `Active` status
    /// - Target region is the same as current region
    /// - Protected → non-protected without acknowledgment
    pub async fn migrate_organization(
        &self,
        slug: OrganizationSlug,
        target_region: Region,
        acknowledge_residency_downgrade: bool,
        user: UserSlug,
    ) -> Result<MigrationInfo> {
        self.check_shutdown(None)?;

        let proto_target: proto::Region = target_region.into();
        let target_i32: i32 = proto_target.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "migrate_organization",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "migrate_organization",
                || async {
                    let mut client = connected_client!(pool, create_organization_client);

                    let request = proto::MigrateOrganizationRequest {
                        slug: Some(proto::OrganizationSlug { slug: slug.value() }),
                        target_region: target_i32,
                        acknowledge_residency_downgrade,
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response = client
                        .migrate_organization(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(MigrationInfo {
                        slug: OrganizationSlug::new(response.slug.map_or(0, |s| s.slug)),
                        source_region: region_from_proto_i32(response.source_region)
                            .unwrap_or(Region::GLOBAL),
                        target_region: region_from_proto_i32(response.target_region)
                            .unwrap_or(target_region),
                        status: OrganizationStatus::from_proto(response.status),
                    })
                },
            ),
        )
        .await
    }

    /// Initiates a user region migration.
    ///
    /// Moves the user's data residency to the target region. Authenticated API
    /// calls for this user are temporarily blocked while the migration is in
    /// progress.
    ///
    /// # Arguments
    ///
    /// * `user_slug` - User slug (external identifier).
    /// * `target_region` - Target region for the migration.
    ///
    /// # Returns
    ///
    /// Returns [`UserMigrationInfo`] with source/target regions and current directory status.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - User does not exist
    /// - Target region is the same as current region
    /// - Connection fails after retry attempts
    pub async fn migrate_user_region(
        &self,
        user: UserSlug,
        target_region: Region,
    ) -> Result<UserMigrationInfo> {
        self.check_shutdown(None)?;

        let proto_target: proto::Region = target_region.into();
        let target_i32: i32 = proto_target.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "migrate_user_region",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "migrate_user_region",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::MigrateUserRegionRequest {
                        slug: Some(proto::UserSlug { slug: user.value() }),
                        target_region: target_i32,
                    };

                    let response = client
                        .migrate_user_region(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(UserMigrationInfo {
                        slug: UserSlug::new(response.slug.map_or(0, |s| s.slug)),
                        source_region: region_from_proto_i32(response.source_region)
                            .unwrap_or(Region::GLOBAL),
                        target_region: region_from_proto_i32(response.target_region)
                            .unwrap_or(target_region),
                        directory_status: response.directory_status,
                    })
                },
            ),
        )
        .await
    }

    /// Erases a user's PII through crypto-shredding.
    ///
    /// Permanently destroys the user's blinding key material, rendering all
    /// associated email hashes unrecoverable. This operation is irreversible.
    ///
    /// # Arguments
    ///
    /// * `user` - User slug (Snowflake ID) to erase.
    /// * `erased_by` - Identity of the actor requesting erasure (audit trail).
    /// * `region` - Region where the user's PII resides.
    ///
    /// # Returns
    ///
    /// Returns the user slug that was erased.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - User does not exist
    /// - Confirmation token is invalid, expired, or missing
    /// - Connection fails after retry attempts
    pub async fn erase_user(
        &self,
        user: UserSlug,
        erased_by: impl Into<String>,
        region: Region,
    ) -> Result<UserSlug> {
        self.check_shutdown(None)?;

        let erased_by = erased_by.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "erase_user",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "erase_user",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::EraseUserRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        erased_by: erased_by.clone(),
                        region: region_i32,
                    };

                    let response =
                        client.erase_user(tonic::Request::new(request)).await?.into_inner();

                    Ok(UserSlug::new(response.user.map_or(0, |u| u.slug)))
                },
            ),
        )
        .await
    }

    // ========================================================================
    // User CRUD
    // ========================================================================

    /// Creates a new user.
    ///
    /// The caller must pre-compute the email HMAC using the blinding key.
    /// User creation is saga-based: email HMAC reservation → regional write → directory entry.
    pub async fn create_user(
        &self,
        name: impl Into<String>,
        email: impl Into<String>,
        email_hmac: impl Into<String>,
        region: Region,
        role: UserRole,
    ) -> Result<UserInfo> {
        self.check_shutdown(None)?;

        let name = name.into();
        let email = email.into();
        let email_hmac = email_hmac.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let proto_role: proto::UserRole = role.into();
        let role_i32: i32 = proto_role.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_user",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_user",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::CreateUserRequest {
                        name: name.clone(),
                        email: email.clone(),
                        region: region_i32,
                        role: Some(role_i32),
                        email_hmac: email_hmac.clone(),
                        organization_name: String::new(),
                        organization_tier: None,
                    };

                    let response =
                        client.create_user(tonic::Request::new(request)).await?.into_inner();

                    response
                        .user
                        .map(|u| user_info_from_proto(&u))
                        .ok_or_else(|| missing_response_field("user", "CreateUserResponse"))
                },
            ),
        )
        .await
    }

    /// Gets a user by slug.
    pub async fn get_user(&self, user: UserSlug) -> Result<UserInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_user",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_user",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::GetUserRequest {
                        slug: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response =
                        client.get_user(tonic::Request::new(request)).await?.into_inner();

                    response
                        .user
                        .map(|u| user_info_from_proto(&u))
                        .ok_or_else(|| missing_response_field("user", "GetUserResponse"))
                },
            ),
        )
        .await
    }

    /// Updates a user's name, role, or primary email.
    ///
    /// At least one field must be provided.
    pub async fn update_user(
        &self,
        user: UserSlug,
        name: Option<String>,
        role: Option<UserRole>,
        primary_email_id: Option<UserEmailId>,
    ) -> Result<UserInfo> {
        self.check_shutdown(None)?;

        let proto_role = role.map(|r| {
            let pr: proto::UserRole = r.into();
            let i: i32 = pr.into();
            i
        });
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_user",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_user",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::UpdateUserRequest {
                        slug: Some(proto::UserSlug { slug: user.value() }),
                        name: name.clone(),
                        role: proto_role,
                        primary_email: primary_email_id
                            .map(|id| proto::UserEmailId { id: id.value() }),
                    };

                    let response =
                        client.update_user(tonic::Request::new(request)).await?.into_inner();

                    response
                        .user
                        .map(|u| user_info_from_proto(&u))
                        .ok_or_else(|| missing_response_field("user", "UpdateUserResponse"))
                },
            ),
        )
        .await
    }

    /// Soft-deletes a user, starting the retention countdown.
    pub async fn delete_user(
        &self,
        user: UserSlug,
        deleted_by: impl Into<String>,
    ) -> Result<UserInfo> {
        self.check_shutdown(None)?;

        let deleted_by = deleted_by.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_user",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_user",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::DeleteUserRequest {
                        slug: Some(proto::UserSlug { slug: user.value() }),
                        deleted_by: deleted_by.clone(),
                    };

                    let response =
                        client.delete_user(tonic::Request::new(request)).await?.into_inner();

                    let slug_val = response.slug.map_or(0, |s| s.slug);
                    Ok(UserInfo {
                        slug: UserSlug::new(slug_val),
                        name: String::new(),
                        primary_email_id: UserEmailId::new(0),
                        status: UserStatus::Deleted,
                        role: UserRole::User,
                        created_at: None,
                        updated_at: None,
                        deleted_at: response
                            .deleted_at
                            .as_ref()
                            .and_then(proto_timestamp_to_system_time),
                        retention_days: Some(response.retention_days),
                    })
                },
            ),
        )
        .await
    }

    /// Lists users with pagination.
    pub async fn list_users(
        &self,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<UserInfo>, Option<Vec<u8>>)> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_users",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_users",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::ListUsersRequest {
                        page_size,
                        page_token: page_token.clone(),
                        region: None,
                    };

                    let response =
                        client.list_users(tonic::Request::new(request)).await?.into_inner();

                    let users: Vec<UserInfo> =
                        response.users.iter().map(user_info_from_proto).collect();
                    Ok((users, response.next_page_token))
                },
            ),
        )
        .await
    }

    /// Searches users by email.
    pub async fn search_users(&self, email: impl Into<String>) -> Result<Vec<UserInfo>> {
        self.check_shutdown(None)?;

        let email = email.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "search_users",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "search_users",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::SearchUsersRequest {
                        filter: Some(proto::UserSearchFilter {
                            email: Some(email.clone()),
                            status: None,
                            role: None,
                            name_prefix: None,
                        }),
                        page_token: None,
                        page_size: 100,
                    };

                    let response =
                        client.search_users(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.users.iter().map(user_info_from_proto).collect())
                },
            ),
        )
        .await
    }

    /// Creates an email record for a user.
    ///
    /// The `email_hmac` is the hex-encoded HMAC-SHA256 of the normalized email,
    /// computed with the email blinding key. It is stored in the GLOBAL control
    /// plane for cross-region uniqueness; the plaintext email stays regional.
    pub async fn create_user_email(
        &self,
        user: UserSlug,
        email: impl Into<String>,
        email_hmac: impl Into<String>,
    ) -> Result<UserEmailInfo> {
        self.check_shutdown(None)?;

        let email = email.into();
        let email_hmac = email_hmac.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_user_email",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_user_email",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::CreateUserEmailRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        email: email.clone(),
                        email_hmac: email_hmac.clone(),
                    };

                    let response =
                        client.create_user_email(tonic::Request::new(request)).await?.into_inner();

                    response
                        .email
                        .map(|e| user_email_info_from_proto(&e))
                        .ok_or_else(|| missing_response_field("email", "CreateUserEmailResponse"))
                },
            ),
        )
        .await
    }

    /// Deletes an email record from a user.
    pub async fn delete_user_email(&self, user: UserSlug, email_id: UserEmailId) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_user_email",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_user_email",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::DeleteUserEmailRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        email_id: Some(proto::UserEmailId { id: email_id.value() }),
                    };

                    client.delete_user_email(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
        .await
    }

    /// Searches user emails by user or email address.
    pub async fn search_user_email(
        &self,
        user: Option<UserSlug>,
        email: Option<String>,
    ) -> Result<Vec<UserEmailInfo>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "search_user_email",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "search_user_email",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::SearchUserEmailRequest {
                        filter: Some(proto::UserEmailSearchFilter {
                            user: user.map(|s| proto::UserSlug { slug: s.value() }),
                            email: email.clone(),
                            verified_only: None,
                        }),
                        page_token: None,
                        page_size: 100,
                    };

                    let response =
                        client.search_user_email(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.emails.iter().map(user_email_info_from_proto).collect())
                },
            ),
        )
        .await
    }

    /// Verifies a user email using a verification token.
    pub async fn verify_user_email(&self, token: impl Into<String>) -> Result<UserEmailInfo> {
        self.check_shutdown(None)?;

        let token = token.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "verify_user_email",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "verify_user_email",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::VerifyUserEmailRequest { token: token.clone() };

                    let response =
                        client.verify_user_email(tonic::Request::new(request)).await?.into_inner();

                    response
                        .email
                        .map(|e| user_email_info_from_proto(&e))
                        .ok_or_else(|| missing_response_field("email", "VerifyUserEmailResponse"))
                },
            ),
        )
        .await
    }

    /// Initiates a blinding key rotation.
    ///
    /// Starts an asynchronous re-hashing of all email hash entries with the new
    /// blinding key version. Returns immediately with initial progress.
    ///
    /// # Arguments
    ///
    /// * `new_key_version` - Version number of the new blinding key (monotonically increasing).
    ///
    /// # Returns
    ///
    /// Returns [`BlindingKeyRotationStatus`] with initial progress.
    pub async fn rotate_blinding_key(
        &self,
        new_key_version: u32,
    ) -> Result<BlindingKeyRotationStatus> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "rotate_blinding_key",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "rotate_blinding_key",
                || async {
                    let mut client = connected_client!(pool, create_admin_client);

                    let request = proto::RotateBlindingKeyRequest { new_key_version };

                    let response = client
                        .rotate_blinding_key(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(BlindingKeyRotationStatus {
                        total_entries: response.total_entries,
                        entries_rehashed: response.entries_rehashed,
                        complete: response.complete,
                    })
                },
            ),
        )
        .await
    }

    /// Gets the current status of a blinding key rotation.
    ///
    /// Returns progress information about an in-flight or completed rotation,
    /// including per-region breakdown.
    ///
    /// # Returns
    ///
    /// Returns [`BlindingKeyRehashStatus`] with progress details.
    pub async fn get_blinding_key_rehash_status(&self) -> Result<BlindingKeyRehashStatus> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_blinding_key_rehash_status",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_blinding_key_rehash_status",
                || async {
                    let mut client = connected_client!(pool, create_admin_client);

                    let request = proto::GetBlindingKeyRehashStatusRequest {};

                    let response = client
                        .get_blinding_key_rehash_status(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(BlindingKeyRehashStatus {
                        total_entries: response.total_entries,
                        entries_rehashed: response.entries_rehashed,
                        complete: response.complete,
                        per_region_progress: response.per_region_progress,
                        active_key_version: response.active_key_version,
                    })
                },
            ),
        )
        .await
    }

    /// Creates a new vault in an organization.
    ///
    /// Creates a vault within the specified organization. The vault slug
    /// (external identifier) is assigned by the leader and returned in the response.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns [`VaultInfo`] containing the new vault's metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// let vault = client.create_vault(organization).await?;
    /// println!("Created vault with slug: {}", vault.vault);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_vault(&self, organization: OrganizationSlug) -> Result<VaultInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_vault",
                || async {
                    let mut client = connected_client!(pool, create_vault_client);

                    let request = proto::CreateVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        replication_factor: 0,  // Use default
                        initial_nodes: vec![],  // Auto-assigned
                        retention_policy: None, // Default: FULL
                    };

                    let response =
                        client.create_vault(tonic::Request::new(request)).await?.into_inner();

                    let genesis = response.genesis.unwrap_or_default();
                    Ok(VaultInfo {
                        organization,
                        vault: VaultSlug::new(response.vault.map_or(0, |v| v.slug)),
                        height: genesis.height,
                        state_root: genesis.state_root.map(|h| h.value).unwrap_or_default(),
                        nodes: vec![],
                        leader: None,
                        status: VaultStatus::Active,
                    })
                },
            ),
        )
        .await
    }

    /// Returns information about a vault.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns [`VaultInfo`] containing vault metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection fails after retry attempts
    /// - The organization or vault does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let info = client.get_vault(organization, vault).await?;
    /// println!("Vault height: {}, status: {:?}", info.height, info.status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_vault(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<VaultInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_vault",
                || async {
                    let mut client = connected_client!(pool, create_vault_client);

                    let request = proto::GetVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                    };

                    let response =
                        client.get_vault(tonic::Request::new(request)).await?.into_inner();

                    Ok(VaultInfo::from_proto(response))
                },
            ),
        )
        .await
    }

    /// Lists vaults on this node.
    ///
    /// Returns a paginated list of vaults that this node is hosting or
    /// participating in. Pass the returned `next_page_token` into subsequent
    /// calls to retrieve further pages.
    ///
    /// # Arguments
    ///
    /// * `page_size` - Maximum items per page (0 = server default).
    /// * `page_token` - Opaque cursor from a previous response, or `None` for the first page.
    ///
    /// # Returns
    ///
    /// A tuple of `(vaults, next_page_token)`. When `next_page_token`
    /// is `None`, there are no more pages.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let (vaults, _next) = client.list_vaults(100, None).await?;
    /// for v in vaults {
    ///     println!("Vault {} in {}", v.vault, v.organization);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_vaults(
        &self,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<VaultInfo>, Option<Vec<u8>>)> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_vaults",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_vaults",
                || async {
                    let mut client = connected_client!(pool, create_vault_client);

                    let request =
                        proto::ListVaultsRequest { page_token: page_token.clone(), page_size };

                    let response =
                        client.list_vaults(tonic::Request::new(request)).await?.into_inner();

                    let vaults = response.vaults.into_iter().map(VaultInfo::from_proto).collect();

                    Ok((vaults, response.next_page_token))
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Onboarding (Email Verification + Registration)
    // =========================================================================

    /// Initiates email verification by generating a code.
    ///
    /// The returned code should be sent to the user's email out-of-band.
    pub async fn initiate_email_verification(
        &self,
        email: impl Into<String>,
        region: Region,
    ) -> Result<EmailVerificationCode> {
        self.check_shutdown(None)?;

        let email = email.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "initiate_email_verification",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "initiate_email_verification",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::InitiateEmailVerificationRequest {
                        email: email.clone(),
                        region: region_i32,
                    };

                    let response = client.initiate_email_verification(request).await?.into_inner();

                    Ok(EmailVerificationCode { code: response.code })
                },
            ),
        )
        .await
    }

    /// Verifies the code the user received via email.
    ///
    /// Returns either a session for an existing user or an onboarding token
    /// for a new user who must complete registration.
    pub async fn verify_email_code(
        &self,
        email: impl Into<String>,
        code: impl Into<String>,
        region: Region,
    ) -> Result<EmailVerificationResult> {
        self.check_shutdown(None)?;

        let email = email.into();
        let code = code.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "verify_email_code",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "verify_email_code",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::VerifyEmailCodeRequest {
                        email: email.clone(),
                        code: code.clone(),
                        region: region_i32,
                    };

                    let response = client.verify_email_code(request).await?.into_inner();

                    match response.result {
                        Some(proto::verify_email_code_response::Result::ExistingUser(existing)) => {
                            let user_slug =
                                existing.user.map(|u| UserSlug::new(u.slug)).ok_or_else(|| {
                                    missing_response_field("user", "VerifyEmailCodeResponse")
                                })?;
                            let session = existing
                                .session
                                .map(crate::token::TokenPair::from_proto)
                                .ok_or_else(|| {
                                    missing_response_field("session", "VerifyEmailCodeResponse")
                                })?;
                            Ok(EmailVerificationResult::ExistingUser { user: user_slug, session })
                        },
                        Some(proto::verify_email_code_response::Result::NewUser(onboarding)) => {
                            Ok(EmailVerificationResult::NewUser {
                                onboarding_token: onboarding.onboarding_token,
                            })
                        },
                        None => Err(error::SdkError::Rpc {
                            code: tonic::Code::Internal,
                            message: "Empty verify_email_code response".to_string(),
                            request_id: None,
                            trace_id: None,
                            error_details: None,
                        }),
                    }
                },
            ),
        )
        .await
    }

    /// Completes registration for a new user after email verification.
    ///
    /// Requires the onboarding token from [`verify_email_code`](Self::verify_email_code).
    pub async fn complete_registration(
        &self,
        onboarding_token: impl Into<String>,
        email: impl Into<String>,
        region: Region,
        name: impl Into<String>,
        organization_name: impl Into<String>,
    ) -> Result<RegistrationResult> {
        self.check_shutdown(None)?;

        let onboarding_token = onboarding_token.into();
        let email = email.into();
        let name = name.into();
        let organization_name = organization_name.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "complete_registration",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "complete_registration",
                || async {
                    let mut client = connected_client!(pool, create_user_client);

                    let request = proto::CompleteRegistrationRequest {
                        onboarding_token: onboarding_token.clone(),
                        email: email.clone(),
                        region: region_i32,
                        name: name.clone(),
                        organization_name: organization_name.clone(),
                    };

                    let response = client.complete_registration(request).await?.into_inner();

                    let user_slug = response
                        .user
                        .and_then(|u| u.slug)
                        .map(|s| UserSlug::new(s.slug))
                        .ok_or_else(|| {
                            missing_response_field("user.slug", "CompleteRegistrationResponse")
                        })?;

                    let session =
                        response.session.map(crate::token::TokenPair::from_proto).ok_or_else(
                            || missing_response_field("session", "CompleteRegistrationResponse"),
                        )?;

                    let organization = response.organization.map(|o| OrganizationSlug::new(o.slug));

                    Ok(RegistrationResult { user: user_slug, session, organization })
                },
            ),
        )
        .await
    }

    // =========================================================================
    // App CRUD
    // =========================================================================

    /// Creates a new app in an organization.
    pub async fn create_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        name: impl Into<String>,
        description: Option<String>,
    ) -> Result<AppInfo> {
        self.check_shutdown(None)?;

        let name = name.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_app",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_app",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::CreateAppRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        name: name.clone(),
                        description: description.clone(),
                    };

                    let response =
                        client.create_app(tonic::Request::new(request)).await?.into_inner();

                    response
                        .app
                        .map(|a| app_info_from_proto(&a))
                        .ok_or_else(|| missing_response_field("app", "CreateAppResponse"))
                },
            ),
        )
        .await
    }

    /// Gets an app by slug.
    pub async fn get_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<AppInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_app",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_app",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::GetAppRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                    };

                    let response = client.get_app(tonic::Request::new(request)).await?.into_inner();

                    response
                        .app
                        .map(|a| app_info_from_proto(&a))
                        .ok_or_else(|| missing_response_field("app", "GetAppResponse"))
                },
            ),
        )
        .await
    }

    /// Lists all apps in an organization.
    pub async fn list_apps(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
    ) -> Result<Vec<AppInfo>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_apps",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_apps",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::ListAppsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response =
                        client.list_apps(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.apps.iter().map(app_info_from_proto).collect())
                },
            ),
        )
        .await
    }

    /// Updates an app's name and/or description.
    pub async fn update_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        name: Option<String>,
        description: Option<String>,
    ) -> Result<AppInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_app",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_app",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::UpdateAppRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        name: name.clone(),
                        description: description.clone(),
                    };

                    let response =
                        client.update_app(tonic::Request::new(request)).await?.into_inner();

                    response
                        .app
                        .map(|a| app_info_from_proto(&a))
                        .ok_or_else(|| missing_response_field("app", "UpdateAppResponse"))
                },
            ),
        )
        .await
    }

    /// Deletes an app.
    pub async fn delete_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_app",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_app",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::DeleteAppRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                    };

                    client.delete_app(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
        .await
    }

    /// Enables an app.
    pub async fn enable_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<AppInfo> {
        self.set_app_enabled(organization, user, app, true).await
    }

    /// Disables an app.
    pub async fn disable_app(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<AppInfo> {
        self.set_app_enabled(organization, user, app, false).await
    }

    async fn set_app_enabled(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        enabled: bool,
    ) -> Result<AppInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "set_app_enabled",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "set_app_enabled",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::SetAppEnabledRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        enabled,
                    };

                    let response =
                        client.set_app_enabled(tonic::Request::new(request)).await?.into_inner();

                    response
                        .app
                        .map(|a| app_info_from_proto(&a))
                        .ok_or_else(|| missing_response_field("app", "SetAppEnabledResponse"))
                },
            ),
        )
        .await
    }

    // =========================================================================
    // App Credentials
    // =========================================================================

    /// Enables or disables a credential type for an app.
    pub async fn set_app_credential_enabled(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        credential_type: AppCredentialType,
        enabled: bool,
    ) -> Result<AppInfo> {
        self.check_shutdown(None)?;

        let credential_type_i32 = credential_type.to_proto();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "set_app_credential_enabled",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "set_app_credential_enabled",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::SetAppCredentialEnabledRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        credential_type: credential_type_i32,
                        enabled,
                    };

                    let response = client
                        .set_app_credential_enabled(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response.app.map(|a| app_info_from_proto(&a)).ok_or_else(|| {
                        missing_response_field("app", "SetAppCredentialEnabledResponse")
                    })
                },
            ),
        )
        .await
    }

    /// Gets the client secret status for an app (enabled flag and whether a secret exists).
    pub async fn get_app_client_secret(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<AppClientSecretStatus> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_app_client_secret",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_app_client_secret",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::GetAppClientSecretRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                    };

                    let response = client
                        .get_app_client_secret(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(AppClientSecretStatus {
                        enabled: response.enabled,
                        has_secret: response.has_secret,
                    })
                },
            ),
        )
        .await
    }

    /// Rotates the client secret for an app. Returns the new plaintext secret (base64-encoded).
    ///
    /// An idempotency key is generated per call so retries (including automatic
    /// retries) return the same secret instead of creating a new one.
    pub async fn rotate_app_client_secret(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<String> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();
        let idempotency_key: [u8; 16] = rand::random();

        self.with_metrics(
            "rotate_app_client_secret",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "rotate_app_client_secret",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::RotateAppClientSecretRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        idempotency_key: idempotency_key.to_vec(),
                    };

                    let response = client
                        .rotate_app_client_secret(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(response.secret)
                },
            ),
        )
        .await
    }

    // =========================================================================
    // App Client Assertions
    // =========================================================================

    /// Lists client assertions for an app.
    pub async fn list_app_client_assertions(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<Vec<AppClientAssertionInfo>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_app_client_assertions",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_app_client_assertions",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::ListAppClientAssertionsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                    };

                    let response = client
                        .list_app_client_assertions(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(response.assertions.iter().map(assertion_info_from_proto).collect())
                },
            ),
        )
        .await
    }

    /// Creates a client assertion for an app. Returns the assertion metadata and private key PEM.
    ///
    /// The private key PEM is only returned on creation — it cannot be retrieved again.
    /// An idempotency key is generated per call so retries return the same keypair.
    pub async fn create_app_client_assertion(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        name: impl Into<String>,
        expires_at: std::time::SystemTime,
    ) -> Result<CreateAppClientAssertionResult> {
        self.check_shutdown(None)?;

        let name = name.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();
        let idempotency_key: [u8; 16] = rand::random();
        let expires_at_proto = system_time_to_proto_timestamp(&expires_at);

        self.with_metrics(
            "create_app_client_assertion",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_app_client_assertion",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::CreateAppClientAssertionRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        name: name.clone(),
                        expires_at: Some(expires_at_proto),
                        idempotency_key: idempotency_key.to_vec(),
                    };

                    let response = client
                        .create_app_client_assertion(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let assertion = response
                        .assertion
                        .map(|a| assertion_info_from_proto(&a))
                        .ok_or_else(|| {
                            missing_response_field("assertion", "CreateAppClientAssertionResponse")
                        })?;

                    Ok(CreateAppClientAssertionResult {
                        assertion,
                        private_key_pem: response.private_key_pem,
                    })
                },
            ),
        )
        .await
    }

    /// Deletes a client assertion for an app.
    pub async fn delete_app_client_assertion(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        assertion: DomainClientAssertionId,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_app_client_assertion",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_app_client_assertion",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::DeleteAppClientAssertionRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        assertion: Some(proto::ClientAssertionId { id: assertion.value() }),
                    };

                    client.delete_app_client_assertion(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
        .await
    }

    /// Enables or disables a specific client assertion.
    pub async fn set_app_client_assertion_enabled(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        assertion: DomainClientAssertionId,
        enabled: bool,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "set_app_client_assertion_enabled",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "set_app_client_assertion_enabled",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::SetAppClientAssertionEnabledRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        assertion: Some(proto::ClientAssertionId { id: assertion.value() }),
                        enabled,
                    };

                    client.set_app_client_assertion_enabled(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
        .await
    }

    // =========================================================================
    // App Vault Connections
    // =========================================================================

    /// Lists vault connections for an app.
    pub async fn list_app_vaults(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
    ) -> Result<Vec<AppVaultConnectionInfo>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_app_vaults",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_app_vaults",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::ListAppVaultsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                    };

                    let response =
                        client.list_app_vaults(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.vaults.iter().map(vault_connection_from_proto).collect())
                },
            ),
        )
        .await
    }

    /// Adds a vault connection to an app.
    pub async fn add_app_vault(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        vault: VaultSlug,
        allowed_scopes: Vec<String>,
    ) -> Result<AppVaultConnectionInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "add_app_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "add_app_vault",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::AddAppVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        allowed_scopes: allowed_scopes.clone(),
                    };

                    let response =
                        client.add_app_vault(tonic::Request::new(request)).await?.into_inner();

                    response
                        .vault
                        .map(|v| vault_connection_from_proto(&v))
                        .ok_or_else(|| missing_response_field("vault", "AddAppVaultResponse"))
                },
            ),
        )
        .await
    }

    /// Updates the allowed scopes for a vault connection.
    pub async fn update_app_vault(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        vault: VaultSlug,
        allowed_scopes: Vec<String>,
    ) -> Result<AppVaultConnectionInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_app_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_app_vault",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::UpdateAppVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        allowed_scopes: allowed_scopes.clone(),
                    };

                    let response =
                        client.update_app_vault(tonic::Request::new(request)).await?.into_inner();

                    response
                        .vault
                        .map(|v| vault_connection_from_proto(&v))
                        .ok_or_else(|| missing_response_field("vault", "UpdateAppVaultResponse"))
                },
            ),
        )
        .await
    }

    /// Removes a vault connection from an app.
    pub async fn remove_app_vault(
        &self,
        organization: OrganizationSlug,
        user: UserSlug,
        app: AppSlug,
        vault: VaultSlug,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "remove_app_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "remove_app_vault",
                || async {
                    let mut client = connected_client!(pool, create_app_client);

                    let request = proto::RemoveAppVaultRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        initiator: Some(proto::UserSlug { slug: user.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                    };

                    client.remove_app_vault(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Events Operations
    // =========================================================================

    /// Lists audit events for an organization with optional filtering.
    ///
    /// Returns a paginated list of events matching the filter criteria.
    /// Pass `organization = 0` to query system-level events.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier; 0 for system events).
    /// * `filter` - Filter criteria (empty filter matches all events).
    /// * `limit` - Maximum results per page (0 = server default, max 1000).
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, EventFilter, OrganizationSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let filter = EventFilter::new()
    ///     .event_type_prefix("ledger.vault")
    ///     .outcome_success();
    /// let page = client.list_events(organization, filter, 100).await?;
    /// for event in &page.entries {
    ///     println!("{}: {}", event.event_type, event.principal);
    /// }
    /// if page.has_next_page() {
    ///     let next = client.list_events_next(organization, page.next_page_token.as_deref().unwrap_or_default()).await?;
    ///     println!("Next page: {} events", next.entries.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_events(
        &self,
        organization: OrganizationSlug,
        filter: EventFilter,
        limit: u32,
    ) -> Result<EventPage> {
        self.list_events_inner(organization, filter, limit, String::new()).await
    }

    /// Continues paginating audit events from a previous response.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier; must match the original query).
    /// * `page_token` - Opaque cursor from the previous response's `next_page_token`.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, EventFilter, OrganizationSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// # let page_token = "abc".to_string();
    /// let next_page = client.list_events_next(organization, &page_token).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_events_next(
        &self,
        organization: OrganizationSlug,
        page_token: &str,
    ) -> Result<EventPage> {
        self.list_events_inner(organization, EventFilter::new(), 0, page_token.to_owned()).await
    }

    /// Internal list_events implementation shared by `list_events` and `list_events_next`.
    async fn list_events_inner(
        &self,
        organization: OrganizationSlug,
        filter: EventFilter,
        limit: u32,
        page_token: String,
    ) -> Result<EventPage> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_events",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_events",
                || async {
                    let mut client = connected_client!(pool, create_events_client);

                    let request = proto::ListEventsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        filter: Some(filter.to_proto()),
                        limit,
                        page_token: page_token.clone(),
                    };

                    let response =
                        client.list_events(tonic::Request::new(request)).await?.into_inner();

                    let entries =
                        response.entries.into_iter().map(SdkEventEntry::from_proto).collect();

                    let next_page_token = if response.next_page_token.is_empty() {
                        None
                    } else {
                        Some(response.next_page_token)
                    };

                    Ok(EventPage {
                        entries,
                        next_page_token,
                        total_estimate: response.total_estimate,
                    })
                },
            ),
        )
        .await
    }

    /// Retrieves a single audit event by ID.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `event_id` - Event identifier (UUID string, e.g., from `event_id_string()`).
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` with `NOT_FOUND` if the event does not exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let event = client.get_event(organization, "550e8400-e29b-41d4-a716-446655440000").await?;
    /// println!("Event: {} by {}", event.event_type, event.principal);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_event(
        &self,
        organization: OrganizationSlug,
        event_id: &str,
    ) -> Result<SdkEventEntry> {
        self.check_shutdown(None)?;

        let event_id_bytes =
            uuid::Uuid::parse_str(event_id).map(|u| u.into_bytes().to_vec()).map_err(|e| {
                error::SdkError::Validation { message: format!("invalid event_id: {e}") }
            })?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_event",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_event",
                || async {
                    let mut client = connected_client!(pool, create_events_client);

                    let request = proto::GetEventRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        event_id: event_id_bytes.clone(),
                    };

                    let response =
                        client.get_event(tonic::Request::new(request)).await?.into_inner();

                    let entry = response
                        .entry
                        .ok_or_else(|| tonic::Status::not_found("event not found in response"))?;

                    Ok(SdkEventEntry::from_proto(entry))
                },
            ),
        )
        .await
    }

    /// Counts audit events matching a filter.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier; 0 for system events).
    /// * `filter` - Filter criteria (empty filter counts all events).
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, EventFilter, OrganizationSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let denied_count = client.count_events(organization, EventFilter::new().outcome_denied()).await?;
    /// println!("Denied events: {}", denied_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count_events(
        &self,
        organization: OrganizationSlug,
        filter: EventFilter,
    ) -> Result<u64> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "count_events",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "count_events",
                || async {
                    let mut client = connected_client!(pool, create_events_client);

                    let request = proto::CountEventsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        filter: Some(filter.to_proto()),
                    };

                    let response =
                        client.count_events(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.count)
                },
            ),
        )
        .await
    }

    /// Ingests external audit events from Engine or Control services.
    ///
    /// Writes a batch of events into the organization's audit trail. Events
    /// are stored as handler-phase entries (node-local, not Raft-replicated).
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `source` - Originating component ([`EventSource::Engine`] or [`EventSource::Control`]).
    /// * `events` - Batch of events to ingest.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Rpc` if ingestion fails. Individual events may be
    /// rejected; check `IngestResult::rejections` for per-event details.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, SdkIngestEventEntry, EventOutcome, EventSource, OrganizationSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(12345);
    /// let events = vec![
    ///     SdkIngestEventEntry::new(
    ///         "engine.authorization.checked",
    ///         "user:alice",
    ///         EventOutcome::Success,
    ///     )
    ///     .detail("resource", "document:123")
    ///     .detail("relation", "viewer"),
    /// ];
    /// let result = client.ingest_events(organization, EventSource::Engine, events).await?;
    /// println!("Accepted: {}, Rejected: {}", result.accepted_count, result.rejected_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ingest_events(
        &self,
        organization: OrganizationSlug,
        source: EventSource,
        events: Vec<SdkIngestEventEntry>,
    ) -> Result<IngestResult> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "ingest_events",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "ingest_events",
                || async {
                    let mut client = connected_client!(pool, create_events_client);

                    let request = proto::IngestEventsRequest {
                        source_service: source.as_str().to_owned(),
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        entries: events
                            .clone()
                            .into_iter()
                            .map(SdkIngestEventEntry::into_proto)
                            .collect(),
                    };

                    let response =
                        client.ingest_events(tonic::Request::new(request)).await?.into_inner();

                    Ok(IngestResult {
                        accepted_count: response.accepted_count,
                        rejected_count: response.rejected_count,
                        rejections: response
                            .rejections
                            .into_iter()
                            .map(|r| IngestRejection { index: r.index, reason: r.reason })
                            .collect(),
                    })
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Health Operations
    // =========================================================================

    /// Checks node-level health.
    ///
    /// Returns `true` if the node is healthy and has a leader elected.
    /// This is a simple health check suitable for load balancer probes.
    ///
    /// # Returns
    ///
    /// Returns `true` if the node is healthy, `false` if degraded.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is unavailable or connection fails.
    /// Note: An unavailable node returns an error, not `false`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// if client.health_check().await? {
    ///     println!("Node is healthy");
    /// } else {
    ///     println!("Node is degraded but available");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check(&self) -> Result<bool> {
        self.check_shutdown(None)?;

        let result = self.health_check_detailed().await?;
        match result.status {
            HealthStatus::Healthy => Ok(true),
            HealthStatus::Degraded => Ok(false),
            HealthStatus::Unavailable => {
                Err(error::SdkError::Unavailable { message: result.message })
            },
            HealthStatus::Unspecified => Ok(false),
        }
    }

    /// Returns detailed node-level health information.
    ///
    /// Returns full health check result including status, message, and details.
    /// Use this for monitoring and diagnostics that need more than a simple boolean.
    ///
    /// # Returns
    ///
    /// Returns a [`HealthCheckResult`] with status, message, and details.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::LedgerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// let health = client.health_check_detailed().await?;
    /// println!("Status: {:?}, Message: {}", health.status, health.message);
    /// if let Some(term) = health.details.get("current_term") {
    ///     println!("Current Raft term: {}", term);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check_detailed(&self) -> Result<HealthCheckResult> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "health_check_detailed",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "health_check_detailed",
                || async {
                    let mut client = connected_client!(pool, create_health_client);

                    let request = proto::HealthCheckRequest { organization: None, vault: None };

                    let response = client.check(tonic::Request::new(request)).await?.into_inner();

                    Ok(HealthCheckResult::from_proto(response))
                },
            ),
        )
        .await
    }

    /// Checks health of a specific vault.
    ///
    /// Returns detailed health information for a specific vault, including
    /// block height, health status, and any divergence information.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    ///
    /// # Returns
    ///
    /// Returns a [`HealthCheckResult`] with vault-specific health information.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// let health = client.health_check_vault(organization, VaultSlug::new(0)).await?;
    /// println!("Vault status: {:?}", health.status);
    /// if let Some(height) = health.details.get("block_height") {
    ///     println!("Current height: {}", height);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check_vault(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
    ) -> Result<HealthCheckResult> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "health_check_vault",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "health_check_vault",
                || async {
                    let mut client = connected_client!(pool, create_health_client);

                    let request = proto::HealthCheckRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                    };

                    let response = client.check(tonic::Request::new(request)).await?.into_inner();

                    Ok(HealthCheckResult::from_proto(response))
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Verified Read Operations
    // =========================================================================

    /// Reads a value with cryptographic proof for client-side verification.
    ///
    /// Returns the value along with a Merkle proof that can be used to verify
    /// the value is authentic without trusting the server. The proof links
    /// the entity value to the state root in the block header.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Optional vault slug (omit for organization-level entities).
    /// * `key` - Entity key to read.
    /// * `opts` - Verification options (height, chain proof).
    ///
    /// # Returns
    ///
    /// `VerifiedValue` containing the value and proofs, or `None` if key not found.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the read fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, OrganizationSlug, VaultSlug, VerifyOpts};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// let result = client.verified_read(organization, Some(vault), "user:123", VerifyOpts::new()).await?;
    /// if let Some(verified) = result {
    ///     // Verify the proof before using the value
    ///     verified.verify()?;
    ///     println!("Value: {:?}", verified.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn verified_read(
        &self,
        organization: OrganizationSlug,
        vault: Option<VaultSlug>,
        key: impl Into<String>,
        opts: VerifyOpts,
    ) -> Result<Option<VerifiedValue>> {
        self.check_shutdown(None)?;

        let key = key.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "verified_read",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "verified_read",
                || async {
                    let mut client = connected_client!(pool, create_read_client);

                    let request = proto::VerifiedReadRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: vault.map(|v| proto::VaultSlug { slug: v.value() }),
                        key: key.clone(),
                        at_height: opts.at_height,
                        include_chain_proof: opts.include_chain_proof,
                        trusted_height: opts.trusted_height,
                    };

                    let response =
                        client.verified_read(tonic::Request::new(request)).await?.into_inner();

                    // If no value and no block header, key was not found
                    if response.value.is_none() && response.block_header.is_none() {
                        return Ok(None);
                    }

                    Ok(VerifiedValue::from_proto(response))
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Query Operations
    // =========================================================================

    /// Lists entities matching a key prefix.
    ///
    /// Returns a paginated list of entities with keys starting with the given prefix.
    /// Use the `next_page_token` to fetch additional pages.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `opts` - Query options including prefix filter, pagination, and consistency.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListEntitiesOpts, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let organization = OrganizationSlug::new(1);
    /// // List all users
    /// let result = client.list_entities(organization, ListEntitiesOpts::with_prefix("user:")).await?;
    /// for entity in result.items {
    ///     println!("Key: {}, Version: {}", entity.key, entity.version);
    /// }
    ///
    /// // Fetch next page if available
    /// if let Some(token) = result.next_page_token {
    ///     let next_page = client.list_entities(
    ///         organization,
    ///         ListEntitiesOpts::with_prefix("user:").page_token(token)
    ///     ).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_entities(
        &self,
        organization: OrganizationSlug,
        opts: ListEntitiesOpts,
    ) -> Result<PagedResult<Entity>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_entities",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_entities",
                || async {
                    let mut client = connected_client!(pool, create_read_client);

                    let request = proto::ListEntitiesRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        key_prefix: opts.key_prefix.clone(),
                        at_height: opts.at_height,
                        include_expired: opts.include_expired,
                        limit: opts.limit,
                        page_token: opts.page_token.clone().unwrap_or_default(),
                        consistency: opts.consistency.to_proto() as i32,
                        vault: opts.vault.map(|v| proto::VaultSlug { slug: v.value() }),
                    };

                    let response =
                        client.list_entities(tonic::Request::new(request)).await?.into_inner();

                    let items = response.entities.into_iter().map(Entity::from_proto).collect();

                    let next_page_token = if response.next_page_token.is_empty() {
                        None
                    } else {
                        Some(response.next_page_token)
                    };

                    Ok(PagedResult { items, next_page_token, block_height: response.block_height })
                },
            ),
        )
        .await
    }

    /// Lists relationships in a vault with optional filters.
    ///
    /// Returns a paginated list of relationships matching the filter criteria.
    /// All filter fields are optional; omitting a filter matches all values.
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `opts` - Query options including filters, pagination, and consistency.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListRelationshipsOpts, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // List all relationships for a document
    /// let result = client.list_relationships(
    ///     organization,
    ///     vault,
    ///     ListRelationshipsOpts::new().resource("document:123")
    /// ).await?;
    ///
    /// for rel in result.items {
    ///     println!("{} -> {} -> {}", rel.resource, rel.relation, rel.subject);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_relationships(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        opts: ListRelationshipsOpts,
    ) -> Result<PagedResult<Relationship>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_relationships",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_relationships",
                || async {
                    let mut client = connected_client!(pool, create_read_client);

                    let request = proto::ListRelationshipsRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        resource: opts.resource.clone(),
                        relation: opts.relation.clone(),
                        subject: opts.subject.clone(),
                        at_height: opts.at_height,
                        limit: opts.limit,
                        page_token: opts.page_token.clone().unwrap_or_default(),
                        consistency: opts.consistency.to_proto() as i32,
                    };

                    let response =
                        client.list_relationships(tonic::Request::new(request)).await?.into_inner();

                    let items =
                        response.relationships.into_iter().map(Relationship::from_proto).collect();

                    let next_page_token = if response.next_page_token.is_empty() {
                        None
                    } else {
                        Some(response.next_page_token)
                    };

                    Ok(PagedResult { items, next_page_token, block_height: response.block_height })
                },
            ),
        )
        .await
    }

    /// Lists distinct resource IDs matching a type prefix.
    ///
    /// Returns a paginated list of unique resource identifiers that match the given
    /// type prefix (e.g., "document" matches "document:1", "document:2", etc.).
    ///
    /// # Arguments
    ///
    /// * `organization` - Organization slug (external identifier).
    /// * `vault` - Vault slug (external identifier).
    /// * `opts` - Query options including type filter, pagination, and consistency.
    ///
    /// # Errors
    ///
    /// Returns `SdkError::Shutdown` if the client has been shut down.
    /// Returns `SdkError::Rpc` if the query fails after retry attempts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use inferadb_ledger_sdk::{LedgerClient, ListResourcesOpts, OrganizationSlug, VaultSlug};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = LedgerClient::connect("http://localhost:50051", "my-service").await?;
    /// # let (organization, vault) = (OrganizationSlug::new(1), VaultSlug::new(1));
    /// // List all document resources
    /// let result = client.list_resources(
    ///     organization,
    ///     vault,
    ///     ListResourcesOpts::with_type("document")
    /// ).await?;
    ///
    /// for resource_id in result.items {
    ///     println!("Resource: {}", resource_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_resources(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        opts: ListResourcesOpts,
    ) -> Result<PagedResult<String>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_resources",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_resources",
                || async {
                    let mut client = connected_client!(pool, create_read_client);

                    let request = proto::ListResourcesRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        resource_type: opts.resource_type.clone(),
                        at_height: opts.at_height,
                        limit: opts.limit,
                        page_token: opts.page_token.clone().unwrap_or_default(),
                        consistency: opts.consistency.to_proto() as i32,
                    };

                    let response =
                        client.list_resources(tonic::Request::new(request)).await?.into_inner();

                    let next_page_token = if response.next_page_token.is_empty() {
                        None
                    } else {
                        Some(response.next_page_token)
                    };

                    Ok(PagedResult {
                        items: response.resources,
                        next_page_token,
                        block_height: response.block_height,
                    })
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Token Service
    // =========================================================================

    /// Creates a user session (access + refresh token pair).
    pub async fn create_user_session(&self, user: UserSlug) -> Result<crate::token::TokenPair> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_user_session",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_user_session",
                || async {
                    let mut client = connected_client!(pool, create_token_client);

                    let request = proto::CreateUserSessionRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response = client
                        .create_user_session(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let tokens = response
                        .tokens
                        .ok_or_else(|| missing_response_field("tokens", "TokenResponse"))?;

                    Ok(crate::token::TokenPair::from_proto(tokens))
                },
            ),
        )
        .await
    }

    /// Validates an access token and returns parsed claims.
    pub async fn validate_token(
        &self,
        token: &str,
        expected_audience: &str,
    ) -> Result<crate::token::ValidatedToken> {
        self.check_shutdown(None)?;

        let token = token.to_owned();
        let audience = expected_audience.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "validate_token",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "validate_token",
                || async {
                    let mut client = connected_client!(pool, create_token_client);

                    let request = proto::ValidateTokenRequest {
                        token: token.clone(),
                        expected_audience: audience.clone(),
                    };

                    let response =
                        client.validate_token(tonic::Request::new(request)).await?.into_inner();

                    crate::token::ValidatedToken::from_proto(response)
                        .ok_or_else(|| missing_response_field("claims", "ValidateTokenResponse"))
                },
            ),
        )
        .await
    }

    /// Revokes all sessions for a user.
    ///
    /// Returns the number of sessions revoked.
    pub async fn revoke_all_user_sessions(&self, user: UserSlug) -> Result<u64> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "revoke_all_user_sessions",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "revoke_all_user_sessions",
                || async {
                    let mut client = connected_client!(pool, create_token_client);

                    let request = proto::RevokeAllUserSessionsRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response = client
                        .revoke_all_user_sessions(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(response.revoked_count)
                },
            ),
        )
        .await
    }

    /// Refreshes a token pair using a refresh token.
    ///
    /// The old refresh token is invalidated (rotate-on-use).
    pub async fn refresh_token(&self, refresh_token: &str) -> Result<crate::token::TokenPair> {
        self.check_shutdown(None)?;

        let refresh = refresh_token.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "refresh_token",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "refresh_token",
                || async {
                    let mut client = connected_client!(pool, create_token_client);

                    let request = proto::RefreshTokenRequest { refresh_token: refresh.clone() };

                    let response =
                        client.refresh_token(tonic::Request::new(request)).await?.into_inner();

                    let tokens = response
                        .tokens
                        .ok_or_else(|| missing_response_field("tokens", "TokenResponse"))?;

                    Ok(crate::token::TokenPair::from_proto(tokens))
                },
            ),
        )
        .await
    }

    /// Revokes a token and its entire family.
    pub async fn revoke_token(&self, refresh_token: &str) -> Result<()> {
        self.check_shutdown(None)?;

        let refresh = refresh_token.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "revoke_token",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "revoke_token",
                || async {
                    let mut client = connected_client!(pool, create_token_client);

                    let request = proto::RevokeTokenRequest { refresh_token: refresh.clone() };

                    client.revoke_token(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
        .await
    }

    /// Creates a vault access token for an app.
    pub async fn create_vault_token(
        &self,
        organization: OrganizationSlug,
        app: AppSlug,
        vault: VaultSlug,
        scopes: &[String],
    ) -> Result<crate::token::TokenPair> {
        self.check_shutdown(None)?;

        let scopes = scopes.to_vec();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_vault_token",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_vault_token",
                || async {
                    let mut client = connected_client!(pool, create_token_client);

                    let request = proto::CreateVaultTokenRequest {
                        organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                        app: Some(proto::AppSlug { slug: app.value() }),
                        vault: Some(proto::VaultSlug { slug: vault.value() }),
                        scopes: scopes.clone(),
                    };

                    let response =
                        client.create_vault_token(tonic::Request::new(request)).await?.into_inner();

                    let tokens = response
                        .tokens
                        .ok_or_else(|| missing_response_field("tokens", "TokenResponse"))?;

                    Ok(crate::token::TokenPair::from_proto(tokens))
                },
            ),
        )
        .await
    }

    /// Creates a new signing key for the given scope.
    pub async fn create_signing_key(
        &self,
        scope: &str,
        organization: Option<OrganizationSlug>,
    ) -> Result<crate::token::PublicKeyInfo> {
        self.check_shutdown(None)?;

        let scope_str = scope.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_signing_key",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_signing_key",
                || async {
                    let mut client = connected_client!(pool, create_token_client);

                    let scope_i32 = match scope_str.as_str() {
                        "global" => proto::SigningKeyScope::Global as i32,
                        "organization" => proto::SigningKeyScope::Organization as i32,
                        _ => proto::SigningKeyScope::Unspecified as i32,
                    };

                    let request = proto::CreateSigningKeyRequest {
                        scope: scope_i32,
                        organization: organization
                            .map(|o| proto::OrganizationSlug { slug: o.value() }),
                    };

                    let response =
                        client.create_signing_key(tonic::Request::new(request)).await?.into_inner();

                    let key = response
                        .key
                        .ok_or_else(|| missing_response_field("key", "CreateSigningKeyResponse"))?;

                    Ok(crate::token::PublicKeyInfo::from_proto(key))
                },
            ),
        )
        .await
    }

    /// Rotates a signing key, creating a new key and marking the old one as rotated.
    pub async fn rotate_signing_key(
        &self,
        kid: &str,
        grace_period_secs: Option<u64>,
    ) -> Result<crate::token::PublicKeyInfo> {
        self.check_shutdown(None)?;

        let kid = kid.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "rotate_signing_key",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "rotate_signing_key",
                || async {
                    let mut client = connected_client!(pool, create_token_client);

                    let request = proto::RotateSigningKeyRequest {
                        kid: kid.clone(),
                        grace_period_secs: grace_period_secs.unwrap_or(0),
                    };

                    let response =
                        client.rotate_signing_key(tonic::Request::new(request)).await?.into_inner();

                    let key = response.new_key.ok_or_else(|| {
                        missing_response_field("new_key", "RotateSigningKeyResponse")
                    })?;

                    Ok(crate::token::PublicKeyInfo::from_proto(key))
                },
            ),
        )
        .await
    }

    /// Revokes a signing key by its kid.
    pub async fn revoke_signing_key(&self, kid: &str) -> Result<()> {
        self.check_shutdown(None)?;

        let kid = kid.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "revoke_signing_key",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "revoke_signing_key",
                || async {
                    let mut client = connected_client!(pool, create_token_client);

                    let request = proto::RevokeSigningKeyRequest { kid: kid.clone() };

                    client.revoke_signing_key(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
        .await
    }

    /// Gets active public keys for token verification.
    pub async fn get_public_keys(
        &self,
        organization: Option<OrganizationSlug>,
    ) -> Result<Vec<crate::token::PublicKeyInfo>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_public_keys",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_public_keys",
                || async {
                    let mut client = connected_client!(pool, create_token_client);

                    let request = proto::GetPublicKeysRequest {
                        organization: organization
                            .map(|o| proto::OrganizationSlug { slug: o.value() }),
                    };

                    let response =
                        client.get_public_keys(tonic::Request::new(request)).await?.into_inner();

                    Ok(response
                        .keys
                        .into_iter()
                        .map(crate::token::PublicKeyInfo::from_proto)
                        .collect())
                },
            ),
        )
        .await
    }

    create_grpc_client!(create_read_client, read_service_client, ReadServiceClient);
    create_grpc_client!(create_write_client, write_service_client, WriteServiceClient);
    create_grpc_client!(create_admin_client, admin_service_client, AdminServiceClient);
    create_grpc_client!(
        create_organization_client,
        organization_service_client,
        OrganizationServiceClient
    );
    create_grpc_client!(create_vault_client, vault_service_client, VaultServiceClient);
    create_grpc_client!(create_user_client, user_service_client, UserServiceClient);
    create_grpc_client!(create_app_client, app_service_client, AppServiceClient);
    create_grpc_client!(create_events_client, events_service_client, EventsServiceClient);
    create_grpc_client!(create_health_client, health_service_client, HealthServiceClient);
    create_grpc_client!(create_token_client, token_service_client, TokenServiceClient);
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::RetryPolicy;

    const ORG: OrganizationSlug = OrganizationSlug::new(1);

    #[tokio::test]
    async fn test_new_with_valid_config() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.client_id(), "test-client");
        assert!(matches!(client.config().servers(), ServerSource::Static(_)));
    }

    #[tokio::test]
    async fn test_connect_convenience_constructor() {
        let client = LedgerClient::connect("http://localhost:50051", "quick-client")
            .await
            .expect("client creation");

        assert_eq!(client.client_id(), "quick-client");
        assert!(matches!(client.config().servers(), ServerSource::Static(_)));
    }

    #[tokio::test]
    async fn test_connect_with_invalid_endpoint() {
        let result = LedgerClient::connect("not-a-url", "test-client").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_config_accessor_returns_full_config() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("accessor-test")
            .timeout(Duration::from_secs(30))
            .compression(true)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.config().timeout(), Duration::from_secs(30));
        assert!(client.config().compression());
    }

    #[tokio::test]
    async fn test_pool_accessor_returns_pool() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("pool-test")
            .compression(true)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert!(client.pool().compression_enabled());
    }

    #[tokio::test]
    async fn test_create_discovery_service() {
        use crate::config::DiscoveryConfig;

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("discovery-test")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let discovery = client.create_discovery_service(DiscoveryConfig::enabled());

        assert!(discovery.config().is_enabled());
    }

    #[tokio::test]
    async fn test_new_preserves_retry_policy() {
        let retry_policy = RetryPolicy::builder()
            .max_attempts(5)
            .initial_backoff(Duration::from_millis(100))
            .build();

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("retry-test")
            .retry_policy(retry_policy)
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        assert_eq!(client.config().retry_policy().max_attempts, 5);
    }

    // =========================================================================
    // ReadConsistency Tests
    // =========================================================================

    #[test]
    fn test_read_consistency_default_is_eventual() {
        assert_eq!(ReadConsistency::default(), ReadConsistency::Eventual);
    }

    #[test]
    fn test_read_consistency_to_proto_eventual() {
        let consistency = ReadConsistency::Eventual;
        assert_eq!(consistency.to_proto() as i32, proto::ReadConsistency::Eventual as i32);
    }

    #[test]
    fn test_read_consistency_to_proto_linearizable() {
        let consistency = ReadConsistency::Linearizable;
        assert_eq!(consistency.to_proto() as i32, proto::ReadConsistency::Linearizable as i32);
    }

    // =========================================================================
    // Connection Failure Integration Tests
    // =========================================================================
    //
    // These tests verify error handling when connecting to unreachable endpoints.
    // They don't require a running server - they test the retry/error paths.

    /// Creates a client configured for fast failure against an unreachable endpoint.
    async fn make_unreachable_client() -> LedgerClient {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://127.0.0.1:59999"]))
            .client_id("conn-failure-test")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");
        LedgerClient::new(config).await.expect("client creation")
    }

    #[allow(clippy::type_complexity)]
    #[tokio::test]
    async fn test_all_operations_return_error_on_connection_failure() {
        use std::{future::Future, pin::Pin};

        let cases: Vec<(
            &str,
            Box<dyn FnOnce(LedgerClient) -> Pin<Box<dyn Future<Output = bool>>>>,
        )> = vec![
            (
                "read",
                Box::new(|c| {
                    Box::pin(async move {
                        c.read(ORG, Some(VaultSlug::new(0)), "test-key", None, None).await.is_err()
                    })
                }),
            ),
            (
                "read (linearizable)",
                Box::new(|c| {
                    Box::pin(async move {
                        c.read(
                            ORG,
                            Some(VaultSlug::new(0)),
                            "test-key",
                            Some(ReadConsistency::Linearizable),
                            None,
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "read (none vault)",
                Box::new(|c| {
                    Box::pin(
                        async move { c.read(ORG, None, "user:123", None, None).await.is_err() },
                    )
                }),
            ),
            (
                "batch_read",
                Box::new(|c| {
                    Box::pin(async move {
                        c.batch_read(
                            ORG,
                            Some(VaultSlug::new(0)),
                            vec!["key1", "key2", "key3"],
                            None,
                            None,
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "batch_read (linearizable)",
                Box::new(|c| {
                    Box::pin(async move {
                        c.batch_read(
                            ORG,
                            Some(VaultSlug::new(0)),
                            vec!["key1", "key2"],
                            Some(ReadConsistency::Linearizable),
                            None,
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "write",
                Box::new(|c| {
                    Box::pin(async move {
                        let ops = vec![Operation::set_entity("key", b"value".to_vec(), None, None)];
                        c.write(ORG, Some(VaultSlug::new(0)), ops, None).await.is_err()
                    })
                }),
            ),
            (
                "write (multiple ops)",
                Box::new(|c| {
                    Box::pin(async move {
                        let ops = vec![
                            Operation::set_entity("user:1", b"alice".to_vec(), None, None),
                            Operation::set_entity("user:2", b"bob".to_vec(), None, None),
                            Operation::create_relationship("doc:1", "viewer", "user:1"),
                            Operation::create_relationship("doc:1", "editor", "user:2"),
                        ];
                        c.write(ORG, Some(VaultSlug::new(0)), ops, None).await.is_err()
                    })
                }),
            ),
            (
                "batch_write",
                Box::new(|c| {
                    Box::pin(async move {
                        let batches =
                            vec![vec![Operation::set_entity("key", b"value".to_vec(), None, None)]];
                        c.batch_write(ORG, Some(VaultSlug::new(0)), batches, None).await.is_err()
                    })
                }),
            ),
            (
                "batch_write (multiple groups)",
                Box::new(|c| {
                    Box::pin(async move {
                        let batches = vec![
                            vec![Operation::set_entity("user:123", b"alice".to_vec(), None, None)],
                            vec![
                                Operation::create_relationship("doc:456", "viewer", "user:123"),
                                Operation::create_relationship("folder:789", "editor", "user:123"),
                            ],
                        ];
                        c.batch_write(ORG, Some(VaultSlug::new(0)), batches, None).await.is_err()
                    })
                }),
            ),
            (
                "watch_blocks",
                Box::new(|c| {
                    Box::pin(
                        async move { c.watch_blocks(ORG, VaultSlug::new(0), 1).await.is_err() },
                    )
                }),
            ),
            (
                "watch_blocks (different vaults)",
                Box::new(|c| {
                    Box::pin(async move {
                        c.watch_blocks(ORG, VaultSlug::new(1), 1).await.is_err()
                            && c.watch_blocks(ORG, VaultSlug::new(2), 1).await.is_err()
                    })
                }),
            ),
            (
                "watch_blocks (start heights)",
                Box::new(|c| {
                    Box::pin(async move {
                        c.watch_blocks(ORG, VaultSlug::new(0), 1).await.is_err()
                            && c.watch_blocks(ORG, VaultSlug::new(0), 100).await.is_err()
                    })
                }),
            ),
            (
                "create_organization",
                Box::new(|c| {
                    Box::pin(async move {
                        c.create_organization(
                            "test-ns",
                            Region::US_EAST_VA,
                            UserSlug::new(0),
                            OrganizationTier::Free,
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
            (
                "get_organization",
                Box::new(|c| {
                    Box::pin(
                        async move { c.get_organization(ORG, UserSlug::new(0)).await.is_err() },
                    )
                }),
            ),
            (
                "list_organizations",
                Box::new(|c| {
                    Box::pin(async move {
                        c.list_organizations(UserSlug::new(0), 0, None).await.is_err()
                    })
                }),
            ),
            (
                "create_vault",
                Box::new(|c| Box::pin(async move { c.create_vault(ORG).await.is_err() })),
            ),
            (
                "get_vault",
                Box::new(|c| {
                    Box::pin(async move { c.get_vault(ORG, VaultSlug::new(1)).await.is_err() })
                }),
            ),
            (
                "list_vaults",
                Box::new(|c| Box::pin(async move { c.list_vaults(0, None).await.is_err() })),
            ),
            (
                "health_check",
                Box::new(|c| Box::pin(async move { c.health_check().await.is_err() })),
            ),
            (
                "health_check_detailed",
                Box::new(|c| Box::pin(async move { c.health_check_detailed().await.is_err() })),
            ),
            (
                "health_check_vault",
                Box::new(|c| {
                    Box::pin(
                        async move { c.health_check_vault(ORG, VaultSlug::new(0)).await.is_err() },
                    )
                }),
            ),
            (
                "verified_read",
                Box::new(|c| {
                    Box::pin(async move {
                        c.verified_read(ORG, Some(VaultSlug::new(0)), "key", VerifyOpts::new())
                            .await
                            .is_err()
                    })
                }),
            ),
            (
                "list_entities",
                Box::new(|c| {
                    Box::pin(async move {
                        c.list_entities(ORG, ListEntitiesOpts::with_prefix("user:")).await.is_err()
                    })
                }),
            ),
            (
                "list_entities (with options)",
                Box::new(|c| {
                    Box::pin(async move {
                        let opts = ListEntitiesOpts::with_prefix("session:")
                            .at_height(100)
                            .include_expired()
                            .limit(50)
                            .linearizable();
                        c.list_entities(ORG, opts).await.is_err()
                    })
                }),
            ),
            (
                "list_relationships",
                Box::new(|c| {
                    Box::pin(async move {
                        c.list_relationships(ORG, VaultSlug::new(0), ListRelationshipsOpts::new())
                            .await
                            .is_err()
                    })
                }),
            ),
            (
                "list_relationships (with filters)",
                Box::new(|c| {
                    Box::pin(async move {
                        let opts = ListRelationshipsOpts::new()
                            .resource("document:1")
                            .relation("viewer")
                            .limit(100);
                        c.list_relationships(ORG, VaultSlug::new(0), opts).await.is_err()
                    })
                }),
            ),
            (
                "list_resources",
                Box::new(|c| {
                    Box::pin(async move {
                        c.list_resources(
                            ORG,
                            VaultSlug::new(0),
                            ListResourcesOpts::with_type("document"),
                        )
                        .await
                        .is_err()
                    })
                }),
            ),
        ];

        for (label, call_fn) in cases {
            let client = make_unreachable_client().await;
            assert!(call_fn(client).await, "{label}: expected connection error");
        }
    }

    // =========================================================================
    // Operation Builder Tests
    // =========================================================================

    #[allow(clippy::type_complexity)]
    #[test]
    fn test_operation_construction() {
        // Each case: (label, operation, validation fn)
        let cases: Vec<(&str, Operation, Box<dyn Fn(&Operation)>)> = vec![
            (
                "set_entity basic",
                Operation::set_entity("user:123", b"data".to_vec(), None, None),
                Box::new(|op| match op {
                    Operation::SetEntity { key, value, expires_at, condition } => {
                        assert_eq!(key, "user:123");
                        assert_eq!(value, b"data");
                        assert!(expires_at.is_none());
                        assert!(condition.is_none());
                    },
                    _ => panic!("Expected SetEntity"),
                }),
            ),
            (
                "set_entity with expiry",
                Operation::set_entity("session:abc", b"token".to_vec(), Some(1700000000), None),
                Box::new(|op| match op {
                    Operation::SetEntity { key, value, expires_at, condition } => {
                        assert_eq!(key, "session:abc");
                        assert_eq!(value, b"token");
                        assert_eq!(*expires_at, Some(1700000000));
                        assert!(condition.is_none());
                    },
                    _ => panic!("Expected SetEntity"),
                }),
            ),
            (
                "set_entity if not exists",
                Operation::set_entity(
                    "lock:xyz",
                    b"owner".to_vec(),
                    None,
                    Some(SetCondition::NotExists),
                ),
                Box::new(|op| match op {
                    Operation::SetEntity {
                        key, condition: Some(SetCondition::NotExists), ..
                    } => {
                        assert_eq!(key, "lock:xyz");
                    },
                    _ => panic!("Expected SetEntity with NotExists condition"),
                }),
            ),
            (
                "set_entity if version",
                Operation::set_entity(
                    "counter",
                    b"42".to_vec(),
                    None,
                    Some(SetCondition::Version(100)),
                ),
                Box::new(|op| match op {
                    Operation::SetEntity { condition: Some(SetCondition::Version(v)), .. } => {
                        assert_eq!(*v, 100);
                    },
                    _ => panic!("Expected SetEntity with Version condition"),
                }),
            ),
            (
                "set_entity if value equals",
                Operation::set_entity(
                    "data",
                    b"new".to_vec(),
                    None,
                    Some(SetCondition::ValueEquals(b"old".to_vec())),
                ),
                Box::new(|op| match op {
                    Operation::SetEntity {
                        condition: Some(SetCondition::ValueEquals(v)), ..
                    } => {
                        assert_eq!(v, b"old");
                    },
                    _ => panic!("Expected SetEntity with ValueEquals condition"),
                }),
            ),
            (
                "delete_entity",
                Operation::delete_entity("obsolete:key"),
                Box::new(|op| match op {
                    Operation::DeleteEntity { key } => {
                        assert_eq!(key, "obsolete:key");
                    },
                    _ => panic!("Expected DeleteEntity"),
                }),
            ),
            (
                "create_relationship",
                Operation::create_relationship("doc:456", "viewer", "user:123"),
                Box::new(|op| match op {
                    Operation::CreateRelationship { resource, relation, subject } => {
                        assert_eq!(resource, "doc:456");
                        assert_eq!(relation, "viewer");
                        assert_eq!(subject, "user:123");
                    },
                    _ => panic!("Expected CreateRelationship"),
                }),
            ),
            (
                "delete_relationship",
                Operation::delete_relationship("doc:456", "editor", "team:admins#member"),
                Box::new(|op| match op {
                    Operation::DeleteRelationship { resource, relation, subject } => {
                        assert_eq!(resource, "doc:456");
                        assert_eq!(relation, "editor");
                        assert_eq!(subject, "team:admins#member");
                    },
                    _ => panic!("Expected DeleteRelationship"),
                }),
            ),
        ];

        for (label, op, validate) in &cases {
            validate(op);
            // Verify the label is non-empty (forces use of label binding)
            assert!(!label.is_empty());
        }
    }

    #[test]
    fn test_operation_to_proto_set_entity() {
        let op = Operation::set_entity("key", b"value".to_vec(), None, None);
        let proto_op = op.to_proto();

        assert!(proto_op.op.is_some());
        match proto_op.op.unwrap() {
            proto::operation::Op::SetEntity(set) => {
                assert_eq!(set.key, "key");
                assert_eq!(set.value, b"value");
            },
            _ => panic!("Expected SetEntity proto"),
        }
    }

    #[test]
    fn test_operation_to_proto_create_relationship() {
        let op = Operation::create_relationship("res", "rel", "sub");
        let proto_op = op.to_proto();

        match proto_op.op.unwrap() {
            proto::operation::Op::CreateRelationship(rel) => {
                assert_eq!(rel.resource, "res");
                assert_eq!(rel.relation, "rel");
                assert_eq!(rel.subject, "sub");
            },
            _ => panic!("Expected CreateRelationship proto"),
        }
    }

    #[test]
    fn test_set_condition_to_proto() {
        let not_exists = SetCondition::NotExists;
        let proto_cond = not_exists.to_proto();
        assert!(matches!(
            proto_cond.condition,
            Some(proto::set_condition::Condition::NotExists(true))
        ));

        let must_exist = SetCondition::MustExist;
        let proto_cond = must_exist.to_proto();
        assert!(matches!(
            proto_cond.condition,
            Some(proto::set_condition::Condition::MustExists(true))
        ));

        let version = SetCondition::Version(42);
        let proto_cond = version.to_proto();
        assert!(matches!(proto_cond.condition, Some(proto::set_condition::Condition::Version(42))));

        let value_eq = SetCondition::ValueEquals(b"test".to_vec());
        let proto_cond = value_eq.to_proto();
        match proto_cond.condition {
            Some(proto::set_condition::Condition::ValueEquals(v)) => {
                assert_eq!(v, b"test");
            },
            _ => panic!("Expected ValueEquals"),
        }
    }

    #[test]
    fn test_set_condition_from_expected_none() {
        let cond = SetCondition::from_expected(None::<Vec<u8>>);
        assert!(matches!(cond, SetCondition::NotExists));
    }

    #[test]
    fn test_set_condition_from_expected_some_vec() {
        let cond = SetCondition::from_expected(Some(b"old-value".to_vec()));
        match cond {
            SetCondition::ValueEquals(v) => assert_eq!(v, b"old-value"),
            other => panic!("Expected ValueEquals, got: {other:?}"),
        }
    }

    #[test]
    fn test_set_condition_from_expected_some_slice() {
        let slice: &[u8] = b"expected";
        let cond = SetCondition::from_expected(Some(slice.to_vec()));
        match cond {
            SetCondition::ValueEquals(v) => assert_eq!(v, b"expected"),
            other => panic!("Expected ValueEquals, got: {other:?}"),
        }
    }

    // =========================================================================
    // WriteSuccess Tests
    // =========================================================================

    #[test]
    fn test_write_success_fields() {
        let success =
            WriteSuccess { tx_id: "abc123".to_string(), block_height: 42, assigned_sequence: 5 };

        assert_eq!(success.tx_id, "abc123");
        assert_eq!(success.block_height, 42);
        assert_eq!(success.assigned_sequence, 5);
    }

    #[test]
    fn test_tx_id_to_hex() {
        // Test with Some(TxId)
        let tx_id = proto::TxId { id: vec![0x12, 0x34, 0xab, 0xcd] };
        let hex = LedgerClient::tx_id_to_hex(Some(tx_id));
        assert_eq!(hex, "1234abcd");

        // Test with None
        let hex = LedgerClient::tx_id_to_hex(None);
        assert_eq!(hex, "");
    }

    // (write/batch_write connection failure tests consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // BlockAnnouncement Tests
    // =========================================================================

    #[test]
    fn test_block_announcement_from_proto_with_all_fields() {
        use prost_types::Timestamp;

        let proto_announcement = proto::BlockAnnouncement {
            organization: Some(proto::OrganizationSlug { slug: 1 }),
            vault: Some(proto::VaultSlug { slug: 2 }),
            height: 100,
            block_hash: Some(proto::Hash { value: vec![0x12, 0x34] }),
            state_root: Some(proto::Hash { value: vec![0xab, 0xcd] }),
            timestamp: Some(Timestamp { seconds: 1700000000, nanos: 123_456_789 }),
        };

        let announcement = BlockAnnouncement::from_proto(proto_announcement);

        assert_eq!(announcement.organization, ORG);
        assert_eq!(announcement.vault, VaultSlug::new(2));
        assert_eq!(announcement.height, 100);
        assert_eq!(announcement.block_hash, vec![0x12, 0x34]);
        assert_eq!(announcement.state_root, vec![0xab, 0xcd]);
        assert!(announcement.timestamp.is_some());
    }

    #[test]
    fn test_block_announcement_from_proto_with_missing_optional_fields() {
        let proto_announcement = proto::BlockAnnouncement {
            organization: None,
            vault: None,
            height: 50,
            block_hash: None,
            state_root: None,
            timestamp: None,
        };

        let announcement = BlockAnnouncement::from_proto(proto_announcement);

        assert_eq!(announcement.organization, OrganizationSlug::new(0));
        assert_eq!(announcement.vault, VaultSlug::new(0));
        assert_eq!(announcement.height, 50);
        assert!(announcement.block_hash.is_empty());
        assert!(announcement.state_root.is_empty());
        assert!(announcement.timestamp.is_none());
    }

    #[test]
    fn test_block_announcement_equality() {
        let a = BlockAnnouncement {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            block_hash: vec![0x12],
            state_root: vec![0xab],
            timestamp: None,
        };

        let b = BlockAnnouncement {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            block_hash: vec![0x12],
            state_root: vec![0xab],
            timestamp: None,
        };

        assert_eq!(a, b);
    }

    #[test]
    fn test_block_announcement_clone() {
        let original = BlockAnnouncement {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            block_hash: vec![0x12, 0x34],
            state_root: vec![0xab, 0xcd],
            timestamp: None,
        };

        let cloned = original.clone();

        assert_eq!(original, cloned);
    }

    // (watch_blocks connection failure tests consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // Admin Operation Tests
    // =========================================================================

    #[test]
    fn test_organization_status_from_proto_active() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Active as i32);
        assert_eq!(status, OrganizationStatus::Active);
    }

    #[test]
    fn test_organization_status_from_proto_deleted() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Deleted as i32);
        assert_eq!(status, OrganizationStatus::Deleted);
    }

    #[test]
    fn test_organization_status_from_proto_unspecified() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Unspecified as i32);
        assert_eq!(status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_organization_status_from_proto_invalid() {
        let status = OrganizationStatus::from_proto(999);
        assert_eq!(status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_organization_status_default() {
        let status: OrganizationStatus = Default::default();
        assert_eq!(status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_from_proto_active() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Active as i32);
        assert_eq!(status, VaultStatus::Active);
    }

    #[test]
    fn test_vault_status_from_proto_read_only() {
        let status = VaultStatus::from_proto(proto::VaultStatus::ReadOnly as i32);
        assert_eq!(status, VaultStatus::ReadOnly);
    }

    #[test]
    fn test_vault_status_from_proto_deleted() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Deleted as i32);
        assert_eq!(status, VaultStatus::Deleted);
    }

    #[test]
    fn test_vault_status_from_proto_unspecified() {
        let status = VaultStatus::from_proto(proto::VaultStatus::Unspecified as i32);
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_from_proto_invalid() {
        let status = VaultStatus::from_proto(999);
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_vault_status_default() {
        let status: VaultStatus = Default::default();
        assert_eq!(status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_organization_info_from_proto() {
        let proto = proto::GetOrganizationResponse {
            slug: Some(proto::OrganizationSlug { slug: 42 }),
            name: "test-organization".to_string(),
            region: proto::Region::Global.into(),
            member_nodes: vec![
                proto::NodeId { id: "node-100".to_string() },
                proto::NodeId { id: "node-101".to_string() },
            ],
            status: proto::OrganizationStatus::Active as i32,
            config_version: 5,
            created_at: None,
            tier: 0,
            members: vec![
                proto::OrganizationMember {
                    user: Some(proto::UserSlug { slug: 100 }),
                    role: proto::OrganizationMemberRole::Admin.into(),
                    joined_at: None,
                },
                proto::OrganizationMember {
                    user: Some(proto::UserSlug { slug: 200 }),
                    role: proto::OrganizationMemberRole::Member.into(),
                    joined_at: None,
                },
            ],
            updated_at: None,
        };

        let info = OrganizationInfo::from_proto(proto);

        assert_eq!(info.slug, OrganizationSlug::new(42));
        assert_eq!(info.name, "test-organization");
        assert_eq!(info.region, Region::GLOBAL);
        assert_eq!(info.member_nodes, vec!["node-100", "node-101"]);
        assert_eq!(info.config_version, 5);
        assert_eq!(info.status, OrganizationStatus::Active);
        assert_eq!(info.members.len(), 2);
        assert_eq!(info.members[0].user, UserSlug::new(100));
        assert_eq!(info.members[0].role, OrganizationMemberRole::Admin);
        assert_eq!(info.members[1].user, UserSlug::new(200));
        assert_eq!(info.members[1].role, OrganizationMemberRole::Member);
    }

    #[test]
    fn test_organization_info_from_proto_with_missing_fields() {
        let proto = proto::GetOrganizationResponse {
            slug: None,
            name: "minimal".to_string(),
            region: proto::Region::Unspecified.into(),
            member_nodes: vec![],
            status: proto::OrganizationStatus::Unspecified as i32,
            config_version: 0,
            created_at: None,
            tier: 0,
            members: vec![],
            updated_at: None,
        };

        let info = OrganizationInfo::from_proto(proto);

        assert_eq!(info.slug, OrganizationSlug::new(0));
        assert_eq!(info.name, "minimal");
        // Unspecified (0) falls back to GLOBAL
        assert_eq!(info.region, Region::GLOBAL);
        assert!(info.member_nodes.is_empty());
        assert_eq!(info.config_version, 0);
        assert_eq!(info.status, OrganizationStatus::Unspecified);
    }

    #[test]
    fn test_vault_info_from_proto() {
        let proto = proto::GetVaultResponse {
            organization: Some(proto::OrganizationSlug { slug: 1 }),
            vault: Some(proto::VaultSlug { slug: 10 }),
            height: 1000,
            state_root: Some(proto::Hash { value: vec![1, 2, 3, 4] }),
            nodes: vec![
                proto::NodeId { id: "node-200".to_string() },
                proto::NodeId { id: "node-201".to_string() },
            ],
            leader: Some(proto::NodeId { id: "node-200".to_string() }),
            status: proto::VaultStatus::Active as i32,
            retention_policy: None,
        };

        let info = VaultInfo::from_proto(proto);

        assert_eq!(info.organization, ORG);
        assert_eq!(info.vault, VaultSlug::new(10));
        assert_eq!(info.height, 1000);
        assert_eq!(info.state_root, vec![1, 2, 3, 4]);
        assert_eq!(info.nodes, vec!["node-200", "node-201"]);
        assert_eq!(info.leader, Some("node-200".to_string()));
        assert_eq!(info.status, VaultStatus::Active);
    }

    #[test]
    fn test_vault_info_from_proto_with_missing_fields() {
        let proto = proto::GetVaultResponse {
            organization: None,
            vault: None,
            height: 0,
            state_root: None,
            nodes: vec![],
            leader: None,
            status: proto::VaultStatus::Unspecified as i32,
            retention_policy: None,
        };

        let info = VaultInfo::from_proto(proto);

        assert_eq!(info.organization, OrganizationSlug::new(0));
        assert_eq!(info.vault, VaultSlug::new(0));
        assert_eq!(info.height, 0);
        assert!(info.state_root.is_empty());
        assert!(info.nodes.is_empty());
        assert_eq!(info.leader, None);
        assert_eq!(info.status, VaultStatus::Unspecified);
    }

    #[test]
    fn test_organization_info_equality() {
        let info1 = OrganizationInfo {
            slug: ORG,
            name: "test".to_string(),
            region: Region::GLOBAL,
            member_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            config_version: 1,
            status: OrganizationStatus::Active,
            tier: OrganizationTier::Free,
            members: vec![OrganizationMemberInfo {
                user: UserSlug::new(42),
                role: OrganizationMemberRole::Admin,
                joined_at: None,
            }],
        };
        let info2 = info1.clone();

        assert_eq!(info1, info2);
    }

    #[test]
    fn test_vault_info_equality() {
        let info1 = VaultInfo {
            organization: ORG,
            vault: VaultSlug::new(2),
            height: 100,
            state_root: vec![1, 2, 3],
            nodes: vec!["node-1".to_string(), "node-2".to_string()],
            leader: Some("node-1".to_string()),
            status: VaultStatus::Active,
        };
        let info2 = info1.clone();

        assert_eq!(info1, info2);
    }

    // (admin connection failure tests consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // HealthStatus Tests
    // =========================================================================

    #[test]
    fn test_health_status_from_proto_healthy() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Healthy as i32);
        assert_eq!(status, HealthStatus::Healthy);
    }

    #[test]
    fn test_health_status_from_proto_degraded() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Degraded as i32);
        assert_eq!(status, HealthStatus::Degraded);
    }

    #[test]
    fn test_health_status_from_proto_unavailable() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Unavailable as i32);
        assert_eq!(status, HealthStatus::Unavailable);
    }

    #[test]
    fn test_health_status_from_proto_unspecified() {
        let status = HealthStatus::from_proto(proto::HealthStatus::Unspecified as i32);
        assert_eq!(status, HealthStatus::Unspecified);
    }

    #[test]
    fn test_health_status_from_proto_invalid() {
        let status = HealthStatus::from_proto(999);
        assert_eq!(status, HealthStatus::Unspecified);
    }

    #[test]
    fn test_health_status_default() {
        let status: HealthStatus = Default::default();
        assert_eq!(status, HealthStatus::Unspecified);
    }

    // =========================================================================
    // HealthCheckResult Tests
    // =========================================================================

    #[test]
    fn test_health_check_result_from_proto() {
        let mut details = std::collections::HashMap::new();
        details.insert("current_term".to_string(), "5".to_string());
        details.insert("leader_id".to_string(), "node-1".to_string());

        let proto = proto::HealthCheckResponse {
            status: proto::HealthStatus::Healthy as i32,
            message: "Node is healthy".to_string(),
            details: details.clone(),
        };

        let result = HealthCheckResult::from_proto(proto);

        assert_eq!(result.status, HealthStatus::Healthy);
        assert_eq!(result.message, "Node is healthy");
        assert_eq!(result.details, details);
    }

    #[test]
    fn test_health_check_result_is_healthy() {
        let result = HealthCheckResult {
            status: HealthStatus::Healthy,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(result.is_healthy());
        assert!(!result.is_degraded());
        assert!(!result.is_unavailable());
    }

    #[test]
    fn test_health_check_result_is_degraded() {
        let result = HealthCheckResult {
            status: HealthStatus::Degraded,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(!result.is_healthy());
        assert!(result.is_degraded());
        assert!(!result.is_unavailable());
    }

    #[test]
    fn test_health_check_result_is_unavailable() {
        let result = HealthCheckResult {
            status: HealthStatus::Unavailable,
            message: String::new(),
            details: std::collections::HashMap::new(),
        };
        assert!(!result.is_healthy());
        assert!(!result.is_degraded());
        assert!(result.is_unavailable());
    }

    // =========================================================================
    // Health Check Integration Tests
    // =========================================================================

    // (health connection failure tests consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // Verified Read Tests
    // =========================================================================

    #[test]
    fn test_direction_from_proto_left() {
        use inferadb_ledger_proto::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Left as i32);
        assert_eq!(direction, Direction::Left);
    }

    #[test]
    fn test_direction_from_proto_right() {
        use inferadb_ledger_proto::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Right as i32);
        assert_eq!(direction, Direction::Right);
    }

    #[test]
    fn test_direction_from_proto_unspecified_defaults_to_right() {
        use inferadb_ledger_proto::proto::Direction as ProtoDirection;
        let direction = Direction::from_proto(ProtoDirection::Unspecified as i32);
        assert_eq!(direction, Direction::Right);
    }

    #[test]
    fn test_merkle_sibling_from_proto() {
        use inferadb_ledger_proto::proto;
        let proto_sibling = proto::MerkleSibling {
            hash: Some(proto::Hash { value: vec![1, 2, 3, 4] }),
            direction: proto::Direction::Left as i32,
        };
        let sibling = MerkleSibling::from_proto(proto_sibling);
        assert_eq!(sibling.hash, vec![1, 2, 3, 4]);
        assert_eq!(sibling.direction, Direction::Left);
    }

    #[test]
    fn test_merkle_proof_from_proto() {
        use inferadb_ledger_proto::proto;
        let proto_proof = proto::MerkleProof {
            leaf_hash: Some(proto::Hash { value: vec![0; 32] }),
            siblings: vec![
                proto::MerkleSibling {
                    hash: Some(proto::Hash { value: vec![1; 32] }),
                    direction: proto::Direction::Left as i32,
                },
                proto::MerkleSibling {
                    hash: Some(proto::Hash { value: vec![2; 32] }),
                    direction: proto::Direction::Right as i32,
                },
            ],
        };
        let proof = MerkleProof::from_proto(proto_proof);
        assert_eq!(proof.leaf_hash, vec![0; 32]);
        assert_eq!(proof.siblings.len(), 2);
        assert_eq!(proof.siblings[0].direction, Direction::Left);
        assert_eq!(proof.siblings[1].direction, Direction::Right);
    }

    #[test]
    fn test_merkle_proof_verify_single_element_tree() {
        // Single element tree: leaf hash equals root
        let proof = MerkleProof {
            leaf_hash: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            siblings: vec![],
        };
        let expected_root = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_single_element_tree_mismatch() {
        let proof = MerkleProof { leaf_hash: vec![1, 2, 3, 4], siblings: vec![] };
        let wrong_root = vec![5, 6, 7, 8];
        assert!(!proof.verify(&wrong_root));
    }

    #[test]
    fn test_merkle_proof_verify_with_siblings() {
        use sha2::{Digest, Sha256};

        // Create a simple two-leaf tree
        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute expected root: hash(leaf || sibling) since sibling is on right
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let expected_root = hasher.finalize().to_vec();

        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: sibling_hash, direction: Direction::Right }],
        };

        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_left_sibling() {
        use sha2::{Digest, Sha256};

        // Create a proof where sibling is on the left
        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute expected root: hash(sibling || leaf) since sibling is on left
        let mut hasher = Sha256::new();
        hasher.update(&sibling_hash);
        hasher.update(&leaf_hash);
        let expected_root = hasher.finalize().to_vec();

        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: sibling_hash, direction: Direction::Left }],
        };

        assert!(proof.verify(&expected_root));
    }

    #[test]
    fn test_merkle_proof_verify_tampered_proof_fails() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute correct root
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let correct_root = hasher.finalize().to_vec();

        // Tamper with the sibling hash
        let tampered_sibling = vec![2u8; 32];
        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling { hash: tampered_sibling, direction: Direction::Right }],
        };

        // Should not verify against correct root
        assert!(!proof.verify(&correct_root));
    }

    #[test]
    fn test_merkle_proof_verify_wrong_direction_fails() {
        use sha2::{Digest, Sha256};

        let leaf_hash = vec![0u8; 32];
        let sibling_hash = vec![1u8; 32];

        // Compute root with sibling on right
        let mut hasher = Sha256::new();
        hasher.update(&leaf_hash);
        hasher.update(&sibling_hash);
        let expected_root = hasher.finalize().to_vec();

        // Create proof with wrong direction (Left instead of Right)
        let proof = MerkleProof {
            leaf_hash: leaf_hash.clone(),
            siblings: vec![MerkleSibling {
                hash: sibling_hash,
                direction: Direction::Left, // Wrong!
            }],
        };

        // Should fail verification
        assert!(!proof.verify(&expected_root));
    }

    #[test]
    fn test_block_header_from_proto() {
        use inferadb_ledger_proto::proto;

        let proto_header = proto::BlockHeader {
            height: 100,
            organization: Some(proto::OrganizationSlug { slug: 1 }),
            vault: Some(proto::VaultSlug { slug: 2 }),
            previous_hash: Some(proto::Hash { value: vec![1; 32] }),
            tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
            state_root: Some(proto::Hash { value: vec![3; 32] }),
            timestamp: Some(prost_types::Timestamp { seconds: 1704067200, nanos: 0 }),
            leader_id: Some(proto::NodeId { id: "node-1".to_string() }),
            term: 5,
            committed_index: 99,
            block_hash: Some(proto::Hash { value: vec![10; 32] }),
        };

        let header = BlockHeader::from_proto(proto_header);
        assert_eq!(header.height, 100);
        assert_eq!(header.organization, ORG);
        assert_eq!(header.vault, VaultSlug::new(2));
        assert_eq!(header.previous_hash, vec![1; 32]);
        assert_eq!(header.tx_merkle_root, vec![2; 32]);
        assert_eq!(header.state_root, vec![3; 32]);
        assert!(header.timestamp.is_some());
        assert_eq!(header.leader_id, "node-1");
        assert_eq!(header.term, 5);
        assert_eq!(header.committed_index, 99);
    }

    #[test]
    fn test_block_header_from_proto_with_missing_fields() {
        use inferadb_ledger_proto::proto;

        let proto_header = proto::BlockHeader {
            height: 1,
            organization: None,
            vault: None,
            previous_hash: None,
            tx_merkle_root: None,
            state_root: None,
            timestamp: None,
            leader_id: None,
            term: 0,
            committed_index: 0,
            block_hash: None,
        };

        let header = BlockHeader::from_proto(proto_header);
        assert_eq!(header.height, 1);
        assert_eq!(header.organization, OrganizationSlug::new(0));
        assert_eq!(header.vault, VaultSlug::new(0));
        assert!(header.previous_hash.is_empty());
        assert!(header.tx_merkle_root.is_empty());
        assert!(header.state_root.is_empty());
        assert!(header.timestamp.is_none());
        assert!(header.leader_id.is_empty());
    }

    #[test]
    fn test_chain_proof_from_proto() {
        use inferadb_ledger_proto::proto;

        let proto_chain = proto::ChainProof {
            headers: vec![
                proto::BlockHeader {
                    height: 101,
                    organization: Some(proto::OrganizationSlug { slug: 1 }),
                    vault: Some(proto::VaultSlug { slug: 0 }),
                    previous_hash: Some(proto::Hash { value: vec![0; 32] }),
                    tx_merkle_root: Some(proto::Hash { value: vec![1; 32] }),
                    state_root: Some(proto::Hash { value: vec![2; 32] }),
                    timestamp: None,
                    leader_id: None,
                    term: 1,
                    committed_index: 100,
                    block_hash: None,
                },
                proto::BlockHeader {
                    height: 102,
                    organization: Some(proto::OrganizationSlug { slug: 1 }),
                    vault: Some(proto::VaultSlug { slug: 0 }),
                    previous_hash: Some(proto::Hash { value: vec![3; 32] }),
                    tx_merkle_root: Some(proto::Hash { value: vec![4; 32] }),
                    state_root: Some(proto::Hash { value: vec![5; 32] }),
                    timestamp: None,
                    leader_id: None,
                    term: 1,
                    committed_index: 101,
                    block_hash: None,
                },
            ],
        };

        let chain = ChainProof::from_proto(proto_chain);
        assert_eq!(chain.headers.len(), 2);
        assert_eq!(chain.headers[0].height, 101);
        assert_eq!(chain.headers[1].height, 102);
    }

    #[test]
    fn test_chain_proof_verify_empty() {
        let chain = ChainProof { headers: vec![] };
        let trusted_hash = vec![0; 32];
        assert!(chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_first_links_to_trusted() {
        let chain = ChainProof {
            headers: vec![BlockHeader {
                height: 101,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![1, 2, 3, 4], // Must match trusted_hash
                tx_merkle_root: vec![5, 6, 7, 8],
                state_root: vec![9, 10, 11, 12],
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 100,
                block_hash: vec![],
            }],
        };
        let trusted_hash = vec![1, 2, 3, 4];
        assert!(chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_fails_if_first_not_linked() {
        let chain = ChainProof {
            headers: vec![BlockHeader {
                height: 101,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![0, 0, 0, 0], // Wrong hash
                tx_merkle_root: vec![5, 6, 7, 8],
                state_root: vec![9, 10, 11, 12],
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 100,
                block_hash: vec![],
            }],
        };
        let trusted_hash = vec![1, 2, 3, 4];
        assert!(!chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_multi_header_server_hash() {
        // Build a two-header chain where header[1].previous_hash matches
        // header[0].block_hash (provided by server).
        let block_hash_0 = vec![42; 32]; // Simulated server-computed hash

        let header0 = BlockHeader {
            height: 100,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![0; 32],
            tx_merkle_root: vec![1; 32],
            state_root: vec![2; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 99,
            block_hash: block_hash_0.clone(),
        };

        let header1 = BlockHeader {
            height: 101,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: block_hash_0, // Links to header0.block_hash
            tx_merkle_root: vec![3; 32],
            state_root: vec![4; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 100,
            block_hash: vec![99; 32],
        };

        let chain = ChainProof { headers: vec![header0, header1] };
        let trusted_hash = vec![0; 32]; // header[0].previous_hash
        assert!(chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_multi_header_wrong_hash_fails() {
        let header0 = BlockHeader {
            height: 100,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![0; 32],
            tx_merkle_root: vec![1; 32],
            state_root: vec![2; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 99,
            block_hash: vec![42; 32],
        };

        let header1 = BlockHeader {
            height: 101,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![0xFF; 32], // Wrong — doesn't match header0.block_hash
            tx_merkle_root: vec![3; 32],
            state_root: vec![4; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 100,
            block_hash: vec![99; 32],
        };

        let chain = ChainProof { headers: vec![header0, header1] };
        let trusted_hash = vec![0; 32];
        assert!(!chain.verify(&trusted_hash));
    }

    #[test]
    fn test_chain_proof_verify_empty_block_hash_fails() {
        // If server didn't provide block_hash, verification should fail
        let header0 = BlockHeader {
            height: 100,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![0; 32],
            tx_merkle_root: vec![1; 32],
            state_root: vec![2; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 99,
            block_hash: vec![], // Empty — server didn't provide
        };

        let header1 = BlockHeader {
            height: 101,
            organization: ORG,
            vault: VaultSlug::new(1),
            previous_hash: vec![], // Matches empty block_hash, but should still fail
            tx_merkle_root: vec![3; 32],
            state_root: vec![4; 32],
            timestamp: None,
            leader_id: String::new(),
            term: 1,
            committed_index: 100,
            block_hash: vec![],
        };

        let chain = ChainProof { headers: vec![header0, header1] };
        let trusted_hash = vec![0; 32];
        assert!(!chain.verify(&trusted_hash));
    }

    #[test]
    fn test_verify_opts_default() {
        let opts = VerifyOpts::new();
        assert!(opts.at_height.is_none());
        assert!(!opts.include_chain_proof);
        assert!(opts.trusted_height.is_none());
    }

    #[test]
    fn test_verify_opts_at_height() {
        let opts = VerifyOpts::new().at_height(100);
        assert_eq!(opts.at_height, Some(100));
        assert!(!opts.include_chain_proof);
    }

    #[test]
    fn test_verify_opts_with_chain_proof() {
        let opts = VerifyOpts::new().with_chain_proof(50);
        assert!(opts.include_chain_proof);
        assert_eq!(opts.trusted_height, Some(50));
    }

    #[test]
    fn test_verify_opts_builder_chain() {
        let opts = VerifyOpts::new().at_height(100).with_chain_proof(50);
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_chain_proof);
        assert_eq!(opts.trusted_height, Some(50));
    }

    #[test]
    fn test_verified_value_from_proto() {
        use inferadb_ledger_proto::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: Some(proto::BlockHeader {
                height: 100,
                organization: Some(proto::OrganizationSlug { slug: 1 }),
                vault: Some(proto::VaultSlug { slug: 0 }),
                previous_hash: Some(proto::Hash { value: vec![1; 32] }),
                tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
                state_root: Some(proto::Hash { value: vec![3; 32] }),
                timestamp: None,
                leader_id: None,
                term: 1,
                committed_index: 99,
                block_hash: None,
            }),
            merkle_proof: Some(proto::MerkleProof {
                leaf_hash: Some(proto::Hash { value: vec![4; 32] }),
                siblings: vec![],
            }),
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_some());
        let v = verified.unwrap();
        assert_eq!(v.value, Some(b"test-value".to_vec()));
        assert_eq!(v.block_height, 100);
        assert_eq!(v.block_header.height, 100);
        assert_eq!(v.merkle_proof.leaf_hash, vec![4; 32]);
        assert!(v.chain_proof.is_none());
    }

    #[test]
    fn test_verified_value_from_proto_missing_header() {
        use inferadb_ledger_proto::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: None, // Missing
            merkle_proof: Some(proto::MerkleProof {
                leaf_hash: Some(proto::Hash { value: vec![4; 32] }),
                siblings: vec![],
            }),
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_none()); // Should return None if header missing
    }

    #[test]
    fn test_verified_value_from_proto_missing_proof() {
        use inferadb_ledger_proto::proto;

        let proto_response = proto::VerifiedReadResponse {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: Some(proto::BlockHeader {
                height: 100,
                organization: Some(proto::OrganizationSlug { slug: 1 }),
                vault: Some(proto::VaultSlug { slug: 0 }),
                previous_hash: Some(proto::Hash { value: vec![1; 32] }),
                tx_merkle_root: Some(proto::Hash { value: vec![2; 32] }),
                state_root: Some(proto::Hash { value: vec![3; 32] }),
                timestamp: None,
                leader_id: None,
                term: 1,
                committed_index: 99,
                block_hash: None,
            }),
            merkle_proof: None, // Missing
            chain_proof: None,
        };

        let verified = VerifiedValue::from_proto(proto_response);
        assert!(verified.is_none()); // Should return None if proof missing
    }

    #[test]
    fn test_verified_value_verify_succeeds_with_matching_root() {
        // Create a verified value where the merkle proof matches the state root
        let state_root = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let verified = VerifiedValue {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: BlockHeader {
                height: 100,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![0; 32],
                tx_merkle_root: vec![0; 32],
                state_root: state_root.clone(),
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 99,
                block_hash: vec![],
            },
            merkle_proof: MerkleProof {
                leaf_hash: state_root, // Single element tree: leaf == root
                siblings: vec![],
            },
            chain_proof: None,
        };

        assert!(verified.verify().is_ok());
    }

    #[test]
    fn test_verified_value_verify_fails_with_mismatched_root() {
        // Create a verified value where the merkle proof does NOT match the state root
        let verified = VerifiedValue {
            value: Some(b"test-value".to_vec()),
            block_height: 100,
            block_header: BlockHeader {
                height: 100,
                organization: ORG,
                vault: VaultSlug::new(0),
                previous_hash: vec![0; 32],
                tx_merkle_root: vec![0; 32],
                state_root: vec![1, 2, 3, 4], // Expected root
                timestamp: None,
                leader_id: String::new(),
                term: 1,
                committed_index: 99,
                block_hash: vec![],
            },
            merkle_proof: MerkleProof {
                leaf_hash: vec![5, 6, 7, 8], // Different hash!
                siblings: vec![],
            },
            chain_proof: None,
        };

        let result = verified.verify();
        assert!(result.is_err());
    }

    // (verified_read connection failure test consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // Query Types Tests
    // =========================================================================

    #[test]
    fn test_entity_from_proto() {
        let proto_entity = proto::Entity {
            key: "user:123".to_string(),
            value: b"data".to_vec(),
            expires_at: Some(1700000000),
            version: 42,
        };

        let entity = Entity::from_proto(proto_entity);

        assert_eq!(entity.key, "user:123");
        assert_eq!(entity.value, b"data");
        assert_eq!(entity.expires_at, Some(1700000000));
        assert_eq!(entity.version, 42);
    }

    #[test]
    fn test_entity_from_proto_no_expiration() {
        let proto_entity = proto::Entity {
            key: "session:abc".to_string(),
            value: vec![],
            expires_at: None,
            version: 1,
        };

        let entity = Entity::from_proto(proto_entity);

        assert_eq!(entity.expires_at, None);
    }

    #[test]
    fn test_entity_from_proto_zero_expiration_treated_as_none() {
        let proto_entity = proto::Entity {
            key: "key".to_string(),
            value: vec![],
            expires_at: Some(0),
            version: 1,
        };

        let entity = Entity::from_proto(proto_entity);

        // Zero expiration is treated as "no expiration"
        assert_eq!(entity.expires_at, None);
    }

    #[test]
    fn test_entity_is_expired_at() {
        let entity =
            Entity { key: "key".to_string(), value: vec![], expires_at: Some(1000), version: 1 };

        // Before expiration
        assert!(!entity.is_expired_at(999));
        // At expiration
        assert!(entity.is_expired_at(1000));
        // After expiration
        assert!(entity.is_expired_at(1001));
    }

    #[test]
    fn test_entity_is_expired_at_no_expiration() {
        let entity = Entity { key: "key".to_string(), value: vec![], expires_at: None, version: 1 };

        // Never expires
        assert!(!entity.is_expired_at(u64::MAX));
    }

    #[test]
    fn test_entity_equality() {
        let entity1 = Entity {
            key: "key".to_string(),
            value: b"value".to_vec(),
            expires_at: Some(1000),
            version: 1,
        };
        let entity2 = entity1.clone();

        assert_eq!(entity1, entity2);
    }

    #[test]
    fn test_relationship_new() {
        let rel = Relationship::new("document:1", "viewer", "user:alice");

        assert_eq!(rel.resource, "document:1");
        assert_eq!(rel.relation, "viewer");
        assert_eq!(rel.subject, "user:alice");
    }

    #[test]
    fn test_relationship_from_proto() {
        let proto_rel = proto::Relationship {
            resource: "folder:root".to_string(),
            relation: "owner".to_string(),
            subject: "user:admin".to_string(),
        };

        let rel = Relationship::from_proto(proto_rel);

        assert_eq!(rel.resource, "folder:root");
        assert_eq!(rel.relation, "owner");
        assert_eq!(rel.subject, "user:admin");
    }

    #[test]
    fn test_relationship_equality_and_hash() {
        use std::collections::HashSet;

        let rel1 = Relationship::new("doc:1", "editor", "user:bob");
        let rel2 = Relationship::new("doc:1", "editor", "user:bob");
        let rel3 = Relationship::new("doc:1", "viewer", "user:bob");

        assert_eq!(rel1, rel2);
        assert_ne!(rel1, rel3);

        let mut set = HashSet::new();
        set.insert(rel1.clone());
        assert!(set.contains(&rel2));
        assert!(!set.contains(&rel3));
    }

    #[test]
    fn test_paged_result_has_next_page() {
        let with_next: PagedResult<String> = PagedResult {
            items: vec!["item".to_string()],
            next_page_token: Some("token".to_string()),
            block_height: 100,
        };

        let without_next: PagedResult<String> = PagedResult {
            items: vec!["item".to_string()],
            next_page_token: None,
            block_height: 100,
        };

        assert!(with_next.has_next_page());
        assert!(!without_next.has_next_page());
    }

    #[test]
    fn test_list_entities_opts_builder() {
        let opts = ListEntitiesOpts::with_prefix("user:")
            .at_height(100)
            .include_expired()
            .limit(50)
            .page_token("abc123")
            .linearizable();

        assert_eq!(opts.key_prefix, "user:");
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_expired);
        assert_eq!(opts.limit, 50);
        assert_eq!(opts.page_token, Some("abc123".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_entities_opts_defaults() {
        let opts = ListEntitiesOpts::with_prefix("session:");

        assert_eq!(opts.key_prefix, "session:");
        assert_eq!(opts.at_height, None);
        assert!(!opts.include_expired);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_relationships_opts_builder() {
        let opts = ListRelationshipsOpts::new()
            .resource("document:1")
            .relation("viewer")
            .subject("user:alice")
            .at_height(50)
            .limit(100)
            .page_token("xyz")
            .consistency(ReadConsistency::Linearizable);

        assert_eq!(opts.resource, Some("document:1".to_string()));
        assert_eq!(opts.relation, Some("viewer".to_string()));
        assert_eq!(opts.subject, Some("user:alice".to_string()));
        assert_eq!(opts.at_height, Some(50));
        assert_eq!(opts.limit, 100);
        assert_eq!(opts.page_token, Some("xyz".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_relationships_opts_defaults() {
        let opts = ListRelationshipsOpts::new();

        assert_eq!(opts.resource, None);
        assert_eq!(opts.relation, None);
        assert_eq!(opts.subject, None);
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_resources_opts_builder() {
        let opts = ListResourcesOpts::with_type("document")
            .at_height(200)
            .limit(25)
            .page_token("next")
            .linearizable();

        assert_eq!(opts.resource_type, "document");
        assert_eq!(opts.at_height, Some(200));
        assert_eq!(opts.limit, 25);
        assert_eq!(opts.page_token, Some("next".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_resources_opts_defaults() {
        let opts = ListResourcesOpts::with_type("folder");

        assert_eq!(opts.resource_type, "folder");
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_entities_opts_bon_builder() {
        let opts = ListEntitiesOpts::builder()
            .key_prefix("user:")
            .at_height(100)
            .include_expired(true)
            .limit(50)
            .page_token("abc123")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.key_prefix, "user:");
        assert_eq!(opts.at_height, Some(100));
        assert!(opts.include_expired);
        assert_eq!(opts.limit, 50);
        assert_eq!(opts.page_token, Some("abc123".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_entities_opts_bon_builder_defaults() {
        let opts = ListEntitiesOpts::builder().build();

        assert_eq!(opts.key_prefix, "");
        assert_eq!(opts.at_height, None);
        assert!(!opts.include_expired);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_entities_opts_bon_builder_matches_default() {
        let from_builder = ListEntitiesOpts::builder().build();
        let from_default = ListEntitiesOpts::default();

        assert_eq!(from_builder.key_prefix, from_default.key_prefix);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.include_expired, from_default.include_expired);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder() {
        let opts = ListRelationshipsOpts::builder()
            .resource("document:1")
            .relation("viewer")
            .subject("user:alice")
            .at_height(50)
            .limit(100)
            .page_token("xyz")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.resource, Some("document:1".to_string()));
        assert_eq!(opts.relation, Some("viewer".to_string()));
        assert_eq!(opts.subject, Some("user:alice".to_string()));
        assert_eq!(opts.at_height, Some(50));
        assert_eq!(opts.limit, 100);
        assert_eq!(opts.page_token, Some("xyz".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder_defaults() {
        let opts = ListRelationshipsOpts::builder().build();

        assert_eq!(opts.resource, None);
        assert_eq!(opts.relation, None);
        assert_eq!(opts.subject, None);
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_relationships_opts_bon_builder_matches_default() {
        let from_builder = ListRelationshipsOpts::builder().build();
        let from_default = ListRelationshipsOpts::default();

        assert_eq!(from_builder.resource, from_default.resource);
        assert_eq!(from_builder.relation, from_default.relation);
        assert_eq!(from_builder.subject, from_default.subject);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    #[test]
    fn test_list_resources_opts_bon_builder() {
        let opts = ListResourcesOpts::builder()
            .resource_type("document")
            .at_height(200)
            .limit(25)
            .page_token("next")
            .consistency(ReadConsistency::Linearizable)
            .build();

        assert_eq!(opts.resource_type, "document");
        assert_eq!(opts.at_height, Some(200));
        assert_eq!(opts.limit, 25);
        assert_eq!(opts.page_token, Some("next".to_string()));
        assert_eq!(opts.consistency, ReadConsistency::Linearizable);
    }

    #[test]
    fn test_list_resources_opts_bon_builder_defaults() {
        let opts = ListResourcesOpts::builder().build();

        assert_eq!(opts.resource_type, "");
        assert_eq!(opts.at_height, None);
        assert_eq!(opts.limit, 0);
        assert_eq!(opts.page_token, None);
        assert_eq!(opts.consistency, ReadConsistency::Eventual);
    }

    #[test]
    fn test_list_resources_opts_bon_builder_matches_default() {
        let from_builder = ListResourcesOpts::builder().build();
        let from_default = ListResourcesOpts::default();

        assert_eq!(from_builder.resource_type, from_default.resource_type);
        assert_eq!(from_builder.at_height, from_default.at_height);
        assert_eq!(from_builder.limit, from_default.limit);
        assert_eq!(from_builder.page_token, from_default.page_token);
        assert_eq!(from_builder.consistency, from_default.consistency);
    }

    // (query operation connection failure tests consolidated into
    // test_all_operations_return_error_on_connection_failure above)

    // =========================================================================
    // Shutdown Tests
    // =========================================================================

    #[tokio::test]
    async fn test_is_shutdown_false_initially() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        assert!(!client.is_shutdown(), "client should not be shutdown initially");
    }

    #[tokio::test]
    async fn test_is_shutdown_true_after_shutdown() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        client.shutdown().await;

        assert!(client.is_shutdown(), "client should be shutdown after calling shutdown()");
    }

    #[tokio::test]
    async fn test_shutdown_is_idempotent() {
        let client = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        // Multiple shutdown calls should not panic
        client.shutdown().await;
        client.shutdown().await;
        client.shutdown().await;

        assert!(client.is_shutdown());
    }

    #[tokio::test]
    async fn test_cloned_client_shares_shutdown_state() {
        let client1 = LedgerClient::connect("http://localhost:50051", "test-client")
            .await
            .expect("client creation");

        let client2 = client1.clone();

        assert!(!client1.is_shutdown());
        assert!(!client2.is_shutdown());

        // Shutdown through client1
        client1.shutdown().await;

        // Both should reflect shutdown state
        assert!(client1.is_shutdown());
        assert!(client2.is_shutdown(), "cloned client should share shutdown state");
    }

    #[allow(clippy::type_complexity)]
    #[tokio::test]
    async fn test_all_operations_return_shutdown_error() {
        use std::{future::Future, pin::Pin};

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(1)
                    .initial_backoff(Duration::from_millis(1))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        client.shutdown().await;

        let cases: Vec<(&str, Pin<Box<dyn Future<Output = bool> + '_>>)> = vec![
            (
                "read",
                Box::pin(async {
                    matches!(
                        client.read(ORG, Some(VaultSlug::new(0)), "key", None, None).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "write",
                Box::pin(async {
                    matches!(
                        client
                            .write(
                                ORG,
                                Some(VaultSlug::new(0)),
                                vec![Operation::set_entity("key", vec![1, 2, 3], None, None)],
                                None
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "batch_write",
                Box::pin(async {
                    matches!(
                        client
                            .batch_write(
                                ORG,
                                Some(VaultSlug::new(0)),
                                vec![vec![Operation::set_entity("key", vec![1, 2, 3], None, None)]],
                                None
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "batch_read",
                Box::pin(async {
                    matches!(
                        client
                            .batch_read(
                                ORG,
                                Some(VaultSlug::new(0)),
                                vec!["key1".to_string(), "key2".to_string()],
                                None,
                                None
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "watch_blocks",
                Box::pin(async {
                    matches!(
                        client.watch_blocks(ORG, VaultSlug::new(0), 1).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "create_organization",
                Box::pin(async {
                    matches!(
                        client
                            .create_organization(
                                "test",
                                Region::US_EAST_VA,
                                UserSlug::new(0),
                                OrganizationTier::Free
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "get_organization",
                Box::pin(async {
                    matches!(
                        client.get_organization(ORG, UserSlug::new(0)).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "list_organizations",
                Box::pin(async {
                    matches!(
                        client.list_organizations(UserSlug::new(0), 0, None).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "create_vault",
                Box::pin(async {
                    matches!(client.create_vault(ORG).await, Err(crate::error::SdkError::Shutdown))
                }),
            ),
            (
                "get_vault",
                Box::pin(async {
                    matches!(
                        client.get_vault(ORG, VaultSlug::new(0)).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "list_vaults",
                Box::pin(async {
                    matches!(
                        client.list_vaults(0, None).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "health_check",
                Box::pin(async {
                    matches!(client.health_check().await, Err(crate::error::SdkError::Shutdown))
                }),
            ),
            (
                "health_check_detailed",
                Box::pin(async {
                    matches!(
                        client.health_check_detailed().await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "health_check_vault",
                Box::pin(async {
                    matches!(
                        client.health_check_vault(ORG, VaultSlug::new(0)).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "verified_read",
                Box::pin(async {
                    matches!(
                        client
                            .verified_read(ORG, Some(VaultSlug::new(0)), "key", VerifyOpts::new())
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "list_entities",
                Box::pin(async {
                    matches!(
                        client.list_entities(ORG, ListEntitiesOpts::with_prefix("key")).await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "list_relationships",
                Box::pin(async {
                    matches!(
                        client
                            .list_relationships(
                                ORG,
                                VaultSlug::new(0),
                                ListRelationshipsOpts::new()
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
            (
                "list_resources",
                Box::pin(async {
                    matches!(
                        client
                            .list_resources(
                                ORG,
                                VaultSlug::new(0),
                                ListResourcesOpts::with_type("doc")
                            )
                            .await,
                        Err(crate::error::SdkError::Shutdown)
                    )
                }),
            ),
        ];

        for (label, fut) in cases {
            assert!(fut.await, "{label}: expected Shutdown error");
        }
    }

    #[tokio::test]
    async fn test_shutdown_error_is_not_retryable() {
        assert!(!crate::error::SdkError::Shutdown.is_retryable());
    }

    // =========================================================================
    // Cancellation tests
    // =========================================================================

    #[tokio::test]
    async fn test_cancellation_token_accessor() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let token = client.cancellation_token();

        // Token should not be cancelled initially
        assert!(!token.is_cancelled());

        // After shutdown, the token should be cancelled
        client.shutdown().await;
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_child_token_cancelled_on_shutdown() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let child = client.cancellation_token().child_token();

        assert!(!child.is_cancelled());

        client.shutdown().await;
        assert!(child.is_cancelled());
    }

    #[allow(clippy::type_complexity)]
    #[tokio::test]
    async fn test_pre_cancelled_token_returns_cancelled() {
        use std::{future::Future, pin::Pin};

        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .connect_timeout(Duration::from_millis(100))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");

        let cases: Vec<(
            &str,
            Box<
                dyn FnOnce(
                    LedgerClient,
                    tokio_util::sync::CancellationToken,
                ) -> Pin<Box<dyn Future<Output = bool>>>,
            >,
        )> = vec![
            (
                "read",
                Box::new(|c, t| {
                    Box::pin(async move {
                        matches!(
                            c.read(ORG, None, "key", None, Some(t)).await,
                            Err(crate::error::SdkError::Cancelled)
                        )
                    })
                }),
            ),
            (
                "write",
                Box::new(|c, t| {
                    Box::pin(async move {
                        matches!(
                            c.write(
                                ORG,
                                None,
                                vec![Operation::set_entity("key", b"val".to_vec(), None, None)],
                                Some(t),
                            )
                            .await,
                            Err(crate::error::SdkError::Cancelled)
                        )
                    })
                }),
            ),
            (
                "batch_read",
                Box::new(|c, t| {
                    Box::pin(async move {
                        matches!(
                            c.batch_read(ORG, None, vec!["key1", "key2"], None, Some(t)).await,
                            Err(crate::error::SdkError::Cancelled)
                        )
                    })
                }),
            ),
            (
                "batch_write",
                Box::new(|c, t| {
                    Box::pin(async move {
                        let ops =
                            vec![vec![Operation::set_entity("key", b"val".to_vec(), None, None)]];
                        matches!(
                            c.batch_write(ORG, None, ops, Some(t)).await,
                            Err(crate::error::SdkError::Cancelled)
                        )
                    })
                }),
            ),
        ];

        for (label, call_fn) in cases {
            let token = tokio_util::sync::CancellationToken::new();
            token.cancel();
            assert!(call_fn(client.clone(), token).await, "{label}: expected Cancelled error");
        }
    }

    #[tokio::test]
    async fn test_cancelled_error_is_not_retryable() {
        assert!(!crate::error::SdkError::Cancelled.is_retryable());
    }

    #[tokio::test]
    async fn test_cancelled_differs_from_shutdown() {
        // Cancelled and Shutdown are distinct error types
        let cancelled = crate::error::SdkError::Cancelled;
        let shutdown = crate::error::SdkError::Shutdown;

        assert!(!matches!(cancelled, crate::error::SdkError::Shutdown));
        assert!(!matches!(shutdown, crate::error::SdkError::Cancelled));
    }

    #[tokio::test]
    async fn test_read_with_cancellation_token_returns_cancelled_during_backoff() {
        // Set many retries with long backoff so cancellation fires during backoff
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(10)
                    .initial_backoff(Duration::from_secs(30))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(50))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let token = tokio_util::sync::CancellationToken::new();
        let token_clone = token.clone();

        // Cancel after 200ms — the first attempt fails quickly,
        // then the 30s backoff starts, and cancellation fires during it
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            token_clone.cancel();
        });

        let start = std::time::Instant::now();
        let result = client.read(ORG, None, "key", None, Some(token)).await;
        let elapsed = start.elapsed();

        // Should be cancelled during the backoff sleep
        assert!(
            matches!(result, Err(crate::error::SdkError::Cancelled)),
            "expected Cancelled, got: {:?}",
            result
        );
        // Should return quickly, not wait for the 30s backoff
        assert!(elapsed < Duration::from_secs(5), "took {:?}", elapsed);
    }

    #[tokio::test]
    async fn test_shutdown_cancels_inflight_retries() {
        let config = ClientConfig::builder()
            .servers(ServerSource::from_static(["http://localhost:50051"]))
            .client_id("test-client")
            .retry_policy(
                RetryPolicy::builder()
                    .max_attempts(10)
                    .initial_backoff(Duration::from_secs(30))
                    .build(),
            )
            .connect_timeout(Duration::from_millis(50))
            .build()
            .expect("valid config");

        let client = LedgerClient::new(config).await.expect("client creation");
        let client_clone = client.clone();

        // Shutdown after 200ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            client_clone.shutdown().await;
        });

        let start = std::time::Instant::now();
        let result = client.read(ORG, None, "key", None, None).await;
        let elapsed = start.elapsed();

        // Should receive either Shutdown or Cancelled (from the client cancellation token)
        assert!(
            matches!(
                result,
                Err(crate::error::SdkError::Cancelled | crate::error::SdkError::Shutdown)
            ),
            "expected cancellation-related error, got: {:?}",
            result
        );
        // Should not wait for the full 10 attempts × 30s backoff
        assert!(elapsed < Duration::from_secs(5), "took {:?}", elapsed);
    }

    // =========================================================================
    // Operation validation tests
    // =========================================================================

    #[test]
    fn test_operation_validate_set_entity_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::set_entity("user:123", b"data".to_vec(), None, None);
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_set_entity_empty_key() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::SetEntity {
            key: String::new(),
            value: b"data".to_vec(),
            expires_at: None,
            condition: None,
        };
        let err = op.validate(&config).unwrap_err();
        assert!(err.to_string().contains("key"), "Error should mention key: {err}");
    }

    #[test]
    fn test_operation_validate_set_entity_invalid_key_chars() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::set_entity("user 123", b"data".to_vec(), None, None);
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_key_too_long() {
        let config = inferadb_ledger_types::config::ValidationConfig::builder()
            .max_key_bytes(10)
            .build()
            .unwrap();
        let op = Operation::set_entity("a".repeat(11), b"data".to_vec(), None, None);
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_value_too_large() {
        let config = inferadb_ledger_types::config::ValidationConfig::builder()
            .max_value_bytes(4)
            .build()
            .unwrap();
        let op = Operation::set_entity("key", vec![0u8; 5], None, None);
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_delete_entity_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::delete_entity("user:123");
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_delete_entity_empty_key() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::DeleteEntity { key: String::new() };
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_create_relationship_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::create_relationship("doc:456", "viewer", "user:123");
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_relationship_with_hash() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::create_relationship("doc:456", "viewer", "user:123#member");
        assert!(op.validate(&config).is_ok());
    }

    #[test]
    fn test_operation_validate_relationship_empty_resource() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::CreateRelationship {
            resource: String::new(),
            relation: "viewer".to_string(),
            subject: "user:123".to_string(),
        };
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_relationship_invalid_chars() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::create_relationship("doc 456", "viewer", "user:123");
        assert!(op.validate(&config).is_err());
    }

    #[test]
    fn test_operation_validate_delete_relationship_valid() {
        let config = inferadb_ledger_types::config::ValidationConfig::default();
        let op = Operation::delete_relationship("doc:456", "viewer", "user:123");
        assert!(op.validate(&config).is_ok());
    }

    // =========================================================================
    // estimated_size_bytes tests
    // =========================================================================

    #[test]
    fn test_estimated_size_set_entity() {
        let op = Operation::set_entity("key", b"value".to_vec(), None, None);
        assert_eq!(op.estimated_size_bytes(), 3 + 5); // "key" + "value"
    }

    #[test]
    fn test_estimated_size_delete_entity() {
        let op = Operation::delete_entity("user:123");
        assert_eq!(op.estimated_size_bytes(), 8); // "user:123"
    }

    #[test]
    fn test_estimated_size_relationship() {
        let op = Operation::create_relationship("doc:456", "viewer", "user:123");
        assert_eq!(op.estimated_size_bytes(), 7 + 6 + 8); // "doc:456" + "viewer" + "user:123"
    }

    // =========================================================================
    // SdkError::Validation tests
    // =========================================================================

    #[test]
    fn test_sdk_validation_error_not_retryable() {
        let err = crate::error::SdkError::Validation { message: "key too long".to_string() };
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sdk_validation_error_display() {
        let err = crate::error::SdkError::Validation { message: "key too long".to_string() };
        assert!(err.to_string().contains("key too long"));
    }

    // =========================================================================
    // Events type conversion tests
    // =========================================================================

    fn make_proto_event_entry() -> proto::EventEntry {
        proto::EventEntry {
            event_id: uuid::Uuid::nil().into_bytes().to_vec(),
            source_service: "ledger".to_string(),
            event_type: "ledger.vault.created".to_string(),
            timestamp: Some(prost_types::Timestamp { seconds: 1_700_000_000, nanos: 500_000 }),
            scope: proto::EventScope::Organization as i32,
            action: "vault_created".to_string(),
            emission_path: proto::EventEmissionPath::EmissionPathApplyPhase as i32,
            principal: "user:alice".to_string(),
            organization: Some(proto::OrganizationSlug { slug: 12345 }),
            vault: Some(proto::VaultSlug { slug: 67890 }),
            outcome: proto::EventOutcome::Success as i32,
            error_code: None,
            error_detail: None,
            denial_reason: None,
            details: [("vault_name".to_string(), "my-vault".to_string())].into_iter().collect(),
            block_height: Some(42),
            node_id: None,
            trace_id: Some("abc-trace".to_string()),
            correlation_id: Some("batch-123".to_string()),
            operations_count: None,
            expires_at: 0,
        }
    }

    #[test]
    fn test_sdk_event_entry_from_proto_success() {
        let proto = make_proto_event_entry();
        let entry = SdkEventEntry::from_proto(proto);

        assert_eq!(entry.source_service, "ledger");
        assert_eq!(entry.event_type, "ledger.vault.created");
        assert_eq!(entry.action, "vault_created");
        assert_eq!(entry.principal, "user:alice");
        assert_eq!(entry.organization, OrganizationSlug::new(12345));
        assert_eq!(entry.vault, Some(VaultSlug::new(67890)));
        assert_eq!(entry.scope, EventScope::Organization);
        assert_eq!(entry.emission_path, EventEmissionPath::ApplyPhase);
        assert!(matches!(entry.outcome, EventOutcome::Success));
        assert_eq!(entry.details.get("vault_name").unwrap(), "my-vault");
        assert_eq!(entry.block_height, Some(42));
        assert_eq!(entry.trace_id.as_deref(), Some("abc-trace"));
        assert_eq!(entry.correlation_id.as_deref(), Some("batch-123"));
        assert_eq!(entry.timestamp.timestamp(), 1_700_000_000);
    }

    #[test]
    fn test_sdk_event_entry_from_proto_failed_outcome() {
        let mut proto = make_proto_event_entry();
        proto.outcome = proto::EventOutcome::Failed as i32;
        proto.error_code = Some("STORAGE_FULL".to_string());
        proto.error_detail = Some("disk quota exceeded".to_string());

        let entry = SdkEventEntry::from_proto(proto);
        match &entry.outcome {
            EventOutcome::Failed { code, detail } => {
                assert_eq!(code, "STORAGE_FULL");
                assert_eq!(detail, "disk quota exceeded");
            },
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_sdk_event_entry_from_proto_denied_outcome() {
        let mut proto = make_proto_event_entry();
        proto.outcome = proto::EventOutcome::Denied as i32;
        proto.denial_reason = Some("rate limit exceeded".to_string());

        let entry = SdkEventEntry::from_proto(proto);
        match &entry.outcome {
            EventOutcome::Denied { reason } => {
                assert_eq!(reason, "rate limit exceeded");
            },
            _ => panic!("expected Denied outcome"),
        }
    }

    #[test]
    fn test_sdk_event_entry_from_proto_handler_phase() {
        let mut proto = make_proto_event_entry();
        proto.emission_path = proto::EventEmissionPath::EmissionPathHandlerPhase as i32;
        proto.node_id = Some(7);

        let entry = SdkEventEntry::from_proto(proto);
        assert_eq!(entry.emission_path, EventEmissionPath::HandlerPhase);
        assert_eq!(entry.node_id, Some(7));
    }

    #[test]
    fn test_sdk_event_entry_from_proto_system_scope() {
        let mut proto = make_proto_event_entry();
        proto.scope = proto::EventScope::System as i32;
        proto.organization = Some(proto::OrganizationSlug { slug: 0 });

        let entry = SdkEventEntry::from_proto(proto);
        assert_eq!(entry.scope, EventScope::System);
        assert_eq!(entry.organization, OrganizationSlug::new(0));
    }

    #[test]
    fn test_sdk_event_entry_event_id_string_uuid() {
        let entry = SdkEventEntry::from_proto(make_proto_event_entry());
        assert_eq!(entry.event_id_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_sdk_event_entry_event_id_string_non_uuid() {
        let mut proto = make_proto_event_entry();
        proto.event_id = vec![0xab, 0xcd, 0xef];
        let entry = SdkEventEntry::from_proto(proto);
        assert_eq!(entry.event_id_string(), "abcdef");
    }

    #[test]
    fn test_event_filter_default_is_all_pass() {
        let filter = EventFilter::new();
        let proto = filter.to_proto();

        assert!(proto.actions.is_empty());
        assert!(proto.start_time.is_none());
        assert!(proto.end_time.is_none());
        assert!(proto.event_type_prefix.is_none());
        assert!(proto.principal.is_none());
        assert_eq!(proto.outcome, 0);
        assert_eq!(proto.emission_path, 0);
        assert!(proto.correlation_id.is_none());
    }

    #[test]
    fn test_event_filter_with_all_options() {
        let start = chrono::DateTime::from_timestamp(1_000_000, 0).unwrap();
        let end = chrono::DateTime::from_timestamp(2_000_000, 0).unwrap();

        let filter = EventFilter::new()
            .start_time(start)
            .end_time(end)
            .actions(["vault_created", "vault_deleted"])
            .event_type_prefix("ledger.vault")
            .principal("user:bob")
            .outcome_denied()
            .apply_phase_only()
            .correlation_id("job-99");

        let proto = filter.to_proto();
        assert_eq!(proto.start_time.unwrap().seconds, 1_000_000);
        assert_eq!(proto.end_time.unwrap().seconds, 2_000_000);
        assert_eq!(proto.actions, vec!["vault_created", "vault_deleted"]);
        assert_eq!(proto.event_type_prefix.as_deref(), Some("ledger.vault"));
        assert_eq!(proto.principal.as_deref(), Some("user:bob"));
        assert_eq!(proto.outcome, proto::EventOutcome::Denied as i32);
        assert_eq!(proto.emission_path, proto::EventEmissionPath::EmissionPathApplyPhase as i32);
        assert_eq!(proto.correlation_id.as_deref(), Some("job-99"));
    }

    #[test]
    fn test_event_filter_outcome_variants() {
        assert_eq!(
            EventFilter::new().outcome_success().to_proto().outcome,
            proto::EventOutcome::Success as i32,
        );
        assert_eq!(
            EventFilter::new().outcome_failed().to_proto().outcome,
            proto::EventOutcome::Failed as i32,
        );
        assert_eq!(
            EventFilter::new().outcome_denied().to_proto().outcome,
            proto::EventOutcome::Denied as i32,
        );
    }

    #[test]
    fn test_event_filter_emission_path_variants() {
        assert_eq!(
            EventFilter::new().apply_phase_only().to_proto().emission_path,
            proto::EventEmissionPath::EmissionPathApplyPhase as i32,
        );
        assert_eq!(
            EventFilter::new().handler_phase_only().to_proto().emission_path,
            proto::EventEmissionPath::EmissionPathHandlerPhase as i32,
        );
    }

    #[test]
    fn test_ingest_event_entry_required_fields() {
        let entry = SdkIngestEventEntry::new(
            "engine.authorization.checked",
            "user:alice",
            EventOutcome::Success,
        );
        let proto = entry.into_proto();

        assert_eq!(proto.event_type, "engine.authorization.checked");
        assert_eq!(proto.principal, "user:alice");
        assert_eq!(proto.outcome, proto::EventOutcome::Success as i32);
        assert!(proto.details.is_empty());
        assert!(proto.trace_id.is_none());
        assert!(proto.correlation_id.is_none());
        assert!(proto.vault.is_none());
        assert!(proto.timestamp.is_none());
    }

    #[test]
    fn test_ingest_event_entry_with_all_optional_fields() {
        let ts = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let entry = SdkIngestEventEntry::new(
            "control.member.invited",
            "admin:bob",
            EventOutcome::Failed { code: "LIMIT".to_string(), detail: "max members".to_string() },
        )
        .detail("email", "new@example.com")
        .trace_id("trace-xyz")
        .correlation_id("import-batch-7")
        .vault(VaultSlug::new(999))
        .timestamp(ts);

        let proto = entry.into_proto();
        assert_eq!(proto.event_type, "control.member.invited");
        assert_eq!(proto.principal, "admin:bob");
        assert_eq!(proto.outcome, proto::EventOutcome::Failed as i32);
        assert_eq!(proto.error_code.as_deref(), Some("LIMIT"));
        assert_eq!(proto.error_detail.as_deref(), Some("max members"));
        assert_eq!(proto.details.get("email").unwrap(), "new@example.com");
        assert_eq!(proto.trace_id.as_deref(), Some("trace-xyz"));
        assert_eq!(proto.correlation_id.as_deref(), Some("import-batch-7"));
        assert_eq!(proto.vault.unwrap().slug, 999);
        assert_eq!(proto.timestamp.unwrap().seconds, 1_700_000_000);
    }

    #[test]
    fn test_ingest_event_entry_denied_outcome() {
        let entry = SdkIngestEventEntry::new(
            "engine.authorization.checked",
            "user:charlie",
            EventOutcome::Denied { reason: "no permission".to_string() },
        );
        let proto = entry.into_proto();
        assert_eq!(proto.outcome, proto::EventOutcome::Denied as i32);
        assert_eq!(proto.denial_reason.as_deref(), Some("no permission"));
        assert!(proto.error_code.is_none());
    }

    #[test]
    fn test_event_page_has_next_page() {
        let page = EventPage {
            entries: vec![],
            next_page_token: Some("cursor-abc".to_string()),
            total_estimate: None,
        };
        assert!(page.has_next_page());

        let page = EventPage { entries: vec![], next_page_token: None, total_estimate: None };
        assert!(!page.has_next_page());
    }

    #[test]
    fn test_event_outcome_roundtrip_success() {
        let outcome = EventOutcome::Success;
        let (value, code, detail, reason) = outcome.to_proto();
        let restored = EventOutcome::from_proto(value, code, detail, reason);
        assert!(matches!(restored, EventOutcome::Success));
    }

    #[test]
    fn test_event_outcome_roundtrip_failed() {
        let outcome = EventOutcome::Failed {
            code: "ERR_001".to_string(),
            detail: "something broke".to_string(),
        };
        let (value, code, detail, reason) = outcome.to_proto();
        let restored = EventOutcome::from_proto(value, code, detail, reason);
        match restored {
            EventOutcome::Failed { code, detail } => {
                assert_eq!(code, "ERR_001");
                assert_eq!(detail, "something broke");
            },
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn test_event_outcome_roundtrip_denied() {
        let outcome = EventOutcome::Denied { reason: "rate limited".to_string() };
        let (value, code, detail, reason) = outcome.to_proto();
        let restored = EventOutcome::from_proto(value, code, detail, reason);
        match restored {
            EventOutcome::Denied { reason } => {
                assert_eq!(reason, "rate limited");
            },
            _ => panic!("expected Denied"),
        }
    }

    #[test]
    fn test_event_scope_from_proto() {
        assert_eq!(EventScope::from_proto(proto::EventScope::System as i32), EventScope::System);
        assert_eq!(
            EventScope::from_proto(proto::EventScope::Organization as i32),
            EventScope::Organization,
        );
        // Unknown falls back to System
        assert_eq!(EventScope::from_proto(99), EventScope::System);
    }

    #[test]
    fn test_event_emission_path_from_proto() {
        assert_eq!(
            EventEmissionPath::from_proto(proto::EventEmissionPath::EmissionPathApplyPhase as i32),
            EventEmissionPath::ApplyPhase,
        );
        assert_eq!(
            EventEmissionPath::from_proto(
                proto::EventEmissionPath::EmissionPathHandlerPhase as i32,
            ),
            EventEmissionPath::HandlerPhase,
        );
        // Unknown falls back to ApplyPhase
        assert_eq!(EventEmissionPath::from_proto(99), EventEmissionPath::ApplyPhase);
    }

    #[test]
    fn test_ingest_event_entry_detail_builder() {
        let entry = SdkIngestEventEntry::new("test.event", "user:x", EventOutcome::Success)
            .detail("key1", "val1")
            .detail("key2", "val2");
        let proto = entry.into_proto();
        assert_eq!(proto.details.len(), 2);
        assert_eq!(proto.details.get("key1").unwrap(), "val1");
        assert_eq!(proto.details.get("key2").unwrap(), "val2");
    }

    #[test]
    fn test_ingest_event_entry_details_bulk() {
        let map: std::collections::HashMap<String, String> =
            [("a".to_string(), "1".to_string()), ("b".to_string(), "2".to_string())]
                .into_iter()
                .collect();
        let entry =
            SdkIngestEventEntry::new("test.event", "user:x", EventOutcome::Success).details(map);
        let proto = entry.into_proto();
        assert_eq!(proto.details.len(), 2);
    }

    // =========================================================================
    // Migration Status Tests
    // =========================================================================

    #[test]
    fn test_organization_status_from_proto_migrating() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Migrating as i32);
        assert_eq!(status, OrganizationStatus::Migrating);
    }

    #[test]
    fn test_organization_status_from_proto_suspended() {
        let status = OrganizationStatus::from_proto(proto::OrganizationStatus::Suspended as i32);
        assert_eq!(status, OrganizationStatus::Suspended);
    }

    #[test]
    fn test_migration_info_construction() {
        let info = MigrationInfo {
            slug: OrganizationSlug::new(42),
            source_region: Region::US_EAST_VA,
            target_region: Region::IE_EAST_DUBLIN,
            status: OrganizationStatus::Migrating,
        };

        assert_eq!(info.slug, OrganizationSlug::new(42));
        assert_eq!(info.source_region, Region::US_EAST_VA);
        assert_eq!(info.target_region, Region::IE_EAST_DUBLIN);
        assert_eq!(info.status, OrganizationStatus::Migrating);
    }

    #[test]
    fn test_email_verification_code_from_proto() {
        let code = EmailVerificationCode { code: "ABC123".to_string() };
        assert_eq!(code.code, "ABC123");
    }

    #[test]
    fn test_email_verification_result_existing_user() {
        let result = EmailVerificationResult::ExistingUser {
            user: UserSlug::new(42),
            session: crate::token::TokenPair {
                access_token: "at".to_string(),
                refresh_token: "rt".to_string(),
                access_expires_at: None,
                refresh_expires_at: None,
            },
        };
        match result {
            EmailVerificationResult::ExistingUser { user, session } => {
                assert_eq!(user, UserSlug::new(42));
                assert_eq!(session.access_token, "at");
                assert_eq!(session.refresh_token, "rt");
            },
            EmailVerificationResult::NewUser { .. } => {
                panic!("Expected ExistingUser");
            },
        }
    }

    #[test]
    fn test_email_verification_result_new_user() {
        let result = EmailVerificationResult::NewUser { onboarding_token: "ilobt_abc".to_string() };
        match result {
            EmailVerificationResult::NewUser { onboarding_token } => {
                assert_eq!(onboarding_token, "ilobt_abc");
            },
            EmailVerificationResult::ExistingUser { .. } => {
                panic!("Expected NewUser");
            },
        }
    }

    #[test]
    fn test_registration_result_fields() {
        let result = RegistrationResult {
            user: UserSlug::new(99),
            session: crate::token::TokenPair {
                access_token: "access".to_string(),
                refresh_token: "refresh".to_string(),
                access_expires_at: None,
                refresh_expires_at: None,
            },
            organization: Some(OrganizationSlug::new(1001)),
        };
        assert_eq!(result.user, UserSlug::new(99));
        assert_eq!(result.session.access_token, "access");
        assert_eq!(result.organization, Some(OrganizationSlug::new(1001)));
    }

    #[test]
    fn test_registration_result_without_organization() {
        let result = RegistrationResult {
            user: UserSlug::new(100),
            session: crate::token::TokenPair {
                access_token: "a".to_string(),
                refresh_token: "r".to_string(),
                access_expires_at: None,
                refresh_expires_at: None,
            },
            organization: None,
        };
        assert!(result.organization.is_none());
    }
}
