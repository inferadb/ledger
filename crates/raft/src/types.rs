//! Core types for OpenRaft integration.
//!
//! This module defines the type configuration for OpenRaft, including:
//! - Node identification
//! - Log entry format
//! - Response types
//! - Snapshot data format

use std::fmt;

use chrono::{DateTime, Utc};
use inferadb_ledger_types::{
    AppId, AppSlug, ClientAssertionId, CredentialData, CredentialType, Hash, InvitationStatus,
    InviteId, InviteSlug, OrganizationId, OrganizationSlug, PasskeyCredential, PrimaryAuthMethod,
    RefreshTokenId, Region, SetCondition, SigningKeyId, TeamId, TeamSlug, TokenSubject, TokenType,
    TokenVersion, Transaction, UserCredentialId, UserEmailId, UserId, UserSlug, VaultId, VaultSlug,
};
// Re-export domain types that originated here but now live in types crate.
pub use inferadb_ledger_types::{BlockRetentionMode, BlockRetentionPolicy, LedgerNodeId};
use serde::{Deserialize, Serialize};

// ============================================================================
// Node Lifecycle Status
// ============================================================================

/// Lifecycle status for a cluster node. Stored in GLOBAL state and read
/// by the DR checker/scheduler to derive desired DR membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NodeStatus {
    /// Normal operation. Participates in all assigned Raft groups.
    Active,
    /// Operator-initiated departure. DRs drain replicas.
    Decommissioning,
    /// System-detected failure (no heartbeat within dead_node_timeout).
    Dead,
    /// Terminal. Node removed from GLOBAL, all DR replicas drained.
    Removed,
}

// ============================================================================
// State Root Commitment
// ============================================================================

/// A leader's computed state root for a vault at a specific height.
///
/// After applying an entry, each node records the state root it computed.
/// The leader attaches its commitments to the *next* `RaftPayload`
/// (piggybacking on entry N+1 for entry N's roots). All nodes verify
/// the commitment against their own archived state root during apply,
/// detecting divergence without extra RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRootCommitment {
    /// Organization owning the vault.
    pub organization: OrganizationId,
    /// Vault whose state root was computed.
    pub vault: VaultId,
    /// Block height at which the state root was computed.
    pub vault_height: u64,
    /// SHA-256 state root the leader computed after applying this block.
    pub state_root: Hash,
}

/// A detected state root divergence between the local node and the leader.
///
/// Sent through a channel from the apply path to the divergence handler,
/// which proposes `UpdateVaultHealth { healthy: false }` via Raft to halt
/// the vault cluster-wide.
#[derive(Debug, Clone)]
pub struct StateRootDivergence {
    /// Organization containing the diverged vault.
    pub organization: OrganizationId,
    /// Vault that diverged.
    pub vault: VaultId,
    /// Block height at which divergence was detected.
    pub vault_height: u64,
    /// State root the local node computed.
    pub local_state_root: Hash,
    /// State root the leader committed.
    pub leader_state_root: Hash,
}

// ============================================================================
// Raft Payload Wrapper
// ============================================================================

/// Wraps a [`LedgerRequest`] with a leader-assigned wall-clock timestamp.
///
/// The leader stamps `proposed_at` at proposal time (`client_write`), and all
/// replicas use this value during apply — guaranteeing byte-identical event
/// timestamps, B+ tree keys, and pagination cursors across the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftPayload {
    /// The application-level request.
    pub request: LedgerRequest,
    /// Leader-assigned wall-clock timestamp at proposal time.
    pub proposed_at: DateTime<Utc>,
    /// Leader's state root commitments from the previous apply batch.
    ///
    /// Piggybacked on entry N+1 for entry N's vault state roots.
    /// Followers verify these against their locally computed roots to
    /// detect state machine divergence.
    #[serde(default)]
    pub state_root_commitments: Vec<StateRootCommitment>,
    /// External slug of the user who initiated this request.
    ///
    /// Propagated from the gRPC `UserSlug caller` field through to
    /// audit records. 0 for system-initiated operations (sagas,
    /// background jobs).
    #[serde(default)]
    pub caller: u64,
}

impl RaftPayload {
    /// Creates a payload for system-initiated operations (no caller).
    ///
    /// Used by background jobs, sagas, and internal operations where
    /// there is no gRPC caller.
    pub fn system(request: LedgerRequest) -> Self {
        Self { request, proposed_at: chrono::Utc::now(), state_root_commitments: vec![], caller: 0 }
    }

    /// Creates a payload with no state root commitments.
    ///
    /// Used by all proposal sites except the leader's write path, which
    /// drains the commitment buffer via [`Self::with_commitments`].
    pub fn new(request: LedgerRequest, caller: u64) -> Self {
        Self { request, proposed_at: chrono::Utc::now(), state_root_commitments: vec![], caller }
    }

    /// Creates a payload carrying piggybacked state root commitments.
    ///
    /// The leader drains its commitment buffer and attaches the results
    /// to the next `RaftPayload` so followers can verify state roots
    /// without extra RPCs.
    pub fn with_commitments(
        request: LedgerRequest,
        state_root_commitments: Vec<StateRootCommitment>,
        caller: u64,
    ) -> Self {
        Self { request, proposed_at: chrono::Utc::now(), state_root_commitments, caller }
    }
}

// openraft type configuration removed — local types in log_storage/types.rs.

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request to the Raft state machine.
///
/// This is the "D" (data) type in OpenRaft's type configuration.
/// Each request targets a specific organization and vault.
///
/// Contains no plaintext PII — see [`SystemRequest`] for the data residency
/// invariant. All variants use numeric IDs, hashes, and enums only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LedgerRequest {
    /// Writes transactions to a vault.
    Write {
        /// Target organization.
        organization: OrganizationId,
        /// Target vault within the organization.
        vault: VaultId,
        /// Transactions to apply atomically.
        transactions: Vec<Transaction>,
        /// Idempotency key (16-byte UUID) for cross-failover deduplication.
        /// Stored in the replicated `ClientSequenceEntry` so new leaders can
        /// detect retries without the moka cache.
        #[serde(default)]
        idempotency_key: [u8; 16],
        /// Hash of the request payload (seahash) for detecting key reuse
        /// with different payloads after failover.
        #[serde(default)]
        request_hash: u64,
    },

    /// Creates a new vault within an organization.
    CreateVault {
        /// Organization to create the vault in.
        organization: OrganizationId,
        /// External slug for API lookups (generated before Raft proposal).
        slug: VaultSlug,
        /// Optional vault name (for display).
        name: Option<String>,
        /// Block retention policy for this vault.
        /// Defaults to Full retention if not specified.
        retention_policy: Option<BlockRetentionPolicy>,
    },

    /// Deletes an organization.
    DeleteOrganization {
        /// Organization ID to delete.
        organization: OrganizationId,
    },

    /// Deletes a vault.
    DeleteVault {
        /// Organization containing the vault.
        organization: OrganizationId,
        /// Vault ID to delete.
        vault: VaultId,
    },

    /// Updates vault metadata (retention policy).
    UpdateVault {
        /// Organization containing the vault.
        organization: OrganizationId,
        /// Vault to update.
        vault: VaultId,
        /// New retention policy (if provided).
        retention_policy: Option<BlockRetentionPolicy>,
    },

    /// Suspends an organization (billing hold or policy violation).
    /// Suspended organizations reject writes but allow reads.
    SuspendOrganization {
        /// Organization to suspend.
        organization: OrganizationId,
        /// Optional reason for suspension (e.g., "Payment overdue", "TOS violation").
        reason: Option<String>,
    },

    /// Resumes a suspended organization.
    ResumeOrganization {
        /// Organization to resume.
        organization: OrganizationId,
    },

    /// Removes a member from an organization.
    RemoveOrganizationMember {
        /// Organization to modify.
        organization: OrganizationId,
        /// User to remove.
        target: UserId,
    },

    /// Updates a member's role within an organization.
    UpdateOrganizationMemberRole {
        /// Organization to modify.
        organization: OrganizationId,
        /// User whose role changes.
        target: UserId,
        /// New role for the member.
        role: inferadb_ledger_state::system::OrganizationMemberRole,
    },

    /// Adds a user as a member of an organization. Idempotent: no-op if already a member.
    AddOrganizationMember {
        /// Organization to add the member to.
        organization: OrganizationId,
        /// User to add.
        user: UserId,
        /// External Snowflake slug for the user.
        user_slug: UserSlug,
        /// Role to assign upon joining.
        role: inferadb_ledger_state::system::OrganizationMemberRole,
    },

    /// Creates a new organization invitation (GLOBAL indexes only).
    ///
    /// Allocates an `InviteId` from the sequence counter, computes
    /// `expires_at = proposed_at + ttl_hours`, and writes three GLOBAL indexes:
    /// - `_idx:invite:slug:{slug}` → `InviteIndexEntry`
    /// - `_idx:invite:token_hash:{hex}` → `InviteIndexEntry`
    /// - `_idx:invite:email_hash:{hmac}:{invite_id}` → `InviteEmailEntry`
    ///
    /// The full invitation record (with PII) is written separately via
    /// `SystemRequest::WriteOrganizationInvite` to the REGIONAL Raft group.
    /// No plaintext email appears in this GLOBAL entry.
    CreateOrganizationInvite {
        /// Organization issuing the invitation.
        organization: OrganizationId,
        /// External Snowflake slug for the invitation.
        slug: InviteSlug,
        /// SHA-256 hash of the raw invitation token.
        token_hash: [u8; 32],
        /// HMAC of the invitee's normalized email (blinding key).
        invitee_email_hmac: String,
        /// Invitation TTL in hours (1–720). `expires_at` is computed by
        /// the apply handler as `proposed_at + ttl_hours`.
        ttl_hours: u32,
    },

    /// Resolves an organization invitation to a terminal state (GLOBAL).
    ///
    /// CAS: apply handler verifies `current_status == Pending` in the email
    /// hash index before applying. Updates `InviteEmailEntry.status` and
    /// removes the `_idx:invite:token_hash` index entry. Returns
    /// `LedgerResponse::Error { code: InvitationAlreadyResolved }` on
    /// CAS failure.
    ///
    /// The `invitee_email_hmac` and `token_hash` fields are included so the
    /// apply handler can construct exact index keys without scanning.
    ResolveOrganizationInvite {
        /// Invitation to resolve.
        invite: InviteId,
        /// Organization that owns the invitation.
        organization: OrganizationId,
        /// Terminal status to transition to (Accepted, Declined, Expired, or Revoked).
        status: InvitationStatus,
        /// HMAC of the invitee's email (for email hash index key construction).
        invitee_email_hmac: String,
        /// SHA-256 hash of the invitation token (for token hash index removal).
        token_hash: [u8; 32],
    },

    /// Removes GLOBAL invitation indexes during retention reaping.
    /// Used by `InviteMaintenanceJob` — deletes slug, email hash, and token hash entries.
    PurgeOrganizationInviteIndexes {
        /// Invitation being purged.
        invite: InviteId,
        /// Invitation slug for slug index cleanup.
        slug: InviteSlug,
        /// HMAC hex for email hash index cleanup.
        invitee_email_hmac: String,
        /// SHA-256 hash of the invitation token for token hash index cleanup.
        token_hash: [u8; 32],
    },

    /// Re-keys the GLOBAL `_idx:invite:email_hash` index during blinding
    /// key rotation. Deletes old HMAC entry, creates new HMAC entry.
    RehashInviteEmailIndex {
        /// Invitation ID (key suffix).
        invite: InviteId,
        /// Old HMAC hex (to delete).
        old_hmac: String,
        /// New HMAC hex (to create).
        new_hmac: String,
        /// Organization ID (preserved in new entry).
        organization: OrganizationId,
        /// Current invitation status (preserved in new entry).
        status: InvitationStatus,
    },

    /// Purges a deleted organization after its retention cooldown.
    ///
    /// Force-deletes all remaining vaults and removes all organization
    /// data including slug index entries. Submitted by the background
    /// `OrganizationPurgeJob` after `region.retention_days()` elapses.
    PurgeOrganization {
        /// Organization to purge.
        organization: OrganizationId,
    },

    /// Starts organization migration to a new region.
    /// Sets status to Migrating, blocking writes until CompleteMigration.
    StartMigration {
        /// Organization to migrate.
        organization: OrganizationId,
        /// Target region for migration.
        target_region_group: Region,
    },

    /// Completes a pending organization migration.
    /// Updates region and returns status to Active.
    CompleteMigration {
        /// Organization being migrated.
        organization: OrganizationId,
    },

    /// Updates vault health status (used during recovery).
    UpdateVaultHealth {
        /// Organization containing the vault.
        organization: OrganizationId,
        /// Vault ID to update.
        vault: VaultId,
        /// New health status: true = Healthy, false = Diverged/Recovering.
        healthy: bool,
        /// If diverged, the expected state root.
        expected_root: Option<Hash>,
        /// If diverged, the computed state root.
        computed_root: Option<Hash>,
        /// If diverged, the height at which divergence was detected.
        diverged_at_height: Option<u64>,
        /// If recovering, the recovery attempt number (1-based).
        recovery_attempt: Option<u8>,
        /// If recovering, the start timestamp (Unix seconds).
        recovery_started_at: Option<i64>,
    },

    /// System operation (user management, node membership, etc.).
    System(SystemRequest),

    /// Batches of requests to apply atomically in a single Raft entry.
    ///
    /// Application-level batching coalesces multiple write requests into a
    /// single Raft proposal to reduce consensus round-trips and improve
    /// throughput.
    ///
    /// Each inner request is processed sequentially, and responses are
    /// returned in the same order via `LedgerResponse::BatchWrite`.
    BatchWrite {
        /// The requests to process.
        requests: Vec<LedgerRequest>,
    },

    /// Creates a new team within an organization (GLOBAL directory entry).
    ///
    /// Allocates the team ID and slug mapping. Does NOT include the team name —
    /// plaintext names are PII and must be written via the regional
    /// [`SystemRequest::WriteTeam`] to avoid leaking into the GLOBAL
    /// Raft log.
    CreateOrganizationTeam {
        /// Organization to create the team in.
        organization: OrganizationId,
        /// External slug for API lookups (generated before Raft proposal).
        slug: TeamSlug,
    },

    /// Deletes a team's GLOBAL directory entry (slug index + in-memory maps).
    ///
    /// Profile deletion and member migration are handled by the REGIONAL
    /// [`SystemRequest::DeleteTeam`] (proposed first by the service handler).
    DeleteOrganizationTeam {
        /// Organization containing the team.
        organization: OrganizationId,
        /// Team to delete.
        team: TeamId,
    },

    /// Creates a new application within an organization (GLOBAL directory entry).
    ///
    /// Allocates the app ID and slug mapping. Does NOT include the app name or
    /// description — plaintext names are PII and must be written via the
    /// regional [`SystemRequest::WriteAppProfile`] to avoid leaking into the
    /// GLOBAL Raft log.
    CreateApp {
        /// Organization to create the app in.
        organization: OrganizationId,
        /// External slug for API lookups (generated before Raft proposal).
        slug: AppSlug,
    },

    /// Deletes an application and all its sub-resources.
    DeleteApp {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App to delete.
        app: AppId,
    },

    /// Enables or disables an application.
    SetAppEnabled {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App to toggle.
        app: AppId,
        /// Whether to enable (`true`) or disable (`false`).
        enabled: bool,
    },

    /// Sets the enabled state of a credential type on an app.
    SetAppCredentialEnabled {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App whose credential to modify.
        app: AppId,
        /// Which credential type to toggle.
        credential_type: inferadb_ledger_state::system::AppCredentialType,
        /// New enabled state.
        enabled: bool,
    },

    /// Rotates the client secret for an app.
    ///
    /// Generates a new secret, stores the bcrypt hash, and returns
    /// the plaintext secret once in the response.
    RotateAppClientSecret {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App whose secret to rotate.
        app: AppId,
        /// Bcrypt hash of the new secret (computed before Raft proposal).
        new_secret_hash: String,
    },

    /// Creates a client assertion entry (Ed25519 keypair).
    ///
    /// Structural entry only (public key, expiry). The user-provided name
    /// is written separately via [`SystemRequest::WriteClientAssertionName`]
    /// to the REGIONAL Raft group (PII isolation).
    CreateAppClientAssertion {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App to add the assertion to.
        app: AppId,
        /// When this entry expires.
        expires_at: DateTime<Utc>,
        /// Raw 32-byte Ed25519 public key.
        public_key_bytes: Vec<u8>,
    },

    /// Deletes a client assertion entry.
    DeleteAppClientAssertion {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App containing the assertion.
        app: AppId,
        /// Assertion entry to delete.
        assertion: ClientAssertionId,
    },

    /// Enables or disables an individual client assertion entry.
    SetAppClientAssertionEnabled {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App containing the assertion.
        app: AppId,
        /// Assertion entry to toggle.
        assertion: ClientAssertionId,
        /// New enabled state.
        enabled: bool,
    },

    /// Adds a vault connection to an app.
    AddAppVault {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App to add the vault connection to.
        app: AppId,
        /// Vault to connect.
        vault: VaultId,
        /// External vault slug (for response construction).
        vault_slug: VaultSlug,
        /// Allowed scopes for this connection.
        allowed_scopes: Vec<String>,
    },

    /// Updates a vault connection's allowed scopes.
    UpdateAppVault {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App containing the vault connection.
        app: AppId,
        /// Vault whose connection to update.
        vault: VaultId,
        /// New allowed scopes.
        allowed_scopes: Vec<String>,
    },

    /// Removes a vault connection from an app.
    RemoveAppVault {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App containing the vault connection.
        app: AppId,
        /// Vault to disconnect.
        vault: VaultId,
    },

    // ── Signing Key Management ──
    /// Creates a new signing key for JWT token signing.
    ///
    /// The service layer generates the Ed25519 keypair, encrypts the private
    /// key with the RMK (envelope encryption), and proposes this entry.
    /// The state machine stores the key entity and indexes.
    CreateSigningKey {
        /// Key scope: Global (user sessions) or Organization (vault tokens).
        scope: inferadb_ledger_state::system::SigningKeyScope,
        /// UUID-format key identifier (generated before proposal).
        kid: String,
        /// 32-byte Ed25519 public key.
        public_key_bytes: Vec<u8>,
        /// `SigningKeyEnvelope` serialized bytes (100 bytes).
        encrypted_private_key: Vec<u8>,
        /// RMK version used to wrap the DEK.
        rmk_version: u32,
    },

    /// Rotates a signing key by creating a new active key and transitioning
    /// the old key to Rotated (grace period) or Revoked (immediate).
    RotateSigningKey {
        /// Kid of the key being rotated.
        old_kid: String,
        /// Kid of the replacement key.
        new_kid: String,
        /// 32-byte Ed25519 public key for the new key.
        new_public_key_bytes: Vec<u8>,
        /// Encrypted private key bytes for the new key.
        new_encrypted_private_key: Vec<u8>,
        /// RMK version used to wrap the new key's DEK.
        rmk_version: u32,
        /// Grace period in seconds. 0 = immediate revocation of old key.
        grace_period_secs: u64,
    },

    /// Immediately revokes a signing key (Active or Rotated → Revoked).
    RevokeSigningKey {
        /// Kid of the key to revoke.
        kid: String,
    },

    /// Transitions a rotated signing key past its grace period to Revoked.
    /// Used by `TokenMaintenanceJob` — state changes must go through Raft.
    TransitionSigningKeyRevoked {
        /// Kid of the rotated key to transition.
        kid: String,
    },

    // ── Refresh Token Management ──
    /// Creates a refresh token record (paired with an access token).
    CreateRefreshToken {
        /// SHA-256 hash of the opaque refresh token string.
        token_hash: [u8; 32],
        /// Token family UUID for theft detection.
        family: [u8; 16],
        /// Whether this is a user session or vault access token.
        token_type: TokenType,
        /// Subject: User or App.
        subject: TokenSubject,
        /// Organization (None for user sessions).
        organization: Option<OrganizationId>,
        /// Vault (set for vault tokens).
        vault: Option<VaultId>,
        /// Which signing key signed the associated access token.
        kid: String,
        /// Refresh TTL in seconds. Apply handler computes `expires_at`
        /// as `proposed_at + ttl_secs`.
        ttl_secs: u64,
    },

    /// Atomically consumes a refresh token and creates a replacement.
    ///
    /// The state machine is the authority for all validation: used, expired,
    /// revoked, family poisoned, version mismatch, app enabled, vault connected.
    UseRefreshToken {
        /// Hash of the token being consumed.
        old_token_hash: [u8; 32],
        /// Hash of the replacement token.
        new_token_hash: [u8; 32],
        /// Current active signing key kid for the new access token.
        new_kid: String,
        /// Refresh TTL in seconds for the new token.
        ttl_secs: u64,
        /// For user session refresh: the `TokenVersion` the caller observed.
        /// State machine rejects if current version differs.
        /// None for vault token refresh.
        expected_version: Option<TokenVersion>,
        /// Maximum family lifetime in seconds. The state machine rejects
        /// the refresh if the family has exceeded this age.
        max_family_lifetime_secs: u64,
    },

    /// Revokes all tokens in a family.
    RevokeTokenFamily {
        /// Token family UUID to revoke.
        family: [u8; 16],
    },

    /// Atomically revokes all user sessions and increments `TokenVersion`.
    RevokeAllUserSessions {
        /// User whose sessions to revoke.
        user: UserId,
    },

    /// Atomically revokes all app sessions and increments the app's `TokenVersion`.
    RevokeAllAppSessions {
        /// Organization owning the app.
        organization: OrganizationId,
        /// App whose sessions to revoke.
        app: AppId,
    },

    /// Deletes expired refresh tokens and garbage-collects poisoned families.
    /// Used by `TokenMaintenanceJob`. Apply handler uses `proposed_at` as cutoff.
    DeleteExpiredRefreshTokens,

    /// Encrypted form of a [`SystemRequest`] for PII crypto-shredding.
    ///
    /// User-scoped REGIONAL requests (profile writes, email operations) are
    /// encrypted with the user's `UserShredKey` before entering the Raft log.
    /// When the user is erased and their `UserShredKey` is destroyed, all
    /// historical log entries become cryptographically unrecoverable.
    ///
    /// The apply handler decrypts using the `UserShredKey` from state. If the
    /// key has been destroyed (user erased), the entry is skipped — the state
    /// machine already reflects the erasure.
    EncryptedUserSystem(crate::entry_crypto::EncryptedUserSystemRequest),

    /// Organization-scoped encrypted form of a [`SystemRequest`].
    ///
    /// Organization-scoped REGIONAL requests (org/team/app profile writes) are
    /// encrypted with the organization's `OrgShredKey` before entering the Raft log.
    /// When the organization is purged and the `OrgShredKey` destroyed, all
    /// historical log entries become cryptographically unrecoverable.
    ///
    /// The apply handler decrypts using the `OrgShredKey` from state. If the
    /// key has been destroyed (org purged), the entry is skipped.
    EncryptedOrgSystem(crate::entry_crypto::EncryptedOrgSystemRequest),

    /// Adds a node as a learner to a data region's Raft group.
    ///
    /// Sent via `RegionalProposal` from the GLOBAL leader to the data
    /// region leader when the GLOBAL leader is not the DR leader. The
    /// `regional_proposal` handler intercepts this variant and calls
    /// `add_learner` + `promote_voter` directly (membership changes bypass
    /// the Raft log). This variant must never reach the apply handler.
    AddRegionLearner {
        /// Node ID to add as a learner (then promote to voter).
        node_id: u64,
        /// Network address of the node (host:port).
        address: String,
    },
}

/// System-level requests that modify the `_system` organization.
///
/// # Data residency invariant
///
/// **No plaintext PII in GLOBAL Raft entries.** Every variant proposed to the
/// GLOBAL Raft group must contain only opaque identifiers (numeric IDs, slugs),
/// cryptographic hashes (email HMACs), enums, and system metadata. Plaintext
/// personal data (names, emails, addresses) must be proposed to the REGIONAL
/// Raft group via `ServiceContext::propose_regional` or written directly to
/// the regional state layer.
///
/// This invariant ensures PII is never replicated across regions via the
/// consensus log. Actor identity for audit purposes is captured in canonical
/// log lines and wide events (local, non-replicated), not in Raft entries.
///
/// Variants that carry PII and are proposed to REGIONAL:
/// - [`CreateUserEmail`](SystemRequest::CreateUserEmail) — encrypted via UserShredKey
/// - [`UpdateUserProfile`](SystemRequest::UpdateUserProfile) — encrypted via UserShredKey
/// - [`WriteOnboardingUserProfile`](SystemRequest::WriteOnboardingUserProfile) — PII sealed with
///   UserShredKey (bootstrap entry, key also in entry for cross-replica provisioning)
/// - [`WriteOrganizationProfile`](SystemRequest::WriteOrganizationProfile) — organization name
/// - [`WriteTeam`](SystemRequest::WriteTeam) — team name
/// - [`WriteAppProfile`](SystemRequest::WriteAppProfile) — app name, description
/// - [`CleanupExpiredOnboarding`](SystemRequest::CleanupExpiredOnboarding) — regional GC
///
/// All other variants are proposed to GLOBAL and contain no PII.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemRequest {
    /// Creates a new user (global control-plane entry only).
    ///
    /// PII fields (name, email) are excluded — the global control plane must
    /// not contain plaintext personal data. User PII is written directly to
    /// the regional store.
    CreateUser {
        /// Pre-allocated user ID from the saga orchestrator.
        ///
        /// The saga allocates this ID via CAS sequence in step 0 and writes it
        /// to the email-hash index. The state machine must use this exact ID
        /// (not allocate a new one) to keep the slug index consistent with
        /// the email-hash index.
        user: UserId,
        /// Whether this user is a global service administrator.
        admin: bool,
        /// External Snowflake identifier.
        slug: UserSlug,
        /// Data residency region for the user's PII.
        region: Region,
    },

    /// Updates an existing user's role or primary email in the GLOBAL Raft log.
    ///
    /// At least one field must be `Some`. No PII — name changes go through
    /// `UpdateUserProfile` proposed to the regional Raft group.
    UpdateUser {
        /// User to update.
        user_id: UserId,
        /// New role (if changing).
        role: Option<inferadb_ledger_types::UserRole>,
        /// New primary email (if changing).
        primary_email: Option<UserEmailId>,
    },

    /// Soft-deletes a user by setting status to `Deleting` and recording `deleted_at`.
    ///
    /// The user's data is retained for the region's retention period before
    /// permanent erasure via `EraseUser`.
    DeleteUser {
        /// User to soft-delete.
        user_id: UserId,
    },

    /// Creates an additional email address for a user.
    CreateUserEmail {
        /// User who owns this email.
        user_id: UserId,
        /// Email address to add.
        email: String,
    },

    /// Deletes a non-primary email address from a user.
    DeleteUserEmail {
        /// User who owns this email.
        user_id: UserId,
        /// Email record to delete.
        email_id: UserEmailId,
    },

    /// Marks a user email as verified.
    VerifyUserEmail {
        /// Email record to verify.
        email_id: UserEmailId,
    },

    /// Sets a node's lifecycle status. Proposed to GLOBAL Raft and applied
    /// deterministically on all nodes. Used for decommissioning and dead-node detection.
    SetNodeStatus {
        /// Node whose status changes.
        node_id: u64,
        /// New lifecycle status.
        status: NodeStatus,
    },

    /// Updates organization-to-region mapping.
    UpdateOrganizationRouting {
        /// Organization to update.
        organization: OrganizationId,
        /// New region assignment.
        region: Region,
    },

    /// Registers an email HMAC hash in the global control plane.
    /// Uses CAS (`MustNotExist`) for uniqueness enforcement.
    RegisterEmailHash {
        /// Hex-encoded HMAC-SHA256 of the normalized email.
        hmac_hex: String,
        /// User ID to associate with this email hash.
        user_id: UserId,
    },

    /// Removes an email HMAC hash from the global control plane.
    RemoveEmailHash {
        /// Hex-encoded HMAC-SHA256 to remove.
        hmac_hex: String,
    },

    /// Sets the active email blinding key version.
    SetBlindingKeyVersion {
        /// New active key version number.
        version: u32,
    },

    /// Updates rehash progress for a region during blinding key rotation.
    UpdateRehashProgress {
        /// Region whose progress is being updated.
        region: Region,
        /// Number of entries rehashed so far.
        entries_rehashed: u64,
    },

    /// Clears rehash progress for a region (rotation complete for that region).
    ClearRehashProgress {
        /// Region whose progress is being cleared.
        region: Region,
    },

    /// Updates a user's directory entry status and optionally their region.
    /// Used during user region migration (mark Migrating, update region, revert).
    UpdateUserDirectoryStatus {
        /// User whose directory entry to update.
        user_id: UserId,
        /// New directory status.
        status: inferadb_ledger_state::system::UserDirectoryStatus,
        /// If `Some`, update the region. If `None`, keep current region.
        region: Option<Region>,
    },

    /// Erases a user's PII via crypto-shredding.
    ///
    /// Forward-only finalization: destroys the per-subject encryption key,
    /// scrubs the directory entry, records an erasure audit trail, and
    /// marks the user for snapshot tombstoning. Each step is idempotent
    /// but irreversible.
    EraseUser {
        /// User whose data to erase.
        user_id: UserId,
        /// Region where the user's PII resides.
        region: Region,
    },

    /// One-time migration of existing users from flat `_system` store to
    /// regional directory structure.
    ///
    /// Each entry contains pre-computed data (email HMAC, subject key) so
    /// the blinding key never enters the Raft log. The state machine creates
    /// directory entries, slug indexes, email hash indexes, subject keys,
    /// and removes old plaintext email indexes atomically.
    MigrateExistingUsers {
        /// Pre-computed migration entries (one per user).
        entries: Vec<inferadb_ledger_state::system::UserMigrationEntry>,
    },

    /// Creates an organization directory entry in the GLOBAL control plane.
    ///
    /// Allocates an `OrganizationId` from the sequence counter, inserts
    /// `OrganizationMeta` into Raft state, writes the `OrganizationRegistry`
    /// to the StateLayer, and registers the slug index.
    CreateOrganization {
        /// External Snowflake slug (generated before Raft proposal).
        slug: OrganizationSlug,
        /// Target data residency region.
        region: Region,
        /// Billing tier.
        tier: inferadb_ledger_state::system::OrganizationTier,
        /// Initial administrator for this organization.
        admin: inferadb_ledger_types::UserId,
    },

    /// Writes the organization profile to the REGIONAL system vault.
    ///
    /// Creates an `OrganizationProfile` keyed as
    /// `org_profile:{organization}` with the provided name.
    ///
    /// The name is AES-256-GCM sealed with `shred_key_bytes` before entering
    /// the Raft log (crypto-shredding). The apply handler stores the OrgShredKey
    /// first, then decrypts the name and writes the profile. On replay after
    /// org purge, the OrgShredKey is absent and the entry is skipped.
    WriteOrganizationProfile {
        /// Organization whose profile to write.
        organization: OrganizationId,
        /// AES-256-GCM sealed organization name.
        sealed_name: Vec<u8>,
        /// Nonce for sealed_name decryption.
        name_nonce: [u8; 12],
        /// Per-organization 256-bit AES key for crypto-shredding.
        /// Stored by the apply handler for future profile writes.
        shred_key_bytes: [u8; 32],
    },

    /// Updates the organization profile name in the REGIONAL Raft log.
    ///
    /// Name is PII and must not appear in the GLOBAL Raft log.
    /// Proposed to the organization's home region via `propose_regional`.
    UpdateOrganizationProfile {
        /// Organization whose profile to update.
        organization: OrganizationId,
        /// New display name.
        name: String,
    },

    /// Updates the organization status in the GLOBAL control plane.
    ///
    /// Syncs both `OrganizationMeta` (in-memory + B+ tree) and
    /// `OrganizationRegistry` (state layer) to the new status.
    UpdateOrganizationStatus {
        /// Organization to update.
        organization: OrganizationId,
        /// New organization status.
        status: inferadb_ledger_state::system::OrganizationStatus,
    },

    /// Updates a user's display name in the REGIONAL Raft log.
    ///
    /// Name is PII and must not appear in the GLOBAL Raft log.
    /// Proposed to the user's home region via `propose_regional`.
    UpdateUserProfile {
        /// User to update.
        user_id: UserId,
        /// New display name.
        name: String,
    },

    // ── Onboarding Requests ──
    /// Stores a verification code for email onboarding.
    ///
    /// Proposed to REGIONAL Raft group — no PII (email excluded from Raft log).
    /// The plaintext email is used by the service handler to send the verification
    /// email *before* the Raft proposal; it is not needed in state.
    /// Rate-limited per email via `rate_limit_count` and `rate_limit_window_start`
    /// in the stored `PendingEmailVerification` record.
    CreateEmailVerification {
        /// HMAC of the email address (deterministic key).
        email_hmac: String,
        /// HMAC-SHA256(blinding_key, "code:" || uppercase(code)).
        code_hash: [u8; 32],
        /// Data residency region.
        region: Region,
        /// When the verification code expires.
        expires_at: DateTime<Utc>,
    },

    /// Verifies a code and consumes the verification record.
    ///
    /// Proposed to REGIONAL Raft group (verification region).
    /// The apply handler validates the code and branches on `existing_user_hmac_hit`:
    /// - `true` + `totp: None`: Returns `ExistingUser` signal (no session created here).
    /// - `true` + `totp: Some(_)`: Atomically consumes code + creates `PendingTotpChallenge`.
    ///   Returns `TotpRequired { nonce }`.
    /// - `false`: Creates `OnboardingAccount` at `_tmp:onboard_account:{email_hmac}`.
    ///
    /// Session creation for existing users happens at the SERVICE LAYER after
    /// this apply, because the user's data may live in a different region.
    VerifyEmailCode {
        /// HMAC of the email address.
        email_hmac: String,
        /// HMAC-SHA256(blinding_key, "code:" || uppercase(code)).
        code_hash: [u8; 32],
        /// Data residency region.
        region: Region,
        /// Pre-resolved at service layer (GLOBAL HMAC index read).
        /// `true` = email maps to an existing user.
        /// `false` = new email, apply handler creates `OnboardingAccount`.
        existing_user_hmac_hit: bool,
        /// Hash of the onboarding token (for new-user `OnboardingAccount` creation).
        /// Ignored when `existing_user_hmac_hit` is `true`.
        onboarding_token_hash: [u8; 32],
        /// Onboarding account expiration.
        /// Ignored when `existing_user_hmac_hit` is `true`.
        onboarding_expires_at: DateTime<Utc>,
        /// Pre-resolved TOTP data. `Some` = user has TOTP enabled, challenge data
        /// pre-generated. `None` = no TOTP. Only meaningful when
        /// `existing_user_hmac_hit` is `true`.
        totp: Option<TotpPreResolve>,
    },

    /// GC expired verification codes, onboarding accounts, and TOTP challenges.
    ///
    /// Proposed to REGIONAL Raft group. Has no fields — the region is
    /// implicit from the target Raft group. Scans `_tmp:onboard_verify:*`,
    /// `_tmp:onboard_account:*`, and `_tmp:totp_challenge:*` up to
    /// `MAX_ONBOARDING_SCAN` limit per prefix.
    CleanupExpiredOnboarding,

    // ── Onboarding Saga Requests ──
    /// Saga step 0 (GLOBAL): Allocate IDs, reserve HMAC, create
    /// provisioning directory entries.
    ///
    /// Idempotency: reads HMAC index — if `Provisioning(reservation)` with
    /// matching slug exists, returns the existing IDs.
    CreateOnboardingUser {
        /// HMAC of the email address.
        email_hmac: String,
        /// External Snowflake slug for the user.
        user_slug: UserSlug,
        /// External Snowflake slug for the organization.
        organization_slug: OrganizationSlug,
        /// Data residency region.
        region: Region,
    },

    /// Saga step 1 (REGIONAL): Write all PII and user/org profile data.
    ///
    /// PII fields (email, name, org_name) are AES-256-GCM sealed with the
    /// `shred_key_bytes` before entering the Raft log. On log replay after
    /// user erasure, the apply handler detects the erasure tombstone and
    /// skips the entry — the sealed PII is unrecoverable without re-provisioning
    /// the key, which the tombstone prevents.
    ///
    /// The `email_hmac` is passed explicitly — NOT derived in the apply handler
    /// (blinding key is external, deriving would break state machine determinism).
    WriteOnboardingUserProfile {
        /// User ID allocated in step 0.
        user_id: UserId,
        /// External user slug.
        user_slug: UserSlug,
        /// Organization ID allocated in step 0.
        organization_id: OrganizationId,
        /// External organization slug.
        organization_slug: OrganizationSlug,
        /// HMAC of the email (explicit, not derived).
        email_hmac: String,
        /// AES-256-GCM sealed PII (email, name, org_name).
        sealed_pii: Vec<u8>,
        /// Nonce for `sealed_pii` decryption.
        pii_nonce: [u8; 12],
        /// Per-user crypto-shredding key (generated by orchestrator).
        shred_key_bytes: [u8; 32],
        /// Refresh token hash.
        refresh_token_hash: [u8; 32],
        /// Refresh token family ID (16-byte random, poison detection).
        refresh_family_id: [u8; 16],
        /// Refresh token expiration.
        refresh_expires_at: DateTime<Utc>,
        /// Signing key identifier for JWT `kid` header.
        kid: String,
        /// Data residency region.
        region: Region,
    },

    /// Saga step 2 (GLOBAL): Activate user + org directory entries and
    /// update HMAC index from `Provisioning` to `Active`.
    ActivateOnboardingUser {
        /// User ID from step 0.
        user_id: UserId,
        /// External user slug.
        user_slug: UserSlug,
        /// Organization ID from step 0.
        organization_id: OrganizationId,
        /// External organization slug.
        organization_slug: OrganizationSlug,
        /// HMAC of the email (for HMAC index update).
        email_hmac: String,
    },

    /// Writes a team record to the regional store (REGIONAL-only, Pattern 1).
    ///
    /// Proposed to the REGIONAL Raft group via `propose_regional()`.
    /// The team directory entry (ID, slug) is created separately via the
    /// GLOBAL `LedgerRequest::CreateOrganizationTeam`. This separation
    /// ensures plaintext team names never enter the GLOBAL Raft log.
    WriteTeam {
        /// Organization containing the team.
        organization: OrganizationId,
        /// Team to write the record for.
        team: TeamId,
        /// External Snowflake slug (needed to create the team record from scratch
        /// in REGIONAL state where GLOBAL slug indices are unavailable).
        slug: TeamSlug,
        /// Team display name (PII — regional only).
        name: String,
    },

    /// Writes an app's display name and description to the regional store (PII).
    ///
    /// Proposed to the REGIONAL Raft group via `propose_regional()`.
    /// The app directory entry (ID, slug) is created separately via the
    /// GLOBAL `LedgerRequest::CreateApp`. This separation ensures plaintext
    /// app names and descriptions never enter the GLOBAL Raft log.
    WriteAppProfile {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App to write the profile for.
        app: AppId,
        /// App display name (PII — regional only).
        name: String,
        /// Optional description (PII — regional only).
        description: Option<String>,
    },

    /// Deletes a team record and name index from the REGIONAL state layer.
    ///
    /// Handles member migration if `move_members_to` is specified (both source
    /// and target records are in REGIONAL state). Must be proposed before the
    /// GLOBAL `DeleteOrganizationTeam` which cleans up slug indices.
    DeleteTeam {
        /// Organization containing the team.
        organization: OrganizationId,
        /// Team being deleted.
        team: TeamId,
        /// If set, move members to this team before deleting.
        move_members_to: Option<TeamId>,
    },

    /// Adds a member to a team record in REGIONAL state.
    ///
    /// Proposed to the REGIONAL Raft group via `propose_regional_org_encrypted()`.
    /// Requires the team profile to exist and the user to not already be a member.
    AddTeamMember {
        /// Organization containing the team.
        organization: OrganizationId,
        /// Team to add the member to.
        team: TeamId,
        /// User to add.
        user_id: UserId,
        /// Role for the new member.
        role: inferadb_ledger_state::system::TeamMemberRole,
    },

    /// Removes a member from a team's profile in REGIONAL state.
    ///
    /// Proposed to the REGIONAL Raft group via `propose_regional_org_encrypted()`.
    /// No-op if the user is not a member.
    RemoveTeamMember {
        /// Organization containing the team.
        organization: OrganizationId,
        /// Team to remove the member from.
        team: TeamId,
        /// User to remove.
        user_id: UserId,
    },

    /// Update a team member's role atomically.
    /// Proposed to the REGIONAL Raft group via `propose_regional_org_encrypted()`.
    UpdateTeamMemberRole {
        /// Organization containing the team.
        organization: OrganizationId,
        /// Team containing the member.
        team: TeamId,
        /// User whose role to update.
        user_id: UserId,
        /// New role.
        role: inferadb_ledger_state::system::TeamMemberRole,
    },

    /// Writes a client assertion's user-provided name to REGIONAL state.
    ///
    /// Proposed to the REGIONAL Raft group after the GLOBAL
    /// `CreateAppClientAssertion`. Separates the user-provided name
    /// (potential PII) from the structural assertion entry.
    WriteClientAssertionName {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App containing the assertion.
        app: AppId,
        /// Assertion whose name is being written.
        assertion: ClientAssertionId,
        /// User-provided name for this assertion entry.
        name: String,
    },

    /// Deletes a client assertion's name from REGIONAL state.
    ///
    /// Proposed to the REGIONAL Raft group before the GLOBAL
    /// `DeleteAppClientAssertion`.
    DeleteClientAssertionName {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App containing the assertion.
        app: AppId,
        /// Assertion whose name is being deleted.
        assertion: ClientAssertionId,
    },

    /// Deletes an app's profile, name index, and assertion names from
    /// the REGIONAL state layer.
    ///
    /// Must be proposed before the GLOBAL `DeleteApp` which cleans up
    /// slug indices, vault connections, and assertions.
    DeleteAppProfile {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App whose profile is being deleted.
        app: AppId,
    },

    /// Purges all REGIONAL data for a deleted organization.
    ///
    /// Deletes team profiles, app profiles, and name index entries from the
    /// REGIONAL state layer. Must be proposed before the GLOBAL
    /// `PurgeOrganization` which cleans up slug indices and structural data.
    PurgeOrganizationRegional {
        /// Organization being purged.
        organization: OrganizationId,
    },

    // ── User Credential Management ──
    /// Creates a new user credential (passkey, TOTP, or recovery code).
    ///
    /// Proposed via `EncryptedUserSystemRequest` — credential data is
    /// encrypted with the user's `UserShredKey` before entering the Raft log.
    ///
    /// The state machine allocates a `UserCredentialId` from the REGIONAL
    /// sequence counter, enforces uniqueness invariants (one TOTP, one
    /// recovery code set, passkey `credential_id` uniqueness per user),
    /// and stores both the entity and the type index entry.
    CreateUserCredential {
        /// User who owns the credential.
        user_id: UserId,
        /// Credential type discriminant.
        credential_type: CredentialType,
        /// Type-specific credential data.
        credential_data: CredentialData,
        /// Human-readable display name (e.g., "MacBook Touch ID").
        name: String,
    },

    /// Updates an existing user credential.
    ///
    /// Proposed via `EncryptedUserSystemRequest`.
    /// Only passkey-specific fields (`sign_count`, `backup_state`) and
    /// common fields (`name`, `enabled`) can be updated. TOTP credentials
    /// are immutable after creation.
    UpdateUserCredential {
        /// Owning user.
        user_id: UserId,
        /// Credential to update.
        credential_id: UserCredentialId,
        /// New display name (if provided).
        name: Option<String>,
        /// New enabled state (if provided).
        enabled: Option<bool>,
        /// Passkey-specific updates (if provided and credential is a passkey).
        passkey_update: Option<PasskeyCredential>,
    },

    /// Deletes a user credential.
    ///
    /// Proposed via `EncryptedUserSystemRequest`.
    /// The last-credential guard in the state machine prevents deleting
    /// the only remaining credential for a user.
    DeleteUserCredential {
        /// Owning user.
        user_id: UserId,
        /// Credential to delete.
        credential_id: UserCredentialId,
    },

    // ── TOTP Challenge Management ──
    /// Creates a pending TOTP challenge after primary authentication.
    ///
    /// Proposed as plain `LedgerRequest::System` (no PII — only IDs and
    /// nonces). Rate-limited to 3 active challenges per user.
    CreateTotpChallenge {
        /// User who must complete TOTP.
        user_id: UserId,
        /// External slug for session creation after TOTP verification.
        user_slug: UserSlug,
        /// Random 32-byte nonce (one-time use).
        nonce: [u8; 32],
        /// Challenge expiration.
        expires_at: DateTime<Utc>,
        /// Which primary method was used (audit trail).
        primary_method: PrimaryAuthMethod,
    },

    /// Consumes a TOTP challenge and creates a session.
    ///
    /// TOTP code verification happens in the service layer (non-deterministic
    /// `SystemTime::now()`). The state machine only performs deterministic
    /// operations: expiry check via `proposed_at`, challenge deletion, and
    /// refresh token creation.
    ConsumeTotpAndCreateSession {
        /// User whose challenge to consume.
        user_id: UserId,
        /// Challenge nonce to consume.
        nonce: [u8; 32],
        /// SHA-256 hash of the new refresh token string.
        token_hash: [u8; 32],
        /// Token family UUID for theft detection.
        family: [u8; 16],
        /// Signing key kid for the associated access token.
        kid: String,
        /// Refresh token TTL in seconds.
        ttl_secs: u64,
    },

    /// Consumes a recovery code and creates a session.
    ///
    /// The service layer pre-hashes the raw recovery code. The state machine
    /// verifies the hash against stored hashes, atomically removes it, deletes
    /// the challenge, and creates a refresh token.
    ConsumeRecoveryAndCreateSession {
        /// User whose recovery code to consume.
        user_id: UserId,
        /// Challenge nonce to consume.
        nonce: [u8; 32],
        /// SHA-256 hash of the raw recovery code (pre-hashed by service layer).
        code_hash: [u8; 32],
        /// Recovery code credential ID.
        credential_id: UserCredentialId,
        /// SHA-256 hash of the new refresh token string.
        token_hash: [u8; 32],
        /// Token family UUID for theft detection.
        family: [u8; 16],
        /// Signing key kid for the associated access token.
        kid: String,
        /// Refresh token TTL in seconds.
        ttl_secs: u64,
    },

    /// Increments the TOTP attempt counter on a challenge.
    ///
    /// Raft-persisted counter survives leader failover. Rejects if
    /// `attempts >= 3` at apply time.
    IncrementTotpAttempt {
        /// User whose challenge to update.
        user_id: UserId,
        /// Challenge nonce.
        nonce: [u8; 32],
    },

    // ── Invitation Operations (REGIONAL) ──
    /// Writes a full invitation record to REGIONAL state (encrypted with OrgShredKey).
    ///
    /// Proposed to the REGIONAL Raft group via `propose_regional_org_encrypted()`.
    /// Contains the plaintext email (PII) and role/team fields that are not
    /// in the GLOBAL `LedgerRequest::CreateOrganizationInvite`.
    WriteOrganizationInvite {
        /// Organization issuing the invitation.
        organization: OrganizationId,
        /// Internal invite ID (allocated by GLOBAL `CreateOrganizationInvite`).
        invite: InviteId,
        /// External Snowflake slug.
        slug: InviteSlug,
        /// SHA-256 hash of the raw invitation token.
        token_hash: [u8; 32],
        /// User who created the invitation.
        inviter: UserId,
        /// HMAC of the invitee's normalized email.
        invitee_email_hmac: String,
        /// Plaintext invitee email (PII — REGIONAL only).
        invitee_email: String,
        /// Role to assign upon acceptance.
        role: inferadb_ledger_state::system::OrganizationMemberRole,
        /// Optional team to auto-join upon acceptance.
        team: Option<TeamId>,
        /// Computed expiration timestamp (from GLOBAL response).
        expires_at: DateTime<Utc>,
    },

    /// Updates an invitation's status in REGIONAL state (encrypted with OrgShredKey).
    ///
    /// CAS: apply handler verifies `current_status == Pending` before applying.
    /// Uses `proposed_at` from `RaftPayload` as `resolved_at` timestamp.
    UpdateOrganizationInviteStatus {
        /// Organization that owns the invitation.
        organization: OrganizationId,
        /// Invitation to update.
        invite: InviteId,
        /// Terminal status to transition to.
        status: InvitationStatus,
    },

    /// Deletes an invitation record from REGIONAL state.
    ///
    /// Used by the retention reaper to clean up terminal invitations
    /// past the 90-day retention window.
    DeleteOrganizationInvite {
        /// Organization that owns the invitation.
        organization: OrganizationId,
        /// Invitation to delete.
        invite: InviteId,
    },

    /// REGIONAL — updates the `invitee_email_hmac` field in an invitation
    /// record during blinding key rotation. Without this, acceptance HMAC
    /// comparison would fail after key rotation.
    RehashInvitationEmailHmac {
        /// Organization that owns the invitation.
        organization: OrganizationId,
        /// Invitation to update.
        invite: InviteId,
        /// New HMAC hex computed with the rotated blinding key.
        new_hmac: String,
    },

    /// Creates a data region on all cluster nodes.
    ///
    /// Proposed to the GLOBAL Raft group. Each node's apply handler sends the
    /// region through a channel so the `RaftManager` can start the local
    /// region group, ensuring cluster-wide consensus on region creation.
    CreateDataRegion {
        /// The region to create.
        region: Region,
        /// Initial members for the region's Raft group.
        /// Carried in the Raft entry so all nodes use the same membership.
        initial_members: Vec<(u64, String)>,
    },

    /// Registers a peer's network address so all nodes can route to it.
    ///
    /// Proposed to GLOBAL when a node joins the cluster. All nodes apply it
    /// by updating their local peer_addresses map, enabling data region
    /// transports to reach the new node.
    RegisterPeerAddress {
        /// The node to register.
        node_id: u64,
        /// The node's gRPC address.
        address: String,
    },

    /// DR leader reports its current membership to GLOBAL.
    ///
    /// Used by the drain monitor to check when decommissioning nodes are fully
    /// drained from all data regions before removing them from GLOBAL.
    RegionMembershipReport {
        /// The data region this report is from.
        region: inferadb_ledger_types::Region,
        /// Current voting members of the data region's Raft group.
        voters: Vec<u64>,
        /// Current non-voting learners of the data region's Raft group.
        learners: Vec<u64>,
        /// Configuration epoch — stale reports with a lower epoch are ignored.
        conf_epoch: u64,
    },
}

/// Response from the Raft state machine.
///
/// This is the "R" (response) type in OpenRaft's type configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LedgerResponse {
    /// Empty response (for operations that don't return data).
    #[default]
    Empty,

    /// Writes operation completed.
    Write {
        /// Block height where the write was committed.
        block_height: u64,
        /// Block hash.
        block_hash: Hash,
        /// Server-assigned sequence number for this write.
        assigned_sequence: u64,
    },

    /// Organization directory entry created in GLOBAL control plane.
    OrganizationCreated {
        /// Allocated internal organization ID.
        organization_id: OrganizationId,
        /// External Snowflake slug.
        organization_slug: OrganizationSlug,
    },

    /// Organization profile written to regional store.
    OrganizationProfileWritten {
        /// Organization ID.
        organization_id: OrganizationId,
    },

    /// Organization directory status updated.
    OrganizationStatusUpdated {
        /// Organization ID.
        organization_id: OrganizationId,
    },

    /// Vault created.
    VaultCreated {
        /// Assigned internal vault ID.
        vault: VaultId,
        /// External Snowflake slug for API lookups.
        slug: VaultSlug,
    },

    /// Organization soft-deleted. Data retained for region-specific cooldown.
    OrganizationDeleted {
        /// Organization that was deleted.
        organization_id: OrganizationId,
        /// When the soft-delete was initiated.
        deleted_at: DateTime<Utc>,
        /// Region-derived retention period in days before purge.
        retention_days: u32,
    },

    /// Organization metadata updated.
    OrganizationUpdated {
        /// Organization that was updated.
        organization_id: OrganizationId,
    },

    /// Organization member removed.
    OrganizationMemberRemoved {
        /// Organization the member was removed from.
        organization_id: OrganizationId,
    },

    /// Organization member role updated.
    OrganizationMemberRoleUpdated {
        /// Organization whose member was updated.
        organization_id: OrganizationId,
    },

    /// Organization member added (or already existed — idempotent).
    OrganizationMemberAdded {
        /// Organization the member was added to.
        organization_id: OrganizationId,
        /// Whether the user was already a member (idempotent no-op).
        already_member: bool,
    },

    /// Organization invitation created (GLOBAL indexes written).
    ///
    /// Returned by `CreateOrganizationInvite`. The service handler needs
    /// `invite_id` and `expires_at` for the REGIONAL follow-up proposal.
    OrganizationInviteCreated {
        /// Allocated internal invite ID.
        invite_id: InviteId,
        /// External Snowflake slug (echoed back for convenience).
        invite_slug: InviteSlug,
        /// Computed expiration: `proposed_at + ttl_hours`.
        expires_at: DateTime<Utc>,
    },

    /// Organization invitation resolved to a terminal state.
    ///
    /// Returned by `ResolveOrganizationInvite` on successful CAS transition.
    OrganizationInviteResolved {
        /// Invitation that was resolved.
        invite_id: InviteId,
    },

    /// Returned by `PurgeOrganizationInviteIndexes` on success.
    OrganizationInviteIndexesPurged {
        /// Invitation that was purged.
        invite_id: InviteId,
    },

    /// Returned by `RehashInviteEmailIndex` on success.
    InviteEmailIndexRehashed {
        /// Invitation whose email index was rehashed.
        invite_id: InviteId,
    },

    /// Organization purged (all data removed).
    OrganizationPurged {
        /// Organization that was purged.
        organization_id: OrganizationId,
    },

    /// Organization migrated to a new region.
    OrganizationMigrated {
        /// Organization that was migrated.
        organization: OrganizationId,
        /// Previous region assignment.
        old_region: Region,
        /// New region assignment.
        new_region: Region,
    },

    /// Organization suspended.
    OrganizationSuspended {
        /// Organization that was suspended.
        organization: OrganizationId,
    },

    /// Organization resumed (suspension lifted).
    OrganizationResumed {
        /// Organization that was resumed.
        organization: OrganizationId,
    },

    /// Organization migration started.
    MigrationStarted {
        /// Organization entering migration.
        organization: OrganizationId,
        /// Target region for migration.
        target_region_group: Region,
    },

    /// Organization migration completed.
    MigrationCompleted {
        /// Organization that was migrated.
        organization: OrganizationId,
        /// Previous region assignment.
        old_region: Region,
        /// New region assignment.
        new_region: Region,
    },

    /// Vault deleted.
    VaultDeleted {
        /// Whether the deletion was successful.
        success: bool,
    },

    /// Vault metadata updated.
    VaultUpdated {
        /// Whether the update was successful.
        success: bool,
    },

    /// Vault health updated.
    VaultHealthUpdated {
        /// Whether the update was successful.
        success: bool,
    },

    /// User created.
    UserCreated {
        /// Assigned user ID.
        user_id: UserId,
        /// External Snowflake identifier.
        slug: UserSlug,
    },

    /// User updated.
    UserUpdated {
        /// Updated user ID.
        user_id: UserId,
    },

    /// User soft-deleted (pending erasure after retention period).
    UserSoftDeleted {
        /// Soft-deleted user ID.
        user_id: UserId,
        /// Region-derived retention period in days.
        retention_days: u32,
    },

    /// User email created.
    UserEmailCreated {
        /// Assigned email record ID.
        email_id: UserEmailId,
    },

    /// User email deleted.
    UserEmailDeleted {
        /// Deleted email record ID.
        email_id: UserEmailId,
    },

    /// User email verified.
    UserEmailVerified {
        /// Verified email record ID.
        email_id: UserEmailId,
    },

    /// User data erased via crypto-shredding.
    UserErased {
        /// User whose data was erased.
        user_id: UserId,
    },

    /// Flat-to-regional user migration completed.
    UsersMigrated {
        /// User records processed.
        users: u64,
        /// Users successfully migrated.
        migrated: u64,
        /// Users skipped (already migrated).
        skipped: u64,
        /// Users that failed migration.
        errors: u64,
    },

    /// Error response.
    Error {
        /// Structured error code for type-safe matching.
        ///
        /// Defaults to `Internal` when deserializing log entries that lack
        /// this field.
        #[serde(default)]
        code: inferadb_ledger_types::ErrorCode,
        /// Human-readable error message.
        message: String,
    },

    /// Precondition failed for conditional write.
    /// Returns current state for client-side conflict resolution.
    PreconditionFailed {
        /// Key that failed the condition.
        key: String,
        /// Current version of the entity (block height when last modified).
        current_version: Option<u64>,
        /// Current value of the entity.
        current_value: Option<Vec<u8>>,
        /// The condition that failed (for specific error code mapping).
        failed_condition: Option<SetCondition>,
    },

    /// Batches of responses from a BatchWrite request.
    ///
    /// Responses are in the same order as the requests in the corresponding
    /// `LedgerRequest::BatchWrite`.
    BatchWrite {
        /// Responses for each request in the batch.
        responses: Vec<LedgerResponse>,
    },

    /// Team created.
    OrganizationTeamCreated {
        /// Assigned internal team ID.
        team_id: TeamId,
        /// External Snowflake slug.
        team_slug: TeamSlug,
    },

    /// Team deleted.
    OrganizationTeamDeleted {
        /// Organization the team belonged to.
        organization_id: OrganizationId,
    },

    /// App created.
    AppCreated {
        /// Assigned internal app ID.
        app_id: AppId,
        /// External Snowflake slug.
        app_slug: AppSlug,
    },

    /// App deleted.
    AppDeleted {
        /// Organization the app belonged to.
        organization_id: OrganizationId,
    },

    /// App enabled or disabled.
    AppToggled {
        /// Organization the app belongs to.
        organization_id: OrganizationId,
    },

    /// App credential enabled/disabled.
    AppCredentialToggled {
        /// Organization the app belongs to.
        organization_id: OrganizationId,
    },

    /// App client secret rotated.
    AppClientSecretRotated {
        /// Organization the app belongs to.
        organization_id: OrganizationId,
    },

    /// Client assertion entry created.
    AppClientAssertionCreated {
        /// Assigned assertion entry ID.
        assertion_id: ClientAssertionId,
    },

    /// Client assertion entry deleted.
    AppClientAssertionDeleted {
        /// Organization the app belongs to.
        organization_id: OrganizationId,
    },

    /// Client assertion entry enabled/disabled.
    AppClientAssertionToggled {
        /// Organization the app belongs to.
        organization_id: OrganizationId,
    },

    /// App vault connection added.
    AppVaultAdded {
        /// Organization the app belongs to.
        organization_id: OrganizationId,
    },

    /// App vault connection updated.
    AppVaultUpdated {
        /// Organization the app belongs to.
        organization_id: OrganizationId,
    },

    /// App vault connection removed.
    AppVaultRemoved {
        /// Organization the app belongs to.
        organization_id: OrganizationId,
    },

    // ── Signing Key Responses ──
    /// Signing key created.
    SigningKeyCreated {
        /// Assigned internal signing key ID.
        id: SigningKeyId,
        /// UUID-format key identifier.
        kid: String,
    },

    /// Signing key rotated (old key transitioned, new key active).
    SigningKeyRotated {
        /// Kid of the key that was rotated out.
        old_kid: String,
        /// Kid of the new active key.
        new_kid: String,
    },

    /// Signing key revoked.
    SigningKeyRevoked {
        /// Kid of the revoked key.
        kid: String,
    },

    /// Rotated signing key transitioned to Revoked (grace period expired).
    SigningKeyTransitioned {
        /// Kid of the transitioned key.
        kid: String,
    },

    // ── Refresh Token Responses ──
    /// Refresh token created.
    RefreshTokenCreated {
        /// Assigned internal refresh token ID.
        id: RefreshTokenId,
    },

    /// Refresh token rotated (old consumed, new created).
    RefreshTokenRotated {
        /// Assigned ID for the new refresh token.
        new_id: RefreshTokenId,
        /// For user sessions: authoritative `TokenVersion` at Raft commit time.
        /// Service signs the access token using this version.
        token_version: Option<TokenVersion>,
        /// For vault tokens: current `allowed_scopes` from `AppVaultConnection`.
        /// Used directly as the new access token's scopes.
        allowed_scopes: Option<Vec<String>>,
    },

    /// Token family revoked.
    TokenFamilyRevoked {
        /// Number of tokens revoked in the family.
        count: u64,
    },

    /// All user sessions revoked and `TokenVersion` incremented.
    AllUserSessionsRevoked {
        /// Number of tokens revoked.
        count: u64,
        /// New `TokenVersion` after increment.
        version: TokenVersion,
    },

    /// All app sessions revoked and `TokenVersion` incremented.
    AllAppSessionsRevoked {
        /// Number of tokens revoked.
        count: u64,
        /// New `TokenVersion` after increment.
        version: TokenVersion,
    },

    /// Expired refresh tokens deleted and poisoned families cleaned.
    ExpiredRefreshTokensDeleted {
        /// Number of tokens cleaned up.
        count: u64,
    },

    /// User profile (name) updated in the regional store.
    UserProfileUpdated {
        /// Updated user ID.
        user_id: UserId,
    },

    // ── Onboarding Responses ──
    /// Email verification code stored successfully.
    EmailVerificationCreated,

    /// Email code verified. Result indicates existing vs new user.
    EmailCodeVerified {
        /// Whether the email belongs to an existing user or is new.
        result: EmailCodeVerifiedResult,
    },

    /// Saga step 0 completed: IDs allocated, HMAC reserved, directories created.
    OnboardingUserCreated {
        /// Allocated internal user ID.
        user_id: UserId,
        /// Allocated internal organization ID.
        organization_id: OrganizationId,
    },

    /// Saga step 1 completed: PII and session material written regionally.
    OnboardingUserProfileWritten {
        /// Assigned refresh token ID.
        refresh_token_id: RefreshTokenId,
    },

    /// Saga step 2 completed: directories activated, HMAC index updated.
    OnboardingUserActivated,

    /// Expired onboarding records and TOTP challenges cleaned up.
    OnboardingCleanedUp {
        /// Number of expired verification codes deleted.
        verification_codes_deleted: u32,
        /// Number of expired onboarding accounts deleted.
        onboarding_accounts_deleted: u32,
        /// Number of expired TOTP challenges deleted.
        totp_challenges_deleted: u32,
    },

    // ── User Credential Responses ──
    /// User credential created.
    UserCredentialCreated {
        /// Allocated credential ID (from REGIONAL sequence).
        credential_id: UserCredentialId,
    },

    /// User credential updated.
    UserCredentialUpdated {
        /// Updated credential ID.
        credential_id: UserCredentialId,
    },

    /// User credential deleted.
    UserCredentialDeleted {
        /// Deleted credential ID.
        credential_id: UserCredentialId,
    },

    /// TOTP challenge created after primary auth.
    TotpChallengeCreated {
        /// Challenge nonce (returned to caller for `VerifyTotp`).
        nonce: [u8; 32],
    },

    /// TOTP verified, session created directly.
    TotpVerified {
        /// Allocated refresh token ID.
        refresh_token_id: RefreshTokenId,
    },

    /// Recovery code consumed, session created directly.
    RecoveryCodeConsumed {
        /// Allocated refresh token ID.
        refresh_token_id: RefreshTokenId,
        /// Number of remaining unused recovery codes.
        remaining_codes: u32,
    },

    /// TOTP attempt counter incremented.
    TotpAttemptIncremented {
        /// Updated attempt count.
        attempts: u8,
    },

    /// Data region created via GLOBAL Raft consensus.
    ///
    /// Each node's apply handler triggers local region startup through
    /// the `region_creation_sender` channel.
    DataRegionCreated {
        /// The region that was created.
        region: Region,
    },
}

/// TOTP pre-resolved data embedded in [`SystemRequest::VerifyEmailCode`].
///
/// The service layer reads GLOBAL indices to determine TOTP status and pre-generates
/// challenge data before proposing to the REGIONAL Raft group. Bundled as a struct
/// so `Some` vs `None` replaces `user_has_totp: bool` + 4 optional fields — making
/// invalid states unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TotpPreResolve {
    /// Pre-generated challenge nonce (32 random bytes).
    pub nonce: [u8; 32],
    /// Absolute expiry for the TOTP challenge (5-minute TTL from proposal).
    pub expires_at: DateTime<Utc>,
    /// User ID, pre-resolved from GLOBAL email hash index.
    pub user_id: UserId,
    /// User slug for session creation after TOTP verification.
    pub user_slug: UserSlug,
}

/// Result of email code verification.
///
/// Three variants — signals to the service handler:
/// - `ExistingUser`: email belongs to an existing user without TOTP, create session.
/// - `TotpRequired`: email belongs to an existing user WITH TOTP, challenge created.
/// - `NewUser`: new email, `OnboardingAccount` created in regional store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmailCodeVerifiedResult {
    /// Code verified, email belongs to an existing user without TOTP.
    /// No session data — session creation is handled by the service handler
    /// (which proposes `CreateRefreshToken` to the user's actual region).
    ExistingUser,
    /// Code verified, existing user has TOTP enabled. `PendingTotpChallenge`
    /// created atomically. Service handler returns `TotpRequired` to caller.
    TotpRequired {
        /// Challenge nonce for the `VerifyTotp` RPC.
        nonce: [u8; 32],
    },
    /// Code verified, new email. `OnboardingAccount` created in regional store.
    NewUser,
}

impl fmt::Display for LedgerResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LedgerResponse::Empty => write!(f, "Empty"),
            LedgerResponse::Write { block_height, .. } => {
                write!(f, "Write(height={})", block_height)
            },
            LedgerResponse::OrganizationCreated { organization_id, organization_slug } => {
                write!(f, "OrganizationCreated(id={}, slug={})", organization_id, organization_slug)
            },
            LedgerResponse::OrganizationProfileWritten { organization_id } => {
                write!(f, "OrganizationProfileWritten(id={})", organization_id)
            },
            LedgerResponse::OrganizationStatusUpdated { organization_id } => {
                write!(f, "OrganizationStatusUpdated(id={})", organization_id)
            },
            LedgerResponse::VaultCreated { vault, slug } => {
                write!(f, "VaultCreated(id={}, slug={})", vault, slug)
            },
            LedgerResponse::UserCreated { user_id, slug } => {
                write!(f, "UserCreated(id={}, slug={})", user_id, slug)
            },
            LedgerResponse::OrganizationDeleted { organization_id, retention_days, .. } => {
                write!(
                    f,
                    "OrganizationDeleted(id={}, retention_days={})",
                    organization_id, retention_days
                )
            },
            LedgerResponse::OrganizationUpdated { organization_id } => {
                write!(f, "OrganizationUpdated(id={})", organization_id)
            },
            LedgerResponse::OrganizationMemberRemoved { organization_id } => {
                write!(f, "OrganizationMemberRemoved(id={})", organization_id)
            },
            LedgerResponse::OrganizationMemberRoleUpdated { organization_id } => {
                write!(f, "OrganizationMemberRoleUpdated(id={})", organization_id)
            },
            LedgerResponse::OrganizationMemberAdded { organization_id, already_member } => {
                write!(
                    f,
                    "OrganizationMemberAdded(id={}, already_member={})",
                    organization_id, already_member
                )
            },
            LedgerResponse::OrganizationInviteCreated { invite_id, invite_slug, expires_at } => {
                write!(
                    f,
                    "OrganizationInviteCreated(id={}, slug={}, expires_at={})",
                    invite_id, invite_slug, expires_at
                )
            },
            LedgerResponse::OrganizationInviteResolved { invite_id } => {
                write!(f, "OrganizationInviteResolved(id={})", invite_id)
            },
            LedgerResponse::OrganizationInviteIndexesPurged { invite_id } => {
                write!(f, "OrganizationInviteIndexesPurged(id={})", invite_id)
            },
            LedgerResponse::InviteEmailIndexRehashed { invite_id } => {
                write!(f, "InviteEmailIndexRehashed(id={})", invite_id)
            },
            LedgerResponse::OrganizationTeamCreated { team_id, team_slug } => {
                write!(f, "OrganizationTeamCreated(id={}, slug={})", team_id, team_slug)
            },
            LedgerResponse::OrganizationTeamDeleted { organization_id } => {
                write!(f, "OrganizationTeamDeleted(org={})", organization_id)
            },
            LedgerResponse::OrganizationPurged { organization_id } => {
                write!(f, "OrganizationPurged(id={})", organization_id)
            },
            LedgerResponse::OrganizationMigrated { organization, old_region, new_region } => {
                write!(
                    f,
                    "OrganizationMigrated(id={}, {}->{})",
                    organization, old_region, new_region
                )
            },
            LedgerResponse::OrganizationSuspended { organization } => {
                write!(f, "OrganizationSuspended(id={})", organization)
            },
            LedgerResponse::OrganizationResumed { organization } => {
                write!(f, "OrganizationResumed(id={})", organization)
            },
            LedgerResponse::MigrationStarted { organization, target_region_group } => {
                write!(f, "MigrationStarted(id={}, target={})", organization, target_region_group)
            },
            LedgerResponse::MigrationCompleted { organization, old_region, new_region } => {
                write!(f, "MigrationCompleted(id={}, {}->{})", organization, old_region, new_region)
            },
            LedgerResponse::VaultDeleted { success } => {
                write!(f, "VaultDeleted(success={})", success)
            },
            LedgerResponse::VaultUpdated { success } => {
                write!(f, "VaultUpdated(success={})", success)
            },
            LedgerResponse::VaultHealthUpdated { success } => {
                write!(f, "VaultHealthUpdated(success={})", success)
            },
            LedgerResponse::UserUpdated { user_id } => {
                write!(f, "UserUpdated(id={})", user_id)
            },
            LedgerResponse::UserSoftDeleted { user_id, retention_days } => {
                write!(f, "UserSoftDeleted(id={}, retention_days={})", user_id, retention_days)
            },
            LedgerResponse::UserEmailCreated { email_id } => {
                write!(f, "UserEmailCreated(id={})", email_id)
            },
            LedgerResponse::UserEmailDeleted { email_id } => {
                write!(f, "UserEmailDeleted(id={})", email_id)
            },
            LedgerResponse::UserEmailVerified { email_id } => {
                write!(f, "UserEmailVerified(id={})", email_id)
            },
            LedgerResponse::UserErased { user_id } => {
                write!(f, "UserErased(id={})", user_id)
            },
            LedgerResponse::UsersMigrated { users, migrated, skipped, errors } => {
                write!(
                    f,
                    "UsersMigrated(total={}, migrated={}, skipped={}, errors={})",
                    users, migrated, skipped, errors
                )
            },
            LedgerResponse::Error { code, message } => {
                write!(f, "Error({code:?}: {message})")
            },
            LedgerResponse::PreconditionFailed { key, .. } => {
                write!(f, "PreconditionFailed(key={})", key)
            },
            LedgerResponse::BatchWrite { responses } => {
                write!(f, "BatchWrite(count={})", responses.len())
            },
            LedgerResponse::AppCreated { app_id, app_slug } => {
                write!(f, "AppCreated(id={}, slug={})", app_id, app_slug)
            },
            LedgerResponse::AppDeleted { organization_id } => {
                write!(f, "AppDeleted(org={})", organization_id)
            },
            LedgerResponse::AppToggled { organization_id } => {
                write!(f, "AppToggled(org={})", organization_id)
            },
            LedgerResponse::AppCredentialToggled { organization_id } => {
                write!(f, "AppCredentialToggled(org={})", organization_id)
            },
            LedgerResponse::AppClientSecretRotated { organization_id } => {
                write!(f, "AppClientSecretRotated(org={})", organization_id)
            },
            LedgerResponse::AppClientAssertionCreated { assertion_id } => {
                write!(f, "AppClientAssertionCreated(id={})", assertion_id)
            },
            LedgerResponse::AppClientAssertionDeleted { organization_id } => {
                write!(f, "AppClientAssertionDeleted(org={})", organization_id)
            },
            LedgerResponse::AppClientAssertionToggled { organization_id } => {
                write!(f, "AppClientAssertionToggled(org={})", organization_id)
            },
            LedgerResponse::AppVaultAdded { organization_id } => {
                write!(f, "AppVaultAdded(org={})", organization_id)
            },
            LedgerResponse::AppVaultUpdated { organization_id } => {
                write!(f, "AppVaultUpdated(org={})", organization_id)
            },
            LedgerResponse::AppVaultRemoved { organization_id } => {
                write!(f, "AppVaultRemoved(org={})", organization_id)
            },
            LedgerResponse::SigningKeyCreated { id, kid } => {
                write!(f, "SigningKeyCreated(id={}, kid={})", id, kid)
            },
            LedgerResponse::SigningKeyRotated { old_kid, new_kid } => {
                write!(f, "SigningKeyRotated(old={}, new={})", old_kid, new_kid)
            },
            LedgerResponse::SigningKeyRevoked { kid } => {
                write!(f, "SigningKeyRevoked(kid={})", kid)
            },
            LedgerResponse::SigningKeyTransitioned { kid } => {
                write!(f, "SigningKeyTransitioned(kid={})", kid)
            },
            LedgerResponse::RefreshTokenCreated { id } => {
                write!(f, "RefreshTokenCreated(id={})", id)
            },
            LedgerResponse::RefreshTokenRotated { new_id, .. } => {
                write!(f, "RefreshTokenRotated(new_id={})", new_id)
            },
            LedgerResponse::TokenFamilyRevoked { count } => {
                write!(f, "TokenFamilyRevoked(count={})", count)
            },
            LedgerResponse::AllUserSessionsRevoked { count, version } => {
                write!(f, "AllUserSessionsRevoked(count={}, version={})", count, version)
            },
            LedgerResponse::AllAppSessionsRevoked { count, version } => {
                write!(f, "AllAppSessionsRevoked(count={}, version={})", count, version)
            },
            LedgerResponse::ExpiredRefreshTokensDeleted { count } => {
                write!(f, "ExpiredRefreshTokensDeleted(count={})", count)
            },
            LedgerResponse::UserProfileUpdated { user_id } => {
                write!(f, "UserProfileUpdated(id={})", user_id)
            },
            LedgerResponse::EmailVerificationCreated => {
                write!(f, "EmailVerificationCreated")
            },
            LedgerResponse::EmailCodeVerified { result } => {
                write!(f, "EmailCodeVerified({result:?})")
            },
            LedgerResponse::OnboardingUserCreated { user_id, organization_id } => {
                write!(f, "OnboardingUserCreated(user={}, org={})", user_id, organization_id)
            },
            LedgerResponse::OnboardingUserProfileWritten { refresh_token_id } => {
                write!(f, "OnboardingUserProfileWritten(refresh={})", refresh_token_id)
            },
            LedgerResponse::OnboardingUserActivated => {
                write!(f, "OnboardingUserActivated")
            },
            LedgerResponse::OnboardingCleanedUp {
                verification_codes_deleted,
                onboarding_accounts_deleted,
                totp_challenges_deleted,
            } => {
                write!(
                    f,
                    "OnboardingCleanedUp(codes={}, accounts={}, totp_challenges={})",
                    verification_codes_deleted,
                    onboarding_accounts_deleted,
                    totp_challenges_deleted
                )
            },
            LedgerResponse::UserCredentialCreated { credential_id } => {
                write!(f, "UserCredentialCreated(id={})", credential_id)
            },
            LedgerResponse::UserCredentialUpdated { credential_id } => {
                write!(f, "UserCredentialUpdated(id={})", credential_id)
            },
            LedgerResponse::UserCredentialDeleted { credential_id } => {
                write!(f, "UserCredentialDeleted(id={})", credential_id)
            },
            LedgerResponse::TotpChallengeCreated { .. } => {
                write!(f, "TotpChallengeCreated")
            },
            LedgerResponse::TotpVerified { refresh_token_id } => {
                write!(f, "TotpVerified(refresh={})", refresh_token_id)
            },
            LedgerResponse::RecoveryCodeConsumed { refresh_token_id, remaining_codes } => {
                write!(
                    f,
                    "RecoveryCodeConsumed(refresh={}, remaining={})",
                    refresh_token_id, remaining_codes
                )
            },
            LedgerResponse::TotpAttemptIncremented { attempts } => {
                write!(f, "TotpAttemptIncremented(attempts={})", attempts)
            },
            LedgerResponse::DataRegionCreated { region } => {
                write!(f, "DataRegionCreated(region={})", region)
            },
        }
    }
}

// ============================================================================
// Liveness Configuration
// ============================================================================

/// Configuration for dead-node detection.
///
/// Controls the timeouts and intervals used by the GLOBAL leader's quorum-based
/// liveness checker to detect and mark dead nodes.
#[derive(Debug, Clone)]
pub struct LivenessConfig {
    /// Time after which a node is suspected dead by the local observer.
    ///
    /// Default: 5 minutes.
    pub dead_node_timeout: std::time::Duration,
    /// How often the GLOBAL leader runs the liveness check.
    ///
    /// Default: 30 seconds.
    pub liveness_check_interval: std::time::Duration,
    /// Timeout for each `CheckPeerLiveness` RPC.
    ///
    /// Default: 2 seconds.
    pub liveness_query_timeout: std::time::Duration,
    /// Hard wall-clock budget for the entire quorum confirmation per suspect.
    ///
    /// Default: 10 seconds.
    pub liveness_cycle_budget: std::time::Duration,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            dead_node_timeout: std::time::Duration::from_secs(300),
            liveness_check_interval: std::time::Duration::from_secs(30),
            liveness_query_timeout: std::time::Duration::from_secs(2),
            liveness_cycle_budget: std::time::Duration::from_secs(10),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn node_status_serde_roundtrip() {
        use crate::types::NodeStatus;
        let statuses = [
            NodeStatus::Active,
            NodeStatus::Decommissioning,
            NodeStatus::Dead,
            NodeStatus::Removed,
        ];
        for status in &statuses {
            let encoded = inferadb_ledger_types::encode(status).unwrap();
            let decoded: NodeStatus = inferadb_ledger_types::decode(&encoded).unwrap();
            assert_eq!(*status, decoded);
        }
    }

    /// Classifies whether a Raft request targets the GLOBAL or REGIONAL group.
    ///
    /// GLOBAL requests contain no plaintext PII (only IDs, slugs, hashes).
    /// REGIONAL requests carry plaintext PII and must be proposed via
    /// encrypted regional channels.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RaftScope {
        /// No plaintext PII — proposed to the global Raft group.
        Global,
        /// Contains plaintext PII — proposed to a regional Raft group.
        Regional,
    }

    #[test]
    fn test_ledger_request_serialization() {
        let request = LedgerRequest::System(SystemRequest::CreateOrganization {
            slug: OrganizationSlug::new(12345),
            region: Region::US_EAST_VA,
            tier: Default::default(),
            admin: UserId::new(1),
        });

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::System(SystemRequest::CreateOrganization { slug, region, .. }) => {
                assert_eq!(slug, OrganizationSlug::new(12345));
                assert_eq!(region, Region::US_EAST_VA);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_add_organization_member_request_serialization_roundtrip() {
        let request = LedgerRequest::AddOrganizationMember {
            organization: OrganizationId::new(5),
            user: UserId::new(42),
            user_slug: UserSlug::new(4200),
            role: inferadb_ledger_state::system::OrganizationMemberRole::Member,
        };
        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");
        match deserialized {
            LedgerRequest::AddOrganizationMember { organization, user, user_slug, role } => {
                assert_eq!(organization, OrganizationId::new(5));
                assert_eq!(user, UserId::new(42));
                assert_eq!(user_slug, UserSlug::new(4200));
                assert_eq!(role, inferadb_ledger_state::system::OrganizationMemberRole::Member);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_organization_member_added_response_serialization_roundtrip() {
        let response = LedgerResponse::OrganizationMemberAdded {
            organization_id: OrganizationId::new(5),
            already_member: true,
        };
        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        match deserialized {
            LedgerResponse::OrganizationMemberAdded { organization_id, already_member } => {
                assert_eq!(organization_id, OrganizationId::new(5));
                assert!(already_member);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_ledger_response_display() {
        let response =
            LedgerResponse::Write { block_height: 42, block_hash: [0u8; 32], assigned_sequence: 1 };
        assert_eq!(format!("{}", response), "Write(height=42)");
    }

    #[test]
    fn test_system_request_serialization() {
        let request = SystemRequest::CreateUser {
            user: UserId::new(42),
            admin: false,
            slug: UserSlug::new(12345),
            region: Region::US_EAST_VA,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::CreateUser { user, admin, slug, region } => {
                assert_eq!(user, UserId::new(42));
                assert!(!admin);
                assert_eq!(slug, UserSlug::new(12345));
                assert_eq!(region, Region::US_EAST_VA);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_register_email_hash_serialization() {
        let request = SystemRequest::RegisterEmailHash {
            hmac_hex: "a1b2c3".to_string(),
            user_id: UserId::new(42),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::RegisterEmailHash { hmac_hex, user_id } => {
                assert_eq!(hmac_hex, "a1b2c3");
                assert_eq!(user_id, UserId::new(42));
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_remove_email_hash_serialization() {
        let request = SystemRequest::RemoveEmailHash { hmac_hex: "deadbeef".to_string() };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::RemoveEmailHash { hmac_hex } => {
                assert_eq!(hmac_hex, "deadbeef");
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_set_blinding_key_version_serialization() {
        let request = SystemRequest::SetBlindingKeyVersion { version: 3 };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::SetBlindingKeyVersion { version } => {
                assert_eq!(version, 3);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_update_rehash_progress_serialization() {
        let request = SystemRequest::UpdateRehashProgress {
            region: Region::IE_EAST_DUBLIN,
            entries_rehashed: 1500,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::UpdateRehashProgress { region, entries_rehashed } => {
                assert_eq!(region, Region::IE_EAST_DUBLIN);
                assert_eq!(entries_rehashed, 1500);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_clear_rehash_progress_serialization() {
        let request = SystemRequest::ClearRehashProgress { region: Region::IN_WEST_MUMBAI };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::ClearRehashProgress { region } => {
                assert_eq!(region, Region::IN_WEST_MUMBAI);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_system_request_update_user_profile_serialization() {
        let request = SystemRequest::UpdateUserProfile {
            user_id: UserId::new(42),
            name: "Alice".to_string(),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            SystemRequest::UpdateUserProfile { user_id, name } => {
                assert_eq!(user_id, UserId::new(42));
                assert_eq!(name, "Alice");
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_ledger_response_user_profile_updated_serialization() {
        let response = LedgerResponse::UserProfileUpdated { user_id: UserId::new(7) };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerResponse::UserProfileUpdated { user_id } => {
                assert_eq!(user_id, UserId::new(7));
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_raft_payload_serde_roundtrip() {
        use chrono::TimeZone;

        let payload = RaftPayload {
            request: LedgerRequest::System(SystemRequest::CreateOrganization {
                slug: OrganizationSlug::new(999),
                region: Region::US_EAST_VA,
                tier: Default::default(),
                admin: UserId::new(1),
            }),
            proposed_at: Utc.with_ymd_and_hms(2099, 6, 15, 12, 30, 0).unwrap(),
            state_root_commitments: vec![],
            caller: 0,
        };

        let bytes = postcard::to_allocvec(&payload).expect("serialize");
        let deserialized: RaftPayload = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(payload, deserialized);
        assert_eq!(deserialized.proposed_at, Utc.with_ymd_and_hms(2099, 6, 15, 12, 30, 0).unwrap());
        match &deserialized.request {
            LedgerRequest::System(SystemRequest::CreateOrganization { slug, .. }) => {
                assert_eq!(*slug, OrganizationSlug::new(999));
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_raft_payload_preserves_proposed_at_across_reserialize() {
        use chrono::TimeZone;

        let ts = Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap();
        let payload = RaftPayload {
            request: LedgerRequest::Write {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0; 16],
                request_hash: 0,
            },
            proposed_at: ts,
            state_root_commitments: vec![],
            caller: 0,
        };

        let bytes1 = postcard::to_allocvec(&payload).expect("serialize");
        let decoded: RaftPayload = postcard::from_bytes(&bytes1).expect("deserialize");
        let bytes2 = postcard::to_allocvec(&decoded).expect("re-serialize");

        assert_eq!(bytes1, bytes2, "re-serialization should produce identical bytes");
    }

    // ============================================
    // State Root Commitment Serialization Tests
    // ============================================

    #[test]
    fn test_state_root_commitment_serialization_roundtrip() {
        let commitment = StateRootCommitment {
            organization: OrganizationId::new(42),
            vault: VaultId::new(7),
            vault_height: 100,
            state_root: [0xAB; 32],
        };

        let bytes = postcard::to_allocvec(&commitment).expect("serialize");
        let deserialized: StateRootCommitment = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(commitment, deserialized);
        assert_eq!(deserialized.organization, OrganizationId::new(42));
        assert_eq!(deserialized.vault, VaultId::new(7));
        assert_eq!(deserialized.vault_height, 100);
        assert_eq!(deserialized.state_root, [0xAB; 32]);
    }

    #[test]
    fn test_state_root_commitment_zero_hash() {
        let commitment = StateRootCommitment {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            vault_height: 0,
            state_root: [0u8; 32],
        };

        let bytes = postcard::to_allocvec(&commitment).expect("serialize");
        let deserialized: StateRootCommitment = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(commitment, deserialized);
        assert_eq!(deserialized.state_root, [0u8; 32]);
    }

    #[test]
    fn test_raft_payload_with_commitments_roundtrip() {
        use chrono::TimeZone;

        let commitments = vec![
            StateRootCommitment {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                vault_height: 10,
                state_root: [0xAA; 32],
            },
            StateRootCommitment {
                organization: OrganizationId::new(2),
                vault: VaultId::new(3),
                vault_height: 20,
                state_root: [0xBB; 32],
            },
        ];

        let payload = RaftPayload {
            request: LedgerRequest::Write {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                transactions: vec![],
                idempotency_key: [0; 16],
                request_hash: 0,
            },
            proposed_at: Utc.with_ymd_and_hms(2099, 6, 15, 12, 0, 0).unwrap(),
            state_root_commitments: commitments.clone(),
            caller: 0,
        };

        let bytes = postcard::to_allocvec(&payload).expect("serialize");
        let deserialized: RaftPayload = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(deserialized.state_root_commitments.len(), 2);
        assert_eq!(deserialized.state_root_commitments[0].organization, OrganizationId::new(1));
        assert_eq!(deserialized.state_root_commitments[0].vault_height, 10);
        assert_eq!(deserialized.state_root_commitments[0].state_root, [0xAA; 32]);
        assert_eq!(deserialized.state_root_commitments[1].organization, OrganizationId::new(2));
        assert_eq!(deserialized.state_root_commitments[1].vault_height, 20);
        assert_eq!(deserialized.state_root_commitments[1].state_root, [0xBB; 32]);
    }

    #[test]
    fn test_raft_payload_backward_compat_empty_commitments() {
        // Payloads serialized before the commitments field was added should
        // deserialize with an empty vec (thanks to #[serde(default)]).
        use chrono::TimeZone;

        let payload_without = RaftPayload {
            request: LedgerRequest::System(SystemRequest::CreateOrganization {
                slug: OrganizationSlug::new(1),
                region: Region::US_EAST_VA,
                tier: Default::default(),
                admin: UserId::new(1),
            }),
            proposed_at: Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap(),
            state_root_commitments: vec![],
            caller: 0,
        };

        let bytes = postcard::to_allocvec(&payload_without).expect("serialize");
        let deserialized: RaftPayload = postcard::from_bytes(&bytes).expect("deserialize");

        assert!(deserialized.state_root_commitments.is_empty());
    }

    #[test]
    fn test_raft_payload_with_commitments_preserves_bytes_across_reserialize() {
        use chrono::TimeZone;

        let payload = RaftPayload {
            request: LedgerRequest::Write {
                organization: OrganizationId::new(5),
                vault: VaultId::new(3),
                transactions: vec![],
                idempotency_key: [42; 16],
                request_hash: 12345,
            },
            proposed_at: Utc.with_ymd_and_hms(2099, 3, 1, 0, 0, 0).unwrap(),
            state_root_commitments: vec![StateRootCommitment {
                organization: OrganizationId::new(5),
                vault: VaultId::new(3),
                vault_height: 99,
                state_root: [0xFF; 32],
            }],
            caller: 0,
        };

        let bytes1 = postcard::to_allocvec(&payload).expect("serialize");
        let decoded: RaftPayload = postcard::from_bytes(&bytes1).expect("deserialize");
        let bytes2 = postcard::to_allocvec(&decoded).expect("re-serialize");

        assert_eq!(bytes1, bytes2, "re-serialization with commitments should be stable");
    }

    // ============================================
    // Signing Key & Refresh Token Variant Tests
    // ============================================

    #[test]
    fn test_create_signing_key_serialization() {
        let request = LedgerRequest::CreateSigningKey {
            scope: inferadb_ledger_state::system::SigningKeyScope::Global,
            kid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            public_key_bytes: vec![0xAA; 32],
            encrypted_private_key: vec![0xBB; 100],
            rmk_version: 1,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::CreateSigningKey {
                scope, kid, public_key_bytes, rmk_version, ..
            } => {
                assert_eq!(scope, inferadb_ledger_state::system::SigningKeyScope::Global);
                assert_eq!(kid, "550e8400-e29b-41d4-a716-446655440000");
                assert_eq!(public_key_bytes.len(), 32);
                assert_eq!(rmk_version, 1);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_create_signing_key_org_scope_serialization() {
        let request = LedgerRequest::CreateSigningKey {
            scope: inferadb_ledger_state::system::SigningKeyScope::Organization(
                OrganizationId::new(42),
            ),
            kid: "key-uuid".to_string(),
            public_key_bytes: vec![0xCC; 32],
            encrypted_private_key: vec![0xDD; 100],
            rmk_version: 2,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::CreateSigningKey { scope, rmk_version, .. } => {
                assert_eq!(
                    scope,
                    inferadb_ledger_state::system::SigningKeyScope::Organization(
                        OrganizationId::new(42)
                    )
                );
                assert_eq!(rmk_version, 2);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_rotate_signing_key_serialization() {
        let request = LedgerRequest::RotateSigningKey {
            old_kid: "old-kid".to_string(),
            new_kid: "new-kid".to_string(),
            new_public_key_bytes: vec![0xAA; 32],
            new_encrypted_private_key: vec![0xBB; 100],
            rmk_version: 3,
            grace_period_secs: 14400,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::RotateSigningKey {
                old_kid,
                new_kid,
                grace_period_secs,
                rmk_version,
                ..
            } => {
                assert_eq!(old_kid, "old-kid");
                assert_eq!(new_kid, "new-kid");
                assert_eq!(grace_period_secs, 14400);
                assert_eq!(rmk_version, 3);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_revoke_signing_key_serialization() {
        let request = LedgerRequest::RevokeSigningKey { kid: "kid-to-revoke".to_string() };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::RevokeSigningKey { kid } => {
                assert_eq!(kid, "kid-to-revoke");
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_transition_signing_key_revoked_serialization() {
        let request = LedgerRequest::TransitionSigningKeyRevoked { kid: "rotated-kid".to_string() };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::TransitionSigningKeyRevoked { kid } => {
                assert_eq!(kid, "rotated-kid");
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_create_refresh_token_serialization() {
        let request = LedgerRequest::CreateRefreshToken {
            token_hash: [0x11; 32],
            family: [0x22; 16],
            token_type: TokenType::UserSession,
            subject: TokenSubject::User(UserSlug::new(99)),
            organization: None,
            vault: None,
            kid: "signing-kid".to_string(),
            ttl_secs: 1_209_600,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::CreateRefreshToken {
                token_hash,
                family,
                token_type,
                subject,
                organization,
                vault,
                kid,
                ttl_secs,
            } => {
                assert_eq!(token_hash, [0x11; 32]);
                assert_eq!(family, [0x22; 16]);
                assert_eq!(token_type, TokenType::UserSession);
                assert_eq!(subject, TokenSubject::User(UserSlug::new(99)));
                assert!(organization.is_none());
                assert!(vault.is_none());
                assert_eq!(kid, "signing-kid");
                assert_eq!(ttl_secs, 1_209_600);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_create_refresh_token_vault_serialization() {
        let request = LedgerRequest::CreateRefreshToken {
            token_hash: [0xAA; 32],
            family: [0xBB; 16],
            token_type: TokenType::VaultAccess,
            subject: TokenSubject::App(AppSlug::new(55)),
            organization: Some(OrganizationId::new(7)),
            vault: Some(VaultId::new(3)),
            kid: "org-kid".to_string(),
            ttl_secs: 3600,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::CreateRefreshToken {
                token_type, subject, organization, vault, ..
            } => {
                assert_eq!(token_type, TokenType::VaultAccess);
                assert_eq!(subject, TokenSubject::App(AppSlug::new(55)));
                assert_eq!(organization, Some(OrganizationId::new(7)));
                assert_eq!(vault, Some(VaultId::new(3)));
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_use_refresh_token_serialization() {
        let request = LedgerRequest::UseRefreshToken {
            old_token_hash: [0x33; 32],
            new_token_hash: [0x44; 32],
            new_kid: "active-kid".to_string(),
            ttl_secs: 1_209_600,
            expected_version: Some(TokenVersion::new(5)),
            max_family_lifetime_secs: 2_592_000,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::UseRefreshToken {
                old_token_hash,
                new_token_hash,
                new_kid,
                ttl_secs,
                expected_version,
                max_family_lifetime_secs,
            } => {
                assert_eq!(old_token_hash, [0x33; 32]);
                assert_eq!(new_token_hash, [0x44; 32]);
                assert_eq!(new_kid, "active-kid");
                assert_eq!(ttl_secs, 1_209_600);
                assert_eq!(expected_version, Some(TokenVersion::new(5)));
                assert_eq!(max_family_lifetime_secs, 2_592_000);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_use_refresh_token_no_version_serialization() {
        let request = LedgerRequest::UseRefreshToken {
            old_token_hash: [0x55; 32],
            new_token_hash: [0x66; 32],
            new_kid: "vault-kid".to_string(),
            ttl_secs: 3600,
            expected_version: None,
            max_family_lifetime_secs: 2_592_000,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::UseRefreshToken { expected_version, .. } => {
                assert!(expected_version.is_none());
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_revoke_token_family_serialization() {
        let request = LedgerRequest::RevokeTokenFamily { family: [0xAB; 16] };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::RevokeTokenFamily { family } => {
                assert_eq!(family, [0xAB; 16]);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_revoke_all_user_sessions_serialization() {
        let request = LedgerRequest::RevokeAllUserSessions { user: UserId::new(42) };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::RevokeAllUserSessions { user } => {
                assert_eq!(user, UserId::new(42));
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_revoke_all_app_sessions_serialization() {
        let request = LedgerRequest::RevokeAllAppSessions {
            organization: OrganizationId::new(1),
            app: AppId::new(42),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::RevokeAllAppSessions { organization, app } => {
                assert_eq!(organization, OrganizationId::new(1));
                assert_eq!(app, AppId::new(42));
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_delete_expired_refresh_tokens_serialization() {
        let request = LedgerRequest::DeleteExpiredRefreshTokens;

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(request, deserialized);
    }

    // ── Response Variant Tests ──

    #[test]
    fn test_signing_key_created_response_serialization() {
        let response = LedgerResponse::SigningKeyCreated {
            id: SigningKeyId::new(1),
            kid: "test-kid".to_string(),
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "SigningKeyCreated(id=sigkey:1, kid=test-kid)");
    }

    #[test]
    fn test_signing_key_rotated_response_serialization() {
        let response = LedgerResponse::SigningKeyRotated {
            old_kid: "old".to_string(),
            new_kid: "new".to_string(),
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "SigningKeyRotated(old=old, new=new)");
    }

    #[test]
    fn test_signing_key_revoked_response_serialization() {
        let response = LedgerResponse::SigningKeyRevoked { kid: "revoked-kid".to_string() };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "SigningKeyRevoked(kid=revoked-kid)");
    }

    #[test]
    fn test_signing_key_transitioned_response_serialization() {
        let response =
            LedgerResponse::SigningKeyTransitioned { kid: "transitioned-kid".to_string() };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
    }

    #[test]
    fn test_refresh_token_created_response_serialization() {
        let response = LedgerResponse::RefreshTokenCreated { id: RefreshTokenId::new(99) };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "RefreshTokenCreated(id=rtoken:99)");
    }

    #[test]
    fn test_refresh_token_rotated_response_serialization() {
        let response = LedgerResponse::RefreshTokenRotated {
            new_id: RefreshTokenId::new(100),
            token_version: Some(TokenVersion::new(3)),
            allowed_scopes: None,
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "RefreshTokenRotated(new_id=rtoken:100)");
    }

    #[test]
    fn test_refresh_token_rotated_with_scopes_serialization() {
        let response = LedgerResponse::RefreshTokenRotated {
            new_id: RefreshTokenId::new(101),
            token_version: None,
            allowed_scopes: Some(vec!["vault:read".to_string(), "entity:write".to_string()]),
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
    }

    #[test]
    fn test_token_family_revoked_response_serialization() {
        let response = LedgerResponse::TokenFamilyRevoked { count: 5 };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "TokenFamilyRevoked(count=5)");
    }

    #[test]
    fn test_all_user_sessions_revoked_response_serialization() {
        let response =
            LedgerResponse::AllUserSessionsRevoked { count: 10, version: TokenVersion::new(2) };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "AllUserSessionsRevoked(count=10, version=v2)");
    }

    #[test]
    fn test_all_app_sessions_revoked_response_serialization() {
        let response =
            LedgerResponse::AllAppSessionsRevoked { count: 7, version: TokenVersion::new(3) };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "AllAppSessionsRevoked(count=7, version=v3)");
    }

    #[test]
    fn test_expired_refresh_tokens_deleted_response_serialization() {
        let response = LedgerResponse::ExpiredRefreshTokensDeleted { count: 42 };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "ExpiredRefreshTokensDeleted(count=42)");
    }

    // ============================================
    // User Credential SystemRequest serialization tests
    // ============================================

    #[test]
    fn test_create_user_credential_serialization() {
        let request = SystemRequest::CreateUserCredential {
            user_id: UserId::new(42),
            credential_type: CredentialType::Passkey,
            credential_data: CredentialData::Passkey(PasskeyCredential {
                credential_id: vec![1, 2, 3],
                public_key: vec![4, 5, 6],
                sign_count: 0,
                transports: vec!["internal".to_string()],
                backup_eligible: true,
                backup_state: false,
                attestation_format: Some("packed".to_string()),
                aaguid: Some([0xAA; 16]),
            }),
            name: "YubiKey 5".to_string(),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_create_user_credential_totp_serialization() {
        let request = SystemRequest::CreateUserCredential {
            user_id: UserId::new(7),
            credential_type: CredentialType::Totp,
            credential_data: CredentialData::Totp(inferadb_ledger_types::TotpCredential {
                secret: zeroize::Zeroizing::new(vec![0xBB; 20]),
                algorithm: inferadb_ledger_types::TotpAlgorithm::Sha1,
                digits: 6,
                period: 30,
            }),
            name: "Authenticator app".to_string(),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_create_user_credential_recovery_code_serialization() {
        let request = SystemRequest::CreateUserCredential {
            user_id: UserId::new(3),
            credential_type: CredentialType::RecoveryCode,
            credential_data: CredentialData::RecoveryCode(
                inferadb_ledger_types::RecoveryCodeCredential {
                    code_hashes: vec![[0xCC; 32], [0xDD; 32]],
                    total_generated: 10,
                },
            ),
            name: "Recovery codes".to_string(),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_update_user_credential_serialization() {
        let request = SystemRequest::UpdateUserCredential {
            user_id: UserId::new(42),
            credential_id: inferadb_ledger_types::UserCredentialId::new(5),
            name: Some("Renamed key".to_string()),
            enabled: Some(false),
            passkey_update: Some(PasskeyCredential {
                credential_id: vec![],
                public_key: vec![],
                sign_count: 100,
                transports: vec![],
                backup_eligible: false,
                backup_state: true,
                attestation_format: None,
                aaguid: None,
            }),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_delete_user_credential_serialization() {
        let request = SystemRequest::DeleteUserCredential {
            user_id: UserId::new(42),
            credential_id: inferadb_ledger_types::UserCredentialId::new(5),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_create_totp_challenge_serialization() {
        let request = SystemRequest::CreateTotpChallenge {
            user_id: UserId::new(42),
            user_slug: UserSlug::new(12345),
            nonce: [0xAA; 32],
            expires_at: chrono::Utc::now(),
            primary_method: PrimaryAuthMethod::EmailCode,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_consume_totp_and_create_session_serialization() {
        let request = SystemRequest::ConsumeTotpAndCreateSession {
            user_id: UserId::new(42),
            nonce: [0xBB; 32],
            token_hash: [0xCC; 32],
            family: [0xDD; 16],
            kid: "key-001".to_string(),
            ttl_secs: 3600,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_consume_recovery_and_create_session_serialization() {
        let request = SystemRequest::ConsumeRecoveryAndCreateSession {
            user_id: UserId::new(42),
            nonce: [0xBB; 32],
            code_hash: [0xEE; 32],
            credential_id: inferadb_ledger_types::UserCredentialId::new(3),
            token_hash: [0xCC; 32],
            family: [0xDD; 16],
            kid: "key-002".to_string(),
            ttl_secs: 7200,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_increment_totp_attempt_serialization() {
        let request =
            SystemRequest::IncrementTotpAttempt { user_id: UserId::new(42), nonce: [0xFF; 32] };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    // ============================================
    // User Credential LedgerResponse serialization tests
    // ============================================

    #[test]
    fn test_user_credential_created_response_serialization() {
        let response = LedgerResponse::UserCredentialCreated {
            credential_id: inferadb_ledger_types::UserCredentialId::new(7),
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "UserCredentialCreated(id=ucred:7)");
    }

    #[test]
    fn test_user_credential_updated_response_serialization() {
        let response = LedgerResponse::UserCredentialUpdated {
            credential_id: inferadb_ledger_types::UserCredentialId::new(7),
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "UserCredentialUpdated(id=ucred:7)");
    }

    #[test]
    fn test_user_credential_deleted_response_serialization() {
        let response = LedgerResponse::UserCredentialDeleted {
            credential_id: inferadb_ledger_types::UserCredentialId::new(7),
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "UserCredentialDeleted(id=ucred:7)");
    }

    #[test]
    fn test_totp_challenge_created_response_serialization() {
        let response = LedgerResponse::TotpChallengeCreated { nonce: [0xAA; 32] };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "TotpChallengeCreated");
    }

    #[test]
    fn test_totp_verified_response_serialization() {
        let response = LedgerResponse::TotpVerified { refresh_token_id: RefreshTokenId::new(99) };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "TotpVerified(refresh=rtoken:99)");
    }

    #[test]
    fn test_recovery_code_consumed_response_serialization() {
        let response = LedgerResponse::RecoveryCodeConsumed {
            refresh_token_id: RefreshTokenId::new(88),
            remaining_codes: 9,
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "RecoveryCodeConsumed(refresh=rtoken:88, remaining=9)");
    }

    #[test]
    fn test_totp_attempt_incremented_response_serialization() {
        let response = LedgerResponse::TotpAttemptIncremented { attempts: 2 };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "TotpAttemptIncremented(attempts=2)");
    }

    #[test]
    fn test_verify_email_code_with_totp_serialization() {
        let request = SystemRequest::VerifyEmailCode {
            email_hmac: "hmac123".to_string(),
            code_hash: [0xAA; 32],
            region: Region::US_EAST_VA,
            existing_user_hmac_hit: true,
            onboarding_token_hash: [0xBB; 32],
            onboarding_expires_at: chrono::Utc::now(),
            totp: Some(TotpPreResolve {
                nonce: [0xCC; 32],
                expires_at: chrono::Utc::now(),
                user_id: UserId::new(42),
                user_slug: UserSlug::new(12345),
            }),
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_verify_email_code_without_totp_serialization() {
        let request = SystemRequest::VerifyEmailCode {
            email_hmac: "hmac456".to_string(),
            code_hash: [0xDD; 32],
            region: Region::US_WEST_OR,
            existing_user_hmac_hit: false,
            onboarding_token_hash: [0xEE; 32],
            onboarding_expires_at: chrono::Utc::now(),
            totp: None,
        };

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_email_code_verified_totp_required_serialization() {
        let response = LedgerResponse::EmailCodeVerified {
            result: EmailCodeVerifiedResult::TotpRequired { nonce: [0xFF; 32] },
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert!(format!("{response}").contains("TotpRequired"));
    }

    #[test]
    fn test_email_code_verified_existing_user_serialization() {
        let response =
            LedgerResponse::EmailCodeVerified { result: EmailCodeVerifiedResult::ExistingUser };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert!(format!("{response}").contains("ExistingUser"));
    }

    #[test]
    fn test_verify_email_code_classification_regional() {
        let request = SystemRequest::VerifyEmailCode {
            email_hmac: "test".to_string(),
            code_hash: [0; 32],
            region: Region::US_EAST_VA,
            existing_user_hmac_hit: true,
            onboarding_token_hash: [0; 32],
            onboarding_expires_at: chrono::Utc::now(),
            totp: Some(TotpPreResolve {
                nonce: [0; 32],
                expires_at: chrono::Utc::now(),
                user_id: UserId::new(1),
                user_slug: UserSlug::new(1),
            }),
        };
        assert_eq!(classify_system_request(&request), RaftScope::Regional);
    }

    #[test]
    fn test_onboarding_cleaned_up_response_serialization() {
        let response = LedgerResponse::OnboardingCleanedUp {
            verification_codes_deleted: 3,
            onboarding_accounts_deleted: 1,
            totp_challenges_deleted: 5,
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(
            format!("{response}"),
            "OnboardingCleanedUp(codes=3, accounts=1, totp_challenges=5)"
        );
    }

    // ============================================
    // Property-based Raft log invariant tests
    // ============================================

    mod proptest_raft_log {
        use inferadb_ledger_types::{OrganizationId, UserId, VaultId, VaultSlug};
        use proptest::prelude::*;

        use crate::{
            log_storage::LogId,
            types::{LedgerRequest, SystemRequest},
        };

        /// Helper to create a LogId from term and index.
        fn make_log_id(term: u64, index: u64) -> LogId {
            LogId::new(term, 0, index)
        }

        /// Represents a Raft log entry with term and index.
        #[derive(Debug, Clone)]
        struct LogEntry {
            term: u64,
            index: u64,
        }

        /// Generates a valid Raft log sequence with monotonic indices and
        /// non-decreasing terms. Optionally includes term changes (leader elections).
        fn arb_valid_log(max_entries: usize) -> impl Strategy<Value = Vec<LogEntry>> {
            proptest::collection::vec(
                (1u64..100, prop::bool::ANY), // (term_increment, is_election)
                1..max_entries,
            )
            .prop_map(|decisions| {
                let mut entries = Vec::new();
                let mut current_term = 1u64;
                let mut current_index = 1u64;

                for (term_inc, is_election) in decisions {
                    if is_election {
                        current_term += term_inc;
                    }
                    entries.push(LogEntry { term: current_term, index: current_index });
                    current_index += 1;
                }
                entries
            })
        }

        proptest! {
            /// Logs indices must be strictly monotonic (sequential, no gaps).
            #[test]
            fn prop_log_indices_strictly_monotonic(log in arb_valid_log(200)) {
                for window in log.windows(2) {
                    prop_assert_eq!(
                        window[1].index,
                        window[0].index + 1,
                        "indices not sequential: {} -> {}",
                        window[0].index,
                        window[1].index
                    );
                }
            }

            /// Logs terms must be non-decreasing (can stay same or increase, never decrease).
            #[test]
            fn prop_log_terms_nondecreasing(log in arb_valid_log(200)) {
                for window in log.windows(2) {
                    prop_assert!(
                        window[1].term >= window[0].term,
                        "term decreased: {} -> {} at indices {}-{}",
                        window[0].term,
                        window[1].term,
                        window[0].index,
                        window[1].index
                    );
                }
            }

            /// LogId ordering: later entries have greater or equal LogId.
            /// This verifies that LogId ordering matches our expectations.
            #[test]
            fn prop_logid_ordering_consistent(log in arb_valid_log(200)) {
                let log_ids: Vec<LogId> = log
                    .iter()
                    .map(|e| make_log_id(e.term, e.index))
                    .collect();

                for window in log_ids.windows(2) {
                    prop_assert!(
                        window[1] >= window[0],
                        "LogId ordering violated: {:?} > {:?}",
                        window[0],
                        window[1]
                    );
                }
            }

            /// First entry always has index >= 1 (0 is reserved for initial state).
            #[test]
            fn prop_first_index_nonzero(log in arb_valid_log(50)) {
                if let Some(first) = log.first() {
                    prop_assert!(
                        first.index >= 1,
                        "first index should be >= 1, got {}",
                        first.index
                    );
                }
            }

            /// Term changes represent leader elections: within the same term,
            /// indices must be contiguous (no gaps within a term).
            #[test]
            fn prop_no_index_gaps_within_term(log in arb_valid_log(200)) {
                // Group consecutive entries by term
                let mut term_groups: Vec<Vec<u64>> = Vec::new();
                let mut current_term = 0u64;

                for entry in &log {
                    if entry.term != current_term {
                        term_groups.push(Vec::new());
                        current_term = entry.term;
                    }
                    if let Some(group) = term_groups.last_mut() {
                        group.push(entry.index);
                    }
                }

                // Within each term group, indices must be contiguous
                for group in &term_groups {
                    for window in group.windows(2) {
                        prop_assert_eq!(
                            window[1],
                            window[0] + 1,
                            "gap within term: indices {} -> {}",
                            window[0],
                            window[1]
                        );
                    }
                }
            }

            /// LedgerRequest serialization roundtrip preserves all variants.
            #[test]
            fn prop_ledger_request_roundtrip(
                variant_idx in 0u8..4,
                name in "[a-z]{1,16}",
                organization in (1i64..10_000).prop_map(OrganizationId::new),
                vault in (1i64..10_000).prop_map(VaultId::new),
                region_idx in 0usize..inferadb_ledger_types::ALL_REGIONS.len(),
            ) {
                let region = inferadb_ledger_types::ALL_REGIONS[region_idx];
                let request = match variant_idx {
                    0 => LedgerRequest::System(SystemRequest::CreateOrganization {
                        slug: inferadb_ledger_types::OrganizationSlug::new(42),
                        region,
                        tier: Default::default(),
                        admin: UserId::new(1),
                    }),
                    1 => LedgerRequest::CreateVault {
                        organization,
                        slug: VaultSlug::new(42),
                        name: Some(name.clone()),
                        retention_policy: None,
                    },
                    2 => LedgerRequest::DeleteOrganization { organization },
                    _ => LedgerRequest::DeleteVault { organization, vault },
                };

                let bytes = postcard::to_allocvec(&request).expect("serialize");
                let decoded: super::LedgerRequest =
                    postcard::from_bytes(&bytes).expect("deserialize");
                prop_assert_eq!(
                    postcard::to_allocvec(&decoded).expect("re-serialize"),
                    bytes,
                    "roundtrip changed encoding"
                );
            }
        }
    }

    /// Classifies a `SystemRequest` variant as GLOBAL or REGIONAL.
    ///
    /// REGIONAL variants carry plaintext PII and are proposed via
    /// `ServiceContext::propose_regional`. GLOBAL variants contain
    /// no PII and are proposed via `ServiceContext::propose_system_request`.
    ///
    /// Exists solely for the exhaustive match test
    /// below. When a new `SystemRequest` variant is added, the compiler
    /// will force the developer to classify it here — catching any
    /// accidental PII in GLOBAL requests at compile time.
    fn classify_system_request(req: &SystemRequest) -> RaftScope {
        match req {
            // GLOBAL variants — no plaintext PII
            SystemRequest::ActivateOnboardingUser { .. } => RaftScope::Global,
            SystemRequest::ClearRehashProgress { .. } => RaftScope::Global,
            SystemRequest::CreateDataRegion { .. } => RaftScope::Global,
            SystemRequest::RegisterPeerAddress { .. } => RaftScope::Global,
            SystemRequest::CreateOnboardingUser { .. } => RaftScope::Global,
            SystemRequest::CreateOrganization { .. } => RaftScope::Global,
            SystemRequest::CreateUser { .. } => RaftScope::Global,
            SystemRequest::DeleteUser { .. } => RaftScope::Global,
            SystemRequest::DeleteUserEmail { .. } => RaftScope::Global,
            SystemRequest::EraseUser { .. } => RaftScope::Global,
            SystemRequest::MigrateExistingUsers { .. } => RaftScope::Global,
            SystemRequest::RegisterEmailHash { .. } => RaftScope::Global,
            SystemRequest::RemoveEmailHash { .. } => RaftScope::Global,
            SystemRequest::SetNodeStatus { .. } => RaftScope::Global,
            SystemRequest::SetBlindingKeyVersion { .. } => RaftScope::Global,
            SystemRequest::UpdateOrganizationStatus { .. } => RaftScope::Global,
            SystemRequest::UpdateOrganizationRouting { .. } => RaftScope::Global,
            SystemRequest::UpdateRehashProgress { .. } => RaftScope::Global,
            SystemRequest::UpdateUser { .. } => RaftScope::Global,
            SystemRequest::UpdateUserDirectoryStatus { .. } => RaftScope::Global,
            SystemRequest::VerifyUserEmail { .. } => RaftScope::Global,
            SystemRequest::RegionMembershipReport { .. } => RaftScope::Global,

            // REGIONAL variants — carry plaintext PII
            SystemRequest::AddTeamMember { .. } => RaftScope::Regional,
            SystemRequest::CleanupExpiredOnboarding => RaftScope::Regional,
            SystemRequest::CreateEmailVerification { .. } => RaftScope::Regional,
            SystemRequest::CreateUserEmail { .. } => RaftScope::Regional,
            SystemRequest::DeleteAppProfile { .. } => RaftScope::Regional,
            SystemRequest::DeleteClientAssertionName { .. } => RaftScope::Regional,
            SystemRequest::DeleteTeam { .. } => RaftScope::Regional,
            SystemRequest::PurgeOrganizationRegional { .. } => RaftScope::Regional,
            SystemRequest::RemoveTeamMember { .. } => RaftScope::Regional,
            SystemRequest::UpdateTeamMemberRole { .. } => RaftScope::Regional,
            SystemRequest::UpdateOrganizationProfile { .. } => RaftScope::Regional,
            SystemRequest::UpdateUserProfile { .. } => RaftScope::Regional,
            SystemRequest::VerifyEmailCode { .. } => RaftScope::Regional,
            SystemRequest::WriteAppProfile { .. } => RaftScope::Regional,
            SystemRequest::WriteClientAssertionName { .. } => RaftScope::Regional,
            SystemRequest::WriteOnboardingUserProfile { .. } => RaftScope::Regional,
            SystemRequest::WriteOrganizationProfile { .. } => RaftScope::Regional,
            SystemRequest::WriteTeam { .. } => RaftScope::Regional,

            // REGIONAL — credential CRUD (encrypted via EncryptedUserSystemRequest)
            SystemRequest::CreateUserCredential { .. } => RaftScope::Regional,
            SystemRequest::UpdateUserCredential { .. } => RaftScope::Regional,
            SystemRequest::DeleteUserCredential { .. } => RaftScope::Regional,

            // REGIONAL — TOTP challenge lifecycle (plain, no PII, but REGIONAL scope)
            SystemRequest::CreateTotpChallenge { .. } => RaftScope::Regional,
            SystemRequest::ConsumeTotpAndCreateSession { .. } => RaftScope::Regional,
            SystemRequest::ConsumeRecoveryAndCreateSession { .. } => RaftScope::Regional,
            SystemRequest::IncrementTotpAttempt { .. } => RaftScope::Regional,

            // REGIONAL — invitation operations (encrypted via EncryptedOrgSystemRequest)
            SystemRequest::WriteOrganizationInvite { .. } => RaftScope::Regional,
            SystemRequest::UpdateOrganizationInviteStatus { .. } => RaftScope::Regional,
            SystemRequest::DeleteOrganizationInvite { .. } => RaftScope::Regional,
            SystemRequest::RehashInvitationEmailHmac { .. } => RaftScope::Regional,
        }
    }

    /// Verifies that the exhaustive classification covers all variants.
    /// If a new `SystemRequest` variant is added, this test fails at
    /// compile time until `classify_system_request` is updated.
    #[test]
    fn test_system_request_pii_classification_exhaustive() {
        // CreateUserEmail — REGIONAL (contains plaintext email)
        let regional_email = SystemRequest::CreateUserEmail {
            user_id: UserId::new(1),
            email: "user@example.com".to_string(),
        };
        assert_eq!(classify_system_request(&regional_email), RaftScope::Regional);

        // UpdateUserProfile — REGIONAL (contains plaintext name)
        let regional_profile =
            SystemRequest::UpdateUserProfile { user_id: UserId::new(1), name: "Alice".to_string() };
        assert_eq!(classify_system_request(&regional_profile), RaftScope::Regional);

        // EraseUser — GLOBAL (no PII, only user_id + region)
        let global_erase =
            SystemRequest::EraseUser { user_id: UserId::new(1), region: Region::US_EAST_VA };
        assert_eq!(classify_system_request(&global_erase), RaftScope::Global);

        // CreateUser — GLOBAL (no PII, only IDs + slug + region)
        let global_create = SystemRequest::CreateUser {
            user: UserId::new(1),
            admin: false,
            slug: UserSlug::new(100),
            region: Region::US_EAST_VA,
        };
        assert_eq!(classify_system_request(&global_create), RaftScope::Global);

        // CreateUserCredential — REGIONAL (encrypted, carries credential data)
        let regional_cred = SystemRequest::CreateUserCredential {
            user_id: UserId::new(1),
            credential_type: CredentialType::Passkey,
            credential_data: CredentialData::Passkey(PasskeyCredential {
                credential_id: vec![1, 2, 3],
                public_key: vec![4, 5, 6],
                sign_count: 0,
                transports: vec!["internal".to_string()],
                backup_eligible: false,
                backup_state: false,
                attestation_format: None,
                aaguid: None,
            }),
            name: "Test Key".to_string(),
        };
        assert_eq!(classify_system_request(&regional_cred), RaftScope::Regional);

        // CreateTotpChallenge — REGIONAL (plain, no PII, but REGIONAL scope)
        let regional_challenge = SystemRequest::CreateTotpChallenge {
            user_id: UserId::new(1),
            user_slug: UserSlug::new(100),
            nonce: [0xAA; 32],
            expires_at: chrono::Utc::now(),
            primary_method: PrimaryAuthMethod::EmailCode,
        };
        assert_eq!(classify_system_request(&regional_challenge), RaftScope::Regional);
    }

    /// Verifies that GLOBAL SystemRequest variants contain no String fields
    /// that could hold plaintext PII. String fields in GLOBAL variants must
    /// be cryptographic hashes (HMAC hex), network addresses, or storage
    /// keys — never user-facing text.
    ///
    /// This test documents every String field in GLOBAL variants and its
    /// purpose, serving as a human-readable audit trail.
    #[test]
    fn test_global_string_fields_are_not_pii() {
        // RegisterEmailHash::hmac_hex — cryptographic hash, not PII
        let register_hash = SystemRequest::RegisterEmailHash {
            hmac_hex: "abcdef1234567890".to_string(),
            user_id: UserId::new(1),
        };
        assert_eq!(classify_system_request(&register_hash), RaftScope::Global);

        // RemoveEmailHash::hmac_hex — cryptographic hash, not PII
        let remove_hash =
            SystemRequest::RemoveEmailHash { hmac_hex: "abcdef1234567890".to_string() };
        assert_eq!(classify_system_request(&remove_hash), RaftScope::Global);

        // WriteOrganizationProfile — carries sealed org name, classified as regional
        let write_profile = SystemRequest::WriteOrganizationProfile {
            organization: OrganizationId::new(1),
            sealed_name: vec![0; 48],
            name_nonce: [0; 12],
            shred_key_bytes: [0xAA; 32],
        };
        assert_eq!(classify_system_request(&write_profile), RaftScope::Regional);

        // UpdateOrganizationProfile — carries org name (PII), classified as regional
        let update_profile = SystemRequest::UpdateOrganizationProfile {
            organization: OrganizationId::new(1),
            name: "Updated Org".to_string(),
        };
        assert_eq!(classify_system_request(&update_profile), RaftScope::Regional);

        // WriteTeam — carries team name (PII), classified as regional
        let write_team = SystemRequest::WriteTeam {
            organization: OrganizationId::new(1),
            team: TeamId::new(1),
            slug: TeamSlug::new(100),
            name: "Engineering".to_string(),
        };
        assert_eq!(classify_system_request(&write_team), RaftScope::Regional);

        // WriteAppProfile — carries app name + description (PII), classified as regional
        let write_app = SystemRequest::WriteAppProfile {
            organization: OrganizationId::new(1),
            app: AppId::new(1),
            name: "My App".to_string(),
            description: Some("App description".to_string()),
        };
        assert_eq!(classify_system_request(&write_app), RaftScope::Regional);

        // DeleteTeam — REGIONAL cleanup (team record + name index)
        let delete_team = SystemRequest::DeleteTeam {
            organization: OrganizationId::new(1),
            team: TeamId::new(1),
            move_members_to: None,
        };
        assert_eq!(classify_system_request(&delete_team), RaftScope::Regional);

        // AddTeamMember — REGIONAL (member data is in team profile)
        let add_member = SystemRequest::AddTeamMember {
            organization: OrganizationId::new(1),
            team: TeamId::new(1),
            user_id: UserId::new(1),
            role: inferadb_ledger_state::system::TeamMemberRole::Member,
        };
        assert_eq!(classify_system_request(&add_member), RaftScope::Regional);

        // RemoveTeamMember — REGIONAL (member data is in team profile)
        let remove_member = SystemRequest::RemoveTeamMember {
            organization: OrganizationId::new(1),
            team: TeamId::new(1),
            user_id: UserId::new(1),
        };
        assert_eq!(classify_system_request(&remove_member), RaftScope::Regional);

        // UpdateTeamMemberRole — REGIONAL
        let update_role = SystemRequest::UpdateTeamMemberRole {
            organization: OrganizationId::new(1),
            team: TeamId::new(1),
            user_id: UserId::new(1),
            role: inferadb_ledger_state::system::TeamMemberRole::Manager,
        };
        assert_eq!(classify_system_request(&update_role), RaftScope::Regional);

        // WriteClientAssertionName — REGIONAL (assertion name is potential PII)
        let write_assertion_name = SystemRequest::WriteClientAssertionName {
            organization: OrganizationId::new(1),
            app: AppId::new(1),
            assertion: ClientAssertionId::new(1),
            name: "test".to_string(),
        };
        assert_eq!(classify_system_request(&write_assertion_name), RaftScope::Regional);

        // DeleteClientAssertionName — REGIONAL cleanup
        let delete_assertion_name = SystemRequest::DeleteClientAssertionName {
            organization: OrganizationId::new(1),
            app: AppId::new(1),
            assertion: ClientAssertionId::new(1),
        };
        assert_eq!(classify_system_request(&delete_assertion_name), RaftScope::Regional);

        // DeleteAppProfile — REGIONAL cleanup (profile + name index + assertion names)
        let delete_app_profile = SystemRequest::DeleteAppProfile {
            organization: OrganizationId::new(1),
            app: AppId::new(1),
        };
        assert_eq!(classify_system_request(&delete_app_profile), RaftScope::Regional);

        // PurgeOrganizationRegional — REGIONAL cleanup during org purge
        let purge_regional =
            SystemRequest::PurgeOrganizationRegional { organization: OrganizationId::new(1) };
        assert_eq!(classify_system_request(&purge_regional), RaftScope::Regional);
    }

    /// Classifies whether a `LedgerRequest` contains plaintext PII.
    ///
    /// All `LedgerRequest` variants are "global" — they contain only numeric
    /// IDs, slugs, hashes, and enums. Plaintext PII (names, descriptions)
    /// has been moved to [`SystemRequest`] regional variants:
    /// - Team names → [`SystemRequest::WriteTeam`]
    /// - App names/descriptions → [`SystemRequest::WriteAppProfile`]
    ///
    /// This exhaustive match ensures new variants are reviewed for PII before
    /// they can be added without a compile error.
    fn classify_ledger_request(req: &LedgerRequest) -> RaftScope {
        match req {
            LedgerRequest::Write { .. } => RaftScope::Global,
            LedgerRequest::CreateVault { .. } => RaftScope::Global,
            LedgerRequest::DeleteOrganization { .. } => RaftScope::Global,
            LedgerRequest::DeleteVault { .. } => RaftScope::Global,
            LedgerRequest::UpdateVault { .. } => RaftScope::Global,
            LedgerRequest::SuspendOrganization { .. } => RaftScope::Global,
            LedgerRequest::ResumeOrganization { .. } => RaftScope::Global,
            LedgerRequest::RemoveOrganizationMember { .. } => RaftScope::Global,
            LedgerRequest::UpdateOrganizationMemberRole { .. } => RaftScope::Global,
            LedgerRequest::AddOrganizationMember { .. } => RaftScope::Global,
            LedgerRequest::CreateOrganizationInvite { .. } => RaftScope::Global,
            LedgerRequest::ResolveOrganizationInvite { .. } => RaftScope::Global,
            LedgerRequest::PurgeOrganizationInviteIndexes { .. } => RaftScope::Global,
            LedgerRequest::RehashInviteEmailIndex { .. } => RaftScope::Global,
            LedgerRequest::PurgeOrganization { .. } => RaftScope::Global,
            LedgerRequest::StartMigration { .. } => RaftScope::Global,
            LedgerRequest::CompleteMigration { .. } => RaftScope::Global,
            LedgerRequest::UpdateVaultHealth { .. } => RaftScope::Global,
            LedgerRequest::BatchWrite { .. } => RaftScope::Global,
            LedgerRequest::CreateOrganizationTeam { .. } => RaftScope::Global,
            LedgerRequest::DeleteOrganizationTeam { .. } => RaftScope::Global,
            LedgerRequest::CreateApp { .. } => RaftScope::Global,
            LedgerRequest::DeleteApp { .. } => RaftScope::Global,
            LedgerRequest::SetAppEnabled { .. } => RaftScope::Global,
            LedgerRequest::SetAppCredentialEnabled { .. } => RaftScope::Global,
            LedgerRequest::RotateAppClientSecret { .. } => RaftScope::Global,
            LedgerRequest::CreateAppClientAssertion { .. } => RaftScope::Global,
            LedgerRequest::DeleteAppClientAssertion { .. } => RaftScope::Global,
            LedgerRequest::SetAppClientAssertionEnabled { .. } => RaftScope::Global,
            LedgerRequest::AddAppVault { .. } => RaftScope::Global,
            LedgerRequest::UpdateAppVault { .. } => RaftScope::Global,
            LedgerRequest::RemoveAppVault { .. } => RaftScope::Global,
            LedgerRequest::CreateSigningKey { .. } => RaftScope::Global,
            LedgerRequest::RotateSigningKey { .. } => RaftScope::Global,
            LedgerRequest::RevokeSigningKey { .. } => RaftScope::Global,
            LedgerRequest::TransitionSigningKeyRevoked { .. } => RaftScope::Global,
            LedgerRequest::CreateRefreshToken { .. } => RaftScope::Global,
            LedgerRequest::UseRefreshToken { .. } => RaftScope::Global,
            LedgerRequest::RevokeTokenFamily { .. } => RaftScope::Global,
            LedgerRequest::RevokeAllUserSessions { .. } => RaftScope::Global,
            LedgerRequest::RevokeAllAppSessions { .. } => RaftScope::Global,
            LedgerRequest::DeleteExpiredRefreshTokens => RaftScope::Global,
            LedgerRequest::System { .. } => RaftScope::Global,
            LedgerRequest::EncryptedUserSystem(_) => RaftScope::Regional,
            LedgerRequest::EncryptedOrgSystem(_) => RaftScope::Regional,
            // AddRegionLearner is intercepted by regional_proposal
            // before reaching the Raft log. It carries no PII.
            LedgerRequest::AddRegionLearner { .. } => RaftScope::Global,
        }
    }

    /// Verifies that `classify_ledger_request` is exhaustive — adding a new
    /// `LedgerRequest` variant will cause a compile error until classified.
    #[test]
    fn test_ledger_request_pii_classification_exhaustive() {
        let write = LedgerRequest::Write {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            transactions: vec![],
            idempotency_key: [0; 16],
            request_hash: 0,
        };
        assert_eq!(classify_ledger_request(&write), RaftScope::Global);

        let create_team = LedgerRequest::CreateOrganizationTeam {
            organization: OrganizationId::new(1),
            slug: TeamSlug::new(100),
        };
        assert_eq!(classify_ledger_request(&create_team), RaftScope::Global);

        let create_app = LedgerRequest::CreateApp {
            organization: OrganizationId::new(1),
            slug: AppSlug::new(200),
        };
        assert_eq!(classify_ledger_request(&create_app), RaftScope::Global);

        // EncryptedUserSystem — REGIONAL (contains encrypted user PII)
        let encrypted =
            LedgerRequest::EncryptedUserSystem(crate::entry_crypto::EncryptedUserSystemRequest {
                sealed: vec![0; 48],
                nonce: [0; 12],
                user_id: UserId::new(1),
            });
        assert_eq!(classify_ledger_request(&encrypted), RaftScope::Regional);

        // EncryptedOrgSystem — REGIONAL (contains encrypted org PII)
        let encrypted_org =
            LedgerRequest::EncryptedOrgSystem(crate::entry_crypto::EncryptedOrgSystemRequest {
                sealed: vec![0; 48],
                nonce: [0; 12],
                organization: OrganizationId::new(1),
            });
        assert_eq!(classify_ledger_request(&encrypted_org), RaftScope::Regional);
    }

    // ── Invitation Request/Response Serialization Tests ──

    #[test]
    fn test_create_organization_invite_serialization_roundtrip() {
        let request = LedgerRequest::CreateOrganizationInvite {
            organization: OrganizationId::new(5),
            slug: InviteSlug::new(9999),
            token_hash: [0xAB; 32],
            invitee_email_hmac: "deadbeef".to_string(),
            ttl_hours: 168,
        };
        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");
        match deserialized {
            LedgerRequest::CreateOrganizationInvite {
                organization,
                slug,
                token_hash,
                invitee_email_hmac,
                ttl_hours,
            } => {
                assert_eq!(organization, OrganizationId::new(5));
                assert_eq!(slug, InviteSlug::new(9999));
                assert_eq!(token_hash, [0xAB; 32]);
                assert_eq!(invitee_email_hmac, "deadbeef");
                assert_eq!(ttl_hours, 168);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_resolve_organization_invite_serialization_roundtrip() {
        let request = LedgerRequest::ResolveOrganizationInvite {
            invite: InviteId::new(42),
            organization: OrganizationId::new(5),
            status: InvitationStatus::Accepted,
            invitee_email_hmac: "cafe0123".to_string(),
            token_hash: [0xCD; 32],
        };
        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");
        match deserialized {
            LedgerRequest::ResolveOrganizationInvite {
                invite,
                organization,
                status,
                invitee_email_hmac,
                token_hash,
            } => {
                assert_eq!(invite, InviteId::new(42));
                assert_eq!(organization, OrganizationId::new(5));
                assert_eq!(status, InvitationStatus::Accepted);
                assert_eq!(invitee_email_hmac, "cafe0123");
                assert_eq!(token_hash, [0xCD; 32]);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_organization_invite_created_response_serialization_roundtrip() {
        use chrono::TimeZone;
        let expires = Utc.with_ymd_and_hms(2099, 6, 15, 12, 30, 0).unwrap();
        let response = LedgerResponse::OrganizationInviteCreated {
            invite_id: InviteId::new(10),
            invite_slug: InviteSlug::new(5000),
            expires_at: expires,
        };
        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        match deserialized {
            LedgerResponse::OrganizationInviteCreated { invite_id, invite_slug, expires_at } => {
                assert_eq!(invite_id, InviteId::new(10));
                assert_eq!(invite_slug, InviteSlug::new(5000));
                assert_eq!(expires_at, expires);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_organization_invite_resolved_response_serialization_roundtrip() {
        let response = LedgerResponse::OrganizationInviteResolved { invite_id: InviteId::new(42) };
        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        match deserialized {
            LedgerResponse::OrganizationInviteResolved { invite_id } => {
                assert_eq!(invite_id, InviteId::new(42));
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_write_organization_invite_serialization_roundtrip() {
        use chrono::TimeZone;
        let request = SystemRequest::WriteOrganizationInvite {
            organization: OrganizationId::new(5),
            invite: InviteId::new(10),
            slug: InviteSlug::new(9999),
            token_hash: [0xAB; 32],
            inviter: UserId::new(1),
            invitee_email_hmac: "deadbeef".to_string(),
            invitee_email: "alice@example.com".to_string(),
            role: inferadb_ledger_state::system::OrganizationMemberRole::Member,
            team: Some(TeamId::new(3)),
            expires_at: Utc.with_ymd_and_hms(2099, 6, 15, 12, 30, 0).unwrap(),
        };
        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_update_organization_invite_status_serialization_roundtrip() {
        let request = SystemRequest::UpdateOrganizationInviteStatus {
            organization: OrganizationId::new(5),
            invite: InviteId::new(10),
            status: InvitationStatus::Declined,
        };
        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_delete_organization_invite_serialization_roundtrip() {
        let request = SystemRequest::DeleteOrganizationInvite {
            organization: OrganizationId::new(5),
            invite: InviteId::new(10),
        };
        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_invitation_system_request_pii_classification() {
        // WriteOrganizationInvite — REGIONAL (contains plaintext email PII)
        let write = SystemRequest::WriteOrganizationInvite {
            organization: OrganizationId::new(1),
            invite: InviteId::new(1),
            slug: InviteSlug::new(100),
            token_hash: [0; 32],
            inviter: UserId::new(1),
            invitee_email_hmac: "hmac".to_string(),
            invitee_email: "alice@example.com".to_string(),
            role: inferadb_ledger_state::system::OrganizationMemberRole::Member,
            team: None,
            expires_at: Utc::now(),
        };
        assert_eq!(classify_system_request(&write), RaftScope::Regional);

        // UpdateOrganizationInviteStatus — REGIONAL
        let update = SystemRequest::UpdateOrganizationInviteStatus {
            organization: OrganizationId::new(1),
            invite: InviteId::new(1),
            status: InvitationStatus::Revoked,
        };
        assert_eq!(classify_system_request(&update), RaftScope::Regional);

        // DeleteOrganizationInvite — REGIONAL
        let delete = SystemRequest::DeleteOrganizationInvite {
            organization: OrganizationId::new(1),
            invite: InviteId::new(1),
        };
        assert_eq!(classify_system_request(&delete), RaftScope::Regional);
    }

    #[test]
    fn test_invitation_ledger_request_pii_classification() {
        // CreateOrganizationInvite — GLOBAL (no PII, only IDs and HMAC)
        let create = LedgerRequest::CreateOrganizationInvite {
            organization: OrganizationId::new(1),
            slug: InviteSlug::new(100),
            token_hash: [0; 32],
            invitee_email_hmac: "hmac".to_string(),
            ttl_hours: 168,
        };
        assert_eq!(classify_ledger_request(&create), RaftScope::Global);

        // ResolveOrganizationInvite — GLOBAL (no PII)
        let resolve = LedgerRequest::ResolveOrganizationInvite {
            invite: InviteId::new(1),
            organization: OrganizationId::new(1),
            status: InvitationStatus::Accepted,
            invitee_email_hmac: "hmac".to_string(),
            token_hash: [0; 32],
        };
        assert_eq!(classify_ledger_request(&resolve), RaftScope::Global);

        // PurgeOrganizationInviteIndexes — GLOBAL (no PII, only IDs and HMAC)
        let purge = LedgerRequest::PurgeOrganizationInviteIndexes {
            invite: InviteId::new(1),
            slug: InviteSlug::new(100),
            invitee_email_hmac: "hmac".to_string(),
            token_hash: [0; 32],
        };
        assert_eq!(classify_ledger_request(&purge), RaftScope::Global);

        // RehashInviteEmailIndex — GLOBAL (no PII, only IDs and HMACs)
        let rehash = LedgerRequest::RehashInviteEmailIndex {
            invite: InviteId::new(1),
            old_hmac: "old_hmac".to_string(),
            new_hmac: "new_hmac".to_string(),
            organization: OrganizationId::new(1),
            status: InvitationStatus::Pending,
        };
        assert_eq!(classify_ledger_request(&rehash), RaftScope::Global);
    }

    #[test]
    fn test_purge_organization_invite_indexes_serialization_roundtrip() {
        let mut test_hash = [0u8; 32];
        test_hash[0] = 0xAB;
        test_hash[31] = 0xCD;
        let request = LedgerRequest::PurgeOrganizationInviteIndexes {
            invite: InviteId::new(42),
            slug: InviteSlug::new(9999),
            invitee_email_hmac: "deadbeef".to_string(),
            token_hash: test_hash,
        };
        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");
        match deserialized {
            LedgerRequest::PurgeOrganizationInviteIndexes {
                invite,
                slug,
                invitee_email_hmac,
                token_hash,
            } => {
                assert_eq!(invite, InviteId::new(42));
                assert_eq!(slug, InviteSlug::new(9999));
                assert_eq!(invitee_email_hmac, "deadbeef");
                assert_eq!(token_hash, test_hash);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_rehash_invite_email_index_serialization_roundtrip() {
        let request = LedgerRequest::RehashInviteEmailIndex {
            invite: InviteId::new(7),
            old_hmac: "oldhmac123".to_string(),
            new_hmac: "newhmac456".to_string(),
            organization: OrganizationId::new(5),
            status: InvitationStatus::Pending,
        };
        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");
        match deserialized {
            LedgerRequest::RehashInviteEmailIndex {
                invite,
                old_hmac,
                new_hmac,
                organization,
                status,
            } => {
                assert_eq!(invite, InviteId::new(7));
                assert_eq!(old_hmac, "oldhmac123");
                assert_eq!(new_hmac, "newhmac456");
                assert_eq!(organization, OrganizationId::new(5));
                assert_eq!(status, InvitationStatus::Pending);
            },
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_organization_invite_indexes_purged_response_serialization_roundtrip() {
        let response =
            LedgerResponse::OrganizationInviteIndexesPurged { invite_id: InviteId::new(42) };
        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        match deserialized {
            LedgerResponse::OrganizationInviteIndexesPurged { invite_id } => {
                assert_eq!(invite_id, InviteId::new(42));
            },
            _ => panic!("unexpected variant"),
        }
        assert_eq!(format!("{response}"), "OrganizationInviteIndexesPurged(id=invite:42)");
    }

    #[test]
    fn test_invite_email_index_rehashed_response_serialization_roundtrip() {
        let response = LedgerResponse::InviteEmailIndexRehashed { invite_id: InviteId::new(7) };
        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        match deserialized {
            LedgerResponse::InviteEmailIndexRehashed { invite_id } => {
                assert_eq!(invite_id, InviteId::new(7));
            },
            _ => panic!("unexpected variant"),
        }
        assert_eq!(format!("{response}"), "InviteEmailIndexRehashed(id=invite:7)");
    }

    #[test]
    fn test_rehash_invitation_email_hmac_serialization_roundtrip() {
        let request = SystemRequest::RehashInvitationEmailHmac {
            organization: OrganizationId::new(5),
            invite: InviteId::new(10),
            new_hmac: "newhmac789".to_string(),
        };
        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: SystemRequest = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_rehash_invitation_email_hmac_pii_classification() {
        let request = SystemRequest::RehashInvitationEmailHmac {
            organization: OrganizationId::new(1),
            invite: InviteId::new(1),
            new_hmac: "hmac".to_string(),
        };
        assert_eq!(classify_system_request(&request), RaftScope::Regional);
    }

    // ============================================
    // Display coverage for all LedgerResponse variants
    // ============================================

    #[test]
    fn test_ledger_response_display_empty() {
        assert_eq!(format!("{}", LedgerResponse::Empty), "Empty");
    }

    #[test]
    fn test_ledger_response_display_organization_created() {
        let r = LedgerResponse::OrganizationCreated {
            organization_id: OrganizationId::new(1),
            organization_slug: OrganizationSlug::new(100),
        };
        assert!(format!("{r}").starts_with("OrganizationCreated("));
    }

    #[test]
    fn test_ledger_response_display_organization_profile_written() {
        let r =
            LedgerResponse::OrganizationProfileWritten { organization_id: OrganizationId::new(2) };
        assert_eq!(format!("{r}"), "OrganizationProfileWritten(id=org:2)");
    }

    #[test]
    fn test_ledger_response_display_organization_status_updated() {
        let r =
            LedgerResponse::OrganizationStatusUpdated { organization_id: OrganizationId::new(3) };
        assert_eq!(format!("{r}"), "OrganizationStatusUpdated(id=org:3)");
    }

    #[test]
    fn test_ledger_response_display_vault_created() {
        let r = LedgerResponse::VaultCreated { vault: VaultId::new(5), slug: VaultSlug::new(500) };
        assert!(format!("{r}").starts_with("VaultCreated("));
    }

    #[test]
    fn test_ledger_response_display_organization_deleted() {
        let r = LedgerResponse::OrganizationDeleted {
            organization_id: OrganizationId::new(4),
            deleted_at: chrono::Utc::now(),
            retention_days: 90,
        };
        assert!(format!("{r}").contains("retention_days=90"));
    }

    #[test]
    fn test_ledger_response_display_organization_updated() {
        let r = LedgerResponse::OrganizationUpdated { organization_id: OrganizationId::new(5) };
        assert_eq!(format!("{r}"), "OrganizationUpdated(id=org:5)");
    }

    #[test]
    fn test_ledger_response_display_organization_member_removed() {
        let r =
            LedgerResponse::OrganizationMemberRemoved { organization_id: OrganizationId::new(6) };
        assert_eq!(format!("{r}"), "OrganizationMemberRemoved(id=org:6)");
    }

    #[test]
    fn test_ledger_response_display_organization_member_role_updated() {
        let r = LedgerResponse::OrganizationMemberRoleUpdated {
            organization_id: OrganizationId::new(7),
        };
        assert_eq!(format!("{r}"), "OrganizationMemberRoleUpdated(id=org:7)");
    }

    #[test]
    fn test_ledger_response_display_organization_member_added() {
        let r = LedgerResponse::OrganizationMemberAdded {
            organization_id: OrganizationId::new(8),
            already_member: true,
        };
        assert!(format!("{r}").contains("already_member=true"));
    }

    #[test]
    fn test_ledger_response_display_invite_created() {
        let r = LedgerResponse::OrganizationInviteCreated {
            invite_id: InviteId::new(1),
            invite_slug: InviteSlug::new(100),
            expires_at: chrono::Utc::now(),
        };
        assert!(format!("{r}").starts_with("OrganizationInviteCreated("));
    }

    #[test]
    fn test_ledger_response_display_invite_resolved() {
        let r = LedgerResponse::OrganizationInviteResolved { invite_id: InviteId::new(2) };
        assert_eq!(format!("{r}"), "OrganizationInviteResolved(id=invite:2)");
    }

    #[test]
    fn test_ledger_response_display_invite_indexes_purged() {
        let r = LedgerResponse::OrganizationInviteIndexesPurged { invite_id: InviteId::new(3) };
        assert_eq!(format!("{r}"), "OrganizationInviteIndexesPurged(id=invite:3)");
    }

    #[test]
    fn test_ledger_response_display_invite_email_rehashed() {
        let r = LedgerResponse::InviteEmailIndexRehashed { invite_id: InviteId::new(4) };
        assert_eq!(format!("{r}"), "InviteEmailIndexRehashed(id=invite:4)");
    }

    #[test]
    fn test_ledger_response_display_organization_purged() {
        let r = LedgerResponse::OrganizationPurged { organization_id: OrganizationId::new(9) };
        assert_eq!(format!("{r}"), "OrganizationPurged(id=org:9)");
    }

    #[test]
    fn test_ledger_response_display_organization_migrated() {
        let r = LedgerResponse::OrganizationMigrated {
            organization: OrganizationId::new(10),
            old_region: Region::US_EAST_VA,
            new_region: Region::DE_CENTRAL_FRANKFURT,
        };
        assert!(format!("{r}").contains("OrganizationMigrated("));
    }

    #[test]
    fn test_ledger_response_display_organization_suspended() {
        let r = LedgerResponse::OrganizationSuspended { organization: OrganizationId::new(11) };
        assert_eq!(format!("{r}"), "OrganizationSuspended(id=org:11)");
    }

    #[test]
    fn test_ledger_response_display_organization_resumed() {
        let r = LedgerResponse::OrganizationResumed { organization: OrganizationId::new(12) };
        assert_eq!(format!("{r}"), "OrganizationResumed(id=org:12)");
    }

    #[test]
    fn test_ledger_response_display_migration_started() {
        let r = LedgerResponse::MigrationStarted {
            organization: OrganizationId::new(13),
            target_region_group: Region::JP_EAST_TOKYO,
        };
        assert!(format!("{r}").starts_with("MigrationStarted("));
    }

    #[test]
    fn test_ledger_response_display_migration_completed() {
        let r = LedgerResponse::MigrationCompleted {
            organization: OrganizationId::new(14),
            old_region: Region::US_EAST_VA,
            new_region: Region::US_WEST_OR,
        };
        assert!(format!("{r}").starts_with("MigrationCompleted("));
    }

    #[test]
    fn test_ledger_response_display_vault_deleted() {
        let r = LedgerResponse::VaultDeleted { success: true };
        assert_eq!(format!("{r}"), "VaultDeleted(success=true)");
    }

    #[test]
    fn test_ledger_response_display_vault_updated() {
        let r = LedgerResponse::VaultUpdated { success: false };
        assert_eq!(format!("{r}"), "VaultUpdated(success=false)");
    }

    #[test]
    fn test_ledger_response_display_vault_health_updated() {
        let r = LedgerResponse::VaultHealthUpdated { success: true };
        assert_eq!(format!("{r}"), "VaultHealthUpdated(success=true)");
    }

    #[test]
    fn test_ledger_response_display_user_created() {
        let r = LedgerResponse::UserCreated { user_id: UserId::new(1), slug: UserSlug::new(100) };
        assert!(format!("{r}").starts_with("UserCreated("));
    }

    #[test]
    fn test_ledger_response_display_user_updated() {
        let r = LedgerResponse::UserUpdated { user_id: UserId::new(2) };
        assert_eq!(format!("{r}"), "UserUpdated(id=user:2)");
    }

    #[test]
    fn test_ledger_response_display_user_soft_deleted() {
        let r = LedgerResponse::UserSoftDeleted { user_id: UserId::new(3), retention_days: 30 };
        assert!(format!("{r}").contains("retention_days=30"));
    }

    #[test]
    fn test_ledger_response_display_user_email_created() {
        let r = LedgerResponse::UserEmailCreated { email_id: UserEmailId::new(1) };
        assert_eq!(format!("{r}"), "UserEmailCreated(id=email:1)");
    }

    #[test]
    fn test_ledger_response_display_user_email_deleted() {
        let r = LedgerResponse::UserEmailDeleted { email_id: UserEmailId::new(2) };
        assert_eq!(format!("{r}"), "UserEmailDeleted(id=email:2)");
    }

    #[test]
    fn test_ledger_response_display_user_email_verified() {
        let r = LedgerResponse::UserEmailVerified { email_id: UserEmailId::new(3) };
        assert_eq!(format!("{r}"), "UserEmailVerified(id=email:3)");
    }

    #[test]
    fn test_ledger_response_display_user_erased() {
        let r = LedgerResponse::UserErased { user_id: UserId::new(4) };
        assert_eq!(format!("{r}"), "UserErased(id=user:4)");
    }

    #[test]
    fn test_ledger_response_display_users_migrated() {
        let r = LedgerResponse::UsersMigrated { users: 100, migrated: 90, skipped: 5, errors: 5 };
        assert!(format!("{r}").contains("migrated=90"));
    }

    #[test]
    fn test_ledger_response_display_error() {
        let r = LedgerResponse::Error {
            code: inferadb_ledger_types::ErrorCode::Internal,
            message: "something broke".to_string(),
        };
        assert!(format!("{r}").contains("something broke"));
    }

    #[test]
    fn test_ledger_response_display_precondition_failed() {
        let r = LedgerResponse::PreconditionFailed {
            key: "my_key".to_string(),
            current_version: Some(5),
            current_value: None,
            failed_condition: None,
        };
        assert_eq!(format!("{r}"), "PreconditionFailed(key=my_key)");
    }

    #[test]
    fn test_ledger_response_display_batch_write() {
        let r = LedgerResponse::BatchWrite {
            responses: vec![LedgerResponse::Empty, LedgerResponse::Empty],
        };
        assert_eq!(format!("{r}"), "BatchWrite(count=2)");
    }

    #[test]
    fn test_ledger_response_display_team_created() {
        let r = LedgerResponse::OrganizationTeamCreated {
            team_id: TeamId::new(1),
            team_slug: TeamSlug::new(100),
        };
        assert!(format!("{r}").starts_with("OrganizationTeamCreated("));
    }

    #[test]
    fn test_ledger_response_display_team_deleted() {
        let r =
            LedgerResponse::OrganizationTeamDeleted { organization_id: OrganizationId::new(15) };
        assert_eq!(format!("{r}"), "OrganizationTeamDeleted(org=org:15)");
    }

    #[test]
    fn test_ledger_response_display_app_created() {
        let r = LedgerResponse::AppCreated { app_id: AppId::new(1), app_slug: AppSlug::new(100) };
        assert!(format!("{r}").starts_with("AppCreated("));
    }

    #[test]
    fn test_ledger_response_display_app_deleted() {
        let r = LedgerResponse::AppDeleted { organization_id: OrganizationId::new(16) };
        assert_eq!(format!("{r}"), "AppDeleted(org=org:16)");
    }

    #[test]
    fn test_ledger_response_display_app_toggled() {
        let r = LedgerResponse::AppToggled { organization_id: OrganizationId::new(17) };
        assert_eq!(format!("{r}"), "AppToggled(org=org:17)");
    }

    #[test]
    fn test_ledger_response_display_app_credential_toggled() {
        let r = LedgerResponse::AppCredentialToggled { organization_id: OrganizationId::new(18) };
        assert_eq!(format!("{r}"), "AppCredentialToggled(org=org:18)");
    }

    #[test]
    fn test_ledger_response_display_app_client_secret_rotated() {
        let r = LedgerResponse::AppClientSecretRotated { organization_id: OrganizationId::new(19) };
        assert_eq!(format!("{r}"), "AppClientSecretRotated(org=org:19)");
    }

    #[test]
    fn test_ledger_response_display_app_client_assertion_created() {
        let r =
            LedgerResponse::AppClientAssertionCreated { assertion_id: ClientAssertionId::new(1) };
        assert!(format!("{r}").starts_with("AppClientAssertionCreated("));
    }

    #[test]
    fn test_ledger_response_display_app_client_assertion_deleted() {
        let r =
            LedgerResponse::AppClientAssertionDeleted { organization_id: OrganizationId::new(20) };
        assert_eq!(format!("{r}"), "AppClientAssertionDeleted(org=org:20)");
    }

    #[test]
    fn test_ledger_response_display_app_client_assertion_toggled() {
        let r =
            LedgerResponse::AppClientAssertionToggled { organization_id: OrganizationId::new(21) };
        assert_eq!(format!("{r}"), "AppClientAssertionToggled(org=org:21)");
    }

    #[test]
    fn test_ledger_response_display_app_vault_added() {
        let r = LedgerResponse::AppVaultAdded { organization_id: OrganizationId::new(22) };
        assert_eq!(format!("{r}"), "AppVaultAdded(org=org:22)");
    }

    #[test]
    fn test_ledger_response_display_app_vault_updated() {
        let r = LedgerResponse::AppVaultUpdated { organization_id: OrganizationId::new(23) };
        assert_eq!(format!("{r}"), "AppVaultUpdated(org=org:23)");
    }

    #[test]
    fn test_ledger_response_display_app_vault_removed() {
        let r = LedgerResponse::AppVaultRemoved { organization_id: OrganizationId::new(24) };
        assert_eq!(format!("{r}"), "AppVaultRemoved(org=org:24)");
    }

    #[test]
    fn test_ledger_response_display_email_verification_created() {
        let r = LedgerResponse::EmailVerificationCreated;
        assert_eq!(format!("{r}"), "EmailVerificationCreated");
    }

    #[test]
    fn test_ledger_response_display_email_code_verified_new_user() {
        let r = LedgerResponse::EmailCodeVerified { result: EmailCodeVerifiedResult::NewUser };
        assert!(format!("{r}").contains("NewUser"));
    }

    #[test]
    fn test_ledger_response_display_onboarding_user_created() {
        let r = LedgerResponse::OnboardingUserCreated {
            user_id: UserId::new(1),
            organization_id: OrganizationId::new(2),
        };
        assert!(format!("{r}").contains("OnboardingUserCreated("));
    }

    #[test]
    fn test_ledger_response_display_onboarding_user_profile_written() {
        let r = LedgerResponse::OnboardingUserProfileWritten {
            refresh_token_id: RefreshTokenId::new(10),
        };
        assert!(format!("{r}").contains("OnboardingUserProfileWritten("));
    }

    #[test]
    fn test_ledger_response_display_onboarding_user_activated() {
        let r = LedgerResponse::OnboardingUserActivated;
        assert_eq!(format!("{r}"), "OnboardingUserActivated");
    }

    #[test]
    fn test_ledger_response_display_user_profile_updated() {
        let r = LedgerResponse::UserProfileUpdated { user_id: UserId::new(5) };
        assert_eq!(format!("{r}"), "UserProfileUpdated(id=user:5)");
    }

    // ============================================
    // RaftPayload constructor tests
    // ============================================

    #[test]
    fn test_raft_payload_system_constructor() {
        let payload = RaftPayload::system(LedgerRequest::DeleteExpiredRefreshTokens);
        assert_eq!(payload.caller, 0);
        assert!(payload.state_root_commitments.is_empty());
        match payload.request {
            LedgerRequest::DeleteExpiredRefreshTokens => {},
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn test_raft_payload_new_constructor() {
        let payload = RaftPayload::new(
            LedgerRequest::DeleteOrganization { organization: OrganizationId::new(1) },
            42,
        );
        assert_eq!(payload.caller, 42);
        assert!(payload.state_root_commitments.is_empty());
    }

    #[test]
    fn test_raft_payload_with_commitments_constructor() {
        let commitments = vec![StateRootCommitment {
            organization: OrganizationId::new(1),
            vault: VaultId::new(1),
            vault_height: 10,
            state_root: [0xAA; 32],
        }];
        let payload = RaftPayload::with_commitments(
            LedgerRequest::DeleteExpiredRefreshTokens,
            commitments.clone(),
            99,
        );
        assert_eq!(payload.caller, 99);
        assert_eq!(payload.state_root_commitments.len(), 1);
    }

    #[test]
    fn test_ledger_response_default_is_empty() {
        let r = LedgerResponse::default();
        assert_eq!(r, LedgerResponse::Empty);
    }

    #[test]
    fn test_state_root_divergence_debug() {
        let d = StateRootDivergence {
            organization: OrganizationId::new(1),
            vault: VaultId::new(2),
            vault_height: 100,
            local_state_root: [0xAA; 32],
            leader_state_root: [0xBB; 32],
        };
        let debug = format!("{d:?}");
        assert!(debug.contains("StateRootDivergence"));
        assert!(debug.contains("vault_height: 100"));
    }
}
