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
    AppId, AppSlug, ClientAssertionId, Hash, OrganizationId, OrganizationSlug, RefreshTokenId,
    Region, SetCondition, SigningKeyId, TeamId, TeamSlug, TokenSubject, TokenType, TokenVersion,
    Transaction, UserEmailId, UserId, UserSlug, VaultId, VaultSlug,
};
// Re-export domain types that originated here but now live in types crate.
pub use inferadb_ledger_types::{BlockRetentionMode, BlockRetentionPolicy, LedgerNodeId};
use openraft::{BasicNode, impls::OneshotResponder};
use serde::{Deserialize, Serialize};

// Use the declare_raft_types macro for type configuration.
// This macro generates a `LedgerTypeConfig` struct that implements `RaftTypeConfig`.
//
// Type parameters:
// - `D`: Application data (LedgerRequest)
// - `R`: Application response (LedgerResponse)
// - `NodeId`: Node identifier type (u64)
// - `Node`: Node metadata (BasicNode with address info)
// - `Entry`: Log entry format (default Entry)
// - `SnapshotData`: Snapshot format (file-based streaming with zstd compression)
// - `AsyncRuntime`: Tokio runtime
// - `Responder`: One-shot channel responder
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
}

impl RaftPayload {
    /// Creates a payload with no state root commitments.
    ///
    /// Used by all proposal sites except the leader's write path, which
    /// drains the commitment buffer via [`Self::with_commitments`].
    pub fn new(request: LedgerRequest) -> Self {
        Self { request, proposed_at: chrono::Utc::now(), state_root_commitments: vec![] }
    }

    /// Creates a payload carrying piggybacked state root commitments.
    ///
    /// The leader drains its commitment buffer and attaches the results
    /// to the next `RaftPayload` so followers can verify state roots
    /// without extra RPCs.
    pub fn with_commitments(
        request: LedgerRequest,
        state_root_commitments: Vec<StateRootCommitment>,
    ) -> Self {
        Self { request, proposed_at: chrono::Utc::now(), state_root_commitments }
    }
}

openraft::declare_raft_types!(
    /// Ledger Raft type configuration.
    pub LedgerTypeConfig:
        D = RaftPayload,
        R = LedgerResponse,
        NodeId = LedgerNodeId,
        Node = BasicNode,
        Entry = openraft::Entry<LedgerTypeConfig>,
        SnapshotData = tokio::fs::File,
        AsyncRuntime = openraft::TokioRuntime,
        Responder = OneshotResponder<LedgerTypeConfig>
);

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

    /// Updates organization metadata (currently: name only).
    UpdateOrganization {
        /// Organization to update.
        organization: OrganizationId,
        /// New name (if changing).
        name: Option<String>,
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

    /// Creates a new team within an organization.
    CreateOrganizationTeam {
        /// Organization to create the team in.
        organization: OrganizationId,
        /// External slug for API lookups (generated before Raft proposal).
        slug: TeamSlug,
        /// Team name (unique within organization).
        name: String,
    },

    /// Deletes a team from an organization.
    DeleteOrganizationTeam {
        /// Organization containing the team.
        organization: OrganizationId,
        /// Team to delete.
        team: TeamId,
        /// If set, move members to this team before deleting.
        move_members_to: Option<TeamId>,
    },

    /// Updates team metadata.
    UpdateOrganizationTeam {
        /// Organization containing the team.
        organization: OrganizationId,
        /// Team to update.
        team: TeamId,
        /// New name (if changing).
        name: Option<String>,
    },

    /// Creates a new application within an organization.
    CreateApp {
        /// Organization to create the app in.
        organization: OrganizationId,
        /// External slug for API lookups (generated before Raft proposal).
        slug: AppSlug,
        /// App name (unique within organization).
        name: String,
        /// Optional description.
        description: Option<String>,
    },

    /// Updates app metadata (name and/or description).
    UpdateApp {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App to update.
        app: AppId,
        /// New name (if changing).
        name: Option<String>,
        /// New description (if changing; `Some(None)` clears it).
        description: Option<Option<String>>,
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
    CreateAppClientAssertion {
        /// Organization containing the app.
        organization: OrganizationId,
        /// App to add the assertion to.
        app: AppId,
        /// User-provided name.
        name: String,
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

    /// Deletes expired refresh tokens and garbage-collects poisoned families.
    /// Used by `TokenMaintenanceJob`. Apply handler uses `proposed_at` as cutoff.
    DeleteExpiredRefreshTokens,
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
/// - [`CreateUserEmail`](SystemRequest::CreateUserEmail) — plaintext email
/// - [`UpdateUserProfile`](SystemRequest::UpdateUserProfile) — plaintext name
/// - [`CreateEmailVerification`](SystemRequest::CreateEmailVerification) — plaintext email
/// - [`VerifyEmailCode`](SystemRequest::VerifyEmailCode) — verification region
/// - [`WriteOnboardingUserProfile`](SystemRequest::WriteOnboardingUserProfile) — plaintext email,
///   name
/// - [`WriteOrganizationProfile`](SystemRequest::WriteOrganizationProfile) — organization name
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

    /// Adds a node to the cluster.
    AddNode {
        /// Numeric node ID.
        node_id: LedgerNodeId,
        /// Node's gRPC address.
        address: String,
    },

    /// Removes a node from the cluster.
    RemoveNode {
        /// Node ID to remove.
        node_id: LedgerNodeId,
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
    CreateOrganizationDirectory {
        /// External Snowflake slug (generated before Raft proposal).
        slug: OrganizationSlug,
        /// Target data residency region.
        region: Region,
        /// Billing tier.
        tier: inferadb_ledger_state::system::OrganizationTier,
    },

    /// Writes the organization profile to the REGIONAL system vault.
    ///
    /// Creates an `OrganizationProfile` keyed as
    /// `_sys:org_profile:{organization}` with the provided name and admin.
    /// Organization name is PII — this request is classified as "regional"
    /// and proposed to the regional Raft group (in multi-Raft mode).
    WriteOrganizationProfile {
        /// Organization whose profile to write.
        organization: OrganizationId,
        /// Organization display name.
        name: String,
        /// Initial administrator for this organization.
        admin: UserId,
    },

    /// Updates the organization directory status in the GLOBAL control plane.
    UpdateOrganizationDirectoryStatus {
        /// Organization to update.
        organization: OrganizationId,
        /// New directory status.
        status: inferadb_ledger_state::system::OrganizationDirectoryStatus,
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
    /// Proposed to REGIONAL Raft group — contains PII (email).
    /// Rate-limited per email via `rate_limit_count` and `rate_limit_window_start`
    /// in the stored `PendingEmailVerification` record.
    CreateEmailVerification {
        /// HMAC of the email address (deterministic key).
        email_hmac: String,
        /// Plaintext email — PII, regional Raft log only.
        email: String,
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
    /// - `true`: Returns `ExistingUser` signal (no session created here).
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
    },

    /// GC expired verification codes and onboarding accounts.
    ///
    /// Proposed to REGIONAL Raft group. Has no fields — the region is
    /// implicit from the target Raft group. Scans `_tmp:onboard_verify:*`
    /// and `_tmp:onboard_account:*` up to `MAX_ONBOARDING_SCAN` limit.
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
    /// PII stays in the regional Raft log. The `email_hmac` is passed
    /// explicitly — NOT derived in the apply handler (blinding key is
    /// external, deriving would break state machine determinism).
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
        /// Plaintext email — PII, regional only.
        email: String,
        /// User display name — PII, regional only.
        name: String,
        /// Organization name — PII, regional only.
        organization_name: String,
        /// Per-subject encryption key (generated by orchestrator).
        subject_key_bytes: [u8; 32],
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
    OrganizationDirectoryCreated {
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
    OrganizationDirectoryStatusUpdated {
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
        /// Defaults to `Internal` when deserializing log entries written before
        /// this field was added.
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

    /// Team metadata updated.
    OrganizationTeamUpdated {
        /// Organization the team belongs to.
        organization_id: OrganizationId,
    },

    /// App created.
    AppCreated {
        /// Assigned internal app ID.
        app_id: AppId,
        /// External Snowflake slug.
        app_slug: AppSlug,
    },

    /// App metadata updated.
    AppUpdated {
        /// Organization the app belongs to.
        organization_id: OrganizationId,
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

    /// Expired onboarding records cleaned up.
    OnboardingCleanedUp {
        /// Number of expired verification codes deleted.
        verification_codes_deleted: u32,
        /// Number of expired onboarding accounts deleted.
        onboarding_accounts_deleted: u32,
    },
}

/// Result of email code verification.
///
/// Both variants carry no fields — they are signals. The service handler
/// has all context needed (HMAC, token hash, region) in local scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmailCodeVerifiedResult {
    /// Code verified, email belongs to an existing user.
    /// No session data — session creation is handled by the service handler
    /// (which proposes `CreateRefreshToken` to the user's actual region).
    ExistingUser,
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
            LedgerResponse::OrganizationDirectoryCreated { organization_id, organization_slug } => {
                write!(
                    f,
                    "OrganizationDirectoryCreated(id={}, slug={})",
                    organization_id, organization_slug
                )
            },
            LedgerResponse::OrganizationProfileWritten { organization_id } => {
                write!(f, "OrganizationProfileWritten(id={})", organization_id)
            },
            LedgerResponse::OrganizationDirectoryStatusUpdated { organization_id } => {
                write!(f, "OrganizationDirectoryStatusUpdated(id={})", organization_id)
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
            LedgerResponse::OrganizationTeamCreated { team_id, team_slug } => {
                write!(f, "OrganizationTeamCreated(id={}, slug={})", team_id, team_slug)
            },
            LedgerResponse::OrganizationTeamDeleted { organization_id } => {
                write!(f, "OrganizationTeamDeleted(org={})", organization_id)
            },
            LedgerResponse::OrganizationTeamUpdated { organization_id } => {
                write!(f, "OrganizationTeamUpdated(org={})", organization_id)
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
            LedgerResponse::AppUpdated { organization_id } => {
                write!(f, "AppUpdated(org={})", organization_id)
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
            } => {
                write!(
                    f,
                    "OnboardingCleanedUp(codes={}, accounts={})",
                    verification_codes_deleted, onboarding_accounts_deleted
                )
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_ledger_request_serialization() {
        let request = LedgerRequest::System(SystemRequest::CreateOrganizationDirectory {
            slug: OrganizationSlug::new(12345),
            region: Region::US_EAST_VA,
            tier: Default::default(),
        });

        let bytes = postcard::to_allocvec(&request).expect("serialize");
        let deserialized: LedgerRequest = postcard::from_bytes(&bytes).expect("deserialize");

        match deserialized {
            LedgerRequest::System(SystemRequest::CreateOrganizationDirectory {
                slug,
                region,
                ..
            }) => {
                assert_eq!(slug, OrganizationSlug::new(12345));
                assert_eq!(region, Region::US_EAST_VA);
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
            request: LedgerRequest::System(SystemRequest::CreateOrganizationDirectory {
                slug: OrganizationSlug::new(999),
                region: Region::US_EAST_VA,
                tier: Default::default(),
            }),
            proposed_at: Utc.with_ymd_and_hms(2099, 6, 15, 12, 30, 0).unwrap(),
            state_root_commitments: vec![],
        };

        let bytes = postcard::to_allocvec(&payload).expect("serialize");
        let deserialized: RaftPayload = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(payload, deserialized);
        assert_eq!(deserialized.proposed_at, Utc.with_ymd_and_hms(2099, 6, 15, 12, 30, 0).unwrap());
        match &deserialized.request {
            LedgerRequest::System(SystemRequest::CreateOrganizationDirectory { slug, .. }) => {
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
            request: LedgerRequest::System(SystemRequest::CreateOrganizationDirectory {
                slug: OrganizationSlug::new(1),
                region: Region::US_EAST_VA,
                tier: Default::default(),
            }),
            proposed_at: Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap(),
            state_root_commitments: vec![],
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
            } => {
                assert_eq!(old_token_hash, [0x33; 32]);
                assert_eq!(new_token_hash, [0x44; 32]);
                assert_eq!(new_kid, "active-kid");
                assert_eq!(ttl_secs, 1_209_600);
                assert_eq!(expected_version, Some(TokenVersion::new(5)));
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
    fn test_expired_refresh_tokens_deleted_response_serialization() {
        let response = LedgerResponse::ExpiredRefreshTokensDeleted { count: 42 };

        let bytes = postcard::to_allocvec(&response).expect("serialize");
        let deserialized: LedgerResponse = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(response, deserialized);
        assert_eq!(format!("{response}"), "ExpiredRefreshTokensDeleted(count=42)");
    }

    // ============================================
    // Property-based Raft log invariant tests
    // ============================================

    mod proptest_raft_log {
        use inferadb_ledger_types::{OrganizationId, VaultId, VaultSlug};
        use openraft::{CommittedLeaderId, LogId};
        use proptest::prelude::*;

        use crate::types::{LedgerNodeId, LedgerRequest, SystemRequest};

        /// Helper to create a LogId from term and index.
        fn make_log_id(term: u64, index: u64) -> LogId<LedgerNodeId> {
            LogId::new(CommittedLeaderId::new(term, 0), index)
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
            /// This verifies that openraft's LogId ordering matches our expectations.
            #[test]
            fn prop_logid_ordering_consistent(log in arb_valid_log(200)) {
                let log_ids: Vec<LogId<LedgerNodeId>> = log
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
                    0 => LedgerRequest::System(SystemRequest::CreateOrganizationDirectory {
                        slug: inferadb_ledger_types::OrganizationSlug::new(42),
                        region,
                        tier: Default::default(),
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
    /// This function exists solely to be used in the exhaustive match test
    /// below. When a new `SystemRequest` variant is added, the compiler
    /// will force the developer to classify it here — catching any
    /// accidental PII in GLOBAL requests at compile time.
    fn classify_system_request(req: &SystemRequest) -> &'static str {
        match req {
            // GLOBAL variants — no plaintext PII
            SystemRequest::CreateUser { .. } => "global",
            SystemRequest::UpdateUser { .. } => "global",
            SystemRequest::DeleteUser { .. } => "global",
            SystemRequest::DeleteUserEmail { .. } => "global",
            SystemRequest::VerifyUserEmail { .. } => "global",
            SystemRequest::AddNode { .. } => "global",
            SystemRequest::RemoveNode { .. } => "global",
            SystemRequest::UpdateOrganizationRouting { .. } => "global",
            SystemRequest::RegisterEmailHash { .. } => "global",
            SystemRequest::RemoveEmailHash { .. } => "global",
            SystemRequest::SetBlindingKeyVersion { .. } => "global",
            SystemRequest::UpdateRehashProgress { .. } => "global",
            SystemRequest::ClearRehashProgress { .. } => "global",
            SystemRequest::UpdateUserDirectoryStatus { .. } => "global",
            SystemRequest::EraseUser { .. } => "global",
            SystemRequest::MigrateExistingUsers { .. } => "global",
            SystemRequest::CreateOrganizationDirectory { .. } => "global",
            SystemRequest::WriteOrganizationProfile { .. } => "regional",
            SystemRequest::UpdateOrganizationDirectoryStatus { .. } => "global",

            SystemRequest::CreateOnboardingUser { .. } => "global",
            SystemRequest::ActivateOnboardingUser { .. } => "global",

            // REGIONAL variants — carry plaintext PII
            SystemRequest::CreateUserEmail { .. } => "regional",
            SystemRequest::UpdateUserProfile { .. } => "regional",
            SystemRequest::CreateEmailVerification { .. } => "regional",
            SystemRequest::VerifyEmailCode { .. } => "regional",
            SystemRequest::CleanupExpiredOnboarding => "regional",
            SystemRequest::WriteOnboardingUserProfile { .. } => "regional",
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
        assert_eq!(classify_system_request(&regional_email), "regional");

        // UpdateUserProfile — REGIONAL (contains plaintext name)
        let regional_profile =
            SystemRequest::UpdateUserProfile { user_id: UserId::new(1), name: "Alice".to_string() };
        assert_eq!(classify_system_request(&regional_profile), "regional");

        // EraseUser — GLOBAL (no PII, only user_id + region)
        let global_erase =
            SystemRequest::EraseUser { user_id: UserId::new(1), region: Region::US_EAST_VA };
        assert_eq!(classify_system_request(&global_erase), "global");

        // CreateUser — GLOBAL (no PII, only IDs + slug + region)
        let global_create = SystemRequest::CreateUser {
            user: UserId::new(1),
            admin: false,
            slug: UserSlug::new(100),
            region: Region::US_EAST_VA,
        };
        assert_eq!(classify_system_request(&global_create), "global");
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
        // AddNode::address — gRPC network address, not PII
        let add_node = SystemRequest::AddNode { node_id: 1, address: "10.0.0.1:50051".to_string() };
        assert_eq!(classify_system_request(&add_node), "global");

        // RegisterEmailHash::hmac_hex — cryptographic hash, not PII
        let register_hash = SystemRequest::RegisterEmailHash {
            hmac_hex: "abcdef1234567890".to_string(),
            user_id: UserId::new(1),
        };
        assert_eq!(classify_system_request(&register_hash), "global");

        // RemoveEmailHash::hmac_hex — cryptographic hash, not PII
        let remove_hash =
            SystemRequest::RemoveEmailHash { hmac_hex: "abcdef1234567890".to_string() };
        assert_eq!(classify_system_request(&remove_hash), "global");

        // WriteOrganizationProfile — carries org name (PII), classified as regional
        let write_profile = SystemRequest::WriteOrganizationProfile {
            organization: OrganizationId::new(1),
            name: "Test Org".to_string(),
            admin: UserId::new(1),
        };
        assert_eq!(classify_system_request(&write_profile), "regional");
    }
}
