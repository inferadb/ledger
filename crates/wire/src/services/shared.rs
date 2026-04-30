//! Cross-service wire types referenced by multiple service modules.
//!
//! Includes:
//!
//! - Re-exports of slug newtypes from `inferadb_ledger_types`. They already derive
//!   `Serialize`/`Deserialize` via the `define_slug!` macro, so they travel over the wire
//!   unchanged.
//! - Domain types referenced from multiple services (`Hash`, `BlockHeader`, `Block`,
//!   `BlockAnnouncement`, `Transaction`, `Operation` and its variants, Merkle / state proofs,
//!   `TokenPair`, etc.).
//! - Enums that span service boundaries (`OrganizationStatus`, `OrganizationTier`, `VaultStatus`,
//!   `UserStatus`, `UserRole`, etc.).
//!
//! Note on error metadata: proto's `ErrorDetails` is superseded on the wire
//! by [`crate::error::WireError`]. Service modules return `WireError` for
//! error frames; there is no `ErrorDetails` re-export here.

use std::collections::BTreeMap;

use bytes::Bytes;
// ---------------------------------------------------------------------------
// ID / slug re-exports
// ---------------------------------------------------------------------------
pub use inferadb_ledger_types::{
    AppId, AppSlug, ClientAssertionId, ClientId, EmailVerifyTokenId, InviteId, InviteSlug,
    LedgerNodeId, NodeId, OrganizationId, OrganizationSlug, RefreshTokenId, SigningKeyId, TeamId,
    TeamSlug, TokenVersion, TxId, UserCredentialId, UserEmailId, UserId, UserSlug, VaultId,
    VaultSlug,
};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Hash
// ---------------------------------------------------------------------------

/// 256-bit hash (SHA-256). Mirrors proto `Hash`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Hash {
    /// 32 bytes.
    pub value: Bytes,
}

// ---------------------------------------------------------------------------
// Block / Transaction types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockHeader {
    pub height: u64,
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub previous_hash: Option<Hash>,
    pub tx_merkle_root: Option<Hash>,
    pub state_root: Option<Hash>,
    /// UNIX nanoseconds.
    pub timestamp: u64,
    pub leader_id: Option<NodeId>,
    pub term: u64,
    pub committed_index: u64,
    pub block_hash: Option<Hash>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Block {
    pub header: Option<BlockHeader>,
    pub transactions: Vec<Transaction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockAnnouncement {
    pub organization: Option<OrganizationSlug>,
    pub vault: Option<VaultSlug>,
    pub height: u64,
    pub block_hash: Option<Hash>,
    pub state_root: Option<Hash>,
    /// UNIX nanoseconds.
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    pub id: Option<TxIdMessage>,
    pub client_id: Option<ClientIdMessage>,
    pub sequence: u64,
    pub operations: Vec<Operation>,
    /// UNIX nanoseconds.
    pub timestamp: u64,
}

/// Wire wrapper around the 16-byte transaction id. Mirrors proto `TxId`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TxIdMessage {
    /// UUID as 16 bytes.
    pub id: Bytes,
}

/// Wire wrapper around `ClientId`. Mirrors proto `ClientId` (separate type
/// so the proto field name `client_id` retains its message shape; the
/// re-exported `inferadb_ledger_types::ClientId` is used in domain code).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientIdMessage {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Operation {
    pub op: Option<OperationKind>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OperationKind {
    CreateRelationship(CreateRelationship),
    DeleteRelationship(DeleteRelationship),
    SetEntity(SetEntity),
    DeleteEntity(DeleteEntity),
    /// GC-initiated removal (distinct from user delete).
    ExpireEntity(ExpireEntity),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateRelationship {
    pub resource: String,
    pub relation: String,
    pub subject: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteRelationship {
    pub resource: String,
    pub relation: String,
    pub subject: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetEntity {
    pub key: String,
    pub value: Bytes,
    pub expires_at: Option<u64>,
    pub condition: Option<SetCondition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetCondition {
    pub condition: Option<SetConditionKind>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SetConditionKind {
    NotExists(bool),
    Version(u64),
    ValueEquals(Bytes),
    MustExists(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteEntity {
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExpireEntity {
    pub key: String,
    pub expired_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Relationship {
    pub resource: String,
    pub relation: String,
    pub subject: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    pub key: String,
    pub value: Bytes,
    pub expires_at: Option<u64>,
    pub version: u64,
}

// ---------------------------------------------------------------------------
// Read consistency / Direction
// ---------------------------------------------------------------------------

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Direction {
    Unspecified = 0,
    Left = 1,
    Right = 2,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReadConsistency {
    Unspecified = 0,
    Eventual = 1,
    Linearizable = 2,
}

// ---------------------------------------------------------------------------
// Merkle / state proofs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MerkleProof {
    pub leaf_hash: Option<Hash>,
    pub siblings: Vec<MerkleSibling>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MerkleSibling {
    pub hash: Option<Hash>,
    pub direction: Direction,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChainProof {
    pub headers: Vec<BlockHeader>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateProof {
    pub key: Bytes,
    pub value: Bytes,
    pub expires_at: u64,
    pub version: u64,
    pub bucket_id: u32,
    pub bucket_root: Option<Hash>,
    pub other_bucket_roots: Vec<Hash>,
    pub block_height: u64,
    pub state_root: Option<Hash>,
}

// ---------------------------------------------------------------------------
// User domain
// ---------------------------------------------------------------------------

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UserStatus {
    Unspecified = 0,
    Active = 1,
    PendingOrg = 2,
    Suspended = 3,
    Deleting = 4,
    Deleted = 5,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UserRole {
    Unspecified = 0,
    User = 1,
    Admin = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct User {
    pub id: Option<UserId>,
    pub name: String,
    pub email: Option<UserEmailId>,
    pub status: UserStatus,
    /// UNIX nanoseconds.
    pub created_at: u64,
    /// UNIX nanoseconds.
    pub updated_at: u64,
    pub role: UserRole,
    pub slug: Option<UserSlug>,
    /// UNIX nanoseconds (None = not soft-deleted).
    pub deleted_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserEmail {
    pub id: Option<UserEmailId>,
    pub user: Option<UserId>,
    pub email: String,
    /// UNIX nanoseconds.
    pub created_at: u64,
    /// UNIX nanoseconds (0 if unverified).
    pub verified_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EmailVerificationToken {
    pub id: Option<EmailVerifyTokenId>,
    pub user_email_id: Option<UserEmailId>,
    pub token: String,
    /// UNIX nanoseconds.
    pub created_at: u64,
    /// UNIX nanoseconds.
    pub expires_at: u64,
    /// UNIX nanoseconds (0 if unused).
    pub used_at: u64,
}

// ---------------------------------------------------------------------------
// Organization domain
// ---------------------------------------------------------------------------

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrganizationStatus {
    Unspecified = 0,
    Active = 1,
    Migrating = 2,
    Suspended = 3,
    Deleted = 4,
    Provisioning = 5,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrganizationTier {
    Unspecified = 0,
    Free = 1,
    Launch = 2,
    Scale = 3,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrganizationMemberRole {
    Unspecified = 0,
    Admin = 1,
    Member = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrganizationMember {
    pub user: Option<UserSlug>,
    pub role: OrganizationMemberRole,
    /// UNIX nanoseconds.
    pub joined_at: u64,
}

// ---------------------------------------------------------------------------
// Team domain
// ---------------------------------------------------------------------------

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrganizationTeamMemberRole {
    Unspecified = 0,
    Manager = 1,
    Member = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrganizationTeamMember {
    pub user: Option<UserSlug>,
    pub role: OrganizationTeamMemberRole,
    /// UNIX nanoseconds.
    pub joined_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrganizationTeam {
    pub slug: Option<TeamSlug>,
    pub organization: Option<OrganizationSlug>,
    pub name: String,
    pub members: Vec<OrganizationTeamMember>,
    /// UNIX nanoseconds.
    pub created_at: u64,
    /// UNIX nanoseconds.
    pub updated_at: u64,
}

// ---------------------------------------------------------------------------
// Vault domain
// ---------------------------------------------------------------------------

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VaultStatus {
    Unspecified = 0,
    Active = 1,
    ReadOnly = 2,
    Deleted = 3,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BlockRetentionMode {
    Unspecified = 0,
    Full = 1,
    Compacted = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockRetentionPolicy {
    pub mode: Option<BlockRetentionMode>,
    pub retention_blocks: Option<u64>,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VaultHealthProto {
    Unspecified = 0,
    Healthy = 1,
    Diverged = 2,
    Recovering = 3,
}

// ---------------------------------------------------------------------------
// Token / credential cross-cutting types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
    /// UNIX nanoseconds.
    pub access_expires_at: u64,
    /// UNIX nanoseconds.
    pub refresh_expires_at: u64,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CredentialType {
    Unspecified = 0,
    Passkey = 1,
    Totp = 2,
    RecoveryCode = 3,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TotpAlgorithm {
    Sha1 = 0,
    Sha256 = 1,
    Sha512 = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CredentialInfo {
    pub credential_type: String,
    pub credential_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TotpRequired {
    /// 32-byte one-time challenge nonce.
    pub challenge_nonce: Bytes,
}

// ---------------------------------------------------------------------------
// System discovery cross-cutting
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: Option<NodeId>,
    pub addresses: Vec<String>,
    pub grpc_port: u32,
    /// UNIX nanoseconds.
    pub last_seen: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: Option<NodeId>,
    pub addresses: Vec<String>,
    pub grpc_port: u32,
    /// UNIX nanoseconds.
    pub last_heartbeat: u64,
    /// UNIX nanoseconds.
    pub joined_at: u64,
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrganizationRegistry {
    pub slug: Option<OrganizationSlug>,
    pub name: String,
    pub region: String,
    pub members: Vec<NodeId>,
    pub status: OrganizationStatus,
    pub config_version: u64,
    /// UNIX nanoseconds.
    pub created_at: u64,
}

// ---------------------------------------------------------------------------
// Health cross-cutting
// ---------------------------------------------------------------------------

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HealthStatus {
    Unspecified = 0,
    Healthy = 1,
    Degraded = 2,
    Unavailable = 3,
}

// ---------------------------------------------------------------------------
// Cluster / membership cross-cutting
// ---------------------------------------------------------------------------

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClusterMemberRole {
    Unspecified = 0,
    Voter = 1,
    Learner = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterMember {
    pub node_id: u64,
    pub address: String,
    pub role: ClusterMemberRole,
    pub is_leader: bool,
}

// ---------------------------------------------------------------------------
// Helper: re-export BTreeMap for service modules
// ---------------------------------------------------------------------------

/// Re-exported to make service modules' `use` lines short.
pub type ContextMap = BTreeMap<String, String>;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn block_header_roundtrips_via_postcard() {
        let header = BlockHeader {
            height: 42,
            organization: Some(OrganizationSlug::new(1234)),
            vault: Some(VaultSlug::new(5678)),
            previous_hash: Some(Hash { value: Bytes::from_static(&[0x11; 32]) }),
            tx_merkle_root: Some(Hash { value: Bytes::from_static(&[0x22; 32]) }),
            state_root: Some(Hash { value: Bytes::from_static(&[0x33; 32]) }),
            timestamp: 1_700_000_000_000_000_000,
            leader_id: Some(NodeId::new("node-1")),
            term: 7,
            committed_index: 100,
            block_hash: Some(Hash { value: Bytes::from_static(&[0x44; 32]) }),
        };

        let bytes = postcard::to_allocvec(&header).unwrap();
        let decoded: BlockHeader = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn operation_oneof_roundtrips() {
        let op = Operation {
            op: Some(OperationKind::SetEntity(SetEntity {
                key: "user:42".to_owned(),
                value: Bytes::from_static(b"hello"),
                expires_at: Some(1_700_000_000),
                condition: Some(SetCondition {
                    condition: Some(SetConditionKind::NotExists(true)),
                }),
            })),
        };
        let bytes = postcard::to_allocvec(&op).unwrap();
        let decoded: Operation = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(op, decoded);
    }

    #[test]
    fn slug_types_serialize_transparently() {
        let slug = OrganizationSlug::new(42);
        let bytes = postcard::to_allocvec(&slug).unwrap();
        let decoded: OrganizationSlug = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(slug, decoded);
    }
}
