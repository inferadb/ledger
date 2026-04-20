//! Core types, errors, and cryptographic primitives for InferaDB Ledger.
//!
//! Provides the foundational types used throughout the ledger:
//! - Newtype identifiers for organizations, vaults, users, teams, apps, invitations, and signing
//!   keys (both internal `i64` IDs and external Snowflake `u64` slugs), plus geographic region enum
//!   (`Region`)
//! - Data structures for blocks, transactions, and operations
//! - Configuration types with validated builders
//! - Cryptographic hashing functions (SHA-256, seahash)
//! - Merkle tree implementation
//! - Error types using snafu

#![deny(unsafe_code)]
#![warn(missing_docs)]

/// Serialization and deserialization via postcard.
pub mod codec;
/// Configuration types with validated builders.
pub mod config;
/// Email blinding key and HMAC-based email hashing for global uniqueness.
pub mod email_hash;
/// Error types using snafu with structured error codes.
pub mod error;
/// Structured error codes for Raft state machine responses.
mod error_code;
/// Event logging domain types for organization-scoped audit trails.
pub mod events;
/// Cryptographic hashing (SHA-256, seahash) and block/transaction hashing.
pub mod hash;
/// Organization invitation types: status, records, and index entries.
pub mod invitation;
/// Merkle tree construction and verification.
pub mod merkle;
/// Constants for user onboarding (email verification + account creation).
pub mod onboarding;
/// Snowflake-style globally unique ID generation.
pub mod snowflake;
/// JWT token types for user sessions and vault access.
pub mod token;
/// W3C Trace Context propagation via gRPC metadata headers.
pub mod trace_context;
/// Core domain types: identifiers, blocks, transactions, operations.
pub mod types;
/// Input validation for gRPC request fields.
pub mod validation;

pub use codec::{CodecError, decode, encode};
pub use email_hash::{
    EmailBlindingKey, EmailBlindingKeyParseError, bytes_to_hex, compute_email_hmac, normalize_email,
};
pub use error::{DiagnosticCode, LedgerError, Result};
pub use error_code::ErrorCode;
pub use hash::{
    BucketHasher, EMPTY_HASH, Hash, ZERO_HASH, bucket_id, compute_chain_commitment,
    compute_tx_merkle_root, hash_eq, sha256, sha256_concat, tx_hash, vault_entry_hash,
};
pub use invitation::{
    InvitationStatus, InviteEmailEntry, InviteIndexEntry, OrganizationInvitation,
    effective_invitation_status,
};
pub use token::{
    SESSION_AUDIENCE, SIGNING_KEY_ENVELOPE_SIZE, SigningKeyEnvelope, TokenError, TokenSubject,
    TokenType, UserSessionClaims, VAULT_AUDIENCE, ValidatedToken, VaultTokenClaims,
};
pub use types::{
    // Constants
    ALL_REGIONS,
    // App identifiers
    AppCredentialType,
    AppId,
    AppSlug,
    // Structs
    BlockHeader,
    BlockRetentionMode,
    BlockRetentionPolicy,
    ChainCommitment,
    ClientAssertionId,
    // Type aliases
    ClientId,
    // Credential types
    CredentialData,
    CredentialType,
    EmailVerifyTokenId,
    Entity,
    // Invitation identifiers
    InviteId,
    InviteSlug,
    // Raft node ID
    LedgerNodeId,
    NodeId,
    // Enums
    Operation,
    OrganizationId,
    OrganizationMemberRole,
    // External organization identifier
    OrganizationSlug,
    // Resource accounting
    OrganizationUsage,
    // Credential data structs
    PasskeyCredential,
    // TOTP challenge
    PendingTotpChallenge,
    // Primary auth method
    PrimaryAuthMethod,
    // Recovery codes
    RecoveryCodeCredential,
    // Refresh token identifier
    RefreshTokenId,
    // Region types
    Region,
    RegionBlock,
    RegionParseError,
    Relationship,
    SetCondition,
    // Signing key types
    SigningKeyId,
    SigningKeyScope,
    SigningKeyStatus,
    // Team identifiers
    TeamId,
    TeamSlug,
    // Token version counter
    TokenVersion,
    // TOTP types
    TotpAlgorithm,
    TotpCredential,
    Transaction,
    TransactionValidationError,
    TxId,
    // User credential types
    UserCredential,
    UserCredentialId,
    UserEmailId,
    UserId,
    // User enums
    UserRole,
    // External user identifier
    UserSlug,
    UserStatus,
    VaultBlock,
    VaultEntry,
    VaultHealth,
    VaultId,
    // External vault identifier
    VaultSlug,
    WriteResult,
    WriteStatus,
};
