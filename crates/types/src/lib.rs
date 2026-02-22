//! Core types, errors, and cryptographic primitives for InferaDB Ledger.
//!
//! Provides the foundational types used throughout the ledger:
//! - Newtype identifiers (`OrganizationId`, `OrganizationSlug`, `VaultId`, `UserId`, `ShardId`)
//! - Data structures for blocks, transactions, and operations
//! - Configuration types with validated builders
//! - Cryptographic hashing functions (SHA-256, seahash)
//! - Merkle tree implementation
//! - Audit event types for compliance logging
//! - Error types using snafu

#![deny(unsafe_code)]
#![warn(missing_docs)]

/// Audit logging types for compliance-ready event tracking.
pub mod audit;
/// Serialization and deserialization via postcard.
pub mod codec;
/// Configuration types with validated builders.
pub mod config;
/// Error types using snafu with structured error codes.
pub mod error;
/// Cryptographic hashing (SHA-256, seahash) and block/transaction hashing.
pub mod hash;
/// Merkle tree construction and verification.
pub mod merkle;
/// Snowflake-style globally unique ID generation.
pub mod snowflake;
/// Core domain types: identifiers, blocks, transactions, operations.
pub mod types;
/// Input validation for gRPC request fields.
pub mod validation;

pub use audit::{AuditAction, AuditEvent, AuditOutcome, AuditResource};
pub use codec::{CodecError, decode, encode};
pub use error::{ErrorCode, LedgerError, Result};
pub use hash::{
    BucketHasher, EMPTY_HASH, Hash, ZERO_HASH, bucket_id, compute_chain_commitment,
    compute_tx_merkle_root, sha256, sha256_concat, tx_hash, vault_entry_hash,
};
pub use types::{
    // Structs
    BlockHeader,
    BlockRetentionMode,
    BlockRetentionPolicy,
    ChainCommitment,
    // Type aliases
    ClientId,
    Entity,
    // Raft node ID
    LedgerNodeId,
    NodeId,
    // Enums
    Operation,
    OrganizationId,
    // External organization identifier
    OrganizationSlug,
    // Resource accounting
    OrganizationUsage,
    Relationship,
    SetCondition,
    ShardBlock,
    ShardId,
    Transaction,
    TransactionValidationError,
    TxId,
    UserId,
    VaultBlock,
    VaultEntry,
    VaultHealth,
    VaultId,
    // External vault identifier
    VaultSlug,
    WriteResult,
    WriteStatus,
};
