//! Core types, errors, and cryptographic primitives for InferaDB Ledger.
//!
//! This crate provides the foundational types used throughout the ledger:
//! - Newtype identifiers (`NamespaceId`, `VaultId`, `UserId`, `ShardId`)
//! - Data structures for blocks, transactions, and operations
//! - Configuration types with validated builders
//! - Cryptographic hashing functions (SHA-256, seahash)
//! - Merkle tree implementation
//! - Audit event types for compliance logging
//! - Error types using snafu

#![deny(unsafe_code)]

pub mod audit;
pub mod codec;
pub mod config;
pub mod error;
pub mod hash;
pub mod merkle;
pub mod types;
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
    ChainCommitment,
    // Type aliases
    ClientId,
    Entity,
    NamespaceId,
    NodeId,
    // Enums
    Operation,
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
    WriteResult,
    WriteStatus,
};
