//! Core types, errors, and cryptographic primitives for InferaDB Ledger.
//!
//! This crate provides the foundational types used throughout the ledger:
//! - Type aliases for identifiers (NamespaceId, VaultId, etc.)
//! - Data structures for blocks, transactions, and operations
//! - Cryptographic hashing functions (SHA-256)
//! - Merkle tree implementation
//! - Error types using snafu

pub mod config;
pub mod error;
pub mod hash;
pub mod merkle;
pub mod types;

// Re-export commonly used types at crate root
pub use error::{LedgerError, Result};
pub use hash::{BucketHasher, EMPTY_HASH, Hash, ZERO_HASH, bucket_id, sha256, sha256_concat};
pub use types::*;
