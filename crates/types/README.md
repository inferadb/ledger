# inferadb-ledger-types

Core types, errors, and cryptographic primitives for InferaDB Ledger.

## Overview

This crate provides foundational types used throughout InferaDB Ledger:

- **Identifiers**: `OrganizationSlug`, `OrganizationId`, `VaultId`, `BlockHeight`, `TxId`
- **Data structures**: Blocks, transactions, operations, entities, relationships
- **Cryptography**: SHA-256 hashing, Merkle tree implementation
- **Errors**: Unified error types using `snafu`
- **Codec**: Serialization via `postcard`

## Usage

```rust
use inferadb_ledger_types::{
    Hash, sha256, sha256_concat,
    OrganizationSlug, VaultId, BlockHeight,
    Operation, Entity, Relationship,
    LedgerError, Result,
};

// Hash data
let hash: Hash = sha256(b"hello");

// Combine hashes
let combined = sha256_concat(&hash, b"world");

// Type-safe identifiers
let org: OrganizationSlug = OrganizationSlug::new(1);
let vault: VaultId = 0;

// Build domain types with type-safe builders
use inferadb_ledger_types::{BlockHeader, Transaction};
use chrono::Utc;

let header = BlockHeader::builder()
    .height(100)
    .organization_slug(org)
    .vault_id(vault)
    .timestamp(Utc::now())
    .prev_hash(Hash::zero())
    .state_root(hash)
    .tx_root(hash)
    .tx_count(1)
    .proposer("node-1")
    .signature(vec![])
    .build();

let tx = Transaction::builder()
    .id("tx-1")
    .actor("user:alice")
    .operations(vec![Operation::set_entity("key", b"value".to_vec())])
    .seq(1)
    .timestamp(Utc::now())
    .build()?;  // Fallible: validates actor and operations
```

## License

MIT OR Apache-2.0
