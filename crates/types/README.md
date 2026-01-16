# inferadb-ledger-types

Core types, errors, and cryptographic primitives for InferaDB Ledger.

## Overview

This crate provides foundational types used throughout InferaDB Ledger:

- **Identifiers**: `NamespaceId`, `VaultId`, `BlockHeight`, `TxId`
- **Data structures**: Blocks, transactions, operations, entities, relationships
- **Cryptography**: SHA-256 hashing, Merkle tree implementation
- **Errors**: Unified error types using `snafu`
- **Codec**: Serialization via `postcard`

## Usage

```rust
use inferadb_ledger_types::{
    Hash, sha256, sha256_concat,
    NamespaceId, VaultId, BlockHeight,
    Operation, Entity, Relationship,
    LedgerError, Result,
};

// Hash data
let hash: Hash = sha256(b"hello");

// Combine hashes
let combined = sha256_concat(&hash, b"world");

// Type-safe identifiers
let ns: NamespaceId = 1;
let vault: VaultId = 0;
```

## License

MIT OR Apache-2.0
