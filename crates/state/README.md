# inferadb-ledger-state

Domain state management for InferaDB Ledger.

## Overview

This crate builds on `inferadb-ledger-store` to provide domain-specific storage:

- **Entity store**: Key-value storage with TTL and versioning
- **Relationship store**: Authorization tuples (resource, relation, subject)
- **State layer**: Block application and bucket-based state root computation
- **Indexes**: Dual indexes for forward/reverse relationship queries
- **Snapshots**: Creation, restoration, and tiered storage (local + S3/GCS/Azure)
- **Time-travel**: Historical queries via temporal indexing

## Architecture

```text
StateLayer (block application, state roots)
    │
    ├── EntityStore
    ├── RelationshipStore
    ├── IndexManager
    └── ShardManager
    │
StorageEngine (inferadb-ledger-store wrapper)
    │
Database (B+ tree)
```

## Usage

```rust
use inferadb_ledger_state::{StateLayer, StorageEngine};

let engine = StorageEngine::open("data.db")?;
let state = StateLayer::new(engine);

// Apply a block
let commitment = state.apply_block(&block)?;

// Query relationships
let rels = state.get_relationships(namespace, vault, "doc:1", "viewer")?;
```

## License

MIT OR Apache-2.0
