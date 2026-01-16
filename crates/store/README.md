# inferadb-ledger-store

Embedded B+ tree database engine for InferaDB Ledger.

## Overview

A purpose-built storage engine optimized for InferaDB's Ledger requirements:

- **Fixed schema**: 13 tables known at compile time
- **Single writer**: Leverages Raft's serialization (no MVCC needed)
- **Append-optimized**: Designed for Raft log access patterns
- **Checksummed pages**: XXHash verification for crash safety
- **Copy-on-write**: Consistent snapshots without blocking writes

## Architecture

```text
Database API
    │
Transaction Layer (ReadTxn/WriteTxn)
    │
B+ Tree Layer (get, insert, delete, range)
    │
Page Layer (allocator, cache, COW)
    │
Storage Backend (File / InMemory)
```

## Usage

```rust
use inferadb_ledger_store::{Database, DatabaseConfig};

// Open database
let db = Database::open("ledger.db", DatabaseConfig::default())?;

// Write transaction
let mut txn = db.write_txn()?;
txn.insert::<RaftLog>(42, &entry)?;
txn.commit()?;

// Read transaction (snapshot isolation)
let txn = db.read_txn()?;
let entry = txn.get::<RaftLog>(42)?;
```

## License

MIT OR Apache-2.0
