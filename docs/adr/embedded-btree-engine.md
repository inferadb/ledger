# ADR: Embedded B+ Tree Storage Engine

## Context

InferaDB Ledger needs an ACID storage layer for:

- **Raft log storage:** Append-heavy, sequential key access.
- **State storage:** Entity/relationship CRUD with range scans and prefix queries.
- **Block archive:** Append-only block storage by height.

The storage must support single-writer MVCC, crash safety, and efficient B+ tree operations within a single-file embedded database.

### Options Evaluated

| Option  | Description                                     |
| ------- | ----------------------------------------------- |
| RocksDB | LSM-tree engine via `rust-rocksdb` FFI bindings |
| sled    | Pure-Rust lock-free B+ tree                     |
| redb    | Pure-Rust ACID B+ tree (LMDB-inspired)          |
| Custom  | Purpose-built B+ tree engine                    |

## Decision

**Build a custom embedded B+ tree engine** (`inferadb-ledger-store` crate).

### Rationale

1. **Fixed schema, compile-time tables.** Ledger has exactly 15 tables known at compile time (`TableId` enum). Generic key-value stores add overhead for schema-agnostic operation that we don't need. Our `Table` trait with associated `KeyType`/`ValueType` types provides compile-time type safety.

2. **Single-writer model.** Raft serializes all writes through a single state machine. This eliminates the need for MVCC write concurrency, lock-free structures, or write-write conflict resolution. A `Mutex<()>` write lock is sufficient.

3. **Dual-slot crash safety.** The custom engine uses a dual-slot commit protocol (shadow paging) that provides crash safety with exactly two fsyncs per commit. This is simpler and more predictable than WAL-based crash recovery used by RocksDB/sled.

4. **Copy-on-Write page management.** COW pages enable lock-free read snapshots — readers never block writers. Combined with `ArcSwap` for atomic state publication, this provides excellent read concurrency without the complexity of epoch-based reclamation (sled) or multi-version garbage collection (RocksDB).

5. **Dependency control.** RocksDB requires C++ toolchain and complex build configuration. sled's lock-free algorithms are difficult to reason about for correctness. A custom engine keeps the dependency tree minimal and the codebase fully auditable (zero `unsafe` code).

6. **Checksummed pages.** Every page includes an XXH3-64 checksum, enabling corruption detection at read time. This integrates naturally with the dual-slot commit — partial writes produce invalid checksums.

## Consequences

### Positive

- Zero external storage dependencies (no C++ toolchain, no FFI).
- Compile-time table type safety via `Table` trait.
- Predictable crash recovery (dual-slot, not WAL replay).
- Full codebase auditability (zero `unsafe`).
- Page-level checksums catch corruption early.

### Negative

- Must implement and maintain B+ tree operations (insert, delete, split, merge, compaction).
- No built-in compression (handled at snapshot level with zstd).
- No built-in bloom filters (acceptable given Raft's access patterns).
- Performance ceiling may be lower than heavily-optimized engines for general workloads.

### Neutral

- Single-file database format is simpler to operate than multi-file engines.
- Page cache is configurable but not as sophisticated as RocksDB's block cache.

## References

- `crates/store/` — Engine implementation
- `crates/store/src/btree/` — B+ tree operations
- `crates/store/src/backend/mod.rs` — Dual-slot commit protocol
- `MANIFEST.md` — Detailed storage format specification
