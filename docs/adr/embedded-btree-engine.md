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

1. **Fixed schema, compile-time tables.** Ledger has exactly 20 tables known at compile time (`TableId` enum). Generic key-value stores add overhead for schema-agnostic operation that we don't need. Our `Table` trait with associated `KeyType`/`ValueType` types provides compile-time type safety.

2. **Single-writer model.** Raft serializes all writes through a single state machine. This eliminates the need for MVCC write concurrency, lock-free structures, or write-write conflict resolution. A `Mutex<()>` write lock is sufficient.

3. **Dual-slot crash safety.** The custom engine uses a dual-slot commit protocol (shadow paging) that provides crash safety with exactly two fsyncs per commit. This is simpler and more predictable than WAL-based crash recovery used by RocksDB/sled.

4. **Copy-on-Write page management.** COW pages enable lock-free read snapshots — readers never block writers. Combined with `ArcSwap` for atomic state publication, this provides excellent read concurrency without the complexity of epoch-based reclamation (sled) or multi-version garbage collection (RocksDB).

5. **Dependency control.** RocksDB requires C++ toolchain and complex build configuration. sled's lock-free algorithms are difficult to reason about for correctness. A custom engine keeps the dependency tree minimal and the codebase fully auditable (zero `unsafe` code).

6. **Checksummed pages.** Every page includes an XXH3-64 checksum, enabling corruption detection at read time. This integrates naturally with the dual-slot commit — partial writes produce invalid checksums.

## Leaf Node Layout

Leaf nodes use a 16-byte node header (following the 16-byte page header):

```
Page Layout (leaf):
├── Page Header (16 bytes): page_id, checksum, flags
├── Node Header (16 bytes):
│   ├── cell_count (4 bytes)
│   ├── free_start (4 bytes)
│   └── next_leaf (8 bytes, PageId)    ← sibling pointer
├── Bloom Filter (256 bytes)
├── Cell Pointer Array (variable)
└── Cell Data (grows from end)
```

`NODE_HEADER_SIZE` is 16 bytes. The `next_leaf` field stores a `PageId` pointing to the next leaf in key order, forming a singly-linked list across all leaves. This enables O(1) leaf-to-leaf advancement during range scans (replacing O(depth) tree re-descent at each leaf boundary).

The leaf linked list is maintained by split and merge operations:
- **Split**: `left.next_leaf = right.page_id`, `right.next_leaf = old_left.next_leaf`
- **Merge**: `left.next_leaf = right.next_leaf` (right page is freed)

Key offsets:
- `LEAF_BLOOM_OFFSET = PAGE_HEADER_SIZE + NODE_HEADER_SIZE` (32)
- `LEAF_CELL_PTRS_OFFSET = LEAF_BLOOM_OFFSET + BLOOM_FILTER_SIZE` (288)

## File I/O

The `FileBackend` uses position-based I/O for lock-free concurrent reads:

- **Unix**: `read_exact_at()` / `write_all_at()` map to `pread(2)` / `pwrite(2)` — truly cursor-free, taking `&self`
- **Windows**: `seek_read()` / `seek_write()` update the internal file cursor, so reads require the write lock on Windows

Reads acquire no lock. Writes and file extension serialize through a `Mutex<()>` write lock. `sync_data()` (fdatasync) takes `&self` and requires no lock.

## Consequences

### Positive

- Zero external storage dependencies (no C++ toolchain, no FFI).
- Compile-time table type safety via `Table` trait.
- Predictable crash recovery (dual-slot, not WAL replay).
- Full codebase auditability (zero `unsafe`).
- Page-level checksums catch corruption early.
- Lock-free concurrent reads via pread on Unix.
- O(1) range scan advancement via leaf linked list.

### Negative

- Must implement and maintain B+ tree operations (insert, delete, split, merge, compaction).
- No built-in compression (handled at snapshot level with zstd).
- No built-in bloom filters (acceptable given Raft's access patterns).
- Performance ceiling may be lower than heavily-optimized engines for general workloads.

### Breaking Format Changes

The leaf node header expanded from 8 to 16 bytes (`NODE_HEADER_SIZE`) to accommodate the `next_leaf` sibling pointer. This shifts all leaf data offsets (`LEAF_BLOOM_OFFSET`, `LEAF_CELL_PTRS_OFFSET`) and makes existing data files incompatible with the new binary.

**Upgrade path**: Stop all nodes, delete data directories, start on the new binary. Nodes rejoin the cluster via snapshot install from the leader. There is no in-place migration — the page layout change affects every leaf page in the file. See [Upgrade Runbook](../operations/runbooks/rolling-upgrade.md) for the full procedure.

### Neutral

- Single-file database format is simpler to operate than multi-file engines.
- Page cache is configurable but not as sophisticated as RocksDB's block cache.

## References

- `crates/store/` — Engine implementation
- `crates/store/src/btree/` — B+ tree operations
- `crates/store/src/btree/node.rs` — Leaf node layout, `next_leaf` sibling pointer
- `crates/store/src/btree/split.rs` — Split/merge with linked list maintenance
- `crates/store/src/backend/mod.rs` — Dual-slot commit protocol
- `crates/store/src/backend/file.rs` — Lock-free pread/pwrite I/O
- `MANIFEST.md` — Detailed storage format specification
