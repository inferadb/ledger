# store

B+ tree, pages, transactions, backends, crypto (key management). Byte-level persistence — everything above this crate reads and writes through `StorageEngine`.

## Responsibilities

- B+ tree implementation: pages, leaves, internal nodes, splits, merges
- Page cache and transaction management
- Pluggable backends (memory, file, encrypted)
- Per-vault crypto key management for AES-256-GCM WAL + page encryption
- Crash recovery via `CrashInjector` (in `test-utils`)

## Invariants (read before editing the B+ tree)

- **`leaf_fill_factor()` must iterate live cells.** `free_space()` gap doesn't account for dead data after deletes — compaction triggers off true live-fill.
- **`merge_leaves()` rebuilds left page from scratch via `LeafNode::init()`** to reclaim dead space. Don't try to merge in-place.
- **`compact()` re-collects `leaf_info` after each merge.** Stale info references freed pages.
- **`Page::new()` sets only the page header.** Leaf nodes additionally require `LeafNode::init()`; `from_page()` assumes an initialized header.
- **Secondary-index entries are transactional with primary writes** — erasure must delete both (crypto-shredding alone is not enough for `_idx:` keys; see state crate).

## Conventions

- `Table::KeyType` / `Table::ValueType` are generic. For `Entities`, both are `Vec<u8>` — pass `&Vec<u8>`, not `&[u8]` (the `insert`/`get` signatures take `&Table::KeyType`).
- Benches: `btree_bench` in `benches/` — tracked by performance-regression CI.
- Backend selection belongs at config boundaries, not inside hot-path code.

## Related tooling

- Agent: `unsafe-panic-auditor` (scrutinizes `.unwrap()` in non-test code)
- Skill: `/just-ci-gate` — run before claiming store changes done
