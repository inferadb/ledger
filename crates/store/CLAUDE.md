# CLAUDE.md — store

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

B+ tree, pages, transactions, pluggable backends, per-vault crypto key management. Byte-level persistence — every crate above reads and writes through `StorageEngine` (in the `state` crate). Bugs here corrupt committed data; treat every change to page layout, leaf invariants, or WAL handling as high-risk.

## Load-Bearing Files

These files are load-bearing — their invariants ripple beyond the local file. Not off-limits; use caution and understand the ramifications before editing.

| File                             | Reason                                                                                                                   |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `src/btree/node.rs`              | `LeafNode` / internal-node layout. `LeafNode::init` + `from_page` contract; changes silently break every persisted page. |
| `src/btree/split.rs`             | Split logic. Race conditions here are hard to reproduce and cause silent data divergence.                                |
| `src/page/mod.rs` and submodules | Page header layout and dual-slot commit. A stray byte here is a file-format break.                                       |
| `src/backend/encrypted.rs`       | Per-vault AES-256-GCM envelope. Changing the nonce scheme or AAD breaks backward-compatible reads.                       |
| `src/transaction.rs`             | Transaction semantics — atomicity assumed by `StorageEngine` above.                                                      |
| `src/db/transactions.rs`         | `WriteTransaction::commit` (strict-durable; dual-slot fsync) vs `commit_in_memory` (lazy; cache-only). Mis-routing is a silent durability or latency regression. |
| `src/db/mod.rs`                  | `Database::sync_state` + `last_synced_snapshot_id` — the durability primitives the raft-layer `StateCheckpointer` + graceful-shutdown paths drive. Coalesces concurrent callers. |

## Owned Surface

- **`StorageBackend` trait** + impls: `FileBackend`, `InMemoryBackend`, `EncryptedBackend<B>`.
- **B+ tree**: `cursor.rs`, `node.rs`, `split.rs` under `src/btree/`.
- **Page cache + commit**: `src/page/`.
- **Crypto**: per-vault key management in `src/crypto/`.
- **`StoreError`** (`src/error.rs`) — I/O, page-corruption, WAL-checksum, transaction-conflict variants.
- **`CrashInjector`** — exposed through `crates/test-utils/`, not from here; store wires the injection points.

## Test Patterns

- Unit tests use `InMemoryBackend`. Integration / recovery tests use `FileBackend` wrapped in `EncryptedBackend`.
- Crash recovery tests inject faults via `CrashInjector` from `test-utils` — never `panic!` in production code to simulate.
- B+ tree proptests cover split/merge invariants; MerkleProof tests restrict to power-of-2 leaves (see `types` crate rules).
- Benches: no performance-regression CI currently; benchmark additions must include a justifying doc comment.

## Local Golden Rules

1. **`leaf_fill_factor()` iterates live cells.** Never fall back to `free_space()` gap — after deletes, gap doesn't reflect dead-space reality.
2. **`merge_leaves()` rebuilds the left page from scratch via `LeafNode::init()`.** In-place merge corrupts the dead-space accounting the next compaction round depends on.
3. **`Page::new()` initializes only the page header.** Leaf pages additionally require `LeafNode::init()` before use; `from_page()` assumes an initialized header.
4. **`compact()` re-collects `leaf_info` after each merge.** Stale `leaf_info` references freed pages; using it after a merge without re-collection is a use-after-free.
5. **Table signatures take `&Table::KeyType` / `&Table::ValueType`.** For `Entities` both are `Vec<u8>` — pass `&Vec<u8>`, not `&[u8]`. The difference matters at the generic boundary.
6. **Crypto-shredding leaves `_idx:` entries behind.** Erasure must delete the primary + every `_idx:` pointing at it. This invariant is enforced in the `state` crate; don't assume it's handled here.
7. **Never `panic!` in production code to force a crash.** Use `CrashInjector` (`crates/test-utils`). Production panics bypass the orderly shutdown path.
8. **`commit_in_memory` leaves pages in cache + advances the `committed_state` ArcSwap; it MUST NOT call `persist_state_to_disk`, `flush_pages`, or `try_free_pending_pages`.** The last one is load-bearing: freeing CoW-displaced pages before the on-disk god byte advances risks reuse-during-CoW. `Database::sync_state` is the only path that advances `last_synced_snapshot_id` and drains `pending_frees`. Callers choosing between `commit` and `commit_in_memory` MUST consult `docs/superpowers/specs/2026-04-19-commit-durability-audit.md` before flipping a call site.
