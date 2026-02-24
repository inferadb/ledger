# ADR: Dual-Slot Commit Protocol

## Context

InferaDB Ledger's embedded storage engine needs crash-safe atomic commits. When committing a write transaction, the database must transition from state A to state B such that any crash during the transition leaves the database in a valid, recoverable state.

### Options Evaluated

| Option                     | Fsyncs per Commit        | Recovery            | Complexity                           |
| -------------------------- | ------------------------ | ------------------- | ------------------------------------ |
| Write-Ahead Log (WAL)      | 1 (log) + 1 (checkpoint) | Replay log          | High (log management, checkpointing) |
| Shadow paging (dual-slot)  | 2 (data + header flip)   | Read alternate slot | Low (no log management)              |
| Copy-on-Write with journal | 1-2                      | Replay journal      | Medium                               |

## Decision

**Use dual-slot commit (shadow paging)** with a single "god byte" as the atomic commit point.

### Protocol

The database header contains two commit slots and a "god byte" that indicates which slot is active:

```
Header (768 bytes):
├── Magic + Version + PageSize (14 bytes)
├── God Byte (1 byte): bit 0 = active slot, bit 1 = recovery flag
├── Commit Slot 0 (64 bytes): table_dir, total_pages, txn_id, timestamp, free_list, checksum
├── Commit Slot 1 (64 bytes): same layout
└── Reserved (624 bytes)
```

Commit sequence:

1. Flush all data pages to disk (COW pages already written to new locations).
2. Write new state to the **secondary** (inactive) slot.
3. Write the full header to disk.
4. **First fsync** — secondary slot is now durable with valid checksum.
5. Flip the god byte (toggle bit 0).
6. Write the header again.
7. **Second fsync** — god byte flip is durable.

### Crash Safety Argument

| Crash Point       | Primary Slot          | Secondary Slot        | Recovery        |
| ----------------- | --------------------- | --------------------- | --------------- |
| Before step 4     | Valid (old)           | Partial (no checksum) | Use primary     |
| Between steps 4-7 | Valid (old)           | Valid (new)           | Either works    |
| After step 7      | Valid (new, promoted) | Valid (old, demoted)  | Use new primary |

The key insight: **at least one slot always has a valid checksum.** Recovery reads both slots, tries the one indicated by the god byte, and falls back to the other if the checksum is invalid.

### Recovery Flag

Bit 1 of the god byte tracks whether a clean shutdown occurred:

- Set to 1 before every commit.
- Cleared on clean shutdown (not yet implemented — planned for graceful shutdown).

If the flag is set on open, the free list may be inconsistent (crash after data commit but before free list persistence). Recovery rebuilds the free list by scanning all reachable pages in the B+ tree — O(total_pages) but only happens after crashes.

### Snapshot Integration

Snapshots use a separate file-based format with zstd compression and SHA-256 integrity verification. The dual-slot protocol secures individual `WriteTransaction` commits, while snapshot installation writes all state into a single `WriteTransaction` — the same atomic commit mechanism protects both normal apply-loop writes and full snapshot installs.

During snapshot installation, the `WriteTransaction` accumulates all dirty pages (COW copies of modified B+ tree nodes). On `commit()`, these are flushed via the dual-slot protocol. If the process crashes before commit, the `WriteTransaction` is dropped and no state changes are visible — the node retries snapshot installation on the next attempt from openraft.

## Consequences

### Positive

- Exactly 2 fsyncs per commit — predictable latency.
- No log file management, compaction, or checkpointing.
- Recovery is instant — read header, pick valid slot, done.
- Simple correctness argument (valid checksum = valid state).

### Negative

- Cannot batch multiple transactions between fsyncs (each commit requires its own 2-fsync sequence).
- No incremental recovery — if data pages are corrupted, must restore from snapshot.
- The 768-byte header is always rewritten in full (not a concern at 2 fsyncs/commit).

### Neutral

- Each commit slot is 64 bytes — room for future fields within the 768-byte header.
- The god byte is a single byte — bit-level atomicity depends on hardware, but checksum validation handles partial writes regardless.

## References

- `crates/store/src/backend/mod.rs` — `DatabaseHeader`, `CommitSlot`
- `crates/store/src/db.rs` — `persist_state_to_disk()`, `load_state_from_disk()`
- `crates/store/tests/crash_recovery.rs` — Crash injection tests
- `crates/raft/src/snapshot.rs` — File-based snapshot writer/reader
- `MANIFEST.md` — Storage format specification
