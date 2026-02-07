# ADR: OpenRaft 0.9 Version Choice

## Context

InferaDB Ledger uses the Raft consensus protocol for replicated writes. The Rust ecosystem offers several Raft implementations. OpenRaft was selected for its async-first design and extensible trait system — but the version choice has significant implications for storage backend flexibility.

### The Problem

OpenRaft's API evolved significantly between versions:

- **0.9:** The `RaftStorage` trait (combining log storage and state machine) is **deprecated but non-sealed**. External crates can implement it.
- **0.9 v2 API:** The replacement traits `RaftLogStorage` and `RaftStateMachine` are **sealed** — only implementations defined within the openraft crate itself can implement them.
- **0.10+:** The v2 sealed traits are the primary API. The deprecated `RaftStorage` trait may be removed.

InferaDB Ledger requires a custom storage backend: an embedded B+ tree engine with copy-on-write semantics, dual-slot commit protocol, and compile-time table safety. This custom storage is central to the project's design (see ADR: Embedded B+ Tree Engine).

### Options Evaluated

| Option                                 | Custom Storage   | API Stability         | Latest Features             | Maintenance Risk       |
| -------------------------------------- | ---------------- | --------------------- | --------------------------- | ---------------------- |
| OpenRaft 0.9, deprecated `RaftStorage` | Yes (non-sealed) | Deprecated but stable | Missing `transfer_leader()` | Medium (older version) |
| OpenRaft 0.9, v2 traits                | No (sealed)      | Current               | All 0.9 features            | Low                    |
| OpenRaft 0.10+                         | No (sealed)      | Current               | All latest features         | Low                    |
| Custom Raft implementation             | Yes              | Full control          | Whatever we build           | Very high              |

## Decision

**Use OpenRaft 0.9 with the deprecated `RaftStorage` trait** to retain custom storage backend flexibility.

```toml
openraft = { version = "0.9", features = ["serde", "tracing-log", "loosen-follower-log-revert"] }
```

### Implementation

The `RaftLogStore` struct implements the deprecated `RaftStorage<LedgerTypeConfig>` trait, which combines log storage and state machine into a single implementation:

```rust
#[allow(deprecated)]
impl RaftStorage<LedgerTypeConfig> for RaftLogStore {
    type LogReader = Self;
    type SnapshotBuilder = LedgerSnapshotBuilder;
    // ... 22 trait methods
}
```

OpenRaft's `Adaptor` bridges the deprecated trait to the v2 interfaces required by the Raft runtime:

```rust
let (log_storage, state_machine) = Adaptor::new(log_store);
```

### What InferaDB's Custom Storage Provides

The custom `RaftLogStore` implementation integrates directly with InferaDB's embedded storage engine:

1. **Embedded B+ tree log storage** — Raft log entries stored in the same B+ tree engine as application data, sharing the page cache and dual-slot commit protocol
2. **Custom snapshot format** — `CombinedSnapshot` packages Raft metadata (`AppliedState`) with vault entity data, using postcard serialization + zstd compression
3. **Integrated state management** — `StateLayer` for state root computation, per-vault block heights, and bucket-based commitment tracking
4. **Compile-time table safety** — `RaftLog` and `RaftState` tables share the same type-safe `Table` trait as application tables
5. **Deterministic ID generation** — `SequenceCounters` within `AppliedState` ensure namespace/vault/user IDs are consistent across Raft replays

Using OpenRaft's built-in storage implementations would require: abandoning the embedded B+ tree, maintaining a separate storage format for Raft data, and losing the shared page cache — fundamentally incompatible with the project's architecture.

### Features Enabled

- **`serde`** — serialization for Raft messages and state
- **`tracing-log`** — integration with the tracing subscriber for structured logging
- **`loosen-follower-log-revert`** — allows followers to revert uncommitted log entries (needed for certain network partition recovery scenarios)

### Known Limitations of 0.9

1. **No `transfer_leader()` method** — graceful shutdown triggers a final snapshot and lets re-election happen automatically, rather than proactively transferring leadership
2. **Deprecated API warnings** — `#[allow(deprecated)]` attribute required on the trait implementation
3. **Potential missing patches** — bug fixes or performance improvements in newer versions may not be backported
4. **`RaftNetwork` trait has fixed signatures** — cannot inject trace context into Raft RPCs (documented limitation in distributed tracing propagation)

## Consequences

### Positive

- **Full control over storage** — custom B+ tree engine with COW semantics, dual-slot commit, and shared page cache
- **Custom snapshot format** — application state (entities, relationships, vault commitments) packaged with Raft metadata in a single compressed blob
- **Architectural coherence** — Raft log and application data share the same storage engine, avoiding the complexity of managing two separate storage systems
- **Stable API** — the deprecated `RaftStorage` trait is unlikely to be removed in a 0.9.x patch release

### Negative

- **Pinned to 0.9** — cannot upgrade to 0.10+ without a migration strategy for the sealed trait barrier
- **Missing features** — no `transfer_leader()` means graceful leadership handoff is less efficient (relies on timeout-based re-election)
- **Deprecated API** — requires `#[allow(deprecated)]` and may not receive new features
- **Maintenance burden** — if a critical bug is found in 0.9 but only fixed in 0.10+, we must backport or work around it

### Migration Path

If OpenRaft unseals the v2 traits in a future version (or provides a built-in storage backend that meets InferaDB's requirements), migration would involve:

1. Implementing `RaftLogStorage` and `RaftStateMachine` separately (splitting the current combined `RaftStorage` implementation)
2. Removing the `Adaptor` bridge
3. Testing snapshot compatibility between old and new formats

The `Adaptor` pattern already splits the implementation conceptually — the migration would formalize this split.

## References

- `Cargo.toml` — `openraft = { version = "0.9", features = [...] }` workspace dependency
- `crates/raft/src/log_storage.rs` — `RaftLogStore` implementation with architecture notes (lines 1–23)
- `crates/server/src/bootstrap.rs` — `Adaptor::new(log_store)` bridge to v2 interfaces
- `docs/adr/embedded-btree-engine.md` — Why custom storage is architecturally required
- `DESIGN.md` §Node Leave — Documents missing `transfer_leader()` limitation
- OpenRaft documentation: `RaftStorage` (deprecated, non-sealed) vs `RaftLogStorage` (sealed)
