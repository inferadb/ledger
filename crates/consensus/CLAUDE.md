# CLAUDE.md — consensus

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

Custom in-house multi-shard Raft. **Not built on openraft** — this is owned end-to-end. Production is always multi-Raft; code and tests must never assume a single-Raft topology. The reactor, shards, WAL, and simulation harness all live here. Bugs in this crate are how data gets lost.

## Load-Bearing Files

These files are load-bearing — their invariants ripple beyond the local file. Not off-limits; use caution and understand the ramifications before editing.

| File                     | Reason                                                                                                                                           |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `src/shard.rs`           | Event-driven Raft state machine. Must return `Action` values and perform zero I/O. Any blocking call here is a correctness bug.                  |
| `src/reactor.rs`         | Single-task event loop. Batches WAL writes and network sends; a side task doing fsync or send outside the reactor violates ordering assumptions. |
| `src/engine.rs`          | `ConsensusEngine`. Public multi-shard API — `propose`, `read_index`, membership. Public surface changes cascade everywhere.                      |
| `src/wal/segmented.rs`   | Production WAL — per-vault AES-256-GCM, segmented, single fsync per batch.                                                                       |
| `src/state_machine.rs`   | `StateMachine` trait consumed by `crates/raft/src/apply_worker.rs`. Trait changes are cross-crate breaks.                                        |
| `src/snapshot_crypto.rs` | Encrypted snapshot envelope. Changes break snapshot compatibility between versions.                                                              |
| `src/committed.rs`       | Commit-index safety. A bug here allows uncommitted state to leak into `apply`.                                                                   |

## Owned Surface

- **`ConsensusEngine`** (`engine.rs`) — multi-shard API.
- **`Shard`** (`shard.rs`), **`Reactor`** (`reactor.rs`), **`Action`** (`action.rs`) — event-driven core.
- **`WalBackend` trait** (`wal_backend.rs`) + impls under `wal/`: `segmented` (production), `encrypted`, `memory`, `io_uring_backend`.
- **`StateMachine` trait** (`state_machine.rs`).
- **Leadership + safety**: `leadership.rs`, `lease.rs`, `committed.rs`, `idempotency.rs`, `recovery.rs`, `split.rs`, `circuit_breaker.rs`.
- **Transport**: `transport.rs`, `network_outbox.rs`, `router.rs`.
- **Time + entropy (injectable)**: `clock.rs`, `timer.rs`, `rng.rs`.
- **Closed timestamps**: `closed_ts.rs`.
- **Deterministic simulation**: `simulation/` (`harness.rs`, `multi_raft.rs`, `network.rs`) + `buggify.rs`.

## Test Patterns

- **Prefer simulation**: `simulation/` runs invariants deterministically without the tokio runtime. New primitives should have simulation coverage when feasible.
- **`buggify.rs`** injects faults only under simulation; never in production.
- Integration tests in `tests/` cover election, membership, replication, WAL durability, encryption round-trips, and multi-raft linearizability.
- Clock / timer / RNG are injectable — tests parametrize them, production uses the defaults.

## Local Golden Rules

1. **`Shard` returns `Action` values and performs no I/O** (root rule 10). Any blocking call, disk read, or network send inside `Shard` is a correctness bug caught by `consensus-reviewer`.
2. **`Reactor` is the only place I/O happens.** A background task spawned from `Shard` or elsewhere to do fsync / send is a layering break.
3. **WAL writes are batched with a single `fsync` per batch.** Never per proposal — destroys throughput and breaks batch ordering assumptions.
4. **Production WAL is `wal/segmented.rs`.** `memory` and `io_uring_backend` are test/experiment backends. Don't default to `memory` in any production config path.
5. **`ConsensusEngine` lives in `engine.rs`.** `lib.rs` re-exports the public surface. Moving `ConsensusEngine` back into `lib.rs` is a layering regression.
6. **No `openraft` imports, ever** (root rule 9). Grep must show zero dependency references.
7. **Snapshots are encrypted end-to-end via `snapshot_crypto.rs`.** Never bypass this for "performance" — unencrypted snapshots on disk defeat the per-vault key rotation story.
8. **`LogId` shape is internal; don't copy openraft's `LogId::new(CommittedLeaderId::new(term, node_id), index)` shape.** The in-house Raft uses its own log-id type.
9. **Simulation coverage is preferred for new primitives** (leader lease, closed timestamps, apply pipeline). If a bug can reproduce under simulation, add a simulation test — don't rely solely on tokio-runtime integration tests.
10. **Cross-shard state changes go through `ConsensusEngine`, never directly between shards.** A `Shard` calling into another `Shard` is a layering violation.
