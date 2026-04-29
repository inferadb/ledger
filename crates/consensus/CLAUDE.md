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
- **`ConsensusState`** (`shard.rs`), **`Reactor`** (`reactor.rs`), **`Action`** (`action.rs`) — event-driven core.
- **`WalBackend` trait** (`wal_backend.rs`) + impls under `wal/`: `segmented` (production), `encrypted`, `memory`, `io_uring_backend`.
- **`StateMachine` trait** (`state_machine.rs`).
- **Leadership + safety**: `leadership.rs`, `lease.rs`, `committed.rs`, `idempotency.rs`, `recovery.rs`, `split.rs`, `circuit_breaker.rs`.
- **Transport**: `transport.rs`, `network_outbox.rs`, `router.rs`.
- **Time + entropy (injectable)**: `clock.rs`, `timer.rs`, `rng.rs`.
- **Closed timestamps**: `closed_ts.rs`.
- **Deterministic simulation**: `simulation/` (`harness.rs`, `multi_raft.rs`, `network.rs`) + `buggify.rs`.
- **O6 hibernation primitives**: `wake.rs` (`ShardWakeNotifier` trait + `NoopShardWakeNotifier` default) + reactor pause/resume infrastructure (`Arc<DashMap<ConsensusStateId, ()>>` shared between `ConsensusEngine` and `Reactor`). Public API: `ConsensusEngine::pause_shard` / `resume_shard` / `is_shard_paused`, `ConsensusEngine::start_with_wake_notifier`. Pass 1 only — raft-layer wake/sleep wiring lands in Pass 2.
- **Snapshot coordination**: `snapshot_coordinator.rs` (`SnapshotCoordinator` trait + `NoopSnapshotCoordinator` default) — Stage 2 callback invoked by the reactor when an `Action::TriggerSnapshot` is processed. Mirrors `ShardWakeNotifier`: same single-task event-loop contract (implementor MUST dispatch any I/O asynchronously and return immediately), same drop-and-let-Raft-retry semantics on a missing scope mapping. Public API: `ConsensusEngine::start_with_coordinators` (full overload accepting both wake notifier and snapshot coordinator). The raft crate installs `RaftManagerSnapshotCoordinator` which builds + persists snapshots off the reactor task and calls back into `ConsensusEngine::notify_snapshot_completed` to advance `last_snapshot_index`.
- **Snapshot wire transfer**: `snapshot_sender.rs` (`SnapshotSender` trait + `NoopSnapshotSender` default) — Stage 3 callback invoked by the reactor when an `Action::SendSnapshot` is processed. Sister of `SnapshotCoordinator`: same single-task event-loop contract, same drop-and-let-Raft-retry semantics on dispatch failure (the leader's heartbeat replicator re-emits `Action::SendSnapshot` on the next cycle). Public API: `ConsensusEngine::start_with_full_coordinators` (accepts wake notifier + snapshot coordinator + snapshot sender). The raft crate installs `RaftManagerSnapshotSender` which resolves the shard ID to its scope, opens the at-rest encrypted snapshot via `SnapshotPersister::open_encrypted`, and streams chunks to the follower over the internal Raft transport's `InstallSnapshotStream` RPC. The reactor's `Action::SendSnapshot` arm replaced its previous empty-`Vec::new()` `Message::InstallSnapshot` placeholder with this dispatch — when no real sender is wired, the action is dropped and the leader retransmits.
- **Snapshot install**: `snapshot_installer.rs` (`SnapshotInstaller` trait + `NoopSnapshotInstaller` default) — Stage 4 callback invoked by the reactor when an `Action::InstallSnapshot` is processed. Sister of `SnapshotSender`: same single-task event-loop contract, same drop-and-let-Raft-retry semantics on dispatch failure (the leader's heartbeat replicator re-emits `Action::SendSnapshot` on the next cycle, restaging the file via Stage 3 and re-emitting this action). Public API: `ConsensusEngine::start_with_all_callbacks` (accepts wake notifier + snapshot coordinator + snapshot sender + snapshot installer). The raft crate installs `RaftManagerSnapshotInstaller` which locates the staged file via `SnapshotPersister::list_staged`, decrypts via the per-scope `SnapshotKeyProvider` / `decrypt_snapshot`, routes the install onto the apply task that owns the `RaftLogStore` via a per-shard `apply_command::ApplyCommand::InstallSnapshot` channel (`InnerGroup::apply_command_tx` for org-level scopes, `InnerVaultGroup::apply_command_tx` for per-vault scopes), and on completion calls back into `ConsensusEngine::notify_snapshot_installed` so the shard advances `last_applied` / `last_snapshot_index` / `commit_index` and truncates the log prefix subsumed by the snapshot. The receiver-side `Message::InstallSnapshot` handler in `consensus_state.rs` previously was a no-op; Stage 4 replaces it with the action emission gated on `last_included_index > last_snapshot_index` for idempotency. Both org- and per-vault install paths are operational; end-to-end coverage lives in `crates/server/tests/snapshot_install.rs`.

## Test Patterns

- **Prefer simulation**: `simulation/` runs invariants deterministically without the tokio runtime. New primitives should have simulation coverage when feasible.
- **`buggify.rs`** injects faults only under simulation; never in production.
- Integration tests in `tests/` cover election, membership, replication, WAL durability, encryption round-trips, and multi-raft linearizability.
- Clock / timer / RNG are injectable — tests parametrize them, production uses the defaults.

## Local Golden Rules

1. **`ConsensusState` returns `Action` values and performs no I/O** (root rule 10). Any blocking call, disk read, or network send inside `ConsensusState` is a correctness bug caught by `consensus-reviewer`.
2. **`Reactor` is the only place I/O happens.** A background task spawned from `ConsensusState` or elsewhere to do fsync / send is a layering break.
3. **WAL writes are batched with a single `fsync` per batch.** Never per proposal — destroys throughput and breaks batch ordering assumptions.
4. **Production WAL is `wal/segmented.rs`.** `memory` and `io_uring_backend` are test/experiment backends. Don't default to `memory` in any production config path.
5. **`ConsensusEngine` lives in `engine.rs`.** `lib.rs` re-exports the public surface. Moving `ConsensusEngine` back into `lib.rs` is a layering regression.
6. **No `openraft` imports, ever** (root rule 9). Grep must show zero dependency references.
7. **Snapshots are encrypted end-to-end via `snapshot_crypto.rs`.** Never bypass this for "performance" — unencrypted snapshots on disk defeat the per-vault key rotation story.
8. **`LogId` shape is internal; don't copy openraft's `LogId::new(CommittedLeaderId::new(term, node_id), index)` shape.** The in-house Raft uses its own log-id type.
9. **Simulation coverage is preferred for new primitives** (leader lease, closed timestamps, apply pipeline). If a bug can reproduce under simulation, add a simulation test — don't rely solely on tokio-runtime integration tests.
10. **Cross-shard state changes go through `ConsensusEngine`, never directly between shards.** A `ConsensusState` calling into another `ConsensusState` is a layering violation.

11. **`ConsensusState::leadership_mode` gates election timers.** `LeadershipMode::SelfElect` runs normal Raft elections; `LeadershipMode::Delegated` no-ops the election timeout and requires the surrounding runtime to drive leadership via `ConsensusState::adopt_leader` (handled by `ReactorEvent::AdoptLeader` routing). A `Delegated` shard without a corresponding `adopt_leader` driver will silently stay leaderless. The raft crate's `RaftManager` installs the driver for per-organization `ConsensusState`s (root raft rule 14); the consensus crate does not know about orgs, so preserve the distinction — don't hard-wire delegated routing into `ConsensusState` or `ConsensusEngine`. Audited by `consensus-reviewer`.

12. **Pause-state lookups stay lock-free.** `ConsensusEngine::is_shard_paused` reads from a `DashMap` shared with the reactor; mutations always round-trip through `ReactorEvent::PauseShard` / `ReactorEvent::ResumeShard` so the reactor can confirm the shard is registered before flipping the flag. Reading the map is on the hot path of every `Action::Send` and every incoming `PeerMessage` — keep it `contains_key`-style. Adding a parking_lot lock or rebuilding the map every tick re-introduces the contention the design avoids. Pause is observational from outside the reactor: it does NOT abort in-flight proposals (those resolve normally if quorum is met before the flag is checked) and does NOT shut down the shard (use `ReactorEvent::RemoveShard` for that). Cleanup timers are explicitly NOT suppressed — pausing a shard does not block decommissioning. The notifier contract is "spawn your wake work and return immediately"; blocking inside `ShardWakeNotifier::on_peer_message_for_paused_shard` stalls every other shard managed by the reactor. Drop-and-let-Raft-retry is the documented semantic for `PeerMessage`-while-paused — the reactor does not buffer the message, and the peer's next AppendEntries heartbeat (~50ms) lands on the awoken shard. Audited by `consensus-reviewer`.
