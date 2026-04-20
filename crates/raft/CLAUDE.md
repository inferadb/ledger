# CLAUDE.md — raft

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

Saga orchestrator, background jobs, apply pipeline, rate limiting, node coordination, leader orchestration. **Despite the name, most of this crate is NOT Raft internals** — those live in `consensus`. This crate is the glue between consensus and services: it turns committed proposals into state mutations, runs long-lived background work, and coordinates across nodes.

## Load-Bearing Files

These files are load-bearing — their invariants ripple beyond the local file. Not off-limits; use caution and understand the ramifications before editing.

| File                                                | Reason                                                                                                                                                                                                                        |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/saga_orchestrator.rs`                          | Saga PII handling + crypt-scratch storage. Any write path here has residency implications.                                                                                                                                    |
| `src/graceful_shutdown.rs`                          | Six-phase drain ordering + `CancellationToken` propagation. Reordering phases causes in-flight request loss or leaked tasks.                                                                                                  |
| `src/apply_pool.rs` + `src/apply_worker.rs`         | Apply pipeline between consensus and state. Routing / ordering bugs = lost writes.                                                                                                                                            |
| `src/node_registry.rs`                              | `NodeConnectionRegistry` — the single per-node channel pool. Incorrect sharing = connection storm.                                                                                                                            |
| `src/rate_limit.rs`                                 | 3-tier token bucket. Atomic fields must stay `AtomicU64` for runtime reconfig via `RuntimeConfigHandle`.                                                                                                                      |
| `src/leader_lease.rs` + `src/leader_transfer.rs`    | Leader orchestration layer on top of consensus primitives.                                                                                                                                                                    |
| `src/state_checkpointer.rs`                         | Per-region checkpointer. Drives `Database::sync_state` on BOTH state.db and raft.db; runtime-reconfigurable via `RuntimeConfigHandle`; skipping raft.db leaves `applied_durable = 0` and forces full WAL replay on next boot. |
| `src/log_storage/raft_impl.rs` (`replay_crash_gap`) | Crash-gap recovery driver. MUST run before the region's apply worker is spawned — concurrent replay + live apply double-applies entries.                                                                                      |

## Owned Surface

- **Connection pool**: `NodeConnectionRegistry` (`node_registry.rs`) — one `tonic::Channel` per peer, HTTP/2-multiplexed across every subsystem.
- **Apply pipeline**: `ApplyPool` + `ApplyWorker` — bounded worker pool applying committed proposals to `StateLayer`. Apply-path admin arms call `StateLayer::apply_operations_lazy` (lazy commit); out-of-apply-pipeline admin / recovery callers call `StateLayer::apply_operations` (strict-durable). `LedgerRequest::IngestExternalEvents` is the apply-path variant for external `IngestEvents` RPC traffic — routed through REGIONAL Raft with event IDs frozen pre-consensus, applied via `EventWriter::write_events` (`commit_in_memory`).
- **Handler-phase event batching**: `EventHandle::record_handler_event` enqueues into a bounded `FlushQueue`; a single `EventFlusher` task per `EventHandle` drains on time / size / shutdown trigger and commits via `commit_in_memory` — durability is realised by the `StateCheckpointer`'s per-tick events.db sync, the same contract as apply-phase events. `EventHandle::flush_for_shutdown` is invoked from `GracefulShutdown` Phase 5b between WAL flush (5a) and `sync_all_state_dbs` (5c); Phase 5c is what fsyncs the drained batches before exit. `EventWriterBatchConfig::enabled = false` bypasses the queue entirely, routes emissions through `EventWriter::write_entry` directly, and restores per-emission fsync semantics — see [`event_writer.rs`](src/event_writer.rs) and `docs/architecture/durability.md` § "Handler-phase event flush window".
- **Transport**: `consensus_transport/` (`mod.rs`, `peer_sender.rs`) — backpressure-aware peer messaging for consensus traffic.
- **Saga orchestration**: `SagaOrchestrator` (`saga_orchestrator.rs`).
- **Coordination**: `RaftManager` (`raft_manager.rs`), `ConsensusHandle` (`consensus_handle.rs`), leader orchestration (`leader_lease.rs`, `leader_transfer.rs`).
- **Rate limit + hot-key**: `RateLimiter` (`rate_limit.rs`), `HotKeyDetector` (`hot_key_detector.rs`).
- **Graceful shutdown**: `GracefulShutdown` + `ShutdownCoordinator` (`graceful_shutdown.rs`); `RaftManager::sync_all_state_dbs` invoked from `pre_shutdown` after WAL flush to sync state.db + raft.db per region.
- **Durability lifecycle**: `StateCheckpointer` (`state_checkpointer.rs`) + `RaftLogStore::replay_crash_gap` (`log_storage/raft_impl.rs`) — driven by `CheckpointConfig` in `types`; operator reference in `docs/architecture/durability.md`.
- **Background jobs** (crate root): `auto_recovery`, `backup`, `btree_compaction`, `block_compaction`, `post_erasure_compaction`, `integrity_scrubber`, `learner_refresh`, `organization_purge`, `events_gc`, `ttl_gc`, `orphan_cleanup`, `invite_maintenance`, `token_maintenance`, `user_retention`, `dek_rewrap`, `BackgroundJobWatchdog`.
- **Observability**: `metrics.rs`, `otel.rs`, `dogstatsd.rs`, `logging/`, `trace_context.rs`.

## Test Patterns

- Unit tests use in-memory state + `RuntimeConfigHandle` injection for rate-limit / hot-key parameter variation.
- Background job lifecycle tests verify metrics emission + lifecycle log lines — not just that the job runs.
- Saga tests use the in-memory state layer; residency is asserted via the `state` crate's proptests.

## Local Golden Rules

1. **Saga PII persists only at `_tmp:saga_pii:{saga_id}`** (Regional + Temporary). PII written to `_meta:saga:` or any GLOBAL key is a residency bug. Enforced by `data-residency-auditor`.
2. **Apply workers never mutate state directly.** They route committed proposals through `StateLayer`. Bypassing the apply pipeline (raw backend writes in a job) corrupts state-machine determinism across replicas.
3. **Graceful shutdown phases are ordered.** The `CancellationToken` propagates through phases 1→6 in sequence; reordering (e.g., draining before closing the listener) leaks tasks or drops in-flight requests.
4. **`NodeConnectionRegistry` is the only place to create peer `tonic::Channel`s.** Every subsystem (consensus, discovery, admin, saga) shares channels via this registry — HTTP/2 multiplexes them all through one TCP connection per peer. Per-subsystem channels cause connection storms.
5. **Rate limiter + hot-key detector atomic fields are `AtomicU64`.** Non-atomic fields can't be reconfigured via `RuntimeConfigHandle` at runtime.
6. **Client routing is redirect-only.** `RegionalProposal` is the only server-to-server forwarding RPC and exists solely for saga orchestration (root rule 11). Don't extend it.
7. **Service-layer helpers live in `crates/services/src/services/`, not here.** `helpers.rs`, `metadata.rs`, `error_details.rs`, `error_classify.rs` are in `services`. Don't introduce a `src/services/` subdirectory under raft.
8. **`HotKeyDetector` is observational only.** It exposes Prometheus metrics; it does not reject traffic. Adding rejection logic tied to hot-key output is a different RFC.
9. **Background jobs emit lifecycle logs + metrics.** A job that silently runs is a debugging nightmare — every job needs `info!("<job> starting")` / `"<job> finished"` + a Prometheus counter.
10. **Deadlines propagate via `deadline.rs`.** Fire-and-forget internal paths are explicit exceptions (documented inline); defaulting to no deadline causes request pileups under load.
11. **`RaftLogStore::replay_crash_gap` runs BEFORE the apply worker is spawned.** A live apply worker during replay would double-apply entries (the WAL range is already in-flight through the replay path). Enforced by `start_region` call ordering in `raft_manager.rs` — reordering that site is a correctness bug.
12. **`StateCheckpointer` syncs BOTH state.db and raft.db on every tick.** Raft.db holds `AppliedStateCore` / `applied_durable`; skipping its sync means post-clean-shutdown restarts read `applied_durable = 0` and replay the entire WAL. Same invariant holds for `sync_all_state_dbs` in `RaftManager`. Both sync targets are enumerated in the code; adding a third DB to the durability contract requires updating every checkpoint / shutdown path in lockstep.
