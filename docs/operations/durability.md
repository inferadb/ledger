# Durability

Reference for Ledger's write-durability contract, crash-recovery behavior, and the `StateCheckpointer` tuning knobs introduced in Sprint 1B2 and extended in Sprint 1B3.

## Overview

A successful `WriteSuccess` response from Ledger means the write is **WAL-durable** — the consensus WAL has fsync'd the batch containing this entry (per root golden rule 10). It does **not** imply that the four state DBs (`state.db`, `raft.db`, `blocks.db`, `events.db`) have fsync'd the corresponding pages. Materialization is lazy for every persistent write on the hot apply path: applies land in each DB's in-process page cache via `WriteTransaction::commit_in_memory`, and the `StateCheckpointer` background task drives the dual-slot `persist_state_to_disk` fsync on a configurable cadence. On crash, state is re-derived by replaying `(applied_durable, last_committed]` from the WAL through the normal apply pipeline — the replay is idempotent by design.

Sprint 1A profiling showed per-entry dual-slot fsync was 62% of p50 write latency (~26ms of 41.6ms). Sprint 1B2 amortized state.db / raft.db fsyncs; Sprint 1B3 extended the same amortization to blocks.db and events.db. Steady-state durability is preserved via periodic checkpoints; correctness under crash is preserved via WAL replay.

This doc is for operators tuning the latency / recovery-window trade-off, and for anyone diagnosing post-crash state or investigating a widening crash gap.

## The Durability Lifecycle

### Write path

```text
client write
  -> gRPC request
  -> Raft propose (consensus layer)
  -> WAL append + fsync      ← batched; one fsync per WAL batch (golden rule 10)
  -> WriteSuccess returned to client      ← durability promise starts here
  -> apply (commit_in_memory)             ← in-process; state visible to reads
  -> StateCheckpointer tick -> sync_state ← dual-slot fsync; state now durable
```

Time budget under default config: WAL fsync is sub-millisecond per batch; `commit_in_memory` is micros; the `StateCheckpointer` tick runs every 500ms (or sooner when apply-count / dirty-page thresholds fire).

### Clean shutdown path

```text
SIGTERM
  -> connection drain (6-phase GracefulShutdown)
  -> WAL flush
  -> RaftManager::sync_all_state_dbs              ← forces state.db + raft.db + blocks.db + events.db sync per region
  -> process exit
```

Post-restart: `RaftLogStore::replay_crash_gap` reads `applied_durable` from raft.db, diffs against WAL `last_committed`, and finds zero entries to replay. `ledger_state_recovery_replay_count_total` increments by 0 for the region (deliberate — dashboards plot `rate()` for restart frequency).

### Crash-recovery path

```text
SIGKILL / panic / OOM / disk loss
  -> process restart
  -> RaftLogStore::open -> load_caches (populates applied_durable from raft.db)
  -> RaftLogStore::replay_crash_gap
     - reads applied_durable (last durable apply index)
     - reads last_committed from WAL checkpoint
     - replays (applied_durable, last_committed] through apply_committed_entries
     - post-replay: sync_state on ALL 4 DBs (state.db + raft.db + blocks.db + events.db)
  -> region starts serving traffic
```

Replay is idempotent: every field reconstructed by `apply_committed_entries` is either CAS-idempotent (entity and relationship writes), monotonic-per-log-index (membership, sequences, region_height, previous_region_hash), or was made idempotent-by-height in Task 1C (`BlockArchive::append_block`). The recovery sweep runs **before** the region's apply worker is spawned — if both ran concurrently, the same entry would apply twice.

### External event ingestion (`IngestEvents` RPC)

Pre-Sprint-1B3, `EventsServiceImpl::ingest_events` wrote directly to `events.db` from the RPC handler and returned after the dual-slot fsync. That path was best-effort strict-durable: the RPC completed only after disk, but the write bypassed Raft entirely — crash between the fsync and the next state-checkpoint could still roll back anything the handler had not yet persisted.

Post-1B3, `IngestEvents` builds a `LedgerRequest::IngestExternalEvents` proposal (with event IDs frozen into the payload before consensus, see [apply-batching determinism](#apply-batching-determinism) below) and routes it through the REGIONAL Raft group via `ProposalService::propose_to_region`. The RPC returns after Raft commit + apply completes — the events are **WAL-durable** on response, with `events.db` materialization following the same lazy-then-checkpoint contract as state.db.

This is a strict **upgrade**. Before: a crash between handler-fsync and checkpoint could lose the last unsynced batch — up to the gap between RPC return and the next `StateCheckpointer` tick. After: every ingested batch that returned `WriteSuccess` is WAL-replayable, with the same durability class as any write.

## Tuning the StateCheckpointer

The checkpointer is the operator's primary durability knob. It fires when **any** of three thresholds is crossed:

| Field                   | Default | Range      | Unit     | Meaning                                                               |
| ----------------------- | ------- | ---------- | -------- | --------------------------------------------------------------------- |
| `interval_ms`           | 500     | [50, 60000] | ms       | Time since last successful checkpoint                                 |
| `applies_threshold`     | 5000    | >= 1       | applies  | Applies accumulated in memory since last checkpoint                   |
| `dirty_pages_threshold` | 10000   | >= 1       | pages    | Dirty pages in the state-DB page cache (~40 MB at 4 KB pages)         |

All three are runtime-reconfigurable via the `UpdateConfig` admin RPC; the `StateCheckpointer` re-reads from `RuntimeConfigHandle` on each wake-up, so changes take effect on the next tick.

### When to tighten (tighter recovery window, more fsync cost)

- High-throughput workloads where a 500ms recovery gap is unacceptable.
- Deployments with fast NVMe where dual-slot fsync cost is negligible.
- Regulatory environments requiring smaller in-flight data loss exposure.

Example (quarter-second interval, half-default counts):

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"state_checkpoint\":{\"interval_ms\":250,\"applies_threshold\":2500,\"dirty_pages_threshold\":5000}}"
}' localhost:9090 ledger.v1.AdminService/UpdateConfig
```

### When to loosen (lower fsync cost, wider recovery window)

- IO-bound nodes where fsyncs spike disk queue depth and hurt p99 tail latency.
- SSD deployments where write amplification is a long-term wear concern.
- Low-write-volume regions where 500ms cadence produces unnecessary idle fsyncs.

Example (two-second interval, ten-minute apply window):

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"state_checkpoint\":{\"interval_ms\":2000,\"applies_threshold\":20000,\"dirty_pages_threshold\":40000}}"
}' localhost:9090 ledger.v1.AdminService/UpdateConfig
```

### Do not

- **Do not set any threshold to zero.** Validation rejects `interval_ms < 50`, `applies_threshold < 1`, or `dirty_pages_threshold < 1`. The validation exists to prevent a zero-cadence checkpointer (which would pin a CPU) and a threshold-never-fires state (which would grow dirty pages unbounded until OOM).
- **Do not bypass the `UpdateConfig` RPC** by editing config on disk — there is no config file, and runtime values live in `ArcSwap<RuntimeConfig>`. Changes must flow through the admin surface so audit logging captures them.

See [configuration.md](configuration.md) for the `UpdateConfig` / `GetConfig` admin-RPC overview and the `RuntimeConfigHandle` architecture.

## Monitoring

Nine Prometheus metrics track checkpoint and recovery behavior. Full signatures in [metrics-reference.md](metrics-reference.md).

| Metric                                           | Type      | Labels                          | Purpose                                                  |
| ------------------------------------------------ | --------- | ------------------------------- | -------------------------------------------------------- |
| `ledger_state_checkpoints_total`                 | Counter   | `region`, `trigger`, `status`   | Checkpoint attempts; trigger ∈ `time`/`applies`/`dirty`/`snapshot`/`backup`/`shutdown` |
| `ledger_state_checkpoint_duration_seconds`       | Histogram | `region`, `trigger`             | Per-checkpoint fsync latency                             |
| `ledger_state_checkpoint_last_timestamp_seconds` | Gauge     | `region`                        | Unix timestamp of last successful checkpoint             |
| `ledger_state_applies_since_checkpoint`          | Gauge     | `region`                        | In-memory applies accumulated since last checkpoint      |
| `ledger_state_dirty_pages`                       | Gauge     | `region`                        | Dirty pages sampled at checkpointer wake-up              |
| `ledger_state_page_cache_len`                    | Gauge     | `region`                        | Total in-memory pages resident in the state-DB cache     |
| `ledger_state_last_synced_snapshot_id`           | Gauge     | `region`                        | Most recent snapshot id durably persisted to disk        |
| `ledger_state_recovery_replay_count_total`       | Counter   | `region`                        | WAL entries replayed on startup (fires once per region)  |
| `ledger_state_recovery_duration_seconds`         | Histogram | `region`                        | Duration of the post-open crash-recovery sweep           |

### Suggested Grafana panels

- Checkpoint rate per region: `rate(ledger_state_checkpoints_total{status="ok"}[5m])` grouped by `trigger`.
- Checkpoint fsync p50/p99: `histogram_quantile(0.50|0.99, ledger_state_checkpoint_duration_seconds_bucket)`.
- In-memory backlog: `ledger_state_applies_since_checkpoint` + `ledger_state_dirty_pages` on twin y-axes.
- Durable snapshot pointer: `ledger_state_last_synced_snapshot_id` per region — monotonically increasing; a flat line is a stalled checkpointer.
- Recovery replay window: `rate(ledger_state_recovery_replay_count_total[1h])` — spikes indicate unclean restarts.
- Recovery sweep latency: `histogram_quantile(0.99, ledger_state_recovery_duration_seconds_bucket)`.

### Suggested alerts

- **Wide crash-gap recovery**: `histogram_quantile(0.99, ledger_state_recovery_duration_seconds_bucket) > 10s` for 5 minutes. Indicates the checkpoint window was too wide before the crash; consider tightening thresholds.
- **Slow disk**: `histogram_quantile(0.99, ledger_state_checkpoint_duration_seconds_bucket) > 100ms` for 10 minutes. Disk or cgroup-IO contention; investigate before it blocks apply throughput.
- **Checkpointer stalled**: `ledger_state_dirty_pages` growing monotonically for > 30 minutes, with no corresponding growth in `ledger_state_checkpoints_total`. The checkpointer is failing or not scheduling; check error logs and `ledger_state_checkpoints_total{status="error"}`.
- **Clean-shutdown audit**: `ledger_state_recovery_replay_count_total > 0` immediately after a planned restart. Indicates `sync_all_state_dbs` did not run or timed out in `pre_shutdown`.

## Contract — What It Promises, What It Does Not

### Promises

- Any `WriteSuccess` response implies WAL durability. The write survives crash and is reconstructed on next startup via `replay_crash_gap`. This applies to all four regional DBs — state.db, raft.db, blocks.db, and events.db — post-1B3.
- Reads after a `WriteSuccess` on the **same node** observe the write immediately (committed to the in-process page cache via `commit_in_memory`).
- Snapshots shipped to followers (via `LedgerSnapshotBuilder::build_snapshot` or `RaftLogStore::get_current_snapshot`) force `sync_state` at the top so the captured `last_applied` reflects durable disk state.
- `IngestEvents` RPC returns only after the external event batch is WAL-committed. A crash between return and the next checkpoint replays the events from the WAL — no data loss, no best-effort-fsync window.

### Does not promise

- Immediate state-DB fsync on `WriteSuccess`. A client that reads from a **different node** immediately after its write may not see the write until that node's apply pipeline + checkpointer catches up.
- Durability of state / blocks / events DB pages between checkpoints. On crash, each DB is rolled back to `applied_durable` and re-derived from WAL — which is correct but not instant.
- **Byte-identical block layout or apply-phase event IDs across a crash-recovery boundary.** See [apply-batching determinism](#apply-batching-determinism) below.

### Strict-durable exceptions (kept on `commit`, not flipped)

The commit-durability audit (`docs/superpowers/specs/2026-04-19-commit-durability-audit.md`) classifies every production `write_txn.commit()` call site. Post-1B3, the WAL-durable-then-lazy contract applies to every hot-path apply-pipeline write. The paths that stayed strict-durable:

- **`RaftLogStore::save_vote`** — Raft election safety. A lost vote after crash could produce split-brain.
- **`RaftLogStore::install_snapshot`** — Receive-side of snapshot replication. Once the leader's WAL truncates past the installed snapshot's `last_applied`, losing the install means the follower has no WAL to replay.
- **`EventWriter::write_entry`** — Handler-phase single-entry writes called from the saga orchestrator / admin RPC handlers / `RequestContext` / `JobContext`. NOT in the Raft apply pipeline — no WAL replay backstop, so strict-durable fsync is the only guarantee.
- **`StateLayer::apply_operations`** (admin / recovery callers only) — `AdminService::recover_vault`, `check_integrity` temp-state replay, `ReadService` historical replay, `AutoRecoveryJob::replay_vault_from_blocks`. IN-APPLY-PIPELINE admin arms use `apply_operations_lazy` (see the audit's Task 2E addendum).
- **`BlockArchive::compact_before`** — Background block compaction writing condensed blocks + watermark. Infrequent; fsync cost amortized.
- **`EventsGc::tick_inner`** — TTL-GC sweep. Background cadence; strict-durable so GC progress survives crashes.
- **Backup-producing paths** — `CreateBackup` must not return success for data that isn't on disk. `BackupManager::create_incremental_backup` / `create_full_page_backup` force `sync_state` before assembling the backup.

### Apply-batching determinism

A subtlety Phase 3 integration tests surfaced. Under steady-state single-client load, each pre-crash proposal lands in its **own** apply batch. Post-crash, `replay_crash_gap` reads ALL pending WAL entries in `(applied_durable, last_committed]` and feeds them as a **single** batch through `apply_committed_entries`. Because block assembly (`vault_entries` per block) and apply-phase event ID derivation (UUID v5 from `(block_height, op_index, action)`) are functions of the batch boundaries, two kinds of artifacts can differ across a crash-recovery boundary:

- **Block layout** — a sequence of writes that produced N blocks before crash can collapse into fewer, denser blocks after replay. The Merkle chain is still valid; the verifiable-audit property is preserved; `vault_entries` per block can differ.
- **Apply-phase event IDs** — events generated inside the apply handler (UUID v5 over `block_height`, `op_index`, `action`) are re-derived on replay. If `block_height` or `op_index` shifts because the batch boundary shifted, the event IDs shift with them.

**What does NOT shift:** the payload data itself (every entity op, every relationship tuple, every authorization record) is byte-identical. The WAL guarantees every committed entry is preserved.

**External ingest event IDs are frozen.** `IngestEvents`-sourced event IDs are UUID v4 generated at RPC entry and baked into the `LedgerRequest::IngestExternalEvents` payload **before** consensus. They survive replay byte-identical and are the safe ID path for external consumers that need stable IDs across crash-recovery boundaries.

If your consumer needs stable IDs post-crash, use external ingestion (UUID v4 frozen pre-Raft). If you rely on apply-phase event IDs, budget for ID shifts under crash-recovery and key downstream joins by `(entity_id, block_height_range)` rather than by event ID.

## Recovery Runbook

On region startup, `RaftLogStore::replay_crash_gap` runs before the apply worker is spawned. It replays every WAL entry in `(applied_durable, last_committed]` through `apply_committed_entries` and then syncs all four regional DBs (state.db, raft.db, blocks.db, events.db) so the recovered state is durable before traffic starts. It logs one line at `info` level:

```text
Crash-recovery replay complete
  region=... replayed_entries=N applied_durable=X last_committed=Y duration_ms=Z
```

- **`replayed_entries == 0`** — Clean restart (or fresh boot). No action needed. The metric `ledger_state_recovery_replay_count_total` increments by 0 for this region — deliberate, so dashboards can plot restart frequency via `rate()`.
- **`replayed_entries > 0`** — Unclean restart. Recovery is idempotent; no action needed unless the replay count is unusually large (see below). Check `ledger_state_recovery_duration_seconds` — a narrow gap (< 1s) is routine; a wide gap (> 10s) warrants investigation.
- **Recovery fails** — The region will **not** start serving. The error is logged with full context. Common causes: disk full (check available space against WAL segment size), corrupted `raft.db` (restore from snapshot + WAL replay), or `state.db` integrity check failure (consider rebuilding from a peer via `InstallSnapshot`).
- **Recovery duration unusually long** — The checkpoint window was too wide before the crash. If this recurs, tighten `StateCheckpointer` thresholds via `UpdateConfig`. Measure steady-state replay duration after the change to confirm.
- **Post-restart clients see stale reads** — If the node was a follower during the crash, it may be lagging the leader. Wait for the follower to catch up (watch `ledger_state_last_synced_snapshot_id` advance) before directing reads at it.
- **`ledger_state_recovery_replay_count_total > 0` after clean shutdown** — `sync_all_state_dbs` did not run or timed out. Check the graceful-shutdown timeout (`pre_shutdown_timeout_secs`) against observed sync latency, and the shutdown log for `sync_all_state_dbs: final state-DB sync ... timed out`.

## Sprint 1B2 / 1B3 Historical Note

The durability contract changed in Sprint 1B2 (April 2026) and extended in Sprint 1B3. Design and audit artifacts:

- **1B2 design doc**: `docs/superpowers/specs/2026-04-19-sprint-1b2-apply-batching-design.md` — state.db / raft.db rationale, primitives, and classification rules.
- **1B3 design doc**: `docs/superpowers/specs/2026-04-19-sprint-1b3-blocks-events-batching-design.md` — blocks.db / events.db extension, external-ingest routing through Raft, and the `apply_operations` / `apply_operations_lazy` split.
- **Commit-durability audit**: `docs/superpowers/specs/2026-04-19-commit-durability-audit.md` — the authoritative `commit` vs `commit_in_memory` classification, kept current as sprint work lands.
- **Simulation deferrals**: `docs/superpowers/specs/2026-04-19-task-3b-simulation-deferrals.md` — follow-up simulation scenarios not covered in Sprint 1B2.

Before Sprint 1B2, the apply-path `WriteTransaction::commit` invoked a full dual-slot persist (2 fsyncs) per call. Sprint 1B2 added `WriteTransaction::commit_in_memory` + `Database::sync_state` + `StateCheckpointer`, flipped two specific state.db / raft.db call sites, and introduced `replay_crash_gap` to recover the gap between `applied_durable` and `last_committed` on crash. Sprint 1B3 flipped the blocks.db apply-path commit (`BlockArchive::append_block`) and the events.db apply-path commit (`EventWriter::write_events`), routed external `IngestEvents` through Raft as `LedgerRequest::IngestExternalEvents`, extended `StateCheckpointer` / `sync_all_state_dbs` / `replay_crash_gap` to cover all four DBs, and split `StateLayer::apply_operations` into a durable entrypoint (admin / recovery) and a lazy entrypoint (`apply_operations_lazy`, used by all IN-APPLY-PIPELINE admin-request arms).
