# Durability

Reference for Ledger's write-durability contract, crash-recovery behavior, and the `StateCheckpointer` tuning knobs introduced in Sprint 1B2, extended in Sprint 1B3, and augmented in Sprint 1B4 with a separate handler-phase event flusher.

## Overview

A successful `WriteSuccess` response from Ledger means the write is **WAL-durable** — the consensus WAL has fsync'd the batch containing this entry (per root golden rule 10). It does **not** imply that the four state DBs (`state.db`, `raft.db`, `blocks.db`, `events.db`) have fsync'd the corresponding pages. Materialization is lazy for every persistent write on the hot apply path: applies land in each DB's in-process page cache via `WriteTransaction::commit_in_memory`, and the `StateCheckpointer` background task drives the dual-slot `persist_state_to_disk` fsync on a configurable cadence. On crash, state is re-derived by replaying `(applied_durable, last_committed]` from the WAL through the normal apply pipeline — the replay is idempotent by design.

Sprint 1A profiling showed per-entry dual-slot fsync was 62% of p50 write latency (~26ms of 41.6ms). Sprint 1B2 amortized state.db / raft.db fsyncs; Sprint 1B3 extended the same amortization to blocks.db and events.db. Sprint 1B4 amortized the remaining per-emission fsync on the **handler-phase event** path behind a 100ms flush window, trading a bounded post-crash loss window for ~42% CPU reduction on the event-emission hot path under concurrency. Steady-state durability is preserved via periodic checkpoints and the handler-event flusher; correctness under crash is preserved via WAL replay for everything WAL-backed, and via the flush window for the handler-phase event path that has no WAL backstop.

This doc is for operators tuning the latency / recovery-window trade-off, and for anyone diagnosing post-crash state or investigating a widening crash gap.

### Three event-emission paths — three durability contracts

Sprint 1B4 makes the event path three-way, not one-way. Each has a distinct contract:

- **Apply-phase events** (emitted inside the apply handler, driven by Raft committed entries; `EventWriter::write_events` via `commit_in_memory`) — WAL-durable on response, materialized lazily to `events.db` via the same checkpoint contract as state.db. Rebuilt deterministically on crash via `replay_crash_gap`. Unchanged from Sprint 1B3.
- **External ingest events** (`IngestEvents` RPC; routed as `LedgerRequest::IngestExternalEvents` through REGIONAL Raft with IDs frozen pre-consensus) — WAL-durable on response. Rebuilt byte-identical on crash. Unchanged from Sprint 1B3.
- **Handler-phase events** (emitted inline from RPC / admin / job / saga handlers via `RequestContext::record_event`, `JobContext::record_event`, or direct `EventHandle::record_handler_event` calls) — **NEW in Sprint 1B4**: enqueued into a bounded in-memory channel on emission; the `EventFlusher` background task batches emissions and commits once per flush window (default 100ms). There is no WAL backstop for this path, so a crash between enqueue and the next flush loses the queued entries permanently. Pre-1B4 this path was synchronous strict-durable `commit()` per emission.

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

## Handler-phase event flush window

Sprint 1B4 introduced a bounded in-memory queue + background flusher for the handler-phase event path. Handlers enqueue `EventEntry` values via `EventHandle::record_handler_event`; a single `EventFlusher` task per `EventHandle` drains the queue and commits once per flush window. Each drained batch is fsync'd through `EventWriter::write_entry`'s `commit()` — one fsync per batch, not per event.

### What changed

- **Pre-1B4:** `RequestContext::record_event` → synchronous `EventWriter::write_entry` → fsync → return. The RPC handler blocked on the fsync; a successful `Response` to the client implied every handler-phase event emitted during the request was fsync'd on `events.db`. A SIGKILL immediately after `Response` preserved the events.
- **Post-1B4:** `RequestContext::record_event` → `FlushQueue::try_send` (lock-free enqueue) → return. The handler builds and sends its `Response` while the event is still in-memory. The `EventFlusher` drains the queue on a **time / size / shutdown** trigger (whichever fires first) and fsyncs the drained batch. A SIGKILL between enqueue and the next flush loses the queued entries.

The contract shifted from "fsync before handler returns success" to "enqueued before handler returns success; fsync within one flush window." Default window is **100ms**.

### Trigger types

The flusher fires on **any** of three triggers:

- **Time** — `flush_interval_ms` since the last flush (default 100ms).
- **Size** — queue depth reaches `flush_size_threshold` (default 1,000 entries).
- **Shutdown** — `EventHandle::flush_for_shutdown(timeout)` is invoked from Phase 5b of `GracefulShutdown`. The flusher drains the queue in one final `commit()` and exits.

Each trigger emits its own label on `ledger_event_flush_triggers_total{trigger=time|size|shutdown}`, so operators can distinguish steady-state cadence flushes from load-driven backpressure flushes.

### What losing the flush window looks like

- **SIGKILL / panic / OOM between enqueue and the next flush tick** — every handler-phase event in the queue at the moment of death is lost. Post-restart, those events are **not** replayed (no WAL backstop). The events do not appear in `events.db`; downstream consumers scanning the audit trail see the request's WAL-backed state transitions without the corresponding handler-phase audit rows.
- **Worst case:** `flush_interval_ms` worth of handler-phase events (default: the last 100ms). Apply-phase events emitted during the same window are unaffected — they come out of `replay_crash_gap` deterministically.
- **Clean shutdown (SIGTERM):** zero events lost. Phase 5b of `GracefulShutdown` invokes `EventHandle::flush_for_shutdown(5s)` between the WAL flush (Phase 5a) and `sync_all_state_dbs` (Phase 5c). The flusher drains the queue and fsyncs the final batch before exit.

### Tuning

All fields of `EventWriterBatchConfig` except `queue_capacity` are runtime-reconfigurable via the `UpdateConfig` RPC. See [configuration.md § Handler-Phase Event Batching](configuration.md#handler-phase-event-batching) for field semantics, defaults, and a full `UpdateConfig` example.

- **Tighter window** (`flush_interval_ms=50`, `flush_size_threshold=500`) — narrower loss window at the cost of more fsyncs per second. Appropriate when the audit trail's recency guarantees matter and disk headroom allows.
- **Looser window** (`flush_interval_ms=250`) — fewer fsyncs, wider loss window. Appropriate on low-write-volume nodes where fsync pressure hurts p99 tail more than a wider audit-loss budget does.
- **Queue capacity** (`queue_capacity`) — not hot-reloadable (a bounded `mpsc` channel's capacity is fixed at construction). Change requires a restart. Raise it if `ledger_event_overflow_total{cause=queue_full}` is non-zero under normal load.
- **Overflow behavior** — `Drop` (default, matches the pre-1B4 "best-effort on commit failure" semantics) or `Block`. Set to `Block` when no audit-loss tolerance is acceptable; understand that a full queue then backpressures the handler thread until space frees up.

### Escapes hatch — strict synchronous durability (pre-1B4 semantics)

Deployments with a compliance posture that cannot accept a 100ms audit-loss window can disable batching entirely:

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"event_writer_batch\":{\"enabled\":false}}"
}' localhost:9090 ledger.v1.AdminService/UpdateConfig
```

With `enabled = false`:

- `record_handler_event` bypasses the queue and calls `EventWriter::write_entry` directly. Each emission fsyncs before the handler returns its `Response`. Durability semantics are identical to pre-1B4.
- The flusher task continues running but receives nothing; its `ledger_event_flush_triggers_total` counters go quiet.
- **Cost:** Sprint 1B4 measured ~42% of active CPU on `concurrent-writes @ 32` attributed to the handler-phase fsync path pre-batching (serialized on the `events.db` write-txn mutex). Flipping `enabled = false` reinstates that cost. Expect throughput to fall back toward the pre-1B4 ~50 ops/s ceiling under similar load; p99 handler latency widens by one fsync per emission.

Flip the switch via `UpdateConfig` rather than restarting with a different config — the change is audit-logged and takes effect on the next emission (the flusher re-reads from `RuntimeConfigHandle` per dequeue).

### Pre-1B4 best-effort caveat — quantified

The pre-1B4 `record_handler_event` implementation was already best-effort on **commit failure**: if the synchronous `commit()` returned an error (disk full, transient IO error, corrupted page), the event was logged at `warn!` and swallowed — the RPC still returned success to the client. Pre-1B4 operators who believed they had absolute durability on handler-phase events were already exposed to silent drops on any transient disk error.

Sprint 1B4 widens this window from "best-effort on commit failure" to "best-effort within a 100ms flush window on crash" — a quantitative widening, not a new category of loss. The `Drop` overflow behavior on a full queue is the same shape as a pre-1B4 commit-error drop: logged, metered, and swallowed.

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
- **Durability of handler-phase audit events inside the flush window on crash.** Up to `flush_interval_ms` worth of handler-phase events can be lost on SIGKILL / panic / OOM. Clean shutdown drains the queue and loses zero. External-ingest and apply-phase events are WAL-backed and unaffected. See [handler-phase event flush window](#handler-phase-event-flush-window) and the `enabled=false` escape hatch for strict synchronous semantics.

### Strict-durable exceptions (kept on `commit`, not flipped)

The commit-durability audit (`docs/superpowers/specs/2026-04-19-commit-durability-audit.md`) classifies every production `write_txn.commit()` call site. Post-1B3, the WAL-durable-then-lazy contract applies to every hot-path apply-pipeline write. The paths that stayed strict-durable:

- **`RaftLogStore::save_vote`** — Raft election safety. A lost vote after crash could produce split-brain.
- **`RaftLogStore::install_snapshot`** — Receive-side of snapshot replication. Once the leader's WAL truncates past the installed snapshot's `last_applied`, losing the install means the follower has no WAL to replay.
- **`EventWriter::write_entry`** — Handler-phase single-entry writes called from the saga orchestrator / admin RPC handlers / `RequestContext` / `JobContext`. NOT in the Raft apply pipeline — no WAL replay backstop. Sprint 1B4 reclassified this path from synchronous strict-durable to **batched-durable within one flush window** (default 100ms). The flusher still uses `commit()` (not `commit_in_memory`), so each flushed batch is fsync'd once — durability is preserved *per batch*, not per emission. `write_entry` remains callable directly when `EventWriterBatchConfig::enabled = false` (per-emission fsync, pre-1B4 semantics) and on the shutdown-drain path. See [handler-phase event flush window](#handler-phase-event-flush-window).
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
- **Handler-phase events missing from `events.db` after crash** — expected if the crash was not a clean shutdown. Handler-phase events are not WAL-backed (see [handler-phase event flush window](#handler-phase-event-flush-window)); up to `flush_interval_ms` worth of emissions can be lost. Look at `ledger_event_flush_triggers_total` immediately before the crash timestamp: if the last flush was more than one `flush_interval_ms` before the fault, the queue held unflushed entries that are now gone. Apply-phase events and external-ingest events are unaffected — they replay from the WAL. On clean shutdown (`ledger_event_flush_triggers_total{trigger="shutdown"}` incremented for the region), zero handler-phase events lost.
- **`ledger_event_overflow_total{cause=shutdown_timeout} > 0`** — Phase 5b's `flush_for_shutdown(5s)` drain budget was exceeded; some queued events did not make it to disk. Raise the 5s timeout in `crates/server/src/main.rs` (Phase 5b) if the queue-at-shutdown depth is regularly above the flusher's drain rate.

## Sprint 1B2 / 1B3 / 1B4 Historical Note

The durability contract changed in Sprint 1B2 (April 2026), extended in Sprint 1B3, and gained a handler-phase batching path in Sprint 1B4. Design and audit artifacts:

- **1B2 design doc**: `docs/superpowers/specs/2026-04-19-sprint-1b2-apply-batching-design.md` — state.db / raft.db rationale, primitives, and classification rules.
- **1B3 design doc**: `docs/superpowers/specs/2026-04-19-sprint-1b3-blocks-events-batching-design.md` — blocks.db / events.db extension, external-ingest routing through Raft, and the `apply_operations` / `apply_operations_lazy` split.
- **1B4 design doc**: `docs/superpowers/specs/2026-04-19-sprint-1b4-handler-event-batching-design.md` — handler-phase event queue + flusher, Phase 5b drain, and the `enabled=false` escape hatch.
- **Commit-durability audit**: `docs/superpowers/specs/2026-04-19-commit-durability-audit.md` — the authoritative `commit` vs `commit_in_memory` classification, kept current as sprint work lands.
- **Simulation deferrals**: `docs/superpowers/specs/2026-04-19-task-3b-simulation-deferrals.md` — follow-up simulation scenarios not covered in Sprint 1B2.

Before Sprint 1B2, the apply-path `WriteTransaction::commit` invoked a full dual-slot persist (2 fsyncs) per call. Sprint 1B2 added `WriteTransaction::commit_in_memory` + `Database::sync_state` + `StateCheckpointer`, flipped two specific state.db / raft.db call sites, and introduced `replay_crash_gap` to recover the gap between `applied_durable` and `last_committed` on crash. Sprint 1B3 flipped the blocks.db apply-path commit (`BlockArchive::append_block`) and the events.db apply-path commit (`EventWriter::write_events`), routed external `IngestEvents` through Raft as `LedgerRequest::IngestExternalEvents`, extended `StateCheckpointer` / `sync_all_state_dbs` / `replay_crash_gap` to cover all four DBs, and split `StateLayer::apply_operations` into a durable entrypoint (admin / recovery) and a lazy entrypoint (`apply_operations_lazy`, used by all IN-APPLY-PIPELINE admin-request arms). Sprint 1B4 put the handler-phase path — which has no WAL backstop, so `commit_in_memory` is unavailable — behind a bounded queue + flusher that batches fsyncs within a 100ms window, added `EventHandle::flush_for_shutdown` invoked from `GracefulShutdown` Phase 5b to preserve zero-loss on clean shutdown, and introduced the `EventWriterBatchConfig::enabled = false` escape hatch that restores pre-1B4 per-emission fsync semantics for strict-compliance workloads.
