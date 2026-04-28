# Durability

Reference for Ledger's write-durability contract, crash-recovery behavior, and the `StateCheckpointer` tuning knobs across all four regional DBs, plus the handler-phase event flusher.

## Overview

A successful `WriteSuccess` response from Ledger means the write is **WAL-durable** — the consensus WAL has fsync'd the batch containing this entry (per root golden rule 10). It does **not** imply that the four state DBs (`state.db`, `raft.db`, `blocks.db`, `events.db`) have fsync'd the corresponding pages. Materialization is lazy for every persistent write on the hot apply path: applies land in each DB's in-process page cache via `WriteTransaction::commit_in_memory`, and the `StateCheckpointer` background task drives the dual-slot `persist_state_to_disk` fsync on a configurable cadence. On crash, state is re-derived by replaying `(applied_durable, last_committed]` from the WAL through the normal apply pipeline — the replay is idempotent by design.

Per-entry dual-slot fsync on the apply path is expensive (historically ~62% of p50 write latency on the pre-amortization code). All four storage DBs (state.db, raft.db, blocks.db, events.db) now amortize fsyncs via `commit_in_memory` + a periodic `StateCheckpointer`. The handler-phase event path runs behind a bounded queue + background flusher that also commits in-memory, so handler-phase events reach disk on the same `StateCheckpointer` cadence as every other apply-path write. The crash-loss window for the handler-phase path is bounded by the checkpoint cadence (default ~500ms) — still WAL-less for the emission itself, but in the same durability class as apply-phase events. Steady-state durability is preserved via periodic checkpoints; correctness under crash is preserved via WAL replay for everything WAL-backed, and via the checkpoint cadence for the handler-phase path.

### Storage layout post-Phase-5 (per-org WAL, per-vault state)

Under per-vault consensus, the durability contract is layered: each `(region, organization_id)` owns one shared per-org WAL + raft.db at `{data_dir}/{region}/{org}/`, and each `(region, organization_id, vault_id)` owns its own per-vault state.db / blocks.db / events.db at `{data_dir}/{region}/{org}/state/vault-{vault_id}/`. Every vault group is a shard on the parent org's `ConsensusEngine`; vault apply tasks share the org's WAL and group-commit fsync (the M4 footing for `replay_shared_wal_for_org` parallel replay), but each vault's state materialization is independent. `RaftManager::sync_all_state_dbs` walks every `(region, org_id, vault_id)` in `vault_groups` plus every `(region, org_id)` in `regions`, fsyncing each per-vault state.db / blocks.db / events.db and the shared per-org raft.db. The crash-recovery story is unchanged in shape — `RaftLogStore::replay_crash_gap` reads `applied_durable` from raft.db and replays `(applied_durable, last_committed]` from the shared WAL, fanning out per-vault apply on each entry — but the I/O fan-out is wider.

This doc is for operators tuning the latency / recovery-window trade-off, and for anyone diagnosing post-crash state or investigating a widening crash gap.

### Three event-emission paths — three durability contracts

The event path is three-way, not one-way. Each has a distinct contract:

- **Apply-phase events** (emitted inside the apply handler, driven by Raft committed entries; `EventWriter::write_events` via `commit_in_memory`) — WAL-durable on response, materialized lazily to `events.db` via the same checkpoint contract as state.db. Rebuilt deterministically on crash via `replay_crash_gap`.
- **External ingest events** (`IngestEvents` RPC; routed as `LedgerRequest::IngestExternalEvents` through REGIONAL Raft with IDs frozen pre-consensus) — WAL-durable on response. Rebuilt byte-identical on crash.
- **Handler-phase events** (emitted inline from RPC / admin / job / saga handlers via `RequestContext::record_event`, `JobContext::record_event`, or direct `EventHandle::record_handler_event` calls) — batched and amortized on the StateCheckpointer cadence: enqueued into a bounded in-memory channel on emission; the `EventFlusher` background task drains the queue on a time / size / shutdown trigger and commits via `commit_in_memory`. Durability lands on the next `StateCheckpointer` tick — same contract as apply-phase events, except the emission itself is not WAL-backed, so a crash before the next checkpoint still loses the unflushed-and-uncheckpointed entries.

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

Replay is idempotent: every field reconstructed by `apply_committed_entries` is either CAS-idempotent (entity and relationship writes), monotonic-per-log-index (membership, sequences, region_height, previous_region_hash), or idempotent-by-height (`BlockArchive::append_block`). The recovery sweep runs **before** the region's apply worker is spawned — if both ran concurrently, the same entry would apply twice.

### External event ingestion (`IngestEvents` RPC)

`IngestEvents` builds a `LedgerRequest::IngestExternalEvents` proposal (with event IDs frozen into the payload before consensus, see [apply-batching determinism](#apply-batching-determinism) below) and routes it through the REGIONAL Raft group via `ProposalService::propose_to_region`. The RPC returns after Raft commit + apply completes — the events are **WAL-durable** on response, with `events.db` materialization following the same lazy-then-checkpoint contract as state.db. Every ingested batch that returned `WriteSuccess` is WAL-replayable, with the same durability class as any other apply-path write.

## Handler-phase event flush window

The handler-phase event path uses a bounded in-memory queue + background flusher. Handlers enqueue `EventEntry` values via `EventHandle::record_handler_event`; a single `EventFlusher` task per `EventHandle` drains the queue and commits once per flush window via `commit_in_memory`. Durability is realized by the per-region `StateCheckpointer`'s `sync_state` sweep on events.db (part of the 4-DB sweep), not by per-flush fsync. The handler-phase path is in the same durability class as apply-phase events (checkpoint-covered, WAL-less for the emission itself) rather than a per-batch-fsync path.

### Write sequence

`RequestContext::record_event` → `FlushQueue::try_send` (lock-free enqueue) → return. The handler builds and sends its `Response` while the event is still in-memory. The `EventFlusher` drains the queue on a **time / size / shutdown** trigger (whichever fires first) and commits the drained batch via `commit_in_memory` — the batch lands in the events.db page cache and returns. The on-disk persist happens on the next `StateCheckpointer` tick (default ~500ms), not inside the flusher. A SIGKILL between enqueue and the next checkpoint loses the queued-or-flushed-but-uncheckpointed entries.

The contract is: "enqueued before handler returns success; on disk within one StateCheckpointer tick." Default StateCheckpointer interval is **~500ms**.

### Trigger types

The flusher fires on **any** of three triggers:

- **Time** — `flush_interval_ms` since the last flush (default 100ms).
- **Size** — queue depth reaches `flush_size_threshold` (default 1,000 entries).
- **Shutdown** — `EventHandle::flush_for_shutdown(timeout)` is invoked from Phase 5b of `GracefulShutdown`. The flusher drains the queue via `commit_in_memory` and exits; Phase 5c's `sync_all_state_dbs` then fsyncs events.db alongside the other three DBs before the process exits.

Each trigger emits its own label on `ledger_event_flush_triggers_total{trigger=time|size|shutdown}`, so operators can distinguish steady-state cadence flushes from load-driven backpressure flushes.

### What losing the flush window looks like

- **SIGKILL / panic / OOM between enqueue and the next `StateCheckpointer` tick** — every handler-phase event that was enqueued-but-not-yet-checkedpointed is lost. Post-restart, those events are **not** replayed (no WAL backstop). The events do not appear in `events.db`; downstream consumers scanning the audit trail see the request's WAL-backed state transitions without the corresponding handler-phase audit rows.
- **Worst case:** `state_checkpoint.interval_ms` worth of handler-phase events (default: the last ~2s), plus anything still in the flusher's in-memory queue. Apply-phase events emitted during the same window are unaffected — they come out of `replay_crash_gap` deterministically.
- **Clean shutdown (SIGTERM):** zero events lost. Phase 5b of `GracefulShutdown` invokes `EventHandle::flush_for_shutdown(5s)` between the WAL flush (Phase 5a) and `sync_all_state_dbs` (Phase 5c). The flusher drains the queue (in-memory commit) and Phase 5c's `sync_all_state_dbs` then fsyncs events.db alongside state.db / raft.db / blocks.db before exit.

### Tuning

All fields of `EventWriterBatchConfig` except `queue_capacity` are runtime-reconfigurable via the `UpdateConfig` RPC. See [configuration.md § Handler-Phase Event Batching](../reference/configuration.md#handler-phase-event-batching) for field semantics, defaults, and a full `UpdateConfig` example.

- **Tighter loss window** — the StateCheckpointer is the dominant loss-window knob, not `flush_interval_ms`. Narrow the window via `state_checkpoint.interval_ms` (floor 50ms); see [Tuning the StateCheckpointer](#tuning-the-statecheckpointer) above. `flush_interval_ms` still controls how quickly enqueued events reach the events.db page cache, which is the read-after-write visibility window for `list_events` queries issued against the same node.
- **Looser window** — raise `state_checkpoint.interval_ms` and/or `flush_interval_ms`. Wider windows cut fsyncs per second; the trade-off is a wider post-crash audit-loss window and slower read-after-write visibility.
- **Queue capacity** (`queue_capacity`) — not hot-reloadable (a bounded `mpsc` channel's capacity is fixed at construction). Change requires a restart. Raise it if `ledger_event_overflow_total{cause=queue_full}` is non-zero under normal load.
- **Overflow behavior** — `Drop` (default; matches "best-effort on commit failure" semantics) or `Block`. Set to `Block` when no audit-loss tolerance is acceptable; understand that a full queue then backpressures the handler thread until space frees up.

### Escape hatch — strict synchronous durability

Deployments with a compliance posture that cannot accept any handler-phase audit-loss window can disable batching entirely and restore a per-emission fsync contract:

```bash
grpcurl -plaintext -d '{
  "config_json": "{\"event_writer_batch\":{\"enabled\":false}}"
}' localhost:9090 ledger.v1.AdminService/UpdateConfig
```

With `enabled = false`:

- `record_handler_event` bypasses the queue and calls `EventWriter::write_entry` directly. Each emission fsyncs before the handler returns its `Response`.
- The flusher task continues running but receives nothing; its `ledger_event_flush_triggers_total` counters go quiet.
- **Cost:** the handler-phase fsync path (serialized on the `events.db` write-txn mutex) has historically measured ~42% of active CPU on `concurrent-writes @ 32`. Flipping `enabled = false` reinstates that cost. Expect throughput to fall back toward a ~50 ops/s ceiling under similar load; p99 handler latency widens by one fsync per emission.

Flip the switch via `UpdateConfig` rather than restarting with a different config — the change is audit-logged and takes effect on the next emission (the flusher re-reads from `RuntimeConfigHandle` per dequeue).

### Best-effort caveat — quantified

Even in the strict-synchronous (`enabled = false`) mode, `record_handler_event` is best-effort on **commit failure**: if the synchronous `commit()` returns an error (disk full, transient IO error, corrupted page), the event is logged at `warn!` and swallowed — the RPC still returns success to the client. Operators cannot assume absolute durability on handler-phase events even in strict mode: transient disk errors produce silent drops.

With batching enabled (the default), the best-effort window widens to "one StateCheckpointer interval on crash" (default ~500ms, floor 50ms). This is a quantitative widening of the same best-effort category, not a new category of loss. The `Drop` overflow behavior on a full queue is the same shape as a commit-error drop: logged, metered, and swallowed.

## WAL commit model

InferaDB Ledger uses a single, opinionated write-path commit model — no opt-in tunables:

1. **Barrier fsync** always. Every WAL commit uses `fcntl(F_BARRIERFSYNC)` on Apple platforms and `fdatasync` on Linux. `F_FULLFSYNC` (non-volatile media flush) is never used — the 4-8× latency cost on APFS isn't justified given the enterprise-SSD power-loss-protection assumption.
2. **Pipelined commit** always. The reactor resolves client proposal responses *after* WAL append but *before* the fsync completes. The fsync still runs on the same tick (in kernel/drive write-cache terms) — it's just no longer on the client's critical path.

Together these mean a typical concurrent-writes p50 of ~0.5-2ms on APFS SSD, down from ~15-25ms under the strictest possible contract.

### Durability matrix

| Failure | Outcome |
|---|---|
| Process crash (SIGKILL / panic) | Safe — kernel writeback flushes the page cache |
| Kernel panic | May lose last ~tick of acked writes (tick ≈ 2ms flush interval) |
| Power loss | Same as kernel panic; size of loss window depends on drive (PLP-equipped drives narrow this to near-zero) |
| WAL fsync error after ACK | Logged + `consensus_pipelined_sync_failures_total` counter. Already-acked responses stand — the kernel buffer is still reachable by process-restart recovery |

The loss window is bounded by the reactor's flush interval (~2ms) plus the batched barrier-fsync latency (~2-5ms on APFS). On enterprise SSDs with power-loss-protection capacitors the "may lose" window collapses to effectively zero because the drive guarantees durability of writes it has acknowledged regardless of subsequent power events.

### Hardware assumption

The durability contract assumes drives honor barrier semantics correctly — acknowledged writes reach the drive's write cache in order, and the drive does not reorder them across an unexpected power event. This is true for:

- Enterprise SSDs with power-loss protection capacitors (standard for any production ledger deployment).
- Cloud block storage (AWS EBS, GCP Persistent Disk, Azure Managed Disks) — all guarantee ordered durability of acknowledged writes.

Commodity consumer SSDs without capacitors may lose the last few seconds of buffered-but-unflushed writes on unexpected power loss. InferaDB's target deployment profile treats this as out-of-scope.

### Metrics

- `consensus_pipelined_sync_failures_total` — counter. Non-zero means an fsync failed after the client had already received success. Alertable: any sustained non-zero rate is a durability regression.
- Response latency histograms (`ledger_grpc_request_duration_seconds`) reflect the critical path *without* fsync; fsync latency is measured separately via the reactor's `wal_sync` span.

The barrier-fsync syscall is isolated in `crates/fs-sync/` — the single workspace crate permitted `unsafe`, since no audited safe-syscall crate exposes `F_BARRIERFSYNC`.

## Tuning the StateCheckpointer

The checkpointer is the operator's primary durability knob. It fires when **any** of three thresholds is crossed:

| Field                   | Default | Range       | Unit    | Meaning                                                        |
| ----------------------- | ------- | ----------- | ------- | -------------------------------------------------------------- |
| `interval_ms`           | 2000    | [50, 60000] | ms      | Time since last successful checkpoint                          |
| `applies_threshold`     | 50000   | >= 1        | applies | Applies accumulated in memory since last checkpoint            |
| `dirty_pages_threshold` | 100000  | >= 1        | pages   | Dirty pages in the state-DB page cache (~400 MB at 4 KB pages) |

All three are runtime-reconfigurable via the `UpdateConfig` admin RPC; the `StateCheckpointer` re-reads from `RuntimeConfigHandle` on each wake-up, so changes take effect on the next tick.

### When to tighten (tighter recovery window, more fsync cost)

- High-throughput workloads where a 2s recovery gap is unacceptable.
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

See [configuration.md](../reference/configuration.md) for the `UpdateConfig` / `GetConfig` admin-RPC overview and the `RuntimeConfigHandle` architecture.

## Monitoring

Nine Prometheus metrics track checkpoint and recovery behavior. Full signatures in the [Metrics reference](../reference/metrics.md).

| Metric                                           | Type      | Labels                        | Purpose                                                                                |
| ------------------------------------------------ | --------- | ----------------------------- | -------------------------------------------------------------------------------------- |
| `ledger_state_checkpoints_total`                 | Counter   | `region`, `trigger`, `status` | Checkpoint attempts; trigger ∈ `time`/`applies`/`dirty`/`snapshot`/`backup`/`shutdown` |
| `ledger_state_checkpoint_duration_seconds`       | Histogram | `region`, `trigger`           | Per-checkpoint fsync latency                                                           |
| `ledger_state_checkpoint_last_timestamp_seconds` | Gauge     | `region`                      | Unix timestamp of last successful checkpoint                                           |
| `ledger_state_applies_since_checkpoint`          | Gauge     | `region`                      | In-memory applies accumulated since last checkpoint                                    |
| `ledger_state_dirty_pages`                       | Gauge     | `region`                      | Dirty pages sampled at checkpointer wake-up                                            |
| `ledger_state_page_cache_len`                    | Gauge     | `region`                      | Total in-memory pages resident in the state-DB cache                                   |
| `ledger_state_last_synced_snapshot_id`           | Gauge     | `region`                      | Most recent snapshot id durably persisted to disk                                      |
| `ledger_state_recovery_replay_count_total`       | Counter   | `region`                      | WAL entries replayed on startup (fires once per region)                                |
| `ledger_state_recovery_duration_seconds`         | Histogram | `region`                      | Duration of the post-open crash-recovery sweep                                         |

### Suggested Grafana panels

The durability-focused panels suggested below ship pre-built in `docs/dashboards/grafana-all-in-one.json` under the **"Durability — StateCheckpointer + Event Flusher"** row (panels 53-61).

- Checkpoint rate per region: `rate(ledger_state_checkpoints_total{status="ok"}[5m])` grouped by `trigger`.
- Checkpoint fsync p50/p99: `histogram_quantile(0.50|0.99, ledger_state_checkpoint_duration_seconds_bucket)`.
- In-memory backlog: `ledger_state_applies_since_checkpoint` + `ledger_state_dirty_pages` on twin y-axes.
- Checkpoint age per region: `time() - ledger_state_checkpoint_last_timestamp_seconds` — flat or monotonically-growing indicates a stalled checkpointer.
- Durable snapshot pointer: `ledger_state_last_synced_snapshot_id` per region — monotonically increasing; a flat line is a stalled checkpointer.
- Event flush rate by trigger: `rate(ledger_event_flush_triggers_total[5m])` grouped by `trigger` — under sustained load `size` should dominate `time`.
- Event flush duration p50/p99: `histogram_quantile(0.50|0.99, ledger_event_flush_duration_seconds_bucket)`.
- Event flush queue depth: `ledger_event_flush_queue_depth` — sustained high values mean producers are outrunning the flusher.
- Event overflow + failure rates: `rate(ledger_event_overflow_total[5m])` grouped by `cause`, and `rate(ledger_event_flush_failures_total[5m])` — non-zero sustained rates are alertable.
- Recovery replay window: `rate(ledger_state_recovery_replay_count_total[1h])` — spikes indicate unclean restarts.
- Recovery sweep latency: `histogram_quantile(0.99, ledger_state_recovery_duration_seconds_bucket)`.

### Suggested alerts

- **Wide crash-gap recovery**: `histogram_quantile(0.99, ledger_state_recovery_duration_seconds_bucket) > 10s` for 5 minutes. Indicates the checkpoint window was too wide before the crash; consider tightening thresholds.
- **Slow disk**: `histogram_quantile(0.99, ledger_state_checkpoint_duration_seconds_bucket) > 100ms` for 10 minutes. Disk or cgroup-IO contention; investigate before it blocks apply throughput.
- **Checkpointer stalled**: `ledger_state_dirty_pages` growing monotonically for > 30 minutes, with no corresponding growth in `ledger_state_checkpoints_total`. The checkpointer is failing or not scheduling; check error logs and `ledger_state_checkpoints_total{status="error"}`.
- **Clean-shutdown audit**: `ledger_state_recovery_replay_count_total > 0` immediately after a planned restart. Indicates `sync_all_state_dbs` did not run or timed out in `pre_shutdown`.

## Contract — What It Promises, What It Does Not

### Promises

- Any `WriteSuccess` response implies WAL durability. The write survives crash and is reconstructed on next startup via `replay_crash_gap`. This applies to all four regional DBs — state.db, raft.db, blocks.db, and events.db.
- Reads after a `WriteSuccess` on the **same node** observe the write immediately (committed to the in-process page cache via `commit_in_memory`).
- Snapshots shipped to followers (via `LedgerSnapshotBuilder::build_snapshot` or `RaftLogStore::get_current_snapshot`) force `sync_state` at the top so the captured `last_applied` reflects durable disk state.
- `IngestEvents` RPC returns only after the external event batch is WAL-committed. A crash between return and the next checkpoint replays the events from the WAL — no data loss, no best-effort-fsync window.

### Does not promise

- Immediate state-DB fsync on `WriteSuccess`. A client that reads from a **different node** immediately after its write may not see the write until that node's apply pipeline + checkpointer catches up.
- Durability of state / blocks / events DB pages between checkpoints. On crash, each DB is rolled back to `applied_durable` and re-derived from WAL — which is correct but not instant.
- **Byte-identical block layout or apply-phase event IDs across a crash-recovery boundary.** See [apply-batching determinism](#apply-batching-determinism) below.
- **Durability of handler-phase audit events inside the checkpoint window on crash.** Handler-phase events reach disk on the `StateCheckpointer` cadence (default ~500ms, not the flusher's 100ms). Up to `state_checkpoint.interval_ms` worth of flushed-but-uncheckpointed events plus anything still in the flusher queue can be lost on SIGKILL / panic / OOM. Clean shutdown drains the queue (Phase 5b) and fsyncs events.db (Phase 5c) — zero loss. External-ingest and apply-phase events are WAL-backed and unaffected. See [handler-phase event flush window](#handler-phase-event-flush-window) and the `enabled=false` escape hatch for strict synchronous semantics.

### Strict-durable exceptions (kept on `commit`, not flipped)

The WAL-durable-then-lazy contract applies to every hot-path apply-pipeline write. The paths that stay strict-durable:

- **`RaftLogStore::save_vote`** — Raft election safety. A lost vote after crash could produce split-brain.
- **`RaftLogStore::install_snapshot`** — Receive-side of snapshot replication. Once the leader's WAL truncates past the installed snapshot's `last_applied`, losing the install means the follower has no WAL to replay.
- **`EventWriter::write_entry`** — Handler-phase single-entry writes. NOT in the Raft apply pipeline — no WAL replay backstop. Kept strict-durable so the `EventWriterBatchConfig::enabled = false` escape hatch restores per-emission fsync semantics verbatim. In steady state this path is NOT on the hot path: production traffic flows through `EventHandle::record_handler_event` → `FlushQueue` → `EventFlusher::commit_batch`, which commits via `commit_in_memory` and is covered by the `StateCheckpointer`'s per-tick events.db sync — the same contract as apply-phase events. `write_entry` is only reached by (a) operators who flipped `enabled = false`, and (b) tests. See [handler-phase event flush window](#handler-phase-event-flush-window).
- **`StateLayer::apply_operations`** (admin / recovery callers only) — `AdminService::recover_vault`, `check_integrity` temp-state replay, `ReadService` historical replay, `AutoRecoveryJob::replay_vault_from_blocks`. IN-APPLY-PIPELINE admin arms use `apply_operations_lazy`.
- **`BlockArchive::compact_before`** — Background block compaction writing condensed blocks + watermark. Infrequent; fsync cost amortized.
- **`EventsGc::tick_inner`** — TTL-GC sweep. Background cadence; strict-durable so GC progress survives crashes.
- **Backup-producing paths** — `CreateBackup` must not return success for data that isn't on disk. `BackupManager::create_incremental_backup` / `create_full_page_backup` force `sync_state` before assembling the backup.

### Apply-batching determinism

A subtlety that integration tests surfaced: under steady-state single-client load, each pre-crash proposal lands in its **own** apply batch. Post-crash, `replay_crash_gap` reads ALL pending WAL entries in `(applied_durable, last_committed]` and feeds them as a **single** batch through `apply_committed_entries`. Because block assembly (`vault_entries` per block) and apply-phase event ID derivation (UUID v5 from `(block_height, op_index, action)`) are functions of the batch boundaries, two kinds of artifacts can differ across a crash-recovery boundary:

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
- **Handler-phase events missing from `events.db` after crash** — expected if the crash was not a clean shutdown. Handler-phase events are not WAL-backed (see [handler-phase event flush window](#handler-phase-event-flush-window)); the loss window is bounded by `state_checkpoint.interval_ms` (default ~500ms) rather than `flush_interval_ms`. Look at `ledger_state_checkpoint_last_timestamp_seconds` for the region against the crash timestamp: any handler-phase event whose flush landed after the last successful checkpoint was in the events.db page cache only and is gone. Apply-phase events and external-ingest events are unaffected — they replay from the WAL. On clean shutdown (`ledger_event_flush_triggers_total{trigger="shutdown"}` incremented plus `sync_all_state_dbs` completed for the region), zero handler-phase events lost.
- **`ledger_event_overflow_total{cause=shutdown_timeout} > 0`** — Phase 5b's `flush_for_shutdown(5s)` drain budget was exceeded; some queued events did not make it to disk. Raise the 5s timeout in `crates/server/src/main.rs` (Phase 5b) if the queue-at-shutdown depth is regularly above the flusher's drain rate.

