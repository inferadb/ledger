# Perf Bottleneck Analysis — concurrent-writes-multivault @ 256 / 16

## Context

| Workload | Throughput |
|---|---:|
| concurrent-writes single-vault @256 | 9,677 ops/s |
| concurrent-writes-multivault @256/16 | 10,201 ops/s |
| concurrent-writes-multivault @512/16 | 10,107 ops/s (saturated, p99=487ms) |
| concurrent-writes-multiorg @256/4 | 10,287 ops/s |
| concurrent-reads @256 | 39,339 ops/s |

Target: 50k+ ops/s. Current: ~10k ops/s — a **5× gap**.

The 10k ceiling is identical across single-vault, multi-vault, and multi-org, despite phases 1–4 of the per-vault consensus spec being landed (per-vault state.db, raft.db, blocks.db, events.db, per-vault apply pipelines). The cap is therefore **above** the per-vault layer — at a process-wide / per-organization or transport-level resource.

## Profile artifacts

This analysis was produced from two captures, both at `CONCURRENCY=256 VAULTS=16`, 60s measured phase:

| Mode | File | Throughput | p99 |
|---|---|---:|---:|
| spans (`tracing-flame`) | `profiles/server-spans-concurrent-writes-multivault-20260427T213713Z.{svg,folded}` | 9,413 ops/s | 304 ms |
| sampling (`samply`) | `profiles/server-concurrent-writes-multivault-20260427T214224Z.json.gz` | 9,660 ops/s | 311 ms |

Spans tells us where logical operation time goes; sampling tells us what threads are doing on-CPU each tick. Cross-checking both modes is essential here because they disagree in a load-bearing way (see "Headline finding").

---

## Headline finding

**The system is not CPU-bound.** It is fsync-bound and lock/queue-bound.

CPU sample distribution (samply, 1.14M samples across worker threads):

| Library | % CPU samples |
|---|---:|
| `libsystem_kernel.dylib` (syscalls / blocking primitives) | **92.4%** |
| `inferadb-ledger` (our code on-CPU) | 5.5% |
| `libsystem_platform` / `libsystem_malloc` | 1.0% |
| Other | < 1% |

92.4% of CPU samples are on syscalls. Combined with `apply_committed_entries` dominating the spans (logical) profile at 70%+ wall-time and `flush_batch + batching::run` taking 50% of self-time, this is a textbook **I/O-wait + lock-contention** signature, not a CPU-throughput problem.

Resolved (via `atos`), the top 8 syscall callers in the binary are:

| % of kernel samples | Call site (resolved from RVA) | What it's doing |
|---:|---|---|
| 39.7% | `tokio::runtime::io::driver::Driver::turn` (driver.rs:188) | kqueue wait — reactor parking when no I/O ready |
| 10.6% + 9.95% | `FileBackend::write_page` (file.rs:173/176) — two distinct inlined call sites | `pwrite` syscall on state/raft/blocks/events DB pages |
| 8.71% | `Database::persist_state_to_disk` (mod.rs:481) | Dual-slot god-byte fsync chain |
| 6.68% | `Database::write` (mod.rs:673) — `write_lock.lock()` | **`std::sync::Mutex` blocking** for write-tx admission |
| 6.40% | `tokio::scheduler::worker::park_internal` | Worker idle, no work to steal |
| 6.09% | `std::sys::sync::condvar::pthread::Condvar::wait_timeout` | Generic mutex/condvar wait |
| 1.32% | `h2::server::SendResponse::send_response` (server.rs:1230) | Sending gRPC response (network write) |

Note: `wal::segmented::sync` (the F_BARRIERFSYNC primitive everyone reaches for first) is only **0.87% of kernel samples**. Group commit is amortising it as designed. **F_BARRIERFSYNC is NOT the bottleneck.** The fsync pressure is on the four state DBs (state.db / raft.db / blocks.db / events.db), not on the WAL.

## Top 5 bottlenecks

### 1. State-DB checkpoint storm — ~36% of CPU samples in `write_page` + `persist_state_to_disk`

**Where:** `crates/raft/src/state_checkpointer.rs` driving `Database::sync_state` → `flush_pages` + `persist_state_to_disk` on every per-org and per-vault DB.

**Why:** The checkpointer's default policy is to fire on *any* of:
- 500 ms elapsed (`default_checkpoint_interval_ms` = 500),
- 5,000 applies (`default_checkpoint_applies_threshold` = 5_000),
- 10,000 dirty pages (`default_checkpoint_dirty_pages_threshold` = 10_000).

At 9.6k ops/s the apply trigger fires roughly every 520 ms, and the dirty-page trigger fires sooner under multi-vault load because each apply dirties pages across **every active vault's state.db + raft.db + blocks.db**. With 16 vaults × ~3 DBs (excluding events.db where it isn't materialised) per organization, plus the org-level DBs, a single tick fans out 50+ `sync_state` calls. Each `sync_state` synchronously walks dirty pages and `pwrite`s them (10.6% + 9.95% of kernel samples are exactly this), then `persist_state_to_disk` fsyncs the dual-slot god byte (8.71%).

**Effect:** The checkpointer steals CPU and fsync bandwidth from the apply path it's supposed to amortise. Tail latency p99 spikes (304 ms) align with checkpoint windows.

**Hypothesised cause:** lock-bound + I/O-bound — tokio workers sit in `condvar::wait_timeout` (6.09%) and `Database::write` lock acquisition (6.68%) while the checkpointer's `spawn_blocking` thread holds the file backend syscalls. Worker `park_internal` (6.40%) is a downstream symptom: workers can't make progress because batching/apply is stalled on the lock chain.

### 2. `apply_committed_entries` is serial across entries within a batch — 70%+ of instrumented wall-time, 24.94% of self-time

**Where:** `crates/raft/src/log_storage/raft_impl.rs:494` (the `for (entry, decoded_payload) in entries.iter().zip(...)` loop).

**Why:** Every committed batch goes through one `for` loop. Within the loop, each entry calls `R::apply_on(...)` which eventually calls `StateLayer::apply_operations_lazy` → `db.write()` (`crates/store/src/db/mod.rs:672`). That `db.write()` holds a `std::sync::Mutex` (`write_lock`) for the duration of the transaction — **single writer per database**.

With per-vault state.db (Phase 4), each vault has its own mutex, so two vaults can apply concurrently in principle. But the apply loop in `raft_impl.rs` is **per-organization Raft group** — entries from different vaults come through the same loop and the same per-org `apply_committed_entries` call, processing them serially.

**Effect:** Even with 16 vaults, a batch of 100 entries spread across 16 vaults applies serially through one loop. The per-vault DB mutexes never contend (each is held briefly and dropped) — but the *loop driver* doesn't dispatch them in parallel. The 16-way fan-out the per-vault spec promises is unrealised on the apply side, even though it's realised on the propose side.

This matches the existing memory note: *"Apply-parallelism blocked by single write_lock per org. Within-batch vault-parallel apply requires per-vault storage isolation (multi-week spec)."*

The post-loop `compute_state_root` and `compute_vault_block_hash` phases DO use rayon (via `apply_pool::APPLY_POOL`, 8 threads), but those are downstream of the serial apply.

### 3. SDK uses ONE HTTP/2 connection for all 256 concurrent tasks

**Where:** `crates/sdk/src/connection.rs::ConnectionPool::get_channel` returns a clone of a single cached `tonic::transport::Channel`. `crates/profile/src/harness.rs` and `concurrent_writes_multivault.rs` clone `harness.client` for every spawned task — all 256 share one `LedgerClient`, one `ConnectionPool`, one channel, one underlying H2 connection.

**Why:** A `tonic::transport::Channel` clone shares the underlying H2 connection internally. With one TCP connection, all 256 streams share **one H2 flow-control window** and one socket. Even though tonic 0.14 / hyper 1.x / h2 0.4 default to unlimited `max_concurrent_streams` server-side and `u32::MAX` initial-max-send-streams client-side, the connection itself becomes the serialization point: every request has to be muxed through one `h2::server::SendResponse::send_response` (which appears at 1.32% of kernel samples — actively bottlenecked on the network write).

**Effect:** Throughput is bounded by single-connection HTTP/2 throughput. With the loopback workload there's no real network limit, but the h2 implementation's per-connection state machine (lock-protected stream table + flow-control accounting) becomes the de-facto serialization point. This is why doubling concurrency from 256 → 512 buys nothing (`10,107 ops/s` at 512 vs `10,201` at 256) and why all three configurations (single-vault, multi-vault, multi-org) hit the same wall.

### 4. Per-org BatchWriter loop runs at fixed 5ms tick — 30.88% of self-time in `flush_batch`, 19.40% in `batching::run`

**Where:** `crates/raft/src/batching.rs::BatchWriter::run` and `flush_batch` (lines 326 and 383).

**Why:** Each per-vault BatchWriter ticks every `tick_interval` (= `batch_timeout / 2`, clamped, default `5ms`). On each tick it acquires a `parking_lot::Mutex` (`state.lock()`) to check the batch state and possibly drain. With 16 vaults each running its own ticker, plus one per org, that's 17 timer-driven mutex-acquire-and-check loops per process, all spinning at 200 Hz (5ms). Spans show `flush_batch` as a 30%+ self-time hot leaf — much of that is the await-on-`submit_fn` (which goes through `propose_and_wait`) but the tick + drain bookkeeping itself is also showing up.

**Effect:** Even at idle, this is 17 × 200 = 3,400 mutex-acquire-and-tick ops per second of pure overhead. Under load each tick triggers a propose round-trip that goes through Raft, applies, and returns through `oneshot::Sender`. The pipeline `max_in_flight_batches = 8` means up to 8 batches-in-flight per writer, but they still drain through one channel back into the writer.

### 5. Group-commit reactor is single-threaded per organization — `reactor::flush` 6.89% of total wall-time

**Where:** `crates/consensus/src/reactor.rs::Reactor::flush` (line 1286).

**Why:** Per `crates/raft/CLAUDE.md` rule 17, there is **one `Reactor` per `(Region, OrganizationId)` tuple**. All 16 vault shards share the parent organization's Reactor. The Reactor is a single async task that:
- Receives propose events via mpsc,
- Calls `handle_propose_batch` (synchronous shard work),
- Aggregates `Action::PersistEntries`,
- Calls `wal_append` (47% of `reactor::flush` time) — segmented WAL write,
- Calls `wal_sync` (35% of `reactor::flush` time) — F_BARRIERFSYNC,
- Dispatches committed batches to per-vault commit pumps.

For 16 vaults under the same org, **everything funnels through one Reactor task**. Even though each vault has its own `ConsensusState` (shard) inside the Reactor's `shards` HashMap, the Reactor itself processes one event at a time. Append-then-sync is the serial path; while it's happening, no other shard's propose makes progress.

**Effect:** A per-org throughput ceiling. With `concurrent-writes-multiorg @256/4` only reaching 10,287 ops/s, this means scaling to 4 orgs **didn't help** — confirming the bottleneck is downstream of the reactor (in apply / checkpoint / DB-mutex land) rather than in the reactor itself. But the reactor is an unsharded prefix that any future optimisation must sit behind.

---

## gRPC / tonic config audit

Audit of `crates/services/src/server.rs::Server::builder()` (lines 496–550) and `crates/server/src/main.rs` (line 57: `#[tokio::main]` with no overrides).

| Setting | Configured value | Default if unset | Recommended for 50k+ ops/s |
|---|---|---|---|
| Tokio `worker_threads` | unset (`#[tokio::main]`) | `num_cpus::get()` (16 on M-series Pro) | Leave default; 16 is fine. No code change. |
| Tokio `max_blocking_threads` | unset | 512 | Probably fine. Profile shows 60+ blocking threads spawned but lightly used. |
| `tonic::transport::Server::http2_keepalive_interval` | 20s | None | OK |
| `tonic::transport::Server::http2_keepalive_timeout` | 5s | None | OK |
| `tonic::transport::Server::tcp_keepalive` | 60s | None | OK |
| `tonic::transport::Server::http2_max_concurrent_streams` | **unset** | None (unlimited) | Leave unset — server-side limit isn't the issue. |
| `tonic::transport::Server::http2_initial_stream_window_size` | **unset** | 64 KB | **Raise to 1–4 MB.** Default 64KB stream window stalls h2 sends every ~64KB on each stream. |
| `tonic::transport::Server::http2_initial_connection_window_size` | **unset** | 64 KB | **Raise to 16–64 MB.** With 256 streams the connection window saturates instantly and the server must wait for client WINDOW_UPDATE frames. |
| `tonic::transport::Server::http2_adaptive_window` | **unset** | false | **Enable** as a fallback if explicit sizing is undesirable. |
| `tonic::transport::Server::concurrency_limit_per_connection` | unset | None | Leave unset. |

**Findings:**
- The HTTP/2 flow-control window defaults are the most likely server-side knob causing observed saturation. With 256 concurrent in-flight streams sharing a 64KB connection window, the writer side stalls constantly.
- No tokio runtime tuning is needed; on a 16-core machine the default is correct for our workload.

## SDK channel audit

`crates/sdk/src/connection.rs::ConnectionPool`:

| Property | Current behaviour | Required for 50k+ ops/s |
|---|---|---|
| Channel pool size | **1** | N (where N ≥ 4–16) |
| Connection establishment | Lazy via `RwLock<Option<Channel>>` | Lazy is fine; pool needs to be a `Vec` |
| Sharing across SDK clients / tasks | `tonic::Channel::clone` (Arc-shared underlying connection) | Same Arc-share ok, but pool must round-robin across N |
| Per-endpoint scaling | One channel per endpoint URL | One channel per endpoint per logical "lane" |
| `http2_keep_alive_interval` | 30s | OK |
| `tcp_nodelay` | true | OK |
| `tcp_keepalive` | 60s | OK |
| Client `http2_initial_stream_window_size` | **unset** | Raise to 1–4 MB |
| Client `http2_initial_connection_window_size` | **unset** | Raise to 16–64 MB |

**Confirmed: ALL 256 concurrent SDK tasks share ONE HTTP/2 connection.** This is the most directly-actionable bottleneck. The harness explicitly clones `harness.client` for every task (`crates/profile/src/workloads/concurrent_writes_multivault.rs:41–45`), and `LedgerClient::clone` shares the underlying `ConnectionPool::Arc<RwLock<Option<Channel>>>`.

Per-connection tonic/h2 throughput on loopback peaks empirically around 10–15k ops/s for small RPCs, regardless of stream count. This matches our observed 10k cap exactly.

## Apply-pipeline audit

| Component | Per-vault? | Per-org? | Comment |
|---|---|---|---|
| `BatchWriter` (`batching.rs`) | Yes | + 1 per org | Each vault has its own writer, ticks at 5ms. Confirmed at `crates/raft/src/raft_manager.rs:3415` (vault) and `:4137` (org). |
| `ConsensusState` (shard) | Yes | + 1 per org (control-plane shard) | Each vault is a distinct shard in the shared engine. |
| `ConsensusEngine` | **No** | One per org | Shared. Vault shards share the parent org's engine (per consensus rule 17). |
| `Reactor` | **No** | One per org | Shared. All 16 vault shards funnel through one async task. |
| `RaftLogStore` (incl. `raft.db`) | Yes | + 1 per org | Per-vault storage from Phase 1–4. |
| `state.db` | Yes | + 1 per org | Per-vault DB lazily materialised by `StateLayer.db_for(vault)`. |
| `blocks.db` | Yes | + 1 per org | Per-vault chain (Phase 4.1.a). |
| `events.db` | Yes | + 1 per org | Per-vault audit log (Phase 4.2). |
| **Apply loop** | **No** | **One per org** | `apply_committed_entries` (`raft_impl.rs:406`) iterates entries serially. Vault shards' commit pumps each have their own apply call (`raft_manager.rs:3146`), but for the parent org's shard the entries route through one loop. |
| `db.write()` mutex | Per DB | n/a | `Database::write` (`store/src/db/mod.rs:672`) — `std::sync::Mutex`, single writer per database. |
| Apply-hash rayon pool | n/a | Process-wide | `apply_pool::APPLY_POOL` — 8 rayon threads for `compute_state_root`, post-apply only. |

**No shared mutex serialises across vaults**, but **the per-org reactor + per-org apply loop are unsharded prefixes** that any vault must funnel through. This is consistent with the per-vault spec's caveat that within-batch vault-parallel apply is a deferred multi-week change.

---

## Recommendations

Each fix has **effort estimate (S/M/L/XL)** and **expected throughput impact** (multiplier, optimistic).

### Fix 1: Increase HTTP/2 flow-control windows on both server and SDK — **S**, **expect 1.5–2.5×**

**Why first:** Cheapest fix, no architectural change, addresses the most clearly observable transport-layer wall.

**Where:**
- `crates/services/src/server.rs:496` — add `.http2_initial_stream_window_size(4 * 1024 * 1024).http2_initial_connection_window_size(64 * 1024 * 1024)` to the `Server::builder()` chain.
- `crates/sdk/src/connection.rs::configure_endpoint` — add `.http2_initial_stream_window_size(4 * 1024 * 1024).http2_initial_connection_window_size(64 * 1024 * 1024)` to the `Endpoint` chain.

**Risk:** Low. The defaults are conservative for memory-constrained environments; production servers typically run with windows in the MB range.

### Fix 2: Pool multiple HTTP/2 connections per endpoint in the SDK — **M**, **expect 2–3×** (compounds with Fix 1)

**Why:** A single H2 connection is the de-facto serialization point for high-concurrency RPC. Round-robin across N=4–16 channels cuts the contention proportionally.

**Where:** `crates/sdk/src/connection.rs::ConnectionPool` — replace `channel: Arc<RwLock<Option<Channel>>>` with `channels: Arc<Vec<RwLock<Option<Channel>>>>` (or an `ArcSwap<Vec<Channel>>` after warm-up). Add `ConnectionPool::pool_size: usize` config (default 8). Round-robin via `AtomicUsize` index in `get_channel`.

**Side-effects:**
- Lower-level integration tests assume a single channel. Audit `tests/` for connection-counted assumptions.
- Memory budget per pool grows linearly with `pool_size`. 8 H2 connections × ~64 KB working state ≈ 512 KB — negligible.
- `circuit_breaker` and `region_cache` keys today use the endpoint URL; this stays correct under pooling (one circuit per endpoint, not per channel).

**Risk:** Medium. Touches the SDK's hot path. Requires a clean abstraction boundary to avoid leaking the round-robin into every retry call.

### Fix 3: Raise checkpoint thresholds and parallelise per-DB checkpointing — **M**, **expect 1.3–1.6×**

**Why:** ~36% of kernel samples are in checkpoint-driven `write_page` / `persist_state_to_disk`. Raising thresholds shifts work into longer-amortised bursts. Parallelising per-DB syncs (the work is already in `spawn_blocking`) lets multiple DBs flush their dirty pages concurrently.

**Where:**
- `crates/types/src/config/storage.rs:243-254` — bump defaults: `interval_ms: 500 → 2_000`, `applies_threshold: 5_000 → 50_000`, `dirty_pages_threshold: 10_000 → 100_000`. Keep the operator overrides intact via `RuntimeConfigHandle`.
- `crates/raft/src/state_checkpointer.rs::do_checkpoint` — verify per-DB syncs are truly concurrent (they may already be via `tokio::join_all` over `spawn_blocking`; confirm and parallelise if not).

**Risk:** Medium. Wider checkpoint window means a longer crash-recovery WAL replay window. Acceptable per the durability docs (the WAL is the source of truth for replay) but should be documented in the operator-tunables section.

### Fix 4: Within-batch parallel apply across vaults — **L**, **expect 2–4×**

**Why:** This is the architectural fix the per-vault spec teased. With per-vault state.db, raft.db, blocks.db, events.db already in place, the only remaining serial point is the `apply_committed_entries` loop in `raft_impl.rs:494`. Group entries by vault, fan out one apply task per unique vault using `tokio::join_all` (or `tokio::task::JoinSet` to bound parallelism to `apply_pool` size), serialise the post-apply `compute_state_root` patch step (already amortised + rayon-parallel).

**Where:**
- `crates/raft/src/log_storage/raft_impl.rs::apply_committed_entries` (line 406+) — split entries by vault (`entries.iter().zip(decoded_payloads).group_by(|e| e.vault)`), fan out per-vault apply via `JoinSet`, await all, then run the existing post-loop state-root + block-hash phase.

**Side-effects:**
- Response ordering: callers expect responses in `entries.iter()` order. Fan-out must preserve that — gather results into a `Vec<Option<Response>>` indexed by entry position, then unwrap. Doable, just careful.
- The atomicity sentinel (`state_layer_sentinel`) is per-vault already (per `last_applied_key_for_vault`) so concurrent updates don't conflict.
- Determinism: per-vault apply is deterministic in isolation; cross-vault ordering doesn't matter because no vault writes another vault's keys.

**Risk:** High. This is a load-bearing path — incorrect parallelisation corrupts state-machine determinism across replicas. Requires extensive simulation coverage (`crates/consensus/src/simulation/`) before merging.

### Fix 5: Spread vaults across multiple orgs (per-org Reactor scaling) — **XL**, **expect 4×+ for multi-tenant workloads**

**Why:** Even after Fixes 1–4, a single org's Reactor is one async task. To scale beyond ~30k ops/s sustained per org, vaults must span multiple orgs (which each have their own Reactor). This is already the architectural axis the multi-org test exercises — the fact that 4 orgs hit the same 10k ceiling means **the bottleneck today is upstream of the reactor**. Once Fixes 1–3 land and that upstream bottleneck moves, the per-org reactor will become the next ceiling.

**Where:** Not a code change — a deployment / API discipline. Already supported.

---

## Validation plan

1. After each fix, re-run `CONCURRENCY=256 VAULTS=16 bash scripts/profile-server.sh sampling concurrent-writes-multivault 60` and compare:
   - Throughput (target progression: 10k → 20k → 30k+ → 50k+).
   - p99 latency (target: < 50 ms — currently 304 ms).
   - Kernel sample share (target: drop from 92% to < 80%; if it stays above 90% after Fix 1+2 and throughput plateaus, the next bottleneck is checkpoint or apply, not transport).
2. Spans-mode profile after Fix 4 to confirm `apply_committed_entries` is no longer the wall-time hot leaf.
3. The throughput gates on the existing benchmark CI (criterion with bencher output) catch regressions; add a new gate for `concurrent-writes-multivault` once the throughput baseline rises.

## Constraints / non-goals

- This analysis does not address read-side scaling (already at 39k ops/s, well above the write ceiling).
- F_BARRIERFSYNC bandwidth is **not** the bottleneck — it costs <1% of CPU. Do not invest in alternative fsync backends until the upstream bottlenecks are cleared.
- The recommendations preserve all golden rules in `CLAUDE.md`, including custom in-house Raft (rule 9), redirect-only routing (rule 11), and the per-org / per-vault tier model (rule 15).

---

STATUS: DONE

**Single biggest bottleneck:** the SDK uses one shared HTTP/2 connection for all 256 concurrent tasks (`crates/sdk/src/connection.rs::ConnectionPool` caches a single `Channel`). All other concurrency layers (per-vault batch writers, per-vault apply pipelines, per-vault DBs) work as intended — the system saturates at ~10k ops/s because that's the empirical throughput ceiling of one tonic 0.14 / hyper 1.x / h2 0.4 connection on loopback with default 64KB flow-control windows. This is why doubling concurrency from 256 → 512 buys nothing and why all three test configurations (single-vault, multi-vault, multi-org) hit the same wall — the bottleneck is upstream of any of them. Fix 1 (raise HTTP/2 windows) + Fix 2 (pool N=8 channels in `ConnectionPool`) should clear this in one engineering week and immediately expose Fix 3 (checkpoint storm) and Fix 4 (apply-loop serialisation) as the next walls — neither of which is visible today because the transport layer is bottlenecking everything else.
