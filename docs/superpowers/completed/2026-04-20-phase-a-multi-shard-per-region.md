# Phase A — Multi-shard per region: Implementation Spec

**Status:** COMPLETE. Phase A landed; B.1 pivoted to org-as-Raft-group; per-vault consensus (the realized end state) is the dedicated `2026-04-23-per-vault-consensus.md` spec, also complete.
**Date opened:** 2026-04-20
**Date closed:** 2026-04-26
**Depends on:** `2026-04-20-multi-shard-scaling-roadmap.md` (strategic context).
**Goal:** Run N independent Raft shards per region so per-node throughput scales with `num_cpus`.

---

## Goal

Per-node write throughput scales linearly with `shards_per_region` up to CPU saturation. Concretely: on a 16-core host, `shards_per_region = 16` should sustain 80k+ ops/s on the concurrent-writes-multiorg workload (vs 10k today with one shard).

**Non-goals:**

- Dynamic shard splitting (Phase C).
- Cross-shard atomic transactions (not needed; orgs atomic within their shard).
- Multi-node validation (Phase B — separate sprint).
- Changing the per-shard Merkle chain structure (each shard keeps its own chain).

## Current state

Today, one `ConsensusEngine` runs per region. The mechanism to support many:

| Component                                         | Today                                                                                          | What it needs                                                                                      |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `RaftManager` (`crates/raft/src/raft_manager.rs`) | `start_region(region)` creates one shard per region. `RegionGroup` holds one `ConsensusEngine` | Create N shards per region; `RegionGroup` holds `Vec<ShardGroup>`                                  |
| `ConsensusEngine` (per shard)                     | Wired to one WAL, one state-layer, one apply-worker                                            | No change — each new shard gets its own instance                                                   |
| `ShardManager` (`crates/state/src/shard.rs`)      | Maps `org_id → shard_id` via seahash; rebuilt from GLOBAL state on boot                        | Already parameterized on shard count; needs correct N                                              |
| Service-layer routing                             | Services resolve `org_slug → region` via directory; propose to `RaftManager`                   | Services resolve to `(region, shard_id)` via `ShardManager`; propose to the correct shard's engine |
| Storage layout                                    | `data_dir/region-{r}/{wal,state.db,raft.db,blocks.db,events.db}`                               | `data_dir/region-{r}/shard-{s}/{wal,state.db,raft.db,blocks.db,events.db}`                         |
| Config                                            | `RaftManagerConfig` has no shard count                                                         | Add `shards_per_region: usize` (default 1)                                                         |

## Design

### Per-shard vs per-region state

**Per-shard (one of each, per shard):**

- Raft log + WAL (`wal/`)
- State DBs (`state.db`, `raft.db`, `blocks.db`, `events.db`)
- `AppliedState` (vault_heights, sequences, region_height, region_chain, org_storage_bytes, …)
- `ApplyWorker` task
- `BatchWriter` task
- Leader election + lease

**Per-region (shared across shards in the region):**

- Storage config (`StorageConfig`, `EventConfig`)
- `EventHandle` (events.db can stay per-shard; `EventHandle` is a reference holder — clarify in implementation)
- Region-level config (residency, retention)
- Background jobs that span shards (cross-shard audit scans, if any — need to enumerate)

**Per-cluster (global, shared across regions):**

- `ShardManager` — one instance; knows all (region, shard) pairs
- GLOBAL region (the single-shard metadata region holding directory, slug index, etc.)
- `NodeConnectionRegistry`

**Key insight:** The "region" concept becomes a _logical grouping_ of N independent chains. Each shard's chain is independently verifiable. Clients doing org-scoped queries only touch one shard; cross-org admin queries may fan across shards.

### Org-to-shard routing

Already implemented in `ShardManager` via `seahash(org_id) % shards_per_region`. What needs to change:

1. `ShardManager` must know `shards_per_region` per region at startup (read from GLOBAL directory state, which records it per region).
2. Services call `shard_manager.resolve_shard(region, org_id) → ShardId`.
3. The resolved `ShardId` becomes part of the proposal routing: `RaftManager::propose(region, shard_id, request)`.

### Storage directory layout

**Before (single-shard):**

```
data_dir/
  node_id
  region-us-east-va/
    wal/segment-000000001.wal
    state.db
    raft.db
    blocks.db
    events.db
```

**After (N-shard):**

```
data_dir/
  node_id
  region-us-east-va/
    shard-0/
      wal/segment-000000001.wal
      state.db
      raft.db
      blocks.db
      events.db
    shard-1/
      wal/...
      state.db
      ...
    ...
```

**Migration for existing deployments:** On first boot with `shards_per_region > 1`, detect old layout (state.db directly in `region-{r}/`) and either:

- (a) Refuse to start with an error instructing manual migration, OR
- (b) Move existing `region-{r}/*` under `region-{r}/shard-0/` automatically.

Recommend (a) for the Phase A MVP — explicit manual migration is safer than automatic file moves. Can add (b) later.

### RaftManager API shape

**Current:**

```rust
impl RaftManager {
    pub async fn propose(&self, region: Region, request: LedgerRequest) -> Result<...>;
    pub async fn start_region(&self, config: RegionConfig) -> Result<()>;
}

struct RegionGroup {
    engine: Arc<ConsensusEngine>,
    apply_worker: JoinHandle<()>,
    ...
}
```

**Target:**

```rust
impl RaftManager {
    pub async fn propose(&self, region: Region, shard: ShardId, request: LedgerRequest) -> Result<...>;
    pub async fn start_region(&self, config: RegionConfig, shards_per_region: usize) -> Result<()>;
}

struct RegionGroup {
    shards: HashMap<ShardId, ShardGroup>,
}

struct ShardGroup {
    engine: Arc<ConsensusEngine>,
    apply_worker: JoinHandle<()>,
    batch_writer: BatchWriterHandle,
    ...
}
```

Services that call `propose(region, request)` become `propose(region, shard_manager.resolve(region, org_id), request)`. The `shard_id` is already computable from the org at the service layer.

## Implementation tasks

Sequential. Each task ends with a compile + test green state. Commit per task so we can bisect.

### Task 1: Config surface for `shards_per_region`

**Files:** `crates/types/src/config/node.rs`, `crates/server/src/config.rs`, `crates/raft/src/raft_manager.rs`, `crates/server/src/bootstrap.rs`

- Add `shards_per_region: usize` to `NodeConfig` (types crate) with `#[serde(default = "default_shards_per_region")]` returning 16.
- Add `--shards-per-region` CLI flag + `INFERADB__LEDGER__SHARDS_PER_REGION` env var on server `Config`. Default 16.
- Add `shards_per_region` to `RaftManagerConfig` with default 16.
- Thread through `bootstrap.rs` from `Config.shards_per_region` to `RaftManagerConfig`.
- Validate `[1, 256]` at the start-region boundary in `bootstrap.rs` (early, operator-friendly error).

**Default rationale:** 16 is tuned for typical multi-core production hosts (matches apply-pool cap of 8 plus headroom for tokio). Test fixtures that need determinism or faster startup set `shards_per_region=1` explicitly.

**Success:** Compiles. Default test pins `shards_per_region == 16`. Bootstrap rejects 0 and >256.

**Effort:** Small — ~1 hour.

### Task 2: ShardManager honors `shards_per_region`

**Files:** `crates/state/src/shard.rs`

- `ShardManager::new` already accepts some parameter for shard count. Verify it's wired correctly.
- Add `ShardManager::resolve_shard(region: Region, org_id: OrganizationId) -> ShardId` that returns `seahash(org_id) % shards_per_region[region]`.
- Persist `shards_per_region` per region in GLOBAL state (key: `_meta:region_shard_count:{region}`). Initialize on region creation; read on `ShardManager` rebuild.

**Success:** `ShardManager` returns deterministic, stable `ShardId` for the same org on any node. Unit tests verify hash distribution is roughly uniform across shards.

**Effort:** Medium — ~half-day. Most of the time is the persistent `shards_per_region` wiring.

### Task 3: Storage layout — `region-{r}/shard-{s}/` sub-directories

**Files:** `crates/raft/src/storage.rs` (region_dir / shard_dir helpers), `crates/raft/src/raft_manager.rs`

- Add `StorageManager::shard_dir(region, shard_id) -> PathBuf` returning `region_dir/shard-{shard_id}/`.
- Every path that today calls `region_dir(region).join(...)` for WAL / state.db / raft.db / blocks.db / events.db must become `shard_dir(region, shard_id).join(...)`.
- On `start_region`: create all N `shard-{s}/` subdirs.
- New product, no deployments in the wild. Always expect the `shard-{s}/` layout — no "legacy single-shard path" to support.

**Success:** Fresh deployments create subdirs correctly. Crash-recovery tests run against the sharded layout and pass.

**Effort:** Medium — ~half-day. Mostly mechanical path rewrites + 1-2 test-fixture updates.

### Task 4: `RegionGroup` holds many `ShardGroup`s

**Files:** `crates/raft/src/raft_manager.rs`

- Refactor `RegionGroup` from a single `ConsensusEngine` holder into a collection of per-shard `ShardGroup`s indexed by `ShardId`.
- `start_region(region, shards_per_region)` loops 0..N, creates a `ShardGroup` per shard (new WAL, new state DBs, new ConsensusEngine, new ApplyWorker, new BatchWriter).
- Each `ShardGroup` is independent; they share nothing except the node-level `NodeConnectionRegistry`.
- Graceful shutdown: iterate all shards across all regions, drain each in the existing 6-phase flow.

**Success:** `start_region` with `shards_per_region = 4` successfully creates 4 Raft groups, each with its own files. A manual RPC-level test shows 4 distinct leaders elected (or the same node as leader for all 4, depending on single-node config).

**Effort:** Large — 1-2 days. Touches the hot wiring.

### Task 5: Propose / Apply routing

**Files:** `crates/raft/src/raft_manager.rs`, `crates/services/src/services/*.rs`

- `RaftManager::propose(region, shard_id, request)` — new signature.
- Update every service call site to resolve `shard_id` via `ShardManager::resolve_shard(region, org_id)` before proposing.
- The `WriteServiceImpl` and similar services become:
  ```rust
  let shard_id = self.shard_manager.resolve_shard(region, org_id);
  self.raft_manager.propose(region, shard_id, request).await
  ```
- `BatchWriter`: one per shard. Services also need to route through the correct shard's `BatchWriter` handle.

**Success:** All existing integration tests pass with `shards_per_region = 1`. A new multi-shard test (see Task 7) shows orgs distributing to different shards.

**Effort:** Medium-large — ~1-2 days. Many call sites to audit, but the transformation is mechanical.

### Task 6: Metrics labels

**Files:** `crates/raft/src/metrics.rs`

- Add `shard` label to per-region histograms/counters where the shard distinction matters:
  - `ledger_state_checkpoints_total`
  - `ledger_state_checkpoint_duration_seconds`
  - `ledger_wal_batch_*`
  - `ledger_raft_apply_*`
- Audit each metric: does it already have enough dimensions? Adding `shard` increases cardinality — make sure it's justified per the metrics-reference's cardinality budget.
- Update Grafana dashboards to include shard drill-down where useful.

**Effort:** Small — ~2-3 hours. Bulk edit + visual verification.

### Task 7: Integration test for multi-shard

**Files:** `crates/server/tests/multi_shard.rs` (new)

Test scenarios:

1. **Distribution:** Create 16 orgs, verify they distribute roughly evenly across `shards_per_region = 4` (no shard gets more than 8).
2. **Isolation:** Write to org A (shard 0) while another process crashes shard 1's WAL. Verify shard 0's writes continue unaffected.
3. **Recovery:** Kill the node mid-flight. On restart, all shards recover independently, no cross-shard state corruption.
4. **Throughput per shard:** With 4 orgs each on a distinct shard, measure that each shard independently sustains ~10k ops/s (i.e., ~40k aggregate). This is the scaling assertion.

**Success:** New test green. Follows `test-isolation-auditor` patterns (`use crate::common::`, `allocate_ports`, etc.).

**Effort:** Medium — ~half-day to a day including wiring + test stability.

### Task 8: Profile preset updates + scaling curve

**Files:** `crates/profile/src/workloads/concurrent_writes_multiorg.rs` (exists, parameterize), harness extensions

- Profile harness's `bootstrap()` should accept a `shards_per_region` value that it passes to the init RPC or embeds in the server start.
- `concurrent-writes-multiorg` preset measured across `(CONCURRENCY, ORGS, SHARDS)` — a scaling curve.
- Script `scripts/measure-phase-a-scaling.sh` to automate the measurement and produce a comparison table.

**Success:** Running the script produces a scaling curve showing throughput vs shard count.

**Effort:** Small-medium — ~half-day.

### Task 9: Docs

**Files:** `docs/operations/deployment.md`, `docs/operations/configuration.md`, `docs/operations/durability.md`, crate-level CLAUDE.md updates

- Document `shards_per_region` config in operator docs.
- Update `durability.md` to note that per-shard chains are independent (this is already true but reinforce it).
- Update `crates/raft/CLAUDE.md` and `crates/state/CLAUDE.md` with the "one Raft group per shard, shards share a region" model.
- Amend `CLAUDE.md` (root) golden rules if needed — probably no new rules, but the multi-Raft framing gets reinforced.

**Effort:** Small — ~2-3 hours.

## Open design questions

With recommended answers — raise in review if any are wrong.

### Q1: Should shard count be settable after region creation?

**Recommendation:** No, for Phase A. Fixed at region creation. Changing shard count later requires rebalancing orgs, which is Phase C (`shard_splitting`). For MVP: operator sets `shards_per_region` at init, cluster-wide.

### Q2: Per-shard or per-region `StateCheckpointer`?

**Recommendation:** Per-shard. Each shard has its own state.db / raft.db / blocks.db / events.db, so checkpointing is naturally per-shard. Background task count: `num_regions × num_shards × 1 checkpointer`. Not a scaling concern at realistic counts (16 regions × 16 shards = 256 checkpointers, negligible).

### Q3: Is the GLOBAL region always single-shard?

**Recommendation:** Yes, for Phase A. GLOBAL holds the directory, slug index, membership metadata — low write volume, never a bottleneck. Single-shard simplifies the ShardManager bootstrap (it reads shard counts for other regions from GLOBAL). In Phase C, reconsider if GLOBAL becomes a bottleneck.

### Q4: How is `shards_per_region` communicated across the cluster?

**Recommendation:** Stored in GLOBAL state as `_meta:region_shard_count:{region}` when a data region is created. Every node reads this on startup when rebuilding `ShardManager`. Proposing a change to shard count means a GLOBAL Raft proposal — out of scope for Phase A (no changes after creation).

### Q5: Does the BatchWriter need per-shard instances, or can one BatchWriter service multiple shards?

**Recommendation:** Per-shard. The BatchWriter already takes a `submit_fn` closure that captures a specific `ConsensusHandle`. N shards = N BatchWriters. Natural and non-coordinating.

### Q6: What happens if the org's shard doesn't exist on this node?

**Recommendation:** In a single-node cluster, all shards are local. In a multi-node cluster (Phase B), this is where the existing "NotLeader + LeaderHint" routing kicks in — if the node is not the leader for this shard, it returns a redirect with the leader's address. No new code needed; the existing redirect-only client routing handles it.

### Q7: Snapshots — one per shard or one per region?

**Recommendation:** One per shard. Each shard has its own state; each takes its own snapshot on its own cadence. `SnapshotManager` probably already works this way (it operates per-engine). Audit during Task 4 to confirm.

## Test strategy

**Unit tests:**

- `ShardManager::resolve_shard` distributes orgs evenly across N shards (proptest, any N orgs, any N shards).
- Shard directory layout construction (`shard_dir(region, shard_id)`).
- Config validation (reject `shards_per_region = 0`, etc.).

**Integration tests (new):**

- `crates/server/tests/multi_shard.rs` — 4-scenario suite above.

**Regression:**

- Every existing integration test with `shards_per_region = 1` (implicit) still passes.
- Proptest: crash-recovery determinism across the apply pipeline remains byte-identical across replicas.

**Stress / soak:**

- New profile preset scaling measurement (Task 8).
- Production-shape workload with `shards_per_region = 16`, 10-minute soak, monitor for:
  - Memory growth (N× shards = N× page caches)
  - Checkpoint failures
  - Cross-shard metric anomalies

## Rollout plan

Phase A is a feature-flag-style rollout:

1. Land all tasks behind `shards_per_region` config, default 1. **Zero behavior change for existing deployments.**
2. Internal testing: run profile at various shard counts, validate scaling curve.
3. Document recommended shard count (probably `min(num_cpus, 16)` for new deployments).
4. Ship with release notes emphasizing "new deployments should configure `shards_per_region`." Migration tool for existing deployments if we choose (b) in Task 3.

## Risks and mitigations

| Risk                                                                | Likelihood | Impact                                          | Mitigation                                                                                                              |
| ------------------------------------------------------------------- | ---------- | ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Hidden cross-shard serialization (some region-global lock or state) | Medium     | Medium — blunts the scaling promise             | Measure scaling curve in Task 8; if sub-linear, profile to find the lock and fix                                        |
| Memory footprint blows up (N × page caches)                         | Low-Medium | Medium — node OOMs at high shard count          | Tune per-shard cache_size_bytes down when N is high; add a per-node memory budget that divides among shards             |
| Integration test flake due to leader election at N shards           | Medium     | Low — just test instability                     | Use `TestCluster::wait_for_stable_leadership(region, shard)` in tests; bound with timeouts                              |
| File handle exhaustion at high shard count                          | Low        | Medium                                          | Each shard = ~5 files (WAL + 4 DBs). 256 shards = 1280 FDs. Well under typical rlimit. Document if deployment hits this |
| Shard-splitting becomes needed before Phase C                       | Low        | Low — defer                                     | Static shard count is fine for Phase A; revisit in Phase C if production data demands                                   |
| GLOBAL region becomes bottleneck with many data regions             | Low        | Low — single-shard GLOBAL handles metadata only | Monitor; if problematic, shard GLOBAL in Phase C                                                                        |

## Success definition

Phase A is shipped when:

1. `shards_per_region = 16` measured at ≥ 80k ops/s sustained single-node writes (8× scaling).
2. All existing tests green; new multi-shard integration test green.
3. Operator docs updated; recommended default documented.
4. Scaling curve published in the memory file for future reference.

## Estimated total effort

~1-2 weeks of focused work:

- Tasks 1-3: ~1 day (config + ShardManager + storage layout)
- Task 4: 1-2 days (RegionGroup refactor — the big one)
- Task 5: 1-2 days (service routing — many call sites)
- Tasks 6-9: 1-2 days (metrics, tests, profile, docs)

Plus measurement + soak testing.

## Open items before starting

1. Confirm Q1-Q7 answers during planning.
2. Decide Task 3 migration policy (automatic file-move vs manual upgrade).
3. Reserve a block of time for Task 4 without interruption — it's the central refactor.
