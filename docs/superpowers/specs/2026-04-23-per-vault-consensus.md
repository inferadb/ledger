# Per-vault consensus

**Status:** Draft — the peak-performance architecture. Supersedes [`2026-04-23-per-vault-storage-isolation.md`](2026-04-23-per-vault-storage-isolation.md) (that spec's per-vault `Database` work is now Phase 1 of this plan).
**Date:** 2026-04-23
**Goal:** Per-vault Raft groups — every vault within an organization gets its own consensus, apply pipeline, and durability unit. Delivers linear throughput scaling with active vault count. Referenced throughout as "Option C" in prior design discussions.

## Executive summary

After γ migration, InferaDB runs one Raft group per organization. All writes to all vaults within an org serialize through one apply pipeline, one WAL, one write lock. This caps single-org throughput at the per-apply-pipeline ceiling (~7.4k ops/s measured 2026-04-20).

Per-vault consensus inverts the primitive: **every vault is its own Raft group.** Leadership is delegated from the parent `OrganizationGroup` (no per-vault elections), heartbeats are coalesced per-(node-pair) regardless of vault count, the WAL is group-committed across all vault groups within an org (one `fdatasync` per batch), and apply pipelines run in parallel across vaults.

The patterns are borrowed from CockroachDB (heartbeat coalescing, centralised membership scheduling), TiKV Raft Engine (shared-log group commit across multi-raft), and TiKV Hibernate Region (dormant vaults contribute zero heartbeat overhead). Net effect: throughput scales linearly with active vault count, bounded only by disk `fdatasync` cost and cpu available for parallel apply.

**Delivery estimate:** 7 phases, ~6-9 months of focused engineering. Each phase is independently green at `just ci`.

## Goals

1. **Per-vault apply pipelines** — one `ApplyWorker<OrganizationRequest>` per vault, parallel across vaults within an org.
2. **Per-vault WAL + group commit** — all vault groups within an org share one append-only WAL file; one `fdatasync` per commit batch regardless of how many vaults contributed.
3. **Coalesced heartbeats** — O(nodes²) heartbeat RPCs, not O(vaults × voters). Dormant vaults contribute zero heartbeat traffic.
4. **Delegated leadership** — vault leaders follow the parent `OrganizationGroup` leader via `Shard::adopt_leader`. No per-vault elections, no per-vault leases.
5. **Centralised membership** — one `ConfChange` decision in the `RegionGroup` (or `OrganizationGroup`) fans out to every affected vault group at a rate-limited pace. Never O(vaults) simultaneous ConfChange RPCs.
6. **Peak read availability** — read from a local vault replica without waiting for the parent org leader (decoupled from write gating).
7. **Hibernation** — vaults with no activity in the last N minutes transition to `Dormant` and consume no scheduler ticks, no heartbeat bytes, no open file descriptors.

## Non-goals

- **Cross-vault transactions.** Vaults are independent consensus groups. An atomic multi-vault write would require 2PC; we're not building that. The existing `OrganizationRequest::BatchWrite` model (multiple inner Writes targeting different vaults within one org) is **removed** by this spec — replaced by per-vault `Write` RPCs at the SDK boundary.
- **Per-vault self-election.** Vaults never run independent elections. All leadership transitions flow from the parent `OrganizationGroup`. Manual leader-change of a vault is not supported.
- **Cross-region vault replication.** Vaults live in one region (their parent org's region). Cross-region migration still goes through the existing `MigrateOrg` saga at org granularity.
- **Per-vault quotas separate from per-org.** Rate limits, storage accounting, and quotas remain per-org. Vaults are internal partitioning, not a billing boundary.

## Design rationale

### Why per-vault consensus over per-vault storage + parallel apply

Per-vault storage isolation (spec'd in [2026-04-23-per-vault-storage-isolation.md](2026-04-23-per-vault-storage-isolation.md)) delivers ~2-3× throughput by eliminating the per-org write-lock bottleneck. Parallel apply with conflict detection on top adds another ~1.5-2×. Together: ~4-6× single-org throughput.

Per-vault consensus delivers linear scaling: throughput proportional to active vault count, bounded only by disk + cpu. At 100 active vaults per org, that's 50-100× over the current 7.4k ops/s ceiling if disk + cpu keep up. The ceiling shifts from "apply serialisation" to "how fast can you `fdatasync`", which group commit makes near-linear across concurrent writers (per TiKV Raft Engine's design).

Per-vault consensus also **eliminates** the need for conflict-detection parallel apply — each vault has its own apply loop, already parallel by construction. Simpler than Option A + Phase 2, not more complex.

### Why CockroachDB's patterns are load-bearing

Multi-raft at thousands of groups per node is a solved problem. CockroachDB, TiKV, and YugabyteDB all run production clusters with this model. The three hard sub-problems (heartbeat coalescing, membership coordination, group-commit WAL) all have published-and-proven solutions. We adopt the CockroachDB / TiKV patterns directly — this is not novel research territory.

### Why delegated leadership (not per-group leases)

CockroachDB uses per-range leases. We're choosing a different path: **vault leadership is delegated from the parent `OrganizationGroup`** via the existing `LeadershipMode::Delegated` primitive introduced by γ Phase B.1.

Rationale (per research):

| Dimension | Per-group lease (CockroachDB) | Delegated (chosen) |
|---|---|---|
| Election storms | N vaults × election_timeout under adversarial conditions | Zero vault elections |
| Heartbeat traffic | N vaults' state must be heartbeated | Piggybacks on org heartbeat; zero per-vault |
| Failover latency | Per-vault | Per-org (one election recovers all vaults) |
| Correlated failure | Bounded per-vault | Org leader stall affects all vaults — mitigated by read-path decoupling (see below) |
| Complexity | N independent state machines | One state machine controls N |

The correlated-failure concern (org leader stall → all vaults stall) is mitigated by **decoupling vault reads from the org leader**. Per-vault apply pipelines serve committed reads locally without asking the org leader. The org leader only gates new write proposals. This recovers read availability while keeping write availability tied to the single org leader.

Precedent: direct precedent is thin in public literature, but the structural analog (parent group controls child groups' leadership) is architecturally clean and maps naturally to the existing three-tier consensus model in InferaDB.

### Why a shared per-org WAL with group commit

Per-vault WAL files would mean N `fdatasync` calls per commit batch — a fsync storm that dominates commit latency at high vault counts.

TiKV Raft Engine's solution: one append-only log file per node, shared across all Raft groups, with region_id-tagged records. One `fdatasync` per batch amortises durability cost across concurrent writers. Throughput is super-linear in group count (more writers → bigger batches → fewer fsyncs per commit).

We adopt this pattern, scoped per-organization: **one shared WAL file per `(region, organization_id)`**, all vault groups within that org share it. The per-vault MemTable (in-memory index) stays per-vault; only the on-disk write stream is shared.

Rationale for per-org scope (vs per-node or per-region):
- **Per-org** is the durability boundary (crypto-shred is per-org; tenant isolation is per-org).
- Per-region would mix orgs in one file — breaks residency isolation.
- Per-node would mix regions — also breaks residency.

## Core design

### Four-tier consensus model

γ Phase B.1 established three tiers: `SystemGroup`, `RegionGroup`, `OrganizationGroup`. This spec adds a fourth: **`VaultGroup`**.

```
SystemGroup (cluster control plane)
  └── RegionGroup (regional control plane, one per region)
        └── OrganizationGroup (organization control plane, one per org)
              └── VaultGroup (per-vault data plane, N per org)
```

Tier responsibilities:

- **SystemGroup**: cluster membership, region directory, organization directory. Unchanged.
- **RegionGroup**: regional placement, hibernation policy, voter set for orgs in this region. Unchanged structurally; gains centralised placement decisions for vaults (see Membership below).
- **OrganizationGroup**: org-level state (name, tier, metadata), slug index for vaults, **leader for all vault groups within this org**. Metadata-plane only — no entity writes.
- **VaultGroup**: entity writes, vault body state (VaultMeta, heights, health), block chain, per-vault events. Data-plane only.

### Vault group shape

```rust
pub struct VaultGroup {
    /// (region, organization_id, vault_id) — cluster-unique.
    scope: VaultScope,

    /// Consensus handle — typed to OrganizationRequest, variant-restricted
    /// to vault-scoped operations (Write, BatchWrite, CreateVault, UpdateVault,
    /// DeleteVault, IngestExternalEvents).
    handle: Arc<ConsensusHandle>,

    /// Per-vault state.db — from the per-vault storage isolation spec,
    /// now subsumed as Phase 1 of this plan.
    state: Arc<StateLayer<FileBackend>>,

    /// Per-vault blocks.db — vault's own Merkle chain.
    blocks_db: Arc<Database<FileBackend>>,
    block_archive: Arc<BlockArchive<FileBackend>>,

    /// Per-vault events.db — vault's own audit stream.
    events_db: Option<Arc<Database<FileBackend>>>,

    /// Per-vault apply pipeline. Workers pulled from a shared tokio
    /// pool — never one tokio task per vault (see TiKV scaling evidence).
    apply_handle: ApplyWorkerHandle<OrganizationRequest>,

    /// Delegated from parent OrganizationGroup. Never runs independent
    /// elections. Adopts leader via adopt_leader() on parent leader change.
    leadership_mode: LeadershipMode, // always Delegated { source: parent_org_leader }

    /// Watch channel — org leader changes drive vault leader transitions.
    parent_leader_rx: watch::Receiver<Option<NodeId>>,

    /// Dormant vaults stop ticking, consume zero scheduler / heartbeat budget.
    /// See "Hibernation" section.
    state_machine: VaultGroupStateMachine, // Active | Dormant | Waking
}

pub enum VaultGroupStateMachine {
    /// Active: receiving writes, participating in heartbeats, applying entries.
    Active,
    /// Dormant: no ticks, no heartbeats, DB file handles evicted. Wake on
    /// first incoming write or admin probe.
    Dormant {
        last_known_leader: NodeId,
        last_committed_index: u64,
        wake_on_message: bool, // always true in B.x versions
    },
    /// Waking: in-flight transition from Dormant → Active. DB handles
    /// re-opened, apply pipeline rebuilding from WAL.
    Waking,
}
```

Every vault group has no per-vault `leader_lease` field — leadership is entirely delegated. The parent org's leader lease is the single source of truth.

### Shared append-only WAL per organization

```
{data_dir}/{region}/{organization_id}/
├── wal/
│   ├── 000001.log              ← shared across all vault groups in this org
│   ├── 000002.log
│   └── ...
├── state/
│   ├── vault-{vault_id}/
│   │   ├── state.db            ← per-vault Database (from Phase 1)
│   │   ├── blocks.db           ← per-vault block chain
│   │   ├── events.db           ← per-vault events
│   │   └── raft.db             ← per-vault Raft metadata (term, voted_for,
│   │                              membership, AppliedState snapshot)
│   ├── ...
│   └── _meta.db                ← per-org coordination (applied_durable
│                                   sentinels, vault directory, hibernation
│                                   state)
└── raft.db                     ← ORG-SHARD Raft metadata (term, voted_for,
                                    membership for the org's OWN consensus
                                    state — for metadata-plane ops like
                                    CreateVault / CreateApp / etc.)
```

**Design clarification (resolves the Option A/B/C design gap surfaced during P2b implementation):** every shard — org-tier and vault-tier — owns its own `raft.db` file. The org's shard uses `{org_id}/raft.db`; each vault's shard uses `{org_id}/state/vault-{vault_id}/raft.db`. Each `RaftLogStore` instance has its own file-backed BTree — no cross-shard contention on vote caches, membership, or `AppliedState` snapshots. The shared resource is ONLY the **WAL** (append-only log) which is tagged per-shard. Per-vault `AppliedState` holds only that vault's data-plane fields (`vault_heights`, `vault_health`, `vaults[(org, this_vault)]`, client sequences for writes targeting this vault); org-tier `AppliedState` holds everything else (vault directory, org metadata, app registry, saga records, slug indexes).

Key property: vault groups inherit parent org's Raft voter set (every voter of the org votes on every vault). Per-vault Raft log entries live in the shared WAL, tagged by `vault_id`. Per-vault apply pipeline reads its shard's entries from the shared WAL and mutates its own per-vault `raft.db` + per-vault `state.db`.

The shared WAL uses the **TiKV Raft Engine model**: append-only, single `fdatasync` per batch, per-vault MemTable for in-memory index. Crash recovery re-reads the shared log once and rebuilds all per-vault MemTables.

### Apply pipeline

**One apply worker per active vault**, backed by a shared tokio worker pool (not one tokio task per vault):

- `ApplyPool` owns `K` worker tasks where `K ≈ num_cpus / 2` (same sizing as current `inferadb_ledger_state::apply_pool::APPLY_POOL`).
- Each worker drains a work queue of committed entry batches.
- Batches are routed to workers by `vault_id % K`, guaranteeing in-order apply per vault while allowing parallel apply across vaults.
- Dormant vaults have no entries in the queue — zero scheduler overhead.

This is the same structure as today's `ApplyPool` (in `crates/raft/src/apply_pool.rs`), just scaled to operate on `VaultGroup` identifiers instead of `(region, organization_id)`.

### Heartbeat coalescing

**CockroachDB's per-(node-pair) model:**

Naive: `MsgHeartbeat` per Raft group, per tick. At N=1000 vaults × 3 voters, that's 3000 heartbeat RPCs per node per tick.

Coalesced: one heartbeat RPC per `(sender, receiver)` node pair per tick, carrying a list of `(vault_id, term, commit_index)` tuples for every vault that has that pair as (leader, follower). Receiver expands the batch into synthetic per-vault heartbeats for its MultiRaft state machines.

Delegation simplifies further: since vault leadership is inherited from the org leader, heartbeats only need to prove **org leader liveness**, not per-vault liveness. Per-vault log-state sync (commit_index advancement, log append) still flows via the coalesced RPC path, but the heartbeat *proof* is org-level.

**Hibernate dormant vaults** (TiKV pattern): vaults with no writes in the last `hibernation_idle_secs` transition to Dormant and are removed from the coalesced heartbeat payload entirely. Wake on:
- First incoming write targeting this vault (leader side)
- First incoming append carrying vault's entries (follower side)
- Admin probe (operational tools)

Wake cost: open the vault's DB file handles, register the apply pipeline, rejoin the coalesced heartbeat. Target: <10ms p99 on warm-OS-cache path.

**Election timeout interaction.** Coalesced heartbeats are sent every tick; the per-vault election timeout remains unchanged. Because delegation eliminates per-vault elections, the election_timeout is effectively a single value tied to the org's election_timeout.

### Centralised membership changes

**Pattern (from CockroachDB + TiKV PD):** membership changes are decided centrally by the `OrganizationGroup` (or `RegionGroup` for org-level changes), not by individual vault groups. Changes are serialised through a rate-limiter to avoid snapshot storms.

Implementation:

1. Admin triggers `AddOrgVoter { node_id }` (or equivalent) on the `OrganizationGroup`.
2. Org leader proposes the membership change at org level (one Raft entry).
3. Org's apply handler enqueues a per-vault `ConfChange` for every vault group in the org into a shared `MembershipQueue`.
4. `MembershipQueue` dispatches one conf-change at a time per node, with `max_concurrent_snapshots_per_store` rate-limit (default 2, from TiKV's default).
5. Each vault group's conf-change is an atomic operation on its own Raft log (in the shared WAL).
6. Admin observes convergence via a per-org metric `voter_add_pending_count`.

**Convergence SLA:** at 1000 vaults/org with 2 concurrent snapshots, a new voter joins all vault groups in ~500 seconds (under 10 minutes) assuming each snapshot transfers in ~1s on local network. Documented; not optimised for speed at cost of snapshot storm protection.

**Pitfall** (from CockroachDB): a single stuck replica can stall the whole queue. Mitigation: per-vault conf-change timeout (60s per TiKV default), after which the queue advances and logs a warning for the stuck vault. Operator investigates async.

### Coalesced commit

**Group commit via shared WAL:** the existing `Reactor` (in `crates/consensus/src/reactor.rs`) already batches WAL writes with single fsync per batch. Extend this to span multiple vault groups within an org:

- Reactor owns the shared per-org WAL.
- Each vault group's `Shard` emits `WalAppend` actions into a shared pending-writes buffer.
- Reactor flushes on: buffer size threshold, time threshold (e.g., 1ms), or explicit sync signal from the consensus engine.
- One `pwrite` per flush, followed by one `fdatasync`.
- Entry-level acknowledgment: each vault's Shard is notified of commit via its own `CommitNotify` channel; multiple vaults can be notified in parallel after the single fsync.

This is golden rule 10's natural extension — "batch WAL writes with single fsync per batch" — now spanning multiple Raft groups within a shared WAL.

### Read path

**Follower reads don't wait for org leader.** Each vault group maintains a local committed index (from the shared WAL). A read request targeting this vault, arriving at a follower node, can be served against the vault's local committed state without round-tripping to the org leader.

Freshness model:
- **Default read**: serves against last committed index on this node. Bounded-staleness.
- **Leader-exclusive read**: explicit API flag to wait for quorum confirmation. Strong consistency. Latency: ~1 RTT to quorum.
- **Index-bound read**: caller specifies minimum committed index. Waits locally until reached.

This is a significant UX improvement over the current model (where reads must query the region leader for strongly-consistent reads). Per-vault local reads are low-latency; strongly-consistent reads opt-in.

## API changes

### Proto

```proto
// Added to Write/Batch request path: vault is the routing key.
// Existing OrganizationSlug field retained for org-level authorization.

message WriteRequest {
  OrganizationSlug organization = 1;   // unchanged; for authorization
  VaultSlug vault = 2;                  // promoted to routing key — mandatory
  ClientId client_id = 3;
  bytes idempotency_key = 4;
  repeated Operation operations = 5;
  bool include_tx_proof = 6;
  UserSlug caller = 7;
}

// BatchWriteRequest: removed. Cross-vault atomicity not supported.
// Clients that previously used BatchWrite now issue multiple Write calls.

// LeaderHint carries the vault-level leader, not org-level:
message LeaderHint {
  OrganizationSlug organization = 1;
  VaultSlug vault = 2;
  optional NodeId leader = 3;
  uint64 term = 4;
}
```

### ProposalService

```rust
#[async_trait]
pub trait ProposalService {
    // Existing methods retained for system- and region-tier traffic:
    async fn propose_bytes(&self, bytes: Vec<u8>, timeout: Duration) -> Result<...>;
    async fn propose_to_region_bytes(&self, region: Region, bytes: Vec<u8>, timeout: Duration) -> Result<...>;
    async fn propose_to_organization_bytes(&self, region: Region, org: OrganizationId, bytes: Vec<u8>, timeout: Duration) -> Result<...>;

    // New method for vault-scoped data-plane writes:
    async fn propose_to_vault_bytes(
        &self,
        region: Region,
        organization: OrganizationId,
        vault: VaultId,
        bytes: Vec<u8>,
        timeout: Duration,
    ) -> Result<...>;
}
```

### SDK

`LedgerClient::VaultLeaderCache` replaces the current `RegionLeaderCache`. Keys by `(region, organization, vault)`; invalidates on `NotLeader` hint carrying `(organization, vault, new_leader)`.

Call pattern:
```rust
client.write(caller, organization, vault, ops, idempotency_key).await?;
// SDK looks up (organization, vault) → cached leader address
// Issues RPC to that address
// On NotLeader, updates cache from hint and retries (with backoff)
```

SDK transport unchanged — reuses the existing `NodeConnectionRegistry` channel pool.

## Operational model

### Hibernation

Per-vault hibernation, governed by a per-org policy:

- `hibernation_idle_secs` (default 300): if a vault has had no writes in this window, transition to `Dormant`.
- `hibernation_max_warm_vaults_per_org` (default 1000): soft cap on active vaults per org; excess transitions to Dormant LRU-fashion.
- Wake on demand: first write or admin probe triggers `Waking → Active` transition.

Dormant state persisted in `_meta.db` per-vault: `{ last_known_leader, last_committed_index, last_activity_ts }`. Wake reads this, re-opens DB handles, rejoins coalesced heartbeat.

**Wake latency target:** <10ms p99 on warm-OS-cache path; <100ms p99 on cold path (DB file needs paging in).

### Observability

Per-vault metrics (labelled by `(region, organization_id, vault_id, ...)`):
- `vault_apply_latency_seconds` (histogram, phase-labelled)
- `vault_commit_index` (gauge)
- `vault_leader` (gauge; 1 if local is leader, 0 otherwise)
- `vault_state_machine` (gauge; Active/Dormant/Waking as strings via label)
- `vault_writes_total` (counter)

**Cardinality cap:** per-vault labels are a cardinality risk. Roll-up metrics at the org level are the primary operational view:
- `org_active_vault_count`
- `org_dormant_vault_count`
- `org_writes_total` (summed from vaults)
- `org_apply_latency_seconds` (avg across vaults)

Per-vault metrics are opt-in (enabled by operator for drill-down), default disabled.

### Recovery

**Crash recovery flow:**

1. Node restarts, reads shared per-org WAL from `{data_dir}/{region}/{organization_id}/wal/`.
2. For each WAL entry, look up `vault_id` tag. Route to per-vault MemTable reconstruction.
3. Per-vault `applied_durable` sentinel in `_meta.db` tells how far each vault's state.db / blocks.db / events.db were committed to disk.
4. Replay each vault's gap: `(applied_durable, last_committed_in_WAL]`.
5. Apply pipeline resumes once all active vaults have caught up.

**Vault-level corruption:** per-vault DBs are isolated — corruption in vault A doesn't affect vault B. Operator tools expose `ledger vaults repair <vault_id>` for targeted recovery.

### Backup

Per-vault backup: each vault's DB files are tarred into a vault-level backup. Org-level backup is a tar of all vault backups + `_meta.db` + `raft.db`. Restore untars in parallel.

Incremental backups: per-vault (each vault has its own WAL offset markers). Small active vaults restart faster from backup than under the current monolithic model.

## Migration — hard cutover

User-confirmed: no existing data, no upgrade path. New clusters start with per-vault consensus. Old clusters (if any exist outside the user's dev environments) are incompatible.

Detection: at startup, check `{data_dir}/{region}/{organization_id}/state.db` — if exists, bail with:

> "Legacy per-org storage layout detected. This build requires per-vault consensus. Data migration is not supported; wipe data_dir and restart."

## Seven-phase implementation plan

Each phase ends with `just ci` green. Phases can be sequenced sequentially or with moderate overlap; dependencies noted.

### Phase 1: Per-vault storage isolation (4-6 weeks)

Subsumes spec [`2026-04-23-per-vault-storage-isolation.md`](2026-04-23-per-vault-storage-isolation.md):

1. Per-vault `Database` factory + lazy `db_for(vault_id)` lookup.
2. `StateLayer::begin_write(vault_id)` signature change; cascade through ~30-50 callers.
3. Per-vault `state.db`, `blocks.db`, `events.db`.
4. `_meta.db` coordinator for applied_durable sentinels, vault directory.
5. StateCheckpointer fan-out across vault DBs per org.
6. Backup/restore/snapshot/integrity-scrub/compaction/post-erasure per-vault routing.

**Still one Raft group per org at the end of Phase 1.** The apply pipeline still serialises — but each write goes to its vault's own Database. Durability is per-vault. Sets the foundation.

**Exit criteria:** single-vault throughput unchanged; multi-vault throughput improves proportional to CPU parallelism for state_root / block_hash computation (no change in apply-loop serialisation).

### Phase 2: VaultGroup type + per-vault apply pipeline (3-4 weeks)

1. Define `VaultGroup` type (newtype wrapper, mirrors existing `OrganizationGroup`).
2. `RaftManager::start_vault_group(region, org, vault_id)` — analog of `start_organization_group`.
3. Apply pool accepts `VaultGroup` identifiers; route by `vault_id % K`.
4. `OrganizationApplyWorker` → split into per-vault `VaultApplyWorker<OrganizationRequest>` pulled from shared pool.
5. `OrganizationRequest` variant validation: `Write`, `BatchWrite` (deprecated but still accepted until Phase 6), `CreateVault`, `UpdateVault`, `DeleteVault`, `IngestExternalEvents` are valid at vault tier; all other variants reject at vault tier with a tier-violation error.

**One Raft group per vault at the end of Phase 2**, BUT still using per-vault WAL files (no group commit yet). Apply is parallel.

**Exit criteria:** multi-vault within single org now scales near-linearly with active vault count in apply dimension; fsync cost dominates (expected — Phase 3 fixes).

### Phase 3: Shared per-org WAL + group commit (4-5 weeks)

1. Shared append-only WAL file per `(region, org)`. Log segments rotate at configurable size.
2. Per-vault MemTable (in-memory log index) stays per-vault.
3. Reactor extended to batch WAL writes across multiple vault Shards within an org's Reactor.
4. Single `fdatasync` per batch; per-vault `CommitNotify` channels fire after sync.
5. Crash recovery: read shared WAL once, rebuild per-vault MemTables, replay per-vault gaps via `replay_crash_gap`.

**Exit criteria:** multi-vault write throughput dominated by apply cpu (not fsync). Single fsync per batch regardless of vault count.

### Phase 4: Delegated leadership for vaults + coalesced heartbeats (4-6 weeks)

1. Vault groups always use `LeadershipMode::Delegated { source: parent_org_leader }`.
2. `OrganizationGroup` leader-change events propagate to every vault group in the org via `adopt_leader` fan-out.
3. Coalesced heartbeats: one RPC per `(sender, receiver)` node-pair per tick, carrying a list of vault_ids' (term, commit_index) tuples.
4. Hibernate: vaults with no activity in `hibernation_idle_secs` → Dormant state.
5. Wake: first incoming message → Waking → Active.
6. Decouple vault read path from org leader — per-vault local committed-index reads.

**Exit criteria:** 10k active vault groups per node is feasible (TiKV-proven scale). Heartbeat overhead is O(nodes²), not O(vaults). Dormant vaults cost ~zero.

### Phase 5: Centralised membership (4-5 weeks)

1. `OrganizationGroup` leader makes placement decisions for all vault groups in the org.
2. Rate-limited `MembershipQueue`: max N concurrent snapshot-producing conf-changes per-store (default 2).
3. Add/remove voter on org fans out to every vault group; each vault's conf-change is a single Raft entry in the shared WAL.
4. Per-vault conf-change timeout (60s) with operator-visible warning on stall.
5. Voter bootstrap: new voter catches up on all vault groups in parallel via the shared WAL replay.

**Exit criteria:** adding a voter to an org with 1000 vaults converges within ~500s with no snapshot storm.

### Phase 6: API + SDK migration (3-4 weeks)

1. Proto changes: `WriteRequest.vault` promoted to routing key (already present post-γ); `BatchWriteRequest` removed.
2. `ProposalService::propose_to_vault_bytes` added.
3. SDK `VaultLeaderCache` replaces `RegionLeaderCache`.
4. `NotLeader` hints carry `(organization, vault, leader, term)`.
5. Server rejects `BatchWrite` with clear deprecation error; SDK's retry loop issues per-vault `Write` calls instead.
6. All tests migrated to single-vault-Write patterns.

**Exit criteria:** no production code path uses cross-vault BatchWrite. SDK routes directly to vault leaders.

### Phase 7: Observability + hibernation policy + operations (2-3 weeks)

1. Per-vault metrics (opt-in) + org-level roll-ups (always on).
2. Hibernation policy config: idle secs, max warm, wake threshold.
3. Operator tools: `ledger vaults list <org>`, `ledger vaults show <org/vault>`, `ledger vaults repair <org/vault>`.
4. Grafana dashboards: per-org with per-vault drill-down.
5. Documentation: update root `CLAUDE.md` tier discipline + `crates/state/CLAUDE.md` storage layout + `crates/raft/CLAUDE.md` group coordination.

**Exit criteria:** operators can see, debug, and intervene at per-vault granularity without drowning in cardinality.

## Success criteria

At the end of Phase 7:

- **Multi-vault throughput scales linearly with active vault count**, bounded by (cpu_cores × apply_parallelism) and (WAL bytes/sec × group_commit_amortisation). Target: ≥50k ops/s sustained at 16 active vaults × 256 clients per vault on a 3-node cluster (vs 10.3k current).
- **Single-vault throughput unchanged or marginally improved** (per-vault Database has the same work plus a hash-map indirection; overhead <2%).
- **Dormant vaults cost zero** in heartbeat, scheduler ticks, and memory (DB file handles evicted).
- **Coalesced heartbeat RPC rate independent of vault count**: 5-node cluster, 1000 vaults, heartbeat RPC/sec bounded by `nodes² × ticks_per_sec` = 100.
- **Membership convergence:** adding a voter to a 1000-vault org converges within 10 minutes; no snapshot storm.
- **Wake latency:** <10ms p99 warm-cache.
- **Crash recovery:** per-vault `replay_crash_gap` reconstructs state from shared WAL.
- **All `just ci` gates green** at every phase boundary. No skipped tests.
- **γ Phase 3 canary tests** (`watch_blocks_realtime`, `three_tier_consensus`) remain green at every phase.

## Alternatives considered

1. **Option A: per-vault DB + parallel apply with conflict detection** (~4-6× throughput). Rejected: ceiling is still single apply pipeline; doesn't scale to 50-100×.
2. **Option B: Calvin-style deterministic concurrency** (theoretical 10-50×). Rejected: 6-12 month rewrite of consensus + transaction model; InferaDB's CAS operations don't map cleanly to Calvin's declare-read-sets-upfront model.
3. **Range-based consensus (CockroachDB model)** — vaults bin-packed into dynamically-split ranges. Rejected for V1: our `vault` is a user-visible API primitive; going range-based would require a range-management layer (multi-month on top of this spec). Can migrate to range-based in a future phase if vault counts explode — the per-vault consensus model is forward-compatible with range-splitting.
4. **Per-vault Database only (no consensus)** — Phase 1 of this plan standalone. Rejected as the final state: only delivers 2-3×.
5. **Self-elect per-vault leadership** — independent per-vault elections. Rejected: heartbeat storm, election storm, high operational complexity with no matching throughput win (leadership doesn't limit throughput; apply does).

## Risk register

| Risk | Mitigation |
|---|---|
| Heartbeat storm at N > 10k vaults per node | Coalesced per-(node-pair); TiKV-proven at "millions of regions"; hibernate dormant vaults |
| fsync storm on commit bursts | Shared per-org WAL with group commit; TiKV Raft Engine pattern |
| Membership coordination blows up at high vault counts | Rate-limited centralised scheduler; CockroachDB-proven pattern; accept "minutes to converge at 1000+ vaults" as the SLA |
| Correlated failure (org leader stall → all vaults stall) | Decouple vault reads from org leader; trigger org leader-transfer on stall; Phase 4 includes read-path decoupling |
| Operational complexity (debugging N groups per org) | Per-vault metrics opt-in, org roll-ups always-on; `ledger vaults` CLI for drill-down |
| Cross-vault BatchWrite deprecation breaks callers | Hard cutover (user-confirmed); SDK auto-fans out, no user-visible regression; error message is explicit |
| Test coverage scales with vault count | Reuse TestCluster.create_organization helpers; add `create_vault_groups(org, n)` helper for multi-vault test setup |
| γ Phase 3 state-root invariants break under per-vault apply | `watch_blocks_realtime` canary on every phase; VaultEntry-carried slugs (γ Phase 3a) already decouple announcement emission from shared state maps |

## Open questions

1. **Shared WAL segment rotation under group commit — concrete size + time thresholds?** Needs benchmarking. Propose defaults: 128MB segment size, 1ms batch window, 100 entries max. Tune per-phase.
2. **Dormant vault memory overhead — acceptable floor?** Target: <1KB per dormant vault (last-known-leader state + metadata). Needs per-vault state audit.
3. **Voter membership inheritance across vaults — always identical or per-vault placement?** V1: identical (inherited from org). Per-vault placement is a future enhancement if workloads demand it.
4. **Range migration to CockroachDB-style bin-packing** — if vault counts exceed 100k per node, bin-pack vaults into ranges. Scope: new spec when needed.

## Appendix: sources

- CockroachDB multi-raft design: https://www.cockroachlabs.com/blog/scaling-raft/
- CockroachDB Quiesce Ranges RFC: GitHub cockroachdb/cockroach `docs/RFCS/20160824_quiesce_ranges.md`
- CockroachDB rebalancing tech note: GitHub `docs/tech-notes/rebalancing.md`
- TiKV multi-raft deep dive: https://tikv.org/deep-dive/scalability/multi-raft/
- TiKV Hibernate Region + tuning: https://tikv.org/blog/tune-with-massive-regions-in-tikv/
- TiKV Raft Engine (shared log): https://github.com/tikv/raft-engine
- YugabyteDB multi-Raft V0: GitHub yugabyte/yugabyte-db#10479
- CockroachDB leader leases (fortified): GitHub cockroachdb/cockroach#123847
- γ migration completion: [`docs/superpowers/completed/2026-04-22-gamma-per-org-vault-allocation.md`](../completed/2026-04-22-gamma-per-org-vault-allocation.md)
- Phase B.1 three-tier spec: [`2026-04-20-phase-b1-org-as-shard.md`](2026-04-20-phase-b1-org-as-shard.md)
