# Per-vault storage isolation

**Status:** **Superseded by [`2026-04-23-per-vault-consensus.md`](2026-04-23-per-vault-consensus.md).** The work described here is now Phase 1 of the larger per-vault consensus plan; the storage-isolation foundation remains the same, but it's delivered as the first phase of a 7-phase sequence that also delivers per-vault Raft groups, shared-per-org group commit, and coalesced heartbeats — ultimately targeting linear throughput scaling with active vault count rather than the ~2-3× ceiling this standalone spec aimed for.
**Date:** 2026-04-23
**Prerequisite:** γ migration (per-org vault allocation) complete — see [`docs/superpowers/completed/2026-04-22-gamma-per-org-vault-allocation.md`](../completed/2026-04-22-gamma-per-org-vault-allocation.md).
**Related:** apply-pipeline audit captured in [`project_apply_parallelism_blocker.md`](../../../.claude/projects/-Users-evan-Developer-inferadb-ledger/memory/project_apply_parallelism_blocker.md); phase-level instrumentation landed 2026-04-23 as the measurement foundation.

## Goal

Split each per-organization `StateLayer` from a single shared `Database` (one write mutex for all vaults in the org) into per-vault `Database` instances (one write mutex per vault). Unlocks concurrent apply across vaults within a single org's batch.

## Why this is the lever

Today's apply pipeline on the hot path:

```
apply_committed_entries (per-org OrganizationApplyWorker)
  → for each entry in batch:
      → state_layer.begin_write()              ── takes Database.write_lock (mutex)
      → state_layer.apply_operations_in_txn()  ── B-tree writes under the lock
      → write_txn.commit_in_memory()
```

**All vaults within an organization share one `state.db` and therefore one `write_lock: Mutex<()>`.** Parallel `par_iter` across vault groups within a batch would contend on that mutex; rayon scheduling overhead + lost dictionary-cache locality would make it *worse* than the current serial loop. This was confirmed by a read-only audit 2026-04-23.

CPU-side rayon parallelism for `compute_state_root` + `compute_vault_block_hash` is already in place (via `inferadb_ledger_state::apply_pool::APPLY_POOL`), because those paths use concurrent read transactions and don't touch the write lock. The apply loop proper — the only remaining serial phase — needs per-vault write-lock isolation to parallelize.

**Measured throughput ceiling today**: single-vault 7.4k ops/s, multi-vault 10.3k ops/s (at 256 clients, APFS SSD, `wal_sync_mode=barrier`). The memory file predicts 2-3× once apply parallelizes per vault.

## Non-goals

- Changing the org→region routing contract.
- Breaking the `crates/store` backend abstraction — per-vault `Database` instances still use the same `StorageBackend` trait.
- Cross-region or cross-organization parallelism — that's already provided by the three-tier consensus model + per-org apply workers.

## Design

### Storage layout change

**Before:**

```
{data_dir}/{region}/{organization_id}/
├── state.db          ← one Database, one write_lock
├── raft.db
├── blocks.db
└── events.db
```

**After:**

```
{data_dir}/{region}/{organization_id}/
├── state/
│   ├── vault-{vault_id}.db   ← one Database per vault, one write_lock per vault
│   ├── vault-0.db            ← SYSTEM_VAULT (audit records, saga PII, _tmp:, _shred:, etc.)
│   └── ...
├── raft.db                   ← unchanged: one Raft log per org
├── blocks.db                 ← unchanged: one block chain per org
└── events.db                 ← unchanged: one events stream per org
```

Only `state.db` splits. `raft.db`, `blocks.db`, `events.db` stay per-org — each has a single writer (the apply worker or its task-spawn children), so there's no lock contention benefit to splitting them. Splitting them would also shred the per-org Merkle chain (for blocks) and the append-only audit timeline (for events), which is undesirable.

### `StateLayer` shape change

```rust
pub struct StateLayer<B: StorageBackend> {
    /// One Database per vault. SYSTEM_VAULT_ID covers audit / saga PII /
    /// crypto-shred keys / _tmp: / _meta: records that are scoped to the
    /// organization but not to a specific vault.
    dbs: parking_lot::RwLock<std::collections::HashMap<VaultId, Arc<Database<B>>>>,

    /// Factory for creating new per-vault Database instances on first
    /// reference (lazy init on CreateVault).
    db_factory: Arc<dyn Fn(VaultId) -> Result<Arc<Database<B>>> + Send + Sync>,

    /// Per-vault dictionary caches stay per-vault — already the case, no change.
    dictionaries: parking_lot::RwLock<HashMap<VaultId, VaultDictionary>>,

    /// ... other fields unchanged
}

impl<B: StorageBackend> StateLayer<B> {
    /// Opens the per-vault Database lazily. First call for a given
    /// `vault_id` creates the file; subsequent calls return the cached
    /// Arc. Thread-safe via double-checked locking.
    fn db_for(&self, vault: VaultId) -> Result<Arc<Database<B>>> { ... }

    /// Scoped write transaction — takes this vault's write_lock.
    /// Concurrent callers for DIFFERENT vaults proceed in parallel.
    pub fn begin_write(&self, vault: VaultId) -> Result<WriteTransaction<'_, B>> {
        self.db_for(vault)?.write()
    }
}
```

Current `begin_write()` takes no argument. New signature adds `vault: VaultId` — caller must know which vault it's writing to. This cascades through every call site; most already have a `vault` in scope at the call point (the write arm has `*vault` from the `OrganizationRequest::Write` destructure).

### Atomicity sentinel

Today the `_meta:last_applied` sentinel lives in the shared state.db's `Entities` table. With per-vault DBs, the sentinel can live in one of:

1. **Per-vault** — each vault's DB has its own `_meta:last_applied`. Apply crash recovery verifies each vault's sentinel matches the Raft commit index. More granular but more work during recovery.
2. **Shared coordination DB** — a tiny per-org `meta.db` (or reuse `raft.db`) holds the sentinel. Matches today's semantics.

**Recommended: option 2.** Add `{data_dir}/{region}/{organization_id}/state/_meta.db` holding `_meta:last_applied`. Single writer (the apply worker) so no lock contention. Per-vault DBs become pure data stores.

### Apply pipeline parallelization

After per-vault isolation lands, modify `apply_committed_entries`:

```rust
// Pre-decode + partition (serial, already done)
// Group consecutive Write entries by vault → parallel groups
// Non-Write entries and group boundaries → serial barriers
// Parallel apply per vault group via APPLY_POOL rayon
// Merge per-group shared-state accumulators (org_storage_bytes, events, pending)
// Post-loop: already parallelized state_root + block_hash
```

Determinism proof obligation: same input batch must produce same AppliedState + same response ordering regardless of parallel schedule. The merge step walks groups in original entry order to preserve response ordering.

### StateCheckpointer fan-out

Today: per region, `StateCheckpointer` iterates `(state.db, raft.db)` and calls `sync_state` on each.

After: per organization, `StateCheckpointer` iterates `(every vault's state.db, raft.db, blocks.db, events.db)`. Config knob `max_dbs_per_tick` caps work per tick to avoid long stalls on orgs with many vaults.

Impact on durability contract:
- Per-vault `commit_in_memory` remains the apply-path pattern (no fsync).
- Per-vault `sync_state` becomes fan-out: checkpointer visits each live vault.
- `RaftManager::sync_all_state_dbs` (called from graceful shutdown) fans out per-vault.

### Backup / restore

`BackupJob` today tars the per-org directory. Now includes all `state/vault-*.db` plus `_meta.db`. Restore applies each file into its target path. Idempotency: if the restore target already has vaults, overwrite (already the current contract).

### Snapshot / install

Raft snapshots today serialize the AppliedState + a reference to state.db content at a given log index. With per-vault DBs, the snapshot serialization must list every vault's DB contents. Incremental snapshots become more complex: a client installing a snapshot needs N per-vault DB files plus the AppliedState blob.

**Recommended:** snapshot format becomes `snapshot.tar` containing AppliedState + every vault's DB snapshot + `_meta.db`. Atomically staged at `{data_dir}/.snapshot-install/` then renamed in.

### Integrity scrubber / compaction / post-erasure

All background jobs that iterate vault contents today call `state_layer.get_entity` / `get_relationship`. Under per-vault DBs, each call routes through `db_for(vault_id)`. No contract change — just a lookup indirection.

B+ tree compaction: each per-vault DB gets its own compaction schedule. `BTreeCompactor` takes `(org, vault)` instead of `(org)` as scheduling unit. Slight increase in scheduler load (N vault DBs instead of 1 org DB) — mitigated by batching small vaults in one cycle.

Post-erasure compaction: after crypto-shredding a vault's AEAD key, the vault's DB is unreadable regardless of compaction. The `_shred:{vault_id}` key still lives in the org's shared meta.db (SYSTEM_VAULT). Erasure deletes the vault's DB file entirely — O(1) filesystem rm instead of O(entities) scan.

### CLAUDE.md rule changes

`crates/state/CLAUDE.md` local rule 11 today says:

> Storage layout is per-organization under each region. Each `(region, organization_id)` owns its own directory at `{data_dir}/{region}/{organization_id}/` holding state.db, raft.db, blocks.db, events.db.

Updates to:

> Storage layout is per-organization under each region. Each `(region, organization_id)` owns its own directory at `{data_dir}/{region}/{organization_id}/` holding raft.db, blocks.db, events.db, a `state/` subdirectory of per-vault `vault-{vault_id}.db`, and a `state/_meta.db` coordinator holding apply-path atomicity sentinels. Per-vault isolation gives each vault its own write lock — apply-loop parallelism across vaults within an organization is the 2-3× throughput lever enabled by this layout.

Root golden rule 6 ("every storage write calls `SystemKeys::validate_key_tier`") is unaffected — the tier assignment is per-key, not per-DB.

## Migration

Ship in a single multi-commit series:

1. **Per-vault `Database` factory + `db_for` lookup** — no callers use it yet. Unit tests cover creation / caching / concurrent lookup.
2. **`StateLayer::begin_write` signature change** — adds `vault: VaultId` parameter. Cascade through every caller (likely 30-50 sites). Each caller already knows the target vault.
3. **Meta DB + sentinel migration** — introduce `_meta.db`, move `_meta:last_applied` there. Apply crash-recovery reads from the new location.
4. **Storage layout migration** — new-install uses per-vault layout. Old-install detector: if `state.db` exists at the old location, refuse to start with a "no backward compat" error (this is a pre-GA product per user directive; no upgrade path).
5. **Apply-loop parallelization** — now that per-vault DBs exist, partition + parallelize the Write arm. Target `ApplyPhase::ApplyLoop` histogram reduction proportional to unique-vault count.
6. **StateCheckpointer fan-out** — visit every vault DB each tick. Benchmark to confirm per-tick overhead is acceptable at expected vault-per-org scale.
7. **Backup / restore / snapshot updates** — tar/untar per-vault DBs. Property test: round-trip snapshot → restore → equality against golden AppliedState.
8. **Integrity scrubber / compaction / post-erasure** — route through `db_for(vault)`. Erasure becomes `rm vault-{vault_id}.db`.

## Success criteria

- `APPLY_PHASE_LATENCY{phase="apply_loop"}` drops by ≥ 2× on a synthetic multi-vault workload at `max_concurrent=256`, `vaults=16` per org.
- Single-vault throughput unchanged (per-vault DB is the same work; just a pointer indirection).
- Multi-vault throughput ≥ 20k ops/s at 32 orgs × 16 vaults (twice the current 10.3k ceiling).
- All 184 integration tests + 5400+ unit tests stay green.
- Crash recovery round-trips correctly: kill mid-apply, restart, verify `applied_durable` replay reconstructs state.
- Backup / restore round-trips correctly: backup → wipe → restore → state equality.
- γ migration canary `watch_blocks_realtime` stays 6/6 green.

## Alternatives considered

1. **Sharded intra-DB mutexes** — split `Database.write_lock` into N buckets by key prefix. Rejected: the single transaction model needs to acquire all buckets before committing → still effectively serial, plus deadlock risk.
2. **Lock-free DB using per-key versioning** — too invasive; touching the entire store layer.
3. **Per-region (not per-org) storage** — the current layout. Doesn't solve the per-org single-writer problem.
4. **Intermediate: per-vault-group mutex within shared state.db** — partition vaults into buckets; each bucket gets its own in-memory mutex wrapping shared backend access. Rejected: still serializes on the backend's internal structures (page cache, WAL) — the win is on paper only.

## Risk register

- **Crash recovery across many vault DBs** — apply worker crashes between committing to vault A and vault B. Recovery replays via the Raft log (unchanged) but must reconstruct state across N DBs. The single `_meta:last_applied` sentinel in meta.db is the synchronization point; each vault DB's content is idempotent per-index.
- **Inode + file descriptor pressure** — N vault DBs per org × K orgs = many open files. Cap via LRU eviction in `db_for` with re-open on demand. Hot vaults stay open; cold vaults unload.
- **Snapshot transfer size** — serializing N per-vault DB files in a single snapshot is larger than one state.db. Compress the snapshot tar; most vault DBs are sparse (1 SST + WAL = small).
- **Per-vault `VaultDictionary` already per-vault** — no contention change; just the lookup now goes through `db_for`.

## Out of scope for this spec

- Cross-vault transactions — per-vault isolation makes them impossible by construction. The existing architecture already disallows them.
- Per-vault backup granularity — could ship later; default is per-org backup.
- Dynamic DB unloading / eviction policy — can stub with "keep all open"; optimize in a follow-up.
