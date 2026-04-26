# Multi-DB Backup Archive Format

**Status:** Proposed.
**Date:** 2026-04-24.
**Tracking:** Task #181 (this spec); Task #180 (per-vault backup paths — blocked on this).
**Related:** [`2026-04-23-per-vault-storage-isolation.md`](2026-04-23-per-vault-storage-isolation.md) (Phase 1 / Phase 4.1.a — per-vault `state.db` and `blocks.db` split); [`2026-04-23-per-vault-consensus.md`](2026-04-23-per-vault-consensus.md) (Phase 2 — per-vault `raft.db`); [`docs/architecture/durability.md`](../../architecture/durability.md) (`applied_durable` / `sync_state` contracts).

## Problem

`AdminService::create_backup` currently produces a logical entity-row snapshot. The handler (`crates/services/src/services/admin.rs:2023`) builds an in-memory `Snapshot { header: SnapshotHeader, state: SnapshotStateData }` whose payload is `vault_entities: HashMap<VaultId, Vec<Entity>>` plus `VaultSnapshotMeta` (height, state_root, bucket_roots) per vault. `BackupManager::create_backup` (`crates/raft/src/backup.rs:271`) writes that snapshot via `Snapshot::write_to_file` and a JSON sidecar. Zero database file bytes leave the node — no `state.db`, no `raft.db`, no `blocks.db`, no `events.db`, no `_meta.db`. The page-level helpers (`create_full_page_backup`, `create_incremental_backup`, `restore_page_chain`) exist in `backup.rs` but the RPC paths return `unimplemented` post per-vault storage split (`admin.rs:2046-2053`, `admin.rs:2247-2261`).

The on-disk layout after Phase 1 + Phase 4.1.a is:

```text
{data_dir}/{region}/{organization_id}/
├── _meta.db                       # per-org sentinel coordinator (_meta:last_applied)
├── raft.db                        # per-org control-plane Raft log
├── blocks.db                      # per-org control-plane blocks (legacy, see open question 1)
├── events.db                      # per-org events stream
└── state/
    └── vault-{vault_id}/
        ├── state.db               # per-vault entity store
        ├── raft.db                # per-vault Raft log (Phase 2)
        ├── blocks.db              # per-vault block chain (Phase 4.1.a)
        └── events.db              # per-vault events (Phase 4.3, in flight)
```

The logical-snapshot format captures none of this. It rebuilds entities from a `StateLayer` walk (`admin.rs:2106-2114`); blocks, audit events, and the Raft log are silently dropped. With per-vault Merkle chains landed in Phase 4.1.a and per-vault audit streams arriving in Phase 4.3, every restored cluster would have empty block chains and empty audit history. That is a production-correctness gap, not an optimization. A backup format that ships file bytes for every DB the node owns is the only path that captures the whole replicated state.

## Goals

- Capture full physical state for an organization: `_meta.db`, control-plane `raft.db` / `blocks.db` / `events.db`, and every per-vault `state.db` / `raft.db` / `blocks.db` / `events.db`.
- Single restorable artefact per organization, addressable by `(region, organization)` — not a directory of loose files.
- Restore is a complete physical replacement: file bytes from the archive overwrite the data directory's per-org tree.
- Pre-flight failure on `RootMasterKey` (RMK) mismatch — the manifest carries an RMK fingerprint so a wrong-region or rotated-RMK restore fails fast instead of producing an unreadable cluster.
- Streaming-friendly so multi-GB DBs do not require buffering the entire archive in memory on either side.
- Standard tooling on the producer and consumer (operator can `tar -tvf | zstd -d` to inspect without ledger-specific binaries).

## Non-goals

- Backwards compatibility with the existing logical-snapshot `.backup` files. Hard cutover — existing artefacts are invalidated.
- Live restore (running node accepts a restore in place). Restore requires the target node to be stopped.
- Cross-region restore. Each region has its own RMK; re-wrapping every DEK is a separate concern.
- Cross-cluster portability. Backups land back into a node with the same cluster identity (region + RMK) or fail.
- Full-cluster (all-orgs) tarballs. Per-org granularity is the unit; an operator running a "back up everything" loop iterates orgs.
- Replacing the Raft snapshot-install path used between peers (`RaftLogStore::install_snapshot`). That path stays internal and untouched.

## Design decisions

### 1. Archive container — `tar` + `zstd`

**Decision:** `tar` archive compressed with `zstd`, single stream. File extension `.tar.zst`.

**Rationale:**
- Standard tooling. An operator with a corrupt archive can crack it open with `zstd -d | tar -xv` and inspect by hand. Custom framing would require a debug CLI we would have to maintain.
- Streaming-friendly. Both `tar` and `zstd` are append-friendly and consume bounded memory; the archive build can `pipe(state.db) -> tar.append -> zstd.encode -> file` without holding any DB in RAM.
- First-class Rust support. The `tar` crate is part of the cargo ecosystem; `zstd` is the canonical Rust binding to libzstd. No NIH.
- `zstd` ratio on B+tree pages is close to gzip without the CPU cost — compression at ingest is cheap relative to the I/O. Level 3 is the default; tuning is deferred until measurement.

**Alternatives considered:**
- **Custom framing.** Reject — reinvents the wheel; no debugging story; no streaming primitives we trust as much as libzstd.
- **`tar` + `gzip`.** Reject — `zstd` strictly dominates on ratio and speed for this workload.
- **Raw tar (no compression).** Reject — `state.db` and `events.db` compress well; storage cost matters at scale.
- **Sqlar / per-DB stream framing.** Reject — adds a custom format on top of a standard one.

### 2. Granularity — per-organization tarball

**Decision:** One archive per `(region, organization)` pair. Filename: `inferadb-{region}-{organization_slug}-{timestamp}.tar.zst`.

**Rationale:**
- Organizations are the residency boundary. PII never crosses orgs in REGIONAL keys, so per-org archives match the residency model: a single archive cannot accidentally span two orgs.
- Operators tend to back up at org granularity already (per-tenant SLAs, per-tenant compliance windows).
- Per-vault granularity is a sub-operation: an operator can extract `vaults/vault-{id}/*` from an org tarball with `tar` alone if they need a single vault. We do not need a separate per-vault format.
- Full-cluster tarballs are unwieldy at scale (hundreds of orgs × per-vault DBs) and obscure residency.

**API surface impact:** `CreateBackupRequest` (`proto/ledger/v1/ledger.proto:2126`) currently takes only `tag` and `base_backup_id`. It must gain `OrganizationSlug organization` (required) so the server knows which org tree to archive. `RestoreBackupRequest` already takes `backup_id`; it gains nothing — the manifest inside the archive identifies the destination org.

**Alternatives considered:**
- **Per-vault archives.** Reject — N archives per org, restore orchestration becomes "loop and pray" if one fails midway. Org-as-unit is atomic.
- **Full-cluster archives.** Reject — see above.
- **Per-region archives.** Reject — a region holds many orgs; one archive containing PII for many tenants violates blast-radius separation.

### 3. RMK / DEK envelope inclusion — sealed envelopes ride along

**Decision:** Archive contains the same on-disk ciphertext bytes for every DB, including per-vault DEK envelopes sealed by the RMK. The RMK itself is **never** included.

**Rationale:**
- Every per-vault `state.db` is encrypted at rest with a vault DEK. The DEK is sealed by the RMK and stored with the DB. Backup includes the sealed DEK; restore writes it back; on first DB open the system unwraps with the local RMK exactly as it does after a normal restart.
- The RMK lives in `{data_dir}/keys/`. `validate_not_keys_directory` (`backup.rs:222`) already enforces "key material must not appear in data backups" — this spec keeps that contract.
- This matches the operational reality: you do not restore an EU-region backup to a US-region node (residency violation), and you do not restore across an RMK rotation without the old envelope. Anyone trying to do either has a different problem (a key recovery problem) and that problem is not in scope.

**Constraint to document loudly:**

> A backup is unrestorable if the destination node's RMK fingerprint differs from the source's. There is no workaround inside the backup system — this is by design.

The manifest carries `rmk_fingerprint: String` (sha256 of the RMK public material, or a comparable opaque fingerprint). Restore performs a pre-flight check: if `node.rmk_fingerprint() != manifest.rmk_fingerprint`, fail with a `FAILED_PRECONDITION` carrying both fingerprints in `ErrorDetails.context`.

**Cross-region restoration:** explicitly not supported. Each region has its own RMK; re-wrapping every DEK across the archive is a separate concern (a "rekey" tool, not a "restore" tool). A backup tagged for region `eu-west-fra` will fail RMK fingerprint check on a node in `us-east-va` regardless of operator intent.

**Alternatives considered:**
- **Re-wrap DEKs at backup time with a "backup key".** Reject — adds a second key-management surface, expands blast radius if the backup key leaks, and breaks the "ciphertext is the same bytes" property that makes verification cheap.
- **Strip DEK envelopes; require operator to re-supply at restore.** Reject — operationally hostile and there's no good way for the operator to know per-vault DEK identity.

### 4. Restore semantics — stop, swap, restart

**Decision:** Restore replaces the per-org subtree on disk while the node is stopped. The `RestoreBackup` admin RPC stages and validates the archive when the node is up; the actual file replacement requires a stop-restore-restart cycle.

**Operational sequence:**

1. Operator calls `RestoreBackup(backup_id, confirm=true)` on the running node. The server validates the archive (manifest schema, RMK fingerprint, per-file checksums) and stages it under `{data_dir}/.restore-staging/{org_id}/`. No live data is touched yet.
2. Server returns `restored=false, staged=true, requires_restart=true` with a message instructing the operator to stop the node.
3. Operator stops the node (`SIGTERM` triggers normal `GracefulShutdown` six-phase drain — phase 5c fsyncs all four DBs before exit).
4. Operator runs the CLI: `inferadb-ledger restore apply --org-id {organization_id}`. This swaps `{data_dir}/{region}/{organization_id}/` for the staged tree (rename-over). Old tree moved to `.restore-trash/` for one boot of grace period.
5. Operator restarts the node. `RaftLogStore::open` reads `applied_durable` from the restored `raft.db`; `replay_crash_gap` runs against the restored WAL (typically zero entries because the source was checkpointed); state is live.

**Rationale:**
- Live restore is multi-week (quiesce all writers, drain in-flight applies, atomically swap, rebuild caches) and not on the critical path. The simple stop-and-swap path matches the existing post-restore contract — `restore_backup` already says "Restart the node to apply" (`admin.rs:2297`).
- Two-phase restore (stage online, swap offline) catches RMK-mismatch / corrupt-archive failures while the node can still respond, instead of producing a dead node on restart.
- Rename-over is atomic on POSIX. Power-loss mid-swap leaves either the old tree or the new tree intact, never a half-merge. (Open question 4 covers Windows.)

**Alternatives considered:**
- **Live in-place restore.** Defer — multi-week and unnecessary for the current product.
- **Single-step "RPC does it all".** Reject — the node can't restore its own running data tree. Would require a separate process anyway.
- **CLI-only, no RPC validation step.** Reject — RMK-fingerprint failures should be caught while the node is up and can return rich `ErrorDetails`, not as an opaque CLI exit code on a stopped node.

### 5. Schema versioning — `manifest.json` carries `schema_version`

**Decision:** Manifest carries `schema_version: u32` (initial value `1`). Schema bumps are **breaking** — a v1 reader rejects v2 archives with `SchemaVersionMismatch`. There is no compatibility shim layer.

**Rationale:**
- The existing logical-snapshot path stays internal: `RaftLogStore::install_snapshot` between peers, `LedgerSnapshotBuilder::build_snapshot` for in-cluster snapshot ship-over. Operator-facing `CreateBackup` / `RestoreBackup` switches to the new format.
- Hard cutover means the legacy `.backup` + `.meta.json` files are discarded. Operators who held existing logical-snapshot backups must take a fresh archive after the cutover.
- A no-shim policy keeps the code small and the test matrix bounded. Future bumps can land migration tooling outside the core path if needed.

**Alternatives considered:**
- **Dual format support during a window.** Reject — the user explicitly approved hard cutover; this is a new product. Code that reads both is code we have to maintain, test, and audit.
- **Versioned filename extension (e.g. `.tar.zst.v2`).** Reject — `schema_version` lives in the manifest because the manifest is read first; a versioned extension is redundant.

### 6. Encryption envelope details

For each DEK-sealed DB file in the archive:

- The on-disk file (`state.db`, `blocks.db`, etc.) is already encrypted at rest with its DEK.
- The DEK is sealed by the RMK; the sealed DEK is stored alongside the DB file (or in `_meta.db` — implementation-specific to the crypto layer).
- Backup includes the sealed DEK exactly as on disk.
- Restore writes the sealed DEK back; on first DB open the system unwraps it with the local RMK.
- If the local RMK does not match the sealing RMK, the unwrap fails — and the manifest's RMK fingerprint check will have rejected the archive long before that.

`_meta.db` is included verbatim. It carries `_meta:last_applied` (the apply-pipeline sentinel — `state.rs:477`); restoring the archive without it would leave the apply pipeline thinking it had to re-replay from index 0.

The pre-flight RMK check is the only crypto-level check at restore time. Per-DB checksum verification (manifest carries blake3 per file) catches archive corruption; envelope unwrap catches RMK mismatch only at first-open time after a restart, which is too late — hence the manifest fingerprint.

### 7. Manifest format

`manifest.json` (uncompressed, first member of the tar so it can be read with a single seek):

```json
{
  "schema_version": 1,
  "format": "inferadb-ledger-multi-db-backup",
  "region": "us-east-va",
  "organization_id": 42,
  "organization_slug": "snowflake-12345",
  "timestamp_micros": 1714000000000000,
  "rmk_fingerprint": "sha256:abc123...",
  "node_id_at_creation": 1,
  "region_height_at_creation": 184293,
  "applied_durable_at_creation": 184293,
  "vault_count": 7,
  "dbs": [
    { "path": "_meta.db",                               "size_bytes": 65536,    "checksum": "blake3:..." },
    { "path": "raft.db",                                "size_bytes": 1048576,  "checksum": "blake3:..." },
    { "path": "blocks.db",                              "size_bytes": 5242880,  "checksum": "blake3:..." },
    { "path": "events.db",                              "size_bytes": 2097152,  "checksum": "blake3:..." },
    { "path": "state/vault-7/state.db",                 "size_bytes": 8192000,  "checksum": "blake3:..." },
    { "path": "state/vault-7/raft.db",                  "size_bytes": 524288,   "checksum": "blake3:..." },
    { "path": "state/vault-7/blocks.db",                "size_bytes": 1048576,  "checksum": "blake3:..." },
    { "path": "state/vault-7/events.db",                "size_bytes": 524288,   "checksum": "blake3:..." }
  ],
  "created_by_app_version": "x.y.z",
  "tag": "pre-migration"
}
```

Notes on individual fields:

- `format` is a literal string sentinel — protects against an operator pointing the restore path at an unrelated `.tar.zst`.
- `rmk_fingerprint` is the sha256 of the RMK public material with the `sha256:` prefix — opaque to consumers; only equality matters.
- `applied_durable_at_creation` lets the restore path verify the archive was taken from a checkpointed state (i.e. all four DBs had reached durable apply at backup time).
- `dbs[*].checksum` uses blake3 (faster than sha256 on modern hardware, already in the workspace via `inferadb_ledger_types::hash`). Manifest fields documenting `checksum` strings carry an explicit `blake3:` prefix to avoid future ambiguity if the algorithm changes.
- `dbs[*].path` is relative to the per-org root (`{data_dir}/{region}/{organization_id}/`). The restore path joins them with the destination root.

## Migration plan

**Hard cutover. No backward compatibility.**

- The legacy `.backup` + `.meta.json` artefacts produced by the current `BackupManager::create_backup` are no longer readable post-cutover. `BackupManager::load_backup` and `restore_backup` reject non-archive inputs with `SchemaVersionMismatch` (or a new `LegacyFormatRejected` variant).
- Operators with existing backups must take a fresh archive **before** the cutover ships, then again **after** it ships. The pre-cutover backup is unrestorable on the post-cutover binary; the post-cutover backup uses the new format.
- The `BackupJob` background task switches to the new format on the same release. There is no flag to fall back. Per the user-approved defaults, this is acceptable for a new product.
- Documentation (`docs/architecture/durability.md`, operator runbook) updates in the same PR that lands the cutover.
- The page-level helpers in `backup.rs` (`create_full_page_backup`, `create_incremental_backup`, `restore_page_chain`, `PAGE_BACKUP_*` constants) are removed — they have been dead-code at the RPC boundary since Phase 1, and the new format does not use them.

## Testing strategy

Unit tests:
- Archive round-trip: build → parse → byte-identical DB files emerge.
- Manifest serde round-trip including unknown-future-field handling (reject if `schema_version != 1`).
- RMK fingerprint mismatch surfaces a typed error before any file is touched.
- Per-DB checksum mismatch surfaces a typed error and identifies the offending path.
- Empty-org case (zero vaults — only `_meta.db` + control-plane DBs).
- Many-vaults case (≥100) — manifest ordering deterministic; archive build does not OOM.

Integration tests (single-binary, `crates/server/tests/integration.rs`):
- Create org with N vaults, write entities + events, take backup, corrupt the per-org tree, restart, restore, verify entities + blocks + audit events all present and verifiable against the chain commitment.
- Create backup, simulate RMK rotation (regenerate `keys/`), attempt restore, expect pre-flight failure.
- Create backup mid-write (writer still issuing requests), verify the archive captures a consistent point-in-time image (every per-vault DB synced before tar).
- Restore with a corrupted member file (truncate one entry inside the tar), verify checksum failure surfaces with the path.

Property tests:
- Random vault counts × random entity counts × random event counts, round-trip the archive, assert state-root and chain-commitment invariants survive.

Stress / recovery:
- Backup → kill the node mid-archive-build, verify no half-written archive left in the destination (atomic-rename on the way out).
- Restore staging → kill the node mid-stage, verify staging directory cleanup on next start.

## Open questions

1. **Control-plane `blocks.db` vs per-vault `blocks.db` — does the per-org `blocks.db` still exist after Phase 4.1.a?** The current code references both per-org and per-vault `blocks.db`. If Phase 4.1.a fully retires the per-org file, the manifest `dbs` list should not include `blocks.db` at the org level. Needs ratification before implementation.
2. **`events.db` ownership transition timing.** Phase 4.3 (Task #180) splits events per-vault. If Phase 4.3 lands before this format ships, the manifest should only enumerate per-vault `events.db`. If after, the manifest should enumerate the per-org `events.db` until cutover. The format must work in both worlds — likely the manifest enumerates whatever is on disk, and the `dbs` array is authoritative.
3. **Should `BackupJob` (background scheduler) iterate all orgs, or accept an org list?** Today it backs up a single region without org granularity; under per-org archives it must enumerate orgs from the cluster directory. Reasonable default: all orgs in the local region. Need to confirm the SLA contract.
4. **Atomic rename-over on Windows.** POSIX rename is atomic; Windows `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING` is atomic on the same volume. If the InferaDB Ledger ever supports Windows hosts, this needs a platform note. Currently moot — no Windows target.
5. **Compression level for `zstd`.** Level 3 (the libzstd default) is the proposal; pages compress well at low levels and producer CPU is unlikely to be the bottleneck. If profiling shows CPU saturation, level 1 is the fallback. Level >3 is unlikely to be worth the CPU cost.
6. **Where does the staging trash live after a successful swap?** Proposal: `{data_dir}/.restore-trash/{timestamp}/{org_id}/`, cleaned by a background sweeper at next startup older than 24h. Needs operator-runbook input.
7. **Multi-org atomic restore.** Out of scope per per-org granularity decision — but if an operator restores 50 orgs and one fails on member 30, the cluster is in a mixed state. Documentation should spell out: "iterate orgs sequentially; failures are per-org and do not roll back successful peers." Confirm this is acceptable.
8. **CLI surface for `restore apply`.** Proposal: a new subcommand on the existing `inferadb-ledger` binary. Alternative: a separate `inferadb-ledger-restore` binary so the main binary cannot accidentally rename its own data directory while running. Needs operator-experience input.

## Implementation plan

Five sequential slices, each landable independently behind a feature flag until the cutover slice flips operators over.

### Slice 1 — `backup_archive` module (format only)

New module `crates/raft/src/backup_archive/` (or a dedicated `backup-archive` crate if the dependency surface justifies it):
- `manifest.rs` — `Manifest` struct + serde + version check.
- `writer.rs` — streaming archive build: takes a per-org root path, enumerates DBs, writes manifest + members through `tar::Builder` over `zstd::Encoder` over `File`.
- `reader.rs` — streaming archive read: parses manifest first, validates schema, exposes member iterator.
- Unit tests for manifest, writer, reader, round-trip, corrupt input rejection.

No RPC wiring yet. Pure format module.

### Slice 2 — `BackupManager` rewrite

Replace the snapshot-based code paths in `crates/raft/src/backup.rs`:
- New `BackupManager::create_archive(org_root, manifest_inputs) -> Result<BackupMetadata>` calls into Slice 1's writer.
- `list_backups` reads `manifest.json` from the archive (seek + read first member) instead of a sidecar `.meta.json`.
- `load_archive` returns a typed reader over the archive contents.
- Remove `create_full_page_backup`, `create_incremental_backup`, `restore_page_chain` and their constants — dead post-cutover.
- Update `BackupMetadata` to mirror manifest fields; sidecar `.meta.json` removed.

### Slice 3 — restore staging + CLI

Server-side:
- `RestoreBackup` RPC validates the archive, stages under `{data_dir}/.restore-staging/{org_id}/`, returns `requires_restart=true`.
- Pre-flight: verify `format` literal, `schema_version`, `rmk_fingerprint`, all `dbs[*].checksum`.
- Reject if the target node is the leader for any per-org group in that org (force operator to transfer leadership first — clean shutdown semantics).

CLI-side:
- `inferadb-ledger restore apply --org-id {id}` — atomic rename of staged tree over live tree. Operator must confirm the node is stopped (PID check + lockfile).
- `inferadb-ledger restore status` — list pending staged restores and their manifest summary.
- `inferadb-ledger restore abort --org-id {id}` — drop staging.

### Slice 4 — proto + RPC update

- `proto/ledger/v1/ledger.proto` — add `OrganizationSlug organization = 3` (required) to `CreateBackupRequest`. Add `bool requires_restart = 4`, `bool staged = 5`, `string staging_path = 6` to `RestoreBackupResponse`. Drop `base_backup_id` and `BACKUP_TYPE_INCREMENTAL` enum (dead post-cutover).
- `crates/proto/src/convert/admin.rs` — update conversions.
- `crates/services/src/services/admin.rs` — `create_backup` resolves the org slug and invokes the new manager path; `restore_backup` returns `staged` instead of `restored`.
- `crates/services/src/services/error_classify.rs` — map `BackupError::*` variants to the right `ErrorDetails`. New variants: `RmkFingerprintMismatch`, `ManifestSchemaMismatch`, `ArchiveCorrupt { path }`.

### Slice 5 — `BackupJob` rewrite + cutover

- `BackupJob::run` enumerates every org from the cluster directory and creates one archive per org per cycle.
- Remove the legacy snapshot path entirely.
- Update `docs/architecture/durability.md` § backup section.
- Update operator runbook.
- Document the cutover in `CHANGELOG.md` as a breaking change for existing deployments (per-deployment recovery guidance: "take a fresh backup before upgrading; legacy backups are unrestorable post-upgrade").

Each slice ends at a `/just-ci-gate` green run with new tests, in line with golden rule 14.
