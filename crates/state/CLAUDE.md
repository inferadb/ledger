# CLAUDE.md — state

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

Entity/Relationship stores, state roots, system services, storage-key discipline. This is where domain writes become persisted keys. Every write into `store` flows through `StorageEngine` or `StateLayer` here — no crate above calls `store` directly. A bug in the key builders, tier registry, or residency patterns is a data-residency compliance incident, not a software bug.

## Load-Bearing Files

These files are load-bearing — their invariants ripple beyond the local file. Not off-limits; use caution and understand the ramifications before editing.

| File                        | Reason                                                                                                                                                                                                                                                 |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `src/system/keys.rs`        | `SystemKeys`, `KeyTier`, `KeyFamily`, `KEY_REGISTRY`, `validate_key_tier`. Every storage key in the system goes through here. A missing registry entry silently disables tier validation; a wrong tier leaks PII. Audited by `data-residency-auditor`. |
| `src/state.rs`              | `StateLayer` apply path. Non-determinism here = state divergence across nodes = data loss.                                                                                                                                                             |
| `src/engine.rs`             | `StorageEngine` transaction semantics; all callers assume atomicity.                                                                                                                                                                                   |
| `src/shard.rs`              | `ShardManager` — orgs → Raft group mapping. Changing shard assignment after orgs exist requires a migration plan.                                                                                                                                      |
| `src/relationship_index.rs` | Authorization-tuple index. A bug here grants wrong permissions.                                                                                                                                                                                        |

## Owned Surface

- **`StorageEngine<B>`, `StateLayer<B>`** — generic over `StorageBackend`. `StateLayer` exposes two apply entrypoints: `apply_operations` (strict-durable; for admin / recovery callers that must return with on-disk durability — `AdminService::recover_vault`, `check_integrity`, `ReadService` historical replay, `AutoRecoveryJob`) and `apply_operations_lazy` (lazy via `commit_in_memory`; for every IN-APPLY-PIPELINE admin arm reachable from `apply_request_with_events`).
- **`SystemKeys::*`** key builders, `KEY_REGISTRY`, `KeyTier` (`Global` / `Regional`), `KeyFamily` (`Entity`, `Directory`, `Index`, `Meta`, `Sequence`, `Shred`, `Temporary`, `Audit`).
- **Stores**: `EntityStore`, `RelationshipStore`, `RelationshipIndex`, `BlockArchive`.
- **`ShardManager`** — org-to-shard routing; not persisted, rebuilt from GLOBAL state on start.
- **`StateError`** variants + `ErrorCode` mappings.

## Storage-key families (recap — authoritative table in root CLAUDE.md)

| Prefix    | Tier     | Residency                                            |
| --------- | -------- | ---------------------------------------------------- |
| _(bare)_  | varies   | Primary domain record                                |
| `_dir:`   | GLOBAL   | Directory routing                                    |
| `_idx:`   | varies   | Secondary index                                      |
| `_meta:`  | GLOBAL   | Sequences, saga state, membership                    |
| `_shred:` | REGIONAL | Crypto-shredding keys                                |
| `_tmp:`   | REGIONAL | TTL-bound ephemeral (e.g. `_tmp:saga_pii:{saga_id}`) |
| `_audit:` | GLOBAL   | Compliance erasure records                           |

Ordering invariant: `:` (0x3A) < `_` (0x5F). `app:{org}:*` scans never match `app_profile:*` keys.

## Residency patterns

| Pattern | Shape                                                                               | Examples                                         |
| ------- | ----------------------------------------------------------------------------------- | ------------------------------------------------ |
| **1**   | REGIONAL-only bare key. No GLOBAL counterpart.                                      | `user:`, `team:`, `user_email:`, `invite:`       |
| **2**   | GLOBAL skeleton (PII empty) + REGIONAL `{entity}_profile:` overlay; merged on read. | `app:` + `app_profile:`, `org:` + `org_profile:` |
| **3**   | GLOBAL-only, no PII.                                                                | `signing_key:`, `refresh_token:`                 |

PII → REGIONAL. Always. If residency is unclear for a new record, it is REGIONAL until proven otherwise.

## Test Patterns

- **Registry tests**: every `*_PREFIX` constant appears in `KEY_REGISTRY` with matching tier + family.
- **Tier-violation tests**: write to wrong tier via `validate_key_tier` and assert the expected panic/error variant.
- **Residency proptests**: Pattern-1 records never appear under any GLOBAL key; Pattern-2 reads merge skeleton + overlay; Pattern-3 records have no PII fields.
- **Prefix-ordering proptest**: random keys under a new family never fall inside any existing scan range (and vice versa).
- **Erasure proptests**: post-erasure, no key referencing the erased ID remains.

## Local Golden Rules

1. **Every `*_PREFIX` / `*_KEY` constant has a matching `KEY_REGISTRY` entry** (root rule 3). The constant test at the bottom of `keys.rs` enforces this; don't weaken it.
2. **Keys are built only via `SystemKeys::*` builders.** Inline `format!("_idx:foo:{id}")` at call sites bypasses both the registry and `validate_key_tier` — a root-rule-6 violation.
3. **Every write calls `SystemKeys::validate_key_tier(&key, expected_tier)` in the same transaction** (root rule 6). The engine helpers enforce this; don't write through raw backend handles.
4. **Storage keys take internal `{Entity}Id(i64)` newtypes only** (root rule 4). A `*Slug` parameter on any function in `src/system/keys.rs` is a bug. Slug translation is `services` crate territory.
5. **Pattern 2 reads merge skeleton + overlay.** A read that returns only the GLOBAL skeleton (PII-empty) without attempting the REGIONAL profile is a silent data-missing bug.
6. **Secondary-index writes are transactional with the primary write.** Erasure deletes primary + every `_idx:` referring to it + emits `_audit:` record. Crypto-shredding (`_shred:` destruction) alone leaves `_idx:` pointing at decryptable-as-garbage references.
7. **Saga PII persists only under `_tmp:saga_pii:{saga_id}`** (Regional + Temporary). Saga steps writing PII to `_meta:` or any GLOBAL key are residency bugs — caught by `data-residency-auditor`.
8. **A new family prefix must preserve `:` < `_` ordering.** Introducing a prefix that falls inside an existing scan range silently breaks that scan.
9. **`ShardManager` is not persisted.** It is rebuilt from GLOBAL directory state on each start. Don't add persistence to `ShardManager` without a migration plan — state-machine determinism depends on the rebuild being deterministic from the directory.
10. **`BlockArchive::append_block` is idempotent-by-height.** A re-append at an existing height with a byte-identical block is a no-op; a re-append with a different block at that height is an error. This property is load-bearing for crash recovery: `RaftLogStore::replay_crash_gap` re-drives the apply pipeline through committed entries, and `append_block` lands in that path. Breaking the idempotency property re-breaks WAL-replay recovery. See `docs/architecture/durability.md` for the full recovery contract.

11. **Storage layout is per-organization under each region, with per-vault state under each organization.** The org-level layout is `{data_dir}/{region}/{organization_id}/` for raft.db (the org's shared per-org WAL + applied-state-core) plus org-scoped state.db / blocks.db / events.db that hold every non-vault key the org owns. The data-region group uses `OrganizationId(0)` (the system organization); per-organization groups use their assigned `OrganizationId`. The `global/0/` path hosts the cluster control plane. **Per-vault state lives at `{data_dir}/{region}/{organization_id}/state/vault-{vault_id}/`** holding that vault's own state.db, blocks.db, and events.db (vault shards share the parent org's WAL — see root rule 17 + raft rule 17). The shared per-org WAL with group-commit is the M4 footing for `replay_shared_wal_for_org` parallel replay. Introducing a new Raft group requires a new directory at the appropriate tier — never co-mingle two groups' state DBs, and never flatten the directory layout (e.g., `{data_dir}/{region}/` or `{data_dir}/{region}/{org}/state/`) without reworking every apply / checkpoint / backup path in lockstep.

12. **Region directory entries are GLOBAL Directory family.** `_dir:region:{name}` (`SystemKeys::REGION_DIRECTORY_PREFIX` in `keys.rs`) holds the post-`ProvisionRegion` `RegionDirectoryEntry` — name, `protected` flag, `conf_epoch`, voter set. Every node consults this directory at boot to decide which regions to start locally; that read path is part of the cluster control plane and must stay GLOBAL. Vault slug → id translation lives in the Index family at `_idx:vault:slug:{slug}` via `SystemKeys::vault_slug_key` (per-γ; the bare body lives on the per-org state per the per-γ migration). Both keys flow through `SystemRequest::RegisterVaultDirectoryEntry` / `UnregisterVaultDirectoryEntry` proposals against the system group; never write either key path outside that proposal.
