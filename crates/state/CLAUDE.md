# state

Entity/Relationship stores, state roots, system services, storage-key discipline. Everything that goes into `store` goes through `StorageEngine` or `StateLayer` — no crate above calls `store` directly.

## Key types

| Type | File | Purpose |
| --- | --- | --- |
| `StorageEngine` | `engine.rs` | Store wrapper with transaction helpers |
| `StateLayer` | `state.rs` | `StateMachine` impl — applies blocks, computes state roots |
| `SystemKeys` + `KeyTier` + `KeyFamily` | `system/keys.rs` | Key builders + tier-validation registry |
| `ShardManager` | `shard.rs` | Orgs → Raft group mapping (not persisted) |
| `EntityStore` | `entity.rs` | Key-value with TTL + versioning |
| `RelationshipStore` | `relationship.rs` + `relationship_index.rs` | Authorization tuples `(resource, relation, subject)` |
| `BlockArchive` | `block_archive.rs` | Blockchain block storage per vault |

## Storage-key discipline (load-bearing)

1. **Every `_PREFIX` / `_KEY` constant MUST have a `KEY_REGISTRY` entry.** Missing entries silently disable `validate_key_tier()` — the exact bug this infrastructure exists to catch.
2. **Keys are built only via `SystemKeys` builders.** Never `format!("_idx:foo:{id}")` at call sites — that bypasses tier validation.
3. **Call `SystemKeys::validate_key_tier(&key, expected_tier)` immediately before every put/insert.**
4. **Storage keys take internal newtype IDs** (`OrganizationId(i64)`, `VaultId(i64)`, …) — never slugs. Slugs only appear at the gRPC boundary.
5. Prefix ordering invariant: `:` (0x3A) < `_` (0x5F), so `app:{org}:` scans never match `app_profile:`. Don't introduce prefixes that break this.

## Data residency patterns

| Pattern | Shape | Examples |
| --- | --- | --- |
| **1 · REGIONAL-only** | Full record at bare key in regional tier. No GLOBAL counterpart. | `user:`, `team:`, `user_email:`, `invite:` |
| **2 · GLOBAL skeleton + overlay** | GLOBAL skeleton (PII empty) + REGIONAL `{entity}_profile:` PII. Merged on read. | `app:` + `app_profile:`, `org:` + `org_profile:` |
| **3 · GLOBAL-only** | No PII, no regional presence. | `signing_key:`, `refresh_token:` |

PII → REGIONAL. Always. If unsure, it is REGIONAL.

## Secondary-index + erasure

- `_idx:` entries are maintained transactionally with primary writes.
- Erasure must delete both the primary and every `_idx:` entry that references it. Crypto-shredding (`_shred:` destruction) alone leaves `_idx:` pointing at garbage.
- Emit an `_audit:` record on erasure.

## Related tooling

- Skill: `/add-storage-key`, `/add-new-entity`
- Agent: `data-residency-auditor`
