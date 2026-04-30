---
name: add-new-entity
description: Use when introducing a new entity family (analogous to `User`, `Vault`, `App`, `Team`, `Invitation`) that needs dual-ID addressing across storage, the wire protocol, and SDK. Orchestrates `{Entity}Id(i64)` + `{Entity}Slug(u64)` newtypes, `SlugResolver` wiring, storage keys with tier validation, wire types, RPC exposure, and SDK surface. Composes `/add-storage-key` and `/new-rpc`.
---

# add-new-entity

A new entity family means: new IDs, new storage keys, new wire types, new `SlugResolver` lookups, new RPCs, new SDK methods. Doing any one of these in isolation leaves the system in a half-committed state (an RPC that can't resolve slugs; a key with no tier entry; a wire request with the wrong ID type). This skill is the orchestrating checklist.

Every past entity rollout (vault, app, team, invitation) followed this shape. MEMORY.md entries for each include specific bugs that followed from skipping a step.

## Before you start

Write down answers before opening any file:

1. **Entity name.** Singular form (`User`, `Vault`, `App`). Use this consistently.
2. **Contains PII?** If yes → `REGIONAL` residency. No exceptions.
3. **Residency pattern:**
   - **Pattern 1** (REGIONAL-only): full record at bare key in regional tier. No GLOBAL counterpart. (`user:`, `team:`, `invite:`)
   - **Pattern 2** (GLOBAL skeleton + REGIONAL overlay): `{entity}:` (no PII) + `{entity}_profile:` (PII). Merged on read. (`app:` + `app_profile:`, `org:` + `org_profile:`)
   - **Pattern 3** (GLOBAL-only): no PII, no regional presence. (`signing_key:`, `refresh_token:`)
4. **Which RPCs?** Create / Get / List / Update / Delete — not every entity needs all five. List only what the product requires.
5. **Does this entity live inside an org / vault?** If yes, keys are scoped (`{entity}:{org}:{id}`).
6. **Secondary indexes?** E.g. email → user_id. Each index is a separate `_idx:` key with its own tier + erasure rules.

If you cannot answer all six, stop and ask.

## Checklist

### 1. Newtype IDs and slugs

- `crates/types/src/types/ids.rs`:
  - `define_id!({Entity}Id)` — internal `i64`.
  - `define_slug!({Entity}Slug)` — external Snowflake `u64`.
- Internal IDs appear in storage keys, the state layer, the Raft state machine.
- Slugs appear in wire messages, SDK signatures, wide-events logging.
- **Slugs NEVER enter the state layer.** If you write a storage key that contains a slug, you've made a mistake.

### 2. Wire types (`crates/wire/src/services/<service>.rs`)

- Add `pub struct <Entity> { ... }` with the typed `<Entity>Slug` for the external identifier plus the entity's externally-visible fields.
- Derive `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`.
- Binary fields use `bytes::Bytes`; map fields use `BTreeMap`; optional timestamps use `Option<u64>` (UNIX nanoseconds).
- Re-export the slug type in `crates/wire/src/services/shared.rs` if other services reference it.
- Add postcard round-trip tests for the new struct.

### 3. Storage keys — invoke `/add-storage-key`

Per the chosen residency pattern, add all required keys:

- Bare `{entity}:` key (every pattern has one, with different content).
- `{entity}_profile:` overlay key (Pattern 2 only).
- `_dir:{entity}:` directory (if cross-region routing is required).
- `_idx:{entity}_…:` for each secondary lookup.
- `_shred:{entity}:` if the entity holds crypto-shredded material.

Each new constant needs a `KEY_REGISTRY` entry — this is the single most common place people have skipped a step. Audited by `data-residency-auditor`.

### 4. State-layer operations

- `crates/state/src/engine.rs` or a new module: `StorageEngine::create_{entity}`, `get_{entity}`, etc.
- Every write passes through `SystemKeys::validate_key_tier(&key, expected_tier)` in the same transaction as the put/insert.
- Construct keys via `SystemKeys::*` builders only — never `format!("_idx:foo:{id}")` at the call site.
- For Pattern 2 entities: reads merge GLOBAL skeleton + REGIONAL profile. Writes split them into the correct tiers.
- Secondary-index maintenance is in the **same transaction** as the primary write. Do not leave it to a post-commit hook.

### 5. `SlugResolver` methods

- `crates/services/src/services/slug_resolver.rs`:
  - `resolve_{entity}_slug_to_id(slug: {Entity}Slug) -> Result<{Entity}Id, …>`
  - `resolve_{entity}_id_to_slug(id: {Entity}Id) -> Result<{Entity}Slug, …>`
- Every wire-protocol handler resolves slug → ID at the top before calling the state layer.

### 6. Wire RPCs — invoke `/new-rpc`

For each RPC identified in step 4 of the pre-flight, follow the `new-rpc` skill: wire request/response types, `define_protocol!` registration, server impl, `WireError` with rich context, audit hook (for mutations), wide-event context.

### 7. SDK methods

- `crates/sdk/src/` — add one method per RPC.
- Method signature takes the typed slug newtype (the SDK does not expose raw `u64` parameters); call `.value()` only when constructing the wire request.
- All SDK methods use `with_retry_cancellable(method: &str, &pool, ...)` — the method-name string is used for retry metrics labels and circuit-breaker key.
- Errors decode to `SdkError` with `ServerErrorDetails` populated as `Box<ServerErrorDetails>` (clippy `result_large_err`).

### 8. Validation

- `crates/types/src/validation.rs`:
  - Add max-length / character-whitelist rules for any string fields.
  - Wire into request size limits if a new aggregate payload is introduced.
- Both SDK and server must call the validator — validation on one side is not validation.

### 9. Wide events + audit

- `crates/types/src/events.rs`: add `set_{entity}_slug()` helper to `RequestContext`.
- Write-path RPCs emit `AuditEvent` in `WriteService` / `AdminService`.
- Mutations also emit Prometheus counters (`ledger_{entity}_created_total`, etc.) — keep the label set minimal.

### 10. Erasure

- If the entity holds PII, add it to the erasure routine (see `_shred:` keys and `_audit:` emission).
- Erasure deletes: primary, overlay, every `_idx:` entry, and emits an `_audit:` record.
- Proptests should verify that post-erasure, no key referencing the erased ID remains.

### 11. Tests

- **Registry tests**: every new key constant appears in `KEY_REGISTRY`.
- **Wire round-trip tests**: postcard encode/decode for every new wire type; proptests for fields with combinatorial structure.
- **Integration tests**: create → get → list → update → delete flow for each RPC. Lives in `crates/server/tests/` as a submodule of `integration.rs` (never `mod common;`).
- **Residency tests**: confirm PII does not appear under any GLOBAL key for a Pattern 1 or 2 entity.

### 12. Gate

- `just ci` must pass — no "pre-existing issue" exceptions.
- `wire-reviewer`, `data-residency-auditor`, `snafu-error-reviewer`, and `test-isolation-auditor` agents audit the diff; address their findings before declaring done.

## Common mistakes

| Mistake                                                               | Effect                                                                     | Detected by                                                     |
| --------------------------------------------------------------------- | -------------------------------------------------------------------------- | --------------------------------------------------------------- |
| PII field under a GLOBAL key                                          | Silent residency violation                                                 | `data-residency-auditor`                                        |
| Forgot `KEY_REGISTRY` entry for new constant                          | `validate_key_tier` becomes a no-op for that key                           | `add-storage-key` checklist, registry tests                     |
| Storage key contains `{Entity}Slug(u64)` instead of `{Entity}Id(i64)` | Keys shift whenever slugs rotate; invalidates snapshots                    | code review; grep for slug types in `system/keys.rs`            |
| Wire request takes raw `u64` instead of typed `{Entity}Slug`          | Loses type-level dual-ID discipline; SDK callers can't see the contract    | `wire-reviewer`                                                 |
| RPC handler skips `SlugResolver`                                      | Handler passes raw `u64` into state — compile fails or wrong entity loaded | type system (newtypes), integration tests otherwise             |
| `HashMap` in a wire payload                                           | Non-deterministic postcard encoding — breaks signing / round-trip tests    | `wire-reviewer`, postcard tests                                 |
| Secondary-index write in a post-commit hook                           | Index drift if the hook fails; stale references survive erasure            | erasure proptests                                               |
| Missing `AuditEvent` on a mutation                                    | Compliance gap                                                             | review against existing Write/Admin handlers                    |
| Missing `set_{entity}_slug()` on `RequestContext`                     | Wide-event logs lose entity correlation                                    | canonical-log-line tests                                        |

## References

- `CLAUDE.md` of `types`, `state`, `wire`, `services`.
- Skills: `/add-storage-key`, `/new-rpc`, `/define-error-type`, `/use-bon-builder`.
- Agents: `data-residency-auditor`, `wire-reviewer`, `snafu-error-reviewer`, `test-isolation-auditor`.
- Precedent: the namespace→organization and vault-slug rollouts in MEMORY.md cover every step above in production.
