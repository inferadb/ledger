---
name: add-new-entity
description: Use when introducing a new entity family (analogous to `User`, `Vault`, `App`, `Team`, `Invitation`) that needs dual-ID addressing across storage, gRPC, and SDK. Orchestrates `{Entity}Id(i64)` + `{Entity}Slug(u64)` newtypes, `SlugResolver` wiring, storage keys with tier validation, proto conversions, RPC exposure, and SDK surface. Composes `/add-storage-key`, `/add-proto-conversion`, and `/new-rpc`.
---

# add-new-entity

A new entity family means: new IDs, new storage keys, new proto messages, new `SlugResolver` lookups, new RPCs, new SDK methods. Doing any one of these in isolation leaves the system in a half-committed state (an RPC that can't resolve slugs; a key with no tier entry; a proto message with no conversion). This skill is the orchestrating checklist.

Every past entity introduction (vault, app, team, invitation) followed this shape. MEMORY.md entries for each include specific bugs that followed from skipping a step.

## Before you start

Write down answers before opening any file:

1. **Entity name.** Singular form (`User`, `Vault`, `App`). Use this consistently.
2. **Contains PII?** If yes → `REGIONAL` residency. No exceptions.
3. **Residency pattern:**
   - **Pattern 1** (REGIONAL-only): full record at bare key in regional tier. No GLOBAL counterpart. (`user:`, `team:`, `invite:`)
   - **Pattern 2** (GLOBAL skeleton + REGIONAL overlay): `{entity}:` (no PII) + `{entity}_profile:` (PII). Merged on read. (`app:` + `app_profile:`, `org:` + `org_profile:`)
   - **Pattern 3** (GLOBAL-only): no PII, no regional presence. (`signing_key:`, `refresh_token:`)
4. **Which RPCs?** Create/Get/List/Update/Delete — not every entity needs all five. List only what the product requires.
5. **Does this entity live inside an org / vault?** If yes, keys are scoped (`{entity}:{org}:{id}`).
6. **Secondary indexes?** E.g. email → user_id. Each index is a separate `_idx:` key with its own tier + erasure rules.

If you cannot answer all six, stop and ask.

## Checklist

### 1. Newtype IDs and slugs

- `crates/types/src/types/ids.rs`:
  - `define_id!({Entity}Id)` — internal `i64`.
  - `define_slug!({Entity}Slug)` — external Snowflake `u64`.
- Internal IDs appear in storage keys, state layer, Raft state machine.
- Slugs appear in proto messages, SDK signatures, wide-events logging.
- **Slugs NEVER enter the state layer.** If you write a storage key that contains a slug, you've made a mistake.

### 2. Proto types

- Edit `proto/ledger/v1/ledger.proto`:
  - Message `{Entity}Slug { uint64 slug = 1; }` for external references (matches `VaultSlug`, `OrganizationSlug`).
  - Domain messages (`{Entity}` with `{Entity}Slug` inside, plus fields).
  - Request/response messages for each RPC.
- Run `just proto` — regenerates `crates/proto/src/generated/ledger.v1.rs`.
- **Do NOT hand-edit generated code.** The PreToolUse hook blocks it.

### 3. Storage keys — invoke `/add-storage-key`

Per the chosen residency pattern, add all required keys:

- Bare `{entity}:` key (every pattern has one, with different content).
- `{entity}_profile:` overlay key (Pattern 2 only).
- `_dir:{entity}:` directory (if cross-region routing is required).
- `_idx:{entity}_…:` for each secondary lookup.
- `_shred:{entity}:` if the entity holds crypto-shredded material.

Each new constant needs a `KEY_REGISTRY` entry — this is the single most common place people have skipped a step.

### 4. Proto conversions — invoke `/add-proto-conversion`

- `crates/proto/src/convert/{entity}.rs` — one file per domain.
- `From<Domain> for Proto` (infallible direction) and `TryFrom<Proto> for Domain` (fallible direction — validates size, range, required fields).
- **Conversions stay slug-typed on the proto side.** Do not resolve slug → ID inside conversion code. That's the service layer's job.

### 5. State-layer operations

- `crates/state/src/engine.rs` or a new module: `StorageEngine::create_{entity}`, `get_{entity}`, etc.
- Every write passes through `SystemKeys::validate_key_tier(&key, expected_tier)`.
- For Pattern 2 entities: reads merge GLOBAL skeleton + REGIONAL profile. Writes split them into the correct tiers.
- Secondary-index maintenance is in the **same transaction** as the primary write. Do not leave it to a post-commit hook.

### 6. `SlugResolver` methods

- `crates/services/src/services/slug_resolver.rs`:
  - `resolve_{entity}_slug_to_id(slug: {Entity}Slug) -> Result<{Entity}Id, …>`
  - `resolve_{entity}_id_to_slug(id: {Entity}Id) -> Result<{Entity}Slug, …>`
- Every gRPC handler resolves slug → ID at the top before calling the state layer.

### 7. gRPC service — invoke `/new-rpc`

For each RPC identified above, follow the `new-rpc` skill: proto method, server impl, version-gate exemption list (probably NOT exempt), `ErrorDetails` on failures, audit hook (for mutations), wide-event context.

### 8. SDK methods

- `crates/sdk/src/` — add one method per RPC.
- Method signature takes `{entity}_slug: u64` (the raw Snowflake) — the SDK does not expose newtype wrappers.
- All SDK methods use `with_retry_cancellable(method: &str, ...)` — the method-name string is used for retry metrics.
- Errors decode to `SdkError` with `ServerErrorDetails` populated from the proto.

### 9. Validation

- `crates/types/src/validation.rs`:
  - Add max-length / character-whitelist rules for any string fields.
  - Wire into request size limits if a new aggregate payload is introduced.
- Both SDK and server must call the validator — validation on one side is not validation.

### 10. Wide events + audit

- `crates/types/src/events.rs`: add `set_{entity}_slug()` helpers to `RequestContext`.
- Write-path RPCs emit `AuditEvent` in `WriteService` / `AdminService`.
- Mutations also emit Prometheus counters (`ledger_{entity}_created_total`, etc.) — keep the label set minimal.

### 11. Erasure

- If the entity holds PII, add it to the erasure routine (see `_shred:` keys and `_audit:` emission).
- Erasure deletes: primary, overlay, every `_idx:` entry, and emits an `_audit:` record.
- Proptests should verify that post-erasure, no key referencing the erased ID remains.

### 12. Tests

- **Registry tests**: every new key constant appears in `KEY_REGISTRY`.
- **Conversion tests**: proto ↔ domain round-trip for every message; proptests for validation boundaries.
- **Integration tests**: create → get → list → update → delete flow for each RPC. Goes in `crates/server/tests/` as a submodule of `integration.rs` (never `mod common;`).
- **Residency tests**: confirm PII does not appear under any GLOBAL key for a Pattern 1 or 2 entity.

### 13. Gate

- `just ci` must pass — no "pre-existing issue" exceptions.
- `proto-reviewer`, `data-residency-auditor`, `snafu-error-reviewer`, and `test-isolation-auditor` agents audit the diff; address their findings before declaring done.

## Common mistakes

| Mistake                                                               | Effect                                                                     | Detected by                                                     |
| --------------------------------------------------------------------- | -------------------------------------------------------------------------- | --------------------------------------------------------------- |
| PII field under a GLOBAL key                                          | Silent residency violation                                                 | `data-residency-auditor`                                        |
| Forgot `KEY_REGISTRY` entry for new constant                          | `validate_key_tier` becomes a no-op for that key                           | `add-storage-key` checklist, registry tests                     |
| Storage key contains `{Entity}Slug(u64)` instead of `{Entity}Id(i64)` | Keys shift whenever slugs rotate; invalidates snapshots                    | code review; grep for slug types in `system/keys.rs`            |
| Proto conversion resolves slug → ID internally                        | Couples proto crate to state layer; breaks independence                    | `proto-reviewer`                                                |
| RPC handler skips `SlugResolver`                                      | Handler passes raw `u64` into state — compile fails or wrong entity loaded | type system (if newtypes are used), integration tests otherwise |
| Secondary-index write in a post-commit hook                           | Index drift if the hook fails; stale references survive erasure            | erasure proptests                                               |
| Missing `AuditEvent` on a mutation                                    | Compliance gap                                                             | review against existing Write/Admin handlers                    |
| Missing `set_{entity}_slug()` on `RequestContext`                     | Wide-event logs lose entity correlation                                    | canonical-log-line tests                                        |

## References

- `CLAUDE.md` of `types`, `state`, `proto`, `services`.
- Skills: `/add-storage-key`, `/add-proto-conversion`, `/new-rpc`, `/define-error-type`, `/use-bon-builder`.
- Agents: `data-residency-auditor`, `proto-reviewer`, `snafu-error-reviewer`, `test-isolation-auditor`.
- Precedent: the namespace→organization and vault-slug rollouts in MEMORY.md cover every step above in production.
