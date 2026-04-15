---
name: add-storage-key
description: Checklist for adding a new storage key to InferaDB Ledger's system keyspace. Use when adding a `*_PREFIX` / `*_KEY` constant to `crates/state/src/system/keys.rs`, introducing a new entity family, or anytime a new key is written via StorageEngine/StateLayer. Covers tier (Global vs Regional), family prefix (`_dir:`/`_idx:`/`_meta:`/`_shred:`/`_tmp:`/`_audit:` or bare), Pattern 1/2/3 data-residency decision, KEY_REGISTRY wiring, tier validation, and tests.
---

# add-storage-key

Storage keys in InferaDB are load-bearing. A key written to the wrong tier is a silent data-residency bug (PII leaks GLOBAL, or structural data becomes region-isolated). A missing `KEY_REGISTRY` entry disables tier validation at the write boundary. A prefix that violates ordering (`:` < `_`) can cause scans to pick up the wrong family.

This checklist exists to make those invariants explicit.

## Before you start

Confirm with the user or the design:

1. **Entity** — what domain concept does this key address (user email, app profile, invitation, shredding key, audit record)?
2. **Contains PII?** — if yes, the answer is almost always Regional or Regional-overlay.
3. **Residency pattern** — is this Pattern 1 (REGIONAL-only), Pattern 2 (GLOBAL skeleton + REGIONAL overlay), or Pattern 3 (GLOBAL-only)?
4. **Access path** — direct lookup, prefix scan, or secondary-index lookup?
5. **Lifetime** — permanent, TTL-bound (`_tmp:`), or crypto-shredded (`_shred:`)?

## Checklist

### 1. Decide the tier

| Tier       | Use for                                                                     |
| ---------- | --------------------------------------------------------------------------- |
| `Global`   | Directory routing, cross-region indexes, sequences, saga state, membership  |
| `Regional` | PII, crypto-shredding keys, ephemeral onboarding, per-user regional records |

PII → `Regional`. Always. If unsure, it is `Regional`.

### 2. Pick the prefix family

Prefix families and their `KeyFamily` enum variants:

| Prefix       | `KeyFamily` | Meaning                                                |
| ------------ | ----------- | ------------------------------------------------------ |
| _(bare)_     | `Entity`    | Primary domain record (keyed by canonical ID)          |
| `_dir:`      | `Directory` | Slug/ID → region/status routing                        |
| `_idx:`      | `Index`     | Secondary index (reverse lookup)                       |
| `_meta:`     | `Meta`      | Sequences, saga state, node/membership bookkeeping     |
| `_meta:seq:` | `Sequence`  | Sequence counters (subset of Meta)                     |
| `_shred:`    | `Shred`     | Crypto-shredding material, destroyed on erasure        |
| `_tmp:`      | `Temporary` | TTL-bound ephemeral records (onboarding, email verify) |
| `_audit:`    | `Audit`     | Compliance/erasure trail (append-only)                 |

Ordering invariant: `:` (0x3A) < `_` (0x5F). This is why bare-entity scans like `app:{org}:*` never match `app_profile:` keys. A new prefix MUST preserve this — never introduce a family that would collide with an existing scan range.

### 3. Pick the residency pattern

| Pattern                           | Shape                                                                                               | Examples                                         |
| --------------------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| **1 · REGIONAL-only**             | Full record at bare key in regional tier. No GLOBAL counterpart.                                    | `user:`, `team:`, `user_email:`, `invite:`       |
| **2 · GLOBAL skeleton + overlay** | GLOBAL skeleton (PII empty) at bare key + REGIONAL `{entity}_profile:` holding PII. Merged on read. | `app:` + `app_profile:`, `org:` + `org_profile:` |
| **3 · GLOBAL-only**               | No PII, no regional presence.                                                                       | `signing_key:`, `refresh_token:`                 |

Pattern 2 requires two keys (skeleton + profile). Add both. Name the overlay `{entity}_profile:` for consistency.

### 4. Add the constant(s)

In `crates/state/src/system/keys.rs`, add the `PREFIX` (or full `KEY` for singletons) constants on the `SystemKeys` impl:

- Naming: `{ENTITY}_{FAMILY}_PREFIX` — e.g., `USER_DIRECTORY_PREFIX`, `APP_PROFILE_PREFIX`, `INVITE_EMAIL_INDEX_PREFIX`.
- Prefix string must match the family (Directory constants start with `_dir:`, etc.).
- Include a doc comment describing the key shape: `_idx:user_email:{email_hash}` → `UserId`.

### 5. Wire into `KEY_REGISTRY`

Every new `PREFIX` / `KEY` constant MUST have a corresponding `KeyRegistryEntry` in `SystemKeys::KEY_REGISTRY`:

- `name` → the constant name as a `&'static str` (used in diagnostics).
- `tier` → the `KeyTier` chosen in step 1.
- `family` → the `KeyFamily` chosen in step 2.

Omitting the registry entry silently disables `validate_key_tier()` for that key — which is exactly the bug class this infrastructure exists to catch.

### 6. Add a key builder

Add a `pub fn` on `SystemKeys` (or the relevant family module) that constructs the full key from its canonical components. Builders MUST:

- Use internal newtype IDs (`UserId`, `OrganizationId`, `VaultId`, …) — never raw `i64` or slugs.
- Encode binary/opaque inputs via `encode_hex()` for readability and to avoid delimiter collisions.
- Be the ONLY way callers construct this key. Never concatenate the prefix at the call site.

### 7. Enforce the tier at every write site

At each callsite that writes the new key, call:

```rust
SystemKeys::validate_key_tier(&key, KeyTier::Global /* or Regional */);
```

immediately before the `put` / `insert`. This is cheap (debug-assert in hot paths, registry lookup otherwise) and is the backstop that turns cross-tier writes into loud test failures instead of silent residency drift.

### 8. Secondary index bookkeeping (if `_idx:`)

- Secondary indexes must be maintained transactionally with the primary write — same `StorageEngine` transaction.
- On delete/erasure, the index entry MUST also be removed. Crypto-shredding alone does not delete `_idx:` keys.
- Document the index shape in the builder doc comment: `_idx:user_email:{sha256(email)}` → `{user_id}`.

### 9. Erasure wiring (if PII)

If the key holds PII or references shredded material:

- Add the key prefix to the erasure routine (see `_shred:` keys and `_audit:` emission).
- Verify `_shred:` destruction leaves the overlay record unreadable.
- Emit a `_audit:` record on erasure.

### 10. Tests

- **Registry test** in `keys.rs`: extend the existing "every constant has a registry entry" test if present; otherwise assert the new entry directly.
- **Prefix-ordering proptest**: if introducing a new family prefix, add a proptest that random keys under the new family never fall inside any existing scan range (and vice versa). This is the test that catches the `:` vs `_` class of bugs.
- **Tier-violation test**: a negative test that writing the key to the wrong tier via `validate_key_tier` panics/returns the expected error.
- **Round-trip test**: builder output parses back to the original components (for keys with structured suffixes).

### 11. Proto + SDK (only if externally visible)

Most storage keys are internal. If the key fronts an external surface (e.g., new directory lookup RPC), follow the `new-rpc` skill for proto + conversions + SDK + version layer.

### 12. CI gate

- `just check-quick` (fmt + clippy) first — catches missing registry entry as a clippy/test failure.
- `just ci` as the final gate.

## Common mistakes

- **Missing `KEY_REGISTRY` entry**: new constant compiles but tier validation is a no-op. Always audit the registry when adding a constant.
- **Wrong tier for PII**: new `{entity}_profile:` overlay keyed under `KeyTier::Global`. The code compiles; the residency guarantee breaks.
- **Concatenating prefix at the call site**: `format!("_idx:foo:{id}")` instead of `SystemKeys::foo_index_key(id)`. The builder is the only legitimate construction path — anything else bypasses tier validation.
- **Introducing a prefix that collides**: e.g., a new family starting with `_d` would interleave with `_dir:`. Scans break silently. Stick to the existing families unless there's a strong reason.
- **Forgetting secondary-index cleanup on erasure**: `_idx:` entries outlive their primary, returning stale (now-decryptable-as-garbage) references.
- **Slug in the storage key**: keys are an internal surface — use `OrganizationId(i64)`, never `OrganizationSlug(u64)`. Slugs only appear at the gRPC boundary.

## References

- `crates/state/src/system/keys.rs` — `SystemKeys`, `KeyTier`, `KeyFamily`, `KEY_REGISTRY`, `validate_key_tier`
- `CLAUDE.md` — storage key prefixes table, Pattern 1/2/3 data residency
- `DESIGN.md` — state-layer architecture
- `PII.md` — residency requirements for PII-bearing records
