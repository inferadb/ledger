---
name: add-proto-conversion
description: Checklist for adding a proto↔domain conversion in InferaDB Ledger. Use when adding or modifying a `From`/`TryFrom` impl in `crates/proto/src/convert/**`, or when a new proto message or domain type is introduced that crosses the gRPC boundary. Covers submodule placement, `From` vs `TryFrom` choice, error surface, round-trip proptests, and tests.rs coverage.
---

# add-proto-conversion

Proto conversions are a narrow, high-churn surface. They cross the gRPC boundary in both directions: domain → proto for responses, proto → domain (validated) for requests. Picking the wrong submodule scatters related code; picking `From` where `TryFrom` is needed hides validation failures as panics; skipping a round-trip proptest lets asymmetric impls ship undetected. This is the conversion-only checklist.

For full new-RPC flow (proto + service impl + SDK + audit + quotas), use the `new-rpc` skill and treat this skill as the "step 2" zoom-in.

## Layout

`crates/proto/src/convert/` contains:

| File             | Conversions                                                                                            |
| ---------------- | ------------------------------------------------------------------------------------------------------ |
| `mod.rs`         | Module registration + doc                                                                              |
| `identifiers.rs` | Slug/ID wrappers: `OrganizationSlug`, `VaultSlug`, `UserSlug`, `AppSlug`, etc.                         |
| `domain.rs`      | Core domain: `Entity`, `Relationship`, `VaultEntry`, `BlockRetentionPolicy`, `MerkleProof`, timestamps |
| `operations.rs`  | Write-side request shapes: `SetCondition`, operation unions                                            |
| `statuses.rs`    | Lifecycle enums: org/user/invitation status                                                            |
| `tokens.rs`      | JWT / token types: `ValidatedToken`, event entries                                                     |
| `credentials.rs` | Auth credentials: passkey, TOTP, recovery codes, email verify                                          |
| `tests.rs`       | Unit + property tests for all of the above                                                             |

If a new conversion fits no existing submodule, add a new file — but err toward placing it in `domain.rs` or the closest match. Proliferating one-conversion modules defeats the dedup work from Phase 2 Task 15.

## Checklist

### 1. Pick the submodule

By feel, in order of preference:

- Wraps an ID newtype / Snowflake slug → `identifiers.rs`
- Describes "a thing that exists" (entity, relationship, vault entry, policy, proof) → `domain.rs`
- Describes a mutation shape / condition / operation → `operations.rs`
- A lifecycle / state enum → `statuses.rs`
- A token, claim, or signing artifact → `tokens.rs`
- A credential (password, passkey, TOTP, recovery code) → `credentials.rs`
- None of the above → ask before creating a new submodule

### 2. Pick `From` vs `TryFrom`

| Direction                          | Trait     | Reason                                                                         |
| ---------------------------------- | --------- | ------------------------------------------------------------------------------ |
| Domain → proto (outbound response) | `From`    | Domain types are already validated; serialization is infallible                |
| Proto → domain (inbound request)   | `TryFrom` | Proto carries raw wire types; validation can fail (length, enum, utf-8, range) |

Hard rules:

- Never wrap a validation that can fail inside a `From` impl that panics on bad input. Always prefer `TryFrom` with an explicit error.
- Never use `.unwrap()` / `.expect()` inside a conversion. The conversion is the validation boundary — if it unwraps, a malformed client request crashes the server.
- `From<&T> for ProtoT` (by reference) is preferred over `From<T> for ProtoT` when `T` is non-Copy and owns allocations we don't want to consume — the existing `From<&Entity> for proto::Entity` is the template.

### 3. Pick the error type for `TryFrom`

Proto-layer conversions return `tonic::Status` directly when the caller is a gRPC service boundary; otherwise return a snafu error from `types` that maps cleanly.

- `Status::invalid_argument(..)` for wire-level validation (bad enum variant, missing required field, bad UTF-8).
- `Status::out_of_range(..)` for numeric / length violations.
- Do **not** invent a new error crate for conversions. Reuse `Status` or the existing typed error family.

Error messages should be specific enough to debug from a client's perspective: name the field, the constraint, and the actual value where not sensitive.

### 4. Handle the sentinel values

The codebase has a handful of zero-sentinel conventions — match them:

- `expires_at == 0` in proto ↔ `None` in domain (never-expires).
- Empty `String` in proto ↔ `None` in domain for optional identifiers.
- `0` for `Option<u64>` slugs on inbound: either the field is required (fail) or it's optional (`None`). Do not silently accept `0` as a valid slug.

Document each sentinel in a `///` doc comment on the impl so the convention stays discoverable.

### 5. Round-trip invariants

If the message is bidirectional (domain → proto → domain), it MUST have a round-trip test.

- **Unit round-trip** in `tests.rs`: construct a domain value, convert both ways, assert equality.
- **Property test** via `proptest` for any message with combinatorial structure (options, vecs, enums): generate random valid domain values, assert `from_proto(to_proto(x)) == x`. Strategies live in `crates/test-utils/src/strategies.rs` — reuse or extend.
- Use `inferadb_ledger_test_utils::strategies::*` — not ad-hoc `prop_oneof!` inside `tests.rs`.

Asymmetric conversions (lossy, one-way) do not need a round-trip proptest but MUST document the lossy direction in the impl's doc comment.

### 6. Deduplication check

Phase 2 Task 15 dedup'd `convert_operation` / `convert_set_condition` helpers that had grown duplicates. Before adding a conversion, grep the convert/ tree for an existing impl for the same types — even partial overlaps (different field subsets) should be consolidated.

### 7. Tests

Every new `From`/`TryFrom` must land in `convert/tests.rs` with:

- **Happy path**: one representative instance, both directions.
- **Failure path** (for `TryFrom`): at least one test per validation branch. Bad enum variant, empty required field, length violation, etc.
- **Sentinel cases**: if the message carries `expires_at`, test both `0` and a real timestamp. If it carries an optional slug, test both `None` and `Some`.
- **Round-trip proptest** (if bidirectional).

### 8. CI gate

`just check-quick` catches style / missing-trait issues; `just ci` runs the full proptest suite. `just test-proptest` runs 10k iterations (nightly CI default).

## Common mistakes

- **`.unwrap()` inside a proto-layer `From` impl** — turns a malformed request into a server-side panic. Use `TryFrom` and propagate `Status::invalid_argument`.
- **Placing the conversion in the wrong submodule** — e.g., an invitation status enum landing in `domain.rs` instead of `statuses.rs`. Scatters related code, loses grep-ability.
- **Skipping the reference variant** — `From<Entity> for proto::Entity` forces callers to clone. Prefer `From<&Entity>` for non-Copy domain types; add the by-value variant only if a specific caller benefits.
- **Asymmetric round-trip without documentation** — the proto type drops a field, the domain reconstructs it with a default. Perfectly fine, but the impl doc must say so explicitly.
- **New proptest strategy ad-hoc in `tests.rs`** — strategies belong in `crates/test-utils/src/strategies.rs` so other proptests (state, storage) can reuse them.
- **`i64` vs `u64` drift** — internal IDs are `i64`, Snowflake slugs are `u64`. A conversion that accepts `i64` from the wire is probably accepting an internal ID externally, which is a dual-ID violation.

## References

- `crates/proto/src/convert/mod.rs` — module registration and top-level doc
- `crates/proto/src/convert/tests.rs` — test style template
- `crates/test-utils/src/strategies.rs` — shared proptest strategies
- `new-rpc` skill — end-to-end RPC checklist that this skill zooms into
- `CLAUDE.md` — dual-ID rules, error-handling discipline, snafu conventions
