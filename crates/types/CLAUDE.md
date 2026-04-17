# CLAUDE.md — types

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

Foundation crate. Defines newtype IDs, errors, config types, Merkle primitives, hashing, and input validation. Zero runtime dependencies on other workspace crates — every other crate depends on `types`. A change here ripples through the workspace; treat every commit as a potential API break.

## Load-Bearing Files

These files are load-bearing — their invariants ripple beyond the local file. Not off-limits; use caution and understand the ramifications before editing.

| File                | Reason                                                                                                                                                                |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/types/ids.rs`  | `define_id!` and `define_slug!` macros. Every internal ID and external slug in the workspace uses these. Renaming a generated impl breaks compile everywhere at once. |
| `src/error_code.rs` | `ErrorCode` enum. Must stay exhaustive — SDK retryability and `suggested_action` classification depend on every variant mapping.                                      |
| `src/merkle.rs`     | Merkle proof construction/verification. `verify()` assumes power-of-2 leaf counts; callers rely on this.                                                              |
| `src/hash.rs`       | Content hashing primitives. Changing the hash function breaks every persisted block.                                                                                  |

## Owned Surface

- **IDs / slugs** (`src/types/ids.rs`): `OrganizationId/Slug`, `VaultId/Slug`, `UserId/Slug`, `AppId/Slug`, `TeamId/Slug`, `InviteId/Slug`, plus `SigningKeyId`, `RefreshTokenId`, `UserEmailId`, `EmailVerifyTokenId`, `ClientAssertionId`, `UserCredentialId`, `TokenVersion`.
- **Errors**: top-level error enum + `ErrorCode`.
- **Config types**: `StorageConfig`, `RaftConfig`, `BatchConfig`, `HealthCheckConfig`, `RuntimeConfig`, `ValidationConfig`, etc.
- **Crypto / codec**: `hash.rs`, `merkle.rs`, `codec.rs`, `snowflake.rs`.

## Test Patterns

- Property tests for ID round-tripping and `define_id!`/`define_slug!` macro output.
- Merkle proptests: restrict input to power-of-2 leaf counts; `verify()` panics otherwise.
- Strategies live in `crates/test-utils/src/strategies.rs` — reuse, do not duplicate.
- Config tests: deserialize YAML/JSON fixture, then builder-construct the same value, assert equality. Catches `#[serde(default)]` vs `#[builder(default)]` drift.

## Local Golden Rules

1. **`ErrorCode` stays exhaustive.** Every error variant in every server crate maps to exactly one `ErrorCode`. Adding a variant without wiring all `code()` impls is a breaking change caught by `snafu-error-reviewer`.
2. **New IDs use `define_id!`; new slugs use `define_slug!`.** The only manual newtype is `TokenVersion` (needs `Default` + `increment()`). Introducing a bare `pub struct FooId(i64);` bypasses the macro's `Display`, `FromStr`, `From`/`Into` contract.
3. **`Merkle::verify()` works only for power-of-2 leaf counts.** Proptests must constrain input; production callers must pad to power-of-2 before calling. Non-power-of-2 input returns `false` silently.
4. **Every `#[builder(default = …)]` pairs with a matching `#[serde(default = …)]`.** Mismatches mean a config that deserializes from disk produces different values than the builder on the same inputs.
5. **Config validation is a last resort.** Prefer compile-time required fields on bon builders over runtime `validate()` checks. A field that must be set should not have a default.
