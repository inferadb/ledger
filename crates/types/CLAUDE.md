# types

Hash primitives, Merkle proofs, config, errors, token types, newtype IDs. Zero runtime dependencies on other workspace crates — this is the foundation layer.

## Responsibilities

- Newtype IDs and slugs (dual-ID: internal `i64` vs external Snowflake `u64`)
- Config structs with serde + fallible builders
- Error codes shared across crates
- Hash + Merkle primitives
- Token types, validation rules, snowflake generation

## Key files

| File | Purpose |
| --- | --- |
| `types/ids.rs` | ID/slug newtypes via `define_id!` / `define_slug!` macros |
| `error.rs` | Top-level error enum (snafu) |
| `error_code.rs` | `ErrorCode` classification for gRPC/SDK |
| `config/` | `StorageConfig`, `RaftConfig`, `BatchConfig`, etc. — public fields + bon fallible `new` |
| `hash.rs`, `merkle.rs` | Content hashing + proofs |
| `validation.rs` | Character whitelists, size limits, request validation |
| `codec.rs` | Serialization helpers |
| `snowflake.rs` | Slug generation |
| `token.rs`, `invitation.rs`, `onboarding.rs` | Token-type domain helpers |

## Dual-ID rules

- Internal IDs (`OrganizationId`, `VaultId`, `UserId`, `AppId`, `TeamId`, `InviteId`, …) are sequential `i64` — used in storage keys, state layer, Raft state machine.
- External slugs (`OrganizationSlug`, `VaultSlug`, …) are Snowflake `u64` — used in gRPC APIs and the SDK.
- Both generated via `define_id!` / `define_slug!` macros. `TokenVersion(u64)` is manual because it needs `Default` + `increment()`.
- `SlugResolver` (in `services`) translates at the gRPC boundary — never do this translation inside the state layer.

## Conventions

- Errors: snafu only — see `/define-error-type` skill and `snafu-error-reviewer` agent.
- Config: pair `#[builder(default = …)]` with `#[serde(default)]` so deserialization and builder stay in sync — see `/use-bon-builder` skill.
- Prefer compile-time required fields on builders over runtime `validate()` where possible.
- `ErrorCode` must stay exhaustive — every error variant in any server crate classifies into it.

## Related tooling

- Skill: `/use-bon-builder`, `/define-error-type`, `/add-new-entity`
- Agent: `snafu-error-reviewer`
