# Code Style & Conventions

## Rust Edition & Toolchain
- Rust 2024 edition (stable since 1.85), MSRV 1.92
- Nightly toolchain required for formatting and `cargo udeps`

## Formatting (rustfmt)
- Max line width: 100 characters
- 4 spaces indentation (no tabs)
- Unix newlines
- Imports/modules reordered automatically

## Linting Rules
**Denied:**
- `unsafe_code` - No unsafe code allowed
- `unwrap_used` - Use snafu error handling (server) or thiserror (SDK)
- `panic` - No panics in production code
- `todo` - No `todo!()` allowed
- `unimplemented` - No `unimplemented!()` allowed

**Warned:**
- `missing_docs` - Document public items
- `expect_used` - Prefer proper error handling

## Error Handling
- **Server crates** (types, store, proto, state, consensus, raft, services, server): snafu with implicit location tracking and `.context(XxxSnafu)?` propagation. Never thiserror or anyhow.
- **SDK crate**: thiserror for consumer-facing error types (`SdkError`, `ResolverError`)
- Classify via `ErrorCode` enum in `crates/types/src/error_code.rs`
- API-facing errors: `message: String` for gRPC serialization
- No `.unwrap()` / `.expect()` outside tests — use `.context()`, `.ok_or()`, or `?`

## Builders (bon)
- `#[derive(bon::Builder)]` for simple structs
- `#[bon]` impl block with `#[builder]` for fallible constructors
- `#[builder(into)]` for `String` fields
- Match `#[builder(default)]` with `#[serde(default)]` for config

## Documentation
- Crate-level docs with `//!` comments
- Document all public types and functions
- Rust code examples: ` ```no_run ` (skipped by `cargo test`, validated by `cargo doc`)
- Non-Rust content: ` ```text ` (ASCII diagrams, pseudo-code, sample output)
- **Never** ` ```ignore `

## Naming
- Types: PascalCase (OrganizationId, VaultBlock)
- Functions: snake_case (bucket_id, sha256_concat)
- Constants: SCREAMING_SNAKE_CASE (EMPTY_HASH)
- Newtype IDs: `define_id!` / `define_slug!` macros in `crates/types/src/types/ids.rs`
  - `TokenVersion` is manual (needs `Default` + `increment()`)
