# Code Style & Conventions

## Rust Edition & Toolchain
- Rust 2024 edition, version 1.92
- Nightly toolchain required for formatting

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
- `todo` - No `todo!()` or `unimplemented!()` allowed

**Warned:**
- `missing_docs` - Document public items
- `expect_used` - Prefer proper error handling

## Error Handling
- **Server crates**: snafu with implicit location tracking and `.context()` propagation
- **SDK crate**: thiserror for consumer-facing error types
- No `.unwrap()` — use `.context()`, `.ok_or()`, or `?`

## Builders (bon)
- `#[derive(bon::Builder)]` for simple structs
- `#[bon]` impl block with `#[builder]` for fallible constructors
- `#[builder(into)]` for String fields
- Match `#[builder(default)]` with `#[serde(default)]` for config

## Documentation
- Crate-level docs with `//!` comments
- Document all public types and functions
- Code examples use ` ```no_run ` (never `ignore` or `text`)

## Naming
- Types: PascalCase (OrganizationId, VaultBlock)
- Functions: snake_case (bucket_id, sha256_concat)
- Constants: SCREAMING_SNAKE_CASE (EMPTY_HASH)
- Newtype IDs: `define_id!` / `define_slug!` macros in types/src/types.rs
