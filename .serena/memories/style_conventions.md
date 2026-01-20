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
- `unwrap_used` - Use snafu error handling
- `panic` - No panics in production code

**Warned:**
- `missing_docs` - Document public items
- `expect_used` - Prefer proper error handling
- `todo` - Allowed but flagged

## Error Handling
- Use `snafu` with backtraces
- No `.unwrap()` - use `.context()` or `.ok_or()`
- Use `?` operator for propagation

## Documentation
- Crate-level docs with `//!` comments
- Document all public types and functions

## Naming
- Types: PascalCase (NamespaceId, VaultBlock)
- Functions: snake_case (bucket_id, sha256_concat)
- Constants: SCREAMING_SNAKE_CASE (EMPTY_HASH)
