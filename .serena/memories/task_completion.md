# Task Completion Checklist

## Before Marking Complete

1. **Format**: `cargo +nightly fmt --check`
2. **Clippy**: `cargo +1.92 clippy --workspace --all-targets -- -D warnings`
3. **Tests**: `cargo +1.92 test --workspace --lib`
4. **Build**: `cargo +1.92 build --workspace`

## Quick One-Liner
```bash
just ci
```

Or manually:
```bash
cargo +nightly fmt --check && cargo +1.92 clippy --workspace --all-targets -- -D warnings && cargo +1.92 test --workspace --lib
```

## Critical Rules
- Never use `.unwrap()` — use snafu `.context()` (server) or thiserror (SDK)
- Never use `panic!`, `todo!()`, `unimplemented!()`
- No unsafe code
- No TODO/FIXME/HACK comments
- Document all public items
- Use `?` for error propagation
