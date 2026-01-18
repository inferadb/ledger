# Task Completion Checklist

## Before Marking Complete

1. **Format**: `cargo +nightly fmt`
2. **Clippy**: `cargo +1.85 clippy --all-targets -- -D warnings`
3. **Tests**: `cargo test`
4. **Build**: `cargo build`

## Quick One-Liner
```bash
cargo +nightly fmt && cargo +1.85 clippy --all-targets -- -D warnings && cargo test
```

## Critical Rules
- Never use `.unwrap()` - always snafu error handling
- Never use `panic!` in non-test code
- No unsafe code
- Document all public items
- Use `?` for error propagation
