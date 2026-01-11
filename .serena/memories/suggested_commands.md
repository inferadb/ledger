# Development Commands

## Build
```bash
cargo build                    # Build all crates
cargo build --release          # Release build
cargo build -p ledger-types    # Build specific crate
```

## Testing
```bash
cargo test                     # Run all tests
cargo test -p ledger-storage   # Test specific crate
cargo test -- --nocapture      # With output
```

## Linting & Formatting
```bash
cargo +nightly fmt             # Format code (nightly required)
cargo +nightly fmt --check     # Check formatting
cargo clippy --all-targets     # Run clippy
cargo clippy -- -D warnings    # Clippy with warnings as errors
```

## Documentation
```bash
cargo doc --open               # Generate and open docs
```

## Running
```bash
cargo run -p ledger-server     # Run server (dev)
cargo run -p ledger-server --release  # Run server (release)
```

## Full Check Before Commit
```bash
cargo +nightly fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
```
