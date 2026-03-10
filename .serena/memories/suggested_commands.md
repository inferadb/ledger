# Development Commands

## Using `just` (preferred)
```bash
just check        # pre-commit: fmt + clippy + test
just check-quick  # fast pre-commit: fmt + clippy only
just ci           # CI validation: fmt + clippy + doc-check + test
just ready        # pre-PR: proto + fmt + clippy + test
just test         # unit tests only (--workspace --lib)
just test-ff      # unit tests, stop on first failure
just test-integration     # integration tests (spawns clusters)
just test-integration-ff  # integration tests, stop on first failure
just fmt          # format code (nightly)
just clippy       # run linter
just doc-check    # build rustdoc with -D warnings
just proto        # regenerate protobuf code
just run          # run server (dev mode)
```

## Using cargo directly
```bash
cargo +1.92 build --workspace              # Build all crates
cargo +1.92 build -p inferadb-ledger-types # Build specific crate
cargo +1.92 test --workspace --lib         # Unit tests
cargo +1.92 test -p inferadb-ledger-state  # Test specific crate
cargo +1.92 test <name> -- --nocapture     # Single test with output
cargo +nightly fmt                         # Format (nightly required)
cargo +1.92 clippy --workspace --all-targets -- -D warnings
cargo +1.92 doc --workspace --no-deps      # Generate docs
```
