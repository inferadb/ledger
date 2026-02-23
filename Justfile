# InferaDB Ledger development commands
# Requires: just (https://github.com/casey/just)

# Default recipe: show available commands
default:
    @just --list

# Rust toolchain versions
rust := "1.92"
nightly := "nightly"

# ============================================================================
# Build
# ============================================================================

# Build all crates
build:
    cargo +{{rust}} build --workspace

# Build in release mode
build-release:
    cargo +{{rust}} build --workspace --release

# Build a specific crate
build-crate crate:
    cargo +{{rust}} build -p {{crate}}

# ============================================================================
# Test
# ============================================================================

# Run fast tests (~15s) - quick validation during development
test-fast:
    cargo +{{rust}} test --workspace --lib

# Run standard tests (~30s) - includes integration tests
test:
    cargo +{{rust}} test --workspace

# Run full test suite (~5min) - includes ignored/slow tests
test-full:
    cargo +{{rust}} test --workspace -- --include-ignored

# Run property tests with high iteration count (default: 10000)
test-proptest cases="10000":
    PROPTEST_CASES={{cases}} cargo +{{rust}} test --workspace --lib -- proptest

# Run tests for a specific crate
test-crate crate:
    cargo +{{rust}} test -p {{crate}}

# Run a specific test with output
test-one name:
    cargo +{{rust}} test {{name}} -- --nocapture

# ============================================================================
# Benchmarks
# ============================================================================

# Run all benchmarks locally
bench:
    cargo +{{rust}} bench --workspace

# Run a specific benchmark suite
bench-suite suite:
    cargo +{{rust}} bench --workspace --bench {{suite}}

# Run benchmarks with bencher output for CI (machine-readable)
bench-ci:
    cargo +{{rust}} bench -p inferadb-ledger-store --bench btree_bench -- --output-format bencher
    cargo +{{rust}} bench -p inferadb-ledger-server --bench read_bench -- --output-format bencher
    cargo +{{rust}} bench -p inferadb-ledger-server --bench write_bench -- --output-format bencher
    cargo +{{rust}} bench -p inferadb-ledger-raft --bench logging_bench -- --output-format bencher

# ============================================================================
# Code Quality
# ============================================================================

# Format code (requires nightly)
fmt:
    cargo +{{nightly}} fmt

# Check formatting without modifying files
fmt-check:
    cargo +{{nightly}} fmt --check

# Run clippy linter
clippy:
    cargo +{{rust}} clippy --workspace --all-targets -- -D warnings

# Run all checks (format + clippy + test) - use before committing
check: fmt-check clippy test

# Quick check (format + clippy only) - faster pre-commit validation
check-quick: fmt-check clippy

# CI validation: format + clippy + doc + tests (excludes integration tests that require a running cluster)
ci: fmt-check clippy doc-check
    cargo +{{rust}} test --workspace --exclude inferadb-ledger-sdk --exclude inferadb-ledger-server
    cargo +{{rust}} test -p inferadb-ledger-sdk --lib
    cargo +{{rust}} test -p inferadb-ledger-server --lib

# Pre-PR validation: regenerate protos, autoformat, lint, and test
ready: proto fmt clippy test

# ============================================================================
# Documentation
# ============================================================================

# Build documentation
doc:
    cargo +{{rust}} doc --workspace --no-deps

# Check documentation builds without warnings (broken links, missing docs)
doc-check:
    RUSTDOCFLAGS="-D warnings" cargo +{{rust}} doc --workspace --no-deps

# Build and open documentation in browser
doc-open:
    cargo +{{rust}} doc --workspace --no-deps --open

# ============================================================================
# Protobuf
# ============================================================================

# Update pre-generated proto code for crates.io publishing
proto:
    cargo +{{rust}} clean -p inferadb-ledger-proto
    cargo +{{rust}} build -p inferadb-ledger-proto
    cp target/debug/build/inferadb-ledger-proto-*/out/ledger.v1.rs crates/proto/src/generated/

# Lint protobuf definitions
proto-lint:
    cd proto && buf lint

# ============================================================================
# Run
# ============================================================================

# Run the server in development mode
run:
    cargo +{{rust}} run -p inferadb-ledger-server

# Run the server in release mode
run-release:
    cargo +{{rust}} run -p inferadb-ledger-server --release

# ============================================================================
# Maintenance
# ============================================================================

# Clean build artifacts
clean:
    cargo clean

# Update dependencies
update:
    cargo update

# Check for unused dependencies (requires cargo-udeps)
udeps:
    cargo +{{nightly}} udeps --workspace
