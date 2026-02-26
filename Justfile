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

# Run unit tests - inline #[cfg(test)] modules only
test:
    cargo +{{rust}} test --workspace --lib

# Run unit tests with fail-fast — stops after first failure for faster iteration.
test-ff:
    cargo +{{rust}} test --workspace --lib -- --fail-fast

# Run integration tests - spawns real clusters, requires ports.
# Parallelism is capped at 8 to avoid resource contention (each test spawns
# 1-3 Raft nodes with their own tokio runtimes, file backends, and gRPC servers).
test-integration:
    cargo +{{rust}} test -p inferadb-ledger-server --test integration -- --test-threads=8

# Run integration tests with fail-fast — stops after first failure for faster iteration.
test-integration-ff:
    cargo +{{rust}} test -p inferadb-ledger-server --test integration -- --test-threads=8 --fail-fast

# Run stress/scale tests — heavy workloads for throughput and scale validation
test-stress:
    cargo +{{rust}} test -p inferadb-ledger-server --test stress -- --test-threads=4

# Run stress/scale tests with fail-fast — stops after first failure for faster iteration.
test-stress-ff:
    cargo +{{rust}} test -p inferadb-ledger-server --test stress -- --test-threads=4 --fail-fast

# Run external tests against a live cluster (requires LEDGER_ENDPOINTS env var).
# Skips gracefully if no cluster is available.
test-external:
    cargo +{{rust}} test -p inferadb-ledger-server --test external -- --test-threads=1 --nocapture

# Start a local cluster and run external tests against it.
test-external-cluster:
    ./scripts/run-server-integration-tests.sh

# Run crash recovery tests — dual-slot commit protocol verification in the store crate.
test-recovery:
    cargo +{{rust}} test -p inferadb-ledger-store --test crash_recovery

# Run crash recovery tests with fail-fast.
test-recovery-ff:
    cargo +{{rust}} test -p inferadb-ledger-store --test crash_recovery -- --fail-fast

# Run full test suite including stress/scale tests
test-all:
    cargo +{{rust}} test --workspace

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

# CI validation: format + clippy + doc + unit tests
ci: fmt-check clippy doc-check test

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
