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

# ============================================================================
# Documentation
# ============================================================================

# Build documentation
doc:
    cargo +{{rust}} doc --workspace --no-deps

# Build and open documentation in browser
doc-open:
    cargo +{{rust}} doc --workspace --no-deps --open

# ============================================================================
# Protobuf
# ============================================================================

# Generate protobuf code (requires buf)
proto:
    cd proto && buf generate

# Lint protobuf definitions
proto-lint:
    cd proto && buf lint

# ============================================================================
# Run
# ============================================================================

# Run the server in development mode
run config="config.toml":
    cargo +{{rust}} run -p inferadb-ledger-server -- --config {{config}}

# Run the server in release mode
run-release config="config.toml":
    cargo +{{rust}} run -p inferadb-ledger-server --release -- --config {{config}}

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
