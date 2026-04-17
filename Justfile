# InferaDB Ledger development commands
# Requires: just (https://github.com/casey/just)

# Default recipe: grouped help. Run 'just --list' for the complete catalog.
default:
    @echo "InferaDB Ledger — developer commands"
    @echo ""
    @echo "First time?"
    @echo "  just setup          # install toolchain via mise"
    @echo ""
    @echo "Daily loop:"
    @echo "  just check          # fmt-check + clippy + unit tests"
    @echo "  just fix            # auto-fix formatting and clippy suggestions"
    @echo "  just watch          # re-run 'just check' on file change"
    @echo ""
    @echo "Before a PR:"
    @echo "  just ci             # full pre-PR gate (adds doc-check)"
    @echo "  just proto          # regenerate proto code (run after .proto edits)"
    @echo ""
    @echo "Troubleshooting:"
    @echo "  just doctor         # check toolchain, ports, disk"
    @echo ""
    @echo "Full catalog:  just --list"

# ============================================================================
# Setup
# ============================================================================

# First-run setup: install pinned Rust toolchain and dev tools via mise.
setup:
    mise trust
    mise install
    @echo ""
    @echo "Setup complete. Next:"
    @echo "  just check     # fast developer loop"
    @echo "  just ci        # full pre-PR gate"
    @echo "  just --list    # full catalog"

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
    cargo +{{rust}} test -p inferadb-ledger-server --test integration -- --test-threads=4

# Run integration tests with fail-fast — stops after first failure for faster iteration.
test-integration-ff:
    cargo +{{rust}} test -p inferadb-ledger-server --test integration -- --test-threads=4 --fail-fast

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

# Start a local cluster and run server external tests against it.
test-external-cluster:
    ./scripts/run-integration-tests.sh server

# Start a local cluster and run SDK e2e tests against it.
test-sdk-cluster:
    ./scripts/run-integration-tests.sh sdk

# Stress: smoke-level throughput tests (~1 min).
test-stress-quick:
    ./scripts/stress-quick.sh

# Stress: throughput measurement against advisory targets.
test-stress-throughput:
    ./scripts/stress-throughput.sh

# Stress: scale-correctness tests (state root parity, snapshot determinism, watch).
test-stress-correctness:
    ./scripts/stress-correctness.sh

# End-to-end cluster lifecycle test (bootstrap, join, transfer, shutdown, rebuild).
test-cluster-lifecycle:
    ./scripts/cluster-lifecycle-test.sh

# Binary-level crash recovery drill: SIGKILL a node mid-write, verify convergence.
test-crash-recovery-drill:
    ./scripts/crash-recovery.sh

# gRPC channel caching regression test (TIME_WAIT accumulation).
test-port-consumption:
    ./scripts/check-port-consumption.sh

# Run crash recovery tests — dual-slot commit protocol verification in the store crate.
test-store-recovery:
    cargo +{{rust}} test -p inferadb-ledger-store --test crash_recovery

# Run crash recovery tests with fail-fast.
test-store-recovery-ff:
    cargo +{{rust}} test -p inferadb-ledger-store --test crash_recovery -- --fail-fast

# Run full test suite including stress/scale tests
test-all:
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

# Fast developer loop: fmt-check + clippy + unit tests. Run during iteration.
check: fmt-check clippy test

# Canonical pre-PR gate: fmt-check + clippy + doc-check + tests. Matches CI and the /just-ci-gate skill.
ci: fmt-check clippy doc-check test

# Auto-fix formatting and lints. Run when `check` complains about style.
fix:
    cargo +{{nightly}} fmt
    cargo +{{rust}} clippy --workspace --all-targets --fix --allow-dirty --allow-staged

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

# Regenerate proto bindings from proto/ledger/v1/*.proto and copy to crates/proto/src/generated/.
proto:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo +{{rust}} clean -p inferadb-ledger-proto
    cargo +{{rust}} build -p inferadb-ledger-proto
    shopt -s nullglob
    dirs=(target/debug/build/inferadb-ledger-proto-*/out)
    if [ "${#dirs[@]}" -ne 1 ]; then
        echo "error: expected exactly one build output dir, found ${#dirs[@]}:" >&2
        printf '  %s\n' "${dirs[@]}" >&2
        echo "hint: run 'cargo clean -p inferadb-ledger-proto' and retry" >&2
        exit 1
    fi
    out="${dirs[0]}"
    cp "$out/ledger.v1.rs" crates/proto/src/generated/
    cp "$out/ledger_v1_descriptor.bin" crates/proto/src/generated/
    echo "proto code regenerated from $out"

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

# Prune stale build artifacts without destroying the current cache.
# Removes incremental compilation caches and dep artifacts older than
# `age` days (default: 7), plus the llvm-cov target directory.
# Safe to run regularly — current builds are unaffected.
clean-stale age="7":
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Pruning build artifacts older than {{age}} days..."
    before=$(du -sh target/ 2>/dev/null | cut -f1)
    # Stale incremental compilation caches (cargo never GCs these)
    find target/debug/incremental -maxdepth 1 -type d -mtime +{{age}} -exec rm -rf {} + 2>/dev/null || true
    # Stale dependency artifacts (old hashes from previous compilations)
    find target/debug/deps -maxdepth 1 -type f -mtime +{{age}} -delete 2>/dev/null || true
    # llvm-cov instrumented builds (fully regenerated each run)
    rm -rf target/llvm-cov-target 2>/dev/null || true
    after=$(du -sh target/ 2>/dev/null | cut -f1)
    echo "target/: ${before} → ${after}"

# Update dependencies
update:
    cargo update

# Check for unused dependencies (requires cargo-udeps)
udeps:
    cargo +{{nightly}} udeps --workspace

# ============================================================================
# Diagnostics
# ============================================================================

# Check development environment: toolchain pinning, disk space, port availability.
doctor:
    #!/usr/bin/env bash
    # Intentionally omit `-e`: keep running through all sections even if a
    # single check fails so the operator sees the full diagnostic.
    set -uo pipefail
    echo "=== toolchain ==="
    rustc +{{rust}} --version || { echo "  missing: rustc +{{rust}}"; exit 1; }
    rustc +{{nightly}} --version || { echo "  missing: rustc +{{nightly}}"; exit 1; }
    cargo +{{rust}} --version
    echo ""
    echo "=== just ==="
    just --version
    echo ""
    echo "=== mise ==="
    command -v mise >/dev/null && mise --version || echo "  mise not on PATH (optional but recommended)"
    echo ""
    echo "=== disk (target/) ==="
    du -sh target/ 2>/dev/null || echo "  no target/ yet"
    df -h . | tail -1
    echo ""
    echo "=== integration-test port range ==="
    # Server integration tests allocate ports dynamically via allocate_ports;
    # we spot-check a common band for conflicts.
    if command -v lsof >/dev/null; then
        lsof -iTCP -sTCP:LISTEN -P 2>/dev/null | awk '$9 ~ /:(9[0-9]{3}|1[0-9]{4})$/ {print "  busy: "$9}' | head -10
        echo "  (listed any listeners in 9000–19999 range)"
    else
        echo "  lsof unavailable — skipping port scan"
    fi
    echo ""
    echo "doctor complete."

# ============================================================================
# Convenience
# ============================================================================

# Re-run 'just check' whenever a Rust source file changes. Requires cargo-watch.
# Install: cargo install cargo-watch
watch:
    cargo +{{rust}} watch -s "just check"

# Produce a line-coverage report for the workspace. Requires cargo-llvm-cov.
# Install: cargo install cargo-llvm-cov
cover:
    cargo +{{rust}} llvm-cov --workspace --lib --html
    @echo ""
    @echo "coverage report: target/llvm-cov/html/index.html"

# Run workspace benchmarks. Store has the most mature suite (btree_bench).
bench:
    cargo +{{rust}} bench --workspace
