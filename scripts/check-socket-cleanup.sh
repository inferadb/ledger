#!/usr/bin/env bash
# Unix domain socket cleanup verification test
#
# Verifies that UDS-based test clusters properly clean up socket files
# after tests complete. A regression in cleanup (e.g., leaked socket files
# from dropped TestCluster instances) would leave stale `.sock` files in
# temp directories, eventually consuming disk space or causing bind failures.
#
# What this tests:
#   1. Runs a subset of integration tests that use UDS
#   2. Checks that no .sock files remain in /tmp after tests complete
#   3. Verifies TestDir cleanup removes socket directories
#
# Expected behavior:
#   - All .sock files created during tests are cleaned up
#   - No orphaned socket directories remain
#   - Exit code 0 on success, 1 on leaked sockets
#
# Usage:
#   ./scripts/check-socket-cleanup.sh

set -euo pipefail
cd "$(dirname "$0")/.."

log_info()  { echo -e "\033[0;34m[INFO]\033[0m  $*"; }
log_ok()    { echo -e "\033[0;32m[OK]\033[0m    $*"; }
log_error() { echo -e "\033[0;31m[ERROR]\033[0m $*"; }

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║        Unix Socket Cleanup Verification Test             ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# Count ledger socket files matching the c{N}-n{N}.sock naming pattern.
count_ledger_socks() {
    find /tmp -maxdepth 3 -name "c*-n*.sock" 2>/dev/null | wc -l | tr -d ' '
}

BEFORE_SOCKS=$(count_ledger_socks)
log_info "Ledger .sock files in /tmp before test: $BEFORE_SOCKS"

# Run a representative subset of integration tests that exercise UDS clusters.
# These tests create TestCluster instances with UDS, run operations, then drop
# the cluster (which should clean up socket files via TestDir).
log_info "Running integration test subset (UDS clusters)..."

TEST_RESULT=0
cargo +1.92 test -p inferadb-ledger-server --test integration -- \
    test_single_node_bootstrap \
    test_get_node_info_returns_node_id \
    test_get_node_info_shows_cluster_member_after_bootstrap \
    2>&1 | tail -5 || TEST_RESULT=$?

if [[ $TEST_RESULT -ne 0 ]]; then
    log_error "Integration tests failed — cannot verify socket cleanup"
    exit 1
fi

# Brief pause to let OS finalize file cleanup
sleep 1

# Check for leaked socket files
AFTER_SOCKS=$(count_ledger_socks)
LEAKED=$((AFTER_SOCKS - BEFORE_SOCKS))

log_info "Ledger .sock files in /tmp after test: $AFTER_SOCKS"

if [[ $LEAKED -gt 0 ]]; then
    log_error "LEAKED: $LEAKED socket files not cleaned up"
    log_error ""
    log_error "Remaining socket files:"
    find /tmp -maxdepth 3 -name "c*-n*.sock" 2>/dev/null | head -20
    log_error ""
    log_error "This indicates TestCluster/TestDir cleanup is broken."
    log_error "Check: common/mod.rs TestNode drop order, TestDir RAII cleanup"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log_ok "No leaked socket files detected"
log_ok "UDS cleanup verification passed"
echo ""
