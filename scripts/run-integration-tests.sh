#!/usr/bin/env bash
# Run server or SDK integration tests against a freshly spawned local cluster.
#
# Unifies the previous run-sdk-integration-tests.sh and run-server-integration-tests.sh.
# The target (sdk|server) selects port range, data directory, settle time, and
# which cargo test binary to invoke.
#
# Usage:
#   ./scripts/run-integration-tests.sh <sdk|server> [OPTIONS] [TEST_PATTERN]
#
# Options:
#   --release     Build in release mode (slower compile, faster tests)
#   --nodes N     Number of cluster nodes (default: 3)
#   --help        Show this help
#
# Examples:
#   ./scripts/run-integration-tests.sh sdk
#   ./scripts/run-integration-tests.sh sdk test_write_read
#   ./scripts/run-integration-tests.sh server --release --nodes 1
#   ./scripts/run-integration-tests.sh server test_voter_detection

set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib/cluster-bootstrap.sh
source "$(dirname "$0")/lib/cluster-bootstrap.sh"

# ---------------------------------------------------------------------------
# Target selection
# ---------------------------------------------------------------------------

if [[ $# -eq 0 || "$1" == "--help" || "$1" == "-h" ]]; then
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

TARGET=$1
shift

case "$TARGET" in
  sdk)
    BASE_PORT=50051
    SETTLE_TIME=3
    DATA_ROOT="/tmp/ledger-sdk-test-$$"
    TEST_CRATE="inferadb-ledger-sdk"
    TEST_BINARY="e2e"
    ;;
  server)
    BASE_PORT=50061
    SETTLE_TIME=10
    DATA_ROOT="/tmp/ledger-server-test-$$"
    TEST_CRATE="inferadb-ledger-server"
    TEST_BINARY="external"
    ;;
  *)
    log_error "Unknown target: $TARGET (expected: sdk|server)"
    exit 2
    ;;
esac

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

NODE_COUNT=3
PROFILE="debug"
TEST_PATTERN=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --release) PROFILE="release"; shift ;;
    --nodes)   NODE_COUNT="$2"; shift 2 ;;
    --help|-h) sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) log_error "Unknown option: $1"; exit 2 ;;
    *)  TEST_PATTERN="$1"; shift ;;
  esac
done

# ---------------------------------------------------------------------------
# Build + bootstrap
# ---------------------------------------------------------------------------

trap cleanup_cluster EXIT
build_ledger_binary "$PROFILE"
bootstrap_cluster "$BASE_PORT" "$NODE_COUNT" "$DATA_ROOT" "$SETTLE_TIME"

# Data regions are no longer auto-created at boot — `init` only brings up the
# GLOBAL region. Provision us-east-va so user-facing RPCs (onboarding, write,
# read) apply through the regional Raft group instead of returning
# `ProposalError: Region us-east-va is not active on this node`.
provision_region us-east-va false
sleep 3

# ---------------------------------------------------------------------------
# Run tests
# ---------------------------------------------------------------------------

log_info "Running $TARGET integration tests..."

TEST_ARGS=(cargo +1.92 test -p "$TEST_CRATE" --test "$TEST_BINARY")
if [[ -n "$TEST_PATTERN" ]]; then
  TEST_ARGS+=("$TEST_PATTERN")
fi
TEST_ARGS+=(-- --test-threads=1 --nocapture)

log_info "Executing: ${TEST_ARGS[*]}"
echo ""

set +e
"${TEST_ARGS[@]}"
TEST_EXIT_CODE=$?
set -e

echo ""
if [[ $TEST_EXIT_CODE -eq 0 ]]; then
  log_success "All $TARGET integration tests passed"
else
  log_error "$TARGET integration tests failed (exit $TEST_EXIT_CODE)"
  log_info "Node logs preserved in $DATA_ROOT (removed on clean exit)"
  # Preserve logs on failure by clearing the data root pointer.
  _CLUSTER_DATA_ROOT=""
fi

exit "$TEST_EXIT_CODE"
