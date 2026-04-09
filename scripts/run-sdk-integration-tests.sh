#!/usr/bin/env bash
# Run SDK end-to-end integration tests against a local Ledger cluster.
#
# This script:
#   1. Builds the inferadb-ledger binary
#   2. Starts a 3-node cluster on localhost (ports 50051-50053)
#   3. Runs SDK integration tests against the cluster
#   4. Cleans up all processes and data directories on exit
#
# Usage:
#   ./scripts/run_sdk_integration_tests.sh                    # Run all SDK e2e tests
#   ./scripts/run_sdk_integration_tests.sh <test_pattern>     # Run matching tests
#   ./scripts/run_sdk_integration_tests.sh --release          # Use release build (slower compile, faster tests)
#   ./scripts/run_sdk_integration_tests.sh --nodes 1          # Single-node cluster

set -euo pipefail
cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NODE_COUNT=3
BASE_PORT=50051
DATA_ROOT="/tmp/ledger-sdk-test-$$"
PROFILE="debug"
BINARY="target/debug/inferadb-ledger"
PIDS=()
TEST_PATTERN=""
HEALTH_TIMEOUT=60
SETTLE_TIME=3

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
  case $1 in
    --release)
      PROFILE="release"
      BINARY="target/release/inferadb-ledger"
      shift
      ;;
    --nodes)
      NODE_COUNT="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage: $0 [OPTIONS] [TEST_PATTERN]"
      echo ""
      echo "Options:"
      echo "  --release      Build in release mode (slower compile, faster tests)"
      echo "  --nodes N      Number of cluster nodes (default: 3)"
      echo "  --help         Show this help"
      echo ""
      echo "Examples:"
      echo "  $0                                     Run all SDK e2e tests"
      echo "  $0 test_write_read                     Run matching tests"
      echo "  $0 --release --nodes 1                 Single-node, release build"
      exit 0
      ;;
    -*)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
    *)
      TEST_PATTERN="$1"
      shift
      ;;
  esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

# ---------------------------------------------------------------------------
# Cleanup — runs on ANY exit (success, failure, signal)
# ---------------------------------------------------------------------------

cleanup() {
  local exit_code=$?
  log_info "Cleaning up..."

  # Kill all node processes
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done

  # Wait for processes to exit (give them a moment for graceful shutdown)
  for pid in "${PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done

  # Remove all data directories
  if [[ -d "$DATA_ROOT" ]]; then
    rm -rf "$DATA_ROOT"
    log_info "Removed data directory: $DATA_ROOT"
  fi

  if [[ $exit_code -eq 0 ]]; then
    log_success "Cleanup complete"
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Step 1: Build the binary
# ---------------------------------------------------------------------------

if [[ "$PROFILE" == "release" ]]; then
  log_info "Building inferadb-ledger (release)..."
  cargo +1.92 build --release -p inferadb-ledger-server
else
  log_info "Building inferadb-ledger (debug)..."
  cargo +1.92 build -p inferadb-ledger-server
fi

if [[ ! -x "$BINARY" ]]; then
  log_error "Binary not found: $BINARY"
  exit 1
fi
log_success "Built: $BINARY"

# ---------------------------------------------------------------------------
# Step 2: Kill any leftover processes on test ports
# ---------------------------------------------------------------------------

for i in $(seq 1 "$NODE_COUNT"); do
  PORT=$((BASE_PORT + i - 1))
  STALE_PID=$(lsof -ti :"$PORT" 2>/dev/null || true)
  if [[ -n "$STALE_PID" ]]; then
    log_warn "Killing stale process on port $PORT (PID $STALE_PID)"
    kill -9 $STALE_PID 2>/dev/null || true
    sleep 0.5
  fi
done

# ---------------------------------------------------------------------------
# Step 3: Create data directories
# ---------------------------------------------------------------------------

mkdir -p "$DATA_ROOT"

# ---------------------------------------------------------------------------
# Step 4: Start cluster nodes
# ---------------------------------------------------------------------------

log_info "Starting $NODE_COUNT-node cluster (ports $BASE_PORT-$((BASE_PORT + NODE_COUNT - 1)))..."

FIRST_PORT=$BASE_PORT
for i in $(seq 1 "$NODE_COUNT"); do
  PORT=$((BASE_PORT + i - 1))
  NODE_DATA="$DATA_ROOT/node$i"
  mkdir -p "$NODE_DATA"

  # Fixed test blinding key (matches TestCluster configuration)
  BLINDING_KEY="deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

  if [[ $i -eq 1 ]]; then
    # First node: start without --join (accepts incoming connections)
    RUST_LOG=info "$BINARY" \
      --listen "127.0.0.1:$PORT" \
      --data "$NODE_DATA" \
      --email-blinding-key "$BLINDING_KEY" \
      --log-format text \
      > "$DATA_ROOT/node$i.log" 2>&1 &
  else
    # Other nodes: --join points to first node as seed
    RUST_LOG=info "$BINARY" \
      --listen "127.0.0.1:$PORT" \
      --data "$NODE_DATA" \
      --join "127.0.0.1:$FIRST_PORT" \
      --email-blinding-key "$BLINDING_KEY" \
      --log-format text \
      > "$DATA_ROOT/node$i.log" 2>&1 &
  fi

  PIDS+=($!)
  log_info "  Node $i: PID $!, port $PORT"
done

# ---------------------------------------------------------------------------
# Step 4: Wait for cluster readiness
# ---------------------------------------------------------------------------

log_info "Waiting for cluster to become ready (timeout: ${HEALTH_TIMEOUT}s)..."
ELAPSED=0

while [[ $ELAPSED -lt $HEALTH_TIMEOUT ]]; do
  ALL_LISTENING=true
  for i in $(seq 1 "$NODE_COUNT"); do
    PORT=$((BASE_PORT + i - 1))
    if ! nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
      ALL_LISTENING=false
      break
    fi
  done

  if [[ "$ALL_LISTENING" == "true" ]]; then
    log_success "All $NODE_COUNT nodes are listening"
    # Initialize the cluster (one-time operation)
    log_info "Initializing cluster via first node (127.0.0.1:$FIRST_PORT)..."
    "$BINARY" init --host="127.0.0.1:$FIRST_PORT" || {
      log_error "Cluster initialization failed"
      for i in $(seq 1 "$NODE_COUNT"); do
        log_error "--- Node $i log (last 10 lines) ---"
        tail -10 "$DATA_ROOT/node$i.log" 2>/dev/null || true
      done
      exit 1
    }
    log_success "Cluster initialized"
    # Allow time for leader election and cluster stabilization
    log_info "Waiting ${SETTLE_TIME}s for leader election..."
    sleep "$SETTLE_TIME"
    break
  fi

  # Check for early crashes
  for idx in "${!PIDS[@]}"; do
    if ! kill -0 "${PIDS[$idx]}" 2>/dev/null; then
      NODE_NUM=$((idx + 1))
      log_error "Node $NODE_NUM (PID ${PIDS[$idx]}) exited prematurely"
      log_error "Log: $DATA_ROOT/node$NODE_NUM.log"
      tail -20 "$DATA_ROOT/node$NODE_NUM.log" 2>/dev/null || true
      exit 1
    fi
  done

  sleep 1
  ELAPSED=$((ELAPSED + 1))
done

if [[ $ELAPSED -ge $HEALTH_TIMEOUT ]]; then
  log_error "Cluster did not become ready within ${HEALTH_TIMEOUT}s"
  for i in $(seq 1 "$NODE_COUNT"); do
    log_error "--- Node $i log (last 10 lines) ---"
    tail -10 "$DATA_ROOT/node$i.log" 2>/dev/null || true
  done
  exit 1
fi

# Export cluster endpoints for tests
ENDPOINTS=""
for i in $(seq 1 "$NODE_COUNT"); do
  PORT=$((BASE_PORT + i - 1))
  [[ $i -gt 1 ]] && ENDPOINTS+=","
  ENDPOINTS+="http://127.0.0.1:$PORT"
  export "LEDGER_NODE$i=http://127.0.0.1:$PORT"
done
export LEDGER_ENDPOINTS="$ENDPOINTS"

echo ""
log_info "Cluster endpoints:"
for i in $(seq 1 "$NODE_COUNT"); do
  PORT=$((BASE_PORT + i - 1))
  echo "  - Node $i: http://127.0.0.1:$PORT"
done
echo ""

# ---------------------------------------------------------------------------
# Step 5: Run SDK integration tests
# ---------------------------------------------------------------------------

log_info "Running SDK integration tests..."

TEST_CMD="cargo +1.92 test -p inferadb-ledger-sdk --test e2e"
if [[ -n "$TEST_PATTERN" ]]; then
  TEST_CMD="$TEST_CMD $TEST_PATTERN"
fi
TEST_CMD="$TEST_CMD -- --test-threads=1 --nocapture"

log_info "Executing: $TEST_CMD"
echo ""

set +e
eval "$TEST_CMD"
TEST_EXIT_CODE=$?
set -e

echo ""
if [[ $TEST_EXIT_CODE -eq 0 ]]; then
  log_success "All SDK integration tests passed!"
else
  log_error "Some SDK integration tests failed (exit code: $TEST_EXIT_CODE)"
  log_info "Node logs are in $DATA_ROOT/node*.log"
fi

exit $TEST_EXIT_CODE
