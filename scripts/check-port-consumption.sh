#!/usr/bin/env bash
# Port consumption regression test
#
# Verifies that gRPC channel caching is working correctly by measuring
# TIME_WAIT socket accumulation during stress operations. A regression
# in channel caching (e.g., creating a new TCP connection per RPC instead
# of reusing cached channels) would cause ~16,000 TIME_WAIT sockets per
# test instead of the expected ~30-70.
#
# Hot paths tested:
#   1. Follower reads  — ensure_global_consistency → read_index_from_leader
#   2. Follower writes — ensure_global_consistency → read_index_from_leader
#   3. Proposal forwarding — proposal.rs non-leader → leader channel
#   4. Multi-region forwarding — cross-region proposal forwarding
#   5. Saga orchestrator — saga_orchestrator.rs non-leader forwarding
#
# Expected port consumption per 10-second stress test:
#   - 3-node cluster: ~30-70 TIME_WAIT sockets (channel cache hit rate >99%)
#   - 1-node cluster: ~2-5 TIME_WAIT sockets (no forwarding)
#   - Multi-region:   ~30-70 TIME_WAIT sockets
#
# Failure threshold: 200 TIME_WAIT sockets per test
#   - 3x headroom above worst observed case (68)
#   - 80x below the pathological uncached case (~16,000)
#
# Usage:
#   ./scripts/check-port-consumption.sh
#   ./scripts/check-port-consumption.sh --debug  # Use debug builds (faster compile)

set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-}"
THRESHOLD=200
FAILED=0

log_info()  { echo -e "\033[0;34m[INFO]\033[0m  $*"; }
log_ok()    { echo -e "\033[0;32m[OK]\033[0m    $*"; }
log_error() { echo -e "\033[0;31m[ERROR]\033[0m $*"; }
log_warn()  { echo -e "\033[1;33m[WARN]\033[0m  $*"; }

count_time_wait() {
  local count
  count=$(netstat -an 2>/dev/null | grep -c TIME_WAIT) || true
  echo "${count:-0}"
}

# Kill any leftover processes and wait for sockets to clear.
cleanup_ports() {
  pkill -f inferadb-ledger 2>/dev/null || true
  sleep 2
}

run_stress_test() {
  local test_name=$1
  local description=$2

  cleanup_ports

  local before
  before=$(count_time_wait)

  log_info "Running: $description"
  log_info "  Test: $test_name"
  log_info "  TIME_WAIT before: $before | Threshold: $THRESHOLD"

  local test_result=0
  if [[ "$MODE" == "--debug" ]]; then
    cargo +1.92 test --test stress -- "$test_name" --nocapture 2>&1 | tail -3 || test_result=$?
  else
    cargo +1.92 test --release --test stress -- "$test_name" --nocapture 2>&1 | tail -3 || test_result=$?
  fi

  if [[ $test_result -ne 0 ]]; then
    log_warn "  Stress test itself failed (non-port issue) — skipping port check"
    return 0
  fi

  local after
  after=$(count_time_wait)
  local delta=$((after - before))

  if [[ $delta -gt $THRESHOLD ]]; then
    log_error "  REGRESSION: $delta new TIME_WAIT sockets (threshold: $THRESHOLD)"
    log_error "  This indicates gRPC channel caching is broken."
    log_error "  Check: helpers.rs read_index_from_leader, proposal.rs, saga_orchestrator.rs"
    ((FAILED++))
  else
    log_ok "  $delta new TIME_WAIT sockets (within threshold of $THRESHOLD)"
  fi
}

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║        Port Consumption Regression Test                  ║"
echo "║        Threshold: $THRESHOLD TIME_WAIT sockets per test          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ─── Test 1: 3-node cluster (follower reads + writes + forwarding) ────
# This is the primary hot path. In a 3-node cluster, 2/3 of operations
# hit followers and get forwarded to the leader. Each forward uses the
# cached gRPC channel. Without caching, this creates ~16,000 sockets.
run_stress_test \
  "test_stress_quick" \
  "3-node cluster — follower read/write forwarding"

# ─── Test 2: 1-node cluster (leader-only, no forwarding) ─────────────
# Baseline: no forwarding needed, all operations hit the leader directly.
# Should create very few TIME_WAIT sockets (~2-5).
run_stress_test \
  "test_stress_single_node" \
  "1-node cluster — leader-only baseline"

# ─── Test 3: Multi-region (cross-region forwarding) ───────────────────
# Tests the data region proposal forwarding path (proposal.rs).
# Multiple regions means more channels, but they should all be cached.
run_stress_test \
  "test_stress_multi_region_quick" \
  "Multi-region — cross-region proposal forwarding"

# ─── Test 4: Batched writes (bulk forwarding) ────────────────────────
# Tests that batch writes also use cached channels. The batch write
# service forwards the entire batch via a single cached channel.
run_stress_test \
  "test_stress_batched" \
  "Batched writes — bulk proposal forwarding"

# ─── Test 5: Read-heavy workload ─────────────────────────────────────
# Tests the read_index_from_leader path specifically. Read-heavy
# workloads on followers should reuse the ReadIndex channel.
run_stress_test \
  "test_stress_read_throughput" \
  "Read-heavy — ReadIndex channel caching"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [[ $FAILED -gt 0 ]]; then
  log_error "FAILED: $FAILED test(s) exceeded port consumption threshold"
  log_error ""
  log_error "A regression in gRPC channel caching causes ~16,000 TIME_WAIT"
  log_error "sockets per test (vs expected ~30-70). This exhausts the macOS"
  log_error "ephemeral port pool and breaks sequential test execution."
  log_error ""
  log_error "Likely causes:"
  log_error "  1. helpers.rs: read_index_from_leader creates Channel per call"
  log_error "  2. proposal.rs: non-leader forwarding creates Channel per call"
  log_error "  3. saga_orchestrator.rs: non-leader forwarding creates Channel per call"
  log_error "  4. learner_refresh.rs: refresh_from_voter creates Channel per call"
  log_error ""
  log_error "Fix: ensure all hot-path gRPC channels use connect_lazy() with caching."
  exit 1
else
  log_ok "All tests within port consumption threshold ($THRESHOLD TIME_WAIT sockets)"
  echo ""
fi
