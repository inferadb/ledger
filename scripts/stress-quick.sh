#!/usr/bin/env bash
# Run quick stress tests
#
# These tests run quickly and are suitable for regular development.
# Uses release mode for accurate results.
# Each test runs as a separate cargo invocation to avoid macOS ephemeral
# port exhaustion — a single stress test creates ~16K TCP connections
# whose TIME_WAIT sockets block subsequent tests within the same process.
#
# Quick tests:
#   - test_stress_quick            3-node cluster, quick smoke test
#   - test_stress_single_node      1-node cluster validation
#   - test_stress_batched          Batched writes throughput
#   - test_stress_read_throughput  Read-heavy workload
#   - test_stress_multi_region_quick  Multi-region quick validation
#
# Usage:
#   ./scripts/stress-quick.sh

set -euo pipefail
cd "$(dirname "$0")/.."

source "$(dirname "$0")/stress-targets.sh"

echo "Running quick stress tests (release mode)..."
echo ""
echo "Targets (advisory — misses emit warnings, not failures):"
echo "  - Write p99 latency: <${STRESS_WRITE_P99_TARGET}"
echo "  - Read p99 latency:  <${STRESS_READ_P99_TARGET}"
echo "  - Write throughput:  ${STRESS_WRITE_THROUGHPUT_TARGET}"
echo ""

FILTERS=(
    test_stress_quick
    test_stress_single_node
    test_stress_batched
    test_stress_read_throughput
    test_stress_multi_region_quick
)

PASSED=0
FAILED=0
TOTAL=${#FILTERS[@]}

for test_name in "${FILTERS[@]}"; do
    echo ""
    echo "━━━ Running $test_name ━━━"
    if cargo +1.92 test --release --test stress -- "$test_name" --nocapture; then
        ((PASSED++))
    else
        ((FAILED++))
        echo "FAILED: $test_name"
    fi
done

echo ""
echo "━━━ Results: $PASSED/$TOTAL passed, $FAILED failed ━━━"
if [[ $FAILED -gt 0 ]]; then
    exit 1
fi
