#!/usr/bin/env bash
# Run throughput-focused stress tests
#
# Tests designed to measure performance against advisory targets.
# Missing a target emits a warning; the test still passes.
# Uses release mode for accurate measurements.
# Tests run sequentially to avoid port conflicts.
#
# Tests:
#   - test_stress_batched               Single-region batched writes
#   - test_stress_read_throughput       Read-heavy workload
#   - test_stress_multi_region_batched  Multi-region batched writes
#   - test_stress_multi_region_target   Multi-region target throughput
#
# Usage:
#   ./scripts/stress-throughput.sh

set -euo pipefail
cd "$(dirname "$0")/.."

source "$(dirname "$0")/stress-targets.sh"

echo "Running throughput stress tests (release mode)..."
echo ""
echo "Targets (advisory — misses emit warnings, not failures):"
echo "  - Write p99 latency:          <${STRESS_WRITE_P99_TARGET}"
echo "  - Read p99 latency:           <${STRESS_READ_P99_TARGET}"
echo "  - Write throughput:           ${STRESS_WRITE_THROUGHPUT_TARGET}"
echo "  - Read throughput:            ${STRESS_READ_THROUGHPUT_TARGET}"
echo "  - Multi-region throughput:    ${STRESS_MULTI_REGION_THROUGHPUT_TARGET}"
echo ""

cargo +1.92 test --release --test stress -- \
    test_stress_batched \
    test_stress_read_throughput \
    test_stress_multi_region_batched \
    test_stress_multi_region_target \
    --test-threads=1 --nocapture
