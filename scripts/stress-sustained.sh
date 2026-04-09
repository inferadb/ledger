#!/usr/bin/env bash
# Run sustained reliability stress tests
#
# Longer-running stability tests to validate reliability under sustained load.
# Includes the standard and all multi-region tests.
# Uses release mode for accurate performance measurements.
#
# Tests:
#   - test_stress_standard                Standard load (~15s)
#   - test_stress_multi_region_quick      Multi-region quick validation
#   - test_stress_multi_region_batched    Multi-region batched writes
#   - test_stress_multi_region            4-region sustained throughput (~15s)
#   - test_stress_multi_region_target     8-region target throughput
#
# Usage:
#   ./scripts/stress-sustained.sh

set -euo pipefail
cd "$(dirname "$0")/.."

source "$(dirname "$0")/stress-targets.sh"

echo "Running sustained stress tests (release mode)..."
echo "This validates long-term stability under load."
echo ""
echo "Targets (advisory — misses emit warnings, not failures):"
echo "  - Write p99 latency:          <${STRESS_WRITE_P99_TARGET}"
echo "  - Read p99 latency:           <${STRESS_READ_P99_TARGET}"
echo "  - Write throughput:           ${STRESS_WRITE_THROUGHPUT_TARGET}"
echo "  - Multi-region throughput:    ${STRESS_MULTI_REGION_THROUGHPUT_TARGET}"
echo ""

cargo +1.92 test --release --test stress -- \
    test_stress_standard \
    test_stress_multi_region \
    --test-threads=1 --nocapture
