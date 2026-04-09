#!/usr/bin/env bash
# Run the standard stress test (~15 seconds)
#
# Moderate load test suitable for regular validation.
# Uses release mode for accurate performance measurements.
#
# Usage:
#   ./scripts/stress-standard.sh

set -euo pipefail
cd "$(dirname "$0")/.."

source "$(dirname "$0")/stress-targets.sh"

echo "Running standard stress test (release mode)..."
echo ""
echo "Targets (advisory — misses emit warnings, not failures):"
echo "  - Write p99 latency: <${STRESS_WRITE_P99_TARGET}"
echo "  - Read p99 latency:  <${STRESS_READ_P99_TARGET}"
echo "  - Write throughput:  ${STRESS_WRITE_THROUGHPUT_TARGET}"
echo ""

cargo +1.92 test --release --test stress test_stress_standard -- --nocapture
