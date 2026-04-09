#!/usr/bin/env bash
# Run full stress test suite
#
# Includes all stress tests across all modules:
#   - stress_test: quick, single-node, batched, read, standard, multi-region
#   - stress_integration: client IDs, snapshots, bulk writes, batch replication
#   - stress_scale: 10k entities, 10k writes, multi-org distribution
#   - stress_watch: high-volume watch reconnection
#
# Always uses release mode for accurate performance measurements.
# Tests run sequentially to avoid port conflicts.
#
# Usage:
#   ./scripts/stress-full.sh

set -euo pipefail
cd "$(dirname "$0")/.."

source "$(dirname "$0")/stress-targets.sh"

echo "Running FULL stress test suite (release mode)..."
echo "This includes all stress tests and may take several minutes."
echo ""
echo "Targets (advisory — misses emit warnings, not failures):"
echo "  - Write p99 latency:          <${STRESS_WRITE_P99_TARGET}"
echo "  - Read p99 latency:           <${STRESS_READ_P99_TARGET}"
echo "  - Write throughput:           ${STRESS_WRITE_THROUGHPUT_TARGET}"
echo "  - Multi-region throughput:    ${STRESS_MULTI_REGION_THROUGHPUT_TARGET}"
echo ""

cargo +1.92 test --release --test stress -- --test-threads=1 --nocapture
