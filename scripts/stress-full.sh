#!/usr/bin/env bash
# Run full stress test suite including long-running tests
#
# Includes all stress tests, even those normally ignored for CI.
# Always uses release mode for accurate performance measurements.
# Tests run sequentially to avoid port conflicts.
#
# Tests included:
#   - Quick tests (single node, batched, multi-shard)
#   - Standard test (~15s)
#   - Heavy test (high load)
#   - Max throughput test (push limits)
#   - Sustained test (5 minutes stability)
#
# Usage:
#   ./scripts/stress-full.sh

set -euo pipefail
cd "$(dirname "$0")/.."

echo "Running FULL stress test suite (release mode)..."
echo "This includes long-running tests and may take several minutes."
echo ""

cargo test --release -p inferadb-ledger-server test_stress -- --test-threads=1 --ignored --nocapture
