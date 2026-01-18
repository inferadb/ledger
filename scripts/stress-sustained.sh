#!/usr/bin/env bash
# Run sustained reliability stress test (5 minutes)
#
# Long-running stability test to validate reliability under sustained load.
# Use this to catch issues that only appear over time.
#
# Usage:
#   ./scripts/stress-sustained.sh

set -euo pipefail
cd "$(dirname "$0")/.."

echo "Running sustained stress test (5 minutes, release mode)..."
echo "This test validates long-term stability under load."
echo ""

cargo test --release -p inferadb-ledger-server test_stress_sustained -- --ignored --nocapture
