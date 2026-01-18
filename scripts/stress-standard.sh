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

echo "Running standard stress test (release mode)..."
cargo test --release -p inferadb-ledger-server test_stress_standard -- --ignored --nocapture
