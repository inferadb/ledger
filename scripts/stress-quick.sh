#!/usr/bin/env bash
# Run quick stress tests
#
# These tests run quickly and are suitable for regular development.
# Uses release mode by default for accurate results.
# Tests run sequentially to avoid port conflicts.
#
# Usage:
#   ./scripts/stress-quick.sh         # Run all quick stress tests (release)
#   ./scripts/stress-quick.sh --debug # Run in debug mode (faster compile, slower tests)

set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-}"

if [[ "$MODE" == "--debug" ]]; then
    echo "Running quick stress tests (debug mode)..."
    echo "Warning: Debug mode may cause false failures due to timeouts"
    echo ""
    cargo test -p inferadb-ledger-server test_stress -- --test-threads=1 --nocapture \
        --skip test_stress_standard \
        --skip test_stress_heavy \
        --skip test_stress_max_throughput \
        --skip test_stress_sustained
else
    echo "Running quick stress tests (release mode)..."
    cargo test --release -p inferadb-ledger-server test_stress -- --test-threads=1 --nocapture \
        --skip test_stress_standard \
        --skip test_stress_heavy \
        --skip test_stress_max_throughput \
        --skip test_stress_sustained
fi
