#!/usr/bin/env bash
# Run throughput-focused stress tests
#
# Tests designed to measure and validate DESIGN.md performance targets:
#   - Write throughput: 5,000 tx/sec
#   - Read throughput: 100,000 req/sec per node
#   - Write p99 latency: <50ms
#   - Read p99 latency: <2ms
#
# Uses release mode for accurate measurements.
# Tests run sequentially to avoid port conflicts.
#
# Usage:
#   ./scripts/stress-throughput.sh

set -euo pipefail
cd "$(dirname "$0")/.."

echo "Running throughput stress tests (release mode)..."
echo ""
echo "Targets:"
echo "  - Write throughput: 5,000 tx/sec"
echo "  - Read throughput: 100,000 req/sec per node"
echo "  - Write p99 latency: <50ms"
echo "  - Read p99 latency: <2ms"
echo ""

# Run batched tests (single-shard and multi-shard)
cargo test --release -p inferadb-ledger-server \
    test_stress_batched \
    test_stress_read_throughput \
    test_stress_multi_shard_batched \
    test_stress_multi_shard_target \
    -- --test-threads=1 --nocapture
