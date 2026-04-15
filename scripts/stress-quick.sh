#!/usr/bin/env bash
# Run the fast-path stress tests in a single cargo invocation.
#
# These are the "smoke-level" throughput tests — not a full sweep. Use
# stress-throughput.sh for target-measurement runs or stress-correctness.sh
# for scale-correctness tests.
#
# Performance targets live in crates/server/tests/stress_test.rs (single
# source of truth). The tests emit advisory warnings when targets are missed.

set -euo pipefail
cd "$(dirname "$0")/.."

cargo +1.92 test --release --test stress -- \
  test_stress_quick \
  test_stress_single_node \
  test_stress_batched \
  test_stress_read_throughput \
  test_stress_multi_region_quick \
  --test-threads=1 --nocapture
