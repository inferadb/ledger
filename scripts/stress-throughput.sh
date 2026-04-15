#!/usr/bin/env bash
# Run the throughput-focused stress tests against advisory targets.
#
# Single-region batched + multi-region batched + target throughput tests.
# Missing a target emits a warning; the test still passes.
#
# Performance targets live in crates/server/tests/stress_test.rs.

set -euo pipefail
cd "$(dirname "$0")/.."

cargo +1.92 test --release --test stress -- \
  test_stress_batched \
  test_stress_read_throughput \
  test_stress_multi_region_batched \
  test_stress_multi_region_target \
  --test-threads=1 --nocapture
