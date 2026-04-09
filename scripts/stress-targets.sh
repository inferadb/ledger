#!/usr/bin/env bash
# Shared performance targets for stress test scripts.
#
# Source this file to access target values:
#   source "$(dirname "$0")/stress-targets.sh"
#
# These values are display-only for the scripts. The actual threshold
# checks are compiled into the Rust stress test binary
# (crates/server/tests/stress_test.rs).

# Latency targets
export STRESS_WRITE_P99_TARGET="50ms"
export STRESS_READ_P99_TARGET="2ms"

# Throughput targets
export STRESS_WRITE_THROUGHPUT_TARGET="5,000 tx/sec"
export STRESS_READ_THROUGHPUT_TARGET="100,000 req/sec per node"
export STRESS_MULTI_REGION_THROUGHPUT_TARGET="5,000 ops/sec"
