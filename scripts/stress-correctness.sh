#!/usr/bin/env bash
# Run the scale-correctness stress tests.
#
# These tests exercise state root determinism, snapshot round-trips, and
# streaming correctness under load. They are NOT throughput benchmarks —
# the goal is to catch replication / snapshot / broadcast channel bugs
# that only surface at scale.
#
# Coverage:
#   - stress_integration.rs: 2000 unique client IDs, 20×5 snapshot round-trip,
#     bulk write state root parity, snapshot-during-apply, snapshot determinism,
#     batch replication.
#   - stress_scale.rs: 10k entities per vault snapshot, 10k writes replicated
#     state root parity, 2k entity multi-org distribution.
#   - stress_watch.rs: high-volume watch reconnection under broadcast channel
#     saturation.

set -euo pipefail
cd "$(dirname "$0")/.."

cargo +1.92 test --release --test stress -- \
  test_2000_unique_client_ids_no_page_full \
  test_snapshot_20_orgs_5_vaults_round_trip \
  test_bulk_writes_replicated_state_roots_match \
  test_snapshot_during_active_apply_loop \
  test_snapshot_determinism_all_nodes_identical_state \
  test_batch_writes_replicated_to_all_nodes \
  test_snapshot_over_10k_entities_per_vault_no_data_loss \
  test_10k_writes_replicated_state_roots_match \
  test_multi_org_vault_write_distribution_2k_entities \
  test_watch_blocks_high_volume_reconnect \
  --test-threads=1 --nocapture
