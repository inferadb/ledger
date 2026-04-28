//! Integration tests for the GLOBAL region directory + `--regions` opt-in flow.
//!
//! Validates the auto-join / opt-in policy for protected regions:
//!
//! - **Unprotected regions** (`protected = false`): every cluster node automatically starts the
//!   region's data-region group when the `CreateDataRegion` entry applies on the GLOBAL log store.
//! - **Protected regions** (`protected = true`): nodes skip the region unless their `--regions
//!   <name>` opt-in includes the directory entry's name. Opt-in is restart-only.
//!
//! Pairs with the boot-time directory scan in `bootstrap.rs::start_directory_regions`,
//! which sweeps `_dir:region:*` keys after `discover_existing_regions()` (restart
//! path) and after `InitCluster` completes (fresh path). The signal-driven
//! `process_region_event` path covers live propagation when a new region is
//! provisioned on an already-running cluster.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use crate::common::TestCluster;

/// Unprotected regions auto-join on every node — the existing "happy path"
/// for production multi-region deployments.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unprotected_region_auto_joins_every_node() {
    let cluster = TestCluster::new(3).await;

    cluster
        .create_data_region_with_protection(inferadb_ledger_types::Region::US_EAST_VA, false)
        .await
        .expect("create unprotected region");

    // Every node must host the region — the unprotected directory entry
    // signals auto-join, and the region-creation handler runs the same
    // `start_data_region` dance the test cluster relied on previously.
    for node in cluster.nodes() {
        assert!(
            node.manager.has_region(inferadb_ledger_types::Region::US_EAST_VA),
            "node {} missed unprotected region auto-join",
            node.id,
        );
    }
}

/// Protected regions are NOT joined when no node opts in. The directory
/// entry persists in GLOBAL state but no per-node Raft group materialises.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protected_region_skipped_without_opt_in() {
    let cluster = TestCluster::new(3).await;

    cluster
        .create_data_region_with_protection(inferadb_ledger_types::Region::IE_EAST_DUBLIN, true)
        .await
        .expect("create protected region");

    // No node should have started the region — none of them carry an
    // opt-in for it.
    for node in cluster.nodes() {
        assert!(
            !node.manager.has_region(inferadb_ledger_types::Region::IE_EAST_DUBLIN),
            "node {} unexpectedly auto-joined protected region",
            node.id,
        );
    }
}

/// Restart a single node with `--regions <name>` opt-in. The boot-time
/// directory scan in `bootstrap.rs::start_directory_regions` picks up the
/// previously-skipped protected region and starts it on that node only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protected_region_started_after_node_opts_in_on_restart() {
    let cluster = TestCluster::new(3).await;
    let region = inferadb_ledger_types::Region::IE_EAST_DUBLIN;

    cluster
        .create_data_region_with_protection(region, true)
        .await
        .expect("create protected region");

    // Pre-restart: no node hosts the region.
    for node in cluster.nodes() {
        assert!(!node.manager.has_region(region), "node {} unexpectedly hosted region", node.id);
    }

    // Graceful-restart with node 0 opted into `ie-east-dublin`.
    let cluster = cluster.graceful_restart_with_regions(0, vec![region.as_str().to_string()]).await;

    // Wait up to 30s for node 0 to start the region on its boot-time
    // directory scan. The scan runs after `discover_existing_regions()`
    // (restart path) — for a previously-skipped region, the entry is in
    // the directory but no on-disk state exists, so `start_data_region`
    // must run.
    let opt_in_node_id = cluster.nodes()[0].id;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let opt_in_started = cluster
            .nodes()
            .iter()
            .find(|n| n.id == opt_in_node_id)
            .map(|n| n.manager.has_region(region))
            .unwrap_or(false);
        if opt_in_started {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("node {opt_in_node_id} did not start protected region after opt-in restart");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Other (non-opted-in) nodes still skip the region — opt-in is
    // strictly per-node.
    for node in cluster.nodes() {
        if node.id == opt_in_node_id {
            continue;
        }
        assert!(
            !node.manager.has_region(region),
            "node {} (no opt-in) unexpectedly started protected region",
            node.id,
        );
    }
}
