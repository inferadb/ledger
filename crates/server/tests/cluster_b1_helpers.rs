//! Smoke test for the B.1 `TestCluster` helpers.
//!
//! Exercises the post-bootstrap on-demand region + organization creation
//! path added as part of B.1 Task 13. The legacy `with_data_regions`
//! bootstrap remains exercised by every other integration test; this file
//! exists purely to verify the new helper surface works end-to-end against
//! a multi-node cluster driven entirely through consensus.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_types::Region;

use crate::common::TestCluster;

#[tokio::test]
async fn b1_helpers_bootstrap_region_and_org_through_consensus() {
    // Lean bootstrap — only the system (GLOBAL) region comes up.
    let cluster = TestCluster::new(3).await;
    let _system_leader = cluster.wait_for_leader().await;

    // Create the region on every node via GLOBAL consensus.
    cluster.create_data_region(Region::US_EAST_VA).await.expect("create_data_region");

    // Every node's region group should agree on a leader.
    let region_leader = cluster.wait_for_region_leader(Region::US_EAST_VA).await;
    assert!(region_leader > 0, "region leader id non-zero");
    assert!(
        cluster.nodes().iter().any(|n| n.id == region_leader),
        "region leader id {region_leader} matches a cluster node",
    );

    // Create an organization and verify the per-org group fans out.
    let org_id = cluster
        .create_organization(Region::US_EAST_VA, "alpha")
        .await
        .expect("create_organization alpha");
    assert!(org_id.value() > 0, "allocated org id non-zero");

    // `organization_group` should return Some on every node.
    for (idx, _) in cluster.nodes().iter().enumerate() {
        let _group = cluster.organization_group(idx, Region::US_EAST_VA, org_id);
    }

    // `region_group_at` still works for the region control plane.
    let _region_group = cluster.region_group_at(0, Region::US_EAST_VA);

    // Exercise `organizations_synced` — idle groups report synced.
    cluster.wait_for_organizations_synced(Region::US_EAST_VA, Duration::from_secs(5)).await;
    assert!(
        cluster.organizations_synced(Region::US_EAST_VA),
        "idle org group should be reported as synced"
    );

    // Create a second organization to verify the helper supports multiple
    // orgs in a single region (the whole point of B.1).
    let org_beta = cluster
        .create_organization(Region::US_EAST_VA, "beta")
        .await
        .expect("create_organization beta");
    assert_ne!(org_beta, org_id, "second org has a distinct id");
    for (idx, _) in cluster.nodes().iter().enumerate() {
        let _group = cluster.organization_group(idx, Region::US_EAST_VA, org_beta);
    }
}
