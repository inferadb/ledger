//! Three-tier consensus integration tests.
//!
//! Validates the observable behaviors of the three-tier Raft topology:
//! `SystemGroup` (cluster control plane) → `RegionGroup` (regional control
//! plane and unified leader) → `OrganizationGroup` (per-organization data
//! plane).
//!
//! These tests exercise invariants that rely on per-organization Raft groups
//! existing as distinct types from the regional control-plane group. Assertions
//! use `route_organization` and `lookup_by_consensus_shard` to verify routing
//! resolves to per-org groups rather than to `(region, OrganizationId::new(0))`.
//!
//! F.1.f.2.Stage1e Wave 4: migrated from the legacy tonic helpers
//! (`create_read_client` / `create_write_client` /
//! `create_test_organization` / `create_test_vault`) to their
//! wire-protocol siblings (`wire_read_client` / `wire_write_client` /
//! `wire_create_test_organization` / `wire_create_test_vault`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use bytes::Bytes;
use inferadb_ledger_types::{OrganizationId, OrganizationSlug, Region, VaultSlug};
use inferadb_ledger_wire::services::{read as wr, shared as ws, write as ww};

use crate::common::{
    TestCluster, TestNode, wire_create_test_organization, wire_create_test_vault, wire_read_client,
    wire_write_client,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Creates an organization via the production saga pipeline and returns its
/// external slug.
async fn create_organization(
    cluster: &TestCluster,
    node_id: u64,
    name: &str,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let (slug, _admin) = wire_create_test_organization(cluster, node_id, name).await?;
    Ok(slug)
}

/// Creates a vault inside an organization.
async fn create_vault(
    cluster: &TestCluster,
    node_id: u64,
    org: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    wire_create_test_vault(cluster, node_id, org).await
}

/// Resolves an external `OrganizationSlug` to its internal
/// `OrganizationId` via the GLOBAL applied-state accessor. Panics if the
/// slug is not yet indexed — callers must have awaited organization
/// activation before calling.
fn resolve_org_id(node: &TestNode, slug: OrganizationSlug) -> OrganizationId {
    node.manager
        .system_region()
        .expect("system region running")
        .applied_state()
        .resolve_slug_to_id(slug)
        .expect("organization slug resolves after CreateOrganization saga commits")
}

/// Writes `(key, value)` into `(org, vault)` and returns the committed
/// block height.
async fn write_entity(
    cluster: &TestCluster,
    node_id: u64,
    org: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
    value: &[u8],
) -> Result<u64, Box<dyn std::error::Error>> {
    let client = wire_write_client(cluster, node_id);
    let request = ww::WriteRequest {
        organization: Some(org),
        vault: Some(vault),
        client_id: Some(ws::ClientIdMessage { id: "three-tier-test".to_string() }),
        idempotency_key: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
        operations: vec![ws::Operation {
            op: Some(ws::OperationKind::SetEntity(ws::SetEntity {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };
    let response = client.write(request, rand::random::<u128>()).await?;
    match response.result {
        Some(ww::WriteResponseResult::Success(s)) => Ok(s.block_height),
        Some(ww::WriteResponseResult::Error(e)) => Err(format!("write error: {:?}", e).into()),
        None => Err("no result in write response".into()),
    }
}

/// Reads a key from `(org, vault)`.
async fn read_entity(
    cluster: &TestCluster,
    node_id: u64,
    org: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let client = wire_read_client(cluster, node_id);
    let request = wr::ReadRequest {
        organization: Some(org),
        vault: Some(vault),
        key: key.to_string(),
        consistency: ws::ReadConsistency::Eventual,
        caller: None,
    };
    let response = client.read(request, rand::random::<u128>()).await?;
    Ok(response.value.map(|b| b.to_vec()))
}

// ============================================================================
// Per-organization Raft groups materialize (B.1.1 + B.1.6 + B.1.8)
// ============================================================================

/// After `CreateOrganization` commits, the node hosts a dedicated
/// `OrganizationGroup` keyed on the new organization's `OrganizationId` —
/// distinct from the data-region group at `OrganizationId::new(0)` and with
/// a distinct consensus shard.
///
/// Regression: without per-organization routing, `route_organization` resolved
/// every organization to the region's shard-0 group, so the "new org group"
/// did not exist as a separate object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_per_organization_group_materializes() {
    let cluster = TestCluster::with_wire_transport(1).await;
    assert!(cluster.wait_for_leaders(Duration::from_secs(10)).await, "leaders elected");
    let node = cluster.any_node();

    let org_slug = create_organization(&cluster, node.id, "materialize-org")
        .await
        .expect("create organization");
    let org_id = resolve_org_id(node, org_slug);
    assert_ne!(
        org_id,
        OrganizationId::new(0),
        "new organization must not collide with the system organization"
    );

    let region_group =
        node.manager.get_region_group(Region::US_EAST_VA).expect("data-region group");
    let org_group = node.manager.route_organization(org_id).expect("per-organization group");

    assert_eq!(org_group.region(), Region::US_EAST_VA);
    assert_eq!(
        org_group.organization_id(),
        org_id,
        "per-org group is keyed on the new organization's id"
    );
    assert_ne!(
        org_group.handle().shard_id(),
        region_group.handle().shard_id(),
        "per-org group owns a distinct consensus shard from the data-region group"
    );
}

// ============================================================================
// Per-organization isolation (storage + routing)
// ============================================================================

/// Writes to organization A with the same `(vault, key)` as organization B
/// remain invisible across the boundary — each organization has its own Raft
/// group and storage path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_per_organization_isolation() {
    let cluster = TestCluster::with_wire_transport(1).await;
    assert!(cluster.wait_for_leaders(Duration::from_secs(10)).await, "leaders elected");
    let node = cluster.any_node();

    let org_a = create_organization(&cluster, node.id, "iso-a").await.expect("create A");
    let org_b = create_organization(&cluster, node.id, "iso-b").await.expect("create B");
    let vault_a = create_vault(&cluster, node.id, org_a).await.expect("vault A");
    let vault_b = create_vault(&cluster, node.id, org_b).await.expect("vault B");

    write_entity(&cluster, node.id, org_a, vault_a, "k", b"value-a").await.expect("write A");
    write_entity(&cluster, node.id, org_b, vault_b, "k", b"value-b").await.expect("write B");

    let from_a = read_entity(&cluster, node.id, org_a, vault_a, "k").await.expect("read A");
    let from_b = read_entity(&cluster, node.id, org_b, vault_b, "k").await.expect("read B");
    assert_eq!(from_a, Some(b"value-a".to_vec()), "A sees its own write");
    assert_eq!(from_b, Some(b"value-b".to_vec()), "B sees its own write");

    // Identity: per-org groups are distinct objects with distinct consensus
    // shards. This catches routing regressions where both orgs silently
    // land on the same underlying group.
    let id_a = resolve_org_id(node, org_a);
    let id_b = resolve_org_id(node, org_b);
    let group_a = node.manager.route_organization(id_a).expect("A group");
    let group_b = node.manager.route_organization(id_b).expect("B group");
    assert_ne!(group_a.organization_id(), group_b.organization_id());
    assert_ne!(
        group_a.handle().shard_id(),
        group_b.handle().shard_id(),
        "per-org routing must return distinct consensus shards"
    );
}

// ============================================================================
// Delegated leadership (B.1.7 LeadershipMode::Delegated)
// ============================================================================

/// Every per-organization group in a region reports the same leader as the
/// region's data-region group — the unified-leadership contract. Under
/// `LeadershipMode::Delegated`, per-org groups do not elect independently;
/// a watcher propagates the region's leader into each per-org shard via
/// `ConsensusState::adopt_leader`.
///
/// Regression: without delegated leadership, per-org groups ran their own
/// election timers, so different orgs could momentarily disagree on the leader
/// even within the same region.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_delegated_leadership_follows_region() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    assert!(cluster.wait_for_leaders(Duration::from_secs(10)).await, "leaders elected");
    let leader = cluster.leader().expect("has leader");

    let org_a = create_organization(&cluster, leader.id, "delegated-a").await.expect("create A");
    let org_b = create_organization(&cluster, leader.id, "delegated-b").await.expect("create B");

    // The `Delegated` watcher runs asynchronously after bootstrap, so poll
    // every node until all per-org groups agree with the region's leader.
    let mut converged = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let mut ok = true;
        'outer: for node in cluster.nodes() {
            let region_group =
                node.manager.get_region_group(Region::US_EAST_VA).expect("region group");
            let region_leader = region_group.handle().current_leader();
            if region_leader.is_none() {
                ok = false;
                break;
            }
            for slug in [org_a, org_b] {
                let org_id = resolve_org_id(node, slug);
                let org_group = node.manager.route_organization(org_id).expect("per-org group");
                if org_group.handle().current_leader() != region_leader {
                    ok = false;
                    break 'outer;
                }
            }
        }
        if ok {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        converged,
        "per-organization groups must adopt the region's leader under Delegated mode"
    );
}

// ============================================================================
// Consensus-shard routing (B.1.7)
// ============================================================================

/// `RaftManager::lookup_by_consensus_shard(shard_id)` resolves a consensus
/// shard back to its owning `OrganizationGroup`. This is how the
/// `RaftService` gRPC server dispatches peer AppendEntries to the correct
/// engine on a node that hosts multiple per-organization groups.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_consensus_shard_lookup_roundtrip() {
    let cluster = TestCluster::with_wire_transport(1).await;
    assert!(cluster.wait_for_leaders(Duration::from_secs(10)).await, "leaders elected");
    let node = cluster.any_node();

    let org = create_organization(&cluster, node.id, "shard-lookup").await.expect("create org");
    let org_id = resolve_org_id(node, org);

    let direct = node.manager.route_organization(org_id).expect("per-org group");
    let shard_id = direct.handle().shard_id();
    let looked_up = node
        .manager
        .lookup_by_consensus_shard(shard_id)
        .expect("consensus shard resolves to a group");

    assert_eq!(looked_up.region(), direct.region());
    assert_eq!(looked_up.organization_id(), direct.organization_id());
    assert_eq!(
        looked_up.handle().shard_id(),
        shard_id,
        "lookup and direct routing must return the same consensus shard"
    );
}

// ============================================================================
// Concurrent organization creation (bootstrap robustness)
// ============================================================================

/// Creating N organizations concurrently from the same leader all succeed,
/// each receives a distinct `OrganizationId`, and each ends up with its own
/// per-organization Raft group. Validates that the multi-tier orchestration
/// (system apply → region placement → per-voter organization bootstrap)
/// does not serialize incorrectly when multiple creations race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_organization_creation() {
    let cluster = TestCluster::with_wire_transport(1).await;
    assert!(cluster.wait_for_leaders(Duration::from_secs(10)).await, "leaders elected");
    let leader = cluster.leader().expect("has leader");

    let cluster_ref = &cluster;
    let leader_id = leader.id;
    let futures = (0..5).map(|i| {
        let name = format!("concurrent-{i}");
        async move { create_organization(cluster_ref, leader_id, &name).await }
    });
    let results: Vec<_> = futures::future::join_all(futures).await;

    let mut ids = Vec::new();
    for result in results {
        let slug = result.expect("each concurrent create succeeds");
        ids.push(resolve_org_id(leader, slug));
    }

    let unique: std::collections::HashSet<_> = ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        ids.len(),
        "concurrent creations must allocate distinct OrganizationIds"
    );

    for id in &ids {
        let group =
            leader.manager.route_organization(*id).expect("per-org group for concurrent id");
        assert_eq!(group.organization_id(), *id);
    }
}

// ============================================================================
// Cross-region independence (B.1.3 three peer group types per region)
// ============================================================================

/// Data-region groups in different regions are independent Raft groups with
/// distinct consensus shards. A new organization in region R1 does not
/// create or alter any group in R2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cross_region_independence() {
    let cluster = TestCluster::with_wire_transport(2).await;
    assert!(cluster.wait_for_leaders(Duration::from_secs(10)).await, "leaders elected");
    let node = cluster.any_node();

    let region_a = Region::US_EAST_VA;
    let region_b = Region::US_WEST_OR;
    let group_a = node.manager.get_region_group(region_a).expect("region A group");
    let group_b = node.manager.get_region_group(region_b).expect("region B group");

    assert_ne!(group_a.region(), group_b.region());
    assert_ne!(
        group_a.handle().shard_id(),
        group_b.handle().shard_id(),
        "distinct regions run distinct consensus shards"
    );

    // A per-organization group created via the production saga today lands
    // in `US_EAST_VA` (per `create_test_organization`). The creation must
    // not add any group in region B.
    let before_b: usize =
        node.manager.list_organization_groups().iter().filter(|(r, _)| *r == region_b).count();
    let _ = create_organization(&cluster, node.id, "cross-region").await.expect("create org");
    let after_b: usize =
        node.manager.list_organization_groups().iter().filter(|(r, _)| *r == region_b).count();
    assert_eq!(
        before_b, after_b,
        "CreateOrganization in region A must not touch region B's group set"
    );
}

// ============================================================================
// Region-group leader transfer cascades to all per-org groups (B.1.7 delegated)
// ============================================================================

/// Transferring leadership of the region's data-region group atomically
/// transfers leadership for every per-organization group in that region.
/// Under `LeadershipMode::Delegated`, per-org groups follow the region's
/// elected leader via the `adopt_leader` watcher — a single region-group
/// leader transfer cascades to every organization without per-org
/// elections.
///
/// Regression: without the `adopt_leader` watcher, each per-org group would
/// have run its own election timer, so leadership transfer would only move the
/// region group, leaving per-org groups stuck on the old leader until their
/// timers fired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_region_leader_transfer_cascades_to_per_org_groups() {
    let cluster = TestCluster::with_wire_transport_and_size(1, 3).await;
    assert!(cluster.wait_for_leaders(Duration::from_secs(10)).await, "leaders elected");
    let leader = cluster.leader().expect("has leader");

    // Create two organizations so the region hosts >1 per-org group.
    let org_a = create_organization(&cluster, leader.id, "xfer-a").await.expect("create A");
    let org_b = create_organization(&cluster, leader.id, "xfer-b").await.expect("create B");
    let id_a = resolve_org_id(leader, org_a);
    let id_b = resolve_org_id(leader, org_b);

    // Wait for the initial delegated watcher to propagate leadership into
    // each per-org group on the current leader node.
    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let initial_leader = loop {
        let region_leader = leader
            .manager
            .get_region_group(Region::US_EAST_VA)
            .expect("region group")
            .handle()
            .current_leader();
        if let Some(l) = region_leader {
            break l;
        }
        if tokio::time::Instant::now() >= initial_deadline {
            panic!("initial region leader never appeared");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Pick a different node to transfer leadership to.
    let target_node = cluster
        .nodes()
        .iter()
        .find(|n| n.id != initial_leader)
        .expect("at least one non-leader node");
    let target_id = target_node.id;

    // Transfer leadership of the region's data-region group.
    let region_group = leader.manager.get_region_group(Region::US_EAST_VA).expect("region group");
    region_group.handle().transfer_leader(target_id).await.expect("leader transfer should succeed");

    // Poll until every node observes both the region group AND both per-org
    // groups reporting the new leader.
    let mut cascaded = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let mut ok = true;
        'outer: for node in cluster.nodes() {
            let region_leader = node
                .manager
                .get_region_group(Region::US_EAST_VA)
                .expect("region group")
                .handle()
                .current_leader();
            if region_leader != Some(target_id) {
                ok = false;
                break;
            }
            for id in [id_a, id_b] {
                let g = node.manager.route_organization(id).expect("org group");
                if g.handle().current_leader() != Some(target_id) {
                    ok = false;
                    break 'outer;
                }
            }
        }
        if ok {
            cascaded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(cascaded, "region leader transfer must cascade to every per-organization group");
}
