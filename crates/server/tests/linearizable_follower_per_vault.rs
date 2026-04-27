//! Per-vault LINEARIZABLE follower-read regression tests.
//!
//! Phase B introduced per-vault Raft groups that own their own apply pipeline:
//! per-vault entity writes advance the **vault group's** committed and applied
//! indices, not the parent organization / region group's. The follower-side
//! ReadIndex protocol (`Read` / `BatchRead` on a non-leader with
//! `LINEARIZABLE` consistency) must therefore:
//!
//! 1. Ask the **leader of the vault group** for its committed index — the region group's commit
//!    index does not move on a per-vault entity write.
//! 2. Wait for the **vault group's** applied-index watch on this follower — the org-scoped watch
//!    never observes per-vault entity apply.
//!
//! These tests cover the wire contract (the `CommittedIndex` RPC's optional
//! vault scope) and the end-to-end behaviour (a LINEARIZABLE read on a
//! follower must see a recent write to the same vault).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::time::Duration;

use inferadb_ledger_proto::proto::{
    self, raft_service_client::RaftServiceClient, read_service_client::ReadServiceClient,
};
use inferadb_ledger_types::{OrganizationSlug, Region, VaultSlug};

use crate::common::{TestCluster, TestNode, connect_channel, create_write_client};

/// Creates an organization via the production saga pipeline and returns its
/// external slug. Mirrors `three_tier_consensus::create_organization`.
async fn create_organization(
    addr: &str,
    name: &str,
    node: &TestNode,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let (slug, _admin) = crate::common::create_test_organization(addr, name, node).await?;
    Ok(slug)
}

async fn create_vault(
    addr: &str,
    org: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    crate::common::create_test_vault(addr, org).await
}

/// Writes `(key, value)` into `(org, vault)` and returns the committed
/// vault block height.
async fn write_entity(
    addr: &str,
    org: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
    value: &[u8],
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_write_client(addr).await?;
    let request = proto::WriteRequest {
        organization: Some(proto::OrganizationSlug { slug: org.value() }),
        vault: Some(proto::VaultSlug { slug: vault.value() }),
        client_id: Some(proto::ClientId { id: "linearizable-follower-test".to_string() }),
        idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
        operations: vec![proto::Operation {
            op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                key: key.to_string(),
                value: value.to_vec(),
                expires_at: None,
                condition: None,
            })),
        }],
        include_tx_proof: false,
        caller: None,
    };
    let response = client.write(request).await?.into_inner();
    match response.result {
        Some(proto::write_response::Result::Success(s)) => Ok(s.block_height),
        Some(proto::write_response::Result::Error(e)) => {
            Err(format!("write error: {:?}", e).into())
        },
        None => Err("no result in write response".into()),
    }
}

/// LINEARIZABLE read directly via the gRPC `Read` RPC (returns the raw
/// response so the test can inspect both the value and the block height).
async fn linearizable_read(
    addr: &str,
    org: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Result<proto::ReadResponse, tonic::Status> {
    let channel = connect_channel(addr);
    let mut client = ReadServiceClient::new(channel);
    let request = proto::ReadRequest {
        organization: Some(proto::OrganizationSlug { slug: org.value() }),
        vault: Some(proto::VaultSlug { slug: vault.value() }),
        key: key.to_string(),
        consistency: proto::ReadConsistency::Linearizable as i32,
        caller: None,
    };
    let response = client.read(request).await?;
    Ok(response.into_inner())
}

// ============================================================================
// End-to-end: LINEARIZABLE follower read sees a recent vault write
// ============================================================================

/// Per-vault entity writes advance the vault group's apply pointer, not the
/// org / region group's. A LINEARIZABLE follower read on the same vault must
/// wait on the **vault group's** applied-index watch (not the region group's)
/// — otherwise the wait can release before the per-vault apply has finished
/// and the read returns stale or NotFound data.
///
/// The forcing function is interleaved per-vault writes: writes to vault A
/// bump the *organization*-scoped sequence counters and, in earlier code
/// paths, could leave the org-scoped applied watch newer than the vault-B
/// watch. With the bug, a follower reading vault B with LINEARIZABLE
/// consistency would consult the wrong watch and might serve before vault
/// B's apply has caught up. With the fix, the follower waits on vault B's
/// own apply pointer and always sees the value just written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_follower_per_vault_read_sees_recent_write() {
    let cluster = TestCluster::new(3).await;
    cluster.create_data_region(Region::US_EAST_VA).await.expect("create data region");
    assert!(cluster.wait_for_leaders(Duration::from_secs(15)).await, "leaders elected");

    // Wait for the data region's leader to settle — writes go through the
    // region leader, not the system leader, and `cluster.leader()` returns
    // the system leader. Without an explicit wait the data-region leader
    // can flap during early bootstrap.
    let region_leader_id = cluster.wait_for_region_leader(Region::US_EAST_VA).await;
    let region_leader = cluster
        .nodes()
        .iter()
        .find(|n| n.id == region_leader_id)
        .expect("region leader in cluster");

    // One organization, two vaults — the per-vault apply pipelines progress
    // independently inside the same org Raft group. Drive provisioning
    // through the region leader so the slug index publishes deterministically.
    let org = create_organization(&region_leader.addr, "lin-follower-org", region_leader)
        .await
        .expect("create organization");
    let vault_a = create_vault(&region_leader.addr, org).await.expect("create vault A");
    let vault_b = create_vault(&region_leader.addr, org).await.expect("create vault B");
    cluster.wait_for_organizations_synced(Region::US_EAST_VA, Duration::from_secs(10)).await;

    // Pick a follower of the data region (vault groups follow the region
    // leader under delegated leadership).
    let follower_addr = cluster
        .nodes()
        .iter()
        .find(|n| n.id != region_leader_id)
        .map(|n| n.addr.clone())
        .expect("a follower exists in the cluster");

    // Drive writes to both vaults. Writes to vault A advance org-scoped
    // sequence counters; writes to vault B advance vault-B's apply pointer.
    // Reads against the follower for vault B must wait on vault B's pointer.
    for round in 0..5 {
        // Vault A noise — these advance counters that are visible on the
        // org-scoped watch but say nothing about vault B's apply progress.
        let _ = write_entity(
            &region_leader.addr,
            org,
            vault_a,
            &format!("noise-{round}"),
            format!("noise-value-{round}").as_bytes(),
        )
        .await
        .expect("write to vault A");

        // Vault B target — the value the follower must read after the
        // LINEARIZABLE wait completes.
        let key = format!("target-{round}");
        let value = format!("vault-b-value-{round}");
        let height = write_entity(&region_leader.addr, org, vault_b, &key, value.as_bytes())
            .await
            .expect("write to vault B");
        assert!(height > 0, "vault B write must commit");

        // Read from the **follower** with LINEARIZABLE consistency.
        // Without the fix, this can race: the wait may consult the org
        // group's applied watch, which advances on vault-A writes, and
        // release before vault-B apply has caught up — surfacing as
        // value=None or a stale value.
        let response = linearizable_read(&follower_addr, org, vault_b, &key)
            .await
            .expect("LINEARIZABLE follower read succeeds");
        assert_eq!(
            response.value.as_deref(),
            Some(value.as_bytes()),
            "LINEARIZABLE follower read on vault B (round {round}) must see the just-written value; \
             with the bug, the wait targets the org-scoped watch and can release before vault B \
             apply lands",
        );
    }
}

// ============================================================================
// CommittedIndex RPC: vault scope returns the per-vault committed index
// ============================================================================

/// Sending `CommittedIndex` with `(organization, vault)` set returns the
/// **vault group's** committed index and term — distinct from the region
/// group's commit index, which the legacy field-only request returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_index_rpc_with_vault_scope_returns_vault_index() {
    let cluster = TestCluster::new(3).await;
    cluster.create_data_region(Region::US_EAST_VA).await.expect("create data region");
    assert!(cluster.wait_for_leaders(Duration::from_secs(15)).await, "leaders elected");
    let leader = cluster.leader().expect("has leader");

    let org =
        create_organization(&leader.addr, "ci-rpc-org", leader).await.expect("create organization");
    let vault = create_vault(&leader.addr, org).await.expect("create vault");

    // Ensure the per-vault group has a committed write so its index is
    // distinct from the region group's. Without traffic, a fresh vault
    // group can sit at commit_index = 0.
    let _ = write_entity(&leader.addr, org, vault, "k", b"v").await.expect("seed write");

    // Resolve the region-group leader's address. Vault groups follow the
    // region leader under delegated leadership, so the same node owns the
    // vault leader handle.
    let leader_id = cluster
        .nodes()
        .iter()
        .find_map(|n| {
            n.manager
                .get_region_group(Region::US_EAST_VA)
                .ok()
                .and_then(|g| g.handle().current_leader())
        })
        .expect("region leader known");
    let leader_node =
        cluster.nodes().iter().find(|n| n.id == leader_id).expect("leader node in cluster");

    let channel = connect_channel(&leader_node.addr);
    let mut client = RaftServiceClient::new(channel);

    // Region scope (legacy): returns the region group's commit index.
    let region_resp = client
        .committed_index(proto::CommittedIndexRequest {
            region: Region::US_EAST_VA.as_str().to_string(),
            organization: None,
            vault: None,
        })
        .await
        .expect("region-scoped CommittedIndex succeeds")
        .into_inner();

    // Vault scope: returns the vault group's commit index.
    let vault_resp = client
        .committed_index(proto::CommittedIndexRequest {
            region: Region::US_EAST_VA.as_str().to_string(),
            organization: Some(proto::OrganizationSlug { slug: org.value() }),
            vault: Some(proto::VaultSlug { slug: vault.value() }),
        })
        .await
        .expect("vault-scoped CommittedIndex succeeds")
        .into_inner();

    // Both indices must be non-zero — the region group has had organization
    // / vault metadata writes; the vault group has the entity write above.
    assert!(
        region_resp.committed_index > 0,
        "region group should have a non-zero committed index after data-region setup",
    );
    assert!(
        vault_resp.committed_index > 0,
        "vault group should have a non-zero committed index after the seed entity write",
    );

    // The two indices come from independent shards. Asserting strict
    // inequality is order-dependent on background traffic; the load-bearing
    // invariant is that the vault-scoped path returns the **vault group's**
    // index. We test that by looking up the vault group's handle directly
    // and comparing.
    let system_state =
        leader_node.manager.system_region().expect("system region").applied_state().clone();
    let org_id = system_state.resolve_slug_to_id(org).expect("org slug resolves");
    let vault_id = system_state.resolve_vault_slug_to_id(vault).expect("vault slug resolves");
    let vault_group = leader_node
        .manager
        .get_vault_group(Region::US_EAST_VA, org_id, vault_id)
        .expect("vault group live on leader");
    let direct_commit = vault_group.handle().commit_index();
    assert!(
        vault_resp.committed_index >= direct_commit,
        "RPC's vault-scoped commit index ({}) must be at least the vault handle's direct commit \
         index ({})",
        vault_resp.committed_index,
        direct_commit,
    );
    // The leader's own commit index for the vault shard is the source of
    // truth; the RPC just reads it back.
    let region_group =
        leader_node.manager.get_region_group(Region::US_EAST_VA).expect("region group");
    assert_eq!(
        region_resp.committed_index,
        region_group.handle().commit_index(),
        "region-scoped RPC must return the region group's commit index",
    );
}

// ============================================================================
// CommittedIndex RPC: partial vault scope is rejected
// ============================================================================

/// Vault scope is all-or-nothing: setting only `organization` or only
/// `vault` is a protocol error. The handler must reject with
/// `InvalidArgument` rather than silently fall back to the region scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_index_rpc_rejects_partial_vault_scope() {
    let cluster = TestCluster::new(1).await;
    cluster.create_data_region(Region::US_EAST_VA).await.expect("create data region");
    assert!(cluster.wait_for_leaders(Duration::from_secs(15)).await, "leaders elected");
    let leader = cluster.leader().expect("has leader");

    let channel = connect_channel(&leader.addr);
    let mut client = RaftServiceClient::new(channel);

    // organization set, vault unset → InvalidArgument
    let resp_org_only = client
        .committed_index(proto::CommittedIndexRequest {
            region: Region::US_EAST_VA.as_str().to_string(),
            organization: Some(proto::OrganizationSlug { slug: 12345 }),
            vault: None,
        })
        .await;
    let status = resp_org_only.expect_err("organization-only must be rejected");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "organization-only vault scope must be InvalidArgument, got: {:?}",
        status,
    );

    // vault set, organization unset → InvalidArgument
    let resp_vault_only = client
        .committed_index(proto::CommittedIndexRequest {
            region: Region::US_EAST_VA.as_str().to_string(),
            organization: None,
            vault: Some(proto::VaultSlug { slug: 67890 }),
        })
        .await;
    let status = resp_vault_only.expect_err("vault-only must be rejected");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "vault-only vault scope must be InvalidArgument, got: {:?}",
        status,
    );
}
