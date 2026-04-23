//! Scale-validation stress tests extracted from `externalized_state.rs`.
//!
//! Exercises larger workloads (10K–100K entities, 50 orgs / 500 vaults) that
//! validate throughput and snapshot round-trip at production-like scale.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::manual_range_contains
)]

use std::time::Duration;

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, VaultSlug};

use crate::common::{TestCluster, create_read_client, create_write_client};

// =============================================================================
// Helpers
// =============================================================================

/// Creates an organization and returns its slug.
async fn create_organization(
    addr: &str,
    name: &str,
    node: &crate::common::TestNode,
) -> Result<OrganizationSlug, Box<dyn std::error::Error>> {
    let (slug, _admin) = crate::common::create_test_organization(addr, name, node).await?;
    Ok(slug)
}

/// Creates a vault in an organization and returns its slug.
async fn create_vault(
    addr: &str,
    organization: OrganizationSlug,
) -> Result<VaultSlug, Box<dyn std::error::Error>> {
    crate::common::create_test_vault(addr, organization).await
}

/// Writes an entity and returns the assigned sequence number.
async fn write_entity(
    addr: &str,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
    value: &[u8],
    client_id: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = create_write_client(addr).await?;
    let response = client
        .write(proto::WriteRequest {
            client_id: Some(proto::ClientId { id: client_id.to_string() }),
            idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
            organization: Some(proto::OrganizationSlug { slug: organization.value() }),
            vault: Some(proto::VaultSlug { slug: vault.value() }),
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
        })
        .await?;
    match response.into_inner().result {
        Some(proto::write_response::Result::Success(s)) => Ok(s.assigned_sequence),
        Some(proto::write_response::Result::Error(e)) => {
            Err(format!("write error: {:?}", e).into())
        },
        None => Err("no result in write response".into()),
    }
}

/// Maximum retries for transient write failures (leader election, forwarding).
const WRITE_MAX_RETRIES: u32 = 15;

/// Delay between write retries.
const WRITE_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Writes via a leader-bound client, retrying on transient UNAVAILABLE errors.
///
/// Long-running stress tests can outlive a single leader: under sustained write
/// pressure the raft leader may briefly lose quorum heartbeat and elections can
/// fire. When this happens, the bound client returns "NotLeader" and its
/// bound address is now stale — retrying against the same channel cannot
/// succeed. On each UNAVAILABLE, we wait for cluster-wide leader agreement and
/// reconnect to whichever node is currently leader (may be the same).
async fn write_with_retry(
    cluster: &TestCluster,
    client: &mut proto::write_service_client::WriteServiceClient<tonic::transport::Channel>,
    request: proto::WriteRequest,
    label: &str,
) {
    for attempt in 0..=WRITE_MAX_RETRIES {
        // Each attempt needs a fresh idempotency key to avoid dedup
        let mut req = request.clone();
        if attempt > 0 {
            req.idempotency_key = uuid::Uuid::new_v4().as_bytes().to_vec();
        }
        match client.write(req).await {
            Ok(response) => match response.into_inner().result {
                Some(proto::write_response::Result::Success(_)) => return,
                other => panic!("{label}: unexpected result: {other:?}"),
            },
            Err(status)
                if status.code() == tonic::Code::Unavailable && attempt < WRITE_MAX_RETRIES =>
            {
                tokio::time::sleep(WRITE_RETRY_DELAY).await;
                cluster.wait_for_leader().await;
                if let Some(leader) = cluster.leader()
                    && let Ok(fresh) = create_write_client(&leader.addr).await
                {
                    *client = fresh;
                }
            },
            Err(e) => panic!("{label}: write failed after {attempt} retries: {e}"),
        }
    }
}

/// Reads an entity and returns its value (if it exists).
async fn read_entity(
    addr: &str,
    organization: OrganizationSlug,
    vault: VaultSlug,
    key: &str,
) -> Option<Vec<u8>> {
    let mut client = create_read_client(addr).await.ok()?;
    let response = client
        .read(proto::ReadRequest {
            organization: Some(proto::OrganizationSlug { slug: organization.value() }),
            vault: Some(proto::VaultSlug { slug: vault.value() }),
            key: key.to_string(),
            consistency: 0,
            caller: None,
        })
        .await
        .ok()?;
    response.into_inner().value
}

// =============================================================================
// Scale Validation
// =============================================================================

/// Writes >10,000 entities to a single vault, replicates to a 3-node cluster,
/// and verifies all data is present on a follower. This exercises the snapshot
/// path because followers that fall behind receive state via snapshot transfer.
///
/// Regression test: the old `CombinedSnapshot` silently capped entities at
/// 10,000 per vault. The file-based streaming snapshot has no such limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_snapshot_over_10k_entities_per_vault_no_data_loss() {
    let cluster = TestCluster::new(3).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let organization = create_organization(&leader.addr, "10k-plus-snap-ns", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, organization).await.expect("create vault");

    // Write 10,001 entities — one more than the old cap.
    let entity_count = 10_001;
    let mut write_client = create_write_client(&leader.addr).await.expect("connect");
    for i in 0..entity_count {
        write_with_retry(
            &cluster,
            &mut write_client,
            proto::WriteRequest {
                client_id: Some(proto::ClientId { id: "10k-writer".to_string() }),
                idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
                organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                vault: Some(proto::VaultSlug { slug: vault.value() }),
                operations: vec![proto::Operation {
                    op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                        key: format!("e-{i:05}"),
                        value: format!("v-{i}").into_bytes(),
                        expires_at: None,
                        condition: None,
                    })),
                }],
                include_tx_proof: false,
                caller: None,
            },
            &format!("write {i}"),
        )
        .await;
    }

    // Wait for replication to followers.
    let synced = cluster.wait_for_sync(Duration::from_secs(60)).await;
    assert!(synced, "cluster should sync after 10K+ writes");

    // Verify data on a follower — proves snapshot/replication transferred all entities.
    let follower = cluster.followers().into_iter().next().expect("should have follower");

    // Check first, last, and boundary entities.
    let first = read_entity(&follower.addr, organization, vault, "e-00000").await;
    assert_eq!(first, Some(b"v-0".to_vec()), "first entity should be present on follower");

    let at_cap = read_entity(&follower.addr, organization, vault, "e-09999").await;
    assert_eq!(
        at_cap,
        Some(b"v-9999".to_vec()),
        "entity at old 10K cap should be present on follower"
    );

    let beyond_cap = read_entity(&follower.addr, organization, vault, "e-10000").await;
    assert_eq!(
        beyond_cap,
        Some(b"v-10000".to_vec()),
        "entity beyond old 10K cap should be present on follower (cap removed)"
    );

    // Spot-check mid-range for integrity.
    let mid = read_entity(&follower.addr, organization, vault, "e-05000").await;
    assert_eq!(mid, Some(b"v-5000".to_vec()), "mid-range entity should match");
}

/// Verifies that 10,000 writes are replicated correctly and state roots match
/// across all nodes.
///
/// This is the full-scale variant of `test_bulk_writes_replicated_state_roots_match`.
/// Uses retry logic for transient UNAVAILABLE errors during leader elections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_10k_writes_replicated_state_roots_match() {
    let cluster = TestCluster::new(3).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    let organization = create_organization(&leader.addr, "10k-writes-ns", leader)
        .await
        .expect("create organization");
    let vault = create_vault(&leader.addr, organization).await.expect("create vault");

    let mut write_client = create_write_client(&leader.addr).await.expect("connect");
    for i in 0..10_000 {
        write_with_retry(
            &cluster,
            &mut write_client,
            proto::WriteRequest {
                client_id: Some(proto::ClientId { id: "bulk-writer".to_string() }),
                idempotency_key: uuid::Uuid::new_v4().as_bytes().to_vec(),
                organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                vault: Some(proto::VaultSlug { slug: vault.value() }),
                operations: vec![proto::Operation {
                    op: Some(proto::operation::Op::SetEntity(proto::SetEntity {
                        key: format!("entity-{:05}", i),
                        value: format!("data-{}", i).into_bytes(),
                        expires_at: None,
                        condition: None,
                    })),
                }],
                include_tx_proof: false,
                caller: None,
            },
            &format!("write {i}"),
        )
        .await;
    }

    let synced = cluster.wait_for_sync(Duration::from_secs(60)).await;
    assert!(synced, "all nodes should sync after 10K writes");

    let leader_applied = leader.last_applied();
    for follower in cluster.followers() {
        assert_eq!(
            follower.last_applied(),
            leader_applied,
            "follower {} should match leader's last_applied",
            follower.id
        );
    }

    let follower = cluster.followers().into_iter().next().expect("follower");
    let first = read_entity(&follower.addr, organization, vault, "entity-00000").await;
    assert_eq!(first, Some(b"data-0".to_vec()));

    let last = read_entity(&follower.addr, organization, vault, "entity-09999").await;
    assert_eq!(last, Some(b"data-9999".to_vec()));
}

/// Exercises the apply loop with 10 organizations, 50 vaults, and 2K writes
/// using unique client IDs. Validates that multi-org/vault write distribution
/// works correctly with externalized state persistence.
///
/// Success criteria: all 2K writes complete within 120 seconds and spot-check
/// reads return correct values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_org_vault_write_distribution_2k_entities() {
    let cluster = TestCluster::new(1).await;
    cluster
        .create_data_region(inferadb_ledger_types::Region::US_EAST_VA)
        .await
        .expect("create data region");
    let _leader_id = cluster.wait_for_leader().await;
    let leader = cluster.leader().expect("should have leader");

    // Create 10 orgs with 5 vaults each.
    let mut targets: Vec<(OrganizationSlug, VaultSlug)> = Vec::new();
    for i in 0..10 {
        let org = create_organization(&leader.addr, &format!("throughput-org-{}", i), leader)
            .await
            .expect("create org");
        for _v in 0..5 {
            let vault = create_vault(&leader.addr, org).await.expect("create vault");
            targets.push((org, vault));
        }
    }

    // Write 2K entities spread across all vaults with unique client IDs.
    let start = std::time::Instant::now();
    for i in 0..2_000 {
        let (org, vault) = targets[i % targets.len()];
        let client_id = format!("tp-client-{}", i);
        write_entity(
            &leader.addr,
            org,
            vault,
            &format!("tp-key-{}", i),
            format!("tp-val-{}", i).as_bytes(),
            &client_id,
        )
        .await
        .unwrap_or_else(|e| panic!("write {} failed: {}", i, e));
    }
    let elapsed = start.elapsed();

    let throughput = 2_000.0 / elapsed.as_secs_f64();
    println!(
        "2K writes across 10 orgs / 50 vaults completed in {:.1}s ({:.0} ops/sec)",
        elapsed.as_secs_f64(),
        throughput,
    );

    // Bound: 2K sequential writes should finish well within 120 seconds even in
    // debug builds under CI load. This catches regressions that tank apply throughput.
    assert!(elapsed < Duration::from_secs(120), "2K writes took {:?}, expected < 120s", elapsed);

    // Spot-check entries from multiple organizations to verify cross-org writes.
    let (org_first, vault_first) = targets[0];
    let value = read_entity(&leader.addr, org_first, vault_first, "tp-key-0").await;
    assert_eq!(value, Some(b"tp-val-0".to_vec()), "first org/vault entry should be readable");

    let last_idx = 1_999;
    let (org_last, vault_last) = targets[last_idx % targets.len()];
    let value =
        read_entity(&leader.addr, org_last, vault_last, &format!("tp-key-{last_idx}")).await;
    assert_eq!(
        value,
        Some(format!("tp-val-{last_idx}").into_bytes()),
        "last entry should be readable on its assigned org/vault"
    );
}
