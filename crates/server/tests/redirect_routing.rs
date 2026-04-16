//! Integration test: Phase 5 redirect-only routing.
//!
//! Proves that a client configured with a `preferred_region` continues to make
//! progress when its cached regional leader becomes stale. Specifically:
//!
//! 1. Bring up a 3-node cluster with the GLOBAL region plus the `US_EAST_VA` data region.
//! 2. Connect an SDK client (preferred_region = US_EAST_VA) through one of the follower nodes for
//!    that data region. The first write resolves the leader via `ResolveRegionLeader` (cache miss)
//!    and lands directly on it.
//! 3. Force a leadership transfer of the data region to a different voter.
//! 4. The next write hits the now-stale cached leader, the server returns `Unavailable` carrying a
//!    `LeaderHint`, and the SDK retries against the new leader. This is exactly the redirect cycle
//!    Phase 5 introduced — `apply_region_leader_hint_or_invalidate` increments
//!    `ledger_sdk_redirect_retries_total{region="us-east-va"}`.
//!
//! What this test does NOT directly assert: the absence of any
//! `ForwardRegionalProposal` traffic on the client path. That RPC is still
//! used by the saga orchestrator and we have no per-caller counter to observe
//! from outside. The redirect-retry counter incrementing is, however, the
//! observable signal that the SDK took the redirect path the server expects.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use inferadb_ledger_sdk::{ClientConfig, LedgerClient, SdkMetrics, ServerSource};
use inferadb_ledger_types::{Region, UserSlug};

use crate::common::{TestCluster, create_test_organization, create_test_vault};

/// SDK metrics sink that counts the events relevant to the redirect path.
#[derive(Debug, Default)]
struct RedirectMetrics {
    redirect_retries: AtomicU64,
    leader_cache_hits: AtomicU64,
    leader_cache_misses: AtomicU64,
}

impl SdkMetrics for RedirectMetrics {
    fn redirect_retry(&self, _region: &str) {
        self.redirect_retries.fetch_add(1, Ordering::SeqCst);
    }
    fn leader_cache_hit(&self, _region: &str) {
        self.leader_cache_hits.fetch_add(1, Ordering::SeqCst);
    }
    fn leader_cache_miss(&self, _region: &str) {
        self.leader_cache_misses.fetch_add(1, Ordering::SeqCst);
    }
}

/// Returns the current leader node ID for the given data region by polling
/// every node's view, since followers may briefly lag.
fn current_region_leader(cluster: &TestCluster, region: Region) -> Option<u64> {
    cluster
        .nodes()
        .iter()
        .find_map(|n| n.region_group(region).and_then(|rg| rg.handle().current_leader()))
}

/// Waits until any node reports a different region leader than `previous`.
///
/// We do not require cluster-wide agreement here: the SDK's stale write goes
/// to the previous leader (now a follower), which already knows about the new
/// term and replies `Unavailable` with a `LeaderHint`. As long as some node
/// has switched to the new leader, the redirect cycle will resolve.
async fn wait_for_new_region_leader(
    cluster: &TestCluster,
    region: Region,
    previous: u64,
    duration: Duration,
) -> Option<u64> {
    let start = tokio::time::Instant::now();
    while start.elapsed() < duration {
        for node in cluster.nodes() {
            if let Some(rg) = node.region_group(region)
                && let Some(leader) = rg.handle().current_leader()
                && leader != previous
            {
                return Some(leader);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_via_stale_region_cache_redirects_and_succeeds() {
    let cluster = TestCluster::with_data_regions(3, 1).await;
    assert!(
        cluster.wait_for_leaders(Duration::from_secs(15)).await,
        "all regions should elect leaders"
    );

    let region = Region::US_EAST_VA;

    // Discover the data region's current leader and pick a follower as the
    // client's gateway endpoint. Routing through a non-leader exercises the
    // resolve+connect path; the actual write will then be served by the
    // resolved leader directly.
    let initial_leader_id =
        current_region_leader(&cluster, region).expect("data region leader should exist");
    let follower_node = cluster
        .nodes()
        .iter()
        .find(|n| n.id != initial_leader_id)
        .expect("at least one follower should exist");
    let gateway_endpoint = format!("http://{}", follower_node.addr);

    // Provision an org + vault on US_EAST_VA via the existing helpers (these
    // talk to whichever node accepts the request; saga + global routing handle
    // it).
    let any_node = cluster.any_node();
    let (org_slug, admin_slug) =
        create_test_organization(any_node.addr, "redirect-routing", any_node)
            .await
            .expect("create organization");
    let vault_slug = create_test_vault(any_node.addr, org_slug).await.expect("create vault");

    // Build the SDK client. `preferred_region` is what enables the
    // region-leader cache on the connection pool — without it the redirect
    // metric is unreachable by construction.
    let metrics = Arc::new(RedirectMetrics::default());
    let config = ClientConfig::builder()
        .servers(ServerSource::from_static([gateway_endpoint.clone()]))
        .client_id("redirect-routing-test")
        .preferred_region(region)
        .metrics(Arc::clone(&metrics) as Arc<dyn SdkMetrics>)
        .build()
        .expect("valid client config");

    let client = LedgerClient::new(config).await.expect("connect SDK client");

    // First write: cold cache. The SDK calls ResolveRegionLeader through the
    // follower (the gateway), gets back the actual leader endpoint, and lands
    // the write on it. We expect no redirect retries here — the server told us
    // the right leader before we ever issued the proposal.
    client
        .set_entity(
            UserSlug::new(admin_slug),
            org_slug,
            Some(vault_slug),
            "redirect-key-1",
            b"value-1".to_vec(),
            None,
            None,
            None,
        )
        .await
        .expect("first write should succeed via region resolver");

    let baseline_redirects = metrics.redirect_retries.load(Ordering::SeqCst);
    assert_eq!(
        baseline_redirects, 0,
        "redirect_retry must not fire on the warm-resolve path; got {baseline_redirects}"
    );

    // Force a leadership transfer in the data region. The SDK's cached leader
    // is the previous holder; the next write must observe `Unavailable` +
    // `LeaderHint` and trigger the redirect cycle.
    let leader_node =
        cluster.node(initial_leader_id).expect("initial region leader is in the cluster");
    let leader_region_group =
        leader_node.region_group(region).expect("leader hosts the region group");

    // Transfer leadership to the SDK's gateway follower. Any voter other than
    // the current leader is a valid target; we use the gateway because it is
    // guaranteed to have replicated the org/vault state (it served the org
    // creation traffic as a forwarder), so the post-transfer write does not
    // race GLOBAL Raft replication on a freshly-joined node. The redirect
    // cycle is still exercised: the SDK's cached leader is `initial_leader_id`,
    // which now responds Unavailable + LeaderHint pointing at the gateway.
    let transfer_target_id = follower_node.id;

    leader_region_group
        .handle()
        .transfer_leader(transfer_target_id)
        .await
        .expect("transfer leadership of data region");

    let new_leader_id =
        wait_for_new_region_leader(&cluster, region, initial_leader_id, Duration::from_secs(10))
            .await
            .expect("new region leader should be elected");
    assert_ne!(new_leader_id, initial_leader_id, "leadership should have moved");

    // Wait until the new region leader's GLOBAL applied state contains the
    // org we just created. The saga's CreateOrganization commit goes through
    // GLOBAL Raft, which replicates to every voter — but a follower-promoted
    // leader may briefly trail. Without this poll, the redirected second
    // write races the replication and gets a NotFound.
    let new_leader_node =
        cluster.node(new_leader_id).expect("new region leader must be present in the cluster");
    let global_accessor = new_leader_node.system_region().applied_state().clone();
    let visibility_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut org_visible = false;
    while tokio::time::Instant::now() < visibility_deadline {
        if let Some(internal_id) = global_accessor.resolve_slug_to_id(org_slug)
            && global_accessor.get_organization(internal_id).is_some()
        {
            org_visible = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Diagnostic: dump per-node GLOBAL slug visibility so a regression here
    // points at replication, not the redirect path under test.
    if !org_visible {
        for n in cluster.nodes() {
            let region_group = n.system_region();
            let acc = region_group.applied_state();
            eprintln!(
                "node {} sees slug {}: {:?}",
                n.id,
                org_slug.value(),
                acc.resolve_slug_to_id(org_slug)
            );
        }
    }
    assert!(
        org_visible,
        "org {} should be visible on the new region leader before redirect-write attempt",
        org_slug.value()
    );

    // Second write: cached leader is now stale. The first attempt hits the
    // old leader, which responds Unavailable + LeaderHint; the SDK applies
    // the hint (incrementing `redirect_retry`) and retries against the new
    // leader.
    client
        .set_entity(
            UserSlug::new(admin_slug),
            org_slug,
            Some(vault_slug),
            "redirect-key-2",
            b"value-2".to_vec(),
            None,
            None,
            None,
        )
        .await
        .expect("second write should succeed via redirect cycle");

    let redirects = metrics.redirect_retries.load(Ordering::SeqCst);
    assert!(
        redirects >= 1,
        "expected at least one redirect_retry after stale-cache write, got {redirects}"
    );

    // Sanity: the cache machinery was actually exercised at least once for
    // both classes of access (the cold resolve and the cached/redirect path).
    assert!(
        metrics.leader_cache_misses.load(Ordering::SeqCst) >= 1,
        "expected at least one leader_cache_miss on the cold-resolve path"
    );
    assert!(
        metrics.leader_cache_hits.load(Ordering::SeqCst) >= 1,
        "expected at least one leader_cache_hit after the cache was warmed"
    );
}
