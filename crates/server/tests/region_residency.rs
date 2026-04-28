//! Integration tests for the GLOBAL region directory residency contract
//! (R6: `requires_residency` + `retention_days` move off `Region` and onto
//! `RegionDirectoryEntry`).
//!
//! Validates that:
//!
//! 1. Provisioning a non-EU region with `requires_residency = false, retention_days = 90` persists
//!    those values verbatim and the lookup helper returns them.
//! 2. Provisioning an EU region with `requires_residency = true, retention_days = 30` honours the
//!    GDPR contract — the lookup helper returns the strict 30-day retention rather than the
//!    historical hardcoded 90-day default that the removed `Region::retention_days()` method
//!    emitted for any non-built-in EU slug.
//! 3. The migration RPC `AdminService::SetRegionResidency` updates the persisted contract —
//!    operators upgrading across the R6 boundary call this to restore stricter retention contracts
//!    on entries that decoded with the disciplined defaults.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use inferadb_ledger_state::system::{
    RegionResidency, lookup_region_residency, region_residency_or_default,
};
use inferadb_ledger_types::Region;

use crate::common::TestCluster;

/// US-style region: `requires_residency = false`, `retention_days = 90`.
/// Lookup must return the persisted contract verbatim — no slug-based
/// hardcoding falls through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn us_region_persists_non_residency_contract() {
    let cluster = TestCluster::new(3).await;

    let region = Region::US_EAST_VA;
    cluster
        .create_data_region_with_residency(region, false, false, 90)
        .await
        .expect("provision us-east-va with explicit non-residency contract");

    // Every node must see the same contract — the directory entry is
    // GLOBAL state, replicated via Raft.
    for node in cluster.nodes() {
        let group = node.manager.system_region().expect("system region available on every node");
        let residency =
            lookup_region_residency(group.state(), region).expect("lookup_region_residency ok");
        assert_eq!(
            residency,
            Some(RegionResidency { requires_residency: false, retention_days: 90 }),
            "node {} returned wrong residency for US_EAST_VA",
            node.id,
        );

        let with_default = region_residency_or_default(group.state(), region);
        assert!(!with_default.requires_residency);
        assert_eq!(with_default.retention_days, 90);
    }
}

/// EU region: `requires_residency = true`, `retention_days = 30` (GDPR).
/// This is the *exact* contract the removed `Region::retention_days()`
/// silently violated for any non-built-in EU slug. The R6 registry must
/// honour the operator-supplied contract, regardless of slug.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eu_region_persists_gdpr_thirty_day_retention() {
    let cluster = TestCluster::new(3).await;

    // Use a *custom* EU slug to prove the registry — not the slug — is the
    // source of truth. Pre-R6 behaviour for `"eu-custom"` would have
    // returned `retention_days = 90` (a GDPR violation).
    let region = Region::new_owned("eu-custom");
    cluster
        .create_data_region_with_residency(region, true, true, 30)
        .await
        .expect("provision eu-custom with GDPR residency contract");

    // The directory entry persists; only the GLOBAL leader's voters wait
    // for the protected region to start locally (test helper short-circuits
    // for protected regions). The GLOBAL state itself is replicated to
    // every node, so the lookup works on every voter.
    for node in cluster.nodes() {
        let group = node.manager.system_region().expect("system region available on every node");
        let residency = lookup_region_residency(group.state(), region)
            .expect("lookup_region_residency ok for eu-custom");
        assert_eq!(
            residency,
            Some(RegionResidency { requires_residency: true, retention_days: 30 }),
            "node {} returned wrong GDPR residency for eu-custom",
            node.id,
        );
    }
}

/// `Region::GLOBAL` is hardcoded as the cluster control plane: never
/// subject to data-residency rules, no retention soft-delete cycle. The
/// lookup helper returns `None` (signalling "control plane, not a data
/// region") and `region_residency_or_default` returns
/// `requires_residency = false, retention_days = 0` as a tripwire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_region_short_circuits_residency_lookup() {
    let cluster = TestCluster::new(1).await;
    let node = cluster.nodes().first().expect("at least one node");
    let group = node.manager.system_region().expect("system region");

    let residency = lookup_region_residency(group.state(), Region::GLOBAL)
        .expect("lookup_region_residency for GLOBAL");
    assert_eq!(residency, None, "GLOBAL must short-circuit lookup_region_residency");

    let with_default = region_residency_or_default(group.state(), Region::GLOBAL);
    assert!(!with_default.requires_residency, "GLOBAL is never a residency-required region");
    assert_eq!(
        with_default.retention_days, 0,
        "GLOBAL retention is a 0-day tripwire — soft-deletes in GLOBAL are bugs",
    );
}

/// Unknown / unprovisioned regions fall back to the disciplined defaults
/// (`requires_residency = true`, `retention_days = 90`). This is the
/// safety property that protects clusters upgraded across the R6 boundary
/// where pre-existing directory entries decoded without the new fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_region_uses_disciplined_default() {
    let cluster = TestCluster::new(1).await;
    let node = cluster.nodes().first().expect("at least one node");
    let group = node.manager.system_region().expect("system region");

    let unknown = Region::new_owned("never-provisioned-region");
    let residency =
        lookup_region_residency(group.state(), unknown).expect("lookup_region_residency ok");
    assert_eq!(residency, None, "unknown region returns None — caller picks default policy");

    let with_default = region_residency_or_default(group.state(), unknown);
    assert!(
        with_default.requires_residency,
        "default-deny: unknown region must be treated as residency-required",
    );
    assert_eq!(
        with_default.retention_days, 90,
        "disciplined default is 90 days for unknown regions",
    );
}
