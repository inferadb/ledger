//! Deterministic org → shard routing.
//!
//! Given an `OrganizationId` and the `shards_per_region` count for the
//! organization's region, [`resolve_shard`] returns a stable `ShardId`
//! via seahash. Every node (leader and follower) that runs the same code
//! against the same `shards_per_region` value computes the same mapping —
//! required for Raft state-machine determinism.
//!
//! # Relationship to other "shard"-named types
//!
//! The workspace has three distinct "shard" concepts:
//!
//! - [`crate::shard::ShardManager`] — per-shard state manager (one instance per Raft group).
//!   Misleadingly named; tracks vault metadata *within* a single shard. Unrelated to routing.
//! - `consensus::router::Router` — an explicit (org → shard) mapping table that supports dynamic
//!   split / merge operations. Reserved for Phase C shard-splitting; unused in Phase A.
//! - This module — the deterministic hash-based routing used at the service boundary. Stateless,
//!   config-driven. **This is what Phase A consumes.**
//!
//! # Routing stability
//!
//! The mapping is stable as long as `shards_per_region` does not change
//! for a given region. Phase A fixes `shards_per_region` at region-creation
//! time. Phase C's dynamic splitting will introduce a migration path when
//! shard count changes; until then, treat the count as immutable.

use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{OrganizationId, Region};

/// Opaque shard identifier within a region. Values are in `[0,
/// shards_per_region)`.
///
/// Mirrors `consensus::types::ShardId` but kept as a thin wrapper in this
/// crate to avoid a dependency cycle (state → consensus is not permitted;
/// consensus depends on state via the apply pipeline). Services convert
/// between the two at call sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardIdx(pub u64);

impl ShardIdx {
    /// Returns the underlying index value.
    #[must_use]
    pub fn value(self) -> u64 {
        self.0
    }
}

/// Resolves the shard owning `org_id` within `region` given the region's
/// shard count.
///
/// Deterministic: `seahash(region_bytes || org_id_bytes) % shards_per_region`.
/// Including the region in the hash input de-correlates the distribution
/// across regions (otherwise, org 1 would always land on shard 0 in every
/// region regardless of traffic shape).
///
/// # Panics
///
/// Panics in debug builds if `shards_per_region == 0`. Release builds
/// return `ShardIdx(0)` in that pathological case rather than dividing by
/// zero — but bootstrap validation rejects 0 long before this is reached.
///
/// # Example
///
/// ```
/// # use inferadb_ledger_state::shard_routing::{resolve_shard, ShardIdx};
/// # use inferadb_ledger_types::{OrganizationId, Region};
/// let shard = resolve_shard(Region::US_EAST_VA, OrganizationId::new(42), 16);
/// assert!(shard.value() < 16);
/// ```
#[must_use]
pub fn resolve_shard(region: Region, org_id: OrganizationId, shards_per_region: usize) -> ShardIdx {
    debug_assert!(shards_per_region > 0, "shards_per_region must be > 0");
    if shards_per_region <= 1 {
        return ShardIdx(0);
    }
    let mut key = Vec::with_capacity(region.as_str().len() + 1 + 8);
    key.extend_from_slice(region.as_str().as_bytes());
    key.push(b':');
    key.extend_from_slice(&org_id.value().to_le_bytes());
    let hash = seahash::hash(&key);
    ShardIdx(hash % shards_per_region as u64)
}

/// Lightweight cross-call holder for the node's shard routing policy.
///
/// Wraps a `shards_per_region` count plus convenience accessors so services
/// can hold one handle and call `router.resolve(region, org_id)` without
/// re-threading the shard count through every call site.
///
/// Phase A treats `shards_per_region` as uniform across all regions (read
/// from `RaftManagerConfig`). Phase C will introduce per-region counts read
/// from GLOBAL state; the API accommodates that evolution without caller
/// changes.
#[derive(Debug, Clone)]
pub struct ShardRouter {
    shards_per_region: usize,
}

impl ShardRouter {
    /// Creates a router with a uniform shard count across all regions.
    #[must_use]
    pub fn new(shards_per_region: usize) -> Self {
        Self { shards_per_region: shards_per_region.max(1) }
    }

    /// Returns the shard count configured for every region.
    #[must_use]
    pub fn shards_per_region(&self) -> usize {
        self.shards_per_region
    }

    /// Resolves `org_id` within `region` to its owning shard.
    #[must_use]
    pub fn resolve(&self, region: Region, org_id: OrganizationId) -> ShardIdx {
        resolve_shard(region, org_id, self.shards_per_region)
    }

    /// Returns every shard index in the region, in stable order. Useful for
    /// cross-shard fan-out queries (admin scans, region-wide compaction).
    #[must_use]
    pub fn all_shards(&self) -> impl Iterator<Item = ShardIdx> + '_ {
        (0..self.shards_per_region as u64).map(ShardIdx)
    }
}

// The trait bound exists solely to tie this module into the state crate's
// public surface alongside StateLayer / ShardManager. No backend access is
// needed for routing itself.
#[allow(dead_code)]
fn _phantom_backend_bound<B: StorageBackend>() {}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn single_shard_always_returns_zero() {
        for org in 0..100 {
            assert_eq!(resolve_shard(Region::US_EAST_VA, OrganizationId::new(org), 1), ShardIdx(0),);
        }
    }

    #[test]
    fn resolution_is_deterministic() {
        let a = resolve_shard(Region::US_EAST_VA, OrganizationId::new(42), 16);
        let b = resolve_shard(Region::US_EAST_VA, OrganizationId::new(42), 16);
        assert_eq!(a, b);
    }

    #[test]
    fn different_regions_decorrelate() {
        // Include `region` in the hash input so two regions with the same
        // shard count don't produce identical org → shard mappings.
        let va = resolve_shard(Region::US_EAST_VA, OrganizationId::new(1), 16);
        let ore = resolve_shard(Region::US_WEST_OR, OrganizationId::new(1), 16);
        // Not a hard correctness requirement that they differ for every
        // input, but if ALL orgs hashed identically across regions the
        // cross-region load shape would be silly. Sanity-check a sample.
        let collisions = (0..256u64)
            .filter(|&i| {
                resolve_shard(Region::US_EAST_VA, OrganizationId::new(i as i64), 16)
                    == resolve_shard(Region::US_WEST_OR, OrganizationId::new(i as i64), 16)
            })
            .count();
        // Expected collision rate for 16 shards and uncorrelated hashes is
        // ~1/16. Over 256 samples that's ~16 collisions. Allow generous
        // slack; fail only if decorrelation is clearly broken.
        assert!(collisions < 64, "too many cross-region collisions: {collisions}");
        let _ = (va, ore);
    }

    #[test]
    fn distribution_is_roughly_uniform_over_many_orgs() {
        let shard_count = 16;
        let mut histogram: HashMap<ShardIdx, usize> = HashMap::new();
        for org in 0..10_000 {
            let shard = resolve_shard(Region::US_EAST_VA, OrganizationId::new(org), shard_count);
            *histogram.entry(shard).or_insert(0) += 1;
        }
        // Each shard should hold ~625 orgs (10_000 / 16). With 1 %
        // relative slop seahash delivers in practice; allow generous
        // 25 % to make the test robust across rust releases.
        let expected = 10_000 / shard_count;
        for shard_idx in 0..shard_count as u64 {
            let count = *histogram.get(&ShardIdx(shard_idx)).unwrap_or(&0);
            let deviation = (count as f64 - expected as f64).abs() / expected as f64;
            assert!(
                deviation < 0.25,
                "shard {shard_idx} got {count} orgs (expected ~{expected}); distribution skewed"
            );
        }
    }

    #[test]
    fn all_shards_iterates_full_range() {
        let router = ShardRouter::new(8);
        let shards: Vec<_> = router.all_shards().collect();
        assert_eq!(shards.len(), 8);
        assert_eq!(shards.first().copied(), Some(ShardIdx(0)));
        assert_eq!(shards.last().copied(), Some(ShardIdx(7)));
    }

    #[test]
    fn router_clamps_zero_to_one() {
        // Bootstrap validation rejects 0, but defense-in-depth: the
        // router never returns a divide-by-zero-prone state.
        let router = ShardRouter::new(0);
        assert_eq!(router.shards_per_region(), 1);
        assert_eq!(router.resolve(Region::GLOBAL, OrganizationId::new(1)), ShardIdx(0));
    }
}
