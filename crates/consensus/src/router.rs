//! Organization-to-shard routing table.
//!
//! Maps organization IDs to the shard responsible for their data,
//! supporting split and merge operations for shard rebalancing.

use std::collections::HashMap;

use crate::types::ConsensusStateId;

/// Routes organization IDs to their owning shard.
pub struct Router {
    routes: HashMap<u64, ConsensusStateId>,
}

impl Router {
    /// Creates an empty routing table.
    pub fn new() -> Self {
        Self { routes: HashMap::new() }
    }

    /// Assigns an organization to a shard.
    pub fn set(&mut self, org_id: u64, shard: ConsensusStateId) {
        self.routes.insert(org_id, shard);
    }

    /// Removes the route for an organization.
    pub fn remove(&mut self, org_id: u64) {
        self.routes.remove(&org_id);
    }

    /// Resolves which shard owns the given organization.
    pub fn resolve(&self, org_id: u64) -> Option<ConsensusStateId> {
        self.routes.get(&org_id).copied()
    }

    /// Splits organizations by moving all with `id >= split_key` to `new_shard`.
    ///
    /// Returns the IDs that were moved.
    pub fn split(&mut self, split_key: u64, new_shard: ConsensusStateId) -> Vec<u64> {
        let moved: Vec<u64> = self.routes.keys().copied().filter(|&id| id >= split_key).collect();
        for &id in &moved {
            self.routes.insert(id, new_shard);
        }
        moved
    }

    /// Merges all organizations from `source_shard` into `target_shard`.
    pub fn merge(&mut self, source_shard: ConsensusStateId, target_shard: ConsensusStateId) {
        for shard in self.routes.values_mut() {
            if *shard == source_shard {
                *shard = target_shard;
            }
        }
    }

    /// Returns the number of organization routes.
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Returns `true` if there are no routes.
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn set_and_resolve() {
        let mut router = Router::new();
        router.set(1, ConsensusStateId(10));
        router.set(2, ConsensusStateId(20));
        assert_eq!(router.resolve(1), Some(ConsensusStateId(10)));
        assert_eq!(router.resolve(2), Some(ConsensusStateId(20)));
    }

    #[test]
    fn set_overwrites_existing_route() {
        let mut router = Router::new();
        router.set(1, ConsensusStateId(10));
        router.set(1, ConsensusStateId(20));
        assert_eq!(router.resolve(1), Some(ConsensusStateId(20)));
        assert_eq!(router.len(), 1);
    }

    #[test]
    fn resolve_returns_none_for_unknown_org() {
        let router = Router::new();
        assert_eq!(router.resolve(42), None);
    }

    #[test]
    fn resolve_returns_none_on_empty_table() {
        let router = Router::new();
        assert_eq!(router.resolve(0), None);
        assert_eq!(router.resolve(u64::MAX), None);
    }

    #[test]
    fn remove_clears_route() {
        let mut router = Router::new();
        router.set(1, ConsensusStateId(10));
        router.remove(1);
        assert_eq!(router.resolve(1), None);
        assert_eq!(router.len(), 0);
    }

    #[test]
    fn remove_nonexistent_org_is_noop() {
        let mut router = Router::new();
        router.set(1, ConsensusStateId(10));
        router.remove(999);
        assert_eq!(router.len(), 1);
        assert_eq!(router.resolve(1), Some(ConsensusStateId(10)));
    }

    #[test]
    fn split_moves_orgs_at_or_above_key() {
        let mut router = Router::new();
        router.set(1, ConsensusStateId(10));
        router.set(5, ConsensusStateId(10));
        router.set(10, ConsensusStateId(10));
        router.set(15, ConsensusStateId(10));

        let mut moved = router.split(10, ConsensusStateId(20));
        moved.sort_unstable();
        assert_eq!(moved, vec![10, 15]);

        assert_eq!(router.resolve(10), Some(ConsensusStateId(20)));
        assert_eq!(router.resolve(15), Some(ConsensusStateId(20)));
        assert_eq!(router.resolve(1), Some(ConsensusStateId(10)));
        assert_eq!(router.resolve(5), Some(ConsensusStateId(10)));
    }

    #[test]
    fn split_with_no_matching_orgs_returns_empty() {
        let mut router = Router::new();
        router.set(1, ConsensusStateId(10));
        router.set(2, ConsensusStateId(10));

        let moved = router.split(100, ConsensusStateId(20));
        assert!(moved.is_empty());
        // Original routes unchanged.
        assert_eq!(router.resolve(1), Some(ConsensusStateId(10)));
        assert_eq!(router.resolve(2), Some(ConsensusStateId(10)));
    }

    #[test]
    fn split_on_empty_table_returns_empty() {
        let mut router = Router::new();
        let moved = router.split(0, ConsensusStateId(20));
        assert!(moved.is_empty());
        assert!(router.is_empty());
    }

    #[test]
    fn split_moves_orgs_across_multiple_source_shards() {
        let mut router = Router::new();
        router.set(1, ConsensusStateId(10));
        router.set(5, ConsensusStateId(20));
        router.set(10, ConsensusStateId(10));
        router.set(15, ConsensusStateId(20));

        let mut moved = router.split(10, ConsensusStateId(30));
        moved.sort_unstable();
        // Both org 10 (was shard 10) and org 15 (was shard 20) move to shard 30.
        assert_eq!(moved, vec![10, 15]);
        assert_eq!(router.resolve(10), Some(ConsensusStateId(30)));
        assert_eq!(router.resolve(15), Some(ConsensusStateId(30)));
    }

    #[test]
    fn split_at_zero_moves_all_orgs() {
        let mut router = Router::new();
        router.set(0, ConsensusStateId(10));
        router.set(5, ConsensusStateId(10));
        router.set(u64::MAX, ConsensusStateId(10));

        let moved = router.split(0, ConsensusStateId(20));
        assert_eq!(moved.len(), 3);
        assert_eq!(router.resolve(0), Some(ConsensusStateId(20)));
        assert_eq!(router.resolve(5), Some(ConsensusStateId(20)));
        assert_eq!(router.resolve(u64::MAX), Some(ConsensusStateId(20)));
    }

    #[test]
    fn merge_reassigns_all_from_source_to_target() {
        let mut router = Router::new();
        router.set(1, ConsensusStateId(10));
        router.set(2, ConsensusStateId(10));
        router.set(3, ConsensusStateId(20));
        router.set(4, ConsensusStateId(20));

        router.merge(ConsensusStateId(20), ConsensusStateId(10));

        for org_id in 1..=4 {
            assert_eq!(router.resolve(org_id), Some(ConsensusStateId(10)));
        }
    }

    #[test]
    fn merge_nonexistent_source_is_noop() {
        let mut router = Router::new();
        router.set(1, ConsensusStateId(10));
        router.merge(ConsensusStateId(99), ConsensusStateId(10));
        assert_eq!(router.resolve(1), Some(ConsensusStateId(10)));
        assert_eq!(router.len(), 1);
    }

    #[test]
    fn merge_leaves_third_shard_untouched() {
        let mut router = Router::new();
        router.set(1, ConsensusStateId(10));
        router.set(2, ConsensusStateId(20));
        router.set(3, ConsensusStateId(30));

        router.merge(ConsensusStateId(20), ConsensusStateId(10));

        assert_eq!(router.resolve(1), Some(ConsensusStateId(10)));
        assert_eq!(router.resolve(2), Some(ConsensusStateId(10)));
        assert_eq!(router.resolve(3), Some(ConsensusStateId(30)));
    }

    #[test]
    fn len_and_is_empty() {
        let mut router = Router::new();
        assert!(router.is_empty());
        assert_eq!(router.len(), 0);

        router.set(1, ConsensusStateId(10));
        assert!(!router.is_empty());
        assert_eq!(router.len(), 1);

        router.set(2, ConsensusStateId(20));
        assert_eq!(router.len(), 2);

        router.remove(1);
        assert_eq!(router.len(), 1);
    }

    #[test]
    fn default_creates_empty_router() {
        let router = Router::default();
        assert!(router.is_empty());
    }
}
