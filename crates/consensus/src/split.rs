//! Split and merge configuration and phase tracking.
//!
//! Defines the thresholds that trigger shard splits/merges and the
//! state machine phases a shard transitions through during a split.

use snafu::Snafu;

use crate::types::ShardId;

/// Configuration controlling when shards split or merge.
pub struct SplitConfig {
    /// Whether automatic split/merge is enabled.
    pub enabled: bool,
    /// Maximum relationships per shard before triggering a split.
    pub max_shard_relationships: u64,
    /// Maximum relationships per tenant before isolating it.
    pub max_tenant_relationships: u64,
    /// Minimum relationships per shard before considering a merge.
    pub min_shard_relationships: u64,
}

impl Default for SplitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_shard_relationships: 10_000_000,
            max_tenant_relationships: 1_000_000,
            min_shard_relationships: 100_000,
        }
    }
}

/// A request to split a shard at the given key boundary.
pub struct SplitRequest {
    /// Organizations with ID >= this value move to the new shard.
    pub split_key: u64,
    /// The shard that will receive the migrated organizations.
    pub new_shard: ShardId,
}

/// A request to merge a shard back into its parent.
pub struct MergeRequest {
    /// The shard to drain and decommission.
    pub source_shard: ShardId,
}

/// Tracks the current phase of a shard split operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitPhase {
    /// No split in progress.
    None,
    /// Writes to the affected key range are frozen.
    Frozen {
        /// The shard being created.
        new_shard: ShardId,
    },
    /// Data is being migrated to the new shard.
    Migrating {
        /// The shard receiving migrated data.
        new_shard: ShardId,
    },
    /// Split is complete; both shards are serving traffic.
    Complete,
}

/// Drives the two-phase split lifecycle.
///
/// Phase 1 (Frozen): Routes updated, writes frozen for migrated orgs.
/// Phase 2 (Migrating): Background state migration in progress.
/// Complete: Migration finished, old shard data can be cleaned up.
pub struct SplitStateMachine {
    phase: SplitPhase,
    /// Organizations that were migrated (tracked for cleanup).
    migrated_orgs: Vec<u64>,
}

impl SplitStateMachine {
    /// Creates a new state machine in the initial `None` phase.
    pub fn new() -> Self {
        Self { phase: SplitPhase::None, migrated_orgs: Vec::new() }
    }

    /// Returns the current phase.
    pub fn phase(&self) -> SplitPhase {
        self.phase.clone()
    }

    /// Begins a split: transitions from `None` → `Frozen`.
    ///
    /// Returns the list of organization IDs that were migrated to the new shard.
    pub fn begin_split(
        &mut self,
        request: &SplitRequest,
        router: &mut crate::router::Router,
    ) -> Result<Vec<u64>, SplitError> {
        if self.phase != SplitPhase::None {
            return Err(SplitError::AlreadyInProgress);
        }
        let moved = router.split(request.split_key, request.new_shard);
        self.migrated_orgs = moved.clone();
        self.phase = SplitPhase::Frozen { new_shard: request.new_shard };
        Ok(moved)
    }

    /// Transitions from `Frozen` → `Migrating` (background migration started).
    pub fn begin_migration(&mut self) -> Result<(), SplitError> {
        match self.phase {
            SplitPhase::Frozen { new_shard } => {
                self.phase = SplitPhase::Migrating { new_shard };
                Ok(())
            },
            _ => Err(SplitError::InvalidTransition {
                from: format!("{:?}", self.phase),
                to: "Migrating".to_string(),
            }),
        }
    }

    /// Completes the split: transitions `Migrating` → `Complete`.
    pub fn complete(&mut self) -> Result<(), SplitError> {
        match self.phase {
            SplitPhase::Migrating { .. } => {
                self.phase = SplitPhase::Complete;
                Ok(())
            },
            _ => Err(SplitError::InvalidTransition {
                from: format!("{:?}", self.phase),
                to: "Complete".to_string(),
            }),
        }
    }

    /// Resets after completion (ready for another split).
    pub fn reset(&mut self) {
        self.phase = SplitPhase::None;
        self.migrated_orgs.clear();
    }

    /// Returns the migrated organization IDs.
    pub fn migrated_orgs(&self) -> &[u64] {
        &self.migrated_orgs
    }
}

impl Default for SplitStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

/// Split operation errors.
#[derive(Debug, Snafu)]
pub enum SplitError {
    /// A split is already in progress.
    #[snafu(display("Split already in progress"))]
    AlreadyInProgress,
    /// The requested phase transition is not valid from the current phase.
    #[snafu(display("Invalid phase transition from {from} to {to}"))]
    InvalidTransition {
        /// The current phase.
        from: String,
        /// The requested target phase.
        to: String,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::router::Router;

    fn make_router_with_orgs(shard: ShardId, org_ids: &[u64]) -> Router {
        let mut router = Router::new();
        for &id in org_ids {
            router.set(id, shard);
        }
        router
    }

    fn make_request(split_key: u64, new_shard: ShardId) -> SplitRequest {
        SplitRequest { split_key, new_shard }
    }

    // --- SplitConfig ---

    #[test]
    fn default_config_disabled_with_expected_thresholds() {
        let config = SplitConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_shard_relationships, 10_000_000);
        assert_eq!(config.max_tenant_relationships, 1_000_000);
        assert_eq!(config.min_shard_relationships, 100_000);
    }

    // --- SplitStateMachine lifecycle ---

    #[test]
    fn new_state_machine_starts_in_none_with_no_migrated_orgs() {
        let sm = SplitStateMachine::new();
        assert_eq!(sm.phase(), SplitPhase::None);
        assert!(sm.migrated_orgs().is_empty());
    }

    #[test]
    fn full_lifecycle_none_frozen_migrating_complete_reset() {
        let mut router = make_router_with_orgs(ShardId(10), &[1, 5, 10, 15]);
        let mut sm = SplitStateMachine::new();

        // None -> Frozen
        let request = make_request(10, ShardId(20));
        let mut moved = sm.begin_split(&request, &mut router).expect("begin_split");
        moved.sort_unstable();
        assert_eq!(moved, vec![10, 15]);
        assert_eq!(sm.phase(), SplitPhase::Frozen { new_shard: ShardId(20) });

        // Migrated orgs tracked through lifecycle.
        let mut tracked = sm.migrated_orgs().to_vec();
        tracked.sort_unstable();
        assert_eq!(tracked, vec![10, 15]);

        // Frozen -> Migrating
        sm.begin_migration().expect("begin_migration");
        assert_eq!(sm.phase(), SplitPhase::Migrating { new_shard: ShardId(20) });

        // Migrating -> Complete
        sm.complete().expect("complete");
        assert_eq!(sm.phase(), SplitPhase::Complete);

        // Reset -> None
        sm.reset();
        assert_eq!(sm.phase(), SplitPhase::None);
        assert!(sm.migrated_orgs().is_empty());
    }

    // --- Invalid transition matrix (table-driven) ---

    #[test]
    fn invalid_transitions_return_error() {
        // Each case: (setup phase, attempted operation name)
        // We test every invalid transition rather than just a couple.

        // begin_migration from None
        {
            let mut sm = SplitStateMachine::new();
            let err = sm.begin_migration().expect_err("None -> Migrating");
            assert!(matches!(err, SplitError::InvalidTransition { .. }));
        }

        // begin_migration from Migrating
        {
            let mut router = make_router_with_orgs(ShardId(10), &[1]);
            let mut sm = SplitStateMachine::new();
            sm.begin_split(&make_request(0, ShardId(20)), &mut router).unwrap();
            sm.begin_migration().unwrap();
            let err = sm.begin_migration().expect_err("Migrating -> Migrating");
            assert!(matches!(err, SplitError::InvalidTransition { .. }));
        }

        // begin_migration from Complete
        {
            let mut router = make_router_with_orgs(ShardId(10), &[1]);
            let mut sm = SplitStateMachine::new();
            sm.begin_split(&make_request(0, ShardId(20)), &mut router).unwrap();
            sm.begin_migration().unwrap();
            sm.complete().unwrap();
            let err = sm.begin_migration().expect_err("Complete -> Migrating");
            assert!(matches!(err, SplitError::InvalidTransition { .. }));
        }

        // complete from None
        {
            let mut sm = SplitStateMachine::new();
            let err = sm.complete().expect_err("None -> Complete");
            assert!(matches!(err, SplitError::InvalidTransition { .. }));
        }

        // complete from Frozen (must go through Migrating)
        {
            let mut router = make_router_with_orgs(ShardId(10), &[1]);
            let mut sm = SplitStateMachine::new();
            sm.begin_split(&make_request(0, ShardId(20)), &mut router).unwrap();
            let err = sm.complete().expect_err("Frozen -> Complete");
            assert!(matches!(err, SplitError::InvalidTransition { .. }));
        }

        // complete from Complete
        {
            let mut router = make_router_with_orgs(ShardId(10), &[1]);
            let mut sm = SplitStateMachine::new();
            sm.begin_split(&make_request(0, ShardId(20)), &mut router).unwrap();
            sm.begin_migration().unwrap();
            sm.complete().unwrap();
            let err = sm.complete().expect_err("Complete -> Complete");
            assert!(matches!(err, SplitError::InvalidTransition { .. }));
        }
    }

    // --- begin_split guards ---

    #[test]
    fn begin_split_rejects_when_already_frozen() {
        let mut router = make_router_with_orgs(ShardId(10), &[5]);
        let mut sm = SplitStateMachine::new();
        sm.begin_split(&make_request(1, ShardId(20)), &mut router).unwrap();

        let err = sm
            .begin_split(&make_request(1, ShardId(30)), &mut router)
            .expect_err("second begin_split");
        assert!(matches!(err, SplitError::AlreadyInProgress));
    }

    #[test]
    fn begin_split_rejects_when_migrating() {
        let mut router = make_router_with_orgs(ShardId(10), &[5]);
        let mut sm = SplitStateMachine::new();
        sm.begin_split(&make_request(1, ShardId(20)), &mut router).unwrap();
        sm.begin_migration().unwrap();

        let err = sm
            .begin_split(&make_request(1, ShardId(30)), &mut router)
            .expect_err("begin_split during Migrating");
        assert!(matches!(err, SplitError::AlreadyInProgress));
    }

    #[test]
    fn begin_split_rejects_when_complete() {
        let mut router = make_router_with_orgs(ShardId(10), &[5]);
        let mut sm = SplitStateMachine::new();
        sm.begin_split(&make_request(1, ShardId(20)), &mut router).unwrap();
        sm.begin_migration().unwrap();
        sm.complete().unwrap();

        let err = sm
            .begin_split(&make_request(1, ShardId(30)), &mut router)
            .expect_err("begin_split during Complete");
        assert!(matches!(err, SplitError::AlreadyInProgress));
    }

    // --- Edge cases ---

    #[test]
    fn begin_split_with_empty_router_moves_nothing() {
        let mut router = Router::new();
        let mut sm = SplitStateMachine::new();
        let moved = sm.begin_split(&make_request(0, ShardId(20)), &mut router).unwrap();
        assert!(moved.is_empty());
        assert!(sm.migrated_orgs().is_empty());
        assert_eq!(sm.phase(), SplitPhase::Frozen { new_shard: ShardId(20) });
    }

    #[test]
    fn reset_from_frozen_clears_state() {
        let mut router = make_router_with_orgs(ShardId(10), &[1, 2]);
        let mut sm = SplitStateMachine::new();
        sm.begin_split(&make_request(1, ShardId(20)), &mut router).unwrap();

        sm.reset();
        assert_eq!(sm.phase(), SplitPhase::None);
        assert!(sm.migrated_orgs().is_empty());
    }

    #[test]
    fn default_creates_state_machine_in_none_phase() {
        let sm = SplitStateMachine::default();
        assert_eq!(sm.phase(), SplitPhase::None);
    }
}
