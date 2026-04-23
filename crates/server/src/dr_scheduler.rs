//! Data region membership operator library.
//!
//! Pure functions for computing and executing DR membership operators.
//! The `PlacementController` in `placement.rs` calls these functions
//! from its event-driven reconciliation loop.

use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, Instant},
};

use inferadb_ledger_raft::{
    InnerGroup, RaftManager, raft_manager::SystemStateReader, types::NodeStatus,
};

/// Priority for DR membership operators.
/// Lower numeric value = higher priority.
///
/// The spec defines a `TransferLeader = 1` variant at the same numeric value as
/// `RepairNormal`. Rust does not allow two enum variants with duplicate discriminants,
/// so `TransferLeader` operators reuse `RepairNormal` (value 1), which matches the
/// spec's intent that self-removal leadership transfer has the same urgency as
/// decommissioning voter removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OperatorPriority {
    /// Dead node removal — immediate safety concern.
    RepairCritical = 0,
    /// Decommissioning voter removal or self-removal leadership transfer.
    RepairNormal = 1,
    /// Stalled learner removal.
    RepairLow = 2,
    /// Learner promotion.
    Promote = 3,
    /// New learner addition.
    Add = 4,
}

/// An action to take on a data region's membership.
#[derive(Debug, Clone)]
pub enum OperatorAction {
    /// Remove a voting member from the region.
    RemoveVoter {
        /// Node to remove.
        node_id: u64,
    },
    /// Add a node as a non-voting learner.
    AddLearner {
        /// Node to add.
        node_id: u64,
    },
    /// Promote an existing learner to voter.
    PromoteLearner {
        /// Node to promote.
        node_id: u64,
    },
    /// Remove a stalled learner.
    RemoveLearner {
        /// Node to remove.
        node_id: u64,
    },
    /// Transfer leadership before self-removal.
    TransferLeader {
        /// Target node to receive leadership.
        to: u64,
    },
}

/// Lag threshold for promoting a learner to voter. If the learner's
/// replication lag exceeds this number of entries, promotion is deferred.
const PROMOTION_LAG_THRESHOLD: u64 = 100;

/// Duration after which a learner that hasn't been promoted is considered
/// stalled and removed so the scheduler can retry with a different target.
const LEARNER_STALL_TIMEOUT: Duration = Duration::from_secs(300);

/// A scheduled membership operation.
#[derive(Debug, Clone)]
pub struct Operator {
    /// Target region for this operation.
    pub region: inferadb_ledger_types::Region,
    /// The membership action to perform.
    pub action: OperatorAction,
    /// Execution priority (lower = higher priority).
    pub priority: OperatorPriority,
    /// If set, the operator is stale-guarded: it will only execute if the
    /// shard's current `conf_epoch` matches this value.
    pub expected_conf_epoch: Option<u64>,
}

/// Computes the desired voter set for a data region based on GLOBAL state.
///
/// Only Active nodes are included. Decommissioning and Dead nodes are excluded.
pub fn desired_dr_voters(reader: &SystemStateReader, global_voters: &[u64]) -> BTreeSet<u64> {
    global_voters
        .iter()
        .filter(|&&node_id| {
            let status = reader.node_status(node_id);
            status == NodeStatus::Active
        })
        .copied()
        .collect()
}

/// Checker: detects problems in DR membership (reactive repair).
///
/// Generates operators for: dead voter removal, decommissioning voter removal,
/// self-removal via leadership transfer, and stalled learner removal.
///
/// `learner_first_seen` tracks when each learner was first observed. Learners
/// present for longer than the learner stall timeout (5 min) without promotion generate
/// a `RemoveLearner` operator so the scheduler can retry with a different target.
pub fn check_dr_health(
    group: &InnerGroup,
    desired: &BTreeSet<u64>,
    reader: &SystemStateReader,
    local_node_id: u64,
    region: inferadb_ledger_types::Region,
    min_voters: usize,
    learner_first_seen: &mut HashMap<u64, Instant>,
) -> Vec<Operator> {
    let state = group.handle().shard_state();
    let current: BTreeSet<u64> = state.voters.iter().map(|n| n.0).collect();
    let conf_epoch = state.conf_epoch;
    let mut ops = Vec::new();

    for &voter_id in &current {
        if desired.contains(&voter_id) {
            continue; // Healthy — should be here.
        }

        let status = reader.node_status(voter_id);
        let priority = match status {
            NodeStatus::Dead => OperatorPriority::RepairCritical,
            NodeStatus::Decommissioning => OperatorPriority::RepairNormal,
            _ => continue, // Active but not in desired set = transient; skip.
        };

        // Don't remove Decommissioning voters below min_voters.
        // Dead voters are always removable.
        if priority == OperatorPriority::RepairNormal && current.len() <= min_voters {
            continue;
        }

        // Self-removal: transfer leadership first.
        // TransferLeader uses RepairNormal priority (= 1), matching the spec's
        // intent that self-removal leadership transfer has the same urgency as
        // decommissioning voter removal.
        if voter_id == local_node_id
            && group.handle().is_leader()
            && let Some(&target) = desired.iter().find(|&&n| n != voter_id)
        {
            ops.push(Operator {
                region,
                action: OperatorAction::TransferLeader { to: target },
                priority: OperatorPriority::RepairNormal,
                expected_conf_epoch: Some(conf_epoch),
            });
            continue;
        }

        ops.push(Operator {
            region,
            action: OperatorAction::RemoveVoter { node_id: voter_id },
            priority,
            expected_conf_epoch: Some(conf_epoch),
        });
    }

    // Track learner first-seen times and detect stalls.
    let now = Instant::now();
    let current_learners: BTreeSet<u64> = state.learners.iter().map(|n| n.0).collect();

    // Remove tracking for nodes no longer learners.
    learner_first_seen.retain(|id, _| current_learners.contains(id));

    // Add tracking for new learners.
    for &learner_id in &current_learners {
        learner_first_seen.entry(learner_id).or_insert(now);
    }

    // Generate RemoveLearner for stalled learners.
    for (&learner_id, &first_seen) in learner_first_seen.iter() {
        if now.duration_since(first_seen) > LEARNER_STALL_TIMEOUT {
            ops.push(Operator {
                region,
                action: OperatorAction::RemoveLearner { node_id: learner_id },
                priority: OperatorPriority::RepairLow,
                expected_conf_epoch: Some(conf_epoch),
            });
        }
    }

    ops
}

/// Scheduler: detects growth opportunities (proactive).
///
/// Generates operators for: learner addition, learner promotion.
pub fn schedule_dr_growth(
    group: &InnerGroup,
    desired: &BTreeSet<u64>,
    region: inferadb_ledger_types::Region,
) -> Vec<Operator> {
    let state = group.handle().shard_state();
    let conf_epoch = state.conf_epoch;
    let current: BTreeSet<u64> = state.voters.iter().map(|n| n.0).collect();
    let current_and_learners: BTreeSet<u64> =
        current.union(&state.learners.iter().map(|n| n.0).collect()).copied().collect();
    let mut ops = Vec::new();

    // Promote learners in the desired voter set (catch-up verified in execute_operator).
    for learner in &state.learners {
        if desired.contains(&learner.0) {
            ops.push(Operator {
                region,
                action: OperatorAction::PromoteLearner { node_id: learner.0 },
                priority: OperatorPriority::Promote,
                expected_conf_epoch: Some(conf_epoch),
            });
            return ops; // One at a time.
        }
    }

    // Add missing voters as learners (one at a time).
    if let Some(&new_node) = desired.difference(&current_and_learners).next() {
        ops.push(Operator {
            region,
            action: OperatorAction::AddLearner { node_id: new_node },
            priority: OperatorPriority::Add,
            expected_conf_epoch: Some(conf_epoch),
        });
    }

    ops
}

/// Executes a single operator against a data region.
pub async fn execute_operator(
    group: &InnerGroup,
    op: &Operator,
    reader: &SystemStateReader,
    manager: &RaftManager,
) {
    // Guard: skip if another membership change is already in-flight.
    if group.handle().has_pending_membership() {
        tracing::debug!(
            region = op.region.as_str(),
            "Skipping operator — membership change already in-flight"
        );
        return;
    }

    // Guard: skip if the conf_epoch has changed since the operator was created.
    if let Some(expected) = op.expected_conf_epoch {
        let current = group.handle().shard_state().conf_epoch;
        if current != expected {
            tracing::debug!(
                region = op.region.as_str(),
                expected,
                current,
                "Skipping operator — conf_epoch mismatch (stale operator)"
            );
            return;
        }
    }

    // Re-verify NodeStatus before removals (guards against resurrection race).
    if let OperatorAction::RemoveVoter { node_id } = &op.action {
        let status = reader.node_status(*node_id);
        if status == NodeStatus::Active {
            tracing::debug!(node_id, "Skipping removal — node is Active (reactivated?)");
            return;
        }
    }

    let mut membership_changed = false;

    match &op.action {
        OperatorAction::RemoveVoter { node_id } => {
            match tokio::time::timeout(Duration::from_secs(3), group.handle().remove_node(*node_id))
                .await
            {
                Ok(Ok(())) => {
                    tracing::info!(
                        region = op.region.as_str(),
                        node_id,
                        priority = ?op.priority,
                        "DR scheduler: removed voter"
                    );
                    membership_changed = true;
                },
                Ok(Err(e)) if e.to_string().contains("no-op") => {},
                Ok(Err(e)) => {
                    tracing::warn!(
                        region = op.region.as_str(),
                        node_id,
                        error = %e,
                        "DR scheduler: failed to remove voter"
                    );
                },
                Err(_) => {
                    tracing::warn!(
                        region = op.region.as_str(),
                        node_id,
                        "DR scheduler: remove voter timed out"
                    );
                },
            }
        },
        OperatorAction::AddLearner { node_id } => {
            let Some(addr) = manager.peer_addresses().get(*node_id) else {
                tracing::debug!(
                    node_id,
                    region = op.region.as_str(),
                    "Skipping AddLearner — peer address not yet registered"
                );
                return;
            };
            if let Some(transport) = group.consensus_transport()
                && let Err(e) = transport.set_peer_via_registry(*node_id, &addr).await
            {
                tracing::warn!(
                    node_id = *node_id,
                    addr = %addr,
                    error = %e,
                    "DR scheduler: failed to register learner transport via registry"
                );
            }
            match tokio::time::timeout(
                Duration::from_secs(3),
                group.handle().add_learner(*node_id, false),
            )
            .await
            {
                Ok(Ok(())) => {
                    tracing::info!(
                        region = op.region.as_str(),
                        node_id,
                        "DR scheduler: added learner"
                    );
                    membership_changed = true;
                },
                Ok(Err(e)) => {
                    tracing::warn!(
                        region = op.region.as_str(),
                        node_id,
                        error = %e,
                        "DR scheduler: failed to add learner"
                    );
                },
                Err(_) => {},
            }
        },
        OperatorAction::PromoteLearner { node_id } => {
            // Verify learner has caught up before promoting.
            if let Some(match_idx) = group.handle().peer_match_index(*node_id).await {
                let log_len = group.handle().shard_state().last_log_index;
                let lag = log_len.saturating_sub(match_idx);
                if lag >= PROMOTION_LAG_THRESHOLD {
                    tracing::debug!(
                        region = op.region.as_str(),
                        node_id,
                        match_idx,
                        log_len,
                        lag,
                        "DR scheduler: learner not caught up, skipping promotion"
                    );
                    return;
                }
            }
            match tokio::time::timeout(
                Duration::from_secs(3),
                group.handle().promote_voter(*node_id),
            )
            .await
            {
                Ok(Ok(())) => {
                    tracing::info!(
                        region = op.region.as_str(),
                        node_id,
                        "DR scheduler: promoted learner to voter"
                    );
                    membership_changed = true;
                },
                Ok(Err(e)) => {
                    tracing::warn!(
                        region = op.region.as_str(),
                        node_id,
                        error = %e,
                        "DR scheduler: failed to promote learner"
                    );
                },
                Err(_) => {},
            }
        },
        OperatorAction::RemoveLearner { node_id } => {
            match tokio::time::timeout(Duration::from_secs(3), group.handle().remove_node(*node_id))
                .await
            {
                Ok(Ok(())) => {
                    tracing::info!(
                        region = op.region.as_str(),
                        node_id,
                        "DR scheduler: removed stalled learner"
                    );
                    membership_changed = true;
                },
                Ok(Err(e)) if e.to_string().contains("no-op") => {},
                Ok(Err(e)) => {
                    tracing::warn!(
                        region = op.region.as_str(),
                        node_id,
                        error = %e,
                        "DR scheduler: failed to remove stalled learner"
                    );
                },
                Err(_) => {},
            }
        },
        OperatorAction::TransferLeader { to } => {
            tracing::info!(
                region = op.region.as_str(),
                transfer_to = to,
                "DR scheduler: transferring leadership before self-removal"
            );
            let _ =
                tokio::time::timeout(Duration::from_secs(3), group.handle().transfer_leader(*to))
                    .await;
        },
    }

    if membership_changed {
        // Report the updated membership to GLOBAL.
        report_membership_to_global(manager, group, op.region).await;

        // After removing a voter, check if it's now fully drained from ALL
        // local DRs. If so AND we are the GLOBAL leader, trigger immediate
        // GLOBAL removal. If we're not the GLOBAL leader, the drain monitor
        // (which runs on the GLOBAL leader) will handle it via reports.
        if let OperatorAction::RemoveVoter { node_id } = &op.action
            && let Ok(global) = manager.system_region()
            && global.handle().is_leader()
        {
            let target = inferadb_ledger_consensus::types::NodeId(*node_id);
            let mut still_in_any_dr = false;
            for region in manager.list_regions() {
                if region == inferadb_ledger_types::Region::GLOBAL {
                    continue;
                }
                if let Ok(g) = manager.get_region_group(region) {
                    let s = g.handle().shard_state();
                    if s.voters.contains(&target) || s.learners.contains(&target) {
                        still_in_any_dr = true;
                        break;
                    }
                }
            }
            if !still_in_any_dr {
                tracing::info!(node_id, "DR scheduler: node fully drained — removing from GLOBAL");
                let _ = global.handle().remove_node(*node_id).await;
                let _ = global
                    .handle()
                    .propose_and_wait(
                        inferadb_ledger_raft::types::RaftPayload::system(
                            inferadb_ledger_raft::types::SystemRequest::SetNodeStatus {
                                node_id: *node_id,
                                status: inferadb_ledger_raft::types::NodeStatus::Removed,
                            },
                        ),
                        Duration::from_secs(5),
                    )
                    .await;
            }
        }
    }
}

/// Reports a data region's current membership to GLOBAL state.
///
/// Best-effort: proposes a `RegionMembershipReport` through GLOBAL Raft.
/// Only succeeds if this node is the GLOBAL leader. On followers, the
/// proposal fails silently (the drain monitor's fallback in `leave_cluster`
/// handles the case where reports can't propagate).
async fn report_membership_to_global(
    manager: &RaftManager,
    group: &InnerGroup,
    region: inferadb_ledger_types::Region,
) {
    let Ok(global) = manager.system_region() else { return };
    let state = group.handle().shard_state();
    let _ = global
        .handle()
        .propose_and_wait(
            inferadb_ledger_raft::types::RaftPayload::system(
                inferadb_ledger_raft::types::SystemRequest::RegionMembershipReport {
                    region,
                    voters: state.voters.iter().map(|n| n.0).collect(),
                    learners: state.learners.iter().map(|n| n.0).collect(),
                    conf_epoch: state.conf_epoch,
                },
            ),
            Duration::from_secs(5),
        )
        .await;
}

/// Per-region learner first-seen tracking for stall detection.
pub type LearnerFirstSeen = HashMap<(inferadb_ledger_types::Region, u64), Instant>;
