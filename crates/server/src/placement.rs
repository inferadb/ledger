//! Event-driven data region membership controller (TiKV-style Placement Driver).
//!
//! Replaces the four timer-based DR background tasks (checker, scheduler,
//! reporter, drain monitor) with a single event-driven loop. Receives signals
//! from the GLOBAL apply worker after every committed batch and immediately
//! reconciles desired vs actual membership for all data regions where this
//! node is the Raft leader.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_raft::{
    RaftManager,
    raft_manager::SystemStateReader,
    types::{NodeStatus, RaftPayload},
};

use crate::dr_scheduler::{
    LearnerFirstSeen, check_dr_health, desired_dr_voters, execute_operator, schedule_dr_growth,
};

/// Event-driven controller for data region membership.
///
/// Receives signals from the GLOBAL apply worker whenever committed entries
/// may affect desired DR membership. On each signal, reconciles all local
/// data regions where this node is leader.
pub struct PlacementController {
    manager: Arc<RaftManager>,
    learner_first_seen: LearnerFirstSeen,
    was_global_leader: bool,
}

impl PlacementController {
    /// Creates a new controller for the given Raft manager.
    pub fn new(manager: Arc<RaftManager>) -> Self {
        Self { manager, learner_first_seen: HashMap::new(), was_global_leader: false }
    }

    /// Main event loop. Blocks until the channel closes (shutdown).
    pub async fn run(mut self, mut rx: tokio::sync::mpsc::UnboundedReceiver<()>) {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut promotion_interval = tokio::time::interval(Duration::from_millis(500));
        promotion_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut has_pending_learners = false;

        loop {
            tokio::select! {
                event = rx.recv() => {
                    let Some(()) = event else { break };
                    while rx.try_recv().is_ok() {}
                    has_pending_learners = self.reconcile().await;
                },
                _ = promotion_interval.tick(), if has_pending_learners => {
                    has_pending_learners = self.reconcile().await;
                },
            }
        }
        tracing::info!("PlacementController shutting down");
    }

    /// Reconciles desired vs actual membership for all data regions where
    /// this node is the Raft leader. Returns true if any region has pending
    /// learners that need promotion checks.
    async fn reconcile(&mut self) -> bool {
        reconcile_transport_channels(&self.manager).await;

        let Some(reader) = self.manager.system_state_reader() else { return false };
        let Ok(global) = self.manager.system_region() else { return false };
        let global_state = global.handle().shard_state();
        let global_voters: Vec<u64> = global_state.voters.iter().map(|n| n.0).collect();
        let local_node_id = self.manager.config().node_id;

        let is_global_leader = global.handle().is_leader();
        if is_global_leader && !self.was_global_leader {
            tracing::info!("PlacementController: GLOBAL leadership acquired, full reconciliation");
        }
        self.was_global_leader = is_global_leader;

        let mut any_learners = false;

        for region in self.manager.list_regions() {
            if region == inferadb_ledger_types::Region::GLOBAL {
                continue;
            }
            let Ok(group) = self.manager.get_region_group(region) else { continue };
            if !group.handle().is_leader() {
                continue;
            }

            let desired = desired_dr_voters(&reader, &global_voters);

            let mut region_learners: HashMap<u64, Instant> = self
                .learner_first_seen
                .iter()
                .filter_map(|(&(r, node), &t)| if r == region { Some((node, t)) } else { None })
                .collect();

            let mut ops = check_dr_health(
                &group,
                &desired,
                &reader,
                local_node_id,
                region,
                1,
                &mut region_learners,
            );

            self.learner_first_seen.retain(|&(r, _), _| r != region);
            for (node, t) in region_learners {
                self.learner_first_seen.insert((region, node), t);
            }

            ops.extend(schedule_dr_growth(&group, &desired, region));
            ops.sort_by_key(|op| op.priority);

            if let Some(op) = ops.first() {
                execute_operator(&group, op, &reader, &self.manager).await;
            }

            let state = group.handle().shard_state();
            if !state.learners.is_empty() {
                any_learners = true;
            }
        }

        if is_global_leader {
            self.check_drain(&reader, &global).await;
        }

        any_learners
    }

    /// Checks decommissioning/dead nodes for complete DR drain, removing
    /// fully-drained nodes from GLOBAL.
    async fn check_drain(
        &self,
        reader: &SystemStateReader,
        global: &inferadb_ledger_raft::OrganizationGroup,
    ) {
        let statuses = reader.all_node_statuses();
        for &(node_id, status) in &statuses {
            if status != NodeStatus::Decommissioning && status != NodeStatus::Dead {
                continue;
            }

            let target = inferadb_ledger_consensus::types::NodeId(node_id);
            let mut has_replicas = false;

            for region in self.manager.list_regions() {
                if region == inferadb_ledger_types::Region::GLOBAL {
                    continue;
                }
                if let Ok(group) = self.manager.get_region_group(region)
                    && group.handle().is_leader()
                {
                    let s = group.handle().shard_state();
                    if s.voters.contains(&target) || s.learners.contains(&target) {
                        has_replicas = true;
                        break;
                    }
                }
            }

            if !has_replicas {
                let memberships = reader.region_memberships();
                has_replicas = memberships.iter().any(|(_, members)| members.contains(&node_id));
            }

            if !has_replicas {
                tracing::info!(node_id, ?status, "Drain complete — removing from GLOBAL");
                match global.handle().remove_node(node_id).await {
                    Ok(()) => {
                        let set_removed = inferadb_ledger_raft::types::LedgerRequest::System(
                            inferadb_ledger_raft::types::SystemRequest::SetNodeStatus {
                                node_id,
                                status: NodeStatus::Removed,
                            },
                        );
                        let _ = global
                            .handle()
                            .propose_and_wait(
                                RaftPayload::system(set_removed),
                                Duration::from_secs(5),
                            )
                            .await;
                    },
                    Err(e) => {
                        let msg = e.to_string();
                        if !msg.contains("no-op") {
                            tracing::debug!(
                                node_id,
                                error = %e,
                                "Drain monitor: GLOBAL remove_node deferred"
                            );
                        }
                    },
                }
            }
        }
    }
}

/// Ensures all regions' consensus transports have channels to known peers.
///
/// Iterates all peer addresses and registers transport channels for any peers
/// that are missing from any region's transport. Critical for followers that
/// only have a channel to the leader — without channels to other voters,
/// leader transfer elections fail (VoteRequest can't reach peers).
pub async fn reconcile_transport_channels(manager: &RaftManager) {
    let peer_addrs = manager.peer_addresses().iter_peers();
    let regions = manager.list_regions();

    for region in regions {
        let Ok(group) = manager.get_region_group(region) else { continue };
        let Some(transport) = group.consensus_transport() else { continue };
        let known_peers = transport.peers();
        let local_node = group.handle().node_id();

        for (node_id, addr) in &peer_addrs {
            if *node_id == local_node || known_peers.contains(node_id) {
                continue;
            }
            if let Err(e) = transport.set_peer_via_registry(*node_id, addr).await {
                tracing::warn!(
                    node_id,
                    region = %region.as_str(),
                    error = %e,
                    "Failed to register transport channel for peer"
                );
            }
        }
    }
}
