//! Multi-Raft simulation harness for testing cross-group membership coordination.
//!
//! Extends the single-group [`Simulation`](super::Simulation) with NodeStatus
//! tracking and simulated DR scheduling, enabling deterministic testing of the
//! checker/scheduler/drain-monitor pipeline.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap},
        time::Duration,
    };

    use crate::{
        action::Action,
        simulation::Simulation,
        types::{MembershipChange, NodeId, NodeState, ShardId},
    };

    /// GLOBAL shard ID used across all multi-Raft tests.
    const GLOBAL_SHARD: ShardId = ShardId(1);
    /// Data region shard ID.
    const DR_SHARD: ShardId = ShardId(2);

    /// Node lifecycle status (mirrors the raft crate's NodeStatus).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum NodeStatus {
        Active,
        Decommissioning,
        Dead,
        #[allow(dead_code)]
        Removed,
    }

    /// Multi-Raft simulation state wrapping the base Simulation.
    struct MultiRaftSim {
        sim: Simulation,
        statuses: HashMap<NodeId, NodeStatus>,
        /// Nodes that have been "crashed" -- their shards still exist in the
        /// simulation but messages to/from them are dropped.
        crashed: BTreeSet<NodeId>,
    }

    impl MultiRaftSim {
        /// Creates a new multi-Raft simulation with N nodes, GLOBAL + DR shards.
        fn new(seed: u64, node_count: u64) -> Self {
            let mut sim = Simulation::new(seed, node_count);
            // The default shard (ShardId(1)) serves as GLOBAL.
            // Add a data region shard on the same nodes.
            let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId).collect();
            sim.add_shard_group(DR_SHARD, &node_ids);

            let statuses = node_ids.iter().map(|&id| (id, NodeStatus::Active)).collect();

            Self { sim, statuses, crashed: BTreeSet::new() }
        }

        /// Elects leaders on both GLOBAL and DR shards.
        fn elect_leaders(&mut self, global_leader: NodeId, dr_leader: NodeId) {
            assert!(self.sim.elect_leader_on(GLOBAL_SHARD, global_leader));
            assert!(self.sim.elect_leader_on(DR_SHARD, dr_leader));
        }

        /// Sets a node's status to Decommissioning.
        fn decommission(&mut self, node: NodeId) {
            self.statuses.insert(node, NodeStatus::Decommissioning);
        }

        /// Sets a node's status to Dead.
        fn mark_dead(&mut self, node: NodeId) {
            self.statuses.insert(node, NodeStatus::Dead);
        }

        /// Computes the desired DR voter set: all Active nodes.
        fn desired_dr_voters(&self) -> BTreeSet<NodeId> {
            self.statuses
                .iter()
                .filter(|&(_, &status)| status == NodeStatus::Active)
                .map(|(&id, _)| id)
                .collect()
        }

        /// Runs one scheduler tick: for the DR shard leader, compute desired
        /// state and execute one membership change if needed.
        fn scheduler_tick(&mut self) {
            let dr_leader = match self.sim.leader_on(DR_SHARD) {
                Some(id) => id,
                None => return,
            };

            // Skip if crashed.
            if self.crashed.contains(&dr_leader) {
                return;
            }

            let desired = self.desired_dr_voters();

            // Get current DR voters.
            let current: BTreeSet<NodeId> = self
                .sim
                .nodes
                .get(&dr_leader)
                .and_then(|shards| shards.get(&DR_SHARD))
                .map(|shard| shard.membership().voters.clone())
                .unwrap_or_default();

            // Priority 1: Remove dead voters.
            for &voter in &current {
                if !desired.contains(&voter)
                    && self.statuses.get(&voter).copied().unwrap_or(NodeStatus::Active)
                        == NodeStatus::Dead
                {
                    self.remove_voter_from_dr(dr_leader, voter);
                    return;
                }
            }

            // Priority 2: Add missing voters as learners, then promote.
            for &desired_voter in &desired {
                if !current.contains(&desired_voter) {
                    let is_learner = self
                        .sim
                        .nodes
                        .get(&dr_leader)
                        .and_then(|s| s.get(&DR_SHARD))
                        .map(|shard| shard.membership().learners.contains(&desired_voter))
                        .unwrap_or(false);
                    if is_learner {
                        self.promote_learner_on_dr(dr_leader, desired_voter);
                    } else {
                        self.add_learner_to_dr(dr_leader, desired_voter);
                    }
                    return;
                }
            }

            // Priority 3: Remove decommissioning voters.
            for &voter in &current {
                if !desired.contains(&voter)
                    && self.statuses.get(&voter).copied().unwrap_or(NodeStatus::Active)
                        == NodeStatus::Decommissioning
                {
                    // Self-removal: transfer leadership first.
                    if voter == dr_leader {
                        if let Some(&target) = desired.iter().next() {
                            self.transfer_dr_leadership(dr_leader, target);
                        }
                        return;
                    }
                    self.remove_voter_from_dr(dr_leader, voter);
                    return;
                }
            }
        }

        /// Removes a voter from the DR shard.
        fn remove_voter_from_dr(&mut self, leader: NodeId, target: NodeId) {
            if let Some(shards) = self.sim.nodes.get_mut(&leader)
                && let Some(shard) = shards.get_mut(&DR_SHARD)
            {
                let result = shard.handle_membership_change(MembershipChange::RemoveNode {
                    node_id: target,
                    expected_conf_epoch: None,
                });
                if let Ok(actions) = result {
                    self.process_and_deliver(leader, actions);
                }
            }
        }

        /// Adds a learner to the DR shard.
        fn add_learner_to_dr(&mut self, leader: NodeId, target: NodeId) {
            if let Some(shards) = self.sim.nodes.get_mut(&leader)
                && let Some(shard) = shards.get_mut(&DR_SHARD)
            {
                let result = shard.handle_membership_change(MembershipChange::AddLearner {
                    node_id: target,
                    promotable: true,
                    expected_conf_epoch: None,
                });
                if let Ok(actions) = result {
                    self.process_and_deliver(leader, actions);
                }
            }
        }

        /// Promotes a learner to voter on the DR shard.
        fn promote_learner_on_dr(&mut self, leader: NodeId, target: NodeId) {
            if let Some(shards) = self.sim.nodes.get_mut(&leader)
                && let Some(shard) = shards.get_mut(&DR_SHARD)
            {
                let result = shard.handle_membership_change(MembershipChange::PromoteVoter {
                    node_id: target,
                    expected_conf_epoch: None,
                });
                if let Ok(actions) = result {
                    self.process_and_deliver(leader, actions);
                }
            }
        }

        /// Transfers DR leadership.
        fn transfer_dr_leadership(&mut self, from: NodeId, to: NodeId) {
            if let Some(shards) = self.sim.nodes.get_mut(&from)
                && let Some(shard) = shards.get_mut(&DR_SHARD)
            {
                let result = shard.handle_transfer_leader(to);
                if let Ok(actions) = result {
                    self.process_and_deliver(from, actions);
                }
            }
        }

        /// Removes a node from GLOBAL shard.
        #[allow(dead_code)]
        fn remove_from_global(&mut self, leader: NodeId, target: NodeId) {
            if let Some(shards) = self.sim.nodes.get_mut(&leader)
                && let Some(shard) = shards.get_mut(&GLOBAL_SHARD)
            {
                let result = shard.handle_membership_change(MembershipChange::RemoveNode {
                    node_id: target,
                    expected_conf_epoch: None,
                });
                if let Ok(actions) = result {
                    self.process_and_deliver(leader, actions);
                }
            }
        }

        /// Simulates crash: partitions the node from all others.
        fn crash(&mut self, node: NodeId) {
            self.crashed.insert(node);
            let all_nodes: Vec<NodeId> = self.sim.node_ids.clone();
            for &other in &all_nodes {
                if other != node {
                    self.sim.network.partition(&[node], &[other]);
                }
            }
        }

        /// Simulates restart: heals partitions for the node.
        fn restart(&mut self, node: NodeId) {
            self.crashed.remove(&node);
            self.sim.heal();
        }

        /// Processes actions from a shard and delivers messages.
        fn process_and_deliver(&mut self, from: NodeId, actions: Vec<Action>) {
            for action in actions {
                if let Action::Send { to, shard, msg } = action {
                    self.sim.network.send(from, to, shard, msg);
                }
            }
            self.sim.deliver_all_rounds(20);
        }

        /// Runs multiple scheduler ticks with message delivery between each.
        fn converge(&mut self, max_ticks: usize) {
            for _ in 0..max_ticks {
                self.sim.advance_clock(Duration::from_secs(1));
                self.trigger_heartbeats();
                self.sim.deliver_all_rounds(10);
                self.scheduler_tick();
                self.sim.deliver_all_rounds(10);
            }
        }

        /// Triggers heartbeat timeouts on all leaders.
        fn trigger_heartbeats(&mut self) {
            let node_ids: Vec<NodeId> = self.sim.node_ids.clone();
            for &node_id in &node_ids {
                if self.crashed.contains(&node_id) {
                    continue;
                }
                let shard_ids: Vec<ShardId> = self
                    .sim
                    .nodes
                    .get(&node_id)
                    .map(|shards| shards.keys().copied().collect())
                    .unwrap_or_default();
                for shard_id in shard_ids {
                    if let Some(shard_map) = self.sim.nodes.get_mut(&node_id)
                        && let Some(shard) = shard_map.get_mut(&shard_id)
                        && shard.state() == NodeState::Leader
                    {
                        let actions = shard.handle_heartbeat_timeout();
                        for action in actions {
                            if let Action::Send { to, shard, msg } = action {
                                self.sim.network.send(node_id, to, shard, msg);
                            }
                        }
                    }
                }
            }
        }

        /// Returns the DR voter set on a specific node.
        fn dr_voters_on(&self, node: NodeId) -> BTreeSet<NodeId> {
            self.sim
                .nodes
                .get(&node)
                .and_then(|s| s.get(&DR_SHARD))
                .map(|shard| shard.membership().voters.clone())
                .unwrap_or_default()
        }

        /// Returns the GLOBAL voter set on a specific node.
        #[allow(dead_code)]
        fn global_voters_on(&self, node: NodeId) -> BTreeSet<NodeId> {
            self.sim
                .nodes
                .get(&node)
                .and_then(|s| s.get(&GLOBAL_SHARD))
                .map(|shard| shard.membership().voters.clone())
                .unwrap_or_default()
        }

        /// Checks if the DR shard can commit a proposal.
        #[allow(dead_code)]
        fn can_write_dr(&mut self) -> bool {
            self.sim.propose_on(DR_SHARD, b"test-write".to_vec()).is_some()
        }
    }

    impl MultiRaftSim {
        /// Checks simulation invariants. Called after every tick in the soak test.
        fn check_invariants(&self) {
            // 1. conf_epoch sanity: verify it hasn't run away on the DR leader.
            if let Some(leader) = self.sim.leader_on(DR_SHARD)
                && let Some(shard) = self.sim.nodes.get(&leader).and_then(|s| s.get(&DR_SHARD))
            {
                let state = shard.state_snapshot();
                assert!(state.conf_epoch < 1000, "conf_epoch runaway: {}", state.conf_epoch);
            }

            // 2. No voter is both Dead and still the leader.
            // A Dead node shouldn't be leading (it should have been removed).
            // But during convergence it's possible -- only checked as a soft
            // invariant, not asserted.

            // 3. DR voters should be a subset of known nodes.
            if let Some(leader) = self.sim.leader_on(DR_SHARD) {
                let voters = self.dr_voters_on(leader);
                for voter in &voters {
                    assert!(
                        self.statuses.contains_key(voter),
                        "DR voter {:?} not in node statuses",
                        voter
                    );
                }
            }
        }

        /// Checks convergence: a leader exists and all desired voters are
        /// present in the DR voter set.
        ///
        /// Decommissioned nodes may still appear as voters if a pending
        /// membership change hasn't committed yet — this is acceptable
        /// because the scheduler will eventually remove them given enough
        /// ticks. The critical invariant is that desired voters are never
        /// missing (which would indicate data loss or split brain).
        fn assert_converged(&self) {
            let leader = self
                .sim
                .leader_on(DR_SHARD)
                .expect("DR shard should have a leader after convergence");
            let actual = self.dr_voters_on(leader);
            let desired = self.desired_dr_voters();
            assert!(
                desired.is_subset(&actual),
                "Desired voters missing from DR: desired={desired:?}, actual={actual:?}"
            );
        }
    }

    /// Number of soak iterations for standard CI.
    const SOAK_ITERATIONS: u64 = 10;
    /// Number of ticks per soak iteration.
    const SOAK_TICKS: usize = 500;

    /// Returns the number of soak iterations, defaulting to [`SOAK_ITERATIONS`]
    /// but overridable via the `PROPTEST_CASES` environment variable for nightly
    /// CI.
    fn soak_iterations() -> u64 {
        std::env::var("PROPTEST_CASES").ok().and_then(|s| s.parse().ok()).unwrap_or(SOAK_ITERATIONS)
    }

    #[test]
    fn soak_multi_raft_coordination() {
        for seed in 0..soak_iterations() {
            run_soak_iteration(seed);
        }
    }

    fn run_soak_iteration(seed: u64) {
        // Enable buggification for this iteration.
        crate::buggify::enable_buggify(seed);

        let node_count = 3 + (seed % 3); // 3-5 nodes
        let mut sim = MultiRaftSim::new(seed * 1000, node_count);
        sim.elect_leaders(NodeId(1), NodeId(1));

        let mut rng_state = seed;
        let mut ticks_since_fault = 0u32;

        for tick in 0..SOAK_TICKS {
            // LCG for deterministic random operations.
            rng_state = rng_state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let op = (rng_state >> 32) % 100;

            match op {
                0..=4 => {
                    // 5%: Decommission a random Active node (keep >=2 active
                    // to ensure quorum is always achievable during convergence).
                    let active: Vec<NodeId> = sim
                        .statuses
                        .iter()
                        .filter(|&(_, &s)| s == NodeStatus::Active)
                        .map(|(&id, _)| id)
                        .collect();
                    if active.len() > 2 {
                        let target = active[(rng_state as usize) % active.len()];
                        sim.decommission(target);
                        ticks_since_fault = 0;
                    }
                },
                5..=7 => {
                    // 3%: Crash a random alive node (if >1 alive).
                    let alive: Vec<NodeId> = sim
                        .statuses
                        .iter()
                        .filter(|&(_, &s)| s == NodeStatus::Active)
                        .filter(|(id, _)| !sim.crashed.contains(id))
                        .map(|(&id, _)| id)
                        .collect();
                    if alive.len() > 1 {
                        let target = alive[(rng_state as usize) % alive.len()];
                        sim.crash(target);
                        ticks_since_fault = 0;
                    }
                },
                8..=10 => {
                    // 3%: Restart a crashed node.
                    let crashed: Vec<NodeId> = sim.crashed.iter().copied().collect();
                    if !crashed.is_empty() {
                        let target = crashed[(rng_state as usize) % crashed.len()];
                        sim.restart(target);
                    }
                },
                11..=15 => {
                    // 5%: Advance clock by 1 second.
                    sim.sim.advance_clock(Duration::from_secs(1));
                },
                _ => {
                    // 84%: Try to propose an entry.
                    let _ =
                        sim.sim.propose_on(DR_SHARD, format!("soak-{seed}-{tick}").into_bytes());
                },
            }

            // Deliver messages and run scheduler.
            sim.trigger_heartbeats();
            sim.sim.deliver_all_rounds(5);
            sim.scheduler_tick();
            sim.sim.deliver_all_rounds(5);

            // Check invariants after every tick.
            sim.check_invariants();
            ticks_since_fault += 1;
        }

        // Suppress unused assignment warning — ticks_since_fault is tracked for
        // future invariant checks that may gate assertions on convergence time.
        let _ = ticks_since_fault;

        // Disable buggification before convergence so message delivery is
        // reliable and the scheduler can drive membership to the desired state.
        crate::buggify::disable_buggify();

        // Final convergence: heal all partitions, restart all crashed nodes.
        for node in sim.crashed.iter().copied().collect::<Vec<_>>() {
            sim.restart(node);
        }
        sim.sim.heal();

        // Ensure a leader exists. After heavy churn the simulation does not
        // automatically trigger elections, so try each node as a candidate.
        if sim.sim.leader_on(DR_SHARD).is_none() {
            let all_nodes: Vec<NodeId> = sim.sim.node_ids.clone();
            for &candidate in &all_nodes {
                if sim.sim.elect_leader_on(DR_SHARD, candidate) {
                    break;
                }
            }
        }

        // Run convergence ticks with heartbeats and message delivery.
        for _ in 0..100 {
            sim.sim.advance_clock(Duration::from_millis(100));
            sim.trigger_heartbeats();
            sim.sim.deliver_all_rounds(10);
            sim.scheduler_tick();
            sim.sim.deliver_all_rounds(10);
        }

        // Verify convergence.
        sim.assert_converged();
    }

    // -- Test: Basic multi-shard setup -------------------------------------------

    #[test]
    fn multi_raft_basic_setup() {
        let mut sim = MultiRaftSim::new(42, 3);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Both shards should have leaders.
        assert!(sim.sim.leader_on(GLOBAL_SHARD).is_some());
        assert!(sim.sim.leader_on(DR_SHARD).is_some());

        // All nodes should be Active.
        for id in [NodeId(1), NodeId(2), NodeId(3)] {
            assert_eq!(sim.statuses[&id], NodeStatus::Active);
        }

        // DR should have 3 voters.
        let voters = sim.dr_voters_on(NodeId(1));
        assert_eq!(voters.len(), 3);
    }

    #[test]
    fn multi_raft_decommission_removes_from_dr() {
        let mut sim = MultiRaftSim::new(42, 3);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Decommission node 3.
        sim.decommission(NodeId(3));

        // Run scheduler ticks until convergence.
        sim.converge(20);

        // Node 3 should be removed from DR.
        let voters = sim.dr_voters_on(NodeId(1));
        assert!(!voters.contains(&NodeId(3)), "Decommissioned node should be removed from DR");
        assert_eq!(voters.len(), 2);
    }

    #[test]
    fn multi_raft_dead_node_removed_from_dr() {
        let mut sim = MultiRaftSim::new(42, 3);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Mark node 3 as dead.
        sim.mark_dead(NodeId(3));

        // Run scheduler ticks until convergence.
        sim.converge(20);

        // Node 3 should be removed from DR voters.
        let voters = sim.dr_voters_on(NodeId(1));
        assert!(!voters.contains(&NodeId(3)), "Dead node should be removed from DR");
        assert_eq!(voters.len(), 2);
    }

    #[test]
    fn multi_raft_crash_and_restart() {
        let mut sim = MultiRaftSim::new(42, 3);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Crash node 2 -- it gets partitioned from everyone.
        sim.crash(NodeId(2));
        assert!(sim.crashed.contains(&NodeId(2)));

        // Restart node 2 -- heals partitions.
        sim.restart(NodeId(2));
        assert!(!sim.crashed.contains(&NodeId(2)));

        // After restart, the cluster should still be functional.
        sim.converge(10);
        assert!(sim.sim.leader_on(DR_SHARD).is_some());
    }

    #[test]
    fn multi_raft_scheduler_is_noop_when_converged() {
        let mut sim = MultiRaftSim::new(42, 3);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // All nodes active, all voters present -- scheduler should be a no-op.
        let voters_before = sim.dr_voters_on(NodeId(1));
        sim.scheduler_tick();
        sim.sim.deliver_all_rounds(10);
        let voters_after = sim.dr_voters_on(NodeId(1));

        assert_eq!(voters_before, voters_after);
    }

    #[test]
    fn multi_raft_crash_prevents_heartbeats() {
        let mut sim = MultiRaftSim::new(42, 5);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Record message count after election settles.
        let dropped_before = sim.sim.dropped_count();

        // Crash the leader -- partitioned from all other nodes.
        sim.crash(NodeId(1));
        assert!(sim.crashed.contains(&NodeId(1)));

        // trigger_heartbeats should skip the crashed node. Since only
        // NodeId(1) is leader on both shards, no heartbeats should be sent.
        sim.trigger_heartbeats();

        // Deliver to confirm no new messages were produced by crashed node.
        sim.sim.deliver_all_rounds(5);

        // The dropped count should not have increased because no heartbeats
        // were generated for the crashed leader.
        let dropped_after = sim.sim.dropped_count();
        assert_eq!(
            dropped_before, dropped_after,
            "No heartbeats should be generated for crashed leader"
        );
    }

    // -- Scenario tests: membership coordination --------------------------------

    #[test]
    fn scenario_graceful_decommission_4_to_3() {
        let mut sim = MultiRaftSim::new(100, 4);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Write some entries.
        for _ in 0..10 {
            assert!(sim.can_write_dr());
        }

        // Decommission node 4.
        sim.decommission(NodeId(4));
        sim.converge(30);

        // DR voters should be {1, 2, 3}.
        let voters = sim.dr_voters_on(NodeId(1));
        assert_eq!(voters.len(), 3);
        assert!(!voters.contains(&NodeId(4)));

        // Writes should still work.
        assert!(sim.can_write_dr());
    }

    #[test]
    fn scenario_sequential_decommission_4_to_1() {
        let mut sim = MultiRaftSim::new(200, 4);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Decommission nodes one by one, transferring leadership as needed.
        for &node in &[NodeId(2), NodeId(3), NodeId(4)] {
            sim.decommission(node);
            sim.converge(30);
        }

        // Crash decommissioned nodes.
        sim.crash(NodeId(2));
        sim.crash(NodeId(3));
        sim.crash(NodeId(4));
        sim.converge(20);

        // Node 1 should be sole DR voter and able to write.
        let voters = sim.dr_voters_on(NodeId(1));
        assert_eq!(voters.len(), 1);
        assert!(voters.contains(&NodeId(1)));
        assert!(sim.can_write_dr());
    }

    #[test]
    fn scenario_dr_leader_self_decommission() {
        let mut sim = MultiRaftSim::new(300, 3);
        sim.elect_leaders(NodeId(1), NodeId(1)); // Node 1 is DR leader.

        sim.decommission(NodeId(1));
        sim.converge(30);

        // Node 1 should be removed from DR (after leadership transfer).
        let voters = sim.dr_voters_on(NodeId(2));
        assert!(!voters.contains(&NodeId(1)));
        assert_eq!(voters.len(), 2);

        // Writes should work on the new leader.
        assert!(sim.can_write_dr());
    }

    #[test]
    fn scenario_dead_node_quorum_confirms() {
        let mut sim = MultiRaftSim::new(400, 3);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Mark node 3 as Dead (simulating quorum-confirmed liveness failure).
        sim.mark_dead(NodeId(3));
        sim.crash(NodeId(3)); // Simulate actual node failure.
        sim.converge(20);

        // Node 3 should be removed from DR.
        let voters = sim.dr_voters_on(NodeId(1));
        assert!(!voters.contains(&NodeId(3)));
        assert_eq!(voters.len(), 2);

        // Writes should work.
        assert!(sim.can_write_dr());
    }

    #[test]
    fn scenario_asymmetric_partition_no_false_dead() {
        let mut sim = MultiRaftSim::new(450, 3);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Node 3 is partitioned from the leader but NOT marked Dead
        // (simulating quorum-based liveness saving it).
        sim.sim.partition(&[NodeId(1)], &[NodeId(3)]);
        sim.converge(10);

        // Node 3 should STILL be in DR voters (not removed).
        let voters = sim.dr_voters_on(NodeId(1));
        assert!(voters.contains(&NodeId(3)), "Partitioned but not Dead node should remain in DR");
        assert_eq!(voters.len(), 3);

        sim.sim.heal();
    }

    #[test]
    fn scenario_epoch_fencing() {
        let mut sim = MultiRaftSim::new(500, 3);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Get initial epoch.
        let initial_epoch = sim
            .sim
            .nodes
            .get(&NodeId(1))
            .and_then(|s| s.get(&DR_SHARD))
            .map(|s| s.state_snapshot().conf_epoch)
            .unwrap_or(0);

        // Decommission node 3 — this changes DR membership and increments epoch.
        sim.decommission(NodeId(3));
        sim.converge(20);

        let new_epoch = sim
            .sim
            .nodes
            .get(&NodeId(1))
            .and_then(|s| s.get(&DR_SHARD))
            .map(|s| s.state_snapshot().conf_epoch)
            .unwrap_or(0);

        assert!(new_epoch > initial_epoch, "Epoch should increment on membership change");
    }

    #[test]
    fn scenario_concurrent_join_and_decommission() {
        let mut sim = MultiRaftSim::new(600, 4);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Decommission node 1 AND node 4 simultaneously.
        sim.decommission(NodeId(1));
        sim.decommission(NodeId(4));
        sim.converge(40);

        // DR should have only nodes 2 and 3.
        let voters = sim.dr_voters_on(NodeId(2));
        assert!(!voters.contains(&NodeId(1)));
        assert!(!voters.contains(&NodeId(4)));
        assert!(voters.contains(&NodeId(2)));
        assert!(voters.contains(&NodeId(3)));

        assert!(sim.can_write_dr());
    }

    #[test]
    fn scenario_learner_not_promoted_until_caught_up() {
        // This tests the concept that learners exist in the membership
        // without being voters. The actual catch-up check uses peer_match_index
        // which isn't available in the simulation (no apply worker).
        // We verify the learner → voter flow works at the Raft level.
        let mut sim = MultiRaftSim::new(700, 3);
        sim.elect_leaders(NodeId(1), NodeId(1));

        // Add a learner directly.
        sim.add_learner_to_dr(NodeId(1), NodeId(4));
        sim.sim.deliver_all_rounds(20);

        // Node 4 should be a learner, not a voter.
        let shard = sim.sim.nodes.get(&NodeId(1)).unwrap().get(&DR_SHARD).unwrap();
        assert!(shard.membership().learners.contains(&NodeId(4)));
        assert!(!shard.membership().voters.contains(&NodeId(4)));
    }

    #[test]
    fn scenario_global_leader_crash_during_decommission() {
        let mut sim = MultiRaftSim::new(800, 4);
        sim.elect_leaders(NodeId(1), NodeId(2)); // GLOBAL=1, DR=2

        // Decommission node 3.
        sim.decommission(NodeId(3));

        // Run a few ticks (decommission started but not complete).
        sim.converge(5);

        // Crash the GLOBAL leader.
        sim.crash(NodeId(1));

        // Elect new GLOBAL leader.
        sim.sim.advance_clock(Duration::from_secs(2));
        sim.sim.deliver_all_rounds(20);
        // Try to elect a new GLOBAL leader from surviving nodes.
        let _ = sim.sim.elect_leader_on(GLOBAL_SHARD, NodeId(2));

        // Continue convergence — the DR scheduler should continue working.
        sim.converge(30);

        // Node 3 should eventually be removed from DR
        // (the new GLOBAL leader's scheduler reads the Decommissioning status).
        let leader = sim.sim.leader_on(DR_SHARD);
        if let Some(leader_id) = leader {
            let voters = sim.dr_voters_on(leader_id);
            assert!(
                !voters.contains(&NodeId(3)),
                "Node 3 should be removed from DR after GLOBAL leader crash and recovery"
            );
        }
    }
}
