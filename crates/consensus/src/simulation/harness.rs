//! Deterministic simulation harness for multi-node, multi-shard Raft clusters.

use std::{collections::HashMap, sync::Arc, time::Duration};

use super::network::SimulatedNetwork;
use crate::{
    action::Action,
    clock::SimulatedClock,
    config::ShardConfig,
    rng::SimulatedRng,
    consensus_state::ConsensusState,
    types::{Membership, NodeId, NodeState, ConsensusStateId},
};

/// Default shard configuration for simulation nodes.
fn default_shard_config() -> ShardConfig {
    ShardConfig {
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
        ..ShardConfig::default()
    }
}

/// Deterministic simulation of a multi-node, multi-shard Raft cluster in a
/// single process.
///
/// Each physical node can host multiple shard groups. All nodes share a single
/// [`SimulatedClock`] and communicate through a [`SimulatedNetwork`] that
/// supports partition injection. Randomness is seeded so that identical seeds
/// produce identical execution traces.
pub struct Simulation {
    /// Per-node, per-shard mapping: `node -> shard_id -> ConsensusState`.
    pub nodes: HashMap<NodeId, HashMap<ConsensusStateId, ConsensusState<Arc<SimulatedClock>, SimulatedRng>>>,
    /// The simulated network connecting all nodes.
    pub network: SimulatedNetwork,
    clock: Arc<SimulatedClock>,
    pub(crate) node_ids: Vec<NodeId>,
    /// Ordered list of shard IDs for deterministic iteration.
    shard_ids: Vec<ConsensusStateId>,
    /// The default shard ID (first shard group added).
    default_shard: ConsensusStateId,
    seed: u64,
}

impl Simulation {
    /// Creates a new simulation with `node_count` nodes and a single shard
    /// group (`ConsensusStateId(1)`), all seeded deterministically.
    ///
    /// Each node gets `SimulatedRng::new(seed + node_index)` so they have
    /// different election timeouts while the overall simulation remains
    /// deterministic for a given seed.
    pub fn new(seed: u64, node_count: u64) -> Self {
        let clock = Arc::new(SimulatedClock::new());
        let shard_id = ConsensusStateId(1);
        let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId).collect();
        let membership = Membership::new(node_ids.clone());

        let mut nodes: HashMap<NodeId, HashMap<ConsensusStateId, ConsensusState<Arc<SimulatedClock>, SimulatedRng>>> =
            HashMap::new();
        for (i, &node_id) in node_ids.iter().enumerate() {
            let rng = SimulatedRng::new(seed + i as u64);
            let config = default_shard_config();
            let shard = ConsensusState::new(
                shard_id,
                node_id,
                membership.clone(),
                config,
                Arc::clone(&clock),
                rng,
                0,
                None,
                0,
            );
            let mut shard_map = HashMap::new();
            shard_map.insert(shard_id, shard);
            nodes.insert(node_id, shard_map);
        }

        Self {
            nodes,
            network: SimulatedNetwork::new(),
            clock,
            node_ids,
            shard_ids: vec![shard_id],
            default_shard: shard_id,
            seed,
        }
    }

    /// Adds a new shard group to the simulation, creating a shard on each of
    /// the specified nodes.
    ///
    /// The RNG for each node's new shard is seeded deterministically using the
    /// original seed, the shard ID, and the node index to avoid collisions with
    /// other shard groups.
    pub fn add_shard_group(&mut self, shard_id: ConsensusStateId, node_ids: &[NodeId]) {
        let membership = Membership::new(node_ids.to_vec());

        for (i, &node_id) in node_ids.iter().enumerate() {
            // Seed incorporates shard_id to avoid collision with the default group.
            let rng = SimulatedRng::new(self.seed + shard_id.0 * 1000 + i as u64);
            let config = default_shard_config();
            let shard = ConsensusState::new(
                shard_id,
                node_id,
                membership.clone(),
                config,
                Arc::clone(&self.clock),
                rng,
                0,
                None,
                0,
            );
            self.nodes.entry(node_id).or_default().insert(shard_id, shard);
            // Ensure node is tracked in node_ids.
            if !self.node_ids.contains(&node_id) {
                self.node_ids.push(node_id);
            }
        }

        if !self.shard_ids.contains(&shard_id) {
            self.shard_ids.push(shard_id);
        }
    }

    // ── Clock ─────────────────────────────────────────────────────

    /// Advances the simulated clock by `duration` for all nodes.
    pub fn advance_clock(&self, duration: Duration) {
        self.clock.advance(duration);
    }

    // ── Election ──────────────────────────────────────────────────

    /// Triggers an election on `candidate` for the default shard group and
    /// delivers messages until a leader emerges or delivery quiesces.
    ///
    /// Returns `true` if a leader was elected.
    pub fn elect_leader(&mut self, candidate: NodeId) -> bool {
        self.elect_leader_on(self.default_shard, candidate)
    }

    /// Triggers an election on `candidate` for the specified shard group.
    ///
    /// Returns `true` if a leader was elected.
    pub fn elect_leader_on(&mut self, shard_id: ConsensusStateId, candidate: NodeId) -> bool {
        // Advance time past election timeout so pre-vote grants succeed.
        self.clock.advance(Duration::from_millis(500));

        if let Some(shard_map) = self.nodes.get_mut(&candidate)
            && let Some(shard) = shard_map.get_mut(&shard_id)
        {
            let actions = shard.handle_election_timeout();
            self.process_actions(candidate, actions);
        }
        self.deliver_all_rounds(20);
        self.has_leader_on(shard_id)
    }

    // ── Proposals ─────────────────────────────────────────────────

    /// Proposes `data` on the default shard group's leader and delivers
    /// messages until the entry is committed or delivery quiesces.
    ///
    /// Returns the commit index if committed, or `None` otherwise.
    pub fn propose(&mut self, data: Vec<u8>) -> Option<u64> {
        self.propose_on(self.default_shard, data)
    }

    /// Proposes `data` on the specified shard group's leader.
    ///
    /// Returns the commit index if committed, or `None` otherwise.
    pub fn propose_on(&mut self, shard_id: ConsensusStateId, data: Vec<u8>) -> Option<u64> {
        let leader_id = self.leader_on(shard_id)?;
        let shard = self.nodes.get_mut(&leader_id)?.get_mut(&shard_id)?;
        let ci_before = shard.commit_index();
        let actions = match shard.handle_propose(data) {
            Ok(a) => a,
            Err(_) => return None,
        };
        self.process_actions(leader_id, actions);

        // Send heartbeats to propagate commit index after replication.
        if let Some(shard_map) = self.nodes.get_mut(&leader_id)
            && let Some(shard) = shard_map.get_mut(&shard_id)
        {
            let hb_actions = shard.handle_heartbeat_timeout();
            self.process_actions(leader_id, hb_actions);
        }

        self.deliver_all_rounds(20);

        let shard = self.nodes.get(&leader_id)?.get(&shard_id)?;
        let ci = shard.commit_index();
        // Check that the commit index actually advanced past where it was
        // before the proposal. With the no-op from become_leader, ci > 0
        // is always true, so comparing against ci_before is more accurate.
        if ci > ci_before { Some(ci) } else { None }
    }

    // ── Message delivery ──────────────────────────────────────────

    /// Delivers messages across the network until quiescent or `max_rounds`
    /// rounds have been executed.
    ///
    /// Returns the number of rounds actually executed.
    pub fn deliver_all_rounds(&mut self, max_rounds: usize) -> usize {
        for round in 0..max_rounds {
            if !self.deliver_one_round() {
                return round;
            }
        }
        max_rounds
    }

    // ── Partitions & node lifecycle ───────────────────────────────

    /// Partitions `group_a` from `group_b` bidirectionally.
    ///
    /// This affects all shard groups on the partitioned nodes — the partition
    /// operates at the network (physical node) level.
    pub fn partition(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        self.network.partition(group_a, group_b);
    }

    /// Heals all network partitions.
    pub fn heal(&mut self) {
        self.network.heal();
    }

    /// Removes a node from the simulation entirely, affecting all shard groups
    /// hosted on that node.
    pub fn kill(&mut self, node: NodeId) {
        self.nodes.remove(&node);
        self.node_ids.retain(|&id| id != node);
    }

    // ── Leader queries ────────────────────────────────────────────

    /// Returns the `NodeId` of the current leader for the default shard group.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader_on(self.default_shard)
    }

    /// Returns the `NodeId` of the current leader for the specified shard group.
    pub fn leader_on(&self, shard_id: ConsensusStateId) -> Option<NodeId> {
        self.node_ids.iter().copied().find(|&id| {
            self.nodes
                .get(&id)
                .and_then(|m| m.get(&shard_id))
                .is_some_and(|s| s.state() == NodeState::Leader)
        })
    }

    /// Returns `true` if any live node is the leader for the default shard group.
    pub fn has_leader(&self) -> bool {
        self.leader().is_some()
    }

    /// Returns `true` if any live node is the leader for the specified shard group.
    pub fn has_leader_on(&self, shard_id: ConsensusStateId) -> bool {
        self.leader_on(shard_id).is_some()
    }

    // ── Commit index queries ──────────────────────────────────────

    /// Returns the commit index of a specific node for the default shard group.
    pub fn commit_index(&self, node: NodeId) -> Option<u64> {
        self.commit_index_on(self.default_shard, node)
    }

    /// Returns the commit index of a specific node for the specified shard group.
    pub fn commit_index_on(&self, shard_id: ConsensusStateId, node: NodeId) -> Option<u64> {
        self.nodes.get(&node)?.get(&shard_id).map(|s| s.commit_index())
    }

    // ── Accessors ─────────────────────────────────────────────────

    /// Returns the seed used to create this simulation.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the number of live (not killed) nodes.
    pub fn live_node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the total number of messages delivered across the network.
    pub fn delivered_count(&self) -> u64 {
        self.network.delivered_count()
    }

    /// Returns the total number of messages dropped due to partitions.
    pub fn dropped_count(&self) -> u64 {
        self.network.dropped_count()
    }

    /// Returns the default shard ID used in this simulation.
    #[allow(dead_code)]
    pub fn shard_id(&self) -> ConsensusStateId {
        self.default_shard
    }

    /// Returns all shard IDs in this simulation.
    #[allow(dead_code)]
    pub fn shard_ids(&self) -> &[ConsensusStateId] {
        &self.shard_ids
    }

    // ── Internal helpers ──────────────────────────────────────────

    /// Delivers one round of messages: receive pending messages for each node
    /// in deterministic order, process them through all shard groups, and route
    /// outbound messages back through the network.
    ///
    /// Returns `true` if any messages were delivered (i.e., more work may remain).
    fn deliver_one_round(&mut self) -> bool {
        let mut any_delivered = false;

        // Snapshot the current node IDs for deterministic iteration order.
        let ids: Vec<NodeId> = self.node_ids.clone();

        for &node_id in &ids {
            let inbound = self.network.receive(node_id);
            if inbound.is_empty() {
                continue;
            }
            any_delivered = true;

            // Route each inbound message to the correct shard based on its shard_id.
            let mut all_actions = Vec::new();
            if let Some(shard_map) = self.nodes.get_mut(&node_id) {
                for msg in inbound {
                    if let Some(shard) = shard_map.get_mut(&msg.shard_id) {
                        let actions = shard.handle_message(msg.from, msg.message);
                        all_actions.extend(actions);
                    }
                }
            }

            // Route outbound Send actions through the network.
            self.process_actions(node_id, all_actions);
        }

        any_delivered
    }

    /// Processes a batch of actions, routing `Send` actions through the network.
    fn process_actions(&mut self, from: NodeId, actions: Vec<Action>) {
        for action in actions {
            if let Action::Send { to, shard, msg } = action {
                self.network.send(from, to, shard, msg);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // ── Election ──────────────────────────────────────────────────

    #[test]
    fn elect_leader_returns_true_for_3_node_cluster() {
        let mut sim = Simulation::new(42, 3);
        assert!(sim.elect_leader(NodeId(1)));
    }

    #[test]
    fn elect_leader_sets_exactly_one_leader() {
        let mut sim = Simulation::new(42, 3);
        sim.elect_leader(NodeId(1));

        let leader_count = sim
            .nodes
            .values()
            .filter_map(|m| m.get(&ConsensusStateId(1)))
            .filter(|s| s.state() == NodeState::Leader)
            .count();
        assert_eq!(leader_count, 1);
    }

    #[test]
    fn leader_returns_elected_node() {
        let mut sim = Simulation::new(42, 3);
        sim.elect_leader(NodeId(1));

        assert!(sim.leader().is_some());
    }

    #[test]
    fn has_leader_returns_false_before_election() {
        let sim = Simulation::new(42, 3);
        assert!(!sim.has_leader());
    }

    // ── Proposals ─────────────────────────────────────────────────

    #[test]
    fn propose_returns_commit_index() {
        let mut sim = Simulation::new(42, 3);
        sim.elect_leader(NodeId(1));

        let commit = sim.propose(b"hello".to_vec());
        assert!(commit.is_some());
        assert!(commit.unwrap() >= 1);
    }

    #[test]
    fn propose_returns_none_without_leader() {
        let mut sim = Simulation::new(42, 3);
        // No election — no leader.
        let commit = sim.propose(b"orphan".to_vec());
        assert!(commit.is_none());
    }

    #[test]
    fn sequential_proposals_have_monotonic_commit_indices() {
        let mut sim = Simulation::new(42, 3);
        sim.elect_leader(NodeId(1));

        let c1 = sim.propose(b"first".to_vec()).unwrap();
        let c2 = sim.propose(b"second".to_vec()).unwrap();
        let c3 = sim.propose(b"third".to_vec()).unwrap();

        assert!(c2 > c1);
        assert!(c3 > c2);
    }

    // ── Determinism ───────────────────────────────────────────────

    #[test]
    fn same_seed_produces_identical_leader() {
        fn leader_for(seed: u64) -> Option<NodeId> {
            let mut sim = Simulation::new(seed, 3);
            sim.elect_leader(NodeId(1));
            sim.leader()
        }

        assert_eq!(leader_for(123), leader_for(123));
    }

    #[test]
    fn same_seed_produces_identical_message_count() {
        fn delivered_for(seed: u64) -> u64 {
            let mut sim = Simulation::new(seed, 3);
            sim.elect_leader(NodeId(1));
            sim.propose(b"data".to_vec());
            sim.delivered_count()
        }

        assert_eq!(delivered_for(123), delivered_for(123));
    }

    // ── Partitions ────────────────────────────────────────────────

    #[test]
    fn partition_drops_messages_from_leader() {
        let mut sim = Simulation::new(42, 3);
        sim.elect_leader(NodeId(1));
        let leader_id = sim.leader().expect("leader should exist");

        sim.partition(&[leader_id], &[NodeId(2), NodeId(3)]);

        let raft_shard = sim.nodes.get_mut(&leader_id).unwrap().get_mut(&ConsensusStateId(1)).unwrap();
        let actions = raft_shard.handle_propose(b"partitioned".to_vec()).unwrap();
        for action in actions {
            if let Action::Send { to, shard: shard_id, msg } = action {
                sim.network.send(leader_id, to, shard_id, msg);
            }
        }

        sim.deliver_all_rounds(10);
        assert!(sim.dropped_count() > 0);
    }

    #[test]
    fn heal_restores_commit_ability() {
        let mut sim = Simulation::new(42, 3);
        sim.elect_leader(NodeId(1));

        sim.partition(&[NodeId(1)], &[NodeId(2), NodeId(3)]);
        sim.heal();

        let commit = sim.propose(b"healed".to_vec());
        assert!(commit.is_some());
    }

    // ── Node lifecycle ────────────────────────────────────────────

    #[test]
    fn kill_decrements_live_node_count() {
        let mut sim = Simulation::new(42, 3);
        assert_eq!(sim.live_node_count(), 3);
        sim.kill(NodeId(3));
        assert_eq!(sim.live_node_count(), 2);
    }

    #[test]
    fn kill_removes_node_from_map() {
        let mut sim = Simulation::new(42, 3);
        sim.kill(NodeId(3));
        assert!(!sim.nodes.contains_key(&NodeId(3)));
    }

    // ── Delivery mechanics ────────────────────────────────────────

    #[test]
    fn deliver_all_rounds_returns_zero_when_quiescent() {
        let sim_harness = Simulation::new(42, 3);
        // No messages in flight — should quiesce immediately.
        let mut sim = sim_harness;
        let rounds = sim.deliver_all_rounds(10);
        assert_eq!(rounds, 0);
    }

    // ── Accessors ─────────────────────────────────────────────────

    #[test]
    fn seed_returns_construction_seed() {
        let sim = Simulation::new(99, 3);
        assert_eq!(sim.seed(), 99);
    }

    #[test]
    fn shard_id_returns_fixed_value() {
        let sim = Simulation::new(42, 3);
        assert_eq!(sim.shard_id(), ConsensusStateId(1));
    }

    #[test]
    fn commit_index_returns_none_for_missing_node() {
        let sim = Simulation::new(42, 3);
        assert!(sim.commit_index(NodeId(99)).is_none());
    }

    #[test]
    fn commit_index_returns_zero_before_any_proposal() {
        let sim = Simulation::new(42, 3);
        assert_eq!(sim.commit_index(NodeId(1)), Some(0));
    }

    #[test]
    fn advance_clock_does_not_panic() {
        let sim = Simulation::new(42, 3);
        sim.advance_clock(Duration::from_secs(10));
        // Verify the clock advanced by checking election timeout would fire.
        // No assertion on exact value — just confirming no panic.
    }

    // ── Multi-shard group tests ───────────────────────────────────

    #[test]
    fn add_shard_group_creates_shards_on_specified_nodes() {
        let mut sim = Simulation::new(42, 3);
        sim.add_shard_group(ConsensusStateId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

        for nid in [NodeId(1), NodeId(2), NodeId(3)] {
            let shard_map = sim.nodes.get(&nid).unwrap();
            assert!(shard_map.contains_key(&ConsensusStateId(1)), "node {nid:?} missing default shard");
            assert!(shard_map.contains_key(&ConsensusStateId(2)), "node {nid:?} missing added shard");
        }
    }

    #[test]
    fn two_independent_shard_groups_elect_leaders_independently() {
        let mut sim = Simulation::new(42, 3);
        sim.add_shard_group(ConsensusStateId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

        assert!(sim.elect_leader_on(ConsensusStateId(1), NodeId(1)));
        assert!(sim.elect_leader_on(ConsensusStateId(2), NodeId(2)));

        assert!(sim.has_leader_on(ConsensusStateId(1)));
        assert!(sim.has_leader_on(ConsensusStateId(2)));
    }

    #[test]
    fn two_shard_groups_have_independent_commit_indices() {
        let mut sim = Simulation::new(42, 3);
        sim.add_shard_group(ConsensusStateId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

        sim.elect_leader_on(ConsensusStateId(1), NodeId(1));
        sim.elect_leader_on(ConsensusStateId(2), NodeId(2));

        // Propose different amounts to each shard group.
        let ci1_a = sim.propose_on(ConsensusStateId(1), b"shard1-a".to_vec());
        let ci1_b = sim.propose_on(ConsensusStateId(1), b"shard1-b".to_vec());
        let ci2_a = sim.propose_on(ConsensusStateId(2), b"shard2-a".to_vec());

        assert!(ci1_a.is_some(), "shard 1 first proposal should commit");
        assert!(ci1_b.is_some(), "shard 1 second proposal should commit");
        assert!(ci2_a.is_some(), "shard 2 first proposal should commit");

        // ConsensusState 1 should be at commit index 2, shard 2 at commit index 1.
        let leader1 = sim.leader_on(ConsensusStateId(1)).unwrap();
        let leader2 = sim.leader_on(ConsensusStateId(2)).unwrap();

        let s1_ci = sim.commit_index_on(ConsensusStateId(1), leader1).unwrap();
        let s2_ci = sim.commit_index_on(ConsensusStateId(2), leader2).unwrap();

        // +1 for the no-op entry committed on leader election
        assert_eq!(s1_ci, 3, "shard 1 should have 3 committed entries (1 no-op + 2 proposed)");
        assert_eq!(s2_ci, 2, "shard 2 should have 2 committed entries (1 no-op + 1 proposed)");
    }

    #[test]
    fn partition_affects_all_shards_on_a_node() {
        let mut sim = Simulation::new(42, 3);
        sim.add_shard_group(ConsensusStateId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

        sim.elect_leader_on(ConsensusStateId(1), NodeId(1));
        sim.elect_leader_on(ConsensusStateId(2), NodeId(1));

        let leader1 = sim.leader_on(ConsensusStateId(1)).unwrap();
        let leader2 = sim.leader_on(ConsensusStateId(2)).unwrap();

        // Partition the leader(s) from followers.
        let followers: Vec<NodeId> =
            [NodeId(1), NodeId(2), NodeId(3)].into_iter().filter(|&id| id != leader1).collect();
        sim.partition(&[leader1], &followers);

        // Proposals on shard 1 should fail (leader is partitioned).
        let s1_result = sim.propose_on(ConsensusStateId(1), b"blocked-s1".to_vec());

        // If leader2 is the same node as leader1, shard 2 proposals should also fail.
        if leader2 == leader1 {
            let s2_result = sim.propose_on(ConsensusStateId(2), b"blocked-s2".to_vec());
            assert!(
                s2_result.is_none(),
                "shard 2 proposals should fail when its leader is partitioned"
            );
        }

        assert!(s1_result.is_none(), "shard 1 proposals should fail when leader is partitioned");
    }

    #[test]
    fn different_nodes_can_lead_different_shards() {
        let mut sim = Simulation::new(42, 3);
        sim.add_shard_group(ConsensusStateId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

        // Elect different candidates for each shard.
        sim.elect_leader_on(ConsensusStateId(1), NodeId(1));
        sim.elect_leader_on(ConsensusStateId(2), NodeId(2));

        let leader1 = sim.leader_on(ConsensusStateId(1));
        let leader2 = sim.leader_on(ConsensusStateId(2));

        assert!(leader1.is_some(), "shard 1 should have a leader");
        assert!(leader2.is_some(), "shard 2 should have a leader");

        // Both shard groups should be functional regardless of leader placement.
        let ci1 = sim.propose_on(ConsensusStateId(1), b"on-shard-1".to_vec());
        let ci2 = sim.propose_on(ConsensusStateId(2), b"on-shard-2".to_vec());
        assert!(ci1.is_some(), "shard 1 should accept proposals");
        assert!(ci2.is_some(), "shard 2 should accept proposals");
    }

    #[test]
    fn kill_removes_all_shards_on_node() {
        let mut sim = Simulation::new(42, 3);
        sim.add_shard_group(ConsensusStateId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

        sim.kill(NodeId(3));

        assert!(!sim.nodes.contains_key(&NodeId(3)));
        assert!(sim.commit_index_on(ConsensusStateId(1), NodeId(3)).is_none());
        assert!(sim.commit_index_on(ConsensusStateId(2), NodeId(3)).is_none());
    }

    #[test]
    fn shard_ids_returns_all_groups() {
        let mut sim = Simulation::new(42, 3);
        sim.add_shard_group(ConsensusStateId(2), &[NodeId(1), NodeId(2), NodeId(3)]);
        sim.add_shard_group(ConsensusStateId(3), &[NodeId(1), NodeId(2), NodeId(3)]);

        assert_eq!(sim.shard_ids(), &[ConsensusStateId(1), ConsensusStateId(2), ConsensusStateId(3)]);
    }

    #[test]
    fn backward_compat_default_methods_use_shard_1() {
        let mut sim = Simulation::new(42, 3);
        // Add a second shard group but use the default (backward-compat) methods.
        sim.add_shard_group(ConsensusStateId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

        assert!(sim.elect_leader(NodeId(1)));
        let ci = sim.propose(b"compat".to_vec());
        assert!(ci.is_some());

        // Default leader/commit_index should reflect ConsensusStateId(1).
        let leader = sim.leader().unwrap();
        let commit = sim.commit_index(leader).unwrap();
        assert!(commit >= 1);
    }
}
