use std::{collections::HashMap, sync::Arc, time::Duration};

use inferadb_ledger_consensus::{
    action::Action,
    clock::SimulatedClock,
    config::ShardConfig,
    error::ConsensusError,
    message::Message,
    rng::SimulatedRng,
    shard::Shard,
    types::{Membership, MembershipChange, NodeId, NodeState, ShardId},
};

#[allow(dead_code)]
pub type TestShard = Shard<Arc<SimulatedClock>, SimulatedRng>;

#[allow(dead_code)]
pub struct TestCluster {
    pub shards: HashMap<NodeId, TestShard>,
    pub clock: Arc<SimulatedClock>,
}

#[allow(dead_code)]
impl TestCluster {
    pub fn new(node_count: u64) -> Self {
        let clock = Arc::new(SimulatedClock::new());
        let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId).collect();
        let membership = Membership::new(node_ids.iter().copied());
        let config = ShardConfig::default();

        let mut shards = HashMap::new();
        for (i, &node_id) in node_ids.iter().enumerate() {
            let rng = SimulatedRng::new(100 + i as u64);
            let shard = Shard::new(
                ShardId(0),
                node_id,
                membership.clone(),
                config.clone(),
                clock.clone(),
                rng,
            );
            shards.insert(node_id, shard);
        }

        Self { shards, clock }
    }

    /// Triggers election on a node and runs message delivery until a leader emerges.
    pub fn elect_leader(&mut self, candidate: NodeId) {
        self.clock.advance(Duration::from_secs(1));
        let actions =
            self.shards.get_mut(&candidate).expect("candidate not found").handle_election_timeout();
        self.deliver_actions(candidate, actions, 0);
    }

    /// Delivers all `Action::Send` messages to their targets, collecting and
    /// delivering responses recursively. A depth limit prevents infinite loops.
    fn deliver_actions(&mut self, from: NodeId, actions: Vec<Action>, depth: usize) {
        if depth > 50 {
            return;
        }

        let mut pending: Vec<(NodeId, NodeId, Message)> = Vec::new();
        for action in actions {
            if let Action::Send { to, msg, .. } = action {
                pending.push((from, to, msg));
            }
        }

        let mut responses: Vec<(NodeId, Vec<Action>)> = Vec::new();
        for (sender, target, msg) in pending {
            if let Some(shard) = self.shards.get_mut(&target) {
                let reply_actions = shard.handle_message(sender, msg);
                responses.push((target, reply_actions));
            }
        }

        for (responder, actions) in responses {
            self.deliver_actions(responder, actions, depth + 1);
        }
    }

    /// Public entry point for delivering actions (used by tests that manually
    /// trigger election timeouts and need to deliver the resulting messages).
    pub fn deliver_actions_public(&mut self, from: NodeId, actions: Vec<Action>) {
        self.deliver_actions(from, actions, 0);
    }

    pub fn has_leader(&self) -> bool {
        self.shards.values().any(|s| s.state() == NodeState::Leader)
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        self.shards.iter().find(|(_, s)| s.state() == NodeState::Leader).map(|(&id, _)| id)
    }

    /// Returns the number of nodes that are followers.
    pub fn follower_count(&self) -> usize {
        self.shards.values().filter(|s| s.state() == NodeState::Follower).count()
    }

    /// Proposes data on the leader and delivers replication messages.
    pub fn propose(&mut self, data: Vec<u8>) -> u64 {
        let leader = self.leader_id().expect("no leader");
        let actions = self
            .shards
            .get_mut(&leader)
            .expect("leader not found")
            .handle_propose(data)
            .expect("propose failed");
        self.deliver_actions(leader, actions, 0);
        self.shards[&leader].commit_index()
    }

    /// Proposes a batch and delivers replication messages.
    pub fn propose_batch(&mut self, batch: Vec<Vec<u8>>) -> u64 {
        let leader = self.leader_id().expect("no leader");
        let actions = self
            .shards
            .get_mut(&leader)
            .expect("leader not found")
            .handle_propose_batch(batch)
            .expect("propose failed");
        self.deliver_actions(leader, actions, 0);
        self.shards[&leader].commit_index()
    }

    /// Proposes a membership change on the leader and delivers replication messages.
    pub fn propose_membership_change(
        &mut self,
        change: MembershipChange,
    ) -> Result<(), ConsensusError> {
        let leader = self.leader_id().expect("no leader");
        let actions = self
            .shards
            .get_mut(&leader)
            .expect("leader not found")
            .handle_membership_change(change)?;
        self.deliver_actions(leader, actions, 0);
        Ok(())
    }

    /// Adds a new node to the cluster (creates the `Shard` instance locally).
    ///
    /// The new node starts as a follower with the given membership. This
    /// simulates a node that has just joined and is ready to receive
    /// replication from the leader.
    pub fn add_node(&mut self, node_id: NodeId, membership: Membership) {
        let rng = SimulatedRng::new(100 + node_id.0);
        let config = ShardConfig::default();
        let shard = Shard::new(ShardId(0), node_id, membership, config, self.clock.clone(), rng);
        self.shards.insert(node_id, shard);
    }

    /// Triggers a leadership transfer and delivers the resulting messages.
    pub fn transfer_leader(&mut self, target: NodeId) -> Result<(), ConsensusError> {
        let leader = self.leader_id().expect("no leader");
        let actions = self
            .shards
            .get_mut(&leader)
            .expect("leader not found")
            .handle_transfer_leader(target)?;
        self.deliver_actions(leader, actions, 0);
        Ok(())
    }
}
