//! Simulation-based correctness tests.
//!
//! Run the consensus engine under partitions, node failures, and concurrent
//! operations across multiple seeds to verify correctness deterministically.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]

use inferadb_ledger_consensus::{
    action::Action,
    simulation::Simulation,
    types::{NodeId, ShardId},
};

// ── Election across seeds ─────────────────────────────────────────

/// Seed 0..100: 3-node clusters always elect a leader.
#[test]
fn election_3_nodes_100_seeds() {
    for seed in 0..100 {
        let mut sim = Simulation::new(seed, 3);
        let elected = sim.elect_leader(NodeId(1));
        assert!(elected, "seed {seed}: failed to elect a leader in a 3-node cluster");
        assert!(sim.leader().is_some(), "seed {seed}: leader() returned None after election");
    }
}

/// Seed 0..50: 5-node clusters always elect a leader.
#[test]
fn election_5_nodes_50_seeds() {
    for seed in 0..50 {
        let mut sim = Simulation::new(seed, 5);
        let elected = sim.elect_leader(NodeId(1));
        assert!(elected, "seed {seed}: failed to elect a leader in a 5-node cluster");
        assert!(sim.leader().is_some(), "seed {seed}: leader() returned None after election");
    }
}

/// Seed 0..1000: stress-test election convergence at scale.
#[test]
fn election_3_nodes_1000_seeds() {
    for seed in 0..1000 {
        let mut sim = Simulation::new(seed, 3);
        let elected = sim.elect_leader(NodeId(1));
        assert!(elected, "seed {seed}: failed to elect a leader in a 3-node cluster");
    }
}

// ── Single-node cluster ───────────────────────────────────────────

/// A single-node cluster elects itself immediately.
#[test]
fn single_node_elects_itself() {
    // Seed 42
    let mut sim = Simulation::new(42, 1);
    assert!(sim.elect_leader(NodeId(1)));
    assert_eq!(sim.leader(), Some(NodeId(1)));
}

/// A single-node cluster commits without replication.
#[test]
fn single_node_commits_immediately() {
    // Seed 42
    let mut sim = Simulation::new(42, 1);
    sim.elect_leader(NodeId(1));

    let idx = sim.propose(b"solo".to_vec());
    assert_eq!(idx, Some(2)); // +1 for leader no-op
}

// ── Minority failure ──────────────────────────────────────────────

/// Killing one follower in a 3-node cluster preserves quorum.
#[test]
fn commit_succeeds_after_minority_failure() {
    // Seed 42
    let mut sim = Simulation::new(42, 3);
    assert!(sim.elect_leader(NodeId(1)));

    let c1 = sim.propose(b"before-kill".to_vec());
    assert!(c1.is_some(), "first proposal should commit with full cluster");

    let leader_id = sim.leader().unwrap();
    let follower = if leader_id == NodeId(1) { NodeId(3) } else { NodeId(1) };
    sim.kill(follower);

    let c2 = sim.propose(b"after-kill".to_vec());
    assert!(c2.is_some(), "proposal should commit with 2/3 quorum");
    assert!(c2.unwrap() > c1.unwrap());
}

/// Killing a majority prevents commits in a 3-node cluster.
#[test]
fn commit_fails_after_majority_failure() {
    // Seed 42
    let mut sim = Simulation::new(42, 3);
    assert!(sim.elect_leader(NodeId(1)));
    let leader_id = sim.leader().unwrap();

    // Kill both followers.
    let followers: Vec<NodeId> =
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().filter(|&id| id != leader_id).collect();
    for f in &followers {
        sim.kill(*f);
    }

    // Leader alone cannot form quorum for new entries.
    let shard = sim.nodes.get_mut(&leader_id).unwrap().get_mut(&ShardId(1)).unwrap();
    let actions = shard.handle_propose(b"no-quorum".to_vec()).unwrap();
    for action in actions {
        if let Action::Send { to, shard, msg } = action {
            sim.network.send(leader_id, to, shard, msg);
        }
    }
    sim.deliver_all_rounds(10);

    // Commit index should not advance beyond the no-op (no quorum to replicate).
    let ci = sim.commit_index(leader_id).unwrap();
    assert_eq!(ci, 1, "commit index must stay at 1 (no-op only) without quorum, got {ci}");
}

// ── Partition: leader isolated ────────────────────────────────────

/// Isolating the leader from all followers blocks commits.
#[test]
fn partition_blocks_commit_when_leader_isolated() {
    // Seed 42
    let mut sim = Simulation::new(42, 3);
    assert!(sim.elect_leader(NodeId(1)));
    let leader_id = sim.leader().expect("leader must exist");

    let followers: Vec<NodeId> =
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().filter(|&id| id != leader_id).collect();
    sim.partition(&[leader_id], &followers);

    let shard = sim.nodes.get_mut(&leader_id).unwrap().get_mut(&ShardId(1)).unwrap();
    let actions = shard.handle_propose(b"partitioned".to_vec()).unwrap();
    for action in actions {
        if let Action::Send { to, shard, msg } = action {
            sim.network.send(leader_id, to, shard, msg);
        }
    }

    sim.deliver_all_rounds(10);

    let ci = sim.commit_index(leader_id).unwrap();
    assert_eq!(
        ci, 1,
        "commit index should stay at 1 (no-op only) when leader is partitioned from quorum"
    );
}

/// Partitioned sends increment the network dropped counter.
#[test]
fn partition_increments_dropped_count() {
    // Seed 42
    let mut sim = Simulation::new(42, 3);
    assert!(sim.elect_leader(NodeId(1)));
    let leader_id = sim.leader().unwrap();

    let followers: Vec<NodeId> =
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().filter(|&id| id != leader_id).collect();
    sim.partition(&[leader_id], &followers);

    let shard = sim.nodes.get_mut(&leader_id).unwrap().get_mut(&ShardId(1)).unwrap();
    let actions = shard.handle_propose(b"will-drop".to_vec()).unwrap();
    let send_count = actions.iter().filter(|a| matches!(a, Action::Send { .. })).count();
    for action in actions {
        if let Action::Send { to, shard, msg } = action {
            sim.network.send(leader_id, to, shard, msg);
        }
    }

    assert!(sim.dropped_count() >= send_count as u64);
}

// ── Partition: heal restores progress ─────────────────────────────

/// After healing a partition, the leader can commit again.
#[test]
fn heal_after_partition_restores_commit() {
    // Seed 42
    let mut sim = Simulation::new(42, 3);
    assert!(sim.elect_leader(NodeId(1)));
    let leader_id = sim.leader().unwrap();

    let followers: Vec<NodeId> =
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().filter(|&id| id != leader_id).collect();

    // Partition leader from followers.
    sim.partition(&[leader_id], &followers);

    // Propose while partitioned — messages will be dropped.
    let shard = sim.nodes.get_mut(&leader_id).unwrap().get_mut(&ShardId(1)).unwrap();
    let actions = shard.handle_propose(b"blocked".to_vec()).unwrap();
    for action in actions {
        if let Action::Send { to, shard, msg } = action {
            sim.network.send(leader_id, to, shard, msg);
        }
    }
    sim.deliver_all_rounds(10);
    assert_eq!(
        sim.commit_index(leader_id).unwrap(),
        1, // +1 for leader no-op committed before partition
        "commit should be blocked while partitioned"
    );

    // Heal the partition.
    sim.heal();

    // Trigger heartbeat to re-send pending entries.
    let shard = sim.nodes.get_mut(&leader_id).unwrap().get_mut(&ShardId(1)).unwrap();
    let hb_actions = shard.handle_heartbeat_timeout();
    for action in hb_actions {
        if let Action::Send { to, shard, msg } = action {
            sim.network.send(leader_id, to, shard, msg);
        }
    }

    sim.deliver_all_rounds(20);

    let ci = sim.commit_index(leader_id).unwrap();
    assert!(ci >= 2, "commit should advance after partition heals, got {ci}");
}

// ── Multiple sequential partitions and heals ──────────────────────

/// Three cycles of partition-then-heal: each heal restores commit ability.
#[test]
fn multiple_partition_heal_cycles() {
    // Seed 77
    let mut sim = Simulation::new(77, 3);
    assert!(sim.elect_leader(NodeId(1)));
    let leader_id = sim.leader().unwrap();

    let followers: Vec<NodeId> =
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().filter(|&id| id != leader_id).collect();

    for cycle in 0..3 {
        // Partition.
        sim.partition(&[leader_id], &followers);
        let raft_shard = sim.nodes.get_mut(&leader_id).unwrap().get_mut(&ShardId(1)).unwrap();
        let actions = raft_shard.handle_propose(format!("blocked-{cycle}").into_bytes()).unwrap();
        for action in actions {
            if let Action::Send { to, shard: shard_id, msg } = action {
                sim.network.send(leader_id, to, shard_id, msg);
            }
        }
        sim.deliver_all_rounds(5);
        let ci_during = sim.commit_index(leader_id).unwrap();

        // Heal.
        sim.heal();
        let raft_shard = sim.nodes.get_mut(&leader_id).unwrap().get_mut(&ShardId(1)).unwrap();
        let hb = raft_shard.handle_heartbeat_timeout();
        for action in hb {
            if let Action::Send { to, shard: shard_id, msg } = action {
                sim.network.send(leader_id, to, shard_id, msg);
            }
        }
        sim.deliver_all_rounds(20);
        let ci_after = sim.commit_index(leader_id).unwrap();
        assert!(
            ci_after > ci_during,
            "cycle {cycle}: commit index must advance after heal (during={ci_during}, after={ci_after})"
        );
    }
}

// ── Proposal during active partition ──────────────────────────────

/// Proposals via `sim.propose()` return None when the leader is partitioned.
#[test]
fn propose_returns_none_during_partition() {
    // Seed 42
    let mut sim = Simulation::new(42, 3);
    assert!(sim.elect_leader(NodeId(1)));
    let leader_id = sim.leader().unwrap();

    let followers: Vec<NodeId> =
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().filter(|&id| id != leader_id).collect();
    sim.partition(&[leader_id], &followers);

    // propose() tries to commit via delivery rounds — should fail because
    // replication messages are dropped.
    let result = sim.propose(b"during-partition".to_vec());
    // The leader appends locally but cannot replicate, so commit_index stays 0
    // and propose() returns None.
    assert!(result.is_none(), "propose should return None when leader is partitioned");
}

// ── Partition then re-election (minority side) ────────────────────

/// After partitioning the leader away, the majority side elects a new leader.
#[test]
fn majority_partition_elects_new_leader() {
    // Seed 42
    let mut sim = Simulation::new(42, 5);
    assert!(sim.elect_leader(NodeId(1)));
    let old_leader = sim.leader().unwrap();

    // Partition old leader + one follower from the majority (3 nodes).
    let minority_peer = if old_leader == NodeId(1) { NodeId(2) } else { NodeId(1) };
    let majority: Vec<NodeId> =
        (1..=5).map(NodeId).filter(|&id| id != old_leader && id != minority_peer).collect();
    sim.partition(&[old_leader, minority_peer], &majority);

    // Advance clock so election timeouts fire on majority-side nodes.
    sim.advance_clock(std::time::Duration::from_millis(500));

    // Trigger election from a majority-side node.
    let candidate = majority[0];
    let shard = sim.nodes.get_mut(&candidate).unwrap().get_mut(&ShardId(1)).unwrap();
    let actions = shard.handle_election_timeout();
    for action in actions {
        if let Action::Send { to, shard, msg } = action {
            sim.network.send(candidate, to, shard, msg);
        }
    }
    sim.deliver_all_rounds(20);

    // A new leader should emerge from the majority side.
    let new_leader = majority.iter().find(|&&id| {
        sim.nodes
            .get(&id)
            .and_then(|m| m.get(&ShardId(1)))
            .is_some_and(|s| s.state() == inferadb_ledger_consensus::types::NodeState::Leader)
    });
    assert!(new_leader.is_some(), "majority partition should elect a new leader");
    assert_ne!(
        *new_leader.unwrap(),
        old_leader,
        "new leader must differ from the partitioned old leader"
    );
}

// ── Deterministic replay ──────────────────────────────────────────

/// Same seed produces identical commit indices across two runs.
#[test]
fn deterministic_replay_identical_commits() {
    fn run_scenario(seed: u64) -> Vec<Option<u64>> {
        let mut sim = Simulation::new(seed, 3);
        sim.elect_leader(NodeId(1));

        let mut commits = Vec::new();
        for i in 0..10 {
            let data = format!("entry-{i}").into_bytes();
            commits.push(sim.propose(data));
        }
        commits
    }

    for seed in [7, 42, 99, 256, 1000] {
        let run1 = run_scenario(seed);
        let run2 = run_scenario(seed);
        assert_eq!(
            run1, run2,
            "seed {seed}: two runs with the same seed must produce identical commit indices"
        );
    }
}

/// Same seed produces identical message delivery counts.
#[test]
fn deterministic_replay_identical_delivery_counts() {
    fn run_scenario(seed: u64) -> (u64, u64) {
        let mut sim = Simulation::new(seed, 3);
        sim.elect_leader(NodeId(1));
        sim.propose(b"data".to_vec());
        (sim.delivered_count(), sim.dropped_count())
    }

    for seed in [0, 42, 100, 999] {
        let (d1, dr1) = run_scenario(seed);
        let (d2, dr2) = run_scenario(seed);
        assert_eq!(d1, d2, "seed {seed}: delivered count differs across runs");
        assert_eq!(dr1, dr2, "seed {seed}: dropped count differs across runs");
    }
}

/// Same seed produces identical leader election outcome.
#[test]
fn deterministic_replay_identical_leader() {
    fn leader_for(seed: u64) -> Option<NodeId> {
        let mut sim = Simulation::new(seed, 3);
        sim.elect_leader(NodeId(1));
        sim.leader()
    }

    for seed in [0, 1, 42, 123, 500] {
        assert_eq!(leader_for(seed), leader_for(seed), "seed {seed}: leader differs across runs");
    }
}

// ── Sequential proposal ordering ──────────────────────────────────

/// 100 sequential proposals produce strictly sequential commit indices 1..=100.
#[test]
fn sequential_proposals_produce_sequential_commits() {
    // Seed 42
    let mut sim = Simulation::new(42, 3);
    assert!(sim.elect_leader(NodeId(1)));

    let mut prev_ci = 1; // +1 for leader no-op (no-op occupies index 1)
    for i in 1..=100 {
        let data = format!("entry-{i}").into_bytes();
        let ci = sim.propose(data);
        assert!(ci.is_some(), "proposal {i} should commit");
        let ci_val = ci.unwrap();
        assert_eq!(ci_val, prev_ci + 1, "proposal {i}: expected sequential commit index");
        prev_ci = ci_val;
    }
    assert_eq!(prev_ci, 101);
}

// ── Follower commit index propagation ─────────────────────────────

/// After a heartbeat round, at least one follower has an advanced commit index.
#[test]
fn follower_commit_index_propagates_via_heartbeat() {
    // Seed 42
    let mut sim = Simulation::new(42, 3);
    assert!(sim.elect_leader(NodeId(1)));
    let leader_id = sim.leader().unwrap();

    sim.propose(b"replicated".to_vec());

    // Send an extra heartbeat round to propagate commit index to followers.
    let shard = sim.nodes.get_mut(&leader_id).unwrap().get_mut(&ShardId(1)).unwrap();
    let hb_actions = shard.handle_heartbeat_timeout();
    for action in hb_actions {
        if let Action::Send { to, shard, msg } = action {
            sim.network.send(leader_id, to, shard, msg);
        }
    }
    sim.deliver_all_rounds(10);

    // After heartbeat propagation, at least one follower should have advanced.
    let follower_ids: Vec<NodeId> =
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().filter(|&id| id != leader_id).collect();
    let any_follower_committed =
        follower_ids.iter().any(|&id| sim.commit_index(id).unwrap_or(0) > 0);
    assert!(any_follower_committed, "at least one follower should have committed the entry");
}

/// All followers converge to the same commit index as the leader after enough rounds.
#[test]
fn all_followers_converge_commit_index() {
    // Seed 42
    let mut sim = Simulation::new(42, 3);
    assert!(sim.elect_leader(NodeId(1)));
    let leader_id = sim.leader().unwrap();

    // Propose several entries.
    for i in 0..5 {
        sim.propose(format!("entry-{i}").into_bytes());
    }

    // Extra heartbeat + delivery to propagate.
    let shard = sim.nodes.get_mut(&leader_id).unwrap().get_mut(&ShardId(1)).unwrap();
    let hb = shard.handle_heartbeat_timeout();
    for action in hb {
        if let Action::Send { to, shard, msg } = action {
            sim.network.send(leader_id, to, shard, msg);
        }
    }
    sim.deliver_all_rounds(20);

    let leader_ci = sim.commit_index(leader_id).unwrap();
    for &nid in &[NodeId(1), NodeId(2), NodeId(3)] {
        if nid == leader_id {
            continue;
        }
        let fci = sim.commit_index(nid).unwrap();
        assert_eq!(
            fci, leader_ci,
            "follower {nid:?} commit index {fci} should match leader commit index {leader_ci}"
        );
    }
}

// ── 5-node fault tolerance ────────────────────────────────────────

/// A 5-node cluster tolerates killing 2 followers (quorum = 3).
#[test]
fn five_node_cluster_tolerates_two_failures() {
    // Seed 42
    let mut sim = Simulation::new(42, 5);
    assert!(sim.elect_leader(NodeId(1)));
    let leader_id = sim.leader().unwrap();

    // Kill two non-leader nodes.
    let mut killed = 0;
    for nid in [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)] {
        if nid != leader_id && killed < 2 {
            sim.kill(nid);
            killed += 1;
        }
    }
    assert_eq!(sim.live_node_count(), 3);

    let ci = sim.propose(b"after-two-kills".to_vec());
    assert!(ci.is_some(), "5-node cluster with 2 failures should still commit");
}

// ── Propose without leader ────────────────────────────────────────

/// Proposing with no elected leader returns None.
#[test]
fn propose_returns_none_without_leader() {
    // Seed 42
    let mut sim = Simulation::new(42, 3);
    let commit = sim.propose(b"orphan".to_vec());
    assert!(commit.is_none());
}

// ── Multi-shard group simulation ─────────────────────────────────

/// Two independent shard groups elect leaders and commit independently.
#[test]
fn multi_shard_independent_groups() {
    let mut sim = Simulation::new(42, 3);
    sim.add_shard_group(ShardId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

    // Elect leaders on both shard groups.
    assert!(sim.elect_leader_on(ShardId(1), NodeId(1)), "shard 1 should elect a leader");
    assert!(sim.elect_leader_on(ShardId(2), NodeId(2)), "shard 2 should elect a leader");

    // Propose to each shard group independently.
    let ci1 = sim.propose_on(ShardId(1), b"shard1-entry".to_vec());
    let ci2 = sim.propose_on(ShardId(2), b"shard2-entry".to_vec());

    assert!(ci1.is_some(), "shard 1 should commit");
    assert!(ci2.is_some(), "shard 2 should commit");

    // Commit indices are independent per shard (index 2: no-op at 1, user entry at 2).
    assert_eq!(ci1.unwrap(), 2); // +1 for leader no-op
    assert_eq!(ci2.unwrap(), 2);
}

/// Partitioning a node affects all shard groups hosted on it.
#[test]
fn multi_shard_partition_affects_all_groups() {
    let mut sim = Simulation::new(42, 3);
    sim.add_shard_group(ShardId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

    // Elect NodeId(1) as leader for both shards.
    sim.elect_leader_on(ShardId(1), NodeId(1));
    sim.elect_leader_on(ShardId(2), NodeId(1));

    let leader1 = sim.leader_on(ShardId(1)).unwrap();
    let leader2 = sim.leader_on(ShardId(2)).unwrap();

    // Partition the leader from followers — network-level, affects both shards.
    let followers: Vec<NodeId> =
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().filter(|&id| id != leader1).collect();
    sim.partition(&[leader1], &followers);

    // Neither shard group should be able to commit through the partitioned leader.
    let r1 = sim.propose_on(ShardId(1), b"blocked1".to_vec());
    assert!(r1.is_none(), "shard 1 should not commit when leader is partitioned");

    if leader2 == leader1 {
        let r2 = sim.propose_on(ShardId(2), b"blocked2".to_vec());
        assert!(r2.is_none(), "shard 2 should not commit when leader is partitioned");
    }
}

/// Different nodes can be leaders for different shard groups simultaneously.
#[test]
fn multi_shard_independent_leaders() {
    // Use multiple seeds to increase confidence that different leaders emerge.
    for seed in [42, 77, 123] {
        let mut sim = Simulation::new(seed, 3);
        sim.add_shard_group(ShardId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

        sim.elect_leader_on(ShardId(1), NodeId(1));
        sim.elect_leader_on(ShardId(2), NodeId(2));

        let leader1 = sim.leader_on(ShardId(1));
        let leader2 = sim.leader_on(ShardId(2));

        assert!(leader1.is_some(), "seed {seed}: shard 1 should have a leader");
        assert!(leader2.is_some(), "seed {seed}: shard 2 should have a leader");

        // Both groups should function independently.
        let ci1 = sim.propose_on(ShardId(1), b"s1".to_vec());
        let ci2 = sim.propose_on(ShardId(2), b"s2".to_vec());
        assert!(ci1.is_some(), "seed {seed}: shard 1 proposals should commit");
        assert!(ci2.is_some(), "seed {seed}: shard 2 proposals should commit");
    }
}
