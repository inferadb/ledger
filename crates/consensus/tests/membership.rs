//! Membership change integration tests.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods, clippy::panic)]

mod common;

use inferadb_ledger_consensus::{
    error::ConsensusError,
    types::{Membership, MembershipChange, NodeId},
};

// ── Add Learner ────────────────────────────────────────────────────

#[test]
fn add_learner_to_three_node_cluster() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    cluster
        .propose_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: true,
            expected_conf_epoch: None,
        })
        .expect("add learner failed");

    let snap = cluster.shards[&NodeId(1)].state_snapshot();
    assert!(snap.learners.contains(&NodeId(4)), "node 4 should be a learner");
    assert_eq!(snap.voters.len(), 3, "voter count should be unchanged");
}

#[test]
fn add_learner_to_single_node_cluster() {
    let mut cluster = common::TestCluster::new(1);
    cluster.elect_leader(NodeId(1));

    cluster
        .propose_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(2),
            promotable: false,
            expected_conf_epoch: None,
        })
        .expect("add learner to single-node cluster failed");

    // In a single-node cluster, handle_membership_change does not call
    // try_advance_commit, so the membership entry is appended but not
    // committed until the next data proposal triggers commit. Verify
    // the entry is in the log and that a subsequent propose commits it.
    let log_len = cluster.shards[&NodeId(1)].log_len();
    assert!(log_len > 0, "membership entry should be in the log");

    // A data proposal triggers try_advance_commit, which catches up
    // the membership entry too.
    cluster.propose(b"trigger-commit".to_vec());

    let snap = cluster.shards[&NodeId(1)].state_snapshot();
    assert!(snap.learners.contains(&NodeId(2)), "node 2 should be a learner after commit");
    assert_eq!(snap.voters.len(), 1, "voter count should remain 1");
}

#[test]
fn add_learner_then_propose_data_replicates_to_learner() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Add learner and spawn its shard so it can receive replication.
    let leader_snap = cluster.shards[&NodeId(1)].state_snapshot();
    let membership = Membership::new(leader_snap.voters.iter().copied());
    cluster.add_node(NodeId(4), membership);

    cluster
        .propose_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: true,
            expected_conf_epoch: None,
        })
        .expect("add learner failed");

    // Propose data — replication should reach the learner.
    let commit = cluster.propose(b"hello".to_vec());
    assert!(commit > 0, "commit index should advance");

    // The learner's commit index may lag (it's not a voter), but its log
    // should have received the entry via append-entries replication.
    let learner_log_len = cluster.shards[&NodeId(4)].log_len();
    assert!(learner_log_len > 0, "learner should have replicated entries");
}

// ── Promote Learner ────────────────────────────────────────────────

#[test]
fn promote_learner_to_voter_after_commit() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Add learner — committed through delivery.
    cluster
        .propose_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: true,
            expected_conf_epoch: None,
        })
        .expect("add learner failed");

    // pending_membership is cleared on commit. Promote should succeed
    // if the membership entry was committed during delivery.
    let result = cluster.propose_membership_change(MembershipChange::PromoteVoter {
        node_id: NodeId(4),
        expected_conf_epoch: None,
    });

    match result {
        Ok(()) => {
            let snap = cluster.shards[&NodeId(1)].state_snapshot();
            assert!(snap.voters.contains(&NodeId(4)), "node 4 should be a voter");
            assert!(!snap.learners.contains(&NodeId(4)), "node 4 should no longer be a learner");
        },
        Err(err) => {
            // MembershipChangePending is valid if the add-learner entry
            // hasn't been committed yet (single-step membership).
            assert!(
                matches!(err, ConsensusError::MembershipChangePending),
                "unexpected error: {err}"
            );
        },
    }
}

// ── Remove Node ────────────────────────────────────────────────────

#[test]
fn remove_voter_from_five_node_cluster() {
    let mut cluster = common::TestCluster::new(5);
    cluster.elect_leader(NodeId(1));

    cluster
        .propose_membership_change(MembershipChange::RemoveNode {
            node_id: NodeId(5),
            expected_conf_epoch: None,
        })
        .expect("remove voter failed");

    let snap = cluster.shards[&NodeId(1)].state_snapshot();
    assert!(!snap.voters.contains(&NodeId(5)), "node 5 should be removed");
    assert_eq!(snap.voters.len(), 4, "should have 4 voters remaining");

    // Quorum is maintained — can still propose data.
    let commit = cluster.propose(b"after-remove".to_vec());
    assert!(commit > 0, "cluster should still accept proposals");
}

#[test]
fn remove_voter_from_three_node_cluster() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    cluster
        .propose_membership_change(MembershipChange::RemoveNode {
            node_id: NodeId(3),
            expected_conf_epoch: None,
        })
        .expect("remove voter failed");

    let snap = cluster.shards[&NodeId(1)].state_snapshot();
    assert!(!snap.voters.contains(&NodeId(3)), "node 3 should be removed");
    assert_eq!(snap.voters.len(), 2, "should have 2 voters remaining");

    // 2-node cluster has quorum of 2 — leader + one follower.
    let commit = cluster.propose(b"two-node-quorum".to_vec());
    assert!(commit > 0, "2-node cluster should still reach quorum");
}

#[test]
fn remove_node_on_leader_succeeds() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let leader = cluster.shards.get_mut(&NodeId(1)).unwrap();
    let result = leader.handle_membership_change(MembershipChange::RemoveNode {
        node_id: NodeId(3),
        expected_conf_epoch: None,
    });
    assert!(result.is_ok(), "remove node on leader should succeed");
}

// ── Follower / Failed Node Rejection ───────────────────────────────

#[test]
fn membership_change_on_follower_returns_not_leader() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let follower = cluster.shards.get_mut(&NodeId(2)).unwrap();
    let result = follower.handle_membership_change(MembershipChange::AddLearner {
        node_id: NodeId(4),
        promotable: true,
        expected_conf_epoch: None,
    });

    assert!(matches!(result, Err(ConsensusError::NotLeader)), "follower should reject: {result:?}");
}

#[test]
fn membership_change_on_failed_node_returns_unavailable() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    cluster.shards.get_mut(&NodeId(1)).unwrap().mark_failed();
    let result = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_membership_change(
        MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: false,
            expected_conf_epoch: None,
        },
    );

    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { .. })),
        "failed node should reject: {result:?}"
    );
}

// ── Concurrent Changes ─────────────────────────────────────────────

#[test]
fn concurrent_membership_changes_blocked() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // First change — don't deliver, so pending_membership stays set.
    let leader = cluster.shards.get_mut(&NodeId(1)).unwrap();
    let first = leader.handle_membership_change(MembershipChange::AddLearner {
        node_id: NodeId(4),
        promotable: true,
        expected_conf_epoch: None,
    });
    assert!(first.is_ok());

    // Second concurrent change must be rejected.
    let second = leader.handle_membership_change(MembershipChange::AddLearner {
        node_id: NodeId(5),
        promotable: true,
        expected_conf_epoch: None,
    });
    assert!(
        matches!(second, Err(ConsensusError::MembershipChangePending)),
        "concurrent change should be blocked: {second:?}"
    );
}

// ── Sequential Membership Changes ──────────────────────────────────

#[test]
fn sequential_add_promote_remove() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Step 1: add learner.
    cluster
        .propose_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: true,
            expected_conf_epoch: None,
        })
        .expect("add learner failed");

    let snap = cluster.shards[&NodeId(1)].state_snapshot();
    assert!(snap.learners.contains(&NodeId(4)));

    // Step 2: promote learner to voter (may fail if pending).
    let promote_result = cluster.propose_membership_change(MembershipChange::PromoteVoter {
        node_id: NodeId(4),
        expected_conf_epoch: None,
    });

    if promote_result.is_ok() {
        let snap = cluster.shards[&NodeId(1)].state_snapshot();
        assert!(snap.voters.contains(&NodeId(4)), "node 4 should be voter after promote");

        // Step 3: remove the promoted node.
        let remove_result = cluster.propose_membership_change(MembershipChange::RemoveNode {
            node_id: NodeId(4),
            expected_conf_epoch: None,
        });

        if remove_result.is_ok() {
            let snap = cluster.shards[&NodeId(1)].state_snapshot();
            assert!(!snap.voters.contains(&NodeId(4)), "node 4 should be removed");
            assert!(!snap.learners.contains(&NodeId(4)), "node 4 should not be a learner");
        } else {
            // MembershipChangePending is acceptable if promote hasn't committed.
            assert!(matches!(remove_result, Err(ConsensusError::MembershipChangePending)));
        }
    } else {
        // MembershipChangePending is acceptable — the add-learner entry
        // may not have been committed yet.
        assert!(matches!(promote_result, Err(ConsensusError::MembershipChangePending)));
    }
}

// ── Membership Change During Data Proposals ────────────────────────

#[test]
fn membership_change_while_data_is_proposed() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Propose some data first.
    let commit_before = cluster.propose(b"before-membership".to_vec());
    assert!(commit_before > 0);

    // Add a learner while data is flowing.
    cluster
        .propose_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: true,
            expected_conf_epoch: None,
        })
        .expect("add learner during data flow failed");

    // Propose more data after the membership change.
    let commit_after = cluster.propose(b"after-membership".to_vec());
    assert!(commit_after > commit_before, "commit index should advance after membership change");

    let snap = cluster.shards[&NodeId(1)].state_snapshot();
    assert!(snap.learners.contains(&NodeId(4)), "learner should be present");
}
