//! Replication integration tests for the consensus engine.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods, clippy::panic)]

mod common;

use inferadb_ledger_consensus::*;

// ── Single proposal ────────────────────────────────────────────────

#[test]
fn replication_single_proposal_commits_on_leader() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let commit = cluster.propose(b"hello world".to_vec());

    assert_eq!(commit, 2); // +1 for leader no-op (Raft §5.4.2)
}

#[test]
fn replication_single_proposal_replicates_to_all_followers() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    cluster.propose(b"data".to_vec());

    for (&id, shard) in &cluster.shards {
        assert_eq!(shard.log_len(), 2, "node {id:?} should have 2 log entries"); // +1 for leader no-op (Raft §5.4.2)
    }
}

// ── Sequential proposals ───────────────────────────────────────────

#[test]
fn replication_sequential_proposals_advance_commit_index_monotonically() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    for i in 0..10 {
        let commit = cluster.propose(format!("entry-{i}").into_bytes());
        assert_eq!(commit, i + 2, "commit index must advance monotonically"); // +1 for leader no-op (Raft §5.4.2)
    }
}

#[test]
fn replication_sequential_proposals_produce_identical_logs_across_nodes() {
    let mut cluster = common::TestCluster::new(5);
    cluster.elect_leader(NodeId(1));

    for i in 0..20 {
        cluster.propose(format!("entry-{i}").into_bytes());
    }

    let expected_len = cluster.shards[&NodeId(1)].log_len();
    for (&id, shard) in &cluster.shards {
        assert_eq!(shard.log_len(), expected_len, "node {id:?} log length mismatch");
    }
}

// ── Batch proposals ────────────────────────────────────────────────

#[test]
fn replication_batch_proposal_commits_all_entries() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let batch: Vec<Vec<u8>> = (0..100).map(|i| format!("batch-{i}").into_bytes()).collect();
    let commit = cluster.propose_batch(batch);

    assert_eq!(commit, 101); // +1 for leader no-op (Raft §5.4.2)
}

#[test]
fn replication_large_batch_of_1000_entries_commits() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let batch: Vec<Vec<u8>> = (0..1000).map(|i| format!("entry-{i}").into_bytes()).collect();
    let commit = cluster.propose_batch(batch);

    assert_eq!(commit, 1001); // +1 for leader no-op (Raft §5.4.2)
    for (&id, shard) in &cluster.shards {
        assert_eq!(shard.log_len(), 1001, "node {id:?} should have 1001 log entries"); // +1 for leader no-op (Raft §5.4.2)
    }
}

#[test]
fn replication_empty_batch_does_not_advance_commit_index() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let commit = cluster.propose_batch(Vec::new());

    assert_eq!(commit, 1, "empty batch should not advance commit index beyond no-op"); // +1 for leader no-op (Raft §5.4.2)
}

// ── Data size variations ───────────────────────────────────────────

#[test]
fn replication_empty_payload_commits() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let commit = cluster.propose(Vec::new());

    assert_eq!(commit, 2, "empty payload should still commit"); // +1 for leader no-op (Raft §5.4.2)
}

#[test]
fn replication_single_byte_payload_commits() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let commit = cluster.propose(vec![0x42]);

    assert_eq!(commit, 2); // +1 for leader no-op (Raft §5.4.2)
}

#[test]
fn replication_large_payload_commits() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let large_payload = vec![0xAB; 64 * 1024]; // 64 KiB
    let commit = cluster.propose(large_payload);

    assert_eq!(commit, 2); // +1 for leader no-op (Raft §5.4.2)
    for shard in cluster.shards.values() {
        assert_eq!(shard.log_len(), 2); // +1 for leader no-op (Raft §5.4.2)
    }
}

// ── Commit index propagation ───────────────────────────────────────

#[test]
fn replication_followers_advance_commit_index_via_subsequent_appends() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Followers learn the leader's commit index via the leader_commit field
    // in the *next* AppendEntries. After three proposals, each follower has
    // seen the commit index from the previous round.
    cluster.propose(b"first".to_vec());
    cluster.propose(b"second".to_vec());
    cluster.propose(b"third".to_vec());

    // Leader has committed all three proposals plus the no-op.
    let leader = &cluster.shards[&NodeId(1)];
    assert_eq!(leader.commit_index(), 4); // +1 for leader no-op (Raft §5.4.2)

    // Followers have committed at least 3 (they learn of commit 4 on the
    // next heartbeat/append, which hasn't been sent yet).
    for (&id, shard) in &cluster.shards {
        if id != NodeId(1) {
            assert!(
                shard.commit_index() >= 3,
                "follower {id:?} commit index {} should be >= 3",
                shard.commit_index()
            );
        }
    }
}

// ── Leader re-election continuity ──────────────────────────────────

#[test]
fn replication_propose_succeeds_after_leader_transfer() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Propose before transfer to establish log state.
    cluster.propose(b"before-transfer".to_vec());

    // Transfer leadership to node 2.
    cluster.transfer_leader(NodeId(2)).expect("transfer failed");
    assert_eq!(cluster.leader_id(), Some(NodeId(2)));

    // Propose on the new leader.
    let commit = cluster.propose(b"after-transfer".to_vec());

    // The new leader should have committed both entries.
    assert!(commit >= 2, "new leader commit index {commit} should be >= 2");
}

// ── Learner replication ────────────────────────────────────────────

#[test]
fn replication_learner_receives_entries_after_add() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Propose some data before adding the learner.
    cluster.propose(b"pre-learner".to_vec());

    // Add a learner node (must create the shard instance first).
    cluster.add_node(NodeId(4), Membership::new([NodeId(4)]));

    cluster
        .propose_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: false,
            expected_conf_epoch: None,
        })
        .expect("add learner failed");

    // Propose more data after the learner is added.
    cluster.propose(b"post-learner".to_vec());

    // The learner should have received log entries from the leader.
    let learner = &cluster.shards[&NodeId(4)];
    assert!(learner.log_len() > 0, "learner should have received log entries");
}

// ── Error cases ────────────────────────────────────────────────────

#[test]
fn replication_propose_on_follower_returns_not_leader() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let follower = cluster.shards.get_mut(&NodeId(2)).expect("follower not found");
    let result = follower.handle_propose(b"should fail".to_vec());

    assert!(result.is_err());
}

#[test]
fn replication_batch_propose_on_follower_returns_not_leader() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let follower = cluster.shards.get_mut(&NodeId(2)).unwrap();
    let result = follower.handle_propose_batch(vec![b"a".to_vec()]);

    assert!(result.is_err());
}

#[test]
fn replication_propose_on_failed_node_returns_unavailable() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    cluster.shards.get_mut(&NodeId(1)).unwrap().mark_failed();
    let result = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_propose(b"data".to_vec());

    assert!(result.is_err());
}
