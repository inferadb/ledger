//! Election integration tests for the consensus engine.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods, clippy::panic)]

mod common;

use inferadb_ledger_consensus::*;

// ── Basic Election ─────────────────────────────────────────────────

#[test]
fn election_three_node_cluster_elects_leader_at_term_one() {
    // Arrange
    let mut cluster = common::TestCluster::new(3);

    // Act
    cluster.elect_leader(NodeId(1));

    // Assert — leader state
    assert!(cluster.has_leader());
    assert_eq!(cluster.leader_id(), Some(NodeId(1)));
    assert_eq!(cluster.shards[&NodeId(1)].current_term(), 1);
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::Leader);

    // Assert — follower state
    assert_eq!(cluster.follower_count(), 2);
    for (&id, shard) in &cluster.shards {
        if id != NodeId(1) {
            assert_eq!(shard.current_term(), 1, "follower {id:?} should be at term 1");
            assert_eq!(shard.state(), NodeState::Follower);
        }
    }
}

#[test]
fn election_five_node_cluster_elects_designated_candidate() {
    // Arrange
    let mut cluster = common::TestCluster::new(5);

    // Act
    cluster.elect_leader(NodeId(3));

    // Assert
    assert!(cluster.has_leader());
    assert_eq!(cluster.leader_id(), Some(NodeId(3)));
    assert_eq!(cluster.follower_count(), 4);
    assert_eq!(cluster.shards[&NodeId(3)].current_term(), 1);
}

#[test]
fn election_seven_node_cluster_elects_leader() {
    // Arrange
    let mut cluster = common::TestCluster::new(7);

    // Act
    cluster.elect_leader(NodeId(5));

    // Assert
    assert!(cluster.has_leader());
    assert_eq!(cluster.leader_id(), Some(NodeId(5)));
    assert_eq!(cluster.follower_count(), 6);
}

#[test]
fn election_single_node_cluster_self_elects() {
    // Arrange
    let mut cluster = common::TestCluster::new(1);

    // Act
    cluster.elect_leader(NodeId(1));

    // Assert
    assert!(cluster.has_leader());
    assert_eq!(cluster.leader_id(), Some(NodeId(1)));
    assert_eq!(cluster.shards[&NodeId(1)].current_term(), 1);
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::Leader);
}

// ── Pre-vote Protocol ──────────────────────────────────────────────

#[test]
fn election_timeout_transitions_follower_to_pre_candidate() {
    // Arrange — 3-node cluster, no initial election
    let mut cluster = common::TestCluster::new(3);
    cluster.clock.advance(std::time::Duration::from_secs(1));

    // Act — trigger election timeout without delivering messages
    let actions = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_election_timeout();

    // Assert — node is PreCandidate and sends PreVoteRequests
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::PreCandidate);
    // Term is NOT incremented during pre-vote
    assert_eq!(cluster.shards[&NodeId(1)].current_term(), 0);

    let pre_vote_count = actions
        .iter()
        .filter(|a| matches!(a, Action::Send { msg: Message::PreVoteRequest { .. }, .. }))
        .count();
    assert_eq!(pre_vote_count, 2, "should send PreVoteRequest to 2 peers");
}

#[test]
fn election_pre_vote_does_not_increment_term() {
    // Arrange
    let mut cluster = common::TestCluster::new(3);
    cluster.clock.advance(std::time::Duration::from_secs(1));

    // Act — start pre-vote
    let _actions = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_election_timeout();

    // Assert — term unchanged (pre-vote uses prospective term = current + 1)
    assert_eq!(cluster.shards[&NodeId(1)].current_term(), 0);
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::PreCandidate);
}

// ── Re-election After Leader Failure ───────────────────────────────

#[test]
fn election_re_election_after_leader_failure() {
    // Arrange — establish a leader
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));
    assert_eq!(cluster.leader_id(), Some(NodeId(1)));

    // Act — fail the leader, then a follower starts a new election
    cluster.shards.get_mut(&NodeId(1)).unwrap().mark_failed();
    cluster.elect_leader(NodeId(2));

    // Assert
    assert_eq!(cluster.leader_id(), Some(NodeId(2)));
    assert_eq!(cluster.shards[&NodeId(2)].state(), NodeState::Leader);
    // Term advanced: original election was term 1, re-election is term 2
    assert_eq!(cluster.shards[&NodeId(2)].current_term(), 2);
}

// ── Term Advancement ───────────────────────────────────────────────

#[test]
fn election_successive_elections_advance_term() {
    // Arrange
    let mut cluster = common::TestCluster::new(3);

    // Act — first election
    cluster.elect_leader(NodeId(1));
    assert_eq!(cluster.shards[&NodeId(1)].current_term(), 1);

    // Transfer leadership to force a new election at a higher term
    cluster.transfer_leader(NodeId(2)).expect("transfer should succeed");
    let term_after_transfer = cluster.shards[&NodeId(2)].current_term();

    // Transfer again
    cluster.transfer_leader(NodeId(3)).expect("transfer should succeed");
    let term_after_second = cluster.shards[&NodeId(3)].current_term();

    // Assert — each transfer increments the term
    assert!(term_after_transfer > 1);
    assert!(term_after_second > term_after_transfer);
}

// ── Step-down on Higher Term ───────────────────────────────────────

#[test]
fn election_leader_steps_down_on_higher_term_append_entries() {
    // Arrange
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::Leader);

    // Act — simulate AppendEntries from a higher-term leader
    let actions = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_message(
        NodeId(2),
        Message::AppendEntries {
            term: 5,
            leader_id: NodeId(2),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: std::sync::Arc::from(Vec::new()),
            leader_commit: 0,
            closed_ts_nanos: 0,
        },
    );

    // Assert
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::Follower);
    assert_eq!(cluster.shards[&NodeId(1)].current_term(), 5);
    assert!(!actions.is_empty(), "should produce AppendEntriesResponse");
}

#[test]
fn election_candidate_steps_down_on_higher_term_vote_response() {
    // Arrange — put node 1 into Candidate state
    let mut cluster = common::TestCluster::new(3);
    cluster.clock.advance(std::time::Duration::from_secs(1));
    let _actions = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_election_timeout();

    // Deliver pre-vote grants to move to Candidate
    let prospective_term = cluster.shards[&NodeId(1)].current_term() + 1;
    cluster.shards.get_mut(&NodeId(1)).unwrap().handle_message(
        NodeId(2),
        Message::PreVoteResponse { term: prospective_term, vote_granted: true },
    );
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::Candidate);
    let candidate_term = cluster.shards[&NodeId(1)].current_term();

    // Act — receive VoteResponse with a higher term
    let _actions = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_message(
        NodeId(2),
        Message::VoteResponse { term: candidate_term + 5, vote_granted: false },
    );

    // Assert — step-down to Follower with the higher term.
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::Follower);
    assert_eq!(cluster.shards[&NodeId(1)].current_term(), candidate_term + 5);
}

// ── Election Suppression ───────────────────────────────────────────

#[test]
fn election_leader_ignores_election_timeout() {
    // Arrange
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Act
    let actions = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_election_timeout();

    // Assert
    assert!(actions.is_empty(), "leader must not start election on timeout");
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::Leader);
}

#[test]
fn election_failed_node_ignores_election_timeout() {
    // Arrange
    let mut cluster = common::TestCluster::new(3);
    cluster.shards.get_mut(&NodeId(1)).unwrap().mark_failed();

    // Act
    let actions = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_election_timeout();

    // Assert
    assert!(actions.is_empty(), "failed node must not start elections");
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::Failed);
}

// ── Leader Transfer ────────────────────────────────────────────────

#[test]
fn election_leader_transfer_changes_leader() {
    // Arrange
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));
    assert_eq!(cluster.leader_id(), Some(NodeId(1)));

    // Act
    cluster.transfer_leader(NodeId(2)).expect("transfer should succeed");

    // Assert
    assert_eq!(cluster.leader_id(), Some(NodeId(2)));
    assert_eq!(cluster.shards[&NodeId(1)].state(), NodeState::Follower);
}

#[test]
fn election_leader_transfer_to_self_returns_error() {
    // Arrange
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Act
    let result = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_transfer_leader(NodeId(1));

    // Assert
    assert!(result.is_err());
}

#[test]
fn election_leader_transfer_to_unknown_node_returns_error() {
    // Arrange
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Act
    let result = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_transfer_leader(NodeId(99));

    // Assert
    assert!(result.is_err());
}

#[test]
fn election_leader_transfer_skips_pre_vote() {
    // Arrange
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Act — transfer sends TimeoutNow which triggers start_election directly
    let transfer_actions = cluster
        .shards
        .get_mut(&NodeId(1))
        .unwrap()
        .handle_transfer_leader(NodeId(2))
        .expect("transfer should succeed");

    // Verify TimeoutNow is sent
    let timeout_now_sent = transfer_actions.iter().any(
        |a| matches!(a, Action::Send { to, msg: Message::TimeoutNow, .. } if *to == NodeId(2)),
    );
    assert!(timeout_now_sent, "transfer should send TimeoutNow to target");

    // Deliver TimeoutNow — target should go directly to Candidate (not PreCandidate)
    let response_actions =
        cluster.shards.get_mut(&NodeId(2)).unwrap().handle_message(NodeId(1), Message::TimeoutNow);

    // Assert — target sends VoteRequests (not PreVoteRequests)
    let vote_requests = response_actions
        .iter()
        .filter(|a| matches!(a, Action::Send { msg: Message::VoteRequest { .. }, .. }))
        .count();
    let pre_vote_requests = response_actions
        .iter()
        .filter(|a| matches!(a, Action::Send { msg: Message::PreVoteRequest { .. }, .. }))
        .count();
    assert!(vote_requests > 0, "should send VoteRequests after TimeoutNow");
    assert_eq!(pre_vote_requests, 0, "should skip PreVoteRequests after TimeoutNow");
    assert_eq!(cluster.shards[&NodeId(2)].state(), NodeState::Candidate);
}

// ── Vote Rejection ─────────────────────────────────────────────────

#[test]
fn election_vote_rejected_when_candidate_log_is_behind() {
    // Arrange — elect leader, propose an entry, then try election from a node
    //           that doesn't have the entry
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));
    cluster.propose(b"entry1".to_vec());

    // Node 3 should have the replicated entry. But if we craft a VoteRequest
    // from an imaginary candidate with an empty log, it should be rejected.
    let actions = cluster.shards.get_mut(&NodeId(3)).unwrap().handle_message(
        NodeId(2),
        Message::VoteRequest {
            term: 10,
            candidate_id: NodeId(2),
            last_log_index: 0,
            last_log_term: 0,
        },
    );

    // Assert — node 3 steps down to term 10 but rejects the vote (candidate log behind)
    let vote_response = actions.iter().find_map(|a| match a {
        Action::Send { msg: Message::VoteResponse { vote_granted, .. }, .. } => Some(*vote_granted),
        _ => None,
    });
    // The vote may or may not be granted depending on whether node 3 already
    // voted in term 10. The key invariant: if the candidate log is behind,
    // vote_granted must be false.
    assert_eq!(vote_response, Some(false), "vote should be rejected when candidate log is behind");
}

// ── Non-voter Election Guard ───────────────────────────────────────

#[test]
fn election_non_voter_does_not_start_pre_vote() {
    use std::sync::Arc;

    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Capture the current term before the membership change.
    let term = cluster.shards[&NodeId(1)].current_term();

    // Remove node 2 from the voter set via the leader.
    let actions = cluster
        .shards
        .get_mut(&NodeId(1))
        .unwrap()
        .handle_membership_change(MembershipChange::RemoveNode {
            node_id: NodeId(2),
            expected_conf_epoch: None,
        })
        .unwrap();
    // Deliver to nodes — this propagates AppendEntries with the membership entry.
    // After the round-trip the leader commits, but followers only learn the new
    // commit_index on the next heartbeat.
    cluster.deliver_actions_public(NodeId(1), actions);

    // Send a heartbeat from the leader to node 2 with the updated commit index so
    // that node 2 commits the membership entry and updates its local membership.
    // At this point the leader's commit_index is 2 (no-op + membership entry).
    let commit_index = cluster.shards[&NodeId(1)].commit_index();
    cluster.shards.get_mut(&NodeId(2)).unwrap().handle_message(
        NodeId(1),
        Message::AppendEntries {
            term,
            leader_id: NodeId(1),
            prev_log_index: commit_index,
            prev_log_term: term,
            entries: Arc::from(Vec::new()),
            leader_commit: commit_index,
            closed_ts_nanos: 0,
        },
    );

    // Advance clock past election timeout on node 2 (now a non-voter).
    cluster.clock.advance(std::time::Duration::from_secs(2));
    let actions = cluster.shards.get_mut(&NodeId(2)).unwrap().handle_election_timeout();

    // Non-voter should NOT send PreVoteRequest or VoteRequest messages.
    let election_sends = actions.iter().filter(|a| {
        matches!(
            a,
            Action::Send { msg: Message::PreVoteRequest { .. } | Message::VoteRequest { .. }, .. }
        )
    });
    assert_eq!(election_sends.count(), 0, "Non-voter should not initiate elections");

    // Should still schedule a timer (stays alive as silent follower).
    assert!(
        actions.iter().any(|a| matches!(a, Action::ScheduleTimer { .. })),
        "Non-voter should still schedule election timer"
    );
}

// ── Split Vote / Competing Candidates ──────────────────────────────

#[test]
fn election_two_simultaneous_candidates_one_wins_after_retry() {
    // Arrange — 5-node cluster: nodes 1 and 3 both timeout simultaneously
    let mut cluster = common::TestCluster::new(5);
    cluster.clock.advance(std::time::Duration::from_secs(1));

    // Both start pre-vote at the same time
    let actions_1 = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_election_timeout();
    let actions_3 = cluster.shards.get_mut(&NodeId(3)).unwrap().handle_election_timeout();

    // Deliver actions from both — votes may split
    cluster.deliver_actions_public(NodeId(1), actions_1);
    cluster.deliver_actions_public(NodeId(3), actions_3);

    // If no leader yet, trigger another election timeout on one node
    if !cluster.has_leader() {
        cluster.clock.advance(std::time::Duration::from_secs(1));
        cluster.elect_leader(NodeId(1));
    }

    // Assert — eventually a leader is elected
    assert!(cluster.has_leader(), "cluster must eventually elect a leader");
    assert_eq!(cluster.follower_count(), 4);
}

// ── Membership Change Precondition (Ongaro §4.1) ───────────────────

#[test]
fn election_membership_change_rejected_before_noop_commits() {
    use std::sync::Arc;

    use inferadb_ledger_consensus::{
        clock::SimulatedClock, config::ShardConfig, error::ConsensusError, message::Message,
        rng::SimulatedRng, consensus_state::ConsensusState, types::Membership,
    };

    // Build a 3-node shard directly so we can control message delivery precisely.
    let clock = Arc::new(SimulatedClock::new());
    let membership = Membership::new([NodeId(1), NodeId(2), NodeId(3)]);
    let config = ShardConfig::default();
    let mut shard = ConsensusState::new(
        ConsensusStateId(0),
        NodeId(1),
        membership,
        config,
        clock.clone(),
        SimulatedRng::new(42),
        0,
        None,
        0,
    );

    // Trigger election timeout to start pre-vote.
    clock.advance(std::time::Duration::from_secs(2));
    let _actions = shard.handle_election_timeout();
    assert_eq!(shard.state(), NodeState::PreCandidate);

    // Grant pre-vote from node 2 — quorum reached, node 1 transitions to Candidate.
    let _actions =
        shard.handle_message(NodeId(2), Message::PreVoteResponse { term: 0, vote_granted: true });

    // Grant real vote from node 2 — quorum reached, node 1 becomes Leader.
    let _actions =
        shard.handle_message(NodeId(2), Message::VoteResponse { term: 1, vote_granted: true });
    assert_eq!(shard.state(), NodeState::Leader, "node 1 should be leader after vote quorum");

    // The no-op has been appended but NOT committed: AppendEntriesResponse from
    // peers has not been delivered, so commit_index has not advanced.
    assert_eq!(shard.commit_index(), 0, "no-op not yet committed");

    // Membership change must be rejected (Ongaro §4.1 guard).
    let result = shard.handle_membership_change(MembershipChange::AddLearner {
        node_id: NodeId(4),
        promotable: false,
        expected_conf_epoch: None,
    });
    assert!(
        matches!(result, Err(ConsensusError::LeaderNotReady)),
        "expected LeaderNotReady before no-op commits, got {result:?}"
    );

    // Simulate quorum acknowledgement of the no-op by delivering an
    // AppendEntriesResponse with match_index == log length.
    let log_len = shard.log_len();
    let _actions = shard.handle_message(
        NodeId(2),
        Message::AppendEntriesResponse { term: 1, success: true, match_index: log_len },
    );

    // No-op should now be committed.
    assert!(shard.commit_index() > 0, "no-op should be committed after AppendEntriesResponse");

    // Membership change must now succeed.
    let result = shard.handle_membership_change(MembershipChange::AddLearner {
        node_id: NodeId(4),
        promotable: false,
        expected_conf_epoch: None,
    });
    assert!(result.is_ok(), "membership change should succeed after no-op commits: {result:?}");
}

// ── ConsensusState Removed on Membership Eviction ───────────────────────────

#[test]
fn election_removed_node_emits_shard_removed() {
    use std::sync::Arc;

    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    let term = cluster.shards[&NodeId(1)].current_term();

    // Propose removal of node 2 on the leader and deliver the replication messages.
    // After delivery the leader commits, but node 2 only learns the commit_index on
    // the next AppendEntries (followers don't see leader_commit until the next round).
    let actions = cluster
        .shards
        .get_mut(&NodeId(1))
        .unwrap()
        .handle_membership_change(MembershipChange::RemoveNode {
            node_id: NodeId(2),
            expected_conf_epoch: None,
        })
        .unwrap();
    cluster.deliver_actions_public(NodeId(1), actions);

    // Send a heartbeat from the leader to node 2 with the updated commit index so
    // node 2 commits the membership entry and applies the new membership locally.
    let commit_index = cluster.shards[&NodeId(1)].commit_index();
    let node2_actions = cluster.shards.get_mut(&NodeId(2)).unwrap().handle_message(
        NodeId(1),
        Message::AppendEntries {
            term,
            leader_id: NodeId(1),
            prev_log_index: commit_index,
            prev_log_term: term,
            entries: Arc::from(Vec::new()),
            leader_commit: commit_index,
            closed_ts_nanos: 0,
        },
    );

    // Node 2 is no longer a voter or learner — it must emit ShardRemoved.
    let has_shard_removed = node2_actions.iter().any(|a| matches!(a, Action::ShardRemoved { .. }));
    assert!(has_shard_removed, "Expected ShardRemoved for removed node, got: {node2_actions:?}");
}

// ── Auto-promote disabled ─────────────────────────────────────────

/// When `auto_promote` is disabled on the shard config, the shard must not
/// emit `MembershipChanged` for learner promotion — even when the learner
/// is fully caught up. This is the foundation for scheduler-managed DRs.
#[test]
fn election_auto_promote_disabled_does_not_promote_caught_up_learner() {
    use inferadb_ledger_consensus::{
        action::Action,
        config::ShardConfig,
        message::Message,
        types::{MembershipChange, NodeId},
    };

    // Build a cluster where all nodes have auto_promote disabled.
    let mut cluster = {
        use std::{collections::HashMap, sync::Arc};

        use inferadb_ledger_consensus::{
            clock::SimulatedClock,
            rng::SimulatedRng,
            consensus_state::ConsensusState,
            types::{Membership, ConsensusStateId},
        };

        let clock = Arc::new(SimulatedClock::new());
        let node_ids: Vec<NodeId> = (1..=3).map(NodeId).collect();
        let membership = Membership::new(node_ids.iter().copied());
        let config = ShardConfig { auto_promote: false, ..ShardConfig::default() };

        let mut shards = HashMap::new();
        for (i, &node_id) in node_ids.iter().enumerate() {
            let rng = SimulatedRng::new(100 + i as u64);
            let shard = ConsensusState::new(
                ConsensusStateId(0),
                node_id,
                membership.clone(),
                config.clone(),
                clock.clone(),
                rng,
                0,
                None,
                0,
            );
            shards.insert(node_id, shard);
        }

        common::TestCluster { shards, clock }
    };

    cluster.elect_leader(NodeId(1));
    assert!(cluster.has_leader(), "cluster should have a leader");

    // Add a promotable learner (node 4).
    cluster
        .propose_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: true,
            expected_conf_epoch: None,
        })
        .expect("add learner should succeed");

    {
        let leader = cluster.shards.get(&NodeId(1)).unwrap();
        assert!(leader.membership().is_learner(NodeId(4)), "node 4 should be a learner");
    }

    // Simulate node 4 catching up: send an ACK at the log tip.
    let term = cluster.shards[&NodeId(1)].current_term();
    let log_tip = cluster.shards[&NodeId(1)].log_len();

    let actions = cluster.shards.get_mut(&NodeId(1)).unwrap().handle_message(
        NodeId(4),
        Message::AppendEntriesResponse { term, success: true, match_index: log_tip },
    );

    // With auto_promote=false, no MembershipChanged promotion should fire.
    let promotion = actions.iter().find(|a| matches!(a, Action::MembershipChanged { .. }));
    assert!(
        promotion.is_none(),
        "auto_promote=false should suppress auto-promotion even when learner is caught up"
    );

    // Verify peer_match_index API returns the learner's match.
    let leader = cluster.shards.get(&NodeId(1)).unwrap();
    assert_eq!(leader.peer_match_index(NodeId(4)), Some(log_tip));
    assert_eq!(leader.peer_match_index(NodeId(99)), None);
}

// ── Stale Epoch Rejection ─────────────────────────────────────────

#[test]
fn election_stale_epoch_rejected() {
    let mut cluster = common::TestCluster::new(3);
    cluster.elect_leader(NodeId(1));

    // Add a learner — this commits a membership change, incrementing conf_epoch to 1.
    let leader = cluster.shards.get_mut(&NodeId(1)).unwrap();
    let actions = leader
        .handle_membership_change(MembershipChange::AddLearner {
            node_id: NodeId(4),
            promotable: false,
            expected_conf_epoch: None, // No epoch check on this one.
        })
        .unwrap();
    cluster.deliver_actions_public(NodeId(1), actions);

    // Now try a membership change with stale epoch (0, but actual is 1).
    let leader = cluster.shards.get_mut(&NodeId(1)).unwrap();
    let result = leader.handle_membership_change(MembershipChange::RemoveNode {
        node_id: NodeId(4),
        expected_conf_epoch: Some(0), // Stale!
    });
    assert!(
        matches!(result, Err(ConsensusError::StaleEpoch { expected: 0, actual: 1 })),
        "Expected StaleEpoch, got {result:?}"
    );

    // With correct epoch, it should succeed.
    let result = leader.handle_membership_change(MembershipChange::RemoveNode {
        node_id: NodeId(4),
        expected_conf_epoch: Some(1), // Correct.
    });
    assert!(result.is_ok(), "Expected success with correct epoch, got {result:?}");
}
