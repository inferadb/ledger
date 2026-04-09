//! End-to-end ConsensusEngine lifecycle tests.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic, clippy::disallowed_methods)]

use std::{sync::Arc, time::Duration};

use inferadb_ledger_consensus::{
    clock::SimulatedClock,
    config::ShardConfig,
    engine::ConsensusEngine,
    error::ConsensusError,
    leadership::ShardState,
    rng::SimulatedRng,
    shard::Shard,
    transport::InMemoryTransport,
    types::{Membership, NodeId, NodeState, ShardId},
    wal::InMemoryWalBackend,
};
use tokio::sync::{mpsc, watch};

const SHARD: ShardId = ShardId(1);
const NODE: NodeId = NodeId(1);
/// Maximum time to wait for an async result before considering it hung.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Shard config with very short election timeouts for fast tests.
fn fast_shard_config() -> ShardConfig {
    ShardConfig {
        election_timeout_min: Duration::from_millis(20),
        election_timeout_max: Duration::from_millis(40),
        heartbeat_interval: Duration::from_millis(10),
        ..ShardConfig::default()
    }
}

/// Creates a single-shard, single-node engine with a simulated clock and
/// aggressive election timeouts so the single-node shard self-elects quickly.
///
/// Returns the engine, commit receiver, state watch receiver, and the clock
/// handle so tests can advance time to trigger elections.
fn make_engine() -> (
    ConsensusEngine,
    mpsc::Receiver<inferadb_ledger_consensus::committed::CommittedBatch>,
    watch::Receiver<ShardState>,
    Arc<SimulatedClock>,
) {
    let clock = Arc::new(SimulatedClock::new());
    let shard = Shard::new(
        SHARD,
        NODE,
        Membership::new([NODE]),
        fast_shard_config(),
        clock.clone(),
        SimulatedRng::new(42),
    );

    let (engine, commit_rx, mut state_rxs) = ConsensusEngine::start(
        vec![shard],
        InMemoryWalBackend::new(),
        clock.clone(),
        InMemoryTransport::new(),
        Duration::from_millis(5),
    );

    let state_rx = state_rxs.remove(&SHARD).expect("shard state receiver");
    (engine, commit_rx, state_rx, clock)
}

/// Advances the simulated clock past the election timeout and yields until the
/// reactor processes the timer, electing the single-node shard as leader.
async fn elect_leader(clock: &SimulatedClock, state_rx: &mut watch::Receiver<ShardState>) {
    // Advance simulated clock well past the election timeout.
    clock.advance(Duration::from_secs(1));

    tokio::time::timeout(TIMEOUT, async {
        loop {
            if state_rx.borrow().state == NodeState::Leader {
                return;
            }
            state_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("timed out waiting for leader election");
}

// ---------------------------------------------------------------------------
// Startup / shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_and_shutdown_completes() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();
    engine.shutdown().await;
}

#[tokio::test]
async fn request_shutdown_via_shared_ref() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();
    engine.request_shutdown().await;
}

// ---------------------------------------------------------------------------
// Propose single entry and receive committed batch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn propose_single_entry_commits_and_delivers_batch() {
    let (engine, mut commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    let commit_index = tokio::time::timeout(TIMEOUT, engine.propose(SHARD, b"hello".to_vec()))
        .await
        .expect("timed out")
        .expect("propose failed");
    assert!(commit_index > 0, "commit index must be positive after propose");

    // The committed batch should arrive on the channel.
    let batch = tokio::time::timeout(TIMEOUT, async {
        loop {
            let batch = commit_rx.recv().await.expect("commit channel closed");
            if batch.entries.iter().any(|e| &*e.data == b"hello") {
                return batch;
            }
        }
    })
    .await
    .expect("timed out waiting for committed batch");

    assert_eq!(batch.shard, SHARD);
    assert!(!batch.entries.is_empty());

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Propose batch (multiple entries)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn propose_batch_commits_multiple_entries() {
    let (engine, mut commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    let entries = vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()];
    let commit_index = tokio::time::timeout(TIMEOUT, engine.propose_batch(SHARD, entries))
        .await
        .expect("timed out")
        .expect("propose_batch failed");
    assert!(commit_index > 0);

    // Drain committed batches until we find all three payloads.
    let mut found = [false; 3];
    let payloads: [&[u8]; 3] = [b"one", b"two", b"three"];

    tokio::time::timeout(TIMEOUT, async {
        while !found.iter().all(|&f| f) {
            let batch = commit_rx.recv().await.expect("commit channel closed");
            for entry in &batch.entries {
                for (i, payload) in payloads.iter().enumerate() {
                    if &*entry.data == *payload {
                        found[i] = true;
                    }
                }
            }
        }
    })
    .await
    .expect("timed out waiting for all batch entries");

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// read_index returns committed index
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_index_returns_zero_for_fresh_shard() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();

    let idx = tokio::time::timeout(TIMEOUT, engine.read_index(SHARD))
        .await
        .expect("timed out")
        .expect("read_index failed");
    assert_eq!(idx, 0, "fresh shard commit index should be 0");

    engine.shutdown().await;
}

#[tokio::test]
async fn read_index_advances_after_propose() {
    let (engine, mut _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    let before = engine.read_index(SHARD).await.expect("read_index failed");

    engine.propose(SHARD, b"bump".to_vec()).await.expect("propose failed");

    let after = tokio::time::timeout(TIMEOUT, engine.read_index(SHARD))
        .await
        .expect("timed out")
        .expect("read_index failed");
    assert!(
        after > before,
        "commit index must advance after propose: before={before}, after={after}"
    );

    engine.shutdown().await;
}

#[tokio::test]
async fn read_index_unknown_shard_returns_error() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();

    let result = engine.read_index(ShardId(99)).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { shard: ShardId(99) })),
        "expected ShardUnavailable, got {result:?}",
    );

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Propose to missing/unknown shard
// ---------------------------------------------------------------------------

#[tokio::test]
async fn propose_to_missing_shard_returns_shard_unavailable() {
    let clock = Arc::new(SimulatedClock::new());
    let shards: Vec<Shard<Arc<SimulatedClock>, SimulatedRng>> = vec![];
    let (engine, _commit_rx, _state_rx) = ConsensusEngine::start(
        shards,
        InMemoryWalBackend::new(),
        clock,
        InMemoryTransport::new(),
        Duration::from_millis(10),
    );

    let result = engine.propose(ShardId(99), b"test".to_vec()).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { shard: ShardId(99) })),
        "expected ShardUnavailable, got {result:?}",
    );

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Propose to non-leader returns NotLeader
// ---------------------------------------------------------------------------

#[tokio::test]
async fn propose_before_election_returns_not_leader() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();
    // Do NOT advance clock — shard is still a follower.

    let result = engine.propose(SHARD, b"too-early".to_vec()).await;
    assert!(matches!(result, Err(ConsensusError::NotLeader)), "expected NotLeader, got {result:?}",);

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Add learner through engine
// ---------------------------------------------------------------------------

#[tokio::test]
async fn add_learner_succeeds_on_leader() {
    let (engine, _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    let result = tokio::time::timeout(TIMEOUT, engine.add_learner(SHARD, NodeId(2), true))
        .await
        .expect("timed out");
    assert!(result.is_ok(), "add_learner failed: {result:?}");

    // Verify learner appears in the shard state.
    tokio::time::timeout(TIMEOUT, async {
        loop {
            // Check current state first (may already be updated).
            if state_rx.borrow().learners.contains(&NodeId(2)) {
                return;
            }
            state_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("timed out waiting for learner to appear in state");

    engine.shutdown().await;
}

#[tokio::test]
async fn add_learner_on_follower_returns_not_leader() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();

    let result = engine.add_learner(SHARD, NodeId(2), true).await;
    assert!(matches!(result, Err(ConsensusError::NotLeader)), "expected NotLeader, got {result:?}",);

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Promote voter through engine
// ---------------------------------------------------------------------------

#[tokio::test]
async fn promote_voter_after_add_learner() {
    let (engine, _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    // Add learner first.
    engine.add_learner(SHARD, NodeId(2), true).await.expect("add_learner failed");

    // Wait for the learner to appear (membership change committed).
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if state_rx.borrow().learners.contains(&NodeId(2)) {
                return;
            }
            state_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("timed out waiting for learner");

    // Now promote.
    let result = tokio::time::timeout(TIMEOUT, engine.promote_voter(SHARD, NodeId(2)))
        .await
        .expect("timed out");
    assert!(result.is_ok(), "promote_voter failed: {result:?}");

    // Verify voter appears in state.
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if state_rx.borrow().voters.contains(&NodeId(2)) {
                return;
            }
            state_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("timed out waiting for voter promotion");

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Remove node through engine
// ---------------------------------------------------------------------------

#[tokio::test]
async fn remove_node_removes_learner() {
    let (engine, _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    // Add then remove a learner.
    engine.add_learner(SHARD, NodeId(2), false).await.expect("add_learner failed");

    tokio::time::timeout(TIMEOUT, async {
        loop {
            if state_rx.borrow().learners.contains(&NodeId(2)) {
                return;
            }
            state_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("timed out waiting for learner");

    let result = tokio::time::timeout(TIMEOUT, engine.remove_node(SHARD, NodeId(2)))
        .await
        .expect("timed out");
    assert!(result.is_ok(), "remove_node failed: {result:?}");

    engine.shutdown().await;
}

#[tokio::test]
async fn remove_node_on_unknown_shard_returns_error() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();

    let result = engine.remove_node(ShardId(99), NodeId(2)).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { .. })),
        "expected ShardUnavailable, got {result:?}",
    );

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Transfer leader
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transfer_leader_to_self_returns_not_leader_error() {
    let (engine, _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    // Transferring to self should fail (shard rejects target == self).
    let result = engine.transfer_leader(SHARD, NODE).await;
    assert!(
        matches!(result, Err(ConsensusError::NotLeader)),
        "expected NotLeader for self-transfer, got {result:?}",
    );

    engine.shutdown().await;
}

#[tokio::test]
async fn transfer_leader_to_non_voter_returns_shard_unavailable() {
    let (engine, _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    // NodeId(99) is not a voter.
    let result = engine.transfer_leader(SHARD, NodeId(99)).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { .. })),
        "expected ShardUnavailable for non-voter target, got {result:?}",
    );

    engine.shutdown().await;
}

#[tokio::test]
async fn transfer_leader_on_follower_returns_not_leader() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();

    let result = engine.transfer_leader(SHARD, NodeId(2)).await;
    assert!(matches!(result, Err(ConsensusError::NotLeader)), "expected NotLeader, got {result:?}",);

    engine.shutdown().await;
}

#[tokio::test]
async fn transfer_leader_to_unknown_shard_returns_error() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();

    let result = engine.transfer_leader(ShardId(99), NodeId(2)).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { .. })),
        "expected ShardUnavailable, got {result:?}",
    );

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Trigger snapshot
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trigger_snapshot_on_fresh_shard_returns_zero() {
    let (engine, _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    let (index, term) = tokio::time::timeout(TIMEOUT, engine.trigger_snapshot(SHARD))
        .await
        .expect("timed out")
        .expect("trigger_snapshot failed");
    // After election, the leader commits a no-op entry (Raft §5.4.2) at
    // index 1, term 1. The snapshot reflects this committed state.
    assert_eq!(term, 1, "snapshot term should match leader's first term");
    assert_eq!(index, 1, "snapshot index should include the no-op entry");

    engine.shutdown().await;
}

#[tokio::test]
async fn trigger_snapshot_after_propose_returns_nonzero() {
    let (engine, mut _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    // Propose an entry to advance commit index.
    engine.propose(SHARD, b"snap-me".to_vec()).await.expect("propose failed");

    let (index, term) = tokio::time::timeout(TIMEOUT, engine.trigger_snapshot(SHARD))
        .await
        .expect("timed out")
        .expect("trigger_snapshot failed");
    assert!(index > 0, "snapshot index should be positive after propose");
    assert!(term > 0, "snapshot term should be positive after propose");

    engine.shutdown().await;
}

#[tokio::test]
async fn trigger_snapshot_unknown_shard_returns_error() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();

    let result = engine.trigger_snapshot(ShardId(99)).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { .. })),
        "expected ShardUnavailable, got {result:?}",
    );

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Multiple shards on same engine
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiple_shards_independent_proposals() {
    let clock = Arc::new(SimulatedClock::new());
    let shard_a = ShardId(10);
    let shard_b = ShardId(20);

    let shards = vec![
        Shard::new(
            shard_a,
            NODE,
            Membership::new([NODE]),
            fast_shard_config(),
            clock.clone(),
            SimulatedRng::new(1),
        ),
        Shard::new(
            shard_b,
            NODE,
            Membership::new([NODE]),
            fast_shard_config(),
            clock.clone(),
            SimulatedRng::new(2),
        ),
    ];

    let (engine, mut commit_rx, mut state_rxs) = ConsensusEngine::start(
        shards,
        InMemoryWalBackend::new(),
        clock.clone(),
        InMemoryTransport::new(),
        Duration::from_millis(5),
    );

    let mut state_a = state_rxs.remove(&shard_a).unwrap();
    let mut state_b = state_rxs.remove(&shard_b).unwrap();

    // Advance clock to trigger elections on both shards.
    clock.advance(Duration::from_secs(1));

    tokio::time::timeout(TIMEOUT, async {
        let mut a_leader = false;
        let mut b_leader = false;
        loop {
            if state_a.borrow().state == NodeState::Leader {
                a_leader = true;
            }
            if state_b.borrow().state == NodeState::Leader {
                b_leader = true;
            }
            if a_leader && b_leader {
                break;
            }
            tokio::select! {
                Ok(()) = state_a.changed(), if !a_leader => {}
                Ok(()) = state_b.changed(), if !b_leader => {}
            }
        }
    })
    .await
    .expect("timed out waiting for both shards to become leader");

    // Propose to shard A.
    let idx_a =
        engine.propose(shard_a, b"shard-a-data".to_vec()).await.expect("propose to A failed");
    assert!(idx_a > 0);

    // Propose to shard B.
    let idx_b =
        engine.propose(shard_b, b"shard-b-data".to_vec()).await.expect("propose to B failed");
    assert!(idx_b > 0);

    // Collect committed batches for both shards.
    let mut found_a = false;
    let mut found_b = false;
    tokio::time::timeout(TIMEOUT, async {
        while !found_a || !found_b {
            let batch = commit_rx.recv().await.expect("commit channel closed");
            if batch.shard == shard_a {
                found_a |= batch.entries.iter().any(|e| &*e.data == b"shard-a-data");
            }
            if batch.shard == shard_b {
                found_b |= batch.entries.iter().any(|e| &*e.data == b"shard-b-data");
            }
        }
    })
    .await
    .expect("timed out waiting for committed batches from both shards");

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// State watch receiver reflects leader election
// ---------------------------------------------------------------------------

#[tokio::test]
async fn state_watch_reflects_leader_election() {
    let (engine, _commit_rx, mut state_rx, clock) = make_engine();

    // Before election: should be Follower.
    assert_eq!(state_rx.borrow().state, NodeState::Follower);

    elect_leader(&clock, &mut state_rx).await;

    let state = state_rx.borrow().clone();
    assert_eq!(state.state, NodeState::Leader);
    assert_eq!(state.leader, Some(NODE));
    assert!(state.term > 0);

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Shutdown while proposals are in-flight
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shutdown_while_proposal_inflight() {
    let (engine, _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    // Race a proposal against shutdown concurrently using select!.
    // request_shutdown() borrows &self (unlike shutdown() which consumes),
    // so we can interleave it with the proposal future.
    let result = tokio::time::timeout(TIMEOUT, async {
        tokio::select! {
            _ = engine.request_shutdown() => {},
            _ = engine.propose(SHARD, b"inflight".to_vec()) => {},
        }
    })
    .await;
    assert!(result.is_ok(), "proposal must not hang during shutdown");

    engine.shutdown().await;
}

#[tokio::test]
async fn shutdown_drains_pending_proposals() {
    let (engine, _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    // Fire several proposals without awaiting them.
    let mut handles = Vec::new();
    for i in 0..5 {
        let data = format!("pending-{i}").into_bytes();
        handles.push(engine.propose(SHARD, data));
    }

    // Request shutdown first, then await all proposals to verify they resolve.
    engine.request_shutdown().await;

    tokio::time::timeout(TIMEOUT, async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await
    .expect("pending proposals must resolve after shutdown");

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Propose batch to missing shard
// ---------------------------------------------------------------------------

#[tokio::test]
async fn propose_batch_to_missing_shard_returns_error() {
    let (engine, _commit_rx, _state_rx, _clock) = make_engine();

    let result = engine.propose_batch(ShardId(99), vec![b"x".to_vec()]).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { shard: ShardId(99) })),
        "expected ShardUnavailable, got {result:?}",
    );

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Committed batch carries leader_node
// ---------------------------------------------------------------------------

#[tokio::test]
async fn committed_batch_carries_leader_node() {
    let (engine, mut commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    engine.propose(SHARD, b"leader-check".to_vec()).await.expect("propose failed");

    let batch = tokio::time::timeout(TIMEOUT, async {
        loop {
            let batch = commit_rx.recv().await.expect("channel closed");
            if batch.entries.iter().any(|e| &*e.data == b"leader-check") {
                return batch;
            }
        }
    })
    .await
    .expect("timed out waiting for committed batch");

    assert_eq!(batch.leader_node, Some(NODE.0), "batch should carry leader node ID");

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Idempotency: duplicate propose returns cached index
// ---------------------------------------------------------------------------

#[tokio::test]
async fn duplicate_propose_returns_cached_commit_index() {
    let (engine, _commit_rx, mut state_rx, clock) = make_engine();

    elect_leader(&clock, &mut state_rx).await;

    let data = b"idempotent-payload".to_vec();
    let first_index = engine.propose(SHARD, data.clone()).await.expect("first propose failed");

    // Second propose with identical data should return the cached index.
    let second_index = engine.propose(SHARD, data).await.expect("second propose failed");
    assert_eq!(first_index, second_index, "duplicate must return cached commit index");
    assert_eq!(engine.idempotency_cache_len(), 1, "cache should have exactly one entry");

    engine.shutdown().await;
}

// ---------------------------------------------------------------------------
// Validator rejects proposals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn validator_rejects_blocked_proposals() {
    let (mut engine, _commit_rx, mut state_rx, clock) = make_engine();

    engine.set_validator(|data| {
        if data.starts_with(b"BLOCKED") {
            return Err("blocked prefix".into());
        }
        Ok(())
    });

    elect_leader(&clock, &mut state_rx).await;

    // Blocked proposal must return an error even when shard is leader.
    let result = engine.propose(SHARD, b"BLOCKED:payload".to_vec()).await;
    assert!(result.is_err(), "validator-rejected proposal must fail");
    assert_eq!(engine.idempotency_cache_len(), 0, "rejected proposal must not enter cache");

    // Allowed proposal should succeed.
    let result = engine.propose(SHARD, b"allowed-payload".to_vec()).await;
    assert!(result.is_ok(), "non-blocked proposal should succeed: {result:?}");

    engine.shutdown().await;
}

#[tokio::test]
async fn validator_rejects_propose_batch_entries() {
    let (mut engine, _commit_rx, mut state_rx, clock) = make_engine();

    engine.set_validator(|data| {
        if data.starts_with(b"BLOCKED") {
            return Err("blocked prefix".into());
        }
        Ok(())
    });

    elect_leader(&clock, &mut state_rx).await;

    // Batch containing a blocked entry must be rejected.
    let result =
        engine.propose_batch(SHARD, vec![b"allowed".to_vec(), b"BLOCKED:bad".to_vec()]).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { .. })),
        "batch with blocked entry must fail, got {result:?}",
    );

    // Batch with only allowed entries should succeed.
    let result = engine.propose_batch(SHARD, vec![b"ok".to_vec()]).await;
    assert!(result.is_ok(), "all-allowed batch should succeed: {result:?}");

    engine.shutdown().await;
}
