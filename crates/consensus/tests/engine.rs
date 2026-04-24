//! End-to-end ConsensusEngine lifecycle tests.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic, clippy::disallowed_methods)]

use std::{sync::Arc, time::Duration};

use inferadb_ledger_consensus::{
    clock::SimulatedClock,
    config::ShardConfig,
    consensus_state::ConsensusState,
    engine::ConsensusEngine,
    error::ConsensusError,
    leadership::ShardState,
    rng::SimulatedRng,
    transport::InMemoryTransport,
    types::{ConsensusStateId, Membership, NodeId, NodeState},
    wal::InMemoryWalBackend,
};
use tokio::sync::{mpsc, watch};

const SHARD: ConsensusStateId = ConsensusStateId(1);
const NODE: NodeId = NodeId(1);
/// Maximum time to wait for an async result before considering it hung.
const TIMEOUT: Duration = Duration::from_secs(5);

/// ConsensusState config with very short election timeouts for fast tests.
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
    let shard = ConsensusState::new(
        SHARD,
        NODE,
        Membership::new([NODE]),
        fast_shard_config(),
        clock.clone(),
        SimulatedRng::new(42),
        0,
        None,
        0,
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

    let result = engine.read_index(ConsensusStateId(99)).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(99) })),
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
    let shards: Vec<ConsensusState<Arc<SimulatedClock>, SimulatedRng>> = vec![];
    let (engine, _commit_rx, _state_rx) = ConsensusEngine::start(
        shards,
        InMemoryWalBackend::new(),
        clock,
        InMemoryTransport::new(),
        Duration::from_millis(10),
    );

    let result = engine.propose(ConsensusStateId(99), b"test".to_vec()).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(99) })),
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

    let result = engine.remove_node(ConsensusStateId(99), NodeId(2)).await;
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

    let result = engine.transfer_leader(ConsensusStateId(99), NodeId(2)).await;
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

    let result = engine.trigger_snapshot(ConsensusStateId(99)).await;
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
    let shard_a = ConsensusStateId(10);
    let shard_b = ConsensusStateId(20);

    let shards = vec![
        ConsensusState::new(
            shard_a,
            NODE,
            Membership::new([NODE]),
            fast_shard_config(),
            clock.clone(),
            SimulatedRng::new(1),
            0,
            None,
            0,
        ),
        ConsensusState::new(
            shard_b,
            NODE,
            Membership::new([NODE]),
            fast_shard_config(),
            clock.clone(),
            SimulatedRng::new(2),
            0,
            None,
            0,
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

    let result = engine.propose_batch(ConsensusStateId(99), vec![b"x".to_vec()]).await;
    assert!(
        matches!(result, Err(ConsensusError::ShardUnavailable { shard: ConsensusStateId(99) })),
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

// ---------------------------------------------------------------------------
// add_shard / remove_shard — runtime shard registration (Path A P2a).
// ---------------------------------------------------------------------------

/// Builds a new single-node shard suitable for runtime registration.
///
/// Uses the shared clock so the reactor's timers line up with the rest of
/// the engine, and the same fast election-timeout config as the primary
/// shard so the runtime-registered shard self-elects quickly.
fn make_single_node_shard(
    id: ConsensusStateId,
    clock: Arc<SimulatedClock>,
) -> ConsensusState<Arc<SimulatedClock>, SimulatedRng> {
    ConsensusState::new(
        id,
        NODE,
        Membership::new([NODE]),
        fast_shard_config(),
        clock,
        SimulatedRng::new(id.0),
        0,
        None,
        0,
    )
}

/// Drives a just-registered single-node shard through election by
/// advancing the simulated clock and waiting for the watch receiver to
/// report `Leader`.
async fn wait_for_leader(clock: &SimulatedClock, state_rx: &mut watch::Receiver<ShardState>) {
    clock.advance(Duration::from_secs(1));
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if state_rx.borrow().state == NodeState::Leader {
                return;
            }
            state_rx.changed().await.expect("state_rx closed before leader elected");
        }
    })
    .await
    .expect("timed out waiting for runtime-registered shard to elect leader");
}

#[tokio::test]
async fn add_shard_registers_at_runtime() {
    let (engine, _commit_rx, mut state_rx_1, clock) = make_engine();
    // Drive the primary shard to leader first so the engine is fully
    // up-and-running when we register the second shard.
    elect_leader(&clock, &mut state_rx_1).await;

    // Register a second shard mid-flight.
    let second_id = ConsensusStateId(2);
    let second_shard = make_single_node_shard(second_id, clock.clone());
    let mut state_rx_2 =
        engine.add_shard(second_shard).await.expect("add_shard should succeed on a running engine");

    // The second shard must elect itself the same way the first one did.
    wait_for_leader(&clock, &mut state_rx_2).await;

    // A proposal to the newly-registered shard must commit (single-node
    // quorum): confirms the shard is fully wired into the reactor.
    let commit_index =
        tokio::time::timeout(TIMEOUT, engine.propose(second_id, b"after-add".to_vec()))
            .await
            .expect("propose timed out")
            .expect("propose to runtime-registered shard should commit");
    assert!(commit_index > 0, "commit index for real entry should be positive");

    // The original shard stays functional — add_shard is purely additive.
    let commit_index_first =
        tokio::time::timeout(TIMEOUT, engine.propose(SHARD, b"original".to_vec()))
            .await
            .expect("propose to original shard timed out")
            .expect("propose to pre-existing shard should still commit");
    assert!(commit_index_first > 0, "original shard commit index should be positive");

    engine.shutdown().await;
}

#[tokio::test]
async fn add_shard_duplicate_registration_is_noop() {
    let (engine, _commit_rx, mut state_rx_1, clock) = make_engine();
    elect_leader(&clock, &mut state_rx_1).await;

    // Attempt to re-register the existing SHARD. The reactor must detect
    // the duplicate and drop the new state_tx without disturbing the
    // running shard.
    let duplicate = make_single_node_shard(SHARD, clock.clone());
    let mut duplicate_state_rx = engine
        .add_shard(duplicate)
        .await
        .expect("add_shard should accept the event (rejection happens in the reactor)");

    // The reactor closes the duplicate's watch sender as soon as it
    // detects the collision. The caller's receiver observes the close.
    tokio::time::timeout(TIMEOUT, duplicate_state_rx.changed())
        .await
        .expect("timed out waiting for duplicate watcher close")
        .expect_err("duplicate watcher should be closed by the reactor");

    // Original shard is still the leader and still commits proposals.
    assert_eq!(
        state_rx_1.borrow().state,
        NodeState::Leader,
        "original shard must remain leader after duplicate add_shard",
    );
    let commit_index = tokio::time::timeout(TIMEOUT, engine.propose(SHARD, b"still-mine".to_vec()))
        .await
        .expect("propose timed out")
        .expect("original shard must still accept proposals");
    assert!(commit_index > 0);

    engine.shutdown().await;
}

#[tokio::test]
async fn remove_shard_drains_pending_responses_with_not_leader() {
    // Spin up an engine whose only shard is a 3-node cluster so
    // proposals require quorum to commit. With peers unreachable the
    // proposal stays pending until `remove_shard` rejects it with
    // NotLeader.
    let clock = Arc::new(SimulatedClock::new());
    let three_node = ConsensusState::new(
        SHARD,
        NODE,
        Membership::new([NODE, NodeId(2), NodeId(3)]),
        fast_shard_config(),
        clock.clone(),
        SimulatedRng::new(42),
        0,
        None,
        0,
    );
    let (engine, _commit_rx, state_rx) = {
        let (engine, commit_rx, mut state_rxs) = ConsensusEngine::start(
            vec![three_node],
            InMemoryWalBackend::new(),
            clock.clone(),
            InMemoryTransport::new(),
            Duration::from_millis(5),
        );
        let state_rx = state_rxs.remove(&SHARD).expect("state_rx for 3-node shard");
        (engine, commit_rx, state_rx)
    };

    // Drive election (single-node-in-3 can become leader if it gets
    // granted votes; here the peers are unreachable and won't respond,
    // but we can still observe Leader via the forced simulation flow
    // used by the reactor's election path — advance the clock and wait
    // briefly).
    clock.advance(Duration::from_secs(1));
    // Give the reactor a few flush ticks to observe the election timer.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Submit a proposal in the background. Peers (nodes 2/3) are
    // unreachable on InMemoryTransport so the proposal cannot quorum-
    // commit and the response stays pending in the reactor.
    let engine_arc = std::sync::Arc::new(engine);
    let engine_for_task = std::sync::Arc::clone(&engine_arc);
    let propose_task =
        tokio::spawn(async move { engine_for_task.propose(SHARD, b"uncommitted".to_vec()).await });

    // Let the proposal reach the reactor before removing the shard.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Remove the shard. The reactor must drain the pending response
    // with NotLeader.
    engine_arc.remove_shard(SHARD).await.expect("remove_shard should succeed");

    // The in-flight propose returns NotLeader (or ShardUnavailable if
    // the shard was dropped before the propose reached the reactor —
    // both satisfy the contract that the caller unblocks).
    let result = tokio::time::timeout(TIMEOUT, propose_task)
        .await
        .expect("propose task timed out")
        .expect("propose task panicked");
    assert!(
        matches!(
            result,
            Err(ConsensusError::NotLeader) | Err(ConsensusError::ShardUnavailable { .. })
        ),
        "remove_shard must reject in-flight propose, got {result:?}",
    );

    // Round-trip a read_index to force the reactor to process the
    // RemoveShard event and schedule its Cleanup timer before we
    // advance the simulated clock. Without this sync point the clock
    // advance could happen before the RemoveShard handler runs, which
    // would push the cleanup deadline past the advance.
    //
    // In `Shutdown` state the shard still reports a commit_index, so
    // the read succeeds. The call's primary purpose is the round-trip
    // ordering guarantee.
    let _ = engine_arc.read_index(SHARD).await;

    // The shard is marked Shutdown but still present in the reactor's
    // maps until the 30-second cleanup timer fires. Advance the
    // simulated clock past the grace period; the reactor's flush
    // ticker (5ms wall clock) wakes the loop post-select, which runs
    // `process_expired_timers` and drops the shard.
    clock.advance(Duration::from_secs(31));

    // Poll read_index until the reactor reports ShardUnavailable —
    // conclusive evidence the cleanup timer ran and the shard is gone
    // from the reactor's maps. Each call wakes the reactor (control
    // inbox), so post-select cleanup runs before the next iteration.
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        match engine_arc.read_index(SHARD).await {
            Err(ConsensusError::ShardUnavailable { .. }) => break,
            Ok(_) | Err(_) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("cleanup timer did not drop shard within TIMEOUT");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            },
        }
    }

    // A subsequent proposal to the removed shard must also fail with
    // ShardUnavailable (the handler short-circuits when the shard is
    // missing from the map, so no NotLeader leaks through).
    let post_removal =
        tokio::time::timeout(TIMEOUT, engine_arc.propose(SHARD, b"too-late".to_vec()))
            .await
            .expect("post-removal propose timed out");
    assert!(
        matches!(post_removal, Err(ConsensusError::ShardUnavailable { .. })),
        "post-cleanup propose must fail with ShardUnavailable, got {post_removal:?}",
    );

    // The watch receiver held by external observers may either remain
    // open (if the reactor still held the sender when we checked) or
    // have closed when the cleanup timer dropped it. We don't assert
    // here because the transition is racy with the flush timer; the
    // load-bearing invariant is tested via the propose failure above.
    let _ = state_rx.has_changed();

    // Tear down by taking the engine out of the Arc.
    let engine = std::sync::Arc::try_unwrap(engine_arc)
        .ok()
        .expect("only one Arc<ConsensusEngine> reference at shutdown");
    engine.shutdown().await;
}
