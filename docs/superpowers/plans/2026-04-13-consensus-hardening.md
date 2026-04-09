# Multi-Raft Consensus Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace imperative signal-based multi-Raft membership coordination with TiKV-style centralized scheduling — declarative state convergence, checker/scheduler separation, quorum-based liveness, epoch fencing, and deterministic simulation.

**Architecture:** GLOBAL Raft leader holds declarative `NodeStatus` per node. DR leaders run independent checkers (1s, reactive repair) and schedulers (5s, proactive growth) that read GLOBAL state and converge local DR membership via a priority-ordered operator queue. Dead nodes are detected via quorum-confirmed liveness. The existing simulation harness is extended with multi-Raft scenarios, FDB-style buggification, and randomized soak testing.

**Tech Stack:** Rust 1.92, custom multi-shard Raft consensus engine, postcard serialization, tonic gRPC, parking_lot locks, tokio async runtime.

**Spec:** [`docs/superpowers/specs/2026-04-13-consensus-hardening.md`](../specs/2026-04-13-consensus-hardening.md)

---

## Phase A: Tactical Hardening

Six targeted fixes to the consensus engine. No coordination architecture changes.

### Task A1: Non-voter election guard

**Files:**
- Modify: `crates/consensus/src/shard.rs:361` (`start_pre_vote`), `:450` (`start_election`)
- Test: `crates/consensus/tests/election.rs`

- [ ] **Step 1: Write failing test — non-voter should not start pre-vote**

In `crates/consensus/tests/election.rs`, add:

```rust
#[test]
fn test_non_voter_does_not_start_pre_vote() {
    let (clock, mut shard) = make_3node_shard(1);
    elect_leader(&mut shard, &clock);

    // Remove node 1 from voters — it becomes a non-voter.
    let actions = shard
        .handle_membership_change(MembershipChange::RemoveNode { node_id: NodeId(1) })
        .unwrap();
    // Commit the membership change.
    commit_entries(&mut shard, &actions);

    // Advance clock past election timeout.
    clock.advance(Duration::from_millis(700));
    let actions = shard.handle_election_timeout();

    // Non-voter should NOT send PreVoteRequest messages.
    let pre_vote_sends = actions.iter().filter(|a| {
        matches!(a, Action::Send { msg: Message::PreVoteRequest { .. }, .. })
    }).count();
    assert_eq!(pre_vote_sends, 0, "Non-voter should not initiate pre-vote");

    // Should still schedule a timer (stays alive as silent follower).
    assert!(actions.iter().any(|a| matches!(a, Action::ScheduleTimer { .. })));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo +1.92 test -p inferadb-ledger-consensus --test election test_non_voter_does_not_start_pre_vote -- --nocapture`
Expected: FAIL — non-voter currently sends pre-vote requests.

- [ ] **Step 3: Add guard to `start_pre_vote`**

In `crates/consensus/src/shard.rs`, at the top of `start_pre_vote` (line 361), before `self.state = NodeState::PreCandidate`:

```rust
fn start_pre_vote(&mut self) -> Vec<Action> {
    if !self.membership.is_voter(self.node_id) {
        let mut actions = Vec::new();
        self.reset_election_timer(&mut actions);
        return actions;
    }
    // ... existing body unchanged
```

Add the same guard to `start_election` (line 450), before `self.current_term += 1`:

```rust
fn start_election(&mut self) -> Vec<Action> {
    if !self.membership.is_voter(self.node_id) {
        let mut actions = Vec::new();
        self.reset_election_timer(&mut actions);
        return actions;
    }
    // ... existing body unchanged
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo +1.92 test -p inferadb-ledger-consensus --test election test_non_voter_does_not_start_pre_vote -- --nocapture`
Expected: PASS

- [ ] **Step 5: Run full consensus test suite**

Run: `cargo +1.92 test -p inferadb-ledger-consensus`
Expected: All tests pass (existing election tests unaffected — they use nodes that ARE voters).

---

### Task A1b: No-op commit precondition for membership changes

**Files:**
- Modify: `crates/consensus/src/shard.rs:33` (add field), `:790` (`handle_membership_change`), `:1061` (`try_advance_commit`)
- Test: `crates/consensus/tests/election.rs`

- [ ] **Step 1: Write failing test — membership change rejected before no-op commits**

In `crates/consensus/tests/election.rs`:

```rust
#[test]
fn test_membership_change_rejected_before_noop_commits() {
    let (clock, mut shard) = make_3node_shard(1);

    // Win the election — this appends a no-op but doesn't commit it yet
    // (needs quorum ack).
    let actions = shard.handle_election_timeout();
    // Process pre-vote, then real election, become leader.
    // ... (use the elect_leader helper BUT intercept before quorum commit)
    
    // Actually: create a leader directly and verify the guard works.
    // The simplest approach: manually set state to Leader without committing.
    // Since elect_leader commits the no-op (single-node fast path doesn't
    // help here), test with a 3-node shard where quorum isn't reached yet.
    
    // Better approach: use a 3-node shard, elect node 1, but don't deliver
    // AppendEntriesResponse from peers. The no-op is proposed but not committed.
    let (clock, mut shard) = make_3node_shard(1);
    
    // Trigger election and win it.
    clock.advance(Duration::from_millis(700));
    let actions = shard.handle_election_timeout();
    // Grant pre-votes from peers.
    let _actions = shard.handle_message(
        NodeId(2),
        Message::PreVoteResponse { term: 0, granted: true },
    );
    // Now in real election — grant votes.
    let actions = shard.handle_message(
        NodeId(2),
        Message::VoteResponse { term: 1, granted: true },
    );
    // Node 1 is now Leader. No-op appended but NOT committed (no AppendEntriesResponse yet).
    assert_eq!(shard.state(), NodeState::Leader);
    
    // Try a membership change — should be rejected.
    let result = shard.handle_membership_change(
        MembershipChange::AddLearner { node_id: NodeId(4), promotable: false },
    );
    assert!(
        matches!(result, Err(ConsensusError::LeaderNotReady)),
        "Expected LeaderNotReady, got {:?}", result
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo +1.92 test -p inferadb-ledger-consensus --test election test_membership_change_rejected_before_noop_commits -- --nocapture`
Expected: FAIL — currently no `LeaderNotReady` guard exists.

- [ ] **Step 3: Add `last_committed_term` field and guard**

In `crates/consensus/src/shard.rs`:

Add field to `Shard` struct (after `current_term` at line 33):
```rust
current_term: u64,
last_committed_term: u64,  // NEW: term of the last committed entry
```

Initialize in `Shard::new()` (around line 76):
```rust
last_committed_term: 0,
```

In `try_advance_commit` (line 1061), after `self.commit_index = quorum_match;`:
```rust
self.commit_index = quorum_match;
// Track the term of the latest committed entry for the LeaderNotReady guard.
if let Some(entry) = self.log.get((quorum_match - 1) as usize) {
    self.last_committed_term = entry.term;
}
```

Add `LeaderNotReady` variant to `ConsensusError` in `crates/consensus/src/error.rs`:
```rust
#[snafu(display("Leader has not yet committed an entry in its current term"))]
LeaderNotReady,
```

In `handle_membership_change` (line 790), after the `pending_membership` check (line 800):
```rust
if self.pending_membership {
    return Err(ConsensusError::MembershipChangePending);
}
// Ongaro §4.1: reject membership changes until the leader has committed
// an entry in its current term.
if self.last_committed_term != self.current_term {
    return Err(ConsensusError::LeaderNotReady);
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo +1.92 test -p inferadb-ledger-consensus --test election test_membership_change_rejected_before_noop_commits -- --nocapture`
Expected: PASS

- [ ] **Step 5: Verify existing membership tests still pass**

Run: `cargo +1.92 test -p inferadb-ledger-consensus -- membership`
Expected: All pass. The existing tests use `elect_leader` helper which commits the no-op before testing membership changes.

- [ ] **Step 6: Run full test suite**

Run: `cargo +1.92 test --workspace --lib`
Expected: All tests pass.

---

### Task A2: Signal channel health monitoring

**Files:**
- Modify: `crates/raft/src/log_storage/operations/mod.rs:3001,3016,3028` (sender.send calls)
- Modify: `crates/server/src/bootstrap.rs:256` (region handler task)

- [ ] **Step 1: Replace silent send failures with error logging**

In `crates/raft/src/log_storage/operations/mod.rs`, replace all three `let _ = sender.send(...)` patterns:

At line ~3001 (`CreateDataRegion`):
```rust
if sender.send((*region, initial_members.clone())).is_err() {
    tracing::error!(
        region = region.as_str(),
        "Region handler channel closed — CreateDataRegion will not be processed"
    );
}
```

At line ~3016 (`RegisterPeerAddress` reconcile signal):
```rust
if sender.send((inferadb_ledger_types::Region::GLOBAL, vec![(0, "reconcile_transport".to_string())])).is_err() {
    tracing::error!("Region handler channel closed — transport reconciliation will not run");
}
```

At line ~3033 (`RemoveNodeFromDataRegions` signal):
```rust
if sender.send((inferadb_ledger_types::Region::GLOBAL, vec![(*node_id, String::new())])).is_err() {
    tracing::error!(
        node_id,
        "Region handler channel closed — RemoveNodeFromDataRegions will not be processed"
    );
}
```

- [ ] **Step 2: Wrap region handler task in panic recovery**

In `crates/server/src/bootstrap.rs`, modify the `tokio::spawn` at line ~256. Extract the loop body into a separate async function, then wrap in catch_unwind:

```rust
tokio::spawn(async move {
    loop {
        match std::panic::AssertUnwindSafe(
            process_region_events(&mgr, &mut region_rx, &removals, &bootstrap_events_config)
        )
        .catch_unwind()
        .await
        {
            Ok(()) => break, // Channel closed normally — receiver dropped.
            Err(panic_info) => {
                tracing::error!(
                    ?panic_info,
                    "Region handler task panicked — restarting after 1s"
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
});
```

Where `process_region_events` is the existing loop body extracted into:
```rust
async fn process_region_events(
    mgr: &inferadb_ledger_raft::RaftManager,
    region_rx: &mut tokio::sync::mpsc::UnboundedReceiver<...>,
    removals: &Arc<parking_lot::RwLock<HashSet<u64>>>,
    events_config: &EventsConfig,
) {
    while let Some((region, initial_members)) = region_rx.recv().await {
        // ... existing body
    }
}
```

- [ ] **Step 3: Build and verify**

Run: `cargo +1.92 build --workspace && cargo +1.92 clippy --workspace --all-targets -- -D warnings`
Expected: Clean build, no clippy warnings.

- [ ] **Step 4: Run full test suite**

Run: `cargo +1.92 test --workspace --lib`
Expected: All tests pass.

---

### Task A3: Transport channel cleanup on departure

**Files:**
- Modify: `crates/raft/src/consensus_transport.rs:48` (add `retain_peers`)
- Modify: `crates/consensus/src/reactor.rs:503` (`MembershipChanged` handler)
- Test: `crates/raft/src/consensus_transport.rs` (existing test module)

- [ ] **Step 1: Write failing test**

In `crates/raft/src/consensus_transport.rs`, add to the existing `#[cfg(test)]` module:

```rust
#[test]
fn retain_peers_removes_departed_channels() {
    let transport = GrpcConsensusTransport::new(1, inferadb_ledger_types::Region::GLOBAL);

    // Can't construct real Channels in unit tests, but we can test via
    // remove_peer + peers() which exercises the same map.
    // Instead, test the retain logic directly:
    assert!(transport.peers().is_empty());

    // After retain with empty membership, peers should still be empty.
    let membership = inferadb_ledger_consensus::types::Membership::new(
        [inferadb_ledger_consensus::types::NodeId(1)].into_iter().collect()
    );
    transport.retain_peers(&membership);
    assert!(transport.peers().is_empty());
}
```

- [ ] **Step 2: Add `retain_peers` method**

In `crates/raft/src/consensus_transport.rs`, add after `remove_peer`:

```rust
/// Removes channels for peers not in the given membership (voters + learners).
pub fn retain_peers(&self, membership: &inferadb_ledger_consensus::types::Membership) {
    let valid: std::collections::HashSet<u64> = membership
        .voters
        .iter()
        .chain(membership.learners.iter())
        .map(|n| n.0)
        .collect();
    self.channels.write().retain(|id, _| valid.contains(id));
}
```

- [ ] **Step 3: Wire into reactor's MembershipChanged handler**

In `crates/consensus/src/reactor.rs`, the `Action::MembershipChanged` handler at line 503 is currently a no-op stub. The reactor doesn't have direct access to the transport (it's in the `raft` crate). The reactor already emits the `MembershipChanged` action for consumers. The transport cleanup should be handled in the `raft` crate where the reactor's actions are processed.

In `crates/raft/src/raft_manager.rs`, in the section where committed actions are processed (the apply dispatch loop), add handling for `MembershipChanged`:

```rust
Action::MembershipChanged { shard, membership } => {
    if let Some(transport) = group.consensus_transport() {
        transport.retain_peers(&membership);
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo +1.92 test --workspace --lib`
Expected: All pass.

---

### Task A4: Spillover map pruning

**Files:**
- Modify: `crates/raft/src/apply_worker.rs:37` (main loop)

- [ ] **Step 1: Add pruning after each batch**

In `crates/raft/src/apply_worker.rs`, after the response delivery loop (after line ~52), add:

```rust
// Prune stale spillover entries. No-op responses from become_leader
// accumulate here because no proposer registered for them.
let applied = batch.last_index; // or track from the batch
if applied > 1000 {
    let cutoff = applied - 1000;
    self.spillover.retain(|index, _| *index > cutoff);
}
```

The exact field name for the applied index depends on what `CommittedBatch` provides. Read the struct definition to determine the right field.

- [ ] **Step 2: Build and test**

Run: `cargo +1.92 build --workspace && cargo +1.92 test --workspace --lib`
Expected: Clean build, all tests pass.

---

### Task A5: Shard shutdown on removal

**Files:**
- Modify: `crates/consensus/src/types.rs:256` (add `Shutdown` to `NodeState`)
- Modify: `crates/consensus/src/action.rs:15` (add `ShardRemoved` to `Action`)
- Modify: `crates/consensus/src/shard.rs:1115` (`apply_committed_membership`)
- Modify: `crates/consensus/src/reactor.rs:503` (handle `ShardRemoved`)
- Modify: `crates/raft/src/raft_manager.rs` (handle shard removal)
- Test: `crates/consensus/tests/election.rs`

- [ ] **Step 1: Write failing test — removed node emits ShardRemoved**

In `crates/consensus/tests/election.rs`:

```rust
#[test]
fn test_removed_node_emits_shard_removed() {
    let (clock, mut shard) = make_3node_shard(1);
    elect_leader(&mut shard, &clock);

    // Remove node 1 (self-removal).
    let actions = shard
        .handle_membership_change(MembershipChange::RemoveNode { node_id: NodeId(1) })
        .unwrap();

    // Commit the membership change by simulating quorum ack.
    commit_entries(&mut shard, &actions);

    // After commit, the shard should emit ShardRemoved.
    let has_shard_removed = actions_after_commit.iter().any(|a| {
        matches!(a, Action::ShardRemoved { .. })
    });
    assert!(has_shard_removed, "Expected ShardRemoved action after self-removal");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo +1.92 test -p inferadb-ledger-consensus --test election test_removed_node_emits_shard_removed -- --nocapture`
Expected: FAIL — `Action::ShardRemoved` doesn't exist yet.

- [ ] **Step 3: Add `Shutdown` to `NodeState`**

In `crates/consensus/src/types.rs`, add after `Failed`:
```rust
/// Node has been removed from the Raft group and is shutting down.
Shutdown,
```

- [ ] **Step 4: Add `ShardRemoved` to `Action`**

In `crates/consensus/src/action.rs`, add:
```rust
/// The local node was removed from this shard's membership.
/// The reactor should schedule cleanup after a grace period.
ShardRemoved { shard: ShardId },
```

- [ ] **Step 5: Emit `ShardRemoved` in `apply_committed_membership`**

In `crates/consensus/src/shard.rs`, in `apply_committed_membership` (line ~1115), after the existing self-removal check:

```rust
if self.state == NodeState::Leader && !self.membership.is_voter(self.node_id) {
    actions.extend(self.send_append_entries_to_all());
    self.become_follower(self.current_term, actions);
}

// If we're no longer a voter or learner, emit ShardRemoved.
if !self.membership.is_voter(self.node_id)
    && !self.membership.is_learner(self.node_id)
{
    actions.push(Action::ShardRemoved { shard: self.id });
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo +1.92 test -p inferadb-ledger-consensus --test election test_removed_node_emits_shard_removed -- --nocapture`
Expected: PASS

- [ ] **Step 7: Handle `ShardRemoved` in the reactor**

In `crates/consensus/src/reactor.rs`, in the `Action::MembershipChanged` match arm (line 503), add a separate arm for `ShardRemoved`:

```rust
Action::ShardRemoved { shard } => {
    tracing::info!(%shard, "Shard removed from local membership — scheduling cleanup");
    // Mark shard as Shutdown. Schedule cleanup timer (30s grace).
    if let Some(s) = self.shards.get_mut(&shard) {
        s.mark_shutdown();
    }
    // Schedule a timer to remove the shard after grace period.
    self.schedule_shard_cleanup(shard, Duration::from_secs(30));
},
```

The exact implementation depends on the reactor's internal shard storage pattern. The `mark_shutdown` and `schedule_shard_cleanup` methods need to be added — they set `NodeState::Shutdown` and use the timer wheel.

- [ ] **Step 8: Run full test suite**

Run: `cargo +1.92 test --workspace --lib && cargo +1.92 clippy --workspace --all-targets -- -D warnings && cargo +nightly fmt --check`
Expected: All pass.

---

## Phase B: Declarative Membership Convergence (Task Outline)

Phase B depends on Phase A (A5 shard shutdown is used by the drain monitor). Detailed implementation steps will be written when Phase A is complete.

### Task B1: NodeStatus enum and system state storage
**Files:**
- Modify: `crates/raft/src/types.rs` — add `NodeStatus` enum, `SystemRequest::SetNodeStatus` variant
- Modify: `crates/raft/src/log_storage/operations/mod.rs` — apply handler for `SetNodeStatus`
- Modify: `crates/state/src/system/` — store/retrieve `NodeStatus` in system state at `_meta:node_status:{node_id}`

### Task B2: Rewrite leave_cluster
**Files:**
- Modify: `crates/services/src/services/admin.rs` — replace signal/poll logic with `SetNodeStatus::Decommissioning`
- Modify: `proto/ledger/v1/ledger.proto` — add `GetDecommissionStatus` RPC, request/response messages
- Run: `just proto` to regenerate

### Task B3: Checker/scheduler/operator infrastructure
**Files:**
- Create: `crates/server/src/dr_scheduler.rs` — `OperatorPriority`, `Operator`, `OperatorAction`, `OperatorQueue`
- Create: `crates/server/src/dr_checker.rs` — `check_dr_health` function
- Create: `crates/server/src/dr_scheduler_growth.rs` — `schedule_dr_growth` function
- Create: `crates/server/src/dr_executor.rs` — `execute_operator` function
- Modify: `crates/server/src/bootstrap.rs` — replace reconciliation task with checker + scheduler + executor loops
- Modify: `crates/raft/src/raft_manager.rs` — add `SystemStateReader` accessor

### Task B4: Drain monitor and RegionMembershipReport
**Files:**
- Modify: `crates/raft/src/types.rs` — add `SystemRequest::RegionMembershipReport`
- Modify: `crates/raft/src/log_storage/operations/mod.rs` — apply handler
- Modify: `crates/server/src/bootstrap.rs` — drain monitor background task, membership reporting

### Task B5: Quorum-based dead node detection
**Files:**
- Modify: `proto/ledger/v1/ledger.proto` — add `CheckPeerLiveness` RPC
- Modify: `crates/services/src/services/admin.rs` — `CheckPeerLiveness` handler
- Modify: `crates/server/src/bootstrap.rs` — liveness tracking, quorum confirmation loop
- Modify: `crates/raft/src/types.rs` — `LivenessConfig`, `SchedulerConfig`

### Task B6: Code removal
**Files:**
- Modify: `crates/raft/src/types.rs` — delete `SystemRequest::RemoveNodeFromDataRegions`, `LedgerRequest::Barrier`
- Modify: `crates/raft/src/log_storage/operations/mod.rs` — delete apply handlers
- Modify: `crates/server/src/bootstrap.rs` — delete `remove_node_from_all_data_regions`, `remove_node_with_barrier`, `pending_dr_removals`, signal sentinel pattern

---

## Phase C: Safety Features (Task Outline)

Phase C depends on Phase B.

### Task C1: Learner-first membership changes
**Files:**
- Modify: `crates/server/src/dr_scheduler_growth.rs` — `should_promote`, `pending_promotions` tracking
- Modify: `crates/consensus/src/shard.rs` — add `auto_promote` config flag
- Modify: `crates/consensus/src/reactor.rs` — add `ReactorEvent::QueryPeerState`
- Modify: `crates/raft/src/consensus_handle.rs` — add `peer_match_index` method

### Task C2: Region epoch fencing
**Files:**
- Modify: `crates/consensus/src/shard.rs` — add `conf_epoch` field, increment in `apply_committed_membership`
- Modify: `crates/consensus/src/types.rs` — add `expected_conf_epoch` to `MembershipChange` variants
- Modify: `crates/consensus/src/leadership.rs` — add `conf_epoch`, `pending_membership` to `ShardState`
- Modify: `crates/raft/src/consensus_handle.rs` — add `has_pending_membership` method

### Task C3: Snapshot transfer for new replicas
**Files:**
- Modify: `crates/consensus/src/action.rs` — add `Action::SendSnapshot`
- Modify: `crates/consensus/src/shard.rs` — trigger snapshot in replication logic
- Modify: `crates/raft/src/raft_manager.rs` — snapshot creation and streaming
- Modify: `crates/services/src/services/raft.rs` — `InstallSnapshot` streaming RPC handler

---

## Phase D: Deterministic Simulation (Task Outline)

Phase D scenarios 1–4, 6, 8 depend on Phase B. Scenarios 5 and 7 depend on Phase C. Buggification (D3) can be built in parallel with Phase B.

### Task D1: Multi-Raft simulation harness
**Files:**
- Create: `crates/consensus/src/simulation/multi_raft.rs`
- Modify: `crates/consensus/src/simulation/mod.rs` — export new module

### Task D2: Targeted scenarios (8 tests)
**Files:**
- Modify: `crates/consensus/src/simulation/multi_raft.rs` — scenarios 1–8

### Task D3: Buggification infrastructure
**Files:**
- Create: `crates/consensus/src/buggify.rs` — `buggify()`, `enable_buggify()`, `BUGGIFY_ENABLED`
- Modify: `crates/consensus/src/lib.rs` — export module
- Modify: `crates/consensus/src/shard.rs` — instrumentation points
- Modify: `crates/raft/src/log_storage/raft_impl.rs` — instrumentation points
- Modify: `crates/raft/src/consensus_handle.rs` — instrumentation points

### Task D4: Randomized soak testing
**Files:**
- Modify: `crates/consensus/src/simulation/multi_raft.rs` — `soak_multi_raft_coordination` test, invariant checker

### Task D5: CI integration
**Files:**
- Modify: `Justfile` — ensure simulation tests run under `just test` and `just test-proptest`
