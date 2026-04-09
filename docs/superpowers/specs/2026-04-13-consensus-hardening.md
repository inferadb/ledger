# Multi-Raft Consensus Hardening

**Date:** 2026-04-13
**Status:** Draft
**Crates:** `consensus`, `raft`, `services`, `server`
**Depends on:** [Custom Consensus Engine](2026-04-06-custom-consensus-engine.md) (sections 1–10, 20)

## Goal

Replace the imperative, signal-based multi-Raft membership coordination with a centralized scheduling model inspired by TiKV's Placement Driver. The GLOBAL Raft leader acts as the scheduler — it holds declarative desired state for all data region memberships. DR leaders read this state and converge. This eliminates fire-and-forget signals, timing-based polling, and reconciliation races.

The changes span four phases:

| Phase | Scope                   | Summary                                                             |
| ----- | ----------------------- | ------------------------------------------------------------------- |
| A     | Tactical hardening      | Five surgical fixes to the consensus engine and coordination layer  |
| B     | Declarative convergence | Node lifecycle state machine, DR scheduler, GLOBAL-driven drain     |
| C     | Safety features         | Learner-first promotion, epoch fencing, snapshot transfer           |
| D     | Simulation testing      | Multi-Raft deterministic simulation covering all coordination paths |

## Problem

The system runs one GLOBAL Raft group (cluster metadata) and N data region Raft groups (per-region vault data). These groups have independent leaders, independent membership, and independent election timers. Membership changes in one group must be coordinated with the others.

The current coordination uses:

- **Imperative signals**: `RemoveNodeFromDataRegions` proposed to GLOBAL, processed asynchronously by a channel-based handler on each node
- **Timing-based polling**: `leave_cluster` polls DR membership every 200ms for up to 15 seconds
- **Reconciliation safety net**: A 5-second periodic task that adds/removes DR members based on GLOBAL voter state
- **Pending-removal guards**: A shared `HashSet<u64>` that prevents the reconciliation from re-adding just-removed voters

This produced eight distinct bugs during lifecycle testing:

1. **Reconciliation race** — Reconciliation re-added a just-removed voter because the GLOBAL removal hadn't committed yet
2. **Self-removal quorum deadlock** — DR leader removed itself, leaving a 2-voter group where the other voter couldn't achieve quorum after the old leader was killed
3. **Signal ordering** — DR removal signal fired before GLOBAL removal, but reconciliation ran between them
4. **No-op decode failure** — Raft §5.4.2 no-op entries have empty data that failed postcard deserialization
5. **Non-founding member log conflict** — Nodes joining after DR creation started their own log, conflicting with the existing leader's log
6. **15-second timeout exceeded** — Self-removal leadership transfer + reconciliation tick exceeded the polling deadline
7. **Signal channel silent failure** — Receiver task panic silently dropped all subsequent signals
8. **Transport channel leak** — Departed nodes' gRPC channels accumulated with no cleanup

All eight are symptoms of one root cause: **no single authoritative source of truth for desired DR membership.** The desired state is implied by a sequence of imperative operations that race, are lost, or arrive in the wrong order.

## Architecture

The fix follows the TiKV Placement Driver pattern, adapted to our architecture where the GLOBAL Raft group already serves as the metadata store:

```
┌─────────────────────────────────────────────────────────────┐
│                    GLOBAL Raft Group                         │
│                                                             │
│  NodeStatus per node:  Active | Decommissioning | Dead      │
│  Peer liveness:        last_seen timestamps                 │
│  Drain monitor:        checks DR replica counts             │
│                                                             │
│  ► GLOBAL leader = Placement Driver                         │
│  ► Declares desired state, does not execute on DRs directly │
└──────────────────────────┬──────────────────────────────────┘
                           │ reads desired state
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ DR us-east   │  │ DR eu-west   │  │ DR ap-south  │
│              │  │              │  │              │
│  Scheduler:  │  │  Scheduler:  │  │  Scheduler:  │
│  Every 2s,   │  │  Every 2s,   │  │  Every 2s,   │
│  converge    │  │  converge    │  │  converge    │
│  toward      │  │  toward      │  │  toward      │
│  desired     │  │  desired     │  │  desired     │
│  state       │  │  state       │  │  state       │
└──────────────┘  └──────────────┘  └──────────────┘
```

**Data flow**: GLOBAL state changes (node joins, decommissions, dies) → each DR leader reads GLOBAL state → computes diff against local DR membership → executes one membership change per scheduler tick → converges.

**Why centralized (TiKV-style) over decentralized (CockroachDB-style)**: CockroachDB's decentralized model has each leaseholder independently decide replica placement. This eliminates a scheduling SPOF but has produced real bugs where multiple leaseholders make conflicting decisions about the same range (CockroachDB issue #32525). Our centralized model — one scheduler per DR, reading one source of truth (GLOBAL state) — avoids this class of bug entirely. The tradeoff is a scheduling gap during GLOBAL leader elections, but `Decommissioning` status survives in GLOBAL state and the new leader's scheduler resumes the drain automatically (Phase D scenario 8 tests this). For our context (single-digit node counts, handful of DRs, data residency constraints requiring global placement knowledge), the centralized model is simpler and more correct.

**What this eliminates**:

- `RemoveNodeFromDataRegions` system request and its apply handler
- `pending_dr_removals` shared set and its cleanup logic
- The signal channel sentinel pattern (`Region::GLOBAL` with node_id tuple)
- The polling loop in `leave_cluster`
- `remove_node_from_all_data_regions` function
- `remove_node_with_barrier` function
- The dual-purpose reconciliation task

---

## Phase A: Tactical Hardening

Five targeted fixes, including minimal engine extensions. No changes to the coordination architecture.

### A1. Non-voter election guard

**File**: `crates/consensus/src/shard.rs`

When a node is removed from a Raft group's voter set, it continues running as a follower. When the election timer fires, it sends pre-vote requests. Peers reject them, but this wastes messages and delays elections.

**Change**: In `start_pre_vote()`, add early return:

```rust
fn start_pre_vote(&mut self) -> Vec<Action> {
    if !self.membership.is_voter(self.node_id) {
        // Non-voter: reset timer, don't initiate elections.
        let mut actions = Vec::new();
        self.reset_election_timer(&mut actions);
        return actions;
    }
    // ... existing pre-vote logic
}
```

Same guard in `start_election()`. The node stays a silent follower, processing incoming AppendEntries but never initiating elections.

### A1b. No-op commit precondition for membership changes

**File**: `crates/consensus/src/shard.rs`

**Safety requirement from Ongaro's dissertation (§4.1) and [raft-dev bug report](https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E)**: A new leader must commit an entry from its current term before processing any membership changes. Without this, uncommitted config entries from a previous term can create non-overlapping quorums across term boundaries.

Our `become_leader` already appends a no-op entry (line 952), but there is a window between `become_leader` and the no-op committing where `handle_membership_change` could be called. The `pending_membership` flag does not block this because the no-op is a `Normal` entry, not a `Membership` entry.

**Change**: Add a `last_committed_term: u64` field to `Shard`, updated in `try_advance_commit` whenever `commit_index` advances:

```rust
// In try_advance_commit, after advancing commit_index:
if let Some(entry) = self.log_entry_at(self.commit_index) {
    self.last_committed_term = entry.term;
}
```

Then in `handle_membership_change`, add a guard:

```rust
// Reject membership changes until the leader has committed an entry
// in its current term. This closes the Ongaro single-server membership
// change bug (raft-dev 2015).
if self.last_committed_term != self.current_term {
    return Err(ConsensusError::LeaderNotReady);
}
```

Using `last_committed_term` instead of indexing into the log avoids breakage after log truncation/snapshotting (the `VecDeque` no longer starts at index 1 post-compaction). The `LeaderNotReady` error causes the checker/scheduler to retry on the next tick — by which time the no-op has long since committed.

### A2. Signal channel health monitoring

**File**: `crates/raft/src/log_storage/operations/mod.rs`, `crates/server/src/bootstrap.rs`

The `region_creation_sender` (UnboundedSender) silently drops signals if the receiver task panics. All DR membership changes stop with no detection.

**Changes**:

1. In the apply worker, replace `let _ = sender.send(...)` with logging on failure:

```rust
if sender.send(payload).is_err() {
    tracing::error!(
        "Region handler channel closed — DR membership changes will not be processed. \
         This node should be restarted."
    );
}
```

2. In bootstrap.rs, wrap the receiver task body in a panic recovery loop:

```rust
tokio::spawn(async move {
    loop {
        let result = std::panic::AssertUnwindSafe(process_region_signals(&mgr, &mut region_rx))
            .catch_unwind()
            .await;
        match result {
            Ok(()) => break, // channel closed normally
            Err(panic) => {
                tracing::error!(?panic, "Region handler panicked — restarting");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
});
```

3. Set a health flag (`AtomicBool`) on task exit. The node's health check reports `NOT_SERVING` if the flag is false.

### A3. Transport channel cleanup on departure

**File**: `crates/raft/src/consensus_transport.rs`, `crates/consensus/src/reactor.rs`

`GrpcConsensusTransport` only adds channels, never removes them.

**Change**: When the reactor processes `Action::MembershipChanged`, call a new `transport.retain_peers(new_membership)` method that removes channels for nodes not in the updated voter or learner set:

```rust
pub fn retain_peers(&self, membership: &Membership) {
    let valid: HashSet<u64> = membership.voters.iter()
        .chain(membership.learners.iter())
        .map(|n| n.0)
        .collect();
    self.channels.write().retain(|id, _| valid.contains(id));
}
```

### A4. Spillover map pruning

**File**: `crates/raft/src/apply_worker.rs`

No-op entries from `become_leader` generate responses in the `spillover` DashMap that accumulate forever.

**Change**: After processing each committed batch, prune stale spillover entries:

```rust
let applied = self.applied_index.load(Ordering::Acquire);
if applied > 1000 {
    let cutoff = applied - 1000;
    spillover.retain(|index, _| *index > cutoff);
}
```

### A5. Shard shutdown on removal

**Files**: `crates/consensus/src/shard.rs`, `crates/consensus/src/reactor.rs`, `crates/raft/src/raft_manager.rs`

After a node is removed from a Raft group, the shard keeps running indefinitely.

**Changes**:

1. Add `Shutdown` variant to `NodeState`.

2. In `apply_committed_membership`, when the local node is no longer a voter or learner, emit `Action::ShardRemoved { shard }`.

3. The reactor handles `ShardRemoved` by:
   - Marking the shard as `NodeState::Shutdown`
   - Scheduling a cleanup timer (30 seconds grace period for in-flight messages)
   - On timer expiry: removing the shard from the groups map

4. `RaftManager` listens for `ShardRemoved` and removes the `RegionGroup`, closing the WAL segment and state.db.

---

## Phase B: Declarative Membership Convergence

The core architectural change. Replaces imperative signal machinery with declarative state convergence.

### B1. Node lifecycle state machine

**Files**: `crates/raft/src/types.rs`, `crates/raft/src/log_storage/operations/mod.rs`, `crates/services/src/services/admin.rs`

Add `NodeStatus` to the system state layer, stored per node_id:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Normal operation. Participates in all assigned Raft groups.
    Active,
    /// Operator-initiated departure. DRs drain replicas. Node serves reads.
    Decommissioning,
    /// System-detected failure (no heartbeat within dead_node_timeout).
    /// DRs drain and re-replicate.
    Dead,
    /// Terminal. Node removed from GLOBAL, all DR replicas drained.
    Removed,
}
```

**State transitions** (proposed as GLOBAL Raft entries):

```
Active → Decommissioning    via LeaveCluster / DecommissionNode RPC
Active → Dead               via dead-node detector (B5)
Decommissioning → Removed   via drain monitor (B4), after all DR replicas gone
Dead → Removed              via drain monitor (B4), after all DR replicas gone
Dead → Active               via ReactivateNode RPC (operator resurrection)
```

Each transition is a `SystemRequest::SetNodeStatus { node_id, status }` entry in the GLOBAL log. All nodes apply it deterministically.

**Storage**: `NodeStatus` is stored in the system state's `node_directory` map (existing infrastructure for node metadata). Keyed by `_meta:node_status:{node_id}`.

### B2. Rewrite `leave_cluster`

**File**: `crates/services/src/services/admin.rs`

Current flow (7 steps, 3 can race):

1. Propose `RemoveNodeFromDataRegions` signal to GLOBAL
2. Wait for signal to commit
3. Poll DR membership for up to 15 seconds
4. Remove from GLOBAL Raft
5. Wait for GLOBAL commit
6. Return success

New flow (2 steps, no races):

1. Propose `SetNodeStatus { node_id, Decommissioning }` to GLOBAL via `propose_and_wait`
2. Return `{ success: true, message: "Decommissioning started" }`

The handler no longer touches DR membership. No signals, no polling, no timeouts. The DR scheduler (B3) handles replica drain asynchronously.

**New RPC: `GetDecommissionStatus`**

Returns progress for a decommissioning node:

```protobuf
message GetDecommissionStatusRequest {
  uint64 node_id = 1;
}

message GetDecommissionStatusResponse {
  string status = 1;                          // "decommissioning", "removed", "not_found"
  repeated DataRegionReplica remaining = 2;   // DRs still hosting this node
  bool global_removed = 3;                    // Whether GLOBAL membership removal is complete
}

message DataRegionReplica {
  string region = 1;
  string role = 2;    // "voter" or "learner"
}
```

Scripts and operators poll this instead of relying on `leave_cluster` to block.

### B3. DR membership coordination: checker/scheduler separation

**File**: New module replacing `reconcile_data_region_membership` in `crates/server/src/bootstrap.rs`

Following TiKV's Placement Driver architecture, DR membership coordination is split into two independent components with distinct roles and cadences:

- **Checkers** are reactive — they patrol DR membership looking for problems (dead replicas, decommissioning replicas, stalled learners). They generate high-priority operators.
- **Schedulers** are proactive — they look for growth opportunities (missing replicas, learners ready for promotion). They generate lower-priority operators.

Both run on DR leaders. Checkers run every 1 second (fast detection). Schedulers run every 5 seconds (no urgency for growth). A shared **operator queue** serializes execution — one operation at a time per DR, highest priority wins.

**How both read GLOBAL state**: Every node hosts a GLOBAL replica. Both checker and scheduler read from the local GLOBAL `StateLayer` via a `SystemStateReader` — a read-only accessor exposed by the GLOBAL `RegionGroup`. This is a local read (no network hop), but it may be stale if the local GLOBAL follower is behind the GLOBAL leader. This is safe: **stale GLOBAL reads cause delayed convergence, not incorrect convergence.** Every mutation is validated by the DR's own Raft membership protocol (which is authoritative). The epoch fencing in C2 provides defense-in-depth.

```rust
impl RaftManager {
    /// Returns a read-only accessor for the GLOBAL region's applied state.
    pub fn system_state_reader(&self) -> Option<SystemStateReader> {
        self.system_region().ok().map(|g| g.state_reader())
    }
}
```

**Desired state derivation** (pure function, used by both checker and scheduler):

```rust
fn desired_dr_voters(
    global_state: &SystemState,
    region: Region,
) -> BTreeSet<NodeId> {
    global_state.voters()
        .filter(|node_id| {
            let status = global_state.node_status(*node_id);
            status == NodeStatus::Active
        })
        .filter(|node_id| {
            region.is_eligible(*node_id, global_state)
        })
        .collect()
}
```

#### Operator model

Both checkers and schedulers produce `Operator` values. A per-DR operator queue holds at most one pending operator. The executor runs continuously, pulling and executing one operator at a time.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OperatorPriority {
    /// Dead node removal — immediate safety concern.
    RepairCritical = 0,
    /// Decommissioning voter removal — operator-initiated, urgent.
    RepairNormal = 1,
    /// Stalled learner removal — cleanup.
    RepairLow = 2,
    /// Learner promotion — convergence toward desired state.
    Promote = 3,
    /// New learner addition — proactive growth.
    Add = 4,
    /// Leadership transfer for self-removal — same priority as RepairNormal
    /// because it's a prerequisite for the node's own removal.
    TransferLeader = 1,
}

struct Operator {
    region: Region,
    action: OperatorAction,
    priority: OperatorPriority,
    expected_conf_epoch: u64,
    created_at: Instant,
}

enum OperatorAction {
    RemoveVoter { node_id: NodeId },
    RemoveLearner { node_id: NodeId },
    AddLearner { node_id: NodeId },
    PromoteLearner { node_id: NodeId },
    TransferLeader { from: NodeId, to: NodeId },
}
```

The queue is a bounded priority queue (max depth = number of DR voters, practically ≤7). Checker and scheduler both push operators; the executor pops and executes the highest-priority one. One execution per tick. When a higher-priority operator is submitted, it sorts ahead of lower-priority pending operators. Operators that become stale (epoch mismatch at execution time) are silently discarded — the checker/scheduler regenerates them on the next cycle.

#### Checker: reactive repair (every 1 second)

```rust
fn check_dr_health(
    group: &RegionGroup,
    desired: &BTreeSet<NodeId>,
    global_state: &SystemState,
) -> Vec<Operator> {
    let state = group.handle().shard_state();
    let current: BTreeSet<NodeId> = state.voters.iter().copied().collect();
    let epoch = state.conf_epoch;
    let mut ops = Vec::new();

    for voter in &current {
        if desired.contains(voter) {
            continue; // Healthy — should be here.
        }

        let status = global_state.node_status(voter.0);
        let priority = match status {
            NodeStatus::Dead => OperatorPriority::RepairCritical,
            NodeStatus::Decommissioning => OperatorPriority::RepairNormal,
            _ => continue, // Active node not in desired set is a transient state; skip.
        };

        // CockroachDB safety: only remove Decommissioning voters if
        // the current voter count exceeds min_voters. Dead voters are
        // always removable (they're already unavailable).
        if priority == OperatorPriority::RepairNormal
            && current.len() <= config.min_voters_per_region
        {
            continue; // Can't remove without replacement.
        }

        // Self-removal: emit TransferLeader instead of RemoveVoter.
        if voter.0 == local_node_id && group.handle().is_leader() {
            if let Some(target) = desired.iter().find(|n| **n != *voter) {
                ops.push(Operator {
                    action: OperatorAction::TransferLeader {
                        from: *voter,
                        to: *target,
                    },
                    priority: OperatorPriority::TransferLeader,
                    expected_conf_epoch: epoch,
                    ..
                });
                continue;
            }
            // No transfer target — sole remaining voter. Remove directly.
        }

        ops.push(Operator {
            action: OperatorAction::RemoveVoter { node_id: *voter },
            priority,
            expected_conf_epoch: epoch,
            ..
        });
    }

    // Check for stalled learners.
    for learner in &state.learners {
        if let Some(added_at) = pending_promotions.get(learner) {
            if added_at.elapsed() > config.learner_stall_timeout {
                ops.push(Operator {
                    action: OperatorAction::RemoveLearner { node_id: *learner },
                    priority: OperatorPriority::RepairLow,
                    expected_conf_epoch: epoch,
                    ..
                });
            }
        }
    }

    ops
}
```

#### Scheduler: proactive growth (every 5 seconds)

```rust
fn schedule_dr_growth(
    group: &RegionGroup,
    desired: &BTreeSet<NodeId>,
) -> Vec<Operator> {
    let state = group.handle().shard_state();
    let current: BTreeSet<NodeId> = state.voters.iter().copied().collect();
    let current_and_learners: BTreeSet<NodeId> = current.union(
        &state.learners.iter().copied().collect()
    ).copied().collect();
    let epoch = state.conf_epoch;
    let mut ops = Vec::new();

    // Promote caught-up learners.
    for learner in &state.learners {
        if desired.contains(learner) && should_promote(group, learner) {
            ops.push(Operator {
                action: OperatorAction::PromoteLearner { node_id: *learner },
                priority: OperatorPriority::Promote,
                expected_conf_epoch: epoch,
                ..
            });
            return ops; // One at a time.
        }
    }

    // Add missing voters as learners (one at a time).
    if let Some(new_node) = desired.difference(&current_and_learners).next() {
        ops.push(Operator {
            action: OperatorAction::AddLearner { node_id: *new_node },
            priority: OperatorPriority::Add,
            expected_conf_epoch: epoch,
            ..
        });
    }

    ops
}
```

#### Executor: one operation at a time

```rust
async fn execute_operator(
    group: &RegionGroup,
    op: Operator,
    global_state: &SystemState,
) -> Result<()> {
    // Epoch check (Phase C2): reject if DR membership changed since
    // operator creation. In Phase B, expected_conf_epoch is None and
    // this check is skipped. Phase C2 activates it.
    if let Some(expected) = op.expected_conf_epoch {
        let state = group.handle().shard_state();
        if state.conf_epoch != expected {
            return Err(StaleEpoch); // Checker/scheduler will regenerate.
        }
    }
    if group.handle().has_pending_membership() {
        return Err(Busy); // Retry on next cycle.
    }

    // Re-verify NodeStatus before removals. Guards against a race where
    // a Dead node was reactivated via ReactivateNode between operator
    // creation and execution.
    match &op.action {
        OperatorAction::RemoveVoter { node_id } => {
            let status = global_state.node_status(node_id.0);
            if status == NodeStatus::Active {
                return Err(StaleOperator); // Node was reactivated.
            }
        },
        _ => {},
    }

    match op.action {
        OperatorAction::RemoveVoter { node_id } => {
            group.handle().remove_node(node_id.0).await?;
            propose_empty_barrier(group).await;
        },
        OperatorAction::RemoveLearner { node_id } => {
            group.handle().remove_node(node_id.0).await?;
        },
        OperatorAction::AddLearner { node_id } => {
            group.handle().add_learner(node_id.0, false).await?;
        },
        OperatorAction::PromoteLearner { node_id } => {
            group.handle().promote_voter(node_id.0).await?;
        },
        OperatorAction::TransferLeader { to, .. } => {
            group.handle().transfer_leader(to.0).await?;
        },
    }
    Ok(())
}

async fn propose_empty_barrier(group: &RegionGroup) {
    let _ = group.handle().propose_and_wait(
        RaftPayload::new_empty(), Duration::from_secs(5)
    ).await;
}
```

#### Why checker/scheduler separation matters

The single-loop model we started with had a structural problem: if a learner addition was in the operator queue, a dead-node removal had to wait for the next tick (2 seconds). With checker/scheduler separation:

- The checker runs every 1 second and generates `RepairCritical` operators for dead nodes.
- These preempt any pending `Add` or `Promote` operator in the queue.
- Dead node removal latency drops from "next 2s tick" to "next 1s checker cycle."

This follows TiKV's design where `ReplicaChecker` (reactive) and `balance-region-scheduler` (proactive) run independently, with checker-generated operators having higher priority.

**What this replaces**:

- `remove_node_from_all_data_regions` — removal is now a scheduler decision
- `remove_node_with_barrier` — barrier logic moves into the scheduler
- `pending_dr_removals` — not needed; `NodeStatus` prevents re-add by excluding `Decommissioning`/`Dead` from desired state
- `RemoveNodeFromDataRegions` system request — not needed; scheduler reads GLOBAL state directly
- The signal channel sentinel pattern — not needed
- The polling loop in `leave_cluster` — not needed; `leave_cluster` returns immediately

### B4. Drain monitor and GLOBAL removal

**File**: `crates/server/src/bootstrap.rs`

A background task on the GLOBAL leader monitors decommissioning and dead nodes. Every 5 seconds:

```rust
async fn drain_monitor_tick(global_state: &SystemState) {
    for (node_id, status) in global_state.node_statuses() {
        if status != NodeStatus::Decommissioning && status != NodeStatus::Dead {
            continue;
        }

        // Check GLOBAL-stored DR membership — NOT local list_regions().
        // The GLOBAL leader may not host all DRs (regulated regions restrict
        // membership to specific nodes). DR leaders report their membership
        // to GLOBAL via RegionMembershipReport entries (see below).
        let has_replicas = global_state.region_memberships()
            .any(|(_, members)| members.contains(&node_id));

        if !has_replicas {
            // All DR replicas drained. Remove from GLOBAL and mark Removed.
            propose_remove_node(node_id).await;
            propose_set_node_status(node_id, NodeStatus::Removed).await;
        }
    }
}
```

**DR membership reporting**: Each DR leader proposes a `RegionMembershipReport { region, voters, learners, conf_epoch }` entry to the GLOBAL log in two situations: (1) **event-driven** — immediately after any DR membership change commits (removal, promotion, learner addition), and (2) **periodic heartbeat** — every 10 seconds as a backstop for missed events or crash recovery. This keeps GLOBAL's view of DR membership up to date without requiring the GLOBAL leader to host every DR. The drain monitor reads these reports from GLOBAL state, not from local `list_regions()`.

```rust
pub struct SystemRequest {
    // ... existing variants ...
    RegionMembershipReport {
        region: Region,
        voters: Vec<u64>,
        learners: Vec<u64>,
        conf_epoch: u64,
    },
}
```

Stored in GLOBAL state at `_meta:region_membership:{region}`. Updated on every report; stale reports (lower `conf_epoch`) are ignored.

**Safety property**: GLOBAL removal never happens while DR replicas are active. Decommission stalls if no replacement nodes are available. The minimum voter count policy is configurable via `SchedulerConfig::min_voters_per_region` (default: 1). The scheduler refuses to remove a voter if doing so would drop below this minimum, unless the node is `Dead` (in which case it's already unavailable and the removal reflects reality).

### B5. Dead node detection (quorum-based)

**Files**: `crates/server/src/bootstrap.rs`, `crates/services/src/services/admin.rs`

Single-observer liveness (where only the GLOBAL leader decides if a node is dead) is vulnerable to asymmetric network partitions: the GLOBAL leader can't reach node X, but other nodes can. This causes false `Dead` markings, unnecessary re-replication, and operator intervention to resurrect the node. Production systems avoid this — CockroachDB uses a distributed liveness table, TiKV's PD receives independent heartbeats from every store.

Our approach: **quorum-confirmed liveness.** Every node tracks peer liveness locally. Before the GLOBAL leader marks a node Dead, it queries other GLOBAL voters and requires a majority to confirm unreachability.

#### Per-node liveness tracking

Every node maintains a `peer_liveness: HashMap<u64, Instant>` for all known peers. Updated on:

- Receiving an AppendEntries or AppendEntriesResponse for any shard
- Successful gRPC response from the node (any RPC)
- Processing a `RegisterPeerAddress` entry for the node

This is local state — not replicated, not persisted. Each node has its own independent view of which peers are reachable.

#### GLOBAL leader liveness check (every 30 seconds)

When the GLOBAL leader's own `last_seen[X]` exceeds `dead_node_timeout`, it does NOT immediately mark node X as Dead. Instead, it initiates a quorum confirmation:

```rust
async fn liveness_check(
    local_liveness: &HashMap<u64, Instant>,
    global_handle: &ConsensusHandle,
    peer_addresses: &PeerAddressMap,
    config: &LivenessConfig,
) {
    // Gate: only run if we're still GLOBAL leader. A partitioned leader
    // can't commit proposals anyway, but this avoids wasted RPCs and
    // misleading log messages.
    if !global_handle.is_leader() {
        return;
    }

    let now = Instant::now();
    for (node_id, last_seen) in local_liveness {
        // Never evaluate our own liveness — if we're running, we're alive.
        if *node_id == local_node_id {
            continue;
        }
        if now.duration_since(*last_seen) <= config.dead_node_timeout {
            continue; // Reachable from our perspective.
        }

        let current_status = global_state.node_status(*node_id);
        if current_status != NodeStatus::Active {
            continue; // Already Decommissioning or Dead.
        }

        // Quorum confirmation: ask other GLOBAL voters if they can reach this node.
        // Queries run concurrently with a hard wall-clock budget to avoid
        // sequential timeout accumulation under network degradation.
        let voters: Vec<u64> = global_handle.shard_state().voters
            .iter().map(|n| n.0)
            .filter(|id| *id != local_node_id && *id != *node_id)
            .collect();

        let mut unreachable_votes = 1u32; // Count ourselves (we already know it's unreachable).
        let mut reachable_votes = 0u32;
        let quorum = (global_handle.shard_state().voters.len() / 2) + 1;

        // Fan out queries concurrently, collect with wall-clock budget.
        let futures: Vec<_> = voters.iter()
            .map(|vid| query_peer_liveness(*vid, *node_id, peer_addresses, config))
            .collect();
        let results = tokio::time::timeout(
            config.liveness_cycle_budget,  // default: 10 seconds
            futures::future::join_all(futures),
        ).await.unwrap_or_default();

        for result in results {
            match result {
                LivenessResponse::Unreachable => unreachable_votes += 1,
                LivenessResponse::Reachable => reachable_votes += 1,
                LivenessResponse::Unknown => {},
            }
        }

        if unreachable_votes >= quorum as u32 {
            tracing::warn!(
                node_id,
                unreachable_votes,
                reachable_votes,
                "Quorum confirms node unreachable — marking Dead"
            );
            propose_set_node_status(*node_id, NodeStatus::Dead).await;
        } else {
            tracing::info!(
                node_id,
                unreachable_votes,
                reachable_votes,
                "Node unreachable from leader but reachable from quorum — not marking Dead"
            );
        }
    }
}
```

#### Liveness query RPC

A lightweight RPC on the admin service:

```protobuf
message CheckPeerLivenessRequest {
  uint64 target_node_id = 1;
}

message CheckPeerLivenessResponse {
  bool reachable = 1;          // Whether this node has heard from the target recently
  uint64 last_seen_ago_ms = 2; // Milliseconds since last contact (for debugging)
}
```

The handler checks the local `peer_liveness` map:

```rust
async fn check_peer_liveness(&self, req: CheckPeerLivenessRequest) -> CheckPeerLivenessResponse {
    let reachable = self.peer_liveness
        .get(&req.target_node_id)
        .map_or(false, |last_seen| {
            last_seen.elapsed() < self.config.dead_node_timeout
        });
    CheckPeerLivenessResponse {
        reachable,
        last_seen_ago_ms: self.peer_liveness
            .get(&req.target_node_id)
            .map_or(u64::MAX, |ls| ls.elapsed().as_millis() as u64),
    }
}
```

The GLOBAL leader calls this on each voter with a short timeout (2 seconds). If the query itself fails (can't reach the voter), that voter's response is `Unknown` and doesn't count toward either quorum. This prevents a partitioned leader from accumulating false `Unreachable` votes from voters it can't reach.

#### Why quorum-based, not gossip

Gossip-based liveness (where nodes propagate "I saw X" messages) introduces consistency challenges — stale gossip can delay dead-node detection or cause conflicting views. The quorum query is simpler: the GLOBAL leader asks a direct question, gets a direct answer, and makes a majority-based decision. The cost is one RPC per voter per suspected-dead node per 30-second cycle — negligible.

#### Configuration

```rust
pub struct LivenessConfig {
    /// Time after which a node is suspected dead by the local observer.
    /// Default: 5 minutes. Deliberately long to avoid thrashing.
    pub dead_node_timeout: Duration,

    /// How often the GLOBAL leader runs the liveness check. Default: 30 seconds.
    pub liveness_check_interval: Duration,

    /// Timeout for each CheckPeerLiveness RPC during quorum confirmation.
    /// Default: 2 seconds. Kept short so the liveness check doesn't block.
    pub liveness_query_timeout: Duration,

    /// Hard wall-clock budget for the entire quorum confirmation per suspect.
    /// Concurrent queries are cancelled if this budget is exceeded.
    /// Default: 10 seconds.
    pub liveness_cycle_budget: Duration,
}
```

#### Known limitation: 2-node clusters

In a 2-node GLOBAL cluster, the voter filter excludes both the leader and the suspect, leaving no voters to query. Quorum is never reached. Dead node detection is inoperative. This is consistent with the existing spec's voter count guidance: "2 voters: Functional but WORSE than 1 — requires both nodes up for writes, with no fault tolerance advantage." For 2-node clusters, the operator must handle node failures manually. Dead node detection requires N ≥ 3.

#### Resurrection

If a dead node comes back online and contacts a peer, the peer responds with the node's current status. The node can:

- Re-join via `JoinCluster` (operator action) — gets fresh `Active` status
- Be reactivated via `ReactivateNode` admin RPC — clears `Dead` back to `Active` if the DR checker hasn't finished re-replicating

---

## Phase C: Safety Features

Three features that make the scheduler's decisions safer and faster.

### C1. Learner-first membership changes

**Files**: `crates/server/src/bootstrap.rs` (scheduler), `crates/consensus/src/shard.rs`

Today, `add_learner` is followed by `promote_voter` with a 200ms sleep. The new voter counts toward quorum immediately, even if it has zero log entries.

**Change**: The DR scheduler (B3) splits addition into two phases across separate ticks:

**Tick N — Add learner**: `add_learner(node_id)`. Node receives log replication but doesn't affect quorum. The scheduler records `(node_id, added_at_tick)` in a local `pending_promotions` map.

**Tick N+k — Promote when caught up**: Check learner progress via peer state:

```rust
fn should_promote(group: &RegionGroup, learner_id: &NodeId) -> bool {
    let leader_log_len = group.handle().log_len();

    // Query per-peer match_index via the consensus engine.
    let learner_match = group.handle().peer_match_index(learner_id.0)
        .unwrap_or(0);

    let lag = leader_log_len.saturating_sub(learner_match);
    lag < group.config().auto_promote_threshold  // default: 100
}
```

`auto_promote_threshold` already exists in `ShardConfig` (consensus engine spec, default 100).

**New API: `ConsensusHandle::peer_match_index`**: Returns a learner or follower's `match_index` — the highest log index confirmed by that peer. This is currently private (`peer_states` on `Shard`). Expose it via a new `ReactorEvent::QueryPeerState` that the reactor handles by reading `peer_states` and returning via a oneshot:

```rust
impl ConsensusHandle {
    pub async fn peer_match_index(&self, node: LedgerNodeId) -> Option<u64> {
        let (tx, rx) = oneshot::channel();
        self.engine.inbox.send(ReactorEvent::QueryPeerState {
            shard: self.shard,
            node: NodeId(node),
            response: tx,
        }).await.ok()?;
        rx.await.ok().flatten()
    }
}
```

**Disabling shard auto-promote**: The shard already has internal auto-promote logic that promotes learners when they catch up within `auto_promote_threshold`. This conflicts with the scheduler's multi-tick learner-first flow. When the scheduler manages membership (Phase B+), set `ShardConfig::auto_promote: false` to disable the shard's internal promotion. The scheduler handles all promotion decisions. On shards without a scheduler (e.g., the GLOBAL shard), `auto_promote` remains `true` for backwards compatibility.

**Constraints**:

- One learner per DR at a time (prevents leader fan-out overload)
- `learner_stall_timeout` (default: 5 minutes) — if a learner doesn't catch up within this window, the scheduler removes it and retries with another target if available
- Promotion requires the epoch check (C2) to confirm the DR hasn't changed since the learner was added

### C2. Region epoch fencing

**Files**: `crates/consensus/src/shard.rs`, `crates/consensus/src/types.rs`

Add a monotonic `conf_epoch: u64` to each shard, incremented on every committed membership change. This maps to TiKV's `conf_ver` component of `RegionEpoch`. TiKV uses a second component (`version`) for splits/merges — when InferaDB ships region split/merge (consensus engine spec §19), a `split_epoch` field should be added alongside `conf_epoch`.

**Shard changes**:

```rust
pub struct Shard {
    // ... existing fields ...
    conf_epoch: u64,  // NEW: starts at 0, incremented in apply_committed_membership
}
```

In `apply_committed_membership`:

```rust
self.membership = new_membership.clone();
self.conf_epoch += 1;
self.pending_membership = false;
```

**MembershipChange gains epoch field**:

```rust
pub enum MembershipChange {
    AddLearner { node_id: NodeId, expected_conf_epoch: Option<u64> },
    PromoteVoter { node_id: NodeId, expected_conf_epoch: Option<u64> },
    RemoveNode { node_id: NodeId, expected_conf_epoch: Option<u64> },
}
```

**Validation in `handle_membership_change`**:

```rust
if let Some(expected) = change.expected_conf_epoch() {
    if expected != self.conf_epoch {
        return Err(ConsensusError::StaleEpoch {
            expected,
            actual: self.conf_epoch,
        });
    }
}
```

On `StaleEpoch`, the scheduler re-reads DR state on its next tick and recomputes. No retry loop.

**Exposed via `ShardState`**:

```rust
pub struct ShardState {
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
    pub leader: Option<NodeId>,
    pub conf_epoch: u64,         // NEW (Phase C2)
    pub pending_membership: bool, // NEW (needed by B3 scheduler)
}
```

The `pending_membership` field is already tracked in the `Shard` struct but not currently exposed through `shard_state()`. The scheduler needs it to avoid proposing membership changes while one is in flight.

`ConsensusHandle` gains a convenience method:

```rust
pub fn has_pending_membership(&self) -> bool {
    self.shard_state().pending_membership
}
```

**Why `conf_epoch` alone is sufficient (no GLOBAL term fencing needed)**: A stale GLOBAL leader's scheduling decisions would reference an outdated DR `conf_epoch` and be rejected by the DR. The DR's own epoch is authoritative because it's part of the DR's committed Raft state. Adding GLOBAL term checking would create a second coordination mechanism that couples DR and GLOBAL election lifecycles unnecessarily — every GLOBAL leader election would create an availability gap where DR membership changes are rejected until DR leaders observe the new term. This matches TiKV's approach: PD operators carry the region epoch, not the PD election term.

### C3. Snapshot transfer for new replicas

**Files**: `crates/consensus/src/shard.rs`, `crates/raft/src/raft_manager.rs`, `crates/services/src/services/raft.rs`

When a learner's `match_index` is zero (brand new) or behind the leader's `last_snapshot_index` (log was truncated), the leader triggers snapshot transfer instead of sending the full log.

**Trigger** (in the leader's replication logic):

```rust
fn send_entries_to_peer(&mut self, peer: &mut PeerState) -> Vec<Action> {
    if peer.next_index <= self.last_snapshot_index {
        // Peer is too far behind — need snapshot, not log entries.
        return vec![Action::SendSnapshot {
            to: peer.id,
            shard: self.id,
        }];
    }
    // ... normal AppendEntries logic
}
```

**Snapshot flow**:

1. Leader creates a COW snapshot of the DR's state.db (reflink or file copy, per consensus engine spec section 9)
2. Leader streams the encrypted snapshot to the learner via `InstallSnapshot` streaming RPC
3. Learner replaces its state.db with the received snapshot, sets `last_applied` to the snapshot's `last_included_index`
4. Learner resumes normal log replication from `last_included_index + 1`

**New Action variant**:

```rust
pub enum Action {
    // ... existing variants ...
    SendSnapshot { to: NodeId, shard: ShardId },
}
```

The reactor handles `SendSnapshot` by delegating to the `RaftManager`, which creates the snapshot and initiates the streaming transfer.

**Integration boundary**: The reactor lives in the `consensus` crate, which has no dependency on `raft` (the reactor doesn't know about `RaftManager`). Snapshot delegation flows through a callback channel provided by the `raft` crate when constructing the reactor — the same pattern used for committed entry dispatch to the apply worker. The `raft` crate registers a `SnapshotSender` trait implementation that the reactor invokes via `Action::SendSnapshot`.

**Interaction with learner-first**: Snapshot transfer happens while the node is a learner. The scheduler doesn't promote until: (a) snapshot is installed, AND (b) the learner catches up on entries committed after the snapshot. The `should_promote` check in C1 handles this naturally — `match_index` advances after snapshot install + log catch-up.

**Configuration**:

```rust
pub struct SnapshotConfig {
    /// Entries behind leader before snapshot transfer is triggered.
    /// Default: 50,000 (from consensus engine spec max_replication_lag).
    pub snapshot_transfer_threshold: u64,

    /// Maximum concurrent snapshot transfers per node.
    /// Default: 2 (prevents disk I/O saturation).
    pub max_concurrent_snapshots: usize,
}
```

---

## Phase D: Deterministic Simulation

Extends the existing simulation harness to cover multi-Raft coordination.

### D1. Multi-Raft simulation harness

**File**: New `crates/consensus/src/simulation/multi_raft.rs`

The existing `SimulationHarness` (`crates/consensus/src/simulation/harness.rs`) already supports multi-node, multi-shard simulations with partitions, kills, deterministic clock/RNG, and message delivery. The multi-Raft harness **extends** the existing `Simulation` struct (not a parallel framework) by adding `NodeStatus` tracking, `SchedulerState`, and crash/restart semantics.

**Architecture**:

```rust
pub struct MultiRaftSimulation {
    /// Simulated nodes, each running GLOBAL + N data region shards.
    nodes: HashMap<NodeId, SimNode>,
    /// Deterministic message delivery with injectable faults.
    network: SimNetwork,
    /// Deterministic clock, manually advanced.
    clock: SimClock,
    /// Deterministic RNG for election timeouts.
    rng: SimRng,
}

pub struct SimNode {
    /// GLOBAL shard instance (real Shard code).
    global_shard: Shard<SimClock, SimRng>,
    /// Data region shards (real Shard code).
    data_shards: HashMap<Region, Shard<SimClock, SimRng>>,
    /// Node status as seen by this node.
    node_statuses: HashMap<NodeId, NodeStatus>,
    /// Per-DR checker state (last check time, pending operators).
    checker_state: HashMap<Region, CheckerState>,
    /// Per-DR scheduler state (pending promotions, operator queue).
    scheduler_state: HashMap<Region, SchedulerState>,
    /// Per-DR operator queue (priority-ordered, one active at a time).
    operator_queues: HashMap<Region, BinaryHeap<Operator>>,
    /// Whether this node is "crashed" (stopped processing).
    alive: bool,
}
```

Each `SimNode` wraps real `Shard` instances — the same code that runs in production. The simulation drives them with `handle_message`, `handle_election_timeout`, `handle_heartbeat_timeout`, and `handle_propose` calls. The `SimNetwork` queues messages and delivers them on `tick()` with configurable behavior.

**SimNetwork primitives**:

```rust
impl SimNetwork {
    /// Normal delivery — message arrives on next tick.
    fn deliver(&mut self, from: NodeId, to: NodeId, shard: ShardId, msg: Message);

    /// Partition — drop all messages between A and B (bidirectional).
    fn partition(&mut self, a: NodeId, b: NodeId);

    /// Isolate — drop all messages to/from A.
    fn isolate(&mut self, node: NodeId);

    /// Heal — restore normal delivery between A and B.
    fn heal(&mut self, a: NodeId, b: NodeId);

    /// Heal all — restore all connections.
    fn heal_all(&mut self);

    /// Crash — stop processing events for a node. Messages are dropped.
    fn crash(&mut self, node: NodeId);

    /// Restart — resume a crashed node with its persisted shard state.
    fn restart(&mut self, node: NodeId);

    /// Advance one tick — deliver all pending messages, fire expired timers.
    fn tick(&mut self) -> Vec<(NodeId, ShardId, Vec<Action>)>;
}
```

### D2. Targeted scenarios

Each scenario is a `#[test]` function. Runs to completion in milliseconds. These test specific known-dangerous interactions.

**Scenario 1 — Graceful decommission (4→3)**:
Boot 4-node cluster, create DR, propose 100 entries. Decommission node 1. Assert: DR voters converge to {2, 3, 4}, all 100 entries readable.

**Scenario 2 — Sequential decommission (4→1, the lifecycle test)**:
Boot 4-node cluster. Decommission nodes 1, 2, 3 sequentially, crash each after drain. Assert: node 4 is sole DR voter, can commit, no quorum deadlock.

**Scenario 3 — DR leader decommissions itself**:
Boot 3-node cluster, ensure node 1 is DR leader. Decommission node 1. Assert: checker generates `TransferLeader` operator, new leader completes removal.

**Scenario 4a — Dead node detection (quorum confirms)**:
Boot 3-node cluster. Isolate node 3 from ALL other nodes. Advance clock past `dead_node_timeout`. GLOBAL leader queries node 2: "can you reach node 3?" → No. Quorum (2/3) confirms unreachable. Assert: GLOBAL marks `Dead`, checker generates `RepairCritical` removal, DR converges.

**Scenario 4b — Asymmetric partition (quorum saves)**:
Boot 3-node cluster. Partition ONLY between GLOBAL leader (node 1) and node 3 — nodes 2 and 3 can still communicate. Advance clock past `dead_node_timeout`. GLOBAL leader queries node 2: "can you reach node 3?" → Yes. Quorum (2) not reached — only the leader (1 vote) considers node 3 unreachable. Assert: node 3 is NOT marked Dead. Assert: DR membership unchanged. Heal partition. Assert: cluster stable with all 3 voters.

**Scenario 5 — Epoch fencing rejects stale decisions** _(requires Phase C)_:
Boot 3-node cluster. Partition GLOBAL leader. Other nodes change DR membership. Heal partition. Assert: old leader's stale operators rejected via epoch mismatch.

**Scenario 6 — Concurrent join + decommission**:
Boot 3-node cluster. Same tick: decommission node 1 AND add node 4. Assert: checker removes dead/decommissioning first (`RepairNormal`), scheduler adds learner after (`Add`). Final membership = {2, 3, 4}.

**Scenario 7 — Learner stall timeout** _(requires Phase C)_:
Boot 3-node cluster. Add node 4 as learner, isolate it. Advance clock past `learner_stall_timeout`. Assert: checker generates `RepairLow` removal of stalled learner.

**Scenario 8 — GLOBAL leader crash during decommission**:
Boot 4-node cluster. Decommission node 1. Crash GLOBAL leader before drain completes. Assert: new leader reads `Decommissioning` from GLOBAL state, checker continues the removal.

### D3. Buggification — code-level fault injection

FoundationDB's most powerful testing mechanism is BUGGIFY — instrumentation points scattered throughout production code that fire probabilistically during simulation. Network-level faults (partitions, crashes) can't catch bugs in application logic. Buggification can.

**Implementation**: A `buggify` module in the `consensus` crate:

```rust
/// Global switch: off in production, on during simulation.
static BUGGIFY_ENABLED: AtomicBool = AtomicBool::new(false);

/// Per-simulation RNG seed for deterministic fault injection.
thread_local! {
    static BUGGIFY_RNG: RefCell<Option<StdRng>> = RefCell::new(None);
}

/// Returns true with the given probability, but ONLY during simulation.
/// In production, always returns false (the branch is never taken).
/// Compiles to a single atomic load + branch in the non-simulation path.
#[inline]
pub fn buggify(probability: f64) -> bool {
    if !BUGGIFY_ENABLED.load(Ordering::Relaxed) {
        return false;
    }
    BUGGIFY_RNG.with(|rng| {
        rng.borrow_mut().as_mut()
            .map_or(false, |r| r.gen_bool(probability))
    })
}

/// Activate buggification with a deterministic seed.
pub fn enable_buggify(seed: u64) {
    BUGGIFY_ENABLED.store(true, Ordering::Relaxed);
    BUGGIFY_RNG.with(|rng| {
        *rng.borrow_mut() = Some(StdRng::seed_from_u64(seed));
    });
}
```

**Instrumentation points in production code** (examples):

```rust
// In shard.rs — send_append_entries_to_peer:
if buggify(0.01) {
    // 1% chance: skip sending entries to this peer (simulates message loss)
    return vec![];
}

// In raft_impl.rs — apply_committed_entries:
if buggify(0.005) {
    // 0.5% chance: delay apply by returning early (simulates slow apply)
    return Ok(vec![]);
}

// In consensus_handle.rs — propose_and_wait:
if buggify(0.01) {
    // 1% chance: timeout despite the entry committing (simulates channel drop)
    return Err(HandleError::Timeout { timeout });
}

// In snapshot transfer:
if buggify(0.02) {
    // 2% chance: corrupt snapshot data (simulates disk error)
    snapshot_bytes[0] ^= 0xFF;
}

// In WAL flush:
if buggify(0.005) {
    // 0.5% chance: WAL write returns error (simulates disk full)
    return Err(WalError::DiskFull);
}
```

Each buggify point is:

- **Compiled into production code** but the `BUGGIFY_ENABLED` atomic is `false`, so the fast path is a single relaxed load (< 1ns).
- **Deterministic under simulation** because the RNG is seeded. A failing test can be reproduced by replaying the same seed.
- **Probability-tuned** to balance coverage (fire often enough to catch bugs) against noise (don't fire so often that the system can't make progress).

### D4. Randomized soak testing

The targeted scenarios (D2) test specific known-dangerous interactions. The soak test discovers unknown-dangerous interactions through randomized exploration — the FoundationDB philosophy that "you cannot enumerate all failure modes."

**The soak scenario**:

```rust
#[test]
fn soak_multi_raft_coordination() {
    for seed in 0..SOAK_ITERATIONS {  // default: 100, nightly: 10_000
        let mut sim = MultiRaftSimulation::new_with_seed(seed);
        enable_buggify(seed);

        // Random cluster size: 3-7 nodes.
        let cluster_size = sim.rng.gen_range(3..=7);
        sim.boot_cluster(cluster_size);
        sim.create_data_region("dr-1");

        // Run random operations for 10,000 ticks.
        for _ in 0..10_000 {
            let op = sim.rng.gen_range(0..100);
            match op {
                0..5   => sim.decommission_random_active_node(),
                5..8   => sim.join_new_node(),
                8..12  => sim.crash_random_node(),
                12..14 => sim.restart_random_crashed_node(),
                14..18 => sim.partition_random_pair(),
                18..22 => sim.heal_random_partition(),
                22..25 => sim.isolate_random_node(),
                25..30 => sim.advance_clock(Duration::from_secs(60)),
                _      => sim.propose_entry_on_random_dr(),
            }

            sim.tick();
            sim.check_invariants();  // After EVERY tick.
        }

        // Final convergence: heal all, advance clock, tick until stable.
        sim.heal_all();
        for _ in 0..1000 {
            sim.tick();
            sim.advance_clock(Duration::from_secs(1));
        }
        sim.assert_converged();
    }
}
```

**Invariant checker** (called after every tick):

```rust
fn check_invariants(&self) {
    for (region, group) in &self.data_regions {
        let state = group.shard_state();

        // 1. No committed entries lost.
        // Every entry committed on the leader is present on a majority.
        self.verify_committed_entries_durable(region);

        // 2. At most one pending membership change per shard.
        assert!(!state.has_two_pending_membership_changes());

        // 3. conf_epoch is monotonically increasing.
        assert!(state.conf_epoch >= self.last_seen_epoch[region]);
        self.last_seen_epoch.insert(*region, state.conf_epoch);

        // 4. No voter is both Dead and still accepting proposals.
        for voter in &state.voters {
            if self.node_status(voter) == NodeStatus::Dead {
                assert!(!self.node_is_alive(voter),
                    "Dead node {} is still alive and voting in {}", voter, region);
            }
        }

        // 5. Desired state convergence (if no faults active).
        // If the network is healthy and enough ticks have passed,
        // DR membership should match desired state.
        if self.network_is_fully_healed() && self.ticks_since_last_fault > 100 {
            let desired = desired_dr_voters(&self.global_state, *region);
            let current: BTreeSet<_> = state.voters.iter().copied().collect();
            assert_eq!(current, desired,
                "DR {} not converged after {} healthy ticks", region,
                self.ticks_since_last_fault);
        }
    }
}
```

The key insight from FoundationDB: invariant 5 (convergence after healing) catches the class of bugs we spent days debugging manually. If the system doesn't converge within 100 healthy ticks after all faults are healed, there's a coordination bug — a stuck scheduler, a quorum deadlock, a stale operator, or a missed state transition.

### D5. CI integration

**Location**: `crates/consensus/src/simulation/multi_raft.rs` as `#[cfg(test)]` modules. Runs as part of `cargo test --workspace --lib`.

**Standard CI** (`just test`):

- All 8 targeted scenarios (D2)
- Soak test with 100 iterations, buggification enabled

**Nightly CI** (`just test-proptest`):

- Soak test with 10,000 iterations
- Varying cluster sizes (3, 5, 7 nodes)
- All targeted scenarios with randomized leader assignment, partition timing, crash timing

The existing single-shard simulation tests (`tests/simulation_linearizability.rs`) are unchanged.

---

## Code Removal

Phase B removes the following from the codebase:

| File                                            | What changes                                                                                                                                                       |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `crates/raft/src/types.rs`                      | `SystemRequest::RemoveNodeFromDataRegions` variant deleted. `LedgerRequest::Barrier` variant deleted.                                                              |
| `crates/raft/src/log_storage/operations/mod.rs` | `RemoveNodeFromDataRegions` and `Barrier` apply handlers deleted. Signal channel send deleted.                                                                     |
| `crates/server/src/bootstrap.rs`                | `remove_node_from_all_data_regions`, `remove_node_with_barrier`, `pending_dr_removals` set, signal channel sentinel pattern, old reconciliation task — all deleted |
| `crates/services/src/services/admin.rs`         | `leave_cluster` signal/poll logic deleted                                                                                                                          |

This is a new product — no existing clusters to migrate. Variants are deleted outright, not tombstoned.

**Barrier mechanism going forward**: The scheduler uses empty-data `propose_and_wait` for post-removal barriers — identical to the Raft §5.4.2 no-op. The apply worker's existing empty-data handler (Phase A fix) returns `LedgerResponse::Empty`.

## Configuration

New configuration fields:

```rust
pub struct SchedulerConfig {
    /// DR checker tick interval (reactive repair). Default: 1 second.
    pub checker_interval: Duration,

    /// DR scheduler tick interval (proactive growth). Default: 5 seconds.
    pub scheduler_interval: Duration,

    /// Time before a learner that hasn't caught up is removed. Default: 5 minutes.
    pub learner_stall_timeout: Duration,

    /// Dead node detection configuration. Embedded, not duplicated —
    /// LivenessConfig is the single authoritative definition (see B5).
    pub liveness: LivenessConfig,

    /// How often the drain monitor checks decommissioning progress. Default: 5 seconds.
    pub drain_monitor_interval: Duration,

    /// Minimum voters per data region. Scheduler refuses to remove a voter
    /// (via decommission) that would drop below this count. Dead nodes are
    /// exempt — they're already unavailable. Default: 1.
    pub min_voters_per_region: usize,
}
```

All fields have sensible defaults and are optional in the config file.

## Breaking Changes

This is a pre-release product. Each phase may include breaking changes to the Raft log format, gRPC API, and internal types. No backward compatibility with prior versions is required. Data directories from before these changes are not expected to be compatible — clusters are recreated from scratch.

## Design Notes

Implementation guidance from review:

1. **Soak test invariant 5** (convergence after healing) only fires in the post-loop convergence phase, not during the main 10,000-tick loop. The 16% per-tick fault rate makes 100 consecutive clean ticks statistically impossible during the main loop. This is by design — the post-loop phase is where convergence is verified.

2. **Buggification is simulation-only.** The `buggify()` module uses `thread_local!` RNG, which is compatible with `#[tokio::test(flavor = "current_thread")]` but not with multi-threaded tokio runtimes. Instrumentation points in the apply worker (`raft_impl.rs`) fire during deterministic simulation (which drives `Shard` directly, no tokio runtime) but not during integration tests. This is acceptable — the simulation is the primary correctness mechanism.

3. **`ShardState` additions are additive.** The spec shows a subset of `ShardState` with new fields (`conf_epoch`, `pending_membership`). The existing fields (`shard`, `term`, `state`, `commit_index`, `last_log_index`) are unchanged. The new fields are added to the existing struct, not a replacement.

4. **Nightly soak test runtime.** 10,000 iterations × 10,000 ticks ≈ 3–8 minutes for a 5–7 node cluster. Acceptable for nightly CI under `just test-proptest`.

5. **WAL write failure via buggify.** When a buggify point causes `WalError::DiskFull`, the reactor marks the affected shard as `Failed` using the existing panic isolation mechanism (consensus engine spec §2). The shard can be recovered via snapshot reinstall. This tests the failure-recovery path, which is the point of buggification.

6. **`SystemStateReader` is new infrastructure.** The spec's `RaftManager::system_state_reader()` wraps the GLOBAL `StateLayer` in a read-only accessor. The `StateLayer` uses MVCC reads that don't require locks, so this is a visibility concern (preventing accidental writes), not a concurrency concern. The accessor exposes: `node_status(node_id)`, `voters()`, `region_memberships()`, `is_eligible(node_id, region)`.

## Dependencies

- Phase A: No dependencies. Can be implemented immediately.
- Phase B: Depends on Phase A (A5 shard shutdown is used by the drain monitor).
- Phase C: Depends on Phase B (the scheduler is the integration point for learner-first and epoch fencing).
- Phase D: Targeted scenarios 1–4, 6, 8 and the soak test depend on Phase B. Scenarios 5 and 7 depend on Phase C (they test epoch fencing and learner stall timeout). Buggification infrastructure (D3) can be implemented in parallel with Phase B — the instrumentation points are additive. The soak test (D4) exercises the checker/scheduler from B3, so it requires Phase B. Phase C scenarios are added incrementally as C lands.
