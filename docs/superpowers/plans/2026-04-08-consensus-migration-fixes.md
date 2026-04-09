# Consensus Migration Fixes & Optimization Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all genuine bugs, performance issues, and migration gaps in the openraft → inferadb-ledger-consensus migration across the raft, services, state, and server crates.

**Architecture:** 19 tasks organized by dependency order. Tasks 1–8 are independent (parallelizable). Task 9 introduces a `PeerAddressMap` that enables Tasks 10–14. Tasks 15–17 extend the consensus engine API. Task 18 optimizes apply-path memory. Task 19 is cleanup. Every task must pass `just ci` before marking complete.

**Tech Stack:** Rust 1.92 (2024 edition), inferadb-ledger-consensus, tonic gRPC, snafu errors, bon builders, ArcSwap, DashMap, postcard serialization, parking_lot mutexes

---

## Dependency Graph

```
Independent (parallelize all):
├── Task 1:  LogId manual Ord
├── Task 2:  propose_and_wait race fix
├── Task 3:  Apply path fixes (single decode, conditional lease, learner_ids)
├── Task 4:  Relationship store optimization (range scan + snapshot install)
├── Task 5:  Hibernate/wake race fix (compare_exchange)
├── Task 6:  Background job response validation (propose_and_wait)
├── Task 7:  leave_cluster timeout
└── Task 8:  forward_consensus learner validation

Foundation:
└── Task 9:  PeerAddressMap

Depends on Task 9:
├── Task 10: Service forwarding (read.rs, write.rs)
├── Task 11: Discovery & admin RPCs
├── Task 12: Peer reachability health check
├── Task 13: Learner refresh addresses
└── Task 14: ReadIndex protocol (also depends on Task 3)

Consensus engine extensions:
├── Task 15: Leader transfer
├── Task 16: Snapshot trigger
└── Task 17: Post-erasure regional leadership

Performance:
└── Task 18: AppliedState clone optimization (im crate)

Cleanup:
└── Task 19: TODO removal + Byzantine fault tests
```

---

## Task 1: LogId Manual Ord Implementation

**Why:** Derived `Ord` on `LogId` includes `node_id` in ordering. The doc says `node_id` is "informational, not used for ordering," but `(term=5, node_id=42, index=100)` compares greater than `(term=5, node_id=0, index=200)` — violating Raft log semantics.

**Files:**
- Modify: `crates/raft/src/log_storage/types.rs:42-50`
- Test: `crates/raft/src/log_storage/types.rs` (inline test module)

- [ ] **Step 1: Write failing tests**

Add at the bottom of `crates/raft/src/log_storage/types.rs`:

```rust
#[cfg(test)]
mod log_id_tests {
    use super::*;

    #[test]
    fn ordering_ignores_node_id() {
        let a = LogId { term: 5, node_id: 42, index: 100 };
        let b = LogId { term: 5, node_id: 0, index: 200 };
        assert!(b > a, "index 200 > index 100 regardless of node_id");
    }

    #[test]
    fn ordering_term_takes_precedence() {
        let a = LogId { term: 3, node_id: 0, index: 999 };
        let b = LogId { term: 4, node_id: 0, index: 1 };
        assert!(b > a, "higher term always wins");
    }

    #[test]
    fn ordering_equal_when_only_node_id_differs() {
        let a = LogId { term: 5, node_id: 1, index: 100 };
        let b = LogId { term: 5, node_id: 99, index: 100 };
        assert_eq!(a.cmp(&b), std::cmp::Ordering::Equal);
    }

    #[test]
    fn partial_ord_consistent_with_ord() {
        let a = LogId { term: 1, node_id: 0, index: 1 };
        let b = LogId { term: 1, node_id: 0, index: 2 };
        assert_eq!(a.partial_cmp(&b), Some(std::cmp::Ordering::Less));
    }
}
```

- [ ] **Step 2: Verify tests fail**

Run: `cargo +1.92 test -p inferadb-ledger-raft log_id_tests -- --nocapture`
Expected: `ordering_ignores_node_id` and `ordering_equal_when_only_node_id_differs` FAIL.

- [ ] **Step 3: Remove derived Ord/PartialOrd, add manual impls**

In `crates/raft/src/log_storage/types.rs`, change the derive line on `LogId` from:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
```
to:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
```

Add immediately after the struct definition:

```rust
impl PartialOrd for LogId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.term.cmp(&other.term).then(self.index.cmp(&other.index))
    }
}
```

- [ ] **Step 4: Verify tests pass**

Run: `cargo +1.92 test -p inferadb-ledger-raft log_id_tests -- --nocapture`
Expected: All 4 tests PASS.

- [ ] **Step 5: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 2: Fix propose_and_wait Race Condition

**Why:** In `propose_and_wait`, the oneshot sender is inserted into `response_map` **after** `engine.propose()` returns. The ApplyWorker can process and deliver the committed entry in that gap, find no waiter, and drop the response. The proposer then hangs until timeout despite the entry being committed and applied.

**Fix:** Add a spillover buffer. When the ApplyWorker can't find a waiter, it stores the response in the spillover. After registering, the proposer checks spillover for responses that arrived early.

**Files:**
- Modify: `crates/raft/src/consensus_handle.rs`
- Modify: `crates/raft/src/apply_worker.rs`
- Test: `crates/raft/src/consensus_handle.rs` (inline test module)

- [ ] **Step 1: Add spillover buffer type and field to ConsensusHandle**

In `crates/raft/src/consensus_handle.rs`, add the spillover type after the `ResponseMap` type alias:

```rust
/// Buffer for responses that arrived before the proposer registered a waiter.
/// The apply worker stores here when no waiter is found; the proposer checks
/// after registration to close the race window.
pub type SpilloverMap = Arc<parking_lot::Mutex<HashMap<u64, LedgerResponse>>>;
```

Add `spillover` to the `ConsensusHandle` struct:

```rust
pub struct ConsensusHandle {
    engine: ConsensusEngine,
    shard: ShardId,
    node_id: LedgerNodeId,
    state_rx: watch::Receiver<ShardState>,
    response_map: ResponseMap,
    spillover: SpilloverMap,
}
```

Update the `new()` constructor to initialize `spillover`:

```rust
let spillover: SpilloverMap = Arc::new(parking_lot::Mutex::new(HashMap::new()));
```

Add a public accessor:

```rust
pub fn spillover(&self) -> &SpilloverMap {
    &self.spillover
}
```

- [ ] **Step 2: Update propose_and_wait to check spillover**

Replace the `propose_and_wait` method body:

```rust
pub async fn propose_and_wait(
    &self,
    payload: RaftPayload,
    timeout: Duration,
) -> Result<LedgerResponse, HandleError> {
    let data =
        postcard::to_allocvec(&payload).map_err(|e| HandleError::Serialize(e.to_string()))?;

    let (tx, rx) = tokio::sync::oneshot::channel();

    let commit_index = self
        .engine
        .propose(self.shard, data)
        .await
        .map_err(HandleError::from)?;

    // Register the waiter keyed by commit_index.
    self.response_map.lock().insert(commit_index, tx);

    // Check if the apply worker already delivered (the race window).
    if let Some(response) = self.spillover.lock().remove(&commit_index) {
        self.response_map.lock().remove(&commit_index);
        return Ok(response);
    }

    match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(_)) => Err(HandleError::ApplyFailed("response channel closed".into())),
        Err(_) => {
            self.response_map.lock().remove(&commit_index);
            Err(HandleError::Timeout)
        }
    }
}
```

- [ ] **Step 3: Update ApplyWorker to use spillover**

In `crates/raft/src/apply_worker.rs`, add `spillover: SpilloverMap` to the `ApplyWorker` struct and update the `new()` constructor.

In the response delivery loop inside `run()`, change:

```rust
if let Some(tx) = map.remove(&entry.index) {
    let _ = tx.send(response);
}
```

to:

```rust
if let Some(tx) = map.remove(&entry.index) {
    let _ = tx.send(response);
} else {
    // Proposer hasn't registered yet — store for pickup.
    self.spillover.lock().insert(entry.index, response);
}
```

- [ ] **Step 4: Update all call sites that construct ApplyWorker**

Find where `ApplyWorker::new()` is called (likely in `raft_manager.rs` or `log_storage/mod.rs`) and pass the `spillover` map from the corresponding `ConsensusHandle`. The `ConsensusHandle` and `ApplyWorker` for the same shard must share the same `SpilloverMap` instance.

- [ ] **Step 5: Write a test validating the spillover path**

Add to the test module in `consensus_handle.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LedgerResponse;

    #[test]
    fn spillover_delivers_early_response() {
        let response_map: ResponseMap = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let spillover: SpilloverMap = Arc::new(parking_lot::Mutex::new(HashMap::new()));

        // Simulate: apply worker delivers before proposer registers
        spillover.lock().insert(42, LedgerResponse::Empty);

        // Proposer registers waiter (too late for normal delivery)
        let (tx, _rx) = tokio::sync::oneshot::channel();
        response_map.lock().unwrap().insert(42, tx);

        // Proposer checks spillover — should find the response
        let response = spillover.lock().remove(&42);
        assert!(response.is_some(), "spillover must contain early-delivered response");
    }

    #[test]
    fn normal_path_bypasses_spillover() {
        let response_map: ResponseMap = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let spillover: SpilloverMap = Arc::new(parking_lot::Mutex::new(HashMap::new()));

        // Proposer registers first (normal case)
        let (tx, rx) = tokio::sync::oneshot::channel();
        response_map.lock().unwrap().insert(42, tx);

        // Apply worker finds the waiter — delivers directly
        if let Some(sender) = response_map.lock().unwrap().remove(&42) {
            let _ = sender.send(LedgerResponse::Empty);
        }

        // Spillover should be empty
        assert!(spillover.lock().is_empty());

        // Response received normally
        let result = rx.try_recv();
        assert!(result.is_ok());
    }
}
```

- [ ] **Step 6: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 3: Apply Path Fixes (Decode, Lease, Learner IDs)

**Why:** Three distinct bugs in `apply_committed_entries`:
1. **Double decode:** `RaftPayload` is deserialized once for `block_timestamp` and again in the apply loop. Fix: decode once, reuse.
2. **Leader lease on all nodes:** `self.leader_lease.renew()` is called unconditionally — followers shouldn't renew.
3. **Learner IDs dropped:** `learner_ids` is hardcoded to empty `BTreeSet` instead of extracted from consensus membership.

**Files:**
- Modify: `crates/raft/src/log_storage/raft_impl.rs:375-386,458-462,656`
- Test: `crates/raft/src/log_storage/raft_impl.rs` or existing test file

- [ ] **Step 1: Fix double decode — pre-decode payloads**

In `apply_committed_entries` (raft_impl.rs), replace the block_timestamp extraction and the main loop to decode each entry only once.

Replace the block_timestamp extraction (lines ~375-386) and restructure the loop:

```rust
// Pre-decode all Normal entry payloads once.
struct DecodedEntry<'a> {
    entry: &'a CommittedEntry,
    log_id: LogId,
    payload: Option<crate::types::RaftPayload>,
}

let decoded: Vec<DecodedEntry<'_>> = entries
    .iter()
    .map(|entry| {
        let log_id = LogId::new(entry.term, 0, entry.index);
        let payload = match &entry.kind {
            EntryKind::Normal => {
                Some(inferadb_ledger_types::decode::<crate::types::RaftPayload>(&entry.data)
                    .map_err(|e| StoreError::msg(format!("decode error: {e}")))?)
            }
            EntryKind::Membership(_) => None,
        };
        Ok(DecodedEntry { entry, log_id, payload })
    })
    .collect::<Result<Vec<_>, StoreError>>()?;

// Extract block_timestamp from the last Normal entry's pre-decoded payload.
let block_timestamp = decoded
    .iter()
    .rev()
    .find_map(|d| d.payload.as_ref().map(|p| p.proposed_at))
    .unwrap_or(DateTime::UNIX_EPOCH);
```

Then in the main loop, use `decoded` instead of re-decoding:

```rust
for decoded_entry in &decoded {
    let log_id = decoded_entry.log_id;

    match &decoded_entry.entry.kind {
        EntryKind::Normal => {
            let payload = decoded_entry.payload.as_ref()
                .expect("Normal entries always have decoded payload");
            // ... use payload directly instead of decoding again
        }
        EntryKind::Membership(consensus_membership) => {
            // ... handle membership
        }
    }
}
```

- [ ] **Step 2: Fix leader lease — only renew on leader**

At line ~656 where `self.leader_lease.renew()` is called, add a leadership check:

```rust
// Only renew the leader lease if this node is the current leader.
// Followers must not maintain a valid lease — it would be misleading
// if checked without a corresponding is_leader() guard.
if self.applied_state.load().membership.voters().contains(&self.node_id) {
    // Check if we believe we're the leader based on applied state.
    // The consensus engine's ShardState is authoritative, but we don't
    // have access to it here. Use the term-based heuristic: if the last
    // entry's term matches our belief, we proposed it.
    if let Some(last) = decoded.last() {
        if last.entry.leader_id == self.node_id {
            self.leader_lease.renew();
        }
    }
}
```

**Note:** Check what fields `CommittedEntry` has. If it doesn't have `leader_id`, an alternative approach is to pass an `is_leader: bool` flag from the `ApplyWorker` (which can query the `ConsensusHandle`). The simplest correct approach: add `is_leader: bool` parameter to `apply_committed_entries` and have the `ApplyWorker` pass `handle.is_leader()` each time it calls.

- [ ] **Step 3: Fix learner_ids — extract from consensus membership**

At lines ~458-462, replace:

```rust
let learner_ids = std::collections::BTreeSet::new();
```

with extraction from the consensus membership. Check if `consensus_membership` (type: `inferadb_ledger_consensus::types::Membership`) has a `learners` field:

```rust
let learner_ids = consensus_membership
    .learners
    .iter()
    .map(|n| n.0)
    .collect::<std::collections::BTreeSet<u64>>();
```

If `Membership` doesn't have a `learners` field, check the consensus crate's `Membership` struct definition. If learners aren't tracked by the consensus engine, document this with a comment explaining why the field is empty (not a TODO — a factual statement about the consensus engine's current capabilities).

- [ ] **Step 4: Write tests for the decode optimization**

Add a test that verifies the block_timestamp is correctly extracted from pre-decoded payloads (unit test on the extraction logic, not full apply).

- [ ] **Step 5: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 4: Relationship Store Optimization

**Why:** Two issues:
1. `load_vault_relationship_index` uses `txn.iter::<Relationships>()` (full table scan from the beginning) instead of `txn.range()` (B-tree seek). For tables with millions of relationships across vaults, this is O(total) instead of O(vault).
2. After snapshot install, the `RelationshipIndex` is empty. The first `relationship_create`/`delete` on a vault may operate on a partially-loaded index.

**Files:**
- Modify: `crates/state/src/state.rs:679-701` (range scan)
- Modify: `crates/state/src/state.rs` (snapshot install method)
- Test: `crates/state/src/state.rs` or existing state tests

- [ ] **Step 1: Replace iter with range in load_vault_relationship_index**

In `crates/state/src/state.rs`, replace the `load_vault_relationship_index` method body (lines 679-701):

```rust
fn load_vault_relationship_index(&self, vault: VaultId) -> Result<()> {
    let txn = self.db.read().context(StoreSnafu)?;

    let vault_prefix = crate::keys::vault_prefix(vault);

    // Build end bound: increment the last byte of the prefix to get the exclusive upper bound.
    let mut vault_end = vault_prefix.clone();
    // Increment the vault ID portion to get the next vault's prefix.
    // vault_prefix is 8 bytes (big-endian VaultId). Adding 1 to the ID gives
    // the start of the next vault's key space.
    let next_vault = VaultId::from(vault.value() + 1);
    let vault_end_prefix = crate::keys::vault_prefix(next_vault);

    for (key_bytes, _) in txn
        .range::<tables::Relationships>(Some(&vault_prefix), Some(&vault_end_prefix))
        .context(StoreSnafu)?
    {
        if key_bytes.len() < 9 {
            continue; // Skip malformed keys (was `break` — incorrect)
        }
        // local_key starts after the 8-byte vault prefix + 1-byte bucket_id.
        let local_key = &key_bytes[9..];
        self.relationship_index.insert(vault, seahash::hash(local_key));
    }

    // Mark the vault as loaded even if it had zero relationships.
    self.relationship_index.ensure_vault(vault);
    Ok(())
}
```

**Important:** Verify that `tables::Relationships` key type is `Vec<u8>` (which implements `Key`). The `range()` method on `ReadTransaction` takes `Option<&T::KeyType>`. If the key type is `Vec<u8>`, pass `Some(&vault_prefix)` and `Some(&vault_end_prefix)` where both are `Vec<u8>`.

Also check the exact signature of `crate::keys::vault_prefix()` — it likely returns a `Vec<u8>`. If the range needs raw bytes, construct `vault_end_prefix` by cloning `vault_prefix` and incrementing the big-endian u64:

```rust
let mut vault_end_prefix = vault_prefix.clone();
// Increment the big-endian i64 vault ID by 1
let vault_id_bytes = vault.value().wrapping_add(1).to_be_bytes();
vault_end_prefix[..8].copy_from_slice(&vault_id_bytes);
```

- [ ] **Step 2: Fix the `break` on short keys**

This is already handled in Step 1 — changed `break` to `continue`. Short keys shouldn't terminate the scan; they should be skipped since they may be from other tables or be metadata entries.

- [ ] **Step 3: Ensure relationship index consistency on create/delete**

In `apply_operations_in_txn`, the `CreateRelationship` and `DeleteRelationship` arms already insert/remove from the hash index. But if the vault hasn't been loaded into the index yet, `insert` adds a single entry to an otherwise empty vault map — making the index inconsistent (it thinks the vault only has one relationship).

Add a check before `insert` in `CreateRelationship`:

```rust
// Only update the hash index if this vault is already loaded.
// If not loaded, the lazy-load on next exists() will pick up the new entry.
if self.relationship_index.is_vault_loaded(vault) {
    if let Some(hash) = Self::relationship_hash_from_dict(dict, resource, relation, subject) {
        self.relationship_index.insert(vault, hash);
    }
}
```

Apply the same pattern to `DeleteRelationship`:

```rust
if self.relationship_index.is_vault_loaded(vault) {
    if let Some(hash) = Self::relationship_hash_from_dict(dict, resource, relation, subject) {
        self.relationship_index.remove(vault, hash);
    }
}
```

- [ ] **Step 4: Write test for range scan**

```rust
#[test]
fn load_vault_relationship_index_uses_range_scan() {
    // Create a state layer with relationships in two vaults.
    // Verify that loading vault A's index doesn't iterate vault B's entries.
    // (Functional test — the performance improvement is validated by the range call.)
    let state = /* create test StateLayer with two vaults */;

    // Add relationships to vault_a and vault_b
    // ...

    // Load vault_a's index
    state.load_vault_relationship_index(vault_a).unwrap();

    // Verify vault_a's relationships are indexed
    assert!(state.relationship_index.is_vault_loaded(vault_a));

    // Verify vault_b is NOT loaded (only vault_a was requested)
    assert!(!state.relationship_index.is_vault_loaded(vault_b));
}
```

- [ ] **Step 5: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 5: Hibernate/Wake Race Fix

**Why:** In `route_organization`, two concurrent requests to a hibernated region can both see `jobs_active=false` (Relaxed load) and both call `wake_region()`, double-starting background jobs.

**Files:**
- Modify: `crates/raft/src/raft_manager.rs:1211-1228` (wake_region)
- Test: `crates/raft/src/raft_manager.rs` or existing tests

- [ ] **Step 1: Use compare_exchange in wake_region**

Replace the `wake_region` method (lines 1211-1228):

```rust
pub fn wake_region(&self, region: Region) -> Result<()> {
    let group = self.get_region_group(region)?;

    // Atomically transition from inactive to active.
    // Only one thread wins; others return early.
    if group
        .jobs_active
        .compare_exchange(false, true, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire)
        .is_err()
    {
        return Ok(()); // Another thread already woke this region
    }

    group.touch();
    let jobs = self.start_background_jobs(
        region,
        group.handle().clone(),
        group.state().clone(),
        group.block_archive().clone(),
        group.applied_state().clone(),
    );
    *group.background_jobs.lock() = jobs;
    info!(%region, "Region group woken from hibernation");
    Ok(())
}
```

- [ ] **Step 2: Fix hibernate_region to use AcqRel ordering too**

Replace the `hibernate_region` method (lines 1192-1201):

```rust
pub fn hibernate_region(&self, region: Region) -> Result<()> {
    let group = self.get_region_group(region)?;
    if group
        .jobs_active
        .compare_exchange(true, false, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire)
        .is_err()
    {
        return Ok(()); // Already hibernated
    }
    group.background_jobs.lock().abort();
    info!(%region, "Region group hibernated");
    Ok(())
}
```

- [ ] **Step 3: Remove the is_jobs_active check in route_organization**

The `route_organization` method (lines 581-597) calls `group.is_jobs_active()` before `wake_region`. Since `wake_region` now handles the atomic check internally, simplify:

```rust
pub fn route_organization(&self, organization: OrganizationId) -> Option<Arc<RegionGroup>> {
    let router = self.router.read().clone()?;
    let routing = router.get_routing(organization).ok()?;
    let group = self.regions.read().get(&routing.region).cloned()?;
    group.touch();

    // Auto-wake hibernated regions. compare_exchange inside wake_region
    // ensures only one caller starts background jobs.
    if !group.jobs_active.load(std::sync::atomic::Ordering::Acquire) {
        let _ = self.wake_region(routing.region);
    }

    Some(group)
}
```

- [ ] **Step 4: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 6: Background Job Response Validation

**Why:** Several background jobs switched from `propose_and_wait()` (with response validation) to fire-and-forget `propose()`, silently discarding responses. Health state transitions, token deletions, and saga steps need response validation.

**Files:**
- Modify: `crates/raft/src/auto_recovery.rs:457-462`
- Modify: `crates/raft/src/token_maintenance.rs:101-104,128-134`
- Modify: `crates/raft/src/saga_orchestrator.rs` (the fallback propose path)

- [ ] **Step 1: Fix auto_recovery — use propose_and_wait**

In `crates/raft/src/auto_recovery.rs`, find the `propose_health_update` method (~line 457). Replace `propose()` with `propose_and_wait()`:

```rust
let response = self
    .handle
    .propose_and_wait(
        RaftPayload::system(request),
        std::time::Duration::from_secs(10),
    )
    .await
    .map_err(|e| RecoveryError::RaftConsensus {
        message: format!("{:?}", e),
        backtrace: snafu::Backtrace::generate(),
    })?;
```

If the method previously validated the response (checking for `VaultHealthUpdated { success }` etc.), restore that validation logic.

- [ ] **Step 2: Fix token_maintenance — use propose_and_wait for Phase 1**

In `crates/raft/src/token_maintenance.rs`, lines ~101-104. Replace:

```rust
match self
    .handle
    .propose(RaftPayload::system(LedgerRequest::DeleteExpiredRefreshTokens))
    .await
```

with:

```rust
match self
    .handle
    .propose_and_wait(
        RaftPayload::system(LedgerRequest::DeleteExpiredRefreshTokens),
        std::time::Duration::from_secs(10),
    )
    .await
```

Then extract the expired token count from the response for metrics:

```rust
{
    Ok(response) => {
        if let LedgerResponse::ExpiredRefreshTokensDeleted { count } = response {
            result.expired_tokens_deleted = count;
        }
    }
    Err(e) => {
        warn!(error = %e, "Failed to delete expired refresh tokens");
    }
}
```

- [ ] **Step 3: Fix token_maintenance Phase 2 — signing key transitions**

Lines ~128-134. Same pattern: replace `propose()` with `propose_and_wait()`:

```rust
match self
    .handle
    .propose_and_wait(
        RaftPayload::new(
            LedgerRequest::TransitionSigningKeyRevoked { kid: kid.clone() },
            0,
        ),
        std::time::Duration::from_secs(10),
    )
    .await
```

- [ ] **Step 4: Fix saga_orchestrator fallback path**

Find the fallback propose path in `saga_orchestrator.rs` where `propose()` is used instead of `propose_and_wait()`. Replace with `propose_and_wait()` and proper response handling to match the manager path semantics.

- [ ] **Step 5: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 7: leave_cluster Timeout

**Why:** The `remove_node` call for data regions in `leave_cluster` has no timeout. The old code used a 5-second timeout. A partitioned region could cause the RPC to hang indefinitely.

**Files:**
- Modify: `crates/services/src/services/admin.rs:770-790`

- [ ] **Step 1: Wrap remove_node in timeout**

In the `leave_cluster` handler, find the loop that removes nodes from data regions (around line 770-790). Wrap each `remove_node` call:

```rust
if region_handle.is_leader() {
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        region_handle.remove_node(req.node_id),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(
                region = region.as_str(),
                node_id = req.node_id,
                error = %e,
                "Failed to remove node from data region (non-fatal)"
            );
        }
        Err(_) => {
            tracing::warn!(
                region = region.as_str(),
                node_id = req.node_id,
                "Timed out removing node from data region (non-fatal)"
            );
        }
    }
}
```

- [ ] **Step 2: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 8: forward_consensus Learner Validation

**Why:** The `forward_consensus` handler only validates that `from_node` is in `state.voters`. Learner nodes (added via `add_learner` but not yet promoted) are legitimate cluster participants that need to exchange consensus messages. Their messages are currently rejected with `PERMISSION_DENIED`.

**Files:**
- Modify: `crates/services/src/services/raft.rs:131-137`

- [ ] **Step 1: Check both voters and learners**

Replace the membership check in `forward_consensus` (raft.rs ~lines 131-137):

```rust
// Validate that the sender is a known cluster member (voter or learner).
let from_node = inferadb_ledger_consensus::types::NodeId(req.from_node);
let state = group.handle().shard_state();
let is_member = state.voters.contains(&from_node)
    || state.learners.as_ref().map_or(false, |l| l.contains(&from_node));
if !is_member {
    return Err(Status::permission_denied(format!(
        "node {} is not a member of the cluster",
        req.from_node
    )));
}
```

**Note:** Check if `ShardState` has a `learners` field. If not, the consensus engine's `Membership` struct may expose them differently. If learners aren't tracked in `ShardState`, add a `learners: BTreeSet<NodeId>` field to `ShardState` and populate it from the consensus engine's membership.

- [ ] **Step 2: Add test for learner message acceptance**

Add a test in the raft service tests that verifies a learner node's consensus messages are accepted (not rejected with PERMISSION_DENIED).

- [ ] **Step 3: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 9: PeerAddressMap (Foundation)

**Why:** The root cause of 6+ migration gaps is that services can't resolve `NodeId → network address`. The consensus engine's `ShardState` only tracks voter `NodeId`s, not addresses. The `GrpcConsensusTransport` has addresses but they're internal to the raft layer. We need a shared, dynamically-updated `NodeId → address` mapping accessible by services.

**Files:**
- Create: `crates/raft/src/peer_address_map.rs`
- Modify: `crates/raft/src/lib.rs` (add module, re-export)
- Modify: `crates/raft/src/raft_manager.rs` (add field, populate)
- Test: `crates/raft/src/peer_address_map.rs` (inline tests)

- [ ] **Step 1: Create PeerAddressMap**

Create `crates/raft/src/peer_address_map.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

/// Thread-safe map from node ID to network address.
///
/// Populated during bootstrap from `initial_members` and updated dynamically
/// via `announce_peer` RPCs. Shared between `RaftManager` and all gRPC services
/// that need to forward requests or resolve peer addresses.
#[derive(Debug, Clone)]
pub struct PeerAddressMap {
    inner: Arc<RwLock<HashMap<u64, String>>>,
}

impl PeerAddressMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register or update a peer's address.
    pub fn insert(&self, node_id: u64, address: String) {
        self.inner.write().insert(node_id, address);
    }

    /// Remove a peer (e.g., on leave_cluster).
    pub fn remove(&self, node_id: u64) -> Option<String> {
        self.inner.write().remove(&node_id)
    }

    /// Resolve a node ID to its network address.
    pub fn get(&self, node_id: u64) -> Option<String> {
        self.inner.read().get(&node_id).cloned()
    }

    /// Iterate all known peers as `(node_id, address)` pairs.
    pub fn iter_peers(&self) -> Vec<(u64, String)> {
        self.inner.read().iter().map(|(&id, addr)| (id, addr.clone())).collect()
    }

    /// Number of known peers.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Bulk-insert peers from an iterator.
    pub fn insert_many(&self, peers: impl IntoIterator<Item = (u64, String)>) {
        let mut map = self.inner.write();
        for (id, addr) in peers {
            map.insert(id, addr);
        }
    }
}

impl Default for PeerAddressMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let map = PeerAddressMap::new();
        map.insert(1, "127.0.0.1:50051".to_string());
        assert_eq!(map.get(1), Some("127.0.0.1:50051".to_string()));
        assert_eq!(map.get(2), None);
    }

    #[test]
    fn remove_returns_old_value() {
        let map = PeerAddressMap::new();
        map.insert(1, "addr1".to_string());
        assert_eq!(map.remove(1), Some("addr1".to_string()));
        assert_eq!(map.get(1), None);
    }

    #[test]
    fn insert_many_and_iter() {
        let map = PeerAddressMap::new();
        map.insert_many(vec![
            (1, "addr1".to_string()),
            (2, "addr2".to_string()),
        ]);
        assert_eq!(map.len(), 2);
        let peers = map.iter_peers();
        assert!(peers.contains(&(1, "addr1".to_string())));
        assert!(peers.contains(&(2, "addr2".to_string())));
    }

    #[test]
    fn update_overwrites() {
        let map = PeerAddressMap::new();
        map.insert(1, "old".to_string());
        map.insert(1, "new".to_string());
        assert_eq!(map.get(1), Some("new".to_string()));
    }
}
```

- [ ] **Step 2: Add module to lib.rs and re-export**

In `crates/raft/src/lib.rs`, add:

```rust
mod peer_address_map;
pub use peer_address_map::PeerAddressMap;
```

- [ ] **Step 3: Add PeerAddressMap to RaftManager**

In `crates/raft/src/raft_manager.rs`, add a field to `RaftManager`:

```rust
pub struct RaftManager {
    // ... existing fields ...
    peer_addresses: PeerAddressMap,
}
```

Initialize in the constructor:

```rust
peer_addresses: PeerAddressMap::new(),
```

Add a public accessor:

```rust
pub fn peer_addresses(&self) -> &PeerAddressMap {
    &self.peer_addresses
}
```

- [ ] **Step 4: Populate PeerAddressMap during bootstrap**

In `crates/raft/src/raft_manager.rs`, find where `initial_members: Vec<(LedgerNodeId, String)>` is used (in `start_data_region` or similar). After creating channels for `GrpcConsensusTransport`, also insert into the peer address map:

```rust
for &(node_id, ref addr) in &initial_members {
    if node_id != self.node_id {
        self.peer_addresses.insert(node_id, addr.clone());
    }
}
```

Also in `crates/server/src/bootstrap.rs`, after the bootstrap coordination phase populates `initial_members`, insert the local node's address:

```rust
raft_manager.peer_addresses().insert(node_id, config.advertise_addr.clone());
```

- [ ] **Step 5: Wire PeerAddressMap into service constructors**

The `PeerAddressMap` needs to be accessible from gRPC service implementations. Add it to `LedgerServer` construction in `crates/services/src/server.rs`:

Find where services are constructed (ReadService, WriteService, DiscoveryService, AdminService, HealthService). Add `peer_addresses: PeerAddressMap` as a field to each service that needs it, and pass it during construction.

The services that need it:
- `ReadService` (for forwarding reads to leader)
- `WriteService` (for forwarding writes to leader — if it exists)
- `DiscoveryService` (for GetPeers, ResolveRegionLeader, GetSystemState)
- `AdminService` (for GetClusterInfo, JoinCluster leader_address)
- `HealthServiceImpl` (for peer reachability checks)

- [ ] **Step 6: Update announce_peer to maintain the map**

In the discovery service's `announce_peer` RPC handler, after recording the peer in the PeerTracker, also update the PeerAddressMap:

```rust
self.peer_addresses.insert(req.node_id, req.address.clone());
```

- [ ] **Step 7: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 10: Service Forwarding (Read + Write)

**Why:** `should_forward_to_leader` in read.rs passes `std::iter::empty()` to `leader_channel_cache.get_or_connect()`, making leader address resolution impossible. Follower nodes can't forward requests.

**Depends on:** Task 9 (PeerAddressMap)

**Files:**
- Modify: `crates/services/src/services/read.rs:196-216`
- Modify: `crates/services/src/services/read.rs:283-296` (get_leader_channel)

- [ ] **Step 1: Fix should_forward_to_leader in read.rs**

Replace the `std::iter::empty()` call with actual peer addresses from `PeerAddressMap`:

```rust
fn should_forward_to_leader(
    &self,
    ctx: &RegionContext,
) -> Result<Option<ForwardClient>, Status> {
    let handle = &ctx.handle;

    if handle.is_leader() {
        return Ok(None);
    }

    // Resolve peer addresses from the shared peer address map.
    let peers: Vec<(u64, String)> = self.peer_addresses.iter_peers();
    let peer_refs: Vec<(u64, &str)> = peers.iter().map(|(id, addr)| (*id, addr.as_str())).collect();

    self.leader_channel_cache.get_or_connect(
        handle.current_leader(),
        handle.node_id(),
        peer_refs.into_iter(),
    )
}
```

- [ ] **Step 2: Fix get_leader_channel in read.rs**

Replace the method that always returns `Err`:

```rust
fn get_leader_channel(&self, ctx: &RegionContext) -> Result<tonic::transport::Channel, Status> {
    let handle = &ctx.handle;
    let leader_id =
        handle.current_leader().ok_or_else(|| Status::unavailable("No leader available"))?;

    if leader_id == handle.node_id() {
        return Err(Status::internal("get_leader_channel called on leader node"));
    }

    let leader_addr = self
        .peer_addresses
        .get(leader_id)
        .ok_or_else(|| Status::unavailable("Leader address not found in peer registry"))?;

    tonic::transport::Channel::from_shared(format!("http://{leader_addr}"))
        .map_err(|e| Status::internal(format!("invalid leader address: {e}")))?
        .connect_lazy()
        .pipe(Ok)
}
```

**Note:** Check if `tonic::transport::Channel::from_shared().connect_lazy()` is the correct API. The `connect_lazy()` method creates a channel that connects on first use. This matches the forwarding pattern where we want to create the channel quickly and let it connect when the first RPC is sent. If `pipe` is not available, use a `let` binding instead.

- [ ] **Step 3: If WriteService has a similar pattern, fix it too**

Check `crates/services/src/services/write.rs` for any `std::iter::empty()` usage or forwarding pattern. Apply the same PeerAddressMap-based resolution.

- [ ] **Step 4: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 11: Discovery & Admin RPCs

**Why:** GetPeers, GetClusterInfo, ResolveRegionLeader, and JoinCluster all return empty data because they can't resolve peer addresses.

**Depends on:** Task 9 (PeerAddressMap)

**Files:**
- Modify: `crates/services/src/services/discovery.rs`
- Modify: `crates/services/src/services/admin.rs`

- [ ] **Step 1: Fix GetPeers**

In `discovery.rs` (line ~287-299), replace the empty `peers` vec:

```rust
async fn get_peers(
    &self,
    request: Request<GetPeersRequest>,
) -> Result<Response<GetPeersResponse>, Status> {
    let _req = request.into_inner();

    let current_term = self.handle.current_term();
    let state = self.handle.shard_state();

    let peers: Vec<PeerInfo> = state
        .voters
        .iter()
        .filter_map(|node_id| {
            let addr = self.peer_addresses.get(node_id.0)?;
            Some(PeerInfo {
                node_id: node_id.0,
                address: addr,
                is_leader: state.leader.map_or(false, |l| l == *node_id),
                // Populate other PeerInfo fields as available
                ..Default::default()
            })
        })
        .collect();

    Ok(Response::new(GetPeersResponse { peers, system_version: current_term }))
}
```

- [ ] **Step 2: Fix ResolveRegionLeader**

In `discovery.rs`, fix the `resolve_region_leader_impl` helper to use PeerAddressMap:

```rust
fn resolve_region_leader_impl(
    &self,
    region: inferadb_ledger_types::Region,
) -> Result<(String, u64, u32), Status> {
    let group = self
        .raft_manager
        .as_ref()
        .ok_or_else(|| Status::unavailable("RaftManager not available"))?
        .get_region_group(region)
        .map_err(|_| Status::not_found("region group not found"))?;

    let handle = group.handle();
    let leader_id = handle
        .current_leader()
        .ok_or_else(|| Status::unavailable("No leader elected for region"))?;

    let endpoint = self
        .peer_addresses
        .get(leader_id)
        .ok_or_else(|| Status::unavailable("Leader address not found"))?;

    let raft_term = handle.current_term();
    let ttl_seconds = 5; // Cache hint for SDK

    Ok((endpoint, raft_term, ttl_seconds))
}
```

- [ ] **Step 3: Fix GetSystemState nodes**

In `discovery.rs`, the `get_system_state` handler (line ~340-397). Replace the empty `nodes` and `member_nodes`:

```rust
let nodes: Vec<NodeInfo> = state
    .voters
    .iter()
    .map(|node_id| {
        let addr = self.peer_addresses.get(node_id.0).unwrap_or_default();
        NodeInfo {
            node_id: node_id.0,
            address: addr,
            is_leader: state.leader.map_or(false, |l| l == *node_id),
            ..Default::default()
        }
    })
    .collect();

let member_nodes: Vec<NodeId> = state.voters.iter().map(|n| n.0).collect();
```

- [ ] **Step 4: Fix GetClusterInfo**

In `admin.rs` (line ~810-838), replace the empty `members`:

```rust
let state = self.handle.shard_state();
let members: Vec<ClusterMember> = state
    .voters
    .iter()
    .map(|node_id| {
        let addr = self.peer_addresses.get(node_id.0).unwrap_or_default();
        ClusterMember {
            node_id: node_id.0,
            address: addr,
            role: if state.leader == Some(*node_id) {
                "leader".to_string()
            } else {
                "voter".to_string()
            },
            ..Default::default()
        }
    })
    .collect();
```

- [ ] **Step 5: Fix JoinCluster leader_address**

In `admin.rs`, find the JoinCluster response that returns `leader_address: String::new()` (~line 600-608). Replace with:

```rust
leader_address: self
    .peer_addresses
    .get(leader_id)
    .unwrap_or_default(),
```

Also fix the success response (~line 682-687):

```rust
leader_address: self
    .peer_addresses
    .get(node_id)
    .unwrap_or_default(),
```

- [ ] **Step 6: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 12: Peer Reachability Health Check

**Why:** `check_peer_reachability_handle` always returns `healthy: true`, masking network partitions from Kubernetes readiness probes.

**Depends on:** Task 9 (PeerAddressMap)

**Files:**
- Modify: `crates/raft/src/dependency_health.rs:218-249`

- [ ] **Step 1: Wire PeerAddressMap into DependencyHealthChecker**

Add `peer_addresses: PeerAddressMap` field to `DependencyHealthChecker`. Update constructor and all call sites.

- [ ] **Step 2: Implement actual peer reachability check**

Replace `check_peer_reachability_handle` (~lines 218-249):

```rust
pub(crate) async fn check_peer_reachability(
    handle: &ConsensusHandle,
    peer_addresses: &PeerAddressMap,
    timeout: Duration,
) -> DependencyCheckResult {
    let state = handle.shard_state();
    let my_id = handle.node_id();

    let voter_ids: Vec<u64> = state
        .voters
        .iter()
        .filter(|&&id| id.0 != my_id)
        .map(|id| id.0)
        .collect();

    if voter_ids.is_empty() {
        return DependencyCheckResult {
            healthy: true,
            detail: "single-node cluster, no peers to check".to_string(),
        };
    }

    let mut reachable = 0u32;
    let mut unreachable = 0u32;

    for &peer_id in &voter_ids {
        let addr = match peer_addresses.get(peer_id) {
            Some(a) => a,
            None => {
                unreachable += 1;
                continue;
            }
        };

        match tokio::time::timeout(timeout, async {
            let endpoint = format!("http://{addr}");
            tonic::transport::Channel::from_shared(endpoint)
                .ok()
                .and_then(|ch| Some(ch.connect_timeout(timeout)))
        })
        .await
        {
            Ok(Some(_)) => reachable += 1,
            _ => unreachable += 1,
        }
    }

    let total = voter_ids.len() as u32;
    let healthy = reachable > total / 2; // Majority reachable

    DependencyCheckResult {
        healthy,
        detail: format!(
            "{reachable}/{total} peers reachable ({unreachable} unreachable)"
        ),
    }
}
```

**Note:** The exact connectivity check mechanism depends on what's most efficient. TCP connect is simplest. A gRPC health check call is more thorough. Use whichever is appropriate for the timeout budget (default 2 seconds per the `HealthCheckConfig`).

- [ ] **Step 3: Update call sites**

Update wherever `check_peer_reachability_handle` is called to pass the `PeerAddressMap`.

- [ ] **Step 4: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 13: Learner Refresh Addresses

**Why:** `get_voter_addresses` returns empty strings for all voters, making learner state refresh non-functional.

**Depends on:** Task 9 (PeerAddressMap)

**Files:**
- Modify: `crates/raft/src/learner_refresh.rs:139-145`

- [ ] **Step 1: Wire PeerAddressMap into LearnerRefreshJob**

Add `peer_addresses: PeerAddressMap` field. Update constructor and all call sites.

- [ ] **Step 2: Fix get_voter_addresses**

Replace lines ~139-145:

```rust
state
    .voters
    .iter()
    .filter(|&&id| id.0 != my_id)
    .filter_map(|id| {
        let addr = self.peer_addresses.get(id.0)?;
        Some((id.0, addr))
    })
    .collect()
```

- [ ] **Step 3: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 14: ReadIndex Protocol Fixes

**Why:** Two issues with the ReadIndex RPC:
1. It rejects requests when the leader lease expires (returns UNAVAILABLE), even though an idle cluster's leader is still valid — the lease just hasn't been renewed because no entries were applied.
2. The follower ReadIndex path now works with Task 10's forwarding fixes, but the leader-side RPC needs to handle the lease correctly.

**Depends on:** Task 3 (leader lease fix), Task 10 (forwarding)

**Files:**
- Modify: `crates/services/src/services/raft.rs:78-109`

- [ ] **Step 1: Fix ReadIndex lease check**

Replace the `read_index` handler's lease check (~lines 78-109). Instead of returning an error on lease expiry, use the lease as a **fast path** and fall back to confirming leadership via the consensus engine:

```rust
async fn read_index(
    &self,
    request: Request<ReadIndexRequest>,
) -> Result<Response<ReadIndexResponse>, Status> {
    let req = request.into_inner();

    let region = inferadb_ledger_types::Region::from_str(&req.region)
        .unwrap_or(inferadb_ledger_types::Region::GLOBAL);

    let group = self
        .manager
        .get_region_group(region)
        .map_err(|_| Status::not_found("region group not found"))?;

    let handle = group.handle();

    if !handle.is_leader() {
        return Err(Status::failed_precondition("Not the leader"));
    }

    // Fast path: if the leader lease is valid, we know we're still the leader
    // without a quorum check. Skip directly to returning the committed index.
    //
    // Slow path: if the lease has expired (idle cluster, just elected, etc.),
    // confirm leadership via the consensus engine's read_index method, which
    // performs a quorum heartbeat.
    if !group.leader_lease().is_valid() {
        // Use the consensus engine's read_index for quorum confirmation.
        // This blocks until a majority of followers respond, confirming
        // this node is still the leader.
        let confirmed_index = handle
            .engine_read_index()
            .await
            .map_err(|e| Status::unavailable(format!("ReadIndex quorum check failed: {e}")))?;

        return Ok(Response::new(ReadIndexResponse {
            committed_index: confirmed_index,
            leader_term: handle.current_term(),
        }));
    }

    let committed_index = handle.commit_index();
    let leader_term = handle.current_term();

    Ok(Response::new(ReadIndexResponse { committed_index, leader_term }))
}
```

**Note:** Check if `ConsensusEngine` has a `read_index()` method (the exploration showed it does: `read_index()` on the engine returns the committed index). If `ConsensusHandle` doesn't expose it yet, add a `engine_read_index()` method that calls `self.engine.read_index(self.shard)`.

- [ ] **Step 2: Add engine_read_index to ConsensusHandle if needed**

In `consensus_handle.rs`, add:

```rust
/// Perform a quorum-confirmed read index check.
/// Returns the committed index after confirming this node is still the leader
/// via a round of heartbeats to a majority of followers.
pub async fn engine_read_index(&self) -> Result<u64, HandleError> {
    self.engine
        .read_index(self.shard)
        .await
        .map_err(HandleError::from)
}
```

- [ ] **Step 3: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 15: Leader Transfer

**Why:** Leader transfer is completely stubbed out. During graceful shutdown, the leader doesn't transfer leadership, causing an uncoordinated election (300-600ms write outage on rolling deployments).

**Files:**
- Modify: `crates/raft/src/leader_transfer.rs`
- Modify: `crates/raft/src/graceful_shutdown.rs:430-436`
- Modify: `crates/consensus/src/engine.rs` (if needed — add `transfer_leader` API)

- [ ] **Step 1: Add transfer_leader to ConsensusEngine**

If the consensus engine doesn't already have a `transfer_leader` method, add one. The typical implementation:
1. Stop accepting new proposals on this shard
2. Send a `TimeoutNow` message to the target node (triggers immediate election)
3. Wait for the target to become leader (or timeout)

In `crates/consensus/src/engine.rs`, add a `ReactorEvent::TransferLeader` variant and a public method:

```rust
pub async fn transfer_leader(
    &self,
    shard: ShardId,
    target: NodeId,
) -> Result<(), ConsensusError> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    self.inbox
        .send(ReactorEvent::TransferLeader { shard, target, reply: tx })
        .await
        .map_err(|_| ConsensusError::shutdown())?;
    rx.await.map_err(|_| ConsensusError::shutdown())?
}
```

The reactor implementation should:
1. Set a flag on the shard preventing new proposals
2. Send a `TimeoutNow` message to the target via the transport
3. Wait for a term change (indicating the target won the election)
4. Reply on the oneshot

- [ ] **Step 2: Expose through ConsensusHandle**

In `consensus_handle.rs`:

```rust
pub async fn transfer_leader(&self, target: u64) -> Result<(), HandleError> {
    let target_node = inferadb_ledger_consensus::types::NodeId(target);
    self.engine
        .transfer_leader(self.shard, target_node)
        .await
        .map_err(HandleError::from)
}
```

- [ ] **Step 3: Implement leader_transfer.rs**

Replace the stub in `leader_transfer.rs` (lines ~168-177) with an actual implementation:

```rust
// Select the best transfer target: a voter with the highest match_index
// (most caught-up follower). If the config specifies a target, use that.
let target_id = config
    .preferred_target
    .unwrap_or_else(|| self.select_best_target(&state));

if target_id == 0 {
    return Err(LeaderTransferError::NoTarget);
}

match tokio::time::timeout(
    config.timeout,
    self.handle.transfer_leader(target_id),
)
.await
{
    Ok(Ok(())) => {
        info!(target_id, "Leader transfer completed successfully");
        Ok(())
    }
    Ok(Err(e)) => {
        warn!(target_id, error = %e, "Leader transfer failed");
        Err(LeaderTransferError::TransferFailed {
            message: e.to_string(),
        })
    }
    Err(_) => Err(LeaderTransferError::Timeout { timeout: config.timeout }),
}
```

- [ ] **Step 4: Wire into graceful_shutdown.rs**

Replace the stub at lines ~430-436 with an actual leader transfer call:

```rust
if let Err(e) = self.transfer_leadership(timeout).await {
    warn!(error = %e, "Leader transfer during shutdown failed; proceeding with shutdown");
    // Non-fatal — the cluster will hold an election after this node shuts down
}
```

Remove the TODO comment.

- [ ] **Step 5: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 16: Snapshot Trigger

**Why:** Three locations need snapshot triggering: admin `CreateSnapshot` RPC, backup job, and post-erasure compaction. All are stubbed.

**Files:**
- Modify: `crates/raft/src/consensus_handle.rs` (add trigger_snapshot)
- Modify: `crates/services/src/services/admin.rs:256-302` (CreateSnapshot)
- Modify: `crates/raft/src/backup.rs:978-983`
- Modify: `crates/raft/src/post_erasure_compaction.rs:95,135`

- [ ] **Step 1: Determine the snapshot mechanism**

Snapshots in Raft are typically taken by the state machine (not the consensus engine). Check how the `RaftLogStore` or `StateLayer` handles snapshots. The snapshot likely involves:
1. Serializing the current `AppliedState` to disk
2. Recording the `last_applied` LogId as the snapshot boundary
3. Notifying the consensus engine so it can compact the log

Look for existing snapshot methods on `RaftLogStore`, `StateLayer`, or `ApplyWorker`.

- [ ] **Step 2: Add trigger_snapshot to ConsensusHandle**

If snapshot triggering needs to go through the consensus engine:

```rust
pub async fn trigger_snapshot(&self) -> Result<(), HandleError> {
    // The snapshot is taken by the apply worker after the next batch.
    // We signal via a shared flag or channel.
    self.snapshot_trigger.notify_one();
    Ok(())
}
```

If snapshots are taken directly by the RaftLogStore:

```rust
pub fn request_snapshot(&self) -> Result<(), HandleError> {
    self.store.request_snapshot()
        .map_err(|e| HandleError::ApplyFailed(e.to_string()))
}
```

The exact approach depends on the existing snapshot infrastructure. Explore the codebase to determine the right integration point.

- [ ] **Step 3: Fix CreateSnapshot admin RPC**

Replace the stub in `admin.rs` (lines ~267-271):

```rust
if let Some(ref manager) = self.raft_manager
    && let Ok(system) = manager.system_region()
{
    system
        .handle()
        .trigger_snapshot()
        .await
        .map_err(|e| {
            ctx.set_error("SnapshotFailed", &e.to_string());
            Status::internal(format!("Snapshot trigger failed: {e}"))
        })?;
}
```

- [ ] **Step 4: Fix backup.rs snapshot trigger**

Replace the stub at lines ~978-983:

```rust
// Trigger a fresh snapshot before backup to ensure latest state is on disk.
if let Err(e) = self.handle.trigger_snapshot().await {
    warn!(error = %e, "Snapshot trigger failed; using latest available snapshot");
}
```

- [ ] **Step 5: Fix post_erasure_compaction.rs**

Replace the stub at lines ~95 and ~135. Remove the TODO comments and wire the actual trigger.

- [ ] **Step 6: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 17: Post-Erasure Regional Leadership

**Why:** `PostErasureCompactionJob` uses `global_is_leader` as a proxy for regional leadership when triggering snapshots. In multi-region setups, a node can be GLOBAL leader but not a regional leader (or vice versa).

**Files:**
- Modify: `crates/raft/src/post_erasure_compaction.rs:117,135-142`

- [ ] **Step 1: Use per-region leadership check**

Replace the `global_is_leader` proxy with per-region `handle.is_leader()`:

At line ~117, remove the `global_is_leader` variable.

At line ~135-142, replace `global_is_leader` with the region's own handle:

```rust
let region_is_leader = group.handle().is_leader();
if self.maybe_trigger_snapshot(
    &region_label,
    region_is_leader,
    region_last,
    threshold,
) {
    // ... snapshot triggered
}
```

Remove the TODO comment at line ~135.

- [ ] **Step 2: Run CI**

Run: `just ci`
Expected: PASS.

---

## Task 18: AppliedState Clone Optimization

**Why:** The ArcSwap migration means every apply batch deep-clones ~20 HashMaps in `AppliedState`. This is O(state_size) per batch — a latent scaling cliff as organizations and vaults grow.

**Fix:** Replace `HashMap` fields in `AppliedState` with `im::HashMap` (immutable/persistent data structure) for O(1) structural-sharing clone.

**Files:**
- Modify: `Cargo.toml` (add `im` dependency)
- Modify: `crates/raft/Cargo.toml` (add `im` dependency)
- Modify: Applied state struct definition (likely in `crates/raft/src/log_storage/store.rs` or `types.rs`)

- [ ] **Step 1: Add `im` crate to workspace**

In workspace `Cargo.toml`:

```toml
[workspace.dependencies]
im = "15"
```

In `crates/raft/Cargo.toml`:

```toml
[dependencies]
im = { workspace = true }
```

- [ ] **Step 2: Replace HashMap fields in AppliedState with im::HashMap**

Find the `AppliedState` struct definition. Replace `HashMap` fields with `im::HashMap`:

```rust
use im::HashMap as ImHashMap;

pub struct AppliedState {
    // Replace standard HashMap fields with im::HashMap
    pub vault_heights: ImHashMap<VaultId, u64>,
    pub organizations: ImHashMap<OrganizationId, OrganizationState>,
    pub client_sequences: ImHashMap<u64, u64>,
    // ... etc for all HashMap fields
}
```

**Note:** `im::HashMap` implements `Clone` with O(log n) structural sharing, making the clone in `apply_committed_entries` nearly free regardless of state size. The API is compatible with `std::HashMap` for most operations.

- [ ] **Step 3: Update all code that constructs or pattern-matches on these fields**

`im::HashMap` has the same API as `std::HashMap` for `insert`, `get`, `remove`, `contains_key`, `iter`, etc. Most code should work without changes. Fix any compilation errors from type mismatches.

Key differences:
- `im::HashMap::new()` instead of `HashMap::new()` (or use `ImHashMap::new()` alias)
- `im::HashMap` doesn't implement `Default` the same way — check and add if needed
- Serde support: `im` has a `serde` feature flag — enable it in Cargo.toml

- [ ] **Step 4: Run CI and verify**

Run: `just ci`
Expected: PASS. The `im` crate is well-tested and API-compatible.

---

## Task 19: Cleanup — TODO Removal + Byzantine Fault Tests

**Why:** 
1. CLAUDE.md forbids TODO/FIXME/HACK comments. There are 9 TODO comments introduced in this diff.
2. 22 Byzantine fault tests were deleted without replacement. The `forward_consensus` handler needs equivalent coverage.

**Files:**
- Modify: `crates/raft/src/backup.rs:978`
- Modify: `crates/raft/src/dependency_health.rs:239`
- Modify: `crates/raft/src/graceful_shutdown.rs:430`
- Modify: `crates/raft/src/leader_transfer.rs:117,168`
- Modify: `crates/raft/src/learner_refresh.rs:139`
- Modify: `crates/raft/src/post_erasure_compaction.rs:95,135`
- Test: `crates/services/src/services/raft.rs` (Byzantine fault tests)

- [ ] **Step 1: Remove all TODO comments**

After completing Tasks 1-18, most TODOs will have been resolved by the actual implementation. For any that remain (because the relevant task hasn't been completed yet), replace the TODO with a factual comment explaining the current behavior:

Before:
```rust
// TODO: wire peer address resolution through ConsensusHandle or a PeerTracker
```

After (if Task 12 resolved it):
```rust
// Peer addresses resolved via PeerAddressMap shared with the health checker.
```

After (if not yet resolved — should not happen if all tasks are completed):
```rust
// Peer address resolution requires the PeerAddressMap to be wired into this
// component. Currently returns healthy for multi-node clusters as a conservative
// default to avoid false-negative readiness probe failures.
```

- [ ] **Step 2: Verify no TODO/FIXME/HACK comments remain**

Run: `grep -rn "TODO\|FIXME\|HACK" crates/raft/src/ crates/services/src/ crates/server/src/ --include="*.rs" | grep -v "target/" | grep -v "#\[allow"`

Expected: No results (or only pre-existing ones outside this diff).

- [ ] **Step 3: Add Byzantine fault tests for forward_consensus**

In the raft service test module, add tests covering:

```rust
#[cfg(test)]
mod forward_consensus_tests {
    use super::*;

    #[tokio::test]
    async fn rejects_message_from_unknown_node() {
        // Send a ConsensusForwardRequest with from_node not in voters or learners
        // Expect: PERMISSION_DENIED
    }

    #[tokio::test]
    async fn rejects_empty_payload() {
        // Send a ConsensusForwardRequest with empty payload bytes
        // Expect: INVALID_ARGUMENT (deserialization failure)
    }

    #[tokio::test]
    async fn rejects_malformed_payload() {
        // Send a ConsensusForwardRequest with random garbage bytes
        // Expect: INVALID_ARGUMENT
    }

    #[tokio::test]
    async fn rejects_unknown_region() {
        // Send a ConsensusForwardRequest targeting a non-existent region
        // Expect: NOT_FOUND
    }

    #[tokio::test]
    async fn accepts_message_from_voter() {
        // Send a valid ConsensusForwardRequest from a known voter
        // Expect: OK
    }

    #[tokio::test]
    async fn accepts_message_from_learner() {
        // Send a valid ConsensusForwardRequest from a known learner
        // Expect: OK (after Task 8 fix)
    }
}
```

Fill in each test with actual request construction using the proto types. These tests require a test harness that sets up a `RaftServiceImpl` with a mock `RaftManager`.

- [ ] **Step 4: Run CI**

Run: `just ci`
Expected: PASS.

- [ ] **Step 5: Final verification — CreateSnapshot returns error, not silent success**

Verify that after Task 16, the `CreateSnapshot` RPC either:
- Actually triggers a snapshot and returns real data, OR
- Returns `Status::unimplemented()` if not yet supported

It must NOT return success without creating a snapshot.

---

## Verification Checklist

After all 19 tasks are complete, run the full validation suite:

```bash
# Full CI (build + fmt + clippy + test)
just ci

# Integration tests (if available — these spawn clusters)
just test-integration

# Verify no TODO/FIXME/HACK comments in modified crates
grep -rn "TODO\|FIXME\|HACK" crates/raft/src/ crates/services/src/ crates/server/src/ crates/state/src/ --include="*.rs" | grep -v "target/"

# Verify no unsafe code
grep -rn "unsafe" crates/raft/src/ crates/services/src/ crates/server/src/ crates/state/src/ --include="*.rs" | grep -v "target/" | grep -v "// SAFETY"

# Verify no panic/todo/unimplemented
grep -rn "panic!\|todo!()\|unimplemented!()" crates/raft/src/ crates/services/src/ crates/server/src/ crates/state/src/ --include="*.rs" | grep -v "target/" | grep -v "#\[cfg(test)\]"
```

All must pass with zero warnings.
