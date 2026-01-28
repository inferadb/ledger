# Consensus

This document covers Raft integration, write/read paths, batching, and state determinism.

## Raft Integration

Ledger uses [Openraft](https://github.com/datafuselabs/openraft) for consensus. Each shard has one Raft group; multiple namespaces share a shard.

### Raft Properties

- **Leader completeness**: Committed entries are present in all future leaders' logs
- **State machine safety**: All nodes apply the same operations in the same order
- **Linearizability**: Writes appear atomic at some point between request and response

### Transport

gRPC/HTTP2 over TCP for both client APIs and inter-node Raft messages. TCP's kernel implementation and hardware offload provide optimal latency for single-stream consensus.

## Write Path

```
Client → Leader: WriteRequest
Leader → Followers: Raft AppendEntries
Followers → Leader: Ack
[Quorum reached]
Leader → Followers: Commit + Apply
Leader: Construct Block, Update State Tree
Leader → Client: WriteResponse (block_height, tx_proof)
```

### Write Stages

1. **Received**: Leader accepts transaction into pending queue
2. **Replicated**: Raft log entry replicated to quorum (2f+1 nodes)
3. **Committed**: Raft marks entry as committed
4. **Applied**: Block constructed, state tree updated, state_root computed
5. **Persisted**: Block written to disk with fsync

**Client guarantee**: `WriteResponse` returns after stage 4. Data is durable once replicated to quorum.

### Block Finality

A block is final when:

- Committed in the Raft log (quorum acknowledged)
- Applied to the state tree
- `state_root` computed and included in the block header

**Reorg safety**: Raft guarantees committed entries are never removed. No reorg risk—once committed, a block is permanent.

## Transaction Batching

Batching amortizes Raft consensus overhead across transactions.

### Authorization Workload Characteristics

| Metric              | Typical Value   | Implication                                |
| ------------------- | --------------- | ------------------------------------------ |
| Read:write ratio    | 100:1 to 1000:1 | Writes are rare, optimize for low latency  |
| Write burst size    | 10-1000 ops     | Policy updates come in batches             |
| Transaction size    | ~200 bytes      | Lightweight, validation cost negligible    |
| Latency sensitivity | High            | Security policies should propagate quickly |

### Adaptive Batching

```rust
struct BatchConfig {
    max_batch_size: usize,     // Default: 50
    batch_timeout: Duration,   // Default: 2ms
    eager_commit: bool,        // Default: true
}
```

**Key behaviors**:

- **Eager commit**: When queue drains, commit immediately. Single-transaction blocks are acceptable.
- **Burst absorption**: During policy update bursts, batches fill before timeout.
- **No artificial floor**: A single transaction can commit in <1ms (plus Raft RTT).

### Configuration Examples

| Workload              | max_batch_size | batch_timeout | eager_commit |
| --------------------- | -------------- | ------------- | ------------ |
| Interactive (default) | 100            | 2ms           | true         |
| Batch import          | 500            | 20ms          | false        |
| Real-time sync        | 10             | 1ms           | true         |

### Latency Breakdown (single transaction, eager commit)

```
Client → Leader:        ~0.5ms (network)
Raft AppendEntries:     ~1-2ms (quorum RTT)
State application:      ~0.1ms
State root computation: ~0.5ms (bucket-based)
Response:               ~0.5ms (network)
─────────────────────────────────
Total p50:              ~3-4ms
Total p99:              ~10-15ms
```

## Read Path

```
Client → Any Node: ReadRequest (key, proof?)
Node: Query State Tree (optionally generate proof)
Node → Client: ReadResponse (value, block_height, merkle_proof?)
```

### Read Types

| Read Type     | Consistency           | Use Case                      |
| ------------- | --------------------- | ----------------------------- |
| Leader read   | Linearizable          | Strong consistency required   |
| Follower read | Eventually consistent | High throughput, staleness OK |
| Verified read | Linearizable + proven | Audit, compliance             |

Default: Reads go to any replica.

## State Determinism

All state machine operations must be deterministic. Identical transactions on identical state must produce identical `state_root`.

### Requirements

- **No timestamps in state**: Use block height, not wall-clock time
- **No random values**: If randomness needed, it comes from the leader
- **Consistent floating-point**: Use fixed-point arithmetic
- **Ordered iteration**: Use sorted maps/sets, not hash-based

### Raft Integration Sequence

```
1. Leader receives transactions
2. Leader applies transactions to state (tracks dirty keys)
3. Leader computes state_root via incremental bucket hashing
4. Leader constructs block with state_root
5. Leader proposes block to Raft (AppendEntries)
6. Followers replicate log entry
7. On commit: followers apply transactions, verify state_root
8. If state_root mismatch: follower halts and alerts
```

## Multi-Vault Failure Isolation

Multiple vaults share a Raft group (shard). A `state_root` divergence in one vault must not cascade to others.

### Isolation Boundaries

| Component                       | Shared | Independent |
| ------------------------------- | ------ | ----------- |
| Raft log (ordering, durability) | Yes    |             |
| ShardBlock delivery             | Yes    |             |
| VaultEntry application          |        | Yes         |
| State commitment (state_root)   |        | Yes         |
| Failure handling (vault health) |        | Yes         |

### Vault Health States

```rust
enum VaultHealth {
    Healthy,
    Diverged { expected: Hash, computed: Hash, at_height: u64 },
    Recovering { started_at: DateTime<Utc>, attempt: u8 },
}
```

### Divergence Handling

When a follower computes a different `state_root` than the block header:

1. Rollback uncommitted state for that vault only
2. Mark vault `Diverged`
3. Emit `state_root_divergence{vault_id, shard_id}` alert
4. Continue processing remaining vaults in the block
5. Return `VAULT_UNAVAILABLE` for reads to diverged vault
6. Continue replicating Raft log; store but don't apply diverged vault's entries

### Automatic Recovery

Diverged vaults recover automatically with bounded retries:

| Attempt | Backoff    | Action on Failure           |
| ------- | ---------- | --------------------------- |
| 1       | Immediate  | Retry                       |
| 2       | 30 seconds | Retry                       |
| 3       | 5 minutes  | Require manual intervention |

After 3 failed attempts, the vault emits `vault_recovery_exhausted{vault_id}` and requires operator intervention.

**Recovery process**:

1. Mark vault as `Recovering`
2. Clear vault's state tree
3. Load latest snapshot for this vault
4. Replay Raft log from snapshot point
5. Verify state roots match at each height
6. On success: transition to `Healthy`

## Durability Model

### Write-Ahead Log (WAL)

Each node maintains a WAL for Raft log entries:

- Entries fsync'd before acknowledging to leader
- WAL truncated after snapshotting
- Recovery replays WAL from last snapshot

### Fault Tolerance

| Node failure (minority)               | Node failure (majority)                  |
| ------------------------------------- | ---------------------------------------- |
| Raft handles automatically            | Vault becomes read-only                  |
| Remaining nodes continue              | Reads still work from surviving replicas |
| Failed node catches up via log replay | Manual intervention to restore quorum    |

## Concurrent Operation Resolution

Raft provides deterministic total ordering. Concurrent operations from different clients serialize in Raft log order:

```
Client A: CREATE(user:alice, viewer, doc:1)  →  Raft index 100
Client B: DELETE(user:alice, viewer, doc:1)  →  Raft index 101

Result: Relationship does NOT exist (DELETE at 101 wins)
```

**Security note**: DELETE then CREATE can unexpectedly grant access. Mitigations:

1. **Check after write**: Read at committed block height to confirm final state
2. **Tombstone TTL**: Configure cooldown during which deleted relationships cannot be recreated

**Fail-secure default**: For authorization, deny access on ambiguity.
