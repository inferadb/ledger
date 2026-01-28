# System Invariants

Formal guarantees the system maintains under all conditions.

## Safety Invariants

| #   | Invariant             | Description                                                             |
| --- | --------------------- | ----------------------------------------------------------------------- |
| 1   | Commitment permanence | Committed blocks cannot be removed or modified                          |
| 2   | State determinism     | Replaying identical transactions produces identical state_root          |
| 3   | Chain continuity      | Every block (except genesis) links to its predecessor via previous_hash |
| 4   | Quorum requirement    | Commits require 2f+1 node acknowledgment                                |

## Liveness Invariants

| #   | Invariant                       | Description                                                     |
| --- | ------------------------------- | --------------------------------------------------------------- |
| 5   | Progress under minority failure | Fewer than f+1 failures → writes continue                       |
| 6   | Eventual convergence            | After partition heals, all replicas converge to same state      |
| 7   | Leader availability             | New leader elected within election timeout after leader failure |

## Integrity Invariants

| #   | Invariant             | Description                                                                                          |
| --- | --------------------- | ---------------------------------------------------------------------------------------------------- |
| 8   | Merkle soundness      | Invalid values cannot produce valid proofs                                                           |
| 9   | Hash chain integrity  | Modifying any block invalidates all subsequent blocks                                                |
| 10  | Transaction atomicity | All operations in a transaction apply together or not at all                                         |
| 11  | State determinism     | Identical transactions on identical state produce identical state_root; divergence halts the replica |

## Operational Invariants

| #   | Invariant                | Description                                                               |
| --- | ------------------------ | ------------------------------------------------------------------------- |
| 12  | Snapshot consistency     | Loading snapshot + applying subsequent blocks produces current state_root |
| 13  | Historical accessibility | Any committed block is readable with verifiable proofs                    |
| 14  | Replication durability   | After Raft commit, data exists on quorum                                  |

## Reconfiguration Invariants

| #   | Invariant                     | Description                                                            |
| --- | ----------------------------- | ---------------------------------------------------------------------- |
| 15  | Joint consensus atomicity     | Membership changes use joint consensus; no single-step reconfiguration |
| 16  | Serialized membership changes | At most one membership change in progress per shard                    |
| 17  | No-op commit on election      | New leaders commit no-op before accepting membership changes           |

## Verification Invariants

| #   | Invariant                  | Description                                                               |
| --- | -------------------------- | ------------------------------------------------------------------------- |
| 18  | Block header permanence    | Block headers (`state_root`, `previous_hash`) are never deleted           |
| 19  | Snapshot chain continuity  | Every snapshot links to genesis via `genesis_hash` and `chain_commitment` |
| 20  | Verifiability preservation | Integrity check succeeds regardless of transaction body compaction        |

## Shard Group Invariants

| #   | Invariant                    | Description                                                                   |
| --- | ---------------------------- | ----------------------------------------------------------------------------- |
| 21  | Vault chain independence     | Each vault's chain is independently verifiable regardless of shard assignment |
| 22  | Shard membership consistency | Namespace-to-shard mapping in `_system` matches actual shard state            |
| 23  | Cross-shard isolation        | Transactions in one shard cannot affect state in another shard                |

## Idempotency Invariants

| #   | Invariant             | Description                                                                          |
| --- | --------------------- | ------------------------------------------------------------------------------------ |
| 24  | Sequence monotonicity | For any client, committed sequences are strictly increasing (no gaps, no duplicates) |
| 25  | Duplicate rejection   | A sequence ≤ `last_committed_seq` never creates a new transaction                    |
| 26  | Sequence persistence  | `last_committed_seq` per client survives leader failover (part of vault state)       |

## Operation Semantics Invariants

| #   | Invariant                         | Description                                                                                                   |
| --- | --------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| 27  | Operation idempotency             | Replaying the same operation produces identical state (CREATE on existing = no-op, DELETE on missing = no-op) |
| 28  | Raft ordering finality            | Operation order determined by Raft log index; no reordering after commit                                      |
| 29  | Deterministic conflict resolution | Given same initial state and operation sequence, all replicas produce identical final state                   |

## Storage Invariants

| #   | Invariant                 | Description                                                       |
| --- | ------------------------- | ----------------------------------------------------------------- |
| 30  | Raft log durability       | Log entries are fsync'd before Raft acknowledgment                |
| 31  | State consistency         | `state.db` reflects all applied log entries up to `applied_index` |
| 32  | Block archive append-only | Segment files are never modified after creation                   |
| 33  | Snapshot validity         | Snapshot `state_root` matches block header at `shard_height`      |

## Multi-Vault Failure Isolation Invariants

| #   | Invariant                         | Description                                                      |
| --- | --------------------------------- | ---------------------------------------------------------------- |
| 34  | Vault divergence read isolation   | Vault divergence does not affect read availability of other vaults in the same shard |
| 35  | Vault divergence write isolation  | Vault divergence does not block writes to other vaults in the same shard |
| 36  | Vault recovery independence       | Vault recovery requires no shard-wide coordination               |
| 37  | Determinism bug surfacing         | Determinism bugs surface during recovery replay                  |

## Storage Layer Boundaries

| Layer         | Contents                                  | Truncatable                       | Purpose                    |
| ------------- | ----------------------------------------- | --------------------------------- | -------------------------- |
| Raft WAL      | Uncommitted/recent log entries            | Yes, after snapshot               | Consensus, leader catch-up |
| Block Archive | Committed blocks (headers + transactions) | Headers: never; Txs: configurable | Verification, audit        |
| State Layer   | Materialized K/V indexes                  | Rebuilt from chain                | Fast queries               |

**Critical distinction**: Raft WAL truncation does not affect the Block Archive. After commit, blocks move from WAL to permanent storage. The WAL can then be truncated without losing verification capability.
