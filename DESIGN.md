# InferaDB Ledger Design Document

**Last Updated**: January 2026

--

## Problem Statement

### The Authorization Audit Gap

Access control failures consistently rank among the most severe security vulnerabilities. OWASP lists Broken Access Control as the #1 web application security risk. When breaches occur, organizations need answers: Who had access to what? When did permissions change? Was this access legitimate when granted?

Traditional authorization systems struggle with definitive answers. Audit logs can be modified, timestamps forged, and historical state reconstruction requires trusting the system that may have been compromised.

### Why Not Existing Solutions?

**Zanzibar-style systems (SpiceDB, OpenFGA)** provide excellent authorization semantics and performance, but audit logs lack cryptographic guarantees. Compliance requires trusting the database operator.

**Traditional blockchains** provide cryptographic verification but impose 10-100ms latency per operation. For authorization workloads with thousands of permission checks per second, this overhead is prohibitive.

**Merkle Patricia Tries** (used by Ethereum) require O(log n) operations per key access. Each state update triggers tree rebalancing and hash recomputations—impractical for real-time authorization.

### The Core Tension

Authorization systems must be:

- **Fast enough** for real-time decisions (sub-millisecond reads)
- **Auditable enough** to withstand legal and security scrutiny

No existing solution satisfies both requirements.

---

## Goals and Non-Goals

### Goals

1. **Sub-millisecond read latency** — Authorization checks must not add perceptible latency to user requests
2. **Cryptographic auditability** — Every state change is committed to a tamper-evident chain that clients can verify independently
3. **Fault isolation** — A failure in one tenant's data cannot corrupt or affect another tenant
4. **Strong consistency** — Permission grants/revokes are immediately visible cluster-wide (linearizable writes)
5. **Operational simplicity** — Snapshot recovery, zero-downtime migrations, standard tooling (gRPC, Prometheus)

### Non-Goals

1. **Byzantine fault tolerance** — We assume trusted operators; Raft provides crash fault tolerance only
2. **Sub-millisecond writes** — Consensus overhead makes this impossible; we target <50ms p99
3. **Cross-vault transactions** — By design, for isolation guarantees
4. **Instant per-key merkle proofs** — We optimize for verification via replay, not instant proofs
5. **Infinite retention** — Storage costs require configurable retention policies
6. **General-purpose blockchain** — Optimized specifically for authorization workloads

---

## Solution Overview

Ledger resolves the performance-verifiability tension through **separation of concerns**:

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Client Request                               │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                 gRPC Services (Read / Write / Admin)                 │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    Raft Consensus (OpenRaft)                         │
│              Strong consistency, automatic leader election           │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
          ┌─────────────────────┴─────────────────────┐
          ▼                                           ▼
┌─────────────────────────┐             ┌─────────────────────────────┐
│    State Storage        │             │    State Commitment         │
│   (Fast K/V Indexes)    │             │   (Per-Block Merkle Roots)  │
│                         │             │                             │
│   • O(1) lookups        │             │   • tx_merkle_root          │
│   • Dual indexes        │             │   • state_root              │
│   • No merkle overhead  │             │   • Computed at block end   │
└─────────────────────────┘             └─────────────────────────────┘
```

**Key insight**: Cryptographic verification doesn't require merkleizing every write. We compute state roots at block boundaries (amortized) rather than per-operation.

### How It Works

1. **Writes flow through Raft** — All mutations are proposed to the leader, replicated to majority, then committed
2. **State updates are fast** — Direct K/V writes with dual indexes for authorization traversal
3. **State roots are computed once per block** — O(k) bucket-based hashing where k = keys modified
4. **Verification happens on-demand** — Replay transactions from snapshot when audit needed

### The Bucket-Based State Root

Traditional approaches hash the entire state tree on every write. We use 256 buckets keyed by the first byte of each key's hash:

```
state_root = SHA-256(bucket_hash[0] || bucket_hash[1] || ... || bucket_hash[255])
```

When a block modifies k keys:

- Identify affected buckets: O(k)
- Recompute affected bucket hashes: O(k)
- Recompute state root: O(256) = O(1)

**Total: O(k)** — independent of total database size.

For a vault with 10M keys and 100 updates per block:

- Bucket approach: ~356 hash operations
- Naive MPT: ~100 × 23 = 2,300 hash operations

### Multi-Tenant Isolation

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Shard Group (Raft)                          │
│                    Nodes A, B, C sharing consensus                  │
├─────────────────────────────┬───────────────────────────────────────┤
│     Namespace: org_acme     │        Namespace: org_startup         │
│  ┌─────────┐  ┌─────────┐   │       ┌─────────┐                     │
│  │ Vault:  │  │ Vault:  │   │       │ Vault:  │                     │
│  │  prod   │  │ staging │   │       │  main   │                     │
│  │ Chain── │  │ Chain── │   │       │ Chain── │                     │
│  └─────────┘  └─────────┘   │       └─────────┘                     │
│      Entities               │           Entities                    │
└─────────────────────────────┴───────────────────────────────────────┘
```

| Level     | Isolation                       | Shared                 |
| --------- | ------------------------------- | ---------------------- |
| Namespace | Entities, vaults, keys          | Shard (Raft consensus) |
| Vault     | Cryptographic chain, state_root | Namespace storage      |
| Shard     | Physical nodes                  | Cluster                |

Each vault maintains its own blockchain. A corruption in Vault A cannot affect Vault B's chain integrity.

---

## Architecture

### Terminology

| Term                    | Definition                                                                                                              |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Namespace**           | Storage unit per organization. Contains entities and vaults. Isolated with separate storage.                            |
| **Vault**               | Relationship store within a namespace. Maintains its own cryptographic chain (state_root, previous_hash, block height). |
| **Entity**              | Key-value data stored in a namespace (users, teams, clients, sessions). Supports TTL, versioning, conditional writes.   |
| **Relationship**        | Authorization tuple: `(resource, relation, subject)`. Used by Engine for permission checks.                             |
| **Shard**               | Operational grouping for consensus. Contains one or more namespaces sharing a Raft group.                               |
| **`_system` namespace** | Special namespace for global data: user accounts and namespace routing. Replicated to all nodes.                        |

### Block Structure

Each vault maintains its own logical blockchain within a namespace:

```rust
struct VaultBlock {
    // Identity
    height: u64,
    namespace_id: NamespaceId,
    vault_id: VaultId,

    // Chain linking (immutability)
    previous_hash: Hash,           // SHA-256 of previous vault block

    // Content
    transactions: Vec<Transaction>,
    tx_merkle_root: Hash,          // Merkle root of this block's transactions

    // State commitment (verification)
    state_root: Hash,              // Vault state AFTER applying this block

    // Metadata
    timestamp: DateTime<Utc>,
    leader_id: NodeId,
    term: u64,
    committed_index: u64,
}

struct Transaction {
    id: TxId,
    client_id: ClientId,
    sequence: u64,                 // Monotonic per-client (idempotency)
    actor: String,                 // Audit logging
    operations: Vec<Operation>,
    timestamp: DateTime<Utc>,
}

enum Operation {
    // Relationship operations (Engine)
    CreateRelationship { resource: String, relation: String, subject: String },
    DeleteRelationship { resource: String, relation: String, subject: String },

    // Entity operations (Control)
    SetEntity { key: String, value: Bytes, condition: Option<SetCondition>, expires_at: Option<u64> },
    DeleteEntity { key: String },
    ExpireEntity { key: String, expired_at: u64 },
}

enum SetCondition {
    MustNotExist,             // Create-only
    MustExist,                // Safe updates
    VersionEquals(u64),       // Optimistic locking
    ValueEquals(Vec<u8>),     // Exact state assertions
}
```

### Write Path

```
Client Request
    │
    ▼
[1] Any node receives request
    │
    ▼
[2] Forward to Raft leader (if not already leader)
    │
    ▼
[3] Leader validates + assigns sequence number
    │
    ▼
[4] Batcher aggregates (up to 100 txs or 10ms timeout)
    │
    ▼
[5] Batch becomes Raft proposal
    │
    ▼
[6] Leader replicates to followers
    │
    ▼
[7] Majority acknowledgment commits
    │
    ▼
[8] State layer applies transactions + computes state_root
    │
    ▼
[9] Response to client with state_root hash
```

**Target latency**: <50ms at p99 under normal load.

### Read Path

```
Client Request
    │
    ▼
[1] Any node receives request
    │
    ├── Linearizable read: confirm leadership or forward
    │
    └── Eventually consistent: serve from local state
    │
    ▼
[2] Storage layer retrieves from B+ tree indexes
    │
    ▼
[3] Response includes current state_root
```

**Target latency**: <2ms at p99.

### State Layer: Hybrid Storage

The state layer separates **state commitment** (merkleized) from **state storage** (fast K/V):

**What's merkleized**:

- `tx_merkle_root`: Proves transaction inclusion in block
- `state_root`: Proves state at height N

**What's NOT merkleized per-write**:

- Individual K/V entries (stored in B+ tree for O(1) lookup)
- Dual indexes for relationship traversal

**Verification approach**:

- Transaction inclusion: Standard merkle proof against `tx_merkle_root`
- State verification: Replay transactions from snapshot
- On-demand proofs: Compute merkle path when explicitly requested

```rust
struct StateLayer {
    kv: Database,                                        // Fast K/V storage
    obj_index: Table<ObjectKey, Vec<SubjectId>>,         // "Who can access X?"
    subj_index: Table<SubjectKey, Vec<ObjectId>>,        // "What can Y access?"
    state_root: Hash,                                    // Computed at block boundaries
}

impl StateLayer {
    fn get(&self, key: &[u8]) -> Option<Value> {
        self.kv.get(key)  // O(1) lookup, no merkle overhead
    }

    fn apply_block(&mut self, block: &Block) -> Hash {
        for tx in &block.transactions {
            for op in &tx.operations {
                self.apply_operation(op);
            }
        }
        self.compute_state_root()  // Amortized O(k) bucket hashing
    }
}
```

### Key Encoding

Storage keys encode vault isolation and bucket assignment:

```
┌─────────────────────────────────────────────────────────────────┐
│  vault_id   │  bucket_id  │         local_key                   │
│  (8 bytes)  │  (1 byte)   │         (N bytes)                   │
└─────────────────────────────────────────────────────────────────┘
```

- `seahash` for bucket assignment (fast, deterministic)
- SHA-256 for cryptographic hashes (state_root, bucket_roots)

### ID Generation

Ledger uses **leader-assigned sequential IDs** for deterministic replay:

| ID Type       | Size     | Generated By  | Strategy                        |
| ------------- | -------- | ------------- | ------------------------------- |
| `NamespaceId` | int64    | Ledger Leader | Sequential (0 = \_system)       |
| `VaultId`     | int64    | Ledger Leader | Sequential                      |
| `UserId`      | int64    | Ledger Leader | Sequential                      |
| `TxId`        | 16 bytes | Ledger Leader | UUID assigned at block creation |
| `NodeId`      | string   | Admin         | Configuration                   |
| `ShardId`     | uint32   | Admin         | Sequential at shard creation    |

The leader assigns IDs during block construction. Followers replay the same IDs from the Raft log—fully deterministic.

### Operation Semantics

All operations are **idempotent** with **Raft total ordering**:

| Operation            | Pre-state  | Post-state | Return           |
| -------------------- | ---------- | ---------- | ---------------- |
| `CreateRelationship` | not exists | created    | `CREATED`        |
| `CreateRelationship` | exists     | no change  | `ALREADY_EXISTS` |
| `DeleteRelationship` | exists     | deleted    | `DELETED`        |
| `DeleteRelationship` | not exists | no change  | `NOT_FOUND`      |
| `SetEntity`          | any        | value set  | `OK`             |
| `DeleteEntity`       | exists     | deleted    | `DELETED`        |
| `DeleteEntity`       | not exists | no change  | `NOT_FOUND`      |

**Concurrent operation resolution**: Raft provides deterministic total ordering. Operations serialize by Raft log index.

```
Client A: CREATE(user:alice, viewer, doc:1)  →  Raft index 100
Client B: DELETE(user:alice, viewer, doc:1)  →  Raft index 101

Result: Relationship does NOT exist (DELETE at 101 wins)
```

### Transaction Batching

Batching amortizes Raft consensus overhead:

| Pattern                         | Writes | Fsyncs       | Throughput       |
| ------------------------------- | ------ | ------------ | ---------------- |
| Single tuple (interactive)      | 3      | 1 (via Raft) | ~200 tx/sec      |
| N tuples to same resource       | 3N     | 1            | ~50K tuples/sec  |
| Bulk import (1000 tuples/block) | 3000   | 1            | ~100K tuples/sec |

Default batch configuration:

- Maximum batch size: 100 transactions
- Maximum batch delay: 10ms

### Fault Tolerance & Recovery

**Raft consensus** provides:

- Automatic leader election (election timeout: configurable)
- Replication to majority before commit
- Crash recovery via log replay

**Per-vault divergence detection**:

1. Follower applies block, computes local state_root
2. Compare against leader's state_root in block header
3. If mismatch: halt vault processing, emit alert
4. Resolution: rebuild from snapshot + log replay

Divergence indicates a bug or corruption, not consensus failure. The committing quorum is authoritative.

### Degraded Operation Modes

When failures occur, Ledger degrades gracefully rather than failing completely:

| Failure Scenario              | Write Availability | Read Availability | Recovery Action                     |
| ----------------------------- | ------------------ | ----------------- | ----------------------------------- |
| Single node down (3-node)     | ✓ Available        | ✓ Available       | Automatic failover                  |
| Minority nodes down           | ✓ Available        | ✓ Available       | Reduced redundancy, monitor closely |
| Majority nodes down           | ✗ Unavailable      | ✓ Stale reads     | Manual intervention required        |
| Leader network isolated       | ✓ After election   | ✓ Available       | New leader elected (~10s)           |
| State root divergence (vault) | ✗ Vault halted     | ✓ Available       | Rebuild vault from snapshot         |
| Disk full                     | ✗ Unavailable      | ✓ Available       | Expand storage, compact logs        |

**Partial availability**: When writes are unavailable, Ledger continues serving eventually consistent reads from any healthy replica. Applications can implement read-only degraded modes.

**Vault-level isolation**: A diverged vault does not affect other vaults in the same namespace. Only the affected vault halts writes pending recovery.

**Automatic recovery scope**: Ledger automatically recovers from transient failures (network blips, brief partitions). Persistent failures (disk corruption, state divergence) require operator intervention to prevent data loss.

---

## Scaling Architecture: Shard Groups

As vaults grow, namespaces can be distributed across **shard groups**—independent Raft clusters.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Cluster                                    │
├─────────────────────────────────────────────────────────────────────────┤
│  Shard 0              │  Shard 1              │  Shard 2                │
│  ┌─────────────────┐  │  ┌─────────────────┐  │  ┌─────────────────┐    │
│  │ Nodes A, B, C   │  │  │ Nodes D, E, F   │  │  │ Nodes G, H, I   │    │
│  │ Raft Group      │  │  │ Raft Group      │  │  │ Raft Group      │    │
│  └─────────────────┘  │  └─────────────────┘  │  └─────────────────┘    │
│  Namespaces: 1-100    │  Namespaces: 101-200  │  Namespaces: 201-300    │
└─────────────────────────────────────────────────────────────────────────┘
```

**Shard assignment**: Namespace-to-shard mapping stored in `_system` namespace. Assignment strategies:

- Hash-based (automatic distribution)
- Explicit (for performance isolation)
- Dedicated shard (for high-volume vaults)

**Cross-shard consistency**: No cross-shard transactions. Application-level coordination required for multi-namespace operations.

---

## Discovery & Coordination

### System Namespace (`_system`)

The `_system` namespace (NamespaceId = 0) stores cluster-wide metadata:

```
_system namespace
├── Users (global accounts)
├── Namespace routing table (namespace → shard mapping)
└── Cluster configuration
```

Replicated to all nodes via dedicated Raft group. All nodes can route requests without cross-shard queries.

### Client Discovery Flow

```
[1] Client connects to any node
[2] Sends request with namespace_id
[3] Node looks up namespace → shard mapping in local _system replica
[4] If local shard: process directly
    If remote shard: redirect client to shard leader
```

### Node Join

```
[1] New node contacts seed node
[2] Seed node adds new node as Raft learner
[3] Learner replicates log (may take time for large state)
[4] Once caught up, promoted to voter via joint consensus
[5] Cluster now has N+1 voting members
```

### Node Leave (Graceful)

```
[1] Operator initiates removal
[2] If leader: transfer leadership first
[3] Remove from Raft group via joint consensus
[4] Node shuts down
```

---

## Durability & Finality

### Finality Model

| Event                | Durability Guarantee                           |
| -------------------- | ---------------------------------------------- |
| Write acknowledged   | Persisted on majority (2f+1) nodes             |
| Block committed      | Cannot be reverted without majority corruption |
| State root published | Independently verifiable by any client         |

**No rollbacks**: Once committed, blocks are permanent. Corrections require compensating transactions.

### Crash Recovery

```
[1] Node restarts, loads last snapshot
[2] Replays Raft log from snapshot index
[3] Verifies state_root matches expected
[4] Rejoins cluster as follower
[5] Catches up with leader if behind
```

---

## Persistent Storage

### Directory Layout

```
/var/lib/ledger/
├── raft/
│   ├── log/                  # Raft log entries
│   └── state/                # Raft hard state (term, vote)
├── state/
│   ├── entities/             # Entity K/V data
│   └── relationships/        # Relationship tuples + indexes
├── snapshots/
│   ├── 000001.snapshot       # Periodic state snapshots
│   └── 000002.snapshot
└── blocks/
    └── archive/              # Compacted block headers
```

### Storage Backend

`inferadb-ledger-store` provides:

- B+ tree engine with page-level management
- ACID transactions
- Configurable backends (file-based, memory for testing)

---

## Control Data Storage

The Control service uses Ledger for organization and user management:

### Entity Key Patterns

```
# Organization entities
org:{org_id}                    → Organization metadata
org:{org_id}:member:{user_id}   → Membership with role

# User entities
user:{user_id}                  → User profile
user_email:{email_hash}         → Email → user_id mapping

# Session entities
session:{session_id}            → Session data (TTL-based expiration)
refresh_token:{token_hash}      → Refresh token metadata

# API clients
client:{client_id}              → Client credentials and permissions
```

### TTL and Expiration

Entities support automatic expiration:

```rust
SetEntity {
    key: "session:abc123",
    value: session_data,
    expires_at: Some(now + 24_hours),
}
```

Background GC removes expired entities, emitting `ExpireEntity` operations for audit trail.

---

## Client Idempotency & Retry Semantics

Clients include `(client_id, sequence)` with each request:

```rust
struct WriteRequest {
    client_id: String,      // Identifies client
    sequence: u64,          // Monotonic per-client
    operations: Vec<Operation>,
}
```

**Idempotency guarantee**: Same `(client_id, sequence)` returns cached response, no re-execution.

**Sequence tracking**: Per-vault in persistent state. Survives leader failover.

**Retry behavior**:

| Scenario                    | Client sees   | Correct action           |
| --------------------------- | ------------- | ------------------------ |
| `CREATE` → `ALREADY_EXISTS` | Goal achieved | No retry needed          |
| `DELETE` → `NOT_FOUND`      | Goal achieved | No retry needed          |
| Network timeout             | Unknown       | Retry with same sequence |

---

## Observability & Monitoring

### Prometheus Metrics

```
# Latency histograms
ledger_read_latency_seconds{quantile="0.99"}
ledger_write_latency_seconds{quantile="0.99"}
ledger_raft_commit_latency_seconds{quantile="0.99"}

# Throughput counters
ledger_transactions_total{status="committed|rejected"}
ledger_operations_total{type="create|delete|set"}

# State metrics
ledger_vault_height{vault_id}
ledger_raft_term{shard_id}
ledger_snapshot_age_seconds{vault_id}

# Error counters
ledger_state_root_divergence_total{vault_id}
ledger_raft_leader_changes_total{shard_id}
```

### Health Checks

- **Liveness**: Process is running
- **Readiness**: Can serve reads (Raft state is healthy)
- **Leader check**: Is this node the shard leader?

---

## Consistency Guarantees

### Within Namespace

| Read Type             | Guarantee          | Latency |
| --------------------- | ------------------ | ------- |
| Linearizable          | Reads own writes   | +1 RTT  |
| Eventually consistent | May see stale data | Fastest |

### Cross-Namespace

No transactions span namespaces. For multi-namespace operations:

- Application-level saga pattern
- Eventual consistency acceptable for most authorization use cases

---

## Trade-offs Analysis

This section consolidates all design trade-offs for decision archaeology.

### Hybrid Storage vs. Fully Merkleized

**Decision**: Separate state storage (fast K/V) from state commitment (per-block merkle roots).

**Why not fully merkleized (like Ethereum MPT)?**

| Aspect              | Fully Merkleized | Hybrid (Our Choice)  |
| ------------------- | ---------------- | -------------------- |
| Write amplification | 10-50x           | ~1x                  |
| Per-key proofs      | Instant          | Requires computation |
| State verification  | O(log n) proof   | Replay from snapshot |
| Query latency       | O(log n)         | O(1)                 |

We sacrifice instant per-key proofs for 10x lower write amplification and O(1) query latency. Verification via replay is acceptable for audit scenarios (not real-time).

### gRPC/HTTP/2 vs. Custom Protocol

**Decision**: Use gRPC for both client API and Raft transport.

| Factor                 | gRPC/HTTP/2                       | Custom Protocol             |
| ---------------------- | --------------------------------- | --------------------------- |
| Development velocity   | High (generated clients, tooling) | Low (manual implementation) |
| Operational complexity | Low (standard load balancers)     | High (custom tooling)       |
| Per-request overhead   | ~0.5ms                            | ~0.1-0.2ms                  |

~0.3ms overhead per request is negligible compared to consensus latency (~2-5ms). Development velocity wins.

### Raft vs. Byzantine Consensus

**Decision**: Raft (crash fault tolerant) over PBFT/Tendermint (Byzantine fault tolerant).

| Factor      | Raft               | Byzantine (PBFT)     |
| ----------- | ------------------ | -------------------- |
| Latency     | 1 RTT (3 messages) | 3 RTTs (9+ messages) |
| Throughput  | High               | Lower                |
| Fault model | Crash faults only  | Malicious nodes      |
| Complexity  | Moderate           | High                 |

We assume trusted operators. Byzantine tolerance would 3x latency for a threat model that doesn't apply.

### Per-Vault Chains vs. Single Chain

**Decision**: Each vault maintains its own blockchain.

**Advantages**:

- Fault isolation: corruption in Vault A doesn't affect Vault B
- Independent verification: auditors verify single vault
- Flexible deployment: different replication factors per vault

**Disadvantages**:

- No cross-vault transactions
- More complex routing

Isolation is more important than cross-vault atomicity for authorization workloads.

### Bucket-Based State Roots vs. Traditional Merkle Trees

**Decision**: 256 buckets with incremental hashing vs. binary Merkle tree.

| Aspect       | Binary Merkle Tree | Bucket-Based (Our Choice) |
| ------------ | ------------------ | ------------------------- |
| Update cost  | O(log n)           | O(k) for k modifications  |
| Proof size   | O(log n)           | O(256) = O(1) fixed       |
| Range proofs | Efficient          | Not supported             |

O(k) updates independent of database size. Larger proof size acceptable for audit scenarios.

### Single Leader vs. Multi-Leader

**Decision**: Single Raft leader per shard handles all writes.

Single leader is a bottleneck but simplifies consistency. Horizontal scaling via sharding.

### SHA-256 vs. Alternative Hash Functions

**Decision**: SHA-256 for all cryptographic commitments (state roots, block hashes, merkle proofs).

| Factor               | SHA-256                | BLAKE3      | SHA-3     |
| -------------------- | ---------------------- | ----------- | --------- |
| Performance          | ~500 MB/s              | ~6 GB/s     | ~300 MB/s |
| Hardware accel       | Widespread (SHA-NI)    | Limited     | Growing   |
| Standardization      | FIPS 180-4, ubiquitous | Not FIPS    | FIPS 202  |
| Tooling/verification | Excellent              | Growing     | Good      |
| Audit familiarity    | Universal              | Less common | Growing   |

SHA-256's universal recognition and hardware acceleration trump BLAKE3's raw speed. For authorization audits, auditors must be able to verify hashes with standard tools—SHA-256 is understood everywhere. Cryptographic operations are not the bottleneck (<5% of request latency).

### seahash vs. Alternative Non-Cryptographic Hashes

**Decision**: seahash for bucket assignment and internal indexing (non-security-critical paths).

| Factor        | seahash  | xxhash         | FNV-1a         | SipHash           |
| ------------- | -------- | -------------- | -------------- | ----------------- |
| Speed         | ~15 GB/s | ~30 GB/s       | ~5 GB/s        | ~2 GB/s           |
| Pure Rust     | Yes      | Requires C FFI | Yes            | Yes (std default) |
| Distribution  | Good     | Excellent      | Poor for short | Good              |
| DoS resistant | No       | No             | No             | Yes               |

seahash provides excellent speed in pure Rust without FFI complexity. For bucket assignment from already-authenticated data, DoS resistance is unnecessary—we're hashing internal keys, not untrusted input.

---

## Threat Model

### Trusted Operator Assumption

Ledger assumes a **trusted operator model**: the organization running the cluster controls all nodes and does not act maliciously. This is distinct from permissionless blockchains where nodes may be adversarial.

**What Ledger protects against**:

- **Crash failures**: Nodes may crash, lose power, or experience hardware failures. Raft tolerates (n-1)/2 simultaneous failures.
- **Network partitions**: Nodes may become temporarily unreachable. Raft maintains safety (no conflicting commits) and makes progress when majority is reachable.
- **Disk corruption**: State root verification detects corruption. Recovery via snapshot + log replay.
- **Accidental misconfiguration**: Sequence numbers and idempotency prevent duplicate operations.
- **Post-hoc tampering**: Cryptographic chain linking makes undetected modification computationally infeasible.

**What Ledger does NOT protect against**:

- **Malicious operator**: A compromised operator with access to majority of nodes can forge state. Raft is not Byzantine fault tolerant.
- **Compromised leader**: A Byzantine leader can propose invalid blocks. Followers verify state roots but cannot prevent a malicious majority from accepting invalid state.
- **Side-channel attacks**: Memory inspection, timing attacks on cryptographic operations are out of scope.

**Mitigation for untrusted environments**: Organizations requiring Byzantine fault tolerance should evaluate Tendermint-based systems or PBFT variants, accepting 3x latency overhead.

---

## Limitations

### Known Limits

| Dimension        | Recommended Max | Hard Limit | Bottleneck                    |
| ---------------- | --------------- | ---------- | ----------------------------- |
| Vaults per shard | 1,000           | 10,000     | Shard block size, memory      |
| Shard groups     | 10,000          | ~100,000   | Cluster coordination overhead |
| Total vaults     | 10M             | ~100M      | `_system` registry size       |
| Keys per vault   | 10M             | ~100M      | State tree memory             |
| Transactions/sec | 5,000 per shard | ~50,000    | Raft throughput               |

### Not Supported

- **Cross-vault transactions** — By design, for isolation
- **Byzantine fault tolerance** — Trusted network assumption
- **Sub-millisecond writes** — Consensus overhead
- **Infinite retention** — Storage costs require policy
- **Instant per-key proofs** — Hybrid storage trade-off
- **Range proofs** — Bucket-based state root limitation

---

## Open Questions

### Unresolved Design Decisions

1. **Hot key contention mitigation**: Current recommendation is application-level sharding. Should Ledger provide built-in mechanisms?

2. **Automatic shard rebalancing**: Currently manual. What triggers and safety checks for automatic rebalancing?

3. **Cross-shard read coordination**: For dashboards aggregating multiple namespaces, what consistency model?

4. **Proof aggregation for bulk verification**: How to efficiently verify thousands of keys without individual proofs?

### Future Considerations

1. **Zero-knowledge proofs**: Could ZK-SNARKs enable private verification without revealing data?

2. **Tiered storage**: Hot data in memory, warm on SSD, cold in object storage?

---

## Success Criteria

### Performance Targets

| Metric              | Target          | Measured        | Measurement Condition       |
| ------------------- | --------------- | --------------- | --------------------------- |
| Read latency (p99)  | < 2 ms          | **2.8 µs**      | Single key lookup           |
| Write latency (p99) | < 50 ms         | **8.1 ms**      | Batch committed             |
| Write throughput    | 5,000 tx/sec    | **11K tx/sec**  | 3-node cluster              |
| Read throughput     | 100,000 req/sec | **952K req/sec**| Eventually consistent reads |

Benchmarks run on Apple M3 (8-core), 24GB RAM, APFS SSD. See [WHITEPAPER.md](WHITEPAPER.md#6-performance-characteristics) for full methodology and latency distributions.

### Correctness Invariants

1. **Commitment permanence**: Committed blocks cannot be removed or modified
2. **State determinism**: Replaying identical transactions produces identical state_root
3. **Chain continuity**: Every block (except genesis) links to predecessor
4. **Quorum requirement**: Commits require 2f+1 acknowledgment

### Operational Requirements

1. **Recovery time**: Node recovers from crash within 5 minutes
2. **Failover time**: New leader elected within 10 seconds
3. **Zero data loss**: No committed transaction lost on minority failure

---

## System Invariants

### Safety Invariants

1. **Commitment permanence**: Committed blocks cannot be removed or modified
2. **State determinism**: Replaying identical transactions produces identical state_root
3. **Chain continuity**: Every block (except genesis) links to its predecessor via previous_hash
4. **Quorum requirement**: Commits require 2f+1 node acknowledgment

### Liveness Invariants

5. **Progress under minority failure**: Fewer than f+1 failures → writes continue
6. **Eventual convergence**: After partition heals, all replicas converge to same state
7. **Leader availability**: New leader elected within election timeout after leader failure

### Integrity Invariants

8. **Merkle soundness**: Invalid values cannot produce valid proofs
9. **Hash chain integrity**: Modifying any block invalidates all subsequent blocks
10. **Transaction atomicity**: All operations in a transaction apply together or not at all

### Operational Invariants

11. **Snapshot consistency**: Loading snapshot + applying subsequent blocks produces current state_root
12. **Historical accessibility**: Any committed block is readable with verifiable proofs
13. **Replication durability**: After Raft commit, data exists on quorum

### Idempotency Invariants

14. **Sequence monotonicity**: Per-client sequences are strictly increasing
15. **Duplicate rejection**: Sequence ≤ last_committed_seq never creates new transaction
16. **Sequence persistence**: last_committed_seq survives leader failover

---

## Appendices

### A. Cryptographic Hash Specifications

This appendix provides normative, language-independent specifications for all cryptographic hashes.

**Hash algorithm**: SHA-256 for all cryptographic commitments (32 bytes).

#### Block Hash

```
block_hash = SHA-256(
    height              || # u64, big-endian (8 bytes)
    namespace_id        || # i64, big-endian (8 bytes)
    vault_id            || # i64, big-endian (8 bytes)
    previous_hash       || # 32 bytes (zero-hash for genesis)
    tx_merkle_root      || # 32 bytes
    state_root          || # 32 bytes
    timestamp_secs      || # i64, big-endian (8 bytes)
    timestamp_nanos     || # u32, big-endian (4 bytes)
    term                || # u64, big-endian (8 bytes)
    committed_index        # u64, big-endian (8 bytes)
)
# Total: 148 bytes fixed-size input
```

#### Transaction Hash

```
tx_hash = SHA-256(
    tx_id               || # 16 bytes (UUID)
    client_id_len       || # u32, little-endian
    client_id           || # UTF-8 bytes
    sequence            || # u64, big-endian
    actor_len           || # u32, little-endian
    actor               || # UTF-8 bytes
    op_count            || # u32, little-endian
    operations          || # See operation encoding below
    timestamp_secs      || # i64, big-endian
    timestamp_nanos        # u32, big-endian
)
```

#### Operation Encoding

```
op_type (1 byte):
  0x01 = CreateRelationship
  0x02 = DeleteRelationship
  0x03 = SetEntity
  0x04 = DeleteEntity
  0x05 = ExpireEntity

CreateRelationship / DeleteRelationship:
  resource_len (u32 LE) || resource ||
  relation_len (u32 LE) || relation ||
  subject_len (u32 LE) || subject

SetEntity:
  key_len (u32 LE) || key ||
  value_len (u32 LE) || value ||
  condition_type (u8) || condition_data ||
  expires_at (u64 BE, 0 = never)

DeleteEntity:
  key_len (u32 LE) || key

ExpireEntity:
  key_len (u32 LE) || key || expired_at (u64 BE)
```

#### Condition Types

```
0x00 = None
0x01 = MustNotExist
0x02 = MustExist
0x03 = VersionEquals (+ u64 BE version)
0x04 = ValueEquals (+ u32 LE length + bytes)
```

#### Transaction Merkle Tree

Standard binary Merkle tree:

1. Leaves are transaction hashes in block order
2. Odd nodes: duplicate last node
3. Parent = SHA-256(left_child || right_child)
4. Empty block: tx_merkle_root = SHA-256("")

#### State Tree Leaf Hash

```
leaf_contribution = (
    key_len (u32 LE)    ||
    key                 ||
    value_len (u32 LE)  ||
    value               ||
    expires_at (u64 BE) ||  # 0 = never
    version (u64 BE)        # block height of last modification
)
```

### B. Snapshot Format

```rust
struct Snapshot {
    vault_id: VaultId,
    block_height: u64,
    block_hash: Hash,
    state_root: Hash,
    state_data: CompressedStateTree,
    created_at: DateTime<Utc>,

    // Chain verification linkage
    genesis_hash: Hash,
    previous_snapshot_height: Option<u64>,
    previous_snapshot_hash: Option<Hash>,
    chain_commitment: ChainCommitment,
}
```

### C. Testing Strategy

#### Unit Tests

- State root computation determinism
- Operation idempotency
- Key encoding/decoding roundtrip
- Bucket hash consistency

#### Integration Tests

- Write path: client → leader → commit → verify
- Read path: linearizable vs. eventually consistent
- Failover: leader dies, new leader elected, no data loss
- Recovery: node crashes, restarts, catches up

#### Chaos Tests

- Network partition simulation
- Random node kills
- Clock skew injection
- Disk corruption injection

---

## References

1. [Zanzibar: Google's Consistent, Global Authorization System](https://research.google/pubs/pub48190/) — USENIX ATC 2019
2. [Raft: In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) — USENIX ATC 2014
3. [OpenRaft](https://github.com/datafuselabs/openraft) — Rust Raft implementation
4. [QMDB: Quick Merkle Database](https://github.com/LayerZero-Labs/qmdb) — Append-only log with O(1) merkleization
5. [SeiDB](https://docs.sei.io/learn/seidb) — Separates state commitment from storage
6. [OWASP Top 10](https://owasp.org/Top10/) — Web application security risks
