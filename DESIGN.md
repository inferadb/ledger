# InferaDB Ledger Design Document

| Field   | Value                                                   |
| ------- | ------------------------------------------------------- |
| Status  | Living                                                  |
| Created | 2026-01-09                                              |
| Updated | 2026-04-15                                              |
| Source  | [DESIGN.md](DESIGN.md) (canonical, checked into `main`) |

---

## How to Read This Document

This document serves multiple audiences. Use these paths to find what matters to you:

- **System architects**: [Problem Statement](#problem-statement) → [Solution Overview](#solution-overview) → [Trade-offs Analysis](#trade-offs-analysis) → [Limitations](#limitations)
- **Implementers**: [Architecture](#architecture) → [Write Path](#write-path) / [Read Path](#read-path) → [Key Encoding](#key-encoding) → [Appendices A–D](#appendices)
- **Operators**: [Health Checks](#health-checks) → [Degraded Operation Modes](#degraded-operation-modes) → [Backup and Restore](#fault-tolerance--recovery) → [Encryption at Rest](#encryption-at-rest) → [Data Residency](#data-residency)
- **Security / Compliance**: [Threat Model](#threat-model) → [System Invariants](#system-invariants) → [Encryption at Rest](#encryption-at-rest) → [Right to Erasure](#right-to-erasure-crypto-shredding) → [Appendix A](#a-cryptographic-hash-specifications)

---

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

```text
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
│            Raft Consensus (Purpose-Built Multi-Shard Engine)         │
│     Event-driven Reactor · per-vault segmented WAL · leader lease    │
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

```text
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

```text
┌─────────────────────────────────────────────────────────────────────┐
│                      Region Group (Raft)                             │
│           Nodes A, B, C sharing consensus for us-east-va            │
├─────────────────────────────┬───────────────────────────────────────┤
│   Organization: org_acme    │      Organization: org_startup        │
│  ┌─────────┐  ┌─────────┐   │       ┌─────────┐                     │
│  │ Vault:  │  │ Vault:  │   │       │ Vault:  │                     │
│  │  prod   │  │ staging │   │       │  main   │                     │
│  │ Chain── │  │ Chain── │   │       │ Chain── │                     │
│  └─────────┘  └─────────┘   │       └─────────┘                     │
│      Entities               │           Entities                    │
└─────────────────────────────┴───────────────────────────────────────┘
```

| Level        | Isolation                       | Shared                  |
| ------------ | ------------------------------- | ----------------------- |
| Organization | Entities, vaults, keys          | Region (Raft consensus) |
| Vault        | Cryptographic chain, state_root | Organization storage    |
| Region       | Physical nodes, encryption key  | Cluster                 |

Each vault maintains its own blockchain. A corruption in Vault A cannot affect Vault B's chain integrity.

---

## Architecture

### Terminology

| Term                        | Definition                                                                                                                                                                                                                                                                                                      |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Organization**            | Storage unit per customer/org. Contains entities and vaults. Isolated with separate storage.                                                                                                                                                                                                                    |
| **Vault**                   | Relationship store within an organization. Maintains its own cryptographic chain (state_root, previous_hash, block height).                                                                                                                                                                                     |
| **Entity**                  | Key-value data stored in an organization (users, teams, clients, sessions). Supports TTL, versioning, conditional writes.                                                                                                                                                                                       |
| **Relationship**            | Authorization tuple: `(resource, relation, subject)`. Used by Engine for permission checks.                                                                                                                                                                                                                     |
| **Region**                  | Geographic zone mapped 1:1 to a Raft consensus group. Organizations declare a region at creation for data residency. See [Scaling Architecture: Regions](#scaling-architecture-regions).                                                                                                                        |
| **`_system` organization**  | Global control plane replicated to all nodes: org registry, sequences, node discovery. See [Discovery & Coordination](#system-organization-_system--global-control-plane).                                                                                                                                      |
| **Consensus engine**        | Purpose-built multi-shard Raft implementation in `crates/consensus/` — event-driven `Reactor`, per-vault segmented WAL, `LeaderLease`, `ClosedTimestampTracker`, `StateMachine` trait. Determinism-first: the Reactor returns `Action`s rather than performing I/O, which makes the engine simulation-testable. |
| **Raft operationalization** | Wrapping layer in `crates/raft/` — custom `RaftLogStore` (combined log + state machine, implemented against the in-house consensus engine), `ApplyPool` / `ApplyWorker` for parallel state-machine apply across shards, `SagaOrchestrator`, background jobs (compaction, retention, events GC), rate limiting, hot-key detection, `LeaderTransfer`, `RaftManager` for region groups. |

### Block Structure

Each vault maintains its own logical blockchain within an organization:

```rust
struct VaultBlock {
    header: BlockHeader,
    transactions: Vec<Transaction>,
}

struct BlockHeader {
    // Identity
    height: u64,
    organization: OrganizationId,
    vault: VaultId,

    // Chain linking (immutability)
    previous_hash: Hash,           // SHA-256 of previous vault block

    // Content summary
    tx_merkle_root: Hash,          // Merkle root of this block's transactions

    // State commitment (verification)
    state_root: Hash,              // Vault state AFTER applying this block

    // Metadata
    timestamp: DateTime<Utc>,
    term: u64,
    committed_index: u64,
}

struct Transaction {
    id: TxId,
    client_id: ClientId,
    sequence: u64,                 // Server-assigned, monotonic per-client-vault
    actor: String,                 // Audit logging
    operations: Vec<Operation>,
    timestamp: DateTime<Utc>,
}

enum Operation {
    // Relationship operations (Engine)
    CreateRelationship { resource: String, relation: String, subject: String },
    DeleteRelationship { resource: String, relation: String, subject: String },

    // Entity operations (Control)
    SetEntity { key: String, value: Vec<u8>, condition: Option<SetCondition>, expires_at: Option<u64> },
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

```text
Client Request (with idempotency_key)
    │
    ▼
[1] Any node receives request (API version validated via interceptor)
    │
    ▼
[2] Forward to Raft leader (if not already leader)
    │
    ▼
[3] Check gRPC deadline — reject if < 100ms remaining
    │
    ▼
[4] Validate input (key/value sizes, character whitelists, batch limits)
    │
    ▼
[5] Leader checks idempotency cache (return cached result on hit)
    │
    ▼
[6] Rate limit check (3-tier: backpressure, organization, client)
    │
    ▼
[7] Record hot key accesses (Count-Min Sketch)
    │
    ▼
[8] Batcher aggregates (up to 500 txs or batch_timeout)
    │
    ▼
[9] Batch becomes Raft proposal (with deadline-aware timeout)
    │
    ▼
[10] Leader replicates to followers
    │
    ▼
[11] Majority acknowledgment commits
    │
    ▼
[12] ApplyPool dispatches committed batch to per-shard ApplyWorker
    │
    ▼
[13] Worker applies transactions, assigns sequences, computes state_root,
     updates AppliedState + client idempotency tracking
    │
    ▼
[14] Response to client with assigned_sequence + state_root hash
     (includes x-request-id, x-trace-id, x-ledger-api-version headers)
```

**Target latency**: <50ms at p99 under normal load.

**Durability**: The response at step [14] is returned only after successful quorum commit (step [11]). `Reactor` batches WAL fsyncs across proposals — one barrier fsync per batch via `crates/fs-sync/src/lib.rs`, not per proposal — so write latency scales with batch cadence rather than per-operation disk work. See [Durability & Finality](#durability--finality) below for the complete contract and [docs/operations/durability.md](docs/operations/durability.md) for the operator-facing recovery matrix.

**Consensus layering**: Steps 10–11 run inside `inferadb-ledger-consensus` (event-driven `Reactor` returns `Action`s: WAL fsync, peer sends, commit). Steps 12–13 run in `inferadb-ledger-raft`'s `ApplyPool` — one `ApplyWorker` per shard parallelizes state-machine application across shards while keeping each shard's apply deterministic.

### Read Path

```text
Client Request
    │
    ▼
[1] Any node receives request
    │
    ├── Linearizable read:
    │     │
    │     ├── Leader: serve directly if LeaderLease is valid (no RPC)
    │     │             (lease from last majority-ack heartbeat)
    │     │
    │     └── Follower: issue ReadIndex RPC to leader, wait for
    │                   local apply index to reach returned index,
    │                   then serve from local state
    │
    ├── Bounded-staleness (closed-timestamp reads):
    │     serve from local state at or below the closed timestamp
    │     (ClosedTimestampTracker)
    │
    └── Eventually consistent: serve from local state immediately
    │
    ▼
[2] Storage layer retrieves from B+ tree indexes
    │
    ▼
[3] Response includes current state_root
```

**Target latency**: <2ms at p99.

**Why three read modes**: `LeaderLease` avoids round-trips on the leader by treating the lease window as proof the leader is still authoritative — a round-trip savings that dominates the <2ms target. `ReadIndex` gives followers linearizability without serving stale data. `ClosedTimestampTracker` lets followers serve reads at bounded staleness (useful for high-QPS read workloads that can tolerate milliseconds of lag).

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
// Simplified conceptual model — actual implementation details below
struct StateLayer<B: StorageBackend> {
    db: Arc<Database<B>>,                                // All tables stored within
    vault_commitments: RwLock<HashMap<VaultId, VaultCommitment>>,
}

// The Database contains 19 compile-time tables including:
//   Entities, Relationships, ObjIndex, SubjIndex,
//   Blocks, VaultBlockIndex, VaultMeta, OrganizationMeta,
//   Sequences, ClientSequences, CompactionMeta,
//   RaftLog, RaftState, OrganizationSlugIndex, VaultSlugIndex,
//   UserSlugIndex, VaultHeights, VaultHashes, VaultHealth

impl StateLayer {
    fn get(&self, key: &[u8]) -> Option<Value> {
        self.db.get(key)  // O(1) lookup, no merkle overhead
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

> **Note:** Indexes (`ObjIndex`, `SubjIndex`) are tables within the `Database`, not fields on `StateLayer`. The `state_root` is computed per-vault at block boundaries and tracked in `VaultCommitment`, not stored as a `StateLayer` field. `TableIterator` uses a streaming resume-key pattern with a `VecDeque` buffer (default 1000 entries) rather than pre-loading all entries.

### Key Encoding

Storage keys encode vault isolation and bucket assignment:

```text
┌─────────────────────────────────────────────────────────────────┐
│  vault_id   │  bucket_id  │         local_key                   │
│  (8 bytes)  │  (1 byte)   │         (N bytes)                   │
└─────────────────────────────────────────────────────────────────┘
```

- `seahash` for bucket assignment (fast, deterministic)
- SHA-256 for cryptographic hashes (state_root, bucket_roots)

### ID Generation

Ledger uses **leader-assigned sequential IDs** for deterministic replay:

| ID Type            | Rust Type              | Size     | Generated By       | Strategy                               |
| ------------------ | ---------------------- | -------- | ------------------ | -------------------------------------- |
| `OrganizationId`   | newtype(`i64`)         | int64    | Ledger Leader      | Sequential (internal only)             |
| `OrganizationSlug` | newtype(`u64`)         | uint64   | Client (Snowflake) | External identifier                    |
| `VaultId`          | newtype(`i64`)         | int64    | Ledger Leader      | Sequential (internal only)             |
| `VaultSlug`        | newtype(`u64`)         | uint64   | Snowflake          | External identifier for vaults         |
| `UserId`           | newtype(`i64`)         | int64    | Ledger Leader      | Sequential                             |
| `TxId`             | type alias(`[u8; 16]`) | 16 bytes | Ledger Leader      | UUID assigned in response construction |
| `NodeId`           | newtype(`String`)      | string   | Admin              | Configuration                          |
| `ClientId`         | newtype(`String`)      | string   | Client             | Client-provided                        |

> **Region**: `Region` is a compile-time enum (25 variants: `GLOBAL` + 24 geographic), not a sequential ID. Organizations declare a `Region` for data residency. Each `Region` maps 1:1 to a Raft consensus group.

Newtypes are generated by the `define_id!` macro with `Display` formatting (e.g., `org:42`, `vault:7`), `Serialize`/`Deserialize` with `#[serde(transparent)]`, and `From`/`Into` conversions. `TxId` remains an unwrapped type alias for compatibility with its usage pattern. `NodeId` and `ClientId` are newtypes generated by the `define_string_id!` macro.

**Dual-ID architecture:** Organizations and vaults use two identifiers — a sequential internal ID (`OrganizationId`/`VaultId`, `i64`) for storage key density and B+ tree performance, and an external Snowflake slug (`OrganizationSlug`/`VaultSlug`, `u64`) as the sole identifier exposed in gRPC APIs and the SDK. Internal IDs are never visible to API consumers. The `SlugResolver` at the gRPC service boundary translates slugs to internal IDs for storage operations.

The leader assigns sequential IDs during block construction. Followers replay the same IDs from the Raft log—fully deterministic. `TxId` UUIDs are generated in the response path after Raft commitment.

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

```text
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

- Maximum batch size: 500 transactions
- Maximum batch delay: 10ms (`batch_timeout`)
- Tick interval: 5ms (runtime `BatchWriter` polling interval, derived from `batch_timeout / 2` clamped to `[1ms, 10ms]`)

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
| Leader network isolated       | ✓ After election   | ✓ Available       | New leader elected (~300-600ms)     |
| Leader planned shutdown       | ✓ Near-instant     | ✓ Available       | Leadership transferred, then exit   |
| State root divergence (vault) | ✗ Vault halted     | ✓ Available       | Rebuild vault from snapshot         |
| Disk full                     | ✗ Unavailable      | ✓ Available       | Expand storage, compact logs        |

**Partial availability**: When writes are unavailable, Ledger continues serving eventually consistent reads from any healthy replica. Applications can implement read-only degraded modes.

**Vault-level isolation**: A diverged vault does not affect other vaults in the same organization. Only the affected vault halts writes pending recovery.

**Automatic recovery scope**: Ledger automatically recovers from transient failures (network blips, brief partitions). Persistent failures (disk corruption, state divergence) require operator intervention to prevent data loss.

**Backup and restore**: Operators can create on-demand backups via `CreateBackup` RPC, list available backups via `ListBackups`, and restore via `RestoreBackup` (with explicit confirmation). Automated periodic backups are available via `BackupConfig`. See `crates/raft/src/backup.rs`.

**Graceful shutdown**: A multi-phase shutdown sequence ensures in-flight requests complete and leadership transfers cleanly: mark Draining (reject new proposals) → leader transfer (best-effort) → mark ShuttingDown → pre-stop delay (for load balancer updates) → grace period → connection drain (via `ConnectionTracker`) → pre-shutdown tasks with timeout (Raft snapshot) → server stop. Configurable via `ShutdownConfig`.

---

## Scaling Architecture: Regions

Each Raft consensus group maps 1:1 to a geographic **region**. Organizations declare a region for data residency. The `Region` enum is exhaustive — adding a region is a code change, not a runtime operation.

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                               Cluster                                     │
├──────────────────────────────────────────────────────────────────────────┤
│  GLOBAL               │  us-east-va           │  ie-east-dublin           │
│  ┌─────────────────┐  │  ┌─────────────────┐  │  ┌─────────────────┐     │
│  │ ALL Nodes       │  │  │ ALL Nodes       │  │  │ Nodes D, E, F   │     │
│  │ Raft Group      │  │  │ Raft Group      │  │  │ (IE only)       │     │
│  │ (_system ctrl)  │  │  │ (org data)      │  │  │ Raft Group      │     │
│  └─────────────────┘  │  └─────────────────┘  │  └─────────────────┘     │
│  Org registry,        │  Orgs assigned to     │  Orgs assigned to        │
│  sequences, discovery │  us-east-va           │  ie-east-dublin (GDPR)   │
└──────────────────────────────────────────────────────────────────────────┘
```

**Region types**:

- **`GLOBAL`**: Control plane. All nodes join. Stores non-PII metadata (org registry, sequences, node discovery). No organization data.
- **Non-protected** (`requires_residency() == false`): `US_EAST_VA`, `US_WEST_OR`. All nodes join regardless of their own region tag — maximizes redundancy for jurisdictions without data residency laws.
- **Protected** (`requires_residency() == true`): `IE_EAST_DUBLIN`, `DE_CENTRAL_FRANKFURT`, `CN_NORTH_BEIJING`, etc. Only nodes tagged with that exact region join. Data replicated exclusively to in-region nodes.

**Region assignment**: Organization-to-region mapping stored in `_system` organization. Region is an explicit admin/user decision (not load-balanced). Every organization must declare a region at creation.

**Cross-region consistency**: No cross-region transactions. Application-level saga pattern required for multi-organization operations. Cross-region request forwarding routes writes to the correct region's Raft leader.

---

## Discovery & Coordination

### System Organization (`_system`) — Global Control Plane

The `_system` organization (OrganizationId = 0) is the **global control plane**, replicated to all nodes via the `GLOBAL` Raft group. It stores non-PII metadata only:

```text
_system organization (GLOBAL Raft group — all nodes)
├── Organization registry (organization → region mapping)
├── Node discovery (node_id, addresses, region tag)
├── Sequence counters (organization ID, vault ID generation)
├── Saga state (cross-region orchestration)
└── Cluster configuration
```

User PII lives in **regional user stores**, not `_system`:

```text
Regional user store (per-region Raft group — membership per region type)
├── User profiles (user:{user_id})
├── Email mappings (user_email:{blinded_email_hash})
├── Email verification tokens (email_verify:{token_hash})
├── Organization entities and relationships
└── Vault data
```

### Client Discovery Flow

```text
[1] Client connects to any node
[2] Sends request with organization_slug
[3] Node looks up organization → region mapping in local _system replica
[4] If this node participates in the target region:
      Forward to that region's Raft leader
[5] If this node does NOT participate (protected region):
      Return redirect with region leader address
```

### Node Join

```text
[1] New node contacts seed node
[2] Seed node adds new node as Raft learner
[3] Learner replicates log (may take time for large state)
[4] Once caught up, promoted to voter via joint consensus
[5] Cluster now has N+1 voting members
```

### Node Leave (Graceful)

```text
[1] Operator initiates removal (SIGTERM or API call)
[2] Enter Draining phase — reject new write proposals (UNAVAILABLE)
[3] If leader: transfer leadership to most caught-up follower (see Leader Transfer)
[4] Mark ShuttingDown — pre-stop delay → connection drain → final snapshot
[5] Remove from Raft group via joint consensus
[6] Node shuts down
```

### Leader Transfer

Graceful leader transfer eliminates election timeout delays during planned shutdowns. The departing leader hands off to a caught-up follower via a triggered election, reducing write unavailability from 300-600ms (election timeout) to one round-trip.

**Protocol** (5 steps, implemented in `leader_transfer.rs`):

```text
[1] Verify this node is the current leader
[2] Select target: explicit node or auto-pick most caught-up follower
[3] Wait for replication — target's match_index reaches leader's last_log_id
[4] Send TriggerElection internal RPC to target
[5] Poll until target wins election or timeout expires
```

**Concurrency**: An `AtomicBool` transfer lock ensures only one transfer runs at a time. Concurrent requests receive `ABORTED`.

**Phase transitions during shutdown**:

```text
Ready ──→ Draining ──→ ShuttingDown
           │              │
           │ reject writes │ drain connections
           │ transfer lead │ final snapshot
           └──────────────→└──→ exit
```

During `Draining`, all mutating RPCs (write, batch_write, create/delete org/vault, join/leave cluster, etc.) return `UNAVAILABLE`. The `TransferLeadership` RPC itself is exempt — it is the mechanism enabling the drain. Reads continue uninterrupted.

**Best-effort**: Transfer failure (timeout, no eligible target, connection error) logs a warning and falls back to standard shutdown. The cluster recovers via election timeout.

**RPCs**:

- `AdminService/TransferLeadership` — client-facing, accepts `target_node_id` (0 = auto) and `timeout_ms` (default 10s, max 60s)
- `RaftService/TriggerElection` — internal, called by leader on the target node

**Configuration**: `ShutdownConfig.leader_transfer_timeout_secs` (default 10, set to 0 to disable).

---

## Durability & Finality

### Finality Model

| Event                | Durability Guarantee                           | Enforced in                                                                                                                                                                                                                            |
| -------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Write acknowledged   | Persisted on majority (2f+1) nodes             | `Shard::handle_append_response` emits `Commit` only once quorum is reached (`crates/consensus/src/shard.rs`); `Reactor` issues a barrier fsync per batch (`crates/consensus/src/reactor.rs` via `crates/fs-sync/src/lib.rs`). Verified by `crates/consensus/src/simulation/multi_raft.rs`. |
| Block committed      | Cannot be reverted without majority corruption | No `Action` variant removes a committed entry; safety is exercised by the deterministic simulation harness in `crates/consensus/src/simulation/`.                                                                                      |
| State root published | Independently verifiable by any client         | `BlockHeader.state_root` returned on write responses and via the `GetTip` / `VerifiedRead` / `GetBlock` RPCs; verification primitives in `crates/types/src/hash.rs` and `crates/types/src/merkle.rs`.                                  |

**No rollbacks**: Once committed, blocks are permanent. Corrections require compensating transactions.

### Crash Recovery

```text
[1] Node restarts, loads last snapshot
[2] Replays Raft log from snapshot index
[3] Verifies state_root matches expected
[4] Rejoins cluster as follower
[5] Catches up with leader if behind
```

---

## Persistent Storage

### Directory Layout

Each region's Raft group gets isolated database files. This eliminates write lock contention between concurrent Raft group applies, enables per-region encryption, and simplifies migration, snapshots, and disk accounting.

```text
{data_dir}/
├── node_id                          # Node identity file
├── global/                          # GLOBAL Raft group (_system control plane)
│   ├── state.db                     # Org registry, sequences, sagas, node info (no PII)
│   ├── blocks.db                    # Global blockchain
│   ├── raft.db                      # Raft log for GLOBAL group
│   └── events.db                    # Audit events for GLOBAL group
├── regions/                         # Per-region data (orgs + user PII)
│   ├── us_east_va/
│   │   ├── state.db                 # Entities, relationships, indexes + user PII tables
│   │   ├── blocks.db
│   │   ├── raft.db
│   │   └── events.db
│   └── .../                         # One directory per region the node participates in
├── snapshots/                       # Per-region snapshot directories
│   ├── global/
│   ├── us_east_va/
│   └── .../
└── keys/                            # Per-region RMK storage (file-based key source)
    ├── global/
    ├── us_east_va/
    └── .../
```

Each region's databases use embedded single-file ACID B-trees with copy-on-write pages. A node only has directories for regions it participates in: all non-protected regions plus its own protected region (if any). User PII shares `state.db` with organization data within each region, isolated by B-tree key prefixes.

### Storage Backend

`inferadb-ledger-store` provides:

- B+ tree engine with page-level management (23 compile-time tables: domain data, block storage, Raft log, metadata, slug indexes for org/vault/user/team/app, vault height/hash/health maps, and the string-interning dictionary)
- ACID transactions with copy-on-write pages
- Configurable backends (file-based, memory for testing)
- Streaming `TableIterator` with resume-key pattern for memory-efficient traversal
- Background compaction via `BTreeCompactor` (leaf merging to reclaim dead space)

---

## Control Data Storage

The Control service uses Ledger for organization and user management:

### Entity Key Patterns

```text
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

## JWT Token Architecture

Ledger is the JWT source of truth for the InferaDB platform. All token signing, validation, and revocation flows through Ledger's Raft consensus layer, providing cryptographic auditability and strong consistency for authentication state.

### Token Types

| Type         | Audience           | Purpose                  | Access TTL | Refresh TTL |
| ------------ | ------------------ | ------------------------ | ---------- | ----------- |
| User Session | `inferadb-control` | Control web/API sessions | 30 min     | 14 days     |
| Vault Access | `inferadb-engine`  | Engine data operations   | 15 min     | 1 hour      |

All tokens use **EdDSA (Ed25519)** signatures. Algorithm enforcement is defense-in-depth: raw JWT header pre-check rejects non-EdDSA `alg` values before any cryptographic operation, and the `jsonwebtoken` library validation is restricted to `[EdDSA]`.

### Signing Key Envelope Encryption

Signing key private material is envelope-encrypted using the same RMK infrastructure as page encryption, adapted for a fixed 32-byte Ed25519 private key:

```text
SigningKeyEnvelope (100 bytes fixed layout):
┌──────────────┬───────────┬──────────────┬──────────────┐
│ wrapped_dek  │   nonce   │  ciphertext  │  auth_tag    │
│   (40 B)     │  (12 B)   │   (32 B)     │   (16 B)     │
└──────────────┴───────────┴──────────────┴──────────────┘
```

1. **DEK generation**: Random 256-bit key
2. **DEK wrapping**: AES-KWP with Region Master Key → 40-byte wrapped DEK
3. **Private key encryption**: AES-256-GCM with random nonce, `kid` as AAD
4. **AAD binding**: The `kid` (UUID) serves as Additional Authenticated Data, preventing envelope transplant attacks (swapping envelopes between keys)

On key load, the JwtEngine unwraps the DEK via RMK, decrypts the private key, builds the signing credential, and zeroizes plaintext key bytes via `Zeroizing<Vec<u8>>`.

**Trade-off**: Raft log entries contain wrapped signing key material (envelope-encrypted, not plaintext). This is mitigated by log compaction and the fact that extracting the private key requires the RMK. On RMK rotation, the DEK re-wrap job updates signing key envelopes.

### JwtEngine Cache Design

The `JwtEngine` uses `ArcSwap<HashMap<String, CachedKey>>` for lock-free reads on every validation:

```text
JwtEngine
├── config: JwtConfig
└── cache: ArcSwap<HashMap<kid → CachedSigningKey>>
    └── CachedSigningKey
        ├── metadata: SigningKey       (kid, scope, status, valid_from, valid_until, etc.)
        ├── decoding_key: DecodingKey  (public key, safe to cache)
        └── private_key_bytes: Zeroizing<Vec<u8>>  (zeroized on drop)
```

**Copy-on-write updates**: `load_key()` clones the current map, inserts the new entry, and atomically swaps via `ArcSwap::store()`. Concurrent readers see either the old or new map — never a partial state.

**Zeroization policy**:

- `EncodingKey` (signing credential with private key) is **not cached** — built on-demand per sign operation, then dropped
- `DecodingKey` (public key only) is cached — no secret material
- Private key bytes are wrapped in `Zeroizing<Vec<u8>>` during decrypt-to-sign flow

### Refresh Token Family Theft Detection

Refresh tokens implement **rotate-on-use** with family-based theft detection:

```text
Family A (created at login):
  RT-1 ──use──▶ RT-2 ──use──▶ RT-3 (current)
                  │
                  └── RT-1 reused (theft!) ──▶ Family A poisoned
```

Each refresh token belongs to a **family** (random 16-byte ID assigned at initial token creation). The state machine tracks:

1. **`used_at` timestamp**: Set atomically when a refresh token is consumed
2. **`poisoned` flag**: Set on the family when reuse is detected

**Reuse detection flow** (state machine authority):

1. `RefreshToken` RPC arrives with token hash
2. State machine looks up token → checks `used_at`
3. If `used_at` is set → token already consumed → **poison the family** (O(1) flag set)
4. All future operations against this family return `RefreshTokenReuse`
5. Background `TokenMaintenanceJob` GCs poisoned families

**Why state machine authority matters**: The Raft state machine is the single arbiter of token state. Two concurrent refresh attempts with the same token are serialized by Raft — one succeeds, the other detects reuse. No distributed locking needed.

### Cascade Revocation

Entity lifecycle changes trigger cascade token revocation through the Raft state machine:

| Event                        | Revocation Scope                      | Mechanism                                           |
| ---------------------------- | ------------------------------------- | --------------------------------------------------- |
| App disabled                 | All tokens for app (all vaults)       | Inline cascade in `SetAppEnabled` apply handler     |
| App-vault connection removed | Tokens for app+vault pair             | Inline cascade in `RemoveAppVault` apply handler    |
| Organization deleted         | All signing keys + all refresh tokens | `delete_org_signing_keys` + subject index scan      |
| Password change              | All user sessions                     | `RevokeAllUserSessions` (increments `TokenVersion`) |

**TokenVersion for immediate user invalidation**: Each user has an atomic `TokenVersion` counter. `RevokeAllUserSessions` increments it. `ValidateToken` compares the token's `version` claim against current state — stale versions are rejected without waiting for token expiry.

### Scope Policy

Vault tokens carry `scopes: Vec<String>` (e.g., `["read", "write"]`). On token creation, requested scopes must be a subset of the app-vault connection's `allowed_scopes`.

**Refresh uses current policy**: When a vault token is refreshed, scopes are read from the current `allowed_scopes` on the app-vault connection — not intersected with the original grant. This means:

- Scopes reduced on connection → new token has reduced scopes
- Scopes expanded on connection → new token gains expanded scopes
- Connection removed → refresh fails with error
- App disabled → refresh rejected

This "current policy is authoritative" design avoids stale scope grants and ensures policy changes take effect at the next refresh boundary.

### Security Boundaries

**Network trust**: Ledger runs behind WireGuard VPN. Token RPCs do not require `authentication_proof` — upstream services (Control, Engine) authenticate callers. Planned: mTLS between services as defense-in-depth.

**Validation consistency**: `ValidateToken` reads local Raft state (no leader forwarding). After `RevokeAllUserSessions`, followers may briefly validate revoked tokens until replication catches up (milliseconds). For security-critical operations, forward to leader or accept the brief window.

**kid validation**: UUID format check before state lookup prevents cache pollution from arbitrary strings. Unknown and revoked kids return identical `UNAUTHENTICATED` (no key existence leakage).

**Timing side channels**: Refresh token hash comparison uses the constant-time `hash_eq()` helper in `crates/types/src/hash.rs` (built on `subtle::ConstantTimeEq`). The same helper gates recovery-code consumption, email-verification-code checks, and state-root verification. No hash equality check on a security-sensitive path uses variable-time comparison.

---

## Client Idempotency & Retry Semantics

Clients include `(client_id, idempotency_key)` with each request:

```rust
struct WriteRequest {
    client_id: String,          // Identifies client
    idempotency_key: [u8; 16],  // UUIDv4, unique per write attempt
    operations: Vec<Operation>,
}
```

The server assigns a monotonically increasing `sequence` per `(client_id, vault)` at Raft apply time and returns it as `assigned_sequence` in the response. This eliminates race conditions from concurrent client writes — the server determines ordering.

**Idempotency guarantee**: Same `(client_id, idempotency_key)` returns cached response with the original `assigned_sequence`, no re-execution.

**Idempotency key reuse**: Reusing an `idempotency_key` with a different payload (different operations) returns `IDEMPOTENCY_KEY_REUSED` error.

**Cache TTL**: Idempotency cache entries expire after 24 hours. Retries beyond this window are treated as new requests.

**Sequence tracking**: Per-client-vault in persistent state. Survives leader failover.

**Retry behavior**:

| Scenario                    | Client sees   | Correct action                    |
| --------------------------- | ------------- | --------------------------------- |
| `CREATE` → `ALREADY_EXISTS` | Goal achieved | No retry needed                   |
| `DELETE` → `NOT_FOUND`      | Goal achieved | No retry needed                   |
| Network timeout             | Unknown       | Retry with same `idempotency_key` |
| `IDEMPOTENCY_KEY_REUSED`    | Client bug    | Generate new key, fix payload     |

---

## Observability & Monitoring

### Prometheus Metrics

All Prometheus writes go through one of two `ObservabilityContext` types. `RequestContext` is created at gRPC handler entry and its `Drop` impl emits `ledger_grpc_requests_total`, `ledger_grpc_request_latency_seconds`, and the SLI histogram — handlers emit no metrics directly. `JobContext` is created at background job cycle start and its `Drop` impl emits the three `ledger_background_job_*` family metrics. Both contexts route writes through `CardinalityTracker`, which enforces per-metric time-series budgets and increments `ledger_metrics_cardinality_overflow_total` rather than letting high-cardinality label sets reach the registry. Entity IDs, user IDs, and region are not metric labels — they appear in structured log lines and wide events only.

Histogram buckets are aligned with SLI targets: `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0]` seconds.

```text
# gRPC request tracking (unified across all 13 services)
ledger_grpc_requests_total{service, method, status}
ledger_grpc_request_latency_seconds{service, method}
ledger_grpc_sli_latency_seconds{service, method, status}

# Batch & rate limit queue depth
ledger_batch_queue_depth
ledger_rate_limit_queue_depth

# Cluster health
ledger_cluster_quorum_status              # 1 = quorum, 0 = lost
ledger_leader_elections_total

# Resource saturation
ledger_disk_total_bytes{path} / ledger_disk_available_bytes{path}
ledger_page_cache_hits_total / _misses_total
ledger_btree_depth
ledger_btree_splits_total
ledger_compaction_lag_blocks
ledger_snapshot_disk_bytes

# Rate limiting & hot keys
ledger_rate_limit_exceeded_total
ledger_hot_key_detected_total

# Raft proposals (note: inferadb_ledger_ prefix for Raft-layer metrics)
inferadb_ledger_raft_proposals_total
inferadb_ledger_raft_proposal_timeouts_total

# Leader transfer
ledger_leader_transfers_total{status}
ledger_leader_transfer_latency_seconds
ledger_trigger_elections_total{result}

# Events (organization audit trail)
ledger_event_writes_total{emission, scope, action}
ledger_events_ingest_total
ledger_events_ingest_duration_seconds

# Onboarding lifecycle (collapsed to single family)
ledger_onboarding_requests_total{stage, status}

# Organization purge + post-erasure compaction (collapsed)
ledger_org_purge_failures_total{tier, exhausted}
ledger_post_erasure_compaction_triggered_total{region}

# Cardinality discipline
ledger_metrics_cardinality_overflow_total{metric_name}  # must be zero

# Background jobs (all 20 jobs, emitted by JobContext)
ledger_background_job_runs_total{job, result}
ledger_background_job_duration_seconds{job}
ledger_background_job_items_processed_total{job}

# SDK-side metrics (optional, ledger_sdk_ prefix)
ledger_sdk_request_duration_seconds{method}
ledger_sdk_retries_total{method}
ledger_sdk_circuit_transitions_total{endpoint, state}
```

See `docs/operations/slo.md` for recommended SLO thresholds and `docs/operations/alerting.md` for alerting rules.

### Health Checks

Three-probe pattern following Kubernetes conventions:

- **Startup**: Data directory exists and is writable; database opens successfully
- **Liveness**: Async runtime responsive; all background jobs heartbeating within expected intervals (via `BackgroundJobWatchdog`)
- **Readiness**: Node in `Ready` phase (not `Starting`, `Draining`, or `ShuttingDown`); dependency checks pass:
  - Disk is writable (touch + delete probe file)
  - At least one Raft peer reachable
  - Raft log not too far behind leader (configurable `max_raft_lag`, default 1000)
- **Leader check**: Available via Raft role in health response details

Dependency check results are cached with configurable TTL (default 5s) to prevent I/O storms from aggressive probe intervals. Per-vault health (divergence status, block height) is available when `vault_slug` is specified.

---

## Consistency Guarantees

### Within Organization

| Read Type             | Guarantee          | Latency |
| --------------------- | ------------------ | ------- |
| Linearizable          | Reads own writes   | +1 RTT  |
| Eventually consistent | May see stale data | Fastest |

### Cross-Organization

No transactions span organizations. For multi-organization operations:

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
| Per-request overhead   | Higher (framing + HTTP/2)         | Lower                       |

The overhead is negligible compared to the consensus round-trip on the write path, and gRPC's development velocity — generated clients, tooling, standard middleware — materially outweighs a custom protocol's marginal latency edge.

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

**Decision**: Single Raft leader per region handles all writes.

Single leader is a bottleneck but simplifies consistency. Horizontal scaling via regions (each region is an independent Raft group).

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

## Data Residency

### Jurisdictional Guarantees

Each organization declares a `Region` at creation. This region determines:

1. **Which Raft group** stores the organization's data (1:1 region-to-Raft-group mapping)
2. **Which nodes** replicate the data (protected regions restrict to in-region nodes only)
3. **Which encryption key** (Region Master Key) protects the data at rest

Data residency is enforced at multiple layers:

| Layer           | Enforcement                                                             |
| --------------- | ----------------------------------------------------------------------- |
| Raft membership | Protected regions only admit nodes tagged with that region              |
| Storage         | Per-region database files in isolated directories                       |
| Encryption      | Per-region RMK — data unreadable if storage media crosses jurisdictions |
| Routing         | `RegionRouter` forwards writes to the correct region's Raft leader      |
| Snapshots       | Per-region snapshot files, encrypted with region's RMK                  |

Region membership rules are defined in [Scaling Architecture: Regions](#scaling-architecture-regions). The key asymmetry: a node in `IE_EAST_DUBLIN` holds replicas of `US_EAST_VA` data (non-protected), but a node in `US_EAST_VA` does NOT hold replicas of `IE_EAST_DUBLIN` data (protected/GDPR).

### Organization Region Migration

Organizations can migrate between regions. Migration uses `OrganizationStatus::Migrating` and a cross-region saga:

1. Mark organization as `Migrating` in `_system` (rejects new writes)
2. Transfer data from source region to target region
3. Update organization-to-region mapping in `_system`
4. Mark organization as `Active`

Rollback: if transfer fails, revert status to `Active` in source region. No data loss.

---

## Encryption at Rest

### Architecture

All on-disk data is encrypted using **per-page envelope encryption**. Each region manages its own **Region Master Key (RMK)**. Key material never leaves its region and is never stored in Raft.

```text
┌──────────────────────────────────────────────────────────┐
│                    Key Hierarchy                          │
├──────────────────────────────────────────────────────────┤
│  Key Backend (secrets manager / env vars / file)          │
│      │                                                    │
│      ├── RMK (us-east-va)  → wraps DEKs for region pages │
│      ├── RMK (ie-east-dublin) → wraps DEKs               │
│      ├── RMK (global) → wraps DEKs                       │
│      └── Email blinding key → HMAC for email uniqueness  │
│                                                           │
│  Per-page DEKs (one-DEK-one-encrypt invariant)            │
│      └── Random 256-bit key per page write                │
│          Wrapped by region RMK via AES-KWP (RFC 5649)    │
│                                                           │
│  Per-subject keys (crypto-shredding)                      │
│      └── Derived from RMK + subject_id                    │
│          Used to encrypt user PII fields                  │
└──────────────────────────────────────────────────────────┘
```

**Infrastructure keys** (RMKs, email blinding key) exist outside Ledger's storage — loaded from a key backend at startup, never stored in Raft.

### Envelope Encryption

Each page is encrypted with a unique random 256-bit **Data Encryption Key (DEK)**:

1. **DEK encrypts page body** — AES-256-GCM with random 12-byte nonce. The 16-byte page header serves as Additional Authenticated Data (AAD), binding header to body and preventing cross-page header swaps.
2. **RMK wraps DEK** — AES-KWP (RFC 5649), a nonce-free key-wrapping algorithm. Output is 40 bytes (32-byte key + 8-byte integrity check). Nonce-free wrapping eliminates nonce-reuse risks at the wrapping layer.
3. **Sidecar stores crypto metadata** — 72-byte entry per page: `rmk_version(4) + wrapped_dek(40) + nonce(12) + auth_tag(16)`. Stored in a separate `.crypto` file indexed by page ID, keeping the page format and `page_size` unchanged.

**Caching**: A two-tier cache minimizes crypto overhead on reads. The `DekCache` (LRU, keyed by `WrappedDek`) avoids repeated AES-KWP unwrap operations. The `RmkCache` maps version numbers to loaded master keys (typically 1–2 entries). Cache misses fall through to the key manager backend for lazy loading.

**Source files**: `crates/store/src/crypto/operations.rs` (encrypt/decrypt/wrap/unwrap), `crates/store/src/crypto/types.rs` (DEK, RMK, CryptoMetadata), `crates/store/src/crypto/sidecar.rs` (per-page metadata storage), `crates/store/src/crypto/cache.rs` (DekCache, RmkCache), `crates/store/src/crypto/backend.rs` (EncryptedBackend).

### RMK Lifecycle

RMK versions progress through three states defined by `RmkStatus`:

| State              | Behavior                                                                      |
| ------------------ | ----------------------------------------------------------------------------- |
| **Active**         | Used for new writes. Highest version wins during brief multi-version overlap. |
| **Deprecated**     | No longer used for new writes. Still loadable for reading existing artifacts. |
| **Decommissioned** | Permanently unloadable. All artifacts must have been re-wrapped first.        |

Multiple versions coexist at runtime for seamless rotation. A background **DEK re-wrapping job** (`DekRewrapJob` in `crates/raft/src/dek_rewrap.rs`) updates sidecar metadata to reference the new RMK version without touching encrypted page bodies — a metadata-only operation. The job runs on the leader, processes pages in configurable batches (`RewrapConfig`), and is resumable across restarts via `RewrapProgress` (atomic counters).

### Key Backends

Three `KeyManagerBackend` variants support different deployment contexts:

| Backend          | Use Case          | Key Source                                         | Trait Implementation       |
| ---------------- | ----------------- | -------------------------------------------------- | -------------------------- |
| `SecretsManager` | Production        | Infisical, Vault, AWS/GCP/Azure KMS                | `SecretsManagerKeyManager` |
| `Env`            | Staging / CI      | `LEDGER_RMK_{REGION}_V{VERSION}` (hex-encoded)     | `EnvKeyManager`            |
| `File`           | Local development | `{key_dir}/{region}/v{version}.key` (raw 32 bytes) | `FileKeyManager`           |

All backends implement the `RegionKeyManager` trait: `current_rmk()`, `rmk_by_version()`, `list_versions()`, `rotate_rmk()`, `decommission_rmk()`, `health_check()`.

**Source files**: `crates/store/src/crypto/key_manager.rs`, `crates/types/src/config/key_management.rs`.

### Node Startup Key Verification

Nodes verify all required RMKs before joining any Raft group:

1. Compute `required_regions(node.region)` — all regions needing RMKs
2. Load and verify each RMK (decrypt test block, check version)
3. Load and verify email blinding key
4. Any failure: fatal startup error with key type, region name, version, remediation

A node that joins Raft before verifying RMKs would deadlock during encrypted snapshot installation.

### Encrypted Storage Backend

Each region's database files use an `EncryptedBackend<B>` wrapping the standard `FileBackend`. On write: generates a fresh DEK, encrypts the page body, wraps the DEK with the current RMK, and stores crypto metadata in the sidecar. On read: loads sidecar metadata, checks DEK cache, unwraps DEK on miss, and decrypts the body. When encryption is disabled, all operations pass through with zero overhead. Different regions use different RMKs — a compromised RMK for `us-east-va` cannot decrypt `ie-east-dublin` data.

---

## Right to Erasure (Crypto-Shredding)

### GDPR Article 17 Compliance

Ledger satisfies right-to-erasure obligations via **crypto-shredding**: destroying a per-subject encryption key renders all encrypted PII unreadable without rewriting the Raft log or re-snapshotting.

### Mechanism

1. Each user's PII is encrypted with a **per-subject key** derived from the region's RMK and the user's `subject_id`
2. Non-PII data (authorization tuples, org metadata) is not per-subject encrypted
3. To erase a user: destroy their per-subject key
4. The encrypted PII remains in storage but is cryptographically unreadable
5. No Raft log rewrite required — the blockchain remains intact

### What Constitutes PII

| Data                         | Classification | Storage Location    |
| ---------------------------- | -------------- | ------------------- |
| User profile                 | PII            | Regional `state.db` |
| Email address (blinded hash) | PII            | Regional `state.db` |
| Email verification tokens    | PII            | Regional `state.db` |
| Organization metadata        | Non-PII        | Regional `state.db` |
| Authorization relationships  | Non-PII        | Regional `state.db` |
| Org registry, sequences      | Non-PII        | Global `state.db`   |

### Bounded Erasure Time

Key destruction is a single Raft-committed operation. Once the per-subject key is deleted, all encrypted PII for that subject becomes unreadable within one Raft round-trip.

---

## Limitations

### Known Limits

| Dimension         | Recommended Max  | Hard Limit | Bottleneck                 |
| ----------------- | ---------------- | ---------- | -------------------------- |
| Vaults per region | 1,000            | 10,000     | Region block size, memory  |
| Regions           | 25 (enum)        | 25         | Compile-time enum variants |
| Total vaults      | 10M              | ~100M      | `_system` registry size    |
| Keys per vault    | 10M              | ~100M      | State tree memory          |
| Transactions/sec  | 5,000 per region | ~50,000    | Raft throughput            |

### Not Supported

- **Cross-vault transactions** — By design, for isolation
- **Byzantine fault tolerance** — Trusted network assumption
- **Sub-millisecond writes** — Consensus overhead
- **Infinite retention** — Storage costs require policy
- **Instant per-key proofs** — Hybrid storage trade-off
- **Range proofs** — Bucket-based state root limitation

---

## Open Questions

### Unresolved

1. **Region migration workflow**: Organizations can migrate between regions (e.g., regulatory change). The `OrganizationStatus::Migrating` variant and cross-region saga orchestrator exist. Open: exact data transfer protocol, partial-failure rollback semantics, and progress reporting during large migrations.

2. **Cross-region read coordination**: For dashboards aggregating multiple organizations across regions, what consistency model? Eventual consistency is likely acceptable; the routing layer can fan out reads to multiple region leaders. Open: whether to provide a formal "scatter-gather" API or leave this to application-level orchestration.

3. **Proof aggregation for bulk verification**: Auditing thousands of keys individually is expensive. Open: batch proof construction (e.g., multi-leaf Merkle proofs, proof compression) to reduce verification overhead for bulk audit scenarios.

### Future Considerations

1. **Zero-knowledge proofs**: ZK-SNARKs could enable private verification — proving a relationship exists without revealing the relationship itself. Relevant for privacy-sensitive authorization checks.

2. **Witness-based state proofs**: An alternative to replay-based verification where lightweight witnesses (state diffs + merkle paths) allow point-in-time state verification without replaying the full transaction history.

---

## Success Criteria

### Performance Targets

| Metric              | Design Target    | Measurement Condition       |
| ------------------- | ---------------- | --------------------------- |
| Read latency (p99)  | < 2 ms           | Single key lookup           |
| Write latency (p99) | < 50 ms          | Batch committed             |
| Write throughput    | 5,000 tx/sec     | 3-node cluster              |
| Read throughput     | 100,000 req/sec  | Eventually consistent reads |

These are design targets — the envelope Ledger is engineered to operate within, not benchmark figures. Actual numbers depend on hardware, workload shape, and configuration; see [WHITEPAPER.md § 7](WHITEPAPER.md#7-performance-characteristics) for the qualitative performance model and how to reproduce benchmarks on your own hardware.

### Correctness Invariants

See [System Invariants](#system-invariants) — safety invariants 1–4 define the correctness contract.

### Operational Requirements

1. **Recovery time**: Node recovers from crash within 5 minutes
2. **Failover time**: New leader elected within 10 seconds
3. **Zero data loss**: No committed transaction lost on minority failure

---

## System Invariants

Every invariant below is named with its enforcement site (the code that establishes the property) and verification site (tests, proptests, or the deterministic simulation harness that exercises it). The primary verification mechanism for consensus-level invariants (1–7, 13) is the deterministic simulation harness under `crates/consensus/src/simulation/` (`harness.rs`, `multi_raft.rs`, `network.rs`); state-level invariants (8–10, 14–16) are enforced in `crates/types/`, `crates/state/`, `crates/store/`, and `crates/raft/`.

### Safety Invariants

1. **Commitment permanence**: Committed blocks cannot be removed or modified. _Enforced by `Shard` in `crates/consensus/src/shard.rs` (no `Action` variant removes a committed entry); verified by the deterministic simulation harness in `crates/consensus/src/simulation/`._
2. **State determinism**: Replaying identical transactions produces identical `state_root`. _Enforced by `RaftPayload.proposed_at` in `crates/raft/src/log_storage/types.rs` (leader-assigned timestamp so all replicas apply with identical inputs) and the deterministic `ApplyWorker` in `crates/raft/src/apply_worker.rs`; verified by state-root parity checks in `just test-stress-correctness`._
3. **Chain continuity**: Every block (except genesis) links to its predecessor via `previous_hash`. _Enforced by `BlockHeader.previous_hash` validation at commit (appendix specifies byte layout); `BlockArchive::append_block` is idempotent-by-height, making this property stable under crash recovery (see `crates/state/CLAUDE.md` rule 10)._
4. **Quorum requirement**: Commits require 2f+1 node acknowledgment. _Enforced by `Shard` commit logic in `crates/consensus/src/shard.rs` (emits `Commit` only once quorum is reached); verified by `crates/consensus/src/simulation/multi_raft.rs`._

### Liveness Invariants

5. **Progress under minority failure**: Fewer than f+1 failures → writes continue. _Verified by partition-and-fail tests in `crates/consensus/src/simulation/`._
6. **Eventual convergence**: After partition heals, all replicas converge to the same state. _Verified by partition-heal tests in `crates/consensus/src/simulation/`._
7. **Leader availability**: A new leader is elected within the election timeout after leader failure. _Enforced by `crates/consensus/src/leadership.rs` (election timer + vote handling); verified by election tests in the simulation harness._

### Integrity Invariants

8. **Merkle soundness**: Invalid values cannot produce valid proofs. _Enforced by `Merkle::verify()` in `crates/types/src/merkle.rs`; verified by proptests constrained to power-of-2 leaf counts (see `crates/types/CLAUDE.md`)._
9. **Hash chain integrity**: Modifying any block invalidates all subsequent blocks. _Enforced by SHA-256 chain construction via `previous_hash`; byte layout fixed in the [Cryptographic Hash Specifications](#a-cryptographic-hash-specifications) appendix and verified by hash-chain property tests._
10. **Transaction atomicity**: All operations in a transaction apply together or not at all. _Enforced by `WriteTransaction` atomicity in `crates/store/` (dual-slot commit protocol) and `StateLayer::apply_operations` / `apply_operations_lazy` in `crates/state/src/state.rs`; verified by crash-recovery tests (`just test-store-recovery`) that exercise every `CrashPoint` in `crates/test-utils/src/crash_injector.rs`._

### Operational Invariants

11. **Snapshot consistency**: Loading a snapshot and applying subsequent blocks produces the current `state_root`. _Enforced by `SnapshotWriter` / `SyncSnapshotReader` in `crates/raft/src/snapshot.rs` and `RaftLogStore::install_snapshot` in `crates/raft/src/log_storage/raft_impl.rs`; verified by snapshot-determinism tests in `just test-stress-correctness`._
12. **Historical accessibility**: Any committed block is readable with verifiable proofs. _Exposed via the `HistoricalRead`, `GetBlock`, and `GetBlockRange` RPCs in `proto/ledger/v1/ledger.proto`, served from `BlockArchive` in `crates/state/src/`._
13. **Replication durability**: After Raft commit, data exists on quorum. _Enforced by batched barrier fsync in `crates/consensus/src/reactor.rs` (one fsync per batch via `crates/fs-sync/src/lib.rs`); contract detailed in [`docs/operations/durability.md`](docs/operations/durability.md); verified by crash-and-recover tests in `crates/consensus/src/simulation/`._

### Idempotency Invariants

14. **Sequence monotonicity**: Server-assigned per-client-vault sequences are strictly increasing. _Enforced by `SequenceCounters` in `crates/raft/src/log_storage/types.rs`; consulted via `AppliedStateAccessor::client_idempotency_check` in `crates/raft/src/log_storage/accessor.rs`._
15. **Idempotency deduplication**: Same `(client_id, idempotency_key)` returns the cached result, never re-executes. _Enforced in `crates/raft/src/idempotency.rs` (moka-tinylfu cache) plus `AppliedStateAccessor::client_idempotency_check` for cross-failover deduplication via replicated `ClientSequenceEntry`; verified by idempotency unit tests in `crates/raft/src/log_storage/accessor.rs`._
16. **Sequence persistence**: Server-assigned sequences survive leader failover (stored in `AppliedState`). _Enforced by `ClientSequenceEntry` persistence in `AppliedState` (`crates/raft/src/log_storage/types.rs`), flushed to `raft.db` on each `StateCheckpointer` tick (`crates/raft/src/state_checkpointer.rs`); verified by state-persistence tests in `crates/raft/src/log_storage/store.rs`._

---

## Appendices

### A. Cryptographic Hash Specifications

This appendix provides normative, language-independent specifications for all cryptographic hashes.

**Hash algorithm**: SHA-256 for all cryptographic commitments (32 bytes).

#### Block Hash

```text
block_hash = SHA-256(
    height              || # u64, big-endian (8 bytes)
    organization_id     || # i64, big-endian (8 bytes)
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

```text
tx_hash = SHA-256(
    tx_id               || # 16 bytes (UUID)
    client_id_len       || # u32, little-endian
    client_id           || # UTF-8 bytes
    sequence            || # u64, big-endian (server-assigned)
    actor_len           || # u32, little-endian
    actor               || # UTF-8 bytes
    op_count            || # u32, little-endian
    operations          || # See operation encoding below
    timestamp_secs      || # i64, big-endian
    timestamp_nanos        # u32, big-endian
)
```

#### Operation Encoding

```text
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

```text
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

```text
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

Snapshots are per-region (not per-vault), containing state for all vaults in the region. The header is written uncompressed; state data is zstd-compressed. Snapshots stored under `{data_dir}/snapshots/{region}/`.

```rust
struct Snapshot {
    header: SnapshotHeader,
    state: SnapshotStateData,
}

struct SnapshotHeader {
    magic: [u8; 4],
    version: u32,                                    // Schema version (currently 2)
    region: Region,
    region_height: u64,                              // Raft log index at snapshot time
    vault_states: Vec<VaultSnapshotMeta>,             // Per-vault metadata
    checksum: Hash,

    // Chain verification linkage
    genesis_hash: Hash,
    previous_snapshot_height: Option<u64>,
    previous_snapshot_hash: Option<Hash>,
    chain_commitment: ChainCommitment,
}

struct VaultSnapshotMeta {
    vault_id: VaultId,
    vault_height: u64,                               // Block height for this vault
    state_root: Hash,
    bucket_roots: [Hash; 256],                       // Per-bucket state roots
    key_count: u64,
}

struct SnapshotStateData {
    vault_entities: HashMap<VaultId, Vec<Entity>>,   // Zstd-compressed
}
```

### C. Testing Strategy

#### Unit Tests

- State root computation determinism
- Operation idempotency
- Key encoding/decoding roundtrip
- Bucket hash consistency
- Input validation boundaries (key/value sizes, character whitelists)
- gRPC status code mapping for all `ServiceError` variants
- Circuit breaker state transitions
- Rate limiter token bucket behavior
- Hot key detection with Count-Min Sketch accuracy
- Deadline propagation and near-deadline rejection

#### Property-Based Tests

- Proto↔domain conversion roundtrips (`Operation`, `SetCondition`, `Relationship`, `Entity`)
- Merkle proof verification for power-of-2 leaf counts
- B-tree insert/get/delete invariants with arbitrary keys
- Pagination token encode/decode roundtrips

#### Integration Tests

- Write path: client → leader → commit → verify
- Read path: linearizable vs. eventually consistent
- Failover: leader dies, new leader elected, no data loss
- Recovery: node crashes, restarts, catches up
- Crash recovery with injected failures (CrashInjector in test-utils)

#### Chaos Tests

- Network partition simulation
- Random node kills
- Clock skew injection
- Disk corruption injection
- Byzantine fault simulation (malformed Raft messages, invalid proposals)

---

### D. Implementation Notes

Key design decisions and their rationale:

| Area              | Decision                                                 | Rationale                                                                                    |
| ----------------- | -------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Table count       | 19 compile-time tables                                   | Slug indexes, vault-scoped state tables, and audit event tables require dedicated storage    |
| Batch timeout     | 5ms (env var default), 2ms (runtime `BatchWriter`)       | Runtime batch writer uses tighter timing for lower latency; env vars allow operator override |
| `TxId` generation | Generated in response path after Raft commit             | Avoids assigning IDs to proposals that may not commit; UUID generation is cheap              |
| Identifier types  | Newtypes via `define_id!` macro                          | Type safety prevents `OrganizationId`/`VaultId` confusion at compile time                    |
| `VaultBlock`      | `BlockHeader` + `transactions` split                     | Header alone needed for most operations (hashing, forwarding); avoids copying tx data        |
| `StateLayer`      | Indexes in `Database`; root in `VaultCommitment`         | Decouples state storage from commitment tracking; enables per-vault isolation                |
| `TableIterator`   | Streaming with `VecDeque` buffer + resume-key            | Memory-efficient for large tables (millions of entries)                                      |
| Directory layout  | Per-region directory trees with single-file ACID B-trees | Per-region isolation, independent encryption keys, no cross-region write lock contention     |
| Snapshot format   | Per-region with multi-vault metadata                     | Aligns with Raft snapshot semantics (one snapshot per Raft group / region)                   |
| Health checks     | Kubernetes three-probe pattern + dependency validation   | Required for production Kubernetes deployments                                               |
| Write path        | 13 steps including validation, rate limiting, hot keys   | Defense-in-depth: input validation, rate limiting, hot key detection, deadline checking      |
| Encryption        | Per-page DEK + RMK envelope encryption                   | Unique DEK per page enables key rotation without re-encrypting data; AES-KWP is nonce-free   |

**Source file cross-references:**

| DESIGN.md Section | Primary Source Files                                                                                                                              |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Block Structure   | `crates/types/src/types.rs`                                                                                                                       |
| Write Path        | `crates/services/src/services/write.rs`                                                                                                           |
| Read Path         | `crates/services/src/services/read.rs`                                                                                                            |
| State Layer       | `crates/state/src/state.rs`                                                                                                                       |
| Storage Backend   | `crates/store/src/db.rs`, `crates/store/src/tables.rs`                                                                                            |
| Region Router     | `crates/raft/src/region_router.rs`                                                                                                                |
| Region Resolver   | `crates/services/src/services/region_resolver.rs`                                                                                                 |
| Raft Consensus    | `crates/raft/src/raft_manager.rs`, `crates/raft/src/raft_network.rs`                                                                              |
| ID Generation     | `crates/types/src/types.rs` (`define_id!` macro)                                                                                                  |
| Key Encoding      | `crates/state/src/keys.rs`                                                                                                                        |
| Batching          | `crates/raft/src/batching.rs`                                                                                                                     |
| Health Checks     | `crates/services/src/services/health.rs`, `crates/raft/src/dependency_health.rs`                                                                  |
| Graceful Shutdown | `crates/raft/src/graceful_shutdown.rs`                                                                                                            |
| Metrics           | `crates/raft/src/metrics.rs`                                                                                                                      |
| Snapshots         | `crates/state/src/snapshot.rs`                                                                                                                    |
| Tiered Storage    | `crates/state/src/tiered_storage.rs`                                                                                                              |
| Rate Limiting     | `crates/raft/src/rate_limit.rs`                                                                                                                   |
| Hot Key Detection | `crates/raft/src/hot_key_detector.rs`                                                                                                             |
| Input Validation  | `crates/types/src/validation.rs`                                                                                                                  |
| Encryption        | `crates/store/src/crypto/` (operations, types, sidecar, cache, backend, key_manager)                                                              |
| DEK Re-wrapping   | `crates/raft/src/dek_rewrap.rs`                                                                                                                   |
| Key Management    | `crates/types/src/config/key_management.rs`                                                                                                       |
| SDK Client        | `crates/sdk/src/client.rs`                                                                                                                        |
| JWT Tokens        | `crates/services/src/jwt.rs`, `crates/services/src/services/token.rs`, `crates/state/src/system/token.rs`, `crates/raft/src/token_maintenance.rs` |

---

## Revision History

| Date       | Change                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2026-04-15 | Extracted `inferadb-ledger-consensus` crate (event-driven Reactor, purpose-built multi-shard engine, segmented WAL). Operationalization concerns (apply-phase parallelism, saga orchestration, background jobs) remain in `inferadb-ledger-raft`. Metric name corrections (`ledger_hot_key_detected_total`, `inferadb_ledger_raft_proposal_timeouts_total`). Added metrics for leader transfer, events, onboarding, organization purge, post-erasure compaction. Table count 19 → 23 (added slug indexes, vault-scoped tables, string dictionary). Constant-time hash comparison (`hash_eq`) now covers all security-sensitive paths. |
| 2026-03-10 | JWT token architecture: EdDSA signing keys, refresh token families, envelope encryption, TokenService, cascade revocation, token maintenance                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 2026-03-03 | Document hygiene: metadata block, audience guide, deduplicated invariants, expanded references                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 2026-03-01 | Regional data residency, multi-region architecture, cross-region saga orchestration                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 2026-02-26 | Graceful leader transfer protocol, shutdown phase transitions                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 2026-02-22 | Dual-ID architecture (`OrganizationSlug`, `VaultSlug`), namespace → organization rename                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 2026-02-06 | Proto conversion deduplication, Raft proposal timeout, write path expanded to 13 steps                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 2026-01-28 | Encryption at rest (per-page envelope encryption, RMK lifecycle), crypto-shredding, tiered storage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 2026-01-28 | Health checks (three-probe pattern), graceful shutdown, backup and restore, degraded operation modes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 2026-01-15 | Observability (Prometheus metrics, SLI/SLO), rate limiting, hot key detection, input validation                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 2026-01-10 | Core architecture: state layer, bucket-based state roots, transaction batching, Raft consensus integration                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 2026-01-09 | Initial design document                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |

---

## References

1. [Zanzibar: Google's Consistent, Global Authorization System](https://research.google/pubs/pub48190/) — USENIX ATC 2019
2. [Raft: In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) — USENIX ATC 2014
3. [OpenRaft](https://github.com/datafuselabs/openraft) — Rust Raft reference implementation (historical inspiration; Ledger's consensus layer is a purpose-built multi-shard engine in `crates/consensus/`, not an openraft dependency)
4. [QMDB: Quick Merkle Database](https://github.com/LayerZero-Labs/qmdb) — Append-only log with O(1) merkleization
5. [SeiDB](https://docs.sei.io/learn/seidb) — Separates state commitment from storage
6. [OWASP Top 10](https://owasp.org/Top10/) — Web application security risks
7. [AES Key Wrap with Padding (RFC 5649)](https://www.rfc-editor.org/rfc/rfc5649) — DEK wrapping algorithm
8. [AES-GCM (NIST SP 800-38D)](https://csrc.nist.gov/pubs/sp/800/38/d/final) — Authenticated encryption for page bodies
9. [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) — Distributed unique ID generation (external slugs)
10. [Count-Min Sketch (Cormode & Muthukrishnan, 2005)](https://doi.org/10.1016/j.jalgor.2003.12.001) — Probabilistic frequency estimation (hot key detection)
11. [seahash](https://docs.rs/seahash) — Non-cryptographic hash for bucket assignment
12. [bon](https://docs.rs/bon) — Compile-time builder generation for Rust structs
