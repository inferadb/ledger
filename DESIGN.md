# InferaDB Ledger Design Document

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
│   Organization: org_acme    │      Organization: org_startup        │
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
| Organization | Entities, vaults, keys          | Shard (Raft consensus)    |
| Vault        | Cryptographic chain, state_root | Organization storage      |
| Shard     | Physical nodes                  | Cluster                |

Each vault maintains its own blockchain. A corruption in Vault A cannot affect Vault B's chain integrity.

---

## Architecture

### Terminology

| Term                    | Definition                                                                                                              |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Organization**        | Storage unit per customer/org. Contains entities and vaults. Isolated with separate storage.                                |
| **Vault**               | Relationship store within an organization. Maintains its own cryptographic chain (state_root, previous_hash, block height). |
| **Entity**              | Key-value data stored in an organization (users, teams, clients, sessions). Supports TTL, versioning, conditional writes.   |
| **Relationship**        | Authorization tuple: `(resource, relation, subject)`. Used by Engine for permission checks.                             |
| **Shard**                  | Operational grouping for consensus. Contains one or more organizations sharing a Raft group.                               |
| **`_system` organization** | Special organization for global data: user accounts and organization routing. Replicated to all nodes.                     |

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
    organization_slug: OrganizationSlug,
    vault_id: VaultId,

    // Chain linking (immutability)
    previous_hash: Hash,           // SHA-256 of previous vault block

    // Content summary
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

```
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
[8] Batcher aggregates (up to 100 txs or batch_timeout)
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
[12] State layer assigns sequence + applies transactions + computes state_root
    │
    ▼
[13] Response to client with assigned_sequence + state_root hash
     (includes x-request-id, x-trace-id, x-ledger-api-version headers)
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
// Simplified conceptual model — actual implementation details below
struct StateLayer<B: StorageBackend> {
    db: Arc<Database<B>>,                                // All tables stored within
    vault_commitments: RwLock<HashMap<VaultId, VaultCommitment>>,
}

// The Database contains 18 compile-time tables including:
//   Entities, Relationships, ObjIndex, SubjIndex,
//   Blocks, VaultBlockIndex, VaultMeta, OrganizationMeta,
//   Sequences, ClientSequences, CompactionMeta,
//   RaftLog, RaftState, OrganizationSlugIndex, VaultSlugIndex,
//   VaultHeights, VaultHashes, VaultHealth

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

| ID Type       | Rust Type              | Size     | Generated By  | Strategy                               |
| ------------- | ---------------------- | -------- | ------------- | -------------------------------------- |
| `OrganizationSlug` | newtype(`u64`)    | uint64   | Client (Snowflake) | External identifier               |
| `VaultId`     | newtype(`i64`)         | int64    | Ledger Leader | Sequential (internal only)             |
| `VaultSlug`   | newtype(`u64`)         | uint64   | Snowflake     | External identifier for vaults         |
| `UserId`      | newtype(`i64`)         | int64    | Ledger Leader | Sequential                             |
| `TxId`        | type alias(`[u8; 16]`) | 16 bytes | Ledger Leader | UUID assigned in response construction |
| `NodeId`      | type alias(`String`)   | string   | Admin         | Configuration                          |
| `ShardId`     | newtype(`u32`)         | uint32   | Admin         | Sequential at shard creation           |
| `ClientId`    | type alias(`String`)   | string   | Client        | Client-provided                        |

Newtypes are generated by the `define_id!` macro with `Display` formatting (e.g., `org:42`, `vault:7`), `Serialize`/`Deserialize` with `#[serde(transparent)]`, and `From`/`Into` conversions. Type aliases (`TxId`, `NodeId`, `ClientId`) remain unwrapped for compatibility with their usage patterns.

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
- Maximum batch delay: 5ms (env var default); 2ms (runtime `BatchWriter` default)
- Tick interval: 500µs (runtime `BatchWriter` polling interval)

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

## Scaling Architecture: Shard Groups

As vaults grow, organizations can be distributed across **shard groups**—independent Raft clusters.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Cluster                                    │
├─────────────────────────────────────────────────────────────────────────┤
│  Shard 0              │  Shard 1              │  Shard 2                │
│  ┌─────────────────┐  │  ┌─────────────────┐  │  ┌─────────────────┐    │
│  │ Nodes A, B, C   │  │  │ Nodes D, E, F   │  │  │ Nodes G, H, I   │    │
│  │ Raft Group      │  │  │ Raft Group      │  │  │ Raft Group      │    │
│  └─────────────────┘  │  └─────────────────┘  │  └─────────────────┘    │
│  Organizations: 1-100 │  Organizations: 101-200│  Organizations: 201-300 │
└─────────────────────────────────────────────────────────────────────────┘
```

**Shard assignment**: Organization-to-shard mapping stored in `_system` organization. Assignment strategies:

- Hash-based (automatic distribution)
- Explicit (for performance isolation)
- Dedicated shard (for high-volume vaults)

**Cross-shard consistency**: No cross-shard transactions. Application-level coordination required for multi-organization operations.

---

## Discovery & Coordination

### System Organization (`_system`)

The `_system` organization (OrganizationId = 0) stores cluster-wide metadata:

```
_system organization
├── Users (global accounts)
├── Organization routing table (organization → shard mapping)
└── Cluster configuration
```

Replicated to all nodes via dedicated Raft group. All nodes can route requests without cross-shard queries.

### Client Discovery Flow

```
[1] Client connects to any node
[2] Sends request with organization_slug
[3] Node looks up organization → shard mapping in local _system replica
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

```
[1] Verify this node is the current leader
[2] Select target: explicit node or auto-pick most caught-up follower
[3] Wait for replication — target's match_index reaches leader's last_log_id
[4] Send TriggerElection internal RPC to target
[5] Poll until target wins election or timeout expires
```

**Concurrency**: An `AtomicBool` transfer lock ensures only one transfer runs at a time. Concurrent requests receive `ABORTED`.

**Phase transitions during shutdown**:

```
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
{data_dir}/
├── state.db              # Single-file ACID B-tree: entities, relationships, indexes,
│                         # blocks, vault metadata, organization metadata, sequences
├── raft.db               # Raft log entries, hard state, vote, snapshots
└── snapshots/
    ├── 000001000.snap    # Periodic state snapshots (numbered by height)
    └── backups/          # Operator-initiated backups (when configured)
```

The storage uses embedded single-file ACID databases with copy-on-write B-trees, not separate directories per data type. All 15 compile-time tables reside within a single `state.db` file.

### Storage Backend

`inferadb-ledger-store` provides:

- B+ tree engine with page-level management (15 compile-time tables)
- ACID transactions with copy-on-write pages
- Configurable backends (file-based, memory for testing)
- Streaming `TableIterator` with resume-key pattern for memory-efficient traversal
- Background compaction via `BTreeCompactor` (leaf merging to reclaim dead space)

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

Histogram buckets are aligned with SLI targets: `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0]` seconds.

```
# Latency histograms
ledger_read_latency_seconds{quantile="0.99"}
ledger_write_latency_seconds{quantile="0.99"}

# gRPC request tracking (with error classification)
ledger_grpc_requests_total{service, method, status, error_class}

# Batch & rate limit queue depth
ledger_batch_queue_depth
ledger_rate_limit_queue_depth

# Cluster health
ledger_cluster_quorum_status              # 1 = quorum, 0 = lost
ledger_leader_elections_total

# Resource saturation
ledger_disk_bytes_total / _free / _used
ledger_page_cache_hits_total / _misses_total / _size
ledger_btree_depth{table}
ledger_btree_page_splits_total
ledger_compaction_lag_blocks
ledger_snapshot_disk_bytes

# Rate limiting & hot keys
ledger_rate_limit_exceeded_total
ledger_hot_keys_detected_total

# Raft proposals
ledger_raft_proposal_timeouts_total

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

### Resolved Design Decisions

1. **Hot key contention mitigation**: Resolved — Ledger includes a built-in `HotKeyDetector` using Count-Min Sketch for probabilistic frequency estimation with rotating time windows, top-k tracking via min-heap, and Prometheus metrics emission. Integrated into both `WriteService` and `MultiShardWriteService`.

2. **Tiered storage**: Resolved — `tiered_storage.rs` implements three tiers (Hot/Warm/Cold) using the `object_store` crate for cloud provider abstraction (S3, GCS, Azure). Used for snapshot lifecycle management.

### Unresolved Design Decisions

1. **Automatic shard rebalancing**: Currently manual. What triggers and safety checks for automatic rebalancing?

2. **Cross-shard read coordination**: For dashboards aggregating multiple organizations, what consistency model?

3. **Proof aggregation for bulk verification**: How to efficiently verify thousands of keys without individual proofs?

### Future Considerations

1. **Zero-knowledge proofs**: Could ZK-SNARKs enable private verification without revealing data?

---

## Success Criteria

### Performance Targets

| Metric              | Target          | Measured         | Measurement Condition       |
| ------------------- | --------------- | ---------------- | --------------------------- |
| Read latency (p99)  | < 2 ms          | **2.8 µs**       | Single key lookup           |
| Write latency (p99) | < 50 ms         | **8.1 ms**       | Batch committed             |
| Write throughput    | 5,000 tx/sec    | **11K tx/sec**   | 3-node cluster              |
| Read throughput     | 100,000 req/sec | **952K req/sec** | Eventually consistent reads |

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

14. **Sequence monotonicity**: Server-assigned per-client-vault sequences are strictly increasing
15. **Idempotency deduplication**: Same `(client_id, idempotency_key)` returns cached result, never re-executes
16. **Sequence persistence**: Server-assigned sequences survive leader failover (stored in `AppliedState`)

---

## Appendices

### A. Cryptographic Hash Specifications

This appendix provides normative, language-independent specifications for all cryptographic hashes.

**Hash algorithm**: SHA-256 for all cryptographic commitments (32 bytes).

#### Block Hash

```
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

```
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

Snapshots are per-shard (not per-vault), containing state for all vaults in the shard. The header is written uncompressed; state data is zstd-compressed.

```rust
struct Snapshot {
    header: SnapshotHeader,
    state: SnapshotStateData,
}

struct SnapshotHeader {
    magic: [u8; 4],
    version: u32,                                    // Schema version (currently 2)
    shard_id: ShardId,
    shard_height: u64,                               // Raft log index at snapshot time
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

Deliberate deviations from earlier versions of this specification, with rationale:

| Area              | Original Spec                     | Implementation                                         | Rationale                                                                                        |
| ----------------- | --------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| Table count       | 13 tables                         | 18 tables                                              | Added slug indexes, vault-scoped state tables; removed `TimeTravelConfig` and `TimeTravelIndex` (superseded by audit events) |
| Batch timeout     | 5ms                               | 5ms (env var default), 2ms (runtime `BatchWriter`)      | Runtime batch writer uses tighter timing for lower latency; env vars allow operator override     |
| `TxId` generation | "At block creation"               | In response path after Raft commit                     | Avoids assigning IDs to proposals that may not commit; UUID generation is cheap                  |
| Identifier types  | Type aliases                      | Newtypes via `define_id!` macro                        | Type safety prevents `OrganizationId`/`VaultId` confusion at compile time                           |
| `VaultBlock`      | Flat struct                       | `BlockHeader` + `transactions` split                   | Header alone needed for most operations (hashing, forwarding); avoids copying transaction data   |
| `StateLayer`      | Fields for indexes and state_root | Indexes in `Database`; root in `VaultCommitment`       | Decouples state storage from commitment tracking; enables per-vault isolation                    |
| `TableIterator`   | Pre-loads all entries             | Streaming with `VecDeque` buffer + resume-key          | Memory-efficient for large tables (millions of entries)                                          |
| Directory layout  | Nested subdirectories             | Single-file ACID B-tree databases                      | Operational simplicity; atomic transactions across all tables                                    |
| Snapshot format   | Per-vault                         | Per-shard with multi-vault metadata                    | Aligns with Raft snapshot semantics (one snapshot per Raft group)                                |
| Health checks     | 3 simple checks                   | Kubernetes three-probe pattern + dependency validation | Required for production Kubernetes deployments                                                   |
| Write path        | 9 steps                           | 13 steps                                               | Added input validation, rate limiting, hot key detection, deadline checking                      |

**Source file cross-references:**

| DESIGN.md Section | Primary Source Files                                                         |
| ----------------- | ---------------------------------------------------------------------------- |
| Block Structure   | `crates/types/src/types.rs`                                                  |
| Write Path        | `crates/raft/src/services/write.rs`                                          |
| Read Path         | `crates/raft/src/services/read.rs`                                           |
| State Layer       | `crates/state/src/state.rs`                                                  |
| Storage Backend   | `crates/store/src/db.rs`, `crates/store/src/tables.rs`                       |
| Raft Consensus    | `crates/raft/src/raft.rs`                                                    |
| ID Generation     | `crates/types/src/types.rs` (`define_id!` macro)                             |
| Key Encoding      | `crates/state/src/keys.rs`                                                   |
| Batching          | `crates/raft/src/batching.rs`                                                |
| Health Checks     | `crates/raft/src/services/health.rs`, `crates/raft/src/dependency_health.rs` |
| Graceful Shutdown | `crates/raft/src/graceful_shutdown.rs`                                       |
| Metrics           | `crates/raft/src/metrics.rs`                                                 |
| Snapshots         | `crates/state/src/snapshot.rs`                                               |
| Tiered Storage    | `crates/state/src/tiered_storage.rs`                                         |
| Rate Limiting     | `crates/raft/src/rate_limit.rs`                                              |
| Hot Key Detection | `crates/raft/src/hot_key_detector.rs`                                        |
| Input Validation  | `crates/types/src/validation.rs`                                             |
| SDK Client        | `crates/sdk/src/client.rs`                                                   |

---

## References

1. [Zanzibar: Google's Consistent, Global Authorization System](https://research.google/pubs/pub48190/) — USENIX ATC 2019
2. [Raft: In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) — USENIX ATC 2014
3. [OpenRaft](https://github.com/datafuselabs/openraft) — Rust Raft implementation
4. [QMDB: Quick Merkle Database](https://github.com/LayerZero-Labs/qmdb) — Append-only log with O(1) merkleization
5. [SeiDB](https://docs.sei.io/learn/seidb) — Separates state commitment from storage
6. [OWASP Top 10](https://owasp.org/Top10/) — Web application security risks
