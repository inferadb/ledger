# InferaDB Ledger Consensus Engine

**Date:** 2026-04-06
**Status:** Approved
**Crate:** `inferadb-ledger-consensus` at `crates/consensus/`

## Goal

Replace openraft with a purpose-built multi-shard Raft consensus engine optimized for high-frequency authorization workloads. The engine batches I/O across shards (40x improvement), uses rkyv zero-copy serialization (eliminates 7 serde cycles), implements pipelined replication, and runs an event-driven reactor that gives idle shards zero CPU cost.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Public API                            │
│  ConsensusEngine::propose(), read_index(), add_learner() │
├─────────────────────────────────────────────────────────┤
│              Multi-Shard Event Loop (Reactor)            │
│  Single tokio task, select! loop, 1ms flush interval     │
├──────────┬──────────┬──────────┬────────────────────────┤
│  Shard 1 │  Shard 2 │  Shard N │  Shard per region  │
│  core.rs │  core.rs │  core.rs │  Event-driven, no I/O  │
├──────────┴──────────┴──────────┤                        │
│         Persistence Layer       │  Network Layer         │
│  Segmented WAL, shared fsync   │  Pipelined gRPC        │
│  rkyv zero-copy                │  Cross-shard batching  │
└─────────────────────────────────┴────────────────────────┘
```

Key architectural decisions:

- Single event loop drives all shards. Groups don't spawn tasks — the reactor drives them.
- Groups return `Action` values (send, persist, commit, schedule timer) instead of performing I/O. The reactor collects actions from all shards and executes them in batches.
- **WAL-only fsync on consensus path:** One fsync commits WAL entries for all shards. State machine apply runs asynchronously. Replaces the current 5 separate fsyncs per write batch with 1 on the critical path.
- **WAL replaces RaftLog B+ tree.** Consensus log uses append-only sequential files, not random-access B+ tree pages. The B+ tree stores only state machine data.
- rkyv-archived bytes flow from propose → WAL → wire → follower → apply without re-serialization. Zero-copy field access during apply via `rkyv::check_archived_root`.
- One gRPC call carries messages for all shards to the same destination.

## Components

### 1. Core Consensus (Shard)

Each `Shard` is a single Raft consensus instance. Evolved from the benchmark prototype (300x faster than openraft in CPU benchmarks) with production hardening.

**State:**

```rust
pub struct Shard {
    id: ShardId,
    node_id: NodeId,
    state: NodeState,           // Follower | Candidate | Leader
    current_term: u64,
    voted_for: Option<NodeId>,
    log: VecDeque<Entry>,       // In-memory log, truncated after snapshot (bounded)
    commit_index: u64,
    last_applied: u64,
    peer_states: Vec<PeerState>, // Array-based, not HashMap
    self_match_index: u64,
    leader_lease: LeaderLease,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
    membership: Membership,
    pending_membership: bool,    // At most one membership change in-flight
    pending_split: bool,         // At most one split/merge in-flight
}
```

**Event-driven interface (what the reactor calls):**

```rust
impl Shard {
    fn handle_propose(&mut self, entry: Entry) -> Vec<Action>;
    fn handle_propose_batch(&mut self, entries: Vec<Entry>) -> Vec<Action>;
    fn handle_message(&mut self, from: NodeId, msg: Message) -> Vec<Action>;
    fn handle_election_timeout(&mut self) -> Vec<Action>;
    fn handle_heartbeat_timeout(&mut self) -> Vec<Action>;
}
```

**Action type (returned to reactor, never executed by the shard):**

```rust
pub enum Action {
    Send { to: NodeId, shard: ShardId, msg: Message },
    PersistEntries { shard: ShardId, entries: Vec<Entry> },
    Committed { shard: ShardId, up_to: u64 },
    ScheduleTimer { shard: ShardId, kind: TimerKind, deadline: Instant },
    RenewLease { shard: ShardId },
    MembershipChanged { shard: ShardId, membership: Membership },
}
```

**Optimizations proven in prototype:**

- Array-based peer state (not HashMap) — direct field access instead of hashing
- `propose_batch` — one replication message for N entries
- Arc<[LogEntry]> log slices for replication — one Arc clone per peer instead of N entry clones
- Committed range tracking — no buffer clone on commit advance
- O(cluster_size) commit check — sort match indices, take quorum-th value
- `#[inline]` on hot methods
- In-memory log truncation after snapshot (entries before snapshot index are dropped from `VecDeque`). Steady-state size: at most `snapshot_threshold` (default 10,000) entries per shard. Global memory cap: configurable `max_total_log_memory` (default 2GB) across all shards, with LRU eviction of oldest entries from the least-active shards when exceeded.

**Leader lease:** First-class field, renewed natively inside the commit path (when quorum ACKs advance commit_index). Not bolted on via metrics polling. Cost: one `Instant::now()` store per commit batch.

**Pipelined replication:** Leader sends entries immediately on propose. Does not wait for previous batch ACK. `next_index` tracks what each peer has. Commit index advances when quorum ACKs arrive regardless of send ordering. Pipeline depth: configurable, default 4 in-flight batches per peer.

**NACK rewind:** When a follower rejects an `AppendEntries` (log mismatch), it returns `match_index` — the highest index it has that matches the leader's log. The leader rewinds `next_index` for that peer to `match_index + 1` and resends from there. All in-flight batches for that peer beyond `match_index` are discarded (they'll be resent with the correct `prev_log_index`). With pipeline depth 4, at most 4 batches are wasted on a NACK — the leader detects this on the first returning NACK and stops pipelining until the follower catches up.

**Pre-vote protocol:** Before starting an election, a candidate sends a `PreVote` request to all peers. Peers respond with whether they WOULD vote for the candidate (without incrementing their term). Only if a quorum would vote does the candidate proceed to a real election.

**Pre-vote response criteria:** A node grants a pre-vote if ALL of:
1. The candidate's term is at least as high as the node's current term
2. The candidate's log is at least as up-to-date as the node's log (standard Raft log comparison)
3. The node's election timer has elapsed (it hasn't heard from a valid leader recently)

Condition 3 is critical: if a node has received a heartbeat from the current leader within the election timeout, it rejects the pre-vote. This prevents a partitioned node from triggering unnecessary elections even if its log is up-to-date. Combined with leader lease: a follower that has a recent heartbeat from a leased leader rejects pre-votes, providing strong leader stability.

Pre-vote is always enabled (no configuration toggle).

### 2. Multi-Shard Event Loop (Reactor)

The architectural centerpiece. Single tokio task drives all shards, batches all I/O.

**State:**

```rust
pub struct Reactor {
    groups: HashMap<ShardId, Shard>,
    pending_sends: Vec<(NodeId, ShardId, Message)>,
    pending_persists: Vec<(ShardId, Vec<Entry>)>,
    pending_commits: Vec<(ShardId, u64)>,
    timers: TimerWheel,
    inbox: mpsc::Receiver<ReactorEvent>,
    wal: SharedWal,
    outbox: NetworkOutbox,
    state_machines: HashMap<ShardId, Box<dyn StateMachine>>,
}
```

**Main loop:**

```rust
async fn run(&mut self) {
    loop {
        tokio::select! {
            Some(event) = self.inbox.recv() => {
                let actions = self.dispatch_event(event);
                self.collect_actions(actions);
            }
            Some((shard_id, kind)) = self.timers.next_expiry() => {
                let actions = match kind {
                    TimerKind::Election => self.groups[&shard_id].handle_election_timeout(),
                    TimerKind::Heartbeat => self.groups[&shard_id].handle_heartbeat_timeout(),
                };
                self.collect_actions(actions);
            }
            _ = self.flush_interval.tick() => {
                self.flush().await;
            }
        }
    }
}
```

**Flush cycle (where batching happens):**

1. Append all shards' entries to WAL (sequential writes, no fsync yet)
2. **WAL fsync** — one `fdatasync()` makes all shards' entries durable
3. Batch-send all messages to each destination node (one gRPC call per destination)
4. Dispatch committed entries to the apply worker (async, non-blocking)
5. Notify application of consensus commit results (propose() futures resolve)

The apply worker, running concurrently:
1. Receives committed entry batches from reactor
2. Applies to state machines (B+ tree mutations, hash index, events, blocks)
3. Calls `flush_dirty_pages()` + B+ tree dual-slot commit
4. Updates `applied_index` via watch channel
5. Reads waiting on `wait_for_apply()` unblock

The reactor never waits for apply. Consensus latency = WAL fsync + quorum ACK.

**Read-after-write latency:** When a client writes then immediately reads, the read blocks on `wait_for_apply(committed_index)` until the apply worker catches up. Under normal load, the apply worker is <1ms behind — reads are near-instant. Under heavy load (apply worker processing large batches), reads may block for 10-100ms. The spec's write latency (<5ms) does not include this read-side delay. To provide strict read-after-write guarantees with bounded latency, the apply worker must keep up with the commit rate. If `applied_index` falls more than `max_apply_lag` (default 10,000 entries) behind `committed_index`, the engine rejects new proposals with `ApplyBackpressure` to prevent unbounded read latency growth.

**Why single-threaded:** Consensus logic is ~33ns per entry (proven in benchmarks). The expensive operations (fsync, network) are async. The reactor collects all CPU work synchronously (fast), then flushes I/O in one batch (async). One thread handles thousands of shards.

**Panic isolation:** Each shard's event handling is wrapped in `std::panic::catch_unwind`. If a shard's `handle_propose`, `handle_message`, or timeout handler panics (bug, corrupt data, OOM), only that shard is marked as `Failed` — the reactor continues processing all other shards. The failed shard can be recovered via snapshot reinstall. This prevents a single malformed message to one shard from killing all shards on the node.

**ReactorEvent (inbound):**

```rust
pub enum ReactorEvent {
    Propose { shard: ShardId, entry: Entry, response: oneshot::Sender<ProposeResult> },
    ProposeBatch { shard: ShardId, entries: Vec<Entry>, response: oneshot::Sender<ProposeResult> },
    PeerMessage { shard: ShardId, from: NodeId, message: Message },
    ReadIndex { shard: ShardId, response: oneshot::Sender<u64> },
    AddShard { shard: ShardId, config: ShardConfig, state_machine: Box<dyn StateMachine> },
    RemoveShard { shard: ShardId },
    AddLearner { shard: ShardId, node_id: NodeId, addr: String, response: oneshot::Sender<Result<()>> },
    PromoteVoter { shard: ShardId, node_id: NodeId, response: oneshot::Sender<Result<()>> },
    RemoveNode { shard: ShardId, node_id: NodeId, response: oneshot::Sender<Result<()>> },
    TransferLeadership { shard: ShardId, target: NodeId, response: oneshot::Sender<Result<()>> },
    Shutdown,
}
```

**Timer wheel:** Hierarchical timer wheel — groups register next deadline. Reactor wakes only when nearest deadline fires. Idle groups have no pending timers. Zero cost for hibernated groups.

**Dual-channel inbox:** The reactor has two inbound channels:
- **External inbox** (`mpsc::Receiver<ReactorEvent>`, bounded, capacity 10,000): Client proposals, peer messages, admin operations. Subject to back-pressure.
- **Internal inbox** (`mpsc::UnboundedReceiver<InternalEvent>`, unbounded): Split/merge shard registration (`AddShard`, `RemoveShard`), apply worker health notifications, timer events. Never dropped — these are consensus-committed operations that must be processed.

The reactor's `select!` loop prioritizes the internal inbox over the external inbox (using `biased;` in `tokio::select!`).

**Flow control and back-pressure:** The external inbox has a bounded capacity (default 10,000). When the inbox is full, `ConsensusEngine::propose()` returns `Err(ProposalQueueFull)` immediately — no blocking. The application layer (gRPC services) translates this to `Status::resource_exhausted`, signaling the client to retry with backoff. For slow followers: if a follower's `match_index` falls more than `max_replication_lag` (default 50,000 entries) behind the leader, the leader stops sending AppendEntries and triggers snapshot transfer instead. This prevents the replication pipeline from unbounded memory growth.

**Decoupled apply worker:** The reactor does NOT apply committed entries to the state machine inline. Instead, committed entries are dispatched to a separate apply worker (a dedicated tokio task or thread). This is critical for latency — consensus logic is ~33ns/entry but B+ tree apply is ~100-1000ns/entry. If apply ran inline, a 100-entry batch would block the reactor for ~100μs (no new proposals or messages processed).

The architecture follows TiKV's proven model:

```
Reactor (fast):       consensus → WAL write → network send → dispatch to apply worker
Apply worker (slow):  B+ tree mutations → hash index update → state root → events → blocks
```

These run concurrently — the reactor processes batch N+1 while the apply worker handles batch N.

**Durability model with decoupled apply:**
- WAL fsync guarantees consensus durability (entries are safe for replication and recovery)
- B+ tree commit happens asynchronously after apply completes (its own dual-slot fsync)
- `propose()` returns when consensus commits (WAL durable + quorum ACK), NOT when apply completes
- Reads use `wait_for_apply(committed_index)` to ensure they see applied state
- On crash recovery: replay any WAL entries committed but not yet applied to B+ tree. Typically 0-100 entries (~1ms). Practically instant but not literally zero.

**Apply worker pool:** The reactor dispatches committed entries to a pool of apply workers (default: one per CPU core, minimum 2). Each shard is assigned to a specific worker via `shard_id % num_workers`, guaranteeing in-order apply per shard while enabling cross-shard parallelism. Workers communicate via bounded `mpsc` channels — if a worker's channel is full (severely backed up), the reactor buffers entries internally rather than blocking (the reactor must never block on apply dispatch).

Each apply worker:
- Processes committed entry batches for its assigned shards in order
- Updates `applied_index` per shard via the watch channel (Section 7)
- Commits B+ tree changes via dual-slot mechanism (independent fsync per worker)
- Computes state roots, builds blocks, writes events — all per-shard

At 1000 shards across 8 workers, each worker handles ~125 shards. State root computation (~10-50μs per shard per commit) is ~1.25-6.25ms per worker per commit cycle — well within budget.

**Apply worker health monitoring:** The reactor monitors each apply worker's `JoinHandle`. If a worker panics (OOM, rkyv validation failure, B+ tree corruption), the reactor detects it via the `JoinHandle` future resolving in the `select!` loop. Response:
1. Mark all shards assigned to the failed worker as `Degraded`
2. Spawn a replacement worker for those shards
3. Trigger snapshot reinstall for the affected shards (state.db may be corrupt)
4. `wait_for_apply` calls for degraded shards return `Err(ShardDegraded)` instead of blocking forever

All `wait_for_apply` calls have a configurable timeout (default: 5 seconds). After timeout, reads return `Err(ApplyTimeout)` — the gRPC layer translates this to `UNAVAILABLE`, signaling the client to retry on another node.

**In-memory entry cache:** The reactor maintains a bounded ring buffer of recent WAL entries per shard (default: last 1000 entries or 10MB, whichever is smaller). When a follower requests entries near the head (normal steady-state replication), they're served from memory — no WAL disk read. The cache is populated during WAL write and evicted as entries age out. Only historical catch-up replication hits the WAL on disk.

### 3. Persistence Layer (Unified WAL + State Durability)

The current architecture uses 5 separate databases per region (raft.db, state.db, blocks.db, events.db, plus metadata) with 3-5 fsyncs per write. The custom engine reduces the consensus critical path to a single WAL fsync. State machine apply, block creation, and event emission happen asynchronously in the apply worker pool.

**Key insight:** The WAL and the B+ tree state machine are separate files (WAL is sequential append, B+ tree is random-access COW) but they share a single durability checkpoint. No separate "RaftLog" B+ tree table — the WAL replaces it.

**Storage layout per region:**
```
data/regions/{region}/
  wal/
    segment-000000.wal  (consensus log — append only)
    segment-000001.wal
  state.db              (B+ tree — entities, relationships, indexes)
```

**Segmented WAL:**

Each segment: fixed maximum size (default 64MB). New segment created on fill. The NEXT segment is pre-allocated when the current segment reaches 75% capacity — this avoids file creation latency during flush (directory metadata I/O can take 50-200μs). Compaction deletes entire old segments after snapshot.

**WAL frame format:**

```
[shard_id: 8 bytes LE]
[entry_len: 4 bytes LE]
[rkyv archived entry: variable]
[crc32: 4 bytes]
```

Frames are self-describing — readers skip irrelevant groups. CRC per frame for integrity. Recovery scans forward from last known-good position.

**WAL flush (reactor's durability path):**

The reactor appends entries to the WAL and fsyncs. State machine apply happens asynchronously in the apply worker — NOT in the flush cycle.

```rust
impl SharedWal {
    /// Appends entries from all shards and fsyncs the WAL.
    /// State machine apply is NOT part of this path — it's handled by the apply worker.
    async fn flush_and_sync(
        &mut self,
        persists: &[(ShardId, Vec<Entry>)],
        committed_index: u64,
    ) -> Result<()> {
        // 1. Append all entries to WAL (sequential write, no fsync yet)
        for (shard_id, entries) in persists {
            for entry in entries {
                let archived = rkyv::to_bytes::<Entry>(entry)?;
                self.write_frame(*shard_id, &archived)?;
            }
        }
        
        // 2. Write commit_index checkpoint frame
        self.write_checkpoint(committed_index)?;
        
        // 3. fsync the WAL — entries are now durable for consensus
        self.file.sync_data()?;
        
        Ok(())
    }
}
```

This replaces the current 5 fsyncs (raft.db log → state.db apply → raft.db metadata → blocks.db archive → events.db audit). The WAL fsync is the ONLY fsync on the consensus critical path. The apply worker commits state.db independently via the B+ tree's dual-slot mechanism.

**Why this is safe:** The WAL is the sole source of truth for consensus. If the node crashes:
- After WAL fsync: entries are durable. On restart, replay any entries between `applied_index` and `committed_index` from the WAL. Typically 0-100 entries (~1ms).
- Before WAL fsync: entries are lost — but they were never acknowledged to clients or peers, so Raft replication resends them.
- State.db may lag behind the WAL. This is correct — the apply worker will catch up, and reads use `wait_for_apply` to block until applied.

**Read path:** Returns raw `Arc<[u8]>` (rkyv archived bytes). No deserialization — same bytes go to WAL, wire, and follower state machine.

**Log compaction:** After snapshot at index N, entries before N are reclaimable. When entire WAL segment is reclaimable, delete the file.

**Zero-copy apply path:**

Committed entries arrive as rkyv-archived bytes (from WAL or replication). The state machine applies them without deserialization:

```rust
// Entry arrives as &[u8] (rkyv archive)
let archived = rkyv::check_archived_root::<Entry>(&bytes)?;

// Access fields via zero-copy references — no heap allocation
match &archived.operation {
    ArchivedOperation::CreateRelationship { resource, relation, subject } => {
        // Compute binary key directly from archived &str references
        let key = encode_relationship_key(resource.as_str(), relation.as_str(), subject.as_str());
        state_db.insert_raw(TableId::Relationships, &key, &[0x01])?;
        relationship_index.insert(vault, seahash(&key));
    }
}
```

rkyv archived strings implement `AsRef<str>` — binary keys are computed directly from archived references. Zero `String` allocation in the entire apply path.

### 4. Network Layer

Pipelined gRPC with cross-shard batching.

**NetworkOutbox:**

```rust
pub struct NetworkOutbox {
    destinations: HashMap<NodeId, DestinationBuffer>,
    channels: HashMap<NodeId, Channel>,
}

struct DestinationBuffer {
    messages: Vec<(ShardId, Message)>,
    in_flight: VecDeque<PipelineSlot>,
}
```

**Pipelined replication:** Leader sends entry N+1 before ACK for N arrives. Pipeline depth: configurable (default 4). Bounded in-flight prevents overwhelming slow followers. ACKs matched to oldest in-flight slot, advancing match_index.

**Cross-shard batching:** Reactor flush collects all outgoing messages, groups by destination, sends one `BatchRaftRequest` gRPC call per destination. Reuses the existing `BatchRaftRequest` proto added in Plan B.

**Vote bypass:** Vote messages (elections) skip the outbox buffer — sent immediately. Election latency is critical.

**Connection management:** One gRPC channel per destination, lazily created, cached. Failed channels trigger reconnection with exponential backoff.

### 5. Snapshot System

Per-shard snapshots for follower catch-up and log compaction.

**StateMachine trait (application implements):**

```rust
pub trait StateMachine: Send + Sync + 'static {
    /// Apply committed entries from rkyv-archived bytes.
    /// Each entry is a raw &[u8] that can be accessed via rkyv::check_archived_root.
    fn apply(&mut self, entries: &[&[u8]]) -> Vec<ApplyResult>;
    
    /// Flush dirty pages/state to durable storage (no fsync — the engine handles that).
    /// Called by the apply worker after mutations are complete.
    fn flush_dirty_pages(&mut self) -> Result<()>;
    
    /// Compute state root for integrity verification (optional).
    fn state_root(&self, vault_id: u64) -> Option<[u8; 32]>;
    
    /// Create a COW snapshot. Returns path to a consistent database file.
    /// The engine handles transfer to followers.
    fn snapshot(&self) -> Result<std::path::PathBuf>;
    
    /// Restore state from a snapshot file received from the leader.
    fn restore(&mut self, snapshot_path: &std::path::Path) -> Result<()>;
    
    /// The last applied log index.
    fn last_applied(&self) -> u64;
    
    /// Provides vault DEKs for WAL frame encryption.
    fn vault_key_provider(&self) -> &dyn VaultKeyProvider;
    
    /// Validates that an operation targets the correct shard/region.
    /// Called before proposing to consensus (pre-proposal access control).
    fn validate_access(&self, data: &[u8]) -> Result<()>;
}

/// Provides per-vault data encryption keys for WAL frame encryption.
pub trait VaultKeyProvider: Send + Sync + 'static {
    /// Returns the AES-256 DEK for a vault at a specific version.
    /// Returns None if the key has been destroyed (crypto-shredded).
    fn vault_key(&self, vault_id: u64, dek_version: u16) -> Option<[u8; 32]>;
    
    /// Returns the current DEK version for new writes.
    fn current_version(&self, vault_id: u64) -> u16;
}
```

The `flush_dirty_pages` method is called by the apply worker after mutations are complete. It writes COW pages to the database file and commits via the B+ tree's dual-slot mechanism. This runs concurrently with the reactor processing the next consensus batch — the reactor never waits for state machine persistence.

InferaDB's state layer (`StateLayer<FileBackend>`) implements this trait. The B+ tree's `flush_pages()` method (which writes dirty pages without fsync) maps directly to `flush_dirty_pages()`.

**Snapshot format:**

```
[magic: 4 bytes "LCSN"]
[version: 1 byte]
[shard_id: 8 bytes LE]
[last_included_index: 8 bytes LE]
[last_included_term: 8 bytes LE]
[membership: rkyv archived]
[state_data_len: 8 bytes LE]
[state_data: variable]
[crc32: 4 bytes]
```

**Snapshot trigger:** Configurable threshold (default 10,000 entries since last snapshot). Reactor schedules snapshot in background tokio task — doesn't block the event loop.

**Snapshot transfer:** Chunked (default 1MB chunks) via `InstallSnapshot` message. Follower writes chunks to temp file, applies on completion.

**Concurrency:** Snapshot reads from state machine (ArcSwap — lock-free). Reactor continues processing during snapshot creation.

### 6. Membership Changes

Single-step changes only (one node at a time). Simpler than joint consensus, proven safe — old and new configurations always overlap in a majority.

**Operations:**

```rust
pub enum MembershipChange {
    AddLearner { node_id: NodeId, addr: String },
    PromoteVoter { node_id: NodeId },
    DemoteVoter { node_id: NodeId },
    RemoveNode { node_id: NodeId },
}
```

**Flow:** Leader creates `Entry::Membership(new_membership)` → replicated via normal consensus → new membership takes effect at commit point → both leader and followers switch configurations atomically.

**Automatic learner promotion:** When `match_index` for a promotable learner is within `auto_promote_threshold` (default 100) entries of the leader's log length, the engine automatically proposes promotion. Application calls `add_learner` and the engine handles the full lifecycle.

**Non-promotable learners (cross-DC DR):** Learners can be added with `promotable: false`. These receive full log replication for disaster recovery but are NEVER auto-promoted to voter — their latency (cross-datacenter RTT) would destroy quorum performance. The application explicitly manages their lifecycle.

```rust
pub enum MembershipChange {
    AddLearner { node_id: NodeId, addr: String, promotable: bool },
    PromoteVoter { node_id: NodeId },  // Fails if learner is non-promotable
    DemoteVoter { node_id: NodeId },
    RemoveNode { node_id: NodeId },
}
```

**Leadership transfer:** `transfer_leadership(shard, target)` sends a `TimeoutNow` message to the target follower, triggering immediate election. Used by the application for rebalancing decisions.

**Safety:** At most one membership entry uncommitted at a time. If leader crashes during change, new leader commits or rolls back via normal log reconciliation.

**Removed node behavior:** When a node processes a committed membership entry that excludes itself, it:
1. Stops participating in elections (does not send or respond to VoteRequest/PreVote)
2. Stops accepting new proposals
3. Continues serving reads from its current applied state (eventual consistency only)
4. Shuts down after a configurable grace period (default: 30 seconds) to allow in-flight reads to drain

This prevents the "removed node disruption" problem: a removed node cannot disrupt the cluster by starting an election with a stale log. The pre-vote protocol provides additional protection — even if the removed node somehow sends a pre-vote request, active members with a current leader reject it.

## Deep Integration Optimizations

These optimizations exploit full-stack ownership — they are impossible with openraft because they break the abstraction boundary between consensus and storage.

### 7. Committed Index Gossip via Heartbeats

The Raft `AppendEntries` message already carries `leader_commit`. Followers learn the leader's committed index from every heartbeat. **The ReadIndex RPC is unnecessary in the common case.**

Follower linearizable read path:
1. Check: `local_applied_index >= leader_commit_from_last_heartbeat`
2. If yes → serve immediately (zero network overhead)
3. If no → wait for local apply via watch channel (no RPC)
4. Only if last heartbeat is stale (> 2 × heartbeat_interval) → fall back to ReadIndex RPC

With 100ms heartbeats, the RPC fallback triggers only during network partitions or leader transitions. Normal linearizable reads on followers cost zero network overhead.

The engine maintains `last_leader_commit: AtomicU64` and `last_leader_term: AtomicU64` on each follower shard, updated on every received `AppendEntries`. Readers check both atomics directly — no lock, no channel, no RPC.

**Term validation:** Followers only trust `last_leader_commit` if `last_leader_term` matches the follower's current known term. If a new leader is elected (term changes), the follower's `last_leader_commit` becomes stale — reads fall back to the ReadIndex RPC until a heartbeat arrives from the new leader. This closes the stale-read window to the time between leader election and first new-leader heartbeat (typically < heartbeat_interval = 100ms).

### 8. Events and Blocks in Unified Write Path

The current architecture uses 5 separate databases per region:
- `raft.db` (eliminated by WAL)
- `state.db` (entities, relationships, indexes)
- `blocks.db` (block archive — VaultEntries, RegionBlocks)
- `events.db` (audit trail)

Events and blocks should be part of the apply worker's write path (same B+ tree transaction as entity/relationship mutations):

**Events:** Add events as a table in state.db. Written during apply alongside entity/relationship mutations. Same B+ tree transaction, same fsync. Eliminates the separate `EventsDatabase` and its transaction overhead.

**Blocks:** Block creation (VaultEntry → RegionBlock → hash chain) currently happens AFTER apply as a separate post-processing step. Move it INTO the apply phase:

```rust
// During apply, after operations are committed:
let vault_entry = VaultEntry {
    vault, vault_height, previous_vault_hash,
    transactions, tx_merkle_root, state_root,
};
let block = RegionBlock { region, height, timestamp, vault_entries: vec![vault_entry] };
state_db.insert_block(block)?;
// Block is now part of the same transaction as entity/relationship writes
```

**Result:** 5 fsyncs → 1 on the consensus critical path (WAL only) + 1 on the apply path (state.db with entities + relationships + indexes + dictionaries + events + blocks + metadata in a single B+ tree transaction). Two total fsyncs instead of five, and they're decoupled — the apply fsync doesn't block consensus.

### 9. COW-Based Snapshots

The current snapshot system serializes the entire state into a compressed file (iterating all tables, postcard-encoding all entries, zstd-compressing). For large databases, this takes seconds to minutes.

Our B+ tree uses COW (copy-on-write) pages. A consistent snapshot is already a set of immutable page references. **Snapshot the B+ tree by copying the database file:**

```rust
// Current: serialize entire state
fn snapshot(&self) -> Result<Vec<u8>> {
    // Iterate all tables → encode → compress → Vec<u8>
    // Cost: O(state_size), seconds to minutes
}

// COW snapshot: reflink copy
fn snapshot(&self) -> Result<PathBuf> {
    let path = format!("snapshots/snapshot-{}.db", self.last_applied());
    reflink_or_copy("state.db", &path)?;
    Ok(path)
    // Cost: O(1) on btrfs/xfs/APFS, O(state_size) on ext4
}
```

**Implementation:** Use filesystem-level copy-on-write where available (NOT hard links — hard links share the same inode and don't create point-in-time copies):

```rust
fn snapshot(&self) -> Result<PathBuf> {
    let snapshot_path = format!("snapshots/snapshot-{}.db", self.last_applied());
    
    // Try reflink copy first (O(1) on btrfs, xfs, APFS)
    // Falls back to full file copy on filesystems without reflink support
    reflink_or_copy("state.db", &snapshot_path)?;
    
    Ok(snapshot_path.into())
}
```

On filesystems with reflink support (btrfs, xfs, APFS), `cp --reflink=auto` creates a COW copy in O(1) — pages are shared until one side modifies them. On ext4 or other filesystems without reflink, a full file copy is performed (O(state_size) but still correct).

**Snapshot encryption:** The COW snapshot file contains all vaults' data in plaintext. Since WAL frames are encrypted per-vault, snapshots must also be encrypted for security boundary consistency. The snapshot is encrypted with a snapshot-specific DEK derived from the RegionMasterKey before writing to disk or transferring:

```rust
fn snapshot(&self) -> Result<PathBuf> {
    let path = format!("snapshots/snapshot-{}.db", self.last_applied());
    reflink_or_copy("state.db", &path)?;
    // Encrypt the snapshot file with a derived snapshot DEK
    encrypt_file_in_place(&path, self.snapshot_dek())?;
    Ok(path.into())
}
```

This protects at-rest copies (warm tier, S3 backups) and prevents a leaked snapshot from being a full multi-tenant data breach. Followers decrypt during ingestion using the same derived key (available via the RegionMasterKey which all nodes in the region share).

**For follower transfer:** Send the encrypted snapshot file via streaming gRPC (server-streaming RPC, not per-chunk request-response). The follower stream-decrypts during ingestion. Use delta/rsync-style transfer for incremental updates when the follower already has a previous snapshot.

### 10. Fast Recovery via WAL Replay

With decoupled apply, the WAL is authoritative and state.db may lag behind. Recovery replays the gap.

**WAL commit_index checkpoints:** The WAL includes periodic checkpoint frames (not just entry frames) that record the current `committed_index`:

```
Checkpoint frame: [CHECKPOINT_MARKER: 4 bytes][committed_index: 8 bytes LE][term: 8 bytes LE][crc32: 4 bytes]
```

Checkpoints are written by the reactor after each flush cycle (alongside entry frames). On recovery, scan the last WAL segment backward to find the most recent checkpoint — this gives the `committed_index`.

**Recovery flow:**
1. Open state.db — read `last_applied` from the state machine
2. Scan last WAL segment for the most recent commit checkpoint → `committed_index`
3. If `committed_index > last_applied`: replay WAL entries from `last_applied + 1` to `committed_index`
4. Rebuild in-memory `Shard` state from WAL metadata
5. Resume consensus

**Typical replay cost:** The apply worker processes entries continuously. On crash, the gap between `committed_index` and `applied_index` is at most one flush interval (1ms) of entries — typically 0-100 entries. Replay cost: ~1ms. In the worst case (apply worker severely backed up), replay could be larger, but bounded by the snapshot threshold (10,000 entries, ~10-50ms replay).

**Entries past committed_index in the WAL are treated as uncommitted.** The WAL may contain entries that were written by the leader but never achieved quorum (leader crashed before replication). On restart, these are ignored — the Raft protocol will either re-replicate them from the new leader or they'll be overwritten.

**Snapshot safety:** Snapshots are only triggered when `applied_index >= target_snapshot_index`. This ensures the snapshot's `last_included_index` matches the state machine's actual state. The apply worker's `applied_index` (not the reactor's `committed_index`) gates snapshot creation.

### 11. Idempotency Pre-Dedup at Proposal Time

Currently, idempotency is checked during the apply phase (after consensus commits the entry). Duplicate requests waste a full consensus round-trip.

The engine checks idempotency BEFORE proposing to the reactor:

```rust
impl ConsensusEngine {
    pub async fn propose(&self, shard: ShardId, data: Vec<u8>) -> Result<u64> {
        // Extract idempotency key from the request
        let key = extract_idempotency_key(&data);
        
        // Check in-memory cache (moka, same as current fast path)
        if let Some(cached_result) = self.idempotency_cache.get(&key) {
            return Ok(cached_result); // Immediate return, no consensus
        }
        
        // Not a duplicate — proceed with consensus
        let result = self.reactor_propose(shard, data).await?;
        
        // Cache the result for future dedup
        self.idempotency_cache.insert(key, result);
        Ok(result)
    }
}
```

This saves 2-5ms per duplicate request (the full Raft pipeline latency). The moka cache is already used in the current architecture — we're just moving the check earlier.

**For cross-failover dedup:** The engine also stores idempotency entries in the replicated state (via a membership-like mechanism). When a new leader takes over, it loads the idempotency state from the state machine. This is the same as the current `ClientSequenceEntry` mechanism but natively integrated.

### 18. Async I/O via io_uring

The WAL flush cycle supports two backends, selected at runtime:

**Synchronous (default, all platforms):** `fdatasync()` blocks the reactor during fsync (~0.5-2ms). Simple, portable, correct. Used on macOS and older Linux.

**Asynchronous (Linux 5.6+, opt-in):** `io_uring` IORING_OP_FSYNC submits the fsync and returns immediately. The reactor continues processing events while the kernel completes the fsync. A completion callback marks entries as durable.

**Fsync state machine (per flush cycle):**
```
Pending → Written → Submitted → Confirmed
```

- **Pending:** Entries received from consensus, not yet in WAL buffer
- **Written:** Appended to WAL buffer (in-memory), not yet on disk
- **Submitted:** Write + fsync submitted to io_uring ring, kernel processing
- **Confirmed:** Kernel confirmed fsync complete. Entries are durable. Client proposals can be acknowledged.

The reactor tracks which entries are in each state. Only `Confirmed` entries trigger client acknowledgment and network replication messages.

```rust
struct FlushState {
    /// Entries written to WAL buffer, awaiting fsync submission.
    written: Vec<(ShardId, Vec<Entry>, Vec<oneshot::Sender<ProposeResult>>)>,
    /// Entries with fsync submitted to io_uring, awaiting kernel confirmation.
    in_flight: Vec<(ShardId, Vec<Entry>, Vec<oneshot::Sender<ProposeResult>>)>,
}
```

**Reactor flush cycle with io_uring:**
```rust
async fn flush(&mut self) {
    // 1. Move current batch to written state, append to WAL
    self.wal.write_batch(&self.pending)?;
    self.flush_state.written.extend(self.pending.drain(..));
    
    // 2. Submit fsync for all written entries
    if !self.flush_state.written.is_empty() {
        self.wal.submit_fsync_async()?;  // io_uring submission, returns immediately
        self.flush_state.in_flight.extend(self.flush_state.written.drain(..));
    }
    
    // 3. Check for completed fsyncs from previous cycles
    while let Some(completed) = self.wal.poll_fsync_completion() {
        // Entries are now durable — acknowledge clients and send replication
        for (shard, entries, senders) in self.flush_state.in_flight.drain(..) {
            self.outbox.enqueue_replication(shard, &entries);
            for sender in senders {
                let _ = sender.send(Ok(commit_index));
            }
        }
    }
}
```

**Feature flag:**
```toml
# Cargo.toml
[features]
default = []
io-uring = ["dep:io-uring"]

[target.'cfg(target_os = "linux")'.dependencies]
io-uring = { version = "0.7", optional = true }
```

**Fallback:** Without the `io-uring` feature (or on non-Linux), the reactor uses `fdatasync()` synchronously. The state machine collapses to `Written → Confirmed` (no `Submitted` intermediate state).

**Crash recovery invariant:** On startup, ANY WAL entries past the last checkpoint's `committed_index` are treated as uncommitted, regardless of whether they're physically present on disk. This handles the case where io_uring's fsync completed in the kernel after the process crashed but before the completion was polled — the entries are durable on disk but were never acknowledged. The Raft protocol will either re-replicate them from the current leader or overwrite them with new entries.

**Quantified benefit:** At 1ms fsync latency and 1ms flush interval, the reactor spends ~50% of time blocked on synchronous fsync. With io_uring, this drops to ~0% — the reactor processes events continuously while fsync completes in parallel. Write throughput approximately doubles in fsync-bound workloads.

### 19. Region Split and Merge

Automatic sub-region sharding for write throughput scaling and large tenant isolation.

**Problem:** All organizations in a region share one shard. A large tenant (100M relationships) monopolizes the write pipeline, starving small tenants. A single leader handles all writes for the region, capping throughput at ~10,000-50,000 writes/sec.

**Solution:** When a shard exceeds a configurable threshold, split it into two shards. Each shard gets its own leader, WAL, state.db, and write throughput. Merge when adjacent shards shrink.

**Split operation:**

Split is proposed as a special Raft log entry, committed via normal consensus:

```rust
pub enum Entry {
    Normal { data: Vec<u8> },
    Membership(Membership),
    Split {
        /// The organization boundary to split on.
        split_key: OrganizationId,
        /// ShardId for the new (right) shard.
        new_shard: ShardId,
        /// Initial membership for the new shard (same nodes as parent shard).
        membership: Membership,
    },
}
```

**Split flow:**
1. **Trigger:** Application detects a shard exceeds `max_shard_size` (default: 100,000 active relationships across all vaults) or a single tenant exceeds `max_tenant_size`
2. **Select split point:** Choose an organization boundary. All organizations with `id >= split_key` move to the new shard. Organizations are sorted by ID for deterministic splitting.
3. **Propose split entry:** Leader proposes `Entry::Split` via normal consensus
4. **All nodes apply atomically:** On commit, each node:
   - Creates a new `Shard` with `new_group` ID
   - Migrates organizations with `id >= split_key` to the new shard
   - Creates new state.db for the right shard (via B+ tree range copy)
   - Updates routing table: `organization → shard` mappings for migrated orgs
   - Starts new shard with the same membership (all nodes participate in both groups)
5. **New shard elects leader:** Independent election in the new shard. May elect a different leader than the parent.
6. **Router updated:** `ConsensusEngine` maintains a `router: HashMap<OrganizationId, ShardId>` updated on split/merge. The application queries the router to find the correct shard for each operation.

**Two-phase state migration:**

Split state migration must NOT happen inline during apply (it would block the apply worker for seconds at scale). Instead, it uses a two-phase approach:

**Phase 1 — Freeze and route (instant, during apply of Split entry):**
1. Update the router: migrated organizations now route to the new shard
2. Freeze writes to migrated organizations on the original shard (proposals rejected with `MOVED`)
3. The new shard starts accepting proposals for migrated organizations immediately — writes go to the new shard's WAL, which is initially empty
4. Reads for migrated organizations are served from the ORIGINAL shard's state.db until migration completes (the new shard's apply worker hasn't built state yet)

**Phase 2 — Background migration (async, does not block apply):**
1. A background task scans the original state.db for keys belonging to migrated organizations
2. Copies entries to the new shard's state.db via the new shard's apply worker (as synthetic apply entries)
3. After migration completes, reads switch to the new shard
4. Original shard's copies are deleted (deferred cleanup)

**Consistency during migration:** Between phase 1 and phase 2 completion:
- Writes to migrated orgs go to the new shard (correct — new state)
- Reads for migrated orgs may come from the old shard (stale but consistent at a point-in-time)
- Once migration completes, reads switch to the new shard (fully current)

This is acceptable because split is an operator-initiated action (not latency-critical), and the migration window is bounded by state size (typically seconds to minutes).

**In-flight request handling:** Requests arriving during/after split:
- Requests for organizations on the original shard proceed normally
- Requests for organizations routed to the new shard: writes go to new shard, reads served from old shard until migration completes
- The `ConsensusEngine::propose()` checks the router before proposing — if the organization moved, it routes to the new shard

**Merge operation:**

Merge is the reverse of split. When two adjacent groups are both small (combined size below `min_group_size`), the application proposes a merge:

```rust
pub enum Entry {
    // ...
    Merge {
        /// The shard to merge into this one.
        source_shard: ShardId,
    },
}
```

Merge is preceded by a **drain phase**: (1) source shard stops accepting new proposals (returns `MOVED` to target shard), (2) waits for all in-flight proposals to commit or timeout, (3) transfers leadership if needed. Only after drain completes is the `Merge` entry proposed on the target shard. This ensures no uncommitted entries are lost.

The source shard's committed state is migrated into the target shard, the source shard is shut down, and the router is updated. Merge is less critical than split (small shards work fine, they just waste a bit of resources) — it's a housekeeping optimization.

**Configuration:**
```rust
pub struct SplitConfig {
    /// Enable automatic split detection.
    pub enabled: bool,                    // default: false (manual splits only)
    /// Maximum relationships across all vaults in a shard before split is triggered.
    pub max_shard_relationships: u64,     // default: 10_000_000
    /// Maximum relationships for a single organization before isolation split.
    pub max_tenant_relationships: u64,    // default: 1_000_000
    /// Minimum shard size below which merge is considered.
    pub min_shard_relationships: u64,     // default: 100_000
}
```

**Default: disabled.** Split is an advanced feature for large deployments. Most deployments won't need it — a single shard per region handles millions of authorization checks/sec (reads are the bottleneck, and reads scale horizontally via ReadIndex).

**When split matters:** A single organization with >1M relationships doing frequent bulk writes (import, migration, policy changes) that starves other tenants in the same region.

## Data Isolation Enhancements

These exploit full-stack ownership to provide stronger security boundaries than any external consensus library can offer.

### 12. Per-Vault WAL Frame Encryption

Each WAL frame is encrypted with the vault's data encryption key (DEK) using AES-256-GCM before writing. The existing envelope encryption hierarchy (`RegionMasterKey` → per-vault DEK) provides the keys.

**Enhanced WAL frame format:**
```
[shard_id: 8 bytes LE]
[vault_id: 8 bytes LE]
[dek_version: 2 bytes LE]
[entry_len: 4 bytes LE]
[AES-256-GCM nonce: 12 bytes (random via OsRng)]
[encrypted rkyv entry: variable]
[GCM auth tag: 16 bytes]
```

The `dek_version` field identifies which DEK encrypted this frame. After DEK rotation (itself a committed Raft entry so all nodes rotate synchronously), new frames use the new version. During recovery, the `VaultKeyProvider` resolves `vault_key(vault_id, dek_version)` to decrypt each frame with the correct historical key.

GCM auth tag provides cryptographic integrity (stronger than CRC). CRC is omitted from encrypted frames. For unencrypted frames (if encryption is disabled), the original CRC-based format is used. Nonces are generated randomly via `OsRng` (never sequential) to prevent nonce reuse across DEK rotation boundaries.

**Cost:** ~100ns per frame for AES-256-GCM (hardware-accelerated via AES-NI on modern CPUs). Negligible compared to fsync (~500,000ns).

**Benefits:**
- WAL file compromise doesn't expose plaintext data — each vault's entries are independently encrypted
- Crypto-shredding is instant: destroy the vault's DEK, all WAL frames become unrecoverable
- GCM authentication tag detects tampering at the frame level (in addition to CRC)

**Key management:** The engine receives a `VaultKeyProvider` trait from the application:
```rust
pub trait VaultKeyProvider: Send + Sync + 'static {
    /// Returns the DEK for encrypting/decrypting a vault's WAL frames.
    fn vault_key(&self, vault_id: u64) -> Option<[u8; 32]>;
}
```

### 13. Consensus-Level Vault Access Control

The engine validates vault access BEFORE proposing to the reactor. Operations targeting the wrong region or violating key tier rules are rejected immediately — the consensus log never contains invalid data.

```rust
impl ConsensusEngine {
    pub async fn propose(&self, shard: ShardId, data: Vec<u8>) -> Result<u64> {
        // 1. Pre-proposal idempotency check (Section 11)
        // 2. Pre-proposal access control validation
        self.validate_vault_access(&data, shard)?;
        // 3. Propose to reactor
        self.reactor_propose(shard, data).await
    }
}
```

The `validate_vault_access` check ensures:
- The operation's target vault belongs to this shard's region
- Key tier rules are satisfied (Global keys → GLOBAL region only, Regional keys → data region only)
- The organization is Active (not Suspended or Deleted)

**Benefit:** Invalid operations are rejected at the API boundary instead of during apply. Saves consensus round-trip for invalid requests and ensures the replicated log is always clean.

### 14. WAL-Level Crypto-Shredding

When an organization is deleted (crypto-shredded), the engine zeros out their WAL frames in active segments without rewriting the file:

```rust
impl SharedWal {
    /// Zeros out all WAL frames for a vault, making data unrecoverable.
    /// Called during organization deletion after the DEK is destroyed.
    fn shred_vault_frames(&mut self, vault_id: u64) -> Result<u64> {
        let mut shredded_count = 0;
        for segment in &mut self.segments {
            for frame in segment.frames_for_vault(vault_id) {
                // Zero the encrypted payload + auth tag
                frame.zero_payload()?;
                shredded_count += 1;
            }
        }
        Ok(shredded_count)
    }
}
```

**Combined with DEK destruction:** The vault's DEK is already destroyed during crypto-shredding. WAL frame zeroing is defense-in-depth — even if a backup of the WAL exists with the DEK somehow preserved, the frames themselves are zeroed.

## Consensus-Verified Cryptographic Ledger

These enhancements integrate the blockchain/ledger system directly into the consensus protocol, turning it from an application-level construct into a consensus-level invariant.

### 15. Consensus-Verified Block Hashes

The leader computes the block hash and state root and includes them in the `AppendEntries` message. Followers verify their independently-computed values match. Disagreement is a byzantine fault detected at consensus time, not by a background job.

**Enhanced Entry type:**
```rust
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,                 // rkyv-archived operations
    pub expected_block_hash: Hash,     // Leader's computed block hash
    pub expected_state_root: Hash,     // Leader's computed state root
}
```

**Follower verification during apply:**
```rust
fn apply(&mut self, entry: &ArchivedEntry) -> ApplyResult {
    // Apply operations to state machine...
    let my_state_root = self.compute_state_root(vault)?;
    let my_block_hash = self.compute_block_hash(entry, my_state_root)?;
    
    // Consensus-level integrity check
    if my_block_hash != entry.expected_block_hash
        || my_state_root != entry.expected_state_root
    {
        return ApplyResult::Diverged {
            expected_block_hash: entry.expected_block_hash,
            actual_block_hash: my_block_hash,
            expected_state_root: entry.expected_state_root,
            actual_state_root: my_state_root,
        };
    }
    
    ApplyResult::Committed { block_hash: my_block_hash }
}
```

**Impact:** Divergence detection drops from "whenever the background verification job runs" to "within one consensus round-trip." Any silent data corruption (bitflip, bug, hardware fault) is caught immediately.

**Safety:** This doesn't change Raft correctness. The leader computes hashes deterministically from the operation list. All correct followers compute the same hash. A faulty follower that computes a different hash is detected but doesn't affect quorum (the leader and other correct followers proceed). The faulty follower triggers recovery.

### 16. Parallel Block Hash Computation

Block hashes depend on the operation list and previous_hash — NOT on the apply results (except state_root). The engine can parallelize:

```
┌─ State machine apply (B+ tree writes)  ─────────────── time ──►
│
├─ tx_merkle_root computation (parallel with apply)  ──────────►
│
├─ block_hash computation (after tx_merkle_root + state_root)  ─►
│
└─ State root computation (after apply)  ──────────────────────►
```

The `tx_merkle_root` is computed from the transaction list (no dependency on apply). It can run on a separate thread/task while apply runs on the reactor. The `block_hash` waits for both `tx_merkle_root` and `state_root`, then computes the final hash.

**Implementation:** The reactor spawns the Merkle computation as a `tokio::task::spawn_blocking` before starting apply. After apply completes (which produces state_root), the Merkle result is awaited (usually already complete) and `block_hash` is computed.

### 17. Pre-Computed Merkle Proofs During Apply

When the engine applies a write (CreateRelationship, SetEntity), it knows exactly which entity was modified and which bucket was affected. The engine pre-computes the Merkle proof path (from the entity's leaf to the state root) and caches it.

```rust
pub struct ApplyResult {
    pub status: WriteStatus,
    /// Pre-computed Merkle proof for the modified entity.
    /// Available immediately after commit — no traversal needed for verified reads.
    pub merkle_proof: Option<MerkleProof>,
}
```

**Benefit:** When a client requests a verified read immediately after a write, the proof is already cached. No B+ tree traversal or Merkle path computation needed. This turns verified reads from "compute proof on demand" (~50-200μs) to "return cached proof" (~50ns).

**Cache lifecycle:**
- Populated during apply (when the engine knows exactly which entities changed)
- Evicted when the next block commits (proofs are height-specific)
- Bounded size: at most `max_entries_per_block × proof_size` (~100KB per block)

## Public API

```rust
/// Type alias for production use.
pub type LedgerEngine = ConsensusEngine<SystemClock, SystemRng, FileWalBackend>;

pub struct ConsensusEngine<C: Clock, R: RngSource, W: WalBackend> {
    inbox: mpsc::Sender<ReactorEvent>,
    // ... clock, rng, wal_backend held by the reactor
}

impl<C: Clock, R: RngSource, W: WalBackend> ConsensusEngine<C, R, W> {
    pub async fn start(config: EngineConfig, clock: C, rng: R, wal: W) -> Result<Self>;
    pub async fn propose(&self, shard: ShardId, data: Vec<u8>) -> Result<u64>;
    pub async fn propose_batch(&self, shard: ShardId, entries: Vec<Vec<u8>>) -> Result<u64>;
    pub async fn read_index(&self, shard: ShardId) -> Result<u64>;
    pub async fn add_shard(&self, shard: ShardId, config: ShardConfig, sm: Box<dyn StateMachine>) -> Result<()>;
    pub async fn remove_shard(&self, shard: ShardId) -> Result<()>;
    pub async fn add_learner(&self, shard: ShardId, node: NodeId, addr: String) -> Result<()>;
    pub async fn promote_voter(&self, shard: ShardId, node: NodeId) -> Result<()>;
    pub async fn remove_node(&self, shard: ShardId, node: NodeId) -> Result<()>;
    pub async fn transfer_leadership(&self, shard: ShardId, target: NodeId) -> Result<()>;
    pub async fn shutdown(self) -> Result<()>;
}
```

## Configuration

```rust
pub struct EngineConfig {
    pub node_id: NodeId,
    pub data_dir: PathBuf,  // --data
    pub wal: WalConfig,
    pub network: NetworkConfig,
    pub default_group: ShardConfig,
}

pub struct WalConfig {
    pub segment_size_bytes: u64,     // default: 64MB
    pub flush_interval_ms: u64,     // default: 1ms
}

pub struct NetworkConfig {
    pub listen_addr: String,
    pub pipeline_depth: usize,      // default: 4
    pub batch_flush_ms: u64,        // default: 1ms (aligned with WAL flush)
}

pub struct ShardConfig {
    pub election_timeout_min_ms: u64,    // default: 300
    pub election_timeout_max_ms: u64,    // default: 600
    pub heartbeat_interval_ms: u64,      // default: 100
    pub snapshot_threshold: u64,         // default: 10000
    pub auto_promote_threshold: u64,     // default: 100
    pub max_entries_per_rpc: u64,        // default: 1000
}
```

## Crate Structure

```
crates/consensus/
├── Cargo.toml
├── src/
│   ├── lib.rs           — Public API (ConsensusEngine)
│   ├── core.rs          — Shard, Message, Action
│   ├── reactor.rs       — Multi-shard event loop
│   ├── wal.rs           — SharedWal, segmented log, frame encryption
│   ├── crypto.rs        — WAL frame encryption, VaultKeyProvider, crypto-shredding
│   ├── network.rs       — NetworkOutbox, pipelined replication
│   ├── snapshot.rs      — COW snapshot creation, transfer, restoration
│   ├── membership.rs    — Single-step membership changes
│   ├── timer.rs         — Timer wheel for deadlines
│   ├── block.rs         — Block creation, hash chain, consensus-verified hashes
│   ├── merkle_cache.rs  — Pre-computed Merkle proofs during apply
│   ├── io_uring.rs      — Async fsync via io_uring (Linux 5.6+, feature-gated)
│   ├── split.rs         — Region split/merge operations
│   ├── router.rs        — Organization → ShardId routing table
│   ├── apply_worker.rs  — Decoupled state machine apply (separate from reactor)
│   ├── entry_cache.rs   — In-memory ring buffer of recent WAL entries
│   ├── closed_ts.rs     — Closed timestamp tracking and follower reads
│   ├── circuit_breaker.rs — Per-shard circuit breakers
│   ├── alarm.rs         — Cluster-wide alarm system (NearDiskFull, IntegrityFailure)
│   ├── bootstrap.rs     — Cluster bootstrap (Init RPC, cluster_id generation)
│   ├── gossip.rs        — Node discovery and cluster_id propagation
│   ├── clock.rs         — Clock trait (SystemClock, SimulatedClock)
│   ├── rng.rs           — RngSource trait (SystemRng, SimulatedRng)
│   ├── wal_backend.rs   — WalBackend trait (FileWalBackend, SimulatedWalBackend)
│   ├── simulation.rs    — Simulation harness (test-only, behind #[cfg(test)])
│   ├── types.rs         — ShardId, NodeId, Entry, Membership
│   └── error.rs         — ConsensusError
└── tests/
    ├── election.rs      — Leader election, pre-vote protocol, split-vote recovery
    ├── replication.rs   — Log replication, pipelining, conflict resolution
    ├── commit.rs        — Quorum commit, leader lease renewal
    ├── wal.rs           — WAL write, read, recovery, compaction
    ├── encryption.rs    — Per-vault frame encryption, crypto-shredding, tamper detection
    ├── snapshot.rs      — COW snapshot create, transfer, restore
    ├── membership.rs    — Add/remove/promote, automatic promotion
    ├── blocks.rs        — Block chain, consensus-verified hashes, divergence detection
    ├── split.rs         — Region split/merge, state migration, router updates
    ├── io_uring.rs      — Async fsync lifecycle, completion handling (Linux only)
    ├── multi_group.rs   — Reactor batching, cross-shard I/O, timer wheel
    └── simulation/
        ├── mod.rs           — Simulation harness setup
        ├── linearizability.rs — Stale reads, partition recovery, lease expiry
        ├── split_merge.rs   — Split/merge under crash, partition, clock skew
        ├── durability.rs    — WAL corruption, sync failures, recovery paths
        └── closed_ts.rs     — Closed timestamp correctness under leader transitions
```

## Testing Strategy

- **Core:** Unit tests for election (including pre-vote protocol), replication, commit — same as benchmark prototype but more thorough (conflict resolution, term changes, log truncation, split votes, pre-vote preventing disruption from partitioned nodes)
- **WAL:** Write/read roundtrip, crash recovery (write partial frame, recover), segment rotation, compaction after snapshot, CRC validation
- **Reactor:** Multi-shard WAL batching (verify one WAL fsync for N shards), timer wheel accuracy, graceful shutdown, apply worker dispatch non-blocking
- **Decoupled apply:** Verify reactor continues processing while apply worker is busy. Verify reads wait correctly via wait_for_apply. Verify crash between WAL commit and apply completion recovers correctly (WAL replay of unapplied entries). Verify entry cache serves recent entries without WAL disk read.
- **Durability:** Crash at every point in the flush cycle — verify WAL is consistent and state.db recovers via replay. Kill process during WAL fsync, verify no acknowledged entries are lost.
- **Network:** Pipeline in-flight tracking, ACK matching, NACK rewind, cross-shard batch composition
- **Committed index gossip:** Verify followers learn committed index from heartbeats. Verify linearizable reads on followers succeed without ReadIndex RPC under normal operation.
- **Snapshot:** COW snapshot creation (verify hard-link is consistent), transfer to follower, restore from snapshot file, verify state matches
- **Fast recovery:** Kill process, restart, verify WAL replay of unapplied entries produces correct state.db. Measure replay time (target: <10ms for 100 entries). Verify entries past committed_index in WAL are ignored.
- **Membership:** Add learner → auto promote, remove voter, leadership transfer, reject concurrent changes
- **Idempotency pre-dedup:** Verify duplicate proposals are rejected before consensus. Verify cross-failover dedup after leader change.
- **Events + blocks in unified write:** Verify events and blocks are persisted atomically with entity/relationship writes. Crash during apply — verify no partial event/block state.
- **WAL encryption:** Verify frames are encrypted with vault DEK. Verify different vaults' frames use different keys. Verify crypto-shredded vault's frames are undecryptable. Verify GCM auth tag catches tampering.
- **Access control:** Verify pre-proposal rejection of wrong-region operations. Verify key tier validation at engine level.
- **Crypto-shredding:** Verify shred_vault_frames zeros payload in active segments. Verify zeroed frames are skipped during WAL replay.
- **Consensus-verified blocks:** Verify leader and follower compute identical block hashes. Inject a bitflip in one follower's state — verify divergence detected at consensus time. Verify diverged follower triggers recovery.
- **Parallel block hashing:** Verify tx_merkle_root computed correctly in parallel with apply. Verify block_hash matches sequential computation.
- **Pre-computed Merkle proofs:** Verify cached proof validates against state root. Verify proof is available immediately after commit. Verify eviction on next block.
- **io_uring:** Verify async fsync lifecycle (Pending → Written → Submitted → Confirmed). Verify entries not acknowledged before fsync confirmed. Verify fallback to fdatasync on non-Linux. Verify throughput improvement vs synchronous path.
- **Region split:** Verify split entry commits via normal consensus. Verify state migration copies correct organizations. Verify router updates atomically. Verify in-flight requests to moved organizations get MOVED error. Verify new shard elects leader independently.
- **Region merge:** Verify merge migrates state back. Verify source shard is shut down. Verify router update.
- **Integration:** Full propose → commit → apply → block creation → event emission cycle with real WAL and simulated network
- **Closed timestamps:** Verify follower serves reads within closed_ts without leader contact. Verify closed_ts advances with leader heartbeats. Verify stale reads correctly fall back to ReadIndex when closed_ts is too old.
- **Circuit breakers:** Verify shard enters Open state after timeout. Verify writes rejected, reads continue. Verify auto-close when shard resumes.
- **Cluster alarms:** Verify NearDiskFull puts cluster in read-only. Verify alarm propagates via Raft to all nodes. Verify clear restores write capability.
- **Bootstrap:** Verify init creates GLOBAL shard and first region shard. Verify idempotent (second init returns AlreadyInitialized). Verify cluster_id rejection for cross-cluster messages. Verify restart ignores --join flags.
- **Deterministic simulation:** Run 10,000+ seeded simulations with fault injection (partitions, crashes, clock skew, slow disk, WAL corruption). Verify linearizability of every recorded history. Reproduce any failure by replaying its seed.
- **Linearizability (Jepsen-style):** Run cluster under partitions + crashes + clock skew. Record operation history. Verify linearizable using Knossos-style checker.
- **Loom atomics:** Exhaustive interleaving tests for LeaderLease, ArcSwap state, committed_index gossip, apply worker dispatch.
- **Property tests:** Same operations on two independent engines produce identical state (determinism). Encrypted WAL roundtrips correctly. Block hashes are deterministic across nodes. Split + merge roundtrip preserves all data.

## Raft Spec Deviations

1. **No joint consensus for membership changes.** Single-step changes only (one node at a time). Safe because old and new configurations always share a majority. Simplifies implementation significantly.

2. **Event-driven instead of tick-driven.** Standard Raft ticks at a fixed interval. Our reactor is event-driven — groups only process when they have work (proposals, messages, timer expiry). Semantically equivalent but eliminates polling for idle shards.

3. **Pipelined replication.** Standard Raft waits for ACK before sending next batch. We send up to `pipeline_depth` batches before waiting. Safe because Raft's log matching property ensures followers reject entries with incorrect prev_log — the leader detects conflicts and rewinds.

4. **Batched persistence across shards.** Standard Raft assumes per-shard durable storage. We batch fsync across shards for performance. Safe because each shard's entries are independently framed and CRC-checked — one shard's corruption doesn't affect others. Recovery replays each shard's entries independently.

5. **WAL-only fsync on consensus critical path.** Standard Raft implementations fsync both the log and state machine. Our reactor fsyncs only the WAL during the consensus path. The apply worker commits state.db asynchronously via the B+ tree's dual-slot mechanism. On crash, replay `committed_index - applied_index` entries from WAL (typically ~1ms). This eliminates 4 of 5 fsyncs from the current architecture's consensus critical path. The WAL is the authoritative source for recovery and replication.

6. **Zero-copy apply via rkyv archives.** Standard Raft implementations deserialize log entries before applying. We apply directly from rkyv-archived bytes — field access is a pointer cast, not byte-by-byte reconstruction. Safe because rkyv archives are validated on receipt (`check_archived_root`) before any field access.

7. **Automatic learner promotion.** Not in the Raft spec (which doesn't define learners). Our engine auto-promotes learners when they're caught up within a configurable threshold.

8. **Region split/merge via consensus.** Not in the Raft spec. Split is proposed as a special log entry committed through normal Raft consensus. All nodes apply the split deterministically at the same log index — creating a new shard, migrating state, and updating routing atomically. Safe because the split is a committed Raft entry; if a node crashes mid-split, it replays the split entry on recovery.

9. **Decoupled apply.** Standard Raft applies committed entries synchronously before processing the next batch. Our reactor dispatches committed entries to a separate apply worker and continues processing immediately. Safe because: (a) WAL durability guarantees entries survive crashes, (b) the ReadIndex/wait_for_apply mechanism ensures reads see applied state, (c) recovery replays any committed-but-unapplied entries from the WAL.

## Integration with InferaDB B+ Tree

The custom engine is designed for deep integration with InferaDB's existing B+ tree storage engine (`inferadb-ledger-store`). Key integration points:

- **WAL replaces RaftLog B+ tree table.** The current architecture stores Raft log entries in a B+ tree (`tables::RaftLog`), which is suboptimal — B+ trees are designed for random-access reads, not append-only logs. The segmented WAL provides optimal sequential write performance.

- **State machine writes use the existing B+ tree.** The `StateMachine::apply()` method writes to the same B+ tree engine (entities, relationships, indexes) that the current `StateLayer` uses. No new storage engine needed.

- **Flush coordination via `flush_dirty_pages()`.** The B+ tree's existing `flush_pages()` method (which writes COW dirty pages without fsync) is called by the reactor during the unified flush cycle. The reactor handles the fsync.

- **Relationship hash index updated during apply.** The `DashMap<VaultId, DashMap<u64, ()>>` relationship index (already implemented) is populated during the apply step, providing O(1) authorization checks without B+ tree traversal.

- **State root computed during apply.** Currently deferred to a separate step after state.db commit. State root computation now happens in the apply worker alongside B+ tree mutations, persisted in the same B+ tree transaction. Consensus-verified block hashes (Section 15) ensure all nodes agree on the state root.

- **Batch-apply with sorted inserts.** When the reactor commits a batch of 100 entries, the state machine receives all mutations at once. Instead of applying them in log order (random B+ tree access), the state machine sorts all mutations by `(table_id, key)` before applying. This converts random page access into sequential access — minimizing page splits, maximizing cache hits, and reducing write amplification. This is impossible with openraft (entries arrive one at a time); the custom engine's batch-apply path makes it natural.

  ```rust
  fn apply(&mut self, entries: &[&[u8]]) -> Vec<ApplyResult> {
      // 1. Collect all mutations from all entries
      let mut mutations: Vec<(TableId, Vec<u8>, Vec<u8>)> = Vec::new();
      for entry in entries {
          let archived = rkyv::check_archived_root::<Entry>(entry)?;
          mutations.extend(self.extract_mutations(archived));
      }
      
      // 2. Sort by (table, key) — sequential B+ tree access
      mutations.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
      
      // 3. Apply in sorted order — minimal page splits, maximal cache reuse
      for (table, key, value) in &mutations {
          self.state_db.insert_raw(*table, key, value)?;
      }
      
      // 4. Update hash index, compute state root, etc.
  }
  ```

## Cluster Bootstrap

Replaces the current `--single`, `--cluster`, `--peers` flags with a CockroachDB-style two-phase bootstrap.

### 20. Start + Init Pattern

Replaces the current `--single`, `--cluster`, `--peers` flags with a streamlined two-phase bootstrap inspired by CockroachDB.

**CLI flags:**
- `--data` — persistent storage directory. Node identity, cluster membership, and all state are stored here.
- `--listen` — bind address for gRPC (default: `0.0.0.0:9090`)
- `--join` — comma-separated list of seed addresses (hostnames or IPs with ports) for initial discovery. NOT an exhaustive peer list — just "introductions." The node connects to any one seed, learns the full cluster membership via Raft state replication, and discovers all other nodes automatically. One address is sufficient (e.g., `--join=node1:9090`), though listing multiple improves availability during initial discovery. Optional — if omitted, the node listens for incoming connections (valid for the first node in a cluster). On restart, the full peer list is read from persisted Raft membership state; `--join` is ignored unless all persisted peers are unreachable (then used as fallback seeds).
- `--region` — (optional) comma-separated list of regulated data residency regions this node serves (e.g., `ie-east-dublin`). Determines which regulated region shards this node participates in. Not needed for non-regulated regions — ALL nodes automatically participate in those. Persisted on first boot, ignored on restart.
- `--advertise` — (optional) the address this node advertises to peers as its reachable address. Defaults to the hostname peers used to connect (from `--join`). Only needed for unusual network setups (NAT, multi-homed hosts, containers with different internal/external addresses). Persisted on first boot.

**Startup:**
```bash
# Non-regulated node (Virginia) — no --region needed:
inferadb-ledger start \
  --data=/data \
  --listen=0.0.0.0:9090 \
  --join=dublin1:9090,virginia1:9090,oregon1:9090

# Regulated node (Dublin) — --region opts into ie-east-dublin shard:
inferadb-ledger start \
  --data=/data \
  --listen=0.0.0.0:9090 \
  --join=dublin1:9090,virginia1:9090,oregon1:9090 \
  --region=ie-east-dublin

# Restart (all flags read from --data, only --data needed):
inferadb-ledger start --data=/data
```

**Bootstrap (once per cluster lifetime, from anywhere, targeting any node):**
```bash
inferadb-ledger init --host=virginia1:9090
```

This is a one-time operation that creates the cluster. It is never run again — not when adding nodes, not on restart, not when creating new regions. All subsequent cluster changes happen via Raft consensus (admin RPCs, automatic membership).

**Node states:**
```
Uninitialized → [receives Init RPC] → Bootstrapping → [GLOBAL shard created] → Running
Uninitialized → [receives gossip with cluster_id] → Joining → [replication starts] → Running
Running → [restart] → Running (reads identity from data-dir)
```

**How it works:**
1. `start`: Node enters `Uninitialized` state. If `--join` is provided, connects to those addresses. If not, listens for incoming connections (valid for the first node — other nodes' `--join` lists will point here). Exchanges handshake with discovered peers (NodeId, cluster_id if known, advertise address, region). Rejects all client RPCs except `Init`. Serves health probes with `NOT_SERVING`.
2. `init`: Sends `Bootstrap` RPC to one node. That node creates the GLOBAL shard (all known nodes as voters), generates root signing keys, writes a `cluster_id` (random UUID). Returns the cluster ID.
3. Other nodes learn the `cluster_id` via the handshake from the bootstrapped node. They transition to `Joining`, receive initial Raft replication, and become `Running`.
4. **On restart:** The node reads its `NodeId`, `cluster_id`, `region`, `advertise` address, and shard assignments from `--data`. Reconnects to peers using addresses from persisted Raft membership state. The `--join`, `--region`, and `--advertise` flags are ignored if data-dir already has state (but harmless to pass — simplifies automation scripts and container orchestration).
5. **Adding a new node:** Same `start` command with `--join` pointing at any running node. The cluster detects the new node via handshake and automatically adds it to:
   - GLOBAL shard (always — every node participates)
   - All non-regulated region shards (always — replicated everywhere for availability)
   - The regulated region shard matching the node's `--region` (if one exists and has been created by admin)

**Shard participation model:**

Each node participates in multiple shards based on its region:

```
Node "virginia1" (no --region):
├── GLOBAL shard              ← always (every node)
├── us-east-va shard          ← non-regulated, all nodes
├── us-west-or shard          ← non-regulated, all nodes
└── (any other non-regulated) ← all nodes

Node "dublin1" (--region=ie-east-dublin):
├── GLOBAL shard              ← always (every node)
├── us-east-va shard          ← non-regulated, all nodes
├── us-west-or shard          ← non-regulated, all nodes
├── ie-east-dublin shard      ← regulated, ONLY nodes with --region=ie-east-dublin
└── (NOT de-central-frankfurt) ← regulated, only --region=de-central-frankfurt nodes
```

Non-regulated regions (those where `Region::requires_residency()` returns false) are replicated across ALL nodes. Regulated regions restrict membership to nodes in the correct geographic region.

**Region shard lifecycle:**

Shards are created lazily — they materialize when first needed, not at bootstrap time.

- **GLOBAL shard:** Created during `init`. All nodes participate. Always exists.
- **Non-regulated region shards:** Created automatically when the first organization in that region is created. All nodes in the cluster participate as voters (these regions replicate everywhere).
- **Regulated region shards:** Created automatically when the first organization in a regulated region is created. ONLY nodes that declared `--region=<that-region>` participate as voters. The shard is created with however many matching nodes are available — even 1 (though the engine warns about fault tolerance). As more nodes with the same region join, they're added as learners and auto-promoted, growing the shard naturally. If no nodes have declared the required region, org creation fails with `NoNodesForRegion { region }`.

**Shard voter count guidance:**
- **1 voter:** Functional but zero fault tolerance. Logged as `WARN`. Suitable for dev/test or initial deployment of a new region before more nodes are provisioned.
- **2 voters:** Functional but WORSE than 1 — requires both nodes up for writes, with no fault tolerance advantage. Logged as `WARN: 2-node shard has no fault tolerance advantage over 1, recommend adding a 3rd node`.
- **3 voters:** Recommended minimum for production. Tolerates 1 node failure.
- **4 voters:** Functional, same fault tolerance as 3. Logged as `INFO: even voter count, consider 3 or 5`.
- **5 voters:** Tolerates 2 node failures. Recommended for critical regulated regions.
- **Late-joining nodes:** When a new node joins with `--region=ie-east-dublin` and an `ie-east-dublin` shard already exists, the node is automatically added as a learner to that shard, then auto-promoted to voter once caught up. The node is also added to GLOBAL and all non-regulated shards.
- **Late-joining nodes without regulated shards:** When a new node joins with `--region=ie-east-dublin` but no `ie-east-dublin` shard exists yet (no orgs in that region), the cluster records the node's region declaration. When an org is later created in `ie-east-dublin`, the shard is created with all available nodes for that region.

**Node identity:**
- `NodeId`: Random u64 generated on first boot, persisted in data-dir. The internal identity.
- Advertise address: The hostname:port peers use to connect. Stored in Raft membership. Defaults to the address from `--join` that the peer connected from. Overridden by `--advertise` for NAT/container scenarios.
- Region(s): Persisted from `--region` on first boot (empty if not specified). Used by the cluster to determine shard membership for regulated regions. Nodes without `--region` participate only in GLOBAL + non-regulated shards.

**Init is idempotent:** Second call returns `AlreadyInitialized { cluster_id }`. No corruption.

**Split-brain prevention:** A node in `Uninitialized` state NEVER self-bootstraps. Only an explicit `init` RPC triggers bootstrap.

**Cluster ID validation:** Every Raft message includes the `cluster_id`. Nodes reject messages from different clusters. Prevents cross-cluster contamination.

### 21. Closed Timestamps for Zero-Hop Follower Reads

Inspired by CockroachDB's closed timestamp mechanism. The leader periodically publishes a timestamp watermark below which no future writes will occur. Followers can serve reads at any timestamp ≤ the closed timestamp without contacting the leader.

**How it works:**
1. Leader tracks the minimum timestamp of all in-flight (uncommitted) write proposals
2. Periodically (every heartbeat), the leader computes: `closed_ts = min_inflight_ts - 1` (or `now() - closed_ts_lag` if no in-flight writes)
3. The closed timestamp is included in every `AppendEntries` message (alongside `leader_commit`)
4. Followers store `last_closed_ts` — the most recent closed timestamp from the leader

**Follower read with bounded staleness:**
```rust
// Client requests: "check permission, at most 3 seconds stale"
fn read_with_staleness(&self, max_staleness: Duration) -> Result<bool> {
    let target_ts = Instant::now() - max_staleness;
    if self.last_closed_ts >= target_ts {
        // Closed timestamp covers our target — serve locally, zero network hops
        self.serve_from_local_state()
    } else {
        // Closed timestamp too old — fall back to ReadIndex
        self.read_index_from_leader().await
    }
}
```

**Three read tiers:**
1. **Leader + lease**: 0 hops, ~50ns (lease check + local read)
2. **Follower + closed timestamp**: 0 hops, ~100ns (timestamp check + local read)
3. **Follower + ReadIndex**: 1 hop, ~0.5ms (leader round-trip + wait for apply)

**Default closed timestamp lag:** 3 seconds. Configurable per shard. For authorization, most permission checks tolerate 3 seconds of staleness — a permission granted 3 seconds ago is overwhelmingly likely to still be valid. Only explicit "check at latest" requests need ReadIndex.

**Impact:** For read-heavy authorization workloads (100:1 read:write ratio), this eliminates the leader as a bottleneck for reads. All replicas can serve the vast majority of permission checks locally.

### 22. Shard-Level Circuit Breakers + Cluster Alarms

**Circuit breakers:** Each shard tracks its last successful write. If a shard hasn't been able to commit a write for longer than `circuit_breaker_timeout` (default: 5 seconds), it enters `Open` state:
- Write proposals are immediately rejected with `ShardUnavailable`
- Reads continue from applied state (eventual consistency) or fail for linearizable
- The circuit breaker closes automatically when the shard resumes committing

**Cluster alarms (etcd-inspired):** Alarms are cluster-wide conditions stored in the GLOBAL shard via Raft consensus. When an alarm fires, ALL nodes see it and react:

```rust
pub enum ClusterAlarm {
    /// Disk usage exceeds threshold. Cluster enters read-only mode.
    NearDiskFull { node_id: NodeId, usage_percent: u8 },
    /// Integrity check failed. Affected shard is degraded.
    IntegrityFailure { shard_id: ShardId, details: String },
}
```

When `NearDiskFull` fires:
- All write proposals across the cluster are rejected with `ClusterReadOnly`
- Reads continue normally
- The operator must free disk space and clear the alarm via admin RPC

When `IntegrityFailure` fires:
- The affected shard is marked `Degraded`
- Writes to that shard are rejected
- Reads return from cache/stale state with a warning header
- The operator must trigger snapshot reinstall for the affected shard

**Alarm lifecycle:** Alarms are set via Raft proposal (all nodes see them atomically). Alarms are cleared via admin RPC (also a Raft proposal). This ensures all nodes agree on the cluster's health state.

## Simulation-First Architecture

Inspired by FoundationDB's deterministic simulation testing. The engine is designed from day one to be fully simulatable — all sources of nondeterminism are abstracted behind injectable traits.

### 23. Injectable Abstractions for Deterministic Simulation

Three trait abstractions ensure the engine can be tested under any failure scenario with full reproducibility:

**Clock trait** — all time-dependent logic (leader lease, election timeout, heartbeat timer) uses this instead of `Instant::now()`:

```rust
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Instant;
}

/// Production: system monotonic clock.
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Instant { Instant::now() }
}

/// Simulation: test harness controls time advancement.
pub struct SimulatedClock {
    nanos: AtomicU64,
}
impl SimulatedClock {
    pub fn advance(&self, duration: Duration) { /* atomically advance */ }
}
```

**RngSource trait** — election timeout randomization uses this instead of `rand::rng()`:

```rust
pub trait RngSource: Send + Sync + 'static {
    fn election_timeout(&self, min_ms: u64, max_ms: u64) -> Duration;
    fn random_u64(&self) -> u64;
}

/// Production: thread-local CSPRNG.
pub struct SystemRng;

/// Simulation: seeded deterministic RNG. Same seed = same sequence.
pub struct SimulatedRng { seed: u64 }
```

**WalBackend trait** — all WAL I/O goes through this instead of direct file operations:

```rust
pub trait WalBackend: Send + Sync + 'static {
    fn append(&mut self, frames: &[WalFrame]) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    fn read_frames(&self, from_offset: u64) -> Result<Vec<WalFrame>>;
    fn truncate(&mut self, before_offset: u64) -> Result<()>;
}

/// Production: file-backed segmented WAL.
pub struct FileWalBackend { /* segments, file handles */ }

/// Simulation: in-memory with injectable faults.
pub struct SimulatedWalBackend {
    frames: Vec<WalFrame>,
    fault_injection: Option<Box<dyn Fn(&WalFrame) -> Result<()>>>,
}
```

**The engine is parameterized by these traits:**

```rust
pub struct ConsensusEngine<C: Clock, R: RngSource, W: WalBackend> {
    clock: C,
    rng: R,
    wal_backend: W,
    // ...
}

// Production:
type ProductionEngine = ConsensusEngine<SystemClock, SystemRng, FileWalBackend>;

// Simulation:
type SimulatedEngine = ConsensusEngine<SimulatedClock, SimulatedRng, SimulatedWalBackend>;
```

**Why this matters:** Every feature in this spec has subtle edge cases under failure. Deterministic simulation lets us test them exhaustively:

| Feature | Simulation test |
|---------|----------------|
| Leader lease | Advance one node's clock past lease expiry while another's is still valid |
| Pipelined replication | Drop specific ACKs, verify NACK rewind logic |
| Closed timestamps | Advance leader's clock, verify followers don't serve reads beyond closed_ts |
| Split/merge | Kill a node mid-migration, verify recovery |
| WAL encryption | Corrupt a frame's GCM tag, verify rejection |
| Decoupled apply | Stall the apply worker, verify reads wait correctly |

A failing simulation test produces a **seed** that reproduces the exact failure every time. Developers debug by replaying the seed step-by-step.

### 24. Deterministic Simulation Harness

The simulation harness creates N virtual nodes in a single process:

```rust
pub struct Simulation {
    nodes: Vec<SimulatedEngine>,
    network: SimulatedNetwork,  // Controllable message delivery
    clock: SimulatedClock,      // Shared, advanced by harness
    rng: SimulatedRng,          // Seeded for reproducibility
    history: Vec<HistoryEntry>, // For linearizability verification
    seed: u64,
}

impl Simulation {
    pub fn new(seed: u64, node_count: usize) -> Self;
    
    /// Advance simulated time.
    pub fn advance_clock(&mut self, duration: Duration);
    
    /// Process all pending events across all nodes (deterministic order).
    pub fn step(&mut self);
    
    /// Inject a network partition between two groups of nodes.
    pub fn partition(&mut self, group_a: &[usize], group_b: &[usize]);
    
    /// Heal all network partitions.
    pub fn heal(&mut self);
    
    /// Kill a node (crash, no graceful shutdown).
    pub fn kill(&mut self, node: usize);
    
    /// Restart a killed node (WAL recovery).
    pub fn restart(&mut self, node: usize);
    
    /// Propose a write and record in history.
    pub fn propose(&mut self, node: usize, shard: ShardId, data: Vec<u8>);
    
    /// Read and record in history.
    pub fn read(&mut self, node: usize, shard: ShardId) -> Option<Vec<u8>>;
    
    /// Verify the recorded history is linearizable.
    pub fn assert_linearizable(&self);
}
```

**Example simulation test:**
```rust
#[test]
fn lease_reads_not_stale_after_partition() {
    for seed in 0..10_000 {
        let mut sim = Simulation::new(seed, 5);
        sim.bootstrap();
        
        // Write a value
        sim.propose(0, SHARD_0, b"grant:alice:read:doc1".to_vec());
        sim.step_until_committed();
        
        // Partition: old leader isolated
        sim.partition(&[0], &[1, 2, 3, 4]);
        
        // New leader elected, revokes access
        sim.advance_clock(Duration::from_secs(1));
        sim.step_until_leader_elected(&[1, 2, 3, 4]);
        sim.propose(1, SHARD_0, b"revoke:alice:read:doc1".to_vec());
        sim.step_until_committed();
        
        // Old leader's lease should have expired
        sim.advance_clock(Duration::from_millis(200));
        let result = sim.read(0, SHARD_0); // Read from old leader
        
        // Must NOT return the revoked permission
        assert_ne!(result, Some(b"grant:alice:read:doc1".to_vec()));
        
        sim.assert_linearizable();
    }
}
```

This test runs 10,000 seeds in under a second (simulated time, no real I/O). If any seed produces a stale read, it's a reproducible bug.

## Testing Enhancements

### 25. Jepsen-Style Linearizability Testing

A test harness that verifies linearizability under adversarial conditions:

1. Run a 3-5 node cluster in-process (using simulated network from the benchmark prototype)
2. Inject faults: network partitions, node crashes, clock skew, slow disk
3. Execute a workload: concurrent writes + linearizable reads
4. Record a history: `{:type :invoke, :f :write, :value 42}`, `{:type :ok, :f :write, :value 42}`, etc.
5. Verify the history is linearizable using Knossos (Jepsen's linearizability checker) or a Rust equivalent

**Key scenarios to test:**
- Leader crash during committed-but-unapplied entries → reads must not see stale data
- Network partition with lease-based reads → stale read window bounded by lease duration
- Membership change during concurrent writes → no lost writes
- Split during concurrent reads/writes → consistent routing

### 26. Loom Tests for Atomic Hot Paths

Use the `loom` crate for exhaustive concurrency testing of lock-free data structures:
- `LeaderLease` — verify `renew()` + `is_valid()` never returns stale results under all interleavings
- `ArcSwap<AppliedState>` — verify readers never see partially-applied state
- `AtomicU64` committed_index gossip — verify term+index updates are consistent
- Apply worker dispatch — verify channel send never blocks the reactor

## Non-Goals (This Spec)

- Integration into existing InferaDB crates (separate spec after engine is built and tested in isolation via simulation)
- Removal of openraft dependency (separate cutover task list, not a design spec)
- Dynamic WAL segment sizing (pre-allocation at 75% solves the rotation stall; static configurability can be added if users report workload-specific needs)
- Online defragmentation of state.db (expose `fragmentation_ratio` metric for monitoring; offline defrag via stop-compact-restart covers org-deletion; revisit when production data shows it's needed)
- Multi-key transactions / OCC conflict resolution (within-shard atomic batches via single Raft entries provide all-or-nothing semantics; cross-shard consistency via sagas; OCC/MVCC complexity unjustified for near-zero conflict authorization workloads)
- Side-loaded SSTables (authorization payloads have a natural size ceiling; WAL frame format handles variable-length entries; bulk import should batch into many normal-sized entries)
- Full FDB-style deterministic simulation replacing the async runtime scheduler (the reactor's single-threaded architecture plus injectable traits — Clock, RngSource, WalBackend — provide equivalent coverage for consensus-critical paths, complemented by Loom and Jepsen testing for concurrent and distributed correctness)
- Raft-level follower-to-leader flow control (snapshot transfer at 50K lag + NearDiskFull alarm + per-shard WAL budget covers pathological cases; defer full admission-token protocol until observed in production)

Note: A per-shard WAL budget (`max_shard_wal_bytes`, default 256MB) provides lightweight throttling — if a shard's unreplicated WAL exceeds this budget, the reactor applies graduated proposal throttling for that shard before reaching the snapshot transfer threshold. This bridges the gap between apply back-pressure and snapshot transfer without protocol changes.
