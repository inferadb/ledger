[Documentation](../README.md) > Internals > Storage

# Storage

This document covers directory layout, database schemas, snapshots, and crash recovery.

## Directory Layout

```
/var/lib/ledger/
├── node_id                      # Persisted node identity (snowflake ID)
├── state.db                     # Entity/relationship state and indexes
├── raft.db                      # Raft log entries and persistent state
├── blocks.db                    # Block archive storage
└── snapshots/
    ├── 000001000.snap           # Snapshot at height 1000
    └── 000002000.snap
```

The current implementation uses a unified database approach rather than per-shard directories. All data is stored in B+ tree tables within these databases.

### Design Decisions

| Decision               | Rationale                                          |
| ---------------------- | -------------------------------------------------- |
| Unified database files | Custom B+ tree engine with MVCC                    |
| Table-based storage    | 15 tables for different data types (see tables.rs) |
| Dual-slot commit       | Atomic commits using header slot flipping          |
| Snapshots by height    | Predictable naming; simple retention policy        |

## Database Backend

Ledger uses a custom B+ tree storage engine providing ACID transactions with MVCC. Each shard uses two databases.

### Raft Log Storage (log.db)

```rust
const LOG_ENTRIES: TableDefinition<u64, &[u8]> = TableDefinition::new("log");
const LOG_META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
```

Key operations:

- `append(entries)`: Append log entries with fsync
- `truncate(log_id)`: Remove entries after log_id (conflict resolution)
- `purge(log_id)`: Remove entries before log_id (after snapshot)
- `save_vote(vote)`: Persist current term and voted_for

### State Storage

The database uses 15 tables (see `crates/store/src/tables.rs`):

| Table           | Key Format                              | Purpose                    |
| --------------- | --------------------------------------- | -------------------------- |
| Relationships   | `{vault_id:8BE}{bucket_id:1}{key}`      | Relationship tuples        |
| Entities        | `{vault_id:8BE}{bucket_id:1}{key}`      | Key-value entities         |
| ObjIndex        | `{vault_id:8BE}{resource}#{relation}`   | Resource→subject lookup    |
| SubjIndex       | `{vault_id:8BE}{subject}#{relation}`    | Subject→resource lookup    |
| VaultMeta       | `{organization_slug:8BE}{vault_id:8BE}`      | Vault metadata             |
| Blocks          | `{shard_height:8BE}`                    | Block storage              |
| VaultBlockIndex | `{organization_slug:8BE}{vault_id:8BE}{h:8}` | Vault height→block mapping |
| RaftLog         | `{log_id:8BE}`                          | Raft log entries           |
| RaftState       | `{key}`                                 | Raft persistent state      |

**Key format**: `vault_id (8 bytes BE) + bucket_id (1 byte) + local_key`

The bucket_id (0-255) enables incremental state root computation.

## Block Archive

Blocks are stored in the `Blocks` table within the database:

```
Key: shard_height (u64 BE)
Value: postcard-serialized ShardBlock
```

A secondary `VaultBlockIndex` table provides fast vault-specific lookups by vault height.

## Snapshots

### Format

```rust
struct SnapshotFile {
    header: SnapshotHeader,
    state_data: CompressedStateData,  // zstd compressed
}

struct SnapshotHeader {
    magic: [u8; 4],                        // "LSNP"
    version: u32,
    shard_id: ShardId,
    shard_height: u64,
    vault_states: Vec<VaultSnapshotMeta>,
    checksum: Hash,                        // SHA-256 of state_data
    genesis_hash: Hash,                    // Links snapshot to shard origin
    previous_snapshot_height: Option<u64>,
    previous_snapshot_hash: Option<Hash>,
    chain_commitment: ChainCommitment,     // Verification without full replay
}

struct VaultSnapshotMeta {
    vault_id: VaultId,
    vault_height: u64,
    state_root: Hash,
    bucket_roots: Vec<Hash>,  // 256 bucket roots for incremental state
    key_count: u64,
}
```

**Naming**: `{shard_height:09}.snap` (e.g., `000001000.snap`)

### Chain Commitment

Snapshots include chain verification data for integrity after block compaction:

```rust
struct ChainCommitment {
    accumulated_header_hash: Hash,  // Sequential hash of all headers
    state_root_accumulator: Hash,   // Merkle root of state_roots
    from_height: u64,
    to_height: u64,
}
```

This enables verification without full block replay.

### Storage Tiers

| Tier | Location            | Contents                   |
| ---- | ------------------- | -------------------------- |
| Hot  | Local SSD           | Last 3 snapshots           |
| Warm | Object storage (S3) | Last 30 days               |
| Cold | Archive (Glacier)   | Older snapshots (optional) |

### Triggering

- **Time-based**: Every 5 minutes
- **Size-based**: Every 10,000 blocks
- **Manual**: On-demand via admin API

## Retention Policies

### Block Retention Modes

```rust
enum BlockRetentionPolicy {
    Full,                              // Keep all blocks indefinitely
    Compacted { full_retention_blocks: u64 },  // Remove old tx bodies
}
```

| Mode      | Transaction Bodies      | Use Case                |
| --------- | ----------------------- | ----------------------- |
| Full      | Kept indefinitely       | SOC 2, HIPAA compliance |
| Compacted | Removed after threshold | High-volume workloads   |

In Compacted mode, block headers (including `state_root`, `tx_merkle_root`) are always preserved.

### Data Retention

| Data Type          | Full Mode         | Compacted Mode             |
| ------------------ | ----------------- | -------------------------- |
| Block headers      | Indefinite        | Indefinite                 |
| Transaction bodies | Indefinite        | Until compaction threshold |
| State snapshots    | 30 days           | Indefinite                 |
| Raft WAL           | Until snapshotted | Until snapshotted          |
| Deleted vault data | 90 days           | 90 days                    |

## Crash Recovery

Recovery follows a deterministic sequence:

```rust
async fn recover(&mut self) -> Result<()> {
    // 1. Load node identity
    let node_id = self.load_or_create_node_id()?;

    // 2. Discover shards from directory structure
    for shard_dir in self.data_dir.join("shards").read_dir()? {
        let shard_id = parse_shard_id(&shard_dir.file_name())?;

        // 3. Recover Raft state
        let raft_storage = RaftLogStore::open(shard_dir.join("raft/log.db"))?;
        let vote = raft_storage.read_vote().await?;
        let log_state = raft_storage.get_log_state().await?;

        // 4. Find latest valid snapshot
        let snapshot = self.find_latest_snapshot(&shard_dir)?;

        // 5. Load state from snapshot
        let mut state = match snapshot {
            Some(snap) => StateTree::from_snapshot(&snap)?,
            None => StateTree::empty(),
        };

        // 6. Replay committed log entries after snapshot
        let start_index = snapshot.map(|s| s.shard_height + 1).unwrap_or(0);
        for entry in raft_storage.read_range(start_index..)? {
            state.apply(&entry.payload)?;
        }

        // 7. Verify state root matches last committed block
        // ... verification code ...

        // 8. Initialize Raft and join cluster
        self.start_raft(shard_id, raft_storage, state).await?;
    }

    Ok(())
}
```

### Recovery Scenarios

| Failure Mode         | Recovery Action                                  |
| -------------------- | ------------------------------------------------ |
| Clean shutdown       | Replay from last snapshot + committed log        |
| Crash during write   | Incomplete transaction rolled back automatically |
| Corrupted snapshot   | Skip to older snapshot, replay more log          |
| Corrupted log entry  | Fetch from peer, or rebuild from snapshot        |
| Missing segment file | Fetch from peer (block archive is replicated)    |

## File Locking

Each node exclusively locks its data directory:

```rust
fn acquire_lock(data_dir: &Path) -> Result<FileLock> {
    let lock_path = data_dir.join(".lock");
    let file = File::create(&lock_path)?;
    file.try_lock_exclusive()
        .map_err(|_| Error::DataDirectoryLocked)?;
    Ok(FileLock { file, path: lock_path })
}
```

## Corruption Detection

### Storage Layers

| Layer         | Contents                       | Truncatable?                      | Purpose                    |
| ------------- | ------------------------------ | --------------------------------- | -------------------------- |
| Raft WAL      | Uncommitted/recent log entries | Yes, after snapshot               | Consensus, leader catch-up |
| Block Archive | Committed blocks               | Headers: never; Txs: configurable | Verification, audit        |
| State Layer   | Materialized K/V indexes       | Rebuilt from chain                | Fast queries               |

### Detection Methods

- **Continuous verification**: Every read can optionally verify against state_root
- **Hash chain verification**: Each block's previous_hash must match prior block's hash
- **State root divergence**: After applying block N, all replicas must have identical state_root

### Resolution

| Corruption Type     | Detection                   | Resolution                           |
| ------------------- | --------------------------- | ------------------------------------ |
| Chain hash break    | previous_hash mismatch      | Re-fetch blocks from healthy replica |
| State divergence    | state_root mismatch         | Rebuild state tree from chain        |
| Partial block       | Incomplete block data       | Re-fetch from quorum                 |
| Snapshot corruption | state_root mismatch on load | Discard, use older or rebuild        |

**Authoritative source**: Quorum determines truth. Corrupted nodes resync from healthy replicas.

## Storage Invariants

1. **Raft log durability**: Log entries fsync'd before Raft acknowledgment
2. **State consistency**: `state.db` reflects all applied log entries up to `applied_index`
3. **Block archive append-only**: Segment files never modified after creation
4. **Snapshot validity**: Snapshot `state_root` matches block header at `shard_height`
