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
| Table-based storage    | 20 tables for different data types (see tables.rs) |
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

The database uses 20 tables (see `crates/store/src/tables.rs`):

| Table              | Key Format                              | Purpose                              |
| ------------------ | --------------------------------------- | ------------------------------------ |
| Relationships      | `{vault_id:8BE}{bucket_id:1}{key}`      | Relationship tuples                  |
| Entities           | `{vault_id:8BE}{bucket_id:1}{key}`      | Key-value entities                   |
| ObjIndex           | `{vault_id:8BE}{resource}#{relation}`   | Resource→subject lookup              |
| SubjIndex          | `{vault_id:8BE}{subject}#{relation}`    | Subject→resource lookup              |
| VaultMeta          | `{organization_slug:8BE}{vault_id:8BE}` | Vault metadata                       |
| Blocks             | `{shard_height:8BE}`                    | Block storage                        |
| VaultBlockIndex    | `{org_slug:8BE}{vault_id:8BE}{h:8}`    | Vault height→block mapping           |
| RaftLog            | `{log_id:8BE}`                          | Raft log entries                     |
| RaftState          | `{key}`                                 | Raft persistent state                |
| OrganizationMeta   | `{org_id:8BE}`                          | Organization metadata                |
| Sequences          | `{counter_name}`                        | Sequence counters                    |
| OrganizationSlugIndex | `{slug:8BE}`                         | Slug→org_id lookup                   |
| VaultSlugIndex     | `{slug:8BE}`                            | Slug→vault_id lookup                 |
| ClientSequences    | `{org_id:8BE}{vault_id:8BE}{client_id}` | Client sequence entries (composite)  |
| VaultHeights       | `{org_id:8BE}{vault_id:8BE}`            | Per-vault blockchain heights         |
| VaultHashes        | `{org_id:8BE}{vault_id:8BE}`            | Per-vault previous block hashes      |
| VaultHealth        | `{org_id:8BE}{vault_id:8BE}`            | Per-vault health status              |

**Key format**: `vault_id (8 bytes BE) + bucket_id (1 byte) + local_key`

The bucket_id (0-255) enables incremental state root computation.

### Externalized State Architecture

The Raft state machine uses a two-tier persistence model:

1. **`AppliedStateCore`** — A compact struct (<512 bytes) containing `last_applied`, `membership`, `shard_height`, and `previous_shard_hash`. Stored in the `RaftState` table with a 2-byte version sentinel prefix (`[0x00, 0x01]`).

2. **Externalized tables** — Nine dedicated B+ tree tables (`OrganizationMeta`, `VaultMeta`, `VaultHeights`, `VaultHashes`, `VaultHealth`, `Sequences`, `ClientSequences`, `OrganizationSlugIndex`, `VaultSlugIndex`) store per-entity state that previously lived in a single serialized `AppliedState` blob.

Both tiers are written atomically in a single `WriteTransaction` during each apply cycle. The in-memory `AppliedState` remains the hot cache for reads — externalized tables are the persistence layer only.

On startup, `load_state_from_tables()` reconstructs the full `AppliedState` from the externalized tables, including derived fields (`id_to_slug`, `vault_id_to_slug`, `organization_storage_bytes`). The version sentinel prefix enables automatic migration from the legacy single-blob format.

## Block Archive

Blocks are stored in the `Blocks` table within the database:

```
Key: shard_height (u64 BE)
Value: postcard-serialized ShardBlock
```

A secondary `VaultBlockIndex` table provides fast vault-specific lookups by vault height.

## Snapshots

### Format

Snapshots use a file-based streaming format with zstd compression and SHA-256 integrity verification. The binary layout is:

```
┌─────────────────────────────────────────────┐
│ Magic: "LSNP" (4 bytes)                     │
│ Version: 1 (u32)                            │
├─────────────────────────────────────────────┤
│ Header section: AppliedStateCore (postcard)  │
├─────────────────────────────────────────────┤
│ Table sections × 9 (externalized tables)    │
│   Each: table_id + entry_count + key/value  │
├─────────────────────────────────────────────┤
│ Entity section: all entities across vaults  │
├─────────────────────────────────────────────┤
│ Event section: apply-phase events           │
├─────────────────────────────────────────────┤
│ SHA-256 checksum (32 bytes, over compressed)│
└─────────────────────────────────────────────┘
```

All sections (header through events) are zstd-compressed (level 3) as a single stream. The SHA-256 checksum covers the compressed bytes, enabling two-pass verification: checksum first (over compressed data), then decompress.

**Naming**: `{shard_height:09}.snap` (e.g., `000001000.snap`)

### Snapshot Data Type

The openraft `SnapshotData` type is `tokio::fs::File`. Snapshots are written to temporary files, transferred as chunked byte streams between nodes, and installed by streaming decompressed data directly into a `WriteTransaction`.

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

## File I/O

The storage engine uses position-based I/O (`pread`/`pwrite` on Unix) for lock-free concurrent reads. Reads use `read_exact_at()` without acquiring any lock, while writes serialize through a `Mutex<()>` write lock.

| Operation     | Lock Required | Syscall        |
| ------------- | ------------- | -------------- |
| `read_page`   | None          | `pread(2)`     |
| `read_header` | None          | `pread(2)`     |
| `write_page`  | `write_lock`  | `pwrite(2)`    |
| `write_header`| `write_lock`  | `pwrite(2)`    |
| `extend`      | `write_lock`  | `ftruncate(2)` |
| `sync`        | None          | `fdatasync(2)` |

On Windows, reads fall back to `seek_read()` which requires the write lock due to cursor mutation.

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
| Snapshot corruption | SHA-256 checksum mismatch   | Discard, use older or rebuild        |

**Authoritative source**: Quorum determines truth. Corrupted nodes resync from healthy replicas.

## Storage Invariants

1. **Raft log durability**: Log entries fsync'd before Raft acknowledgment
2. **State consistency**: `state.db` reflects all applied log entries up to `applied_index`
3. **Block archive append-only**: Segment files never modified after creation
4. **Snapshot validity**: Snapshot `state_root` matches block header at `shard_height`
5. **Externalized table atomicity**: `AppliedStateCore` and all 9 externalized tables are written in a single `WriteTransaction` — either all succeed or none are visible
6. **Snapshot integrity**: SHA-256 checksum over compressed bytes is verified before any decompression or state changes during installation

## Format Compatibility

The B+ tree leaf node layout (`NODE_HEADER_SIZE = 16`) includes a `next_leaf` sibling pointer that was not present in earlier versions (`NODE_HEADER_SIZE = 8`). This is a breaking page-level format change — existing data files cannot be read by the new binary.

Nodes with old-format data directories must delete the directory and rejoin the cluster via snapshot install. See the [Upgrade Runbook](../operations/runbooks/rolling-upgrade.md) for the full procedure.
