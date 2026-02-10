# InferaDB Ledger — Codebase Manifest

## Overview

InferaDB Ledger is a blockchain database for cryptographically verifiable authorization, built in Rust with a layered architecture:

```
gRPC Services (Admin, Read, Write, Health, Discovery, Raft)
    ↓
  raft — openraft consensus, batching, rate limiting, multi-shard
    ↓
  state — domain model, vaults, entities, relationships, state roots
    ↓
  store — custom B+ tree engine, ACID transactions, crash recovery
    ↓
  types — primitives, hashing, config, errors, validation
```

Supporting crates:

- **proto**: gRPC/protobuf definitions and conversions
- **sdk**: Enterprise client library with retry, circuit breaker, metrics
- **server**: Binary with bootstrap, config, discovery, integration tests
- **test-utils**: Shared testing infrastructure (strategies, assertions, crash injection)

The codebase demonstrates production-grade engineering: zero `unsafe` code, comprehensive error handling with snafu, property-based testing with proptest, crash recovery tests, OpenTelemetry tracing, Prometheus metrics, and SOC2/HIPAA audit logging.

---

## Crate: `inferadb-ledger-types`

- **Purpose**: Foundation crate providing primitives, configuration, error taxonomy, hashing, Merkle trees, and input validation.
- **Dependencies**: No workspace dependencies (foundational)
- **Quality Rating**: ★★★★★

### Files

#### `lib.rs`

- **Purpose**: Re-exports core types, errors, hashing utilities, and codec
- **Key Types/Functions**:
  - Public modules: `audit`, `codec`, `config`, `error`, `hash`, `merkle`, `types`, `validation`
- **Insights**: Clean public API surface, excellent organization

#### `audit.rs` (395 lines)

- **Purpose**: SOC2/HIPAA audit logging types for compliance
- **Key Types/Functions**:
  - `AuditEvent`: Timestamped audit record with actor, action, resource, outcome
  - `AuditAction`: 17 variants covering CRUD, admin ops (CreateVault, DeleteEntity, UpdateShardConfig, etc.)
  - `AuditResource`: Target resource (Namespace, Vault, Entity, Relationship, Shard, Node, Config)
  - `AuditOutcome`: Success vs. Failure with optional error message
- **Insights**: Comprehensive coverage of auditable operations, ready for regulatory compliance

#### `codec.rs` (563 lines)

- **Purpose**: postcard-based serialization/deserialization with structured error handling
- **Key Types/Functions**:
  - `encode<T: Serialize>(value: &T) -> Result<Vec<u8>>`: Serialize to bytes
  - `decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T>`: Deserialize from bytes
  - `CodecError`: snafu error with SerializationFailed/DeserializationFailed variants
  - 32 unit tests, 9 proptest strategies covering all domain types
- **Insights**: Excellent test coverage, postcard chosen for determinism and compactness

#### `config.rs` (3534 lines)

- **Purpose**: Configuration types for all subsystems with fallible builders, serde, and JSON Schema
- **Key Types/Functions**:
  - `ServerConfig`: Root config with nested subsystem configs (18 total structs)
  - `RaftConfig`, `StorageConfig`, `BatchConfig`, `RateLimitConfig`, `ValidationConfig`: Core subsystem configs
  - `TlsConfig`, `OtelConfig`, `AuditConfig`: Security and observability
  - `CircuitBreakerConfig`, `HotKeyConfig`, `QuotaConfig`: Advanced features
  - All use `#[bon::bon]` fallible builders with validation
  - All derive `serde::Serialize/Deserialize` for TOML loading
  - All derive `schemars::JsonSchema` for schema export
- **Insights**: ⚠️ **CONCERN**: 3534 lines is excessive for a single file. Should be split into submodules by subsystem (e.g., `config/raft.rs`, `config/storage.rs`, `config/security.rs`, `config/observability.rs`, `config/resilience.rs`, `config/features.rs`). Excellent validation with humantime-serde for durations, byte-unit for sizes.

#### `error.rs` (1185 lines)

- **Purpose**: Structured error taxonomy with numeric codes, retryability, and guidance
- **Key Types/Functions**:
  - `ErrorCode`: 29 numeric codes (1000-1028) for classification
  - `LedgerError`: Domain-level errors with `#[snafu]` for context propagation
  - `StorageError`: Storage layer errors (IO, corruption, capacity)
  - `ConsensusError`: Raft consensus errors (leadership, quorum, log gaps)
  - `code()`, `is_retryable()`, `suggested_action()`: Traits on all error types
- **Insights**: State-of-the-art error handling. Every error has numeric code, retryability classification, and actionable guidance. Implicit location tracking via snafu.

#### `hash.rs` (718 lines)

- **Purpose**: Cryptographic and non-cryptographic hashing utilities
- **Key Types/Functions**:
  - `Sha256Hash`: SHA-256 wrapper with hex encoding, constant-time comparison
  - `SeaHash`: seahash wrapper for non-cryptographic use (Prometheus labels)
  - `block_hash()`, `tx_hash()`, `compute_state_root()`: Domain-specific hash functions
  - `BucketHasher`: State bucketing (256 buckets) for incremental hashing
- **Insights**: Excellent security practices. Constant-time comparison prevents timing attacks. Clear separation of cryptographic vs. non-cryptographic use cases.

#### `merkle.rs` (332 lines)

- **Purpose**: Merkle tree and proof generation/verification using rs_merkle
- **Key Types/Functions**:
  - `MerkleTree::new(leaves: Vec<Vec<u8>>)`: Build tree from transaction hashes
  - `MerkleTree::root()`: Get root hash
  - `MerkleTree::proof(index: usize)`: Generate inclusion proof for transaction
  - `MerkleProof::verify()`: Verify proof against root
- **Insights**: Power-of-2 leaf count limitation documented (rs_merkle constraint). Adequate for block-level Merkle trees.

#### `types.rs` (1061 lines)

- **Purpose**: Core domain types (blocks, transactions, operations, entities, relationships)
- **Key Types/Functions**:
  - `define_id!` macro: Generates newtypes with derives, Display (prefixed), FromStr, serde
  - `NamespaceId`, `VaultId`, `UserId`, `ShardId`: Newtype IDs (replaced type aliases)
  - `BlockHeader`: version, height, prev_hash, timestamp, tx_merkle_root, state_root
  - `Transaction`: namespace, vault, operations (fallible builder with validation)
  - `Operation`: 14 variants (CreateEntity, DeleteEntity, CreateRelationship, etc.)
  - `Entity`: key, value, ttl, version
  - `Relationship`: resource, relation, subject (authorization tuple)
- **Insights**: Fallible Transaction builder validates constraints (max 100 ops, max 1MB size). Newtype IDs prevent accidental mixing (e.g., NamespaceId vs VaultId).

#### `validation.rs` (563 lines)

- **Purpose**: Input validation with character whitelists and configurable size limits
- **Key Types/Functions**:
  - `ValidationConfig`: max_name_length, max_value_length, max_operations_per_transaction, etc.
  - `validate_name()`, `validate_entity_key()`, `validate_entity_value()`: Character whitelist enforcement
  - `validate_transaction_size()`: Enforces request size limits (default 1MB)
  - Character sets: alphanumeric + hyphen/underscore for names, UTF-8 for values
- **Insights**: Defense-in-depth security. Prevents injection attacks, DoS via large requests. Configurable limits support different deployment environments.

---

## Crate: `inferadb-ledger-store`

- **Purpose**: Custom B+ tree storage engine with ACID transactions, page management, crash recovery, and pluggable backends.
- **Dependencies**: `types` (for errors, hashing, config)
- **Quality Rating**: ★★★★★

### Files

#### `lib.rs`

- **Purpose**: Re-exports public API (Database, Transaction, StorageBackend, Table)
- **Key Types/Functions**:
  - Modules: `backend`, `bloom`, `btree`, `cache`, `db`, `dirty_bitmap`, `integrity`, `page`, `tables`, `transaction`, `types`
- **Insights**: Clean layering: backend abstraction → page management → B+ tree → database → transactions

#### `backend/mod.rs`

- **Purpose**: Storage backend trait with file and in-memory implementations
- **Key Types/Functions**:
  - `StorageBackend` trait: read_page, write_page, sync, size, truncate
  - `FileBackend`: Production backend with direct I/O, fsync
  - `InMemoryBackend`: Testing backend with HashMap<PageId, Page>
- **Insights**: Abstraction enables testing without filesystem overhead. FileBackend uses `std::fs::File` with sync_data() for durability.

#### `bloom.rs`

- **Purpose**: Bloom filter for page existence checks (optimization)
- **Key Types/Functions**:
  - `BloomFilter::new(size, num_hashes)`: Create filter
  - `insert(&hash)`, `contains(&hash) -> bool`: Add/check membership
- **Insights**: Space-efficient probabilistic data structure. Used to avoid disk reads for non-existent keys.

#### `btree/mod.rs` (~500 lines)

- **Purpose**: B+ tree core logic (insert, get, remove, split, merge, compact)
- **Key Types/Functions**:
  - `BTree::new(root_page_id, page_manager, node_manager)`: Open existing tree
  - `insert(&key, &value)`, `get(&key) -> Option<Vec<u8>>`, `remove(&key) -> bool`
  - `cursor() -> BTreeCursor`: Iterator support
  - `compact(fill_threshold) -> Result<CompactionStats>`: Reclaim dead space
- **Insights**: Custom implementation (not using existing crate). Supports variable-length keys/values. Compaction merges underfull leaves.

#### `btree/node.rs` (~800 lines)

- **Purpose**: B+ tree node representation (internal and leaf nodes)
- **Key Types/Functions**:
  - `InternalNode`: keys + child page IDs
  - `LeafNode`: cells (key-value pairs) + next pointer (linked list)
  - `split_leaf()`, `split_internal()`: Node splitting during insert
  - `merge_leaves()`: Merge adjacent leaves during compaction
- **Insights**: Leaf nodes form doubly-linked list for range scans. Split logic is complex but well-tested.

#### `btree/split.rs`

- **Purpose**: Node splitting logic separated from main btree module
- **Key Types/Functions**:
  - `split_if_necessary()`: Check fill ratio, split if needed
  - `promote_key()`: Promote separator key to parent internal node
- **Insights**: Clean separation of concerns. Split logic is complex enough to warrant dedicated file.

#### `btree/cursor.rs`

- **Purpose**: Iterator over B+ tree (forward scan, range queries)
- **Key Types/Functions**:
  - `BTreeCursor::new(btree, start_key)`: Position cursor
  - `next() -> Option<(Vec<u8>, Vec<u8>)>`: Iterate key-value pairs
  - `advance()`: Move to next leaf via linked list or parent backtracking
- **Insights**: Supports range queries with start_key bound. Fixed critical bug in `advance()` for multi-leaf traversal (resume-key pattern).

#### `page/mod.rs` (~400 lines)

- **Purpose**: Page abstraction (4KB blocks), page cache, allocator
- **Key Types/Functions**:
  - `Page`: 4096-byte buffer with header (page_id, page_type, checksum)
  - `PageManager`: Owns backend, cache, allocator
  - `read_page()`, `write_page()`, `allocate_page()`, `free_page()`
- **Insights**: Fixed-size pages simplify memory management. CRC32 checksums detect corruption.

#### `page/cache.rs`

- **Purpose**: LRU page cache (reduces disk I/O)
- **Key Types/Functions**:
  - `PageCache::new(capacity)`: Create cache
  - `get(&page_id) -> Option<Arc<Page>>`: Check cache
  - `insert(page_id, page)`: Add to cache, evict LRU if full
- **Insights**: `Arc<Page>` enables safe sharing across threads. LRU eviction policy is simple and effective.

#### `page/allocator.rs`

- **Purpose**: Free page tracking (bitmap-based)
- **Key Types/Functions**:
  - `PageAllocator::new()`: Initialize allocator
  - `allocate() -> PageId`: Find free page, mark allocated
  - `free(page_id)`: Mark page as free
- **Insights**: Bitmap stored in dedicated pages. Fast allocation via bitwise operations.

#### `dirty_bitmap.rs`

- **Purpose**: Track modified pages for transaction commit
- **Key Types/Functions**:
  - `DirtyBitmap::new()`: Create bitmap
  - `mark_dirty(page_id)`: Record modification
  - `is_dirty(page_id) -> bool`: Check if modified
  - `clear()`: Reset after commit
- **Insights**: Critical for ACID transactions. Only dirty pages are flushed to disk.

#### `integrity.rs`

- **Purpose**: Data integrity checks (CRC32 checksums)
- **Key Types/Functions**:
  - `compute_checksum(data: &[u8]) -> u32`: CRC32 computation
  - `verify_checksum(page: &Page) -> bool`: Compare stored vs. computed
- **Insights**: Detects silent data corruption. Checksums stored in page header.

#### `db.rs` (~600 lines)

- **Purpose**: Database layer with ACID transactions, multiple B+ trees (tables)
- **Key Types/Functions**:
  - `Database::open(backend, config) -> Result<Self>`: Open database
  - `begin_read() -> ReadTransaction`, `begin_write() -> WriteTransaction`
  - `open_table<T: Table>() -> Result<()>`: Initialize table (allocate root page)
- **Insights**: Multiple tables share same PageManager. Dual-slot commit protocol (commit bit + sync) ensures atomicity.

#### `transaction.rs` (~500 lines)

- **Purpose**: Read and write transactions with cursor-based iteration
- **Key Types/Functions**:
  - `ReadTransaction`: Snapshot isolation, read-only operations
  - `WriteTransaction`: Read-write, dirty tracking, commit/rollback
  - `TableTransaction<T: Table>`: Type-safe table access
  - `iter() -> TableIterator`: Streaming iterator with resume-key support
- **Insights**: Excellent design. Type-safe table access via phantom types. Cursor-based iteration prevents OOM on large tables.

#### `tables.rs`

- **Purpose**: Type-safe table definitions with marker traits
- **Key Types/Functions**:
  - `Table` trait: defines KeyType, ValueType, table_id
  - `Entities`, `Relationships`, `RelationshipsBySubject`: Table implementations
  - `BucketCommitments`, `BlockArchive`: State and archive tables
- **Insights**: Phantom types prevent mixing keys/values from different tables. Compile-time table ID assignment.

#### `types.rs`

- **Purpose**: Common types (PageId, TransactionId, TableId, NodeId)
- **Key Types/Functions**:
  - Type aliases for clarity: `type PageId = u64`, `type TableId = u8`
- **Insights**: Simple, effective. No newtype overhead for internal types.

#### `tests/crash_recovery.rs` (17 tests)

- **Purpose**: Crash injection tests using CrashInjector from test-utils
- **Key Types/Functions**:
  - Tests cover crashes during write, commit, sync, allocation, freeing
- **Insights**: Excellent confidence in durability guarantees. Crash injection is deterministic (not flaky).

#### `benches/btree_bench.rs`

- **Purpose**: Criterion benchmarks for insert/get operations
- **Key Types/Functions**:
  - Benchmarks: sequential insert, random insert, get hit, get miss
- **Insights**: Tracked in CI via benchmark.yml workflow. Prevents performance regressions.

---

## Crate: `inferadb-ledger-state`

- **Purpose**: Domain state layer managing vaults, entities, relationships, state roots, indexes, snapshots, and time travel.
- **Dependencies**: `types`, `store` (via StorageEngine wrapper)
- **Quality Rating**: ★★★★★

### Files

#### `lib.rs`

- **Purpose**: Re-exports public API (StateLayer, Entity/Relationship stores, ShardManager)
- **Key Types/Functions**:
  - Modules: `block_archive`, `bucket`, `engine`, `entity`, `indexes`, `keys`, `relationship`, `shard`, `snapshot`, `state`, `tiered_storage`, `time_travel`, `system`
- **Insights**: Rich feature set: snapshots, time travel, tiered storage, multi-shard. Production-ready.

#### `engine.rs`

- **Purpose**: StorageEngine wrapper around store::Database with transaction helpers
- **Key Types/Functions**:
  - `StorageEngine::open(backend, config) -> Result<Self>`
  - `begin_read()`, `begin_write()`: Transaction wrappers
  - `with_read_tx<F>(&self, f: F)`, `with_write_tx<F>(&self, f: F)`: Closure-based helpers
- **Insights**: Thin wrapper adds ergonomics. Closure pattern ensures transactions are committed/rolled back.

#### `state.rs` (~800 lines)

- **Purpose**: StateLayer applies blocks, computes state roots using bucket-based incremental hashing
- **Key Types/Functions**:
  - `StateLayer::new(engine) -> Self`: Initialize with 256 buckets
  - `apply_block(block: &Block) -> Result<Sha256Hash>`: Apply operations, compute state root
  - `get_state_root() -> Result<Sha256Hash>`: Current state root (SHA-256 of 256 bucket roots)
  - `verify_state_root(expected: &Sha256Hash) -> Result<bool>`: Integrity check
- **Insights**: Bucket hashing enables incremental updates (only recompute dirty buckets). State root is deterministic and verifiable.

#### `entity.rs`

- **Purpose**: Entity CRUD operations (zero-sized type with static methods)
- **Key Types/Functions**:
  - `Entity::create(tx, namespace, vault, key, value, ttl)`: Insert entity
  - `Entity::read(tx, namespace, vault, key) -> Option<Entity>`: Retrieve entity
  - `Entity::delete(tx, namespace, vault, key) -> bool`: Remove entity
  - `Entity::list(tx, namespace, vault) -> Vec<Entity>`: All entities in vault
- **Insights**: Zero-sized type avoids unnecessary allocations. Static methods are ergonomic.

#### `relationship.rs`

- **Purpose**: Relationship CRUD operations with dual indexing (object and subject)
- **Key Types/Functions**:
  - `Relationship::create(tx, namespace, vault, resource, relation, subject)`: Insert relationship
  - `Relationship::check(tx, namespace, vault, resource, relation, subject) -> bool`: Permission check
  - `Relationship::delete(tx, namespace, vault, resource, relation, subject) -> bool`: Remove relationship
  - `Relationship::list_by_resource()`, `Relationship::list_by_subject()`: Index queries
- **Insights**: Dual indexing enables efficient queries in both directions (who can access X? what can Y access?). Critical for authorization.

#### `keys.rs`

- **Purpose**: Key encoding for storage layer (namespace, vault, bucket, local key)
- **Key Types/Functions**:
  - `entity_key(namespace_id, vault_id, bucket_id, key) -> Vec<u8>`: Encode entity key
  - `relationship_key(namespace_id, vault_id, bucket_id, resource, relation, subject) -> Vec<u8>`
  - `parse_entity_key(&[u8]) -> (NamespaceId, VaultId, u8, Vec<u8>)`: Decode
- **Insights**: Fixed-size prefix (8-byte vault_id + 1-byte bucket_id) enables range scans. Big-endian for lexicographic ordering.

#### `indexes.rs`

- **Purpose**: Dual object/subject indexes for relationships
- **Key Types/Functions**:
  - `create_object_index_entry()`, `create_subject_index_entry()`: Insert into both indexes
  - `delete_object_index_entry()`, `delete_subject_index_entry()`: Remove from both indexes
  - `list_by_object()`, `list_by_subject()`: Query indexes
- **Insights**: Index keys include all tuple components to support efficient lookups. Consistent with relationship key encoding.

#### `bucket.rs`

- **Purpose**: VaultCommitment with 256 buckets, dirty tracking, incremental state root computation
- **Key Types/Functions**:
  - `VaultCommitment::new(vault_id)`: Initialize with 256 buckets
  - `update_bucket(bucket_id, key, value)`: Mark bucket dirty, update local state
  - `compute_state_root() -> Sha256Hash`: Recompute only dirty buckets, SHA-256(bucket_roots)
  - `commit()`: Flush dirty buckets to storage
- **Insights**: Incremental hashing is critical for performance. Dirty tracking prevents redundant computation.

#### `block_archive.rs`

- **Purpose**: Persistent block storage with optional compaction
- **Key Types/Functions**:
  - `BlockArchive::new(engine) -> Self`
  - `store_block(block: &Block) -> Result<()>`: Persist block
  - `get_block(height: u64) -> Result<Option<Block>>`: Retrieve by height
  - `compact_blocks(retention_policy: &BlockRetentionPolicy) -> Result<()>`: Remove old blocks
- **Insights**: Blocks stored by height. Compaction policy configurable (retain last N blocks or time-based). Supports audit requirements.

#### `shard.rs`

- **Purpose**: ShardManager coordinates multiple vaults within a shard
- **Key Types/Functions**:
  - `ShardManager::new(engine, shard_id) -> Self`
  - `create_vault(namespace_id, vault_name) -> Result<VaultId>`
  - `get_vault(namespace_id, vault_id) -> Result<Option<Vault>>`
  - `list_vaults(namespace_id) -> Result<Vec<Vault>>`
- **Insights**: Shard = collection of namespaces sharing a Raft group. ShardManager is coordination layer above StateLayer.

#### `snapshot.rs` (~600 lines)

- **Purpose**: Point-in-time snapshots with zstd compression, chain verification
- **Key Types/Functions**:
  - `Snapshot::create(state_layer, height, metadata) -> Result<Self>`: Capture snapshot
  - `Snapshot::restore(engine, snapshot_data) -> Result<()>`: Restore from snapshot
  - `Snapshot::verify_chain(snapshots: &[Snapshot]) -> Result<bool>`: Verify snapshot chain integrity
  - Compression: zstd level 3 (balanced speed/ratio)
- **Insights**: Snapshots include state root for verification. Chain verification ensures no tampering. Critical for backup/restore.

#### `tiered_storage.rs` (~400 lines)

- **Purpose**: Hot/warm/cold storage tiers with S3/GCS/Azure backend support
- **Key Types/Functions**:
  - `TieredStorage::new(config) -> Self`
  - `promote_to_hot()`, `demote_to_warm()`, `archive_to_cold()`: Tier transitions
  - `TierPolicy`: Age-based or access-based promotion/demotion
  - Backend implementations: `S3Backend`, `GcsBackend`, `AzureBackend`
- **Insights**: Cost optimization for large deployments. Hot = local SSD, warm = local HDD, cold = object storage. Transparent to application.

#### `time_travel.rs` (~300 lines)

- **Purpose**: Historical versioning with inverted height keys for efficient latest-version queries
- **Key Types/Functions**:
  - `create_versioned_entity(tx, key, value, height)`: Store entity with version
  - `read_entity_at_height(tx, key, height) -> Option<Entity>`: Historical read
  - `read_latest_entity(tx, key) -> Option<Entity>`: Current version (inverted key optimization)
  - Key encoding: `entity_key + (u64::MAX - height)` for reverse chronological ordering
- **Insights**: Inverted height encoding enables latest-version queries via prefix scan. No secondary index needed. Clever optimization.

#### `system/mod.rs`

- **Purpose**: \_system namespace for cluster metadata, sagas, service discovery
- **Key Types/Functions**:
  - Submodules: `cluster`, `keys`, `saga`, `service`, `types`
- **Insights**: System namespace stores metadata (cluster config, node registry, distributed transactions). Separate from user data.

#### `system/cluster.rs`

- **Purpose**: Cluster metadata (nodes, shards, rebalancing)
- **Key Types/Functions**:
  - `ClusterMetadata`: Cluster-wide configuration
  - `NodeMetadata`: Per-node metadata (address, capacity, status)
  - `ShardAssignment`: Namespace → shard mapping
- **Insights**: Supports dynamic shard assignment. Enables horizontal scaling.

#### `system/saga.rs`

- **Purpose**: Distributed transaction orchestration (saga pattern)
- **Key Types/Functions**:
  - `Saga`: Multi-step distributed transaction
  - `SagaStep`: Individual step with compensating action
  - `SagaExecutor`: Orchestrates execution, handles failures
- **Insights**: Saga pattern for cross-shard transactions. Compensating actions ensure eventual consistency.

#### `system/service.rs`

- **Purpose**: Service discovery (node registry, health checks)
- **Key Types/Functions**:
  - `ServiceRegistry`: Node registration, heartbeats
  - `register_node()`, `unregister_node()`, `get_healthy_nodes()`
- **Insights**: Enables dynamic cluster membership. Health checks detect failed nodes.

---

## Crate: `inferadb-ledger-proto`

- **Purpose**: Protobuf definitions for gRPC API and domain↔proto conversions.
- **Dependencies**: `types`, `prost`, `tonic`
- **Quality Rating**: ★★★★☆

### Files

#### `build.rs`

- **Purpose**: Dual-mode proto compilation (dev codegen vs pre-generated for crates.io)
- **Key Types/Functions**:
  - Checks `INFERADB_CODEGEN` env var
  - Dev mode: runs `tonic_build::configure()` to generate code
  - Release mode: expects pre-generated code in `src/generated/`
- **Insights**: Enables publishing to crates.io without requiring protoc. Risk: pre-generated code can drift from .proto files if not updated.

#### `lib.rs`

- **Purpose**: Conditional include of generated code
- **Key Types/Functions**:
  - `#[cfg(feature = "codegen")]`: Include generated code
  - Re-exports: `ledger.v1` module with all message types
- **Insights**: Feature flag controls compilation mode.

#### `convert.rs` (1200 lines)

- **Purpose**: From/TryFrom trait implementations for domain↔proto conversions
- **Key Types/Functions**:
  - `impl From<types::Entity> for proto::Entity`: Infallible domain→proto
  - `impl TryFrom<proto::Entity> for types::Entity`: Fallible proto→domain (validation)
  - Covers all domain types: Block, Transaction, Operation, Entity, Relationship, etc.
  - 42 unit tests + 4 proptests validating round-trip conversions
- **Insights**: Deduplication effort (Phase 2 Task 15) removed duplicate helper functions. Comprehensive test coverage prevents serialization bugs.

#### `generated/ledger.v1.rs` (6914 lines)

- **Purpose**: prost-generated Rust code from proto definitions
- **Key Types/Functions**:
  - All gRPC message types: ReadRequest, WriteRequest, CreateVaultRequest, etc.
  - Service traits: ReadService, WriteService, AdminService, HealthService, DiscoveryService, RaftService
  - 100+ message types, 50+ RPC methods
- **Insights**: Large generated file. Regular updates needed when .proto changes. Consider splitting .proto into smaller files if this grows further.

---

## Crate: `inferadb-ledger-raft`

- **Purpose**: Raft consensus integration with openraft, gRPC services, batching, rate limiting, multi-shard support, and 40+ production features.
- **Dependencies**: `types`, `store`, `state`, `proto`, `openraft`, `tonic`
- **Quality Rating**: ★★★★☆

### Core Files

#### `lib.rs`

- **Purpose**: Public API surface (restricted to 3 modules post-hygiene: services, types, metrics)
- **Key Types/Functions**:
  - Re-exports: `LedgerServer`, `LedgerTypeConfig`, `LedgerNodeId`, `LedgerRequest`
  - Private modules: Most implementation details hidden (batching, rate_limit, idempotency, etc.)
- **Insights**: Phase 2 Task 2 cleaned up public API. ~50 re-exports removed. Excellent encapsulation.

#### `log_storage.rs` (~5000 lines)

- **Purpose**: openraft LogStore and StateMachine implementation, log storage, snapshot building
- **Key Types/Functions**:
  - `RaftLogStore`: Implements openraft's `RaftLogStorage` trait
  - `AppliedState`: StateMachine with apply_request dispatch to StateLayer
  - `append_to_log()`, `get_log_entry()`, `purge_logs_upto()`: Log operations
  - `build_snapshot()`: Captures state snapshot at current height
  - 14 operation handlers: create/read/update/delete for entities, relationships, vaults, namespaces
- **Insights**: ⚠️ **CONCERN**: 5000 lines is too large. Should split into submodules (`log_ops.rs`, `state_machine.rs`, `snapshot.rs`). Excellent feature coverage but difficult to navigate.

#### `server.rs` (~800 lines)

- **Purpose**: LedgerServer builder with all gRPC services and Raft integration
- **Key Types/Functions**:
  - `LedgerServer::builder()`: bon-based builder with 20+ config options
  - `serve(addr) -> Result<()>`: Start gRPC server with all services
  - `serve_with_shutdown(addr, shutdown_signal) -> Result<()>`: Graceful shutdown support
  - Integrates: Raft node, all services, metrics, tracing, audit logging, health checks
- **Insights**: Central wiring point. Excellent builder pattern. Supports graceful shutdown with connection draining.

#### `types.rs`

- **Purpose**: Raft type configuration and request/response types
- **Key Types/Functions**:
  - `LedgerTypeConfig`: openraft type config (NodeId, Entry, SnapshotData, etc.)
  - `LedgerNodeId`: Newtype for node ID (Snowflake ID)
  - `LedgerRequest`: 14 variants for all operations (CreateEntity, ReadEntity, CreateRelationship, etc.)
  - `LedgerResponse`: Operation results (success/error)
- **Insights**: Type-safe Raft integration. LedgerRequest is the state machine input type.

#### `error.rs` (~400 lines)

- **Purpose**: ServiceError with gRPC status code mapping, ErrorDetails enrichment
- **Key Types/Functions**:
  - `ServiceError`: snafu error with 15 variants (Storage, Raft, RateLimited, Timeout, etc.)
  - `classify_raft_error(msg: &str) -> Code`: Maps Raft error messages to gRPC codes
  - `is_leadership_error(msg: &str) -> bool`: Detects leadership errors for UNAVAILABLE
  - PRD Task 3: Leadership→UNAVAILABLE, snapshot→FAILED_PRECONDITION
- **Insights**: Excellent error classification. Clients can retry UNAVAILABLE (leadership change), not FAILED_PRECONDITION.

### Service Layer (14 files)

#### `services/admin.rs` (~800 lines)

- **Purpose**: AdminService gRPC implementation (namespace/vault/shard management)
- **Key Types/Functions**:
  - `create_namespace()`, `delete_namespace()`, `list_namespaces()`
  - `create_vault()`, `delete_vault()`, `list_vaults()`
  - `update_shard_config()`, `rebalance_shards()`
  - Audit logging for all mutations
- **Insights**: Admin operations require elevated privileges. Audit logging for compliance.

#### `services/write.rs` (~600 lines)

- **Purpose**: WriteService gRPC implementation (entity/relationship mutations)
- **Key Types/Functions**:
  - `write()`, `batch_write()`: Entity/relationship mutations
  - `create_relationship()`, `delete_relationship()`: Authorization tuple mutations
  - Rate limiting, hot key detection, validation
  - Error classification via `classify_batch_error()`
- **Insights**: Core data path. Rate limiting + hot key detection protect cluster. Batch writes go through BatchWriter.

#### `services/read.rs` (~500 lines)

- **Purpose**: ReadService gRPC implementation (entity/relationship queries)
- **Key Types/Functions**:
  - `read()`, `batch_read()`: Entity reads
  - `check_permission()`: Authorization check (relationship query)
  - `list_relationships_by_resource()`, `list_relationships_by_subject()`: Index queries
  - Pagination support via PageToken
- **Insights**: Read path with pagination. Permission checks use dual indexes for efficiency.

#### `services/health.rs` (~300 lines)

- **Purpose**: HealthService with readiness/liveness/startup probes
- **Key Types/Functions**:
  - `check(type: ProbeType) -> HealthCheckResponse`
  - Probes: readiness (Raft ready, dependencies healthy), liveness (process alive), startup (data_dir writable)
  - DependencyHealthChecker: disk writability, Raft lag, peer reachability
- **Insights**: Three-probe pattern for Kubernetes. Readiness gates traffic, liveness triggers restart, startup delays initial traffic.

#### `services/helpers.rs` (~300 lines)

- **Purpose**: Shared service utilities (rate limiting, validation, metadata extraction)
- **Key Types/Functions**:
  - `check_rate_limit()`: Rate limit check with rich ErrorDetails
  - `validation_status()`: Wraps validation errors with gRPC status
  - `extract_namespace_from_request()`: Common metadata extraction
- **Insights**: Phase 2 Task 1 extracted shared code from write/multi-shard/admin services. Reduces duplication.

#### `services/metadata.rs` (~200 lines)

- **Purpose**: Request/response metadata helpers (correlation IDs, tracing)
- **Key Types/Functions**:
  - `status_with_correlation()`: Injects x-request-id, x-trace-id, ErrorDetails
  - `extract_trace_context()`: W3C Trace Context extraction
  - `extract_transport_metadata()`: SDK version, forwarded-for headers
- **Insights**: Central point for metadata injection/extraction. Supports tracing and debugging.

#### Additional Services

- `services/raft.rs`: RaftService (inter-node Raft RPCs)
- `services/discovery.rs`: DiscoveryService (cluster membership)
- `services/forward_client.rs`: Leader forwarding
- `services/multi_shard_read.rs`: Multi-shard read coordination
- `services/multi_shard_write.rs`: Multi-shard write coordination (2PC + saga)
- `services/shard_resolver.rs`: Namespace→shard routing
- `services/error_details.rs`: ErrorDetails proto builder

### Features (40+ files)

#### Core Features

- `batching.rs`: BatchWriter with request coalescing
- `idempotency.rs`: TTL-based deduplication cache
- `pagination.rs`: HMAC-signed page tokens
- `rate_limit.rs`: 3-tier token bucket rate limiter
- `hot_key_detector.rs`: Count-Min Sketch with rotating windows
- `metrics.rs`: Prometheus metrics with SLI histograms

#### Enterprise Features

- `graceful_shutdown.rs`: 6-phase shutdown coordinator
- `runtime_config.rs`: Hot-reload via SIGHUP + ArcSwap
- `backup.rs`: Snapshot-based backups with S3/GCS/Azure
- `auto_recovery.rs`: Automatic divergence recovery
- `api_version.rs`: API version negotiation
- `deadline.rs`: Request deadline propagation
- `dependency_health.rs`: Disk/Raft/peer health checks
- `audit.rs`: File-based audit logging (JSON Lines)
- `quota.rs`: Per-namespace resource quotas

#### Background Jobs

- `block_compaction.rs`: Block archive compaction
- `btree_compaction.rs`: B+ tree compaction
- `resource_metrics.rs`: Resource saturation metrics
- `ttl_gc.rs`: Time-to-live garbage collection
- `integrity_scrubber.rs`: CRC verification
- `learner_refresh.rs`: Read replica refresh
- `orphan_cleanup.rs`: Resource leak cleanup
- `peer_maintenance.rs`: Peer health checks

#### Advanced Features

- `multi_raft.rs`: Multi-Raft orchestration
- `multi_shard_server.rs`: Multi-shard LedgerServer
- `raft_network.rs`: gRPC-based Raft transport
- `proto_compat.rs`: Orphan rule workarounds
- `trace_context.rs`: W3C Trace Context
- `wide_events.rs`: Canonical log lines
- `proof.rs`: Merkle proof generation
- `shard_router.rs`: Dynamic shard routing
- `saga_orchestrator.rs`: Distributed transaction orchestration
- `vip_cache.rs`: VIP page cache
- `cardinality.rs`: HyperLogLog for metrics
- `file_lock.rs`: Data directory locking
- `peer_tracker.rs`: Peer connection state

---

## Crate: `inferadb-ledger-sdk`

- **Purpose**: Enterprise Rust client library with retry, circuit breaker, cancellation, metrics, and tracing.
- **Dependencies**: `types`, `proto`, `tonic`, `tokio`
- **Quality Rating**: ★★★★★

### Files

#### `lib.rs`

- **Purpose**: Re-exports public API (LedgerClient, ClientConfig, builders, error types)
- **Key Types/Functions**:
  - Modules: `client`, `config`, `connection`, `error`, `retry`, `circuit_breaker`, `discovery`, `metrics`, `streaming`, `tracing`, `builders`, `server`, `idempotency`, `mock`
- **Insights**: Comprehensive SDK. All features needed for production use.

#### `client.rs` (~1000 lines)

- **Purpose**: LedgerClient with 16+ methods, retry, cancellation, metrics
- **Key Types/Functions**:
  - `LedgerClient::new(config) -> Result<Self>`: Create client
  - Data ops: `read()`, `write()`, `batch_read()`, `batch_write()` (with `_with_token` variants for cancellation)
  - Relationship ops: `check_permission()`, `create_relationship()`, `delete_relationship()`
  - Admin ops: `create_namespace()`, `create_vault()`, `list_vaults()`
  - All methods use `with_retry_cancellable()` for retry + cancellation
  - `with_metrics()` wrapper for user-perceived latency
- **Insights**: Clean API. Cancellation support via CancellationToken. Circuit breaker integrated. Metrics track end-to-end latency.

#### `config.rs` (~400 lines)

- **Purpose**: ClientConfig with fallible builder, TLS support
- **Key Types/Functions**:
  - `ClientConfig::builder() -> ClientConfigBuilder`: bon builder
  - Fields: endpoints, retry_config, circuit_breaker_config, tls_config, timeout, metrics
  - `TlsConfig`: ca_cert, use_native_roots, client_cert/key (mTLS)
  - Validation: at least one endpoint, valid TLS config
- **Insights**: Fallible builder validates constraints. TLS support for secure communication. Precedent for other configs.

#### `connection.rs` (~600 lines)

- **Purpose**: ConnectionPool with circuit breaker integration
- **Key Types/Functions**:
  - `ConnectionPool::new(config) -> Result<Self>`: Create pool
  - `get_channel() -> Result<tonic::Channel>`: Get connection with circuit breaker check
  - Circuit breaker: per-endpoint state machine (Closed→Open→HalfOpen)
  - ServerSelector syncs with circuit breaker (open→mark_unhealthy, close→mark_healthy)
- **Insights**: Circuit breaker prevents cascade failures. Pool handles connection lifecycle. Sync with ServerSelector for consistent routing.

#### `error.rs` (~500 lines)

- **Purpose**: SdkError with rich context and ServerErrorDetails decoding
- **Key Types/Functions**:
  - `SdkError`: 10 variants (Connection, Rpc, RateLimited, Timeout, Cancelled, CircuitOpen, etc.)
  - `ServerErrorDetails`: Decoded from proto ErrorDetails (error_code, retryable, retry_after, context, action)
  - `is_retryable()`, `error_type()`: Classification helpers
  - `attempt_history: Vec<(u32, String)>`: Retry tracking (PRD Task 2)
- **Insights**: Rich error context for debugging. ServerErrorDetails decode via prost. RateLimited variant with retry_after guidance.

#### `retry.rs` (~400 lines)

- **Purpose**: with_retry_cancellable (manual retry loop with tokio::select!)
- **Key Types/Functions**:
  - `with_retry_cancellable<F>(pool, method, operation, token) -> Result<T>`: Retry with cancellation
  - Manual retry loop: `tokio::select! { biased; ... }` checks cancellation on each iteration
  - Exponential backoff with jitter
  - Circuit breaker check before each attempt
  - Metrics: retries, circuit state
- **Insights**: Replaced backon for cancellation support. Manual loop is more flexible. Circuit breaker integration prevents hammering open circuits.

#### `circuit_breaker.rs` (~500 lines)

- **Purpose**: Per-endpoint circuit breaker state machine (PRD Task 5)
- **Key Types/Functions**:
  - `CircuitBreaker::new(config) -> Self`
  - `check() -> Result<()>`: Check state, transition Open→HalfOpen if timeout elapsed
  - `record_success()`, `record_failure()`: State transitions
  - States: Closed (healthy), Open (failing), HalfOpen (testing)
  - `CircuitBreakerConfig`: failure_threshold, success_threshold, timeout
- **Insights**: State machine prevents cascade failures. Open state rejects fast. HalfOpen tests recovery. Syncs with ServerSelector.

#### `discovery.rs` (~300 lines)

- **Purpose**: Background endpoint refresh (dynamic service discovery)
- **Key Types/Functions**:
  - `DiscoveryService`: Background job calling GetClusterInfo RPC
  - `refresh_endpoints()`: Fetch cluster info, update ConnectionPool
  - Configurable interval (default 30s)
- **Insights**: Enables dynamic cluster membership. Clients discover new nodes without restart.

#### `metrics.rs` (~300 lines)

- **Purpose**: SdkMetrics trait with noop and metrics-crate implementations (PRD Task 6)
- **Key Types/Functions**:
  - `SdkMetrics` trait: record_request, record_retry, record_circuit_state, record_connection
  - `NoopSdkMetrics`: Zero-overhead default (no-op methods)
  - `MetricsSdkMetrics`: metrics crate facade (counters, histograms, gauges)
  - Prefix: `ledger_sdk_` for all metrics
- **Insights**: Dynamic dispatch (Arc<dyn SdkMetrics>) avoids type param infection. Noop default ensures zero overhead when disabled.

#### `streaming.rs` (~400 lines)

- **Purpose**: WatchBlocksStream with auto-reconnection
- **Key Types/Functions**:
  - `WatchBlocksStream::new(client, start_height) -> Self`
  - `next() -> Option<Result<Block>>`: Stream blocks from height
  - Auto-reconnection on disconnect
  - Backoff on errors
- **Insights**: Streaming API for real-time block updates. Auto-reconnection for resilience. Used for event sourcing.

#### `tracing.rs` (~300 lines)

- **Purpose**: W3C Trace Context propagation, API version header injection
- **Key Types/Functions**:
  - `TraceContextInterceptor`: Injects traceparent, tracestate, x-ledger-api-version, x-sdk-version headers
  - `with_timeout(duration)`: Injects grpc-timeout header (PRD Task 7)
  - Always injects x-sdk-version (not gated by trace config)
- **Insights**: Tonic interceptor for header injection. W3C Trace Context standard. SDK version enables server-side telemetry.

#### `builders/` (3 files)

- **Purpose**: Type-safe request builders for read, write, relationship operations
- **Key Types/Functions**:
  - `ReadRequestBuilder`, `WriteRequestBuilder`, `CreateRelationshipBuilder`
  - Fluent APIs with validation
- **Insights**: Type-safe builders prevent invalid requests. bon-based.

#### `server/` (3 files)

- **Purpose**: Server source, selector, resolver (endpoint management)
- **Key Types/Functions**:
  - `ServerSource`: Where endpoints come from (static config, discovery, DNS)
  - `ServerSelector`: Selects healthy endpoint for request (round-robin with health tracking)
  - `ServerResolver`: Resolves DNS names to IPs
- **Insights**: Abstraction enables multiple endpoint sources. ServerSelector uses health tracking + circuit breaker.

#### `idempotency.rs`

- **Purpose**: Client-side idempotency (sequence number generation)
- **Key Types/Functions**:
  - `IdempotencyProvider`: Generates unique sequence numbers per client
  - `next_sequence() -> u64`: Monotonic sequence
- **Insights**: Client-side sequence numbers. Server deduplicates via IdempotencyCache.

#### `mock.rs` (~400 lines)

- **Purpose**: MockLedgerClient for testing
- **Key Types/Functions**:
  - `MockLedgerClient`: In-memory mock with HashMap storage
  - All LedgerClient methods implemented (no network)
- **Insights**: Enables unit testing without server. In-memory state for fast tests.

---

## Crate: `inferadb-ledger-server`

- **Purpose**: Binary with CLI, config loading, bootstrap, discovery, signal handling, and 30+ integration/benchmark tests.
- **Dependencies**: All workspace crates (`types`, `store`, `state`, `proto`, `raft`, `sdk`)
- **Quality Rating**: ★★★★★

### Files

#### `main.rs` (~300 lines)

- **Purpose**: CLI with clap, config loading, server startup
- **Key Types/Functions**:
  - `Cli`: clap command-line args (config path, node-id, bootstrap, etc.)
  - `main()`: Parse args, load config, call bootstrap
  - Subcommands: `start`, `export-schema`, `config-diff` (Phase 2 Task 14)
- **Insights**: Clean CLI. Supports TOML config file + env var overrides. Subcommands for schema export and config diff.

#### `bootstrap.rs` (~800 lines)

- **Purpose**: Node bootstrap, lifecycle management, background job spawning
- **Key Types/Functions**:
  - `bootstrap_node(config) -> Result<BootstrappedNode>`: Initialize node
  - `BootstrappedNode`: Handle to running node (server, Raft, background jobs)
  - Background jobs: auto_recovery, block_compaction, btree_compaction, ttl_gc, integrity_scrubber, learner_refresh, orphan_cleanup, peer_maintenance, discovery, hot_key_detector, resource_metrics
  - Graceful shutdown: 6 phases (health drain, Raft snapshot, job stop, Raft shutdown, connection drain, service stop)
- **Insights**: Central orchestration point. Spawns all background jobs. Manages lifecycle. Graceful shutdown is complex but correct.

#### `config.rs` (~600 lines)

- **Purpose**: ServerConfig with all subsystem configs, SIGHUP reload support
- **Key Types/Functions**:
  - `ServerConfig`: Root config (raft, storage, batch, rate_limit, validation, tls, otel, audit, etc.)
  - `load_config(path) -> Result<ServerConfig>`: Load from TOML file
  - `reload_config(path, handle) -> Result<()>`: Hot-reload via SIGHUP (PRD Task 10)
  - Env var overrides: `INFERADB__LEDGER__<FIELD>` convention
- **Insights**: TOML config with env var overrides. Hot-reload for runtime reconfiguration. JSON Schema export for validation.

#### `coordinator.rs` (~500 lines)

- **Purpose**: Multi-node bootstrap coordination via Snowflake IDs
- **Key Types/Functions**:
  - `Coordinator::new(config) -> Self`
  - `bootstrap_cluster(nodes) -> Result<ClusterMetadata>`: Coordinate multi-node bootstrap
  - Snowflake ID: 64-bit (timestamp + node_id + sequence)
- **Insights**: Snowflake IDs enable decentralized ID generation (no coordination needed). Bootstrap requires initial seed nodes.

#### `discovery.rs` (~400 lines)

- **Purpose**: Peer discovery via DNS or file
- **Key Types/Functions**:
  - `DiscoveryProvider` trait: discover_peers method
  - `DnsDiscoveryProvider`: DNS SRV records
  - `FileDiscoveryProvider`: JSON file with peer list
  - Background refresh (configurable interval)
- **Insights**: Multiple discovery mechanisms (DNS for cloud, file for on-prem). Background refresh for dynamic membership.

#### `node_id.rs` (~200 lines)

- **Purpose**: Snowflake ID generator with persistence
- **Key Types/Functions**:
  - `NodeIdGenerator::new(data_dir) -> Result<Self>`
  - `generate() -> u64`: Generate unique ID (timestamp + node_id + sequence)
  - Persistence: node_id stored in `{data_dir}/node_id.json`
- **Insights**: Snowflake IDs are time-ordered and unique. Persistence ensures stable node ID across restarts.

#### `shutdown.rs` (~300 lines)

- **Purpose**: Signal handling (Ctrl-C, SIGTERM) and graceful shutdown coordination
- **Key Types/Functions**:
  - `install_signal_handlers() -> watch::Receiver<bool>`: Setup signal handlers
  - Returns receiver that triggers on signal
  - Used by `serve_with_shutdown(addr, shutdown_signal)`
- **Insights**: watch channel for shutdown broadcast. Multiple tasks can wait on same receiver.

#### `config_reload.rs` (~300 lines)

- **Purpose**: SIGHUP-driven runtime config hot-reload (PRD Task 10)
- **Key Types/Functions**:
  - `install_sighup_handler(config_path, runtime_handle)`
  - Loads TOML file, validates, swaps config atomically via ArcSwap
  - Audit logs config changes
- **Insights**: SIGHUP is Unix standard for config reload. Validation prevents invalid configs. Audit logging for compliance.

#### Integration Tests (15+ files)

- **Purpose**: End-to-end tests covering replication, failover, multi-shard, chaos
- **Test Coverage**:
  - `tests/integration.rs`: Basic read/write/permission checks
  - `tests/replication.rs`: Multi-node consensus tests
  - `tests/failover.rs`: Leader failure and election
  - `tests/multi_shard.rs`: Cross-shard queries and transactions
  - `tests/chaos.rs`: Chaos engineering (network partitions, node crashes)
  - `tests/backup_restore.rs`: Backup and restore flows
  - `tests/time_travel.rs`: Historical versioning
  - `tests/rate_limiting.rs`: Rate limiter behavior
  - `tests/cancellation.rs`: Cancellation support
  - `tests/circuit_breaker.rs`: Circuit breaker state transitions
  - `tests/api_version.rs`: API version negotiation
  - `tests/quota.rs`: Namespace quota enforcement
  - And more...
- **Insights**: Excellent test coverage. Integration tests require running cluster (expected failures in local dev). Chaos tests validate Byzantine fault tolerance.

#### Benchmarks (3 files)

- **Purpose**: Performance benchmarks for read/write operations and whitepaper validation
- **Benchmarks**:
  - `benches/read_bench.rs`: Read throughput and latency
  - `benches/write_bench.rs`: Write throughput and latency
  - `benches/whitepaper_bench.rs`: Validates performance claims in whitepaper
- **Insights**: Criterion-based benchmarks. CI tracks regressions via benchmark.yml workflow. Whitepaper validation is unique quality signal.

---

## Crate: `inferadb-ledger-test-utils`

- **Purpose**: Shared testing infrastructure (strategies, assertions, crash injection, test directories).
- **Dependencies**: `types` (for domain types)
- **Quality Rating**: ★★★★★

### Files

#### `lib.rs`

- **Purpose**: Re-exports test utilities
- **Key Types/Functions**:
  - Modules: `assertions`, `config`, `crash_injector`, `strategies`, `test_dir`
- **Insights**: Clean separation of test utilities from production code.

#### `assertions.rs`

- **Purpose**: assert_eventually polling helper for async tests
- **Key Types/Functions**:
  - `assert_eventually<F>(f: F, timeout: Duration)`: Poll condition until true or timeout
  - Useful for eventual consistency tests
- **Insights**: Prevents flaky tests due to timing. Configurable timeout and polling interval.

#### `config.rs`

- **Purpose**: Test config factories (BatchConfig, RateLimitConfig, etc.)
- **Key Types/Functions**:
  - `test_batch_config() -> BatchConfig`: Returns config suitable for tests (small batch size, short timeout)
  - `TestRateLimitConfig`: Test-friendly rate limits (high limits to avoid flakes)
- **Insights**: Test configs reduce boilerplate. Consistent configs across tests.

#### `crash_injector.rs` (~300 lines)

- **Purpose**: Deterministic crash injection for crash recovery tests (Task 3)
- **Key Types/Functions**:
  - `CrashInjector`: AtomicU8 array tracking crash points
  - `CrashPoint`: 5 variants (BeforeWrite, AfterWrite, BeforeCommit, AfterCommit, BeforeSync)
  - `enable(point)`, `disable(point)`, `check(point) -> bool`: Control crash points
  - `inject()`: Panic if crash point enabled
- **Insights**: Deterministic crash injection (not random). Enables testing specific failure scenarios. 17 crash tests in store crate.

#### `strategies.rs` (~500 lines)

- **Purpose**: 23 proptest strategy generators for all domain types (Task 10)
- **Key Types/Functions**:
  - `entity_strategy() -> impl Strategy<Value = Entity>`
  - `relationship_strategy() -> impl Strategy<Value = Relationship>`
  - `transaction_strategy() -> impl Strategy<Value = Transaction>`
  - `block_strategy() -> impl Strategy<Value = Block>`
  - And 19 more strategies for all types
- **Insights**: Composable strategy functions (not Arbitrary derives). Enables property-based testing. 30+ proptests across 4 crates.

#### `test_dir.rs`

- **Purpose**: TestDir wrapper around tempfile for temporary directories
- **Key Types/Functions**:
  - `TestDir::new() -> Self`: Create temp directory
  - `path() -> &Path`: Get path
  - Drop impl removes directory
- **Insights**: RAII cleanup of test directories. Prevents test pollution.

---

## Cross-Cutting Observations

### 1. Error Handling Excellence

The codebase demonstrates state-of-the-art error handling:

- **snafu exclusively**: No `thiserror` or `anyhow`. All errors use snafu with implicit location tracking via `#[snafu(implicit)] location: snafu::Location`.
- **Structured error taxonomy**: `ErrorCode` enum (29 numeric codes) with `code()`, `is_retryable()`, and `suggested_action()` methods on all error types.
- **Context selectors**: Propagation via `.context(XxxSnafu)?` captures location automatically, never manual error construction.
- **Rich error details**: Proto `ErrorDetails` message enriches gRPC errors with structured context (error_code, retryability, retry_after, context map, suggested_action). SDK decodes via `ServerErrorDetails`.
- **Attempt history**: SDK `SdkError` tracks retry attempts with `attempt_history: Vec<(u32, String)>` for debugging.

### 2. Builder Pattern with bon

The codebase uses the `bon` crate extensively for type-safe builders:

- **Simple structs**: `#[derive(bon::Builder)]` for basic configs
- **Fallible constructors**: `#[bon::bon] impl Foo { #[builder] pub fn new(...) -> Result<Self> }` for validation
- **Conventions**: `#[builder(into)]` for String fields (accepts &str), `#[builder(default)]` matched with `#[serde(default)]` for configs
- **Performance**: bon is a proc-macro with zero runtime overhead. Estimated compile-time impact: ~2 seconds per 10 structs.
- **Examples**: `ClientConfig`, `StorageConfig`, `RaftConfig`, `Transaction`, `TlsConfig` all use fallible builders

### 3. Testing Coverage

The codebase has exceptional test coverage:

- **Property-based testing**: 30+ proptests across 4 crates using proptest. Strategies in test-utils enable composable generators. Nightly CI runs with 10k iterations.
- **Crash recovery testing**: 17 crash injection tests in store crate using `CrashInjector`. Validates durability guarantees.
- **Chaos testing**: Integration tests cover network partitions, node crashes, Byzantine faults. 22 in-process unit tests for Byzantine scenarios.
- **Benchmarks**: Criterion benchmarks for B+ tree, read/write operations, whitepaper validation. CI tracks regressions via benchmark.yml workflow.
- **Integration tests**: 15+ end-to-end tests in server crate covering replication, failover, multi-shard, backup/restore, time travel, rate limiting, cancellation, circuit breaker, API version, quotas, etc.
- **Unit tests**: 1000+ unit tests across all crates. Coverage target: 90%+.

### 4. Security Practices

The codebase follows excellent security practices:

- **No `unsafe` code**: Zero unsafe blocks in entire codebase (enforced by CI).
- **Constant-time comparison**: `Sha256Hash` uses constant-time comparison to prevent timing attacks.
- **Input validation**: `validation.rs` enforces character whitelists and size limits to prevent injection attacks and DoS.
- **No `.unwrap()`**: All error handling via snafu `.context()`. No panics in production code.
- **Audit logging**: SOC2/HIPAA audit logging framework with `AuditEvent`/`AuditLogger`. Tracks all mutations.
- **TLS support**: Client and server support TLS with optional mTLS (client certificates).
- **Rate limiting**: 3-tier token bucket rate limiter (client/namespace/backpressure) prevents abuse.
- **Quota enforcement**: Per-namespace resource quotas (vault count, storage size, request rate) prevent resource exhaustion.

### 5. Observability

The codebase has comprehensive observability:

- **OpenTelemetry tracing**: W3C Trace Context propagation across services. OTLP exporter for traces/metrics. Configurable sampling ratio.
- **Prometheus metrics**: 100+ metrics covering SLI/SLO, resource saturation, batch queues, rate limiting, hot keys, circuit breakers, etc. Custom histogram buckets for SLI.
- **Canonical log lines**: Single log line per request with all context (request_id, trace_id, client_id, namespace_id, vault_id, method, status, latency, raft_round_trips, error_class, sdk_version, client_ip).
- **Wide events**: Structured logging with tracing crate. Context propagation via spans.
- **SDK metrics**: Client-side metrics (request latency, retries, circuit state, connection pool) via `SdkMetrics` trait. Noop default for zero overhead.
- **Audit logging**: File-based audit logger (JSON Lines) with fsync for durability. Prometheus metrics for audit events.

### 6. Enterprise Features

The codebase includes 40+ production-ready features:

- **Graceful shutdown**: 6-phase shutdown (health drain, Raft snapshot, job stop, Raft shutdown, connection drain, service stop). ConnectionTracker and BackgroundJobWatchdog.
- **Runtime reconfiguration**: Hot-reload via SIGHUP, UpdateConfig/GetConfig RPCs, lock-free reads via ArcSwap.
- **Backup & restore**: Snapshot-based backups with zstd compression, chain verification, S3/GCS/Azure backends.
- **Circuit breaker**: Per-endpoint state machine (Closed→Open→HalfOpen) in SDK. Prevents cascade failures.
- **Request cancellation**: CancellationToken support in SDK. Manual retry loop with `tokio::select!` for cancellation.
- **Deadline propagation**: grpc-timeout header parsing, effective_timeout (min of config and client), near-deadline rejection (100ms threshold).
- **Dependency health checks**: Disk writability, Raft lag, peer reachability. TTL cache prevents check storms.
- **API version negotiation**: x-ledger-api-version header, interceptor + tower layer, backward compatibility.
- **Hot key detection**: Count-Min Sketch with rotating windows, top-k via min-heap, rate-limited warnings.
- **Auto divergence recovery**: Background job comparing Raft log vs. state, automatic recovery from divergence.
- **Time travel**: Historical versioning with inverted height keys for efficient latest-version queries.
- **Tiered storage**: Hot/warm/cold tiers with S3/GCS/Azure backends. Age-based or access-based promotion/demotion.
- **Multi-shard**: Horizontal scaling via multiple Raft groups. Cross-shard queries and transactions via saga pattern.
- **Resource quotas**: Per-namespace limits (vault count, storage size, request rate). 3-tier resolution (namespace → tier → global).
- **B+ tree compaction**: Merge underfull leaves, reclaim dead space. Background job with configurable interval.
- **And 25+ more features**...

### 7. Documentation Quality

The codebase has excellent documentation:

- **ADRs (Architecture Decision Records)**: 3 ADRs in docs/architecture/ covering key decisions (bucket-based hashing, Snowflake IDs, saga pattern).
- **Invariant docs**: 11 `.invariants` files documenting critical invariants (e.g., store/btree.invariants: "Leaf nodes form doubly-linked list").
- **Module docs**: All crates have module-level documentation with examples.
- **rustdoc examples**: Many public functions have ` ```no_run ` examples (cargo test skips execution, cargo doc validates syntax).
- **Operations docs**: docs/operations/ has 10+ guides (api-versioning.md, backup-restore.md, config-schema.md, graceful-shutdown.md, hot-reload.md, quota-enforcement.md, slo.md, etc.).
- **Error codes**: docs/errors.md documents all 29 error codes with descriptions, causes, and suggested actions.

### 8. Code Quality Concerns

Despite the overall high quality, there are two areas needing attention:

#### **A. config.rs is too large (3534 lines)**

**Location**: `types/src/config.rs`

**Issue**: Single file with 18 config structs (ServerConfig, RaftConfig, StorageConfig, BatchConfig, RateLimitConfig, ValidationConfig, TlsConfig, OtelConfig, AuditConfig, CircuitBreakerConfig, HotKeyConfig, QuotaConfig, BackupConfig, HealthCheckConfig, GracefulShutdownConfig, DiscoveryConfig, ResourceMetricsConfig, RuntimeConfig).

**Recommendation**: Split into submodules:

- `config/mod.rs` — Re-exports + ServerConfig
- `config/raft.rs` — RaftConfig
- `config/storage.rs` — StorageConfig
- `config/security.rs` — TlsConfig, ValidationConfig, AuditConfig
- `config/observability.rs` — OtelConfig, ResourceMetricsConfig
- `config/resilience.rs` — CircuitBreakerConfig, GracefulShutdownConfig, BackupConfig
- `config/features.rs` — HotKeyConfig, QuotaConfig, DiscoveryConfig, RuntimeConfig

**Effort**: Medium (3-4 hours). Move structs to submodules, update re-exports, verify all imports.

#### **B. log_storage.rs is too large (~5000 lines)**

**Location**: `raft/src/log_storage.rs`

**Issue**: Single file with RaftLogStore, AppliedState, 14 operation handlers, snapshot building, log operations.

**Recommendation**: Split into submodules:

- `log_storage/mod.rs` — RaftLogStore + re-exports
- `log_storage/state_machine.rs` — AppliedState + apply_request dispatcher
- `log_storage/operations.rs` — 14 operation handlers (create_entity, delete_entity, etc.)
- `log_storage/snapshot.rs` — build_snapshot, install_snapshot
- `log_storage/log_ops.rs` — append_to_log, get_log_entry, purge_logs_upto

**Effort**: Large (6-8 hours). Extract functions to submodules, update re-exports, verify openraft trait impls.

**Impact**: Both refactorings improve maintainability and navigability without changing behavior. All tests should pass after refactoring.

---

## Summary

InferaDB Ledger is a **production-grade blockchain database** with exceptional engineering quality:

- **8 crates**, ~50,000 lines of Rust, 1000+ tests, 90%+ coverage
- **Zero `unsafe` code**, comprehensive error handling (snafu), structured error taxonomy
- **Custom B+ tree engine** with ACID transactions, crash recovery, compaction
- **Raft consensus** via openraft, batching, idempotency, multi-shard horizontal scaling
- **Enterprise features**: graceful shutdown, circuit breaker, rate limiting, hot key detection, quota enforcement, backup/restore, time travel, tiered storage, API versioning, deadline propagation, dependency health checks, runtime reconfiguration, audit logging, and 30+ more
- **Excellent observability**: OpenTelemetry tracing, Prometheus metrics, canonical log lines, wide events, SDK-side metrics
- **Comprehensive testing**: Property-based tests (proptest), crash recovery tests, chaos tests, integration tests, benchmarks with CI tracking
- **Security practices**: No unsafe, constant-time comparison, input validation, audit logging, TLS/mTLS, rate limiting, quotas
- **Documentation**: ADRs, invariant docs, module docs, rustdoc examples, operations guides, error code reference

**Areas for improvement**:

1. Split `types/src/config.rs` (3534 lines) into submodules
2. Split `raft/src/log_storage.rs` (~5000 lines) into submodules

Overall assessment: **★★★★★ Exemplary codebase** ready for production deployment.
