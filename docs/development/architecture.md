[Documentation](../README.md) > Development > Architecture

# Development Architecture

Crate structure, key abstractions, and code organization for contributors.

## Crate Dependency Graph

```
                    ┌────────────────┐
                    │     server     │  Binary entry point
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │   services     │  gRPC services, server assembly
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │      raft      │  Consensus, batching, background jobs
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │     state      │  Domain logic
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │     store      │  B+ tree storage engine
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │     types      │  Shared primitives
                    └────────────────┘

  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
  │     proto      │  │      sdk       │  │   test-utils   │
  └────────────────┘  └────────────────┘  └────────────────┘
```

## Crate Overview

### `inferadb-ledger-types`

Foundational types used by all other crates.

**Key modules:**

- `hash.rs` - SHA-256 and seahash implementations
- `merkle.rs` - Merkle tree and proof types
- `error.rs` - Error types with snafu
- `identifiers.rs` - OrganizationSlug, VaultId, NodeId, etc.

**Design principle:** No I/O, no external dependencies beyond crypto.

### `inferadb-ledger-store`

Custom B+ tree storage engine with MVCC.

**Key abstractions:**

- `Backend` - Storage interface (memory, file)
- `PageManager` - Page allocation and caching
- `BTree` - B+ tree implementation
- `Transaction` - MVCC transaction with snapshot isolation

**Tables:** 21 compile-time tables (`TableId` enum) for entities, relationships, blocks, Raft log, vault metadata, slug indexes, client sequences, compaction metadata, and more.

### `inferadb-ledger-state`

Domain logic built on top of store.

**Key abstractions:**

- `StorageEngine` - Store wrapper with transaction helpers
- `StateLayer` - Applies blocks, computes state roots
- `EntityStore` - Entity CRUD operations
- `RelationshipStore` - Relationship CRUD operations

**Invariant:** All state modifications go through `StateLayer.apply_block()`.

### `inferadb-ledger-raft`

Raft consensus via [Openraft](https://github.com/datafuselabs/openraft), batching, background jobs.

**Key abstractions:**

- `RaftNode` - Openraft node wrapper
- `LogStorage` - Raft log backed by store
- `StateMachine` - State machine implementation (apply handlers for all `LedgerRequest` variants)
- `BatchProcessor` - Transaction batching
- `SagaOrchestrator` - Multi-step workflow orchestration (org creation, signing key bootstrap)
- `TokenMaintenanceJob` - Background cleanup for expired tokens and key lifecycle transitions
- `RateLimiter` - 3-tier rate limiting (per-app, per-org, global)

### `inferadb-ledger-services`

gRPC service implementations and server assembly.

**Key abstractions:**

- `LedgerServer` - Assembles all gRPC services with shared infrastructure
- `JwtEngine` - Ed25519 token signing/validation with ArcSwap key cache
- `ServiceContext` - Shared Raft/state/logging infrastructure for service handlers

**gRPC services:**

- `ReadService` - Query entities and relationships
- `WriteService` - Modify state with cross-region forwarding
- `AdminService` - Cluster management, configuration, backups
- `OrganizationService` - Organization CRUD, members, teams
- `VaultService` - Vault CRUD and health
- `UserService` - User accounts, emails, verification
- `AppService` - Application management, credentials, connections
- `TokenService` - JWT token lifecycle (sessions, vault tokens, signing keys)
- `EventsService` - Audit event queries and ingestion
- `HealthService` - Health checks with dependency probes
- `RaftService` - Node-to-node Raft RPCs
- `SystemDiscoveryService` - Peer discovery

### `inferadb-ledger-proto`

Generated protobuf code and domain type conversions.

**Key modules:**

- `generated/` - Auto-generated from `proto/ledger/v1/ledger.proto`
- `convert.rs` - `From`/`TryFrom` conversions between proto and domain types

### `inferadb-ledger-server`

Binary entry point and configuration.

**Key modules:**

- `main.rs` - CLI parsing, bootstrap
- `config.rs` - Configuration from environment
- `bootstrap.rs` - Cluster formation logic
- `node_id.rs` - Snowflake ID generation

### `inferadb-ledger-sdk`

Client SDK for Rust applications.

**Key types:**

- `LedgerClient` - High-level client
- `ConnectionPool` - Connection management
- `RetryPolicy` - Automatic retry with backoff

### `inferadb-ledger-test-utils`

Shared test utilities.

**Features:**

- `TestCluster` - Multi-node cluster for integration tests
- Test fixtures and helpers
- Mock implementations

## Code Organization Conventions

### File Structure

```
crates/<name>/
├── Cargo.toml
├── src/
│   ├── lib.rs          # Public API, re-exports
│   ├── <module>.rs     # Module implementation
│   └── <module>/       # Complex modules with submodules
│       ├── mod.rs
│       └── *.rs
└── tests/              # Integration tests
```

### Module Guidelines

1. **One type per file** (for complex types)
2. **Unit tests in same file** with `#[cfg(test)] mod tests`
3. **Public API at crate root** via `lib.rs` re-exports
4. **Internal modules** marked `pub(crate)`

### Error Handling

All crates use snafu with implicit location tracking:

```rust
#[derive(Debug, Snafu)]
pub enum CrateError {
    #[snafu(display("Operation failed: {source}"))]
    Operation {
        source: SomeError,
        #[snafu(implicit)]
        location: snafu::Location,
    },
}
```

Propagate with context selectors, never construct errors manually.

## Key Data Flows

### Write Path

```
Client → WriteService → BatchProcessor → RaftNode
    → LogStorage (append) → Quorum replication
    → StateMachine (apply) → StateLayer (execute)
    → Store (commit) → Response
```

### Read Path

```
Client → ReadService → StateLayer → Store → Response
```

### Bootstrap Path

```
Server start → Config::from_env() → NodeId generation
    → Peer discovery → Raft cluster formation
    → Leader election → Ready
```

## Adding a New Feature

### New gRPC RPC

1. Define in `proto/ledger/v1/ledger.proto`
2. Run `just proto`
3. Add proto conversions in `crates/proto/src/convert.rs`
4. Implement handler in `crates/services/src/services/<service>.rs`
5. Add tests in `crates/server/tests/`

### New Storage Table

1. Add table definition in `crates/store/src/tables.rs`
2. Add operations in `crates/state/src/`
3. Update state root computation if needed

### New Configuration Option

1. Add field to `Config` in `crates/server/src/config.rs`
2. Add serde attributes for env var mapping
3. Document in `docs/operations/configuration.md`

## Related Documentation

- [Testing Guide](testing.md) - How to run and write tests
- [Internals](../internals/) - Implementation details
- [DESIGN.md](../../DESIGN.md) - System design specification
