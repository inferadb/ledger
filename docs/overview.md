# Architecture Overview

Ledger is InferaDB's storage layer—a blockchain database for cryptographically verifiable auditing. It exposes a gRPC API and uses Raft consensus for replication.

## Design Goals

1. **High Performance**: Sub-millisecond reads, <50ms writes
2. **Fault Isolation**: Chain-per-vault ensures failures don't cascade
3. **Cryptographic Auditability**: Every state provable against a merkle root
4. **Fearless Replication**: Raft consensus with self-verifying chain structure
5. **Operational Simplicity**: Snapshot recovery, zero-downtime migrations

## Terminology

| Term              | Definition                                                                                                                               |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **Organization**  | Storage unit per organization. Contains entities, vaults, relationships. Isolated with separate Raft consensus per region.               |
| **Vault**         | Relationship store within an organization. Maintains its own cryptographic chain (separate `state_root`, `previous_hash`, block height). |
| **Entity**        | Key-value data in an organization (users, teams, clients, sessions). Supports TTL, versioning, conditional writes.                       |
| **Relationship**  | Authorization tuple: `(resource, relation, subject)`. Used by Engine for permission checks.                                              |
| **`_system`**     | Special organization for global data: user accounts, organization routing, signing keys, and refresh tokens. Replicated to all nodes.    |
| **Signing Key**   | Ed25519 key pair for JWT signing. Private key stored envelope-encrypted (RMK→DEK→key). Scoped globally or per-organization.              |
| **Refresh Token** | Long-lived opaque token for obtaining new JWT access tokens. Family-tracked for theft detection.                                         |
| **Credential**    | User authentication factor (passkey, TOTP, recovery code). Stored REGIONAL, encrypted in Raft log via `EncryptedUserSystemRequest`.      |
| **Passkey**       | WebAuthn credential storing a COSE public key and replay-protection counter. Verified by the calling service (e.g., Control).            |
| **TOTP**          | Time-based One-Time Password (RFC 6238). Secret stored encrypted; Ledger verifies codes in the service layer (not Raft apply).           |
| **Recovery Code** | One-time backup codes (SHA-256 hashed) generated during TOTP setup. Bypass TOTP when device is lost.                                     |

## Hierarchy

```
_system organization (global)
├── Users (global accounts)
├── Organization routing table
├── Signing keys (global + per-org)
└── Refresh tokens (session + vault)

org (per-organization)
├── Organization metadata
├── Members, Teams, Clients
├── Vault A (relationships + grants) ─── separate chain
├── Vault B (relationships + grants) ─── separate chain
└── ...
```

## Isolation Model

| Level        | Isolated                          | Shared                  |
| ------------ | --------------------------------- | ----------------------- |
| Organization | Entities, vaults, keys            | Region (Raft consensus) |
| Vault        | Cryptographic chain, `state_root` | Organization storage    |
| Region       | Physical nodes                    | Cluster                 |

Organizations within a region share physical nodes and a Raft group but maintain independent data. Vaults within an organization share storage but maintain separate cryptographic chains.

## Region Groups

A naive 1:1 organization-to-Raft mapping doesn't scale. Multiple organizations in the same geographic region share a single Raft group. Within each organization, vaults maintain independent cryptographic chains.

**What's shared (per region):**

- Raft consensus (single leader election, heartbeat, log)
- Physical replication
- Snapshot coordination

**What's independent (per vault):**

- Cryptographic chain (height, previous_hash, state_root)
- State tree and indexes
- Merkle proofs and verification

### Scaling

| Organizations | Vaults (avg 3/org) | Vaults/Region | Region Groups | Raft Machines | Heartbeats |
| ------------- | ------------------ | ------------- | ------------- | ------------- | ---------- |
| 1,000         | 3,000              | 100           | 30            | 90            | ~600/sec   |
| 10,000        | 30,000             | 100           | 300           | 900           | ~6K/sec    |
| 100,000       | 300,000            | 1,000         | 300           | 900           | ~6K/sec    |

### Region Assignment

- Organizations are assigned to a region at creation time
- All vaults within an organization reside in the same region (data residency)
- Organizations can migrate between regions (coordinated via `_system`)

## Block Structure

Blocks are packed into `RegionBlock` entries for Raft efficiency:

```rust
struct RegionBlock {
    region: Region,
    region_height: u64,
    previous_region_hash: Hash,
    vault_entries: Vec<VaultEntry>,
    timestamp: DateTime<Utc>,
    leader_id: NodeId,
    term: u64,
    committed_index: u64,
}

struct VaultEntry {
    organization_slug: OrganizationSlug,
    vault_id: VaultId,
    vault_height: u64,
    previous_vault_hash: Hash,
    transactions: Vec<Transaction>,
    tx_merkle_root: Hash,
    state_root: Hash,
}
```

Each `VaultEntry` computes `tx_merkle_root` from its own transactions only. Clients verify transaction inclusion without accessing other organizations' data.

**Physical storage**: RegionBlocks only (single write path). VaultBlocks extracted on-demand via O(1) offset lookup.

## ID Generation

Ledger uses **leader-assigned sequential IDs**. The leader assigns IDs during block construction; followers replay the same IDs from the Raft log.

| ID Type            | Size     | Generated By  | Strategy                                                 |
| ------------------ | -------- | ------------- | -------------------------------------------------------- |
| `OrganizationId`   | int64    | Ledger Leader | Sequential from `_meta:seq:organization` (internal only) |
| `OrganizationSlug` | uint64   | Snowflake     | External identifier for organizations in gRPC APIs       |
| `VaultId`          | int64    | Ledger Leader | Sequential from `_meta:seq:vault` (internal only)        |
| `VaultSlug`        | uint64   | Snowflake     | External identifier for vaults in gRPC APIs              |
| `UserId`           | int64    | Ledger Leader | Sequential from `_meta:seq:user`                         |
| `UserEmailId`      | int64    | Ledger Leader | Sequential from `_meta:seq:user_email`                   |
| `ClientId`         | string   | Control       | Opaque API key identifier                                |
| `TxId`             | 16 bytes | Ledger Leader | UUID assigned during block creation                      |
| `NodeId`           | string   | Admin         | Configuration (hostname or UUID)                         |

**Sequence counters** stored in `_system` organization:

```
_meta:seq:organization    → next OrganizationId (starts at 1; 0 = _system)
_meta:seq:vault           → next VaultId (internal)
_meta:seq:user            → next UserId
_meta:seq:user_email      → next UserEmailId
_meta:seq:email_verify    → next TokenId
_meta:seq:signing_key     → next SigningKeyId
_meta:seq:refresh_token   → next RefreshTokenId
```

**Dual-ID architecture:** Organizations and vaults use two identifiers — a sequential internal ID (`OrganizationId`/`VaultId`, `i64`) for storage key density and B+ tree performance, and an external Snowflake slug (`OrganizationSlug`/`VaultSlug`, `u64`) as the sole identifier exposed in gRPC APIs and the SDK. Internal IDs are never visible to API consumers. The `SlugResolver` at the gRPC service boundary translates slugs to internal IDs for storage operations.

**Determinism guarantee:**

1. Leader allocates ID and increments counter atomically (same block)
2. Block proposed to Raft with the assigned ID
3. Followers apply the block—they read the ID from the log, never generate
4. All nodes arrive at identical state

## Key Patterns

### In `_system` (organization_slug = 0)

| Entity Type          | Key Pattern                               | Example                                |
| -------------------- | ----------------------------------------- | -------------------------------------- |
| User                 | `user:{id}`                               | `user:1`                               |
| User email           | `user_email:{id}`                         | `user_email:1`                         |
| Email index          | `_idx:email:{email}`                      | `_idx:email:alice@example.com`         |
| User emails index    | `_idx:user_emails:{user_id}`              | `_idx:user_emails:1`                   |
| Organization         | `org:{id}`                                | `org:1`                                |
| Organization routing | `_dir:org_registry:{id}`                  | `_dir:org_registry:1`                  |
| Node info            | `_meta:node:{id}`                         | `_meta:node:A`                         |
| Signing key          | `signing_key:{id}`                        | `signing_key:1`                        |
| Signing key index    | `_idx:signing_key:kid:{kid}`              | `_idx:signing_key:kid:{uuid}`          |
| Signing key scope    | `_idx:signing_key:scope:...`              | `_idx:signing_key:scope:global`        |
| Refresh token        | `refresh_token:{id}`                      | `refresh_token:1`                      |
| Refresh token hash   | `_idx:refresh_token:hash:..`              | `_idx:refresh_token:hash:{hex}`        |
| User credential      | `user_credential:{uid}:{id}`              | `user_credential:1:1`                  |
| Credential type idx  | `_idx:user_credential:type:{uid}:{type}:` | `_idx:user_credential:type:1:passkey:` |
| TOTP challenge       | `_tmp:totp_challenge:{uid}:{nonce}`       | `_tmp:totp_challenge:1:a1b2...`        |
| Sequence counter     | `_meta:seq:{entity_type}`                 | `_meta:seq:user`                       |

### In an Organization

| Entity Type        | Key Pattern                           | Example                       |
| ------------------ | ------------------------------------- | ----------------------------- |
| Org metadata       | `_meta`                               | `_meta`                       |
| Member             | `member:{id}`                         | `member:100`                  |
| Member index       | `_idx:member:user:{user_id}`          | `_idx:member:user:1`          |
| Team               | `team:{id}`                           | `team:200`                    |
| Team members index | `_idx:team_members:{team_id}`         | `_idx:team_members:200`       |
| Client             | `client:{id}`                         | `client:400`                  |
| Session            | `session:{token}`                     | `session:abc123`              |
| Vault metadata     | `vault:{vault_id}:_meta`              | `vault:1:_meta`               |
| User grant         | `vault:{vault_id}:grant:user:{id}`    | `vault:1:grant:user:500`      |
| Team grant         | `vault:{vault_id}:grant:team:{id}`    | `vault:1:grant:team:501`      |
| Relationship       | `rel:{resource}#{relation}@{subject}` | `rel:doc:1#viewer@user:alice` |

### Index Patterns

| Relationship | Index Pattern                     | Value           | Example                              |
| ------------ | --------------------------------- | --------------- | ------------------------------------ |
| 1:1 lookup   | `_idx:{type}:{lookup_key}`        | entity_id       | `_idx:email:alice@example.com` → `1` |
| 1:many list  | `_idx:{parent_type}s:{parent_id}` | [child_id, ...] | `_idx:user_emails:1` → `[1, 2]`      |

**Atomicity requirement**: Entities and their indexes must be created/deleted atomically in the same transaction.

## Consistency Model

Ledger offers two read consistency levels:

| Read Type             | Guarantee                     | Latency Impact |
| --------------------- | ----------------------------- | -------------- |
| Eventually consistent | May return stale data (by ms) | Fastest        |
| Linearizable          | Reads own writes, total order | +1 RTT         |

**Eventually consistent** (default): Any node can serve the read immediately. During Raft replication (typically milliseconds), reads may return slightly stale data. Use for high-throughput scenarios where millisecond staleness is acceptable.

**Linearizable**: The node confirms it has applied all committed entries before responding. Guarantees you see your own writes and all writes that completed before your read. Use when correctness requires the latest state.

```bash
# Eventually consistent read (default)
grpcurl -plaintext \
  -d '{"organization_slug": {"slug": 1234567890}, "key": "user:alice"}' \
  localhost:50051 ledger.v1.ReadService/Read

# Linearizable read
grpcurl -plaintext \
  -d '{"organization_slug": {"slug": 1234567890}, "key": "user:alice", "linearizable": true}' \
  localhost:50051 ledger.v1.ReadService/Read
```

**Authorization recommendation**: For permission checks, eventual consistency is usually safe—the replication window is milliseconds. Use linearizable reads only when you need absolute certainty about the current state (e.g., immediately after granting access).

## Network Protocol

Ledger uses **gRPC/HTTP/2** for both client-facing APIs and inter-node Raft consensus.

| Factor                 | gRPC/HTTP/2                                  | Custom TCP Protocol                |
| ---------------------- | -------------------------------------------- | ---------------------------------- |
| Development velocity   | High (generated clients, rich tooling)       | Low                                |
| Operational complexity | Low (standard load balancers, observability) | High                               |
| Performance            | ~0.5ms per RPC overhead                      | ~0.1-0.2ms theoretical improvement |

At target scale (<50K ops/sec per cluster), the ~0.3ms overhead is negligible compared to consensus latency (~2-5ms) and disk I/O.

### Serialization

```
Client Request
    ↓
[1] Proto → Internal Types (gRPC decode)
    ↓
[2] Internal Types → Postcard (Raft log serialization)
    ↓
[3] Postcard → Disk (B-tree write)
```

**Why postcard for storage**: Proto optimizes for wire transmission; postcard optimizes for in-process speed (no schema overhead, minimal allocation).

## Performance Targets

### Same Datacenter (3-node cluster, 1M keys)

| Metric             | Target          | Rationale                                   |
| ------------------ | --------------- | ------------------------------------------- |
| Read (p50)         | <0.5ms          | B-tree lookup + gRPC                        |
| Read (p99)         | <2ms            | Tail latency from GC/compaction             |
| Read + proof (p99) | <10ms           | Bucket-based O(k) proof                     |
| Write (p50)        | <10ms           | Raft RTT + fsync                            |
| Write (p99)        | <50ms           | Includes state_root computation             |
| Write throughput   | 5,000 tx/sec    | 100 tx/batch × 50 batches/sec               |
| Read throughput    | 100,000 req/sec | Per node, follower reads scale horizontally |

### Multi-Region (~50ms RTT)

| Metric      | Target                            |
| ----------- | --------------------------------- |
| Read (p99)  | <5ms (follower reads still local) |
| Write (p99) | <150ms (dominated by network RTT) |

## Limitations

| Dimension         | Recommended Max  | Hard Limit         | Bottleneck                    |
| ----------------- | ---------------- | ------------------ | ----------------------------- |
| Vaults per region | 1,000            | 10,000             | Region block size, memory     |
| Region groups     | 10,000           | ~100,000           | Cluster coordination overhead |
| Total vaults      | 10M              | ~100M              | `_system` registry size       |
| Keys per vault    | 10M              | ~100M              | State tree memory             |
| Transactions/sec  | 5,000 per region | ~50,000 per region | Raft throughput               |

### Not Supported

- Cross-vault transactions (by design)
- Byzantine fault tolerance (trusted network assumption)
- Sub-millisecond writes (consensus overhead)
- Infinite retention (storage costs)
- Instant per-key merkle proofs (hybrid storage trade-off)
