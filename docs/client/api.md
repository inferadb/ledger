# Client API

This document covers read and write operations, error handling, and pagination.

## Write Operations

### Single Write

```rust
struct WriteRequest {
    namespace_id: NamespaceId,
    vault_id: Option<VaultId>,  // None for namespace-level entities
    client_id: ClientId,
    sequence: u64,
    operations: Vec<Operation>,
}

enum Operation {
    // Relationship operations
    CreateRelationship { resource: String, relation: String, subject: String },
    DeleteRelationship { resource: String, relation: String, subject: String },

    // Entity operations
    SetEntity { key: String, value: Bytes, expires_at: Option<u64>, condition: Option<SetCondition> },
    DeleteEntity { key: String },
}
```

### Operation Semantics

All operations are idempotent, resolved by Raft total ordering:

| Operation            | Pre-state  | Post-state | Return           |
| -------------------- | ---------- | ---------- | ---------------- |
| `CreateRelationship` | not exists | created    | `CREATED`        |
| `CreateRelationship` | exists     | no change  | `ALREADY_EXISTS` |
| `DeleteRelationship` | exists     | deleted    | `DELETED`        |
| `DeleteRelationship` | not exists | no change  | `NOT_FOUND`      |
| `SetEntity`          | any        | value set  | `OK`             |
| `DeleteEntity`       | exists     | deleted    | `DELETED`        |
| `DeleteEntity`       | not exists | no change  | `NOT_FOUND`      |

### Conditional Writes

`SetEntity` supports optional conditions for compare-and-set:

```protobuf
message SetCondition {
  oneof condition {
    bool not_exists = 1;     // Only set if key doesn't exist
    uint64 version = 2;      // Only set if version matches
    bytes value_equals = 3;  // Only set if current value matches
  }
}
```

| Condition      | Use Case                        | Error Code         |
| -------------- | ------------------------------- | ------------------ |
| `not_exists`   | Create-only, unique constraints | `KEY_EXISTS`       |
| `version`      | Optimistic locking              | `VERSION_MISMATCH` |
| `value_equals` | Exact state assertions          | `VALUE_MISMATCH`   |

**Version tracking**: Each entity stores version (block height of last modification) embedded in the value:

```rust
struct StoredEntity {
    version: u64,       // 8 bytes, block height
    expires_at: u64,    // 8 bytes, 0 = never
    value: Vec<u8>,     // User-provided value
}
```

### Batch Writes

`BatchWriteRequest` provides atomic multi-write operations:

| Property              | Guarantee                                                 |
| --------------------- | --------------------------------------------------------- |
| **Atomicity**         | All-or-nothing—if ANY condition fails, entire batch fails |
| **Ordering**          | Writes applied in array order                             |
| **Single block**      | All writes committed in same block                        |
| **Failure isolation** | On failure, no writes applied                             |

**Sequencing rules**:

- Single `client_id` per batch (set at batch level)
- Batch has single sequence number (all writes share it)
- All writes must target same scope (all with `vault_id` OR all without)

**Example: Unique email constraint**

```rust
ledger.batch_write(BatchWriteRequest {
    vault_id,
    writes: vec![WriteRequest {
        operations: vec![
            SetEntity {
                key: format!("_idx:user:email:{}", email),
                value: user_id.as_bytes().to_vec(),
                condition: Some(SetCondition::NotExists(true)),
            },
            SetEntity {
                key: format!("user:{}", user_id),
                value: serialize(&user),
                condition: None,
            },
        ],
    }],
}).await?;
```

### TTL and Expiration

```protobuf
message SetEntity {
  string key = 1;
  bytes value = 2;
  optional uint64 expires_at = 3;  // Unix epoch seconds; 0 = never
}
```

| Behavior         | Description                                       |
| ---------------- | ------------------------------------------------- |
| Read filtering   | Reads return null/skip if `now > expires_at`      |
| State inclusion  | Expired entities remain in state tree until GC    |
| Historical reads | Compare against block timestamp, not current time |

**Garbage collection**: Leader periodically removes expired entities using `ExpireEntity` operation (distinct from `DeleteEntity` for audit trail).

## Read Operations

### Current State Read

```rust
// Fast read (no proof)
let entity = client.read(ReadRequest {
    namespace_id,
    vault_id,
    key: "user:123".into(),
}).await?;

// Verified read (includes proof)
let verified = client.verified_read(VerifiedReadRequest {
    namespace_id,
    vault_id,
    key: "user:123".into(),
    include_chain_proof: false,
}).await?;
```

### Historical Reads

Read state as it existed at a specific block height:

```rust
// Historical read (proofs optional)
let entity = client.historical_read(HistoricalReadRequest {
    namespace_id,
    vault_id,
    key: "user:123".into(),
    at_height: 1000,
    include_proof: false,  // Set true for verification
}).await?;
```

**Latency by tier**:

| Scenario               | Latency   |
| ---------------------- | --------- |
| Current state          | <1ms      |
| Hot snapshot range     | 5-20ms    |
| Warm snapshot (S3)     | 100-500ms |
| Cold archive (Glacier) | 1-10s     |

**Height unavailability**:

| Condition     | Error                    | gRPC Status           | Recovery               |
| ------------- | ------------------------ | --------------------- | ---------------------- |
| Height > tip  | `at_height` out of range | `INVALID_ARGUMENT`    | Use `GetTip()`         |
| Height pruned | `HEIGHT_UNAVAILABLE`     | `FAILED_PRECONDITION` | Use more recent height |

### Read Consistency

| Read Type     | Consistency           | Use Case                      |
| ------------- | --------------------- | ----------------------------- |
| Leader read   | Linearizable          | Strong consistency required   |
| Follower read | Eventually consistent | High throughput, staleness OK |
| Verified read | Linearizable + proven | Audit, compliance             |

Default: Reads go to any replica. For strict consistency, read from leader or use verified reads.

### Query Operations

**List Relationships**:

```rust
let relationships = client.list_relationships(ListRelationshipsRequest {
    namespace_id,
    vault_id,
    resource: Some("doc:readme".into()),  // Filter by resource
    relation: Some("viewer".into()),       // Filter by relation
    subject: None,                          // No subject filter
    limit: 100,
    page_token: String::new(),
}).await?;
```

Index usage:

| Filters Provided    | Index Used | Example                          |
| ------------------- | ---------- | -------------------------------- |
| resource only       | obj_index  | "Who can access doc:readme?"     |
| subject only        | subj_index | "What can user:alice access?"    |
| resource + relation | obj_index  | "Who are viewers of doc:readme?" |
| relation only       | Full scan  | Avoid in production              |

**List Entities**:

```rust
let entities = client.list_entities(ListEntitiesRequest {
    namespace_id,
    vault_id: None,  // Namespace-level entities
    key_prefix: "user:".into(),
    include_expired: false,
    limit: 100,
    page_token: String::new(),
}).await?;
```

### Pagination

Page tokens are opaque to clients—base64-encoded, server-managed cursors.

**Internal structure** (implementation detail):

```rust
struct PageToken {
    version: u8,
    namespace_id: i64,
    vault_id: i64,
    last_key: Vec<u8>,
    at_height: u64,        // Consistent reads
    query_hash: [u8; 8],   // Prevents filter changes
    hmac: [u8; 16],        // Prevents tampering
}
```

**Validation on each request**:

1. Decode and verify HMAC
2. Check namespace/vault match request
3. Check query_hash matches current params
4. Resume from `last_key` at `at_height`

**Errors**:

- Invalid token: `INVALID_ARGUMENT` with "invalid page token"
- Mismatched context: `INVALID_ARGUMENT` with "page token does not match request"
- Changed filters: `INVALID_ARGUMENT` with "query parameters changed"

### Block Subscription

Subscribe to new blocks via gRPC streaming:

```rust
// Get current tip, then subscribe from tip+1
let tip = client.get_tip(GetTipRequest { vault_id }).await?;
let stream = client.watch_blocks(WatchBlocksRequest {
    vault_id,
    start_height: tip.height + 1,
}).await?;

// Server replays any blocks committed between GetTip and WatchBlocks
// No blocks are missed
```

`start_height` must be >= 1. For full replay from genesis, use `start_height = 1`.

## Error Handling

### Two-Tier Strategy

| Layer               | Mechanism                    | When Used                                   |
| ------------------- | ---------------------------- | ------------------------------------------- |
| **Transport/Auth**  | gRPC status codes            | Network, auth, validation, availability     |
| **Domain-specific** | `WriteError`/`ReadErrorCode` | Conditional write failures, sequence errors |

### gRPC Status Codes

| Status                | Meaning                            | Examples                           |
| --------------------- | ---------------------------------- | ---------------------------------- |
| `UNAUTHENTICATED`     | Missing or invalid credentials     | Bad API key, expired session       |
| `PERMISSION_DENIED`   | Valid auth but insufficient access | Write to read-only vault           |
| `INVALID_ARGUMENT`    | Malformed request                  | Invalid key format, bad page_token |
| `NOT_FOUND`           | Resource doesn't exist             | Unknown namespace_id, vault_id     |
| `UNAVAILABLE`         | Temporary failure                  | Leader election, node down         |
| `RESOURCE_EXHAUSTED`  | Rate limit or quota                | Too many requests                  |
| `FAILED_PRECONDITION` | Precondition failed                | Height unavailable (pruned)        |

### Domain-Specific Errors

```protobuf
message WriteError {
  WriteErrorCode code = 1;
  string key = 2;             // Which key's condition failed
  uint64 current_version = 3; // For retry
  bytes current_value = 4;    // If small
  string message = 5;
}
```

| Error Type      | Codes                                                                                   | Purpose                   |
| --------------- | --------------------------------------------------------------------------------------- | ------------------------- |
| `WriteError`    | `KEY_EXISTS`, `VERSION_MISMATCH`, `VALUE_MISMATCH`, `ALREADY_COMMITTED`, `SEQUENCE_GAP` | CAS failures, idempotency |
| `ReadErrorCode` | `HEIGHT_UNAVAILABLE`                                                                    | Historical read failures  |

**Smart retry pattern**:

```rust
match write_response.result {
    WriteResult::Success(s) => Ok(s.block_height),
    WriteResult::Error(e) if e.code == VERSION_MISMATCH => {
        retry_with_version(e.current_version)
    }
    WriteResult::Error(e) if e.code == KEY_EXISTS => {
        Err(AlreadyExists(e.key))
    }
    WriteResult::Error(e) => Err(WriteError(e)),
}
```

## Verification Scope

Not all operations are cryptographically verifiable:

| Operation               | Verifiable | Proof Type                      |
| ----------------------- | ---------- | ------------------------------- |
| Point read (single key) | Yes        | StateProof                      |
| Historical point read   | Yes        | StateProof + ChainProof         |
| Transaction inclusion   | Yes        | MerkleProof                     |
| Write committed         | Yes        | BlockHeader + TxProof           |
| List relationships      | No         | Trust server completeness       |
| List entities           | No         | Trust server completeness       |
| Pagination completeness | No         | Cannot prove no results omitted |

For verifiable list operations, maintain your own state by subscribing to `WatchBlocks` and replaying transactions.
