[Documentation](../README.md) > Client API

# Client API

This document covers read and write operations, error handling, and pagination.

## Write Operations

### Single Write

```rust
struct WriteRequest {
    organization_slug: OrganizationSlug,
    vault_slug: Option<u64>,    // None for organization-level entities
    client_id: ClientId,
    sequence: u64,
    operations: Vec<Operation>,
    include_tx_proof: bool,     // Include transaction proof in response
}

enum Operation {
    // Relationship operations
    CreateRelationship { resource: String, relation: String, subject: String },
    DeleteRelationship { resource: String, relation: String, subject: String },

    // Entity operations
    SetEntity { key: String, value: Bytes, expires_at: Option<u64>, condition: Option<SetCondition> },
    DeleteEntity { key: String },
    ExpireEntity { key: String, expired_at: u64 },  // GC-initiated removal (distinct from user delete)
}
```

### Operation Semantics

All operations are idempotent, resolved by Raft total ordering:

| Operation            | Pre-state  | Post-state | Behavior                     |
| -------------------- | ---------- | ---------- | ---------------------------- |
| `CreateRelationship` | not exists | created    | Transaction succeeds         |
| `CreateRelationship` | exists     | no change  | Transaction succeeds (no-op) |
| `DeleteRelationship` | exists     | deleted    | Transaction succeeds         |
| `DeleteRelationship` | not exists | no change  | Transaction succeeds (no-op) |
| `SetEntity`          | any        | value set  | Transaction succeeds         |
| `DeleteEntity`       | exists     | deleted    | Transaction succeeds         |
| `DeleteEntity`       | not exists | no change  | Transaction succeeds (no-op) |

Note: `WriteResponse` is transaction-level (success or error), not per-operation. All operations in a transaction either succeed together or fail together.

### Conditional Writes

`SetEntity` supports optional conditions for compare-and-set:

```protobuf
message SetCondition {
  oneof condition {
    bool not_exists = 1;     // Only set if key doesn't exist
    uint64 version = 2;      // Only set if version matches
    bytes value_equals = 3;  // Only set if current value matches
    bool must_exists = 4;    // Only set if key already exists
  }
}
```

| Condition      | Use Case                        | Error Code         |
| -------------- | ------------------------------- | ------------------ |
| `not_exists`   | Create-only, unique constraints | `KEY_EXISTS`       |
| `must_exists`  | Update-only, safe modifications | `KEY_NOT_FOUND`    |
| `version`      | Optimistic locking              | `VERSION_MISMATCH` |
| `value_equals` | Exact state assertions          | `VALUE_MISMATCH`   |

**Version tracking**: Each entity stores version (block height of last modification) as a separate field in the Entity response.

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
- All writes must target same scope (all with `vault_slug` OR all without)

**Example: Unique email constraint**

```rust
ledger.batch_write(BatchWriteRequest {
    organization_slug,
    vault_slug: None,  // Entities are organization-level
    client_id: ClientId { id: "my-client".into() },
    sequence: 1,
    operations: vec![
        // Each BatchWriteOperation groups operations that succeed/fail together
        BatchWriteOperation {
            operations: vec![
                Operation {
                    op: Some(Op::SetEntity(SetEntity {
                        key: format!("_idx:user:email:{}", email),
                        value: user_id.as_bytes().to_vec(),
                        condition: Some(SetCondition {
                            condition: Some(Condition::NotExists(true)),
                        }),
                        expires_at: None,
                    })),
                },
                Operation {
                    op: Some(Op::SetEntity(SetEntity {
                        key: format!("user:{}", user_id),
                        value: serialize(&user),
                        condition: None,
                        expires_at: None,
                    })),
                },
            ],
        },
    ],
    include_tx_proofs: false,
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
    organization_slug,
    vault_slug,
    key: "user:123".into(),
}).await?;

// Verified read (includes proof)
let verified = client.verified_read(VerifiedReadRequest {
    organization_slug,
    vault_slug,
    key: "user:123".into(),
    include_chain_proof: false,
}).await?;
```

### Batch Read

Read multiple keys in a single RPC call for higher throughput:

```rust
let results = client.batch_read(BatchReadRequest {
    organization_slug,
    vault_slug: Some(vault_slug),
    keys: vec!["user:1".into(), "user:2".into(), "user:3".into()],
    consistency: ReadConsistency::Eventual,
}).await?;

for result in results.results {
    if result.found {
        println!("{}: {:?}", result.key, result.value);
    } else {
        println!("{}: not found", result.key);
    }
}
```

**Request:**

| Field          | Type            | Description                     |
| -------------- | --------------- | ------------------------------- |
| `organization_slug` | OrganizationSlug     | Target organization                |
| `vault`        | VaultSlug       | (Optional) Target vault         |
| `keys`         | string[]        | Keys to read (max 1000)         |
| `consistency`  | ReadConsistency | Consistency level for all reads |

**Response:**

| Field          | Type              | Description                   |
| -------------- | ----------------- | ----------------------------- |
| `results`      | BatchReadResult[] | Result for each requested key |
| `block_height` | uint64            | Block height at time of read  |

**BatchReadResult fields:**

| Field   | Type   | Description                |
| ------- | ------ | -------------------------- |
| `key`   | string | Requested key              |
| `value` | bytes  | Value (empty if not found) |
| `found` | bool   | Whether key exists         |

Results are returned in the same order as requested keys.

### Historical Reads

Read state as it existed at a specific block height:

```rust
// Historical read (proofs optional)
let entity = client.historical_read(HistoricalReadRequest {
    organization_slug,
    vault_slug,
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
    organization_slug,
    vault_slug,
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
    organization_slug,
    key_prefix: "user:".into(),
    at_height: None,           // Current state (or specific height)
    include_expired: false,
    limit: 100,
    page_token: String::new(),
    consistency: ReadConsistency::Eventual,
}).await?;
```

### Pagination

Page tokens are opaque to clients—base64-encoded, server-managed cursors.

**Internal structure** (implementation detail):

```rust
struct PageToken {
    version: u8,
    organization_slug: i64,
    vault_id: i64,  // Internal ID (not exposed to clients)
    last_key: Vec<u8>,
    at_height: u64,        // Consistent reads
    query_hash: [u8; 8],   // Prevents filter changes
}

struct EncodedToken {
    token: PageToken,
    hmac: [u8; 16],        // Prevents tampering
}
```

**Validation on each request**:

1. Decode and verify HMAC
2. Check organization/vault match request
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
let tip = client.get_tip(GetTipRequest { organization_slug, vault_slug }).await?;
let stream = client.watch_blocks(WatchBlocksRequest {
    organization_slug,
    vault_slug,
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
| `NOT_FOUND`           | Resource doesn't exist             | Unknown organization_slug, vault_slug   |
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

| Error Type      | Codes                                                                                                    | Purpose                   |
| --------------- | -------------------------------------------------------------------------------------------------------- | ------------------------- |
| `WriteError`    | `KEY_EXISTS`, `KEY_NOT_FOUND`, `VERSION_MISMATCH`, `VALUE_MISMATCH`, `ALREADY_COMMITTED`, `SEQUENCE_GAP` | CAS failures, idempotency |
| `ReadErrorCode` | `HEIGHT_UNAVAILABLE`                                                                                     | Historical read failures  |

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
