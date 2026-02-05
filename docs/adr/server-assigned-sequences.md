# ADR: Server-Assigned Sequences

## Context

### The Problem

The InferaDB Ledger SDK experiences race conditions when multiple async writes target the same vault concurrently. The current design requires clients to assign monotonically increasing sequence numbers:

```
Client assigns seq=N   ──────────────────> RPC starts
Client assigns seq=N+1 ──────────────────> RPC starts
                       seq=N+1 arrives first ──> Server commits, expects N+2
                       seq=N arrives second ──> Server rejects as SEQUENCE_GAP
```

This is a fundamental design flaw—not a bug fix. Network timing is inherently non-deterministic, so client-assigned sequences cannot guarantee in-order arrival.

### Current Implementation

```protobuf
message WriteRequest {
  NamespaceId namespace_id = 1;
  optional VaultId vault_id = 2;
  ClientId client_id = 3;
  uint64 sequence = 4;           // Client-assigned, problematic
  repeated Operation operations = 5;
}
```

The server validates `sequence == last_committed + 1` and rejects gaps. This creates:

1. **False-positive rejections**: Valid concurrent writes rejected due to arrival order
2. **SDK complexity**: ~500 LoC for `SequenceTracker`, `PersistentSequenceTracker`, `FileSequenceStorage`
3. **Client burden**: Must persist sequence state durably before each write

### Why Not Fix at the SDK Level?

The SDK could serialize concurrent writes, but this:

- Adds latency (writes wait in queue)
- Complicates the SDK (locks, queues, timeouts)
- Doesn't solve the fundamental issue (server still expects strict ordering)

## Decision

**Move sequence assignment to the server.** Clients send a unique `idempotency_key` (UUID) per write; the server assigns monotonic sequences at Raft commit time.

### Protocol Changes

```protobuf
message WriteRequest {
  NamespaceId namespace_id = 1;
  optional VaultId vault_id = 2;
  ClientId client_id = 3;
  bytes idempotency_key = 4;     // 16-byte UUID (replaces sequence)
  repeated Operation operations = 5;
  bool include_tx_proof = 6;
}

message WriteSuccess {
  TxId tx_id = 1;
  uint64 block_height = 2;
  uint64 assigned_sequence = 3;  // Server-assigned (new)
  optional BlockHeader block_header = 4;
  optional MerkleProof tx_proof = 5;
}

message WriteError {
  WriteErrorCode code = 1;
  string key = 2;
  optional uint64 current_version = 3;
  optional bytes current_value = 4;
  string message = 5;
  optional TxId committed_tx_id = 6;
  optional uint64 committed_block_height = 7;
  // Removed: last_committed_sequence (no longer needed)
}

enum WriteErrorCode {
  WRITE_ERROR_CODE_UNSPECIFIED = 0;
  WRITE_ERROR_CODE_KEY_EXISTS = 1;
  WRITE_ERROR_CODE_KEY_NOT_FOUND = 2;
  WRITE_ERROR_CODE_VERSION_MISMATCH = 3;
  WRITE_ERROR_CODE_VALUE_MISMATCH = 4;
  WRITE_ERROR_CODE_ALREADY_COMMITTED = 5;
  // Removed: WRITE_ERROR_CODE_SEQUENCE_GAP
  WRITE_ERROR_CODE_IDEMPOTENCY_KEY_REUSED = 6; // New: same key, different payload
}
```

### Idempotency Model

| Aspect                | Old (Sequence-Based)                     | New (UUID-Based)                                |
| --------------------- | ---------------------------------------- | ----------------------------------------------- |
| **Key**               | `(ns_id, vault_id, client_id, sequence)` | `(ns_id, vault_id, client_id, idempotency_key)` |
| **Assignment**        | Client                                   | Client (key), Server (sequence)                 |
| **Validation**        | `seq == last + 1`                        | Key exists in cache?                            |
| **Retry**             | Same sequence returns cached result      | Same UUID returns cached result                 |
| **Concurrent writes** | One succeeds, others fail                | All succeed with different sequences            |

### Idempotency Key Format

**Format**: UUID v4 (128-bit, 16 bytes)

**Rationale**:

- 122 bits of entropy—collision probability is ~1 in 2^61 after 2^61 keys (birthday paradox)
- Standard format, well-understood security properties
- Compact wire representation (16 bytes vs 36-byte string)
- Generated client-side without coordination

**Wire encoding**: Raw bytes, not string. Proto field `bytes idempotency_key = 4`.

### TTL and Cache Behavior

**Cache TTL**: 24 hours (configurable via `LedgerConfig`)

**Cache behavior**:

| Scenario                             | Cache State  | Action                                |
| ------------------------------------ | ------------ | ------------------------------------- |
| New key                              | Not in cache | Process write, cache result           |
| Retry (same key + same payload)      | In cache     | Return cached `WriteSuccess`          |
| Reuse (same key + different payload) | In cache     | Return `IDEMPOTENCY_KEY_REUSED` error |
| Key after TTL expiry                 | Not in cache | Process as new write                  |

**Cache eviction**: LRU with TinyLFU admission (moka crate). Max entries configurable.

**After TTL expiry**: A reused key with different payload will succeed as a new write. This is acceptable because:

1. 24 hours is ample time for retry completion
2. Client-side UUID generation makes accidental reuse virtually impossible
3. Intentional reuse after 24 hours is a client bug, not a server concern

### Error Handling

**`IDEMPOTENCY_KEY_REUSED`**: Returned when:

- The idempotency key exists in cache
- The request payload differs from the cached request

This error is **not retryable**—it indicates a client bug (key reuse without proper retry semantics).

**Payload comparison**: Hash of serialized request (excluding idempotency_key field). This enables efficient comparison without storing full payloads.

### Server-Side Sequence Assignment

Sequences are assigned in the Raft state machine during log application:

```
1. Client sends WriteRequest with idempotency_key
2. Leader checks idempotency cache
   - Cache hit: Return cached WriteSuccess
   - Cache miss with same key, different payload: Return IDEMPOTENCY_KEY_REUSED
   - Cache miss: Continue
3. Leader proposes to Raft (no sequence yet)
4. Raft replicates and commits
5. State machine applies:
   a. Increment AppliedState.client_sequences[client_id]
   b. assigned_sequence = new value
   c. Apply operations to state
6. Cache (idempotency_key -> WriteSuccess with assigned_sequence)
7. Return WriteSuccess to client
```

**Why assign at apply time?**

- Deterministic: Same sequence assigned during leader failover replay
- Consistent: Followers compute identical sequences
- Avoids gaps: No sequence reserved until commit succeeds

### Migration Path

InferaDB Ledger is a new product—breaking changes are acceptable. No migration required:

1. Update proto definitions
2. Remove client-side sequence tracking
3. Update server idempotency cache
4. Deploy new server version
5. Deploy new SDK version

Existing deployments (if any) must upgrade atomically. Mixed versions are not supported.

## Consequences

### Positive

1. **Eliminates race condition**: Concurrent writes all succeed; order determined by Raft
2. **Simplifies SDK**: Remove ~500 LoC of sequence tracking complexity
3. **Removes client state burden**: No durable sequence storage required
4. **Better concurrency**: No client-side serialization needed
5. **Cleaner retry semantics**: Generate UUID once, retry with same UUID

### Negative

1. **Larger cache keys**: 16-byte UUID vs 8-byte sequence (minor impact)
2. **No client-visible ordering**: Clients can't predict assigned sequences (by design)
3. **Breaking change**: Requires coordinated SDK + server upgrade

### Neutral

1. **Cache size unchanged**: Same number of entries, slightly larger keys
2. **`GetClientState` still useful**: Returns `last_committed_sequence` for monitoring
3. **Audit trail unchanged**: Transactions still have sequences (server-assigned)

## Alternatives Considered

### Alternative 1: Client-Side Serialization

Serialize concurrent writes in the SDK using a per-vault mutex.

**Rejected because**:

- Adds latency (writes wait in queue)
- Complicates SDK (locks, timeouts, error handling)
- Doesn't solve the fundamental issue

### Alternative 2: Speculative Sequences

Client sends speculative sequence; server reassigns if wrong.

**Rejected because**:

- Adds complexity without benefit
- Still requires cache for reassignment lookup
- Complicates retry semantics

### Alternative 3: Request ID (String)

Use string request IDs instead of UUID bytes.

**Rejected because**:

- 36-byte string vs 16-byte binary (2x wire overhead)
- String comparison slower than byte comparison
- No additional benefit

## References

- [PRD: Server-Assigned Sequences](../../PRD.md)
- [DESIGN.md §5.3: Client Session & Sequencing](../../DESIGN.md)
- [docs/client/idempotency.md](../client/idempotency.md) (to be updated)
- [RFC 4122: UUID Specification](https://tools.ietf.org/html/rfc4122)
