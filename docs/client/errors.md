[Documentation](../README.md) > Client API > Errors

# Error Reference

gRPC error codes and Ledger-specific error conditions.

## gRPC Status Codes

Ledger uses standard gRPC status codes with additional context in error details.

### Common Codes

| Code | Name                  | Meaning                         | Retry?             |
| ---- | --------------------- | ------------------------------- | ------------------ |
| 0    | `OK`                  | Success                         | -                  |
| 3    | `INVALID_ARGUMENT`    | Malformed request               | No                 |
| 5    | `NOT_FOUND`           | Resource doesn't exist          | No                 |
| 6    | `ALREADY_EXISTS`      | Resource already exists         | No                 |
| 9    | `FAILED_PRECONDITION` | State requirements not met      | Maybe              |
| 13   | `INTERNAL`            | Server error                    | Yes (with backoff) |
| 14   | `UNAVAILABLE`         | Service temporarily unavailable | Yes (with backoff) |

## Write Errors

### SEQUENCE_GAP

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `SEQUENCE_GAP`)

**Cause**: Client sent a sequence number higher than expected.

```
expected sequence 5, got 7
```

**Resolution**:

1. Call `GetClientState` to get the current sequence
2. Resume from `last_committed_sequence + 1`

```bash
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "client_id": {"id": "my-client"}}' \
  localhost:50051 ledger.v1.ReadService/GetClientState
```

**Recovery fields in error response:**

| Field                     | Type   | Description                        |
| ------------------------- | ------ | ---------------------------------- |
| `committed_tx_id`         | TxId   | Last successfully committed tx     |
| `committed_block_height`  | uint64 | Block containing last committed tx |
| `last_committed_sequence` | uint64 | Use this + 1 for next write        |

### KEY_EXISTS

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `KEY_EXISTS`)

**Cause**: Conditional write with `not_exists` condition failed because key already exists.

**Resolution**: Use `SetEntity` without condition to overwrite, or handle the existing value.

### KEY_NOT_FOUND

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `KEY_NOT_FOUND`)

**Cause**: Conditional write with `must_exists` condition failed because key doesn't exist.

**Resolution**: Create the key first, or remove the condition.

### VERSION_MISMATCH

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `VERSION_MISMATCH`)

**Cause**: Conditional write with `version` condition failed—current version differs from expected.

**Resolution**: Re-read the entity to get current version, then retry with updated version.

### VALUE_MISMATCH

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `VALUE_MISMATCH`)

**Cause**: Conditional write with `value_equals` condition failed—current value differs from expected.

**Resolution**: Re-read the entity to get current value, then decide how to proceed.

### ALREADY_COMMITTED

**Code**: `FAILED_PRECONDITION` (WriteErrorCode: `ALREADY_COMMITTED`)

**Cause**: Transaction with same `(client_id, sequence)` was already committed.

**Resolution**: This is expected for idempotent retries. The cached response is returned.

### NAMESPACE_NOT_FOUND

**Code**: `NOT_FOUND`

**Cause**: Operation references a non-existent namespace.

**Resolution**: Create the namespace first with `AdminService/CreateNamespace`.

### VAULT_NOT_FOUND

**Code**: `NOT_FOUND`

**Cause**: Operation references a non-existent vault.

**Resolution**: Create the vault first with `AdminService/CreateVault`.

### VAULT_UNAVAILABLE

**Code**: `UNAVAILABLE`

**Cause**: Vault has detected state divergence and is not accepting operations.

**Resolution**:

1. Check `ledger_determinism_bug_total` metric
2. Run `AdminService/RecoverVault` to attempt recovery
3. If recovery fails, restore from healthy replica

### LEADER_NOT_AVAILABLE

**Code**: `UNAVAILABLE`

**Cause**: No Raft leader is currently elected.

**Resolution**: Retry with exponential backoff. Election should complete within seconds.

```go
// Example retry logic
maxRetries := 5
for i := 0; i < maxRetries; i++ {
    resp, err := client.Write(ctx, req)
    if status.Code(err) == codes.Unavailable {
        time.Sleep(time.Duration(1<<i) * 100 * time.Millisecond)
        continue
    }
    return resp, err
}
```

### RATE_LIMITED

**Code**: `RESOURCE_EXHAUSTED`

**Cause**: Request rate exceeded namespace limits.

**Resolution**: Implement rate limiting on the client side or request limit increase.

## Read Errors

### HEIGHT_UNAVAILABLE

**Code**: `FAILED_PRECONDITION`

**Cause**: Requested historical height has been pruned (compacted retention policy).

**Resolution**: Use a more recent height or omit height for current state.

```bash
# Get current tip to find available height
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "vault_id": {"id": "1"}}' \
  localhost:50051 ledger.v1.ReadService/GetTip
```

## Admin Errors

### CLUSTER_NOT_READY

**Code**: `FAILED_PRECONDITION`

**Cause**: Cluster hasn't completed bootstrap.

**Resolution**: Wait for bootstrap to complete. Check `GetNodeInfo` for status.

### NODE_ALREADY_MEMBER

**Code**: `ALREADY_EXISTS`

**Cause**: `JoinCluster` called for a node that's already a member.

**Resolution**: Node is already in the cluster; no action needed.

### QUORUM_LOSS

**Code**: `UNAVAILABLE`

**Cause**: Operation would result in quorum loss.

**Resolution**: Ensure sufficient nodes remain before removing members.

### SNAPSHOT_IN_PROGRESS

**Code**: `FAILED_PRECONDITION`

**Cause**: `CreateSnapshot` called while another snapshot is in progress.

**Resolution**: Wait for current snapshot to complete.

## Error Details

Ledger includes structured error details when available:

```protobuf
message ErrorDetail {
  string code = 1;          // Ledger-specific error code
  string message = 2;       // Human-readable description
  map<string, string> metadata = 3;  // Additional context
}
```

Example with grpcurl:

```bash
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "999"}}' \
  localhost:50051 ledger.v1.ReadService/Read 2>&1
```

Output:

```
ERROR:
  Code: NotFound
  Message: namespace not found
  Details:
    - code: NAMESPACE_NOT_FOUND
      message: namespace with id 999 does not exist
      metadata:
        namespace_id: "999"
```

## Client Best Practices

### Retry Strategy

```python
# Pseudo-code retry strategy
RETRIABLE_CODES = {
    codes.UNAVAILABLE,
    codes.INTERNAL,
    codes.RESOURCE_EXHAUSTED,
}

def call_with_retry(fn, max_retries=5):
    for attempt in range(max_retries):
        try:
            return fn()
        except grpc.RpcError as e:
            if e.code() not in RETRIABLE_CODES:
                raise
            if attempt == max_retries - 1:
                raise
            sleep(min(2 ** attempt * 0.1, 5))  # Exponential backoff, max 5s
```

### Handling Sequence Gaps

```rust
// Pseudo-code for sequence recovery
async fn write_with_recovery(client: &Client, ops: Vec<Op>) -> Result<()> {
    loop {
        match client.write(namespace_id, client_id, sequence, &ops).await {
            Ok(resp) => {
                sequence = resp.committed_sequence + 1;
                return Ok(());
            }
            Err(e) if e.code() == FAILED_PRECONDITION => {
                // Sequence gap - recover
                let state = client.get_client_state(namespace_id, client_id).await?;
                sequence = state.last_committed_sequence + 1;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Idempotency

All write operations are idempotent when using the same `(client_id, sequence)` pair:

```bash
# First call
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "client_id": {"id": "c1"}, "sequence": "1", ...}' \
  localhost:50051 ledger.v1.WriteService/Write

# Retry with same client_id and sequence returns cached response
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "client_id": {"id": "c1"}, "sequence": "1", ...}' \
  localhost:50051 ledger.v1.WriteService/Write
```

The idempotency cache ensures the operation is only applied once.

> **Note**: The idempotency cache is in-memory and does not survive leader failover. After a failover, a retried request may execute twice if the original response was not received. For operations requiring exactly-once semantics across failovers, use application-level idempotency keys stored in the vault itself.
