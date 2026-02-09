# Error Codes

InferaDB Ledger assigns a unique numeric error code to every error variant.
These codes enable programmatic error handling without parsing message strings.

## Wire Format

Error codes are transmitted as strings in gRPC error detail metadata
(e.g., `"1000"`). Use `ErrorCode::as_u16()` for serialization and
`ErrorCode::from_u16()` for deserialization.

## Code Ranges

| Range     | Domain    | Description                           |
| --------- | --------- | ------------------------------------- |
| 1000-1099 | Storage   | Database open, transactions, tables   |
| 1100-1199 | Storage   | Corruption, snapshots, key encoding   |
| 2000-2099 | Consensus | Leadership, proposals                 |
| 2100-2199 | Consensus | Log storage, state machine, network   |
| 3000-3099 | App       | Storage/consensus wrappers, hashing   |
| 3100-3199 | App       | Not-found, preconditions, idempotency |
| 3200-3299 | App       | Serialization, config, I/O, internal  |

## Storage Errors (1000-1199)

| Code | Name                    | Retryable | Cause                                                    | Recovery Action                                                                    |
| ---- | ----------------------- | --------- | -------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| 1000 | `StorageDatabaseOpen`   | No        | Database file not found, permission denied, or corrupted | Verify path exists with correct permissions. Restore from backup if corrupted.     |
| 1001 | `StorageTransaction`    | Yes       | Commit conflict or write lock timeout                    | Retry with backoff. Only one write transaction can be active at a time.            |
| 1002 | `StorageTableOperation` | No        | B-tree insert, delete, or lookup failure                 | Check table exists and key/value sizes are within page size limits.                |
| 1003 | `StorageKeyEncoding`    | No        | Key format doesn't match table schema                    | Verify the key conforms to the table's key type encoding requirements.             |
| 1100 | `StorageSnapshot`       | Yes       | Snapshot creation, compression, or transfer failure      | Check disk space. Requires approximately 1x database size in free space.           |
| 1101 | `StorageCorruption`     | No        | Page checksum mismatch or invalid B-tree structure       | Restore from backup. Report corruption details (page_id, checksums) for forensics. |

## Consensus Errors (2000-2199)

| Code | Name                      | Retryable | Cause                                           | Recovery Action                                                              |
| ---- | ------------------------- | --------- | ----------------------------------------------- | ---------------------------------------------------------------------------- |
| 2000 | `ConsensusNotLeader`      | Yes       | Request reached a follower node                 | Retry with backoff. Request will be forwarded to the current leader.         |
| 2001 | `ConsensusLeaderUnknown`  | Yes       | Cluster is electing a new leader                | Retry with exponential backoff. Election typically completes in seconds.     |
| 2002 | `ConsensusProposalFailed` | Yes       | Raft proposal rejected during leader transition | Retry with backoff. Check `rejection_reason` and `rejecting_node_id` fields. |
| 2100 | `ConsensusLogStorage`     | No        | Raft log storage I/O failure                    | Check Raft log storage health. May indicate disk failure on a peer.          |
| 2101 | `ConsensusStateMachine`   | No        | Error applying committed entries                | Check state machine health. May require manual intervention.                 |
| 2102 | `ConsensusNetwork`        | Yes       | Communication failure between Raft peers        | Check network connectivity, firewall rules, and DNS resolution.              |

## Application Errors (3000-3299)

| Code | Name                    | Retryable | Cause                                             | Recovery Action                                                                 |
| ---- | ----------------------- | --------- | ------------------------------------------------- | ------------------------------------------------------------------------------- |
| 3000 | `AppStorage`            | No\*      | Storage-layer error surfaced at application level | Check disk space, filesystem permissions. \*May be retryable for transient I/O. |
| 3001 | `AppConsensus`          | Yes       | Consensus-layer error at application level        | Retry with exponential backoff. Leader election may be in progress.             |
| 3002 | `AppHashMismatch`       | No        | Cryptographic hash verification failed            | Trigger integrity check. Indicates data corruption or hash computation bug.     |
| 3003 | `AppVaultDiverged`      | No        | Vault state diverged from commitment              | Automatic recovery in progress. Wait for vault health to return to Healthy.     |
| 3004 | `AppVaultUnavailable`   | Yes       | Vault temporarily unavailable                     | Retry after short delay. Vault may be recovering or migrating.                  |
| 3100 | `AppNamespaceNotFound`  | No        | Namespace does not exist                          | Verify via `AdminService::get_namespace` or create it.                          |
| 3101 | `AppVaultNotFound`      | No        | Vault does not exist in namespace                 | Verify via `AdminService::get_vault` or create it.                              |
| 3102 | `AppEntityNotFound`     | No        | Entity key not found                              | Expected for first reads. Create with `SetEntity` operation.                    |
| 3103 | `AppPreconditionFailed` | No        | Conditional write CAS conflict                    | Re-read current state and retry with updated condition.                         |
| 3104 | `AppAlreadyCommitted`   | No        | Duplicate transaction (idempotent success)        | Not an error. Original write succeeded. Treat as success.                       |
| 3105 | `AppSequenceViolation`  | No        | Client sequence number out of order               | Reset sequence counter from server's last committed sequence.                   |
| 3200 | `AppSerialization`      | No        | Codec serialization/deserialization failure       | Codec bug or data corruption. Report as issue.                                  |
| 3201 | `AppConfig`             | No        | Invalid configuration value                       | Fix the configuration value and restart.                                        |
| 3202 | `AppIo`                 | Yes       | Filesystem or network I/O error                   | Check disk space, permissions, mount health. May be retryable.                  |
| 3203 | `AppInvalidArgument`    | No        | Malformed request parameter                       | Fix request parameters and resubmit.                                            |
| 3204 | `AppInternal`           | No        | Unexpected state or invariant violation           | Collect context and report as issue.                                            |

## SDK Usage

The SDK's `SdkError` extracts error codes from gRPC error detail metadata
when available. Use `SdkError::is_retryable()` for retry decisions.

```rust
match client.write(request).await {
    Ok(response) => { /* success */ }
    Err(e) if e.is_retryable() => {
        // Retry with backoff
    }
    Err(e) => {
        // Non-retryable — log and surface to caller
        eprintln!("Error: {e}");
    }
}
```

## Alerting

Build alerting rules on error codes rather than message strings:

- **Rate limit on code 2000-2002**: Consensus instability, possible network partition
- **Any code 1101**: Data corruption, immediate investigation required
- **Sustained code 3004**: Vault recovery stalled, check `AutoRecoveryJob` health
