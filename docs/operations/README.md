# Operations Guide

This directory contains operational documentation for running InferaDB Ledger in production.

## Contents

| Document                           | Description                                    |
| ---------------------------------- | ---------------------------------------------- |
| [deployment.md](deployment.md)     | Cluster setup, scaling, backup, and Kubernetes |
| [vault-repair.md](vault-repair.md) | Diagnosing and repairing diverged vaults       |

## Quick Reference

### Health Checks

```bash
# Check cluster health
ledger-admin cluster-status

# Check specific vault
ledger-admin vault-status --namespace <NS> --vault <ID>
```

### Common Operations

| Operation             | Command                                                            |
| --------------------- | ------------------------------------------------------------------ |
| View logs             | `ledger-admin logs --since 1h`                                     |
| Check replication lag | `ledger-admin lag-report`                                          |
| Force leader election | `ledger-admin stepdown --node <ID>`                                |
| Vault recovery        | `ledger-admin recover-vault --namespace <NS> --vault <ID> --force` |

### Key Metrics

| Metric                          | Normal      | Warning     | Critical     |
| ------------------------------- | ----------- | ----------- | ------------ |
| `raft_replication_lag`          | < 10 blocks | > 50 blocks | > 100 blocks |
| `vault_health{state="healthy"}` | 100%        | < 100%      | < 90%        |
| `request_latency_p99`           | < 50ms      | > 100ms     | > 500ms      |

## Related Documentation

### Technical Reference

- [Overview](../overview.md) - Architecture, data model, terminology
- [Client API](../client/api.md) - Read/write operations, errors, pagination
- [Idempotency](../client/idempotency.md) - Sequence tracking, retry semantics

### Internals

- [Consensus](../internals/consensus.md) - Raft integration, write/read paths, batching
- [Storage](../internals/storage.md) - Directory layout, snapshots, crash recovery
- [Discovery](../internals/discovery.md) - Bootstrap, node lifecycle, `_system` namespace

### Specifications

- [Cryptographic Specs](../specs/crypto.md) - Hash algorithms, proof formats
- [System Invariants](../specs/invariants.md) - Formal guarantees

### Source

- [DESIGN.md](../../DESIGN.md) - Complete system design specification
- [proto/README.md](../../proto/README.md) - Protobuf API documentation
