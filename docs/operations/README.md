# Operations Guide

This directory contains operational documentation for running InferaDB Ledger in production.

## Contents

| Document                           | Description                                    |
| ---------------------------------- | ---------------------------------------------- |
| [deployment.md](deployment.md)     | Cluster setup, scaling, backup, and Kubernetes |
| [vault-repair.md](vault-repair.md) | Diagnosing and repairing diverged vaults       |

## Quick Reference

### Health Checks

Use the gRPC `HealthService` to check node status:

```bash
# Check node health via custom HealthService
grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check

# Check vault-specific health
grpcurl -plaintext -d '{"namespace_id": {"id": 1}, "vault_id": {"id": 123}}' \
  localhost:50051 ledger.v1.HealthService/Check
```

Note: The standard `grpc-health-probe` tool is not compatible with InferaDB's custom health service.

### Key Metrics

| Metric                                   | Normal     | Warning     | Critical    |
| ---------------------------------------- | ---------- | ----------- | ----------- |
| `inferadb_ledger_raft_proposals_pending` | < 10       | > 50        | > 100       |
| `ledger_grpc_request_latency_seconds`    | p99 < 50ms | p99 > 100ms | p99 > 500ms |
| `ledger_recovery_failure_total`          | 0          | > 0         | increasing  |

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
