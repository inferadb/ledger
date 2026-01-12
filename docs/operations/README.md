# Operations Guide

This directory contains operational documentation for running InferaDB Ledger in production.

## Contents

| Document | Description |
|----------|-------------|
| [vault-repair.md](vault-repair.md) | Diagnosing and repairing diverged vaults |

## Quick Reference

### Health Checks

```bash
# Check cluster health
ledger-admin cluster-status

# Check specific vault
ledger-admin vault-status --namespace <NS> --vault <ID>
```

### Common Operations

| Operation | Command |
|-----------|---------|
| View logs | `ledger-admin logs --since 1h` |
| Check replication lag | `ledger-admin lag-report` |
| Force leader election | `ledger-admin stepdown --node <ID>` |
| Vault recovery | `ledger-admin recover-vault --namespace <NS> --vault <ID> --force` |

### Key Metrics

| Metric | Normal | Warning | Critical |
|--------|--------|---------|----------|
| `raft_replication_lag` | < 10 blocks | > 50 blocks | > 100 blocks |
| `vault_health{state="healthy"}` | 100% | < 100% | < 90% |
| `request_latency_p99` | < 50ms | > 100ms | > 500ms |

## Related Documentation

- [DESIGN.md](../../DESIGN.md) - System design specification
- [proto/README.md](../../proto/README.md) - API documentation
