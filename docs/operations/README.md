# Operations Guide

Operational documentation for running InferaDB Ledger in production.

## Getting Started

| Document                                                            | Description                             |
| ------------------------------------------------------------------- | --------------------------------------- |
| [Production Deployment Tutorial](production-deployment-tutorial.md) | Step-by-step Kubernetes deployment      |
| [Deployment Guide](deployment.md)                                   | Cluster setup, bootstrap modes, backup  |
| [Configuration](configuration.md)                                   | Environment variables and CLI reference |

## Day-to-Day Operations

| Document                                  | Description                                   |
| ----------------------------------------- | --------------------------------------------- |
| [Logging](logging.md)                     | Request-level JSON logging and queries        |
| [Dashboard Templates](dashboards/)        | Pre-built Grafana, Kibana, Datadog dashboards |
| [Alerting](alerting.md)                   | Prometheus alerting rules and SLOs            |
| [Metrics Reference](metrics-reference.md) | All Prometheus metrics                        |
| [Troubleshooting](troubleshooting.md)     | Common issues and solutions                   |
| [Capacity Planning](capacity-planning.md) | Sizing and scaling guidelines                 |

## Architecture & Security

| Document                                | Description                              |
| --------------------------------------- | ---------------------------------------- |
| [Security](security.md)                 | Trust model, network security, hardening |
| [Multi-Region](multi-region.md)         | Geographic distribution patterns         |
| [Shard Management](shard-management.md) | Organization-to-shard routing               |

## Maintenance & Recovery

| Document                        | Description                              |
| ------------------------------- | ---------------------------------------- |
| [Vault Repair](vault-repair.md) | Diagnosing and repairing diverged vaults |
| [Runbooks](runbooks/)           | Operational procedures                   |

### Runbooks

| Runbook                                                | Description              |
| ------------------------------------------------------ | ------------------------ |
| [Upgrade Runbook](runbooks/rolling-upgrade.md)         | Version upgrades         |
| [Backup Verification](runbooks/backup-verification.md) | Testing backup integrity |
| [Disaster Recovery](runbooks/disaster-recovery.md)     | Recovery procedures      |

## Quick Reference

### Health Checks

```bash
# Node health
grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check

# Vault-specific health
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
  localhost:50051 ledger.v1.HealthService/Check
```

### Key Metrics

| Metric                                   | Normal     | Warning     | Critical    |
| ---------------------------------------- | ---------- | ----------- | ----------- |
| `inferadb_ledger_raft_proposals_pending` | < 10       | > 50        | > 100       |
| `ledger_write_latency_seconds`           | p99 < 50ms | p99 > 100ms | p99 > 500ms |
| `ledger_determinism_bug_total`           | 0          | -           | > 0         |

### Common Commands

```bash
# Cluster info
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetClusterInfo

# List organizations
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListOrganizations

# Metrics
curl localhost:9090/metrics
```

## Related Documentation

- [Client API](../client/api.md) - Read/write operations
- [Admin API](../client/admin.md) - Cluster management
- [DESIGN.md](../../DESIGN.md) - System design specification
