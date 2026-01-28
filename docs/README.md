# InferaDB Ledger Documentation

Blockchain database for cryptographically verifiable authorization.

## Getting Started

- [Quickstart](quickstart.md) - Run Ledger locally in 5 minutes
- [Architecture](DESIGN.md) - System design and data model
- [FAQ](faq.md) - Common questions answered

## Client API

- [API Overview](client/api.md) - Read/write operations, pagination, verification
- [AdminService](client/admin.md) - Namespace, vault, and cluster management
- [HealthService](client/health.md) - Liveness and readiness checks
- [SystemDiscoveryService](client/discovery.md) - Peer discovery and bootstrap
- [Error Reference](client/errors.md) - Error codes and handling patterns

## Operations

### Deployment

- [Deployment Guide](operations/deployment.md) - Production deployment patterns
- [Configuration](operations/configuration.md) - Environment variables reference
- [Security](operations/security.md) - Trust model and network security
- [Multi-Region](operations/multi-region.md) - Geographic distribution

### Monitoring

- [Metrics Reference](operations/metrics-reference.md) - All Prometheus metrics
- [Capacity Planning](operations/capacity-planning.md) - Sizing and scaling

### Runbooks

- [Rolling Upgrade](operations/runbooks/rolling-upgrade.md) - Zero-downtime upgrades
- [Backup Verification](operations/runbooks/backup-verification.md) - Backup testing
- [Disaster Recovery](operations/runbooks/disaster-recovery.md) - Recovery procedures
- [Troubleshooting](troubleshooting.md) - Common issues and solutions

## Development

- [Testing Guide](development/testing.md) - Test categories and commands
- [Contributing](CONTRIBUTING.md) - Development workflow

## Quick Reference

| Task              | Command                                                                    |
| ----------------- | -------------------------------------------------------------------------- |
| Start single node | `inferadb-ledger --single --data /tmp/ledger`                              |
| Health check      | `grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check`         |
| List namespaces   | `grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListNamespaces` |
| View metrics      | `curl localhost:9090/metrics`                                              |

## gRPC Services

| Service                  | Purpose                                  |
| ------------------------ | ---------------------------------------- |
| `HealthService`          | Liveness and readiness checks            |
| `ReadService`            | Query entities, relationships, state     |
| `WriteService`           | Create/update entities and relationships |
| `AdminService`           | Namespace, vault, cluster management     |
| `SystemDiscoveryService` | Peer discovery and cluster bootstrap     |
