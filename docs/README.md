# InferaDB Ledger — Operator Documentation

Guides for deploying, configuring, monitoring, and maintaining InferaDB Ledger.

## Understanding Ledger

- [Architecture overview](overview.md) — what Ledger is, how its components fit together
- [FAQ](faq.md) — trust model, Raft consensus, organizations, vaults

## Deploying

- [Deployment guide](operations/deployment.md) — production deployment patterns
- [Production tutorial](operations/production-deployment-tutorial.md) — step-by-step Kubernetes deployment
- [Configuration reference](operations/configuration.md) — CLI flags and environment variables
- [Security](operations/security.md) — trust model, TLS, encryption at rest
- [Multi-region](operations/multi-region.md) — geographic distribution and data residency
- [Region management](operations/region-management.md) — organization-to-region routing
- [Data residency](operations/data-residency-architecture.md) — PII isolation and compliance

## Monitoring

- [Metrics reference](operations/metrics-reference.md) — all Prometheus metrics
- [Logging](operations/logging.md) — canonical log lines, structured fields
- [Events](operations/events.md) — audit event system, event catalog, retention
- [Alerting](operations/alerting.md) — Prometheus alerting rules and thresholds
- [Dashboard templates](operations/dashboards/) — Grafana, Kibana, Datadog
- [SLI/SLO reference](operations/slo.md) — service level indicators and objectives
- [Organization metrics](operations/organization-metrics.md) — per-tenant resource tracking
- [Background jobs](operations/background-jobs.md) — job observability and health
- [Capacity planning](operations/capacity-planning.md) — sizing and resource estimation

## Maintaining

- [Troubleshooting](operations/troubleshooting.md) — common issues and solutions
- [Vault repair](operations/vault-repair.md) — recovering diverged vaults
- [API versioning](operations/api-versioning.md) — version negotiation and compatibility

## Runbooks

- [Rolling upgrade](operations/runbooks/rolling-upgrade.md)
- [Backup verification](operations/runbooks/backup-verification.md)
- [Disaster recovery](operations/runbooks/disaster-recovery.md)
- [Key provisioning](operations/runbooks/key-provisioning.md)
- [Scaling](operations/runbooks/scaling.md)

## Quick reference

| Task              | Command                                                            |
| ----------------- | ------------------------------------------------------------------ |
| Start single node | `inferadb-ledger --single --data /tmp/ledger`                      |
| Health check      | `grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check` |
| View metrics      | `curl localhost:9090/metrics`                                      |
