# Operations Guide

Operational documentation for running InferaDB Ledger in production.

## Getting Started

| Document                                                            | Description                             |
| ------------------------------------------------------------------- | --------------------------------------- |
| [Production Deployment Tutorial](production-deployment-tutorial.md) | Step-by-step Kubernetes deployment      |
| [Deployment Guide](deployment.md)                                   | Cluster setup, bootstrap modes, backup  |
| [Configuration](configuration.md)                                   | Environment variables and CLI reference |
| [API Versioning](api-versioning.md)                                 | Client–server compatibility and headers |

## Day-to-Day Operations

| Document                                  | Description                                              |
| ----------------------------------------- | -------------------------------------------------------- |
| [Logging](logging.md)                     | Request-level JSON logging and queries                   |
| [Dashboard Templates](../dashboards/)     | Pre-built Grafana, Kibana, Datadog dashboards            |
| [Alerting](alerting.md)                   | Prometheus alerting thresholds and PromQL rules          |
| [SLOs](slo.md)                            | Service-level objectives and SLI definitions             |
| [Metrics Reference](metrics-reference.md) | All Prometheus metrics (includes per-organization view)  |
| [Errors](errors.md)                       | `ErrorCode` reference (gRPC mapping, retryability)       |
| [Troubleshooting](troubleshooting.md)     | Symptom index routing to runbooks and common fixes       |
| [Capacity Planning](capacity-planning.md) | Sizing and scaling guidelines                            |
| [Observability Cost](observability-cost.md) | Cost-tuning for Datadog and comparable paid vendors    |
| [Profiling](profiling.md)                 | Flamegraph capture and performance investigation         |

## Architecture & Compliance

| Document                                                  | Description                                         |
| --------------------------------------------------------- | --------------------------------------------------- |
| [Security](security.md)                                   | Trust model, network security, hardening            |
| [Durability](durability.md)                               | WAL, checkpoints, recovery contract                 |
| [Events](events.md)                                       | Audit event pipeline and queryability               |
| [Background Jobs](background-jobs.md)                     | Scheduled jobs that run inside the node             |
| [Multi-Region](multi-region.md)                           | Geographic distribution patterns                    |
| [Region Management](region-management.md)                 | Organization-to-region routing                      |
| [Data Residency Architecture](data-residency-architecture.md) | PII isolation, GLOBAL/REGIONAL split, crypto-shredding |

## Incident Response & Scheduled Work

| Document                                | Description                                        |
| --------------------------------------- | -------------------------------------------------- |
| [Runbooks](runbooks/)                   | On-call incident runbooks + scheduled playbooks    |
| [Troubleshooting](troubleshooting.md)   | First-stop symptom index                           |

See [`runbooks/README.md`](runbooks/README.md) for the full symptom → runbook table.

## Quick Reference

### Health Checks

```bash
# Node health
grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check

# Vault-specific health
grpcurl -plaintext \
  -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
  localhost:50051 ledger.v1.HealthService/Check
```

### Key Metrics

| Metric                                                              | Normal | Warning | Critical |
| ------------------------------------------------------------------- | ------ | ------- | -------- |
| `inferadb_ledger_raft_proposals_pending`                            | < 10   | > 50    | > 100    |
| `ledger_grpc_request_latency_seconds{service="WriteService"}` (p99) | < 50ms | > 100ms | > 500ms  |
| `ledger_determinism_bug_total`                                      | 0      | -       | > 0      |

### Common Commands

```bash
# Cluster info
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetClusterInfo

# List organizations
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListOrganizations

# Metrics
curl localhost:9090/metrics
```
