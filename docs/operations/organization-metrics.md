# Organization Resource Metrics

Per-organization resource accounting for capacity planning and tenant billing.

> See [metrics-reference.md](metrics-reference.md) for the full Prometheus metric catalog. This page focuses on the per-tenant subset operators use for billing and quota enforcement.

## Metrics

| Metric                                 | Type      | Labels                         | Description                        |
| -------------------------------------- | --------- | ------------------------------ | ---------------------------------- |
| `ledger_organization_storage_bytes`    | Gauge     | `organization_id`              | Cumulative estimated storage bytes |
| `ledger_organization_operations_total` | Counter   | `organization_id`, `operation` | Operations by type                 |
| `ledger_organization_latency_seconds`  | Histogram | `organization_id`, `operation` | Request latency by type            |

**Labels:**

- `organization_id`: Internal organization identifier
- `operation`: `write`, `read`, or `admin`

## Storage Accounting

Storage bytes are tracked in Raft-replicated state, updated on every committed write:

- **SetEntity**: adds `len(key) + len(value)` bytes
- **DeleteEntity**: subtracts `len(key)` bytes (conservative; old value size unknown)
- **CreateRelationship**: adds `len(resource) + len(relation) + len(subject)` bytes
- **DeleteRelationship**: subtracts the same
- **ExpireEntity**: subtracts `len(key)` bytes

The counter floors at zero via saturating subtraction. Because only committed writes update the counter, all replicas converge to the same value after snapshot restore.

## Tier-Based Limits

Resource limits are derived from the organization's `OrganizationTier` (`Free`, `Launch`, `Scale`) by the downstream Engine and Control layers. The Ledger tracks cumulative storage bytes but does not enforce limits itself — enforcement is handled externally based on tier.

## Dashboard Queries

### Storage by organization (top 10)

```promql
topk(10, ledger_organization_storage_bytes)
```

### Write rate per organization (5m window)

```promql
sum by (organization_id) (rate(ledger_organization_operations_total{operation="write"}[5m]))
```

### Read/write ratio per organization

```promql
sum by (organization_id) (rate(ledger_organization_operations_total{operation="read"}[5m]))
/
sum by (organization_id) (rate(ledger_organization_operations_total{operation="write"}[5m]))
```

### p99 write latency per organization

```promql
histogram_quantile(0.99,
  sum by (organization_id, le) (
    rate(ledger_organization_latency_seconds_bucket{operation="write"}[5m])
  )
)
```

### Operations per organization over 24h

```promql
increase(ledger_organization_operations_total[24h])
```

## Alerting

Example Prometheus alert rules:

```yaml
groups:
  - name: organization-capacity
    rules:
      - alert: OrganizationHighWriteRate
        expr: sum by (organization_id) (rate(ledger_organization_operations_total{operation="write"}[5m])) > 1000
        for: 10m
        labels:
          severity: info
        annotations:
          summary: "Organization {{ $labels.organization_id }} sustained >1k writes/s"
```

## Cardinality

Label cardinality is bounded by the number of organizations, which is operator-controlled. For typical deployments (10s-100s of organizations), this adds minimal overhead to the Prometheus scrape.
