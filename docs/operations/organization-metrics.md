# Organization Resource Metrics

Per-organization resource accounting for capacity planning, quota enforcement, and tenant billing.

## Metrics

| Metric                              | Type      | Labels                      | Description                        |
| ----------------------------------- | --------- | --------------------------- | ---------------------------------- |
| `ledger_organization_storage_bytes`    | Gauge     | `organization_slug`              | Cumulative estimated storage bytes |
| `ledger_organization_operations_total` | Counter   | `organization_slug`, `operation` | Operations by type                 |
| `ledger_organization_latency_seconds`  | Histogram | `organization_slug`, `operation` | Request latency by type            |

**Labels:**

- `organization_slug`: Numeric organization identifier
- `operation`: `write`, `read`, or `admin`

## Storage Accounting

Storage bytes are tracked in Raft-replicated state, updated on every committed write:

- **SetEntity**: adds `len(key) + len(value)` bytes
- **DeleteEntity**: subtracts `len(key)` bytes (conservative; old value size unknown)
- **CreateRelationship**: adds `len(resource) + len(relation) + len(subject)` bytes
- **DeleteRelationship**: subtracts the same
- **ExpireEntity**: subtracts `len(key)` bytes

The counter floors at zero via saturating subtraction. Because only committed writes update the counter, all replicas converge to the same value after snapshot restore.

## Quota Enforcement

`QuotaChecker::check_storage_estimate()` uses the cumulative storage counter:

```text
projected = current_usage + estimated_write_bytes
if projected > organization_quota.max_storage_bytes → reject
```

Configure per-organization quotas via `CreateOrganization` or `UpdateConfig`.

## Dashboard Queries

### Storage by organization (top 10)

```promql
topk(10, ledger_organization_storage_bytes)
```

### Write rate per organization (5m window)

```promql
sum by (organization_slug) (rate(ledger_organization_operations_total{operation="write"}[5m]))
```

### Read/write ratio per organization

```promql
sum by (organization_slug) (rate(ledger_organization_operations_total{operation="read"}[5m]))
/
sum by (organization_slug) (rate(ledger_organization_operations_total{operation="write"}[5m]))
```

### p99 write latency per organization

```promql
histogram_quantile(0.99,
  sum by (organization_slug, le) (
    rate(ledger_organization_latency_seconds_bucket{operation="write"}[5m])
  )
)
```

### Organizations approaching quota (>80% utilization)

```promql
ledger_organization_storage_bytes / on(organization_slug) ledger_organization_quota_bytes > 0.8
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
      - alert: OrganizationNearQuota
        expr: ledger_organization_storage_bytes / on(organization_slug) ledger_organization_quota_bytes > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Organization {{ $labels.organization_slug }} at {{ $value | humanizePercentage }} of quota"

      - alert: OrganizationHighWriteRate
        expr: sum by (organization_slug) (rate(ledger_organization_operations_total{operation="write"}[5m])) > 1000
        for: 10m
        labels:
          severity: info
        annotations:
          summary: "Organization {{ $labels.organization_slug }} sustained >1k writes/s"
```

## Cardinality

Label cardinality is bounded by the number of organizations, which is operator-controlled. For typical deployments (10s-100s of organizations), this adds minimal overhead to the Prometheus scrape.
