# Namespace Resource Metrics

Per-namespace resource accounting for capacity planning, quota enforcement, and tenant billing.

## Metrics

| Metric                              | Type      | Labels                      | Description                        |
| ----------------------------------- | --------- | --------------------------- | ---------------------------------- |
| `ledger_namespace_storage_bytes`    | Gauge     | `namespace_id`              | Cumulative estimated storage bytes |
| `ledger_namespace_operations_total` | Counter   | `namespace_id`, `operation` | Operations by type                 |
| `ledger_namespace_latency_seconds`  | Histogram | `namespace_id`, `operation` | Request latency by type            |

**Labels:**

- `namespace_id`: Numeric namespace identifier
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
if projected > namespace_quota.max_storage_bytes → reject
```

Configure per-namespace quotas via `CreateNamespace` or `UpdateConfig`.

## Dashboard Queries

### Storage by namespace (top 10)

```promql
topk(10, ledger_namespace_storage_bytes)
```

### Write rate per namespace (5m window)

```promql
sum by (namespace_id) (rate(ledger_namespace_operations_total{operation="write"}[5m]))
```

### Read/write ratio per namespace

```promql
sum by (namespace_id) (rate(ledger_namespace_operations_total{operation="read"}[5m]))
/
sum by (namespace_id) (rate(ledger_namespace_operations_total{operation="write"}[5m]))
```

### p99 write latency per namespace

```promql
histogram_quantile(0.99,
  sum by (namespace_id, le) (
    rate(ledger_namespace_latency_seconds_bucket{operation="write"}[5m])
  )
)
```

### Namespaces approaching quota (>80% utilization)

```promql
ledger_namespace_storage_bytes / on(namespace_id) ledger_namespace_quota_bytes > 0.8
```

### Operations per namespace over 24h

```promql
increase(ledger_namespace_operations_total[24h])
```

## Alerting

Example Prometheus alert rules:

```yaml
groups:
  - name: namespace-capacity
    rules:
      - alert: NamespaceNearQuota
        expr: ledger_namespace_storage_bytes / on(namespace_id) ledger_namespace_quota_bytes > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Namespace {{ $labels.namespace_id }} at {{ $value | humanizePercentage }} of quota"

      - alert: NamespaceHighWriteRate
        expr: sum by (namespace_id) (rate(ledger_namespace_operations_total{operation="write"}[5m])) > 1000
        for: 10m
        labels:
          severity: info
        annotations:
          summary: "Namespace {{ $labels.namespace_id }} sustained >1k writes/s"
```

## Cardinality

Label cardinality is bounded by the number of namespaces, which is operator-controlled. For typical deployments (10s-100s of namespaces), this adds minimal overhead to the Prometheus scrape.
