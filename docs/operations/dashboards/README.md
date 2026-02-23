# Dashboard Templates

Pre-built dashboard templates for monitoring InferaDB Ledger via Prometheus metrics and structured request logging.

## Prometheus/Grafana (Metrics)

### Prerequisites

- Grafana 10.0+
- Prometheus datasource configured
- Ledger nodes exposing metrics on port 9090

### Quick Start

1. Open Grafana → Dashboards → Import
2. Upload `grafana-prometheus-v1.json` or paste its contents
3. Select your Prometheus datasource
4. Click Import

### Key Panels

The metrics dashboard includes these essential panels:

#### Cluster Health

| Panel             | Query                                    | Description                |
| ----------------- | ---------------------------------------- | -------------------------- |
| Leader Status     | `inferadb_ledger_raft_is_leader`         | Shows which node is leader |
| Raft Term         | `inferadb_ledger_raft_term`              | Current consensus term     |
| Commit Index      | `inferadb_ledger_raft_commit_index`      | Raft log progress          |
| Proposals Pending | `inferadb_ledger_raft_proposals_pending` | Backlog indicator          |

#### Request Throughput

| Panel      | Query                                                | Description           |
| ---------- | ---------------------------------------------------- | --------------------- |
| Writes/sec | `rate(ledger_writes_total[5m])`                      | Write request rate    |
| Reads/sec  | `rate(ledger_reads_total[5m])`                       | Read request rate     |
| Batch Size | `histogram_quantile(0.95, ledger_batch_size_bucket)` | Requests per batch    |
| gRPC Rate  | `rate(ledger_grpc_requests_total[5m])` by method     | Per-method throughput |

#### Latency

| Panel      | Query                                                                                   | Description        |
| ---------- | --------------------------------------------------------------------------------------- | ------------------ |
| Write p99  | `histogram_quantile(0.99, rate(ledger_write_latency_seconds_bucket[5m]))`               | Write latency      |
| Read p99   | `histogram_quantile(0.99, rate(ledger_read_latency_seconds_bucket[5m]))`                | Read latency       |
| Raft Apply | `histogram_quantile(0.99, rate(inferadb_ledger_raft_apply_latency_seconds_bucket[5m]))` | Consensus latency  |
| State Root | `histogram_quantile(0.99, rate(inferadb_ledger_state_root_latency_seconds_bucket[5m]))` | Merkle computation |

#### Storage

| Panel         | Query                                          | Description        |
| ------------- | ---------------------------------------------- | ------------------ |
| Bytes Written | `rate(ledger_storage_bytes_written_total[5m])` | Disk write rate    |
| Bytes Read    | `rate(ledger_storage_bytes_read_total[5m])`    | Disk read rate     |
| Snapshot Size | `ledger_snapshot_size_bytes`                   | Last snapshot size |

#### Reliability

| Panel                | Query                                                                                                                                               | Description                    |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| Idempotency Hit Rate | `rate(ledger_idempotency_cache_hits_total[5m]) / (rate(ledger_idempotency_cache_hits_total[5m]) + rate(ledger_idempotency_cache_misses_total[5m]))` | Cache effectiveness            |
| Recovery Events      | `ledger_recovery_success_total`, `ledger_recovery_failure_total`                                                                                    | Vault recovery tracking        |
| Determinism Bugs     | `ledger_determinism_bug_total`                                                                                                                      | State divergence (should be 0) |

### Grafana Dashboard JSON

```json
{
  "annotations": { "list": [] },
  "title": "InferaDB Ledger Metrics",
  "uid": "inferadb-ledger-metrics",
  "version": 1,
  "schemaVersion": 39,
  "templating": {
    "list": [
      {
        "name": "datasource",
        "type": "datasource",
        "query": "prometheus"
      },
      {
        "name": "instance",
        "type": "query",
        "datasource": { "type": "prometheus", "uid": "${datasource}" },
        "query": "label_values(inferadb_ledger_raft_is_leader, instance)",
        "multi": true,
        "includeAll": true
      }
    ]
  },
  "panels": [
    {
      "title": "Cluster Leader",
      "type": "stat",
      "gridPos": { "h": 4, "w": 6, "x": 0, "y": 0 },
      "targets": [
        {
          "expr": "inferadb_ledger_raft_is_leader{instance=~\"$instance\"} == 1",
          "legendFormat": "{{instance}}"
        }
      ],
      "options": { "colorMode": "value" }
    },
    {
      "title": "Raft Term",
      "type": "stat",
      "gridPos": { "h": 4, "w": 6, "x": 6, "y": 0 },
      "targets": [
        {
          "expr": "max(inferadb_ledger_raft_term{instance=~\"$instance\"})",
          "legendFormat": "Current Term"
        }
      ]
    },
    {
      "title": "Proposals Pending",
      "type": "gauge",
      "gridPos": { "h": 4, "w": 6, "x": 12, "y": 0 },
      "targets": [
        {
          "expr": "sum(inferadb_ledger_raft_proposals_pending{instance=~\"$instance\"})"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "steps": [
              { "color": "green", "value": null },
              { "color": "yellow", "value": 10 },
              { "color": "red", "value": 50 }
            ]
          },
          "max": 100
        }
      }
    },
    {
      "title": "Active Connections",
      "type": "stat",
      "gridPos": { "h": 4, "w": 6, "x": 18, "y": 0 },
      "targets": [
        {
          "expr": "sum(ledger_active_connections{instance=~\"$instance\"})"
        }
      ]
    },
    {
      "title": "Write Throughput",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 4 },
      "targets": [
        {
          "expr": "sum(rate(ledger_writes_total{instance=~\"$instance\"}[5m]))",
          "legendFormat": "writes/sec"
        }
      ]
    },
    {
      "title": "Read Throughput",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 4 },
      "targets": [
        {
          "expr": "sum(rate(ledger_reads_total{instance=~\"$instance\"}[5m]))",
          "legendFormat": "reads/sec"
        }
      ]
    },
    {
      "title": "Write Latency Percentiles",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 12 },
      "targets": [
        {
          "expr": "histogram_quantile(0.50, sum(rate(ledger_write_latency_seconds_bucket{instance=~\"$instance\"}[5m])) by (le))",
          "legendFormat": "p50"
        },
        {
          "expr": "histogram_quantile(0.95, sum(rate(ledger_write_latency_seconds_bucket{instance=~\"$instance\"}[5m])) by (le))",
          "legendFormat": "p95"
        },
        {
          "expr": "histogram_quantile(0.99, sum(rate(ledger_write_latency_seconds_bucket{instance=~\"$instance\"}[5m])) by (le))",
          "legendFormat": "p99"
        }
      ],
      "fieldConfig": { "defaults": { "unit": "s" } }
    },
    {
      "title": "Read Latency Percentiles",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 12 },
      "targets": [
        {
          "expr": "histogram_quantile(0.50, sum(rate(ledger_read_latency_seconds_bucket{instance=~\"$instance\"}[5m])) by (le))",
          "legendFormat": "p50"
        },
        {
          "expr": "histogram_quantile(0.95, sum(rate(ledger_read_latency_seconds_bucket{instance=~\"$instance\"}[5m])) by (le))",
          "legendFormat": "p95"
        },
        {
          "expr": "histogram_quantile(0.99, sum(rate(ledger_read_latency_seconds_bucket{instance=~\"$instance\"}[5m])) by (le))",
          "legendFormat": "p99"
        }
      ],
      "fieldConfig": { "defaults": { "unit": "s" } }
    },
    {
      "title": "Raft Apply Latency",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 0, "y": 20 },
      "targets": [
        {
          "expr": "histogram_quantile(0.99, sum(rate(inferadb_ledger_raft_apply_latency_seconds_bucket{instance=~\"$instance\"}[5m])) by (le))",
          "legendFormat": "p99"
        }
      ],
      "fieldConfig": { "defaults": { "unit": "s" } }
    },
    {
      "title": "State Root Computation",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 8, "y": 20 },
      "targets": [
        {
          "expr": "histogram_quantile(0.99, sum(rate(inferadb_ledger_state_root_latency_seconds_bucket{instance=~\"$instance\"}[5m])) by (le))",
          "legendFormat": "p99"
        }
      ],
      "fieldConfig": { "defaults": { "unit": "s" } }
    },
    {
      "title": "Batch Coalescing",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 16, "y": 20 },
      "targets": [
        {
          "expr": "rate(ledger_batch_eager_commits_total{instance=~\"$instance\"}[5m])",
          "legendFormat": "eager commits"
        },
        {
          "expr": "rate(ledger_batch_timeout_commits_total{instance=~\"$instance\"}[5m])",
          "legendFormat": "timeout commits"
        }
      ]
    },
    {
      "title": "Idempotency Cache",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 28 },
      "targets": [
        {
          "expr": "rate(ledger_idempotency_cache_hits_total{instance=~\"$instance\"}[5m])",
          "legendFormat": "hits/sec"
        },
        {
          "expr": "rate(ledger_idempotency_cache_misses_total{instance=~\"$instance\"}[5m])",
          "legendFormat": "misses/sec"
        }
      ]
    },
    {
      "title": "Storage I/O",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 28 },
      "targets": [
        {
          "expr": "sum(rate(ledger_storage_bytes_written_total{instance=~\"$instance\"}[5m]))",
          "legendFormat": "write bytes/sec"
        },
        {
          "expr": "sum(rate(ledger_storage_bytes_read_total{instance=~\"$instance\"}[5m]))",
          "legendFormat": "read bytes/sec"
        }
      ],
      "fieldConfig": { "defaults": { "unit": "Bps" } }
    },
    {
      "title": "Determinism Bugs (should be 0)",
      "type": "stat",
      "gridPos": { "h": 4, "w": 6, "x": 0, "y": 36 },
      "targets": [
        {
          "expr": "sum(ledger_determinism_bug_total{instance=~\"$instance\"})"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "steps": [
              { "color": "green", "value": null },
              { "color": "red", "value": 1 }
            ]
          }
        }
      }
    },
    {
      "title": "Recovery Events",
      "type": "stat",
      "gridPos": { "h": 4, "w": 6, "x": 6, "y": 36 },
      "targets": [
        {
          "expr": "sum(ledger_recovery_success_total{instance=~\"$instance\"})",
          "legendFormat": "success"
        },
        {
          "expr": "sum(ledger_recovery_failure_total{instance=~\"$instance\"})",
          "legendFormat": "failure"
        }
      ]
    }
  ]
}
```

Save this JSON as `grafana-prometheus-v1.json` and import into Grafana.

### Alertmanager Integration

Configure Alertmanager routing for Ledger alerts:

```yaml
# alertmanager.yml
route:
  receiver: "default"
  routes:
    - match:
        alertname: LedgerDeterminismBug
      receiver: "pagerduty-critical"
    - match:
        alertname: LedgerNoLeader
      receiver: "pagerduty-critical"
    - match_re:
        alertname: Ledger.*
      receiver: "slack-warnings"

receivers:
  - name: "pagerduty-critical"
    pagerduty_configs:
      - service_key: "<YOUR_PD_KEY>"
  - name: "slack-warnings"
    slack_configs:
      - channel: "#ledger-alerts"
        api_url: "<YOUR_SLACK_WEBHOOK>"
```

---

# Logging Dashboard Templates

Pre-built dashboard templates for visualizing InferaDB Ledger request logs across common log aggregation platforms.

## Quick Start

1. Choose your platform's template file
2. Configure the required datasource
3. Import the dashboard
4. Adjust filters as needed

## Available Templates

| Platform               | File                   | Datasource                              |
| ---------------------- | ---------------------- | --------------------------------------- |
| Grafana (Loki)         | `grafana-loki-v1.json` | Loki                                    |
| Kibana (Elasticsearch) | `kibana-v1.ndjson`     | Elasticsearch index `inferadb-ledger-*` |
| Datadog                | `datadog-v1.json`      | Datadog Logs                            |

## Grafana (Loki)

### Prerequisites

- Grafana 10.0+
- Loki datasource configured
- Request logs ingested with `job="inferadb-ledger"` label

### Installation

1. Open Grafana → Dashboards → Import
2. Upload `grafana-loki-v1.json` or paste its contents
3. Select your Loki datasource
4. Click Import

### Loki Label Configuration

Ensure your Loki pipeline extracts these labels from the JSON:

```yaml
# promtail config example
pipeline_stages:
  - json:
      expressions:
        service: service
        method: method
        outcome: outcome
        organization_slug: organization_slug
  - labels:
      service:
      method:
      outcome:
      organization_slug:
```

Alternatively, use Loki's JSON parser in queries (already configured in the dashboard).

### Variables

| Variable     | Description                                                      |
| ------------ | ---------------------------------------------------------------- |
| `datasource` | Loki datasource to query                                         |
| `service`    | Filter by gRPC service (WriteService, ReadService, AdminService) |
| `method`     | Filter by gRPC method                                            |
| `organization`  | Filter by organization ID                                           |
| `outcome`    | Filter by request outcome                                        |

## Kibana (Elasticsearch)

### Prerequisites

- Kibana 8.0+
- Elasticsearch cluster with request logs indexed
- Index pattern matching `inferadb-ledger-*`

### Installation

1. Open Kibana → Stack Management → Saved Objects
2. Click Import
3. Upload `kibana-v1.ndjson`
4. Resolve any conflicts (usually select "Overwrite")
5. Open Dashboards → InferaDB Ledger Logging

### Index Configuration

Create an index template for proper field mappings:

```json
PUT _index_template/inferadb-logging
{
  "index_patterns": ["inferadb-ledger-*"],
  "template": {
    "mappings": {
      "properties": {
        "@timestamp": { "type": "date" },
        "request_id": { "type": "keyword" },
        "client_id": { "type": "keyword" },
        "sequence": { "type": "long" },
        "organization_slug": { "type": "long" },
        "vault_id": { "type": "long" },
        "service": { "type": "keyword" },
        "method": { "type": "keyword" },
        "node_id": { "type": "long" },
        "is_leader": { "type": "boolean" },
        "raft_term": { "type": "long" },
        "outcome": { "type": "keyword" },
        "error_code": { "type": "keyword" },
        "error_message": { "type": "text" },
        "duration_ms": { "type": "float" },
        "raft_latency_ms": { "type": "float" },
        "storage_latency_ms": { "type": "float" },
        "batch_size": { "type": "integer" },
        "batch_coalesced": { "type": "boolean" },
        "idempotency_hit": { "type": "boolean" },
        "is_vip": { "type": "boolean" },
        "trace_id": { "type": "keyword" },
        "span_id": { "type": "keyword" }
      }
    }
  }
}
```

### Saved Searches

The template includes pre-configured saved searches:

- **All Requests**: All request logs with key fields
- **Errors**: Error events with context
- **Slow Requests**: Requests exceeding 100ms with timing breakdown

## Datadog

### Prerequisites

- Datadog account with Log Management
- Request logs forwarded to Datadog
- Service tag set to `inferadb-ledger`

### Installation

1. Open Datadog → Dashboards → New Dashboard → Import Dashboard JSON
2. Paste contents of `datadog-v1.json`
3. Save the dashboard

Alternatively, use the Datadog API:

```bash
curl -X POST "https://api.datadoghq.com/api/v1/dashboard" \
  -H "Content-Type: application/json" \
  -H "DD-API-KEY: ${DD_API_KEY}" \
  -H "DD-APPLICATION-KEY: ${DD_APP_KEY}" \
  -d @datadog-v1.json
```

### Facet Configuration

Create facets for efficient filtering. In Datadog Logs → Configuration → Facets:

| Facet        | Path            | Type    |
| ------------ | --------------- | ------- |
| Service      | `@service`      | String  |
| Method       | `@method`       | String  |
| Outcome      | `@outcome`      | String  |
| Client ID    | `@client_id`    | String  |
| Organization ID | `@organization_slug` | Integer |
| Duration     | `@duration_ms`  | Double  |
| Error Code   | `@error_code`   | String  |
| Trace ID     | `@trace_id`     | String  |

### Template Variables

| Variable    | Description                    |
| ----------- | ------------------------------ |
| `service`   | Filter by gRPC service         |
| `method`    | Filter by gRPC method          |
| `organization` | Filter by organization ID         |
| `outcome`   | Filter by request outcome      |
| `trace_id`  | Filter by distributed trace ID |

## Dashboard Panels

All templates include equivalent panels:

| Panel                          | Description                       |
| ------------------------------ | --------------------------------- |
| Request Rate by Service/Method | Time series of request throughput |
| Error Rate by Error Code       | Stacked error counts by code      |
| Latency Percentiles by Method  | p50/p95/p99 latency over time     |
| Latency Heatmap                | Distribution of request durations |
| Top 10 Clients                 | Highest-volume clients            |
| Top 10 Organizations by Errors    | Problem organizations                |
| Sampling/Outcomes              | Pie chart of request outcomes     |
| Batch Coalescing Efficiency    | Average batch size over time      |
| Idempotency Cache Hit Rate     | Cache effectiveness metric        |

## Customization

### Adding Panels

Follow platform-specific patterns:

**Grafana**: Add panel → Choose visualization → Configure LogQL query

**Kibana**: Create Visualization → Select index pattern → Configure aggregations

**Datadog**: Edit dashboard → Add widget → Configure log query

### Common Query Patterns

Filter by client:

```
# Loki
{job="inferadb-ledger"} | json | client_id="api_acme_corp"

# Elasticsearch
client_id:"api_acme_corp"

# Datadog
@client_id:api_acme_corp
```

Show slow writes:

```
# Loki
{job="inferadb-ledger"} | json | method="write" | duration_ms > 100

# Elasticsearch
method:write AND duration_ms:>100

# Datadog
@method:write @duration_ms:>100
```

### Threshold Customization

Adjust panel thresholds for your environment:

- **Latency**: Default slow threshold is 100ms for writes
- **Error counts**: Yellow at 10, Red at 50 errors
- **Batch size**: Typical range is 1-20 requests per batch

## Versioning

Dashboard templates include metadata for compatibility tracking:

```json
{
  "schema_version": 1,
  "min_ledger_version": "0.5.0"
}
```

- `schema_version`: Increments on breaking field changes
- `min_ledger_version`: Minimum InferaDB Ledger version required

Check your Ledger version:

```bash
inferadb-ledger --version
```

### Compatibility Matrix

| Dashboard Schema | Min Ledger Version | Changes                           |
| ---------------- | ------------------ | --------------------------------- |
| 1                | 0.5.0              | Initial logging release           |

**Future breaking changes will increment the schema version.**

Operators can run dashboards with `schema_version` higher than their ledger version (queries for new fields return empty, but dashboards remain functional).

### Field Evolution Policy

| Change Type        | Compatibility                    | Dashboard Action |
| ------------------ | -------------------------------- | ---------------- |
| New field added    | Backward compatible              | No update needed |
| Field renamed      | Breaking (increments schema)     | Update queries   |
| Field type changed | Breaking (increments schema)     | Update queries   |
| Field removed      | Breaking (increments schema)     | Remove from queries |

## Upgrading Dashboards

When upgrading to a new schema version:

1. Export any customizations from your current dashboard
2. Import the new template
3. Reapply customizations
4. Verify panels render correctly

New fields are additive; old dashboards continue working with newer Ledger versions.

## Troubleshooting

### No Data Appearing

1. Verify logging is enabled:

   ```bash
   grep LOGGING /path/to/config
   ```

2. Check log output format is JSON:

   ```bash
   grep LOG_FORMAT /path/to/config
   ```

3. Verify logs are reaching your aggregation system

### Query Errors

**Loki**: Ensure JSON parsing is working:

```
{job="inferadb-ledger"} | json | __error__=""
```

**Elasticsearch**: Check field mappings match expected types

**Datadog**: Verify facets are configured for filtered fields

### Performance Issues

For high-volume deployments:

1. Reduce time range for initial queries
2. Add filters to reduce result set
3. Consider increasing sampling rates for development environments
