# Wide Events Dashboard Templates

Pre-built dashboard templates for visualizing InferaDB Ledger wide events across common log aggregation platforms.

## Quick Start

1. Choose your platform's template file
2. Configure the required datasource
3. Import the dashboard
4. Adjust filters as needed

## Available Templates

| Platform | File | Datasource |
|----------|------|------------|
| Grafana (Loki) | `grafana-loki-v1.json` | Loki |
| Kibana (Elasticsearch) | `kibana-v1.ndjson` | Elasticsearch index `inferadb-ledger-*` |
| Datadog | `datadog-v1.json` | Datadog Logs |

## Grafana (Loki)

### Prerequisites

- Grafana 10.0+
- Loki datasource configured
- Wide events logs ingested with `job="inferadb-ledger"` label

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
        namespace_id: namespace_id
  - labels:
      service:
      method:
      outcome:
      namespace_id:
```

Alternatively, use Loki's JSON parser in queries (already configured in the dashboard).

### Variables

| Variable | Description |
|----------|-------------|
| `datasource` | Loki datasource to query |
| `service` | Filter by gRPC service (WriteService, ReadService, AdminService) |
| `method` | Filter by gRPC method |
| `namespace` | Filter by namespace ID |
| `outcome` | Filter by request outcome |

## Kibana (Elasticsearch)

### Prerequisites

- Kibana 8.0+
- Elasticsearch cluster with wide events indexed
- Index pattern matching `inferadb-ledger-*`

### Installation

1. Open Kibana → Stack Management → Saved Objects
2. Click Import
3. Upload `kibana-v1.ndjson`
4. Resolve any conflicts (usually select "Overwrite")
5. Open Dashboards → InferaDB Ledger Wide Events

### Index Configuration

Create an index template for proper field mappings:

```json
PUT _index_template/inferadb-wide-events
{
  "index_patterns": ["inferadb-ledger-*"],
  "template": {
    "mappings": {
      "properties": {
        "@timestamp": { "type": "date" },
        "request_id": { "type": "keyword" },
        "client_id": { "type": "keyword" },
        "sequence": { "type": "long" },
        "namespace_id": { "type": "long" },
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

- **All Requests**: All wide events with key fields
- **Errors**: Error events with context
- **Slow Requests**: Requests exceeding 100ms with timing breakdown

## Datadog

### Prerequisites

- Datadog account with Log Management
- Wide events forwarded to Datadog
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

| Facet | Path | Type |
|-------|------|------|
| Service | `@service` | String |
| Method | `@method` | String |
| Outcome | `@outcome` | String |
| Client ID | `@client_id` | String |
| Namespace ID | `@namespace_id` | Integer |
| Duration | `@duration_ms` | Double |
| Error Code | `@error_code` | String |
| Trace ID | `@trace_id` | String |

### Template Variables

| Variable | Description |
|----------|-------------|
| `service` | Filter by gRPC service |
| `method` | Filter by gRPC method |
| `namespace` | Filter by namespace ID |
| `outcome` | Filter by request outcome |
| `trace_id` | Filter by distributed trace ID |

## Dashboard Panels

All templates include equivalent panels:

| Panel | Description |
|-------|-------------|
| Request Rate by Service/Method | Time series of request throughput |
| Error Rate by Error Code | Stacked error counts by code |
| Latency Percentiles by Method | p50/p95/p99 latency over time |
| Latency Heatmap | Distribution of request durations |
| Top 10 Clients | Highest-volume clients |
| Top 10 Namespaces by Errors | Problem namespaces |
| Sampling/Outcomes | Pie chart of request outcomes |
| Batch Coalescing Efficiency | Average batch size over time |
| Idempotency Cache Hit Rate | Cache effectiveness metric |

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

## Upgrading Dashboards

When upgrading to a new schema version:

1. Export any customizations from your current dashboard
2. Import the new template
3. Reapply customizations
4. Verify panels render correctly

New fields are additive; old dashboards continue working with newer Ledger versions.

## Troubleshooting

### No Data Appearing

1. Verify wide events are enabled:
   ```bash
   grep WIDE_EVENTS /path/to/config
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
