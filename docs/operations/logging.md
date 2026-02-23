# Logging

InferaDB Ledger emits one comprehensive JSON event per gRPC request containing 50+ contextual fields for queryable observability.

## Quick Start

Enable request logging in production:

```bash
export INFERADB__LEDGER__LOGGING__ENABLED=true
export INFERADB__LEDGER__LOG_FORMAT=json
```

Query errors in your log aggregation system:

```
outcome:"error" AND service:"WriteService"
```

See [Configuration](configuration.md#logging) for all options.

## Field Reference

Fields are categorized by requirement level:

- **Required** - Always present in every event
- **Conditional** - Present when condition is met
- **Recommended** - Present when available (e.g., tracing enabled)

### Request Metadata

| Field          | Level       | Type   | Condition                | Description                                   | Example                                  |
| -------------- | ----------- | ------ | ------------------------ | --------------------------------------------- | ---------------------------------------- |
| `request_id`   | Required    | UUID   | -                        | Unique identifier for correlation             | `"550e8400-e29b-41d4-a716-446655440000"` |
| `service`      | Required    | String | -                        | gRPC service name                             | `"WriteService"`                         |
| `method`       | Required    | String | -                        | gRPC method name                              | `"write"`                                |
| `client_id`    | Conditional | String | When provided            | Idempotency client identifier (max 128 chars) | `"api_acme_corp"`                        |
| `sequence`     | Conditional | u64    | When provided            | Per-client sequence number                    | `42`                                     |
| `organization_slug` | Conditional | i64    | When targeting organization | Target organization                              | `1001`                                   |
| `vault_slug`   | Conditional | u64    | When targeting vault     | Target vault (0 for organization-level ops)      | `7180591718400`                           |
| `actor`        | Conditional | String | When authenticated       | Identity performing operation                 | `"user:123"`                             |

### System Context

| Field       | Level    | Type | Condition | Description                            | Example |
| ----------- | -------- | ---- | --------- | -------------------------------------- | ------- |
| `node_id`   | Required | u64  | -         | Server node identifier                 | `1`     |
| `is_leader` | Required | bool | -         | Raft leadership status at request time | `true`  |
| `raft_term` | Required | u64  | -         | Current Raft term                      | `15`    |
| `shard_id`  | Required | u32  | -         | Shard routing identifier               | `0`     |
| `is_vip`    | Required | bool | -         | VIP organization indicator                | `false` |

### Write Operation Fields

| Field              | Level       | Type   | Condition              | Description                             | Example                                 |
| ------------------ | ----------- | ------ | ---------------------- | --------------------------------------- | --------------------------------------- |
| `operations_count` | Conditional | usize  | Write ops              | Number of operations in request         | `5`                                     |
| `operation_types`  | Conditional | Vec    | Write ops              | Operation names                         | `["create_relationship", "set_entity"]` |
| `include_tx_proof` | Conditional | bool   | Write ops              | Whether transaction proof was requested | `false`                                 |
| `idempotency_hit`  | Conditional | bool   | Write ops              | True if returning cached result         | `false`                                 |
| `batch_coalesced`  | Conditional | bool   | Batching enabled       | True if batched with other requests     | `true`                                  |
| `batch_size`       | Conditional | usize  | `batch_coalesced=true` | Number of requests in batch             | `12`                                    |
| `condition_type`   | Conditional | String | CAS write              | CAS condition type if present           | `"version_match"`                       |

### Read Operation Fields

| Field              | Level       | Type   | Condition            | Description                                         | Example          |
| ------------------ | ----------- | ------ | -------------------- | --------------------------------------------------- | ---------------- |
| `key`              | Conditional | String | Single-key read      | Entity key or relationship resource (max 256 chars) | `"user:alice"`   |
| `keys_count`       | Conditional | usize  | Batch read           | Number of keys in batch read                        | `10`             |
| `found_count`      | Conditional | usize  | Batch read           | Number of keys found                                | `8`              |
| `consistency`      | Conditional | String | Read ops             | Read consistency level                              | `"linearizable"` |
| `at_height`        | Conditional | u64    | Historical read      | Block height for historical reads                   | `1000`           |
| `include_proof`    | Conditional | bool   | Read ops             | Whether proof was requested                         | `true`           |
| `proof_size_bytes` | Conditional | usize  | `include_proof=true` | Size of cryptographic proof                         | `2048`           |
| `found`            | Conditional | bool   | Single-key read      | Whether key existed                                 | `true`           |
| `value_size_bytes` | Conditional | usize  | `found=true`         | Response payload size                               | `512`            |

### Admin Operation Fields

| Field                   | Level       | Type   | Condition      | Description                           | Example              |
| ----------------------- | ----------- | ------ | -------------- | ------------------------------------- | -------------------- |
| `admin_action`          | Conditional | String | Admin ops      | Administrative action name            | `"create_organization"` |
| `target_organization_name` | Conditional | String | Organization ops  | Target organization name (max 128 chars) | `"acme_production"`  |
| `retention_mode`        | Conditional | String | Vault creation | Vault retention mode                  | `"compliance"`       |
| `recovery_force`        | Conditional | bool   | Recovery ops   | Whether force mode was used           | `false`              |

### Outcome Fields

| Field           | Level       | Type   | Condition            | Description                          | Example                   |
| --------------- | ----------- | ------ | -------------------- | ------------------------------------ | ------------------------- |
| `outcome`       | Required    | String | -                    | Result classification                | `"success"`               |
| `error_code`    | Conditional | String | `outcome=error`      | gRPC status or WriteErrorCode        | `"SEQUENCE_GAP"`          |
| `error_message` | Conditional | String | `outcome=error`      | Human-readable error (max 512 chars) | `"Sequence gap detected"` |
| `error_key`     | Conditional | String | Precondition failure | Key that failed (max 256 chars)      | `"user:bob"`              |
| `block_height`  | Conditional | u64    | Write success        | Committed block height               | `5432`                    |
| `block_hash`    | Conditional | String | Write success        | Block hash (first 16 hex chars)      | `"a1b2c3d4e5f67890"`      |
| `state_root`    | Conditional | String | Write success        | State root (first 16 hex chars)      | `"0123456789abcdef"`      |

**Outcome Values:**

| Value                 | Description                                      |
| --------------------- | ------------------------------------------------ |
| `success`             | Operation completed successfully                 |
| `error`               | Operation failed with error                      |
| `cached`              | Idempotency cache hit, returning cached response |
| `rate_limited`        | Request rejected due to rate limiting            |
| `precondition_failed` | CAS condition not met                            |

### Timing Fields

| Field                | Level       | Type | Condition      | Description            | Example |
| -------------------- | ----------- | ---- | -------------- | ---------------------- | ------- |
| `duration_ms`        | Required    | f64  | -              | Total request duration | `45.2`  |
| `raft_latency_ms`    | Conditional | f64  | Write ops      | Time in Raft consensus | `12.5`  |
| `storage_latency_ms` | Conditional | f64  | Storage access | Time in storage layer  | `8.3`   |

### Tracing Fields

| Field            | Level       | Type   | Condition              | Description                   | Example                              |
| ---------------- | ----------- | ------ | ---------------------- | ----------------------------- | ------------------------------------ |
| `trace_id`       | Recommended | String | OTEL enabled           | W3C trace ID (32 hex chars)   | `"4bf92f3577b34da6a3ce929d0e0e4736"` |
| `span_id`        | Recommended | String | OTEL enabled           | Span ID (16 hex chars)        | `"00f067aa0ba902b7"`                 |
| `parent_span_id` | Recommended | String | Incoming trace context | Parent span ID (16 hex chars) | `"b7ad6b7169203331"`                 |
| `trace_flags`    | Recommended | u8     | OTEL enabled           | W3C trace flags (sampled bit) | `1`                                  |

## Query Cookbook

Queries organized by operational problem. Each section shows equivalent queries for major platforms.

### Incident Response

#### Find All Errors

<details>
<summary>Elasticsearch / OpenSearch</summary>

```json
{
  "query": {
    "bool": {
      "must": [
        { "term": { "outcome": "error" } },
        { "range": { "@timestamp": { "gte": "now-1h" } } }
      ]
    }
  }
}
```

</details>

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
{job="inferadb-ledger"} | json | outcome="error"
```

</details>

<details>
<summary>Datadog</summary>

```
service:inferadb-ledger outcome:error
```

</details>

<details>
<summary>Splunk</summary>

```spl
index=inferadb outcome=error earliest=-1h
```

</details>

#### Error Distribution by Code

<details>
<summary>Elasticsearch / OpenSearch</summary>

```json
{
  "query": { "term": { "outcome": "error" } },
  "aggs": {
    "by_code": { "terms": { "field": "error_code" } }
  }
}
```

</details>

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
sum by (error_code) (
  count_over_time({job="inferadb-ledger"} | json | outcome="error" [1h])
)
```

</details>

<details>
<summary>Datadog</summary>

```
service:inferadb-ledger outcome:error | group by error_code | count()
```

</details>

<details>
<summary>Splunk</summary>

```spl
index=inferadb outcome=error | stats count by error_code
```

</details>

#### Investigate Rate Limiting

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
{job="inferadb-ledger"} | json | outcome="rate_limited"
| line_format "{{.client_id}}: {{.organization_slug}}"
```

</details>

<details>
<summary>Datadog</summary>

```
service:inferadb-ledger outcome:rate_limited | group by client_id | count()
```

</details>

### Performance Analysis

#### Find Slow Writes (>100ms)

<details>
<summary>Elasticsearch / OpenSearch</summary>

```json
{
  "query": {
    "bool": {
      "must": [
        { "term": { "method": "write" } },
        { "range": { "duration_ms": { "gt": 100 } } }
      ]
    }
  },
  "aggs": {
    "by_organization": {
      "terms": { "field": "organization_slug" },
      "aggs": { "avg_duration": { "avg": { "field": "duration_ms" } } }
    }
  }
}
```

</details>

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
{job="inferadb-ledger"} | json | method="write" | duration_ms > 100
```

</details>

<details>
<summary>Datadog</summary>

```
service:inferadb-ledger method:write @duration_ms:>100 | group by organization_slug | avg(@duration_ms)
```

</details>

<details>
<summary>Splunk</summary>

```spl
index=inferadb method=write duration_ms>100
| table _time client_id organization_slug duration_ms raft_latency_ms storage_latency_ms
| sort -duration_ms
```

</details>

#### Latency Percentiles by Service

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
quantile_over_time(0.95,
  {job="inferadb-ledger"} | json | unwrap duration_ms [5m]
) by (service)
```

</details>

<details>
<summary>Datadog</summary>

```
service:inferadb-ledger | group by service | percentile(@duration_ms, 0.95)
```

</details>

#### Raft vs Storage Latency Breakdown

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
{job="inferadb-ledger"} | json | method="write" | outcome="success"
| line_format "raft={{.raft_latency_ms}}ms storage={{.storage_latency_ms}}ms total={{.duration_ms}}ms"
```

</details>

<details>
<summary>Datadog</summary>

```
service:inferadb-ledger method:write outcome:success
| group by node_id | percentile(@raft_latency_ms, 0.95)
```

</details>

### Client Attribution

#### Specific Client's Request History

<details>
<summary>Elasticsearch / OpenSearch</summary>

```json
{
  "query": {
    "bool": {
      "must": [
        { "term": { "client_id": "api_acme_corp" } },
        { "range": { "@timestamp": { "gte": "now-24h" } } }
      ]
    }
  },
  "sort": [{ "@timestamp": "desc" }]
}
```

</details>

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
{job="inferadb-ledger"} | json | client_id="api_acme_corp"
```

</details>

<details>
<summary>Datadog</summary>

```
service:inferadb-ledger @client_id:"api_acme_corp"
```

</details>

<details>
<summary>Splunk</summary>

```spl
index=inferadb client_id="api_acme_corp" | stats count by outcome method | sort -count
```

</details>

#### Top Clients by Request Volume

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
topk(10,
  sum by (client_id) (count_over_time({job="inferadb-ledger"} | json [1h]))
)
```

</details>

<details>
<summary>Datadog</summary>

```
service:inferadb-ledger | group by client_id | count() | top 10
```

</details>

### Capacity Planning

#### Request Distribution by Organization

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
topk(10,
  sum by (organization_slug) (count_over_time({job="inferadb-ledger"} | json [1h]))
)
```

</details>

<details>
<summary>Datadog</summary>

```
service:inferadb-ledger | timeseries count() by organization_slug
```

</details>

#### Batch Coalescing Efficiency

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
avg_over_time(
  {job="inferadb-ledger"} | json | batch_coalesced="true" | unwrap batch_size [5m]
)
```

</details>

<details>
<summary>Splunk</summary>

```spl
index=inferadb batch_coalesced=true | timechart avg(batch_size) span=5m
```

</details>

#### Idempotency Cache Hit Rate

<details>
<summary>Elasticsearch / OpenSearch</summary>

```json
{
  "aggs": {
    "cache_hit_rate": {
      "filters": {
        "filters": {
          "hits": { "term": { "idempotency_hit": true } },
          "total": { "term": { "method": "write" } }
        }
      }
    }
  }
}
```

</details>

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
sum(count_over_time({job="inferadb-ledger"} | json | idempotency_hit="true" [1h]))
/
sum(count_over_time({job="inferadb-ledger"} | json | method="write" [1h]))
```

</details>

### Compliance and Audit

#### All Admin Operations

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
{job="inferadb-ledger"} | json | service="AdminService"
```

</details>

<details>
<summary>Datadog</summary>

```
service:inferadb-ledger service:AdminService
```

</details>

#### Precondition Failures with Context

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
{job="inferadb-ledger"} | json | outcome="precondition_failed"
| line_format "{{.error_key}}: {{.error_message}}"
```

</details>

### Distributed Tracing

#### Find All Events in a Trace

<details>
<summary>Elasticsearch / OpenSearch</summary>

```json
{
  "query": { "term": { "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736" } },
  "sort": [{ "@timestamp": "asc" }]
}
```

</details>

<details>
<summary>Grafana Loki (LogQL)</summary>

```logql
{job="inferadb-ledger"} | json | trace_id="4bf92f3577b34da6a3ce929d0e0e4736"
```

</details>

## Sampling

Request logging uses **tail sampling**: the decision to emit or suppress an event is made at request completion, not at start. This ensures important events are never dropped.

### Sampling Decision Flow

```
Request completes
       │
       ▼
┌──────────────────┐
│ Is outcome error?│──Yes──► EMIT (100%)
└────────┬─────────┘
         │ No
         ▼
┌──────────────────┐
│ Is request slow? │──Yes──► EMIT (100%)
└────────┬─────────┘
         │ No
         ▼
┌──────────────────┐
│ Is admin op?     │──Yes──► EMIT (100%)
└────────┬─────────┘
         │ No
         ▼
┌──────────────────┐
│ Is VIP organization?│──Yes──► SAMPLE at 50%
└────────┬─────────┘
         │ No
         ▼
┌──────────────────┐
│ Is write op?     │──Yes──► SAMPLE at 10%
└────────┬─────────┘
         │ No (read)
         ▼
       SAMPLE at 1%
```

### Deterministic Sampling

Sampling uses `request_id` as a seed: `seahash(request_id) % 10000 < (rate * 10000)`.

This means:

- The same `request_id` always produces the same sampling decision
- Useful for debugging: you can determine why a specific request was/wasn't sampled
- No randomness required at runtime

### Slow Thresholds

| Operation | Default Threshold | Rationale                                      |
| --------- | ----------------- | ---------------------------------------------- |
| Read      | 10ms              | Reads should be fast; 10ms indicates a problem |
| Write     | 100ms             | Writes involve consensus; allow more time      |
| Admin     | 1000ms            | Admin ops may involve bulk operations          |

### VIP Organizations

VIP organizations get 50% sampling (vs 10% for writes, 1% for reads):

1. **Static configuration**: List organization IDs in `vip_organizations`
2. **Dynamic discovery**: Tag organizations with `vip=true` in `_system` metadata
3. **Override behavior**: Static config takes precedence over dynamic discovery

Dynamic VIP tags can be modified at runtime without restarting the server. Changes take effect within 60 seconds (cache TTL).

## Migration from Previous Logging

Canonical log lines replace scattered log statements with a single structured event:

| Before                                 | After                                                               |
| -------------------------------------- | ------------------------------------------------------------------- |
| Multiple `debug!`, `info!` per request | Single JSON event on completion                                     |
| Unstructured text messages             | Typed, queryable fields                                             |
| Manual correlation via grep            | Automatic correlation via `request_id`, `trace_id`                  |
| Inconsistent timing measurements       | Standardized `duration_ms`, `raft_latency_ms`, `storage_latency_ms` |
| Ad-hoc error formats                   | Consistent `outcome`, `error_code`, `error_message`                 |

### Field Naming

All fields use `snake_case` for Elasticsearch compatibility (`request_id` not `requestId`).

### Timestamp Format

RFC3339 with microsecond precision: `2026-01-28T18:30:45.123456Z`

### Hash Truncation

Block hashes and state roots are truncated to 16 hex characters for storage efficiency while maintaining correlation capability.

### Metrics vs Request Logging

Request logs **supplement**, not replace, existing Prometheus metrics:

| Metrics                          | Request Logs                             |
| -------------------------------- | ---------------------------------------- |
| Aggregated time-series           | Individual request detail                |
| Low cardinality (method, status) | High cardinality (request_id, client_id) |
| Real-time dashboards, alerts     | Forensic debugging, auditing             |
| Prometheus scrape model          | Log streaming model                      |

Both are emitted from the same code paths. Keep existing metrics for alerting; use request logs for debugging.

### Schema Versioning

Request logs include a `schema_version` field. See [Dashboard Templates](dashboards/) for version compatibility.

**Compatibility guarantees:**

- New fields are additive; old queries continue to work
- Field types never change without a major version bump
- Removed fields are documented in release notes

## Troubleshooting

### No Events Appearing

1. Verify logging is enabled:

   ```bash
   echo $INFERADB__LEDGER__LOGGING__ENABLED  # Should be true or unset (default true)
   ```

2. Check log format is JSON for production:

   ```bash
   echo $INFERADB__LEDGER__LOG_FORMAT  # Should be json
   ```

3. Verify logs reach your aggregation system (check Promtail, Fluent Bit, etc.)

### Events Being Sampled Out

If you're not seeing expected events:

1. Check if the request was sampled using the deterministic formula
2. Errors and slow requests are always emitted (100% sample rate)
3. Temporarily increase sampling rates for debugging

### Query Errors

**Loki**: Verify JSON parsing works:

```logql
{job="inferadb-ledger"} | json | __error__=""
```

**Elasticsearch**: Confirm field mappings match expected types.

**Datadog**: Ensure facets are configured for filtered fields.

## Performance

Request logging adds minimal overhead:

| Metric                | Measured | Target     |
| --------------------- | -------- | ---------- |
| Context creation      | ~147ns   | <1μs       |
| Full field population | ~1.2μs   | <10μs      |
| Sampling decision     | ~2-4ns   | <1μs       |
| End-to-end overhead   | ~1.1μs   | <100μs p99 |

Run benchmarks:

```bash
cargo bench -p inferadb-ledger-raft --bench logging_bench
```

## Related Documentation

- [Configuration Reference](configuration.md#logging) - All environment variables
- [Dashboard Templates](dashboards/) - Pre-built Grafana, Kibana, Datadog dashboards
- [Metrics Reference](metrics-reference.md) - Prometheus metrics (complementary)
- [Alerting Guide](alerting.md) - Using metrics for alerts
- [Developer Guide](../development/logging.md) - Adding request logging to new services
