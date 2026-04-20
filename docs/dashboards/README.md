# Dashboard Templates

Pre-built observability dashboards for InferaDB Ledger. Import into your monitoring platform of choice.

All templates target the **current** metric and log-field names shipped by the server and SDK. Dashboard files do not carry version suffixes in their filenames; versioning lives in the JSON metadata (`schema_version`, `min_ledger_version`) so the file names stay stable as the schema evolves.

## Grafana (Prometheus data source)

| File                           | Scope                                                         | Audience                      |
| ------------------------------ | ------------------------------------------------------------- | ----------------------------- |
| `grafana-all-in-one.json`      | Comprehensive: cluster health, latency, batching, durability, storage | SREs / on-call                |
| `api-performance.json`         | Focused: gRPC latency, throughput, error rates                | Platform / API owners         |
| `resource-saturation.json`     | Focused: disk, cache, B-tree saturation indicators            | Capacity planners / SREs      |
| `storage-engine.json`          | Focused: B+ tree internals and compaction                     | Storage engineers             |
| `grafana-events.json`          | Events service: ingest rate, flush latency, denial patterns   | Event-stream consumers        |

**Import**

```bash
# Option A — Grafana UI
# Dashboards → New → Import → Upload JSON
# Select a Prometheus data source when prompted.

# Option B — API
curl -X POST \
  -H "Authorization: Bearer $GRAFANA_API_KEY" \
  -H "Content-Type: application/json" \
  -d @docs/dashboards/grafana-all-in-one.json \
  https://grafana.example.com/api/dashboards/db
```

Prerequisites: Prometheus data source scraping the Ledger metrics endpoint (see `docs/operations/metrics-reference.md`).

## Grafana (Loki data source)

| File                  | Scope                                            |
| --------------------- | ------------------------------------------------ |
| `grafana-loki.json`   | Request logs, error patterns, audit trail queries. |

Prerequisites: Loki collecting Ledger stdout/stderr; server configured for JSON structured logging (see `docs/operations/logging.md`).

## Kibana

| File              | Scope                                            |
| ----------------- | ------------------------------------------------ |
| `kibana.ndjson`   | Saved searches, visualizations, and dashboards for log-based observability. |

**Import**: Kibana → Stack Management → Saved Objects → Import → select `kibana.ndjson`.

Prerequisites: Elasticsearch / OpenSearch index configured for Ledger log ingestion, with field mappings matching the canonical log schema (request_id, trace_id, organization_slug, vault_slug, action, outcome, error_code, etc.).

## Datadog

| File             | Scope                                               |
| ---------------- | --------------------------------------------------- |
| `datadog.json`   | Log-based dashboard: throughput, latency, error breakdown by service/method. |

**Import**: Datadog → Dashboards → New Dashboard → Import Dashboard JSON.

Prerequisites: Datadog agent configured to ingest Ledger logs (`pipeline` + facet configuration for the request-log schema). See [`docs/operations/observability-cost.md`](../operations/observability-cost.md) for cost-tuning guidance.

## Compatibility

Every template declares `schema_version` and `min_ledger_version` in its JSON metadata. If you pin Ledger below `min_ledger_version`, some panels may reference metrics/fields not yet present. Check the dashboard's metadata before importing against an older deployment.

Metric drift is protected by the `documentation-reviewer` agent (invariant #5 — dashboard metric references must resolve in `crates/raft/src/metrics.rs` or `crates/sdk/src/metrics.rs`). If you rename or remove a metric, update the affected dashboards in the same PR.

## Related

- [Metrics reference](../operations/metrics-reference.md) — authoritative metric catalog.
- [Alerting thresholds](../operations/alerting.md) — recommended warning/critical levels and PromQL expressions.
- [SLO reference](../operations/slo.md) — service-level objectives and SLI definitions.
- [Logging](../operations/logging.md) — log schema, fields, and query patterns.
- [Observability cost](../operations/observability-cost.md) — cost-tuning for Datadog and comparable vendors.
