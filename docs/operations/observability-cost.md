# Observability Cost

Guidance for operators running InferaDB Ledger against paid observability vendors (Datadog, New Relic, Honeycomb, etc.) to control ingestion and custom-metric costs.

This guide uses **Datadog** as the worked example because its pricing model and knobs are widely documented. The same principles — cardinality budgeting, metric filtering at source, sampling hot log streams, using vendor-native "metrics-without-limits" or equivalent features — apply to comparable platforms; check your vendor's documentation for the specific knob names.

## Custom Metric Pricing Model

Datadog bills by **unique time series**, where each time series is the combination of a metric name and a distinct tag set. A single metric emitted with 5 unique label combinations counts as 5 custom metrics. At scale, high-cardinality labels (organization ID, user ID, vault ID) are the primary cost driver.

InferaDB Ledger avoids high-cardinality labels on Prometheus metrics by design. Entity IDs, user IDs, and region are emitted as structured log fields, not metric tags. See [Metrics Reference](metrics-reference.md) for the "What's NOT a metric" rules.

## Distributions vs Histograms

By default, Ledger uses the standard Prometheus histogram format. Each histogram produces `_bucket`, `_sum`, and `_count` series, plus a series per bucket boundary per label combination.

If you run the DogStatsD agent and want native Datadog distribution metrics (which support arbitrary post-ingest percentile queries and Metrics Without Limits):

```bash
# Enable DogStatsD distribution emission at build time
cargo build --features dogstatsd
```

Or set the environment variable at runtime:

```bash
INFERADB__LEDGER__DOGSTATSD_ADDR=127.0.0.1:8125
```

When enabled, latency histograms emit as `distribution` type to DogStatsD **in addition to** the standard Prometheus `/metrics` endpoint. Use distributions when you need percentile slicing by tag combinations not known at scrape time. Disable if you are scraping Prometheus only — double-emission wastes DogStatsD ingestion budget.

## Per-Region Tenant-Tag Pattern

Do not add `organization_id` or `vault_id` tags to metrics — these are intentionally absent from the Prometheus metric schema. Instead, add region-level context at the Datadog agent level:

```yaml
# datadog-agent.yaml
tags:
  - region:us-east-1
  - env:production
  - cluster:ledger-primary
```

This adds a single `region` tag to every metric scraped from that agent, costing O(1) additional series per metric rather than O(organizations) — typically a 1000x cost reduction for multi-tenant deployments.

Per-organization breakdowns belong in Logs, not metrics. See [Per-Tenant Queries via Logs](#per-tenant-queries-via-logs) below.

## Metrics Without Limits Config

Cluster-aggregate metrics (quorum status, leader status) should not carry `host` tags, which add one series per node. Apply Metrics Without Limits in the Datadog UI or via the API to drop `host` from these:

```yaml
metrics_without_limits:
  queries:
    - metric: ledger_cluster_quorum_status
      tags: []
    - metric: inferadb_ledger_raft_is_leader
      tags: []
    - metric: ledger_leader_elections_total
      tags: []
    - metric: inferadb_ledger_raft_term
      tags: []
```

For metrics that carry meaningful per-node context (queue depths, peer send queues), retain `instance` or `peer` tags but drop `host`:

```yaml
metrics_without_limits:
  queries:
    - metric: ledger_peer_send_queue_depth
      tags: ["peer"]
    - metric: inferadb_ledger_raft_proposals_pending
      tags: ["instance"]
```

## Per-Tenant Queries via Logs

For organization-scoped diagnostics, query structured logs instead of metrics. Ledger emits a canonical log line per request with all relevant fields, including `organization_id`, `vault_id`, `method`, `outcome`, and `duration_ms`.

Example Datadog Logs queries:

```text
# Error rate for a specific organization
service:inferadb-ledger @organization_id:12345 @outcome:error

# p99 write latency for a specific vault (use the Percentile aggregation)
service:inferadb-ledger @vault_id:67890 @method:SetRelationships
group by @method, aggregate: pc99(@duration_ms)

# Rate-limited requests by organization over the last hour
service:inferadb-ledger @error_code:RESOURCE_EXHAUSTED
group by @organization_id
```

This keeps metric cardinality flat while giving full per-tenant observability through Logs analytics.

## Cost Estimates

Approximate custom metric counts for a 3-node cluster at steady state:

| Configuration                             | Approx. Custom Metrics | Monthly Cost (Datadog Pro) |
| ----------------------------------------- | ---------------------- | -------------------------- |
| Default (no optimization)                 | ~2,400                 | ~$36,000/mo at 1K orgs     |
| With Metrics Without Limits + no org tags | ~200–300               | ~$200–300/mo               |

The "before" estimate assumes naive tagging of every metric with `organization_id` across ~1,000 organizations. The "after" estimate reflects Ledger's default schema (no org-ID tags) plus Metrics Without Limits applied to cluster-aggregate metrics.

> These are estimates. Actual costs depend on Datadog contract tiers, included allocations, and which optional metrics are enabled.

## Checklist

- [ ] DogStatsD enabled only if you need distribution percentile queries
- [ ] `region`, `env`, `cluster` added at agent level — not per-request
- [ ] `organization_id` / `vault_id` labels absent from all scraped metrics (verify: no spikes in custom metric count when org count grows)
- [ ] Metrics Without Limits applied to cluster-aggregate metrics (`quorum_status`, `is_leader`, `raft_term`)
- [ ] Per-tenant diagnostics routed to Logs queries, not metric dashboards
