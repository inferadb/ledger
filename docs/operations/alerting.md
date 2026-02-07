# Alerting Thresholds

Recommended alerting thresholds for InferaDB Ledger resource saturation metrics.

## Disk Space

| Metric                                             | Warning | Critical | Action                                              |
| -------------------------------------------------- | ------- | -------- | --------------------------------------------------- |
| `ledger_disk_bytes_used / ledger_disk_bytes_total` | > 80%   | > 90%    | Expand storage, run compaction, prune old snapshots |

```promql
# Warning: disk usage above 80%
ledger_disk_bytes_used / ledger_disk_bytes_total > 0.80

# Critical: disk usage above 90%
ledger_disk_bytes_used / ledger_disk_bytes_total > 0.90
```

## Page Cache

| Metric                                                                                                                                   | Warning        | Critical       | Action                                    |
| ---------------------------------------------------------------------------------------------------------------------------------------- | -------------- | -------------- | ----------------------------------------- |
| Hit rate: `rate(ledger_page_cache_hits_total[5m]) / (rate(ledger_page_cache_hits_total[5m]) + rate(ledger_page_cache_misses_total[5m]))` | < 90%          | < 80%          | Increase `cache_size` in `StorageConfig`  |
| `ledger_page_cache_size`                                                                                                                 | > 90% capacity | > 95% capacity | Increase cache size or reduce working set |

```promql
# Warning: cache hit rate below 90%
rate(ledger_page_cache_hits_total[5m])
/ (rate(ledger_page_cache_hits_total[5m]) + rate(ledger_page_cache_misses_total[5m]))
< 0.90

# Cache size relative to configured capacity (application-specific)
ledger_page_cache_size
```

## B-tree Health

| Metric                                     | Warning   | Critical  | Action                                          |
| ------------------------------------------ | --------- | --------- | ----------------------------------------------- |
| `ledger_btree_depth`                       | > 5       | > 7       | Increase page size, review key distribution     |
| `rate(ledger_btree_page_splits_total[5m])` | > 100/min | > 500/min | Indicates heavy write load; review batch sizing |

```promql
# Warning: excessive B-tree depth
ledger_btree_depth > 5

# Warning: high split rate
rate(ledger_btree_page_splits_total[5m]) > 100/60
```

## Compaction Lag

| Metric                         | Warning | Critical | Action                                            |
| ------------------------------ | ------- | -------- | ------------------------------------------------- |
| `ledger_compaction_lag_blocks` | > 1000  | > 5000   | Run manual compaction, reduce compaction interval |

```promql
# Warning: compaction falling behind
ledger_compaction_lag_blocks > 1000

# Critical: large compaction backlog
ledger_compaction_lag_blocks > 5000
```

## Snapshot Disk Usage

| Metric                       | Warning | Critical | Action                                                |
| ---------------------------- | ------- | -------- | ----------------------------------------------------- |
| `ledger_snapshot_disk_bytes` | > 10 GB | > 50 GB  | Reduce `max_snapshots`, increase compaction frequency |

```promql
# Warning: snapshot disk usage above 10GB
ledger_snapshot_disk_bytes > 10737418240

# Critical: snapshot disk usage above 50GB
ledger_snapshot_disk_bytes > 53687091200
```

## Combined USE Method Dashboard

For each resource, monitor Utilization, Saturation, and Errors:

| Resource   | Utilization                                        | Saturation                                      | Errors                                                               |
| ---------- | -------------------------------------------------- | ----------------------------------------------- | -------------------------------------------------------------------- |
| Disk       | `ledger_disk_bytes_used / ledger_disk_bytes_total` | `ledger_compaction_lag_blocks`                  | Disk I/O errors in logs                                              |
| Page Cache | `ledger_page_cache_size / capacity`                | Miss rate: `rate(misses) / rate(hits + misses)` | `ledger_page_cache_misses_total` spikes                              |
| B-tree     | `ledger_btree_depth`                               | `rate(ledger_btree_page_splits_total)`          | Store errors in `ledger_grpc_requests_total{error_class="internal"}` |
| Snapshots  | `ledger_snapshot_disk_bytes`                       | —                                               | Snapshot creation failures in logs                                   |

## Collection Interval

Resource metrics are collected every 30 seconds by default. Alerting rules should use windows of at least `[5m]` to avoid false positives from transient spikes.
