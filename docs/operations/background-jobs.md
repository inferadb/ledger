# Background Job Observability

InferaDB Ledger runs several background jobs that maintain data integrity, manage storage, handle key rotation, and ensure high availability. Most jobs emit three standardized metrics for unified monitoring.

## Metrics

| Metric                                        | Type      | Labels          | Description                |
| --------------------------------------------- | --------- | --------------- | -------------------------- |
| `ledger_background_job_duration_seconds`      | Histogram | `job`           | Duration of each job cycle |
| `ledger_background_job_runs_total`            | Counter   | `job`, `result` | Total cycles executed      |
| `ledger_background_job_items_processed_total` | Counter   | `job`           | Work items processed       |

### Label Values

**`job`**: `gc`, `compaction`, `integrity_scrub`, `auto_recovery`, `backup`, `dek_rewrap`, `orphan_cleanup`, `saga_orchestrator`

**`result`**: `success`, `failure`

### Items Processed per Job

| Job                 | Item Meaning                          |
| ------------------- | ------------------------------------- |
| `gc`                | Blocks compacted (retention mode)     |
| `compaction`        | B-tree pages merged                   |
| `integrity_scrub`   | Pages checked (checksum + structural) |
| `auto_recovery`     | Vaults successfully recovered         |
| `backup`            | Backups created                       |
| `dek_rewrap`        | Pages re-wrapped with new RMK         |
| `orphan_cleanup`    | Orphaned membership records removed   |
| `saga_orchestrator` | Sagas processed per cycle             |

## Jobs

### GC (Block Compactor)

Removes transaction bodies from old blocks in vaults with `COMPACTED` retention mode. Preserves block headers for chain verification.

- **Default interval**: 5 minutes
- **Leader only**: Yes
- **Config**: Per-vault `retention_policy` (mode + retention_blocks)

### B-tree Compaction

Merges underfull leaf nodes after deletions to reclaim space and maintain read performance. Uses forward-only O(N) single-pass compaction via the leaf linked list (`next_leaf` sibling pointers).

The compaction algorithm:

1. Starts at the leftmost leaf and walks the `next_leaf` chain forward.
2. For each underfull leaf, checks whether the next neighbor shares the same parent branch node.
3. If both are underfull and share a parent, merges them (greedy — re-checks the merged leaf against its new neighbor).
4. If they span a branch boundary (different parents), skips to avoid cross-branch complexity.
5. After merging, the right leaf is freed and the left leaf's `next_leaf` is updated to skip the freed page.
6. If the parent becomes empty and is the root, performs root collapse.

This replaces the previous O(N²) algorithm that collected all leaf info and restarted from the beginning after each merge.

- **Default interval**: 1 hour
- **Leader only**: Yes
- **Config**: `BTreeCompactionConfig` (interval_secs, min_fill_factor)

### Integrity Scrubber

Progressively verifies page checksums and B-tree structural invariants to detect silent corruption (bit rot).

- **Default interval**: 1 hour
- **Leader only**: Yes
- **Config**: `IntegrityConfig` (scrub_interval_secs, pages_per_cycle_percent, full_scan_period_secs)

### Auto-Recovery

Scans for diverged vaults and attempts automatic recovery via snapshot replay with exponential backoff.

- **Default interval**: 30 seconds
- **Leader only**: Yes
- **Config**: `RecoveryConfig` (scan_interval, base_retry_delay, max_retry_delay)

### Backup

Creates periodic full-snapshot backups with metadata and optional pruning.

- **Default interval**: Configurable via `BackupConfig`
- **Leader only**: Yes
- **Config**: `BackupConfig` (interval_secs, retention_count, backup_dir)

### DEK Re-wrap

Re-wraps page-level data encryption keys (DEKs) after RMK (Root Master Key) rotation. Iterates all pages in the crypto sidecar, unwrapping each DEK with the old RMK and re-wrapping with the new RMK. Only sidecar metadata changes — encrypted page bodies are never touched. The job is resumable and idempotent: pages already at the target version are skipped.

- **Default interval**: 5 minutes (300 seconds)
- **Leader only**: Yes
- **Config**: `RewrapConfig` (enabled, batch_size, interval_secs, target_rmk_version)
- **Status**: Query progress via `AdminService/GetRewrapStatus`

### Orphan Cleanup

Removes orphaned membership records left behind when users are deleted from the system organization. Scans each organization's vaults for memberships referencing deleted users and removes them through Raft consensus. Yields between organization scans to avoid I/O bursts.

- **Default interval**: 1 hour
- **Leader only**: Yes
- **Actor**: `system:orphan_cleanup` (audit trail)

### Saga Orchestrator

Drives multi-step distributed workflows (sagas) to completion. Each cycle scans for in-progress sagas, advances them through their next step, and handles compensation on failure. Sagas cover operations like user deletion (which spans system and organization vaults).

- **Default interval**: Configurable
- **Leader only**: Yes

## Alerting

### Job Not Running

Alert when a job hasn't completed a cycle in 2x its expected interval:

```promql
# GC job not running (expected every 5 minutes)
time() - (ledger_background_job_runs_total{job="gc"} > 0)
  > 600
```

Use `increase()` for a more practical approach:

```promql
# No GC runs in the last 15 minutes
increase(ledger_background_job_runs_total{job="gc"}[15m]) == 0
```

### Failure Rate

Alert when failure rate exceeds 10% over a 1-hour window:

```promql
rate(ledger_background_job_runs_total{job=~".+", result="failure"}[1h])
/
rate(ledger_background_job_runs_total{job=~".+"}[1h])
> 0.1
```

### Duration Anomaly

Alert when p99 duration exceeds 2x the median:

```promql
histogram_quantile(0.99, rate(ledger_background_job_duration_seconds_bucket{job="compaction"}[1h]))
>
2 * histogram_quantile(0.50, rate(ledger_background_job_duration_seconds_bucket{job="compaction"}[1h]))
```

### Zero Items Processed

Alert when a job runs successfully but processes nothing over an extended period (may indicate misconfiguration):

```promql
# Compaction running but no pages merged in 24 hours
increase(ledger_background_job_runs_total{job="compaction", result="success"}[24h]) > 0
and
increase(ledger_background_job_items_processed_total{job="compaction"}[24h]) == 0
```

## Dashboard Queries

### Job Health Overview

```promql
# Success rate per job (last hour)
sum by (job) (rate(ledger_background_job_runs_total{result="success"}[1h]))
/
sum by (job) (rate(ledger_background_job_runs_total[1h]))
```

### Duration Percentiles

```promql
# p50/p95/p99 duration per job
histogram_quantile(0.50, sum by (job, le) (rate(ledger_background_job_duration_seconds_bucket[1h])))
histogram_quantile(0.95, sum by (job, le) (rate(ledger_background_job_duration_seconds_bucket[1h])))
histogram_quantile(0.99, sum by (job, le) (rate(ledger_background_job_duration_seconds_bucket[1h])))
```

### Throughput

```promql
# Items processed per second per job
rate(ledger_background_job_items_processed_total[5m])
```
