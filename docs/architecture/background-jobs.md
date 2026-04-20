# Background Job Observability

InferaDB Ledger runs several background jobs that maintain data integrity, manage storage, handle key rotation, and ensure high availability. All jobs emit metrics through `JobContext`, which captures duration, result, and items processed automatically on cycle completion. Jobs do not call Prometheus APIs directly.

```rust
// Typical job cycle pattern
let ctx = JobContext::start("my_job");
let items = do_work()?;
ctx.set_items_processed(items);
// ctx.drop() emits all three metrics
```

## Metrics

| Metric                                        | Type      | Labels          | Description                |
| --------------------------------------------- | --------- | --------------- | -------------------------- |
| `ledger_background_job_duration_seconds`      | Histogram | `job`           | Duration of each job cycle |
| `ledger_background_job_runs_total`            | Counter   | `job`, `result` | Total cycles executed      |
| `ledger_background_job_items_processed_total` | Counter   | `job`           | Work items processed       |

### Label Values

**`job`**: `auto_recovery`, `backup`, `block_compaction`, `btree_compaction`, `dek_rewrap`, `dependency_health`, `events_gc`, `hot_key_detector`, `integrity_scrub`, `invite_maintenance`, `learner_refresh`, `organization_purge`, `orphan_cleanup`, `post_erasure_compaction`, `resource_metrics`, `saga_orchestrator`, `state_root_verifier`, `token_maintenance`, `ttl_gc`, `user_retention` (20 jobs)

**`result`**: `success`, `failure`

### Items Processed per Job

| Job                       | Item Meaning                                                                  |
| ------------------------- | ----------------------------------------------------------------------------- |
| `auto_recovery`           | Vaults successfully recovered from Diverged state                             |
| `backup`                  | Backups created                                                               |
| `block_compaction`        | Transaction bodies reclaimed from blocks in vaults with `COMPACTED` retention |
| `btree_compaction`        | B-tree leaf pages merged                                                      |
| `dek_rewrap`              | Per-page DEK sidecars re-wrapped to a newer RMK version                       |
| `dependency_health`       | Dependency checks performed (disk, peers, Raft lag)                           |
| `events_gc`               | Expired events.db entries removed                                             |
| `hot_key_detector`        | Detector ticks (Count-Min Sketch windows rotated)                             |
| `integrity_scrub`         | Pages checked (checksum + structural)                                         |
| `invite_maintenance`      | Invitations expired/reaped                                                    |
| `learner_refresh`         | Learner nodes refreshed                                                       |
| `organization_purge`      | Organizations purged after retention                                          |
| `orphan_cleanup`          | Orphaned membership / token records removed                                   |
| `post_erasure_compaction` | Proactive Raft snapshots triggered after crypto-shredding                     |
| `resource_metrics`        | Metric collection cycles completed                                            |
| `saga_orchestrator`       | Sagas processed per cycle                                                     |
| `state_root_verifier`     | State-root verification rounds (detects divergence)                           |
| `token_maintenance`       | Expired refresh tokens deleted + signing keys transitioned                    |
| `ttl_gc`                  | Expired entities removed                                                      |
| `user_retention`          | Users reaped after retention expiry                                           |

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

### Token Maintenance

Cleans up expired refresh tokens, transitions rotated signing keys, and garbage-collects expired onboarding and TOTP challenge records. Three phases per cycle:

1. **Phase 1**: Proposes `DeleteExpiredRefreshTokens` through Raft (also garbage-collects poisoned token families)
2. **Phase 2**: Scans for rotated signing keys past `valid_until`, proposes `TransitionSigningKeyRevoked` for each
3. **Phase 3**: Proposes `CleanupExpiredOnboarding` which scans three `_tmp:` prefixes: `_tmp:onboard_verify:` (email verification codes), `_tmp:onboard_account:` (onboarding accounts), and `_tmp:totp_challenge:` (TOTP second-factor challenges). Expired records are deleted in a single Raft proposal.

All phases go through Raft — background jobs cannot bypass consensus for state changes.

- **Default interval**: 5 minutes (300 seconds)
- **Leader only**: Yes
- **Config**: `token_maintenance_interval_secs` in server Config

### TTL Garbage Collector

Removes expired entities (those with `expires_at` in the past) from vault storage.

- **Default interval**: 5 minutes
- **Leader only**: Yes

### Learner Refresh

Refreshes Raft learner nodes to keep them caught up with the cluster.

- **Default interval**: Configurable
- **Leader only**: Yes

### Organization Purge

Purges soft-deleted organizations after their retention period expires. Removes all associated data (vaults, entities, relationships, members).

- **Default interval**: 1 hour
- **Leader only**: Yes
- **Config**: `OrganizationPurgeConfig` (retention period)

### User Retention Reaper

Reaps soft-deleted users after the retention period expires. Removes user records from the system organization.

- **Default interval**: 1 hour
- **Leader only**: Yes
- **Config**: `UserRetentionConfig` (retention period)

### Events Garbage Collector

Removes expired audit events based on their TTL. Runs per-region to clean up events that have exceeded their configured time-to-live.

- **Default interval**: 1 hour
- **Leader only**: Yes
- **Config**: `EventConfig` (default_ttl_days)

### Resource Metrics Collector

Periodically collects disk space, page cache, and B-tree depth metrics for Prometheus exposure. Not a traditional "job" but a periodic metric collection cycle.

- **Default interval**: 30 seconds
- **Leader only**: No (runs on all nodes)

### Saga Orchestrator

Drives multi-step distributed workflows (sagas) to completion. Each cycle scans for in-progress sagas, advances them through their next step, and handles compensation on failure. Covers operations like user creation, organization creation, and signing key bootstrap (`CreateSigningKeySaga`).

- **Default interval**: Configurable
- **Leader only**: Yes

### Hot Key Detector

Maintains a Count-Min Sketch over incoming write/read keys to surface hot spots to Prometheus. The "job" tick rotates the detector's sliding window rather than performing I/O.

- **Default interval**: Window-rotation cadence (seconds)
- **Leader only**: No (runs on every node that serves data-plane RPCs)

### State Root Verifier

Recomputes state roots and cross-checks them against committed block headers to detect silent divergence (non-determinism bugs, disk corruption, timestamp drift). Complements apply-time verification: catches cases where a node applied "successfully" but produced a root that disagrees with the cluster.

- **Default interval**: Configurable
- **Leader only**: No (runs on all nodes so divergence is detected wherever it occurs)

### Dependency Health

Periodically probes external dependencies (disk writability, peer reachability, Raft replication lag) and caches the results for the `HealthService` readiness probe. Cached with a short TTL so aggressive probe intervals don't storm the checks.

- **Default interval**: Configurable (`dependency_check_timeout_secs`, default `2`); cache TTL `health_cache_ttl_secs` (default `5`)
- **Leader only**: No

### Invite Maintenance

Two-phase invitation lifecycle management. Phase 1 expires `Pending` invitations whose `expires_at` has passed. Phase 2 reaps terminal invitations (Accepted/Declined/Expired/Revoked) past a 90-day retention window.

- **Default interval**: Configurable
- **Leader only**: Yes
- **Per-cycle bounds**: 200 expirations, 100 reaps; scans up to 5000 email-hash index entries per cycle

### Post-Erasure Compaction

After crypto-shredding removes a `UserShredKey` or `OrgShredKey`, sealed plaintext in prior Raft log entries is already unreadable — but still occupies disk. This job triggers proactive Raft snapshots to evict those ciphertext entries from the log, completing the erasure at the storage layer.

- **Default interval**: `check_interval_secs` (default `300`); retention window `max_log_retention_secs` (default `3600`)
- **Leader only**: No (runs on GLOBAL + all regional groups; per-group tracking of last snapshot time)

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
