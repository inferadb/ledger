# Events Operations Guide

Operations reference for InferaDB Ledger's audit event system.

## Architecture Overview

The events system uses two layers:

1. **Blockchain** — Immutable, Raft-replicated record of every committed write (entities, relationships). Query via `GetBlock` with `block_height` from event entries.
2. **Events table** — Supplementary `Events` B+ tree in `events.db` for admin operations, denied requests, and system events. Organization-scoped with TTL-based garbage collection.

### Emission Paths

- **Apply-phase**: Generated during Raft state machine apply. Deterministic — identical on all replicas. Included in snapshots for follower joins.
- **Handler-phase**: Generated at gRPC service layer for pre-Raft rejections and admin operations. Node-local, not replicated.

### Multi-Service Context

Ledger serves as the central audit store. Engine and Control write events via the `IngestEvents` RPC. See [audit-protocol.md](../architecture/audit-protocol.md) for integration details.

## Configuration Reference

### EventConfig

| Field                      | Env Var                                              | Default  | Description                         |
| -------------------------- | ---------------------------------------------------- | -------- | ----------------------------------- |
| `enabled`                  | `INFERADB__LEDGER__EVENTS__ENABLED`                  | `true`   | Master switch for event logging     |
| `default_ttl_days`         | `INFERADB__LEDGER__EVENTS__DEFAULT_TTL_DAYS`         | `90`     | TTL in days (1–3650)                |
| `max_details_size_bytes`   | `INFERADB__LEDGER__EVENTS__MAX_DETAILS_SIZE_BYTES`   | `4096`   | Max details map size (256–65536)    |
| `system_log_enabled`       | `INFERADB__LEDGER__EVENTS__SYSTEM_LOG_ENABLED`       | `true`   | Enable system-scope event logging   |
| `organization_log_enabled` | `INFERADB__LEDGER__EVENTS__ORGANIZATION_LOG_ENABLED` | `true`   | Enable org-scope event logging      |
| `max_snapshot_events`      | `INFERADB__LEDGER__EVENTS__MAX_SNAPSHOT_EVENTS`      | `100000` | Max apply-phase events in snapshots |

### IngestionConfig

| Field                                    | Env Var                                                             | Default                | Description                       |
| ---------------------------------------- | ------------------------------------------------------------------- | ---------------------- | --------------------------------- |
| `ingestion.ingest_enabled`               | `INFERADB__LEDGER__EVENTS__INGESTION__INGEST_ENABLED`               | `true`                 | Master switch for external ingest |
| `ingestion.allowed_sources`              | `INFERADB__LEDGER__EVENTS__INGESTION__ALLOWED_SOURCES`              | `["engine","control"]` | Allow-list of source services     |
| `ingestion.max_ingest_batch_size`        | `INFERADB__LEDGER__EVENTS__INGESTION__MAX_INGEST_BATCH_SIZE`        | `500`                  | Max events per IngestEvents call  |
| `ingestion.ingest_rate_limit_per_source` | `INFERADB__LEDGER__EVENTS__INGESTION__INGEST_RATE_LIMIT_PER_SOURCE` | `10000`                | Events/sec per source service     |

### TOML Example

```toml
[events]
enabled = true
default_ttl_days = 90
max_details_size_bytes = 4096
system_log_enabled = true
organization_log_enabled = true
max_snapshot_events = 100000

[events.ingestion]
ingest_enabled = true
allowed_sources = ["engine", "control"]
max_ingest_batch_size = 500
ingest_rate_limit_per_source = 10000
```

### Runtime Reconfiguration

These fields are updatable via the `UpdateConfig` RPC without restart:

- `default_ttl_days`, `enabled`, `system_log_enabled`, `organization_log_enabled`
- `ingest_enabled`, `ingest_rate_limit_per_source`

## Event Catalog

### System Scope Events

| Event Type                      | Action                   | Emission    | Trigger                     | Details                                |
| ------------------------------- | ------------------------ | ----------- | --------------------------- | -------------------------------------- |
| `ledger.organization.created`   | `organization_created`   | Apply-phase | CreateOrganization RPC      | `slug`                                 |
| `ledger.organization.deleted`   | `organization_deleted`   | Apply-phase | DeleteOrganization RPC      | `slug`, `vault_count`                  |
| `ledger.organization.suspended` | `organization_suspended` | Apply-phase | SuspendOrganization RPC     | `slug`, `reason`                       |
| `ledger.organization.resumed`   | `organization_resumed`   | Apply-phase | ResumeOrganization RPC      | `slug`                                 |
| `ledger.user.created`           | `user_created`           | Apply-phase | CreateUser RPC              | `user_id`, `name`, `email`, `is_admin` |
| `ledger.user.deleted`           | `user_deleted`           | Handler     | DeleteUserSaga completion   | `memberships_removed`                  |
| `ledger.migration.started`      | `migration_started`      | Apply-phase | StartMigration RPC          |                                        |
| `ledger.migration.completed`    | `migration_completed`    | Apply-phase | CompleteMigration RPC       | `shard_id`                             |
| `ledger.node.joined`            | `node_joined_cluster`    | Apply-phase | AddNode Raft command        | `node_id`, `address`                   |
| `ledger.node.left`              | `node_left_cluster`      | Apply-phase | RemoveNode Raft command     | `node_id`                              |
| `ledger.routing.updated`        | `routing_updated`        | Apply-phase | RoutingUpdated Raft command | `shard_id`                             |
| `ledger.config.changed`         | `configuration_changed`  | Handler     | UpdateConfig RPC            | `changed_fields`, `dry_run`            |
| `ledger.backup.created`         | `backup_created`         | Handler     | CreateBackup RPC            | `backup_id`                            |
| `ledger.backup.restored`        | `backup_restored`        | Handler     | RestoreBackup RPC           | `backup_id`                            |
| `ledger.snapshot.created`       | `snapshot_created`       | Handler     | CreateSnapshot RPC          |                                        |

### Organization Scope Events

| Event Type                         | Action                      | Emission    | Trigger                  | Details                                         |
| ---------------------------------- | --------------------------- | ----------- | ------------------------ | ----------------------------------------------- |
| `ledger.vault.created`             | `vault_created`             | Apply-phase | CreateVault RPC          | `vault_slug`, `vault_name`                      |
| `ledger.vault.deleted`             | `vault_deleted`             | Apply-phase | DeleteVault RPC          | `vault_slug`                                    |
| `ledger.write.committed`           | `write_committed`           | Apply-phase | Write RPC commit         | `block_height`, `operations_count`              |
| `ledger.write.batch_committed`     | `batch_write_committed`     | Apply-phase | BatchWrite RPC commit    | `block_height`, `operations_count`              |
| `ledger.entity.expired`            | `entity_expired`            | Apply-phase | TTL GC during apply      | `key`                                           |
| `ledger.vault.health_updated`      | `vault_health_updated`      | Apply-phase | VaultHealth Raft command | `vault_slug`, `status`                          |
| `ledger.integrity.checked`         | `integrity_checked`         | Handler     | CheckIntegrity RPC       | `vault_slug`, `issues_found`, `full_check`      |
| `ledger.vault.recovered`           | `vault_recovered`           | Handler     | RecoverVault RPC         | `vault_slug`, `recovery_method`, `final_height` |
| `ledger.request.rate_limited`      | `request_rate_limited`      | Handler     | Rate limiter rejection   | `level`, `reason`                               |
| `ledger.request.validation_failed` | `request_validation_failed` | Handler     | Validation rejection     | `field`, `reason`                               |
| `ledger.request.quota_exceeded`    | `quota_exceeded`            | Handler     | Quota check rejection    | `quota_type`, `current`, `limit`                |

## Consistency Model

### Apply-Phase Events

- **Identical across replicas**: Same inputs produce same outputs (deterministic UUID v5, block timestamp)
- **Survive failover**: Included in Raft snapshots for follower joins
- **No data loss**: As durable as Raft log entries

### Handler-Phase Events

- **Node-local**: Exist only on the node that handled the request
- **Lost on failover**: Not transferred to new leader
- **Best-effort**: Write failures are logged but don't fail the RPC

This trade-off is intentional. Handler-phase events capture pre-Raft rejections (rate limits, validation failures) that depend on per-node state. Replicating them would require separate Raft proposals for each denial, defeating the purpose of pre-Raft rejection.

## Monitoring

### Prometheus Metrics

| Metric                                    | Type      | Labels                        | Description                       |
| ----------------------------------------- | --------- | ----------------------------- | --------------------------------- |
| `ledger_event_writes_total`               | Counter   | `emission`, `scope`, `action` | Events written                    |
| `ledger_events_gc_entries_deleted_total`  | Counter   |                               | Events deleted by GC              |
| `ledger_events_gc_cycle_duration_seconds` | Histogram |                               | GC cycle duration                 |
| `ledger_events_gc_cycles_total`           | Counter   | `result`                      | GC cycles (success/error/skipped) |

### Grafana Queries

```promql
# Event write rate by emission path
rate(ledger_event_writes_total[5m])

# GC deletion rate
rate(ledger_events_gc_entries_deleted_total[5m])

# GC cycle latency p99
histogram_quantile(0.99, rate(ledger_events_gc_cycle_duration_seconds_bucket[5m]))
```

### Alert Recommendations

| Alert                 | Condition                                                         | Severity |
| --------------------- | ----------------------------------------------------------------- | -------- |
| Events GC failing     | `rate(ledger_events_gc_cycles_total{result="error"}[15m]) > 0`    | Warning  |
| Events GC not running | `time() - ledger_events_gc_cycles_total > 600`                    | Warning  |
| High denial rate      | `rate(ledger_event_writes_total{action=~"request_.*"}[5m]) > 100` | Info     |

## Retention

- **Default TTL**: 90 days (configurable 1–3650)
- **GC interval**: Every 5 minutes (300s)
- **GC batch size**: Up to 5,000 entries per cycle
- **GC runs on all nodes** (not leader-only) — events.db is local

### Storage Growth Estimation

Typical event entry: ~200 bytes (postcard encoded). At 100 events/second with 90-day retention:

```
100 events/sec × 86,400 sec/day × 90 days × 200 bytes ≈ 155 GB
```

For most workloads (1–10 events/second), expect 1–15 GB.

## Troubleshooting

### GC Lag

**Symptom**: `events.db` grows beyond expected size.

**Cause**: GC interval too long or batch size too small for event volume.

**Fix**: Reduce TTL or check `ledger_events_gc_cycle_duration_seconds` for slow cycles.

### Missing Handler-Phase Events After Failover

**Symptom**: Some denial events disappear after leader election.

**Cause**: Handler-phase events are node-local. When a new leader is elected, its `events.db` does not contain handler-phase events from the old leader.

**Fix**: This is expected behavior. For compliance, rely on apply-phase events (replicated) or external log aggregation (canonical log lines).

### Storage Contention

**Symptom**: High write latency on state.db during event writes.

**Cause**: Events use a separate `events.db` file specifically to avoid this. If observed, check for I/O contention at the filesystem level.
