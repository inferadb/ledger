# Troubleshooting

Common issues and their solutions.

## Quick Diagnostics

### Check Node Health

```bash
grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check
```

| Status             | Meaning                                |
| ------------------ | -------------------------------------- |
| `SERVING`          | Node is healthy and accepting requests |
| `NOT_SERVING`      | Node is unhealthy (check logs)         |
| Connection refused | Node is down or port blocked           |

### Check Cluster State

```bash
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetClusterInfo
```

Look for:

- `leader_id`: Should be set (non-empty)
- `members`: Should list all expected nodes
- `commit_index`: Should be advancing

### Check Vault Health

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
  localhost:50051 ledger.v1.ReadService/GetTip
```

## Common Issues

### Write Operations Failing

#### Symptom: `UNAVAILABLE` error on writes

**Cause**: No leader elected (cluster has no quorum)

**Diagnosis**:

```bash
# Check if this node thinks it's leader
curl -s localhost:9090/metrics | grep inferadb_ledger_raft_is_leader
# 0 = follower, 1 = leader

# Check pending proposals (should be low)
curl -s localhost:9090/metrics | grep inferadb_ledger_raft_proposals_pending
```

**Solutions**:

1. Ensure majority of nodes are running (2/3, 3/5, etc.)
2. Check network connectivity between nodes
3. Wait for leader election (typically <5 seconds)

#### Symptom: `SEQUENCE_GAP` error

**Cause**: Client sent wrong sequence number

**Diagnosis**:

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "client_id": {"id": "my-client"}}' \
  localhost:50051 ledger.v1.ReadService/GetClientState
```

**Solution**: Resume from `last_committed_sequence + 1`

#### Symptom: Writes are slow (>50ms p99)

**Diagnosis**:

```bash
# Check Raft apply latency
curl -s localhost:9090/metrics | grep inferadb_ledger_raft_apply_latency_seconds

# Check pending proposals (backlog)
curl -s localhost:9090/metrics | grep inferadb_ledger_raft_proposals_pending

# Check batch flush latency
curl -s localhost:9090/metrics | grep ledger_batch_flush_latency_seconds
```

**Solutions**:

1. If `proposals_pending` > 50: Network latency or disk I/O bottleneck
2. If `apply_latency` high: Disk fsync performance
3. Tune batching via `INFERADB__LEDGER__BATCH_SIZE` and `INFERADB__LEDGER__BATCH_DELAY` environment variables

### Read Operations Failing

#### Symptom: `NOT_FOUND` for organization or vault

**Cause**: Resource doesn't exist or wrong ID

**Solution**:

```bash
# List all organizations
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListOrganizations

# List vaults in organization
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/ListVaults
```

#### Symptom: `FAILED_PRECONDITION` with `HEIGHT_UNAVAILABLE`

**Cause**: Requested historical height has been pruned

**Solution**: Use a more recent height or current state:

```bash
# Get current tip
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
  localhost:50051 ledger.v1.ReadService/GetTip
```

### Cluster Issues

#### Symptom: Leader keeps changing (flapping)

**Diagnosis**:

```bash
# Watch leader changes
watch -n1 'curl -s localhost:9090/metrics | grep inferadb_ledger_raft_term'
```

**Causes & Solutions**:

1. **Network partition**: Check connectivity between all nodes
2. **Clock skew**: Synchronize clocks (NTP)
3. **Resource starvation**: Check CPU/memory/disk on leader
4. **Asymmetric firewall**: Ensure bidirectional traffic

#### Symptom: Node won't join cluster

**Diagnosis**:

```bash
# Check node logs for bootstrap errors
journalctl -u ledger -f

# Verify peer discovery
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetNodeInfo
```

**Solutions**:

1. Verify `--peers` DNS resolves correctly
2. Check network connectivity to seed nodes
3. Ensure consistent cluster configuration

### Vault Divergence

#### Symptom: Vault returns `VAULT_UNAVAILABLE`

**Diagnosis**:

```bash
# Check vault health
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
  localhost:50051 ledger.v1.ReadService/GetTip

# Check metrics
curl -s localhost:9090/metrics | grep ledger_determinism_bug_total
```

**Solution**: See [Vault Repair](vault-repair.md) runbook

### Snapshot Issues

#### Symptom: Snapshot creation failing

**Diagnosis**:

```bash
curl -s localhost:9090/metrics | grep ledger_snapshots_created_total
curl -s localhost:9090/metrics | grep ledger_snapshot_create_latency_seconds
```

**Solutions**:

1. Check disk space: `df -h /var/lib/ledger`
2. Check for I/O errors: `dmesg | grep -i error`
3. Manually trigger snapshot to see error:
   ```bash
   grpcurl -plaintext localhost:50051 ledger.v1.AdminService/CreateSnapshot
   ```

## Debug Logging

Enable verbose logging:

```bash
# Full debug logging
RUST_LOG=debug inferadb-ledger --single

# Module-specific logging
RUST_LOG=inferadb_ledger_raft=debug,inferadb_ledger_state=info inferadb-ledger

# Raft consensus debugging
RUST_LOG=openraft=debug inferadb-ledger
```

### Log Patterns

| Pattern                   | Meaning                                    |
| ------------------------- | ------------------------------------------ |
| `state_root_divergence`   | Vault has determinism bug                  |
| `leader_election`         | Leadership change                          |
| `snapshot_created`        | Successful snapshot                        |
| `append_entries_rejected` | Raft log conflict (normal during recovery) |

## Metrics Cheatsheet

### Health Indicators

| Metric                                   | Healthy | Warning | Critical |
| ---------------------------------------- | ------- | ------- | -------- |
| `inferadb_ledger_raft_proposals_pending` | <10     | 10-50   | >50      |
| `inferadb_ledger_raft_is_leader`         | 0 or 1  | -       | All 0s   |
| `ledger_active_connections`              | <100    | 100-500 | >500     |
| `ledger_determinism_bug_total`           | 0       | -       | >0       |

### Performance Baselines

| Metric                                       | Expected p50 | Expected p99 |
| -------------------------------------------- | ------------ | ------------ |
| `ledger_write_latency_seconds`               | <10ms        | <50ms        |
| `ledger_read_latency_seconds`                | <1ms         | <5ms         |
| `inferadb_ledger_raft_apply_latency_seconds` | <5ms         | <20ms        |
| `inferadb_ledger_state_root_latency_seconds` | <1ms         | <5ms         |

## Getting Help

If issues persist:

1. Collect diagnostics:

   ```bash
   # Export all metrics
   curl localhost:9090/metrics > metrics.txt

   # Export recent logs
   journalctl -u ledger --since "1 hour ago" > logs.txt

   # Get cluster state
   grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetClusterInfo > cluster.json
   ```

2. Check for known issues in release notes

3. File an issue with diagnostics attached
