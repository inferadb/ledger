# Rolling Upgrade Playbook

Scheduled procedure for upgrading Ledger to a new version. This is planned work, not incident response — run during a maintenance window with stakeholder notification.

> **Pre-GA Disclaimer**: InferaDB Ledger is pre-1.0 software. Until 1.0, upgrades between minor versions require a full cluster wipe and restore. Rolling upgrades (zero-downtime, node-by-node) will be supported starting with 1.0 stable releases.

## Purpose

Move the cluster from one released version to another. Until 1.0 this requires a full wipe + restore from backup; post-1.0 this becomes a true rolling upgrade (node-by-node, zero-downtime).

- **When to run**: during a scheduled maintenance window after validating the new version in staging.
- **Expected duration**: 30–90 minutes depending on cluster size and backup size.
- **Who runs it**: SRE with backup-operator access and cluster-admin authority.

## Preconditions

- New version validated in staging against the current production workload shape.
- Recent backup taken and verified via the [backup-verification playbook](backup-verification.md) — the upgrade restores from this backup.
- Maintenance window communicated to stakeholders; write traffic paused or rerouted.
- Authority to stop, wipe, and restart every node in the cluster.
- Rollback path identified (prior-version binary available, prior-version backup retained).

## Steps

1. Announce the maintenance window; pause writes per your traffic-management policy.
2. Take a final pre-upgrade backup (in addition to the recent verified one) and confirm its checksum.
3. Stop every node; confirm all are down.
4. Wipe each node's `data_dir` (or rename aside for forensic preservation).
5. Install the new binary / deploy the new container image on every node.
6. On node 1: restore the backup into `data_dir` and start the node.
7. Run `inferadb-ledger init --host node1:50051` to re-bootstrap the cluster with restored data.
8. Start nodes 2 and 3 with `--join node1:50051`; confirm they catch up.
9. Run the verification steps below before resuming write traffic.

Exact commands live in [Deep reference](#restore-strategy-pre-ga) and the existing step-by-step content below.

## Verification

- `AdminService.GetClusterInfo` shows all three nodes as voters.
- `ReadService.GetTip` for a sampled set of vaults returns block heights equal to pre-upgrade heights (recorded in step 1 of the deep reference).
- `AdminService.CheckIntegrity` passes on sampled vaults.
- No new entries in `ledger_determinism_bug_total`.

## Rollback

If verification fails at any step:

1. Stop all nodes.
2. Wipe `data_dir` again.
3. Install the **prior** version binary.
4. Restore from the same backup using the same steps.
5. Re-bootstrap and verify the cluster is back to the pre-upgrade state.

Post-1.0 rolling upgrades will have true per-node rollback; for pre-GA upgrades the rollback path is another full-restore cycle.

## Escalation

- Restore verification shows block heights below pre-upgrade values: **stop immediately** — you may be restoring from a stale backup. Verify backup freshness before any further action; page the data-integrity owner.
- Any `ledger_determinism_bug_total` increment after upgrade: stop writes, page the consensus owner — version-to-version determinism broke.

## Deep reference

### Version Compatibility (Pre-GA)

| From Version | To Version | Upgrade Path      | Notes                                 |
| ------------ | ---------- | ----------------- | ------------------------------------- |
| 0.x          | 0.y        | Full cluster wipe | Schema/format changes between minors  |
| 0.x.y        | 0.x.z      | Full cluster wipe | Even patch versions may change format |

**Pre-GA constraint**: The on-disk format (B+ tree page layout, snapshot binary format, Raft log encoding) is not yet stable. Any version bump may change internal formats. In-place upgrades risk data corruption.

### Mixed-Version Cluster Behavior

Running nodes on different binary versions in the same Raft group is **not supported** and causes snapshot transfer failures.

**Root cause**: The `SnapshotData` type changed from `Cursor<Vec<u8>>` (in-memory blob) to `tokio::fs::File` (file-based streaming). This is a compile-time type change in the `declare_raft_types!` macro — the snapshot wire format is incompatible between the two representations.

**What happens if attempted**:

1. Leader and follower negotiate a snapshot transfer via the consensus engine's `install_snapshot` path (custom in-house implementation in `crates/raft/src/log_storage/raft_impl.rs`, streaming zstd-compressed snapshots via `crates/raft/src/snapshot.rs`).
2. The sender serializes snapshot chunks using the new file-based streaming format (zstd-compressed, SHA-256 verified).
3. The receiver expects the old in-memory blob format and fails to decode the chunks.
4. The follower cannot catch up — it will retry indefinitely, never joining the cluster.
5. If a majority of nodes are on the old version, the new-version leader cannot replicate to a quorum.

**Additional incompatibilities**:

- `AppliedStateCore` gained a `last_applied_timestamp_ns` field. Old nodes cannot deserialize the new format (postcard is not self-describing — field count mismatch causes decode failure).
- Snapshot event collection uses per-organization range scans with timestamp cutoff. Old nodes use full-table scan with sort-then-truncate — the event sets may differ for the same logical state.

**Required action**: Always use the full cluster wipe procedure below. Do not attempt to upgrade one node at a time.

## Prerequisites

- [ ] Cluster is healthy (all nodes reporting, leader elected)
- [ ] Backup created and verified within 1 hour (see [Backup Verification](backup-verification.md))
- [ ] New version tested in staging environment
- [ ] Maintenance window scheduled (cluster unavailable during upgrade)
- [ ] All clients notified of downtime
- [ ] Raft log compacted on all nodes (see [Snapshot Compaction](#snapshot-compaction) below)

## Snapshot Compaction

**Required when upgrading across versions that remove `SystemRequest` or `LedgerRequest` variants** (check CHANGELOG.md for "Removed ... Raft log variant" entries). Uncompacted log entries with removed variant discriminants will fail postcard deserialization, blocking node startup.

### When is this required?

This is required when the CHANGELOG lists removal of any Raft log entry variant. The postcard serialization format uses variant indices — removing a variant shifts indices and breaks deserialization of old log entries.

### Procedure

Before deploying the new binary, trigger a snapshot on the leader and wait for all followers to replicate:

```bash
# Trigger snapshot on leader (compacts Raft WAL up to current applied index)
grpcurl -plaintext leader:50051 ledger.v1.AdminService/TriggerSnapshot

# Wait for all nodes to install the snapshot (check applied_index matches)
for node in node1 node2 node3; do
  grpcurl -plaintext $node:50051 ledger.v1.AdminService/GetClusterInfo \
    | jq '.nodes[] | {id, applied_index, snapshot_index}'
done
```

Verify that `snapshot_index >= applied_index` on all nodes before proceeding with the upgrade. This ensures no uncompacted log entries with removed variants remain.

## Pre-Upgrade Checks

### 1. Verify Cluster Health

```bash
# Check all nodes are healthy
for node in node1 node2 node3; do
  grpcurl -plaintext $node:50051 ledger.v1.HealthService/Check
done

# Verify leader exists
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo
```

Expected: All nodes return `SERVING`, one `leader_id` is set.

### 2. Create Fresh Backup

```bash
# Trigger backup on leader
grpcurl -plaintext leader:50051 ledger.v1.AdminService/CreateBackup

# Verify backup exists
grpcurl -plaintext leader:50051 ledger.v1.AdminService/ListBackups
```

### 3. Verify Backup Integrity

Follow the [Weekly Restore Test](backup-verification.md#weekly-restore-test) procedure to confirm the backup is restorable.

### 4. Record Current State

```bash
# Save cluster state for post-upgrade comparison
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo > pre-upgrade-cluster.json

# Record vault heights
for vault in $(grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults | jq -r '.vaults[].vault.slug'); do
  echo "Vault $vault:"
  grpcurl -plaintext \
    -d "{\"organization\": {\"slug\": 1234567890}, \"vault\": {\"slug\": $vault}}" \
    localhost:50051 ledger.v1.ReadService/GetTip | jq '.height'
done > pre-upgrade-heights.txt
```

## Upgrade Procedure (Full Cluster Wipe)

### Step 1: Stop All Nodes

Stop all cluster nodes simultaneously. Order does not matter since the cluster will be rebuilt.

```bash
# Kubernetes
kubectl scale statefulset ledger --replicas=0

# Or systemd (on each node)
systemctl stop ledger
```

### Step 2: Delete Data Directories

Remove all on-disk state from every node. The backup created in pre-upgrade will be the restore source.

```bash
# On each node
rm -rf /var/lib/ledger/raft/
rm -rf /var/lib/ledger/state/
# Keep node_id file if you want nodes to retain their identities
# rm /var/lib/ledger/node_id  # Only if node IDs must change
```

> **Warning**: This deletes all Raft logs, state databases, and snapshots. Ensure your backup is verified before proceeding.

### Step 3: Deploy New Binary

```bash
# Kubernetes
kubectl set image statefulset/ledger ledger=inferadb/ledger:v0.NEW.0

# Or systemd (on each node)
# Install new binary, then:
systemctl daemon-reload
```

### Step 4: Start First Node

Start one node and bootstrap it as a single-node cluster to perform the restore:

```bash
# Start the node
inferadb-ledger --listen 0.0.0.0:50051 --data /var/lib/ledger &

# Bootstrap it as a single-node cluster
inferadb-ledger init --host localhost:50051
```

### Step 5: Restore from Backup

```bash
# Restore the pre-upgrade backup
grpcurl -plaintext localhost:50051 \
  -d '{"backup_id": "BACKUP_ID_FROM_STEP_2"}' \
  ledger.v1.AdminService/RestoreBackup
```

### Step 6: Verify Restored State

```bash
# Check organizations
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListOrganizations

# Verify vault heights match pre-upgrade
for vault in $(grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults | jq -r '.vaults[].vault.slug'); do
  echo "Vault $vault:"
  grpcurl -plaintext \
    -d "{\"organization\": {\"slug\": 1234567890}, \"vault\": {\"slug\": $vault}}" \
    localhost:50051 ledger.v1.ReadService/GetTip | jq '.height'
done

# Run integrity checks
for vault in $(grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults | jq -r '.vaults[].vault.slug'); do
  grpcurl -plaintext \
    -d "{\"organization\": {\"slug\": 1234567890}, \"vault\": {\"slug\": $vault}, \"full_check\": true}" \
    localhost:50051 ledger.v1.AdminService/CheckIntegrity
done
```

### Step 7: Expand to Full Cluster

Stop the single-node instance and restart as a cluster:

```bash
# Stop single-node mode
systemctl stop ledger

# Start all nodes in cluster mode
# Node 1 (has restored data):
INFERADB__LEDGER__DATA=/var/lib/ledger \
INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
INFERADB__LEDGER__JOIN=node1:50051,node2:50051,node3:50051 \
inferadb-ledger

# Nodes 2 and 3 (empty, will sync from node 1):
INFERADB__LEDGER__DATA=/var/lib/ledger \
INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
INFERADB__LEDGER__JOIN=node1:50051 \
inferadb-ledger
```

### Step 8: Verify Cluster Health

```bash
# Wait for all nodes to join
watch -n1 'grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo | jq ".members | length"'

# Verify leader elected
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo

# Run functional test
grpcurl -plaintext \
  -d '{"organization": {"slug": 1234567890}, "client_id": {"id": "upgrade-test"}, "idempotency_key": "AAAAAAAAAAAAAAAAAAAAAA==", "operations": [{"set_entity": {"key": "test:upgrade", "value": "dGVzdA=="}}]}' \
  localhost:50051 ledger.v1.WriteService/Write
```

## Post-Upgrade Verification

### 1. Compare State

```bash
# Compare vault heights to pre-upgrade
diff pre-upgrade-heights.txt <(for vault in $(grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults | jq -r '.vaults[].vault.slug'); do
  echo "Vault $vault:"
  grpcurl -plaintext \
    -d "{\"organization\": {\"slug\": 1234567890}, \"vault\": {\"slug\": $vault}}" \
    localhost:50051 ledger.v1.ReadService/GetTip | jq '.height'
done)
```

### 2. Metrics Comparison

```bash
curl -s localhost:9090/metrics | grep -E '(proposals_pending|write_latency|is_leader)'
```

### 3. Re-enable Traffic

```bash
# Kubernetes
kubectl label pods -l app=ledger ledger.inferadb.com/ready=true --overwrite
```

## Rollback Procedure

If the new version has issues after restore:

1. Stop all nodes
2. Delete data directories again
3. Deploy the **previous** binary version
4. Restore from the same pre-upgrade backup
5. Expand to full cluster

## Timing Guidelines

| Cluster Size | Total Upgrade Time | Downtime Window |
| ------------ | ------------------ | --------------- |
| 3 nodes      | 15-30 minutes      | 15-30 minutes   |
| 5 nodes      | 20-40 minutes      | 20-40 minutes   |
| 7 nodes      | 25-45 minutes      | 25-45 minutes   |

Most time is spent on backup verification and post-restore integrity checks, not the wipe/restart itself.

## Future: Rolling Upgrades (Post-1.0)

After the 1.0 stable release, Ledger will support zero-downtime rolling upgrades between compatible versions:

- Patch versions (1.0.x → 1.0.y): always rolling-upgrade compatible
- Minor versions (1.x → 1.y): rolling-upgrade compatible within one minor version
- Major versions: may require full cluster wipe (documented per release)

This runbook will be updated with rolling upgrade procedures when 1.0 stabilizes the on-disk format.

## Checklist Summary

Pre-upgrade:

- [ ] All nodes healthy
- [ ] Fresh backup created and verified
- [ ] Maintenance window scheduled
- [ ] Clients notified

Upgrade:

- [ ] All nodes stopped
- [ ] Data directories deleted
- [ ] New binary deployed
- [ ] First node started in single-node mode
- [ ] Backup restored
- [ ] Restored state verified
- [ ] Cluster expanded
- [ ] Cluster health verified

Post-upgrade:

- [ ] Vault heights match pre-upgrade
- [ ] Integrity checks pass
- [ ] Write operations succeed
- [ ] Metrics normal
- [ ] Traffic re-enabled
