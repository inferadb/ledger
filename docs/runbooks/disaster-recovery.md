# Disaster Recovery

On-call runbook for catastrophic failures: node failures, quorum loss, total cluster loss, data corruption, or regional failure. For specific narrow-scope failures (quorum loss with surviving minority, snapshot restore failing on a catching-up node) see the dedicated runbooks listed under [Escalation](#escalation); use this runbook as the parent entry point.

## Symptom

- Cluster returns `UNAVAILABLE` on all write paths simultaneously (quorum loss).
- Multiple nodes unreachable; `ledger_cluster_quorum_status` = 0.
- Widespread state corruption indicated by `ledger_determinism_bug_total` or `ledger_state_root_divergences_total` across replicas.
- An entire region's nodes are unreachable (regional failure).

## Alert / Trigger

- `LedgerClusterQuorumLost` — `ledger_cluster_quorum_status == 0` for > 2 minutes.
- Cluster-wide `VaultUnavailable` alerts firing across many vaults (coordinated failure).
- External monitoring (pingdom / synthetic probes) reporting total outage.

## Blast radius

- **Scope**: by scenario — see the Failure Scenarios table below. Ranges from a single node (no user impact, automatic recovery) to total cluster loss (full outage).
- **Downstream impact**: application-visible write and read unavailability for affected regions / vaults. Cryptographic chain integrity is preserved in all recovery scenarios — no block is lost once committed.

## Preconditions

- Incident commander designated and incident response posture active.
- Authority to perform destructive operations (full-cluster wipe + restore from backup requires this).
- Recent backup available and verified per [backup-verification playbook](../playbooks/backup-verification.md).
- Access to `AdminService` on surviving nodes.
- DNS / load-balancer control if regional failover is needed.

## Steps

The recovery steps are scenario-specific. Start by classifying the failure using the [Failure Scenarios](#failure-scenarios) table below, then jump to the named procedure. Do **not** execute procedures outside of their classified scenario — in particular, do **not** run the quorum-loss re-bootstrap procedure while other nodes of the original cluster may rejoin (split-brain risk, called out explicitly in [Quorum Recovery](#quorum-recovery)).

## Verification

- `ledger_cluster_quorum_status` returns to 1.
- All restored nodes report healthy via `HealthService.Check`.
- Write-path smoke test succeeds: create a test vault, write an entity, read it back.
- Block heights on all vaults match pre-incident values (verify via `ReadService.GetTip`).
- `ledger_state_root_divergences_total` stable (not incrementing).

## Rollback

DR procedures are themselves the rollback path — you cannot "rollback" a disaster recovery. What you **can** do: before any destructive step (full-cluster wipe, force-new-cluster via re-init), snapshot the current `data_dir` to a parallel location so forensic analysis remains possible after the cluster is restored. This is called out in the relevant procedures below.

## Escalation

- Beyond the recovery-time targets in the Failure Scenarios table: escalate to engineering leadership.
- If restored cluster shows `ledger_determinism_bug_total > 0` on first writes: stop writes and page the consensus owner — a determinism bug on a fresh cluster indicates a code-level issue, not a DR execution issue.
- Specific failure modes with dedicated runbooks:
  - Single vault diverged but cluster otherwise healthy → [vault-repair.md](vault-repair.md)
  - Snapshot fails to install on a catching-up node → [snapshot-restore-failure.md](snapshot-restore-failure.md)
  - Quorum loss with minority surviving → [quorum-loss.md](quorum-loss.md) (narrower than the Quorum Recovery procedure in this runbook; choose the one that matches your exact posture)
  - PII erasure failing mid-procedure → [pii-erasure-failure.md](pii-erasure-failure.md)
  - Encryption key rotation stalled → [key-rotation-failure.md](key-rotation-failure.md)

## Deep reference

### Failure Scenarios

| Scenario                        | Impact                      | RTO       | Procedure                                        |
| ------------------------------- | --------------------------- | --------- | ------------------------------------------------ |
| Single node failure             | None (if quorum maintained) | Automatic | [Node Recovery](#node-recovery)                  |
| Quorum loss (minority survives) | Write unavailable           | 5-15 min  | [Quorum Recovery](#quorum-recovery)              |
| Total cluster loss              | Full outage                 | 15-30 min | [Full Restore](#full-cluster-restore)            |
| Data corruption                 | Partial/full                | 30-60 min | [Corruption Recovery](#data-corruption-recovery) |
| Region failure                  | Regional outage             | 15-30 min | [Regional Failover](#regional-failover)          |

## Node Recovery

### Single Node Failure (Quorum Maintained)

When one node fails but majority remains:

1. **Cluster auto-recovers** - No immediate action needed
2. **Replace failed node**:

```bash
# Remove failed node from cluster
grpcurl -plaintext healthy-node:50051 \
  -d '{"node_id": {"id": "FAILED_NODE_ID"}}' \
  ledger.v1.AdminService/LeaveCluster

# Start replacement node
INFERADB__LEDGER__DATA=/data \
INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
INFERADB__LEDGER__JOIN=healthy-node:50051 \
inferadb-ledger
```

3. **Verify recovery**:

```bash
grpcurl -plaintext healthy-node:50051 ledger.v1.AdminService/GetClusterInfo
```

## Quorum Recovery

### Minority of Nodes Survive

When less than majority survives (e.g., 1 of 3 nodes):

1. **Stop surviving nodes**:

```bash
systemctl stop ledger
```

2. **Force new cluster from surviving node**:

> **⚠️ WARNING: Split-Brain Risk**
>
> Starting a node and running `init` against it creates a NEW cluster from that node's data, abandoning the old cluster identity. If other nodes from the old cluster come back online, you will have TWO separate clusters with potentially diverged data.
>
> **Only do this when you are CERTAIN the old cluster is permanently destroyed** or all other nodes' data will be wiped before rejoining.

```bash
# Start the surviving node
inferadb-ledger --listen 0.0.0.0:50051 --data /data &

# Re-bootstrap it as a new single-node cluster
inferadb-ledger init --host localhost:50051
```

3. **Verify data**:

```bash
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults

# Run integrity check
for vault in $(grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults | jq -r '.vaults[].vault.slug'); do
  grpcurl -plaintext \
    -d "{\"organization\": {\"slug\": 1234567890}, \"vault\": {\"slug\": $vault}, \"full_check\": true}" \
    localhost:50051 ledger.v1.AdminService/CheckIntegrity
done
```

4. **Scale back to cluster**:

```bash
# Stop single-node mode
systemctl stop ledger

# Restart as cluster with new nodes (each node runs with --join pointing
# at the seed addresses of the other members)
INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
INFERADB__LEDGER__JOIN=node1:50051,node2:50051,node3:50051 \
inferadb-ledger
```

## Full Cluster Restore

### Total Loss - Restore from Backup

When all nodes and data are lost:

1. **Retrieve latest backup**:

```bash
RESTORE_DIR=/data/restore
mkdir -p $RESTORE_DIR

# From S3
aws s3 cp s3://ledger-backups/latest/snapshot.tar.gz $RESTORE_DIR/
tar -xzf $RESTORE_DIR/snapshot.tar.gz -C $RESTORE_DIR
```

2. **Start first node with restored data**:

> **⚠️ WARNING: New Cluster Identity**
>
> Starting a node against restored data and running `init` creates a NEW cluster. This is appropriate here because you're restoring from backup after total loss. Do NOT do this if any nodes from the original cluster might still have data.

```bash
inferadb-ledger --listen 0.0.0.0:50051 --data $RESTORE_DIR &
inferadb-ledger init --host localhost:50051
```

3. **Verify restored state**:

```bash
# Check organizations
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListOrganizations

# Check vaults and run integrity
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults

# Verify block heights
for vault in $(grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults | jq -r '.vaults[].vault.slug'); do
  echo "Vault $vault:"
  grpcurl -plaintext \
    -d "{\"organization\": {\"slug\": 1234567890}, \"vault\": {\"slug\": $vault}}" \
    localhost:50051 ledger.v1.ReadService/GetTip | jq '.height'
done
```

4. **Expand to full cluster**:

```bash
# Stop single-node
systemctl stop ledger

# Clear restored data on new nodes
rm -rf /data/*

# Start cluster (node1 has restored data)
# Node 1:
INFERADB__LEDGER__DATA=$RESTORE_DIR \
INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
INFERADB__LEDGER__JOIN=node1:50051,node2:50051,node3:50051 \
inferadb-ledger

# Nodes 2 and 3:
INFERADB__LEDGER__DATA=/data \
INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
INFERADB__LEDGER__JOIN=node1:50051 \
inferadb-ledger
```

5. **Verify cluster health**:

```bash
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo
```

## Data Corruption Recovery

### Vault State Divergence

When `ledger_determinism_bug_total > 0`:

1. **Identify affected vault**:

```bash
curl -s localhost:9090/metrics | grep determinism_bug
```

2. **Check vault status**:

```bash
grpcurl -plaintext \
  -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
  localhost:50051 ledger.v1.AdminService/GetVault
```

Status will be `DIVERGED`. The auto-recovery background job will attempt recovery automatically with exponential backoff.

3. **Attempt manual recovery** (if auto-recovery has not resolved it):

```bash
grpcurl -plaintext \
  -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
  localhost:50051 ledger.v1.AdminService/RecoverVault
```

4. **If recovery fails, restore from healthy replica**:

```bash
# Find healthy node
for node in node1 node2 node3; do
  echo "$node:"
  grpcurl -plaintext $node:50051 \
    -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
    ledger.v1.AdminService/CheckIntegrity
done

# Snapshot from healthy node
grpcurl -plaintext healthy-node:50051 \
  -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
  ledger.v1.AdminService/CreateSnapshot

# Remove corrupted node, let it resync from snapshot
```

**Note on snapshots**: Snapshots are zstd-compressed binary files containing `AppliedStateCore` plus 12 externalized B+ tree tables, entity data, and event data. A SHA-256 checksum footer over the compressed bytes is verified before any decompression. Snapshot installation writes all state into a single `WriteTransaction` — either the entire install succeeds atomically or no state changes are visible.

### Table-Level Inconsistency

If the integrity scrubber detects corruption in specific externalized tables (e.g., `VaultHeights`, `ClientSequences`) while the core `AppliedStateCore` is intact, the affected region can be recovered by installing a snapshot from a healthy replica. The snapshot will overwrite all 12 externalized tables atomically via a single `WriteTransaction`.

## Regional Failover

### Primary Region Unavailable

1. **Detect failure**:

```bash
# Automated via monitoring
# Alert: LedgerRegionDown fires
```

2. **Activate DR cluster**:

```bash
# Start DR cluster with replicated snapshots
cd /dr-region
aws s3 sync s3://ledger-primary-backups/latest/ ./data/

INFERADB__LEDGER__DATA=./data \
INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
INFERADB__LEDGER__JOIN=dr-node1:50051,dr-node2:50051,dr-node3:50051 \
inferadb-ledger &

# After all three DR nodes are up, re-bootstrap as a new cluster:
inferadb-ledger init --host dr-node1:50051
```

3. **Update DNS/routing**:

```bash
# Update Control to point to DR region
kubectl patch configmap ledger-config \
  -p '{"data":{"LEDGER_ENDPOINT":"ledger.dr-region.internal:50051"}}'

# Restart Engine pods
kubectl rollout restart deployment/engine
```

4. **Verify DR cluster**:

```bash
grpcurl -plaintext dr-ledger:50051 ledger.v1.HealthService/Check
grpcurl -plaintext dr-ledger:50051 ledger.v1.AdminService/GetClusterInfo
```

5. **Accept data loss window**:

Data committed after last backup replication is lost. Document the RPO gap.

## Post-Recovery Checklist

After any recovery:

- [ ] All nodes healthy and reporting
- [ ] Leader elected
- [ ] All vaults accessible
- [ ] Integrity checks pass
- [ ] Write operations succeed
- [ ] Read operations succeed
- [ ] Metrics flowing
- [ ] Alerts resolved
- [ ] Incident documented

## Recovery Time Objectives

| Scenario            | Target RTO | Max RTO |
| ------------------- | ---------- | ------- |
| Single node         | Automatic  | 5 min   |
| Quorum loss         | 10 min     | 15 min  |
| Full restore        | 20 min     | 30 min  |
| Regional failover   | 15 min     | 30 min  |
| Corruption recovery | 30 min     | 60 min  |

## Emergency Contacts

Escalation path for production incidents:

1. On-call engineer (PagerDuty)
2. Platform team lead
3. Infrastructure director

## Related Runbooks

- [Backup Verification](backup-verification.md) - Ensure backups are restorable
- [Upgrade Runbook](rolling-upgrade.md) - Version upgrade procedures
- [Troubleshooting](../how-to/troubleshooting.md) - Common issues
