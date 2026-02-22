# Disaster Recovery Runbook

Procedures for recovering from catastrophic failures.

## Failure Scenarios

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
INFERADB__LEDGER__JOIN=true \
INFERADB__LEDGER__PEERS=healthy-node:50051 \
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
> Using `--single` creates a NEW cluster from this node's data, abandoning the old cluster identity. If other nodes from the old cluster come back online, you will have TWO separate clusters with potentially diverged data.
>
> **Only use `--single` when you are CERTAIN the old cluster is permanently destroyed** or all other nodes' data will be wiped before rejoining.

```bash
# Start single node as new cluster
inferadb-ledger --single --data /data
```

3. **Verify data**:

```bash
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults

# Run integrity check
for vault in $(grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults | jq -r '.vaults[].vault.slug'); do
  grpcurl -plaintext \
    -d "{\"organization_slug\": {\"id\": \"1\"}, \"vault\": {\"slug\": \"$vault\"}, \"full_check\": true}" \
    localhost:50051 ledger.v1.AdminService/CheckIntegrity
done
```

4. **Scale back to cluster**:

```bash
# Stop single-node mode
systemctl stop ledger

# Restart as cluster with new nodes
INFERADB__LEDGER__CLUSTER=3 \
INFERADB__LEDGER__PEERS=node1,node2,node3 \
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
> Using `--single` creates a NEW cluster. This is appropriate here because you're restoring from backup after total loss. Do NOT use this if any nodes from the original cluster might still have data.

```bash
inferadb-ledger --single --data $RESTORE_DIR
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
    -d "{\"organization_slug\": {\"id\": \"1\"}, \"vault\": {\"slug\": \"$vault\"}}" \
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
INFERADB__LEDGER__CLUSTER=3 \
INFERADB__LEDGER__PEERS=node1,node2,node3 \
inferadb-ledger

# Nodes 2 and 3:
INFERADB__LEDGER__DATA=/data \
INFERADB__LEDGER__JOIN=true \
INFERADB__LEDGER__PEERS=node1:50051 \
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
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "AFFECTED_VAULT_SLUG"}}' \
  localhost:50051 ledger.v1.AdminService/GetVault
```

Status will be `DIVERGED`.

3. **Attempt automatic recovery**:

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "AFFECTED_VAULT_SLUG"}}' \
  localhost:50051 ledger.v1.AdminService/RecoverVault
```

4. **If recovery fails, restore from healthy replica**:

```bash
# Find healthy node
for node in node1 node2 node3; do
  echo "$node:"
  grpcurl -plaintext $node:50051 \
    -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "AFFECTED_VAULT_SLUG"}}' \
    ledger.v1.AdminService/CheckIntegrity
done

# Snapshot from healthy node
grpcurl -plaintext healthy-node:50051 \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "AFFECTED_VAULT_SLUG"}}' \
  ledger.v1.AdminService/CreateSnapshot

# Remove corrupted node, let it resync from snapshot
```

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
INFERADB__LEDGER__CLUSTER=3 \
inferadb-ledger
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
- [Rolling Upgrade](rolling-upgrade.md) - Upgrade without downtime
- [Troubleshooting](../../troubleshooting.md) - Common issues
