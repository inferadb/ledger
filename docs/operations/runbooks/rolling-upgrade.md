# Rolling Upgrade Runbook

Procedure for upgrading Ledger nodes with zero downtime.

## Version Compatibility

Ledger supports rolling upgrades between adjacent minor versions. Major version upgrades may require additional steps.

| From Version | To Version | Upgrade Path   | Notes                             |
| ------------ | ---------- | -------------- | --------------------------------- |
| 1.0.x        | 1.0.y      | Direct rolling | Patch versions always compatible  |
| 1.x          | 1.y        | Direct rolling | Minor versions forward-compatible |
| 0.x          | 1.0        | Stop-start     | Breaking proto changes            |

**Compatibility rules:**

1. Nodes can replicate across one minor version difference
2. Patch version upgrades are always safe
3. All nodes must be upgraded within 24 hours
4. Never skip minor versions (1.0 → 1.2 requires 1.0 → 1.1 → 1.2)

## Prerequisites

- [ ] Cluster is healthy (all nodes reporting, leader elected)
- [ ] No ongoing migrations or recoveries
- [ ] Backup verification completed within 24 hours
- [ ] New version tested in staging environment
- [ ] Rollback plan reviewed

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

### 2. Check Proposal Backlog

```bash
curl -s localhost:9090/metrics | grep inferadb_ledger_raft_proposals_pending
```

Expected: Value < 10. If higher, wait for backlog to clear.

### 3. Record Current State

```bash
# Save cluster state for comparison
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo > pre-upgrade-cluster.json

# Record current version
inferadb-ledger --version > pre-upgrade-version.txt
```

## Upgrade Procedure

### Order of Operations

1. **Upgrade followers first** (non-leaders)
2. **Upgrade leader last** (triggers election)

This minimizes leader elections during the upgrade.

### Identify Leader

```bash
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo | jq -r '.leader_id.id'
```

### Upgrade a Follower Node

For each follower (non-leader) node:

#### Step 1: Drain Traffic (if using load balancer)

```bash
# Remove from load balancer
kubectl label pod ledger-1 ledger.inferadb.com/ready=false --overwrite
```

#### Step 2: Stop the Node

```bash
# Graceful shutdown
kubectl delete pod ledger-1

# Or systemd
systemctl stop ledger
```

#### Step 3: Verify Node Left Cleanly

```bash
# Check from another node
grpcurl -plaintext node2:50051 ledger.v1.AdminService/GetClusterInfo
```

Cluster should show reduced membership.

#### Step 4: Upgrade and Start

```bash
# Pull new image / install new binary
docker pull inferadb/ledger:v1.2.0

# Start node
kubectl apply -f ledger-1-pod.yaml

# Or systemd
systemctl start ledger
```

#### Step 5: Verify Node Rejoined

```bash
# Check health
grpcurl -plaintext node1:50051 ledger.v1.HealthService/Check

# Check cluster membership
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo
```

Wait for node to catch up (commit index should match other nodes).

#### Step 6: Re-enable Traffic

```bash
kubectl label pod ledger-1 ledger.inferadb.com/ready=true --overwrite
```

#### Step 7: Wait Before Next Node

Wait 2 minutes and verify:

- No errors in logs
- Metrics look normal
- Write latency stable

### Upgrade Leader Node

After all followers are upgraded:

#### Step 1: Identify Current Leader

```bash
LEADER=$(grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo | jq -r '.leader_id.id')
echo "Current leader: $LEADER"
```

#### Step 2: Gracefully Transfer Leadership (Optional)

```bash
# If supported, request leadership transfer
grpcurl -plaintext $LEADER:50051 ledger.v1.AdminService/TransferLeadership
```

#### Step 3: Stop Leader

```bash
# Stop the leader node
kubectl delete pod $LEADER

# New leader will be elected automatically
```

#### Step 4: Monitor Election

```bash
# Watch for new leader
watch -n1 'grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo | jq -r .leader_id.id'
```

Election should complete within 5 seconds.

#### Step 5: Upgrade and Start Former Leader

Same as follower procedure (Steps 4-7).

## Post-Upgrade Verification

### 1. Version Check

```bash
for node in node1 node2 node3; do
  echo "$node: $(grpcurl -plaintext $node:50051 ledger.v1.AdminService/GetNodeInfo | jq -r .version)"
done
```

All nodes should report new version.

### 2. Cluster Health

```bash
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo > post-upgrade-cluster.json
diff pre-upgrade-cluster.json post-upgrade-cluster.json
```

Membership should be identical.

### 3. Functional Test

```bash
# Write test
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "client_id": {"id": "upgrade-test"}, "sequence": "1", "operations": [{"set_entity": {"key": "test:upgrade", "value": "dGVzdA=="}}]}' \
  localhost:50051 ledger.v1.WriteService/Write

# Read test
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "key": "test:upgrade"}' \
  localhost:50051 ledger.v1.ReadService/Read
```

### 4. Metrics Comparison

```bash
# Compare key metrics
curl -s localhost:9090/metrics | grep -E '(proposals_pending|write_latency|is_leader)'
```

Values should be similar to pre-upgrade baseline.

## Rollback Procedure

If issues are detected during upgrade:

### 1. Stop the Problematic Node

```bash
kubectl delete pod ledger-bad
```

### 2. Restore Previous Version

```bash
docker pull inferadb/ledger:v1.1.0  # Previous version
```

### 3. Start with Old Version

```bash
kubectl apply -f ledger-1-pod-v1.1.0.yaml
```

### 4. If Multiple Nodes Affected

If more than one node has issues:

1. Stop all upgraded nodes
2. Restore from backup if data corruption suspected
3. Start nodes with old version one at a time

## Timing Guidelines

| Cluster Size | Total Upgrade Time | Per-Node Window |
| ------------ | ------------------ | --------------- |
| 3 nodes      | 15-20 minutes      | 5 minutes       |
| 5 nodes      | 25-35 minutes      | 5 minutes       |
| 7 nodes      | 35-45 minutes      | 5 minutes       |

## Common Issues

### Node Won't Rejoin

**Symptoms**: Node starts but doesn't appear in cluster membership.

**Solutions**:

1. Check logs for bootstrap errors
2. Verify peer DNS is resolving correctly
3. Check network connectivity to other nodes

### Leader Election Stuck

**Symptoms**: No leader elected after stopping old leader.

**Solutions**:

1. Verify quorum is available (majority of nodes)
2. Check for network partitions
3. Review Raft logs for split-brain indicators

### Performance Degradation

**Symptoms**: Higher latency or errors after upgrade.

**Solutions**:

1. Check for new configuration requirements
2. Review release notes for breaking changes
3. Consider rollback if issues persist

## Checklist Summary

Pre-upgrade:

- [ ] All nodes healthy
- [ ] Proposal backlog cleared
- [ ] State recorded

Per follower:

- [ ] Drain traffic
- [ ] Stop node
- [ ] Verify left cleanly
- [ ] Upgrade and start
- [ ] Verify rejoined
- [ ] Re-enable traffic
- [ ] Wait 2 minutes

Leader:

- [ ] Identify leader
- [ ] Stop (triggers election)
- [ ] Monitor new leader election
- [ ] Upgrade and start
- [ ] Verify rejoined

Post-upgrade:

- [ ] All versions match
- [ ] Cluster healthy
- [ ] Functional tests pass
- [ ] Metrics normal
