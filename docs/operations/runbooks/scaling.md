# Scaling Runbook

Procedures for scaling Ledger clusters up or down.

## Overview

Ledger uses Raft consensus, which requires odd-numbered cluster sizes for optimal quorum:

| Size | Voters | Quorum | Fault Tolerance |
| ---- | ------ | ------ | --------------- |
| 1    | 1      | 1      | 0 nodes         |
| 3    | 3      | 2      | 1 node          |
| 5    | 5      | 3      | 2 nodes         |
| 7    | 7      | 4      | 3 nodes         |

**Recommendation**: Use 3 voters for most deployments, 5 for high availability requirements.

## Scaling Up (Adding Nodes)

### Prerequisites

- [ ] Cluster is healthy (all nodes reporting)
- [ ] Leader is elected
- [ ] Storage provisioned for new node
- [ ] Network connectivity confirmed

### Procedure: 3 → 5 Nodes

#### Step 1: Verify Current State

```bash
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo
```

Expected: 3 members, 1 leader.

#### Step 2: Prepare New Nodes

For each new node (node4, node5):

```bash
# Create data directory
mkdir -p /var/lib/ledger-new

# Create peer file pointing to existing cluster
cat > /var/lib/ledger-new/peers.json << 'EOF'
{"cached_at": 0, "peers": [
  {"addr": "node1:50051"},
  {"addr": "node2:50051"},
  {"addr": "node3:50051"}
]}
EOF
```

#### Step 3: Start New Node in Join Mode

```bash
inferadb-ledger \
  --listen 0.0.0.0:50051 \
  --data /var/lib/ledger-new \
  --peers /var/lib/ledger-new/peers.json \
  --join
```

The node will:

1. Start gRPC server
2. Connect to discovered peers
3. Wait for AdminService `JoinCluster` call

#### Step 4: Add Node to Cluster

From the leader or any cluster member:

```bash
# Get the new node's info
NEW_NODE_ID=$(grpcurl -plaintext node4:50051 ledger.v1.AdminService/GetNodeInfo | jq -r '.node_id')

# Add to cluster
grpcurl -plaintext node1:50051 \
  -d "{\"node_id\": \"$NEW_NODE_ID\", \"address\": \"node4:50051\"}" \
  ledger.v1.AdminService/JoinCluster
```

#### Step 5: Verify Node Joined

```bash
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo
```

Expected: 4 members. The new node will initially be a learner, then promoted to voter.

#### Step 6: Wait for Sync

Monitor the new node's commit index until it catches up:

```bash
# On new node
curl -s localhost:9090/metrics | grep raft_commit_index
```

Compare with existing nodes. When commit indexes match, the node is fully synced.

#### Step 7: Repeat for Additional Nodes

Repeat steps 3-6 for node5.

#### Step 8: Final Verification

```bash
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo
```

Expected: 5 members, 1 leader.

### Kubernetes Scaling

For Kubernetes deployments using StatefulSet:

```bash
# Scale from 3 to 5 replicas
kubectl scale statefulset ledger --replicas=5

# Watch pods come up
kubectl get pods -l app=ledger -w

# Verify cluster membership
kubectl exec ledger-0 -- grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetClusterInfo
```

**Note**: The Helm chart automatically configures `INFERADB__LEDGER__CLUSTER` based on `replicaCount`. When scaling, update the Helm release:

```bash
helm upgrade ledger ./deploy/helm/inferadb-ledger --set replicaCount=5
```

## Scaling Down (Removing Nodes)

### Prerequisites

- [ ] Cluster is healthy
- [ ] Target size maintains quorum (minimum 3 for production)
- [ ] No ongoing migrations or recoveries

### Procedure: 5 → 3 Nodes

**Important**: Remove one node at a time. Never remove multiple nodes simultaneously.

#### Step 1: Identify Nodes to Remove

Choose non-leader nodes to minimize disruption:

```bash
LEADER=$(grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo | jq -r '.leader_id')
echo "Leader: $LEADER (keep this node)"
```

#### Step 2: Drain Node (Optional)

If using a load balancer, remove the node from rotation:

```bash
kubectl label pod ledger-4 ledger.inferadb.com/ready=false --overwrite
```

#### Step 3: Leave Cluster Gracefully

From the node being removed:

```bash
# Get own node ID
NODE_ID=$(grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetNodeInfo | jq -r '.node_id')

# Request removal
grpcurl -plaintext node1:50051 \
  -d "{\"node_id\": \"$NODE_ID\"}" \
  ledger.v1.AdminService/LeaveCluster
```

#### Step 4: Stop the Node

```bash
# Systemd
systemctl stop ledger

# Docker
docker stop ledger-node4

# Kubernetes (handled by StatefulSet scale down)
```

#### Step 5: Verify Removal

```bash
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo
```

Expected: 4 members.

#### Step 6: Wait Before Next Removal

Wait 2-5 minutes and verify:

- No errors in remaining node logs
- Metrics are stable
- Write operations succeed

#### Step 7: Repeat for Additional Nodes

Repeat steps 2-6 for node5.

### Kubernetes Scale Down

```bash
# Scale from 5 to 3 replicas
kubectl scale statefulset ledger --replicas=3

# Update Helm release
helm upgrade ledger ./deploy/helm/inferadb-ledger --set replicaCount=3
```

Kubernetes will terminate pods in reverse order (ledger-4, then ledger-3).

## Removing the Leader

If you must remove the current leader:

### Option 1: Let Leadership Transfer Naturally

1. Stop the leader node
2. Remaining nodes will elect a new leader within 5 seconds
3. Continue with removal procedure

### Option 2: Graceful Leadership Transfer (if supported)

```bash
# Request leadership transfer to another node
grpcurl -plaintext $LEADER:50051 ledger.v1.AdminService/TransferLeadership

# Wait for new leader
sleep 5

# Verify new leader
grpcurl -plaintext node1:50051 ledger.v1.AdminService/GetClusterInfo

# Now safe to remove old leader
```

## Emergency Scale Down

If nodes are unresponsive and you need to force scale down:

**Warning**: This can cause data loss if quorum is lost.

### Force Remove Unresponsive Node

If a node is permanently failed:

```bash
# From a healthy node, remove the failed node
grpcurl -plaintext healthy-node:50051 \
  -d '{"node_id": "FAILED_NODE_ID"}' \
  ledger.v1.AdminService/LeaveCluster
```

The cluster will remove the node from Raft membership.

### Recovery from Split Brain

If cluster split into multiple partitions:

1. **Stop all nodes**
2. **Identify which partition has latest data** (highest commit index)
3. **Restart surviving partition** with `--single` to force new cluster
4. **Add new nodes** to replace lost ones

See [Disaster Recovery](disaster-recovery.md) for detailed procedures.

## Timing Guidelines

| Operation   | Expected Duration | Notes                           |
| ----------- | ----------------- | ------------------------------- |
| Add node    | 2-5 minutes       | Includes sync time              |
| Remove node | 30 seconds        | Plus wait time between removals |
| Scale 3→5   | 10-15 minutes     | Add nodes sequentially          |
| Scale 5→3   | 10-15 minutes     | Remove nodes sequentially       |

## Troubleshooting

### Node Won't Join

**Symptoms**: New node starts but never appears in cluster membership.

**Solutions**:

1. Verify peer discovery is working: check logs for "discovered N peers"
2. Check network connectivity: `grpcurl -plaintext existing-node:50051 ledger.v1.HealthService/Check`
3. Verify node ID is unique (check `/var/lib/ledger/node_id`)

### Cluster Stuck After Scale

**Symptoms**: Cluster operations slow or failing after adding/removing nodes.

**Solutions**:

1. Check proposal backlog: `curl localhost:9090/metrics | grep proposals_pending`
2. Verify all nodes have same commit index
3. Check for network issues between nodes

### Cannot Remove Node

**Symptoms**: `LeaveCluster` returns error.

**Solutions**:

1. Ensure you're calling a healthy cluster member, not the removed node
2. If node already removed, error is expected
3. Check if removal would break quorum

## See Also

- [Deployment Guide](../deployment.md) - Initial cluster setup
- [Upgrade Runbook](rolling-upgrade.md) - Version upgrade procedures
- [Disaster Recovery](disaster-recovery.md) - Recovery procedures
