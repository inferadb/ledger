# Deployment Guide

This guide covers cluster deployment, scaling, backup, and recovery for InferaDB Ledger.

## Cluster Setup

Ledger uses coordinated bootstrap based on `bootstrap_expect`:

| Value | Mode        | Behavior                                              |
| ----- | ----------- | ----------------------------------------------------- |
| `0`   | Join        | Wait to be added to existing cluster via AdminService |
| `1`   | Single-node | Bootstrap immediately, no coordination                |
| `2+`  | Coordinated | Wait for N peers, lowest-ID node bootstraps all       |

### Single-Node Cluster

```bash
mkdir -p /var/lib/ledger

INFERADB__LEDGER__LISTEN_ADDR=127.0.0.1:50051 \
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=1 \
./target/release/inferadb-ledger
```

### Multi-Node Cluster (3 nodes)

Each node needs its own data directory and a peer cache file listing other nodes.

**Node 1** (`/var/lib/ledger-1`):

```bash
cat > /var/lib/ledger-1/peers.cache << 'EOF'
{"cached_at": 0, "peers": [
  {"addr": "192.168.1.102:50051"},
  {"addr": "192.168.1.103:50051"}
]}
EOF

INFERADB__LEDGER__LISTEN_ADDR=192.168.1.101:50051 \
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger-1 \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=3 \
INFERADB__LEDGER__DISCOVERY_CACHE_PATH=/var/lib/ledger-1/peers.cache \
./target/release/inferadb-ledger
```

**Node 2** (`/var/lib/ledger-2`):

```bash
cat > /var/lib/ledger-2/peers.cache << 'EOF'
{"cached_at": 0, "peers": [
  {"addr": "192.168.1.101:50051"},
  {"addr": "192.168.1.103:50051"}
]}
EOF

INFERADB__LEDGER__LISTEN_ADDR=192.168.1.102:50051 \
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger-2 \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=3 \
INFERADB__LEDGER__DISCOVERY_CACHE_PATH=/var/lib/ledger-2/peers.cache \
./target/release/inferadb-ledger
```

**Node 3** (`/var/lib/ledger-3`):

```bash
cat > /var/lib/ledger-3/peers.cache << 'EOF'
{"cached_at": 0, "peers": [
  {"addr": "192.168.1.101:50051"},
  {"addr": "192.168.1.102:50051"}
]}
EOF

INFERADB__LEDGER__LISTEN_ADDR=192.168.1.103:50051 \
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger-3 \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=3 \
INFERADB__LEDGER__DISCOVERY_CACHE_PATH=/var/lib/ledger-3/peers.cache \
./target/release/inferadb-ledger
```

Start all three nodes. They will:

1. Discover each other via peer cache
2. Exchange node info via `GetNodeInfo` RPC
3. The node with lowest Snowflake ID bootstraps the cluster
4. Other nodes join automatically

### DNS-Based Discovery (Production / Kubernetes)

For production, use DNS A records instead of static peer caches:

```bash
INFERADB__LEDGER__DISCOVERY_DOMAIN=ledger.default.svc.cluster.local \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=3 \
./target/release/inferadb-ledger
```

For non-Kubernetes environments, configure DNS A records:

```bash
# DNS A records (all nodes share the same name, each with its own IP)
ledger.cluster.example.com. 300 IN A 192.168.1.101
ledger.cluster.example.com. 300 IN A 192.168.1.102
ledger.cluster.example.com. 300 IN A 192.168.1.103
```

## Shutdown and Restart

### Graceful Shutdown

Send SIGTERM to stop a node gracefully:

```bash
kill $(cat /var/lib/ledger/ledger.pid)
# Or if running in foreground: Ctrl+C
```

The node will:

1. Stop accepting new requests
2. Complete in-flight operations
3. Flush pending writes to disk
4. Exit cleanly

### Restarting a Node

Start the node with the same `data_dir`. The persisted `node_id` file ensures it rejoins with its original identity:

```bash
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger \
./target/release/inferadb-ledger
```

On restart:

1. Node loads persisted `node_id` from `{data_dir}/node_id`
2. Recovers Raft state from `raft.db`
3. Replays log entries after last snapshot
4. Rejoins cluster with same identity

### Cluster Restart (All Nodes)

Stop nodes in any order. Restart in any order—Raft leader election handles coordination.

For a 3-node cluster, maintain quorum (2 nodes) during rolling restarts:

1. Stop node 1, wait for it to be removed from Raft
2. Restart node 1, wait for it to rejoin
3. Repeat for nodes 2 and 3

## Adding and Removing Nodes

### Adding a Node

Start the new node with `bootstrap_expect=0` (join mode) and a peer cache pointing to existing nodes:

```bash
cat > /var/lib/ledger-new/peers.cache << 'EOF'
{"cached_at": 0, "peers": [
  {"addr": "192.168.1.101:50051"}
]}
EOF

INFERADB__LEDGER__LISTEN_ADDR=192.168.1.104:50051 \
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger-new \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=0 \
INFERADB__LEDGER__DISCOVERY_CACHE_PATH=/var/lib/ledger-new/peers.cache \
./target/release/inferadb-ledger
```

The node will:

1. Start its gRPC server
2. Connect to discovered peers
3. Wait for AdminService `JoinCluster` RPC from the leader
4. Receive Raft log and become a cluster member

### Removing a Node

**Graceful removal**: Stop the node with SIGTERM. After the heartbeat timeout (default: 30s), the cluster automatically removes it from Raft membership.

**Immediate removal**: The cluster leader will automatically propose `RemoveNode` after detecting missing heartbeats.

## Backup

### What to Back Up

The `data_dir` contains all persistent state:

```
/var/lib/ledger/
├── node_id           # Node identity (preserve for same-node restore)
├── state.db          # Current state
├── raft.db           # Raft log
├── blocks.db         # Block archive
└── snapshots/        # State snapshots
```

### Backup Methods

**Cold backup** (node stopped):

```bash
kill $(cat /var/lib/ledger/ledger.pid)
cp -r /var/lib/ledger /backup/ledger-$(date +%Y%m%d-%H%M%S)
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger \
./target/release/inferadb-ledger
```

**Snapshot-based backup** (node running):

Snapshots in `{data_dir}/snapshots/` are self-contained and safe to copy while the node runs:

```bash
LATEST=$(ls -t /var/lib/ledger/snapshots/*.snap | head -1)
cp "$LATEST" /backup/
```

Snapshots include:

- State data (zstd compressed)
- Shard height
- Vault state roots
- SHA-256 checksum

### Backup Frequency

| Data                 | Recommended Frequency                              |
| -------------------- | -------------------------------------------------- |
| Full `data_dir`      | Daily (cold backup during maintenance window)      |
| Snapshots            | Hourly (automatic, copy latest)                    |
| Off-site replication | Real-time (run 3+ nodes across availability zones) |

## Restore

### Restoring a Single Node

```bash
kill $(cat /var/lib/ledger/ledger.pid) 2>/dev/null || true
rm -rf /var/lib/ledger
cp -r /backup/ledger-20240115-030000 /var/lib/ledger
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger \
./target/release/inferadb-ledger
```

The node will:

1. Load the restored `node_id`
2. Detect it's behind the cluster
3. Catch up via Raft log replication from peers

### Restoring from Snapshot Only

If you only have a snapshot (not the full `data_dir`):

```bash
mkdir -p /var/lib/ledger/snapshots
cp /backup/000010000.snap /var/lib/ledger/snapshots/

INFERADB__LEDGER__DATA_DIR=/var/lib/ledger \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=0 \
./target/release/inferadb-ledger
```

The node will:

1. Generate a new `node_id`
2. Load state from snapshot
3. Join the cluster as a new member
4. Receive missing log entries from peers

### Full Cluster Restore (Disaster Recovery)

If all nodes are lost, restore from the most recent backup:

```bash
# On each node, restore from backup
cp -r /backup/ledger-node1 /var/lib/ledger

# Start first node with bootstrap_expect=1 to force bootstrap
INFERADB__LEDGER__BOOTSTRAP_EXPECT=1 \
./target/release/inferadb-ledger

# Start remaining nodes with bootstrap_expect=0
INFERADB__LEDGER__BOOTSTRAP_EXPECT=0 \
./target/release/inferadb-ledger
```

## Configuration Reference

Configuration can be set via CLI arguments or environment variables. CLI arguments take precedence.

### Core Options

| CLI           | Environment Variable              | Default           | Description                               |
| ------------- | --------------------------------- | ----------------- | ----------------------------------------- |
| `--listen`    | `INFERADB__LEDGER__LISTEN_ADDR`   | `127.0.0.1:50051` | Host and port to accept connections       |
| `--data`      | `INFERADB__LEDGER__DATA_DIR`      | (ephemeral)       | Where to store data ([layout](../internals/storage.md)) |
| `--bootstrap` | `INFERADB__LEDGER__BOOTSTRAP_EXPECT` | `3`            | Cluster size (1=solo, 2+=cluster, 0=join) |
| `--metrics`   | `INFERADB__LEDGER__METRICS_ADDR`  | (disabled)        | Expose Prometheus metrics at this address |

### Discovery Options

How nodes find each other. See [discovery internals](../internals/discovery.md) for details.

| CLI          | Environment Variable                         | Default     | Description                           |
| ------------ | -------------------------------------------- | ----------- | ------------------------------------- |
| `--discovery`| `INFERADB__LEDGER__DISCOVERY_DOMAIN`         | (disabled)  | Find nodes via DNS (for Kubernetes)   |
| `--join`     | `INFERADB__LEDGER__DISCOVERY_CACHE_PATH`     | (disabled)  | File listing nodes to connect to      |
| `--join-ttl` | `INFERADB__LEDGER__DISCOVERY_CACHE_TTL_SECS` | `3600`      | How long cached node list stays valid |

### Tuning Options

These defaults work well for most deployments. See [consensus internals](../internals/consensus.md) for batching details.

| CLI                  | Environment Variable                        | Default | Description                             |
| -------------------- | ------------------------------------------- | ------- | --------------------------------------- |
| `--bootstrap-timeout`| `INFERADB__LEDGER__BOOTSTRAP_TIMEOUT_SECS`  | `60`    | How long to wait for other nodes (secs) |
| `--poll`             | `INFERADB__LEDGER__BOOTSTRAP_POLL_SECS`     | `2`     | How often to check for other nodes      |
| `--batch-size`       | `INFERADB__LEDGER__BATCH_MAX_SIZE`          | `100`   | Writes to group before committing       |
| `--batch-delay`      | `INFERADB__LEDGER__BATCH_MAX_DELAY_MS`      | `10`    | Max wait before committing a batch (ms) |
| `--concurrent`       | `INFERADB__LEDGER__REQUESTS_MAX_CONCURRENT` | `100`   | Simultaneous requests allowed           |
| `--timeout`          | `INFERADB__LEDGER__REQUESTS_TIMEOUT_SECS`   | `30`    | Max time for a request to complete      |

### Notes

**Ephemeral Mode**: When `--data` is not specified, the server runs in ephemeral mode using a temporary directory. All data is lost on shutdown. Useful for development and testing.

**Security**: The default listen address is `127.0.0.1` (localhost only). Set `--listen 0.0.0.0:50051` or a specific IP to accept remote connections.

Run `inferadb-ledger --help` for usage information.

**Coordinated Bootstrap**: Nodes automatically generate Snowflake IDs (persisted to `{data_dir}/node_id`) and coordinate cluster formation:

1. Each node starts its gRPC server and polls discovery for peers
2. Once `bootstrap_expect` nodes discover each other, they exchange node info via `GetNodeInfo` RPC
3. The node with the lowest Snowflake ID (earliest started) bootstraps the cluster
4. Other nodes wait to be added as Raft voters

This prevents split-brain scenarios where multiple nodes independently bootstrap separate clusters.

## Kubernetes Deployment

Use the provided Helm chart or raw manifests.

**Helm:**

```bash
helm install ledger ./deploy/helm/inferadb-ledger \
  --namespace inferadb \
  --create-namespace
```

**Kustomize:**

```bash
kubectl apply -k deploy/kubernetes/
```

The Kubernetes deployment uses:

- **StatefulSet** for stable pod identities and persistent storage
- **Headless Service** for DNS-based peer discovery
- **PodDisruptionBudget** to maintain Raft quorum during rolling updates

Pods discover each other via DNS A records from the headless service. Set `discovery_domain` to the service FQDN (e.g., `ledger.inferadb.svc.cluster.local`).

See [`deploy/kubernetes/`](../../deploy/kubernetes/) for raw manifests and [`deploy/helm/`](../../deploy/helm/) for the Helm chart.
