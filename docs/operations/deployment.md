# Deployment Guide

This guide covers cluster deployment, scaling, backup, and recovery for InferaDB Ledger.

## Cluster Setup

Ledger uses a CockroachDB-style `start`+`init` bootstrap pattern. Nodes start their gRPC servers but wait for cluster initialization before accepting writes.

### Single-Node Cluster

```bash
mkdir -p /var/lib/ledger

./target/release/inferadb-ledger \
  --listen 127.0.0.1:9090 \
  --data /var/lib/ledger

# Initialize (once, from any machine that can reach the node)
./target/release/inferadb-ledger init --host 127.0.0.1:9090
```

### Multi-Node Cluster (3 nodes)

Start each node with `--join` pointing to one or more seed addresses. Each node needs its own data directory.

**Node 1** (`192.168.1.101`):

```bash
./target/release/inferadb-ledger \
  --listen 192.168.1.101:9090 \
  --data /var/lib/ledger
```

**Node 2** (`192.168.1.102`):

```bash
./target/release/inferadb-ledger \
  --listen 192.168.1.102:9090 \
  --data /var/lib/ledger \
  --join 192.168.1.101:9090
```

**Node 3** (`192.168.1.103`):

```bash
./target/release/inferadb-ledger \
  --listen 192.168.1.103:9090 \
  --data /var/lib/ledger \
  --join 192.168.1.101:9090,192.168.1.102:9090
```

**Initialize the cluster (once):**

```bash
./target/release/inferadb-ledger init --host 192.168.1.101:9090
```

All nodes discover each other via `--join` seed addresses, exchange node info, and form the cluster automatically after initialization.

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
INFERADB__LEDGER__DATA=/var/lib/ledger \
./target/release/inferadb-ledger
```

On restart:

1. Node loads persisted `node_id` from `{data_dir}/node_id`
2. Recovers Raft state from `raft.db`
3. Replays log entries after last snapshot
4. Rejoins cluster with same identity

### Cluster Restart (All Nodes)

> **Warning**: Never stop a majority of nodes simultaneously. A 3-node cluster requires 2 nodes running to maintain quorum. Losing quorum makes the cluster read-only until quorum is restored.

For a full cluster restart, use rolling restart to maintain availability:

1. Stop node 1, wait for it to be removed from Raft
2. Restart node 1, wait for it to rejoin
3. Repeat for nodes 2 and 3

## Adding and Removing Nodes

### Adding a Node

Start the new node with `--join` pointing to any existing cluster member:

```bash
./target/release/inferadb-ledger \
  --listen 192.168.1.104:9090 \
  --data /var/lib/ledger \
  --join 192.168.1.101:9090
```

The node will:

1. Start its gRPC server
2. Connect to the seed address and discover other peers
3. Join the cluster as a learner
4. Receive Raft log and be promoted to voter once caught up

### Removing a Node

**Graceful removal**: Stop the node with SIGTERM. After the heartbeat timeout (default: 30s), the cluster automatically removes it from Raft membership.

**Immediate removal**: The cluster leader will automatically propose `RemoveNode` after detecting missing heartbeats.

## Backup

### What to Back Up

The `data_dir` contains all persistent state:

```text
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
INFERADB__LEDGER__DATA=/var/lib/ledger \
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
- Region height
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
INFERADB__LEDGER__DATA=/var/lib/ledger \
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

./target/release/inferadb-ledger \
  --listen 0.0.0.0:9090 \
  --data /var/lib/ledger \
  --join existing-node:9090
```

The node will:

1. Generate a new `node_id`
2. Load state from snapshot
3. Join the cluster as a new member via the seed address
4. Receive missing log entries from peers

### Full Cluster Restore (Disaster Recovery)

If all nodes are lost, restore from the most recent backup:

```bash
# On each node, restore from backup
cp -r /backup/ledger-node1 /var/lib/ledger

# Start the first node
./target/release/inferadb-ledger --listen 0.0.0.0:9090 --data /var/lib/ledger

# Initialize the restored cluster
./target/release/inferadb-ledger init --host node1:9090

# Start remaining nodes with --join
./target/release/inferadb-ledger --listen 0.0.0.0:9090 --data /var/lib/ledger --join node1:9090
```

## Configuration Reference

Configuration can be set via CLI arguments or environment variables. CLI arguments take precedence.

### Core Options

| CLI                        | Environment Variable                       | Default       | Description                                                             |
| -------------------------- | ------------------------------------------ | ------------- | ----------------------------------------------------------------------- |
| `--listen`                 | `INFERADB__LEDGER__LISTEN`                 | _(none)_      | TCP address for gRPC API                                                |
| `--socket`                 | `INFERADB__LEDGER__SOCKET`                 | _(none)_      | Unix domain socket path for gRPC API                                    |
| `--data`                   | `INFERADB__LEDGER__DATA`                   | _(ephemeral)_ | Persistent storage directory                                            |
| `--join`                   | `INFERADB__LEDGER__JOIN`                   | _(none)_      | Comma-separated seed addresses for cluster discovery                    |
| `--region`                 | `INFERADB__LEDGER__REGION`                 | `global`      | Geographic data residency region                                        |
| `--advertise`              | `INFERADB__LEDGER__ADVERTISE`              | _(auto)_      | Address advertised to peers                                             |
| `--metrics`                | `INFERADB__LEDGER__METRICS`                | _(disabled)_  | Prometheus metrics address                                              |
| `--email-blinding-key`     | `INFERADB__LEDGER__EMAIL_BLINDING_KEY`     | _(required)_  | HMAC key for email hashing (required for user onboarding RPCs)          |
| `--enable-grpc-reflection` | `INFERADB__LEDGER__ENABLE_GRPC_REFLECTION` | `false`       | Enable gRPC reflection (needed for `grpcurl` and other dynamic clients) |
| `--log-format`             | —                                          | `auto`        | Log format: `text`, `json`, or `auto`                                   |
| `--concurrent`             | `INFERADB__LEDGER__MAX_CONCURRENT`         | `100`         | Max concurrent requests                                                 |
| `--timeout`                | `INFERADB__LEDGER__TIMEOUT`                | `30`          | Request timeout in seconds                                              |

At least one of `--listen` or `--socket` must be specified.

### Bootstrap

Nodes start their gRPC servers but wait for initialization before accepting writes.

1. Start all nodes with `inferadb-ledger --listen ... --data ... [--join seed1,seed2]`
2. Run `inferadb-ledger init --host <any-node>` once to initialize the cluster
3. Nodes discover each other via `--join` seed addresses and form membership automatically

The `init` subcommand sends a one-time `InitCluster` RPC to the specified host. Run it exactly once per cluster lifetime.

### Notes

**Ephemeral Mode**: When `--data` is omitted, the server runs in ephemeral mode using a temporary directory. All data is lost on shutdown. Useful for development and testing.

**Unix Domain Sockets**: Use `--socket /path/to/ledger.sock` for local-only access without TCP overhead. Both `--listen` and `--socket` can be used simultaneously.

Run `inferadb-ledger --help` for usage information.

## Network Connectivity

### Clients → Cluster

Under redirect-based routing, clients need direct network access to **every node** that could hold regional Raft leadership, not just a subset of gateway nodes. This is because:

1. At cold start the client may connect to any node for discovery.
2. After receiving a `NotLeader` hint, the client reconnects directly to the regional leader's advertised endpoint.

For single-VPC/VNet deployments this is typically already the case. For cross-VPC deployments, ensure ingress rules and firewall policies allow client traffic to every cluster node on the configured gRPC port.

### Unix Domain Sockets

For co-located clients (sidecar proxies, local CLIs), use `--socket` to expose a Unix domain socket listener:

```bash
inferadb-ledger --socket /var/run/ledger.sock --data /var/lib/ledger
```

Both `--listen` (TCP) and `--socket` (UDS) can be used simultaneously for mixed access patterns.

See: [Redirect-Based Client Routing Architecture](runbooks/architecture-redirect-routing.md).

## Kubernetes Deployment

Use the provided Helm chart or raw manifests.

**Helm:**

```bash
helm install ledger ./deploy/helm/inferadb-ledger \
  --organization inferadb \
  --create-organization
```

**Kustomize:**

```bash
kubectl apply -k deploy/kubernetes/
```

The Kubernetes deployment uses:

- **StatefulSet** for stable pod identities and persistent storage
- **Headless Service** for DNS-based peer discovery
- **PodDisruptionBudget** to maintain Raft quorum during rolling updates

Pods discover each other via `--join` with the headless service FQDN (e.g., `--join ledger.inferadb.svc.cluster.local`). Run `inferadb-ledger init` once after the StatefulSet is ready.

See [`deploy/kubernetes/`](../../deploy/kubernetes/) for raw manifests and [`deploy/helm/`](../../deploy/helm/) for the Helm chart.
