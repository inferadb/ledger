<div align="center">
    <p><a href="https://inferadb.com"><img src=".github/inferadb.png" width="100" alt="InferaDB Logo" /></a></p>
    <h1>InferaDB Ledger</h1>
    <p>
        <a href="https://discord.gg/inferadb"><img src="https://img.shields.io/badge/Discord-Join%20us-5865F2?logo=discord&logoColor=white" alt="Discord" /></a>
        <a href="#license"><img src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg" alt="License" /></a>
        <a href="https://github.com/inferadb/ledger/actions"><img src="https://img.shields.io/github/actions/workflow/status/inferadb/ledger/ci.yml?branch=main" alt="CI" /></a>
    </p>
    <p><b>Blockchain storage for cryptographically verifiable authorization.</b></p>
</div>

> [!IMPORTANT]
> Under active development. Not production-ready.

[InferaDB](https://inferadb.com) Ledger is a distributed blockchain database optimized for authorization workfloads. It's optimized for low query latency, making it suitable for global applications. It commits every state change cryptographically, replicates via Raft consensus, and lets clients verify independently.

## Features

**Cryptographic Verification**

- Per-vault blockchain with chain-linked state roots
- Merkle proofs for transaction inclusion
- SHA-256 commitments clients can verify independently

**Raft Consensus**

- Strong consistency with quorum-based replication
- Automatic leader election and failover
- Log replay for deterministic state recovery

**Performance**

- Sub-millisecond reads from any replica
- <50ms p99 write latency (same datacenter)
- Bucket-based state roots for O(k) computation (k = dirty keys)
- Batched transactions amortize consensus overhead

**Multi-Tenancy**

- Namespace isolation per organization
- Multiple vaults per namespace with independent chains
- Shard groups for efficient Raft resource usage

**Storage**

- Embedded ACID database with zero-copy reads
- Hybrid architecture: fast K/V queries + merkle commitments
- Tiered snapshots (hot/warm/cold) for historical reads

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      gRPC API (HTTP/2)                      │
├─────────────────────────────────────────────────────────────┤
│                     Raft Consensus Layer                    │
│         (Leader election, log replication, quorum)          │
├─────────────────────────────────────────────────────────────┤
│  Namespace: org_acme          │  Namespace: org_startup     │
│  ├─ Vault: prod [chain]       │  ├─ Vault: main [chain]     │
│  └─ Vault: staging [chain]    │  └─ ...                     │
├─────────────────────────────────────────────────────────────┤
│                State Layer (inferadb-ledger-store)          │
│    Relationships │ Entities │ Indexes │ State Roots         │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- [mise](https://mise.jdx.dev/) for development tools (Rust, protoc, buf)

### Build

```bash
# Clone the repository
git clone https://github.com/inferadb/ledger.git
cd ledger

# Install development tools (rust, protoc, buf)
mise trust && mise install

# Build all crates
cargo build --release

# Run tests
cargo test
```

### Run a Single Node

```bash
# Create a config file from the example
cp inferadb-ledger.example.toml inferadb-ledger.toml

# For single-node development, set bootstrap_expect=1:
INFERADB__LEDGER__LISTEN_ADDR=127.0.0.1:50051 \
INFERADB__LEDGER__DATA_DIR=/tmp/ledger \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=1 \
cargo run --release -p inferadb-ledger-server
```

## Operations

### Cluster Setup

Ledger uses coordinated bootstrap based on `bootstrap_expect`:

| Value | Mode        | Behavior                                              |
| ----- | ----------- | ----------------------------------------------------- |
| `0`   | Join        | Wait to be added to existing cluster via AdminService |
| `1`   | Single-node | Bootstrap immediately, no coordination                |
| `2+`  | Coordinated | Wait for N peers, lowest-ID node bootstraps all       |

#### Single-Node Cluster

```bash
# Create data directory
mkdir -p /var/lib/ledger

# Start with bootstrap_expect=1
INFERADB__LEDGER__LISTEN_ADDR=127.0.0.1:50051 \
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=1 \
./target/release/ledger
```

#### Multi-Node Cluster (3 nodes)

Each node needs its own data directory and a peer cache file listing other nodes.

**Node 1** (`/var/lib/ledger-1`):

```bash
# Create peer cache with other nodes
cat > /var/lib/ledger-1/peers.cache << 'EOF'
{"cached_at": 0, "peers": [
  {"addr": "192.168.1.102:50051", "priority": 10, "weight": 100},
  {"addr": "192.168.1.103:50051", "priority": 10, "weight": 100}
]}
EOF

INFERADB__LEDGER__LISTEN_ADDR=192.168.1.101:50051 \
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger-1 \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=3 \
INFERADB__LEDGER__DISCOVERY_CACHE_PATH=/var/lib/ledger-1/peers.cache \
./target/release/ledger
```

**Node 2** (`/var/lib/ledger-2`):

```bash
cat > /var/lib/ledger-2/peers.cache << 'EOF'
{"cached_at": 0, "peers": [
  {"addr": "192.168.1.101:50051", "priority": 10, "weight": 100},
  {"addr": "192.168.1.103:50051", "priority": 10, "weight": 100}
]}
EOF

INFERADB__LEDGER__LISTEN_ADDR=192.168.1.102:50051 \
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger-2 \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=3 \
INFERADB__LEDGER__DISCOVERY_CACHE_PATH=/var/lib/ledger-2/peers.cache \
./target/release/ledger
```

**Node 3** (`/var/lib/ledger-3`):

```bash
cat > /var/lib/ledger-3/peers.cache << 'EOF'
{"cached_at": 0, "peers": [
  {"addr": "192.168.1.101:50051", "priority": 10, "weight": 100},
  {"addr": "192.168.1.102:50051", "priority": 10, "weight": 100}
]}
EOF

INFERADB__LEDGER__LISTEN_ADDR=192.168.1.103:50051 \
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger-3 \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=3 \
INFERADB__LEDGER__DISCOVERY_CACHE_PATH=/var/lib/ledger-3/peers.cache \
./target/release/ledger
```

Start all three nodes. They will:

1. Discover each other via peer cache
2. Exchange node info via `GetNodeInfo` RPC
3. The node with lowest Snowflake ID bootstraps the cluster
4. Other nodes join automatically

#### DNS-Based Discovery (Production / Kubernetes)

For production, use DNS A records instead of static peer caches. This is optimized for Kubernetes headless Services:

```bash
# Set discovery_domain to a DNS name that returns A records for all peers
# In Kubernetes, this is the headless service DNS name
INFERADB__LEDGER__DISCOVERY_DOMAIN=ledger.default.svc.cluster.local \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=3 \
./target/release/ledger
```

For non-Kubernetes environments, configure DNS A records:

```bash
# DNS A records (all nodes share the same name, each with its own IP)
ledger.cluster.example.com. 300 IN A 192.168.1.101
ledger.cluster.example.com. 300 IN A 192.168.1.102
ledger.cluster.example.com. 300 IN A 192.168.1.103
```

### Shutdown and Restart

#### Graceful Shutdown

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

#### Restarting a Node

Simply start the node with the same `data_dir`. The persisted `node_id` file ensures it rejoins with its original identity:

```bash
./target/release/ledger --config /etc/ledger/config.toml
```

On restart:

1. Node loads persisted `node_id` from `{data_dir}/node_id`
2. Recovers Raft state from `raft.db`
3. Replays log entries after last snapshot
4. Rejoins cluster with same identity

#### Cluster Restart (All Nodes)

Stop nodes in any order. Restart in any order—Raft leader election handles coordination.

For a 3-node cluster, maintain quorum (2 nodes) during rolling restarts:

1. Stop node 1, wait for it to be removed from Raft
2. Restart node 1, wait for it to rejoin
3. Repeat for nodes 2 and 3

### Adding and Removing Nodes

#### Adding a Node

Start the new node with `bootstrap_expect=0` (join mode) and a peer cache pointing to existing nodes:

```bash
cat > /var/lib/ledger-new/peers.cache << 'EOF'
{"cached_at": 0, "peers": [
  {"addr": "192.168.1.101:50051", "priority": 10, "weight": 100}
]}
EOF

INFERADB__LEDGER__LISTEN_ADDR=192.168.1.104:50051 \
INFERADB__LEDGER__DATA_DIR=/var/lib/ledger-new \
INFERADB__LEDGER__BOOTSTRAP_EXPECT=0 \
INFERADB__LEDGER__DISCOVERY_CACHE_PATH=/var/lib/ledger-new/peers.cache \
./target/release/ledger
```

The node will:

1. Start its gRPC server
2. Connect to discovered peers
3. Wait for AdminService `JoinCluster` RPC from the leader
4. Receive Raft log and become a cluster member

#### Removing a Node

**Graceful removal**: Stop the node with SIGTERM. After the heartbeat timeout (default: 30s), the cluster automatically removes it from Raft membership.

**Immediate removal**: The cluster leader will automatically propose `RemoveNode` after detecting missing heartbeats.

### Backup

#### What to Back Up

The `data_dir` contains all persistent state:

```
/var/lib/ledger/
├── node_id           # Node identity (preserve for same-node restore)
├── state.db          # Current state
├── raft.db           # Raft log
├── blocks.db         # Block archive
└── snapshots/        # State snapshots
```

#### Backup Methods

**Cold backup** (node stopped):

```bash
# Stop the node
kill $(cat /var/lib/ledger/ledger.pid)

# Copy entire data directory
cp -r /var/lib/ledger /backup/ledger-$(date +%Y%m%d-%H%M%S)

# Restart node
./target/release/ledger --config /etc/ledger/config.toml
```

**Snapshot-based backup** (node running):

Snapshots in `{data_dir}/snapshots/` are self-contained and safe to copy while the node runs:

```bash
# Copy latest snapshot
LATEST=$(ls -t /var/lib/ledger/snapshots/*.snap | head -1)
cp "$LATEST" /backup/
```

Snapshots include:

- State data (zstd compressed)
- Shard height
- Vault state roots
- SHA-256 checksum

#### Backup Frequency

| Data                 | Recommended Frequency                              |
| -------------------- | -------------------------------------------------- |
| Full `data_dir`      | Daily (cold backup during maintenance window)      |
| Snapshots            | Hourly (automatic, copy latest)                    |
| Off-site replication | Real-time (run 3+ nodes across availability zones) |

### Restore

#### Restoring a Single Node

To restore a node to its previous state:

```bash
# Stop the node if running
kill $(cat /var/lib/ledger/ledger.pid) 2>/dev/null || true

# Remove corrupted data
rm -rf /var/lib/ledger

# Restore from backup
cp -r /backup/ledger-20240115-030000 /var/lib/ledger

# Start the node
./target/release/ledger --config /etc/ledger/config.toml
```

The node will:

1. Load the restored `node_id`
2. Detect it's behind the cluster
3. Catch up via Raft log replication from peers

#### Restoring from Snapshot Only

If you only have a snapshot (not the full `data_dir`):

```bash
# Create fresh data directory
mkdir -p /var/lib/ledger/snapshots

# Copy snapshot
cp /backup/000010000.snap /var/lib/ledger/snapshots/

# Start with bootstrap_expect=0 to join existing cluster
INFERADB__LEDGER__BOOTSTRAP_EXPECT=0 \
./target/release/ledger --config /etc/ledger/config.toml
```

The node will:

1. Generate a new `node_id`
2. Load state from snapshot
3. Join the cluster as a new member
4. Receive missing log entries from peers

#### Full Cluster Restore (Disaster Recovery)

If all nodes are lost, restore from the most recent backup:

```bash
# On each node, restore from backup
cp -r /backup/ledger-node1 /var/lib/ledger

# Start first node with bootstrap_expect=1 to force bootstrap
INFERADB__LEDGER__BOOTSTRAP_EXPECT=1 \
./target/release/ledger

# Start remaining nodes with bootstrap_expect=0
INFERADB__LEDGER__BOOTSTRAP_EXPECT=0 \
./target/release/ledger
```

### Configuration

See [`inferadb-ledger.example.toml`](inferadb-ledger.example.toml) for all options. Key settings:

```toml
listen_addr = "0.0.0.0:50051"         # gRPC listen address
data_dir = "/var/lib/ledger"          # Raft logs and state
bootstrap_expect = 3                  # 0=join, 1=single-node, 2+=coordinated
```

**Coordinated Bootstrap**: Nodes automatically generate Snowflake IDs (persisted to `{data_dir}/node_id`) and coordinate cluster formation:

1. Each node starts its gRPC server and polls discovery for peers
2. Once `bootstrap_expect` nodes discover each other, they exchange node info via `GetNodeInfo` RPC
3. The node with the lowest Snowflake ID (earliest started) bootstraps the cluster
4. Other nodes wait to be added as Raft voters

This prevents split-brain scenarios where multiple nodes independently bootstrap separate clusters.

Environment variables override config file values using the `INFERADB__LEDGER__` prefix (e.g., `INFERADB__LEDGER__BOOTSTRAP_EXPECT=3`).

### Kubernetes Deployment

For Kubernetes deployments, use the provided Helm chart or raw manifests.

**Quick Start with Helm:**

```bash
helm install ledger ./deploy/helm/inferadb-ledger \
  --namespace inferadb \
  --create-namespace
```

**Quick Start with Kustomize:**

```bash
kubectl apply -k deploy/kubernetes/
```

The Kubernetes deployment uses:

- **StatefulSet** for stable pod identities and persistent storage
- **Headless Service** for DNS-based peer discovery
- **PodDisruptionBudget** to maintain Raft quorum during rolling updates

Pods discover each other via DNS A records from the headless service. Set `discovery_domain` to the service FQDN (e.g., `ledger.inferadb.svc.cluster.local`).

See [`deploy/kubernetes/`](deploy/kubernetes/) for raw manifests and [`deploy/helm/`](deploy/helm/) for the Helm chart with configurable values.

## Crates

| Crate                        | Description                                    |
| ---------------------------- | ---------------------------------------------- |
| `inferadb-ledger-types`      | Core types, hashing, and protobuf definitions  |
| `inferadb-ledger-store`      | Embedded B+ tree database engine               |
| `inferadb-ledger-state`      | Domain state management, indexes, snapshots    |
| `inferadb-ledger-raft`       | Raft consensus, log storage, network transport |
| `inferadb-ledger-server`     | gRPC server, request routing, client handling  |
| `inferadb-ledger-sdk`        | Production-grade Rust SDK for client apps      |
| `inferadb-ledger-test-utils` | Shared test utilities                          |

## Design

See [DESIGN.md](DESIGN.md) for details on:

- Block structure and chain linking
- State root computation (bucket-based hashing)
- ID generation and determinism requirements
- Historical reads and snapshot tiers
- Multi-vault failure isolation
- Shard group scaling architecture

## Community

Join us on [Discord](https://discord.gg/inferadb) for questions, discussions, and contributions.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
