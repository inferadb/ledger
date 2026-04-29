# Deployment Guide

This guide covers cluster deployment, scaling, backup, and recovery for InferaDB Ledger.

## Cluster Setup

Ledger uses a CockroachDB-style `start`+`init` bootstrap pattern. Nodes start their gRPC servers but wait for cluster initialization before accepting writes.

### Single-Node Cluster

```bash
mkdir -p /var/lib/ledger

./target/release/inferadb-ledger \
  --listen 127.0.0.1:50051 \
  --data /var/lib/ledger

# Initialize (once, from any machine that can reach the node)
./target/release/inferadb-ledger init --host 127.0.0.1:50051
```

### Multi-Node Cluster (3 nodes)

Start each node with `--join` pointing to one or more seed addresses. Each node needs its own data directory.

**Node 1** (`192.168.1.101`):

```bash
./target/release/inferadb-ledger \
  --listen 192.168.1.101:50051 \
  --data /var/lib/ledger
```

**Node 2** (`192.168.1.102`):

```bash
./target/release/inferadb-ledger \
  --listen 192.168.1.102:50051 \
  --data /var/lib/ledger \
  --join 192.168.1.101:50051
```

**Node 3** (`192.168.1.103`):

```bash
./target/release/inferadb-ledger \
  --listen 192.168.1.103:50051 \
  --data /var/lib/ledger \
  --join 192.168.1.101:50051,192.168.1.102:50051
```

**Initialize the cluster (once):**

```bash
./target/release/inferadb-ledger init --host 192.168.1.101:50051
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
  --listen 192.168.1.104:50051 \
  --data /var/lib/ledger \
  --join 192.168.1.101:50051
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
  --listen 0.0.0.0:50051 \
  --data /var/lib/ledger \
  --join existing-node:50051
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
./target/release/inferadb-ledger --listen 0.0.0.0:50051 --data /var/lib/ledger

# Initialize the restored cluster
./target/release/inferadb-ledger init --host node1:50051

# Start remaining nodes with --join
./target/release/inferadb-ledger --listen 0.0.0.0:50051 --data /var/lib/ledger --join node1:50051
```

## Configuration

Every CLI flag and environment variable — with defaults, safe ranges, and security notes — is documented in [reference/configuration.md](../reference/configuration.md). The flags this guide uses (`--listen`, `--data`, `--join`, `--region`, `--metrics`, `--email-blinding-key`) are the minimum viable set for a cluster; `reference/configuration.md` covers the rest.

**Bootstrap in one line**: start every node with `--listen` + `--data` + (for joiners) `--join`, then run `inferadb-ledger init --host <any-node>` exactly once per cluster lifetime.

**Ephemeral mode**: pass `--dev` (instead of `--data <path>`) to run against an auto-generated tempdir; useful for development, unsuitable for production. Exactly one of `--data` or `--dev` is required — supplying neither or both returns a CLI parse error.

**Unix domain sockets**: `--socket /path/to/ledger.sock` exposes a UDS listener in addition to TCP. Both can run simultaneously.

Run `inferadb-ledger --help` for the full usage line.

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

See: [Request Routing](../architecture/request-routing.md).

## Kubernetes Deployment

For a guided first-deployment walkthrough with monitoring and alerts wired in, see the [production-deployment tutorial](../getting-started/production-deployment.md). This section covers Kubernetes deployment patterns in general — Helm, Kustomize, and raw manifests — for operators who already have a Kubernetes target and want to choose the right packaging approach.

The Kubernetes deployment uses:

- **StatefulSet** for stable pod identities and persistent storage.
- **Headless Service** for DNS-based peer discovery (`$pod.ledger-headless.namespace.svc.cluster.local`).
- **PodDisruptionBudget** to maintain Raft quorum during rolling updates.

Pods discover each other via `--join` pointing at the headless service FQDN (e.g., `--join ledger-headless.inferadb.svc.cluster.local:50051`). Run `inferadb-ledger init --host <any-pod>:50051` once after the StatefulSet is Ready.

### Helm (recommended)

```bash
helm install ledger ./deploy/helm/inferadb-ledger \
  --namespace inferadb \
  --create-namespace \
  --set replicaCount=3 \
  --set persistence.size=50Gi
```

`replicaCount` must be odd (1, 3, 5, 7) for Raft quorum. See `values.yaml` for all available options.

### Kustomize

```bash
kubectl apply -k deploy/kubernetes/
```

Use Kustomize when your pipeline already standardises on it or when your policy layer rejects arbitrary Helm templating. The `deploy/kubernetes/` base directory mirrors the Helm chart's default values.

### Raw Manifests (alternative to Helm)

Use this path if you can't use Helm (air-gapped registry, policy restrictions, bespoke manifest pipeline). Five resources compose the deployment.

**ConfigMap:**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ledger-config
  namespace: inferadb
data:
  INFERADB__LEDGER__LISTEN: "0.0.0.0:50051"
  INFERADB__LEDGER__METRICS: "0.0.0.0:9090"
  INFERADB__LEDGER__DATA: "/data"
  INFERADB__LEDGER__JOIN: "ledger-headless.inferadb.svc.cluster.local:50051"
```

**Headless Service:**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ledger-headless
  namespace: inferadb
spec:
  clusterIP: None
  selector:
    app: ledger
  ports:
    - name: grpc
      port: 50051
      targetPort: grpc
    - name: metrics
      port: 9090
      targetPort: metrics
```

**Client Service:**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ledger
  namespace: inferadb
spec:
  selector:
    app: ledger
  ports:
    - name: grpc
      port: 50051
      targetPort: grpc
```

**StatefulSet:**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ledger
  namespace: inferadb
spec:
  serviceName: ledger-headless
  replicas: 3
  selector:
    matchLabels:
      app: ledger
  template:
    metadata:
      labels:
        app: ledger
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: ledger
              topologyKey: kubernetes.io/hostname
      containers:
        - name: ledger
          image: inferadb/ledger:latest
          ports:
            - name: grpc
              containerPort: 50051
            - name: metrics
              containerPort: 9090
          envFrom:
            - configMapRef:
                name: ledger-config
          volumeMounts:
            - name: data
              mountPath: /data
          resources:
            requests:
              memory: "8Gi"
              cpu: "4"
            limits:
              memory: "16Gi"
              cpu: "8"
          livenessProbe:
            grpc:
              port: 50051
            initialDelaySeconds: 10
            periodSeconds: 10
          readinessProbe:
            grpc:
              port: 50051
            initialDelaySeconds: 5
            periodSeconds: 5
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 50Gi
```

**PodDisruptionBudget:**

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: ledger-pdb
  namespace: inferadb
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: ledger
```

Apply in order:

```bash
kubectl apply -f ledger-config.yaml
kubectl apply -f ledger-headless-service.yaml
kubectl apply -f ledger-client-service.yaml
kubectl apply -f ledger-statefulset.yaml
kubectl apply -f ledger-pdb.yaml
```

Once the StatefulSet reports 3/3 Ready, bootstrap the cluster:

```bash
kubectl -n inferadb exec ledger-0 -- inferadb-ledger init --host localhost:50051
```

### Ingress alternatives

The production-deployment tutorial's happy path uses **NGINX Ingress** for external gRPC. Alternatives for other cloud ingress controllers:

**AWS ALB (via ALB Ingress Controller):**

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ledger
  namespace: inferadb
  annotations:
    kubernetes.io/ingress.class: alb
    alb.ingress.kubernetes.io/scheme: internet-facing
    alb.ingress.kubernetes.io/target-type: ip
    alb.ingress.kubernetes.io/backend-protocol-version: GRPC
spec:
  rules:
    - host: ledger.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: ledger
                port:
                  number: 50051
```

**GKE (GCE Ingress)**: gRPC support requires an HTTP/2 backend via `cloud.google.com/app-protocols` on the Service. See the [GKE gRPC load-balancing docs](https://cloud.google.com/kubernetes-engine/docs/how-to/ingress-configuration).

**Azure (AGIC)**: Requires `appgw-ingress-class`; set `appgw.ingress.kubernetes.io/backend-protocol: http2` on the Ingress.

All three need the cluster's `:50051` port reachable from the target nodes of the ingress fleet. Remember that under redirect-based routing, clients ultimately connect directly to individual cluster nodes — the ingress is used at cold start for discovery only.

See [`deploy/kubernetes/`](../../deploy/kubernetes/) for raw manifests and [`deploy/helm/`](../../deploy/helm/) for the Helm chart.
