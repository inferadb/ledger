[Documentation](../README.md) > Internals > Discovery

# Discovery & Coordination

This document covers the `_system` namespace, bootstrap discovery, node lifecycle, and namespace routing.

## System Namespace

A dedicated `_system` Raft group serves as the authoritative service registry, replicated to all nodes.

**Properties:**

- O(1) lookups from local cache
- Strongly consistent (Raft consensus)
- No additional dependencies beyond Raft
- WireGuard provides all necessary security

### Data Model

The `_system` namespace stores individual entities (not a unified struct):

```rust
struct NamespaceRegistry {
    namespace_id: NamespaceId,
    name: String,
    shard_id: ShardId,
    member_nodes: Vec<NodeId>,
    status: NamespaceStatus,
    config_version: u64,
    created_at: DateTime<Utc>,
}

struct NodeInfo {
    node_id: NodeId,
    addresses: Vec<SocketAddr>,
    grpc_port: u16,
    role: NodeRole,               // Voter or Learner
    last_heartbeat: DateTime<Utc>,
    joined_at: DateTime<Utc>,
}
```

Note: `leader_hint` is computed dynamically from Raft state, not stored in `NamespaceRegistry`.

### Voter/Learner Scaling

Raft quorum size limits voter count. Large clusters use a fixed voter set with learners:

| Cluster Size | Voters | Learners | Quorum  |
| ------------ | ------ | -------- | ------- |
| 1-5 nodes    | all    | 0        | (n/2)+1 |
| 6-10 nodes   | 5      | rest     | 3       |
| 11+ nodes    | 5      | rest     | 3       |

**Voter election**: When cluster exceeds 5 nodes, the 5 oldest nodes (by join time) become voters. On voter departure, next-oldest learner promotes.

### Learner Staleness

Learners receive updates via Raft replication but may lag:

```rust
struct LearnerCacheConfig {
    cache_ttl: Duration,        // Default: 5s
    refresh_interval: Duration, // Default: 1s
}
```

| Role                  | Local Query       | Consistency        | Latency    |
| --------------------- | ----------------- | ------------------ | ---------- |
| Voter                 | Always valid      | Linearizable       | O(1)       |
| Learner (fresh cache) | Valid             | Eventual (≤5s lag) | O(1)       |
| Learner (stale cache) | Fallback to voter | Linearizable       | O(network) |

## Bootstrap Discovery

Nodes discover initial peers through multiple mechanisms:

```
1. Cached peers? ─── Found ──→ Connected to cluster
       │
       └─ Empty ──→ 2. DNS A record lookup? ─── Found ──→ Connected
                              │
                              └─ Failed ──→ Bootstrap failed
```

### DNS A Records

DNS A records enable dynamic bootstrap node management. This is optimized for Kubernetes headless Services:

**Kubernetes (recommended):**

```yaml
# Headless Service creates DNS A records for each pod
apiVersion: v1
kind: Service
metadata:
  name: ledger
spec:
  clusterIP: None # headless
  selector:
    app: ledger
  ports:
    - port: 50051
```

Query `ledger.default.svc.cluster.local` returns A records for all pod IPs.

**Non-Kubernetes environments:**

```
# DNS A records (all nodes share the same name)
ledger.cluster.example.com. 300 IN A 192.168.1.101
ledger.cluster.example.com. 300 IN A 192.168.1.102
ledger.cluster.example.com. 300 IN A 192.168.1.103
```

**Benefits:**

- Update bootstrap nodes via DNS without client reconfiguration
- Native Kubernetes headless Service support
- TTL-based caching reduces DNS load
- Works with split-horizon DNS for different environments

### Peer Exchange Protocol

```protobuf
service SystemDiscoveryService {
    rpc GetPeers(GetPeersRequest) returns (GetPeersResponse);
    rpc AnnouncePeer(AnnouncePeerRequest) returns (AnnouncePeerResponse);
    rpc GetSystemState(GetSystemStateRequest) returns (GetSystemStateResponse);
}
```

**Behavior:**

- On connection to any node: Call `GetPeers()`, merge with local cache, persist
- Periodic refresh (every 5 minutes): Query random subset, merge, prune peers not seen in >1 hour
- On `_system` state change: Broadcast `AnnouncePeer()` to connected nodes

## Node Join

```
New Node Startup:
  1. Resolve bootstrap peers (cached → DNS A lookup)
  2. Connect to ANY reachable peer
  3. Call GetPeers() to discover more nodes
  4. Request _system Raft membership (AddNode)
  5. Existing nodes vote on membership change
  6. Once accepted, node receives full _system log
  7. Node persists peer list locally (survives restarts)

Namespace Assignment:
  1. Control creates namespace via CreateNamespace(name) → returns leader-assigned NamespaceId
  2. _system assigns namespace to shard with lowest load
  3. Shard leader initializes namespace state
  4. Shard leader updates _system with NamespaceRegistry
```

## Node Leave

### Graceful Departure

```
1. Node announces intention to leave via LeaveCluster RPC
2. _system leader proposes RemoveNode to Raft
3. Node is removed from _system membership
4. Shard Raft groups handle the departure:
   - If node was shard member, Raft reconfigures
   - If node was shard leader, triggers election
5. Other nodes update cached peer lists
```

### Unexpected Departure

```
1. _system leader detects missing heartbeats (default: 30s timeout)
2. Leader proposes RemoveNode after timeout
3. Node marked as unavailable in _system state
4. Vault Raft groups detect member failure, continue with remaining quorum
5. If departed node returns, it must rejoin as new member
```

## Membership Reconfiguration

Ledger uses Openraft's joint consensus with additional safety constraints.

### Joint Consensus

Configuration change `[A, B, C] → [A, B, D]`:

```
1. Leader proposes C_old,new = [{A,B,C}, {A,B,D}] (joint config)
2. Joint config committed when majority of BOTH old AND new configs agree
3. Leader proposes C_new = [A,B,D] (final config)
4. Final config committed, old config members can be safely removed
```

Single-step membership changes can create disjoint majorities during leader failures. Joint consensus ensures at most one valid configuration can achieve quorum.

### Safety Constraints

| Constraint                                 | Enforcement                            |
| ------------------------------------------ | -------------------------------------- |
| No concurrent membership changes per shard | Shard-level mutex                      |
| Learner must sync before promotion         | `wait_for_log_sync()` check            |
| No-op after leader election                | Openraft built-in                      |
| Joint consensus required                   | Openraft-only mode                     |
| Minimum quorum maintained                  | Reject changes that would break quorum |

### Quorum Protection

```rust
fn validate_membership_change(current: &[NodeId], proposed: &[NodeId]) -> Result<()> {
    let current_quorum = (current.len() / 2) + 1;

    // Can't remove nodes if it would break quorum
    if proposed.len() < current_quorum {
        return Err(MembershipError::WouldBreakQuorum);
    }

    // Can't add more than one node at a time
    let added = proposed.len().saturating_sub(current.len());
    if added > 1 {
        return Err(MembershipError::TooManyAdditions);
    }

    Ok(())
}
```

## Namespace Routing

Clients route requests to the correct shard by looking up namespace → shard mappings in `_system`.

```
Control request for namespace_id=1
       │
       ▼
Local cache? ─── Hit ──→ Fresh? ─── Yes ──→ Connect to shard leader
       │                    │
       └─ Miss              └─ No
            │                    │
            ▼                    ▼
    Query _system: ns:1 ────────┘
            │
            ▼
    Connect to shard leader
            │
            ▼
    Success? ─── Yes ──→ Proceed with request
       │
       └─ NotLeader ──→ Follow redirect, update cache
```

## Client Discovery

```rust
impl Client {
    async fn bootstrap(config: &ClientConfig) -> Result<Self> {
        // Try bootstrap nodes from config
        for addr in &config.bootstrap_nodes {
            if let Ok(conn) = try_connect(addr).await {
                let system = conn.get_system_state().await?;
                return Ok(Client { conn, system_cache: system, .. });
            }
        }

        // Fallback: cached nodes from previous session
        if let Some(cached) = config.load_cached_nodes() {
            for node in cached.nodes.values() {
                if let Ok(conn) = try_connect(&node.addresses[0]).await {
                    let system = conn.get_system_state().await?;
                    return Ok(Client { conn, system_cache: system, .. });
                }
            }
        }

        Err(ClusterUnavailable)
    }
}
```

## Peer-to-Peer Properties

Fully decentralized:

- **No privileged nodes**: Any node can serve discovery queries, bootstrap new nodes, or become `_system` leader
- **Bootstrap nodes are entry points, not authorities**: After joining, nodes discover all peers
- **Dynamic peer list**: Nodes cache peers from `_system` state, surviving bootstrap node failures
- **Symmetric roles**: All nodes participate equally in `_system` consensus

## Failure Modes

| Scenario                    | Behavior                                                 |
| --------------------------- | -------------------------------------------------------- |
| All bootstrap nodes down    | Control uses cached peer list from previous session      |
| Namespace shard unknown     | Query any node's `_system` state for routing             |
| Network partition           | Majority partition continues; minority becomes read-only |
| Node crashes                | Removed from `_system` after 30s heartbeat timeout       |
| Node gracefully leaves      | Immediately removed from `_system`, Raft reconfigures    |
| `_system` leader fails      | Raft elects new leader automatically (<500ms)            |
| Majority of nodes fail      | Cluster halts until quorum restored                      |
| Namespace shard unavailable | Requests for that namespace fail; others unaffected      |
