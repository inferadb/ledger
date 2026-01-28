[Documentation](../README.md) > Client API > SystemDiscoveryService

# SystemDiscoveryService API

Peer discovery and system state for cluster coordination.

## Service Definition

```protobuf
service SystemDiscoveryService {
  rpc GetPeers(GetPeersRequest) returns (GetPeersResponse);
  rpc AnnouncePeer(AnnouncePeerRequest) returns (AnnouncePeerResponse);
  rpc GetSystemState(GetSystemStateRequest) returns (GetSystemStateResponse);
}
```

## Overview

SystemDiscoveryService enables nodes to discover each other and coordinate cluster operations. It provides:

- Peer discovery for new nodes joining
- System state including node registry and namespace routing
- Cache invalidation via version tracking

## GetPeers

Returns the list of known peers in the cluster.

```bash
grpcurl -plaintext localhost:50051 ledger.v1.SystemDiscoveryService/GetPeers

# Limit response size
grpcurl -plaintext \
  -d '{"max_peers": 10}' \
  localhost:50051 ledger.v1.SystemDiscoveryService/GetPeers
```

**Request:**

| Field       | Type   | Description                    |
| ----------- | ------ | ------------------------------ |
| `max_peers` | uint32 | (Optional) Limit response size |

**Response:**

| Field            | Type       | Description            |
| ---------------- | ---------- | ---------------------- |
| `peers`          | PeerInfo[] | Known cluster peers    |
| `system_version` | uint64     | For cache invalidation |

**PeerInfo fields:**

| Field       | Type      | Description                           |
| ----------- | --------- | ------------------------------------- |
| `node_id`   | NodeId    | Node identifier                       |
| `addresses` | string[]  | IP addresses (private/WireGuard only) |
| `grpc_port` | uint32    | gRPC port (typically 50051)           |
| `last_seen` | Timestamp | Last successful communication         |

**Example response:**

```json
{
  "peers": [
    {
      "node_id": { "id": "7394820571234567890" },
      "addresses": ["10.0.0.1", "10.0.0.2"],
      "grpc_port": 50051,
      "last_seen": "2026-01-28T10:30:00Z"
    }
  ],
  "system_version": 42
}
```

To connect to a peer: `addresses[i]:grpc_port` (e.g., `10.0.0.1:50051`)

## AnnouncePeer

Announces a peer's presence to the cluster.

```bash
grpcurl -plaintext \
  -d '{"peer": {"node_id": {"id": "123"}, "addresses": ["10.0.0.4"], "grpc_port": 50051}}' \
  localhost:50051 ledger.v1.SystemDiscoveryService/AnnouncePeer
```

**Request:**

| Field  | Type     | Description      |
| ------ | -------- | ---------------- |
| `peer` | PeerInfo | Peer to announce |

**Response:**

| Field      | Type | Description                   |
| ---------- | ---- | ----------------------------- |
| `accepted` | bool | Whether announcement accepted |

## GetSystemState

Returns full system state including nodes and namespace routing.

```bash
grpcurl -plaintext localhost:50051 ledger.v1.SystemDiscoveryService/GetSystemState

# With cache optimization (only return if version changed)
grpcurl -plaintext \
  -d '{"if_version_greater_than": 41}' \
  localhost:50051 ledger.v1.SystemDiscoveryService/GetSystemState
```

**Request:**

| Field                     | Type   | Description                             |
| ------------------------- | ------ | --------------------------------------- |
| `if_version_greater_than` | uint64 | Return empty if version <= this (cache) |

**Response:**

| Field        | Type                | Description                  |
| ------------ | ------------------- | ---------------------------- |
| `version`    | uint64              | Current system state version |
| `nodes`      | NodeInfo[]          | All cluster nodes            |
| `namespaces` | NamespaceRegistry[] | Namespace → shard routing    |

**NodeInfo fields:**

| Field            | Type      | Description                           |
| ---------------- | --------- | ------------------------------------- |
| `node_id`        | NodeId    | Node identifier                       |
| `addresses`      | string[]  | IP addresses (private/WireGuard only) |
| `grpc_port`      | uint32    | gRPC port                             |
| `role`           | NodeRole  | Cluster role (VOTER or LEARNER)       |
| `last_heartbeat` | Timestamp | Last heartbeat received               |
| `joined_at`      | Timestamp | When node joined cluster              |

**NodeRole values:**

| Value               | Description                            |
| ------------------- | -------------------------------------- |
| `NODE_ROLE_VOTER`   | Full voting member (max 5 per cluster) |
| `NODE_ROLE_LEARNER` | Replicates data but doesn't vote       |

**NamespaceRegistry fields:**

| Field            | Type            | Description            |
| ---------------- | --------------- | ---------------------- |
| `namespace_id`   | NamespaceId     | Namespace identifier   |
| `name`           | string          | Human-readable name    |
| `shard_id`       | ShardId         | Hosting Raft group     |
| `members`        | NodeId[]        | Nodes in the shard     |
| `status`         | NamespaceStatus | Lifecycle state        |
| `config_version` | uint64          | For cache invalidation |
| `created_at`     | Timestamp       | Creation time          |

## Bootstrap Sequence

During coordinated bootstrap (`--cluster N`):

1. **DNS discovery**: Each node resolves `--peers` to find peer IPs
2. **GetNodeInfo exchange**: Nodes call `AdminService/GetNodeInfo` on discovered peers
3. **Leader election**: Lowest Snowflake ID becomes bootstrap leader
4. **Cluster formation**: Bootstrap leader initializes Raft, others join

```
Node 1 (ID: 100)          Node 2 (ID: 200)          Node 3 (ID: 300)
      │                         │                         │
      │ ◄──GetNodeInfo─────────│                         │
      │ ◄──GetNodeInfo────────────────────────────────│
      │                         │ ◄──GetNodeInfo─────────│
      │                         │                         │
      │     [Quorum: 3/3]       │     [Quorum: 3/3]       │
      │                         │                         │
      │  Bootstrap Leader       │                         │
      │  (lowest ID: 100)       │                         │
      │                         │                         │
      │──────JoinCluster───────►│                         │
      │──────JoinCluster───────────────────────────────►│
      │                         │                         │
      │     [RUNNING]           │     [RUNNING]           │     [RUNNING]
```

## Kubernetes DNS Discovery

In Kubernetes, nodes discover peers through the headless service:

```yaml
env:
  - name: INFERADB__LEDGER__PEERS
    value: "ledger-headless.inferadb.svc.cluster.local"
```

Ledger resolves this DNS name to get all pod IPs, then calls `GetNodeInfo` on each.

## See Also

- [Deployment Guide](../operations/deployment.md) - Cluster bootstrap options
- [Configuration](../operations/configuration.md) - Peer discovery settings
