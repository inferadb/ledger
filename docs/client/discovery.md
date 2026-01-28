# SystemDiscoveryService API

Peer discovery and cluster bootstrap coordination.

## Service Definition

```protobuf
service SystemDiscoveryService {
  rpc GetPeers(GetPeersRequest) returns (GetPeersResponse);
  rpc AnnouncePeer(AnnouncePeerRequest) returns (AnnouncePeerResponse);
  rpc GetSystemState(GetSystemStateRequest) returns (GetSystemStateResponse);
}
```

## Overview

SystemDiscoveryService enables nodes to discover each other during cluster bootstrap. It's used internally by Ledger and typically not called by external clients.

## GetPeers

Returns the list of known peers in the cluster.

```bash
grpcurl -plaintext localhost:50051 ledger.v1.SystemDiscoveryService/GetPeers
```

**Response:**

| Field   | Type       | Description             |
| ------- | ---------- | ----------------------- |
| `peers` | PeerInfo[] | All known cluster peers |

**PeerInfo fields:**

| Field     | Type   | Description               |
| --------- | ------ | ------------------------- |
| `node_id` | NodeId | Snowflake node identifier |
| `addr`    | string | gRPC address (host:port)  |
| `role`    | Role   | Node role in cluster      |

**Role values:**

| Value      | Description                   |
| ---------- | ----------------------------- |
| `VOTER`    | Full cluster member with vote |
| `LEARNER`  | Replicating but non-voting    |
| `OBSERVER` | Read-only, no replication     |

## AnnouncePeer

Announces this node's presence to another peer during bootstrap.

```bash
grpcurl -plaintext \
  -d '{"node_id": {"id": "123"}, "addr": "10.0.0.4:50051"}' \
  localhost:50051 ledger.v1.SystemDiscoveryService/AnnouncePeer
```

**Request:**

| Field     | Type   | Description               |
| --------- | ------ | ------------------------- |
| `node_id` | NodeId | Announcing node's ID      |
| `addr`    | string | Announcing node's address |

**Response:**

| Field      | Type       | Description                   |
| ---------- | ---------- | ----------------------------- |
| `accepted` | bool       | Whether announcement accepted |
| `peers`    | PeerInfo[] | Current known peers           |

## GetSystemState

Returns the current system state including bootstrap status.

```bash
grpcurl -plaintext localhost:50051 ledger.v1.SystemDiscoveryService/GetSystemState
```

**Response:**

| Field              | Type        | Description                 |
| ------------------ | ----------- | --------------------------- |
| `state`            | SystemState | Current system state        |
| `node_id`          | NodeId      | This node's ID              |
| `cluster_size`     | uint32      | Expected cluster size       |
| `discovered_peers` | uint32      | Number of discovered peers  |
| `leader_id`        | NodeId      | Current leader (if elected) |

**SystemState values:**

| Value           | Description                            |
| --------------- | -------------------------------------- |
| `INITIALIZING`  | Node starting, loading state           |
| `DISCOVERING`   | Discovering peers, waiting for quorum  |
| `BOOTSTRAPPING` | Bootstrap in progress, electing leader |
| `RUNNING`       | Cluster operational                    |
| `JOINING`       | Node joining existing cluster          |

## Bootstrap Sequence

1. **Node starts**: Each node generates a Snowflake ID and enters `DISCOVERING` state
2. **Peer discovery**: Nodes call `AnnouncePeer` on discovered addresses
3. **Quorum reached**: When `discovered_peers >= cluster_size`, lowest ID becomes bootstrap leader
4. **Bootstrap**: Leader initializes Raft, others join as voters
5. **Running**: All nodes enter `RUNNING` state

```
Node 1 (ID: 100)          Node 2 (ID: 200)          Node 3 (ID: 300)
      │                         │                         │
      │ ◄──AnnouncePeer────────│                         │
      │ ◄──AnnouncePeer────────────────────────────────│
      │                         │ ◄──AnnouncePeer────────│
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

Ledger resolves this DNS name to get all pod IPs, then calls `AnnouncePeer` on each.

## Monitoring

Track discovery with these metrics:

| Metric                              | Description                |
| ----------------------------------- | -------------------------- |
| `ledger_discovery_peers_discovered` | Number of discovered peers |
| `ledger_discovery_announcements`    | Peer announcement attempts |
| `ledger_bootstrap_duration_seconds` | Time to complete bootstrap |
