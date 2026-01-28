[Documentation](../README.md) > Internals > Raft Protocol

# Raft Protocol

Internal gRPC protocol for Raft consensus between nodes. This is **not** a client API—it handles node-to-node communication for leader election, log replication, and snapshot transfer.

For higher-level Raft concepts, see [Consensus](consensus.md).

## Service Definition

```protobuf
service RaftService {
  // Request vote from peer during leader election.
  rpc Vote(RaftVoteRequest) returns (RaftVoteResponse);

  // Replicate log entries to followers.
  rpc AppendEntries(RaftAppendEntriesRequest) returns (RaftAppendEntriesResponse);

  // Install snapshot on a follower that is too far behind.
  rpc InstallSnapshot(RaftInstallSnapshotRequest) returns (RaftInstallSnapshotResponse);
}
```

## RPCs

### Vote

Sent by candidates to request votes during leader election. Followers grant votes to candidates with logs at least as up-to-date as their own.

**Request:**

| Field         | Type      | Description                           |
| ------------- | --------- | ------------------------------------- |
| `vote`        | RaftVote  | Candidate's term, node ID, commitment |
| `last_log_id` | RaftLogId | (Optional) Candidate's last log entry |
| `shard_id`    | uint64    | (Optional) Target shard (default: 0)  |

**Response:**

| Field          | Type      | Description                           |
| -------------- | --------- | ------------------------------------- |
| `vote`         | RaftVote  | Responder's current term and node ID  |
| `vote_granted` | bool      | True if vote granted to candidate     |
| `last_log_id`  | RaftLogId | (Optional) Responder's last log entry |

### AppendEntries

Sent by leaders to replicate log entries and maintain heartbeats. Empty `entries` acts as a heartbeat.

**Request:**

| Field           | Type      | Description                            |
| --------------- | --------- | -------------------------------------- |
| `vote`          | RaftVote  | Leader's term and node ID              |
| `prev_log_id`   | RaftLogId | (Optional) Entry immediately preceding |
| `entries`       | bytes[]   | Serialized log entries to append       |
| `leader_commit` | RaftLogId | (Optional) Leader's commit index       |
| `shard_id`      | uint64    | (Optional) Target shard (default: 0)   |

**Response:**

| Field      | Type     | Description                         |
| ---------- | -------- | ----------------------------------- |
| `vote`     | RaftVote | Responder's current term            |
| `success`  | bool     | True if entries successfully stored |
| `conflict` | bool     | True if log conflict detected       |

### InstallSnapshot

Sent by leaders to followers that are too far behind to catch up via log replay. Transfers a snapshot in chunks.

**Request:**

| Field      | Type             | Description                          |
| ---------- | ---------------- | ------------------------------------ |
| `vote`     | RaftVote         | Leader's term and node ID            |
| `meta`     | RaftSnapshotMeta | Snapshot metadata                    |
| `offset`   | uint64           | Byte offset in snapshot data         |
| `data`     | bytes            | Chunk of snapshot data               |
| `done`     | bool             | True if this is the final chunk      |
| `shard_id` | uint64           | (Optional) Target shard (default: 0) |

**Response:**

| Field  | Type     | Description              |
| ------ | -------- | ------------------------ |
| `vote` | RaftVote | Responder's current term |

## Message Types

### RaftVote

Represents a node's vote in Raft (term + node ID + committed flag).

| Field       | Type   | Description                              |
| ----------- | ------ | ---------------------------------------- |
| `term`      | uint64 | Raft term number                         |
| `node_id`   | uint64 | Node's unique identifier                 |
| `committed` | bool   | Whether the vote is committed (pre-vote) |

### RaftLogId

Identifies a specific log entry by term and index.

| Field   | Type   | Description       |
| ------- | ------ | ----------------- |
| `term`  | uint64 | Term when created |
| `index` | uint64 | Position in log   |

### RaftSnapshotMeta

Metadata for a snapshot.

| Field             | Type           | Description                    |
| ----------------- | -------------- | ------------------------------ |
| `last_log_id`     | RaftLogId      | (Optional) Last included entry |
| `last_membership` | RaftMembership | Cluster membership at snapshot |
| `snapshot_id`     | string         | Unique identifier for snapshot |

### RaftMembership

Cluster membership configuration. Supports joint consensus with multiple configurations during membership changes.

| Field     | Type                   | Description                      |
| --------- | ---------------------- | -------------------------------- |
| `configs` | RaftMembershipConfig[] | Active membership configurations |

### RaftMembershipConfig

Single membership configuration.

| Field     | Type                | Description                     |
| --------- | ------------------- | ------------------------------- |
| `members` | map<uint64, string> | Node ID to gRPC address mapping |

## Multi-Shard Routing

The optional `shard_id` field enables a single cluster to run multiple independent Raft groups. Each shard maintains its own:

- Leader election
- Log sequence
- Committed index
- Membership

Nodes receiving a request for a shard they don't host return `NOT_FOUND`.

## Typical Message Flow

### Leader Election

```
1. Follower timeout expires, becomes candidate
2. Candidate: Vote(term=T, node_id=N) → all peers
3. Peers: Compare logs, grant/deny vote
4. Candidate with majority: becomes leader
5. Leader: AppendEntries(entries=[]) → heartbeat to all
```

### Log Replication

```
1. Client → Leader: WriteRequest
2. Leader: Append to local log
3. Leader: AppendEntries(entries=[E]) → all followers
4. Followers: Append to log, respond success
5. Leader: Quorum acks → commit entry
6. Leader: Next AppendEntries includes leader_commit
7. Followers: Apply committed entries to state machine
```

### Snapshot Installation

```
1. Leader detects follower is too far behind
2. Leader: InstallSnapshot(chunk=1, offset=0) → follower
3. Follower: Write chunk to temp file
4. ... repeat for all chunks ...
5. Leader: InstallSnapshot(done=true) → follower
6. Follower: Apply snapshot, truncate log
7. Follower: Resume normal AppendEntries
```

## Related Documentation

- [Consensus](consensus.md) - Raft integration, write/read paths, batching
- [Discovery](discovery.md) - Peer discovery and cluster formation
- [AdminService](../client/admin.md) - Cluster management operations
