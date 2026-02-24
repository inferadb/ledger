# Shard Management

Shards are Raft groups that host one or more organizations. This guide covers shard concepts and management.

## Overview

```
┌─────────────────────────────────────────────────────────────┐
│                         Cluster                              │
│                                                              │
│   ┌─────────────────────┐     ┌─────────────────────┐       │
│   │     Shard 0         │     │     Shard 1         │       │
│   │  (System Shard)     │     │                     │       │
│   │                     │     │                     │       │
│   │  ┌───────────────┐  │     │  ┌───────────────┐  │       │
│   │  │ Organization A   │  │     │  │ Organization C   │  │       │
│   │  │ (vaults 1-3)  │  │     │  │ (vaults 1-2)  │  │       │
│   │  └───────────────┘  │     │  └───────────────┘  │       │
│   │                     │     │                     │       │
│   │  ┌───────────────┐  │     │  ┌───────────────┐  │       │
│   │  │ Organization B   │  │     │  │ Organization D   │  │       │
│   │  │ (vaults 1-5)  │  │     │  │ (vaults 1-10) │  │       │
│   │  └───────────────┘  │     │  └───────────────┘  │       │
│   │                     │     │                     │       │
│   │  Nodes: 1, 2, 3     │     │  Nodes: 4, 5, 6     │       │
│   └─────────────────────┘     └─────────────────────┘       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**Key concepts:**

- **Shard**: A Raft group with its own leader election and consensus
- **Organization**: Isolated storage unit assigned to exactly one shard
- **Vault**: Blockchain within an organization
- **System Shard (Shard 0)**: Hosts cluster metadata and routing tables

## Shard Architecture

### Single-Shard Deployment (Default)

Most deployments use a single shard (Shard 0) for all organizations:

```
┌─────────────────────────────────────┐
│            Shard 0                  │
│                                     │
│  All organizations, all vaults         │
│  Nodes: all cluster nodes           │
│                                     │
└─────────────────────────────────────┘
```

**Benefits:**

- Simpler operations
- Single leader handles all writes
- Automatic failover across all data

**Limitations:**

- Write throughput limited by single leader
- All data must fit on cluster nodes

### Multi-Shard Deployment

For larger deployments, organizations can be distributed across multiple shards:

**Benefits:**

- Horizontal write scalability
- Data partitioning for large datasets
- Independent failure domains

**When to use:**

- Write throughput exceeds single-leader capacity
- Data exceeds single-node storage
- Regulatory requirements for data isolation

## Organization-to-Shard Assignment

### Automatic Assignment

By default, organizations are assigned to the shard with lowest load:

```bash
grpcurl -plaintext \
  -d '{"name": "acme_corp"}' \
  localhost:50051 ledger.v1.AdminService/CreateOrganization
```

Response includes assigned shard:

```json
{
  "organization_slug": { "id": "1" },
  "shard_id": { "id": 0 }
}
```

### Explicit Assignment

Specify a shard when creating an organization:

```bash
grpcurl -plaintext \
  -d '{"name": "acme_corp", "shard_id": {"id": 1}}' \
  localhost:50051 ledger.v1.AdminService/CreateOrganization
```

This is useful for:

- Data locality requirements
- Performance isolation
- Compliance (geographic data residency)

## Viewing Shard Information

### Get Organization Shard

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/GetOrganization
```

Response:

```json
{
  "organization_slug": { "id": "1" },
  "name": "acme_corp",
  "shard_id": { "id": 0 },
  "member_nodes": [
    { "id": "123456789" },
    { "id": "234567890" },
    { "id": "345678901" }
  ],
  "status": "ORGANIZATION_STATUS_ACTIVE"
}
```

### Get System State

View all organization-to-shard assignments:

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.SystemDiscoveryService/GetSystemState
```

Response includes routing table:

```json
{
  "version": 42,
  "nodes": [...],
  "organizations": [
    {
      "organization_slug": {"id": "1"},
      "name": "acme_corp",
      "shard_id": {"id": 0},
      "members": [{"id": "123"}, {"id": "234"}, {"id": "345"}],
      "status": "ORGANIZATION_STATUS_ACTIVE"
    },
    {
      "organization_slug": {"id": "2"},
      "name": "other_org",
      "shard_id": {"id": 1},
      "members": [{"id": "456"}, {"id": "567"}, {"id": "678"}],
      "status": "ORGANIZATION_STATUS_ACTIVE"
    }
  ]
}
```

## Write Forwarding

In multi-shard deployments, the `MultiShardWriteService` transparently forwards writes to the correct shard leader. Clients can send write requests to **any node** in the cluster — if the target organization lives on a different shard, the receiving node forwards the request to the correct shard's leader.

```
Client → Node A (Shard 0) → resolve_with_forward(org) → Shard 1 Leader → Response → Node A → Client
```

**Behavior:**

- The originating node performs pre-flight validation (rate limiting, input validation, idempotency check) before forwarding
- The destination shard leader performs vault slug resolution and Raft proposal
- Forwarding is transparent to clients — the response is relayed back through the originating node
- If the destination leader is unavailable, the client receives `UNAVAILABLE` and can retry

This means clients do not need shard-aware routing for writes. Any node accepts writes for any organization. For reads, the same forwarding applies via `MultiShardReadService`.

## Client Routing

Clients use the routing table to send requests to the correct shard:

1. **Cache system state** from `GetSystemState`
2. **Look up shard** for target organization
3. **Send request** to any node in that shard
4. **Invalidate cache** when `system_version` changes

Example client routing:

```rust
// Fetch routing table
let state = client.get_system_state().await?;

// Find shard for organization
let organization_entry = state.organizations
    .iter()
    .find(|ns| ns.organization_slug == target_organization_slug)
    .ok_or(Error::OrganizationNotFound)?;

let shard_members = &organization_entry.members;

// Connect to any member of the shard
let node = shard_members.choose(&mut rand::thread_rng());
let connection = connect_to_node(node)?;

// Send request
connection.write(request).await?;
```

### Cache Invalidation

Check `system_version` to detect routing changes:

```bash
grpcurl -plaintext \
  -d '{"if_version_greater_than": 41}' \
  localhost:50051 ledger.v1.SystemDiscoveryService/GetSystemState
```

If version <= 41, response is empty (use cached data).
If version > 41, response contains updated routing table.

## Shard Membership

### Node Roles

Each shard has its own Raft group with:

- **Voters**: Full voting members (max 5 per shard)
- **Learners**: Replicate data but don't vote (for read scaling)

### Adding Nodes to a Shard

Nodes are added to shards when:

1. Cluster bootstrap assigns initial shard membership
2. `JoinCluster` RPC adds a node to the system shard
3. Organization creation on a new shard triggers shard formation

### Shard Rebalancing

Currently, organization-to-shard assignment is permanent. Rebalancing requires:

1. Create new organization on target shard
2. Migrate data (application-level)
3. Update client routing
4. Delete old organization

Future versions may support automated shard migration.

## Capacity Planning

### Sizing Shards

| Metric               | Single Shard Limit | Recommendation         |
| -------------------- | ------------------ | ---------------------- |
| Write throughput     | ~10k ops/sec       | Add shard at 80%       |
| Total entities       | ~100M              | Add shard at 80M       |
| Organizations per shard | Unlimited          | 100 for manageability  |
| Nodes per shard      | 3-7 voters         | 3 for most deployments |

### When to Add Shards

Monitor these metrics:

```promql
# Write latency trending up
histogram_quantile(0.99, rate(ledger_write_latency_seconds_bucket[5m])) > 0.05

# Proposal backlog building
inferadb_ledger_raft_proposals_pending > 30
```

## Best Practices

### Organization Placement

- **Correlated data**: Same shard for organizations that are queried together
- **High-volume organizations**: Isolate to dedicated shards
- **Geographic requirements**: Place on region-local shards

### Shard Sizing

- Start with single shard
- Add shards when write throughput requires
- Keep shard count minimal for operational simplicity

### Monitoring

Track per-shard metrics:

```promql
# Leader per shard
sum by (shard_id) (inferadb_ledger_raft_is_leader)

# Write latency per shard
histogram_quantile(0.99, rate(ledger_write_latency_seconds_bucket[5m])) by (shard_id)
```

## See Also

- [Deployment Guide](deployment.md) - Cluster setup
- [Capacity Planning](capacity-planning.md) - Sizing guidelines
- [SystemDiscoveryService](../client/discovery.md) - Routing API
