# Capacity Planning

Guidelines for sizing Ledger deployments.

## Resource Requirements

### Minimum (Development)

| Resource | Requirement |
| -------- | ----------- |
| CPU      | 1 core      |
| Memory   | 512 MB      |
| Disk     | 1 GB SSD    |
| Network  | 100 Mbps    |

### Recommended (Production)

| Resource | 3-Node Cluster       | 5-Node Cluster       |
| -------- | -------------------- | -------------------- |
| CPU      | 4 cores/node         | 4 cores/node         |
| Memory   | 8 GB/node            | 8 GB/node            |
| Disk     | 100 GB NVMe SSD/node | 100 GB NVMe SSD/node |
| Network  | 1 Gbps               | 1 Gbps               |

## Sizing Calculations

### Storage

Storage grows with:

1. **Entities**: Key-value pairs
2. **Relationships**: Authorization tuples
3. **Block archive**: Transaction history (if FULL retention)
4. **Snapshots**: Periodic state snapshots

Formula (per vault):

```
Storage = (entities × avg_entity_size) +
          (relationships × ~100 bytes) +
          (blocks × avg_block_size) +
          (snapshots × snapshot_size)
```

**Example**:

- 1M entities at 500 bytes avg = 500 MB
- 10M relationships at 100 bytes = 1 GB
- 30 days of blocks at 10 MB/day = 300 MB
- 2 snapshots at 1.5 GB each = 3 GB
- **Total**: ~5 GB per vault

### Memory

Memory usage:

1. **State machine**: Active entities and relationships
2. **Raft log buffer**: In-flight proposals
3. **Idempotency cache**: Recent client operations
4. **B+ tree cache**: Hot pages

Rough sizing:

```
Memory = base (200 MB) +
         state_cache (entities × 0.5 KB) +
         raft_buffer (max_proposals × 10 KB) +
         idempotency (cache_entries × 1 KB)
```

**Example for 1M entities**:

- Base: 200 MB
- State: 500 MB
- Raft: 100 MB (10K proposals)
- Idempotency: 100 MB (100K entries)
- **Total**: ~1 GB active memory

### CPU

CPU is consumed by:

1. **Serialization**: Protobuf ↔ internal types ↔ storage
2. **Cryptography**: SHA-256 for state roots and proofs
3. **Raft consensus**: Heartbeats, log replication
4. **State root computation**: Merkle tree updates

Throughput per core (approximate):

- Writes: 5,000-10,000/second
- Reads: 50,000-100,000/second
- State root computation: <1ms per block

### Network

Network bandwidth between nodes:

```
Bandwidth = (writes/sec × avg_write_size × replication_factor) +
            (heartbeat_rate × heartbeat_size × nodes)
```

**Example for 5,000 writes/sec**:

- Writes: 5,000 × 500 bytes × 3 = 7.5 MB/s
- Heartbeats: 10/s × 100 bytes × 5 = 5 KB/s
- **Total**: ~8 MB/s between nodes

## Scaling Strategies

### Vertical Scaling

When to scale up a node:

| Symptom             | Metric                                         | Action                |
| ------------------- | ---------------------------------------------- | --------------------- |
| High write latency  | `write_latency_seconds{quantile="0.99"} > 0.1` | More CPU, faster disk |
| Memory pressure     | OOM or high swap                               | More RAM              |
| Disk I/O bottleneck | High iowait, `apply_latency` high              | NVMe SSD              |

### Horizontal Scaling

When to add shards:

| Symptom            | Metric                       | Action          |
| ------------------ | ---------------------------- | --------------- |
| CPU saturation     | All cores at 100%            | Add shard       |
| Organization growth   | Many organizations on one shard | Rebalance       |
| Geographic latency | Cross-region writes slow     | Regional shards |

### Vault Sharding

Within an organization, use multiple vaults to parallelize:

```
Throughput = vaults × per_vault_throughput
```

**Example**: 4 vaults × 5,000 writes/sec = 20,000 writes/sec

Vaults are independent; operations on different vaults don't block each other.

## Performance Baselines

### Latency Targets

| Operation     | p50    | p99   | p999   |
| ------------- | ------ | ----- | ------ |
| Write         | 5 ms   | 20 ms | 100 ms |
| Read          | 0.5 ms | 2 ms  | 10 ms  |
| Verified read | 1 ms   | 5 ms  | 20 ms  |
| State root    | 0.5 ms | 2 ms  | 10 ms  |

### Throughput Targets

| Configuration       | Writes/sec     | Reads/sec       |
| ------------------- | -------------- | --------------- |
| Single vault        | 5,000-10,000   | 50,000-100,000  |
| 4 vaults            | 20,000-40,000  | 200,000-400,000 |
| 4 shards × 4 vaults | 80,000-160,000 | 800,000+        |

## Monitoring for Capacity

### Key Metrics

```promql
# Write throughput approaching limit
rate(ledger_writes_total[5m]) > 4000

# High proposal backlog (approaching capacity)
inferadb_ledger_raft_proposals_pending > 30

# Memory pressure
process_resident_memory_bytes / container_memory_limit > 0.8

# Disk usage
node_filesystem_avail_bytes{mountpoint="/data"} /
node_filesystem_size_bytes{mountpoint="/data"} < 0.2
```

### Capacity Alerts

```yaml
- alert: LedgerApproachingCapacity
  expr: rate(ledger_writes_total[5m]) > 4000
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Ledger approaching write capacity"

- alert: LedgerDiskFilling
  expr: |
    node_filesystem_avail_bytes{mountpoint="/data"} /
    node_filesystem_size_bytes{mountpoint="/data"} < 0.2
  labels:
    severity: warning
  annotations:
    summary: "Ledger disk usage above 80%"
```

## Growth Planning

### Estimating Growth

Track these trends:

1. **Entity growth rate**: `rate(ledger_entities_total[30d])`
2. **Relationship growth rate**: `rate(ledger_relationships_total[30d])`
3. **Storage growth rate**: `rate(node_filesystem_size_bytes[30d])`

### Capacity Runway

```
Runway (days) = (available_capacity - current_usage) / daily_growth_rate
```

Plan capacity additions when runway < 90 days.

## Reference Configurations

### Small (Startup)

- 3 nodes
- 4 cores, 8 GB RAM, 100 GB SSD each
- 1-2 organizations, 1-2 vaults
- Expected: 1,000-5,000 writes/sec

### Medium (Growing)

- 5 nodes, 2 shards
- 8 cores, 16 GB RAM, 500 GB NVMe each
- 10-50 organizations, 10-100 vaults
- Expected: 10,000-50,000 writes/sec

### Large (Enterprise)

- 7+ nodes per shard, 4+ shards
- 16 cores, 32 GB RAM, 1 TB NVMe each
- 100+ organizations, 1000+ vaults
- Expected: 100,000+ writes/sec

## Cost Estimation

### Cloud Pricing (Approximate)

| Provider | Instance        | Monthly Cost (3-node) |
| -------- | --------------- | --------------------- |
| AWS      | m6i.xlarge      | ~$450                 |
| GCP      | n2-standard-4   | ~$400                 |
| Azure    | Standard_D4s_v3 | ~$420                 |

Add:

- Storage: ~$0.10/GB/month (SSD)
- Network: ~$0.01/GB egress
- Monitoring: ~$50-100/month

### Total Cost Model

```
Monthly Cost = (nodes × instance_cost) +
               (storage_gb × storage_rate) +
               (egress_gb × egress_rate) +
               monitoring
```
