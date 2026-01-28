# Multi-Region Deployment

Guidance for deploying Ledger across multiple geographic regions.

## Architecture Options

### Option 1: Regional Shards (Recommended)

Each region operates an independent Ledger cluster. Namespaces are assigned to regional shards based on data locality requirements.

```
┌────────────────────────────────────────────────────────────────────┐
│                          Global Control                             │
│              (Namespace → Region routing table)                     │
└───────────────────────────┬────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  US-East      │   │  EU-West      │   │  AP-South     │
│  Shard 1      │   │  Shard 2      │   │  Shard 3      │
│               │   │               │   │               │
│  ┌─────────┐  │   │  ┌─────────┐  │   │  ┌─────────┐  │
│  │ 3-node  │  │   │  │ 3-node  │  │   │  │ 3-node  │  │
│  │ Raft    │  │   │  │ Raft    │  │   │  │ Raft    │  │
│  └─────────┘  │   │  └─────────┘  │   │  └─────────┘  │
│               │   │               │   │               │
│  Namespaces:  │   │  Namespaces:  │   │  Namespaces:  │
│  - us_corp_a  │   │  - eu_corp_b  │   │  - ap_corp_c  │
│  - us_corp_d  │   │  - eu_corp_e  │   │  - ap_corp_f  │
└───────────────┘   └───────────────┘   └───────────────┘
```

**Pros:**

- Low latency for regional operations
- Regional fault isolation
- Compliance with data residency requirements
- Independent scaling per region

**Cons:**

- No cross-region namespace access
- Requires namespace-to-region planning

### Option 2: Stretched Cluster (Not Recommended)

Single Raft cluster spanning multiple regions.

**Pros:**

- Single namespace accessible from any region

**Cons:**

- High cross-region latency on every write
- Reduced availability (network partition = quorum loss)
- Complexity without significant benefit

Ledger does not optimize for this configuration.

## Regional Shard Deployment

### 1. Deploy per-Region Clusters

Each region runs an independent Ledger cluster:

```yaml
# US-East deployment
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ledger
  namespace: ledger-us-east
spec:
  replicas: 3
  template:
    spec:
      containers:
        - name: ledger
          env:
            - name: INFERADB__LEDGER__CLUSTER
              value: "3"
            - name: INFERADB__LEDGER__PEERS
              value: "ledger-headless.ledger-us-east.svc.cluster.local"
```

### 2. Configure Namespace Routing

Control maintains the namespace-to-shard mapping:

```sql
-- Example routing table (managed by Control)
namespace_id | name      | shard_id | region
-------------+-----------+----------+---------
1            | acme_us   | 1        | us-east
2            | acme_eu   | 2        | eu-west
3            | globex    | 1        | us-east
```

### 3. Engine Routing

Engine routes requests based on namespace:

```rust
// Pseudo-code for Engine routing
async fn route_request(&self, namespace_id: NamespaceId) -> LedgerClient {
    let routing = self.control.get_namespace_routing(namespace_id).await?;
    self.ledger_clients.get(&routing.region)
}
```

## Cross-Region Considerations

### Data Residency

Assign namespaces to regions based on compliance requirements:

| Requirement       | Strategy                                    |
| ----------------- | ------------------------------------------- |
| GDPR (EU data)    | EU-only shard                               |
| CCPA (California) | US shard with appropriate controls          |
| Data sovereignty  | Region-locked shard, no replication outside |

### Disaster Recovery

For DR across regions:

1. **Backup replication**: Replicate snapshots to secondary region
2. **Cold standby**: Maintain inactive cluster in DR region
3. **Active-passive**: Route to DR region on primary failure

```bash
# Replicate snapshots to DR region
aws s3 sync s3://ledger-us-east/snapshots/ s3://ledger-us-west-dr/snapshots/
```

### Failover Procedure

1. **Detect failure**: Monitor regional Ledger health
2. **Promote DR cluster**: Start DR cluster with replicated data
3. **Update routing**: Point Control to DR region
4. **Notify clients**: Engine reconnects automatically

## Network Configuration

### Inter-Region Connectivity

Each regional cluster is isolated. No direct Ledger-to-Ledger communication across regions.

```
┌─────────────────────────────────────────────────────────────────────┐
│                            Control Plane                             │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Global Control (manages all regions)            │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                │                                    │
│              ┌─────────────────┼─────────────────┐                  │
│              ▼                 ▼                 ▼                  │
│     ┌────────────┐    ┌────────────┐    ┌────────────┐            │
│     │ US Control │    │ EU Control │    │ AP Control │            │
│     └─────┬──────┘    └─────┬──────┘    └─────┬──────┘            │
└───────────┼─────────────────┼─────────────────┼─────────────────────┘
            │                 │                 │
┌───────────┼─────────────────┼─────────────────┼─────────────────────┐
│           │    Data Plane   │                 │                     │
│           ▼                 ▼                 ▼                     │
│    ┌────────────┐    ┌────────────┐    ┌────────────┐              │
│    │ US Ledger  │    │ EU Ledger  │    │ AP Ledger  │              │
│    │ Cluster    │    │ Cluster    │    │ Cluster    │              │
│    └────────────┘    └────────────┘    └────────────┘              │
│                                                                     │
│    No direct communication between regional Ledger clusters         │
└─────────────────────────────────────────────────────────────────────┘
```

### WireGuard per Region

Each region maintains its own WireGuard mesh:

```ini
# US-East node1 WireGuard config
[Interface]
Address = 10.1.0.1/24
PrivateKey = <us_east_node1_key>

[Peer]
# US-East node2
PublicKey = <us_east_node2_pubkey>
AllowedIPs = 10.1.0.2/32

[Peer]
# US-East node3
PublicKey = <us_east_node3_pubkey>
AllowedIPs = 10.1.0.3/32
```

No cross-region WireGuard peering for Ledger traffic.

## Monitoring

### Per-Region Dashboards

Monitor each region independently:

```promql
# Per-region write latency
histogram_quantile(0.99,
  sum by (le, region) (
    ledger_write_latency_seconds_bucket{region="us-east"}
  )
)

# Per-region leader status
sum by (region) (inferadb_ledger_raft_is_leader)
```

### Global Health View

Aggregate metrics for global visibility:

```yaml
# Prometheus federation
scrape_configs:
  - job_name: "ledger-federation"
    honor_labels: true
    metrics_path: "/federate"
    static_configs:
      - targets:
          - "prometheus.us-east.internal:9090"
          - "prometheus.eu-west.internal:9090"
          - "prometheus.ap-south.internal:9090"
```

### Cross-Region Alerts

```yaml
- alert: LedgerRegionDown
  expr: |
    count(up{job="ledger"}) by (region) < 2
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "Ledger region {{ $labels.region }} has insufficient nodes"
```

## Migration Between Regions

Moving a namespace to a different region:

### 1. Create Namespace in Target Region

```bash
grpcurl -plaintext target-region-ledger:50051 \
  -d '{"name": "migrating_ns"}' \
  ledger.v1.AdminService/CreateNamespace
```

### 2. Export Data from Source

```bash
# Export all entities and relationships
grpcurl -plaintext source-region-ledger:50051 \
  -d '{"namespace_id": {"id": "OLD_NS"}}' \
  ledger.v1.ReadService/ExportNamespace > export.json
```

### 3. Import to Target

```bash
# Import to new region
grpcurl -plaintext target-region-ledger:50051 \
  -d @export.json \
  ledger.v1.WriteService/ImportNamespace
```

### 4. Update Routing

Update Control's routing table to point to new region.

### 5. Verify and Cleanup

```bash
# Verify data integrity
grpcurl -plaintext target-region-ledger:50051 \
  -d '{"namespace_id": {"id": "NEW_NS"}}' \
  ledger.v1.AdminService/CheckIntegrity

# Delete from source (after verification period)
grpcurl -plaintext source-region-ledger:50051 \
  -d '{"namespace_id": {"id": "OLD_NS"}}' \
  ledger.v1.AdminService/DeleteNamespace
```

## Cost Considerations

| Component | Per-Region Cost Factor                              |
| --------- | --------------------------------------------------- |
| Compute   | 3-5 nodes × instance cost                           |
| Storage   | Data size × storage rate                            |
| Network   | Intra-region (low), cross-region (avoid for Ledger) |
| Backup    | Snapshot storage + replication                      |

Estimate total multi-region cost:

```
Total = Σ (regions) × (compute + storage + backup)
```

No cross-region network costs for Ledger traffic in the recommended architecture.
