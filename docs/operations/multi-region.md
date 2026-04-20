# Multi-Region Deployment

Guidance for deploying Ledger across multiple geographic regions.

> **Related**: For the mechanics of how regions work (Region enum, storage layout, organization-to-region assignment, write forwarding), see [Region Management](region-management.md). For data-residency compliance guarantees (GLOBAL vs REGIONAL split, pseudonymization, crypto-shredding), see [Data Residency Architecture](data-residency-architecture.md).

## Architecture Options

### Option 1: Regional Clusters (Recommended)

Each region operates an independent Ledger cluster. Organizations are assigned to regions based on data locality requirements.

```text
┌────────────────────────────────────────────────────────────────────┐
│                          Global Control                             │
│              (Organization → Region routing table)                     │
└───────────────────────────┬────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  US-East      │   │  EU-West      │   │  AP-South     │
│  Region 1     │   │  Region 2     │   │  Region 3     │
│               │   │               │   │               │
│  ┌─────────┐  │   │  ┌─────────┐  │   │  ┌─────────┐  │
│  │ 3-node  │  │   │  │ 3-node  │  │   │  │ 3-node  │  │
│  │ Raft    │  │   │  │ Raft    │  │   │  │ Raft    │  │
│  └─────────┘  │   │  └─────────┘  │   │  └─────────┘  │
│               │   │               │   │               │
│  Organizations:  │   │  Organizations:  │   │  Organizations:  │
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

- No cross-region organization access
- Requires organization-to-region planning

### Option 2: Stretched Cluster (Not Recommended)

Single Raft cluster spanning multiple regions.

**Pros:**

- Single organization accessible from any region

**Cons:**

- High cross-region latency on every write
- Reduced availability (network partition = quorum loss)
- Complexity without significant benefit

Ledger does not optimize for this configuration.

## Regional Deployment

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
            - name: INFERADB__LEDGER__LISTEN
              value: "0.0.0.0:50051"
            - name: INFERADB__LEDGER__JOIN
              value: "ledger-headless.ledger-us-east.svc.cluster.local:50051"
```

### 2. Configure Organization Routing

Control maintains the organization-to-region mapping:

```sql
-- Example routing table (managed by Control)
organization_slug | name      | region
-------------+-----------+---------
1            | acme_us   | us-east
2            | acme_eu   | eu-west
3            | globex    | us-east
```

### 3. Engine Routing

Engine routes requests based on organization:

```rust
// Pseudo-code for Engine routing
async fn route_request(&self, organization_slug: OrganizationSlug) -> LedgerClient {
    let routing = self.control.get_organization_routing(organization_slug).await?;
    self.ledger_clients.get(&routing.region)
}
```

Within a region, the SDK redirects directly to the regional Raft leader via
`NotLeader` hints — the Ledger server does not proxy or forward client
requests across nodes on the client's behalf. Clients must therefore have
direct network reachability to every node in the target region.

See: [Redirect-Based Client Routing Architecture](runbooks/architecture-redirect-routing.md).

## Cross-Region Considerations

### Data Residency

Assign organizations to regions based on compliance requirements:

| Requirement       | Strategy                                      |
| ----------------- | --------------------------------------------- |
| GDPR (EU data)    | EU-only region                                |
| CCPA (California) | US region with appropriate controls           |
| Data sovereignty  | Region-locked cluster, no replication outside |

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

```text
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
  sum by (le) (
    rate(ledger_grpc_request_latency_seconds_bucket{service="WriteService"}[5m])
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

Moving an organization to a different region:

### 1. Migrate the Organization

Use the `MigrateOrganization` RPC on AdminService to move the organization to the target region. This handles data transfer and routing updates atomically via a saga:

```bash
grpcurl -plaintext ledger:50051 \
  -d '{
    "slug": {"slug": 7890123456},
    "targetRegion": "REGION_US_WEST_OR"
  }' \
  ledger.v1.AdminService/MigrateOrganization
```

If the source region is a protected region (mandatory in-country storage), you must acknowledge the residency downgrade:

```bash
grpcurl -plaintext ledger:50051 \
  -d '{
    "slug": {"slug": 7890123456},
    "targetRegion": "REGION_US_WEST_OR",
    "acknowledge_residency_downgrade": true
  }' \
  ledger.v1.AdminService/MigrateOrganization
```

Writes to the organization are rejected during migration.

### 2. Verify Integrity

After migration completes, verify data integrity in the target region:

```bash
grpcurl -plaintext ledger:50051 \
  -d '{"organization": {"slug": 7890123456}, "vault": {"slug": 7180591718400}, "full_check": true}' \
  ledger.v1.AdminService/CheckIntegrity
```

## Cost Considerations

| Component | Per-Region Cost Factor                              |
| --------- | --------------------------------------------------- |
| Compute   | 3-5 nodes × instance cost                           |
| Storage   | Data size × storage rate                            |
| Network   | Intra-region (low), cross-region (avoid for Ledger) |
| Backup    | Snapshot storage + replication                      |

Estimate total multi-region cost:

```text
Total = Σ (regions) × (compute + storage + backup)
```

No cross-region network costs for Ledger traffic in the recommended architecture.
