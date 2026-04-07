# Region Management

[Documentation](../README.md) > Operations > Region Management

Regions are geographic data residency zones. Each region maps 1:1 to a Raft consensus group with isolated storage. This guide covers region concepts, organization assignment, write forwarding, and monitoring.

## Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Cluster                                    │
│                                                                         │
│   ┌──────────────────────────┐                                          │
│   │     GLOBAL (control)     │  Org registry, sagas, sequences (no PII) │
│   │     Nodes: all           │  Replicated to every node in every region │
│   └──────────────────────────┘                                          │
│                                                                         │
│   ┌──────────────────────┐     ┌──────────────────────┐                 │
│   │   us-east-va         │     │   ie-east-dublin     │                 │
│   │                      │     │                      │                 │
│   │  ┌────────────────┐  │     │  ┌────────────────┐  │                 │
│   │  │ Organization A │  │     │  │ Organization C │  │                 │
│   │  │ (vaults 1-3)   │  │     │  │ (vaults 1-2)   │  │                 │
│   │  └────────────────┘  │     │  └────────────────┘  │                 │
│   │                      │     │                      │                 │
│   │  ┌────────────────┐  │     │  ┌────────────────┐  │                 │
│   │  │ Organization B │  │     │  │ Organization D │  │                 │
│   │  │ (vaults 1-5)   │  │     │  │ (vaults 1-10)  │  │                 │
│   │  └────────────────┘  │     │  └────────────────┘  │                 │
│   │                      │     │                      │                 │
│   │  Nodes: 1, 2, 3     │     │  Nodes: 4, 5, 6     │                 │
│   └──────────────────────┘     └──────────────────────┘                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key concepts:**

- **Region**: A geographic data residency zone backed by its own Raft group with independent leader election and consensus
- **Organization**: Isolated storage unit assigned to exactly one region at creation time
- **Vault**: Blockchain within an organization
- **GLOBAL**: A special Raft group for the control plane (org registry, sagas, sequences) that stores no PII and replicates to all nodes

## Region Enum

The `Region` enum defines 25 geographic variants plus a GLOBAL control plane. Each data region corresponds to a jurisdiction with specific privacy regulations:

| Group                | Regions                                                                                                                             | Regulations             |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| North America        | `US_EAST_VA`, `US_WEST_OR`, `CA_CENTRAL_QC`                                                                                         | PIPEDA (Canada)         |
| South America        | `BR_SOUTHEAST_SP`                                                                                                                   | LGPD (Brazil)           |
| EU/EEA               | `IE_EAST_DUBLIN`, `FR_NORTH_PARIS`, `DE_CENTRAL_FRANKFURT`, `SE_EAST_STOCKHOLM`, `IT_NORTH_MILAN`                                   | GDPR                    |
| United Kingdom       | `UK_SOUTH_LONDON`                                                                                                                   | UK GDPR                 |
| Middle East & Africa | `SA_CENTRAL_RIYADH`, `BH_CENTRAL_MANAMA`, `AE_CENTRAL_DUBAI`, `IL_CENTRAL_TEL_AVIV`, `ZA_SOUTH_CAPE_TOWN`, `NG_WEST_LAGOS`          | PDPL, PDPA, POPIA, NDPA |
| Asia Pacific         | `SG_CENTRAL_SINGAPORE`, `AU_EAST_SYDNEY`, `ID_WEST_JAKARTA`, `JP_EAST_TOKYO`, `KR_CENTRAL_SEOUL`, `IN_WEST_MUMBAI`, `VN_SOUTH_HCMC` | PDPA, APPI, PIPA, DPDPA |
| China                | `CN_NORTH_BEIJING`                                                                                                                  | PIPL                    |

Proto enum values use geographic range prefixes: North America (10-19), South America (20-29), EU/EEA (30-39), UK (40-49), Middle East & Africa (50-59), Asia Pacific (60-69), China (70-79).

## Storage Layout

Each region gets isolated database files under a dedicated directory. The GLOBAL control plane uses its own top-level directory:

```
{data_dir}/
├── node_id                          # node identity
├── global/                          # GLOBAL Raft group (control plane)
│   ├── state.db                     # org registry, sequences, sagas (no PII)
│   ├── blocks.db                    # global blockchain
│   ├── raft.db                      # Raft log for GLOBAL group
│   └── events.db                    # audit events for GLOBAL group
├── regions/                         # per-region data (organizations + PII)
│   ├── us-east-va/
│   │   ├── state.db                 # entity/relationship stores, indexes
│   │   ├── blocks.db                # regional blockchain
│   │   ├── raft.db                  # Raft log for this region
│   │   └── events.db               # audit events for this region
│   ├── ie-east-dublin/
│   │   ├── state.db
│   │   ├── blocks.db
│   │   ├── raft.db
│   │   └── events.db
│   └── .../
├── snapshots/                       # per-region snapshot directories
│   ├── global/
│   ├── us-east-va/
│   └── .../
└── keys/                            # per-region RMK storage
```

This layout eliminates write lock contention between concurrent Raft group applies, enables per-region encryption (one `EncryptedBackend` per file with that region's RMK), and simplifies migration, snapshots, crypto-shredding, and disk accounting.

## Organization-to-Region Assignment

Organizations are assigned to exactly one region at creation time via the `region` parameter on `CreateOrganization`. The region determines where all organization data (vaults, entities, relationships) is stored.

### Creating an Organization

The `region` field is required. It accepts a proto enum value from the `Region` enum:

```bash
grpcurl -plaintext \
  -d '{"name": "acme_corp", "region": "REGION_IE_EAST_DUBLIN"}' \
  localhost:50051 ledger.v1.AdminService/CreateOrganization
```

Response includes the assigned region:

```json
{
  "slug": { "slug": "482938471625728" },
  "region": "REGION_IE_EAST_DUBLIN"
}
```

### Viewing Organization Region

```bash
grpcurl -plaintext \
  -d '{"slug": {"slug": "482938471625728"}}' \
  localhost:50051 ledger.v1.AdminService/GetOrganization
```

Response:

```json
{
  "slug": { "slug": "482938471625728" },
  "name": "acme_corp",
  "region": "REGION_IE_EAST_DUBLIN",
  "memberNodes": [
    { "id": "123456789" },
    { "id": "234567890" },
    { "id": "345678901" }
  ],
  "status": "ORGANIZATION_STATUS_ACTIVE"
}
```

### Region Migration

Organizations can be migrated between regions via `MigrateOrganization`:

```bash
grpcurl -plaintext \
  -d '{
    "slug": {"slug": "482938471625728"},
    "targetRegion": "REGION_DE_CENTRAL_FRANKFURT"
  }' \
  localhost:50051 ledger.v1.AdminService/MigrateOrganization
```

Migrations between non-protected regions are metadata-only. Migrations involving protected regions (mandatory in-country storage, e.g., `SA_CENTRAL_RIYADH`, `ID_WEST_JAKARTA`, `CN_NORTH_BEIJING`) involve full data transfer via saga. Writes to the organization are rejected during migration.

Downgrading from a protected region to a non-protected region requires explicit acknowledgment:

```bash
grpcurl -plaintext \
  -d '{
    "slug": {"slug": "482938471625728"},
    "targetRegion": "REGION_US_EAST_VA",
    "acknowledgeResidencyDowngrade": true
  }' \
  localhost:50051 ledger.v1.AdminService/MigrateOrganization
```

## Viewing System State

View all organization-to-region assignments via the routing table:

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.SystemDiscoveryService/GetSystemState
```

Response:

```json
{
  "version": "42",
  "nodes": [...],
  "organizations": [
    {
      "slug": { "slug": "482938471625728" },
      "name": "acme_corp",
      "region": "REGION_IE_EAST_DUBLIN",
      "members": [{"id": "123"}, {"id": "234"}, {"id": "345"}],
      "status": "ORGANIZATION_STATUS_ACTIVE"
    },
    {
      "slug": { "slug": "582938471625729" },
      "name": "other_org",
      "region": "REGION_US_EAST_VA",
      "members": [{"id": "456"}, {"id": "567"}, {"id": "678"}],
      "status": "ORGANIZATION_STATUS_ACTIVE"
    }
  ]
}
```

### Cache Invalidation

Check `version` to detect routing changes:

```bash
grpcurl -plaintext \
  -d '{"if_version_greater_than": 41}' \
  localhost:50051 ledger.v1.SystemDiscoveryService/GetSystemState
```

If version <= 41, response is empty (use cached data). If version > 41, response contains the updated routing table.

## Write Forwarding

In multi-region deployments, the `WriteService` transparently forwards writes to the correct region's leader. Clients can send write requests to **any node** in the cluster — if the target organization lives in a different region, the receiving node forwards the request to that region's leader via `RegionResolver`.

```
Client --> Node A (us-east-va) --> RegionResolver(org) --> ie-east-dublin Leader --> Response --> Node A --> Client
```

**Behavior:**

- The originating node performs pre-flight validation (rate limiting, input validation, idempotency check) before forwarding
- The `RegionResolver` determines the target region from the organization's registry entry in GLOBAL state
- The destination region's leader performs vault slug resolution and Raft proposal
- Forwarding is transparent to clients -- the response is relayed back through the originating node
- If the destination leader is unavailable, the client receives `UNAVAILABLE` and can retry

Clients do not need region-aware routing for writes. Any node accepts writes for any organization. The same forwarding applies for reads via `ReadService`.

## Region Membership

### Node Roles

Each region has its own Raft group with:

- **Voters**: Full voting members (max 5 per region)
- **Learners**: Replicate data but don't vote (for read scaling)

Nodes join specific region Raft groups. A single physical node can participate in multiple regions (the GLOBAL group plus one or more data regions).

### Node-to-Region Assignment

Nodes are added to region Raft groups when:

1. Cluster bootstrap assigns initial region membership
2. `JoinCluster` RPC adds a node to the GLOBAL group
3. Organization creation in a new region triggers region Raft group formation

### Region Rebalancing

Organization-to-region assignment reflects data residency requirements and is not automatically rebalanced. To move an organization between regions, use the `MigrateOrganization` RPC.

## Client Routing

Clients use the routing table from `GetSystemState` to send requests directly to the correct region's nodes:

1. **Cache system state** from `GetSystemState`
2. **Look up region** for target organization
3. **Send request** to any node in that region
4. **Invalidate cache** when `version` changes

Example client routing:

```rust
// Fetch routing table
let state = client.get_system_state().await?;

// Find region for organization
let org_entry = state.organizations
    .iter()
    .find(|o| o.slug == target_org_slug)
    .ok_or(Error::OrganizationNotFound)?;

let region_members = &org_entry.members;

// Connect to any member of the region
let node = region_members.choose(&mut rand::thread_rng());
let connection = connect_to_node(node)?;

// Send request (or rely on write forwarding from any node)
connection.write(request).await?;
```

Direct routing reduces latency by avoiding forwarding hops. Write forwarding provides a fallback so clients without routing awareness still work.

## Capacity Planning

### Per-Region Sizing

| Metric                   | Single Region Limit | Recommendation         |
| ------------------------ | ------------------- | ---------------------- |
| Write throughput         | ~10k ops/sec        | Monitor at 80%         |
| Total entities           | ~100M               | Monitor at 80M         |
| Organizations per region | Unlimited           | 100 for manageability  |
| Nodes per region         | 3-7 voters          | 3 for most deployments |

### When to Spread Load Across Regions

Monitor these metrics per region:

```promql
# Write latency trending up for a region
histogram_quantile(0.99, rate(ledger_write_latency_seconds_bucket{region="us-east-va"}[5m])) > 0.05

# Proposal backlog building in a region
inferadb_ledger_raft_proposals_pending{region="us-east-va"} > 30
```

If a single region is overloaded, assign new organizations to a less loaded region in the same geographic area or a nearby one.

## Monitoring

Track per-region metrics:

```promql
# Leader per region
sum by (region) (inferadb_ledger_raft_is_leader)

# Write latency per region
histogram_quantile(0.99, rate(ledger_write_latency_seconds_bucket[5m])) by (region)

# Storage size per region
inferadb_ledger_disk_bytes{region="us-east-va"}

# Raft log lag per region (follower behind leader)
inferadb_ledger_raft_log_lag{region="ie-east-dublin"}
```

### Alerting

Key alerts for multi-region deployments:

```promql
# Region has no leader (split-brain or total failure)
sum by (region) (inferadb_ledger_raft_is_leader) == 0

# Cross-region forwarding latency elevated
histogram_quantile(0.99, rate(ledger_forward_latency_seconds_bucket[5m])) > 0.5

# Region storage approaching capacity
inferadb_ledger_disk_bytes / inferadb_ledger_disk_capacity_bytes > 0.85
```

## Best Practices

### Region Placement

- **Compliance first**: Choose the region that satisfies the organization's data residency requirements. Protected regions (`SA_CENTRAL_RIYADH`, `ID_WEST_JAKARTA`, `CN_NORTH_BEIJING`) enforce mandatory in-country storage.
- **Latency**: Place organizations in the region closest to their users to minimize read/write latency.
- **Isolation**: High-volume organizations benefit from regions with fewer tenants to reduce contention.

### Operational Simplicity

- Start with a single data region plus the GLOBAL control plane.
- Add regions only when data residency requirements or latency constraints demand it.
- Every active region requires its own Raft group (leader election, log replication, snapshots), so fewer regions means less operational overhead.

### GLOBAL Group

- The GLOBAL Raft group stores only non-PII metadata: org registry, node discovery, Snowflake sequences, and saga state.
- Every node in the cluster participates in the GLOBAL group.
- GLOBAL availability is required for organization creation and routing, but individual region Raft groups can serve reads and writes independently once routing is cached.

### Data Residency Verification

- Use `GetOrganization` or `GetSystemState` to verify region assignments.
- Audit the on-disk layout: organization data for region `ie-east-dublin` should only exist under `{data_dir}/regions/ie-east-dublin/`.
- Per-region encryption keys (RMK) in `{data_dir}/keys/` enable crypto-shredding of an entire region's data.

## See Also

- [Deployment Guide](deployment.md) - Cluster setup
- [Multi-Region](multi-region.md) - Geographic distribution patterns
- [Capacity Planning](capacity-planning.md) - Sizing guidelines
- SystemDiscoveryService `GetSystemState` RPC - Routing API
