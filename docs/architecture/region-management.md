# Region Management

[Documentation](../README.md) > Architecture > Region Management

Regions are geographic data residency zones. Each region maps 1:1 to a Raft consensus group with isolated storage. Region names are arbitrary lowercase-hyphenated slug strings (`us-east-va`, `ie-east-dublin`, `eu-central-frankfurt`, ...) registered dynamically through the cluster's GLOBAL control plane. There is no hardcoded region enum — operators provision regions explicitly, and the cluster's region directory in GLOBAL state is the single source of truth.

> **Related**: For multi-region deployment patterns (regional vs stretched clusters, DR strategies), see [Multi-Region Deployment](multi-region.md). For data-residency compliance guarantees (why GLOBAL/REGIONAL split exists, pseudonymization, crypto-shredding), see [Data Residency Architecture](data-residency.md).

## Overview

```text
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
│   │  Nodes: 1, 2, 3      │     │  Nodes: 4, 5, 6      │                 │
│   └──────────────────────┘     └──────────────────────┘                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key concepts:**

- **Region**: A geographic data residency zone backed by its own Raft group with independent leader election and consensus. Identified by a slug string (e.g. `us-east-va`).
- **Organization**: Isolated storage unit assigned to exactly one region at creation time.
- **Vault**: Blockchain within an organization.
- **GLOBAL**: A reserved Raft group for the control plane (region directory, org registry, sagas, sequences) that stores no PII and replicates to every node. Always present, identified by the slug `global`.

## Region Slugs

Region names are validated lowercase-hyphenated slugs:

- ASCII lowercase letters, digits, and `-`
- No leading or trailing hyphen
- Non-empty

`global` is the reserved control-plane region. Any other slug must be explicitly provisioned before organizations or users can reference it. Operators can adopt any naming convention that suits their compliance regime — common choices follow the `<country>-<bearing>-<city>` shape used by InferaDB-managed clusters (`us-east-va`, `ie-east-dublin`, `de-central-frankfurt`, ...), but the cluster does not enforce that pattern.

## Provisioning a Region

Regions are created through `AdminService.ProvisionRegion`, which proposes a `RegisterRegionDirectoryEntry` operation through GLOBAL Raft. Once committed, every node observes the new region in the directory and starts the regional Raft group on the assigned voters.

```bash
grpcurl -plaintext \
  -d '{"name": "ie-east-dublin", "protected": true}' \
  localhost:50051 ledger.v1.AdminService/ProvisionRegion
```

The `protected` bool flags regions whose data must remain in-country (GDPR jurisdictions, PIPL, ID PDP, KSA PDPL, etc.). Protected regions:

- Require a minimum quorum of in-region nodes before organizations can be assigned (default: 3).
- Cannot be downgraded to a non-protected region without `acknowledge_residency_downgrade = true`.
- Restrict which nodes can host the region's data — see "Region Membership" below.

The region directory entry is the **only** authoritative residency policy. The `RegionDirectoryEntry.requires_residency` and `RegionDirectoryEntry.retention_days` fields, set at `ProvisionRegion` time (or rewritten via `SetRegionResidency`), drive every residency / retention decision in the workspace. The `Region` newtype carries no semantics beyond its slug — the previously hardcoded `Region::requires_residency()` / `Region::retention_days()` methods were removed because they silently mis-classified custom slugs (e.g. `"eu-custom"` defaulted to `retention_days=90`, a GDPR violation).

## Storage Layout

Each region gets isolated database files under a dedicated directory. The GLOBAL control plane uses its own top-level directory:

```text
{data_dir}/
├── node_id                          # node identity
├── global/                          # GLOBAL Raft group (control plane)
│   ├── state.db                     # region directory, org registry, sequences, sagas (no PII)
│   ├── blocks.db                    # global blockchain
│   ├── raft.db                      # Raft log for GLOBAL group
│   └── events.db                    # audit events for GLOBAL group
├── regions/                         # per-region data (organizations + PII)
│   ├── us-east-va/
│   │   ├── state.db                 # entity/relationship stores, indexes
│   │   ├── blocks.db                # regional blockchain
│   │   ├── raft.db                  # Raft log for this region
│   │   └── events.db                # audit events for this region
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

Per-region directories are named after the region slug verbatim. This layout eliminates write-lock contention between concurrent Raft group applies, enables per-region encryption (one `EncryptedBackend` per file with that region's RMK), and simplifies migration, snapshots, crypto-shredding, and disk accounting.

## Organization-to-Region Assignment

Organizations are assigned to exactly one region at creation time via the `region` slug parameter on `CreateOrganization`. The region must already exist in the GLOBAL region directory.

### Creating an Organization

```bash
grpcurl -plaintext \
  -d '{"name": "acme_corp", "region": "ie-east-dublin"}' \
  localhost:50051 ledger.v1.OrganizationService/CreateOrganization
```

Response includes the assigned region slug:

```json
{
  "slug": { "slug": "482938471625728" },
  "region": "ie-east-dublin"
}
```

### Viewing Organization Region

```bash
grpcurl -plaintext \
  -d '{"slug": {"slug": "482938471625728"}}' \
  localhost:50051 ledger.v1.OrganizationService/GetOrganization
```

Response:

```json
{
  "slug": { "slug": "482938471625728" },
  "name": "acme_corp",
  "region": "ie-east-dublin",
  "memberNodes": [
    { "id": "123456789" },
    { "id": "234567890" },
    { "id": "345678901" }
  ],
  "status": "ORGANIZATION_STATUS_ACTIVE"
}
```

### Region Migration

Organizations can be migrated between regions via `MigrateOrganization` (executed by `MigrateOrgSaga` in the state-machine saga orchestrator; per-user residency moves are executed by `MigrateUserSaga`):

```bash
grpcurl -plaintext \
  -d '{
    "slug": {"slug": "482938471625728"},
    "targetRegion": "de-central-frankfurt"
  }' \
  localhost:50051 ledger.v1.OrganizationService/MigrateOrganization
```

Migrations between non-protected regions are metadata-only. Migrations involving protected regions involve full data transfer via saga. Writes to the organization are rejected during migration.

Downgrading from a protected region to a non-protected region requires explicit acknowledgment:

```bash
grpcurl -plaintext \
  -d '{
    "slug": {"slug": "482938471625728"},
    "targetRegion": "us-east-va",
    "acknowledgeResidencyDowngrade": true
  }' \
  localhost:50051 ledger.v1.OrganizationService/MigrateOrganization
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
      "region": "ie-east-dublin",
      "members": [{"id": "123"}, {"id": "234"}, {"id": "345"}],
      "status": "ORGANIZATION_STATUS_ACTIVE"
    },
    {
      "slug": { "slug": "582938471625729" },
      "name": "other_org",
      "region": "us-east-va",
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

## Client Routing

Clients use redirect-only routing: every node accepts any region slug; if the receiving node is not the leader for that region, it returns `NotLeader` along with a `LeaderHint` carrying the region's leader endpoint. The SDK's `RegionLeaderCache` consumes the hint and reconnects directly to the leader on retry. Server-side request forwarding is reserved for saga orchestration only.

## Region Membership

### Node Roles

Each region has its own Raft group with:

- **Voters**: Full voting members (max 5 per region).
- **Learners**: Replicate data but don't vote (for read scaling).

A single physical node can participate in multiple regions (the GLOBAL group plus one or more data regions).

### Opting Into Protected Regions

A node opts into hosting a protected region by listing the region slug at startup via `--regions` (or the `INFERADB_REGIONS` environment variable):

```bash
inferadb-ledger start \
  --node-id 1 \
  --addr 127.0.0.1:50051 \
  --regions us-east-va,ie-east-dublin
```

If a node is not opted into a protected region, it will not be added as a voter for that region's Raft group. Non-protected regions are joined automatically when the cluster needs additional capacity.

### Region Rebalancing

Organization-to-region assignment reflects data residency requirements and is not automatically rebalanced. To move an organization between regions, use the `MigrateOrganization` RPC.

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
# Write latency trending up for a region (region tag added at agent level)
histogram_quantile(0.99, rate(ledger_grpc_request_latency_seconds_bucket{service="WriteService"}[5m])) > 0.05

# Proposal backlog building in a region
inferadb_ledger_raft_proposals_pending{region="us-east-va"} > 30
```

If a single region is overloaded, assign new organizations to a less loaded region in the same geographic area or a nearby one.

## Monitoring

Track per-region metrics:

```promql
# Leader per region
sum by (region) (inferadb_ledger_raft_is_leader)

# Write latency per region (region tag added at scrape/agent level)
histogram_quantile(0.99, rate(ledger_grpc_request_latency_seconds_bucket{service="WriteService"}[5m]))

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

- **Compliance first**: Choose the region whose protected/non-protected designation satisfies the organization's data residency requirements. Provision protected regions for jurisdictions that mandate in-country processing.
- **Latency**: Place organizations in the region closest to their users to minimize read/write latency.
- **Isolation**: High-volume organizations benefit from regions with fewer tenants to reduce contention.

### Operational Simplicity

- Start with a single data region plus the GLOBAL control plane.
- Provision new regions only when data residency requirements or latency constraints demand it.
- Every active region requires its own Raft group (leader election, log replication, snapshots), so fewer regions means less operational overhead.

### GLOBAL Group

- The GLOBAL Raft group stores only non-PII metadata: region directory, org registry, node discovery, Snowflake sequences, and saga state.
- Every node in the cluster participates in the GLOBAL group.
- GLOBAL availability is required for region provisioning, organization creation, and routing, but individual region Raft groups can serve reads and writes independently once routing is cached.

### Data Residency Verification

- Use `GetOrganization` or `GetSystemState` to verify region assignments.
- Audit the on-disk layout: organization data for region `ie-east-dublin` should only exist under `{data_dir}/regions/ie-east-dublin/`.
- Per-region encryption keys (RMK) in `{data_dir}/keys/` enable crypto-shredding of an entire region's data.

## See Also

- [Deployment Guide](../how-to/deployment.md) - Cluster setup
- [Multi-Region](multi-region.md) - Geographic distribution patterns
- [Capacity Planning](../how-to/capacity-planning.md) - Sizing guidelines
- SystemDiscoveryService `GetSystemState` RPC - Routing API
- AdminService `ProvisionRegion` RPC - Region registration
