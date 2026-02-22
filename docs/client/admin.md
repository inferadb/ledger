[Documentation](../README.md) > Client API > AdminService

# AdminService API

Operations for organization, vault, and cluster management.

## Service Definition

```protobuf
service AdminService {
  // Organization operations
  rpc CreateOrganization(CreateOrganizationRequest) returns (CreateOrganizationResponse);
  rpc DeleteOrganization(DeleteOrganizationRequest) returns (DeleteOrganizationResponse);
  rpc GetOrganization(GetOrganizationRequest) returns (GetOrganizationResponse);
  rpc ListOrganizations(ListOrganizationsRequest) returns (ListOrganizationsResponse);

  // Vault operations
  rpc CreateVault(CreateVaultRequest) returns (CreateVaultResponse);
  rpc DeleteVault(DeleteVaultRequest) returns (DeleteVaultResponse);
  rpc GetVault(GetVaultRequest) returns (GetVaultResponse);
  rpc ListVaults(ListVaultsRequest) returns (ListVaultsResponse);

  // Cluster operations
  rpc JoinCluster(JoinClusterRequest) returns (JoinClusterResponse);
  rpc LeaveCluster(LeaveClusterRequest) returns (LeaveClusterResponse);
  rpc GetClusterInfo(GetClusterInfoRequest) returns (GetClusterInfoResponse);
  rpc GetNodeInfo(GetNodeInfoRequest) returns (GetNodeInfoResponse);

  // Maintenance operations
  rpc CreateSnapshot(CreateSnapshotRequest) returns (CreateSnapshotResponse);
  rpc CheckIntegrity(CheckIntegrityRequest) returns (CheckIntegrityResponse);
  rpc RecoverVault(RecoverVaultRequest) returns (RecoverVaultResponse);
  rpc ForceGc(ForceGcRequest) returns (ForceGcResponse);
}
```

## Organization Operations

### CreateOrganization

Creates a new organization and assigns it to a shard.

```bash
grpcurl -plaintext \
  -d '{"name": "acme_corp"}' \
  localhost:50051 ledger.v1.AdminService/CreateOrganization
```

**Request:**

| Field      | Type    | Description                                       |
| ---------- | ------- | ------------------------------------------------- |
| `name`     | string  | Human-readable organization name                     |
| `shard_id` | ShardId | (Optional) Target shard; auto-assigned if omitted |

**Response:**

| Field          | Type        | Description                   |
| -------------- | ----------- | ----------------------------- |
| `organization_slug` | OrganizationSlug | Assigned organization identifier |
| `shard_id`     | ShardId     | Assigned shard                |

### DeleteOrganization

Marks an organization for deletion. All vaults must be deleted first.

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/DeleteOrganization
```

### GetOrganization

Retrieves organization metadata. Lookup by ID or name.

```bash
# By ID
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/GetOrganization

# By name
grpcurl -plaintext \
  -d '{"name": "acme_corp"}' \
  localhost:50051 ledger.v1.AdminService/GetOrganization
```

**Response fields:**

| Field            | Type            | Description            |
| ---------------- | --------------- | ---------------------- |
| `organization_slug`   | OrganizationSlug     | Organization identifier   |
| `name`           | string          | Organization name         |
| `shard_id`       | ShardId         | Hosting shard          |
| `member_nodes`   | NodeId[]        | Nodes in the shard     |
| `status`         | OrganizationStatus | Lifecycle state        |
| `config_version` | uint64          | For cache invalidation |
| `created_at`     | Timestamp       | Creation time          |

**OrganizationStatus values:**

| Value       | Description                     |
| ----------- | ------------------------------- |
| `ACTIVE`    | Accepting requests              |
| `MIGRATING` | Being migrated to another shard |
| `SUSPENDED` | Billing or policy suspension    |
| `DELETING`  | Deletion in progress            |
| `DELETED`   | Tombstone                       |

### ListOrganizations

Lists all organizations with pagination.

```bash
grpcurl -plaintext \
  -d '{"page_size": 100}' \
  localhost:50051 ledger.v1.AdminService/ListOrganizations
```

**Request:**

| Field        | Type   | Description                                |
| ------------ | ------ | ------------------------------------------ |
| `page_token` | bytes  | (Optional) Continuation token              |
| `page_size`  | uint32 | Results per page (default: 100, max: 1000) |

## Vault Operations

### CreateVault

Creates a new vault within an organization.

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/CreateVault
```

**Request:**

| Field                | Type                 | Description                             |
| -------------------- | -------------------- | --------------------------------------- |
| `organization_slug`       | OrganizationSlug          | Parent organization                        |
| `replication_factor` | uint32               | (Optional) Number of replicas           |
| `initial_nodes`      | NodeId[]             | (Optional) Specific nodes to host vault |
| `retention_policy`   | BlockRetentionPolicy | (Optional) Block retention mode         |

**BlockRetentionMode values:**

| Value       | Description                                           |
| ----------- | ----------------------------------------------------- |
| `FULL`      | Keep all blocks with full transactions (SOC 2, HIPAA) |
| `COMPACTED` | Remove old transaction bodies after snapshot          |

**Response:**

| Field      | Type        | Description                                  |
| ---------- | ----------- | -------------------------------------------- |
| `vault`    | VaultSlug   | Assigned vault slug (Snowflake identifier)   |
| `genesis`  | BlockHeader | Genesis block header with initial state root |

### DeleteVault

Marks a vault for deletion. Vault must be empty.

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
  localhost:50051 ledger.v1.AdminService/DeleteVault
```

### GetVault

Retrieves vault metadata and current state.

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
  localhost:50051 ledger.v1.AdminService/GetVault
```

**Response:**

| Field              | Type                 | Description              |
| ------------------ | -------------------- | ------------------------ |
| `organization_slug`     | OrganizationSlug          | Parent organization         |
| `vault`            | VaultSlug            | Vault slug               |
| `height`           | uint64               | Current block height     |
| `state_root`       | Hash                 | Current state root       |
| `nodes`            | NodeId[]             | Hosting nodes            |
| `leader`           | NodeId               | Current leader           |
| `status`           | VaultStatus          | Lifecycle state          |
| `retention_policy` | BlockRetentionPolicy | Block retention settings |

**VaultStatus values:**

| Value       | Description                         |
| ----------- | ----------------------------------- |
| `ACTIVE`    | Accepting reads and writes          |
| `READ_ONLY` | Reads only (migration, maintenance) |
| `DELETED`   | Tombstone                           |

### ListVaults

Lists all vaults on this node.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/ListVaults
```

## Cluster Operations

### JoinCluster

Requests to join an existing cluster. The target node must be a cluster member.

```bash
grpcurl -plaintext \
  -d '{"node_id": "123", "address": "10.0.0.4:50051"}' \
  localhost:50051 ledger.v1.AdminService/JoinCluster
```

### LeaveCluster

Gracefully removes a node from the cluster.

```bash
grpcurl -plaintext \
  -d '{"node_id": "123"}' \
  localhost:50051 ledger.v1.AdminService/LeaveCluster
```

### GetClusterInfo

Returns current cluster membership and Raft state.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/GetClusterInfo
```

**Response fields:**

| Field       | Type            | Description         |
| ----------- | --------------- | ------------------- |
| `leader_id` | uint64          | Current Raft leader |
| `members`   | ClusterMember[] | All cluster members |
| `term`      | uint64          | Current Raft term   |

**ClusterMember fields:**

| Field     | Type   | Description       |
| --------- | ------ | ----------------- |
| `node_id` | uint64 | Node identifier   |
| `address` | string | Node gRPC address |
| `role`    | string | VOTER or LEARNER  |

### GetNodeInfo

Returns node information. Available before cluster formation.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/GetNodeInfo
```

Used during coordinated bootstrap for nodes to discover each other's Snowflake IDs.

## Maintenance Operations

### CreateSnapshot

Triggers a manual snapshot for a vault.

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
  localhost:50051 ledger.v1.AdminService/CreateSnapshot
```

**Response:**

| Field           | Type   | Description            |
| --------------- | ------ | ---------------------- |
| `block_height`  | uint64 | Snapshot height        |
| `state_root`    | Hash   | State root at snapshot |
| `snapshot_path` | string | Local file path        |

### CheckIntegrity

Runs an integrity check on a vault.

```bash
# Quick check
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}, "full_check": false}' \
  localhost:50051 ledger.v1.AdminService/CheckIntegrity

# Full replay from genesis
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}, "full_check": true}' \
  localhost:50051 ledger.v1.AdminService/CheckIntegrity
```

**Response:**

| Field     | Type             | Description             |
| --------- | ---------------- | ----------------------- |
| `healthy` | bool             | True if no issues found |
| `issues`  | IntegrityIssue[] | List of detected issues |

### RecoverVault

Recovers a diverged vault by replaying transactions from block archive.

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
  localhost:50051 ledger.v1.AdminService/RecoverVault
```

Only works on vaults in `DIVERGED` state unless `force: true` is set.

**Response:**

| Field              | Type             | Description                 |
| ------------------ | ---------------- | --------------------------- |
| `success`          | bool             | Whether recovery succeeded  |
| `message`          | string           | Status or error message     |
| `health_status`    | VaultHealthProto | Vault health after recovery |
| `final_height`     | uint64           | Block height after recovery |
| `final_state_root` | Hash             | State root after recovery   |

### SimulateDivergence

Simulates vault divergence for testing recovery procedures. Forces a vault into the `DIVERGED` state without actual data corruption.

**Note**: Only available when the server is built with the `test-utils` feature.

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}, "expected_state_root": {"bytes": "AAAA..."}, "computed_state_root": {"bytes": "BBBB..."}, "at_height": 100}' \
  localhost:50051 ledger.v1.AdminService/SimulateDivergence
```

**Request:**

| Field                 | Type        | Description                          |
| --------------------- | ----------- | ------------------------------------ |
| `organization_slug`        | OrganizationSlug | Target organization                     |
| `vault`               | VaultSlug   | Target vault                         |
| `expected_state_root` | Hash        | Fake expected state root             |
| `computed_state_root` | Hash        | Fake computed state root (different) |
| `at_height`           | uint64      | Height where "divergence" occurred   |

**Response:**

| Field           | Type             | Description                             |
| --------------- | ---------------- | --------------------------------------- |
| `success`       | bool             | Whether simulation succeeded            |
| `message`       | string           | Status message                          |
| `health_status` | VaultHealthProto | Should be `VAULT_HEALTH_PROTO_DIVERGED` |

Use `RecoverVault` to restore the vault to `HEALTHY` state after testing.

### ForceGc

Forces a garbage collection cycle for expired entities.

```bash
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "vault": {"slug": "7180591718400"}}' \
  localhost:50051 ledger.v1.AdminService/ForceGc
```

Only the leader can run GC. Returns error on followers.

**Response:**

| Field            | Type   | Description                        |
| ---------------- | ------ | ---------------------------------- |
| `success`        | bool   | Whether GC completed successfully  |
| `message`        | string | Status or error message            |
| `expired_count`  | uint64 | Number of expired entities removed |
| `vaults_scanned` | uint64 | Number of vaults scanned           |
