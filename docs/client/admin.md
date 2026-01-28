# AdminService API

Operations for namespace, vault, and cluster management.

## Service Definition

```protobuf
service AdminService {
  // Namespace operations
  rpc CreateNamespace(CreateNamespaceRequest) returns (CreateNamespaceResponse);
  rpc DeleteNamespace(DeleteNamespaceRequest) returns (DeleteNamespaceResponse);
  rpc GetNamespace(GetNamespaceRequest) returns (GetNamespaceResponse);
  rpc ListNamespaces(ListNamespacesRequest) returns (ListNamespacesResponse);

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

## Namespace Operations

### CreateNamespace

Creates a new namespace and assigns it to a shard.

```bash
grpcurl -plaintext \
  -d '{"name": "acme_corp"}' \
  localhost:50051 ledger.v1.AdminService/CreateNamespace
```

**Request:**

| Field      | Type    | Description                                       |
| ---------- | ------- | ------------------------------------------------- |
| `name`     | string  | Human-readable namespace name                     |
| `shard_id` | ShardId | (Optional) Target shard; auto-assigned if omitted |

**Response:**

| Field          | Type        | Description                   |
| -------------- | ----------- | ----------------------------- |
| `namespace_id` | NamespaceId | Assigned namespace identifier |
| `shard_id`     | ShardId     | Assigned shard                |

### DeleteNamespace

Marks a namespace for deletion. All vaults must be deleted first.

```bash
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/DeleteNamespace
```

### GetNamespace

Retrieves namespace metadata. Lookup by ID or name.

```bash
# By ID
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/GetNamespace

# By name
grpcurl -plaintext \
  -d '{"name": "acme_corp"}' \
  localhost:50051 ledger.v1.AdminService/GetNamespace
```

**Response fields:**

| Field            | Type            | Description            |
| ---------------- | --------------- | ---------------------- |
| `namespace_id`   | NamespaceId     | Namespace identifier   |
| `name`           | string          | Namespace name         |
| `shard_id`       | ShardId         | Hosting shard          |
| `member_nodes`   | NodeId[]        | Nodes in the shard     |
| `status`         | NamespaceStatus | Lifecycle state        |
| `config_version` | uint64          | For cache invalidation |
| `created_at`     | Timestamp       | Creation time          |

**NamespaceStatus values:**

| Value       | Description                     |
| ----------- | ------------------------------- |
| `ACTIVE`    | Accepting requests              |
| `MIGRATING` | Being migrated to another shard |
| `SUSPENDED` | Billing or policy suspension    |
| `DELETING`  | Deletion in progress            |
| `DELETED`   | Tombstone                       |

### ListNamespaces

Lists all namespaces with pagination.

```bash
grpcurl -plaintext \
  -d '{"page_size": 100}' \
  localhost:50051 ledger.v1.AdminService/ListNamespaces
```

**Request:**

| Field        | Type   | Description                                |
| ------------ | ------ | ------------------------------------------ |
| `page_token` | bytes  | (Optional) Continuation token              |
| `page_size`  | uint32 | Results per page (default: 100, max: 1000) |

## Vault Operations

### CreateVault

Creates a new vault within a namespace.

```bash
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/CreateVault
```

**Request:**

| Field                | Type                 | Description                             |
| -------------------- | -------------------- | --------------------------------------- |
| `namespace_id`       | NamespaceId          | Parent namespace                        |
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
| `vault_id` | VaultId     | Assigned vault identifier                    |
| `genesis`  | BlockHeader | Genesis block header with initial state root |

### DeleteVault

Marks a vault for deletion. Vault must be empty.

```bash
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "vault_id": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/DeleteVault
```

### GetVault

Retrieves vault metadata and current state.

```bash
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "vault_id": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/GetVault
```

**Response:**

| Field              | Type                 | Description              |
| ------------------ | -------------------- | ------------------------ |
| `namespace_id`     | NamespaceId          | Parent namespace         |
| `vault_id`         | VaultId              | Vault identifier         |
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
  -d '{"node_id": {"id": "123"}, "addr": "10.0.0.4:50051"}' \
  localhost:50051 ledger.v1.AdminService/JoinCluster
```

### LeaveCluster

Gracefully removes a node from the cluster.

```bash
grpcurl -plaintext \
  -d '{"node_id": {"id": "123"}}' \
  localhost:50051 ledger.v1.AdminService/LeaveCluster
```

### GetClusterInfo

Returns current cluster membership and Raft state.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/GetClusterInfo
```

**Response fields:**

| Field       | Type       | Description         |
| ----------- | ---------- | ------------------- |
| `leader_id` | NodeId     | Current Raft leader |
| `members`   | NodeInfo[] | All cluster members |
| `term`      | uint64     | Current Raft term   |

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
  -d '{"namespace_id": {"id": "1"}, "vault_id": {"id": "1"}}' \
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
  -d '{"namespace_id": {"id": "1"}, "vault_id": {"id": "1"}, "full_check": false}' \
  localhost:50051 ledger.v1.AdminService/CheckIntegrity

# Full replay from genesis
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "vault_id": {"id": "1"}, "full_check": true}' \
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
  -d '{"namespace_id": {"id": "1"}, "vault_id": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/RecoverVault
```

Only works on vaults in `DIVERGED` state unless `force: true` is set.

### ForceGc

Forces a garbage collection cycle for expired entities.

```bash
grpcurl -plaintext \
  -d '{"namespace_id": {"id": "1"}, "vault_id": {"id": "1"}}' \
  localhost:50051 ledger.v1.AdminService/ForceGc
```

Only the leader can run GC. Returns error on followers.
