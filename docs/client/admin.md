[Documentation](../README.md) > Client API > AdminService

# AdminService API

Operations for organization, vault, and cluster management.

## Service Definition

```protobuf
service AdminService {
  // Cluster membership
  rpc JoinCluster(JoinClusterRequest) returns (JoinClusterResponse);
  rpc LeaveCluster(LeaveClusterRequest) returns (LeaveClusterResponse);
  rpc GetClusterInfo(GetClusterInfoRequest) returns (GetClusterInfoResponse);
  rpc GetNodeInfo(GetNodeInfoRequest) returns (GetNodeInfoResponse);
  rpc TransferLeadership(TransferLeadershipRequest) returns (TransferLeadershipResponse);

  // Maintenance
  rpc CreateSnapshot(CreateSnapshotRequest) returns (CreateSnapshotResponse);
  rpc CheckIntegrity(CheckIntegrityRequest) returns (CheckIntegrityResponse);
  rpc RecoverVault(RecoverVaultRequest) returns (RecoverVaultResponse);
  rpc SimulateDivergence(SimulateDivergenceRequest) returns (SimulateDivergenceResponse);
  rpc ForceGc(ForceGcRequest) returns (ForceGcResponse);

  // Runtime configuration
  rpc UpdateConfig(UpdateConfigRequest) returns (UpdateConfigResponse);
  rpc GetConfig(GetConfigRequest) returns (GetConfigResponse);

  // Backup & restore
  rpc CreateBackup(CreateBackupRequest) returns (CreateBackupResponse);
  rpc ListBackups(ListBackupsRequest) returns (ListBackupsResponse);
  rpc RestoreBackup(RestoreBackupRequest) returns (RestoreBackupResponse);

  // Key rotation
  rpc RotateBlindingKey(RotateBlindingKeyRequest) returns (RotateBlindingKeyResponse);
  rpc GetBlindingKeyRehashStatus(GetBlindingKeyRehashStatusRequest) returns (GetBlindingKeyRehashStatusResponse);
  rpc RotateRegionKey(RotateRegionKeyRequest) returns (RotateRegionKeyResponse);
  rpc GetRewrapStatus(GetRewrapStatusRequest) returns (GetRewrapStatusResponse);
}
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

### TransferLeadership

Transfers Raft leadership to a specific node before shutdown or maintenance.

```bash
# Transfer to a specific node
grpcurl -plaintext \
  -d '{"target_node_id": "456"}' \
  localhost:50051 ledger.v1.AdminService/TransferLeadership

# Let the leader pick the most caught-up follower
grpcurl -plaintext \
  -d '{}' \
  localhost:50051 ledger.v1.AdminService/TransferLeadership
```

If `target_node_id` is 0, the leader picks the most caught-up follower. Returns once the target has won the election or the timeout expires.

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

| Field                 | Type             | Description                          |
| --------------------- | ---------------- | ------------------------------------ |
| `organization_slug`   | OrganizationSlug | Target organization                  |
| `vault`               | VaultSlug        | Target vault                         |
| `expected_state_root` | Hash             | Fake expected state root             |
| `computed_state_root` | Hash             | Fake computed state root (different) |
| `at_height`           | uint64           | Height where "divergence" occurred   |

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

## Runtime Configuration

### UpdateConfig

Updates runtime-reconfigurable parameters without server restart.

```bash
grpcurl -plaintext \
  -d '{"config_json": "{\"rate_limit\":{\"requests_per_second\":500}}", "dry_run": true}' \
  localhost:50051 ledger.v1.AdminService/UpdateConfig
```

Supports rate limit thresholds, hot key detection, compaction intervals, and validation limits. Non-reconfigurable parameters (listen address, data directory, Raft topology) are rejected with `INVALID_ARGUMENT`. Set `dry_run: true` to validate without applying.

### GetConfig

Returns the current runtime configuration.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/GetConfig
```

## Backup & Restore

### CreateBackup

Triggers a consistent snapshot, compresses it, and writes to the configured backup destination.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/CreateBackup
```

### ListBackups

Lists available backups with metadata.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/ListBackups
```

### RestoreBackup

Restores from a backup. Stops the region, restores state, and resumes. Requires explicit confirmation via the `confirm` field.

```bash
grpcurl -plaintext \
  -d '{"backup_id": "20260309T120000Z", "confirm": true}' \
  localhost:50051 ledger.v1.AdminService/RestoreBackup
```

## Key Rotation

### RotateBlindingKey

Initiates rotation of the email blinding key. Triggers asynchronous re-hashing of all email HMAC entries. Returns immediately; poll `GetBlindingKeyRehashStatus` for progress.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/RotateBlindingKey
```

### GetBlindingKeyRehashStatus

Checks the progress of an in-flight blinding key rotation.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/GetBlindingKeyRehashStatus
```

### RotateRegionKey

Initiates rotation of the Region Master Key (RMK). Triggers asynchronous DEK re-wrapping of all page sidecar metadata. Returns immediately; poll `GetRewrapStatus` for progress.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/RotateRegionKey
```

### GetRewrapStatus

Checks progress of DEK re-wrapping after RMK rotation.

```bash
grpcurl -plaintext \
  localhost:50051 ledger.v1.AdminService/GetRewrapStatus
```
