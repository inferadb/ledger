# Key Provisioning Runbook

Operational guide for provisioning Region Master Keys (RMKs) to InferaDB Ledger nodes.

## Overview

Each node independently acquires RMKs from its configured key source. Keys never travel via Raft or any cluster protocol. A node must have all required RMK versions for every region it participates in before joining any Raft group.

## Required Regions

A node holds RMKs for:

- **All non-protected regions**: `GLOBAL`, `US_EAST_VA`, `US_WEST_OR`
- **Its own region** if it requires data residency

Examples:

| Node Region            | Required RMK Regions                                         |
| ---------------------- | ------------------------------------------------------------ |
| `US_EAST_VA`           | `GLOBAL`, `US_EAST_VA`, `US_WEST_OR`                         |
| `DE_CENTRAL_FRANKFURT` | `GLOBAL`, `US_EAST_VA`, `US_WEST_OR`, `DE_CENTRAL_FRANKFURT` |
| `GLOBAL`               | `GLOBAL`, `US_EAST_VA`, `US_WEST_OR`                         |

## Multi-Version Requirement

A new node must have **all non-decommissioned RMK versions** for each required region, not just the latest. Snapshots and log entries may reference any active or deprecated version. Missing a deprecated version causes `install_snapshot()` to fail.

## Backend-Specific Provisioning

### SecretsManagerKeyManager (Production)

Recommended for production. Keys fetched at runtime, held in memory only.

**Infisical setup:**

1. Store each region's RMK as a versioned secret. Path: `ledger/rmk/{region}`.
2. Authenticate nodes via machine identity (Universal Auth token, Kubernetes Auth, or cloud-native identity).
3. Scope access policies per region: a `US_EAST_VA` node can read `ledger/rmk/global`, `ledger/rmk/us-east-va`, `ledger/rmk/us-west-or` but not `ledger/rmk/de-central-frankfurt`.
4. Multi-version handled natively by the secrets manager.

**Node configuration:**

```yaml
key_manager:
  type: SecretsManager
  provider: Infisical
  endpoint: "https://infisical.example.com"
  project_id: "proj_xxx"
  environment: "production"
  region_secret_paths:
    global: "ledger/rmk/global"
    us-east-va: "ledger/rmk/us-east-va"
    us-west-or: "ledger/rmk/us-west-or"
```

**HashiCorp Vault:** Use AppRole or Kubernetes auth. Policies grant `read` on `secret/data/ledger/rmk/{region}`.

**AWS KMS / GCP KMS / Azure Key Vault:** Use IAM/RBAC scoped to region-specific key resources. Service account or managed identity for node authentication.

### EnvKeyManager (Staging / CI)

Keys injected via environment variables. Suitable for container environments where secrets injection is available.

**Variable naming:** `LEDGER_RMK_{REGION_UPPER}_V{VERSION}` (hex-encoded 32 bytes).

**Example:**

```bash
LEDGER_RMK_GLOBAL_V1=aabb...ff           # 64 hex chars = 32 bytes
LEDGER_RMK_US_EAST_VA_V1=ccdd...ee
LEDGER_RMK_US_WEST_OR_V1=1122...33
```

**Kubernetes pattern:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: ledger-rmk
data:
  LEDGER_RMK_GLOBAL_V1: <base64>
  LEDGER_RMK_US_EAST_VA_V1: <base64>
  LEDGER_RMK_US_WEST_OR_V1: <base64>
```

Reference in StatefulSet pod spec via `envFrom`.

**Limitation:** Rotation requires Secret update + rolling restart.

### FileKeyManager (Local Development Only)

Keys stored as raw 32-byte files on disk.

**Setup:**

```bash
mkdir -p data/keys/{global,us-east-va,us-west-or}
# Generate initial keys (one per region)
dd if=/dev/urandom bs=32 count=1 of=data/keys/global/v1.key 2>/dev/null
dd if=/dev/urandom bs=32 count=1 of=data/keys/us-east-va/v1.key 2>/dev/null
dd if=/dev/urandom bs=32 count=1 of=data/keys/us-west-or/v1.key 2>/dev/null
chmod 0400 data/keys/*/v1.key
```

**Directory structure:**

```
data/keys/
  global/
    v1.key
    v2.key
    versions.json
  us-east-va/
    v1.key
    versions.json
```

**`versions.json` sidecar** (auto-maintained by `rotate_rmk()`):

```json
[
  { "version": 1, "status": "Deprecated" },
  { "version": 2, "status": "Active" }
]
```

`health_check()` validates that key files listed in `versions.json` exist on disk.

**Warning:** Emits a warning in release builds. Not intended for production.

## Adding a New Node

1. **Provision key material** using your backend-specific steps above.
2. **Configure the node**: set region, listen address, bootstrap peers.
3. **Start the node**: startup verifies RMKs for all required regions, opens encrypted databases, joins cluster.
4. **Monitor catch-up**: use `GetClusterInfo` RPC until the node is fully replicated.

If a required version is missing, startup fails with a clear error:

```
RMK v1 for region global is listed as Deprecated but failed to load.
Provision this key version before joining the cluster.
```

## RMK Rotation

Before triggering `RotateRegionKey`:

1. **Verify all nodes have the new version**: check `GetRewrapStatus` or the `rmk_provisioning` health check detail. All nodes must report the new version.
2. **Trigger rotation**: call `RotateRegionKey` RPC on the leader.
3. **Monitor re-wrapping**: call `GetRewrapStatus` for progress. The background `DekRewrapJob` processes pages in batches.
4. **After completion**: the old version can be deprecated and eventually decommissioned.

## Health Monitoring

The `rmk_provisioning` dependency health check reports loaded versions per region:

```
rmk_versions: {global: [v1, v2], us-east-va: [v1], us-west-or: [v1]}
```

This appears in the readiness probe response. Monitor for:

- Missing regions (no versions listed)
- Missing deprecated versions before rotation
- Nodes reporting different version sets (cluster inconsistency)
