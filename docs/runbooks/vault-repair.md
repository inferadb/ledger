# Vault Repair

On-call runbook for diverged vaults: a replica's computed state root no longer matches the leader's expected state root for a specific vault.

## Symptom

- `HealthService.Check` against the vault returns `UNAVAILABLE` with a `VAULT_UNAVAILABLE` error class.
- Dashboards show a non-zero value for `ledger_state_root_divergences_total{vault_id=…}` on the affected node.
- Recent node logs contain `state_root_divergence` or `vault_diverged` events naming the vault.

## Alert / Trigger

- `VaultDiverged` — fires when `ledger_state_root_divergences_total` increments.
- `VaultRecoveryExhausted` — fires when automatic recovery fails three times in a row for the same vault.

## Blast radius

- **Scope**: one vault on one replica. Other vaults on the same node continue serving normally; other replicas of the same vault continue serving if they have not diverged. This is the per-vault isolation property documented in [DESIGN.md](../../DESIGN.md) § "Per-vault chains".
- **Downstream impact**: reads to the diverged vault return `UNAVAILABLE`; writes keep replicating through Raft but are not applied on the diverged replica until recovery completes.

## Preconditions

- Authority to call `AdminService.RecoverVault` (typically requires the operator-tier admin token).
- gRPC tooling (`grpcurl` or an SDK client) configured against the affected node's `--listen` address.
- A recent clean backup is available if the repair needs to escalate to a rebuild (see [backup-verification playbook](../playbooks/backup-verification.md)).

## Steps

1. **Confirm the divergence** — query `HealthService.Check` for the affected vault; check `journalctl -u ledger --since "1 hour ago" | grep -E "(diverge|state_root)"` for the logged event. Note the vault slug + height.
2. **Triage root cause** using the symptom → cause table in [Deep reference § Step 2: Identify Root Cause](#step-2-identify-root-cause). Classify as single-node (disk corruption), multi-node (non-determinism bug), or all-node (coordinated corruption) before acting.
3. **Force a recovery attempt** via `AdminService.RecoverVault` — resets the attempt counter and replays from the latest snapshot. See the exact command in [Deep reference § Step 3: Force Recovery](#step-3-force-recovery).
4. **If recovery cannot succeed** (classified multi-node divergence or a single-node fix that keeps failing), escalate per the **Escalation** section below — do not keep retrying the same recovery automatically.

## Verification

- `HealthService.Check` for the vault returns `OK`.
- A test read via `ReadService.Read` against the vault succeeds.
- `ledger_state_root_divergences_total{vault_id=…}` stops incrementing.
- `ledger_recovery_success_total` incremented during the repair.

## Rollback

Recovery from a clean snapshot is idempotent — if the recovery misbehaves, stop the node, restore the pre-recovery state.db from `state.db.corrupt.<date>` (created by the [Runbook: Complete Vault Rebuild](#runbook-complete-vault-rebuild) procedure), and re-start. The vault returns to `Diverged` and can be repaired via a different path. Do **not** attempt to repair a vault by manually editing state files.

## Escalation

- Single-vault issue that RecoverVault fixes: no escalation.
- After two failed RecoverVault attempts **or** if any replica shows `ledger_determinism_bug_total > 0`: page the consensus / state-layer owner; this indicates a code-level bug, not an operational one.
- If all three replicas show divergence for the same vault at the same height: this is a coordinated-corruption incident — stop writes to the cluster and begin full incident response per your organization's policy.

## Deep reference

The sections below are the original long-form content — supporting detail the on-call can reach for after the quick-reference above. The 8 sections above are load-bearing; the material below is commentary and procedure detail.

### Overview

A vault can become **diverged** when a replica's computed state root doesn't match the expected state root from the leader. This is a critical condition that indicates either:

1. **Non-deterministic bug**: A state machine operation produced different results on different nodes
2. **Corruption**: Storage corruption on this replica
3. **Version mismatch**: Different code versions running on leader vs follower

Importantly, a diverged vault is **isolated** from other vaults in the same region. Other vaults continue operating normally.

## Vault Health States

```text
┌─────────┐     divergence     ┌───────────┐     recovery      ┌─────────────┐
│ Healthy │ ──────────────────►│ Diverged  │ ─────────────────►│ Recovering  │
└─────────┘                    └───────────┘                   └─────────────┘
     ▲                                                              │
     │                     successful recovery                      │
     └──────────────────────────────────────────────────────────────┘
```

| State        | Description                  | Read Availability  | Write Availability         |
| ------------ | ---------------------------- | ------------------ | -------------------------- |
| `Healthy`    | Normal operation             | Yes                | Yes                        |
| `Diverged`   | State root mismatch detected | No (`UNAVAILABLE`) | Replicated but not applied |
| `Recovering` | Auto-recovery in progress    | No (`UNAVAILABLE`) | Replicated but not applied |

## Divergence Detection

When a follower applies a block, it:

1. Applies all transactions in the block to its local state
2. Computes the resulting state root
3. Compares against the `expected_state_root` from the leader

If they don't match:

```yaml
CRITICAL: state_root_divergence{vault_id=123, region="us-east", height=45678}  # internal vault_id in metrics
  expected: 0x7a3f...
  computed: 0x8b2e...
```

The follower:

- Rolls back uncommitted state for this vault only
- Marks vault as `Diverged`
- Emits critical alert
- Continues processing other vaults in the block
- Returns `VAULT_UNAVAILABLE` for reads to this vault

## Automatic Recovery

Diverged vaults attempt automatic recovery with exponential backoff:

| Attempt | Backoff    | Action                                  |
| ------- | ---------- | --------------------------------------- |
| 1       | 5 seconds  | Clear state, replay from snapshot + log |
| 2       | 10 seconds | Same as attempt 1                       |
| 3       | 20 seconds | Require manual intervention if fails    |

Backoff formula: `base_delay × 2^(attempt-1)` with base=5s, max=300s.

The recovery process:

1. Mark vault as `Recovering { attempt: N }`
2. Clear vault's state tree (preserve other vaults)
3. Load latest snapshot for this vault
4. Replay Raft log from snapshot point
5. Verify state roots match at each height
6. On success: transition to `Healthy`
7. On failure: schedule retry with backoff (or halt if attempt 3)

After 3 failed attempts:

```text
CRITICAL: vault_recovery_exhausted{vault_id=123}  # internal vault_id in metrics
```

Manual intervention is required.

## Manual Recovery

### Prerequisites

- Access to gRPC tools (`grpcurl`) or AdminService client
- Direct access to the diverged node
- Ability to take the node offline if needed

### Step 1: Diagnose the Issue

```bash
# Check vault health status via HealthService
grpcurl -plaintext -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
  localhost:50051 ledger.v1.HealthService/Check

# View recent divergence events in logs
journalctl -u ledger --since "1 hour ago" | grep -E "(diverge|state_root)"

# Check node info and cluster state
grpcurl -plaintext localhost:50051 ledger.v1.AdminService/GetNodeInfo
```

### Step 2: Identify Root Cause

| Symptom                  | Likely Cause            | Resolution                              |
| ------------------------ | ----------------------- | --------------------------------------- |
| Single node diverged     | Disk corruption         | Rebuild from snapshot                   |
| Multiple nodes diverged  | Non-deterministic bug   | Code fix required, then rebuild         |
| All nodes diverged       | Corrupted leader state  | Rollback cluster to known-good snapshot |
| Divergence after upgrade | Version incompatibility | Rollback upgrade, coordinate releases   |

### Step 3: Force Recovery

Once you've identified and addressed the root cause:

```bash
# Force recovery for a specific vault via AdminService
grpcurl -plaintext -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
  localhost:50051 ledger.v1.AdminService/RecoverVault

# Monitor recovery progress via health checks
watch -n 5 'grpcurl -plaintext -d "{\"organization\": {\"slug\": 1234567890}, \"vault\": {\"slug\": 7180591718400}}" \
  localhost:50051 ledger.v1.HealthService/Check'
```

The `RecoverVault` RPC:

- Resets the recovery attempt counter
- Clears vault state immediately
- Begins replay from the latest snapshot

### Step 4: Verify Recovery

```bash
# Check vault is healthy
grpcurl -plaintext -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
  localhost:50051 ledger.v1.HealthService/Check

# Test read operations
grpcurl -plaintext -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}, "key": "test"}' \
  localhost:50051 ledger.v1.ReadService/Read
```

## Preventing Divergence

### Determinism Requirements

All state machine operations must be deterministic:

- **No timestamps in state**: Use block height, not wall-clock time
- **No random values**: If randomness is needed, it must come from the leader
- **Consistent floating-point**: Use fixed-point arithmetic or consistent rounding
- **Ordered iteration**: Use sorted maps/sets, not hash-based

### Testing for Determinism

Run the state determinism property tests:

```bash
cargo test --package inferadb-ledger-state -- proptest_determinism --nocapture
```

This replays the same transactions on multiple state machines and verifies identical state roots.

### Version Compatibility

Before upgrading:

1. Review changelog for state machine changes
2. Test upgrade path in staging
3. Follow the [Rolling Upgrade Playbook](../playbooks/rolling-upgrade.md) (pre-GA: full cluster wipe required)
4. Monitor for divergence during and after upgrade

## Runbook: Complete Vault Rebuild

For severe corruption or when automatic recovery fails repeatedly:

```bash
# 1. Stop the affected node
systemctl stop ledger

# 2. Backup current state (for investigation)
cp -r /var/lib/ledger/state.db /var/lib/ledger/state.db.corrupt.$(date +%Y%m%d)

# 3. Start the node (automatic recovery will replay from snapshot)
systemctl start ledger

# 4. Monitor recovery via HealthService
grpcurl -plaintext -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
  localhost:50051 ledger.v1.HealthService/Check

# 5. Check logs for recovery progress
journalctl -u ledger -f | grep -E "(recovery|diverged|Recovering)"
```

Note: Vault state is automatically cleared and rebuilt during recovery. Manual state deletion is not required.

## Metrics and Alerts

### Key Metrics

| Metric                                       | Alert Threshold | Meaning                        |
| -------------------------------------------- | --------------- | ------------------------------ |
| `ledger_recovery_failure_total`              | > 0             | Recovery attempt failed        |
| `ledger_determinism_bug_total`               | > 0             | Non-deterministic state change |
| `ledger_recovery_success_total`              | increasing      | Recoveries completing          |
| `inferadb_ledger_state_root_latency_seconds` | p99 > 100ms     | State computation slow         |

Note: Vault health status is queried via `HealthService.Check()` RPC, not Prometheus metrics.

### Alert Responses

**Alert: `VaultDiverged`**

```text
severity: critical
action: Page on-call, begin investigation
```

**Alert: `VaultRecoveryExhausted`**

```text
severity: critical
action: Manual intervention required
runbook: See "Manual Recovery" section above
```

## FAQ

### Q: Does a diverged vault affect other vaults?

No. Vault divergence is isolated. Other vaults in the same region continue operating normally. This is by design—see DESIGN.md §8.5 "Multi-Vault Failure Isolation".

### Q: Can writes still replicate to a diverged vault?

Yes. Raft log entries for the diverged vault continue replicating but are not applied to state. Once the vault recovers, it replays from the log.

### Q: How long does recovery take?

Depends on:

- Snapshot age (more log to replay = longer)
- Vault size (larger state tree = longer)
- Disk I/O performance

Typical recoveries complete in seconds to minutes. Very large vaults with old snapshots may take longer.

### Q: Can I read historical data from a diverged vault?

No. The vault is marked unavailable for all reads until recovery completes. If you need urgent access to historical data, consider:

- Reading from a healthy replica
- Using the block archive directly (requires tooling)

### Q: What if all replicas diverge?

This indicates a serious bug or coordinated corruption. Steps:

1. Stop all writes to the cluster
2. Identify the most recent known-good state
3. Rollback all nodes to that snapshot
4. Investigate root cause before resuming

This is a rare scenario that should trigger incident response.
