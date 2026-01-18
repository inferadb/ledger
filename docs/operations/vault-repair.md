# Vault Repair Workflow

This document describes how to diagnose and repair diverged vaults in InferaDB Ledger.

## Overview

A vault can become **diverged** when a replica's computed state root doesn't match the expected state root from the leader. This is a critical condition that indicates either:

1. **Non-deterministic bug**: A state machine operation produced different results on different nodes
2. **Corruption**: Storage corruption on this replica
3. **Version mismatch**: Different code versions running on leader vs follower

Importantly, a diverged vault is **isolated** from other vaults in the same shard. Other vaults continue operating normally.

## Vault Health States

```
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

```
CRITICAL: state_root_divergence{vault_id=123, shard_id=1, height=45678}
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

| Attempt | Backoff   | Action                                  |
| ------- | --------- | --------------------------------------- |
| 1       | Immediate | Clear state, replay from snapshot + log |
| 2       | 1 minute  | Same as attempt 1                       |
| 3       | 5 minutes | Require manual intervention             |

The recovery process:

1. Mark vault as `Recovering { attempt: N }`
2. Clear vault's state tree (preserve other vaults)
3. Load latest snapshot for this vault
4. Replay Raft log from snapshot point
5. Verify state roots match at each height
6. On success: transition to `Healthy`
7. On failure: schedule retry with backoff (or halt if attempt 3)

After 3 failed attempts:

```
CRITICAL: vault_recovery_exhausted{vault_id=123}
```

Manual intervention is required.

## Manual Recovery

### Prerequisites

- Access to `ledger-admin` CLI
- Direct access to the diverged node
- Ability to take the node offline if needed

### Step 1: Diagnose the Issue

```bash
# Check vault health status
ledger-admin vault-status --namespace 1 --vault 123

# View recent divergence events
ledger-admin logs --filter "state_root_divergence" --since 1h

# Compare state roots across replicas
ledger-admin compare-state-roots --namespace 1 --vault 123 --height 45678
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
# Force recovery for a specific vault
ledger-admin recover-vault --namespace 1 --vault 123 --force

# Monitor recovery progress
ledger-admin watch-recovery --namespace 1 --vault 123
```

The `--force` flag:

- Resets the recovery attempt counter
- Clears vault state immediately
- Begins replay from the latest snapshot

### Step 4: Verify Recovery

```bash
# Check vault is healthy
ledger-admin vault-status --namespace 1 --vault 123

# Verify state root matches cluster
ledger-admin compare-state-roots --namespace 1 --vault 123

# Test read/write operations
ledger-admin ping --namespace 1 --vault 123
```

## Preventing Divergence

### Determinism Requirements

All state machine operations must be deterministic:

- **No timestamps in state**: Use block height, not wall-clock time
- **No random values**: If randomness is needed, it must come from the leader
- **Consistent floating-point**: Use fixed-point arithmetic or consistent rounding
- **Ordered iteration**: Use sorted maps/sets, not hash-based

### Testing for Determinism

Run the determinism test suite:

```bash
cargo test --package inferadb-ledger-raft -- determinism --nocapture
```

This replays the same transactions on multiple state machines and verifies identical state roots.

### Version Compatibility

Before upgrading:

1. Review changelog for state machine changes
2. Test upgrade path in staging
3. Coordinate rolling upgrades (followers first, then leader)
4. Monitor for divergence during and after upgrade

## Runbook: Complete Vault Rebuild

For severe corruption or when automatic recovery fails repeatedly:

```bash
# 1. Stop the affected node
systemctl stop ledger

# 2. Backup current state (for investigation)
cp -r /var/lib/ledger/state /var/lib/ledger/state.corrupt.$(date +%Y%m%d)

# 3. Remove vault state only (preserve Raft log)
ledger-admin clear-vault-state --namespace 1 --vault 123 --data-dir /var/lib/ledger

# 4. Start the node
systemctl start ledger

# 5. Monitor recovery (will replay from snapshot)
ledger-admin watch-recovery --namespace 1 --vault 123

# 6. Verify health
ledger-admin vault-status --namespace 1 --vault 123
```

## Metrics and Alerts

### Key Metrics

| Metric                                    | Alert Threshold | Meaning                      |
| ----------------------------------------- | --------------- | ---------------------------- |
| `vault_health{state="diverged"}`          | > 0             | Vault is diverged            |
| `vault_health{state="recovering"}`        | > 0 for 10m     | Recovery taking too long     |
| `vault_recovery_attempts_total`           | > 3             | Automatic recovery exhausted |
| `state_root_computation_duration_seconds` | p99 > 100ms     | Performance issue            |

### Alert Responses

**Alert: `VaultDiverged`**

```
severity: critical
action: Page on-call, begin investigation
```

**Alert: `VaultRecoveryExhausted`**

```
severity: critical
action: Manual intervention required
runbook: See "Manual Recovery" section above
```

## FAQ

### Q: Does a diverged vault affect other vaults?

No. Vault divergence is isolated. Other vaults in the same shard continue operating normally. This is by design—see DESIGN.md §8.5 "Multi-Vault Failure Isolation".

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
