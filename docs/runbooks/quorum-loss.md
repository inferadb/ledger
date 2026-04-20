# Quorum Loss

On-call runbook for the narrow scenario where a Raft group has lost quorum but a **minority of nodes survives**. Use this runbook when you still have data on surviving nodes — the cluster has not been totally lost. For total cluster loss (all nodes gone), use [disaster-recovery.md § Full Cluster Restore](disaster-recovery.md#full-cluster-restore).

## Symptom

- Writes return `UNAVAILABLE` indefinitely.
- `ledger_cluster_quorum_status` = 0 for > 2 minutes.
- Cluster-info RPC shows a minority of voters reachable (e.g. 1 of 3, 2 of 5).
- No leader has been elected within the election timeout; elections keep failing because there are not enough voters to form a quorum.

## Alert / Trigger

- `LedgerClusterQuorumLost` — `ledger_cluster_quorum_status == 0` sustained for 2 minutes.
- Supporting evidence: `inferadb_ledger_raft_is_leader` sums to 0 across the cluster; `raft_elections_total` rising without any election succeeding.

## Blast radius

- **Scope**: the affected Raft group only — typically one region, but the GLOBAL group being affected is cluster-wide. Other regions' Raft groups keep operating.
- **Downstream impact**: writes to the affected group fail; reads continue to work via `ReadIndex` against surviving nodes for eventually-consistent paths; linearizable reads fail.

## Preconditions

- Confirmation that the missing majority is **permanently gone** (not a transient network partition). This is the single most important check in this runbook — re-bootstrapping a minority while a majority with different data still exists creates split-brain with permanent divergence.
- Authority to perform destructive operations (the recovery involves re-bootstrapping, which abandons the old cluster identity).
- Recent backup available (ideally from before the failure event) and verified per [backup-verification playbook](../playbooks/backup-verification.md).
- Access to surviving node's `data_dir`.

## Steps

**STOP and confirm permanent loss before proceeding.** If the missing majority might come back (power outage, network partition healing, hosts recovering), wait for them. Re-bootstrapping a surviving minority while the majority is merely unreachable is split-brain.

1. **Confirm permanent loss**: verify with infrastructure providers that the missing hosts / disks are unrecoverable. Document this confirmation before proceeding.
2. **Stop writes at the application layer** to prevent new writes landing on a cluster that's about to be re-bootstrapped.
3. **Preserve forensics**: copy each surviving node's `data_dir` aside (e.g. `cp -r /var/lib/ledger /var/lib/ledger.pre-requorum.$(date +%Y%m%d-%H%M%S)`).
4. **Pick the surviving node with the highest `applied_durable` index** — this is the most up-to-date data. Query each survivor's `AdminService.GetNodeInfo` to identify.
5. **Stop the surviving nodes** (other than the chosen one) if they are still running.
6. **Re-bootstrap the chosen node as a new cluster**:
   ```bash
   # (still with the surviving data_dir)
   inferadb-ledger --listen 0.0.0.0:50051 --data /var/lib/ledger &
   inferadb-ledger init --host localhost:50051
   ```
   This creates a new Raft group identity from the surviving data. The old cluster identity is permanently abandoned.
7. **Add replacement voters** by starting new nodes with `--join localhost:50051`. Wait for each to catch up and be promoted.
8. **Resume writes** at the application layer.

## Verification

- `ledger_cluster_quorum_status` returns to 1.
- `AdminService.GetClusterInfo` shows the expected voter count (3 or 5 nodes).
- Read + write smoke test against a sample vault succeeds.
- Pre-incident block heights on a sampled set of vaults match or exceed what was recorded before step 1 (survivor had the highest committed index).

## Rollback

Rollback is not possible once step 6 (`init`) executes — the new cluster identity replaces the old one permanently. The forensic copies from step 3 allow you to **restart from backup** instead of re-bootstrapping the survivor, if you realise mid-flow that the recovery is going wrong:

1. Stop the re-bootstrapped node.
2. Wipe its `data_dir`.
3. Restore from the pre-incident backup.
4. Run [disaster-recovery § Full Cluster Restore](disaster-recovery.md#full-cluster-restore).

This is a full-restore cycle (longer recovery time) but preserves backed-up data integrity.

## Escalation

- Uncertain whether the missing majority is permanently gone: **do not proceed.** Page the incident commander and wait for infrastructure confirmation.
- Survivor data shows `ledger_state_root_divergences_total > 0`: the survivor itself may have been part of the failure; restore from backup instead of re-bootstrapping — see [disaster-recovery § Full Cluster Restore](disaster-recovery.md#full-cluster-restore).
- Multiple regions lost quorum simultaneously: this is a coordinated failure; invoke full incident response and coordinate with [disaster-recovery.md](disaster-recovery.md).

## Related

- [disaster-recovery.md](disaster-recovery.md) — parent runbook for the full range of catastrophic scenarios.
- [backup-verification playbook](../playbooks/backup-verification.md) — preconditions you rely on before invoking this runbook.
- [DESIGN.md § Quorum requirement](../../DESIGN.md#system-invariants) — the safety invariant that underlies this procedure.
