# Snapshot Restore Failure

On-call runbook for a catching-up node that cannot install a snapshot — the node is stuck as a learner, the snapshot transfer from the leader fails repeatedly, and the node never reaches `applied = committed`.

## Symptom

- A newly joined node (or a restarted node that fell behind) is stuck in the `learner` state and never promotes to voter.
- `AdminService.GetClusterInfo` on the leader shows a voter count below expected; the stuck node appears as a learner with a stale `match_index`.
- Log messages from the affected node mention `install_snapshot` failures, checksum mismatches, or zstd decode errors.
- `rate(ledger_snapshots_created_total)` may be rising on the leader (leader re-sending) while `ledger_state_last_synced_snapshot_id` stays flat on the learner.

## Alert / Trigger

- `LedgerSnapshotInstallFailing` — logged `install_snapshot` error rate on any node > 0 for 10 minutes.
- Supporting evidence: `ledger_snapshot_restore_duration_seconds_count` flat on the affected node while leader's `ledger_snapshots_created_total` rises.

## Blast radius

- **Scope**: the one catching-up node. The rest of the cluster keeps serving — quorum is preserved if the failing node was only a learner or if the cluster still has a majority without it.
- **Downstream impact**: **none immediately**. The cluster continues operating. Risk accumulates over time: without the failing node catching up, the cluster is one node short of its intended replication factor, reducing fault tolerance.

## Preconditions

- Read access to node logs on both the sender (leader) and receiver (stuck learner).
- Access to `AdminService.GetClusterInfo` + `GetNodeInfo`.
- Authority to restart the affected node / wipe its `data_dir` if a clean retry is needed.

## Steps

1. **Identify the failing node and the failing snapshot**: look at `install_snapshot` error logs on the learner. The error type tells you the failure mode:
   - **Checksum mismatch** → snapshot bytes corrupted in transit or at source.
   - **zstd decode error** → same class — corrupted or truncated.
   - **Unknown table id** → learner running a version that pre-dates a snapshot table introduced by the leader (version skew).
   - **IO error / disk full** → learner's `data_dir` is out of space or the disk is failing.
2. **Check disk space on the learner**: `df -h /var/lib/ledger` (or equivalent). Out-of-space is the most common cause.
3. **Check version skew**: compare binaries/images on leader and learner. A pre-1.0 cluster requires identical versions (no snapshot compatibility across versions — see the [rolling-upgrade playbook](../playbooks/rolling-upgrade.md)). If skewed, upgrade the learner to match.
4. **If checksum/decode errors recur**: the snapshot on the leader may itself be corrupt. Trigger a new snapshot on the leader (`AdminService.CreateSnapshot`) and let the learner re-attempt.
5. **Clean-retry**: stop the learner, wipe its `data_dir` (preserve a forensic copy first), start it with `--join` pointing at the leader. This forces a fresh `install_snapshot` path.
6. **If clean-retry still fails and the snapshot is valid** (other learners can install it successfully): the specific learner has a local corruption or configuration issue — file a bug with captured logs and consider retiring that node.

## Verification

- Learner log shows `install_snapshot` success followed by catch-up complete.
- `AdminService.GetClusterInfo` promotes the learner to voter within the normal catch-up window.
- `ledger_state_last_synced_snapshot_id` on the learner matches the leader.
- `ledger_cluster_quorum_status` = 1 after the learner is promoted (only relevant if membership changes occurred during recovery).

## Rollback

The learner has no committed state to lose (it's catching up from zero by definition). Wiping `data_dir` is the rollback. The parent cluster is unaffected throughout — a stuck learner cannot corrupt the leader or other voters.

If you suspect the snapshot source (leader) is corrupt, restore the leader's state from backup **only after confirming** via another learner that the snapshot really is bad. A single learner failing to install a snapshot is not evidence of source corruption on its own.

## Escalation

- Checksum/decode errors persist across multiple fresh snapshot transfers on a clean `data_dir`: page the consensus / snapshot owner; likely a code-level bug in `crates/raft/src/snapshot.rs` or `log_storage/raft_impl.rs`.
- Snapshot fails on a specific table ID only: likely schema/format change — page the state-layer owner.
- Multiple learners fail to install from the same leader: the leader's snapshot is bad — escalate to [disaster-recovery.md](disaster-recovery.md) to decide whether to replace the leader's data from backup.

## Related

- [Scaling playbook](../playbooks/scaling.md) — this runbook is typically invoked when a scale-up operation stalls.
- [disaster-recovery.md](disaster-recovery.md) — parent runbook for the broader failure taxonomy.
- [rolling-upgrade playbook](../playbooks/rolling-upgrade.md) — version-skew is a common root cause.
- [DESIGN.md § Snapshot consistency](../../DESIGN.md#system-invariants) — the invariant this procedure maintains.
