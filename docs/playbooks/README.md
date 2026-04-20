# Playbooks

Scheduled maintenance procedures for InferaDB Ledger. These are planned work, not incident response — run during designated maintenance windows.

For **incident response** (on-call pages, unexpected failures), see [../runbooks/](../runbooks/README.md).

## Index

| Playbook                                           | When to run                                                                |
| -------------------------------------------------- | -------------------------------------------------------------------------- |
| [Rolling Upgrade](rolling-upgrade.md)              | Version upgrades. Pre-GA: full cluster wipe + restore from backup.         |
| [Backup Verification](backup-verification.md)      | Scheduled (weekly full restore test, daily integrity check).               |
| [Scaling](scaling.md)                              | Add or remove voters; replace-in-place via leave-and-join.                 |
| [Key Provisioning](key-provisioning.md)            | Provision RMKs to new nodes; scheduled rotation of Region Master Keys.     |

## Playbook shape

Each playbook follows a lighter contract than an incident runbook (it isn't triggered by a page):

1. **Purpose** — when to run, expected duration, who runs it.
2. **Preconditions** — what must be true before starting.
3. **Steps** — the procedure proper.
4. **Verification** — how to confirm success.
5. **Rollback** — how to undo (or escalate to forward-fix) if the procedure misbehaves.
6. **Escalation** — when and whom to page if something goes wrong mid-procedure.

The full content of each file is split into this 6-section quick reference (at the top) plus a **Deep reference** section (retained detailed material).

## Relationship to runbooks

A playbook may escalate into an incident runbook if the maintenance procedure fails in a specific way:

| Playbook            | Possible escalation                                                                      |
| ------------------- | ---------------------------------------------------------------------------------------- |
| rolling-upgrade     | Post-upgrade determinism divergence → [../runbooks/vault-repair.md](../runbooks/vault-repair.md) |
| backup-verification | Backup itself corrupt → page storage/backup pipeline owner (no runbook — compliance incident) |
| scaling             | New learner can't catch up → [../runbooks/snapshot-restore-failure.md](../runbooks/snapshot-restore-failure.md). Quorum lost during operation → [../runbooks/quorum-loss.md](../runbooks/quorum-loss.md) |
| key-provisioning    | Rotation stalls → [../runbooks/key-rotation-failure.md](../runbooks/key-rotation-failure.md) |

## Related

- [Runbooks](../runbooks/README.md) — incident-response runbooks.
- [Troubleshooting](../how-to/troubleshooting.md) — symptom → runbook routing.
- [Documentation home](../README.md) — operator documentation index.
