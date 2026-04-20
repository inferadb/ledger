# Runbooks

Incident-response runbooks for on-call. Each runbook is task-oriented: symptom → diagnosis → remediation.

If you're landing here during an active incident, scan the **symptom table** below and jump to the matching runbook. If no symptom matches, start with [troubleshooting.md](../troubleshooting.md) for a broader symptom index or escalate per your incident-response policy.

## Symptom → Runbook

| Symptom / alert                                                          | Runbook                                                                          |
| ------------------------------------------------------------------------ | -------------------------------------------------------------------------------- |
| Vault returns `VaultUnavailable`, state-root mismatch, divergence alert  | [vault-repair.md](vault-repair.md)                                               |
| Peer send queue filling up, high drop rate, consensus replication stalls | [consensus-transport-backpressure.md](consensus-transport-backpressure.md)       |
| Leader changes frequently; SDK leader-cache flapping                     | [leader-cache-diagnosis.md](leader-cache-diagnosis.md)                           |
| Node won't join cluster; registry churn; peer-reachability alerts        | [node-connection-registry.md](node-connection-registry.md)                       |
| Client receives `NotLeader` unexpectedly; redirect routing anomalies     | [architecture-redirect-routing.md](architecture-redirect-routing.md)             |
| Minority survives a failure; quorum loss; full-cluster restore needed    | [disaster-recovery.md](disaster-recovery.md)                                     |

See `../troubleshooting.md` for the broader symptom index that routes to these runbooks plus common issues that don't warrant a full runbook.

## Scheduled Playbooks

These are maintenance procedures, not incident responses — planned work with a defined window rather than on-call pages.

| Task                                                                     | Playbook                                                       |
| ------------------------------------------------------------------------ | -------------------------------------------------------------- |
| Version upgrade (currently pre-GA — full cluster wipe + restore)         | [rolling-upgrade.md](rolling-upgrade.md)                       |
| Weekly backup integrity verification                                     | [backup-verification.md](backup-verification.md)               |
| Cluster scaling (add/remove nodes)                                       | [scaling.md](scaling.md)                                       |
| Encryption-key provisioning and rotation                                 | [key-provisioning.md](key-provisioning.md)                     |

(A dedicated `playbooks/` directory split may come in a future phase — for now scheduled work lives alongside incident runbooks but is separated in this index.)

## Runbook shape

Every runbook in this directory (incident-response ones) should contain these sections:

1. **Symptom** — what an operator or dashboard sees first.
2. **Alert / Trigger** — the specific alert name or metric threshold that pages this runbook.
3. **Blast radius** — what's affected (single vault, single node, single region, whole cluster) and what downstream systems may notice.
4. **Preconditions** — what must be true before acting (authority to intervene, tools available, backups current).
5. **Steps** — numbered recovery steps, each ending in a verifiable outcome.
6. **Verification** — how to confirm recovery succeeded.
7. **Rollback** — how to undo if a step makes things worse.
8. **Escalation** — when to page next layer of on-call, and who.

Optional ninth: **Post-incident actions** — what to file / log / follow up on after the immediate fire is out.

This shape is enforced by the `documentation-reviewer` agent (invariant #18). Scheduled playbooks are exempt from sections 1–3 but still benefit from Preconditions / Steps / Verification / Rollback structure.

## Related

- [troubleshooting.md](../troubleshooting.md) — higher-level symptom index; fast triage before opening a runbook.
- [alerting.md](../alerting.md) — recommended Prometheus thresholds that fire the alerts above.
- [metrics-reference.md](../metrics-reference.md) — authoritative metric catalog.
- [errors.md](../errors.md) — `ErrorCode` reference, mapped to remediation.
