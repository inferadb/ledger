# Runbooks

Incident-response runbooks for on-call. Each runbook is task-oriented: symptom → diagnosis → remediation.

If you're here during an active incident, scan the **symptom table** below and jump to the matching runbook. If no symptom matches, start with [../troubleshooting.md](../how-to/troubleshooting.md) for a broader symptom index, or escalate per your incident-response policy.

For **planned maintenance** (upgrades, scaling, scheduled backup verification, key provisioning), see [../playbooks/](../playbooks/README.md). Those are not on-call runbooks.

## Symptom → Runbook

| Symptom / alert                                                           | Runbook                                                                          |
| ------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Vault returns `VaultUnavailable`, state-root mismatch, divergence alert   | [vault-repair.md](vault-repair.md)                                               |
| Peer send queue filling up, high drop rate, consensus replication stalls  | [consensus-transport-backpressure.md](consensus-transport-backpressure.md)       |
| Leader changes frequently; SDK leader-cache flapping                      | [leader-cache-diagnosis.md](leader-cache-diagnosis.md)                           |
| Node won't join cluster; registry churn; peer-reachability alerts         | [node-connection-registry.md](node-connection-registry.md)                       |
| Quorum lost; cluster can't elect a leader; minority of voters surviving   | [quorum-loss.md](quorum-loss.md)                                                 |
| Catching-up learner can't install snapshot; stuck as learner              | [snapshot-restore-failure.md](snapshot-restore-failure.md)                       |
| `UserService.EraseUser` fails; user stuck in Deleting; compliance risk    | [pii-erasure-failure.md](pii-erasure-failure.md)                                 |
| RMK or blinding-key rotation stalls; rewrap progress flat                 | [key-rotation-failure.md](key-rotation-failure.md)                               |
| Catastrophic multi-scenario failure (node loss / corruption / regional)   | [disaster-recovery.md](disaster-recovery.md) (parent runbook; routes to others)  |

See [../troubleshooting.md](../how-to/troubleshooting.md) for broader symptom triage that routes to these runbooks plus common fixes that don't warrant a full runbook.

## Runbook shape

Every incident runbook in this directory contains these sections as H2 headings (enforced by the `documentation-reviewer` agent's invariant #18):

1. **Symptom** — what an operator or dashboard sees first.
2. **Alert / Trigger** — the specific alert name or metric threshold that pages this runbook.
3. **Blast radius** — what's affected (single vault, single node, single region, whole cluster) and what downstream systems may notice.
4. **Preconditions** — what must be true before acting (authority to intervene, tools available, backups current).
5. **Steps** — numbered recovery steps, each ending in a verifiable outcome.
6. **Verification** — how to confirm recovery succeeded.
7. **Rollback** — how to undo if a step makes things worse.
8. **Escalation** — when to page next layer of on-call, and who.

Optional ninth: **Post-incident actions** — what to file / log / follow up on after the immediate fire is out.

Detailed long-form content (diagrams, alternate procedures, historical context) lives in a `## Deep reference` section below the required eight. The required sections are the load-bearing part for on-call; the deep reference is commentary.

Scheduled playbooks use a lighter 6-section shape — see [../playbooks/README.md](../playbooks/README.md).

## Related

- [../troubleshooting.md](../how-to/troubleshooting.md) — higher-level symptom index; fast triage before opening a runbook.
- [../alerting.md](../reference/alerting.md) — recommended Prometheus thresholds that fire the alerts above.
- [../metrics-reference.md](../reference/metrics.md) — authoritative metric catalog.
- [../errors.md](../reference/errors.md) — `ErrorCode` reference, mapped to remediation.
- [../playbooks/](../playbooks/README.md) — scheduled maintenance procedures.
