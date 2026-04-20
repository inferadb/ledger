# Encryption Key Rotation Failure

On-call runbook for stalled or failing encryption-key rotations. Applies to both **Region Master Keys** (RMK rotation via `AdminService.RotateRegionKey`) and **email-blinding keys** (via `AdminService.RotateBlindingKey`).

## Symptom

- `AdminService.RotateRegionKey` / `RotateBlindingKey` RPC returns non-OK, or the accompanying background re-wrap progress halts.
- `ledger_dek_rewrap_progress` or `ledger_invitation_rehash_progress` gauges stop advancing.
- `AdminService.GetRewrapStatus` returns `complete: false` past the expected window.
- Some nodes appear to lack the new key version (startup validation failures).

## Alert / Trigger

- `LedgerRewrapStalled` — `changes(ledger_dek_rewrap_progress[1h]) == 0` while `AdminService.GetRewrapStatus` reports in-progress.
- `LedgerBlindingKeyRehashStalled` — equivalent for email-hash rehash (`AdminService.GetBlindingKeyRehashStatus`).
- `LedgerRotateKeyError` — non-OK response rate from `RotateRegionKey` or `RotateBlindingKey` elevated.

## Blast radius

- **Scope** (RMK): region-specific. A stalled rotation means new writes encrypt with the new version but existing ciphertext remains under the old version; until re-wrap completes, the cluster must retain both key versions for reads.
- **Scope** (blinding key): cluster-wide — affects email HMAC indices. Email-based lookups continue working with mixed-version hashes during the rehash window.
- **Downstream impact**: rotation stall is **not** a data-loss risk as long as the old key version remains accessible. The risk is compliance (key ages beyond policy) and the blast radius if a rotation was triggered by key compromise (old key still in use).

## Preconditions

- The old key version is still accessible to every node's key source — confirm via the secrets manager before taking any destructive action.
- Authority to re-drive rotation RPCs.
- Access to `AdminService.GetRewrapStatus` / `GetBlindingKeyRehashStatus`.

## Steps

1. **Determine which rotation is affected**: RMK (per-region) or blinding key (cluster-wide). Each has a different status RPC and different background job.
2. **Confirm every node sees both key versions**: check each node's startup log; for RMK, `ledger_rmk_versions_loaded` should include both old and new. If any node is missing the new version, the key source wasn't updated for that node — fix that first.
3. **Call the status RPC** (`AdminService.GetRewrapStatus` for RMK, `GetBlindingKeyRehashStatus` for blinding key) and record the reported progress.
4. **Inspect the background job's logs**: `journalctl -u ledger --since "1 hour ago" | grep -iE "rewrap|rehash"`. The job logs each batch; a persistent error on a specific page or key points to corruption or a residency-tier violation.
5. **Classify**:
   - **Resource-bound stall** (disk I/O, CPU saturation): the job is running but slow. Verify progress is advancing over a 10-minute window; if yes, no action needed. If no: check node resource telemetry.
   - **Error-bound stall** (specific key / page failing): inspect the logged error. File a bug if corruption; restart the job if transient. Do not destroy the old key version.
   - **Missing-key-version stall** (node can't see the new key): fix the key source on the affected node, restart it, let the job resume.
6. **Re-trigger the job** if it's not running: the relevant `RotateRegionKey` / `RotateBlindingKey` RPC is idempotent on re-call — it resumes from where the background job stopped.

## Verification

- `AdminService.GetRewrapStatus` (or the blinding-key equivalent) reports `complete: true`.
- Relevant progress gauge reached its expected terminal value.
- Post-rotation read + write smoke test against a sample vault succeeds.
- Post-rotation, the **old** key version can be decommissioned only after confirming no on-disk ciphertext references it (verify via `AdminService.GetRewrapStatus` for RMK, or the rehash status for blinding keys).

## Rollback

Key rotation is forward-only. You cannot roll the active version back once propagated via GLOBAL Raft — the cluster has committed the change. What you **can** do:

- Keep the old key version available in the secrets manager until re-wrap completes; do not retire it prematurely.
- If the new key itself is bad (generated incorrectly, compromised during provisioning), roll forward to a new version — bad key → new bump, not a rollback.
- If the rotation was triggered in error: pause here (the job is harmless; ciphertext will continue re-wrapping to the current active version) and plan the corrective forward rotation.

## Escalation

- Rotation stall with logged errors on specific keys / pages: page the state-layer owner; may indicate storage corruption.
- Node consistently refuses to load a key version despite the key being present in the secrets manager: page the key-management owner (IAM / secrets-access issue).
- Compliance deadline at risk because rotation is not completing: notify compliance / legal per your incident-response policy.

## Related

- [Key Provisioning playbook](../playbooks/key-provisioning.md) — normal provisioning and scheduled rotation.
- [Security](../architecture/security.md) — threat model, envelope encryption, and blinding-key semantics.
- [Data Residency Architecture](../architecture/data-residency.md) — how keys map to regions.
