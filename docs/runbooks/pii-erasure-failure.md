# PII Erasure Failure

On-call runbook for failures in the user-erasure pipeline. Ledger's `EraseUser` flow is compliance-critical — a stuck or failing erasure is a potential regulatory exposure (GDPR / CCPA right-to-erasure).

## Symptom

- `UserService.EraseUser` RPC returns a non-OK status.
- `UserRetentionReaper` background job reports repeated failures in logs.
- `AdminService.ListErasureAudits` shows a user stuck in `Deleting` status past the retention window (typical 30 days + regional offset).
- A user-data request audit indicates erasure was initiated but the `_audit:erasure:{user_id}` record never appeared.

## Alert / Trigger

- `LedgerEraseUserFailed` — `rate(ledger_grpc_requests_total{service="UserService",method="EraseUser",status!="OK"}[5m]) > 0` sustained for 10 minutes.
- `LedgerUserRetentionReaperStalled` — `time() - max(timestamp(changes(ledger_background_job_runs_total{job="user_retention_reaper",result="success"}[10m]) > 0)) > 3600` (no successful cycle in the last hour).

## Blast radius

- **Scope**: one user's PII across a single region's storage (plus GLOBAL directory). Does not affect other users or other regions.
- **Downstream impact**: the user's crypto-shredding key may be deleted without the rest of the erasure steps completing, leaving orphan ciphertext (readable as garbage). Compliance impact: the user-facing promise of erasure is not fulfilled until every step lands.

The 10-step erasure sequence is documented in [PII.md § Destruction Flows](../../PII.md#destruction-flows) and sourced from `crates/state/src/system/service/erasure.rs`. Steps are idempotent — re-running after a partial failure is safe.

## Preconditions

- Authority to re-drive `EraseUser` for the affected user (typically admin-tier token).
- gRPC tooling configured against a GLOBAL-Raft-leader node.
- Read access to audit logs and `journalctl -u ledger --since "…" | grep -i erasure`.
- For root-cause analysis: access to the regional node where the erasure was running.

## Steps

1. **Identify which step failed.** Inspect node logs for `erase_user` entries; each of the 10 steps logs start/finish. Identify the last successful step and the one that errored (e.g. step 6 — `delete_all_user_credentials`).
2. **Classify the failure**:
   - **Transient** (network hiccup, Raft leader transition mid-flow): retry the full RPC. Steps already completed are no-ops on re-execution.
   - **Persistent** (specific step consistently errors): read the error and escalate per the Escalation section — do not mask the root cause by retrying.
3. **Re-drive the erasure**: call `UserService.EraseUser` again with the same `user_id`. The function is forward-only and idempotent per step.
4. **Verify completion** via the Verification section below.
5. **If a specific step has a known cause** (listed under [Deep reference § Known failure modes](#known-failure-modes)), apply the targeted fix before re-running.

## Verification

- `UserService.EraseUser` returns OK.
- `AdminService.GetErasureAudit` for the user returns a record with `erased_at` set.
- `SystemKeys::validate_key_tier` audit shows no residency violations for the user's keys during the erasure flow.
- The user's `_shred:user:{user_id}` key is gone (verified via a dry-run state inspection — do not expose this check on the operator interface).
- The user's primary record at `user:{user_id}` is gone.

## Rollback

**There is no rollback for a partial erasure.** Once the `UserShredKey` is destroyed (step 8), the user's regional PII is cryptographically unrecoverable. If erasure fails *before* step 8, the user's data is intact and re-running the full flow is safe. If failure occurs *at or after* step 8, the remaining cleanup must complete — do not abort halfway.

Forensic preservation: if you suspect a bug is causing the failure, snapshot the regional `state.db` aside *before* re-running so the failing state can be analysed. Re-running the erasure overwrites the evidence.

## Escalation

- Persistent failure in any step: page the state-layer owner (the 10 steps are implemented in `crates/state/src/system/service/erasure.rs`).
- Failure indicates a residency violation (wrong tier, missing `KEY_REGISTRY` entry): page the data-residency owner; this is a compliance bug, not an ops bug.
- Audit record is missing despite steps 1–9 apparently succeeding: page the audit-logging owner; compliance evidence integrity is affected.

## Deep reference

### The 10-step erasure sequence

Forward-only, idempotent on crash-resume. Full detail in [PII.md § User Erasure](../../PII.md#user-erasure) and the source in `crates/state/src/system/service/erasure.rs` (`erase_user`, lines 60–176).

1. Read directory entry to capture slug.
2. Mark directory entry as `Deleted`.
3. Revoke all refresh token families and bump token version.
4. Remove global email hash index entries for this user.
5. Delete all `UserEmail` records and indices.
6. Delete all user credentials and type indices.
7. Delete all pending TOTP challenges.
8. Delete per-subject encryption key (crypto-shred — point of no return).
9. Delete the `User` record.
10. Write erasure audit record.

### Known failure modes

| Step | Symptom | Likely cause | Action |
|------|---------|--------------|--------|
| 4 | `MAX_EMAIL_HASH_SCAN` limit hit | Unusually large email-hash index | Raise the scan limit or file a ticket to paginate the scan |
| 8 | `validate_key_tier` refuses the `_shred:user:{id}` key | `KEY_REGISTRY` entry missing or wrong tier | Residency bug — page the data-residency owner |
| 10 | Audit write to `_audit:erasure:{user_id}` refused | GLOBAL Raft unhealthy | Resolve the consensus issue first, then re-run erasure |

### Related

- [PII.md](../../PII.md) — canonical compliance reference.
- [errors.md](../reference/errors.md) — error codes the SDK surfaces for this flow.
- [data-residency-architecture.md](../architecture/data-residency.md) — the GLOBAL / REGIONAL split that the erasure must respect.
