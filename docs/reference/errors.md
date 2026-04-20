# Error Codes

Operator-facing reference for every `ErrorCode` variant the Ledger state machine returns. The SDK surfaces these through `ServerErrorDetails.error_code`; logs and audit records include them verbatim; dashboards split by them.

Canonical source: `crates/types/src/error_code.rs` (the `ErrorCode` enum).

## How errors reach the operator

1. The Raft state machine returns `LedgerResponse::Error { code, message }` where `code` is an `ErrorCode` variant.
2. The service layer maps `code` to a gRPC status via `ErrorCode::grpc_code_name()` and attaches an `ErrorDetails` payload (retryability, suggested action, context map) via `status_with_correlation()` in `crates/services/src/services/metadata.rs`.
3. The SDK decodes `ErrorDetails` into `ServerErrorDetails`; structured logging records the variant.
4. Operators see the variant name in logs, in dashboard filters, and in SDK error surfaces.

## Variants

| Variant                       | gRPC status          | Retryable? | Typical cause                                                   | Suggested action                                                                 |
| ----------------------------- | -------------------- | ---------- | --------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `NotFound`                    | `NOT_FOUND`          | No         | Slug resolves to no entity (organization / vault / user / …).   | Verify the resource exists; re-check the slug.                                   |
| `AlreadyExists`               | `ALREADY_EXISTS`     | No         | Duplicate name or slug collision.                               | Treat as idempotent-success; no retry needed.                                    |
| `FailedPrecondition`          | `FAILED_PRECONDITION`| No         | State doesn't satisfy the operation's precondition.             | Re-read current state; retry with updated values.                                |
| `PermissionDenied`            | `PERMISSION_DENIED`  | No         | Caller lacks permission for this operation.                     | Check the JWT scopes / role assignment.                                          |
| `InvalidArgument`             | `INVALID_ARGUMENT`   | No         | Bad input (missing field, size limit exceeded, invalid chars).  | Fix request parameters to conform to field limits.                               |
| `Internal`                    | `INTERNAL`           | No         | Unexpected server-side failure (storage error, serialization).  | Check server logs; file a bug if reproducible.                                   |
| `Unauthenticated`             | `UNAUTHENTICATED`    | No         | Invalid/expired/revoked token or bad signature.                 | Reauthenticate; obtain a fresh token.                                            |
| `RateLimited`                 | `RESOURCE_EXHAUSTED` | **Yes**    | Request rate exceeded the configured bucket.                    | Retry with exponential backoff; honour `retry_after_ms` if set.                  |
| `Expired`                     | `FAILED_PRECONDITION`| No         | Verification code / onboarding token / challenge has expired.   | Request a fresh code/token.                                                      |
| `TooManyAttempts`             | `FAILED_PRECONDITION`| No         | Too many failed verification or auth attempts.                  | Wait for the lockout window to elapse; then request a fresh challenge.           |
| `InvitationRateLimited`       | `RESOURCE_EXHAUSTED` | **Yes**    | Invitation-specific rate limit hit (per-user / per-org / cooldown). | Retry with exponential backoff after the cooldown.                            |
| `InvitationAlreadyResolved`   | `FAILED_PRECONDITION`| No         | Invitation is no longer `Pending` (accepted/declined/expired/revoked). | Treat as terminal; no action.                                               |
| `InvitationEmailMismatch`     | `NOT_FOUND`          | No         | Invitee's email doesn't match the accepting user's email. (Maps to `NOT_FOUND` for privacy — avoids confirming invitation existence.) | Ensure the accepting user signs in with the invited email. |
| `InvitationAlreadyMember`     | `ALREADY_EXISTS`     | No         | Invitee's email already belongs to an organization member.      | Treat as idempotent-success.                                                     |
| `InvitationDuplicatePending`  | `ALREADY_EXISTS`     | No         | A Pending invitation already exists for this org + email pair.  | Resolve or revoke the existing invitation before issuing a new one.              |
| `StaleRouting`                | `FAILED_PRECONDITION`| **Yes**    | Vault routing changed between slug resolution and proposal submission (rare; a region migration or split). | Retry — the SDK's `RegionLeaderCache` re-resolves and reroutes automatically on retry. |

> **Note**: The gRPC status codes `DEADLINE_EXCEEDED`, `UNAVAILABLE`, and `ABORTED` are **not** emitted by state-machine `ErrorCode` variants — they surface at the transport / consensus layer (client deadline, `NotLeader`, Raft conflict). The SDK treats them as retryable with backoff.

## SDK behaviour

The SDK's retry logic (in `crates/sdk/src/client.rs`) reads `ServerErrorDetails.is_retryable` and the gRPC status code. You do not need to special-case individual `ErrorCode` variants client-side — the server already encodes retryability in `ErrorDetails` — but dashboards, audit log splits, and operator alerting rules should filter / group by `error_code` for meaningful breakdowns.

## Related

- [Troubleshooting](../how-to/troubleshooting.md) — symptom index routing to specific remediations.
- [Alerting](alerting.md) — thresholds for error-rate alerts.
- [Metrics reference](metrics.md) — `ledger_grpc_requests_total` carries an `error_class` label derived from these codes.
- Canonical source: [`crates/types/src/error_code.rs`](../../crates/types/src/error_code.rs).
