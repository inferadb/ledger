---
name: new-rpc
description: Checklist for adding a new gRPC RPC method to InferaDB Ledger. Use when adding or modifying a service method in proto/ledger/v1/*.proto, or when the user says "add an RPC", "new endpoint", "new gRPC method", etc. Covers proto, conversions, service impl, slug resolution, error enrichment, SDK client, version exempt-list, audit + validation, quota, and tests.
---

# new-rpc

Adding a gRPC method in this codebase touches 6–10 files across 4 crates. Missing a step leaves a subtly broken surface — wrong ID types leaking externally, ErrorDetails not attached, audit logs missing, SDK mismatch. This skill is the checklist.

## Before you start

Confirm with the user:

1. **Service** — which of the 13 services (Read, Write, Admin, Organization, Vault, User, App, Token, Invitation, Events, Health, Discovery, Raft)?
2. **Mutation or read?** — determines audit hook, quota check, Raft proposal path.
3. **Tenancy** — organization-scoped, vault-scoped, or global?
4. **ID surface** — which slug types does it expose (`OrganizationSlug`, `VaultSlug`, etc.)?
5. **Failure modes** — what structured errors (validation, quota, rate limit, not-found) need `ErrorDetails` context?

## Checklist

### 1. Proto definition (`proto/ledger/v1/*.proto`)

- [ ] Add the `rpc` line inside the right service.
- [ ] Define request + response messages in the same or referenced `.proto` file.
- [ ] External-facing ID fields use **Snowflake slugs** (`u64`), never internal `int64` IDs. Use `OrganizationSlug`, `VaultSlug`, etc.
- [ ] Run `just proto` to regenerate. Confirm `just proto-lint` passes.

### 2. Proto conversions (`crates/proto/src/convert/`)

- [ ] Add `From`/`TryFrom` impls for each new message. `TryFrom` when validation can fail.
- [ ] Place in the correct submodule: `domain.rs`, `identifiers.rs`, `operations.rs`, `statuses.rs`, `tokens.rs`, `credentials.rs` — or create a new one if none fits.
- [ ] Add unit tests to `convert/tests.rs`. Add proptests for round-trip (`to_proto ∘ from_proto = id`) if the message has non-trivial structure.

### 3. Service implementation (`crates/services/src/`)

- [ ] Implement the method on the appropriate service impl.
- [ ] At the boundary: translate slugs → internal IDs via `SlugResolver`.
- [ ] Internal state/store calls use internal `*Id` newtypes — never pass slugs into state layer.
- [ ] For mutations that go through Raft: propose via `ProposalService` trait; do not write to storage directly.
- [ ] Extract request context (trace, correlation, transport metadata) **before** `request.into_inner()` consumes metadata.

### 4. Validation + request size (`helpers.rs`)

- [ ] User-controlled strings go through `ValidationConfig` whitelist + length checks.
- [ ] Validation errors wrap through `validation_status()` for consistent gRPC surface.

### 5. Error enrichment (`metadata.rs` + `error_details.rs`)

- [ ] Return errors via `status_with_correlation()` so `ErrorDetails` auto-attaches.
- [ ] For actionable errors (rate limit, quota, not-found, retryable-but-deferred): build rich `ErrorDetails` with populated `context` map + `suggested_action`.
- [ ] Map Raft errors with `classify_raft_error`:
  - Leadership → `UNAVAILABLE`
  - Snapshot-in-progress → `FAILED_PRECONDITION`

### 6. Audit + quotas

- [ ] Mutations on `WriteService` or `AdminService` emit an `AuditEvent` via the `AuditLogger`.
- [ ] Multi-tenant write RPCs check `QuotaChecker` (vault count, storage bytes).
- [ ] Rate limit check via `check_rate_limit()` if the RPC is throttleable.

### 7. API version negotiation

- [ ] New RPCs are subject to `ApiVersionLayer` by default. Do NOT add the service to the exempt list (`Health`, `Discovery`, `Raft` only) without justification.
- [ ] If the RPC has a version-gated field, document it in `docs/operations/api-versioning.md`.

### 8. SDK client (`crates/sdk/src/`)

- [ ] Add the client method. Signature uses the **slug** (`u64`), never the internal ID.
- [ ] Wrap in `with_retry_cancellable` — pass the `method` name + `ConnectionPool` for circuit-breaker + metrics.
- [ ] Classify error retryability in `SdkError::error_type()`.
- [ ] Decode server `ErrorDetails` via `decode_error_details()` — store as `Box<ServerErrorDetails>` (clippy `result_large_err`).
- [ ] Emit SDK metrics via the `SdkMetrics` trait (`record_request`, `record_retry`).

### 9. Tracing

- [ ] For RPCs forwarded internally: propagate trace context via `ForwardClient` child spans.
- [ ] Extract trace context from request metadata at the service boundary.

### 10. Tests

- [ ] Service-level integration test in `crates/server/tests/integration.rs` (as a submodule — no `mod common;`, use `crate::common::`).
- [ ] SDK test in `crates/sdk/tests/`.
- [ ] Property tests for conversion invariants if applicable.

### 11. CI gate

- [ ] Run `just ready` (proto + fmt + clippy + test) since proto was regenerated.
- [ ] Then `just ci` as the final gate.

## Common mistakes

- **Leaking `i64` IDs**: Accepting `OrganizationId` in a request proto, or returning `vault.id` instead of `vault.slug`. The `SlugResolver` exists specifically to prevent this.
- **Missing ErrorDetails**: Returning `Status::internal("...")` directly skips enrichment. Always go through `status_with_correlation()`.
- **Forgetting audit**: Write-side mutations without `AuditEvent` — CI won't catch this, but compliance will.
- **Exempting from version layer**: Adding the new service to the exempt list because tests fail. The right fix is to include the version header in tests, not exempt the service.
- **SDK drift**: Server accepts a new field; SDK doesn't send it. Check for parity in every new request type.
- **Proto-only change**: Regenerated proto without running `just proto` — stale generated code in `crates/proto/src/generated/`.

## References

- `DESIGN.md` — architecture overview
- `docs/operations/api-versioning.md` — version header contract
- `docs/errors.md` — `ErrorCode` enum + classification
- `CLAUDE.md` — data residency (Pattern 1/2/3), storage key prefixes, dual-ID rules
