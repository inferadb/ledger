---
name: new-rpc
description: Checklist for adding a new RPC method to InferaDB Ledger's wire protocol. Use when adding or modifying a service method in `crates/wire-services/src/lib.rs` (the `define_protocol!` invocation), or when the user says "add an RPC", "new endpoint", "new wire method", etc. Covers wire types, define_protocol! registration, service impl, slug resolution, error enrichment, SDK client, audit + validation, quota, and tests.
---

# new-rpc

Adding a wire-protocol RPC in this codebase touches 5–7 files across 3 crates. Missing a step leaves a subtly broken surface — wrong ID types leaking externally, `WireError` not enriched with correlation, audit logs missing, SDK out of parity. This skill is the checklist.

## Before you start

Confirm with the user:

1. **Service** — which of the 14 wire services (`Read`, `Write`, `Organization`, `Vault`, `Schema`, `Admin`, `User`, `Invitation`, `App`, `Token`, `Events`, `Health`, `SystemDiscovery`, `Raft`)?
2. **Mutation or read?** — determines audit hook, quota check, Raft-proposal path.
3. **Tenancy** — organization-scoped, vault-scoped, or global?
4. **ID surface** — which slug types does it expose (`OrganizationSlug`, `VaultSlug`, etc.)?
5. **Failure modes** — what structured `WireError` context (validation, quota, rate limit, not-found, leader-hint) does the operation need?

## Checklist

### 1. Wire request/response types (`crates/wire/src/services/<service>.rs`)

- [ ] Add `pub struct <Method>Request { ... }` and `pub struct <Method>Response { ... }`.
- [ ] Derive `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`. Add `Hash` only if needed.
- [ ] External-facing ID fields use **Snowflake slug newtypes** (`OrganizationSlug`, `VaultSlug`, etc.) — never raw `u64` and never internal `*Id(i64)`.
- [ ] Binary fields use `bytes::Bytes` (not `Vec<u8>`).
- [ ] Map fields use `BTreeMap<K, V>` (not `HashMap`) — postcard requires deterministic key order.
- [ ] Optional timestamps use `Option<u64>` (UNIX nanoseconds), not `Option<SystemTime>`.
- [ ] Add a postcard round-trip test covering the new type in the same module.

### 2. Register the RPC in `define_protocol!` (`crates/wire-services/src/lib.rs`)

- [ ] Add a new `rpc <Method>(...) -> ... = <opcode>;` line under the appropriate `service <ServiceName>` block.
- [ ] Opcode MUST fall in `[base, base + 0xFF]` for that service. The macro enforces this at compile time (range-overlap check).
- [ ] Pick the next free opcode in the service's range — avoid renumbering existing entries (clients in flight use the old numbers).
- [ ] Reserved range `0x0001..0x000F` is for transport control opcodes — don't put service RPCs there.

### 3. Service implementation (`crates/services/src/services/<service>_wire.rs`)

- [ ] Implement the wire-generated trait method emitted by the macro — the signature is `async fn <method>(&self, ctx: RequestContext, req: <Method>Request) -> Result<<Method>Response, WireError>`.
- [ ] At the boundary: translate slugs → internal IDs via `SlugResolver` (`crates/services/src/services/slug_resolver.rs`).
- [ ] Internal state/store calls use internal `*Id` newtypes — never pass slugs into the state layer.
- [ ] For mutations that go through Raft: propose via the `ProposalService` trait; do not write to storage directly.
- [ ] Use `RequestContext` (from `crates/wire/src/context.rs`) for `deadline`, `correlation_id`, `trace_id`, transport metadata. Don't synthesize these from request fields.

### 4. Validation + request size

- [ ] User-controlled strings go through `ValidationConfig` whitelist + length checks (`crates/services/src/services/helpers.rs`).
- [ ] Validation errors return a `WireError` with `ErrorCode::InvalidArgument` and `context["field"] = "<field name>"`.

### 5. Error enrichment (`wire_helpers`)

- [ ] Build `WireError` via `wire_helpers::build_wire_error(...)` for actionable failures (rate limit, quota, leader-hint, validation) — populates `context` map and `suggested_action`.
- [ ] Wrap the final result via `wire_error_with_correlation(ctx, err)` (in `wire_helpers.rs`) so `request_id` / `trace_id` are stamped into `WireError.context` for the response-frame headers.
- [ ] Map Raft errors via `classify_raft_error_wire`:
  - Leadership / unavailability → `ErrorCode::Unavailable`
  - Snapshot-in-progress → `ErrorCode::FailedPrecondition`
  - Auth failures → `ErrorCode::Unauthenticated`
  - Permission denied → `ErrorCode::PermissionDenied`
  - Rate limit → `ErrorCode::RateLimited` with `retry_after_ms` populated.
- [ ] For `NotLeader`, build the `WireError.context` map via the `not_leader_wire_error_*` family — encodes leader hint coordinates so the SDK's `RegionLeaderCache` / `VaultLeaderCache` can redirect.

### 6. Audit + quotas

- [ ] Mutations on `WriteService` or `AdminService` emit an `AuditEvent` via the `AuditLogger`.
- [ ] Multi-tenant write RPCs check `QuotaChecker` (vault count, storage bytes).
- [ ] Rate limit check via `check_rate_limit()` if the RPC is throttleable.

### 7. SDK client (`crates/sdk/src/`)

- [ ] Add the client method on `LedgerClient`. Signature uses the **slug** (the typed newtype, accessed via `.value()` for the wire field), never the internal ID.
- [ ] Wrap in `with_retry_cancellable(method, &pool, ...)` — pass the method-name string for retry metrics + circuit-breaker key.
- [ ] Classify error retryability in `SdkError::error_type()`. `CircuitOpen` is non-retryable.
- [ ] Decode `WireError` into `SdkError` — store `ServerErrorDetails` as `Box<ServerErrorDetails>` (clippy `result_large_err` requires the box).
- [ ] Emit SDK metrics via the `SdkMetrics` trait (`record_request`, `record_retry`).

### 8. Tracing

- [ ] At the service boundary, read trace context from `RequestContext` (the dispatcher already extracts it from `FrameHeader`).
- [ ] For RPCs that fan out to other nodes: rebuild `RequestContext` at the destination, preserving the `deadline` and trace IDs.

### 9. Tests

- [ ] **Wire types**: postcard round-trip test in the wire module.
- [ ] **Service**: integration test in `crates/server/tests/integration.rs` (as a submodule — `use crate::common::`, never `mod common;`). Use `TestCluster` and `allocate_ports`.
- [ ] **SDK**: client test in `crates/sdk/tests/`. SDK e2e tests fail with transport errors in local dev by design — that's not a regression.

### 10. CI gate

- [ ] `just check-quick` for fast iteration.
- [ ] `just ci` as the final gate (`fmt-check` + `clippy` + `doc-check` + `test`). No "pre-existing issue" exceptions.

## Common mistakes

- **Leaking `i64` IDs**: A wire request with `organization_id: i64` instead of `organization: OrganizationSlug`. The `SlugResolver` exists specifically to prevent this.
- **`HashMap` in a wire payload**: Postcard requires deterministic key order — use `BTreeMap`.
- **`Vec<u8>` in a wire payload**: Use `bytes::Bytes` for binary fields (zero-copy on the receive path).
- **Building `WireError` by hand inside a handler**: Skips correlation stamping. Always go through `wire_error_with_correlation`.
- **Missing audit**: Write-side mutations without `AuditEvent` — CI won't catch this, but compliance will.
- **Hand-editing macro-emitted code**: The `define_protocol!` macro generates server traits, client structs, and dispatch fns. Never edit the expansion — change the macro input or the proc macro itself.
- **Reusing an opcode**: Cross-service collisions are caught at compile time. Within a service, picking a number already in use silently breaks dispatch — always pick the next free opcode in the service's `[base, base + 0xFF]` range.

## References

- `CLAUDE.md` — golden rules 1, 4, 7, 11, 12 (wire types, dual-ID, snafu vs thiserror, slug newtypes, error enrichment).
- `.claude/agents/wire-reviewer.md` — full audit checklist (this skill is the build-time companion).
- `crates/wire/src/services/*.rs` — request/response type templates.
- `crates/wire-services/src/lib.rs` — the canonical service catalog.
- `crates/services/src/services/wire_helpers.rs` — `build_wire_error`, `wire_error_with_correlation`, `classify_raft_error_wire`.
- `docs/architecture/wire-protocol.md` (if present) — frame layout, opcode allocation, version negotiation.
