# CLAUDE.md — services

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

RPC service implementations for all 14 external services, shared service-layer helpers, JWT engine, server assembly. Each service implements the wire-protocol trait emitted by `define_protocol!` in `crates/wire-services/`; this crate is where the wire transport meets the state layer. Bugs here leak internal IDs, skip audit, or break version negotiation — every client sees them.

## Load-Bearing Files

These files are load-bearing — their invariants ripple beyond the local file. Not off-limits; use caution and understand the ramifications before editing.

| File                             | Reason                                                                                                                                                              |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/services/slug_resolver.rs`  | Slug ↔ ID boundary. A bug here leaks internal `i64` IDs over the wire or fails to validate slug ownership.                                                          |
| `src/services/wire_helpers.rs`   | `wire_error_with_correlation()` + `build_wire_error()` — the single path for RPC error responses. Bypassing them strips correlation IDs and structured context.    |
| `src/services/error_classify.rs` | Raft/consensus error classification → `WireError` codes. `classify_raft_error` is the canonical helper.                                                             |
| `src/api_version.rs`             | `ApiVersionLayer` interceptor. Version-exempt list is deliberately small (`Health`, `SystemDiscovery`, `Raft`); additions need written justification.               |
| `src/jwt.rs`                     | Ed25519 signing / verification. Changes require security review.                                                                                                    |
| `src/server.rs`                  | `LedgerServer` assembly — wires all 14 services + consensus + interceptors.                                                                                         |

## Owned Surface

- **14 `*Service` structs** (one per service in the `define_protocol!` invocation). All in `src/services/`:
  - Read, Write, Admin, Organization, Vault, Schema, User, Invitation, App, Events, Health, Raft, plus `TokenServiceImpl` (renamed — `TokenService` is the wire-generated trait) and `DiscoveryService` (impls `SystemDiscoveryService`). Each `*_wire.rs` companion file holds the wire-trait method bodies.
- **Region routing**: `RegionResolverService` (`services/region_resolver.rs`) backs `ResolveRegionLeader` on SystemDiscovery.
- **Shared helpers** (all in `src/services/`): `error_classify`, `helpers`, `wire_helpers`, `service_infra`, `slug_resolver`, `auth_errors`. The legacy `metadata.rs` / `error_details.rs` / `tonic_compat.rs` shims survive only to bridge a small number of callers that still construct `tonic::Status` for `LedgerWireDispatcher` adapter glue; new code targets `WireError` directly via `wire_helpers`.
- **`SlugResolver`, `JwtEngine`, `ApiVersionLayer`, `ProposalService` trait**.
- **Per-vault proposal entry point**: `ProposalService::propose_organization_request_to_vault` (`src/proposal.rs`) — the canonical primitive for vault-scoped writes under per-vault consensus. Every entity-mutating handler (`WriteServiceImpl::write`, ingest, etc.) routes through this method. It accepts `(region, organization_id, vault_id, organization_slug, vault_slug, request, caller, timeout)`, locates the per-vault `VaultGroup`, and proposes through that vault's `ConsensusHandle`. Returns `WireError` with `ErrorCode::Unavailable` + `LeaderHint` context (carrying the vault's `(organization_slug, vault_slug, leader_node_id, term)`) when the local node is not the vault leader; the SDK's `VaultLeaderCache` consumes the hint and reconnects directly.
- **Operator vault RPCs**: `AdminService::admin_list_vaults`, `show_vault`, `repair_vault` (`services/admin.rs`) — list, inspect, and best-effort repair per-vault Raft groups on the queried node. Backs the `inferadb-ledger vaults {list,show,repair}` CLI subcommand in `crates/server/src/main.rs`.
- **`LedgerServer`** (`server.rs`).

## Test Patterns

- Unit tests per service file with mocked `ProposalService`.
- Validation tests exercise `ValidationConfig` whitelist + size-limit checks in `helpers.rs`.
- `WireError` decode tests assert every error variant round-trips through `wire_error_with_correlation` → SDK decode.
- Integration tests live in `crates/server/tests/` (not here) — single-binary per root rule 13.

## Local Golden Rules

1. **Every RPC resolves slugs at the top of the handler via `SlugResolver`.** Passing a `*Slug` into `StorageEngine` or `StateLayer` is a bug caught by `wire-reviewer`.
2. **All RPC errors return via `wire_error_with_correlation()`** (root rule 12). Constructing a `WireError` directly inside a handler loses correlation IDs and trace context; use `build_wire_error()` to assemble structured errors and `wire_error_with_correlation()` to stamp the frame metadata.
3. **Version-exempt list is exactly `Health`, `SystemDiscovery`, `Raft`.** Adding a service to the exempt list requires a written justification (K8s probe, peer-to-peer Raft transport, pre-negotiation client). Audited by `wire-reviewer`.
4. **`SubmitRegionalProposal` is the only forwarding RPC.** Do not extend it into general cross-region proxying or add a second forwarding RPC.
5. **Mutations emit `AuditEvent` via `AuditLogger`.** Every new mutating RPC on `WriteService` or `AdminService` needs an audit hook. Read-only RPCs (get / list) are not audited.
6. **User-controlled strings go through `ValidationConfig`** — character whitelist + length + size limits in `helpers.rs`. Don't accept raw user input into state without validation.
7. **Set `RequestContext` domain fields at the top of the handler**, before consuming the request body. `set_vault_slug`, `set_user_id`, etc. enable canonical log lines; order matters.
8. **Extract wire-frame metadata via `WireRequestContext` at the top of the handler.** Drop the request body only after copying `request_id` / `trace_id` into the local `RequestContext` — otherwise correlation IDs are lost.
9. **Leader-based errors map to `ErrorCode::Unavailable`, snapshot-in-progress to `ErrorCode::FailedPrecondition`.** Use `classify_raft_error` — manual `WireError` construction for known Raft conditions strips retry classification on the SDK side.
10. **Circuit-breaker policy on the SDK side is non-retryable.** Service code returns the normal error; the SDK decides not to retry based on `CircuitOpen`. Don't add server-side retry logic for client errors.
