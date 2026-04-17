# CLAUDE.md — services

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

gRPC service implementations for all 14 external services, shared service-layer helpers, JWT engine, server assembly. This is where everything below the wire meets everything above it. Bugs here leak internal IDs, skip audit, or break version negotiation — every client sees them.

## Load-Bearing Files

These files are load-bearing — their invariants ripple beyond the local file. Not off-limits; use caution and understand the ramifications before editing.

| File                             | Reason                                                                                                                                                              |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/services/slug_resolver.rs`  | Slug ↔ ID boundary. A bug here leaks internal `i64` IDs over the wire or fails to validate slug ownership.                                                          |
| `src/services/metadata.rs`       | `status_with_correlation()` — the single path for gRPC error responses. Bypassing it strips `ErrorDetails` from every response it misses.                           |
| `src/services/error_details.rs`  | `build_error_details()` — canonical `ErrorDetails` builder. Don't add specialized variants (they were removed as dead code).                                        |
| `src/services/error_classify.rs` | Raft/consensus error classification → gRPC status codes. `classify_raft_error` is the canonical helper.                                                             |
| `src/api_version.rs`             | `ApiVersionLayer` interceptor + tower layer. Version-exempt list is deliberately small (`Health`, `SystemDiscovery`, `Raft`); additions need written justification. |
| `src/jwt.rs`                     | Ed25519 signing / verification. Changes require security review.                                                                                                    |
| `src/server.rs`                  | `LedgerServer` assembly — wires all 14 services + consensus + interceptors.                                                                                         |

## Owned Surface

- **14 `*Service` structs** (one per proto service). All in `src/services/`:
  - Read, Write, Admin, Organization, Vault, Schema, User, Invitation, App, Events, Health, Raft, plus `TokenServiceImpl` (renamed — `TokenService` is the proto trait) and `DiscoveryService` (impls `SystemDiscoveryService`).
- **Region routing**: `RegionResolverService` (`services/region_resolver.rs`) backs `ResolveRegionLeader` on SystemDiscovery.
- **Shared helpers** (all in `src/services/`): `error_classify`, `error_details`, `helpers`, `metadata`, `service_infra`, `slug_resolver`.
- **`SlugResolver`, `JwtEngine`, `ApiVersionLayer`, `ProposalService` trait**.
- **`LedgerServer`** (`server.rs`).

## Test Patterns

- Unit tests per service file with mocked `ProposalService`.
- Validation tests exercise `ValidationConfig` whitelist + size-limit checks in `helpers.rs`.
- `ErrorDetails` decode tests assert every error variant round-trips through `status_with_correlation` → SDK decode.
- Integration tests live in `crates/server/tests/` (not here) — single-binary per root rule 13.

## Local Golden Rules

1. **Every RPC resolves slugs at the top of the handler via `SlugResolver`.** Passing a `*Slug` into `StorageEngine` or `StateLayer` is a bug caught by `proto-reviewer`.
2. **All gRPC errors return via `status_with_correlation()`** (root rule 12). `tonic::Status::new(...)` or `Status::internal(...)` by hand loses `ErrorDetails`, correlation ID, trace ID, and classification metadata.
3. **Version-exempt list is exactly `Health`, `SystemDiscovery`, `Raft`.** Adding a service to the exempt list requires a written justification (K8s probe, peer-to-peer Raft transport, pre-negotiation client). Audited by `proto-reviewer`.
4. **`RegionalProposal` is the only forwarding RPC.** Do not extend it into general cross-region proxying or add a second forwarding RPC.
5. **Mutations emit `AuditEvent` via `AuditLogger`.** Every new mutating RPC on `WriteService` or `AdminService` needs an audit hook. Read-only RPCs (get / list) are not audited.
6. **User-controlled strings go through `ValidationConfig`** — character whitelist + length + size limits in `helpers.rs`. Don't accept raw user input into state without validation.
7. **Set `RequestContext` domain fields at the top of the handler**, before `request.into_inner()` consumes metadata. `set_vault_slug`, `set_user_id`, etc. enable canonical log lines; order matters.
8. **Extract transport metadata before consuming the request.** `request.metadata().clone()` before `request.into_inner()` — otherwise trace context and correlation metadata are lost.
9. **Leader-based errors map to `UNAVAILABLE`, snapshot-in-progress to `FAILED_PRECONDITION`.** Use `classify_raft_error` — manual `Status::internal()` for known Raft conditions strips retry classification on the SDK side.
10. **Circuit-breaker policy on the SDK side is non-retryable.** Service code returns the normal error; the SDK decides not to retry based on `CircuitOpen`. Don't add server-side retry logic for client errors.
