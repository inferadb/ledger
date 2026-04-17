# services

gRPC service implementations, `ProposalService` trait, JWT engine, server assembly. This is the crate that wires everything below it into the 13 external services.

## The 13 services

Read, Write, Admin, Organization, Vault, User, App, Token, Invitation, Events, Health, Discovery, Raft.

| Type | File | Purpose |
| --- | --- | --- |
| `LedgerServer` | `server.rs` | gRPC server assembly: all 13 services + consensus |
| `SlugResolver` | `services/slug_resolver.rs` | External slug ↔ internal ID translation at the RPC boundary |
| `ApiVersion` interceptor + tower layer | `api_version.rs` | `x-ledger-api-version` request validation + response header |
| `JwtEngine` | `jwt.rs` | Ed25519 signing; scope (`Global`/`Organization`); status (`Active`/`Rotated`/`Revoked`) |
| `ProposalService` trait | `proposal.rs` | Abstraction for proposing writes to Raft |

## RPC conventions

- **All RPCs accept slugs externally and internal IDs never cross the gRPC boundary.** Resolve with `SlugResolver` at the top of the handler.
- **Health / Discovery / Raft are exempt from API version checks** — K8s probes, openraft, and pre-negotiation clients hit these before version handshake.
- Attach `ErrorDetails` via `raft::services::metadata::status_with_correlation`. Don't build `tonic::Status` manually.
- Wide events: set domain context on `RequestContext` (e.g., `set_vault_slug()`, `set_user_id()`) — enables canonical log lines.
- Audit hooks live on `WriteService` and `AdminService`. Every mutating RPC emits an `AuditEvent`.

## `SubmitRegionalProposal`

This is the **only** server-to-server forwarding RPC. It exists for saga orchestration — a region-leader submitting proposals to a peer region's leader. Do not extend it into a general "forward any client request" mechanism.

## Tonic details

- Interceptors are request-only. For response headers (like API version), use a tower `Layer` applied via `Server::builder().layer(...)`.
- `tonic::codegen::http` re-exports `http` — no extra workspace dep needed.
- Per-service request interception: `*ServiceServer::with_interceptor(service, fn)`.

## Related tooling

- Skill: `/new-rpc`, `/add-new-entity`, `/add-proto-conversion`
- Agent: `proto-reviewer` (every proto change touches this crate)
