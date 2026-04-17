# raft

Saga orchestrator, background jobs, rate limiting, coordination — the glue between `consensus` and `services`. Despite the name, most of this crate is *not* Raft internals (those live in `consensus`).

## Key types

| Type | File | Purpose |
| --- | --- | --- |
| `NodeConnectionRegistry` | `node_registry.rs` | Node-level connection pool — one `Channel` per peer, shared by consensus, discovery, admin, saga forwarding. HTTP/2 multiplexing serves all traffic through one TCP connection per peer. |
| `RateLimiter` | `rate_limit/` (re-exported at crate root) | 3-tier token bucket; `AtomicU64` for runtime-updatable fields |
| `HotKeyDetector` | `hot_key.rs` | Count-Min Sketch for hot-key metrics |
| `SagaOrchestrator` | `saga_orchestrator.rs` | Cross-region saga coordination; submits regional proposals via `SubmitRegionalProposal` RPC |
| Background jobs | `jobs/` | `AutoRecoveryJob`, `BTreeCompactor`, `BackupJob`, `BackgroundJobWatchdog`, divergence-recovery lifecycle |
| `GracefulShutdown` + `ShutdownCoordinator` | `graceful_shutdown.rs` | Six-phase drain — `HealthState` (`AtomicU8`), `watch::Receiver<bool>`, connection tracker |

## Service-layer helpers

These are shared across gRPC service impls in `services`:

- `helpers.rs` — `check_rate_limit()` (builds rich `ErrorDetails` with namespace/level/reason context); `validation_status()` (wraps validation errors).
- `metadata.rs` — `status_with_correlation()` auto-attaches `ErrorDetails` when `status.details().is_empty()`; extracts correlation metadata.
- `services/error_details.rs` — `build_error_details()`: the single `ErrorDetails` builder. Specialized helpers were removed as dead code; don't add them back.

## Client routing

- **Redirect-only from server.** A node that receives a request for a region it does not lead returns `NotLeader` + `LeaderHint` inside `ErrorDetails`. The SDK reconnects directly to the regional leader — the server does not proxy client requests.
- **Server-side forwarding is reserved for saga orchestration** (`SubmitRegionalProposal` RPC), not client-request proxying.
- SDK owns leader caching (`RegionLeaderCache`) — don't add leader-aware state to server-side code.

## Rate limiting + hot-key

- `RateLimiter` is 3-tier (global, organization, vault). All tiers checked on every request; emit rich `ErrorDetails` on denial.
- `HotKeyDetector` is observational only — it exposes Prometheus metrics; it does not reject traffic.

## Error details

Attach via `status_with_correlation()`. Don't construct `tonic::Status` manually when `ErrorDetails` is available — the helper does the right thing.

## Related tooling

- Skill: `/new-rpc`, `/define-error-type`
- Agent: `consensus-reviewer` (saga-orchestrator changes also trip this)
