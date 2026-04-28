# CLAUDE.md — sdk

> Extends [root CLAUDE.md](../../CLAUDE.md). Root rules always take precedence.

## Purpose

Consumer-facing Rust client. Every external caller — first-party services, third-party integrators — uses this crate. **The one workspace crate that uses `thiserror` instead of `snafu`**, because error ergonomics matter more here than server-side diagnostic detail. Retry, cancellation, leader caching, and circuit-breaker behavior live here; a regression is visible to every consumer.

## Load-Bearing Files

These files are load-bearing — their invariants ripple beyond the local file. Not off-limits; use caution and understand the ramifications before editing.

| File                     | Reason                                                                                                                                                                 |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/error.rs`           | `SdkError` shape. `ServerErrorDetails` must stay `Box`-wrapped — bare `Option<ServerErrorDetails>` inflates `Result<T, SdkError>` and trips clippy `result_large_err`. |
| `src/retry.rs`           | `with_retry_cancellable` loop. Retry ordering + cancellation via `tokio::select! { biased; }`; a naive `backon` retry loop cannot be cancelled.                        |
| `src/client.rs`          | `LedgerClient` retry cycle, metrics emission, connection-pool integration.                                                                                             |
| `src/circuit_breaker.rs` | State machine (Closed / Open / HalfOpen). A bug here either hammers failing endpoints or rejects healthy ones.                                                         |

## Owned Surface

- **`LedgerClient`** — public API: authenticate, read/write, admin, leader discovery.
- **Config** (`src/config.rs`): `ClientConfig`, `TlsConfig`, `CircuitBreakerConfig` — all bon fallible builders.
- **Errors** (`src/error.rs`): `SdkError`, `ResolverError` (thiserror).
- **Connection layer**: `connection.rs`, `circuit_breaker.rs`, leader caching via `region_resolver.rs` (region-level) and `vault_resolver.rs` (per-vault).
- **Vault leader cache** (`src/vault_resolver.rs`): `VaultLeaderCache` — slug-keyed `(OrganizationSlug, VaultSlug)` LRU map, term-gated. Populated from server-emitted `LeaderHint` carrying `(organization_slug, vault_slug, leader_node_id, term)` (see `LeaderHint` in `src/error.rs`). Updated on every `NotLeader` reply; reads on every retry to short-circuit the next hop directly to the vault leader.
- **Metrics**: `SdkMetrics` trait + `NoopSdkMetrics` / `MetricsSdkMetrics` in `src/metrics.rs`.
- **Retry entry point**: `with_retry_cancellable(method, pool, ...)` in `src/retry.rs` — used by every RPC call in `client.rs`.
- **Streaming / ops**: `src/streaming.rs`, `src/ops/`.

## Removed surface

- **`LedgerClient::batch_write` and `BatchWriteRequest` / `BatchWriteResponse` / `BatchWriteOperation` / `BatchWriteSuccess`** — removed in Phase 6 of the per-vault consensus migration. Cross-vault atomic writes are not supported under per-vault consensus (every vault is its own Raft group; a single proposal cannot span vaults). Callers that previously issued `batch_write` now issue a per-vault `write` per target vault. Server-side already returns `FAILED_PRECONDITION` + `AppDeprecated` for any legacy proto wire; the client-side surface is gone.

## Test Patterns

- Unit tests use `mock` fixtures (`src/mock/`) and mocked `tonic::Channel`.
- Retry tests use fake endpoints with known-failing addresses to exercise the `tokio::select!` cancellation path.
- E2E tests (`tests/`) require a running cluster; they fail with transport errors in local dev by design.
- Circuit-breaker tests parameterize thresholds; never assume production defaults.

## Local Golden Rules

1. **`thiserror` only.** Do not import `snafu` types into SDK error definitions. The crate boundary is contractual: server crates are snafu, SDK is thiserror.
2. **`ServerErrorDetails` is boxed** — `Option<Box<ServerErrorDetails>>` on every error variant that carries it. Bare `Option<ServerErrorDetails>` inflates `Result<T, SdkError>` (clippy `result_large_err`).
3. **`with_retry_cancellable` requires `method: &str`.** Used for retry metrics labels and circuit-breaker key. Call sites that pass `""` lose observability; call sites that pass a dynamic string blow up label cardinality.
4. **bon `Option<T>` setters accept `T`, not `Option<T>`.** `.field(Some(x))` is a compile error — write `.field(x)`. bon auto-wraps.
5. **`#[builder(default)]` on `Option<T>` is redundant.** bon auto-infers `None`; adding the attribute produces a compiler error.
6. **Leader cache updates are term-gated.** Hints for older terms MUST be dropped — a stale reply reaching the cache first would overwrite a newer leader and cause a retry storm. Both `RegionLeaderCache` (region-level) and `VaultLeaderCache` (per-vault, keyed on `(OrganizationSlug, VaultSlug)`) follow this rule. `LeaderHint` carries `(leader_node_id, term, organization_slug, vault_slug)` so `VaultLeaderCache` can be populated directly from the hint without a slug→id round trip.
7. **`SdkError::CircuitOpen` is non-retryable.** The retry loop exits immediately when the circuit opens; otherwise retries would hammer an open circuit.
8. **Routing is redirect-only.** The SDK reconnects directly to the leader on `NotLeader` + `LeaderHint`. Never ask the server to forward client requests — server-side forwarding is reserved for saga orchestration (see root rule 11).
9. **UDS endpoints bypass TCP keep-alive.** Endpoints starting with `/` are treated as Unix-socket paths; don't apply TCP-specific settings to them.

10. **`ConnectionPool.channels` is the single source of truth for cached channels.** Never reintroduce a singular `Option<Channel>` slot — the pool round-robins across `connection_pool_size` independent tonic Channels via `cached_channel()`, and any direct `Option<Channel>` cache shadows the pool and silently routes every request through one Buffer worker. Each tonic `Channel` wraps a tower `Buffer` (single mpsc worker → single hyper HTTP/2 connection); a single Channel saturates around 24-30k ops/s on loopback when in-flight RPCs pile up in the buffer queue. Pool size `> 1` materializes N independent Channels and round-robins requests across them, scaling dispatch parallelism linearly with pool size up to the server's per-connection ceiling. Cache invalidation (`clear_pool` / `reset` / leader-hint redirect) drops the entire pool atomically — never just one entry. Tests live next to the helper in `src/connection.rs` (`cached_channel_round_robins_across_pooled_entries`, `pool_size_reflects_config`, `connection_pool_size_zero_is_rejected`).
