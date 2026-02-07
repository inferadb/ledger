# ADR: Network Trust Model (No Application-Layer Authentication)

## Context

InferaDB Ledger is a blockchain database that stores authorization data — entities, relationships, and cryptographic chains. A natural question is whether Ledger itself should authenticate and authorize incoming requests.

### Deployment Architecture

Ledger runs as an internal service within a private network boundary (WireGuard VPN, Kubernetes with NetworkPolicy, or VPC). Two upstream services are the sole callers:

- **Engine** — the authorization decision engine that queries Ledger for permission checks
- **Control** — the user/organization management service that writes authorization data

Both Engine and Control authenticate end-users before forwarding requests to Ledger. Ledger never receives direct traffic from external clients.

### Options Evaluated

| Option                              | Latency Impact     | Operational Complexity | Description                                         |
| ----------------------------------- | ------------------ | ---------------------- | --------------------------------------------------- |
| No application auth (network trust) | None               | Low                    | Rely on network perimeter for access control        |
| Mutual TLS (mTLS)                   | +0.5–2ms handshake | High                   | Client certificate verification on every connection |
| Per-request API tokens              | +0.1–1ms per RPC   | Medium                 | JWT/HMAC validation on every request                |
| Built-in RBAC                       | +0.5–5ms per RPC   | High                   | Ledger manages its own roles and permissions        |

## Decision

**Rely on network-level access control with no application-layer authentication.** Ledger trusts all inbound connections by design.

### Rationale

**1. Circular dependency avoidance.** Ledger IS the storage layer for authorization data. If Ledger needed to authorize its own access, it would depend on itself — creating a bootstrap problem. Engine queries Ledger to answer "can user X do Y?" — Ledger cannot query itself for the same question.

**2. Sub-millisecond read latency.** Read operations target p99 < 10ms. Adding token validation (crypto operations or database lookups) to every RPC would consume a significant fraction of this budget. The health probe path (`phase()`) is a single atomic load — any lock acquisition would be strictly worse.

**3. Separation of concerns.** Authentication belongs at the API gateway (Engine/Control). Ledger's responsibility is durable, consistent storage with cryptographic integrity — not identity verification. This follows the sidecar/mesh pattern used by Istio, Linkerd, and similar infrastructure.

**4. Actor field for audit.** The `Transaction.actor` field (e.g., `"user:alice"`, `"system:engine"`) is provided by the caller for audit logging. Ledger records what the caller claims, not a cryptographically verified identity. This is intentional — audit attribution is the caller's responsibility.

### Why Not mTLS?

TLS is supported for transport encryption (defense in depth) but not enforced for access control:

- Certificate rotation requires coordinated rollout across all services
- Certificate management adds operational complexity (CA infrastructure, renewal automation)
- Kubernetes service meshes (Istio, Linkerd) can provide transparent mTLS without application changes
- No benefit over network-level isolation when all callers are trusted internal services

### Why Not JWT/OAuth2?

- Adds 0.1–1ms per request for token parsing, signature verification, and expiry checks
- Requires token issuance infrastructure and clock synchronization across the cluster
- Authorization system depending on authorization to function is architecturally unsound

## Consequences

### Positive

- **Zero authentication overhead** on every read and write RPC
- **Simplified codebase** — no credential management, token validation, or session handling code
- **Fewer failure modes** — no auth service dependency, no token expiry issues, no certificate rotation incidents
- **Clear responsibility boundary** — Engine/Control handle identity, Ledger handles storage

### Negative

- **Network perimeter is a single layer of defense** — if breached, Ledger is fully accessible with no secondary check
- **Cannot expose Ledger outside trusted boundary** — direct internet access is never safe
- **Audit attribution is trust-based** — the `actor` field reflects what the caller claims, not cryptographic proof of identity
- **Deployment constraint** — requires WireGuard, VPC, or Kubernetes NetworkPolicy; cannot run on shared networks

### Mitigations

- Default `listen_addr` is `127.0.0.1:50051` (localhost-only) — requires explicit override for cluster deployment
- Optional TLS for transport encryption within the private network
- Input validation (Task 1) and rate limiting (Task 4) protect against malformed or excessive requests from buggy upstream callers
- Audit logging records all mutations with caller-provided actor identity

## References

- `crates/raft/src/server.rs` — Server setup with no auth middleware; only `api_version_interceptor`
- `crates/types/src/types.rs` — `Transaction.actor` field for audit attribution
- `crates/server/src/config.rs` — Default localhost binding, no auth configuration options
- `docs/operations/security.md` — Network trust model documentation
- `DESIGN.md` §Threat Model — "Trusted Operator Assumption"
