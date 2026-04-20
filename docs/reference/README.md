# Reference

Comprehensive, consistent, opinion-free lookup. Each entry has the same shape; no discursive prose.

If you're looking for *how* to do something, see [how-to/](../how-to/). For *why* things are a certain way, see [architecture/](../architecture/).

## Index

| Reference                              | Covers                                                                     |
| -------------------------------------- | -------------------------------------------------------------------------- |
| [Configuration](configuration.md)      | Every CLI flag, env var, default, validator.                              |
| [Metrics](metrics.md)                  | Every Prometheus metric (+ per-organization resource accounting subsection). |
| [Alerting](alerting.md)                | Recommended Prometheus thresholds and PromQL rules.                        |
| [SLOs](slo.md)                         | Service-level objectives and SLI definitions.                              |
| [API versioning](api-versioning.md)    | Client/server compatibility and the `x-ledger-api-version` header.         |
| [Errors](errors.md)                    | `ErrorCode` catalog: gRPC status, retryability, suggested action.          |

## Authoritative sources

Each reference doc anchors to canonical code:

- Configuration → `crates/types/src/config/*` + `crates/server/src/config.rs`
- Metrics → `crates/raft/src/metrics.rs` + `crates/sdk/src/metrics.rs`
- Errors → `crates/types/src/error_code.rs`
- API version → `crates/services/src/api_version.rs`

When these sources change, the references in this directory must change in the same PR. The `documentation-reviewer` agent enforces this.
