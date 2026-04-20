# Architecture

Explanation-oriented: why Ledger is built the way it is. Discursive prose, design rationale, trade-offs.

These docs are load-bearing for the internals-reader audience. If you want to know *how* to do something, see [how-to/](../how-to/). If you need an exhaustive lookup, see [reference/](../reference/).

## Index

| Doc                                        | Explains                                                            |
| ------------------------------------------ | ------------------------------------------------------------------- |
| [Durability](durability.md)                | WAL batching, barrier fsync, StateCheckpointer, crash recovery.     |
| [Security](security.md)                    | Trust model, network security, encryption at rest, hardening.      |
| [Events](events.md)                        | Audit-event pipeline, catalog, retention.                          |
| [Multi-region](multi-region.md)            | Geographic distribution patterns; regional vs stretched clusters.   |
| [Region management](region-management.md)  | Region enum, organization-to-region assignment, write forwarding.   |
| [Data residency](data-residency.md)        | GLOBAL/REGIONAL split, PII isolation, crypto-shredding, compliance. |
| [Request routing](request-routing.md)      | Redirect-based client routing to regional Raft leaders.             |
| [Background jobs](background-jobs.md)      | Scheduled jobs that run inside the node.                            |

## Related

- [DESIGN.md](../../DESIGN.md) (repo root) — the canonical system-design document.
- [WHITEPAPER.md](../../WHITEPAPER.md) (repo root) — public-facing architecture summary.
- Per-crate architecture lives in each crate's own `CLAUDE.md`.
