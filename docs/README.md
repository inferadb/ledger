# InferaDB Ledger — Documentation

Operator and contributor documentation. Structured by [Diátaxis](https://diataxis.fr/): each document is a tutorial, how-to guide, reference, or explanation — the directory tells you which.

## Start here

- **First time?** Read [overview.md](overview.md) — what Ledger is and how its pieces fit together.
- **Evaluating?** See [faq.md](faq.md) — common questions about the trust model, consistency, and design choices.
- **Deploying?** Start with the tutorial in [getting-started/](getting-started/production-deployment.md).
- **On-call, paged?** Go to [runbooks/](runbooks/README.md) and scan the symptom → runbook table.

## The tiers

### [getting-started/](getting-started/) — tutorials

Learning-oriented, single-happy-path, reproducible in one sitting.

- [Production deployment](getting-started/production-deployment.md) — step-by-step Kubernetes deployment.

### [reference/](reference/) — reference

Information-oriented, comprehensive lookup. Consistent entry shape per item.

- [Configuration](reference/configuration.md) — every CLI flag, env var, default, and validator.
- [Metrics](reference/metrics.md) — every Prometheus metric (includes per-organization resource accounting).
- [Alerting](reference/alerting.md) — recommended Prometheus thresholds and PromQL rules.
- [SLOs](reference/slo.md) — service-level objectives and SLI definitions.
- [API versioning](reference/api-versioning.md) — client/server compatibility and headers.
- [Errors](reference/errors.md) — `ErrorCode` catalog: gRPC status, retryability, suggested action.

### [how-to/](how-to/) — goal-oriented guides

Task-oriented. Preconditions + steps + outcome.

- [Deployment](how-to/deployment.md) — cluster setup, bootstrap, backup, restore.
- [Logging](how-to/logging.md) — enabling + querying canonical log lines.
- [Profiling](how-to/profiling.md) — flamegraph capture and performance investigation.
- [Observability cost](how-to/observability-cost.md) — cost-tuning for paid observability vendors.
- [Troubleshooting](how-to/troubleshooting.md) — symptom index routing to runbooks.
- [Capacity planning](how-to/capacity-planning.md) — sizing and resource estimation.

### [architecture/](architecture/) — explanation

Understanding-oriented. Why things are the way they are.

- [Durability](architecture/durability.md) — WAL, checkpoints, recovery contract.
- [Security](architecture/security.md) — trust model, TLS, encryption at rest, hardening.
- [Events](architecture/events.md) — audit event pipeline, catalog, retention.
- [Multi-region](architecture/multi-region.md) — geographic distribution patterns.
- [Region management](architecture/region-management.md) — organization-to-region routing mechanics.
- [Data residency](architecture/data-residency.md) — GLOBAL/REGIONAL split, PII isolation, crypto-shredding.
- [Request routing](architecture/request-routing.md) — redirect-based client routing to regional leaders.
- [Background jobs](architecture/background-jobs.md) — scheduled jobs inside the node.

## Operational artefacts

### [runbooks/](runbooks/) — incident response

Task-oriented, on-call-paged procedures. Each runbook has the 8-section shape (Symptom / Alert / Blast radius / Preconditions / Steps / Verification / Rollback / Escalation).

- [Disaster recovery](runbooks/disaster-recovery.md) — parent runbook for catastrophic failures.
- [Vault repair](runbooks/vault-repair.md) — diverged vaults.
- [Quorum loss](runbooks/quorum-loss.md) — minority of voters surviving.
- [Snapshot restore failure](runbooks/snapshot-restore-failure.md) — learner can't install snapshot.
- [PII erasure failure](runbooks/pii-erasure-failure.md) — stuck or failing `EraseUser`.
- [Key rotation failure](runbooks/key-rotation-failure.md) — RMK / blinding-key rotation stalled.
- [Leader cache diagnosis](runbooks/leader-cache-diagnosis.md) — SDK leader-cache flapping.
- [Consensus transport backpressure](runbooks/consensus-transport-backpressure.md) — peer send queue saturating.
- [Node connection registry](runbooks/node-connection-registry.md) — channel-pool drift.

### [playbooks/](playbooks/) — scheduled maintenance

Task-oriented, planned-window procedures. 6-section shape (Purpose / Preconditions / Steps / Verification / Rollback / Escalation).

- [Rolling upgrade](playbooks/rolling-upgrade.md)
- [Backup verification](playbooks/backup-verification.md)
- [Scaling](playbooks/scaling.md)
- [Key provisioning](playbooks/key-provisioning.md)

### [dashboards/](dashboards/) — observability templates

Importable Grafana, Kibana, and Datadog dashboards. See [dashboards/README.md](dashboards/README.md) for import steps per platform.

### [testing/](testing/) — test strategy (internals / contributors)

- [README](testing/README.md) · [property](testing/property.md) · [simulation](testing/simulation.md) · [fuzzing](testing/fuzzing.md)

## Related

- [DESIGN.md](../DESIGN.md) (repo root) — canonical system design.
- [WHITEPAPER.md](../WHITEPAPER.md) (repo root) — public-facing architecture summary.
- Per-crate architecture lives in each crate's own `CLAUDE.md` (e.g. `crates/consensus/CLAUDE.md`).

## Quick reference

| Task               | Command                                                            |
| ------------------ | ------------------------------------------------------------------ |
| Start single node  | `inferadb-ledger --listen 0.0.0.0:50051 --data /tmp/ledger`        |
| Initialize cluster | `inferadb-ledger init --host localhost:50051`                      |
| Health check       | `grpcurl -plaintext localhost:50051 ledger.v1.HealthService/Check` |
| View metrics       | `curl localhost:9090/metrics` (requires `--metrics 0.0.0.0:9090`)  |
