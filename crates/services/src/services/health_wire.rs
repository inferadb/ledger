//! Wire-protocol implementation for `HealthService` (Phase F.1.f.1.1).
//!
//! Provides the [`inferadb_ledger_wire_services::HealthService`] impl on
//! [`super::health::HealthService`]. The wire impl operates on wire types
//! directly — no proto conversion. This is the only bound transport for
//! the health service.

use std::sync::atomic::Ordering;

use inferadb_ledger_raft::log_storage::VaultHealthStatus;
use inferadb_ledger_wire::{
    RequestContext, WireError,
    services::{health as w, shared as ws},
};

use super::{health::HealthService, slug_resolver::SlugResolver};

// ---------------------------------------------------------------------------
// Wire-trait implementation for `HealthService`.
//
// `SlugResolver::resolve` and `resolve_vault` return `WireError` directly
// (F.1.f.0.1) — no adapter required on the wire path.
// ---------------------------------------------------------------------------

impl inferadb_ledger_wire_services::HealthService for HealthService {
    /// Returns the current health status of the node or a specific vault.
    ///
    /// Mirrors the behaviour documented on the tonic
    /// [`HealthService::check`](super::health::HealthService) impl: when
    /// `request.vault` is set, resolves the vault slug and reports
    /// per-vault applied state (block height, divergence status, last write
    /// timestamp, vault name); when absent, reports node-level health using
    /// the three-probe pattern (startup / liveness / readiness) plus
    /// dependency-check results, leader/term Raft state, and the top-5
    /// metrics by cardinality.
    async fn check(
        &self,
        request: w::HealthCheckRequest,
        _ctx: RequestContext,
    ) -> Result<w::HealthCheckResponse, WireError> {
        // Vault-scoped health check.
        if let Some(vault_slug_wire) = request.vault {
            let slug_resolver = SlugResolver::new(self.applied_state.clone());
            let vault_id = slug_resolver
                .resolve_vault(inferadb_ledger_types::VaultSlug::new(vault_slug_wire.value()))?;

            // OrganizationId defaults to 0 when absent or zero — matches
            // the tonic impl exactly. SlugResolver::resolve only fires for
            // a non-zero slug.
            let organization_id = match request.organization.as_ref() {
                Some(slug) if slug.value() != 0 => SlugResolver::new(self.applied_state.clone())
                    .resolve(inferadb_ledger_types::OrganizationSlug::new(slug.value()))?,
                _ => inferadb_ledger_types::OrganizationId::new(0),
            };

            let mut details: std::collections::BTreeMap<String, String> =
                std::collections::BTreeMap::new();

            // Block height from the global applied-state accessor.
            let height = self.applied_state.vault_height(organization_id, vault_id);
            details.insert("block_height".to_string(), height.to_string());

            // Vault health status: prefer the per-organization group's
            // applied state (B.1.8 routing); fall back to the GLOBAL
            // accessor when the manager is missing or the organization
            // hasn't been routed yet.
            let health_status = if let Some(ref manager) = self.manager {
                if let Some(group) = manager.route_organization(organization_id) {
                    group.applied_state().vault_health(organization_id, vault_id)
                } else {
                    self.applied_state.vault_health(organization_id, vault_id)
                }
            } else {
                self.applied_state.vault_health(organization_id, vault_id)
            };
            let (status, message) = match &health_status {
                VaultHealthStatus::Healthy => {
                    details.insert("health_status".to_string(), "healthy".to_string());
                    (ws::HealthStatus::Healthy, "Vault is healthy")
                },
                VaultHealthStatus::Diverged { expected, computed, at_height } => {
                    details.insert("health_status".to_string(), "diverged".to_string());
                    details.insert("diverged_at_height".to_string(), at_height.to_string());
                    details.insert("expected_root".to_string(), format!("{:x?}", &expected[..8]));
                    details.insert("computed_root".to_string(), format!("{:x?}", &computed[..8]));
                    (ws::HealthStatus::Unavailable, "Vault has diverged state")
                },
                VaultHealthStatus::Recovering { started_at, attempt } => {
                    details.insert("health_status".to_string(), "recovering".to_string());
                    details.insert("recovery_started_at".to_string(), started_at.to_string());
                    details.insert("recovery_attempt".to_string(), attempt.to_string());
                    (ws::HealthStatus::Degraded, "Vault is recovering from divergence")
                },
            };

            // Vault metadata: last write timestamp + name (when present).
            if let Some(vault_meta) = self.applied_state.get_vault(organization_id, vault_id) {
                if vault_meta.last_write_timestamp > 0 {
                    details.insert(
                        "last_write_timestamp".to_string(),
                        vault_meta.last_write_timestamp.to_string(),
                    );
                }
                if let Some(name) = &vault_meta.name {
                    details.insert("vault_name".to_string(), name.clone());
                }
            }

            // Best-effort entity presence check (the tonic impl fetches
            // 1 entity to detect "has any entities" without a full scan).
            if let Ok(entities) = self.state.list_entities(vault_id, None, None, 1) {
                details.insert("has_entities".to_string(), (!entities.is_empty()).to_string());
            }

            return Ok(w::HealthCheckResponse { status, message: message.to_string(), details });
        }

        // Node-level health check (no vault slug).
        let shard_state = self.handle.shard_state();
        let mut details: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();

        // SLI metrics: quorum status + leader-election detection via term
        // delta. Identical to the tonic impl.
        let has_quorum = shard_state.leader.is_some();
        inferadb_ledger_raft::metrics::set_cluster_quorum_status(has_quorum);

        let prev_term = self.last_observed_term.swap(shard_state.term, Ordering::Relaxed);
        if prev_term > 0 && shard_state.term > prev_term {
            for _ in 0..(shard_state.term - prev_term) {
                inferadb_ledger_raft::metrics::record_leader_election();
            }
        }

        details.insert("current_term".to_string(), shard_state.term.to_string());
        if let Some(leader) = shard_state.leader {
            details.insert("leader_id".to_string(), leader.0.to_string());
        }
        details.insert("member_count".to_string(), shard_state.voters.len().to_string());

        details.insert("startup".to_string(), self.health_state.startup_check().to_string());
        details.insert("liveness".to_string(), self.health_state.liveness_check().to_string());
        details.insert("readiness".to_string(), self.health_state.readiness_check().to_string());
        details.insert("phase".to_string(), self.health_state.phase().as_str().to_string());

        // Dependency checks (disk, peer, raft lag) — TTL-cached upstream.
        let deps_healthy = if let Some(checker) = &self.dependency_checker {
            let dep_health = checker.check_all().await;
            for (name, result) in &dep_health.details {
                details.insert(
                    format!("dep:{name}"),
                    if result.healthy {
                        format!("ok: {}", result.detail)
                    } else {
                        format!("FAIL: {}", result.detail)
                    },
                );
            }
            dep_health.all_healthy
        } else {
            true
        };

        if let Some(tracker) = inferadb_ledger_raft::cardinality::tracker() {
            for (name, count) in tracker.top_by_cardinality(5) {
                details.insert(format!("cardinality:{name}"), count.to_string());
            }
        }

        let phase = self.health_state.phase();
        let (status, message) = match phase {
            inferadb_ledger_raft::graceful_shutdown::NodePhase::Starting => {
                if let Some(checker) = &self.dependency_checker {
                    let startup_health = checker.check_startup();
                    for (name, result) in &startup_health.details {
                        details.insert(
                            format!("startup:{name}"),
                            if result.healthy {
                                format!("ok: {}", result.detail)
                            } else {
                                format!("FAIL: {}", result.detail)
                            },
                        );
                    }
                }
                (ws::HealthStatus::Degraded, "Node is starting up")
            },
            inferadb_ledger_raft::graceful_shutdown::NodePhase::Draining => {
                (ws::HealthStatus::Unavailable, "Node is draining — not accepting new writes")
            },
            inferadb_ledger_raft::graceful_shutdown::NodePhase::ShuttingDown => {
                (ws::HealthStatus::Unavailable, "Node is shutting down")
            },
            inferadb_ledger_raft::graceful_shutdown::NodePhase::Ready => {
                if !deps_healthy {
                    (ws::HealthStatus::Degraded, "Dependencies unhealthy")
                } else if shard_state.leader.is_some() {
                    (ws::HealthStatus::Healthy, "Node is healthy and has a leader")
                } else {
                    (ws::HealthStatus::Degraded, "No leader elected")
                }
            },
        };

        Ok(w::HealthCheckResponse { status, message: message.to_string(), details })
    }
}
