//! Background invitation maintenance job.
//!
//! Periodically scans GLOBAL `_idx:invite:email_hash:` indexes and:
//! - **Phase 1**: Expires Pending invitations past their `expires_at` timestamp.
//! - **Phase 2**: Reaps terminal invitations older than the retention window (90 days).
//!
//! Only runs on the leader node. Proposals go through Raft for deterministic
//! state machine replay. Follows the [`TokenMaintenanceJob`] pattern.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::Utc;
use inferadb_ledger_state::{StateLayer, system::SystemOrganizationService};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    InvitationStatus, InviteEmailEntry, InviteId, OrganizationId, Region, decode,
};
use openraft::Raft;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::{
    metrics::{
        record_background_job_duration, record_background_job_items, record_background_job_run,
    },
    raft_manager::RaftManager,
    trace_context::TraceContext,
    types::{LedgerNodeId, LedgerRequest, LedgerTypeConfig, RaftPayload, SystemRequest},
};

/// Default interval between maintenance cycles (5 minutes).
const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(300);

/// Maximum expirations per cycle (Phase 1).
const MAX_EXPIRATIONS_PER_CYCLE: usize = 200;

/// Maximum deletions per cycle (Phase 2 — retention reaping).
const MAX_DELETIONS_PER_CYCLE: usize = 100;

/// Retention window for terminal invitations (90 days).
const RETENTION_DAYS: i64 = 90;

/// Maximum entries to scan from GLOBAL email hash index per cycle.
const MAX_SCAN_ENTRIES: usize = 5000;

/// Prefix for scanning all invitation email hash index entries.
const EMAIL_HASH_INDEX_PREFIX: &str = "_idx:invite:email_hash:";

/// Result of an invitation maintenance cycle.
#[derive(Debug, Default)]
pub struct InviteMaintenanceResult {
    /// Number of Pending invitations expired (Phase 1).
    pub invitations_expired: u64,
    /// Number of terminal invitations reaped (Phase 2).
    pub invitations_reaped: u64,
}

/// Background job for invitation lifecycle maintenance.
///
/// Runs two phases each cycle:
/// 1. Scans GLOBAL `_idx:invite:email_hash:` for Pending entries, reads REGIONAL records to check
///    `expires_at`, and proposes expiration for those past their deadline.
/// 2. Scans terminal entries, reads REGIONAL records to check `created_at`, and proposes full
///    cleanup for those past the 90-day retention window.
///
/// Only runs on the leader node.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct InviteMaintenanceJob<B: StorageBackend + 'static> {
    /// GLOBAL Raft consensus handle for proposing index changes.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// GLOBAL state layer for scanning `_idx:invite:email_hash:` prefix.
    state: Arc<StateLayer<B>>,
    /// Interval between maintenance cycles.
    #[builder(default = DEFAULT_MAINTENANCE_INTERVAL)]
    interval: Duration,
    /// Watchdog heartbeat handle. Updated each cycle to prove liveness.
    #[builder(default)]
    watchdog_handle: Option<Arc<AtomicU64>>,
    /// Multi-region Raft manager for reading REGIONAL records and proposing
    /// REGIONAL updates.
    #[builder(default)]
    manager: Option<Arc<RaftManager>>,
}

/// Parsed email hash index entry with metadata extracted from the key.
struct ScannedEntry {
    invite_id: InviteId,
    email_hmac: String,
    entry: InviteEmailEntry,
}

impl<B: StorageBackend + 'static> InviteMaintenanceJob<B> {
    /// Checks if this node is the current leader.
    fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Scans GLOBAL email hash index entries.
    ///
    /// Key format: `_idx:invite:email_hash:{hmac}:{invite_id}`
    fn scan_email_entries(&self) -> Vec<ScannedEntry> {
        use inferadb_ledger_state::system::SYSTEM_VAULT_ID;

        let entities = match self.state.list_entities(
            SYSTEM_VAULT_ID,
            Some(EMAIL_HASH_INDEX_PREFIX),
            None,
            MAX_SCAN_ENTRIES,
        ) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "Failed to scan email hash index");
                return Vec::new();
            },
        };

        let mut results = Vec::with_capacity(entities.len());
        for entity in &entities {
            let key_str = String::from_utf8_lossy(&entity.key);

            // Strip prefix, then split into {hmac}:{invite_id}
            let Some(without_prefix) = key_str.strip_prefix(EMAIL_HASH_INDEX_PREFIX) else {
                continue;
            };
            let Some((hmac, id_str)) = without_prefix.rsplit_once(':') else {
                continue;
            };
            let Ok(id_val) = id_str.parse::<i64>() else {
                continue;
            };
            let Ok(entry) = decode::<InviteEmailEntry>(&entity.value) else {
                continue;
            };

            results.push(ScannedEntry {
                invite_id: InviteId::new(id_val),
                email_hmac: hmac.to_string(),
                entry,
            });
        }
        results
    }

    /// Looks up the region for an organization from GLOBAL state.
    fn org_region(&self, org_id: OrganizationId) -> Option<Region> {
        let sys_svc = SystemOrganizationService::new(self.state.clone());
        match sys_svc.get_organization(org_id) {
            Ok(Some(registry)) => Some(registry.region),
            Ok(None) => {
                debug!(org_id = org_id.value(), "Organization not found for invite maintenance");
                None
            },
            Err(e) => {
                warn!(org_id = org_id.value(), error = %e, "Failed to read org registry");
                None
            },
        }
    }

    /// Phase 1: Expire Pending invitations past their `expires_at`.
    async fn expire_pending(&self, entries: &[ScannedEntry], trace_id: &str) -> (u64, bool) {
        let Some(ref manager) = self.manager else {
            debug!("Skipping invite expiration (no region manager)");
            return (0, false);
        };

        let mut expired_count = 0u64;
        let mut had_errors = false;
        let now = Utc::now();

        let pending_entries: Vec<&ScannedEntry> =
            entries.iter().filter(|e| e.entry.status == InvitationStatus::Pending).collect();

        for scanned in pending_entries.iter().take(MAX_EXPIRATIONS_PER_CYCLE) {
            let Some(region) = self.org_region(scanned.entry.organization) else {
                continue;
            };

            // Read REGIONAL record to check expires_at
            let group = match manager.get_region_group(region) {
                Ok(g) => g,
                Err(e) => {
                    warn!(
                        trace_id = %trace_id,
                        region = %region,
                        error = ?e,
                        "Region group not found for invite expiration"
                    );
                    had_errors = true;
                    continue;
                },
            };

            let sys_svc = SystemOrganizationService::new(group.state().clone());
            let invitation =
                match sys_svc.read_invitation(scanned.entry.organization, scanned.invite_id) {
                    Ok(Some(inv)) => inv,
                    Ok(None) => continue, // REGIONAL record gone, skip
                    Err(e) => {
                        warn!(
                            trace_id = %trace_id,
                            invite_id = scanned.invite_id.value(),
                            error = %e,
                            "Failed to read REGIONAL invitation"
                        );
                        had_errors = true;
                        continue;
                    },
                };

            if invitation.status != InvitationStatus::Pending || invitation.expires_at >= now {
                continue;
            }

            // Propose GLOBAL resolve: Pending → Expired
            let global_request = LedgerRequest::ResolveOrganizationInvite {
                invite: scanned.invite_id,
                organization: scanned.entry.organization,
                status: InvitationStatus::Expired,
                invitee_email_hmac: scanned.email_hmac.clone(),
                token_hash: invitation.token_hash,
            };

            match self.raft.client_write(RaftPayload::new(global_request)).await {
                Ok(_) => {},
                Err(e) => {
                    warn!(
                        trace_id = %trace_id,
                        invite_id = scanned.invite_id.value(),
                        error = %e,
                        "Failed to propose GLOBAL invite expiration"
                    );
                    had_errors = true;
                    continue;
                },
            }

            // Propose REGIONAL status update
            let regional_request =
                LedgerRequest::System(SystemRequest::UpdateOrganizationInviteStatus {
                    organization: scanned.entry.organization,
                    invite: scanned.invite_id,
                    status: InvitationStatus::Expired,
                });

            if let Err(e) = group.raft().client_write(RaftPayload::new(regional_request)).await {
                warn!(
                    trace_id = %trace_id,
                    invite_id = scanned.invite_id.value(),
                    error = %e,
                    "REGIONAL expire failed after GLOBAL; maintenance will reconcile"
                );
            }

            expired_count += 1;
            info!(
                trace_id = %trace_id,
                invite_id = scanned.invite_id.value(),
                org_id = scanned.entry.organization.value(),
                "Expired pending invitation"
            );
        }

        (expired_count, had_errors)
    }

    /// Phase 2: Reap terminal invitations past the retention window (90 days).
    async fn reap_terminal(&self, entries: &[ScannedEntry], trace_id: &str) -> (u64, bool) {
        let Some(ref manager) = self.manager else {
            debug!("Skipping invite reaping (no region manager)");
            return (0, false);
        };

        let mut reaped_count = 0u64;
        let mut had_errors = false;
        let now = Utc::now();
        let retention = chrono::Duration::days(RETENTION_DAYS);

        let terminal_entries: Vec<&ScannedEntry> =
            entries.iter().filter(|e| e.entry.status.is_terminal()).collect();

        for scanned in terminal_entries.iter().take(MAX_DELETIONS_PER_CYCLE) {
            let Some(region) = self.org_region(scanned.entry.organization) else {
                continue;
            };

            let group = match manager.get_region_group(region) {
                Ok(g) => g,
                Err(e) => {
                    warn!(
                        trace_id = %trace_id,
                        region = %region,
                        error = ?e,
                        "Region group not found for invite reaping"
                    );
                    had_errors = true;
                    continue;
                },
            };

            // Read REGIONAL record for created_at and slug
            let sys_svc = SystemOrganizationService::new(group.state().clone());
            let invitation = match sys_svc
                .read_invitation(scanned.entry.organization, scanned.invite_id)
            {
                Ok(Some(inv)) => inv,
                Ok(None) => {
                    // REGIONAL record already gone — still clean up GLOBAL indexes.
                    // Use a dummy slug; the delete operation is idempotent.
                    let global_request = LedgerRequest::PurgeOrganizationInviteIndexes {
                        invite: scanned.invite_id,
                        slug: inferadb_ledger_types::InviteSlug::new(0),
                        invitee_email_hmac: scanned.email_hmac.clone(),
                    };
                    if let Err(e) = self.raft.client_write(RaftPayload::new(global_request)).await {
                        warn!(
                            trace_id = %trace_id,
                            invite_id = scanned.invite_id.value(),
                            error = %e,
                            "Failed to purge orphaned GLOBAL invite indexes"
                        );
                        had_errors = true;
                    } else {
                        reaped_count += 1;
                    }
                    continue;
                },
                Err(e) => {
                    warn!(
                        trace_id = %trace_id,
                        invite_id = scanned.invite_id.value(),
                        error = %e,
                        "Failed to read REGIONAL invitation for reaping"
                    );
                    had_errors = true;
                    continue;
                },
            };

            // Check retention window
            if now < invitation.created_at + retention {
                continue;
            }

            // Propose GLOBAL index cleanup
            let global_request = LedgerRequest::PurgeOrganizationInviteIndexes {
                invite: scanned.invite_id,
                slug: invitation.slug,
                invitee_email_hmac: scanned.email_hmac.clone(),
            };

            match self.raft.client_write(RaftPayload::new(global_request)).await {
                Ok(_) => {},
                Err(e) => {
                    warn!(
                        trace_id = %trace_id,
                        invite_id = scanned.invite_id.value(),
                        error = %e,
                        "Failed to purge GLOBAL invite indexes"
                    );
                    had_errors = true;
                    continue;
                },
            }

            // Propose REGIONAL record deletion
            let regional_request = LedgerRequest::System(SystemRequest::DeleteOrganizationInvite {
                organization: scanned.entry.organization,
                invite: scanned.invite_id,
            });

            if let Err(e) = group.raft().client_write(RaftPayload::new(regional_request)).await {
                warn!(
                    trace_id = %trace_id,
                    invite_id = scanned.invite_id.value(),
                    error = %e,
                    "REGIONAL delete failed after GLOBAL purge; maintenance will reconcile"
                );
            }

            reaped_count += 1;
            info!(
                trace_id = %trace_id,
                invite_id = scanned.invite_id.value(),
                org_id = scanned.entry.organization.value(),
                "Reaped terminal invitation (retention expired)"
            );
        }

        (reaped_count, had_errors)
    }

    /// Runs a single maintenance cycle.
    pub async fn run_cycle(&self) -> InviteMaintenanceResult {
        if !self.is_leader() {
            debug!("Skipping invite maintenance cycle (not leader)");
            return InviteMaintenanceResult::default();
        }

        let trace_ctx = TraceContext::new();
        let cycle_start = Instant::now();
        debug!(trace_id = %trace_ctx.trace_id, "Starting invite maintenance cycle");

        let mut result = InviteMaintenanceResult::default();
        let mut had_errors = false;

        // Scan GLOBAL email hash index
        let entries = self.scan_email_entries();

        if entries.is_empty() {
            let duration = cycle_start.elapsed().as_secs_f64();
            record_background_job_duration("invite_maintenance", duration);
            record_background_job_run("invite_maintenance", "success");
            return result;
        }

        // Phase 1: Expire pending invitations
        let (expired, phase1_errors) = self.expire_pending(&entries, &trace_ctx.trace_id).await;
        result.invitations_expired = expired;
        had_errors |= phase1_errors;

        // Phase 2: Reap terminal invitations past retention
        let (reaped, phase2_errors) = self.reap_terminal(&entries, &trace_ctx.trace_id).await;
        result.invitations_reaped = reaped;
        had_errors |= phase2_errors;

        let duration = cycle_start.elapsed().as_secs_f64();
        record_background_job_duration("invite_maintenance", duration);
        let status = if had_errors { "failure" } else { "success" };
        record_background_job_run("invite_maintenance", status);
        record_background_job_items(
            "invite_maintenance",
            result.invitations_expired + result.invitations_reaped,
        );

        debug!(
            trace_id = %trace_ctx.trace_id,
            expired = result.invitations_expired,
            reaped = result.invitations_reaped,
            scanned = entries.len(),
            duration_secs = duration,
            "Invite maintenance cycle complete"
        );

        result
    }

    /// Starts the invite maintenance background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;
                if let Some(ref handle) = self.watchdog_handle {
                    handle.store(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        Ordering::Relaxed,
                    );
                }
                self.run_cycle().await;
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_result_default() {
        let result = InviteMaintenanceResult::default();
        assert_eq!(result.invitations_expired, 0);
        assert_eq!(result.invitations_reaped, 0);
    }

    #[test]
    fn maintenance_result_accumulates() {
        let mut result = InviteMaintenanceResult::default();
        result.invitations_expired += 5;
        result.invitations_reaped += 3;
        assert_eq!(result.invitations_expired, 5);
        assert_eq!(result.invitations_reaped, 3);
    }

    #[test]
    fn constants_are_reasonable() {
        assert_eq!(MAX_EXPIRATIONS_PER_CYCLE, 200);
        assert_eq!(MAX_DELETIONS_PER_CYCLE, 100);
        assert_eq!(RETENTION_DAYS, 90);
        assert_eq!(MAX_SCAN_ENTRIES, 5000);
        assert_eq!(DEFAULT_MAINTENANCE_INTERVAL, Duration::from_secs(300));
    }

    #[test]
    fn scanned_entry_fields() {
        let entry = ScannedEntry {
            invite_id: InviteId::new(42),
            email_hmac: "deadbeef".to_string(),
            entry: InviteEmailEntry {
                organization: OrganizationId::new(1),
                status: InvitationStatus::Pending,
            },
        };
        assert_eq!(entry.invite_id.value(), 42);
        assert_eq!(entry.email_hmac, "deadbeef");
        assert_eq!(entry.entry.status, InvitationStatus::Pending);
    }
}
