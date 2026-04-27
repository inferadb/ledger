//! Admin service implementation.
//!
//! Handles cluster membership, snapshots, integrity checks, vault recovery,
//! runtime configuration, backup/restore, and cryptographic key operations.
//! Organization lifecycle is handled by [`super::OrganizationService`].
//! Vault CRUD is handled by [`super::VaultService`].

use std::{str::FromStr, sync::Arc, time::Duration};

use inferadb_ledger_proto::proto::{
    BackupInfo, CheckIntegrityRequest, CheckIntegrityResponse, CheckPeerLivenessRequest,
    CheckPeerLivenessResponse, ClusterMember, CreateBackupRequest, CreateBackupResponse,
    CreateSnapshotRequest, CreateSnapshotResponse, DataRegionReplica,
    GetBlindingKeyRehashStatusRequest, GetBlindingKeyRehashStatusResponse, GetClusterInfoRequest,
    GetClusterInfoResponse, GetConfigRequest, GetConfigResponse, GetDecommissionStatusRequest,
    GetDecommissionStatusResponse, GetNodeInfoRequest, GetNodeInfoResponse, GetRewrapStatusRequest,
    GetRewrapStatusResponse, Hash, IntegrityIssue, JoinClusterRequest, JoinClusterResponse,
    LeaveClusterRequest, LeaveClusterResponse, ListBackupsRequest, ListBackupsResponse,
    MigrateExistingUsersRequest, MigrateExistingUsersResponse, ProvisionRegionRequest,
    ProvisionRegionResponse, RecoverVaultRequest, RecoverVaultResponse, Region as ProtoRegion,
    RestoreBackupRequest, RestoreBackupResponse, RotateBlindingKeyRequest,
    RotateBlindingKeyResponse, RotateRegionKeyRequest, RotateRegionKeyResponse,
    TransferLeadershipRequest, TransferLeadershipResponse, UpdateConfigRequest,
    UpdateConfigResponse, VaultHealthProto,
};
use inferadb_ledger_raft::{
    ConsensusHandle, HandleError, NodeStatus,
    log_storage::{AppliedStateAccessor, VaultHealthStatus},
    logging::{OperationType, RequestContext, Sampler},
    metrics,
    types::{LedgerResponse, OrganizationRequest, RaftPayload, SystemRequest},
};
use inferadb_ledger_state::{BlockArchive, StateLayer, system::SystemOrganizationService};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{
    ALL_REGIONS, OrganizationId as DomainOrganizationId, VaultEntry, VaultId as DomainVaultId,
    ZERO_HASH,
    config::ValidationConfig,
    events::{EventAction, EventOutcome as EventOutcomeType},
    hash_eq,
};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

use super::{error_classify, slug_resolver::SlugResolver};

/// gRPC handler for cluster administration operations.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct AdminService {
    /// Consensus handle for proposing admin operations and leadership checks.
    handle: Arc<ConsensusHandle>,
    /// State layer for entity and relationship reads during admin operations.
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
    /// Block archive for integrity verification.
    #[builder(default)]
    block_archive: Option<Arc<BlockArchive<FileBackend>>>,
    /// The address other nodes should use to reach this node.
    ///
    /// Set from `--advertise` (or `--listen` as fallback). Used in
    /// `GetNodeInfo` responses and data region initial_members.
    #[builder(into)]
    advertise_addr: String,
    /// Sampler for log tail sampling.
    #[builder(default)]
    sampler: Option<Sampler>,
    /// Node ID for logging system context.
    #[builder(default)]
    node_id: Option<u64>,
    /// Input validation configuration for request field limits.
    #[builder(default = Arc::new(ValidationConfig::default()))]
    validation_config: Arc<ValidationConfig>,
    /// Maximum time to wait for a Raft proposal to commit.
    ///
    /// If a gRPC deadline is shorter, the deadline takes precedence.
    #[builder(default = Duration::from_secs(30))]
    proposal_timeout: Duration,
    /// Runtime configuration handle for hot-reloadable settings.
    #[builder(default)]
    runtime_config: Option<inferadb_ledger_raft::runtime_config::RuntimeConfigHandle>,
    /// Rate limiter for propagating config changes.
    #[builder(default)]
    rate_limiter: Option<Arc<inferadb_ledger_raft::rate_limit::RateLimiter>>,
    /// Hot key detector for propagating config changes.
    #[builder(default)]
    hot_key_detector: Option<Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>>,
    /// Backup manager for backup and restore operations.
    ///
    /// Must be constructed via [`BackupManager::with_data_dir`] for the
    /// archive-based RPC path; the legacy snapshot path is gone.
    #[builder(default)]
    backup_manager: Option<Arc<inferadb_ledger_raft::backup::BackupManager>>,
    /// Region key manager — supplies the local node's RMK fingerprint for
    /// stamping outgoing archives and validating incoming archives at
    /// restore-stage time.
    #[builder(default)]
    key_manager: Option<Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>>,
    /// Backup root directory — every archive lives at
    /// `{backups_dir}/backup-{id}.tar.zst`. Mirrors the path the
    /// [`backup_manager`] writes through; held here so [`restore_backup`]
    /// can resolve a `backup_id` to an archive path without round-tripping
    /// through the manager.
    #[builder(default)]
    backups_dir: Option<std::path::PathBuf>,
    /// Handler-phase event handle for recording denial events.
    #[builder(default)]
    event_handle:
        Option<inferadb_ledger_raft::event_writer::EventHandle<inferadb_ledger_store::FileBackend>>,
    /// Health state for drain-phase write rejection.
    #[builder(default)]
    health_state: Option<inferadb_ledger_raft::graceful_shutdown::HealthState>,
    /// Lock to prevent concurrent leader transfer attempts.
    #[builder(default = Arc::new(std::sync::atomic::AtomicBool::new(false)))]
    transfer_lock: Arc<std::sync::atomic::AtomicBool>,
    /// Shared DEK re-wrapping progress (read by `GetRewrapStatus`).
    #[builder(default)]
    rewrap_progress: Option<Arc<inferadb_ledger_raft::dek_rewrap::RewrapProgress>>,
    /// Raft manager for lazy region provisioning.
    #[builder(default)]
    raft_manager: Option<Arc<inferadb_ledger_raft::raft_manager::RaftManager>>,
    /// Shared peer address map for resolving peer network addresses.
    ///
    /// Used by admin RPCs that need to contact specific peers.
    #[builder(default)]
    peer_addresses: Option<inferadb_ledger_raft::PeerAddressMap>,
    /// GLOBAL region consensus transport for registering new peer channels
    /// during dynamic cluster membership changes (JoinCluster/LeaveCluster).
    #[builder(default)]
    consensus_transport: Option<inferadb_ledger_raft::GrpcConsensusTransport>,
    /// Initialization signal sender for fresh (uninitialized) nodes.
    ///
    /// When present, `init_cluster()` generates a cluster ID, persists it to
    /// `init_data_dir`, and sends `Some(cluster_id)` through this channel to
    /// unblock bootstrap. When absent (restart path), `init_cluster()` returns
    /// `already_initialized = true`.
    #[builder(default)]
    init_sender: Option<Arc<tokio::sync::watch::Sender<Option<u64>>>>,
    /// Data directory path for cluster ID persistence (used by `init_cluster`).
    #[builder(default)]
    init_data_dir: Option<std::path::PathBuf>,
    /// Static cluster ID for restarted (already-initialized) nodes.
    ///
    /// Set during bootstrap for the restart path. For fresh nodes, this is `None`
    /// and `get_node_info` reads the cluster ID from the `init_sender` channel.
    #[builder(default)]
    cluster_id: Option<u64>,
    /// Per-node liveness timestamps. Updated on every successful RPC or Raft message.
    /// Shared with the Raft service and bootstrap liveness checker.
    #[builder(default)]
    peer_liveness:
        Option<Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>>,
}

impl AdminService {
    /// Attaches input validation configuration for request field limits.
    #[must_use]
    pub fn with_validation_config(mut self, config: Arc<ValidationConfig>) -> Self {
        self.validation_config = config;
        self
    }

    /// Sets the maximum time to wait for Raft proposals.
    #[must_use]
    pub fn with_proposal_timeout(mut self, timeout: Duration) -> Self {
        self.proposal_timeout = timeout;
        self
    }

    /// Attaches the runtime configuration handle for hot-reloadable settings.
    #[must_use]
    pub fn with_runtime_config(
        mut self,
        handle: inferadb_ledger_raft::runtime_config::RuntimeConfigHandle,
        rate_limiter: Option<Arc<inferadb_ledger_raft::rate_limit::RateLimiter>>,
        hot_key_detector: Option<Arc<inferadb_ledger_raft::hot_key_detector::HotKeyDetector>>,
    ) -> Self {
        self.runtime_config = Some(handle);
        self.rate_limiter = rate_limiter;
        self.hot_key_detector = hot_key_detector;
        self
    }

    /// Attaches the backup manager for the multi-DB archive
    /// backup/restore RPCs.
    ///
    /// The legacy snapshot-based path is gone; the backup manager must
    /// be constructed via
    /// [`BackupManager::with_data_dir`](inferadb_ledger_raft::backup::BackupManager::with_data_dir)
    /// so the archive path can enumerate per-org and per-vault DB
    /// files. Pair this with [`Self::with_key_manager`] +
    /// [`Self::with_backups_dir`] at server-bootstrap time.
    #[must_use]
    pub fn with_backup(
        mut self,
        backup_manager: Arc<inferadb_ledger_raft::backup::BackupManager>,
    ) -> Self {
        self.backup_manager = Some(backup_manager);
        self
    }

    /// Attaches the region key manager — required for the multi-DB
    /// archive backup path so the manifest can stamp the local node's
    /// RMK fingerprint on outgoing archives and pre-flight check
    /// incoming archives at restore-stage time.
    #[must_use]
    pub fn with_key_manager(
        mut self,
        key_manager: Arc<dyn inferadb_ledger_store::crypto::RegionKeyManager>,
    ) -> Self {
        self.key_manager = Some(key_manager);
        self
    }

    /// Attaches the backups root directory — used by [`restore_backup`] to
    /// resolve a `backup_id` to an archive path on disk.
    #[must_use]
    pub fn with_backups_dir(mut self, backups_dir: std::path::PathBuf) -> Self {
        self.backups_dir = Some(backups_dir);
        self
    }

    /// Attaches the handler-phase event handle for recording denial events.
    #[must_use]
    pub fn with_event_handle(
        mut self,
        handle: inferadb_ledger_raft::event_writer::EventHandle<inferadb_ledger_store::FileBackend>,
    ) -> Self {
        self.event_handle = Some(handle);
        self
    }

    /// Attaches health state for drain-phase write rejection.
    #[must_use]
    pub fn with_health_state(
        mut self,
        health_state: inferadb_ledger_raft::graceful_shutdown::HealthState,
    ) -> Self {
        self.health_state = Some(health_state);
        self
    }

    /// Attaches the Raft manager for lazy region provisioning.
    #[must_use]
    pub fn with_raft_manager(
        mut self,
        manager: Arc<inferadb_ledger_raft::raft_manager::RaftManager>,
    ) -> Self {
        self.raft_manager = Some(manager);
        self
    }

    /// Attaches the GLOBAL consensus transport for peer channel management.
    ///
    /// When set, `JoinCluster` and `LeaveCluster` will register/unregister
    /// gRPC channels for the affected nodes, enabling Raft replication to
    /// dynamically-added cluster members.
    #[must_use]
    pub fn with_consensus_transport(
        mut self,
        transport: inferadb_ledger_raft::GrpcConsensusTransport,
    ) -> Self {
        self.consensus_transport = Some(transport);
        self
    }

    /// Attaches the initialization signal sender and data directory.
    ///
    /// Used on fresh (uninitialized) nodes so the `InitCluster` RPC can generate
    /// a cluster ID, persist it, and signal bootstrap to proceed.
    #[must_use]
    pub fn with_init_sender(
        mut self,
        sender: Arc<tokio::sync::watch::Sender<Option<u64>>>,
        data_dir: Option<std::path::PathBuf>,
    ) -> Self {
        self.init_sender = Some(sender);
        self.init_data_dir = data_dir;
        self
    }

    /// Sets the static cluster ID for already-initialized nodes.
    ///
    /// On the restart path, the cluster ID is loaded from disk during bootstrap
    /// and passed here so `get_node_info` and `init_cluster` can return it
    /// without needing the init sender channel.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: u64) -> Self {
        self.cluster_id = Some(cluster_id);
        self
    }

    /// Attaches a shared peer liveness map for quorum-based dead node detection.
    #[must_use]
    pub fn with_peer_liveness(
        mut self,
        liveness: Arc<parking_lot::RwLock<std::collections::HashMap<u64, std::time::Instant>>>,
    ) -> Self {
        self.peer_liveness = Some(liveness);
        self
    }

    /// Creates a `RequestContext` for an admin operation using the unified
    /// `from_request` constructor, which extracts trace context and transport
    /// metadata from the tonic request in one step.
    ///
    /// Must be called **before** `request.into_inner()`.
    fn make_request_context_unified<T>(
        &self,
        method: &'static str,
        request: &tonic::Request<T>,
    ) -> RequestContext {
        let event_handle: Option<Arc<dyn inferadb_ledger_raft::event_writer::EventEmitter>> =
            self.event_handle.as_ref().map(|h| Arc::new(h.clone()) as _);
        let mut ctx = RequestContext::from_request("AdminService", method, request, event_handle);
        ctx.set_operation_type(OperationType::Admin);
        ctx.set_admin_action(method);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }
        ctx
    }

    /// Proposes a `SystemRequest` through Raft with deadline handling.
    ///
    /// Handles timeout computation, Raft proposal submission, and error
    /// classification (leadership errors → UNAVAILABLE, others → INTERNAL).
    async fn propose_raft_request(
        &self,
        request: SystemRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        let grpc_deadline =
            inferadb_ledger_raft::deadline::extract_deadline_from_metadata(grpc_metadata);
        let timeout =
            inferadb_ledger_raft::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);
        let payload = RaftPayload::system(request);

        match self.handle.propose_and_wait(payload, timeout).await {
            Ok(response) => Ok(response),
            Err(HandleError::Consensus { source, .. }) => {
                ctx.set_error("RaftError", &source.to_string());
                Err(crate::proposal::consensus_error_to_status(source))
            },
            Err(HandleError::Timeout { .. }) => {
                inferadb_ledger_raft::metrics::record_raft_proposal_timeout();
                ctx.set_error("Timeout", "Raft proposal timed out");
                Err(Status::deadline_exceeded(format!(
                    "Raft proposal timed out after {}ms",
                    timeout.as_millis()
                )))
            },
            Err(e) => {
                ctx.set_error("RaftError", &e.to_string());
                Err(Status::internal(e.to_string()))
            },
        }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::admin_service_server::AdminService for AdminService {
    /// Triggers a Raft snapshot and returns the current block height.
    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let mut ctx = self.make_request_context_unified("create_snapshot", &request);
        let _req = request.into_inner();

        // Trigger a Raft snapshot via the consensus engine.
        ctx.start_raft_timer();
        match self.handle.trigger_snapshot().await {
            Ok((idx, _term)) if idx > 0 => {
                tracing::info!(
                    last_included_index = idx,
                    "Snapshot triggered via CreateSnapshot RPC"
                );
            },
            Ok(_) => {
                tracing::info!("No new snapshot needed (commit index unchanged)");
            },
            Err(e) => {
                tracing::warn!(error = %e, "Snapshot trigger failed");
            },
        }
        ctx.end_raft_timer();

        // Get actual region height from applied state
        let block_height = self.applied_state.region_height();
        ctx.set_block_height(block_height);
        ctx.set_success();

        // Emit SnapshotCreated handler-phase event
        ctx.record_event(
            EventAction::SnapshotCreated,
            EventOutcomeType::Success,
            &[("height", &block_height.to_string())],
        );

        Ok(Response::new(CreateSnapshotResponse {
            block_height,
            state_root: None,
            snapshot_path: format!("/snapshots/snapshot_{}", chrono::Utc::now().timestamp()),
        }))
    }

    /// Verifies vault state integrity by recomputing block hashes and state roots against the
    /// stored blockchain.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn check_integrity(
        &self,
        request: Request<CheckIntegrityRequest>,
    ) -> Result<Response<CheckIntegrityResponse>, Status> {
        let mut ctx = self.make_request_context_unified("check_integrity", &request);
        let req = request.into_inner();
        let mut issues = Vec::new();

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_id = slug_resolver.extract_and_resolve_optional(&req.organization)?;
        let vault_id = slug_resolver.extract_and_resolve_vault_optional(&req.vault)?;

        if let Some(ref n) = req.organization {
            ctx.set_organization(n.slug);
        }
        if let Some(ref v) = req.vault {
            ctx.set_vault(v.slug);
        }

        // Get all vault heights to check.
        //
        // Post-γ, vault heights live in each per-organization group's
        // `AppliedState`, not GLOBAL. Cross-cutting "all vaults" scans
        // aggregate across per-org groups via the raft manager; single-
        // vault scans look up the owning per-org group directly.
        let vault_heights: Vec<(DomainOrganizationId, DomainVaultId, u64)> =
            if let (Some(org), Some(v)) = (organization_id, vault_id) {
                let height = self
                    .raft_manager
                    .as_ref()
                    .and_then(|m| m.route_organization(org))
                    .map(|g| g.applied_state().vault_height(org, v))
                    .unwrap_or_else(|| self.applied_state.vault_height(org, v));
                if height > 0 { vec![(org, v, height)] } else { vec![] }
            } else if let Some(ref manager) = self.raft_manager {
                let mut heights = Vec::new();
                manager.for_each_vault_across_groups(|org, vault, h| heights.push((org, vault, h)));
                heights
            } else {
                let mut heights = Vec::new();
                self.applied_state
                    .for_each_vault_height(|org, vault, h| heights.push((org, vault, h)));
                heights
            };

        if vault_heights.is_empty() {
            ctx.set_success();
            return Ok(Response::new(CheckIntegrityResponse { healthy: true, issues: vec![] }));
        }

        if req.full_check {
            // Full check: Replay blocks and verify state roots
            let archive = match &self.block_archive {
                Some(a) => a,
                None => {
                    ctx.set_error("Unavailable", "Block archive not configured");
                    return Err(Status::unavailable(
                        "Block archive not configured for full integrity check",
                    ));
                },
            };

            for (org_id, v_id, expected_height) in &vault_heights {
                // Create temporary state for replay verification
                let (_temp_dir, temp_state) = match super::helpers::create_replay_context() {
                    Ok(ctx_pair) => ctx_pair,
                    Err(e) => {
                        issues.push(IntegrityIssue {
                            block_height: 0,
                            issue_type: "internal_error".to_string(),
                            description: e.message().to_string(),
                        });
                        continue;
                    },
                };

                let mut last_vault_hash: Option<[u8; 32]> = None;

                // Replay all blocks for this vault
                for height in 1..=*expected_height {
                    let region_height = match archive.find_region_height(*org_id, *v_id, height) {
                        Ok(Some(h)) => h,
                        Ok(None) => {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "missing_block".to_string(),
                                description: format!(
                                    "Block not found in archive: org={}, vault={}, height={}",
                                    org_id, v_id, height
                                ),
                            });
                            continue;
                        },
                        Err(e) => {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "archive_error".to_string(),
                                description: format!("Index lookup failed: {:?}", e),
                            });
                            continue;
                        },
                    };

                    let region_block = match archive.read_block(region_height) {
                        Ok(b) => b,
                        Err(e) => {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "read_error".to_string(),
                                description: format!("Block read failed: {:?}", e),
                            });
                            continue;
                        },
                    };

                    // Find the vault entry in this region block
                    let vault_entry = region_block.vault_entries.iter().find(|e| {
                        e.organization == *org_id && e.vault == *v_id && e.vault_height == height
                    });

                    let entry = match vault_entry {
                        Some(e) => e,
                        None => {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "missing_entry".to_string(),
                                description: format!(
                                    "Vault entry not found in region block: org={}, vault={}",
                                    org_id, v_id
                                ),
                            });
                            continue;
                        },
                    };

                    // Verify chain continuity (previous_vault_hash)
                    if let Some(expected_prev) = last_vault_hash
                        && !hash_eq(&entry.previous_vault_hash, &expected_prev)
                    {
                        issues.push(IntegrityIssue {
                            block_height: height,
                            issue_type: "chain_break".to_string(),
                            description: format!(
                                "Previous hash mismatch at height {}: expected {:x?}, got {:x?}",
                                height,
                                &expected_prev[..8],
                                &entry.previous_vault_hash[..8]
                            ),
                        });
                    }

                    // Apply transactions to temp state
                    for tx in &entry.transactions {
                        if let Err(e) = temp_state.apply_operations(*v_id, &tx.operations, height) {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "apply_error".to_string(),
                                description: format!("Transaction apply failed: {:?}", e),
                            });
                        }
                    }

                    // Compute and verify state root at this height
                    match temp_state.compute_state_root(*v_id) {
                        Ok(computed_root) => {
                            if computed_root != entry.state_root {
                                issues.push(IntegrityIssue {
                                    block_height: height,
                                    issue_type: "state_divergence".to_string(),
                                    description: format!(
                                        "State root mismatch: computed {:x?}, stored {:x?}",
                                        &computed_root[..8],
                                        &entry.state_root[..8]
                                    ),
                                });
                            }
                        },
                        Err(e) => {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "compute_error".to_string(),
                                description: format!("State root computation failed: {:?}", e),
                            });
                        },
                    }

                    // Track hash for next iteration's chain verification
                    // Compute vault block hash from entry
                    last_vault_hash = Some(compute_vault_block_hash(entry));
                }

                // Compare final replayed state root against current state
                let current_state = &*self.state;
                if let Ok(current_root) = current_state.compute_state_root(*v_id)
                    && let Ok(replayed_root) = temp_state.compute_state_root(*v_id)
                    && current_root != replayed_root
                {
                    issues.push(IntegrityIssue {
                        block_height: *expected_height,
                        issue_type: "final_state_mismatch".to_string(),
                        description: format!(
                            "Final state root mismatch for vault {}: current {:x?}, replayed {:x?}",
                            v_id,
                            &current_root[..8],
                            &replayed_root[..8]
                        ),
                    });
                }
            }
        } else {
            // Quick check: Verify state roots can be computed without errors
            let state = &*self.state;

            for (_org_id, v_id, height) in &vault_heights {
                match state.compute_state_root(*v_id) {
                    Ok(_root) => {
                        // State root computed successfully - state is internally consistent
                    },
                    Err(e) => {
                        issues.push(IntegrityIssue {
                            block_height: *height,
                            issue_type: "state_error".to_string(),
                            description: format!(
                                "Failed to compute state root for vault {}: {:?}",
                                v_id, e
                            ),
                        });
                    },
                }
            }
        }

        // Set outcome based on issues found
        if issues.is_empty() {
            ctx.set_success();
        } else {
            ctx.set_error("IntegrityIssues", &format!("{} issues found", issues.len()));
        }

        // Emit IntegrityChecked handler-phase event (org-scoped when org is specified)
        if organization_id.is_some() {
            let outcome = if issues.is_empty() {
                EventOutcomeType::Success
            } else {
                EventOutcomeType::Failed {
                    code: "integrity_issues".to_string(),
                    detail: format!("{} issues found", issues.len()),
                }
            };
            ctx.record_event(
                EventAction::IntegrityChecked,
                outcome,
                &[
                    ("issues_found", &issues.len().to_string()),
                    ("full_check", &req.full_check.to_string()),
                ],
            );
        }

        Ok(Response::new(CheckIntegrityResponse { healthy: issues.is_empty(), issues }))
    }

    // =========================================================================
    // Cluster Membership
    // =========================================================================

    /// Adds a node to the Raft cluster as a learner, then promotes it to voter.
    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterResponse>, Status> {
        // Reject if node is draining
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut ctx = self.make_request_context_unified("join_cluster", &request);
        let req = request.into_inner();

        // Check if we're the leader via consensus handle
        let node_id = self.handle.node_id();
        let current_leader = self.handle.current_leader();
        let current_term = self.handle.current_term();
        ctx.set_raft_term(current_term);
        ctx.set_is_leader(self.handle.is_leader());

        // If we're not the leader, return the leader info for redirect
        if !self.handle.is_leader() {
            ctx.set_error("NotLeader", "Not the leader, redirect to leader");
            let leader_id = current_leader.unwrap_or(0);
            return Ok(Response::new(JoinClusterResponse {
                success: false,
                message: "Not the leader, redirect to leader".to_string(),
                leader_id,
                leader_address: self
                    .peer_addresses
                    .as_ref()
                    .and_then(|m| m.get(leader_id))
                    .unwrap_or_default(),
            }));
        }

        // Check existing membership via the RaftManager's consensus state
        // (ConsensusHandle doesn't expose full membership with node addresses).
        let already_voter = self
            .raft_manager
            .as_ref()
            .and_then(|m| m.system_region().ok())
            .map(|s| {
                let state = s.handle().shard_state();
                state.voters.iter().any(|n| n.0 == req.node_id)
            })
            .unwrap_or(false);

        // Pre-compute this node's address once (we're the leader for all remaining responses).
        let my_address =
            self.peer_addresses.as_ref().and_then(|m| m.get(node_id)).unwrap_or_default();

        if already_voter {
            ctx.set_success();
            return Ok(Response::new(JoinClusterResponse {
                success: true,
                message: "Node is already a voter in the cluster".to_string(),
                leader_id: node_id,
                leader_address: my_address.clone(),
            }));
        }

        // Register the joining node's gRPC channel so the consensus transport
        // can send AppendEntries for replication. Without this, the leader
        // cannot replicate entries to the new node, and multi-voter quorum
        // commits would stall. The channel is resolved through the node-level
        // `NodeConnectionRegistry` so it's shared with all other subsystems.
        if let Some(ref transport) = self.consensus_transport
            && let Err(e) = transport.set_peer_via_registry(req.node_id, &req.address).await
        {
            tracing::warn!(
                node_id = req.node_id,
                address = %req.address,
                error = %e,
                "Failed to register joining node via registry",
            );
        }
        // Also update the peer address map for forwarding.
        if let Some(ref peers) = self.peer_addresses {
            peers.insert(req.node_id, req.address.clone());
        }

        // Replicate the peer address to all nodes via GLOBAL Raft so that
        // the PlacementController on the DR leader can reach the new peer.
        // This MUST commit before the GLOBAL AddVoter — otherwise the
        // controller wakes on the AddVoter event but the peer address is
        // unknown, causing AddLearner to be skipped until the next event.
        {
            if let Err(e) = self
                .handle
                .propose_and_wait(
                    inferadb_ledger_raft::types::RaftPayload::system(
                        inferadb_ledger_raft::types::SystemRequest::RegisterPeerAddress {
                            node_id: req.node_id,
                            address: req.address.clone(),
                        },
                    ),
                    std::time::Duration::from_secs(5),
                )
                .await
            {
                tracing::warn!(
                    node_id = req.node_id,
                    error = %e,
                    "RegisterPeerAddress failed — proceeding with join (DR scheduler will reconcile)"
                );
            }
        }

        // Step 1: Add as learner via ConsensusHandle
        ctx.start_raft_timer();
        let mut add_success = false;
        for attempt in 0..10 {
            match self.handle.add_learner(req.node_id, false).await {
                Ok(()) => {
                    add_success = true;
                    break;
                },
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("already undergoing a configuration change") {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            100 * (attempt + 1) as u64,
                        ))
                        .await;
                    } else {
                        ctx.end_raft_timer();
                        ctx.set_error("AddLearnerFailed", &err_str);
                        return Ok(Response::new(JoinClusterResponse {
                            success: false,
                            message: format!("Failed to add learner: {}", err_str),
                            leader_id: node_id,
                            leader_address: my_address.clone(),
                        }));
                    }
                },
            }
        }
        if !add_success {
            ctx.end_raft_timer();
            ctx.set_error("Timeout", "Cluster membership changes not completing");
            return Ok(Response::new(JoinClusterResponse {
                success: false,
                message: "Timeout: cluster membership changes not completing".to_string(),
                leader_id: node_id,
                leader_address: my_address.clone(),
            }));
        }

        // Step 2: Wait for the learner to appear in membership, then promote.
        // Use a short delay to let the membership change commit.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Step 3: Promote to voter via ConsensusHandle
        for attempt in 0..10 {
            match self.handle.promote_voter(req.node_id).await {
                Ok(()) => {
                    ctx.end_raft_timer();

                    // The joining node just communicated with us — mark it live.
                    if let Some(ref liveness) = self.peer_liveness {
                        liveness.write().insert(req.node_id, std::time::Instant::now());
                    }

                    ctx.set_success();
                    return Ok(Response::new(JoinClusterResponse {
                        success: true,
                        message: "Node joined cluster successfully".to_string(),
                        leader_id: node_id,
                        leader_address: my_address.clone(),
                    }));
                },
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("already undergoing a configuration change") {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            100 * (attempt + 1) as u64,
                        ))
                        .await;
                    } else {
                        ctx.end_raft_timer();
                        ctx.set_error("PromotionFailed", &err_str);
                        return Ok(Response::new(JoinClusterResponse {
                            success: false,
                            message: format!("Failed to promote to voter: {}", err_str),
                            leader_id: node_id,
                            leader_address: my_address.clone(),
                        }));
                    }
                },
            }
        }

        ctx.end_raft_timer();
        ctx.set_error("Timeout", "Could not complete voter promotion");
        Ok(Response::new(JoinClusterResponse {
            success: false,
            message: "Timeout: could not complete voter promotion".to_string(),
            leader_id: node_id,
            leader_address: my_address,
        }))
    }

    /// Removes a node from the Raft cluster membership.
    async fn leave_cluster(
        &self,
        request: Request<LeaveClusterRequest>,
    ) -> Result<Response<LeaveClusterResponse>, Status> {
        // Reject if node is draining
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut ctx = self.make_request_context_unified("leave_cluster", &request);
        let req = request.into_inner();

        // Check leadership via consensus handle
        let current_term = self.handle.current_term();
        ctx.set_raft_term(current_term);
        ctx.set_is_leader(self.handle.is_leader());

        // Only the leader can change membership
        if !self.handle.is_leader() {
            ctx.set_error("NotLeader", "Cannot process leave request");
            return Ok(Response::new(LeaveClusterResponse {
                success: false,
                message: "Not the leader, cannot process leave request".to_string(),
            }));
        }

        // Check current voters from shard state.
        // If there's only one voter left, refuse the removal regardless of who
        // it is — removing the last voter would make the cluster unrecoverable.
        let state = self.handle.shard_state();
        if state.voters.len() <= 1 {
            ctx.set_error("InvalidOperation", "Cannot remove the last voter");
            return Ok(Response::new(LeaveClusterResponse {
                success: false,
                message: "Cannot remove the last voter from cluster".to_string(),
            }));
        }

        ctx.start_raft_timer();

        // Mark the node as Decommissioning in GLOBAL state.
        // The DR scheduler (B3) reads this status to derive desired DR
        // membership and generates RemoveVoter operators. The drain
        // monitor (B4) removes the node from GLOBAL once fully drained.
        match self
            .handle
            .propose_and_wait(
                RaftPayload::system(SystemRequest::SetNodeStatus {
                    node_id: req.node_id,
                    status: NodeStatus::Decommissioning,
                }),
                std::time::Duration::from_secs(5),
            )
            .await
        {
            Ok(_) => {
                tracing::info!(node_id = req.node_id, "Node marked as Decommissioning");
            },
            Err(e) => {
                ctx.set_error("RaftProposalFailed", &e.to_string());
                return Ok(Response::new(LeaveClusterResponse {
                    success: false,
                    message: format!("Failed to mark node as Decommissioning: {e}"),
                }));
            },
        }

        // Synchronously remove the departing node from ALL data regions BEFORE
        // removing from GLOBAL. After GLOBAL removal, the node stops receiving
        // Raft entries and can't process DR changes. Without this synchronous
        // step, nodes killed immediately after leave_cluster returns leave stale
        // voters in data region membership, preventing quorum.
        if let Some(ref manager) = self.raft_manager {
            let target = inferadb_ledger_consensus::types::NodeId(req.node_id);
            let local_node_id = manager.config().node_id;

            for region in manager.list_regions() {
                if region == inferadb_ledger_types::Region::GLOBAL {
                    continue;
                }
                let Ok(group) = manager.get_region_group(region) else { continue };
                let state = group.handle().shard_state();
                if !state.voters.contains(&target) && !state.learners.contains(&target) {
                    continue;
                }

                if !group.handle().is_leader() {
                    // This node is not the DR leader — it cannot propose membership
                    // changes directly. The Decommissioning status is already
                    // committed to GLOBAL Raft (replicated to all nodes), so the
                    // DR checker on the actual leader will detect the node is no
                    // longer in the desired voter set and propose RemoveVoter on
                    // its next cycle (1s timer). The poll loop below waits for
                    // this to complete before proceeding to GLOBAL removal.
                    tracing::info!(
                        region = region.as_str(),
                        node_id = req.node_id,
                        dr_leader = ?state.leader,
                        "Not DR leader — DR checker on leader will handle removal"
                    );
                    continue;
                }

                // Self-removal: transfer DR leadership to a surviving voter first,
                // then let the new leader remove us via the DR checker.
                if req.node_id == local_node_id {
                    let global_state_reader = manager.system_state_reader();
                    let other_voter = state.voters.iter().find(|n| {
                        n.0 != local_node_id
                            && global_state_reader.as_ref().is_none_or(|r| {
                                r.node_status(n.0)
                                    == inferadb_ledger_raft::types::NodeStatus::Active
                            })
                    });
                    if let Some(transfer_target) = other_voter {
                        tracing::info!(
                            region = region.as_str(),
                            transfer_to = transfer_target.0,
                            "Transferring DR leadership before self-removal"
                        );
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_secs(3),
                            group.handle().transfer_leader(transfer_target.0),
                        )
                        .await;
                        // Wait for transfer to complete.
                        for _ in 0..30 {
                            if !group.handle().is_leader() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                    // The new DR leader will remove us — handled by the poll
                    // loop below after we wake the DR checker.
                    continue;
                }

                // Other-removal: remove directly with retry.
                let mut data_region_removed = false;
                for attempt in 0..10u32 {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(3),
                        group.handle().remove_node(req.node_id),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            tracing::info!(
                                region = region.as_str(),
                                node_id = req.node_id,
                                "Removed departing node from data region"
                            );
                            data_region_removed = true;
                            break;
                        },
                        Ok(Err(e)) if e.to_string().contains("already undergoing") => {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                200 * u64::from(attempt + 1),
                            ))
                            .await;
                        },
                        Ok(Err(e)) if e.to_string().contains("no-op") => {
                            data_region_removed = true;
                            break;
                        },
                        _ => break,
                    }
                }

                // Cascade the removal to every per-organization and per-vault
                // group in this region — they share the parent's elected
                // leader (delegated leadership, root rule 14) but maintain
                // independent membership state. Without this cascade,
                // departing voters linger in child shards as quorum-blocking
                // ghosts the moment they're killed. Surfaced as the
                // lifecycle Phase 5 "Raft proposal timed out" regression
                // when the original three voters died and only the
                // late-joining node survived.
                if data_region_removed {
                    manager
                        .cascade_membership_to_children(
                            region,
                            req.node_id,
                            inferadb_ledger_raft::raft_manager::CascadeMembershipAction::Remove,
                        )
                        .await;
                }
            }

            // Wake both the DR checker and scheduler on this node immediately.
            // The checker handles removals for regions where this node IS the
            // DR leader but the direct removal above was skipped (self-removal).
            // For regions where the DR leader is on a different node, that
            // node's GLOBAL apply worker fires the same notification when it
            // processes the Decommissioning entry, waking its local checker.
            //
            // The GLOBAL removal below proceeds immediately. The caller (CLI,
            // script, SDK) should poll GetClusterInfo to confirm removal from
            // both GLOBAL and data regions before killing the node. Blocking
            // here caused gRPC RST_STREAM timeouts.
            manager.notify_dr_membership_change();
        }

        // Remove from GLOBAL.
        if let Err(e) = self.handle.remove_node(req.node_id).await {
            ctx.end_raft_timer();
            ctx.set_error("MembershipChangeFailed", &e.to_string());
            return Ok(Response::new(LeaveClusterResponse {
                success: false,
                message: format!("Failed to remove node from GLOBAL: {e}"),
            }));
        }

        // Wait for the GLOBAL membership change to commit and apply.
        let target_node = inferadb_ledger_consensus::types::NodeId(req.node_id);
        for _ in 0..50 {
            let state = self.handle.shard_state();
            if !state.voters.contains(&target_node) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        ctx.end_raft_timer();
        ctx.set_success();
        Ok(Response::new(LeaveClusterResponse {
            success: true,
            message: "Node decommissioned.".to_string(),
        }))
    }

    /// Returns decommission progress for a node.
    ///
    /// Callers poll this after `LeaveCluster` to determine when it is safe to
    /// shut down the departing node. The response reports:
    /// - `status`: the node's `NodeStatus` from GLOBAL state
    /// - `remaining`: DR regions where the node still holds a replica
    /// - `global_removed`: whether the node has been removed from GLOBAL membership
    async fn get_decommission_status(
        &self,
        request: Request<GetDecommissionStatusRequest>,
    ) -> Result<Response<GetDecommissionStatusResponse>, Status> {
        let req = request.into_inner();

        // Read node status from GLOBAL state.
        let status_str = if let Some(ref manager) = self.raft_manager {
            if let Some(reader) = manager.system_state_reader() {
                match reader.node_status(req.node_id) {
                    NodeStatus::Active => "active",
                    NodeStatus::Decommissioning => "decommissioning",
                    NodeStatus::Dead => "dead",
                    NodeStatus::Removed => "removed",
                }
            } else {
                "unknown"
            }
        } else {
            "unknown"
        };

        // Check remaining DR replicas for the target node.
        let mut remaining = Vec::new();
        if let Some(ref manager) = self.raft_manager {
            let target = inferadb_ledger_consensus::types::NodeId(req.node_id);
            for region in manager.list_regions() {
                if region == inferadb_ledger_types::Region::GLOBAL {
                    continue;
                }
                if let Ok(group) = manager.get_region_group(region) {
                    let state = group.handle().shard_state();
                    if state.voters.contains(&target) {
                        remaining.push(DataRegionReplica {
                            region: region.to_string(),
                            role: "voter".to_string(),
                        });
                    } else if state.learners.contains(&target) {
                        remaining.push(DataRegionReplica {
                            region: region.to_string(),
                            role: "learner".to_string(),
                        });
                    }
                }
            }
        }

        // Check if GLOBAL membership removal is complete.
        let global_removed = {
            let state = self.handle.shard_state();
            let target = inferadb_ledger_consensus::types::NodeId(req.node_id);
            !state.voters.contains(&target) && !state.learners.contains(&target)
        };

        Ok(Response::new(GetDecommissionStatusResponse {
            status: status_str.to_string(),
            remaining,
            global_removed,
        }))
    }

    async fn check_peer_liveness(
        &self,
        request: Request<CheckPeerLivenessRequest>,
    ) -> Result<Response<CheckPeerLivenessResponse>, Status> {
        let req = request.into_inner();
        let (reachable, last_seen_ago_ms) = if let Some(ref liveness) = self.peer_liveness {
            let map = liveness.read();
            match map.get(&req.target_node_id) {
                Some(last_seen) => {
                    let ago = last_seen.elapsed();
                    // Consider reachable if seen within 5 minutes (dead_node_timeout default).
                    (ago < std::time::Duration::from_secs(300), ago.as_millis() as u64)
                },
                None => (false, u64::MAX),
            }
        } else {
            (false, u64::MAX)
        };

        Ok(Response::new(CheckPeerLivenessResponse { reachable, last_seen_ago_ms }))
    }

    /// Returns current cluster membership, leader ID, and Raft term.
    async fn get_cluster_info(
        &self,
        request: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        let mut ctx = self.make_request_context_unified("get_cluster_info", &request);
        let _req = request.into_inner();

        let current_leader = self.handle.current_leader();
        let current_term = self.handle.current_term();

        ctx.set_raft_term(current_term);
        ctx.set_is_leader(self.handle.is_leader());

        let shard_state = self.handle.shard_state();
        let mut members: Vec<ClusterMember> = shard_state
            .voters
            .iter()
            .map(|node_id| {
                let address =
                    self.peer_addresses.as_ref().and_then(|m| m.get(node_id.0)).unwrap_or_default();
                ClusterMember {
                    node_id: node_id.0,
                    address,
                    role: inferadb_ledger_proto::proto::ClusterMemberRole::Voter.into(),
                    is_leader: shard_state.leader == Some(*node_id),
                }
            })
            .collect();
        let learner_members: Vec<ClusterMember> = shard_state
            .learners
            .iter()
            .map(|node_id| {
                let address =
                    self.peer_addresses.as_ref().and_then(|m| m.get(node_id.0)).unwrap_or_default();
                ClusterMember {
                    node_id: node_id.0,
                    address,
                    role: inferadb_ledger_proto::proto::ClusterMemberRole::Learner.into(),
                    is_leader: false,
                }
            })
            .collect();
        members.extend(learner_members);

        ctx.set_keys_count(members.len());
        ctx.set_success();

        Ok(Response::new(GetClusterInfoResponse {
            members,
            leader_id: current_leader.unwrap_or(0),
            term: current_term,
        }))
    }

    /// Returns node information for pre-bootstrap coordination.
    ///
    /// Returns the node's identity information including Snowflake ID, address,
    /// cluster membership status, and current Raft term. This RPC is available
    /// even before the cluster is formed, enabling nodes to discover each other's
    /// IDs and determine who should bootstrap.
    ///
    /// # Security Considerations
    ///
    /// This RPC is intentionally unauthenticated to enable pre-cluster coordination
    /// when nodes cannot yet share credentials. The following threat model applies:
    ///
    /// **Minimal Information Exposure**: Only returns data necessary for coordination:
    /// - `node_id`: Snowflake ID (timestamp + random, not secret)
    /// - `address`: Already known to caller (they connected to it)
    /// - `is_cluster_member`: Cluster state observable via connection behavior
    /// - `term`: Raft term (not sensitive)
    ///
    /// **Threat: Malicious Node ID**: An attacker could provide an artificially low Snowflake
    /// ID to force leadership. Mitigations:
    /// - Snowflake IDs use 42-bit timestamp + 22-bit random, making prediction difficult
    /// - IDs are persisted on first startup, preventing replay
    /// - Production clusters should use authenticated discovery (DNS, etc.)
    /// - Network-level controls (firewalls, VPNs) limit who can participate
    ///
    /// **Threat: Information Gathering**: Attacker discovers cluster topology.
    /// Mitigations:
    /// - This RPC returns only this node's info, not full cluster membership
    /// - Use network-level access controls in production
    ///
    /// **Not Rate Limited**: The RPC is lightweight and coordination is time-bounded
    /// by `bootstrap_timeout_secs`. The discovery polling interval provides natural
    /// throttling for legitimate use.
    async fn get_node_info(
        &self,
        request: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        let mut ctx = self.make_request_context_unified("get_node_info", &request);
        let _req = request.into_inner();

        let current_term = self.handle.current_term();
        let node_id = self.handle.node_id();

        ctx.set_raft_term(current_term);
        ctx.set_is_leader(self.handle.is_leader());

        // Resolve cluster_id: use the static value (restart path) or check
        // the init sender's current value (fresh path, set after InitCluster).
        let cluster_id = self
            .cluster_id
            .or_else(|| self.init_sender.as_ref().and_then(|s| *s.borrow()))
            .unwrap_or(0);

        // Node is a cluster member only if it has been initialized (cluster_id assigned).
        // Fresh nodes start with themselves as a voter, so checking voters is unreliable.
        let is_cluster_member = cluster_id != 0;

        ctx.set_success();

        Ok(Response::new(GetNodeInfoResponse {
            node_id,
            address: self.advertise_addr.clone(),
            is_cluster_member,
            term: current_term,
            cluster_id,
            state: if cluster_id != 0 {
                "running".to_string()
            } else {
                "uninitialized".to_string()
            },
        }))
    }

    /// Initialize a new cluster.
    ///
    /// On fresh nodes, generates a cluster ID, persists it to disk, and signals
    /// bootstrap to start background jobs. On already-initialized nodes, returns
    /// `already_initialized = true` with the existing cluster ID.
    async fn init_cluster(
        &self,
        _request: tonic::Request<inferadb_ledger_proto::proto::InitClusterRequest>,
    ) -> Result<tonic::Response<inferadb_ledger_proto::proto::InitClusterResponse>, tonic::Status>
    {
        // Restart path: node was already initialized (cluster_id loaded from disk).
        if let Some(cid) = self.cluster_id {
            return Ok(tonic::Response::new(inferadb_ledger_proto::proto::InitClusterResponse {
                initialized: false,
                cluster_id: cid,
                already_initialized: true,
            }));
        }

        // Fresh path: check if already initialized via a prior InitCluster or seed join.
        if let Some(ref sender) = self.init_sender {
            if let Some(cid) = *sender.borrow() {
                return Ok(tonic::Response::new(
                    inferadb_ledger_proto::proto::InitClusterResponse {
                        initialized: false,
                        cluster_id: cid,
                        already_initialized: true,
                    },
                ));
            }
        } else {
            // No init_sender and no cluster_id — should not happen, but guard against it.
            return Ok(tonic::Response::new(inferadb_ledger_proto::proto::InitClusterResponse {
                initialized: false,
                cluster_id: 0,
                already_initialized: true,
            }));
        }

        // Generate a new cluster ID.
        let cluster_id = inferadb_ledger_types::snowflake::generate()
            .map_err(|e| tonic::Status::internal(format!("failed to generate cluster_id: {e}")))?;

        // Persist to disk so restarts skip the init flow.
        if let Some(ref dir) = self.init_data_dir {
            write_cluster_id_to_disk(dir, cluster_id)?;
        }

        // Signal bootstrap to proceed.
        if let Some(ref sender) = self.init_sender {
            let _ = sender.send(Some(cluster_id));
        }

        tracing::info!(cluster_id, "Cluster initialized via InitCluster RPC");

        Ok(tonic::Response::new(inferadb_ledger_proto::proto::InitClusterResponse {
            initialized: true,
            cluster_id,
            already_initialized: false,
        }))
    }

    // =========================================================================
    // Vault Recovery
    // =========================================================================

    /// Recovers a diverged vault by replaying its blockchain to rebuild state.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn recover_vault(
        &self,
        request: Request<RecoverVaultRequest>,
    ) -> Result<Response<RecoverVaultResponse>, Status> {
        // Reject if node is draining
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut ctx = self.make_request_context_unified("recover_vault", &request);
        let req = request.into_inner();
        ctx.set_recovery_force(req.force);

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_slug_val = req.organization.as_ref().map_or(0, |n| n.slug);
        let organization_id =
            slug_resolver.extract_and_resolve(&req.organization).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;
        let vault_id =
            slug_resolver.extract_and_resolve_vault(&req.vault).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let vault_val = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(organization_slug_val, vault_val);

        // Check current vault health
        let current_health = self.applied_state.vault_health(organization_id, vault_id);

        // Only recover diverged/recovering vaults unless force is set
        if !req.force {
            match &current_health {
                VaultHealthStatus::Healthy => {
                    ctx.set_error("AlreadyHealthy", "Vault is already healthy");
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: "Vault is already healthy. Use force=true to recover anyway."
                            .to_string(),
                        health_status: VaultHealthProto::Healthy.into(),
                        final_height: self.applied_state.vault_height(organization_id, vault_id),
                        final_state_root: None,
                    }));
                },
                VaultHealthStatus::Diverged { .. } | VaultHealthStatus::Recovering { .. } => {
                    // Proceed with recovery
                },
            }
        }

        // Require block archive for recovery
        let archive = match &self.block_archive {
            Some(a) => a,
            None => {
                ctx.set_error("Unavailable", "Block archive not configured");
                return Err(Status::unavailable(
                    "Block archive not configured, cannot recover vault",
                ));
            },
        };

        // Get expected height from applied state
        let expected_height = self.applied_state.vault_height(organization_id, vault_id);
        if expected_height == 0 {
            ctx.set_error("NoBlocks", "Vault has no blocks to recover");
            return Ok(Response::new(RecoverVaultResponse {
                success: false,
                message: "Vault has no blocks to recover".to_string(),
                health_status: VaultHealthProto::Healthy.into(),
                final_height: 0,
                final_state_root: None,
            }));
        }

        // Step 1: Clear vault state
        {
            let state = &*self.state;
            if let Err(e) = state.clear_vault(vault_id) {
                ctx.set_error("ClearFailed", &format!("{:?}", e));
                return Ok(Response::new(RecoverVaultResponse {
                    success: false,
                    message: format!("Failed to clear vault state: {:?}", e),
                    health_status: VaultHealthProto::Diverged.into(),
                    final_height: 0,
                    final_state_root: None,
                }));
            }
        }

        // Step 2: Replay blocks from archive
        ctx.start_storage_timer();
        let mut last_vault_hash: Option<[u8; 32]> = None;
        let mut divergence_detected = false;
        let mut final_state_root = ZERO_HASH;

        for height in 1..=expected_height {
            // Find region height for this vault height
            let region_height = match archive.find_region_height(organization_id, vault_id, height)
            {
                Ok(Some(h)) => h,
                Ok(None) => {
                    ctx.end_storage_timer();
                    ctx.set_error("MissingBlock", &format!("Block not found at height {}", height));
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: format!(
                            "Block not found in archive: org={}, vault={}, height={}",
                            organization_id, vault_id, height
                        ),
                        health_status: VaultHealthProto::Diverged.into(),
                        final_height: height - 1,
                        final_state_root: Some(Hash { value: final_state_root.to_vec() }),
                    }));
                },
                Err(e) => {
                    ctx.end_storage_timer();
                    ctx.set_error("IndexLookupFailed", &format!("{:?}", e));
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: format!("Index lookup failed at height {}: {:?}", height, e),
                        health_status: VaultHealthProto::Diverged.into(),
                        final_height: height - 1,
                        final_state_root: Some(Hash { value: final_state_root.to_vec() }),
                    }));
                },
            };

            // Read the region block
            let region_block = match archive.read_block(region_height) {
                Ok(b) => b,
                Err(e) => {
                    ctx.end_storage_timer();
                    ctx.set_error("BlockReadFailed", &format!("{:?}", e));
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: format!("Block read failed at height {}: {:?}", height, e),
                        health_status: VaultHealthProto::Diverged.into(),
                        final_height: height - 1,
                        final_state_root: Some(Hash { value: final_state_root.to_vec() }),
                    }));
                },
            };

            // Find the vault entry
            let entry = match region_block.vault_entries.iter().find(|e| {
                e.organization == organization_id && e.vault == vault_id && e.vault_height == height
            }) {
                Some(e) => e,
                None => {
                    ctx.end_storage_timer();
                    ctx.set_error("MissingEntry", "Vault entry not found in region block");
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: format!(
                            "Vault entry not found in region block at height {}",
                            height
                        ),
                        health_status: VaultHealthProto::Diverged.into(),
                        final_height: height - 1,
                        final_state_root: Some(Hash { value: final_state_root.to_vec() }),
                    }));
                },
            };

            // Verify chain continuity
            if let Some(expected_prev) = last_vault_hash
                && !hash_eq(&entry.previous_vault_hash, &expected_prev)
            {
                tracing::warn!(
                    height,
                    "Chain break detected during recovery: expected {:x?}, got {:x?}",
                    &expected_prev[..8],
                    &entry.previous_vault_hash[..8]
                );
            }

            // Apply transactions
            {
                let state = &*self.state;
                for tx in &entry.transactions {
                    if let Err(e) = state.apply_operations(vault_id, &tx.operations, height) {
                        ctx.end_storage_timer();
                        ctx.set_error("ApplyFailed", &format!("{:?}", e));
                        return Ok(Response::new(RecoverVaultResponse {
                            success: false,
                            message: format!(
                                "Transaction apply failed at height {}: {:?}",
                                height, e
                            ),
                            health_status: VaultHealthProto::Diverged.into(),
                            final_height: height - 1,
                            final_state_root: Some(Hash { value: final_state_root.to_vec() }),
                        }));
                    }
                }

                // Compute and verify state root
                match state.compute_state_root(vault_id) {
                    Ok(computed_root) => {
                        if computed_root != entry.state_root {
                            tracing::error!(
                                height,
                                "State divergence reproduced during recovery: computed {:x?}, expected {:x?}",
                                &computed_root[..8],
                                &entry.state_root[..8]
                            );
                            divergence_detected = true;
                            // Continue anyway to see if it recovers
                        }
                        final_state_root = computed_root;
                    },
                    Err(e) => {
                        ctx.end_storage_timer();
                        ctx.set_error("StateRootFailed", &format!("{:?}", e));
                        return Ok(Response::new(RecoverVaultResponse {
                            success: false,
                            message: format!(
                                "State root computation failed at height {}: {:?}",
                                height, e
                            ),
                            health_status: VaultHealthProto::Diverged.into(),
                            final_height: height - 1,
                            final_state_root: Some(Hash { value: final_state_root.to_vec() }),
                        }));
                    },
                }
            }

            // Track hash for next iteration
            last_vault_hash = Some(compute_vault_block_hash(entry));
        }
        ctx.end_storage_timer();

        // Step 3: Update vault health based on recovery result via Raft
        ctx.start_raft_timer();
        if divergence_detected {
            tracing::error!(
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                "Recovery reproduced divergence - possible determinism bug"
            );

            // Update vault health to Diverged via Raft for cluster-wide consistency
            if let Err(e) = self
                .handle
                .propose(RaftPayload::system(OrganizationRequest::UpdateVaultHealth {
                    organization: organization_id,
                    vault: vault_id,
                    healthy: false,
                    expected_root: None, // Already diverged during recovery
                    computed_root: Some(final_state_root),
                    diverged_at_height: Some(expected_height),
                    recovery_attempt: None,
                    recovery_started_at: None,
                }))
                .await
            {
                tracing::error!("Failed to update vault health via Raft: {}", e);
                // Continue with response - the local state will be inconsistent but
                // the next recovery attempt can retry
            }
            ctx.end_raft_timer();

            ctx.set_block_height(expected_height);
            ctx.set_error("DivergenceReproduced", "Recovery reproduced divergence");
            // Emit VaultRecovered handler-phase event (failed recovery)
            ctx.record_event(
                EventAction::VaultRecovered,
                EventOutcomeType::Failed {
                    code: "divergence_reproduced".to_string(),
                    detail: "Recovery reproduced divergence".to_string(),
                },
                &[("recovery_method", "replay"), ("final_height", &expected_height.to_string())],
            );
            Ok(Response::new(RecoverVaultResponse {
                success: false,
                message: "Recovery reproduced divergence - possible determinism bug. Manual investigation required.".to_string(),
                health_status: VaultHealthProto::Diverged.into(),
                final_height: expected_height,
                final_state_root: Some(Hash {
                    value: final_state_root.to_vec(),
                }),
            }))
        } else {
            // Update vault health to Healthy via Raft for cluster-wide consistency
            if let Err(e) = self
                .handle
                .propose(RaftPayload::system(OrganizationRequest::UpdateVaultHealth {
                    organization: organization_id,
                    vault: vault_id,
                    healthy: true,
                    expected_root: None,
                    computed_root: None,
                    diverged_at_height: None,
                    recovery_attempt: None,
                    recovery_started_at: None,
                }))
                .await
            {
                tracing::error!("Failed to update vault health via Raft: {}", e);
                // The vault was successfully recovered locally - log error but return success
            }
            ctx.end_raft_timer();

            ctx.set_block_height(expected_height);
            ctx.set_success();
            // Emit VaultRecovered handler-phase event (successful recovery)
            ctx.record_event(
                EventAction::VaultRecovered,
                EventOutcomeType::Success,
                &[("recovery_method", "replay"), ("final_height", &expected_height.to_string())],
            );
            Ok(Response::new(RecoverVaultResponse {
                success: true,
                message: "Vault recovered successfully".to_string(),
                health_status: VaultHealthProto::Healthy.into(),
                final_height: expected_height,
                final_state_root: Some(Hash { value: final_state_root.to_vec() }),
            }))
        }
    }

    /// Simulate vault divergence for testing.
    ///
    /// This forces a vault into the `Diverged` state without actual state corruption.
    /// Only available for testing purposes. The vault can be recovered using
    /// `RecoverVault` with `force=true`.
    async fn simulate_divergence(
        &self,
        request: Request<inferadb_ledger_proto::proto::SimulateDivergenceRequest>,
    ) -> Result<Response<inferadb_ledger_proto::proto::SimulateDivergenceResponse>, Status> {
        let mut ctx = self.make_request_context_unified("simulate_divergence", &request);
        let req = request.into_inner();

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_slug_val = req.organization.as_ref().map_or(0, |n| n.slug);
        let organization_id =
            slug_resolver.extract_and_resolve(&req.organization).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let vault_id =
            slug_resolver.extract_and_resolve_vault(&req.vault).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        let vault_val = req.vault.as_ref().map_or(0, |v| v.slug);
        ctx.set_target(organization_slug_val, vault_val);

        // Extract fake state roots for the simulated divergence
        let expected_root: [u8; 32] = req
            .expected_state_root
            .as_ref()
            .map(|h| h.value.as_slice().try_into().unwrap_or([0u8; 32]))
            .unwrap_or([1u8; 32]); // Default to non-zero for visibility

        let computed_root: [u8; 32] = req
            .computed_state_root
            .as_ref()
            .map(|h| h.value.as_slice().try_into().unwrap_or([0u8; 32]))
            .unwrap_or([2u8; 32]); // Different from expected

        let at_height = if req.at_height > 0 {
            req.at_height
        } else {
            // Get current vault height if not specified
            self.applied_state.vault_height(organization_id, vault_id).max(1)
        };

        ctx.set_block_height(at_height);

        tracing::warn!(
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
            at_height,
            "Simulating vault divergence for testing"
        );

        // Write to BOTH the GLOBAL and the ORGANIZATION'S Raft group.
        // - GLOBAL: HealthService reads vault health from GLOBAL applied state (cluster-wide view).
        // - ORGANIZATION group: WriteService / ReadService check vault health at request time from
        //   the per-organization applied state — that's where routing lands under B.1's per-org
        //   Raft groups with delegated leadership.
        ctx.start_raft_timer();

        let make_health_request = || OrganizationRequest::UpdateVaultHealth {
            organization: organization_id,
            vault: vault_id,
            healthy: false,
            expected_root: Some(expected_root),
            computed_root: Some(computed_root),
            diverged_at_height: Some(at_height),
            recovery_attempt: None,
            recovery_started_at: None,
        };

        // GLOBAL write (for HealthService)
        if let Err(e) = self.handle.propose(RaftPayload::system(make_health_request())).await {
            ctx.end_raft_timer();
            ctx.set_error("RaftError", &e.to_string());
            return Err(error_classify::storage_error(&e));
        }

        // ORGANIZATION group write (for WriteService / ReadService
        // apply-time checks). Route to the per-org group via
        // `RaftManager::route_organization`, which returns the
        // delegated-leadership organization group that services read
        // from. Falls back to the data-region group at
        // OrganizationId(0) while the per-org group is still
        // bootstrapping (route_organization handles the race itself).
        if let Some(ref manager) = self.raft_manager
            && let Some(org_group) = manager.route_organization(organization_id)
        {
            let _ = org_group.handle().propose(RaftPayload::system(make_health_request())).await;
        }

        ctx.end_raft_timer();
        ctx.set_success();

        Ok(Response::new(inferadb_ledger_proto::proto::SimulateDivergenceResponse {
            success: true,
            message: format!(
                "Vault {}:{} marked as diverged at height {}",
                organization_id.value(),
                vault_id.value(),
                at_height
            ),
            health_status: VaultHealthProto::Diverged.into(),
        }))
    }

    /// Forces TTL garbage collection on all vaults, removing expired entities.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn force_gc(
        &self,
        request: Request<inferadb_ledger_proto::proto::ForceGcRequest>,
    ) -> Result<Response<inferadb_ledger_proto::proto::ForceGcResponse>, Status> {
        // Reject if node is draining
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut ctx = self.make_request_context_unified("force_gc", &request);
        let req = request.into_inner();

        // Check if this node is the leader
        ctx.set_raft_term(self.handle.current_term());
        ctx.set_is_leader(self.handle.is_leader());

        if !self.handle.is_leader() {
            let msg = "Only the leader can run garbage collection";
            ctx.set_error("NotLeader", msg);
            return Err(super::metadata::not_leader_status_from_handle(
                self.handle.as_ref(),
                self.peer_addresses.as_ref(),
                msg,
            ));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut total_expired = 0u64;
        let mut vaults_scanned = 0u64;

        // Determine which vaults to scan
        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let vault_heights: Vec<((DomainOrganizationId, DomainVaultId), u64)> =
            if let (Some(ref organization_proto), Some(ref vault_proto)) =
                (req.organization, req.vault)
            {
                let org_id = slug_resolver.extract_and_resolve(&Some(*organization_proto))?;
                let v_id = slug_resolver
                    .resolve_vault(inferadb_ledger_types::VaultSlug::new(vault_proto.slug))?;
                ctx.set_target(organization_proto.slug, vault_proto.slug);
                // Single vault — post-γ, vault heights live in the
                // per-organization group's applied state.
                let height = self
                    .raft_manager
                    .as_ref()
                    .and_then(|m| m.route_organization(org_id))
                    .map(|g| g.applied_state().vault_height(org_id, v_id))
                    .unwrap_or_else(|| self.applied_state.vault_height(org_id, v_id));
                vec![((org_id, v_id), height)]
            } else if let Some(ref manager) = self.raft_manager {
                // All vaults — aggregate across per-organization groups.
                let mut heights = Vec::new();
                manager
                    .for_each_vault_across_groups(|org, vault, h| heights.push(((org, vault), h)));
                heights
            } else {
                let mut heights = Vec::new();
                self.applied_state
                    .for_each_vault_height(|org, vault, h| heights.push(((org, vault), h)));
                heights
            };

        ctx.start_raft_timer();
        for ((organization_id, vault_id), _height) in vault_heights {
            vaults_scanned += 1;

            // Resolve the vault's per-organization Raft group. Under B.1.8
            // routing, both entity state AND writes land in the per-org group
            // (not the data-region group), so both the read and the propose
            // target must come from `route_organization`. Falls back to the
            // local GLOBAL state + handle when no manager is configured
            // (single-Raft / unit-test mode).
            let (regional_state, regional_handle) = if let Some(ref manager) = self.raft_manager
                && let Some(group) = manager.route_organization(organization_id)
            {
                (group.state().clone(), Some(group.handle().clone()))
            } else {
                (self.state.clone(), None)
            };

            // Find expired entities in this vault
            let expired = {
                match regional_state.list_entities(vault_id, None, None, 1000) {
                    Ok(entities) => entities
                        .into_iter()
                        .filter(|e| e.expires_at > 0 && e.expires_at < now)
                        .map(|e| {
                            let key = String::from_utf8_lossy(&e.key).to_string();
                            (key, e.expires_at)
                        })
                        .collect::<Vec<_>>(),
                    Err(e) => {
                        tracing::warn!(organization_id = organization_id.value(), vault_id = vault_id.value(), error = %e, "Failed to list entities for GC");
                        continue;
                    },
                }
            };

            if expired.is_empty() {
                continue;
            }

            let count = expired.len();

            // Create ExpireEntity operations
            let operations: Vec<inferadb_ledger_types::Operation> = expired
                .iter()
                .map(|(key, expired_at)| inferadb_ledger_types::Operation::ExpireEntity {
                    key: key.clone(),
                    expired_at: *expired_at,
                })
                .collect();

            let transaction = inferadb_ledger_types::Transaction {
                id: *uuid::Uuid::new_v4().as_bytes(),
                client_id: inferadb_ledger_types::ClientId::new("system:gc"),
                sequence: now, // Use timestamp as sequence for GC
                operations,
                timestamp: chrono::Utc::now(),
            };

            // Propose to REGIONAL handle (where entity data lives), or fall back to GLOBAL
            let handle_target = regional_handle.as_ref().unwrap_or(&self.handle);
            match handle_target
                .propose(RaftPayload::system(OrganizationRequest::Write {
                    vault: vault_id,
                    transactions: vec![transaction],
                    idempotency_key: [0; 16],
                    request_hash: 0,
                    organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
                    vault_slug: inferadb_ledger_types::VaultSlug::new(0),
                }))
                .await
            {
                Ok(_) => {
                    total_expired += count as u64;
                },
                Err(e) => {
                    tracing::warn!(organization_id = organization_id.value(), vault_id = vault_id.value(), error = %e, "GC write failed");
                },
            }
        }
        ctx.end_raft_timer();

        // Set success with counts
        ctx.set_keys_count(vaults_scanned as usize);
        ctx.set_operations_count(total_expired as usize);
        ctx.set_success();

        Ok(Response::new(inferadb_ledger_proto::proto::ForceGcResponse {
            success: true,
            message: format!(
                "GC cycle complete: {} expired entities removed from {} vaults",
                total_expired, vaults_scanned
            ),
            expired_count: total_expired,
            vaults_scanned,
        }))
    }

    /// Updates runtime configuration (rate limits, hot key detection, compaction, validation)
    /// with optional dry-run support.
    async fn update_config(
        &self,
        request: Request<UpdateConfigRequest>,
    ) -> Result<Response<UpdateConfigResponse>, Status> {
        // Reject if node is draining
        super::helpers::check_not_draining(self.health_state.as_ref())?;
        let mut ctx = self.make_request_context_unified("update_config", &request);
        let inner = request.into_inner();

        let runtime_config = self.runtime_config.as_ref().ok_or_else(|| {
            Status::failed_precondition("Runtime configuration is not enabled on this node")
        })?;

        // Parse the JSON config.
        let new_config: inferadb_ledger_types::config::RuntimeConfig =
            serde_json::from_str(&inner.config_json)
                .map_err(|e| Status::invalid_argument(format!("Invalid config JSON: {e}")))?;

        // Validate before applying.
        new_config
            .validate()
            .map_err(|e| Status::invalid_argument(format!("Config validation failed: {e}")))?;

        // Compute diff for response.
        let current = runtime_config.load();
        let changed = current.diff(&new_config);

        if changed.is_empty() {
            let current_json = serde_json::to_string_pretty(&*current).unwrap_or_default();
            return Ok(Response::new(UpdateConfigResponse {
                applied: false,
                message: "No changes detected".to_string(),
                current_config_json: current_json,
                changed_fields: Vec::new(),
            }));
        }

        if inner.dry_run {
            let current_json = serde_json::to_string_pretty(&*current).unwrap_or_default();
            return Ok(Response::new(UpdateConfigResponse {
                applied: false,
                message: format!("Dry run: would change {}", changed.join(", ")),
                current_config_json: current_json,
                changed_fields: changed,
            }));
        }

        // Apply the update.
        let changed = runtime_config
            .update(new_config, self.rate_limiter.as_ref(), self.hot_key_detector.as_ref())
            .map_err(|e| Status::invalid_argument(format!("Config update failed: {e}")))?;

        // Emit ConfigurationChanged handler-phase event
        let changed_fields_str = changed.join(", ");
        ctx.set_success();
        ctx.record_event(
            EventAction::ConfigurationChanged,
            EventOutcomeType::Success,
            &[("changed_fields", &changed_fields_str)],
        );

        let updated = runtime_config.load();
        let updated_json = serde_json::to_string_pretty(&*updated).unwrap_or_default();

        Ok(Response::new(UpdateConfigResponse {
            applied: true,
            message: format!("Updated: {changed_fields_str}"),
            current_config_json: updated_json,
            changed_fields: changed,
        }))
    }

    /// Returns the current runtime configuration as JSON.
    async fn get_config(
        &self,
        _request: Request<GetConfigRequest>,
    ) -> Result<Response<GetConfigResponse>, Status> {
        let runtime_config = self.runtime_config.as_ref().ok_or_else(|| {
            Status::failed_precondition("Runtime configuration is not enabled on this node")
        })?;

        let current = runtime_config.load();
        let config_json = serde_json::to_string_pretty(&*current).unwrap_or_default();

        Ok(Response::new(GetConfigResponse { config_json }))
    }

    /// Creates a multi-DB archive backup of one organization.
    ///
    /// Captures the organization's full physical state — its per-org
    /// `_meta.db` / `raft.db` / `blocks.db` / `events.db` plus every
    /// per-vault `state.db` / `raft.db` / `blocks.db` / `events.db` — into
    /// a single `tar.zst` archive at
    /// `{backups_dir}/backup-{org_id}-{timestamp_micros}.tar.zst`.
    ///
    /// The manifest carries a SHA-256 fingerprint of the local node's
    /// RMK so [`Self::restore_backup`] can pre-flight any incoming
    /// archive against the local key material before unwrapping any DEK.
    async fn create_backup(
        &self,
        request: Request<CreateBackupRequest>,
    ) -> Result<Response<CreateBackupResponse>, Status> {
        let mut ctx = self.make_request_context_unified("create_backup", &request);
        let req = request.into_inner();

        let backup_manager = self
            .backup_manager
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Backup is not configured on this node"))?;

        let key_manager = self.key_manager.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "Key manager is not configured on this node — cannot stamp \
                 RMK fingerprint on backup archive",
            )
        })?;

        let raft_manager = self.raft_manager.as_ref().ok_or_else(|| {
            Status::failed_precondition("Raft manager is not configured on this node")
        })?;

        // Resolve organization slug → internal id at the service boundary.
        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_id =
            slug_resolver.extract_and_resolve(&req.organization).inspect_err(|status| {
                ctx.set_error("OrganizationNotFound", status.message());
            })?;
        let organization_slug = slug_resolver.resolve_slug(organization_id)?;

        let tag = req.tag.unwrap_or_default();

        // The organization's home region governs which `{data_dir}/{region}/{org_id}/`
        // tree the archive enumerates. The local node's region (the
        // control-plane host) is *not* always the same — orgs are
        // routed to their declared data region at creation time.
        let region = raft_manager.get_organization_region(organization_id).ok_or_else(|| {
            ctx.set_error("OrganizationNotPlaced", &organization_id.to_string());
            Status::failed_precondition(format!(
                "Organization {} has no region placement on file",
                organization_id.value()
            ))
        })?;

        // Compute the local RMK fingerprint up front — we stamp the
        // manifest with this so a future restore on a node with a
        // different RMK is rejected at the pre-flight check.
        let rmk_fingerprint = key_manager.rmk_fingerprint(region).map_err(|e| {
            ctx.set_error("RmkFingerprintError", &e.to_string());
            error_classify::crypto_error(&e)
        })?;

        // Sync every per-vault state DB so the file bytes the archive
        // captures are durable. The archive copies file bytes verbatim;
        // un-synced pages remain in the page cache and would not appear
        // in the tar stream.
        let all_vaults = self.applied_state.all_vaults();
        for (_org_id, vault_id) in all_vaults.keys() {
            let db = self.state.db_for(*vault_id).map_err(|e| {
                ctx.set_error("BackupVaultOpenError", &e.to_string());
                error_classify::storage_error(&e)
            })?;
            db.sync_state().await.map_err(|e| {
                ctx.set_error("SyncStateError", &e.to_string());
                error_classify::storage_error(&e)
            })?;
        }

        // Resolve archive output path. The backup_id encodes
        // `{org_id}-{timestamp_micros}` so list/restore can resolve a
        // backup_id back to a path without a sidecar file.
        let backups_dir = self.backups_dir.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "Backup destination directory is not configured on this node",
            )
        })?;
        let timestamp_micros = chrono::Utc::now().timestamp_micros();
        let backup_id = format!("{}-{}", organization_id.value(), timestamp_micros);
        let archive_filename = format!("backup-{backup_id}.tar.zst");
        let archive_path = backups_dir.join(&archive_filename);

        // Build the archive. `create_archive` stamps the resolved
        // organization slug and RMK fingerprint into the manifest
        // before the tar stream is finalized, so the on-disk archive
        // carries them durably (no post-hoc patching).
        let manifest = backup_manager
            .create_archive(
                region,
                organization_id,
                organization_slug,
                rmk_fingerprint,
                &archive_path,
            )
            .await
            .map_err(|e| {
                ctx.set_error("BackupError", &e.to_string());
                error_classify::storage_error(&e)
            })?;

        let size_bytes = std::fs::metadata(&archive_path)
            .map(|m| m.len())
            .map_err(|e| error_classify::storage_error(&e))?;

        ctx.set_success();
        inferadb_ledger_raft::metrics::record_backup_created(0, size_bytes);

        ctx.record_event(
            EventAction::BackupCreated,
            EventOutcomeType::Success,
            &[
                ("backup_id", &backup_id),
                ("tag", &tag),
                ("organization_slug", &organization_slug.value().to_string()),
            ],
        );

        let manifest_proto = backup_manifest_to_proto(&manifest);

        Ok(Response::new(CreateBackupResponse {
            backup_id,
            backup_path: archive_path.display().to_string(),
            size_bytes,
            manifest: Some(manifest_proto),
        }))
    }

    /// Lists available archive backups in the local backups directory.
    ///
    /// Iterates `{backups_dir}/backup-*.tar.zst`, opens each manifest
    /// without extracting the rest of the archive, and returns a
    /// summary entry per archive. Corrupt or unreadable archives are
    /// skipped with a `WARN` log entry rather than failing the whole
    /// list.
    async fn list_backups(
        &self,
        request: Request<ListBackupsRequest>,
    ) -> Result<Response<ListBackupsResponse>, Status> {
        let req = request.into_inner();

        let backups_dir = self.backups_dir.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "Backup destination directory is not configured on this node",
            )
        })?;

        let mut entries: Vec<BackupInfo> = Vec::new();

        if backups_dir.exists() {
            for dir_entry in
                std::fs::read_dir(backups_dir).map_err(|e| error_classify::storage_error(&e))?
            {
                let dir_entry = dir_entry.map_err(|e| error_classify::storage_error(&e))?;
                let filename = dir_entry.file_name();
                let name = filename.to_string_lossy();

                let Some(rest) = name.strip_prefix("backup-") else {
                    continue;
                };
                let Some(backup_id) = rest.strip_suffix(".tar.zst") else {
                    continue;
                };

                let path = dir_entry.path();
                let Ok(metadata) = std::fs::metadata(&path) else {
                    continue;
                };
                let size_bytes = metadata.len();
                let created_at =
                    metadata.created().ok().or_else(|| metadata.modified().ok()).and_then(|st| {
                        st.duration_since(std::time::UNIX_EPOCH).ok().map(|d| {
                            prost_types::Timestamp {
                                seconds: d.as_secs() as i64,
                                nanos: i32::try_from(d.subsec_nanos()).unwrap_or(0),
                            }
                        })
                    });

                let manifest_proto = match std::fs::File::open(&path) {
                    Ok(file) => match inferadb_ledger_raft::backup::archive::read_manifest(file) {
                        Ok(manifest) => Some(backup_manifest_to_proto(&manifest)),
                        Err(e) => {
                            tracing::warn!(
                                path = %path.display(),
                                error = %e,
                                "Skipping backup with unreadable manifest"
                            );
                            None
                        },
                    },
                    Err(e) => {
                        tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "Skipping backup whose archive cannot be opened"
                        );
                        None
                    },
                };

                let tag = manifest_proto.as_ref().map(|_| String::new()).unwrap_or_default();

                entries.push(BackupInfo {
                    backup_id: backup_id.to_string(),
                    backup_path: path.display().to_string(),
                    size_bytes,
                    created_at,
                    tag,
                    manifest: manifest_proto,
                });
            }
        }

        // Sort newest first by `created_at` (falling back to backup_id
        // suffix for archives that lost their fs metadata).
        entries.sort_by(|a, b| match (a.created_at.as_ref(), b.created_at.as_ref()) {
            (Some(at), Some(bt)) => bt.seconds.cmp(&at.seconds).then(bt.nanos.cmp(&at.nanos)),
            _ => b.backup_id.cmp(&a.backup_id),
        });

        if req.limit > 0 && entries.len() > req.limit as usize {
            entries.truncate(req.limit as usize);
        }

        Ok(Response::new(ListBackupsResponse { backups: entries }))
    }

    /// Stages a multi-DB archive for offline restore.
    ///
    /// Resolves `backup_id` to `{backups_dir}/backup-{id}.tar.zst`,
    /// pre-flights the archive against the local node's RMK
    /// fingerprint, then unpacks it under
    /// `{data_dir}/.restore-staging/`. The archive is *not* swapped onto
    /// the live data directory by this RPC — operators run
    /// `inferadb-ledger restore apply` after stopping the node to swap
    /// the staged tree atomically.
    async fn restore_backup(
        &self,
        request: Request<RestoreBackupRequest>,
    ) -> Result<Response<RestoreBackupResponse>, Status> {
        // Reject if node is draining
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut ctx = self.make_request_context_unified("restore_backup", &request);
        let req = request.into_inner();

        let backup_manager = self
            .backup_manager
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Backup is not configured on this node"))?;

        let key_manager = self.key_manager.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "Key manager is not configured on this node — cannot validate \
                 backup archive RMK fingerprint",
            )
        })?;

        let backups_dir = self.backups_dir.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "Backup destination directory is not configured on this node",
            )
        })?;

        // Resolve `backup_id` → archive path. The id is operator-supplied;
        // validate it has no path separators or `..` segments before
        // joining onto the backups directory.
        if req.backup_id.is_empty()
            || req.backup_id.contains('/')
            || req.backup_id.contains('\\')
            || req.backup_id.split('-').any(|seg| seg == "..")
        {
            ctx.set_error("InvalidBackupId", &req.backup_id);
            return Err(Status::invalid_argument(format!("Invalid backup_id: {}", req.backup_id)));
        }

        let archive_path = backups_dir.join(format!("backup-{}.tar.zst", req.backup_id));
        if !archive_path.exists() {
            ctx.set_error("BackupNotFound", &archive_path.display().to_string());
            return Err(Status::not_found(format!(
                "Backup archive not found: {}",
                archive_path.display()
            )));
        }

        // Compute the local RMK fingerprint up front so the manager can
        // pre-flight the manifest before staging any bytes. Restore
        // requires the local node to hold a key for the manifest's
        // region; we read the manifest cheaply first to find the right
        // region, then ask the key manager for that region's
        // fingerprint.
        let preflight_file = std::fs::File::open(&archive_path).map_err(|e| {
            ctx.set_error("ArchiveOpenError", &e.to_string());
            error_classify::storage_error(&e)
        })?;
        let preflight_manifest =
            inferadb_ledger_raft::backup::archive::read_manifest(preflight_file).map_err(|e| {
                ctx.set_error("ManifestReadError", &e.to_string());
                error_classify::storage_error(&e)
            })?;
        let region = preflight_manifest.region;
        let local_fingerprint = key_manager.rmk_fingerprint(region).map_err(|e| {
            ctx.set_error("RmkFingerprintError", &e.to_string());
            error_classify::crypto_error(&e)
        })?;

        let staging_result = backup_manager
            .stage_restore(&archive_path, Some(&local_fingerprint))
            .await
            .map_err(|e| {
                ctx.set_error("StageRestoreError", &e.to_string());
                error_classify::storage_error(&e)
            })?;

        let manifest_proto = backup_manifest_to_proto(&staging_result.manifest);
        let staging_dir = staging_result.staging_dir.display().to_string();

        ctx.set_success();

        // Emit BackupRestored handler-phase event — operators see the
        // staged restore in the audit trail even before the offline swap.
        ctx.record_event(
            EventAction::BackupRestored,
            EventOutcomeType::Success,
            &[("backup_id", &req.backup_id), ("staging_dir", &staging_dir)],
        );

        Ok(Response::new(RestoreBackupResponse { staging_dir, manifest: Some(manifest_proto) }))
    }

    /// Transfers Raft leadership to a target node, or the best candidate if unspecified.
    async fn transfer_leadership(
        &self,
        request: Request<TransferLeadershipRequest>,
    ) -> Result<Response<TransferLeadershipResponse>, Status> {
        let mut ctx = self.make_request_context_unified("transfer_leadership", &request);
        let req = request.into_inner();

        // Validate and default timeout
        let timeout_ms = if req.timeout_ms == 0 { 10_000u32 } else { req.timeout_ms.min(60_000) };

        let target = if req.target_node_id == 0 { None } else { Some(req.target_node_id) };

        let config = inferadb_ledger_raft::leader_transfer::LeaderTransferConfig::builder()
            .timeout(std::time::Duration::from_millis(u64::from(timeout_ms)))
            .build();

        let start = std::time::Instant::now();
        let result = inferadb_ledger_raft::leader_transfer::transfer_leadership(
            &self.handle,
            target,
            &self.transfer_lock,
            &config,
        )
        .await;

        let latency = start.elapsed().as_secs_f64();

        match result {
            Ok(new_leader) => {
                metrics::record_leader_transfer(true, latency);
                ctx.set_success();
                Ok(Response::new(TransferLeadershipResponse {
                    success: true,
                    new_leader_id: new_leader,
                    message: String::new(),
                }))
            },
            Err(e) => {
                metrics::record_leader_transfer(false, latency);
                ctx.set_error("LeaderTransferError", &e.to_string());
                let status = match &e {
                    inferadb_ledger_raft::leader_transfer::LeaderTransferError::NotLeader
                    | inferadb_ledger_raft::leader_transfer::LeaderTransferError::NoTarget
                    | inferadb_ledger_raft::leader_transfer::LeaderTransferError::TargetRejected { .. } => {
                        Status::failed_precondition(e.to_string())
                    },
                    inferadb_ledger_raft::leader_transfer::LeaderTransferError::TransferInProgress => {
                        Status::aborted(e.to_string())
                    },
                    inferadb_ledger_raft::leader_transfer::LeaderTransferError::ReplicationTimeout
                    | inferadb_ledger_raft::leader_transfer::LeaderTransferError::Timeout { .. } => {
                        Status::deadline_exceeded(e.to_string())
                    },
                    inferadb_ledger_raft::leader_transfer::LeaderTransferError::Connection { .. }
                    | inferadb_ledger_raft::leader_transfer::LeaderTransferError::Rpc { .. } => {
                        error_classify::storage_error(&e)
                    },
                };
                Err(status)
            },
        }
    }

    /// Initiates rotation of the email blinding key by recording the new version through Raft.
    ///
    /// Returns `complete: false` after committing the version change. The actual re-hashing
    /// of email HMAC entries is performed asynchronously by a background job.
    /// Poll `GetBlindingKeyRehashStatus` to track progress; `complete: true` there means
    /// the rehash is finished.
    async fn rotate_blinding_key(
        &self,
        request: Request<RotateBlindingKeyRequest>,
    ) -> Result<Response<RotateBlindingKeyResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut ctx = self.make_request_context_unified("rotate_blinding_key", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        if req.new_key_version == 0 {
            ctx.set_error("InvalidArgument", "new_key_version must be > 0");
            return Err(Status::invalid_argument("new_key_version must be > 0"));
        }

        // Read current version to validate monotonic increase
        let system_service = SystemOrganizationService::new(Arc::clone(&self.state));
        let current_version = system_service
            .get_blinding_key_version()
            .map_err(|e| error_classify::crypto_error(&e))?
            .unwrap_or(0);

        if req.new_key_version <= current_version {
            ctx.set_error(
                "InvalidArgument",
                "new_key_version must be greater than the current version",
            );
            return Err(Status::invalid_argument(format!(
                "new_key_version ({}) must be greater than current version ({current_version})",
                req.new_key_version,
            )));
        }

        // Check if a rotation is already in progress
        let rotation_in_progress = system_service
            .is_rotation_in_progress()
            .map_err(|e| error_classify::crypto_error(&e))?;

        if rotation_in_progress {
            ctx.set_error("FailedPrecondition", "A blinding key rotation is already in progress");
            return Err(Status::failed_precondition(
                "A blinding key rotation is already in progress",
            ));
        }

        // Propose the version change through Raft
        let response = self
            .propose_raft_request(
                SystemRequest::SetBlindingKeyVersion { version: req.new_key_version },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::Empty => {
                // Record audit event
                ctx.record_event(
                    EventAction::ConfigurationChanged,
                    EventOutcomeType::Success,
                    &[
                        ("resource", "blinding_key"),
                        ("new_version", &req.new_key_version.to_string()),
                        ("previous_version", &current_version.to_string()),
                    ],
                );

                ctx.set_success();
                Ok(Response::new(RotateBlindingKeyResponse {
                    total_entries: 0,
                    entries_rehashed: 0,
                    complete: false,
                }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(code, message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    /// Returns the current status of a blinding key rotation, including per-region progress.
    ///
    /// This is a local read — no Raft proposal needed.
    async fn get_blinding_key_rehash_status(
        &self,
        request: Request<GetBlindingKeyRehashStatusRequest>,
    ) -> Result<Response<GetBlindingKeyRehashStatusResponse>, Status> {
        let mut ctx = self.make_request_context_unified("get_blinding_key_rehash_status", &request);
        let _req = request.into_inner();

        let system_service = SystemOrganizationService::new(Arc::clone(&self.state));

        let active_key_version = system_service
            .get_blinding_key_version()
            .map_err(|e| error_classify::crypto_error(&e))?
            .unwrap_or(0);

        // Collect per-region progress
        let mut per_region_progress = std::collections::HashMap::new();
        let mut total_rehashed: u64 = 0;

        for region in ALL_REGIONS {
            match system_service.get_rehash_progress(region) {
                Ok(Some(entries)) => {
                    per_region_progress.insert(region.as_str().to_string(), entries);
                    total_rehashed = total_rehashed.saturating_add(entries);
                },
                Ok(None) => {},
                Err(e) => {
                    ctx.set_error("Internal", &e.to_string());
                    return Err(error_classify::storage_error(&e));
                },
            }
        }

        let complete = per_region_progress.is_empty();

        ctx.set_success();
        Ok(Response::new(GetBlindingKeyRehashStatusResponse {
            total_entries: 0,
            entries_rehashed: total_rehashed,
            complete,
            per_region_progress,
            active_key_version,
        }))
    }

    /// Initiates RMK rotation and triggers asynchronous DEK re-wrapping.
    ///
    /// The re-wrapping runs as a background job. Poll `GetRewrapStatus` for progress.
    async fn rotate_region_key(
        &self,
        request: Request<RotateRegionKeyRequest>,
    ) -> Result<Response<RotateRegionKeyResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut ctx = self.make_request_context_unified("rotate_region_key", &request);
        let req = request.into_inner();

        let progress = self.rewrap_progress.as_ref().ok_or_else(|| {
            Status::failed_precondition("DEK re-wrapping not configured for this node")
        })?;

        // Get total page count from the state layer
        let total_pages = self.state.sidecar_page_count().map_err(|e| {
            ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;

        let target_version = if req.target_version > 0 { req.target_version } else { 0 };

        // If no pages, already complete
        if total_pages == 0 {
            ctx.set_success();
            return Ok(Response::new(RotateRegionKeyResponse {
                new_version: target_version,
                total_pages: 0,
                complete: true,
            }));
        }

        // Start the re-wrapping cycle
        progress.start_rotation(target_version, total_pages);

        ctx.set_success();
        Ok(Response::new(RotateRegionKeyResponse {
            new_version: target_version,
            total_pages,
            complete: false,
        }))
    }

    /// Returns the current status of DEK re-wrapping progress.
    ///
    /// This is a local read — no Raft proposal needed.
    async fn get_rewrap_status(
        &self,
        request: Request<GetRewrapStatusRequest>,
    ) -> Result<Response<GetRewrapStatusResponse>, Status> {
        let mut ctx = self.make_request_context_unified("get_rewrap_status", &request);
        let _req = request.into_inner();

        let progress = self.rewrap_progress.as_ref().ok_or_else(|| {
            Status::failed_precondition("DEK re-wrapping not configured for this node")
        })?;

        let total = progress.total_pages.load(std::sync::atomic::Ordering::Acquire);
        let processed = progress.next_page_id.load(std::sync::atomic::Ordering::Acquire);
        let rewrapped = progress.pages_rewrapped.load(std::sync::atomic::Ordering::Acquire);
        let complete = progress.complete.load(std::sync::atomic::Ordering::Acquire);
        let target = progress.target_version.load(std::sync::atomic::Ordering::Acquire);
        let est_remaining = progress.estimated_remaining_secs();

        ctx.set_success();
        Ok(Response::new(GetRewrapStatusResponse {
            total_pages: total,
            pages_processed: processed,
            pages_rewrapped: rewrapped,
            complete,
            target_version: target as u32,
            estimated_remaining_secs: est_remaining,
        }))
    }

    /// Migrates existing users from flat `_system` store to regional directory
    /// structure. One-time admin operation.
    ///
    /// Pre-validates blinding key and default region. Reads flat user records,
    /// computes email HMACs (blinding key stays in handler, never enters Raft
    /// log), generates per-subject keys, and proposes a single atomic Raft
    /// write with all migration entries.
    async fn migrate_existing_users(
        &self,
        request: Request<MigrateExistingUsersRequest>,
    ) -> Result<Response<MigrateExistingUsersResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let start = std::time::Instant::now();
        let mut ctx = self.make_request_context_unified("migrate_existing_users", &request);
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Pre-validation: parse default region (reject UNSPECIFIED).
        let default_region = inferadb_ledger_proto::convert::region_from_i32(req.default_region)?;

        // Pre-validation: reject GLOBAL as default region (users must live in
        // a data region, not the control plane).
        if default_region == inferadb_ledger_types::Region::GLOBAL {
            ctx.set_error("InvalidArgument", "default_region cannot be GLOBAL");
            return Err(Status::invalid_argument(
                "default_region cannot be GLOBAL: users must reside in a data region",
            ));
        }

        // Pre-validation: parse email blinding key from hex.
        if req.email_blinding_key.is_empty() {
            ctx.set_error("InvalidArgument", "email_blinding_key is required");
            return Err(Status::invalid_argument("email_blinding_key is required"));
        }
        let blinding_key =
            match inferadb_ledger_types::EmailBlindingKey::from_str(&req.email_blinding_key) {
                Ok(k) => k,
                Err(e) => {
                    ctx.set_error("InvalidArgument", &format!("invalid blinding key: {e}"));
                    return Err(Status::invalid_argument(format!(
                        "invalid email_blinding_key: {e}"
                    )));
                },
            };

        // Read all flat user records from the applied state.
        let sys = SystemOrganizationService::new(self.state.clone());
        let users = match sys.list_flat_users() {
            Ok(u) => u,
            Err(e) => {
                ctx.set_error("Internal", &format!("failed to list users: {e}"));
                return Err(error_classify::storage_error(&e));
            },
        };

        if users.is_empty() {
            ctx.set_success();
            return Ok(Response::new(MigrateExistingUsersResponse {
                users: 0,
                migrated: 0,
                skipped: 0,
                errors: 0,
                elapsed_secs: start.elapsed().as_secs_f64(),
            }));
        }

        // Build pre-computed migration entries.
        let mut entries = Vec::with_capacity(users.len());
        let mut pre_skipped: u64 = 0;

        for user in &users {
            // Check if already migrated (directory entry exists).
            match sys.get_user_directory(user.id) {
                Ok(Some(_)) => {
                    pre_skipped += 1;
                    continue;
                },
                Ok(None) => {},
                Err(_) => {
                    pre_skipped += 1;
                    continue;
                },
            }

            // Determine target region: use user's declared region if not GLOBAL,
            // otherwise use the default.
            let target_region = if user.region == inferadb_ledger_types::Region::GLOBAL {
                default_region
            } else {
                user.region
            };

            // Read the user's email to compute HMAC.
            let email_hmac_hex = match sys.get_user_email(user.email) {
                Ok(Some(user_email)) => {
                    inferadb_ledger_types::compute_email_hmac(&blinding_key, &user_email.email)
                },
                _ => {
                    // No email record — skip this user (corrupted data).
                    pre_skipped += 1;
                    continue;
                },
            };

            // Generate random per-user crypto-shredding key.
            let mut shred_key_bytes = [0u8; 32];
            rand::Rng::fill_bytes(&mut rand::rng(), &mut shred_key_bytes);

            entries.push(inferadb_ledger_state::system::UserMigrationEntry {
                user: user.id,
                slug: user.slug,
                region: target_region,
                hmac: email_hmac_hex,
                bytes: shred_key_bytes,
            });
        }

        // If all users were pre-skipped, return early without Raft proposal.
        if entries.is_empty() {
            ctx.set_success();
            return Ok(Response::new(MigrateExistingUsersResponse {
                users: users.len() as u64,
                migrated: 0,
                skipped: pre_skipped,
                errors: 0,
                elapsed_secs: start.elapsed().as_secs_f64(),
            }));
        }

        // Propose migration through Raft.
        let response = self
            .propose_raft_request(
                SystemRequest::MigrateExistingUsers { entries },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::UsersMigrated { users, migrated, skipped, errors } => {
                // Record audit event.
                ctx.record_event(
                    EventAction::UsersMigrated,
                    EventOutcomeType::Success,
                    &[
                        ("users", &users.to_string()),
                        ("migrated", &migrated.to_string()),
                        ("skipped", &(skipped + pre_skipped).to_string()),
                        ("errors", &errors.to_string()),
                        ("default_region", default_region.as_str()),
                    ],
                );

                ctx.set_success();
                Ok(Response::new(MigrateExistingUsersResponse {
                    users,
                    migrated,
                    skipped: skipped + pre_skipped,
                    errors,
                    elapsed_secs: start.elapsed().as_secs_f64(),
                }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(
                    code,
                    format!("Migration failed: {message}"),
                ))
            },
            other => {
                ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                Err(Status::internal("Unexpected response from Raft state machine"))
            },
        }
    }

    /// Eagerly provisions a Raft group for a region.
    ///
    /// Normally regional groups are created lazily on first organization or
    /// user assignment. This RPC allows pre-provisioning for capacity planning.
    async fn provision_region(
        &self,
        request: Request<ProvisionRegionRequest>,
    ) -> Result<Response<ProvisionRegionResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let start = std::time::Instant::now();
        let mut ctx = self.make_request_context_unified("provision_region", &request);
        let req = request.into_inner();

        let region = inferadb_ledger_proto::convert::region_from_i32(req.region)?;

        if region == inferadb_ledger_types::Region::GLOBAL {
            ctx.set_error("InvalidArgument", "GLOBAL region cannot be provisioned");
            return Err(Status::invalid_argument(
                "GLOBAL region is always created eagerly on startup",
            ));
        }

        let manager = self.raft_manager.as_ref().ok_or_else(|| {
            Status::unavailable("Region provisioning not available on single-region nodes")
        })?;

        // Idempotency fast path: if the region already exists locally, the
        // CreateDataRegion entry was already proposed (and applied on this
        // node). Return success without re-proposing — the apply path is
        // a no-op for an already-running region but the propose-and-wait
        // handshake here would still pay a Raft round-trip.
        if manager.get_region_group(region).is_ok() {
            tracing::info!(
                region = region.as_str(),
                created = false,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "provision_region completed (already provisioned)"
            );
            let proto_region: ProtoRegion = region.into();
            return Ok(Response::new(ProvisionRegionResponse {
                created: false,
                region: proto_region.into(),
            }));
        }

        // Propose CreateDataRegion through GLOBAL Raft so every node
        // (leader and followers) starts the data region group locally via
        // its apply-side region-creation handler. Without this, only the
        // node receiving the RPC starts the region group; followers
        // remain unaware. Subsequent membership changes (DR scheduler
        // adding peers as voters) then promote nodes that have no local
        // region group, leaving regional proposals unable to reach
        // quorum.
        //
        // Initial members is the full GLOBAL voter set so apply-time
        // peer-transport setup runs on every node. The DR scheduler
        // refines membership as nodes' health states require.
        let initial_members: Vec<(u64, String)> = manager
            .system_region()
            .map_err(|e| {
                ctx.set_error("Internal", &format!("system region not available: {e}"));
                Status::unavailable(format!("system region not available: {e}"))
            })?
            .handle()
            .shard_state()
            .voters
            .iter()
            .filter_map(|n| {
                let id = n.0;
                manager.peer_addresses().get(id).map(|addr| (id, addr))
            })
            .collect();

        if initial_members.is_empty() {
            ctx.set_error("Internal", "no GLOBAL voters with known addresses");
            return Err(Status::failed_precondition(
                "Cannot provision region: no GLOBAL voters with registered peer addresses",
            ));
        }

        let req = inferadb_ledger_raft::types::SystemRequest::CreateDataRegion {
            region,
            initial_members,
        };
        let response = self
            .handle
            .propose_and_wait(
                inferadb_ledger_raft::types::RaftPayload::system(req),
                std::time::Duration::from_secs(10),
            )
            .await
            .map_err(|e| {
                ctx.set_error("Internal", &format!("CreateDataRegion propose failed: {e}"));
                error_classify::raft_error(&e)
            })?;

        let created = matches!(
            response,
            inferadb_ledger_raft::types::LedgerResponse::DataRegionCreated { .. }
        );

        // Wait for the local node's region-creation handler to spin up
        // the region group. The apply-side signal is fire-and-forget;
        // returning here before the local group is registered would let
        // a follow-on RPC against this same node race the handler.
        let local_ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if manager.get_region_group(region).is_ok() {
                break;
            }
            if tokio::time::Instant::now() >= local_ready_deadline {
                ctx.set_error("Internal", "local region group did not start within 5s");
                return Err(Status::deadline_exceeded(
                    "Region created in cluster state but local group did not start within 5s",
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        tracing::info!(
            region = region.as_str(),
            created,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "provision_region completed"
        );

        let proto_region: ProtoRegion = region.into();
        Ok(Response::new(ProvisionRegionResponse { created, region: proto_region.into() }))
    }
}

/// Computes the hash of a vault block entry for chain verification.
///
/// The vault block hash commits to all content: height, previous hash,
/// transactions (via tx_merkle_root), and state root.
fn compute_vault_block_hash(entry: &VaultEntry) -> [u8; 32] {
    let mut hasher = Sha256::new();

    // Hash the vault block header fields
    hasher.update(entry.organization.value().to_le_bytes());
    hasher.update(entry.vault.value().to_le_bytes());
    hasher.update(entry.vault_height.to_le_bytes());
    hasher.update(entry.previous_vault_hash);
    hasher.update(entry.tx_merkle_root);
    hasher.update(entry.state_root);

    hasher.finalize().into()
}

/// Persists a cluster ID to `{data_dir}/cluster_id`.
///
/// Converts a domain [`inferadb_ledger_raft::backup::archive::BackupManifest`]
/// into its proto wire representation for inclusion in
/// [`CreateBackupResponse`] / [`RestoreBackupResponse`] / [`BackupInfo`].
///
/// The proto mirror is intentionally a flat copy — the conversion only
/// re-shapes types (`Region` enum → i32, `OrganizationSlug` → message
/// wrapper, `Vec<DbEntry>` → `Vec<BackupDbEntry>`). No validation logic
/// lives here; a manifest that round-trips through this helper carries
/// the same byte values as the on-disk JSON manifest member.
fn backup_manifest_to_proto(
    manifest: &inferadb_ledger_raft::backup::archive::BackupManifest,
) -> inferadb_ledger_proto::proto::BackupManifest {
    let proto_region: inferadb_ledger_proto::proto::Region = manifest.region.into();

    let dbs = manifest
        .dbs
        .iter()
        .map(|entry| inferadb_ledger_proto::proto::BackupDbEntry {
            path: entry.path.clone(),
            size_bytes: entry.size_bytes,
            checksum: entry.checksum.clone(),
        })
        .collect();

    inferadb_ledger_proto::proto::BackupManifest {
        schema_version: manifest.schema_version,
        format: manifest.format.clone(),
        region: proto_region as i32,
        organization: Some(inferadb_ledger_proto::proto::OrganizationSlug {
            slug: manifest.organization_slug.value(),
        }),
        organization_id: manifest.organization_id.value(),
        timestamp_micros: manifest.timestamp_micros,
        rmk_fingerprint: manifest.rmk_fingerprint.clone(),
        node_id_at_creation: manifest.node_id_at_creation,
        dbs,
        vault_count: manifest.vault_count,
        created_by_app_version: manifest.created_by_app_version.clone(),
    }
}

/// Thin wrapper matching the format used by the server crate's `cluster_id` module.
/// Duplicated here because the services crate cannot depend on the server crate.
fn write_cluster_id_to_disk(data_dir: &std::path::Path, cluster_id: u64) -> Result<(), Status> {
    let path = data_dir.join("cluster_id");
    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| Status::internal(format!("failed to create cluster_id directory: {e}")))?;
    }
    std::fs::write(&path, cluster_id.to_string())
        .map_err(|e| Status::internal(format!("failed to persist cluster_id: {e}")))?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    // =========================================================================
    // compute_vault_block_hash Tests
    // =========================================================================

    #[test]
    fn vault_block_hash_deterministic() {
        // Same input should always produce the same hash
        let entry = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash1 = compute_vault_block_hash(&entry);
        let hash2 = compute_vault_block_hash(&entry);

        assert_eq!(hash1, hash2, "Hash must be deterministic");
    }

    #[test]
    fn vault_block_hash_different_for_different_inputs() {
        let entry1 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let entry2 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 11, // Different height
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash1 = compute_vault_block_hash(&entry1);
        let hash2 = compute_vault_block_hash(&entry2);

        assert_ne!(hash1, hash2, "Different inputs should produce different hashes");
    }

    #[test]
    fn vault_block_hash_different_state_root() {
        let entry1 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let entry2 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [1u8; 32], // Different state root,

            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash1 = compute_vault_block_hash(&entry1);
        let hash2 = compute_vault_block_hash(&entry2);

        assert_ne!(hash1, hash2, "Different state_root should produce different hash");
    }

    #[test]
    fn vault_block_hash_chain_continuity() {
        // Simulate a chain of blocks
        let entry1 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(1),
            vault_height: 1,
            previous_vault_hash: ZERO_HASH, // Genesis block
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [1u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash1 = compute_vault_block_hash(&entry1);

        let entry2 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(1),
            vault_height: 2,
            previous_vault_hash: hash1, // Chain to previous block
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [2u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash2 = compute_vault_block_hash(&entry2);

        // Verify the hash commits to the chain
        assert_ne!(hash1, hash2);

        // If we create entry2 with wrong previous_vault_hash, it should differ
        let entry2_wrong = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(1),
            vault_height: 2,
            previous_vault_hash: ZERO_HASH, // Wrong previous hash
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [2u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let hash2_wrong = compute_vault_block_hash(&entry2_wrong);
        assert_ne!(
            hash2, hash2_wrong,
            "Different previous_vault_hash should produce different hash"
        );
    }

    #[test]
    fn vault_block_hash_includes_all_fields() {
        let base_entry = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 3,
            previous_vault_hash: [4u8; 32],
            transactions: vec![],
            tx_merkle_root: [5u8; 32],
            state_root: [6u8; 32],
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(0),
            vault_slug: inferadb_ledger_types::VaultSlug::new(0),
        };

        let base_hash = compute_vault_block_hash(&base_entry);

        // Changing organization_id should change hash
        let mut modified = base_entry.clone();
        modified.organization = DomainOrganizationId::new(99);
        assert_ne!(compute_vault_block_hash(&modified), base_hash, "organization_id affects hash");

        // Changing vault_id should change hash
        let mut modified = base_entry.clone();
        modified.vault = DomainVaultId::new(99);
        assert_ne!(compute_vault_block_hash(&modified), base_hash, "vault_id affects hash");

        // Changing tx_merkle_root should change hash
        let mut modified = base_entry.clone();
        modified.tx_merkle_root = [99u8; 32];
        assert_ne!(compute_vault_block_hash(&modified), base_hash, "tx_merkle_root affects hash");
    }
}
