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
    error::classify_raft_error,
    event_writer::HandlerPhaseEmitter,
    log_storage::{AppliedStateAccessor, VaultHealthStatus},
    logging::{OperationType, RequestContext, Sampler},
    metrics, trace_context,
    types::{LedgerRequest, LedgerResponse, RaftPayload, SystemRequest},
};
use inferadb_ledger_state::{BlockArchive, StateLayer, system::SystemOrganizationService};
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{
    ALL_REGIONS, OrganizationId as DomainOrganizationId,
    OrganizationSlug as DomainOrganizationSlug, VaultEntry, VaultId as DomainVaultId, VaultSlug,
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
    #[builder(default)]
    backup_manager: Option<Arc<inferadb_ledger_raft::backup::BackupManager>>,
    /// Snapshot manager for reading Raft snapshots during backup.
    #[builder(default)]
    snapshot_manager: Option<Arc<inferadb_ledger_state::SnapshotManager>>,
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

    /// Attaches the backup manager and snapshot manager for backup/restore operations.
    #[must_use]
    pub fn with_backup(
        mut self,
        backup_manager: Arc<inferadb_ledger_raft::backup::BackupManager>,
        snapshot_manager: Arc<inferadb_ledger_state::SnapshotManager>,
    ) -> Self {
        self.backup_manager = Some(backup_manager);
        self.snapshot_manager = Some(snapshot_manager);
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

    /// Creates a `RequestContext` for an admin operation, filling common fields
    /// from gRPC metadata and trace context.
    fn make_request_context(
        &self,
        method: &'static str,
        grpc_metadata: &tonic::metadata::MetadataMap,
        trace_ctx: &trace_context::TraceContext,
    ) -> RequestContext {
        let mut ctx = RequestContext::new("AdminService", method);
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(grpc_metadata);
        ctx.set_admin_action(method);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );
        ctx
    }

    /// Records a handler-phase event (best-effort).
    fn record_handler_event(&self, entry: inferadb_ledger_types::events::EventEntry) {
        if let Some(ref handle) = self.event_handle {
            handle.record_handler_event(entry);
        }
    }

    /// Proposes a `LedgerRequest` through Raft with deadline handling.
    ///
    /// Handles timeout computation, Raft proposal submission, and error
    /// classification (leadership errors → UNAVAILABLE, others → INTERNAL).
    async fn propose_raft_request(
        &self,
        request: LedgerRequest,
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
                Err(classify_raft_error(&source.to_string()))
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
        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context("create_snapshot", request.metadata(), &trace_ctx);
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
        if let Some(node_id) = self.node_id {
            self.record_handler_event(
                HandlerPhaseEmitter::for_system(EventAction::SnapshotCreated, node_id)
                    .principal("system")
                    .detail("height", &block_height.to_string())
                    .trace_id(&trace_ctx.trace_id)
                    .outcome(EventOutcomeType::Success)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
            );
        }

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
        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let mut issues = Vec::new();

        let mut ctx = self.make_request_context("check_integrity", request.metadata(), &trace_ctx);
        let req = request.into_inner();

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_slug_val =
            req.organization.as_ref().map(|n| DomainOrganizationSlug::new(n.slug));
        let organization_id = slug_resolver.extract_and_resolve_optional(&req.organization)?;
        let vault_id = slug_resolver.extract_and_resolve_vault_optional(&req.vault)?;

        if let Some(slug_val) = organization_slug_val {
            ctx.set_organization(slug_val.value());
        }
        if let Some(ref v) = req.vault {
            ctx.set_vault(v.slug);
        }

        // Get all vault heights to check
        let vault_heights: Vec<(DomainOrganizationId, DomainVaultId, u64)> =
            if let (Some(org), Some(v)) = (organization_id, vault_id) {
                // Specific vault
                let height = self.applied_state.vault_height(org, v);
                if height > 0 { vec![(org, v, height)] } else { vec![] }
            } else {
                // All vaults
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
        if let (Some(org_id), Some(node_id)) = (organization_id, self.node_id) {
            let outcome = if issues.is_empty() {
                EventOutcomeType::Success
            } else {
                EventOutcomeType::Failed {
                    code: "integrity_issues".to_string(),
                    detail: format!("{} issues found", issues.len()),
                }
            };
            let mut emitter =
                inferadb_ledger_raft::event_writer::HandlerPhaseEmitter::for_organization(
                    EventAction::IntegrityChecked,
                    org_id,
                    organization_slug_val,
                    node_id,
                )
                .detail("issues_found", &issues.len().to_string())
                .detail("full_check", &req.full_check.to_string())
                .trace_id(&trace_ctx.trace_id)
                .outcome(outcome);
            if let Some(ref v) = req.vault {
                emitter = emitter.vault(VaultSlug::new(v.slug));
            }
            self.record_handler_event(
                emitter
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
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
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context("join_cluster", request.metadata(), &trace_ctx);
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

        // Check existing membership via the RaftManager's openraft handle
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
        // commits would stall.
        if let Some(ref transport) = self.consensus_transport
            && let Ok(endpoint) =
                tonic::transport::Channel::from_shared(format!("http://{}", req.address))
        {
            transport.set_peer(req.node_id, endpoint.connect_lazy());
        }
        // Also update the peer address map for forwarding.
        if let Some(ref peers) = self.peer_addresses {
            peers.insert(req.node_id, req.address.clone());
        }

        // Replicate the peer address to all nodes via GLOBAL Raft so that
        // data region leaders on other nodes can reach the new peer.
        // Fire-and-forget — we don't need to wait for this to commit before
        // proceeding with the membership change. The address will replicate
        // through normal Raft flow.
        let register_handle = self.handle.clone();
        let register_node_id = req.node_id;
        let register_address = req.address.clone();
        tokio::spawn(async move {
            let register_request = inferadb_ledger_raft::types::LedgerRequest::System(
                inferadb_ledger_raft::types::SystemRequest::RegisterPeerAddress {
                    node_id: register_node_id,
                    address: register_address,
                },
            );
            let _ = register_handle
                .propose_and_wait(
                    inferadb_ledger_raft::types::RaftPayload::system(register_request),
                    std::time::Duration::from_secs(5),
                )
                .await;
        });

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

                    // After the node is a GLOBAL voter, add it to all existing
                    // data regions where this node is the data region leader.
                    if let Some(ref manager) = self.raft_manager {
                        add_node_to_data_regions(manager, req.node_id, &req.address).await;
                    }

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
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context("leave_cluster", request.metadata(), &trace_ctx);
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
        let set_status = LedgerRequest::System(SystemRequest::SetNodeStatus {
            node_id: req.node_id,
            status: NodeStatus::Decommissioning,
        });
        match self
            .handle
            .propose_and_wait(RaftPayload::system(set_status), std::time::Duration::from_secs(5))
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

        // Synchronously remove the departing node from data regions BEFORE
        // removing from GLOBAL. After GLOBAL removal, the node stops receiving
        // Raft entries and can't process DR changes. The DR scheduler handles
        // normal async operations; this synchronous path ensures correctness
        // when nodes are killed immediately after leave_cluster returns.
        if let Some(ref manager) = self.raft_manager {
            let target = inferadb_ledger_consensus::types::NodeId(req.node_id);
            let local_node_id = manager.config().node_id;

            for region in manager.list_regions() {
                if region == inferadb_ledger_types::Region::GLOBAL {
                    continue;
                }
                let Ok(group) = manager.get_region_group(region) else { continue };
                if !group.handle().is_leader() {
                    continue;
                }
                let state = group.handle().shard_state();
                if !state.voters.contains(&target) && !state.learners.contains(&target) {
                    continue;
                }

                // Self-removal: transfer DR leadership to a surviving voter first.
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
                    continue; // New DR leader handles removal via scheduler.
                }

                // Other-removal: remove directly with retry.
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
                            break;
                        },
                        Ok(Err(e)) if e.to_string().contains("already undergoing") => {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                200 * u64::from(attempt + 1),
                            ))
                            .await;
                        },
                        Ok(Err(e)) if e.to_string().contains("no-op") => break,
                        _ => break,
                    }
                }
            }
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
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context("get_cluster_info", request.metadata(), &trace_ctx);
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
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context("get_node_info", request.metadata(), &trace_ctx);
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
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context("recover_vault", request.metadata(), &trace_ctx);
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
            let health_request = LedgerRequest::UpdateVaultHealth {
                organization: organization_id,
                vault: vault_id,
                healthy: false,
                expected_root: None, // Already diverged during recovery
                computed_root: Some(final_state_root),
                diverged_at_height: Some(expected_height),
                recovery_attempt: None,
                recovery_started_at: None,
            };

            if let Err(e) = self.handle.propose(RaftPayload::system(health_request)).await {
                tracing::error!("Failed to update vault health via Raft: {}", e);
                // Continue with response - the local state will be inconsistent but
                // the next recovery attempt can retry
            }
            ctx.end_raft_timer();

            ctx.set_block_height(expected_height);
            ctx.set_error("DivergenceReproduced", "Recovery reproduced divergence");
            // Emit VaultRecovered handler-phase event (failed recovery)
            if let Some(node_id) = self.node_id {
                self.record_handler_event(
                    inferadb_ledger_raft::event_writer::HandlerPhaseEmitter::for_organization(
                        EventAction::VaultRecovered,
                        organization_id,
                        Some(DomainOrganizationSlug::new(organization_slug_val)),
                        node_id,
                    )
                    .vault(VaultSlug::new(vault_val))
                    .detail("recovery_method", "replay")
                    .detail("final_height", &expected_height.to_string())
                    .trace_id(&trace_ctx.trace_id)
                    .outcome(EventOutcomeType::Failed {
                        code: "divergence_reproduced".to_string(),
                        detail: "Recovery reproduced divergence".to_string(),
                    })
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
                );
            }
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
            let health_request = LedgerRequest::UpdateVaultHealth {
                organization: organization_id,
                vault: vault_id,
                healthy: true,
                expected_root: None,
                computed_root: None,
                diverged_at_height: None,
                recovery_attempt: None,
                recovery_started_at: None,
            };

            if let Err(e) = self.handle.propose(RaftPayload::system(health_request)).await {
                tracing::error!("Failed to update vault health via Raft: {}", e);
                // The vault was successfully recovered locally - log error but return success
            }
            ctx.end_raft_timer();

            ctx.set_block_height(expected_height);
            ctx.set_success();
            // Emit VaultRecovered handler-phase event (successful recovery)
            if let Some(node_id) = self.node_id {
                self.record_handler_event(
                    inferadb_ledger_raft::event_writer::HandlerPhaseEmitter::for_organization(
                        EventAction::VaultRecovered,
                        organization_id,
                        Some(DomainOrganizationSlug::new(organization_slug_val)),
                        node_id,
                    )
                    .vault(VaultSlug::new(vault_val))
                    .detail("recovery_method", "replay")
                    .detail("final_height", &expected_height.to_string())
                    .trace_id(&trace_ctx.trace_id)
                    .outcome(EventOutcomeType::Success)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
                );
            }
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
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx =
            self.make_request_context("simulate_divergence", request.metadata(), &trace_ctx);
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

        // Update vault health to Diverged via Raft
        let health_request = LedgerRequest::UpdateVaultHealth {
            organization: organization_id,
            vault: vault_id,
            healthy: false,
            expected_root: Some(expected_root),
            computed_root: Some(computed_root),
            diverged_at_height: Some(at_height),
            recovery_attempt: None,
            recovery_started_at: None,
        };

        // Write to BOTH the GLOBAL and DATA REGION Raft groups.
        // - GLOBAL: HealthService reads vault health from GLOBAL applied state
        // - DATA REGION: WriteService's apply handler checks vault health at write time
        ctx.start_raft_timer();

        // GLOBAL write (for HealthService)
        if let Err(e) = self.handle.propose(RaftPayload::system(health_request.clone())).await {
            ctx.end_raft_timer();
            ctx.set_error("RaftError", &e.to_string());
            return Err(error_classify::storage_error(&e));
        }

        // DATA REGION write (for WriteService apply-time checks)
        if let Some(ref manager) = self.raft_manager {
            let sys_svc =
                inferadb_ledger_state::system::SystemOrganizationService::new(self.state.clone());
            if let Ok(Some(region)) = sys_svc.get_region_for_organization(organization_id)
                && let Ok(rg) = manager.get_region_group(region)
            {
                let _ = rg.handle().propose(RaftPayload::system(health_request)).await;
            }
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
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context("force_gc", request.metadata(), &trace_ctx);
        let req = request.into_inner();

        // Check if this node is the leader
        ctx.set_raft_term(self.handle.current_term());
        ctx.set_is_leader(self.handle.is_leader());

        if !self.handle.is_leader() {
            ctx.set_error("NotLeader", "Only the leader can run garbage collection");
            let shard_state = self.handle.shard_state();
            let term = self.handle.current_term();
            let leader_id = shard_state.leader.map(|n| n.0);
            let leader_endpoint =
                leader_id.and_then(|id| self.peer_addresses.as_ref().and_then(|m| m.get(id)));
            return Err(super::metadata::status_with_not_leader_hint(
                "Only the leader can run garbage collection",
                leader_id,
                leader_endpoint.as_deref(),
                Some(term),
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
                // Single vault
                let height = self.applied_state.vault_height(org_id, v_id);
                vec![((org_id, v_id), height)]
            } else {
                // All vaults
                let mut heights = Vec::new();
                self.applied_state
                    .for_each_vault_height(|org, vault, h| heights.push(((org, vault), h)));
                heights
            };

        ctx.start_raft_timer();
        for ((organization_id, vault_id), _height) in vault_heights {
            vaults_scanned += 1;

            // Resolve the vault's region to read entities from the correct state layer.
            // Entity data lives in REGIONAL state, not GLOBAL.
            let (regional_state, regional_handle) = if let Some(ref manager) = self.raft_manager {
                let sys_svc = inferadb_ledger_state::system::SystemOrganizationService::new(
                    self.state.clone(),
                );
                let region = sys_svc
                    .get_region_for_organization(organization_id)
                    .ok()
                    .flatten()
                    .unwrap_or(inferadb_ledger_types::Region::GLOBAL);
                if let Ok(rg) = manager.get_region_group(region) {
                    (rg.state().clone(), Some(rg.handle().clone()))
                } else {
                    (self.state.clone(), None)
                }
            } else {
                // Single-Raft mode: GLOBAL state has everything
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

            let gc_request = LedgerRequest::Write {
                organization: organization_id,
                vault: vault_id,
                transactions: vec![transaction],
                idempotency_key: [0; 16],
                request_hash: 0,
            };

            // Propose to REGIONAL handle (where entity data lives), or fall back to GLOBAL
            let handle_target = regional_handle.as_ref().unwrap_or(&self.handle);
            match handle_target.propose(RaftPayload::system(gc_request)).await {
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
        if let Some(node_id) = self.node_id {
            self.record_handler_event(
                HandlerPhaseEmitter::for_system(EventAction::ConfigurationChanged, node_id)
                    .principal("admin")
                    .detail("changed_fields", &changed.join(", "))
                    .outcome(EventOutcomeType::Success)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
            );
        }

        let updated = runtime_config.load();
        let updated_json = serde_json::to_string_pretty(&*updated).unwrap_or_default();

        Ok(Response::new(UpdateConfigResponse {
            applied: true,
            message: format!("Updated: {}", changed.join(", ")),
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

    /// Creates a point-in-time backup by snapshotting the current Raft state.
    async fn create_backup(
        &self,
        request: Request<CreateBackupRequest>,
    ) -> Result<Response<CreateBackupResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context("create_backup", request.metadata(), &trace_ctx);
        let req = request.into_inner();

        let backup_manager = self
            .backup_manager
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Backup is not configured on this node"))?;

        let tag = req.tag.unwrap_or_default();

        let meta = if let Some(base_backup_id) = req.base_backup_id {
            // Incremental backup: capture only pages changed since the base backup.
            let base_meta = backup_manager.get_metadata(&base_backup_id).map_err(|e| {
                ctx.set_error("BaseBackupNotFound", &e.to_string());
                Status::not_found(format!("Base backup not found: {e}"))
            })?;

            let region_height = self.applied_state.region_height();
            let db = self.state.database();

            let meta = backup_manager
                .create_incremental_backup(
                    db.as_ref(),
                    &base_backup_id,
                    base_meta.region,
                    region_height,
                    &tag,
                )
                .map_err(|e| {
                    ctx.set_error("BackupError", &e.to_string());
                    error_classify::storage_error(&e)
                })?;

            // Clear dirty bitmap after successful incremental backup
            db.clear_dirty_bitmap();

            meta
        } else {
            // Full snapshot-based backup: build a state snapshot directly from
            // the current StateLayer + AppliedState rather than going through
            // openraft's snapshot mechanism (which produces an in-memory Raft
            // snapshot, not a file-based state::Snapshot).
            use std::collections::HashMap;

            use inferadb_ledger_state::{
                NUM_BUCKETS, Snapshot, SnapshotChainParams, SnapshotStateData, VaultSnapshotMeta,
            };
            use inferadb_ledger_types::EMPTY_HASH;

            let region_height = self.applied_state.region_height();
            let all_vaults = self.applied_state.all_vaults();

            let mut vault_states = Vec::new();
            let mut vault_entities = HashMap::new();

            for (org_id, vault_id) in all_vaults.keys() {
                let height = self.applied_state.vault_height(*org_id, *vault_id);

                let bucket_roots =
                    self.state.get_bucket_roots(*vault_id).unwrap_or([EMPTY_HASH; NUM_BUCKETS]);

                let entities = self
                    .state
                    .list_entities(*vault_id, None, None, usize::MAX)
                    .map_err(|e| error_classify::storage_error(&e))?;

                let state_root = self
                    .state
                    .compute_state_root(*vault_id)
                    .map_err(|e| error_classify::storage_error(&e))?;

                vault_states.push(VaultSnapshotMeta::new(
                    *vault_id,
                    height,
                    state_root,
                    bucket_roots,
                    entities.len() as u64,
                ));

                vault_entities.insert(*vault_id, entities);
            }

            let state_data = SnapshotStateData { vault_entities };
            let snapshot = Snapshot::new(
                inferadb_ledger_types::Region::GLOBAL,
                region_height,
                vault_states,
                state_data,
                SnapshotChainParams::default(),
            )
            .map_err(|e: inferadb_ledger_state::SnapshotError| {
                ctx.set_error("SnapshotError", &e.to_string());
                error_classify::storage_error(&e)
            })?;

            backup_manager.create_backup(&snapshot, &tag).map_err(|e| {
                ctx.set_error("BackupError", &e.to_string());
                error_classify::storage_error(&e)
            })?
        };

        ctx.set_block_height(meta.region_height);
        ctx.set_success();

        inferadb_ledger_raft::backup::record_backup_created(meta.region_height, meta.size_bytes);

        // Emit BackupCreated handler-phase event
        if let Some(node_id) = self.node_id {
            self.record_handler_event(
                HandlerPhaseEmitter::for_system(EventAction::BackupCreated, node_id)
                    .principal("system")
                    .detail("backup_id", &meta.backup_id)
                    .detail("tag", &tag)
                    .trace_id(&trace_ctx.trace_id)
                    .outcome(EventOutcomeType::Success)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
            );
        }

        Ok(Response::new(CreateBackupResponse {
            backup_id: meta.backup_id,
            region_height: meta.region_height,
            backup_path: meta.backup_path,
            size_bytes: meta.size_bytes,
            checksum: Some(Hash { value: meta.checksum.to_vec() }),
        }))
    }

    /// Lists available backups, optionally filtered by organization slug.
    async fn list_backups(
        &self,
        request: Request<ListBackupsRequest>,
    ) -> Result<Response<ListBackupsResponse>, Status> {
        let req = request.into_inner();

        let backup_manager = self
            .backup_manager
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Backup is not configured on this node"))?;

        let backups = backup_manager
            .list_backups(req.limit as usize)
            .map_err(|e| error_classify::storage_error(&e))?;

        let backup_infos: Vec<BackupInfo> = backups
            .into_iter()
            .map(|meta| {
                let created_at = prost_types::Timestamp {
                    seconds: meta.created_at.timestamp(),
                    nanos: meta.created_at.timestamp_subsec_nanos() as i32,
                };

                let backup_type = match meta.backup_type {
                    inferadb_ledger_raft::backup::BackupType::Full => 1,
                    inferadb_ledger_raft::backup::BackupType::Incremental => 2,
                };

                BackupInfo {
                    backup_id: meta.backup_id,
                    region_height: meta.region_height,
                    backup_path: meta.backup_path,
                    size_bytes: meta.size_bytes,
                    created_at: Some(created_at),
                    checksum: Some(Hash { value: meta.checksum.to_vec() }),
                    chain_commitment_hash: Some(Hash {
                        value: meta.chain_commitment_hash.to_vec(),
                    }),
                    schema_version: meta.schema_version,
                    tag: meta.tag,
                    backup_type,
                    base_backup_id: meta.base_backup_id,
                    page_count: meta.page_count,
                }
            })
            .collect();

        Ok(Response::new(ListBackupsResponse { backups: backup_infos }))
    }

    /// Restores state from a backup, requiring explicit confirmation.
    async fn restore_backup(
        &self,
        request: Request<RestoreBackupRequest>,
    ) -> Result<Response<RestoreBackupResponse>, Status> {
        // Reject if node is draining
        super::helpers::check_not_draining(self.health_state.as_ref())?;
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context("restore_backup", request.metadata(), &trace_ctx);
        let req = request.into_inner();

        // Safety gate: require explicit confirmation
        if !req.confirm {
            return Err(Status::failed_precondition(
                "Restore requires confirm=true. This operation will replace current region state with the backup.",
            ));
        }

        let backup_manager = self
            .backup_manager
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Backup is not configured on this node"))?;

        // Verify the backup exists and is valid
        let meta = backup_manager.get_metadata(&req.backup_id).map_err(|e| {
            ctx.set_error("BackupNotFound", &e.to_string());
            Status::not_found(format!("Backup not found: {e}"))
        })?;

        let (restored_height, message) = if meta.page_count.is_some() {
            // Page-level backup (full page or incremental): resolve chain and restore pages
            let chain = backup_manager.resolve_backup_chain(&req.backup_id).map_err(|e| {
                ctx.set_error("ChainResolutionError", &e.to_string());
                error_classify::storage_error(&e)
            })?;

            let db = self.state.database();
            let height = backup_manager.restore_page_chain(&chain, db.as_ref()).map_err(|e| {
                ctx.set_error("RestoreError", &e.to_string());
                error_classify::storage_error(&e)
            })?;

            let msg = format!(
                "Page backup {} (chain length {}, height {}) restored. Restart the node to apply.",
                meta.backup_id,
                chain.len(),
                height
            );
            (height, msg)
        } else {
            // Snapshot-based backup (existing behavior)
            let snapshot = backup_manager.load_backup(&req.backup_id).map_err(|e| {
                ctx.set_error("BackupLoadError", &e.to_string());
                error_classify::storage_error(&e)
            })?;

            // Verify schema version compatibility
            let current_schema_version = 2_u32; // SNAPSHOT_VERSION from state crate
            if snapshot.header.version != current_schema_version {
                ctx.set_error(
                    "SchemaVersionMismatch",
                    &format!(
                        "backup version {} != server version {}",
                        snapshot.header.version, current_schema_version
                    ),
                );
                return Err(Status::failed_precondition(format!(
                    "Schema version mismatch: backup has version {}, server expects version {}. \
                     Cannot restore from incompatible backup.",
                    snapshot.header.version, current_schema_version,
                )));
            }

            let snapshot_manager = self.snapshot_manager.as_ref().ok_or_else(|| {
                Status::failed_precondition("Snapshot manager is not available on this node")
            })?;

            snapshot_manager.save(&snapshot).map_err(|e| {
                ctx.set_error("RestoreError", &e.to_string());
                error_classify::storage_error(&e)
            })?;

            let height = snapshot.header.region_height;
            let msg = format!(
                "Backup {} (height {}) restored as snapshot. Restart the node to apply.",
                meta.backup_id, height
            );
            (height, msg)
        };

        ctx.set_block_height(restored_height);
        ctx.set_success();

        // Emit BackupRestored handler-phase event
        if let Some(node_id) = self.node_id {
            self.record_handler_event(
                HandlerPhaseEmitter::for_system(EventAction::BackupRestored, node_id)
                    .principal("system")
                    .detail("backup_id", &meta.backup_id)
                    .trace_id(&trace_ctx.trace_id)
                    .outcome(EventOutcomeType::Success)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
            );
        }

        Ok(Response::new(RestoreBackupResponse { success: true, message, restored_height }))
    }

    /// Transfers Raft leadership to a target node, or the best candidate if unspecified.
    async fn transfer_leadership(
        &self,
        request: Request<TransferLeadershipRequest>,
    ) -> Result<Response<TransferLeadershipResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx =
            self.make_request_context("transfer_leadership", request.metadata(), &trace_ctx);
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

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.make_request_context("rotate_blinding_key", &grpc_metadata, &trace_ctx);

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
        let ledger_request = LedgerRequest::System(SystemRequest::SetBlindingKeyVersion {
            version: req.new_key_version,
        });
        let response = self.propose_raft_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::Empty => {
                // Record audit event
                if let Some(node_id) = self.node_id {
                    self.record_handler_event(
                        HandlerPhaseEmitter::for_system(EventAction::ConfigurationChanged, node_id)
                            .principal("system")
                            .detail("resource", "blinding_key")
                            .detail("new_version", &req.new_key_version.to_string())
                            .detail("previous_version", &current_version.to_string())
                            .trace_id(&trace_ctx.trace_id)
                            .outcome(EventOutcomeType::Success)
                            .build(
                                self.event_handle
                                    .as_ref()
                                    .map_or(90, |h| h.config().default_ttl_days),
                            ),
                    );
                }

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
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context(
            "get_blinding_key_rehash_status",
            request.metadata(),
            &trace_ctx,
        );
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

        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx =
            self.make_request_context("rotate_region_key", request.metadata(), &trace_ctx);
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
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx =
            self.make_request_context("get_rewrap_status", request.metadata(), &trace_ctx);
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
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx =
            self.make_request_context("migrate_existing_users", &grpc_metadata, &trace_ctx);

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
        let ledger_request = LedgerRequest::System(SystemRequest::MigrateExistingUsers { entries });
        let response = self.propose_raft_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::UsersMigrated { users, migrated, skipped, errors } => {
                // Record audit event.
                if let Some(node_id) = self.node_id {
                    self.record_handler_event(
                        HandlerPhaseEmitter::for_system(EventAction::UsersMigrated, node_id)
                            .principal("admin")
                            .detail("users", &users.to_string())
                            .detail("migrated", &migrated.to_string())
                            .detail("skipped", &(skipped + pre_skipped).to_string())
                            .detail("errors", &errors.to_string())
                            .detail("default_region", default_region.as_str())
                            .trace_id(&trace_ctx.trace_id)
                            .outcome(EventOutcomeType::Success)
                            .build(
                                self.event_handle
                                    .as_ref()
                                    .map_or(90, |h| h.config().default_ttl_days),
                            ),
                    );
                }

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
        let trace_ctx = trace_context::extract_or_generate(request.metadata());

        let mut ctx = self.make_request_context("provision_region", request.metadata(), &trace_ctx);
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

        let node_id = self.node_id.ok_or_else(|| {
            Status::failed_precondition("Node ID not configured for region provisioning")
        })?;
        let addr = self.advertise_addr.clone();
        let region_config =
            inferadb_ledger_raft::raft_manager::RegionConfig::data(region, vec![(node_id, addr)]);
        let (_group, created) = manager.ensure_data_region(region_config).await.map_err(|e| {
            ctx.set_error("Internal", &format!("{e}"));
            error_classify::raft_error(&e)
        })?;

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

/// Adds a newly-joined node to all existing data region Raft groups.
///
/// After a node joins the GLOBAL cluster, it must also join each data region
/// so that regional Raft replication includes the new node. This follows the
/// same `add_learner` → `promote_voter` pattern as the GLOBAL join.
///
/// Errors are logged but not propagated — data region membership is
/// best-effort during the join. The node will participate after the next
/// region creation if any regions fail to add it.
async fn add_node_to_data_regions(
    manager: &inferadb_ledger_raft::raft_manager::RaftManager,
    node_id: u64,
    address: &str,
) {
    let regions = manager.list_regions();
    for region in regions {
        if region == inferadb_ledger_types::Region::GLOBAL {
            continue;
        }
        let group = match manager.get_region_group(region) {
            Ok(g) => g,
            Err(_) => continue,
        };

        // Skip if this node is already a member.
        let state = group.handle().shard_state();
        let node = inferadb_ledger_consensus::types::NodeId(node_id);
        if state.voters.contains(&node) || state.learners.contains(&node) {
            continue;
        }

        // Wait for a data region leader to be elected (may be in progress).
        let mut leader = None;
        for _ in 0..50 {
            if let Some(lid) = group.handle().current_leader() {
                leader = Some(lid);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let Some(leader_id) = leader else {
            tracing::warn!(
                region = region.as_str(),
                node_id,
                "No data region leader elected — skipping join"
            );
            continue;
        };

        if group.handle().is_leader() {
            // We ARE the data region leader — add directly.
            if let Some(transport) = group.consensus_transport()
                && let Ok(endpoint) =
                    tonic::transport::Channel::from_shared(format!("http://{address}"))
            {
                transport.set_peer(node_id, endpoint.connect_lazy());
            }
            add_member_to_region(&group, node_id, region).await;
        } else {
            // Forward the join to the data region leader.
            let leader_addr = match manager.peer_addresses().get(leader_id) {
                Some(a) => a,
                None => {
                    tracing::warn!(
                        region = region.as_str(),
                        leader_id,
                        "No address for data region leader — skipping join"
                    );
                    continue;
                },
            };
            forward_data_region_join(&leader_addr, node_id, address).await;
        }
    }
}

/// Adds a node as learner then promotes to voter on a data region group.
async fn add_member_to_region(
    group: &inferadb_ledger_raft::raft_manager::RegionGroup,
    node_id: u64,
    region: inferadb_ledger_types::Region,
) {
    for attempt in 0..5u32 {
        match group.handle().add_learner(node_id, false).await {
            Ok(()) => break,
            Err(e) if e.to_string().contains("already undergoing") => {
                tokio::time::sleep(std::time::Duration::from_millis(100 * u64::from(attempt + 1)))
                    .await;
            },
            Err(e) => {
                tracing::warn!(
                    region = region.as_str(),
                    node_id,
                    error = %e,
                    "Failed to add node as learner to data region"
                );
                return;
            },
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for attempt in 0..5u32 {
        match group.handle().promote_voter(node_id).await {
            Ok(()) => {
                tracing::info!(region = region.as_str(), node_id, "Added node to data region");
                return;
            },
            Err(e) if e.to_string().contains("already undergoing") => {
                tokio::time::sleep(std::time::Duration::from_millis(100 * u64::from(attempt + 1)))
                    .await;
            },
            Err(e) => {
                tracing::warn!(
                    region = region.as_str(),
                    node_id,
                    error = %e,
                    "Failed to promote node to voter in data region"
                );
                return;
            },
        }
    }
}

/// Forwards a data region join request to the data region leader via JoinCluster RPC.
///
/// The JoinCluster RPC is reused — the leader handles it the same way as a
/// GLOBAL join but the data region membership change happens through
/// `add_node_to_data_regions` on the receiving leader.
async fn forward_data_region_join(leader_addr: &str, node_id: u64, address: &str) {
    let endpoint = format!("http://{leader_addr}");
    let channel = match tonic::transport::Channel::from_shared(endpoint) {
        Ok(ep) => ep.connect_timeout(std::time::Duration::from_secs(5)).connect_lazy(),
        Err(e) => {
            tracing::warn!(leader_addr, error = %e, "Invalid data region leader address");
            return;
        },
    };

    // Use JoinCluster on the leader — the leader's JoinCluster handler will
    // call add_node_to_data_regions which adds this node to data regions where
    // the leader IS the data region leader.
    let mut client =
        inferadb_ledger_proto::proto::admin_service_client::AdminServiceClient::new(channel);
    match client
        .join_cluster(inferadb_ledger_proto::proto::JoinClusterRequest {
            node_id,
            address: address.to_string(),
        })
        .await
    {
        Ok(resp) => {
            let inner = resp.into_inner();
            if inner.success {
                tracing::info!(node_id, "Data region join forwarded successfully");
            } else {
                tracing::info!(
                    node_id,
                    message = %inner.message,
                    "Data region join forwarded (already member or non-leader)"
                );
            }
        },
        Err(e) => {
            tracing::warn!(node_id, error = %e, "Failed to forward data region join");
        },
    }
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
        };

        let entry2 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 11, // Different height
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
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
        };

        let entry2 = VaultEntry {
            organization: DomainOrganizationId::new(1),
            vault: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [1u8; 32], // Different state root
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
