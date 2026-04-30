//! Wire-trait implementation for `AdminService`.
//!
//! Every handler operates on wire types directly — no proto round-trip.
//! Slug ↔ ID translation happens at the top of each handler via
//! [`SlugResolver`]; Raft proposals use the typed `SystemRequest` /
//! `OrganizationRequest` payload enums; responses are constructed inline as
//! wire structs from `inferadb_ledger_wire::services::admin`.
//!
//! Shared cross-handler helpers live in [`super::admin`] (vault info
//! projection, vault block hash computation, manifest re-shaping, cluster
//! ID persistence).

use std::{str::FromStr, sync::Arc, time::Duration};

use bytes::Bytes;
use inferadb_ledger_raft::types::{
    LedgerResponse, OrganizationRequest, RaftPayload, SystemRequest,
};
use inferadb_ledger_state::system::SystemOrganizationService;
use inferadb_ledger_types::{
    ALL_REGIONS, ZERO_HASH,
    events::{EventAction, EventOutcome as EventOutcomeType},
    hash_eq,
};
use inferadb_ledger_wire::{
    ErrorCode, RequestContext, WireError,
    services::{admin as w, shared as ws},
};

use super::{
    admin::{
        AdminService, backup_manifest_to_wire, compute_vault_block_hash, unix_secs_to_ns_opt,
        write_cluster_id_to_disk,
    },
    error_classify,
    slug_resolver::SlugResolver,
    wire_helpers::tonic_status_to_wire_error,
};

// ---------------------------------------------------------------------------
// Wire-trait implementation for `AdminService`.
// ---------------------------------------------------------------------------

impl inferadb_ledger_wire_services::AdminService for AdminService {
    async fn join_cluster(
        &self,
        request: w::JoinClusterRequest,
        _ctx: RequestContext,
    ) -> Result<w::JoinClusterResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut log_ctx = self.make_request_context_unified_wire("join_cluster", &_ctx);

        let node_id = self.handle.node_id();
        let current_leader = self.handle.current_leader();
        let current_term = self.handle.current_term();
        log_ctx.set_raft_term(current_term);
        log_ctx.set_is_leader(self.handle.is_leader());

        if !self.handle.is_leader() {
            log_ctx.set_error("NotLeader", "Not the leader, redirect to leader");
            let leader_id = current_leader.unwrap_or(0);
            return Ok(w::JoinClusterResponse {
                success: false,
                message: "Not the leader, redirect to leader".to_string(),
                leader_id,
                leader_address: self
                    .peer_addresses
                    .as_ref()
                    .and_then(|m| m.get(leader_id))
                    .unwrap_or_default(),
            });
        }

        let already_voter = self
            .raft_manager
            .as_ref()
            .and_then(|m| m.system_region().ok())
            .map(|s| {
                let state = s.handle().shard_state();
                state.voters.iter().any(|n| n.0 == request.node_id)
            })
            .unwrap_or(false);

        let my_address =
            self.peer_addresses.as_ref().and_then(|m| m.get(node_id)).unwrap_or_default();

        if already_voter {
            log_ctx.set_success();
            return Ok(w::JoinClusterResponse {
                success: true,
                message: "Node is already a voter in the cluster".to_string(),
                leader_id: node_id,
                leader_address: my_address.clone(),
            });
        }

        // Register the joining node's gRPC channel via NodeConnectionRegistry.
        if let Some(ref transport) = self.consensus_transport
            && let Err(e) = transport.set_peer_via_registry(request.node_id, &request.address).await
        {
            tracing::warn!(
                node_id = request.node_id,
                address = %request.address,
                error = %e,
                "Failed to register joining node via registry",
            );
        }
        if let Some(ref peers) = self.peer_addresses {
            peers.insert(request.node_id, request.address.clone());
        }

        // Replicate the peer address to all nodes via GLOBAL Raft.
        if let Err(e) = self
            .handle
            .propose_and_wait(
                RaftPayload::system(SystemRequest::RegisterPeerAddress {
                    node_id: request.node_id,
                    address: request.address.clone(),
                }),
                Duration::from_secs(5),
            )
            .await
        {
            tracing::warn!(
                node_id = request.node_id,
                error = %e,
                "RegisterPeerAddress failed — proceeding with join (DR scheduler will reconcile)"
            );
        }

        // Step 1: Add as learner
        log_ctx.start_raft_timer();
        let mut add_success = false;
        for attempt in 0..10 {
            match self.handle.add_learner(request.node_id, false).await {
                Ok(()) => {
                    add_success = true;
                    break;
                },
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("already undergoing a configuration change") {
                        tokio::time::sleep(Duration::from_millis(100 * (attempt + 1) as u64)).await;
                    } else {
                        log_ctx.end_raft_timer();
                        log_ctx.set_error("AddLearnerFailed", &err_str);
                        return Ok(w::JoinClusterResponse {
                            success: false,
                            message: format!("Failed to add learner: {}", err_str),
                            leader_id: node_id,
                            leader_address: my_address.clone(),
                        });
                    }
                },
            }
        }
        if !add_success {
            log_ctx.end_raft_timer();
            log_ctx.set_error("Timeout", "Cluster membership changes not completing");
            return Ok(w::JoinClusterResponse {
                success: false,
                message: "Timeout: cluster membership changes not completing".to_string(),
                leader_id: node_id,
                leader_address: my_address.clone(),
            });
        }

        // Step 2: Wait for the learner to appear in membership
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Step 3: Promote to voter
        for attempt in 0..10 {
            match self.handle.promote_voter(request.node_id).await {
                Ok(()) => {
                    log_ctx.end_raft_timer();
                    if let Some(ref liveness) = self.peer_liveness {
                        liveness.write().insert(request.node_id, std::time::Instant::now());
                    }
                    log_ctx.set_success();
                    return Ok(w::JoinClusterResponse {
                        success: true,
                        message: "Node joined cluster successfully".to_string(),
                        leader_id: node_id,
                        leader_address: my_address.clone(),
                    });
                },
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("already undergoing a configuration change") {
                        tokio::time::sleep(Duration::from_millis(100 * (attempt + 1) as u64)).await;
                    } else {
                        log_ctx.end_raft_timer();
                        log_ctx.set_error("PromotionFailed", &err_str);
                        return Ok(w::JoinClusterResponse {
                            success: false,
                            message: format!("Failed to promote to voter: {}", err_str),
                            leader_id: node_id,
                            leader_address: my_address.clone(),
                        });
                    }
                },
            }
        }

        log_ctx.end_raft_timer();
        log_ctx.set_error("Timeout", "Could not complete voter promotion");
        Ok(w::JoinClusterResponse {
            success: false,
            message: "Timeout: could not complete voter promotion".to_string(),
            leader_id: node_id,
            leader_address: my_address,
        })
    }

    async fn leave_cluster(
        &self,
        request: w::LeaveClusterRequest,
        _ctx: RequestContext,
    ) -> Result<w::LeaveClusterResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut log_ctx = self.make_request_context_unified_wire("leave_cluster", &_ctx);

        let current_term = self.handle.current_term();
        log_ctx.set_raft_term(current_term);
        log_ctx.set_is_leader(self.handle.is_leader());

        if !self.handle.is_leader() {
            log_ctx.set_error("NotLeader", "Cannot process leave request");
            return Ok(w::LeaveClusterResponse {
                success: false,
                message: "Not the leader, cannot process leave request".to_string(),
            });
        }

        let state = self.handle.shard_state();
        if state.voters.len() <= 1 {
            log_ctx.set_error("InvalidOperation", "Cannot remove the last voter");
            return Ok(w::LeaveClusterResponse {
                success: false,
                message: "Cannot remove the last voter from cluster".to_string(),
            });
        }

        log_ctx.start_raft_timer();

        // Mark the node as Decommissioning in GLOBAL state.
        match self
            .handle
            .propose_and_wait(
                RaftPayload::system(SystemRequest::SetNodeStatus {
                    node_id: request.node_id,
                    status: inferadb_ledger_raft::NodeStatus::Decommissioning,
                }),
                Duration::from_secs(5),
            )
            .await
        {
            Ok(_) => {
                tracing::info!(node_id = request.node_id, "Node marked as Decommissioning");
            },
            Err(e) => {
                log_ctx.set_error("RaftProposalFailed", &e.to_string());
                return Ok(w::LeaveClusterResponse {
                    success: false,
                    message: format!("Failed to mark node as Decommissioning: {e}"),
                });
            },
        }

        // Synchronously remove the departing node from ALL data regions BEFORE
        // removing from GLOBAL.
        if let Some(ref manager) = self.raft_manager {
            let target = inferadb_ledger_consensus::types::NodeId(request.node_id);
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
                    tracing::info!(
                        region = region.as_str(),
                        node_id = request.node_id,
                        dr_leader = ?state.leader,
                        "Not DR leader — DR checker on leader will handle removal"
                    );
                    continue;
                }

                if request.node_id == local_node_id {
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
                            Duration::from_secs(3),
                            group.handle().transfer_leader(transfer_target.0),
                        )
                        .await;
                        for _ in 0..30 {
                            if !group.handle().is_leader() {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                    continue;
                }

                for attempt in 0..10u32 {
                    match tokio::time::timeout(
                        Duration::from_secs(3),
                        group.handle().remove_node(request.node_id),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            tracing::info!(
                                region = region.as_str(),
                                node_id = request.node_id,
                                "Removed departing node from data region"
                            );
                            break;
                        },
                        Ok(Err(e)) if e.to_string().contains("already undergoing") => {
                            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt + 1)))
                                .await;
                        },
                        Ok(Err(e)) if e.to_string().contains("no-op") => {
                            break;
                        },
                        _ => break,
                    }
                }
            }

            manager.notify_dr_membership_change();
        }

        // Remove from GLOBAL.
        if let Err(e) = self.handle.remove_node(request.node_id).await {
            log_ctx.end_raft_timer();
            log_ctx.set_error("MembershipChangeFailed", &e.to_string());
            return Ok(w::LeaveClusterResponse {
                success: false,
                message: format!("Failed to remove node from GLOBAL: {e}"),
            });
        }

        // Wait for the GLOBAL membership change to commit and apply.
        let target_node = inferadb_ledger_consensus::types::NodeId(request.node_id);
        for _ in 0..50 {
            let state = self.handle.shard_state();
            if !state.voters.contains(&target_node) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        log_ctx.end_raft_timer();
        log_ctx.set_success();
        Ok(w::LeaveClusterResponse { success: true, message: "Node decommissioned.".to_string() })
    }

    async fn get_decommission_status(
        &self,
        request: w::GetDecommissionStatusRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetDecommissionStatusResponse, WireError> {
        let status_str = if let Some(ref manager) = self.raft_manager {
            if let Some(reader) = manager.system_state_reader() {
                match reader.node_status(request.node_id) {
                    inferadb_ledger_raft::NodeStatus::Active => "active",
                    inferadb_ledger_raft::NodeStatus::Decommissioning => "decommissioning",
                    inferadb_ledger_raft::NodeStatus::Dead => "dead",
                    inferadb_ledger_raft::NodeStatus::Removed => "removed",
                }
            } else {
                "unknown"
            }
        } else {
            "unknown"
        };

        let mut remaining = Vec::new();
        if let Some(ref manager) = self.raft_manager {
            let target = inferadb_ledger_consensus::types::NodeId(request.node_id);
            for region in manager.list_regions() {
                if region == inferadb_ledger_types::Region::GLOBAL {
                    continue;
                }
                if let Ok(group) = manager.get_region_group(region) {
                    let state = group.handle().shard_state();
                    if state.voters.contains(&target) {
                        remaining.push(w::DataRegionReplica {
                            region: region.to_string(),
                            role: "voter".to_string(),
                        });
                    } else if state.learners.contains(&target) {
                        remaining.push(w::DataRegionReplica {
                            region: region.to_string(),
                            role: "learner".to_string(),
                        });
                    }
                }
            }
        }

        let global_removed = {
            let state = self.handle.shard_state();
            let target = inferadb_ledger_consensus::types::NodeId(request.node_id);
            !state.voters.contains(&target) && !state.learners.contains(&target)
        };

        Ok(w::GetDecommissionStatusResponse {
            status: status_str.to_string(),
            remaining,
            global_removed,
        })
    }

    async fn check_peer_liveness(
        &self,
        request: w::CheckPeerLivenessRequest,
        _ctx: RequestContext,
    ) -> Result<w::CheckPeerLivenessResponse, WireError> {
        let (reachable, last_seen_ago_ms) = if let Some(ref liveness) = self.peer_liveness {
            let map = liveness.read();
            match map.get(&request.target_node_id) {
                Some(last_seen) => {
                    let ago = last_seen.elapsed();
                    (ago < Duration::from_secs(300), ago.as_millis() as u64)
                },
                None => (false, u64::MAX),
            }
        } else {
            (false, u64::MAX)
        };

        Ok(w::CheckPeerLivenessResponse { reachable, last_seen_ago_ms })
    }

    async fn get_cluster_info(
        &self,
        _request: w::GetClusterInfoRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetClusterInfoResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("get_cluster_info", &_ctx);

        let current_leader = self.handle.current_leader();
        let current_term = self.handle.current_term();

        log_ctx.set_raft_term(current_term);
        log_ctx.set_is_leader(self.handle.is_leader());

        let shard_state = self.handle.shard_state();
        let mut members: Vec<ws::ClusterMember> = shard_state
            .voters
            .iter()
            .map(|node_id| {
                let address =
                    self.peer_addresses.as_ref().and_then(|m| m.get(node_id.0)).unwrap_or_default();
                ws::ClusterMember {
                    node_id: node_id.0,
                    address,
                    role: ws::ClusterMemberRole::Voter,
                    is_leader: shard_state.leader == Some(*node_id),
                }
            })
            .collect();
        let learner_members: Vec<ws::ClusterMember> = shard_state
            .learners
            .iter()
            .map(|node_id| {
                let address =
                    self.peer_addresses.as_ref().and_then(|m| m.get(node_id.0)).unwrap_or_default();
                ws::ClusterMember {
                    node_id: node_id.0,
                    address,
                    role: ws::ClusterMemberRole::Learner,
                    is_leader: false,
                }
            })
            .collect();
        members.extend(learner_members);

        log_ctx.set_keys_count(members.len());
        log_ctx.set_success();

        Ok(w::GetClusterInfoResponse {
            members,
            leader_id: current_leader.unwrap_or(0),
            term: current_term,
        })
    }

    async fn get_node_info(
        &self,
        _request: w::GetNodeInfoRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetNodeInfoResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("get_node_info", &_ctx);

        let current_term = self.handle.current_term();
        let node_id = self.handle.node_id();

        log_ctx.set_raft_term(current_term);
        log_ctx.set_is_leader(self.handle.is_leader());

        let cluster_id = self
            .cluster_id
            .or_else(|| self.init_sender.as_ref().and_then(|s| *s.borrow()))
            .unwrap_or(0);

        let is_cluster_member = cluster_id != 0;

        log_ctx.set_success();

        Ok(w::GetNodeInfoResponse {
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
        })
    }

    async fn init_cluster(
        &self,
        _request: w::InitClusterRequest,
        _ctx: RequestContext,
    ) -> Result<w::InitClusterResponse, WireError> {
        if let Some(cid) = self.cluster_id {
            return Ok(w::InitClusterResponse {
                initialized: false,
                cluster_id: cid,
                already_initialized: true,
            });
        }

        if let Some(ref sender) = self.init_sender {
            if let Some(cid) = *sender.borrow() {
                return Ok(w::InitClusterResponse {
                    initialized: false,
                    cluster_id: cid,
                    already_initialized: true,
                });
            }
        } else {
            return Ok(w::InitClusterResponse {
                initialized: false,
                cluster_id: 0,
                already_initialized: true,
            });
        }

        let cluster_id = inferadb_ledger_types::snowflake::generate().map_err(|e| {
            WireError::new(ErrorCode::Internal, format!("failed to generate cluster_id: {e}"))
        })?;

        if let Some(ref dir) = self.init_data_dir {
            write_cluster_id_to_disk(dir, cluster_id).map_err(tonic_status_to_wire_error)?;
        }

        if let Some(ref sender) = self.init_sender {
            let _ = sender.send(Some(cluster_id));
        }

        tracing::info!(cluster_id, "Cluster initialized via InitCluster RPC");

        Ok(w::InitClusterResponse { initialized: true, cluster_id, already_initialized: false })
    }

    async fn transfer_leadership(
        &self,
        request: w::TransferLeadershipRequest,
        _ctx: RequestContext,
    ) -> Result<w::TransferLeadershipResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("transfer_leadership", &_ctx);

        let timeout_ms =
            if request.timeout_ms == 0 { 10_000u32 } else { request.timeout_ms.min(60_000) };
        let target = if request.target_node_id == 0 { None } else { Some(request.target_node_id) };

        let config = inferadb_ledger_raft::leader_transfer::LeaderTransferConfig::builder()
            .timeout(Duration::from_millis(u64::from(timeout_ms)))
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
                inferadb_ledger_raft::metrics::record_leader_transfer(true, latency);
                log_ctx.set_success();
                Ok(w::TransferLeadershipResponse {
                    success: true,
                    new_leader_id: new_leader,
                    message: String::new(),
                })
            },
            Err(e) => {
                inferadb_ledger_raft::metrics::record_leader_transfer(false, latency);
                log_ctx.set_error("LeaderTransferError", &e.to_string());
                let status = match &e {
                    inferadb_ledger_raft::leader_transfer::LeaderTransferError::NotLeader
                    | inferadb_ledger_raft::leader_transfer::LeaderTransferError::NoTarget
                    | inferadb_ledger_raft::leader_transfer::LeaderTransferError::TargetRejected { .. } => {
                        tonic::Status::failed_precondition(e.to_string())
                    },
                    inferadb_ledger_raft::leader_transfer::LeaderTransferError::TransferInProgress => {
                        tonic::Status::aborted(e.to_string())
                    },
                    inferadb_ledger_raft::leader_transfer::LeaderTransferError::ReplicationTimeout
                    | inferadb_ledger_raft::leader_transfer::LeaderTransferError::Timeout { .. } => {
                        tonic::Status::deadline_exceeded(e.to_string())
                    },
                };
                Err(tonic_status_to_wire_error(status))
            },
        }
    }

    async fn create_snapshot(
        &self,
        _request: w::CreateSnapshotRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateSnapshotResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("create_snapshot", &_ctx);

        log_ctx.start_raft_timer();
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
        log_ctx.end_raft_timer();

        let block_height = self.applied_state.region_height();
        log_ctx.set_block_height(block_height);
        log_ctx.set_success();

        log_ctx.record_event(
            EventAction::SnapshotCreated,
            EventOutcomeType::Success,
            &[("height", &block_height.to_string())],
        );

        Ok(w::CreateSnapshotResponse {
            block_height,
            state_root: None,
            snapshot_path: format!("/snapshots/snapshot_{}", chrono::Utc::now().timestamp()),
        })
    }

    async fn check_integrity(
        &self,
        request: w::CheckIntegrityRequest,
        _ctx: RequestContext,
    ) -> Result<w::CheckIntegrityResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("check_integrity", &_ctx);
        let mut issues = Vec::new();

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_id = slug_resolver.extract_and_resolve_optional(
            request
                .organization
                .as_ref()
                .map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
        )?;
        let vault_id = slug_resolver.extract_and_resolve_vault_optional(
            request.vault.as_ref().map(|s| inferadb_ledger_types::VaultSlug::new(s.value())),
        )?;

        if let Some(ref n) = request.organization {
            log_ctx.set_organization(n.value());
        }
        if let Some(ref v) = request.vault {
            log_ctx.set_vault(v.value());
        }

        let vault_heights: Vec<(
            inferadb_ledger_types::OrganizationId,
            inferadb_ledger_types::VaultId,
            u64,
        )> = if let (Some(org), Some(v)) = (organization_id, vault_id) {
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
            self.applied_state.for_each_vault_height(|org, vault, h| heights.push((org, vault, h)));
            heights
        };

        if vault_heights.is_empty() {
            log_ctx.set_success();
            return Ok(w::CheckIntegrityResponse { healthy: true, issues: vec![] });
        }

        if request.full_check {
            let archive = match &self.block_archive {
                Some(a) => a,
                None => {
                    log_ctx.set_error("Unavailable", "Block archive not configured");
                    return Err(WireError::new(
                        ErrorCode::FailedPrecondition,
                        "Block archive not configured for full integrity check",
                    ));
                },
            };

            for (org_id, v_id, expected_height) in &vault_heights {
                let (_temp_dir, temp_state) = match super::helpers::create_replay_context() {
                    Ok(ctx_pair) => ctx_pair,
                    Err(e) => {
                        issues.push(w::IntegrityIssue {
                            block_height: 0,
                            issue_type: "internal_error".to_string(),
                            description: e.message.clone(),
                        });
                        continue;
                    },
                };

                let mut last_vault_hash: Option<[u8; 32]> = None;

                for height in 1..=*expected_height {
                    let region_height = match archive.find_region_height(*org_id, *v_id, height) {
                        Ok(Some(h)) => h,
                        Ok(None) => {
                            issues.push(w::IntegrityIssue {
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
                            issues.push(w::IntegrityIssue {
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
                            issues.push(w::IntegrityIssue {
                                block_height: height,
                                issue_type: "read_error".to_string(),
                                description: format!("Block read failed: {:?}", e),
                            });
                            continue;
                        },
                    };

                    let vault_entry = region_block.vault_entries.iter().find(|e| {
                        e.organization == *org_id && e.vault == *v_id && e.vault_height == height
                    });

                    let entry = match vault_entry {
                        Some(e) => e,
                        None => {
                            issues.push(w::IntegrityIssue {
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

                    if let Some(expected_prev) = last_vault_hash
                        && !hash_eq(&entry.previous_vault_hash, &expected_prev)
                    {
                        issues.push(w::IntegrityIssue {
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

                    for tx in &entry.transactions {
                        if let Err(e) = temp_state.apply_operations(*v_id, &tx.operations, height) {
                            issues.push(w::IntegrityIssue {
                                block_height: height,
                                issue_type: "apply_error".to_string(),
                                description: format!("Transaction apply failed: {:?}", e),
                            });
                        }
                    }

                    match temp_state.compute_state_root(*v_id) {
                        Ok(computed_root) => {
                            if computed_root != entry.state_root {
                                issues.push(w::IntegrityIssue {
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
                            issues.push(w::IntegrityIssue {
                                block_height: height,
                                issue_type: "compute_error".to_string(),
                                description: format!("State root computation failed: {:?}", e),
                            });
                        },
                    }

                    last_vault_hash = Some(compute_vault_block_hash(entry));
                }

                let current_state = &*self.state;
                if let Ok(current_root) = current_state.compute_state_root(*v_id)
                    && let Ok(replayed_root) = temp_state.compute_state_root(*v_id)
                    && current_root != replayed_root
                {
                    issues.push(w::IntegrityIssue {
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
            let state = &*self.state;
            for (_org_id, v_id, height) in &vault_heights {
                match state.compute_state_root(*v_id) {
                    Ok(_root) => {},
                    Err(e) => {
                        issues.push(w::IntegrityIssue {
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

        if issues.is_empty() {
            log_ctx.set_success();
        } else {
            log_ctx.set_error("IntegrityIssues", &format!("{} issues found", issues.len()));
        }

        if organization_id.is_some() {
            let outcome = if issues.is_empty() {
                EventOutcomeType::Success
            } else {
                EventOutcomeType::Failed {
                    code: "integrity_issues".to_string(),
                    detail: format!("{} issues found", issues.len()),
                }
            };
            log_ctx.record_event(
                EventAction::IntegrityChecked,
                outcome,
                &[
                    ("issues_found", &issues.len().to_string()),
                    ("full_check", &request.full_check.to_string()),
                ],
            );
        }

        Ok(w::CheckIntegrityResponse { healthy: issues.is_empty(), issues })
    }

    async fn recover_vault(
        &self,
        request: w::RecoverVaultRequest,
        _ctx: RequestContext,
    ) -> Result<w::RecoverVaultResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut log_ctx = self.make_request_context_unified_wire("recover_vault", &_ctx);
        log_ctx.set_recovery_force(request.force);

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_slug_val = request.organization.as_ref().map_or(0, |n| n.value());
        let organization_id = slug_resolver
            .extract_and_resolve(
                request
                    .organization
                    .as_ref()
                    .map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let vault_id = slug_resolver
            .extract_and_resolve_vault(
                request.vault.as_ref().map(|s| inferadb_ledger_types::VaultSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let vault_val = request.vault.as_ref().map_or(0, |v| v.value());
        log_ctx.set_target(organization_slug_val, vault_val);

        let current_health = self.applied_state.vault_health(organization_id, vault_id);

        if !request.force {
            match &current_health {
                inferadb_ledger_raft::log_storage::VaultHealthStatus::Healthy => {
                    log_ctx.set_error("AlreadyHealthy", "Vault is already healthy");
                    return Ok(w::RecoverVaultResponse {
                        success: false,
                        message: "Vault is already healthy. Use force=true to recover anyway."
                            .to_string(),
                        health_status: ws::VaultHealthProto::Healthy,
                        final_height: self.applied_state.vault_height(organization_id, vault_id),
                        final_state_root: None,
                    });
                },
                inferadb_ledger_raft::log_storage::VaultHealthStatus::Diverged { .. }
                | inferadb_ledger_raft::log_storage::VaultHealthStatus::Recovering { .. } => {},
            }
        }

        let archive = match &self.block_archive {
            Some(a) => a,
            None => {
                log_ctx.set_error("Unavailable", "Block archive not configured");
                return Err(WireError::new(
                    ErrorCode::FailedPrecondition,
                    "Block archive not configured, cannot recover vault",
                ));
            },
        };

        let expected_height = self.applied_state.vault_height(organization_id, vault_id);
        if expected_height == 0 {
            log_ctx.set_error("NoBlocks", "Vault has no blocks to recover");
            return Ok(w::RecoverVaultResponse {
                success: false,
                message: "Vault has no blocks to recover".to_string(),
                health_status: ws::VaultHealthProto::Healthy,
                final_height: 0,
                final_state_root: None,
            });
        }

        // Step 1: Clear vault state
        {
            let state = &*self.state;
            if let Err(e) = state.clear_vault(vault_id) {
                log_ctx.set_error("ClearFailed", &format!("{:?}", e));
                return Ok(w::RecoverVaultResponse {
                    success: false,
                    message: format!("Failed to clear vault state: {:?}", e),
                    health_status: ws::VaultHealthProto::Diverged,
                    final_height: 0,
                    final_state_root: None,
                });
            }
        }

        log_ctx.start_storage_timer();
        let mut last_vault_hash: Option<[u8; 32]> = None;
        let mut divergence_detected = false;
        let mut final_state_root = ZERO_HASH;

        for height in 1..=expected_height {
            let region_height = match archive.find_region_height(organization_id, vault_id, height)
            {
                Ok(Some(h)) => h,
                Ok(None) => {
                    log_ctx.end_storage_timer();
                    log_ctx.set_error(
                        "MissingBlock",
                        &format!("Block not found at height {}", height),
                    );
                    return Ok(w::RecoverVaultResponse {
                        success: false,
                        message: format!(
                            "Block not found in archive: org={}, vault={}, height={}",
                            organization_id, vault_id, height
                        ),
                        health_status: ws::VaultHealthProto::Diverged,
                        final_height: height - 1,
                        final_state_root: Some(ws::Hash {
                            value: Bytes::copy_from_slice(&final_state_root),
                        }),
                    });
                },
                Err(e) => {
                    log_ctx.end_storage_timer();
                    log_ctx.set_error("IndexLookupFailed", &format!("{:?}", e));
                    return Ok(w::RecoverVaultResponse {
                        success: false,
                        message: format!("Index lookup failed at height {}: {:?}", height, e),
                        health_status: ws::VaultHealthProto::Diverged,
                        final_height: height - 1,
                        final_state_root: Some(ws::Hash {
                            value: Bytes::copy_from_slice(&final_state_root),
                        }),
                    });
                },
            };

            let region_block = match archive.read_block(region_height) {
                Ok(b) => b,
                Err(e) => {
                    log_ctx.end_storage_timer();
                    log_ctx.set_error("BlockReadFailed", &format!("{:?}", e));
                    return Ok(w::RecoverVaultResponse {
                        success: false,
                        message: format!("Block read failed at height {}: {:?}", height, e),
                        health_status: ws::VaultHealthProto::Diverged,
                        final_height: height - 1,
                        final_state_root: Some(ws::Hash {
                            value: Bytes::copy_from_slice(&final_state_root),
                        }),
                    });
                },
            };

            let entry = match region_block.vault_entries.iter().find(|e| {
                e.organization == organization_id && e.vault == vault_id && e.vault_height == height
            }) {
                Some(e) => e,
                None => {
                    log_ctx.end_storage_timer();
                    log_ctx.set_error("MissingEntry", "Vault entry not found in region block");
                    return Ok(w::RecoverVaultResponse {
                        success: false,
                        message: format!(
                            "Vault entry not found in region block at height {}",
                            height
                        ),
                        health_status: ws::VaultHealthProto::Diverged,
                        final_height: height - 1,
                        final_state_root: Some(ws::Hash {
                            value: Bytes::copy_from_slice(&final_state_root),
                        }),
                    });
                },
            };

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

            {
                let state = &*self.state;
                for tx in &entry.transactions {
                    if let Err(e) = state.apply_operations(vault_id, &tx.operations, height) {
                        log_ctx.end_storage_timer();
                        log_ctx.set_error("ApplyFailed", &format!("{:?}", e));
                        return Ok(w::RecoverVaultResponse {
                            success: false,
                            message: format!(
                                "Transaction apply failed at height {}: {:?}",
                                height, e
                            ),
                            health_status: ws::VaultHealthProto::Diverged,
                            final_height: height - 1,
                            final_state_root: Some(ws::Hash {
                                value: Bytes::copy_from_slice(&final_state_root),
                            }),
                        });
                    }
                }

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
                        }
                        final_state_root = computed_root;
                    },
                    Err(e) => {
                        log_ctx.end_storage_timer();
                        log_ctx.set_error("StateRootFailed", &format!("{:?}", e));
                        return Ok(w::RecoverVaultResponse {
                            success: false,
                            message: format!(
                                "State root computation failed at height {}: {:?}",
                                height, e
                            ),
                            health_status: ws::VaultHealthProto::Diverged,
                            final_height: height - 1,
                            final_state_root: Some(ws::Hash {
                                value: Bytes::copy_from_slice(&final_state_root),
                            }),
                        });
                    },
                }
            }

            last_vault_hash = Some(compute_vault_block_hash(entry));
        }
        log_ctx.end_storage_timer();

        log_ctx.start_raft_timer();
        if divergence_detected {
            tracing::error!(
                organization_id = organization_id.value(),
                vault_id = vault_id.value(),
                "Recovery reproduced divergence - possible determinism bug"
            );

            if let Err(e) = self
                .handle
                .propose(RaftPayload::system(OrganizationRequest::UpdateVaultHealth {
                    organization: organization_id,
                    vault: vault_id,
                    healthy: false,
                    expected_root: None,
                    computed_root: Some(final_state_root),
                    diverged_at_height: Some(expected_height),
                    recovery_attempt: None,
                    recovery_started_at: None,
                }))
                .await
            {
                tracing::error!("Failed to update vault health via Raft: {}", e);
            }
            log_ctx.end_raft_timer();

            log_ctx.set_block_height(expected_height);
            log_ctx.set_error("DivergenceReproduced", "Recovery reproduced divergence");
            log_ctx.record_event(
                EventAction::VaultRecovered,
                EventOutcomeType::Failed {
                    code: "divergence_reproduced".to_string(),
                    detail: "Recovery reproduced divergence".to_string(),
                },
                &[("recovery_method", "replay"), ("final_height", &expected_height.to_string())],
            );
            Ok(w::RecoverVaultResponse {
                success: false,
                message: "Recovery reproduced divergence - possible determinism bug. Manual investigation required.".to_string(),
                health_status: ws::VaultHealthProto::Diverged,
                final_height: expected_height,
                final_state_root: Some(ws::Hash {
                    value: Bytes::copy_from_slice(&final_state_root),
                }),
            })
        } else {
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
            }
            log_ctx.end_raft_timer();

            log_ctx.set_block_height(expected_height);
            log_ctx.set_success();
            log_ctx.record_event(
                EventAction::VaultRecovered,
                EventOutcomeType::Success,
                &[("recovery_method", "replay"), ("final_height", &expected_height.to_string())],
            );
            Ok(w::RecoverVaultResponse {
                success: true,
                message: "Vault recovered successfully".to_string(),
                health_status: ws::VaultHealthProto::Healthy,
                final_height: expected_height,
                final_state_root: Some(ws::Hash {
                    value: Bytes::copy_from_slice(&final_state_root),
                }),
            })
        }
    }

    async fn simulate_divergence(
        &self,
        request: w::SimulateDivergenceRequest,
        _ctx: RequestContext,
    ) -> Result<w::SimulateDivergenceResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("simulate_divergence", &_ctx);

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_slug_val = request.organization.as_ref().map_or(0, |n| n.value());
        let organization_id = slug_resolver
            .extract_and_resolve(
                request
                    .organization
                    .as_ref()
                    .map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let vault_id = slug_resolver
            .extract_and_resolve_vault(
                request.vault.as_ref().map(|s| inferadb_ledger_types::VaultSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let vault_val = request.vault.as_ref().map_or(0, |v| v.value());
        log_ctx.set_target(organization_slug_val, vault_val);

        let expected_root: [u8; 32] = request
            .expected_state_root
            .as_ref()
            .map(|h| h.value.as_ref().try_into().unwrap_or([0u8; 32]))
            .unwrap_or([1u8; 32]);

        let computed_root: [u8; 32] = request
            .computed_state_root
            .as_ref()
            .map(|h| h.value.as_ref().try_into().unwrap_or([0u8; 32]))
            .unwrap_or([2u8; 32]);

        let at_height = if request.at_height > 0 {
            request.at_height
        } else {
            self.applied_state.vault_height(organization_id, vault_id).max(1)
        };

        log_ctx.set_block_height(at_height);

        tracing::warn!(
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
            at_height,
            "Simulating vault divergence for testing"
        );

        log_ctx.start_raft_timer();

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

        if let Err(e) = self.handle.propose(RaftPayload::system(make_health_request())).await {
            log_ctx.end_raft_timer();
            log_ctx.set_error("RaftError", &e.to_string());
            return Err(error_classify::storage_error(&e));
        }

        if let Some(ref manager) = self.raft_manager
            && let Some(org_group) = manager.route_organization(organization_id)
        {
            let _ = org_group.handle().propose(RaftPayload::system(make_health_request())).await;
        }

        log_ctx.end_raft_timer();
        log_ctx.set_success();

        Ok(w::SimulateDivergenceResponse {
            success: true,
            message: format!(
                "Vault {}:{} marked as diverged at height {}",
                organization_id.value(),
                vault_id.value(),
                at_height
            ),
            health_status: ws::VaultHealthProto::Diverged,
        })
    }

    async fn force_gc(
        &self,
        request: w::ForceGcRequest,
        _ctx: RequestContext,
    ) -> Result<w::ForceGcResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut log_ctx = self.make_request_context_unified_wire("force_gc", &_ctx);

        log_ctx.set_raft_term(self.handle.current_term());
        log_ctx.set_is_leader(self.handle.is_leader());

        if !self.handle.is_leader() {
            let msg = "Only the leader can run garbage collection";
            log_ctx.set_error("NotLeader", msg);
            return Err(tonic_status_to_wire_error(
                super::metadata::not_leader_status_from_handle(
                    self.handle.as_ref(),
                    self.peer_addresses.as_ref(),
                    msg,
                    None,
                    None,
                    None,
                ),
            ));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut total_expired = 0u64;
        let mut vaults_scanned = 0u64;

        let slug_resolver = SlugResolver::new(self.applied_state.clone());

        let vault_heights: Vec<(
            (inferadb_ledger_types::OrganizationId, inferadb_ledger_types::VaultId),
            u64,
        )> = if let (Some(ref organization_slug), Some(ref vault_slug)) =
            (request.organization, request.vault)
        {
            let org_id = slug_resolver.extract_and_resolve(Some(
                inferadb_ledger_types::OrganizationSlug::new(organization_slug.value()),
            ))?;
            let v_id = slug_resolver
                .resolve_vault(inferadb_ledger_types::VaultSlug::new(vault_slug.value()))?;
            log_ctx.set_target(organization_slug.value(), vault_slug.value());
            let height = self
                .raft_manager
                .as_ref()
                .and_then(|m| m.route_organization(org_id))
                .map(|g| g.applied_state().vault_height(org_id, v_id))
                .unwrap_or_else(|| self.applied_state.vault_height(org_id, v_id));
            vec![((org_id, v_id), height)]
        } else if let Some(ref manager) = self.raft_manager {
            let mut heights = Vec::new();
            manager.for_each_vault_across_groups(|org, vault, h| heights.push(((org, vault), h)));
            heights
        } else {
            let mut heights = Vec::new();
            self.applied_state
                .for_each_vault_height(|org, vault, h| heights.push(((org, vault), h)));
            heights
        };

        log_ctx.start_raft_timer();
        for ((organization_id, vault_id), _height) in vault_heights {
            vaults_scanned += 1;

            let (regional_state, regional_handle) = if let Some(ref manager) = self.raft_manager
                && let Some(group) = manager.route_organization(organization_id)
            {
                (group.state().clone(), Some(group.handle().clone()))
            } else {
                (self.state.clone(), None)
            };

            let expired = match regional_state.list_entities(vault_id, None, None, 1000) {
                Ok(entities) => entities
                    .into_iter()
                    .filter(|e| e.expires_at > 0 && e.expires_at < now)
                    .map(|e| {
                        let key = String::from_utf8_lossy(&e.key).to_string();
                        (key, e.expires_at)
                    })
                    .collect::<Vec<_>>(),
                Err(e) => {
                    tracing::warn!(
                        organization_id = organization_id.value(),
                        vault_id = vault_id.value(),
                        error = %e,
                        "Failed to list entities for GC"
                    );
                    continue;
                },
            };

            if expired.is_empty() {
                continue;
            }

            let count = expired.len();

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
                sequence: now,
                operations,
                timestamp: chrono::Utc::now(),
            };

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
                    tracing::warn!(
                        organization_id = organization_id.value(),
                        vault_id = vault_id.value(),
                        error = %e,
                        "GC write failed"
                    );
                },
            }
        }
        log_ctx.end_raft_timer();

        log_ctx.set_keys_count(vaults_scanned as usize);
        log_ctx.set_operations_count(total_expired as usize);
        log_ctx.set_success();

        Ok(w::ForceGcResponse {
            success: true,
            message: format!(
                "GC cycle complete: {} expired entities removed from {} vaults",
                total_expired, vaults_scanned
            ),
            expired_count: total_expired,
            vaults_scanned,
        })
    }

    async fn update_config(
        &self,
        request: w::UpdateConfigRequest,
        _ctx: RequestContext,
    ) -> Result<w::UpdateConfigResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;
        let mut log_ctx = self.make_request_context_unified_wire("update_config", &_ctx);

        let runtime_config = self.runtime_config.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Runtime configuration is not enabled on this node",
            )
        })?;

        let new_config: inferadb_ledger_types::config::RuntimeConfig =
            serde_json::from_str(&request.config_json).map_err(|e| {
                WireError::new(ErrorCode::InvalidArgument, format!("Invalid config JSON: {e}"))
            })?;

        new_config.validate().map_err(|e| {
            WireError::new(ErrorCode::InvalidArgument, format!("Config validation failed: {e}"))
        })?;

        let current = runtime_config.load();
        let changed = current.diff(&new_config);

        if changed.is_empty() {
            let current_json = serde_json::to_string_pretty(&*current).unwrap_or_default();
            return Ok(w::UpdateConfigResponse {
                applied: false,
                message: "No changes detected".to_string(),
                current_config_json: current_json,
                changed_fields: Vec::new(),
            });
        }

        if request.dry_run {
            let current_json = serde_json::to_string_pretty(&*current).unwrap_or_default();
            return Ok(w::UpdateConfigResponse {
                applied: false,
                message: format!("Dry run: would change {}", changed.join(", ")),
                current_config_json: current_json,
                changed_fields: changed,
            });
        }

        let hibernation_manager = self.raft_manager.clone();
        let hibernation_closure = hibernation_manager.map(|mgr| {
            move |new_policy: &inferadb_ledger_types::config::HibernationConfig| {
                mgr.set_hibernation_config(new_policy.clone());
                if new_policy.enabled {
                    mgr.start_hibernation_idle_detector();
                }
            }
        });
        let hibernation_cb: Option<
            inferadb_ledger_raft::runtime_config::HibernationReconfigCallback,
        > = hibernation_closure.as_ref().map(|f| {
            f as &(dyn Fn(&inferadb_ledger_types::config::HibernationConfig) + Send + Sync)
        });

        let changed = runtime_config
            .update(
                new_config,
                self.rate_limiter.as_ref(),
                self.hot_key_detector.as_ref(),
                hibernation_cb,
            )
            .map_err(|e| {
                WireError::new(ErrorCode::InvalidArgument, format!("Config update failed: {e}"))
            })?;

        let changed_fields_str = changed.join(", ");
        log_ctx.set_success();
        log_ctx.record_event(
            EventAction::ConfigurationChanged,
            EventOutcomeType::Success,
            &[("changed_fields", &changed_fields_str)],
        );

        let updated = runtime_config.load();
        let updated_json = serde_json::to_string_pretty(&*updated).unwrap_or_default();

        Ok(w::UpdateConfigResponse {
            applied: true,
            message: format!("Updated: {changed_fields_str}"),
            current_config_json: updated_json,
            changed_fields: changed,
        })
    }

    async fn get_config(
        &self,
        _request: w::GetConfigRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetConfigResponse, WireError> {
        let runtime_config = self.runtime_config.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Runtime configuration is not enabled on this node",
            )
        })?;

        let current = runtime_config.load();
        let config_json = serde_json::to_string_pretty(&*current).unwrap_or_default();

        Ok(w::GetConfigResponse { config_json })
    }

    async fn create_backup(
        &self,
        request: w::CreateBackupRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateBackupResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("create_backup", &_ctx);

        let backup_manager = self.backup_manager.as_ref().ok_or_else(|| {
            WireError::new(ErrorCode::FailedPrecondition, "Backup is not configured on this node")
        })?;

        let key_manager = self.key_manager.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Key manager is not configured on this node — cannot stamp \
                 RMK fingerprint on backup archive",
            )
        })?;

        let raft_manager = self.raft_manager.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Raft manager is not configured on this node",
            )
        })?;

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_id = slug_resolver
            .extract_and_resolve(
                request
                    .organization
                    .as_ref()
                    .map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("OrganizationNotFound", &err.message);
            })?;
        let organization_slug = slug_resolver.resolve_slug(organization_id)?;

        let tag = request.tag.unwrap_or_default();

        let region = raft_manager.get_organization_region(organization_id).ok_or_else(|| {
            log_ctx.set_error("OrganizationNotPlaced", &organization_id.to_string());
            WireError::new(
                ErrorCode::FailedPrecondition,
                format!("Organization {} has no region placement on file", organization_id.value()),
            )
        })?;

        let rmk_fingerprint = key_manager.rmk_fingerprint(region).map_err(|e| {
            log_ctx.set_error("RmkFingerprintError", &e.to_string());
            error_classify::crypto_error(&e)
        })?;

        let all_vaults = self.applied_state.all_vaults();
        for (_org_id, vault_id) in all_vaults.keys() {
            let db = self.state.db_for(*vault_id).map_err(|e| {
                log_ctx.set_error("BackupVaultOpenError", &e.to_string());
                error_classify::storage_error(&e)
            })?;
            db.sync_state().await.map_err(|e| {
                log_ctx.set_error("SyncStateError", &e.to_string());
                error_classify::storage_error(&e)
            })?;
        }

        let backups_dir = self.backups_dir.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Backup destination directory is not configured on this node",
            )
        })?;
        let timestamp_micros = chrono::Utc::now().timestamp_micros();
        let backup_id = format!("{}-{}", organization_id.value(), timestamp_micros);
        let archive_filename = format!("backup-{backup_id}.tar.zst");
        let archive_path = backups_dir.join(&archive_filename);

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
                log_ctx.set_error("BackupError", &e.to_string());
                error_classify::storage_error(&e)
            })?;

        let size_bytes = std::fs::metadata(&archive_path)
            .map(|m| m.len())
            .map_err(|e| error_classify::storage_error(&e))?;

        log_ctx.set_success();
        inferadb_ledger_raft::metrics::record_backup_created(0, size_bytes);

        log_ctx.record_event(
            EventAction::BackupCreated,
            EventOutcomeType::Success,
            &[
                ("backup_id", &backup_id),
                ("tag", &tag),
                ("organization_slug", &organization_slug.value().to_string()),
            ],
        );

        Ok(w::CreateBackupResponse {
            backup_id,
            backup_path: archive_path.display().to_string(),
            size_bytes,
            manifest: Some(backup_manifest_to_wire(&manifest)),
        })
    }

    async fn list_backups(
        &self,
        request: w::ListBackupsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListBackupsResponse, WireError> {
        let backups_dir = self.backups_dir.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Backup destination directory is not configured on this node",
            )
        })?;

        let mut entries: Vec<w::BackupInfo> = Vec::new();

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
                        st.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos() as u64)
                    });

                let manifest_wire = match std::fs::File::open(&path) {
                    Ok(file) => match inferadb_ledger_raft::backup::archive::read_manifest(file) {
                        Ok(manifest) => Some(backup_manifest_to_wire(&manifest)),
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

                let tag = manifest_wire.as_ref().map(|_| String::new()).unwrap_or_default();

                entries.push(w::BackupInfo {
                    backup_id: backup_id.to_string(),
                    backup_path: path.display().to_string(),
                    size_bytes,
                    created_at: created_at.unwrap_or(0),
                    tag,
                    manifest: manifest_wire,
                });
            }
        }

        // Sort newest-first by `created_at`; fall back to `backup_id` reverse
        // string compare when both are absent / equal — keeps ordering stable
        // and matches the previous proto-side behavior.
        entries.sort_by(|a, b| match (a.created_at, b.created_at) {
            (0, 0) => b.backup_id.cmp(&a.backup_id),
            (a_ts, b_ts) => b_ts.cmp(&a_ts),
        });

        if request.limit > 0 && entries.len() > request.limit as usize {
            entries.truncate(request.limit as usize);
        }

        Ok(w::ListBackupsResponse { backups: entries })
    }

    async fn restore_backup(
        &self,
        request: w::RestoreBackupRequest,
        _ctx: RequestContext,
    ) -> Result<w::RestoreBackupResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut log_ctx = self.make_request_context_unified_wire("restore_backup", &_ctx);

        let backup_manager = self.backup_manager.as_ref().ok_or_else(|| {
            WireError::new(ErrorCode::FailedPrecondition, "Backup is not configured on this node")
        })?;

        let key_manager = self.key_manager.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Key manager is not configured on this node — cannot validate \
                 backup archive RMK fingerprint",
            )
        })?;

        let backups_dir = self.backups_dir.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Backup destination directory is not configured on this node",
            )
        })?;

        if request.backup_id.is_empty()
            || request.backup_id.contains('/')
            || request.backup_id.contains('\\')
            || request.backup_id.split('-').any(|seg| seg == "..")
        {
            log_ctx.set_error("InvalidBackupId", &request.backup_id);
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                format!("Invalid backup_id: {}", request.backup_id),
            ));
        }

        let archive_path = backups_dir.join(format!("backup-{}.tar.zst", request.backup_id));
        if !archive_path.exists() {
            log_ctx.set_error("BackupNotFound", &archive_path.display().to_string());
            return Err(WireError::new(
                ErrorCode::NotFound,
                format!("Backup archive not found: {}", archive_path.display()),
            ));
        }

        let preflight_file = std::fs::File::open(&archive_path).map_err(|e| {
            log_ctx.set_error("ArchiveOpenError", &e.to_string());
            error_classify::storage_error(&e)
        })?;
        let preflight_manifest =
            inferadb_ledger_raft::backup::archive::read_manifest(preflight_file).map_err(|e| {
                log_ctx.set_error("ManifestReadError", &e.to_string());
                error_classify::storage_error(&e)
            })?;
        let region = preflight_manifest.region;
        let local_fingerprint = key_manager.rmk_fingerprint(region).map_err(|e| {
            log_ctx.set_error("RmkFingerprintError", &e.to_string());
            error_classify::crypto_error(&e)
        })?;

        let staging_result = backup_manager
            .stage_restore(&archive_path, Some(&local_fingerprint))
            .await
            .map_err(|e| {
                log_ctx.set_error("StageRestoreError", &e.to_string());
                error_classify::storage_error(&e)
            })?;

        let manifest_wire = backup_manifest_to_wire(&staging_result.manifest);
        let staging_dir = staging_result.staging_dir.display().to_string();

        log_ctx.set_success();

        log_ctx.record_event(
            EventAction::BackupRestored,
            EventOutcomeType::Success,
            &[("backup_id", &request.backup_id), ("staging_dir", &staging_dir)],
        );

        Ok(w::RestoreBackupResponse { staging_dir, manifest: Some(manifest_wire) })
    }

    async fn rotate_blinding_key(
        &self,
        request: w::RotateBlindingKeyRequest,
        _ctx: RequestContext,
    ) -> Result<w::RotateBlindingKeyResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut log_ctx = self.make_request_context_unified_wire("rotate_blinding_key", &_ctx);

        if request.new_key_version == 0 {
            log_ctx.set_error("InvalidArgument", "new_key_version must be > 0");
            return Err(WireError::new(ErrorCode::InvalidArgument, "new_key_version must be > 0"));
        }

        let system_service = SystemOrganizationService::new(Arc::clone(&self.state));
        let current_version = system_service
            .get_blinding_key_version()
            .map_err(|e| error_classify::crypto_error(&e))?
            .unwrap_or(0);

        if request.new_key_version <= current_version {
            log_ctx.set_error(
                "InvalidArgument",
                "new_key_version must be greater than the current version",
            );
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "new_key_version ({}) must be greater than current version ({current_version})",
                    request.new_key_version,
                ),
            ));
        }

        let rotation_in_progress = system_service
            .is_rotation_in_progress()
            .map_err(|e| error_classify::crypto_error(&e))?;

        if rotation_in_progress {
            log_ctx
                .set_error("FailedPrecondition", "A blinding key rotation is already in progress");
            return Err(WireError::new(
                ErrorCode::FailedPrecondition,
                "A blinding key rotation is already in progress",
            ));
        }

        let response = self
            .propose_raft_request_wire(
                SystemRequest::SetBlindingKeyVersion { version: request.new_key_version },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::Empty => {
                log_ctx.record_event(
                    EventAction::ConfigurationChanged,
                    EventOutcomeType::Success,
                    &[
                        ("resource", "blinding_key"),
                        ("new_version", &request.new_key_version.to_string()),
                        ("previous_version", &current_version.to_string()),
                    ],
                );

                log_ctx.set_success();
                Ok(w::RotateBlindingKeyResponse {
                    total_entries: 0,
                    entries_rehashed: 0,
                    complete: false,
                })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_wire_error(code, message))
            },
            _ => {
                log_ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response type"))
            },
        }
    }

    async fn get_blinding_key_rehash_status(
        &self,
        _request: w::GetBlindingKeyRehashStatusRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetBlindingKeyRehashStatusResponse, WireError> {
        let mut log_ctx =
            self.make_request_context_unified_wire("get_blinding_key_rehash_status", &_ctx);

        let system_service = SystemOrganizationService::new(Arc::clone(&self.state));

        let active_key_version = system_service
            .get_blinding_key_version()
            .map_err(|e| error_classify::crypto_error(&e))?
            .unwrap_or(0);

        let mut per_region_progress = std::collections::BTreeMap::new();
        let mut total_rehashed: u64 = 0;

        for region in ALL_REGIONS {
            match system_service.get_rehash_progress(region) {
                Ok(Some(entries)) => {
                    per_region_progress.insert(region.as_str().to_string(), entries);
                    total_rehashed = total_rehashed.saturating_add(entries);
                },
                Ok(None) => {},
                Err(e) => {
                    log_ctx.set_error("Internal", &e.to_string());
                    return Err(error_classify::storage_error(&e));
                },
            }
        }

        let complete = per_region_progress.is_empty();

        log_ctx.set_success();
        Ok(w::GetBlindingKeyRehashStatusResponse {
            total_entries: 0,
            entries_rehashed: total_rehashed,
            complete,
            per_region_progress,
            active_key_version,
        })
    }

    async fn rotate_region_key(
        &self,
        request: w::RotateRegionKeyRequest,
        _ctx: RequestContext,
    ) -> Result<w::RotateRegionKeyResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut log_ctx = self.make_request_context_unified_wire("rotate_region_key", &_ctx);

        let progress = self.rewrap_progress.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "DEK re-wrapping not configured for this node",
            )
        })?;

        let total_pages = self.state.sidecar_page_count().map_err(|e| {
            log_ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;

        let target_version = if request.target_version > 0 { request.target_version } else { 0 };

        if total_pages == 0 {
            log_ctx.set_success();
            return Ok(w::RotateRegionKeyResponse {
                new_version: target_version,
                total_pages: 0,
                complete: true,
            });
        }

        progress.start_rotation(target_version, total_pages);

        log_ctx.set_success();
        Ok(w::RotateRegionKeyResponse { new_version: target_version, total_pages, complete: false })
    }

    async fn get_rewrap_status(
        &self,
        _request: w::GetRewrapStatusRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetRewrapStatusResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("get_rewrap_status", &_ctx);

        let progress = self.rewrap_progress.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "DEK re-wrapping not configured for this node",
            )
        })?;

        let total = progress.total_pages.load(std::sync::atomic::Ordering::Acquire);
        let processed = progress.next_page_id.load(std::sync::atomic::Ordering::Acquire);
        let rewrapped = progress.pages_rewrapped.load(std::sync::atomic::Ordering::Acquire);
        let complete = progress.complete.load(std::sync::atomic::Ordering::Acquire);
        let target = progress.target_version.load(std::sync::atomic::Ordering::Acquire);
        let est_remaining = progress.estimated_remaining_secs();

        log_ctx.set_success();
        Ok(w::GetRewrapStatusResponse {
            total_pages: total,
            pages_processed: processed,
            pages_rewrapped: rewrapped,
            complete,
            target_version: target as u32,
            estimated_remaining_secs: est_remaining,
        })
    }

    async fn migrate_existing_users(
        &self,
        request: w::MigrateExistingUsersRequest,
        _ctx: RequestContext,
    ) -> Result<w::MigrateExistingUsersResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let start = std::time::Instant::now();
        let mut log_ctx = self.make_request_context_unified_wire("migrate_existing_users", &_ctx);

        let default_region = inferadb_ledger_types::Region::from_str(&request.default_region)
            .map_err(|err| {
                WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
            })?;

        if default_region == inferadb_ledger_types::Region::GLOBAL {
            log_ctx.set_error("InvalidArgument", "default_region cannot be GLOBAL");
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "default_region cannot be GLOBAL: users must reside in a data region",
            ));
        }

        if request.email_blinding_key.is_empty() {
            log_ctx.set_error("InvalidArgument", "email_blinding_key is required");
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "email_blinding_key is required",
            ));
        }
        let blinding_key =
            match inferadb_ledger_types::EmailBlindingKey::from_str(&request.email_blinding_key) {
                Ok(k) => k,
                Err(e) => {
                    log_ctx.set_error("InvalidArgument", &format!("invalid blinding key: {e}"));
                    return Err(WireError::new(
                        ErrorCode::InvalidArgument,
                        format!("invalid email_blinding_key: {e}"),
                    ));
                },
            };

        let sys = SystemOrganizationService::new(self.state.clone());
        let users = match sys.list_flat_users() {
            Ok(u) => u,
            Err(e) => {
                log_ctx.set_error("Internal", &format!("failed to list users: {e}"));
                return Err(error_classify::storage_error(&e));
            },
        };

        if users.is_empty() {
            log_ctx.set_success();
            return Ok(w::MigrateExistingUsersResponse {
                users: 0,
                migrated: 0,
                skipped: 0,
                errors: 0,
                elapsed_secs: start.elapsed().as_secs_f64(),
            });
        }

        let mut entries = Vec::with_capacity(users.len());
        let mut pre_skipped: u64 = 0;

        for user in &users {
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

            let target_region = if user.region == inferadb_ledger_types::Region::GLOBAL {
                default_region
            } else {
                user.region
            };

            let email_hmac_hex = match sys.get_user_email(user.email) {
                Ok(Some(user_email)) => {
                    inferadb_ledger_types::compute_email_hmac(&blinding_key, &user_email.email)
                },
                _ => {
                    pre_skipped += 1;
                    continue;
                },
            };

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

        if entries.is_empty() {
            log_ctx.set_success();
            return Ok(w::MigrateExistingUsersResponse {
                users: users.len() as u64,
                migrated: 0,
                skipped: pre_skipped,
                errors: 0,
                elapsed_secs: start.elapsed().as_secs_f64(),
            });
        }

        let response = self
            .propose_raft_request_wire(
                SystemRequest::MigrateExistingUsers { entries },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::UsersMigrated { users, migrated, skipped, errors } => {
                log_ctx.record_event(
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

                log_ctx.set_success();
                Ok(w::MigrateExistingUsersResponse {
                    users,
                    migrated,
                    skipped: skipped + pre_skipped,
                    errors,
                    elapsed_secs: start.elapsed().as_secs_f64(),
                })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_wire_error(
                    code,
                    format!("Migration failed: {message}"),
                ))
            },
            other => {
                log_ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                Err(WireError::new(
                    ErrorCode::Internal,
                    "Unexpected response from Raft state machine",
                ))
            },
        }
    }

    async fn provision_region(
        &self,
        request: w::ProvisionRegionRequest,
        _ctx: RequestContext,
    ) -> Result<w::ProvisionRegionResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let start = std::time::Instant::now();
        let mut log_ctx = self.make_request_context_unified_wire("provision_region", &_ctx);

        let protected = request.protected;
        let region = inferadb_ledger_types::Region::from_str(&request.name).map_err(|err| {
            WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
        })?;

        let requires_residency = request.requires_residency;
        let retention_days = if request.retention_days == 0 {
            inferadb_ledger_state::system::default_retention_days()
        } else {
            request.retention_days
        };

        if region == inferadb_ledger_types::Region::GLOBAL {
            log_ctx.set_error("InvalidArgument", "GLOBAL region cannot be provisioned");
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "GLOBAL region is always created eagerly on startup",
            ));
        }

        let manager = self.raft_manager.as_ref().ok_or_else(|| {
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Region provisioning not available on single-region nodes",
            )
        })?;

        if manager.get_region_group(region).is_ok() {
            tracing::info!(
                region = region.as_str(),
                protected,
                created = false,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "provision_region completed (already provisioned)"
            );
            return Ok(w::ProvisionRegionResponse {
                created: false,
                name: region.as_str().to_owned(),
            });
        }

        let initial_members: Vec<(u64, String)> = manager
            .system_region()
            .map_err(|e| {
                log_ctx.set_error("Internal", &format!("system region not available: {e}"));
                WireError::new(
                    ErrorCode::FailedPrecondition,
                    format!("system region not available: {e}"),
                )
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
            log_ctx.set_error("Internal", "no GLOBAL voters with known addresses");
            return Err(WireError::new(
                ErrorCode::FailedPrecondition,
                "Cannot provision region: no GLOBAL voters with registered peer addresses",
            ));
        }

        let raft_req = SystemRequest::CreateDataRegion {
            region,
            protected,
            requires_residency,
            retention_days,
            initial_members,
        };
        let response = self
            .handle
            .propose_and_wait(RaftPayload::system(raft_req), Duration::from_secs(10))
            .await
            .map_err(|e| {
                log_ctx.set_error("Internal", &format!("CreateDataRegion propose failed: {e}"));
                error_classify::raft_error(&e)
            })?;

        let created = matches!(response, LedgerResponse::DataRegionCreated { .. });

        let local_ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if manager.get_region_group(region).is_ok() {
                break;
            }
            if tokio::time::Instant::now() >= local_ready_deadline {
                log_ctx.set_error("Internal", "local region group did not start within 5s");
                return Err(WireError::new(
                    ErrorCode::FailedPrecondition,
                    "Region created in cluster state but local group did not start within 5s",
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        tracing::info!(
            region = region.as_str(),
            protected,
            created,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "provision_region completed"
        );

        Ok(w::ProvisionRegionResponse { created, name: region.as_str().to_owned() })
    }

    async fn set_region_residency(
        &self,
        request: w::SetRegionResidencyRequest,
        _ctx: RequestContext,
    ) -> Result<w::SetRegionResidencyResponse, WireError> {
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        let mut log_ctx = self.make_request_context_unified_wire("set_region_residency", &_ctx);

        let region = inferadb_ledger_types::Region::from_str(&request.name).map_err(|err| {
            WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
        })?;
        if region == inferadb_ledger_types::Region::GLOBAL {
            log_ctx.set_error("InvalidArgument", "GLOBAL region has no residency contract");
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "GLOBAL region is the cluster control plane and has no residency contract",
            ));
        }

        let propose = SystemRequest::SetRegionResidency {
            region,
            requires_residency: request.requires_residency,
            retention_days: request.retention_days,
        };
        let response = self
            .handle
            .propose_and_wait(RaftPayload::system(propose), Duration::from_secs(10))
            .await
            .map_err(|e| {
                log_ctx.set_error("Internal", &format!("SetRegionResidency propose failed: {e}"));
                error_classify::raft_error(&e)
            })?;

        match response {
            LedgerResponse::RegionResidencyUpdated {
                region: r,
                requires_residency,
                retention_days,
            } => Ok(w::SetRegionResidencyResponse {
                name: r.as_str().to_owned(),
                requires_residency,
                retention_days,
            }),
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_wire_error(code, message))
            },
            other => {
                log_ctx.set_error("Internal", &format!("Unexpected response: {other:?}"));
                Err(WireError::new(
                    ErrorCode::Internal,
                    format!("Unexpected response from SetRegionResidency: {other:?}"),
                ))
            },
        }
    }

    async fn admin_list_vaults(
        &self,
        request: w::AdminListVaultsRequest,
        _ctx: RequestContext,
    ) -> Result<w::AdminListVaultsResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("admin_list_vaults", &_ctx);

        let manager = self.raft_manager.as_ref().ok_or_else(|| {
            log_ctx.set_error("FailedPrecondition", "Raft manager is not configured on this node");
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Raft manager is not configured on this node",
            )
        })?;

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_slug_val = request.organization.as_ref().map_or(0, |n| n.value());
        let organization_id = slug_resolver
            .extract_and_resolve(
                request
                    .organization
                    .as_ref()
                    .map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        log_ctx.set_target(organization_slug_val, 0);

        let mut vaults = Vec::new();
        for (region, org_id, vault_id) in manager.list_vault_groups() {
            if org_id != organization_id {
                continue;
            }
            let Ok(group) = manager.get_vault_group(region, org_id, vault_id) else {
                continue;
            };
            vaults.push(self.vault_info_from_group(&group));
        }

        log_ctx.set_success();
        Ok(w::AdminListVaultsResponse { vaults })
    }

    async fn show_vault(
        &self,
        request: w::ShowVaultRequest,
        _ctx: RequestContext,
    ) -> Result<w::ShowVaultResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("show_vault", &_ctx);

        let manager = self.raft_manager.as_ref().ok_or_else(|| {
            log_ctx.set_error("FailedPrecondition", "Raft manager is not configured on this node");
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Raft manager is not configured on this node",
            )
        })?;

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_slug_val = request.organization.as_ref().map_or(0, |n| n.value());
        let vault_slug_val = request.vault.as_ref().map_or(0, |v| v.value());
        let organization_id = slug_resolver
            .extract_and_resolve(
                request
                    .organization
                    .as_ref()
                    .map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let vault_id = slug_resolver
            .extract_and_resolve_vault(
                request.vault.as_ref().map(|s| inferadb_ledger_types::VaultSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        log_ctx.set_target(organization_slug_val, vault_slug_val);

        let region = manager.get_organization_region(organization_id).ok_or_else(|| {
            log_ctx.set_error(
                "OrganizationNotPlaced",
                &format!("Organization {} has no region placement on file", organization_id),
            );
            WireError::new(
                ErrorCode::FailedPrecondition,
                format!("Organization {} has no region placement on file", organization_id),
            )
        })?;

        let group = manager.get_vault_group(region, organization_id, vault_id).map_err(|e| {
            log_ctx.set_error("VaultGroupNotFound", &e.to_string());
            WireError::new(
                ErrorCode::NotFound,
                format!(
                    "Vault group not registered on this node: organization={}, vault={}, region={}",
                    organization_id,
                    vault_id,
                    region.as_str()
                ),
            )
        })?;

        let info = self.vault_info_from_group(&group);
        let shard_state = group.handle().shard_state();
        let last_activity_secs = group.last_activity_unix_secs();
        let pending_membership_secs = group.pending_membership_started_unix_secs();
        let lifecycle = group.lifecycle_state();

        let response = w::ShowVaultResponse {
            info: Some(info),
            region: region.as_str().to_owned(),
            organization_id: organization_id.value() as u64,
            vault_id: vault_id.value() as u64,
            voters: shard_state.voters.iter().map(|n| n.0).collect(),
            learners: shard_state.learners.iter().map(|n| n.0).collect(),
            conf_epoch: shard_state.conf_epoch,
            lifecycle_state: lifecycle.as_metric_label().to_owned(),
            last_activity_at: unix_secs_to_ns_opt(last_activity_secs),
            pending_membership_started_at: unix_secs_to_ns_opt(pending_membership_secs),
        };

        log_ctx.set_success();
        Ok(response)
    }

    async fn repair_vault(
        &self,
        request: w::RepairVaultRequest,
        _ctx: RequestContext,
    ) -> Result<w::RepairVaultResponse, WireError> {
        let mut log_ctx = self.make_request_context_unified_wire("repair_vault", &_ctx);

        let manager = self.raft_manager.as_ref().ok_or_else(|| {
            log_ctx.set_error("FailedPrecondition", "Raft manager is not configured on this node");
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Raft manager is not configured on this node",
            )
        })?;

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let organization_slug_val = request.organization.as_ref().map_or(0, |n| n.value());
        let vault_slug_val = request.vault.as_ref().map_or(0, |v| v.value());
        let organization_id = slug_resolver
            .extract_and_resolve(
                request
                    .organization
                    .as_ref()
                    .map(|s| inferadb_ledger_types::OrganizationSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let vault_id = slug_resolver
            .extract_and_resolve_vault(
                request.vault.as_ref().map(|s| inferadb_ledger_types::VaultSlug::new(s.value())),
            )
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        log_ctx.set_target(organization_slug_val, vault_slug_val);

        let region = manager.get_organization_region(organization_id).ok_or_else(|| {
            log_ctx.set_error(
                "OrganizationNotPlaced",
                &format!("Organization {} has no region placement on file", organization_id),
            );
            WireError::new(
                ErrorCode::FailedPrecondition,
                format!("Organization {} has no region placement on file", organization_id),
            )
        })?;

        let _ = manager.get_vault_group(region, organization_id, vault_id).map_err(|e| {
            log_ctx.set_error("VaultGroupNotFound", &e.to_string());
            WireError::new(
                ErrorCode::NotFound,
                format!(
                    "Vault group not registered on this node: organization={}, vault={}, region={}",
                    organization_id,
                    vault_id,
                    region.as_str()
                ),
            )
        })?;

        tracing::warn!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            vault_id = vault_id.value(),
            organization_slug = organization_slug_val,
            vault_slug = vault_slug_val,
            "RepairVault requested — no consensus-engine repair API yet, returning noop"
        );

        log_ctx.set_success();
        Ok(w::RepairVaultResponse {
            status: "noop".to_owned(),
            message: "Vault repair is not yet implemented. The consensus engine does not \
                      currently expose a force-snapshot or force-re-replication entrypoint. \
                      Operator intent has been logged; track follow-up in \
                      docs/superpowers/completed/2026-04-23-per-vault-consensus.md Phase 7."
                .to_owned(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    //! The wire-trait handlers operate on wire types directly — there are no
    //! proto round-trip helpers left to unit-test. The wire structs already
    //! have their own postcard round-trip tests in
    //! `crates/wire/src/services/admin.rs` and shared-type round-trips in
    //! `crates/wire/src/services/shared.rs`. End-to-end behavior is covered
    //! by the server integration tests (`wire_test_cluster_smoke`,
    //! `wire_multi_node`, `election`, `region_residency`).
    //!
    //! This module retains a smoke-test that the wire-native helper paths
    //! used by the handlers (`unix_secs_to_ns_opt`,
    //! `backup_manifest_to_wire`) produce the expected shapes for the
    //! sentinel inputs the handlers depend on.

    use super::{super::admin::unix_secs_to_ns_opt, *};

    #[test]
    fn unix_secs_to_ns_opt_collapses_zero_to_none() {
        assert_eq!(unix_secs_to_ns_opt(0), None);
    }

    #[test]
    fn unix_secs_to_ns_opt_multiplies_to_nanoseconds() {
        assert_eq!(unix_secs_to_ns_opt(1), Some(1_000_000_000));
        assert_eq!(unix_secs_to_ns_opt(1_700_000_000), Some(1_700_000_000_000_000_000));
    }

    #[test]
    fn backup_manifest_to_wire_preserves_all_fields() {
        let manifest = inferadb_ledger_raft::backup::archive::BackupManifest {
            schema_version: 1,
            format: "ledger.backup.v1".to_owned(),
            region: inferadb_ledger_types::Region::from_str("us-east-va").unwrap(),
            organization_slug: inferadb_ledger_types::OrganizationSlug::new(42),
            organization_id: inferadb_ledger_types::OrganizationId::new(7),
            timestamp_micros: 1_700_000_000_000_000,
            rmk_fingerprint: "sha256:abcdef".to_owned(),
            node_id_at_creation: 1,
            dbs: vec![inferadb_ledger_raft::backup::archive::DbEntry {
                path: "_meta.db".to_owned(),
                size_bytes: 4096,
                checksum: "blake3:deadbeef".to_owned(),
            }],
            vault_count: 0,
            created_by_app_version: "1.0.0".to_owned(),
        };

        let wire = backup_manifest_to_wire(&manifest);
        assert_eq!(wire.schema_version, 1);
        assert_eq!(wire.format, "ledger.backup.v1");
        assert_eq!(wire.region, "us-east-va");
        assert_eq!(wire.organization, Some(ws::OrganizationSlug::new(42)));
        assert_eq!(wire.organization_id, 7);
        assert_eq!(wire.timestamp_micros, 1_700_000_000_000_000);
        assert_eq!(wire.rmk_fingerprint, "sha256:abcdef");
        assert_eq!(wire.node_id_at_creation, 1);
        assert_eq!(wire.dbs.len(), 1);
        assert_eq!(wire.dbs[0].path, "_meta.db");
        assert_eq!(wire.dbs[0].size_bytes, 4096);
        assert_eq!(wire.dbs[0].checksum, "blake3:deadbeef");
        assert_eq!(wire.vault_count, 0);
        assert_eq!(wire.created_by_app_version, "1.0.0");
    }
}
