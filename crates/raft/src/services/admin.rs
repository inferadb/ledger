//! Admin service implementation.
//!
//! Handles namespace and vault management, cluster membership, snapshots, and integrity checks.

use std::collections::BTreeSet;
use std::sync::Arc;

use openraft::{BasicNode, Raft};
use tonic::{Request, Response, Status};

use crate::error::ServiceError;
use crate::log_storage::AppliedStateAccessor;
use crate::log_storage::VaultHealthStatus;
use crate::proto::admin_service_server::AdminService;
use crate::proto::{
    BlockHeader, CheckIntegrityRequest, CheckIntegrityResponse, ClusterMember, ClusterMemberRole,
    CreateNamespaceRequest, CreateNamespaceResponse, CreateSnapshotRequest, CreateSnapshotResponse,
    CreateVaultRequest, CreateVaultResponse, DeleteNamespaceRequest, DeleteNamespaceResponse,
    DeleteVaultRequest, DeleteVaultResponse, GetClusterInfoRequest, GetClusterInfoResponse,
    GetNamespaceRequest, GetNamespaceResponse, GetVaultRequest, GetVaultResponse, Hash,
    IntegrityIssue, JoinClusterRequest, JoinClusterResponse, LeaveClusterRequest,
    LeaveClusterResponse, ListNamespacesRequest, ListNamespacesResponse, ListVaultsRequest,
    ListVaultsResponse, NamespaceId, NodeId, RecoverVaultRequest, RecoverVaultResponse, ShardId,
    VaultHealthProto, VaultId,
};
use crate::types::{
    BlockRetentionMode, BlockRetentionPolicy, LedgerRequest, LedgerResponse, LedgerTypeConfig,
};

use inferadb_ledger_state::{BlockArchive, StateLayer};
use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::{VaultEntry, ZERO_HASH};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

/// Admin service implementation.
pub struct AdminServiceImpl {
    /// The Raft instance.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// The state layer.
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
    /// Block archive for integrity verification.
    block_archive: Option<Arc<BlockArchive<FileBackend>>>,
}

impl AdminServiceImpl {
    /// Create a new admin service.
    pub fn new(
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<StateLayer<FileBackend>>,
        applied_state: AppliedStateAccessor,
    ) -> Self {
        Self {
            raft,
            state,
            applied_state,
            block_archive: None,
        }
    }

    /// Create with block archive for integrity verification.
    pub fn with_block_archive(
        raft: Arc<Raft<LedgerTypeConfig>>,
        state: Arc<StateLayer<FileBackend>>,
        applied_state: AppliedStateAccessor,
        block_archive: Arc<BlockArchive<FileBackend>>,
    ) -> Self {
        Self {
            raft,
            state,
            applied_state,
            block_archive: Some(block_archive),
        }
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        let req = request.into_inner();

        // Submit create namespace through Raft
        // Map proto ShardId to inferadb_ledger_types::ShardId (i32)
        let ledger_request = LedgerRequest::CreateNamespace {
            name: req.name,
            shard_id: req.shard_id.map(|s| s.id),
        };

        let result = self
            .raft
            .client_write(ledger_request)
            .await
            .map_err(ServiceError::raft)?;

        match result.data {
            LedgerResponse::NamespaceCreated {
                namespace_id,
                shard_id,
            } => Ok(Response::new(CreateNamespaceResponse {
                namespace_id: Some(NamespaceId { id: namespace_id }),
                shard_id: Some(ShardId { id: shard_id }),
            })),
            LedgerResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("Unexpected response type")),
        }
    }

    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| ServiceError::invalid_arg("Missing namespace_id"))?;

        // Submit delete namespace through Raft
        let ledger_request = LedgerRequest::DeleteNamespace { namespace_id };

        let result = self
            .raft
            .client_write(ledger_request)
            .await
            .map_err(ServiceError::raft)?;

        match result.data {
            LedgerResponse::NamespaceDeleted { success } => {
                if success {
                    Ok(Response::new(DeleteNamespaceResponse {
                        deleted_at: Some(
                            prost_types::Timestamp::from(std::time::SystemTime::now()),
                        ),
                    }))
                } else {
                    Err(ServiceError::precondition("Namespace has vaults, cannot delete").into())
                }
            }
            LedgerResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("Unexpected response type")),
        }
    }

    async fn get_namespace(
        &self,
        request: Request<GetNamespaceRequest>,
    ) -> Result<Response<GetNamespaceResponse>, Status> {
        let req = request.into_inner();

        // Extract namespace from lookup oneof (by ID or name)
        let ns_meta = match req.lookup {
            Some(crate::proto::get_namespace_request::Lookup::NamespaceId(n)) => {
                self.applied_state.get_namespace(n.id)
            }
            Some(crate::proto::get_namespace_request::Lookup::Name(name)) => {
                self.applied_state.get_namespace_by_name(&name)
            }
            None => return Err(ServiceError::invalid_arg("Missing namespace lookup").into()),
        };

        match ns_meta {
            Some(ns) => Ok(Response::new(GetNamespaceResponse {
                namespace_id: Some(NamespaceId {
                    id: ns.namespace_id,
                }),
                name: ns.name,
                shard_id: Some(ShardId { id: ns.shard_id }),
                member_nodes: vec![],
                leader_hint: None,
                config_version: 0,
                status: crate::proto::NamespaceStatus::Active.into(),
            })),
            None => Err(ServiceError::not_found("Namespace", "unknown").into()),
        }
    }

    async fn list_namespaces(
        &self,
        _request: Request<ListNamespacesRequest>,
    ) -> Result<Response<ListNamespacesResponse>, Status> {
        let namespaces = self
            .applied_state
            .list_namespaces()
            .into_iter()
            .map(|ns| crate::proto::GetNamespaceResponse {
                namespace_id: Some(NamespaceId {
                    id: ns.namespace_id,
                }),
                name: ns.name,
                shard_id: Some(ShardId { id: ns.shard_id }),
                member_nodes: vec![],
                leader_hint: None,
                config_version: 0,
                status: crate::proto::NamespaceStatus::Active.into(),
            })
            .collect();

        Ok(Response::new(ListNamespacesResponse {
            namespaces,
            next_page_token: None,
        }))
    }

    async fn create_vault(
        &self,
        request: Request<CreateVaultRequest>,
    ) -> Result<Response<CreateVaultResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        // Convert proto retention policy to internal type
        let retention_policy = req.retention_policy.map(|proto_policy| {
            use crate::proto::BlockRetentionMode as ProtoMode;
            let mode = match proto_policy.mode() {
                ProtoMode::Unspecified | ProtoMode::Full => BlockRetentionMode::Full,
                ProtoMode::Compacted => BlockRetentionMode::Compacted,
            };
            BlockRetentionPolicy {
                mode,
                retention_blocks: if proto_policy.retention_blocks > 0 {
                    proto_policy.retention_blocks
                } else {
                    10_000 // Default
                },
            }
        });

        // Submit create vault through Raft
        let ledger_request = LedgerRequest::CreateVault {
            namespace_id,
            name: None, // CreateVaultRequest doesn't have name field
            retention_policy,
        };

        let result = self
            .raft
            .client_write(ledger_request)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        match result.data {
            LedgerResponse::VaultCreated { vault_id } => {
                // Get Raft metrics for leader_id and term
                let metrics = self.raft.metrics().borrow().clone();
                let leader_id = metrics.current_leader.unwrap_or(metrics.id);

                // Compute empty state root for genesis block
                let state = &*self.state;
                let state_root = state.compute_state_root(vault_id).unwrap_or(ZERO_HASH);

                // Build genesis block header (height 0)
                let genesis = BlockHeader {
                    height: 0,
                    namespace_id: Some(NamespaceId { id: namespace_id }),
                    vault_id: Some(VaultId { id: vault_id }),
                    previous_hash: Some(Hash {
                        value: ZERO_HASH.to_vec(),
                    }),
                    tx_merkle_root: Some(Hash {
                        value: ZERO_HASH.to_vec(), // Empty transaction list
                    }),
                    state_root: Some(Hash {
                        value: state_root.to_vec(),
                    }),
                    timestamp: Some(prost_types::Timestamp {
                        seconds: chrono::Utc::now().timestamp(),
                        nanos: 0,
                    }),
                    leader_id: Some(NodeId {
                        id: leader_id.to_string(),
                    }),
                    term: metrics.current_term,
                    committed_index: result.log_id.index,
                };

                Ok(Response::new(CreateVaultResponse {
                    vault_id: Some(VaultId { id: vault_id }),
                    genesis: Some(genesis),
                }))
            }
            LedgerResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("Unexpected response type")),
        }
    }

    async fn delete_vault(
        &self,
        request: Request<DeleteVaultRequest>,
    ) -> Result<Response<DeleteVaultResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req
            .vault_id
            .as_ref()
            .map(|v| v.id)
            .ok_or_else(|| Status::invalid_argument("Missing vault_id"))?;

        // Submit delete vault through Raft
        let ledger_request = LedgerRequest::DeleteVault {
            namespace_id,
            vault_id,
        };

        let result = self
            .raft
            .client_write(ledger_request)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        match result.data {
            LedgerResponse::VaultDeleted { success } => {
                if success {
                    Ok(Response::new(DeleteVaultResponse {
                        deleted_at: Some(
                            prost_types::Timestamp::from(std::time::SystemTime::now()),
                        ),
                    }))
                } else {
                    Err(Status::internal("Failed to delete vault"))
                }
            }
            LedgerResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("Unexpected response type")),
        }
    }

    async fn get_vault(
        &self,
        request: Request<GetVaultRequest>,
    ) -> Result<Response<GetVaultResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req
            .vault_id
            .as_ref()
            .map(|v| v.id)
            .ok_or_else(|| Status::invalid_argument("Missing vault_id"))?;

        // Get vault metadata and height
        let vault_meta = self.applied_state.get_vault(namespace_id, vault_id);
        let height = self.applied_state.vault_height(namespace_id, vault_id);

        match vault_meta {
            Some(_vault) => Ok(Response::new(GetVaultResponse {
                namespace_id: Some(NamespaceId { id: namespace_id }),
                vault_id: Some(VaultId { id: vault_id }),
                height,
                state_root: None,
                nodes: vec![],
                leader: None,
                status: crate::proto::VaultStatus::Active.into(),
                retention_policy: None,
            })),
            None => Err(Status::not_found("Vault not found")),
        }
    }

    async fn list_vaults(
        &self,
        _request: Request<ListVaultsRequest>,
    ) -> Result<Response<ListVaultsResponse>, Status> {
        // List all vaults across all namespaces
        let vaults = self
            .applied_state
            .all_vault_heights()
            .keys()
            .filter_map(|(ns_id, vault_id)| {
                self.applied_state.get_vault(*ns_id, *vault_id).map(|v| {
                    let height = self.applied_state.vault_height(v.namespace_id, v.vault_id);
                    crate::proto::GetVaultResponse {
                        namespace_id: Some(NamespaceId { id: v.namespace_id }),
                        vault_id: Some(VaultId { id: v.vault_id }),
                        height,
                        state_root: None,
                        nodes: vec![],
                        leader: None,
                        status: crate::proto::VaultStatus::Active.into(),
                        retention_policy: None,
                    }
                })
            })
            .collect();

        Ok(Response::new(ListVaultsResponse { vaults }))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let _req = request.into_inner();

        // Trigger Raft snapshot
        let _ = self
            .raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| Status::internal(format!("Snapshot error: {}", e)))?;

        // Get actual shard height from applied state
        let block_height = self.applied_state.shard_height();

        Ok(Response::new(CreateSnapshotResponse {
            block_height,
            state_root: None,
            snapshot_path: format!("/snapshots/snapshot_{}", chrono::Utc::now().timestamp()),
        }))
    }

    async fn check_integrity(
        &self,
        request: Request<CheckIntegrityRequest>,
    ) -> Result<Response<CheckIntegrityResponse>, Status> {
        let req = request.into_inner();
        let mut issues = Vec::new();

        let namespace_id = req.namespace_id.as_ref().map(|n| n.id);
        let vault_id = req.vault_id.as_ref().map(|v| v.id);

        // Get all vault heights to check
        let vault_heights: Vec<(i64, i64, u64)> =
            if let (Some(ns), Some(v)) = (namespace_id, vault_id) {
                // Specific vault
                let height = self.applied_state.vault_height(ns, v);
                if height > 0 {
                    vec![(ns, v, height)]
                } else {
                    vec![]
                }
            } else {
                // All vaults
                self.applied_state
                    .all_vault_heights()
                    .into_iter()
                    .map(|((ns, v), h)| (ns, v, h))
                    .collect()
            };

        if vault_heights.is_empty() {
            return Ok(Response::new(CheckIntegrityResponse {
                healthy: true,
                issues: vec![],
            }));
        }

        if req.full_check {
            // Full check: Replay blocks and verify state roots
            let archive = match &self.block_archive {
                Some(a) => a,
                None => {
                    return Err(Status::unavailable(
                        "Block archive not configured for full integrity check",
                    ));
                }
            };

            for (ns_id, v_id, expected_height) in &vault_heights {
                // Create temporary state for replay verification using temp directory
                let temp_dir = match TempDir::new() {
                    Ok(d) => d,
                    Err(e) => {
                        issues.push(IntegrityIssue {
                            block_height: 0,
                            issue_type: "internal_error".to_string(),
                            description: format!("Failed to create temp dir: {:?}", e),
                        });
                        continue;
                    }
                };
                let temp_db =
                    match Database::<FileBackend>::create(temp_dir.path().join("verify.db")) {
                        Ok(db) => Arc::new(db),
                        Err(e) => {
                            issues.push(IntegrityIssue {
                                block_height: 0,
                                issue_type: "internal_error".to_string(),
                                description: format!("Failed to create temp db: {:?}", e),
                            });
                            continue;
                        }
                    };
                let temp_state = StateLayer::new(temp_db);

                let mut last_vault_hash: Option<[u8; 32]> = None;

                // Replay all blocks for this vault
                for height in 1..=*expected_height {
                    let shard_height = match archive.find_shard_height(*ns_id, *v_id, height) {
                        Ok(Some(h)) => h,
                        Ok(None) => {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "missing_block".to_string(),
                                description: format!(
                                    "Block not found in archive: ns={}, vault={}, height={}",
                                    ns_id, v_id, height
                                ),
                            });
                            continue;
                        }
                        Err(e) => {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "archive_error".to_string(),
                                description: format!("Index lookup failed: {:?}", e),
                            });
                            continue;
                        }
                    };

                    let shard_block = match archive.read_block(shard_height) {
                        Ok(b) => b,
                        Err(e) => {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "read_error".to_string(),
                                description: format!("Block read failed: {:?}", e),
                            });
                            continue;
                        }
                    };

                    // Find the vault entry in this shard block
                    let vault_entry = shard_block.vault_entries.iter().find(|e| {
                        e.namespace_id == *ns_id && e.vault_id == *v_id && e.vault_height == height
                    });

                    let entry = match vault_entry {
                        Some(e) => e,
                        None => {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "missing_entry".to_string(),
                                description: format!(
                                    "Vault entry not found in shard block: ns={}, vault={}",
                                    ns_id, v_id
                                ),
                            });
                            continue;
                        }
                    };

                    // Verify chain continuity (previous_vault_hash)
                    if let Some(expected_prev) = last_vault_hash {
                        if entry.previous_vault_hash != expected_prev {
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
                        }
                        Err(e) => {
                            issues.push(IntegrityIssue {
                                block_height: height,
                                issue_type: "compute_error".to_string(),
                                description: format!("State root computation failed: {:?}", e),
                            });
                        }
                    }

                    // Track hash for next iteration's chain verification
                    // Compute vault block hash from entry
                    last_vault_hash = Some(compute_vault_block_hash(entry));
                }

                // Compare final replayed state root against current state
                let current_state = &*self.state;
                if let Ok(current_root) = current_state.compute_state_root(*v_id) {
                    if let Ok(replayed_root) = temp_state.compute_state_root(*v_id) {
                        if current_root != replayed_root {
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
                }
            }
        } else {
            // Quick check: Verify state roots can be computed without errors
            let state = &*self.state;

            for (_ns_id, v_id, height) in &vault_heights {
                match state.compute_state_root(*v_id) {
                    Ok(_root) => {
                        // State root computed successfully - state is internally consistent
                    }
                    Err(e) => {
                        issues.push(IntegrityIssue {
                            block_height: *height,
                            issue_type: "state_error".to_string(),
                            description: format!(
                                "Failed to compute state root for vault {}: {:?}",
                                v_id, e
                            ),
                        });
                    }
                }
            }
        }

        Ok(Response::new(CheckIntegrityResponse {
            healthy: issues.is_empty(),
            issues,
        }))
    }

    // =========================================================================
    // Cluster Membership
    // =========================================================================

    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterResponse>, Status> {
        let req = request.into_inner();

        // Get current metrics to check if we're the leader
        let metrics = self.raft.metrics().borrow().clone();
        let current_leader = metrics.current_leader;

        // If we're not the leader, return the leader info for redirect
        if current_leader != Some(metrics.id) {
            // Get leader address from membership
            let leader_addr = current_leader
                .and_then(|leader_id| {
                    metrics
                        .membership_config
                        .membership()
                        .nodes()
                        .find(|(id, _)| **id == leader_id)
                        .map(|(_, node)| node.addr.clone())
                })
                .unwrap_or_default();

            return Ok(Response::new(JoinClusterResponse {
                success: false,
                message: "Not the leader, redirect to leader".to_string(),
                leader_id: current_leader.unwrap_or(0),
                leader_address: leader_addr,
            }));
        }

        // We are the leader - add the new node as a learner first
        let node = BasicNode {
            addr: req.address.clone(),
        };

        // Check if node is already in the membership (idempotent handling)
        let current_membership = metrics.membership_config.membership();
        let already_voter = current_membership.voter_ids().any(|id| id == req.node_id);
        let already_in_membership = current_membership.nodes().any(|(id, _)| *id == req.node_id);

        if already_voter {
            // Already a voter, nothing to do
            return Ok(Response::new(JoinClusterResponse {
                success: true,
                message: "Node is already a voter in the cluster".to_string(),
                leader_id: metrics.id,
                leader_address: String::new(),
            }));
        }

        // Step 1: Add as learner if not already in membership
        // Use blocking=false so we don't wait for replication - the new node
        // might not be ready yet. We'll wait for the config change to commit below.
        // Retry with backoff if there's a pending config change.
        if !already_in_membership {
            let mut add_success = false;
            for attempt in 0..10 {
                match self
                    .raft
                    .add_learner(req.node_id, node.clone(), false)
                    .await
                {
                    Ok(_) => {
                        tracing::info!(node_id = req.node_id, "Initiated add_learner");
                        add_success = true;
                        break;
                    }
                    Err(e) => {
                        let err_str = format!("{}", e);
                        if err_str.contains("already undergoing a configuration change") {
                            tracing::info!(
                                node_id = req.node_id,
                                attempt,
                                "Config change in progress, retrying"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (attempt + 1) as u64,
                            ))
                            .await;
                        } else {
                            return Ok(Response::new(JoinClusterResponse {
                                success: false,
                                message: format!("Failed to add learner: {}", e),
                                leader_id: metrics.id,
                                leader_address: String::new(),
                            }));
                        }
                    }
                }
            }
            if !add_success {
                return Ok(Response::new(JoinClusterResponse {
                    success: false,
                    message: "Timeout: cluster membership changes not completing".to_string(),
                    leader_id: metrics.id,
                    leader_address: String::new(),
                }));
            }
        } else {
            tracing::info!(
                node_id = req.node_id,
                "Node already in membership as learner"
            );
        }

        // Step 2: Wait for the learner membership change to commit
        // OpenRaft requires serialized membership changes - we must wait for the
        // add_learner to commit before we can change_membership
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(5);

        loop {
            let fresh_metrics = self.raft.metrics().borrow().clone();
            let membership = fresh_metrics.membership_config.membership();

            // Check if the node is now in the membership as a learner
            let is_in_membership = membership.nodes().any(|(id, _)| *id == req.node_id);

            if is_in_membership {
                break;
            }

            if start.elapsed() > timeout {
                return Ok(Response::new(JoinClusterResponse {
                    success: false,
                    message: "Timeout waiting for learner membership to commit".to_string(),
                    leader_id: metrics.id,
                    leader_address: String::new(),
                }));
            }

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Step 3: Promote to voter by changing membership
        // Retry with backoff if there's still a pending config change
        for attempt in 0..10 {
            let fresh_metrics = self.raft.metrics().borrow().clone();
            let current_membership = fresh_metrics.membership_config.membership();
            let mut new_voters: BTreeSet<u64> = current_membership.voter_ids().collect();
            new_voters.insert(req.node_id);

            match self.raft.change_membership(new_voters, false).await {
                Ok(_) => {
                    tracing::info!(node_id = req.node_id, "Promoted node to voter");
                    return Ok(Response::new(JoinClusterResponse {
                        success: true,
                        message: "Node joined cluster successfully".to_string(),
                        leader_id: fresh_metrics.id,
                        leader_address: String::new(),
                    }));
                }
                Err(e) => {
                    let err_str = format!("{}", e);
                    if err_str.contains("already undergoing a configuration change") {
                        tracing::info!(
                            node_id = req.node_id,
                            attempt,
                            "Config change in progress for promotion, retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(
                            100 * (attempt + 1) as u64,
                        ))
                        .await;
                    } else {
                        return Ok(Response::new(JoinClusterResponse {
                            success: false,
                            message: format!("Failed to promote to voter: {}", e),
                            leader_id: fresh_metrics.id,
                            leader_address: String::new(),
                        }));
                    }
                }
            }
        }

        Ok(Response::new(JoinClusterResponse {
            success: false,
            message: "Timeout: could not complete voter promotion".to_string(),
            leader_id: metrics.id,
            leader_address: String::new(),
        }))
    }

    async fn leave_cluster(
        &self,
        request: Request<LeaveClusterRequest>,
    ) -> Result<Response<LeaveClusterResponse>, Status> {
        let req = request.into_inner();

        // Get current metrics
        let metrics = self.raft.metrics().borrow().clone();
        let current_leader = metrics.current_leader;

        // Only the leader can change membership
        if current_leader != Some(metrics.id) {
            return Ok(Response::new(LeaveClusterResponse {
                success: false,
                message: "Not the leader, cannot process leave request".to_string(),
            }));
        }

        // Remove the node from voters
        let current_membership = metrics.membership_config.membership();
        let new_voters: BTreeSet<u64> = current_membership
            .voter_ids()
            .filter(|id| *id != req.node_id)
            .collect();

        // Prevent removing the last voter
        if new_voters.is_empty() {
            return Ok(Response::new(LeaveClusterResponse {
                success: false,
                message: "Cannot remove the last voter from cluster".to_string(),
            }));
        }

        match self.raft.change_membership(new_voters, false).await {
            Ok(_) => {
                tracing::info!(node_id = req.node_id, "Removed node from cluster");
                Ok(Response::new(LeaveClusterResponse {
                    success: true,
                    message: "Node left cluster successfully".to_string(),
                }))
            }
            Err(e) => Ok(Response::new(LeaveClusterResponse {
                success: false,
                message: format!("Failed to remove node: {}", e),
            })),
        }
    }

    async fn get_cluster_info(
        &self,
        _request: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        let current_leader = metrics.current_leader;

        // Build member list from membership config
        let mut members = Vec::new();

        // Add voters
        for (node_id, node) in membership.nodes() {
            let is_voter = membership.voter_ids().any(|id| id == *node_id);
            members.push(ClusterMember {
                node_id: *node_id,
                address: node.addr.clone(),
                role: if is_voter {
                    ClusterMemberRole::Voter.into()
                } else {
                    ClusterMemberRole::Learner.into()
                },
                is_leader: current_leader == Some(*node_id),
            });
        }

        Ok(Response::new(GetClusterInfoResponse {
            members,
            leader_id: current_leader.unwrap_or(0),
            term: metrics.vote.leader_id().term,
        }))
    }

    // =========================================================================
    // Vault Recovery
    // =========================================================================

    async fn recover_vault(
        &self,
        request: Request<RecoverVaultRequest>,
    ) -> Result<Response<RecoverVaultResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("namespace_id required"))?;
        let vault_id = req
            .vault_id
            .as_ref()
            .map(|v| v.id)
            .ok_or_else(|| Status::invalid_argument("vault_id required"))?;

        // Check current vault health
        let current_health = self.applied_state.vault_health(namespace_id, vault_id);

        // Only recover diverged/recovering vaults unless force is set
        if !req.force {
            match &current_health {
                VaultHealthStatus::Healthy => {
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: "Vault is already healthy. Use force=true to recover anyway."
                            .to_string(),
                        health_status: VaultHealthProto::Healthy.into(),
                        final_height: self.applied_state.vault_height(namespace_id, vault_id),
                        final_state_root: None,
                    }));
                }
                VaultHealthStatus::Diverged { .. } | VaultHealthStatus::Recovering { .. } => {
                    // Proceed with recovery
                }
            }
        }

        // Require block archive for recovery
        let archive = match &self.block_archive {
            Some(a) => a,
            None => {
                return Err(Status::unavailable(
                    "Block archive not configured, cannot recover vault",
                ));
            }
        };

        // Get expected height from applied state
        let expected_height = self.applied_state.vault_height(namespace_id, vault_id);
        if expected_height == 0 {
            return Ok(Response::new(RecoverVaultResponse {
                success: false,
                message: "Vault has no blocks to recover".to_string(),
                health_status: VaultHealthProto::Healthy.into(),
                final_height: 0,
                final_state_root: None,
            }));
        }

        tracing::info!(
            namespace_id,
            vault_id,
            expected_height,
            "Starting vault recovery"
        );

        // Step 1: Clear vault state
        {
            let state = &*self.state;
            if let Err(e) = state.clear_vault(vault_id) {
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
        let mut last_vault_hash: Option<[u8; 32]> = None;
        let mut divergence_detected = false;
        let mut final_state_root = ZERO_HASH;

        for height in 1..=expected_height {
            // Find shard height for this vault height
            let shard_height = match archive.find_shard_height(namespace_id, vault_id, height) {
                Ok(Some(h)) => h,
                Ok(None) => {
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: format!(
                            "Block not found in archive: ns={}, vault={}, height={}",
                            namespace_id, vault_id, height
                        ),
                        health_status: VaultHealthProto::Diverged.into(),
                        final_height: height - 1,
                        final_state_root: Some(Hash {
                            value: final_state_root.to_vec(),
                        }),
                    }));
                }
                Err(e) => {
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: format!("Index lookup failed at height {}: {:?}", height, e),
                        health_status: VaultHealthProto::Diverged.into(),
                        final_height: height - 1,
                        final_state_root: Some(Hash {
                            value: final_state_root.to_vec(),
                        }),
                    }));
                }
            };

            // Read the shard block
            let shard_block = match archive.read_block(shard_height) {
                Ok(b) => b,
                Err(e) => {
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: format!("Block read failed at height {}: {:?}", height, e),
                        health_status: VaultHealthProto::Diverged.into(),
                        final_height: height - 1,
                        final_state_root: Some(Hash {
                            value: final_state_root.to_vec(),
                        }),
                    }));
                }
            };

            // Find the vault entry
            let entry = match shard_block.vault_entries.iter().find(|e| {
                e.namespace_id == namespace_id && e.vault_id == vault_id && e.vault_height == height
            }) {
                Some(e) => e,
                None => {
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: format!(
                            "Vault entry not found in shard block at height {}",
                            height
                        ),
                        health_status: VaultHealthProto::Diverged.into(),
                        final_height: height - 1,
                        final_state_root: Some(Hash {
                            value: final_state_root.to_vec(),
                        }),
                    }));
                }
            };

            // Verify chain continuity
            if let Some(expected_prev) = last_vault_hash {
                if entry.previous_vault_hash != expected_prev {
                    tracing::warn!(
                        height,
                        "Chain break detected during recovery: expected {:x?}, got {:x?}",
                        &expected_prev[..8],
                        &entry.previous_vault_hash[..8]
                    );
                }
            }

            // Apply transactions
            {
                let state = &*self.state;
                for tx in &entry.transactions {
                    if let Err(e) = state.apply_operations(vault_id, &tx.operations, height) {
                        return Ok(Response::new(RecoverVaultResponse {
                            success: false,
                            message: format!(
                                "Transaction apply failed at height {}: {:?}",
                                height, e
                            ),
                            health_status: VaultHealthProto::Diverged.into(),
                            final_height: height - 1,
                            final_state_root: Some(Hash {
                                value: final_state_root.to_vec(),
                            }),
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
                    }
                    Err(e) => {
                        return Ok(Response::new(RecoverVaultResponse {
                            success: false,
                            message: format!(
                                "State root computation failed at height {}: {:?}",
                                height, e
                            ),
                            health_status: VaultHealthProto::Diverged.into(),
                            final_height: height - 1,
                            final_state_root: Some(Hash {
                                value: final_state_root.to_vec(),
                            }),
                        }));
                    }
                }
            }

            // Track hash for next iteration
            last_vault_hash = Some(compute_vault_block_hash(entry));
        }

        // Step 3: Update vault health based on recovery result via Raft
        if divergence_detected {
            tracing::error!(
                namespace_id,
                vault_id,
                "Recovery reproduced divergence - possible determinism bug"
            );

            // Update vault health to Diverged via Raft for cluster-wide consistency
            let health_request = LedgerRequest::UpdateVaultHealth {
                namespace_id,
                vault_id,
                healthy: false,
                expected_root: None, // Already diverged during recovery
                computed_root: Some(final_state_root),
                diverged_at_height: Some(expected_height),
                recovery_attempt: None,
                recovery_started_at: None,
            };

            if let Err(e) = self.raft.client_write(health_request).await {
                tracing::error!("Failed to update vault health via Raft: {}", e);
                // Continue with response - the local state will be inconsistent but
                // the next recovery attempt can retry
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
            tracing::info!(
                namespace_id,
                vault_id,
                expected_height,
                "Vault recovery successful"
            );

            // Update vault health to Healthy via Raft for cluster-wide consistency
            let health_request = LedgerRequest::UpdateVaultHealth {
                namespace_id,
                vault_id,
                healthy: true,
                expected_root: None,
                computed_root: None,
                diverged_at_height: None,
                recovery_attempt: None,
                recovery_started_at: None,
            };

            if let Err(e) = self.raft.client_write(health_request).await {
                tracing::error!("Failed to update vault health via Raft: {}", e);
                // The vault was successfully recovered locally - log error but return success
            }

            Ok(Response::new(RecoverVaultResponse {
                success: true,
                message: "Vault recovered successfully".to_string(),
                health_status: VaultHealthProto::Healthy.into(),
                final_height: expected_height,
                final_state_root: Some(Hash {
                    value: final_state_root.to_vec(),
                }),
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
        request: Request<crate::proto::SimulateDivergenceRequest>,
    ) -> Result<Response<crate::proto::SimulateDivergenceResponse>, Status> {
        let req = request.into_inner();

        let namespace_id = req
            .namespace_id
            .as_ref()
            .map(|n| n.id)
            .ok_or_else(|| Status::invalid_argument("Missing namespace_id"))?;

        let vault_id = req
            .vault_id
            .as_ref()
            .map(|v| v.id)
            .ok_or_else(|| Status::invalid_argument("Missing vault_id"))?;

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
            self.applied_state
                .vault_height(namespace_id, vault_id)
                .max(1)
        };

        tracing::warn!(
            namespace_id,
            vault_id,
            at_height,
            "Simulating vault divergence for testing"
        );

        // Update vault health to Diverged via Raft
        let health_request = LedgerRequest::UpdateVaultHealth {
            namespace_id,
            vault_id,
            healthy: false,
            expected_root: Some(expected_root),
            computed_root: Some(computed_root),
            diverged_at_height: Some(at_height),
            recovery_attempt: None,
            recovery_started_at: None,
        };

        match self.raft.client_write(health_request).await {
            Ok(_) => {
                tracing::info!(
                    namespace_id,
                    vault_id,
                    "Vault marked as diverged for testing"
                );

                Ok(Response::new(crate::proto::SimulateDivergenceResponse {
                    success: true,
                    message: format!(
                        "Vault {}:{} marked as diverged at height {}",
                        namespace_id, vault_id, at_height
                    ),
                    health_status: VaultHealthProto::Diverged.into(),
                }))
            }
            Err(e) => {
                tracing::error!(
                    namespace_id,
                    vault_id,
                    error = %e,
                    "Failed to simulate divergence"
                );

                Err(Status::internal(format!(
                    "Failed to update vault health: {}",
                    e
                )))
            }
        }
    }

    async fn force_gc(
        &self,
        request: Request<crate::proto::ForceGcRequest>,
    ) -> Result<Response<crate::proto::ForceGcResponse>, Status> {
        let req = request.into_inner();

        // Check if this node is the leader
        let metrics = self.raft.metrics().borrow().clone();
        let node_id = metrics.id;
        if metrics.current_leader != Some(node_id) {
            return Err(Status::failed_precondition(
                "Only the leader can run garbage collection",
            ));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut total_expired = 0u64;
        let mut vaults_scanned = 0u64;

        // Determine which vaults to scan
        let vault_heights: Vec<((i64, i64), u64)> =
            if let (Some(ns), Some(v)) = (req.namespace_id, req.vault_id) {
                // Single vault
                let height = self.applied_state.vault_height(ns.id, v.id);
                vec![((ns.id, v.id), height)]
            } else {
                // All vaults
                self.applied_state.all_vault_heights().into_iter().collect()
            };

        for ((namespace_id, vault_id), _height) in vault_heights {
            vaults_scanned += 1;

            // Find expired entities in this vault
            let expired = {
                let state = &*self.state;
                match state.list_entities(vault_id, None, None, 1000) {
                    Ok(entities) => entities
                        .into_iter()
                        .filter(|e| e.expires_at > 0 && e.expires_at < now)
                        .map(|e| {
                            let key = String::from_utf8_lossy(&e.key).to_string();
                            (key, e.expires_at)
                        })
                        .collect::<Vec<_>>(),
                    Err(e) => {
                        tracing::warn!(namespace_id, vault_id, error = %e, "Failed to list entities for GC");
                        continue;
                    }
                }
            };

            if expired.is_empty() {
                continue;
            }

            let count = expired.len();

            // Create ExpireEntity operations
            let operations: Vec<inferadb_ledger_types::Operation> = expired
                .iter()
                .map(
                    |(key, expired_at)| inferadb_ledger_types::Operation::ExpireEntity {
                        key: key.clone(),
                        expired_at: *expired_at,
                    },
                )
                .collect();

            let transaction = inferadb_ledger_types::Transaction {
                id: *uuid::Uuid::new_v4().as_bytes(),
                client_id: "system:gc".to_string(),
                sequence: now, // Use timestamp as sequence for GC
                operations,
                timestamp: chrono::Utc::now(),
                actor: "system:gc".to_string(),
            };

            let gc_request = LedgerRequest::Write {
                namespace_id,
                vault_id,
                transactions: vec![transaction],
            };

            match self.raft.client_write(gc_request).await {
                Ok(_) => {
                    total_expired += count as u64;
                    tracing::info!(
                        namespace_id,
                        vault_id,
                        count,
                        "GC expired entities via ForceGc"
                    );
                }
                Err(e) => {
                    tracing::warn!(namespace_id, vault_id, error = %e, "GC write failed");
                }
            }
        }

        Ok(Response::new(crate::proto::ForceGcResponse {
            success: true,
            message: format!(
                "GC cycle complete: {} expired entities removed from {} vaults",
                total_expired, vaults_scanned
            ),
            expired_count: total_expired,
            vaults_scanned,
        }))
    }
}

/// Compute the hash of a vault block entry for chain verification.
///
/// The vault block hash commits to all content: height, previous hash,
/// transactions (via tx_merkle_root), and state root.
fn compute_vault_block_hash(entry: &VaultEntry) -> [u8; 32] {
    let mut hasher = Sha256::new();

    // Hash the vault block header fields
    hasher.update(entry.namespace_id.to_le_bytes());
    hasher.update(entry.vault_id.to_le_bytes());
    hasher.update(entry.vault_height.to_le_bytes());
    hasher.update(entry.previous_vault_hash);
    hasher.update(entry.tx_merkle_root);
    hasher.update(entry.state_root);

    hasher.finalize().into()
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    clippy::panic
)]
mod tests {
    use super::*;

    // =========================================================================
    // compute_vault_block_hash Tests
    // =========================================================================

    #[test]
    fn test_vault_block_hash_deterministic() {
        // Same input should always produce the same hash
        let entry = VaultEntry {
            namespace_id: 1,
            vault_id: 2,
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
    fn test_vault_block_hash_different_for_different_inputs() {
        let entry1 = VaultEntry {
            namespace_id: 1,
            vault_id: 2,
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
        };

        let entry2 = VaultEntry {
            namespace_id: 1,
            vault_id: 2,
            vault_height: 11, // Different height
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
        };

        let hash1 = compute_vault_block_hash(&entry1);
        let hash2 = compute_vault_block_hash(&entry2);

        assert_ne!(
            hash1, hash2,
            "Different inputs should produce different hashes"
        );
    }

    #[test]
    fn test_vault_block_hash_different_state_root() {
        let entry1 = VaultEntry {
            namespace_id: 1,
            vault_id: 2,
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
        };

        let entry2 = VaultEntry {
            namespace_id: 1,
            vault_id: 2,
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [1u8; 32], // Different state root
        };

        let hash1 = compute_vault_block_hash(&entry1);
        let hash2 = compute_vault_block_hash(&entry2);

        assert_ne!(
            hash1, hash2,
            "Different state_root should produce different hash"
        );
    }

    #[test]
    fn test_vault_block_hash_chain_continuity() {
        // Simulate a chain of blocks
        let entry1 = VaultEntry {
            namespace_id: 1,
            vault_id: 1,
            vault_height: 1,
            previous_vault_hash: ZERO_HASH, // Genesis block
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [1u8; 32],
        };

        let hash1 = compute_vault_block_hash(&entry1);

        let entry2 = VaultEntry {
            namespace_id: 1,
            vault_id: 1,
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
            namespace_id: 1,
            vault_id: 1,
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
    fn test_vault_block_hash_includes_all_fields() {
        let base_entry = VaultEntry {
            namespace_id: 1,
            vault_id: 2,
            vault_height: 3,
            previous_vault_hash: [4u8; 32],
            transactions: vec![],
            tx_merkle_root: [5u8; 32],
            state_root: [6u8; 32],
        };

        let base_hash = compute_vault_block_hash(&base_entry);

        // Changing namespace_id should change hash
        let mut modified = base_entry.clone();
        modified.namespace_id = 99;
        assert_ne!(
            compute_vault_block_hash(&modified),
            base_hash,
            "namespace_id affects hash"
        );

        // Changing vault_id should change hash
        let mut modified = base_entry.clone();
        modified.vault_id = 99;
        assert_ne!(
            compute_vault_block_hash(&modified),
            base_hash,
            "vault_id affects hash"
        );

        // Changing tx_merkle_root should change hash
        let mut modified = base_entry.clone();
        modified.tx_merkle_root = [99u8; 32];
        assert_ne!(
            compute_vault_block_hash(&modified),
            base_hash,
            "tx_merkle_root affects hash"
        );
    }
}
