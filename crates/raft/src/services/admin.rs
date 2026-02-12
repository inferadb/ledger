//! Admin service implementation.
//!
//! Handles namespace and vault management, cluster membership, snapshots, and integrity checks.

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc, time::Duration};

use inferadb_ledger_proto::proto::{
    BackupInfo, BlockHeader, CheckIntegrityRequest, CheckIntegrityResponse, ClusterMember,
    ClusterMemberRole, CreateBackupRequest, CreateBackupResponse, CreateNamespaceRequest,
    CreateNamespaceResponse, CreateSnapshotRequest, CreateSnapshotResponse, CreateVaultRequest,
    CreateVaultResponse, DeleteNamespaceRequest, DeleteNamespaceResponse, DeleteVaultRequest,
    DeleteVaultResponse, GetClusterInfoRequest, GetClusterInfoResponse, GetConfigRequest,
    GetConfigResponse, GetNamespaceRequest, GetNamespaceResponse, GetNodeInfoRequest,
    GetNodeInfoResponse, GetVaultRequest, GetVaultResponse, Hash, IntegrityIssue,
    JoinClusterRequest, JoinClusterResponse, LeaveClusterRequest, LeaveClusterResponse,
    ListBackupsRequest, ListBackupsResponse, ListNamespacesRequest, ListNamespacesResponse,
    ListVaultsRequest, ListVaultsResponse, NamespaceId, NodeId, RecoverVaultRequest,
    RecoverVaultResponse, RestoreBackupRequest, RestoreBackupResponse, ShardId,
    UpdateConfigRequest, UpdateConfigResponse, VaultHealthProto, VaultId,
    admin_service_server::AdminService,
};
use inferadb_ledger_state::{BlockArchive, StateLayer};
use inferadb_ledger_store::{Database, FileBackend};
use inferadb_ledger_types::{
    NamespaceId as DomainNamespaceId, ShardId as DomainShardId, VaultEntry,
    VaultId as DomainVaultId, ZERO_HASH,
    audit::{AuditAction, AuditEvent, AuditOutcome, AuditResource},
    config::ValidationConfig,
    validation,
};
use openraft::{BasicNode, Raft};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tonic::{Request, Response, Status};

use crate::{
    error::{ServiceError, classify_raft_error},
    log_storage::{AppliedStateAccessor, VaultHealthStatus},
    metrics, trace_context,
    types::{
        BlockRetentionMode, BlockRetentionPolicy, LedgerRequest, LedgerResponse, LedgerTypeConfig,
    },
    wide_events::{OperationType, RequestContext, Sampler},
};

/// Handles namespace and vault lifecycle, cluster membership, snapshots, and runtime configuration
/// via Raft consensus.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct AdminServiceImpl {
    /// Raft consensus handle for proposing admin operations.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// State layer for entity and relationship reads during admin operations.
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (vault heights, health).
    applied_state: AppliedStateAccessor,
    /// Block archive for integrity verification.
    #[builder(default)]
    block_archive: Option<Arc<BlockArchive<FileBackend>>>,
    /// The node's listen address (for GetNodeInfo RPC).
    listen_addr: SocketAddr,
    /// Sampler for wide events tail sampling.
    #[builder(default)]
    sampler: Option<Sampler>,
    /// Node ID for wide events system context.
    #[builder(default)]
    node_id: Option<u64>,
    /// Audit logger for compliance-ready event tracking.
    #[builder(default)]
    audit_logger: Option<Arc<dyn crate::audit::AuditLogger>>,
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
    runtime_config: Option<crate::runtime_config::RuntimeConfigHandle>,
    /// Rate limiter for propagating config changes.
    #[builder(default)]
    rate_limiter: Option<Arc<crate::rate_limit::RateLimiter>>,
    /// Hot key detector for propagating config changes.
    #[builder(default)]
    hot_key_detector: Option<Arc<crate::hot_key_detector::HotKeyDetector>>,
    /// Backup manager for backup and restore operations.
    #[builder(default)]
    backup_manager: Option<Arc<crate::backup::BackupManager>>,
    /// Snapshot manager for reading Raft snapshots during backup.
    #[builder(default)]
    snapshot_manager: Option<Arc<inferadb_ledger_state::SnapshotManager>>,
    /// Per-namespace resource quota checker.
    #[builder(default)]
    quota_checker: Option<crate::quota::QuotaChecker>,
}

impl AdminServiceImpl {
    /// Adds audit logger for compliance event tracking.
    #[must_use]
    pub fn with_audit_logger(mut self, logger: Arc<dyn crate::audit::AuditLogger>) -> Self {
        self.audit_logger = Some(logger);
        self
    }

    /// Sets input validation configuration for request field limits.
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

    /// Sets the runtime configuration handle for hot-reloadable settings.
    #[must_use]
    pub fn with_runtime_config(
        mut self,
        handle: crate::runtime_config::RuntimeConfigHandle,
        rate_limiter: Option<Arc<crate::rate_limit::RateLimiter>>,
        hot_key_detector: Option<Arc<crate::hot_key_detector::HotKeyDetector>>,
    ) -> Self {
        self.runtime_config = Some(handle);
        self.rate_limiter = rate_limiter;
        self.hot_key_detector = hot_key_detector;
        self
    }

    /// Sets the backup manager and snapshot manager for backup/restore operations.
    #[must_use]
    pub fn with_backup(
        mut self,
        backup_manager: Arc<crate::backup::BackupManager>,
        snapshot_manager: Arc<inferadb_ledger_state::SnapshotManager>,
    ) -> Self {
        self.backup_manager = Some(backup_manager);
        self.snapshot_manager = Some(snapshot_manager);
        self
    }

    /// Sets the per-namespace resource quota checker.
    #[must_use]
    pub fn with_quota_checker(mut self, checker: crate::quota::QuotaChecker) -> Self {
        self.quota_checker = Some(checker);
        self
    }

    /// Builds an audit event for an admin operation.
    fn build_audit_event(
        &self,
        action: AuditAction,
        principal: &str,
        resource: AuditResource,
        outcome: AuditOutcome,
        trace_id: Option<&str>,
    ) -> AuditEvent {
        super::helpers::build_audit_event(
            action,
            principal,
            resource,
            outcome,
            self.node_id,
            trace_id,
            None,
        )
    }

    /// Emits an audit event and records the corresponding Prometheus metric.
    fn emit_audit_event(&self, event: &AuditEvent) {
        super::helpers::emit_audit_event(self.audit_logger.as_ref(), event);
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        crate::deadline::check_near_deadline(&request)?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "create_namespace");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("create_namespace");
        ctx.set_target_namespace_name(&req.name);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Validate namespace name
        validation::validate_namespace_name(&req.name, &self.validation_config)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Submit create namespace through Raft
        // Map proto ShardId to domain ShardId newtype
        // Convert proto quota to domain type
        let quota = req.quota.map(|q| inferadb_ledger_types::config::NamespaceQuota {
            max_storage_bytes: q.max_storage_bytes,
            max_vaults: q.max_vaults,
            max_write_ops_per_sec: q.max_write_ops_per_sec,
            max_read_ops_per_sec: q.max_read_ops_per_sec,
        });

        let ledger_request = LedgerRequest::CreateNamespace {
            name: req.name,
            shard_id: req.shard_id.map(|s| DomainShardId::new(s.id)),
            quota,
        };

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout = crate::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        ctx.start_raft_timer();
        let result =
            match tokio::time::timeout(timeout, self.raft.client_write(ledger_request)).await {
                Ok(Ok(result)) => {
                    ctx.end_raft_timer();
                    result
                },
                Ok(Err(e)) => {
                    ctx.end_raft_timer();
                    ctx.set_error("RaftError", &e.to_string());
                    return Err(ServiceError::raft(e).into());
                },
                Err(_elapsed) => {
                    ctx.end_raft_timer();
                    crate::metrics::record_raft_proposal_timeout();
                    ctx.set_error("Timeout", "Raft proposal timed out");
                    return Err(Status::deadline_exceeded(format!(
                        "Raft proposal timed out after {}ms",
                        timeout.as_millis()
                    )));
                },
            };

        match result.data {
            LedgerResponse::NamespaceCreated { namespace_id, shard_id } => {
                ctx.set_namespace_id(namespace_id.value());
                ctx.set_success();
                metrics::record_namespace_operation(namespace_id.value(), "admin");
                metrics::record_namespace_latency(
                    namespace_id.value(),
                    "admin",
                    ctx.elapsed_secs(),
                );
                self.emit_audit_event(
                    &self.build_audit_event(
                        AuditAction::CreateNamespace,
                        "system",
                        AuditResource::namespace(namespace_id)
                            .with_detail(format!("shard:{}", shard_id.value())),
                        AuditOutcome::Success,
                        Some(&trace_ctx.trace_id),
                    ),
                );
                Ok(Response::new(CreateNamespaceResponse {
                    namespace_id: Some(NamespaceId { id: namespace_id.value() }),
                    shard_id: Some(ShardId { id: shard_id.value() }),
                }))
            },
            LedgerResponse::Error { message } => {
                ctx.set_error("Unspecified", &message);
                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::CreateNamespace,
                    "system",
                    AuditResource::cluster(),
                    AuditOutcome::Failed {
                        code: "Unspecified".to_string(),
                        detail: message.clone(),
                    },
                    Some(&trace_ctx.trace_id),
                ));
                Err(Status::internal(message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        crate::deadline::check_near_deadline(&request)?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "delete_namespace");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("delete_namespace");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        let namespace_id =
            req.namespace_id.as_ref().map(|n| DomainNamespaceId::new(n.id)).ok_or_else(|| {
                ctx.set_error("InvalidArgument", "Missing namespace_id");
                ServiceError::invalid_arg("Missing namespace_id")
            })?;

        ctx.set_namespace_id(namespace_id.value());

        // Submit delete namespace through Raft
        let ledger_request = LedgerRequest::DeleteNamespace { namespace_id };

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout = crate::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        ctx.start_raft_timer();
        let result =
            match tokio::time::timeout(timeout, self.raft.client_write(ledger_request)).await {
                Ok(Ok(result)) => {
                    ctx.end_raft_timer();
                    result
                },
                Ok(Err(e)) => {
                    ctx.end_raft_timer();
                    ctx.set_error("RaftError", &e.to_string());
                    return Err(ServiceError::raft(e).into());
                },
                Err(_elapsed) => {
                    ctx.end_raft_timer();
                    crate::metrics::record_raft_proposal_timeout();
                    ctx.set_error("Timeout", "Raft proposal timed out");
                    return Err(Status::deadline_exceeded(format!(
                        "Raft proposal timed out after {}ms",
                        timeout.as_millis()
                    )));
                },
            };

        match result.data {
            LedgerResponse::NamespaceDeleted { success, blocking_vault_ids } => {
                if success {
                    ctx.set_success();
                    metrics::record_namespace_operation(namespace_id.value(), "admin");
                    metrics::record_namespace_latency(
                        namespace_id.value(),
                        "admin",
                        ctx.elapsed_secs(),
                    );
                    self.emit_audit_event(&self.build_audit_event(
                        AuditAction::DeleteNamespace,
                        "system",
                        AuditResource::namespace(namespace_id),
                        AuditOutcome::Success,
                        Some(&trace_ctx.trace_id),
                    ));
                    Ok(Response::new(DeleteNamespaceResponse {
                        deleted_at: Some(
                            prost_types::Timestamp::from(std::time::SystemTime::now()),
                        ),
                    }))
                } else {
                    let vault_ids: Vec<String> =
                        blocking_vault_ids.iter().map(|id| id.value().to_string()).collect();
                    ctx.set_error("PreconditionFailed", "Namespace has active vaults");
                    self.emit_audit_event(&self.build_audit_event(
                        AuditAction::DeleteNamespace,
                        "system",
                        AuditResource::namespace(namespace_id),
                        AuditOutcome::Failed {
                            code: "PreconditionFailed".to_string(),
                            detail: "Namespace has active vaults".to_string(),
                        },
                        Some(&trace_ctx.trace_id),
                    ));
                    Err(ServiceError::precondition(format!(
                        "Namespace has active vaults that must be deleted first: [{}]",
                        vault_ids.join(", ")
                    ))
                    .into())
                }
            },
            LedgerResponse::Error { message } => {
                ctx.set_error("Unspecified", &message);
                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::DeleteNamespace,
                    "system",
                    AuditResource::namespace(namespace_id),
                    AuditOutcome::Failed {
                        code: "Unspecified".to_string(),
                        detail: message.clone(),
                    },
                    Some(&trace_ctx.trace_id),
                ));
                Err(Status::internal(message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    async fn get_namespace(
        &self,
        request: Request<GetNamespaceRequest>,
    ) -> Result<Response<GetNamespaceResponse>, Status> {
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "get_namespace");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("get_namespace");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Extract namespace from lookup oneof (by ID or name)
        let ns_meta = match req.lookup {
            Some(inferadb_ledger_proto::proto::get_namespace_request::Lookup::NamespaceId(n)) => {
                ctx.set_namespace_id(n.id);
                self.applied_state.get_namespace(DomainNamespaceId::new(n.id))
            },
            Some(inferadb_ledger_proto::proto::get_namespace_request::Lookup::Name(name)) => {
                ctx.set_target_namespace_name(&name);
                self.applied_state.get_namespace_by_name(&name)
            },
            None => {
                ctx.set_error("InvalidArgument", "Missing namespace lookup");
                return Err(ServiceError::invalid_arg("Missing namespace lookup").into());
            },
        };

        match ns_meta {
            Some(ns) => {
                ctx.set_namespace_id(ns.namespace_id.value());
                ctx.set_success();
                let status = crate::proto_compat::namespace_status_to_proto(ns.status);
                Ok(Response::new(GetNamespaceResponse {
                    namespace_id: Some(NamespaceId { id: ns.namespace_id.value() }),
                    name: ns.name,
                    shard_id: Some(ShardId { id: ns.shard_id.value() }),
                    member_nodes: vec![],
                    status: status.into(),
                    config_version: 0,
                    created_at: None,
                }))
            },
            None => {
                ctx.set_error("NotFound", "Namespace not found");
                Err(ServiceError::not_found("Namespace", "unknown").into())
            },
        }
    }

    async fn list_namespaces(
        &self,
        _request: Request<ListNamespacesRequest>,
    ) -> Result<Response<ListNamespacesResponse>, Status> {
        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "list_namespaces");
        ctx.set_operation_type(OperationType::Admin);
        ctx.set_admin_action("list_namespaces");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        let namespaces = self
            .applied_state
            .list_namespaces()
            .into_iter()
            .map(|ns| {
                let status = crate::proto_compat::namespace_status_to_proto(ns.status);
                inferadb_ledger_proto::proto::GetNamespaceResponse {
                    namespace_id: Some(NamespaceId { id: ns.namespace_id.value() }),
                    name: ns.name,
                    shard_id: Some(ShardId { id: ns.shard_id.value() }),
                    member_nodes: vec![],
                    status: status.into(),
                    config_version: 0,
                    created_at: None,
                }
            })
            .collect::<Vec<_>>();

        // Set count in context for observability
        ctx.set_keys_count(namespaces.len());
        ctx.set_success();

        Ok(Response::new(ListNamespacesResponse { namespaces, next_page_token: None }))
    }

    async fn create_vault(
        &self,
        request: Request<CreateVaultRequest>,
    ) -> Result<Response<CreateVaultResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        crate::deadline::check_near_deadline(&request)?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "create_vault");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("create_vault");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        let namespace_id =
            req.namespace_id.as_ref().map(|n| DomainNamespaceId::new(n.id)).ok_or_else(|| {
                ctx.set_error("InvalidArgument", "Missing namespace_id");
                Status::invalid_argument("Missing namespace_id")
            })?;

        ctx.set_namespace_id(namespace_id.value());

        // Check vault count quota before submitting to Raft
        super::helpers::check_vault_quota(self.quota_checker.as_ref(), namespace_id).map_err(
            |status| {
                super::metadata::status_with_correlation(
                    status,
                    &ctx.request_id(),
                    &trace_ctx.trace_id,
                )
            },
        )?;

        // Convert proto retention policy to internal type
        let retention_policy = req.retention_policy.map(|proto_policy| {
            let policy = BlockRetentionPolicy::from(&proto_policy);
            match policy.mode {
                BlockRetentionMode::Full => ctx.set_retention_mode("full"),
                BlockRetentionMode::Compacted => ctx.set_retention_mode("compacted"),
            }
            policy
        });

        // Submit create vault through Raft
        let ledger_request = LedgerRequest::CreateVault {
            namespace_id,
            name: None, // CreateVaultRequest doesn't have name field
            retention_policy,
        };

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout = crate::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        ctx.start_raft_timer();
        let result =
            match tokio::time::timeout(timeout, self.raft.client_write(ledger_request)).await {
                Ok(Ok(result)) => {
                    ctx.end_raft_timer();
                    result
                },
                Ok(Err(e)) => {
                    ctx.end_raft_timer();
                    ctx.set_error("RaftError", &e.to_string());
                    return Err(classify_raft_error(&e.to_string()));
                },
                Err(_elapsed) => {
                    ctx.end_raft_timer();
                    crate::metrics::record_raft_proposal_timeout();
                    ctx.set_error("Timeout", "Raft proposal timed out");
                    return Err(Status::deadline_exceeded(format!(
                        "Raft proposal timed out after {}ms",
                        timeout.as_millis()
                    )));
                },
            };

        match result.data {
            LedgerResponse::VaultCreated { vault_id } => {
                ctx.set_vault_id(vault_id.value());
                ctx.set_success();
                metrics::record_namespace_operation(namespace_id.value(), "admin");
                metrics::record_namespace_latency(
                    namespace_id.value(),
                    "admin",
                    ctx.elapsed_secs(),
                );
                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::CreateVault,
                    "system",
                    AuditResource::vault(namespace_id, vault_id),
                    AuditOutcome::Success,
                    Some(&trace_ctx.trace_id),
                ));

                // Get Raft metrics for leader_id and term
                let metrics = self.raft.metrics().borrow().clone();
                let leader_id = metrics.current_leader.unwrap_or(metrics.id);
                ctx.set_raft_term(metrics.current_term);

                // Compute empty state root for genesis block
                let state = &*self.state;
                let state_root = state.compute_state_root(vault_id).unwrap_or(ZERO_HASH);

                // Build genesis block header (height 0)
                let genesis = BlockHeader {
                    height: 0,
                    namespace_id: Some(NamespaceId { id: namespace_id.value() }),
                    vault_id: Some(VaultId { id: vault_id.value() }),
                    previous_hash: Some(Hash { value: ZERO_HASH.to_vec() }),
                    tx_merkle_root: Some(Hash {
                        value: ZERO_HASH.to_vec(), // Empty transaction list
                    }),
                    state_root: Some(Hash { value: state_root.to_vec() }),
                    timestamp: Some(prost_types::Timestamp {
                        seconds: chrono::Utc::now().timestamp(),
                        nanos: 0,
                    }),
                    leader_id: Some(NodeId { id: leader_id.to_string() }),
                    term: metrics.current_term,
                    committed_index: result.log_id.index,
                };

                Ok(Response::new(CreateVaultResponse {
                    vault_id: Some(VaultId { id: vault_id.value() }),
                    genesis: Some(genesis),
                }))
            },
            LedgerResponse::Error { message } => {
                ctx.set_error("Unspecified", &message);
                Err(Status::internal(message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    async fn delete_vault(
        &self,
        request: Request<DeleteVaultRequest>,
    ) -> Result<Response<DeleteVaultResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        crate::deadline::check_near_deadline(&request)?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "delete_vault");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("delete_vault");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        let namespace_id =
            req.namespace_id.as_ref().map(|n| DomainNamespaceId::new(n.id)).ok_or_else(|| {
                ctx.set_error("InvalidArgument", "Missing namespace_id");
                Status::invalid_argument("Missing namespace_id")
            })?;

        let vault_id =
            req.vault_id.as_ref().map(|v| DomainVaultId::new(v.id)).ok_or_else(|| {
                ctx.set_error("InvalidArgument", "Missing vault_id");
                Status::invalid_argument("Missing vault_id")
            })?;

        ctx.set_target(namespace_id.value(), vault_id.value());

        // Submit delete vault through Raft
        let ledger_request = LedgerRequest::DeleteVault { namespace_id, vault_id };

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout = crate::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        ctx.start_raft_timer();
        let result =
            match tokio::time::timeout(timeout, self.raft.client_write(ledger_request)).await {
                Ok(Ok(result)) => {
                    ctx.end_raft_timer();
                    result
                },
                Ok(Err(e)) => {
                    ctx.end_raft_timer();
                    ctx.set_error("RaftError", &e.to_string());
                    return Err(classify_raft_error(&e.to_string()));
                },
                Err(_elapsed) => {
                    ctx.end_raft_timer();
                    crate::metrics::record_raft_proposal_timeout();
                    ctx.set_error("Timeout", "Raft proposal timed out");
                    return Err(Status::deadline_exceeded(format!(
                        "Raft proposal timed out after {}ms",
                        timeout.as_millis()
                    )));
                },
            };

        match result.data {
            LedgerResponse::VaultDeleted { success } => {
                if success {
                    ctx.set_success();
                    metrics::record_namespace_operation(namespace_id.value(), "admin");
                    metrics::record_namespace_latency(
                        namespace_id.value(),
                        "admin",
                        ctx.elapsed_secs(),
                    );
                    self.emit_audit_event(&self.build_audit_event(
                        AuditAction::DeleteVault,
                        "system",
                        AuditResource::vault(namespace_id, vault_id),
                        AuditOutcome::Success,
                        Some(&trace_ctx.trace_id),
                    ));
                    Ok(Response::new(DeleteVaultResponse {
                        deleted_at: Some(
                            prost_types::Timestamp::from(std::time::SystemTime::now()),
                        ),
                    }))
                } else {
                    ctx.set_error("DeleteFailed", "Failed to delete vault");
                    self.emit_audit_event(&self.build_audit_event(
                        AuditAction::DeleteVault,
                        "system",
                        AuditResource::vault(namespace_id, vault_id),
                        AuditOutcome::Failed {
                            code: "DeleteFailed".to_string(),
                            detail: "Failed to delete vault".to_string(),
                        },
                        Some(&trace_ctx.trace_id),
                    ));
                    Err(Status::internal("Failed to delete vault"))
                }
            },
            LedgerResponse::Error { message } => {
                ctx.set_error("Unspecified", &message);
                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::DeleteVault,
                    "system",
                    AuditResource::vault(namespace_id, vault_id),
                    AuditOutcome::Failed {
                        code: "Unspecified".to_string(),
                        detail: message.clone(),
                    },
                    Some(&trace_ctx.trace_id),
                ));
                Err(Status::internal(message))
            },
            _ => {
                ctx.set_error("UnexpectedResponse", "Unexpected response type");
                Err(Status::internal("Unexpected response type"))
            },
        }
    }

    async fn get_vault(
        &self,
        request: Request<GetVaultRequest>,
    ) -> Result<Response<GetVaultResponse>, Status> {
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "get_vault");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("get_vault");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        let namespace_id =
            req.namespace_id.as_ref().map(|n| DomainNamespaceId::new(n.id)).ok_or_else(|| {
                ctx.set_error("InvalidArgument", "Missing namespace_id");
                Status::invalid_argument("Missing namespace_id")
            })?;

        let vault_id =
            req.vault_id.as_ref().map(|v| DomainVaultId::new(v.id)).ok_or_else(|| {
                ctx.set_error("InvalidArgument", "Missing vault_id");
                Status::invalid_argument("Missing vault_id")
            })?;

        ctx.set_target(namespace_id.value(), vault_id.value());

        // Get vault metadata and height
        let vault_meta = self.applied_state.get_vault(namespace_id, vault_id);
        let height = self.applied_state.vault_height(namespace_id, vault_id);

        match vault_meta {
            Some(_vault) => {
                ctx.set_block_height(height);
                ctx.set_success();
                Ok(Response::new(GetVaultResponse {
                    namespace_id: Some(NamespaceId { id: namespace_id.value() }),
                    vault_id: Some(VaultId { id: vault_id.value() }),
                    height,
                    state_root: None,
                    nodes: vec![],
                    leader: None,
                    status: inferadb_ledger_proto::proto::VaultStatus::Active.into(),
                    retention_policy: None,
                }))
            },
            None => {
                ctx.set_error("NotFound", "Vault not found");
                Err(Status::not_found("Vault not found"))
            },
        }
    }

    async fn list_vaults(
        &self,
        _request: Request<ListVaultsRequest>,
    ) -> Result<Response<ListVaultsResponse>, Status> {
        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "list_vaults");
        ctx.set_operation_type(OperationType::Admin);
        ctx.set_admin_action("list_vaults");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // List all vaults across all namespaces
        let vaults = self
            .applied_state
            .all_vault_heights()
            .keys()
            .filter_map(|(ns_id, vault_id)| {
                self.applied_state.get_vault(*ns_id, *vault_id).map(|v| {
                    let height = self.applied_state.vault_height(v.namespace_id, v.vault_id);
                    inferadb_ledger_proto::proto::GetVaultResponse {
                        namespace_id: Some(NamespaceId { id: v.namespace_id.value() }),
                        vault_id: Some(VaultId { id: v.vault_id.value() }),
                        height,
                        state_root: None,
                        nodes: vec![],
                        leader: None,
                        status: inferadb_ledger_proto::proto::VaultStatus::Active.into(),
                        retention_policy: None,
                    }
                })
            })
            .collect::<Vec<_>>();

        // Set count in context for observability
        ctx.set_keys_count(vaults.len());
        ctx.set_success();

        Ok(Response::new(ListVaultsResponse { vaults }))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let _req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "create_snapshot");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("create_snapshot");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Trigger Raft snapshot
        ctx.start_raft_timer();
        let _ = self.raft.trigger().snapshot().await.map_err(|e| {
            ctx.end_raft_timer();
            ctx.set_error("SnapshotError", &e.to_string());
            Status::failed_precondition(format!("Snapshot error: {}", e))
        })?;
        ctx.end_raft_timer();

        // Get actual shard height from applied state
        let block_height = self.applied_state.shard_height();
        ctx.set_block_height(block_height);
        ctx.set_success();

        self.emit_audit_event(&self.build_audit_event(
            AuditAction::CreateSnapshot,
            "system",
            AuditResource::cluster().with_detail(format!("height:{block_height}")),
            AuditOutcome::Success,
            Some(&trace_ctx.trace_id),
        ));

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
        // Extract trace context and transport metadata from gRPC headers before consuming
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();
        let mut issues = Vec::new();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "check_integrity");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("check_integrity");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        let namespace_id = req.namespace_id.as_ref().map(|n| DomainNamespaceId::new(n.id));
        let vault_id = req.vault_id.as_ref().map(|v| DomainVaultId::new(v.id));

        if let Some(ns_id) = namespace_id {
            ctx.set_namespace_id(ns_id.value());
        }
        if let Some(v_id) = vault_id {
            ctx.set_vault_id(v_id.value());
        }

        // Get all vault heights to check
        let vault_heights: Vec<(DomainNamespaceId, DomainVaultId, u64)> =
            if let (Some(ns), Some(v)) = (namespace_id, vault_id) {
                // Specific vault
                let height = self.applied_state.vault_height(ns, v);
                if height > 0 { vec![(ns, v, height)] } else { vec![] }
            } else {
                // All vaults
                self.applied_state
                    .all_vault_heights()
                    .into_iter()
                    .map(|((ns, v), h)| (ns, v, h))
                    .collect()
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
                    },
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
                        },
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

                    let shard_block = match archive.read_block(shard_height) {
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
                        },
                    };

                    // Verify chain continuity (previous_vault_hash)
                    if let Some(expected_prev) = last_vault_hash
                        && entry.previous_vault_hash != expected_prev
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

            for (_ns_id, v_id, height) in &vault_heights {
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

        Ok(Response::new(CheckIntegrityResponse { healthy: issues.is_empty(), issues }))
    }

    // =========================================================================
    // Cluster Membership
    // =========================================================================

    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterResponse>, Status> {
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "join_cluster");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("join_cluster");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Get current metrics to check if we're the leader
        let metrics = self.raft.metrics().borrow().clone();
        let current_leader = metrics.current_leader;
        ctx.set_raft_term(metrics.current_term);
        ctx.set_is_leader(current_leader == Some(metrics.id));

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

            ctx.set_error("NotLeader", "Not the leader, redirect to leader");
            return Ok(Response::new(JoinClusterResponse {
                success: false,
                message: "Not the leader, redirect to leader".to_string(),
                leader_id: current_leader.unwrap_or(0),
                leader_address: leader_addr,
            }));
        }

        // We are the leader - add the new node as a learner first
        let node = BasicNode { addr: req.address.clone() };

        // Check if node is already in the membership (idempotent handling)
        let current_membership = metrics.membership_config.membership();
        let already_voter = current_membership.voter_ids().any(|id| id == req.node_id);
        let already_in_membership = current_membership.nodes().any(|(id, _)| *id == req.node_id);

        if already_voter {
            // Already a voter, nothing to do
            ctx.set_success();
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
        ctx.start_raft_timer();
        if !already_in_membership {
            let mut add_success = false;
            for attempt in 0..10 {
                match self.raft.add_learner(req.node_id, node.clone(), false).await {
                    Ok(_) => {
                        add_success = true;
                        break;
                    },
                    Err(e) => {
                        let err_str = format!("{}", e);
                        if err_str.contains("already undergoing a configuration change") {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (attempt + 1) as u64,
                            ))
                            .await;
                        } else {
                            ctx.end_raft_timer();
                            ctx.set_error("AddLearnerFailed", &e.to_string());
                            return Ok(Response::new(JoinClusterResponse {
                                success: false,
                                message: format!("Failed to add learner: {}", e),
                                leader_id: metrics.id,
                                leader_address: String::new(),
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
                    leader_id: metrics.id,
                    leader_address: String::new(),
                }));
            }
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
                ctx.end_raft_timer();
                ctx.set_error("Timeout", "Waiting for learner membership to commit");
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
                    ctx.end_raft_timer();
                    ctx.set_success();
                    self.emit_audit_event(&self.build_audit_event(
                        AuditAction::JoinCluster,
                        "system",
                        AuditResource::cluster().with_detail(format!("node:{}", req.node_id)),
                        AuditOutcome::Success,
                        Some(&trace_ctx.trace_id),
                    ));
                    return Ok(Response::new(JoinClusterResponse {
                        success: true,
                        message: "Node joined cluster successfully".to_string(),
                        leader_id: fresh_metrics.id,
                        leader_address: String::new(),
                    }));
                },
                Err(e) => {
                    let err_str = format!("{}", e);
                    if err_str.contains("already undergoing a configuration change") {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            100 * (attempt + 1) as u64,
                        ))
                        .await;
                    } else {
                        ctx.end_raft_timer();
                        ctx.set_error("PromotionFailed", &e.to_string());
                        return Ok(Response::new(JoinClusterResponse {
                            success: false,
                            message: format!("Failed to promote to voter: {}", e),
                            leader_id: fresh_metrics.id,
                            leader_address: String::new(),
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
            leader_id: metrics.id,
            leader_address: String::new(),
        }))
    }

    async fn leave_cluster(
        &self,
        request: Request<LeaveClusterRequest>,
    ) -> Result<Response<LeaveClusterResponse>, Status> {
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "leave_cluster");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("leave_cluster");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Get current metrics
        let metrics = self.raft.metrics().borrow().clone();
        let current_leader = metrics.current_leader;
        ctx.set_raft_term(metrics.current_term);
        ctx.set_is_leader(current_leader == Some(metrics.id));

        // Only the leader can change membership
        if current_leader != Some(metrics.id) {
            ctx.set_error("NotLeader", "Cannot process leave request");
            return Ok(Response::new(LeaveClusterResponse {
                success: false,
                message: "Not the leader, cannot process leave request".to_string(),
            }));
        }

        // Remove the node from voters
        let current_membership = metrics.membership_config.membership();
        let new_voters: BTreeSet<u64> =
            current_membership.voter_ids().filter(|id| *id != req.node_id).collect();

        // Prevent removing the last voter
        if new_voters.is_empty() {
            ctx.set_error("InvalidOperation", "Cannot remove the last voter");
            return Ok(Response::new(LeaveClusterResponse {
                success: false,
                message: "Cannot remove the last voter from cluster".to_string(),
            }));
        }

        ctx.start_raft_timer();
        match self.raft.change_membership(new_voters, false).await {
            Ok(_) => {
                ctx.end_raft_timer();
                ctx.set_success();
                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::LeaveCluster,
                    "system",
                    AuditResource::cluster().with_detail(format!("node:{}", req.node_id)),
                    AuditOutcome::Success,
                    Some(&trace_ctx.trace_id),
                ));
                Ok(Response::new(LeaveClusterResponse {
                    success: true,
                    message: "Node left cluster successfully".to_string(),
                }))
            },
            Err(e) => {
                ctx.end_raft_timer();
                ctx.set_error("MembershipChangeFailed", &e.to_string());
                self.emit_audit_event(&self.build_audit_event(
                    AuditAction::LeaveCluster,
                    "system",
                    AuditResource::cluster().with_detail(format!("node:{}", req.node_id)),
                    AuditOutcome::Failed {
                        code: "MembershipChangeFailed".to_string(),
                        detail: e.to_string(),
                    },
                    Some(&trace_ctx.trace_id),
                ));
                Ok(Response::new(LeaveClusterResponse {
                    success: false,
                    message: format!("Failed to remove node: {}", e),
                }))
            },
        }
    }

    async fn get_cluster_info(
        &self,
        _request: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "get_cluster_info");
        ctx.set_operation_type(OperationType::Admin);
        ctx.set_admin_action("get_cluster_info");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        let current_leader = metrics.current_leader;

        ctx.set_raft_term(metrics.vote.leader_id().term);
        ctx.set_is_leader(current_leader == Some(metrics.id));

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

        ctx.set_keys_count(members.len());
        ctx.set_success();

        Ok(Response::new(GetClusterInfoResponse {
            members,
            leader_id: current_leader.unwrap_or(0),
            term: metrics.vote.leader_id().term,
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
        _request: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "get_node_info");
        ctx.set_operation_type(OperationType::Admin);
        ctx.set_admin_action("get_node_info");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        let metrics = self.raft.metrics().borrow().clone();

        ctx.set_raft_term(metrics.current_term);
        ctx.set_is_leader(metrics.current_leader == Some(metrics.id));

        // Node is a cluster member if it has a leader or is in membership
        // (has at least one voter including itself)
        let is_cluster_member = metrics.current_leader.is_some()
            || metrics.membership_config.membership().voter_ids().count() > 0;

        ctx.set_success();

        Ok(Response::new(GetNodeInfoResponse {
            node_id: metrics.id,
            address: self.listen_addr.to_string(),
            is_cluster_member,
            term: metrics.current_term,
        }))
    }

    // =========================================================================
    // Vault Recovery
    // =========================================================================

    async fn recover_vault(
        &self,
        request: Request<RecoverVaultRequest>,
    ) -> Result<Response<RecoverVaultResponse>, Status> {
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "recover_vault");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("recover_vault");
        ctx.set_recovery_force(req.force);
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        let namespace_id =
            req.namespace_id.as_ref().map(|n| DomainNamespaceId::new(n.id)).ok_or_else(|| {
                ctx.set_error("InvalidArgument", "namespace_id required");
                Status::invalid_argument("namespace_id required")
            })?;
        let vault_id =
            req.vault_id.as_ref().map(|v| DomainVaultId::new(v.id)).ok_or_else(|| {
                ctx.set_error("InvalidArgument", "vault_id required");
                Status::invalid_argument("vault_id required")
            })?;

        ctx.set_target(namespace_id.value(), vault_id.value());

        // Check current vault health
        let current_health = self.applied_state.vault_health(namespace_id, vault_id);

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
                        final_height: self.applied_state.vault_height(namespace_id, vault_id),
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
        let expected_height = self.applied_state.vault_height(namespace_id, vault_id);
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
            // Find shard height for this vault height
            let shard_height = match archive.find_shard_height(namespace_id, vault_id, height) {
                Ok(Some(h)) => h,
                Ok(None) => {
                    ctx.end_storage_timer();
                    ctx.set_error("MissingBlock", &format!("Block not found at height {}", height));
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: format!(
                            "Block not found in archive: ns={}, vault={}, height={}",
                            namespace_id, vault_id, height
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

            // Read the shard block
            let shard_block = match archive.read_block(shard_height) {
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
            let entry = match shard_block.vault_entries.iter().find(|e| {
                e.namespace_id == namespace_id && e.vault_id == vault_id && e.vault_height == height
            }) {
                Some(e) => e,
                None => {
                    ctx.end_storage_timer();
                    ctx.set_error("MissingEntry", "Vault entry not found in shard block");
                    return Ok(Response::new(RecoverVaultResponse {
                        success: false,
                        message: format!(
                            "Vault entry not found in shard block at height {}",
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
                && entry.previous_vault_hash != expected_prev
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
                namespace_id = namespace_id.value(),
                vault_id = vault_id.value(),
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
            ctx.end_raft_timer();

            ctx.set_block_height(expected_height);
            ctx.set_error("DivergenceReproduced", "Recovery reproduced divergence");
            self.emit_audit_event(
                &self.build_audit_event(
                    AuditAction::RecoverVault,
                    "system",
                    AuditResource::vault(namespace_id, vault_id)
                        .with_detail(format!("height:{expected_height},divergence_reproduced")),
                    AuditOutcome::Failed {
                        code: "DivergenceReproduced".to_string(),
                        detail: "Recovery reproduced divergence".to_string(),
                    },
                    Some(&trace_ctx.trace_id),
                ),
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
            ctx.end_raft_timer();

            ctx.set_block_height(expected_height);
            ctx.set_success();
            self.emit_audit_event(
                &self.build_audit_event(
                    AuditAction::RecoverVault,
                    "system",
                    AuditResource::vault(namespace_id, vault_id)
                        .with_detail(format!("height:{expected_height}")),
                    AuditOutcome::Success,
                    Some(&trace_ctx.trace_id),
                ),
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
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "simulate_divergence");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("simulate_divergence");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        let namespace_id =
            req.namespace_id.as_ref().map(|n| DomainNamespaceId::new(n.id)).ok_or_else(|| {
                ctx.set_error("InvalidArgument", "Missing namespace_id");
                Status::invalid_argument("Missing namespace_id")
            })?;

        let vault_id =
            req.vault_id.as_ref().map(|v| DomainVaultId::new(v.id)).ok_or_else(|| {
                ctx.set_error("InvalidArgument", "Missing vault_id");
                Status::invalid_argument("Missing vault_id")
            })?;

        ctx.set_target(namespace_id.value(), vault_id.value());

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
            self.applied_state.vault_height(namespace_id, vault_id).max(1)
        };

        ctx.set_block_height(at_height);

        tracing::warn!(
            namespace_id = namespace_id.value(),
            vault_id = vault_id.value(),
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

        ctx.start_raft_timer();
        match self.raft.client_write(health_request).await {
            Ok(_) => {
                ctx.end_raft_timer();
                ctx.set_success();

                Ok(Response::new(inferadb_ledger_proto::proto::SimulateDivergenceResponse {
                    success: true,
                    message: format!(
                        "Vault {}:{} marked as diverged at height {}",
                        namespace_id.value(),
                        vault_id.value(),
                        at_height
                    ),
                    health_status: VaultHealthProto::Diverged.into(),
                }))
            },
            Err(e) => {
                ctx.end_raft_timer();
                ctx.set_error("RaftError", &e.to_string());
                tracing::error!(
                    namespace_id = namespace_id.value(),
                    vault_id = vault_id.value(),
                    error = %e,
                    "Failed to simulate divergence"
                );

                Err(Status::internal(format!("Failed to update vault health: {}", e)))
            },
        }
    }

    async fn force_gc(
        &self,
        request: Request<inferadb_ledger_proto::proto::ForceGcRequest>,
    ) -> Result<Response<inferadb_ledger_proto::proto::ForceGcResponse>, Status> {
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        // Create wide event context for this admin operation
        let mut ctx = RequestContext::new("AdminService", "force_gc");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("force_gc");
        if let Some(ref sampler) = self.sampler {
            ctx.set_sampler(sampler.clone());
        }
        if let Some(node_id) = self.node_id {
            ctx.set_node_id(node_id);
        }

        // Set trace context for distributed tracing correlation
        ctx.set_trace_context(
            &trace_ctx.trace_id,
            &trace_ctx.span_id,
            trace_ctx.parent_span_id.as_deref(),
            trace_ctx.trace_flags,
        );

        // Check if this node is the leader
        let metrics = self.raft.metrics().borrow().clone();
        let node_id = metrics.id;
        ctx.set_raft_term(metrics.current_term);
        ctx.set_is_leader(metrics.current_leader == Some(node_id));

        if metrics.current_leader != Some(node_id) {
            ctx.set_error("NotLeader", "Only the leader can run garbage collection");
            self.emit_audit_event(&self.build_audit_event(
                AuditAction::ForceGc,
                "system",
                AuditResource::cluster(),
                AuditOutcome::Failed {
                    code: "NotLeader".to_string(),
                    detail: "Only the leader can run garbage collection".to_string(),
                },
                Some(&trace_ctx.trace_id),
            ));
            return Err(Status::failed_precondition("Only the leader can run garbage collection"));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut total_expired = 0u64;
        let mut vaults_scanned = 0u64;

        // Determine which vaults to scan
        let vault_heights: Vec<((DomainNamespaceId, DomainVaultId), u64)> =
            if let (Some(ns), Some(v)) = (req.namespace_id, req.vault_id) {
                let ns_id = DomainNamespaceId::new(ns.id);
                let v_id = DomainVaultId::new(v.id);
                ctx.set_target(ns_id.value(), v_id.value());
                // Single vault
                let height = self.applied_state.vault_height(ns_id, v_id);
                vec![((ns_id, v_id), height)]
            } else {
                // All vaults
                self.applied_state.all_vault_heights().into_iter().collect()
            };

        ctx.start_raft_timer();
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
                        tracing::warn!(namespace_id = namespace_id.value(), vault_id = vault_id.value(), error = %e, "Failed to list entities for GC");
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
                client_id: "system:gc".to_string(),
                sequence: now, // Use timestamp as sequence for GC
                operations,
                timestamp: chrono::Utc::now(),
                actor: "system:gc".to_string(),
            };

            let gc_request =
                LedgerRequest::Write { namespace_id, vault_id, transactions: vec![transaction] };

            match self.raft.client_write(gc_request).await {
                Ok(_) => {
                    total_expired += count as u64;
                },
                Err(e) => {
                    tracing::warn!(namespace_id = namespace_id.value(), vault_id = vault_id.value(), error = %e, "GC write failed");
                },
            }
        }
        ctx.end_raft_timer();

        // Set success with counts
        ctx.set_keys_count(vaults_scanned as usize);
        ctx.set_operations_count(total_expired as usize);
        ctx.set_success();

        self.emit_audit_event(
            &self.build_audit_event(
                AuditAction::ForceGc,
                "system",
                AuditResource::cluster().with_detail(format!(
                    "expired:{total_expired},vaults_scanned:{vaults_scanned}"
                )),
                AuditOutcome::Success,
                Some(&trace_ctx.trace_id),
            ),
        );

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

    async fn update_config(
        &self,
        request: Request<UpdateConfigRequest>,
    ) -> Result<Response<UpdateConfigResponse>, Status> {
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

        // Compute diff for audit and response.
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

        // Audit the config change.
        self.emit_audit_event(&self.build_audit_event(
            AuditAction::UpdateConfig,
            "admin",
            AuditResource::cluster().with_detail(format!("changed:{}", changed.join(","))),
            AuditOutcome::Success,
            None,
        ));

        let updated = runtime_config.load();
        let updated_json = serde_json::to_string_pretty(&*updated).unwrap_or_default();

        Ok(Response::new(UpdateConfigResponse {
            applied: true,
            message: format!("Updated: {}", changed.join(", ")),
            current_config_json: updated_json,
            changed_fields: changed,
        }))
    }

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

    async fn create_backup(
        &self,
        request: Request<CreateBackupRequest>,
    ) -> Result<Response<CreateBackupResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = RequestContext::new("AdminService", "create_backup");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("create_backup");
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

            let shard_height = self.applied_state.shard_height();
            let db = self.state.database();

            let meta = backup_manager
                .create_incremental_backup(
                    db.as_ref(),
                    &base_backup_id,
                    base_meta.shard_id,
                    shard_height,
                    &tag,
                )
                .map_err(|e| {
                    ctx.set_error("BackupError", &e.to_string());
                    Status::internal(format!("Failed to create incremental backup: {e}"))
                })?;

            // Clear dirty bitmap after successful incremental backup
            db.clear_dirty_bitmap();

            meta
        } else {
            // Full snapshot-based backup (existing behavior)
            let snapshot_manager = self.snapshot_manager.as_ref().ok_or_else(|| {
                Status::failed_precondition("Snapshot manager is not available on this node")
            })?;

            // Trigger Raft snapshot for consistent state
            ctx.start_raft_timer();
            let _ = self.raft.trigger().snapshot().await.map_err(|e| {
                ctx.end_raft_timer();
                ctx.set_error("SnapshotError", &e.to_string());
                Status::failed_precondition(format!("Failed to trigger snapshot: {e}"))
            })?;
            ctx.end_raft_timer();

            // Load the latest snapshot
            let snapshot = snapshot_manager
                .load_latest()
                .map_err(|e| Status::internal(format!("Failed to load snapshot: {e}")))?
                .ok_or_else(|| Status::failed_precondition("No snapshot available for backup"))?;

            backup_manager.create_backup(&snapshot, &tag).map_err(|e| {
                ctx.set_error("BackupError", &e.to_string());
                Status::internal(format!("Failed to create backup: {e}"))
            })?
        };

        ctx.set_block_height(meta.shard_height);
        ctx.set_success();

        crate::backup::record_backup_created(meta.shard_height, meta.size_bytes);

        self.emit_audit_event(
            &self.build_audit_event(
                AuditAction::CreateSnapshot,
                "system",
                AuditResource::cluster()
                    .with_detail(format!("backup:{},height:{}", meta.backup_id, meta.shard_height)),
                AuditOutcome::Success,
                Some(&trace_ctx.trace_id),
            ),
        );

        Ok(Response::new(CreateBackupResponse {
            backup_id: meta.backup_id,
            shard_height: meta.shard_height,
            backup_path: meta.backup_path,
            size_bytes: meta.size_bytes,
            checksum: Some(Hash { value: meta.checksum.to_vec() }),
        }))
    }

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
            .map_err(|e| Status::internal(format!("Failed to list backups: {e}")))?;

        let backup_infos: Vec<BackupInfo> = backups
            .into_iter()
            .map(|meta| {
                let created_at = prost_types::Timestamp {
                    seconds: meta.created_at.timestamp(),
                    nanos: meta.created_at.timestamp_subsec_nanos() as i32,
                };

                let backup_type = match meta.backup_type {
                    crate::backup::BackupType::Full => 0,
                    crate::backup::BackupType::Incremental => 1,
                };

                BackupInfo {
                    backup_id: meta.backup_id,
                    shard_height: meta.shard_height,
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

    async fn restore_backup(
        &self,
        request: Request<RestoreBackupRequest>,
    ) -> Result<Response<RestoreBackupResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = RequestContext::new("AdminService", "restore_backup");
        ctx.set_operation_type(OperationType::Admin);
        ctx.extract_transport_metadata(&grpc_metadata);
        ctx.set_admin_action("restore_backup");
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

        // Safety gate: require explicit confirmation
        if !req.confirm {
            return Err(Status::failed_precondition(
                "Restore requires confirm=true. This operation will replace current shard state with the backup.",
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
                Status::internal(format!("Failed to resolve backup chain: {e}"))
            })?;

            let db = self.state.database();
            let height = backup_manager.restore_page_chain(&chain, db.as_ref()).map_err(|e| {
                ctx.set_error("RestoreError", &e.to_string());
                Status::internal(format!("Failed to restore page backup chain: {e}"))
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
                Status::internal(format!("Failed to load backup: {e}"))
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
                Status::internal(format!("Failed to save backup as snapshot: {e}"))
            })?;

            let height = snapshot.header.shard_height;
            let msg = format!(
                "Backup {} (height {}) restored as snapshot. Restart the node to apply.",
                meta.backup_id, height
            );
            (height, msg)
        };

        ctx.set_block_height(restored_height);
        ctx.set_success();

        self.emit_audit_event(
            &self.build_audit_event(
                AuditAction::CreateSnapshot,
                "system",
                AuditResource::cluster()
                    .with_detail(format!("restore:{},height:{}", meta.backup_id, restored_height)),
                AuditOutcome::Success,
                Some(&trace_ctx.trace_id),
            ),
        );

        Ok(Response::new(RestoreBackupResponse { success: true, message, restored_height }))
    }
}

/// Computes the hash of a vault block entry for chain verification.
///
/// The vault block hash commits to all content: height, previous hash,
/// transactions (via tx_merkle_root), and state root.
fn compute_vault_block_hash(entry: &VaultEntry) -> [u8; 32] {
    let mut hasher = Sha256::new();

    // Hash the vault block header fields
    hasher.update(entry.namespace_id.value().to_le_bytes());
    hasher.update(entry.vault_id.value().to_le_bytes());
    hasher.update(entry.vault_height.to_le_bytes());
    hasher.update(entry.previous_vault_hash);
    hasher.update(entry.tx_merkle_root);
    hasher.update(entry.state_root);

    hasher.finalize().into()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    // =========================================================================
    // compute_vault_block_hash Tests
    // =========================================================================

    #[test]
    fn test_vault_block_hash_deterministic() {
        // Same input should always produce the same hash
        let entry = VaultEntry {
            namespace_id: DomainNamespaceId::new(1),
            vault_id: DomainVaultId::new(2),
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
            namespace_id: DomainNamespaceId::new(1),
            vault_id: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
        };

        let entry2 = VaultEntry {
            namespace_id: DomainNamespaceId::new(1),
            vault_id: DomainVaultId::new(2),
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
    fn test_vault_block_hash_different_state_root() {
        let entry1 = VaultEntry {
            namespace_id: DomainNamespaceId::new(1),
            vault_id: DomainVaultId::new(2),
            vault_height: 10,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [0u8; 32],
        };

        let entry2 = VaultEntry {
            namespace_id: DomainNamespaceId::new(1),
            vault_id: DomainVaultId::new(2),
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
    fn test_vault_block_hash_chain_continuity() {
        // Simulate a chain of blocks
        let entry1 = VaultEntry {
            namespace_id: DomainNamespaceId::new(1),
            vault_id: DomainVaultId::new(1),
            vault_height: 1,
            previous_vault_hash: ZERO_HASH, // Genesis block
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [1u8; 32],
        };

        let hash1 = compute_vault_block_hash(&entry1);

        let entry2 = VaultEntry {
            namespace_id: DomainNamespaceId::new(1),
            vault_id: DomainVaultId::new(1),
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
            namespace_id: DomainNamespaceId::new(1),
            vault_id: DomainVaultId::new(1),
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
            namespace_id: DomainNamespaceId::new(1),
            vault_id: DomainVaultId::new(2),
            vault_height: 3,
            previous_vault_hash: [4u8; 32],
            transactions: vec![],
            tx_merkle_root: [5u8; 32],
            state_root: [6u8; 32],
        };

        let base_hash = compute_vault_block_hash(&base_entry);

        // Changing namespace_id should change hash
        let mut modified = base_entry.clone();
        modified.namespace_id = DomainNamespaceId::new(99);
        assert_ne!(compute_vault_block_hash(&modified), base_hash, "namespace_id affects hash");

        // Changing vault_id should change hash
        let mut modified = base_entry.clone();
        modified.vault_id = DomainVaultId::new(99);
        assert_ne!(compute_vault_block_hash(&modified), base_hash, "vault_id affects hash");

        // Changing tx_merkle_root should change hash
        let mut modified = base_entry.clone();
        modified.tx_merkle_root = [99u8; 32];
        assert_ne!(compute_vault_block_hash(&modified), base_hash, "tx_merkle_root affects hash");
    }
}
