//! Multi-shard write service implementation.
//!
//! Routes write requests to the appropriate shard based on organization.
//! Each organization is assigned to a shard, and requests are routed to the
//! Raft instance for that shard.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use inferadb_ledger_proto::proto::{
    BatchWriteRequest, BatchWriteResponse, BatchWriteSuccess, Operation, TxId, WriteError,
    WriteErrorCode, WriteRequest, WriteResponse, WriteSuccess, write_service_server::WriteService,
};
use inferadb_ledger_types::config::ValidationConfig;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    error::classify_raft_error,
    idempotency::IdempotencyCache,
    metrics,
    multi_raft::MultiRaftManager,
    proof::{self, ProofError},
    rate_limit::RateLimiter,
    services::{
        ForwardClient,
        metadata::{response_with_correlation, status_with_correlation},
        shard_resolver::{RemoteShardInfo, ResolveResult, ShardResolver},
        slug_resolver::SlugResolver,
    },
    trace_context,
    types::{LedgerRequest, LedgerResponse, RaftPayload},
};

// Note: SetCondition conversion is internal to convert_set_condition

/// Routes write requests to the correct shard via Raft consensus based on organization ID.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct MultiShardWriteService {
    /// Shard resolver for routing requests.
    resolver: Arc<dyn ShardResolver>,
    /// Multi-raft manager for creating forward clients when needed.
    /// Optional to support standalone single-shard deployments.
    #[builder(default)]
    manager: Option<Arc<MultiRaftManager>>,
    /// Idempotency cache for duplicate detection.
    idempotency: Arc<IdempotencyCache>,
    /// Per-organization rate limiter (optional).
    #[builder(default)]
    rate_limiter: Option<Arc<RateLimiter>>,
    /// Hot key detector for identifying frequently accessed keys (optional).
    #[builder(default)]
    hot_key_detector: Option<Arc<crate::hot_key_detector::HotKeyDetector>>,
    /// Input validation configuration for request field limits.
    #[builder(default = Arc::new(ValidationConfig::default()))]
    validation_config: Arc<ValidationConfig>,
    /// Maximum time to wait for a Raft proposal to commit.
    ///
    /// If a gRPC deadline is shorter, the deadline takes precedence.
    #[builder(default = Duration::from_secs(30))]
    proposal_timeout: Duration,
    /// Per-organization resource quota checker.
    #[builder(default)]
    quota_checker: Option<crate::quota::QuotaChecker>,
    /// Handler-phase event handle for recording denial events.
    #[builder(default)]
    event_handle: Option<crate::event_writer::EventHandle<inferadb_ledger_store::FileBackend>>,
    /// Node ID for handler-phase event attribution.
    #[builder(default)]
    node_id: Option<u64>,
}

#[allow(clippy::result_large_err)]
impl MultiShardWriteService {
    /// Checks all rate limit tiers (backpressure, organization, client).
    fn check_rate_limit(
        &self,
        client_id: &str,
        organization_id: inferadb_ledger_types::OrganizationId,
    ) -> Result<(), Status> {
        super::helpers::check_rate_limit(self.rate_limiter.as_ref(), client_id, organization_id)
    }

    /// Records a handler-phase event (best-effort).
    fn record_handler_event(&self, entry: inferadb_ledger_types::events::EventEntry) {
        if let Some(ref handle) = self.event_handle {
            handle.record_handler_event(entry);
        }
    }

    /// Returns a forward client for a remote shard.
    ///
    /// Creates a gRPC connection to the remote shard's leader (or any member if leader unknown).
    async fn get_forward_client(&self, remote: &RemoteShardInfo) -> Result<ForwardClient, Status> {
        let manager = self.manager.as_ref().ok_or_else(|| {
            Status::unavailable("Request forwarding not configured for this service")
        })?;

        let router =
            manager.router().ok_or_else(|| Status::unavailable("Shard router not initialized"))?;

        let connection = router
            .get_connection(
                remote.shard_id,
                &remote.routing.member_nodes,
                remote.routing.leader_hint.as_deref(),
            )
            .await
            .map_err(|e| {
                Status::unavailable(format!("Failed to connect to remote shard: {}", e))
            })?;

        Ok(ForwardClient::new(connection))
    }

    /// Sets input validation configuration for request field limits.
    #[must_use]
    pub fn with_validation_config(mut self, config: Arc<ValidationConfig>) -> Self {
        self.validation_config = config;
        self
    }

    /// Validates all operations in a proto operation list.
    fn validate_operations(&self, operations: &[Operation]) -> Result<(), Status> {
        super::helpers::validate_operations(operations, &self.validation_config)
    }

    /// Records key accesses from operations for hot key detection.
    fn record_hot_keys(&self, vault_id: inferadb_ledger_types::VaultId, operations: &[Operation]) {
        super::helpers::record_hot_keys(self.hot_key_detector.as_ref(), vault_id, operations);
    }

    /// Computes a hash of operations for idempotency payload comparison.
    fn hash_operations(operations: &[Operation]) -> Vec<u8> {
        super::helpers::hash_operations(operations)
    }

    /// Generates block header and transaction proof for a committed write.
    fn generate_write_proof(
        &self,
        organization_id: inferadb_ledger_types::OrganizationId,
        vault_id: inferadb_ledger_types::VaultId,
        vault_height: u64,
    ) -> (
        Option<inferadb_ledger_proto::proto::BlockHeader>,
        Option<inferadb_ledger_proto::proto::MerkleProof>,
    ) {
        let ctx = match self.resolver.resolve(organization_id) {
            Ok(ctx) => ctx,
            Err(_) => return (None, None),
        };

        // Resolve internal IDs to slugs for response construction
        let slug = ctx.applied_state.resolve_id_to_slug(organization_id);
        let vault_slug = ctx.applied_state.resolve_vault_id_to_slug(vault_id);

        // Use the proof module's implementation
        match proof::generate_write_proof(
            &ctx.block_archive,
            organization_id,
            slug,
            vault_id,
            vault_slug,
            vault_height,
            0,
        ) {
            Ok(write_proof) => (Some(write_proof.block_header), Some(write_proof.tx_proof)),
            Err(e) => {
                match &e {
                    ProofError::BlockNotFound { .. } | ProofError::NoTransactions => {
                        debug!(error = %e, "Proof generation skipped");
                    },
                    _ => {
                        warn!(error = %e, "Proof generation failed");
                    },
                }
                (None, None)
            },
        }
    }

    /// Builds a ledger request from operations.
    ///
    /// Server-assigned sequences: The transaction's sequence is set to 0 here;
    /// the actual sequence will be assigned by the Raft state machine at apply time.
    fn build_request(
        &self,
        operations: &[Operation],
        organization_id: inferadb_ledger_types::OrganizationId,
        vault_id: inferadb_ledger_types::VaultId,
        client_id: &str,
        actor: &str,
    ) -> Result<LedgerRequest, Status> {
        let internal_ops: Vec<inferadb_ledger_types::Operation> = operations
            .iter()
            .map(inferadb_ledger_types::Operation::try_from)
            .collect::<Result<Vec<_>, Status>>()?;

        if internal_ops.is_empty() {
            return Err(Status::invalid_argument("No operations provided"));
        }

        // Server-assigned sequences: sequence=0 is a placeholder; actual sequence
        // is assigned at Raft apply time for deterministic replay.
        let transaction = inferadb_ledger_types::Transaction {
            id: *Uuid::new_v4().as_bytes(),
            client_id: client_id.to_string(),
            sequence: 0, // Server-assigned at apply time
            operations: internal_ops,
            timestamp: chrono::Utc::now(),
            actor: actor.to_string(),
        };

        Ok(LedgerRequest::Write {
            organization_id,
            vault_id,
            transactions: vec![transaction],
            idempotency_key: [0; 16],
            request_hash: 0,
        })
    }
}

#[tonic::async_trait]
impl WriteService for MultiShardWriteService {
    #[instrument(skip(self, request), fields(client_id, organization_id, vault_slug))]
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        crate::deadline::check_near_deadline(&request)?;

        let start = Instant::now();
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let request_id = Uuid::new_v4();
        let req = request.into_inner();

        // Extract identifiers
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();
        let system = self.resolver.system_shard()?;
        let organization_id =
            SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;

        // Check for remote forwarding — if the organization is on a remote shard,
        // run pre-flight checks and forward the raw request (no vault resolution).
        if self.resolver.supports_forwarding()
            && let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
        {
            // Pre-flight: validation on originating node
            if let Err(status) = self.validate_operations(&req.operations) {
                self.record_handler_event(
                    crate::event_writer::HandlerPhaseEmitter::for_organization(
                        inferadb_ledger_types::events::EventAction::RequestValidationFailed,
                        organization_id,
                        req.organization.as_ref().map(|n| n.slug),
                        self.node_id.unwrap_or(0),
                    )
                    .principal(&client_id)
                    .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: status.message().to_string(),
                    })
                    .trace_id(&trace_ctx.trace_id)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
                );
                return Err(status);
            }

            // Pre-flight: rate limit on originating node
            if let Err(status) = self.check_rate_limit(&client_id, organization_id) {
                self.record_handler_event(
                    crate::event_writer::HandlerPhaseEmitter::for_organization(
                        inferadb_ledger_types::events::EventAction::RequestRateLimited,
                        organization_id,
                        req.organization.as_ref().map(|n| n.slug),
                        self.node_id.unwrap_or(0),
                    )
                    .principal(&client_id)
                    .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: "rate_limited".to_string(),
                    })
                    .trace_id(&trace_ctx.trace_id)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
                );
                return Err(status_with_correlation(status, &request_id, &trace_ctx.trace_id));
            }

            // Pre-flight: quota on originating node
            let estimated_bytes = super::helpers::estimate_operations_bytes(&req.operations);
            super::helpers::check_storage_quota(
                self.quota_checker.as_ref(),
                organization_id,
                estimated_bytes,
            )
            .map_err(|status| {
                self.record_handler_event(
                    crate::event_writer::HandlerPhaseEmitter::for_organization(
                        inferadb_ledger_types::events::EventAction::QuotaExceeded,
                        organization_id,
                        req.organization.as_ref().map(|n| n.slug),
                        self.node_id.unwrap_or(0),
                    )
                    .principal(&client_id)
                    .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: "storage_quota_exceeded".to_string(),
                    })
                    .detail("estimated_bytes", &estimated_bytes.to_string())
                    .trace_id(&trace_ctx.trace_id)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
                );
                status_with_correlation(status, &request_id, &trace_ctx.trace_id)
            })?;

            // Forward to remote shard — destination resolves vault slug
            debug!(
                organization_id = organization_id.value(),
                shard_id = remote.shard_id.value(),
                "Forwarding write to remote shard"
            );
            let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
            let mut client = self.get_forward_client(&remote).await?;
            return client.forward_write(req, Some(&trace_ctx), grpc_deadline).await;
        }

        // Local processing path (single-shard or forwarding-enabled with local org)
        let ctx = self.resolver.resolve(organization_id)?;
        let vault_id =
            SlugResolver::new(ctx.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;
        // Actor is set by upstream Engine/Control services - Ledger uses "system" internally
        let actor = "system".to_string();

        // Parse idempotency key (must be exactly 16 bytes for UUID)
        let idempotency_key: [u8; 16] =
            req.idempotency_key.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("idempotency_key must be exactly 16 bytes")
            })?;

        // Validate all operations before any processing
        if let Err(status) = self.validate_operations(&req.operations) {
            self.record_handler_event(
                crate::event_writer::HandlerPhaseEmitter::for_organization(
                    inferadb_ledger_types::events::EventAction::RequestValidationFailed,
                    organization_id,
                    req.organization.as_ref().map(|n| n.slug),
                    self.node_id.unwrap_or(0),
                )
                .principal(&client_id)
                .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: status.message().to_string(),
                })
                .trace_id(&trace_ctx.trace_id)
                .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
            );
            return Err(status);
        }

        // Compute request hash for payload comparison
        let request_hash = seahash::hash(&Self::hash_operations(&req.operations));

        // Record span fields
        tracing::Span::current().record("client_id", &client_id);
        tracing::Span::current().record("organization_id", organization_id.value());
        tracing::Span::current().record("vault_slug", req.vault.as_ref().map_or(0, |v| v.slug));

        // Check idempotency cache
        use crate::idempotency::IdempotencyCheckResult;
        match self.idempotency.check(
            organization_id.value(),
            vault_id.value(),
            &client_id,
            idempotency_key,
            request_hash,
        ) {
            IdempotencyCheckResult::Duplicate(cached) => {
                debug!("Returning cached result for duplicate request");
                metrics::record_idempotency_hit();
                metrics::record_write(true, start.elapsed().as_secs_f64());
                return Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::write_response::Result::Success(cached),
                        ),
                    },
                    &request_id,
                    &trace_ctx.trace_id,
                ));
            },
            IdempotencyCheckResult::KeyReused => {
                metrics::record_write(false, start.elapsed().as_secs_f64());
                return Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(inferadb_ledger_proto::proto::write_response::Result::Error(WriteError {
                            code: WriteErrorCode::IdempotencyKeyReused.into(),
                            key: String::new(),
                            current_version: None,
                            current_value: None,
                            message:
                                "Idempotency key was already used with a different request payload"
                                    .to_string(),
                            committed_tx_id: None,
                            committed_block_height: None,
                            assigned_sequence: None,
                        })),
                    },
                    &request_id,
                    &trace_ctx.trace_id,
                ));
            },
            IdempotencyCheckResult::NewRequest => {
                metrics::record_idempotency_miss();
            },
        }

        // Check rate limit
        if let Err(status) = self.check_rate_limit(&client_id, organization_id) {
            self.record_handler_event(
                crate::event_writer::HandlerPhaseEmitter::for_organization(
                    inferadb_ledger_types::events::EventAction::RequestRateLimited,
                    organization_id,
                    req.organization.as_ref().map(|n| n.slug),
                    self.node_id.unwrap_or(0),
                )
                .principal(&client_id)
                .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: "rate_limited".to_string(),
                })
                .trace_id(&trace_ctx.trace_id)
                .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
            );
            return Err(status_with_correlation(status, &request_id, &trace_ctx.trace_id));
        }

        // Check storage quota (estimated from operation payload size)
        let estimated_bytes = super::helpers::estimate_operations_bytes(&req.operations);
        super::helpers::check_storage_quota(
            self.quota_checker.as_ref(),
            organization_id,
            estimated_bytes,
        )
        .map_err(|status| {
            self.record_handler_event(
                crate::event_writer::HandlerPhaseEmitter::for_organization(
                    inferadb_ledger_types::events::EventAction::QuotaExceeded,
                    organization_id,
                    req.organization.as_ref().map(|n| n.slug),
                    self.node_id.unwrap_or(0),
                )
                .principal(&client_id)
                .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: "storage_quota_exceeded".to_string(),
                })
                .detail("estimated_bytes", &estimated_bytes.to_string())
                .trace_id(&trace_ctx.trace_id)
                .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
            );
            status_with_correlation(status, &request_id, &trace_ctx.trace_id)
        })?;

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &req.operations);

        // Server-assigned sequences: no gap check needed

        // Build request
        let ledger_request =
            self.build_request(&req.operations, organization_id, vault_id, &client_id, &actor)?;

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout = crate::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        // Submit to the resolved shard's Raft
        metrics::record_raft_proposal();
        let result = match tokio::time::timeout(
            timeout,
            ctx.raft.client_write(RaftPayload {
                request: ledger_request,
                proposed_at: chrono::Utc::now(),
            }),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                warn!(error = %e, "Raft write failed");
                metrics::record_write(false, start.elapsed().as_secs_f64());
                return Err(status_with_correlation(
                    classify_raft_error(&e.to_string()),
                    &request_id,
                    &trace_ctx.trace_id,
                ));
            },
            Err(_elapsed) => {
                metrics::record_raft_proposal_timeout();
                metrics::record_write(false, start.elapsed().as_secs_f64());
                return Err(status_with_correlation(
                    Status::deadline_exceeded(format!(
                        "Raft proposal timed out after {}ms",
                        timeout.as_millis()
                    )),
                    &request_id,
                    &trace_ctx.trace_id,
                ));
            },
        };

        let response = result.data;
        let latency = start.elapsed().as_secs_f64();

        match response {
            LedgerResponse::Write { block_height, block_hash: _, assigned_sequence } => {
                // Generate proof if requested
                let (block_header, tx_proof) = if req.include_tx_proof {
                    self.generate_write_proof(organization_id, vault_id, block_height)
                } else {
                    (None, None)
                };

                let success = WriteSuccess {
                    tx_id: Some(TxId { id: Uuid::new_v4().as_bytes().to_vec() }),
                    block_height,
                    block_header,
                    tx_proof,
                    assigned_sequence,
                };

                // Cache the successful result
                self.idempotency.insert(
                    organization_id.value(),
                    vault_id.value(),
                    client_id.clone(),
                    idempotency_key,
                    request_hash,
                    success.clone(),
                );

                metrics::record_write(true, latency);
                metrics::record_organization_operation(organization_id.value(), "write");
                metrics::record_organization_latency(organization_id.value(), "write", latency);
                info!(
                    trace_id = %trace_ctx.trace_id,
                    block_height,
                    assigned_sequence,
                    latency_ms = latency * 1000.0,
                    "Write committed"
                );

                Ok(response_with_correlation(
                    WriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::write_response::Result::Success(success),
                        ),
                    },
                    &request_id,
                    &trace_ctx.trace_id,
                ))
            },
            _ => {
                warn!("Unexpected Raft response for write");
                metrics::record_write(false, latency);
                Err(status_with_correlation(
                    Status::internal("Unexpected response type"),
                    &request_id,
                    &trace_ctx.trace_id,
                ))
            },
        }
    }

    #[instrument(
        skip(self, request),
        fields(client_id, sequence, organization_id, vault_slug, batch_size)
    )]
    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        crate::deadline::check_near_deadline(&request)?;

        let start = Instant::now();
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let request_id = Uuid::new_v4();
        let req = request.into_inner();

        // Extract identifiers
        let client_id = req.client_id.as_ref().map(|c| c.id.clone()).unwrap_or_default();
        let system = self.resolver.system_shard()?;
        let organization_id =
            SlugResolver::new(system.applied_state).extract_and_resolve(&req.organization)?;

        // Check for remote forwarding — if the organization is on a remote shard,
        // run pre-flight checks and forward the raw request (no vault resolution).
        if self.resolver.supports_forwarding()
            && let ResolveResult::Remote(remote) =
                self.resolver.resolve_with_forward(organization_id)?
        {
            // Flatten operations for pre-flight validation
            let all_operations: Vec<inferadb_ledger_proto::proto::Operation> =
                req.operations.iter().flat_map(|group| group.operations.clone()).collect();

            // Pre-flight: validation on originating node
            if let Err(status) = self.validate_operations(&all_operations) {
                self.record_handler_event(
                    crate::event_writer::HandlerPhaseEmitter::for_organization(
                        inferadb_ledger_types::events::EventAction::RequestValidationFailed,
                        organization_id,
                        req.organization.as_ref().map(|n| n.slug),
                        self.node_id.unwrap_or(0),
                    )
                    .principal(&client_id)
                    .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: status.message().to_string(),
                    })
                    .trace_id(&trace_ctx.trace_id)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
                );
                return Err(status);
            }

            // Pre-flight: rate limit on originating node
            if let Err(status) = self.check_rate_limit(&client_id, organization_id) {
                self.record_handler_event(
                    crate::event_writer::HandlerPhaseEmitter::for_organization(
                        inferadb_ledger_types::events::EventAction::RequestRateLimited,
                        organization_id,
                        req.organization.as_ref().map(|n| n.slug),
                        self.node_id.unwrap_or(0),
                    )
                    .principal(&client_id)
                    .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: "rate_limited".to_string(),
                    })
                    .operations_count(all_operations.len() as u32)
                    .trace_id(&trace_ctx.trace_id)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
                );
                return Err(status_with_correlation(status, &request_id, &trace_ctx.trace_id));
            }

            // Pre-flight: quota on originating node
            let estimated_bytes = super::helpers::estimate_operations_bytes(&all_operations);
            super::helpers::check_storage_quota(
                self.quota_checker.as_ref(),
                organization_id,
                estimated_bytes,
            )
            .map_err(|status| {
                self.record_handler_event(
                    crate::event_writer::HandlerPhaseEmitter::for_organization(
                        inferadb_ledger_types::events::EventAction::QuotaExceeded,
                        organization_id,
                        req.organization.as_ref().map(|n| n.slug),
                        self.node_id.unwrap_or(0),
                    )
                    .principal(&client_id)
                    .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                        reason: "storage_quota_exceeded".to_string(),
                    })
                    .detail("estimated_bytes", &estimated_bytes.to_string())
                    .trace_id(&trace_ctx.trace_id)
                    .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
                );
                status_with_correlation(status, &request_id, &trace_ctx.trace_id)
            })?;

            // Forward to remote shard — destination resolves vault slug
            debug!(
                organization_id = organization_id.value(),
                shard_id = remote.shard_id.value(),
                "Forwarding batch_write to remote shard"
            );
            let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
            let mut client = self.get_forward_client(&remote).await?;
            return client.forward_batch_write(req, Some(&trace_ctx), grpc_deadline).await;
        }

        // Local processing path (single-shard or forwarding-enabled with local org)
        let ctx = self.resolver.resolve(organization_id)?;
        let vault_id =
            SlugResolver::new(ctx.applied_state.clone()).extract_and_resolve_vault(&req.vault)?;

        // Parse idempotency key (must be exactly 16 bytes for UUID)
        let idempotency_key: [u8; 16] =
            req.idempotency_key.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("idempotency_key must be exactly 16 bytes")
            })?;

        // Flatten all operations from all groups
        let all_operations: Vec<inferadb_ledger_proto::proto::Operation> =
            req.operations.iter().flat_map(|group| group.operations.clone()).collect();

        // Validate all operations before any processing
        if let Err(status) = self.validate_operations(&all_operations) {
            self.record_handler_event(
                crate::event_writer::HandlerPhaseEmitter::for_organization(
                    inferadb_ledger_types::events::EventAction::RequestValidationFailed,
                    organization_id,
                    req.organization.as_ref().map(|n| n.slug),
                    self.node_id.unwrap_or(0),
                )
                .principal(&client_id)
                .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: status.message().to_string(),
                })
                .trace_id(&trace_ctx.trace_id)
                .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
            );
            return Err(status);
        }

        let batch_size = all_operations.len();

        // Compute request hash for payload comparison
        let request_hash = seahash::hash(&Self::hash_operations(&all_operations));

        // Record span fields
        tracing::Span::current().record("client_id", &client_id);
        tracing::Span::current().record("organization_id", organization_id.value());
        tracing::Span::current().record("vault_slug", req.vault.as_ref().map_or(0, |v| v.slug));
        tracing::Span::current().record("batch_size", batch_size);

        // Check idempotency cache
        use crate::idempotency::IdempotencyCheckResult;
        match self.idempotency.check(
            organization_id.value(),
            vault_id.value(),
            &client_id,
            idempotency_key,
            request_hash,
        ) {
            IdempotencyCheckResult::Duplicate(cached) => {
                debug!("Returning cached result for duplicate batch request");
                metrics::record_idempotency_hit();
                metrics::record_batch_write(true, 0, start.elapsed().as_secs_f64());
                return Ok(response_with_correlation(
                    BatchWriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::batch_write_response::Result::Success(
                                BatchWriteSuccess {
                                    tx_id: cached.tx_id,
                                    block_height: cached.block_height,
                                    block_header: cached.block_header,
                                    tx_proof: cached.tx_proof,
                                    assigned_sequence: cached.assigned_sequence,
                                },
                            ),
                        ),
                    },
                    &request_id,
                    &trace_ctx.trace_id,
                ));
            },
            IdempotencyCheckResult::KeyReused => {
                metrics::record_batch_write(false, batch_size, start.elapsed().as_secs_f64());
                return Ok(response_with_correlation(BatchWriteResponse {
                    result: Some(inferadb_ledger_proto::proto::batch_write_response::Result::Error(WriteError {
                        code: WriteErrorCode::IdempotencyKeyReused.into(),
                        key: String::new(),
                        current_version: None,
                        current_value: None,
                        message:
                            "Idempotency key was already used with a different request payload"
                                .to_string(),
                        committed_tx_id: None,
                        committed_block_height: None,
                        assigned_sequence: None,
                    })),
                }, &request_id, &trace_ctx.trace_id));
            },
            IdempotencyCheckResult::NewRequest => {
                metrics::record_idempotency_miss();
            },
        }

        // Check rate limit
        if let Err(status) = self.check_rate_limit(&client_id, organization_id) {
            self.record_handler_event(
                crate::event_writer::HandlerPhaseEmitter::for_organization(
                    inferadb_ledger_types::events::EventAction::RequestRateLimited,
                    organization_id,
                    req.organization.as_ref().map(|n| n.slug),
                    self.node_id.unwrap_or(0),
                )
                .principal(&client_id)
                .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: "rate_limited".to_string(),
                })
                .operations_count(batch_size as u32)
                .trace_id(&trace_ctx.trace_id)
                .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
            );
            return Err(status_with_correlation(status, &request_id, &trace_ctx.trace_id));
        }

        // Check storage quota (estimated from operation payload size)
        let estimated_bytes = super::helpers::estimate_operations_bytes(&all_operations);
        super::helpers::check_storage_quota(
            self.quota_checker.as_ref(),
            organization_id,
            estimated_bytes,
        )
        .map_err(|status| {
            self.record_handler_event(
                crate::event_writer::HandlerPhaseEmitter::for_organization(
                    inferadb_ledger_types::events::EventAction::QuotaExceeded,
                    organization_id,
                    req.organization.as_ref().map(|n| n.slug),
                    self.node_id.unwrap_or(0),
                )
                .principal(&client_id)
                .outcome(inferadb_ledger_types::events::EventOutcome::Denied {
                    reason: "storage_quota_exceeded".to_string(),
                })
                .detail("estimated_bytes", &estimated_bytes.to_string())
                .trace_id(&trace_ctx.trace_id)
                .build(self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)),
            );
            status_with_correlation(status, &request_id, &trace_ctx.trace_id)
        })?;

        // Track key access frequency for hot key detection.
        self.record_hot_keys(vault_id, &all_operations);

        // Build request with flattened operations
        let ledger_request =
            self.build_request(&all_operations, organization_id, vault_id, &client_id, "system")?;

        // Compute effective timeout: min(proposal_timeout, grpc_deadline)
        let grpc_deadline = crate::deadline::extract_deadline_from_metadata(&grpc_metadata);
        let timeout = crate::deadline::effective_timeout(self.proposal_timeout, grpc_deadline);

        // Submit to Raft
        metrics::record_raft_proposal();
        let result = match tokio::time::timeout(
            timeout,
            ctx.raft.client_write(RaftPayload {
                request: ledger_request,
                proposed_at: chrono::Utc::now(),
            }),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                warn!(error = %e, "Raft batch write failed");
                metrics::record_batch_write(false, batch_size, start.elapsed().as_secs_f64());
                return Err(status_with_correlation(
                    classify_raft_error(&e.to_string()),
                    &request_id,
                    &trace_ctx.trace_id,
                ));
            },
            Err(_elapsed) => {
                metrics::record_raft_proposal_timeout();
                metrics::record_batch_write(false, batch_size, start.elapsed().as_secs_f64());
                return Err(status_with_correlation(
                    Status::deadline_exceeded(format!(
                        "Raft proposal timed out after {}ms",
                        timeout.as_millis()
                    )),
                    &request_id,
                    &trace_ctx.trace_id,
                ));
            },
        };

        let response = result.data;
        let latency = start.elapsed().as_secs_f64();

        match response {
            LedgerResponse::Write { block_height, block_hash: _, assigned_sequence } => {
                // Generate proof if requested
                let (block_header, tx_proof) = if req.include_tx_proofs {
                    self.generate_write_proof(organization_id, vault_id, block_height)
                } else {
                    (None, None)
                };

                // Create WriteSuccess for idempotency cache
                let write_success = WriteSuccess {
                    tx_id: Some(TxId { id: Uuid::new_v4().as_bytes().to_vec() }),
                    block_height,
                    block_header: block_header.clone(),
                    tx_proof: tx_proof.clone(),
                    assigned_sequence,
                };

                // Cache the successful result
                self.idempotency.insert(
                    organization_id.value(),
                    vault_id.value(),
                    client_id.clone(),
                    idempotency_key,
                    request_hash,
                    write_success,
                );

                let success = BatchWriteSuccess {
                    tx_id: Some(TxId { id: Uuid::new_v4().as_bytes().to_vec() }),
                    block_height,
                    block_header,
                    tx_proof,
                    assigned_sequence,
                };

                metrics::record_batch_write(true, batch_size, latency);
                metrics::record_organization_operation(organization_id.value(), "write");
                metrics::record_organization_latency(organization_id.value(), "write", latency);
                info!(
                    trace_id = %trace_ctx.trace_id,
                    block_height,
                    assigned_sequence,
                    batch_size,
                    latency_ms = latency * 1000.0,
                    "Batch write committed"
                );

                Ok(response_with_correlation(
                    BatchWriteResponse {
                        result: Some(
                            inferadb_ledger_proto::proto::batch_write_response::Result::Success(
                                success,
                            ),
                        ),
                    },
                    &request_id,
                    &trace_ctx.trace_id,
                ))
            },
            _ => {
                warn!("Unexpected Raft response for batch write");
                metrics::record_batch_write(false, batch_size, latency);
                Err(status_with_correlation(
                    Status::internal("Unexpected response type"),
                    &request_id,
                    &trace_ctx.trace_id,
                ))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_shard_write_service_creation() {
        // Basic struct test - full testing requires Raft setup
    }

    #[test]
    fn test_single_shard_resolver_does_not_support_forwarding() {
        // Verify SingleShardResolver returns supports_forwarding() = false,
        // which ensures the write forwarding code path is never entered
        // for single-shard deployments.
        fn assert_no_forwarding(resolver: &dyn ShardResolver) {
            assert!(!resolver.supports_forwarding());
        }
        // SingleShardResolver::new requires Raft and state instances.
        // Instead, verify the trait default returns false.
        struct StubResolver;
        impl ShardResolver for StubResolver {
            fn resolve(
                &self,
                _: inferadb_ledger_types::OrganizationId,
            ) -> Result<crate::services::shard_resolver::ShardContext, tonic::Status> {
                Err(tonic::Status::unimplemented("stub"))
            }
            fn system_shard(
                &self,
            ) -> Result<crate::services::shard_resolver::ShardContext, tonic::Status> {
                Err(tonic::Status::unimplemented("stub"))
            }
        }
        let resolver = StubResolver;
        assert_no_forwarding(&resolver);
    }

    #[test]
    fn test_manager_field_defaults_to_none() {
        // Verify that MultiShardWriteService can be built without a manager
        // (the default for single-shard deployments). The builder should not
        // require the manager field.
        //
        // Cannot fully construct without a resolver and idempotency cache,
        // but we verify the struct definition accepts Option<_> for manager.
        fn _check_field_type(service: &MultiShardWriteService) {
            let _: &Option<Arc<MultiRaftManager>> = &service.manager;
        }
    }

    // Proto↔domain conversion tests (set_condition, operation variants) are in
    // proto_convert.rs which owns the centralized From/TryFrom implementations.
}
