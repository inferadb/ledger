//! Vault service implementation.
//!
//! Handles vault lifecycle: creation, deletion, retrieval, and listing.
//! Write operations (create, delete) flow through Raft for consistency;
//! read operations (get, list) hit the local applied state directly.
//!
//! Vault creation generates a Snowflake slug and initializes the vault's
//! blockchain with a genesis block via a single Raft entry.

use inferadb_ledger_proto::proto::{
    BlockHeader, CreateVaultRequest, CreateVaultResponse, DeleteVaultRequest, DeleteVaultResponse,
    GetVaultRequest, GetVaultResponse, Hash, ListVaultsRequest, ListVaultsResponse, NodeId,
    OrganizationSlug, UpdateVaultRequest, UpdateVaultResponse, VaultSlug as ProtoVaultSlug,
};
use inferadb_ledger_raft::{
    metrics, trace_context,
    types::{BlockRetentionMode, BlockRetentionPolicy, LedgerRequest, LedgerResponse},
};
use inferadb_ledger_types::{
    OrganizationSlug as DomainOrganizationSlug, VaultEntry, VaultSlug as DomainVaultSlug,
    ZERO_HASH,
    events::{EventAction, EventOutcome as EventOutcomeType},
};
use tonic::{Request, Response, Status};

use super::{service_infra::ServiceContext, slug_resolver::SlugResolver};

/// gRPC handler for vault lifecycle operations.
pub struct VaultService {
    ctx: ServiceContext,
}

impl VaultService {
    /// Creates a new `VaultService` from shared service infrastructure.
    pub(crate) fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::vault_service_server::VaultService for VaultService {
    /// Creates a new vault within an organization, generating a Snowflake slug and initializing
    /// its blockchain via Raft.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn create_vault(
        &self,
        request: Request<CreateVaultRequest>,
    ) -> Result<Response<CreateVaultResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        // Reject if node is draining
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "VaultService",
            "create_vault",
            &grpc_metadata,
            &trace_ctx,
        );
        super::helpers::extract_caller(&mut ctx, &req.caller);

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let organization_slug_val = req.organization.as_ref().map_or(0, |n| n.slug);
        let organization_id =
            slug_resolver.extract_and_resolve(&req.organization).inspect_err(|status| {
                ctx.set_error("InvalidArgument", status.message());
            })?;

        ctx.set_organization(organization_slug_val);

        // Convert proto retention policy to internal type
        let retention_policy = req.retention_policy.map(|proto_policy| {
            let policy = BlockRetentionPolicy::from(&proto_policy);
            match policy.mode {
                BlockRetentionMode::Full => ctx.set_retention_mode("full"),
                BlockRetentionMode::Compacted => ctx.set_retention_mode("compacted"),
            }
            policy
        });

        // Generate vault slug via Snowflake before Raft proposal
        let vault = inferadb_ledger_types::snowflake::generate_vault_slug().map_err(|e| {
            tracing::error!(error = %e, "Failed to generate vault slug");
            Status::internal("Internal error")
        })?;

        let ledger_request = LedgerRequest::CreateVault {
            organization: organization_id,
            slug: vault,
            name: None,
            retention_policy,
        };

        let response = self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::VaultCreated { vault: vault_id, slug } => {
                ctx.set_vault(slug.value());
                ctx.set_success();
                metrics::record_organization_operation(organization_id, "create_vault");
                metrics::record_organization_latency(
                    organization_id,
                    "create_vault",
                    ctx.elapsed_secs(),
                );

                if let Some(node_id) = self.ctx.node_id {
                    self.ctx.record_handler_event(
                        inferadb_ledger_raft::event_writer::HandlerPhaseEmitter::for_organization(
                            EventAction::VaultCreated,
                            organization_id,
                            Some(DomainOrganizationSlug::new(organization_slug_val)),
                            node_id,
                        )
                        .vault(slug)
                        .principal("system")
                        .trace_id(&trace_ctx.trace_id)
                        .outcome(EventOutcomeType::Success)
                        .build(self.ctx.default_ttl_days()),
                    );
                }

                // Get Raft metrics for leader_id and term
                let (leader_id, current_term) = if let Some(raft_metrics) = self.ctx.raft_metrics()
                {
                    let leader = raft_metrics.current_leader.unwrap_or(raft_metrics.id);
                    ctx.set_raft_term(raft_metrics.current_term);
                    (leader, raft_metrics.current_term)
                } else {
                    // Fallback when metrics are unavailable (e.g., test mocks).
                    (0, 0)
                };

                // Compute empty state root for genesis block
                let state_root = self.ctx.state.compute_state_root(vault_id).map_err(|e| {
                    tracing::error!(
                        vault_id = vault_id.value(),
                        error = %e,
                        "Failed to compute state root for genesis block"
                    );
                    Status::internal("Failed to compute state root for genesis block")
                })?;

                // Build genesis block header (height 0)
                let genesis_entry = VaultEntry {
                    organization: organization_id,
                    vault: vault_id,
                    vault_height: 0,
                    previous_vault_hash: ZERO_HASH,
                    transactions: vec![],
                    tx_merkle_root: ZERO_HASH,
                    state_root,
                };
                let genesis_hash = inferadb_ledger_types::vault_entry_hash(&genesis_entry);
                let organization = slug_resolver.resolve_slug(organization_id)?;
                let genesis = BlockHeader {
                    height: 0,
                    organization: Some(OrganizationSlug { slug: organization.value() }),
                    vault: Some(ProtoVaultSlug { slug: slug.value() }),
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
                    term: current_term,
                    committed_index: 0, // Not available via propose_request helper
                    block_hash: Some(Hash { value: genesis_hash.to_vec() }),
                };

                Ok(Response::new(CreateVaultResponse {
                    vault: Some(ProtoVaultSlug { slug: slug.value() }),
                    genesis: Some(genesis),
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

    /// Deletes a vault and its associated blockchain data via Raft consensus.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn delete_vault(
        &self,
        request: Request<DeleteVaultRequest>,
    ) -> Result<Response<DeleteVaultResponse>, Status> {
        // Reject requests with insufficient remaining deadline
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        // Reject if node is draining
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "VaultService",
            "delete_vault",
            &grpc_metadata,
            &trace_ctx,
        );
        super::helpers::extract_caller(&mut ctx, &req.caller);

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
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

        // Submit delete vault through Raft
        let ledger_request =
            LedgerRequest::DeleteVault { organization: organization_id, vault: vault_id };

        let response = self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::VaultDeleted { success } => {
                if success {
                    ctx.set_success();
                    metrics::record_organization_operation(organization_id, "delete_vault");
                    metrics::record_organization_latency(
                        organization_id,
                        "delete_vault",
                        ctx.elapsed_secs(),
                    );

                    if let Some(node_id) = self.ctx.node_id {
                        self.ctx.record_handler_event(
                            inferadb_ledger_raft::event_writer::HandlerPhaseEmitter::for_organization(
                                EventAction::VaultDeleted,
                                organization_id,
                                Some(DomainOrganizationSlug::new(organization_slug_val)),
                                node_id,
                            )
                            .vault(DomainVaultSlug::new(vault_val))
                            .principal("system")
                            .trace_id(&trace_ctx.trace_id)
                            .outcome(EventOutcomeType::Success)
                            .build(self.ctx.default_ttl_days()),
                        );
                    }

                    Ok(Response::new(DeleteVaultResponse {
                        deleted_at: Some(
                            prost_types::Timestamp::from(std::time::SystemTime::now()),
                        ),
                    }))
                } else {
                    ctx.set_error("DeleteFailed", "Failed to delete vault");
                    Err(Status::internal("Failed to delete vault"))
                }
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

    /// Retrieves vault metadata by organization and vault slug, including block height and status.
    ///
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn get_vault(
        &self,
        request: Request<GetVaultRequest>,
    ) -> Result<Response<GetVaultResponse>, Status> {
        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx =
            self.ctx.make_request_context("VaultService", "get_vault", &grpc_metadata, &trace_ctx);
        super::helpers::extract_caller(&mut ctx, &req.caller);

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
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

        // Get vault metadata and height
        let vault_meta = self.ctx.applied_state.get_vault(organization_id, vault_id);
        let height = self.ctx.applied_state.vault_height(organization_id, vault_id);

        match vault_meta {
            Some(vault) => {
                ctx.set_block_height(height);
                ctx.set_success();
                let organization = slug_resolver.resolve_slug(organization_id)?;
                Ok(Response::new(GetVaultResponse {
                    organization: Some(OrganizationSlug { slug: organization.value() }),
                    vault: Some(ProtoVaultSlug { slug: vault.slug.value() }),
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

    /// Lists all vaults across all organizations with their block heights.
    ///
    /// Supports cursor-based pagination via `page_token` / `page_size`.
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn list_vaults(
        &self,
        request: Request<ListVaultsRequest>,
    ) -> Result<Response<ListVaultsResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "VaultService",
            "list_vaults",
            &grpc_metadata,
            &trace_ctx,
        );
        super::helpers::extract_caller(&mut ctx, &req.caller);

        let page_size = crate::proto_compat::normalize_page_size(req.page_size);
        let start_after = crate::proto_compat::decode_page_token(&req.page_token);

        // Build (vault_slug, response) pairs for pagination
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let org_filter = req.organization.as_ref().map(|o| o.slug);
        // Collect vault identifiers without cloning the entire heights map
        let mut vault_keys = Vec::new();
        self.ctx.applied_state.for_each_vault_height(|org, vault, _| vault_keys.push((org, vault)));

        let vaults_with_slugs: Vec<(u64, inferadb_ledger_proto::proto::GetVaultResponse)> =
            vault_keys
                .iter()
                .filter_map(|(org_id, vault_id)| {
                    self.ctx.applied_state.get_vault(*org_id, *vault_id).map(|v| {
                        let height = self.ctx.applied_state.vault_height(v.organization, v.vault);
                        let organization = slug_resolver.resolve_slug(v.organization)?;
                        // Skip if org filter is set and doesn't match
                        if let Some(filter_slug) = org_filter
                            && organization.value() != filter_slug
                        {
                            return Ok(None);
                        }
                        Ok(Some((
                            v.slug.value(),
                            inferadb_ledger_proto::proto::GetVaultResponse {
                                organization: Some(OrganizationSlug { slug: organization.value() }),
                                vault: Some(ProtoVaultSlug { slug: v.slug.value() }),
                                height,
                                state_root: None,
                                nodes: vec![],
                                leader: None,
                                status: inferadb_ledger_proto::proto::VaultStatus::Active.into(),
                                retention_policy: None,
                            },
                        )))
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?
                .into_iter()
                .flatten()
                .collect();

        let (vaults, next_page_token) =
            crate::proto_compat::paginate_by_slug(vaults_with_slugs, start_after, page_size);

        ctx.set_keys_count(vaults.len());
        ctx.set_success();

        Ok(Response::new(ListVaultsResponse { vaults, next_page_token }))
    }

    /// Updates vault metadata (retention policy).
    async fn update_vault(
        &self,
        request: Request<UpdateVaultRequest>,
    ) -> Result<Response<UpdateVaultResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "VaultService",
            "update_vault",
            &grpc_metadata,
            &trace_ctx,
        );
        super::helpers::extract_caller(&mut ctx, &req.caller);

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
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

        // Convert proto retention policy to domain type
        let retention_policy = req.retention_policy.map(|p| {
            let mode = match p.mode() {
                inferadb_ledger_proto::proto::BlockRetentionMode::Unspecified
                | inferadb_ledger_proto::proto::BlockRetentionMode::Full => {
                    BlockRetentionMode::Full
                },
                inferadb_ledger_proto::proto::BlockRetentionMode::Compacted => {
                    BlockRetentionMode::Compacted
                },
            };
            BlockRetentionPolicy { mode, retention_blocks: p.retention_blocks }
        });

        let ledger_request = LedgerRequest::UpdateVault {
            organization: organization_id,
            vault: vault_id,
            retention_policy,
        };

        let response = self.ctx.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::VaultUpdated { success } => {
                if success {
                    ctx.set_success();
                    metrics::record_organization_operation(organization_id, "update_vault");
                    metrics::record_organization_latency(
                        organization_id,
                        "update_vault",
                        ctx.elapsed_secs(),
                    );
                    Ok(Response::new(UpdateVaultResponse {}))
                } else {
                    ctx.set_error("UpdateFailed", "Failed to update vault");
                    Err(Status::internal("Failed to update vault"))
                }
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
}
