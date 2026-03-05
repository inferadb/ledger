//! Vault service implementation.
//!
//! Handles vault lifecycle: creation, deletion, retrieval, and listing.
//! Write operations (create, delete) flow through Raft for consistency;
//! read operations (get, list) hit the local applied state directly.
//!
//! Vault creation generates a Snowflake slug and initializes the vault's
//! blockchain with a genesis block via a single Raft entry.

use std::{sync::Arc, time::Duration};

use inferadb_ledger_proto::proto::{
    BlockHeader, CreateVaultRequest, CreateVaultResponse, DeleteVaultRequest, DeleteVaultResponse,
    GetVaultRequest, GetVaultResponse, Hash, ListVaultsRequest, ListVaultsResponse, NodeId,
    OrganizationSlug, VaultSlug as ProtoVaultSlug,
};
use inferadb_ledger_raft::{
    log_storage::AppliedStateAccessor,
    logging::{RequestContext, Sampler},
    metrics, trace_context,
    types::{
        BlockRetentionMode, BlockRetentionPolicy, LedgerRequest, LedgerResponse, LedgerTypeConfig,
    },
};
use inferadb_ledger_state::StateLayer;
use inferadb_ledger_store::FileBackend;
use inferadb_ledger_types::{
    OrganizationSlug as DomainOrganizationSlug, VaultEntry, VaultSlug as DomainVaultSlug,
    ZERO_HASH,
    config::ValidationConfig,
    events::{EventAction, EventOutcome as EventOutcomeType},
};
use openraft::Raft;
use tonic::{Request, Response, Status};

use super::slug_resolver::SlugResolver;

/// Creates a `RequestContext` for a VaultService method and fills in common fields.
macro_rules! vault_ctx {
    ($self:expr, $method:literal, $grpc_metadata:expr, $trace_ctx:expr) => {{
        let mut ctx = RequestContext::new("VaultService", $method);
        ctx.set_admin_action($method);
        $self.fill_context(&mut ctx, $grpc_metadata, $trace_ctx);
        ctx
    }};
}

/// Vault lifecycle: creation, deletion, retrieval, and listing.
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct VaultService {
    /// Raft consensus handle for proposing write operations.
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// State layer for entity reads (state root computation).
    state: Arc<StateLayer<FileBackend>>,
    /// Accessor for applied state (slug resolution, vault metadata).
    applied_state: AppliedStateAccessor,
    /// Sampler for log tail sampling.
    #[builder(default)]
    sampler: Option<Sampler>,
    /// Node ID for logging and events.
    #[builder(default)]
    node_id: Option<u64>,
    /// Input validation configuration.
    #[builder(default = Arc::new(ValidationConfig::default()))]
    #[allow(dead_code)] // reserved for future vault-level input validation
    validation_config: Arc<ValidationConfig>,
    /// Maximum Raft proposal timeout.
    #[builder(default = Duration::from_secs(30))]
    proposal_timeout: Duration,
    /// Handler-phase event handle for audit events.
    #[builder(default)]
    event_handle:
        Option<inferadb_ledger_raft::event_writer::EventHandle<inferadb_ledger_store::FileBackend>>,
    /// Health state for drain-phase write rejection.
    #[builder(default)]
    health_state: Option<inferadb_ledger_raft::graceful_shutdown::HealthState>,
}

impl VaultService {
    /// Records a handler-phase audit event if the event handle is configured.
    fn record_handler_event(&self, entry: inferadb_ledger_types::events::EventEntry) {
        if let Some(ref handle) = self.event_handle {
            handle.record_handler_event(entry);
        }
    }

    /// Returns the configured TTL for handler-phase events, defaulting to 90 days.
    fn default_ttl_days(&self) -> u32 {
        self.event_handle.as_ref().map_or(90, |h| h.config().default_ttl_days)
    }

    /// Fills in common fields on a pre-created `RequestContext`.
    fn fill_context(
        &self,
        ctx: &mut RequestContext,
        grpc_metadata: &tonic::metadata::MetadataMap,
        trace_ctx: &trace_context::TraceContext,
    ) {
        super::service_infra::fill_context(
            ctx,
            grpc_metadata,
            trace_ctx,
            self.sampler.as_ref(),
            self.node_id,
        );
    }

    /// Proposes a request through Raft with deadline handling.
    async fn propose_request(
        &self,
        request: LedgerRequest,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut RequestContext,
    ) -> Result<LedgerResponse, Status> {
        super::service_infra::propose_request(
            &self.raft,
            request,
            grpc_metadata,
            self.proposal_timeout,
            ctx,
        )
        .await
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
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = vault_ctx!(self, "create_vault", &grpc_metadata, &trace_ctx);

        let slug_resolver = SlugResolver::new(self.applied_state.clone());
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
        let vault = inferadb_ledger_types::snowflake::generate_vault_slug()
            .map_err(|e| Status::internal(format!("Failed to generate vault slug: {}", e)))?;

        let ledger_request = LedgerRequest::CreateVault {
            organization: organization_id,
            slug: vault,
            name: None, // CreateVaultRequest doesn't have name field
            retention_policy,
        };

        let response = self.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

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

                if let Some(node_id) = self.node_id {
                    self.record_handler_event(
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
                        .build(self.default_ttl_days()),
                    );
                }

                // Get Raft metrics for leader_id and term
                let raft_metrics = self.raft.metrics().borrow().clone();
                let leader_id = raft_metrics.current_leader.unwrap_or(raft_metrics.id);
                ctx.set_raft_term(raft_metrics.current_term);

                // Compute empty state root for genesis block
                let state_root = self.state.compute_state_root(vault_id).unwrap_or_else(|e| {
                    tracing::warn!(
                        vault_id = vault_id.value(),
                        error = %e,
                        "Failed to compute state root for genesis block, using ZERO_HASH"
                    );
                    ZERO_HASH
                });

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
                    term: raft_metrics.current_term,
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
                Err(crate::proto_compat::error_code_to_status(code, message))
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
        super::helpers::check_not_draining(self.health_state.as_ref())?;

        // Extract trace context from gRPC metadata before consuming the request
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = vault_ctx!(self, "delete_vault", &grpc_metadata, &trace_ctx);

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

        // Submit delete vault through Raft
        let ledger_request =
            LedgerRequest::DeleteVault { organization: organization_id, vault: vault_id };

        let response = self.propose_request(ledger_request, &grpc_metadata, &mut ctx).await?;

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

                    if let Some(node_id) = self.node_id {
                        self.record_handler_event(
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
                            .build(self.default_ttl_days()),
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
                Err(crate::proto_compat::error_code_to_status(code, message))
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

        let mut ctx = vault_ctx!(self, "get_vault", &grpc_metadata, &trace_ctx);

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

        // Get vault metadata and height
        let vault_meta = self.applied_state.get_vault(organization_id, vault_id);
        let height = self.applied_state.vault_height(organization_id, vault_id);

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
    /// Slug-to-ID resolution occurs at the service boundary via `SlugResolver`.
    async fn list_vaults(
        &self,
        request: Request<ListVaultsRequest>,
    ) -> Result<Response<ListVaultsResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let _req = request.into_inner();

        let mut ctx = vault_ctx!(self, "list_vaults", &grpc_metadata, &trace_ctx);

        // List all vaults across all organizations
        let slug_resolver = SlugResolver::new(self.applied_state.clone());
        let vaults = self
            .applied_state
            .all_vault_heights()
            .keys()
            .filter_map(|(org_id, vault_id)| {
                self.applied_state.get_vault(*org_id, *vault_id).map(|v| {
                    let height = self.applied_state.vault_height(v.organization, v.vault);
                    let organization = slug_resolver.resolve_slug(v.organization)?;
                    Ok(inferadb_ledger_proto::proto::GetVaultResponse {
                        organization: Some(OrganizationSlug { slug: organization.value() }),
                        vault: Some(ProtoVaultSlug { slug: v.slug.value() }),
                        height,
                        state_root: None,
                        nodes: vec![],
                        leader: None,
                        status: inferadb_ledger_proto::proto::VaultStatus::Active.into(),
                        retention_policy: None,
                    })
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        // Set count in context for observability
        ctx.set_keys_count(vaults.len());
        ctx.set_success();

        Ok(Response::new(ListVaultsResponse { vaults, next_page_token: None }))
    }
}
