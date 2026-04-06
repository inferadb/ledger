//! App service implementation.
//!
//! Handles organization-scoped client application lifecycle: creation, deletion,
//! retrieval, listing, credential management, and vault connections.
//! All mutations flow through Raft for consistency; read operations hit the
//! local applied state and state layer directly.
//!
//! All mutations require Organization Administrator authorization (enforced
//! at the Engine/Control layer upstream; Ledger trusts all requests).

use std::{sync::Arc, time::Duration};

use inferadb_ledger_proto::proto::{
    self, AddAppVaultRequest, AddAppVaultResponse, AppClientAssertionInfo, AppCredentialType,
    AppCredentialsInfo, AppInfo, AppVaultConnectionInfo, CreateAppClientAssertionRequest,
    CreateAppClientAssertionResponse, CreateAppRequest, CreateAppResponse,
    DeleteAppClientAssertionRequest, DeleteAppClientAssertionResponse, DeleteAppRequest,
    DeleteAppResponse, GetAppClientSecretRequest, GetAppClientSecretResponse, GetAppRequest,
    GetAppResponse, ListAppClientAssertionsRequest, ListAppClientAssertionsResponse,
    ListAppVaultsRequest, ListAppVaultsResponse, ListAppsRequest, ListAppsResponse,
    RemoveAppVaultRequest, RemoveAppVaultResponse, RotateAppClientSecretRequest,
    RotateAppClientSecretResponse, SetAppClientAssertionEnabledRequest,
    SetAppClientAssertionEnabledResponse, SetAppCredentialEnabledRequest,
    SetAppCredentialEnabledResponse, SetAppEnabledRequest, SetAppEnabledResponse, UpdateAppRequest,
    UpdateAppResponse, UpdateAppVaultRequest, UpdateAppVaultResponse,
};
use inferadb_ledger_raft::{
    trace_context,
    types::{LedgerRequest, LedgerResponse},
};
use inferadb_ledger_state::system::{
    App, AppCredentialType as DomainAppCredentialType, AppVaultConnection, ClientAssertionEntry,
    SYSTEM_VAULT_ID, SystemKeys,
};
use inferadb_ledger_types::{
    AppId as DomainAppId, ClientAssertionId as DomainClientAssertionId,
    OrganizationId as DomainOrganizationId, OrganizationSlug as DomainOrganizationSlug, decode,
    events::{EventAction, EventOutcome as EventOutcomeType},
};
use moka::sync::Cache as MokaCache;
use tonic::{Request, Response, Status};
use zeroize::Zeroizing;

use super::{service_infra::ServiceContext, slug_resolver::SlugResolver};

/// Cache key for credential idempotency, scoped to prevent cross-tenant collisions.
type CredentialCacheKey = (DomainOrganizationId, DomainAppId, [u8; 16]);

/// Cached credential material for idempotent retry.
///
/// When a client retries `RotateAppClientSecret` or `CreateAppClientAssertion`
/// with the same idempotency key, the cached secret/PEM is returned instead of
/// generating new material (which would orphan the previously generated one).
///
/// Sensitive fields are wrapped in [`Zeroizing`] to ensure plaintext secrets
/// and private key material are wiped from memory on eviction/drop.
#[derive(Clone, Debug)]
pub(crate) enum CachedCredential {
    /// Cached plaintext secret from `RotateAppClientSecret`.
    Secret(Zeroizing<String>),
    /// Cached keypair from `CreateAppClientAssertion`.
    Assertion {
        /// Public key bytes (Ed25519, 32 bytes) — not secret, but included for completeness.
        public_key_bytes: Vec<u8>,
        /// Private key PEM (PKCS#8).
        private_key_pem: Zeroizing<String>,
    },
}

/// Organization-scoped client application management.
pub struct AppService {
    ctx: ServiceContext,
    /// Credential idempotency cache scoped by `(org, app, idempotency_key)`.
    ///
    /// Caches generated secret/keypair material so retries with the same
    /// idempotency key return the same material instead of generating new
    /// credentials that would orphan the previously generated ones.
    ///
    /// TTL is 10 minutes — sufficient for retry windows while minimizing
    /// the duration sensitive material persists in heap memory.
    credential_cache: Arc<MokaCache<CredentialCacheKey, CachedCredential>>,
}

impl AppService {
    /// Creates a new `AppService` from shared service infrastructure.
    pub(crate) fn new(ctx: ServiceContext) -> Self {
        Self {
            ctx,
            credential_cache: Arc::new(
                MokaCache::builder()
                    .max_capacity(10_000)
                    .time_to_live(Duration::from_secs(10 * 60))
                    .build(),
            ),
        }
    }

    /// Constructs a slug resolver from the applied state.
    fn resolver(&self) -> SlugResolver {
        SlugResolver::new(self.ctx.applied_state.clone())
    }

    /// Records an audit event for an app mutation if the event handle is configured.
    fn emit_event(
        &self,
        action: EventAction,
        org_id: DomainOrganizationId,
        org_slug_val: u64,
        trace_id: &str,
    ) {
        if let Some(node_id) = self.ctx.node_id {
            self.ctx.record_handler_event(
                inferadb_ledger_raft::event_writer::HandlerPhaseEmitter::for_organization(
                    action,
                    org_id,
                    Some(DomainOrganizationSlug::new(org_slug_val)),
                    node_id,
                )
                .principal("system")
                .trace_id(trace_id)
                .outcome(EventOutcomeType::Success)
                .build(self.ctx.default_ttl_days()),
            );
        }
    }

    /// Loads an app from GLOBAL state and merges with `AppProfile` from REGIONAL state.
    ///
    /// The structural `App` record (credentials, enabled, slug) lives in GLOBAL.
    /// PII fields (name, description) live in a separate `AppProfile` in REGIONAL.
    /// Loads both and overlays the PII fields onto the returned `App`.
    fn load_app(&self, org_id: DomainOrganizationId, app_id: DomainAppId) -> Result<App, Status> {
        let mut app = super::helpers::load_app(&self.ctx.state, org_id, app_id)?;
        self.overlay_app_profile(&mut app, org_id);
        Ok(app)
    }

    /// Overlays PII fields from a REGIONAL `AppProfile` onto a GLOBAL `App`.
    fn overlay_app_profile(&self, app: &mut App, org_id: DomainOrganizationId) {
        let regional = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .and_then(|meta| self.ctx.regional_state(meta.region).ok());
        let Some(state) = regional else { return };
        let key = SystemKeys::app_profile_key(org_id, app.id);
        let Ok(Some(entity)) = state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) else { return };
        if let Ok(profile) = decode::<inferadb_ledger_state::system::AppProfile>(&entity.value) {
            app.name = profile.name;
            app.description = profile.description;
        }
    }

    /// Converts a domain `App` to a proto `AppInfo` message.
    fn app_to_proto(&self, app: &App, include_credentials: bool) -> Result<AppInfo, Status> {
        let resolver = self.resolver();
        let slug = resolver.resolve_app_slug(app.id)?;

        let credentials = if include_credentials {
            Some(AppCredentialsInfo {
                client_secret_enabled: app.credentials.client_secret.enabled,
                mtls_ca_enabled: app.credentials.mtls_ca.enabled,
                mtls_self_signed_enabled: app.credentials.mtls_self_signed.enabled,
                client_assertion_enabled: app.credentials.client_assertion.enabled,
            })
        } else {
            None
        };

        Ok(AppInfo {
            slug: Some(proto::AppSlug { slug: slug.value() }),
            name: app.name.clone(),
            description: app.description.clone(),
            enabled: app.enabled,
            credentials,
            created_at: Some(crate::proto_compat::datetime_to_proto(&app.created_at)),
            updated_at: Some(crate::proto_compat::datetime_to_proto(&app.updated_at)),
        })
    }

    /// Lists all apps in an organization, merging GLOBAL structural data with
    /// REGIONAL PII (name, description) from `AppProfile` records.
    fn list_apps_internal(&self, org_id: DomainOrganizationId) -> Result<Vec<App>, Status> {
        let prefix = SystemKeys::app_prefix(org_id);
        let entities =
            self.ctx.state.list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, 10000).map_err(
                |e| {
                    tracing::error!(error = %e, "Failed to list apps");
                    Status::internal("Internal error")
                },
            )?;

        let mut apps = Vec::with_capacity(entities.len());
        for entity in &entities {
            match decode::<App>(&entity.value) {
                Ok(mut app) => {
                    self.overlay_app_profile(&mut app, org_id);
                    apps.push(app);
                },
                Err(e) => {
                    tracing::warn!(error = %e, "corrupt app entry, skipping");
                },
            }
        }
        Ok(apps)
    }

    /// Lists all vault connections for an app from the state layer.
    fn list_vault_connections(
        &self,
        org_id: DomainOrganizationId,
        app_id: DomainAppId,
    ) -> Result<Vec<AppVaultConnection>, Status> {
        let prefix = SystemKeys::app_vault_prefix(org_id, app_id);
        let entities =
            self.ctx.state.list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, 10000).map_err(
                |e| {
                    tracing::error!(error = %e, "Failed to list vault connections");
                    Status::internal("Internal error")
                },
            )?;

        let mut connections = Vec::with_capacity(entities.len());
        for entity in &entities {
            match decode::<AppVaultConnection>(&entity.value) {
                Ok(conn) => connections.push(conn),
                Err(e) => {
                    tracing::warn!(error = %e, "corrupt vault connection entry, skipping");
                },
            }
        }
        Ok(connections)
    }

    /// Reads a vault connection from the state layer after a Raft mutation.
    fn read_vault_connection(
        &self,
        org_id: DomainOrganizationId,
        app_id: DomainAppId,
        vault_id: inferadb_ledger_types::VaultId,
    ) -> Result<AppVaultConnection, Status> {
        super::helpers::read_vault_connection(
            &self.ctx.state,
            org_id,
            app_id,
            vault_id,
            Status::internal("Vault connection not found after mutation"),
        )
    }

    /// Converts a domain `AppVaultConnection` to a proto `AppVaultConnectionInfo`.
    fn vault_connection_to_proto(conn: &AppVaultConnection) -> AppVaultConnectionInfo {
        AppVaultConnectionInfo {
            vault: Some(proto::VaultSlug { slug: conn.vault_slug.value() }),
            allowed_scopes: conn.allowed_scopes.clone(),
            created_at: Some(crate::proto_compat::datetime_to_proto(&conn.created_at)),
            updated_at: Some(crate::proto_compat::datetime_to_proto(&conn.updated_at)),
        }
    }

    /// Lists all client assertion entries for an app from the state layer.
    fn list_assertions_internal(
        &self,
        org_id: DomainOrganizationId,
        app_id: DomainAppId,
    ) -> Result<Vec<ClientAssertionEntry>, Status> {
        let prefix = SystemKeys::app_assertion_prefix(org_id, app_id);
        let entities =
            self.ctx.state.list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, 10000).map_err(
                |e| {
                    tracing::error!(error = %e, "Failed to list assertions");
                    Status::internal("Internal error")
                },
            )?;

        let mut entries = Vec::with_capacity(entities.len());
        for entity in &entities {
            match decode::<ClientAssertionEntry>(&entity.value) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    tracing::warn!(error = %e, "corrupt assertion entry, skipping");
                },
            }
        }
        Ok(entries)
    }

    /// Converts a domain `ClientAssertionEntry` to a proto `AppClientAssertionInfo`.
    ///
    /// The `name` parameter is loaded separately from REGIONAL state.
    fn assertion_to_proto(entry: &ClientAssertionEntry, name: String) -> AppClientAssertionInfo {
        AppClientAssertionInfo {
            id: Some(proto::ClientAssertionId { id: entry.id.value() }),
            name,
            enabled: entry.enabled,
            expires_at: Some(crate::proto_compat::datetime_to_proto(&entry.expires_at)),
            created_at: Some(crate::proto_compat::datetime_to_proto(&entry.created_at)),
        }
    }

    /// Loads the assertion name from REGIONAL state.
    ///
    /// Returns an empty string if the organization's region is unavailable
    /// or the name record does not exist (graceful degradation for read path).
    fn load_assertion_name(
        &self,
        org_id: DomainOrganizationId,
        app_id: DomainAppId,
        assertion_id: inferadb_ledger_types::ClientAssertionId,
    ) -> String {
        let regional = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .and_then(|meta| self.ctx.regional_state(meta.region).ok());
        let Some(state) = regional else {
            return String::new();
        };
        let key = SystemKeys::assertion_name_key(org_id, app_id, assertion_id);
        match state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
            Ok(Some(entity)) => String::from_utf8(entity.value).unwrap_or_default(),
            _ => String::new(),
        }
    }
}

/// Parses a 16-byte idempotency key from raw bytes.
///
/// Returns `Ok(None)` for empty input (no idempotency), `Ok(Some(key))` for
/// exactly 16 bytes, and `Err` for any other length.
fn parse_idempotency_key(bytes: &[u8]) -> Result<Option<[u8; 16]>, Status> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let key: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("idempotency_key must be exactly 16 bytes"))?;
    Ok(Some(key))
}

/// Maps a proto `AppCredentialType` to the domain enum.
fn map_credential_type(proto_type: i32) -> Result<DomainAppCredentialType, Status> {
    match AppCredentialType::try_from(proto_type) {
        Ok(AppCredentialType::ClientSecret) => Ok(DomainAppCredentialType::ClientSecret),
        Ok(AppCredentialType::MtlsCa) => Ok(DomainAppCredentialType::MtlsCa),
        Ok(AppCredentialType::MtlsSelfSigned) => Ok(DomainAppCredentialType::MtlsSelfSigned),
        Ok(AppCredentialType::ClientAssertion) => Ok(DomainAppCredentialType::ClientAssertion),
        _ => Err(Status::invalid_argument("Invalid or unspecified credential type")),
    }
}

#[tonic::async_trait]
impl proto::app_service_server::AppService for AppService {
    async fn create_app(
        &self,
        request: Request<CreateAppRequest>,
    ) -> Result<Response<CreateAppResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx =
            self.ctx.make_request_context("AppService", "CreateApp", &grpc_metadata, &trace_ctx);

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let slug = inferadb_ledger_types::snowflake::generate_app_slug().map_err(|e| {
            tracing::error!(error = %e, "Failed to generate app slug");
            Status::internal("Internal error")
        })?;

        let name = inner.name.trim().to_string();
        if name.is_empty() {
            return Err(Status::invalid_argument("App name must not be empty"));
        }

        // Step 1 (GLOBAL): Create app directory entry (ID + slug only, no PII).
        let response = self
            .ctx
            .propose_request(
                LedgerRequest::CreateApp { organization: org_id, slug },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            LedgerResponse::Error { code, message } => {
                return Err(super::helpers::error_code_to_status(code, message));
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                return Err(Status::internal("Unexpected response from state machine"));
            },
        };

        // Step 2 (REGIONAL): Write app name and description to the org's regional store.
        // Encrypted with OrgShredKey for crypto-shredding on organization purge.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| tonic::Status::not_found("Organization not found"))?;
        let system_request = inferadb_ledger_raft::types::SystemRequest::WriteAppProfile {
            organization: org_id,
            app: app_id,
            name,
            description: inner.description,
        };
        let profile_response = self
            .ctx
            .propose_regional_org_encrypted(
                org_meta.region,
                system_request,
                org_id,
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        if let LedgerResponse::Error { code, message } = profile_response {
            return Err(super::helpers::error_code_to_status(code, message));
        }

        self.emit_event(EventAction::AppCreated, org_id, org_slug_val, &trace_ctx.trace_id);
        let app = self.load_app(org_id, app_id)?;
        let info = self.app_to_proto(&app, true)?;
        Ok(Response::new(CreateAppResponse { app: Some(info) }))
    }

    async fn get_app(
        &self,
        request: Request<GetAppRequest>,
    ) -> Result<Response<GetAppResponse>, Status> {
        let inner = request.into_inner();
        let resolver = self.resolver();
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (resolved_org, app_id) = resolver.extract_and_resolve_app(&inner.app)?;
        if org_id != resolved_org {
            return Err(Status::not_found("App not found in the specified organization"));
        }

        let app = self.load_app(resolved_org, app_id)?;
        let info = self.app_to_proto(&app, true)?;
        Ok(Response::new(GetAppResponse { app: Some(info) }))
    }

    async fn list_apps(
        &self,
        request: Request<ListAppsRequest>,
    ) -> Result<Response<ListAppsResponse>, Status> {
        let inner = request.into_inner();
        let resolver = self.resolver();
        let org_id = resolver.extract_and_resolve(&inner.organization)?;

        let apps = self.list_apps_internal(org_id)?;
        let mut proto_apps = Vec::with_capacity(apps.len());
        for app in &apps {
            match self.app_to_proto(app, false) {
                Ok(info) => proto_apps.push(info),
                Err(e) => {
                    tracing::warn!(error = %e, "failed to convert app to proto, skipping");
                },
            }
        }
        Ok(Response::new(ListAppsResponse { apps: proto_apps }))
    }

    async fn update_app(
        &self,
        request: Request<UpdateAppRequest>,
    ) -> Result<Response<UpdateAppResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx =
            self.ctx.make_request_context("AppService", "UpdateApp", &grpc_metadata, &trace_ctx);

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;

        let name = inner
            .name
            .map(|n| {
                let trimmed = n.trim().to_string();
                if trimmed.is_empty() {
                    return Err(Status::invalid_argument("App name must not be empty"));
                }
                Ok(trimmed)
            })
            .transpose()?;

        let description = inner.description;

        // Require at least one field to update
        if name.is_none() && description.is_none() {
            return Err(Status::invalid_argument("At least one field must be provided for update"));
        }

        // Load current app to merge unchanged fields
        let current_app = self.load_app(org_id, app_id)?;
        let effective_name = name.unwrap_or_else(|| current_app.name.clone());
        let effective_description = match description {
            Some(d) => Some(d),
            None => current_app.description.clone(),
        };

        // Route to the org's regional Raft group (name/description are PII).
        // Encrypted with OrgShredKey for crypto-shredding on organization purge.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| tonic::Status::not_found("Organization not found"))?;
        let system_request = inferadb_ledger_raft::types::SystemRequest::WriteAppProfile {
            organization: org_id,
            app: app_id,
            name: effective_name,
            description: effective_description,
        };
        let response = self
            .ctx
            .propose_regional_org_encrypted(
                org_meta.region,
                system_request,
                org_id,
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            _ => {
                self.emit_event(EventAction::AppUpdated, org_id, org_slug_val, &trace_ctx.trace_id);
                let app = self.load_app(org_id, app_id)?;
                let info = self.app_to_proto(&app, true)?;
                Ok(Response::new(UpdateAppResponse { app: Some(info) }))
            },
        }
    }

    async fn delete_app(
        &self,
        request: Request<DeleteAppRequest>,
    ) -> Result<Response<DeleteAppResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx =
            self.ctx.make_request_context("AppService", "DeleteApp", &grpc_metadata, &trace_ctx);

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;

        // Step 1 (REGIONAL): Delete AppProfile and name index.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| Status::not_found("Organization not found"))?;
        let delete_profile = inferadb_ledger_raft::types::SystemRequest::DeleteAppProfile {
            organization: org_id,
            app: app_id,
        };
        let profile_response = self
            .ctx
            .propose_regional(org_meta.region, delete_profile, &grpc_metadata, &mut ctx)
            .await?;
        if let LedgerResponse::Error { code, message } = profile_response {
            return Err(super::helpers::error_code_to_status(code, message));
        }

        // Step 2 (GLOBAL): Delete App record, slug index, vault connections, assertions.
        let response = self
            .ctx
            .propose_request(
                LedgerRequest::DeleteApp { organization: org_id, app: app_id },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppDeleted { .. } => {
                self.emit_event(EventAction::AppDeleted, org_id, org_slug_val, &trace_ctx.trace_id);
                Ok(Response::new(DeleteAppResponse {}))
            },
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(Status::internal("Unexpected response from state machine"))
            },
        }
    }

    async fn set_app_enabled(
        &self,
        request: Request<SetAppEnabledRequest>,
    ) -> Result<Response<SetAppEnabledResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx = self.ctx.make_request_context(
            "AppService",
            "SetAppEnabled",
            &grpc_metadata,
            &trace_ctx,
        );

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;

        let response = self
            .ctx
            .propose_request(
                LedgerRequest::SetAppEnabled {
                    organization: org_id,
                    app: app_id,
                    enabled: inner.enabled,
                },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppToggled { organization_id } => {
                self.emit_event(
                    EventAction::AppStatusChanged,
                    org_id,
                    org_slug_val,
                    &trace_ctx.trace_id,
                );
                let app = self.load_app(organization_id, app_id)?;
                let info = self.app_to_proto(&app, true)?;
                Ok(Response::new(SetAppEnabledResponse { app: Some(info) }))
            },
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(Status::internal("Unexpected response from state machine"))
            },
        }
    }

    async fn set_app_credential_enabled(
        &self,
        request: Request<SetAppCredentialEnabledRequest>,
    ) -> Result<Response<SetAppCredentialEnabledResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx = self.ctx.make_request_context(
            "AppService",
            "SetAppCredentialEnabled",
            &grpc_metadata,
            &trace_ctx,
        );

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;
        let credential_type = map_credential_type(inner.credential_type)?;

        let response = self
            .ctx
            .propose_request(
                LedgerRequest::SetAppCredentialEnabled {
                    organization: org_id,
                    app: app_id,
                    credential_type,
                    enabled: inner.enabled,
                },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppCredentialToggled { organization_id } => {
                self.emit_event(
                    EventAction::AppCredentialStatusChanged,
                    org_id,
                    org_slug_val,
                    &trace_ctx.trace_id,
                );
                let app = self.load_app(organization_id, app_id)?;
                let info = self.app_to_proto(&app, true)?;
                Ok(Response::new(SetAppCredentialEnabledResponse { app: Some(info) }))
            },
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(Status::internal("Unexpected response from state machine"))
            },
        }
    }

    async fn get_app_client_secret(
        &self,
        request: Request<GetAppClientSecretRequest>,
    ) -> Result<Response<GetAppClientSecretResponse>, Status> {
        let inner = request.into_inner();
        let resolver = self.resolver();
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (resolved_org, app_id) = resolver.extract_and_resolve_app(&inner.app)?;
        if org_id != resolved_org {
            return Err(Status::not_found("App not found in the specified organization"));
        }

        let app = self.load_app(resolved_org, app_id)?;
        Ok(Response::new(GetAppClientSecretResponse {
            enabled: app.credentials.client_secret.enabled,
            has_secret: app.credentials.client_secret.secret_hash.is_some(),
        }))
    }

    async fn rotate_app_client_secret(
        &self,
        request: Request<RotateAppClientSecretRequest>,
    ) -> Result<Response<RotateAppClientSecretResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx = self.ctx.make_request_context(
            "AppService",
            "RotateAppClientSecret",
            &grpc_metadata,
            &trace_ctx,
        );

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;

        let idempotency_key = parse_idempotency_key(&inner.idempotency_key)?;

        // Check idempotency cache for a previously generated secret
        let cache_key = idempotency_key.map(|k| (org_id, app_id, k));
        if let Some(ref ck) = cache_key
            && let Some(CachedCredential::Secret(cached_secret)) = self.credential_cache.get(ck)
        {
            return Ok(Response::new(RotateAppClientSecretResponse {
                secret: cached_secret.to_string(),
            }));
        }

        // Generate a new secret and bcrypt-hash it off the async runtime.
        // Raw secret bytes are wrapped in Zeroizing to wipe from memory after encoding.
        use base64::Engine;
        let secret_bytes = Zeroizing::new(rand::random::<[u8; 32]>());
        let plaintext_secret =
            base64::engine::general_purpose::STANDARD.encode(secret_bytes.as_ref());
        let secret_to_hash = plaintext_secret.clone();
        let secret_hash =
            tokio::task::spawn_blocking(move || bcrypt::hash(secret_to_hash, bcrypt::DEFAULT_COST))
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, "Hash task panicked");
                    Status::internal("Internal error")
                })?
                .map_err(|e| {
                    tracing::error!(error = %e, "Failed to hash secret");
                    Status::internal("Internal error")
                })?;

        let response = self
            .ctx
            .propose_request(
                LedgerRequest::RotateAppClientSecret {
                    organization: org_id,
                    app: app_id,
                    new_secret_hash: secret_hash,
                },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppClientSecretRotated { .. } => {
                self.emit_event(
                    EventAction::AppSecretRotated,
                    org_id,
                    org_slug_val,
                    &trace_ctx.trace_id,
                );
                // Cache the secret for idempotent retries
                if let Some(ck) = cache_key {
                    self.credential_cache
                        .insert(ck, CachedCredential::Secret(plaintext_secret.clone().into()));
                }
                Ok(Response::new(RotateAppClientSecretResponse { secret: plaintext_secret }))
            },
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(Status::internal("Unexpected response from state machine"))
            },
        }
    }

    async fn list_app_client_assertions(
        &self,
        request: Request<ListAppClientAssertionsRequest>,
    ) -> Result<Response<ListAppClientAssertionsResponse>, Status> {
        let inner = request.into_inner();
        let resolver = self.resolver();
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (resolved_org, app_id) = resolver.extract_and_resolve_app(&inner.app)?;
        if org_id != resolved_org {
            return Err(Status::not_found("App not found in the specified organization"));
        }

        let entries = self.list_assertions_internal(resolved_org, app_id)?;
        let assertions: Vec<AppClientAssertionInfo> = entries
            .iter()
            .map(|entry| {
                let name = self.load_assertion_name(resolved_org, app_id, entry.id);
                Self::assertion_to_proto(entry, name)
            })
            .collect();
        Ok(Response::new(ListAppClientAssertionsResponse { assertions }))
    }

    async fn create_app_client_assertion(
        &self,
        request: Request<CreateAppClientAssertionRequest>,
    ) -> Result<Response<CreateAppClientAssertionResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx = self.ctx.make_request_context(
            "AppService",
            "CreateAppClientAssertion",
            &grpc_metadata,
            &trace_ctx,
        );

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;

        let name = inner.name.trim().to_string();
        if name.is_empty() {
            return Err(Status::invalid_argument("Client assertion name must not be empty"));
        }

        let idempotency_key = parse_idempotency_key(&inner.idempotency_key)?;

        let expires_at =
            inner.expires_at.as_ref().map(crate::proto_compat::proto_to_datetime).ok_or_else(
                || Status::invalid_argument("expires_at is required for client assertions"),
            )?;

        // Check idempotency cache for a previously generated keypair
        let cache_key = idempotency_key.map(|k| (org_id, app_id, k));
        let (public_key_bytes, private_key_pem) = if let Some(ref ck) = cache_key
            && let Some(CachedCredential::Assertion { public_key_bytes, private_key_pem }) =
                self.credential_cache.get(ck)
        {
            (public_key_bytes, private_key_pem)
        } else {
            // Generate Ed25519 keypair from a random seed.
            // Seed is wrapped in Zeroizing to wipe raw key material after use.
            use ed25519_dalek::pkcs8::EncodePrivateKey;
            let seed = Zeroizing::new(rand::random::<[u8; 32]>());
            let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
            let pk = signing_key.verifying_key().to_bytes().to_vec();
            let pem: Zeroizing<String> = signing_key
                .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
                .map_err(|e| {
                    tracing::error!(error = %e, "Failed to encode private key");
                    Status::internal("Internal error")
                })?
                .to_string()
                .into();

            // Cache before proposing so retries during Raft round-trip reuse the same keypair
            if let Some(ck) = cache_key {
                self.credential_cache.insert(
                    ck,
                    CachedCredential::Assertion {
                        public_key_bytes: pk.clone(),
                        private_key_pem: pem.clone(),
                    },
                );
            }

            (pk, pem)
        };

        // Phase 1: Propose structural entry to GLOBAL Raft (no PII).
        let response = self
            .ctx
            .propose_request(
                LedgerRequest::CreateAppClientAssertion {
                    organization: org_id,
                    app: app_id,
                    expires_at,
                    public_key_bytes,
                },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppClientAssertionCreated { assertion_id } => {
                // Phase 2: Write assertion name to REGIONAL Raft (PII isolation).
                // Encrypted with OrgShredKey for crypto-shredding on organization purge.
                let org_meta = self
                    .ctx
                    .applied_state
                    .get_organization(org_id)
                    .ok_or_else(|| Status::not_found("Organization not found"))?;
                let name_request =
                    inferadb_ledger_raft::types::SystemRequest::WriteClientAssertionName {
                        organization: org_id,
                        app: app_id,
                        assertion: assertion_id,
                        name: name.clone(),
                    };
                let name_response = self
                    .ctx
                    .propose_regional_org_encrypted(
                        org_meta.region,
                        name_request,
                        org_id,
                        &grpc_metadata,
                        &mut ctx,
                    )
                    .await?;
                if let LedgerResponse::Error { code, message } = name_response {
                    return Err(super::helpers::error_code_to_status(code, message));
                }

                self.emit_event(
                    EventAction::AppAssertionCreated,
                    org_id,
                    org_slug_val,
                    &trace_ctx.trace_id,
                );

                // Build the assertion info from the committed data
                let assertion_info = AppClientAssertionInfo {
                    id: Some(proto::ClientAssertionId { id: assertion_id.value() }),
                    name: name.clone(),
                    enabled: true,
                    expires_at: Some(crate::proto_compat::datetime_to_proto(&expires_at)),
                    created_at: None,
                };

                // Read back from state for accurate created_at
                let key = SystemKeys::app_assertion_key(org_id, app_id, assertion_id);
                let final_info = match self.ctx.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
                    Ok(Some(entity)) => match decode::<ClientAssertionEntry>(&entity.value) {
                        Ok(entry) => Self::assertion_to_proto(&entry, name),
                        Err(_) => assertion_info,
                    },
                    _ => assertion_info,
                };

                Ok(Response::new(CreateAppClientAssertionResponse {
                    assertion: Some(final_info),
                    private_key_pem: private_key_pem.to_string(),
                }))
            },
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(Status::internal("Unexpected response from state machine"))
            },
        }
    }

    async fn delete_app_client_assertion(
        &self,
        request: Request<DeleteAppClientAssertionRequest>,
    ) -> Result<Response<DeleteAppClientAssertionResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx = self.ctx.make_request_context(
            "AppService",
            "DeleteAppClientAssertion",
            &grpc_metadata,
            &trace_ctx,
        );

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;
        let assertion_id = inner
            .assertion
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing assertion ID"))?;
        let assertion = DomainClientAssertionId::new(assertion_id.id);

        // Phase 1: Delete assertion name from REGIONAL Raft (best-effort).
        if let Some(org_meta) = self.ctx.applied_state.get_organization(org_id) {
            let name_request =
                inferadb_ledger_raft::types::SystemRequest::DeleteClientAssertionName {
                    organization: org_id,
                    app: app_id,
                    assertion,
                };
            // Regional delete is best-effort: if region is unavailable, the
            // GLOBAL delete still proceeds and `PurgeOrganizationRegional`
            // will clean up orphaned names.
            if let Err(e) = self
                .ctx
                .propose_regional(org_meta.region, name_request, &grpc_metadata, &mut ctx)
                .await
            {
                tracing::warn!(
                    %e,
                    org_id = %org_id,
                    app_id = %app_id,
                    assertion_id = %assertion,
                    "Failed to delete assertion name from regional state"
                );
            }
        }

        // Phase 2: Delete structural entry from GLOBAL Raft.
        let response = self
            .ctx
            .propose_request(
                LedgerRequest::DeleteAppClientAssertion {
                    organization: org_id,
                    app: app_id,
                    assertion,
                },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppClientAssertionDeleted { .. } => {
                self.emit_event(
                    EventAction::AppAssertionDeleted,
                    org_id,
                    org_slug_val,
                    &trace_ctx.trace_id,
                );
                Ok(Response::new(DeleteAppClientAssertionResponse {}))
            },
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(Status::internal("Unexpected response from state machine"))
            },
        }
    }

    async fn set_app_client_assertion_enabled(
        &self,
        request: Request<SetAppClientAssertionEnabledRequest>,
    ) -> Result<Response<SetAppClientAssertionEnabledResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx = self.ctx.make_request_context(
            "AppService",
            "SetAppClientAssertionEnabled",
            &grpc_metadata,
            &trace_ctx,
        );

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;
        let assertion_id = inner
            .assertion
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing assertion ID"))?;
        let assertion = DomainClientAssertionId::new(assertion_id.id);

        let response = self
            .ctx
            .propose_request(
                LedgerRequest::SetAppClientAssertionEnabled {
                    organization: org_id,
                    app: app_id,
                    assertion,
                    enabled: inner.enabled,
                },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppClientAssertionToggled { .. } => {
                self.emit_event(
                    EventAction::AppCredentialStatusChanged,
                    org_id,
                    org_slug_val,
                    &trace_ctx.trace_id,
                );
                Ok(Response::new(SetAppClientAssertionEnabledResponse {}))
            },
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(Status::internal("Unexpected response from state machine"))
            },
        }
    }

    async fn list_app_vaults(
        &self,
        request: Request<ListAppVaultsRequest>,
    ) -> Result<Response<ListAppVaultsResponse>, Status> {
        let inner = request.into_inner();
        let resolver = self.resolver();
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (resolved_org, app_id) = resolver.extract_and_resolve_app(&inner.app)?;
        if org_id != resolved_org {
            return Err(Status::not_found("App not found in the specified organization"));
        }

        let connections = self.list_vault_connections(resolved_org, app_id)?;
        let vaults: Vec<AppVaultConnectionInfo> =
            connections.iter().map(Self::vault_connection_to_proto).collect();
        Ok(Response::new(ListAppVaultsResponse { vaults }))
    }

    async fn add_app_vault(
        &self,
        request: Request<AddAppVaultRequest>,
    ) -> Result<Response<AddAppVaultResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx =
            self.ctx.make_request_context("AppService", "AddAppVault", &grpc_metadata, &trace_ctx);

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;
        let vault_slug = SlugResolver::extract_vault_slug(&inner.vault)?;
        let vault_id = resolver.resolve_vault(vault_slug)?;

        let response = self
            .ctx
            .propose_request(
                LedgerRequest::AddAppVault {
                    organization: org_id,
                    app: app_id,
                    vault: vault_id,
                    vault_slug,
                    allowed_scopes: inner.allowed_scopes,
                },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppVaultAdded { organization_id } => {
                self.emit_event(
                    EventAction::AppVaultConnected,
                    org_id,
                    org_slug_val,
                    &trace_ctx.trace_id,
                );
                let conn = self.read_vault_connection(organization_id, app_id, vault_id)?;
                Ok(Response::new(AddAppVaultResponse {
                    vault: Some(Self::vault_connection_to_proto(&conn)),
                }))
            },
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(Status::internal("Unexpected response from state machine"))
            },
        }
    }

    async fn update_app_vault(
        &self,
        request: Request<UpdateAppVaultRequest>,
    ) -> Result<Response<UpdateAppVaultResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx = self.ctx.make_request_context(
            "AppService",
            "UpdateAppVault",
            &grpc_metadata,
            &trace_ctx,
        );

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;
        let vault_slug = SlugResolver::extract_vault_slug(&inner.vault)?;
        let vault_id = resolver.resolve_vault(vault_slug)?;

        let response = self
            .ctx
            .propose_request(
                LedgerRequest::UpdateAppVault {
                    organization: org_id,
                    app: app_id,
                    vault: vault_id,
                    allowed_scopes: inner.allowed_scopes,
                },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppVaultUpdated { organization_id } => {
                self.emit_event(
                    EventAction::AppVaultUpdated,
                    org_id,
                    org_slug_val,
                    &trace_ctx.trace_id,
                );
                let conn = self.read_vault_connection(organization_id, app_id, vault_id)?;
                Ok(Response::new(UpdateAppVaultResponse {
                    vault: Some(Self::vault_connection_to_proto(&conn)),
                }))
            },
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(Status::internal("Unexpected response from state machine"))
            },
        }
    }

    async fn remove_app_vault(
        &self,
        request: Request<RemoveAppVaultRequest>,
    ) -> Result<Response<RemoveAppVaultResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let mut ctx = self.ctx.make_request_context(
            "AppService",
            "RemoveAppVault",
            &grpc_metadata,
            &trace_ctx,
        );

        let inner = request.into_inner();
        super::helpers::extract_caller(&mut ctx, &inner.caller);
        let resolver = self.resolver();
        let org_slug_val = inner.organization.as_ref().map_or(0, |n| n.slug);
        let org_id = resolver.extract_and_resolve(&inner.organization)?;
        let (_, app_id) = resolver.extract_and_resolve_app(&inner.app)?;
        let vault_slug = SlugResolver::extract_vault_slug(&inner.vault)?;
        let vault_id = resolver.resolve_vault(vault_slug)?;

        let response = self
            .ctx
            .propose_request(
                LedgerRequest::RemoveAppVault {
                    organization: org_id,
                    app: app_id,
                    vault: vault_id,
                },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppVaultRemoved { .. } => {
                self.emit_event(
                    EventAction::AppVaultDisconnected,
                    org_id,
                    org_slug_val,
                    &trace_ctx.trace_id,
                );
                Ok(Response::new(RemoveAppVaultResponse {}))
            },
            LedgerResponse::Error { code, message } => {
                Err(super::helpers::error_code_to_status(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(Status::internal("Unexpected response from state machine"))
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // =========================================================================
    // parse_idempotency_key
    // =========================================================================

    #[test]
    fn parse_idempotency_key_empty_returns_none() {
        assert_eq!(parse_idempotency_key(&[]).unwrap(), None);
    }

    #[test]
    fn parse_idempotency_key_exact_16_bytes_returns_some() {
        let bytes = [1u8; 16];
        let result = parse_idempotency_key(&bytes).unwrap();
        assert_eq!(result, Some([1u8; 16]));
    }

    #[test]
    fn parse_idempotency_key_too_short_returns_error() {
        let bytes = [1u8; 8];
        let err = parse_idempotency_key(&bytes).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("16 bytes"));
    }

    #[test]
    fn parse_idempotency_key_too_long_returns_error() {
        let bytes = [1u8; 32];
        let err = parse_idempotency_key(&bytes).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // =========================================================================
    // map_credential_type
    // =========================================================================

    #[test]
    fn map_credential_type_client_secret() {
        let result = map_credential_type(AppCredentialType::ClientSecret as i32).unwrap();
        assert_eq!(result, DomainAppCredentialType::ClientSecret);
    }

    #[test]
    fn map_credential_type_mtls_ca() {
        let result = map_credential_type(AppCredentialType::MtlsCa as i32).unwrap();
        assert_eq!(result, DomainAppCredentialType::MtlsCa);
    }

    #[test]
    fn map_credential_type_mtls_self_signed() {
        let result = map_credential_type(AppCredentialType::MtlsSelfSigned as i32).unwrap();
        assert_eq!(result, DomainAppCredentialType::MtlsSelfSigned);
    }

    #[test]
    fn map_credential_type_client_assertion() {
        let result = map_credential_type(AppCredentialType::ClientAssertion as i32).unwrap();
        assert_eq!(result, DomainAppCredentialType::ClientAssertion);
    }

    #[test]
    fn map_credential_type_unspecified_returns_error() {
        let err = map_credential_type(AppCredentialType::Unspecified as i32).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn map_credential_type_invalid_value_returns_error() {
        let err = map_credential_type(999).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // =========================================================================
    // vault_connection_to_proto
    // =========================================================================

    #[test]
    fn vault_connection_to_proto_maps_all_fields() {
        let conn = AppVaultConnection {
            vault_id: inferadb_ledger_types::VaultId::new(5),
            vault_slug: inferadb_ledger_types::VaultSlug::new(500),
            allowed_scopes: vec!["read".to_string(), "write".to_string()],
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let proto_conn = AppService::vault_connection_to_proto(&conn);
        assert!(proto_conn.vault.is_some());
        assert_eq!(proto_conn.vault.unwrap().slug, 500);
        assert_eq!(proto_conn.allowed_scopes, vec!["read", "write"]);
        assert!(proto_conn.created_at.is_some());
        assert!(proto_conn.updated_at.is_some());
    }

    #[test]
    fn vault_connection_to_proto_empty_scopes() {
        let conn = AppVaultConnection {
            vault_id: inferadb_ledger_types::VaultId::new(1),
            vault_slug: inferadb_ledger_types::VaultSlug::new(100),
            allowed_scopes: vec![],
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let proto_conn = AppService::vault_connection_to_proto(&conn);
        assert!(proto_conn.allowed_scopes.is_empty());
    }

    // =========================================================================
    // assertion_to_proto
    // =========================================================================

    #[test]
    fn assertion_to_proto_maps_all_fields() {
        let entry = ClientAssertionEntry {
            id: DomainClientAssertionId::new(42),
            public_key_bytes: vec![1, 2, 3],
            enabled: true,
            expires_at: chrono::Utc::now() + chrono::Duration::hours(24),
            created_at: chrono::Utc::now(),
        };
        let proto_assertion = AppService::assertion_to_proto(&entry, "my-assertion".to_string());
        assert!(proto_assertion.id.is_some());
        assert_eq!(proto_assertion.id.unwrap().id, 42);
        assert_eq!(proto_assertion.name, "my-assertion");
        assert!(proto_assertion.enabled);
        assert!(proto_assertion.expires_at.is_some());
        assert!(proto_assertion.created_at.is_some());
    }

    #[test]
    fn assertion_to_proto_disabled_entry() {
        let entry = ClientAssertionEntry {
            id: DomainClientAssertionId::new(1),
            public_key_bytes: vec![],
            enabled: false,
            expires_at: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
        };
        let proto_assertion = AppService::assertion_to_proto(&entry, String::new());
        assert!(!proto_assertion.enabled);
        assert!(proto_assertion.name.is_empty());
    }

    // =========================================================================
    // parse_idempotency_key edge cases
    // =========================================================================

    #[test]
    fn parse_idempotency_key_all_zeros() {
        let bytes = [0u8; 16];
        let result = parse_idempotency_key(&bytes).unwrap();
        assert_eq!(result, Some([0u8; 16]));
    }

    #[test]
    fn parse_idempotency_key_one_byte_returns_error() {
        let err = parse_idempotency_key(&[42]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_idempotency_key_fifteen_bytes_returns_error() {
        let bytes = [1u8; 15];
        let err = parse_idempotency_key(&bytes).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_idempotency_key_seventeen_bytes_returns_error() {
        let bytes = [1u8; 17];
        let err = parse_idempotency_key(&bytes).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
