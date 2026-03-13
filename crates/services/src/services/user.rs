//! User service implementation.
//!
//! Handles user lifecycle (CRUD), email management, region migration,
//! and GDPR erasure. Write operations flow through Raft for consistency;
//! read operations hit the local state layer directly.
//!
//! User creation and region migration use sagas (multi-step distributed
//! transactions) driven by the saga orchestrator. Other write operations
//! are single-step Raft proposals.

use chrono::Utc;
use inferadb_ledger_proto::proto::{
    self, CompleteRegistrationRequest, CompleteRegistrationResponse, CreateUserEmailRequest,
    CreateUserEmailResponse, CreateUserRequest, CreateUserResponse, DeleteUserEmailRequest,
    DeleteUserEmailResponse, DeleteUserRequest, DeleteUserResponse, EraseUserRequest,
    EraseUserResponse, GetUserRequest, GetUserResponse, InitiateEmailVerificationRequest,
    InitiateEmailVerificationResponse, ListUsersRequest, ListUsersResponse,
    MigrateUserRegionRequest, MigrateUserRegionResponse, Region as ProtoRegion,
    SearchUserEmailRequest, SearchUserEmailResponse, SearchUsersRequest, SearchUsersResponse,
    UpdateUserRequest, UpdateUserResponse, UserSlug as ProtoUserSlug, VerifyEmailCodeRequest,
    VerifyEmailCodeResponse, VerifyUserEmailRequest, VerifyUserEmailResponse,
};
use inferadb_ledger_raft::{
    error::ServiceError,
    event_writer::HandlerPhaseEmitter,
    trace_context,
    types::{LedgerRequest, LedgerResponse, SystemRequest},
};
use inferadb_ledger_state::system::{
    CreateUserInput, CreateUserSaga, EmailHashEntry, MigrateUserInput, MigrateUserSaga, Saga,
    SagaId, SystemOrganizationService,
};
use inferadb_ledger_types::{
    UserEmailId as DomainUserEmailId, VaultId as DomainVaultId,
    events::{EventAction, EventOutcome as EventOutcomeType},
    validation,
};
use tonic::{Request, Response, Status};

use super::{service_infra::ServiceContext, slug_resolver::SlugResolver};

/// User lifecycle, email management, region migration, and GDPR erasure.
pub struct UserService {
    ctx: ServiceContext,
}

/// Session tokens returned by [`UserService::create_user_session`].
struct SessionTokens {
    access_token: String,
    access_expires_at: chrono::DateTime<Utc>,
    refresh_token: String,
    refresh_expires_at: chrono::DateTime<Utc>,
}

impl UserService {
    /// Creates a new `UserService` from shared service infrastructure.
    pub(crate) fn new(ctx: ServiceContext) -> Self {
        Self { ctx }
    }

    /// Ensures the active global signing key is loaded and cached in the JWT engine.
    ///
    /// Returns the signing key metadata. Subsequent calls are fast (cache hit).
    fn ensure_signing_key_cached(
        &self,
        sys_svc: &SystemOrganizationService<inferadb_ledger_store::FileBackend>,
    ) -> Result<inferadb_ledger_state::system::SigningKey, Status> {
        let jwt_engine = self.ctx.jwt_engine.as_ref().ok_or_else(|| {
            Status::failed_precondition("JWT engine not configured for session signing")
        })?;
        let key_manager = self
            .ctx
            .key_manager
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Key manager not configured"))?;

        let signing_key = sys_svc
            .get_active_signing_key(&inferadb_ledger_state::system::SigningKeyScope::Global)
            .map_err(|e| Status::internal(format!("Failed to read signing key: {e}")))?
            .ok_or_else(|| Status::failed_precondition("No active signing key"))?;

        if !jwt_engine.has_cached_key(&signing_key.kid) {
            let scope_region = crate::jwt::scope_to_region(&signing_key.scope, sys_svc)
                .map_err(|e| Status::internal(format!("Failed to resolve key region: {e}")))?;
            let rmk = key_manager
                .rmk_by_version(scope_region, signing_key.rmk_version)
                .map_err(|e| Status::internal(format!("Failed to load RMK: {e}")))?;
            jwt_engine
                .load_key(&signing_key, &rmk)
                .map_err(|e| Status::internal(format!("Failed to load signing key: {e}")))?;
        }

        Ok(signing_key)
    }

    /// Creates a full user session: signs a JWT access token, generates a
    /// refresh token, and proposes `CreateRefreshToken` to the user's region.
    async fn create_user_session(
        &self,
        sys_svc: &SystemOrganizationService<inferadb_ledger_store::FileBackend>,
        user_slug: inferadb_ledger_types::UserSlug,
        role: inferadb_ledger_types::UserRole,
        token_version: inferadb_ledger_types::TokenVersion,
        region: inferadb_ledger_types::Region,
        grpc_metadata: &tonic::metadata::MetadataMap,
        ctx: &mut inferadb_ledger_raft::logging::CanonicalLogLine,
    ) -> Result<SessionTokens, Status> {
        use inferadb_ledger_types::{TokenSubject, TokenType};

        let signing_key = self.ensure_signing_key_cached(sys_svc)?;
        let jwt_engine = self
            .ctx
            .jwt_engine
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("JWT engine not configured"))?;
        let jwt_config = self
            .ctx
            .jwt_config
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("JWT config not configured"))?;

        let role_str = match role {
            inferadb_ledger_types::UserRole::Admin => "admin",
            inferadb_ledger_types::UserRole::User => "user",
        };

        let (access_token, access_expires_at) = jwt_engine
            .sign_user_session(user_slug, role_str, token_version, &signing_key.kid)
            .map_err(|e| Status::internal(format!("Failed to sign access token: {e}")))?;

        let (refresh_token_str, refresh_token_hash) = crate::jwt::generate_refresh_token();
        let family = crate::jwt::generate_family_id();

        let refresh_request = LedgerRequest::CreateRefreshToken {
            token_hash: refresh_token_hash,
            family,
            token_type: TokenType::UserSession,
            subject: TokenSubject::User(user_slug),
            organization: None,
            vault: None,
            kid: signing_key.kid.clone(),
            ttl_secs: jwt_config.session_refresh_ttl_secs,
        };
        self.ctx
            .propose_regional_ledger_request(region, refresh_request, grpc_metadata, ctx)
            .await?;

        let refresh_expires_at =
            Utc::now() + chrono::Duration::seconds(jwt_config.session_refresh_ttl_secs as i64);

        Ok(SessionTokens {
            access_token,
            access_expires_at,
            refresh_token: refresh_token_str,
            refresh_expires_at,
        })
    }
}

/// Converts a domain `User` to a proto `User` message.
fn domain_user_to_proto(
    user: &inferadb_ledger_state::system::User,
    slug: Option<inferadb_ledger_types::UserSlug>,
) -> proto::User {
    let proto_status: proto::UserStatus = user.status.into();
    let proto_role: proto::UserRole = user.role.into();
    proto::User {
        id: Some(proto::UserId { id: user.id.value() }),
        name: user.name.clone(),
        email: Some(proto::UserEmailId { id: user.email.value() }),
        status: proto_status.into(),
        created_at: Some(crate::proto_compat::datetime_to_proto(&user.created_at)),
        updated_at: Some(crate::proto_compat::datetime_to_proto(&user.updated_at)),
        role: proto_role.into(),
        slug: slug.map(|s| proto::UserSlug { slug: s.value() }),
        deleted_at: user.deleted_at.as_ref().map(crate::proto_compat::datetime_to_proto),
    }
}

/// Converts a domain `UserEmail` to a proto `UserEmail` message.
fn domain_email_to_proto(email: &inferadb_ledger_state::system::UserEmail) -> proto::UserEmail {
    proto::UserEmail {
        id: Some(proto::UserEmailId { id: email.id.value() }),
        user: Some(proto::UserId { id: email.user.value() }),
        email: email.email.clone(),
        created_at: Some(crate::proto_compat::datetime_to_proto(&email.created_at)),
        verified_at: email.verified_at.as_ref().map(crate::proto_compat::datetime_to_proto),
    }
}

#[tonic::async_trait]
impl proto::user_service_server::UserService for UserService {
    /// Creates a user via the saga orchestrator.
    ///
    /// The caller provides a pre-computed email HMAC (using the active blinding key).
    /// This RPC persists a `CreateUserSaga` for the orchestrator to drive through:
    /// 1. Reserve email HMAC (global uniqueness)
    /// 2. Create user record (regional)
    /// 3. Create directory entry (global)
    ///
    /// Returns immediately once the saga is persisted. The user and slug in the
    /// response are populated after saga completion.
    async fn create_user(
        &self,
        request: Request<CreateUserRequest>,
    ) -> Result<Response<CreateUserResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx =
            self.ctx.make_request_context("UserService", "create_user", &grpc_metadata, &trace_ctx);

        // Validate inputs
        validation::validate_user_name(&req.name).map_err(|e| {
            let msg = e.to_string();
            ctx.set_error("InvalidArgument", &msg);
            Status::invalid_argument(msg)
        })?;
        validation::validate_email(&req.email).map_err(|e| {
            let msg = e.to_string();
            ctx.set_error("InvalidArgument", &msg);
            Status::invalid_argument(msg)
        })?;
        if req.email_hmac.is_empty() {
            ctx.set_error("InvalidArgument", "email_hmac is required");
            return Err(Status::invalid_argument("email_hmac is required"));
        }
        if !req.organization_name.is_empty() {
            validation::validate_organization_name(
                &req.organization_name,
                &self.ctx.validation_config,
            )
            .map_err(|e| {
                let msg = e.to_string();
                ctx.set_error("InvalidArgument", &msg);
                Status::invalid_argument(msg)
            })?;
        }

        let region = inferadb_ledger_proto::convert::region_from_i32(req.region)?;
        let role = match req.role {
            Some(r) => inferadb_ledger_proto::convert::user_role_from_i32(r)?,
            None => inferadb_ledger_types::UserRole::User,
        };
        let admin = role == inferadb_ledger_types::UserRole::Admin;
        let default_org_tier = req
            .organization_tier
            .and_then(|t| inferadb_ledger_proto::proto::OrganizationTier::try_from(t).ok())
            .map(crate::proto_compat::organization_tier_from_proto)
            .unwrap_or(inferadb_ledger_state::system::OrganizationTier::Free);

        // Create the saga for the orchestrator to drive
        let saga_id = SagaId::new(uuid::Uuid::new_v4().to_string());
        let saga = CreateUserSaga::new(
            saga_id.clone(),
            CreateUserInput {
                hmac: req.email_hmac,
                region,
                admin,
                organization_name: req.organization_name.clone(),
                default_org_tier,
            },
        );
        let saga_key = format!("saga:{saga_id}");
        let saga_wrapped = Saga::CreateUser(saga);
        let saga_bytes = serde_json::to_vec(&saga_wrapped)
            .map_err(|e| Status::internal(format!("Failed to serialize saga: {e}")))?;

        let saga_op = inferadb_ledger_types::Operation::SetEntity {
            key: saga_key,
            value: saga_bytes,
            expires_at: None,
            condition: Some(inferadb_ledger_types::SetCondition::MustNotExist),
        };

        let saga_txn = inferadb_ledger_types::Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: inferadb_ledger_types::ClientId::new("system:user"),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations: vec![saga_op],
            timestamp: Utc::now(),
            actor: "system:user".to_string(),
        };
        let saga_request = LedgerRequest::Write {
            organization: inferadb_ledger_types::OrganizationId::new(0),
            vault: DomainVaultId::new(0),
            transactions: vec![saga_txn],
            idempotency_key: [0; 16],
            request_hash: 0,
        };

        self.ctx.propose_request(saga_request, &grpc_metadata, &mut ctx).await?;

        // Saga is now persisted. The orchestrator will drive it to completion.
        // We return a response with the saga ID; the user/slug will be
        // available once the saga completes (poll via GetUser).
        if let Some(node_id) = self.ctx.node_id {
            self.ctx.record_handler_event(
                HandlerPhaseEmitter::for_system(EventAction::UserCreated, node_id)
                    .principal("system")
                    .detail("saga_id", saga_id.value())
                    .detail("region", region.as_str())
                    .detail("admin", &admin.to_string())
                    .trace_id(&trace_ctx.trace_id)
                    .outcome(EventOutcomeType::Success)
                    .build(self.ctx.default_ttl_days()),
            );
        }

        ctx.set_success();
        Ok(Response::new(CreateUserResponse {
            slug: None,
            user: None,
            default_organization_slug: None,
        }))
    }

    async fn get_user(
        &self,
        request: Request<GetUserRequest>,
    ) -> Result<Response<GetUserResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx =
            self.ctx.make_request_context("UserService", "get_user", &grpc_metadata, &trace_ctx);

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver.extract_and_resolve_user(&req.slug).inspect_err(|status| {
            ctx.set_error("InvalidArgument", status.message());
        })?;
        let user_slug = SlugResolver::extract_user_slug(&req.slug)?;

        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let user = sys_svc.get_user(user_id).map_err(|e| {
            ctx.set_error("Internal", &e.to_string());
            Status::internal(format!("Failed to read user: {e}"))
        })?;

        let user = user.ok_or_else(|| {
            ctx.set_error("NotFound", "User not found");
            Status::not_found(format!("User with slug {} not found", user_slug.value()))
        })?;

        let emails = sys_svc.get_user_emails(user_id).map_err(|e| {
            ctx.set_error("Internal", &e.to_string());
            Status::internal(format!("Failed to read user emails: {e}"))
        })?;

        ctx.set_success();
        Ok(Response::new(GetUserResponse {
            user: Some(domain_user_to_proto(&user, Some(user_slug))),
            emails: emails.iter().map(domain_email_to_proto).collect(),
        }))
    }

    async fn update_user(
        &self,
        request: Request<UpdateUserRequest>,
    ) -> Result<Response<UpdateUserResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx =
            self.ctx.make_request_context("UserService", "update_user", &grpc_metadata, &trace_ctx);

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver.extract_and_resolve_user(&req.slug).inspect_err(|status| {
            ctx.set_error("InvalidArgument", status.message());
        })?;
        let user_slug = SlugResolver::extract_user_slug(&req.slug)?;

        // Validate optional name
        if let Some(ref name) = req.name {
            validation::validate_user_name(name).map_err(|e| {
                let msg = e.to_string();
                ctx.set_error("InvalidArgument", &msg);
                Status::invalid_argument(msg)
            })?;
        }

        let role = req.role.map(inferadb_ledger_proto::convert::user_role_from_i32).transpose()?;

        let primary_email = req.primary_email.map(|e| DomainUserEmailId::new(e.id));

        let has_global_fields = role.is_some() || primary_email.is_some();
        let has_regional_fields = req.name.is_some();

        // At least one field must be provided.
        if !has_global_fields && !has_regional_fields {
            return Err(Status::invalid_argument(
                "At least one field (name, role, primary_email) must be provided",
            ));
        }

        // Step 1 (GLOBAL): Update role / primary_email — no PII in the GLOBAL Raft log.
        if has_global_fields {
            let response = self
                .ctx
                .propose_system_request(
                    SystemRequest::UpdateUser { user_id, role, primary_email },
                    &grpc_metadata,
                    &mut ctx,
                )
                .await?;

            match response {
                LedgerResponse::UserUpdated { .. } => {},
                LedgerResponse::Error { code, message } => {
                    ctx.set_error(code.grpc_code_name(), &message);
                    return Err(super::helpers::error_code_to_status(
                        code,
                        format!("User update failed: {message}"),
                    ));
                },
                other => {
                    ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                    return Err(Status::internal("Unexpected response from Raft state machine"));
                },
            }
        }

        // Step 2 (REGIONAL): Update name — PII stays in the regional Raft log.
        // Look up user's region from GLOBAL directory for regional proposal.
        // Capture region for reuse when reading the response.
        let user_region = if let Some(name) = req.name {
            let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
            let dir_entry = sys_svc.get_user_directory(user_id).map_err(|e| {
                ctx.set_error("Internal", &e.to_string());
                Status::internal(format!("Failed to read user directory: {e}"))
            })?;
            let dir_entry = dir_entry.ok_or_else(|| {
                ctx.set_error("NotFound", "User directory entry not found");
                Status::not_found("User directory entry not found")
            })?;
            let region = dir_entry.region.ok_or_else(|| {
                ctx.set_error("FailedPrecondition", "User has no region (erased?)");
                Status::failed_precondition("User has no region assigned")
            })?;

            let response = self
                .ctx
                .propose_regional_encrypted(
                    region,
                    SystemRequest::UpdateUserProfile { user_id, name },
                    user_id,
                    &grpc_metadata,
                    &mut ctx,
                )
                .await?;

            match response {
                LedgerResponse::UserProfileUpdated { .. } => {},
                LedgerResponse::Error { code, message } => {
                    ctx.set_error(code.grpc_code_name(), &message);
                    return Err(super::helpers::error_code_to_status(
                        code,
                        format!("User profile update failed: {message}"),
                    ));
                },
                other => {
                    ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                    return Err(Status::internal("Unexpected response from Raft state machine"));
                },
            }

            Some(region)
        } else {
            None
        };

        // Emit handler event.
        if let Some(node_id) = self.ctx.node_id {
            self.ctx.record_handler_event(
                HandlerPhaseEmitter::for_system(EventAction::UserUpdated, node_id)
                    .principal("system")
                    .detail("user_id", &user_id.to_string())
                    .trace_id(&trace_ctx.trace_id)
                    .outcome(EventOutcomeType::Success)
                    .build(self.ctx.default_ttl_days()),
            );
        }

        // Read from the appropriate state layer for the response.
        // If we updated regional fields, read from regional state (freshest data).
        // Otherwise, read from GLOBAL state.
        let user = if let Some(region) = user_region {
            let regional_state = self.ctx.regional_state(region)?;
            let regional_sys = SystemOrganizationService::new(regional_state);
            regional_sys
                .get_user(user_id)
                .map_err(|e| Status::internal(format!("Failed to read updated user: {e}")))?
        } else {
            let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
            sys_svc
                .get_user(user_id)
                .map_err(|e| Status::internal(format!("Failed to read updated user: {e}")))?
        };

        ctx.set_success();
        Ok(Response::new(UpdateUserResponse {
            user: user.map(|u| domain_user_to_proto(&u, Some(user_slug))),
        }))
    }

    async fn delete_user(
        &self,
        request: Request<DeleteUserRequest>,
    ) -> Result<Response<DeleteUserResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx =
            self.ctx.make_request_context("UserService", "delete_user", &grpc_metadata, &trace_ctx);

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver.extract_and_resolve_user(&req.slug).inspect_err(|status| {
            ctx.set_error("InvalidArgument", status.message());
        })?;
        let user_slug = SlugResolver::extract_user_slug(&req.slug)?;

        if req.deleted_by.is_empty() {
            ctx.set_error("InvalidArgument", "deleted_by must be non-empty");
            return Err(Status::invalid_argument("deleted_by must be non-empty"));
        }

        let response = self
            .ctx
            .propose_system_request(SystemRequest::DeleteUser { user_id }, &grpc_metadata, &mut ctx)
            .await?;

        match response {
            LedgerResponse::UserSoftDeleted { user_id: deleted_id, retention_days } => {
                if let Some(node_id) = self.ctx.node_id {
                    self.ctx.record_handler_event(
                        HandlerPhaseEmitter::for_system(EventAction::UserSoftDeleted, node_id)
                            .principal(&req.deleted_by)
                            .detail("user_id", &deleted_id.to_string())
                            .detail("retention_days", &retention_days.to_string())
                            .trace_id(&trace_ctx.trace_id)
                            .outcome(EventOutcomeType::Success)
                            .build(self.ctx.default_ttl_days()),
                    );
                }

                ctx.set_success();
                Ok(Response::new(DeleteUserResponse {
                    slug: Some(ProtoUserSlug { slug: user_slug.value() }),
                    deleted_at: Some(crate::proto_compat::datetime_to_proto(&Utc::now())),
                    retention_days,
                }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(
                    code,
                    format!("User deletion failed: {message}"),
                ))
            },
            other => {
                ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                Err(Status::internal("Unexpected response from Raft state machine"))
            },
        }
    }

    async fn list_users(
        &self,
        request: Request<ListUsersRequest>,
    ) -> Result<Response<ListUsersResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx =
            self.ctx.make_request_context("UserService", "list_users", &grpc_metadata, &trace_ctx);

        let page_size = if req.page_size == 0 { 100 } else { req.page_size.min(1000) as usize };

        // Decode page token as the last entity key seen (opaque cursor)
        let start_after_key =
            req.page_token.as_ref().and_then(|token| String::from_utf8(token.clone()).ok());

        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let users = sys_svc.list_users(start_after_key.as_deref(), page_size + 1).map_err(|e| {
            ctx.set_error("Internal", &e.to_string());
            Status::internal(format!("Failed to list users: {e}"))
        })?;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let has_more = users.len() > page_size;
        let page_users: Vec<_> = users.into_iter().take(page_size).collect();

        let next_page_token = if has_more {
            page_users.last().map(|u| format!("user:{}", u.id.value()).into_bytes())
        } else {
            None
        };

        let proto_users: Vec<proto::User> = page_users
            .iter()
            .map(|u| {
                let slug = slug_resolver.resolve_user_slug(u.id).ok();
                domain_user_to_proto(u, slug)
            })
            .collect();

        ctx.set_success();
        Ok(Response::new(ListUsersResponse { users: proto_users, next_page_token }))
    }

    async fn search_users(
        &self,
        request: Request<SearchUsersRequest>,
    ) -> Result<Response<SearchUsersResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "UserService",
            "search_users",
            &grpc_metadata,
            &trace_ctx,
        );

        let filter = req.filter.ok_or_else(|| {
            ctx.set_error("InvalidArgument", "filter is required");
            Status::invalid_argument("filter is required")
        })?;

        // Currently only email search is implemented
        let email = filter.email.ok_or_else(|| {
            ctx.set_error("InvalidArgument", "email filter is required");
            Status::invalid_argument("At least email filter must be provided")
        })?;

        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let user = sys_svc.search_users_by_email(&email).map_err(|e| {
            ctx.set_error("Internal", &e.to_string());
            Status::internal(format!("Search failed: {e}"))
        })?;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let users: Vec<proto::User> = user
            .into_iter()
            .map(|u| {
                let slug = slug_resolver.resolve_user_slug(u.id).ok();
                domain_user_to_proto(&u, slug)
            })
            .collect();

        ctx.set_success();
        Ok(Response::new(SearchUsersResponse { users, next_page_token: None }))
    }

    async fn create_user_email(
        &self,
        request: Request<CreateUserEmailRequest>,
    ) -> Result<Response<CreateUserEmailResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "UserService",
            "create_user_email",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver.extract_and_resolve_user(&req.user).inspect_err(|status| {
            ctx.set_error("InvalidArgument", status.message());
        })?;

        validation::validate_email(&req.email).map_err(|e| {
            let msg = e.to_string();
            ctx.set_error("InvalidArgument", &msg);
            Status::invalid_argument(msg)
        })?;

        if req.email_hmac.is_empty() {
            ctx.set_error("InvalidArgument", "email_hmac is required");
            return Err(Status::invalid_argument("email_hmac is required"));
        }

        // Look up user's region from GLOBAL directory for regional proposal.
        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let dir_entry = sys_svc.get_user_directory(user_id).map_err(|e| {
            ctx.set_error("Internal", &e.to_string());
            Status::internal(format!("Failed to read user directory: {e}"))
        })?;
        let dir_entry = dir_entry.ok_or_else(|| {
            ctx.set_error("NotFound", "User directory entry not found");
            Status::not_found("User directory entry not found")
        })?;
        let region = dir_entry.region.ok_or_else(|| {
            ctx.set_error("FailedPrecondition", "User has no region (erased?)");
            Status::failed_precondition("User has no region assigned")
        })?;

        // Step 1 (GLOBAL): Register email HMAC — no plaintext PII in the GLOBAL Raft log.
        // Pre-check for idempotent retry: if the HMAC is already registered to
        // this user (from a previous partial attempt), skip the GLOBAL proposal.
        let existing_owner = sys_svc.get_email_hash(&req.email_hmac).map_err(|e| {
            ctx.set_error("Internal", &e.to_string());
            Status::internal(format!("Failed to check email hash: {e}"))
        })?;
        match existing_owner {
            Some(EmailHashEntry::Active(owner)) if owner == user_id => {
                // Idempotent retry — HMAC already registered to this user.
            },
            Some(_) => {
                ctx.set_error("AlreadyExists", "Email already registered to another user");
                return Err(Status::already_exists("Email already registered to another user"));
            },
            None => {
                // Register the HMAC in GLOBAL.
                let hmac_response = self
                    .ctx
                    .propose_system_request(
                        SystemRequest::RegisterEmailHash {
                            hmac_hex: req.email_hmac.clone(),
                            user_id,
                        },
                        &grpc_metadata,
                        &mut ctx,
                    )
                    .await?;

                match hmac_response {
                    LedgerResponse::Empty => {},
                    LedgerResponse::Error {
                        code: inferadb_ledger_types::ErrorCode::AlreadyExists,
                        ..
                    } => {
                        // Race: another request registered between our check and proposal.
                        // Re-check ownership.
                        let owner = sys_svc.get_email_hash(&req.email_hmac).map_err(|e| {
                            ctx.set_error("Internal", &e.to_string());
                            Status::internal(format!("Failed to re-check email hash: {e}"))
                        })?;
                        if owner != Some(EmailHashEntry::Active(user_id)) {
                            ctx.set_error(
                                "AlreadyExists",
                                "Email already registered to another user",
                            );
                            return Err(Status::already_exists(
                                "Email already registered to another user",
                            ));
                        }
                        // HMAC registered to this user (concurrent request) — safe to proceed.
                    },
                    LedgerResponse::Error { code, message } => {
                        ctx.set_error(code.grpc_code_name(), &message);
                        return Err(super::helpers::error_code_to_status(
                            code,
                            format!("HMAC registration failed: {message}"),
                        ));
                    },
                    other => {
                        ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                        return Err(Status::internal("Unexpected response from HMAC registration"));
                    },
                }
            },
        }

        // Step 2 (Regional): Store the email record — plaintext stays in-region.
        let response = self
            .ctx
            .propose_regional_encrypted(
                region,
                SystemRequest::CreateUserEmail { user_id, email: req.email },
                user_id,
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::UserEmailCreated { email_id } => {
                if let Some(node_id) = self.ctx.node_id {
                    self.ctx.record_handler_event(
                        HandlerPhaseEmitter::for_system(EventAction::UserEmailCreated, node_id)
                            .principal("system")
                            .detail("user_id", &user_id.to_string())
                            .detail("email_id", &email_id.to_string())
                            .trace_id(&trace_ctx.trace_id)
                            .outcome(EventOutcomeType::Success)
                            .build(self.ctx.default_ttl_days()),
                    );
                }

                // Read back from REGIONAL state (email record lives in-region).
                let regional_state = self.ctx.regional_state(region)?;
                let regional_sys = SystemOrganizationService::new(regional_state);
                let email = regional_sys
                    .get_user_email(email_id)
                    .map_err(|e| Status::internal(format!("Failed to read created email: {e}")))?;

                ctx.set_success();
                Ok(Response::new(CreateUserEmailResponse {
                    email: email.map(|e| domain_email_to_proto(&e)),
                }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(
                    code,
                    format!("Email creation failed: {message}"),
                ))
            },
            other => {
                ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                Err(Status::internal("Unexpected response from Raft state machine"))
            },
        }
    }

    async fn delete_user_email(
        &self,
        request: Request<DeleteUserEmailRequest>,
    ) -> Result<Response<DeleteUserEmailResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "UserService",
            "delete_user_email",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver.extract_and_resolve_user(&req.user).inspect_err(|status| {
            ctx.set_error("InvalidArgument", status.message());
        })?;

        let email_id = req.email_id.ok_or_else(|| {
            ctx.set_error("InvalidArgument", "email_id is required");
            Status::invalid_argument("email_id is required")
        })?;
        let domain_email_id = DomainUserEmailId::new(email_id.id);

        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::DeleteUserEmail { user_id, email_id: domain_email_id },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::UserEmailDeleted { .. } => {
                if let Some(node_id) = self.ctx.node_id {
                    self.ctx.record_handler_event(
                        HandlerPhaseEmitter::for_system(EventAction::UserEmailDeleted, node_id)
                            .principal("system")
                            .detail("user_id", &user_id.to_string())
                            .detail("email_id", &domain_email_id.to_string())
                            .trace_id(&trace_ctx.trace_id)
                            .outcome(EventOutcomeType::Success)
                            .build(self.ctx.default_ttl_days()),
                    );
                }

                ctx.set_success();
                Ok(Response::new(DeleteUserEmailResponse {
                    deleted_at: Some(crate::proto_compat::datetime_to_proto(&Utc::now())),
                }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(
                    code,
                    format!("Email deletion failed: {message}"),
                ))
            },
            other => {
                ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                Err(Status::internal("Unexpected response from Raft state machine"))
            },
        }
    }

    async fn search_user_email(
        &self,
        request: Request<SearchUserEmailRequest>,
    ) -> Result<Response<SearchUserEmailResponse>, Status> {
        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "UserService",
            "search_user_email",
            &grpc_metadata,
            &trace_ctx,
        );

        let filter = req.filter.ok_or_else(|| {
            ctx.set_error("InvalidArgument", "filter is required");
            Status::invalid_argument("filter is required")
        })?;

        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());

        // If user filter is set, list that user's emails
        if let Some(ref user_slug) = filter.user {
            let user_id = slug_resolver.extract_and_resolve_user(&Some(*user_slug)).inspect_err(
                |status| {
                    ctx.set_error("InvalidArgument", status.message());
                },
            )?;

            let emails = sys_svc.get_user_emails(user_id).map_err(|e| {
                ctx.set_error("Internal", &e.to_string());
                Status::internal(format!("Failed to list user emails: {e}"))
            })?;

            ctx.set_success();
            return Ok(Response::new(SearchUserEmailResponse {
                emails: emails.iter().map(domain_email_to_proto).collect(),
                next_page_token: None,
            }));
        }

        // If email filter is set, search by email
        if let Some(ref email) = filter.email {
            let user = sys_svc.search_users_by_email(email).map_err(|e| {
                ctx.set_error("Internal", &e.to_string());
                Status::internal(format!("Email search failed: {e}"))
            })?;

            if let Some(user) = user {
                let emails = sys_svc
                    .get_user_emails(user.id)
                    .map_err(|e| Status::internal(format!("Failed to list user emails: {e}")))?;
                let matching: Vec<proto::UserEmail> = emails
                    .iter()
                    .filter(|e| e.email.eq_ignore_ascii_case(email))
                    .map(domain_email_to_proto)
                    .collect();

                ctx.set_success();
                return Ok(Response::new(SearchUserEmailResponse {
                    emails: matching,
                    next_page_token: None,
                }));
            }
        }

        ctx.set_success();
        Ok(Response::new(SearchUserEmailResponse { emails: vec![], next_page_token: None }))
    }

    async fn verify_user_email(
        &self,
        request: Request<VerifyUserEmailRequest>,
    ) -> Result<Response<VerifyUserEmailResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "UserService",
            "verify_user_email",
            &grpc_metadata,
            &trace_ctx,
        );

        if req.token.is_empty() {
            ctx.set_error("InvalidArgument", "token must be non-empty");
            return Err(Status::invalid_argument("token must be non-empty"));
        }

        // Hash the plaintext token and look up the verification record
        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let token_record = sys_svc.get_verification_token_by_hash(&req.token).map_err(|e| {
            ctx.set_error("Internal", &e.to_string());
            Status::internal(format!("Failed to look up verification token: {e}"))
        })?;
        let token_record = token_record.ok_or_else(|| {
            ctx.set_error("NotFound", "Verification token not found or expired");
            Status::not_found("Verification token not found or expired")
        })?;

        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::VerifyUserEmail { email_id: token_record.email_id },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::UserEmailVerified { email_id } => {
                if let Some(node_id) = self.ctx.node_id {
                    self.ctx.record_handler_event(
                        HandlerPhaseEmitter::for_system(EventAction::UserEmailVerified, node_id)
                            .principal("system")
                            .detail("email_id", &email_id.to_string())
                            .trace_id(&trace_ctx.trace_id)
                            .outcome(EventOutcomeType::Success)
                            .build(self.ctx.default_ttl_days()),
                    );
                }

                let email = sys_svc
                    .get_user_email(email_id)
                    .map_err(|e| Status::internal(format!("Failed to read verified email: {e}")))?;

                ctx.set_success();
                Ok(Response::new(VerifyUserEmailResponse {
                    email: email.map(|e| domain_email_to_proto(&e)),
                }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(
                    code,
                    format!("Email verification failed: {message}"),
                ))
            },
            other => {
                ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                Err(Status::internal("Unexpected response from Raft state machine"))
            },
        }
    }

    async fn migrate_user_region(
        &self,
        request: Request<MigrateUserRegionRequest>,
    ) -> Result<Response<MigrateUserRegionResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "UserService",
            "migrate_user_region",
            &grpc_metadata,
            &trace_ctx,
        );

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_slug_val = req.slug.as_ref().map_or(0, |s| s.slug);
        let user_id = slug_resolver.extract_and_resolve_user(&req.slug).inspect_err(|status| {
            ctx.set_error("InvalidArgument", status.message());
        })?;

        let target_region = inferadb_ledger_proto::convert::region_from_i32(req.target_region)?;

        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let dir_entry = sys_svc.get_user_directory(user_id).map_err(|e| {
            ctx.set_error("Internal", &e.to_string());
            Status::internal(format!("Failed to read user directory: {e}"))
        })?;
        let dir_entry = dir_entry.ok_or_else(|| {
            ctx.set_error("NotFound", "User directory entry not found");
            let err: Status = ServiceError::not_found("User", user_slug_val.to_string()).into();
            err
        })?;

        let source_region = dir_entry.region.unwrap_or(inferadb_ledger_types::Region::GLOBAL);

        if target_region == source_region {
            return Err(Status::invalid_argument(format!(
                "User is already in region {}",
                source_region.as_str()
            )));
        }

        if dir_entry.status != inferadb_ledger_state::system::UserDirectoryStatus::Active {
            let mut context = std::collections::HashMap::new();
            context.insert("status".to_string(), format!("{:?}", dir_entry.status));
            let details = super::error_details::build_error_details(
                inferadb_ledger_types::DiagnosticCode::AppUserMigrating.as_u16(),
                true,
                None,
                context,
                Some(inferadb_ledger_types::DiagnosticCode::AppUserMigrating.suggested_action()),
            );
            let encoded = prost::Message::encode_to_vec(&details);
            return Err(Status::with_details(
                tonic::Code::FailedPrecondition,
                format!("User is not Active (current status: {:?})", dir_entry.status),
                encoded.into(),
            ));
        }

        if target_region == inferadb_ledger_types::Region::GLOBAL {
            return Err(Status::invalid_argument("Cannot migrate to GLOBAL control plane region"));
        }

        if target_region.requires_residency() {
            let nodes = sys_svc.list_nodes().map_err(|e| {
                Status::internal(format!("Failed to list nodes for region validation: {e}"))
            })?;
            let in_region_count = nodes.iter().filter(|n| n.region == target_region).count();
            if in_region_count < 3 {
                let mut context = std::collections::HashMap::new();
                context.insert("region".to_string(), target_region.as_str().to_string());
                context.insert("available_nodes".to_string(), in_region_count.to_string());
                context.insert("required_nodes".to_string(), "3".to_string());
                let details = super::error_details::build_error_details(
                    inferadb_ledger_types::DiagnosticCode::AppInsufficientRegionNodes.as_u16(),
                    false,
                    None,
                    context,
                    Some(
                        inferadb_ledger_types::DiagnosticCode::AppInsufficientRegionNodes
                            .suggested_action(),
                    ),
                );
                let encoded = prost::Message::encode_to_vec(&details);
                return Err(Status::with_details(
                    tonic::Code::FailedPrecondition,
                    format!(
                        "Insufficient in-region nodes for protected region {}: \
                         {in_region_count} available, 3 required",
                        target_region.as_str()
                    ),
                    encoded.into(),
                ));
            }
        }

        let saga_id = SagaId::new(uuid::Uuid::new_v4().to_string());
        let saga = MigrateUserSaga::new(
            saga_id,
            MigrateUserInput { user: user_id, source_region, target_region },
        );

        let saga_key = format!("saga:{}", saga.id);
        let saga_wrapped = Saga::MigrateUser(saga);
        let saga_bytes = serde_json::to_vec(&saga_wrapped).map_err(|e| {
            Status::internal(format!("Failed to serialize user migration saga: {e}"))
        })?;

        let saga_op = inferadb_ledger_types::Operation::SetEntity {
            key: saga_key,
            value: saga_bytes,
            expires_at: None,
            condition: Some(inferadb_ledger_types::SetCondition::MustNotExist),
        };
        let saga_txn = inferadb_ledger_types::Transaction {
            id: *uuid::Uuid::new_v4().as_bytes(),
            client_id: inferadb_ledger_types::ClientId::new("system:user"),
            sequence: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            operations: vec![saga_op],
            timestamp: Utc::now(),
            actor: "system:user".to_string(),
        };
        let saga_request = LedgerRequest::Write {
            organization: inferadb_ledger_types::OrganizationId::new(0),
            vault: DomainVaultId::new(0),
            transactions: vec![saga_txn],
            idempotency_key: [0; 16],
            request_hash: 0,
        };

        self.ctx.propose_request(saga_request, &grpc_metadata, &mut ctx).await?;

        ctx.set_success();
        let proto_source: ProtoRegion = source_region.into();
        let proto_target: ProtoRegion = target_region.into();
        Ok(Response::new(MigrateUserRegionResponse {
            slug: Some(ProtoUserSlug { slug: user_slug_val }),
            source_region: proto_source.into(),
            target_region: proto_target.into(),
            directory_status: "migrating".to_string(),
        }))
    }

    async fn erase_user(
        &self,
        request: Request<EraseUserRequest>,
    ) -> Result<Response<EraseUserResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx =
            self.ctx.make_request_context("UserService", "erase_user", &grpc_metadata, &trace_ctx);

        let region = inferadb_ledger_proto::convert::region_from_i32(req.region)?;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver.extract_and_resolve_user(&req.user).inspect_err(|status| {
            ctx.set_error("InvalidArgument", status.message());
        })?;

        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::EraseUser { user_id, region },
                &grpc_metadata,
                &mut ctx,
            )
            .await?;

        match response {
            LedgerResponse::UserErased { user_id: erased_id } => {
                if let Some(node_id) = self.ctx.node_id {
                    self.ctx.record_handler_event(
                        HandlerPhaseEmitter::for_system(EventAction::UserErased, node_id)
                            .principal(&req.erased_by)
                            .detail("user_id", &erased_id.to_string())
                            .detail("region", region.as_str())
                            .trace_id(&trace_ctx.trace_id)
                            .outcome(EventOutcomeType::Success)
                            .build(self.ctx.default_ttl_days()),
                    );
                }

                ctx.set_success();
                let erased_slug =
                    slug_resolver.resolve_user_slug(erased_id).inspect_err(|status| {
                        ctx.set_error("InternalError", status.message());
                    })?;
                Ok(Response::new(EraseUserResponse {
                    user: Some(proto::UserSlug { slug: erased_slug.value() }),
                }))
            },
            LedgerResponse::Error { code, message } => {
                ctx.set_error(code.grpc_code_name(), &message);
                Err(super::helpers::error_code_to_status(
                    code,
                    format!("Erasure failed: {message}"),
                ))
            },
            other => {
                ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                Err(Status::internal("Unexpected response from Raft state machine"))
            },
        }
    }

    async fn initiate_email_verification(
        &self,
        request: Request<InitiateEmailVerificationRequest>,
    ) -> Result<Response<InitiateEmailVerificationResponse>, Status> {
        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "UserService",
            "initiate_email_verification",
            &grpc_metadata,
            &trace_ctx,
        );

        // Validate inputs
        validation::validate_email(&req.email).map_err(|e| {
            let msg = e.to_string();
            ctx.set_error("InvalidArgument", &msg);
            Status::invalid_argument(msg)
        })?;
        let region = inferadb_ledger_proto::convert::region_from_i32(req.region)?;

        // Require email blinding key
        let blinding_key = self.ctx.email_blinding_key.as_ref().ok_or_else(|| {
            ctx.set_error("FailedPrecondition", "email blinding key not configured");
            Status::failed_precondition(
                "Onboarding is not enabled: email blinding key not configured",
            )
        })?;

        // Compute HMAC and generate verification code
        let email_hmac = inferadb_ledger_types::compute_email_hmac(blinding_key, &req.email);
        let (code, code_hash) =
            inferadb_ledger_types::email_hash::generate_verification_code(blinding_key);
        let expires_at = Utc::now() + inferadb_ledger_types::onboarding::CODE_TTL;

        // Propose to REGIONAL Raft (code hash is not PII, email excluded from log)
        let system_request =
            SystemRequest::CreateEmailVerification { email_hmac, code_hash, region, expires_at };
        let response =
            self.ctx.propose_regional(region, system_request, &grpc_metadata, &mut ctx).await?;

        match response {
            LedgerResponse::EmailVerificationCreated => {},
            LedgerResponse::Error { code: err_code, message } => {
                ctx.set_error(err_code.grpc_code_name(), &message);
                return Err(super::helpers::error_code_to_status(err_code, message));
            },
            other => {
                ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                return Err(Status::internal("Unexpected response from Raft state machine"));
            },
        }

        inferadb_ledger_raft::metrics::record_onboarding_initiation("success");
        Ok(Response::new(InitiateEmailVerificationResponse { code }))
    }

    async fn verify_email_code(
        &self,
        request: Request<VerifyEmailCodeRequest>,
    ) -> Result<Response<VerifyEmailCodeResponse>, Status> {
        use inferadb_ledger_raft::types::EmailCodeVerifiedResult;

        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "UserService",
            "verify_email_code",
            &grpc_metadata,
            &trace_ctx,
        );

        // Validate inputs
        validation::validate_email(&req.email).map_err(|e| {
            let msg = e.to_string();
            ctx.set_error("InvalidArgument", &msg);
            Status::invalid_argument(msg)
        })?;
        if req.code.is_empty() {
            ctx.set_error("InvalidArgument", "code is required");
            return Err(Status::invalid_argument("code is required"));
        }
        let region = inferadb_ledger_proto::convert::region_from_i32(req.region)?;

        // Require email blinding key
        let blinding_key = self.ctx.email_blinding_key.as_ref().ok_or_else(|| {
            ctx.set_error("FailedPrecondition", "email blinding key not configured");
            Status::failed_precondition(
                "Onboarding is not enabled: email blinding key not configured",
            )
        })?;

        // Compute HMAC and code hash
        let email_hmac = inferadb_ledger_types::compute_email_hmac(blinding_key, &req.email);
        let code_hash =
            inferadb_ledger_types::email_hash::compute_code_hash(blinding_key, &req.code);

        // Pre-resolve: check GLOBAL HMAC index for existing *active* user.
        // Provisioning entries (in-flight onboarding sagas) are not existing users.
        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        let existing_user_hmac_hit = matches!(
            sys_svc
                .get_email_hash(&email_hmac)
                .map_err(|e| Status::internal(format!("Failed to read email hash index: {e}")))?,
            Some(EmailHashEntry::Active(_))
        );

        // Pre-generate onboarding token for new-user path
        let (onboarding_token, onboarding_token_hash) =
            inferadb_ledger_types::onboarding::generate_onboarding_token();
        let onboarding_expires_at = Utc::now() + inferadb_ledger_types::onboarding::ONBOARDING_TTL;

        // Propose to REGIONAL Raft (verification region)
        let system_request = SystemRequest::VerifyEmailCode {
            email_hmac: email_hmac.clone(),
            code_hash,
            region,
            existing_user_hmac_hit,
            onboarding_token_hash,
            onboarding_expires_at,
        };
        let response =
            self.ctx.propose_regional(region, system_request, &grpc_metadata, &mut ctx).await?;

        // Branch on result
        let verified = match response {
            LedgerResponse::EmailCodeVerified { result } => result,
            LedgerResponse::Error { code: err_code, message } => {
                ctx.set_error(err_code.grpc_code_name(), &message);
                return Err(super::helpers::error_code_to_status(err_code, message));
            },
            other => {
                ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                return Err(Status::internal("Unexpected response from Raft state machine"));
            },
        };

        match verified {
            EmailCodeVerifiedResult::ExistingUser => {
                // Re-read UserDirectoryEntry from GLOBAL to find actual region
                let hash_entry = sys_svc
                    .get_email_hash(&email_hmac)
                    .map_err(|e| Status::internal(format!("Failed to re-read email hash: {e}")))?
                    .ok_or_else(|| Status::internal("Email hash vanished after verification"))?;

                let user_id = match hash_entry {
                    EmailHashEntry::Active(uid) => uid,
                    EmailHashEntry::Provisioning(_) => {
                        return Err(Status::internal(
                            "Email in provisioning state after ExistingUser result",
                        ));
                    },
                };

                let dir_entry = sys_svc
                    .get_user_directory(user_id)
                    .map_err(|e| Status::internal(format!("Failed to read user directory: {e}")))?
                    .ok_or_else(|| Status::internal("User directory entry not found"))?;

                let user_region = dir_entry
                    .region
                    .ok_or_else(|| Status::internal("User directory entry has no region"))?;
                let user_slug = dir_entry
                    .slug
                    .ok_or_else(|| Status::internal("User directory entry has no slug"))?;

                // Read user from actual region
                let regional_state = self.ctx.regional_state(user_region)?;
                let regional_sys = SystemOrganizationService::new(regional_state);
                let user = regional_sys
                    .get_user(user_id)
                    .map_err(|e| Status::internal(format!("Failed to read user: {e}")))?
                    .ok_or_else(|| Status::not_found("User not found in region"))?;

                // Check status — non-Active users get the same error as invalid code
                // to prevent information leakage about account state (PRD #6)
                if user.status != inferadb_ledger_types::UserStatus::Active {
                    return Err(Status::permission_denied("Invalid verification code"));
                }

                let session = self
                    .create_user_session(
                        &sys_svc,
                        user_slug,
                        user.role,
                        user.version,
                        user_region,
                        &grpc_metadata,
                        &mut ctx,
                    )
                    .await?;

                inferadb_ledger_raft::metrics::record_onboarding_verification("success");
                ctx.set_success();
                Ok(Response::new(VerifyEmailCodeResponse {
                    result: Some(proto::verify_email_code_response::Result::ExistingUser(
                        proto::ExistingUserSession {
                            user: Some(ProtoUserSlug { slug: user_slug.value() }),
                            session: Some(proto::TokenPair {
                                access_token: session.access_token,
                                refresh_token: session.refresh_token,
                                access_expires_at: Some(crate::proto_compat::datetime_to_proto(
                                    &session.access_expires_at,
                                )),
                                refresh_expires_at: Some(crate::proto_compat::datetime_to_proto(
                                    &session.refresh_expires_at,
                                )),
                            }),
                        },
                    )),
                }))
            },
            EmailCodeVerifiedResult::NewUser => {
                inferadb_ledger_raft::metrics::record_onboarding_verification("success");
                ctx.set_success();
                Ok(Response::new(VerifyEmailCodeResponse {
                    result: Some(proto::verify_email_code_response::Result::NewUser(
                        proto::OnboardingSession { onboarding_token },
                    )),
                }))
            },
        }
    }

    async fn complete_registration(
        &self,
        request: Request<CompleteRegistrationRequest>,
    ) -> Result<Response<CompleteRegistrationResponse>, Status> {
        use inferadb_ledger_raft::{OnboardingPii, SagaSubmission};
        use inferadb_ledger_state::system::{
            CreateOnboardingUserInput, CreateOnboardingUserSaga, Saga, SagaId,
        };
        use inferadb_ledger_types::{
            TokenVersion,
            onboarding::SAGA_COMPLETION_TIMEOUT,
            snowflake::{generate_organization_slug, generate_user_slug},
        };

        inferadb_ledger_raft::deadline::check_near_deadline(&request)?;
        super::helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let trace_ctx = trace_context::extract_or_generate(request.metadata());
        let grpc_metadata = request.metadata().clone();
        let req = request.into_inner();

        let mut ctx = self.ctx.make_request_context(
            "UserService",
            "complete_registration",
            &grpc_metadata,
            &trace_ctx,
        );

        // Validate inputs
        validation::validate_email(&req.email).map_err(|e| {
            let msg = e.to_string();
            ctx.set_error("InvalidArgument", &msg);
            Status::invalid_argument(msg)
        })?;
        validation::validate_user_name(&req.name).map_err(|e| {
            let msg = e.to_string();
            ctx.set_error("InvalidArgument", &msg);
            Status::invalid_argument(msg)
        })?;
        validation::validate_organization_name(&req.organization_name, &self.ctx.validation_config)
            .map_err(|e| {
                let msg = e.to_string();
                ctx.set_error("InvalidArgument", &msg);
                Status::invalid_argument(msg)
            })?;
        if req.onboarding_token.is_empty() {
            ctx.set_error("InvalidArgument", "onboarding_token is required");
            return Err(Status::invalid_argument("onboarding_token is required"));
        }
        let region = inferadb_ledger_proto::convert::region_from_i32(req.region)?;

        // Require email blinding key
        let blinding_key = self.ctx.email_blinding_key.as_ref().ok_or_else(|| {
            ctx.set_error("FailedPrecondition", "email blinding key not configured");
            Status::failed_precondition(
                "Onboarding is not enabled: email blinding key not configured",
            )
        })?;

        // Compute email HMAC
        let email_hmac = inferadb_ledger_types::compute_email_hmac(blinding_key, &req.email);

        // Decode token → raw bytes → SHA-256(raw_bytes)
        let raw_bytes =
            inferadb_ledger_types::onboarding::decode_onboarding_token(&req.onboarding_token)
                .map_err(|e| {
                    ctx.set_error("InvalidArgument", &format!("Invalid onboarding token: {e}"));
                    Status::invalid_argument(format!("Invalid onboarding token: {e}"))
                })?;
        let token_hash: [u8; 32] = {
            use sha2::Digest as _;
            sha2::Sha256::digest(raw_bytes).into()
        };

        // Idempotency check: read GLOBAL HMAC index
        let sys_svc = SystemOrganizationService::new(self.ctx.state.clone());
        if let Some(hash_entry) = sys_svc
            .get_email_hash(&email_hmac)
            .map_err(|e| Status::internal(format!("Failed to read email hash index: {e}")))?
        {
            match hash_entry {
                EmailHashEntry::Active(user_id) => {
                    // Security tradeoff #15: return fresh session for idempotent re-registration
                    let dir_entry = sys_svc
                        .get_user_directory(user_id)
                        .map_err(|e| {
                            Status::internal(format!("Failed to read user directory: {e}"))
                        })?
                        .ok_or_else(|| Status::internal("User directory entry not found"))?;

                    let user_slug = dir_entry
                        .slug
                        .ok_or_else(|| Status::internal("User directory has no slug"))?;
                    let user_region = dir_entry
                        .region
                        .ok_or_else(|| Status::internal("User directory has no region"))?;

                    // Read user from actual region to get role + version
                    let regional_state = self.ctx.regional_state(user_region)?;
                    let regional_sys = SystemOrganizationService::new(regional_state);
                    let user = regional_sys
                        .get_user(user_id)
                        .map_err(|e| Status::internal(format!("Failed to read user: {e}")))?
                        .ok_or_else(|| Status::internal("User record not found in region"))?;

                    let session = self
                        .create_user_session(
                            &sys_svc,
                            user_slug,
                            user.role,
                            user.version,
                            user_region,
                            &grpc_metadata,
                            &mut ctx,
                        )
                        .await?;

                    let token_pair = proto::TokenPair {
                        access_token: session.access_token,
                        refresh_token: session.refresh_token,
                        access_expires_at: Some(crate::proto_compat::datetime_to_proto(
                            &session.access_expires_at,
                        )),
                        refresh_expires_at: Some(crate::proto_compat::datetime_to_proto(
                            &session.refresh_expires_at,
                        )),
                    };

                    return Ok(Response::new(CompleteRegistrationResponse {
                        user: Some(proto::User {
                            slug: Some(ProtoUserSlug { slug: user_slug.value() }),
                            ..Default::default()
                        }),
                        session: Some(token_pair),
                        organization: None,
                    }));
                },
                EmailHashEntry::Provisioning(_) => {
                    return Err(Status::already_exists(
                        "Registration already in progress for this email",
                    ));
                },
            }
        }

        // Read regional onboarding account and validate token hash
        let regional_state = self.ctx.regional_state(region)?;
        let regional_sys = SystemOrganizationService::new(regional_state);
        let account = regional_sys
            .get_onboarding_account_by_hmac(&email_hmac)
            .map_err(|e| Status::internal(format!("Failed to read onboarding account: {e}")))?
            .ok_or_else(|| Status::not_found("No onboarding account found — verify email first"))?;

        // Validate token hash (constant-time comparison)
        if !inferadb_ledger_types::hash_eq(&token_hash, &account.token_hash) {
            return Err(Status::permission_denied("Invalid onboarding token"));
        }

        // Check expiration
        if Utc::now() > account.expires_at {
            return Err(Status::failed_precondition("Onboarding token has expired"));
        }

        // Generate slugs for the new user and organization
        let user_slug = generate_user_slug()
            .map_err(|e| Status::internal(format!("Failed to generate user slug: {e}")))?;
        let organization_slug = generate_organization_slug()
            .map_err(|e| Status::internal(format!("Failed to generate org slug: {e}")))?;

        // Build saga
        let saga_id = SagaId::new(uuid::Uuid::new_v4().to_string());
        let saga = CreateOnboardingUserSaga::new(
            saga_id,
            CreateOnboardingUserInput {
                email_hmac: email_hmac.clone(),
                region,
                user_slug,
                organization_slug,
            },
        );

        // Create oneshot for saga completion notification
        let (notify_tx, notify_rx) = tokio::sync::oneshot::channel();

        // Get saga handle
        let saga_handle = self.ctx.saga_handle.get().ok_or_else(|| {
            Status::unavailable("Saga orchestrator not ready — try again shortly")
        })?;

        // Submit saga
        saga_handle
            .submit_saga(SagaSubmission {
                record: Saga::CreateOnboardingUser(saga),
                pii: Some(OnboardingPii {
                    email: req.email,
                    name: req.name,
                    organization_name: req.organization_name,
                }),
                org_pii: None,
                notify: Some(notify_tx),
            })
            .await
            .map_err(|e| Status::unavailable(format!("Failed to submit saga: {e}")))?;

        // Await completion with timeout
        let saga_result = tokio::time::timeout(SAGA_COMPLETION_TIMEOUT, notify_rx)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("Registration saga timed out — it may still complete")
            })?
            .map_err(|_| Status::internal("Saga orchestrator dropped notification channel"))?
            .map_err(|e| Status::internal(format!("Saga failed: {e}")))?;

        // Sign JWT for the new user session
        let signing_key = self.ensure_signing_key_cached(&sys_svc)?;
        let jwt_engine = self
            .ctx
            .jwt_engine
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("JWT engine not configured"))?;
        let (access_token, access_expires_at) = jwt_engine
            .sign_user_session(
                saga_result.user_slug,
                "user",
                TokenVersion::new(1),
                &signing_key.kid,
            )
            .map_err(|e| Status::internal(format!("Failed to sign access token: {e}")))?;

        let token_pair = proto::TokenPair {
            access_token,
            refresh_token: saga_result.refresh_token,
            access_expires_at: Some(crate::proto_compat::datetime_to_proto(&access_expires_at)),
            refresh_expires_at: Some(crate::proto_compat::datetime_to_proto(
                &saga_result.refresh_expires_at,
            )),
        };

        // Build response user
        let response_user = proto::User {
            slug: Some(ProtoUserSlug { slug: saga_result.user_slug.value() }),
            ..Default::default()
        };

        inferadb_ledger_raft::metrics::record_onboarding_registration("success");
        ctx.set_success();
        Ok(Response::new(CompleteRegistrationResponse {
            user: Some(response_user),
            session: Some(token_pair),
            organization: Some(proto::OrganizationSlug {
                slug: saga_result.organization_slug.value(),
            }),
        }))
    }
}
