//! Wire-protocol shim for `UserService` (Phase 0.0.E.4.13).
//!
//! Adds an [`inferadb_ledger_wire_services::UserService`] impl alongside
//! the tonic impl on [`super::user::UserService`]. The wire impl converts
//! wire types → proto types, delegates to the existing tonic handler body,
//! then converts the proto response → wire response. Both impls coexist
//! during migration; F.1 deletes the tonic impl and inlines the body onto
//! the wire trait directly.
//!
//! Conversions are mechanical field-by-field copies because the wire types
//! in [`inferadb_ledger_wire::services::user`] mirror the proto layout
//! exactly (E.2). The non-trivial mappings:
//!
//! - `created_at` / `updated_at` / `verified_at` / `deleted_at` / `last_used_at` are
//!   `Option<prost_types::Timestamp>` in proto and `u64` (or `Option<u64>`) UNIX nanoseconds on the
//!   wire — bridged by the shared helpers in [`super::wire_shared`].
//! - `page_token` / `next_page_token` are `Option<Vec<u8>>` in proto and `Option<Bytes>` on the
//!   wire.
//! - The proto enums (`UserStatus`, `UserRole`, `OrganizationTier`, `CredentialType`,
//!   `TotpAlgorithm`) carry raw `i32` values; the wire enums are `#[repr(i32)]` with matching
//!   numeric tags. Unknown tags collapse to `Unspecified` (or `Sha1` for `TotpAlgorithm`, whose
//!   zero value is `Sha1` rather than an `Unspecified` placeholder) to match proto's open-enum
//!   semantics.
//! - `UserCredential.data` and `CreateUserCredentialRequest.data` are proto oneofs (`Passkey` /
//!   `Totp` / `RecoveryCode`); the wire types are Rust enums with the same three variants.
//! - `VerifyEmailCodeResponse.result` is a proto oneof (`ExistingUser` / `NewUser` /
//!   `TotpRequired`); the wire type is `VerifyEmailCodeResult`.
//! - `TokenPair`, `CredentialInfo`, and `TotpRequired` are shared types defined in
//!   `wire::services::shared`; the bridging helpers are duplicated locally to avoid concurrent
//!   `wire_shared.rs` writes during the migration. Future deduplication is out of scope.

use std::str::FromStr;

use bytes::Bytes;
use chrono::Utc;
use inferadb_ledger_raft::types::{LedgerResponse, SystemRequest};
use inferadb_ledger_state::system::{
    CreateUserInput, CreateUserSaga, EmailHashEntry, MigrateUserInput, MigrateUserSaga, Saga,
    SagaId,
};
use inferadb_ledger_types::{
    UserEmailId as DomainUserEmailId, VaultId as DomainVaultId,
    events::{EventAction, EventOutcome as EventOutcomeType},
    validation,
};
use inferadb_ledger_wire::{
    ErrorCode, RequestContext, WireError,
    services::{shared as ws, user as w},
};

use super::{
    auth_errors::{self, AuthFailureReason},
    error_classify, helpers,
    slug_resolver::SlugResolver,
    user::UserService,
    wire_helpers::tonic_status_to_wire_error,
};

// (Numeric-tag enum mappings + CredentialInfo / TotpRequired proto bridges
// were dropped along with the wire-test inverses they served — the wire
// path produces wire types directly via `domain_user_credential_to_wire`
// and the inlined `verify_totp` / `consume_recovery_code` handlers.)

// ---------------------------------------------------------------------------
// Wire credential payload → domain (validation-bearing).
//
// Mirrors the validation rules previously delivered by
// `inferadb_ledger_types::*Credential::TryFrom<&proto::*>` so that the
// create / update credential paths no longer round-trip through the proto
// crate. Errors return wire-shaped `InvalidArgument`.
// ---------------------------------------------------------------------------

fn wire_to_domain_passkey(
    d: w::PasskeyCredentialData,
) -> Result<inferadb_ledger_types::PasskeyCredential, WireError> {
    if d.credential_id.is_empty() {
        return Err(WireError::new(
            ErrorCode::InvalidArgument,
            "passkey credential_id must not be empty",
        ));
    }
    if d.public_key.is_empty() {
        return Err(WireError::new(
            ErrorCode::InvalidArgument,
            "passkey public_key must not be empty",
        ));
    }
    let aaguid = if let Some(ref bytes) = d.aaguid {
        let arr: [u8; 16] = bytes.as_ref().try_into().map_err(|_| {
            WireError::new(ErrorCode::InvalidArgument, "aaguid must be exactly 16 bytes")
        })?;
        Some(arr)
    } else {
        None
    };
    Ok(inferadb_ledger_types::PasskeyCredential {
        credential_id: d.credential_id.to_vec(),
        public_key: d.public_key.to_vec(),
        sign_count: d.sign_count,
        transports: d.transports,
        backup_eligible: d.backup_eligible,
        backup_state: d.backup_state,
        attestation_format: d.attestation_format,
        aaguid,
    })
}

fn wire_to_domain_totp(
    d: w::TotpCredentialData,
) -> Result<inferadb_ledger_types::TotpCredential, WireError> {
    if d.secret.is_empty() {
        return Err(WireError::new(ErrorCode::InvalidArgument, "totp secret must not be empty"));
    }
    let digits = u8::try_from(d.digits).map_err(|_| {
        WireError::new(ErrorCode::InvalidArgument, "totp digits must fit in u8 (typically 6 or 8)")
    })?;
    let algorithm = match d.algorithm {
        ws::TotpAlgorithm::Sha1 => inferadb_ledger_types::TotpAlgorithm::Sha1,
        ws::TotpAlgorithm::Sha256 => inferadb_ledger_types::TotpAlgorithm::Sha256,
        ws::TotpAlgorithm::Sha512 => inferadb_ledger_types::TotpAlgorithm::Sha512,
    };
    Ok(inferadb_ledger_types::TotpCredential {
        secret: zeroize::Zeroizing::new(d.secret.to_vec()),
        algorithm,
        digits,
        period: d.period,
    })
}

fn wire_to_domain_recovery_code(
    d: w::RecoveryCodeCredentialData,
) -> Result<inferadb_ledger_types::RecoveryCodeCredential, WireError> {
    let total_generated = u8::try_from(d.total_generated).map_err(|_| {
        WireError::new(ErrorCode::InvalidArgument, "recovery code total_generated must fit in u8")
    })?;
    let code_hashes = d
        .code_hashes
        .into_iter()
        .map(|h| {
            <[u8; 32]>::try_from(h.as_ref()).map_err(|_| {
                WireError::new(
                    ErrorCode::InvalidArgument,
                    "recovery code hash must be exactly 32 bytes (SHA-256)",
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(inferadb_ledger_types::RecoveryCodeCredential { code_hashes, total_generated })
}

// ---------------------------------------------------------------------------
// Wire-trait implementation for `UserService`.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Domain → wire helpers (mirror the proto helpers at the bottom of `user.rs`).
// ---------------------------------------------------------------------------

/// Convert a domain `User` to a wire `User` directly (no proto round-trip).
fn domain_user_to_wire(
    user: &inferadb_ledger_state::system::User,
    slug: Option<inferadb_ledger_types::UserSlug>,
) -> ws::User {
    let status = match user.status {
        inferadb_ledger_types::UserStatus::Active => ws::UserStatus::Active,
        inferadb_ledger_types::UserStatus::PendingOrg => ws::UserStatus::PendingOrg,
        inferadb_ledger_types::UserStatus::Suspended => ws::UserStatus::Suspended,
        inferadb_ledger_types::UserStatus::Deleting => ws::UserStatus::Deleting,
        inferadb_ledger_types::UserStatus::Deleted => ws::UserStatus::Deleted,
    };
    let role = match user.role {
        inferadb_ledger_types::UserRole::User => ws::UserRole::User,
        inferadb_ledger_types::UserRole::Admin => ws::UserRole::Admin,
    };
    ws::User {
        id: Some(ws::UserId::new(user.id.value())),
        name: user.name.clone(),
        email: Some(ws::UserEmailId::new(user.email.value())),
        status,
        created_at: u64::try_from(user.created_at.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0),
        updated_at: u64::try_from(user.updated_at.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0),
        role,
        slug: slug.map(|s| ws::UserSlug::new(s.value())),
        deleted_at: user
            .deleted_at
            .as_ref()
            .map(|d| u64::try_from(d.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0)),
    }
}

/// Convert a domain `UserEmail` to a wire `UserEmail` directly (no proto round-trip).
fn domain_email_to_wire(email: &inferadb_ledger_state::system::UserEmail) -> ws::UserEmail {
    ws::UserEmail {
        id: Some(ws::UserEmailId::new(email.id.value())),
        user: Some(ws::UserId::new(email.user.value())),
        email: email.email.clone(),
        created_at: u64::try_from(email.created_at.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0),
        verified_at: email
            .verified_at
            .as_ref()
            .map(|d| u64::try_from(d.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0))
            .unwrap_or(0),
    }
}

/// Convert a domain `UserCredential` to a wire `UserCredential` directly.
///
/// `strip_secrets` controls whether sensitive credential data is redacted —
/// list/update responses pass `true` (TOTP secret cleared, recovery code
/// hashes cleared), while `create_user_credential` passes `false` so the
/// caller can persist the one-time TOTP setup secret.
fn domain_user_credential_to_wire(
    cred: &inferadb_ledger_types::UserCredential,
    user_slug: inferadb_ledger_types::UserSlug,
    strip_secrets: bool,
) -> w::UserCredential {
    use inferadb_ledger_types::CredentialData;

    let credential_type = match cred.credential_type {
        inferadb_ledger_types::CredentialType::Passkey => ws::CredentialType::Passkey,
        inferadb_ledger_types::CredentialType::Totp => ws::CredentialType::Totp,
        inferadb_ledger_types::CredentialType::RecoveryCode => ws::CredentialType::RecoveryCode,
    };

    let data = Some(match &cred.credential_data {
        CredentialData::Passkey(pk) => w::UserCredentialData::Passkey(w::PasskeyCredentialData {
            credential_id: Bytes::copy_from_slice(&pk.credential_id),
            public_key: Bytes::copy_from_slice(&pk.public_key),
            sign_count: pk.sign_count,
            transports: pk.transports.clone(),
            backup_eligible: pk.backup_eligible,
            backup_state: pk.backup_state,
            attestation_format: pk.attestation_format.clone(),
            aaguid: pk.aaguid.map(|a| Bytes::copy_from_slice(&a)),
        }),
        CredentialData::Totp(totp) => w::UserCredentialData::Totp(w::TotpCredentialData {
            secret: if strip_secrets {
                Bytes::new()
            } else {
                Bytes::copy_from_slice(totp.secret.as_slice())
            },
            algorithm: match totp.algorithm {
                inferadb_ledger_types::TotpAlgorithm::Sha1 => ws::TotpAlgorithm::Sha1,
                inferadb_ledger_types::TotpAlgorithm::Sha256 => ws::TotpAlgorithm::Sha256,
                inferadb_ledger_types::TotpAlgorithm::Sha512 => ws::TotpAlgorithm::Sha512,
            },
            digits: u32::from(totp.digits),
            period: totp.period,
        }),
        CredentialData::RecoveryCode(rc) => {
            w::UserCredentialData::RecoveryCode(w::RecoveryCodeCredentialData {
                code_hashes: if strip_secrets {
                    Vec::new()
                } else {
                    rc.code_hashes.iter().map(|h| Bytes::copy_from_slice(h)).collect()
                },
                total_generated: u32::from(rc.total_generated),
            })
        },
    });

    w::UserCredential {
        id: cred.id.value(),
        user: Some(ws::UserSlug::new(user_slug.value())),
        credential_type,
        name: cred.name.clone(),
        enabled: cred.enabled,
        created_at: u64::try_from(cred.created_at.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0),
        last_used_at: cred
            .last_used_at
            .as_ref()
            .map(|d| u64::try_from(d.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0)),
        data,
    }
}

impl inferadb_ledger_wire_services::UserService for UserService {
    async fn create_user(
        &self,
        request: w::CreateUserRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateUserResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from("UserService", "create_user", &_ctx);
        let req = request;

        // Validate inputs.
        validation::validate_user_name(&req.name).map_err(|e| {
            let msg = e.to_string();
            log_ctx.set_error("InvalidArgument", &msg);
            WireError::new(ErrorCode::InvalidArgument, msg)
        })?;
        validation::validate_email(&req.email).map_err(|e| {
            let msg = e.to_string();
            log_ctx.set_error("InvalidArgument", &msg);
            WireError::new(ErrorCode::InvalidArgument, msg)
        })?;
        if req.email_hmac.is_empty() {
            log_ctx.set_error("InvalidArgument", "email_hmac is required");
            return Err(WireError::new(ErrorCode::InvalidArgument, "email_hmac is required"));
        }
        if !req.organization_name.is_empty() {
            validation::validate_organization_name(
                &req.organization_name,
                &self.ctx.validation_config,
            )
            .map_err(|e| {
                let msg = e.to_string();
                log_ctx.set_error("InvalidArgument", &msg);
                WireError::new(ErrorCode::InvalidArgument, msg)
            })?;
        }

        let region = inferadb_ledger_types::Region::from_str(&req.region).map_err(|err| {
            WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
        })?;
        let role = req.role.unwrap_or(ws::UserRole::User);
        let domain_role = match role {
            ws::UserRole::Admin => inferadb_ledger_types::UserRole::Admin,
            _ => inferadb_ledger_types::UserRole::User,
        };
        let admin = domain_role == inferadb_ledger_types::UserRole::Admin;
        let default_org_tier = match req.organization_tier {
            Some(ws::OrganizationTier::Launch) => {
                inferadb_ledger_state::system::OrganizationTier::Launch
            },
            Some(ws::OrganizationTier::Scale) => {
                inferadb_ledger_state::system::OrganizationTier::Scale
            },
            _ => inferadb_ledger_state::system::OrganizationTier::Free,
        };

        // Capture the originating W3C `traceparent` so the saga can propagate
        // it into downstream regional proposals (keeps distributed traces
        // linked across the GLOBAL → regional hop).
        let traceparent = crate::services::wire_helpers::wire_context_traceparent(&_ctx);

        // Create the saga for the orchestrator to drive.
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
        )
        .with_traceparent(traceparent);
        let saga_key = format!("_meta:saga:{saga_id}");
        let saga_wrapped = Saga::CreateUser(saga);
        let saga_bytes = serde_json::to_vec(&saga_wrapped)
            .map_err(|e| error_classify::serialization_error(&e))?;

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
        };
        self.ctx
            .propose_system_request(
                SystemRequest::Write {
                    vault: DomainVaultId::new(0),
                    transactions: vec![saga_txn],
                    idempotency_key: [0; 16],
                    request_hash: 0,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        // Saga is now persisted. The orchestrator will drive it to completion.
        log_ctx.record_event(
            EventAction::UserCreated,
            EventOutcomeType::Success,
            &[
                ("saga_id", saga_id.value()),
                ("region", region.as_str()),
                ("admin", if admin { "true" } else { "false" }),
            ],
        );

        log_ctx.set_success();
        Ok(w::CreateUserResponse { slug: None, user: None, default_organization_slug: None })
    }

    async fn get_user(
        &self,
        request: w::GetUserRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetUserResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from("UserService", "get_user", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let typed_slug = req.slug.map(|s| inferadb_ledger_types::UserSlug::new(s.value()));
        let user_id = slug_resolver
            .extract_and_resolve_user(typed_slug)
            .inspect_err(|err| log_ctx.set_error("InvalidArgument", &err.message))?;
        let user_slug = SlugResolver::extract_user_slug(typed_slug)?;

        let sys_svc = self.ctx.system_service();
        let user = sys_svc.get_user(user_id).map_err(|e| {
            log_ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;

        let user = user.ok_or_else(|| {
            log_ctx.set_error("NotFound", "User not found");
            WireError::new(
                ErrorCode::NotFound,
                format!("User with slug {} not found", user_slug.value()),
            )
        })?;

        let emails = sys_svc.get_user_emails(user_id).map_err(|e| {
            log_ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;

        log_ctx.set_success();
        Ok(w::GetUserResponse {
            user: Some(domain_user_to_wire(&user, Some(user_slug))),
            emails: emails.iter().map(domain_email_to_wire).collect(),
        })
    }

    async fn update_user(
        &self,
        request: w::UpdateUserRequest,
        _ctx: RequestContext,
    ) -> Result<w::UpdateUserResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from("UserService", "update_user", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let typed_slug = req.slug.map(|s| inferadb_ledger_types::UserSlug::new(s.value()));
        let user_id = slug_resolver
            .extract_and_resolve_user(typed_slug)
            .inspect_err(|err| log_ctx.set_error("InvalidArgument", &err.message))?;
        let user_slug = SlugResolver::extract_user_slug(typed_slug)?;

        // Validate optional name.
        if let Some(ref name) = req.name {
            validation::validate_user_name(name).map_err(|e| {
                let msg = e.to_string();
                log_ctx.set_error("InvalidArgument", &msg);
                WireError::new(ErrorCode::InvalidArgument, msg)
            })?;
        }

        let role = req.role.map(|r| match r {
            ws::UserRole::Admin => inferadb_ledger_types::UserRole::Admin,
            _ => inferadb_ledger_types::UserRole::User,
        });

        let primary_email = req.primary_email.map(|e| DomainUserEmailId::new(e.value()));

        let has_global_fields = role.is_some() || primary_email.is_some();
        let has_regional_fields = req.name.is_some();

        if !has_global_fields && !has_regional_fields {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "At least one field (name, role, primary_email) must be provided",
            ));
        }

        // Step 1 (GLOBAL): Update role / primary_email — no PII in the GLOBAL Raft log.
        if has_global_fields {
            let response = self
                .ctx
                .propose_system_request(
                    SystemRequest::UpdateUser { user_id, role, primary_email },
                    &_ctx,
                    &mut log_ctx,
                )
                .await?;

            match response {
                LedgerResponse::UserUpdated { .. } => {},
                LedgerResponse::Error { code, message } => {
                    log_ctx.set_error(code.grpc_code_name(), &message);
                    return Err(helpers::error_code_to_wire_error(
                        code,
                        format!("User update failed: {message}"),
                    ));
                },
                other => {
                    log_ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                    return Err(WireError::new(
                        ErrorCode::Internal,
                        "Unexpected response from Raft state machine",
                    ));
                },
            }
        }

        // Step 2 (REGIONAL): Update name — PII stays in the regional Raft log.
        let user_region = if let Some(name) = req.name {
            let sys_svc = self.ctx.system_service();
            let dir_entry = sys_svc.get_user_directory(user_id).map_err(|e| {
                log_ctx.set_error("Internal", &e.to_string());
                error_classify::storage_error(&e)
            })?;
            let dir_entry = dir_entry.ok_or_else(|| {
                log_ctx.set_error("NotFound", "User directory entry not found");
                WireError::new(ErrorCode::NotFound, "User directory entry not found")
            })?;
            let region = dir_entry.region.ok_or_else(|| {
                log_ctx.set_error("FailedPrecondition", "User has no region (erased?)");
                WireError::new(ErrorCode::FailedPrecondition, "User has no region assigned")
            })?;

            let response = self
                .ctx
                .propose_regional_encrypted(
                    region,
                    SystemRequest::UpdateUserProfile { user_id, name },
                    user_id,
                    &_ctx,
                    &mut log_ctx,
                )
                .await?;

            match response {
                LedgerResponse::UserProfileUpdated { .. } => {},
                LedgerResponse::Error { code, message } => {
                    log_ctx.set_error(code.grpc_code_name(), &message);
                    return Err(helpers::error_code_to_wire_error(
                        code,
                        format!("User profile update failed: {message}"),
                    ));
                },
                other => {
                    log_ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                    return Err(WireError::new(
                        ErrorCode::Internal,
                        "Unexpected response from Raft state machine",
                    ));
                },
            }

            Some(region)
        } else {
            None
        };

        log_ctx.record_event(
            EventAction::UserUpdated,
            EventOutcomeType::Success,
            &[("user_id", &user_id.to_string())],
        );

        // Read from the appropriate state layer for the response.
        let user = if let Some(region) = user_region {
            let regional_state =
                self.ctx.regional_state(region).map_err(tonic_status_to_wire_error)?;
            let regional_sys = self.ctx.regional_system_service(regional_state);
            regional_sys.get_user(user_id).map_err(|e| error_classify::storage_error(&e))?
        } else {
            let sys_svc = self.ctx.system_service();
            sys_svc.get_user(user_id).map_err(|e| error_classify::storage_error(&e))?
        };

        log_ctx.set_success();
        Ok(w::UpdateUserResponse { user: user.map(|u| domain_user_to_wire(&u, Some(user_slug))) })
    }

    async fn delete_user(
        &self,
        request: w::DeleteUserRequest,
        _ctx: RequestContext,
    ) -> Result<w::DeleteUserResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from("UserService", "delete_user", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let typed_slug = req.slug.map(|s| inferadb_ledger_types::UserSlug::new(s.value()));
        let user_id = slug_resolver
            .extract_and_resolve_user(typed_slug)
            .inspect_err(|err| log_ctx.set_error("InvalidArgument", &err.message))?;
        let user_slug = SlugResolver::extract_user_slug(typed_slug)?;

        if req.caller.is_none() {
            log_ctx.set_error("InvalidArgument", "deleted_by must be non-empty");
            return Err(WireError::new(ErrorCode::InvalidArgument, "deleted_by must be non-empty"));
        }

        let response = self
            .ctx
            .propose_system_request(SystemRequest::DeleteUser { user_id }, &_ctx, &mut log_ctx)
            .await?;

        match response {
            LedgerResponse::UserSoftDeleted { user_id: deleted_id, retention_days } => {
                log_ctx.record_event(
                    EventAction::UserSoftDeleted,
                    EventOutcomeType::Success,
                    &[
                        ("user_id", &deleted_id.to_string()),
                        ("retention_days", &retention_days.to_string()),
                    ],
                );

                log_ctx.set_success();
                let now_ns =
                    u64::try_from(Utc::now().timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0);
                Ok(w::DeleteUserResponse {
                    slug: Some(ws::UserSlug::new(user_slug.value())),
                    deleted_at: now_ns,
                    retention_days,
                })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(
                    code,
                    format!("User deletion failed: {message}"),
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

    async fn list_users(
        &self,
        request: w::ListUsersRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListUsersResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from("UserService", "list_users", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let page_size = if req.page_size == 0 { 100 } else { req.page_size.min(1000) as usize };

        // Decode page token as the last entity key seen (opaque cursor).
        let start_after_key =
            req.page_token.as_ref().and_then(|token| String::from_utf8(token.to_vec()).ok());

        let sys_svc = self.ctx.system_service();
        let users = sys_svc.list_users(start_after_key.as_deref(), page_size + 1).map_err(|e| {
            log_ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let has_more = users.len() > page_size;
        let page_users: Vec<_> = users.into_iter().take(page_size).collect();

        let next_page_token = if has_more {
            page_users.last().map(|u| Bytes::from(format!("user:{}", u.id.value()).into_bytes()))
        } else {
            None
        };

        let wire_users: Vec<ws::User> = page_users
            .iter()
            .map(|u| {
                let slug = slug_resolver.resolve_user_slug(u.id).ok();
                domain_user_to_wire(u, slug)
            })
            .collect();

        log_ctx.set_success();
        Ok(w::ListUsersResponse { users: wire_users, next_page_token })
    }

    async fn search_users(
        &self,
        request: w::SearchUsersRequest,
        _ctx: RequestContext,
    ) -> Result<w::SearchUsersResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from("UserService", "search_users", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let filter = req.filter.ok_or_else(|| {
            log_ctx.set_error("InvalidArgument", "filter is required");
            WireError::new(ErrorCode::InvalidArgument, "filter is required")
        })?;

        // Currently only email search is implemented.
        let email = filter.email.ok_or_else(|| {
            log_ctx.set_error("InvalidArgument", "email filter is required");
            WireError::new(ErrorCode::InvalidArgument, "At least email filter must be provided")
        })?;

        let sys_svc = self.ctx.system_service();
        let user = sys_svc.search_users_by_email(&email).map_err(|e| {
            log_ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let users: Vec<ws::User> = user
            .into_iter()
            .map(|u| {
                let slug = slug_resolver.resolve_user_slug(u.id).ok();
                domain_user_to_wire(&u, slug)
            })
            .collect();

        log_ctx.set_success();
        Ok(w::SearchUsersResponse { users, next_page_token: None })
    }

    async fn create_user_email(
        &self,
        request: w::CreateUserEmailRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateUserEmailResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "create_user_email", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver
            .extract_and_resolve_user(
                req.user.as_ref().map(|s| inferadb_ledger_types::UserSlug::new(s.value())),
            )
            .inspect_err(|err| log_ctx.set_error("InvalidArgument", &err.message))?;

        validation::validate_email(&req.email).map_err(|e| {
            let msg = e.to_string();
            log_ctx.set_error("InvalidArgument", &msg);
            WireError::new(ErrorCode::InvalidArgument, msg)
        })?;

        if req.email_hmac.is_empty() {
            log_ctx.set_error("InvalidArgument", "email_hmac is required");
            return Err(WireError::new(ErrorCode::InvalidArgument, "email_hmac is required"));
        }

        // Look up user's region from GLOBAL directory for regional proposal.
        let sys_svc = self.ctx.system_service();
        let dir_entry = sys_svc.get_user_directory(user_id).map_err(|e| {
            log_ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;
        let dir_entry = dir_entry.ok_or_else(|| {
            log_ctx.set_error("NotFound", "User directory entry not found");
            WireError::new(ErrorCode::NotFound, "User directory entry not found")
        })?;
        let region = dir_entry.region.ok_or_else(|| {
            log_ctx.set_error("FailedPrecondition", "User has no region (erased?)");
            WireError::new(ErrorCode::FailedPrecondition, "User has no region assigned")
        })?;

        // Step 1 (GLOBAL): Register email HMAC — no plaintext PII in the GLOBAL Raft log.
        // Pre-check for idempotent retry: if the HMAC is already registered to
        // this user (from a previous partial attempt), skip the GLOBAL proposal.
        let existing_owner = sys_svc.get_email_hash(&req.email_hmac).map_err(|e| {
            log_ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;
        match existing_owner {
            Some(EmailHashEntry::Active(owner)) if owner == user_id => {
                // Idempotent retry — HMAC already registered to this user.
            },
            Some(_) => {
                log_ctx.set_error("AlreadyExists", "Email already registered to another user");
                return Err(WireError::new(
                    ErrorCode::AlreadyExists,
                    "Email already registered to another user",
                ));
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
                        &_ctx,
                        &mut log_ctx,
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
                            log_ctx.set_error("Internal", &e.to_string());
                            error_classify::storage_error(&e)
                        })?;
                        if owner != Some(EmailHashEntry::Active(user_id)) {
                            log_ctx.set_error(
                                "AlreadyExists",
                                "Email already registered to another user",
                            );
                            return Err(WireError::new(
                                ErrorCode::AlreadyExists,
                                "Email already registered to another user",
                            ));
                        }
                        // HMAC registered to this user (concurrent request) — safe to proceed.
                    },
                    LedgerResponse::Error { code, message } => {
                        log_ctx.set_error(code.grpc_code_name(), &message);
                        return Err(helpers::error_code_to_wire_error(
                            code,
                            format!("HMAC registration failed: {message}"),
                        ));
                    },
                    other => {
                        log_ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                        return Err(WireError::new(
                            ErrorCode::Internal,
                            "Unexpected response from HMAC registration",
                        ));
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
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::UserEmailCreated { email_id } => {
                log_ctx.record_event(
                    EventAction::UserEmailCreated,
                    EventOutcomeType::Success,
                    &[("user_id", &user_id.to_string()), ("email_id", &email_id.to_string())],
                );

                // Read back from REGIONAL state (email record lives in-region).
                let regional_state =
                    self.ctx.regional_state(region).map_err(tonic_status_to_wire_error)?;
                let regional_sys = self.ctx.regional_system_service(regional_state);
                let email = regional_sys
                    .get_user_email(email_id)
                    .map_err(|e| error_classify::storage_error(&e))?;

                log_ctx.set_success();
                Ok(w::CreateUserEmailResponse { email: email.as_ref().map(domain_email_to_wire) })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(
                    code,
                    format!("Email creation failed: {message}"),
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

    async fn delete_user_email(
        &self,
        request: w::DeleteUserEmailRequest,
        _ctx: RequestContext,
    ) -> Result<w::DeleteUserEmailResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "delete_user_email", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver
            .extract_and_resolve_user(
                req.user.as_ref().map(|s| inferadb_ledger_types::UserSlug::new(s.value())),
            )
            .inspect_err(|err| log_ctx.set_error("InvalidArgument", &err.message))?;

        let email_id = req.email_id.ok_or_else(|| {
            log_ctx.set_error("InvalidArgument", "email_id is required");
            WireError::new(ErrorCode::InvalidArgument, "email_id is required")
        })?;
        let domain_email_id = DomainUserEmailId::new(email_id.value());

        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::DeleteUserEmail { user_id, email_id: domain_email_id },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::UserEmailDeleted { .. } => {
                log_ctx.record_event(
                    EventAction::UserEmailDeleted,
                    EventOutcomeType::Success,
                    &[
                        ("user_id", &user_id.to_string()),
                        ("email_id", &domain_email_id.to_string()),
                    ],
                );

                log_ctx.set_success();
                let now_ns =
                    u64::try_from(Utc::now().timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0);
                Ok(w::DeleteUserEmailResponse { deleted_at: now_ns })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(
                    code,
                    format!("Email deletion failed: {message}"),
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

    async fn search_user_email(
        &self,
        request: w::SearchUserEmailRequest,
        _ctx: RequestContext,
    ) -> Result<w::SearchUserEmailResponse, WireError> {
        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "search_user_email", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let filter = req.filter.ok_or_else(|| {
            log_ctx.set_error("InvalidArgument", "filter is required");
            WireError::new(ErrorCode::InvalidArgument, "filter is required")
        })?;

        let sys_svc = self.ctx.system_service();
        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());

        // If user filter is set, list that user's emails.
        if let Some(ref user_slug) = filter.user {
            let user_id = slug_resolver
                .extract_and_resolve_user(Some(inferadb_ledger_types::UserSlug::new(
                    user_slug.value(),
                )))
                .inspect_err(|err| log_ctx.set_error("InvalidArgument", &err.message))?;

            let emails = sys_svc.get_user_emails(user_id).map_err(|e| {
                log_ctx.set_error("Internal", &e.to_string());
                error_classify::storage_error(&e)
            })?;

            log_ctx.set_success();
            return Ok(w::SearchUserEmailResponse {
                emails: emails.iter().map(domain_email_to_wire).collect(),
                next_page_token: None,
            });
        }

        // If email filter is set, search by email.
        if let Some(ref email) = filter.email {
            let user = sys_svc.search_users_by_email(email).map_err(|e| {
                log_ctx.set_error("Internal", &e.to_string());
                error_classify::storage_error(&e)
            })?;

            if let Some(user) = user {
                let emails = sys_svc
                    .get_user_emails(user.id)
                    .map_err(|e| error_classify::storage_error(&e))?;
                let matching: Vec<ws::UserEmail> = emails
                    .iter()
                    .filter(|e| e.email.eq_ignore_ascii_case(email))
                    .map(domain_email_to_wire)
                    .collect();

                log_ctx.set_success();
                return Ok(w::SearchUserEmailResponse { emails: matching, next_page_token: None });
            }
        }

        log_ctx.set_success();
        Ok(w::SearchUserEmailResponse { emails: vec![], next_page_token: None })
    }

    async fn verify_user_email(
        &self,
        request: w::VerifyUserEmailRequest,
        _ctx: RequestContext,
    ) -> Result<w::VerifyUserEmailResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "verify_user_email", &_ctx);
        let req = request;

        if req.token.is_empty() {
            log_ctx.set_error("InvalidArgument", "token must be non-empty");
            return Err(WireError::new(ErrorCode::InvalidArgument, "token must be non-empty"));
        }

        // Hash the plaintext token and look up the verification record.
        let sys_svc = self.ctx.system_service();
        let token_record = sys_svc.get_verification_token_by_hash(&req.token).map_err(|e| {
            log_ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;
        let token_record = token_record.ok_or_else(|| {
            log_ctx.set_error("NotFound", "Verification token not found or expired");
            WireError::new(ErrorCode::NotFound, "Verification token not found or expired")
        })?;

        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::VerifyUserEmail { email_id: token_record.email_id },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::UserEmailVerified { email_id } => {
                log_ctx.record_event(
                    EventAction::UserEmailVerified,
                    EventOutcomeType::Success,
                    &[("email_id", &email_id.to_string())],
                );

                let email = sys_svc
                    .get_user_email(email_id)
                    .map_err(|e| error_classify::storage_error(&e))?;

                log_ctx.set_success();
                Ok(w::VerifyUserEmailResponse { email: email.as_ref().map(domain_email_to_wire) })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(
                    code,
                    format!("Email verification failed: {message}"),
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

    async fn migrate_user_region(
        &self,
        request: w::MigrateUserRegionRequest,
        _ctx: RequestContext,
    ) -> Result<w::MigrateUserRegionResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "migrate_user_region", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_slug_val = req.slug.as_ref().map_or(0, |s| s.value());
        let typed_slug = req.slug.as_ref().map(|s| inferadb_ledger_types::UserSlug::new(s.value()));
        let user_id = slug_resolver
            .extract_and_resolve_user(typed_slug)
            .inspect_err(|err| log_ctx.set_error("InvalidArgument", &err.message))?;

        let target_region =
            inferadb_ledger_types::Region::from_str(&req.target_region).map_err(|err| {
                WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
            })?;

        let sys_svc = self.ctx.system_service();
        let dir_entry = sys_svc.get_user_directory(user_id).map_err(|e| {
            log_ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;
        let dir_entry = dir_entry.ok_or_else(|| {
            log_ctx.set_error("NotFound", "User directory entry not found");
            WireError::new(ErrorCode::NotFound, format!("User with slug {user_slug_val} not found"))
        })?;

        let source_region = dir_entry.region.unwrap_or(inferadb_ledger_types::Region::GLOBAL);

        if target_region == source_region {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                format!("User is already in region {}", source_region.as_str()),
            ));
        }

        if dir_entry.status != inferadb_ledger_state::system::UserDirectoryStatus::Active {
            let mut context = std::collections::BTreeMap::new();
            context.insert("status".to_string(), format!("{:?}", dir_entry.status));
            return Err(crate::services::wire_helpers::build_wire_error(
                ErrorCode::FailedPrecondition,
                format!("User is not Active (current status: {:?})", dir_entry.status),
                inferadb_ledger_types::DiagnosticCode::AppUserMigrating.as_u16().to_string(),
                true,
                0,
                context,
                inferadb_ledger_types::DiagnosticCode::AppUserMigrating.suggested_action(),
            ));
        }

        if target_region == inferadb_ledger_types::Region::GLOBAL {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "Cannot migrate to GLOBAL control plane region",
            ));
        }

        // Resolve `requires_residency` from the GLOBAL region directory;
        // an unknown region (no directory entry) is treated as
        // non-protected — production paths write the entry first.
        let target_requires_residency = match inferadb_ledger_state::system::lookup_region_residency(
            &self.ctx.state,
            target_region,
        ) {
            Ok(Some(r)) => r.requires_residency,
            Ok(None) | Err(_) => false,
        };
        if target_requires_residency {
            let nodes = sys_svc.list_nodes().map_err(|e| error_classify::storage_error(&e))?;
            let in_region_count = nodes.iter().filter(|n| n.region == target_region).count();
            if in_region_count < 3 {
                let mut context = std::collections::BTreeMap::new();
                context.insert("region".to_string(), target_region.as_str().to_string());
                context.insert("available_nodes".to_string(), in_region_count.to_string());
                context.insert("required_nodes".to_string(), "3".to_string());
                return Err(crate::services::wire_helpers::build_wire_error(
                    ErrorCode::FailedPrecondition,
                    format!(
                        "Insufficient in-region nodes for protected region {}: \
                         {in_region_count} available, 3 required",
                        target_region.as_str()
                    ),
                    inferadb_ledger_types::DiagnosticCode::AppInsufficientRegionNodes
                        .as_u16()
                        .to_string(),
                    false,
                    0,
                    context,
                    inferadb_ledger_types::DiagnosticCode::AppInsufficientRegionNodes
                        .suggested_action(),
                ));
            }
        }

        // Capture the originating W3C `traceparent` so the saga can propagate
        // it into downstream regional proposals.
        let traceparent = crate::services::wire_helpers::wire_context_traceparent(&_ctx);

        let saga_id = SagaId::new(uuid::Uuid::new_v4().to_string());
        let saga = MigrateUserSaga::new(
            saga_id,
            MigrateUserInput { user: user_id, source_region, target_region },
        )
        .with_traceparent(traceparent);

        let saga_key = format!("_meta:saga:{}", saga.id);
        let saga_wrapped = Saga::MigrateUser(saga);
        let saga_bytes = serde_json::to_vec(&saga_wrapped)
            .map_err(|e| error_classify::serialization_error(&e))?;

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
        };
        self.ctx
            .propose_system_request(
                SystemRequest::Write {
                    vault: DomainVaultId::new(0),
                    transactions: vec![saga_txn],
                    idempotency_key: [0; 16],
                    request_hash: 0,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        log_ctx.set_success();
        Ok(w::MigrateUserRegionResponse {
            slug: Some(ws::UserSlug::new(user_slug_val)),
            source_region: source_region.as_str().to_string(),
            target_region: target_region.as_str().to_string(),
            directory_status: "migrating".to_string(),
        })
    }

    async fn erase_user(
        &self,
        request: w::EraseUserRequest,
        _ctx: RequestContext,
    ) -> Result<w::EraseUserResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from("UserService", "erase_user", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let region = inferadb_ledger_types::Region::from_str(&req.region).map_err(|err| {
            WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
        })?;

        let slug_resolver = SlugResolver::new(self.ctx.applied_state.clone());
        let user_id = slug_resolver
            .extract_and_resolve_user(
                req.user.as_ref().map(|s| inferadb_ledger_types::UserSlug::new(s.value())),
            )
            .inspect_err(|err| log_ctx.set_error("InvalidArgument", &err.message))?;

        let response = self
            .ctx
            .propose_system_request(
                SystemRequest::EraseUser { user_id, region },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::UserErased { user_id: erased_id } => {
                log_ctx.record_event(
                    EventAction::UserErased,
                    EventOutcomeType::Success,
                    &[("user_id", &erased_id.to_string()), ("region", region.as_str())],
                );

                log_ctx.set_success();
                let erased_slug = slug_resolver
                    .resolve_user_slug(erased_id)
                    .inspect_err(|err| log_ctx.set_error("InternalError", &err.message))?;
                Ok(w::EraseUserResponse { user: Some(ws::UserSlug::new(erased_slug.value())) })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, format!("Erasure failed: {message}")))
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

    async fn initiate_email_verification(
        &self,
        request: w::InitiateEmailVerificationRequest,
        _ctx: RequestContext,
    ) -> Result<w::InitiateEmailVerificationResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "initiate_email_verification", &_ctx);
        let req = request;

        // Validate inputs.
        validation::validate_email(&req.email).map_err(|e| {
            let msg = e.to_string();
            log_ctx.set_error("InvalidArgument", &msg);
            WireError::new(ErrorCode::InvalidArgument, msg)
        })?;
        let region = inferadb_ledger_types::Region::from_str(&req.region).map_err(|err| {
            WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
        })?;

        // Require email blinding key.
        let blinding_key = self.ctx.email_blinding_key.as_ref().ok_or_else(|| {
            log_ctx.set_error("FailedPrecondition", "email blinding key not configured");
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Onboarding is not enabled: email blinding key not configured",
            )
        })?;

        // Compute HMAC and generate verification code.
        let email_hmac = inferadb_ledger_types::compute_email_hmac(blinding_key, &req.email);
        let (code, code_hash) =
            inferadb_ledger_types::email_hash::generate_verification_code(blinding_key);
        let expires_at = Utc::now() + inferadb_ledger_types::onboarding::CODE_TTL;

        // Propose to REGIONAL Raft (code hash is not PII, email excluded from log).
        let system_request =
            SystemRequest::CreateEmailVerification { email_hmac, code_hash, region, expires_at };
        let response =
            self.ctx.propose_regional(region, system_request, &_ctx, &mut log_ctx).await?;

        match response {
            LedgerResponse::EmailVerificationCreated => {},
            LedgerResponse::Error { code: err_code, message } => {
                log_ctx.set_error(err_code.grpc_code_name(), &message);
                return Err(helpers::error_code_to_wire_error(err_code, message));
            },
            other => {
                log_ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                return Err(WireError::new(
                    ErrorCode::Internal,
                    "Unexpected response from Raft state machine",
                ));
            },
        }

        inferadb_ledger_raft::metrics::record_onboarding_request("initiate", "success");
        Ok(w::InitiateEmailVerificationResponse { code })
    }

    async fn verify_email_code(
        &self,
        request: w::VerifyEmailCodeRequest,
        _ctx: RequestContext,
    ) -> Result<w::VerifyEmailCodeResponse, WireError> {
        use inferadb_ledger_raft::types::EmailCodeVerifiedResult;

        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "verify_email_code", &_ctx);
        let req = request;

        // Validate inputs.
        validation::validate_email(&req.email).map_err(|e| {
            let msg = e.to_string();
            log_ctx.set_error("InvalidArgument", &msg);
            WireError::new(ErrorCode::InvalidArgument, msg)
        })?;
        if req.code.is_empty() {
            log_ctx.set_error("InvalidArgument", "code is required");
            return Err(WireError::new(ErrorCode::InvalidArgument, "code is required"));
        }
        let region = inferadb_ledger_types::Region::from_str(&req.region).map_err(|err| {
            WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
        })?;

        // Require email blinding key.
        let blinding_key = self.ctx.email_blinding_key.as_ref().ok_or_else(|| {
            log_ctx.set_error("FailedPrecondition", "email blinding key not configured");
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Onboarding is not enabled: email blinding key not configured",
            )
        })?;

        // Compute HMAC and code hash.
        let email_hmac = inferadb_ledger_types::compute_email_hmac(blinding_key, &req.email);
        let code_hash =
            inferadb_ledger_types::email_hash::compute_code_hash(blinding_key, &req.code);

        // Pre-resolve: check GLOBAL HMAC index for existing *active* user.
        // Provisioning entries (in-flight onboarding sagas) are not existing users.
        let sys_svc = self.ctx.system_service();
        let email_hash_entry =
            sys_svc.get_email_hash(&email_hmac).map_err(|e| error_classify::storage_error(&e))?;
        let existing_user_hmac_hit = matches!(&email_hash_entry, Some(EmailHashEntry::Active(_)));

        // TOTP pre-resolve: if this is an existing active user, check whether
        // they have a TOTP credential in their region. If so, pre-generate
        // challenge data so the state machine can atomically create it.
        let totp_pre_resolve = if let Some(EmailHashEntry::Active(user_id)) = &email_hash_entry {
            let dir_entry = sys_svc
                .get_user_directory(*user_id)
                .map_err(|e| error_classify::storage_error(&e))?;
            if let Some(ref dir) = dir_entry {
                if let (Some(user_region), Some(slug)) = (dir.region, dir.slug) {
                    let regional_state =
                        self.ctx.regional_state(user_region).map_err(tonic_status_to_wire_error)?;
                    let regional_sys = self.ctx.regional_system_service(regional_state);
                    let totp_creds = regional_sys
                        .list_user_credentials(
                            *user_id,
                            Some(inferadb_ledger_types::CredentialType::Totp),
                        )
                        .map_err(|e| error_classify::storage_error(&e))?;
                    if totp_creds.iter().any(|c| c.enabled) {
                        let mut nonce = [0u8; 32];
                        rand::Rng::fill_bytes(&mut rand::rng(), &mut nonce);
                        let expires_at = Utc::now() + chrono::Duration::minutes(5);
                        Some(inferadb_ledger_raft::types::TotpPreResolve {
                            nonce,
                            expires_at,
                            user_id: *user_id,
                            user_slug: slug,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Pre-generate onboarding token for new-user path.
        let (onboarding_token, onboarding_token_hash) =
            inferadb_ledger_types::onboarding::generate_onboarding_token();
        let onboarding_expires_at = Utc::now() + inferadb_ledger_types::onboarding::ONBOARDING_TTL;

        // Propose to REGIONAL Raft (verification region).
        let system_request = SystemRequest::VerifyEmailCode {
            email_hmac: email_hmac.clone(),
            code_hash,
            region,
            existing_user_hmac_hit,
            onboarding_token_hash,
            onboarding_expires_at,
            totp: totp_pre_resolve,
        };
        let response =
            self.ctx.propose_regional(region, system_request, &_ctx, &mut log_ctx).await?;

        // Branch on result.
        let verified = match response {
            LedgerResponse::EmailCodeVerified { result } => result,
            LedgerResponse::Error { code: err_code, message } => {
                log_ctx.set_error(err_code.grpc_code_name(), &message);
                return Err(helpers::error_code_to_wire_error(err_code, message));
            },
            other => {
                log_ctx.set_error("UnexpectedResponse", &format!("{other:?}"));
                return Err(WireError::new(
                    ErrorCode::Internal,
                    "Unexpected response from Raft state machine",
                ));
            },
        };

        match verified {
            EmailCodeVerifiedResult::ExistingUser => {
                // Re-read UserDirectoryEntry from GLOBAL to find actual region.
                let hash_entry = sys_svc
                    .get_email_hash(&email_hmac)
                    .map_err(|e| error_classify::storage_error(&e))?
                    .ok_or_else(|| {
                        WireError::new(
                            ErrorCode::Internal,
                            "Email hash vanished after verification",
                        )
                    })?;

                let user_id = match hash_entry {
                    EmailHashEntry::Active(uid) => uid,
                    EmailHashEntry::Provisioning(_) => {
                        return Err(WireError::new(
                            ErrorCode::Internal,
                            "Email in provisioning state after ExistingUser result",
                        ));
                    },
                };

                let dir_entry = sys_svc
                    .get_user_directory(user_id)
                    .map_err(|e| error_classify::storage_error(&e))?
                    .ok_or_else(|| {
                        WireError::new(ErrorCode::Internal, "User directory entry not found")
                    })?;

                let user_region = dir_entry.region.ok_or_else(|| {
                    WireError::new(ErrorCode::Internal, "User directory entry has no region")
                })?;
                let user_slug = dir_entry.slug.ok_or_else(|| {
                    WireError::new(ErrorCode::Internal, "User directory entry has no slug")
                })?;

                // Read user from actual region.
                let regional_state =
                    self.ctx.regional_state(user_region).map_err(tonic_status_to_wire_error)?;
                let regional_sys = self.ctx.regional_system_service(regional_state);
                let user = regional_sys
                    .get_user(user_id)
                    .map_err(|e| error_classify::storage_error(&e))?
                    .ok_or_else(|| {
                        WireError::new(ErrorCode::PermissionDenied, "Invalid verification code")
                    })?;

                // Check status — non-Active users get the same error as invalid code
                // to prevent information leakage about account state.
                if user.status != inferadb_ledger_types::UserStatus::Active {
                    return Err(WireError::new(
                        ErrorCode::PermissionDenied,
                        "Invalid verification code",
                    ));
                }

                let session = self
                    .create_user_session(
                        &sys_svc,
                        user_slug,
                        user.role,
                        user.version,
                        user_region,
                        &_ctx,
                        &mut log_ctx,
                    )
                    .await
                    .map_err(tonic_status_to_wire_error)?;

                inferadb_ledger_raft::metrics::record_onboarding_request("verify", "success");
                log_ctx.set_success();
                let access_expires_ns =
                    u64::try_from(session.access_expires_at.timestamp_nanos_opt().unwrap_or(0))
                        .unwrap_or(0);
                let refresh_expires_ns =
                    u64::try_from(session.refresh_expires_at.timestamp_nanos_opt().unwrap_or(0))
                        .unwrap_or(0);
                Ok(w::VerifyEmailCodeResponse {
                    result: Some(w::VerifyEmailCodeResult::ExistingUser(w::ExistingUserSession {
                        user: Some(ws::UserSlug::new(user_slug.value())),
                        session: Some(ws::TokenPair {
                            access_token: session.access_token,
                            refresh_token: session.refresh_token,
                            access_expires_at: access_expires_ns,
                            refresh_expires_at: refresh_expires_ns,
                        }),
                    })),
                })
            },
            EmailCodeVerifiedResult::TotpRequired { nonce } => {
                inferadb_ledger_raft::metrics::record_onboarding_request("verify", "totp_required");
                log_ctx.set_success();
                Ok(w::VerifyEmailCodeResponse {
                    result: Some(w::VerifyEmailCodeResult::TotpRequired(ws::TotpRequired {
                        challenge_nonce: Bytes::from(nonce.to_vec()),
                    })),
                })
            },
            EmailCodeVerifiedResult::NewUser => {
                inferadb_ledger_raft::metrics::record_onboarding_request("verify", "success");
                log_ctx.set_success();
                Ok(w::VerifyEmailCodeResponse {
                    result: Some(w::VerifyEmailCodeResult::NewUser(w::OnboardingSession {
                        onboarding_token,
                    })),
                })
            },
        }
    }

    async fn complete_registration(
        &self,
        request: w::CompleteRegistrationRequest,
        _ctx: RequestContext,
    ) -> Result<w::CompleteRegistrationResponse, WireError> {
        use inferadb_ledger_raft::{OnboardingPii, SagaSubmission};
        use inferadb_ledger_state::system::{CreateOnboardingUserInput, CreateOnboardingUserSaga};
        use inferadb_ledger_types::{
            TokenVersion,
            onboarding::SAGA_COMPLETION_TIMEOUT,
            snowflake::{generate_organization_slug, generate_user_slug},
        };

        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        // CompleteRegistration submits a saga to the GLOBAL leader's saga
        // orchestrator. The saga caches PII in-memory on the node that
        // received the submission. If this node is not the GLOBAL leader,
        // the PII would be cached on the wrong node and the saga would
        // fail with PiiLost. Reject early with NotLeader so the client
        // retries on the GLOBAL leader.
        if let Some(ref manager) = self.ctx.manager
            && let Ok(global) = manager.system_region()
            && !global.handle().is_leader()
        {
            return Err(tonic_status_to_wire_error(
                super::metadata::not_leader_status_from_handle(
                    global.handle(),
                    Some(manager.peer_addresses()),
                    "Not the GLOBAL leader — registration saga must run on the leader",
                    None,
                    None,
                    None,
                ),
            ));
        }

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "complete_registration", &_ctx);
        let req = request;

        // Validate inputs.
        validation::validate_email(&req.email).map_err(|e| {
            let msg = e.to_string();
            log_ctx.set_error("InvalidArgument", &msg);
            WireError::new(ErrorCode::InvalidArgument, msg)
        })?;
        validation::validate_user_name(&req.name).map_err(|e| {
            let msg = e.to_string();
            log_ctx.set_error("InvalidArgument", &msg);
            WireError::new(ErrorCode::InvalidArgument, msg)
        })?;
        validation::validate_organization_name(&req.organization_name, &self.ctx.validation_config)
            .map_err(|e| {
                let msg = e.to_string();
                log_ctx.set_error("InvalidArgument", &msg);
                WireError::new(ErrorCode::InvalidArgument, msg)
            })?;
        if req.onboarding_token.is_empty() {
            log_ctx.set_error("InvalidArgument", "onboarding_token is required");
            return Err(WireError::new(ErrorCode::InvalidArgument, "onboarding_token is required"));
        }
        let region = inferadb_ledger_types::Region::from_str(&req.region).map_err(|err| {
            WireError::new(ErrorCode::InvalidArgument, format!("invalid region: {err}"))
        })?;

        // Require email blinding key.
        let blinding_key = self.ctx.email_blinding_key.as_ref().ok_or_else(|| {
            log_ctx.set_error("FailedPrecondition", "email blinding key not configured");
            WireError::new(
                ErrorCode::FailedPrecondition,
                "Onboarding is not enabled: email blinding key not configured",
            )
        })?;

        // Compute email HMAC.
        let email_hmac = inferadb_ledger_types::compute_email_hmac(blinding_key, &req.email);

        // Decode token → raw bytes → SHA-256(raw_bytes).
        let raw_bytes =
            inferadb_ledger_types::onboarding::decode_onboarding_token(&req.onboarding_token)
                .map_err(|e| {
                    log_ctx.set_error("InvalidArgument", &format!("Invalid onboarding token: {e}"));
                    WireError::new(
                        ErrorCode::InvalidArgument,
                        format!("Invalid onboarding token: {e}"),
                    )
                })?;
        let token_hash: [u8; 32] = {
            use sha2::Digest as _;
            sha2::Sha256::digest(raw_bytes).into()
        };

        // Idempotency check: read GLOBAL HMAC index.
        let sys_svc = self.ctx.system_service();
        if let Some(hash_entry) =
            sys_svc.get_email_hash(&email_hmac).map_err(|e| error_classify::storage_error(&e))?
        {
            match hash_entry {
                EmailHashEntry::Active(user_id) => {
                    // Return fresh session for idempotent re-registration.
                    let dir_entry = sys_svc
                        .get_user_directory(user_id)
                        .map_err(|e| error_classify::storage_error(&e))?
                        .ok_or_else(|| {
                            WireError::new(ErrorCode::Internal, "User directory entry not found")
                        })?;

                    let user_slug = dir_entry.slug.ok_or_else(|| {
                        WireError::new(ErrorCode::Internal, "User directory has no slug")
                    })?;
                    let user_region = dir_entry.region.ok_or_else(|| {
                        WireError::new(ErrorCode::Internal, "User directory has no region")
                    })?;

                    // Read user from actual region to get role + version.
                    let regional_state =
                        self.ctx.regional_state(user_region).map_err(tonic_status_to_wire_error)?;
                    let regional_sys = self.ctx.regional_system_service(regional_state);
                    let user = regional_sys
                        .get_user(user_id)
                        .map_err(|e| error_classify::storage_error(&e))?
                        .ok_or_else(|| {
                            WireError::new(ErrorCode::Internal, "User record not found in region")
                        })?;

                    let session = self
                        .create_user_session(
                            &sys_svc,
                            user_slug,
                            user.role,
                            user.version,
                            user_region,
                            &_ctx,
                            &mut log_ctx,
                        )
                        .await
                        .map_err(tonic_status_to_wire_error)?;

                    let access_expires_ns =
                        u64::try_from(session.access_expires_at.timestamp_nanos_opt().unwrap_or(0))
                            .unwrap_or(0);
                    let refresh_expires_ns = u64::try_from(
                        session.refresh_expires_at.timestamp_nanos_opt().unwrap_or(0),
                    )
                    .unwrap_or(0);
                    let token_pair = ws::TokenPair {
                        access_token: session.access_token,
                        refresh_token: session.refresh_token,
                        access_expires_at: access_expires_ns,
                        refresh_expires_at: refresh_expires_ns,
                    };

                    return Ok(w::CompleteRegistrationResponse {
                        user: Some(ws::User {
                            id: None,
                            name: String::new(),
                            email: None,
                            status: ws::UserStatus::Unspecified,
                            created_at: 0,
                            updated_at: 0,
                            role: ws::UserRole::Unspecified,
                            slug: Some(ws::UserSlug::new(user_slug.value())),
                            deleted_at: None,
                        }),
                        session: Some(token_pair),
                        organization: None,
                    });
                },
                EmailHashEntry::Provisioning(_) => {
                    return Err(WireError::new(
                        ErrorCode::AlreadyExists,
                        "Registration already in progress for this email",
                    ));
                },
            }
        }

        // Read regional onboarding account and validate token hash.
        // Retry briefly: in multi-node clusters the VerifyEmailCode write may have
        // been forwarded to the data region leader. Raft replication to this follower
        // node can lag by a few milliseconds; poll until the entry is applied locally.
        let regional_state = self.ctx.regional_state(region).map_err(tonic_status_to_wire_error)?;
        let regional_sys = self.ctx.regional_system_service(regional_state);
        let account = {
            let mut found = None;
            for attempt in 0..20u32 {
                match regional_sys.get_onboarding_account_by_hmac(&email_hmac) {
                    Ok(Some(a)) => {
                        found = Some(a);
                        break;
                    },
                    Ok(None) if attempt < 19 => {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    },
                    Ok(None) => {},
                    Err(e) => {
                        return Err(error_classify::storage_error(&e));
                    },
                }
            }
            found.ok_or_else(|| {
                tonic_status_to_wire_error(auth_errors::unified_auth_error(
                    AuthFailureReason::OnboardingRecordMissing,
                ))
            })?
        };

        // Validate token hash (constant-time comparison).
        // Unified with "record missing" and "expired" so probes cannot
        // distinguish "this email has a pending onboarding" from "wrong token"
        // from "expired" — all three return identical responses.
        if !inferadb_ledger_types::hash_eq(&token_hash, &account.token_hash) {
            return Err(tonic_status_to_wire_error(auth_errors::unified_auth_error(
                AuthFailureReason::OnboardingTokenInvalid,
            )));
        }

        // Check expiration.
        if Utc::now() > account.expires_at {
            return Err(tonic_status_to_wire_error(auth_errors::unified_auth_error(
                AuthFailureReason::OnboardingTokenInvalid,
            )));
        }

        // Generate slugs for the new user and organization.
        let user_slug = generate_user_slug()
            .map_err(|e| error_classify::internal_error("id-generation", &e))?;
        let organization_slug = generate_organization_slug()
            .map_err(|e| error_classify::internal_error("id-generation", &e))?;

        // Build saga.
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

        // Create oneshot for saga completion notification.
        let (notify_tx, notify_rx) = tokio::sync::oneshot::channel();

        // Get saga handle.
        let saga_handle = self.ctx.saga_handle.get().ok_or_else(|| {
            WireError::new(
                ErrorCode::StaleRouting,
                "Saga orchestrator not ready — try again shortly",
            )
        })?;

        // Capture the originating W3C `traceparent` so the saga can propagate
        // it into downstream regional proposals.
        let traceparent = crate::services::wire_helpers::wire_context_traceparent(&_ctx);

        // Submit saga.
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
                traceparent,
            })
            .await
            .map_err(|e| {
                WireError::new(ErrorCode::StaleRouting, format!("Failed to submit saga: {e}"))
            })?;

        // Await completion with timeout.
        let saga_result = tokio::time::timeout(SAGA_COMPLETION_TIMEOUT, notify_rx)
            .await
            .map_err(|_| {
                WireError::new(
                    ErrorCode::FailedPrecondition,
                    "Registration saga timed out — it may still complete",
                )
            })?
            .map_err(|_| {
                WireError::new(
                    ErrorCode::Internal,
                    "Saga orchestrator dropped notification channel",
                )
            })?
            .map_err(|e| error_classify::raft_error(&e))?;

        // Sign JWT for the new user session.
        let signing_key =
            self.ensure_signing_key_cached(&sys_svc).map_err(tonic_status_to_wire_error)?;
        let jwt_engine = self.ctx.jwt_engine.as_ref().ok_or_else(|| {
            WireError::new(ErrorCode::FailedPrecondition, "JWT engine not configured")
        })?;
        let (access_token, access_expires_at) = jwt_engine
            .sign_user_session(
                saga_result.user_slug,
                "user",
                TokenVersion::new(1),
                &signing_key.kid,
            )
            .map_err(|e| error_classify::crypto_error(&e))?;

        let access_expires_ns =
            u64::try_from(access_expires_at.timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0);
        let refresh_expires_ns =
            u64::try_from(saga_result.refresh_expires_at.timestamp_nanos_opt().unwrap_or(0))
                .unwrap_or(0);
        let token_pair = ws::TokenPair {
            access_token,
            refresh_token: saga_result.refresh_token,
            access_expires_at: access_expires_ns,
            refresh_expires_at: refresh_expires_ns,
        };

        // Build response user.
        let response_user = ws::User {
            id: None,
            name: String::new(),
            email: None,
            status: ws::UserStatus::Unspecified,
            created_at: 0,
            updated_at: 0,
            role: ws::UserRole::Unspecified,
            slug: Some(ws::UserSlug::new(saga_result.user_slug.value())),
            deleted_at: None,
        };

        inferadb_ledger_raft::metrics::record_onboarding_request("register", "success");
        log_ctx.set_success();
        Ok(w::CompleteRegistrationResponse {
            user: Some(response_user),
            session: Some(token_pair),
            organization: Some(ws::OrganizationSlug::new(saga_result.organization_slug.value())),
        })
    }

    async fn create_user_credential(
        &self,
        request: w::CreateUserCredentialRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateUserCredentialResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "create_user_credential", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        // Resolve user slug → internal ID + region. Wire `UserSlug` is the
        // same `inferadb_ledger_types::UserSlug` newtype — pass through directly.
        let (user_id, user_slug, region) =
            self.resolve_user_region(req.user, &mut log_ctx).map_err(tonic_status_to_wire_error)?;

        // Verify user is active (defense-in-depth: credentials for deleted users are useless).
        let regional_state = self.ctx.regional_state(region).map_err(tonic_status_to_wire_error)?;
        let regional_sys = self.ctx.regional_system_service(regional_state);
        let user = regional_sys.get_user(user_id).map_err(|e| {
            log_ctx.set_error("Internal", &e.to_string());
            error_classify::storage_error(&e)
        })?;
        if !matches!(
            user.as_ref().map(|u| u.status),
            Some(inferadb_ledger_types::UserStatus::Active)
        ) {
            log_ctx.set_error("FailedPrecondition", "User is not active");
            return Err(WireError::new(ErrorCode::FailedPrecondition, "User is not active"));
        }

        // Validate credential type.
        let credential_type: inferadb_ledger_types::CredentialType = match req.credential_type {
            ws::CredentialType::Passkey => inferadb_ledger_types::CredentialType::Passkey,
            ws::CredentialType::Totp => inferadb_ledger_types::CredentialType::Totp,
            ws::CredentialType::RecoveryCode => inferadb_ledger_types::CredentialType::RecoveryCode,
            _ => {
                return Err(WireError::new(
                    ErrorCode::InvalidArgument,
                    "credential_type must be specified",
                ));
            },
        };

        // Validate name.
        validation::validate_user_name(&req.name).map_err(|e| {
            let msg = e.to_string();
            log_ctx.set_error("InvalidArgument", &msg);
            WireError::new(ErrorCode::InvalidArgument, msg)
        })?;

        // Convert wire credential data → domain via the wire-native
        // validators in this module (no proto round-trip).
        let credential_data = match req.data {
            Some(w::CreateUserCredentialData::Passkey(pk)) => {
                inferadb_ledger_types::CredentialData::Passkey(wire_to_domain_passkey(pk)?)
            },
            Some(w::CreateUserCredentialData::Totp(totp)) => {
                inferadb_ledger_types::CredentialData::Totp(wire_to_domain_totp(totp)?)
            },
            Some(w::CreateUserCredentialData::RecoveryCode(rc)) => {
                inferadb_ledger_types::CredentialData::RecoveryCode(wire_to_domain_recovery_code(
                    rc,
                )?)
            },
            None => {
                log_ctx.set_error("InvalidArgument", "credential data is required");
                return Err(WireError::new(
                    ErrorCode::InvalidArgument,
                    "credential data is required",
                ));
            },
        };

        // Verify credential_type matches data discriminant.
        let data_type = match &credential_data {
            inferadb_ledger_types::CredentialData::Passkey(_) => {
                inferadb_ledger_types::CredentialType::Passkey
            },
            inferadb_ledger_types::CredentialData::Totp(_) => {
                inferadb_ledger_types::CredentialType::Totp
            },
            inferadb_ledger_types::CredentialData::RecoveryCode(_) => {
                inferadb_ledger_types::CredentialType::RecoveryCode
            },
        };
        if credential_type != data_type {
            log_ctx.set_error("InvalidArgument", "credential_type does not match data");
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "credential_type does not match the provided credential data",
            ));
        }

        // Propose encrypted credential creation.
        let system_request = SystemRequest::CreateUserCredential {
            user_id,
            credential_type,
            credential_data: credential_data.clone(),
            name: req.name.clone(),
        };
        let response = self
            .ctx
            .propose_regional_encrypted(region, system_request, user_id, &_ctx, &mut log_ctx)
            .await?;

        match response {
            LedgerResponse::UserCredentialCreated { credential_id } => {
                // Build proto response from input data + allocated ID.
                let domain_cred = inferadb_ledger_types::UserCredential {
                    id: credential_id,
                    user: user_id,
                    credential_type,
                    credential_data,
                    name: req.name,
                    enabled: true,
                    created_at: Utc::now(),
                    last_used_at: None,
                };
                // For create response: include TOTP secret (one-time setup).
                // All other responses strip it (`strip_secrets = true`).
                let wire_cred = domain_user_credential_to_wire(&domain_cred, user_slug, false);

                log_ctx.set_success();
                Ok(w::CreateUserCredentialResponse { credential: Some(wire_cred) })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
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

    async fn list_user_credentials(
        &self,
        request: w::ListUserCredentialsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListUserCredentialsResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "list_user_credentials", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let (user_id, user_slug, region) =
            self.resolve_user_region(req.user, &mut log_ctx).map_err(tonic_status_to_wire_error)?;

        // Optional type filter.
        let type_filter: Option<inferadb_ledger_types::CredentialType> =
            req.credential_type.and_then(|ct| match ct {
                ws::CredentialType::Passkey => Some(inferadb_ledger_types::CredentialType::Passkey),
                ws::CredentialType::Totp => Some(inferadb_ledger_types::CredentialType::Totp),
                ws::CredentialType::RecoveryCode => {
                    Some(inferadb_ledger_types::CredentialType::RecoveryCode)
                },
                _ => None,
            });

        // Read credentials from regional state.
        let regional_state = self.ctx.regional_state(region).map_err(tonic_status_to_wire_error)?;
        let regional_sys = self.ctx.regional_system_service(regional_state);
        let credentials =
            regional_sys.list_user_credentials(user_id, type_filter).map_err(|e| {
                log_ctx.set_error("Internal", &e.to_string());
                error_classify::storage_error(&e)
            })?;

        // Strip sensitive credential data (TOTP secret, recovery code hashes)
        // before converting to wire — list responses must not expose them.
        let wire_creds: Vec<w::UserCredential> = credentials
            .iter()
            .map(|c| domain_user_credential_to_wire(c, user_slug, true))
            .collect();

        log_ctx.set_success();
        Ok(w::ListUserCredentialsResponse { credentials: wire_creds })
    }

    async fn update_user_credential(
        &self,
        request: w::UpdateUserCredentialRequest,
        _ctx: RequestContext,
    ) -> Result<w::UpdateUserCredentialResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "update_user_credential", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let (user_id, user_slug, region) =
            self.resolve_user_region(req.user, &mut log_ctx).map_err(tonic_status_to_wire_error)?;

        let credential_id = inferadb_ledger_types::UserCredentialId::new(req.credential_id);

        // Validate name if provided.
        if let Some(ref name) = req.name {
            validation::validate_user_name(name).map_err(|e| {
                let msg = e.to_string();
                log_ctx.set_error("InvalidArgument", &msg);
                WireError::new(ErrorCode::InvalidArgument, msg)
            })?;
        }

        // Convert passkey update data if provided (wire-native validator).
        let passkey_update =
            if let Some(pk) = req.passkey { Some(wire_to_domain_passkey(pk)?) } else { None };

        let system_request = SystemRequest::UpdateUserCredential {
            user_id,
            credential_id,
            name: req.name,
            enabled: req.enabled,
            passkey_update,
        };
        let response = self
            .ctx
            .propose_regional_encrypted(region, system_request, user_id, &_ctx, &mut log_ctx)
            .await?;

        match response {
            LedgerResponse::UserCredentialUpdated { credential_id: updated_id } => {
                // Read back the updated credential.
                let regional_state =
                    self.ctx.regional_state(region).map_err(tonic_status_to_wire_error)?;
                let regional_sys = self.ctx.regional_system_service(regional_state);
                let cred = regional_sys
                    .get_user_credential(user_id, updated_id)
                    .map_err(|e| error_classify::storage_error(&e))?
                    .ok_or_else(|| {
                        WireError::new(ErrorCode::Internal, "Credential vanished after update")
                    })?;

                // Update responses redact sensitive credential data
                // (TOTP secret + recovery code hashes are write-once).
                let wire_cred = domain_user_credential_to_wire(&cred, user_slug, true);

                log_ctx.set_success();
                Ok(w::UpdateUserCredentialResponse { credential: Some(wire_cred) })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
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

    async fn delete_user_credential(
        &self,
        request: w::DeleteUserCredentialRequest,
        _ctx: RequestContext,
    ) -> Result<w::DeleteUserCredentialResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "delete_user_credential", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let (user_id, _user_slug, region) =
            self.resolve_user_region(req.user, &mut log_ctx).map_err(tonic_status_to_wire_error)?;

        let credential_id = inferadb_ledger_types::UserCredentialId::new(req.credential_id);

        let system_request = SystemRequest::DeleteUserCredential { user_id, credential_id };
        let response = self
            .ctx
            .propose_regional_encrypted(region, system_request, user_id, &_ctx, &mut log_ctx)
            .await?;

        match response {
            LedgerResponse::UserCredentialDeleted { .. } => {
                log_ctx.set_success();
                Ok(w::DeleteUserCredentialResponse {})
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
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

    async fn create_totp_challenge(
        &self,
        request: w::CreateTotpChallengeRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateTotpChallengeResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "create_totp_challenge", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let (user_id, user_slug, region) =
            self.resolve_user_region(req.user, &mut log_ctx).map_err(tonic_status_to_wire_error)?;

        // Validate primary_method.
        let primary_method = match req.primary_method.as_str() {
            "passkey" => inferadb_ledger_types::PrimaryAuthMethod::Passkey,
            "email_code" => inferadb_ledger_types::PrimaryAuthMethod::EmailCode,
            _ => {
                log_ctx.set_error("InvalidArgument", "invalid primary_method");
                return Err(WireError::new(
                    ErrorCode::InvalidArgument,
                    "primary_method must be \"passkey\" or \"email_code\"",
                ));
            },
        };

        // Generate challenge nonce and expiry.
        let mut nonce = [0u8; 32];
        rand::Rng::fill_bytes(&mut rand::rng(), &mut nonce);
        let expires_at = Utc::now() + chrono::Duration::minutes(5);

        // Plain proposal (no PII — only IDs and nonces).
        let system_request = SystemRequest::CreateTotpChallenge {
            user_id,
            user_slug,
            nonce,
            expires_at,
            primary_method,
        };
        let response =
            self.ctx.propose_regional(region, system_request, &_ctx, &mut log_ctx).await?;

        match response {
            LedgerResponse::TotpChallengeCreated { nonce: created_nonce } => {
                log_ctx.set_success();
                Ok(w::CreateTotpChallengeResponse {
                    challenge_nonce: Bytes::from(created_nonce.to_vec()),
                })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
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

    async fn verify_totp(
        &self,
        request: w::VerifyTotpRequest,
        _ctx: RequestContext,
    ) -> Result<w::VerifyTotpResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from("UserService", "verify_totp", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let (user_id, _user_slug, region) =
            self.resolve_user_region(req.user, &mut log_ctx).map_err(tonic_status_to_wire_error)?;

        // Parse challenge nonce.
        let nonce: [u8; 32] = req.challenge_nonce.as_ref().try_into().map_err(|_| {
            log_ctx.set_error("InvalidArgument", "challenge_nonce must be 32 bytes");
            WireError::new(ErrorCode::InvalidArgument, "challenge_nonce must be exactly 32 bytes")
        })?;

        // Validate TOTP code format (must be 6 or 8 digits).
        if req.totp_code.is_empty()
            || !req.totp_code.bytes().all(|b| b.is_ascii_digit())
            || (req.totp_code.len() != 6 && req.totp_code.len() != 8)
        {
            log_ctx.set_error("InvalidArgument", "invalid TOTP code format");
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "TOTP code must be 6 or 8 digits",
            ));
        }

        // Read challenge from regional state.
        let regional_state = self.ctx.regional_state(region).map_err(tonic_status_to_wire_error)?;
        let regional_sys = self.ctx.regional_system_service(regional_state);

        let challenge = regional_sys
            .get_totp_challenge(user_id, &nonce)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                log_ctx.set_error("NotFound", "Challenge not found");
                WireError::new(ErrorCode::NotFound, "Challenge not found")
            })?;

        // Service-layer expiry check (defense-in-depth; state machine checks deterministically).
        if Utc::now() >= challenge.expires_at {
            log_ctx.set_error("FailedPrecondition", "Challenge expired");
            return Err(WireError::new(ErrorCode::FailedPrecondition, "Challenge expired"));
        }

        // Service-layer attempt check (defense-in-depth; state machine enforces independently).
        if challenge.attempts >= 3 {
            log_ctx.set_error("ResourceExhausted", "Too many attempts");
            return Err(WireError::new(ErrorCode::RateLimited, "Too many attempts"));
        }

        // Read TOTP credential from regional state.
        let totp_creds = regional_sys
            .list_user_credentials(user_id, Some(inferadb_ledger_types::CredentialType::Totp))
            .map_err(|e| error_classify::storage_error(&e))?;

        let totp_cred = totp_creds.first().ok_or_else(|| {
            log_ctx.set_error("FailedPrecondition", "No TOTP credential configured");
            WireError::new(
                ErrorCode::FailedPrecondition,
                "No TOTP credential configured for this user",
            )
        })?;

        let totp_data = match &totp_cred.credential_data {
            inferadb_ledger_types::CredentialData::Totp(t) => t,
            _ => {
                return Err(WireError::new(ErrorCode::Internal, "Credential type mismatch"));
            },
        };

        // Service-layer TOTP verification (non-deterministic, leader only).
        let code_valid = UserService::verify_totp_code(
            &totp_data.secret,
            &req.totp_code,
            totp_data.algorithm,
            totp_data.digits,
            totp_data.period,
        );

        if !code_valid {
            // Persist failed attempt via Raft (survives leader failover).
            let increment_request = SystemRequest::IncrementTotpAttempt { user_id, nonce };
            if let Err(e) =
                self.ctx.propose_regional(region, increment_request, &_ctx, &mut log_ctx).await
            {
                tracing::warn!("Failed to persist TOTP attempt increment: {}", e.message);
            }

            log_ctx.set_error("Unauthenticated", "Verification failed");
            return Err(WireError::new(ErrorCode::Unauthenticated, "Verification failed"));
        }

        // TOTP code valid — create session directly.
        let sys_svc = self.ctx.system_service();
        let signing_key =
            self.ensure_signing_key_cached(&sys_svc).map_err(tonic_status_to_wire_error)?;
        let jwt_config = self.ctx.jwt_config.as_ref().ok_or_else(|| {
            WireError::new(ErrorCode::FailedPrecondition, "JWT config not configured")
        })?;

        let (refresh_token_str, refresh_token_hash) = crate::jwt::generate_refresh_token();
        let family = crate::jwt::generate_family_id();

        let consume_request = SystemRequest::ConsumeTotpAndCreateSession {
            user_id,
            nonce,
            token_hash: refresh_token_hash,
            family,
            kid: signing_key.kid.clone(),
            ttl_secs: jwt_config.session_refresh_ttl_secs,
        };
        let response =
            self.ctx.propose_regional(region, consume_request, &_ctx, &mut log_ctx).await?;

        match response {
            LedgerResponse::TotpVerified { .. } => {
                let token_pair = self.sign_session_after_challenge(
                    user_id,
                    challenge.user_slug,
                    refresh_token_str,
                    &signing_key,
                    jwt_config.session_refresh_ttl_secs,
                )?;

                log_ctx.set_success();
                Ok(w::VerifyTotpResponse { tokens: Some(token_pair) })
            },
            LedgerResponse::Error { code, message } => {
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
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

    async fn consume_recovery_code(
        &self,
        request: w::ConsumeRecoveryCodeRequest,
        _ctx: RequestContext,
    ) -> Result<w::ConsumeRecoveryCodeResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("UserService", "consume_recovery_code", &_ctx);
        let req = request;
        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let (user_id, _user_slug, region) =
            self.resolve_user_region(req.user, &mut log_ctx).map_err(tonic_status_to_wire_error)?;

        // Parse challenge nonce.
        let nonce: [u8; 32] = req.challenge_nonce.as_ref().try_into().map_err(|_| {
            log_ctx.set_error("InvalidArgument", "challenge_nonce must be 32 bytes");
            WireError::new(ErrorCode::InvalidArgument, "challenge_nonce must be exactly 32 bytes")
        })?;

        // Validate recovery code format.
        if req.code.is_empty() {
            log_ctx.set_error("InvalidArgument", "code is required");
            return Err(WireError::new(ErrorCode::InvalidArgument, "recovery code is required"));
        }

        // Read challenge from regional state.
        let regional_state = self.ctx.regional_state(region).map_err(tonic_status_to_wire_error)?;
        let regional_sys = self.ctx.regional_system_service(regional_state);

        let challenge = regional_sys
            .get_totp_challenge(user_id, &nonce)
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| {
                log_ctx.set_error("NotFound", "Challenge not found");
                WireError::new(ErrorCode::NotFound, "Challenge not found")
            })?;

        // Service-layer expiry check.
        if Utc::now() >= challenge.expires_at {
            log_ctx.set_error("FailedPrecondition", "Challenge expired");
            return Err(WireError::new(ErrorCode::FailedPrecondition, "Challenge expired"));
        }

        // Service-layer attempt check.
        if challenge.attempts >= 3 {
            log_ctx.set_error("ResourceExhausted", "Too many attempts");
            return Err(WireError::new(ErrorCode::RateLimited, "Too many attempts"));
        }

        // Find recovery code credential.
        let recovery_creds = regional_sys
            .list_user_credentials(
                user_id,
                Some(inferadb_ledger_types::CredentialType::RecoveryCode),
            )
            .map_err(|e| error_classify::storage_error(&e))?;

        let Some(recovery_cred) = recovery_creds.first() else {
            // No recovery credential — still increment attempts to prevent probing.
            let increment_request = SystemRequest::IncrementTotpAttempt { user_id, nonce };
            if let Err(e) =
                self.ctx.propose_regional(region, increment_request, &_ctx, &mut log_ctx).await
            {
                tracing::warn!("Failed to persist TOTP attempt increment: {}", e.message);
            }
            // Generic error — no distinguishing detail (PRD security requirement).
            log_ctx.set_error("Unauthenticated", "Verification failed");
            return Err(WireError::new(ErrorCode::Unauthenticated, "Verification failed"));
        };

        // Hash the raw recovery code (SHA-256).
        use sha2::Digest;
        let code_hash: [u8; 32] = sha2::Sha256::digest(req.code.as_bytes()).into();

        // Prepare session tokens.
        let sys_svc = self.ctx.system_service();
        let signing_key =
            self.ensure_signing_key_cached(&sys_svc).map_err(tonic_status_to_wire_error)?;
        let jwt_config = self.ctx.jwt_config.as_ref().ok_or_else(|| {
            WireError::new(ErrorCode::FailedPrecondition, "JWT config not configured")
        })?;

        let (refresh_token_str, refresh_token_hash) = crate::jwt::generate_refresh_token();
        let family = crate::jwt::generate_family_id();

        let consume_request = SystemRequest::ConsumeRecoveryAndCreateSession {
            user_id,
            nonce,
            code_hash,
            credential_id: recovery_cred.id,
            token_hash: refresh_token_hash,
            family,
            kid: signing_key.kid.clone(),
            ttl_secs: jwt_config.session_refresh_ttl_secs,
        };
        let response =
            self.ctx.propose_regional(region, consume_request, &_ctx, &mut log_ctx).await?;

        match response {
            LedgerResponse::RecoveryCodeConsumed { remaining_codes, .. } => {
                let token_pair = self.sign_session_after_challenge(
                    user_id,
                    challenge.user_slug,
                    refresh_token_str,
                    &signing_key,
                    jwt_config.session_refresh_ttl_secs,
                )?;

                log_ctx.set_success();
                Ok(w::ConsumeRecoveryCodeResponse { tokens: Some(token_pair), remaining_codes })
            },
            LedgerResponse::Error { code, message } => {
                // Generic error for recovery code failures (PRD: no distinguishing detail).
                if code == inferadb_ledger_types::ErrorCode::Unauthenticated
                    || code == inferadb_ledger_types::ErrorCode::NotFound
                {
                    // Increment attempt counter on the challenge.
                    let increment_request = SystemRequest::IncrementTotpAttempt { user_id, nonce };
                    if let Err(e) = self
                        .ctx
                        .propose_regional(region, increment_request, &_ctx, &mut log_ctx)
                        .await
                    {
                        tracing::warn!("Failed to persist TOTP attempt increment: {}", e.message);
                    }

                    log_ctx.set_error("Unauthenticated", "Verification failed");
                    return Err(WireError::new(ErrorCode::Unauthenticated, "Verification failed"));
                }
                log_ctx.set_error(code.grpc_code_name(), &message);
                Err(helpers::error_code_to_wire_error(code, message))
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn populated_user_round_trips_through_serde_json() {
        let original = ws::User {
            id: Some(ws::UserId::new(42)),
            name: "Alice".to_owned(),
            email: Some(ws::UserEmailId::new(7)),
            status: ws::UserStatus::Active,
            created_at: 1_700_000_000_000_000_000,
            updated_at: 1_700_000_001_000_000_000,
            role: ws::UserRole::Admin,
            slug: Some(ws::UserSlug::new(0xDEAD_BEEF)),
            deleted_at: Some(1_700_000_002_000_000_000),
        };
        let bytes = serde_json::to_vec(&original).unwrap();
        let back: ws::User = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, original);
    }
}
