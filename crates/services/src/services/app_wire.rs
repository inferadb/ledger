//! Wire-protocol implementation for `AppService` (Phase F.1.f.1.7).
//!
//! Provides the [`inferadb_ledger_wire_services::AppService`] impl on
//! [`super::app::AppService`]. The wire impl operates on wire types
//! end-to-end — it does not delegate through the tonic impl. Each
//! handler body mirrors the corresponding tonic [`AppService`] method
//! one-for-one; both impls access the same `AppService` private fields
//! (made `pub(super)` for this purpose) and call into the same
//! `SlugResolver` / `helpers::*` / `ServiceContext::propose_*`
//! primitives.
//!
//! Helpers in this module that bridge wire ↔ proto representations are
//! preserved (and marked `#[allow(dead_code)]` where the wire impl no
//! longer routes through them) so the round-trip tests at the bottom of
//! the file continue to exercise the conversion shape — the tonic impl
//! still depends on those proto types in the meantime, and F.1.f.2
//! deletes both the tonic impl and the now-redundant helpers in the
//! same pass.
//!
//! `SlugResolver::*`, `helpers::*`, and `ServiceContext::propose_*`
//! return [`WireError`] directly (F.1.f.0) — no adapter required on the
//! wire path. Helpers that still return `tonic::Status` (F.1.f.0 gaps:
//! the `parse_idempotency_key` and `map_credential_type` helpers on
//! `super::app`, plus the storage-layer error mapping) are bridged
//! inline via [`tonic_status_to_wire_error`].
//!
//! Conversions are mechanical field-by-field copies because the wire
//! types in [`inferadb_ledger_wire::services::app`] mirror the proto
//! layout exactly (E.2). The non-trivial mappings are:
//!
//! - `created_at` / `updated_at` / `expires_at` are `Option<prost_types::Timestamp>` in proto and
//!   `u64` UNIX nanoseconds on the wire — bridged by the shared helpers in [`super::wire_shared`].
//! - `idempotency_key` is `Vec<u8>` in proto and `Bytes` on the wire.
//! - `AppCredentialType` is a `#[repr(i32)]` enum on both sides, mapped by numeric tag with unknown
//!   values collapsing to `Unspecified` to match proto's open-enum semantics.
//!
//! Helpers are scoped locally rather than lifted to `wire_shared.rs` to
//! avoid write conflicts with parallel service shim work; future
//! deduplication passes are out of scope.

use inferadb_ledger_raft::types::{LedgerResponse, OrganizationRequest};
use inferadb_ledger_state::system::{
    AppCredentialType as DomainAppCredentialType, AppVaultConnection, ClientAssertionEntry,
    SYSTEM_VAULT_ID, SystemKeys,
};
use inferadb_ledger_types::{
    AppSlug as DomainAppSlug, ClientAssertionId as DomainClientAssertionId,
    OrganizationSlug as DomainOrganizationSlug, VaultSlug as DomainVaultSlug, decode,
    events::{EventAction, EventOutcome as EventOutcomeType},
};
use inferadb_ledger_wire::{
    ErrorCode, RequestContext, WireError,
    services::{app as w, shared as ws},
};
use zeroize::Zeroizing;

use super::{
    app::{AppService, CachedCredential},
    error_classify, helpers,
    slug_resolver::SlugResolver,
    wire_helpers::tonic_status_to_wire_error,
};

// ---------------------------------------------------------------------------
// Enum tag mappings.
// ---------------------------------------------------------------------------

/// Numeric-tag mapping from proto `AppCredentialType` (raw i32) to the wire enum.
///
/// Both enums declare `Unspecified = 0`, `ClientSecret = 1`, `MtlsCa = 2`,
/// `MtlsSelfSigned = 3`, `ClientAssertion = 4`. Unknown tags collapse to
/// `Unspecified` to match proto's open-enum semantics — protects against
/// future proto additions.
///
/// Used only in test inverses today (`proto_to_wire_set_app_credential_enabled_request`);
/// the production request shim writes the i32 tag straight onto the wire enum's
/// repr. Marked `#[cfg(test)]` to avoid a dead-code warning while still
/// exercising the round-trip in unit tests.
#[cfg(test)]
fn proto_app_credential_type_to_wire(value: i32) -> w::AppCredentialType {
    match value {
        x if x == w::AppCredentialType::ClientSecret as i32 => w::AppCredentialType::ClientSecret,
        x if x == w::AppCredentialType::MtlsCa as i32 => w::AppCredentialType::MtlsCa,
        x if x == w::AppCredentialType::MtlsSelfSigned as i32 => {
            w::AppCredentialType::MtlsSelfSigned
        },
        x if x == w::AppCredentialType::ClientAssertion as i32 => {
            w::AppCredentialType::ClientAssertion
        },
        _ => w::AppCredentialType::Unspecified,
    }
}

// ---------------------------------------------------------------------------
// Helpers operating directly on domain types — used by the wire-trait impl.
// ---------------------------------------------------------------------------

/// Convert a `chrono::DateTime<Utc>` into UNIX nanoseconds.
///
/// Negative or far-future timestamps clamp to 0 / `u64::MAX` rather than
/// panicking; matches the convention in `token_wire.rs`.
fn datetime_to_ns(dt: &chrono::DateTime<chrono::Utc>) -> u64 {
    u64::try_from(dt.timestamp_nanos_opt().unwrap_or(0).max(0)).unwrap_or(0)
}

/// Inverse of [`datetime_to_ns`].
///
/// Used to parse client-supplied `expires_at` (UNIX ns) for the
/// `CreateAppClientAssertion` flow. The wire field is `u64`; the domain
/// type is `chrono::DateTime<Utc>`. Saturates at `i64::MAX` if the wire
/// value exceeds the domain range, matching the implicit
/// `prost_types::Timestamp` saturation in the original tonic path.
fn ns_to_datetime(ns: u64) -> chrono::DateTime<chrono::Utc> {
    let signed = i64::try_from(ns).unwrap_or(i64::MAX);
    chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(signed)
}

/// Build a wire `AppInfo` from a domain `App`, resolving the slug from
/// applied state. Mirrors `AppService::app_to_proto`.
fn app_to_wire_info(
    resolver: &SlugResolver,
    app: &inferadb_ledger_state::system::App,
    include_credentials: bool,
) -> Result<w::AppInfo, WireError> {
    let slug = resolver.resolve_app_slug(app.id)?;
    let credentials = if include_credentials {
        Some(w::AppCredentialsInfo {
            client_secret_enabled: app.credentials.client_secret.enabled,
            mtls_ca_enabled: app.credentials.mtls_ca.enabled,
            mtls_self_signed_enabled: app.credentials.mtls_self_signed.enabled,
            client_assertion_enabled: app.credentials.client_assertion.enabled,
        })
    } else {
        None
    };
    Ok(w::AppInfo {
        slug: Some(ws::AppSlug::new(slug.value())),
        name: app.name.clone(),
        description: app.description.clone(),
        enabled: app.enabled,
        credentials,
        created_at: datetime_to_ns(&app.created_at),
        updated_at: datetime_to_ns(&app.updated_at),
    })
}

/// Build a wire `AppVaultConnectionInfo` from a domain `AppVaultConnection`.
/// Mirrors `AppService::vault_connection_to_proto`.
fn vault_connection_to_wire_info(conn: &AppVaultConnection) -> w::AppVaultConnectionInfo {
    w::AppVaultConnectionInfo {
        vault: Some(ws::VaultSlug::new(conn.vault_slug.value())),
        allowed_scopes: conn.allowed_scopes.clone(),
        created_at: datetime_to_ns(&conn.created_at),
        updated_at: datetime_to_ns(&conn.updated_at),
    }
}

/// Build a wire `AppClientAssertionInfo` from a domain `ClientAssertionEntry`
/// plus the regional name. Mirrors `AppService::assertion_to_proto`.
fn assertion_to_wire_info(entry: &ClientAssertionEntry, name: String) -> w::AppClientAssertionInfo {
    w::AppClientAssertionInfo {
        id: Some(ws::ClientAssertionId::new(entry.id.value())),
        name,
        enabled: entry.enabled,
        expires_at: datetime_to_ns(&entry.expires_at),
        created_at: datetime_to_ns(&entry.created_at),
    }
}

/// Numeric-tag mapping from wire `AppCredentialType` to the domain enum.
///
/// Mirrors the tonic path's `map_credential_type` but operates on the
/// wire enum directly. `Unspecified` and any unknown numeric value
/// produce `InvalidArgument` — preserves the parity with the tonic
/// validation.
fn wire_credential_type_to_domain(
    ct: w::AppCredentialType,
) -> Result<DomainAppCredentialType, WireError> {
    match ct {
        w::AppCredentialType::ClientSecret => Ok(DomainAppCredentialType::ClientSecret),
        w::AppCredentialType::MtlsCa => Ok(DomainAppCredentialType::MtlsCa),
        w::AppCredentialType::MtlsSelfSigned => Ok(DomainAppCredentialType::MtlsSelfSigned),
        w::AppCredentialType::ClientAssertion => Ok(DomainAppCredentialType::ClientAssertion),
        w::AppCredentialType::Unspecified => Err(WireError::new(
            ErrorCode::InvalidArgument,
            "Invalid or unspecified credential type",
        )),
    }
}

/// Parses a 16-byte idempotency key from raw bytes.
///
/// Returns `Ok(None)` for empty input (no idempotency), `Ok(Some(key))`
/// for exactly 16 bytes, and `Err` for any other length. Mirrors the
/// tonic path's `parse_idempotency_key`.
fn parse_idempotency_key_wire(bytes: &[u8]) -> Result<Option<[u8; 16]>, WireError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let key: [u8; 16] = bytes.try_into().map_err(|_| {
        WireError::new(ErrorCode::InvalidArgument, "idempotency_key must be exactly 16 bytes")
    })?;
    Ok(Some(key))
}

// ---------------------------------------------------------------------------
// Wire-trait implementation for `AppService`.
// ---------------------------------------------------------------------------

impl inferadb_ledger_wire_services::AppService for AppService {
    /// Mirrors the tonic [`app`](super::app::AppService) `create_app` handler:
    /// resolves the org slug, validates the client-supplied app slug + name,
    /// dual-proposes (GLOBAL `CreateApp` then REGIONAL `WriteAppProfile`),
    /// and returns the new `AppInfo` enriched with regional PII.
    async fn create_app(
        &self,
        request: w::CreateAppRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateAppResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from("AppService", "create_app", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        // Client-supplied Snowflake slug — required. Retries across lost
        // responses MUST reuse the same slug so the per-org apply
        // idempotency path returns the existing `AppId` instead of
        // creating an orphan directory entry.
        let slug_wire = req.slug.ok_or_else(|| {
            WireError::new(ErrorCode::InvalidArgument, "CreateAppRequest.slug is required")
        })?;
        if slug_wire.value() == 0 {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "CreateAppRequest.slug must be a non-zero Snowflake",
            ));
        }
        let slug = DomainAppSlug::new(slug_wire.value());

        let name = req.name.trim().to_string();
        if name.is_empty() {
            return Err(WireError::new(ErrorCode::InvalidArgument, "App name must not be empty"));
        }

        // Resolve org region before the Raft proposal so Step 2 can use it.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;

        // Step 1 (GLOBAL): Create app directory entry (ID + slug only, no PII).
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::CreateApp { organization: org_id, slug },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        let app_id = match response {
            LedgerResponse::AppCreated { app_id, .. } => app_id,
            LedgerResponse::Error { code, message } => {
                return Err(helpers::error_code_to_wire_error(code, message));
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                return Err(WireError::new(
                    ErrorCode::Internal,
                    "Unexpected response from state machine",
                ));
            },
        };

        // Step 2 (REGIONAL): Write app name and description to the org's regional store.
        // Encrypted with OrgShredKey for crypto-shredding on organization purge.
        let system_request = inferadb_ledger_raft::types::SystemRequest::WriteAppProfile {
            organization: org_id,
            app: app_id,
            name,
            description: req.description,
        };
        let profile_response = self
            .ctx
            .propose_regional_org_encrypted(
                org_meta.region,
                system_request,
                org_id,
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        if let LedgerResponse::Error { code, message } = profile_response {
            return Err(helpers::error_code_to_wire_error(code, message));
        }

        log_ctx.record_event(EventAction::AppCreated, EventOutcomeType::Success, &[]);
        let app = self.load_app_wire(org_id, app_id)?;
        let info = app_to_wire_info(&resolver, &app, true)?;
        Ok(w::CreateAppResponse { app: Some(info) })
    }

    /// Mirrors the tonic [`app`](super::app::AppService) `get_app` handler:
    /// resolves org + app slugs, loads the app from GLOBAL state with REGIONAL
    /// PII overlay, and returns the merged `AppInfo`.
    async fn get_app(
        &self,
        request: w::GetAppRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetAppResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from("AppService", "get_app", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (resolved_org, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        if org_id != resolved_org {
            return Err(WireError::new(
                ErrorCode::NotFound,
                "App not found in the specified organization",
            ));
        }

        let app = self.load_app_wire(resolved_org, app_id)?;
        let info = app_to_wire_info(&resolver, &app, true)?;
        Ok(w::GetAppResponse { app: Some(info) })
    }

    /// Mirrors the tonic [`app`](super::app::AppService) `list_apps` handler:
    /// resolves the org slug and lists every app in the organization, merging
    /// REGIONAL PII into each `AppInfo`.
    async fn list_apps(
        &self,
        request: w::ListAppsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListAppsResponse, WireError> {
        let mut log_ctx = self.ctx.make_request_context_from("AppService", "list_apps", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let apps = self.list_apps_internal(org_id).map_err(tonic_status_to_wire_error)?;
        let mut wire_apps = Vec::with_capacity(apps.len());
        for app in &apps {
            match app_to_wire_info(&resolver, app, false) {
                Ok(info) => wire_apps.push(info),
                Err(e) => {
                    tracing::warn!(error = ?e, "failed to convert app to wire info, skipping");
                },
            }
        }
        Ok(w::ListAppsResponse { apps: wire_apps })
    }

    /// Mirrors the tonic [`app`](super::app::AppService) `update_app` handler:
    /// resolves the org + app, validates that at least one mutating field is
    /// supplied, and proposes a REGIONAL `WriteAppProfile` (encrypted).
    async fn update_app(
        &self,
        request: w::UpdateAppRequest,
        _ctx: RequestContext,
    ) -> Result<w::UpdateAppResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from("AppService", "update_app", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let name = req
            .name
            .map(|n| {
                let trimmed = n.trim().to_string();
                if trimmed.is_empty() {
                    return Err(WireError::new(
                        ErrorCode::InvalidArgument,
                        "App name must not be empty",
                    ));
                }
                Ok(trimmed)
            })
            .transpose()?;

        let description = req.description;

        // Require at least one field to update.
        if name.is_none() && description.is_none() {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "At least one field must be provided for update",
            ));
        }

        // Load current app to merge unchanged fields.
        let current_app = self.load_app_wire(org_id, app_id)?;
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
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
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
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            _ => {
                log_ctx.record_event(EventAction::AppUpdated, EventOutcomeType::Success, &[]);
                let app = self.load_app_wire(org_id, app_id)?;
                let info = app_to_wire_info(&resolver, &app, true)?;
                Ok(w::UpdateAppResponse { app: Some(info) })
            },
        }
    }

    /// Mirrors the tonic [`app`](super::app::AppService) `delete_app` handler:
    /// proposes REGIONAL `DeleteAppProfile` then GLOBAL `DeleteApp`. Step 1
    /// failure short-circuits; step 2 cleans up the structural record + slug
    /// index + assertions + vault connections.
    async fn delete_app(
        &self,
        request: w::DeleteAppRequest,
        _ctx: RequestContext,
    ) -> Result<w::DeleteAppResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from("AppService", "delete_app", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        // Step 1 (REGIONAL): Delete AppProfile and name index.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let delete_profile = inferadb_ledger_raft::types::SystemRequest::DeleteAppProfile {
            organization: org_id,
            app: app_id,
        };
        let profile_response =
            self.ctx.propose_regional(org_meta.region, delete_profile, &_ctx, &mut log_ctx).await?;
        if let LedgerResponse::Error { code, message } = profile_response {
            return Err(helpers::error_code_to_wire_error(code, message));
        }

        // Step 2 (GLOBAL): Delete App record, slug index, vault connections, assertions.
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::DeleteApp { organization: org_id, app: app_id },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppDeleted { .. } => {
                log_ctx.record_event(EventAction::AppDeleted, EventOutcomeType::Success, &[]);
                Ok(w::DeleteAppResponse {})
            },
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from state machine"))
            },
        }
    }

    /// Mirrors the tonic [`app`](super::app::AppService) `set_app_enabled`
    /// handler: proposes a GLOBAL `SetAppEnabled` and returns the updated
    /// `AppInfo`.
    async fn set_app_enabled(
        &self,
        request: w::SetAppEnabledRequest,
        _ctx: RequestContext,
    ) -> Result<w::SetAppEnabledResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "set_app_enabled", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::SetAppEnabled {
                    organization: org_id,
                    app: app_id,
                    enabled: req.enabled,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppToggled { organization_id } => {
                log_ctx.record_event(EventAction::AppStatusChanged, EventOutcomeType::Success, &[]);
                let app = self.load_app_wire(organization_id, app_id)?;
                let info = app_to_wire_info(&resolver, &app, true)?;
                Ok(w::SetAppEnabledResponse { app: Some(info) })
            },
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from state machine"))
            },
        }
    }

    /// Mirrors the tonic [`app`](super::app::AppService)
    /// `set_app_credential_enabled` handler: maps the wire credential type to
    /// the domain enum and proposes a GLOBAL `SetAppCredentialEnabled`.
    async fn set_app_credential_enabled(
        &self,
        request: w::SetAppCredentialEnabledRequest,
        _ctx: RequestContext,
    ) -> Result<w::SetAppCredentialEnabledResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "set_app_credential_enabled", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let credential_type = wire_credential_type_to_domain(req.credential_type)?;

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::SetAppCredentialEnabled {
                    organization: org_id,
                    app: app_id,
                    credential_type,
                    enabled: req.enabled,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppCredentialToggled { organization_id } => {
                log_ctx.record_event(
                    EventAction::AppCredentialStatusChanged,
                    EventOutcomeType::Success,
                    &[],
                );
                let app = self.load_app_wire(organization_id, app_id)?;
                let info = app_to_wire_info(&resolver, &app, true)?;
                Ok(w::SetAppCredentialEnabledResponse { app: Some(info) })
            },
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from state machine"))
            },
        }
    }

    /// Mirrors the tonic [`app`](super::app::AppService)
    /// `get_app_client_secret` handler: returns whether the client_secret
    /// credential is enabled and whether a hash is set.
    async fn get_app_client_secret(
        &self,
        request: w::GetAppClientSecretRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetAppClientSecretResponse, WireError> {
        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "get_app_client_secret", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (resolved_org, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        if org_id != resolved_org {
            return Err(WireError::new(
                ErrorCode::NotFound,
                "App not found in the specified organization",
            ));
        }

        let app = self.load_app_wire(resolved_org, app_id)?;
        Ok(w::GetAppClientSecretResponse {
            enabled: app.credentials.client_secret.enabled,
            has_secret: app.credentials.client_secret.secret_hash.is_some(),
        })
    }

    /// Mirrors the tonic [`app`](super::app::AppService)
    /// `rotate_app_client_secret` handler: generates a fresh random secret,
    /// bcrypt-hashes it off the runtime, proposes GLOBAL
    /// `RotateAppClientSecret`, and returns the plaintext exactly once.
    /// Idempotent retries with the same `idempotency_key` return the cached
    /// secret instead of generating a new one.
    async fn rotate_app_client_secret(
        &self,
        request: w::RotateAppClientSecretRequest,
        _ctx: RequestContext,
    ) -> Result<w::RotateAppClientSecretResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "rotate_app_client_secret", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let idempotency_key = parse_idempotency_key_wire(&req.idempotency_key)?;

        // Check idempotency cache for a previously generated secret.
        let cache_key = idempotency_key.map(|k| (org_id, app_id, k));
        if let Some(ref ck) = cache_key
            && let Some(CachedCredential::Secret(cached_secret)) = self.credential_cache.get(ck)
        {
            return Ok(w::RotateAppClientSecretResponse { secret: cached_secret.to_string() });
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
                .map_err(|e| error_classify::crypto_error(&e))?
                .map_err(|e| error_classify::crypto_error(&e))?;

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::RotateAppClientSecret {
                    organization: org_id,
                    app: app_id,
                    new_secret_hash: secret_hash,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppClientSecretRotated { .. } => {
                log_ctx.record_event(EventAction::AppSecretRotated, EventOutcomeType::Success, &[]);
                // Cache the secret for idempotent retries.
                if let Some(ck) = cache_key {
                    self.credential_cache
                        .insert(ck, CachedCredential::Secret(plaintext_secret.clone().into()));
                }
                Ok(w::RotateAppClientSecretResponse { secret: plaintext_secret })
            },
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from state machine"))
            },
        }
    }

    /// Mirrors the tonic [`app`](super::app::AppService)
    /// `list_app_client_assertions` handler: scans the GLOBAL assertion
    /// entries for the app and merges in each assertion's REGIONAL name.
    async fn list_app_client_assertions(
        &self,
        request: w::ListAppClientAssertionsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListAppClientAssertionsResponse, WireError> {
        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "list_app_client_assertions", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (resolved_org, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        if org_id != resolved_org {
            return Err(WireError::new(
                ErrorCode::NotFound,
                "App not found in the specified organization",
            ));
        }

        let entries = self
            .list_assertions_internal(resolved_org, app_id)
            .map_err(tonic_status_to_wire_error)?;
        let assertions: Vec<w::AppClientAssertionInfo> = entries
            .iter()
            .map(|entry| {
                let name = self.load_assertion_name(resolved_org, app_id, entry.id);
                assertion_to_wire_info(entry, name)
            })
            .collect();
        Ok(w::ListAppClientAssertionsResponse { assertions })
    }

    /// Mirrors the tonic [`app`](super::app::AppService)
    /// `get_app_client_assertion` handler: reads a single assertion entry by
    /// id from GLOBAL state and decorates it with the REGIONAL name.
    async fn get_app_client_assertion(
        &self,
        request: w::GetAppClientAssertionRequest,
        _ctx: RequestContext,
    ) -> Result<w::GetAppClientAssertionResponse, WireError> {
        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "get_app_client_assertion", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (resolved_org, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        if org_id != resolved_org {
            return Err(WireError::new(
                ErrorCode::NotFound,
                "App not found in the specified organization",
            ));
        }
        let assertion_id_wire = req
            .assertion
            .ok_or_else(|| WireError::new(ErrorCode::InvalidArgument, "Missing assertion ID"))?;
        let assertion_id = DomainClientAssertionId::new(assertion_id_wire.value());

        let key = SystemKeys::app_assertion_key(resolved_org, app_id, assertion_id);
        let entity = self
            .ctx
            .state
            .get_entity(SYSTEM_VAULT_ID, key.as_bytes())
            .map_err(|e| error_classify::storage_error(&e))?
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Client assertion not found"))?;
        let entry = decode::<ClientAssertionEntry>(&entity.value)
            .map_err(|e| error_classify::serialization_error(&e))?;

        let name = self.load_assertion_name(resolved_org, app_id, entry.id);
        let info = assertion_to_wire_info(&entry, name);
        Ok(w::GetAppClientAssertionResponse { assertion: Some(info) })
    }

    /// Mirrors the tonic [`app`](super::app::AppService)
    /// `create_app_client_assertion` handler: generates an Ed25519 keypair
    /// off the runtime, proposes a GLOBAL structural entry then a REGIONAL
    /// name entry (encrypted), and returns the assertion info plus the
    /// private key PEM exactly once. Idempotent retries with the same
    /// `idempotency_key` return the cached keypair.
    async fn create_app_client_assertion(
        &self,
        request: w::CreateAppClientAssertionRequest,
        _ctx: RequestContext,
    ) -> Result<w::CreateAppClientAssertionResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "create_app_client_assertion", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;

        let name = req.name.trim().to_string();
        if name.is_empty() {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "Client assertion name must not be empty",
            ));
        }

        let idempotency_key = parse_idempotency_key_wire(&req.idempotency_key)?;

        if req.expires_at == 0 {
            return Err(WireError::new(
                ErrorCode::InvalidArgument,
                "expires_at is required for client assertions",
            ));
        }
        let expires_at = ns_to_datetime(req.expires_at);

        // Check idempotency cache for a previously generated keypair.
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
                .map_err(|e| error_classify::crypto_error(&e))?
                .to_string()
                .into();

            // Cache before proposing so retries during Raft round-trip reuse the same keypair.
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

        // Resolve org region before the Raft proposal so Phase 2 can use it.
        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;

        // Phase 1: Propose structural entry to GLOBAL Raft (no PII).
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::CreateAppClientAssertion {
                    organization: org_id,
                    app: app_id,
                    expires_at,
                    public_key_bytes,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppClientAssertionCreated { assertion_id } => {
                // Phase 2: Write assertion name to REGIONAL Raft (PII isolation).
                // Encrypted with OrgShredKey for crypto-shredding on organization purge.
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
                        &_ctx,
                        &mut log_ctx,
                    )
                    .await?;
                if let LedgerResponse::Error { code, message } = name_response {
                    return Err(helpers::error_code_to_wire_error(code, message));
                }

                log_ctx.record_event(
                    EventAction::AppAssertionCreated,
                    EventOutcomeType::Success,
                    &[],
                );

                // Build the assertion info from the committed data.
                let fallback_info = w::AppClientAssertionInfo {
                    id: Some(ws::ClientAssertionId::new(assertion_id.value())),
                    name: name.clone(),
                    enabled: true,
                    expires_at: datetime_to_ns(&expires_at),
                    created_at: 0,
                };

                // Read back from state for accurate created_at.
                let key = SystemKeys::app_assertion_key(org_id, app_id, assertion_id);
                let final_info = match self.ctx.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()) {
                    Ok(Some(entity)) => match decode::<ClientAssertionEntry>(&entity.value) {
                        Ok(entry) => assertion_to_wire_info(&entry, name),
                        Err(_) => fallback_info,
                    },
                    _ => fallback_info,
                };

                Ok(w::CreateAppClientAssertionResponse {
                    assertion: Some(final_info),
                    private_key_pem: private_key_pem.to_string(),
                })
            },
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from state machine"))
            },
        }
    }

    /// Mirrors the tonic [`app`](super::app::AppService)
    /// `delete_app_client_assertion` handler: best-effort REGIONAL name
    /// delete (logged on failure) followed by GLOBAL structural delete.
    async fn delete_app_client_assertion(
        &self,
        request: w::DeleteAppClientAssertionRequest,
        _ctx: RequestContext,
    ) -> Result<w::DeleteAppClientAssertionResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "delete_app_client_assertion", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let assertion_id_wire = req
            .assertion
            .ok_or_else(|| WireError::new(ErrorCode::InvalidArgument, "Missing assertion ID"))?;
        let assertion = DomainClientAssertionId::new(assertion_id_wire.value());

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;

        // Phase 1: Delete assertion name from REGIONAL Raft (best-effort).
        {
            let name_request =
                inferadb_ledger_raft::types::SystemRequest::DeleteClientAssertionName {
                    organization: org_id,
                    app: app_id,
                    assertion,
                };
            // Regional delete is best-effort: if region is unavailable, the
            // GLOBAL delete still proceeds and `PurgeOrganizationRegional`
            // will clean up orphaned names.
            if let Err(e) =
                self.ctx.propose_regional(org_meta.region, name_request, &_ctx, &mut log_ctx).await
            {
                tracing::warn!(
                    error = ?e,
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
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::DeleteAppClientAssertion {
                    organization: org_id,
                    app: app_id,
                    assertion,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppClientAssertionDeleted { .. } => {
                log_ctx.record_event(
                    EventAction::AppAssertionDeleted,
                    EventOutcomeType::Success,
                    &[],
                );
                Ok(w::DeleteAppClientAssertionResponse {})
            },
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from state machine"))
            },
        }
    }

    /// Mirrors the tonic [`app`](super::app::AppService)
    /// `set_app_client_assertion_enabled` handler: proposes a GLOBAL
    /// `SetAppClientAssertionEnabled` and emits the canonical credential
    /// status-change audit event.
    async fn set_app_client_assertion_enabled(
        &self,
        request: w::SetAppClientAssertionEnabledRequest,
        _ctx: RequestContext,
    ) -> Result<w::SetAppClientAssertionEnabledResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from(
            "AppService",
            "set_app_client_assertion_enabled",
            &_ctx,
        );
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let assertion_id_wire = req
            .assertion
            .ok_or_else(|| WireError::new(ErrorCode::InvalidArgument, "Missing assertion ID"))?;
        let assertion = DomainClientAssertionId::new(assertion_id_wire.value());

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::SetAppClientAssertionEnabled {
                    organization: org_id,
                    app: app_id,
                    assertion,
                    enabled: req.enabled,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppClientAssertionToggled { .. } => {
                log_ctx.record_event(
                    EventAction::AppCredentialStatusChanged,
                    EventOutcomeType::Success,
                    &[],
                );
                Ok(w::SetAppClientAssertionEnabledResponse {})
            },
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from state machine"))
            },
        }
    }

    /// Mirrors the tonic [`app`](super::app::AppService) `list_app_vaults`
    /// handler: scans the app's GLOBAL vault-connection entries and returns
    /// each as a wire `AppVaultConnectionInfo`.
    async fn list_app_vaults(
        &self,
        request: w::ListAppVaultsRequest,
        _ctx: RequestContext,
    ) -> Result<w::ListAppVaultsResponse, WireError> {
        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "list_app_vaults", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (resolved_org, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        if org_id != resolved_org {
            return Err(WireError::new(
                ErrorCode::NotFound,
                "App not found in the specified organization",
            ));
        }

        let connections = self
            .list_vault_connections(resolved_org, app_id)
            .map_err(tonic_status_to_wire_error)?;
        let vaults: Vec<w::AppVaultConnectionInfo> =
            connections.iter().map(vault_connection_to_wire_info).collect();
        Ok(w::ListAppVaultsResponse { vaults })
    }

    /// Mirrors the tonic [`app`](super::app::AppService) `add_app_vault`
    /// handler: resolves the vault slug to its internal id, proposes a
    /// GLOBAL `AddAppVault`, and reads back the committed connection.
    async fn add_app_vault(
        &self,
        request: w::AddAppVaultRequest,
        _ctx: RequestContext,
    ) -> Result<w::AddAppVaultResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx = self.ctx.make_request_context_from("AppService", "add_app_vault", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let vault_slug =
            SlugResolver::extract_vault_slug(req.vault.map(|s| DomainVaultSlug::new(s.value())))?;
        let vault_id = resolver.resolve_vault(vault_slug)?;

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::AddAppVault {
                    organization: org_id,
                    app: app_id,
                    vault: vault_id,
                    vault_slug,
                    allowed_scopes: req.allowed_scopes,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppVaultAdded { organization_id } => {
                log_ctx.record_event(
                    EventAction::AppVaultConnected,
                    EventOutcomeType::Success,
                    &[],
                );
                let conn = self
                    .read_vault_connection(organization_id, app_id, vault_id)
                    .map_err(tonic_status_to_wire_error)?;
                Ok(w::AddAppVaultResponse { vault: Some(vault_connection_to_wire_info(&conn)) })
            },
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from state machine"))
            },
        }
    }

    /// Mirrors the tonic [`app`](super::app::AppService) `update_app_vault`
    /// handler: proposes a GLOBAL `UpdateAppVault` and reads back the
    /// committed connection.
    async fn update_app_vault(
        &self,
        request: w::UpdateAppVaultRequest,
        _ctx: RequestContext,
    ) -> Result<w::UpdateAppVaultResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "update_app_vault", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let vault_slug =
            SlugResolver::extract_vault_slug(req.vault.map(|s| DomainVaultSlug::new(s.value())))?;
        let vault_id = resolver.resolve_vault(vault_slug)?;

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::UpdateAppVault {
                    organization: org_id,
                    app: app_id,
                    vault: vault_id,
                    allowed_scopes: req.allowed_scopes,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppVaultUpdated { organization_id } => {
                log_ctx.record_event(EventAction::AppVaultUpdated, EventOutcomeType::Success, &[]);
                let conn = self
                    .read_vault_connection(organization_id, app_id, vault_id)
                    .map_err(tonic_status_to_wire_error)?;
                Ok(w::UpdateAppVaultResponse { vault: Some(vault_connection_to_wire_info(&conn)) })
            },
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from state machine"))
            },
        }
    }

    /// Mirrors the tonic [`app`](super::app::AppService) `remove_app_vault`
    /// handler: proposes a GLOBAL `RemoveAppVault` and emits the canonical
    /// vault-disconnected audit event.
    async fn remove_app_vault(
        &self,
        request: w::RemoveAppVaultRequest,
        _ctx: RequestContext,
    ) -> Result<w::RemoveAppVaultResponse, WireError> {
        helpers::check_not_draining(self.ctx.health_state.as_ref())?;

        let mut log_ctx =
            self.ctx.make_request_context_from("AppService", "remove_app_vault", &_ctx);
        let req = request;

        if let Some(ref c) = req.caller {
            log_ctx.set_caller(c.value());
        }

        let resolver = self.resolver();
        let org_id = resolver
            .extract_and_resolve(req.organization.map(|s| DomainOrganizationSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let (_, app_id) = resolver
            .extract_and_resolve_app(req.app.map(|s| DomainAppSlug::new(s.value())))
            .inspect_err(|err| {
                log_ctx.set_error("InvalidArgument", &err.message);
            })?;
        let vault_slug =
            SlugResolver::extract_vault_slug(req.vault.map(|s| DomainVaultSlug::new(s.value())))?;
        let vault_id = resolver.resolve_vault(vault_slug)?;

        let org_meta = self
            .ctx
            .applied_state
            .get_organization(org_id)
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "Organization not found"))?;
        let response = self
            .ctx
            .propose_organization_request(
                org_meta.region,
                org_id,
                OrganizationRequest::RemoveAppVault {
                    organization: org_id,
                    app: app_id,
                    vault: vault_id,
                },
                &_ctx,
                &mut log_ctx,
            )
            .await?;

        match response {
            LedgerResponse::AppVaultRemoved { .. } => {
                log_ctx.record_event(
                    EventAction::AppVaultDisconnected,
                    EventOutcomeType::Success,
                    &[],
                );
                Ok(w::RemoveAppVaultResponse {})
            },
            LedgerResponse::Error { code, message } => {
                Err(helpers::error_code_to_wire_error(code, message))
            },
            other => {
                tracing::error!(?other, "Unexpected response from state machine");
                Err(WireError::new(ErrorCode::Internal, "Unexpected response from state machine"))
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // AppCredentialType enum tag mapping.
    // -----------------------------------------------------------------------

    #[test]
    fn proto_app_credential_type_each_known_tag_maps_correctly() {
        assert_eq!(proto_app_credential_type_to_wire(0), w::AppCredentialType::Unspecified);
        assert_eq!(proto_app_credential_type_to_wire(1), w::AppCredentialType::ClientSecret);
        assert_eq!(proto_app_credential_type_to_wire(2), w::AppCredentialType::MtlsCa);
        assert_eq!(proto_app_credential_type_to_wire(3), w::AppCredentialType::MtlsSelfSigned);
        assert_eq!(proto_app_credential_type_to_wire(4), w::AppCredentialType::ClientAssertion);
    }

    #[test]
    fn proto_app_credential_type_unknown_tag_collapses_to_unspecified() {
        assert_eq!(proto_app_credential_type_to_wire(99), w::AppCredentialType::Unspecified);
    }

    #[test]
    fn wire_app_credential_type_round_trips_through_proto_i32() {
        for t in [
            w::AppCredentialType::Unspecified,
            w::AppCredentialType::ClientSecret,
            w::AppCredentialType::MtlsCa,
            w::AppCredentialType::MtlsSelfSigned,
            w::AppCredentialType::ClientAssertion,
        ] {
            let tag = t as i32;
            assert_eq!(proto_app_credential_type_to_wire(tag), t);
        }
    }
}
