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

use inferadb_ledger_state::system::{
    App, AppVaultConnection, ClientAssertionEntry, SYSTEM_VAULT_ID, SystemKeys,
};
use inferadb_ledger_types::{AppId as DomainAppId, OrganizationId as DomainOrganizationId, decode};
use moka::sync::Cache as MokaCache;
use tonic::Status;
use zeroize::Zeroizing;

use super::{
    error_classify, service_infra::ServiceContext, slug_resolver::SlugResolver,
    wire_helpers::wire_error_to_tonic_status,
};

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
    pub(super) ctx: ServiceContext,
    /// Credential idempotency cache scoped by `(org, app, idempotency_key)`.
    ///
    /// Caches generated secret/keypair material so retries with the same
    /// idempotency key return the same material instead of generating new
    /// credentials that would orphan the previously generated ones.
    ///
    /// TTL is 10 minutes — sufficient for retry windows while minimizing
    /// the duration sensitive material persists in heap memory.
    pub(super) credential_cache: Arc<MokaCache<CredentialCacheKey, CachedCredential>>,
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
    pub(super) fn resolver(&self) -> SlugResolver {
        SlugResolver::new(self.ctx.applied_state.clone())
    }

    /// Loads an app from GLOBAL state and merges with `AppProfile` from
    /// REGIONAL state, returning the wire-native [`inferadb_ledger_wire::WireError`]
    /// on failure.
    ///
    /// The structural `App` record (credentials, enabled, slug) lives in GLOBAL.
    /// PII fields (name, description) live in a separate `AppProfile` in REGIONAL.
    /// Loads both and overlays the PII fields onto the returned `App`.
    pub(super) fn load_app_wire(
        &self,
        org_id: DomainOrganizationId,
        app_id: DomainAppId,
    ) -> Result<App, inferadb_ledger_wire::WireError> {
        let mut app = super::helpers::load_app(&self.ctx.state, org_id, app_id)?;
        self.overlay_app_profile(&mut app, org_id);
        Ok(app)
    }

    /// Overlays PII fields from a REGIONAL `AppProfile` onto a GLOBAL `App`.
    pub(super) fn overlay_app_profile(&self, app: &mut App, org_id: DomainOrganizationId) {
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

    /// Lists all apps in an organization, merging GLOBAL structural data with
    /// REGIONAL PII (name, description) from `AppProfile` records.
    pub(super) fn list_apps_internal(
        &self,
        org_id: DomainOrganizationId,
    ) -> Result<Vec<App>, Status> {
        let prefix = SystemKeys::app_prefix(org_id);
        let entities = self
            .ctx
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, 10000)
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?;

        let mut apps = Vec::with_capacity(entities.len());
        for entity in &entities {
            match decode::<App>(&entity.value) {
                Ok(mut app) => {
                    self.overlay_app_profile(&mut app, org_id);
                    apps.push(app);
                },
                Err(e) => {
                    tracing::warn!(error = ?e, "corrupt app entry, skipping");
                },
            }
        }
        Ok(apps)
    }

    /// Lists all vault connections for an app from the state layer.
    pub(super) fn list_vault_connections(
        &self,
        org_id: DomainOrganizationId,
        app_id: DomainAppId,
    ) -> Result<Vec<AppVaultConnection>, Status> {
        let prefix = SystemKeys::app_vault_prefix(org_id, app_id);
        let entities = self
            .ctx
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, 10000)
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?;

        let mut connections = Vec::with_capacity(entities.len());
        for entity in &entities {
            match decode::<AppVaultConnection>(&entity.value) {
                Ok(conn) => connections.push(conn),
                Err(e) => {
                    tracing::warn!(error = ?e, "corrupt vault connection entry, skipping");
                },
            }
        }
        Ok(connections)
    }

    /// Reads a vault connection from the state layer after a Raft mutation.
    pub(super) fn read_vault_connection(
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
            super::wire_helpers::build_wire_error(
                inferadb_ledger_wire::ErrorCode::Internal,
                "Vault connection not found after mutation",
                "",
                false,
                0,
                std::collections::BTreeMap::new(),
                "",
            ),
        )
        .map_err(wire_error_to_tonic_status)
    }

    /// Lists all client assertion entries for an app from the state layer.
    pub(super) fn list_assertions_internal(
        &self,
        org_id: DomainOrganizationId,
        app_id: DomainAppId,
    ) -> Result<Vec<ClientAssertionEntry>, Status> {
        let prefix = SystemKeys::app_assertion_prefix(org_id, app_id);
        let entities = self
            .ctx
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, 10000)
            .map_err(|e| wire_error_to_tonic_status(error_classify::storage_error(&e)))?;

        let mut entries = Vec::with_capacity(entities.len());
        for entity in &entities {
            match decode::<ClientAssertionEntry>(&entity.value) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    tracing::warn!(error = ?e, "corrupt assertion entry, skipping");
                },
            }
        }
        Ok(entries)
    }

    /// Loads the assertion name from REGIONAL state.
    ///
    /// Returns an empty string if the organization's region is unavailable
    /// or the name record does not exist (graceful degradation for read path).
    pub(super) fn load_assertion_name(
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
