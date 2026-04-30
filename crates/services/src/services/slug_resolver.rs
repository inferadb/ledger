//! Slug resolution for gRPC service boundaries.
//!
//! Translates external [`OrganizationSlug`], [`VaultSlug`], and [`UserSlug`] values
//! to their internal counterparts ([`OrganizationId`], [`VaultId`], and [`UserId`]) at the service
//! boundary. Every inbound request carrying a slug must resolve it before any
//! internal operation.
//!
//! Resolution reads from the Raft-replicated [`AppliedStateAccessor`] slug index,
//! ensuring consistency across all nodes.
//!
//! All methods return [`WireError`] on failure — the wire-protocol-native error
//! shape. Callers on the tonic path bridge via
//! [`super::wire_helpers::wire_error_to_tonic_status`]; callers on the wire
//! path propagate directly.
//!
//! ## Usage
//!
//! ```no_run
//! # use inferadb_ledger_raft::log_storage::AppliedStateAccessor;
//! # use inferadb_ledger_wire::WireError;
//! # fn example(applied_state: &AppliedStateAccessor) -> Result<(), WireError> {
//! use inferadb_ledger_services::services::slug_resolver::SlugResolver;
//!
//! let resolver = SlugResolver::new(applied_state.clone());
//! // resolver.extract_and_resolve(request.organization)?;
//! // resolver.extract_and_resolve_vault(request.vault)?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;

use inferadb_ledger_raft::log_storage::AppliedStateAccessor;
use inferadb_ledger_types::{
    AppId, AppSlug, OrganizationId, OrganizationSlug, TeamId, TeamSlug, UserId, UserSlug, VaultId,
    VaultSlug,
};
use inferadb_ledger_wire::{ErrorCode, WireError};

use super::wire_helpers::build_wire_error;

/// Build an `INVALID_ARGUMENT` `WireError` for slug-validation failures.
///
/// Used by every `extract_*_slug` helper. The empty `error_code`,
/// non-retryable, no `retry_after_ms`, empty `context`, and empty
/// `suggested_action` shape matches what `Status::invalid_argument(msg)`
/// would have produced via the legacy tonic path.
fn invalid_argument(message: impl Into<String>) -> WireError {
    build_wire_error(ErrorCode::InvalidArgument, message, "", false, 0, BTreeMap::new(), "")
}

/// Build a `NOT_FOUND` `WireError` for slug-resolution failures.
fn not_found(message: impl Into<String>) -> WireError {
    build_wire_error(ErrorCode::NotFound, message, "", false, 0, BTreeMap::new(), "")
}

/// Resolves external slugs to internal IDs at gRPC service boundaries.
///
/// Handles both organization slugs ([`OrganizationSlug`] → [`OrganizationId`])
/// and vault slugs ([`VaultSlug`] → [`VaultId`]). Wraps an
/// [`AppliedStateAccessor`] and provides validated, error-handling resolution
/// methods.
#[derive(Clone)]
pub struct SlugResolver {
    state: AppliedStateAccessor,
}

impl SlugResolver {
    /// Creates a resolver backed by the given applied state.
    pub fn new(state: AppliedStateAccessor) -> Self {
        Self { state }
    }

    /// Extracts and validates an organization slug.
    ///
    /// Returns `INVALID_ARGUMENT` if the slug is missing or zero.
    pub fn extract_slug(slug: Option<OrganizationSlug>) -> Result<OrganizationSlug, WireError> {
        let slug = slug.ok_or_else(|| invalid_argument("Missing organization"))?;
        if slug.value() == 0 {
            return Err(invalid_argument("organization must be non-zero"));
        }
        Ok(slug)
    }

    /// Resolves an organization slug to its internal ID.
    ///
    /// Returns `NOT_FOUND` if the slug is not registered.
    pub fn resolve(&self, slug: OrganizationSlug) -> Result<OrganizationId, WireError> {
        self.state
            .resolve_slug_to_id(slug)
            .ok_or_else(|| not_found(format!("Organization with slug {} not found", slug.value())))
    }

    /// Reverse lookup: internal ID to external slug.
    ///
    /// Returns `NOT_FOUND` if the ID has no associated slug.
    pub fn resolve_slug(&self, id: OrganizationId) -> Result<OrganizationSlug, WireError> {
        self.state
            .resolve_id_to_slug(id)
            .ok_or_else(|| not_found(format!("Organization {} not found", id)))
    }

    /// Extracts a slug and resolves it to an internal ID.
    ///
    /// Combines [`extract_slug`](Self::extract_slug) and [`resolve`](Self::resolve)
    /// for the common case where a request carries a required organization slug.
    pub fn extract_and_resolve(
        &self,
        slug: Option<OrganizationSlug>,
    ) -> Result<OrganizationId, WireError> {
        let slug = Self::extract_slug(slug)?;
        self.resolve(slug)
    }

    /// Extracts an optional slug and resolves it if present.
    ///
    /// Returns `Ok(None)` when the slug is absent. Returns an error
    /// only if the slug is present but zero or not found in the index.
    pub fn extract_and_resolve_optional(
        &self,
        slug: Option<OrganizationSlug>,
    ) -> Result<Option<OrganizationId>, WireError> {
        match slug {
            None => Ok(None),
            Some(slug) if slug.value() == 0 => {
                Err(invalid_argument("organization must be non-zero"))
            },
            Some(slug) => self.resolve(slug).map(Some),
        }
    }

    // --- System organization bypass for events ---

    /// Extracts and resolves an organization slug, with special handling for the
    /// system organization (slug = 0).
    ///
    /// Unlike [`extract_and_resolve`](Self::extract_and_resolve), this method
    /// allows slug = 0 and maps it directly to `SYSTEM_ORGANIZATION_ID` (0)
    /// without an index lookup. This is needed because the system organization
    /// is hardcoded and never created via `CreateOrganization`.
    ///
    /// Returns `INVALID_ARGUMENT` if the slug is missing.
    pub fn extract_and_resolve_for_events(
        &self,
        slug: Option<OrganizationSlug>,
    ) -> Result<OrganizationId, WireError> {
        let slug = slug.ok_or_else(|| invalid_argument("Missing organization"))?;

        // System organization: slug=0 maps to OrganizationId(0) directly
        if slug.value() == 0 {
            return Ok(OrganizationId::new(0));
        }

        self.resolve(slug)
    }

    // --- Vault slug resolution ---

    /// Extracts and validates a vault slug.
    ///
    /// Returns `INVALID_ARGUMENT` if the slug is missing or zero.
    pub fn extract_vault_slug(slug: Option<VaultSlug>) -> Result<VaultSlug, WireError> {
        let slug = slug.ok_or_else(|| invalid_argument("Missing vault"))?;
        if slug.value() == 0 {
            return Err(invalid_argument("vault must be non-zero"));
        }
        Ok(slug)
    }

    /// Resolves a vault slug to its internal ID.
    ///
    /// `VaultSlug` is globally unique (Snowflake), so a slug-only
    /// lookup is unambiguous — the tuple-keyed index returns a single
    /// `VaultId`. Callers that need the owning organization alongside
    /// the vault id should use
    /// [`resolve_vault_pair`](Self::resolve_vault_pair).
    /// Returns `NOT_FOUND` if the slug is not registered.
    pub fn resolve_vault(&self, slug: VaultSlug) -> Result<VaultId, WireError> {
        self.state
            .resolve_vault_slug_to_id(slug)
            .ok_or_else(|| not_found(format!("Vault with slug {} not found", slug.value())))
    }

    /// Resolves a vault slug to its owning `(OrganizationId, VaultId)` pair.
    ///
    /// Preferred over [`resolve_vault`](Self::resolve_vault) when the
    /// caller does not already have the organization in scope — e.g.
    /// the `HealthService` probe path that accepts a vault slug
    /// without a required organization slug in the request.
    pub fn resolve_vault_pair(
        &self,
        slug: VaultSlug,
    ) -> Result<(OrganizationId, VaultId), WireError> {
        self.state
            .resolve_vault_slug_to_pair(slug)
            .ok_or_else(|| not_found(format!("Vault with slug {} not found", slug.value())))
    }

    /// Reverse lookup: `(OrganizationId, VaultId)` to external slug.
    ///
    /// Returns `NOT_FOUND` if the pair has no associated slug. The tuple
    /// is required because `VaultId` alone does not uniquely identify a
    /// vault across organizations under per-org allocation.
    pub fn resolve_vault_slug(
        &self,
        organization: OrganizationId,
        id: VaultId,
    ) -> Result<VaultSlug, WireError> {
        self.state
            .resolve_vault_id_to_slug(organization, id)
            .ok_or_else(|| not_found(format!("Vault {}:{} not found", organization, id)))
    }

    /// Extracts a vault slug and resolves it to an internal `VaultId`.
    ///
    /// Combines [`extract_vault_slug`](Self::extract_vault_slug) and
    /// [`resolve_vault`](Self::resolve_vault) for the common case where a
    /// request carries a required vault slug and the caller has already
    /// resolved the organization independently.
    pub fn extract_and_resolve_vault(&self, slug: Option<VaultSlug>) -> Result<VaultId, WireError> {
        let slug = Self::extract_vault_slug(slug)?;
        self.resolve_vault(slug)
    }

    /// Extracts an optional vault slug and resolves it if present.
    ///
    /// Returns `Ok(None)` when the slug is absent. Returns an error
    /// only if the slug is present but zero or not found in the index.
    pub fn extract_and_resolve_vault_optional(
        &self,
        slug: Option<VaultSlug>,
    ) -> Result<Option<VaultId>, WireError> {
        match slug {
            None => Ok(None),
            Some(slug) if slug.value() == 0 => Err(invalid_argument("vault must be non-zero")),
            Some(slug) => self.resolve_vault(slug).map(Some),
        }
    }

    // =========================================================================
    // User slug resolution
    // =========================================================================

    /// Extracts and validates a user slug.
    ///
    /// Returns `INVALID_ARGUMENT` if the slug is missing or zero.
    pub fn extract_user_slug(slug: Option<UserSlug>) -> Result<UserSlug, WireError> {
        let slug = slug.ok_or_else(|| invalid_argument("Missing user"))?;
        if slug.value() == 0 {
            return Err(invalid_argument("user must be non-zero"));
        }
        Ok(slug)
    }

    /// Resolves a user slug to its internal ID.
    ///
    /// Returns `NOT_FOUND` if the slug is not registered.
    pub fn resolve_user(&self, slug: UserSlug) -> Result<UserId, WireError> {
        self.state
            .resolve_user_slug_to_id(slug)
            .ok_or_else(|| not_found(format!("User with slug {} not found", slug.value())))
    }

    /// Reverse lookup: internal user ID to external slug.
    ///
    /// Returns `NOT_FOUND` if the ID has no associated slug.
    pub fn resolve_user_slug(&self, id: UserId) -> Result<UserSlug, WireError> {
        self.state
            .resolve_user_id_to_slug(id)
            .ok_or_else(|| not_found(format!("User {} not found", id)))
    }

    /// Extracts a user slug and resolves it to an internal ID.
    ///
    /// Combines [`extract_user_slug`](Self::extract_user_slug) and
    /// [`resolve_user`](Self::resolve_user) for the common case where a
    /// request carries a required user slug.
    pub fn extract_and_resolve_user(&self, slug: Option<UserSlug>) -> Result<UserId, WireError> {
        let slug = Self::extract_user_slug(slug)?;
        self.resolve_user(slug)
    }

    /// Extracts an optional user slug and resolves it if present.
    ///
    /// Returns `Ok(None)` when the slug is absent. Returns an error
    /// only if the slug is present but zero or not found in the index.
    pub fn extract_and_resolve_user_optional(
        &self,
        slug: Option<UserSlug>,
    ) -> Result<Option<UserId>, WireError> {
        match slug {
            None => Ok(None),
            Some(slug) if slug.value() == 0 => Err(invalid_argument("user must be non-zero")),
            Some(slug) => self.resolve_user(slug).map(Some),
        }
    }

    // =========================================================================
    // Team slug resolution
    // =========================================================================

    /// Extracts and validates a team slug.
    ///
    /// Returns `INVALID_ARGUMENT` if the slug is missing or zero.
    pub fn extract_team_slug(slug: Option<TeamSlug>) -> Result<TeamSlug, WireError> {
        let slug = slug.ok_or_else(|| invalid_argument("Missing team"))?;
        if slug.value() == 0 {
            return Err(invalid_argument("team must be non-zero"));
        }
        Ok(slug)
    }

    /// Resolves a team slug to its internal (organization ID, team ID) pair.
    ///
    /// Returns `NOT_FOUND` if the slug is not registered.
    pub fn resolve_team(&self, slug: TeamSlug) -> Result<(OrganizationId, TeamId), WireError> {
        self.state
            .resolve_team_slug(slug)
            .ok_or_else(|| not_found(format!("Team with slug {} not found", slug.value())))
    }

    /// Extracts a team slug and resolves it to internal IDs.
    pub fn extract_and_resolve_team(
        &self,
        slug: Option<TeamSlug>,
    ) -> Result<(OrganizationId, TeamId), WireError> {
        let slug = Self::extract_team_slug(slug)?;
        self.resolve_team(slug)
    }

    /// Extracts an optional team slug and resolves it if present.
    pub fn extract_and_resolve_team_optional(
        &self,
        slug: Option<TeamSlug>,
    ) -> Result<Option<(OrganizationId, TeamId)>, WireError> {
        match slug {
            None => Ok(None),
            Some(slug) if slug.value() == 0 => Err(invalid_argument("team must be non-zero")),
            Some(slug) => self.resolve_team(slug).map(Some),
        }
    }

    // =========================================================================
    // App slug resolution
    // =========================================================================

    /// Extracts and validates an app slug.
    ///
    /// Returns `INVALID_ARGUMENT` if the slug is missing or zero.
    pub fn extract_app_slug(slug: Option<AppSlug>) -> Result<AppSlug, WireError> {
        let slug = slug.ok_or_else(|| invalid_argument("Missing app"))?;
        if slug.value() == 0 {
            return Err(invalid_argument("app must be non-zero"));
        }
        Ok(slug)
    }

    /// Resolves an app slug to its internal (organization ID, app ID) pair.
    ///
    /// Returns `NOT_FOUND` if the slug is not registered.
    pub fn resolve_app(&self, slug: AppSlug) -> Result<(OrganizationId, AppId), WireError> {
        self.state
            .resolve_app_slug(slug)
            .ok_or_else(|| not_found(format!("App with slug {} not found", slug.value())))
    }

    /// Reverse lookup: internal app ID to external slug.
    ///
    /// Returns `NOT_FOUND` if the ID has no associated slug.
    pub fn resolve_app_slug(&self, id: AppId) -> Result<AppSlug, WireError> {
        self.state
            .resolve_app_id_to_slug(id)
            .ok_or_else(|| not_found(format!("App {} not found", id)))
    }

    /// Extracts an app slug and resolves it to internal IDs.
    pub fn extract_and_resolve_app(
        &self,
        slug: Option<AppSlug>,
    ) -> Result<(OrganizationId, AppId), WireError> {
        let slug = Self::extract_app_slug(slug)?;
        self.resolve_app(slug)
    }

    /// Extracts an optional app slug and resolves it if present.
    ///
    /// Returns `Ok(None)` when the slug is absent. Returns an error
    /// only if the slug is present but zero or not found in the index.
    pub fn extract_and_resolve_app_optional(
        &self,
        slug: Option<AppSlug>,
    ) -> Result<Option<(OrganizationId, AppId)>, WireError> {
        match slug {
            None => Ok(None),
            Some(slug) if slug.value() == 0 => Err(invalid_argument("app must be non-zero")),
            Some(slug) => self.resolve_app(slug).map(Some),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use inferadb_ledger_raft::log_storage::AppliedState;

    use super::*;

    fn make_resolver(slug_entries: &[(u64, i64)]) -> SlugResolver {
        make_resolver_with_vaults(slug_entries, &[])
    }

    fn make_resolver_with_vaults(
        org_entries: &[(u64, i64)],
        vault_entries: &[(u64, i64)],
    ) -> SlugResolver {
        let mut state = AppliedState::default();
        for &(slug_val, org_id_val) in org_entries {
            let slug = OrganizationSlug::new(slug_val);
            let org_id = OrganizationId::new(org_id_val);
            state.slug_index.insert(slug, org_id);
            state.id_to_slug.insert(org_id, slug);
        }
        for &(slug_val, vault_id_val) in vault_entries {
            let slug = VaultSlug::new(slug_val);
            let vault_id = VaultId::new(vault_id_val);
            // Tests that only supply a vault without an owning org use
            // `OrganizationId::new(0)` (the system org) — the tuple
            // requirement post-γ means every fixture entry needs an
            // owner, but arbitrary test slugs default to system org.
            let org_id = OrganizationId::new(0);
            state.vault_slug_index.insert(slug, (org_id, vault_id));
            state.vault_id_to_slug.insert((org_id, vault_id), slug);
        }
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));
        SlugResolver::new(accessor)
    }

    #[test]
    fn extract_slug_valid() {
        let result = SlugResolver::extract_slug(Some(OrganizationSlug::new(42))).unwrap();
        assert_eq!(result.value(), 42);
    }

    #[test]
    fn extract_slug_missing_returns_invalid_argument() {
        let result = SlugResolver::extract_slug(None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("Missing"));
    }

    #[test]
    fn extract_slug_zero_returns_invalid_argument() {
        let result = SlugResolver::extract_slug(Some(OrganizationSlug::new(0)));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("non-zero"));
    }

    #[test]
    fn resolve_valid_slug() {
        let resolver = make_resolver(&[(100, 1)]);
        let slug = OrganizationSlug::new(100);
        let org_id = resolver.resolve(slug).unwrap();
        assert_eq!(org_id, OrganizationId::new(1));
    }

    #[test]
    fn resolve_unknown_slug_returns_not_found() {
        let resolver = make_resolver(&[(100, 1)]);
        let slug = OrganizationSlug::new(999);
        let result = resolver.resolve(slug);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert!(err.message.contains("999"));
    }

    #[test]
    fn resolve_slug_reverse_lookup() {
        let resolver = make_resolver(&[(100, 1)]);
        let slug = resolver.resolve_slug(OrganizationId::new(1)).unwrap();
        assert_eq!(slug.value(), 100);
    }

    #[test]
    fn resolve_slug_reverse_unknown_returns_not_found() {
        let resolver = make_resolver(&[(100, 1)]);
        let result = resolver.resolve_slug(OrganizationId::new(42));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[test]
    fn extract_and_resolve_valid() {
        let resolver = make_resolver(&[(42, 7)]);
        let org_id = resolver.extract_and_resolve(Some(OrganizationSlug::new(42))).unwrap();
        assert_eq!(org_id, OrganizationId::new(7));
    }

    #[test]
    fn extract_and_resolve_missing_slug() {
        let resolver = make_resolver(&[]);
        let result = resolver.extract_and_resolve(None);
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_zero_slug() {
        let resolver = make_resolver(&[]);
        let result = resolver.extract_and_resolve(Some(OrganizationSlug::new(0)));
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_unknown_slug() {
        let resolver = make_resolver(&[(100, 1)]);
        let result = resolver.extract_and_resolve(Some(OrganizationSlug::new(999)));
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn extract_and_resolve_optional_none() {
        let resolver = make_resolver(&[]);
        let result = resolver.extract_and_resolve_optional(None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn extract_and_resolve_optional_valid() {
        let resolver = make_resolver(&[(42, 7)]);
        let result =
            resolver.extract_and_resolve_optional(Some(OrganizationSlug::new(42))).unwrap();
        assert_eq!(result, Some(OrganizationId::new(7)));
    }

    #[test]
    fn extract_and_resolve_optional_zero() {
        let resolver = make_resolver(&[]);
        let result = resolver.extract_and_resolve_optional(Some(OrganizationSlug::new(0)));
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_optional_unknown() {
        let resolver = make_resolver(&[(100, 1)]);
        let result = resolver.extract_and_resolve_optional(Some(OrganizationSlug::new(999)));
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn multiple_slugs_resolved_independently() {
        let resolver = make_resolver(&[(100, 1), (200, 2), (300, 3)]);
        assert_eq!(resolver.resolve(OrganizationSlug::new(100)).unwrap(), OrganizationId::new(1));
        assert_eq!(resolver.resolve(OrganizationSlug::new(200)).unwrap(), OrganizationId::new(2));
        assert_eq!(resolver.resolve(OrganizationSlug::new(300)).unwrap(), OrganizationId::new(3));
    }

    #[test]
    fn bidirectional_resolution() {
        let resolver = make_resolver(&[(42, 7)]);
        let org_id = resolver.resolve(OrganizationSlug::new(42)).unwrap();
        let slug = resolver.resolve_slug(org_id).unwrap();
        assert_eq!(slug.value(), 42);
    }

    #[test]
    fn resolver_is_clone() {
        let resolver = make_resolver(&[(1, 1)]);
        let cloned = resolver.clone();
        assert_eq!(
            resolver.resolve(OrganizationSlug::new(1)).unwrap(),
            cloned.resolve(OrganizationSlug::new(1)).unwrap()
        );
    }

    // --- System organization bypass tests ---

    #[test]
    fn extract_and_resolve_for_events_system_slug() {
        let resolver = make_resolver(&[]);
        let org_id =
            resolver.extract_and_resolve_for_events(Some(OrganizationSlug::new(0))).unwrap();
        assert_eq!(org_id, OrganizationId::new(0));
    }

    #[test]
    fn extract_and_resolve_for_events_regular_slug() {
        let resolver = make_resolver(&[(42, 7)]);
        let org_id =
            resolver.extract_and_resolve_for_events(Some(OrganizationSlug::new(42))).unwrap();
        assert_eq!(org_id, OrganizationId::new(7));
    }

    #[test]
    fn extract_and_resolve_for_events_missing_slug() {
        let resolver = make_resolver(&[]);
        let result = resolver.extract_and_resolve_for_events(None);
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_for_events_unknown_slug() {
        let resolver = make_resolver(&[(100, 1)]);
        let result = resolver.extract_and_resolve_for_events(Some(OrganizationSlug::new(999)));
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }

    // --- Vault slug tests ---

    #[test]
    fn extract_vault_slug_valid() {
        let result = SlugResolver::extract_vault_slug(Some(VaultSlug::new(42))).unwrap();
        assert_eq!(result.value(), 42);
    }

    #[test]
    fn extract_vault_slug_missing_returns_invalid_argument() {
        let result = SlugResolver::extract_vault_slug(None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("Missing"));
    }

    #[test]
    fn extract_vault_slug_zero_returns_invalid_argument() {
        let result = SlugResolver::extract_vault_slug(Some(VaultSlug::new(0)));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("non-zero"));
    }

    #[test]
    fn resolve_vault_valid_slug() {
        let resolver = make_resolver_with_vaults(&[], &[(100, 1)]);
        let slug = VaultSlug::new(100);
        let vault_id = resolver.resolve_vault(slug).unwrap();
        assert_eq!(vault_id, VaultId::new(1));
    }

    #[test]
    fn resolve_vault_unknown_slug_returns_not_found() {
        let resolver = make_resolver_with_vaults(&[], &[(100, 1)]);
        let slug = VaultSlug::new(999);
        let result = resolver.resolve_vault(slug);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert!(err.message.contains("999"));
    }

    #[test]
    fn resolve_vault_slug_reverse_lookup() {
        let resolver = make_resolver_with_vaults(&[], &[(100, 1)]);
        let slug = resolver.resolve_vault_slug(OrganizationId::new(0), VaultId::new(1)).unwrap();
        assert_eq!(slug.value(), 100);
    }

    #[test]
    fn resolve_vault_slug_reverse_unknown_returns_not_found() {
        let resolver = make_resolver_with_vaults(&[], &[(100, 1)]);
        let result = resolver.resolve_vault_slug(OrganizationId::new(0), VaultId::new(42));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[test]
    fn extract_and_resolve_vault_valid() {
        let resolver = make_resolver_with_vaults(&[], &[(42, 7)]);
        let vault_id = resolver.extract_and_resolve_vault(Some(VaultSlug::new(42))).unwrap();
        assert_eq!(vault_id, VaultId::new(7));
    }

    #[test]
    fn extract_and_resolve_vault_missing_slug() {
        let resolver = make_resolver_with_vaults(&[], &[]);
        let result = resolver.extract_and_resolve_vault(None);
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_vault_zero_slug() {
        let resolver = make_resolver_with_vaults(&[], &[]);
        let result = resolver.extract_and_resolve_vault(Some(VaultSlug::new(0)));
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_vault_unknown_slug() {
        let resolver = make_resolver_with_vaults(&[], &[(100, 1)]);
        let result = resolver.extract_and_resolve_vault(Some(VaultSlug::new(999)));
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn extract_and_resolve_vault_optional_none() {
        let resolver = make_resolver_with_vaults(&[], &[]);
        let result = resolver.extract_and_resolve_vault_optional(None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn extract_and_resolve_vault_optional_valid() {
        let resolver = make_resolver_with_vaults(&[], &[(42, 7)]);
        let result = resolver.extract_and_resolve_vault_optional(Some(VaultSlug::new(42))).unwrap();
        assert_eq!(result, Some(VaultId::new(7)));
    }

    #[test]
    fn extract_and_resolve_vault_optional_zero() {
        let resolver = make_resolver_with_vaults(&[], &[]);
        let result = resolver.extract_and_resolve_vault_optional(Some(VaultSlug::new(0)));
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_vault_optional_unknown() {
        let resolver = make_resolver_with_vaults(&[], &[(100, 1)]);
        let result = resolver.extract_and_resolve_vault_optional(Some(VaultSlug::new(999)));
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn multiple_vault_slugs_resolved_independently() {
        let resolver = make_resolver_with_vaults(&[], &[(100, 1), (200, 2), (300, 3)]);
        assert_eq!(resolver.resolve_vault(VaultSlug::new(100)).unwrap(), VaultId::new(1));
        assert_eq!(resolver.resolve_vault(VaultSlug::new(200)).unwrap(), VaultId::new(2));
        assert_eq!(resolver.resolve_vault(VaultSlug::new(300)).unwrap(), VaultId::new(3));
    }

    #[test]
    fn vault_bidirectional_resolution() {
        let resolver = make_resolver_with_vaults(&[], &[(42, 7)]);
        let vault_id = resolver.resolve_vault(VaultSlug::new(42)).unwrap();
        // Test fixtures use OrganizationId(0) as the owning org; the
        // reverse lookup requires the tuple post-γ.
        let slug = resolver.resolve_vault_slug(OrganizationId::new(0), vault_id).unwrap();
        assert_eq!(slug.value(), 42);
    }

    #[test]
    fn org_and_vault_slugs_independent() {
        let resolver = make_resolver_with_vaults(&[(100, 1)], &[(200, 2)]);
        // Org resolution works
        assert_eq!(resolver.resolve(OrganizationSlug::new(100)).unwrap(), OrganizationId::new(1));
        // Vault resolution works
        assert_eq!(resolver.resolve_vault(VaultSlug::new(200)).unwrap(), VaultId::new(2));
        // Cross-type lookups don't interfere — vault slug doesn't resolve as org
        assert!(resolver.resolve(OrganizationSlug::new(200)).is_err());
        assert!(resolver.resolve_vault(VaultSlug::new(100)).is_err());
    }

    // =========================================================================
    // User slug tests
    // =========================================================================

    fn make_resolver_with_users(user_entries: &[(u64, i64)]) -> SlugResolver {
        let mut state = AppliedState::default();
        for &(slug_val, user_id_val) in user_entries {
            let slug = UserSlug::new(slug_val);
            let user_id = UserId::new(user_id_val);
            state.user_slug_index.insert(slug, user_id);
            state.user_id_to_slug.insert(user_id, slug);
        }
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));
        SlugResolver::new(accessor)
    }

    #[test]
    fn extract_user_slug_valid() {
        let result = SlugResolver::extract_user_slug(Some(UserSlug::new(42))).unwrap();
        assert_eq!(result.value(), 42);
    }

    #[test]
    fn extract_user_slug_missing_returns_invalid_argument() {
        let result = SlugResolver::extract_user_slug(None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("Missing"));
    }

    #[test]
    fn extract_user_slug_zero_returns_invalid_argument() {
        let result = SlugResolver::extract_user_slug(Some(UserSlug::new(0)));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("non-zero"));
    }

    #[test]
    fn resolve_user_valid_slug() {
        let resolver = make_resolver_with_users(&[(100, 1)]);
        let slug = UserSlug::new(100);
        let user_id = resolver.resolve_user(slug).unwrap();
        assert_eq!(user_id, UserId::new(1));
    }

    #[test]
    fn resolve_user_unknown_slug_returns_not_found() {
        let resolver = make_resolver_with_users(&[(100, 1)]);
        let slug = UserSlug::new(999);
        let result = resolver.resolve_user(slug);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert!(err.message.contains("999"));
    }

    #[test]
    fn resolve_user_slug_reverse_lookup() {
        let resolver = make_resolver_with_users(&[(100, 1)]);
        let slug = resolver.resolve_user_slug(UserId::new(1)).unwrap();
        assert_eq!(slug.value(), 100);
    }

    #[test]
    fn resolve_user_slug_reverse_unknown_returns_not_found() {
        let resolver = make_resolver_with_users(&[(100, 1)]);
        let result = resolver.resolve_user_slug(UserId::new(42));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[test]
    fn extract_and_resolve_user_valid() {
        let resolver = make_resolver_with_users(&[(42, 7)]);
        let user_id = resolver.extract_and_resolve_user(Some(UserSlug::new(42))).unwrap();
        assert_eq!(user_id, UserId::new(7));
    }

    #[test]
    fn extract_and_resolve_user_missing_slug() {
        let resolver = make_resolver_with_users(&[]);
        let result = resolver.extract_and_resolve_user(None);
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_user_zero_slug() {
        let resolver = make_resolver_with_users(&[]);
        let result = resolver.extract_and_resolve_user(Some(UserSlug::new(0)));
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_user_unknown_slug() {
        let resolver = make_resolver_with_users(&[(100, 1)]);
        let result = resolver.extract_and_resolve_user(Some(UserSlug::new(999)));
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn extract_and_resolve_user_optional_none() {
        let resolver = make_resolver_with_users(&[]);
        let result = resolver.extract_and_resolve_user_optional(None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn extract_and_resolve_user_optional_valid() {
        let resolver = make_resolver_with_users(&[(42, 7)]);
        let result = resolver.extract_and_resolve_user_optional(Some(UserSlug::new(42))).unwrap();
        assert_eq!(result, Some(UserId::new(7)));
    }

    #[test]
    fn extract_and_resolve_user_optional_zero() {
        let resolver = make_resolver_with_users(&[]);
        let result = resolver.extract_and_resolve_user_optional(Some(UserSlug::new(0)));
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_user_optional_unknown() {
        let resolver = make_resolver_with_users(&[(100, 1)]);
        let result = resolver.extract_and_resolve_user_optional(Some(UserSlug::new(999)));
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn org_vault_and_user_slugs_independent() {
        let mut state = AppliedState::default();
        state.slug_index.insert(OrganizationSlug::new(100), OrganizationId::new(1));
        state.id_to_slug.insert(OrganizationId::new(1), OrganizationSlug::new(100));
        let vault_org = OrganizationId::new(1);
        state.vault_slug_index.insert(VaultSlug::new(200), (vault_org, VaultId::new(2)));
        state.vault_id_to_slug.insert((vault_org, VaultId::new(2)), VaultSlug::new(200));
        state.user_slug_index.insert(UserSlug::new(300), UserId::new(3));
        state.user_id_to_slug.insert(UserId::new(3), UserSlug::new(300));
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));
        let resolver = SlugResolver::new(accessor);

        assert_eq!(resolver.resolve(OrganizationSlug::new(100)).unwrap(), OrganizationId::new(1));
        assert_eq!(resolver.resolve_vault(VaultSlug::new(200)).unwrap(), VaultId::new(2));
        assert_eq!(resolver.resolve_user(UserSlug::new(300)).unwrap(), UserId::new(3));
        // Cross-type lookups don't interfere
        assert!(resolver.resolve_user(UserSlug::new(100)).is_err());
        assert!(resolver.resolve_user(UserSlug::new(200)).is_err());
    }

    // =========================================================================
    // App slug resolution
    // =========================================================================

    fn make_resolver_with_apps(app_entries: &[(u64, i64, i64)]) -> SlugResolver {
        let mut state = AppliedState::default();
        for &(slug_val, org_id_val, app_id_val) in app_entries {
            let slug = AppSlug::new(slug_val);
            let org_id = OrganizationId::new(org_id_val);
            let app_id = AppId::new(app_id_val);
            state.app_slug_index.insert(slug, (org_id, app_id));
            state.app_id_to_slug.insert(app_id, slug);
        }
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(ArcSwap::from_pointee(state)));
        SlugResolver::new(accessor)
    }

    #[test]
    fn extract_app_slug_valid() {
        let result = SlugResolver::extract_app_slug(Some(AppSlug::new(42))).unwrap();
        assert_eq!(result.value(), 42);
    }

    #[test]
    fn extract_app_slug_missing_returns_invalid_argument() {
        let result = SlugResolver::extract_app_slug(None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("Missing"));
    }

    #[test]
    fn extract_app_slug_zero_returns_invalid_argument() {
        let result = SlugResolver::extract_app_slug(Some(AppSlug::new(0)));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("non-zero"));
    }

    #[test]
    fn resolve_app_valid_slug() {
        let resolver = make_resolver_with_apps(&[(100, 1, 10)]);
        let (org_id, app_id) = resolver.resolve_app(AppSlug::new(100)).unwrap();
        assert_eq!(org_id, OrganizationId::new(1));
        assert_eq!(app_id, AppId::new(10));
    }

    #[test]
    fn resolve_app_unknown_slug_returns_not_found() {
        let resolver = make_resolver_with_apps(&[(100, 1, 10)]);
        let result = resolver.resolve_app(AppSlug::new(999));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert!(err.message.contains("999"));
    }

    #[test]
    fn resolve_app_slug_reverse_lookup() {
        let resolver = make_resolver_with_apps(&[(100, 1, 10)]);
        let slug = resolver.resolve_app_slug(AppId::new(10)).unwrap();
        assert_eq!(slug.value(), 100);
    }

    #[test]
    fn resolve_app_slug_reverse_unknown_returns_not_found() {
        let resolver = make_resolver_with_apps(&[(100, 1, 10)]);
        let result = resolver.resolve_app_slug(AppId::new(999));
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn extract_and_resolve_app_valid() {
        let resolver = make_resolver_with_apps(&[(42, 1, 10)]);
        let (org_id, app_id) = resolver.extract_and_resolve_app(Some(AppSlug::new(42))).unwrap();
        assert_eq!(org_id, OrganizationId::new(1));
        assert_eq!(app_id, AppId::new(10));
    }

    #[test]
    fn extract_and_resolve_app_missing_slug() {
        let resolver = make_resolver_with_apps(&[]);
        let result = resolver.extract_and_resolve_app(None);
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_app_zero_slug() {
        let resolver = make_resolver_with_apps(&[]);
        let result = resolver.extract_and_resolve_app(Some(AppSlug::new(0)));
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_app_unknown_slug() {
        let resolver = make_resolver_with_apps(&[(42, 1, 10)]);
        let result = resolver.extract_and_resolve_app(Some(AppSlug::new(999)));
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn extract_and_resolve_app_optional_none() {
        let resolver = make_resolver_with_apps(&[]);
        let result = resolver.extract_and_resolve_app_optional(None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn extract_and_resolve_app_optional_valid() {
        let resolver = make_resolver_with_apps(&[(42, 1, 10)]);
        let result = resolver.extract_and_resolve_app_optional(Some(AppSlug::new(42))).unwrap();
        assert_eq!(result, Some((OrganizationId::new(1), AppId::new(10))));
    }

    #[test]
    fn extract_and_resolve_app_optional_zero() {
        let resolver = make_resolver_with_apps(&[]);
        let result = resolver.extract_and_resolve_app_optional(Some(AppSlug::new(0)));
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_and_resolve_app_optional_unknown() {
        let resolver = make_resolver_with_apps(&[(100, 1, 10)]);
        let result = resolver.extract_and_resolve_app_optional(Some(AppSlug::new(999)));
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }
}
