//! Token service operations: sessions, validation, refresh, and signing keys.

use inferadb_ledger_types::{AppSlug, OrganizationSlug, UserSlug, VaultSlug};

use crate::{LedgerClient, error::Result};

impl LedgerClient {
    // =========================================================================
    // Token Service
    // =========================================================================

    /// Creates a user session (access + refresh token pair).
    pub async fn create_user_session(&self, user: UserSlug) -> Result<crate::token::TokenPair> {
        let pool = self.pool.clone();
        self.call_with_retry("create_user_session", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::create_user_session(wire_client, request_id, user).await
            }
        })
        .await
    }

    /// Validates an access token and returns parsed claims.
    pub async fn validate_token(
        &self,
        token: &str,
        expected_audience: &str,
    ) -> Result<crate::token::ValidatedToken> {
        let token = token.to_owned();
        let audience = expected_audience.to_owned();
        let pool = self.pool.clone();
        self.call_with_retry("validate_token", || {
            let pool = pool.clone();
            let audience = audience.clone();
            let token = token.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::validate_token(wire_client, request_id, token, audience)
                    .await
            }
        })
        .await
    }

    /// Revokes all sessions for a user.
    ///
    /// Returns the number of sessions revoked.
    pub async fn revoke_all_user_sessions(&self, user: UserSlug) -> Result<u64> {
        let pool = self.pool.clone();
        self.call_with_retry("revoke_all_user_sessions", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::revoke_all_user_sessions(wire_client, request_id, user)
                    .await
            }
        })
        .await
    }

    /// Revokes all sessions for an app.
    ///
    /// Returns the number of sessions revoked.
    pub async fn revoke_all_app_sessions(&self, app: AppSlug) -> Result<u64> {
        let pool = self.pool.clone();
        self.call_with_retry("revoke_all_app_sessions", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::revoke_all_app_sessions(wire_client, request_id, app).await
            }
        })
        .await
    }

    /// Refreshes a token pair using a refresh token.
    ///
    /// The old refresh token is invalidated (rotate-on-use).
    pub async fn refresh_token(&self, refresh_token: &str) -> Result<crate::token::TokenPair> {
        let refresh = refresh_token.to_owned();
        let pool = self.pool.clone();
        self.call_with_retry("refresh_token", || {
            let pool = pool.clone();
            let refresh = refresh.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::refresh_token(wire_client, request_id, refresh).await
            }
        })
        .await
    }

    /// Revokes a token and its entire family.
    pub async fn revoke_token(&self, refresh_token: &str) -> Result<()> {
        let refresh = refresh_token.to_owned();
        let pool = self.pool.clone();
        self.call_with_retry("revoke_token", || {
            let pool = pool.clone();
            let refresh = refresh.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::revoke_token(wire_client, request_id, refresh).await
            }
        })
        .await
    }

    /// Creates a vault access token for an app.
    pub async fn create_vault_token(
        &self,
        organization: OrganizationSlug,
        app: AppSlug,
        vault: VaultSlug,
        scopes: &[String],
    ) -> Result<crate::token::TokenPair> {
        let scopes = scopes.to_vec();
        let pool = self.pool.clone();
        self.call_with_retry("create_vault_token", || {
            let pool = pool.clone();
            let scopes = scopes.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::create_vault_token(
                    wire_client,
                    request_id,
                    organization,
                    app,
                    vault,
                    scopes,
                )
                .await
            }
        })
        .await
    }

    /// Authenticates a client assertion JWT and returns a vault access token.
    ///
    /// Ledger verifies the JWT signature against the app's registered client
    /// assertion public keys, validates claims (iss, sub, exp, aud), and issues
    /// a scoped vault token if the app is authorized.
    pub async fn authenticate_client_assertion(
        &self,
        organization: OrganizationSlug,
        vault: VaultSlug,
        assertion_jwt: &str,
        scopes: &[String],
    ) -> Result<crate::token::TokenPair> {
        let assertion_jwt = assertion_jwt.to_owned();
        let scopes = scopes.to_vec();
        let pool = self.pool.clone();
        self.call_with_retry("authenticate_client_assertion", || {
            let pool = pool.clone();
            let assertion_jwt = assertion_jwt.clone();
            let scopes = scopes.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::authenticate_client_assertion(
                    wire_client,
                    request_id,
                    organization,
                    vault,
                    assertion_jwt,
                    scopes,
                )
                .await
            }
        })
        .await
    }

    /// Creates a new signing key for the given scope.
    ///
    /// `scope` must be `"global"` or `"organization"`. Supply `organization`
    /// when `scope` is `"organization"`; it is ignored for `"global"` keys.
    pub async fn create_signing_key(
        &self,
        caller: UserSlug,
        scope: &str,
        organization: Option<OrganizationSlug>,
    ) -> Result<crate::token::PublicKeyInfo> {
        let scope_str = scope.to_owned();
        let pool = self.pool.clone();
        self.call_with_retry("create_signing_key", || {
            let pool = pool.clone();
            let scope_str = scope_str.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::create_signing_key(
                    wire_client,
                    request_id,
                    caller,
                    &scope_str,
                    organization,
                )
                .await
            }
        })
        .await
    }

    /// Rotates a signing key, creating a new key and marking the old one as rotated.
    ///
    /// `kid` identifies the key to rotate. `grace_period_secs` controls how
    /// long the old key remains valid for verification after rotation (0 =
    /// immediate). Set `force_revoke` to `true` to revoke the old key
    /// immediately regardless of grace period.
    pub async fn rotate_signing_key(
        &self,
        caller: UserSlug,
        kid: &str,
        grace_period_secs: Option<u64>,
        force_revoke: bool,
    ) -> Result<crate::token::PublicKeyInfo> {
        let kid = kid.to_owned();
        let pool = self.pool.clone();
        self.call_with_retry("rotate_signing_key", || {
            let pool = pool.clone();
            let kid = kid.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::rotate_signing_key(
                    wire_client,
                    request_id,
                    caller,
                    kid,
                    grace_period_secs,
                    force_revoke,
                )
                .await
            }
        })
        .await
    }

    /// Revokes a signing key by its `kid`.
    pub async fn revoke_signing_key(&self, caller: UserSlug, kid: &str) -> Result<()> {
        let kid = kid.to_owned();
        let pool = self.pool.clone();
        self.call_with_retry("revoke_signing_key", || {
            let pool = pool.clone();
            let kid = kid.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::revoke_signing_key(wire_client, request_id, caller, kid)
                    .await
            }
        })
        .await
    }

    /// Gets active public keys for token verification.
    ///
    /// Supply `organization` to retrieve organization-scoped keys; pass `None`
    /// for global keys.
    pub async fn get_public_keys(
        &self,
        caller: UserSlug,
        organization: Option<OrganizationSlug>,
    ) -> Result<Vec<crate::token::PublicKeyInfo>> {
        let pool = self.pool.clone();
        self.call_with_retry("get_public_keys", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::token::get_public_keys(
                    wire_client,
                    request_id,
                    caller,
                    organization,
                )
                .await
            }
        })
        .await
    }
}
