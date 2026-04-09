//! Token service operations: sessions, validation, refresh, and signing keys.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{AppSlug, OrganizationSlug, UserSlug, VaultSlug};

use crate::{LedgerClient, error::Result, proto_util::missing_response_field};

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
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::CreateUserSessionRequest {
                    user: Some(proto::UserSlug { slug: user.value() }),
                    credential_used: None,
                    caller: Some(proto::UserSlug { slug: user.value() }),
                };

                let response =
                    client.create_user_session(tonic::Request::new(request)).await?.into_inner();

                let tokens = response
                    .tokens
                    .ok_or_else(|| missing_response_field("tokens", "TokenResponse"))?;

                Ok(crate::token::TokenPair::from_proto(tokens))
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
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::ValidateTokenRequest {
                    token: token.clone(),
                    expected_audience: audience.clone(),
                };

                let response =
                    client.validate_token(tonic::Request::new(request)).await?.into_inner();

                crate::token::ValidatedToken::from_proto(response)
                    .ok_or_else(|| missing_response_field("claims", "ValidateTokenResponse"))
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
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::RevokeAllUserSessionsRequest {
                    user: Some(proto::UserSlug { slug: user.value() }),
                    caller: Some(proto::UserSlug { slug: user.value() }),
                };

                let response = client
                    .revoke_all_user_sessions(tonic::Request::new(request))
                    .await?
                    .into_inner();

                Ok(response.revoked_count)
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
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::RevokeAllAppSessionsRequest {
                    organization: None,
                    app: Some(proto::AppSlug { slug: app.value() }),
                };

                let response = client
                    .revoke_all_app_sessions(tonic::Request::new(request))
                    .await?
                    .into_inner();

                Ok(response.revoked_count)
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
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::RefreshTokenRequest { refresh_token: refresh.clone() };

                let response =
                    client.refresh_token(tonic::Request::new(request)).await?.into_inner();

                let tokens = response
                    .tokens
                    .ok_or_else(|| missing_response_field("tokens", "TokenResponse"))?;

                Ok(crate::token::TokenPair::from_proto(tokens))
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
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::RevokeTokenRequest { refresh_token: refresh.clone() };

                client.revoke_token(tonic::Request::new(request)).await?;

                Ok(())
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
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::CreateVaultTokenRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    app: Some(proto::AppSlug { slug: app.value() }),
                    vault: Some(proto::VaultSlug { slug: vault.value() }),
                    scopes: scopes.clone(),
                };

                let response =
                    client.create_vault_token(tonic::Request::new(request)).await?.into_inner();

                let tokens = response
                    .tokens
                    .ok_or_else(|| missing_response_field("tokens", "TokenResponse"))?;

                Ok(crate::token::TokenPair::from_proto(tokens))
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
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::AuthenticateClientAssertionRequest {
                    organization: Some(proto::OrganizationSlug { slug: organization.value() }),
                    vault: Some(proto::VaultSlug { slug: vault.value() }),
                    assertion_jwt: assertion_jwt.clone(),
                    scopes: scopes.clone(),
                };

                let response = client
                    .authenticate_client_assertion(tonic::Request::new(request))
                    .await?
                    .into_inner();

                let tokens = response.tokens.ok_or_else(|| {
                    missing_response_field("tokens", "AuthenticateClientAssertionResponse")
                })?;

                Ok(crate::token::TokenPair::from_proto(tokens))
            }
        })
        .await
    }

    /// Creates a new signing key for the given scope.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
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
                let mut client = crate::connected_client!(pool, create_token_client);

                let scope_i32 = match scope_str.as_str() {
                    "global" => proto::SigningKeyScope::Global as i32,
                    "organization" => proto::SigningKeyScope::Organization as i32,
                    _ => proto::SigningKeyScope::Unspecified as i32,
                };

                let request = proto::CreateSigningKeyRequest {
                    scope: scope_i32,
                    organization: organization.map(|o| proto::OrganizationSlug { slug: o.value() }),
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                let response =
                    client.create_signing_key(tonic::Request::new(request)).await?.into_inner();

                let key = response
                    .key
                    .ok_or_else(|| missing_response_field("key", "CreateSigningKeyResponse"))?;

                Ok(crate::token::PublicKeyInfo::from_proto(key))
            }
        })
        .await
    }

    /// Rotates a signing key, creating a new key and marking the old one as rotated.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
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
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::RotateSigningKeyRequest {
                    kid: kid.clone(),
                    grace_period_secs: grace_period_secs.unwrap_or(0),
                    force_revoke,
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                let response =
                    client.rotate_signing_key(tonic::Request::new(request)).await?.into_inner();

                let key = response
                    .new_key
                    .ok_or_else(|| missing_response_field("new_key", "RotateSigningKeyResponse"))?;

                Ok(crate::token::PublicKeyInfo::from_proto(key))
            }
        })
        .await
    }

    /// Revokes a signing key by its kid.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    pub async fn revoke_signing_key(&self, caller: UserSlug, kid: &str) -> Result<()> {
        let kid = kid.to_owned();
        let pool = self.pool.clone();
        self.call_with_retry("revoke_signing_key", || {
            let pool = pool.clone();
            let kid = kid.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::RevokeSigningKeyRequest {
                    kid: kid.clone(),
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                client.revoke_signing_key(tonic::Request::new(request)).await?;

                Ok(())
            }
        })
        .await
    }

    /// Gets active public keys for token verification.
    ///
    /// # Arguments
    ///
    /// * `caller` - Identity of the user performing this operation (external slug).
    pub async fn get_public_keys(
        &self,
        caller: UserSlug,
        organization: Option<OrganizationSlug>,
    ) -> Result<Vec<crate::token::PublicKeyInfo>> {
        let pool = self.pool.clone();
        self.call_with_retry("get_public_keys", || {
            let pool = pool.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_token_client);

                let request = proto::GetPublicKeysRequest {
                    organization: organization.map(|o| proto::OrganizationSlug { slug: o.value() }),
                    caller: Some(proto::UserSlug { slug: caller.value() }),
                };

                let response =
                    client.get_public_keys(tonic::Request::new(request)).await?.into_inner();

                Ok(response.keys.into_iter().map(crate::token::PublicKeyInfo::from_proto).collect())
            }
        })
        .await
    }
}
