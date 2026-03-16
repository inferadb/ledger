//! Token service operations: sessions, validation, refresh, and signing keys.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{AppSlug, OrganizationSlug, UserSlug, VaultSlug};

use crate::{
    LedgerClient, error::Result, proto_util::missing_response_field, retry::with_retry_cancellable,
};

impl LedgerClient {
    // =========================================================================
    // Token Service
    // =========================================================================

    /// Creates a user session (access + refresh token pair).
    pub async fn create_user_session(&self, user: UserSlug) -> Result<crate::token::TokenPair> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_user_session",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_user_session",
                || async {
                    let mut client = crate::connected_client!(pool, create_token_client);

                    let request = proto::CreateUserSessionRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        credential_used: None,
                    };

                    let response = client
                        .create_user_session(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let tokens = response
                        .tokens
                        .ok_or_else(|| missing_response_field("tokens", "TokenResponse"))?;

                    Ok(crate::token::TokenPair::from_proto(tokens))
                },
            ),
        )
        .await
    }

    /// Validates an access token and returns parsed claims.
    pub async fn validate_token(
        &self,
        token: &str,
        expected_audience: &str,
    ) -> Result<crate::token::ValidatedToken> {
        self.check_shutdown(None)?;

        let token = token.to_owned();
        let audience = expected_audience.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "validate_token",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "validate_token",
                || async {
                    let mut client = crate::connected_client!(pool, create_token_client);

                    let request = proto::ValidateTokenRequest {
                        token: token.clone(),
                        expected_audience: audience.clone(),
                    };

                    let response =
                        client.validate_token(tonic::Request::new(request)).await?.into_inner();

                    crate::token::ValidatedToken::from_proto(response)
                        .ok_or_else(|| missing_response_field("claims", "ValidateTokenResponse"))
                },
            ),
        )
        .await
    }

    /// Revokes all sessions for a user.
    ///
    /// Returns the number of sessions revoked.
    pub async fn revoke_all_user_sessions(&self, user: UserSlug) -> Result<u64> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "revoke_all_user_sessions",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "revoke_all_user_sessions",
                || async {
                    let mut client = crate::connected_client!(pool, create_token_client);

                    let request = proto::RevokeAllUserSessionsRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response = client
                        .revoke_all_user_sessions(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(response.revoked_count)
                },
            ),
        )
        .await
    }

    /// Revokes all sessions for an app.
    ///
    /// Returns the number of sessions revoked.
    pub async fn revoke_all_app_sessions(&self, app: AppSlug) -> Result<u64> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "revoke_all_app_sessions",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "revoke_all_app_sessions",
                || async {
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
                },
            ),
        )
        .await
    }

    /// Refreshes a token pair using a refresh token.
    ///
    /// The old refresh token is invalidated (rotate-on-use).
    pub async fn refresh_token(&self, refresh_token: &str) -> Result<crate::token::TokenPair> {
        self.check_shutdown(None)?;

        let refresh = refresh_token.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "refresh_token",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "refresh_token",
                || async {
                    let mut client = crate::connected_client!(pool, create_token_client);

                    let request = proto::RefreshTokenRequest { refresh_token: refresh.clone() };

                    let response =
                        client.refresh_token(tonic::Request::new(request)).await?.into_inner();

                    let tokens = response
                        .tokens
                        .ok_or_else(|| missing_response_field("tokens", "TokenResponse"))?;

                    Ok(crate::token::TokenPair::from_proto(tokens))
                },
            ),
        )
        .await
    }

    /// Revokes a token and its entire family.
    pub async fn revoke_token(&self, refresh_token: &str) -> Result<()> {
        self.check_shutdown(None)?;

        let refresh = refresh_token.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "revoke_token",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "revoke_token",
                || async {
                    let mut client = crate::connected_client!(pool, create_token_client);

                    let request = proto::RevokeTokenRequest { refresh_token: refresh.clone() };

                    client.revoke_token(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let scopes = scopes.to_vec();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_vault_token",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_vault_token",
                || async {
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
                },
            ),
        )
        .await
    }

    /// Creates a new signing key for the given scope.
    pub async fn create_signing_key(
        &self,
        scope: &str,
        organization: Option<OrganizationSlug>,
    ) -> Result<crate::token::PublicKeyInfo> {
        self.check_shutdown(None)?;

        let scope_str = scope.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_signing_key",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_signing_key",
                || async {
                    let mut client = crate::connected_client!(pool, create_token_client);

                    let scope_i32 = match scope_str.as_str() {
                        "global" => proto::SigningKeyScope::Global as i32,
                        "organization" => proto::SigningKeyScope::Organization as i32,
                        _ => proto::SigningKeyScope::Unspecified as i32,
                    };

                    let request = proto::CreateSigningKeyRequest {
                        scope: scope_i32,
                        organization: organization
                            .map(|o| proto::OrganizationSlug { slug: o.value() }),
                    };

                    let response =
                        client.create_signing_key(tonic::Request::new(request)).await?.into_inner();

                    let key = response
                        .key
                        .ok_or_else(|| missing_response_field("key", "CreateSigningKeyResponse"))?;

                    Ok(crate::token::PublicKeyInfo::from_proto(key))
                },
            ),
        )
        .await
    }

    /// Rotates a signing key, creating a new key and marking the old one as rotated.
    pub async fn rotate_signing_key(
        &self,
        kid: &str,
        grace_period_secs: Option<u64>,
    ) -> Result<crate::token::PublicKeyInfo> {
        self.check_shutdown(None)?;

        let kid = kid.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "rotate_signing_key",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "rotate_signing_key",
                || async {
                    let mut client = crate::connected_client!(pool, create_token_client);

                    let request = proto::RotateSigningKeyRequest {
                        kid: kid.clone(),
                        grace_period_secs: grace_period_secs.unwrap_or(0),
                    };

                    let response =
                        client.rotate_signing_key(tonic::Request::new(request)).await?.into_inner();

                    let key = response.new_key.ok_or_else(|| {
                        missing_response_field("new_key", "RotateSigningKeyResponse")
                    })?;

                    Ok(crate::token::PublicKeyInfo::from_proto(key))
                },
            ),
        )
        .await
    }

    /// Revokes a signing key by its kid.
    pub async fn revoke_signing_key(&self, kid: &str) -> Result<()> {
        self.check_shutdown(None)?;

        let kid = kid.to_owned();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "revoke_signing_key",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "revoke_signing_key",
                || async {
                    let mut client = crate::connected_client!(pool, create_token_client);

                    let request = proto::RevokeSigningKeyRequest { kid: kid.clone() };

                    client.revoke_signing_key(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
        .await
    }

    /// Gets active public keys for token verification.
    pub async fn get_public_keys(
        &self,
        organization: Option<OrganizationSlug>,
    ) -> Result<Vec<crate::token::PublicKeyInfo>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_public_keys",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_public_keys",
                || async {
                    let mut client = crate::connected_client!(pool, create_token_client);

                    let request = proto::GetPublicKeysRequest {
                        organization: organization
                            .map(|o| proto::OrganizationSlug { slug: o.value() }),
                    };

                    let response =
                        client.get_public_keys(tonic::Request::new(request)).await?.into_inner();

                    Ok(response
                        .keys
                        .into_iter()
                        .map(crate::token::PublicKeyInfo::from_proto)
                        .collect())
                },
            ),
        )
        .await
    }
}
