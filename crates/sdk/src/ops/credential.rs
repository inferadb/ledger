//! Credential CRUD, TOTP verification, and recovery code operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{UserCredentialId, UserSlug};

use crate::{
    LedgerClient,
    error::Result,
    proto_util::missing_response_field,
    retry::with_retry_cancellable,
    types::credential::{
        CredentialData, CredentialType, PasskeyCredentialInfo, RecoveryCodeResult,
        UserCredentialInfo,
    },
};

impl LedgerClient {
    // =========================================================================
    // Credential CRUD
    // =========================================================================

    /// Creates a new authentication credential for a user.
    ///
    /// The credential type is derived from the `data` variant. For TOTP
    /// credentials, the response includes the secret (one-time only).
    /// Subsequent reads will have the secret stripped.
    pub async fn create_user_credential(
        &self,
        user: UserSlug,
        name: impl Into<String>,
        data: CredentialData,
    ) -> Result<UserCredentialInfo> {
        self.check_shutdown(None)?;

        let name = name.into();
        let credential_type_i32 = CredentialType::from_data(&data).to_proto_i32();
        let proto_data = match &data {
            CredentialData::Passkey(pk) => {
                proto::create_user_credential_request::Data::Passkey(pk.to_proto())
            },
            CredentialData::Totp(totp) => {
                proto::create_user_credential_request::Data::Totp(totp.to_proto())
            },
            CredentialData::RecoveryCode(rc) => {
                proto::create_user_credential_request::Data::RecoveryCode(rc.to_proto())
            },
        };
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_user_credential",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_user_credential",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::CreateUserCredentialRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        credential_type: credential_type_i32,
                        name: name.clone(),
                        data: Some(proto_data.clone()),
                    };

                    let response = client
                        .create_user_credential(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response.credential.as_ref().map(UserCredentialInfo::from_proto).ok_or_else(
                        || missing_response_field("credential", "CreateUserCredentialResponse"),
                    )
                },
            ),
        )
        .await
    }

    /// Lists credentials for a user, optionally filtered by type.
    ///
    /// TOTP secrets are stripped from the response.
    pub async fn list_user_credentials(
        &self,
        user: UserSlug,
        credential_type: Option<CredentialType>,
    ) -> Result<Vec<UserCredentialInfo>> {
        self.check_shutdown(None)?;

        let type_filter = credential_type.map(|ct| ct.to_proto_i32());
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_user_credentials",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_user_credentials",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::ListUserCredentialsRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        credential_type: type_filter,
                    };

                    let response = client
                        .list_user_credentials(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(response.credentials.iter().map(UserCredentialInfo::from_proto).collect())
                },
            ),
        )
        .await
    }

    /// Updates credential metadata (name, enabled) or passkey-specific fields.
    ///
    /// TOTP and recovery code credentials are immutable after creation —
    /// only `name` and `enabled` can be changed for those types.
    pub async fn update_user_credential(
        &self,
        user: UserSlug,
        credential_id: UserCredentialId,
        name: Option<String>,
        enabled: Option<bool>,
        passkey_data: Option<PasskeyCredentialInfo>,
    ) -> Result<UserCredentialInfo> {
        self.check_shutdown(None)?;

        let passkey_proto = passkey_data.as_ref().map(|pk| pk.to_proto());
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_user_credential",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_user_credential",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::UpdateUserCredentialRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        credential_id: credential_id.value(),
                        name: name.clone(),
                        enabled,
                        passkey: passkey_proto.clone(),
                    };

                    let response = client
                        .update_user_credential(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    response.credential.as_ref().map(UserCredentialInfo::from_proto).ok_or_else(
                        || missing_response_field("credential", "UpdateUserCredentialResponse"),
                    )
                },
            ),
        )
        .await
    }

    /// Deletes a credential.
    ///
    /// Rejects if this is the user's last credential (safety guard).
    pub async fn delete_user_credential(
        &self,
        user: UserSlug,
        credential_id: UserCredentialId,
    ) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_user_credential",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_user_credential",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::DeleteUserCredentialRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        credential_id: credential_id.value(),
                    };

                    client.delete_user_credential(tonic::Request::new(request)).await?;

                    Ok(())
                },
            ),
        )
        .await
    }

    // =========================================================================
    // TOTP Challenge / Verification
    // =========================================================================

    /// Creates a TOTP challenge after passkey authentication.
    ///
    /// Called by trusted services that have already verified passkey auth.
    /// Returns a 32-byte challenge nonce for the subsequent `verify_totp` call.
    pub async fn create_totp_challenge(
        &self,
        user: UserSlug,
        primary_method: impl Into<String>,
    ) -> Result<Vec<u8>> {
        self.check_shutdown(None)?;

        let primary_method = primary_method.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_totp_challenge",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_totp_challenge",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::CreateTotpChallengeRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        primary_method: primary_method.clone(),
                    };

                    let response = client
                        .create_totp_challenge(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(response.challenge_nonce)
                },
            ),
        )
        .await
    }

    /// Verifies a TOTP code against a pending challenge.
    ///
    /// On success, atomically consumes the challenge and creates a user session.
    /// Returns the token pair (access + refresh) directly.
    pub async fn verify_totp(
        &self,
        user: UserSlug,
        totp_code: impl Into<String>,
        challenge_nonce: Vec<u8>,
    ) -> Result<crate::token::TokenPair> {
        self.check_shutdown(None)?;

        let totp_code = totp_code.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "verify_totp",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "verify_totp",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::VerifyTotpRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        totp_code: totp_code.clone(),
                        challenge_nonce: challenge_nonce.clone(),
                        credential_used: None,
                    };

                    let response =
                        client.verify_totp(tonic::Request::new(request)).await?.into_inner();

                    let tokens = response
                        .tokens
                        .ok_or_else(|| missing_response_field("tokens", "VerifyTotpResponse"))?;

                    Ok(crate::token::TokenPair::from_proto(tokens))
                },
            ),
        )
        .await
    }

    // =========================================================================
    // Recovery Code
    // =========================================================================

    /// Consumes a recovery code to bypass TOTP verification.
    ///
    /// Atomically removes the code hash and creates a session.
    /// Returns the token pair and the number of remaining unused codes.
    pub async fn consume_recovery_code(
        &self,
        user: UserSlug,
        code: impl Into<String>,
        challenge_nonce: Vec<u8>,
    ) -> Result<RecoveryCodeResult> {
        self.check_shutdown(None)?;

        let code = code.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "consume_recovery_code",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "consume_recovery_code",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::ConsumeRecoveryCodeRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        code: code.clone(),
                        challenge_nonce: challenge_nonce.clone(),
                        credential_used: None,
                    };

                    let response = client
                        .consume_recovery_code(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    let tokens = response.tokens.ok_or_else(|| {
                        missing_response_field("tokens", "ConsumeRecoveryCodeResponse")
                    })?;

                    Ok(RecoveryCodeResult {
                        tokens: crate::token::TokenPair::from_proto(tokens),
                        remaining_codes: response.remaining_codes,
                    })
                },
            ),
        )
        .await
    }
}
