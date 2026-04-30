//! Credential CRUD, TOTP verification, and recovery code operations.

use inferadb_ledger_types::{UserCredentialId, UserSlug};

use crate::{
    LedgerClient,
    error::Result,
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
        caller: UserSlug,
        user: UserSlug,
        name: impl Into<String>,
        data: CredentialData,
    ) -> Result<UserCredentialInfo> {
        let name = name.into();
        let pool = self.pool.clone();
        self.call_with_retry("create_user_credential", || {
            let pool = pool.clone();
            let name = name.clone();
            let data = data.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::credential::create_user_credential(
                    wire_client,
                    request_id,
                    caller,
                    user,
                    name,
                    data,
                )
                .await
            }
        })
        .await
    }

    /// Lists credentials for a user, optionally filtered by type.
    ///
    /// TOTP secrets are stripped from the response.
    pub async fn list_user_credentials(
        &self,
        caller: UserSlug,
        user: UserSlug,
        credential_type: Option<CredentialType>,
    ) -> Result<Vec<UserCredentialInfo>> {
        let pool = self.pool.clone();
        self.call_with_retry("list_user_credentials", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::credential::list_user_credentials(
                    wire_client,
                    request_id,
                    caller,
                    user,
                    credential_type,
                )
                .await
            }
        })
        .await
    }

    /// Updates credential metadata (name, enabled) or passkey-specific fields.
    ///
    /// TOTP and recovery code credentials are immutable after creation —
    /// only `name` and `enabled` can be changed for those types.
    pub async fn update_user_credential(
        &self,
        caller: UserSlug,
        user: UserSlug,
        credential_id: UserCredentialId,
        name: Option<String>,
        enabled: Option<bool>,
        passkey_data: Option<PasskeyCredentialInfo>,
    ) -> Result<UserCredentialInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("update_user_credential", || {
            let pool = pool.clone();
            let name = name.clone();
            let passkey_data = passkey_data.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::credential::update_user_credential(
                    wire_client,
                    request_id,
                    caller,
                    user,
                    credential_id,
                    name,
                    enabled,
                    passkey_data,
                )
                .await
            }
        })
        .await
    }

    /// Deletes a credential.
    ///
    /// Rejects if this is the user's last credential (safety guard).
    pub async fn delete_user_credential(
        &self,
        caller: UserSlug,
        user: UserSlug,
        credential_id: UserCredentialId,
    ) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("delete_user_credential", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::credential::delete_user_credential(
                    wire_client,
                    request_id,
                    caller,
                    user,
                    credential_id,
                )
                .await
            }
        })
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
        caller: UserSlug,
        user: UserSlug,
        primary_method: impl Into<String>,
    ) -> Result<Vec<u8>> {
        let primary_method = primary_method.into();
        let pool = self.pool.clone();
        self.call_with_retry("create_totp_challenge", || {
            let pool = pool.clone();
            let primary_method = primary_method.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::credential::create_totp_challenge(
                    wire_client,
                    request_id,
                    caller,
                    user,
                    primary_method,
                )
                .await
            }
        })
        .await
    }

    /// Verifies a TOTP code against a pending challenge.
    ///
    /// On success, atomically consumes the challenge and creates a user session.
    /// Returns the token pair (access + refresh) directly.
    pub async fn verify_totp(
        &self,
        caller: UserSlug,
        user: UserSlug,
        totp_code: impl Into<String>,
        challenge_nonce: Vec<u8>,
    ) -> Result<crate::token::TokenPair> {
        let totp_code = totp_code.into();
        let pool = self.pool.clone();
        self.call_with_retry("verify_totp", || {
            let pool = pool.clone();
            let challenge_nonce = challenge_nonce.clone();
            let totp_code = totp_code.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::credential::verify_totp(
                    wire_client,
                    request_id,
                    caller,
                    user,
                    totp_code,
                    challenge_nonce,
                )
                .await
            }
        })
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
        caller: UserSlug,
        user: UserSlug,
        code: impl Into<String>,
        challenge_nonce: Vec<u8>,
    ) -> Result<RecoveryCodeResult> {
        let code = code.into();
        let pool = self.pool.clone();
        self.call_with_retry("consume_recovery_code", || {
            let pool = pool.clone();
            let challenge_nonce = challenge_nonce.clone();
            let code = code.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::credential::consume_recovery_code(
                    wire_client,
                    request_id,
                    caller,
                    user,
                    code,
                    challenge_nonce,
                )
                .await
            }
        })
        .await
    }
}
