//! Onboarding operations (email verification and registration).

use inferadb_ledger_types::Region;

use crate::{
    LedgerClient,
    error::Result,
    types::admin::{EmailVerificationCode, EmailVerificationResult, RegistrationResult},
};

impl LedgerClient {
    // =========================================================================
    // Onboarding (Email Verification + Registration)
    // =========================================================================

    /// Initiates email verification by generating a code.
    ///
    /// The returned code should be sent to the user's email out-of-band.
    pub async fn initiate_email_verification(
        &self,
        email: impl Into<String>,
        region: Region,
    ) -> Result<EmailVerificationCode> {
        let email = email.into();
        let pool = self.pool.clone();
        self.call_with_retry("initiate_email_verification", || {
            let pool = pool.clone();
            let email = email.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::onboarding::initiate_email_verification(
                    wire_client,
                    request_id,
                    email,
                    region,
                )
                .await
            }
        })
        .await
    }

    /// Verifies the code the user received via email.
    ///
    /// Returns either a session for an existing user or an onboarding token
    /// for a new user who must complete registration.
    pub async fn verify_email_code(
        &self,
        email: impl Into<String>,
        code: impl Into<String>,
        region: Region,
    ) -> Result<EmailVerificationResult> {
        let email = email.into();
        let code = code.into();
        let pool = self.pool.clone();
        self.call_with_retry("verify_email_code", || {
            let pool = pool.clone();
            let code = code.clone();
            let email = email.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::onboarding::verify_email_code(
                    wire_client,
                    request_id,
                    email,
                    code,
                    region,
                )
                .await
            }
        })
        .await
    }

    /// Completes registration for a new user after email verification.
    ///
    /// Requires the onboarding token from [`verify_email_code`](Self::verify_email_code).
    pub async fn complete_registration(
        &self,
        onboarding_token: impl Into<String>,
        email: impl Into<String>,
        region: Region,
        name: impl Into<String>,
        organization_name: impl Into<String>,
    ) -> Result<RegistrationResult> {
        let onboarding_token = onboarding_token.into();
        let email = email.into();
        let name = name.into();
        let organization_name = organization_name.into();
        let pool = self.pool.clone();
        self.call_with_retry("complete_registration", || {
            let pool = pool.clone();
            let email = email.clone();
            let name = name.clone();
            let onboarding_token = onboarding_token.clone();
            let organization_name = organization_name.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::onboarding::complete_registration(
                    wire_client,
                    request_id,
                    onboarding_token,
                    email,
                    region,
                    name,
                    organization_name,
                )
                .await
            }
        })
        .await
    }
}
