//! Onboarding operations (email verification and registration).

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, Region, UserSlug};

use crate::{
    LedgerClient,
    error::{self, Result},
    proto_util::missing_response_field,
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
        let region_slug = region.as_str().to_string();
        let pool = self.pool.clone();
        self.call_with_retry("initiate_email_verification", || {
            let pool = pool.clone();
            let email = email.clone();
            let region_slug = region_slug.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_user_client);

                let request = proto::InitiateEmailVerificationRequest {
                    email: email.clone(),
                    region: region_slug,
                };

                let response = client.initiate_email_verification(request).await?.into_inner();

                Ok(EmailVerificationCode { code: response.code })
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
        let region_slug = region.as_str().to_string();
        let pool = self.pool.clone();
        self.call_with_retry("verify_email_code", || {
            let pool = pool.clone();
            let code = code.clone();
            let email = email.clone();
            let region_slug = region_slug.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_user_client);

                let request = proto::VerifyEmailCodeRequest {
                    email: email.clone(),
                    code: code.clone(),
                    region: region_slug,
                };

                let response = client.verify_email_code(request).await?.into_inner();

                match response.result {
                    Some(proto::verify_email_code_response::Result::ExistingUser(existing)) => {
                        let user_slug =
                            existing.user.map(|u| UserSlug::new(u.slug)).ok_or_else(|| {
                                missing_response_field("user", "VerifyEmailCodeResponse")
                            })?;
                        let session =
                            existing.session.map(crate::token::TokenPair::from_proto).ok_or_else(
                                || missing_response_field("session", "VerifyEmailCodeResponse"),
                            )?;
                        Ok(EmailVerificationResult::ExistingUser { user: user_slug, session })
                    },
                    Some(proto::verify_email_code_response::Result::NewUser(onboarding)) => {
                        Ok(EmailVerificationResult::NewUser {
                            onboarding_token: onboarding.onboarding_token,
                        })
                    },
                    Some(proto::verify_email_code_response::Result::TotpRequired(challenge)) => {
                        Ok(EmailVerificationResult::TotpRequired {
                            challenge_nonce: challenge.challenge_nonce,
                        })
                    },
                    None => Err(error::SdkError::Rpc {
                        code: tonic::Code::Internal,
                        message: "Empty verify_email_code response".to_string(),
                        request_id: None,
                        trace_id: None,
                        error_details: None,
                    }),
                }
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
        let region_slug = region.as_str().to_string();
        let pool = self.pool.clone();
        self.call_with_retry("complete_registration", || {
            let pool = pool.clone();
            let email = email.clone();
            let name = name.clone();
            let onboarding_token = onboarding_token.clone();
            let organization_name = organization_name.clone();
            let region_slug = region_slug.clone();
            async move {
                let mut client = crate::connected_client!(pool, create_user_client);

                let request = proto::CompleteRegistrationRequest {
                    onboarding_token: onboarding_token.clone(),
                    email: email.clone(),
                    region: region_slug,
                    name: name.clone(),
                    organization_name: organization_name.clone(),
                };

                let response = client.complete_registration(request).await?.into_inner();

                let user_slug =
                    response.user.and_then(|u| u.slug).map(|s| UserSlug::new(s.slug)).ok_or_else(
                        || missing_response_field("user.slug", "CompleteRegistrationResponse"),
                    )?;

                let session =
                    response.session.map(crate::token::TokenPair::from_proto).ok_or_else(|| {
                        missing_response_field("session", "CompleteRegistrationResponse")
                    })?;

                let organization = response.organization.map(|o| OrganizationSlug::new(o.slug));

                Ok(RegistrationResult { user: user_slug, session, organization })
            }
        })
        .await
    }
}
