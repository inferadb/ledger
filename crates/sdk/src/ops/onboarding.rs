//! Onboarding operations (email verification and registration).

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{OrganizationSlug, Region, UserSlug};

use crate::{
    LedgerClient,
    error::{self, Result},
    proto_util::missing_response_field,
    retry::with_retry_cancellable,
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
        self.check_shutdown(None)?;

        let email = email.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "initiate_email_verification",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "initiate_email_verification",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::InitiateEmailVerificationRequest {
                        email: email.clone(),
                        region: region_i32,
                    };

                    let response = client.initiate_email_verification(request).await?.into_inner();

                    Ok(EmailVerificationCode { code: response.code })
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let email = email.into();
        let code = code.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "verify_email_code",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "verify_email_code",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::VerifyEmailCodeRequest {
                        email: email.clone(),
                        code: code.clone(),
                        region: region_i32,
                    };

                    let response = client.verify_email_code(request).await?.into_inner();

                    match response.result {
                        Some(proto::verify_email_code_response::Result::ExistingUser(existing)) => {
                            let user_slug =
                                existing.user.map(|u| UserSlug::new(u.slug)).ok_or_else(|| {
                                    missing_response_field("user", "VerifyEmailCodeResponse")
                                })?;
                            let session = existing
                                .session
                                .map(crate::token::TokenPair::from_proto)
                                .ok_or_else(|| {
                                    missing_response_field("session", "VerifyEmailCodeResponse")
                                })?;
                            Ok(EmailVerificationResult::ExistingUser { user: user_slug, session })
                        },
                        Some(proto::verify_email_code_response::Result::NewUser(onboarding)) => {
                            Ok(EmailVerificationResult::NewUser {
                                onboarding_token: onboarding.onboarding_token,
                            })
                        },
                        Some(proto::verify_email_code_response::Result::TotpRequired(
                            challenge,
                        )) => Ok(EmailVerificationResult::TotpRequired {
                            challenge_nonce: challenge.challenge_nonce,
                        }),
                        None => Err(error::SdkError::Rpc {
                            code: tonic::Code::Internal,
                            message: "Empty verify_email_code response".to_string(),
                            request_id: None,
                            trace_id: None,
                            error_details: None,
                        }),
                    }
                },
            ),
        )
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
        self.check_shutdown(None)?;

        let onboarding_token = onboarding_token.into();
        let email = email.into();
        let name = name.into();
        let organization_name = organization_name.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "complete_registration",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "complete_registration",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::CompleteRegistrationRequest {
                        onboarding_token: onboarding_token.clone(),
                        email: email.clone(),
                        region: region_i32,
                        name: name.clone(),
                        organization_name: organization_name.clone(),
                    };

                    let response = client.complete_registration(request).await?.into_inner();

                    let user_slug = response
                        .user
                        .and_then(|u| u.slug)
                        .map(|s| UserSlug::new(s.slug))
                        .ok_or_else(|| {
                            missing_response_field("user.slug", "CompleteRegistrationResponse")
                        })?;

                    let session =
                        response.session.map(crate::token::TokenPair::from_proto).ok_or_else(
                            || missing_response_field("session", "CompleteRegistrationResponse"),
                        )?;

                    let organization = response.organization.map(|o| OrganizationSlug::new(o.slug));

                    Ok(RegistrationResult { user: user_slug, session, organization })
                },
            ),
        )
        .await
    }
}
