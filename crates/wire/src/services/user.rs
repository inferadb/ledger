//! Wire types for `UserService`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::services::shared::{
    CredentialInfo, CredentialType, OrganizationSlug, OrganizationTier, TokenPair, TotpAlgorithm,
    TotpRequired, User, UserEmail, UserEmailId, UserRole, UserSlug, UserStatus,
};

// ---------------------------------------------------------------------------
// User CRUD
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateUserRequest {
    pub name: String,
    pub email: String,
    pub region: String,
    pub role: Option<UserRole>,
    pub email_hmac: String,
    pub organization_name: String,
    pub organization_tier: Option<OrganizationTier>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateUserResponse {
    pub slug: Option<UserSlug>,
    pub user: Option<User>,
    pub default_organization_slug: Option<OrganizationSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetUserRequest {
    pub slug: Option<UserSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetUserResponse {
    pub user: Option<User>,
    pub emails: Vec<UserEmail>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateUserRequest {
    pub slug: Option<UserSlug>,
    pub name: Option<String>,
    pub role: Option<UserRole>,
    pub primary_email: Option<UserEmailId>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateUserResponse {
    pub user: Option<User>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteUserRequest {
    pub slug: Option<UserSlug>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteUserResponse {
    pub slug: Option<UserSlug>,
    /// UNIX nanoseconds.
    pub deleted_at: u64,
    pub retention_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListUsersRequest {
    pub page_token: Option<Bytes>,
    pub page_size: u32,
    pub region: Option<String>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListUsersResponse {
    pub users: Vec<User>,
    pub next_page_token: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserSearchFilter {
    pub email: Option<String>,
    pub status: Option<UserStatus>,
    pub role: Option<UserRole>,
    pub name_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SearchUsersRequest {
    pub filter: Option<UserSearchFilter>,
    pub page_token: Option<Bytes>,
    pub page_size: u32,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SearchUsersResponse {
    pub users: Vec<User>,
    pub next_page_token: Option<Bytes>,
}

// ---------------------------------------------------------------------------
// User Email management
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateUserEmailRequest {
    pub user: Option<UserSlug>,
    pub email: String,
    pub email_hmac: String,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateUserEmailResponse {
    pub email: Option<UserEmail>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteUserEmailRequest {
    pub user: Option<UserSlug>,
    pub email_id: Option<UserEmailId>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteUserEmailResponse {
    /// UNIX nanoseconds.
    pub deleted_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserEmailSearchFilter {
    pub email: Option<String>,
    pub user: Option<UserSlug>,
    pub verified_only: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SearchUserEmailRequest {
    pub filter: Option<UserEmailSearchFilter>,
    pub page_token: Option<Bytes>,
    pub page_size: u32,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SearchUserEmailResponse {
    pub emails: Vec<UserEmail>,
    pub next_page_token: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VerifyUserEmailRequest {
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VerifyUserEmailResponse {
    pub email: Option<UserEmail>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MigrateUserRegionRequest {
    pub slug: Option<UserSlug>,
    pub target_region: String,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MigrateUserRegionResponse {
    pub slug: Option<UserSlug>,
    pub source_region: String,
    pub target_region: String,
    pub directory_status: String,
}

// ---------------------------------------------------------------------------
// User Erasure (GDPR)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EraseUserRequest {
    pub user: Option<UserSlug>,
    pub caller: Option<UserSlug>,
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EraseUserResponse {
    pub user: Option<UserSlug>,
}

// ---------------------------------------------------------------------------
// Onboarding (email verification + registration)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InitiateEmailVerificationRequest {
    pub email: String,
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InitiateEmailVerificationResponse {
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VerifyEmailCodeRequest {
    pub email: String,
    pub code: String,
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VerifyEmailCodeResponse {
    pub result: Option<VerifyEmailCodeResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VerifyEmailCodeResult {
    ExistingUser(ExistingUserSession),
    NewUser(OnboardingSession),
    TotpRequired(TotpRequired),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExistingUserSession {
    pub user: Option<UserSlug>,
    pub session: Option<TokenPair>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OnboardingSession {
    pub onboarding_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CompleteRegistrationRequest {
    pub onboarding_token: String,
    pub email: String,
    pub region: String,
    pub name: String,
    pub organization_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CompleteRegistrationResponse {
    pub user: Option<User>,
    pub session: Option<TokenPair>,
    pub organization: Option<OrganizationSlug>,
}

// ---------------------------------------------------------------------------
// User credentials
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserCredential {
    pub id: i64,
    pub user: Option<UserSlug>,
    pub credential_type: CredentialType,
    pub name: String,
    pub enabled: bool,
    /// UNIX nanoseconds.
    pub created_at: u64,
    /// UNIX nanoseconds.
    pub last_used_at: Option<u64>,
    pub data: Option<UserCredentialData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UserCredentialData {
    Passkey(PasskeyCredentialData),
    Totp(TotpCredentialData),
    RecoveryCode(RecoveryCodeCredentialData),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PasskeyCredentialData {
    pub credential_id: Bytes,
    pub public_key: Bytes,
    pub sign_count: u32,
    pub transports: Vec<String>,
    pub backup_eligible: bool,
    pub backup_state: bool,
    pub attestation_format: Option<String>,
    pub aaguid: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TotpCredentialData {
    pub secret: Bytes,
    pub algorithm: TotpAlgorithm,
    pub digits: u32,
    pub period: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecoveryCodeCredentialData {
    pub code_hashes: Vec<Bytes>,
    pub total_generated: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateUserCredentialRequest {
    pub user: Option<UserSlug>,
    pub credential_type: CredentialType,
    pub name: String,
    pub caller: Option<UserSlug>,
    pub data: Option<CreateUserCredentialData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CreateUserCredentialData {
    Passkey(PasskeyCredentialData),
    Totp(TotpCredentialData),
    RecoveryCode(RecoveryCodeCredentialData),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateUserCredentialResponse {
    pub credential: Option<UserCredential>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListUserCredentialsRequest {
    pub user: Option<UserSlug>,
    pub credential_type: Option<CredentialType>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListUserCredentialsResponse {
    pub credentials: Vec<UserCredential>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateUserCredentialRequest {
    pub user: Option<UserSlug>,
    pub credential_id: i64,
    pub name: Option<String>,
    pub enabled: Option<bool>,
    pub caller: Option<UserSlug>,
    pub passkey: Option<PasskeyCredentialData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateUserCredentialResponse {
    pub credential: Option<UserCredential>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteUserCredentialRequest {
    pub user: Option<UserSlug>,
    pub credential_id: i64,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteUserCredentialResponse {}

// ---------------------------------------------------------------------------
// TOTP challenge / verification / recovery code
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateTotpChallengeRequest {
    pub user: Option<UserSlug>,
    pub primary_method: String,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CreateTotpChallengeResponse {
    pub challenge_nonce: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VerifyTotpRequest {
    pub user: Option<UserSlug>,
    pub totp_code: String,
    pub challenge_nonce: Bytes,
    pub credential_used: Option<CredentialInfo>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VerifyTotpResponse {
    pub tokens: Option<TokenPair>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsumeRecoveryCodeRequest {
    pub user: Option<UserSlug>,
    pub code: String,
    pub challenge_nonce: Bytes,
    pub credential_used: Option<CredentialInfo>,
    pub caller: Option<UserSlug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsumeRecoveryCodeResponse {
    pub tokens: Option<TokenPair>,
    pub remaining_codes: u32,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn create_user_credential_request_with_passkey_roundtrips() {
        let req = CreateUserCredentialRequest {
            user: Some(UserSlug::new(42)),
            credential_type: CredentialType::Passkey,
            name: "yubikey-5".to_owned(),
            caller: Some(UserSlug::new(42)),
            data: Some(CreateUserCredentialData::Passkey(PasskeyCredentialData {
                credential_id: Bytes::from_static(&[0xAB; 16]),
                public_key: Bytes::from_static(&[0xCD; 65]),
                sign_count: 0,
                transports: vec!["usb".to_owned(), "nfc".to_owned()],
                backup_eligible: true,
                backup_state: false,
                attestation_format: Some("packed".to_owned()),
                aaguid: Some(Bytes::from_static(&[0xEF; 16])),
            })),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: CreateUserCredentialRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req, decoded);
    }
}
