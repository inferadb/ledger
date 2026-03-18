use std::sync::{Arc, atomic::Ordering};

use inferadb_ledger_proto::proto;
use tonic::{Request, Response, Status};

use super::MockState;

pub(super) struct MockUserService {
    pub(super) state: Arc<MockState>,
}

impl MockUserService {
    pub(super) fn new(state: Arc<MockState>) -> Self {
        Self { state }
    }

    fn now_timestamp() -> prost_types::Timestamp {
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        prost_types::Timestamp { seconds: now.as_secs() as i64, nanos: now.subsec_nanos() as i32 }
    }
}

#[tonic::async_trait]
impl inferadb_ledger_proto::proto::user_service_server::UserService for MockUserService {
    async fn create_user(
        &self,
        request: Request<proto::CreateUserRequest>,
    ) -> Result<Response<proto::CreateUserResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = self.state.next_user_slug.fetch_add(1, Ordering::SeqCst);
        let email_id = self.state.next_user_email_id.fetch_add(1, Ordering::SeqCst);
        let now = Self::now_timestamp();

        let role = req.role.unwrap_or(proto::UserRole::User as i32);

        let user = proto::User {
            id: Some(proto::UserId { id: slug as i64 }),
            name: req.name,
            email: Some(proto::UserEmailId { id: email_id as i64 }),
            status: proto::UserStatus::Active as i32,
            created_at: Some(now),
            updated_at: Some(now),
            role,
            slug: Some(proto::UserSlug { slug }),
            deleted_at: None,
        };

        let user_email = proto::UserEmail {
            id: Some(proto::UserEmailId { id: email_id as i64 }),
            user: Some(proto::UserId { id: slug as i64 }),
            email: req.email,
            created_at: Some(now),
            verified_at: None,
        };

        self.state.users.write().insert(slug, user.clone());
        self.state.user_emails.write().insert(email_id as i64, user_email);

        Ok(Response::new(proto::CreateUserResponse {
            slug: Some(proto::UserSlug { slug }),
            user: Some(user),
            default_organization_slug: None,
        }))
    }

    async fn get_user(
        &self,
        request: Request<proto::GetUserRequest>,
    ) -> Result<Response<proto::GetUserResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = req.slug.map_or(0, |s| s.slug);

        let users = self.state.users.read();
        let user = users.get(&slug).cloned();

        if user.is_none() {
            return Err(Status::not_found(format!("User {slug} not found")));
        }

        // Collect emails for this user
        let user_emails = self.state.user_emails.read();
        let emails: Vec<proto::UserEmail> = user_emails
            .values()
            .filter(|e| e.user.is_some_and(|u| u.id == slug as i64))
            .cloned()
            .collect();

        Ok(Response::new(proto::GetUserResponse { user, emails }))
    }

    async fn update_user(
        &self,
        request: Request<proto::UpdateUserRequest>,
    ) -> Result<Response<proto::UpdateUserResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = req.slug.map_or(0, |s| s.slug);

        let mut users = self.state.users.write();
        let user = users
            .get_mut(&slug)
            .ok_or_else(|| Status::not_found(format!("User {slug} not found")))?;

        if let Some(name) = req.name {
            user.name = name;
        }
        if let Some(role) = req.role {
            user.role = role;
        }
        if let Some(primary_email) = req.primary_email {
            user.email = Some(primary_email);
        }
        user.updated_at = Some(Self::now_timestamp());

        Ok(Response::new(proto::UpdateUserResponse { user: Some(user.clone()) }))
    }

    async fn delete_user(
        &self,
        request: Request<proto::DeleteUserRequest>,
    ) -> Result<Response<proto::DeleteUserResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = req.slug.map_or(0, |s| s.slug);
        let now = Self::now_timestamp();

        let mut users = self.state.users.write();
        let user = users
            .get_mut(&slug)
            .ok_or_else(|| Status::not_found(format!("User {slug} not found")))?;

        user.status = proto::UserStatus::Deleting as i32;
        user.deleted_at = Some(now);
        user.updated_at = Some(now);

        Ok(Response::new(proto::DeleteUserResponse {
            slug: Some(proto::UserSlug { slug }),
            deleted_at: Some(now),
            retention_days: 90,
        }))
    }

    async fn list_users(
        &self,
        request: Request<proto::ListUsersRequest>,
    ) -> Result<Response<proto::ListUsersResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let page_size = if req.page_size == 0 { 100 } else { req.page_size as usize };

        let users = self.state.users.read();
        let all_users: Vec<proto::User> = users.values().cloned().collect();
        let page = all_users.into_iter().take(page_size).collect();

        Ok(Response::new(proto::ListUsersResponse { users: page, next_page_token: None }))
    }

    async fn search_users(
        &self,
        request: Request<proto::SearchUsersRequest>,
    ) -> Result<Response<proto::SearchUsersResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();
        let page_size = if req.page_size == 0 { 100 } else { req.page_size as usize };

        let users = self.state.users.read();
        let user_emails = self.state.user_emails.read();

        let results: Vec<proto::User> = users
            .values()
            .filter(|u| {
                if filter.status.is_some_and(|status| u.status != status) {
                    return false;
                }
                if filter.role.is_some_and(|role| u.role != role) {
                    return false;
                }
                if filter
                    .name_prefix
                    .as_ref()
                    .is_some_and(|prefix| !u.name.starts_with(prefix.as_str()))
                {
                    return false;
                }
                if let Some(ref email_filter) = filter.email {
                    let user_id = u.id.map_or(0, |id| id.id);
                    let has_match = user_emails.values().any(|e| {
                        e.user.is_some_and(|uid| uid.id == user_id)
                            && e.email.contains(email_filter.as_str())
                    });
                    if !has_match {
                        return false;
                    }
                }
                true
            })
            .take(page_size)
            .cloned()
            .collect();

        Ok(Response::new(proto::SearchUsersResponse { users: results, next_page_token: None }))
    }

    async fn create_user_email(
        &self,
        request: Request<proto::CreateUserEmailRequest>,
    ) -> Result<Response<proto::CreateUserEmailResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let user_slug = req.user.map_or(0, |s| s.slug);

        // Verify user exists
        let users = self.state.users.read();
        if !users.contains_key(&user_slug) {
            return Err(Status::not_found(format!("User {user_slug} not found")));
        }
        drop(users);

        let email_id = self.state.next_user_email_id.fetch_add(1, Ordering::SeqCst);
        let now = Self::now_timestamp();

        let user_email = proto::UserEmail {
            id: Some(proto::UserEmailId { id: email_id as i64 }),
            user: Some(proto::UserId { id: user_slug as i64 }),
            email: req.email,
            created_at: Some(now),
            verified_at: None,
        };

        self.state.user_emails.write().insert(email_id as i64, user_email.clone());

        Ok(Response::new(proto::CreateUserEmailResponse { email: Some(user_email) }))
    }

    async fn delete_user_email(
        &self,
        request: Request<proto::DeleteUserEmailRequest>,
    ) -> Result<Response<proto::DeleteUserEmailResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let email_id = req.email_id.map_or(0, |e| e.id);

        let mut emails = self.state.user_emails.write();
        if emails.remove(&email_id).is_none() {
            return Err(Status::not_found(format!("Email {email_id} not found")));
        }

        Ok(Response::new(proto::DeleteUserEmailResponse {
            deleted_at: Some(Self::now_timestamp()),
        }))
    }

    async fn search_user_email(
        &self,
        request: Request<proto::SearchUserEmailRequest>,
    ) -> Result<Response<proto::SearchUserEmailResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();
        let page_size = if req.page_size == 0 { 100 } else { req.page_size as usize };

        let emails = self.state.user_emails.read();
        let results: Vec<proto::UserEmail> = emails
            .values()
            .filter(|e| {
                if filter.email.as_ref().is_some_and(|ef| !e.email.contains(ef.as_str())) {
                    return false;
                }
                if filter
                    .user
                    .as_ref()
                    .is_some_and(|uf| e.user.is_none_or(|u| u.id != uf.slug as i64))
                {
                    return false;
                }
                if filter.verified_only.is_some_and(|v| v && e.verified_at.is_none()) {
                    return false;
                }
                true
            })
            .take(page_size)
            .cloned()
            .collect();

        Ok(Response::new(proto::SearchUserEmailResponse { emails: results, next_page_token: None }))
    }

    async fn verify_user_email(
        &self,
        request: Request<proto::VerifyUserEmailRequest>,
    ) -> Result<Response<proto::VerifyUserEmailResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        // In the mock, we treat the token as the email_id string for simplicity
        let email_id: i64 =
            req.token.parse().map_err(|_| Status::not_found("Invalid verification token"))?;

        let mut emails = self.state.user_emails.write();
        let email = emails
            .get_mut(&email_id)
            .ok_or_else(|| Status::not_found("Verification token not found"))?;

        email.verified_at = Some(Self::now_timestamp());

        Ok(Response::new(proto::VerifyUserEmailResponse { email: Some(email.clone()) }))
    }

    async fn migrate_user_region(
        &self,
        request: Request<proto::MigrateUserRegionRequest>,
    ) -> Result<Response<proto::MigrateUserRegionResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = req.slug.map_or(0, |s| s.slug);

        let users = self.state.users.read();
        if !users.contains_key(&slug) {
            return Err(Status::not_found(format!("User {slug} not found")));
        }

        Ok(Response::new(proto::MigrateUserRegionResponse {
            slug: Some(proto::UserSlug { slug }),
            source_region: proto::Region::Global as i32,
            target_region: req.target_region,
            directory_status: "MIGRATING".to_string(),
        }))
    }

    async fn erase_user(
        &self,
        request: Request<proto::EraseUserRequest>,
    ) -> Result<Response<proto::EraseUserResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        let slug = req.user.map_or(0, |s| s.slug);

        let mut users = self.state.users.write();
        if users.remove(&slug).is_none() {
            return Err(Status::not_found(format!("User {slug} not found")));
        }

        // Also remove their emails
        let mut emails = self.state.user_emails.write();
        emails.retain(|_, e| e.user.is_none_or(|u| u.id != slug as i64));

        Ok(Response::new(proto::EraseUserResponse { user: Some(proto::UserSlug { slug }) }))
    }

    async fn initiate_email_verification(
        &self,
        request: Request<proto::InitiateEmailVerificationRequest>,
    ) -> Result<Response<proto::InitiateEmailVerificationResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        if req.email.is_empty() {
            return Err(Status::invalid_argument("email is required"));
        }

        Ok(Response::new(proto::InitiateEmailVerificationResponse { code: "ABC123".to_string() }))
    }

    async fn verify_email_code(
        &self,
        request: Request<proto::VerifyEmailCodeRequest>,
    ) -> Result<Response<proto::VerifyEmailCodeResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        if req.email.is_empty() || req.code.is_empty() {
            return Err(Status::invalid_argument("email and code are required"));
        }

        // Mock always returns new-user path
        Ok(Response::new(proto::VerifyEmailCodeResponse {
            result: Some(proto::verify_email_code_response::Result::NewUser(
                proto::OnboardingSession { onboarding_token: "ilobt_mock_token".to_string() },
            )),
        }))
    }

    async fn complete_registration(
        &self,
        request: Request<proto::CompleteRegistrationRequest>,
    ) -> Result<Response<proto::CompleteRegistrationResponse>, Status> {
        self.state.check_injection().await?;

        let req = request.into_inner();
        if req.email.is_empty() || req.name.is_empty() || req.onboarding_token.is_empty() {
            return Err(Status::invalid_argument("email, name, and onboarding_token are required"));
        }

        let user_slug = self.state.next_user_slug.fetch_add(1, Ordering::SeqCst);
        let now = Self::now_timestamp();

        Ok(Response::new(proto::CompleteRegistrationResponse {
            user: Some(proto::User {
                slug: Some(proto::UserSlug { slug: user_slug }),
                name: req.name,
                status: proto::UserStatus::Active.into(),
                role: proto::UserRole::User.into(),
                created_at: Some(now),
                updated_at: Some(now),
                ..Default::default()
            }),
            session: Some(proto::TokenPair {
                access_token: format!("mock_access_{user_slug}"),
                refresh_token: format!("mock_refresh_{user_slug}"),
                access_expires_at: None,
                refresh_expires_at: None,
            }),
            organization: Some(proto::OrganizationSlug { slug: user_slug + 1000 }),
        }))
    }

    async fn create_user_credential(
        &self,
        _request: Request<proto::CreateUserCredentialRequest>,
    ) -> Result<Response<proto::CreateUserCredentialResponse>, Status> {
        Err(Status::unimplemented("CreateUserCredential not implemented in mock"))
    }

    async fn list_user_credentials(
        &self,
        _request: Request<proto::ListUserCredentialsRequest>,
    ) -> Result<Response<proto::ListUserCredentialsResponse>, Status> {
        Err(Status::unimplemented("ListUserCredentials not implemented in mock"))
    }

    async fn update_user_credential(
        &self,
        _request: Request<proto::UpdateUserCredentialRequest>,
    ) -> Result<Response<proto::UpdateUserCredentialResponse>, Status> {
        Err(Status::unimplemented("UpdateUserCredential not implemented in mock"))
    }

    async fn delete_user_credential(
        &self,
        _request: Request<proto::DeleteUserCredentialRequest>,
    ) -> Result<Response<proto::DeleteUserCredentialResponse>, Status> {
        Err(Status::unimplemented("DeleteUserCredential not implemented in mock"))
    }

    async fn create_totp_challenge(
        &self,
        _request: Request<proto::CreateTotpChallengeRequest>,
    ) -> Result<Response<proto::CreateTotpChallengeResponse>, Status> {
        Err(Status::unimplemented("CreateTotpChallenge not implemented in mock"))
    }

    async fn verify_totp(
        &self,
        _request: Request<proto::VerifyTotpRequest>,
    ) -> Result<Response<proto::VerifyTotpResponse>, Status> {
        Err(Status::unimplemented("VerifyTotp not implemented in mock"))
    }

    async fn consume_recovery_code(
        &self,
        _request: Request<proto::ConsumeRecoveryCodeRequest>,
    ) -> Result<Response<proto::ConsumeRecoveryCodeResponse>, Status> {
        Err(Status::unimplemented("ConsumeRecoveryCode not implemented in mock"))
    }
}
