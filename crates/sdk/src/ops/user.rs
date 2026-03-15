//! User CRUD, email management, and blinding key operations.

use inferadb_ledger_proto::proto;
use inferadb_ledger_types::{Region, UserEmailId, UserRole, UserSlug, UserStatus};

use crate::{
    LedgerClient,
    error::Result,
    proto_util::{
        missing_response_field, proto_timestamp_to_system_time, user_email_info_from_proto,
        user_info_from_proto,
    },
    retry::with_retry_cancellable,
    types::admin::{BlindingKeyRehashStatus, BlindingKeyRotationStatus, UserEmailInfo, UserInfo},
};

impl LedgerClient {
    // ========================================================================
    // User CRUD
    // ========================================================================

    /// Creates a new user.
    ///
    /// The caller must pre-compute the email HMAC using the blinding key.
    /// User creation is saga-based: email HMAC reservation -> regional write -> directory entry.
    pub async fn create_user(
        &self,
        name: impl Into<String>,
        email: impl Into<String>,
        email_hmac: impl Into<String>,
        region: Region,
        role: UserRole,
    ) -> Result<UserInfo> {
        self.check_shutdown(None)?;

        let name = name.into();
        let email = email.into();
        let email_hmac = email_hmac.into();
        let proto_region: proto::Region = region.into();
        let region_i32: i32 = proto_region.into();
        let proto_role: proto::UserRole = role.into();
        let role_i32: i32 = proto_role.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_user",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_user",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::CreateUserRequest {
                        name: name.clone(),
                        email: email.clone(),
                        region: region_i32,
                        role: Some(role_i32),
                        email_hmac: email_hmac.clone(),
                        organization_name: String::new(),
                        organization_tier: None,
                    };

                    let response =
                        client.create_user(tonic::Request::new(request)).await?.into_inner();

                    response
                        .user
                        .map(|u| user_info_from_proto(&u))
                        .ok_or_else(|| missing_response_field("user", "CreateUserResponse"))
                },
            ),
        )
        .await
    }

    /// Gets a user by slug.
    pub async fn get_user(&self, user: UserSlug) -> Result<UserInfo> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_user",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_user",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::GetUserRequest {
                        slug: Some(proto::UserSlug { slug: user.value() }),
                    };

                    let response =
                        client.get_user(tonic::Request::new(request)).await?.into_inner();

                    response
                        .user
                        .map(|u| user_info_from_proto(&u))
                        .ok_or_else(|| missing_response_field("user", "GetUserResponse"))
                },
            ),
        )
        .await
    }

    /// Updates a user's name, role, or primary email.
    ///
    /// At least one field must be provided.
    pub async fn update_user(
        &self,
        user: UserSlug,
        name: Option<String>,
        role: Option<UserRole>,
        email: Option<UserEmailId>,
    ) -> Result<UserInfo> {
        self.check_shutdown(None)?;

        let proto_role = role.map(|r| {
            let pr: proto::UserRole = r.into();
            let i: i32 = pr.into();
            i
        });
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "update_user",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "update_user",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::UpdateUserRequest {
                        slug: Some(proto::UserSlug { slug: user.value() }),
                        name: name.clone(),
                        role: proto_role,
                        primary_email: email.map(|id| proto::UserEmailId { id: id.value() }),
                    };

                    let response =
                        client.update_user(tonic::Request::new(request)).await?.into_inner();

                    response
                        .user
                        .map(|u| user_info_from_proto(&u))
                        .ok_or_else(|| missing_response_field("user", "UpdateUserResponse"))
                },
            ),
        )
        .await
    }

    /// Soft-deletes a user, starting the retention countdown.
    pub async fn delete_user(
        &self,
        user: UserSlug,
        deleted_by: impl Into<String>,
    ) -> Result<UserInfo> {
        self.check_shutdown(None)?;

        let deleted_by = deleted_by.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_user",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_user",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::DeleteUserRequest {
                        slug: Some(proto::UserSlug { slug: user.value() }),
                        deleted_by: deleted_by.clone(),
                    };

                    let response =
                        client.delete_user(tonic::Request::new(request)).await?.into_inner();

                    let slug_val = response.slug.map_or(0, |s| s.slug);
                    Ok(UserInfo {
                        slug: UserSlug::new(slug_val),
                        name: String::new(),
                        email: UserEmailId::new(0),
                        status: UserStatus::Deleted,
                        role: UserRole::User,
                        created_at: None,
                        updated_at: None,
                        deleted_at: response
                            .deleted_at
                            .as_ref()
                            .and_then(proto_timestamp_to_system_time),
                        retention_days: Some(response.retention_days),
                    })
                },
            ),
        )
        .await
    }

    /// Lists users with pagination.
    pub async fn list_users(
        &self,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<UserInfo>, Option<Vec<u8>>)> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "list_users",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "list_users",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::ListUsersRequest {
                        page_size,
                        page_token: page_token.clone(),
                        region: None,
                    };

                    let response =
                        client.list_users(tonic::Request::new(request)).await?.into_inner();

                    let users: Vec<UserInfo> =
                        response.users.iter().map(user_info_from_proto).collect();
                    Ok((users, response.next_page_token))
                },
            ),
        )
        .await
    }

    /// Searches users by email.
    pub async fn search_users(&self, email: impl Into<String>) -> Result<Vec<UserInfo>> {
        self.check_shutdown(None)?;

        let email = email.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "search_users",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "search_users",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::SearchUsersRequest {
                        filter: Some(proto::UserSearchFilter {
                            email: Some(email.clone()),
                            status: None,
                            role: None,
                            name_prefix: None,
                        }),
                        page_token: None,
                        page_size: 100,
                    };

                    let response =
                        client.search_users(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.users.iter().map(user_info_from_proto).collect())
                },
            ),
        )
        .await
    }

    /// Creates an email record for a user.
    ///
    /// The `email_hmac` is the hex-encoded HMAC-SHA256 of the normalized email,
    /// computed with the email blinding key. It is stored in the GLOBAL control
    /// plane for cross-region uniqueness; the plaintext email stays regional.
    pub async fn create_user_email(
        &self,
        user: UserSlug,
        email: impl Into<String>,
        email_hmac: impl Into<String>,
    ) -> Result<UserEmailInfo> {
        self.check_shutdown(None)?;

        let email = email.into();
        let email_hmac = email_hmac.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "create_user_email",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "create_user_email",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::CreateUserEmailRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        email: email.clone(),
                        email_hmac: email_hmac.clone(),
                    };

                    let response =
                        client.create_user_email(tonic::Request::new(request)).await?.into_inner();

                    response
                        .email
                        .map(|e| user_email_info_from_proto(&e))
                        .ok_or_else(|| missing_response_field("email", "CreateUserEmailResponse"))
                },
            ),
        )
        .await
    }

    /// Deletes an email record from a user.
    pub async fn delete_user_email(&self, user: UserSlug, email_id: UserEmailId) -> Result<()> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "delete_user_email",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "delete_user_email",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::DeleteUserEmailRequest {
                        user: Some(proto::UserSlug { slug: user.value() }),
                        email_id: Some(proto::UserEmailId { id: email_id.value() }),
                    };

                    client.delete_user_email(tonic::Request::new(request)).await?;
                    Ok(())
                },
            ),
        )
        .await
    }

    /// Searches user emails by user or email address.
    pub async fn search_user_email(
        &self,
        user: Option<UserSlug>,
        email: Option<String>,
    ) -> Result<Vec<UserEmailInfo>> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "search_user_email",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "search_user_email",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::SearchUserEmailRequest {
                        filter: Some(proto::UserEmailSearchFilter {
                            user: user.map(|s| proto::UserSlug { slug: s.value() }),
                            email: email.clone(),
                            verified_only: None,
                        }),
                        page_token: None,
                        page_size: 100,
                    };

                    let response =
                        client.search_user_email(tonic::Request::new(request)).await?.into_inner();

                    Ok(response.emails.iter().map(user_email_info_from_proto).collect())
                },
            ),
        )
        .await
    }

    /// Verifies a user email using a verification token.
    pub async fn verify_user_email(&self, token: impl Into<String>) -> Result<UserEmailInfo> {
        self.check_shutdown(None)?;

        let token = token.into();
        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "verify_user_email",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "verify_user_email",
                || async {
                    let mut client = crate::connected_client!(pool, create_user_client);

                    let request = proto::VerifyUserEmailRequest { token: token.clone() };

                    let response =
                        client.verify_user_email(tonic::Request::new(request)).await?.into_inner();

                    response
                        .email
                        .map(|e| user_email_info_from_proto(&e))
                        .ok_or_else(|| missing_response_field("email", "VerifyUserEmailResponse"))
                },
            ),
        )
        .await
    }

    /// Initiates a blinding key rotation.
    ///
    /// Starts an asynchronous re-hashing of all email hash entries with the new
    /// blinding key version. Returns immediately with initial progress.
    ///
    /// # Arguments
    ///
    /// * `new_key_version` - Version number of the new blinding key (monotonically increasing).
    ///
    /// # Returns
    ///
    /// Returns [`BlindingKeyRotationStatus`] with initial progress.
    pub async fn rotate_blinding_key(
        &self,
        new_key_version: u32,
    ) -> Result<BlindingKeyRotationStatus> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "rotate_blinding_key",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "rotate_blinding_key",
                || async {
                    let mut client = crate::connected_client!(pool, create_admin_client);

                    let request = proto::RotateBlindingKeyRequest { new_key_version };

                    let response = client
                        .rotate_blinding_key(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(BlindingKeyRotationStatus {
                        total_entries: response.total_entries,
                        entries_rehashed: response.entries_rehashed,
                        complete: response.complete,
                    })
                },
            ),
        )
        .await
    }

    /// Gets the current status of a blinding key rotation.
    ///
    /// Returns progress information about an in-flight or completed rotation,
    /// including per-region breakdown.
    ///
    /// # Returns
    ///
    /// Returns [`BlindingKeyRehashStatus`] with progress details.
    pub async fn get_blinding_key_rehash_status(&self) -> Result<BlindingKeyRehashStatus> {
        self.check_shutdown(None)?;

        let pool = self.pool.clone();
        let retry_policy = self.pool.config().retry_policy().clone();

        self.with_metrics(
            "get_blinding_key_rehash_status",
            with_retry_cancellable(
                &retry_policy,
                &self.cancellation,
                Some(&pool),
                "get_blinding_key_rehash_status",
                || async {
                    let mut client = crate::connected_client!(pool, create_admin_client);

                    let request = proto::GetBlindingKeyRehashStatusRequest {};

                    let response = client
                        .get_blinding_key_rehash_status(tonic::Request::new(request))
                        .await?
                        .into_inner();

                    Ok(BlindingKeyRehashStatus {
                        total_entries: response.total_entries,
                        entries_rehashed: response.entries_rehashed,
                        complete: response.complete,
                        per_region_progress: response.per_region_progress,
                        active_key_version: response.active_key_version,
                    })
                },
            ),
        )
        .await
    }
}
