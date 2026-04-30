//! User CRUD and email management operations.

use inferadb_ledger_types::{Region, UserEmailId, UserRole, UserSlug};

use crate::{
    LedgerClient,
    error::Result,
    types::admin::{UserEmailInfo, UserInfo},
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
        let name = name.into();
        let email = email.into();
        let email_hmac = email_hmac.into();
        let pool = self.pool.clone();
        self.call_with_retry("create_user", || {
            let pool = pool.clone();
            let email = email.clone();
            let email_hmac = email_hmac.clone();
            let name = name.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::create_user(
                    wire_client,
                    request_id,
                    name,
                    email,
                    email_hmac,
                    region,
                    role,
                )
                .await
            }
        })
        .await
    }

    /// Gets a user by slug.
    pub async fn get_user(&self, user: UserSlug) -> Result<UserInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("get_user", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::get_user(wire_client, request_id, user).await
            }
        })
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
        let pool = self.pool.clone();
        self.call_with_retry("update_user", || {
            let pool = pool.clone();
            let name = name.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::update_user(wire_client, request_id, user, name, role, email)
                    .await
            }
        })
        .await
    }

    /// Soft-deletes a user, starting the retention countdown.
    pub async fn delete_user(&self, user: UserSlug, caller: UserSlug) -> Result<UserInfo> {
        let pool = self.pool.clone();
        self.call_with_retry("delete_user", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::delete_user(wire_client, request_id, user, caller).await
            }
        })
        .await
    }

    /// Lists users with pagination.
    ///
    /// `page_size` is the maximum items per page (0 = server default). Pass
    /// `page_token` from a previous response to fetch the next page; pass
    /// `None` for the first page. Returns a tuple `(users, next_page_token)`
    /// where `next_page_token` is `None` when there are no more pages.
    pub async fn list_users(
        &self,
        caller: UserSlug,
        page_size: u32,
        page_token: Option<Vec<u8>>,
    ) -> Result<(Vec<UserInfo>, Option<Vec<u8>>)> {
        let pool = self.pool.clone();
        self.call_with_retry("list_users", || {
            let pool = pool.clone();
            let page_token = page_token.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::list_users(
                    wire_client,
                    request_id,
                    caller,
                    page_size,
                    page_token,
                )
                .await
            }
        })
        .await
    }

    /// Searches users by email.
    pub async fn search_users(
        &self,
        caller: UserSlug,
        email: impl Into<String>,
    ) -> Result<Vec<UserInfo>> {
        let email = email.into();
        let pool = self.pool.clone();
        self.call_with_retry("search_users", || {
            let pool = pool.clone();
            let email = email.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::search_users(wire_client, request_id, caller, email).await
            }
        })
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
        let email = email.into();
        let email_hmac = email_hmac.into();
        let pool = self.pool.clone();
        self.call_with_retry("create_user_email", || {
            let pool = pool.clone();
            let email = email.clone();
            let email_hmac = email_hmac.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::create_user_email(
                    wire_client,
                    request_id,
                    user,
                    email,
                    email_hmac,
                )
                .await
            }
        })
        .await
    }

    /// Deletes an email record from a user.
    pub async fn delete_user_email(&self, user: UserSlug, email_id: UserEmailId) -> Result<()> {
        let pool = self.pool.clone();
        self.call_with_retry("delete_user_email", || {
            let pool = pool.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::delete_user_email(wire_client, request_id, user, email_id)
                    .await
            }
        })
        .await
    }

    /// Searches user emails by user or email address.
    pub async fn search_user_email(
        &self,
        caller: UserSlug,
        user: Option<UserSlug>,
        email: Option<String>,
    ) -> Result<Vec<UserEmailInfo>> {
        let pool = self.pool.clone();
        self.call_with_retry("search_user_email", || {
            let pool = pool.clone();
            let email = email.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::search_user_email(
                    wire_client,
                    request_id,
                    caller,
                    user,
                    email,
                )
                .await
            }
        })
        .await
    }

    /// Verifies a user email using a verification token.
    pub async fn verify_user_email(&self, token: impl Into<String>) -> Result<UserEmailInfo> {
        let token = token.into();
        let pool = self.pool.clone();
        self.call_with_retry("verify_user_email", || {
            let pool = pool.clone();
            let token = token.clone();
            async move {
                let wire_client = crate::connected_wire_client!(pool);
                let request_id: u128 = rand::random();
                crate::ops_wire::user::verify_user_email(wire_client, request_id, token).await
            }
        })
        .await
    }
}
