//! User CRUD operations and verification token lookups.

use chrono::Utc;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    EmailVerifyTokenId, Operation, Region, SetCondition, TokenVersion, UserEmailId, UserId,
    UserRole, UserSlug, UserStatus, decode, encode,
};
use snafu::ResultExt;
use tracing::warn;

use super::{
    CodecSnafu, KeyTier, Result, SYSTEM_VAULT_ID, StateSnafu, SystemError, SystemKeys,
    SystemOrganizationService, require_tier,
};
use crate::{
    state::StateError,
    system::types::{
        EmailVerificationToken, User, UserDirectoryEntry, UserDirectoryStatus, UserEmail,
    },
};

impl<B: StorageBackend> SystemOrganizationService<B> {
    // =========================================================================
    // User CRUD Operations
    // =========================================================================

    /// Creates a user and their primary email record atomically.
    ///
    /// Writes the user record, email record, user-to-emails index, email
    /// uniqueness index, and directory entry. The email uniqueness index uses
    /// `MustNotExist` to prevent duplicate email addresses.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::AlreadyExists`] if the email address is already
    /// registered, [`super::SystemError::Codec`] if serialization fails, or
    /// [`super::SystemError::State`] for other storage failures.
    pub fn create_user(
        &self,
        name: &str,
        email: &str,
        region: Region,
        role: UserRole,
        slug: UserSlug,
    ) -> Result<(UserId, UserEmailId)> {
        let user_id = self.next_sequence(SystemKeys::USER_SEQ_KEY, 1).map(UserId::new)?;
        let email_id =
            self.next_sequence(SystemKeys::USER_EMAIL_SEQ_KEY, 1).map(UserEmailId::new)?;

        let now = Utc::now();
        let user = User {
            id: user_id,
            slug,
            region,
            name: name.to_string(),
            email: email_id,
            status: UserStatus::Active,
            role,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            version: TokenVersion::default(),
        };
        let user_key = SystemKeys::user_key(user_id);
        require_tier(&user_key, KeyTier::Regional)?;
        let user_value = encode(&user).context(CodecSnafu)?;

        let email_lower = email.to_lowercase();
        let user_email = UserEmail {
            id: email_id,
            user: user_id,
            email: email_lower.clone(),
            created_at: now,
            verified_at: None,
        };
        let email_key = SystemKeys::user_email_key(email_id);
        require_tier(&email_key, KeyTier::Regional)?;
        let email_value = encode(&user_email).context(CodecSnafu)?;

        let emails_index_key = SystemKeys::user_emails_index_key(user_id);
        require_tier(&emails_index_key, KeyTier::Regional)?;
        let email_ids: Vec<UserEmailId> = vec![email_id];
        let emails_index_value = encode(&email_ids).context(CodecSnafu)?;

        let email_idx_key = SystemKeys::email_index_key(&email_lower);
        require_tier(&email_idx_key, KeyTier::Regional)?;
        let email_idx_value = email_id.value().to_string().into_bytes();

        let ops = vec![
            Operation::SetEntity {
                key: user_key,
                value: user_value,
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: email_key,
                value: email_value,
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: emails_index_key,
                value: emails_index_value,
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: email_idx_key,
                value: email_idx_value,
                condition: Some(SetCondition::MustNotExist),
                expires_at: None,
            },
        ];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).map_err(|e| {
            if matches!(e, StateError::PreconditionFailed { .. }) {
                SystemError::AlreadyExists { entity: format!("email:{email_lower}") }
            } else {
                SystemError::State { source: Box::new(e) }
            }
        })?;

        let dir_entry = UserDirectoryEntry {
            user: user_id,
            slug: Some(slug),
            region: Some(region),
            status: UserDirectoryStatus::Active,
            updated_at: Some(now),
        };
        self.register_user_directory(&dir_entry)?;

        Ok((user_id, email_id))
    }

    /// Returns a user by internal ID.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if the read fails, or
    /// [`super::SystemError::Codec`] if deserialization fails.
    pub fn get_user(&self, user_id: UserId) -> Result<Option<User>> {
        let key = SystemKeys::user_key(user_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let user: User = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(user))
            },
            None => Ok(None),
        }
    }

    /// Returns all email records for a user.
    ///
    /// Reads the user-to-emails index, then fetches each email record by ID.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if any read fails, or
    /// [`super::SystemError::Codec`] if deserialization fails.
    pub fn get_user_emails(&self, user_id: UserId) -> Result<Vec<UserEmail>> {
        let email_ids = self.get_user_email_ids(user_id)?;
        let mut emails = Vec::new();
        for email_id in email_ids {
            if let Some(email) = self.get_user_email(email_id)? {
                emails.push(email);
            }
        }
        Ok(emails)
    }

    /// Updates a user's role or primary email (non-PII fields).
    ///
    /// Proposed to the GLOBAL Raft group. At least one field must be `Some`.
    /// When changing the primary email, verifies that the email belongs to
    /// this user.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::NotFound`] if the user does not exist or the
    /// specified email is not owned by the user, [`super::SystemError::Codec`] if
    /// serialization fails, or [`super::SystemError::State`] for storage failures.
    pub fn update_user(
        &self,
        user_id: UserId,
        role: Option<UserRole>,
        primary_email: Option<UserEmailId>,
    ) -> Result<User> {
        let mut user = self
            .get_user(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user:{user_id}") })?;

        if let Some(role) = role {
            user.role = role;
        }
        if let Some(email_id) = primary_email {
            let email = self.get_user_email(email_id)?.ok_or_else(|| SystemError::NotFound {
                entity: format!("user_email:{email_id}"),
            })?;
            if email.user != user_id {
                return Err(SystemError::NotFound {
                    entity: format!("user_email:{email_id} not owned by user:{user_id}"),
                });
            }
            user.email = email_id;
        }
        user.updated_at = Utc::now();

        let key = SystemKeys::user_key(user_id);
        let value = encode(&user).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(user)
    }

    /// Updates a user's display name (PII field).
    ///
    /// Proposed to the REGIONAL Raft group to keep PII out of the GLOBAL log.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::NotFound`] if the user does not exist,
    /// [`super::SystemError::Codec`] if serialization fails, or
    /// [`super::SystemError::State`] for storage failures.
    pub fn update_user_profile(&self, user_id: UserId, name: &str) -> Result<User> {
        let mut user = self
            .get_user(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user:{user_id}") })?;

        user.name = name.to_string();
        user.updated_at = Utc::now();

        let key = SystemKeys::user_key(user_id);
        let value = encode(&user).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(user)
    }

    /// Initiates soft-delete by setting the user status to `Deleting`.
    ///
    /// Records `deleted_at` timestamp. Rejects users already in `Deleting`
    /// or `Deleted` state.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::NotFound`] if the user does not exist,
    /// [`super::SystemError::AlreadyExists`] if already in a deletion state,
    /// [`super::SystemError::Codec`] if serialization fails, or
    /// [`super::SystemError::State`] for storage failures.
    pub fn soft_delete_user(&self, user_id: UserId) -> Result<User> {
        let mut user = self
            .get_user(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user:{user_id}") })?;

        if user.status == UserStatus::Deleting || user.status == UserStatus::Deleted {
            return Err(SystemError::AlreadyExists {
                entity: format!("user:{user_id} is already in deletion state"),
            });
        }

        let now = Utc::now();
        user.status = UserStatus::Deleting;
        user.deleted_at = Some(now);
        user.updated_at = now;

        let key = SystemKeys::user_key(user_id);
        let value = encode(&user).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(user)
    }

    /// Lists users with optional pagination.
    ///
    /// Scans user records by prefix, filtering out non-user keys that share
    /// the prefix.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if the scan fails.
    pub fn list_users(&self, start_after: Option<&str>, limit: usize) -> Result<Vec<User>> {
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(SystemKeys::USER_PREFIX), start_after, limit)
            .context(StateSnafu)?;

        let mut users = Vec::new();
        for entity in &entities {
            let key_str = String::from_utf8_lossy(&entity.key);
            if SystemKeys::parse_user_key(&key_str).is_none() {
                continue;
            }
            match decode::<User>(&entity.value) {
                Ok(user) => users.push(user),
                Err(e) => warn!(error = %e, "Skipping corrupt User entry during list_erased_users"),
            }
        }
        Ok(users)
    }

    /// Searches for a user by email address.
    ///
    /// Looks up the email uniqueness index, then reads the email record to
    /// find the owning user.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if the read fails, or
    /// [`super::SystemError::Codec`] if deserialization fails.
    pub fn search_users_by_email(&self, email: &str) -> Result<Option<User>> {
        let email_lower = email.to_lowercase();
        let idx_key = SystemKeys::email_index_key(&email_lower);

        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, idx_key.as_bytes()).context(StateSnafu)?;
        let Some(entity) = entity_opt else {
            return Ok(None);
        };

        let email_id_str = String::from_utf8_lossy(&entity.value);
        let email_id: i64 = email_id_str
            .parse()
            .map_err(|_| SystemError::NotFound { entity: format!("email_index:{email_lower}") })?;
        let email_record = self.get_user_email(UserEmailId::new(email_id))?;

        match email_record {
            Some(email_rec) => self.get_user(email_rec.user),
            None => Ok(None),
        }
    }

    /// Adds an email address to an existing user.
    ///
    /// Creates the email record, email uniqueness index (with CAS), and
    /// updates the user-to-emails index.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::NotFound`] if the user does not exist,
    /// [`super::SystemError::State`] for storage failures (including duplicate
    /// email via CAS rejection), or [`super::SystemError::Codec`] if serialization
    /// fails.
    pub fn create_user_email_record(&self, user_id: UserId, email: &str) -> Result<UserEmailId> {
        let _ = self
            .get_user(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user:{user_id}") })?;

        let email_id =
            self.next_sequence(SystemKeys::USER_EMAIL_SEQ_KEY, 1).map(UserEmailId::new)?;
        let now = Utc::now();
        let email_lower = email.to_lowercase();

        let user_email = UserEmail {
            id: email_id,
            user: user_id,
            email: email_lower.clone(),
            created_at: now,
            verified_at: None,
        };
        let email_key = SystemKeys::user_email_key(email_id);
        let email_value = encode(&user_email).context(CodecSnafu)?;

        let idx_key = SystemKeys::email_index_key(&email_lower);
        let idx_value = email_id.value().to_string().into_bytes();

        let ops = vec![
            Operation::SetEntity {
                key: email_key,
                value: email_value,
                condition: None,
                expires_at: None,
            },
            Operation::SetEntity {
                key: idx_key,
                value: idx_value,
                condition: Some(SetCondition::MustNotExist),
                expires_at: None,
            },
        ];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).map_err(|e| {
            if matches!(e, StateError::PreconditionFailed { .. }) {
                SystemError::AlreadyExists { entity: format!("email:{email_lower}") }
            } else {
                SystemError::State { source: Box::new(e) }
            }
        })?;

        // Update user-to-emails index.
        let mut email_ids = self.get_user_email_ids(user_id)?;
        email_ids.push(email_id);
        let index_key = SystemKeys::user_emails_index_key(user_id);
        let index_value = encode(&email_ids).context(CodecSnafu)?;
        let index_ops = vec![Operation::SetEntity {
            key: index_key,
            value: index_value,
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &index_ops, 0).context(StateSnafu)?;

        Ok(email_id)
    }

    /// Removes a non-primary email address from a user.
    ///
    /// Deletes the email record, email uniqueness index, and updates the
    /// user-to-emails index. Refuses to delete the user's primary email.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::AlreadyExists`] if attempting to delete the
    /// primary email, [`super::SystemError::NotFound`] if the user or email does not
    /// exist or is not owned by this user, [`super::SystemError::Codec`] if
    /// serialization fails, or [`super::SystemError::State`] for storage failures.
    pub fn delete_user_email_record(&self, user_id: UserId, email_id: UserEmailId) -> Result<()> {
        let user = self
            .get_user(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user:{user_id}") })?;

        if user.email == email_id {
            return Err(SystemError::AlreadyExists {
                entity: format!("user_email:{email_id} is the primary email and cannot be deleted"),
            });
        }

        let email = self
            .get_user_email(email_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user_email:{email_id}") })?;

        if email.user != user_id {
            return Err(SystemError::NotFound {
                entity: format!("user_email:{email_id} not owned by user:{user_id}"),
            });
        }

        let email_key = SystemKeys::user_email_key(email_id);
        let idx_key = SystemKeys::email_index_key(&email.email);
        let ops = vec![
            Operation::DeleteEntity { key: email_key },
            Operation::DeleteEntity { key: idx_key },
        ];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        // Update user-to-emails index.
        let mut email_ids = self.get_user_email_ids(user_id)?;
        email_ids.retain(|id| *id != email_id);
        let index_key = SystemKeys::user_emails_index_key(user_id);
        let index_value = encode(&email_ids).context(CodecSnafu)?;
        let index_ops = vec![Operation::SetEntity {
            key: index_key,
            value: index_value,
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &index_ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Sets the `verified_at` timestamp on an email record.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::NotFound`] if the email record does not exist,
    /// [`super::SystemError::Codec`] if serialization fails, or
    /// [`super::SystemError::State`] for storage failures.
    pub fn verify_user_email_record(&self, email_id: UserEmailId) -> Result<UserEmail> {
        let mut email = self
            .get_user_email(email_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user_email:{email_id}") })?;

        email.verified_at = Some(Utc::now());

        let key = SystemKeys::user_email_key(email_id);
        let value = encode(&email).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(email)
    }

    /// Reads the user-to-emails index for a given user.
    pub(super) fn get_user_email_ids(&self, user_id: UserId) -> Result<Vec<UserEmailId>> {
        let key = SystemKeys::user_emails_index_key(user_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let ids: Vec<UserEmailId> = decode(&entity.value).context(CodecSnafu)?;
                Ok(ids)
            },
            None => Ok(Vec::new()),
        }
    }

    // ========================================================================
    // Verification Token Lookups
    // ========================================================================

    /// Looks up a verification token by hashing the plaintext token and
    /// querying the hash index.
    ///
    /// Returns the full [`EmailVerificationToken`] record if found, or `None`
    /// if no token matches the hash.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] for storage failures or
    /// [`super::SystemError::Codec`] if deserialization fails.
    pub fn get_verification_token_by_hash(
        &self,
        plaintext_token: &str,
    ) -> Result<Option<EmailVerificationToken>> {
        let token_hash = inferadb_ledger_types::sha256(plaintext_token.as_bytes());
        let hash_hex = {
            use std::fmt::Write;
            token_hash.iter().fold(String::with_capacity(64), |mut acc, b| {
                let _ = write!(acc, "{b:02x}");
                acc
            })
        };
        let index_key = SystemKeys::email_verify_hash_index_key(&hash_hex);

        let index_entity =
            self.state.get_entity(SYSTEM_VAULT_ID, index_key.as_bytes()).context(StateSnafu)?;

        let Some(entity) = index_entity else {
            return Ok(None);
        };

        let token_id: EmailVerifyTokenId = decode(&entity.value).context(CodecSnafu)?;
        let token_key = SystemKeys::email_verify_key(token_id);

        let token_entity =
            self.state.get_entity(SYSTEM_VAULT_ID, token_key.as_bytes()).context(StateSnafu)?;

        match token_entity {
            Some(entity) => {
                let token: EmailVerificationToken = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(token))
            },
            None => Ok(None),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_types::{Region, UserId, UserRole, UserSlug, UserStatus};

    use crate::system::{service::SystemOrganizationService, types::UserDirectoryStatus};

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        crate::system::create_test_service()
    }

    #[test]
    fn test_create_user_and_get() {
        let svc = create_test_service();
        let slug = UserSlug::new(100);
        let (user_id, email_id) = svc
            .create_user("Alice", "alice@example.com", Region::US_EAST_VA, UserRole::User, slug)
            .unwrap();

        let user = svc.get_user(user_id).unwrap().unwrap();
        assert_eq!(user.name, "Alice");
        assert_eq!(user.slug, slug);
        assert_eq!(user.email, email_id);
        assert_eq!(user.status, UserStatus::Active);
        assert!(user.deleted_at.is_none());
    }

    #[test]
    fn test_create_user_creates_email() {
        let svc = create_test_service();
        let (user_id, email_id) = svc
            .create_user(
                "Bob",
                "bob@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(200),
            )
            .unwrap();

        let emails = svc.get_user_emails(user_id).unwrap();
        assert_eq!(emails.len(), 1);
        assert_eq!(emails[0].email, "bob@example.com");
        assert_eq!(emails[0].id, email_id);
    }

    #[test]
    fn test_create_user_creates_directory_entry() {
        let svc = create_test_service();
        let slug = UserSlug::new(300);
        let (user_id, _) = svc
            .create_user(
                "Charlie",
                "charlie@example.com",
                Region::IE_EAST_DUBLIN,
                UserRole::User,
                slug,
            )
            .unwrap();

        let dir = svc.get_user_directory(user_id).unwrap().unwrap();
        assert_eq!(dir.slug, Some(slug));
        assert_eq!(dir.region, Some(Region::IE_EAST_DUBLIN));
        assert_eq!(dir.status, UserDirectoryStatus::Active);
    }

    #[test]
    fn test_create_user_duplicate_email_fails() {
        let svc = create_test_service();
        svc.create_user(
            "Alice",
            "same@example.com",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(100),
        )
        .unwrap();

        let result = svc.create_user(
            "Bob",
            "same@example.com",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(200),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_update_user_name() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let updated = svc.update_user_profile(user_id, "Alicia").unwrap();
        assert_eq!(updated.name, "Alicia");
    }

    #[test]
    fn test_update_user_role() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let updated = svc.update_user(user_id, Some(UserRole::Admin), None).unwrap();
        assert_eq!(updated.role, UserRole::Admin);
    }

    #[test]
    fn test_update_user_primary_email() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let new_email_id = svc.create_user_email_record(user_id, "alice2@example.com").unwrap();
        let updated = svc.update_user(user_id, None, Some(new_email_id)).unwrap();
        assert_eq!(updated.email, new_email_id);
    }

    #[test]
    fn test_update_user_primary_email_wrong_owner() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();
        let (_, other_email_id) = svc
            .create_user(
                "Bob",
                "bob@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(200),
            )
            .unwrap();

        let result = svc.update_user(user_id, None, Some(other_email_id));
        assert!(result.is_err());
    }

    #[test]
    fn test_update_user_profile_not_found() {
        let svc = create_test_service();
        let result = svc.update_user_profile(UserId::new(9999), "NewName");
        assert!(result.is_err());
    }

    #[test]
    fn test_soft_delete_user() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let deleted = svc.soft_delete_user(user_id).unwrap();
        assert_eq!(deleted.status, UserStatus::Deleting);
        assert!(deleted.deleted_at.is_some());
    }

    #[test]
    fn test_soft_delete_already_deleting() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        svc.soft_delete_user(user_id).unwrap();
        let result = svc.soft_delete_user(user_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_users() {
        let svc = create_test_service();
        svc.create_user(
            "Alice",
            "alice@example.com",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(100),
        )
        .unwrap();
        svc.create_user(
            "Bob",
            "bob@example.com",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(200),
        )
        .unwrap();

        let users = svc.list_users(None, 100).unwrap();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn test_search_users_by_email() {
        let svc = create_test_service();
        svc.create_user(
            "Alice",
            "alice@example.com",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(100),
        )
        .unwrap();

        let found = svc.search_users_by_email("alice@example.com").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Alice");
    }

    #[test]
    fn test_search_users_by_email_not_found() {
        let svc = create_test_service();
        let found = svc.search_users_by_email("nobody@example.com").unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_create_user_email_record() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let email_id = svc.create_user_email_record(user_id, "alice2@example.com").unwrap();
        let emails = svc.get_user_emails(user_id).unwrap();
        assert_eq!(emails.len(), 2);
        assert!(emails.iter().any(|e| e.id == email_id && e.email == "alice2@example.com"));
    }

    #[test]
    fn test_delete_user_email_record() {
        let svc = create_test_service();
        let (user_id, _) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let email_id = svc.create_user_email_record(user_id, "alice2@example.com").unwrap();
        svc.delete_user_email_record(user_id, email_id).unwrap();

        let emails = svc.get_user_emails(user_id).unwrap();
        assert_eq!(emails.len(), 1); // Only primary remains
    }

    #[test]
    fn test_delete_primary_email_fails() {
        let svc = create_test_service();
        let (user_id, primary_email_id) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let result = svc.delete_user_email_record(user_id, primary_email_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_user_email() {
        let svc = create_test_service();
        let (_, email_id) = svc
            .create_user(
                "Alice",
                "alice@example.com",
                Region::US_EAST_VA,
                UserRole::User,
                UserSlug::new(100),
            )
            .unwrap();

        let email = svc.get_user_email(email_id).unwrap().unwrap();
        assert!(email.verified_at.is_none());

        let verified = svc.verify_user_email_record(email_id).unwrap();
        assert!(verified.verified_at.is_some());
    }

    #[test]
    fn test_search_users_by_email_case_insensitive() {
        let svc = create_test_service();
        svc.create_user(
            "Alice",
            "Alice@Example.COM",
            Region::US_EAST_VA,
            UserRole::User,
            UserSlug::new(100),
        )
        .unwrap();

        let found = svc.search_users_by_email("alice@example.com").unwrap();
        assert!(found.is_some());
    }

    #[test]
    fn test_get_user_directory_by_slug() {
        let svc = create_test_service();
        let slug = UserSlug::new(500);
        let (user_id, _) = svc
            .create_user("Alice", "alice@example.com", Region::US_EAST_VA, UserRole::User, slug)
            .unwrap();

        let dir = svc.get_user_directory_by_slug(slug).unwrap().unwrap();
        assert_eq!(dir.user, user_id);
    }
}
