//! User migration operations (flat `_system` to regional).

use chrono::Utc;
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{Operation, UserId, decode};
use snafu::ResultExt;
use tracing::warn;

use super::{
    CodecSnafu, Result, SYSTEM_VAULT_ID, StateSnafu, SystemKeys, SystemOrganizationService,
};
use crate::system::types::{
    MigrationSummary, User, UserDirectoryEntry, UserDirectoryStatus, UserEmail, UserMigrationEntry,
};

/// Maximum number of users returned by `list_flat_users` and `list_erased_user_ids`.
const MAX_LIST_USERS: usize = 100_000;

impl<B: StorageBackend> SystemOrganizationService<B> {
    // ========================================================================
    // User Migration (flat `_system` → regional)
    // ========================================================================

    /// Migrates pre-computed user entries from flat `_system` store to
    /// regional directory structure.
    ///
    /// For each entry in `entries`:
    /// 1. Creates `UserDirectoryEntry` in GLOBAL (`_dir:user:{id}`).
    /// 2. Creates slug index (`_idx:user:slug:{slug}`).
    /// 3. Creates email hash index (`_idx:email_hash:{hmac}`).
    /// 4. Stores per-user crypto-shredding key (`_shred:user:{id}`).
    /// 5. Removes old plaintext email index (`_idx:email:{email}`) for email records belonging to
    ///    this user.
    ///
    /// The caller (admin handler) pre-computes HMACs and shred keys so the
    /// blinding key never enters the Raft log.
    ///
    /// Returns a [`MigrationSummary`] counting total, migrated, skipped, errors.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] or [`super::SystemError::Codec`] if underlying
    /// operations fail.
    pub fn migrate_existing_users(
        &self,
        entries: &[UserMigrationEntry],
    ) -> Result<MigrationSummary> {
        let mut migrated: u64 = 0;
        let mut skipped: u64 = 0;
        let mut errors: u64 = 0;

        for entry in entries {
            // Idempotency: skip if directory entry already exists.
            match self.get_user_directory(entry.user) {
                Ok(Some(_)) => {
                    skipped += 1;
                    continue;
                },
                Ok(None) => {},
                Err(_) => {
                    errors += 1;
                    continue;
                },
            }

            // Step 1+2: Create directory entry and slug index.
            let directory_entry = UserDirectoryEntry {
                user: entry.user,
                slug: Some(entry.slug),
                region: Some(entry.region),
                status: UserDirectoryStatus::Active,
                updated_at: Some(Utc::now()),
            };
            if self.register_user_directory(&directory_entry).is_err() {
                errors += 1;
                continue;
            }

            // Step 3: Create email hash index (CAS for uniqueness).
            // AlreadyExists means another user owns this email — count as error.
            if self.register_email_hash(&entry.hmac, entry.user).is_err() {
                errors += 1;
                continue;
            }

            // Step 4: Store per-subject encryption key.
            if self.store_user_shred_key(entry.user, &entry.bytes).is_err() {
                errors += 1;
                continue;
            }

            // Step 5: Remove old plaintext email index entries for this user.
            // Non-fatal: directory/key entries already created.
            let _ = self.remove_plaintext_email_indexes_for_user(entry.user);

            migrated += 1;
        }

        Ok(MigrationSummary { users: entries.len() as u64, migrated, skipped, errors })
    }

    /// Removes plaintext `_idx:email:*` entries that reference emails belonging
    /// to the given user.
    ///
    /// Reads the `_idx:user_emails:{user_id}` index to find the user's email
    /// IDs, then reads each `user_email:{id}` record to get the email address,
    /// and deletes the corresponding `_idx:email:{email}` entries.
    fn remove_plaintext_email_indexes_for_user(&self, user_id: UserId) -> Result<()> {
        // Read the user_emails index to find email IDs.
        let emails_index_key = SystemKeys::user_emails_index_key(user_id);
        let emails_index_entity = self
            .state
            .get_entity(SYSTEM_VAULT_ID, emails_index_key.as_bytes())
            .context(StateSnafu)?;

        let Some(emails_entity) = emails_index_entity else {
            return Ok(());
        };

        // The index value is a list of email IDs (serialized).
        // Try to decode as a Vec<UserEmailId> first, fall back to
        // reading individual email records by scanning.
        let email_ids: Vec<inferadb_ledger_types::UserEmailId> = match decode(&emails_entity.value)
        {
            Ok(ids) => ids,
            Err(_) => return Ok(()), // Index format unrecognizable, skip cleanup.
        };

        let mut delete_ops: Vec<Operation> = Vec::new();
        for email_id in &email_ids {
            let email_key = SystemKeys::user_email_key(*email_id);
            if let Ok(Some(email_entity)) =
                self.state.get_entity(SYSTEM_VAULT_ID, email_key.as_bytes())
            {
                let user_email: UserEmail = match decode(&email_entity.value) {
                    Ok(ue) => ue,
                    Err(_) => continue,
                };
                let idx_key = SystemKeys::email_index_key(&user_email.email);
                delete_ops.push(Operation::DeleteEntity { key: idx_key });
            }
        }

        if !delete_ops.is_empty() {
            self.state.apply_operations(SYSTEM_VAULT_ID, &delete_ops, 0).context(StateSnafu)?;
        }

        Ok(())
    }

    /// Lists all user records from the flat `_system` store.
    ///
    /// Scans `user:*` keys in the system vault and deserializes each as a
    /// [`User`] record. Used by the migration admin handler to enumerate
    /// users that need migration. Returns at most 100,000 records.
    pub fn list_flat_users(&self) -> Result<Vec<User>> {
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(SystemKeys::USER_PREFIX), None, MAX_LIST_USERS)
            .context(StateSnafu)?;

        let mut users = Vec::new();
        for entity in &entities {
            let key_str = String::from_utf8_lossy(&entity.key);
            // Skip non-user keys that happen to share the prefix.
            if SystemKeys::parse_user_key(&key_str).is_none() {
                continue;
            }
            match decode::<User>(&entity.value) {
                Ok(user) => users.push(user),
                Err(e) => warn!(error = %e, "Skipping corrupt User entry during list_flat_users"),
            }
        }

        Ok(users)
    }

    /// Reads a single user email record by its email ID.
    ///
    /// Returns the [`UserEmail`] if found, `None` otherwise.
    pub fn get_user_email(
        &self,
        email_id: inferadb_ledger_types::UserEmailId,
    ) -> Result<Option<UserEmail>> {
        let key = SystemKeys::user_email_key(email_id);
        let entity = self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;

        match entity {
            Some(e) => {
                let user_email: UserEmail = decode(&e.value).context(CodecSnafu)?;
                Ok(Some(user_email))
            },
            None => Ok(None),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use chrono::Utc;
    use inferadb_ledger_types::{
        Operation, Region, TokenVersion, UserEmailId, UserId, UserRole, UserSlug, UserStatus,
        encode,
    };

    use crate::system::{
        keys::SystemKeys,
        service::{SYSTEM_VAULT_ID, SystemOrganizationService},
        types::{
            EmailHashEntry, User, UserDirectoryEntry, UserDirectoryStatus, UserEmail,
            UserMigrationEntry,
        },
    };

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        crate::system::create_test_service()
    }

    fn make_directory_entry(user_id: i64, slug: u64, region: Region) -> UserDirectoryEntry {
        UserDirectoryEntry {
            user: UserId::new(user_id),
            slug: Some(UserSlug::new(slug)),
            region: Some(region),
            status: UserDirectoryStatus::Active,
            updated_at: Some(Utc::now()),
        }
    }

    /// Helper: write a flat User record into the system vault.
    fn write_flat_user(
        svc: &SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>,
        user: &User,
    ) {
        let key = SystemKeys::user_key(user.id);
        let value = encode(user).unwrap();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        svc.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).unwrap();
    }

    /// Helper: write a flat UserEmail record into the system vault.
    fn write_flat_user_email(
        svc: &SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>,
        email: &UserEmail,
    ) {
        let key = SystemKeys::user_email_key(email.id);
        let value = encode(email).unwrap();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        svc.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).unwrap();
    }

    /// Helper: write a user_emails index entry (Vec<UserEmailId>).
    fn write_user_emails_index(
        svc: &SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>,
        user_id: UserId,
        email_ids: &[UserEmailId],
    ) {
        let key = SystemKeys::user_emails_index_key(user_id);
        let value = encode(&email_ids.to_vec()).unwrap();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        svc.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).unwrap();
    }

    /// Helper: write a plaintext email index entry.
    fn write_email_index(
        svc: &SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>,
        email: &str,
        email_id: UserEmailId,
    ) {
        let key = SystemKeys::email_index_key(email);
        let value = email_id.value().to_string().into_bytes();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        svc.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).unwrap();
    }

    fn make_test_user(id: i64, slug: u64, region: Region) -> User {
        User {
            id: UserId::new(id),
            slug: UserSlug::new(slug),
            region,
            name: format!("User {id}"),
            email: UserEmailId::new(id * 100),
            status: UserStatus::default(),
            role: UserRole::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
            version: TokenVersion::default(),
        }
    }

    fn make_test_user_email(id: i64, user_id: i64, email: &str) -> UserEmail {
        UserEmail {
            id: UserEmailId::new(id),
            user: UserId::new(user_id),
            email: email.to_string(),
            created_at: Utc::now(),
            verified_at: Some(Utc::now()),
        }
    }

    #[test]
    fn test_list_flat_users_empty() {
        let svc = create_test_service();
        let users = svc.list_flat_users().unwrap();
        assert!(users.is_empty());
    }

    #[test]
    fn test_list_flat_users() {
        let svc = create_test_service();
        let user1 = make_test_user(1, 10001, Region::US_EAST_VA);
        let user2 = make_test_user(2, 10002, Region::IE_EAST_DUBLIN);
        write_flat_user(&svc, &user1);
        write_flat_user(&svc, &user2);

        let users = svc.list_flat_users().unwrap();
        assert_eq!(users.len(), 2);
        let ids: Vec<i64> = users.iter().map(|u| u.id.value()).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }

    #[test]
    fn test_get_user_email() {
        let svc = create_test_service();
        let email_id = UserEmailId::new(100);
        let email = make_test_user_email(100, 1, "alice@example.com");
        write_flat_user_email(&svc, &email);

        let result = svc.get_user_email(email_id).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().email, "alice@example.com");
    }

    #[test]
    fn test_get_user_email_not_found() {
        let svc = create_test_service();
        let result = svc.get_user_email(UserEmailId::new(999)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_migrate_existing_users_single() {
        let svc = create_test_service();

        let entries = vec![UserMigrationEntry {
            user: UserId::new(1),
            slug: UserSlug::new(10001),
            region: Region::US_EAST_VA,
            hmac: "abc123def456".to_string(),
            bytes: [0xAAu8; 32],
        }];

        let summary = svc.migrate_existing_users(&entries).unwrap();
        assert_eq!(summary.users, 1);
        assert_eq!(summary.migrated, 1);
        assert_eq!(summary.skipped, 0);
        assert_eq!(summary.errors, 0);

        // Verify directory entry created.
        let dir = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(dir.user, UserId::new(1));
        assert_eq!(dir.slug, Some(UserSlug::new(10001)));
        assert_eq!(dir.region, Some(Region::US_EAST_VA));
        assert_eq!(dir.status, UserDirectoryStatus::Active);

        // Verify slug index created.
        let found_id = svc.get_user_id_by_slug(UserSlug::new(10001)).unwrap();
        assert_eq!(found_id, Some(UserId::new(1)));

        // Verify email hash index created.
        let email_hash_user = svc.get_email_hash("abc123def456").unwrap();
        assert_eq!(email_hash_user, Some(EmailHashEntry::Active(UserId::new(1))));

        // Verify user shred key stored.
        let shred_key = svc.get_user_shred_key(UserId::new(1)).unwrap().unwrap();
        assert_eq!(shred_key.key, [0xAAu8; 32]);
    }

    #[test]
    fn test_migrate_existing_users_idempotent() {
        let svc = create_test_service();

        let entries = vec![UserMigrationEntry {
            user: UserId::new(1),
            slug: UserSlug::new(10001),
            region: Region::US_EAST_VA,
            hmac: "abc123".to_string(),
            bytes: [0xBBu8; 32],
        }];

        // First run: migrates.
        let summary1 = svc.migrate_existing_users(&entries).unwrap();
        assert_eq!(summary1.migrated, 1);
        assert_eq!(summary1.skipped, 0);

        // Second run: skips (directory entry exists).
        let summary2 = svc.migrate_existing_users(&entries).unwrap();
        assert_eq!(summary2.migrated, 0);
        assert_eq!(summary2.skipped, 1);
    }

    #[test]
    fn test_migrate_existing_users_empty() {
        let svc = create_test_service();
        let summary = svc.migrate_existing_users(&[]).unwrap();
        assert_eq!(summary.users, 0);
        assert_eq!(summary.migrated, 0);
        assert_eq!(summary.skipped, 0);
        assert_eq!(summary.errors, 0);
    }

    #[test]
    fn test_migrate_existing_users_multiple() {
        let svc = create_test_service();

        let entries = vec![
            UserMigrationEntry {
                user: UserId::new(1),
                slug: UserSlug::new(10001),
                region: Region::US_EAST_VA,
                hmac: "hash1".to_string(),
                bytes: [0x11u8; 32],
            },
            UserMigrationEntry {
                user: UserId::new(2),
                slug: UserSlug::new(10002),
                region: Region::IE_EAST_DUBLIN,
                hmac: "hash2".to_string(),
                bytes: [0x22u8; 32],
            },
            UserMigrationEntry {
                user: UserId::new(3),
                slug: UserSlug::new(10003),
                region: Region::JP_EAST_TOKYO,
                hmac: "hash3".to_string(),
                bytes: [0x33u8; 32],
            },
        ];

        let summary = svc.migrate_existing_users(&entries).unwrap();
        assert_eq!(summary.users, 3);
        assert_eq!(summary.migrated, 3);
        assert_eq!(summary.skipped, 0);
        assert_eq!(summary.errors, 0);

        // Verify all directory entries.
        for (i, entry) in entries.iter().enumerate() {
            let dir = svc.get_user_directory(entry.user).unwrap().unwrap();
            assert_eq!(dir.region, Some(entry.region), "user {} region mismatch", i);
        }
    }

    #[test]
    fn test_migrate_existing_users_mixed_skip_and_migrate() {
        let svc = create_test_service();

        // Pre-register user 1's directory entry.
        let existing = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&existing).unwrap();

        let entries = vec![
            UserMigrationEntry {
                user: UserId::new(1),
                slug: UserSlug::new(10001),
                region: Region::US_EAST_VA,
                hmac: "hash1".to_string(),
                bytes: [0x11u8; 32],
            },
            UserMigrationEntry {
                user: UserId::new(2),
                slug: UserSlug::new(10002),
                region: Region::IE_EAST_DUBLIN,
                hmac: "hash2".to_string(),
                bytes: [0x22u8; 32],
            },
        ];

        let summary = svc.migrate_existing_users(&entries).unwrap();
        assert_eq!(summary.users, 2);
        assert_eq!(summary.migrated, 1);
        assert_eq!(summary.skipped, 1);
        assert_eq!(summary.errors, 0);

        // User 2 should be migrated.
        assert!(svc.get_user_directory(UserId::new(2)).unwrap().is_some());
    }

    #[test]
    fn test_remove_plaintext_email_indexes_for_user() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let email_id = UserEmailId::new(100);

        // Write a flat user email record.
        let user_email = make_test_user_email(100, 1, "alice@example.com");
        write_flat_user_email(&svc, &user_email);

        // Write the user_emails index.
        write_user_emails_index(&svc, user_id, &[email_id]);

        // Write the plaintext email index.
        write_email_index(&svc, "alice@example.com", email_id);

        // Verify the index exists.
        let idx_key = SystemKeys::email_index_key("alice@example.com");
        let before = svc.state.get_entity(SYSTEM_VAULT_ID, idx_key.as_bytes()).unwrap();
        assert!(before.is_some());

        // Remove plaintext email indexes.
        svc.remove_plaintext_email_indexes_for_user(user_id).unwrap();

        // Verify the index is gone.
        let after = svc.state.get_entity(SYSTEM_VAULT_ID, idx_key.as_bytes()).unwrap();
        assert!(after.is_none());
    }

    #[test]
    fn test_remove_plaintext_email_indexes_no_index() {
        let svc = create_test_service();
        // No user_emails index exists — should be a no-op.
        svc.remove_plaintext_email_indexes_for_user(UserId::new(999)).unwrap();
    }
}
