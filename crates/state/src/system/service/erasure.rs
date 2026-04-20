//! Crypto-shredding erasure operations (GLOBAL control plane).

use chrono::{DateTime, Utc};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{Operation, OrganizationId, Region, UserId, decode, encode};
use snafu::ResultExt;

use super::{
    CodecSnafu, KeyTier, Result, SYSTEM_VAULT_ID, StateSnafu, SystemKeys,
    SystemOrganizationService, require_tier,
};
use crate::system::types::{
    EmailHashEntry, ErasureAuditRecord, OrgShredKey, UserDirectoryStatus, UserShredKey,
};

/// Maximum number of email hash entries scanned by `erase_user`.
const MAX_EMAIL_HASH_SCAN: usize = 100_000;
/// Maximum number of erasure audit records returned.
const MAX_LIST_ERASURE_AUDITS: usize = 100_000;

impl<B: StorageBackend> SystemOrganizationService<B> {
    /// Erases a user's PII via crypto-shredding finalization sequence.
    ///
    /// # Scope
    ///
    /// This operation erases **user-level** PII only: the user record,
    /// email records, credentials, TOTP challenges, and the per-user shred
    /// key. It does **not** cascade to organization-level PII (app profiles,
    /// organization profiles, teams) because:
    ///
    /// 1. Those records are organization-scoped, not user-scoped — deleting them during a single
    ///    user's erasure would affect other users who share the organization.
    /// 2. Organization-scoped cleanup runs through the separate `PurgeOrganizationRegional` system
    ///    request, which handles `app_profile:`, `org_profile:`, team records, and assertion name
    ///    indices atomically. That request is triggered when the organization itself is deleted,
    ///    not when a member is erased.
    ///
    /// # Ownership model limitation
    ///
    /// The state layer does not currently track user-to-organization
    /// membership (users have a global `role` field, not per-org roles), so
    /// "sole admin" cascade logic cannot be expressed here. If an
    /// organization becomes orphaned because its only admin was erased, the
    /// operator must explicitly purge the organization via the admin API.
    /// Extending to per-org roles is a separate RFC.
    ///
    /// # Sequence
    ///
    /// Forward-only, idempotent on crash-resume. Steps:
    /// 1. Read directory entry to capture slug.
    /// 2. Mark directory entry as `Deleted`.
    /// 3. Revoke all refresh token families and bump token version.
    /// 4. Remove global email hash index entries for this user.
    /// 5. Delete all `UserEmail` records and indices.
    /// 6. Delete all user credentials and type indices.
    /// 7. Delete all pending TOTP challenges.
    /// 8. Delete per-subject encryption key.
    /// 9. Delete the User record.
    /// 10. Write erasure audit record.
    pub fn erase_user(
        &self,
        user_id: UserId,
        region: Region,
        block_timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let entry_opt = self.get_user_directory(user_id)?;

        if let Some(entry) = &entry_opt
            && entry.status != UserDirectoryStatus::Deleted
        {
            self.update_user_directory_status(
                user_id,
                UserDirectoryStatus::Deleted,
                block_timestamp,
            )?;
        }

        if let Some(entry) = &entry_opt
            && let Some(slug) = entry.slug
            && let Err(e) = self.revoke_all_user_sessions(user_id, slug, block_timestamp)
        {
            tracing::warn!(user_id = user_id.value(), error = %e, "Failed to revoke user sessions during erasure");
        }

        // Step 4: Remove global email hash index entries.
        let email_hashes = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::EMAIL_HASH_INDEX_PREFIX),
                None,
                MAX_EMAIL_HASH_SCAN,
            )
            .context(StateSnafu)?;
        let mut delete_ops: Vec<Operation> = Vec::new();
        for entity in &email_hashes {
            let belongs_to_user = match decode::<EmailHashEntry>(&entity.value) {
                Ok(EmailHashEntry::Active(uid)) => uid == user_id,
                Ok(EmailHashEntry::Provisioning(ref res)) => res.user_id == user_id,
                Err(_) => false,
            };
            if belongs_to_user {
                let key = String::from_utf8_lossy(&entity.key).to_string();
                require_tier(&key, KeyTier::Global)?;
                delete_ops.push(Operation::DeleteEntity { key });
            }
        }
        if !delete_ops.is_empty() {
            self.state
                .apply_operations_lazy(SYSTEM_VAULT_ID, &delete_ops, 0)
                .context(StateSnafu)?;
        }

        // Step 5: Delete all UserEmail records and their plaintext indices.
        let email_ids = self.get_user_email_ids(user_id)?;
        if !email_ids.is_empty() {
            let mut email_delete_ops: Vec<Operation> = Vec::new();
            for email_id in &email_ids {
                if let Some(email_record) = self.get_user_email(*email_id)? {
                    let idx_key = SystemKeys::email_index_key(&email_record.email);
                    require_tier(&idx_key, KeyTier::Regional)?;
                    email_delete_ops.push(Operation::DeleteEntity { key: idx_key });
                }
                let ue_key = SystemKeys::user_email_key(*email_id);
                require_tier(&ue_key, KeyTier::Regional)?;
                email_delete_ops.push(Operation::DeleteEntity { key: ue_key });
            }
            let ue_idx_key = SystemKeys::user_emails_index_key(user_id);
            require_tier(&ue_idx_key, KeyTier::Regional)?;
            email_delete_ops.push(Operation::DeleteEntity { key: ue_idx_key });
            self.state
                .apply_operations_lazy(SYSTEM_VAULT_ID, &email_delete_ops, 0)
                .context(StateSnafu)?;
        }

        // Step 6: Delete all user credentials and type index entries.
        if let Err(e) = self.delete_all_user_credentials(user_id) {
            tracing::warn!(user_id = user_id.value(), error = %e, "Failed to delete user credentials during erasure");
        }

        // Step 7: Delete all pending TOTP challenges.
        if let Err(e) = self.delete_all_totp_challenges(user_id) {
            tracing::warn!(user_id = user_id.value(), error = %e, "Failed to delete TOTP challenges during erasure");
        }

        // Step 8: Delete per-user crypto-shredding key.
        let shred_key = SystemKeys::user_shred_key(user_id);
        require_tier(&shred_key, KeyTier::Regional)?;
        let delete_key_ops = vec![Operation::DeleteEntity { key: shred_key }];
        self.state
            .apply_operations_lazy(SYSTEM_VAULT_ID, &delete_key_ops, 0)
            .context(StateSnafu)?;

        // Step 9: Delete the User record.
        let user_key = SystemKeys::user_key(user_id);
        require_tier(&user_key, KeyTier::Regional)?;
        let delete_user_ops = vec![Operation::DeleteEntity { key: user_key }];
        self.state
            .apply_operations_lazy(SYSTEM_VAULT_ID, &delete_user_ops, 0)
            .context(StateSnafu)?;

        // Step 10: Write erasure audit record.
        let audit_record = ErasureAuditRecord { user_id, erased_at: block_timestamp, region };
        let audit_key = SystemKeys::erasure_audit_key(user_id);
        require_tier(&audit_key, KeyTier::Global)?;
        let audit_value = encode(&audit_record).context(CodecSnafu)?;
        let audit_ops = vec![Operation::SetEntity {
            key: audit_key,
            value: audit_value,
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &audit_ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Returns the erasure audit record for a user, if one exists.
    pub fn get_erasure_audit(&self, user_id: UserId) -> Result<Option<ErasureAuditRecord>> {
        let key = SystemKeys::erasure_audit_key(user_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let record: ErasureAuditRecord = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(record))
            },
            None => Ok(None),
        }
    }

    /// Stores a per-user crypto-shredding key.
    pub fn store_user_shred_key(&self, user_id: UserId, key_bytes: &[u8; 32]) -> Result<()> {
        let shred_key = UserShredKey { user_id, key: *key_bytes, created_at: Utc::now() };
        let storage_key = SystemKeys::user_shred_key(user_id);
        require_tier(&storage_key, KeyTier::Regional)?;
        let value = encode(&shred_key).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity {
            key: storage_key,
            value,
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Returns a per-user crypto-shredding key, if one exists.
    pub fn get_user_shred_key(&self, user_id: UserId) -> Result<Option<UserShredKey>> {
        let key = SystemKeys::user_shred_key(user_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let shred_key: UserShredKey = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(shred_key))
            },
            None => Ok(None),
        }
    }

    /// Stores a per-organization crypto-shredding key.
    pub fn store_org_shred_key(
        &self,
        organization: OrganizationId,
        key_bytes: &[u8; 32],
    ) -> Result<()> {
        let shred_key = OrgShredKey { organization, key: *key_bytes, created_at: Utc::now() };
        let storage_key = SystemKeys::org_shred_key(organization);
        require_tier(&storage_key, KeyTier::Regional)?;
        let value = encode(&shred_key).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity {
            key: storage_key,
            value,
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Returns a per-organization crypto-shredding key, if one exists.
    pub fn get_org_shred_key(&self, organization: OrganizationId) -> Result<Option<OrgShredKey>> {
        let key = SystemKeys::org_shred_key(organization);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let shred_key: OrgShredKey = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(shred_key))
            },
            None => Ok(None),
        }
    }

    /// Returns the set of erased user IDs by scanning erasure audit records.
    pub fn list_erased_user_ids(&self) -> Result<std::collections::HashSet<UserId>> {
        let entities = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::ERASURE_AUDIT_PREFIX),
                None,
                MAX_LIST_ERASURE_AUDITS,
            )
            .context(StateSnafu)?;
        let mut erased = std::collections::HashSet::new();
        for entity in &entities {
            let key_str = String::from_utf8_lossy(&entity.key);
            if let Some(user_id) = SystemKeys::parse_erasure_audit_key(&key_str) {
                erased.insert(user_id);
            }
        }
        Ok(erased)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use chrono::Utc;
    use inferadb_ledger_types::{
        CredentialData, CredentialType, Operation, OrganizationId, PasskeyCredential,
        PendingTotpChallenge, PrimaryAuthMethod, Region, TokenVersion, UserEmailId, UserId,
        UserRole, UserSlug, UserStatus, encode,
    };
    use zeroize::Zeroizing;

    use crate::system::{
        keys::SystemKeys,
        service::{SYSTEM_VAULT_ID, SystemOrganizationService},
        types::{
            EmailHashEntry, ProvisioningReservation, User, UserDirectoryEntry, UserDirectoryStatus,
            UserEmail,
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

    fn write_flat_user(
        svc: &SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>,
        user: &User,
    ) {
        let key = SystemKeys::user_key(user.id);
        let value = encode(user).unwrap();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        svc.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).unwrap();
    }

    fn write_flat_user_email(
        svc: &SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>,
        email: &UserEmail,
    ) {
        let key = SystemKeys::user_email_key(email.id);
        let value = encode(email).unwrap();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        svc.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).unwrap();
    }

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

    fn make_passkey_data(credential_id: &[u8]) -> CredentialData {
        CredentialData::Passkey(PasskeyCredential {
            credential_id: credential_id.to_vec(),
            public_key: vec![0xA0; 32],
            sign_count: 0,
            transports: vec!["internal".to_string()],
            backup_eligible: true,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        })
    }

    fn make_totp_data() -> CredentialData {
        CredentialData::Totp(inferadb_ledger_types::TotpCredential {
            secret: Zeroizing::new(vec![0x42; 20]),
            algorithm: inferadb_ledger_types::TotpAlgorithm::default(),
            digits: 6,
            period: 30,
        })
    }

    fn make_challenge(
        user_id: UserId,
        user_slug: UserSlug,
        nonce: [u8; 32],
    ) -> PendingTotpChallenge {
        PendingTotpChallenge {
            nonce,
            user: user_id,
            user_slug,
            expires_at: Utc::now() + chrono::Duration::minutes(5),
            attempts: 0,
            primary_method: PrimaryAuthMethod::EmailCode,
        }
    }

    #[test]
    fn test_store_and_get_user_shred_key() {
        let svc = create_test_service();
        svc.store_user_shred_key(UserId::new(42), &[0xABu8; 32]).unwrap();
        let retrieved = svc.get_user_shred_key(UserId::new(42)).unwrap().unwrap();
        assert_eq!(retrieved.user_id, UserId::new(42));
        assert_eq!(retrieved.key, [0xABu8; 32]);
    }

    #[test]
    fn test_user_shred_key_not_found_after_erasure() {
        let svc = create_test_service();
        let user_id = UserId::new(42);
        svc.store_user_shred_key(user_id, &[0xCDu8; 32]).unwrap();
        svc.register_user_directory(&make_directory_entry(42, 10042, Region::US_EAST_VA)).unwrap();
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
        assert!(svc.get_user_shred_key(user_id).unwrap().is_none());
    }

    #[test]
    fn test_erase_user_tombstone_minimization() {
        let svc = create_test_service();
        let user_id = UserId::new(55);
        svc.register_user_directory(&make_directory_entry(55, 10055, Region::US_EAST_VA)).unwrap();
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
        let tombstone = svc.get_user_directory(user_id).unwrap().unwrap();
        assert_eq!(tombstone.status, UserDirectoryStatus::Deleted);
        assert!(tombstone.slug.is_none());
        assert!(tombstone.region.is_none());
        assert!(tombstone.updated_at.is_none());
        assert!(svc.get_user_id_by_slug(UserSlug::new(10055)).unwrap().is_none());
    }

    #[test]
    fn test_erase_user_removes_email_hash_entries() {
        let svc = create_test_service();
        let user_id = UserId::new(77);
        svc.register_user_directory(&make_directory_entry(77, 10077, Region::IE_EAST_DUBLIN))
            .unwrap();
        svc.register_email_hash("hash_alpha", user_id).unwrap();
        svc.register_email_hash("hash_beta", user_id).unwrap();
        svc.erase_user(user_id, Region::IE_EAST_DUBLIN, Utc::now()).unwrap();
        assert!(svc.get_email_hash("hash_alpha").unwrap().is_none());
        assert!(svc.get_email_hash("hash_beta").unwrap().is_none());
    }

    #[test]
    fn test_erase_user_creates_audit_record() {
        let svc = create_test_service();
        let user_id = UserId::new(88);
        svc.register_user_directory(&make_directory_entry(88, 10088, Region::US_EAST_VA)).unwrap();
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
        let audit = svc.get_erasure_audit(user_id).unwrap().unwrap();
        assert_eq!(audit.user_id, user_id);
        assert_eq!(audit.region, Region::US_EAST_VA);
    }

    #[test]
    fn test_erase_user_idempotent() {
        let svc = create_test_service();
        let user_id = UserId::new(99);
        svc.register_user_directory(&make_directory_entry(99, 10099, Region::US_EAST_VA)).unwrap();
        svc.store_user_shred_key(user_id, &[0x11u8; 32]).unwrap();
        svc.register_email_hash("idempotent_hash", user_id).unwrap();
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
        assert!(svc.get_user_shred_key(user_id).unwrap().is_none());
        assert!(svc.get_email_hash("idempotent_hash").unwrap().is_none());
    }

    #[test]
    fn test_erase_user_without_directory_entry() {
        let svc = create_test_service();
        svc.erase_user(UserId::new(111), Region::US_EAST_VA, Utc::now()).unwrap();
        assert!(svc.get_erasure_audit(UserId::new(111)).unwrap().is_some());
    }

    #[test]
    fn test_erase_user_other_users_unaffected() {
        let svc = create_test_service();
        svc.register_user_directory(&make_directory_entry(1, 10001, Region::US_EAST_VA)).unwrap();
        svc.register_user_directory(&make_directory_entry(2, 10002, Region::US_EAST_VA)).unwrap();
        svc.store_user_shred_key(UserId::new(1), &[0xAAu8; 32]).unwrap();
        svc.store_user_shred_key(UserId::new(2), &[0xBBu8; 32]).unwrap();
        svc.register_email_hash("user1_hash", UserId::new(1)).unwrap();
        svc.register_email_hash("user2_hash", UserId::new(2)).unwrap();
        svc.erase_user(UserId::new(1), Region::US_EAST_VA, Utc::now()).unwrap();
        let user2 = svc.get_user_directory(UserId::new(2)).unwrap().unwrap();
        assert_eq!(user2.status, UserDirectoryStatus::Active);
        assert!(svc.get_user_shred_key(UserId::new(2)).unwrap().is_some());
        assert!(svc.get_email_hash("user2_hash").unwrap().is_some());
    }

    #[test]
    fn test_list_erased_user_ids() {
        let svc = create_test_service();
        assert!(svc.list_erased_user_ids().unwrap().is_empty());
        svc.register_user_directory(&make_directory_entry(10, 10010, Region::US_EAST_VA)).unwrap();
        svc.register_user_directory(&make_directory_entry(20, 10020, Region::IE_EAST_DUBLIN))
            .unwrap();
        svc.erase_user(UserId::new(10), Region::US_EAST_VA, Utc::now()).unwrap();
        svc.erase_user(UserId::new(20), Region::IE_EAST_DUBLIN, Utc::now()).unwrap();
        let erased = svc.list_erased_user_ids().unwrap();
        assert_eq!(erased.len(), 2);
        assert!(erased.contains(&UserId::new(10)));
        assert!(erased.contains(&UserId::new(20)));
    }

    #[test]
    fn test_erase_user_end_to_end() {
        let svc = create_test_service();
        let user_id = UserId::new(42);
        let now = Utc::now();
        svc.register_user_directory(&make_directory_entry(42, 10042, Region::US_EAST_VA)).unwrap();
        svc.store_user_shred_key(user_id, &[0xFFu8; 32]).unwrap();
        svc.register_email_hash("user42_email_hash", user_id).unwrap();
        write_flat_user(&svc, &make_test_user(42, 10042, Region::US_EAST_VA));
        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"e2e_pk"),
                "E2E Passkey",
                now,
            )
            .unwrap();
        svc.create_totp_challenge(&make_challenge(user_id, UserSlug::new(10042), [0xE2; 32]))
            .unwrap();
        assert!(svc.get_user(user_id).unwrap().is_some());
        svc.erase_user(user_id, Region::US_EAST_VA, now).unwrap();
        assert!(svc.get_user(user_id).unwrap().is_none());
        let tombstone = svc.get_user_directory(user_id).unwrap().unwrap();
        assert_eq!(tombstone.status, UserDirectoryStatus::Deleted);
        assert!(svc.get_user_shred_key(user_id).unwrap().is_none());
        assert!(svc.get_email_hash("user42_email_hash").unwrap().is_none());
        assert!(svc.get_user_credential(user_id, cred.id).unwrap().is_none());
        assert!(svc.get_totp_challenge(user_id, &[0xE2; 32]).unwrap().is_none());
        assert!(svc.get_erasure_audit(user_id).unwrap().is_some());
        assert!(svc.list_erased_user_ids().unwrap().contains(&user_id));
    }

    #[test]
    fn test_erase_user_deletes_user_record() {
        let svc = create_test_service();
        let user_id = UserId::new(80);
        svc.register_user_directory(&make_directory_entry(80, 10080, Region::US_EAST_VA)).unwrap();
        write_flat_user(&svc, &make_test_user(80, 10080, Region::US_EAST_VA));
        assert!(svc.get_user(user_id).unwrap().is_some());
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
        assert!(svc.get_user(user_id).unwrap().is_none());
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
    }

    #[test]
    fn test_erase_user_deletes_email_records() {
        let svc = create_test_service();
        let user_id = UserId::new(70);
        svc.register_user_directory(&make_directory_entry(70, 10070, Region::US_EAST_VA)).unwrap();
        write_flat_user(&svc, &make_test_user(70, 10070, Region::US_EAST_VA));
        write_flat_user_email(&svc, &make_test_user_email(700, 70, "primary@example.com"));
        write_flat_user_email(&svc, &make_test_user_email(701, 70, "secondary@example.com"));
        write_user_emails_index(&svc, user_id, &[UserEmailId::new(700), UserEmailId::new(701)]);
        write_email_index(&svc, "primary@example.com", UserEmailId::new(700));
        write_email_index(&svc, "secondary@example.com", UserEmailId::new(701));
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
        assert!(svc.get_user_email(UserEmailId::new(700)).unwrap().is_none());
        assert!(svc.get_user_email(UserEmailId::new(701)).unwrap().is_none());
        assert!(svc.search_users_by_email("primary@example.com").unwrap().is_none());
    }

    #[test]
    fn test_erase_user_deletes_email_indices() {
        let svc = create_test_service();
        let user_id = UserId::new(90);
        svc.register_user_directory(&make_directory_entry(90, 10090, Region::US_EAST_VA)).unwrap();
        write_flat_user(&svc, &make_test_user(90, 10090, Region::US_EAST_VA));
        write_flat_user_email(&svc, &make_test_user_email(900, 90, "primary@example.com"));
        write_flat_user_email(&svc, &make_test_user_email(901, 90, "secondary@example.com"));
        write_user_emails_index(&svc, user_id, &[UserEmailId::new(900), UserEmailId::new(901)]);
        write_email_index(&svc, "primary@example.com", UserEmailId::new(900));
        write_email_index(&svc, "secondary@example.com", UserEmailId::new(901));
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
        assert!(svc.search_users_by_email("primary@example.com").unwrap().is_none());
        assert!(svc.search_users_by_email("secondary@example.com").unwrap().is_none());
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
    }

    #[test]
    fn test_erase_user_deletes_credentials() {
        let svc = create_test_service();
        let user_id = UserId::new(91);
        let now = Utc::now();
        svc.register_user_directory(&make_directory_entry(91, 10091, Region::US_EAST_VA)).unwrap();
        let pk = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"erase_pk1"),
                "Test Passkey",
                now,
            )
            .unwrap();
        let totp = svc
            .create_user_credential(
                user_id,
                CredentialType::Totp,
                make_totp_data(),
                "Authenticator",
                now,
            )
            .unwrap();
        svc.erase_user(user_id, Region::US_EAST_VA, now).unwrap();
        assert!(svc.get_user_credential(user_id, pk.id).unwrap().is_none());
        assert!(svc.get_user_credential(user_id, totp.id).unwrap().is_none());
        let type_idx_prefix = SystemKeys::user_credential_type_index_prefix(user_id);
        let idx_entries =
            svc.state.list_entities(SYSTEM_VAULT_ID, Some(&type_idx_prefix), None, 100).unwrap();
        assert!(idx_entries.is_empty());
        svc.erase_user(user_id, Region::US_EAST_VA, now).unwrap();
    }

    #[test]
    fn test_erase_user_deletes_totp_challenges() {
        let svc = create_test_service();
        let user_id = UserId::new(92);
        svc.register_user_directory(&make_directory_entry(92, 10092, Region::US_EAST_VA)).unwrap();
        svc.create_totp_challenge(&make_challenge(user_id, UserSlug::new(10092), [0x01; 32]))
            .unwrap();
        svc.create_totp_challenge(&make_challenge(user_id, UserSlug::new(10092), [0x02; 32]))
            .unwrap();
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
        assert!(svc.get_totp_challenge(user_id, &[0x01; 32]).unwrap().is_none());
        assert!(svc.get_totp_challenge(user_id, &[0x02; 32]).unwrap().is_none());
    }

    #[test]
    fn test_erase_user_credentials_other_users_unaffected() {
        let svc = create_test_service();
        let user_id = UserId::new(93);
        let other_user = UserId::new(94);
        let now = Utc::now();
        svc.register_user_directory(&make_directory_entry(93, 10093, Region::US_EAST_VA)).unwrap();
        svc.register_user_directory(&make_directory_entry(94, 10094, Region::US_EAST_VA)).unwrap();
        svc.create_user_credential(
            user_id,
            CredentialType::Passkey,
            make_passkey_data(b"erase_iso1"),
            "User 93 key",
            now,
        )
        .unwrap();
        let other_cred = svc
            .create_user_credential(
                other_user,
                CredentialType::Passkey,
                make_passkey_data(b"erase_iso2"),
                "User 94 key",
                now,
            )
            .unwrap();
        svc.create_totp_challenge(&make_challenge(user_id, UserSlug::new(10093), [0xA0; 32]))
            .unwrap();
        svc.create_totp_challenge(&make_challenge(other_user, UserSlug::new(10094), [0xB0; 32]))
            .unwrap();
        svc.erase_user(user_id, Region::US_EAST_VA, now).unwrap();
        assert!(svc.list_user_credentials(user_id, None).unwrap().is_empty());
        assert!(svc.get_user_credential(other_user, other_cred.id).unwrap().is_some());
        assert!(svc.get_totp_challenge(other_user, &[0xB0; 32]).unwrap().is_some());
    }

    /// Documents and enforces the scope boundary: user erasure must NOT
    /// cascade to organization-scoped PII (app profiles, org profiles).
    /// Those records belong to the organization, not the user, and are
    /// cleaned up via `PurgeOrganizationRegional` when the org itself is
    /// deleted. See the `erase_user` doc comment for rationale.
    #[test]
    fn test_erase_user_does_not_cascade_to_org_scoped_pii() {
        use inferadb_ledger_types::AppId;

        let svc = create_test_service();
        let user_id = UserId::new(300);
        let org_id = OrganizationId::new(301);
        let app_id = AppId::new(302);

        svc.register_user_directory(&make_directory_entry(300, 10300, Region::US_EAST_VA)).unwrap();
        write_flat_user(&svc, &make_test_user(300, 10300, Region::US_EAST_VA));

        // Write an app_profile entry for an org the user could be a member of.
        let app_profile_key = SystemKeys::app_profile_key(org_id, app_id);
        svc.state
            .apply_operations(
                SYSTEM_VAULT_ID,
                &[Operation::SetEntity {
                    key: app_profile_key.clone(),
                    value: b"organizational-pii-payload".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                0,
            )
            .unwrap();

        // Write an org_profile entry.
        let org_profile_key = SystemKeys::organization_profile_key(org_id);
        svc.state
            .apply_operations(
                SYSTEM_VAULT_ID,
                &[Operation::SetEntity {
                    key: org_profile_key.clone(),
                    value: b"org-name-pii-payload".to_vec(),
                    condition: None,
                    expires_at: None,
                }],
                0,
            )
            .unwrap();

        // Erase the user.
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();

        // User-level records are gone.
        assert!(svc.get_user(user_id).unwrap().is_none());
        assert!(svc.get_erasure_audit(user_id).unwrap().is_some());

        // Org-scoped PII remains intact — the organization continues to
        // exist and its profile PII must not be collateral damage of a
        // single member's erasure.
        let app_profile =
            svc.state.get_entity(SYSTEM_VAULT_ID, app_profile_key.as_bytes()).unwrap();
        assert!(
            app_profile.is_some(),
            "user erasure must not cascade to app_profile (org-scoped PII)"
        );

        let org_profile =
            svc.state.get_entity(SYSTEM_VAULT_ID, org_profile_key.as_bytes()).unwrap();
        assert!(
            org_profile.is_some(),
            "user erasure must not cascade to org_profile (org-scoped PII)"
        );
    }

    #[test]
    fn test_erase_user_removes_provisioning_email_hash() {
        let svc = create_test_service();
        let user_id = UserId::new(200);
        let other_user_id = UserId::new(201);
        svc.register_user_directory(&make_directory_entry(200, 10200, Region::US_EAST_VA)).unwrap();
        svc.register_email_hash("hash_active", user_id).unwrap();
        let prov_entry = EmailHashEntry::Provisioning(ProvisioningReservation {
            user_id,
            organization_id: OrganizationId::new(1),
        });
        let prov_key = SystemKeys::email_hash_index_key("hash_provisioning");
        let prov_value = encode(&prov_entry).unwrap();
        svc.state
            .apply_operations(
                SYSTEM_VAULT_ID,
                &[Operation::SetEntity {
                    key: prov_key,
                    value: prov_value,
                    condition: None,
                    expires_at: None,
                }],
                0,
            )
            .unwrap();
        let other_prov = EmailHashEntry::Provisioning(ProvisioningReservation {
            user_id: other_user_id,
            organization_id: OrganizationId::new(2),
        });
        let other_key = SystemKeys::email_hash_index_key("hash_other_prov");
        let other_value = encode(&other_prov).unwrap();
        svc.state
            .apply_operations(
                SYSTEM_VAULT_ID,
                &[Operation::SetEntity {
                    key: other_key,
                    value: other_value,
                    condition: None,
                    expires_at: None,
                }],
                0,
            )
            .unwrap();
        svc.erase_user(user_id, Region::US_EAST_VA, Utc::now()).unwrap();
        assert!(svc.get_email_hash("hash_active").unwrap().is_none());
        assert!(svc.get_email_hash("hash_provisioning").unwrap().is_none());
        let surviving = svc.get_email_hash("hash_other_prov").unwrap().unwrap();
        assert_eq!(
            surviving,
            EmailHashEntry::Provisioning(ProvisioningReservation {
                user_id: other_user_id,
                organization_id: OrganizationId::new(2)
            })
        );
    }
}
