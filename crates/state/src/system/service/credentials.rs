//! Credential CRUD, TOTP challenge management, and recovery code consumption.

use chrono::{DateTime, Utc};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    CredentialData, CredentialType, Operation, PasskeyCredential, PendingTotpChallenge,
    SetCondition, UserCredential, UserCredentialId, UserId, decode, encode, hash_eq,
};
use snafu::ResultExt;
use tracing::warn;

use super::{
    CodecSnafu, KeyTier, Result, SYSTEM_VAULT_ID, StateSnafu, SystemError, SystemKeys,
    SystemOrganizationService, require_tier,
};

impl<B: StorageBackend> SystemOrganizationService<B> {
    // ========================================================================
    // Credential CRUD
    // ========================================================================

    /// Maximum number of credentials returned by `list_user_credentials`.
    const MAX_LIST_CREDENTIALS: usize = 100;

    /// Maximum number of TOTP challenges scanned per user.
    const MAX_TOTP_CHALLENGES: usize = 10;

    /// Maximum active TOTP challenges per user before rate-limiting kicks in.
    const MAX_ACTIVE_TOTP_CHALLENGES: u8 = 3;

    /// Maximum failed TOTP attempts per challenge before lockout.
    const MAX_TOTP_ATTEMPTS: u8 = 3;

    /// Creates a new user credential.
    ///
    /// Allocates a new `UserCredentialId` from the REGIONAL sequence counter,
    /// stores the credential, and writes the type index entry.
    ///
    /// # Safety invariants enforced
    ///
    /// - At most one TOTP credential per user.
    /// - At most one recovery code set per user.
    /// - Passkey `credential_id` uniqueness per user.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::AlreadyExists`] if a uniqueness invariant is
    /// violated, [`super::SystemError::Codec`] for serialization failures, or
    /// [`super::SystemError::State`] for storage failures.
    pub fn create_user_credential(
        &self,
        user_id: UserId,
        credential_type: CredentialType,
        credential_data: CredentialData,
        name: &str,
        now: DateTime<Utc>,
    ) -> Result<UserCredential> {
        // Enforce one-TOTP and one-recovery-code invariants.
        match credential_type {
            CredentialType::Totp | CredentialType::RecoveryCode => {
                let existing = self.list_user_credentials(user_id, Some(credential_type))?;
                if !existing.is_empty() {
                    return Err(SystemError::AlreadyExists {
                        entity: format!(
                            "credential:{credential_type} for user:{}",
                            user_id.value()
                        ),
                    });
                }
            },
            CredentialType::Passkey => {
                // Enforce passkey credential_id uniqueness per user.
                // credential_type is Passkey, so credential_data must be Passkey.
                let CredentialData::Passkey(ref pk) = credential_data else {
                    return Err(SystemError::FailedPrecondition {
                        message: "credential_type/credential_data mismatch".to_string(),
                    });
                };
                let existing_passkeys =
                    self.list_user_credentials(user_id, Some(CredentialType::Passkey))?;
                for existing in &existing_passkeys {
                    let CredentialData::Passkey(ref epk) = existing.credential_data else {
                        continue;
                    };
                    if epk.credential_id == pk.credential_id {
                        return Err(SystemError::AlreadyExists {
                            entity: format!("passkey credential_id for user:{}", user_id.value()),
                        });
                    }
                }
            },
        }

        let credential_id = self
            .next_sequence(SystemKeys::USER_CREDENTIAL_SEQ_KEY, 1)
            .map(UserCredentialId::new)?;

        let credential = UserCredential {
            id: credential_id,
            user: user_id,
            credential_type,
            credential_data,
            name: name.to_string(),
            enabled: true,
            created_at: now,
            last_used_at: None,
        };

        let primary_key = SystemKeys::user_credential_key(user_id, credential_id);
        require_tier(&primary_key, KeyTier::Regional)?;
        let primary_value = encode(&credential).context(CodecSnafu)?;

        // Type index: key includes credential_id for multi-passkey support.
        // Pattern: `_idx:user_credential:type:{user_id}:{type}:{credential_id}`
        let type_idx_key = format!(
            "{}{}",
            SystemKeys::user_credential_type_index_key(user_id, &credential_type),
            credential_id.value()
        );
        require_tier(&type_idx_key, KeyTier::Regional)?;

        let ops = vec![
            Operation::SetEntity {
                key: primary_key,
                value: primary_value,
                condition: Some(SetCondition::MustNotExist),
                expires_at: None,
            },
            Operation::SetEntity {
                key: type_idx_key,
                value: Vec::new(),
                condition: None,
                expires_at: None,
            },
        ];

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(credential)
    }

    /// Lists all credentials for a user, optionally filtered by type.
    ///
    /// Performs a prefix scan on `user_credential:{user_id}:` and
    /// deserializes each entry.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if the scan fails, or
    /// [`super::SystemError::Codec`] if deserialization fails.
    pub fn list_user_credentials(
        &self,
        user_id: UserId,
        credential_type: Option<CredentialType>,
    ) -> Result<Vec<UserCredential>> {
        let prefix = SystemKeys::user_credential_prefix(user_id);
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, Self::MAX_LIST_CREDENTIALS)
            .context(StateSnafu)?;

        let mut credentials = Vec::new();
        for entity in &entities {
            let key_str = String::from_utf8_lossy(&entity.key);
            if SystemKeys::parse_user_credential_key(&key_str).is_none() {
                continue;
            }
            match decode::<UserCredential>(&entity.value) {
                Ok(cred) => {
                    if credential_type.is_none_or(|ct| ct == cred.credential_type) {
                        credentials.push(cred);
                    }
                },
                Err(e) => {
                    warn!(error = %e, key = %key_str, "Skipping corrupt credential entry");
                },
            }
        }
        Ok(credentials)
    }

    /// Returns a single credential by user and credential ID.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if the read fails, or
    /// [`super::SystemError::Codec`] if deserialization fails.
    pub fn get_user_credential(
        &self,
        user_id: UserId,
        credential_id: UserCredentialId,
    ) -> Result<Option<UserCredential>> {
        let key = SystemKeys::user_credential_key(user_id, credential_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let cred: UserCredential = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(cred))
            },
            None => Ok(None),
        }
    }

    /// Updates a user credential.
    ///
    /// Only passkey-specific fields (`sign_count`, `backup_state`) and
    /// common fields (`name`, `enabled`, `last_used_at`) can be updated.
    /// TOTP credentials are immutable after creation.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::NotFound`] if the credential doesn't exist,
    /// [`super::SystemError::Codec`] for serialization failures, or
    /// [`super::SystemError::State`] for storage failures.
    pub fn update_user_credential(
        &self,
        user_id: UserId,
        credential_id: UserCredentialId,
        name: Option<&str>,
        enabled: Option<bool>,
        last_used_at: Option<DateTime<Utc>>,
        passkey_update: Option<&PasskeyCredential>,
    ) -> Result<UserCredential> {
        let mut cred = self.get_user_credential(user_id, credential_id)?.ok_or_else(|| {
            SystemError::NotFound { entity: format!("credential:{}", credential_id.value()) }
        })?;

        if let Some(name) = name {
            cred.name = name.to_string();
        }
        if let Some(enabled) = enabled {
            cred.enabled = enabled;
        }
        if let Some(ts) = last_used_at {
            cred.last_used_at = Some(ts);
        }

        // Apply passkey-specific updates (sign_count, backup_state).
        // Per WebAuthn spec (section 7.2 step 21), sign_count must be strictly
        // increasing to detect cloned authenticators. A value of 0 for both old
        // and new indicates the authenticator doesn't support counters.
        if let Some(pk_update) = passkey_update
            && let CredentialData::Passkey(ref mut pk) = cred.credential_data
        {
            if pk.sign_count > 0 && pk_update.sign_count <= pk.sign_count {
                warn!(
                    credential_id = cred.id.value(),
                    old_count = pk.sign_count,
                    new_count = pk_update.sign_count,
                    "Passkey sign_count not monotonically increasing — possible cloned authenticator"
                );
                return Err(SystemError::FailedPrecondition {
                    message: format!(
                        "Passkey sign_count regression: new {} <= current {} (possible cloned authenticator)",
                        pk_update.sign_count, pk.sign_count
                    ),
                });
            }
            pk.sign_count = pk_update.sign_count;
            pk.backup_state = pk_update.backup_state;
        }

        let key = SystemKeys::user_credential_key(user_id, credential_id);
        require_tier(&key, KeyTier::Regional)?;
        let value = encode(&cred).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(cred)
    }

    /// Deletes a user credential.
    ///
    /// Removes the primary record and its type index entry. The
    /// last-credential guard prevents deleting the only remaining
    /// credential for a user.
    ///
    /// # Safety invariants enforced
    ///
    /// - Cannot delete the last credential (prevents user lockout).
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::NotFound`] if the credential doesn't exist,
    /// [`super::SystemError::FailedPrecondition`] if this is the last credential,
    /// [`super::SystemError::State`] for storage failures.
    pub fn delete_user_credential(
        &self,
        user_id: UserId,
        credential_id: UserCredentialId,
    ) -> Result<()> {
        let cred = self.get_user_credential(user_id, credential_id)?.ok_or_else(|| {
            SystemError::NotFound { entity: format!("credential:{}", credential_id.value()) }
        })?;

        // Last-credential guard: scan all credentials for this user.
        let all_credentials = self.list_user_credentials(user_id, None)?;
        if all_credentials.len() <= 1 {
            return Err(SystemError::FailedPrecondition {
                message: format!("Cannot delete last credential for user:{}", user_id.value()),
            });
        }

        let primary_key = SystemKeys::user_credential_key(user_id, credential_id);
        require_tier(&primary_key, KeyTier::Regional)?;

        let type_idx_key = format!(
            "{}{}",
            SystemKeys::user_credential_type_index_key(user_id, &cred.credential_type),
            credential_id.value()
        );
        require_tier(&type_idx_key, KeyTier::Regional)?;

        let ops = vec![
            Operation::DeleteEntity { key: primary_key },
            Operation::DeleteEntity { key: type_idx_key },
        ];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Deletes all credentials for a user (used during user erasure).
    ///
    /// Scans all credential records and type index entries for the user
    /// and deletes them in a single batch. No last-credential guard —
    /// this is called during full erasure.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if the scan or delete fails.
    pub fn delete_all_user_credentials(&self, user_id: UserId) -> Result<()> {
        let prefix = SystemKeys::user_credential_prefix(user_id);
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, Self::MAX_LIST_CREDENTIALS)
            .context(StateSnafu)?;

        let mut ops: Vec<Operation> = Vec::new();
        for entity in &entities {
            let key = String::from_utf8_lossy(&entity.key).to_string();
            ops.push(Operation::DeleteEntity { key });
        }

        // Also clean up type index entries.
        let type_idx_prefix = SystemKeys::user_credential_type_index_prefix(user_id);
        let idx_entities = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(&type_idx_prefix),
                None,
                Self::MAX_LIST_CREDENTIALS,
            )
            .context(StateSnafu)?;
        for entity in &idx_entities {
            let key = String::from_utf8_lossy(&entity.key).to_string();
            ops.push(Operation::DeleteEntity { key });
        }

        if !ops.is_empty() {
            self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        }
        Ok(())
    }

    // ========================================================================
    // TOTP Challenge Management
    // ========================================================================

    /// Creates a pending TOTP challenge after primary authentication.
    ///
    /// The challenge is stored as a `_tmp:` record with the given expiry.
    /// Rate-limited to `MAX_ACTIVE_TOTP_CHALLENGES` per user.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::ResourceExhausted`] if the user already has
    /// 3 active challenges, [`super::SystemError::Codec`] for serialization
    /// failures, or [`super::SystemError::State`] for storage failures.
    pub fn create_totp_challenge(&self, challenge: &PendingTotpChallenge) -> Result<()> {
        // Rate limit: reject if user already has MAX_ACTIVE_TOTP_CHALLENGES
        // unexpired challenges.
        let active_count = self.count_active_totp_challenges(challenge.user)?;
        if active_count >= Self::MAX_ACTIVE_TOTP_CHALLENGES {
            return Err(SystemError::ResourceExhausted {
                message: format!(
                    "Too many active TOTP challenges for user:{}",
                    challenge.user.value()
                ),
            });
        }

        let key = SystemKeys::totp_challenge_key(challenge.user, &challenge.nonce);
        require_tier(&key, KeyTier::Regional)?;
        let value = encode(challenge).context(CodecSnafu)?;

        let ops = vec![Operation::SetEntity {
            key,
            value,
            condition: Some(SetCondition::MustNotExist),
            expires_at: Some(challenge.expires_at.timestamp() as u64),
        }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(())
    }

    /// Reads a pending TOTP challenge by user ID and nonce.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if the read fails, or
    /// [`super::SystemError::Codec`] if deserialization fails.
    pub fn get_totp_challenge(
        &self,
        user_id: UserId,
        nonce: &[u8; 32],
    ) -> Result<Option<PendingTotpChallenge>> {
        let key = SystemKeys::totp_challenge_key(user_id, nonce);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let challenge: PendingTotpChallenge = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(challenge))
            },
            None => Ok(None),
        }
    }

    /// Deletes a pending TOTP challenge (consumed after verification).
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if the delete fails.
    pub fn delete_totp_challenge(&self, user_id: UserId, nonce: &[u8; 32]) -> Result<()> {
        let key = SystemKeys::totp_challenge_key(user_id, nonce);
        require_tier(&key, KeyTier::Regional)?;
        let ops = vec![Operation::DeleteEntity { key }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Increments the attempt counter on a TOTP challenge.
    ///
    /// Returns the updated challenge. Rejects if attempts >= 3 at
    /// apply time (Raft-persisted counter survives leader failover).
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::NotFound`] if the challenge doesn't exist,
    /// [`super::SystemError::FailedPrecondition`] if max attempts exceeded,
    /// [`super::SystemError::Codec`] for codec failures, or
    /// [`super::SystemError::State`] for storage failures.
    pub fn increment_totp_attempts(
        &self,
        user_id: UserId,
        nonce: &[u8; 32],
    ) -> Result<PendingTotpChallenge> {
        let mut challenge = self.get_totp_challenge(user_id, nonce)?.ok_or_else(|| {
            SystemError::NotFound { entity: format!("totp_challenge for user:{}", user_id.value()) }
        })?;

        if challenge.attempts >= Self::MAX_TOTP_ATTEMPTS {
            return Err(SystemError::FailedPrecondition {
                message: "Too many TOTP attempts".to_string(),
            });
        }

        challenge.attempts += 1;

        let key = SystemKeys::totp_challenge_key(user_id, nonce);
        let value = encode(&challenge).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity {
            key,
            value,
            condition: None,
            expires_at: Some(challenge.expires_at.timestamp() as u64),
        }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(challenge)
    }

    /// Deletes all TOTP challenges for a user (used during erasure).
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::State`] if the scan or delete fails.
    pub fn delete_all_totp_challenges(&self, user_id: UserId) -> Result<()> {
        let prefix = SystemKeys::totp_challenge_prefix(user_id);
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, Self::MAX_TOTP_CHALLENGES)
            .context(StateSnafu)?;

        let mut ops: Vec<Operation> = Vec::new();
        for entity in &entities {
            let key = String::from_utf8_lossy(&entity.key).to_string();
            ops.push(Operation::DeleteEntity { key });
        }

        if !ops.is_empty() {
            self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        }
        Ok(())
    }

    /// Counts unexpired TOTP challenges for a user.
    fn count_active_totp_challenges(&self, user_id: UserId) -> Result<u8> {
        let prefix = SystemKeys::totp_challenge_prefix(user_id);
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, Self::MAX_TOTP_CHALLENGES)
            .context(StateSnafu)?;

        // All _tmp: records have expires_at — the storage engine filters
        // expired records during reads, so all returned entities are active.
        Ok(u8::try_from(entities.len()).unwrap_or(u8::MAX))
    }

    // ========================================================================
    // Recovery Code Consumption
    // ========================================================================

    /// Consumes a single recovery code hash from a recovery code credential.
    ///
    /// Finds the matching hash, removes it, and writes the updated
    /// credential back. Returns the number of remaining codes.
    ///
    /// # Errors
    ///
    /// Returns [`super::SystemError::NotFound`] if no matching hash is found or
    /// the credential doesn't exist.
    pub fn consume_recovery_code(
        &self,
        user_id: UserId,
        credential_id: UserCredentialId,
        code_hash: &[u8; 32],
        now: DateTime<Utc>,
    ) -> Result<u32> {
        let mut cred = self.get_user_credential(user_id, credential_id)?.ok_or_else(|| {
            SystemError::NotFound { entity: format!("credential:{}", credential_id.value()) }
        })?;

        let remaining = match cred.credential_data {
            CredentialData::RecoveryCode(ref mut rc) => {
                let initial_len = rc.code_hashes.len();
                // Constant-time comparison prevents timing side-channel attacks
                // that could reveal prefix-length information about stored hashes.
                rc.code_hashes.retain(|h| !hash_eq(h, code_hash));
                if rc.code_hashes.len() == initial_len {
                    return Err(SystemError::NotFound {
                        entity: "matching recovery code hash".to_string(),
                    });
                }
                rc.code_hashes.len()
            },
            _ => {
                return Err(SystemError::FailedPrecondition {
                    message: format!(
                        "credential:{} is not a recovery code credential",
                        credential_id.value()
                    ),
                });
            },
        };

        cred.last_used_at = Some(now);

        let key = SystemKeys::user_credential_key(user_id, credential_id);
        require_tier(&key, KeyTier::Regional)?;
        let value = encode(&cred).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(u32::try_from(remaining).unwrap_or(u32::MAX))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use chrono::Utc;
    use inferadb_ledger_types::{
        CredentialData, CredentialType, PasskeyCredential, PendingTotpChallenge, PrimaryAuthMethod,
        RecoveryCodeCredential, TotpAlgorithm, TotpCredential, UserCredentialId, UserId, UserSlug,
    };
    use zeroize::Zeroizing;

    use crate::system::service::{SystemError, SystemOrganizationService};

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        crate::system::create_test_service()
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
        CredentialData::Totp(TotpCredential {
            secret: Zeroizing::new(vec![0x42; 20]),
            algorithm: TotpAlgorithm::default(),
            digits: 6,
            period: 30,
        })
    }

    fn make_recovery_code_data() -> CredentialData {
        CredentialData::RecoveryCode(RecoveryCodeCredential {
            code_hashes: vec![[0xAA; 32], [0xBB; 32], [0xCC; 32]],
            total_generated: 3,
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

    // ========================================================================
    // Credential CRUD tests
    // ========================================================================

    #[test]
    fn test_create_and_get_credential() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk1"),
                "MacBook Touch ID",
                now,
            )
            .unwrap();

        assert_eq!(cred.user, user_id);
        assert_eq!(cred.credential_type, CredentialType::Passkey);
        assert_eq!(cred.name, "MacBook Touch ID");
        assert!(cred.enabled);
        assert!(cred.last_used_at.is_none());

        // Get by ID
        let fetched = svc.get_user_credential(user_id, cred.id).unwrap().unwrap();
        assert_eq!(fetched.id, cred.id);
        assert_eq!(fetched.name, "MacBook Touch ID");
    }

    #[test]
    fn test_create_credential_assigns_sequential_ids() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let c1 = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk1"),
                "Key 1",
                now,
            )
            .unwrap();
        let c2 = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk2"),
                "Key 2",
                now,
            )
            .unwrap();

        assert_eq!(c1.id, UserCredentialId::new(1));
        assert_eq!(c2.id, UserCredentialId::new(2));
    }

    #[test]
    fn test_list_user_credentials() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        svc.create_user_credential(
            user_id,
            CredentialType::Passkey,
            make_passkey_data(b"pk1"),
            "Passkey 1",
            now,
        )
        .unwrap();
        svc.create_user_credential(
            user_id,
            CredentialType::Totp,
            make_totp_data(),
            "Authenticator",
            now,
        )
        .unwrap();

        // List all
        let all = svc.list_user_credentials(user_id, None).unwrap();
        assert_eq!(all.len(), 2);

        // Filter by type
        let passkeys = svc.list_user_credentials(user_id, Some(CredentialType::Passkey)).unwrap();
        assert_eq!(passkeys.len(), 1);
        assert_eq!(passkeys[0].credential_type, CredentialType::Passkey);

        let totps = svc.list_user_credentials(user_id, Some(CredentialType::Totp)).unwrap();
        assert_eq!(totps.len(), 1);
    }

    #[test]
    fn test_list_credentials_empty() {
        let svc = create_test_service();
        let creds = svc.list_user_credentials(UserId::new(999), None).unwrap();
        assert!(creds.is_empty());
    }

    #[test]
    fn test_one_totp_per_user() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        svc.create_user_credential(user_id, CredentialType::Totp, make_totp_data(), "TOTP 1", now)
            .unwrap();

        let err = svc
            .create_user_credential(user_id, CredentialType::Totp, make_totp_data(), "TOTP 2", now)
            .unwrap_err();

        assert!(matches!(err, SystemError::AlreadyExists { .. }));
    }

    #[test]
    fn test_one_recovery_code_set_per_user() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        svc.create_user_credential(
            user_id,
            CredentialType::RecoveryCode,
            make_recovery_code_data(),
            "Recovery Codes",
            now,
        )
        .unwrap();

        let err = svc
            .create_user_credential(
                user_id,
                CredentialType::RecoveryCode,
                make_recovery_code_data(),
                "Recovery Codes 2",
                now,
            )
            .unwrap_err();

        assert!(matches!(err, SystemError::AlreadyExists { .. }));
    }

    #[test]
    fn test_passkey_credential_id_uniqueness() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        svc.create_user_credential(
            user_id,
            CredentialType::Passkey,
            make_passkey_data(b"same_id"),
            "Key 1",
            now,
        )
        .unwrap();

        let err = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"same_id"),
                "Key 2",
                now,
            )
            .unwrap_err();

        assert!(matches!(err, SystemError::AlreadyExists { .. }));
    }

    #[test]
    fn test_passkey_different_credential_ids_allowed() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        svc.create_user_credential(
            user_id,
            CredentialType::Passkey,
            make_passkey_data(b"pk1"),
            "Key 1",
            now,
        )
        .unwrap();

        svc.create_user_credential(
            user_id,
            CredentialType::Passkey,
            make_passkey_data(b"pk2"),
            "Key 2",
            now,
        )
        .unwrap();

        let passkeys = svc.list_user_credentials(user_id, Some(CredentialType::Passkey)).unwrap();
        assert_eq!(passkeys.len(), 2);
    }

    #[test]
    fn test_update_credential_name_and_enabled() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk1"),
                "Old Name",
                now,
            )
            .unwrap();

        let updated = svc
            .update_user_credential(user_id, cred.id, Some("New Name"), Some(false), None, None)
            .unwrap();

        assert_eq!(updated.name, "New Name");
        assert!(!updated.enabled);
    }

    #[test]
    fn test_update_credential_passkey_fields() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk1"),
                "Key",
                now,
            )
            .unwrap();

        let pk_update = PasskeyCredential {
            credential_id: vec![],
            public_key: vec![],
            sign_count: 42,
            transports: vec![],
            backup_eligible: false,
            backup_state: true,
            attestation_format: None,
            aaguid: None,
        };

        let updated = svc
            .update_user_credential(user_id, cred.id, None, None, None, Some(&pk_update))
            .unwrap();

        let CredentialData::Passkey(pk) = &updated.credential_data else {
            unreachable!("Expected passkey data")
        };
        assert_eq!(pk.sign_count, 42);
        assert!(pk.backup_state);
    }

    #[test]
    fn test_passkey_sign_count_reset_to_zero_rejected() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk-reset"),
                "Key",
                now,
            )
            .unwrap();

        // First update: set sign_count to 5 (counter starts)
        let pk_update_5 = PasskeyCredential {
            credential_id: vec![],
            public_key: vec![],
            sign_count: 5,
            transports: vec![],
            backup_eligible: false,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        };
        svc.update_user_credential(user_id, cred.id, None, None, None, Some(&pk_update_5)).unwrap();

        // Attempt to reset to 0 — should be rejected (cloned authenticator)
        let pk_update_0 = PasskeyCredential {
            credential_id: vec![],
            public_key: vec![],
            sign_count: 0,
            transports: vec![],
            backup_eligible: false,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        };
        let err = svc
            .update_user_credential(user_id, cred.id, None, None, None, Some(&pk_update_0))
            .unwrap_err();
        assert!(
            err.to_string().contains("sign_count regression"),
            "Expected sign_count regression error, got: {err}"
        );
    }

    #[test]
    fn test_passkey_sign_count_zero_to_zero_allowed() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        // Create with sign_count=0 (authenticator doesn't support counters)
        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk-nocount"),
                "Key",
                now,
            )
            .unwrap();

        // Update with sign_count=0 — should succeed (both sides indicate no counter)
        let pk_update = PasskeyCredential {
            credential_id: vec![],
            public_key: vec![],
            sign_count: 0,
            transports: vec![],
            backup_eligible: false,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        };
        svc.update_user_credential(user_id, cred.id, None, None, None, Some(&pk_update)).unwrap();
    }

    #[test]
    fn test_passkey_sign_count_same_value_rejected() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk-same"),
                "Key",
                now,
            )
            .unwrap();

        // Set to 10
        let pk_update_10 = PasskeyCredential {
            credential_id: vec![],
            public_key: vec![],
            sign_count: 10,
            transports: vec![],
            backup_eligible: false,
            backup_state: false,
            attestation_format: None,
            aaguid: None,
        };
        svc.update_user_credential(user_id, cred.id, None, None, None, Some(&pk_update_10))
            .unwrap();

        // Same value (10→10) — should be rejected (not strictly increasing)
        let err = svc
            .update_user_credential(user_id, cred.id, None, None, None, Some(&pk_update_10))
            .unwrap_err();
        assert!(err.to_string().contains("sign_count regression"));
    }

    #[test]
    fn test_update_credential_last_used_at() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk1"),
                "Key",
                now,
            )
            .unwrap();
        assert!(cred.last_used_at.is_none());

        let updated =
            svc.update_user_credential(user_id, cred.id, None, None, Some(now), None).unwrap();
        assert_eq!(updated.last_used_at, Some(now));
    }

    #[test]
    fn test_update_credential_not_found() {
        let svc = create_test_service();
        let err = svc
            .update_user_credential(
                UserId::new(1),
                UserCredentialId::new(999),
                Some("Name"),
                None,
                None,
                None,
            )
            .unwrap_err();
        assert!(matches!(err, SystemError::NotFound { .. }));
    }

    #[test]
    fn test_delete_credential() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        // Create two credentials so we can delete one
        let c1 = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk1"),
                "Key 1",
                now,
            )
            .unwrap();
        svc.create_user_credential(
            user_id,
            CredentialType::Passkey,
            make_passkey_data(b"pk2"),
            "Key 2",
            now,
        )
        .unwrap();

        svc.delete_user_credential(user_id, c1.id).unwrap();

        assert!(svc.get_user_credential(user_id, c1.id).unwrap().is_none());
        let remaining = svc.list_user_credentials(user_id, None).unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_delete_last_credential_blocked() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk1"),
                "Only Key",
                now,
            )
            .unwrap();

        let err = svc.delete_user_credential(user_id, cred.id).unwrap_err();
        assert!(matches!(err, SystemError::FailedPrecondition { .. }));

        // Credential still exists
        assert!(svc.get_user_credential(user_id, cred.id).unwrap().is_some());
    }

    #[test]
    fn test_delete_credential_not_found() {
        let svc = create_test_service();
        let err =
            svc.delete_user_credential(UserId::new(1), UserCredentialId::new(999)).unwrap_err();
        assert!(matches!(err, SystemError::NotFound { .. }));
    }

    #[test]
    fn test_delete_all_user_credentials() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        svc.create_user_credential(
            user_id,
            CredentialType::Passkey,
            make_passkey_data(b"pk1"),
            "Key 1",
            now,
        )
        .unwrap();
        svc.create_user_credential(user_id, CredentialType::Totp, make_totp_data(), "TOTP", now)
            .unwrap();

        svc.delete_all_user_credentials(user_id).unwrap();

        let remaining = svc.list_user_credentials(user_id, None).unwrap();
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_credentials_isolated_between_users() {
        let svc = create_test_service();
        let user1 = UserId::new(1);
        let user2 = UserId::new(2);
        let now = Utc::now();

        svc.create_user_credential(
            user1,
            CredentialType::Passkey,
            make_passkey_data(b"pk1"),
            "User 1 Key",
            now,
        )
        .unwrap();
        svc.create_user_credential(
            user2,
            CredentialType::Passkey,
            make_passkey_data(b"pk1"), // Same credential_id, different user — allowed
            "User 2 Key",
            now,
        )
        .unwrap();

        assert_eq!(svc.list_user_credentials(user1, None).unwrap().len(), 1);
        assert_eq!(svc.list_user_credentials(user2, None).unwrap().len(), 1);
    }

    #[test]
    fn test_totp_uniqueness_per_user_not_global() {
        let svc = create_test_service();
        let user1 = UserId::new(1);
        let user2 = UserId::new(2);
        let now = Utc::now();

        svc.create_user_credential(user1, CredentialType::Totp, make_totp_data(), "TOTP", now)
            .unwrap();
        // Different user can also have TOTP
        svc.create_user_credential(user2, CredentialType::Totp, make_totp_data(), "TOTP", now)
            .unwrap();
    }

    // ========================================================================
    // TOTP Challenge tests
    // ========================================================================

    #[test]
    fn test_create_and_get_totp_challenge() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let nonce = [0x01; 32];
        let challenge = make_challenge(user_id, UserSlug::new(100), nonce);

        svc.create_totp_challenge(&challenge).unwrap();

        let fetched = svc.get_totp_challenge(user_id, &nonce).unwrap().unwrap();
        assert_eq!(fetched.nonce, nonce);
        assert_eq!(fetched.user, user_id);
        assert_eq!(fetched.attempts, 0);
    }

    #[test]
    fn test_get_totp_challenge_not_found() {
        let svc = create_test_service();
        let result = svc.get_totp_challenge(UserId::new(1), &[0xFF; 32]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_totp_challenge() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let nonce = [0x01; 32];
        let challenge = make_challenge(user_id, UserSlug::new(100), nonce);

        svc.create_totp_challenge(&challenge).unwrap();
        svc.delete_totp_challenge(user_id, &nonce).unwrap();

        assert!(svc.get_totp_challenge(user_id, &nonce).unwrap().is_none());
    }

    #[test]
    fn test_increment_totp_attempts() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let nonce = [0x01; 32];
        let challenge = make_challenge(user_id, UserSlug::new(100), nonce);

        svc.create_totp_challenge(&challenge).unwrap();

        let c1 = svc.increment_totp_attempts(user_id, &nonce).unwrap();
        assert_eq!(c1.attempts, 1);

        let c2 = svc.increment_totp_attempts(user_id, &nonce).unwrap();
        assert_eq!(c2.attempts, 2);

        let c3 = svc.increment_totp_attempts(user_id, &nonce).unwrap();
        assert_eq!(c3.attempts, 3);

        // Fourth attempt should fail (max is 3)
        let err = svc.increment_totp_attempts(user_id, &nonce).unwrap_err();
        assert!(matches!(err, SystemError::FailedPrecondition { .. }));
    }

    #[test]
    fn test_increment_totp_attempts_not_found() {
        let svc = create_test_service();
        let err = svc.increment_totp_attempts(UserId::new(1), &[0xFF; 32]).unwrap_err();
        assert!(matches!(err, SystemError::NotFound { .. }));
    }

    #[test]
    fn test_totp_scan_cap_covers_rate_limit() {
        // Scan cap must be >= rate limit to avoid under-counting active challenges.
        type Svc = SystemOrganizationService<inferadb_ledger_store::InMemoryBackend>;
        assert!(Svc::MAX_TOTP_CHALLENGES >= Svc::MAX_ACTIVE_TOTP_CHALLENGES as usize);
    }

    #[test]
    fn test_totp_challenge_rate_limiting() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let slug = UserSlug::new(100);

        // Create 3 challenges (the maximum)
        for i in 0..3u8 {
            let mut nonce = [0u8; 32];
            nonce[0] = i;
            let challenge = make_challenge(user_id, slug, nonce);
            svc.create_totp_challenge(&challenge).unwrap();
        }

        // Fourth should be rate-limited
        let mut nonce = [0u8; 32];
        nonce[0] = 3;
        let challenge = make_challenge(user_id, slug, nonce);
        let err = svc.create_totp_challenge(&challenge).unwrap_err();
        assert!(matches!(err, SystemError::ResourceExhausted { .. }));
    }

    #[test]
    fn test_totp_challenges_isolated_between_users() {
        let svc = create_test_service();
        let user1 = UserId::new(1);
        let user2 = UserId::new(2);

        // Each user can have their own challenges
        let c1 = make_challenge(user1, UserSlug::new(100), [0x01; 32]);
        let c2 = make_challenge(user2, UserSlug::new(200), [0x01; 32]);

        svc.create_totp_challenge(&c1).unwrap();
        svc.create_totp_challenge(&c2).unwrap();

        // Reading user1's challenge doesn't return user2's
        let fetched = svc.get_totp_challenge(user1, &[0x01; 32]).unwrap().unwrap();
        assert_eq!(fetched.user, user1);
    }

    #[test]
    fn test_delete_all_totp_challenges() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let slug = UserSlug::new(100);

        for i in 0..3u8 {
            let mut nonce = [0u8; 32];
            nonce[0] = i;
            let challenge = make_challenge(user_id, slug, nonce);
            svc.create_totp_challenge(&challenge).unwrap();
        }

        svc.delete_all_totp_challenges(user_id).unwrap();

        // All challenges gone
        for i in 0..3u8 {
            let mut nonce = [0u8; 32];
            nonce[0] = i;
            assert!(svc.get_totp_challenge(user_id, &nonce).unwrap().is_none());
        }
    }

    // ========================================================================
    // Recovery code consumption tests
    // ========================================================================

    #[test]
    fn test_consume_recovery_code() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::RecoveryCode,
                make_recovery_code_data(),
                "Recovery Codes",
                now,
            )
            .unwrap();

        // Consume the first code ([0xAA; 32])
        let remaining = svc.consume_recovery_code(user_id, cred.id, &[0xAA; 32], now).unwrap();
        assert_eq!(remaining, 2);

        // Verify the credential was updated
        let updated = svc.get_user_credential(user_id, cred.id).unwrap().unwrap();
        let CredentialData::RecoveryCode(rc) = &updated.credential_data else {
            unreachable!("Expected recovery code data")
        };
        assert_eq!(rc.code_hashes.len(), 2);
        assert!(!rc.code_hashes.contains(&[0xAA; 32]));
        assert!(updated.last_used_at.is_some());
    }

    #[test]
    fn test_consume_recovery_code_not_found() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::RecoveryCode,
                make_recovery_code_data(),
                "Recovery Codes",
                now,
            )
            .unwrap();

        let err = svc.consume_recovery_code(user_id, cred.id, &[0xFF; 32], now).unwrap_err();
        assert!(matches!(err, SystemError::NotFound { .. }));
    }

    #[test]
    fn test_consume_recovery_code_wrong_credential_type() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::Passkey,
                make_passkey_data(b"pk1"),
                "Passkey",
                now,
            )
            .unwrap();

        let err = svc.consume_recovery_code(user_id, cred.id, &[0xAA; 32], now).unwrap_err();
        assert!(matches!(err, SystemError::FailedPrecondition { .. }));
    }

    #[test]
    fn test_consume_recovery_code_same_code_twice() {
        let svc = create_test_service();
        let user_id = UserId::new(1);
        let now = Utc::now();

        let cred = svc
            .create_user_credential(
                user_id,
                CredentialType::RecoveryCode,
                make_recovery_code_data(),
                "Recovery Codes",
                now,
            )
            .unwrap();

        svc.consume_recovery_code(user_id, cred.id, &[0xAA; 32], now).unwrap();

        // Second consumption of same code fails
        let err = svc.consume_recovery_code(user_id, cred.id, &[0xAA; 32], now).unwrap_err();
        assert!(matches!(err, SystemError::NotFound { .. }));
    }
}
