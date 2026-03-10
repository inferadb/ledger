//! Token state operations for signing keys and refresh tokens.
//!
//! Extends [`SystemOrganizationService`] with CRUD operations for JWT
//! signing keys and refresh tokens, called from Raft apply handlers.

use chrono::{DateTime, Utc};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    AppSlug, Operation, OrganizationId, RefreshTokenId, SigningKeyId, TokenSubject, TokenVersion,
    UserId, UserSlug, VaultId, decode, encode,
};
use snafu::ResultExt;
use tracing::warn;

use super::{
    keys::SystemKeys,
    service::{CodecSnafu, Result, SYSTEM_VAULT_ID, StateSnafu, SystemOrganizationService},
    types::{RefreshToken, SigningKey, SigningKeyScope, SigningKeyStatus, User},
};

/// Maximum number of entries scanned in a prefix iteration.
///
/// Scans that hit this limit log a warning — see [`warn_if_truncated`].
const MAX_TOKEN_SCAN: usize = 100_000;

/// Creates an unconditional, non-expiring `SetEntity` operation.
fn set_entity(key: String, value: Vec<u8>) -> Operation {
    Operation::SetEntity { key, value, condition: None, expires_at: None }
}

/// Logs a warning if the scan result was truncated at `MAX_TOKEN_SCAN`.
///
/// Used by GC paths where truncation is non-fatal (the next cycle catches up).
/// Security-critical revocation paths use [`check_revocation_truncation`] instead.
fn warn_if_truncated(count: usize, operation: &str) {
    if count >= MAX_TOKEN_SCAN {
        warn!(
            count,
            limit = MAX_TOKEN_SCAN,
            operation,
            "Token scan hit MAX_TOKEN_SCAN limit — results may be incomplete"
        );
    }
}

/// Returns `RevocationIncomplete` if the scan was truncated.
///
/// Used by security-critical revocation paths where partial processing is an error.
fn check_revocation_truncation(truncated: bool, operation: &str, revoked: u64) -> Result<()> {
    if truncated {
        return Err(super::service::SystemError::RevocationIncomplete {
            operation: operation.to_owned(),
            limit: MAX_TOKEN_SCAN,
            revoked,
        });
    }
    Ok(())
}

/// Result of revoking tokens by family.
#[derive(Debug)]
pub struct FamilyRevocationResult {
    /// Number of individual tokens revoked.
    pub revoked_count: u64,
}

/// Result of revoking tokens by subject.
#[derive(Debug)]
pub struct SubjectRevocationResult {
    /// Number of individual tokens revoked.
    pub revoked_count: u64,
}

/// Result of revoking all user sessions (tokens + version bump).
#[derive(Debug)]
pub struct AllUserSessionsRevocationResult {
    /// Number of individual tokens revoked.
    pub revoked_count: u64,
    /// New token version after increment.
    pub new_version: TokenVersion,
}

/// Result of cleaning up expired refresh tokens.
#[derive(Debug)]
pub struct ExpiredTokenCleanupResult {
    /// Number of expired tokens deleted.
    pub expired_count: u64,
    /// Number of poisoned families garbage-collected.
    pub poisoned_families_cleaned: u64,
}
impl<B: StorageBackend> SystemOrganizationService<B> {
    // =========================================================================
    // Signing Key Operations
    // =========================================================================

    /// Stores a signing key with all associated indexes.
    ///
    /// Writes: primary entity, kid index, scope index, and org prefix index
    /// (for organization-scoped keys).
    pub fn store_signing_key(&self, key: &SigningKey) -> Result<()> {
        let mut ops = vec![
            set_entity(SystemKeys::signing_key(key.id), encode(key).context(CodecSnafu)?),
            set_entity(
                SystemKeys::signing_key_kid_index(&key.kid),
                encode(&key.id).context(CodecSnafu)?,
            ),
            set_entity(
                SystemKeys::signing_key_scope_index(&key.scope),
                key.kid.as_bytes().to_vec(),
            ),
        ];

        // For organization-scoped keys, add an org prefix index entry
        // so org deletion can find all signing keys for a given org.
        if let SigningKeyScope::Organization(org_id) = key.scope {
            let org_index_key =
                format!("{}{}", SystemKeys::signing_key_org_prefix(org_id), key.id.value());
            ops.push(set_entity(org_index_key, encode(&key.id).context(CodecSnafu)?));
        }

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Looks up a signing key by its kid (UUID key identifier).
    ///
    /// Returns `None` if no key exists with the given kid.
    pub fn get_signing_key_by_kid(&self, kid: &str) -> Result<Option<SigningKey>> {
        let index_key = SystemKeys::signing_key_kid_index(kid);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, index_key.as_bytes()).context(StateSnafu)?;

        let id: SigningKeyId = match entity_opt {
            Some(entity) => decode(&entity.value).context(CodecSnafu)?,
            None => return Ok(None),
        };

        let primary_key = SystemKeys::signing_key(id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, primary_key.as_bytes()).context(StateSnafu)?;

        match entity_opt {
            Some(entity) => {
                let key: SigningKey = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(key))
            },
            None => Ok(None),
        }
    }

    /// Returns the active signing key for a given scope, if one exists.
    ///
    /// Looks up the scope index to find the current active key's kid,
    /// then retrieves the full signing key entity.
    pub fn get_active_signing_key(&self, scope: &SigningKeyScope) -> Result<Option<SigningKey>> {
        let scope_key = SystemKeys::signing_key_scope_index(scope);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, scope_key.as_bytes()).context(StateSnafu)?;

        let kid = match entity_opt {
            Some(entity) => String::from_utf8_lossy(&entity.value).to_string(),
            None => return Ok(None),
        };

        self.get_signing_key_by_kid(&kid)
    }

    /// Lists all signing keys for a given scope (active + rotated, including revoked).
    ///
    /// For organization-scoped keys, scans the org prefix index.
    /// For global keys, scans the signing key prefix and filters.
    pub fn list_signing_keys(&self, scope: &SigningKeyScope) -> Result<Vec<SigningKey>> {
        match scope {
            SigningKeyScope::Organization(org_id) => {
                let prefix = SystemKeys::signing_key_org_prefix(*org_id);
                let entries = self
                    .state
                    .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, MAX_TOKEN_SCAN)
                    .context(StateSnafu)?;

                let mut keys = Vec::new();
                for entry in entries {
                    let id: SigningKeyId = match decode(&entry.value) {
                        Ok(id) => id,
                        Err(e) => {
                            warn!(error = %e, "Skipping corrupt signing key org index entry");
                            continue;
                        },
                    };
                    let primary_key = SystemKeys::signing_key(id);
                    if let Some(entity) = self
                        .state
                        .get_entity(SYSTEM_VAULT_ID, primary_key.as_bytes())
                        .context(StateSnafu)?
                    {
                        match decode::<SigningKey>(&entity.value) {
                            Ok(key) => keys.push(key),
                            Err(e) => {
                                warn!(error = %e, "Skipping corrupt signing key entry");
                            },
                        }
                    }
                }
                Ok(keys)
            },
            SigningKeyScope::Global => {
                let entries = self
                    .state
                    .list_entities(
                        SYSTEM_VAULT_ID,
                        Some(SystemKeys::SIGNING_KEY_PREFIX),
                        None,
                        MAX_TOKEN_SCAN,
                    )
                    .context(StateSnafu)?;

                let mut keys = Vec::new();
                for entry in entries {
                    match decode::<SigningKey>(&entry.value) {
                        Ok(key) if key.scope == SigningKeyScope::Global => keys.push(key),
                        Ok(_) => {}, // Different scope, skip
                        Err(e) => {
                            warn!(error = %e, "Skipping corrupt signing key entry");
                        },
                    }
                }
                Ok(keys)
            },
        }
    }

    /// Returns the KIDs of all signing keys in `Rotated` status whose
    /// `valid_until` has passed relative to `now`.
    ///
    /// Scans all signing keys regardless of scope. Used by the maintenance
    /// job to propose `TransitionSigningKeyRevoked` for each.
    pub fn list_rotated_keys_past_grace(&self, now: DateTime<Utc>) -> Result<Vec<String>> {
        let entries = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::SIGNING_KEY_PREFIX),
                None,
                MAX_TOKEN_SCAN,
            )
            .context(StateSnafu)?;

        let mut kids = Vec::new();
        for entry in entries {
            let key_str = String::from_utf8_lossy(&entry.key);
            // Skip org index entries (they're secondary indexes, not primary records)
            if key_str.contains(":org:") {
                continue;
            }
            let key: SigningKey = match decode(&entry.value) {
                Ok(k) => k,
                Err(e) => {
                    warn!(error = %e, "Skipping corrupt signing key during maintenance scan");
                    continue;
                },
            };
            if key.status == SigningKeyStatus::Rotated
                && let Some(valid_until) = key.valid_until
                && valid_until < now
            {
                kids.push(key.kid);
            }
        }
        Ok(kids)
    }

    /// Updates a signing key's status and related timestamps.
    ///
    /// Used by rotation (Active→Rotated), revocation (Active/Rotated→Revoked),
    /// and grace period expiry transition (Rotated→Revoked).
    pub fn update_signing_key_status(
        &self,
        kid: &str,
        new_status: SigningKeyStatus,
        valid_until: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<Option<SigningKey>> {
        let mut key = match self.get_signing_key_by_kid(kid)? {
            Some(key) => key,
            None => return Ok(None),
        };

        let previous_status = key.status;
        key.status = new_status;
        match new_status {
            SigningKeyStatus::Rotated => {
                key.rotated_at = Some(now);
                key.valid_until = valid_until;
            },
            SigningKeyStatus::Revoked => {
                key.revoked_at = Some(now);
                key.valid_until = valid_until;
            },
            SigningKeyStatus::Active => {
                // No timestamp changes for re-activation
            },
        }

        let primary_key = SystemKeys::signing_key(key.id);
        let value = encode(&key).context(CodecSnafu)?;

        let mut ops = vec![set_entity(primary_key, value)];

        // Only remove scope index when revoking a key that was previously Active.
        // Rotated keys have already been replaced in the scope index by their
        // successor, so deleting the index here would break active key lookup.
        if new_status == SigningKeyStatus::Revoked && previous_status == SigningKeyStatus::Active {
            let scope_key = SystemKeys::signing_key_scope_index(&key.scope);
            ops.push(Operation::DeleteEntity { key: scope_key });
        }

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(Some(key))
    }

    /// Deletes all signing keys for an organization (cascade on org deletion).
    ///
    /// Removes primary entities, kid indexes, org prefix indexes, and scope index.
    pub fn delete_org_signing_keys(&self, org: OrganizationId) -> Result<u64> {
        let keys = self.list_signing_keys(&SigningKeyScope::Organization(org))?;
        if keys.is_empty() {
            return Ok(0);
        }

        let mut ops = Vec::new();
        for key in &keys {
            // Primary entity
            ops.push(Operation::DeleteEntity { key: SystemKeys::signing_key(key.id) });
            // Kid index
            ops.push(Operation::DeleteEntity { key: SystemKeys::signing_key_kid_index(&key.kid) });
            // Org prefix index entry
            let org_index_key =
                format!("{}{}", SystemKeys::signing_key_org_prefix(org), key.id.value());
            ops.push(Operation::DeleteEntity { key: org_index_key });
        }

        // Scope index
        ops.push(Operation::DeleteEntity {
            key: SystemKeys::signing_key_scope_index(&SigningKeyScope::Organization(org)),
        });

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(keys.len() as u64)
    }

    // =========================================================================
    // Refresh Token Operations
    // =========================================================================

    /// Stores a refresh token with all associated indexes.
    ///
    /// Writes: primary entity, hash index, family index, subject index,
    /// and app_vault index (if vault is set).
    pub fn store_refresh_token(&self, token: &RefreshToken) -> Result<()> {
        let id_value = encode(&token.id).context(CodecSnafu)?;

        let mut ops = vec![
            set_entity(SystemKeys::refresh_token(token.id), encode(token).context(CodecSnafu)?),
            set_entity(SystemKeys::refresh_token_hash_index(&token.token_hash), id_value.clone()),
            set_entity(
                SystemKeys::refresh_token_family_entry(&token.family, token.id),
                id_value.clone(),
            ),
            set_entity(
                SystemKeys::refresh_token_subject_entry(&token.subject, token.id),
                id_value.clone(),
            ),
        ];

        // Add app_vault index entry for vault tokens
        if let (Some(vault), TokenSubject::App(app_slug)) = (token.vault, &token.subject) {
            ops.push(set_entity(
                SystemKeys::refresh_token_app_vault_entry(*app_slug, vault, token.id),
                id_value,
            ));
        }

        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Looks up a refresh token by its SHA-256 hash.
    ///
    /// Returns `None` if no token exists with the given hash.
    pub fn get_refresh_token_by_hash(&self, hash: &[u8; 32]) -> Result<Option<RefreshToken>> {
        let index_key = SystemKeys::refresh_token_hash_index(hash);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, index_key.as_bytes()).context(StateSnafu)?;

        let id: RefreshTokenId = match entity_opt {
            Some(entity) => decode(&entity.value).context(CodecSnafu)?,
            None => return Ok(None),
        };

        let primary_key = SystemKeys::refresh_token(id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, primary_key.as_bytes()).context(StateSnafu)?;

        match entity_opt {
            Some(entity) => {
                let token: RefreshToken = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(token))
            },
            None => Ok(None),
        }
    }

    /// Marks a refresh token as used (rotate-on-use).
    pub fn mark_refresh_token_used(
        &self,
        id: RefreshTokenId,
        used_at: DateTime<Utc>,
    ) -> Result<bool> {
        let primary_key = SystemKeys::refresh_token(id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, primary_key.as_bytes()).context(StateSnafu)?;

        let mut token: RefreshToken = match entity_opt {
            Some(entity) => decode(&entity.value).context(CodecSnafu)?,
            None => return Ok(false),
        };

        token.used = true;
        token.used_at = Some(used_at);

        let value = encode(&token).context(CodecSnafu)?;
        let ops = vec![set_entity(primary_key, value)];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(true)
    }

    /// Writes a poisoned family marker (O(1) reuse detection flag).
    ///
    /// Background `TokenMaintenanceJob` garbage-collects poisoned families.
    pub fn poison_token_family(&self, family: &[u8; 16]) -> Result<()> {
        let key = SystemKeys::refresh_token_family_poisoned(family);
        let ops = vec![set_entity(key, vec![1])];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Checks whether a token family is poisoned (reuse detected).
    pub fn is_family_poisoned(&self, family: &[u8; 16]) -> Result<bool> {
        let key = SystemKeys::refresh_token_family_poisoned(family);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        Ok(entity_opt.is_some())
    }

    /// Revokes all refresh tokens in a family and removes the poisoned marker.
    ///
    /// The poisoned marker is only removed after all tokens are confirmed revoked.
    /// If the scan is truncated at `MAX_TOKEN_SCAN`, the marker is preserved so
    /// the next GC cycle retries the remaining tokens.
    pub fn revoke_token_family(
        &self,
        family: &[u8; 16],
        now: DateTime<Utc>,
    ) -> Result<FamilyRevocationResult> {
        let prefix = SystemKeys::refresh_token_family_prefix(family);
        let entries = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(&prefix), None, MAX_TOKEN_SCAN)
            .context(StateSnafu)?;
        let truncated = entries.len() >= MAX_TOKEN_SCAN;

        let mut revoked_count = 0u64;
        let mut ops = Vec::new();

        for entry in entries {
            let id: RefreshTokenId = match decode(&entry.value) {
                Ok(id) => id,
                Err(e) => {
                    warn!(error = %e, "Skipping corrupt family index entry");
                    continue;
                },
            };

            let primary_key = SystemKeys::refresh_token(id);
            if let Some(entity) = self
                .state
                .get_entity(SYSTEM_VAULT_ID, primary_key.as_bytes())
                .context(StateSnafu)?
            {
                let mut token: RefreshToken = match decode(&entity.value) {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(error = %e, "Skipping corrupt refresh token entry");
                        continue;
                    },
                };
                if token.revoked_at.is_none() {
                    token.revoked_at = Some(now);
                    let value = encode(&token).context(CodecSnafu)?;
                    ops.push(set_entity(primary_key, value));
                    revoked_count += 1;
                }
            }
        }

        if !ops.is_empty() {
            self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        }

        // Only remove poisoned marker after all tokens are confirmed revoked.
        // If truncated, the marker must remain so the next GC cycle retries.
        if !truncated {
            let marker_ops = vec![Operation::DeleteEntity {
                key: SystemKeys::refresh_token_family_poisoned(family),
            }];
            self.state.apply_operations(SYSTEM_VAULT_ID, &marker_ops, 0).context(StateSnafu)?;
        }

        check_revocation_truncation(truncated, "revoke_token_family", revoked_count)?;

        Ok(FamilyRevocationResult { revoked_count })
    }

    /// Revokes all refresh tokens for a given subject (user or app).
    ///
    /// Iterates the subject index, collects family IDs, and revokes each family.
    pub fn revoke_all_subject_tokens(
        &self,
        subject: &TokenSubject,
        now: DateTime<Utc>,
    ) -> Result<SubjectRevocationResult> {
        let prefix = SystemKeys::refresh_token_subject_prefix(subject);
        self.revoke_families_from_index(&prefix, now)
    }

    /// Revokes all refresh tokens for a specific app+vault combination.
    ///
    /// Used for vault disconnect cascade. Uses the dedicated app_vault index.
    pub fn revoke_app_vault_tokens(
        &self,
        app: AppSlug,
        vault: VaultId,
        now: DateTime<Utc>,
    ) -> Result<SubjectRevocationResult> {
        let prefix = SystemKeys::refresh_token_app_vault_prefix(app, vault);
        self.revoke_families_from_index(&prefix, now)
    }

    /// Revokes all refresh tokens for an organization.
    ///
    /// Scans all refresh tokens and revokes those belonging to the given org.
    pub fn revoke_all_org_refresh_tokens(
        &self,
        org: OrganizationId,
        now: DateTime<Utc>,
    ) -> Result<SubjectRevocationResult> {
        let entries = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::REFRESH_TOKEN_PREFIX),
                None,
                MAX_TOKEN_SCAN,
            )
            .context(StateSnafu)?;
        let mut truncated = entries.len() >= MAX_TOKEN_SCAN;

        let mut total_revoked = 0u64;
        let mut seen_families = std::collections::HashSet::new();

        for entry in entries {
            let token: RefreshToken = match decode(&entry.value) {
                Ok(t) => t,
                Err(e) => {
                    warn!(error = %e, "Skipping corrupt refresh token during org revocation");
                    continue;
                },
            };

            if token.organization == Some(org) && seen_families.insert(token.family) {
                match self.revoke_token_family(&token.family, now) {
                    Ok(result) => total_revoked += result.revoked_count,
                    Err(super::service::SystemError::RevocationIncomplete { revoked, .. }) => {
                        total_revoked += revoked;
                        truncated = true;
                    },
                    Err(e) => return Err(e),
                }
            }
        }

        check_revocation_truncation(truncated, "revoke_all_org_refresh_tokens", total_revoked)?;

        Ok(SubjectRevocationResult { revoked_count: total_revoked })
    }

    /// Deletes expired refresh tokens and garbage-collects poisoned families.
    ///
    /// `before` is the cutoff timestamp — tokens with `expires_at < before` are deleted.
    /// Uses `proposed_at` from the Raft payload for deterministic replay.
    pub fn delete_expired_refresh_tokens(
        &self,
        before: DateTime<Utc>,
    ) -> Result<ExpiredTokenCleanupResult> {
        // Phase 1: Delete expired tokens
        let entries = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::REFRESH_TOKEN_PREFIX),
                None,
                MAX_TOKEN_SCAN,
            )
            .context(StateSnafu)?;
        warn_if_truncated(entries.len(), "delete_expired_refresh_tokens");

        let mut expired_count = 0u64;
        let mut ops = Vec::new();

        for entry in &entries {
            let token: RefreshToken = match decode(&entry.value) {
                Ok(t) => t,
                Err(e) => {
                    warn!(error = %e, "Skipping corrupt refresh token during cleanup");
                    continue;
                },
            };

            if token.expires_at < before {
                // Delete primary entity
                ops.push(Operation::DeleteEntity { key: SystemKeys::refresh_token(token.id) });
                // Delete hash index
                ops.push(Operation::DeleteEntity {
                    key: SystemKeys::refresh_token_hash_index(&token.token_hash),
                });
                // Delete family index entry
                ops.push(Operation::DeleteEntity {
                    key: SystemKeys::refresh_token_family_entry(&token.family, token.id),
                });
                // Delete subject index entry
                ops.push(Operation::DeleteEntity {
                    key: SystemKeys::refresh_token_subject_entry(&token.subject, token.id),
                });
                // Delete app_vault index entry if applicable
                if let (Some(vault), TokenSubject::App(app_slug)) = (token.vault, &token.subject) {
                    ops.push(Operation::DeleteEntity {
                        key: SystemKeys::refresh_token_app_vault_entry(*app_slug, vault, token.id),
                    });
                }
                expired_count += 1;
            }
        }

        if !ops.is_empty() {
            self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        }

        // Phase 2: Garbage-collect poisoned families
        let poisoned_families_cleaned = self.gc_poisoned_families(before)?;

        Ok(ExpiredTokenCleanupResult { expired_count, poisoned_families_cleaned })
    }

    /// Garbage-collects poisoned token families.
    ///
    /// Iterates all poisoned family markers, revokes remaining tokens in
    /// each family, and removes the markers.
    fn gc_poisoned_families(&self, now: DateTime<Utc>) -> Result<u64> {
        let entries = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::REFRESH_TOKEN_FAMILY_POISONED_PREFIX),
                None,
                MAX_TOKEN_SCAN,
            )
            .context(StateSnafu)?;
        warn_if_truncated(entries.len(), "gc_poisoned_families");

        let mut cleaned = 0u64;

        for entry in entries {
            let key_str = String::from_utf8_lossy(&entry.key);
            // Extract family hex from the key: _idx:refresh_token:family_poisoned:{hex}
            let hex = match key_str.strip_prefix(SystemKeys::REFRESH_TOKEN_FAMILY_POISONED_PREFIX) {
                Some(h) => h,
                None => continue,
            };

            // Parse hex back to family bytes
            let family = match parse_hex_family(hex) {
                Some(f) => f,
                None => {
                    warn!(hex = %hex, "Skipping invalid poisoned family hex");
                    continue;
                },
            };

            match self.revoke_token_family(&family, now) {
                Ok(_) => cleaned += 1,
                Err(super::service::SystemError::RevocationIncomplete { revoked, .. }) => {
                    // Partial revocation — poisoned marker preserved by revoke_token_family
                    // so the next GC cycle will retry remaining tokens.
                    tracing::warn!(
                        revoked,
                        "Poisoned family partially revoked — marker preserved for retry"
                    );
                },
                Err(e) => return Err(e),
            }
        }

        Ok(cleaned)
    }

    /// Increments a user's token version and returns the new value.
    ///
    /// Used by `RevokeAllUserSessions` for forced session invalidation.
    fn increment_user_token_version(
        &self,
        user_id: UserId,
        now: DateTime<Utc>,
    ) -> Result<TokenVersion> {
        let user_key = SystemKeys::user_key(user_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, user_key.as_bytes()).context(StateSnafu)?;

        let mut user: User = match entity_opt {
            Some(entity) => decode(&entity.value).context(CodecSnafu)?,
            None => {
                return Err(super::service::SystemError::NotFound {
                    entity: format!("user:{user_id}"),
                });
            },
        };

        let new_version = user.version.increment();
        user.version = new_version;
        user.updated_at = now;

        let value = encode(&user).context(CodecSnafu)?;
        let ops = vec![set_entity(user_key, value)];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;

        Ok(new_version)
    }

    /// Revokes all user sessions atomically: revokes tokens + increments version.
    ///
    /// Used by password change, account compromise, admin force-revoke.
    pub fn revoke_all_user_sessions(
        &self,
        user_id: UserId,
        user_slug: UserSlug,
        now: DateTime<Utc>,
    ) -> Result<AllUserSessionsRevocationResult> {
        let subject = TokenSubject::User(user_slug);
        let revocation_result = self.revoke_all_subject_tokens(&subject, now);

        // Always bump the token version, even on partial revocation.
        // The version bump is the backstop that invalidates all outstanding JWTs
        // via the version check, regardless of whether individual token revocation
        // completed fully.
        let (revoked_count, was_incomplete) = match revocation_result {
            Ok(result) => (result.revoked_count, false),
            Err(super::service::SystemError::RevocationIncomplete { revoked, .. }) => {
                (revoked, true)
            },
            Err(e) => return Err(e),
        };

        let new_version = self.increment_user_token_version(user_id, now)?;

        if was_incomplete {
            warn!(
                revoked_count,
                new_version = new_version.value(),
                "Partial revocation in revoke_all_user_sessions — version bumped as backstop"
            );
        }

        Ok(AllUserSessionsRevocationResult { revoked_count, new_version })
    }

    /// Scans a token index prefix, resolves each entry to a token, and revokes
    /// all unique families found.
    ///
    /// Shared implementation for `revoke_all_subject_tokens` and `revoke_app_vault_tokens`.
    fn revoke_families_from_index(
        &self,
        prefix: &str,
        now: DateTime<Utc>,
    ) -> Result<SubjectRevocationResult> {
        let entries = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(prefix), None, MAX_TOKEN_SCAN)
            .context(StateSnafu)?;
        let mut truncated = entries.len() >= MAX_TOKEN_SCAN;

        let mut total_revoked = 0u64;
        let mut seen_families = std::collections::HashSet::new();

        for entry in entries {
            let id: RefreshTokenId = match decode(&entry.value) {
                Ok(id) => id,
                Err(e) => {
                    warn!(error = %e, "Skipping corrupt token index entry");
                    continue;
                },
            };

            let primary_key = SystemKeys::refresh_token(id);
            if let Some(entity) = self
                .state
                .get_entity(SYSTEM_VAULT_ID, primary_key.as_bytes())
                .context(StateSnafu)?
            {
                let token: RefreshToken = match decode(&entity.value) {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(error = %e, "Skipping corrupt refresh token");
                        continue;
                    },
                };

                if seen_families.insert(token.family) {
                    match self.revoke_token_family(&token.family, now) {
                        Ok(result) => total_revoked += result.revoked_count,
                        Err(super::service::SystemError::RevocationIncomplete {
                            revoked, ..
                        }) => {
                            total_revoked += revoked;
                            truncated = true;
                        },
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        check_revocation_truncation(truncated, "revoke_families_from_index", total_revoked)?;

        Ok(SubjectRevocationResult { revoked_count: total_revoked })
    }
}

/// Parses a 32-character hex string into a 16-byte array.
fn parse_hex_family(hex: &str) -> Option<[u8; 16]> {
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let hi = hex.as_bytes().get(i * 2)?;
        let lo = hex.as_bytes().get(i * 2 + 1)?;
        *byte = (hex_digit(*hi)? << 4) | hex_digit(*lo)?;
    }
    Some(bytes)
}

/// Converts an ASCII hex digit to its numeric value.
fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::Arc;

    use chrono::{Duration, Utc};
    use inferadb_ledger_types::{
        AppSlug, OrganizationId, RefreshTokenId, SigningKeyId, TokenSubject, TokenType, UserSlug,
        VaultId,
    };

    use super::*;
    use crate::{
        engine::InMemoryStorageEngine, state::StateLayer,
        system::service::SystemOrganizationService,
    };

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        let engine = InMemoryStorageEngine::open().unwrap();
        let state = Arc::new(StateLayer::new(engine.db()));
        SystemOrganizationService::new(state)
    }

    fn make_signing_key(
        id: i64,
        kid: &str,
        scope: SigningKeyScope,
        status: SigningKeyStatus,
    ) -> SigningKey {
        let now = Utc::now();
        SigningKey {
            id: SigningKeyId::new(id),
            kid: kid.to_string(),
            public_key_bytes: vec![0u8; 32],
            encrypted_private_key: vec![0u8; 100],
            rmk_version: 1,
            scope,
            status,
            valid_from: now,
            valid_until: None,
            created_at: now,
            rotated_at: None,
            revoked_at: None,
        }
    }

    fn make_refresh_token(
        id: i64,
        hash: [u8; 32],
        family: [u8; 16],
        token_type: TokenType,
        subject: TokenSubject,
        organization: Option<OrganizationId>,
        vault: Option<VaultId>,
    ) -> RefreshToken {
        let now = Utc::now();
        RefreshToken {
            id: RefreshTokenId::new(id),
            token_hash: hash,
            family,
            token_type,
            subject,
            organization,
            vault,
            kid: "test-kid".to_string(),
            expires_at: now + Duration::hours(1),
            used: false,
            created_at: now,
            used_at: None,
            revoked_at: None,
        }
    }

    // =========================================================================
    // Signing Key Tests
    // =========================================================================

    #[test]
    fn test_store_and_get_signing_key_by_kid() {
        let svc = create_test_service();
        let key = make_signing_key(1, "kid-001", SigningKeyScope::Global, SigningKeyStatus::Active);

        svc.store_signing_key(&key).unwrap();

        let retrieved = svc.get_signing_key_by_kid("kid-001").unwrap().unwrap();
        assert_eq!(retrieved.id, key.id);
        assert_eq!(retrieved.kid, "kid-001");
        assert_eq!(retrieved.scope, SigningKeyScope::Global);
    }

    #[test]
    fn test_get_signing_key_by_kid_not_found() {
        let svc = create_test_service();
        assert!(svc.get_signing_key_by_kid("nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_get_active_signing_key_global() {
        let svc = create_test_service();
        let key =
            make_signing_key(1, "global-kid", SigningKeyScope::Global, SigningKeyStatus::Active);

        svc.store_signing_key(&key).unwrap();

        let active = svc.get_active_signing_key(&SigningKeyScope::Global).unwrap().unwrap();
        assert_eq!(active.kid, "global-kid");
    }

    #[test]
    fn test_get_active_signing_key_org() {
        let svc = create_test_service();
        let org_id = OrganizationId::new(42);
        let scope = SigningKeyScope::Organization(org_id);
        let key = make_signing_key(1, "org-kid", scope, SigningKeyStatus::Active);

        svc.store_signing_key(&key).unwrap();

        let active = svc.get_active_signing_key(&scope).unwrap().unwrap();
        assert_eq!(active.kid, "org-kid");
    }

    #[test]
    fn test_get_active_signing_key_none() {
        let svc = create_test_service();
        assert!(svc.get_active_signing_key(&SigningKeyScope::Global).unwrap().is_none());
    }

    #[test]
    fn test_list_signing_keys_global() {
        let svc = create_test_service();
        let key1 =
            make_signing_key(1, "global-1", SigningKeyScope::Global, SigningKeyStatus::Active);
        let key2 =
            make_signing_key(2, "global-2", SigningKeyScope::Global, SigningKeyStatus::Rotated);

        svc.store_signing_key(&key1).unwrap();
        svc.store_signing_key(&key2).unwrap();

        let keys = svc.list_signing_keys(&SigningKeyScope::Global).unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_list_signing_keys_org() {
        let svc = create_test_service();
        let org_id = OrganizationId::new(10);
        let scope = SigningKeyScope::Organization(org_id);

        let key1 = make_signing_key(1, "org-1", scope, SigningKeyStatus::Active);
        let key2 = make_signing_key(2, "org-2", scope, SigningKeyStatus::Rotated);

        svc.store_signing_key(&key1).unwrap();
        svc.store_signing_key(&key2).unwrap();

        let keys = svc.list_signing_keys(&scope).unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_list_signing_keys_org_isolation() {
        let svc = create_test_service();
        let org1 = OrganizationId::new(10);
        let org2 = OrganizationId::new(20);

        let key1 = make_signing_key(
            1,
            "org1-key",
            SigningKeyScope::Organization(org1),
            SigningKeyStatus::Active,
        );
        let key2 = make_signing_key(
            2,
            "org2-key",
            SigningKeyScope::Organization(org2),
            SigningKeyStatus::Active,
        );
        let global_key =
            make_signing_key(3, "global-key", SigningKeyScope::Global, SigningKeyStatus::Active);

        svc.store_signing_key(&key1).unwrap();
        svc.store_signing_key(&key2).unwrap();
        svc.store_signing_key(&global_key).unwrap();

        let org1_keys = svc.list_signing_keys(&SigningKeyScope::Organization(org1)).unwrap();
        assert_eq!(org1_keys.len(), 1);
        assert_eq!(org1_keys[0].kid, "org1-key");

        let org2_keys = svc.list_signing_keys(&SigningKeyScope::Organization(org2)).unwrap();
        assert_eq!(org2_keys.len(), 1);
        assert_eq!(org2_keys[0].kid, "org2-key");

        let global_keys = svc.list_signing_keys(&SigningKeyScope::Global).unwrap();
        assert_eq!(global_keys.len(), 1);
        assert_eq!(global_keys[0].kid, "global-key");
    }

    #[test]
    fn test_update_signing_key_status_to_rotated() {
        let svc = create_test_service();
        let key = make_signing_key(1, "kid-rot", SigningKeyScope::Global, SigningKeyStatus::Active);
        svc.store_signing_key(&key).unwrap();

        let now = Utc::now();
        let valid_until = now + Duration::hours(4);
        let updated = svc
            .update_signing_key_status("kid-rot", SigningKeyStatus::Rotated, Some(valid_until), now)
            .unwrap()
            .unwrap();

        assert_eq!(updated.status, SigningKeyStatus::Rotated);
        assert!(updated.rotated_at.is_some());
        assert!(updated.valid_until.is_some());
    }

    #[test]
    fn test_update_signing_key_status_to_revoked() {
        let svc = create_test_service();
        let key = make_signing_key(1, "kid-rev", SigningKeyScope::Global, SigningKeyStatus::Active);
        svc.store_signing_key(&key).unwrap();

        let now = Utc::now();
        let updated = svc
            .update_signing_key_status("kid-rev", SigningKeyStatus::Revoked, None, now)
            .unwrap()
            .unwrap();

        assert_eq!(updated.status, SigningKeyStatus::Revoked);
        assert!(updated.revoked_at.is_some());

        // Scope index should be removed — active key lookup returns None
        assert!(svc.get_active_signing_key(&SigningKeyScope::Global).unwrap().is_none());
    }

    #[test]
    fn test_revoke_rotated_key_preserves_successor_scope_index() {
        let svc = create_test_service();
        let scope = SigningKeyScope::Global;
        let now = Utc::now();

        // Key A is the active key — scope index points to "kid-A"
        let key_a = make_signing_key(1, "kid-A", scope, SigningKeyStatus::Active);
        svc.store_signing_key(&key_a).unwrap();
        assert_eq!(svc.get_active_signing_key(&scope).unwrap().unwrap().kid, "kid-A");

        // Key B replaces A as active — scope index now points to "kid-B"
        let key_b = make_signing_key(2, "kid-B", scope, SigningKeyStatus::Active);
        svc.store_signing_key(&key_b).unwrap();
        assert_eq!(svc.get_active_signing_key(&scope).unwrap().unwrap().kid, "kid-B");

        // Rotate A (Active → Rotated)
        let grace_end = now + Duration::hours(4);
        svc.update_signing_key_status("kid-A", SigningKeyStatus::Rotated, Some(grace_end), now)
            .unwrap()
            .unwrap();

        // Revoke A (Rotated → Revoked) — must NOT delete scope index
        svc.update_signing_key_status("kid-A", SigningKeyStatus::Revoked, None, now)
            .unwrap()
            .unwrap();

        // Scope index must still point to B
        let active = svc.get_active_signing_key(&scope).unwrap().unwrap();
        assert_eq!(active.kid, "kid-B");

        // Key A must be persisted as Revoked
        let revoked_a = svc.get_signing_key_by_kid("kid-A").unwrap().unwrap();
        assert_eq!(revoked_a.status, SigningKeyStatus::Revoked);
        assert!(revoked_a.revoked_at.is_some());
    }

    #[test]
    fn test_update_signing_key_status_not_found() {
        let svc = create_test_service();
        let result = svc
            .update_signing_key_status("nonexistent", SigningKeyStatus::Revoked, None, Utc::now())
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_org_signing_keys() {
        let svc = create_test_service();
        let org = OrganizationId::new(42);
        let scope = SigningKeyScope::Organization(org);

        let key1 = make_signing_key(1, "org42-a", scope, SigningKeyStatus::Active);
        let key2 = make_signing_key(2, "org42-b", scope, SigningKeyStatus::Rotated);

        svc.store_signing_key(&key1).unwrap();
        svc.store_signing_key(&key2).unwrap();

        let deleted = svc.delete_org_signing_keys(org).unwrap();
        assert_eq!(deleted, 2);

        // Verify cleanup
        assert!(svc.get_signing_key_by_kid("org42-a").unwrap().is_none());
        assert!(svc.get_signing_key_by_kid("org42-b").unwrap().is_none());
        assert!(svc.get_active_signing_key(&scope).unwrap().is_none());
        assert!(svc.list_signing_keys(&scope).unwrap().is_empty());
    }

    #[test]
    fn test_delete_org_signing_keys_empty() {
        let svc = create_test_service();
        let deleted = svc.delete_org_signing_keys(OrganizationId::new(99)).unwrap();
        assert_eq!(deleted, 0);
    }

    // =========================================================================
    // Refresh Token Tests
    // =========================================================================

    #[test]
    fn test_store_and_get_refresh_token_by_hash() {
        let svc = create_test_service();
        let hash = [1u8; 32];
        let family = [2u8; 16];
        let token = make_refresh_token(
            1,
            hash,
            family,
            TokenType::UserSession,
            TokenSubject::User(UserSlug::new(100)),
            None,
            None,
        );

        svc.store_refresh_token(&token).unwrap();

        let retrieved = svc.get_refresh_token_by_hash(&hash).unwrap().unwrap();
        assert_eq!(retrieved.id, token.id);
        assert_eq!(retrieved.token_hash, hash);
        assert_eq!(retrieved.family, family);
    }

    #[test]
    fn test_get_refresh_token_by_hash_not_found() {
        let svc = create_test_service();
        assert!(svc.get_refresh_token_by_hash(&[0u8; 32]).unwrap().is_none());
    }

    #[test]
    fn test_store_refresh_token_with_vault_index() {
        let svc = create_test_service();
        let hash = [3u8; 32];
        let family = [4u8; 16];
        let app_slug = AppSlug::new(50);
        let vault_id = VaultId::new(60);
        let token = make_refresh_token(
            1,
            hash,
            family,
            TokenType::VaultAccess,
            TokenSubject::App(app_slug),
            Some(OrganizationId::new(10)),
            Some(vault_id),
        );

        svc.store_refresh_token(&token).unwrap();

        let retrieved = svc.get_refresh_token_by_hash(&hash).unwrap().unwrap();
        assert_eq!(retrieved.vault, Some(vault_id));
    }

    #[test]
    fn test_mark_refresh_token_used() {
        let svc = create_test_service();
        let hash = [5u8; 32];
        let family = [6u8; 16];
        let token = make_refresh_token(
            1,
            hash,
            family,
            TokenType::UserSession,
            TokenSubject::User(UserSlug::new(200)),
            None,
            None,
        );

        svc.store_refresh_token(&token).unwrap();

        let now = Utc::now();
        let marked = svc.mark_refresh_token_used(token.id, now).unwrap();
        assert!(marked);

        let retrieved = svc.get_refresh_token_by_hash(&hash).unwrap().unwrap();
        assert!(retrieved.used);
        assert!(retrieved.used_at.is_some());
    }

    #[test]
    fn test_mark_refresh_token_used_not_found() {
        let svc = create_test_service();
        let result = svc.mark_refresh_token_used(RefreshTokenId::new(999), Utc::now()).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_poison_and_check_family() {
        let svc = create_test_service();
        let family = [7u8; 16];

        assert!(!svc.is_family_poisoned(&family).unwrap());

        svc.poison_token_family(&family).unwrap();

        assert!(svc.is_family_poisoned(&family).unwrap());
    }

    #[test]
    fn test_revoke_token_family() {
        let svc = create_test_service();
        let family = [8u8; 16];

        let token1 = make_refresh_token(
            1,
            [10u8; 32],
            family,
            TokenType::UserSession,
            TokenSubject::User(UserSlug::new(300)),
            None,
            None,
        );
        let token2 = make_refresh_token(
            2,
            [11u8; 32],
            family,
            TokenType::UserSession,
            TokenSubject::User(UserSlug::new(300)),
            None,
            None,
        );

        svc.store_refresh_token(&token1).unwrap();
        svc.store_refresh_token(&token2).unwrap();

        // Poison then revoke
        svc.poison_token_family(&family).unwrap();

        let result = svc.revoke_token_family(&family, Utc::now()).unwrap();
        assert_eq!(result.revoked_count, 2);

        // Poisoned marker should be removed
        assert!(!svc.is_family_poisoned(&family).unwrap());

        // Both tokens should be revoked
        let t1 = svc.get_refresh_token_by_hash(&[10u8; 32]).unwrap().unwrap();
        assert!(t1.revoked_at.is_some());
        let t2 = svc.get_refresh_token_by_hash(&[11u8; 32]).unwrap().unwrap();
        assert!(t2.revoked_at.is_some());
    }

    #[test]
    fn test_revoke_token_family_already_revoked() {
        let svc = create_test_service();
        let family = [9u8; 16];

        let mut token = make_refresh_token(
            1,
            [12u8; 32],
            family,
            TokenType::UserSession,
            TokenSubject::User(UserSlug::new(400)),
            None,
            None,
        );
        token.revoked_at = Some(Utc::now());

        svc.store_refresh_token(&token).unwrap();

        let result = svc.revoke_token_family(&family, Utc::now()).unwrap();
        assert_eq!(result.revoked_count, 0); // Already revoked
    }

    #[test]
    fn test_revoke_all_subject_tokens() {
        let svc = create_test_service();
        let user_slug = UserSlug::new(500);
        let subject = TokenSubject::User(user_slug);
        let family1 = [20u8; 16];
        let family2 = [21u8; 16];

        let token1 =
            make_refresh_token(1, [30u8; 32], family1, TokenType::UserSession, subject, None, None);
        let token2 =
            make_refresh_token(2, [31u8; 32], family2, TokenType::UserSession, subject, None, None);

        svc.store_refresh_token(&token1).unwrap();
        svc.store_refresh_token(&token2).unwrap();

        let result = svc.revoke_all_subject_tokens(&subject, Utc::now()).unwrap();
        assert_eq!(result.revoked_count, 2);

        let t1 = svc.get_refresh_token_by_hash(&[30u8; 32]).unwrap().unwrap();
        assert!(t1.revoked_at.is_some());
        let t2 = svc.get_refresh_token_by_hash(&[31u8; 32]).unwrap().unwrap();
        assert!(t2.revoked_at.is_some());
    }

    #[test]
    fn test_revoke_app_vault_tokens() {
        let svc = create_test_service();
        let app_slug = AppSlug::new(60);
        let vault = VaultId::new(70);
        let org = OrganizationId::new(10);
        let family = [40u8; 16];

        let token = make_refresh_token(
            1,
            [50u8; 32],
            family,
            TokenType::VaultAccess,
            TokenSubject::App(app_slug),
            Some(org),
            Some(vault),
        );

        svc.store_refresh_token(&token).unwrap();

        let result = svc.revoke_app_vault_tokens(app_slug, vault, Utc::now()).unwrap();
        assert_eq!(result.revoked_count, 1);

        let retrieved = svc.get_refresh_token_by_hash(&[50u8; 32]).unwrap().unwrap();
        assert!(retrieved.revoked_at.is_some());
    }

    #[test]
    fn test_revoke_all_org_refresh_tokens() {
        let svc = create_test_service();
        let org = OrganizationId::new(42);

        let token1 = make_refresh_token(
            1,
            [60u8; 32],
            [70u8; 16],
            TokenType::VaultAccess,
            TokenSubject::App(AppSlug::new(1)),
            Some(org),
            Some(VaultId::new(1)),
        );
        let token2 = make_refresh_token(
            2,
            [61u8; 32],
            [71u8; 16],
            TokenType::VaultAccess,
            TokenSubject::App(AppSlug::new(2)),
            Some(org),
            Some(VaultId::new(2)),
        );
        // Token from a different org — should not be revoked
        let token3 = make_refresh_token(
            3,
            [62u8; 32],
            [72u8; 16],
            TokenType::VaultAccess,
            TokenSubject::App(AppSlug::new(3)),
            Some(OrganizationId::new(99)),
            Some(VaultId::new(3)),
        );

        svc.store_refresh_token(&token1).unwrap();
        svc.store_refresh_token(&token2).unwrap();
        svc.store_refresh_token(&token3).unwrap();

        let result = svc.revoke_all_org_refresh_tokens(org, Utc::now()).unwrap();
        assert_eq!(result.revoked_count, 2);

        // Other org's token is unaffected
        let t3 = svc.get_refresh_token_by_hash(&[62u8; 32]).unwrap().unwrap();
        assert!(t3.revoked_at.is_none());
    }

    #[test]
    fn test_delete_expired_refresh_tokens() {
        let svc = create_test_service();
        let now = Utc::now();

        // Expired token
        let mut expired = make_refresh_token(
            1,
            [80u8; 32],
            [81u8; 16],
            TokenType::UserSession,
            TokenSubject::User(UserSlug::new(600)),
            None,
            None,
        );
        expired.expires_at = now - Duration::hours(2);

        // Valid token
        let valid = make_refresh_token(
            2,
            [82u8; 32],
            [83u8; 16],
            TokenType::UserSession,
            TokenSubject::User(UserSlug::new(601)),
            None,
            None,
        );

        svc.store_refresh_token(&expired).unwrap();
        svc.store_refresh_token(&valid).unwrap();

        let result = svc.delete_expired_refresh_tokens(now).unwrap();
        assert_eq!(result.expired_count, 1);

        // Expired token is gone
        assert!(svc.get_refresh_token_by_hash(&[80u8; 32]).unwrap().is_none());

        // Valid token remains
        assert!(svc.get_refresh_token_by_hash(&[82u8; 32]).unwrap().is_some());
    }

    #[test]
    fn test_gc_poisoned_families() {
        let svc = create_test_service();
        let family = [90u8; 16];

        let token = make_refresh_token(
            1,
            [91u8; 32],
            family,
            TokenType::UserSession,
            TokenSubject::User(UserSlug::new(700)),
            None,
            None,
        );

        svc.store_refresh_token(&token).unwrap();
        svc.poison_token_family(&family).unwrap();

        let result = svc.delete_expired_refresh_tokens(Utc::now()).unwrap();
        assert_eq!(result.poisoned_families_cleaned, 1);

        // Token should be revoked
        let t = svc.get_refresh_token_by_hash(&[91u8; 32]).unwrap().unwrap();
        assert!(t.revoked_at.is_some());

        // Poisoned marker should be removed
        assert!(!svc.is_family_poisoned(&family).unwrap());
    }

    #[test]
    fn test_increment_user_token_version() {
        let svc = create_test_service();
        let slug = UserSlug::new(800);
        let (user_id, _) = svc
            .create_user(
                "Test User",
                "test@example.com",
                inferadb_ledger_types::Region::US_EAST_VA,
                inferadb_ledger_types::UserRole::User,
                slug,
            )
            .unwrap();

        let now = Utc::now();
        let v1 = svc.increment_user_token_version(user_id, now).unwrap();
        assert_eq!(v1.value(), 1);

        let v2 = svc.increment_user_token_version(user_id, now).unwrap();
        assert_eq!(v2.value(), 2);
    }

    #[test]
    fn test_increment_user_token_version_not_found() {
        let svc = create_test_service();
        let result = svc.increment_user_token_version(UserId::new(999), Utc::now());
        assert!(result.is_err());
    }

    #[test]
    fn test_revoke_all_user_sessions() {
        let svc = create_test_service();
        let slug = UserSlug::new(900);
        let (user_id, _) = svc
            .create_user(
                "Session User",
                "session@example.com",
                inferadb_ledger_types::Region::US_EAST_VA,
                inferadb_ledger_types::UserRole::User,
                slug,
            )
            .unwrap();

        let family = [95u8; 16];
        let token = make_refresh_token(
            1,
            [96u8; 32],
            family,
            TokenType::UserSession,
            TokenSubject::User(slug),
            None,
            None,
        );
        svc.store_refresh_token(&token).unwrap();

        let result = svc.revoke_all_user_sessions(user_id, slug, Utc::now()).unwrap();
        assert_eq!(result.revoked_count, 1);
        assert_eq!(result.new_version.value(), 1);

        // Token should be revoked
        let t = svc.get_refresh_token_by_hash(&[96u8; 32]).unwrap().unwrap();
        assert!(t.revoked_at.is_some());
    }

    #[test]
    fn test_parse_hex_family() {
        let family = [
            0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55,
            0x66, 0x77,
        ];
        let hex = "abcdef012345678900112233445566 77".replace(' ', "");
        let parsed = parse_hex_family(&hex).unwrap();
        assert_eq!(parsed, family);
    }

    #[test]
    fn test_parse_hex_family_invalid_length() {
        assert!(parse_hex_family("abcd").is_none());
    }

    #[test]
    fn test_parse_hex_family_invalid_chars() {
        assert!(parse_hex_family("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_none());
    }
}
