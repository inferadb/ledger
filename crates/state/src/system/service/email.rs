//! Email hash index and blinding key metadata operations (GLOBAL control plane).

use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{Operation, Region, UserId, decode, encode};
use snafu::ResultExt;

use super::{
    CodecSnafu, Result, SYSTEM_VAULT_ID, StateSnafu, SystemError, SystemKeys,
    SystemOrganizationService,
};
use crate::{state::StateError, system::types::EmailHashEntry};

impl<B: StorageBackend> SystemOrganizationService<B> {
    /// Registers an email HMAC hash → user ID mapping in the global control plane.
    pub fn register_email_hash(&self, hmac_hex: &str, user_id: UserId) -> Result<()> {
        let key = SystemKeys::email_hash_index_key(hmac_hex);
        let entry = EmailHashEntry::Active(user_id);
        let value = encode(&entry).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity {
            key: key.clone(),
            value,
            condition: Some(inferadb_ledger_types::SetCondition::MustNotExist),
            expires_at: None,
        }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).map_err(|e| {
            if matches!(e, StateError::PreconditionFailed { .. }) {
                SystemError::AlreadyExists { entity: format!("email_hash:{hmac_hex}") }
            } else {
                SystemError::State { source: Box::new(e) }
            }
        })?;
        Ok(())
    }

    /// Looks up an email hash entry by HMAC hex string.
    pub fn get_email_hash(&self, hmac_hex: &str) -> Result<Option<EmailHashEntry>> {
        let key = SystemKeys::email_hash_index_key(hmac_hex);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let entry: EmailHashEntry = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(entry))
            },
            None => Ok(None),
        }
    }

    /// Removes an email HMAC hash from the global index.
    pub fn remove_email_hash(&self, hmac_hex: &str) -> Result<()> {
        let key = SystemKeys::email_hash_index_key(hmac_hex);
        let ops = vec![Operation::DeleteEntity { key }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Returns the active email blinding key version, or `None` if not yet set.
    pub fn get_blinding_key_version(&self) -> Result<Option<u32>> {
        let entity_opt = self
            .state
            .get_entity(SYSTEM_VAULT_ID, SystemKeys::BLINDING_KEY_VERSION_KEY.as_bytes())
            .context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let version_str = String::from_utf8_lossy(&entity.value);
                let version: u32 = version_str.parse().map_err(|_| SystemError::NotFound {
                    entity: "blinding_key_version (corrupt)".to_string(),
                })?;
                Ok(Some(version))
            },
            None => Ok(None),
        }
    }

    /// Sets the active email blinding key version.
    pub fn set_blinding_key_version(&self, version: u32) -> Result<()> {
        let ops = vec![Operation::SetEntity {
            key: SystemKeys::BLINDING_KEY_VERSION_KEY.to_string(),
            value: version.to_string().into_bytes(),
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Returns the rehash progress (entries completed) for a region.
    pub fn get_rehash_progress(&self, region: Region) -> Result<Option<u64>> {
        let key = SystemKeys::rehash_progress_key(region);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let count_str = String::from_utf8_lossy(&entity.value);
                let count: u64 = count_str.parse().map_err(|_| SystemError::NotFound {
                    entity: format!("rehash_progress:{}", region.as_str()),
                })?;
                Ok(Some(count))
            },
            None => Ok(None),
        }
    }

    /// Updates the rehash progress for a region.
    pub fn set_rehash_progress(&self, region: Region, entries_rehashed: u64) -> Result<()> {
        let key = SystemKeys::rehash_progress_key(region);
        let ops = vec![Operation::SetEntity {
            key,
            value: entries_rehashed.to_string().into_bytes(),
            condition: None,
            expires_at: None,
        }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Clears the rehash progress for a region (rotation complete for that region).
    pub fn clear_rehash_progress(&self, region: Region) -> Result<()> {
        let key = SystemKeys::rehash_progress_key(region);
        let ops = vec![Operation::DeleteEntity { key }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Checks whether a blinding key rotation is in progress.
    pub fn is_rotation_in_progress(&self) -> Result<bool> {
        let entities = self
            .state
            .list_entities(SYSTEM_VAULT_ID, Some(SystemKeys::REHASH_PROGRESS_PREFIX), None, 1)
            .context(StateSnafu)?;
        Ok(!entities.is_empty())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_types::{Region, UserId};

    use crate::system::{
        service::{SystemError, SystemOrganizationService},
        types::EmailHashEntry,
    };

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        crate::system::create_test_service()
    }

    #[test]
    fn test_register_and_get_email_hash() {
        let svc = create_test_service();
        let hmac = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        svc.register_email_hash(hmac, UserId::new(42)).unwrap();
        let result = svc.get_email_hash(hmac).unwrap();
        assert_eq!(result, Some(EmailHashEntry::Active(UserId::new(42))));
    }

    #[test]
    fn test_get_email_hash_not_found() {
        let svc = create_test_service();
        assert_eq!(svc.get_email_hash("nonexistent_hmac").unwrap(), None);
    }

    #[test]
    fn test_register_email_hash_duplicate_rejected() {
        let svc = create_test_service();
        let hmac = "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222";
        svc.register_email_hash(hmac, UserId::new(1)).unwrap();
        let result = svc.register_email_hash(hmac, UserId::new(2));
        assert!(matches!(result.unwrap_err(), SystemError::AlreadyExists { .. }));
        assert_eq!(svc.get_email_hash(hmac).unwrap(), Some(EmailHashEntry::Active(UserId::new(1))));
    }

    #[test]
    fn test_remove_email_hash() {
        let svc = create_test_service();
        let hmac = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        svc.register_email_hash(hmac, UserId::new(99)).unwrap();
        svc.remove_email_hash(hmac).unwrap();
        assert_eq!(svc.get_email_hash(hmac).unwrap(), None);
    }

    #[test]
    fn test_remove_email_hash_nonexistent_is_noop() {
        let svc = create_test_service();
        svc.remove_email_hash("does_not_exist").unwrap();
    }

    #[test]
    fn test_email_hash_independent_per_hmac() {
        let svc = create_test_service();
        let hmac_a = "aaaa".repeat(16);
        let hmac_b = "bbbb".repeat(16);
        svc.register_email_hash(&hmac_a, UserId::new(1)).unwrap();
        svc.register_email_hash(&hmac_b, UserId::new(2)).unwrap();
        assert_eq!(
            svc.get_email_hash(&hmac_a).unwrap(),
            Some(EmailHashEntry::Active(UserId::new(1)))
        );
        assert_eq!(
            svc.get_email_hash(&hmac_b).unwrap(),
            Some(EmailHashEntry::Active(UserId::new(2)))
        );
    }

    #[test]
    fn test_register_email_hash_then_remove_allows_reregistration() {
        let svc = create_test_service();
        let hmac = "deadbeef".repeat(8);
        svc.register_email_hash(&hmac, UserId::new(1)).unwrap();
        svc.remove_email_hash(&hmac).unwrap();
        svc.register_email_hash(&hmac, UserId::new(2)).unwrap();
        assert_eq!(
            svc.get_email_hash(&hmac).unwrap(),
            Some(EmailHashEntry::Active(UserId::new(2)))
        );
    }

    #[test]
    fn test_blinding_key_version_not_set() {
        let svc = create_test_service();
        assert_eq!(svc.get_blinding_key_version().unwrap(), None);
    }

    #[test]
    fn test_set_and_get_blinding_key_version() {
        let svc = create_test_service();
        svc.set_blinding_key_version(1).unwrap();
        assert_eq!(svc.get_blinding_key_version().unwrap(), Some(1));
        svc.set_blinding_key_version(2).unwrap();
        assert_eq!(svc.get_blinding_key_version().unwrap(), Some(2));
    }

    #[test]
    fn test_rehash_progress_lifecycle() {
        let svc = create_test_service();
        let region = Region::US_EAST_VA;
        assert_eq!(svc.get_rehash_progress(region).unwrap(), None);
        svc.set_rehash_progress(region, 50).unwrap();
        assert_eq!(svc.get_rehash_progress(region).unwrap(), Some(50));
        svc.set_rehash_progress(region, 100).unwrap();
        assert_eq!(svc.get_rehash_progress(region).unwrap(), Some(100));
        svc.clear_rehash_progress(region).unwrap();
        assert_eq!(svc.get_rehash_progress(region).unwrap(), None);
    }

    #[test]
    fn test_rehash_progress_per_region() {
        let svc = create_test_service();
        svc.set_rehash_progress(Region::US_EAST_VA, 10).unwrap();
        svc.set_rehash_progress(Region::IE_EAST_DUBLIN, 20).unwrap();
        assert_eq!(svc.get_rehash_progress(Region::US_EAST_VA).unwrap(), Some(10));
        assert_eq!(svc.get_rehash_progress(Region::IE_EAST_DUBLIN).unwrap(), Some(20));
        assert_eq!(svc.get_rehash_progress(Region::JP_EAST_TOKYO).unwrap(), None);
    }

    #[test]
    fn test_is_rotation_in_progress() {
        let svc = create_test_service();
        assert!(!svc.is_rotation_in_progress().unwrap());
        svc.set_rehash_progress(Region::US_EAST_VA, 0).unwrap();
        assert!(svc.is_rotation_in_progress().unwrap());
        svc.clear_rehash_progress(Region::US_EAST_VA).unwrap();
        assert!(!svc.is_rotation_in_progress().unwrap());
    }

    #[test]
    fn test_is_rotation_in_progress_multiple_regions() {
        let svc = create_test_service();
        svc.set_rehash_progress(Region::US_EAST_VA, 100).unwrap();
        svc.set_rehash_progress(Region::IE_EAST_DUBLIN, 50).unwrap();
        assert!(svc.is_rotation_in_progress().unwrap());
        svc.clear_rehash_progress(Region::US_EAST_VA).unwrap();
        assert!(svc.is_rotation_in_progress().unwrap());
        svc.clear_rehash_progress(Region::IE_EAST_DUBLIN).unwrap();
        assert!(!svc.is_rotation_in_progress().unwrap());
    }
}
