//! User directory operations (GLOBAL control plane).

use chrono::{DateTime, Utc};
use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{Operation, Region, UserId, UserSlug, decode, encode};
use snafu::ResultExt;
use tracing::warn;

use super::{
    CodecSnafu, Result, SYSTEM_VAULT_ID, StateSnafu, SystemError, SystemKeys,
    SystemOrganizationService,
};
use crate::system::types::{UserDirectoryEntry, UserDirectoryStatus};

/// Maximum number of user directory entries returned by `list_user_directory`.
const MAX_LIST_USER_DIRECTORY: usize = 10_000;

impl<B: StorageBackend> SystemOrganizationService<B> {
    /// Registers a user directory entry in the GLOBAL control plane.
    pub fn register_user_directory(&self, entry: &UserDirectoryEntry) -> Result<()> {
        let slug = entry.slug.ok_or_else(|| SystemError::NotFound {
            entity: format!("user_directory:{} has no slug", entry.user),
        })?;
        let key = SystemKeys::user_directory_key(entry.user);
        let value = encode(entry).context(CodecSnafu)?;
        let slug_index_key = SystemKeys::user_slug_index_key(slug);
        let slug_index_value = entry.user.value().to_string().into_bytes();
        let ops = vec![
            Operation::SetEntity { key, value, condition: None, expires_at: None },
            Operation::SetEntity {
                key: slug_index_key,
                value: slug_index_value,
                condition: None,
                expires_at: None,
            },
        ];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Returns a user directory entry by user ID.
    pub fn get_user_directory(&self, user_id: UserId) -> Result<Option<UserDirectoryEntry>> {
        let key = SystemKeys::user_directory_key(user_id);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let entry: UserDirectoryEntry = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(entry))
            },
            None => Ok(None),
        }
    }

    /// Resolves a user slug to a user ID via the GLOBAL slug index.
    pub fn get_user_id_by_slug(&self, slug: UserSlug) -> Result<Option<UserId>> {
        let key = SystemKeys::user_slug_index_key(slug);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        Ok(entity_opt.and_then(|entity| {
            let id_str = String::from_utf8_lossy(&entity.value);
            id_str.parse::<UserId>().ok()
        }))
    }

    /// Returns a user directory entry by slug.
    pub fn get_user_directory_by_slug(&self, slug: UserSlug) -> Result<Option<UserDirectoryEntry>> {
        match self.get_user_id_by_slug(slug)? {
            Some(user_id) => self.get_user_directory(user_id),
            None => Ok(None),
        }
    }

    /// Updates the status of a user directory entry.
    pub fn update_user_directory_status(
        &self,
        user_id: UserId,
        status: UserDirectoryStatus,
        block_timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let mut entry = self
            .get_user_directory(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user_directory:{user_id}") })?;
        if entry.status == UserDirectoryStatus::Deleted {
            return Err(SystemError::AlreadyExists {
                entity: format!("user_directory:{user_id} is deleted (permanent tombstone)"),
            });
        }
        let old_slug = entry.slug;
        entry.status = status;
        if status == UserDirectoryStatus::Deleted {
            entry.slug = None;
            entry.region = None;
            entry.updated_at = None;
        } else {
            entry.updated_at = Some(block_timestamp);
        }
        let key = SystemKeys::user_directory_key(user_id);
        let value = encode(&entry).context(CodecSnafu)?;
        let mut ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        if status == UserDirectoryStatus::Deleted
            && let Some(slug) = old_slug
        {
            ops.push(Operation::DeleteEntity { key: SystemKeys::user_slug_index_key(slug) });
        }
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Updates the region of a user directory entry (for migration).
    pub fn update_user_directory_region(&self, user_id: UserId, region: Region) -> Result<()> {
        let mut entry = self
            .get_user_directory(user_id)?
            .ok_or_else(|| SystemError::NotFound { entity: format!("user_directory:{user_id}") })?;
        if entry.status == UserDirectoryStatus::Deleted {
            return Err(SystemError::AlreadyExists {
                entity: format!("user_directory:{user_id} is deleted (permanent tombstone)"),
            });
        }
        entry.region = Some(region);
        entry.updated_at = Some(Utc::now());
        let key = SystemKeys::user_directory_key(user_id);
        let value = encode(&entry).context(CodecSnafu)?;
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Lists all user directory entries in the GLOBAL control plane.
    pub fn list_user_directory(&self) -> Result<Vec<UserDirectoryEntry>> {
        let entities = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::USER_DIRECTORY_PREFIX),
                None,
                MAX_LIST_USER_DIRECTORY,
            )
            .context(StateSnafu)?;
        let mut entries = Vec::new();
        for entity in entities {
            match decode::<UserDirectoryEntry>(&entity.value) {
                Ok(entry) => entries.push(entry),
                Err(e) => warn!(error = %e, "Skipping corrupt UserDirectoryEntry during list"),
            }
        }
        Ok(entries)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use chrono::Utc;
    use inferadb_ledger_types::{Region, UserId, UserSlug};

    use crate::system::{
        service::SystemOrganizationService,
        types::{UserDirectoryEntry, UserDirectoryStatus},
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

    #[test]
    fn test_register_and_get_user_directory() {
        let svc = create_test_service();
        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();
        let retrieved = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(retrieved.user, UserId::new(1));
        assert_eq!(retrieved.slug, Some(UserSlug::new(10001)));
        assert_eq!(retrieved.region, Some(Region::US_EAST_VA));
        assert_eq!(retrieved.status, UserDirectoryStatus::Active);
    }

    #[test]
    fn test_get_user_directory_not_found() {
        let svc = create_test_service();
        let result = svc.get_user_directory(UserId::new(999)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_user_directory_slug_lookup() {
        let svc = create_test_service();
        let entry = make_directory_entry(1, 10001, Region::IE_EAST_DUBLIN);
        let slug = UserSlug::new(10001);
        svc.register_user_directory(&entry).unwrap();
        let user_id = svc.get_user_id_by_slug(slug).unwrap();
        assert_eq!(user_id, Some(UserId::new(1)));
        let by_slug = svc.get_user_directory_by_slug(slug).unwrap();
        assert!(by_slug.is_some());
        assert_eq!(by_slug.unwrap().region, Some(Region::IE_EAST_DUBLIN));
    }

    #[test]
    fn test_user_directory_slug_not_found() {
        let svc = create_test_service();
        let result = svc.get_user_id_by_slug(UserSlug::new(99999)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_update_user_directory_status_to_migrating() {
        let svc = create_test_service();
        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();
        svc.update_user_directory_status(
            UserId::new(1),
            UserDirectoryStatus::Migrating,
            Utc::now(),
        )
        .unwrap();
        let updated = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(updated.status, UserDirectoryStatus::Migrating);
        assert!(updated.slug.is_some());
        assert!(updated.region.is_some());
    }

    #[test]
    fn test_update_user_directory_status_to_deleted() {
        let svc = create_test_service();
        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();
        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Deleted, Utc::now())
            .unwrap();
        let tombstone = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(tombstone.status, UserDirectoryStatus::Deleted);
        assert_eq!(tombstone.slug, None);
        assert_eq!(tombstone.region, None);
        assert_eq!(tombstone.updated_at, None);
        assert_eq!(tombstone.user, UserId::new(1));
        let slug_lookup = svc.get_user_id_by_slug(UserSlug::new(10001)).unwrap();
        assert!(slug_lookup.is_none());
    }

    #[test]
    fn test_update_user_directory_status_not_found() {
        let svc = create_test_service();
        let result = svc.update_user_directory_status(
            UserId::new(999),
            UserDirectoryStatus::Deleted,
            Utc::now(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_deleted_user_directory_rejects_status_change() {
        let svc = create_test_service();
        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();
        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Deleted, Utc::now())
            .unwrap();
        assert!(
            svc.update_user_directory_status(
                UserId::new(1),
                UserDirectoryStatus::Active,
                Utc::now()
            )
            .is_err()
        );
        assert!(
            svc.update_user_directory_status(
                UserId::new(1),
                UserDirectoryStatus::Migrating,
                Utc::now()
            )
            .is_err()
        );
        assert!(
            svc.update_user_directory_status(
                UserId::new(1),
                UserDirectoryStatus::Deleted,
                Utc::now()
            )
            .is_err()
        );
    }

    #[test]
    fn test_deleted_user_directory_rejects_region_change() {
        let svc = create_test_service();
        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();
        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Deleted, Utc::now())
            .unwrap();
        assert!(svc.update_user_directory_region(UserId::new(1), Region::IE_EAST_DUBLIN).is_err());
    }

    #[test]
    fn test_update_user_directory_region() {
        let svc = create_test_service();
        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();
        svc.update_user_directory_region(UserId::new(1), Region::IE_EAST_DUBLIN).unwrap();
        let updated = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(updated.region, Some(Region::IE_EAST_DUBLIN));
    }

    #[test]
    fn test_update_user_directory_region_not_found() {
        let svc = create_test_service();
        assert!(
            svc.update_user_directory_region(UserId::new(999), Region::IE_EAST_DUBLIN).is_err()
        );
    }

    #[test]
    fn test_list_user_directory() {
        let svc = create_test_service();
        for i in 1..=3 {
            let entry = make_directory_entry(i, 10000 + i as u64, Region::US_EAST_VA);
            svc.register_user_directory(&entry).unwrap();
        }
        let entries = svc.list_user_directory().unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_user_directory_migration_lifecycle() {
        let svc = create_test_service();
        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();
        svc.update_user_directory_status(
            UserId::new(1),
            UserDirectoryStatus::Migrating,
            Utc::now(),
        )
        .unwrap();
        let migrating = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(migrating.status, UserDirectoryStatus::Migrating);
        svc.update_user_directory_region(UserId::new(1), Region::IE_EAST_DUBLIN).unwrap();
        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Active, Utc::now())
            .unwrap();
        let completed = svc.get_user_directory(UserId::new(1)).unwrap().unwrap();
        assert_eq!(completed.status, UserDirectoryStatus::Active);
        assert_eq!(completed.region, Some(Region::IE_EAST_DUBLIN));
        assert_eq!(completed.slug, Some(UserSlug::new(10001)));
    }

    #[test]
    fn test_user_directory_erasure_lifecycle() {
        let svc = create_test_service();
        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();
        svc.update_user_directory_status(UserId::new(1), UserDirectoryStatus::Deleted, Utc::now())
            .unwrap();
        let tombstone = svc.get_user_directory(UserId::new(1)).unwrap();
        assert!(tombstone.is_some());
        let tombstone = tombstone.unwrap();
        assert_eq!(tombstone.status, UserDirectoryStatus::Deleted);
        assert!(svc.get_user_id_by_slug(UserSlug::new(10001)).unwrap().is_none());
        assert!(svc.get_user_directory_by_slug(UserSlug::new(10001)).unwrap().is_none());
    }

    #[test]
    fn test_user_directory_does_not_collide_with_user_records() {
        let svc = create_test_service();
        let entry = make_directory_entry(1, 10001, Region::US_EAST_VA);
        svc.register_user_directory(&entry).unwrap();
        let entries = svc.list_user_directory().unwrap();
        assert_eq!(entries.len(), 1);
        let nodes = svc.list_nodes().unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_multiple_user_directories_different_regions() {
        let svc = create_test_service();
        let regions = [Region::US_EAST_VA, Region::IE_EAST_DUBLIN, Region::JP_EAST_TOKYO];
        for (i, region) in regions.iter().enumerate() {
            let entry = make_directory_entry((i + 1) as i64, 10000 + (i + 1) as u64, *region);
            svc.register_user_directory(&entry).unwrap();
        }
        for (i, _region) in regions.iter().enumerate() {
            let user_id = svc.get_user_id_by_slug(UserSlug::new(10000 + (i + 1) as u64)).unwrap();
            assert_eq!(user_id, Some(UserId::new((i + 1) as i64)));
        }
        for (i, region) in regions.iter().enumerate() {
            let entry = svc.get_user_directory(UserId::new((i + 1) as i64)).unwrap().unwrap();
            assert_eq!(entry.region, Some(*region));
        }
    }
}
