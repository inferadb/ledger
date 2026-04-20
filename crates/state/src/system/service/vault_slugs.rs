//! Vault slug index operations.

use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{Operation, VaultId, VaultSlug};
use snafu::ResultExt;

use super::{Result, SYSTEM_VAULT_ID, StateSnafu, SystemKeys, SystemOrganizationService};

impl<B: StorageBackend> SystemOrganizationService<B> {
    /// Registers a vault slug → internal ID mapping.
    pub fn register_vault_slug(&self, slug: VaultSlug, vault: VaultId) -> Result<()> {
        let key = SystemKeys::vault_slug_key(slug);
        let value = vault.value().to_string().into_bytes();
        let ops = vec![Operation::SetEntity { key, value, condition: None, expires_at: None }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Removes a vault slug mapping.
    pub fn remove_vault_slug(&self, slug: VaultSlug) -> Result<()> {
        let key = SystemKeys::vault_slug_key(slug);
        let ops = vec![Operation::DeleteEntity { key }];
        self.state.apply_operations_lazy(SYSTEM_VAULT_ID, &ops, 0).context(StateSnafu)?;
        Ok(())
    }

    /// Looks up the internal vault ID for a given vault slug.
    pub fn get_vault_id_by_slug(&self, slug: VaultSlug) -> Result<Option<VaultId>> {
        let key = SystemKeys::vault_slug_key(slug);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        Ok(entity_opt.and_then(|entity| {
            let id_str = String::from_utf8_lossy(&entity.value);
            id_str.parse::<VaultId>().ok()
        }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_types::{VaultId, VaultSlug};

    use crate::system::service::SystemOrganizationService;

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        crate::system::create_test_service()
    }

    #[test]
    fn test_register_and_get_vault_slug() {
        let svc = create_test_service();
        let slug = VaultSlug::new(12345);
        let vault_id = VaultId::new(1);
        svc.register_vault_slug(slug, vault_id).unwrap();
        let result = svc.get_vault_id_by_slug(slug).unwrap();
        assert_eq!(result, Some(vault_id));
    }

    #[test]
    fn test_get_vault_slug_not_found() {
        let svc = create_test_service();
        let result = svc.get_vault_id_by_slug(VaultSlug::new(99999)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_remove_vault_slug() {
        let svc = create_test_service();
        let slug = VaultSlug::new(11111);
        let vault_id = VaultId::new(5);
        svc.register_vault_slug(slug, vault_id).unwrap();
        assert!(svc.get_vault_id_by_slug(slug).unwrap().is_some());
        svc.remove_vault_slug(slug).unwrap();
        assert_eq!(svc.get_vault_id_by_slug(slug).unwrap(), None);
    }

    #[test]
    fn test_multiple_vault_slugs() {
        let svc = create_test_service();
        let slug1 = VaultSlug::new(100);
        let slug2 = VaultSlug::new(200);
        let vault1 = VaultId::new(1);
        let vault2 = VaultId::new(2);
        svc.register_vault_slug(slug1, vault1).unwrap();
        svc.register_vault_slug(slug2, vault2).unwrap();
        assert_eq!(svc.get_vault_id_by_slug(slug1).unwrap(), Some(vault1));
        assert_eq!(svc.get_vault_id_by_slug(slug2).unwrap(), Some(vault2));
    }
}
