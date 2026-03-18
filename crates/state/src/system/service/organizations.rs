//! Organization registry and shard routing operations.

use inferadb_ledger_store::StorageBackend;
use inferadb_ledger_types::{
    NodeId, Operation, OrganizationId, OrganizationSlug, Region, SigningKeyScope, decode, encode,
};
use snafu::ResultExt;
use tracing::warn;

use super::{
    CodecSnafu, Result, SYSTEM_VAULT_ID, StateSnafu, SystemError, SystemKeys,
    SystemOrganizationService,
};
use crate::system::types::{OrganizationRegistry, OrganizationStatus};

/// Maximum number of organizations returned by `list_organizations`.
const MAX_LIST_ORGANIZATIONS: usize = 10_000;

impl<B: StorageBackend> SystemOrganizationService<B> {
    /// Registers a new organization.
    pub fn register_organization(
        &self,
        registry: &OrganizationRegistry,
        slug: OrganizationSlug,
    ) -> Result<()> {
        let key = SystemKeys::organization_registry_key(registry.organization_id);
        let value = encode(registry).context(CodecSnafu)?;
        let slug_index_key = SystemKeys::organization_slug_key(slug);
        let slug_index_value = registry.organization_id.value().to_string().into_bytes();
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

    /// Returns an organization by ID.
    pub fn get_organization(
        &self,
        organization: OrganizationId,
    ) -> Result<Option<OrganizationRegistry>> {
        let key = SystemKeys::organization_registry_key(organization);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, key.as_bytes()).context(StateSnafu)?;
        match entity_opt {
            Some(entity) => {
                let registry: OrganizationRegistry = decode(&entity.value).context(CodecSnafu)?;
                Ok(Some(registry))
            },
            None => Ok(None),
        }
    }

    /// Returns an organization by slug.
    pub fn get_organization_by_slug(
        &self,
        slug: OrganizationSlug,
    ) -> Result<Option<OrganizationRegistry>> {
        let index_key = SystemKeys::organization_slug_key(slug);
        let entity_opt =
            self.state.get_entity(SYSTEM_VAULT_ID, index_key.as_bytes()).context(StateSnafu)?;
        let organization_id = match entity_opt {
            Some(entity) => {
                let id_str = String::from_utf8_lossy(&entity.value);
                id_str.parse::<OrganizationId>().ok()
            },
            None => None,
        };
        match organization_id {
            Some(id) => self.get_organization(id),
            None => Ok(None),
        }
    }

    /// Lists all organizations.
    pub fn list_organizations(&self) -> Result<Vec<OrganizationRegistry>> {
        let entities = self
            .state
            .list_entities(
                SYSTEM_VAULT_ID,
                Some(SystemKeys::ORG_REGISTRY_PREFIX),
                None,
                MAX_LIST_ORGANIZATIONS,
            )
            .context(StateSnafu)?;
        let mut organizations = Vec::new();
        for entity in entities {
            match decode::<OrganizationRegistry>(&entity.value) {
                Ok(registry) => organizations.push(registry),
                Err(e) => {
                    warn!(error = %e, "Skipping corrupt OrganizationRegistry entry during list")
                },
            }
        }
        Ok(organizations)
    }

    /// Updates organization status.
    pub fn update_organization_status(
        &self,
        organization: OrganizationId,
        slug: OrganizationSlug,
        status: OrganizationStatus,
    ) -> Result<()> {
        let mut registry = self.get_organization(organization)?.ok_or_else(|| {
            SystemError::NotFound { entity: format!("organization:{}", organization) }
        })?;
        registry.status = status;
        registry.config_version += 1;
        self.register_organization(&registry, slug)
    }

    /// Returns the region for an organization.
    pub fn get_region_for_organization(
        &self,
        organization: OrganizationId,
    ) -> Result<Option<Region>> {
        self.get_organization(organization).map(|opt| opt.map(|r| r.region))
    }

    /// Resolves a [`SigningKeyScope`] to the [`Region`] whose RMK protects it.
    pub fn resolve_scope_region(&self, scope: &SigningKeyScope) -> Result<Region> {
        match scope {
            SigningKeyScope::Global => Ok(Region::GLOBAL),
            SigningKeyScope::Organization(org_id) => self
                .get_region_for_organization(*org_id)?
                .ok_or_else(|| SystemError::NotFound { entity: format!("Organization {org_id}") }),
        }
    }

    /// Assigns an organization to a region.
    pub fn assign_organization_to_region(
        &self,
        organization: OrganizationId,
        slug: OrganizationSlug,
        region: Region,
        member_nodes: Vec<NodeId>,
    ) -> Result<()> {
        let mut registry = self.get_organization(organization)?.ok_or_else(|| {
            SystemError::NotFound { entity: format!("organization:{}", organization) }
        })?;
        registry.region = region;
        registry.member_nodes = member_nodes;
        registry.config_version += 1;
        self.register_organization(&registry, slug)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use chrono::Utc;
    use inferadb_ledger_types::{
        NodeId, OrganizationId, OrganizationSlug, Region, VaultId, VaultSlug,
    };

    use crate::system::{
        service::SystemOrganizationService,
        types::{OrganizationRegistry, OrganizationStatus},
    };

    fn create_test_service() -> SystemOrganizationService<inferadb_ledger_store::InMemoryBackend> {
        crate::system::create_test_service()
    }

    #[test]
    fn test_next_organization_id() {
        let svc = create_test_service();
        let id1 = svc.next_organization_id().unwrap();
        let id2 = svc.next_organization_id().unwrap();
        let id3 = svc.next_organization_id().unwrap();
        assert_eq!(id1, OrganizationId::new(1));
        assert_eq!(id2, OrganizationId::new(2));
        assert_eq!(id3, OrganizationId::new(3));
    }

    #[test]
    fn test_register_and_get_organization() {
        let svc = create_test_service();
        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(1),
            region: Region::GLOBAL,
            member_nodes: vec![NodeId::new("node-1"), NodeId::new("node-2")],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
            deleted_at: None,
        };
        let slug = OrganizationSlug::new(9999);
        svc.register_organization(&registry, slug).unwrap();
        let retrieved = svc.get_organization(OrganizationId::new(1)).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().organization_id, OrganizationId::new(1));
        let by_slug = svc.get_organization_by_slug(slug).unwrap();
        assert!(by_slug.is_some());
        assert_eq!(by_slug.unwrap().organization_id, OrganizationId::new(1));
    }

    #[test]
    fn test_list_organizations() {
        let svc = create_test_service();
        for i in 1..=3 {
            let registry = OrganizationRegistry {
                organization_id: OrganizationId::new(i),
                region: Region::GLOBAL,
                member_nodes: vec![],
                status: OrganizationStatus::Active,
                config_version: 1,
                created_at: Utc::now(),
                deleted_at: None,
            };
            svc.register_organization(&registry, OrganizationSlug::new(1000 + i as u64)).unwrap();
        }
        let organizations = svc.list_organizations().unwrap();
        assert_eq!(organizations.len(), 3);
    }

    #[test]
    fn test_update_organization_status() {
        let svc = create_test_service();
        let slug = OrganizationSlug::new(5555);
        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(1),
            region: Region::GLOBAL,
            member_nodes: vec![],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
            deleted_at: None,
        };
        svc.register_organization(&registry, slug).unwrap();
        svc.update_organization_status(OrganizationId::new(1), slug, OrganizationStatus::Suspended)
            .unwrap();
        let updated = svc.get_organization(OrganizationId::new(1)).unwrap().unwrap();
        assert_eq!(updated.status, OrganizationStatus::Suspended);
        assert_eq!(updated.config_version, 2);
    }

    #[test]
    fn test_vault_and_org_slugs_independent() {
        let svc = create_test_service();
        let organization = OrganizationSlug::new(42);
        let vault = VaultSlug::new(42);
        let vault_id = VaultId::new(7);
        let registry = OrganizationRegistry {
            organization_id: OrganizationId::new(1),
            region: Region::GLOBAL,
            member_nodes: vec![],
            status: OrganizationStatus::Active,
            config_version: 1,
            created_at: Utc::now(),
            deleted_at: None,
        };
        svc.register_organization(&registry, organization).unwrap();
        svc.register_vault_slug(vault, vault_id).unwrap();
        let org = svc.get_organization_by_slug(organization).unwrap();
        assert!(org.is_some());
        assert_eq!(org.unwrap().organization_id, OrganizationId::new(1));
        let resolved = svc.get_vault_id_by_slug(vault).unwrap();
        assert_eq!(resolved, Some(vault_id));
    }
}
