//! Shared read handle for Raft applied state.
//!
//! Provides concurrent read access to the state machine's applied state
//! without holding the write lock.

use std::{collections::HashMap, sync::Arc};

use inferadb_ledger_types::{
    OrganizationId, OrganizationSlug, OrganizationUsage, VaultId, VaultSlug,
};
use parking_lot::RwLock;

use super::types::{AppliedState, OrganizationMeta, VaultHealthStatus, VaultMeta};

/// Provides shared read access to the Raft state machine's applied state.
///
/// Services use this to query vault heights and health status
/// without holding a direct reference to the Raft log store.
#[derive(Clone)]
pub struct AppliedStateAccessor {
    pub(super) state: Arc<RwLock<AppliedState>>,
}

impl AppliedStateAccessor {
    /// Returns the current height for a vault.
    pub fn vault_height(&self, organization_id: OrganizationId, vault_id: VaultId) -> u64 {
        self.state.read().vault_heights.get(&(organization_id, vault_id)).copied().unwrap_or(0)
    }

    /// Returns the health status for a vault.
    pub fn vault_health(
        &self,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> VaultHealthStatus {
        self.state
            .read()
            .vault_health
            .get(&(organization_id, vault_id))
            .cloned()
            .unwrap_or_default()
    }

    /// Returns all vault heights (for GetTip when no specific vault is requested).
    pub fn all_vault_heights(&self) -> HashMap<(OrganizationId, VaultId), u64> {
        self.state.read().vault_heights.clone()
    }

    /// Returns organization metadata by ID.
    pub fn get_organization(&self, organization_id: OrganizationId) -> Option<OrganizationMeta> {
        use inferadb_ledger_state::system::OrganizationStatus;
        self.state
            .read()
            .organizations
            .get(&organization_id)
            .filter(|ns| ns.status != OrganizationStatus::Deleted)
            .cloned()
    }

    /// Lists all active organizations.
    pub fn list_organizations(&self) -> Vec<OrganizationMeta> {
        use inferadb_ledger_state::system::OrganizationStatus;
        self.state
            .read()
            .organizations
            .values()
            .filter(|ns| ns.status != OrganizationStatus::Deleted)
            .cloned()
            .collect()
    }

    /// Returns vault metadata by ID.
    pub fn get_vault(
        &self,
        organization_id: OrganizationId,
        vault_id: VaultId,
    ) -> Option<VaultMeta> {
        self.state.read().vaults.get(&(organization_id, vault_id)).filter(|v| !v.deleted).cloned()
    }

    /// Lists all active vaults in an organization.
    pub fn list_vaults(&self, organization_id: OrganizationId) -> Vec<VaultMeta> {
        self.state
            .read()
            .vaults
            .values()
            .filter(|v| v.organization_id == organization_id && !v.deleted)
            .cloned()
            .collect()
    }

    /// Returns the last committed sequence for a client.
    ///
    /// Returns 0 if no sequence has been committed for this client.
    pub fn client_sequence(
        &self,
        organization_id: OrganizationId,
        vault_id: VaultId,
        client_id: &str,
    ) -> u64 {
        self.state
            .read()
            .client_sequences
            .get(&(organization_id, vault_id, client_id.to_string()))
            .copied()
            .unwrap_or(0)
    }

    /// Returns the current shard height (for snapshot info).
    pub fn shard_height(&self) -> u64 {
        self.state.read().shard_height
    }

    /// Returns all vault metadata (for retention policy checks).
    pub fn all_vaults(&self) -> HashMap<(OrganizationId, VaultId), VaultMeta> {
        self.state
            .read()
            .vaults
            .iter()
            .filter(|(_, v)| !v.deleted)
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Returns the number of active (non-deleted) vaults in an organization.
    pub fn vault_count(&self, organization_id: OrganizationId) -> u32 {
        let count = self
            .state
            .read()
            .vaults
            .values()
            .filter(|v| v.organization_id == organization_id && !v.deleted)
            .count();
        // Safe: vault count is bounded by u32::MAX in practice
        #[allow(clippy::cast_possible_truncation)]
        {
            count as u32
        }
    }

    /// Returns cumulative estimated storage bytes for an organization.
    ///
    /// Returns 0 if no data has been written to the organization.
    pub fn organization_storage_bytes(&self, organization_id: OrganizationId) -> u64 {
        self.state.read().organization_storage_bytes.get(&organization_id).copied().unwrap_or(0)
    }

    /// Returns a snapshot of resource usage for an organization.
    ///
    /// Combines `storage_bytes` and `vault_count` into a single struct
    /// for quota enforcement and capacity-planning APIs.
    pub fn organization_usage(&self, organization_id: OrganizationId) -> OrganizationUsage {
        let state = self.state.read();
        let storage_bytes =
            state.organization_storage_bytes.get(&organization_id).copied().unwrap_or(0);
        let vault_count = state
            .vaults
            .values()
            .filter(|v| v.organization_id == organization_id && !v.deleted)
            .count();
        #[allow(clippy::cast_possible_truncation)]
        OrganizationUsage { storage_bytes, vault_count: vault_count as u32 }
    }

    /// Returns the organization quota (per-organization override or None for server default).
    pub fn organization_quota(
        &self,
        organization_id: OrganizationId,
    ) -> Option<inferadb_ledger_types::config::OrganizationQuota> {
        self.state.read().organizations.get(&organization_id).and_then(|ns| ns.quota.clone())
    }

    /// Resolves an external organization slug to its internal ID.
    ///
    /// Returns `None` if the slug is not registered in the slug index.
    pub fn resolve_slug_to_id(&self, slug: OrganizationSlug) -> Option<OrganizationId> {
        self.state.read().slug_index.get(&slug).copied()
    }

    /// Resolves an internal organization ID to its external slug.
    ///
    /// Returns `None` if the ID is not registered in the reverse index.
    pub fn resolve_id_to_slug(&self, id: OrganizationId) -> Option<OrganizationSlug> {
        self.state.read().id_to_slug.get(&id).copied()
    }

    /// Resolves an external vault slug to its internal ID.
    ///
    /// Returns `None` if the slug is not registered in the vault slug index.
    pub fn resolve_vault_slug_to_id(&self, slug: VaultSlug) -> Option<VaultId> {
        self.state.read().vault_slug_index.get(&slug).copied()
    }

    /// Resolves an internal vault ID to its external slug.
    ///
    /// Returns `None` if the ID is not registered in the vault reverse index.
    pub fn resolve_vault_id_to_slug(&self, id: VaultId) -> Option<VaultSlug> {
        self.state.read().vault_id_to_slug.get(&id).copied()
    }

    /// Creates an accessor from a pre-built state (for testing).
    #[cfg(test)]
    pub fn new_for_test(state: Arc<RwLock<AppliedState>>) -> Self {
        Self { state }
    }
}
