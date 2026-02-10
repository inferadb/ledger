use std::{collections::HashMap, sync::Arc};

use inferadb_ledger_types::{NamespaceId, NamespaceUsage, VaultId};
use parking_lot::RwLock;

use super::types::{AppliedState, NamespaceMeta, VaultHealthStatus, VaultMeta};

/// Shared accessor for applied state.
///
/// This allows services to read vault heights and health status
/// without needing direct access to the Raft storage.
#[derive(Clone)]
pub struct AppliedStateAccessor {
    pub(super) state: Arc<RwLock<AppliedState>>,
}

impl AppliedStateAccessor {
    /// Get the current height for a vault.
    pub fn vault_height(&self, namespace_id: NamespaceId, vault_id: VaultId) -> u64 {
        self.state.read().vault_heights.get(&(namespace_id, vault_id)).copied().unwrap_or(0)
    }

    /// Get the health status for a vault.
    pub fn vault_health(&self, namespace_id: NamespaceId, vault_id: VaultId) -> VaultHealthStatus {
        self.state.read().vault_health.get(&(namespace_id, vault_id)).cloned().unwrap_or_default()
    }

    /// Get all vault heights (for GetTip when no specific vault is requested).
    pub fn all_vault_heights(&self) -> HashMap<(NamespaceId, VaultId), u64> {
        self.state.read().vault_heights.clone()
    }

    /// Get namespace metadata by ID.
    pub fn get_namespace(&self, namespace_id: NamespaceId) -> Option<NamespaceMeta> {
        use inferadb_ledger_state::system::NamespaceStatus;
        self.state
            .read()
            .namespaces
            .get(&namespace_id)
            .filter(|ns| ns.status != NamespaceStatus::Deleted)
            .cloned()
    }

    /// Get namespace metadata by name.
    pub fn get_namespace_by_name(&self, name: &str) -> Option<NamespaceMeta> {
        use inferadb_ledger_state::system::NamespaceStatus;
        self.state
            .read()
            .namespaces
            .values()
            .find(|ns| ns.status != NamespaceStatus::Deleted && ns.name == name)
            .cloned()
    }

    /// List all active namespaces.
    pub fn list_namespaces(&self) -> Vec<NamespaceMeta> {
        use inferadb_ledger_state::system::NamespaceStatus;
        self.state
            .read()
            .namespaces
            .values()
            .filter(|ns| ns.status != NamespaceStatus::Deleted)
            .cloned()
            .collect()
    }

    /// Get vault metadata by ID.
    pub fn get_vault(&self, namespace_id: NamespaceId, vault_id: VaultId) -> Option<VaultMeta> {
        self.state.read().vaults.get(&(namespace_id, vault_id)).filter(|v| !v.deleted).cloned()
    }

    /// List all active vaults in a namespace.
    pub fn list_vaults(&self, namespace_id: NamespaceId) -> Vec<VaultMeta> {
        self.state
            .read()
            .vaults
            .values()
            .filter(|v| v.namespace_id == namespace_id && !v.deleted)
            .cloned()
            .collect()
    }

    /// Get the last committed sequence for a client.
    ///
    /// Returns 0 if no sequence has been committed for this client.
    pub fn client_sequence(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        client_id: &str,
    ) -> u64 {
        self.state
            .read()
            .client_sequences
            .get(&(namespace_id, vault_id, client_id.to_string()))
            .copied()
            .unwrap_or(0)
    }

    /// Get the current shard height (for snapshot info).
    pub fn shard_height(&self) -> u64 {
        self.state.read().shard_height
    }

    /// Get all vault metadata (for retention policy checks).
    pub fn all_vaults(&self) -> HashMap<(NamespaceId, VaultId), VaultMeta> {
        self.state
            .read()
            .vaults
            .iter()
            .filter(|(_, v)| !v.deleted)
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Get the number of active (non-deleted) vaults in a namespace.
    pub fn vault_count(&self, namespace_id: NamespaceId) -> u32 {
        let count = self
            .state
            .read()
            .vaults
            .values()
            .filter(|v| v.namespace_id == namespace_id && !v.deleted)
            .count();
        // Safe: vault count is bounded by u32::MAX in practice
        #[allow(clippy::cast_possible_truncation)]
        {
            count as u32
        }
    }

    /// Get cumulative estimated storage bytes for a namespace.
    ///
    /// Returns 0 if no data has been written to the namespace.
    pub fn namespace_storage_bytes(&self, namespace_id: NamespaceId) -> u64 {
        self.state.read().namespace_storage_bytes.get(&namespace_id).copied().unwrap_or(0)
    }

    /// Get a snapshot of resource usage for a namespace.
    ///
    /// Combines `storage_bytes` and `vault_count` into a single struct
    /// for quota enforcement and capacity-planning APIs.
    pub fn namespace_usage(&self, namespace_id: NamespaceId) -> NamespaceUsage {
        let state = self.state.read();
        let storage_bytes = state.namespace_storage_bytes.get(&namespace_id).copied().unwrap_or(0);
        let vault_count =
            state.vaults.values().filter(|v| v.namespace_id == namespace_id && !v.deleted).count();
        #[allow(clippy::cast_possible_truncation)]
        NamespaceUsage { storage_bytes, vault_count: vault_count as u32 }
    }

    /// Get the namespace quota (per-namespace override or None for server default).
    pub fn namespace_quota(
        &self,
        namespace_id: NamespaceId,
    ) -> Option<inferadb_ledger_types::config::NamespaceQuota> {
        self.state.read().namespaces.get(&namespace_id).and_then(|ns| ns.quota.clone())
    }

    /// Create an accessor from a pre-built state (for testing).
    #[cfg(test)]
    pub fn new_for_test(state: Arc<RwLock<AppliedState>>) -> Self {
        Self { state }
    }
}
