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

/// Result of checking the replicated client sequence table for idempotency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyCheckResult {
    /// The idempotency key matches and the request hash matches.
    /// The request was already committed with the given sequence number.
    AlreadyCommitted { sequence: u64 },
    /// The idempotency key matches but the request hash differs.
    /// A different request was already committed with this key.
    KeyReused,
    /// No entry found for this client, or the stored idempotency key
    /// does not match. Proceed with the new proposal.
    Miss,
}

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
            .map_or(0, |entry| entry.sequence)
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

    /// Checks the replicated client sequence table for idempotency.
    ///
    /// This is the cross-failover deduplication path: when the node-local
    /// moka cache misses (e.g. after leader failover), this method checks
    /// the Raft-replicated `ClientSequenceEntry` for a matching idempotency key.
    ///
    /// Returns [`IdempotencyCheckResult::AlreadyCommitted`] if the key and hash
    /// match, [`IdempotencyCheckResult::KeyReused`] if the key matches but the
    /// hash differs, or [`IdempotencyCheckResult::Miss`] if no entry exists.
    pub fn client_idempotency_check(
        &self,
        organization_id: OrganizationId,
        vault_id: VaultId,
        client_id: &str,
        idempotency_key: &[u8; 16],
        request_hash: u64,
    ) -> IdempotencyCheckResult {
        // Zero key means the client didn't provide an idempotency key
        if *idempotency_key == [0u8; 16] {
            return IdempotencyCheckResult::Miss;
        }

        let key = (organization_id, vault_id, client_id.to_string());
        let state = self.state.read();

        match state.client_sequences.get(&key) {
            Some(entry) if entry.last_idempotency_key == *idempotency_key => {
                if entry.last_request_hash == request_hash {
                    IdempotencyCheckResult::AlreadyCommitted { sequence: entry.sequence }
                } else {
                    IdempotencyCheckResult::KeyReused
                }
            },
            _ => IdempotencyCheckResult::Miss,
        }
    }

    /// Creates an accessor from a pre-built state (for testing).
    #[cfg(test)]
    pub fn new_for_test(state: Arc<RwLock<AppliedState>>) -> Self {
        Self { state }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use inferadb_ledger_types::{OrganizationId, VaultId};
    use parking_lot::RwLock;

    use super::*;
    use crate::log_storage::types::ClientSequenceEntry;

    fn make_accessor_with_entry(
        org: OrganizationId,
        vault: VaultId,
        client_id: &str,
        entry: ClientSequenceEntry,
    ) -> AppliedStateAccessor {
        let mut state = AppliedState::default();
        state.client_sequences.insert((org, vault, client_id.to_string()), entry);
        AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)))
    }

    #[test]
    fn idempotency_check_already_committed() {
        let key = [1u8; 16];
        let entry = ClientSequenceEntry {
            sequence: 42,
            last_seen: 1000,
            last_idempotency_key: key,
            last_request_hash: 12345,
        };
        let accessor =
            make_accessor_with_entry(OrganizationId::new(1), VaultId::new(1), "client-1", entry);

        let result = accessor.client_idempotency_check(
            OrganizationId::new(1),
            VaultId::new(1),
            "client-1",
            &key,
            12345,
        );
        assert_eq!(result, IdempotencyCheckResult::AlreadyCommitted { sequence: 42 });
    }

    #[test]
    fn idempotency_check_key_reused() {
        let key = [2u8; 16];
        let entry = ClientSequenceEntry {
            sequence: 10,
            last_seen: 1000,
            last_idempotency_key: key,
            last_request_hash: 111,
        };
        let accessor =
            make_accessor_with_entry(OrganizationId::new(1), VaultId::new(1), "client-1", entry);

        // Same key, different hash
        let result = accessor.client_idempotency_check(
            OrganizationId::new(1),
            VaultId::new(1),
            "client-1",
            &key,
            999,
        );
        assert_eq!(result, IdempotencyCheckResult::KeyReused);
    }

    #[test]
    fn idempotency_check_miss_no_entry() {
        let accessor =
            AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(AppliedState::default())));

        let result = accessor.client_idempotency_check(
            OrganizationId::new(1),
            VaultId::new(1),
            "client-1",
            &[3u8; 16],
            42,
        );
        assert_eq!(result, IdempotencyCheckResult::Miss);
    }

    #[test]
    fn idempotency_check_miss_different_key() {
        let stored_key = [4u8; 16];
        let check_key = [5u8; 16];
        let entry = ClientSequenceEntry {
            sequence: 10,
            last_seen: 1000,
            last_idempotency_key: stored_key,
            last_request_hash: 42,
        };
        let accessor =
            make_accessor_with_entry(OrganizationId::new(1), VaultId::new(1), "client-1", entry);

        // Different idempotency key — treated as new request
        let result = accessor.client_idempotency_check(
            OrganizationId::new(1),
            VaultId::new(1),
            "client-1",
            &check_key,
            42,
        );
        assert_eq!(result, IdempotencyCheckResult::Miss);
    }

    #[test]
    fn idempotency_check_zero_key_always_miss() {
        let zero_key = [0u8; 16];
        let entry = ClientSequenceEntry {
            sequence: 10,
            last_seen: 1000,
            last_idempotency_key: zero_key,
            last_request_hash: 42,
        };
        let accessor =
            make_accessor_with_entry(OrganizationId::new(1), VaultId::new(1), "client-1", entry);

        // Zero key means client didn't provide idempotency — always miss
        let result = accessor.client_idempotency_check(
            OrganizationId::new(1),
            VaultId::new(1),
            "client-1",
            &zero_key,
            42,
        );
        assert_eq!(result, IdempotencyCheckResult::Miss);
    }

    #[test]
    fn idempotency_check_post_eviction_is_miss() {
        // After TTL eviction removes a ClientSequenceEntry, a retry with
        // the same idempotency key is treated as a new request (Miss).
        let accessor =
            AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(AppliedState::default())));

        // Entry was evicted — no entry in state
        let result = accessor.client_idempotency_check(
            OrganizationId::new(1),
            VaultId::new(1),
            "client-1",
            &[6u8; 16],
            12345,
        );
        assert_eq!(result, IdempotencyCheckResult::Miss);
    }

    #[test]
    fn idempotency_check_different_vault_is_miss() {
        let key = [7u8; 16];
        let entry = ClientSequenceEntry {
            sequence: 42,
            last_seen: 1000,
            last_idempotency_key: key,
            last_request_hash: 12345,
        };
        let accessor =
            make_accessor_with_entry(OrganizationId::new(1), VaultId::new(1), "client-1", entry);

        // Same key, same hash, but different vault
        let result = accessor.client_idempotency_check(
            OrganizationId::new(1),
            VaultId::new(2),
            "client-1",
            &key,
            12345,
        );
        assert_eq!(result, IdempotencyCheckResult::Miss);
    }
}
