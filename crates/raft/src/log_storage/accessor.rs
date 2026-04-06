//! Shared read handle for Raft applied state.
//!
//! Provides concurrent read access to the state machine's applied state
//! without holding the write lock.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use inferadb_ledger_types::{
    AppId, AppSlug, ClientId, OrganizationId, OrganizationSlug, OrganizationUsage, TeamId,
    TeamSlug, UserId, UserSlug, VaultId, VaultSlug,
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
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    /// * `vault` - Internal vault identifier (`VaultId`).
    pub fn vault_height(&self, organization: OrganizationId, vault: VaultId) -> u64 {
        self.state.read().vault_heights.get(&(organization, vault)).copied().unwrap_or(0)
    }

    /// Returns the health status for a vault.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    /// * `vault` - Internal vault identifier (`VaultId`).
    pub fn vault_health(&self, organization: OrganizationId, vault: VaultId) -> VaultHealthStatus {
        self.state.read().vault_health.get(&(organization, vault)).cloned().unwrap_or_default()
    }

    /// Calls `f` for each `(organization, vault, height)` triple under a single read lock.
    pub fn for_each_vault_height(&self, mut f: impl FnMut(OrganizationId, VaultId, u64)) {
        let state = self.state.read();
        for (&(org, vault), &height) in &state.vault_heights {
            f(org, vault, height);
        }
    }

    /// Returns vault heights filtered to a single organization.
    pub fn org_vault_heights(&self, organization: OrganizationId) -> Vec<(VaultId, u64)> {
        let state = self.state.read();
        state
            .vault_heights
            .iter()
            .filter(|&(&(org, _), _)| org == organization)
            .map(|(&(_, vault), &height)| (vault, height))
            .collect()
    }

    /// Returns the maximum vault height across all vaults.
    pub fn max_vault_height(&self) -> u64 {
        self.state.read().vault_heights.values().copied().max().unwrap_or(0)
    }

    /// Returns the maximum vault height within a specific organization.
    pub fn org_max_vault_height(&self, organization: OrganizationId) -> u64 {
        let state = self.state.read();
        state
            .vault_heights
            .iter()
            .filter(|&(&(org, _), _)| org == organization)
            .map(|(_, &h)| h)
            .max()
            .unwrap_or(0)
    }

    /// Returns the total number of tracked vault heights.
    pub fn vault_height_count(&self) -> usize {
        self.state.read().vault_heights.len()
    }

    /// Returns organization metadata by internal ID.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn get_organization(&self, organization: OrganizationId) -> Option<OrganizationMeta> {
        use inferadb_ledger_state::system::OrganizationStatus;
        self.state
            .read()
            .organizations
            .get(&organization)
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

    /// Returns the set of organization IDs a user belongs to.
    ///
    /// Uses the `user_org_index` for O(1) lookup instead of scanning
    /// all organization profiles.
    pub fn user_organization_ids(&self, user: UserId) -> HashSet<OrganizationId> {
        self.state.read().user_org_index.get(&user).cloned().unwrap_or_default()
    }

    /// Returns vault metadata by internal ID.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    /// * `vault` - Internal vault identifier (`VaultId`).
    pub fn get_vault(&self, organization: OrganizationId, vault: VaultId) -> Option<VaultMeta> {
        self.state.read().vaults.get(&(organization, vault)).filter(|v| !v.deleted).cloned()
    }

    /// Lists all active vaults in an organization.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn list_vaults(&self, organization: OrganizationId) -> Vec<VaultMeta> {
        self.state
            .read()
            .vaults
            .values()
            .filter(|v| v.organization == organization && !v.deleted)
            .cloned()
            .collect()
    }

    /// Returns the last committed sequence for a client.
    ///
    /// Returns 0 if no sequence has been committed for this client.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    /// * `vault` - Internal vault identifier (`VaultId`).
    pub fn client_sequence(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        client_id: &str,
    ) -> u64 {
        self.state
            .read()
            .client_sequences
            .get(&(organization, vault, ClientId::new(client_id)))
            .map_or(0, |entry| entry.sequence)
    }

    /// Returns the current region height (for snapshot info).
    pub fn region_height(&self) -> u64 {
        self.state.read().region_height
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
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn vault_count(&self, organization: OrganizationId) -> u32 {
        let count = self
            .state
            .read()
            .vaults
            .values()
            .filter(|v| v.organization == organization && !v.deleted)
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
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn organization_storage_bytes(&self, organization: OrganizationId) -> u64 {
        self.state.read().organization_storage_bytes.get(&organization).copied().unwrap_or(0)
    }

    /// Returns a snapshot of resource usage for an organization.
    ///
    /// Combines `storage_bytes` and `vault_count` into a single struct
    /// for quota enforcement and capacity-planning APIs.
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    pub fn organization_usage(&self, organization: OrganizationId) -> OrganizationUsage {
        let state = self.state.read();
        let storage_bytes =
            state.organization_storage_bytes.get(&organization).copied().unwrap_or(0);
        let vault_count =
            state.vaults.values().filter(|v| v.organization == organization && !v.deleted).count();
        #[allow(clippy::cast_possible_truncation)]
        OrganizationUsage { storage_bytes, vault_count: vault_count as u32 }
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

    /// Resolves an external user slug to the internal user ID.
    ///
    /// Returns `None` if the slug is not registered in the user slug index.
    pub fn resolve_user_slug_to_id(&self, slug: UserSlug) -> Option<UserId> {
        self.state.read().user_slug_index.get(&slug).copied()
    }

    /// Resolves an internal user ID to its external slug.
    ///
    /// Returns `None` if the ID is not registered in the user reverse index.
    pub fn resolve_user_id_to_slug(&self, id: UserId) -> Option<UserSlug> {
        self.state.read().user_id_to_slug.get(&id).copied()
    }

    // --- Team slug resolution ---

    /// Resolves an external team slug to its internal (organization, team) IDs.
    pub fn resolve_team_slug(&self, slug: TeamSlug) -> Option<(OrganizationId, TeamId)> {
        self.state.read().team_slug_index.get(&slug).copied()
    }

    /// Resolves an internal team ID to its external slug.
    pub fn resolve_team_id_to_slug(&self, id: TeamId) -> Option<TeamSlug> {
        self.state.read().team_id_to_slug.get(&id).copied()
    }

    // --- App slug resolution ---

    /// Resolves an external app slug to its internal (organization, app) IDs.
    pub fn resolve_app_slug(&self, slug: AppSlug) -> Option<(OrganizationId, AppId)> {
        self.state.read().app_slug_index.get(&slug).copied()
    }

    /// Resolves an internal app ID to its external slug.
    pub fn resolve_app_id_to_slug(&self, id: AppId) -> Option<AppSlug> {
        self.state.read().app_id_to_slug.get(&id).copied()
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
    ///
    /// * `organization` - Internal organization identifier (`OrganizationId`).
    /// * `vault` - Internal vault identifier (`VaultId`).
    pub fn client_idempotency_check(
        &self,
        organization: OrganizationId,
        vault: VaultId,
        client_id: &str,
        idempotency_key: &[u8; 16],
        request_hash: u64,
    ) -> IdempotencyCheckResult {
        // Zero key means the client didn't provide an idempotency key
        if *idempotency_key == [0u8; 16] {
            return IdempotencyCheckResult::Miss;
        }

        let key = (organization, vault, ClientId::new(client_id));
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
    #[doc(hidden)]
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
        state.client_sequences.insert((org, vault, ClientId::new(client_id)), entry);
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

    // ── Vault height tests ──

    #[test]
    fn vault_height_returns_zero_for_unknown() {
        let accessor =
            AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(AppliedState::default())));
        assert_eq!(accessor.vault_height(OrganizationId::new(1), VaultId::new(1)), 0);
    }

    #[test]
    fn vault_height_returns_stored_value() {
        let mut state = AppliedState::default();
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(2)), 42);
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(accessor.vault_height(OrganizationId::new(1), VaultId::new(2)), 42);
    }

    #[test]
    fn max_vault_height_empty() {
        let accessor =
            AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(AppliedState::default())));
        assert_eq!(accessor.max_vault_height(), 0);
    }

    #[test]
    fn max_vault_height_returns_max() {
        let mut state = AppliedState::default();
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 10);
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(2)), 50);
        state.vault_heights.insert((OrganizationId::new(2), VaultId::new(1)), 30);
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(accessor.max_vault_height(), 50);
    }

    #[test]
    fn org_max_vault_height_filters_by_org() {
        let mut state = AppliedState::default();
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 10);
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(2)), 50);
        state.vault_heights.insert((OrganizationId::new(2), VaultId::new(1)), 100);
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(accessor.org_max_vault_height(OrganizationId::new(1)), 50);
        assert_eq!(accessor.org_max_vault_height(OrganizationId::new(2)), 100);
        assert_eq!(accessor.org_max_vault_height(OrganizationId::new(3)), 0);
    }

    #[test]
    fn vault_height_count() {
        let mut state = AppliedState::default();
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 10);
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(2)), 20);
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(accessor.vault_height_count(), 2);
    }

    #[test]
    fn for_each_vault_height_visits_all() {
        let mut state = AppliedState::default();
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 10);
        state.vault_heights.insert((OrganizationId::new(2), VaultId::new(3)), 20);
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        let mut visited = Vec::new();
        accessor.for_each_vault_height(|org, vault, height| {
            visited.push((org, vault, height));
        });
        assert_eq!(visited.len(), 2);
    }

    #[test]
    fn org_vault_heights_filters_correctly() {
        let mut state = AppliedState::default();
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(1)), 10);
        state.vault_heights.insert((OrganizationId::new(1), VaultId::new(2)), 20);
        state.vault_heights.insert((OrganizationId::new(2), VaultId::new(1)), 30);
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        let heights = accessor.org_vault_heights(OrganizationId::new(1));
        assert_eq!(heights.len(), 2);
    }

    // ── Vault metadata tests ──

    #[test]
    fn get_vault_returns_none_for_unknown() {
        let accessor =
            AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(AppliedState::default())));
        assert!(accessor.get_vault(OrganizationId::new(1), VaultId::new(1)).is_none());
    }

    #[test]
    fn get_vault_excludes_deleted() {
        let mut state = AppliedState::default();
        state.vaults.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            super::super::types::VaultMeta {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                slug: VaultSlug::new(100),
                name: Some("test".to_string()),
                deleted: true,
                last_write_timestamp: 0,
                retention_policy: Default::default(),
            },
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert!(accessor.get_vault(OrganizationId::new(1), VaultId::new(1)).is_none());
    }

    #[test]
    fn list_vaults_excludes_deleted_and_other_orgs() {
        let mut state = AppliedState::default();
        state.vaults.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            super::super::types::VaultMeta {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                slug: VaultSlug::new(100),
                name: None,
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: Default::default(),
            },
        );
        state.vaults.insert(
            (OrganizationId::new(1), VaultId::new(2)),
            super::super::types::VaultMeta {
                organization: OrganizationId::new(1),
                vault: VaultId::new(2),
                slug: VaultSlug::new(200),
                name: None,
                deleted: true,
                last_write_timestamp: 0,
                retention_policy: Default::default(),
            },
        );
        state.vaults.insert(
            (OrganizationId::new(2), VaultId::new(3)),
            super::super::types::VaultMeta {
                organization: OrganizationId::new(2),
                vault: VaultId::new(3),
                slug: VaultSlug::new(300),
                name: None,
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: Default::default(),
            },
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        let vaults = accessor.list_vaults(OrganizationId::new(1));
        assert_eq!(vaults.len(), 1);
        assert_eq!(vaults[0].vault, VaultId::new(1));
    }

    #[test]
    fn vault_count_excludes_deleted() {
        let mut state = AppliedState::default();
        state.vaults.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            super::super::types::VaultMeta {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                slug: VaultSlug::new(100),
                name: None,
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: Default::default(),
            },
        );
        state.vaults.insert(
            (OrganizationId::new(1), VaultId::new(2)),
            super::super::types::VaultMeta {
                organization: OrganizationId::new(1),
                vault: VaultId::new(2),
                slug: VaultSlug::new(200),
                name: None,
                deleted: true,
                last_write_timestamp: 0,
                retention_policy: Default::default(),
            },
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(accessor.vault_count(OrganizationId::new(1)), 1);
    }

    // ── Organization tests ──

    #[test]
    fn list_organizations_excludes_deleted() {
        use inferadb_ledger_state::system::OrganizationStatus;
        let mut state = AppliedState::default();
        state.organizations.insert(
            OrganizationId::new(1),
            super::super::types::OrganizationMeta {
                organization: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(100),
                region: inferadb_ledger_types::Region::GLOBAL,
                status: OrganizationStatus::Active,
                tier: Default::default(),
                pending_region: None,
                storage_bytes: 0,
            },
        );
        state.organizations.insert(
            OrganizationId::new(2),
            super::super::types::OrganizationMeta {
                organization: OrganizationId::new(2),
                slug: inferadb_ledger_types::OrganizationSlug::new(200),
                region: inferadb_ledger_types::Region::GLOBAL,
                status: OrganizationStatus::Deleted,
                tier: Default::default(),
                pending_region: None,
                storage_bytes: 0,
            },
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        let orgs = accessor.list_organizations();
        assert_eq!(orgs.len(), 1);
        assert_eq!(orgs[0].organization, OrganizationId::new(1));
    }

    #[test]
    fn get_organization_excludes_deleted() {
        use inferadb_ledger_state::system::OrganizationStatus;
        let mut state = AppliedState::default();
        state.organizations.insert(
            OrganizationId::new(1),
            super::super::types::OrganizationMeta {
                organization: OrganizationId::new(1),
                slug: inferadb_ledger_types::OrganizationSlug::new(100),
                region: inferadb_ledger_types::Region::GLOBAL,
                status: OrganizationStatus::Deleted,
                tier: Default::default(),
                pending_region: None,
                storage_bytes: 0,
            },
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert!(accessor.get_organization(OrganizationId::new(1)).is_none());
    }

    // ── Slug resolution tests ──

    #[test]
    fn resolve_slug_to_id_and_back() {
        let mut state = AppliedState::default();
        state
            .slug_index
            .insert(inferadb_ledger_types::OrganizationSlug::new(100), OrganizationId::new(1));
        state
            .id_to_slug
            .insert(OrganizationId::new(1), inferadb_ledger_types::OrganizationSlug::new(100));
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(
            accessor.resolve_slug_to_id(inferadb_ledger_types::OrganizationSlug::new(100)),
            Some(OrganizationId::new(1))
        );
        assert_eq!(
            accessor.resolve_id_to_slug(OrganizationId::new(1)),
            Some(inferadb_ledger_types::OrganizationSlug::new(100))
        );
    }

    #[test]
    fn resolve_vault_slug_to_id_and_back() {
        let mut state = AppliedState::default();
        state.vault_slug_index.insert(VaultSlug::new(200), VaultId::new(2));
        state.vault_id_to_slug.insert(VaultId::new(2), VaultSlug::new(200));
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(accessor.resolve_vault_slug_to_id(VaultSlug::new(200)), Some(VaultId::new(2)));
        assert_eq!(accessor.resolve_vault_id_to_slug(VaultId::new(2)), Some(VaultSlug::new(200)));
    }

    #[test]
    fn resolve_user_slug_to_id_and_back() {
        let mut state = AppliedState::default();
        state.user_slug_index.insert(
            inferadb_ledger_types::UserSlug::new(300),
            inferadb_ledger_types::UserId::new(3),
        );
        state.user_id_to_slug.insert(
            inferadb_ledger_types::UserId::new(3),
            inferadb_ledger_types::UserSlug::new(300),
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(
            accessor.resolve_user_slug_to_id(inferadb_ledger_types::UserSlug::new(300)),
            Some(inferadb_ledger_types::UserId::new(3))
        );
        assert_eq!(
            accessor.resolve_user_id_to_slug(inferadb_ledger_types::UserId::new(3)),
            Some(inferadb_ledger_types::UserSlug::new(300))
        );
    }

    #[test]
    fn resolve_team_slug() {
        let mut state = AppliedState::default();
        state.team_slug_index.insert(
            inferadb_ledger_types::TeamSlug::new(400),
            (OrganizationId::new(1), inferadb_ledger_types::TeamId::new(4)),
        );
        state.team_id_to_slug.insert(
            inferadb_ledger_types::TeamId::new(4),
            inferadb_ledger_types::TeamSlug::new(400),
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(
            accessor.resolve_team_slug(inferadb_ledger_types::TeamSlug::new(400)),
            Some((OrganizationId::new(1), inferadb_ledger_types::TeamId::new(4)))
        );
        assert_eq!(
            accessor.resolve_team_id_to_slug(inferadb_ledger_types::TeamId::new(4)),
            Some(inferadb_ledger_types::TeamSlug::new(400))
        );
    }

    #[test]
    fn resolve_app_slug() {
        let mut state = AppliedState::default();
        state.app_slug_index.insert(
            inferadb_ledger_types::AppSlug::new(500),
            (OrganizationId::new(1), inferadb_ledger_types::AppId::new(5)),
        );
        state
            .app_id_to_slug
            .insert(inferadb_ledger_types::AppId::new(5), inferadb_ledger_types::AppSlug::new(500));
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(
            accessor.resolve_app_slug(inferadb_ledger_types::AppSlug::new(500)),
            Some((OrganizationId::new(1), inferadb_ledger_types::AppId::new(5)))
        );
        assert_eq!(
            accessor.resolve_app_id_to_slug(inferadb_ledger_types::AppId::new(5)),
            Some(inferadb_ledger_types::AppSlug::new(500))
        );
    }

    // ── Storage and usage tests ──

    #[test]
    fn organization_storage_bytes_default_zero() {
        let accessor =
            AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(AppliedState::default())));
        assert_eq!(accessor.organization_storage_bytes(OrganizationId::new(1)), 0);
    }

    #[test]
    fn organization_storage_bytes_returns_stored() {
        let mut state = AppliedState::default();
        state.organization_storage_bytes.insert(OrganizationId::new(1), 1024);
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(accessor.organization_storage_bytes(OrganizationId::new(1)), 1024);
    }

    #[test]
    fn organization_usage_combines_storage_and_vault_count() {
        let mut state = AppliedState::default();
        state.organization_storage_bytes.insert(OrganizationId::new(1), 2048);
        state.vaults.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            super::super::types::VaultMeta {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                slug: VaultSlug::new(100),
                name: None,
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: Default::default(),
            },
        );
        state.vaults.insert(
            (OrganizationId::new(1), VaultId::new(2)),
            super::super::types::VaultMeta {
                organization: OrganizationId::new(1),
                vault: VaultId::new(2),
                slug: VaultSlug::new(200),
                name: None,
                deleted: true, // deleted, should not count
                last_write_timestamp: 0,
                retention_policy: Default::default(),
            },
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        let usage = accessor.organization_usage(OrganizationId::new(1));
        assert_eq!(usage.storage_bytes, 2048);
        assert_eq!(usage.vault_count, 1);
    }

    // ── Region height and client sequence tests ──

    #[test]
    fn region_height_default_zero() {
        let accessor =
            AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(AppliedState::default())));
        assert_eq!(accessor.region_height(), 0);
    }

    #[test]
    fn region_height_returns_stored() {
        let state = AppliedState { region_height: 999, ..Default::default() };
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(accessor.region_height(), 999);
    }

    #[test]
    fn client_sequence_returns_zero_for_unknown() {
        let accessor =
            AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(AppliedState::default())));
        assert_eq!(accessor.client_sequence(OrganizationId::new(1), VaultId::new(1), "unknown"), 0);
    }

    #[test]
    fn client_sequence_returns_stored() {
        let mut state = AppliedState::default();
        state.client_sequences.insert(
            (OrganizationId::new(1), VaultId::new(1), ClientId::new("client-1")),
            ClientSequenceEntry { sequence: 42, ..Default::default() },
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        assert_eq!(
            accessor.client_sequence(OrganizationId::new(1), VaultId::new(1), "client-1"),
            42
        );
    }

    // ── User organization index test ──

    #[test]
    fn user_organization_ids_returns_empty_for_unknown() {
        let accessor =
            AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(AppliedState::default())));
        assert!(accessor.user_organization_ids(inferadb_ledger_types::UserId::new(1)).is_empty());
    }

    #[test]
    fn user_organization_ids_returns_stored() {
        let mut state = AppliedState::default();
        let mut orgs = HashSet::new();
        orgs.insert(OrganizationId::new(1));
        orgs.insert(OrganizationId::new(2));
        state.user_org_index.insert(inferadb_ledger_types::UserId::new(42), orgs);
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        let result = accessor.user_organization_ids(inferadb_ledger_types::UserId::new(42));
        assert_eq!(result.len(), 2);
        assert!(result.contains(&OrganizationId::new(1)));
    }

    // ── all_vaults test ──

    #[test]
    fn all_vaults_excludes_deleted() {
        let mut state = AppliedState::default();
        state.vaults.insert(
            (OrganizationId::new(1), VaultId::new(1)),
            super::super::types::VaultMeta {
                organization: OrganizationId::new(1),
                vault: VaultId::new(1),
                slug: VaultSlug::new(100),
                name: None,
                deleted: false,
                last_write_timestamp: 0,
                retention_policy: Default::default(),
            },
        );
        state.vaults.insert(
            (OrganizationId::new(1), VaultId::new(2)),
            super::super::types::VaultMeta {
                organization: OrganizationId::new(1),
                vault: VaultId::new(2),
                slug: VaultSlug::new(200),
                name: None,
                deleted: true,
                last_write_timestamp: 0,
                retention_policy: Default::default(),
            },
        );
        let accessor = AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(state)));
        let all = accessor.all_vaults();
        assert_eq!(all.len(), 1);
    }

    // ── vault_health test ──

    #[test]
    fn vault_health_returns_default_for_unknown() {
        let accessor =
            AppliedStateAccessor::new_for_test(Arc::new(RwLock::new(AppliedState::default())));
        let health = accessor.vault_health(OrganizationId::new(1), VaultId::new(1));
        // Default should be Healthy
        assert_eq!(health, VaultHealthStatus::Healthy);
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
