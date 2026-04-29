//! Per-scope encryption keys for snapshot persistence and install.
//!
//! Provides a [`SnapshotKeyProvider`] trait for retrieving AES-256 keys
//! scoped by `(region, organization_id, Option<vault_id>)`. Org-level
//! snapshots use `vault_id = None`; per-vault snapshots use `vault_id = Some(_)`.
//!
//! Symmetric with [`VaultKeyProvider`](inferadb_ledger_consensus::crypto::VaultKeyProvider),
//! which keys WAL-frame DEKs by `(vault_id, version)`. The two trait surfaces
//! are deliberately disjoint: WAL frames already carry version metadata in
//! their header, while snapshots key by the full residency triple so an
//! org-level snapshot file and a per-vault snapshot file in the same region
//! can be encrypted under independent keys.
//!
//! Two implementations ship with the trait:
//! - [`NoopSnapshotKeyProvider`] — always returns `None`. Stage 2's persister will skip the
//!   encryption envelope when the provider returns `None`; valid only for tests / unencrypted
//!   local-dev configurations.
//! - [`InMemorySnapshotKeyProvider`] — a `HashMap` of keys behind a `RwLock`. Suitable for tests
//!   and single-node deployments.
//!
//! # Stage 1a scaffolding
//!
//! This module exists to thread a typed key provider through
//! [`RaftManagerConfig`](crate::raft_manager::RaftManagerConfig) ->
//! [`RaftLogStore`](crate::log_storage::RaftLogStore) ->
//! [`LedgerSnapshotBuilder`](crate::log_storage::LedgerSnapshotBuilder)
//! ahead of the Stage 1b builder bifurcation and Stage 2 persister wiring.
//! No call site dereferences the provider yet.

use std::{collections::HashMap, sync::Arc};

use inferadb_ledger_types::{OrganizationId, Region, VaultId};
use parking_lot::RwLock;

/// Provides per-scope AES-256 encryption keys for snapshot persistence and install.
///
/// Implementations must be safe to share across threads.
pub trait SnapshotKeyProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Returns the encryption key for a snapshot in the given scope.
    ///
    /// `vault_id = None` denotes an org-level snapshot; `vault_id = Some(_)`
    /// denotes a per-vault snapshot. Returning `None` means the snapshot in
    /// this scope is unencrypted — the Stage 2 persister will write the
    /// snapshot in plaintext when this returns `None`. Plaintext snapshots
    /// are valid only for tests and explicitly unencrypted local-dev
    /// configurations.
    fn snapshot_key(
        &self,
        region: &Region,
        organization_id: OrganizationId,
        vault_id: Option<VaultId>,
    ) -> Option<[u8; 32]>;
}

/// A snapshot key provider that always returns `None`.
///
/// Used as the bootstrap default and in tests where snapshot encryption is
/// disabled.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSnapshotKeyProvider;

impl SnapshotKeyProvider for NoopSnapshotKeyProvider {
    fn snapshot_key(
        &self,
        _region: &Region,
        _organization_id: OrganizationId,
        _vault_id: Option<VaultId>,
    ) -> Option<[u8; 32]> {
        None
    }
}

/// An in-memory snapshot key provider backed by a `HashMap` behind a [`RwLock`].
///
/// Keys are registered with [`insert_key`](Self::insert_key). The `(region,
/// organization_id, vault_id)` tuple is the lookup key, with `vault_id = None`
/// reserved for org-level snapshots. Suitable for tests and single-node
/// deployments where keys are loaded once at startup; production deployments
/// expect a KMS-backed implementation.
#[derive(Debug, Default)]
pub struct InMemorySnapshotKeyProvider {
    keys: RwLock<HashMap<(Region, OrganizationId, Option<VaultId>), [u8; 32]>>,
}

impl InMemorySnapshotKeyProvider {
    /// Creates an empty provider with no registered keys.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a snapshot encryption key for the given scope.
    ///
    /// If a key already exists for this `(region, organization_id, vault_id)`
    /// triple it is overwritten.
    pub fn insert_key(
        &self,
        region: Region,
        organization_id: OrganizationId,
        vault_id: Option<VaultId>,
        key: [u8; 32],
    ) {
        self.keys.write().insert((region, organization_id, vault_id), key);
    }
}

impl SnapshotKeyProvider for InMemorySnapshotKeyProvider {
    fn snapshot_key(
        &self,
        region: &Region,
        organization_id: OrganizationId,
        vault_id: Option<VaultId>,
    ) -> Option<[u8; 32]> {
        self.keys.read().get(&(*region, organization_id, vault_id)).copied()
    }
}

impl<K: SnapshotKeyProvider + ?Sized> SnapshotKeyProvider for Arc<K> {
    fn snapshot_key(
        &self,
        region: &Region,
        organization_id: OrganizationId,
        vault_id: Option<VaultId>,
    ) -> Option<[u8; 32]> {
        (**self).snapshot_key(region, organization_id, vault_id)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn org(id: i64) -> OrganizationId {
        OrganizationId::new(id)
    }

    fn vault(id: i64) -> VaultId {
        VaultId::new(id)
    }

    // ---- NoopSnapshotKeyProvider ----

    #[test]
    fn noop_provider_returns_none_for_org_scope() {
        let provider = NoopSnapshotKeyProvider;
        assert!(provider.snapshot_key(&Region::US_EAST_VA, org(1), None).is_none());
    }

    #[test]
    fn noop_provider_returns_none_for_vault_scope() {
        let provider = NoopSnapshotKeyProvider;
        assert!(provider.snapshot_key(&Region::US_EAST_VA, org(1), Some(vault(7))).is_none());
    }

    #[test]
    fn noop_provider_returns_none_across_arbitrary_scopes() {
        let provider = NoopSnapshotKeyProvider;
        for region in [Region::GLOBAL, Region::US_EAST_VA, Region::IE_EAST_DUBLIN] {
            for org_id in [org(0), org(1), org(i64::MAX)] {
                for vault_id in [None, Some(vault(0)), Some(vault(i64::MAX))] {
                    assert!(provider.snapshot_key(&region, org_id, vault_id).is_none());
                }
            }
        }
    }

    // ---- InMemorySnapshotKeyProvider basic ops ----

    #[test]
    fn inmemory_round_trips_org_scope_key() {
        let provider = InMemorySnapshotKeyProvider::new();
        let key = [0xAB; 32];
        provider.insert_key(Region::US_EAST_VA, org(1), None, key);

        assert_eq!(provider.snapshot_key(&Region::US_EAST_VA, org(1), None), Some(key));
    }

    #[test]
    fn inmemory_round_trips_vault_scope_key() {
        let provider = InMemorySnapshotKeyProvider::new();
        let key = [0xCD; 32];
        provider.insert_key(Region::US_EAST_VA, org(1), Some(vault(7)), key);

        assert_eq!(provider.snapshot_key(&Region::US_EAST_VA, org(1), Some(vault(7))), Some(key));
    }

    #[test]
    fn inmemory_returns_none_for_unregistered_scope() {
        let provider = InMemorySnapshotKeyProvider::new();
        assert!(provider.snapshot_key(&Region::US_EAST_VA, org(999), None).is_none());
        assert!(provider.snapshot_key(&Region::US_EAST_VA, org(999), Some(vault(1))).is_none());
    }

    // ---- Scope isolation: every coordinate is part of the key ----

    #[test]
    fn inmemory_distinguishes_by_region() {
        let provider = InMemorySnapshotKeyProvider::new();
        let key_us = [0x01; 32];
        let key_ie = [0x02; 32];

        provider.insert_key(Region::US_EAST_VA, org(1), None, key_us);
        provider.insert_key(Region::IE_EAST_DUBLIN, org(1), None, key_ie);

        assert_eq!(provider.snapshot_key(&Region::US_EAST_VA, org(1), None), Some(key_us));
        assert_eq!(provider.snapshot_key(&Region::IE_EAST_DUBLIN, org(1), None), Some(key_ie));
    }

    #[test]
    fn inmemory_distinguishes_by_organization() {
        let provider = InMemorySnapshotKeyProvider::new();
        let key_a = [0x10; 32];
        let key_b = [0x20; 32];

        provider.insert_key(Region::US_EAST_VA, org(1), None, key_a);
        provider.insert_key(Region::US_EAST_VA, org(2), None, key_b);

        assert_eq!(provider.snapshot_key(&Region::US_EAST_VA, org(1), None), Some(key_a));
        assert_eq!(provider.snapshot_key(&Region::US_EAST_VA, org(2), None), Some(key_b));
    }

    #[test]
    fn inmemory_distinguishes_org_scope_from_vault_zero_scope() {
        // `None` (org-level) and `Some(VaultId::new(0))` must not collide.
        // A flat `i64`-keyed map would silently merge these.
        let provider = InMemorySnapshotKeyProvider::new();
        let key_org = [0xAA; 32];
        let key_vault0 = [0xBB; 32];

        provider.insert_key(Region::US_EAST_VA, org(1), None, key_org);
        provider.insert_key(Region::US_EAST_VA, org(1), Some(vault(0)), key_vault0);

        assert_eq!(provider.snapshot_key(&Region::US_EAST_VA, org(1), None), Some(key_org));
        assert_eq!(
            provider.snapshot_key(&Region::US_EAST_VA, org(1), Some(vault(0))),
            Some(key_vault0)
        );
    }

    #[test]
    fn inmemory_distinguishes_by_vault_id() {
        let provider = InMemorySnapshotKeyProvider::new();
        let key_v1 = [0x33; 32];
        let key_v2 = [0x44; 32];

        provider.insert_key(Region::US_EAST_VA, org(1), Some(vault(1)), key_v1);
        provider.insert_key(Region::US_EAST_VA, org(1), Some(vault(2)), key_v2);

        assert_eq!(
            provider.snapshot_key(&Region::US_EAST_VA, org(1), Some(vault(1))),
            Some(key_v1)
        );
        assert_eq!(
            provider.snapshot_key(&Region::US_EAST_VA, org(1), Some(vault(2))),
            Some(key_v2)
        );
    }

    #[test]
    fn inmemory_overwrites_existing_key_for_same_scope() {
        let provider = InMemorySnapshotKeyProvider::new();
        let original = [0x55; 32];
        let replacement = [0x66; 32];

        provider.insert_key(Region::US_EAST_VA, org(1), None, original);
        provider.insert_key(Region::US_EAST_VA, org(1), None, replacement);

        assert_eq!(provider.snapshot_key(&Region::US_EAST_VA, org(1), None), Some(replacement));
    }

    // ---- Construction + Arc blanket impl ----

    #[test]
    fn noop_provider_compiles_under_arc_dyn() {
        let provider: Arc<dyn SnapshotKeyProvider> = Arc::new(NoopSnapshotKeyProvider);
        assert!(provider.snapshot_key(&Region::GLOBAL, org(0), None).is_none());
    }

    #[test]
    fn inmemory_provider_compiles_under_arc_dyn() {
        let backing = Arc::new(InMemorySnapshotKeyProvider::new());
        backing.insert_key(Region::US_EAST_VA, org(1), Some(vault(2)), [0x77; 32]);

        let provider: Arc<dyn SnapshotKeyProvider> = backing;
        assert_eq!(
            provider.snapshot_key(&Region::US_EAST_VA, org(1), Some(vault(2))),
            Some([0x77; 32])
        );
    }

    #[test]
    fn arc_blanket_impl_delegates_correctly() {
        let arc = Arc::new(InMemorySnapshotKeyProvider::new());
        arc.insert_key(Region::US_EAST_VA, org(1), None, [0x88; 32]);

        // Use the blanket impl through the trait method.
        let key = SnapshotKeyProvider::snapshot_key(&arc, &Region::US_EAST_VA, org(1), None);
        assert_eq!(key, Some([0x88; 32]));
    }
}
