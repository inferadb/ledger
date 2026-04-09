//! In-memory relationship hash index for O(1) existence checks.
//!
//! Per-vault hash map keyed by seahash of the binary relationship key.
//! Contains all relationships for active vaults. Evicted on hibernate,
//! rebuilt from B+ tree on demand.

use dashmap::DashMap;
use inferadb_ledger_types::VaultId;

/// In-memory hash index providing O(1) relationship existence checks.
///
/// Each vault maintains an independent set of seahash digests. A vault
/// entry in the outer map signals "this vault has been indexed" — an
/// empty inner map means the vault was loaded but contains no
/// relationships.
#[derive(Debug)]
pub struct RelationshipIndex {
    vaults: DashMap<VaultId, DashMap<u64, ()>>,
}

impl RelationshipIndex {
    /// Creates a new, empty relationship index.
    pub fn new() -> Self {
        Self { vaults: DashMap::new() }
    }

    /// Checks whether a relationship exists in the index.
    ///
    /// Returns `Some(true)` if the hash is present, `Some(false)` if the
    /// vault is indexed but the hash is absent, and `None` if the vault
    /// has not been loaded into the index.
    pub fn exists(&self, vault: VaultId, key_hash: u64) -> Option<bool> {
        self.vaults.get(&vault).map(|set| set.contains_key(&key_hash))
    }

    /// Inserts a relationship hash into the index, creating the vault
    /// entry if it does not already exist.
    pub fn insert(&self, vault: VaultId, key_hash: u64) {
        self.vaults.entry(vault).or_default().insert(key_hash, ());
    }

    /// Removes a relationship hash from the index.
    ///
    /// No-op if the vault is not loaded or the hash is absent.
    pub fn remove(&self, vault: VaultId, key_hash: u64) {
        if let Some(set) = self.vaults.get(&vault) {
            set.remove(&key_hash);
        }
    }

    /// Marks a vault as loaded (possibly empty) without inserting any hashes.
    pub fn ensure_vault(&self, vault: VaultId) {
        self.vaults.entry(vault).or_default();
    }

    /// Evicts all index data for a vault, freeing memory.
    ///
    /// After eviction, [`exists`](Self::exists) returns `None` for this vault.
    pub fn evict_vault(&self, vault: VaultId) {
        self.vaults.remove(&vault);
    }

    /// Returns whether the vault is currently loaded in the index.
    pub fn is_vault_loaded(&self, vault: VaultId) -> bool {
        self.vaults.contains_key(&vault)
    }
}

impl Default for RelationshipIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[test]
    fn empty_vault_returns_none() {
        let index = RelationshipIndex::new();
        let vault = VaultId::new(1);
        assert!(index.exists(vault, 42).is_none());
    }

    #[test]
    fn loaded_vault_returns_some() {
        let index = RelationshipIndex::new();
        let vault = VaultId::new(1);

        index.insert(vault, 100);

        assert_eq!(index.exists(vault, 100), Some(true));
        assert_eq!(index.exists(vault, 999), Some(false));
    }

    #[test]
    fn vault_isolation() {
        let index = RelationshipIndex::new();
        let vault1 = VaultId::new(1);
        let vault2 = VaultId::new(2);

        index.insert(vault1, 42);

        assert_eq!(index.exists(vault1, 42), Some(true));
        assert!(index.exists(vault2, 42).is_none());
    }

    #[test]
    fn insert_and_remove() {
        let index = RelationshipIndex::new();
        let vault = VaultId::new(1);

        index.insert(vault, 50);
        assert_eq!(index.exists(vault, 50), Some(true));

        index.remove(vault, 50);
        assert_eq!(index.exists(vault, 50), Some(false));
    }

    #[test]
    fn evict_vault_frees_memory() {
        let index = RelationshipIndex::new();
        let vault = VaultId::new(1);

        index.insert(vault, 10);
        assert_eq!(index.exists(vault, 10), Some(true));

        index.evict_vault(vault);
        assert!(index.exists(vault, 10).is_none());
        assert!(!index.is_vault_loaded(vault));
    }

    #[test]
    fn ensure_vault_creates_empty_index() {
        let index = RelationshipIndex::new();
        let vault = VaultId::new(1);

        assert!(!index.is_vault_loaded(vault));

        index.ensure_vault(vault);

        assert!(index.is_vault_loaded(vault));
        assert_eq!(index.exists(vault, 42), Some(false));
    }

    #[test]
    fn concurrent_access_is_safe() {
        use std::sync::Arc;

        let index = Arc::new(RelationshipIndex::new());
        let vault = VaultId::new(1);

        let handles: Vec<_> = (0..100)
            .map(|i| {
                let idx = Arc::clone(&index);
                std::thread::spawn(move || {
                    idx.insert(vault, i);
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread should not panic");
        }

        for i in 0..100 {
            assert_eq!(index.exists(vault, i), Some(true));
        }
    }
}
