//! Caches for the encryption layer: DEK cache and RMK cache.
//!
//! The DEK cache avoids repeated AES-KWP unwrap operations by keeping
//! recently-used plaintext DEKs in memory (keyed by their wrapped form).
//! The RMK cache maps version numbers to loaded master keys.

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use super::types::{DataEncryptionKey, RegionMasterKey, WrappedDek};

/// Bounded LRU cache for unwrapped Data Encryption Keys.
///
/// Keyed by [`WrappedDek`] so that RMK rotation (which re-wraps DEKs)
/// naturally invalidates stale entries without explicit purge.
///
/// All cached keys implement [`zeroize::ZeroizeOnDrop`], so evicted
/// entries are securely wiped from memory.
pub struct DekCache {
    /// Moka sync cache: WrappedDek → Arc<DataEncryptionKey>.
    ///
    /// Arc is needed because moka returns clones; the inner key
    /// is still zeroized when all Arcs are dropped.
    cache: moka::sync::Cache<WrappedDek, Arc<DataEncryptionKey>>,
}

impl DekCache {
    /// Creates a new DEK cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self { cache: moka::sync::Cache::new(capacity as u64) }
    }

    /// Looks up an unwrapped DEK by its wrapped form.
    pub fn get(&self, wrapped: &WrappedDek) -> Option<Arc<DataEncryptionKey>> {
        self.cache.get(wrapped)
    }

    /// Inserts an unwrapped DEK keyed by its wrapped form.
    pub fn insert(&self, wrapped: WrappedDek, dek: DataEncryptionKey) {
        self.cache.insert(wrapped, Arc::new(dek));
    }

    /// Invalidates all cached entries.
    ///
    /// Used during RMK rotation to force re-unwrap with new keys.
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// Returns the number of entries currently cached.
    #[cfg(test)]
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }
}

impl std::fmt::Debug for DekCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DekCache")
            .field("capacity", &self.cache.policy().max_capacity())
            .field("entries", &self.cache.entry_count())
            .finish()
    }
}

/// Cache for loaded Region Master Keys, keyed by version.
///
/// Typically holds 1-2 entries (current + previous during rotation).
/// All keys implement [`zeroize::ZeroizeOnDrop`].
pub struct RmkCache {
    keys: RwLock<HashMap<u32, Arc<RegionMasterKey>>>,
}

impl RmkCache {
    /// Creates a new empty RMK cache.
    pub fn new() -> Self {
        Self { keys: RwLock::new(HashMap::new()) }
    }

    /// Looks up an RMK by version.
    pub fn get(&self, version: u32) -> Option<Arc<RegionMasterKey>> {
        self.keys.read().get(&version).cloned()
    }

    /// Inserts an RMK for the given version.
    pub fn insert(&self, rmk: RegionMasterKey) {
        let version = rmk.version;
        self.keys.write().insert(version, Arc::new(rmk));
    }

    /// Returns the highest-versioned (current) RMK, if any.
    pub fn current(&self) -> Option<Arc<RegionMasterKey>> {
        let keys = self.keys.read();
        keys.keys().max().and_then(|&v| keys.get(&v).cloned())
    }

    /// Removes a specific RMK version (decommission).
    pub fn remove(&self, version: u32) {
        self.keys.write().remove(&version);
    }

    /// Removes all cached RMKs.
    pub fn purge(&self) {
        self.keys.write().clear();
    }
}

impl Default for RmkCache {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RmkCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys = self.keys.read();
        f.debug_struct("RmkCache").field("versions", &keys.keys().collect::<Vec<_>>()).finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make_dek(val: u8) -> DataEncryptionKey {
        DataEncryptionKey::from_bytes([val; 32])
    }

    fn make_wrapped(val: u8) -> WrappedDek {
        WrappedDek::from_bytes([val; 40])
    }

    fn make_rmk(version: u32) -> RegionMasterKey {
        RegionMasterKey::new(version, [version as u8; 32])
    }

    // --- DekCache tests ---

    #[test]
    fn test_dek_cache_insert_and_get() {
        let cache = DekCache::new(100);
        let wrapped = make_wrapped(1);
        let dek = make_dek(1);

        cache.insert(wrapped.clone(), dek.clone());

        let retrieved = cache.get(&wrapped).unwrap();
        assert_eq!(retrieved.as_bytes(), dek.as_bytes());
    }

    #[test]
    fn test_dek_cache_miss() {
        let cache = DekCache::new(100);
        let wrapped = make_wrapped(1);
        assert!(cache.get(&wrapped).is_none());
    }

    #[test]
    fn test_dek_cache_invalidate_all() {
        let cache = DekCache::new(100);
        cache.insert(make_wrapped(1), make_dek(1));
        cache.insert(make_wrapped(2), make_dek(2));

        cache.invalidate_all();
        // Moka invalidation is lazy, so run_pending may be needed
        // but get() on invalidated entries returns None
        assert!(cache.get(&make_wrapped(1)).is_none());
        assert!(cache.get(&make_wrapped(2)).is_none());
    }

    #[test]
    fn test_dek_cache_overwrites_on_same_key() {
        let cache = DekCache::new(100);
        let wrapped = make_wrapped(1);

        cache.insert(wrapped.clone(), make_dek(0xAA));
        cache.insert(wrapped.clone(), make_dek(0xBB));

        let retrieved = cache.get(&wrapped).unwrap();
        assert_eq!(retrieved.as_bytes(), &[0xBB; 32]);
    }

    // --- RmkCache tests ---

    #[test]
    fn test_rmk_cache_insert_and_get() {
        let cache = RmkCache::new();
        let rmk = make_rmk(1);

        cache.insert(rmk);

        let retrieved = cache.get(1).unwrap();
        assert_eq!(retrieved.version, 1);
    }

    #[test]
    fn test_rmk_cache_miss() {
        let cache = RmkCache::new();
        assert!(cache.get(99).is_none());
    }

    #[test]
    fn test_rmk_cache_current_returns_highest_version() {
        let cache = RmkCache::new();
        cache.insert(make_rmk(1));
        cache.insert(make_rmk(3));
        cache.insert(make_rmk(2));

        let current = cache.current().unwrap();
        assert_eq!(current.version, 3);
    }

    #[test]
    fn test_rmk_cache_current_empty() {
        let cache = RmkCache::new();
        assert!(cache.current().is_none());
    }

    #[test]
    fn test_rmk_cache_remove() {
        let cache = RmkCache::new();
        cache.insert(make_rmk(1));
        cache.insert(make_rmk(2));

        cache.remove(1);
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_some());
    }

    #[test]
    fn test_rmk_cache_purge() {
        let cache = RmkCache::new();
        cache.insert(make_rmk(1));
        cache.insert(make_rmk(2));

        cache.purge();
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_none());
        assert!(cache.current().is_none());
    }
}
