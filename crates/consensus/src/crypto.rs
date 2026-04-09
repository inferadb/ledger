//! Encryption primitives for per-vault data encryption.
//!
//! Provides a [`VaultKeyProvider`] trait for retrieving AES-256 data encryption
//! keys (DEKs) keyed by vault ID and version. This enables crypto-shredding:
//! destroying a vault's DEK makes all its encrypted data permanently
//! unrecoverable.
//!
//! Two implementations are provided:
//! - [`NoopKeyProvider`] — always returns `None`, used when encryption is disabled.
//! - [`InMemoryKeyProvider`] — stores keys in memory behind a lock, suitable for tests and
//!   single-node deployments.

use std::collections::HashMap;

use parking_lot::RwLock;

/// Provides per-vault AES-256 data encryption keys.
///
/// Implementations must be safe to share across threads.
pub trait VaultKeyProvider: Send + Sync {
    /// Returns the DEK for the given vault and version, or `None` if the key
    /// has been destroyed (crypto-shredded) or was never registered.
    fn vault_key(&self, vault_id: u64, dek_version: u16) -> Option<[u8; 32]>;

    /// Returns the current DEK version for new writes to this vault.
    fn current_version(&self, vault_id: u64) -> u16;
}

/// A key provider that always returns `None`.
///
/// Used as the default in tests and when encryption is disabled.
#[derive(Debug, Default)]
pub struct NoopKeyProvider;

impl VaultKeyProvider for NoopKeyProvider {
    fn vault_key(&self, _vault_id: u64, _dek_version: u16) -> Option<[u8; 32]> {
        None
    }

    fn current_version(&self, _vault_id: u64) -> u16 {
        0
    }
}

/// An in-memory key provider backed by a `HashMap` behind a [`RwLock`].
///
/// Keys can be registered with [`set_key`](InMemoryKeyProvider::set_key) and
/// destroyed with [`destroy_key`](InMemoryKeyProvider::destroy_key). Destroying
/// a key simulates crypto-shredding: subsequent lookups return `None`.
#[derive(Debug, Default)]
pub struct InMemoryKeyProvider {
    keys: RwLock<HashMap<(u64, u16), [u8; 32]>>,
    current_versions: RwLock<HashMap<u64, u16>>,
}

impl InMemoryKeyProvider {
    /// Creates an empty provider with no registered keys.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a DEK for the given vault and version.
    ///
    /// If a key already exists for this `(vault_id, version)` pair it is
    /// overwritten. The current version for this vault is updated to
    /// `dek_version` when it exceeds the previously recorded version.
    pub fn set_key(&self, vault_id: u64, dek_version: u16, key: [u8; 32]) {
        self.keys.write().insert((vault_id, dek_version), key);

        let mut versions = self.current_versions.write();
        let current = versions.entry(vault_id).or_insert(0);
        if dek_version > *current {
            *current = dek_version;
        }
    }

    /// Destroys the DEK for the given vault and version (crypto-shredding).
    ///
    /// After destruction, [`vault_key`](VaultKeyProvider::vault_key) returns
    /// `None` for this `(vault_id, version)` pair.
    pub fn destroy_key(&self, vault_id: u64, dek_version: u16) {
        self.keys.write().remove(&(vault_id, dek_version));
    }
}

impl VaultKeyProvider for InMemoryKeyProvider {
    fn vault_key(&self, vault_id: u64, dek_version: u16) -> Option<[u8; 32]> {
        self.keys.read().get(&(vault_id, dek_version)).copied()
    }

    fn current_version(&self, vault_id: u64) -> u16 {
        self.current_versions.read().get(&vault_id).copied().unwrap_or(0)
    }
}

impl<K: VaultKeyProvider> VaultKeyProvider for std::sync::Arc<K> {
    fn vault_key(&self, vault_id: u64, dek_version: u16) -> Option<[u8; 32]> {
        (**self).vault_key(vault_id, dek_version)
    }

    fn current_version(&self, vault_id: u64) -> u16 {
        (**self).current_version(vault_id)
    }
}

/// Header prepended to an encrypted frame, identifying which key to use
/// for decryption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncryptedFrameHeader {
    /// The vault that owns this encrypted data.
    pub vault_id: u64,
    /// The DEK version used to encrypt the frame.
    pub dek_version: u16,
}

/// Result of a decryption attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecryptResult {
    /// Decryption succeeded.
    Ok(Vec<u8>),
    /// The DEK has been destroyed; data is permanently unrecoverable.
    KeyDestroyed,
    /// Decryption failed for another reason.
    Failed(String),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use super::*;

    // ---- NoopKeyProvider ----

    #[test]
    fn noop_provider_always_returns_none_and_version_zero() {
        let provider = NoopKeyProvider;
        assert!(provider.vault_key(1, 0).is_none());
        assert!(provider.vault_key(u64::MAX, u16::MAX).is_none());
        assert_eq!(provider.current_version(1), 0);
        assert_eq!(provider.current_version(u64::MAX), 0);
    }

    // ---- InMemoryKeyProvider basic ops ----

    #[test]
    fn set_key_stores_and_retrieves_key() {
        let provider = InMemoryKeyProvider::new();
        let key = [0xAB; 32];
        provider.set_key(1, 1, key);

        assert_eq!(provider.vault_key(1, 1), Some(key));
        assert_eq!(provider.current_version(1), 1);
    }

    #[test]
    fn vault_key_returns_none_for_unregistered_vault() {
        let provider = InMemoryKeyProvider::new();
        assert!(provider.vault_key(999, 0).is_none());
        assert_eq!(provider.current_version(999), 0);
    }

    #[test]
    fn destroy_key_makes_lookup_return_none() {
        let provider = InMemoryKeyProvider::new();
        provider.set_key(1, 1, [0xCD; 32]);

        provider.destroy_key(1, 1);
        assert!(provider.vault_key(1, 1).is_none());
    }

    #[test]
    fn destroy_nonexistent_key_is_harmless() {
        let provider = InMemoryKeyProvider::new();
        provider.destroy_key(42, 7); // no panic, no error
        assert!(provider.vault_key(42, 7).is_none());
    }

    // ---- Vault and version isolation ----

    #[test]
    fn different_vaults_are_independent() {
        let provider = InMemoryKeyProvider::new();
        let key_a = [0x01; 32];
        let key_b = [0x02; 32];

        provider.set_key(1, 1, key_a);
        provider.set_key(2, 1, key_b);

        assert_eq!(provider.vault_key(1, 1), Some(key_a));
        assert_eq!(provider.vault_key(2, 1), Some(key_b));

        provider.destroy_key(1, 1);
        assert!(provider.vault_key(1, 1).is_none());
        assert_eq!(
            provider.vault_key(2, 1),
            Some(key_b),
            "destroying vault 1 must not affect vault 2"
        );
    }

    #[test]
    fn different_versions_are_independent() {
        let provider = InMemoryKeyProvider::new();
        let key_v1 = [0x10; 32];
        let key_v2 = [0x20; 32];

        provider.set_key(1, 1, key_v1);
        provider.set_key(1, 2, key_v2);

        assert_eq!(provider.vault_key(1, 1), Some(key_v1));
        assert_eq!(provider.vault_key(1, 2), Some(key_v2));
        assert_eq!(provider.current_version(1), 2);

        provider.destroy_key(1, 1);
        assert!(provider.vault_key(1, 1).is_none());
        assert_eq!(provider.vault_key(1, 2), Some(key_v2));
    }

    // ---- Version tracking edge cases ----

    #[test]
    fn set_key_with_lower_version_does_not_downgrade_current() {
        let provider = InMemoryKeyProvider::new();
        provider.set_key(1, 5, [0xAA; 32]);
        assert_eq!(provider.current_version(1), 5);

        provider.set_key(1, 3, [0xBB; 32]);
        assert_eq!(provider.current_version(1), 5, "lower version must not downgrade current");

        // Both keys are still accessible.
        assert!(provider.vault_key(1, 5).is_some());
        assert!(provider.vault_key(1, 3).is_some());
    }

    #[test]
    fn destroy_current_version_leaves_stale_current_version() {
        // This documents the known behavior: destroy_key does not update
        // current_versions, so current_version() still reports the destroyed
        // version. Callers must handle the None return from vault_key().
        let provider = InMemoryKeyProvider::new();
        provider.set_key(1, 1, [0xCC; 32]);
        assert_eq!(provider.current_version(1), 1);

        provider.destroy_key(1, 1);
        assert_eq!(provider.current_version(1), 1, "current_version is stale after destroy");
        assert!(
            provider.vault_key(1, 1).is_none(),
            "vault_key must return None for destroyed current version"
        );
    }

    #[test]
    fn set_key_overwrites_existing_key_for_same_vault_version() {
        let provider = InMemoryKeyProvider::new();
        let original = [0x11; 32];
        let replacement = [0x22; 32];

        provider.set_key(1, 1, original);
        provider.set_key(1, 1, replacement);

        assert_eq!(provider.vault_key(1, 1), Some(replacement));
    }

    // ---- Arc blanket impl ----

    #[test]
    fn arc_wrapped_provider_delegates_correctly() {
        let provider = Arc::new(InMemoryKeyProvider::new());
        provider.set_key(5, 2, [0xFF; 32]);

        // Access through the VaultKeyProvider trait via Arc.
        let arc_ref: &dyn VaultKeyProvider = &provider;
        assert_eq!(arc_ref.vault_key(5, 2), Some([0xFF; 32]));
        assert_eq!(arc_ref.current_version(5), 2);
        assert!(arc_ref.vault_key(5, 99).is_none());
    }
}
