//! Encrypted storage backend wrapper.
//!
//! `EncryptedBackend<B>` wraps any [`StorageBackend`] and transparently
//! encrypts page content on write and decrypts on read. When encryption
//! is disabled, all operations pass through with zero overhead.

use std::sync::Arc;

use inferadb_ledger_types::types::Region;

use super::{
    cache::{DekCache, RmkCache},
    key_manager::RegionKeyManager,
    operations::{decrypt_page_body, encrypt_page_body, generate_dek, unwrap_dek, wrap_dek},
    sidecar::CryptoSidecar,
    types::{CryptoMetadata, RegionMasterKey},
};
use crate::{
    backend::StorageBackend,
    error::{Error, PageId, Result},
    page::PAGE_HEADER_SIZE,
};

/// Encrypted storage backend that wraps an inner [`StorageBackend`].
///
/// On write: generates a fresh DEK per page, encrypts the body
/// (bytes after the 16-byte page header) with AES-256-GCM using
/// the header as AAD, wraps the DEK with the current RMK, and
/// stores crypto metadata in the sidecar.
///
/// On read: loads crypto metadata, unwraps the DEK (checking the
/// DEK cache first), and decrypts the body. The header is returned
/// unmodified.
///
/// When `enabled` is false, all operations are pure pass-through.
pub struct EncryptedBackend<B: StorageBackend> {
    /// Inner storage backend.
    inner: B,
    /// Whether encryption is active.
    enabled: bool,
    /// Per-page crypto metadata storage.
    sidecar: CryptoSidecar,
    /// Cache of unwrapped DEKs (avoids AES-KWP on every read).
    dek_cache: DekCache,
    /// Cache of loaded RMKs by version.
    rmk_cache: RmkCache,
    /// Optional key manager for lazy-load-on-miss of RMK versions.
    key_manager: Option<Arc<dyn RegionKeyManager>>,
    /// Region this backend encrypts for (used with key_manager).
    region: Option<Region>,
}

impl<B: StorageBackend> EncryptedBackend<B> {
    /// Creates a new encrypted backend.
    ///
    /// - `inner`: The underlying storage backend to wrap.
    /// - `enabled`: Whether encryption is active.
    /// - `sidecar`: Storage for per-page crypto metadata.
    /// - `dek_cache_capacity`: Maximum DEKs to cache.
    /// - `rmk`: Initial Region Master Key (ignored if `enabled` is false).
    pub fn new(
        inner: B,
        enabled: bool,
        sidecar: CryptoSidecar,
        dek_cache_capacity: usize,
        rmk: Option<RegionMasterKey>,
    ) -> Self {
        let dek_cache = DekCache::new(dek_cache_capacity);
        let rmk_cache = RmkCache::new();
        if let Some(key) = rmk {
            rmk_cache.insert(key);
        }
        Self { inner, enabled, sidecar, dek_cache, rmk_cache, key_manager: None, region: None }
    }

    /// Creates a pass-through (unencrypted) backend.
    pub fn passthrough(inner: B) -> Self {
        Self {
            inner,
            enabled: false,
            sidecar: CryptoSidecar::new_memory(),
            dek_cache: DekCache::new(0),
            rmk_cache: RmkCache::new(),
            key_manager: None,
            region: None,
        }
    }

    /// Attaches a key manager for lazy-load-on-miss of RMK versions.
    ///
    /// When a read encounters an `rmk_version` not in cache, the backend
    /// fetches it from the key manager, caches it, and retries. Without
    /// a key manager, unknown versions produce an error.
    pub fn with_key_manager(mut self, manager: Arc<dyn RegionKeyManager>, region: Region) -> Self {
        self.key_manager = Some(manager);
        self.region = Some(region);
        self
    }

    /// Returns a reference to the inner backend.
    pub fn inner(&self) -> &B {
        &self.inner
    }

    /// Returns a mutable reference to the inner backend.
    pub fn inner_mut(&mut self) -> &mut B {
        &mut self.inner
    }

    /// Returns a reference to the DEK cache (for testing/metrics).
    pub fn dek_cache(&self) -> &DekCache {
        &self.dek_cache
    }

    /// Returns a reference to the RMK cache.
    pub fn rmk_cache(&self) -> &RmkCache {
        &self.rmk_cache
    }

    /// Returns a reference to the sidecar.
    pub fn sidecar(&self) -> &CryptoSidecar {
        &self.sidecar
    }

    /// Whether encryption is active.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Re-wraps a single page's DEK from an old RMK version to the target RMK.
    ///
    /// This is a metadata-only operation — the encrypted page body is not
    /// touched. Only `rmk_version` and `wrapped_dek` in the sidecar are
    /// updated.
    ///
    /// Returns `true` if the page was re-wrapped, `false` if it already
    /// uses the target version or has no metadata.
    pub fn rewrap_page(&self, page_id: u64, target_rmk: &RegionMasterKey) -> Result<bool> {
        let meta = match self.sidecar.read(page_id)? {
            Some(m) => m,
            None => return Ok(false), // No metadata — unencrypted or free page
        };

        // Already using the target version — no-op
        if meta.rmk_version == target_rmk.version {
            return Ok(false);
        }

        // Unwrap DEK with old RMK
        let old_rmk = self.rmk_by_version(meta.rmk_version)?;
        let dek = unwrap_dek(&meta.wrapped_dek, &old_rmk).map_err(|_| Error::Encryption {
            reason: format!(
                "Failed to unwrap DEK for page {page_id} (RMK version {})",
                meta.rmk_version
            )
            .into(),
        })?;

        // Re-wrap with target RMK
        let new_wrapped = wrap_dek(&dek, target_rmk)?;

        // Update sidecar — only rmk_version and wrapped_dek change
        let updated_meta = CryptoMetadata {
            rmk_version: target_rmk.version,
            wrapped_dek: new_wrapped.clone(),
            nonce: meta.nonce,
            auth_tag: meta.auth_tag,
        };
        self.sidecar.write(page_id, &updated_meta)?;

        // Update DEK cache with new wrapped key
        self.dek_cache.insert(new_wrapped, dek);

        Ok(true)
    }

    /// Re-wraps a batch of pages to the target RMK version.
    ///
    /// Scans pages starting from `start_page_id` up to `batch_size` pages.
    /// Returns `(pages_rewrapped, next_page_id)` where `next_page_id` is
    /// `None` if all pages have been processed.
    pub fn rewrap_batch(
        &self,
        start_page_id: u64,
        batch_size: usize,
        target_rmk: &RegionMasterKey,
    ) -> Result<(usize, Option<u64>)> {
        let total_pages = self.sidecar.page_count()?;
        let mut rewrapped = 0;
        let mut page_id = start_page_id;

        for _ in 0..batch_size {
            if page_id >= total_pages {
                return Ok((rewrapped, None)); // Done — all pages processed
            }
            if self.rewrap_page(page_id, target_rmk)? {
                rewrapped += 1;
            }
            page_id += 1;
        }

        let next = if page_id >= total_pages { None } else { Some(page_id) };
        Ok((rewrapped, next))
    }

    /// Returns the current (highest-version) RMK.
    ///
    /// If no RMK is cached and a key manager is attached, loads the
    /// current version from the key manager.
    fn current_rmk(&self) -> Result<Arc<RegionMasterKey>> {
        if let Some(cached) = self.rmk_cache.current() {
            return Ok(cached);
        }

        // Lazy load from key manager
        if let (Some(manager), Some(region)) = (&self.key_manager, self.region) {
            let rmk = manager.current_rmk(region)?;
            self.rmk_cache.insert(rmk);
            return self
                .rmk_cache
                .current()
                .ok_or_else(|| Error::Encryption { reason: "Failed to cache loaded RMK".into() });
        }

        Err(Error::Encryption { reason: "No Region Master Key loaded".into() })
    }

    /// Looks up an RMK by version, lazy-loading from the key manager on cache miss.
    fn rmk_by_version(&self, version: u32) -> Result<Arc<RegionMasterKey>> {
        if let Some(cached) = self.rmk_cache.get(version) {
            return Ok(cached);
        }

        // Lazy load from key manager
        if let (Some(manager), Some(region)) = (&self.key_manager, self.region) {
            let rmk = manager.rmk_by_version(region, version)?;
            self.rmk_cache.insert(rmk);
            return self.rmk_cache.get(version).ok_or_else(|| Error::Encryption {
                reason: format!("Failed to cache loaded RMK version {version}").into(),
            });
        }

        Err(Error::Encryption {
            reason: format!("RMK version {version} not found (may have been decommissioned)")
                .into(),
        })
    }
}

impl<B: StorageBackend> StorageBackend for EncryptedBackend<B> {
    fn read_header(&self) -> Result<Vec<u8>> {
        // Header is never encrypted
        self.inner.read_header()
    }

    fn write_header(&self, header: &[u8]) -> Result<()> {
        // Header is never encrypted
        self.inner.write_header(header)
    }

    fn read_page(&self, page_id: PageId) -> Result<Vec<u8>> {
        let raw = self.inner.read_page(page_id)?;

        if !self.enabled {
            return Ok(raw);
        }

        // Check if this page has crypto metadata
        let meta = match self.sidecar.read(page_id)? {
            Some(m) => m,
            None => return Ok(raw), // Unencrypted page (pre-encryption or free page)
        };

        if raw.len() < PAGE_HEADER_SIZE {
            return Err(Error::Corrupted {
                reason: format!("Page {page_id} too small for header: {} bytes", raw.len()).into(),
            });
        }

        // Split into header (AAD) and ciphertext body
        let (header, ciphertext) = raw.split_at(PAGE_HEADER_SIZE);

        // Look up DEK in cache, or unwrap from sidecar
        let dek = match self.dek_cache.get(&meta.wrapped_dek) {
            Some(cached) => cached,
            None => {
                let rmk = self.rmk_by_version(meta.rmk_version)?;
                let dek = unwrap_dek(&meta.wrapped_dek, &rmk)
                    .map_err(|_| Error::AuthenticationFailed { page_id })?;
                // Cache for future reads
                self.dek_cache.insert(meta.wrapped_dek.clone(), dek.clone());
                Arc::new(dek)
            },
        };

        // Decrypt the body
        let plaintext = decrypt_page_body(ciphertext, &meta.nonce, header, &meta.auth_tag, &dek)
            .map_err(|_| Error::AuthenticationFailed { page_id })?;

        // Reassemble header + plaintext body
        let mut result = Vec::with_capacity(header.len() + plaintext.len());
        result.extend_from_slice(header);
        result.extend_from_slice(&plaintext);
        Ok(result)
    }

    fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        if !self.enabled {
            return self.inner.write_page(page_id, data);
        }

        if data.len() < PAGE_HEADER_SIZE {
            return Err(Error::Corrupted {
                reason: format!("Page {page_id} data too small for header: {} bytes", data.len())
                    .into(),
            });
        }

        // Split into header (AAD) and plaintext body
        let (header, body) = data.split_at(PAGE_HEADER_SIZE);

        // Generate fresh DEK for this page
        let dek = generate_dek();
        let rmk = self.current_rmk()?;

        // Encrypt body with DEK, using header as AAD
        let (ciphertext, nonce, auth_tag) = encrypt_page_body(body, header, &dek)?;

        // Wrap DEK with current RMK
        let wrapped_dek = wrap_dek(&dek, &rmk)?;

        // Store crypto metadata in sidecar
        let meta = CryptoMetadata {
            rmk_version: rmk.version,
            wrapped_dek: wrapped_dek.clone(),
            nonce,
            auth_tag,
        };
        self.sidecar.write(page_id, &meta)?;

        // Cache the DEK for read-back
        self.dek_cache.insert(wrapped_dek, dek);

        // Write header + ciphertext to inner backend
        let mut encrypted_page = Vec::with_capacity(header.len() + ciphertext.len());
        encrypted_page.extend_from_slice(header);
        encrypted_page.extend_from_slice(&ciphertext);
        self.inner.write_page(page_id, &encrypted_page)
    }

    fn sync(&self) -> Result<()> {
        self.sidecar.sync()?;
        self.inner.sync()
    }

    fn evict_page_cache(&self) -> Result<()> {
        // Evict both the encrypted page file and the per-page crypto-
        // metadata sidecar so the entire on-disk footprint of this
        // backend stops pinning page-cache memory.
        self.sidecar.evict_page_cache()?;
        self.inner.evict_page_cache()
    }

    fn file_size(&self) -> Result<u64> {
        self.inner.file_size()
    }

    fn extend(&self, new_size: u64) -> Result<()> {
        self.inner.extend(new_size)
    }

    fn page_size(&self) -> usize {
        self.inner.page_size()
    }

    fn rewrap_pages(
        &self,
        start_page_id: u64,
        batch_size: usize,
        target_version: Option<u32>,
    ) -> Result<(usize, Option<u64>)> {
        if !self.enabled {
            return Ok((0, None));
        }

        let target_rmk = match target_version {
            Some(v) => self.rmk_by_version(v)?,
            None => self.current_rmk()?,
        };

        self.rewrap_batch(start_page_id, batch_size, &target_rmk)
    }

    fn sidecar_page_count(&self) -> Result<u64> {
        if !self.enabled {
            return Ok(0);
        }
        self.sidecar.page_count()
    }
}

impl<B: StorageBackend + std::fmt::Debug> std::fmt::Debug for EncryptedBackend<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedBackend")
            .field("enabled", &self.enabled)
            .field("inner", &self.inner)
            .field("dek_cache", &self.dek_cache)
            .field("rmk_cache", &self.rmk_cache)
            .field("sidecar", &self.sidecar)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::InMemoryBackend;

    const PAGE_SIZE: usize = 4096;

    fn test_rmk() -> RegionMasterKey {
        RegionMasterKey::new(1, [0xAA; 32])
    }

    /// Creates a test page: 16-byte header + body filled with the given byte.
    fn make_page(fill: u8) -> Vec<u8> {
        let mut page = vec![0u8; PAGE_SIZE];
        // Page header: type=2 (BTreeLeaf), flags=0, item_count=0, checksum=0, txn_id=0
        page[0] = 2;
        // Fill body with test data
        for byte in &mut page[PAGE_HEADER_SIZE..] {
            *byte = fill;
        }
        page
    }

    fn encrypted_backend() -> EncryptedBackend<InMemoryBackend> {
        let inner = InMemoryBackend::new();
        EncryptedBackend::new(inner, true, CryptoSidecar::new_memory(), 1024, Some(test_rmk()))
    }

    fn passthrough_backend() -> EncryptedBackend<InMemoryBackend> {
        let inner = InMemoryBackend::new();
        EncryptedBackend::passthrough(inner)
    }

    // --- Encrypt/decrypt round-trip ---

    #[test]
    fn test_write_read_roundtrip() {
        let backend = encrypted_backend();
        let page = make_page(0x42);

        // Pre-allocate space (InMemoryBackend needs extend)
        backend.extend(PAGE_SIZE as u64 * 2).unwrap();

        backend.write_page(0, &page).unwrap();
        let read = backend.read_page(0).unwrap();

        assert_eq!(read, page, "Decrypted page should match original");
    }

    #[test]
    fn test_write_read_multiple_pages() {
        let backend = encrypted_backend();
        backend.extend(PAGE_SIZE as u64 * 10).unwrap();

        let page0 = make_page(0xAA);
        let page1 = make_page(0xBB);
        let page5 = make_page(0xCC);

        backend.write_page(0, &page0).unwrap();
        backend.write_page(1, &page1).unwrap();
        backend.write_page(5, &page5).unwrap();

        assert_eq!(backend.read_page(0).unwrap(), page0);
        assert_eq!(backend.read_page(1).unwrap(), page1);
        assert_eq!(backend.read_page(5).unwrap(), page5);
    }

    // --- Passthrough (disabled) ---

    #[test]
    fn test_passthrough_no_encryption() {
        let backend = passthrough_backend();
        backend.extend(PAGE_SIZE as u64 * 2).unwrap();

        let page = make_page(0x42);
        backend.write_page(0, &page).unwrap();

        // Read directly from inner — should be identical (no encryption)
        let inner_read = backend.inner().read_page(0).unwrap();
        assert_eq!(inner_read, page);

        // Read through backend — also identical
        let read = backend.read_page(0).unwrap();
        assert_eq!(read, page);
    }

    // --- Data actually encrypted on disk ---

    #[test]
    fn test_data_encrypted_on_disk() {
        let backend = encrypted_backend();
        backend.extend(PAGE_SIZE as u64 * 2).unwrap();

        let page = make_page(0x42);
        backend.write_page(0, &page).unwrap();

        // Read raw from inner backend — body should be encrypted (different)
        let raw = backend.inner().read_page(0).unwrap();
        // Header should be preserved (AAD, not encrypted)
        assert_eq!(&raw[..PAGE_HEADER_SIZE], &page[..PAGE_HEADER_SIZE]);
        // Body should be encrypted (different from plaintext)
        assert_ne!(&raw[PAGE_HEADER_SIZE..], &page[PAGE_HEADER_SIZE..]);
    }

    // --- Wrong RMK ---

    #[test]
    fn test_read_with_wrong_rmk_fails() {
        let inner = InMemoryBackend::new();
        let rmk1 = RegionMasterKey::new(1, [0xAA; 32]);
        let backend1 =
            EncryptedBackend::new(inner, true, CryptoSidecar::new_memory(), 1024, Some(rmk1));
        backend1.extend(PAGE_SIZE as u64 * 2).unwrap();

        // Write with RMK v1
        let page = make_page(0x42);
        backend1.write_page(0, &page).unwrap();

        // Replace RMK with a different key — read should fail
        backend1.rmk_cache().purge();
        backend1.rmk_cache().insert(RegionMasterKey::new(1, [0xBB; 32]));
        // Clear DEK cache so it tries to unwrap with wrong RMK
        backend1.dek_cache().invalidate_all();

        let result = backend1.read_page(0);
        assert!(result.is_err());
    }

    // --- DEK uniqueness per page ---

    #[test]
    fn test_each_page_gets_unique_dek() {
        let backend = encrypted_backend();
        backend.extend(PAGE_SIZE as u64 * 4).unwrap();

        let page = make_page(0x42);
        backend.write_page(0, &page).unwrap();
        backend.write_page(1, &page).unwrap();

        // Read sidecar metadata for both pages — wrapped DEKs should differ
        let meta0 = backend.sidecar().read(0).unwrap().unwrap();
        let meta1 = backend.sidecar().read(1).unwrap().unwrap();
        assert_ne!(meta0.wrapped_dek, meta1.wrapped_dek);
    }

    // --- Rewrite produces new DEK ---

    #[test]
    fn test_rewrite_page_produces_new_dek() {
        let backend = encrypted_backend();
        backend.extend(PAGE_SIZE as u64 * 2).unwrap();

        let page = make_page(0x42);
        backend.write_page(0, &page).unwrap();
        let meta_first = backend.sidecar().read(0).unwrap().unwrap();

        backend.write_page(0, &page).unwrap();
        let meta_second = backend.sidecar().read(0).unwrap().unwrap();

        // Fresh DEK on each write
        assert_ne!(meta_first.wrapped_dek, meta_second.wrapped_dek);
    }

    // --- DekCache hit avoids unwrap ---

    #[test]
    fn test_dek_cache_hit_avoids_rmk_lookup() {
        let backend = encrypted_backend();
        backend.extend(PAGE_SIZE as u64 * 2).unwrap();

        let page = make_page(0x42);
        backend.write_page(0, &page).unwrap();

        // Write populates DEK cache. Remove RMK to prove cache is used.
        backend.rmk_cache().purge();

        // Should still succeed via DEK cache hit
        let read = backend.read_page(0).unwrap();
        assert_eq!(read, page);
    }

    // --- Header is preserved as AAD ---

    #[test]
    fn test_header_preserved_as_aad() {
        let backend = encrypted_backend();
        backend.extend(PAGE_SIZE as u64 * 2).unwrap();

        let mut page = make_page(0x42);
        // Set a distinctive header
        page[0] = 2; // BTreeLeaf
        page[1] = 0xFF; // flags
        page[2] = 0x01; // item_count lo
        page[3] = 0x00; // item_count hi

        backend.write_page(0, &page).unwrap();
        let read = backend.read_page(0).unwrap();

        // Header bytes must match exactly
        assert_eq!(&read[..PAGE_HEADER_SIZE], &page[..PAGE_HEADER_SIZE]);
    }

    // --- AAD tamper detection ---

    #[test]
    fn test_tampered_header_detected() {
        let backend = encrypted_backend();
        backend.extend(PAGE_SIZE as u64 * 2).unwrap();

        let page = make_page(0x42);
        backend.write_page(0, &page).unwrap();

        // Tamper with the header in the inner backend (simulates corruption)
        let mut raw = backend.inner().read_page(0).unwrap();
        raw[0] ^= 0xFF; // Flip page type byte
        backend.inner().write_page(0, &raw).unwrap();

        // Clear DEK cache to force decrypt with tampered AAD
        backend.dek_cache().invalidate_all();

        let result = backend.read_page(0);
        assert!(result.is_err(), "Tampered header should cause authentication failure");
    }

    // --- Sync cascades ---

    #[test]
    fn test_sync_cascades() {
        let backend = encrypted_backend();
        // Just verify it doesn't panic
        backend.sync().unwrap();
    }

    // --- Page size preserved ---

    #[test]
    fn test_page_size_preserved() {
        let backend = encrypted_backend();
        assert_eq!(backend.page_size(), PAGE_SIZE);
    }

    // --- Header passthrough ---

    #[test]
    fn test_header_not_encrypted() {
        let backend = encrypted_backend();
        let header = vec![0xDE; 768]; // HEADER_SIZE
        backend.write_header(&header).unwrap();

        let read = backend.read_header().unwrap();
        assert_eq!(read, header);
    }

    // --- Lazy-load-on-miss ---

    /// Test key manager that returns a fixed RMK for any version.
    struct TestKeyManager {
        rmk: RegionMasterKey,
    }

    impl RegionKeyManager for TestKeyManager {
        fn current_rmk(&self, _region: Region) -> crate::error::Result<RegionMasterKey> {
            Ok(RegionMasterKey::new(self.rmk.version, *self.rmk.as_bytes()))
        }

        fn rmk_by_version(
            &self,
            _region: Region,
            version: u32,
        ) -> crate::error::Result<RegionMasterKey> {
            if version == self.rmk.version {
                Ok(RegionMasterKey::new(version, *self.rmk.as_bytes()))
            } else {
                Err(Error::Encryption { reason: format!("Unknown version {version}").into() })
            }
        }

        fn list_versions(
            &self,
            _region: Region,
        ) -> crate::error::Result<Vec<inferadb_ledger_types::config::RmkVersionInfo>> {
            Ok(Vec::new())
        }

        fn rotate_rmk(&self, _region: Region) -> crate::error::Result<u32> {
            Err(Error::Encryption { reason: "not supported".into() })
        }

        fn decommission_rmk(&self, _region: Region, _version: u32) -> crate::error::Result<()> {
            Err(Error::Encryption { reason: "not supported".into() })
        }

        fn health_check(&self, _region: Region) -> crate::error::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_lazy_load_rmk_on_cache_miss() {
        let rmk = test_rmk();
        let manager =
            Arc::new(TestKeyManager { rmk: RegionMasterKey::new(rmk.version, *rmk.as_bytes()) });

        // Create backend with key manager but NO initial RMK in cache
        let inner = InMemoryBackend::new();
        let backend = EncryptedBackend::new(inner, true, CryptoSidecar::new_memory(), 1024, None)
            .with_key_manager(manager, Region::US_EAST_VA);

        // current_rmk should lazy-load from the key manager
        let loaded = backend.current_rmk().unwrap();
        assert_eq!(loaded.version, 1);
    }

    #[test]
    fn test_lazy_load_rmk_by_version_on_cache_miss() {
        let rmk = test_rmk();
        let manager =
            Arc::new(TestKeyManager { rmk: RegionMasterKey::new(rmk.version, *rmk.as_bytes()) });

        let inner = InMemoryBackend::new();
        let backend =
            EncryptedBackend::new(inner, true, CryptoSidecar::new_memory(), 1024, Some(rmk))
                .with_key_manager(manager, Region::US_EAST_VA);

        // Write a page (uses RMK v1 from cache)
        backend.extend(PAGE_SIZE as u64 * 2).unwrap();
        let page = make_page(0x42);
        backend.write_page(0, &page).unwrap();

        // Purge caches to simulate restart
        backend.rmk_cache().purge();
        backend.dek_cache().invalidate_all();

        // Read should lazy-load RMK v1 from key manager
        let read = backend.read_page(0).unwrap();
        assert_eq!(read, page);
    }

    #[test]
    fn test_lazy_load_fails_without_key_manager() {
        let inner = InMemoryBackend::new();
        let backend = EncryptedBackend::new(inner, true, CryptoSidecar::new_memory(), 1024, None);

        // No key manager, no cached RMK — should fail
        let result = backend.current_rmk();
        assert!(result.is_err());
    }

    // --- Re-wrapping ---

    #[test]
    fn test_rewrap_page_header_only() {
        let rmk_v1 = RegionMasterKey::new(1, [0xAA; 32]);
        let rmk_v2 = RegionMasterKey::new(2, [0xBB; 32]);
        let backend = EncryptedBackend::new(
            InMemoryBackend::new(),
            true,
            CryptoSidecar::new_memory(),
            1024,
            Some(RegionMasterKey::new(1, [0xAA; 32])),
        );
        backend.extend(PAGE_SIZE as u64 * 4).unwrap();

        // Write pages with RMK v1
        let page0 = make_page(0x42);
        let page1 = make_page(0x99);
        backend.write_page(0, &page0).unwrap();
        backend.write_page(1, &page1).unwrap();

        // Capture raw ciphertext before re-wrap
        let raw_before_0 = backend.inner().read_page(0).unwrap();
        let raw_before_1 = backend.inner().read_page(1).unwrap();

        // Add RMK v2 to cache and re-wrap
        backend.rmk_cache().insert(RegionMasterKey::new(2, [0xBB; 32]));

        assert!(backend.rewrap_page(0, &rmk_v2).unwrap());
        assert!(backend.rewrap_page(1, &rmk_v2).unwrap());

        // Raw ciphertext must be IDENTICAL (header-only rewrite)
        let raw_after_0 = backend.inner().read_page(0).unwrap();
        let raw_after_1 = backend.inner().read_page(1).unwrap();
        assert_eq!(raw_before_0, raw_after_0, "Page body must not change during re-wrap");
        assert_eq!(raw_before_1, raw_after_1, "Page body must not change during re-wrap");

        // Sidecar metadata should now reference v2
        let meta0 = backend.sidecar().read(0).unwrap().unwrap();
        assert_eq!(meta0.rmk_version, 2);
        let meta1 = backend.sidecar().read(1).unwrap().unwrap();
        assert_eq!(meta1.rmk_version, 2);

        // Clear DEK cache, add RMK v2 — pages still readable
        backend.dek_cache().invalidate_all();
        backend.rmk_cache().purge();
        backend.rmk_cache().insert(rmk_v2);

        assert_eq!(backend.read_page(0).unwrap(), page0);
        assert_eq!(backend.read_page(1).unwrap(), page1);

        // Old RMK v1 no longer needed for reading
        let _ = rmk_v1;
    }

    #[test]
    fn test_rewrap_idempotent() {
        let rmk_v2 = RegionMasterKey::new(2, [0xBB; 32]);
        let backend = EncryptedBackend::new(
            InMemoryBackend::new(),
            true,
            CryptoSidecar::new_memory(),
            1024,
            Some(RegionMasterKey::new(1, [0xAA; 32])),
        );
        backend.extend(PAGE_SIZE as u64 * 2).unwrap();
        backend.write_page(0, &make_page(0x42)).unwrap();

        backend.rmk_cache().insert(RegionMasterKey::new(2, [0xBB; 32]));

        // First re-wrap changes it
        assert!(backend.rewrap_page(0, &rmk_v2).unwrap());
        // Second re-wrap is a no-op (already v2)
        assert!(!backend.rewrap_page(0, &rmk_v2).unwrap());
    }

    #[test]
    fn test_rewrap_no_metadata_returns_false() {
        let backend = encrypted_backend();
        let rmk_v2 = RegionMasterKey::new(2, [0xBB; 32]);
        // Page 99 was never written — no metadata
        assert!(!backend.rewrap_page(99, &rmk_v2).unwrap());
    }

    #[test]
    fn test_rewrap_batch_processes_pages() {
        let backend = EncryptedBackend::new(
            InMemoryBackend::new(),
            true,
            CryptoSidecar::new_memory(),
            1024,
            Some(RegionMasterKey::new(1, [0xAA; 32])),
        );
        backend.extend(PAGE_SIZE as u64 * 10).unwrap();

        // Write 5 pages
        for i in 0..5 {
            backend.write_page(i, &make_page(i as u8)).unwrap();
        }

        let rmk_v2 = RegionMasterKey::new(2, [0xBB; 32]);
        backend.rmk_cache().insert(RegionMasterKey::new(2, [0xBB; 32]));

        // Re-wrap first 3 pages
        let (rewrapped, next) = backend.rewrap_batch(0, 3, &rmk_v2).unwrap();
        assert_eq!(rewrapped, 3);
        assert_eq!(next, Some(3));

        // Re-wrap remaining pages
        let (rewrapped, next) = backend.rewrap_batch(3, 10, &rmk_v2).unwrap();
        assert_eq!(rewrapped, 2); // pages 3 and 4
        assert!(next.is_none()); // Done
    }

    #[test]
    fn test_rewrap_batch_empty_sidecar() {
        let backend = encrypted_backend();
        let rmk_v2 = RegionMasterKey::new(2, [0xBB; 32]);
        let (rewrapped, next) = backend.rewrap_batch(0, 100, &rmk_v2).unwrap();
        assert_eq!(rewrapped, 0);
        assert!(next.is_none());
    }
}
