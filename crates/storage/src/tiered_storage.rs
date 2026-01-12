//! Tiered snapshot storage.
//!
//! Per DESIGN.md §4.4:
//! - Hot: Last N snapshots on local SSD (fast access)
//! - Warm: Older snapshots on object storage (S3)
//! - Cold: Archive snapshots (Glacier, optional)
//!
//! This module provides:
//! - Storage backend trait for abstracting local/remote storage
//! - Tiered manager that moves snapshots between tiers
//! - Transparent loading from any tier

use std::collections::HashMap;
use std::path::PathBuf;

use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};

use crate::snapshot::{Snapshot, SnapshotError, SnapshotManager, snapshot_filename};

/// Storage tier for snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageTier {
    /// Local SSD storage (fastest, limited capacity).
    Hot,
    /// Object storage like S3 (moderate latency, high capacity).
    Warm,
    /// Archive storage like Glacier (high latency, lowest cost).
    Cold,
}

/// Error types for tiered storage operations.
#[derive(Debug, Snafu)]
pub enum TieredStorageError {
    /// Error from underlying snapshot operations.
    #[snafu(display("Snapshot error: {source}"))]
    Snapshot {
        /// The underlying snapshot error.
        source: SnapshotError,
    },

    /// The requested tier is not configured.
    #[snafu(display("Tier not available: {tier:?}"))]
    TierNotAvailable {
        /// The tier that was requested.
        tier: StorageTier,
    },

    /// No snapshot exists at the requested height.
    #[snafu(display("Snapshot not found at height {height}"))]
    SnapshotNotFound {
        /// The height that was requested.
        height: u64,
    },

    /// IO error during file operations.
    #[snafu(display("IO error: {source}"))]
    Io {
        /// The underlying IO error.
        source: std::io::Error,
    },

    /// Error from object storage operations.
    #[snafu(display("Object storage error: {message}"))]
    ObjectStorage {
        /// Description of the error.
        message: String,
    },
}

/// Result type for tiered storage operations.
pub type Result<T> = std::result::Result<T, TieredStorageError>;

/// Trait for storage backends that can store and retrieve snapshots.
pub trait StorageBackend: Send + Sync {
    /// The tier this backend represents.
    fn tier(&self) -> StorageTier;

    /// Check if a snapshot exists at the given height.
    fn exists(&self, height: u64) -> Result<bool>;

    /// Store a snapshot.
    fn store(&self, snapshot: &Snapshot) -> Result<()>;

    /// Load a snapshot by height.
    fn load(&self, height: u64) -> Result<Snapshot>;

    /// Delete a snapshot.
    fn delete(&self, height: u64) -> Result<()>;

    /// List all available snapshot heights.
    fn list(&self) -> Result<Vec<u64>>;
}

/// Local filesystem storage backend (Hot tier).
pub struct LocalBackend {
    manager: SnapshotManager,
}

impl LocalBackend {
    /// Create a new local storage backend.
    pub fn new(snapshot_dir: PathBuf, max_snapshots: usize) -> Self {
        Self {
            manager: SnapshotManager::new(snapshot_dir, max_snapshots),
        }
    }
}

impl StorageBackend for LocalBackend {
    fn tier(&self) -> StorageTier {
        StorageTier::Hot
    }

    fn exists(&self, height: u64) -> Result<bool> {
        let snapshots = self.manager.list_snapshots().context(SnapshotSnafu)?;
        Ok(snapshots.contains(&height))
    }

    fn store(&self, snapshot: &Snapshot) -> Result<()> {
        self.manager.save(snapshot).context(SnapshotSnafu)?;
        Ok(())
    }

    fn load(&self, height: u64) -> Result<Snapshot> {
        self.manager.load(height).context(SnapshotSnafu)
    }

    fn delete(&self, height: u64) -> Result<()> {
        let path = self
            .manager
            .snapshot_dir()
            .join(snapshot_filename(height));
        if path.exists() {
            std::fs::remove_file(&path).context(IoSnafu)?;
        }
        Ok(())
    }

    fn list(&self) -> Result<Vec<u64>> {
        self.manager.list_snapshots().context(SnapshotSnafu)
    }
}

/// Object storage backend stub (Warm tier).
///
/// This is a placeholder for S3/compatible object storage.
/// The actual implementation would use the AWS SDK or similar.
pub struct ObjectStorageBackend {
    /// Bucket name or path.
    #[allow(dead_code)]
    bucket: String,
    /// Prefix for snapshot objects.
    #[allow(dead_code)]
    prefix: String,
    /// Simulated storage for testing.
    #[cfg(test)]
    test_storage: std::sync::Arc<RwLock<HashMap<u64, Vec<u8>>>>,
}

impl ObjectStorageBackend {
    /// Create a new object storage backend.
    pub fn new(bucket: String, prefix: String) -> Self {
        Self {
            bucket,
            prefix,
            #[cfg(test)]
            test_storage: std::sync::Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Object key for a snapshot.
    #[allow(dead_code)]
    fn object_key(&self, height: u64) -> String {
        format!("{}/{}", self.prefix, snapshot_filename(height))
    }
}

impl StorageBackend for ObjectStorageBackend {
    fn tier(&self) -> StorageTier {
        StorageTier::Warm
    }

    fn exists(&self, height: u64) -> Result<bool> {
        #[cfg(test)]
        {
            return Ok(self.test_storage.read().contains_key(&height));
        }

        #[cfg(not(test))]
        {
            // In production, this would call S3 HeadObject
            // For now, return false (not implemented)
            let _ = height;
            Ok(false)
        }
    }

    fn store(&self, snapshot: &Snapshot) -> Result<()> {
        #[cfg(test)]
        {
            // For testing, serialize to memory via a temp file
            let temp_dir = tempfile::TempDir::new().map_err(|e| TieredStorageError::Io {
                source: e,
            })?;
            let temp_path = temp_dir.path().join("temp.snap");
            snapshot
                .write_to_file(&temp_path)
                .context(SnapshotSnafu)?;
            let buf = std::fs::read(&temp_path).context(IoSnafu)?;
            self.test_storage
                .write()
                .insert(snapshot.shard_height(), buf);
            return Ok(());
        }

        #[cfg(not(test))]
        {
            // In production, this would upload to S3
            let _ = snapshot;
            Err(TieredStorageError::ObjectStorage {
                message: "Object storage upload not implemented".to_string(),
            })
        }
    }

    fn load(&self, height: u64) -> Result<Snapshot> {
        #[cfg(test)]
        {
            let guard = self.test_storage.read();
            let data = guard.get(&height).ok_or(TieredStorageError::SnapshotNotFound { height })?;

            // Write to temp file and read back as Snapshot
            let temp_dir = tempfile::TempDir::new().map_err(|e| TieredStorageError::Io {
                source: e,
            })?;
            let temp_path = temp_dir.path().join("temp.snap");
            std::fs::write(&temp_path, data).context(IoSnafu)?;
            return Snapshot::read_from_file(&temp_path).context(SnapshotSnafu);
        }

        #[cfg(not(test))]
        {
            // In production, this would download from S3
            let _ = height;
            Err(TieredStorageError::ObjectStorage {
                message: "Object storage download not implemented".to_string(),
            })
        }
    }

    fn delete(&self, height: u64) -> Result<()> {
        #[cfg(test)]
        {
            self.test_storage.write().remove(&height);
            return Ok(());
        }

        #[cfg(not(test))]
        {
            // In production, this would call S3 DeleteObject
            let _ = height;
            Err(TieredStorageError::ObjectStorage {
                message: "Object storage delete not implemented".to_string(),
            })
        }
    }

    fn list(&self) -> Result<Vec<u64>> {
        #[cfg(test)]
        {
            let guard = self.test_storage.read();
            let mut heights: Vec<u64> = guard.keys().copied().collect();
            heights.sort_unstable();
            return Ok(heights);
        }

        #[cfg(not(test))]
        {
            // In production, this would call S3 ListObjects
            Err(TieredStorageError::ObjectStorage {
                message: "Object storage list not implemented".to_string(),
            })
        }
    }
}

/// Snapshot location tracking.
#[derive(Debug, Clone)]
pub struct SnapshotLocation {
    /// Height of the snapshot.
    pub height: u64,
    /// Tier where the snapshot is stored.
    pub tier: StorageTier,
    /// When the snapshot was created.
    pub created_at: u64,
}

/// Configuration for tiered storage.
#[derive(Debug, Clone)]
pub struct TieredConfig {
    /// Number of snapshots to keep in hot tier.
    pub hot_count: usize,
    /// Number of days to keep in warm tier before moving to cold.
    pub warm_days: u32,
    /// Whether cold tier is enabled.
    pub cold_enabled: bool,
}

impl Default for TieredConfig {
    fn default() -> Self {
        Self {
            hot_count: 3,
            warm_days: 30,
            cold_enabled: false,
        }
    }
}

/// Tiered snapshot manager.
///
/// Manages snapshots across multiple storage tiers with automatic
/// promotion/demotion based on age and access patterns.
pub struct TieredSnapshotManager {
    /// Hot tier (local SSD).
    hot: Box<dyn StorageBackend>,
    /// Warm tier (object storage), optional.
    warm: Option<Box<dyn StorageBackend>>,
    /// Cold tier (archive), optional.
    cold: Option<Box<dyn StorageBackend>>,
    /// Configuration.
    config: TieredConfig,
    /// Location cache.
    locations: RwLock<HashMap<u64, StorageTier>>,
}

impl TieredSnapshotManager {
    /// Create a new tiered manager with only hot tier.
    pub fn new_hot_only(hot: Box<dyn StorageBackend>, config: TieredConfig) -> Self {
        Self {
            hot,
            warm: None,
            cold: None,
            config,
            locations: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new tiered manager with hot and warm tiers.
    pub fn new_with_warm(
        hot: Box<dyn StorageBackend>,
        warm: Box<dyn StorageBackend>,
        config: TieredConfig,
    ) -> Self {
        Self {
            hot,
            warm: Some(warm),
            cold: None,
            config,
            locations: RwLock::new(HashMap::new()),
        }
    }

    /// Store a snapshot in the hot tier.
    pub fn store(&self, snapshot: &Snapshot) -> Result<()> {
        let height = snapshot.shard_height();
        self.hot.store(snapshot)?;
        self.locations.write().insert(height, StorageTier::Hot);
        Ok(())
    }

    /// Load a snapshot from any tier.
    ///
    /// Tries hot tier first, then warm, then cold.
    pub fn load(&self, height: u64) -> Result<Snapshot> {
        // Check cache first
        if let Some(tier) = self.locations.read().get(&height).copied() {
            return self.load_from_tier(height, tier);
        }

        // Try each tier
        if self.hot.exists(height)? {
            self.locations.write().insert(height, StorageTier::Hot);
            return self.hot.load(height);
        }

        if let Some(ref warm) = self.warm {
            if warm.exists(height)? {
                self.locations.write().insert(height, StorageTier::Warm);
                return warm.load(height);
            }
        }

        if let Some(ref cold) = self.cold {
            if cold.exists(height)? {
                self.locations.write().insert(height, StorageTier::Cold);
                return cold.load(height);
            }
        }

        Err(TieredStorageError::SnapshotNotFound { height })
    }

    /// Load from a specific tier.
    fn load_from_tier(&self, height: u64, tier: StorageTier) -> Result<Snapshot> {
        match tier {
            StorageTier::Hot => self.hot.load(height),
            StorageTier::Warm => self
                .warm
                .as_ref()
                .ok_or(TieredStorageError::TierNotAvailable { tier })?
                .load(height),
            StorageTier::Cold => self
                .cold
                .as_ref()
                .ok_or(TieredStorageError::TierNotAvailable { tier })?
                .load(height),
        }
    }

    /// List all snapshots across all tiers.
    pub fn list_all(&self) -> Result<Vec<SnapshotLocation>> {
        let mut locations = Vec::new();

        // Hot tier
        for height in self.hot.list()? {
            locations.push(SnapshotLocation {
                height,
                tier: StorageTier::Hot,
                created_at: 0, // Would need metadata for actual timestamp
            });
        }

        // Warm tier
        if let Some(ref warm) = self.warm {
            for height in warm.list()? {
                locations.push(SnapshotLocation {
                    height,
                    tier: StorageTier::Warm,
                    created_at: 0,
                });
            }
        }

        // Cold tier
        if let Some(ref cold) = self.cold {
            for height in cold.list()? {
                locations.push(SnapshotLocation {
                    height,
                    tier: StorageTier::Cold,
                    created_at: 0,
                });
            }
        }

        // Sort by height
        locations.sort_by_key(|l| l.height);
        Ok(locations)
    }

    /// Demote snapshots from hot to warm tier.
    ///
    /// Keeps only `config.hot_count` snapshots in hot tier,
    /// moving older ones to warm tier.
    pub fn demote_to_warm(&self) -> Result<u64> {
        let warm = match &self.warm {
            Some(w) => w,
            None => return Ok(0),
        };

        let hot_snapshots = self.hot.list()?;
        if hot_snapshots.len() <= self.config.hot_count {
            return Ok(0);
        }

        let to_demote = hot_snapshots.len() - self.config.hot_count;
        let mut demoted = 0;

        // Demote oldest snapshots (list is sorted ascending)
        for &height in hot_snapshots.iter().take(to_demote) {
            // Load from hot
            let snapshot = self.hot.load(height)?;

            // Store in warm
            warm.store(&snapshot)?;

            // Delete from hot
            self.hot.delete(height)?;

            // Update location cache
            self.locations.write().insert(height, StorageTier::Warm);

            demoted += 1;
        }

        Ok(demoted)
    }

    /// Find the best snapshot for a given target height.
    ///
    /// Returns the highest snapshot at or below the target height.
    pub fn find_snapshot_for(&self, target_height: u64) -> Result<Option<u64>> {
        let all = self.list_all()?;
        Ok(all
            .into_iter()
            .filter(|l| l.height <= target_height)
            .map(|l| l.height)
            .max())
    }

    /// Get the tier where a snapshot is stored.
    pub fn get_tier(&self, height: u64) -> Result<Option<StorageTier>> {
        if let Some(tier) = self.locations.read().get(&height).copied() {
            return Ok(Some(tier));
        }

        // Check each tier
        if self.hot.exists(height)? {
            return Ok(Some(StorageTier::Hot));
        }

        if let Some(ref warm) = self.warm {
            if warm.exists(height)? {
                return Ok(Some(StorageTier::Warm));
            }
        }

        if let Some(ref cold) = self.cold {
            if cold.exists(height)? {
                return Ok(Some(StorageTier::Cold));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::bucket::NUM_BUCKETS;
    use crate::snapshot::{SnapshotChainParams, SnapshotStateData, VaultSnapshotMeta};
    use ledger_types::EMPTY_HASH;
    use std::collections::HashMap as StdHashMap;
    use tempfile::TempDir;

    fn create_test_snapshot(height: u64) -> Snapshot {
        let vault_states = vec![VaultSnapshotMeta {
            vault_id: 1,
            vault_height: height / 2,
            state_root: [height as u8; 32],
            bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
            key_count: 0,
        }];

        let state = SnapshotStateData {
            vault_entities: StdHashMap::new(),
        };

        Snapshot::new(
            1,
            height,
            vault_states,
            state,
            SnapshotChainParams::default(),
        )
        .expect("create snapshot")
    }

    #[test]
    fn test_local_backend() {
        let temp = TempDir::new().expect("create temp dir");
        let backend = LocalBackend::new(temp.path().join("snapshots"), 10);

        // Initially empty
        assert!(!backend.exists(100).unwrap());
        assert!(backend.list().unwrap().is_empty());

        // Store a snapshot
        let snapshot = create_test_snapshot(100);
        backend.store(&snapshot).unwrap();

        // Now exists
        assert!(backend.exists(100).unwrap());
        assert_eq!(backend.list().unwrap(), vec![100]);

        // Load it back
        let loaded = backend.load(100).unwrap();
        assert_eq!(loaded.shard_height(), 100);

        // Delete it
        backend.delete(100).unwrap();
        assert!(!backend.exists(100).unwrap());
    }

    #[test]
    fn test_tiered_manager_hot_only() {
        let temp = TempDir::new().expect("create temp dir");
        let hot = Box::new(LocalBackend::new(temp.path().join("hot"), 10));

        let config = TieredConfig {
            hot_count: 3,
            ..Default::default()
        };

        let manager = TieredSnapshotManager::new_hot_only(hot, config);

        // Store some snapshots
        for height in [100, 200, 300] {
            let snapshot = create_test_snapshot(height);
            manager.store(&snapshot).unwrap();
        }

        // List all
        let all = manager.list_all().unwrap();
        assert_eq!(all.len(), 3);
        assert!(all.iter().all(|l| l.tier == StorageTier::Hot));

        // Load from hot
        let loaded = manager.load(200).unwrap();
        assert_eq!(loaded.shard_height(), 200);

        // Find snapshot for height
        assert_eq!(manager.find_snapshot_for(250).unwrap(), Some(200));
        assert_eq!(manager.find_snapshot_for(100).unwrap(), Some(100));
        assert_eq!(manager.find_snapshot_for(50).unwrap(), None);
    }

    #[test]
    fn test_tiered_manager_with_warm() {
        let temp = TempDir::new().expect("create temp dir");
        let hot = Box::new(LocalBackend::new(temp.path().join("hot"), 10));
        let warm = Box::new(ObjectStorageBackend::new(
            "test-bucket".to_string(),
            "snapshots".to_string(),
        ));

        let config = TieredConfig {
            hot_count: 2,
            ..Default::default()
        };

        let manager = TieredSnapshotManager::new_with_warm(hot, warm, config);

        // Store 4 snapshots (more than hot_count)
        for height in [100, 200, 300, 400] {
            let snapshot = create_test_snapshot(height);
            manager.store(&snapshot).unwrap();
        }

        // All should be in hot tier initially
        let all = manager.list_all().unwrap();
        assert_eq!(all.len(), 4);
        assert!(all.iter().all(|l| l.tier == StorageTier::Hot));

        // Demote to warm
        let demoted = manager.demote_to_warm().unwrap();
        assert_eq!(demoted, 2);

        // Check locations
        let all = manager.list_all().unwrap();
        let hot_count = all.iter().filter(|l| l.tier == StorageTier::Hot).count();
        let warm_count = all.iter().filter(|l| l.tier == StorageTier::Warm).count();
        assert_eq!(hot_count, 2);
        assert_eq!(warm_count, 2);

        // Oldest should be in warm
        assert_eq!(manager.get_tier(100).unwrap(), Some(StorageTier::Warm));
        assert_eq!(manager.get_tier(200).unwrap(), Some(StorageTier::Warm));
        // Newest should be in hot
        assert_eq!(manager.get_tier(300).unwrap(), Some(StorageTier::Hot));
        assert_eq!(manager.get_tier(400).unwrap(), Some(StorageTier::Hot));

        // Can still load from warm
        let loaded = manager.load(100).unwrap();
        assert_eq!(loaded.shard_height(), 100);
    }

    #[test]
    fn test_snapshot_not_found() {
        let temp = TempDir::new().expect("create temp dir");
        let hot = Box::new(LocalBackend::new(temp.path().join("hot"), 10));

        let manager = TieredSnapshotManager::new_hot_only(hot, TieredConfig::default());

        // Try to load non-existent snapshot
        let result = manager.load(999);
        assert!(matches!(
            result,
            Err(TieredStorageError::SnapshotNotFound { height: 999 })
        ));
    }
}
