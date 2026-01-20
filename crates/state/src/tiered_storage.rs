//! Tiered snapshot storage.
//!
//! Per DESIGN.md §4.4:
//! - Hot: Last N snapshots on local SSD (fast access)
//! - Warm: Older snapshots on object storage (S3/GCS/Azure)
//! - Cold: Archive snapshots (Glacier, optional)
//!
//! This module provides:
//! - Storage backend trait for abstracting local/remote storage
//! - Real object storage backend using `object_store` crate
//! - Tiered manager that moves snapshots between tiers
//! - Transparent loading from any tier

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{ObjectStore, PutPayload, path::Path as ObjectPath};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};
use url::Url;

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
        Self { manager: SnapshotManager::new(snapshot_dir, max_snapshots) }
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
        let path = self.manager.snapshot_dir().join(snapshot_filename(height));
        if path.exists() {
            std::fs::remove_file(&path).context(IoSnafu)?;
        }
        Ok(())
    }

    fn list(&self) -> Result<Vec<u64>> {
        self.manager.list_snapshots().context(SnapshotSnafu)
    }
}

/// Object storage backend using `object_store` crate (Warm tier).
///
/// Supports S3, GCS, Azure Blob Storage, and local filesystem via URL schemes:
/// - `s3://bucket/prefix` - Amazon S3 (or compatible: MinIO, Wasabi, etc.)
/// - `gs://bucket/prefix` - Google Cloud Storage
/// - `az://container/prefix` - Azure Blob Storage
/// - `file:///path/to/dir` - Local filesystem (for testing/development)
///
/// Credentials are read from environment variables:
/// - S3: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
/// - GCS: GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SERVICE_ACCOUNT
/// - Azure: AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY
pub struct ObjectStorageBackend {
    /// The underlying object store implementation.
    store: Arc<dyn ObjectStore>,
    /// Prefix path for all snapshot objects.
    prefix: ObjectPath,
    /// Tokio runtime handle for running async operations.
    runtime: tokio::runtime::Handle,
    /// Simulated storage for testing (only used in test mode).
    #[cfg(test)]
    test_storage: Arc<RwLock<HashMap<u64, Vec<u8>>>>,
    /// Flag indicating if this is a test-only backend.
    #[cfg(test)]
    is_test_backend: bool,
}

impl ObjectStorageBackend {
    /// Create a new object storage backend from a URL.
    ///
    /// # Supported URLs
    ///
    /// - `s3://bucket-name/prefix/path`
    /// - `gs://bucket-name/prefix/path`
    /// - `az://container-name/prefix/path`
    /// - `file:///local/path/to/dir`
    ///
    /// # Environment Variables
    ///
    /// S3 (also works with MinIO, Wasabi, etc.):
    /// - `AWS_ACCESS_KEY_ID` - Access key
    /// - `AWS_SECRET_ACCESS_KEY` - Secret key
    /// - `AWS_REGION` - Region (default: us-east-1)
    /// - `AWS_ENDPOINT` - Custom endpoint for S3-compatible services
    ///
    /// GCS:
    /// - `GOOGLE_APPLICATION_CREDENTIALS` - Path to service account JSON
    ///
    /// Azure:
    /// - `AZURE_STORAGE_ACCOUNT_NAME` - Storage account name
    /// - `AZURE_STORAGE_ACCOUNT_KEY` - Storage account key
    pub fn new(url: &str) -> Result<Self> {
        let parsed = Url::parse(url).map_err(|e| TieredStorageError::ObjectStorage {
            message: format!("Invalid URL '{}': {}", url, e),
        })?;

        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            TieredStorageError::ObjectStorage {
                message: "No tokio runtime available - ObjectStorageBackend requires async runtime"
                    .to_string(),
            }
        })?;

        let (store, prefix) = Self::create_store_from_url(&parsed)?;

        Ok(Self {
            store,
            prefix,
            runtime,
            #[cfg(test)]
            test_storage: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(test)]
            is_test_backend: false,
        })
    }

    /// Create a test-only in-memory backend (for unit tests).
    #[cfg(test)]
    #[allow(clippy::expect_used)] // Acceptable in test code - panic on failure is fine
    pub fn new_test(bucket: String, prefix: String) -> Self {
        // Create an in-memory store for testing
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

        Self {
            store,
            prefix: ObjectPath::from(format!("{}/{}", bucket, prefix)),
            runtime: tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
                // Create a minimal runtime for tests
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("create test runtime")
                    .handle()
                    .clone()
            }),
            test_storage: Arc::new(RwLock::new(HashMap::new())),
            is_test_backend: true,
        }
    }

    /// Create object store from parsed URL.
    fn create_store_from_url(url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let scheme = url.scheme();

        match scheme {
            "s3" => Self::create_s3_store(url),
            "gs" => Self::create_gcs_store(url),
            "az" | "azure" => Self::create_azure_store(url),
            "file" => Self::create_local_store(url),
            _ => Err(TieredStorageError::ObjectStorage {
                message: format!(
                    "Unsupported URL scheme '{}'. Supported: s3, gs, az, file",
                    scheme
                ),
            }),
        }
    }

    /// Create S3 object store.
    fn create_s3_store(url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let bucket = url.host_str().ok_or_else(|| TieredStorageError::ObjectStorage {
            message: "S3 URL must include bucket name as host".to_string(),
        })?;

        let prefix = url.path().trim_start_matches('/');

        // Build S3 store with environment credentials
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()));

        // Add credentials from environment
        if let Ok(key_id) = std::env::var("AWS_ACCESS_KEY_ID") {
            builder = builder.with_access_key_id(key_id);
        }
        if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret);
        }

        // Support custom endpoint for MinIO/Wasabi/etc.
        if let Ok(endpoint) = std::env::var("AWS_ENDPOINT") {
            builder = builder.with_endpoint(endpoint).with_virtual_hosted_style_request(false);
        }

        let store = builder.build().map_err(|e| TieredStorageError::ObjectStorage {
            message: format!("Failed to create S3 store: {}", e),
        })?;

        Ok((Arc::new(store), ObjectPath::from(prefix)))
    }

    /// Create GCS object store.
    fn create_gcs_store(url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let bucket = url.host_str().ok_or_else(|| TieredStorageError::ObjectStorage {
            message: "GCS URL must include bucket name as host".to_string(),
        })?;

        let prefix = url.path().trim_start_matches('/');

        let mut builder =
            object_store::gcp::GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

        // Add service account from environment
        if let Ok(creds_path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            builder = builder.with_service_account_path(creds_path);
        }

        let store = builder.build().map_err(|e| TieredStorageError::ObjectStorage {
            message: format!("Failed to create GCS store: {}", e),
        })?;

        Ok((Arc::new(store), ObjectPath::from(prefix)))
    }

    /// Create Azure Blob object store.
    fn create_azure_store(url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let container = url.host_str().ok_or_else(|| TieredStorageError::ObjectStorage {
            message: "Azure URL must include container name as host".to_string(),
        })?;

        let prefix = url.path().trim_start_matches('/');

        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").map_err(|_| {
            TieredStorageError::ObjectStorage {
                message: "AZURE_STORAGE_ACCOUNT_NAME environment variable not set".to_string(),
            }
        })?;

        let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
            .with_account(account)
            .with_container_name(container);

        if let Ok(key) = std::env::var("AZURE_STORAGE_ACCOUNT_KEY") {
            builder = builder.with_access_key(key);
        }

        let store = builder.build().map_err(|e| TieredStorageError::ObjectStorage {
            message: format!("Failed to create Azure store: {}", e),
        })?;

        Ok((Arc::new(store), ObjectPath::from(prefix)))
    }

    /// Create local filesystem object store.
    fn create_local_store(url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let path = url.path();

        // Create directory if it doesn't exist
        std::fs::create_dir_all(path).map_err(|e| TieredStorageError::ObjectStorage {
            message: format!("Failed to create local storage directory '{}': {}", path, e),
        })?;

        let store = object_store::local::LocalFileSystem::new_with_prefix(path).map_err(|e| {
            TieredStorageError::ObjectStorage {
                message: format!("Failed to create local file store: {}", e),
            }
        })?;

        Ok((Arc::new(store), ObjectPath::from("")))
    }

    /// Object path for a snapshot.
    fn object_path(&self, height: u64) -> ObjectPath {
        let filename = snapshot_filename(height);
        if self.prefix.as_ref().is_empty() {
            ObjectPath::from(filename)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, filename))
        }
    }

    /// Run an async operation synchronously using the tokio runtime.
    ///
    /// Uses `block_in_place` to allow blocking within async contexts (like tests),
    /// which requires a multi-threaded tokio runtime.
    fn block_on<F, T>(&self, future: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        // Use block_in_place to allow nesting within async contexts
        tokio::task::block_in_place(|| self.runtime.block_on(future))
    }
}

impl StorageBackend for ObjectStorageBackend {
    fn tier(&self) -> StorageTier {
        StorageTier::Warm
    }

    fn exists(&self, height: u64) -> Result<bool> {
        #[cfg(test)]
        if self.is_test_backend {
            return Ok(self.test_storage.read().contains_key(&height));
        }

        let path = self.object_path(height);
        let result = self.block_on(async { self.store.head(&path).await });

        match result {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(TieredStorageError::ObjectStorage {
                message: format!("Failed to check object existence: {}", e),
            }),
        }
    }

    fn store(&self, snapshot: &Snapshot) -> Result<()> {
        #[cfg(test)]
        if self.is_test_backend {
            // For testing, serialize to bytes
            let temp_dir =
                tempfile::TempDir::new().map_err(|e| TieredStorageError::Io { source: e })?;
            let temp_path = temp_dir.path().join("temp.snap");
            snapshot.write_to_file(&temp_path).context(SnapshotSnafu)?;
            let buf = std::fs::read(&temp_path).context(IoSnafu)?;
            self.test_storage.write().insert(snapshot.shard_height(), buf);
            return Ok(());
        }

        // Serialize snapshot to bytes
        let temp_dir =
            tempfile::TempDir::new().map_err(|e| TieredStorageError::Io { source: e })?;
        let temp_path = temp_dir.path().join("temp.snap");
        snapshot.write_to_file(&temp_path).context(SnapshotSnafu)?;
        let data = std::fs::read(&temp_path).context(IoSnafu)?;

        let path = self.object_path(snapshot.shard_height());
        let payload = PutPayload::from(Bytes::from(data));

        self.block_on(async { self.store.put(&path, payload).await }).map_err(|e| {
            TieredStorageError::ObjectStorage {
                message: format!("Failed to upload snapshot: {}", e),
            }
        })?;

        Ok(())
    }

    fn load(&self, height: u64) -> Result<Snapshot> {
        #[cfg(test)]
        if self.is_test_backend {
            let guard = self.test_storage.read();
            let data = guard.get(&height).ok_or(TieredStorageError::SnapshotNotFound { height })?;

            let temp_dir =
                tempfile::TempDir::new().map_err(|e| TieredStorageError::Io { source: e })?;
            let temp_path = temp_dir.path().join("temp.snap");
            std::fs::write(&temp_path, data).context(IoSnafu)?;
            return Snapshot::read_from_file(&temp_path).context(SnapshotSnafu);
        }

        let path = self.object_path(height);

        let get_result =
            self.block_on(async { self.store.get(&path).await }).map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    TieredStorageError::SnapshotNotFound { height }
                },
                _ => TieredStorageError::ObjectStorage {
                    message: format!("Failed to download snapshot: {}", e),
                },
            })?;

        let data = self.block_on(async { get_result.bytes().await }).map_err(|e| {
            TieredStorageError::ObjectStorage {
                message: format!("Failed to read snapshot bytes: {}", e),
            }
        })?;

        // Write to temp file and parse
        let temp_dir =
            tempfile::TempDir::new().map_err(|e| TieredStorageError::Io { source: e })?;
        let temp_path = temp_dir.path().join("temp.snap");
        std::fs::write(&temp_path, &data).context(IoSnafu)?;

        Snapshot::read_from_file(&temp_path).context(SnapshotSnafu)
    }

    fn delete(&self, height: u64) -> Result<()> {
        #[cfg(test)]
        if self.is_test_backend {
            self.test_storage.write().remove(&height);
            return Ok(());
        }

        let path = self.object_path(height);

        self.block_on(async { self.store.delete(&path).await }).map_err(|e| {
            TieredStorageError::ObjectStorage {
                message: format!("Failed to delete snapshot: {}", e),
            }
        })?;

        Ok(())
    }

    fn list(&self) -> Result<Vec<u64>> {
        #[cfg(test)]
        if self.is_test_backend {
            let guard = self.test_storage.read();
            let mut heights: Vec<u64> = guard.keys().copied().collect();
            heights.sort_unstable();
            return Ok(heights);
        }

        let prefix = if self.prefix.as_ref().is_empty() { None } else { Some(&self.prefix) };

        let result = self.block_on(async {
            let stream = self.store.list(prefix);
            stream.try_collect::<Vec<_>>().await
        });

        let objects = result.map_err(|e| TieredStorageError::ObjectStorage {
            message: format!("Failed to list snapshots: {}", e),
        })?;

        // Parse heights from filenames (snapshot_NNNN.snap)
        let mut heights = Vec::new();
        for meta in objects {
            let filename = meta.location.filename().unwrap_or("");
            if let Some(height) = parse_snapshot_height(filename) {
                heights.push(height);
            }
        }

        heights.sort_unstable();
        Ok(heights)
    }
}

/// Parse snapshot height from filename.
///
/// Expected format: `000000100.snap` -> 100
fn parse_snapshot_height(filename: &str) -> Option<u64> {
    // Expected format is 9 digits followed by .snap (e.g., "000000100.snap")
    if !filename.ends_with(".snap") {
        return None;
    }

    let height_str = filename.strip_suffix(".snap")?;

    // Should be all digits
    if !height_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    height_str.parse().ok()
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
        Self { hot_count: 3, warm_days: 30, cold_enabled: false }
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
        Self { hot, warm: None, cold: None, config, locations: RwLock::new(HashMap::new()) }
    }

    /// Create a new tiered manager with hot and warm tiers.
    pub fn new_with_warm(
        hot: Box<dyn StorageBackend>,
        warm: Box<dyn StorageBackend>,
        config: TieredConfig,
    ) -> Self {
        Self { hot, warm: Some(warm), cold: None, config, locations: RwLock::new(HashMap::new()) }
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

        if let Some(ref warm) = self.warm
            && warm.exists(height)?
        {
            self.locations.write().insert(height, StorageTier::Warm);
            return warm.load(height);
        }

        if let Some(ref cold) = self.cold
            && cold.exists(height)?
        {
            self.locations.write().insert(height, StorageTier::Cold);
            return cold.load(height);
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
                locations.push(SnapshotLocation { height, tier: StorageTier::Warm, created_at: 0 });
            }
        }

        // Cold tier
        if let Some(ref cold) = self.cold {
            for height in cold.list()? {
                locations.push(SnapshotLocation { height, tier: StorageTier::Cold, created_at: 0 });
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
        Ok(all.into_iter().filter(|l| l.height <= target_height).map(|l| l.height).max())
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

        if let Some(ref warm) = self.warm
            && warm.exists(height)?
        {
            return Ok(Some(StorageTier::Warm));
        }

        if let Some(ref cold) = self.cold
            && cold.exists(height)?
        {
            return Ok(Some(StorageTier::Cold));
        }

        Ok(None)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use std::collections::HashMap as StdHashMap;

    use inferadb_ledger_types::EMPTY_HASH;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        bucket::NUM_BUCKETS,
        snapshot::{SnapshotChainParams, SnapshotStateData, VaultSnapshotMeta},
    };

    fn create_test_snapshot(height: u64) -> Snapshot {
        let vault_states = vec![VaultSnapshotMeta {
            vault_id: 1,
            vault_height: height / 2,
            state_root: [height as u8; 32],
            bucket_roots: [EMPTY_HASH; NUM_BUCKETS].to_vec(),
            key_count: 0,
        }];

        let state = SnapshotStateData { vault_entities: StdHashMap::new() };

        Snapshot::new(1, height, vault_states, state, SnapshotChainParams::default())
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

        let config = TieredConfig { hot_count: 3, ..Default::default() };

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
        let warm: Box<dyn StorageBackend> = Box::new(ObjectStorageBackend::new_test(
            "test-bucket".to_string(),
            "snapshots".to_string(),
        ));

        let config = TieredConfig { hot_count: 2, ..Default::default() };

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
        assert!(matches!(result, Err(TieredStorageError::SnapshotNotFound { height: 999 })));
    }

    /// Test the real ObjectStorageBackend with local filesystem.
    ///
    /// This test validates the actual integration with `object_store` crate,
    /// using `file://` URLs which work identically to S3/GCS/Azure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_object_storage_backend_local_fs() {
        let temp = TempDir::new().expect("create temp dir");
        let storage_path = temp.path().join("object_storage");

        // Create backend using file:// URL (works like S3/GCS/Azure)
        let url = format!("file://{}", storage_path.display());
        let backend = ObjectStorageBackend::new(&url).expect("create backend");

        assert_eq!(backend.tier(), StorageTier::Warm);

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

        // Store more snapshots
        for height in [200, 300, 400] {
            let snapshot = create_test_snapshot(height);
            backend.store(&snapshot).unwrap();
        }

        // List all
        let heights = backend.list().unwrap();
        assert_eq!(heights, vec![100, 200, 300, 400]);

        // Delete one
        backend.delete(200).unwrap();
        assert!(!backend.exists(200).unwrap());

        let heights = backend.list().unwrap();
        assert_eq!(heights, vec![100, 300, 400]);
    }

    /// Test tiered manager with real object storage backend (local filesystem).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tiered_manager_with_real_object_storage() {
        let temp = TempDir::new().expect("create temp dir");
        let hot = Box::new(LocalBackend::new(temp.path().join("hot"), 10));

        let storage_path = temp.path().join("warm");
        let url = format!("file://{}", storage_path.display());
        let warm: Box<dyn StorageBackend> =
            Box::new(ObjectStorageBackend::new(&url).expect("create warm backend"));

        let config = TieredConfig { hot_count: 2, ..Default::default() };

        let manager = TieredSnapshotManager::new_with_warm(hot, warm, config);

        // Store 4 snapshots
        for height in [100, 200, 300, 400] {
            let snapshot = create_test_snapshot(height);
            manager.store(&snapshot).unwrap();
        }

        // All in hot tier initially
        let all = manager.list_all().unwrap();
        assert_eq!(all.len(), 4);
        assert!(all.iter().all(|l| l.tier == StorageTier::Hot));

        // Demote oldest to warm
        let demoted = manager.demote_to_warm().unwrap();
        assert_eq!(demoted, 2);

        // Verify tier locations
        assert_eq!(manager.get_tier(100).unwrap(), Some(StorageTier::Warm));
        assert_eq!(manager.get_tier(200).unwrap(), Some(StorageTier::Warm));
        assert_eq!(manager.get_tier(300).unwrap(), Some(StorageTier::Hot));
        assert_eq!(manager.get_tier(400).unwrap(), Some(StorageTier::Hot));

        // Can load from warm (actual object storage, not test mock)
        let loaded = manager.load(100).unwrap();
        assert_eq!(loaded.shard_height(), 100);

        // Find snapshot for height
        assert_eq!(manager.find_snapshot_for(150).unwrap(), Some(100));
        assert_eq!(manager.find_snapshot_for(350).unwrap(), Some(300));
    }
}
