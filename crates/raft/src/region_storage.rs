//! Per-region database file layout management.
//!
//! Each region's Raft group gets isolated database files under a dedicated directory.
//! This eliminates write lock contention between concurrent Raft group applies,
//! enables per-region encryption (one `EncryptedBackend` per file with that region's RMK),
//! and simplifies migration, snapshots, crypto-shredding, and disk accounting.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/
//! ├── node_id                          # node identity (unchanged)
//! ├── global/                          # GLOBAL Raft group (_system control plane)
//! │   ├── state.db                     # org registry, sequences, sagas, node info (no PII)
//! │   ├── blocks.db                    # global blockchain
//! │   ├── raft.db                      # Raft log for GLOBAL group
//! │   └── events.db                    # audit events for GLOBAL group
//! ├── regions/                         # per-region data (orgs + user PII)
//! │   ├── us_east_va/
//! │   │   ├── state.db
//! │   │   ├── blocks.db
//! │   │   ├── raft.db
//! │   │   └── events.db
//! │   └── .../
//! ├── snapshots/                       # per-region snapshot directories
//! │   ├── global/
//! │   └── .../
//! └── keys/                            # per-region RMK storage (managed by RegionKeyManager)
//! ```

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use inferadb_ledger_state::EventsDatabase;
use inferadb_ledger_store::{Database, DatabaseConfig, FileBackend};
use inferadb_ledger_types::Region;
use parking_lot::RwLock;
use snafu::Snafu;

/// Page size for all per-region databases (16KB).
///
/// Matches `RAFT_PAGE_SIZE` in `RaftLogStore` to support larger batch sizes.
const REGION_PAGE_SIZE: usize = 16 * 1024;

// ============================================================================
// Error Types
// ============================================================================

/// Errors from region storage operations.
#[derive(Debug, Snafu)]
pub enum RegionStorageError {
    /// Region is already open.
    #[snafu(display("Region {region} is already open"))]
    AlreadyOpen {
        /// The region that was already open.
        region: Region,
    },

    /// Region is not open.
    #[snafu(display("Region {region} is not open"))]
    NotOpen {
        /// The region that was not found.
        region: Region,
    },

    /// Legacy flat layout detected.
    #[snafu(display(
        "Legacy flat layout detected: found state.db in {data_dir}. \
         Per-region directory layout required. \
         Start with a fresh data directory or migrate via backup/restore."
    ))]
    LegacyLayout {
        /// The data directory containing the legacy layout.
        data_dir: String,
    },

    /// Storage I/O or database error.
    #[snafu(display("Storage error for region {region}: {message}"))]
    Storage {
        /// The region the error pertains to.
        region: Region,
        /// Description of the error.
        message: String,
    },
}

// ============================================================================
// RegionStorage
// ============================================================================

/// Storage components for a single region.
///
/// Holds the raw database handles (state, blocks, events) for one region.
/// The Raft log database (`raft.db`) is owned by
/// [`RaftLogStore`](crate::log_storage::RaftLogStore), not this struct — it is consumed by the
/// openraft `Adaptor`.
///
/// Higher-level wrappers ([`StateLayer`](inferadb_ledger_state::StateLayer),
/// [`BlockArchive`](inferadb_ledger_state::BlockArchive)) are created from
/// these raw handles in [`RegionGroup`](crate::multi_raft::RegionGroup).
pub struct RegionStorage {
    /// Region this storage belongs to.
    region: Region,
    /// State database (entity/relationship stores, indexes, org registry for GLOBAL).
    state_db: Arc<Database<FileBackend>>,
    /// Blocks database (historical blockchain data).
    blocks_db: Arc<Database<FileBackend>>,
    /// Events database (audit event persistence).
    events_db: Arc<EventsDatabase<FileBackend>>,
}

impl RegionStorage {
    /// Returns the region this storage belongs to.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Returns the state database handle.
    pub fn state_db(&self) -> &Arc<Database<FileBackend>> {
        &self.state_db
    }

    /// Returns the blocks database handle.
    pub fn blocks_db(&self) -> &Arc<Database<FileBackend>> {
        &self.blocks_db
    }

    /// Returns the events database handle.
    pub fn events_db(&self) -> &Arc<EventsDatabase<FileBackend>> {
        &self.events_db
    }
}

// ============================================================================
// RegionStorageManager
// ============================================================================

/// Manages per-region database file layout and lifecycle.
///
/// Opens and closes per-region database directories, enforcing the directory
/// tree convention (`global/` for the GLOBAL Raft group, `regions/{name}/`
/// for data regions). Detects legacy flat layouts and produces clear errors.
pub struct RegionStorageManager {
    /// Base data directory.
    data_dir: PathBuf,
    /// Open region storage indexed by region.
    regions: RwLock<HashMap<Region, Arc<RegionStorage>>>,
}

impl RegionStorageManager {
    /// Creates a new storage manager for the given data directory.
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir, regions: RwLock::new(HashMap::new()) }
    }

    /// Returns the base data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the data directory for a specific region.
    ///
    /// - `GLOBAL` → `{data_dir}/global/`
    /// - Others → `{data_dir}/regions/{region_name}/`
    pub fn region_dir(&self, region: Region) -> PathBuf {
        if region == Region::GLOBAL {
            self.data_dir.join("global")
        } else {
            self.data_dir.join("regions").join(region.as_str())
        }
    }

    /// Returns the snapshot directory for a specific region.
    ///
    /// - `GLOBAL` → `{data_dir}/snapshots/global/`
    /// - Others → `{data_dir}/snapshots/{region_name}/`
    pub fn snapshot_dir(&self, region: Region) -> PathBuf {
        if region == Region::GLOBAL {
            self.data_dir.join("snapshots").join("global")
        } else {
            self.data_dir.join("snapshots").join(region.as_str())
        }
    }

    /// Returns the Raft log database path for a specific region.
    pub fn raft_db_path(&self, region: Region) -> PathBuf {
        self.region_dir(region).join("raft.db")
    }

    /// Checks for legacy flat layout and returns an error if detected.
    ///
    /// A legacy flat layout has `state.db` directly in the data directory root.
    /// The per-region layout places databases under `global/` or `regions/{name}/`.
    ///
    /// # Errors
    ///
    /// Returns [`RegionStorageError::LegacyLayout`] if `{data_dir}/state.db` exists.
    pub fn detect_legacy_layout(&self) -> Result<(), RegionStorageError> {
        let legacy_state = self.data_dir.join("state.db");
        if legacy_state.exists() {
            return Err(RegionStorageError::LegacyLayout {
                data_dir: self.data_dir.display().to_string(),
            });
        }
        Ok(())
    }

    /// Opens storage for a region.
    ///
    /// Creates the region directory if it does not exist, then opens (or creates)
    /// state, blocks, and events databases within it.
    ///
    /// # Errors
    ///
    /// Returns [`RegionStorageError::AlreadyOpen`] if the region is already open.
    /// Returns [`RegionStorageError::Storage`] if directory creation or database
    /// opening fails.
    pub fn open_region(&self, region: Region) -> Result<Arc<RegionStorage>, RegionStorageError> {
        // Hold write lock for the entire operation to prevent TOCTOU race where
        // two concurrent callers both pass the existence check and open databases,
        // with the second silently overwriting the first.
        let mut regions = self.regions.write();
        if regions.contains_key(&region) {
            return Err(RegionStorageError::AlreadyOpen { region });
        }

        let region_dir = self.region_dir(region);

        // Create directory tree
        std::fs::create_dir_all(&region_dir).map_err(|e| RegionStorageError::Storage {
            region,
            message: format!("failed to create region directory {}: {e}", region_dir.display()),
        })?;

        // Open or create state database
        let state_db_path = region_dir.join("state.db");
        let state_db = open_or_create_db(&state_db_path, region)?;
        let state_db = Arc::new(state_db);

        // Open or create blocks database
        let blocks_db_path = region_dir.join("blocks.db");
        let blocks_db = open_or_create_db(&blocks_db_path, region)?;
        let blocks_db = Arc::new(blocks_db);

        // Open or create events database
        let events_db = EventsDatabase::<FileBackend>::open(&region_dir).map_err(|e| {
            RegionStorageError::Storage {
                region,
                message: format!("failed to open events db: {e}"),
            }
        })?;
        let events_db = Arc::new(events_db);

        let storage = Arc::new(RegionStorage { region, state_db, blocks_db, events_db });

        regions.insert(region, storage.clone());

        tracing::info!(region = region.as_str(), dir = %region_dir.display(), "Opened region storage");

        Ok(storage)
    }

    /// Closes storage for a region.
    ///
    /// Removes the region from tracking. Actual database file closure happens
    /// when all `Arc` references are dropped.
    ///
    /// # Errors
    ///
    /// Returns [`RegionStorageError::NotOpen`] if the region is not currently open.
    pub fn close_region(&self, region: Region) -> Result<(), RegionStorageError> {
        let removed = self.regions.write().remove(&region);
        if removed.is_none() {
            return Err(RegionStorageError::NotOpen { region });
        }
        tracing::info!(region = region.as_str(), "Closed region storage");
        Ok(())
    }

    /// Returns the storage for a region, if open.
    pub fn get(&self, region: Region) -> Option<Arc<RegionStorage>> {
        self.regions.read().get(&region).cloned()
    }

    /// Lists all currently open regions.
    pub fn regions(&self) -> Vec<Region> {
        self.regions.read().keys().copied().collect()
    }

    /// Returns the number of open regions.
    pub fn open_count(&self) -> usize {
        self.regions.read().len()
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Opens an existing database or creates a new one with the region page size.
fn open_or_create_db(
    path: &Path,
    region: Region,
) -> Result<Database<FileBackend>, RegionStorageError> {
    if path.exists() {
        Database::<FileBackend>::open(path)
    } else {
        let config = DatabaseConfig { page_size: REGION_PAGE_SIZE, ..Default::default() };
        Database::<FileBackend>::create_with_config(path, config)
    }
    .map_err(|e| RegionStorageError::Storage {
        region,
        message: format!("failed to open database {}: {e}", path.display()),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use inferadb_ledger_test_utils::TestDir;

    use super::*;

    #[test]
    fn test_region_dir_global() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let dir = mgr.region_dir(Region::GLOBAL);
        assert!(dir.ends_with("global"));
        assert!(!dir.to_string_lossy().contains("regions"));
    }

    #[test]
    fn test_region_dir_data_regions() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let dir = mgr.region_dir(Region::US_EAST_VA);
        assert!(dir.ends_with("regions/us-east-va"));

        let dir = mgr.region_dir(Region::IE_EAST_DUBLIN);
        assert!(dir.ends_with("regions/ie-east-dublin"));

        let dir = mgr.region_dir(Region::JP_EAST_TOKYO);
        assert!(dir.ends_with("regions/jp-east-tokyo"));
    }

    #[test]
    fn test_snapshot_dir() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let dir = mgr.snapshot_dir(Region::GLOBAL);
        assert!(dir.ends_with("snapshots/global"));

        let dir = mgr.snapshot_dir(Region::DE_CENTRAL_FRANKFURT);
        assert!(dir.ends_with("snapshots/de-central-frankfurt"));
    }

    #[test]
    fn test_raft_db_path() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let path = mgr.raft_db_path(Region::GLOBAL);
        assert!(path.ends_with("global/raft.db"));

        let path = mgr.raft_db_path(Region::US_WEST_OR);
        assert!(path.ends_with("regions/us-west-or/raft.db"));
    }

    #[test]
    fn test_open_region_creates_directory_and_databases() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let storage = mgr.open_region(Region::GLOBAL).expect("open global");
        assert_eq!(storage.region(), Region::GLOBAL);

        // Verify directory structure
        let global_dir = temp.path().join("global");
        assert!(global_dir.exists());
        assert!(global_dir.join("state.db").exists());
        assert!(global_dir.join("blocks.db").exists());
        assert!(global_dir.join("events.db").exists());
    }

    #[test]
    fn test_open_data_region_creates_under_regions() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        mgr.open_region(Region::US_EAST_VA).expect("open us-east-va");

        let region_dir = temp.path().join("regions").join("us-east-va");
        assert!(region_dir.exists());
        assert!(region_dir.join("state.db").exists());
        assert!(region_dir.join("blocks.db").exists());
        assert!(region_dir.join("events.db").exists());
    }

    #[test]
    fn test_open_multiple_regions() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        mgr.open_region(Region::GLOBAL).expect("open global");
        mgr.open_region(Region::US_EAST_VA).expect("open us-east-va");
        mgr.open_region(Region::IE_EAST_DUBLIN).expect("open ie-east-dublin");

        assert_eq!(mgr.open_count(), 3);

        let mut regions = mgr.regions();
        regions.sort_by_key(|r| r.as_str());
        assert_eq!(regions.len(), 3);
    }

    #[test]
    fn test_open_duplicate_region_errors() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        mgr.open_region(Region::GLOBAL).expect("open global");

        let result = mgr.open_region(Region::GLOBAL);
        assert!(matches!(
            result,
            Err(RegionStorageError::AlreadyOpen { region }) if region == Region::GLOBAL
        ));
    }

    #[test]
    fn test_close_region() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        mgr.open_region(Region::GLOBAL).expect("open global");
        assert_eq!(mgr.open_count(), 1);

        mgr.close_region(Region::GLOBAL).expect("close global");
        assert_eq!(mgr.open_count(), 0);
        assert!(mgr.get(Region::GLOBAL).is_none());
    }

    #[test]
    fn test_close_nonexistent_region_errors() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let result = mgr.close_region(Region::US_EAST_VA);
        assert!(matches!(
            result,
            Err(RegionStorageError::NotOpen { region }) if region == Region::US_EAST_VA
        ));
    }

    #[test]
    fn test_get_region() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        assert!(mgr.get(Region::GLOBAL).is_none());

        mgr.open_region(Region::GLOBAL).expect("open global");

        let storage = mgr.get(Region::GLOBAL).expect("should be open");
        assert_eq!(storage.region(), Region::GLOBAL);
    }

    #[test]
    fn test_detect_legacy_layout_clean() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // No state.db in root — no legacy layout
        assert!(mgr.detect_legacy_layout().is_ok());
    }

    #[test]
    fn test_detect_legacy_layout_found() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // Create legacy state.db in root
        std::fs::write(temp.path().join("state.db"), b"legacy").expect("create legacy file");

        let result = mgr.detect_legacy_layout();
        assert!(matches!(result, Err(RegionStorageError::LegacyLayout { .. })));
    }

    #[test]
    fn test_reopen_region_after_close() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // Open, close, reopen
        mgr.open_region(Region::US_EAST_VA).expect("open");
        mgr.close_region(Region::US_EAST_VA).expect("close");
        mgr.open_region(Region::US_EAST_VA).expect("reopen");

        assert_eq!(mgr.open_count(), 1);
    }

    #[test]
    fn test_concurrent_region_writes_no_contention() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let storage_a = mgr.open_region(Region::GLOBAL).expect("open global");
        let storage_b = mgr.open_region(Region::US_EAST_VA).expect("open us-east-va");

        // Each region has its own database files — no shared write lock
        // Verify by starting write transactions on both simultaneously
        let write_a = storage_a.state_db().write();
        let write_b = storage_b.state_db().write();

        assert!(write_a.is_ok(), "write transaction on global state should succeed");
        assert!(write_b.is_ok(), "write transaction on us-east-va state should succeed");
    }

    #[test]
    fn test_open_region_idempotent_directory() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // Pre-create the directory
        let region_dir = temp.path().join("global");
        std::fs::create_dir_all(&region_dir).expect("pre-create dir");

        // Opening should succeed even if directory already exists
        let storage = mgr.open_region(Region::GLOBAL).expect("open global");
        assert_eq!(storage.region(), Region::GLOBAL);
    }
}
