//! Per-shard database file layout management.
//!
//! Each shard (Raft group) gets isolated database files under a dedicated
//! directory. A region hosts N shards via
//! `NodeConfig.shards_per_region`; orgs within the region route to shards
//! via [`inferadb_ledger_state::shard_routing::ShardRouter`]. This
//! eliminates write-lock contention between concurrent Raft-group applies,
//! enables per-region encryption (one `EncryptedBackend` per file with
//! that region's RMK), and keeps migration / snapshot / crypto-shredding
//! scoped to a single shard directory.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/
//! ├── node_id                          # node identity (unchanged)
//! ├── global/                          # GLOBAL Raft group (_system control plane)
//! │   └── shard-0/                     # GLOBAL is always single-shard (Phase A)
//! │       ├── state.db                 # org registry, sequences, sagas, node info (no PII)
//! │       ├── blocks.db                # global blockchain
//! │       ├── raft.db                  # Raft log for GLOBAL group
//! │       ├── events.db                # audit events for GLOBAL group
//! │       └── wal/                     # consensus WAL segments
//! ├── regions/                         # per-region data (orgs + user PII)
//! │   ├── us_east_va/
//! │   │   ├── shard-0/
//! │   │   │   ├── state.db
//! │   │   │   ├── blocks.db
//! │   │   │   ├── raft.db
//! │   │   │   ├── events.db
//! │   │   │   └── wal/
//! │   │   ├── shard-1/
//! │   │   │   └── ...
//! │   │   └── shard-N/
//! │   └── .../
//! ├── snapshots/                       # per-shard snapshot directories
//! │   ├── global/shard-0/
//! │   └── .../
//! └── keys/                            # per-region RMK storage (managed by RegionKeyManager)
//! ```

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use inferadb_ledger_state::{EventsDatabase, shard_routing::ShardIdx};
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
/// these raw handles in [`RegionGroup`](crate::raft_manager::RegionGroup).
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

/// Manages per-shard database file layout and lifecycle.
///
/// Opens and closes per-shard database directories under the per-region
/// tree (`global/shard-{N}/`, `regions/{name}/shard-{N}/`). Detects legacy
/// flat layouts and produces clear errors.
///
/// Each region hosts `shards_per_region` shards (Phase A: fixed at region
/// creation). Orgs within the region route to shards via
/// [`inferadb_ledger_state::shard_routing::ShardRouter`]; the storage
/// manager is agnostic to routing — it just opens what it's asked to open.
pub struct RegionStorageManager {
    /// Base data directory.
    data_dir: PathBuf,
    /// Open per-shard storage indexed by `(region, shard_idx)`.
    shards: RwLock<HashMap<(Region, ShardIdx), Arc<RegionStorage>>>,
}

impl RegionStorageManager {
    /// Creates a new storage manager for the given data directory.
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir, shards: RwLock::new(HashMap::new()) }
    }

    /// Returns the base data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the base directory for a region (parent of its shard subdirs).
    ///
    /// - `GLOBAL` → `{data_dir}/global/`
    /// - Others → `{data_dir}/regions/{region_name}/`
    ///
    /// Per-shard files live under [`shard_dir`](Self::shard_dir). This
    /// method is retained for operations that span all shards in a region
    /// (e.g. directory discovery, region-level cleanup).
    pub fn region_dir(&self, region: Region) -> PathBuf {
        if region == Region::GLOBAL {
            self.data_dir.join("global")
        } else {
            self.data_dir.join("regions").join(region.as_str())
        }
    }

    /// Returns the data directory for a specific (region, shard) pair.
    ///
    /// Layout: `{region_dir}/shard-{shard_idx}/`. Contains `state.db`,
    /// `blocks.db`, `raft.db`, `events.db`, and `wal/` for that shard.
    pub fn shard_dir(&self, region: Region, shard: ShardIdx) -> PathBuf {
        self.region_dir(region).join(format!("shard-{}", shard.value()))
    }

    /// Returns the snapshot directory for a specific (region, shard) pair.
    ///
    /// - `GLOBAL` → `{data_dir}/snapshots/global/shard-{N}/`
    /// - Others → `{data_dir}/snapshots/{region_name}/shard-{N}/`
    pub fn snapshot_dir(&self, region: Region, shard: ShardIdx) -> PathBuf {
        let base = if region == Region::GLOBAL {
            self.data_dir.join("snapshots").join("global")
        } else {
            self.data_dir.join("snapshots").join(region.as_str())
        };
        base.join(format!("shard-{}", shard.value()))
    }

    /// Returns the Raft log database path for a specific (region, shard).
    pub fn raft_db_path(&self, region: Region, shard: ShardIdx) -> PathBuf {
        self.shard_dir(region, shard).join("raft.db")
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

    /// Opens storage for a specific shard of a region.
    ///
    /// Creates the shard directory if it does not exist, then opens (or
    /// creates) state, blocks, and events databases within it.
    ///
    /// # Errors
    ///
    /// Returns [`RegionStorageError::AlreadyOpen`] if that shard is
    /// already open. Returns [`RegionStorageError::Storage`] if directory
    /// creation or database opening fails.
    pub fn open_shard(
        &self,
        region: Region,
        shard: ShardIdx,
    ) -> Result<Arc<RegionStorage>, RegionStorageError> {
        // Hold write lock for the entire operation to prevent TOCTOU race
        // where two concurrent callers both pass the existence check and
        // open databases, with the second silently overwriting the first.
        let mut shards = self.shards.write();
        if shards.contains_key(&(region, shard)) {
            return Err(RegionStorageError::AlreadyOpen { region });
        }

        let shard_dir = self.shard_dir(region, shard);

        // Create directory tree
        std::fs::create_dir_all(&shard_dir).map_err(|e| RegionStorageError::Storage {
            region,
            message: format!("failed to create shard directory {}: {e}", shard_dir.display()),
        })?;

        // Open or create state database
        let state_db_path = shard_dir.join("state.db");
        let state_db = open_or_create_db(&state_db_path, region)?;
        let state_db = Arc::new(state_db);

        // Open or create blocks database
        let blocks_db_path = shard_dir.join("blocks.db");
        let blocks_db = open_or_create_db(&blocks_db_path, region)?;
        let blocks_db = Arc::new(blocks_db);

        // Open or create events database
        let events_db = EventsDatabase::<FileBackend>::open(&shard_dir).map_err(|e| {
            RegionStorageError::Storage {
                region,
                message: format!("failed to open events db: {e}"),
            }
        })?;
        let events_db = Arc::new(events_db);

        let storage = Arc::new(RegionStorage { region, state_db, blocks_db, events_db });

        shards.insert((region, shard), storage.clone());

        tracing::info!(
            region = region.as_str(),
            shard = shard.value(),
            dir = %shard_dir.display(),
            "Opened shard storage"
        );

        Ok(storage)
    }

    /// Closes storage for a specific shard.
    ///
    /// Removes the shard from tracking. Actual database file closure
    /// happens when all `Arc` references are dropped.
    ///
    /// # Errors
    ///
    /// Returns [`RegionStorageError::NotOpen`] if that shard is not open.
    pub fn close_shard(&self, region: Region, shard: ShardIdx) -> Result<(), RegionStorageError> {
        let removed = self.shards.write().remove(&(region, shard));
        if removed.is_none() {
            return Err(RegionStorageError::NotOpen { region });
        }
        tracing::info!(region = region.as_str(), shard = shard.value(), "Closed shard storage");
        Ok(())
    }

    /// Returns the storage for a shard, if open.
    pub fn get_shard(&self, region: Region, shard: ShardIdx) -> Option<Arc<RegionStorage>> {
        self.shards.read().get(&(region, shard)).cloned()
    }

    /// Lists all currently open (region, shard) pairs.
    pub fn open_shards(&self) -> Vec<(Region, ShardIdx)> {
        self.shards.read().keys().copied().collect()
    }

    /// Returns the number of open shards (across all regions).
    pub fn open_count(&self) -> usize {
        self.shards.read().len()
    }

    // ── Legacy single-shard convenience aliases ──
    //
    // Task 3 keeps these so the raft_manager + tests don't all have to
    // change at once. Task 4 (RegionGroup per-shard refactor) will call
    // the shard-aware methods directly and these can be removed.

    /// Legacy shim: opens shard-0 of `region`. Kept until Task 4 migrates
    /// callers to iterate over `shards_per_region`.
    #[doc(hidden)]
    pub fn open_region(&self, region: Region) -> Result<Arc<RegionStorage>, RegionStorageError> {
        self.open_shard(region, ShardIdx(0))
    }

    /// Legacy shim: closes shard-0 of `region`.
    #[doc(hidden)]
    pub fn close_region(&self, region: Region) -> Result<(), RegionStorageError> {
        self.close_shard(region, ShardIdx(0))
    }

    /// Legacy shim: returns shard-0 storage of `region`.
    #[doc(hidden)]
    pub fn get(&self, region: Region) -> Option<Arc<RegionStorage>> {
        self.get_shard(region, ShardIdx(0))
    }

    /// Legacy shim: lists regions that have shard-0 open. Task 4 replaces
    /// this with full (region, shard) enumeration.
    #[doc(hidden)]
    pub fn regions(&self) -> Vec<Region> {
        self.shards.read().keys().filter_map(|(r, s)| (s.value() == 0).then_some(*r)).collect()
    }

    /// Discovers regions with existing data on disk.
    ///
    /// Scans `{data_dir}/regions/` for subdirectories whose names match a known
    /// `Region` variant. Returns the list of regions that have on-disk data and
    /// should be opened eagerly on startup. GLOBAL is not included — it is always
    /// started eagerly via `start_system_region`.
    ///
    /// Unknown directory names are silently skipped (future regions added after
    /// a binary downgrade).
    pub fn discover_existing_regions(&self) -> Vec<Region> {
        let regions_dir = self.data_dir.join("regions");
        let entries = match std::fs::read_dir(&regions_dir) {
            Ok(entries) => entries,
            Err(_) => return Vec::new(), // No regions/ directory yet
        };

        let mut discovered = Vec::new();
        for entry in entries.flatten() {
            if !entry.path().is_dir() {
                continue;
            }
            let dir_name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };
            // Try to parse directory name as a Region (kebab-case)
            if let Ok(region) = dir_name.parse::<Region>()
                && region != Region::GLOBAL
            {
                discovered.push(region);
            }
        }
        discovered
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

        let dir = mgr.snapshot_dir(Region::GLOBAL, ShardIdx(0));
        assert!(dir.ends_with("snapshots/global/shard-0"));

        let dir = mgr.snapshot_dir(Region::DE_CENTRAL_FRANKFURT, ShardIdx(3));
        assert!(dir.ends_with("snapshots/de-central-frankfurt/shard-3"));
    }

    #[test]
    fn test_raft_db_path() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let path = mgr.raft_db_path(Region::GLOBAL, ShardIdx(0));
        assert!(path.ends_with("global/shard-0/raft.db"));

        let path = mgr.raft_db_path(Region::US_WEST_OR, ShardIdx(7));
        assert!(path.ends_with("regions/us-west-or/shard-7/raft.db"));
    }

    #[test]
    fn test_shard_dir_layout() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let dir = mgr.shard_dir(Region::GLOBAL, ShardIdx(0));
        assert!(dir.ends_with("global/shard-0"));

        let dir = mgr.shard_dir(Region::US_EAST_VA, ShardIdx(5));
        assert!(dir.ends_with("regions/us-east-va/shard-5"));
    }

    #[test]
    fn test_open_region_creates_directory_and_databases() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let storage = mgr.open_region(Region::GLOBAL).expect("open global");
        assert_eq!(storage.region(), Region::GLOBAL);

        // Verify directory structure — files land under `shard-0/` (Phase
        // A single-shard layout). The legacy flat layout is gone.
        let shard_dir = temp.path().join("global").join("shard-0");
        assert!(shard_dir.exists());
        assert!(shard_dir.join("state.db").exists());
        assert!(shard_dir.join("blocks.db").exists());
        assert!(shard_dir.join("events.db").exists());
    }

    #[test]
    fn test_open_data_region_creates_under_regions() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        mgr.open_region(Region::US_EAST_VA).expect("open us-east-va");

        let shard_dir = temp.path().join("regions").join("us-east-va").join("shard-0");
        assert!(shard_dir.exists());
        assert!(shard_dir.join("state.db").exists());
        assert!(shard_dir.join("blocks.db").exists());
        assert!(shard_dir.join("events.db").exists());
    }

    #[test]
    fn test_open_multiple_shards_per_region() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        mgr.open_shard(Region::US_EAST_VA, ShardIdx(0)).expect("shard 0");
        mgr.open_shard(Region::US_EAST_VA, ShardIdx(1)).expect("shard 1");
        mgr.open_shard(Region::US_EAST_VA, ShardIdx(2)).expect("shard 2");

        for shard in 0..3 {
            let dir = temp.path().join("regions").join("us-east-va").join(format!("shard-{shard}"));
            assert!(dir.exists(), "shard-{shard} dir missing");
            assert!(dir.join("state.db").exists());
            assert!(dir.join("blocks.db").exists());
            assert!(dir.join("events.db").exists());
        }

        assert_eq!(mgr.open_count(), 3);
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

    #[test]
    fn test_discover_existing_regions_empty() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // No regions/ directory yet — should return empty
        let discovered = mgr.discover_existing_regions();
        assert!(discovered.is_empty());
    }

    #[test]
    fn test_discover_existing_regions_no_regions_dir() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // Create global/ but not regions/ — only GLOBAL would exist
        std::fs::create_dir_all(temp.path().join("global")).expect("create global");

        let discovered = mgr.discover_existing_regions();
        assert!(discovered.is_empty());
    }

    #[test]
    fn test_discover_existing_regions_finds_opened_regions() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // Open some regions (creates directories with databases)
        mgr.open_region(Region::GLOBAL).expect("open global");
        mgr.open_region(Region::US_EAST_VA).expect("open us-east-va");
        mgr.open_region(Region::IE_EAST_DUBLIN).expect("open ie-east-dublin");

        // Close all (discover reads the filesystem, not the open map)
        mgr.close_region(Region::GLOBAL).expect("close");
        mgr.close_region(Region::US_EAST_VA).expect("close");
        mgr.close_region(Region::IE_EAST_DUBLIN).expect("close");

        let mut discovered = mgr.discover_existing_regions();
        discovered.sort_by_key(|r| r.as_str());

        // GLOBAL is excluded from discover — it's always opened eagerly
        assert_eq!(discovered.len(), 2);
        assert_eq!(discovered[0], Region::IE_EAST_DUBLIN);
        assert_eq!(discovered[1], Region::US_EAST_VA);
    }

    #[test]
    fn test_discover_existing_regions_skips_unknown_dirs() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let regions_dir = temp.path().join("regions");
        std::fs::create_dir_all(&regions_dir).expect("create regions/");

        // Create a valid region directory
        std::fs::create_dir_all(regions_dir.join("us-east-va")).expect("create us-east-va");

        // Create invalid directories that should be skipped
        std::fs::create_dir_all(regions_dir.join("unknown-region")).expect("create unknown");
        std::fs::create_dir_all(regions_dir.join("temp")).expect("create temp");

        // Create a file (not a directory) — should be skipped
        std::fs::write(regions_dir.join("not-a-dir.txt"), b"").expect("create file");

        let discovered = mgr.discover_existing_regions();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0], Region::US_EAST_VA);
    }

    #[test]
    fn test_discover_existing_regions_excludes_global() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let regions_dir = temp.path().join("regions");
        std::fs::create_dir_all(&regions_dir).expect("create regions/");

        // A directory named "global" under regions/ would match Region::GLOBAL
        // but should be excluded from discovery
        std::fs::create_dir_all(regions_dir.join("global")).expect("create global");
        std::fs::create_dir_all(regions_dir.join("us-west-or")).expect("create us-west-or");

        let discovered = mgr.discover_existing_regions();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0], Region::US_WEST_OR);
    }
}
