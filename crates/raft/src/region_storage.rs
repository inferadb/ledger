//! Per-organization database file layout management.
//!
//! Each organization (Raft group, in the B.1 three-tier model) gets isolated
//! database files under a dedicated directory. Routing is by `OrganizationId`
//! — the organization id IS the storage key. No hash function, no fixed shard
//! count.
//!
//! This commit (B.1.1 — storage layout migration) handles only the path
//! scheme + the `ShardIdx → OrganizationId` rename. The three peer-group
//! distinction (`SystemGroup` / `RegionGroup` / `OrganizationGroup`) and its
//! storage shapes (system + organization carry blocks.db; region group does
//! not) land in B.1.3. For now this module manages a uniform per-organization
//! storage shape (state, blocks, events, raft + WAL); the region group's
//! `_meta/` subdirectory will be introduced in B.1.3.
//!
//! ## On-disk layout (B.1.1)
//!
//! ```text
//! {data_dir}/
//! ├── node_id                          # node identity (unchanged)
//! ├── global/                          # GLOBAL region — hosts the system org only
//! │   └── 0/                           # system org (OrganizationId(0))
//! │       ├── state.db                 # cluster directory, signing keys, refresh tokens
//! │       ├── blocks.db                # global blockchain
//! │       ├── raft.db                  # Raft log for system org
//! │       ├── events.db                # audit events for system org
//! │       └── wal/                     # consensus WAL segments
//! ├── {region}/                        # data regions — one subdirectory per region
//! │   ├── {organization_id_a}/         # organization Raft group
//! │   │   ├── state.db
//! │   │   ├── blocks.db
//! │   │   ├── raft.db
//! │   │   ├── events.db
//! │   │   └── wal/
//! │   ├── {organization_id_b}/
//! │   │   └── ...
//! │   └── _meta/                       # added in B.1.3 — region group state
//! ├── snapshots/                       # per-organization snapshot directories
//! │   ├── global/0/
//! │   └── {region}/{organization_id}/
//! └── keys/                            # per-region RMK storage (managed by RegionKeyManager)
//! ```
//!
//! Notes on the path scheme:
//! - The data-region parent directory is `{data_dir}/{region}/` directly — no intermediate
//!   `regions/` directory. Operators reading `ls` of the data dir see one directory per region
//!   (plus `global/`, `snapshots/`, `keys/`). Cleaner mental model than the Phase A
//!   `regions/{region}/` nesting.
//! - GLOBAL keeps the top-level `global/` name (rather than collapsing into the data-region scheme)
//!   because the system org is structurally special: it is the cluster control plane and is
//!   bootstrapped before any data regions exist.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use inferadb_ledger_state::EventsDatabase;
use inferadb_ledger_store::{Database, DatabaseConfig, FileBackend};
use inferadb_ledger_types::{OrganizationId, Region};
use parking_lot::RwLock;
use snafu::Snafu;

/// Page size for all per-organization databases (16KB).
///
/// Matches `RAFT_PAGE_SIZE` in `RaftLogStore` to support larger batch sizes.
const ORGANIZATION_PAGE_SIZE: usize = 16 * 1024;

// ============================================================================
// Error Types
// ============================================================================

/// Errors from organization storage operations.
#[derive(Debug, Snafu)]
pub enum RegionStorageError {
    /// Organization storage is already open.
    #[snafu(display("Organization {organization_id} in region {region} is already open"))]
    AlreadyOpen {
        /// The region.
        region: Region,
        /// The organization that was already open.
        organization_id: OrganizationId,
    },

    /// Organization storage is not open.
    #[snafu(display("Organization {organization_id} in region {region} is not open"))]
    NotOpen {
        /// The region.
        region: Region,
        /// The organization that was not found.
        organization_id: OrganizationId,
    },

    /// Legacy flat layout detected.
    #[snafu(display(
        "Legacy flat layout detected: found state.db in {data_dir}. \
         Per-organization directory layout required. \
         Start with a fresh data directory or migrate via backup/restore."
    ))]
    LegacyLayout {
        /// The data directory containing the legacy layout.
        data_dir: String,
    },

    /// Storage I/O or database error.
    #[snafu(display(
        "Storage error for organization {organization_id} in region {region}: {message}"
    ))]
    Storage {
        /// The region the error pertains to.
        region: Region,
        /// The organization the error pertains to.
        organization_id: OrganizationId,
        /// Description of the error.
        message: String,
    },

    /// Legacy pre–Slice 1 layout detected: the `_meta:last_applied`
    /// sentinel lives in state.db (old layout) but the `_meta.db`
    /// coordinator database is empty or missing. This build moves the
    /// sentinel to `_meta.db`; migration is not supported.
    #[snafu(display(
        "Legacy state-layer layout detected for organization {organization_id} in region \
         {region}: _meta:last_applied found in state.db at {state_db_path} but no sentinel \
         in meta.db at {meta_db_path}. This build requires the _meta.db coordinator \
         introduced by spec 2026-04-23-per-vault-consensus.md (Slice 1). Wipe the data \
         directory and restart."
    ))]
    LegacyStateLayout {
        /// The region the organization belongs to.
        region: Region,
        /// The organization whose data directory carries the legacy artefact.
        organization_id: OrganizationId,
        /// Path of the state.db that carries the legacy sentinel.
        state_db_path: String,
        /// Expected path of the meta.db coordinator.
        meta_db_path: String,
    },

    /// Inconsistent layout: the legacy sentinel is present in state.db AND
    /// a sentinel has been written to meta.db. This can happen only if a
    /// previous boot crashed mid-migration or someone hand-edited the data
    /// directory. Operator investigates; automatic recovery is unsafe.
    #[snafu(display(
        "Inconsistent state-layer layout for organization {organization_id} in region \
         {region}: legacy sentinel present in state.db at {state_db_path} AND a sentinel \
         is already recorded in meta.db at {meta_db_path}. This indicates a mid-migration \
         crash or manual tampering; automatic recovery is unsafe. Operator must \
         investigate."
    ))]
    InconsistentStateLayout {
        /// The region the organization belongs to.
        region: Region,
        /// The organization whose data directory is inconsistent.
        organization_id: OrganizationId,
        /// Path of the state.db carrying the stale legacy sentinel.
        state_db_path: String,
        /// Path of the meta.db with its own sentinel.
        meta_db_path: String,
    },
}

// ============================================================================
// RegionStorage
// ============================================================================

/// Storage components for a single organization's Raft group.
///
/// Holds the raw database handles (state, blocks, events) for one organization.
/// The Raft log database (`raft.db`) is owned by
/// [`RaftLogStore`](crate::log_storage::RaftLogStore), not this struct — it is
/// consumed by the consensus engine's `Adaptor`.
///
/// Higher-level wrappers ([`StateLayer`](inferadb_ledger_state::StateLayer),
/// [`BlockArchive`](inferadb_ledger_state::BlockArchive)) are created from
/// these raw handles in
/// [`RegionGroup`](crate::raft_manager::RegionGroup) (which is renamed to
/// `OrganizationGroup` in B.1.3).
///
/// **Naming note.** This struct retains its B.1.1 transitional name
/// `RegionStorage` to keep the diff scoped. B.1.3 renames it to
/// `OrganizationStorage` alongside the broader `RegionGroup → OrganizationGroup`
/// type rename.
pub struct RegionStorage {
    /// Region this organization belongs to.
    region: Region,
    /// Organization this storage belongs to.
    organization_id: OrganizationId,
    /// State database (entity / relationship stores, indexes; for the system
    /// org this also holds the cluster + organization directory).
    state_db: Arc<Database<FileBackend>>,
    /// Per-organization coordination database (`_meta.db`) introduced by
    /// Slice 1 of per-vault consensus. Owns the `_meta:last_applied`
    /// crash-recovery sentinel; syncs **after** state.db / raft.db /
    /// blocks.db / events.db in the checkpointer + shutdown fsync order so
    /// the sentinel on disk never outruns the entity data it references.
    meta_db: Arc<Database<FileBackend>>,
    /// Blocks database (historical blockchain data for this organization).
    blocks_db: Arc<Database<FileBackend>>,
    /// Events database (apply-phase audit events for this organization).
    events_db: Arc<EventsDatabase<FileBackend>>,
}

impl RegionStorage {
    /// Returns the region this organization belongs to.
    pub fn region(&self) -> Region {
        self.region
    }

    /// Returns the organization this storage belongs to.
    pub fn organization_id(&self) -> OrganizationId {
        self.organization_id
    }

    /// Returns the state database handle.
    pub fn state_db(&self) -> &Arc<Database<FileBackend>> {
        &self.state_db
    }

    /// Returns the per-organization meta database handle (`_meta.db`).
    ///
    /// Owns the `_meta:last_applied` crash-recovery sentinel; the
    /// checkpointer must sync this handle **after** state.db / raft.db /
    /// blocks.db / events.db on every tick.
    pub fn meta_db(&self) -> &Arc<Database<FileBackend>> {
        &self.meta_db
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

/// Manages on-disk storage layout for organizations across regions.
///
/// Lays out one directory per `(Region, OrganizationId)` pair under the data
/// directory. Tracks which `(region, organization)` storage handles are
/// currently open and prevents double-opening.
///
/// **Naming note.** Retains its B.1.1 transitional name `RegionStorageManager`
/// alongside `RegionStorage` for diff scoping. B.1.3's broader rename pass
/// renames it to `OrganizationStorageManager` (or absorbs it into the new
/// per-tier storage manager triplet — TBD during B.1.3 implementation).
pub struct RegionStorageManager {
    /// Root data directory.
    data_dir: PathBuf,
    /// Open organization storage handles, keyed by `(region, organization_id)`.
    organizations: RwLock<HashMap<(Region, OrganizationId), Arc<RegionStorage>>>,
}

impl RegionStorageManager {
    /// Creates a new storage manager rooted at `data_dir`.
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir, organizations: RwLock::new(HashMap::new()) }
    }

    /// Returns the root data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the base directory for a region (parent of its organization
    /// subdirectories).
    ///
    /// - `GLOBAL` → `{data_dir}/global/`
    /// - Others → `{data_dir}/{region_name}/`
    ///
    /// Per-organization files live under [`organization_dir`](Self::organization_dir).
    /// This method is retained for operations that span all organizations
    /// in a region (directory discovery, region-level cleanup).
    pub fn region_dir(&self, region: Region) -> PathBuf {
        if region == Region::GLOBAL {
            self.data_dir.join("global")
        } else {
            self.data_dir.join(region.as_str())
        }
    }

    /// Returns the data directory for a specific `(region, organization)` pair.
    ///
    /// Layout: `{region_dir}/{organization_id}/`. Contains `state.db`,
    /// `blocks.db`, `raft.db`, `events.db`, and `wal/` for that organization.
    pub fn organization_dir(&self, region: Region, organization_id: OrganizationId) -> PathBuf {
        self.region_dir(region).join(organization_id.value().to_string())
    }

    /// Returns the snapshot directory for a specific `(region, organization)` pair.
    ///
    /// - `GLOBAL` → `{data_dir}/snapshots/global/{organization_id}/`
    /// - Others → `{data_dir}/snapshots/{region_name}/{organization_id}/`
    pub fn snapshot_dir(&self, region: Region, organization_id: OrganizationId) -> PathBuf {
        let base = if region == Region::GLOBAL {
            self.data_dir.join("snapshots").join("global")
        } else {
            self.data_dir.join("snapshots").join(region.as_str())
        };
        base.join(organization_id.value().to_string())
    }

    /// Returns the Raft log database path for a specific `(region, organization)`.
    pub fn raft_db_path(&self, region: Region, organization_id: OrganizationId) -> PathBuf {
        self.organization_dir(region, organization_id).join("raft.db")
    }

    /// Checks for legacy flat layout and returns an error if detected.
    ///
    /// A legacy flat layout has `state.db` directly in the data directory root.
    /// The per-organization layout places databases under
    /// `global/{organization_id}/` or `{region_name}/{organization_id}/`.
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

    /// Opens storage for a specific organization in a region.
    ///
    /// Creates the organization's directory if it does not exist, then opens
    /// (or creates) state, blocks, and events databases within it.
    ///
    /// # Errors
    ///
    /// Returns [`RegionStorageError::AlreadyOpen`] if that organization is
    /// already open. Returns [`RegionStorageError::Storage`] if directory
    /// creation or database opening fails.
    pub fn open_organization(
        &self,
        region: Region,
        organization_id: OrganizationId,
    ) -> Result<Arc<RegionStorage>, RegionStorageError> {
        // Hold write lock for the entire operation to prevent TOCTOU race
        // where two concurrent callers both pass the existence check and
        // open databases, with the second silently overwriting the first.
        let mut organizations = self.organizations.write();
        if organizations.contains_key(&(region, organization_id)) {
            return Err(RegionStorageError::AlreadyOpen { region, organization_id });
        }

        let organization_dir = self.organization_dir(region, organization_id);

        // Create directory tree
        std::fs::create_dir_all(&organization_dir).map_err(|e| RegionStorageError::Storage {
            region,
            organization_id,
            message: format!(
                "failed to create organization directory {}: {e}",
                organization_dir.display()
            ),
        })?;

        // Open or create state database
        let state_db_path = organization_dir.join("state.db");
        let state_db = open_or_create_db(&state_db_path, region, organization_id)?;
        let state_db = Arc::new(state_db);

        // Open or create blocks database
        let blocks_db_path = organization_dir.join("blocks.db");
        let blocks_db = open_or_create_db(&blocks_db_path, region, organization_id)?;
        let blocks_db = Arc::new(blocks_db);

        // Open or create the per-organization meta database.
        //
        // `_meta.db` was introduced by Slice 1 of per-vault consensus — it
        // owns the `_meta:last_applied` crash-recovery sentinel (previously
        // bundled into state.db's Entities table). Opening this DB here
        // ensures every call path that consumes `state_db()` also has
        // `meta_db()` available without a second discovery step.
        let meta_db_path = organization_dir.join("_meta.db");
        let meta_db = open_or_create_db(&meta_db_path, region, organization_id)?;
        let meta_db = Arc::new(meta_db);

        // Legacy-layout detection, run exactly here (post-open, pre-apply).
        //
        // Pre–Slice 1 builds persisted `_meta:last_applied` inside
        // `state.db`'s Entities table. Now the sentinel lives in
        // `_meta.db`. On startup, a data_dir written by an older build
        // will carry the sentinel in state.db and the freshly-created
        // `_meta.db` will have no sentinel — that signal alone is
        // sufficient to refuse to boot.
        //
        // If BOTH databases carry a sentinel, the data_dir is in an
        // inconsistent state — a prior crash between migration steps or
        // manual tampering. Refuse to boot with a different error so the
        // operator investigates.
        detect_legacy_layout(
            region,
            organization_id,
            &state_db_path,
            &meta_db_path,
            &state_db,
            &meta_db,
        )?;

        // Open or create events database
        let events_db = EventsDatabase::<FileBackend>::open(&organization_dir).map_err(|e| {
            RegionStorageError::Storage {
                region,
                organization_id,
                message: format!("failed to open events db: {e}"),
            }
        })?;
        let events_db = Arc::new(events_db);

        let storage = Arc::new(RegionStorage {
            region,
            organization_id,
            state_db,
            meta_db,
            blocks_db,
            events_db,
        });

        organizations.insert((region, organization_id), storage.clone());

        tracing::info!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            dir = %organization_dir.display(),
            "Opened organization storage"
        );

        Ok(storage)
    }

    /// Closes storage for a specific organization.
    ///
    /// Removes the organization from tracking. Actual database file closure
    /// happens when all `Arc` references are dropped.
    ///
    /// # Errors
    ///
    /// Returns [`RegionStorageError::NotOpen`] if that organization is not open.
    pub fn close_organization(
        &self,
        region: Region,
        organization_id: OrganizationId,
    ) -> Result<(), RegionStorageError> {
        let removed = self.organizations.write().remove(&(region, organization_id));
        if removed.is_none() {
            return Err(RegionStorageError::NotOpen { region, organization_id });
        }
        tracing::info!(
            region = region.as_str(),
            organization_id = organization_id.value(),
            "Closed organization storage"
        );
        Ok(())
    }

    /// Returns the storage for an organization, if open.
    pub fn get_organization(
        &self,
        region: Region,
        organization_id: OrganizationId,
    ) -> Option<Arc<RegionStorage>> {
        self.organizations.read().get(&(region, organization_id)).cloned()
    }

    /// Lists all currently open `(region, organization_id)` pairs.
    pub fn open_organizations(&self) -> Vec<(Region, OrganizationId)> {
        self.organizations.read().keys().copied().collect()
    }

    /// Returns the number of open organizations (across all regions).
    pub fn open_count(&self) -> usize {
        self.organizations.read().len()
    }

    /// Discovers regions with existing data on disk.
    ///
    /// Scans `{data_dir}/` for top-level subdirectories whose names match a
    /// known `Region` variant. Returns the list of regions that have on-disk
    /// data and should be opened eagerly on startup. GLOBAL is not included
    /// — it is always started eagerly via the system-group bootstrap.
    ///
    /// Unknown directory names are silently skipped (future regions added
    /// after a binary downgrade, plus the special `global`, `snapshots`,
    /// `keys` top-level directories).
    pub fn discover_existing_regions(&self) -> Vec<Region> {
        let entries = match std::fs::read_dir(&self.data_dir) {
            Ok(entries) => entries,
            Err(_) => return Vec::new(),
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
            // Try to parse directory name as a Region (kebab-case).
            // Skips top-level directories like `global`, `snapshots`,
            // `keys`, `node_id` (`global` would also fail to parse as a
            // data Region; the `region != GLOBAL` guard is defensive).
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

/// Opens an existing database or creates a new one with the organization
/// page size.
fn open_or_create_db(
    path: &Path,
    region: Region,
    organization_id: OrganizationId,
) -> Result<Database<FileBackend>, RegionStorageError> {
    if path.exists() {
        Database::<FileBackend>::open(path)
    } else {
        let config = DatabaseConfig { page_size: ORGANIZATION_PAGE_SIZE, ..Default::default() };
        Database::<FileBackend>::create_with_config(path, config)
    }
    .map_err(|e| RegionStorageError::Storage {
        region,
        organization_id,
        message: format!("failed to open database {}: {e}", path.display()),
    })
}

/// Boot-time detector for the pre–Slice 1 state-layer layout.
///
/// Slice 1 of per-vault consensus moved the `_meta:last_applied` sentinel
/// from state.db's Entities table into a dedicated `_meta.db`. Two cases
/// to catch:
///
/// 1. Legacy sentinel in state.db + no sentinel in meta.db (typical
///    upgrade scenario from an older build) → refuse to boot; operator
///    must wipe the data directory.
/// 2. Legacy sentinel in state.db + sentinel in meta.db (inconsistent;
///    previous boot crashed mid-migration or the data_dir was tampered
///    with) → refuse to boot with a distinct error.
///
/// The detector runs after the databases are opened but before any apply /
/// replay activity so the apply pipeline never observes a mid-migration
/// state.
fn detect_legacy_layout(
    region: Region,
    organization_id: OrganizationId,
    state_db_path: &Path,
    meta_db_path: &Path,
    state_db: &Database<FileBackend>,
    meta_db: &Database<FileBackend>,
) -> Result<(), RegionStorageError> {
    use inferadb_ledger_store::tables::Entities;

    let legacy_key = inferadb_ledger_state::StateLayer::<FileBackend>::LAST_APPLIED_KEY.to_vec();

    // Inspect state.db for the legacy sentinel.
    let state_txn = state_db.read().map_err(|e| RegionStorageError::Storage {
        region,
        organization_id,
        message: format!("failed to open state.db read txn for legacy detection: {e}"),
    })?;
    let legacy_sentinel =
        state_txn.get::<Entities>(&legacy_key).map_err(|e| RegionStorageError::Storage {
            region,
            organization_id,
            message: format!("failed to read legacy sentinel from state.db: {e}"),
        })?;
    drop(state_txn);

    if legacy_sentinel.is_none() {
        // Clean layout (or brand-new data_dir). Nothing to do.
        return Ok(());
    }

    // Legacy artefact present — inspect meta.db to distinguish the two
    // cases.
    let meta_txn = meta_db.read().map_err(|e| RegionStorageError::Storage {
        region,
        organization_id,
        message: format!("failed to open meta.db read txn for legacy detection: {e}"),
    })?;
    let meta_sentinel =
        meta_txn.get::<Entities>(&legacy_key).map_err(|e| RegionStorageError::Storage {
            region,
            organization_id,
            message: format!("failed to read sentinel from meta.db: {e}"),
        })?;
    drop(meta_txn);

    if meta_sentinel.is_some() {
        return Err(RegionStorageError::InconsistentStateLayout {
            region,
            organization_id,
            state_db_path: state_db_path.display().to_string(),
            meta_db_path: meta_db_path.display().to_string(),
        });
    }

    Err(RegionStorageError::LegacyStateLayout {
        region,
        organization_id,
        state_db_path: state_db_path.display().to_string(),
        meta_db_path: meta_db_path.display().to_string(),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use inferadb_ledger_test_utils::TestDir;

    use super::*;

    /// System organization id (matches `state::system::SYSTEM_ORGANIZATION_ID`).
    /// Used in tests as the canonical "always-present in GLOBAL" organization.
    const SYSTEM_ORG: OrganizationId = OrganizationId::new(0);

    #[test]
    fn test_region_dir_global() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let dir = mgr.region_dir(Region::GLOBAL);
        assert!(dir.ends_with("global"));
    }

    #[test]
    fn test_region_dir_data_regions() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let dir = mgr.region_dir(Region::US_EAST_VA);
        assert!(dir.ends_with("us-east-va"));
        assert!(!dir.to_string_lossy().contains("regions"));

        let dir = mgr.region_dir(Region::IE_EAST_DUBLIN);
        assert!(dir.ends_with("ie-east-dublin"));

        let dir = mgr.region_dir(Region::JP_EAST_TOKYO);
        assert!(dir.ends_with("jp-east-tokyo"));
    }

    #[test]
    fn test_snapshot_dir() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let dir = mgr.snapshot_dir(Region::GLOBAL, SYSTEM_ORG);
        assert!(dir.ends_with("snapshots/global/0"));

        let dir = mgr.snapshot_dir(Region::DE_CENTRAL_FRANKFURT, OrganizationId::new(42));
        assert!(dir.ends_with("snapshots/de-central-frankfurt/42"));
    }

    #[test]
    fn test_raft_db_path() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let path = mgr.raft_db_path(Region::GLOBAL, SYSTEM_ORG);
        assert!(path.ends_with("global/0/raft.db"));

        let path = mgr.raft_db_path(Region::US_WEST_OR, OrganizationId::new(7));
        assert!(path.ends_with("us-west-or/7/raft.db"));
    }

    #[test]
    fn test_organization_dir_layout() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let dir = mgr.organization_dir(Region::GLOBAL, SYSTEM_ORG);
        assert!(dir.ends_with("global/0"));

        let dir = mgr.organization_dir(Region::US_EAST_VA, OrganizationId::new(5));
        assert!(dir.ends_with("us-east-va/5"));
    }

    #[test]
    fn test_open_organization_creates_directory_and_databases() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let storage = mgr.open_organization(Region::GLOBAL, SYSTEM_ORG).expect("open system org");
        assert_eq!(storage.region(), Region::GLOBAL);
        assert_eq!(storage.organization_id(), SYSTEM_ORG);

        // Verify directory structure under the new layout.
        let org_dir = temp.path().join("global").join("0");
        assert!(org_dir.exists());
        assert!(org_dir.join("state.db").exists());
        assert!(org_dir.join("blocks.db").exists());
        assert!(org_dir.join("events.db").exists());
    }

    #[test]
    fn test_open_data_region_organization_creates_under_region() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let org = OrganizationId::new(304789);
        mgr.open_organization(Region::US_EAST_VA, org).expect("open us-east-va org");

        let org_dir = temp.path().join("us-east-va").join("304789");
        assert!(org_dir.exists());
        assert!(org_dir.join("state.db").exists());
        assert!(org_dir.join("blocks.db").exists());
        assert!(org_dir.join("events.db").exists());
    }

    #[test]
    fn test_open_multiple_organizations_per_region() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let orgs = [OrganizationId::new(100), OrganizationId::new(200), OrganizationId::new(300)];
        for org in orgs {
            mgr.open_organization(Region::US_EAST_VA, org)
                .unwrap_or_else(|e| panic!("open org {}: {e}", org.value()));
        }

        for org in orgs {
            let dir = temp.path().join("us-east-va").join(org.value().to_string());
            assert!(dir.exists(), "org-{} dir missing", org.value());
            assert!(dir.join("state.db").exists());
            assert!(dir.join("blocks.db").exists());
            assert!(dir.join("events.db").exists());
        }

        assert_eq!(mgr.open_count(), 3);
    }

    #[test]
    fn test_open_multiple_regions_distinct_directories() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let org = OrganizationId::new(42);
        mgr.open_organization(Region::GLOBAL, SYSTEM_ORG).expect("open global system");
        mgr.open_organization(Region::US_EAST_VA, org).expect("open us-east-va org");
        mgr.open_organization(Region::IE_EAST_DUBLIN, org).expect("open ie-east-dublin org");

        assert_eq!(mgr.open_count(), 3);
        assert!(temp.path().join("global").join("0").exists());
        assert!(temp.path().join("us-east-va").join("42").exists());
        assert!(temp.path().join("ie-east-dublin").join("42").exists());
    }

    #[test]
    fn test_open_duplicate_organization_errors() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        mgr.open_organization(Region::GLOBAL, SYSTEM_ORG).expect("open system org");

        let result = mgr.open_organization(Region::GLOBAL, SYSTEM_ORG);
        assert!(matches!(
            result,
            Err(RegionStorageError::AlreadyOpen { region, organization_id })
                if region == Region::GLOBAL && organization_id == SYSTEM_ORG
        ));
    }

    #[test]
    fn test_close_organization() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        mgr.open_organization(Region::GLOBAL, SYSTEM_ORG).expect("open system org");
        assert_eq!(mgr.open_count(), 1);

        mgr.close_organization(Region::GLOBAL, SYSTEM_ORG).expect("close system org");
        assert_eq!(mgr.open_count(), 0);
        assert!(mgr.get_organization(Region::GLOBAL, SYSTEM_ORG).is_none());
    }

    #[test]
    fn test_close_nonexistent_organization_errors() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let org = OrganizationId::new(99);
        let result = mgr.close_organization(Region::US_EAST_VA, org);
        assert!(matches!(
            result,
            Err(RegionStorageError::NotOpen { region, organization_id })
                if region == Region::US_EAST_VA && organization_id == org
        ));
    }

    #[test]
    fn test_get_organization() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        assert!(mgr.get_organization(Region::GLOBAL, SYSTEM_ORG).is_none());

        mgr.open_organization(Region::GLOBAL, SYSTEM_ORG).expect("open system org");

        let storage = mgr.get_organization(Region::GLOBAL, SYSTEM_ORG).expect("should be open");
        assert_eq!(storage.region(), Region::GLOBAL);
        assert_eq!(storage.organization_id(), SYSTEM_ORG);
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
    fn test_reopen_organization_after_close() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let org = OrganizationId::new(7);
        mgr.open_organization(Region::US_EAST_VA, org).expect("open");
        mgr.close_organization(Region::US_EAST_VA, org).expect("close");
        mgr.open_organization(Region::US_EAST_VA, org).expect("reopen");

        assert_eq!(mgr.open_count(), 1);
    }

    #[test]
    fn test_concurrent_organization_writes_no_contention() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let storage_a = mgr.open_organization(Region::GLOBAL, SYSTEM_ORG).expect("open system org");
        let storage_b = mgr
            .open_organization(Region::US_EAST_VA, OrganizationId::new(42))
            .expect("open us-east-va org");

        // Each organization has its own database files — no shared write lock.
        // Verify by starting write transactions on both simultaneously.
        let write_a = storage_a.state_db().write();
        let write_b = storage_b.state_db().write();

        assert!(write_a.is_ok(), "write transaction on system state should succeed");
        assert!(write_b.is_ok(), "write transaction on us-east-va org state should succeed");
    }

    #[test]
    fn test_open_organization_idempotent_directory() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // Pre-create the parent directory
        let region_dir = temp.path().join("global");
        std::fs::create_dir_all(&region_dir).expect("pre-create dir");

        // Opening should succeed even if directory already exists
        let storage = mgr.open_organization(Region::GLOBAL, SYSTEM_ORG).expect("open system org");
        assert_eq!(storage.region(), Region::GLOBAL);
    }

    #[test]
    fn test_discover_existing_regions_empty() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // Empty data dir — should return empty
        let discovered = mgr.discover_existing_regions();
        assert!(discovered.is_empty());
    }

    #[test]
    fn test_discover_existing_regions_only_global() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // Create global/ but no data region directories — only system org would exist
        std::fs::create_dir_all(temp.path().join("global")).expect("create global");

        let discovered = mgr.discover_existing_regions();
        assert!(discovered.is_empty(), "GLOBAL is not a discovered data region");
    }

    #[test]
    fn test_discover_existing_regions_finds_data_regions() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        std::fs::create_dir_all(temp.path().join("us-east-va")).expect("us-east-va");
        std::fs::create_dir_all(temp.path().join("ie-east-dublin")).expect("ie-east-dublin");
        // Non-Region directory names should be ignored.
        std::fs::create_dir_all(temp.path().join("snapshots")).expect("snapshots");
        std::fs::create_dir_all(temp.path().join("keys")).expect("keys");

        let mut discovered = mgr.discover_existing_regions();
        discovered.sort_by_key(|r| r.as_str());
        assert_eq!(discovered.len(), 2);
        assert!(discovered.contains(&Region::US_EAST_VA));
        assert!(discovered.contains(&Region::IE_EAST_DUBLIN));
    }

    // ========================================================================
    // Slice 1 legacy-layout detection
    // ========================================================================

    /// Simulates a pre–Slice 1 data_dir by writing the legacy
    /// `_meta:last_applied` sentinel into state.db before open_organization
    /// runs. open_organization must refuse to boot with LegacyStateLayout.
    #[test]
    fn test_open_organization_refuses_legacy_sentinel_in_state_db() {
        use inferadb_ledger_store::tables::Entities;
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let org = OrganizationId::new(42);
        let org_dir = mgr.organization_dir(Region::US_EAST_VA, org);
        std::fs::create_dir_all(&org_dir).expect("create org dir");

        // Pre-seed state.db with the legacy sentinel key (mirrors what a
        // pre–Slice 1 apply arm wrote into the Entities table).
        let state_db_path = org_dir.join("state.db");
        {
            let db = Database::<FileBackend>::create(&state_db_path).expect("create state.db");
            let mut txn = db.write().expect("open write txn");
            let key =
                inferadb_ledger_state::StateLayer::<FileBackend>::LAST_APPLIED_KEY.to_vec();
            txn.insert_raw(
                inferadb_ledger_store::tables::TableId::Entities,
                &key,
                b"legacy_sentinel",
            )
            .expect("insert legacy sentinel");
            txn.commit().expect("commit legacy sentinel");
            // Sanity: the legacy sentinel is visible before we close.
            let rtxn = db.read().expect("read txn");
            assert!(rtxn.get::<Entities>(&key).expect("get").is_some());
        }

        let result = mgr.open_organization(Region::US_EAST_VA, org);
        match result {
            Err(RegionStorageError::LegacyStateLayout {
                region,
                organization_id,
                state_db_path: reported_state_db_path,
                meta_db_path,
            }) => {
                assert_eq!(region, Region::US_EAST_VA);
                assert_eq!(organization_id, org);
                assert!(
                    reported_state_db_path.contains("state.db"),
                    "state.db path reported: {reported_state_db_path}"
                );
                assert!(
                    meta_db_path.contains("_meta.db"),
                    "meta.db path reported: {meta_db_path}"
                );
            },
            Ok(_) => panic!("expected LegacyStateLayout, got Ok"),
            Err(other) => panic!("expected LegacyStateLayout, got: {other:?}"),
        }
    }

    /// Both the legacy sentinel in state.db AND a sentinel in meta.db
    /// indicate mid-migration state. open_organization must refuse with
    /// InconsistentStateLayout so an operator intervenes.
    #[test]
    fn test_open_organization_refuses_inconsistent_layout() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let org = OrganizationId::new(43);
        let org_dir = mgr.organization_dir(Region::US_EAST_VA, org);
        std::fs::create_dir_all(&org_dir).expect("create org dir");

        let key = inferadb_ledger_state::StateLayer::<FileBackend>::LAST_APPLIED_KEY.to_vec();

        // Legacy sentinel in state.db.
        {
            let db = Database::<FileBackend>::create(org_dir.join("state.db"))
                .expect("create state.db");
            let mut txn = db.write().expect("open write txn");
            txn.insert_raw(
                inferadb_ledger_store::tables::TableId::Entities,
                &key,
                b"legacy_sentinel",
            )
            .expect("insert legacy sentinel");
            txn.commit().expect("commit");
        }
        // Sentinel also in meta.db — inconsistent state.
        {
            let db = Database::<FileBackend>::create(org_dir.join("_meta.db"))
                .expect("create meta.db");
            let mut txn = db.write().expect("open write txn");
            txn.insert_raw(inferadb_ledger_store::tables::TableId::Entities, &key, b"new_sentinel")
                .expect("insert meta sentinel");
            txn.commit().expect("commit");
        }

        let result = mgr.open_organization(Region::US_EAST_VA, org);
        match result {
            Err(RegionStorageError::InconsistentStateLayout { region, organization_id, .. }) => {
                assert_eq!(region, Region::US_EAST_VA);
                assert_eq!(organization_id, org);
            },
            Ok(_) => panic!("expected InconsistentStateLayout, got Ok"),
            Err(other) => panic!("expected InconsistentStateLayout, got: {other:?}"),
        }
    }

    /// A brand-new data_dir must open cleanly — no legacy artefacts.
    #[test]
    fn test_open_organization_fresh_data_dir_has_no_legacy_false_positive() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let storage = mgr
            .open_organization(Region::US_EAST_VA, OrganizationId::new(99))
            .expect("open fresh org");

        // meta.db must exist under the org dir.
        let meta_db_path = temp.path().join("us-east-va").join("99").join("_meta.db");
        assert!(meta_db_path.exists(), "meta.db must be created alongside state.db");

        // Round-trip: meta_db() returns a usable handle.
        assert!(storage.meta_db().read().is_ok());
    }
}
