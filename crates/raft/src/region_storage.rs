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
pub(crate) const ORGANIZATION_PAGE_SIZE: usize = 16 * 1024;

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

    /// Slice 2b legacy detection: the pre-Slice-2b per-organization
    /// layout held a singleton `state.db` file alongside `blocks.db` /
    /// `_meta.db` / `raft.db`. Slice 2b of per-vault consensus replaces
    /// that file with a `state/` directory holding one
    /// `vault-{vault_id}.db` per vault. A data_dir carrying the old
    /// `state.db` file at this path is rejected — migration is not
    /// supported.
    #[snafu(display(
        "Legacy per-org state.db layout detected at {state_db_path} for organization \
         {organization_id} in region {region}. This build requires the per-vault state.db \
         layout introduced by spec 2026-04-23-per-vault-consensus.md (Slice 2b). Wipe the \
         data directory and restart."
    ))]
    LegacyPerOrgStateDb {
        /// The region the organization belongs to.
        region: Region,
        /// The organization whose data directory carries the legacy artefact.
        organization_id: OrganizationId,
        /// Path of the legacy state.db file.
        state_db_path: String,
    },

    /// P2b.0 legacy detection: between Slice 2b and P2b.0 the per-vault
    /// state DB lived as a flat file at
    /// `{state_dir}/vault-{vault_id}.db`. P2b.0 moves it into a
    /// per-vault directory at `{state_dir}/vault-{vault_id}/state.db`
    /// so future slices can add `raft.db` / `blocks.db` / `events.db`
    /// alongside. A data_dir carrying the old flat layout is rejected —
    /// migration is not supported; operators must wipe the data
    /// directory and restart.
    #[snafu(display(
        "Legacy flat-file vault state layout detected at {path} for organization \
         {organization_id} in region {region}. This build requires the per-vault directory \
         form introduced by spec 2026-04-23-per-vault-consensus.md (P2b.0). Data migration \
         is not supported; wipe the data directory and restart."
    ))]
    LegacyVaultLayout {
        /// The region the organization belongs to.
        region: Region,
        /// The organization whose state directory carries the legacy artefact.
        organization_id: OrganizationId,
        /// Path of the legacy flat-file vault state DB
        /// (`{state_dir}/vault-{id}.db`).
        path: String,
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
    /// The `state/` directory holding per-vault subdirectories
    /// (`vault-{vault_id}/`). Slice 2b replaced the singleton `state.db`
    /// file with one file per vault under `state/`; P2b.0 drops each
    /// vault's `state.db` into its own subdirectory so future slices
    /// can add per-vault `raft.db` / `blocks.db` / `events.db`
    /// alongside. The per-org `StateLayer`'s factory resolves each
    /// vault's state DB path via [`RegionStorage::vault_db_path`]
    /// (which composes `state_dir.join("vault-{id}").join("state.db")`)
    /// and creates the parent `vault-{id}/` directory lazily on first
    /// reference.
    state_dir: PathBuf,
    /// Per-organization coordination database (`_meta.db`) introduced by
    /// Slice 1 of per-vault consensus. Owns the `_meta:last_applied`
    /// crash-recovery sentinel; syncs **after** every per-vault state
    /// DB / raft.db / blocks.db / events.db in the checkpointer +
    /// shutdown fsync order so the sentinel on disk never outruns the
    /// entity data it references.
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

    /// Returns the per-organization `state/` directory — the parent of
    /// the per-vault `vault-{id}/` subdirectories. The directory is
    /// created by [`RegionStorageManager::open_organization`] before
    /// this value is returned; each `vault-{id}/` child directory is
    /// created lazily by the factory closure in
    /// [`RaftManager::open_region_storage`] the first time a vault is
    /// materialised.
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    /// Returns the per-organization meta database handle (`_meta.db`).
    ///
    /// Owns the `_meta:last_applied` crash-recovery sentinel; the
    /// checkpointer must sync this handle **after** every per-vault
    /// state DB / raft.db / blocks.db / events.db on every tick.
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

    /// Composes the on-disk path of a vault's state database
    /// (`{state_dir}/vault-{vault_id}/state.db`). Used by the factory
    /// closure passed to [`StateLayer::new`] — the factory calls this
    /// helper to derive the file path, then opens (or creates) the DB.
    ///
    /// P2b.0 migrates the per-vault state DB from the flat
    /// `vault-{id}.db` file form down into a `vault-{id}/` directory so
    /// future slices (P2b.2+) can add `raft.db`, `blocks.db`, and
    /// `events.db` alongside it for per-vault consensus.
    pub fn vault_db_path(&self, vault_id: inferadb_ledger_types::VaultId) -> PathBuf {
        self.vault_dir(vault_id).join("state.db")
    }

    /// Composes the on-disk directory for a single vault's per-vault
    /// files (`{state_dir}/vault-{vault_id}/`).
    ///
    /// Introduced by P2b.0: the directory holds `state.db` today and
    /// will hold the per-vault `raft.db`, `blocks.db`, and `events.db`
    /// in later slices of the per-vault consensus plan. The directory
    /// is created lazily by the factory closure immediately before it
    /// opens the first per-vault file (state.db) — no eager creation
    /// per vault at `open_organization` time.
    pub fn vault_dir(&self, vault_id: inferadb_ledger_types::VaultId) -> PathBuf {
        self.state_dir.join(format!("vault-{}", vault_id.value()))
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

        // Slice 2b legacy detection: the per-vault consensus layout
        // replaces the pre-Slice-2b per-org `state.db` **file** with a
        // `state/` **directory** holding one `vault-{id}.db` per vault.
        // If we find `state.db` as a file at the old path, the data_dir
        // was written by a pre-Slice-2b build — refuse to boot before
        // opening any DB so the apply pipeline never observes the
        // conflicting layout.
        //
        // Runs BEFORE anything touches state.db on disk: opening a legacy
        // file would need to be unwound on detection, and opening the
        // directory path as a Database would fail with a less specific
        // error.
        let legacy_state_db = organization_dir.join("state.db");
        if legacy_state_db.is_file() {
            return Err(RegionStorageError::LegacyPerOrgStateDb {
                region,
                organization_id,
                state_db_path: legacy_state_db.display().to_string(),
            });
        }

        // P2b.0: the per-vault state DBs live under
        // `{organization_dir}/state/vault-{id}/state.db`. Create the
        // parent `state/` directory eagerly so the per-vault factory
        // (closure supplied to `StateLayer::new`) can assume it exists;
        // the `vault-{id}/` child dir is created lazily by the factory
        // itself on first reference to each vault.
        let state_dir = organization_dir.join("state");
        std::fs::create_dir_all(&state_dir).map_err(|e| RegionStorageError::Storage {
            region,
            organization_id,
            message: format!("failed to create state directory {}: {e}", state_dir.display()),
        })?;

        // P2b.0 legacy detection: between Slice 2b and P2b.0 the
        // per-vault state DB lived as a flat file at
        // `{state_dir}/vault-{id}.db`. P2b.0 moves each vault's
        // state.db down one level into `{state_dir}/vault-{id}/`. If
        // we find any `vault-{id}.db` **file** directly inside
        // `state_dir`, the data_dir was written by a pre-P2b.0 build —
        // refuse to boot before any vault DB is opened so a partial
        // migration can't race and double-open a file under a stale
        // path. The check scans `state_dir` itself (not
        // `organization_dir`) because only `state_dir` contains the
        // `vault-*` naming convention.
        let state_entries =
            std::fs::read_dir(&state_dir).map_err(|e| RegionStorageError::Storage {
                region,
                organization_id,
                message: format!("failed to scan state directory {}: {e}", state_dir.display()),
            })?;
        for entry in state_entries {
            let entry = entry.map_err(|e| RegionStorageError::Storage {
                region,
                organization_id,
                message: format!(
                    "failed to read state directory entry in {}: {e}",
                    state_dir.display()
                ),
            })?;
            let path = entry.path();
            if path.is_file()
                && let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.starts_with("vault-")
                && name.ends_with(".db")
            {
                return Err(RegionStorageError::LegacyVaultLayout {
                    region,
                    organization_id,
                    path: path.display().to_string(),
                });
            }
        }

        // Open or create blocks database
        let blocks_db_path = organization_dir.join("blocks.db");
        let blocks_db = open_or_create_db(&blocks_db_path, region, organization_id)?;
        let blocks_db = Arc::new(blocks_db);

        // Open or create the per-organization meta database.
        //
        // `_meta.db` was introduced by Slice 1 of per-vault consensus — it
        // owns the `_meta:last_applied` crash-recovery sentinel (previously
        // bundled into state.db's Entities table). Opening this DB here
        // ensures every call path that consumes `state_dir()` also has
        // `meta_db()` available without a second discovery step.
        //
        // Slice 2b retires the pre-Slice-1 in-state.db legacy detector:
        // under the new layout `state.db` as a file is caught by
        // `LegacyPerOrgStateDb` above, and there is no singleton
        // `state.db` left to probe for the old sentinel. A freshly-
        // created `_meta.db` stays pristine until the apply path
        // records its first sentinel.
        let meta_db_path = organization_dir.join("_meta.db");
        let meta_db = open_or_create_db(&meta_db_path, region, organization_id)?;
        let meta_db = Arc::new(meta_db);

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
            state_dir,
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

    /// Discovers per-organization Raft groups with persisted data under
    /// `region`.
    ///
    /// Scans `{region_dir}/` for subdirectories whose names parse as a
    /// non-negative integer and that carry the per-org layout marker
    /// (`state/` subdirectory). Returns the internal [`OrganizationId`]
    /// list in ascending order.
    ///
    /// `OrganizationId::new(0)` is skipped — the system org in the GLOBAL
    /// region, and the data-region control plane in data regions, are
    /// both rehydrated via [`discover_existing_regions`] (and the
    /// system-group bootstrap) rather than through this path.
    ///
    /// Entries that fail the layout check (non-numeric name, missing
    /// `state/` subdirectory, non-directory entry) are silently skipped
    /// and logged at debug level. A single malformed entry does not fail
    /// the whole scan.
    ///
    /// # Errors
    ///
    /// Returns [`RegionStorageError::Storage`] if the region directory
    /// cannot be opened for reading for a reason other than "does not
    /// exist". A missing region directory yields an empty result.
    pub fn discover_existing_organizations(
        &self,
        region: Region,
    ) -> Result<Vec<OrganizationId>, RegionStorageError> {
        let region_dir = self.region_dir(region);
        let entries = match std::fs::read_dir(&region_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(RegionStorageError::Storage {
                    region,
                    organization_id: OrganizationId::new(0),
                    message: format!(
                        "failed to read region directory {}: {e}",
                        region_dir.display()
                    ),
                });
            },
        };

        let mut discovered: Vec<OrganizationId> = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    tracing::debug!(
                        region = region.as_str(),
                        error = %e,
                        "Organization discovery: skipping entry with I/O error",
                    );
                    continue;
                },
            };
            let path = entry.path();
            if !path.is_dir() {
                tracing::debug!(
                    region = region.as_str(),
                    entry = %path.display(),
                    "Organization discovery: skipping non-directory entry",
                );
                continue;
            }
            let dir_name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => {
                    tracing::debug!(
                        region = region.as_str(),
                        entry = %path.display(),
                        "Organization discovery: skipping non-UTF-8 directory name",
                    );
                    continue;
                },
            };
            let organization_id = match dir_name.parse::<i64>() {
                Ok(n) => OrganizationId::new(n),
                Err(_) => {
                    // `_meta/` (the region-group subdirectory) and any
                    // non-numeric sibling directories land here.
                    tracing::debug!(
                        region = region.as_str(),
                        dir_name = %dir_name,
                        "Organization discovery: skipping non-numeric directory",
                    );
                    continue;
                },
            };
            // Skip the data-region control plane (organization id 0).
            if organization_id == OrganizationId::new(0) {
                continue;
            }
            // Strict P2b.0 layout check: the per-org directory must have
            // a `state/` subdirectory.
            if !path.join("state").is_dir() {
                tracing::debug!(
                    region = region.as_str(),
                    organization_id = organization_id.value(),
                    entry = %path.display(),
                    "Organization discovery: skipping org directory without state/ subdir",
                );
                continue;
            }
            discovered.push(organization_id);
        }
        discovered.sort_by_key(|id| id.value());
        Ok(discovered)
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

// Slice 2b retires `detect_legacy_layout` (the Slice 1 detector for an
// old `_meta:last_applied` sentinel buried inside state.db's Entities
// table). The new per-vault layout removes the singleton `state.db`
// file; the Slice 2b legacy detector inlined above (`LegacyPerOrgStateDb`)
// catches the pre-Slice-2b layout before any Database is opened. With
// no singleton `state.db`, there is no artefact left to probe for the
// old sentinel, so the Slice 1 detector is moot.

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

        // Verify directory structure under the P2b.0 layout: `state/`
        // is a directory (per-vault `vault-{id}/` subdirs land inside
        // via the factory on first open), blocks.db / events.db are
        // files at the org root.
        let org_dir = temp.path().join("global").join("0");
        assert!(org_dir.exists());
        assert!(
            org_dir.join("state").is_dir(),
            "state/ must be a directory under the per-vault layout"
        );
        assert!(
            !org_dir.join("state.db").exists(),
            "pre-Slice-2b state.db singleton must not be created"
        );
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
        assert!(org_dir.join("state").is_dir());
        assert!(!org_dir.join("state.db").exists());
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
            assert!(dir.join("state").is_dir());
            assert!(!dir.join("state.db").exists());
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

        // Slice 2b: each org's state lives under its own `state/`
        // directory; the per-vault DB is opened lazily by the StateLayer
        // factory, not by `RegionStorage`. This test only needs to
        // confirm the two orgs have disjoint on-disk homes, which the
        // existing blocks.db write lock can prove.
        let write_a = storage_a.blocks_db().write();
        let write_b = storage_b.blocks_db().write();

        assert!(write_a.is_ok(), "write transaction on system blocks should succeed");
        assert!(write_b.is_ok(), "write transaction on us-east-va org blocks should succeed");
        // And the `state/` directories are independent under each org dir.
        assert!(storage_a.state_dir().ends_with("global/0/state"));
        assert!(storage_b.state_dir().ends_with("us-east-va/42/state"));
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

    #[test]
    fn test_discover_existing_organizations_empty_region() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // Region directory does not exist — empty result, no error.
        let discovered = mgr
            .discover_existing_organizations(Region::US_EAST_VA)
            .expect("scan must succeed on missing region dir");
        assert!(discovered.is_empty());
    }

    #[test]
    fn test_discover_existing_organizations_filters_and_sorts() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let region_dir = temp.path().join("us-east-va");
        // Three well-formed per-org directories (including the id=0
        // control plane, which must be filtered out).
        for org in [0_i64, 1, 2] {
            let dir = region_dir.join(org.to_string()).join("state");
            std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
        }
        // Junk entries: non-numeric directory name (future `_meta/`
        // sibling), numeric directory without the `state/` marker, and
        // a plain file with a numeric name.
        std::fs::create_dir_all(region_dir.join("not_a_number"))
            .expect("create non-numeric entry");
        std::fs::create_dir_all(region_dir.join("99")).expect("create malformed org 99");
        std::fs::write(region_dir.join("42"), b"file, not a directory")
            .expect("create file masquerading as org");

        let discovered = mgr
            .discover_existing_organizations(Region::US_EAST_VA)
            .expect("scan must succeed");
        assert_eq!(
            discovered,
            vec![OrganizationId::new(1), OrganizationId::new(2)],
            "must exclude org 0, non-numeric names, files, and dirs missing state/"
        );
    }

    #[test]
    fn test_discover_existing_organizations_global_skips_system_org() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        // GLOBAL always houses the system org at id 0 — the discovery
        // helper must skip it, since the system-group bootstrap brings
        // it back up on restart.
        let global_dir = temp.path().join("global");
        std::fs::create_dir_all(global_dir.join("0").join("state")).expect("create global/0/state");

        let discovered = mgr
            .discover_existing_organizations(Region::GLOBAL)
            .expect("scan must succeed");
        assert!(
            discovered.is_empty(),
            "GLOBAL's system org (id 0) must not be returned by this helper"
        );
    }

    // ========================================================================
    // Slice 2b legacy-layout detection
    // ========================================================================

    /// Under the Slice 2b per-vault layout, the pre-Slice-2b singleton
    /// `state.db` file is illegal. If `open_organization` finds such a
    /// file at the old path, it must refuse to boot with
    /// `LegacyPerOrgStateDb` — before creating `state/` or opening
    /// blocks.db / _meta.db / events.db.
    #[test]
    fn test_open_organization_refuses_pre_slice_2b_state_db_file() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let org = OrganizationId::new(42);
        let org_dir = mgr.organization_dir(Region::US_EAST_VA, org);
        std::fs::create_dir_all(&org_dir).expect("create org dir");

        // Simulate a pre-Slice-2b data_dir: a `state.db` file alongside
        // the other per-org DBs.
        std::fs::write(org_dir.join("state.db"), b"legacy-state-db-bytes")
            .expect("create legacy state.db file");

        let result = mgr.open_organization(Region::US_EAST_VA, org);
        match result {
            Err(RegionStorageError::LegacyPerOrgStateDb {
                region,
                organization_id,
                state_db_path,
            }) => {
                assert_eq!(region, Region::US_EAST_VA);
                assert_eq!(organization_id, org);
                assert!(
                    state_db_path.ends_with("state.db"),
                    "state.db path reported: {state_db_path}"
                );
            },
            Ok(_) => panic!("expected LegacyPerOrgStateDb, got Ok"),
            Err(other) => panic!("expected LegacyPerOrgStateDb, got: {other:?}"),
        }
    }

    /// A `state/` subdirectory (the new layout marker) must not be
    /// mistaken for the legacy `state.db` file — `open_organization`
    /// should succeed.
    #[test]
    fn test_open_organization_state_directory_is_not_legacy() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let org = OrganizationId::new(44);
        let org_dir = mgr.organization_dir(Region::US_EAST_VA, org);
        std::fs::create_dir_all(&org_dir).expect("create org dir");
        // Pre-create `state/` as a directory (what Slice 2b wants).
        std::fs::create_dir_all(org_dir.join("state")).expect("pre-create state/ dir");
        // And pre-create the `state.db` path **as a directory**, not a
        // file, to confirm the detector is strictly file-based.
        // (Intentionally skipped — the detector uses `is_file()`, so a
        // directory at that path would be treated as absent.)

        mgr.open_organization(Region::US_EAST_VA, org)
            .expect("open with pre-existing state/ directory");
    }

    /// P2b.0 moves per-vault state DBs from
    /// `{state_dir}/vault-{id}.db` (flat file) to
    /// `{state_dir}/vault-{id}/state.db` (nested inside per-vault
    /// directory). `open_organization` must refuse to boot on the old
    /// flat layout — no migration is supported.
    #[test]
    fn test_open_organization_refuses_pre_p2b0_flat_vault_db_file() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let org = OrganizationId::new(77);
        let org_dir = mgr.organization_dir(Region::US_EAST_VA, org);
        let state_dir = org_dir.join("state");
        std::fs::create_dir_all(&state_dir).expect("create state/ dir");

        // Simulate a pre-P2b.0 data_dir: a flat `vault-{id}.db` file
        // sitting directly inside `state/`.
        std::fs::write(state_dir.join("vault-1.db"), b"legacy-flat-vault-state-bytes")
            .expect("create legacy flat vault-1.db file");

        let result = mgr.open_organization(Region::US_EAST_VA, org);
        match result {
            Err(RegionStorageError::LegacyVaultLayout { region, organization_id, path }) => {
                assert_eq!(region, Region::US_EAST_VA);
                assert_eq!(organization_id, org);
                assert!(
                    path.ends_with("vault-1.db"),
                    "legacy flat vault-{{id}}.db path reported: {path}"
                );
            },
            Ok(_) => panic!("expected LegacyVaultLayout, got Ok"),
            Err(other) => panic!("expected LegacyVaultLayout, got: {other:?}"),
        }
    }

    /// A `vault-{id}/` **directory** inside `state/` (the P2b.0 layout
    /// marker) must not be mistaken for the legacy flat file — the
    /// detector is strictly file-based.
    #[test]
    fn test_open_organization_vault_directory_is_not_legacy() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let org = OrganizationId::new(88);
        let org_dir = mgr.organization_dir(Region::US_EAST_VA, org);
        let state_dir = org_dir.join("state");
        std::fs::create_dir_all(state_dir.join("vault-3")).expect("pre-create vault-3/ directory");

        mgr.open_organization(Region::US_EAST_VA, org)
            .expect("open with pre-existing vault-{id}/ directory");
    }

    /// Confirms `vault_db_path` returns the P2b.0 nested form and the
    /// `vault_dir` helper returns the matching parent directory.
    #[test]
    fn test_vault_db_path_returns_nested_per_vault_form() {
        let temp = TestDir::new();
        let mgr = RegionStorageManager::new(temp.path().to_path_buf());

        let storage =
            mgr.open_organization(Region::US_EAST_VA, OrganizationId::new(101)).expect("open org");

        let vault = inferadb_ledger_types::VaultId::new(42);
        let path = storage.vault_db_path(vault);
        assert!(
            path.ends_with("state/vault-42/state.db"),
            "vault_db_path must be nested under vault-{{id}}/ per P2b.0: {}",
            path.display()
        );

        let dir = storage.vault_dir(vault);
        assert!(
            dir.ends_with("state/vault-42"),
            "vault_dir must be the per-vault directory under state/: {}",
            dir.display()
        );
        assert_eq!(path.parent(), Some(dir.as_path()));
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
        assert!(meta_db_path.exists(), "meta.db must be created alongside the state/ dir");

        // And `state/` must be a directory (Slice 2b layout marker).
        let state_dir = temp.path().join("us-east-va").join("99").join("state");
        assert!(state_dir.is_dir(), "state/ must be a directory under Slice 2b");

        // Round-trip: meta_db() returns a usable handle.
        assert!(storage.meta_db().read().is_ok());
        // And `state_dir()` surfaces the on-disk path for the factory.
        assert_eq!(storage.state_dir(), state_dir);
    }
}
