//! Backup and restore operations for InferaDB Ledger.
//!
//! Builds on the existing snapshot infrastructure to provide:
//! - On-demand backup creation via `CreateBackup` RPC
//! - Backup listing with metadata via `ListBackups` RPC
//! - Restore from backup via `RestoreBackup` RPC
//! - Automated periodic backups via `BackupJob`
//!
//! Backups are snapshot files copied to a configurable destination directory
//! with metadata files for fast listing without reading full snapshots.

use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use inferadb_ledger_state::{Snapshot, SnapshotError, SnapshotManager};
use inferadb_ledger_store::{Database, StorageBackend, TableId};
use inferadb_ledger_types::{Hash, Region, config::BackupConfig, hash::sha256};
use snafu::{ResultExt, Snafu};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::consensus_handle::ConsensusHandle;

/// Backup file extension.
const BACKUP_EXT: &str = ".backup";

/// Metadata file extension (JSON sidecar).
const META_EXT: &str = ".meta.json";

/// Backup operation failure.
#[derive(Debug, Snafu)]
pub enum BackupError {
    /// IO error during backup operations.
    #[snafu(display("Backup IO error: {source}"))]
    Io {
        /// The underlying IO error.
        source: std::io::Error,
    },

    /// Snapshot error during backup creation.
    #[snafu(display("Snapshot error: {source}"))]
    Snapshot {
        /// The underlying snapshot error.
        source: SnapshotError,
    },

    /// Backup not found.
    #[snafu(display("Backup not found: {backup_id}"))]
    NotFound {
        /// The requested backup ID.
        backup_id: String,
    },

    /// Restore not confirmed.
    #[snafu(display("Restore requires confirm=true for safety"))]
    NotConfirmed,

    /// Invalid backup (checksum mismatch or corrupt).
    #[snafu(display("Invalid backup: {message}"))]
    Invalid {
        /// Description of what is invalid.
        message: String,
    },

    /// Schema version mismatch.
    #[snafu(display(
        "Schema version mismatch: backup has {backup_version}, server expects {server_version}"
    ))]
    SchemaVersionMismatch {
        /// Schema version found in the backup.
        backup_version: u32,
        /// Schema version expected by this server.
        server_version: u32,
    },

    /// Serialization error.
    #[snafu(display("Serialization error: {message}"))]
    Serialization {
        /// Description of the serialization failure.
        message: String,
    },

    /// Storage engine error during page export/import.
    #[snafu(display("Storage error: {source}"))]
    Storage {
        /// The underlying storage error.
        source: inferadb_ledger_store::Error,
    },

    /// Cross-region restore rejected for protected region backup.
    #[snafu(display(
        "Cross-region restore rejected: backup region {backup_region} requires \
         residency, but restoring node is in region {node_region}"
    ))]
    CrossRegionRestore {
        /// Region the backup belongs to.
        backup_region: Region,
        /// Region of the restoring node.
        node_region: Region,
    },

    /// Backup path overlaps with key directory.
    #[snafu(display(
        "Backup path {path} is inside the keys directory — key material must not be \
         included in data backups"
    ))]
    KeyDirectoryExcluded {
        /// The offending path.
        path: String,
    },

    /// Invalid backup ID (path traversal attempt).
    #[snafu(display("Invalid backup ID: must not contain path separators or '..' ({backup_id})"))]
    InvalidBackupId {
        /// The offending backup ID.
        backup_id: String,
    },
}

/// Result type for backup operations.
pub type Result<T> = std::result::Result<T, BackupError>;

/// Validates that a backup ID does not contain path traversal sequences.
///
/// Rejects IDs containing `/`, `\`, or `..` to prevent directory traversal
/// attacks when the ID is used in file path construction.
fn validate_backup_id(backup_id: &str) -> Result<()> {
    if backup_id.contains('/')
        || backup_id.contains('\\')
        || backup_id.contains("..")
        || backup_id.is_empty()
    {
        return Err(BackupError::InvalidBackupId { backup_id: backup_id.to_string() });
    }
    Ok(())
}

/// Type of backup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BackupType {
    /// Full backup containing complete database state (snapshot-based).
    Full,
    /// Incremental backup containing only pages changed since the base backup.
    Incremental,
}

/// Metadata about an available backup (stored as JSON sidecar).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackupMetadata {
    /// Unique backup identifier (timestamp-based).
    pub backup_id: String,
    /// Region this backup belongs to.
    pub region: Region,
    /// Region height at backup time.
    pub region_height: u64,
    /// Backup file path.
    pub backup_path: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// When the backup was created.
    pub created_at: DateTime<Utc>,
    /// SHA-256 checksum of the backup file.
    pub checksum: Hash,
    /// Chain commitment hash (accumulated header hash from snapshot).
    pub chain_commitment_hash: Hash,
    /// Snapshot format version.
    pub schema_version: u32,
    /// Optional user-provided tag.
    pub tag: String,
    /// Backup type (full or incremental). Defaults to Full for backward compatibility.
    #[serde(default = "default_backup_type")]
    pub backup_type: BackupType,
    /// For incremental backups, the ID of the base (full) backup.
    #[serde(default)]
    pub base_backup_id: Option<String>,
    /// Number of pages in this backup (for incremental backups).
    #[serde(default)]
    pub page_count: Option<u64>,
}

fn default_backup_type() -> BackupType {
    BackupType::Full
}

impl BackupMetadata {
    /// Validates that this backup can be restored on a node in `node_region`.
    ///
    /// Protected regions restrict restore to in-region nodes only.
    /// Non-protected regions allow restore on any node.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::CrossRegionRestore`] if the backup belongs to a
    /// protected region and `node_region` differs.
    pub fn validate_restore_region(&self, node_region: Region) -> Result<()> {
        if self.region.requires_residency() && self.region != node_region {
            return Err(BackupError::CrossRegionRestore {
                backup_region: self.region,
                node_region,
            });
        }
        Ok(())
    }
}

/// Validates that a path does not overlap with the keys directory.
///
/// Key material (`{data_dir}/keys/`) must be excluded from data backup
/// operations. Including the RMK alongside the ciphertext it protects
/// defeats envelope encryption's purpose.
///
/// # Errors
///
/// Returns [`BackupError::KeyDirectoryExcluded`] if `path` is inside `keys_dir`.
pub fn validate_not_keys_directory(path: &Path, keys_dir: &Path) -> Result<()> {
    if path.starts_with(keys_dir) {
        return Err(BackupError::KeyDirectoryExcluded { path: path.display().to_string() });
    }
    Ok(())
}

/// Manages backup operations for a region.
pub struct BackupManager {
    /// Backup destination directory.
    backup_dir: PathBuf,
    /// Maximum number of backups to retain.
    retention_count: usize,
}

impl BackupManager {
    /// Creates a new backup manager.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::Io`] if the backup destination directory
    /// cannot be created.
    pub fn new(config: &BackupConfig) -> Result<Self> {
        let backup_dir = PathBuf::from(&config.destination);
        fs::create_dir_all(&backup_dir).context(IoSnafu)?;

        Ok(Self { backup_dir, retention_count: config.retention_count })
    }

    /// Creates a backup from the given snapshot.
    ///
    /// Writes the snapshot to the backup directory and creates a metadata sidecar.
    /// Returns the backup metadata. Old backups are pruned according to the
    /// retention count.
    ///
    /// No `Database::sync_state` hook here. This function
    /// accepts a pre-built `Snapshot` value and writes it plus a sidecar to
    /// the backup directory. The sync-before-read guarantee is the caller's
    /// responsibility: any caller that builds the `Snapshot` from live state
    /// must call `Database::sync_state` first. The snapshot-build hook on
    /// `LedgerSnapshotBuilder::build_snapshot` covers the Raft snapshot path;
    /// other callers (e.g. `AdminService::create_backup` assembling a
    /// `Snapshot` from `StateLayer` + `AppliedState`) must sync before
    /// calling in.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError`] if the snapshot file or metadata sidecar cannot
    /// be written, serialization fails, or pruning encounters an I/O error.
    pub fn create_backup(&self, snapshot: &Snapshot, tag: &str) -> Result<BackupMetadata> {
        let now = Utc::now();
        let backup_id =
            format!("{}-{:09}", now.format("%Y%m%dT%H%M%SZ"), snapshot.header.region_height);

        let backup_filename = format!("{backup_id}{BACKUP_EXT}");
        let backup_path = self.backup_dir.join(&backup_filename);

        // Write snapshot to backup destination
        snapshot.write_to_file(&backup_path).context(SnapshotSnafu)?;

        // Get file size
        let size_bytes = fs::metadata(&backup_path).context(IoSnafu)?.len();

        let metadata = BackupMetadata {
            backup_id: backup_id.clone(),
            region: snapshot.header.region,
            region_height: snapshot.header.region_height,
            backup_path: backup_path.display().to_string(),
            size_bytes,
            created_at: now,
            checksum: snapshot.header.checksum,
            chain_commitment_hash: snapshot.header.chain_commitment.accumulated_header_hash,
            schema_version: snapshot.header.version,
            tag: tag.to_string(),
            backup_type: BackupType::Full,
            base_backup_id: None,
            page_count: None,
        };

        // Write metadata sidecar
        let meta_path = self.backup_dir.join(format!("{backup_id}{META_EXT}"));
        let meta_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| BackupError::Serialization { message: e.to_string() })?;
        fs::write(&meta_path, meta_json).context(IoSnafu)?;

        // Prune old backups
        self.prune()?;

        info!(
            backup_id = %metadata.backup_id,
            region_height = metadata.region_height,
            size_bytes = metadata.size_bytes,
            "Backup created"
        );

        Ok(metadata)
    }

    /// Lists available backups sorted by creation time (newest first).
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::Io`] if the backup directory cannot be read.
    /// Corrupt metadata files are logged and skipped rather than failing.
    pub fn list_backups(&self, limit: usize) -> Result<Vec<BackupMetadata>> {
        if !self.backup_dir.exists() {
            return Ok(Vec::new());
        }

        let mut backups = Vec::new();

        for entry in fs::read_dir(&self.backup_dir).context(IoSnafu)? {
            let entry = entry.context(IoSnafu)?;
            let filename = entry.file_name();
            let name = filename.to_string_lossy();

            if name.ends_with(META_EXT) {
                let contents = fs::read_to_string(entry.path()).context(IoSnafu)?;
                match serde_json::from_str::<BackupMetadata>(&contents) {
                    Ok(meta) => backups.push(meta),
                    Err(e) => {
                        warn!(
                            path = %entry.path().display(),
                            error = %e,
                            "Skipping corrupt backup metadata file"
                        );
                    },
                }
            }
        }

        // Sort newest first
        backups.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        if limit > 0 && backups.len() > limit {
            backups.truncate(limit);
        }

        Ok(backups)
    }

    /// Loads a backup snapshot by backup ID.
    ///
    /// Reads the backup file and verifies its integrity (checksum validation
    /// is performed by `Snapshot::read_from_file`).
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::NotFound`] if no backup with the given ID exists,
    /// or [`BackupError::Snapshot`] if the file is corrupt or unreadable.
    pub fn load_backup(&self, backup_id: &str) -> Result<Snapshot> {
        validate_backup_id(backup_id)?;
        let backup_path = self.backup_dir.join(format!("{backup_id}{BACKUP_EXT}"));

        if !backup_path.exists() {
            return Err(BackupError::NotFound { backup_id: backup_id.to_string() });
        }

        Snapshot::read_from_file(&backup_path).context(SnapshotSnafu)
    }

    /// Returns metadata for a specific backup.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::NotFound`] if no metadata file exists for the
    /// given ID, or [`BackupError::Io`] / [`BackupError::Serialization`] if
    /// the file cannot be read or parsed.
    pub fn get_metadata(&self, backup_id: &str) -> Result<BackupMetadata> {
        validate_backup_id(backup_id)?;
        let meta_path = self.backup_dir.join(format!("{backup_id}{META_EXT}"));

        if !meta_path.exists() {
            return Err(BackupError::NotFound { backup_id: backup_id.to_string() });
        }

        let contents = fs::read_to_string(&meta_path).context(IoSnafu)?;
        serde_json::from_str(&contents)
            .map_err(|e| BackupError::Serialization { message: e.to_string() })
    }

    /// Prunes old backups, keeping only the most recent `retention_count`.
    fn prune(&self) -> Result<()> {
        let backups = self.list_backups(0)?;

        if backups.len() <= self.retention_count {
            return Ok(());
        }

        // Remove oldest backups (list is sorted newest-first)
        for backup in backups.into_iter().skip(self.retention_count) {
            let backup_path = self.backup_dir.join(format!("{}{BACKUP_EXT}", backup.backup_id));
            let meta_path = self.backup_dir.join(format!("{}{META_EXT}", backup.backup_id));

            if backup_path.exists() {
                fs::remove_file(&backup_path).context(IoSnafu)?;
            }
            if meta_path.exists() {
                fs::remove_file(&meta_path).context(IoSnafu)?;
            }

            debug!(
                backup_id = %backup.backup_id,
                region_height = backup.region_height,
                "Pruned old backup"
            );
        }

        Ok(())
    }

    /// Returns the backup directory path.
    pub fn backup_dir(&self) -> &Path {
        &self.backup_dir
    }
}

/// Magic bytes for page-level backup files.
const PAGE_BACKUP_MAGIC: &[u8; 4] = b"LPBK";

/// Current page backup format version.
const PAGE_BACKUP_VERSION: u32 = 1;

/// Extension for page-level backup files.
const PAGE_BACKUP_EXT: &str = ".pagebackup";

impl BackupManager {
    /// Creates an incremental page-level backup containing only dirty pages.
    ///
    /// The base backup must exist and be a full backup. The incremental file
    /// contains a header followed by `(page_id, page_data)` entries for each
    /// dirty page, followed by a SHA-256 checksum.
    ///
    /// After a successful backup, the caller should clear the database's dirty
    /// bitmap via `Database::clear_dirty_bitmap()`.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::NotFound`] if the base backup does not exist,
    /// [`BackupError::Invalid`] if the base is incremental or there are no
    /// dirty pages, or [`BackupError::Io`] if the file cannot be written.
    pub async fn create_incremental_backup<B: StorageBackend + 'static>(
        &self,
        db: &Arc<Database<B>>,
        base_backup_id: &str,
        region: Region,
        region_height: u64,
        tag: &str,
    ) -> Result<BackupMetadata> {
        // Force `sync_state` so the backend's last-synced state matches
        // `committed_state`. Incremental backups read directly
        // from the backend (`dirty_page_ids` + `export_pages`); under lazy
        // commits those dirty pages may still be in the in-process cache,
        // not yet persisted. Syncing first guarantees the emitted pages are
        // the same bytes a fresh reader would see on disk.
        Arc::clone(db).sync_state().await.context(StorageSnafu)?;

        // Verify the base backup exists
        let base_meta = self.get_metadata(base_backup_id)?;

        if base_meta.backup_type == BackupType::Incremental {
            return Err(BackupError::Invalid {
                message: format!(
                    "Base backup {base_backup_id} is incremental; \
                     incremental backups must reference a full backup"
                ),
            });
        }

        let dirty_ids = db.dirty_page_ids();
        if dirty_ids.is_empty() {
            return Err(BackupError::Invalid { message: "No dirty pages to back up".to_string() });
        }

        let pages = db.export_pages(&dirty_ids);
        let page_size = db.page_size();
        let table_roots = db.table_root_pages();

        let now = Utc::now();
        let backup_id = format!("{}-{region_height:09}-inc", now.format("%Y%m%dT%H%M%SZ"));
        let backup_path = self.backup_dir.join(format!("{backup_id}{PAGE_BACKUP_EXT}"));

        let data = Self::encode_page_backup(
            region_height,
            page_size as u32,
            &table_roots,
            &pages,
            Some(base_backup_id),
        );

        let checksum = sha256(&data);
        let mut file = fs::File::create(&backup_path).context(IoSnafu)?;
        file.write_all(&data).context(IoSnafu)?;
        file.write_all(&checksum).context(IoSnafu)?;
        file.sync_all().context(IoSnafu)?;

        let size_bytes = (data.len() + 32) as u64;

        let metadata = BackupMetadata {
            backup_id: backup_id.clone(),
            region,
            region_height,
            backup_path: backup_path.display().to_string(),
            size_bytes,
            created_at: now,
            checksum,
            chain_commitment_hash: base_meta.chain_commitment_hash,
            schema_version: base_meta.schema_version,
            tag: tag.to_string(),
            backup_type: BackupType::Incremental,
            base_backup_id: Some(base_backup_id.to_string()),
            page_count: Some(pages.len() as u64),
        };

        // Write metadata sidecar
        let meta_path = self.backup_dir.join(format!("{backup_id}{META_EXT}"));
        let meta_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| BackupError::Serialization { message: e.to_string() })?;
        fs::write(&meta_path, meta_json).context(IoSnafu)?;

        info!(
            backup_id = %metadata.backup_id,
            base_backup_id = %base_backup_id,
            pages = pages.len(),
            size_bytes = metadata.size_bytes,
            "Incremental backup created"
        );

        Ok(metadata)
    }

    /// Encodes page data into the page-level backup binary format.
    ///
    /// Format:
    /// ```text
    /// [4 bytes: magic "LPBK"]
    /// [4 bytes: version]
    /// [8 bytes: region_height]
    /// [4 bytes: page_size]
    /// [8 bytes: page_count]
    /// [TableId::COUNT * 8 bytes: table_roots]
    /// [1 byte: has_base (0 or 1)]
    /// [if has_base: 4 bytes len + N bytes base_backup_id UTF-8]
    /// --- page entries ---
    /// [8 bytes: page_id][page_size bytes: page_data] × page_count
    /// --- (checksum appended externally) ---
    /// ```
    fn encode_page_backup(
        region_height: u64,
        page_size: u32,
        table_roots: &[u64; TableId::COUNT],
        pages: &[(u64, Vec<u8>)],
        base_backup_id: Option<&str>,
    ) -> Vec<u8> {
        let mut buf = Vec::new();

        // Header
        buf.extend_from_slice(PAGE_BACKUP_MAGIC);
        buf.extend_from_slice(&PAGE_BACKUP_VERSION.to_le_bytes());
        buf.extend_from_slice(&region_height.to_le_bytes());
        buf.extend_from_slice(&page_size.to_le_bytes());
        buf.extend_from_slice(&(pages.len() as u64).to_le_bytes());

        // Table roots
        for &root in table_roots {
            buf.extend_from_slice(&root.to_le_bytes());
        }

        // Base backup ID (optional)
        match base_backup_id {
            Some(id) => {
                buf.push(1);
                let id_bytes = id.as_bytes();
                buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(id_bytes);
            },
            None => {
                buf.push(0);
            },
        }

        // Page entries
        for (page_id, data) in pages {
            buf.extend_from_slice(&page_id.to_le_bytes());
            buf.extend_from_slice(data);
        }

        buf
    }

    /// Decodes a page-level backup file, verifying its checksum.
    ///
    /// Returns the decoded header and page entries.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::NotFound`] if no page backup exists for the given ID,
    /// [`BackupError::Io`] if the file cannot be read, or [`BackupError::Invalid`]
    /// if the checksum fails or the binary format is corrupt.
    pub fn load_page_backup(&self, backup_id: &str) -> Result<PageBackupData> {
        validate_backup_id(backup_id)?;
        let path = self.backup_dir.join(format!("{backup_id}{PAGE_BACKUP_EXT}"));
        if !path.exists() {
            return Err(BackupError::NotFound { backup_id: backup_id.to_string() });
        }

        let mut file_data = Vec::new();
        fs::File::open(&path).context(IoSnafu)?.read_to_end(&mut file_data).context(IoSnafu)?;

        if file_data.len() < 32 {
            return Err(BackupError::Invalid { message: "Page backup file too small".to_string() });
        }

        // Split data and checksum
        let (data, stored_checksum) = file_data.split_at(file_data.len() - 32);
        let computed_checksum = sha256(data);

        if computed_checksum
            != <[u8; 32]>::try_from(stored_checksum).map_err(|_| BackupError::Invalid {
                message: "Invalid checksum length".to_string(),
            })?
        {
            return Err(BackupError::Invalid {
                message: "Page backup checksum mismatch".to_string(),
            });
        }

        Self::decode_page_backup(data)
    }

    /// Decodes page backup binary data (without the trailing checksum).
    fn decode_page_backup(data: &[u8]) -> Result<PageBackupData> {
        let mut pos = 0;

        // Magic
        if data.len() < 4 || &data[0..4] != PAGE_BACKUP_MAGIC {
            return Err(BackupError::Invalid { message: "Invalid page backup magic".to_string() });
        }
        pos += 4;

        // Version
        let version = u32::from_le_bytes(
            data[pos..pos + 4]
                .try_into()
                .map_err(|_| BackupError::Invalid { message: "Truncated version".to_string() })?,
        );
        if version != PAGE_BACKUP_VERSION {
            return Err(BackupError::Invalid {
                message: format!("Unsupported page backup version: {version}"),
            });
        }
        pos += 4;

        // Region height
        let region_height = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
            BackupError::Invalid { message: "Truncated region_height".to_string() }
        })?);
        pos += 8;

        // Page size
        let page_size =
            u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| {
                BackupError::Invalid { message: "Truncated page_size".to_string() }
            })?);
        pos += 4;

        // Page count
        let page_count =
            u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
                BackupError::Invalid { message: "Truncated page_count".to_string() }
            })?);
        pos += 8;

        // Table roots
        let mut table_roots = [0u64; TableId::COUNT];
        for root in &mut table_roots {
            *root = u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
                BackupError::Invalid { message: "Truncated table_roots".to_string() }
            })?);
            pos += 8;
        }

        // Base backup ID
        let has_base = data[pos];
        pos += 1;

        let base_backup_id = if has_base == 1 {
            let id_len = u32::from_le_bytes(data[pos..pos + 4].try_into().map_err(|_| {
                BackupError::Invalid { message: "Truncated base_backup_id length".to_string() }
            })?) as usize;
            pos += 4;

            let id = std::str::from_utf8(&data[pos..pos + id_len])
                .map_err(|_| BackupError::Invalid {
                    message: "Invalid base_backup_id UTF-8".to_string(),
                })?
                .to_string();
            pos += id_len;
            Some(id)
        } else {
            None
        };

        // Page entries
        let mut pages = Vec::with_capacity(page_count as usize);
        for _ in 0..page_count {
            let page_id =
                u64::from_le_bytes(data[pos..pos + 8].try_into().map_err(|_| {
                    BackupError::Invalid { message: "Truncated page_id".to_string() }
                })?);
            pos += 8;

            let page_data = data[pos..pos + page_size as usize].to_vec();
            pos += page_size as usize;
            pages.push((page_id, page_data));
        }

        Ok(PageBackupData { region_height, page_size, table_roots, base_backup_id, pages })
    }

    /// Resolves the full backup chain for a given backup ID.
    ///
    /// For a full backup, returns just that ID. For an incremental backup,
    /// walks the `base_backup_id` chain back to the full backup and returns
    /// the chain in application order (full first, then incrementals).
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::NotFound`] if any backup in the chain is missing,
    /// or [`BackupError::Invalid`] if an incremental backup lacks a `base_backup_id`.
    pub fn resolve_backup_chain(&self, backup_id: &str) -> Result<Vec<String>> {
        let mut chain = vec![backup_id.to_string()];
        let mut current_id = backup_id.to_string();

        loop {
            let meta = self.get_metadata(&current_id)?;
            match meta.backup_type {
                BackupType::Full => break,
                BackupType::Incremental => {
                    let base_id = meta.base_backup_id.ok_or_else(|| BackupError::Invalid {
                        message: format!("Incremental backup {current_id} has no base_backup_id"),
                    })?;
                    chain.push(base_id.clone());
                    current_id = base_id;
                },
            }
        }

        // Reverse so full backup is first
        chain.reverse();
        Ok(chain)
    }

    /// Restores from a page-level backup chain.
    ///
    /// Applies the full (base) backup first, then each incremental in order.
    /// The full backup is restored via Raft snapshot, then incrementals are
    /// applied as page-level patches.
    ///
    /// Returns the region height from the last backup in the chain.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::Invalid`] if the chain is empty or the first
    /// entry is not a full backup, [`BackupError::NotFound`] if any backup
    /// in the chain is missing, or [`BackupError::Storage`] if page import fails.
    pub fn restore_page_chain<B: StorageBackend>(
        &self,
        chain: &[String],
        db: &Database<B>,
    ) -> Result<u64> {
        if chain.is_empty() {
            return Err(BackupError::Invalid { message: "Empty backup chain".to_string() });
        }

        let mut last_height = 0;

        for (i, backup_id) in chain.iter().enumerate() {
            let meta = self.get_metadata(backup_id)?;

            if i == 0 && meta.backup_type != BackupType::Full {
                return Err(BackupError::Invalid {
                    message: format!(
                        "First backup in chain must be full, got {:?} for {backup_id}",
                        meta.backup_type
                    ),
                });
            }

            if meta.backup_type == BackupType::Full {
                // Full backup: load as snapshot and apply all pages
                let backup_data = self.load_page_backup(backup_id)?;
                let page_refs: Vec<(u64, &[u8])> =
                    backup_data.pages.iter().map(|(id, data)| (*id, data.as_slice())).collect();
                db.import_pages(&page_refs).context(StorageSnafu)?;
                last_height = backup_data.region_height;

                info!(
                    backup_id = %backup_id,
                    pages = backup_data.pages.len(),
                    region_height = last_height,
                    "Applied full page backup"
                );
            } else {
                // Incremental: apply page patches
                let backup_data = self.load_page_backup(backup_id)?;
                let page_refs: Vec<(u64, &[u8])> =
                    backup_data.pages.iter().map(|(id, data)| (*id, data.as_slice())).collect();
                db.import_pages(&page_refs).context(StorageSnafu)?;
                last_height = backup_data.region_height;

                info!(
                    backup_id = %backup_id,
                    pages = backup_data.pages.len(),
                    region_height = last_height,
                    "Applied incremental page backup"
                );
            }
        }

        Ok(last_height)
    }

    /// Creates a full page-level backup of the entire database.
    ///
    /// Exports all live (non-free, non-zero) pages into the page backup format.
    /// After a successful backup, the caller should clear the database's dirty
    /// bitmap via `Database::clear_dirty_bitmap()`.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::Io`] if the backup file cannot be created or
    /// written, or [`BackupError::Serialization`] if metadata serialization fails.
    pub async fn create_full_page_backup<B: StorageBackend + 'static>(
        &self,
        db: &Arc<Database<B>>,
        region: Region,
        region_height: u64,
        chain_commitment_hash: Hash,
        tag: &str,
    ) -> Result<BackupMetadata> {
        // Force `sync_state` before reading the backend.
        // A full page backup exports every live page; under lazy commits
        // some of those pages may be dirty in the in-process cache and
        // therefore not yet written via `flush_pages`. Syncing first
        // guarantees the backup captures durable bytes.
        Arc::clone(db).sync_state().await.context(StorageSnafu)?;

        let total_pages = db.total_page_count();
        let all_page_ids: Vec<u64> = (0..total_pages).collect();
        let pages = db.export_pages(&all_page_ids);
        let page_size = db.page_size();
        let table_roots = db.table_root_pages();

        let now = Utc::now();
        let backup_id = format!("{}-{region_height:09}", now.format("%Y%m%dT%H%M%SZ"));
        let backup_path = self.backup_dir.join(format!("{backup_id}{PAGE_BACKUP_EXT}"));

        let data =
            Self::encode_page_backup(region_height, page_size as u32, &table_roots, &pages, None);

        let checksum = sha256(&data);
        let mut file = fs::File::create(&backup_path).context(IoSnafu)?;
        file.write_all(&data).context(IoSnafu)?;
        file.write_all(&checksum).context(IoSnafu)?;
        file.sync_all().context(IoSnafu)?;

        let size_bytes = (data.len() + 32) as u64;

        let metadata = BackupMetadata {
            backup_id: backup_id.clone(),
            region,
            region_height,
            backup_path: backup_path.display().to_string(),
            size_bytes,
            created_at: now,
            checksum,
            chain_commitment_hash,
            schema_version: PAGE_BACKUP_VERSION,
            tag: tag.to_string(),
            backup_type: BackupType::Full,
            base_backup_id: None,
            page_count: Some(pages.len() as u64),
        };

        // Write metadata sidecar
        let meta_path = self.backup_dir.join(format!("{backup_id}{META_EXT}"));
        let meta_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| BackupError::Serialization { message: e.to_string() })?;
        fs::write(&meta_path, meta_json).context(IoSnafu)?;

        // Prune old backups
        self.prune()?;

        info!(
            backup_id = %metadata.backup_id,
            region_height = metadata.region_height,
            pages = pages.len(),
            size_bytes = metadata.size_bytes,
            "Full page backup created"
        );

        Ok(metadata)
    }
}

/// Decoded page-level backup data.
#[derive(Debug)]
pub struct PageBackupData {
    /// Region height at backup time.
    pub region_height: u64,
    /// Page size in bytes.
    pub page_size: u32,
    /// Table root page IDs.
    pub table_roots: [u64; TableId::COUNT],
    /// Base backup ID (for incremental backups).
    pub base_backup_id: Option<String>,
    /// Page entries: (page_id, page_data).
    pub pages: Vec<(u64, Vec<u8>)>,
}

/// Automated backup background job.
///
/// Periodically creates backups when this node is the leader. Follows the
/// same pattern as `BTreeCompactor` and `ResourceMetricsCollector`.
///
/// The backup flow:
/// 1. Trigger a Raft snapshot (ensures consistent state on disk)
/// 2. Load the latest snapshot from the snapshot directory
/// 3. Copy it to the backup destination with metadata
#[derive(bon::Builder)]
#[builder(on(_, required))]
pub struct BackupJob {
    /// Consensus handle for verifying leadership.
    handle: Arc<ConsensusHandle>,
    /// Snapshot manager for reading snapshots.
    snapshot_manager: Arc<SnapshotManager>,
    /// Backup manager for file operations.
    backup_manager: Arc<BackupManager>,
    /// Interval between backup cycles.
    interval: Duration,
    /// Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
}

impl BackupJob {
    /// Starts the backup job as a background task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {}
                    _ = self.cancellation_token.cancelled() => {
                        info!("BackupJob shutting down");
                        break;
                    }
                }

                // Only the leader creates backups
                if !self.handle.is_leader() {
                    debug!("Not leader, skipping backup cycle");
                    continue;
                }

                let cycle_start = Instant::now();
                let mut job = crate::logging::JobContext::new("backup", None);
                info!("Starting automated backup");

                // Trigger a snapshot to ensure the latest committed state
                // is persisted to disk before creating the backup.
                match self.handle.trigger_snapshot().await {
                    Ok((idx, _term)) if idx > 0 => {
                        info!(last_included_index = idx, "Snapshot triggered for backup");
                    },
                    Ok(_) => {
                        info!("No new snapshot needed (commit index unchanged)");
                    },
                    Err(e) => {
                        warn!(error = %e, "Snapshot trigger failed; using latest available snapshot");
                    },
                }

                // Load the latest snapshot from disk
                match self.snapshot_manager.load_latest() {
                    Ok(Some(snapshot)) => {
                        match self.backup_manager.create_backup(&snapshot, "auto") {
                            Ok(meta) => {
                                let duration_secs = cycle_start.elapsed().as_secs_f64();
                                crate::metrics::record_backup_created(
                                    meta.region_height,
                                    meta.size_bytes,
                                );
                                job.record_items(1);
                                info!(
                                    backup_id = %meta.backup_id,
                                    region_height = meta.region_height,
                                    size_bytes = meta.size_bytes,
                                    duration_secs,
                                    "Automated backup completed"
                                );
                            },
                            Err(e) => {
                                let duration_secs = cycle_start.elapsed().as_secs_f64();
                                error!(error = %e, duration_secs, "Failed to write backup");
                                crate::metrics::record_backup_failed();
                                job.set_failure();
                            },
                        }
                    },
                    Ok(None) => {
                        let duration_secs = cycle_start.elapsed().as_secs_f64();
                        warn!(duration_secs, "No snapshot available for backup");
                        crate::metrics::record_backup_failed();
                        job.set_failure();
                    },
                    Err(e) => {
                        let duration_secs = cycle_start.elapsed().as_secs_f64();
                        error!(error = %e, duration_secs, "Failed to load snapshot for backup");
                        crate::metrics::record_backup_failed();
                        job.set_failure();
                    },
                }
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::collections::HashMap;

    use inferadb_ledger_state::{SnapshotChainParams, SnapshotStateData, VaultSnapshotMeta};
    use inferadb_ledger_types::{ChainCommitment, EMPTY_HASH, Entity, VaultId};
    use tempfile::TempDir;

    use super::*;

    fn create_test_backup_config(dir: &Path) -> BackupConfig {
        BackupConfig::builder()
            .destination(dir.display().to_string())
            .retention_count(3_usize)
            .build()
            .expect("valid config")
    }

    fn create_test_snapshot(region_height: u64) -> Snapshot {
        let vault_states = vec![VaultSnapshotMeta::new(
            VaultId::new(1),
            region_height / 2,
            [42u8; 32],
            [EMPTY_HASH; 256],
            5,
        )];

        let mut vault_entities = HashMap::new();
        vault_entities.insert(
            VaultId::new(1),
            vec![Entity {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                expires_at: 0,
                version: 1,
            }],
        );

        let state = SnapshotStateData { vault_entities };
        let chain_params = SnapshotChainParams {
            genesis_hash: [1u8; 32],
            previous_snapshot_height: None,
            previous_snapshot_hash: None,
            chain_commitment: ChainCommitment {
                accumulated_header_hash: [3u8; 32],
                state_root_accumulator: [4u8; 32],
                from_height: 0,
                to_height: region_height,
            },
            erased_users: Default::default(),
        };

        Snapshot::new(Region::GLOBAL, region_height, vault_states, state, chain_params)
            .expect("create snapshot")
    }

    #[test]
    fn test_backup_manager_create_and_list() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");

        let snapshot = create_test_snapshot(1000);
        let meta = manager.create_backup(&snapshot, "test").expect("create backup");

        assert_eq!(meta.region_height, 1000);
        assert_eq!(meta.tag, "test");
        assert!(meta.size_bytes > 0);
        assert!(meta.backup_id.contains("000001000"));

        let backups = manager.list_backups(0).expect("list backups");
        assert_eq!(backups.len(), 1);
        assert_eq!(backups[0].backup_id, meta.backup_id);
    }

    #[test]
    fn test_backup_manager_load_backup() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");

        let snapshot = create_test_snapshot(2000);
        let meta = manager.create_backup(&snapshot, "").expect("create backup");

        let loaded = manager.load_backup(&meta.backup_id).expect("load backup");
        assert_eq!(loaded.header.region_height, 2000);
        assert_eq!(loaded.header.region, Region::GLOBAL);
    }

    #[test]
    fn test_backup_manager_not_found() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");

        let result = manager.load_backup("nonexistent");
        assert!(matches!(result, Err(BackupError::NotFound { .. })));
    }

    #[test]
    fn test_backup_manager_get_metadata() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");

        let snapshot = create_test_snapshot(3000);
        let meta = manager.create_backup(&snapshot, "tagged").expect("create backup");

        let loaded_meta = manager.get_metadata(&meta.backup_id).expect("get metadata");
        assert_eq!(loaded_meta.region_height, 3000);
        assert_eq!(loaded_meta.tag, "tagged");
        assert_eq!(loaded_meta.checksum, meta.checksum);
    }

    #[test]
    fn test_backup_manager_metadata_not_found() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");

        let result = manager.get_metadata("nonexistent");
        assert!(matches!(result, Err(BackupError::NotFound { .. })));
    }

    #[test]
    fn test_backup_retention_pruning() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path()); // retention_count = 3
        let manager = BackupManager::new(&config).expect("create manager");

        // Create 5 backups
        for height in [100, 200, 300, 400, 500] {
            let snapshot = create_test_snapshot(height);
            manager.create_backup(&snapshot, "").expect("create backup");
            // Small sleep to ensure different timestamps
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let backups = manager.list_backups(0).expect("list backups");
        assert_eq!(backups.len(), 3, "Should retain only 3 backups");

        // Newest should be first (height 500, 400, 300)
        assert_eq!(backups[0].region_height, 500);
        assert_eq!(backups[1].region_height, 400);
        assert_eq!(backups[2].region_height, 300);
    }

    #[test]
    fn test_backup_list_with_limit() {
        let temp = TempDir::new().expect("create temp dir");
        let config = BackupConfig::builder()
            .destination(temp.path().display().to_string())
            .retention_count(10_usize)
            .build()
            .expect("valid config");
        let manager = BackupManager::new(&config).expect("create manager");

        for height in [100, 200, 300] {
            let snapshot = create_test_snapshot(height);
            manager.create_backup(&snapshot, "").expect("create backup");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let backups = manager.list_backups(2).expect("list with limit");
        assert_eq!(backups.len(), 2);
    }

    #[test]
    fn test_backup_corrupted_file_detected() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");

        let snapshot = create_test_snapshot(1000);
        let meta = manager.create_backup(&snapshot, "").expect("create backup");

        // Corrupt the backup file
        let backup_path = temp.path().join(format!("{}{BACKUP_EXT}", meta.backup_id));
        let mut data = fs::read(&backup_path).expect("read file");
        if let Some(last) = data.last_mut() {
            *last ^= 0xFF;
        }
        fs::write(&backup_path, data).expect("write corrupted");

        // Loading should fail with checksum mismatch
        let result = manager.load_backup(&meta.backup_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_backup_includes_chain_commitment() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");

        let snapshot = create_test_snapshot(5000);
        let meta = manager.create_backup(&snapshot, "chain-test").expect("create backup");

        // Verify chain commitment is captured in metadata
        assert_eq!(meta.chain_commitment_hash, [3u8; 32]);
        assert_eq!(meta.schema_version, 2); // SNAPSHOT_VERSION
    }

    #[test]
    fn test_backup_empty_directory() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");

        let backups = manager.list_backups(0).expect("list empty");
        assert!(backups.is_empty());
    }

    #[test]
    fn test_backup_config_validation() {
        // Valid config
        let result = BackupConfig::builder().destination("/tmp/backups").build();
        assert!(result.is_ok());

        // Empty destination
        let result = BackupConfig::builder().destination("").build();
        assert!(result.is_err());

        // Zero retention
        let result =
            BackupConfig::builder().destination("/tmp/backups").retention_count(0_usize).build();
        assert!(result.is_err());

        // Enabled with too-short interval
        let result = BackupConfig::builder()
            .destination("/tmp/backups")
            .enabled(true)
            .interval_secs(30_u64)
            .build();
        assert!(result.is_err());

        // Disabled with short interval is OK (interval not used)
        let result =
            BackupConfig::builder().destination("/tmp/backups").interval_secs(30_u64).build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_backup_config_defaults() {
        let config =
            BackupConfig::builder().destination("/tmp/backups").build().expect("valid config");

        assert_eq!(config.retention_count, 7);
        assert!(!config.enabled);
        assert_eq!(config.interval_secs, 86400);
    }

    // --- Incremental backup tests ---

    fn create_test_db_with_data()
    -> Arc<inferadb_ledger_store::Database<inferadb_ledger_store::InMemoryBackend>> {
        let db = Arc::new(
            inferadb_ledger_store::Database::open_in_memory().expect("create in-memory database"),
        );
        {
            let mut txn = db.write().expect("begin write txn");
            txn.insert::<inferadb_ledger_store::tables::Entities>(
                &b"key1".to_vec(),
                &b"value1".to_vec(),
            )
            .expect("insert");
            txn.insert::<inferadb_ledger_store::tables::Entities>(
                &b"key2".to_vec(),
                &b"value2".to_vec(),
            )
            .expect("insert");
            txn.commit().expect("commit");
        }
        db
    }

    #[tokio::test]
    async fn test_full_page_backup_create_and_load() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        let meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 100, [5u8; 32], "full-test")
            .await
            .expect("create full page backup");

        assert_eq!(meta.backup_type, BackupType::Full);
        assert_eq!(meta.region_height, 100);
        assert_eq!(meta.tag, "full-test");
        assert!(meta.page_count.is_some());
        assert!(meta.page_count.expect("page_count") > 0);
        assert!(meta.base_backup_id.is_none());
        assert!(meta.size_bytes > 0);

        // Load and verify
        let loaded = manager.load_page_backup(&meta.backup_id).expect("load page backup");
        assert_eq!(loaded.region_height, 100);
        assert!(loaded.base_backup_id.is_none());
        assert!(!loaded.pages.is_empty());
    }

    #[tokio::test]
    async fn test_incremental_backup_captures_dirty_pages() {
        let temp = TempDir::new().expect("create temp dir");
        let config = BackupConfig::builder()
            .destination(temp.path().display().to_string())
            .retention_count(10_usize)
            .build()
            .expect("valid config");
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        // Create full page backup and clear dirty bitmap
        let full_meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 100, [5u8; 32], "base")
            .await
            .expect("create full page backup");
        db.clear_dirty_bitmap();

        // Write more data to create dirty pages
        {
            let mut txn = db.write().expect("begin write txn");
            txn.insert::<inferadb_ledger_store::tables::Entities>(
                &b"key3".to_vec(),
                &b"value3".to_vec(),
            )
            .expect("insert");
            txn.commit().expect("commit");
        }

        assert!(db.dirty_page_count() > 0, "Should have dirty pages after write");

        let inc_meta = manager
            .create_incremental_backup(&db, &full_meta.backup_id, Region::GLOBAL, 200, "incr")
            .await
            .expect("create incremental backup");

        assert_eq!(inc_meta.backup_type, BackupType::Incremental);
        assert_eq!(inc_meta.region_height, 200);
        assert_eq!(inc_meta.base_backup_id.as_deref(), Some(full_meta.backup_id.as_str()));
        assert!(inc_meta.page_count.is_some());

        // Incremental should be smaller than full
        assert!(
            inc_meta.size_bytes <= full_meta.size_bytes,
            "Incremental ({}) should be <= full ({})",
            inc_meta.size_bytes,
            full_meta.size_bytes
        );
    }

    #[tokio::test]
    async fn test_incremental_backup_rejects_incremental_base() {
        let temp = TempDir::new().expect("create temp dir");
        let config = BackupConfig::builder()
            .destination(temp.path().display().to_string())
            .retention_count(10_usize)
            .build()
            .expect("valid config");
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        // Create full → incremental chain
        let full_meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 100, [5u8; 32], "base")
            .await
            .expect("create full");
        db.clear_dirty_bitmap();

        {
            let mut txn = db.write().expect("write txn");
            txn.insert::<inferadb_ledger_store::tables::Entities>(
                &b"key3".to_vec(),
                &b"value3".to_vec(),
            )
            .expect("insert");
            txn.commit().expect("commit");
        }

        let inc_meta = manager
            .create_incremental_backup(&db, &full_meta.backup_id, Region::GLOBAL, 200, "incr1")
            .await
            .expect("create incremental");
        db.clear_dirty_bitmap();

        // Write more and try to chain off the incremental — should fail
        {
            let mut txn = db.write().expect("write txn");
            txn.insert::<inferadb_ledger_store::tables::Entities>(
                &b"key4".to_vec(),
                &b"value4".to_vec(),
            )
            .expect("insert");
            txn.commit().expect("commit");
        }

        let result = manager
            .create_incremental_backup(&db, &inc_meta.backup_id, Region::GLOBAL, 300, "incr2")
            .await;
        assert!(
            matches!(result, Err(BackupError::Invalid { .. })),
            "Should reject incremental base"
        );
    }

    #[tokio::test]
    async fn test_incremental_backup_no_dirty_pages() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        let full_meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 100, [5u8; 32], "")
            .await
            .expect("create full");
        db.clear_dirty_bitmap();

        // No writes → no dirty pages → should error
        let result = manager
            .create_incremental_backup(&db, &full_meta.backup_id, Region::GLOBAL, 200, "")
            .await;
        assert!(
            matches!(result, Err(BackupError::Invalid { .. })),
            "Should reject when no dirty pages"
        );
    }

    #[tokio::test]
    async fn test_resolve_backup_chain() {
        let temp = TempDir::new().expect("create temp dir");
        let config = BackupConfig::builder()
            .destination(temp.path().display().to_string())
            .retention_count(10_usize)
            .build()
            .expect("valid config");
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        let full_meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 100, [5u8; 32], "")
            .await
            .expect("create full");
        db.clear_dirty_bitmap();

        // Create incremental
        {
            let mut txn = db.write().expect("write txn");
            txn.insert::<inferadb_ledger_store::tables::Entities>(
                &b"key3".to_vec(),
                &b"value3".to_vec(),
            )
            .expect("insert");
            txn.commit().expect("commit");
        }

        let inc_meta = manager
            .create_incremental_backup(&db, &full_meta.backup_id, Region::GLOBAL, 200, "")
            .await
            .expect("create incremental");

        // Resolve chain from incremental → should be [full, incremental]
        let chain = manager.resolve_backup_chain(&inc_meta.backup_id).expect("resolve chain");
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0], full_meta.backup_id);
        assert_eq!(chain[1], inc_meta.backup_id);

        // Resolve chain from full → should be just [full]
        let chain =
            manager.resolve_backup_chain(&full_meta.backup_id).expect("resolve chain from full");
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0], full_meta.backup_id);
    }

    #[tokio::test]
    async fn test_restore_page_chain_full_only() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        let full_meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 100, [5u8; 32], "")
            .await
            .expect("create full");

        // Create a fresh database and restore into it
        let target_db =
            inferadb_ledger_store::Database::open_in_memory().expect("create target db");

        let chain = manager.resolve_backup_chain(&full_meta.backup_id).expect("resolve chain");
        let restored_height =
            manager.restore_page_chain(&chain, &target_db).expect("restore chain");

        assert_eq!(restored_height, 100);
    }

    #[tokio::test]
    async fn test_restore_page_chain_with_incremental() {
        let temp = TempDir::new().expect("create temp dir");
        let config = BackupConfig::builder()
            .destination(temp.path().display().to_string())
            .retention_count(10_usize)
            .build()
            .expect("valid config");
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        let full_meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 100, [5u8; 32], "")
            .await
            .expect("create full");
        db.clear_dirty_bitmap();

        // Write more data and create incremental
        {
            let mut txn = db.write().expect("write txn");
            txn.insert::<inferadb_ledger_store::tables::Entities>(
                &b"key3".to_vec(),
                &b"value3".to_vec(),
            )
            .expect("insert");
            txn.commit().expect("commit");
        }

        let inc_meta = manager
            .create_incremental_backup(&db, &full_meta.backup_id, Region::GLOBAL, 200, "")
            .await
            .expect("create incremental");

        // Restore the full chain into a fresh database
        let target_db =
            inferadb_ledger_store::Database::open_in_memory().expect("create target db");
        let chain = manager.resolve_backup_chain(&inc_meta.backup_id).expect("resolve chain");
        let restored_height =
            manager.restore_page_chain(&chain, &target_db).expect("restore chain");

        assert_eq!(restored_height, 200);
    }

    #[tokio::test]
    async fn test_page_backup_checksum_verification() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        let meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 100, [5u8; 32], "")
            .await
            .expect("create full page backup");

        // Corrupt the page backup file
        let backup_path = temp.path().join(format!("{}{PAGE_BACKUP_EXT}", meta.backup_id));
        let mut data = fs::read(&backup_path).expect("read file");
        if let Some(byte) = data.get_mut(20) {
            *byte ^= 0xFF;
        }
        fs::write(&backup_path, data).expect("write corrupted");

        let result = manager.load_page_backup(&meta.backup_id);
        assert!(
            matches!(result, Err(BackupError::Invalid { .. })),
            "Should detect corruption via checksum mismatch"
        );
    }

    #[test]
    fn test_page_backup_encode_decode_roundtrip() {
        let pages = vec![(0_u64, vec![1u8; 128]), (5, vec![2u8; 128]), (10, vec![3u8; 128])];
        let table_roots = [0_u64; TableId::COUNT];

        let encoded =
            BackupManager::encode_page_backup(1000, 128, &table_roots, &pages, Some("base-123"));
        let decoded = BackupManager::decode_page_backup(&encoded).expect("decode");

        assert_eq!(decoded.region_height, 1000);
        assert_eq!(decoded.page_size, 128);
        assert_eq!(decoded.table_roots, table_roots);
        assert_eq!(decoded.base_backup_id.as_deref(), Some("base-123"));
        assert_eq!(decoded.pages.len(), 3);
        assert_eq!(decoded.pages[0].0, 0);
        assert_eq!(decoded.pages[1].0, 5);
        assert_eq!(decoded.pages[2].0, 10);
        assert_eq!(decoded.pages[0].1, vec![1u8; 128]);
    }

    #[test]
    fn test_page_backup_encode_decode_no_base() {
        let pages = vec![(1_u64, vec![0xAB; 64])];
        let table_roots = [42_u64; TableId::COUNT];

        let encoded = BackupManager::encode_page_backup(500, 64, &table_roots, &pages, None);
        let decoded = BackupManager::decode_page_backup(&encoded).expect("decode");

        assert_eq!(decoded.region_height, 500);
        assert_eq!(decoded.page_size, 64);
        assert!(decoded.base_backup_id.is_none());
        assert_eq!(decoded.pages.len(), 1);
    }

    #[tokio::test]
    async fn test_incremental_backup_listed_with_type() {
        let temp = TempDir::new().expect("create temp dir");
        let config = BackupConfig::builder()
            .destination(temp.path().display().to_string())
            .retention_count(10_usize)
            .build()
            .expect("valid config");
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        // Create full snapshot backup
        let snapshot = create_test_snapshot(100);
        let snap_meta = manager.create_backup(&snapshot, "snapshot").expect("snapshot backup");

        // Create full page backup
        let page_meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 200, [5u8; 32], "page-full")
            .await
            .expect("page backup");
        db.clear_dirty_bitmap();

        // Create incremental
        {
            let mut txn = db.write().expect("write txn");
            txn.insert::<inferadb_ledger_store::tables::Entities>(
                &b"key3".to_vec(),
                &b"value3".to_vec(),
            )
            .expect("insert");
            txn.commit().expect("commit");
        }

        let _inc_meta = manager
            .create_incremental_backup(&db, &page_meta.backup_id, Region::GLOBAL, 300, "incr")
            .await
            .expect("incremental backup");

        let backups = manager.list_backups(0).expect("list all");
        assert_eq!(backups.len(), 3);

        // Check that types are preserved
        let full_snapshot = backups.iter().find(|b| b.backup_id == snap_meta.backup_id);
        assert!(full_snapshot.is_some());
        assert_eq!(full_snapshot.expect("found").backup_type, BackupType::Full);

        let full_page = backups.iter().find(|b| b.backup_id == page_meta.backup_id);
        assert!(full_page.is_some());
        assert_eq!(full_page.expect("found").backup_type, BackupType::Full);

        let incremental = backups.iter().find(|b| b.backup_type == BackupType::Incremental);
        assert!(incremental.is_some());
        assert!(incremental.expect("found").base_backup_id.is_some());
    }

    fn test_backup_metadata_for_region(region: Region) -> BackupMetadata {
        BackupMetadata {
            backup_id: format!("test-{}", region.as_str()),
            region,
            region_height: 100,
            backup_path: format!("/backups/test-{}.backup", region.as_str()),
            size_bytes: 1024,
            created_at: Utc::now(),
            checksum: [0u8; 32],
            chain_commitment_hash: EMPTY_HASH,
            schema_version: 2,
            tag: String::new(),
            backup_type: BackupType::Full,
            base_backup_id: None,
            page_count: None,
        }
    }

    #[test]
    fn test_backup_validate_restore_region_protected_same_ok() {
        let meta = test_backup_metadata_for_region(Region::CA_CENTRAL_QC);
        assert!(meta.validate_restore_region(Region::CA_CENTRAL_QC).is_ok());
    }

    #[test]
    fn test_backup_validate_restore_region_protected_different_rejected() {
        let meta = test_backup_metadata_for_region(Region::IE_EAST_DUBLIN);
        let err = meta.validate_restore_region(Region::US_EAST_VA).unwrap_err();
        assert!(matches!(err, BackupError::CrossRegionRestore { .. }));
    }

    #[test]
    fn test_backup_validate_restore_region_non_protected_ok() {
        let meta = test_backup_metadata_for_region(Region::US_EAST_VA);
        assert!(meta.validate_restore_region(Region::CA_CENTRAL_QC).is_ok());
    }

    #[test]
    fn test_backup_validate_restore_region_global_ok() {
        let meta = test_backup_metadata_for_region(Region::GLOBAL);
        assert!(meta.validate_restore_region(Region::IE_EAST_DUBLIN).is_ok());
    }

    #[test]
    fn test_validate_not_keys_directory_ok() {
        let keys_dir = Path::new("/data/keys");
        let backup_path = Path::new("/data/backups/snapshot-001.backup");
        assert!(validate_not_keys_directory(backup_path, keys_dir).is_ok());
    }

    #[test]
    fn test_validate_not_keys_directory_rejected() {
        let keys_dir = Path::new("/data/keys");
        let backup_path = Path::new("/data/keys/v1.key");
        let err = validate_not_keys_directory(backup_path, keys_dir).unwrap_err();
        assert!(matches!(err, BackupError::KeyDirectoryExcluded { .. }));
    }

    #[test]
    fn test_validate_not_keys_directory_subdirectory_rejected() {
        let keys_dir = Path::new("/data/keys");
        let backup_path = Path::new("/data/keys/region/ca-central/v2.key");
        let err = validate_not_keys_directory(backup_path, keys_dir).unwrap_err();
        assert!(matches!(err, BackupError::KeyDirectoryExcluded { .. }));
    }

    // ── validate_backup_id tests ──

    #[test]
    fn test_validate_backup_id_valid() {
        assert!(validate_backup_id("20260101T000000Z-000001000").is_ok());
        assert!(validate_backup_id("simple-id").is_ok());
    }

    #[test]
    fn test_validate_backup_id_empty() {
        let err = validate_backup_id("").unwrap_err();
        assert!(matches!(err, BackupError::InvalidBackupId { .. }));
    }

    #[test]
    fn test_validate_backup_id_slash() {
        let err = validate_backup_id("foo/bar").unwrap_err();
        assert!(matches!(err, BackupError::InvalidBackupId { .. }));
    }

    #[test]
    fn test_validate_backup_id_backslash() {
        let err = validate_backup_id("foo\\bar").unwrap_err();
        assert!(matches!(err, BackupError::InvalidBackupId { .. }));
    }

    #[test]
    fn test_validate_backup_id_dotdot() {
        let err = validate_backup_id("..sneaky").unwrap_err();
        assert!(matches!(err, BackupError::InvalidBackupId { .. }));
    }

    // ── BackupError Display tests ──

    #[test]
    fn test_backup_error_display_io() {
        let e =
            BackupError::Io { source: std::io::Error::new(std::io::ErrorKind::NotFound, "gone") };
        assert!(format!("{e}").contains("Backup IO error"));
    }

    #[test]
    fn test_backup_error_display_not_found() {
        let e = BackupError::NotFound { backup_id: "backup-123".to_string() };
        assert!(format!("{e}").contains("backup-123"));
    }

    #[test]
    fn test_backup_error_display_not_confirmed() {
        let e = BackupError::NotConfirmed;
        assert!(format!("{e}").contains("confirm=true"));
    }

    #[test]
    fn test_backup_error_display_invalid() {
        let e = BackupError::Invalid { message: "corrupt header".to_string() };
        assert!(format!("{e}").contains("corrupt header"));
    }

    #[test]
    fn test_backup_error_display_schema_version_mismatch() {
        let e = BackupError::SchemaVersionMismatch { backup_version: 1, server_version: 2 };
        assert!(format!("{e}").contains("1"));
        assert!(format!("{e}").contains("2"));
    }

    #[test]
    fn test_backup_error_display_serialization() {
        let e = BackupError::Serialization { message: "bad json".to_string() };
        assert!(format!("{e}").contains("bad json"));
    }

    #[test]
    fn test_backup_error_display_cross_region_restore() {
        let e = BackupError::CrossRegionRestore {
            backup_region: Region::DE_CENTRAL_FRANKFURT,
            node_region: Region::US_EAST_VA,
        };
        let msg = format!("{e}");
        assert!(msg.contains("Cross-region restore rejected"));
    }

    #[test]
    fn test_backup_error_display_key_directory_excluded() {
        let e = BackupError::KeyDirectoryExcluded { path: "/data/keys/v1.key".to_string() };
        assert!(format!("{e}").contains("keys directory"));
    }

    #[test]
    fn test_backup_error_display_invalid_backup_id() {
        let e = BackupError::InvalidBackupId { backup_id: "../foo".to_string() };
        assert!(format!("{e}").contains("path separators"));
    }

    // ── BackupType serde ──

    #[test]
    fn test_backup_type_serde_roundtrip() {
        let full = serde_json::to_string(&BackupType::Full).expect("serialize");
        let deserialized: BackupType = serde_json::from_str(&full).expect("deserialize");
        assert_eq!(deserialized, BackupType::Full);

        let inc = serde_json::to_string(&BackupType::Incremental).expect("serialize");
        let deserialized: BackupType = serde_json::from_str(&inc).expect("deserialize");
        assert_eq!(deserialized, BackupType::Incremental);
    }

    #[test]
    fn test_default_backup_type_is_full() {
        assert_eq!(default_backup_type(), BackupType::Full);
    }

    #[test]
    fn test_backup_manager_backup_dir() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");
        assert_eq!(manager.backup_dir(), temp.path());
    }

    // ── decode_page_backup error cases ──

    #[test]
    fn test_decode_page_backup_empty_data() {
        let result = BackupManager::decode_page_backup(&[]);
        assert!(matches!(result, Err(BackupError::Invalid { .. })));
    }

    #[test]
    fn test_decode_page_backup_wrong_magic() {
        let data = b"XXXX";
        let result = BackupManager::decode_page_backup(data);
        assert!(matches!(result, Err(BackupError::Invalid { .. })));
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("magic"));
    }

    #[test]
    fn test_decode_page_backup_unsupported_version() {
        let mut data = Vec::new();
        data.extend_from_slice(PAGE_BACKUP_MAGIC);
        data.extend_from_slice(&99u32.to_le_bytes()); // unsupported version
        let result = BackupManager::decode_page_backup(&data);
        assert!(matches!(result, Err(BackupError::Invalid { .. })));
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("version"));
    }

    #[test]
    fn test_decode_page_backup_short_data_no_magic() {
        // Only 3 bytes — not enough for magic, should fail at magic check
        let result = BackupManager::decode_page_backup(&[0x4C, 0x50, 0x42]);
        assert!(matches!(result, Err(BackupError::Invalid { .. })));
    }

    // ── BackupMetadata serde roundtrip ──

    #[test]
    fn test_backup_metadata_serde_roundtrip() {
        let meta = BackupMetadata {
            backup_id: "test-001".to_string(),
            region: Region::US_EAST_VA,
            region_height: 42,
            backup_path: "/tmp/test.backup".to_string(),
            size_bytes: 1024,
            created_at: Utc::now(),
            checksum: [1u8; 32],
            chain_commitment_hash: [2u8; 32],
            schema_version: 2,
            tag: "nightly".to_string(),
            backup_type: BackupType::Full,
            base_backup_id: None,
            page_count: Some(10),
        };

        let json = serde_json::to_string(&meta).expect("serialize");
        let deserialized: BackupMetadata = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.backup_id, "test-001");
        assert_eq!(deserialized.region, Region::US_EAST_VA);
        assert_eq!(deserialized.region_height, 42);
        assert_eq!(deserialized.size_bytes, 1024);
        assert_eq!(deserialized.tag, "nightly");
        assert_eq!(deserialized.backup_type, BackupType::Full);
        assert!(deserialized.base_backup_id.is_none());
        assert_eq!(deserialized.page_count, Some(10));
    }

    #[test]
    fn test_backup_metadata_serde_incremental() {
        let meta = BackupMetadata {
            backup_id: "inc-001".to_string(),
            region: Region::GLOBAL,
            region_height: 100,
            backup_path: "/tmp/inc.pagebackup".to_string(),
            size_bytes: 512,
            created_at: Utc::now(),
            checksum: [0u8; 32],
            chain_commitment_hash: [0u8; 32],
            schema_version: 1,
            tag: String::new(),
            backup_type: BackupType::Incremental,
            base_backup_id: Some("full-001".to_string()),
            page_count: Some(3),
        };

        let json = serde_json::to_string(&meta).expect("serialize");
        let deserialized: BackupMetadata = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.backup_type, BackupType::Incremental);
        assert_eq!(deserialized.base_backup_id.as_deref(), Some("full-001"));
    }

    #[test]
    fn test_backup_metadata_deserialize_missing_optional_fields() {
        // Simulates older metadata without backup_type/base_backup_id/page_count
        let json = r#"{
            "backup_id": "old-001",
            "region": "global",
            "region_height": 50,
            "backup_path": "/tmp/old.backup",
            "size_bytes": 256,
            "created_at": "2025-01-01T00:00:00Z",
            "checksum": [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            "chain_commitment_hash": [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            "schema_version": 2,
            "tag": ""
        }"#;

        let meta: BackupMetadata = serde_json::from_str(json).expect("deserialize old format");
        assert_eq!(meta.backup_type, BackupType::Full); // default
        assert!(meta.base_backup_id.is_none()); // default
        assert!(meta.page_count.is_none()); // default
    }

    // ── PageBackupData debug ──

    #[test]
    fn test_page_backup_data_debug() {
        let data = PageBackupData {
            region_height: 100,
            page_size: 4096,
            table_roots: [0; TableId::COUNT],
            base_backup_id: None,
            pages: vec![],
        };
        let debug = format!("{:?}", data);
        assert!(debug.contains("region_height: 100"));
        assert!(debug.contains("page_size: 4096"));
    }

    // ── Validate backup ID edge cases ──

    #[test]
    fn test_validate_backup_id_with_dots_but_not_dotdot() {
        // Single dots should be fine
        assert!(validate_backup_id("backup.v1").is_ok());
        assert!(validate_backup_id("2025.01.01-snapshot").is_ok());
    }

    #[test]
    fn test_validate_backup_id_dotdot_in_middle() {
        let err = validate_backup_id("backup..sneaky").unwrap_err();
        assert!(matches!(err, BackupError::InvalidBackupId { .. }));
    }

    // ── Constants ──

    #[test]
    fn test_backup_constants() {
        assert_eq!(BACKUP_EXT, ".backup");
        assert_eq!(META_EXT, ".meta.json");
        assert_eq!(PAGE_BACKUP_MAGIC, b"LPBK");
        assert_eq!(PAGE_BACKUP_VERSION, 1);
        assert_eq!(PAGE_BACKUP_EXT, ".pagebackup");
    }

    // ── encode_page_backup with empty pages ──

    #[test]
    fn test_encode_page_backup_empty_pages() {
        let table_roots = [0u64; TableId::COUNT];
        let encoded = BackupManager::encode_page_backup(0, 4096, &table_roots, &[], None);
        let decoded = BackupManager::decode_page_backup(&encoded).expect("decode empty");
        assert_eq!(decoded.region_height, 0);
        assert_eq!(decoded.page_size, 4096);
        assert!(decoded.pages.is_empty());
        assert!(decoded.base_backup_id.is_none());
    }

    // ========================================================================
    // Backup-path sync hooks fire before reading the backend
    // ========================================================================

    /// Helper: commit a write in-memory only (no dual-slot persist), simulating
    /// an apply-path commit.
    fn commit_in_memory_one<B: inferadb_ledger_store::StorageBackend>(
        db: &Arc<Database<B>>,
        key: &[u8],
        value: &[u8],
    ) {
        let mut txn = db.write().expect("begin write txn");
        txn.insert::<inferadb_ledger_store::tables::Entities>(&key.to_vec(), &value.to_vec())
            .expect("insert");
        txn.commit_in_memory().expect("commit_in_memory");
    }

    /// `create_full_page_backup` must force `sync_state` at the top so the
    /// backend's durable state matches the in-memory `committed_state` before
    /// any pages are exported. Proved by asserting
    /// `last_synced_snapshot_id` advances past where it was after the
    /// most recent in-memory commit.
    #[tokio::test]
    async fn create_full_page_backup_forces_sync_state() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        // Commit an in-memory-only write on top of the test-data commit.
        // After this, `committed_state` is ahead of the durable dual-slot.
        commit_in_memory_one(&db, b"sync-hook-probe", b"v1");
        let synced_before = db.last_synced_snapshot_id();

        let _meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 100, [5u8; 32], "sync-hook")
            .await
            .expect("full page backup");

        let synced_after = db.last_synced_snapshot_id();
        assert!(
            synced_after > synced_before,
            "create_full_page_backup must advance last_synced_snapshot_id \
             (sync hook): before={synced_before} after={synced_after}"
        );
    }

    /// Same coverage for `create_incremental_backup`.
    #[tokio::test]
    async fn create_incremental_backup_forces_sync_state() {
        let temp = TempDir::new().expect("create temp dir");
        let config = BackupConfig::builder()
            .destination(temp.path().display().to_string())
            .retention_count(10_usize)
            .build()
            .expect("valid config");
        let manager = BackupManager::new(&config).expect("create manager");
        let db = create_test_db_with_data();

        let full_meta = manager
            .create_full_page_backup(&db, Region::GLOBAL, 100, [5u8; 32], "base")
            .await
            .expect("full backup");
        db.clear_dirty_bitmap();

        // In-memory-only write: committed_state ahead of disk.
        commit_in_memory_one(&db, b"incremental-sync-probe", b"v1");
        let synced_before = db.last_synced_snapshot_id();

        let _inc_meta = manager
            .create_incremental_backup(&db, &full_meta.backup_id, Region::GLOBAL, 200, "incr")
            .await
            .expect("incremental backup");

        let synced_after = db.last_synced_snapshot_id();
        assert!(
            synced_after > synced_before,
            "create_incremental_backup must advance last_synced_snapshot_id \
             (sync hook): before={synced_before} after={synced_after}"
        );
    }
}
