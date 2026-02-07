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
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use inferadb_ledger_state::{Snapshot, SnapshotError, SnapshotManager};
use inferadb_ledger_types::{Hash, ShardId, config::BackupConfig};
use openraft::Raft;
use snafu::{ResultExt, Snafu};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::{LedgerNodeId, types::LedgerTypeConfig};

/// Backup file extension.
const BACKUP_EXT: &str = ".backup";

/// Metadata file extension (JSON sidecar).
const META_EXT: &str = ".meta.json";

/// Error types for backup operations.
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
}

/// Result type for backup operations.
pub type Result<T> = std::result::Result<T, BackupError>;

/// Metadata about an available backup (stored as JSON sidecar).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackupMetadata {
    /// Unique backup identifier (timestamp-based).
    pub backup_id: String,
    /// Shard ID.
    pub shard_id: ShardId,
    /// Shard height at backup time.
    pub shard_height: u64,
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
}

/// Manages backup operations for a shard.
pub struct BackupManager {
    /// Backup destination directory.
    backup_dir: PathBuf,
    /// Maximum number of backups to retain.
    retention_count: usize,
}

impl BackupManager {
    /// Create a new backup manager.
    pub fn new(config: &BackupConfig) -> Result<Self> {
        let backup_dir = PathBuf::from(&config.destination);
        fs::create_dir_all(&backup_dir).context(IoSnafu)?;

        Ok(Self { backup_dir, retention_count: config.retention_count })
    }

    /// Create a backup from the given snapshot.
    ///
    /// Writes the snapshot to the backup directory and creates a metadata sidecar.
    /// Returns the backup metadata.
    pub fn create_backup(&self, snapshot: &Snapshot, tag: &str) -> Result<BackupMetadata> {
        let now = Utc::now();
        let backup_id =
            format!("{}-{:09}", now.format("%Y%m%dT%H%M%SZ"), snapshot.header.shard_height);

        let backup_filename = format!("{backup_id}{BACKUP_EXT}");
        let backup_path = self.backup_dir.join(&backup_filename);

        // Write snapshot to backup destination
        snapshot.write_to_file(&backup_path).context(SnapshotSnafu)?;

        // Get file size
        let size_bytes = fs::metadata(&backup_path).context(IoSnafu)?.len();

        let metadata = BackupMetadata {
            backup_id: backup_id.clone(),
            shard_id: snapshot.header.shard_id,
            shard_height: snapshot.header.shard_height,
            backup_path: backup_path.display().to_string(),
            size_bytes,
            created_at: now,
            checksum: snapshot.header.checksum,
            chain_commitment_hash: snapshot.header.chain_commitment.accumulated_header_hash,
            schema_version: snapshot.header.version,
            tag: tag.to_string(),
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
            shard_height = metadata.shard_height,
            size_bytes = metadata.size_bytes,
            "Backup created"
        );

        Ok(metadata)
    }

    /// List available backups sorted by creation time (newest first).
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

    /// Load a backup snapshot by backup ID.
    ///
    /// Reads the backup file and verifies its integrity (checksum validation
    /// is performed by `Snapshot::read_from_file`).
    pub fn load_backup(&self, backup_id: &str) -> Result<Snapshot> {
        let backup_path = self.backup_dir.join(format!("{backup_id}{BACKUP_EXT}"));

        if !backup_path.exists() {
            return Err(BackupError::NotFound { backup_id: backup_id.to_string() });
        }

        Snapshot::read_from_file(&backup_path).context(SnapshotSnafu)
    }

    /// Get metadata for a specific backup.
    pub fn get_metadata(&self, backup_id: &str) -> Result<BackupMetadata> {
        let meta_path = self.backup_dir.join(format!("{backup_id}{META_EXT}"));

        if !meta_path.exists() {
            return Err(BackupError::NotFound { backup_id: backup_id.to_string() });
        }

        let contents = fs::read_to_string(&meta_path).context(IoSnafu)?;
        serde_json::from_str(&contents)
            .map_err(|e| BackupError::Serialization { message: e.to_string() })
    }

    /// Prune old backups, keeping only the most recent `retention_count`.
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
                shard_height = backup.shard_height,
                "Pruned old backup"
            );
        }

        Ok(())
    }

    /// Get the backup directory path.
    pub fn backup_dir(&self) -> &Path {
        &self.backup_dir
    }
}

/// Metric recording for backup operations.
pub fn record_backup_created(shard_height: u64, size_bytes: u64) {
    metrics::counter!("ledger_backups_created_total").increment(1);
    metrics::gauge!("ledger_backup_last_height").set(shard_height as f64);
    metrics::gauge!("ledger_backup_last_size_bytes").set(size_bytes as f64);
}

/// Record a backup failure.
pub fn record_backup_failed() {
    metrics::counter!("ledger_backup_failures_total").increment(1);
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
    /// The Raft instance (for leader check and triggering snapshots).
    raft: Arc<Raft<LedgerTypeConfig>>,
    /// This node's ID.
    node_id: LedgerNodeId,
    /// Snapshot manager for reading Raft snapshots.
    snapshot_manager: Arc<SnapshotManager>,
    /// Backup manager for file operations.
    backup_manager: Arc<BackupManager>,
    /// Interval between backup cycles.
    interval: Duration,
}

impl BackupJob {
    /// Start the backup job as a background task.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.interval);

            loop {
                ticker.tick().await;

                // Only the leader creates backups
                let metrics = self.raft.metrics().borrow().clone();
                if metrics.current_leader != Some(self.node_id) {
                    debug!("Not leader, skipping backup cycle");
                    continue;
                }

                info!("Starting automated backup");

                // Trigger a Raft snapshot to ensure latest state is on disk
                if let Err(e) = self.raft.trigger().snapshot().await {
                    warn!(error = %e, "Failed to trigger snapshot for backup");
                    record_backup_failed();
                    continue;
                }

                // Load the latest snapshot from disk
                match self.snapshot_manager.load_latest() {
                    Ok(Some(snapshot)) => {
                        match self.backup_manager.create_backup(&snapshot, "auto") {
                            Ok(meta) => {
                                record_backup_created(meta.shard_height, meta.size_bytes);
                                info!(
                                    backup_id = %meta.backup_id,
                                    shard_height = meta.shard_height,
                                    size_bytes = meta.size_bytes,
                                    "Automated backup completed"
                                );
                            },
                            Err(e) => {
                                error!(error = %e, "Failed to write backup");
                                record_backup_failed();
                            },
                        }
                    },
                    Ok(None) => {
                        warn!("No snapshot available for backup");
                        record_backup_failed();
                    },
                    Err(e) => {
                        error!(error = %e, "Failed to load snapshot for backup");
                        record_backup_failed();
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

    fn create_test_snapshot(shard_height: u64) -> Snapshot {
        let vault_states = vec![VaultSnapshotMeta::new(
            VaultId::new(1),
            shard_height / 2,
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
                to_height: shard_height,
            },
        };

        Snapshot::new(ShardId::new(1), shard_height, vault_states, state, chain_params)
            .expect("create snapshot")
    }

    #[test]
    fn test_backup_manager_create_and_list() {
        let temp = TempDir::new().expect("create temp dir");
        let config = create_test_backup_config(temp.path());
        let manager = BackupManager::new(&config).expect("create manager");

        let snapshot = create_test_snapshot(1000);
        let meta = manager.create_backup(&snapshot, "test").expect("create backup");

        assert_eq!(meta.shard_height, 1000);
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
        assert_eq!(loaded.header.shard_height, 2000);
        assert_eq!(loaded.header.shard_id, ShardId::new(1));
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
        assert_eq!(loaded_meta.shard_height, 3000);
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
        assert_eq!(backups[0].shard_height, 500);
        assert_eq!(backups[1].shard_height, 400);
        assert_eq!(backups[2].shard_height, 300);
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
}
