//! Time-travel index for fast historical queries.
//!
//! Per DESIGN.md §3.5: Optional index that stores historical versions of
//! entity values for high-frequency audit targets. Enables O(1) historical
//! reads without snapshot replay.
//!
//! ## Usage
//!
//! Time-travel indexing is opt-in per vault:
//! - Enable via vault configuration
//! - Storage overhead ~10x for indexed keys
//! - Best for frequently audited entities
//!
//! ## Storage Format
//!
//! Index entries are stored as:
//! - Key: `(vault_id, key, block_height)` - descending height for efficient range scans
//! - Value: Serialized entity value (or tombstone marker for deletions)

use std::sync::Arc;

use parking_lot::RwLock;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

/// Table for time-travel index configuration per vault.
const TIME_TRAVEL_CONFIG_TABLE: TableDefinition<u64, &[u8]> =
    TableDefinition::new("time_travel_config");

/// Table for time-travel index entries.
/// Key: (vault_id, key_hash, height) encoded as bytes
/// Value: Serialized TimeTravelEntry
const TIME_TRAVEL_INDEX_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("time_travel_index");

/// Error types for time-travel index operations.
#[derive(Debug, Snafu)]
pub enum TimeTravelError {
    /// Error from redb operations.
    #[snafu(display("Database error: {source}"))]
    Database {
        /// The underlying redb error.
        source: redb::Error,
    },

    /// Error from redb transaction operations.
    #[snafu(display("Transaction error: {source}"))]
    Transaction {
        /// The underlying redb transaction error.
        source: redb::TransactionError,
    },

    /// Error from redb commit operations.
    #[snafu(display("Commit error: {source}"))]
    Commit {
        /// The underlying redb commit error.
        source: redb::CommitError,
    },

    /// Error from redb table operations.
    #[snafu(display("Table error: {source}"))]
    Table {
        /// The underlying redb table error.
        source: redb::TableError,
    },

    /// Error from redb storage operations.
    #[snafu(display("Storage error: {source}"))]
    Storage {
        /// The underlying redb storage error.
        source: redb::StorageError,
    },

    /// Serialization error.
    #[snafu(display("Serialization error: {message}"))]
    Serialization {
        /// Description of the serialization error.
        message: String,
    },

    /// The vault is not configured for time-travel indexing.
    #[snafu(display("Vault {vault_id} is not configured for time-travel indexing"))]
    NotEnabled {
        /// The vault ID.
        vault_id: u64,
    },

    /// Height not found in index.
    #[snafu(display("No entry found for key at height {height}"))]
    HeightNotFound {
        /// The requested height.
        height: u64,
    },
}

/// Result type for time-travel operations.
pub type Result<T> = std::result::Result<T, TimeTravelError>;

/// Configuration for time-travel indexing on a vault.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelConfig {
    /// Whether time-travel indexing is enabled.
    pub enabled: bool,
    /// Optional list of key prefixes to index (empty = index all).
    pub key_prefixes: Vec<String>,
    /// Maximum history depth to retain (0 = unlimited).
    pub max_history_depth: u64,
}

impl Default for TimeTravelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            key_prefixes: vec![],
            max_history_depth: 0,
        }
    }
}

impl TimeTravelConfig {
    /// Create a new config with time-travel enabled for all keys.
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Create a config with time-travel enabled for specific key prefixes.
    pub fn with_prefixes(prefixes: Vec<String>) -> Self {
        Self {
            enabled: true,
            key_prefixes: prefixes,
            max_history_depth: 0,
        }
    }

    /// Check if a key should be indexed.
    pub fn should_index(&self, key: &str) -> bool {
        if !self.enabled {
            return false;
        }
        if self.key_prefixes.is_empty() {
            return true;
        }
        self.key_prefixes.iter().any(|p| key.starts_with(p))
    }
}

/// A single historical entry in the time-travel index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelEntry {
    /// The block height when this value was set.
    pub height: u64,
    /// The value at this height (None = deleted/tombstone).
    pub value: Option<Vec<u8>>,
    /// The version number at this height.
    pub version: u64,
}

/// Index key format: vault_id (8) + key_hash (8) + inverted_height (8)
/// Height is inverted (u64::MAX - height) for descending order iteration.
fn make_index_key(vault_id: u64, key: &str, height: u64) -> Vec<u8> {
    let key_hash = seahash::hash(key.as_bytes());
    let inverted_height = u64::MAX - height;

    let mut buf = Vec::with_capacity(24);
    buf.extend_from_slice(&vault_id.to_be_bytes());
    buf.extend_from_slice(&key_hash.to_be_bytes());
    buf.extend_from_slice(&inverted_height.to_be_bytes());
    buf
}

/// Parse an index key back into components.
fn parse_index_key(key: &[u8]) -> Option<(u64, u64, u64)> {
    if key.len() != 24 {
        return None;
    }
    let vault_id = u64::from_be_bytes(key[0..8].try_into().ok()?);
    let key_hash = u64::from_be_bytes(key[8..16].try_into().ok()?);
    let inverted_height = u64::from_be_bytes(key[16..24].try_into().ok()?);
    let height = u64::MAX - inverted_height;
    Some((vault_id, key_hash, height))
}

/// Time-travel index manager.
///
/// Manages historical versions of entity values for fast point-in-time queries.
pub struct TimeTravelIndex {
    db: Arc<Database>,
    /// Cache of vault configurations.
    config_cache: RwLock<std::collections::HashMap<u64, TimeTravelConfig>>,
}

impl TimeTravelIndex {
    /// Create a new time-travel index manager.
    pub fn new(db: Arc<Database>) -> Result<Self> {
        // Ensure tables exist
        let write_txn = db.begin_write().context(TransactionSnafu)?;
        {
            let _ = write_txn
                .open_table(TIME_TRAVEL_CONFIG_TABLE)
                .context(TableSnafu)?;
            let _ = write_txn
                .open_table(TIME_TRAVEL_INDEX_TABLE)
                .context(TableSnafu)?;
        }
        write_txn.commit().context(CommitSnafu)?;

        Ok(Self {
            db,
            config_cache: RwLock::new(std::collections::HashMap::new()),
        })
    }

    /// Configure time-travel indexing for a vault.
    pub fn configure_vault(&self, vault_id: u64, config: TimeTravelConfig) -> Result<()> {
        let serialized =
            postcard::to_allocvec(&config).map_err(|e| TimeTravelError::Serialization {
                message: e.to_string(),
            })?;

        let write_txn = self.db.begin_write().context(TransactionSnafu)?;
        {
            let mut table = write_txn
                .open_table(TIME_TRAVEL_CONFIG_TABLE)
                .context(TableSnafu)?;
            table
                .insert(vault_id, serialized.as_slice())
                .context(StorageSnafu)?;
        }
        write_txn.commit().context(CommitSnafu)?;

        // Update cache
        self.config_cache.write().insert(vault_id, config);

        Ok(())
    }

    /// Get the configuration for a vault.
    pub fn get_config(&self, vault_id: u64) -> Result<Option<TimeTravelConfig>> {
        // Check cache first
        if let Some(config) = self.config_cache.read().get(&vault_id) {
            return Ok(Some(config.clone()));
        }

        // Load from database
        let read_txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = read_txn
            .open_table(TIME_TRAVEL_CONFIG_TABLE)
            .context(TableSnafu)?;

        match table.get(vault_id).context(StorageSnafu)? {
            Some(data) => {
                let config: TimeTravelConfig = postcard::from_bytes(data.value()).map_err(|e| {
                    TimeTravelError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                self.config_cache.write().insert(vault_id, config.clone());
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }

    /// Check if time-travel is enabled for a vault and key.
    pub fn is_enabled(&self, vault_id: u64, key: &str) -> Result<bool> {
        match self.get_config(vault_id)? {
            Some(config) => Ok(config.should_index(key)),
            None => Ok(false),
        }
    }

    /// Record a value change in the time-travel index.
    pub fn record(
        &self,
        vault_id: u64,
        key: &str,
        height: u64,
        value: Option<Vec<u8>>,
        version: u64,
    ) -> Result<()> {
        // Check if enabled for this key
        if !self.is_enabled(vault_id, key)? {
            return Ok(());
        }

        let entry = TimeTravelEntry {
            height,
            value,
            version,
        };

        let serialized =
            postcard::to_allocvec(&entry).map_err(|e| TimeTravelError::Serialization {
                message: e.to_string(),
            })?;

        let index_key = make_index_key(vault_id, key, height);

        let write_txn = self.db.begin_write().context(TransactionSnafu)?;
        {
            let mut table = write_txn
                .open_table(TIME_TRAVEL_INDEX_TABLE)
                .context(TableSnafu)?;
            table
                .insert(index_key.as_slice(), serialized.as_slice())
                .context(StorageSnafu)?;
        }
        write_txn.commit().context(CommitSnafu)?;

        Ok(())
    }

    /// Get the value at a specific height.
    ///
    /// Returns the entry at or before the requested height.
    pub fn get_at_height(
        &self,
        vault_id: u64,
        key: &str,
        height: u64,
    ) -> Result<Option<TimeTravelEntry>> {
        let read_txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = read_txn
            .open_table(TIME_TRAVEL_INDEX_TABLE)
            .context(TableSnafu)?;

        // Create range start key (vault_id, key_hash, height)
        // Since height is inverted, we start from the requested height
        let start_key = make_index_key(vault_id, key, height);
        let key_hash = seahash::hash(key.as_bytes());

        // Scan for first entry at or before requested height
        let range = table.range(start_key.as_slice()..).context(StorageSnafu)?;

        for result in range {
            let (k, v) = result.context(StorageSnafu)?;
            let key_bytes = k.value();

            // Parse key to check vault_id and key_hash match
            if let Some((v_id, k_hash, _entry_height)) = parse_index_key(key_bytes) {
                if v_id != vault_id || k_hash != key_hash {
                    // Different vault or key - no entry found
                    break;
                }

                // Found an entry at or before requested height
                let entry: TimeTravelEntry = postcard::from_bytes(v.value()).map_err(|e| {
                    TimeTravelError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    /// Get all historical entries for a key.
    pub fn get_history(
        &self,
        vault_id: u64,
        key: &str,
        limit: Option<usize>,
    ) -> Result<Vec<TimeTravelEntry>> {
        let read_txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = read_txn
            .open_table(TIME_TRAVEL_INDEX_TABLE)
            .context(TableSnafu)?;

        // Start from most recent (height = u64::MAX means inverted = 0)
        let start_key = make_index_key(vault_id, key, u64::MAX);
        let key_hash = seahash::hash(key.as_bytes());

        let range = table.range(start_key.as_slice()..).context(StorageSnafu)?;

        let mut entries = Vec::new();
        let max_entries = limit.unwrap_or(usize::MAX);

        for result in range {
            if entries.len() >= max_entries {
                break;
            }

            let (k, v) = result.context(StorageSnafu)?;
            let key_bytes = k.value();

            if let Some((v_id, k_hash, _height)) = parse_index_key(key_bytes) {
                if v_id != vault_id || k_hash != key_hash {
                    break;
                }

                let entry: TimeTravelEntry = postcard::from_bytes(v.value()).map_err(|e| {
                    TimeTravelError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    /// Prune old entries beyond the max history depth.
    pub fn prune(&self, vault_id: u64, current_height: u64) -> Result<u64> {
        let config = match self.get_config(vault_id)? {
            Some(c) if c.max_history_depth > 0 => c,
            _ => return Ok(0), // No pruning configured
        };

        let cutoff_height = current_height.saturating_sub(config.max_history_depth);
        if cutoff_height == 0 {
            return Ok(0);
        }

        let write_txn = self.db.begin_write().context(TransactionSnafu)?;
        let mut pruned = 0u64;
        {
            let mut table = write_txn
                .open_table(TIME_TRAVEL_INDEX_TABLE)
                .context(TableSnafu)?;

            // Find keys to delete (height < cutoff)
            // Note: inverted > cutoff_inverted when height < cutoff

            // We need to scan all keys for this vault
            let mut keys_to_delete = Vec::new();

            // Scan range for this vault
            let start = vault_id.to_be_bytes();
            let end = (vault_id + 1).to_be_bytes();

            let range = table
                .range(start.as_slice()..end.as_slice())
                .context(StorageSnafu)?;

            for result in range {
                let (k, _) = result.context(StorageSnafu)?;
                let key_bytes = k.value();

                if let Some((v_id, _, height)) = parse_index_key(key_bytes) {
                    if v_id == vault_id && height < cutoff_height {
                        keys_to_delete.push(key_bytes.to_vec());
                    }
                }
            }

            // Delete the keys
            for key in keys_to_delete {
                table.remove(key.as_slice()).context(StorageSnafu)?;
                pruned += 1;
            }
        }
        write_txn.commit().context(CommitSnafu)?;

        Ok(pruned)
    }

    /// Get statistics for time-travel index.
    pub fn stats(&self, vault_id: u64) -> Result<TimeTravelStats> {
        let read_txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = read_txn
            .open_table(TIME_TRAVEL_INDEX_TABLE)
            .context(TableSnafu)?;

        let start = vault_id.to_be_bytes();
        let end = (vault_id + 1).to_be_bytes();

        let range = table
            .range(start.as_slice()..end.as_slice())
            .context(StorageSnafu)?;

        let mut entry_count = 0u64;
        let mut total_bytes = 0u64;
        let mut unique_keys = std::collections::HashSet::new();

        for result in range {
            let (k, v) = result.context(StorageSnafu)?;
            if let Some((_, key_hash, _)) = parse_index_key(k.value()) {
                unique_keys.insert(key_hash);
                entry_count += 1;
                total_bytes += v.value().len() as u64;
            }
        }

        Ok(TimeTravelStats {
            entry_count,
            unique_keys: unique_keys.len() as u64,
            total_bytes,
        })
    }
}

/// Statistics for time-travel index.
#[derive(Debug, Clone)]
pub struct TimeTravelStats {
    /// Total number of historical entries.
    pub entry_count: u64,
    /// Number of unique keys tracked.
    pub unique_keys: u64,
    /// Total storage used in bytes.
    pub total_bytes: u64,
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    clippy::panic
)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_db() -> (TempDir, Arc<Database>) {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path().join("test.redb")).unwrap();
        (dir, Arc::new(db))
    }

    #[test]
    fn test_config_should_index() {
        let config = TimeTravelConfig::default();
        assert!(!config.should_index("any_key"));

        let config = TimeTravelConfig::enabled();
        assert!(config.should_index("any_key"));

        let config = TimeTravelConfig::with_prefixes(vec!["user:".to_string()]);
        assert!(config.should_index("user:123"));
        assert!(!config.should_index("order:456"));
    }

    #[test]
    fn test_index_key_encoding() {
        let vault_id = 42;
        let key = "test_key";
        let height = 1000;

        let encoded = make_index_key(vault_id, key, height);
        let (v, k, h) = parse_index_key(&encoded).unwrap();

        assert_eq!(v, vault_id);
        assert_eq!(k, seahash::hash(key.as_bytes()));
        assert_eq!(h, height);
    }

    #[test]
    fn test_height_ordering() {
        // Keys should be ordered descending by height (most recent first)
        let key1 = make_index_key(1, "key", 100);
        let key2 = make_index_key(1, "key", 200);
        let key3 = make_index_key(1, "key", 50);

        // Higher heights should come first (smaller inverted values)
        assert!(key2 < key1);
        assert!(key1 < key3);
    }

    #[test]
    fn test_configure_vault() {
        let (_dir, db) = create_test_db();
        let index = TimeTravelIndex::new(db).unwrap();

        // Initially not configured
        assert!(index.get_config(1).unwrap().is_none());

        // Configure vault
        let config = TimeTravelConfig::enabled();
        index.configure_vault(1, config).unwrap();

        // Now configured
        let loaded = index.get_config(1).unwrap().unwrap();
        assert!(loaded.enabled);
    }

    #[test]
    fn test_record_and_retrieve() {
        let (_dir, db) = create_test_db();
        let index = TimeTravelIndex::new(db).unwrap();

        // Enable time-travel for vault
        index
            .configure_vault(1, TimeTravelConfig::enabled())
            .unwrap();

        // Record some values
        index
            .record(1, "key1", 10, Some(b"value_v1".to_vec()), 1)
            .unwrap();
        index
            .record(1, "key1", 20, Some(b"value_v2".to_vec()), 2)
            .unwrap();
        index
            .record(1, "key1", 30, Some(b"value_v3".to_vec()), 3)
            .unwrap();

        // Get at specific heights
        let entry = index.get_at_height(1, "key1", 25).unwrap().unwrap();
        assert_eq!(entry.height, 20);
        assert_eq!(entry.value, Some(b"value_v2".to_vec()));

        let entry = index.get_at_height(1, "key1", 30).unwrap().unwrap();
        assert_eq!(entry.height, 30);
        assert_eq!(entry.value, Some(b"value_v3".to_vec()));

        let entry = index.get_at_height(1, "key1", 100).unwrap().unwrap();
        assert_eq!(entry.height, 30); // Most recent before 100
    }

    #[test]
    fn test_get_history() {
        let (_dir, db) = create_test_db();
        let index = TimeTravelIndex::new(db).unwrap();

        index
            .configure_vault(1, TimeTravelConfig::enabled())
            .unwrap();

        // Record multiple versions
        for i in 1..=5 {
            index
                .record(1, "key1", i * 10, Some(format!("v{}", i).into_bytes()), i)
                .unwrap();
        }

        // Get full history
        let history = index.get_history(1, "key1", None).unwrap();
        assert_eq!(history.len(), 5);
        // Should be in descending order (most recent first)
        assert_eq!(history[0].height, 50);
        assert_eq!(history[4].height, 10);

        // Get limited history
        let history = index.get_history(1, "key1", Some(2)).unwrap();
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn test_tombstone() {
        let (_dir, db) = create_test_db();
        let index = TimeTravelIndex::new(db).unwrap();

        index
            .configure_vault(1, TimeTravelConfig::enabled())
            .unwrap();

        // Record value then delete
        index
            .record(1, "key1", 10, Some(b"value".to_vec()), 1)
            .unwrap();
        index.record(1, "key1", 20, None, 2).unwrap(); // Tombstone

        // Before delete
        let entry = index.get_at_height(1, "key1", 15).unwrap().unwrap();
        assert!(entry.value.is_some());

        // After delete
        let entry = index.get_at_height(1, "key1", 25).unwrap().unwrap();
        assert!(entry.value.is_none()); // Tombstone
    }

    #[test]
    fn test_prefix_filtering() {
        let (_dir, db) = create_test_db();
        let index = TimeTravelIndex::new(db).unwrap();

        // Only index "user:" prefix
        index
            .configure_vault(
                1,
                TimeTravelConfig::with_prefixes(vec!["user:".to_string()]),
            )
            .unwrap();

        // Record user key (should be indexed)
        index
            .record(1, "user:123", 10, Some(b"alice".to_vec()), 1)
            .unwrap();

        // Record order key (should NOT be indexed)
        index
            .record(1, "order:456", 10, Some(b"pizza".to_vec()), 1)
            .unwrap();

        // User key should be retrievable
        assert!(index.get_at_height(1, "user:123", 10).unwrap().is_some());

        // Order key should NOT be retrievable
        assert!(index.get_at_height(1, "order:456", 10).unwrap().is_none());
    }

    #[test]
    fn test_stats() {
        let (_dir, db) = create_test_db();
        let index = TimeTravelIndex::new(db).unwrap();

        index
            .configure_vault(1, TimeTravelConfig::enabled())
            .unwrap();

        // Record entries for multiple keys
        index
            .record(1, "key1", 10, Some(b"v1".to_vec()), 1)
            .unwrap();
        index
            .record(1, "key1", 20, Some(b"v2".to_vec()), 2)
            .unwrap();
        index
            .record(1, "key2", 10, Some(b"v1".to_vec()), 1)
            .unwrap();

        let stats = index.stats(1).unwrap();
        assert_eq!(stats.entry_count, 3);
        assert_eq!(stats.unique_keys, 2);
        assert!(stats.total_bytes > 0);
    }

    #[test]
    fn test_not_enabled_skips_recording() {
        let (_dir, db) = create_test_db();
        let index = TimeTravelIndex::new(db).unwrap();

        // Don't configure vault - time-travel is disabled

        // Record should succeed but not store anything
        index
            .record(1, "key1", 10, Some(b"value".to_vec()), 1)
            .unwrap();

        // Nothing should be retrievable
        assert!(index.get_at_height(1, "key1", 10).unwrap().is_none());
    }
}
