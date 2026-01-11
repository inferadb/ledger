//! Block archive for permanent block storage.
//!
//! Per DESIGN.md:
//! - Segment-based storage (10,000 blocks per segment)
//! - Append-only within segments
//! - Index files for fast block lookup
//! - Headers are never truncated; transactions are configurable

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use redb::{Database, ReadableTable};
use snafu::{ResultExt, Snafu};

use ledger_types::{NamespaceId, ShardBlock, VaultId};

use crate::tables::Tables;

/// Blocks per segment file.
const SEGMENT_SIZE: u64 = 10_000;

/// Block archive error types.
#[derive(Debug, Snafu)]
pub enum BlockArchiveError {
    #[snafu(display("IO error: {source}"))]
    Io { source: std::io::Error },

    #[snafu(display("Block not found at height {height}"))]
    BlockNotFound { height: u64 },

    #[snafu(display("Serialization error: {message}"))]
    Serialization { message: String },

    #[snafu(display("Storage error: {source}"))]
    Storage { source: redb::StorageError },

    #[snafu(display("Table error: {source}"))]
    Table { source: redb::TableError },

    #[snafu(display("Transaction error: {source}"))]
    Transaction { source: redb::TransactionError },

    #[snafu(display("Commit error: {source}"))]
    Commit { source: redb::CommitError },
}

/// Result type for block archive operations.
pub type Result<T> = std::result::Result<T, BlockArchiveError>;

/// Index entry for a block within a segment.
#[derive(Debug, Clone, Copy)]
struct BlockIndex {
    /// Shard height of this block.
    shard_height: u64,
    /// Offset in segment file.
    offset: u64,
    /// Length of serialized block (including 4-byte length prefix).
    length: u32,
}

/// Segment writer for active segment.
struct SegmentWriter {
    segment_id: u64,
    file: BufWriter<File>,
    index: Vec<BlockIndex>,
}

/// Block archive for a shard.
///
/// Uses redb for the primary block storage (Tables::BLOCKS) and maintains
/// a vault_block_index for looking up shard heights by vault/namespace/height.
pub struct BlockArchive {
    /// Database handle.
    db: Arc<Database>,
    /// Directory for segment files (optional, for large deployments).
    blocks_dir: Option<PathBuf>,
    /// Cached segment indexes.
    segment_indexes: RwLock<HashMap<u64, Vec<BlockIndex>>>,
    /// Current segment writer (for file-based storage).
    current_segment: RwLock<Option<SegmentWriter>>,
}

impl BlockArchive {
    /// Create a new block archive backed by redb.
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            blocks_dir: None,
            segment_indexes: RwLock::new(HashMap::new()),
            current_segment: RwLock::new(None),
        }
    }

    /// Create a block archive with file-based segment storage.
    ///
    /// Use this for large deployments where blocks exceed redb's practical limits.
    pub fn with_segment_files(db: Arc<Database>, blocks_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&blocks_dir).context(IoSnafu)?;
        Ok(Self {
            db,
            blocks_dir: Some(blocks_dir),
            segment_indexes: RwLock::new(HashMap::new()),
            current_segment: RwLock::new(None),
        })
    }

    /// Append a block to the archive.
    pub fn append_block(&self, block: &ShardBlock) -> Result<()> {
        let encoded =
            postcard::to_allocvec(block).map_err(|e| BlockArchiveError::Serialization {
                message: e.to_string(),
            })?;

        // Store in redb
        let txn = self.db.begin_write().context(TransactionSnafu)?;

        {
            let mut blocks_table = txn.open_table(Tables::BLOCKS).context(TableSnafu)?;
            let mut index_table = txn
                .open_table(Tables::VAULT_BLOCK_INDEX)
                .context(TableSnafu)?;

            // Store the block
            blocks_table
                .insert(block.shard_height, &encoded[..])
                .context(StorageSnafu)?;

            // Update vault block index for each vault entry
            for entry in &block.vault_entries {
                let index_key = encode_vault_block_index_key(
                    entry.namespace_id,
                    entry.vault_id,
                    entry.vault_height,
                );
                index_table
                    .insert(&index_key[..], block.shard_height)
                    .context(StorageSnafu)?;
            }
        }

        txn.commit().context(CommitSnafu)?;

        // Also write to segment files if configured
        if self.blocks_dir.is_some() {
            self.append_to_segment(block, &encoded)?;
        }

        Ok(())
    }

    /// Append block to segment file.
    fn append_to_segment(&self, block: &ShardBlock, encoded: &[u8]) -> Result<()> {
        let segment_id = block.shard_height / SEGMENT_SIZE;

        let mut current = self.current_segment.write();

        // Rotate segment if needed
        if current.as_ref().map(|s| s.segment_id) != Some(segment_id) {
            // Flush current segment if exists
            if let Some(writer) = current.take() {
                drop(writer);
            }

            // Open new segment
            let segment_path = self.segment_path(segment_id);
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&segment_path)
                .context(IoSnafu)?;

            *current = Some(SegmentWriter {
                segment_id,
                file: BufWriter::new(file),
                index: Vec::new(),
            });
        }

        let writer = current.as_mut().expect("segment writer should exist");

        // Get current offset
        let offset = writer.file.seek(SeekFrom::End(0)).context(IoSnafu)?;

        // Write length-prefixed block
        writer
            .file
            .write_all(&(encoded.len() as u32).to_le_bytes())
            .context(IoSnafu)?;
        writer.file.write_all(encoded).context(IoSnafu)?;
        writer.file.flush().context(IoSnafu)?;

        // Update index
        writer.index.push(BlockIndex {
            shard_height: block.shard_height,
            offset,
            length: (encoded.len() + 4) as u32,
        });

        Ok(())
    }

    /// Read a block by shard height.
    pub fn read_block(&self, shard_height: u64) -> Result<ShardBlock> {
        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::BLOCKS).context(TableSnafu)?;

        match table.get(shard_height).context(StorageSnafu)? {
            Some(data) => {
                let block = postcard::from_bytes(data.value()).map_err(|e| {
                    BlockArchiveError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                Ok(block)
            }
            None => Err(BlockArchiveError::BlockNotFound {
                height: shard_height,
            }),
        }
    }

    /// Find the shard height containing a specific vault block.
    pub fn find_shard_height(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        vault_height: u64,
    ) -> Result<Option<u64>> {
        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn
            .open_table(Tables::VAULT_BLOCK_INDEX)
            .context(TableSnafu)?;

        let key = encode_vault_block_index_key(namespace_id, vault_id, vault_height);

        match table.get(&key[..]).context(StorageSnafu)? {
            Some(height) => Ok(Some(height.value())),
            None => Ok(None),
        }
    }

    /// Get the latest shard height in the archive.
    pub fn latest_height(&self) -> Result<Option<u64>> {
        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::BLOCKS).context(TableSnafu)?;

        // redb tables iterate in key order, so we need to find the last entry
        let mut latest = None;
        for result in table.range(0u64..).context(StorageSnafu)? {
            let (key, _) = result.context(StorageSnafu)?;
            latest = Some(key.value());
        }

        Ok(latest)
    }

    /// Get the range of heights in the archive.
    pub fn height_range(&self) -> Result<Option<(u64, u64)>> {
        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::BLOCKS).context(TableSnafu)?;

        let mut first = None;
        let mut last = None;

        for result in table.range(0u64..).context(StorageSnafu)? {
            let (key, _) = result.context(StorageSnafu)?;
            let height = key.value();
            if first.is_none() {
                first = Some(height);
            }
            last = Some(height);
        }

        match (first, last) {
            (Some(f), Some(l)) => Ok(Some((f, l))),
            _ => Ok(None),
        }
    }

    /// Read blocks in a range.
    pub fn read_range(&self, start: u64, end: u64) -> Result<Vec<ShardBlock>> {
        let txn = self.db.begin_read().context(TransactionSnafu)?;
        let table = txn.open_table(Tables::BLOCKS).context(TableSnafu)?;

        let mut blocks = Vec::new();

        for result in table.range(start..=end).context(StorageSnafu)? {
            let (_, data) = result.context(StorageSnafu)?;
            let block = postcard::from_bytes(data.value()).map_err(|e| {
                BlockArchiveError::Serialization {
                    message: e.to_string(),
                }
            })?;
            blocks.push(block);
        }

        Ok(blocks)
    }

    /// Get segment file path.
    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.blocks_dir
            .as_ref()
            .expect("blocks_dir should be set")
            .join(format!("segment_{:06}.blk", segment_id))
    }
}

/// Encode vault block index key.
///
/// Format: {namespace_id:8BE}{vault_id:8BE}{vault_height:8BE}
fn encode_vault_block_index_key(
    namespace_id: NamespaceId,
    vault_id: VaultId,
    vault_height: u64,
) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[0..8].copy_from_slice(&namespace_id.to_be_bytes());
    key[8..16].copy_from_slice(&vault_id.to_be_bytes());
    key[16..24].copy_from_slice(&vault_height.to_be_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::StorageEngine;
    use chrono::Utc;
    use ledger_types::{Hash, VaultEntry};

    fn create_test_block(shard_height: u64) -> ShardBlock {
        ShardBlock {
            shard_id: 1,
            shard_height,
            previous_shard_hash: [0u8; 32],
            vault_entries: vec![VaultEntry {
                namespace_id: 1,
                vault_id: 1,
                vault_height: shard_height, // Simplify: vault_height == shard_height
                previous_vault_hash: [0u8; 32],
                transactions: vec![],
                tx_merkle_root: [0u8; 32],
                state_root: [shard_height as u8; 32],
            }],
            timestamp: Utc::now(),
            leader_id: "node-1".to_string(),
            term: 1,
            committed_index: shard_height,
        }
    }

    #[test]
    fn test_append_and_read_block() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        let block = create_test_block(100);
        archive.append_block(&block).expect("append block");

        let loaded = archive.read_block(100).expect("read block");
        assert_eq!(loaded.shard_height, 100);
        assert_eq!(loaded.vault_entries.len(), 1);
    }

    #[test]
    fn test_vault_block_index() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        let block = create_test_block(100);
        archive.append_block(&block).expect("append block");

        // Should find the shard height from vault coordinates
        let shard_height = archive
            .find_shard_height(1, 1, 100)
            .expect("find")
            .expect("should exist");
        assert_eq!(shard_height, 100);

        // Non-existent vault block
        let not_found = archive.find_shard_height(1, 1, 999).expect("find");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_read_range() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        // Append multiple blocks
        for height in [100, 101, 102, 103, 104] {
            let block = create_test_block(height);
            archive.append_block(&block).expect("append block");
        }

        // Read range
        let blocks = archive.read_range(101, 103).expect("read range");
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].shard_height, 101);
        assert_eq!(blocks[1].shard_height, 102);
        assert_eq!(blocks[2].shard_height, 103);
    }

    #[test]
    fn test_height_range() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        // Empty archive
        assert!(archive.height_range().expect("range").is_none());

        // Add blocks
        for height in [100, 200, 300] {
            let block = create_test_block(height);
            archive.append_block(&block).expect("append block");
        }

        let (min, max) = archive.height_range().expect("range").expect("not empty");
        assert_eq!(min, 100);
        assert_eq!(max, 300);
    }

    #[test]
    fn test_latest_height() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        assert!(archive.latest_height().expect("latest").is_none());

        archive
            .append_block(&create_test_block(50))
            .expect("append");
        assert_eq!(archive.latest_height().expect("latest"), Some(50));

        archive
            .append_block(&create_test_block(100))
            .expect("append");
        assert_eq!(archive.latest_height().expect("latest"), Some(100));
    }

    #[test]
    fn test_multiple_vaults_in_block() {
        let engine = StorageEngine::open_in_memory().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        let mut block = create_test_block(100);
        block.vault_entries.push(VaultEntry {
            namespace_id: 1,
            vault_id: 2, // Different vault
            vault_height: 50,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [42u8; 32],
        });

        archive.append_block(&block).expect("append block");

        // Both vaults should point to same shard height
        let h1 = archive.find_shard_height(1, 1, 100).expect("find");
        let h2 = archive.find_shard_height(1, 2, 50).expect("find");

        assert_eq!(h1, Some(100));
        assert_eq!(h2, Some(100));
    }
}
