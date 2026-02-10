//! Block archive for permanent block storage.
//!
//! Per DESIGN.md:
//! - Segment-based storage (10,000 blocks per segment)
//! - Append-only within segments
//! - Index files for fast block lookup
//! - Headers are never truncated; transactions are configurable
//!
//! ## Block Compaction
//!
//! In compacted mode, transaction bodies are removed while preserving:
//! - Block headers (height, previous_hash, tx_merkle_root, state_root)
//! - Vault metadata (namespace_id, vault_id, vault_height)
//! - Cryptographic commitments (all hashes preserved for verification)
//!
//! This allows verification via:
//! - Chain integrity (header hash links)
//! - Transaction inclusion (if client provides tx body + merkle proof)
//! - Snapshot-based state reconstruction

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
};

use inferadb_ledger_store::{Database, StorageBackend, tables};
use inferadb_ledger_types::{NamespaceId, ShardBlock, VaultId, decode, encode};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};

/// Key for compaction watermark in COMPACTION_META table.
const COMPACTION_WATERMARK_KEY: &str = "compacted_before";

/// Blocks per segment file.
const SEGMENT_SIZE: u64 = 10_000;

/// Block archive error types.
#[derive(Debug, Snafu)]
pub enum BlockArchiveError {
    /// IO error during file operations.
    #[snafu(display("IO error: {source}"))]
    Io {
        /// The underlying IO error.
        source: std::io::Error,
    },

    /// Block not found at the requested height.
    #[snafu(display("Block not found at height {height}"))]
    BlockNotFound {
        /// The height that was not found.
        height: u64,
    },

    /// Codec error during serialization/deserialization.
    #[snafu(display("Codec error: {source}"))]
    Codec {
        /// The underlying codec error.
        source: inferadb_ledger_types::CodecError,
    },

    /// Storage engine error from inferadb-ledger-store.
    #[snafu(display("Storage error: {source}"))]
    Store {
        /// The underlying storage error.
        source: inferadb_ledger_store::Error,
    },
}

/// Result type for block archive operations.
pub type Result<T> = std::result::Result<T, BlockArchiveError>;

/// Statistics about block compaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionStats {
    /// Current compaction watermark (blocks below this are compacted).
    pub watermark: Option<u64>,
    /// Minimum block height in archive.
    pub min_height: Option<u64>,
    /// Maximum block height in archive.
    pub max_height: Option<u64>,
    /// Total number of blocks.
    pub total_blocks: u64,
    /// Number of compacted blocks (transaction bodies removed).
    pub compacted_blocks: u64,
    /// Number of blocks with full transaction bodies.
    pub uncompacted_blocks: u64,
}

/// Index entry for a block within a segment.
///
/// Fields are prefixed with `_` because segment file reading is not yet implemented;
/// the current implementation stores blocks in the database and only uses segment files
/// for writing. The index is preserved for future optimization of segment file reads.
#[derive(Debug, Clone, Copy)]
struct BlockIndex {
    /// Shard height of this block.
    _shard_height: u64,
    /// Offset in segment file.
    _offset: u64,
    /// Length of serialized block (including 4-byte length prefix).
    _length: u32,
}

/// Segment writer for active segment.
struct SegmentWriter {
    segment_id: u64,
    file: BufWriter<File>,
    index: Vec<BlockIndex>,
}

/// Block archive for a shard.
///
/// Uses inferadb-ledger-store for the primary block storage (tables::Blocks) and maintains
/// a vault_block_index for looking up shard heights by vault/namespace/height.
#[allow(clippy::result_large_err)]
pub struct BlockArchive<B: StorageBackend> {
    /// Database handle.
    db: Arc<Database<B>>,
    /// Directory for segment files (optional, for large deployments).
    blocks_dir: Option<PathBuf>,
    /// Cached segment indexes.
    #[allow(dead_code)] // reserved for segment-based block retrieval
    segment_indexes: RwLock<HashMap<u64, Vec<BlockIndex>>>,
    /// Current segment writer (for file-based storage).
    current_segment: RwLock<Option<SegmentWriter>>,
}

#[allow(clippy::result_large_err)]
impl<B: StorageBackend> BlockArchive<B> {
    /// Creates a new block archive backed by inferadb-ledger-store.
    pub fn new(db: Arc<Database<B>>) -> Self {
        Self {
            db,
            blocks_dir: None,
            segment_indexes: RwLock::new(HashMap::new()),
            current_segment: RwLock::new(None),
        }
    }

    /// Creates a block archive with file-based segment storage.
    ///
    /// Use this for large deployments where blocks exceed inferadb-ledger-store's practical limits.
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Io`] if creating the blocks directory fails.
    pub fn with_segment_files(db: Arc<Database<B>>, blocks_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&blocks_dir).context(IoSnafu)?;
        Ok(Self {
            db,
            blocks_dir: Some(blocks_dir),
            segment_indexes: RwLock::new(HashMap::new()),
            current_segment: RwLock::new(None),
        })
    }

    /// Append a block to the archive.
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Codec`] if serialization of the block fails.
    /// Returns [`BlockArchiveError::Store`] if the write transaction or commit fails.
    /// Returns [`BlockArchiveError::Io`] if writing to a segment file fails.
    pub fn append_block(&self, block: &ShardBlock) -> Result<()> {
        let encoded = encode(block).context(CodecSnafu)?;

        let mut txn = self.db.write().context(StoreSnafu)?;
        txn.insert::<tables::Blocks>(&block.shard_height, &encoded).context(StoreSnafu)?;

        for entry in &block.vault_entries {
            let index_key = encode_vault_block_index_key(
                entry.namespace_id,
                entry.vault_id,
                entry.vault_height,
            );
            txn.insert::<tables::VaultBlockIndex>(&index_key.to_vec(), &block.shard_height)
                .context(StoreSnafu)?;
        }

        txn.commit().context(StoreSnafu)?;

        if self.blocks_dir.is_some() {
            self.append_to_segment(block, &encoded)?;
        }

        Ok(())
    }

    /// Append block to segment file.
    fn append_to_segment(&self, block: &ShardBlock, encoded: &[u8]) -> Result<()> {
        let segment_id = block.shard_height / SEGMENT_SIZE;

        let mut current = self.current_segment.write();

        if current.as_ref().map(|s| s.segment_id) != Some(segment_id) {
            if let Some(writer) = current.take() {
                drop(writer);
            }

            let segment_path = self.segment_path(segment_id);
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&segment_path)
                .context(IoSnafu)?;

            *current =
                Some(SegmentWriter { segment_id, file: BufWriter::new(file), index: Vec::new() });
        }

        #[allow(clippy::expect_used)]
        let writer = current.as_mut().expect("segment writer exists after init above");

        let offset = writer.file.seek(SeekFrom::End(0)).context(IoSnafu)?;
        writer.file.write_all(&(encoded.len() as u32).to_le_bytes()).context(IoSnafu)?;
        writer.file.write_all(encoded).context(IoSnafu)?;
        writer.file.flush().context(IoSnafu)?;

        // Update index
        writer.index.push(BlockIndex {
            _shard_height: block.shard_height,
            _offset: offset,
            _length: (encoded.len() + 4) as u32,
        });

        Ok(())
    }

    /// Reads a block by shard height.
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Store`] if the read transaction fails.
    /// Returns [`BlockArchiveError::Codec`] if deserialization of the block fails.
    /// Returns [`BlockArchiveError::BlockNotFound`] if no block exists at the given height.
    pub fn read_block(&self, shard_height: u64) -> Result<ShardBlock> {
        let txn = self.db.read().context(StoreSnafu)?;

        match txn.get::<tables::Blocks>(&shard_height).context(StoreSnafu)? {
            Some(data) => {
                let block = decode(&data).context(CodecSnafu)?;
                Ok(block)
            },
            None => Err(BlockArchiveError::BlockNotFound { height: shard_height }),
        }
    }

    /// Finds the shard height containing a specific vault block.
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Store`] if the read transaction fails.
    pub fn find_shard_height(
        &self,
        namespace_id: NamespaceId,
        vault_id: VaultId,
        vault_height: u64,
    ) -> Result<Option<u64>> {
        let txn = self.db.read().context(StoreSnafu)?;

        let key = encode_vault_block_index_key(namespace_id, vault_id, vault_height);

        match txn.get::<tables::VaultBlockIndex>(&key.to_vec()).context(StoreSnafu)? {
            Some(data) => {
                // The value is a u64 encoded as big-endian bytes (by Value trait)
                if data.len() == 8 {
                    Ok(Some(u64::from_be_bytes(data.try_into().unwrap_or([0; 8]))))
                } else {
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    /// Returns the latest shard height in the archive.
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Store`] if the read transaction fails.
    pub fn latest_height(&self) -> Result<Option<u64>> {
        let txn = self.db.read().context(StoreSnafu)?;

        match txn.last::<tables::Blocks>().context(StoreSnafu)? {
            Some((key_bytes, _)) => {
                if key_bytes.len() == 8 {
                    Ok(Some(u64::from_be_bytes(key_bytes.try_into().unwrap_or([0; 8]))))
                } else {
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    /// Returns the range of heights in the archive.
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Store`] if the read transaction fails.
    pub fn height_range(&self) -> Result<Option<(u64, u64)>> {
        let txn = self.db.read().context(StoreSnafu)?;

        let first = txn.first::<tables::Blocks>().context(StoreSnafu)?;
        let last = txn.last::<tables::Blocks>().context(StoreSnafu)?;

        match (first, last) {
            (Some((first_key, _)), Some((last_key, _))) => {
                let first_height = u64::from_be_bytes(first_key.try_into().unwrap_or([0; 8]));
                let last_height = u64::from_be_bytes(last_key.try_into().unwrap_or([0; 8]));
                Ok(Some((first_height, last_height)))
            },
            _ => Ok(None),
        }
    }

    /// Reads blocks in a range.
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Store`] if the read transaction or iteration fails.
    /// Returns [`BlockArchiveError::Codec`] if deserialization of any block fails.
    pub fn read_range(&self, start: u64, end: u64) -> Result<Vec<ShardBlock>> {
        let txn = self.db.read().context(StoreSnafu)?;
        let mut blocks = Vec::new();

        for (key_bytes, value) in txn.iter::<tables::Blocks>().context(StoreSnafu)? {
            if key_bytes.len() != 8 {
                continue;
            }
            let height = u64::from_be_bytes(key_bytes.try_into().unwrap_or([0; 8]));

            if height < start {
                continue;
            }
            if height > end {
                break;
            }

            let block = decode(&value).context(CodecSnafu)?;
            blocks.push(block);
        }

        Ok(blocks)
    }

    // =========================================================================
    // Block Compaction
    // =========================================================================

    /// Returns the current compaction watermark.
    ///
    /// All blocks with height < watermark have been compacted (transaction bodies removed).
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Store`] if the read transaction fails.
    pub fn compaction_watermark(&self) -> Result<Option<u64>> {
        let txn = self.db.read().context(StoreSnafu)?;

        match txn
            .get::<tables::CompactionMeta>(&COMPACTION_WATERMARK_KEY.to_string())
            .context(StoreSnafu)?
        {
            Some(data) => {
                // Value is u64 encoded as big-endian by Value trait
                if data.len() == 8 {
                    Ok(Some(u64::from_be_bytes(data.try_into().unwrap_or([0; 8]))))
                } else {
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    /// Checks if a block at the given height has been compacted.
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Store`] if reading the compaction watermark fails.
    pub fn is_compacted(&self, height: u64) -> Result<bool> {
        match self.compaction_watermark()? {
            Some(watermark) => Ok(height < watermark),
            None => Ok(false),
        }
    }

    /// Compacts all blocks before the given height.
    ///
    /// Per DESIGN.md §4.4: After compaction, transaction bodies are removed but:
    /// - Block headers are preserved (state_root, tx_merkle_root, previous_hash)
    /// - Vault metadata is preserved (namespace_id, vault_id, vault_height)
    /// - Chain verification remains possible via ChainCommitment
    ///
    /// # Arguments
    /// * `before_height` - Compact all blocks with height < before_height
    ///
    /// # Returns
    /// The number of blocks that were compacted.
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Store`] if any read/write transaction fails.
    /// Returns [`BlockArchiveError::Codec`] if serialization or deserialization of blocks fails.
    pub fn compact_before(&self, before_height: u64) -> Result<u64> {
        let current_watermark = self.compaction_watermark()?.unwrap_or(0);
        if before_height <= current_watermark {
            return Ok(0);
        }

        let mut compacted_count = 0u64;
        let txn = self.db.read().context(StoreSnafu)?;
        let mut blocks_to_compact = Vec::new();

        for (key_bytes, value) in txn.iter::<tables::Blocks>().context(StoreSnafu)? {
            if key_bytes.len() != 8 {
                continue;
            }
            let height = u64::from_be_bytes(key_bytes.try_into().unwrap_or([0; 8]));

            if height < current_watermark {
                continue;
            }
            if height >= before_height {
                break;
            }

            let mut block: ShardBlock = decode(&value).context(CodecSnafu)?;

            let needs_compaction = block.vault_entries.iter().any(|e| !e.transactions.is_empty());

            if needs_compaction {
                for entry in &mut block.vault_entries {
                    entry.transactions.clear();
                }

                blocks_to_compact.push((height, block));
            }
        }

        drop(txn);

        if !blocks_to_compact.is_empty() {
            let mut txn = self.db.write().context(StoreSnafu)?;

            for (height, block) in &blocks_to_compact {
                let encoded = encode(block).context(CodecSnafu)?;

                txn.insert::<tables::Blocks>(height, &encoded).context(StoreSnafu)?;
            }

            compacted_count = blocks_to_compact.len() as u64;

            txn.insert::<tables::CompactionMeta>(
                &COMPACTION_WATERMARK_KEY.to_string(),
                &before_height,
            )
            .context(StoreSnafu)?;

            txn.commit().context(StoreSnafu)?;
        } else {
            // No blocks needed compaction, but still update watermark
            let mut txn = self.db.write().context(StoreSnafu)?;
            txn.insert::<tables::CompactionMeta>(
                &COMPACTION_WATERMARK_KEY.to_string(),
                &before_height,
            )
            .context(StoreSnafu)?;
            txn.commit().context(StoreSnafu)?;
        }

        Ok(compacted_count)
    }

    /// Returns compaction statistics.
    ///
    /// # Errors
    ///
    /// Returns [`BlockArchiveError::Store`] if reading the watermark or height range fails.
    pub fn compaction_stats(&self) -> Result<CompactionStats> {
        let watermark = self.compaction_watermark()?;
        let range = self.height_range()?;

        let (min_height, max_height, total_blocks) = match range {
            Some((min, max)) => (Some(min), Some(max), max.saturating_sub(min) + 1),
            None => (None, None, 0),
        };

        let compacted_count = match (watermark, min_height) {
            (Some(w), Some(min)) if w > min => w - min,
            _ => 0,
        };

        Ok(CompactionStats {
            watermark,
            min_height,
            max_height,
            total_blocks,
            compacted_blocks: compacted_count,
            uncompacted_blocks: total_blocks.saturating_sub(compacted_count),
        })
    }

    /// Returns segment file path.
    ///
    /// # Panics
    /// Panics if `blocks_dir` is not set (only called when segment files are enabled).
    #[allow(clippy::expect_used)]
    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.blocks_dir
            .as_ref()
            .expect("blocks_dir should be set")
            .join(format!("segment_{:06}.blk", segment_id))
    }
}

/// Encodes vault block index key.
///
/// Format: {namespace_id:8BE}{vault_id:8BE}{vault_height:8BE}
fn encode_vault_block_index_key(
    namespace_id: NamespaceId,
    vault_id: VaultId,
    vault_height: u64,
) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[0..8].copy_from_slice(&namespace_id.value().to_be_bytes());
    key[8..16].copy_from_slice(&vault_id.value().to_be_bytes());
    key[16..24].copy_from_slice(&vault_height.to_be_bytes());
    key
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests {
    use chrono::Utc;
    use inferadb_ledger_types::{ShardId, VaultEntry};

    use super::*;
    use crate::engine::InMemoryStorageEngine;

    fn create_test_block(shard_height: u64) -> ShardBlock {
        ShardBlock {
            shard_id: ShardId::new(1),
            shard_height,
            previous_shard_hash: [0u8; 32],
            vault_entries: vec![VaultEntry {
                namespace_id: NamespaceId::new(1),
                vault_id: VaultId::new(1),
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
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        let block = create_test_block(100);
        archive.append_block(&block).expect("append block");

        let loaded = archive.read_block(100).expect("read block");
        assert_eq!(loaded.shard_height, 100);
        assert_eq!(loaded.vault_entries.len(), 1);
    }

    #[test]
    fn test_vault_block_index() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        let block = create_test_block(100);
        archive.append_block(&block).expect("append block");

        // Should find the shard height from vault coordinates
        let shard_height = archive
            .find_shard_height(NamespaceId::new(1), VaultId::new(1), 100)
            .expect("find")
            .expect("should exist");
        assert_eq!(shard_height, 100);

        // Non-existent vault block
        let not_found =
            archive.find_shard_height(NamespaceId::new(1), VaultId::new(1), 999).expect("find");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_read_range() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
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
        let engine = InMemoryStorageEngine::open().expect("open engine");
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
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        assert!(archive.latest_height().expect("latest").is_none());

        archive.append_block(&create_test_block(50)).expect("append");
        assert_eq!(archive.latest_height().expect("latest"), Some(50));

        archive.append_block(&create_test_block(100)).expect("append");
        assert_eq!(archive.latest_height().expect("latest"), Some(100));
    }

    #[test]
    fn test_multiple_vaults_in_block() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        let mut block = create_test_block(100);
        block.vault_entries.push(VaultEntry {
            namespace_id: NamespaceId::new(1),
            vault_id: VaultId::new(2), // Different vault
            vault_height: 50,
            previous_vault_hash: [0u8; 32],
            transactions: vec![],
            tx_merkle_root: [0u8; 32],
            state_root: [42u8; 32],
        });

        archive.append_block(&block).expect("append block");

        // Both vaults should point to same shard height
        let h1 =
            archive.find_shard_height(NamespaceId::new(1), VaultId::new(1), 100).expect("find");
        let h2 = archive.find_shard_height(NamespaceId::new(1), VaultId::new(2), 50).expect("find");

        assert_eq!(h1, Some(100));
        assert_eq!(h2, Some(100));
    }

    // =========================================================================
    // Compaction Tests
    // =========================================================================

    fn create_block_with_transactions(shard_height: u64, tx_count: usize) -> ShardBlock {
        use inferadb_ledger_types::{Operation, Transaction};

        let transactions: Vec<Transaction> = (0..tx_count)
            .map(|i| Transaction {
                id: [i as u8; 16],
                client_id: format!("client-{}", i),
                sequence: i as u64,
                actor: "actor".to_string(),
                operations: vec![Operation::SetEntity {
                    key: format!("key-{}", i),
                    value: format!("value-{}", i).into_bytes(),
                    condition: None,
                    expires_at: None,
                }],
                timestamp: Utc::now(),
            })
            .collect();

        ShardBlock {
            shard_id: ShardId::new(1),
            shard_height,
            previous_shard_hash: [shard_height as u8; 32],
            vault_entries: vec![VaultEntry {
                namespace_id: NamespaceId::new(1),
                vault_id: VaultId::new(1),
                vault_height: shard_height,
                previous_vault_hash: [(shard_height.saturating_sub(1)) as u8; 32],
                transactions,
                tx_merkle_root: [100u8; 32],
                state_root: [shard_height as u8; 32],
            }],
            timestamp: Utc::now(),
            leader_id: "node-1".to_string(),
            term: 1,
            committed_index: shard_height,
        }
    }

    #[test]
    fn test_compaction_watermark_initially_none() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        assert!(archive.compaction_watermark().expect("get watermark").is_none());
        assert!(!archive.is_compacted(100).expect("is_compacted"));
    }

    #[test]
    fn test_compact_removes_transaction_bodies() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        // Add blocks with transactions
        for height in [100, 101, 102, 103, 104] {
            let block = create_block_with_transactions(height, 3);
            archive.append_block(&block).expect("append block");
        }

        // Verify transactions exist before compaction
        let block = archive.read_block(100).expect("read block");
        assert_eq!(block.vault_entries[0].transactions.len(), 3);

        // Compact blocks below height 103
        let compacted = archive.compact_before(103).expect("compact");
        assert_eq!(compacted, 3); // blocks 100, 101, 102

        // Verify watermark is set
        assert_eq!(archive.compaction_watermark().expect("watermark"), Some(103));

        // Verify compacted blocks have empty transactions
        for height in [100, 101, 102] {
            let block = archive.read_block(height).expect("read block");
            assert!(
                block.vault_entries[0].transactions.is_empty(),
                "Block {} should have empty transactions",
                height
            );
            // But headers are preserved
            assert_eq!(block.vault_entries[0].tx_merkle_root, [100u8; 32]);
            assert_eq!(block.vault_entries[0].state_root, [height as u8; 32]);
        }

        // Verify uncompacted blocks still have transactions
        for height in [103, 104] {
            let block = archive.read_block(height).expect("read block");
            assert_eq!(
                block.vault_entries[0].transactions.len(),
                3,
                "Block {} should still have transactions",
                height
            );
        }
    }

    #[test]
    fn test_is_compacted() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        // Add blocks
        for height in [100, 101, 102] {
            let block = create_block_with_transactions(height, 2);
            archive.append_block(&block).expect("append block");
        }

        // Before compaction
        assert!(!archive.is_compacted(100).expect("is_compacted"));
        assert!(!archive.is_compacted(101).expect("is_compacted"));

        // Compact
        archive.compact_before(101).expect("compact");

        // After compaction
        assert!(archive.is_compacted(100).expect("is_compacted")); // < 101
        assert!(!archive.is_compacted(101).expect("is_compacted")); // >= 101
        assert!(!archive.is_compacted(102).expect("is_compacted")); // >= 101
    }

    #[test]
    fn test_compaction_stats() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        // Initial stats
        let stats = archive.compaction_stats().expect("stats");
        assert!(stats.watermark.is_none());
        assert_eq!(stats.total_blocks, 0);

        // Add blocks
        for height in [100, 101, 102, 103, 104] {
            let block = create_block_with_transactions(height, 1);
            archive.append_block(&block).expect("append block");
        }

        // Stats before compaction
        let stats = archive.compaction_stats().expect("stats");
        assert_eq!(stats.total_blocks, 5);
        assert_eq!(stats.compacted_blocks, 0);
        assert_eq!(stats.uncompacted_blocks, 5);

        // Compact
        archive.compact_before(103).expect("compact");

        // Stats after compaction
        let stats = archive.compaction_stats().expect("stats");
        assert_eq!(stats.watermark, Some(103));
        assert_eq!(stats.min_height, Some(100));
        assert_eq!(stats.max_height, Some(104));
        assert_eq!(stats.total_blocks, 5);
        assert_eq!(stats.compacted_blocks, 3); // 100, 101, 102
        assert_eq!(stats.uncompacted_blocks, 2); // 103, 104
    }

    #[test]
    fn test_compact_is_idempotent() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        for height in [100, 101, 102] {
            let block = create_block_with_transactions(height, 2);
            archive.append_block(&block).expect("append block");
        }

        // First compaction
        let count1 = archive.compact_before(102).expect("compact");
        assert_eq!(count1, 2); // 100, 101

        // Second compaction with same watermark
        let count2 = archive.compact_before(102).expect("compact");
        assert_eq!(count2, 0); // No new blocks to compact

        // Watermark unchanged
        assert_eq!(archive.compaction_watermark().expect("watermark"), Some(102));
    }

    #[test]
    fn test_compact_preserves_vault_index() {
        let engine = InMemoryStorageEngine::open().expect("open engine");
        let archive = BlockArchive::new(engine.db());

        // Add block with transactions
        let block = create_block_with_transactions(100, 3);
        archive.append_block(&block).expect("append block");

        // Compact
        archive.compact_before(101).expect("compact");

        // Index should still work
        let shard_height = archive
            .find_shard_height(NamespaceId::new(1), VaultId::new(1), 100)
            .expect("find")
            .expect("should exist");
        assert_eq!(shard_height, 100);
    }
}
